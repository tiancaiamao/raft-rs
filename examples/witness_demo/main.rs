// Extended Raft Witness Demo — Full Lifecycle
//
// 2 voters (node 1, 2) + 1 witness (node 3):
//
//   Phase 1: Bootstrap — node 1 as leader, add node 2 as voter
//   Phase 2: Add witness — node 3 joins as witness
//   Phase 3: Normal write — propose data, observe replication
//   Phase 4: Node 2 fails — leader detects, switches replication set,
//            continues committing via witness
//   Phase 5: Node 2 recovers — catches up
//
// Run: cargo run --example witness_demo

use std::collections::{HashMap, HashSet};

use raft::eraftpb::WitnessMessage;
use raft::prelude::*;
use raft::protocompat::PbMessage;
use raft::storage::MemStorage;
use raft::witness::{Witness, WitnessResponse};
use raft::StateRole;
use raft_proto::ConfChangeI;

// ─── Cluster Simulator ────────────────────────────────────────────────────

struct Cluster {
    nodes: HashMap<u64, Option<RawNode<MemStorage>>>,
    witness: Witness,
    witness_id: u64,
    pending_msgs: Vec<Message>,
    pending_witness_msgs: Vec<WitnessMessage>,
    dead_nodes: HashSet<u64>,
    witness_heartbeat_count: usize,
    witness_append_count: usize,
}

impl Cluster {
    fn new(witness_id: u64) -> Self {
        Self {
            nodes: HashMap::new(),
            witness: Witness::new(witness_id),
            witness_id,
            pending_msgs: Vec::new(),
            pending_witness_msgs: Vec::new(),
            dead_nodes: HashSet::new(),
            witness_heartbeat_count: 0,
            witness_append_count: 0,
        }
    }

    fn add_node(&mut self, id: u64, voters: Vec<u64>, logger: &slog::Logger) {
        let cfg = Config {
            id,
            check_quorum: true,
            ..Default::default()
        };
        let store = MemStorage::new_with_conf_state(ConfState::from((voters, vec![])));
        let node = RawNode::new(&cfg, store, logger).unwrap();
        self.nodes.insert(id, Some(node));
    }

    fn node(&mut self, id: u64) -> &mut RawNode<MemStorage> {
        self.nodes.get_mut(&id).unwrap().as_mut().unwrap()
    }

    fn leader_id(&self) -> u64 {
        for (&id, node) in &self.nodes {
            if let Some(n) = node {
                if n.raft.state == StateRole::Leader {
                    return id;
                }
            }
        }
        0
    }

    fn kill(&mut self, id: u64) {
        self.dead_nodes.insert(id);
        println!("  ☠ node {} killed", id);
    }

    fn recover(&mut self, id: u64) {
        self.dead_nodes.remove(&id);
        println!("  ♻ node {} recovered", id);
    }

    fn tick(&mut self) {
        for (&id, node_opt) in &mut self.nodes {
            if self.dead_nodes.contains(&id) {
                continue;
            }
            if let Some(node) = node_opt {
                node.tick();
            }
        }
    }

    fn process(&mut self) {
        let node_ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in node_ids {
            if self.dead_nodes.contains(&id) {
                continue;
            }
            let has_ready = self
                .nodes
                .get(&id)
                .and_then(|n| n.as_ref())
                .map(|n| n.has_ready())
                .unwrap_or(false);
            if !has_ready {
                continue;
            }

            let node = self.node(id);
            let mut ready = node.ready();
            let store = node.raft.raft_log.store.clone();

            if !ready.snapshot().is_empty() {
                store.wl().apply_snapshot(ready.snapshot().clone()).unwrap();
            }
            if !ready.entries().is_empty() {
                store.wl().append(ready.entries()).unwrap();
            }
            if let Some(hs) = ready.hs() {
                store.wl().set_hardstate(hs.clone());
            }
            let msgs = ready.take_messages();
            let persisted_msgs = ready.take_persisted_messages();
            let witness_msgs = ready.take_witness_messages();
            let mut committed = ready.take_committed_entries();

            let mut light = self.node(id).advance(ready);
            if let Some(commit) = light.commit_index() {
                store.wl().mut_hard_state().set_commit(commit);
            }
            let light_msgs: Vec<Message> = light.take_messages();
            let mut light_committed = light.take_committed_entries();
            self.node(id).advance_apply();

            committed.append(&mut light_committed);
            for entry in committed.drain(..) {
                self.apply_entry(id, &entry);
            }

            self.pending_msgs.extend(msgs);
            self.pending_msgs.extend(persisted_msgs);
            self.pending_msgs.extend(light_msgs);
            self.pending_witness_msgs.extend(witness_msgs);
        }

        // Deliver regular messages.
        self.deliver_msgs();

        // Deliver witness messages and translate responses.
        let wmsgs = std::mem::take(&mut self.pending_witness_msgs);
        for wm in wmsgs {
            if let Some(resp) = self.witness.process(&wm) {
                self.translate_witness_response(&wm, resp);
            }
        }

        // Deliver witness-translated responses.
        self.deliver_msgs();
    }

    fn deliver_msgs(&mut self) {
        let msgs = std::mem::take(&mut self.pending_msgs);
        for m in msgs {
            if self.dead_nodes.contains(&m.to) || self.dead_nodes.contains(&m.from) {
                continue;
            }
            if let Some(node_opt) = self.nodes.get_mut(&m.to) {
                if let Some(node) = node_opt.as_mut() {
                    let _ = node.step(m);
                }
            }
        }
    }

    /// Translate witness response back to a regular Raft message.
    /// This is the KEY integration point: the witness processes WitnessMessages
    /// and returns a response. The application translates this back into a
    /// standard Raft message and feeds it to the leader via step().
    fn translate_witness_response(&mut self, orig: &WitnessMessage, resp: WitnessResponse) {
        match resp {
            WitnessResponse::Persist(state) => {
                let term = state.get_state().term;
                let commit = state.get_state().commit;
                match orig.msg_type {
                    MessageType::MsgAppend => {
                        self.witness_append_count += 1;
                        let mut m = Message::default();
                        m.set_msg_type(MessageType::MsgAppendResponse);
                        m.from = self.witness_id;
                        m.to = orig.from;
                        m.term = term;
                        m.index = state.last_log_index;
                        m.commit = commit;
                        self.pending_msgs.push(m);
                    }
                    MessageType::MsgHeartbeat => {
                        self.witness_heartbeat_count += 1;
                        let mut m = Message::default();
                        m.set_msg_type(MessageType::MsgHeartbeatResponse);
                        m.from = self.witness_id;
                        m.to = orig.from;
                        m.term = term;
                        m.commit = commit;
                        self.pending_msgs.push(m);
                    }
                    _ => {}
                }
            }
            WitnessResponse::VoteGrant(granted) => {
                let mut m = Message::default();
                m.set_msg_type(MessageType::MsgRequestVoteResponse);
                m.from = self.witness_id;
                m.to = orig.from;
                m.term = self.witness.term;
                m.reject = !granted;
                self.pending_msgs.push(m);
            }
        }
    }

    fn apply_entry(&mut self, node_id: u64, entry: &Entry) {
        match entry.get_entry_type() {
            EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                let cc = if entry.get_entry_type() == EntryType::EntryConfChange {
                    let mut v1 = ConfChange::default();
                    v1.merge_from_bytes(&entry.data).unwrap();
                    v1.into_v2()
                } else {
                    let mut v2 = ConfChangeV2::default();
                    v2.merge_from_bytes(&entry.data).unwrap();
                    v2
                };
                let is_leader = self
                    .nodes
                    .get(&node_id)
                    .and_then(|n| n.as_ref())
                    .map(|n| n.raft.state == StateRole::Leader)
                    .unwrap_or(false);
                let node = self.node(node_id);
                node.apply_conf_change(&cc).unwrap();
                let conf = node.raft.prs().conf();
                let cs = conf.to_conf_state();
                let witnesses = conf.witnesses;
                let voters: Vec<_> = conf.voters().ids().iter().collect();
                node.raft.raft_log.store.wl().set_conf_state(cs);
                if is_leader {
                    println!("  [conf] voters={:?} witnesses={:?}", voters, witnesses);
                }
            }
            EntryType::EntryNormal if !entry.data.is_empty() => {
                println!(
                    "  [apply] node {} applied: {:?} (idx={})",
                    node_id,
                    String::from_utf8_lossy(&entry.data),
                    entry.index
                );
            }
            _ => {}
        }
    }

    fn step_n(&mut self, n: usize) {
        for _ in 0..n {
            self.tick();
            self.process();
        }
    }

    fn commit_index(&self, id: u64) -> u64 {
        self.nodes
            .get(&id)
            .and_then(|n| n.as_ref())
            .map(|n| n.raft.raft_log.committed)
            .unwrap_or(0)
    }

    fn last_index(&self, id: u64) -> u64 {
        self.nodes
            .get(&id)
            .and_then(|n| n.as_ref())
            .map(|n| n.raft.raft_log.last_index())
            .unwrap_or(0)
    }
}

// ─── Main Demo ────────────────────────────────────────────────────────────

fn main() {
    let logger = raft::default_logger();
    let mut cluster = Cluster::new(3);

    // ── Phase 1: Bootstrap ─────────────────────────────────────────────
    println!("╔══ Phase 1: Bootstrap ════════════════════════════════════════╗");
    println!("║  Start node 1 as single-node leader, then add node 2.       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    cluster.add_node(1, vec![1], &logger);
    cluster.node(1).campaign().unwrap();
    cluster.process();
    println!("  ✓ Node 1 is leader (term {})", cluster.node(1).raft.term);

    cluster.add_node(2, vec![1], &logger);
    let mut cc = ConfChangeV2::default();
    cc.set_changes(
        vec![raft_proto::new_conf_change_single(
            2,
            ConfChangeType::AddNode,
        )]
        .into(),
    );
    cluster.node(1).propose_conf_change(vec![], cc).unwrap();
    cluster.step_n(30);
    println!(
        "  ✓ Node 2 added. Voters: {:?}",
        cluster
            .node(1)
            .raft
            .prs()
            .conf()
            .voters()
            .ids()
            .iter()
            .collect::<Vec<_>>()
    );

    // ── Phase 2: Add witness ──────────────────────────────────────────
    println!("\n╔══ Phase 2: Add Witness (node 3) ═════════════════════════════╗");
    println!("║  Node 3 joins as a witness — it's in the voter set but      ║");
    println!("║  doesn't run a full Raft instance. It stores state          ║");
    println!("║  externally (e.g. S3 in CSE).                               ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    cluster.witness_heartbeat_count = 0;
    let mut cc_w = ConfChangeV2::default();
    cc_w.set_changes(
        vec![raft_proto::new_conf_change_single(
            3,
            ConfChangeType::AddWitness,
        )]
        .into(),
    );
    cluster.node(1).propose_conf_change(vec![], cc_w).unwrap();
    cluster.step_n(30);
    println!(
        "  ✓ Witness added. Witnesses: {:?}",
        cluster.node(1).raft.prs().conf().witnesses
    );
    println!(
        "  ✓ Witness received {} heartbeats so far",
        cluster.witness_heartbeat_count
    );

    // ── Phase 3: Normal write ─────────────────────────────────────────
    println!("\n╔══ Phase 3: Normal Write ═════════════════════════════════════╗");
    println!("║  Propose data. Leader replicates to node 2 normally.        ║");
    println!("║  Witness receives heartbeats (but not entries — not needed  ║");
    println!("║  since both voters are healthy).                            ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    cluster.node(1).propose(vec![], b"data-1".to_vec()).unwrap();
    cluster.step_n(20);

    println!(
        "  ✓ Node 1 commit={} last={}",
        cluster.commit_index(1),
        cluster.last_index(1)
    );
    println!(
        "  ✓ Node 2 commit={} last={}",
        cluster.commit_index(2),
        cluster.last_index(2)
    );
    println!(
        "  ✓ Witness: {} heartbeats, {} appends",
        cluster.witness_heartbeat_count, cluster.witness_append_count
    );

    // ── Phase 4: Node 2 fails ─────────────────────────────────────────
    println!("\n╔══ Phase 4: Node 2 Fails ═════════════════════════════════════╗");
    println!("║  Node 2 goes down. In standard Raft (2 voters), the leader  ║");
    println!("║  can't commit — no majority. But with the witness, the      ║");
    println!("║  leader detects the failure and switches the replication    ║");
    println!("║  set to use the witness for commits.                        ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    cluster.kill(2);
    cluster.witness_heartbeat_count = 0;
    cluster.witness_append_count = 0;
    cluster.step_n(40);

    let leader = cluster.leader_id();
    if leader == 1 {
        println!("  ✓ Node 1 still leader (survived with witness!)");
    } else {
        println!("  ⚠ Leader status: id={}", leader);
    }

    // Try to commit while node 2 is down.
    if leader == 1 {
        let commit_before = cluster.commit_index(1);
        cluster
            .node(1)
            .propose(vec![], b"survived-node2-down".to_vec())
            .unwrap();
        cluster.step_n(40);
        let commit_after = cluster.commit_index(1);

        if commit_after > commit_before {
            println!(
                "  ✓✓ COMMITTED while node 2 is down! (commit {} → {})",
                commit_before, commit_after
            );
            println!("     This is the power of Extended Raft: 2 voters + 1 witness");
            println!("     can tolerate 1 voter failure for commits.");
        } else {
            println!("  ✗ Could not commit (stayed at {})", commit_before);
        }
    }
    println!(
        "  ✓ Witness: {} heartbeats, {} appends (during failure)",
        cluster.witness_heartbeat_count, cluster.witness_append_count
    );

    // ── Phase 5: Node 2 recovers ──────────────────────────────────────
    println!("\n╔══ Phase 5: Node 2 Recovers ══════════════════════════════════╗");
    println!("║  Node 2 comes back. The leader catches it up via normal     ║");
    println!("║  Raft replication.                                          ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    cluster.recover(2);
    cluster.step_n(40);

    println!(
        "  ✓ Node 1 commit={} last={}",
        cluster.commit_index(1),
        cluster.last_index(1)
    );
    println!(
        "  ✓ Node 2 commit={} last={}",
        cluster.commit_index(2),
        cluster.last_index(2)
    );
    if cluster.commit_index(1) == cluster.commit_index(2) {
        println!("  ✓ Node 2 caught up!");
    }

    // ── Summary ───────────────────────────────────────────────────────
    println!("\n═══════════════════════════════════════════════════════════════");
    println!(
        "  Witness final state: term={} index={} commit={}",
        cluster.witness.term, cluster.witness.last_log_index, cluster.witness.commit
    );
    println!("  Done!");
}
