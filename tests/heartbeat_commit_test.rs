// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Test that a follower receiving a heartbeat does not advance its
//! committed index.
//!
//! A heartbeat is a liveness signal, not a commit signal. The leader may
//! hold entries the follower has not matched (e.g. after a divergent branch
//! of the log, or while the follower is excluded from the leader's
//! replication set). Advancing committed from a heartbeat without log
//! matching would corrupt the follower's committed index permanently: it
//! never regresses, so a later overwrite of those entries leaves committed
//! pointing past the actual log and blocks snapshot catch-up.

use raft::eraftpb::{ConfState, Entry, HardState, Message, MessageType};
use raft::storage::MemStorage;
use raft::{Config, RawNode};
use slog::{o, Logger};

fn make_logger() -> Logger {
    Logger::root(slog::Discard, o!())
}

/// Creates a follower RawNode (id=2) in a 2-node cluster.
/// Storage is initialized with 5 entries (indices 1-5) at term 1,
/// committed = 3.
fn make_follower() -> RawNode<MemStorage> {
    let logger = make_logger();
    let storage = MemStorage::default();

    let mut cs = ConfState::default();
    cs.set_voters(vec![1, 2]);
    storage.initialize_with_conf_state(cs);

    // Initialize storage with entries [1,5], commit=3, term=1.
    let entries: Vec<Entry> = (1..=5)
        .map(|i| {
            let mut e = Entry::default();
            e.set_term(1);
            e.set_index(i);
            e
        })
        .collect();
    let mut hs = HardState::default();
    hs.set_term(1);
    hs.set_commit(3);
    {
        let mut core = storage.wl();
        core.append(&entries).unwrap();
        core.commit_to(3).unwrap();
        core.set_hardstate(hs);
    }

    let config = Config {
        id: 2,
        ..Default::default()
    };
    RawNode::new(&config, storage, &logger).unwrap()
}

fn heartbeat(from: u64, to: u64, term: u64, commit: u64) -> Message {
    let mut m = Message::default();
    m.set_from(from);
    m.set_to(to);
    m.set_msg_type(MessageType::MsgHeartbeat);
    m.set_term(term);
    m.set_commit(commit);
    m
}

/// Creates a RawNode (id=1 or id=3) in a 3-voter cluster.
/// Storage is initialized with 9 entries (indices 1-9) at term 1,
/// committed = 3.
fn make_node(node_id: u64, with_leader_entry: bool) -> RawNode<MemStorage> {
    let logger = make_logger();
    let storage = MemStorage::default();

    let mut cs = ConfState::default();
    cs.set_voters(vec![1, 2, 3]);
    storage.initialize_with_conf_state(cs);

    let mut entries: Vec<Entry> = (1..=9)
        .map(|i| {
            let mut e = Entry::default();
            e.set_term(1);
            e.set_index(i);
            e
        })
        .collect();
    if with_leader_entry {
        // The empty entry a leader appends at its own term (index 10, term 2
        // here). A follower that has replicated it can be matched with the
        // leader's last index yet still lag behind on committed.
        let mut e = Entry::default();
        e.set_term(2);
        e.set_index(10);
        entries.push(e);
    }
    let mut hs = HardState::default();
    hs.set_term(1);
    hs.set_commit(3);
    {
        let mut core = storage.wl();
        core.append(&entries).unwrap();
        core.commit_to(3).unwrap();
        core.set_hardstate(hs);
    }

    let config = Config {
        id: node_id,
        ..Default::default()
    };
    RawNode::new(&config, storage, &logger).unwrap()
}

/// An append response acknowledging the leader's last index (10).
fn append_ack(from: u64, term: u64, commit: u64) -> Message {
    let mut m = Message::default();
    m.set_msg_type(MessageType::MsgAppendResponse);
    m.set_from(from);
    m.set_to(1);
    m.set_term(term);
    m.set_index(10);
    m.set_log_term(2); // term of the entry the follower matched at index 10
    m.set_commit(commit);
    m
}

fn heartbeat_resp(from: u64, term: u64, commit: u64) -> Message {
    let mut m = Message::default();
    m.set_msg_type(MessageType::MsgHeartbeatResponse);
    m.set_from(from);
    m.set_to(1);
    m.set_term(term);
    m.set_commit(commit);
    m
}

/// When the empty append that carried a new commit index is lost, a
/// follower can be matched with the leader's last index yet still lag
/// behind on committed. A heartbeat never advances committed (by design),
/// so the leader must re-push the commit index via an append on the next
/// heartbeat response.
#[test]
fn test_leader_repushes_commit_to_lagging_follower() {
    let mut leader = make_node(1, false);
    leader.raft.become_candidate();
    leader.raft.become_leader(); // appends the empty term-2 entry at index 10
    leader.raft.msgs.clear(); // initial probe appends, discard

    let mut follower3 = make_node(3, true);

    // Node 2 acks entry 10; with leader + node 2 the commit advances to 10.
    leader.raft.step(append_ack(2, 2, 3)).unwrap();
    leader.raft.msgs.clear();

    // Node 3 acks entry 10 too. The leader's commit broadcast (the empty
    // append carrying commit=10) is produced here and then "lost" — we
    // discard it without delivering it to node 3, leaving node 3 matched
    // with the leader's last index but still committed at 3.
    leader.raft.step(append_ack(3, 2, 3)).unwrap();
    leader.raft.msgs.clear();
    assert_eq!(
        follower3.status().hs.commit,
        3,
        "precondition: follower committed must lag after the lost commit broadcast"
    );

    // The next heartbeat response reports the follower's real committed
    // index. The leader must notice the lag and re-send an (empty) append
    // carrying commit=10.
    leader.raft.step(heartbeat_resp(3, 2, 3)).unwrap();
    let repush = leader
        .raft
        .msgs
        .iter()
        .find(|m| m.get_msg_type() == MessageType::MsgAppend && m.get_to() == 3)
        .cloned()
        .expect("leader must re-push commit via append after heartbeat response");
    assert_eq!(
        repush.get_commit(),
        10,
        "re-pushed append must carry the commit"
    );
    assert!(
        repush.get_entries().is_empty(),
        "re-pushed append must be empty (follower already matched)"
    );
    leader.raft.msgs.clear();

    // Delivering it makes the follower commit through the append path,
    // where log matching is re-verified before committed may move.
    follower3.raft.step(repush).unwrap();
    assert_eq!(
        follower3.status().hs.commit,
        10,
        "follower must commit after receiving the re-pushed append"
    );
}

#[test]
fn test_heartbeat_does_not_advance_committed() {
    let mut follower = make_follower(); // committed=3, last_index=5
    let committed_before = follower.status().hs.commit;

    // The leader believes the follower has matched index 18 and sends a
    // heartbeat with commit=18, far above the follower's last_index=5.
    // A heartbeat must not move the follower's committed index: the
    // follower never matched 4..18 via append. Before the fix, this would
    // both panic ("to_commit 18 is out of range") and, when clamped,
    // advance committed past the matched log.
    follower
        .step(heartbeat(1, 2, 1, 18))
        .expect("heartbeat must not panic");

    assert_eq!(
        follower.status().hs.commit,
        committed_before,
        "HB_TEST_V2 heartbeat must not advance committed"
    );
}

#[test]
fn test_heartbeat_committed_unchanged_within_range() {
    // Normal case: heartbeat commit within the follower's log range.
    // Still no advancement — committed only moves via the append path.
    let mut follower = make_follower(); // committed=3, last_index=5
    let committed_before = follower.status().hs.commit;

    follower
        .step(heartbeat(1, 2, 1, 4))
        .expect("heartbeat must not panic");

    assert_eq!(
        follower.status().hs.commit,
        committed_before,
        "HB_TEST_V2 heartbeat must not advance committed"
    );
}
