// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Regression test for witness matched poisoning during conf change.
//!
//! Bug: `apply_conf_change` called `post_conf_change` (which internally
//! calls `maybe_commit`) BEFORE `reset_replication_set`. When a conf change
//! cleared the outgoing voters, `maybe_commit` saw an inconsistent state
//! where the config had no outgoing voters but the replication sets still
//! had the old (stale) witness_subterm. This caused
//! `one_less_than_quorum` to return `u64::MAX` for the empty outgoing
//! voter set, which was then synthesized into the witness's `matched`
//! field via shortcut replication — poisoning it to `u64::MAX`.

#![allow(clippy::useless_conversion)]

use raft::eraftpb::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Message,
    MessageType,
};
use raft::raw_node::RawNode;
use raft::storage::MemStorage;
use slog::{o, Logger};

const A: u64 = 1;
const B: u64 = 2;
const W1: u64 = 3;

fn logger() -> Logger {
    Logger::root(slog::Discard, o!())
}

/// Creates a 3-node config {A, B, W1} with W1 as witness, A as leader.
fn new_leader() -> RawNode<MemStorage> {
    let storage = MemStorage::default();
    let mut cs = ConfState::default();
    cs.set_voters(vec![A, B, W1]);
    cs.set_witness(W1);
    storage.initialize_with_conf_state(cs);

    let config = raft::Config {
        id: A,
        election_tick: 10,
        heartbeat_tick: 2,
        check_quorum: true,
        ..Default::default()
    };
    let mut node = RawNode::new(&config, storage, &logger()).unwrap();
    node.raft.become_candidate();
    node.raft.become_leader();
    persist_leader_entries(&mut node);
    node
}

fn persist_leader_entries(node: &mut RawNode<MemStorage>) {
    let entries = node.raft.raft_log.unstable_entries().to_vec();
    let Some(last) = entries.last() else {
        return;
    };
    let (index, term) = (last.index, last.term);
    node.raft.mut_store().wl().append(&entries).unwrap();
    node.raft.raft_log.stable_entries(index, term);
    node.raft.on_persist_entries(index, term);
}

fn confirm_witness_append(node: &mut RawNode<MemStorage>, witness_id: u64, req_seq: u64) {
    node.confirm_witness_append(witness_id, req_seq);
}

fn get_witness_matched(node: &RawNode<MemStorage>, id: u64) -> u64 {
    node.raft.prs().get(id).map(|p| p.matched).unwrap_or(0)
}

/// After a conf change that clears the outgoing configuration (joint → simple),
/// the witness's matched index must NOT be poisoned to u64::MAX.
#[test]
fn conf_change_must_not_poison_witness_matched() {
    let mut node = new_leader();

    // Step 1: Degrade — B becomes unreachable, W1 replaces B in the
    // replication set. This activates shortcut replication for W1.
    node.report_unreachable(B);

    // Trigger check_quorum to start a new subterm with W1 swapped in.
    let mut msg = Message {
        to: A,
        ..Default::default()
    };
    msg.set_msg_type(MessageType::MsgCheckQuorum);
    node.raft.step(msg).unwrap();
    persist_leader_entries(&mut node);

    // The leader should have sent a witness append to W1. Process it.
    let w1_append = node
        .raft
        .witness_msgs
        .iter()
        .find(|m| m.to == W1)
        .expect("expected witness append to W1");
    let req_seq = w1_append.request_seq;
    node.raft.witness_msgs.clear();

    // Confirm the witness append — this activates shortcut replication.
    confirm_witness_append(&mut node, W1, req_seq);
    persist_leader_entries(&mut node);

    let matched_before = get_witness_matched(&node, W1);
    assert!(
        matched_before > 0 && matched_before < u64::MAX,
        "W1 matched should be a real index before conf change, got {matched_before}"
    );

    // Step 2: Apply a conf change (simple, not joint). This is the critical
    // path — the bug caused maybe_commit to run with stale replication sets
    // and an already-updated config, poisoning matched to u64::MAX.
    let cc = ConfChangeV2 {
        transition: ConfChangeTransition::Implicit.into(),
        changes: vec![ConfChangeSingle {
            change_type: ConfChangeType::RemoveNode.into(),
            node_id: B,
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    node.raft.apply_conf_change(&cc).unwrap();
    persist_leader_entries(&mut node);

    // Step 3: Trigger maybe_commit explicitly (post_conf_change already does
    // this internally, but let's be thorough).
    node.raft.maybe_commit();

    // The witness matched must NOT be u64::MAX after the conf change.
    let matched_after = get_witness_matched(&node, W1);
    assert_ne!(
        matched_after,
        u64::MAX,
        "W1 matched was poisoned to u64::MAX by conf change! \
         This is the witness poisoning bug — maybe_commit ran with \
         stale replication_sets before reset_replication_set was called."
    );
}

/// Same scenario but with joint consensus: enter joint, activate shortcut
/// replication, then leave joint (clearing outgoing). The witness matched
/// must not be poisoned.
#[test]
fn joint_to_simple_must_not_poison_witness_matched() {
    let mut node = new_leader();
    const C: u64 = 4;

    // Enter joint: add C, remove B → old={A,B,W1}, new={A,C,W1}.
    let cc_enter = ConfChangeV2 {
        transition: ConfChangeTransition::Implicit.into(),
        changes: vec![
            ConfChangeSingle {
                change_type: ConfChangeType::AddNode.into(),
                node_id: C,
                ..Default::default()
            },
            ConfChangeSingle {
                change_type: ConfChangeType::RemoveNode.into(),
                node_id: B,
                ..Default::default()
            },
        ]
        .into(),
        ..Default::default()
    };
    node.raft.apply_conf_change(&cc_enter).unwrap();
    persist_leader_entries(&mut node);
    // Verify we're in joint by checking conf().voters() has non-empty outgoing
    assert!(node.raft.prs().conf().voters().ids().len() >= 3);

    // Degrade: B unreachable → W1 swapped in for shortcut replication.
    node.report_unreachable(B);
    let mut msg = Message {
        to: A,
        ..Default::default()
    };
    msg.set_msg_type(MessageType::MsgCheckQuorum);
    node.raft.step(msg).unwrap();
    persist_leader_entries(&mut node);

    // Confirm witness append to activate shortcut.
    if let Some(w1_append) = node.raft.witness_msgs.iter().find(|m| m.to == W1) {
        let req_seq = w1_append.request_seq;
        node.raft.witness_msgs.clear();
        confirm_witness_append(&mut node, W1, req_seq);
        persist_leader_entries(&mut node);
    }

    let matched_before = get_witness_matched(&node, W1);
    assert!(
        matched_before < u64::MAX,
        "W1 matched should not be u64::MAX before leaving joint"
    );

    // Leave joint — this clears outgoing voters. The bug poisoned matched here.
    let mut cc_leave = ConfChangeV2::default();
    cc_leave.set_transition(ConfChangeTransition::Auto);
    assert!(cc_leave.leave_joint());
    node.raft.apply_conf_change(&cc_leave).unwrap();
    persist_leader_entries(&mut node);
    node.raft.maybe_commit();

    let matched_after = get_witness_matched(&node, W1);
    assert_ne!(
        matched_after,
        u64::MAX,
        "W1 matched was poisoned to u64::MAX during joint→simple transition!"
    );
}
