// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Known-bug reproductions for witness replacement under Joint Consensus.
//!
//! These tests assert the behavior required for safety and liveness. They are
//! ignored by default because the PR under review is expected to fail them. Run with:
//!
//!     cargo test --test witness_joint_repro -- --ignored
//!
//! The scenarios use normal configuration changes, peer reachability reports,
//! Raft messages, and witness processing. The only direct log manipulation is
//! in `persist_leader_entries`, which models the application's durable-storage
//! callback.

#![allow(clippy::useless_conversion)]

use std::collections::HashSet;

use raft::eraftpb::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState, Message,
    MessageType, WitnessMessage,
};
use raft::raw_node::RawNode;
use raft::storage::MemStorage;
use raft::{Witness, WitnessResponse};
use slog::{o, Logger};

const A: u64 = 1;
const B: u64 = 2;
const W1: u64 = 3;
const W2: u64 = 4;

fn logger() -> Logger {
    Logger::root(slog::Discard, o!())
}

/// Creates the old configuration {A, B, W1} and makes A leader.
///
/// Direct role transitions are the standard raft-rs test-harness shorthand
/// for an election won by A+B; none of the reproductions depend on forged
/// vote or log state.
fn new_old_config_leader() -> RawNode<MemStorage> {
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

/// Models the application persisting all unstable entries and then notifying
/// Raft. This is the real boundary at which the leader's own matched index can
/// advance.
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

/// Delivers the same local event that the leader's election-timeout tick
/// generates when check_quorum is enabled.
fn run_check_quorum(node: &mut RawNode<MemStorage>) {
    let mut msg = Message {
        to: A,
        ..Default::default()
    };
    msg.set_msg_type(MessageType::MsgCheckQuorum);
    node.raft.step(msg).unwrap();
}

/// Models a normal AppendEntries response from B after B has caught up.
fn acknowledge_from_b(node: &mut RawNode<MemStorage>) {
    let mut msg = Message {
        from: B,
        to: A,
        term: node.raft.term,
        index: node.raft.raft_log.last_index(),
        ..Default::default()
    };
    msg.set_msg_type(MessageType::MsgAppendResponse);
    node.raft.step(msg).unwrap();
}

fn enter_joint_replacing_w1_with_w2(node: &mut RawNode<MemStorage>) {
    let cc = ConfChangeV2 {
        transition: ConfChangeTransition::Implicit.into(),
        changes: vec![
            ConfChangeSingle {
                change_type: ConfChangeType::RemoveNode.into(),
                node_id: W1,
                ..Default::default()
            },
            ConfChangeSingle {
                change_type: ConfChangeType::AddWitness.into(),
                node_id: W2,
                ..Default::default()
            },
        ]
        .into(),
        ..Default::default()
    };

    // This is the application hook used when the committed EnterJoint entry is
    // applied. The resulting real configuration is:
    // old={A,B,W1}, new={A,B,W2}.
    node.raft.apply_conf_change(&cc).unwrap();
    assert_eq!(node.raft.prs().conf().witnesses, [W2, W1]);
}

/// Reports B unreachable, lets the normal check-quorum path replace B with
/// each configuration's witness, persists the resulting barrier, and returns
/// the witness append requests produced by the leader.
fn degrade_after_b_failure(node: &mut RawNode<MemStorage>) -> Vec<WitnessMessage> {
    node.raft.witness_msgs.clear();
    node.report_unreachable(B);
    run_check_quorum(node);
    persist_leader_entries(node);
    node.raft.maybe_commit();
    std::mem::take(&mut node.raft.witness_msgs)
        .into_iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgAppend)
        .collect()
}

fn message_to(messages: &[WitnessMessage], witness_id: u64) -> WitnessMessage {
    messages
        .iter()
        .find(|m| m.to == witness_id)
        .unwrap_or_else(|| panic!("missing witness append to {witness_id}: {messages:?}"))
        .clone()
}

#[test]
fn delayed_witness_success_must_not_confirm_a_new_replication_version() {
    let mut node = new_old_config_leader();
    enter_joint_replacing_w1_with_w2(&mut node);

    // T0: B becomes unavailable during the joint configuration. The leader
    // starts replication version X and sends W2 a witness append request.
    //
    // T1: The external storage write for version X succeeds, but the runtime
    // delays its completion callback. The callback API retains only W2 and
    // request_seq; it does not retain X's term/subterm/config generation.
    let first_requests = degrade_after_b_failure(&mut node);
    let delayed_w2_success = message_to(&first_requests, W2);
    let first_subterm = node.raft.prs().epoch.subterm;

    // T2: B recovers and acknowledges the leader's log. The next check-quorum
    // round changes the replication sets and leaves version X.
    acknowledge_from_b(&mut node);
    run_check_quorum(&mut node);
    persist_leader_entries(&mut node);
    assert!(node.raft.prs().epoch.subterm > first_subterm);

    // T3: B fails again. The leader starts version Y and sends a new W2 write.
    // Because request_seq is carried forward across epoch resets (the fix),
    // the new request gets a different seq from the delayed one.
    let second_requests = degrade_after_b_failure(&mut node);
    let _current_w2_request = message_to(&second_requests, W2);
    let current_subterm = node.raft.prs().epoch.subterm;
    assert_ne!(
        node.raft.prs().epoch.witness_subterm[0],
        current_subterm,
        "the current W2 request has not completed yet"
    );

    // T4: Before Y's storage write completes, deliver X's delayed success.
    // A safe implementation must reject it — the stale callback's request_seq
    // must not match the current pending request.
    node.confirm_witness_append(W2, delayed_w2_success.request_seq);
    assert_ne!(
        node.raft.prs().epoch.witness_subterm[0],
        current_subterm,
        "an old W2 storage callback authorized the current replication version"
    );
}

#[test]
fn isolated_joint_leader_must_not_finish_read_index_without_any_response() {
    let mut node = new_old_config_leader();

    // T0: Commit A's leader no-op through a real response from B. A has met
    // Raft's normal prerequisite for serving ReadIndex in its current term.
    acknowledge_from_b(&mut node);
    assert!(node.raft.commit_to_current_term());

    // T1: Enter old={A,B,W1}, new={A,B,W2} joint configuration.
    enter_joint_replacing_w1_with_w2(&mut node);
    node.raft.read_states.clear();
    node.raft.msgs.clear();

    // T2: A becomes isolated from B, W1, and W2. A has not reached its local
    // check-quorum timeout yet, so it still believes it is leader.
    //
    // T3: A receives a local ReadIndex request. We deliver no heartbeat response
    // from B and no proof from either witness. Safe ReadIndex must remain pending.
    // The PR inserts W1 and W2 into the acknowledgement set locally and completes
    // the read synchronously.
    node.read_index(b"isolated-joint-leader".to_vec());
    assert!(
        node.raft.read_states.is_empty(),
        "Safe ReadIndex completed without receiving any quorum response"
    );
}

#[test]
fn failure_of_w2_must_not_erase_the_still_active_old_configuration() {
    let mut node = new_old_config_leader();
    enter_joint_replacing_w1_with_w2(&mut node);

    // T0: B fails. The old configuration replaces B with W1 and the new
    // configuration replaces B with W2. Complete both real witness writes, so
    // W1 and W2 are active in this replication version.
    let requests = degrade_after_b_failure(&mut node);
    let w1_request = message_to(&requests, W1);
    let w2_request = message_to(&requests, W2);
    node.confirm_witness_append(W1, w1_request.request_seq);
    node.confirm_witness_append(W2, w2_request.request_seq);

    assert_eq!(node.raft.prs().epoch.replication_sets[0].witness, W2);
    assert_eq!(node.raft.prs().epoch.replication_sets[1].witness, W1);

    // T1: Only the W2 store becomes unavailable. The store-liveness detector
    // reports W2 unreachable; W1 remains active and receives no failure event.
    node.report_unreachable(W2);

    // T2: Check-quorum adjusts only the new configuration. The implementation
    // builds a fresh, empty Epoch, fills the changed W2 side, and then replaces
    // the whole Epoch. The unchanged W1 side is therefore erased.
    run_check_quorum(&mut node);
    assert_eq!(
        node.raft.prs().epoch.replication_sets[1].witness,
        W1,
        "handling W2 failure erased the unchanged old configuration"
    );
    assert!(
        node.raft.prs().epoch.replication_sets[1]
            .non_witness_voters
            .contains(&W1),
        "the active W1 was removed from the old configuration's replication set"
    );
}

#[test]
fn each_witness_must_store_only_the_replication_set_it_represents() {
    let mut node = new_old_config_leader();
    enter_joint_replacing_w1_with_w2(&mut node);

    // T0: B fails. The old configuration is protected by replication set
    // {A,W1}, while the new configuration is protected by {A,W2}.
    let requests = degrade_after_b_failure(&mut node);
    let w1_request = message_to(&requests, W1);
    let w2_request = message_to(&requests, W2);

    let expected_w1: HashSet<u64> = w1_request
        .replication_set_outgoing
        .iter()
        .copied()
        .collect();
    let expected_w2: HashSet<u64> = w2_request
        .replication_set_incoming
        .iter()
        .copied()
        .collect();
    assert_ne!(expected_w1, expected_w2);

    // T1: The leader sends each witness a real first-write message. Each message
    // contains both incoming and outgoing replication-set fields.
    let mut w1 = Witness::new(W1);
    let mut w2 = Witness::new(W2);
    assert!(matches!(
        w1.process(&w1_request),
        Some(WitnessResponse::Persist(_))
    ));
    assert!(matches!(
        w2.process(&w2_request),
        Some(WitnessResponse::Persist(_))
    ));

    // T2: Witness::process merges both fields. W1 and W2 therefore persist the
    // union instead of the configuration-specific voting evidence each one is
    // supposed to validate. This test proves the scope mixing; an end-to-end
    // stale-candidate election test is still required to prove the final safety
    // consequence for a particular membership-change topology.
    assert_eq!(
        w1.replication_set, expected_w1,
        "W1 accepted nodes belonging only to the new configuration"
    );
    assert_eq!(
        w2.replication_set, expected_w2,
        "W2 accepted nodes belonging only to the old configuration"
    );
}
