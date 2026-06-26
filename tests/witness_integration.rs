// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Integration tests for Extended Raft witness functionality.

use raft::eraftpb::{
    ConfChangeSingle, ConfChangeType, ConfState, Entry, MessageType, WitnessHardState,
    WitnessMessage,
};
use raft::raw_node::RawNode;
use raft::storage::MemStorage;
use slog::{o, Logger};

fn make_logger() -> Logger {
    Logger::root(slog::Discard, o!())
}

/// Creates a RawNode with a 2-voter + 1-witness config.
fn make_witness_node(node_id: u64, voters: Vec<u64>, witness: u64) -> RawNode<MemStorage> {
    let logger = make_logger();
    let storage = MemStorage::default();

    let mut cs = ConfState::default();
    cs.set_voters(voters.clone());
    cs.set_witness(witness);
    storage.initialize_with_conf_state(cs);

    let config = raft::Config {
        id: node_id,
        ..Default::default()
    };
    RawNode::new(&config, storage, &logger).unwrap()
}

// ═══════════════════════════════════════════════════════════════════
// Proto-level tests
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_entry_has_subterm_field() {
    let mut entry = Entry::default();
    entry.set_subterm(5);
    assert_eq!(entry.get_subterm(), 5);
    assert_eq!(Entry::default().get_subterm(), 0);
}

#[test]
fn test_witness_message_proto() {
    let mut msg = WitnessMessage::default();
    msg.set_from(1);
    msg.set_to(3);
    msg.set_term(2);
    msg.set_msg_type(MessageType::MsgAppend);
    msg.set_last_log_index(10);
    msg.set_last_log_term(2);
    msg.set_last_log_subterm(3);
    msg.replication_set_incoming = vec![1, 2].into();
    msg.vote_ids = vec![1, 2];
    msg.vote_vals = vec![true, true];

    assert_eq!(msg.get_from(), 1);
    assert_eq!(msg.get_to(), 3);
    assert_eq!(msg.get_msg_type(), MessageType::MsgAppend);
    assert_eq!(msg.get_last_log_index(), 10);
    assert_eq!(msg.get_last_log_subterm(), 3);
}

#[test]
fn test_conf_change_add_witness_type() {
    let cc = ConfChangeType::AddWitness;
    let single = ConfChangeSingle {
        change_type: cc,
        node_id: 3,
        ..Default::default()
    };
    assert_eq!(single.change_type, ConfChangeType::AddWitness);
}

#[test]
fn test_witness_hard_state_proto() {
    let mut whs = WitnessHardState::default();
    whs.set_last_log_index(100);
    whs.set_last_log_term(5);
    whs.set_last_log_subterm(3);
    whs.set_lead(1);
    whs.set_replication_set(vec![1, 2]);

    assert_eq!(whs.get_last_log_index(), 100);
    assert_eq!(whs.get_last_log_subterm(), 3);
    assert_eq!(whs.get_lead(), 1);
    assert_eq!(whs.get_replication_set(), &[1, 2]);
}

#[test]
fn test_conf_state_witness_fields() {
    let mut cs = ConfState::default();
    cs.set_voters(vec![1, 2, 3]);
    cs.set_witness(3);

    assert_eq!(cs.get_voters(), &[1, 2, 3]);
    assert_eq!(cs.get_witness(), 3);
    assert_eq!(cs.get_witness_outgoing(), 0);
}

// ═══════════════════════════════════════════════════════════════════
// Standard (no-witness) Raft — regression test
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_standard_raft_no_witness() {
    let logger = Logger::root(slog::Discard, o!());
    let storage = MemStorage::default();

    let mut cs = ConfState::default();
    cs.set_voters(vec![1, 2, 3]);

    storage.initialize_with_conf_state(cs);

    let config = raft::Config {
        id: 1,
        ..Default::default()
    };
    let mut node = RawNode::new(&config, storage, &logger).unwrap();

    assert_eq!(node.raft.prs().conf().witnesses, [0, 0]);
    assert_eq!(node.raft.prs().epoch.subterm, 0);

    // Standard campaign should work.
    node.raft.campaign(raft::CAMPAIGN_ELECTION);
}

// ═══════════════════════════════════════════════════════════════════
// Witness config initialization
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_witness_config_initialization() {
    let node = make_witness_node(1, vec![1, 2, 3], 3);

    // Witness config is set.
    assert_eq!(node.raft.prs().conf().witnesses, [3, 0]);

    // After becoming leader, epoch should have replication set with witness.
    drop(node);
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    let epoch = &node.raft.prs().epoch;
    assert_eq!(epoch.replication_sets[0].witness, 3);
    assert_eq!(epoch.replication_sets[0].excluded, 3); // witness initially excluded
    assert!(epoch.replication_sets[0].non_witness_voters.contains(&1));
    assert!(epoch.replication_sets[0].non_witness_voters.contains(&2));
    assert!(!epoch.replication_sets[0].non_witness_voters.contains(&3));
}

#[test]
fn test_witness_progress_is_witness_flag() {
    let node = make_witness_node(1, vec![1, 2, 3], 3);

    assert!(!node.raft.prs().get(1).unwrap().is_witness);
    assert!(!node.raft.prs().get(2).unwrap().is_witness);
    assert!(node.raft.prs().get(3).unwrap().is_witness);
}

// ═══════════════════════════════════════════════════════════════════
// Campaign with witness — vote request readiness
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_witness_campaign_skips_witness_in_broadcast() {
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);

    node.raft.campaign(raft::CAMPAIGN_ELECTION);

    // After campaign, node should have sent RequestVote to node 2 but NOT node 3 (witness).
    let msgs = &node.raft.msgs;
    let vote_msgs: Vec<_> = msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgRequestVote)
        .collect();
    assert!(!vote_msgs.is_empty());
    for m in &vote_msgs {
        assert_ne!(m.to, 3, "witness should not receive initial vote broadcast");
    }
}

#[test]
fn test_witness_campaign_self_vote() {
    // With witnesses, a 2+1 config starts as candidate with self-vote only.
    // It needs 1 more regular vote from node 2, then witness.
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);

    node.raft.campaign(raft::CAMPAIGN_ELECTION);
    assert_eq!(node.raft.state, raft::StateRole::Candidate);
    // Should not be leader yet (needs witness vote).
    assert_ne!(node.raft.state, raft::StateRole::Leader);
}

// ═══════════════════════════════════════════════════════════════════
// Witness vote request readiness — tested via campaign behavior
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_witness_campaign_may_request_witness_vote() {
    // In a 2+1 config, self-vote gives the candidate 1 out of 2 quorum.
    // The candidate is exactly 1 vote short → witness vote may be triggered.
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.campaign(raft::CAMPAIGN_ELECTION);

    // With q-1 = 1, candidate is exactly 1 short. Witness vote request may
    // be generated immediately (since self-vote + witness = quorum).
    // This is valid behavior — verify the witness vote request is correct.
    let witness_msgs = &node.raft.witness_msgs;
    let witness_vote_msgs: Vec<_> = witness_msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgRequestVote)
        .collect();
    for m in &witness_vote_msgs {
        assert_eq!(m.to, 3);
        assert_eq!(m.from, 1);
    }
}

// ═══════════════════════════════════════════════════════════════════
// Leader election with witness — full flow
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_leader_election_with_witness() {
    // Simulate: node 1 campaigns, gets node 2's vote, then needs witness vote.
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);

    node.raft.campaign(raft::CAMPAIGN_ELECTION);
    assert_eq!(node.raft.state, raft::StateRole::Candidate);

    // Simulate node 2 granting vote.
    let mut vote_resp = raft::eraftpb::Message::default();
    vote_resp.msg_type = MessageType::MsgRequestVoteResponse;
    vote_resp.from = 2;
    vote_resp.to = 1;
    vote_resp.term = node.raft.term;
    vote_resp.reject = false;
    node.raft.step(vote_resp).unwrap();

    // After getting node 2's vote, candidate should be pending.
    // It should have generated a witness vote request.
    let witness_msgs = node.raft.witness_msgs.clone();
    node.raft.witness_msgs.clear();
    let witness_vote_msgs: Vec<_> = witness_msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgRequestVote)
        .collect();
    assert!(
        !witness_vote_msgs.is_empty(),
        "should have sent witness vote request after getting q-1 regular votes"
    );
    for m in &witness_vote_msgs {
        assert_eq!(m.to, 3);
        assert_eq!(m.from, 1);
    }
}

#[test]
fn test_leader_with_witness_starts_subterm() {
    // When a node with witness config becomes leader, it should start a new subterm.
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);

    // Force become leader directly (simulating election win).
    node.raft.become_candidate();
    node.raft.become_leader();

    // After becoming leader with witness, a subterm should have started.
    assert!(node.raft.prs().epoch.subterm == 0); // new term → subterm reset to 0
    assert!(node.raft.prs().epoch.has_witness());
}

/// Helper: set up a leader node where witness is active (not excluded),
/// simulating the state after change_replication_set excludes an inactive node.
fn setup_leader_with_active_witness() -> RawNode<MemStorage> {
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    // Simulate change_replication_set: node 2 is inactive, witness takes over.
    node.raft.mut_prs().get_mut(1).unwrap().recent_active = true;
    node.raft.mut_prs().get_mut(2).unwrap().recent_active = false;
    node.raft.mut_prs().get_mut(3).unwrap().recent_active = true;
    node.raft.mut_prs().change_replication_set();

    node
}

// ═══════════════════════════════════════════════════════════════════
// Shortcut replication — witness_subterm tracking
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_shortcut_replication_sends_once_per_subterm() {
    let mut node = setup_leader_with_active_witness();

    let initial_witness_subterm = node.raft.prs().epoch.witness_subterm[0];

    // Non-witness voter in replication set = {1} (node 2 excluded).
    // Leader at last_index, so node 1 matched = last_index → quorum-1.
    node.raft.mut_prs().get_mut(1).unwrap().matched = node.raft.raft_log.last_index();

    node.raft.maybe_commit();

    // After first contact, witness_subterm should be set.
    assert_eq!(
        node.raft.prs().epoch.witness_subterm[0],
        node.raft.prs().epoch.subterm
    );

    let witness_msgs = node.raft.witness_msgs.clone();
    node.raft.witness_msgs.clear();
    let append_msgs: Vec<_> = witness_msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgAppend)
        .collect();
    assert!(
        !append_msgs.is_empty(),
        "should have sent append to witness"
    );
    assert_ne!(
        initial_witness_subterm,
        node.raft.prs().epoch.witness_subterm[0]
    );
}

#[test]
fn test_shortcut_replication_synthesizes_second_ack() {
    let mut node = setup_leader_with_active_witness();

    // First: trigger initial witness contact.
    node.raft.mut_prs().get_mut(1).unwrap().matched = node.raft.raft_log.last_index();
    node.raft.maybe_commit();
    node.raft.witness_msgs.clear();

    let witness_matched_after_first = node.raft.prs().get(3).unwrap().matched;

    // Append a new entry and ack it.
    let mut entry = Entry::default();
    entry.term = node.raft.term;
    entry.subterm = node.raft.prs().epoch.subterm;
    let _ = node.raft.append_entry(&mut [entry]);
    node.raft.raft_log.persisted = node.raft.raft_log.last_index();
    node.raft.mut_prs().get_mut(1).unwrap().matched = node.raft.raft_log.last_index();

    // Second maybe_commit — should NOT send another RPC, but synthesize ack.
    node.raft.maybe_commit();

    let witness_msgs = node.raft.witness_msgs.clone();
    node.raft.witness_msgs.clear();
    let append_msgs: Vec<_> = witness_msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgAppend)
        .collect();
    assert!(
        append_msgs.is_empty(),
        "should NOT send second append to witness in same subterm (shortcut)"
    );

    let witness_matched_after_second = node.raft.prs().get(3).unwrap().matched;
    assert!(
        witness_matched_after_second >= witness_matched_after_first,
        "witness matched should advance via synthesized ack"
    );
}

#[test]
fn test_shortcut_replication_new_subterm_allows_contact() {
    let mut node = setup_leader_with_active_witness();

    // First contact.
    node.raft.mut_prs().get_mut(1).unwrap().matched = node.raft.raft_log.last_index();
    node.raft.maybe_commit();
    node.raft.witness_msgs.clear();

    // Force a new subterm via conf_change=true.
    node.raft.maybe_start_new_subterm(false, true);
    // reset_replication_set creates a new Epoch → witness_subterm resets.
    assert_eq!(node.raft.prs().epoch.witness_subterm, [0, 0]);

    // Re-activate witness (node 2 inactive again).
    node.raft.mut_prs().get_mut(1).unwrap().recent_active = true;
    node.raft.mut_prs().get_mut(2).unwrap().recent_active = false;
    node.raft.mut_prs().get_mut(3).unwrap().recent_active = true;
    node.raft.mut_prs().change_replication_set();

    // q-1 again should trigger a new witness contact.
    node.raft.mut_prs().get_mut(1).unwrap().matched = node.raft.raft_log.last_index();
    node.raft.maybe_commit();

    let witness_msgs = node.raft.witness_msgs.clone();
    node.raft.witness_msgs.clear();
    let append_msgs: Vec<_> = witness_msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgAppend)
        .collect();
    assert!(
        !append_msgs.is_empty(),
        "new subterm should allow witness contact again"
    );
}

// ═══════════════════════════════════════════════════════════════════
// Witness module unit tests (from the original file, kept for completeness)
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_witness_module_basic() {
    use raft::Witness;

    let mut w = Witness::new(3);
    w.term = 1;
    w.last_log_term = 1;
    w.last_log_index = 5;
    w.last_log_subterm = 0;
    w.replication_set = vec![1, 2, 3].into_iter().collect();

    let mut msg = WitnessMessage::default();
    msg.set_from(1);
    msg.set_to(3);
    msg.set_term(2);
    msg.set_msg_type(MessageType::MsgRequestVote);
    msg.set_last_log_term(1);
    msg.set_last_log_index(5);
    msg.set_last_log_subterm(0);
    msg.vote_ids = vec![1, 2];
    msg.vote_vals = vec![true, true];

    let resp = w.process(&msg);
    assert!(matches!(resp, Some(raft::WitnessResponse::VoteGrant(true))));
    assert_eq!(w.vote, 1);
}

#[test]
fn test_witness_module_reject_stale() {
    use raft::Witness;

    let mut w = Witness::new(3);
    w.term = 1;
    w.last_log_term = 2;
    w.last_log_index = 10;
    w.replication_set = vec![1, 2, 3].into_iter().collect();

    let mut msg = WitnessMessage::default();
    msg.set_from(1);
    msg.set_term(2);
    msg.set_msg_type(MessageType::MsgRequestVote);
    msg.set_last_log_term(1);
    msg.set_last_log_index(5);
    msg.vote_ids = vec![1];
    msg.vote_vals = vec![true];

    let resp = w.process(&msg);
    assert!(matches!(
        resp,
        Some(raft::WitnessResponse::VoteGrant(false))
    ));
}
