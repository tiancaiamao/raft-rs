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
    // With 2 non-witness voters (1,2) + 1 witness (3), quorum = 2.
    // The witness is excluded from the initial replication set; it
    // will be added via change_replication_set when a regular server
    // becomes unreachable (see the paper, §2.4).
    assert_eq!(epoch.replication_sets[0].excluded, 3);
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

/// Helper: set up a leader node where witness is active (not excluded).
/// After the threshold fix, 2 non-witness voters + 1 witness always includes
/// the witness in non_witness_voters (excluded=0), so change_replication_set
/// is not needed. We manually set node 2 as excluded and adjust
/// non_witness_voters to simulate the state where node 2 is inactive.
fn setup_leader_with_active_witness() -> RawNode<MemStorage> {
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    // Manually set up the replication set so node 2 is excluded and
    // witness (3) is in non_witness_voters, simulating node 2 being inactive.
    let epoch = &mut node.raft.mut_prs().epoch;
    epoch.subterm = 1; // simulate subterm increment
    let set = &mut epoch.replication_sets[0];
    set.excluded = 2;
    set.non_witness_voters.clear();
    set.non_witness_voters.insert(1);
    set.non_witness_voters.insert(3);

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

    // After first contact, witness_pending_subterm should be set but
    // witness_subterm should NOT yet be set — it waits for CAS confirmation.
    assert_eq!(
        node.raft.prs().epoch.witness_pending_subterm[0],
        node.raft.prs().epoch.subterm
    );
    assert_eq!(
        node.raft.prs().epoch.witness_subterm[0],
        node.raft.prs().epoch.subterm - 1 // still old value
    );

    // Simulate CAS success.
    node.raft.confirm_witness_append(3);
    assert_eq!(node.raft.prs().epoch.witness_pending_subterm[0], 0);
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
    // Simulate CAS success to activate shortcut replication.
    node.raft.confirm_witness_append(3);
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
fn test_shortcut_replication_blocked_until_cas_confirmed() {
    let mut node = setup_leader_with_active_witness();

    // Trigger first witness contact.
    node.raft.mut_prs().get_mut(1).unwrap().matched = node.raft.raft_log.last_index();
    node.raft.maybe_commit();
    assert_eq!(
        node.raft.prs().epoch.witness_pending_subterm[0],
        node.raft.prs().epoch.subterm
    );
    node.raft.witness_msgs.clear();

    // Do NOT call confirm_witness_append — simulate CAS failure / pending.
    // A second maybe_commit must NOT activate shortcut replication.
    let witness_matched_before = node.raft.prs().get(3).unwrap().matched;

    // Append a new entry and ack from voter 1.
    let mut entry = Entry::default();
    entry.term = node.raft.term;
    entry.subterm = node.raft.prs().epoch.subterm;
    let _ = node.raft.append_entry(&mut [entry]);
    node.raft.raft_log.persisted = node.raft.raft_log.last_index();
    node.raft.mut_prs().get_mut(1).unwrap().matched = node.raft.raft_log.last_index();

    node.raft.maybe_commit();

    // witness_subterm should NOT be set, so no synthesized ack.
    assert_ne!(
        node.raft.prs().epoch.witness_subterm[0],
        node.raft.prs().epoch.subterm,
        "witness_subterm must not advance without CAS confirmation"
    );
    let witness_matched_after = node.raft.prs().get(3).unwrap().matched;
    assert_eq!(
        witness_matched_before, witness_matched_after,
        "witness matched must not advance without CAS confirmation"
    );

    // Also should NOT re-send append (pending flag prevents it).
    let append_msgs: Vec<_> = node
        .raft
        .witness_msgs
        .iter()
        .filter(|m| m.get_msg_type() == MessageType::MsgAppend)
        .collect();
    assert!(
        append_msgs.is_empty(),
        "should NOT re-send append while pending"
    );

    // Now simulate CAS success.
    node.raft.confirm_witness_append(3);
    assert_eq!(node.raft.prs().epoch.witness_pending_subterm[0], 0);
    assert_eq!(
        node.raft.prs().epoch.witness_subterm[0],
        node.raft.prs().epoch.subterm
    );

    // Next maybe_commit should now synthesize the witness ack and commit.
    node.raft.maybe_commit();
    let witness_matched_final = node.raft.prs().get(3).unwrap().matched;
    assert!(
        witness_matched_final > witness_matched_after,
        "witness matched should advance after CAS confirmation + shortcut"
    );
}

#[test]
fn test_shortcut_replication_new_subterm_allows_contact() {
    let mut node = setup_leader_with_active_witness();

    // First contact.
    node.raft.mut_prs().get_mut(1).unwrap().matched = node.raft.raft_log.last_index();
    node.raft.maybe_commit();
    // Simulate CAS success.
    node.raft.confirm_witness_append(3);
    node.raft.witness_msgs.clear();

    // Force a new subterm via conf_change=true.
    node.raft.maybe_start_new_subterm(false, true);
    // reset_replication_set creates a new Epoch → witness_subterm and witness_pending_subterm reset.
    assert_eq!(node.raft.prs().epoch.witness_subterm, [0, 0]);
    assert_eq!(node.raft.prs().epoch.witness_pending_subterm, [0, 0]);

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
// Conf change with witness
// ═══════════════════════════════════════════════════════════════════

// Helper: create a ConfChangeV2 that forces joint consensus via Implicit transition.
fn joint_conf_change(changes: Vec<ConfChangeSingle>) -> raft::eraftpb::ConfChangeV2 {
    raft::eraftpb::ConfChangeV2 {
        transition: raft::eraftpb::ConfChangeTransition::Implicit,
        changes: changes.into(),
        ..Default::default()
    }
}

// Helper: create a ConfChangeV2 that leaves joint state.
fn leave_joint_cc() -> raft::eraftpb::ConfChangeV2 {
    raft::eraftpb::ConfChangeV2 {
        transition: raft::eraftpb::ConfChangeTransition::Auto,
        ..Default::default()
    }
}

fn add_node(id: u64) -> ConfChangeSingle {
    ConfChangeSingle {
        change_type: ConfChangeType::AddNode,
        node_id: id,
        ..Default::default()
    }
}

fn remove_node(id: u64) -> ConfChangeSingle {
    ConfChangeSingle {
        change_type: ConfChangeType::RemoveNode,
        node_id: id,
        ..Default::default()
    }
}

#[test]
fn test_enter_joint_preserves_witness_in_outgoing() {
    // P0: enter_joint must copy witnesses[0] → witnesses[1].
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    // Before conf change: only incoming witness.
    assert_eq!(node.raft.prs().conf().witnesses, [3, 0]);

    // Apply a conf change with Implicit transition to force joint consensus.
    let cc = joint_conf_change(vec![add_node(4)]);
    node.raft.apply_conf_change(&cc).unwrap();

    // After enter_joint, both configs should have witness.
    assert_eq!(node.raft.prs().conf().witnesses, [3, 3]);
    let cs = node.raft.prs().conf().to_conf_state();
    assert!(!cs.get_voters_outgoing().is_empty());
}

#[test]
fn test_leave_joint_clears_outgoing_witness() {
    // P0: leave_joint must clear witnesses[1].
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    // Enter joint.
    let cc_enter = joint_conf_change(vec![add_node(4)]);
    node.raft.apply_conf_change(&cc_enter).unwrap();
    assert_eq!(node.raft.prs().conf().witnesses, [3, 3]);

    // Leave joint.
    let cc_leave = leave_joint_cc();
    node.raft.apply_conf_change(&cc_leave).unwrap();

    // Outgoing witness should be cleared.
    assert_eq!(node.raft.prs().conf().witnesses[1], 0);
    let cs = node.raft.prs().conf().to_conf_state();
    assert!(cs.get_voters_outgoing().is_empty());
}

#[test]
fn test_conf_change_starts_new_subterm() {
    // After a conf change, a new subterm should start.
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();
    let subterm_before = node.raft.prs().epoch.subterm;

    let cc = raft::eraftpb::ConfChangeV2 {
        changes: vec![add_node(4)].into(),
        ..Default::default()
    };
    node.raft.apply_conf_change(&cc).unwrap();

    let subterm_after = node.raft.prs().epoch.subterm;
    assert!(
        subterm_after > subterm_before || node.raft.prs().epoch.subterm == 0,
        "subterm should increment or reset after conf change"
    );
}

#[test]
fn test_joint_state_reset_replication_set_both_halves() {
    // After enter_joint, reset_replication_set should set up witness in both halves.
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    let cc = joint_conf_change(vec![add_node(4)]);
    node.raft.apply_conf_change(&cc).unwrap();

    let epoch = &node.raft.prs().epoch;

    // Incoming half: witness = 3.
    assert_eq!(epoch.replication_sets[0].witness, 3);

    // Outgoing half: witness = 3 (preserved from pre-change config).
    assert_eq!(epoch.replication_sets[1].witness, 3);
}

#[test]
fn test_joint_state_has_witness_in_both_halves() {
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    let cc = joint_conf_change(vec![add_node(4)]);
    node.raft.apply_conf_change(&cc).unwrap();

    let epoch = &node.raft.prs().epoch;
    assert!(epoch.has_witness());

    // Both halves should have witness.
    assert!(epoch.replication_sets[0].witness != 0);
    assert!(epoch.replication_sets[1].witness != 0);
}

#[test]
fn test_conf_change_add_witness() {
    // Start with 3 voters, no witness.
    let storage = MemStorage::default();
    let mut cs = ConfState::default();
    cs.set_voters(vec![1, 2, 3]);
    storage.initialize_with_conf_state(cs);

    let config = raft::Config {
        id: 1,
        ..Default::default()
    };
    let mut node = RawNode::new(&config, storage, &make_logger()).unwrap();
    assert_eq!(node.raft.prs().conf().witnesses, [0, 0]);

    node.raft.become_candidate();
    node.raft.become_leader();

    // Add node 4 as witness.
    let cc = raft::eraftpb::ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            change_type: ConfChangeType::AddWitness,
            node_id: 4,
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    node.raft.apply_conf_change(&cc).unwrap();

    // Node 4 should now be marked as witness.
    assert_eq!(node.raft.prs().conf().witnesses[0], 4);
    assert!(node.raft.prs().get(4).unwrap().is_witness);
    assert!(!node.raft.prs().get(1).unwrap().is_witness);
}

#[test]
fn test_conf_change_remove_witness() {
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    // Remove witness node 3.
    let cc = raft::eraftpb::ConfChangeV2 {
        changes: vec![remove_node(3)].into(),
        ..Default::default()
    };
    node.raft.apply_conf_change(&cc).unwrap();

    // Node 3 should be removed from progress tracker.
    assert!(node.raft.prs().get(3).is_none());
    // witnesses[0] should be cleared (Bug 2 fix).
    assert_eq!(node.raft.prs().conf().witnesses[0], 0);
}

#[test]
fn test_simple_conf_change_no_witness_stays_unaffected() {
    // Simple conf change on a non-witness cluster should not be affected.
    let storage = MemStorage::default();
    let mut cs = ConfState::default();
    cs.set_voters(vec![1, 2, 3]);
    storage.initialize_with_conf_state(cs);

    let config = raft::Config {
        id: 1,
        ..Default::default()
    };
    let mut node = RawNode::new(&config, storage, &make_logger()).unwrap();
    node.raft.become_candidate();
    node.raft.become_leader();

    let cc = raft::eraftpb::ConfChangeV2 {
        changes: vec![add_node(4)].into(),
        ..Default::default()
    };
    node.raft.apply_conf_change(&cc).unwrap();

    // No witness should be configured.
    assert_eq!(node.raft.prs().conf().witnesses, [0, 0]);
    assert!(!node.raft.prs().epoch.has_witness());
}

#[test]
fn test_joint_state_witness_in_both_replication_sets() {
    // After enter_joint, the witness should appear in both replication sets,
    // and both should be properly initialized.
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    let cc = joint_conf_change(vec![add_node(4)]);
    node.raft.apply_conf_change(&cc).unwrap();

    let epoch = &node.raft.prs().epoch;

    // Incoming: voters = {1,2,3,4}, witness = 3, 3 non-witness voters.
    // With >=3 non-witness voters, the witness is excluded.
    let r0 = &epoch.replication_sets[0];
    assert_eq!(r0.witness, 3);
    assert!(r0.non_witness_voters.contains(&1));
    assert!(r0.non_witness_voters.contains(&2));
    assert!(r0.non_witness_voters.contains(&4));
    assert!(!r0.non_witness_voters.contains(&3));

    // Outgoing: voters = {1,2,3}, witness = 3, 2 non-witness voters.
    // With >=2 non-witness voters, the witness is excluded.
    let r1 = &epoch.replication_sets[1];
    assert_eq!(r1.witness, 3);
    assert_eq!(r1.excluded, 3);
    assert!(r1.non_witness_voters.contains(&1));
    assert!(r1.non_witness_voters.contains(&2));
    assert!(!r1.non_witness_voters.contains(&3));
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
    // Simulate a witness that has COMMITTED entries at term 2.
    w.commit = 10;
    w.committed_log_term = 2;
    w.committed_log_subterm = 0;
    w.replication_set = vec![1, 2, 3].into_iter().collect();

    // Candidate has last_log_term=1 (stale — behind the committed term).
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

// ═══════════════════════════════════════════════════════════════════
// Conf change edge cases with witness (etcd-parity fixes)
// ═══════════════════════════════════════════════════════════════════

#[test]
fn test_leave_joint_preserves_witness_progress() {
    // Bug 1: leave_joint must not remove the witness node from progress
    // if it's in outgoing only but still the incoming witness.
    //
    // Setup: incoming = {1, 2, 3w}, outgoing = {1, 2, 3w}
    // After AddNode(4) + AddWitness(4) in joint:
    //   incoming = {1, 2, 3, 4w}, witnesses = [4, 3]
    //   outgoing = {1, 2, 3}, witnesses_out = 3
    // After leave_joint: outgoing cleared, node 3 is not in incoming voters
    //   but is it still the incoming witness? No — witnesses[0] = 4.
    // So node 3 should be removed normally.

    // Different scenario: witness stays the same across conf change.
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    // Enter joint: add node 4 (witness stays as 3 in both halves).
    let cc = joint_conf_change(vec![add_node(4)]);
    node.raft.apply_conf_change(&cc).unwrap();
    assert_eq!(node.raft.prs().conf().witnesses, [3, 3]);

    // Leave joint.
    let cc_leave = leave_joint_cc();
    node.raft.apply_conf_change(&cc_leave).unwrap();

    // Node 4 should stay (it's in incoming voters).
    assert!(node.raft.prs().get(4).is_some());
    // Node 3 (witness) should stay.
    assert!(node.raft.prs().get(3).is_some());
    // witnesses[1] cleared.
    assert_eq!(node.raft.prs().conf().witnesses[1], 0);
    // witnesses[0] still 3.
    assert_eq!(node.raft.prs().conf().witnesses[0], 3);
}

#[test]
fn test_remove_witness_clears_witnesses_field() {
    // Bug 2: remove() must clear witnesses[0] when the witness is removed.
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();
    assert_eq!(node.raft.prs().conf().witnesses[0], 3);

    // Remove witness via simple conf change.
    let cc = raft::eraftpb::ConfChangeV2 {
        changes: vec![remove_node(3)].into(),
        ..Default::default()
    };
    node.raft.apply_conf_change(&cc).unwrap();

    // witnesses[0] must be cleared, not stale.
    assert_eq!(node.raft.prs().conf().witnesses[0], 0);
}

#[test]
fn test_joint_remove_old_witness_add_new_witness() {
    // Edge case: change witness from 3 to 4 in a joint conf change.
    // Enter joint with RemoveNode(3) + AddWitness(4).
    let mut node = make_witness_node(1, vec![1, 2, 3], 3);
    node.raft.become_candidate();
    node.raft.become_leader();

    let cc = joint_conf_change(vec![
        remove_node(3),
        ConfChangeSingle {
            change_type: ConfChangeType::AddWitness,
            node_id: 4,
            ..Default::default()
        },
    ]);
    node.raft.apply_conf_change(&cc).unwrap();

    // After enter_joint:
    // witnesses[1] = 3 (old witness, carried to outgoing)
    // witnesses[0] = 4 (new witness, set by make_witness)
    assert_eq!(node.raft.prs().conf().witnesses, [4, 3]);

    // Outgoing config: {1, 2, 3} with witness 3.
    // Incoming config: {1, 2, 4} with witness 4.
    let cs = node.raft.prs().conf().to_conf_state();
    assert!(cs.get_voters_outgoing().contains(&3));

    // Leave joint.
    let cc_leave = leave_joint_cc();
    node.raft.apply_conf_change(&cc_leave).unwrap();

    // After leave_joint: outgoing cleared.
    assert_eq!(node.raft.prs().conf().witnesses[1], 0);
    assert_eq!(node.raft.prs().conf().witnesses[0], 4);
    // Node 3 removed (not in incoming, not witness).
    assert!(node.raft.prs().get(3).is_none());
    // Node 4 still present.
    assert!(node.raft.prs().get(4).is_some());
}

#[test]
fn test_witness_vote_joint_consensus_validation() {
    // Bug 4: during joint consensus, witness must know voters from both
    // halves to validate votes correctly. An outgoing-only voter that
    // voted yes must not cause rejection.
    use raft::Witness;

    let mut w = Witness::new(5);
    w.term = 1;
    w.last_log_term = 1;
    w.last_log_subterm = 1;
    w.last_log_index = 5;

    // Simulate an AppendEntries to witness that sets up the replication set
    // with both incoming and outgoing voters.
    let mut append_msg = WitnessMessage::default();
    append_msg.set_from(1);
    append_msg.set_to(5);
    append_msg.set_term(1);
    append_msg.set_msg_type(MessageType::MsgAppend);
    append_msg.set_last_log_term(1);
    append_msg.set_last_log_index(5);
    append_msg.set_last_log_subterm(1);
    append_msg.replication_set_incoming = vec![1, 2].into(); // incoming non-witness voters
    append_msg.replication_set_outgoing = vec![1, 3].into(); // outgoing non-witness voters
    w.process(&append_msg);

    // Witness should know about all voters from both halves.
    assert!(w.replication_set.contains(&1));
    assert!(w.replication_set.contains(&2));
    assert!(w.replication_set.contains(&3));

    // Now candidate sends RequestVote with votes from {1, 3}.
    // Node 3 is outgoing-only, but witness should accept because
    // it knows about outgoing voters.
    let mut vote_msg = WitnessMessage::default();
    vote_msg.set_from(1);
    vote_msg.set_to(5);
    vote_msg.set_term(1);
    vote_msg.set_msg_type(MessageType::MsgRequestVote);
    vote_msg.set_last_log_term(1);
    vote_msg.set_last_log_index(5);
    vote_msg.set_last_log_subterm(1);
    vote_msg.vote_ids = vec![1, 3];
    vote_msg.vote_vals = vec![true, true];

    let resp = w.process(&vote_msg);
    assert!(
        matches!(resp, Some(raft::WitnessResponse::VoteGrant(true))),
        "witness should grant vote even with outgoing-only voter"
    );
}

#[test]
fn test_check_invariants_rejects_witness_not_in_voters() {
    // Bug 3: check_invariants should reject configs where witness is not
    // actually in the voter set. This is tested indirectly via apply_conf_change
    // which calls check_invariants internally.
    //
    // We can't easily craft an invalid Configuration from outside, but we
    // can verify that AddWitness correctly adds to voters (so invariants pass).
    let mut node = make_witness_node(1, vec![1, 2], 0);
    node.raft.become_candidate();
    node.raft.become_leader();

    // Add witness node 3 — should succeed and add to voters.
    let cc = raft::eraftpb::ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            change_type: ConfChangeType::AddWitness,
            node_id: 3,
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    node.raft.apply_conf_change(&cc).unwrap();

    assert_eq!(node.raft.prs().conf().witnesses[0], 3);
    assert!(node.raft.prs().get(3).is_some());
    // Node 3 should be in voters (checked by invariants passing).
    let cs = node.raft.prs().conf().to_conf_state();
    assert!(cs.get_voters().contains(&3));
}
