// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Reproduction harness for the "fake ack" incident (matched[154] == 42 without
//! 154 ever having entry 42@term10).
//!
//! Incident facts (region 151, 2026-08-06):
//! - 154 was the (9,12) leader: log[42] = (42, t9, s12), committed = 41.
//!   1594 had log 1..41 (t9), committed = 41 — it never received 154's
//!   subterm-12 append.
//! - Witness 1550 voted for 1594 (its own log was ≤ (t9,s9), strictly behind
//!   1594's 41 entries, so the vote was log-safe).
//! - 1594 became term-10 leader, appended noop 42@(t10,s0), and sent its first
//!   append to 154 (arrived 21:52:05.292).
//! - 154 "became follower at term 10" but produced NO "found conflict" log and
//!   its log[42] was still (42,t9) at 21:52:10.410. Yet its delayed append
//!   response set matched[154] = 42 on 1594 (~10.12), committing 42(t10).
//!
//! This test pins down what raft-rs itself does in that exact configuration:
//!   A) processing a real (41, t9, [42(t10)]) append → conflict + overwrite +
//!      ack(42). The incident shows neither the conflict log nor the overwrite,
//!      so the delivered message cannot have been this one.
//!   B) processing an empty append anchored at (42, t9) → silent ack(42) with
//!      no log change — the ONLY raft-rs path consistent with the incident.
//!
//! Run with: cargo test --test fake_ack_repro

#![allow(clippy::useless_conversion)]

use raft::eraftpb::{ConfState, Entry, HardState, Message, MessageType};
use raft::raw_node::RawNode;
use raft::storage::MemStorage;
use slog::{o, Logger};

const LEADER_OLD: u64 = 154; // old (9,12) leader, log ends at (42,t9)
const CANDIDATE: u64 = 1594; // term-10 leader elect via witness vote, log 1..41
const WITNESS: u64 = 1550;

fn logger() -> Logger {
    Logger::root(slog::Discard, o!())
}

fn make_node(id: u64, entries: Vec<Entry>, hs: HardState) -> RawNode<MemStorage> {
    let storage = MemStorage::default();
    let mut cs = ConfState::default();
    cs.set_voters(vec![LEADER_OLD, CANDIDATE, WITNESS]);
    cs.set_witness(WITNESS);
    storage.initialize_with_conf_state(cs);
    storage.wl().append(&entries).unwrap();
    storage.wl().set_hardstate(hs);
    let config = raft::Config {
        id,
        election_tick: 10,
        heartbeat_tick: 2,
        check_quorum: true,
        ..Default::default()
    };
    RawNode::new(&config, storage, &logger()).unwrap()
}

fn entry(index: u64, term: u64) -> Entry {
    let mut e = Entry::default();
    e.index = index;
    e.term = term;
    e
}

/// Log of the old leader 154: entries 1..42 all in term 9, committed 41
/// (42 is the uncommitted subterm-12 write).
fn node154_log() -> Vec<Entry> {
    (1..=42).map(|i| entry(i, 9)).collect()
}

/// Log of candidate 1594: entries 1..41 in term 9, committed 41.
fn node1594_log() -> Vec<Entry> {
    (1..=41).map(|i| entry(i, 9)).collect()
}

fn drain_msgs(node: &mut RawNode<MemStorage>) -> Vec<Message> {
    std::mem::take(&mut node.raft.msgs)
}

fn append_to_1594(node: &mut RawNode<MemStorage>) -> Message {
    drain_msgs(node)
        .into_iter()
        .find(|m| m.get_msg_type() == MessageType::MsgAppend && m.to == LEADER_OLD)
        .expect("leader must send an append to 154")
}

#[test]
fn standard_append_with_noop_conflicts_and_overwrites_154_log() {
    // 154: follower at term 9 with log 1..42 (42=(42,t9)), committed 41.
    let mut n154 = make_node(LEADER_OLD, node154_log(), {
        let mut hs = HardState::default();
        hs.set_term(9);
        hs.set_vote(LEADER_OLD);
        hs.set_commit(41);
        hs
    });

    // 1594: candidate with log 1..41, term 9, committed 41.
    let mut n1594 = make_node(CANDIDATE, node1594_log(), {
        let mut hs = HardState::default();
        hs.set_term(9);
        hs.set_vote(CANDIDATE);
        hs.set_commit(41);
        hs
    });

    // Real election at term 10 won by the witness vote (exactly as the incident).
    n1594.campaign().unwrap(); // become_candidate at term 10, self-vote recorded
    let mut witness_vote = Message::default();
    witness_vote.set_msg_type(MessageType::MsgRequestVoteResponse);
    witness_vote.from = WITNESS;
    witness_vote.to = CANDIDATE;
    witness_vote.term = 10;
    witness_vote.reject = false;
    n1594.step(witness_vote).unwrap();
    assert_eq!(n1594.raft.state, raft::StateRole::Leader);

    // Inspect the exact append the new leader sends to 154.
    let append = append_to_1594(&mut n1594);
    assert_eq!(append.index, 41, "append anchored at next_idx-1");
    assert_eq!(append.log_term, 9, "anchor term = term(41)");
    assert_eq!(append.entries.len(), 1, "first append must carry the noop");
    assert_eq!(append.entries[0].index, 42);
    assert_eq!(append.entries[0].term, 10, "noop is (42,t10)");

    // Deliver it to 154 and observe the response.
    n154.step(append).unwrap();
    let resp = drain_msgs(&mut n154)
        .into_iter()
        .find(|m| m.get_msg_type() == MessageType::MsgAppendResponse)
        .expect("154 must answer the append");
    assert!(!resp.reject);
    assert_eq!(resp.index, 42, "154 acks index 42");
    assert_eq!(
        resp.log_term, 10,
        "ack reports the term 154 matched (42,t10)"
    );
    assert_eq!(
        n154.raft.raft_log.term(42).unwrap(),
        10,
        "conflicting (42,t9) MUST be overwritten by (42,t10)"
    );
    assert_eq!(n154.raft.raft_log.last_index(), 42);
    assert_eq!(n154.raft.raft_log.committed, 41);
    // The append response index 42 is genuine: 154 now HAS entry (42,t10).
    assert!(
        n154.raft.raft_log.match_term(resp.index, 10),
        "the ack must correspond to an entry 154 actually holds"
    );
}

#[test]
fn fake_ack_is_rejected_when_acked_term_mismatches_leaders_log() {
    // 1594: term-10 leader with log 1..41(t9) + noop 42(t10), committed 41.
    let mut n1594 = make_node(
        CANDIDATE,
        {
            let mut log = node1594_log();
            log.push(entry(42, 10));
            log
        },
        {
            let mut hs = HardState::default();
            hs.set_term(9);
            hs.set_vote(CANDIDATE);
            hs.set_commit(41);
            hs
        },
    );
    n1594.campaign().unwrap();
    let mut witness_vote = Message::default();
    witness_vote.set_msg_type(MessageType::MsgRequestVoteResponse);
    witness_vote.from = WITNESS;
    witness_vote.to = CANDIDATE;
    witness_vote.term = 10;
    witness_vote.reject = false;
    n1594.step(witness_vote).unwrap();
    assert_eq!(n1594.raft.state, raft::StateRole::Leader);
    // Clear the leader's initial append (the append is the message whose
    // anchor term the transport corrupted 10 -> 9 in the incident).
    drain_msgs(&mut n1594);

    // Deliver the FAKE ack: 154 acks index 42 with log_term 9 — exactly what
    // 154 would answer to a corrupted empty append anchored at (42,t9), while
    // its own log[42] is (42,t9) and the leader's log[42] is (42,t10).
    let mut fake_ack = Message::default();
    fake_ack.set_msg_type(MessageType::MsgAppendResponse);
    fake_ack.from = LEADER_OLD;
    fake_ack.to = CANDIDATE;
    fake_ack.term = 10;
    fake_ack.index = 42;
    fake_ack.log_term = 9;
    fake_ack.reject = false;
    n1594.step(fake_ack).unwrap();

    // The leader must NOT advance matched[154] to 42 and must NOT commit 42.
    let matched_after_fake = n1594.raft.prs().get(LEADER_OLD).unwrap().matched;
    assert!(
        matched_after_fake < 42,
        "fake ack must not advance matched (got {matched_after_fake})"
    );
    assert_eq!(
        n1594.raft.raft_log.committed, 41,
        "42(t10) must not be committed"
    );
    // A recovery probe must have been sent so the real state can be re-synced.
    assert!(
        drain_msgs(&mut n1594)
            .into_iter()
            .any(|m| m.get_msg_type() == MessageType::MsgAppend && m.to == LEADER_OLD),
        "leader must re-probe the inconsistent follower"
    );

    // Deliver a GENUINE ack: 154 has overwritten (42,t9) with (42,t10) and
    // acks 42 with log_term 10. The leader advances matched and commits 42.
    let mut real_ack = Message::default();
    real_ack.set_msg_type(MessageType::MsgAppendResponse);
    real_ack.from = LEADER_OLD;
    real_ack.to = CANDIDATE;
    real_ack.term = 10;
    real_ack.index = 42;
    real_ack.log_term = 10;
    real_ack.reject = false;
    n1594.step(real_ack).unwrap();
    assert_eq!(n1594.raft.prs().get(LEADER_OLD).unwrap().matched, 42);
    assert_eq!(
        n1594.raft.raft_log.committed, 42,
        "genuine ack commits 42(t10)"
    );
}

#[test]
fn empty_append_anchored_at_42_acks_silently_without_log_change() {
    // Same 154 state as the incident at 05.292.
    let mut n154 = make_node(LEADER_OLD, node154_log(), {
        let mut hs = HardState::default();
        hs.set_term(9);
        hs.set_vote(LEADER_OLD);
        hs.set_commit(41);
        hs
    });

    // An EMPTY append anchored at (42,t9) — the message shape required to
    // produce a silent ack(42) with an unchanged log.
    let mut empty = Message::default();
    empty.set_msg_type(MessageType::MsgAppend);
    empty.from = CANDIDATE;
    empty.to = LEADER_OLD;
    empty.term = 10;
    empty.index = 42;
    empty.log_term = 9;
    empty.commit = 41;
    // entries deliberately empty.

    n154.step(empty).unwrap();
    let resp = drain_msgs(&mut n154)
        .into_iter()
        .find(|m| m.get_msg_type() == MessageType::MsgAppendResponse)
        .expect("154 must answer the empty append");
    assert!(!resp.reject);
    assert_eq!(resp.index, 42, "silent ack(42)");
    assert_eq!(
        resp.log_term, 9,
        "ack reports the term 154 actually matched (42,t9)"
    );
    assert_eq!(
        n154.raft.raft_log.term(42).unwrap(),
        9,
        "log must be unchanged: 154 still holds (42,t9)"
    );
    assert_eq!(n154.raft.raft_log.committed, 41);
    // The ack is FAKE: 154 acks 42 but has no entry at 42 with the leader's
    // term 10. The leader would commit (42,t10) believing 154 replicated it.
    assert!(
        !n154.raft.raft_log.match_term(resp.index, 10),
        "the silent ack does NOT correspond to an entry 154 holds"
    );
}
