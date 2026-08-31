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
    make_node_with_witness(id, entries, hs, true)
}

fn make_node_with_witness(
    id: u64,
    entries: Vec<Entry>,
    hs: HardState,
    with_witness: bool,
) -> RawNode<MemStorage> {
    let storage = MemStorage::default();
    let mut cs = ConfState::default();
    cs.set_voters(vec![LEADER_OLD, CANDIDATE, WITNESS]);
    if with_witness {
        cs.set_witness(WITNESS);
    }
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
    Entry {
        index,
        term,
        ..Default::default()
    }
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
    // No immediate re-probe: re-sending right away against a follower that
    // keeps answering inconsistently would pin the leader in a tight
    // send/reject loop (the log-divergence incident). The next probe is
    // instead driven by the heartbeat.
    assert!(
        !drain_msgs(&mut n1594)
            .into_iter()
            .any(|m| m.get_msg_type() == MessageType::MsgAppend && m.to == LEADER_OLD),
        "inconsistent ack must not trigger an immediate re-probe"
    );

    // A heartbeat response resumes the progress and re-probes.
    let mut hb = Message::default();
    hb.set_msg_type(MessageType::MsgHeartbeatResponse);
    hb.from = LEADER_OLD;
    hb.to = CANDIDATE;
    hb.term = 10;
    n1594.step(hb).unwrap();
    assert!(
        drain_msgs(&mut n1594)
            .into_iter()
            .any(|m| m.get_msg_type() == MessageType::MsgAppend && m.to == LEADER_OLD),
        "heartbeat response drives the re-probe"
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
fn termless_append_ack_is_accepted_for_pure_3f() {
    // A pure 3F configuration must accept an ACK from an old follower, which
    // does not populate log_term in MsgAppendResponse.
    let mut leader = make_node_with_witness(
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
        false,
    );
    leader.campaign().unwrap();

    let mut vote = Message::default();
    vote.set_msg_type(MessageType::MsgRequestVoteResponse);
    vote.from = LEADER_OLD;
    vote.to = CANDIDATE;
    vote.term = 10;
    vote.reject = false;
    leader.step(vote).unwrap();
    assert_eq!(leader.raft.state, raft::StateRole::Leader);
    drain_msgs(&mut leader);

    let mut old_follower_ack = Message::default();
    old_follower_ack.set_msg_type(MessageType::MsgAppendResponse);
    old_follower_ack.from = LEADER_OLD;
    old_follower_ack.to = CANDIDATE;
    old_follower_ack.term = 10;
    old_follower_ack.index = 42;
    old_follower_ack.log_term = 0;
    old_follower_ack.reject = false;
    leader.step(old_follower_ack).unwrap();

    assert_eq!(
        leader.raft.prs().get(LEADER_OLD).unwrap().matched,
        42,
        "a pure 3F leader must accept an old follower's term-less ACK"
    );
    assert_eq!(leader.raft.raft_log.committed, 42);
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

/// The observed incident shape (region 594, 2026-08-11): a term-37 leader whose
/// log[626] = 36 probes a follower holding a divergent committed log
/// (log[626] = 35, committed = 626). The follower ignored the leader's
/// snapshot (older than its committed index) and answered with its committed
/// index. Pre-fix, handle_snapshot's responses carried no log_term, so the
/// leader's backward-compat check (`log_term == 0`) trusted the ack blindly,
/// advanced matched to 626, and pinned replication in an infinite reject loop.
#[test]
fn snapshot_ack_without_term_must_not_advance_matched() {
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
    drain_msgs(&mut n1594);

    // The follower acks index 42 but reports no term — exactly what
    // handle_snapshot answered before the fix (a bare `index` only).
    let mut ack = Message::default();
    ack.set_msg_type(MessageType::MsgAppendResponse);
    ack.from = LEADER_OLD;
    ack.to = CANDIDATE;
    ack.term = 10;
    ack.index = 42;
    ack.log_term = 0;
    ack.reject = false;
    n1594.step(ack).unwrap();

    // The leader must NOT advance matched and must NOT commit 42: the ack
    // carries no term evidence that the follower actually holds (42,t10).
    assert!(
        n1594.raft.prs().get(LEADER_OLD).unwrap().matched < 42,
        "an ack without a term must not advance matched"
    );
    assert_eq!(
        n1594.raft.raft_log.committed, 41,
        "42(t10) must not be committed from a term-less ack"
    );

    // A genuine ack (correct term) still advances matched and commits.
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

/// handle_snapshot's responses must report the term and commit of the acked
/// index (mirroring handle_append_entries) so the leader can verify the ack.
/// Pre-fix both branches answered with only an index: log_term == 0 and
/// commit == 0, indistinguishable from an unverifiable ack.
#[test]
fn ignored_snapshot_response_reports_committed_term_and_commit() {
    // 154: follower at term 9 with log 1..42(t9), committed 41.
    let mut n154 = make_node(LEADER_OLD, node154_log(), {
        let mut hs = HardState::default();
        hs.set_term(9);
        hs.set_vote(LEADER_OLD);
        hs.set_commit(41);
        hs
    });

    // A snapshot older than the follower's committed index (40 < 41) is
    // ignored by restore(); the response must still carry the term/commit of
    // the acked (committed) index.
    let mut snap_msg = Message::default();
    snap_msg.set_msg_type(MessageType::MsgSnapshot);
    snap_msg.from = CANDIDATE;
    snap_msg.to = LEADER_OLD;
    snap_msg.term = 10;
    let mut snap = raft::eraftpb::Snapshot::default();
    let mut meta = raft::eraftpb::SnapshotMetadata::default();
    meta.set_index(40);
    meta.set_term(9);
    snap.set_metadata(meta);
    snap_msg.set_snapshot(snap);

    n154.step(snap_msg).unwrap();
    let resp = drain_msgs(&mut n154)
        .into_iter()
        .find(|m| m.get_msg_type() == MessageType::MsgAppendResponse)
        .expect("154 must answer the snapshot");
    assert!(!resp.reject);
    assert_eq!(resp.index, 41, "ignored snapshot acks the committed index");
    assert_eq!(resp.log_term, 9, "ack reports term(committed)");
    assert_eq!(resp.commit, 41, "ack reports the committed index");
}

/// The restored-snapshot branch: a snapshot newer than the log is installed
/// and the response reports the snapshot's last index and term.
#[test]
fn restored_snapshot_response_reports_last_index_term() {
    // 154: follower at term 9 with log 1..42(t9), committed 41.
    let mut n154 = make_node(LEADER_OLD, node154_log(), {
        let mut hs = HardState::default();
        hs.set_term(9);
        hs.set_vote(LEADER_OLD);
        hs.set_commit(41);
        hs
    });

    let mut snap_msg = Message::default();
    snap_msg.set_msg_type(MessageType::MsgSnapshot);
    snap_msg.from = CANDIDATE;
    snap_msg.to = LEADER_OLD;
    snap_msg.term = 10;
    let mut snap = raft::eraftpb::Snapshot::default();
    let mut meta = raft::eraftpb::SnapshotMetadata::default();
    meta.set_index(50);
    meta.set_term(10);
    let mut cs = raft::eraftpb::ConfState::default();
    cs.set_voters(vec![LEADER_OLD, CANDIDATE, WITNESS]);
    cs.set_witness(WITNESS);
    meta.set_conf_state(cs);
    snap.set_metadata(meta);
    snap_msg.set_snapshot(snap);

    n154.step(snap_msg).unwrap();
    let resp = drain_msgs(&mut n154)
        .into_iter()
        .find(|m| m.get_msg_type() == MessageType::MsgAppendResponse)
        .expect("154 must answer the snapshot");
    assert!(!resp.reject);
    assert_eq!(resp.index, 50, "restored snapshot acks its last index");
    assert_eq!(resp.log_term, 10, "ack reports the snapshot's term");
    assert_eq!(resp.commit, 50, "ack reports the restored committed index");
}

/// Term-10 leader with log 1..100(t9) + noop 101(t10), committed 100, whose
/// storage has been compacted at 91 (truncated_index = 90).
fn make_compacted_leader() -> (RawNode<MemStorage>, MemStorage) {
    let storage = MemStorage::default();
    let mut cs = ConfState::default();
    cs.set_voters(vec![LEADER_OLD, CANDIDATE, WITNESS]);
    cs.set_witness(WITNESS);
    storage.initialize_with_conf_state(cs);
    storage
        .wl()
        .append(&(1..=100).map(|i| entry(i, 9)).collect::<Vec<_>>())
        .unwrap();
    let mut hs = HardState::default();
    hs.set_term(9);
    hs.set_vote(CANDIDATE);
    hs.set_commit(100);
    storage.wl().set_hardstate(hs);
    let config = raft::Config {
        id: CANDIDATE,
        election_tick: 10,
        heartbeat_tick: 2,
        check_quorum: true,
        // Real peers always have applied >= truncated_index; 90 is the
        // compaction anchor here (compact(91) => truncated_index = 90).
        applied: 90,
        ..Default::default()
    };
    let mut n = RawNode::new(&config, storage.clone(), &logger()).unwrap();
    storage.wl().compact(91).unwrap();

    n.campaign().unwrap();
    let mut witness_vote = Message::default();
    witness_vote.set_msg_type(MessageType::MsgRequestVoteResponse);
    witness_vote.from = WITNESS;
    witness_vote.to = CANDIDATE;
    witness_vote.term = 10;
    witness_vote.reject = false;
    n.step(witness_vote).unwrap();
    assert_eq!(n.raft.state, raft::StateRole::Leader);
    drain_msgs(&mut n);
    (n, storage)
}

/// The incident shape (region 605/623/637): a follower acks a snapshot index
/// the leader has already compacted away. `raft_log.term(index)` returns
/// Ok(0) — indistinguishable from "beyond the log end" — so pre-fix the term
/// check rejected the ack and stalled replication. The acked index is below
/// the leader's committed index, so accepting it cannot advance the commit
/// index, and any lie is caught by the next append probe from index+1.
#[test]
fn compacted_ack_with_real_term_is_accepted() {
    let (mut n, _storage) = make_compacted_leader();
    assert_eq!(
        n.raft.raft_log.term(50).unwrap(),
        0,
        "50 is compacted away: the leader can no longer look up its term"
    );

    // The follower restored the leader's snapshot at 50 and acks (50, t9).
    let mut ack = Message::default();
    ack.set_msg_type(MessageType::MsgAppendResponse);
    ack.from = LEADER_OLD;
    ack.to = CANDIDATE;
    ack.term = 10;
    ack.index = 50;
    ack.log_term = 9;
    ack.reject = false;
    n.step(ack).unwrap();
    assert_eq!(
        n.raft.prs().get(LEADER_OLD).unwrap().matched,
        50,
        "an ack below committed must be accepted even if compacted away"
    );
    assert_eq!(
        n.raft.raft_log.committed, 100,
        "accepting an ack below committed must not raise the commit index"
    );
}

/// The other Ok(0) case must keep being rejected: an ack for an index beyond
/// the leader's log end (> last_index, hence > committed) with a non-zero
/// term asserts an entry that cannot exist in the leader's log; trusting it
/// could commit entries the follower never replicated.
#[test]
fn ack_beyond_log_end_with_wrong_term_is_rejected() {
    let (mut n, _storage) = make_compacted_leader();
    let last_index = n.raft.raft_log.last_index();
    assert!(
        n.raft.raft_log.term(200).unwrap() == 0 && 200 > last_index,
        "200 must be beyond the leader's log end"
    );

    let mut ack = Message::default();
    ack.set_msg_type(MessageType::MsgAppendResponse);
    ack.from = LEADER_OLD;
    ack.to = CANDIDATE;
    ack.term = 10;
    ack.index = 200;
    ack.log_term = 5;
    ack.reject = false;
    n.step(ack).unwrap();
    assert_eq!(
        n.raft.prs().get(LEADER_OLD).unwrap().matched,
        0,
        "an ack beyond the leader's log end must not advance matched"
    );
}
