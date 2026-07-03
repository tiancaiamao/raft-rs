// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Extended Raft Witness module.
//!
//! A witness is a special voter that participates in elections and commits
//! via shortcut replication, but does not run a full Raft instance. The witness
//! state is stored externally (e.g., S3/object storage).
//!
//! The witness processes `WitnessMessage`s from the leader/candidate:
//! - `MsgAppend`: shortcut replication — store the entries, update last_log info
//! - `MsgRequestVote`/`MsgRequestPreVote`: vote if the candidate is up-to-date
//!   and its votesGranted ⊆ replicationSet
//! - `MsgHeartbeat`: keep-alive, update leader info

use crate::eraftpb::{Entry, MessageType, WitnessHardState, WitnessMessage};
use crate::Result;
use std::collections::HashSet;

/// Trait for witness storage backends.
///
/// The witness state must be persisted atomically (conditional write).
/// In CSE, this is backed by S3 conditional writes (If-Match ETag).
pub trait WitnessStorage {
    /// Loads the current witness state. Returns None if no state exists.
    fn load(&self) -> Result<Option<WitnessHardState>>;

    /// Saves the witness state unconditionally.
    fn save(&self, state: &WitnessHardState) -> Result<()>;

    /// Conditionally saves: only succeeds if the current state matches `expected_subterm`.
    /// Returns Ok(true) if saved, Ok(false) if the condition was not met.
    fn conditional_save(&self, state: &WitnessHardState, expected_subterm: u64) -> Result<bool>;
}

/// A witness processor that handles WitnessMessages.
///
/// The witness maintains:
/// - Current term, vote, commit (like HardState)
/// - Last log index, term, subterm (for vote decisions)
/// - Leader ID
/// - Replication set (for validating vote requests)
#[derive(Default)]
pub struct Witness {
    /// The witness node ID.
    pub id: u64,

    /// Current term.
    pub term: u64,

    /// Who this witness voted for in the current term.
    pub vote: u64,

    /// Last log index known to this witness.
    pub last_log_index: u64,

    /// Last log term known to this witness.
    pub last_log_term: u64,

    /// Last log subterm known to this witness.
    pub last_log_subterm: u64,

    /// Current commit index.
    pub commit: u64,

    /// Current leader ID.
    pub lead: u64,

    /// Replication set (non-witness voters that the leader replicates to).
    /// Includes both incoming and outgoing voters during joint consensus.
    pub replication_set: HashSet<u64>,
}

impl Witness {
    /// Creates a new witness with the given ID.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    /// Restores witness state from storage.
    pub fn restore(&mut self, state: &WitnessHardState) {
        self.term = state.get_state().term;
        self.vote = state.get_state().vote;
        self.commit = state.get_state().commit;
        self.last_log_index = state.last_log_index;
        self.last_log_term = state.last_log_term;
        self.last_log_subterm = state.last_log_subterm;
        self.lead = state.lead;
        self.replication_set = state.replication_set.iter().copied().collect();
    }

    /// Converts the witness state to WitnessHardState for persistence.
    pub fn to_hard_state(&self) -> WitnessHardState {
        let mut state = WitnessHardState::default();
        {
            let hs = state.mut_state();
            hs.set_term(self.term);
            hs.set_vote(self.vote);
            hs.set_commit(self.commit);
        }
        state.set_last_log_index(self.last_log_index);
        state.set_last_log_term(self.last_log_term);
        state.set_last_log_subterm(self.last_log_subterm);
        state.set_lead(self.lead);
        state.set_replication_set(self.replication_set.iter().copied().collect());
        state
    }

    /// Processes a WitnessMessage and returns a response message if needed.
    ///
    /// The response (if any) is a regular Message that should be sent back to the
    /// leader/candidate via the normal Raft message channel.
    pub fn process(&mut self, msg: &WitnessMessage) -> Option<WitnessResponse> {
        match msg.msg_type {
            MessageType::MsgAppend => self.handle_append(msg),
            MessageType::MsgRequestVote => self.handle_vote(msg, false),
            MessageType::MsgRequestPreVote => self.handle_vote(msg, true),
            MessageType::MsgHeartbeat => self.handle_heartbeat(msg),
            _ => None,
        }
    }

    fn handle_append(&mut self, msg: &WitnessMessage) -> Option<WitnessResponse> {
        // Ignore stale messages.
        if msg.term < self.term {
            return None;
        }

        // Update term/leader if needed.
        if msg.term > self.term {
            self.term = msg.term;
            self.vote = 0; // Reset vote when term changes.
        }
        self.lead = msg.from;

        // Update replication set. During joint consensus, the witness needs to
        // know voters from both halves so it can validate votes correctly.
        self.replication_set.clear();
        self.replication_set
            .extend(msg.replication_set_incoming.iter().copied());
        self.replication_set
            .extend(msg.replication_set_outgoing.iter().copied());

        // Process entries.
        let entries = msg.get_entries();
        if !entries.is_empty() {
            let last = entries.last().unwrap();
            // Accept entries that are contiguous or overlapping, but only
            // advance state if the result moves forward (never regress).
            if last.index > self.last_log_index {
                self.last_log_index = last.index;
                self.last_log_term = last.term;
                self.last_log_subterm = last.subterm;
            }
        }

        // Update commit.
        if msg.commit > self.commit {
            self.commit = msg.commit;
        }

        Some(WitnessResponse::Persist(self.to_hard_state()))
    }

    fn handle_vote(&mut self, msg: &WitnessMessage, is_pre_vote: bool) -> Option<WitnessResponse> {
        // Ignore stale vote requests.
        if msg.term < self.term {
            return None;
        }

        // Update term if needed.
        if msg.term > self.term {
            self.term = msg.term;
            self.vote = 0;
        }

        // Determine log consistency using standard Raft rules (term, index)
        // refined by subterm for the shortcut-replication protocol.
        //
        // A candidate's log is at least as up-to-date as the witness's when:
        //   1. Higher last_log_term, OR
        //   2. Same last_log_term and higher/equal last_log_index (standard
        //      Raft tie-break), AND one of:
        //      a. higher subterm (witness saw fewer shortcut commits), or
        //      b. same subterm with compatible replication set, or
        //      c. lower subterm but higher/equal index (candidate has entries
        //         beyond what the witness saw — standard Raft log wins).
        let log_ok = if msg.last_log_term > self.last_log_term {
            // Higher term → always up-to-date.
            true
        } else if msg.last_log_term == self.last_log_term {
            if msg.last_log_index > self.last_log_index {
                // Candidate has entries beyond the witness's last seen index.
                true
            } else if msg.last_log_index == self.last_log_index {
                // Same (term, index): use subterm to break the tie.
                if msg.last_log_subterm > self.last_log_subterm {
                    true
                } else if msg.last_log_subterm == self.last_log_subterm {
                    // Same (term, index, subterm): check replication set.
                    msg.vote_ids
                        .iter()
                        .zip(msg.vote_vals.iter())
                        .filter(|(_, &v)| v)
                        .all(|(&id, _)| self.replication_set.contains(&id))
                } else {
                    // Same (term, index) but lower subterm: the candidate
                    // has not progressed through shortcut replication as
                    // far as the witness recorded. This can happen after a
                    // leader change where the new leader's log entries have
                    // subterm=0 (normal Raft entries). Since (term, index)
                    // match, the log is equivalent — accept.
                    true
                }
            } else {
                // Candidate's index is lower → not up-to-date.
                false
            }
        } else {
            // Lower term → not up-to-date.
            false
        };

        // votedFor check: can only grant if we haven't voted for someone else.
        // PreVote is exempt from this check — it must not disrupt a real
        // election, nor be blocked by a prior vote in the same term.
        let can_vote = is_pre_vote || self.vote == 0 || self.vote == msg.from;

        let grant = log_ok && can_vote;

        if grant && !is_pre_vote {
            self.vote = msg.from;
        }

        Some(WitnessResponse::VoteGrant(grant))
    }

    fn handle_heartbeat(&mut self, msg: &WitnessMessage) -> Option<WitnessResponse> {
        if msg.term < self.term {
            return None;
        }

        if msg.term > self.term {
            self.term = msg.term;
            self.vote = 0;
        }
        self.lead = msg.from;

        // Update commit if heartbeat indicates a higher commit.
        if msg.commit > self.commit {
            self.commit = msg.commit;
        }

        Some(WitnessResponse::Persist(self.to_hard_state()))
    }
}

/// Response from witness processing.
#[derive(Debug)]
pub enum WitnessResponse {
    /// Witness state should be persisted (conditional write to storage).
    Persist(WitnessHardState),
    /// Vote response (true = granted, false = rejected).
    VoteGrant(bool),
}

#[cfg(test)]
mod tests {
    use super::*;

    // ──────────────────────────────────────────────────────────────
    // Helper to build a witness vote message
    // ──────────────────────────────────────────────────────────────
    fn make_vote_msg(
        from: u64,
        term: u64,
        last_log_term: u64,
        last_log_subterm: u64,
        last_log_index: u64,
        vote_ids: &[u64],
        vote_vals: &[bool],
    ) -> WitnessMessage {
        let mut msg = WitnessMessage::default();
        msg.from = from;
        msg.term = term;
        msg.msg_type = MessageType::MsgRequestVote;
        msg.last_log_term = last_log_term;
        msg.last_log_subterm = last_log_subterm;
        msg.last_log_index = last_log_index;
        msg.vote_ids = vote_ids.to_vec().into();
        msg.vote_vals = vote_vals.to_vec().into();
        msg
    }

    // ──────────────────────────────────────────────────────────────
    // Helper to build a witness append message
    // ──────────────────────────────────────────────────────────────
    fn make_append_msg(
        from: u64,
        term: u64,
        entries: &[(u64, u64, u64)], // (index, term, subterm)
        commit: u64,
    ) -> WitnessMessage {
        let mut msg = WitnessMessage::default();
        msg.from = from;
        msg.term = term;
        msg.msg_type = MessageType::MsgAppend;
        msg.commit = commit;
        if !entries.is_empty() {
            let first = entries[0];
            msg.last_log_term = first.1;
            msg.last_log_subterm = first.2;
            msg.last_log_index = first.0;
        }
        msg.entries = entries
            .iter()
            .map(|(idx, t, st)| {
                let mut e = Entry::default();
                e.index = *idx;
                e.term = *t;
                e.subterm = *st;
                e
            })
            .collect::<Vec<_>>()
            .into();
        msg
    }

    // ══════════════════════════════════════════════════════════════
    // P0: votedFor — no double vote in the same term
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_witness_vote_grant() {
        let mut w = Witness::new(3);
        w.term = 1;
        w.last_log_term = 1;
        w.last_log_index = 5;
        w.last_log_subterm = 0;
        w.replication_set = vec![1, 2, 3].into_iter().collect();

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 2;
        msg.msg_type = MessageType::MsgRequestVote;
        msg.last_log_term = 1;
        msg.last_log_index = 5;
        msg.last_log_subterm = 0;
        msg.vote_ids = vec![1, 2];
        msg.vote_vals = vec![true, true];

        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(true))));
        assert_eq!(w.vote, 1);
    }

    #[test]
    fn test_witness_vote_reject_stale_log() {
        let mut w = Witness::new(3);
        w.term = 1;
        w.last_log_term = 2;
        w.last_log_index = 10;
        w.replication_set = vec![1, 2, 3].into_iter().collect();

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 2;
        msg.msg_type = MessageType::MsgRequestVote;
        msg.last_log_term = 1; // Stale
        msg.last_log_index = 5;
        msg.vote_ids = vec![1];
        msg.vote_vals = vec![true];

        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(false))));
    }

    #[test]
    fn test_witness_vote_no_double_vote_same_term() {
        // P0 regression: same term, two different candidates.
        // The second must be rejected because vote is already cast.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 2;
        w.replication_set = vec![1, 2, 3].into_iter().collect();

        // Candidate 1 requests vote at term 5 (same term).
        let msg1 = make_vote_msg(1, 5, 3, 2, 10, &[1], &[true]);
        let resp1 = w.process(&msg1);
        assert!(matches!(resp1, Some(WitnessResponse::VoteGrant(true))));
        assert_eq!(w.vote, 1);

        // Candidate 2 requests vote at same term 5, must be rejected.
        let msg2 = make_vote_msg(2, 5, 3, 2, 10, &[2], &[true]);
        let resp2 = w.process(&msg2);
        assert!(matches!(resp2, Some(WitnessResponse::VoteGrant(false))));
        assert_eq!(w.vote, 1); // vote unchanged
    }

    #[test]
    fn test_witness_vote_revote_same_candidate_same_term() {
        // Re-vote from the same candidate in the same term should be OK (idempotent).
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 2;
        w.replication_set = vec![1, 2, 3].into_iter().collect();

        let msg = make_vote_msg(1, 5, 3, 2, 10, &[1], &[true]);
        let resp1 = w.process(&msg);
        assert!(matches!(resp1, Some(WitnessResponse::VoteGrant(true))));

        // Same candidate asks again — should still grant.
        let resp2 = w.process(&msg);
        assert!(matches!(resp2, Some(WitnessResponse::VoteGrant(true))));
    }

    #[test]
    fn test_witness_vote_resets_on_higher_term() {
        // A new term resets vote, so the new candidate can win.
        let mut w = Witness::new(3);
        w.term = 5;
        w.vote = 1;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 2;
        w.replication_set = vec![1, 2, 3].into_iter().collect();

        // Candidate 2 at term 6 (higher term).
        let msg = make_vote_msg(2, 6, 4, 0, 10, &[2], &[true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(true))));
        assert_eq!(w.term, 6);
        assert_eq!(w.vote, 2);
    }

    // ══════════════════════════════════════════════════════════════
    // P1#3: logOk three cases + subset check
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_witness_log_ok_higher_term() {
        // Case 1: candidate's lastLogTerm > witness's → always log-ok.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 2;
        w.replication_set = vec![1, 2].into_iter().collect();

        let msg = make_vote_msg(1, 5, 4, 0, 8, &[1], &[true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(true))));
    }

    #[test]
    fn test_witness_log_ok_same_term_higher_subterm() {
        // Case 2: same term, same index, candidate's subterm > witness's → log-ok.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 2;
        w.replication_set = vec![1, 2].into_iter().collect();

        let msg = make_vote_msg(1, 5, 3, 3, 10, &[1], &[true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(true))));
    }

    #[test]
    fn test_witness_log_ok_same_term_lower_subterm() {
        // Same (term, index) but lower subterm: after a leader change, the
        // new leader's entries may have subterm=0 even though the witness
        // saw higher subterms via shortcut replication. Since (term, index)
        // match, the log is equivalent → grant.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 5;

        let msg = make_vote_msg(1, 5, 3, 2, 10, &[1], &[true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(true))));
    }

    #[test]
    fn test_witness_log_ok_same_term_subterm_subset_pass() {
        // Case 3: same term AND subterm, votesGranted ⊆ replicationSet → ok.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 2;
        w.replication_set = vec![1, 2, 3].into_iter().collect();

        // votesGranted = {1, 2}, both in replicationSet.
        let msg = make_vote_msg(1, 5, 3, 2, 10, &[1, 2, 4], &[true, true, false]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(true))));
    }

    #[test]
    fn test_witness_log_ok_same_term_subterm_subset_fail() {
        // Case 3: same term AND subterm, but a granted voter NOT in replicationSet → reject.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 2;
        w.replication_set = vec![1, 2].into_iter().collect();

        // votesGranted = {1, 5}, but 5 ∉ replicationSet.
        let msg = make_vote_msg(1, 5, 3, 2, 10, &[1, 5], &[true, true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(false))));
    }

    #[test]
    fn test_witness_log_ok_same_term_subterm_subset_strict() {
        // Even a single stray vote outside replicationSet causes rejection.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 2;
        w.replication_set = vec![1, 2].into_iter().collect();

        // votesGranted = {1, 2, 99}, 99 ∉ replicationSet.
        let msg = make_vote_msg(1, 5, 3, 2, 10, &[1, 2, 99], &[true, true, true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(false))));
    }

    #[test]
    fn test_witness_log_ok_lower_term_rejected() {
        // Candidate's lastLogTerm < witness's → rejected regardless.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 5;
        w.last_log_index = 10;
        w.last_log_subterm = 2;

        let msg = make_vote_msg(1, 5, 3, 5, 15, &[1], &[true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(false))));
    }

    // ══════════════════════════════════════════════════════════════
    // Pre-vote: should not set self.vote
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_witness_prevote_does_not_record_vote() {
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 2;
        w.replication_set = vec![1, 2, 3].into_iter().collect();

        let mut msg = make_vote_msg(1, 5, 3, 2, 10, &[1], &[true]);
        msg.msg_type = MessageType::MsgRequestPreVote;
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(true))));
        assert_eq!(w.vote, 0); // Pre-vote should NOT set vote.
    }

    // ══════════════════════════════════════════════════════════════
    // P2#5: handle_append — last_log_index never regresses
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_witness_append() {
        let mut w = Witness::new(3);
        w.term = 1;
        w.last_log_index = 4;

        let mut entry = Entry::default();
        entry.index = 5;
        entry.term = 1;
        entry.subterm = 0;

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 1;
        msg.msg_type = MessageType::MsgAppend;
        msg.last_log_index = 5;
        msg.last_log_term = 1;
        msg.entries = vec![entry].into();
        msg.commit = 3;

        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.last_log_index, 5);
        assert_eq!(w.commit, 3);
    }

    #[test]
    fn test_witness_append_no_regression() {
        // P2#5 regression: older entries must not cause last_log_index to go backward.
        let mut w = Witness::new(3);
        w.term = 1;
        w.last_log_index = 10;
        w.last_log_term = 2;
        w.last_log_subterm = 1;

        // Entries at index 3-4 (older than current 10).
        let msg = make_append_msg(1, 1, &[(3, 1, 0), (4, 1, 0)], 2);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.last_log_index, 10); // unchanged
        assert_eq!(w.last_log_term, 2); // unchanged
        assert_eq!(w.last_log_subterm, 1); // unchanged
    }

    #[test]
    fn test_witness_append_forward_progress() {
        // Normal case: entries extend the log forward.
        let mut w = Witness::new(3);
        w.term = 1;
        w.last_log_index = 5;
        w.last_log_term = 1;
        w.last_log_subterm = 0;

        let msg = make_append_msg(1, 1, &[(6, 1, 1), (7, 1, 1)], 4);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.last_log_index, 7);
        assert_eq!(w.last_log_term, 1);
        assert_eq!(w.last_log_subterm, 1);
    }

    #[test]
    fn test_witness_append_overlapping_no_regression() {
        // Entries overlap current position but extend further — should advance.
        let mut w = Witness::new(3);
        w.term = 1;
        w.last_log_index = 5;

        // Entries start at 3 but go to 8.
        let msg = make_append_msg(1, 1, &[(3, 1, 0), (4, 1, 0), (8, 1, 0)], 0);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.last_log_index, 8);
    }

    #[test]
    fn test_witness_append_empty_entries() {
        let mut w = Witness::new(3);
        w.term = 1;
        w.last_log_index = 5;

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 1;
        msg.msg_type = MessageType::MsgAppend;
        msg.commit = 3;

        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.last_log_index, 5); // unchanged
        assert_eq!(w.commit, 3);
    }

    #[test]
    fn test_witness_append_stale_term_ignored() {
        // Messages with lower term should be ignored entirely.
        let mut w = Witness::new(3);
        w.term = 3;
        w.last_log_index = 5;

        let msg = make_append_msg(1, 2, &[(6, 2, 0)], 0);
        let resp = w.process(&msg);
        assert!(resp.is_none()); // stale → no response
        assert_eq!(w.last_log_index, 5); // unchanged
    }

    // ══════════════════════════════════════════════════════════════
    // Heartbeat
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_witness_heartbeat_updates_commit() {
        let mut w = Witness::new(3);
        w.term = 2;
        w.commit = 3;

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 2;
        msg.msg_type = MessageType::MsgHeartbeat;
        msg.commit = 5;

        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.commit, 5);
        assert_eq!(w.lead, 1);
    }

    #[test]
    fn test_witness_heartbeat_higher_term_resets_vote() {
        let mut w = Witness::new(3);
        w.term = 2;
        w.vote = 1;

        let mut msg = WitnessMessage::default();
        msg.from = 2;
        msg.term = 3;
        msg.msg_type = MessageType::MsgHeartbeat;

        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.term, 3);
        assert_eq!(w.vote, 0); // reset on new term
        assert_eq!(w.lead, 2);
    }

    #[test]
    fn test_witness_heartbeat_stale_ignored() {
        let mut w = Witness::new(3);
        w.term = 5;

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 3;
        msg.msg_type = MessageType::MsgHeartbeat;

        let resp = w.process(&msg);
        assert!(resp.is_none());
    }

    #[test]
    fn test_witness_heartbeat_commit_never_regresses() {
        let mut w = Witness::new(3);
        w.term = 2;
        w.commit = 10;

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 2;
        msg.msg_type = MessageType::MsgHeartbeat;
        msg.commit = 5; // lower

        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.commit, 10); // unchanged
    }

    // ══════════════════════════════════════════════════════════════
    // Restore / to_hard_state round-trip
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_witness_restore_and_serialize() {
        let mut w = Witness::new(3);
        w.term = 5;
        w.vote = 1;
        w.last_log_index = 100;
        w.last_log_term = 4;
        w.last_log_subterm = 3;
        w.commit = 50;
        w.lead = 1;
        w.replication_set = vec![1, 2].into_iter().collect();

        let hs = w.to_hard_state();

        // Restore into a fresh witness.
        let mut w2 = Witness::new(3);
        w2.restore(&hs);

        assert_eq!(w2.term, 5);
        assert_eq!(w2.vote, 1);
        assert_eq!(w2.last_log_index, 100);
        assert_eq!(w2.last_log_term, 4);
        assert_eq!(w2.last_log_subterm, 3);
        assert_eq!(w2.commit, 50);
        assert_eq!(w2.lead, 1);
        assert_eq!(w2.replication_set, vec![1, 2].into_iter().collect());
    }

    // ══════════════════════════════════════════════════════════════
    // Edge cases
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_witness_unknown_message_type_ignored() {
        let mut w = Witness::new(3);
        w.term = 1;

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 2;
        msg.msg_type = MessageType::MsgPropose;

        let resp = w.process(&msg);
        assert!(resp.is_none());
    }

    #[test]
    fn test_witness_append_updates_replication_set() {
        let mut w = Witness::new(3);
        w.term = 1;
        w.replication_set = vec![1, 2, 3].into_iter().collect();

        let mut msg = make_append_msg(1, 1, &[(5, 1, 0)], 0);
        msg.replication_set_incoming = vec![1, 2].into();
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.replication_set, vec![1, 2].into_iter().collect());
    }

    #[test]
    fn test_witness_initial_state() {
        let w = Witness::new(5);
        assert_eq!(w.id, 5);
        assert_eq!(w.term, 0);
        assert_eq!(w.vote, 0);
        assert_eq!(w.last_log_index, 0);
        assert_eq!(w.last_log_term, 0);
        assert_eq!(w.last_log_subterm, 0);
        assert_eq!(w.commit, 0);
        assert_eq!(w.lead, 0);
        assert!(w.replication_set.is_empty());
    }
}
