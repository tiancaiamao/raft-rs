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
/// - Last log index, term, subterm (for tracking received entries)
/// - Committed log term, subterm (for vote decisions — the witness compares
///   a candidate's log against the COMMITTED log, not the received log,
///   because shortcut-replicated entries may be uncommitted)
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

    /// Last log term known to this witness (from ALL received entries,
    /// including uncommitted shortcut-replicated entries).
    pub last_log_term: u64,

    /// Last log subterm known to this witness.
    pub last_log_subterm: u64,

    /// Current commit index.
    pub commit: u64,

    /// Term of the entry at the committed index.
    /// Updated only when `msg.commit > self.commit` and we can determine
    /// the term (from `msg.commit_term` or from entries in the append).
    /// Used in `handle_vote` for the log up-to-date comparison.
    pub committed_log_term: u64,

    /// Subterm of the entry at the committed index.
    pub committed_log_subterm: u64,

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
        self.committed_log_term = state.committed_log_term;
        self.committed_log_subterm = state.committed_log_subterm;
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
        state.set_committed_log_term(self.committed_log_term);
        state.set_committed_log_subterm(self.committed_log_subterm);
        state.set_lead(self.lead);
        state.set_replication_set(self.replication_set.iter().copied().collect());
        state
    }

    /// Processes a WitnessMessage and returns a response message if needed.
    ///
    /// The response (if any) is a regular Message that should be sent back to the
    /// leader/candidate via the normal Raft message channel.
    pub fn process(&mut self, msg: &WitnessMessage) -> Option<WitnessResponse> {
        match msg.get_msg_type() {
            MessageType::MsgAppend => self.handle_append(msg),
            MessageType::MsgRequestVote => self.handle_vote(msg, false),
            MessageType::MsgRequestPreVote => self.handle_vote(msg, true),
            MessageType::MsgHeartbeat => self.handle_heartbeat(msg),
            _ => None,
        }
    }

    fn handle_append(&mut self, msg: &WitnessMessage) -> Option<WitnessResponse> {
        // Per Figure 2.4: reject stale-term requests, replying with our
        // (higher) term so the leader learns it must step down.
        if msg.term < self.term {
            return Some(WitnessResponse::StaleTerm(self.term));
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

        // Update commit and committed_log_term.
        if msg.commit > self.commit {
            self.commit = msg.commit;
            // Determine the term at the commit index:
            // 1. If msg.commit_term is provided (non-zero by the leader), use it.
            // 2. Otherwise, scan entries for the entry at index msg.commit.
            if msg.commit_term != 0 {
                self.committed_log_term = msg.commit_term;
                self.committed_log_subterm = msg.commit_subterm;
            } else if !entries.is_empty() {
                // If the commit falls within the entries sent, look up its term.
                // If not (e.g. commit advanced via a previous ack), keep the
                // existing committed_log_term — it's a safe upper bound.
                let first_idx = entries.first().unwrap().index;
                let last_idx = entries.last().unwrap().index;
                if msg.commit >= first_idx && msg.commit <= last_idx {
                    let offset = (msg.commit - first_idx) as usize;
                    if let Some(entry) = entries.get(offset) {
                        self.committed_log_term = entry.term;
                        self.committed_log_subterm = entry.subterm;
                    }
                }
            }
        }

        Some(WitnessResponse::Persist(self.to_hard_state()))
    }

    fn handle_vote(&mut self, msg: &WitnessMessage, is_pre_vote: bool) -> Option<WitnessResponse> {
        // Ignore stale vote requests.
        if msg.term < self.term {
            return None;
        }

        let _old_term = self.term;
        let _old_committed_log_term = self.committed_log_term;
        let _old_committed_log_subterm = self.committed_log_subterm;
        let _old_replication_set = self.replication_set.clone();

        // Update term if needed — only for real votes, not pre-votes.
        // Pre-votes must not advance the witness term; doing so would cause
        // the current leader's heartbeats (at the old term) to be rejected,
        // triggering a chain reaction where the witness steps down the leader,
        // and both nodes become followers stuck in an election loop.
        if msg.term > self.term && !is_pre_vote {
            self.term = msg.term;
            self.vote = 0;
        }

        // Per the paper (Fig 2.7, HandleRequestWitnessVoteRequest):
        // Condition 1: m.mvotesGranted ⊆ witnessReplicationSet
        //   All votes the candidate claims must be from servers within the
        //   witness's current replication set. This prevents a stale candidate
        //   from winning the witness's vote after the replication set has been
        //   adjusted by the current leader.
        //
        // Condition 2: votedFor[WitnessID] ∈ {Nil, j}
        //   The witness must not have already voted for a different candidate
        //   in this term.
        //
        // Both conditions must hold regardless of the log comparison outcome.

        // Condition 1: all voters that have granted the candidate are in our replication set.
        // Skip this check if the replication set is empty (uninitialized witness).
        // This can happen when a witness is freshly created via ConfChange and hasn't
        // yet received its first heartbeat/append from the leader. In this state, there
        // is no quorum defined yet, so condition 1 is not applicable.
        let replication_set_ok = if !self.replication_set.is_empty() {
            msg.vote_ids
                .iter()
                .zip(msg.vote_vals.iter())
                .filter(|(_, &v)| v)
                .all(|(&id, _)| self.replication_set.contains(&id))
        } else {
            true
        };

        // Per the paper (Fig 2.7, HandleRequestWitnessVoteRequest):
        //
        //   let logOk = ∨ m.mlastLogTerm > witnessLastLogTerm
        //               ∨ ∧ m.mlastLogTerm = witnessLastLogTerm
        //                 ∧ m.mlastLogSubterm > witnessLastLogSubterm
        //               ∨ ∧ m.mlastLogTerm = witnessLastLogTerm
        //                 ∧ m.mlastLogSubterm = witnessLastLogSubterm
        //                 ∧ m.mvotesGranted ⊆ witnessReplicationSet
        //
        // The witness compares against `last_log_term`/`last_log_subterm` —
        // the (term, subterm) of entries it has RECEIVED via shortcut
        // replication (the paper's witnessLastLogTerm/witnessLastLogSubterm).
        // These are updated eagerly in `handle_append` when the leader sends
        // entries, so they can never lag behind the actual commit.
        //
        // This is critical for safety: the previous implementation compared
        // against `committed_log_term` instead, which is derived from the
        // leader's commit index at send time. If the leader commits an entry
        // via shortcut replication and then crashes before the next heartbeat
        // reaches the witness, `committed_log_term` stays stale and a
        // candidate missing that committed entry could win the witness vote —
        // violating Election Safety.
        //
        // Unlike standard Raft there is NO index comparison here. The witness
        // does not store the full log prefix, so it cannot compare indices.
        // Safety is preserved because the candidate has already won q−1 regular
        // votes, and those regular voters applied the full Raft log-up-to-date
        // check (including index). The witness only breaks the tie.
        //
        // For the equal-(term, subterm) case the paper additionally requires
        // `mvotesGranted ⊆ witnessReplicationSet`; that is enforced by the
        // separate `replication_set_ok` check below, which is applied to all
        // branches (strictly more restrictive than the paper — safe).
        let log_ok = if msg.last_log_term > self.last_log_term {
            true
        } else if msg.last_log_term == self.last_log_term {
            msg.last_log_subterm >= self.last_log_subterm
        } else {
            false
        };

        // Condition 2: votedFor[WitnessID] ∈ {Nil, j}
        //   The witness must not have already voted for a different candidate
        //   in this term. However, a request at a higher term from a different
        //   candidate is always allowed — it means the current leader may have
        //   crashed and a new election is starting. The `msg.term > self.term`
        //   clause handles this case:
        //
        //   - For real votes (msg.term > self.term), `self.term` is advanced
        //     and `self.vote` reset to 0 earlier in this function, so `can_vote`
        //     naturally passes via `self.vote == 0`. The `msg.term > self.term`
        //     clause is not strictly needed here but kept for clarity.
        //
        //   - For pre-votes, `self.term` is NOT advanced (to avoid disrupting
        //     the current leader). Without the term check, a pre-vote from a
        //     different candidate at a higher term would be rejected because
        //     `self.vote` still belongs to the old term's election. This would
        //     prevent the stale candidate from ever starting an election —
        //     a liveness failure.
        let can_vote = msg.term > self.term || self.vote == 0 || self.vote == msg.from;

        let grant = log_ok && replication_set_ok && can_vote;

        #[cfg(feature = "witness-debug")]
        println!(
            "WITNESS_DEBUG: handle_vote(is_pre_vote={}) grant={} old_term={} term_now={} \
             old_committed_log_term={} self_committed_log_term={} \
             self_committed_log_subterm={} msg_term={} msg_last_log_term={} \
             msg_last_log_subterm={} log_ok={} can_vote={} replication_set_ok={} \
             replication_set={:?} vote_ids={:?} vote_vals={:?} last_log_index={} commit={}",
            is_pre_vote,
            grant,
            _old_term,
            self.term,
            _old_committed_log_term,
            self.committed_log_term,
            self.committed_log_subterm,
            msg.term,
            msg.last_log_term,
            msg.last_log_subterm,
            log_ok,
            can_vote,
            replication_set_ok,
            _old_replication_set,
            msg.vote_ids,
            msg.vote_vals,
            msg.last_log_index,
            self.commit,
        );

        if grant && !is_pre_vote {
            self.vote = msg.from;
        }

        Some(WitnessResponse::VoteGrant(grant))
    }

    fn handle_heartbeat(&mut self, msg: &WitnessMessage) -> Option<WitnessResponse> {
        // Stale leader: reply with our higher term so it steps down.
        if msg.term < self.term {
            return Some(WitnessResponse::StaleTerm(self.term));
        }

        if msg.term > self.term {
            self.term = msg.term;
            self.vote = 0;
        }
        self.lead = msg.from;

        // Update commit if heartbeat indicates a higher commit.
        if msg.commit > self.commit {
            self.commit = msg.commit;
            // Use the commit_term from the heartbeat if provided.
            if msg.commit_term != 0 {
                self.committed_log_term = msg.commit_term;
                self.committed_log_subterm = msg.commit_subterm;
            }
            // If commit_term is 0, keep the existing committed_log_term.
            // This is safe because the committed_log_term can only be
            // too low (causing us to be more permissive in voting),
            // never too high (which would incorrectly reject candidates).
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
    /// The request came from a leader at a stale term. The host should
    /// build a response message carrying the witness's current (higher)
    /// term and step it into the leader's RawNode so it steps down to
    /// follower.  This implements the paper's Figure 2.4 reject branch:
    ///
    ///   m.mterm < currentTerm[WitnessID]
    ///   → Reply(AppendEntriesResponse, mterm → currentTerm, msuccess → false)
    ///
    /// Without this the stale leader never learns its term is outdated
    /// and becomes a "zombie leader" that hangs proposals indefinitely.
    StaleTerm(u64),
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
        msg.set_msg_type(MessageType::MsgRequestVote);
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
        msg.set_msg_type(MessageType::MsgAppend);
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
        msg.set_msg_type(MessageType::MsgRequestVote);
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
        // The committed state reflects the same log position — the witness
        // has committed entries at term 2, so a candidate with last_log_term=1
        // is stale and must be rejected.
        w.commit = 10;
        w.committed_log_term = 2;
        w.committed_log_subterm = 0;
        w.replication_set = vec![1, 2, 3].into_iter().collect();

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 2;
        msg.set_msg_type(MessageType::MsgRequestVote);
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
        // Same term but lower subterm: the candidate missed entries that
        // were committed via shortcut replication in a newer subterm. Per
        // the paper (§2.6), the witness must reject this candidate — its
        // (term, subterm) is strictly behind the witness's committed state.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 10;
        w.last_log_subterm = 5;
        // The committed state has the same term+index but a higher subterm,
        // meaning the candidate missed entries committed in a newer subterm.
        w.commit = 10;
        w.committed_log_term = 3;
        w.committed_log_subterm = 5;

        let msg = make_vote_msg(1, 5, 3, 2, 10, &[1], &[true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(false))));
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
        w.commit = 10;
        w.committed_log_term = 3;
        w.committed_log_subterm = 2;
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
        w.commit = 10;
        w.committed_log_term = 3;
        w.committed_log_subterm = 2;
        w.replication_set = vec![1, 2].into_iter().collect();

        // votesGranted = {1, 2, 99}, 99 ∉ replicationSet.
        let msg = make_vote_msg(1, 5, 3, 2, 10, &[1, 2, 99], &[true, true, true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(false))));
    }

    #[test]
    fn test_witness_log_ok_higher_term_no_index_check() {
        // Per the paper (Fig 2.7), the witness vote comparison is purely
        // (term, subterm) based — there is NO index comparison (the witness
        // does not store the full log prefix). A candidate whose last_log_term
        // is strictly higher is always log-ok, regardless of index.
        //
        // This is safe because the candidate has already won q−1 regular votes;
        // those regular voters applied the full Raft up-to-date check including
        // index, ensuring the candidate has all committed entries.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 3;
        w.last_log_index = 20;
        w.last_log_subterm = 2;
        w.replication_set = vec![1, 2].into_iter().collect();

        // last_log_term=5 > witness last_log_term=3 → log_ok, even though
        // the candidate's index (8) is behind the witness's last_log_index (20).
        let msg = make_vote_msg(1, 5, 5, 0, 8, &[1], &[true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(true))));
    }

    #[test]
    fn test_witness_log_ok_lower_term_rejected() {
        // Candidate's lastLogTerm < witness's committed term → rejected.
        let mut w = Witness::new(3);
        w.term = 5;
        w.last_log_term = 5;
        w.last_log_index = 10;
        w.last_log_subterm = 2;
        w.commit = 10;
        w.committed_log_term = 5;
        w.committed_log_subterm = 2;

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
        msg.set_msg_type(MessageType::MsgRequestPreVote);
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
        msg.set_msg_type(MessageType::MsgAppend);
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
        msg.set_msg_type(MessageType::MsgAppend);
        msg.commit = 3;

        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.last_log_index, 5); // unchanged
        assert_eq!(w.commit, 3);
    }

    #[test]
    fn test_witness_append_stale_term_returns_stale_term() {
        // Stale-term append must return StaleTerm so the old leader steps down.
        let mut w = Witness::new(3);
        w.term = 3;
        w.last_log_index = 5;

        let msg = make_append_msg(1, 2, &[(6, 2, 0)], 0);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::StaleTerm(3))));
        assert_eq!(w.last_log_index, 5); // witness state unchanged
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
        msg.set_msg_type(MessageType::MsgHeartbeat);
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
        msg.set_msg_type(MessageType::MsgHeartbeat);

        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::Persist(_))));
        assert_eq!(w.term, 3);
        assert_eq!(w.vote, 0); // reset on new term
        assert_eq!(w.lead, 2);
    }

    #[test]
    fn test_witness_heartbeat_stale_returns_stale_term() {
        let mut w = Witness::new(3);
        w.term = 5;

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 3;
        msg.set_msg_type(MessageType::MsgHeartbeat);

        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::StaleTerm(5))));
    }

    #[test]
    fn test_witness_heartbeat_commit_never_regresses() {
        let mut w = Witness::new(3);
        w.term = 2;
        w.commit = 10;

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 2;
        msg.set_msg_type(MessageType::MsgHeartbeat);
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
        msg.set_msg_type(MessageType::MsgPropose);

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

    // ══════════════════════════════════════════════════════════════
    // Bug B: witness pollution — shortcut replication advances the
    // witness's last_log_index past a lagging real voter. The voter
    // shares the same (term, subterm) but a lower index. The witness
    // must NOT reject based on index — only (term, subterm) matters
    // per the paper (§2.6, Figure 2.7).
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_witness_pollution_lagging_voter_granted() {
        // Reproduces the exact chaos-test scenario:
        //   - Witness last_log_index = 4053 (advanced by shortcut replication)
        //   - Candidate store2 last_log_index = 4051 (lagging real voter)
        //   - Same term 8, same subterm
        //   - Candidate's granted votes are all in the witness replication set
        // The witness must grant the vote despite the index gap.
        let mut w = Witness::new(3);
        w.term = 7; // will be bumped by msg.term=8
        w.last_log_term = 8;
        w.last_log_index = 4053;
        w.last_log_subterm = 1;
        w.replication_set = vec![1, 2, 3].into_iter().collect();

        // Candidate store2 (id=2) at term 8, same (term=8, subterm=1).
        // It got a vote from store1 (id=1, in replication set).
        let msg = make_vote_msg(2, 8, 8, 1, 4051, &[1, 2], &[true, true]);
        let resp = w.process(&msg);
        assert!(
            matches!(resp, Some(WitnessResponse::VoteGrant(true))),
            "lagging voter with same (term, subterm) must be granted despite lower index"
        );
    }

    #[test]
    fn test_witness_vote_higher_term_wins_over_stale_witness() {
        // Scenario: witness has never received any append (last_log_term=0).
        // Candidate has last_log_term=6 (higher). Witness should grant.
        // This tests the fresh-witness-after-election scenario.
        let mut w = Witness::new(278);
        w.term = 7; // term from leader's heartbeat
        w.last_log_term = 0; // NEVER updated because replicate_to_witness=false
        w.last_log_subterm = 0;
        w.last_log_index = 0;

        // Candidate (store4, id=277) sends pre-vote with term=7, last_log_term=6
        let msg = make_vote_msg(277, 7, 6, 0, 5, &[276, 277], &[false, true]);
        let resp = w.process(&msg);
        assert!(
            matches!(resp, Some(WitnessResponse::VoteGrant(true))),
            "candidate with higher last_log_term (6 > 0) must get grant even with empty replication_set"
        );
    }

    #[test]
    fn test_witness_prevote_higher_term_empty_replication_set() {
        // Pre-vote (not real vote) with candidate having higher last_log_term.
        let mut w = Witness::new(278);
        w.term = 7;
        w.last_log_term = 0;
        w.last_log_subterm = 0;
        w.last_log_index = 0;
        w.replication_set = HashSet::new(); // empty!

        let mut msg = make_vote_msg(277, 7, 6, 0, 5, &[276, 277], &[false, true]);
        msg.set_msg_type(MessageType::MsgRequestPreVote);

        let resp = w.process(&msg);
        eprintln!("\n=== WITNESS_DEBUG_TEST ===");
        eprintln!("term=7, last_log_term=0, last_log_subterm=0");
        eprintln!("msg term=7, msg last_log_term=6, msg last_log_subterm=0");
        eprintln!("result: {:?}", resp);

        assert!(
            matches!(resp, Some(WitnessResponse::VoteGrant(true))),
            "pre-vote with higher last_log_term (6 > 0) must grant even with empty replication_set"
        );
    }

    #[test]
    fn test_witness_re_election_without_append() {
        // Full integration test:
        // 1. First pre-vote: witness at term=0, candidate term=5 → grant
        // 2. Real vote: witness term=0, candidate term=6 → grant (higher term)
        //    Persisted: term=6, last_log_term=0
        // 3. Leader sends heartbeat: witness term=6 → term=7 (higher)
        //    Persisted: term=7, last_log_term=0
        // 4. Second candidate pre-vote: msg term=7, msg last_log_term=6
        //    witness last_log_term=0 → 6 > 0 → grant!
        let mut w = Witness::new(278);

        // Step 1: First pre-vote at term 6
        let mut msg1 = make_vote_msg(277, 6, 5, 0, 4, &[277], &[true]);
        msg1.set_msg_type(MessageType::MsgRequestPreVote);
        let resp1 = w.process(&msg1);
        eprintln!("\n=== Step 1: First pre-vote ===");
        eprintln!("result: {:?}", resp1);
        assert!(matches!(resp1, Some(WitnessResponse::VoteGrant(true))));
        eprintln!(
            "After pre-vote: term={}, last_log_term={}",
            w.term, w.last_log_term
        );

        // Step 2: Real vote at term 6
        let mut msg2 = make_vote_msg(277, 6, 5, 0, 4, &[276, 277], &[false, true]);
        msg2.set_msg_type(MessageType::MsgRequestVote);
        let resp2 = w.process(&msg2);
        eprintln!("\n=== Step 2: Real vote ===");
        eprintln!("result: {:?}", resp2);
        assert!(matches!(resp2, Some(WitnessResponse::VoteGrant(true))));
        assert_eq!(w.vote, 277);
        eprintln!(
            "After real vote: term={}, last_log_term={}, vote={}",
            w.term, w.last_log_term, w.vote
        );
        let hs = w.to_hard_state();
        eprintln!(
            "Persisted state: term={}, last_log_term={}, vote={}, last_log_index={}",
            hs.state.as_ref().map(|s| s.term).unwrap_or(0),
            hs.last_log_term,
            hs.state.as_ref().map(|s| s.vote).unwrap_or(0),
            hs.last_log_index
        );

        // Step 3: Leader heartbeat at term 7
        let mut msg3 = WitnessMessage::default();
        msg3.from = 276;
        msg3.set_msg_type(MessageType::MsgHeartbeat);
        msg3.term = 7;
        msg3.commit = 5;
        let resp3 = w.process(&msg3);
        eprintln!("\n=== Step 3: Heartbeat ===");
        eprintln!("result: {:?}", resp3);
        assert!(matches!(resp3, Some(WitnessResponse::Persist(_))));
        eprintln!(
            "After heartbeat: term={}, last_log_term={}, vote={}, lead={}",
            w.term, w.last_log_term, w.vote, w.lead
        );

        // Step 4: Second pre-vote at term 7 (another candidate)
        let mut msg4 = make_vote_msg(277, 7, 6, 0, 5, &[276, 277], &[false, true]);
        msg4.set_msg_type(MessageType::MsgRequestPreVote);
        let resp4 = w.process(&msg4);
        eprintln!("\n=== Step 4: Second pre-vote ===");
        eprintln!("result: {:?}", resp4);
        assert!(
            matches!(resp4, Some(WitnessResponse::VoteGrant(true))),
            "Second pre-vote: candidate last_log_term=6 > witness last_log_term=0 => grant"
        );

        // Step 5: Real vote at term 7 (second election)
        let mut msg5 = make_vote_msg(277, 7, 6, 0, 5, &[276, 277], &[false, true]);
        msg5.set_msg_type(MessageType::MsgRequestVote);
        let resp5 = w.process(&msg5);
        eprintln!("\n=== Step 5: Second real vote ===");
        eprintln!("result: {:?}", resp5);
        assert!(
            matches!(resp5, Some(WitnessResponse::VoteGrant(true))),
            "Second real vote: candidate last_log_term=6 > witness last_log_term=0 => grant"
        );
    }

    // ══════════════════════════════════════════════════════════════
    // Regression: stale committed_log_term must NOT cause an unsafe grant.
    //
    // Leader A commits e2@(idx6,term2) via shortcut replication ({A,B,W}),
    // then crashes before heartbeat reaches the witness. The witness's
    // committed_log_term is stale (still 1) but last_log_term is accurate
    // (2, because the witness received e2 via shortcut replication).
    //
    // A candidate C missing committed e2 (last_log_term=1) must be REJECTED.
    // With the old committed_log_term comparison it was wrongly GRANTED —
    // a safety violation. With the paper-faithful last_log_term comparison
    // it is correctly rejected.
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_witness_stale_committed_log_term_still_safe() {
        // Cluster: 4 regular (1,2,3,4) + witness W=5. Quorum = 3.
        let mut w = Witness::new(5);
        w.term = 2;
        w.vote = 1; // voted for leader A (id=1) in term 2
        w.commit = 5; // STALE: actual commit is 6
        w.committed_log_term = 1; // STALE: actual committed term is 2
        w.committed_log_subterm = 0;
        w.last_log_term = 2; // ACCURATE: witness received e2 (term 2)
        w.last_log_subterm = 0;
        w.last_log_index = 6;
        w.replication_set = vec![1, 2, 3, 4].into_iter().collect();

        // Candidate C (id=3) is missing committed e2: only has entries to idx5.
        let msg = make_vote_msg(3, 3, 1, 0, 5, &[3, 4], &[true, true]);
        let resp = w.process(&msg);

        assert!(
            matches!(resp, Some(WitnessResponse::VoteGrant(false))),
            "candidate missing committed e2 (last_log_term=1 < witness last_log_term=2) \
             must be rejected even when committed_log_term is stale"
        );
    }

    #[test]
    fn test_witness_accurate_committed_log_term_also_safe() {
        // Same scenario, but with ACCURATE committed_log_term — for completeness.
        // Both last_log_term and committed_log_term agree that e2 (term 2) exists.
        let mut w = Witness::new(5);
        w.term = 2;
        w.vote = 1;
        w.commit = 6;
        w.committed_log_term = 2;
        w.committed_log_subterm = 0;
        w.last_log_term = 2;
        w.last_log_subterm = 0;
        w.last_log_index = 6;
        w.replication_set = vec![1, 2, 3, 4].into_iter().collect();

        let msg = make_vote_msg(3, 3, 1, 0, 5, &[3, 4], &[true, true]);
        let resp = w.process(&msg);
        assert!(matches!(resp, Some(WitnessResponse::VoteGrant(false))));
    }

    // ══════════════════════════════════════════════════════════════
    // Stale-term append/heartbeat → StaleTerm reply (Figure 2.4 reject)
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn test_witness_stale_append_returns_stale_term() {
        // Witness is at term 3 (e.g., it already voted for a new leader).
        let mut w = Witness::new(5);
        w.term = 3;

        // Old leader at term 2 sends an append.
        let msg = make_append_msg(1, 2, &[(1, 2, 0)], 1);
        let resp = w.process(&msg);
        assert!(
            matches!(resp, Some(WitnessResponse::StaleTerm(t)) if t == 3),
            "stale-term append must return StaleTerm(witness_term), got {:?}",
            resp
        );
    }

    #[test]
    fn test_witness_stale_heartbeat_returns_stale_term() {
        let mut w = Witness::new(5);
        w.term = 3;

        let mut msg = WitnessMessage::default();
        msg.from = 1;
        msg.term = 2; // stale
        msg.set_msg_type(MessageType::MsgHeartbeat);
        msg.commit = 5;

        let resp = w.process(&msg);
        assert!(
            matches!(resp, Some(WitnessResponse::StaleTerm(t)) if t == 3),
            "stale-term heartbeat must return StaleTerm(witness_term), got {:?}",
            resp
        );
    }
}
