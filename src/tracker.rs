// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod inflights;
mod progress;
mod state;

pub use self::inflights::Inflights;
pub use self::progress::Progress;
pub use self::state::ProgressState;

use crate::confchange::{MapChange, MapChangeType};
use crate::eraftpb::ConfState;
use crate::quorum::{AckedIndexer, Index, VoteResult};
use crate::{DefaultHashBuilder, HashMap, HashSet, JointConfig};
use getset::Getters;
use std::fmt::Debug;

/// Config reflects the configuration tracked in a ProgressTracker.
#[derive(Clone, Debug, Default, PartialEq, Eq, Getters)]
pub struct Configuration {
    #[get = "pub"]
    pub(crate) voters: JointConfig,
    /// Learners is a set of IDs corresponding to the learners active in the
    /// current configuration.
    ///
    /// Invariant: Learners and Voters does not intersect, i.e. if a peer is in
    /// either half of the joint config, it can't be a learner; if it is a
    /// learner it can't be in either half of the joint config. This invariant
    /// simplifies the implementation since it allows peers to have clarity about
    /// its current role without taking into account joint consensus.
    #[get = "pub"]
    pub(crate) learners: HashSet<u64>,
    /// When we turn a voter into a learner during a joint consensus transition,
    /// we cannot add the learner directly when entering the joint state. This is
    /// because this would violate the invariant that the intersection of
    /// voters and learners is empty. For example, assume a Voter is removed and
    /// immediately re-added as a learner (or in other words, it is demoted):
    ///
    /// Initially, the configuration will be
    ///
    ///   voters:   {1 2 3}
    ///   learners: {}
    ///
    /// and we want to demote 3. Entering the joint configuration, we naively get
    ///
    ///   voters:   {1 2} & {1 2 3}
    ///   learners: {3}
    ///
    /// but this violates the invariant (3 is both voter and learner). Instead,
    /// we get
    ///
    ///   voters:   {1 2} & {1 2 3}
    ///   learners: {}
    ///   next_learners: {3}
    ///
    /// Where 3 is now still purely a voter, but we are remembering the intention
    /// to make it a learner upon transitioning into the final configuration:
    ///
    ///   voters:   {1 2}
    ///   learners: {3}
    ///   next_learners: {}
    ///
    /// Note that next_learners is not used while adding a learner that is not
    /// also a voter in the joint config. In this case, the learner is added
    /// right away when entering the joint configuration, so that it is caught up
    /// as soon as possible.
    #[get = "pub"]
    pub(crate) learners_next: HashSet<u64>,
    /// True if the configuration is joint and a transition to the incoming
    /// configuration should be carried out automatically by Raft when this is
    /// possible. If false, the configuration will be joint until the application
    /// initiates the transition manually.
    #[get = "pub"]
    pub(crate) auto_leave: bool,
    /// Witness IDs for incoming and outgoing configs.
    /// witnesses[0] = incoming witness, witnesses[1] = outgoing witness.
    /// 0 means no witness in that half.
    pub witnesses: [u64; 2],
}

// Display and crate::itertools used only for test
#[cfg(test)]
impl std::fmt::Display for Configuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use itertools::Itertools;
        if self.voters.outgoing.is_empty() {
            write!(f, "voters={}", self.voters.incoming)?
        } else {
            write!(
                f,
                "voters={}&&{}",
                self.voters.incoming, self.voters.outgoing
            )?
        }
        if !self.learners.is_empty() {
            write!(
                f,
                " learners=({})",
                self.learners
                    .iter()
                    .sorted_by(|&a, &b| a.cmp(b))
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>()
                    .join(" ")
            )?
        }
        if !self.learners_next.is_empty() {
            write!(
                f,
                " learners_next=({})",
                self.learners_next
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>()
                    .join(" ")
            )?
        }
        if self.auto_leave {
            write!(f, " autoleave")?
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Progress;

    fn make_tracker_with_witness(voters: &[u64], witness: u64) -> ProgressTracker {
        let mut tracker = ProgressTracker::with_capacity(voters.len(), 0, 256);
        let mut conf = Configuration::new(voters.iter().copied(), vec![]);
        conf.witnesses[0] = witness;
        let changes: MapChange = voters.iter().map(|&id| (id, MapChangeType::Add)).collect();
        tracker.apply_conf(conf, changes, 1);
        tracker.reset_replication_set(true);
        tracker
    }

    // ──────────────────────────────────────────────────────────────
    // Epoch / ReplicationSet basics
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn test_epoch_has_witness() {
        let mut epoch = Epoch::default();
        assert!(!epoch.has_witness());

        epoch.replication_sets[0].witness = 3;
        assert!(epoch.has_witness());

        let mut epoch2 = Epoch::default();
        epoch2.replication_sets[1].witness = 5;
        assert!(epoch2.has_witness());
    }

    #[test]
    fn test_epoch_replicate_to_witness() {
        let mut epoch = Epoch::default();
        // No witness → both false.
        assert_eq!(epoch.replicate_to_witness(), (false, false));

        // Witness in incoming, not excluded.
        epoch.replication_sets[0].witness = 3;
        assert_eq!(epoch.replicate_to_witness(), (true, false));

        // Witness excluded → don't replicate.
        epoch.replication_sets[0].excluded = 3;
        assert_eq!(epoch.replicate_to_witness(), (false, false));

        // Different node excluded → still replicate to witness.
        epoch.replication_sets[0].excluded = 2;
        assert_eq!(epoch.replicate_to_witness(), (true, false));
    }

    #[test]
    fn test_epoch_witness_subterm_default() {
        let epoch = Epoch::default();
        assert_eq!(epoch.witness_subterm, [0, 0]);
    }

    // ──────────────────────────────────────────────────────────────
    // reset_replication_set
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn test_reset_replication_set_basic() {
        let tracker = make_tracker_with_witness(&[1, 2, 3], 3);
        let epoch = &tracker.epoch;

        assert_eq!(epoch.subterm, 0); // reset_subterm=true → 0
        assert_eq!(epoch.replication_sets[0].witness, 3);
        assert_eq!(epoch.replication_sets[0].excluded, 3);
        assert!(epoch.replication_sets[0].non_witness_voters.contains(&1));
        assert!(epoch.replication_sets[0].non_witness_voters.contains(&2));
        assert!(!epoch.replication_sets[0].non_witness_voters.contains(&3));
    }

    #[test]
    fn test_reset_replication_set_increment_subterm() {
        let mut tracker = make_tracker_with_witness(&[1, 2, 3], 3);
        tracker.epoch.subterm = 5;
        tracker.epoch.witness_subterm[0] = 5;

        // reset_subterm=false → subterm should increment.
        tracker.reset_replication_set(false);
        assert_eq!(tracker.epoch.subterm, 6);
        assert_eq!(tracker.epoch.witness_subterm, [0, 0]); // new epoch, reset
    }

    #[test]
    fn test_reset_replication_set_no_witness() {
        let mut tracker = ProgressTracker::with_capacity(3, 0, 256);
        let conf = Configuration::new(vec![1, 2, 3], vec![]);
        let changes: MapChange = vec![
            (1, MapChangeType::Add),
            (2, MapChangeType::Add),
            (3, MapChangeType::Add),
        ];
        tracker.apply_conf(conf, changes, 1);
        tracker.reset_replication_set(true);

        assert_eq!(tracker.epoch.replication_sets[0].witness, 0);
        assert_eq!(tracker.epoch.replication_sets[0].excluded, 0);
    }

    #[test]
    fn test_reset_replication_set_2_voters_1_witness_includes_witness() {
        // Voters: [1, 2(witness)], only 1 non-witness voter.
        // With non_witness_count=1 < 2, the witness must be included as a
        // regular voter so quorum (2/2) can be reached.
        let tracker = make_tracker_with_witness(&[1, 2], 2);
        let epoch = &tracker.epoch;

        assert_eq!(epoch.subterm, 0);
        assert_eq!(epoch.replication_sets[0].witness, 2);
        assert_eq!(epoch.replication_sets[0].excluded, 0);
        assert!(epoch.replication_sets[0].non_witness_voters.contains(&1));
        assert!(epoch.replication_sets[0].non_witness_voters.contains(&2));
        assert_eq!(epoch.replication_sets[0].non_witness_voters.len(), 2);
    }

    #[test]
    fn test_reset_replication_set_2_voters_replicate_to_witness() {
        // With 2 voters (1 non-witness + 1 witness), reset should produce
        // a config where replicate_to_witness() returns (true, false):
        // witness is needed for quorum and is not excluded.
        let tracker = make_tracker_with_witness(&[1, 2], 2);
        let (needs_witness, witness_is_excluded) = tracker.epoch.replicate_to_witness();
        assert!(needs_witness);
        assert!(!witness_is_excluded);
    }

    #[test]
    fn test_reset_replication_set_2_voters_shortcut_replication() {
        // With 2 voters (1 non-witness + 1 witness), the witness is in
        // non_witness_voters. Verify that one_less_than_quorum_in_replication_set
        // can find the threshold index for shortcut replication.
        let mut tracker = make_tracker_with_witness(&[1, 2], 2);

        // Set the active voter's matched index.
        tracker.get_mut(1).unwrap().matched = 42;
        // Witness (2) has is_witness=true, matched starts at 0.

        let result = tracker.one_less_than_quorum_in_replication_set();
        assert_eq!(result.len(), 1);
        // n=2 voters {1, 2}, scope=non_witness_voters={1, 2}.
        // acked_index(1)=42, acked_index(2)=0.
        // position = n/2 - 1 = 0 → the max of the sorted values.
        assert!(result.contains_key(&2));
        assert_eq!(result[&2], 42);
    }

    // ──────────────────────────────────────────────────────────────
    // one_less_than_quorum_in_replication_set
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn test_one_less_than_quorum_2_voters_1_witness() {
        // Voters: [1, 2, 3(witness)] → non-witness voters in set = {1, 2}.
        // After reset, witness is excluded. To trigger shortcut replication,
        // we need witness NOT excluded (someone else excluded or nobody).
        let mut tracker = make_tracker_with_witness(&[1, 2, 3], 3);
        // Make witness not excluded — exclude node 2 instead.
        tracker.epoch.replication_sets[0].excluded = 2;
        tracker.epoch.replication_sets[0]
            .non_witness_voters
            .insert(3);
        tracker.epoch.replication_sets[0]
            .non_witness_voters
            .remove(&2);

        // Set match indexes: node 1 at 5, node 3 at 3.
        tracker.get_mut(1).unwrap().matched = 5;
        tracker.get_mut(3).unwrap().matched = 3;

        let result = tracker.one_less_than_quorum_in_replication_set();
        assert_eq!(result.len(), 1);
        // quorum-1 from {1,2,3} = 1 voter ack needed.
        // n=3 voters, scoped to {1,3}. position = n/2-1 = 0 → max.
        assert!(result[&3] >= 3);
    }

    #[test]
    fn test_one_less_than_quorum_3_voters_1_witness() {
        // Voters: [1, 2, 3, 4(witness)].
        let mut tracker = make_tracker_with_witness(&[1, 2, 3, 4], 4);
        // Exclude node 2 so witness is not excluded.
        tracker.epoch.replication_sets[0].excluded = 2;
        tracker.epoch.replication_sets[0]
            .non_witness_voters
            .insert(4);
        tracker.epoch.replication_sets[0]
            .non_witness_voters
            .remove(&2);

        tracker.get_mut(1).unwrap().matched = 10;
        tracker.get_mut(3).unwrap().matched = 8;
        tracker.get_mut(4).unwrap().matched = 6;

        let result = tracker.one_less_than_quorum_in_replication_set();
        // n=4 voters, scoped to {1,3,4}. position = n/2-1 = 1 → 2nd highest.
        assert_eq!(result.len(), 1);
        // sorted desc: [10, 8, 6, 0], pos=1 → 8.
        assert_eq!(result[&4], 8);
    }

    #[test]
    fn test_one_less_than_quorum_no_witness_empty() {
        let tracker = make_tracker_with_witness(&[1, 2, 3], 0);
        let result = tracker.one_less_than_quorum_in_replication_set();
        assert!(result.is_empty());
    }

    #[test]
    fn test_one_less_than_quorum_witness_excluded() {
        // If witness is the excluded node, replicate_to_witness returns false.
        let mut tracker = make_tracker_with_witness(&[1, 2, 3], 3);
        tracker.epoch.replication_sets[0].excluded = 3; // witness excluded

        let result = tracker.one_less_than_quorum_in_replication_set();
        assert!(result.is_empty()); // witness excluded → no replication needed
    }

    // ──────────────────────────────────────────────────────────────
    // apply_conf — is_witness flag
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn test_apply_conf_sets_witness_flag() {
        let tracker = make_tracker_with_witness(&[1, 2, 3], 3);

        assert!(!tracker.get(1).unwrap().is_witness);
        assert!(!tracker.get(2).unwrap().is_witness);
        assert!(tracker.get(3).unwrap().is_witness);
    }

    #[test]
    fn test_apply_conf_no_witness() {
        let mut tracker = ProgressTracker::with_capacity(3, 0, 256);
        let conf = Configuration::new(vec![1, 2, 3], vec![]);
        let changes: MapChange = vec![
            (1, MapChangeType::Add),
            (2, MapChangeType::Add),
            (3, MapChangeType::Add),
        ];
        tracker.apply_conf(conf, changes, 1);

        for id in 1u64..=3 {
            assert!(!tracker.get(id).unwrap().is_witness);
        }
    }

    // ──────────────────────────────────────────────────────────────
    // ScopedAckIndexer
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn test_scoped_indexer_only_reports_scoped() {
        let mut tracker = make_tracker_with_witness(&[1, 2, 3], 3);

        tracker.get_mut(1).unwrap().matched = 10;
        tracker.get_mut(2).unwrap().matched = 20;
        tracker.get_mut(3).unwrap().matched = 99; // witness, not in scope

        let scope: HashSet<u64> = vec![1, 2].into_iter().collect();
        let scoped = ScopedAckIndexer {
            indexer: &tracker.progress,
            scope: &scope,
        };

        assert_eq!(scoped.acked_index(1).unwrap().index, 10);
        assert_eq!(scoped.acked_index(2).unwrap().index, 20);
        assert!(scoped.acked_index(3).is_none()); // not in scope
    }

    // ──────────────────────────────────────────────────────────────
    // change_replication_set
    // ──────────────────────────────────────────────────────────────

    #[test]
    fn test_change_replication_set_no_change_when_all_active() {
        let mut tracker = make_tracker_with_witness(&[1, 2, 3], 3);

        // All nodes active → no change needed.
        for id in &[1, 2, 3] {
            tracker.get_mut(*id).unwrap().recent_active = true;
        }

        let changed = tracker.change_replication_set();
        assert!(!changed);
    }

    #[test]
    fn test_change_replication_set_excludes_inactive() {
        let mut tracker = make_tracker_with_witness(&[1, 2, 3], 3);

        // Node 1 is active, node 2 is inactive.
        tracker.get_mut(1).unwrap().recent_active = true;
        tracker.get_mut(2).unwrap().recent_active = false;
        // Witness (3) is excluded and active.
        tracker.get_mut(3).unwrap().recent_active = true;

        let changed = tracker.change_replication_set();
        assert!(changed);
        // After change, node 2 (inactive) should be excluded,
        // and the witness (3) should be back in the replication set.
        assert_eq!(tracker.epoch.replication_sets[0].excluded, 2);
        assert!(tracker.epoch.replication_sets[0]
            .non_witness_voters
            .contains(&1));
        assert!(tracker.epoch.replication_sets[0]
            .non_witness_voters
            .contains(&3));
    }

    #[test]
    fn test_change_replication_set_degrade_swap_unreachable_with_witness() {
        // Simulate the production bug scenario:
        // Voters = [43, 44], witness = 230, but witness is NOT in progress map.
        // After reset: non_witness_voters = {43, 44}, excluded = 230.
        // Voter 43 goes unreachable → change_replication_set should swap 43 with 230.
        let mut tracker = ProgressTracker::with_capacity(2, 0, 256);

        let mut conf = Configuration::new(vec![43u64, 44u64], vec![]);
        conf.witnesses[0] = 230;
        // Only add regular voters to progress (not the witness).
        let changes: MapChange = vec![(43, MapChangeType::Add), (44, MapChangeType::Add)]
            .into_iter()
            .collect();
        tracker.apply_conf(conf, changes, 1);
        tracker.reset_replication_set(true);

        // Verify initial state: witness excluded, both voters in set.
        assert_eq!(tracker.epoch.replication_sets[0].witness, 230);
        assert_eq!(tracker.epoch.replication_sets[0].excluded, 230);
        assert!(tracker.epoch.replication_sets[0]
            .non_witness_voters
            .contains(&43));
        assert!(tracker.epoch.replication_sets[0]
            .non_witness_voters
            .contains(&44));
        // Witness should NOT be in non_witness_voters initially.
        assert!(!tracker.epoch.replication_sets[0]
            .non_witness_voters
            .contains(&230));

        // Voter 44 is active, voter 43 is unreachable.
        tracker.get_mut(43).unwrap().recent_active = false;
        tracker.get_mut(44).unwrap().recent_active = true;
        // Witness has no progress entry (production scenario).

        let changed = tracker.change_replication_set();
        assert!(changed);
        let rs = &tracker.epoch.replication_sets[0];
        // The unreachable voter 43 should now be excluded.
        assert_eq!(rs.excluded, 43);
        // Witness (230) should be in non_witness_voters.
        assert!(rs.non_witness_voters.contains(&44));
        assert!(rs.non_witness_voters.contains(&230));
        assert!(!rs.non_witness_voters.contains(&43));
        // replicate_to_witness should return true (witness in set, not excluded).
        assert_eq!(tracker.epoch.replicate_to_witness(), (true, false));
    }
}

impl Configuration {
    /// Create a new configuration with the given configuration.
    pub fn new(
        voters: impl IntoIterator<Item = u64>,
        learners: impl IntoIterator<Item = u64>,
    ) -> Self {
        Self {
            voters: JointConfig::new(voters.into_iter().collect()),
            auto_leave: false,
            learners: learners.into_iter().collect(),
            learners_next: HashSet::default(),
            witnesses: [0, 0],
        }
    }

    fn with_capacity(voters: usize, learners: usize) -> Self {
        Self {
            voters: JointConfig::with_capacity(voters),
            learners: HashSet::with_capacity_and_hasher(learners, DefaultHashBuilder::default()),
            learners_next: HashSet::default(),
            auto_leave: false,
            witnesses: [0, 0],
        }
    }

    /// Create a new `ConfState` from the configuration itself.
    pub fn to_conf_state(&self) -> ConfState {
        // Note: Different from etcd, we don't sort.
        let mut state = ConfState::default();
        state.set_voters(self.voters.incoming.raw_slice());
        state.set_voters_outgoing(self.voters.outgoing.raw_slice());
        state.set_learners(self.learners.iter().cloned().collect());
        state.set_learners_next(self.learners_next.iter().cloned().collect());
        state.auto_leave = self.auto_leave;
        state.witness = self.witnesses[0];
        state.witness_outgoing = self.witnesses[1];
        state
    }

    fn clear(&mut self) {
        self.voters.clear();
        self.learners.clear();
        self.learners_next.clear();
        self.auto_leave = false;
        self.witnesses = [0, 0];
    }
}

/// A replication set identifies the subset of voters that the leader
/// replicates to within a subterm. In Extended Raft, the leader can
/// adjust the replication set to exclude one node (the witness or an
/// inactive voter) per subterm.
#[derive(Clone, Debug, Default)]
pub struct ReplicationSet {
    /// The witness node ID in this config half (0 if none).
    pub witness: u64,
    /// The node currently excluded from replication (0 if none).
    pub excluded: u64,
    /// Non-witness voters in this replication set.
    pub non_witness_voters: HashSet<u64>,
}

/// An epoch tracks the current subterm and replication sets for both
/// incoming and outgoing configs. This is volatile state, only used
/// by the leader.
#[derive(Clone, Debug, Default)]
pub struct Epoch {
    /// The subterm counter, monotonically increasing within a term.
    pub subterm: u64,
    /// Replication sets for [incoming, outgoing].
    pub replication_sets: [ReplicationSet; 2],
    /// The latest subterm during which the leader replicated to the witness
    /// (for each config half). 0 means witness has not been contacted yet.
    /// When witness_subterm[i] == subterm, shortcut replication is active:
    /// the leader synthesizes acks on behalf of the witness.
    pub witness_subterm: [u64; 2],
    /// The subterm in which a witness append is pending CAS confirmation
    /// (for each half). 0 means not pending.
    /// Set to `current_subterm` when `send_append_to_witness` emits a
    /// WitnessMessage; cleared by `confirm_witness_append` (CAS success) or
    /// `reject_witness_append` (CAS failure). While pending, `maybe_commit`
    /// will not re-send — it waits for the external storage layer to report
    /// the outcome before activating shortcut replication.
    ///
    /// Storing the subterm (rather than a plain bool) prevents a stale CAS
    /// confirmation from a prior subterm from activating shortcut replication
    /// in the current subterm.
    pub witness_pending_subterm: [u64; 2],
}

impl Epoch {
    /// Returns true if there's a witness in either config half.
    pub fn has_witness(&self) -> bool {
        self.replication_sets[0].witness != 0 || self.replication_sets[1].witness != 0
    }

    /// Returns (incoming_needs_witness, outgoing_needs_witness) — whether
    /// the witness in each half should receive shortcut replication.
    pub fn replicate_to_witness(&self) -> (bool, bool) {
        let r0 = &self.replication_sets[0];
        let r1 = &self.replication_sets[1];
        (
            r0.witness != 0 && r0.excluded != r0.witness,
            r1.witness != 0 && r1.excluded != r1.witness,
        )
    }
}

pub type ProgressMap = HashMap<u64, Progress>;

impl AckedIndexer for ProgressMap {
    fn acked_index(&self, voter_id: u64) -> Option<Index> {
        self.get(&voter_id).map(|p| Index {
            index: p.matched,
            group_id: p.commit_group_id,
        })
    }
}

/// `ProgressTracker` contains several `Progress`es,
/// which could be `Leader`, `Follower` and `Learner`.
#[derive(Clone, Getters)]
pub struct ProgressTracker {
    progress: ProgressMap,

    /// The current configuration state of the cluster.
    #[get = "pub"]
    conf: Configuration,
    #[doc(hidden)]
    #[get = "pub"]
    votes: HashMap<u64, bool>,
    #[get = "pub(crate)"]
    max_inflight: usize,

    group_commit: bool,

    /// Extended Raft epoch state (subterm + replication sets).
    /// Only used when witnesses are configured.
    pub epoch: Epoch,
}

impl ProgressTracker {
    /// Creates a new ProgressTracker.
    pub fn new(max_inflight: usize) -> Self {
        Self::with_capacity(0, 0, max_inflight)
    }

    /// Create a progress set with the specified sizes already reserved.
    pub fn with_capacity(voters: usize, learners: usize, max_inflight: usize) -> Self {
        ProgressTracker {
            progress: HashMap::with_capacity_and_hasher(
                voters + learners,
                DefaultHashBuilder::default(),
            ),
            conf: Configuration::with_capacity(voters, learners),
            votes: HashMap::with_capacity_and_hasher(voters, DefaultHashBuilder::default()),
            max_inflight,
            group_commit: false,
            epoch: Epoch::default(),
        }
    }

    /// Configures group commit.
    pub fn enable_group_commit(&mut self, enable: bool) {
        self.group_commit = enable;
    }

    /// Whether enable group commit.
    pub fn group_commit(&self) -> bool {
        self.group_commit
    }

    pub(crate) fn clear(&mut self) {
        self.progress.clear();
        self.conf.clear();
        self.votes.clear();
    }

    /// Returns true if (and only if) there is only one voting member
    /// (i.e. the leader) in the current configuration.
    pub fn is_singleton(&self) -> bool {
        self.conf.voters.is_singleton()
    }

    /// Grabs a reference to the progress of a node.
    #[inline]
    pub fn get(&self, id: u64) -> Option<&Progress> {
        self.progress.get(&id)
    }

    /// Grabs a mutable reference to the progress of a node.
    #[inline]
    pub fn get_mut(&mut self, id: u64) -> Option<&mut Progress> {
        self.progress.get_mut(&id)
    }

    /// Returns an iterator across all the nodes and their progress.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two quorums. Use `has_quorum` instead.
    #[inline]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&u64, &Progress)> {
        self.progress.iter()
    }

    /// Returns a mutable iterator across all the nodes and their progress.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two quorums. Use `has_quorum` instead.
    #[inline]
    pub fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&u64, &mut Progress)> {
        self.progress.iter_mut()
    }

    /// Returns the maximal committed index for the cluster. The bool flag indicates whether
    /// the index is computed by group commit algorithm successfully.
    ///
    /// Eg. If the matched indexes are `[2,2,2,4,5]`, it will return `2`.
    /// If the matched indexes and groups are `[(1, 1), (2, 2), (3, 2)]`, it will return `1`.
    pub fn maximal_committed_index(&mut self) -> (u64, bool) {
        self.conf
            .voters
            .committed_index(self.group_commit, &self.progress)
    }

    /// Prepares for a new round of vote counting via recordVote.
    pub fn reset_votes(&mut self) {
        self.votes.clear();
    }

    /// Records that the node with the given id voted for this Raft
    /// instance if v == true (and declined it otherwise).
    pub fn record_vote(&mut self, id: u64, vote: bool) {
        self.votes.entry(id).or_insert(vote);
    }

    /// TallyVotes returns the number of granted and rejected Votes, and whether the
    /// election outcome is known.
    pub fn tally_votes(&self) -> (usize, usize, VoteResult) {
        // Make sure to populate granted/rejected correctly even if the Votes slice
        // contains members no longer part of the configuration. This doesn't really
        // matter in the way the numbers are used (they're informational), but might
        // as well get it right.
        let (mut granted, mut rejected) = (0, 0);
        for (id, vote) in &self.votes {
            if !self.conf.voters.contains(*id) {
                continue;
            }
            if *vote {
                granted += 1;
            } else {
                rejected += 1;
            }
        }
        let result = self.vote_result(&self.votes);
        (granted, rejected, result)
    }

    /// Returns the Candidate's eligibility in the current election.
    ///
    /// If it is still eligible, it should continue polling nodes and checking.
    /// Eventually, the election will result in this returning either `Elected`
    /// or `Ineligible`, meaning the election can be concluded.
    pub fn vote_result(&self, votes: &HashMap<u64, bool>) -> VoteResult {
        self.conf.voters.vote_result(|id| votes.get(&id).cloned())
    }

    /// Determines if the current quorum is active according to the this raft node.
    /// Doing this will set the `recent_active` of each peer to false.
    ///
    /// This should only be called by the leader.
    pub fn quorum_recently_active(&mut self, perspective_of: u64) -> bool {
        let mut active =
            HashSet::with_capacity_and_hasher(self.progress.len(), DefaultHashBuilder::default());
        for (id, pr) in &mut self.progress {
            if *id == perspective_of {
                pr.recent_active = true;
                active.insert(*id);
            } else if pr.is_witness {
                // Extended Raft: witness is a logical entity backed by etcd.
                // It is always considered active — its liveness does not depend
                // on network heartbeats like physical peers.
                pr.recent_active = true;
                active.insert(*id);
            } else if pr.recent_active {
                // It doesn't matter whether it's learner. As we calculate quorum
                // by actual ids instead of count.
                active.insert(*id);
                pr.recent_active = false;
            }
        }
        self.has_quorum(&active)
    }

    /// Determine if a quorum is formed from the given set of nodes.
    ///
    /// This is the only correct way to verify you have reached a quorum for the whole group.
    #[inline]
    pub fn has_quorum(&self, potential_quorum: &HashSet<u64>) -> bool {
        self.conf
            .voters
            .vote_result(|id| potential_quorum.get(&id).map(|_| true))
            == VoteResult::Won
    }

    #[inline]
    pub(crate) fn progress(&self) -> &ProgressMap {
        &self.progress
    }

    /// Applies configuration and updates progress map to match the configuration.
    pub fn apply_conf(&mut self, conf: Configuration, changes: MapChange, next_idx: u64) {
        self.conf = conf;
        for (id, change_type) in changes {
            match change_type {
                MapChangeType::Add => {
                    let mut pr = Progress::new(next_idx, self.max_inflight);
                    // When a node is first added, we should mark it as recently active.
                    // Otherwise, CheckQuorum may cause us to step down if it is invoked
                    // before the added node has had a chance to communicate with us.
                    pr.recent_active = true;
                    // Set witness flag if this node is a witness.
                    pr.is_witness = self.conf.witnesses[0] == id || self.conf.witnesses[1] == id;
                    self.progress.insert(id, pr);
                }
                MapChangeType::Remove => {
                    self.progress.remove(&id);
                }
            }
        }
    }

    /// Resets the replication set and optionally the subterm counter.
    /// Called when a new leader is elected (reset_subterm=true) or when
    /// a conf change is applied (reset_subterm=false, increment subterm).
    pub fn reset_replication_set(&mut self, reset_subterm: bool) {
        let mut epoch = Epoch::default();
        epoch.subterm = if reset_subterm {
            0
        } else {
            self.epoch.subterm + 1
        };

        for (i, voters) in [&self.conf.voters.incoming, &self.conf.voters.outgoing]
            .iter()
            .enumerate()
        {
            let set = &mut epoch.replication_sets[i];
            set.excluded = 0;
            set.witness = 0;
            let mut non_witness_count = 0u64;
            for &id in voters.ids() {
                if self.conf.witnesses[i] == id {
                    set.witness = id;
                } else {
                    set.non_witness_voters.insert(id);
                    non_witness_count += 1;
                }
            }
            // If the witness is configured but is NOT in the voter set
            // (e.g. 2 regular voters + 1 witness in production), set it
            // up here. The witness only participates in decision-making
            // when one regular voter is unreachable.
            if set.witness == 0 && self.conf.witnesses[i] != 0 {
                set.witness = self.conf.witnesses[i];
            }
            // Only exclude the witness from quorum if there are at least 2
            // non-witness voters. With only 1 non-witness voter, quorum is 2
            // (out of 2 voters), and the witness must be included to commit.
            // When excluded, the witness is contacted when q-1 non-witness
            // voters have acknowledged (a tie-breaker for 3+ voter groups).
            if set.witness != 0 && non_witness_count >= 2 {
                set.excluded = set.witness;
            } else if set.witness != 0 {
                // Include the witness as a regular voter for quorum.
                set.non_witness_voters.insert(set.witness);
            }
        }

        self.epoch = epoch;
    }

    /// Adjusts the replication set based on node liveness.
    /// The leader calls this to exclude inactive nodes or include recovered ones.
    /// Returns true if the replication set changed (caller should start a new subterm).
    /// Adjusts the replication set based on node liveness.
    ///
    /// Per the Extended Raft paper §2.4:
    /// 1. A new leader initializes its replication set to all regular servers
    ///    (`RegularServer`). The witness is outside the replication set.
    /// 2. If leader receives no response from a regular peer server over an
    ///    election timeout, it assumes the server is unreachable and adjusts
    ///    the replication set by swapping the unreachable server with the
    ///    witness (or another reachable regular server).
    /// 3. As a recovery path, when an excluded node becomes active again,
    ///    the leader swaps it back into the replication set, excluding an
    ///    inactive node instead (or clearing the exclusion).
    ///
    /// Returns true if the replication set changed (caller should start a
    /// new subterm and append an empty entry).
    pub fn change_replication_set(&mut self) -> bool {
        let old_epoch = &self.epoch;
        let mut new_epoch = Epoch {
            subterm: old_epoch.subterm + 1,
            ..Default::default()
        };
        let mut changed = false;

        for i in 0..2 {
            let set = &old_epoch.replication_sets[i];

            // ──────────────────────────────────────────────
            // Case 1: Recovery — an excluded node is ready
            // to come back into the replication set.
            // ──────────────────────────────────────────────
            if set.excluded != 0 {
                // Look up the excluded node. A regular voter has a progress
                // entry. A witness may not have one (witnesses run in external
                // storage). If not found, skip recovery and fall through to
                // the degradation check below.
                if let Some(excluded_pr) = self.progress.get(&set.excluded) {
                    let is_excluded_ready = excluded_pr.recent_active
                        && (set.excluded == set.witness
                            || excluded_pr.state == ProgressState::Replicate);

                    if is_excluded_ready {
                        // Find an inactive node to swap out.
                        let mut inactive_id = 0u64;
                        for (&id, pr) in &self.progress {
                            if !pr.recent_active
                                && (set.non_witness_voters.contains(&id) || id == set.witness)
                            {
                                inactive_id = id;
                                break;
                            }
                        }

                                                if inactive_id > 0 || set.excluded != set.witness {
                            // Build the new non-witness set starting from the
                            // current one.
                            let mut new_non_witness = set.non_witness_voters.clone();

                            if inactive_id > 0 {
                                // Swap: remove the inactive node.
                                new_non_witness.remove(&inactive_id);
                            } else {
                                // No inactive node to swap — the recovered
                                // node is a regular voter. Re-exclude the
                                // witness to restore steady state.
                                new_non_witness.remove(&set.witness);
                            }
                            // The old excluded node is now reachable and
                            // comes back into the replication set.
                            new_non_witness.insert(set.excluded);

                            new_epoch.replication_sets[i] = ReplicationSet {
                                witness: set.witness,
                                excluded: if inactive_id > 0 {
                                    inactive_id
                                } else {
                                    // Re-exclude witness (steady state).
                                    set.witness
                                },
                                non_witness_voters: new_non_witness,
                            };
                            changed = true;
                            continue;
                        }
                        // excluded is witness and no inactive found:
                        // fall through to degradation check below.
                    }
                }
                // If excluded node was not in progress (witness in
                // production) or not ready, fall through.
            }

            // ──────────────────────────────────────────────
            // Case 2: Degradation — the witness is outside the
            // replication set. If any non-witness voter inside
            // the set is unreachable, swap it with the witness.
            // ──────────────────────────────────────────────
            //
            // Per the paper §2.4: "If leader receives no response from
            // a regular peer server over an election timeout, it assumes
            // the regular server is unreachable and initiates a
            // replication set adjustment."
            if set.witness != 0 && !set.non_witness_voters.contains(&set.witness) {
                for &id in &set.non_witness_voters {
                    if let Some(pr) = self.progress.get(&id) {
                        if !pr.recent_active {
                            // Found unreachable regular voter.
                            // Swap it with the witness.
                            let mut new_non_witness = set.non_witness_voters.clone();
                            new_non_witness.remove(&id);
                            new_non_witness.insert(set.witness);

                            new_epoch.replication_sets[i] = ReplicationSet {
                                witness: set.witness,
                                excluded: id,
                                non_witness_voters: new_non_witness,
                            };
                            changed = true;
                            break;
                        }
                    }
                }
            }
        }

        if changed {
            self.epoch = new_epoch;
        }
        changed
    }

    /// For each witness that should receive shortcut replication, compute the
    /// committed index at quorum-1 within its replication set.
    /// Returns a map of witness_id → index.
    pub fn one_less_than_quorum_in_replication_set(&self) -> HashMap<u64, u64> {
        let (w0, w1) = self.epoch.replicate_to_witness();
        if !w0 && !w1 {
            return HashMap::default();
        }

        let mut result = HashMap::default();

        for (i, needs_witness) in [(0usize, w0), (1usize, w1)].iter() {
            if !needs_witness {
                continue;
            }
            let set = &self.epoch.replication_sets[*i];
            // Build a scoped indexer limited to non-witness voters in this set.
            let scope = &set.non_witness_voters;
            let scoped = ScopedAckIndexer {
                indexer: &self.progress,
                scope,
            };
            let idx = if *i == 0 {
                self.conf.voters.incoming.one_less_than_quorum(&scoped)
            } else {
                self.conf.voters.outgoing.one_less_than_quorum(&scoped)
            };
            result.insert(set.witness, idx);
        }

        result
    }

    /// Same as tally_votes but also returns how many more votes are needed
    /// to win in each config half: [incoming, outgoing].
    pub fn tally_votes_with_diff(&self) -> (usize, usize, VoteResult, [usize; 2]) {
        let (mut granted, mut rejected) = (0, 0);
        for (id, vote) in &self.votes {
            if !self.conf.voters.contains(*id) {
                continue;
            }
            if *vote {
                granted += 1;
            } else {
                rejected += 1;
            }
        }
        let (result, diff) = self
            .conf
            .voters
            .vote_result_with_diff(|id| self.votes.get(&id).cloned());
        (granted, rejected, result, diff)
    }
}

/// A wrapper around AckedIndexer that only reports acked indexes for
/// voters within a specific scope (replication set).
struct ScopedAckIndexer<'a> {
    indexer: &'a ProgressMap,
    scope: &'a HashSet<u64>,
}

impl<'a> AckedIndexer for ScopedAckIndexer<'a> {
    fn acked_index(&self, voter_id: u64) -> Option<Index> {
        if !self.scope.contains(&voter_id) {
            return None;
        }
        self.indexer.acked_index(voter_id)
    }
}
