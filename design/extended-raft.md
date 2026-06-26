# Extended Raft Implementation Plan (raft-rs)

Reference: [etcd-io/raft PR #168](https://github.com/etcd-io/raft/pull/168)

## Overview

Extended Raft adds **witness** support to raft-rs, enabling 2F1A (2 fire + 1 available) region
configurations. A witness is a special voter that participates in elections and commits via a
shortcut replication path, but does not run a full Raft instance. In CSE, the witness is backed
by S3/object storage.

## Key Concepts

1. **Witness**: A voter node whose state is stored externally (S3). It votes in elections and
   receives log entries via shortcut replication. It does not run RaftCore — it's a passive
   responder.

2. **Subterm**: A monotonically increasing counter within a term. Incremented when:
   - A new leader is elected (new term → reset to 0 or new subterm)
   - A conf change is applied
   - The replication set is adjusted (leader unilateral decision based on node liveness)

3. **Replication Set**: The set of nodes the leader replicates to, within a subterm. It can
   exclude one node per subterm (the witness or an inactive voter). The leader adjusts it
   based on `RecentActive` status.

4. **Shortcut Replication**: When q-1 voters have acked an entry, the leader sends it to the
   witness. This is the key optimization — witness is contacted at most once per subterm.

5. **Witness Vote**: A candidate needs q-1 regular votes before asking the witness to vote.
   The witness votes if `(term, subterm, lastLogIndex, lastLogTerm)` is up-to-date AND the
   candidate's votesGranted ⊆ replicationSet.

## Changes by Layer

### 1. Proto (`proto/proto/eraftpb.proto`)

- Add `uint64 subterm = 7` to `Entry` (tag 7, after `context = 6`)
- Add `uint64 witness = 6` and `uint64 witness_outgoing = 7` to `ConfState`
- Add `ConfChangeAddWitness = 3` to `ConfChangeType`
- Add `WitnessHardState` message (for witness persistence)
- Add `WitnessMessage` message (separate from regular `Message`, carries replication set info)

### 2. Quorum (`src/quorum/majority.rs`, `src/quorum/joint.rs`)

- `MajorityConfig::one_less_than_quorum()` — committed index at position q-1
- `MajorityConfig::vote_result_with_diff()` — returns VoteResult + how many more votes needed
- `JointConfig::one_less_than_quorum()` — min of both halves
- `JointConfig::vote_result_with_diff()` — returns VoteResult + [votes_needed_in, votes_needed_out]

### 3. Tracker (`src/tracker.rs`, `src/tracker/progress.rs`)

- `Configuration`: add `witnesses: [u64; 2]` (incoming, outgoing)
- `Progress`: add `is_witness: bool`
- New `ReplicationSet` struct: `{ witness, excluded, non_witness_voters }`
- New `Epoch` struct: `{ subterm, replication_sets: [ReplicationSet; 2] }`
- `ProgressTracker`: add `epoch: Option<Epoch>`
- New methods: `reset_replication_set()`, `change_replication_set()`,
  `one_less_than_quorum_in_replication_set()`, `tally_votes_with_difference()`

### 4. Raft Core (`src/raft.rs`)

- `Raft<T>`: add `witness_msgs: Vec<WitnessMessage>`
- `append_entry()`: stamp entries with `epoch.subterm`
- `become_leader()`: call `maybe_start_new_subterm(true, false)` instead of appending empty entry
- `campaign()`: skip witnesses in initial vote broadcast
- `poll()` → `poll_and_report_diff()`: return votes still needed
- `step_candidate()`: when VotePending, check if ready to ask witness, send witness vote request
- `step_leader()`: skip witnesses for MsgTransferLeader
- `maybe_commit()`: after regular commit check, find q-1 acks in replication set → send to witness
- `maybe_send_append()`: skip witnesses (they don't receive regular AppEntries)
- `bcast_heartbeat()`: send witness heartbeats to witness peers
- New methods: `maybe_start_new_subterm()`, `send_append_to_witness()`,
  `send_request_vote_to_witness()`, `send_heartbeat_to_witness()`,
  `get_witness_vote_request_readiness()`
- `apply_conf_change()`: if leader, call `maybe_start_new_subterm(false, true)`

### 5. RawNode (`src/raw_node.rs`)

- `Ready`: add `witness_messages: Vec<WitnessMessage>`
- `HasReady()`: check `witness_msgs`
- `accept_ready()`: clear witness_msgs

### 6. Witness Module (`src/witness.rs`)

- `Witness` struct: processes WitnessMessages from leader
- `WitnessStorage` trait: `save()`, `load()`, `conditional_save()`
- `process()`: handles MsgApp, MsgVote, MsgPreVote, MsgHeartbeat
- Vote logic: check up-to-date + votesGranted ⊆ replicationSet

### 7. ConfChange (`src/confchange/`)

- Handle `ConfChangeAddWitness` type
- Witness is treated like a voter for quorum but with `is_witness` flag

## Implementation Order

1. ✅ Proto changes
2. ✅ Quorum layer (one_less_than_quorum, vote_result_with_diff)
3. ✅ Tracker layer (ReplicationSet, Epoch, progress fields)
4. ✅ Raft core (subterm stamping, witness msgs, vote logic, shortcut replication)
5. ✅ RawNode (Ready.witness_messages)
6. ✅ Witness module
7. ✅ ConfChange handling
8. ✅ Tests

## Compatibility

- No witness configured = standard Raft (subterm stays 0, replication set = all voters)
- `subterm` field defaults to 0 (proto3 default), backward compatible on wire
- Existing tests must pass without modification (when no witness is configured)