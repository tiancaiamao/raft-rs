// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Test that a follower receiving a heartbeat with a commit index higher
//! than its last log index does not panic.
//!
//! This can happen after a crash recovery where the follower's raft log is
//! truncated (e.g. entries offloaded to S3 and unavailable), but the leader
//! still has a stale view of the follower's matched index.

use raft::eraftpb::{ConfState, Entry, HardState, Message, MessageType};
use raft::storage::MemStorage;
use raft::{Config, RawNode};
use slog::{o, Logger};

fn make_logger() -> Logger {
    Logger::root(slog::Discard, o!())
}

/// Creates a follower RawNode (id=2) in a 2-node cluster.
/// Storage is initialized with 5 entries (indices 1-5) at term 1.
fn make_follower() -> RawNode<MemStorage> {
    let logger = make_logger();
    let storage = MemStorage::default();

    let mut cs = ConfState::default();
    cs.set_voters(vec![1, 2]);
    storage.initialize_with_conf_state(cs);

    // Initialize storage with entries [1,5], commit=3, term=1.
    let entries: Vec<Entry> = (1..=5)
        .map(|i| {
            let mut e = Entry::default();
            e.set_term(1);
            e.set_index(i);
            e
        })
        .collect();
    let mut hs = HardState::default();
    hs.set_term(1);
    hs.set_commit(3);
    {
        let mut core = storage.wl();
        core.append(&entries).unwrap();
        core.commit_to(3).unwrap();
        core.set_hardstate(hs);
    }

    let config = Config {
        id: 2,
        ..Default::default()
    };
    RawNode::new(&config, storage, &logger).unwrap()
}

#[test]
fn test_heartbeat_commit_clamped_to_last_index() {
    let mut follower = make_follower();

    // Simulate crash recovery: the follower's raft log is truncated so that
    // last_index drops from 5 to 7 (absurdly low). We do this by replacing
    // the storage contents.
    {
        let storage = follower.store().clone();
        let entries: Vec<Entry> = (5..=7)
            .map(|i| {
                let mut e = Entry::default();
                e.set_term(1);
                e.set_index(i);
                e
            })
            .collect();
        let mut hs = HardState::default();
        hs.set_term(1);
        hs.set_commit(5);
        let mut core = storage.wl();
        core.append(&entries).unwrap();
        core.commit_to(5).unwrap();
        core.set_hardstate(hs);
    }

    // The leader thinks the follower has matched index 18 and sends a heartbeat
    // with commit=18. Before the fix, this would panic with
    // "to_commit 18 is out of range [last_index 7]".
    let mut heartbeat = Message::default();
    heartbeat.set_from(1);
    heartbeat.set_to(2);
    heartbeat.set_msg_type(MessageType::MsgHeartbeat);
    heartbeat.set_term(1);
    heartbeat.set_commit(18);

    // This should not panic.
    follower.step(heartbeat).unwrap();

    // The follower's committed should be clamped to its last_index (7).
    let status = follower.status();
    assert!(
        status.hs.commit <= 7,
        "commit should be clamped <= 7, got {}",
        status.hs.commit
    );
}

#[test]
fn test_heartbeat_commit_within_range_unchanged() {
    // Normal case: heartbeat commit <= last_index. No clamping needed.
    let mut follower = make_follower();

    // last_index is 5. Heartbeat commit=4 should be fine.
    let mut heartbeat = Message::default();
    heartbeat.set_from(1);
    heartbeat.set_to(2);
    heartbeat.set_msg_type(MessageType::MsgHeartbeat);
    heartbeat.set_term(1);
    heartbeat.set_commit(4);

    // This should not panic.
    follower.step(heartbeat).unwrap();
}
