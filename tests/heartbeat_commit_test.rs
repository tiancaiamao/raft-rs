// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Test that a follower receiving a heartbeat does not advance its
//! committed index.
//!
//! A heartbeat is a liveness signal, not a commit signal. The leader may
//! hold entries the follower has not matched (e.g. after a divergent branch
//! of the log, or while the follower is excluded from the leader's
//! replication set). Advancing committed from a heartbeat without log
//! matching would corrupt the follower's committed index permanently: it
//! never regresses, so a later overwrite of those entries leaves committed
//! pointing past the actual log and blocks snapshot catch-up.

use raft::eraftpb::{ConfState, Entry, HardState, Message, MessageType};
use raft::storage::MemStorage;
use raft::{Config, RawNode};
use slog::{o, Logger};

fn make_logger() -> Logger {
    Logger::root(slog::Discard, o!())
}

/// Creates a follower RawNode (id=2) in a 2-node cluster.
/// Storage is initialized with 5 entries (indices 1-5) at term 1,
/// committed = 3.
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

fn heartbeat(from: u64, to: u64, term: u64, commit: u64) -> Message {
    let mut m = Message::default();
    m.set_from(from);
    m.set_to(to);
    m.set_msg_type(MessageType::MsgHeartbeat);
    m.set_term(term);
    m.set_commit(commit);
    m
}

#[test]
fn test_heartbeat_does_not_advance_committed() {
    let mut follower = make_follower(); // committed=3, last_index=5
    let committed_before = follower.status().hs.commit;

    // The leader believes the follower has matched index 18 and sends a
    // heartbeat with commit=18, far above the follower's last_index=5.
    // A heartbeat must not move the follower's committed index: the
    // follower never matched 4..18 via append. Before the fix, this would
    // both panic ("to_commit 18 is out of range") and, when clamped,
    // advance committed past the matched log.
    follower
        .step(heartbeat(1, 2, 1, 18))
        .expect("heartbeat must not panic");

    assert_eq!(
        follower.status().hs.commit,
        committed_before,
        "HB_TEST_V2 heartbeat must not advance committed"
    );
}

#[test]
fn test_heartbeat_committed_unchanged_within_range() {
    // Normal case: heartbeat commit within the follower's log range.
    // Still no advancement — committed only moves via the append path.
    let mut follower = make_follower(); // committed=3, last_index=5
    let committed_before = follower.status().hs.commit;

    follower
        .step(heartbeat(1, 2, 1, 4))
        .expect("heartbeat must not panic");

    assert_eq!(
        follower.status().hs.commit,
        committed_before,
        "HB_TEST_V2 heartbeat must not advance committed"
    );
}