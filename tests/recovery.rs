use raft::eraftpb::{Entry, HardState};
use raft::{GetEntriesContext, Storage};
use slatedb_etcd::state::StateMachine;
use slatedb_etcd::storage::RaftStorage;
use slatedb_etcd::{Command, Operation, Request};

fn entry(index: u64, term: u64) -> Entry {
    Entry {
        index,
        term,
        ..Entry::default()
    }
}

fn storage(dir: &std::path::Path) -> RaftStorage {
    RaftStorage::open(dir, 1, "test", vec![1, 2, 3]).unwrap()
}

#[test]
fn separate_logs_do_not_share_cached_entries() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let mut left = storage(left_dir.path());
    let mut right = storage(right_dir.path());
    let mut left_entry = entry(1, 1);
    left_entry.data = b"left".to_vec().into();
    let mut right_entry = entry(1, 2);
    right_entry.data = b"rght".to_vec().into();
    left.persist(&[left_entry.clone()], None, None).unwrap();
    right.persist(&[right_entry.clone()], None, None).unwrap();
    for _ in 0..3 {
        for (store, expected) in [(&left, &left_entry), (&right, &right_entry)] {
            let actual = store
                .entries(1, 2, None, GetEntriesContext::empty(false))
                .unwrap();
            assert_eq!(actual, vec![expected.clone()]);
            assert_eq!(store.term(1).unwrap(), expected.term);
        }
    }
    drop(left);
    drop(right);
    for (dir, expected) in [
        (left_dir.path(), left_entry),
        (right_dir.path(), right_entry),
    ] {
        let reopened = storage(dir);
        let actual = reopened
            .entries(1, 2, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(actual, vec![expected]);
    }
}

#[test]
fn commit_progress_skips_sync_but_entries_terms_and_votes_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = storage(dir.path());
    let mut hs = HardState {
        term: 1,
        vote: 1,
        ..Default::default()
    };
    store.persist(&[entry(1, 1)], Some(&hs), None).unwrap();
    assert_eq!(store.sync_writes, 1);
    hs.commit = 1;
    store.persist(&[], Some(&hs), None).unwrap();
    assert_eq!(store.sync_writes, 1);
    hs.term = 2;
    hs.vote = 0;
    store.persist(&[], Some(&hs), None).unwrap();
    assert_eq!(store.sync_writes, 2);
    hs.vote = 2;
    store.persist(&[], Some(&hs), None).unwrap();
    assert_eq!(store.sync_writes, 3);
    store.persist(&[entry(2, 2)], None, None).unwrap();
    assert_eq!(store.sync_writes, 4);
    store.persist_commit(2).unwrap();
    assert_eq!(store.sync_writes, 5);
    assert!(store.persist_commit(3).is_err());
    drop(store);
    let store = storage(dir.path());
    assert_eq!(store.initial_state().unwrap().hard_state.vote, 2);
    assert_eq!(store.commit_index(), 2);
    assert_eq!(store.last_index().unwrap(), 2);
}

#[test]
fn durable_log_replaces_an_uncommitted_suffix() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut store = storage(dir.path());
        let hs = HardState {
            term: 1,
            vote: 1,
            commit: 1,
            ..HardState::default()
        };
        store
            .persist(&[entry(1, 1), entry(2, 1), entry(3, 1)], Some(&hs), None)
            .unwrap();
        let hs = HardState {
            term: 2,
            vote: 2,
            commit: 2,
            ..HardState::default()
        };
        store.persist(&[entry(2, 2)], Some(&hs), None).unwrap();
    }
    let store = storage(dir.path());
    assert_eq!(store.commit_index(), 2);
    assert_eq!(store.last_index().unwrap(), 2);
    assert_eq!(store.term(2).unwrap(), 2);
    assert!(store.term(3).is_err());
    let entries = store
        .entries(1, 3, None, GetEntriesContext::empty(false))
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(store.initial_state().unwrap().hard_state.vote, 2);
}

#[test]
fn checkpoint_keeps_its_boundary_term_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut store = storage(dir.path());
        let hs = HardState {
            term: 2,
            vote: 1,
            commit: 3,
            ..HardState::default()
        };
        store
            .persist(&[entry(1, 1), entry(2, 2), entry(3, 2)], Some(&hs), None)
            .unwrap();
        assert!(store.checkpoint(4, vec![]).is_err());
        store.checkpoint(2, b"full-state-at-2".to_vec()).unwrap();
    }
    let store = storage(dir.path());
    assert_eq!(store.first_index().unwrap(), 3);
    assert_eq!(store.term(2).unwrap(), 2);
    assert_eq!(store.last_index().unwrap(), 3);
    assert!(store.term(1).is_err());
    assert_eq!(store.snapshot(2, 3).unwrap().get_data(), b"full-state-at-2");
    assert!(store.snapshot(3, 3).is_err());
    assert_eq!(
        store
            .entries(3, 4, Some(0), GetEntriesContext::empty(false))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn node_identity_cannot_be_reused() {
    let dir = tempfile::tempdir().unwrap();
    drop(storage(dir.path()));
    assert!(RaftStorage::open(dir.path(), 2, "test", vec![1, 2, 3]).is_err());
    assert!(RaftStorage::open(dir.path(), 1, "other", vec![1, 2, 3]).is_err());
}

#[tokio::test]
async fn restart_finishes_a_snapshot_persisted_before_state_installation() {
    let source = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(&source.path().join("objects"))
        .await
        .unwrap();
    state
        .apply(
            1,
            Some(&Command {
                id: "one".to_owned(),
                request: Request {
                    operations: vec![Operation::Put {
                        key: "key".to_owned(),
                        value: "snapshot-value".to_owned(),
                    }],
                },
            }),
        )
        .await
        .unwrap();
    let data = state.snapshot_data().await.unwrap();
    let mut source_store = storage(&source.path().join("raft"));
    let hs = HardState {
        term: 1,
        vote: 1,
        commit: 1,
        ..HardState::default()
    };
    source_store
        .persist(&[entry(1, 1)], Some(&hs), None)
        .unwrap();
    source_store.checkpoint(1, data).unwrap();
    let snapshot = source_store.recovery_snapshot().clone();
    {
        let mut store = storage(&destination.path().join("raft"));
        // Stop after saving the received snapshot. Do not update SlateDB.
        store.persist(&[], Some(&hs), Some(&snapshot)).unwrap();
    }
    let store = storage(&destination.path().join("raft"));
    let mut restored = StateMachine::open(&destination.path().join("objects"))
        .await
        .unwrap();
    assert_eq!(restored.applied, 0);
    restored
        .install_snapshot(store.recovery_snapshot())
        .await
        .unwrap();
    assert_eq!(restored.applied, 1);
    assert_eq!(
        restored.local_value("key").await.unwrap().as_deref(),
        Some("snapshot-value")
    );
    restored
        .install_snapshot(store.recovery_snapshot())
        .await
        .unwrap();
    assert_eq!(restored.applied, 1);
}

#[tokio::test]
async fn background_snapshot_excludes_later_writes() {
    let source = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(&source.path().join("objects"))
        .await
        .unwrap();
    let put = |value: &str| Command {
        id: value.to_owned(),
        request: Request {
            operations: vec![Operation::Put {
                key: "key".to_owned(),
                value: value.to_owned(),
            }],
        },
    };
    state.apply(1, Some(&put("before"))).await.unwrap();
    let job = state.snapshot_job().unwrap();
    state.apply(2, Some(&put("after"))).await.unwrap();
    let data = job.await.unwrap();
    let mut store = storage(&source.path().join("raft"));
    let hs = HardState {
        term: 1,
        vote: 1,
        commit: 2,
        ..HardState::default()
    };
    store
        .persist(&[entry(1, 1), entry(2, 1)], Some(&hs), None)
        .unwrap();
    store.checkpoint(1, data).unwrap();
    let mut restored = StateMachine::open(&destination.path().join("objects"))
        .await
        .unwrap();
    restored
        .install_snapshot(store.recovery_snapshot())
        .await
        .unwrap();
    assert_eq!(restored.applied, 1);
    assert_eq!(
        restored.local_value("key").await.unwrap().as_deref(),
        Some("before")
    );
    restored.apply(2, Some(&put("after"))).await.unwrap();
    assert_eq!(
        restored.local_value("key").await.unwrap().as_deref(),
        Some("after")
    );
}

#[test]
fn checkpoint_recovery_requires_the_referenced_bytes_and_discards_only_orphans() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let current = root.join("checkpoint-0000000000000002.data");
    let older = root.join("checkpoint-0000000000000001.data");
    let future = root.join("checkpoint-0000000000000003.data");
    let pending = root.join("checkpoint.pending");
    let unrelated = root.join("checkpoint-manual.data");
    {
        let mut store = storage(root);
        let hs = HardState {
            term: 1,
            vote: 1,
            commit: 2,
            ..Default::default()
        };
        store
            .persist(&[entry(1, 1), entry(2, 1)], Some(&hs), None)
            .unwrap();
        store.checkpoint(1, b"old".to_vec()).unwrap();
        store.checkpoint(2, b"new".to_vec()).unwrap();
        assert!(!older.exists());
    }
    for path in [&older, &future, &pending, &unrelated] {
        std::fs::write(path, b"orphan").unwrap();
    }
    for bad in [Some(b"bad".as_slice()), Some(b"shorter".as_slice()), None] {
        match bad {
            Some(data) => std::fs::write(&current, data).unwrap(),
            None => std::fs::remove_file(&current).unwrap(),
        }
        assert!(RaftStorage::open(root, 1, "test", vec![1, 2, 3]).is_err());
        assert!(older.exists() && future.exists());
    }
    std::fs::write(&current, b"new").unwrap();
    let store = storage(root);
    assert_eq!(store.recovery_snapshot().data.as_ref(), b"new");
    assert!(current.exists() && unrelated.exists());
    assert!(!older.exists() && !future.exists() && !pending.exists());
}
