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
    left_entry.data = b"left".to_vec();
    let mut right_entry = entry(1, 2);
    right_entry.data = b"rght".to_vec();
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
        };
        store
            .persist(&[entry(1, 1), entry(2, 1), entry(3, 1)], Some(&hs), None)
            .unwrap();
        let hs = HardState {
            term: 2,
            vote: 2,
            commit: 2,
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
    assert_eq!(store.recovery_snapshot().data.as_slice(), b"new");
    assert!(current.exists() && unrelated.exists());
    assert!(!older.exists() && !future.exists() && !pending.exists());
}

#[test]
fn checkpoint_reopens_plain_and_compressed_images_and_rejects_damage() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut bits = 42_u64;
    let random: Vec<u8> = (0..16_384)
        .flat_map(|_| {
            bits ^= bits << 13;
            bits ^= bits >> 7;
            bits ^= bits << 17;
            bits.to_le_bytes()
        })
        .collect();
    let repeated = vec![0x31; random.len()];
    let path = |index| root.join(format!("checkpoint-{index:016x}.data"));
    let mut store = storage(root);
    let hs = HardState {
        term: 1,
        vote: 1,
        commit: 2,
    };
    store
        .persist(&[entry(1, 1), entry(2, 1)], Some(&hs), None)
        .unwrap();
    store.checkpoint(1, random.clone()).unwrap();
    drop(store);
    assert_eq!(std::fs::read(path(1)).unwrap(), random);
    let mut store = storage(root);
    assert_eq!(store.recovery_snapshot().data.as_ref(), random);
    store.checkpoint(2, repeated.clone()).unwrap();
    drop(store);
    let original = std::fs::read(path(2)).unwrap();
    assert!(original.len() < repeated.len() / 2);
    assert_eq!(storage(root).recovery_snapshot().data.as_ref(), repeated);

    std::fs::write(path(2), &original[..original.len() - 1]).unwrap();
    assert!(RaftStorage::open(root, 1, "test", vec![1, 2, 3]).is_err());
    let mut damaged = original.clone();
    damaged[0] ^= 1;
    std::fs::write(path(2), damaged).unwrap();
    assert!(RaftStorage::open(root, 1, "test", vec![1, 2, 3]).is_err());
    std::fs::write(path(2), original).unwrap();
    assert_eq!(storage(root).recovery_snapshot().data.as_ref(), repeated);
}

#[test]
fn prost_reads_and_writes_the_previous_raft_wire_format() {
    use prost::Message as _;
    use raft::eraftpb::{Entry, EntryType, Message, MessageType, Snapshot};

    // Raft 0.7.0 produced these bytes with rust-protobuf 2.28.
    let snapshot = [
        10, 4, 115, 110, 97, 112, 18, 15, 10, 8, 8, 1, 8, 2, 8, 3, 16, 4, 16, 129, 1, 24, 3,
    ];
    let state = Snapshot::decode(snapshot.as_slice()).unwrap();
    assert_eq!(state.data.as_slice(), b"snap");
    let metadata = state.get_metadata();
    assert_eq!((metadata.term, metadata.index), (3, 129));
    assert_eq!(metadata.get_conf_state().voters, [1, 2, 3]);
    assert_eq!(metadata.get_conf_state().learners, [4]);
    // Raft 0.7.0 wrote unpacked node IDs. Upstream Prost writes packed IDs, and both decoders
    // accept either form.
    let packed = state.encode_to_vec();
    assert_ne!(packed, snapshot);
    assert_eq!(Snapshot::decode(packed.as_slice()).unwrap(), state);

    let entry = [
        8, 1, 16, 3, 24, 130, 1, 34, 3, 108, 111, 103, 50, 2, 105, 100,
    ];
    let record = Entry::decode(entry.as_slice()).unwrap();
    assert_eq!(record.entry_type(), EntryType::EntryConfChange);
    assert_eq!((record.term, record.index), (3, 130));
    assert_eq!(record.data.as_slice(), b"log");
    assert_eq!(record.context.as_slice(), b"id");
    assert_eq!(record.encode_to_vec(), entry);

    let append = [
        8, 3, 16, 2, 24, 1, 58, 16, 8, 1, 16, 3, 24, 130, 1, 34, 3, 108, 111, 103, 50, 2, 105, 100,
    ];
    let msg = Message::decode(append.as_slice()).unwrap();
    assert_eq!(msg.msg_type(), MessageType::MsgAppend);
    assert_eq!((msg.from, msg.to), (1, 2));
    assert_eq!(msg.entries, [record]);
    assert_eq!(msg.encode_to_vec(), append);
}
