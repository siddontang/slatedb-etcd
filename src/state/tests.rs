use super::*;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::{DeleteRangeRequest, PutRequest, RangeRequest};

fn put(key: &[u8], value: &[u8]) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestPut(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        })),
    }
}

async fn read(state: &StateMachine, key: &[u8], revision: i64) -> Outcome {
    state
        .read_etcd(&RequestOp {
            request: Some(Op::RequestRange(RangeRequest {
                key: key.to_vec(),
                revision,
                ..Default::default()
            })),
        })
        .await
        .unwrap()
}

async fn history_len(state: &StateMachine) -> usize {
    let mut scan = state.db.scan_prefix(b"etcd/history/", ..).await.unwrap();
    let mut count = 0;
    while scan.next().await.unwrap().is_some() {
        count += 1;
    }
    count
}

#[test]
fn native_snapshot_bytes_use_fixed_width_lengths() {
    let bytes = hex::decode(concat!(
        "03000000",         // Format 3.
        "2a00000000000000", // Applied index 42.
        "0100000000000000", // One row.
        "01000000000000006b",
        "0200000000000000007f",
    ))
    .unwrap();
    let data: SnapshotData = crate::codec::decode(&bytes).unwrap();
    assert_eq!(data.version, crate::STORAGE_FORMAT);
    assert_eq!(data.applied, 42);
    assert_eq!(data.rows, vec![(vec![b'k'], vec![0, 127])]);
    assert_eq!(crate::codec::encode(&data).unwrap(), bytes);
}

#[tokio::test]
async fn read_path_matches_transaction_evaluation_without_a_write_batch() {
    use crate::proto::etcdserverpb::TxnRequest;
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    for (index, request) in [put(b"a", b"first"), put(b"b", b"second"), put(b"a", b"new")]
        .iter()
        .enumerate()
    {
        state
            .apply_etcd(index as u64 + 1, request)
            .await
            .unwrap()
            .result
            .unwrap();
    }
    state
        .apply_compaction(
            4,
            &CompactionRequest {
                revision: 3,
                physical: false,
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    let applied = state.applied;
    for key in [b"a".to_vec(), b"missing".to_vec()] {
        for end in [vec![], vec![0]] {
            for revision in [-1, 0, 2, 3, 4, 5] {
                for mode in 0..4 {
                    let request = RequestOp {
                        request: Some(Op::RequestRange(RangeRequest {
                            key: key.clone(),
                            range_end: end.clone(),
                            revision,
                            keys_only: mode == 1,
                            count_only: mode == 2,
                            min_mod_revision: if mode == 3 { 4 } else { 0 },
                            limit: 1,
                            sort_order: 2,
                            sort_target: 4,
                            ..Default::default()
                        })),
                    };
                    for request in [
                        request.clone(),
                        RequestOp {
                            request: Some(Op::RequestTxn(TxnRequest {
                                success: vec![request],
                                ..Default::default()
                            })),
                        },
                    ] {
                        let expected = mvcc::prepare(
                            state.db.as_ref(),
                            state.revision,
                            state.compact_revision,
                            &request,
                            &state.leases,
                        )
                        .await
                        .map(|(response, _, _)| response);
                        let actual = state.read_etcd(&request).await.unwrap().result;
                        assert_eq!(
                            serde_json::to_value(actual).unwrap(),
                            serde_json::to_value(expected).unwrap()
                        );
                    }
                }
            }
        }
    }
    assert!(state
        .read_etcd(&put(b"a", b"forbidden"))
        .await
        .unwrap()
        .result
        .is_err());
    assert_eq!(state.applied, applied);
    state.close().await.unwrap();
}

#[tokio::test]
async fn ordered_range_keeps_the_total_count_after_the_response_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    for (index, key) in [b"c", b"a", b"b"].into_iter().enumerate() {
        state
            .apply_etcd(index as u64 + 1, &put(key, b"value"))
            .await
            .unwrap()
            .result
            .unwrap();
    }
    for (limit, keys_only, count_only, expected, more) in [
        (1, false, false, 1, true),
        (3, true, false, 3, false),
        (1, false, true, 0, false),
    ] {
        let op = RequestOp {
            request: Some(Op::RequestRange(RangeRequest {
                key: vec![0],
                range_end: vec![0],
                limit,
                keys_only,
                count_only,
                ..Default::default()
            })),
        };
        let result = state.read_etcd(&op).await.unwrap().result.unwrap().response;
        let Some(Resp::ResponseRange(result)) = result else {
            panic!("expected range")
        };
        assert_eq!(
            (result.count, result.kvs.len(), result.more),
            (3, expected, more)
        );
        for (row, key) in result.kvs.iter().zip([b"a", b"b", b"c"]) {
            assert_eq!(&row.key, key);
            assert_eq!(row.value.is_empty(), keys_only);
        }
    }
    state.close().await.unwrap();
}

#[tokio::test]
async fn compaction_removes_history_and_preserves_boundary_versions() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    let delete = RequestOp {
        request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
            key: b"a".to_vec(),
            ..Default::default()
        })),
    };
    for (index, request) in [
        put(b"a", b"first"),
        put(b"b", b"stable"),
        put(b"a", b"second"),
        delete,
        put(b"a", b"third"),
        put(b"a", b"fourth"),
    ]
    .iter()
    .enumerate()
    {
        state
            .apply_etcd(index as u64 + 1, request)
            .await
            .unwrap()
            .result
            .unwrap();
    }
    assert_eq!(history_len(&state).await, 6);
    state
        .apply_compaction(
            state.applied + 1,
            &CompactionRequest {
                revision: 5,
                physical: false,
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    // Compact applies the boundary first. Later steps remove history.
    assert_eq!(history_len(&state).await, 6);
    super::compaction_tests::finish(&mut state).await;
    assert_eq!(history_len(&state).await, 4);
    let deleted = read(&state, b"a", 5).await.result.unwrap();
    assert!(matches!(deleted.response, Some(Resp::ResponseRange(r)) if r.kvs.is_empty()));
    let stable = read(&state, b"b", 5).await.result.unwrap();
    assert!(matches!(stable.response, Some(Resp::ResponseRange(r))
        if r.kvs[0].value == b"stable" && r.kvs[0].mod_revision == 3));
    let compacted = read(&state, b"a", 4).await.result.unwrap_err();
    assert_eq!(compacted.code, tonic::Code::OutOfRange as i32);
    let future = state
        .apply_compaction(
            state.applied + 1,
            &CompactionRequest {
                revision: 8,
                physical: false,
            },
        )
        .await
        .unwrap();
    assert!(future.result.is_err());
    assert_eq!(state.compact_revision, 5);
    assert_eq!(history_len(&state).await, 4);
    state
        .apply_compaction(
            state.applied + 1,
            &CompactionRequest {
                revision: 7,
                physical: false,
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    assert_eq!(state.revision, 7);
    super::compaction_tests::finish(&mut state).await;
    assert_eq!(history_len(&state).await, 2);
    let current = read(&state, b"a", 7).await.result.unwrap();
    assert!(matches!(current.response, Some(Resp::ResponseRange(r))
        if r.kvs[0].value == b"fourth" && r.kvs[0].create_revision == 6));
    state.db.flush().await.unwrap();
    state.db.close().await.unwrap();
    drop(state);
    let state = StateMachine::open(dir.path()).await.unwrap();
    assert_eq!(
        (
            state.revision,
            state.compact_revision,
            state.physical_revision()
        ),
        (7, 7, 7)
    );
    assert_eq!(history_len(&state).await, 2);
    assert!(read(&state, b"a", 6).await.result.is_err());
    assert!(read(&state, b"a", 7).await.result.is_ok());
}

#[tokio::test]
async fn streamed_snapshot_matches_the_complete_state_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    for index in 1..=100 {
        state
            .apply_etcd(index, &put(&index.to_be_bytes(), &[0, 255, 42]))
            .await
            .unwrap()
            .result
            .unwrap();
    }
    let mut rows = vec![];
    let mut scan = state.db.scan_prefix(b"", ..).await.unwrap();
    while let Some(row) = scan.next().await.unwrap() {
        // The image omits the term row. Install takes the term from the Raft snapshot.
        if row.key.as_ref() != crate::cluster::KEY && row.key.as_ref() != APPLIED_TERM {
            rows.push((row.key.to_vec(), row.value.to_vec()));
        }
    }
    rows.push((
        crate::cluster::KEY.to_vec(),
        serde_json::to_vec(state.members.as_ref()).unwrap(),
    ));
    assert!(rows.len() > 255);
    let expected = crate::codec::encode(&SnapshotData {
        version: crate::SNAPSHOT_FORMAT,
        applied: state.applied,
        rows,
    })
    .unwrap();
    assert_eq!(state.snapshot_job().unwrap().await.unwrap(), expected);
    state.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_keeps_the_boundary_at_its_pinned_apply_position() {
    let dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    state
        .apply_etcd(1, &put(b"key", b"first"))
        .await
        .unwrap()
        .result
        .unwrap();
    state
        .apply_etcd(2, &put(b"key", b"second"))
        .await
        .unwrap()
        .result
        .unwrap();
    state
        .apply_compaction(
            3,
            &CompactionRequest {
                revision: 2,
                physical: false,
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    let snapshot_job = state.snapshot_job().unwrap();
    state
        .apply_compaction(
            4,
            &CompactionRequest {
                revision: 3,
                physical: false,
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    super::compaction_tests::finish(&mut state).await;
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 3;
    snapshot.data = snapshot_job.await.unwrap();
    let mut restored = StateMachine::open(other.path()).await.unwrap();
    restored.install_snapshot(&snapshot).await.unwrap();
    assert_eq!((restored.applied, restored.compact_revision), (3, 2));
    assert_eq!(history_len(&restored).await, 2);
    assert_eq!(history_len(&state).await, 1);
    assert!(read(&restored, b"key", 2).await.result.is_ok());
    assert!(read(&state, b"key", 2).await.result.is_err());
    restored
        .apply_compaction(
            4,
            &CompactionRequest {
                revision: 3,
                physical: false,
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    super::compaction_tests::finish(&mut restored).await;
    assert_eq!(history_len(&restored).await, 1);
    assert!(read(&restored, b"key", 2).await.result.is_err());
}

#[tokio::test]
async fn renewed_lease_and_watch_history_survive_snapshot_and_stale_expiry() {
    use crate::lease::LeaseCommand;
    use crate::proto::etcdserverpb::{TxnRequest, WatchCreateRequest};
    let dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    state
        .apply_lease(1, &LeaseCommand::Grant { id: 7, ttl: 30 })
        .await
        .unwrap()
        .result
        .unwrap();
    let mut writes = vec![];
    for key in [b"z", b"a"] {
        let mut request = put(key, key);
        if let Some(Op::RequestPut(p)) = &mut request.request {
            p.lease = 7;
        }
        writes.push(request);
    }
    state
        .apply_etcd(
            2,
            &RequestOp {
                request: Some(Op::RequestTxn(TxnRequest {
                    success: writes,
                    ..Default::default()
                })),
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    state
        .apply_lease(3, &LeaseCommand::KeepAlive { id: 7 })
        .await
        .unwrap()
        .result
        .unwrap();
    super::compaction_tests::finish(&mut state).await;
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 3;
    snapshot.data = state.snapshot_data().await.unwrap();
    let mut restored = StateMachine::open(other.path()).await.unwrap();
    restored.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(restored.leases[&7].generation, 3);
    restored
        .apply_lease(
            4,
            &LeaseCommand::Expire {
                id: 7,
                generation: 1,
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    assert_eq!(restored.revision, 2);
    assert!(restored.leases.contains_key(&7));
    restored
        .apply_lease(
            5,
            &LeaseCommand::Expire {
                id: 7,
                generation: 3,
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    assert_eq!(restored.revision, 3);
    assert!(restored.leases.is_empty());
    let request = WatchCreateRequest {
        key: vec![0],
        range_end: vec![0],
        start_revision: 1,
        prev_kv: true,
        ..Default::default()
    };
    let page = restored.watch(&request, None).await.unwrap();
    assert_eq!(page.responses.len(), 2);
    let put_events = &page.responses[0].events;
    assert_eq!(put_events[0].kv.as_ref().unwrap().key, b"z");
    assert_eq!(put_events[1].kv.as_ref().unwrap().key, b"a");
    let deletes = &page.responses[1].events;
    assert_eq!(deletes.len(), 2);
    for event in deletes {
        assert_eq!(event.r#type, 1);
        assert_eq!(event.kv.as_ref().unwrap().mod_revision, 3);
        assert_eq!(event.prev_kv.as_ref().unwrap().lease, 7);
        let key = &event.kv.as_ref().unwrap().key;
        assert!(
            matches!(read(&restored, key, 0).await.result.unwrap().response,
            Some(Resp::ResponseRange(r)) if r.kvs.is_empty())
        );
    }
    restored.db.flush().await.unwrap();
    restored.db.close().await.unwrap();
    drop(restored);
    let restored = StateMachine::open(other.path()).await.unwrap();
    assert_eq!(restored.applied, 5);
    assert!(restored.leases.is_empty());
    assert_eq!(
        restored.watch(&request, None).await.unwrap().responses,
        page.responses
    );
}

#[tokio::test]
async fn lease_revoke_deletes_more_keys_than_the_user_transaction_limit() {
    use crate::lease::LeaseCommand;
    use crate::proto::etcdserverpb::WatchCreateRequest;
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    state
        .apply_lease(1, &LeaseCommand::Grant { id: 9, ttl: 30 })
        .await
        .unwrap()
        .result
        .unwrap();
    for n in 0..130 {
        let mut request = put(format!("key-{n}").as_bytes(), b"value");
        if let Some(Op::RequestPut(p)) = &mut request.request {
            p.lease = 9;
        }
        state
            .apply_etcd(n + 2, &request)
            .await
            .unwrap()
            .result
            .unwrap();
    }
    state
        .apply_lease(132, &LeaseCommand::Revoke { id: 9 })
        .await
        .unwrap()
        .result
        .unwrap();
    let page = state
        .watch(
            &WatchCreateRequest {
                key: vec![0],
                range_end: vec![0],
                start_revision: 132,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(page.responses.len(), 1);
    assert_eq!(page.responses[0].events.len(), 130);
    assert!(crate::mvcc::lease_keys(state.db.as_ref(), 9)
        .await
        .unwrap()
        .is_empty());
}

#[test]
fn flush_cache_range_covers_only_current_rows() {
    let range = crate::mvcc::CURRENT..CURRENT_END;
    for key in [b"".as_slice(), b"\x00", b"\xff\xff", b"a/b"] {
        assert!(range.contains(&[crate::mvcc::CURRENT, key].concat().as_slice()));
    }
    for key in [
        crate::mvcc::HISTORY,
        crate::watch::EVENTS,
        crate::mvcc::REVISION,
        APPLIED,
    ] {
        assert!(!range.contains(&key));
    }
}

type SnapshotRows = Vec<(Vec<u8>, Vec<u8>)>;

/// Rewrite format 4 rows into format 3: full hash values and inline Watch journal values.
fn format_3_rows(rows: SnapshotRows) -> (SnapshotRows, usize) {
    use crate::proto::mvccpb::KeyValue;
    use crate::proto::state::{Journal, Version};
    use prost::Message;
    let history: std::collections::HashMap<Vec<u8>, KeyValue> = rows
        .iter()
        .filter(|(key, _)| key.starts_with(mvcc::HISTORY))
        .filter_map(|(key, value)| {
            let kv = Version::decode(value.as_slice()).unwrap().value?;
            Some((key.clone(), kv))
        })
        .collect();
    let full = |key: &[u8], revision: i64| history[&mvcc::history_key(key, revision)].clone();
    let mut rewritten = 0;
    let rows = rows
        .into_iter()
        .map(|(key, value)| {
            let hash = crate::maintenance::HASH;
            if key.starts_with(hash) && key.len() == hash.len() + 17 {
                let user_key = crate::state::maintenance::hash_record_key(&value).unwrap();
                let revision =
                    i64::from_be_bytes(key[hash.len()..hash.len() + 8].try_into().unwrap());
                let old = full(&user_key, revision).encode_to_vec();
                rewritten += usize::from(old != value);
                return (key, old);
            }
            if key.starts_with(crate::watch::EVENTS) {
                let mut journal = Journal::decode(value.as_slice()).unwrap();
                if journal.values_in_history {
                    for event in journal.events.iter_mut().filter(|e| e.r#type == 0) {
                        let kv = event.kv.as_ref().unwrap();
                        event.kv = Some(full(&kv.key, kv.mod_revision));
                    }
                    journal.values_in_history = false;
                    rewritten += 1;
                    return (key, journal.encode_to_vec());
                }
            }
            (key, value)
        })
        .collect();
    (rows, rewritten)
}

#[tokio::test]
async fn a_format_3_snapshot_installs_and_older_readers_reject_format_4() {
    use crate::proto::etcdserverpb::WatchCreateRequest;
    let dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    let big = vec![b'v'; 16 << 10];
    let txn = RequestOp {
        request: Some(Op::RequestTxn(crate::proto::etcdserverpb::TxnRequest {
            success: vec![put(b"c", &big), put(b"d", b"d")],
            ..Default::default()
        })),
    };
    let delete = RequestOp {
        request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
            key: b"b".to_vec(),
            ..Default::default()
        })),
    };
    for request in [put(b"a", b"one"), put(b"b", &big), txn, delete] {
        state
            .apply_etcd(state.applied + 1, &request)
            .await
            .unwrap()
            .result
            .unwrap();
    }
    let grouped = [put(b"a", &big), put(b"b", b"two"), put(b"a", b"three")];
    let puts: Vec<_> = grouped
        .iter()
        .enumerate()
        .map(|(offset, request)| put_batch::KvEntry {
            index: state.applied + offset as u64 + 1,
            request,
            credentials: None,
            origin: 1,
        })
        .collect();
    state.apply_puts(&puts).await.unwrap();

    let bytes = state.snapshot_data().await.unwrap();
    let mut data: SnapshotData = crate::codec::decode(&bytes).unwrap();
    // Binaries that read only format 3 compare the version with their storage format.
    assert_eq!(data.version, crate::SNAPSHOT_FORMAT);
    assert_ne!(data.version, crate::STORAGE_FORMAT);
    let (rows, rewritten) = format_3_rows(std::mem::take(&mut data.rows));
    assert!(rewritten >= 10, "{rewritten} rows changed");
    data.rows = rows;
    data.version = crate::STORAGE_FORMAT;

    assert_eq!(data.applied, state.applied);
    let install = |version: u32| {
        let data = SnapshotData {
            version,
            applied: data.applied,
            rows: data.rows.clone(),
        };
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = state.applied;
        snapshot.set_data(crate::codec::encode(&data).unwrap());
        snapshot
    };
    let mut restored = StateMachine::open(other.path()).await.unwrap();
    assert!(restored.install_snapshot(&install(5)).await.is_err());
    restored
        .install_snapshot(&install(crate::STORAGE_FORMAT))
        .await
        .unwrap();
    for revision in 0..=state.revision {
        assert_eq!(
            serde_json::to_value(restored.hash_kv(revision).await).unwrap(),
            serde_json::to_value(state.hash_kv(revision).await).unwrap(),
            "HashKV revision {revision}"
        );
    }
    let request = WatchCreateRequest {
        key: vec![0],
        range_end: vec![0],
        start_revision: 1,
        prev_kv: true,
        ..Default::default()
    };
    let events = |page: crate::watch::WatchPage| -> Vec<_> {
        page.responses.into_iter().flat_map(|r| r.events).collect()
    };
    let expected = events(state.watch(&request, None).await.unwrap());
    assert_eq!(expected.len(), 8);
    assert!(expected.iter().any(|e| e.kv.as_ref().unwrap().value == big));
    assert_eq!(
        events(restored.watch(&request, None).await.unwrap()),
        expected
    );
    restored.close().await.unwrap();
    state.close().await.unwrap();
}

/// Hash rows in all three stored forms must give the same HashKV and compaction results.
///
/// A directory written by earlier binaries holds full KeyValue rows (format 3) and digests that
/// are marked by protobuf fields 14 and 15 (the first format 4 binaries). This binary writes
/// digests that start with an explicit marker byte. All three can share one directory.
#[tokio::test]
async fn hash_rows_in_every_stored_form_hash_and_compact_alike() {
    use crate::proto::mvccpb::KeyValue;
    use crate::proto::state::Version;
    use prost::Message;
    let dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    let big = vec![b'v'; 16 << 10];
    let txn = RequestOp {
        request: Some(Op::RequestTxn(crate::proto::etcdserverpb::TxnRequest {
            success: vec![put(b"c", &big), put(b"d", b"d")],
            ..Default::default()
        })),
    };
    let delete = RequestOp {
        request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
            key: b"b".to_vec(),
            ..Default::default()
        })),
    };
    let requests = [
        put(b"a", b"one"),
        put(b"b", &big),
        txn,
        delete,
        put(b"a", &big),
        put(b"b", b"two"),
        put(b"c", b"three"),
        put(b"a", b"four"),
    ];
    for request in &requests {
        state
            .apply_etcd(state.applied + 1, request)
            .await
            .unwrap()
            .result
            .unwrap();
    }
    let bytes = state.snapshot_data().await.unwrap();
    let mut data: SnapshotData = crate::codec::decode(&bytes).unwrap();
    let history: std::collections::HashMap<Vec<u8>, Vec<u8>> = data
        .rows
        .iter()
        .filter(|(key, _)| key.starts_with(mvcc::HISTORY))
        .filter_map(|(key, value)| {
            let kv: KeyValue = Version::decode(value.as_slice()).unwrap().value?;
            Some((key.clone(), kv.encode_to_vec()))
        })
        .collect();
    let hash = crate::maintenance::HASH;
    let mut forms = [0; 3];
    let mut digests = 0;
    for (key, value) in &mut data.rows {
        if !key.starts_with(hash) || key.len() != hash.len() + 17 {
            continue;
        }
        let user_key = maintenance::hash_record_key(value).unwrap();
        let revision = i64::from_be_bytes(key[hash.len()..hash.len() + 8].try_into().unwrap());
        let kv = &history[&mvcc::history_key(&user_key, revision)];
        assert_eq!(*value, maintenance::hash_record(&user_key, kv));
        let form = digests % 3;
        digests += 1;
        forms[form] += 1;
        *value = match form {
            0 => value.clone(),
            1 => maintenance::legacy_hash_record(&user_key, kv),
            _ => kv.clone(),
        };
    }
    assert!(forms.iter().all(|&count| count >= 2), "{forms:?}");
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = state.applied;
    snapshot.set_data(crate::codec::encode(&data).unwrap());
    let mut mixed = StateMachine::open(other.path()).await.unwrap();
    mixed.install_snapshot(&snapshot).await.unwrap();
    async fn compare(state: &StateMachine, mixed: &StateMachine, from: i64) {
        for revision in from..=state.revision {
            assert_eq!(
                serde_json::to_value(mixed.hash_kv(revision).await).unwrap(),
                serde_json::to_value(state.hash_kv(revision).await).unwrap(),
                "HashKV revision {revision}"
            );
        }
    }
    compare(&state, &mixed, 0).await;
    // Physical compaction deletes hash rows by user key and combines their CRCs.
    for target in [3, 6] {
        let request = CompactionRequest {
            revision: target,
            physical: false,
        };
        for state in [&mut state, &mut mixed] {
            state
                .apply_compaction(state.applied + 1, &request)
                .await
                .unwrap()
                .result
                .unwrap();
            compaction_tests::finish(state).await;
        }
        compare(&state, &mixed, target).await;
        let hash_keys = |state: &StateMachine| {
            let db = state.db.clone();
            async move {
                let mut scan = db.scan_prefix(hash, ..).await.unwrap();
                let mut keys = vec![];
                while let Some(row) = scan.next().await.unwrap() {
                    keys.push(row.key);
                }
                keys
            }
        };
        assert_eq!(hash_keys(&mixed).await, hash_keys(&state).await);
    }
    mixed.close().await.unwrap();
    state.close().await.unwrap();
}

/// A put-only transaction on the direct apply path must write the value-once rows. Its HashKV
/// rows hold digests, and its Watch journal omits Put values and sets `values_in_history`. Rows, HashKV, Watch catch-up, and the in-memory Watch feed must
/// match the general evaluator. HashKV and Watch must also match the same rows in format 3.
#[tokio::test]
async fn direct_put_txns_write_value_once_rows_like_the_evaluator() {
    use crate::proto::etcdserverpb::{TxnRequest, WatchCreateRequest};
    use crate::proto::mvccpb::KeyValue;
    use crate::proto::state::{Journal, Version};
    use prost::Message;

    async fn all_rows(state: &StateMachine) -> Vec<(bytes::Bytes, bytes::Bytes)> {
        let mut scan = state.db.scan_prefix(b"", ..).await.unwrap();
        let mut rows = vec![];
        while let Some(row) = scan.next().await.unwrap() {
            rows.push((row.key, row.value));
        }
        rows
    }
    fn feed(state: &mut StateMachine, previous: i64) -> Vec<(i64, Journal)> {
        state
            .take_watch_batch(previous)
            .unwrap()
            .journals()
            .expect("complete watch batch")
            .to_vec()
    }
    let txn = |success: Vec<RequestOp>| RequestOp {
        request: Some(Op::RequestTxn(TxnRequest {
            success,
            ..Default::default()
        })),
    };
    let with_prev = |key: &[u8], value: &[u8]| RequestOp {
        request: Some(Op::RequestPut(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            prev_kv: true,
            ..Default::default()
        })),
    };
    let big = vec![b'v'; 64 << 10];
    // New keys, then overwrites whose previous values are in history.
    let requests = [
        txn(vec![put(b"a", &big), put(b"b", b"small")]),
        txn(vec![with_prev(b"a", b"again"), put(b"c", &big)]),
        txn(vec![
            with_prev(b"c", b"1"),
            put(b"d", &big),
            put(b"e", b"e"),
        ]),
    ];
    let values: [Vec<&[u8]>; 3] = [
        vec![&big, b"small"],
        vec![b"again", &big],
        vec![b"1", &big, b"e"],
    ];

    let left = tempfile::tempdir().unwrap();
    let right = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut direct = StateMachine::open(left.path()).await.unwrap();
    let mut evaluator = StateMachine::open(right.path()).await.unwrap();
    for state in [&mut direct, &mut evaluator] {
        state.watch_capture = Some(Default::default());
    }
    let mut revisions = vec![];
    for (i, request) in requests.iter().enumerate() {
        let previous = direct.revision;
        let before = crate::mvcc::DIRECT_TXNS.with(|count| count.get());
        let replies = direct
            .apply_puts(&[put_batch::KvEntry {
                index: direct.applied + 1,
                request,
                credentials: None,
                origin: 1,
            }])
            .await
            .unwrap();
        assert_eq!(
            crate::mvcc::DIRECT_TXNS.with(|count| count.get()),
            before + 1,
            "request {i} took the direct path"
        );
        let reply = evaluator
            .apply_etcd(evaluator.applied + 1, request)
            .await
            .unwrap();
        assert!(reply.result.is_ok());
        assert_eq!(
            serde_json::to_value(&replies[0]).unwrap(),
            serde_json::to_value(&reply).unwrap(),
            "request {i}"
        );
        // The in-memory feed restores the omitted values from the history rows of the job.
        let direct_feed = feed(&mut direct, previous);
        assert_eq!(direct_feed, feed(&mut evaluator, previous), "request {i}");
        let [(revision, journal)] = direct_feed.as_slice() else {
            panic!("request {i} has one journal");
        };
        assert!(!journal.values_in_history);
        let fed: Vec<&[u8]> = journal
            .events
            .iter()
            .map(|e| e.kv.as_ref().unwrap().value.as_slice())
            .collect();
        assert_eq!(fed, values[i], "request {i}");
        revisions.push(*revision);
    }
    // A repeated key is invalid in etcd, so no direct journal holds an event that history lacks.
    let repeated = txn(vec![put(b"d", b"1"), put(b"d", &big)]);
    let rejected = direct
        .apply_puts(&[put_batch::KvEntry {
            index: direct.applied + 1,
            request: &repeated,
            credentials: None,
            origin: 1,
        }])
        .await
        .unwrap();
    let reply = evaluator
        .apply_etcd(evaluator.applied + 1, &repeated)
        .await
        .unwrap();
    assert!(reply.result.is_err());
    assert_eq!(
        serde_json::to_value(&rejected[0]).unwrap(),
        serde_json::to_value(&reply).unwrap()
    );
    assert_eq!(all_rows(&direct).await, all_rows(&evaluator).await);

    // The stored rows use the value-once format.
    let get = |state: &StateMachine, key: Vec<u8>| {
        let db = state.db.clone();
        async move { db.get(key).await.unwrap().unwrap() }
    };
    for (i, &revision) in revisions.iter().enumerate() {
        let journal = Journal::decode(get(&direct, crate::watch::key(revision)).await).unwrap();
        assert!(journal.values_in_history, "request {i}");
        for (sub, event) in journal.events.iter().enumerate() {
            let kv = event.kv.as_ref().unwrap();
            assert_eq!(kv.mod_revision, revision);
            assert!(kv.value.is_empty(), "request {i}, event {sub}");
            let hash = get(
                &direct,
                crate::state::maintenance::hash_key(revision, sub as i64, false),
            )
            .await;
            assert!(hash.len() < 64, "request {i}, event {sub}");
            let full = KeyValue {
                value: values[i][sub].to_vec(),
                ..kv.clone()
            }
            .encode_to_vec();
            let seed = crc32c::crc32c(b"seed");
            assert_eq!(
                crate::state::maintenance::hash_append(seed, &hash).unwrap(),
                (crc32c::crc32c_append(seed, &full), full.len() as u64),
                "request {i}, event {sub}"
            );
            // The history version at the revision holds the omitted value.
            let version = Version::decode(get(&direct, mvcc::history_key(&kv.key, revision)).await)
                .unwrap()
                .value
                .unwrap();
            assert_eq!(version.encode_to_vec(), full, "request {i}, event {sub}");
        }
    }

    // Rewrite the rows to format 3 with full hash values and inline journal values.
    let bytes = direct.snapshot_data().await.unwrap();
    let mut data: SnapshotData = crate::codec::decode(&bytes).unwrap();
    let (rows, rewritten) = format_3_rows(std::mem::take(&mut data.rows));
    // Seven hash rows and three journals omit values.
    assert_eq!(rewritten, 10);
    data.rows = rows;
    data.version = crate::STORAGE_FORMAT;
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = direct.applied;
    snapshot.set_data(crate::codec::encode(&data).unwrap());
    let mut inline = StateMachine::open(other.path()).await.unwrap();
    inline.install_snapshot(&snapshot).await.unwrap();

    for revision in 0..=direct.revision {
        let expected = serde_json::to_value(inline.hash_kv(revision).await).unwrap();
        for state in [&direct, &evaluator] {
            assert_eq!(
                serde_json::to_value(state.hash_kv(revision).await).unwrap(),
                expected,
                "HashKV revision {revision}"
            );
        }
    }
    let request = WatchCreateRequest {
        key: vec![0],
        range_end: vec![0],
        start_revision: 1,
        prev_kv: true,
        ..Default::default()
    };
    let events = |page: crate::watch::WatchPage| -> Vec<_> {
        page.responses.into_iter().flat_map(|r| r.events).collect()
    };
    let expected = events(inline.watch(&request, None).await.unwrap());
    let stored: Vec<Vec<u8>> = expected
        .iter()
        .map(|e| e.kv.as_ref().unwrap().value.clone())
        .collect();
    let all: Vec<Vec<u8>> = values.iter().flatten().map(|v| v.to_vec()).collect();
    assert_eq!(stored, all);
    // The overwrites return their previous values from history.
    let prev = |key: &[u8]| {
        expected
            .iter()
            .find(|e| e.kv.as_ref().unwrap().key == key && e.prev_kv.is_some())
            .map(|e| e.prev_kv.clone().unwrap().value)
    };
    assert_eq!(prev(b"a"), Some(big.clone()));
    assert_eq!(prev(b"c"), Some(big.clone()));
    for state in [&direct, &evaluator] {
        assert_eq!(events(state.watch(&request, None).await.unwrap()), expected);
    }
    // Watch catch-up reads the history rows after a reopen too.
    direct.db.flush().await.unwrap();
    direct.close().await.unwrap();
    let reopened = StateMachine::open(left.path()).await.unwrap();
    assert_eq!(
        events(reopened.watch(&request, None).await.unwrap()),
        expected
    );
    reopened.close().await.unwrap();
    inline.close().await.unwrap();
    evaluator.close().await.unwrap();
}

#[test]
fn slatedb_settings_bound_compaction_buffers() {
    let defaults = Settings::default();
    let settings = slatedb_settings();
    settings.validate().unwrap();
    // Durability comes from the Raft log and explicit flushes, not from a SlateDB WAL.
    assert!(!settings.wal_enabled);
    assert_eq!(settings.flush_interval, None);
    assert_eq!(settings.max_unflushed_bytes, defaults.max_unflushed_bytes);
    assert_eq!(settings.l0_sst_size_bytes, defaults.l0_sst_size_bytes);
    let worker = settings
        .compactor_options
        .as_ref()
        .and_then(|compactor| compactor.worker.as_ref())
        .unwrap();
    assert_eq!(worker.max_subcompactions, SUBCOMPACTIONS);
    assert!(
        worker.max_subcompactions >= 2,
        "a subcompaction overlaps one merge with its I/O"
    );
}

/// The directory gets the row marker before its first hash or Watch journal row, on both the
/// general and the batched put path, and not for writes that keep format 3 rows.
#[tokio::test]
async fn the_first_hash_or_journal_row_marks_the_directory() {
    for batched in [false, true] {
        let raft = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::RaftStorage::open(raft.path(), 1, "test", vec![1]).unwrap();
        let marker = storage.row_marker();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        state.row_marker = Some(marker.clone());
        // Empty entries, failed compactions, and reads write no hash or journal row.
        state.apply(1, None).await.unwrap();
        state.apply(2, None).await.unwrap();
        let outcome = state
            .apply_compaction(
                3,
                &CompactionRequest {
                    revision: 5,
                    physical: false,
                },
            )
            .await
            .unwrap();
        assert!(outcome.result.is_err());
        read(&state, b"a", 0).await;
        assert!(!marker.is_marked());
        let hash_or_journal = |state: &StateMachine| {
            let db = state.db.clone();
            async move {
                for prefix in [crate::maintenance::HASH, crate::watch::EVENTS] {
                    if db
                        .scan_prefix(prefix, ..)
                        .await
                        .unwrap()
                        .next()
                        .await
                        .unwrap()
                        .is_some()
                    {
                        return true;
                    }
                }
                false
            }
        };
        assert!(!hash_or_journal(&state).await);
        if batched {
            let request = put(b"a", b"one");
            state
                .apply_puts(&[put_batch::KvEntry {
                    index: state.applied + 1,
                    request: &request,
                    credentials: None,
                    origin: 1,
                }])
                .await
                .unwrap();
        } else {
            state
                .apply_etcd(state.applied + 1, &put(b"a", b"one"))
                .await
                .unwrap()
                .result
                .unwrap();
        }
        assert!(hash_or_journal(&state).await);
        assert!(marker.is_marked());
        state.close().await.unwrap();
        drop((storage, marker, state));
        let storage = crate::storage::RaftStorage::open(raft.path(), 1, "test", vec![1]).unwrap();
        assert!(storage.row_marker().is_marked());
    }
}

/// Return the stored `(index, term)` apply position, or `None` when the row is missing.
async fn stored_position(state: &StateMachine) -> Option<(u64, u64)> {
    let row = state.applied_state().get(APPLIED_TERM).await.unwrap()?;
    assert_eq!(row.len(), 16);
    let index = u64::from_be_bytes(row[..8].try_into().unwrap());
    let term = u64::from_be_bytes(row[8..].try_into().unwrap());
    Some((index, term))
}

/// Return the stored term. It is valid only when the row holds the applied index.
async fn stored_term(state: &StateMachine) -> Option<u64> {
    let (index, term) = stored_position(state).await?;
    assert_eq!(index, state.applied);
    Some(term)
}

/// Write a state batch the way binaries without the term row did: `meta/applied` alone.
async fn apply_without_term(state: &mut StateMachine, index: u64) {
    let mut batch = WriteBatch::new();
    batch.put(data_key("key"), index.to_string());
    batch.put(APPLIED, index.to_be_bytes());
    state.write_batch(batch, index).await.unwrap();
    state.applied = index;
}

async fn reopen(state: StateMachine, dir: &std::path::Path) -> StateMachine {
    state.db.flush().await.unwrap();
    state.close().await.unwrap();
    drop(state);
    StateMachine::open(dir).await.unwrap()
}

/// The term row is valid only at its own index. A database from an older binary has no row, or
/// a row that an older binary left behind when it advanced `meta/applied`. Both open with an
/// unknown term, and the next state batch stores the correct position.
#[tokio::test]
async fn a_term_row_is_valid_only_at_its_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    apply_without_term(&mut state, 1).await;
    let mut state = reopen(state, dir.path()).await;
    assert_eq!((state.applied, state.applied_term), (1, 0));
    assert_eq!(stored_position(&state).await, None);
    assert_eq!(
        state.local_value("key").await.unwrap().as_deref(),
        Some("1")
    );
    state.entry_term = 2;
    state.apply(2, None).await.unwrap();
    assert_eq!(stored_position(&state).await, Some((2, 2)));
    let mut state = reopen(state, dir.path()).await;
    assert_eq!((state.applied, state.applied_term), (2, 2));
    // An older binary applies index 3 in term 3 and keeps the row of index 2.
    apply_without_term(&mut state, 3).await;
    let mut state = reopen(state, dir.path()).await;
    assert_eq!(stored_position(&state).await, Some((2, 2)));
    assert_eq!((state.applied, state.applied_term), (3, 0));
    state.entry_term = 3;
    state.apply(4, None).await.unwrap();
    assert_eq!(stored_position(&state).await, Some((4, 3)));
    assert_eq!(state.applied_term, 3);
    state.close().await.unwrap();
}

/// Check native snapshot bytes with the rules of the previous format 4 reader.
fn check_format_4(data: &SnapshotData) {
    assert!(crate::readable_snapshot(data.version));
    let mut seen = std::collections::BTreeSet::new();
    for (key, _) in &data.rows {
        assert!(
            key.starts_with(DATA) || key.starts_with(b"etcd/") || key == APPLIED,
            "the previous reader rejects {}",
            String::from_utf8_lossy(key)
        );
        assert!(seen.insert(key.clone()));
    }
}

/// Check a native backup like the previous restore: checksum, exact decode, and format 4 rows.
fn check_format_4_backup(bytes: &[u8]) {
    use sha2::{Digest, Sha256};
    let (payload, checksum) = bytes.split_at(bytes.len() - 32);
    assert_eq!(Sha256::digest(payload).as_slice(), checksum);
    let (data, consumed): (SnapshotData, _) = bincode::serde::decode_from_slice(
        payload,
        bincode::config::legacy().with_limit::<{ 64 * 1024 * 1024 }>(),
    )
    .unwrap();
    assert_eq!(consumed, payload.len());
    check_format_4(&data);
}

fn backup_bytes(mut image: Vec<u8>) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let checksum = Sha256::digest(&image);
    image.extend_from_slice(&checksum);
    image
}

/// Restore `backup` and install its Raft snapshot. Return the restored state.
async fn restore(dir: &std::path::Path, backup: Vec<u8>) -> (StateMachine, Snapshot) {
    let path = dir.join("backup");
    std::fs::write(&path, backup).unwrap();
    let members = crate::cluster::Store::initial(&std::collections::BTreeMap::from([(
        1,
        "http://127.0.0.1:2380".into(),
    )]));
    let restore = dir.join("restore");
    StateMachine::restore_backup(&path, &restore, 1, "test", &members).unwrap();
    let snapshot =
        crate::storage::RaftStorage::open(&restore.join("raft"), 1, "test", members.conf().voters)
            .unwrap()
            .recovery_snapshot()
            .clone();
    let mut restored = StateMachine::open(&restore.join("state")).await.unwrap();
    restored.install_snapshot(&snapshot).await.unwrap();
    (restored, snapshot)
}

/// Images and backups keep the format 4 rows, so the previous binary reads them. Install takes
/// the term from the Raft snapshot metadata, and a restored backup starts at (1, 1).
#[tokio::test]
async fn images_and_backups_omit_the_term_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = StateMachine::open(&dir.path().join("source"))
        .await
        .unwrap();
    for (index, term) in [(1, 4), (2, 6)] {
        source.entry_term = term;
        source
            .apply_etcd(index, &put(&[index as u8], b"value"))
            .await
            .unwrap()
            .result
            .unwrap();
    }
    assert_eq!(stored_position(&source).await, Some((2, 6)));
    let image = source.snapshot_data().await.unwrap();
    let data: SnapshotData = crate::codec::decode(&image).unwrap();
    assert_eq!(data.version, crate::SNAPSHOT_FORMAT);
    check_format_4(&data);
    check_format_4_backup(&backup_bytes(image.clone()));

    let mut target = StateMachine::open(&dir.path().join("target"))
        .await
        .unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 2;
    snapshot.mut_metadata().term = 6;
    snapshot.set_data(image.clone());
    target.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(
        (target.applied_term, stored_term(&target).await),
        (6, Some(6))
    );
    let target = reopen(target, &dir.path().join("target")).await;
    assert_eq!((target.applied, target.applied_term), (2, 6));
    target.close().await.unwrap();

    let (restored, snapshot) = restore(dir.path(), backup_bytes(image)).await;
    check_format_4(&crate::codec::decode(&snapshot.data).unwrap());
    assert_eq!((restored.applied, restored.applied_term), (1, 1));
    assert_eq!(stored_term(&restored).await, Some(1));
    restored.close().await.unwrap();
    source.close().await.unwrap();
}

/// An older member that installed a copied checkpoint of this binary exports the term row in
/// its format 4 images and backups. Install and restore accept it and use the Raft term.
#[tokio::test]
async fn images_and_backups_from_an_older_exporter_may_hold_the_term_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = StateMachine::open(&dir.path().join("source"))
        .await
        .unwrap();
    source.entry_term = 5;
    source
        .apply_etcd(1, &put(b"key", b"value"))
        .await
        .unwrap()
        .result
        .unwrap();
    let mut data: SnapshotData =
        crate::codec::decode(&source.snapshot_data().await.unwrap()).unwrap();
    data.rows
        .push((APPLIED_TERM.to_vec(), applied_term_row(1, 5).to_vec()));
    let image = crate::codec::encode(&data).unwrap();

    let mut target = StateMachine::open(&dir.path().join("target"))
        .await
        .unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 1;
    snapshot.mut_metadata().term = 5;
    snapshot.set_data(image.clone());
    target.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(
        (target.applied_term, stored_term(&target).await),
        (5, Some(5))
    );
    target.close().await.unwrap();

    let (restored, snapshot) = restore(dir.path(), backup_bytes(image)).await;
    check_format_4(&crate::codec::decode(&snapshot.data).unwrap());
    assert_eq!((restored.applied, restored.applied_term), (1, 1));
    assert_eq!(stored_term(&restored).await, Some(1));
    restored.close().await.unwrap();
    source.close().await.unwrap();
}

/// The term row is outside `etcd/`, so the quota counter and the hashes do not depend on it.
#[tokio::test]
async fn the_applied_term_changes_no_quota_or_hash() {
    let mut states = vec![];
    let mut dirs = vec![];
    for term in [0, 9] {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        state.entry_term = term;
        for index in 1..=3 {
            state
                .apply_etcd(index, &put(b"key", &[index as u8; 10]))
                .await
                .unwrap()
                .result
                .unwrap();
        }
        assert_eq!(stored_term(&state).await, Some(term));
        assert_eq!(
            state.logical_size(),
            maintenance::count_logical_size(state.db.as_ref())
                .await
                .unwrap()
        );
        states.push(state);
        dirs.push(dir);
    }
    assert_eq!(states[0].logical_size(), states[1].logical_size());
    let mut hashes = vec![];
    for state in &mut states {
        let command = crate::maintenance::Command::HashAll;
        let outcome = state
            .maintenance(&command, &Default::default(), Default::default())
            .await
            .unwrap();
        hashes.push(serde_json::to_value(outcome).unwrap());
    }
    assert_eq!(hashes[0], hashes[1]);
    for revision in [0, 2, 4] {
        assert_eq!(
            serde_json::to_value(states[0].hash_kv(revision).await).unwrap(),
            serde_json::to_value(states[1].hash_kv(revision).await).unwrap()
        );
    }
    for state in states {
        state.close().await.unwrap();
    }
}
