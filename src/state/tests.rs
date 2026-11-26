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
    let mut scan = state.db().scan_prefix(b"etcd/history/", ..).await.unwrap();
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
                        let expected = match mvcc::prepare_with_counts(
                            state.db().as_ref(),
                            None,
                            state.revision,
                            state.compact_revision,
                            &request,
                            &state.leases,
                        )
                        .await
                        {
                            Ok((response, ..)) => Ok(response),
                            Err(mvcc::ApplyError::Request(error)) => Err(error),
                            Err(mvcc::ApplyError::Unapplied(error)) => panic!("{error:#}"),
                        };
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
    state.db().flush().await.unwrap();
    state.db().close().await.unwrap();
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
    let mut scan = state.db().scan_prefix(b"", ..).await.unwrap();
    while let Some(row) = scan.next().await.unwrap() {
        // The image omits the derived rows. Install takes the term from the Raft snapshot.
        if !derived_key(&row.key) {
            rows.push((row.key.to_vec(), row.value.to_vec()));
        }
    }
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
    restored.db().flush().await.unwrap();
    restored.db().close().await.unwrap();
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
    assert!(crate::mvcc::scan_lease_keys(state.db().as_ref(), 9)
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
            let db = state.db().clone();
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
        let mut scan = state.db().scan_prefix(b"", ..).await.unwrap();
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
        let db = state.db().clone();
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
    direct.db().flush().await.unwrap();
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

/// Only a member-local directory runs compacted-SST GC every minute. Every store keeps SlateDB's
/// compactor checkpoint lifetime: a cold scan can run for minutes, and GC would delete an SST
/// that the scan still needs.
#[test]
fn member_settings_shorten_gc_only_on_local_disk() {
    let defaults = Settings::default();
    let remote = member_settings(false);
    remote.validate().unwrap();
    assert_eq!(gc_retention(&remote), gc_retention(&defaults));
    assert_eq!(gc_retention(&slatedb_settings()), gc_retention(&defaults));
    let local = member_settings(true);
    local.validate().unwrap();
    assert_eq!(
        gc_retention(&local),
        (gc_retention(&defaults).0, Some(COMPACTED_GC_INTERVAL))
    );
    assert!(Some(COMPACTED_GC_INTERVAL) < gc_retention(&defaults).1);
}

/// The compactor checkpoint lifetime and the compacted-SST GC interval of `settings`.
fn gc_retention(settings: &Settings) -> (Duration, Option<Duration>) {
    let lifetime = settings
        .compactor_options
        .as_ref()
        .unwrap()
        .checkpoint_lifetime;
    let gc = settings.garbage_collector_options.as_ref().unwrap();
    (lifetime, gc.compacted_options.as_ref().unwrap().interval)
}

/// A cold scan of a member-local directory reads every row although a compaction replaces its
/// SST and GC runs 7 minutes later, as when the index rebuild at open or a DeleteRange reads a
/// large data set from a busy disk. A 5-minute checkpoint lifetime lost the SST: `NotFound`.
#[tokio::test]
async fn cold_scan_survives_compaction_and_gc_on_local_disk() {
    use slatedb_common::clock::{DefaultSystemClock, SystemClock, SystemClockTicker};

    /// The default clock plus an offset that the test moves forward.
    #[derive(Debug, Default)]
    struct Ahead {
        clock: DefaultSystemClock,
        offset: std::sync::Mutex<Duration>,
    }
    impl SystemClock for Ahead {
        fn now(&self) -> chrono::DateTime<chrono::Utc> {
            self.clock.now() + *self.offset.lock().unwrap()
        }
        fn sleep<'a>(
            &'a self,
            duration: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
            self.clock.sleep(duration)
        }
        fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
            SystemClockTicker::new(self, duration)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(
        slatedb::object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    );
    let settings = member_settings(true);
    let gc = settings.garbage_collector_options.clone().unwrap();
    // The embedded compactor uses the real clock for its checkpoint; only GC runs ahead.
    let db = Db::builder("db", store.clone())
        .with_settings(Settings {
            garbage_collector_options: None,
            ..settings
        })
        .with_db_cache_disabled()
        .build()
        .await
        .unwrap();
    let rows = 4096;
    let value = vec![7u8; 1024];
    let key = |i: usize, batch: usize| format!("k{batch}-{i:05}").into_bytes();
    let write = |batch: usize| {
        let (db, value) = (&db, &value);
        async move {
            for i in 0..rows {
                db.put(key(i, batch), value).await.unwrap();
            }
            db.flush().await.unwrap();
        }
    };
    // The scan starts on the first L0 SST and reads only its first block.
    write(0).await;
    let mut scan = db.scan(key(0, 0)..key(0, 1)).await.unwrap();
    let first = scan.next().await.unwrap().unwrap();
    assert_eq!(first.key.as_ref(), key(0, 0).as_slice());
    // GC keeps the newest L0 SST that a compaction replaced, so later L0 SSTs must be newer.
    tokio::time::sleep(Duration::from_millis(10)).await;
    // Four L0 SSTs make the size-tiered scheduler compact them.
    for batch in 1..4 {
        write(batch).await;
    }

    let clock = Arc::new(Ahead::default());
    let admin = slatedb::admin::Admin::builder("db", store.clone())
        .with_system_clock(clock.clone())
        .build();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let manifest = admin.read_manifest(None).await.unwrap().unwrap();
        if manifest.l0().is_empty() && !manifest.compacted().is_empty() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no compaction");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    *clock.offset.lock().unwrap() = Duration::from_secs(7 * 60);
    admin.run_gc_once(gc).await.unwrap();

    let mut read = 1;
    while let Some(row) = scan.next().await.unwrap() {
        assert_eq!(row.key.as_ref(), key(read, 0).as_slice());
        assert_eq!(row.value.as_ref(), value.as_slice());
        read += 1;
    }
    assert_eq!(read, rows);
    db.close().await.unwrap();
}

/// The directory constructor marks its store as local disk. A store that a caller passes to
/// `open_store` is opaque, even when it is a local filesystem, so it keeps the default GC interval,
/// and so does the database that replaces it.
#[tokio::test]
async fn open_store_keeps_default_gc_retention() {
    let dir = tempfile::tempdir().unwrap();
    let state = StateMachine::open(&dir.path().join("dir")).await.unwrap();
    assert!(state.location.local_disk);
    state.close().await.unwrap();

    let memory: Arc<dyn slatedb::object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let state = StateMachine::open_store(memory, "state").await.unwrap();
    assert!(!state.location.local_disk);
    let replacement = state.location.path("state-1");
    let next = StateMachine::open_at(state.location.clone(), &replacement)
        .await
        .unwrap();
    assert!(!next.location.local_disk);
    next.close().await.unwrap();
    state.close().await.unwrap();

    std::fs::create_dir_all(dir.path().join("store")).unwrap();
    let local =
        slatedb::object_store::local::LocalFileSystem::new_with_prefix(dir.path().join("store"))
            .unwrap();
    let state =
        StateMachine::open_store_with_cache(Arc::new(local), "state", StateCache::default())
            .await
            .unwrap();
    assert!(!state.location.local_disk);
    state.close().await.unwrap();
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
            let db = state.db().clone();
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

pub(super) async fn reopen(state: StateMachine, dir: &std::path::Path) -> StateMachine {
    state.db().flush().await.unwrap();
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

/// A Raft snapshot at the applied index must agree with a known stored term.
#[tokio::test]
async fn a_snapshot_at_the_applied_index_checks_a_known_term() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    state.entry_term = 6;
    state.apply(1, None).await.unwrap();
    let mut state = reopen(state, dir.path()).await;
    assert_eq!((state.applied, state.applied_term), (1, 6));
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 1;
    snapshot.mut_metadata().term = 7;
    let error = state.install_snapshot(&snapshot).await.unwrap_err();
    assert!(error.to_string().contains("snapshot term"), "{error}");
    snapshot.mut_metadata().term = 6;
    state.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(
        (state.applied_term, stored_term(&state).await),
        (6, Some(6))
    );
    state.close().await.unwrap();
}

/// Check native snapshot bytes with the rules of the previous format 4 reader.
pub(super) fn check_format_4(data: &SnapshotData) {
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
pub(super) fn check_format_4_backup(bytes: &[u8]) {
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

pub(super) fn backup_bytes(mut image: Vec<u8>) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let checksum = Sha256::digest(&image);
    image.extend_from_slice(&checksum);
    image
}

/// Restore `backup`, open the restored state, and install the Raft snapshot like startup.
/// The Raft snapshot records the restored index with term 1 and holds no image.
pub(super) async fn restore(dir: &std::path::Path, backup: Vec<u8>) -> StateMachine {
    let path = dir.join("backup");
    std::fs::write(&path, backup).unwrap();
    let members = crate::cluster::Store::initial(&std::collections::BTreeMap::from([(
        1,
        "http://127.0.0.1:2380".into(),
    )]));
    let restore = dir.join("restore");
    StateMachine::restore_backup(
        &path,
        &restore,
        1,
        "test",
        &members,
        &crate::object::Config::Local,
        StateCache::default(),
    )
    .await
    .unwrap();
    let snapshot =
        crate::storage::RaftStorage::open(&restore.join("raft"), 1, "test", members.conf().voters)
            .unwrap()
            .recovery_snapshot()
            .clone();
    assert!(snapshot.data.is_empty());
    assert_eq!(snapshot.get_metadata().term, 1);
    let mut restored = StateMachine::open(&restore.join("objects")).await.unwrap();
    assert_eq!(restored.applied, snapshot.get_metadata().index);
    restored.install_snapshot(&snapshot).await.unwrap();
    restored
}

/// Collect a streamed native backup.
pub(super) async fn streamed_backup(state: &StateMachine) -> Vec<u8> {
    let mut stream = state.backup().await.unwrap();
    let mut data = vec![];
    while let Some(chunk) = stream.recv().await {
        data.extend_from_slice(&chunk.unwrap().blob);
    }
    data
}

/// Images and backups keep the format 4 rows, so the previous binary reads them. Install takes
/// the term from the Raft snapshot metadata, and a restored backup starts at term 1.
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
    check_format_4_backup(&streamed_backup(&source).await);

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

    let restored = restore(dir.path(), backup_bytes(image)).await;
    assert_eq!(restored.applied_term, 1);
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

    let restored = restore(dir.path(), backup_bytes(image)).await;
    assert_eq!(restored.applied_term, 1);
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
            state.logical_usage(),
            maintenance::count_logical_usage(state.db().as_ref())
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

/// A counter row whose split overflows must not pass the total check by wrapping.
#[test]
fn logical_size_split_that_overflows_is_rejected() {
    let counter = crate::proto::state::LogicalSize {
        applied: 7,
        bytes: 0,
        current: Some(u64::MAX),
        history: Some(1),
        journal: Some(0),
        other: Some(0),
    };
    assert!(stored_usage(&counter, 7).is_err());
    let valid = crate::proto::state::LogicalSize {
        bytes: 3,
        current: Some(1),
        history: Some(2),
        ..counter
    };
    assert_eq!(
        stored_usage(&valid, 7).unwrap().map(|usage| usage.total()),
        Some(3)
    );
}

/// The replicated membership row. See `state::cluster`.
mod members {
    use super::*;
    use crate::cluster::{Command as C, Store};
    use crate::proto::etcdserverpb::Member;
    use std::collections::BTreeMap;

    fn peers() -> BTreeMap<u64, String> {
        (1..=3).map(|id| (id, format!("127.0.0.1:{id}"))).collect()
    }

    /// The bootstrap membership of a member whose client URLs use the host text `host`.
    fn flags(host: &str) -> Store {
        let clients = (1..=3)
            .map(|id| (id, format!("http://{host}:{}", 2379 + id)))
            .collect();
        Store::bootstrap(&peers(), &clients)
    }

    /// Open a replicated state with the bootstrap membership of `host` in memory, as a node does.
    async fn member(dir: &std::path::Path, host: &str) -> StateMachine {
        let mut state = StateMachine::open(dir).await.unwrap();
        state.members = Arc::new(flags(host));
        state
    }

    async fn row(state: &StateMachine) -> Option<Vec<u8>> {
        state
            .applied_state()
            .get(crate::cluster::KEY)
            .await
            .unwrap()
            .map(|row| row.to_vec())
    }

    fn learner(id: u64) -> C {
        C::Add(Member {
            id,
            name: String::new(),
            peer_ur_ls: vec![format!("http://127.0.0.1:{id}")],
            client_ur_ls: vec![],
            is_learner: true,
        })
    }

    fn json(members: &Store) -> serde_json::Value {
        serde_json::to_value(members).unwrap()
    }

    fn admin() -> crate::auth::Credentials {
        crate::auth::Credentials::default()
    }

    /// A `SeedMembers` entry of leader `origin` with the row `members`.
    fn seed_entry(
        origin: u64,
        members: &str,
        digest: Option<&str>,
        applied: u64,
    ) -> crate::cluster::Seed {
        crate::cluster::Seed {
            id: String::new(),
            origin,
            members: members.to_owned(),
            digest: digest.map(str::to_owned),
            applied,
        }
    }

    /// The seed replaces the bootstrap membership in memory. A later configuration change starts
    /// from the stored row, so members with other client URL text still store the same row.
    #[tokio::test]
    async fn a_change_after_the_seed_writes_the_same_row_on_every_member() {
        let dir = tempfile::tempdir().unwrap();
        let seed = serde_json::to_string(&flags("leader")).unwrap();
        let mut states = vec![];
        for host in ["127.0.0.1", "other.example"] {
            let mut state = member(&dir.path().join(host), host).await;
            state.apply(1, None).await.unwrap();
            // The bootstrap membership does not count toward the logical size.
            assert_eq!((state.logical_size(), row(&state).await), (0, None));
            state
                .apply_seed_members(2, &seed_entry(1, &seed, None, 1))
                .await
                .unwrap();
            assert_eq!(row(&state).await.unwrap(), seed.as_bytes());
            assert_eq!(
                state.members.members[&1].client_ur_ls,
                ["http://leader:2380"]
            );
            let outcome = state.apply_cluster(3, &learner(4), &admin()).await.unwrap();
            outcome.result.unwrap();
            let publish = C::Publish {
                id: 4,
                client_urls: vec!["http://127.0.0.1:2383".into()],
            };
            state.apply_cluster(4, &publish, &admin()).await.unwrap();
            states.push(state);
        }
        let (first, second) = (&states[0], &states[1]);
        assert_eq!(row(first).await, row(second).await);
        assert_eq!(first.logical_size(), second.logical_size());
        assert_eq!(first.members_row, second.members_row);
        assert_eq!(first.members_row.as_ref().unwrap().written, 4);
        // A seed of a later term keeps the row.
        let other = serde_json::to_string(&flags("next")).unwrap();
        for state in &mut states {
            let before = row(state).await;
            let digest = state.members_row.as_ref().unwrap().digest.clone();
            state
                .apply_seed_members(5, &seed_entry(2, &other, Some(digest.as_str()), 4))
                .await
                .unwrap();
            assert_eq!(row(state).await, before);
            assert_eq!(state.members_mismatch, None);
        }
        for state in states {
            state.close().await.unwrap();
        }
    }

    /// A rejected cluster command writes no row: not the stored row, and not the bootstrap
    /// membership of a state without one.
    #[tokio::test]
    async fn a_rejected_cluster_command_writes_no_row() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        let outcome = state
            .apply_cluster(1, &C::Remove(9), &admin())
            .await
            .unwrap();
        assert!(outcome.result.is_err());
        assert_eq!((row(&state).await, state.logical_size()), (None, 0));
        let seed = serde_json::to_string(&flags("leader")).unwrap();
        state
            .apply_seed_members(2, &seed_entry(1, &seed, None, 1))
            .await
            .unwrap();
        let (size, written) = (state.logical_size(), state.members_row.clone());
        for (index, request) in [
            (3, C::Remove(9)),
            (4, learner(1)),
            (5, C::List(Default::default())),
        ] {
            let outcome = state
                .apply_cluster(index, &request, &admin())
                .await
                .unwrap();
            assert_eq!(outcome.result.is_err(), index != 5);
            assert_eq!(row(&state).await.unwrap(), seed.as_bytes());
            assert_eq!((state.logical_size(), &state.members_row), (size, &written));
        }
        state.close().await.unwrap();
    }

    /// A seed of another term compares digests only when the row did not change after the index of
    /// the leader. A member that applied a later change raises no warning. A different row at the
    /// same index sets the mismatch, and an equal digest clears it.
    #[tokio::test]
    async fn a_seed_compares_the_row_only_at_the_index_of_the_leader() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = member(dir.path(), "127.0.0.1").await;
        let seed = serde_json::to_string(&flags("leader")).unwrap();
        state
            .apply_seed_members(1, &seed_entry(1, &seed, None, 0))
            .await
            .unwrap();
        state.apply_cluster(2, &learner(4), &admin()).await.unwrap();
        let digest = state.members_row.as_ref().unwrap().digest.clone();
        let stale = crate::cluster::digest(seed.as_bytes());
        // The leader read its row at index 1, before the change at index 2.
        state
            .apply_seed_members(3, &seed_entry(2, &seed, Some(stale.as_str()), 1))
            .await
            .unwrap();
        assert_eq!(state.members_mismatch, None);
        state
            .apply_seed_members(4, &seed_entry(2, &seed, Some("0000000000000000"), 3))
            .await
            .unwrap();
        assert_eq!(state.members_mismatch, Some(2));
        state
            .apply_seed_members(5, &seed_entry(3, &seed, Some(digest.as_str()), 4))
            .await
            .unwrap();
        assert_eq!(state.members_mismatch, None);
        // A restart knows only that the row changed at or before the applied index.
        state.db().flush().await.unwrap();
        state.close().await.unwrap();
        drop(state);
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        assert_eq!(state.members_row.as_ref().unwrap().written, 5);
        assert_eq!(state.members.members.len(), 4);
        state
            .apply_seed_members(6, &seed_entry(3, &seed, Some("0000000000000000"), 4))
            .await
            .unwrap();
        assert_eq!(state.members_mismatch, None);
        state.close().await.unwrap();
    }

    /// A Raft snapshot carries the stored row and no member-local membership. An install of an image
    /// without the row keeps the bootstrap membership in memory only.
    #[tokio::test]
    async fn an_image_carries_only_the_stored_row() {
        let dir = tempfile::tempdir().unwrap();
        let mut leader = member(&dir.path().join("leader"), "leader").await;
        leader.apply(1, None).await.unwrap();
        let image = leader.snapshot_data().await.unwrap();
        let data: SnapshotData = crate::codec::decode(&image).unwrap();
        assert!(data.rows.iter().all(|(key, _)| key != crate::cluster::KEY));
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 1;
        snapshot.mut_metadata().term = 1;
        snapshot.data = image;
        let mut follower = member(&dir.path().join("follower"), "127.0.0.1").await;
        follower.install_snapshot(&snapshot).await.unwrap();
        assert_eq!((row(&follower).await, &follower.members_row), (None, &None));
        assert_eq!(json(&follower.members), json(&flags("127.0.0.1")));

        let seed = serde_json::to_string(&flags("leader")).unwrap();
        leader
            .apply_seed_members(2, &seed_entry(1, &seed, None, 1))
            .await
            .unwrap();
        snapshot.mut_metadata().index = 2;
        snapshot.data = leader.snapshot_data().await.unwrap();
        follower.install_snapshot(&snapshot).await.unwrap();
        assert_eq!(row(&follower).await.unwrap(), seed.as_bytes());
        assert_eq!(json(&follower.members), json(&flags("leader")));
        assert_eq!(follower.logical_size(), leader.logical_size());
        assert_eq!(
            follower.members_row.as_ref().unwrap().digest,
            leader.members_row.as_ref().unwrap().digest
        );
        leader.close().await.unwrap();
        follower.close().await.unwrap();
    }

    /// A replicated restore drops the membership row of the backup. Each member restores with its
    /// own flags, so the first leader of the new cluster seeds the row.
    #[tokio::test]
    async fn a_replicated_restore_stores_no_member_local_row() {
        let dir = tempfile::tempdir().unwrap();
        let mut source = member(&dir.path().join("source"), "source").await;
        let seed = serde_json::to_string(&flags("source")).unwrap();
        source
            .apply_seed_members(1, &seed_entry(1, &seed, None, 0))
            .await
            .unwrap();
        let backup = backup_bytes(source.snapshot_data().await.unwrap());
        let mut restored = restore(dir.path(), backup).await;
        assert_eq!((row(&restored).await, &restored.members_row), (None, &None));
        let seed = serde_json::to_string(&flags("new")).unwrap();
        restored
            .apply_seed_members(2, &seed_entry(1, &seed, None, 1))
            .await
            .unwrap();
        assert_eq!(row(&restored).await.unwrap(), seed.as_bytes());
        source.close().await.unwrap();
        restored.close().await.unwrap();
    }

    #[test]
    fn differences_compare_ids_and_urls() {
        assert!(flags("a").differences(&flags("a")).is_empty());
        assert_eq!(flags("a").differences(&flags("b")), [1, 2, 3]);
        let mut other = flags("a");
        other.members.remove(&2);
        other.members.get_mut(&3).unwrap().peer_ur_ls = vec!["http://127.0.0.1:9".into()];
        assert_eq!(flags("a").differences(&other), [2, 3]);
    }
}
