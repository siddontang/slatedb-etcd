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
    assert_eq!(data.version, STORAGE_FORMAT);
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
                            &state.db,
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
        if row.key.as_ref() != crate::cluster::KEY {
            rows.push((row.key.to_vec(), row.value.to_vec()));
        }
    }
    rows.push((
        crate::cluster::KEY.to_vec(),
        serde_json::to_vec(state.members.as_ref()).unwrap(),
    ));
    assert!(rows.len() > 255);
    let expected = crate::codec::encode(&SnapshotData {
        version: STORAGE_FORMAT,
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
    snapshot.data = snapshot_job.await.unwrap().into();
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
    snapshot.data = state.snapshot_data().await.unwrap().into();
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
    assert!(crate::mvcc::lease_keys(&state.db, 9)
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
