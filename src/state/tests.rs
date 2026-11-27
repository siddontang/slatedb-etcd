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
    let mut scan = state
        .applied_state()
        .scan_under(b"etcd/history/", &Default::default())
        .await
        .unwrap();
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
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
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
                            state.applied_state(),
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
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
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
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
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
}

#[tokio::test]
async fn streamed_snapshot_matches_the_complete_state_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
    for index in 1..=100 {
        state
            .apply_etcd(index, &put(&index.to_be_bytes(), &[0, 255, 42]))
            .await
            .unwrap()
            .result
            .unwrap();
    }
    let mut rows = vec![];
    let mut scan = state
        .applied_state()
        .scan_under(b"", &Default::default())
        .await
        .unwrap();
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
async fn renewed_lease_and_watch_history_survive_stale_expiry() {
    use crate::lease::LeaseCommand;
    use crate::proto::etcdserverpb::{TxnRequest, WatchCreateRequest};
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
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
    let mut restored = state;
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
}

#[tokio::test]
async fn lease_revoke_deletes_more_keys_than_the_user_transaction_limit() {
    use crate::lease::LeaseCommand;
    use crate::proto::etcdserverpb::WatchCreateRequest;
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
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
    assert!(crate::mvcc::scan_lease_keys(state.applied_state(), 9)
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

/// A put-only transaction on the direct apply path must write the value-once rows. Its HashKV
/// rows hold digests, and its Watch journal omits Put values and sets `values_in_history`.
/// Rows, HashKV, Watch catch-up, and the in-memory Watch feed must match the general evaluator.
#[tokio::test]
async fn direct_put_txns_write_value_once_rows_like_the_evaluator() {
    use crate::proto::etcdserverpb::TxnRequest;
    use crate::proto::mvccpb::KeyValue;
    use crate::proto::state::{Journal, Version};
    use prost::Message;

    async fn all_rows(state: &StateMachine) -> Vec<(bytes::Bytes, bytes::Bytes)> {
        let mut scan = state
            .applied_state()
            .scan_under(b"", &Default::default())
            .await
            .unwrap();
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
    let mut direct = StateMachine::open_test(left.path()).await.unwrap();
    let mut evaluator = StateMachine::open_test(right.path()).await.unwrap();
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
        let db = state.applied_state().clone();
        async move { db.get(&key).await.unwrap().unwrap() }
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

    direct.close().await.unwrap();
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

/// Check native snapshot bytes with the rules of the format 4 reader.
pub(super) fn check_format_4(data: &SnapshotData) {
    assert!(crate::readable_snapshot(data.version));
    let mut seen = std::collections::BTreeSet::new();
    for (key, _) in &data.rows {
        assert!(
            key.starts_with(DATA) || key.starts_with(b"etcd/") || key == APPLIED,
            "the format 4 reader rejects {}",
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

/// Collect a streamed native backup.
pub(super) async fn streamed_backup(state: &StateMachine) -> Vec<u8> {
    let mut stream = state.backup().await.unwrap();
    let mut data = vec![];
    while let Some(chunk) = stream.recv().await {
        data.extend_from_slice(&chunk.unwrap().blob);
    }
    data
}

/// The term row is outside `etcd/`, so the quota counter and the hashes do not depend on it.
#[tokio::test]
async fn the_applied_term_changes_no_quota_or_hash() {
    let mut states = vec![];
    let mut dirs = vec![];
    for term in [0, 9] {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open_test(dir.path()).await.unwrap();
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
            maintenance::count_logical_usage(state.applied_state())
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
        let mut state = StateMachine::open_test(dir).await.unwrap();
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
        let mut state = StateMachine::open_test(dir.path()).await.unwrap();
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
        state.close().await.unwrap();
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
