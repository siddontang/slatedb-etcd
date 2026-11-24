use super::*;
use crate::lease::LeaseCommand;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::*;
use crate::proto::mvccpb::{Event, KeyValue};

fn put(key: &[u8], value: &[u8]) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestPut(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        })),
    }
}

fn txn(compare: Vec<Compare>, success: Vec<RequestOp>, failure: Vec<RequestOp>) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestTxn(TxnRequest {
            compare,
            success,
            failure,
        })),
    }
}

fn delete(key: &[u8], end: &[u8]) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
            key: key.to_vec(),
            range_end: end.to_vec(),
            ..Default::default()
        })),
    }
}

fn compare(key: &[u8], target: i32, result: i32, union: compare::TargetUnion) -> Compare {
    Compare {
        key: key.to_vec(),
        target,
        result,
        target_union: Some(union),
        ..Default::default()
    }
}

async fn current(state: &StateMachine, key: &[u8]) -> Option<KeyValue> {
    let outcome = state
        .read_etcd(&RequestOp {
            request: Some(Op::RequestRange(RangeRequest {
                key: key.to_vec(),
                ..Default::default()
            })),
        })
        .await
        .unwrap();
    match outcome.result.unwrap().response {
        Some(Resp::ResponseRange(range)) => range.kvs.into_iter().next(),
        _ => unreachable!(),
    }
}

async fn check(state: &StateMachine) {
    assert!(state.key_index.matches(state.db()).await.unwrap());
}

async fn apply(state: &mut StateMachine, request: &RequestOp) -> ResponseOp {
    let index = state.applied + 1;
    state
        .apply_etcd(index, request)
        .await
        .unwrap()
        .result
        .unwrap()
}

async fn events(state: &StateMachine, start: i64) -> Vec<Event> {
    let mut events = vec![];
    let mut start = start;
    loop {
        let page = state
            .watch(
                &WatchCreateRequest {
                    key: vec![0],
                    range_end: vec![0],
                    start_revision: start,
                    prev_kv: true,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        for response in page.responses {
            events.extend(response.events);
        }
        if page.next_revision > state.revision {
            return events;
        }
        start = page.next_revision;
    }
}

#[tokio::test]
async fn index_follows_puts_transactions_deletes_revocations_snapshots_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    let requests = [put(b"a", b"1"), put(b"b", b"1"), put(b"a", b"2")];
    let puts: Vec<_> = requests
        .iter()
        .enumerate()
        .map(|(offset, request)| put_batch::KvEntry {
            index: offset as u64 + 1,
            request,
            credentials: None,
            origin: 1,
        })
        .collect();
    state.apply_puts(&puts).await.unwrap();
    check(&state).await;
    let a = state.key_index.get(b"a").copied().unwrap();
    assert_eq!((a.create_revision, a.mod_revision, a.version), (2, 4, 2));
    apply(
        &mut state,
        &txn(
            vec![compare(b"a", 0, 0, compare::TargetUnion::Version(2))],
            vec![put(b"c", b"txn"), delete(b"b", b"")],
            vec![],
        ),
    )
    .await;
    check(&state).await;
    assert!(state.key_index.get(b"b").is_none());
    assert_eq!(state.key_index.get(b"c").unwrap().mod_revision, 5);
    state
        .apply_lease(state.applied + 1, &LeaseCommand::Grant { id: 7, ttl: 30 })
        .await
        .unwrap()
        .result
        .unwrap();
    let mut leased = put(b"d", b"leased");
    if let Some(Op::RequestPut(p)) = &mut leased.request {
        p.lease = 7;
    }
    apply(&mut state, &leased).await;
    let mut keep_lease = put(b"d", b"new");
    if let Some(Op::RequestPut(p)) = &mut keep_lease.request {
        p.ignore_lease = true;
    }
    apply(&mut state, &keep_lease).await;
    check(&state).await;
    assert_eq!(state.key_index.get(b"d").unwrap().lease, 7);
    state
        .apply_lease(state.applied + 1, &LeaseCommand::Revoke { id: 7 })
        .await
        .unwrap()
        .result
        .unwrap();
    check(&state).await;
    assert!(state.key_index.get(b"d").is_none());
    apply(&mut state, &delete(b"a", b"b")).await;
    check(&state).await;
    assert_eq!(state.key_index.len(), 1);

    let mut target = StateMachine::open(other.path()).await.unwrap();
    target
        .apply_etcd(1, &put(b"removed-by-snapshot", b"x"))
        .await
        .unwrap()
        .result
        .unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = state.applied;
    snapshot.set_data(state.snapshot_data().await.unwrap());
    target.install_snapshot(&snapshot).await.unwrap();
    check(&target).await;
    assert!(target.key_index.get(b"removed-by-snapshot").is_none());
    assert_eq!(target.key_index.get(b"c"), state.key_index.get(b"c"));
    target.close().await.unwrap();

    state.db().flush().await.unwrap();
    let expected = *state.key_index.get(b"c").unwrap();
    state.close().await.unwrap();
    drop(state);
    let state = StateMachine::open(dir.path()).await.unwrap();
    check(&state).await;
    assert_eq!(state.key_index.get(b"c"), Some(&expected));
    assert_eq!(state.key_index.len(), 1);
    state.close().await.unwrap();
}

#[tokio::test]
async fn indexed_transactions_match_storage_evaluation() {
    use compare::TargetUnion as T;
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    state
        .apply_lease(1, &LeaseCommand::Grant { id: 9, ttl: 30 })
        .await
        .unwrap()
        .result
        .unwrap();
    let mut leased = put(b"k", b"value");
    if let Some(Op::RequestPut(p)) = &mut leased.request {
        p.lease = 9;
    }
    apply(&mut state, &leased).await;
    apply(&mut state, &put(b"k", b"value2")).await;
    apply(&mut state, &put(b"other", b"x")).await;
    let mut with_prev = put(b"k", b"prev");
    if let Some(Op::RequestPut(p)) = &mut with_prev.request {
        p.prev_kv = true;
    }
    let mut keep_value = put(b"k", b"");
    if let Some(Op::RequestPut(p)) = &mut keep_value.request {
        p.ignore_value = true;
    }
    let mut keep_lease = put(b"k", b"lease");
    if let Some(Op::RequestPut(p)) = &mut keep_lease.request {
        p.ignore_lease = true;
    }
    let mut missing = put(b"missing", b"");
    if let Some(Op::RequestPut(p)) = &mut missing.request {
        p.ignore_value = true;
    }
    let mut requests = vec![
        put(b"k", b"plain"),
        put(b"new", b"plain"),
        with_prev.clone(),
        keep_value.clone(),
        keep_lease,
        missing,
        delete(b"k", b""),
        delete(b"a", b"z"),
    ];
    for (target, union) in [
        (0, T::Version(2)),
        (1, T::CreateRevision(2)),
        (2, T::ModRevision(3)),
        (3, T::Value(b"value2".to_vec())),
        (4, T::Lease(0)),
    ] {
        for result in 0..4 {
            for key in [&b"k"[..], b"missing"] {
                requests.push(txn(
                    vec![compare(key, target, result, union.clone())],
                    vec![with_prev.clone(), put(b"new", b"x")],
                    vec![keep_value.clone(), delete(b"other", b"")],
                ));
            }
        }
        let mut range = compare(b"a", target, 1, union.clone());
        range.range_end = vec![0];
        requests.push(txn(vec![range], vec![put(b"new", b"x")], vec![]));
    }
    requests.push(txn(
        vec![],
        vec![txn(vec![], vec![with_prev], vec![]), delete(b"other", b"")],
        vec![],
    ));
    for request in &requests {
        let expected = mvcc::prepare_with_counts(
            state.db().as_ref(),
            None,
            state.revision,
            state.compact_revision,
            request,
            &state.leases,
        )
        .await;
        let actual = mvcc::prepare_with_counts(
            state.db().as_ref(),
            Some(&state.key_index),
            state.revision,
            state.compact_revision,
            request,
            &state.leases,
        )
        .await;
        match (expected, actual) {
            (Ok(expected), Ok(actual)) => {
                assert_eq!(expected.0, actual.0, "{request:?}");
                assert_eq!(expected.2, actual.2);
                let rows = |batch: WriteBatch| batch.into_rows().collect::<Vec<_>>();
                assert_eq!(rows(expected.1), rows(actual.1), "{request:?}");
            }
            (Err(expected), Err(actual)) => assert_eq!(expected.message, actual.message),
            (expected, actual) => panic!("{request:?}: {} != {}", expected.is_ok(), actual.is_ok()),
        }
    }
}

#[tokio::test]
async fn watch_previous_values_match_the_replaced_rows() {
    let dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    // Record each current row before a change. Watch must return it as the previous value.
    let mut expected: Vec<(i64, Vec<u8>, Option<KeyValue>)> = vec![];
    for key in [&b"a"[..], b"b"] {
        expected.push((state.revision + 1, key.to_vec(), None));
        apply(&mut state, &put(key, b"first")).await;
    }
    let group = [put(b"a", b"g1"), put(b"a", b"g2"), put(b"b", b"g3")];
    let before_a = current(&state, b"a").await;
    let before_b = current(&state, b"b").await;
    let puts: Vec<_> = group
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
    let base = state.revision - 3;
    expected.push((base + 1, b"a".to_vec(), before_a));
    let mut grouped = current(&state, b"a").await.unwrap();
    grouped.value = b"g1".to_vec();
    grouped.mod_revision = base + 1;
    grouped.version -= 1;
    expected.push((base + 2, b"a".to_vec(), Some(grouped)));
    expected.push((base + 3, b"b".to_vec(), before_b));
    for request in [
        txn(vec![], vec![put(b"a", b"txn"), put(b"c", b"new")], vec![]),
        delete(b"a", b"c"),
        put(b"a", b"again"),
    ] {
        let next = state.revision + 1;
        for key in [&b"a"[..], b"b", b"c"] {
            let touches = match &request.request {
                Some(Op::RequestPut(p)) => p.key == key,
                Some(Op::RequestDeleteRange(_)) => key != b"c",
                _ => key != b"b",
            };
            if touches {
                expected.push((next, key.to_vec(), current(&state, key).await));
            }
        }
        apply(&mut state, &request).await;
    }
    let values: Vec<(i64, Vec<u8>, Vec<u8>)> = [
        (base - 1, "a", "first"),
        (base, "b", "first"),
        (base + 1, "a", "g1"),
        (base + 2, "a", "g2"),
        (base + 3, "b", "g3"),
        (base + 4, "a", "txn"),
        (base + 4, "c", "new"),
        (base + 6, "a", "again"),
    ]
    .into_iter()
    .map(|(r, k, v)| (r, k.into(), v.into()))
    .collect();
    assert_eq!(state.revision, base + 6);
    let check_events = |events: Vec<Event>, start: i64, compact: i64| {
        let wanted: Vec<_> = expected
            .iter()
            .filter(|(revision, _, _)| *revision >= start)
            .collect();
        assert_eq!(events.len(), wanted.len());
        for (event, (revision, key, prev)) in events.iter().zip(wanted) {
            let kv = event.kv.as_ref().unwrap();
            assert_eq!((kv.mod_revision, &kv.key), (*revision, key));
            let prev = prev.clone().filter(|_| *revision > compact);
            assert_eq!(&event.prev_kv, &prev, "revision {revision}");
            // Journals omit Put values. Watch must load them from the history versions.
            let value = values.iter().find(|(r, k, _)| (r, k) == (revision, key));
            if event.r#type == 0 {
                assert_eq!(
                    Some(&kv.value),
                    value.map(|(_, _, v)| v),
                    "revision {revision}"
                );
            } else {
                assert!(value.is_none());
            }
        }
    };
    check_events(events(&state, 1).await, 1, -1);

    // Compaction keeps the previous version of each event after the boundary.
    let boundary = base + 2;
    state
        .apply_compaction(
            state.applied + 1,
            &CompactionRequest {
                revision: boundary,
                physical: true,
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    let after = events(&state, boundary).await;
    assert!(after[0].prev_kv.is_none());
    check_events(after, boundary, boundary);
    // The physical pass keeps the history version of every event at or after the boundary.
    super::compaction_tests::finish(&mut state).await;
    check_events(events(&state, boundary).await, boundary, boundary);

    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = state.applied;
    snapshot.set_data(state.snapshot_data().await.unwrap());
    let mut restored = StateMachine::open(other.path()).await.unwrap();
    restored.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(
        events(&restored, boundary).await,
        events(&state, boundary).await
    );
    restored.close().await.unwrap();
    state.close().await.unwrap();
}

#[tokio::test]
async fn watch_omits_a_missing_previous_value() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    apply(&mut state, &put(b"a", b"first")).await;
    let first = state.revision;
    apply(&mut state, &put(b"a", b"second")).await;
    assert!(events(&state, first + 1).await[0].prev_kv.is_some());

    // Like etcd, Watch leaves prev_kv empty when the previous version cannot be found.
    state
        .db()
        .delete(crate::mvcc::history_key(b"a", first))
        .await
        .unwrap();
    let events = events(&state, first + 1).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kv.as_ref().unwrap().value, b"second");
    assert!(events[0].prev_kv.is_none());
    state.close().await.unwrap();
}
