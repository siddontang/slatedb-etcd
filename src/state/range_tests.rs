//! Range assembly against a model of every version, across sort orders, limits, filters,
//! revisions, and the response byte cap.

use super::*;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::*;
use crate::proto::mvccpb::KeyValue;
use std::collections::BTreeMap;

fn range_op(r: RangeRequest) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestRange(r)),
    }
}

async fn read_range(
    view: &ReadView,
    r: &RangeRequest,
) -> Result<RangeResponse, crate::mvcc::KvError> {
    let response = view
        .read(&range_op(r.clone()), None)
        .await
        .unwrap()
        .result?;
    let Some(Resp::ResponseRange(reply)) = response.response else {
        panic!("expected range");
    };
    Ok(reply)
}

/// Every version of every key: `(revision, value)` in revision order; `None` is a delete.
type Model = BTreeMap<Vec<u8>, Vec<(i64, Option<KeyValue>)>>;

/// The pre-streaming algorithm: collect the interval at the revision, filter, sort stably,
/// truncate, then clear values for keys-only reads.
fn expected(model: &Model, r: &RangeRequest, current: i64) -> RangeResponse {
    let at = if r.revision > 0 { r.revision } else { current };
    let rows: Vec<KeyValue> = model
        .iter()
        .filter(|(key, _)| crate::mvcc::contains(key, &r.key, &r.range_end))
        .filter_map(|(_, versions)| {
            versions
                .iter()
                .rev()
                .find(|(revision, _)| *revision <= at)
                .and_then(|(_, kv)| kv.clone())
        })
        .collect();
    let count = rows.len() as i64;
    let mut kvs: Vec<KeyValue> = if r.count_only {
        vec![]
    } else {
        rows.into_iter()
            .filter(|kv| {
                (r.min_mod_revision == 0 || kv.mod_revision >= r.min_mod_revision)
                    && (r.max_mod_revision == 0 || kv.mod_revision <= r.max_mod_revision)
                    && (r.min_create_revision == 0 || kv.create_revision >= r.min_create_revision)
                    && (r.max_create_revision == 0 || kv.create_revision <= r.max_create_revision)
            })
            .collect()
    };
    kvs.sort_by(|a, b| {
        let order = match r.sort_target {
            1 => a.version.cmp(&b.version),
            2 => a.create_revision.cmp(&b.create_revision),
            3 => a.mod_revision.cmp(&b.mod_revision),
            4 => a.value.cmp(&b.value),
            _ => a.key.cmp(&b.key),
        };
        if r.sort_order == 2 {
            order.reverse()
        } else {
            order
        }
    });
    let more = r.limit > 0 && kvs.len() as i64 > r.limit;
    if more {
        kvs.truncate(r.limit as usize);
    }
    if r.keys_only {
        for kv in &mut kvs {
            kv.value.clear();
        }
    }
    RangeResponse {
        header: crate::mvcc::header(current),
        kvs,
        more,
        count,
    }
}

fn key(n: u64) -> Vec<u8> {
    // Short keys that are prefixes of longer ones check the history key order.
    match n % 3 {
        0 => format!("k{}", n / 3).into_bytes(),
        1 => format!("k{}x", n / 3).into_bytes(),
        _ => format!("k{}\u{1}", n / 3).into_bytes(),
    }
}

#[tokio::test]
async fn ranges_match_the_collected_model() {
    for seed in 1..=3u64 {
        let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut random = move |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x % n
        };
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        let mut model = Model::new();
        for index in 1..=120u64 {
            let k = key(random(18));
            let request = if random(5) == 0 {
                RequestOp {
                    request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
                        key: k.clone(),
                        ..Default::default()
                    })),
                }
            } else {
                let size = random(3) as usize * 40;
                RequestOp {
                    request: Some(Op::RequestPut(PutRequest {
                        key: k.clone(),
                        value: vec![b'a' + random(26) as u8; size + 1],
                        ..Default::default()
                    })),
                }
            };
            let before = state.revision;
            state
                .apply_etcd(index, &request)
                .await
                .unwrap()
                .result
                .unwrap();
            if state.revision == before {
                continue;
            }
            // A point read decodes the stored row directly.
            let view = state.read_view().unwrap();
            let current = read_range(
                &view,
                &RangeRequest {
                    key: k.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            model
                .entry(k)
                .or_default()
                .push((state.revision, current.kvs.into_iter().next()));
        }
        let view = state.read_view().unwrap();
        for _ in 0..600 {
            let a = key(random(20));
            let (start, end) = match random(5) {
                0 => (vec![0], vec![0]),
                1 => (a, vec![]),
                2 => (a, vec![0]),
                _ => (a, key(random(20))),
            };
            let revision = match random(3) {
                0 => 0,
                _ => 1 + random(state.revision as u64) as i64,
            };
            let filter = |random: &mut dyn FnMut(u64) -> u64| match random(4) {
                0 => 1 + random(state.revision as u64) as i64,
                _ => 0,
            };
            let r = RangeRequest {
                key: start,
                range_end: end,
                revision,
                limit: random(5) as i64,
                sort_order: random(3) as i32,
                sort_target: random(5) as i32,
                count_only: random(6) == 0,
                keys_only: random(3) == 0,
                min_mod_revision: filter(&mut random),
                max_mod_revision: filter(&mut random),
                min_create_revision: filter(&mut random),
                max_create_revision: filter(&mut random),
                ..Default::default()
            };
            let actual = read_range(&view, &r).await.unwrap();
            assert_eq!(actual, expected(&model, &r, state.revision), "{r:?}");
            // A read-only transaction returns the same Range.
            let txn = RequestOp {
                request: Some(Op::RequestTxn(TxnRequest {
                    success: vec![range_op(r.clone())],
                    ..Default::default()
                })),
            };
            let response = view.read(&txn, None).await.unwrap().result.unwrap();
            let Some(Resp::ResponseTxn(mut txn)) = response.response else {
                panic!("expected txn");
            };
            let Some(Resp::ResponseRange(mut nested)) = txn.responses.remove(0).response else {
                panic!("expected range");
            };
            nested.header = actual.header;
            assert_eq!(nested, actual, "txn {r:?}");
        }
        drop(view);
        state.close().await.unwrap();
    }
}

#[tokio::test]
async fn response_cap_bounds_kept_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    // Ten rows of about 1 KiB; the cap holds about three of them.
    for index in 1..=10u64 {
        let put = RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: format!("k{index:02}").into_bytes(),
                value: vec![b'v'; 1000],
                ..Default::default()
            })),
        };
        state.apply_etcd(index, &put).await.unwrap().result.unwrap();
    }
    state.max_response_bytes = Some(3500);
    let view = state.read_view().unwrap();
    let all = |revision, sort_order, sort_target, limit| RangeRequest {
        key: b"k".to_vec(),
        range_end: b"l".to_vec(),
        revision,
        sort_order,
        sort_target,
        limit,
        ..Default::default()
    };
    for revision in [0, 11] {
        for (sort_order, sort_target) in [(0, 0), (2, 0), (1, 3), (2, 4)] {
            // The whole interval exceeds the cap.
            let error = read_range(&view, &all(revision, sort_order, sort_target, 0))
                .await
                .unwrap_err();
            assert_eq!(error.code, tonic::Code::ResourceExhausted as i32);
            assert_eq!(error.message, crate::mvcc::TOO_LARGE);
            // A limit keeps only its rows, so the read fits.
            let reply = read_range(&view, &all(revision, sort_order, sort_target, 3))
                .await
                .unwrap();
            assert_eq!((reply.kvs.len(), reply.more, reply.count), (3, true, 10));
            // Counts and keys read no values, so they fit.
            let mut count = all(revision, sort_order, sort_target, 0);
            count.count_only = true;
            assert_eq!(read_range(&view, &count).await.unwrap().count, 10);
            if sort_target != 4 {
                let mut keys = all(revision, sort_order, sort_target, 0);
                keys.keys_only = true;
                assert_eq!(read_range(&view, &keys).await.unwrap().kvs.len(), 10);
            }
        }
    }
    assert_eq!(state.metrics.too_large_reads.get(), 8);
    // A transaction counts the rows of all its Ranges together.
    let two = RequestOp {
        request: Some(Op::RequestTxn(TxnRequest {
            success: vec![range_op(all(0, 0, 0, 2)), range_op(all(0, 0, 0, 2))],
            ..Default::default()
        })),
    };
    let error = view.read(&two, None).await.unwrap().result.unwrap_err();
    assert_eq!(error.message, crate::mvcc::TOO_LARGE);
    assert_eq!(state.metrics.too_large_reads.get(), 9);
    // Apply-side reads never use the member-local cap: a write transaction returns every row.
    let write = RequestOp {
        request: Some(Op::RequestTxn(TxnRequest {
            success: vec![
                range_op(all(0, 0, 0, 0)),
                RequestOp {
                    request: Some(Op::RequestPut(PutRequest {
                        key: b"z".to_vec(),
                        ..Default::default()
                    })),
                },
            ],
            ..Default::default()
        })),
    };
    let response = state.apply_etcd(11, &write).await.unwrap().result.unwrap();
    let Some(Resp::ResponseTxn(txn)) = response.response else {
        panic!("expected txn");
    };
    let Some(Resp::ResponseRange(range)) = &txn.responses[0].response else {
        panic!("expected range");
    };
    assert_eq!(range.kvs.len(), 10);
    drop(view);
    state.close().await.unwrap();
}
