//! Range assembly against a model of every version, across sort orders, limits, filters,
//! and revisions.

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
    let filters = r.min_mod_revision != 0
        || r.max_mod_revision != 0
        || r.min_create_revision != 0
        || r.max_create_revision != 0;
    // Like etcd, a sort target without a sort order and without filters sorts only the first
    // `limit + 1` keys.
    let page = r.sort_order == 0 && r.sort_target != 0 && !filters && r.limit > 0;
    let mut kvs: Vec<KeyValue> = if r.count_only {
        vec![]
    } else {
        rows.into_iter()
            .take(if page {
                r.limit as usize + 1
            } else {
                usize::MAX
            })
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

/// A pinned state whose scans fail after `rows` rows.
struct FailAfter {
    state: StateView,
    rows: usize,
}

struct FailScan<'a> {
    scan: <StateView as StateRead>::Scan<'a>,
    left: usize,
}

impl StateScan for FailScan<'_> {
    async fn next(&mut self) -> Result<Option<crate::state::Row>, slatedb::Error> {
        if self.left == 0 {
            return Err(slatedb::Error::unavailable("injected scan failure".into()));
        }
        self.left -= 1;
        self.scan.next().await
    }

    async fn seek(&mut self, key: &[u8]) -> Result<(), slatedb::Error> {
        self.scan.seek(key).await
    }
}

impl StateRead for FailAfter {
    type Scan<'a> = FailScan<'a>;

    async fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>, slatedb::Error> {
        self.state.get(key).await
    }

    async fn scan_range(
        &self,
        range: std::ops::Range<Vec<u8>>,
        options: &slatedb::config::ScanOptions,
    ) -> Result<FailScan<'_>, slatedb::Error> {
        Ok(FailScan {
            scan: self.state.scan_range(range, options).await?,
            left: self.rows,
        })
    }

    async fn scan_under(
        &self,
        prefix: &[u8],
        options: &slatedb::config::ScanOptions,
    ) -> Result<FailScan<'_>, slatedb::Error> {
        Ok(FailScan {
            scan: self.state.scan_under(prefix, options).await?,
            left: self.rows,
        })
    }
}

/// A page whose rows, count, and `more` are known does not read the next row of the interval.
#[tokio::test]
async fn complete_pages_stop_before_the_next_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    for index in 1..=3u64 {
        let put = RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: format!("k{index}").into_bytes(),
                value: b"v".to_vec(),
                ..Default::default()
            })),
        };
        state.apply_etcd(index, &put).await.unwrap().result.unwrap();
    }
    let view = state.read_view().unwrap();
    // A page of one row needs two rows: the second sets `more`. The third read fails.
    let db = FailAfter {
        state: view.read_state(),
        rows: 2,
    };
    let page = |sort_target| {
        range_op(RangeRequest {
            key: b"k".to_vec(),
            range_end: b"l".to_vec(),
            limit: 1,
            sort_target,
            ..Default::default()
        })
    };
    for sort_target in [0, 4] {
        let response = view.read_from(&db, &page(sort_target)).await.unwrap();
        let Some(Resp::ResponseRange(reply)) = response.response else {
            panic!("expected range");
        };
        assert_eq!(
            (reply.kvs[0].key.as_slice(), reply.more, reply.count),
            (b"k1".as_slice(), true, 3),
            "sort target {sort_target}"
        );
    }
    drop(view);
    state.close().await.unwrap();
}
