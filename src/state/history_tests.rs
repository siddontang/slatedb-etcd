//! Check historical Range reads against a model of every past revision.

use std::collections::BTreeMap;

use super::*;
use crate::lease::LeaseCommand;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::*;
use crate::proto::mvccpb::KeyValue;

type Model = BTreeMap<i64, BTreeMap<Vec<u8>, KeyValue>>;

struct Random(u64);

impl Random {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % bound
    }
}

// Prefix pairs and high bytes check that the history key order matches the user key order.
const KEYS: &[&[u8]] = &[
    b"a",
    b"a\0",
    b"a\x01",
    b"ab",
    b"a\xff",
    b"b",
    b"b/",
    b"b0",
    b"\xff",
    b"\xff\xff",
];

fn key(random: &mut Random) -> Vec<u8> {
    KEYS[random.next(KEYS.len() as u64) as usize].to_vec()
}

fn range(key: &[u8], end: &[u8], revision: i64) -> RangeRequest {
    RangeRequest {
        key: key.to_vec(),
        range_end: end.to_vec(),
        revision,
        ..Default::default()
    }
}

async fn apply(state: &mut StateMachine, request: Op) {
    state
        .apply_etcd(
            state.applied + 1,
            &RequestOp {
                request: Some(request),
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
}

/// Record the current state with current-revision reads.
async fn record(state: &StateMachine, model: &mut Model) {
    let reply = read(state, range(&[0], &[0], 0)).await.unwrap();
    model.insert(
        state.revision,
        reply
            .kvs
            .into_iter()
            .map(|kv| (kv.key.clone(), kv))
            .collect(),
    );
}

async fn read(state: &StateMachine, request: RangeRequest) -> Result<RangeResponse, i32> {
    let request = RequestOp {
        request: Some(Op::RequestRange(request)),
    };
    // Pinned views and the live database use different scan paths.
    let pinned = state
        .read_view()
        .unwrap()
        .read(&request, None)
        .await
        .unwrap();
    let live = state.read_etcd(&request).await.unwrap();
    assert_eq!(
        serde_json::to_value(&pinned).unwrap(),
        serde_json::to_value(&live).unwrap()
    );
    match live.result {
        Ok(ResponseOp {
            response: Some(Resp::ResponseRange(reply)),
        }) => Ok(reply),
        Ok(other) => panic!("unexpected response {other:?}"),
        Err(error) => Err(error.code),
    }
}

fn expected(model: &Model, revision: i64, key: &[u8], end: &[u8]) -> Vec<KeyValue> {
    model[&revision]
        .values()
        .filter(|kv| crate::mvcc::contains(&kv.key, key, end))
        .cloned()
        .collect()
}

/// Read `[key, end)` at `revision` in pages of `limit` rows, as a paginated LIST does.
async fn check_pages(
    state: &StateMachine,
    model: &Model,
    revision: i64,
    key: &[u8],
    end: &[u8],
    limit: i64,
) {
    let all = expected(model, revision, key, end);
    let mut start = key.to_vec();
    let mut seen = vec![];
    loop {
        let reply = read(
            state,
            RangeRequest {
                limit,
                ..range(&start, end, revision)
            },
        )
        .await
        .unwrap();
        let rest = expected(model, revision, &start, end);
        let context = format!("revision {revision} start {start:?} end {end:?} limit {limit}");
        assert_eq!(reply.count, rest.len() as i64, "{context}");
        assert_eq!(reply.more, rest.len() as i64 > limit, "{context}");
        assert_eq!(
            reply.kvs,
            rest[..rest.len().min(limit as usize)],
            "{context}"
        );
        assert_eq!(reply.header.unwrap().revision, state.revision);
        seen.extend(reply.kvs.iter().cloned());
        if !reply.more {
            break;
        }
        start = [reply.kvs.last().unwrap().key.as_slice(), b"\0"].concat();
    }
    assert_eq!(seen, all);
}

/// Compare every retained revision with the model.
async fn check(state: &StateMachine, model: &Model, random: &mut Random) {
    let first = state.compact_revision.max(1);
    for revision in first..=state.revision {
        for (key, end) in [
            (vec![0], vec![0]),
            (b"a".to_vec(), b"b".to_vec()),
            (b"a".to_vec(), b"a\x01".to_vec()),
            (b"a\xff".to_vec(), vec![0]),
            (b"b".to_vec(), b"a".to_vec()),
        ] {
            check_pages(
                state,
                model,
                revision,
                &key,
                &end,
                1 + random.next(4) as i64,
            )
            .await;
        }
        for key in KEYS {
            let reply = read(state, range(key, b"", revision)).await.unwrap();
            assert_eq!(
                reply.kvs,
                expected(model, revision, key, b""),
                "{key:?}@{revision}"
            );
        }
        // Sorted, filtered, and count-only reads collect the whole interval.
        let all = expected(model, revision, &[0], &[0]);
        let reply = read(
            state,
            RangeRequest {
                count_only: true,
                limit: 1,
                ..range(&[0], &[0], revision)
            },
        )
        .await
        .unwrap();
        assert_eq!((reply.count, reply.kvs.len()), (all.len() as i64, 0));
        let reply = read(
            state,
            RangeRequest {
                sort_order: 2,
                limit: 2,
                keys_only: true,
                ..range(&[0], &[0], revision)
            },
        )
        .await
        .unwrap();
        let keys: Vec<_> = all.iter().rev().take(2).map(|kv| kv.key.clone()).collect();
        assert_eq!(
            reply.kvs.iter().map(|kv| &kv.key).collect::<Vec<_>>(),
            keys.iter().collect::<Vec<_>>()
        );
        assert!(reply.kvs.iter().all(|kv| kv.value.is_empty()));
        let reply = read(
            state,
            RangeRequest {
                min_mod_revision: revision,
                limit: 1,
                ..range(&[0], &[0], revision)
            },
        )
        .await
        .unwrap();
        let recent: Vec<_> = all
            .iter()
            .filter(|kv| kv.mod_revision >= revision)
            .cloned()
            .collect();
        assert_eq!(reply.count, all.len() as i64);
        assert_eq!(reply.kvs, recent[..recent.len().min(1)]);
    }
    if state.compact_revision > 1 {
        let reply = read(state, range(&[0], &[0], state.compact_revision - 1)).await;
        assert_eq!(reply.unwrap_err(), tonic::Code::OutOfRange as i32);
    }
}

#[tokio::test]
async fn historical_ranges_match_every_retained_revision() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
    let mut random = Random(66);
    let mut model = Model::new();
    record(&state, &mut model).await;
    state
        .apply_lease(state.applied + 1, &LeaseCommand::Grant { id: 7, ttl: 60 })
        .await
        .unwrap()
        .result
        .unwrap();
    let mut revoked = false;
    for round in 0..12 {
        // Hot keys get more versions than the forward probe reads.
        for step in 0..10 {
            let request = match random.next(10) {
                0..=3 => Op::RequestPut(PutRequest {
                    key: key(&mut random),
                    value: format!("{round}-{step}").into_bytes(),
                    lease: if !revoked && random.next(3) == 0 {
                        7
                    } else {
                        0
                    },
                    ..Default::default()
                }),
                4..=6 => Op::RequestPut(PutRequest {
                    key: b"a".to_vec(),
                    value: format!("hot-{round}-{step}").into_bytes(),
                    ..Default::default()
                }),
                7 => Op::RequestDeleteRange(DeleteRangeRequest {
                    key: key(&mut random),
                    ..Default::default()
                }),
                8 => Op::RequestDeleteRange(DeleteRangeRequest {
                    key: b"a\x01".to_vec(),
                    range_end: b"b0".to_vec(),
                    ..Default::default()
                }),
                // One revision changes several keys.
                _ => Op::RequestTxn(TxnRequest {
                    success: [
                        Op::RequestPut(PutRequest {
                            key: b"b".to_vec(),
                            value: format!("txn-{round}-{step}").into_bytes(),
                            ..Default::default()
                        }),
                        Op::RequestPut(PutRequest {
                            key: b"\xff".to_vec(),
                            value: b"txn".to_vec(),
                            ..Default::default()
                        }),
                        Op::RequestDeleteRange(DeleteRangeRequest {
                            key: b"ab".to_vec(),
                            ..Default::default()
                        }),
                    ]
                    .into_iter()
                    .map(|request| RequestOp {
                        request: Some(request),
                    })
                    .collect(),
                    ..Default::default()
                }),
            };
            let before = state.revision;
            apply(&mut state, request).await;
            if state.revision != before {
                record(&state, &mut model).await;
            }
        }
        if round == 6 && !revoked {
            // Revoking the lease deletes its keys at one revision.
            state
                .apply_lease(state.applied + 1, &LeaseCommand::Revoke { id: 7 })
                .await
                .unwrap();
            revoked = true;
            record(&state, &mut model).await;
        }
        check(&state, &model, &mut random).await;
        if round % 3 == 2 {
            let target = state.compact_revision.max(1) + random.next(10) as i64;
            let target = target.min(state.revision);
            state
                .apply_compaction(
                    state.applied + 1,
                    &CompactionRequest {
                        revision: target,
                        physical: false,
                    },
                )
                .await
                .unwrap()
                .result
                .unwrap();
            // Reads during the pending pass see rows that the pass will remove.
            check(&state, &model, &mut random).await;
            if round % 2 == 0 {
                super::compaction_tests::finish(&mut state).await;
                check(&state, &model, &mut random).await;
            }
        }
    }
    super::compaction_tests::finish(&mut state).await;
    check(&state, &model, &mut random).await;
    let (revision, compacted) = (state.revision, state.compact_revision);
    assert!(revision > 50 && compacted > 1, "{revision} {compacted}");
}

/// A page at an old revision returns the old versions when many newer versions follow each key.
#[tokio::test]
async fn historical_pages_skip_versions_after_the_revision() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
    for key in 0..50u8 {
        apply(
            &mut state,
            Op::RequestPut(PutRequest {
                key: vec![b'k', key],
                value: b"old".to_vec(),
                ..Default::default()
            }),
        )
        .await;
    }
    let old = state.revision;
    for round in 0..40 {
        for key in 0..50u8 {
            apply(
                &mut state,
                Op::RequestPut(PutRequest {
                    key: vec![b'k', key],
                    value: format!("new-{round}").into_bytes(),
                    ..Default::default()
                }),
            )
            .await;
        }
    }
    let reply = read(
        &state,
        RangeRequest {
            limit: 10,
            ..range(b"k", b"l", old)
        },
    )
    .await
    .unwrap();
    assert_eq!((reply.count, reply.kvs.len(), reply.more), (50, 10, true));
    assert!(reply
        .kvs
        .iter()
        .all(|kv| kv.value == b"old" && kv.mod_revision <= old));
}

/// Count the bytes that the test thread allocates while `PEAK_TRACKING` is set.
struct PeakAlloc;

thread_local! {
    static PEAK_TRACKING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static PEAK_LIVE: std::cell::Cell<isize> = const { std::cell::Cell::new(0) };
    static PEAK_MAX: std::cell::Cell<isize> = const { std::cell::Cell::new(0) };
}

fn track(delta: isize) {
    let _ = PEAK_TRACKING.try_with(|tracking| {
        if tracking.get() {
            let live = PEAK_LIVE.get() + delta;
            PEAK_LIVE.set(live);
            PEAK_MAX.set(PEAK_MAX.get().max(live));
        }
    });
}

// SAFETY: every call forwards to the system allocator unchanged.
unsafe impl std::alloc::GlobalAlloc for PeakAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let ptr = unsafe { std::alloc::System.alloc(layout) };
        if !ptr.is_null() {
            track(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        track(-(layout.size() as isize));
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        let new = unsafe { std::alloc::System.realloc(ptr, layout, size) };
        if !new.is_null() {
            track(size as isize - layout.size() as isize);
        }
        new
    }
}

#[global_allocator]
static PEAK_ALLOC: PeakAlloc = PeakAlloc;

/// Read `request` from a pinned view and return the reply with the peak bytes that the read
/// allocated on this thread.
async fn peak_read(state: &StateMachine, request: RangeRequest) -> (RangeResponse, usize) {
    let request = RequestOp {
        request: Some(Op::RequestRange(request)),
    };
    let view = state.read_view().unwrap();
    PEAK_LIVE.set(0);
    PEAK_MAX.set(0);
    PEAK_TRACKING.set(true);
    let outcome = view.read(&request, None).await;
    PEAK_TRACKING.set(false);
    let peak = PEAK_MAX.get() as usize;
    match outcome.unwrap().result {
        Ok(ResponseOp {
            response: Some(Resp::ResponseRange(reply)),
        }) => (reply, peak),
        other => panic!("unexpected response {other:?}"),
    }
}

const LARGE: usize = 512 << 10;
const LARGE_KEYS: u8 = 16;

async fn put(state: &mut StateMachine, key: Vec<u8>, value: Vec<u8>) {
    let request = Op::RequestPut(PutRequest {
        key,
        value,
        ..Default::default()
    });
    apply(state, request).await;
}

/// A page and a count at a revision before many large values changed hold at most the page.
#[tokio::test]
async fn journal_pages_hold_only_the_values_of_the_page() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
    for key in 0..LARGE_KEYS {
        put(&mut state, vec![b'k', key], vec![key; LARGE]).await;
    }
    let old = state.revision;
    for key in 0..LARGE_KEYS {
        put(&mut state, vec![b'k', key], b"small".to_vec()).await;
    }
    let all = usize::from(LARGE_KEYS) * LARGE;
    for (count_only, limit) in [(true, 0), (false, 1)] {
        let request = RangeRequest {
            count_only,
            limit,
            ..range(b"k", b"l", old)
        };
        let (reply, peak) = peak_read(&state, request).await;
        assert_eq!(reply.count, i64::from(LARGE_KEYS));
        assert_eq!(reply.kvs.len(), limit as usize);
        assert!(
            reply.kvs.iter().all(|kv| kv.value.len() == LARGE),
            "{count_only} {limit}"
        );
        // The page, its older version, one decoded version, and the debug history check each
        // hold a few values. Holding every changed value takes more than all of them.
        assert!(
            peak < all / 2,
            "count_only {count_only} limit {limit}: peak {peak} of {all} bytes"
        );
    }
}

/// A page of an unrelated prefix does not copy the values that a later range delete removed.
#[tokio::test]
async fn journal_pages_skip_the_values_of_unrelated_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
    put(&mut state, b"a".to_vec(), b"a".to_vec()).await;
    for key in 0..LARGE_KEYS {
        put(&mut state, vec![b'd', key], vec![key; LARGE]).await;
    }
    let old = state.revision;
    let request = Op::RequestDeleteRange(DeleteRangeRequest {
        key: b"d".to_vec(),
        range_end: b"e".to_vec(),
        ..Default::default()
    });
    apply(&mut state, request).await;
    let all = usize::from(LARGE_KEYS) * LARGE;
    let request = RangeRequest {
        limit: 1,
        ..range(b"a", b"b", old)
    };
    let (reply, peak) = peak_read(&state, request).await;
    assert_eq!((reply.count, reply.kvs.len()), (1, 1));
    assert!(peak < all / 4, "peak {peak} of {all} bytes");
}

/// Read `request` and return the reply with the versions that its journal page read.
async fn counted_read(state: &StateMachine, request: RangeRequest) -> (RangeResponse, u64) {
    crate::mvcc::JOURNAL_VERSION_READS.set(0);
    let reply = read(state, request).await.unwrap();
    (reply, crate::mvcc::JOURNAL_VERSION_READS.get())
}

/// The count of a page at an old revision follows the first later event of each key: a new key,
/// a deleted key, a key deleted and created again, and a key created and deleted again. Only the
/// named keys that the page holds read a version.
#[tokio::test]
async fn historical_counts_follow_the_first_later_event() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
    let mut model = Model::new();
    let put = |key: &[u8], value: &str| {
        Op::RequestPut(PutRequest {
            key: key.to_vec(),
            value: value.as_bytes().to_vec(),
            ..Default::default()
        })
    };
    let delete = |key: &[u8]| {
        Op::RequestDeleteRange(DeleteRangeRequest {
            key: key.to_vec(),
            ..Default::default()
        })
    };
    for key in [b"k1", b"k3", b"k5", b"k7"] {
        apply(&mut state, put(key, "old")).await;
    }
    record(&state, &mut model).await;
    let old = state.revision;
    let later = [
        put(b"k0", "new"), // created after `old`
        delete(b"k1"),     // deleted after `old`
        put(b"k2", "new"), // created, then deleted
        delete(b"k2"),
        delete(b"k3"), // deleted, then created again
        put(b"k3", "again"),
        put(b"k5", "update"), // updated twice
        put(b"k5", "update2"),
        delete(b"k7"),     // deleted after the page
        put(b"k9", "new"), // created after the page
    ];
    for request in later {
        apply(&mut state, request).await;
        record(&state, &mut model).await;
    }
    for revision in old..=state.revision {
        for limit in 1..=5 {
            check_pages(&state, &model, revision, b"k", b"l", limit).await;
        }
    }
    let request = RangeRequest {
        limit: 2,
        ..range(b"k", b"l", old)
    };
    let (reply, reads) = counted_read(&state, request).await;
    assert_eq!((reply.count, reply.more), (4, true));
    assert_eq!(
        reply
            .kvs
            .iter()
            .map(|kv| kv.key.as_slice())
            .collect::<Vec<_>>(),
        [b"k1", b"k3"]
    );
    assert!(reply.kvs.iter().all(|kv| kv.value == b"old"));
    // The page holds two named keys and reads one more row to set `more`. The count reads none
    // of the six named keys.
    assert_eq!(reads, 3);
    let request = RangeRequest {
        count_only: true,
        ..range(b"k", b"l", old)
    };
    let (reply, reads) = counted_read(&state, request).await;
    assert_eq!((reply.count, reads), (4, 0));
}

/// A sorted current Range with a limit keeps only its best rows, even when each later row
/// replaces the winner.
#[tokio::test]
async fn sorted_pages_hold_only_the_kept_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
    for key in 0..LARGE_KEYS {
        put(&mut state, vec![b'k', key], vec![key; LARGE]).await;
    }
    let all = usize::from(LARGE_KEYS) * LARGE;
    let request = RangeRequest {
        limit: 1,
        sort_order: 2,
        ..range(b"k", b"l", 0)
    };
    let (reply, peak) = peak_read(&state, request).await;
    assert_eq!(reply.count, i64::from(LARGE_KEYS));
    assert_eq!(reply.kvs.len(), 1);
    assert_eq!(reply.kvs[0].key, vec![b'k', LARGE_KEYS - 1]);
    // The winner, its replacement, and the scan's blocks hold a few values. Sorting the whole
    // interval holds every value.
    assert!(peak < all / 2, "peak {peak} of {all} bytes");
}
