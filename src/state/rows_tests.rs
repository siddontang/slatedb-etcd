//! Randomized checks of the shared row cache against storage without the cache.

use super::*;
use crate::lease::LeaseCommand as Lease;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::*;

struct Random(u64);

impl Random {
    fn next(&mut self, bound: u64) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % bound
    }
}

const KEYS: [&[u8]; 6] = [b"a", b"b", b"c", b"d", b"e", b"f"];

/// Open a test state with the row cache budget of `cache`.
async fn open(dir: &std::path::Path, cache: StateCache) -> StateMachine {
    let store = crate::object::test_store(dir);
    let options = shared::tests::options();
    StateMachine::open_shared(store, "cluster", "test", cache, &options)
        .await
        .unwrap()
}

fn key(random: &mut Random) -> Vec<u8> {
    KEYS[random.next(KEYS.len() as u64) as usize].to_vec()
}

fn put(key: Vec<u8>, value: u64, lease: i64) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestPut(PutRequest {
            key,
            value: format!("v{value}").into_bytes(),
            lease,
            ..Default::default()
        })),
    }
}

fn delete(key: Vec<u8>, range_end: Vec<u8>) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
            key,
            range_end,
            ..Default::default()
        })),
    }
}

fn get(key: Vec<u8>, revision: i64) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestRange(RangeRequest {
            key,
            revision,
            ..Default::default()
        })),
    }
}

fn txn(random: &mut Random) -> RequestOp {
    let target = key(random);
    RequestOp {
        request: Some(Op::RequestTxn(TxnRequest {
            compare: vec![Compare {
                key: target.clone(),
                result: compare::CompareResult::Greater as i32,
                target: compare::CompareTarget::Mod as i32,
                target_union: Some(compare::TargetUnion::ModRevision(random.next(40) as i64)),
                ..Default::default()
            }],
            success: vec![put(key(random), random.next(1000), 0), get(target, 0)],
            failure: vec![delete(key(random), vec![]), get(key(random), 0)],
        })),
    }
}

/// A read-only request: a point Range at the head or at a past revision, or a read-only Txn.
fn read(random: &mut Random, revision: i64) -> RequestOp {
    match random.next(4) {
        0 | 1 => get(key(random), 0),
        2 => get(key(random), 1 + random.next(revision.max(1) as u64) as i64),
        _ => RequestOp {
            request: Some(Op::RequestTxn(TxnRequest {
                compare: vec![Compare {
                    key: key(random),
                    result: compare::CompareResult::Equal as i32,
                    target: compare::CompareTarget::Version as i32,
                    target_union: Some(compare::TargetUnion::Version(random.next(3) as i64)),
                    ..Default::default()
                }],
                success: vec![get(key(random), 0)],
                failure: vec![get(key(random), 0), get(key(random), 0)],
            })),
        },
    }
}

fn outcome_text(outcome: Outcome) -> String {
    match outcome.result {
        Ok(response) => format!("{response:?}"),
        Err(error) => format!("error {} {}", error.code, error.message),
    }
}

/// Apply one random write to `state`. `lease` is a granted lease id or 0.
async fn write(state: &mut StateMachine, random: &mut Random, lease: &mut i64) {
    let index = state.applied + 1;
    match random.next(12) {
        0..=3 => {
            let lease = if random.next(3) == 0 { *lease } else { 0 };
            let request = put(key(random), random.next(1000), lease);
            state.apply_etcd(index, &request).await.unwrap();
        }
        4 => {
            // One grouped state write for several entries.
            let requests: Vec<_> = (0..1 + random.next(4))
                .map(|_| put(key(random), random.next(1000), 0))
                .collect();
            let entries: Vec<_> = requests
                .iter()
                .enumerate()
                .map(|(offset, request)| put_batch::KvEntry {
                    index: index + offset as u64,
                    request,
                    credentials: None,
                    origin: 0,
                })
                .collect();
            state.apply_puts(&entries).await.unwrap();
        }
        5 => {
            state
                .apply_etcd(index, &delete(key(random), vec![]))
                .await
                .unwrap();
        }
        6 => {
            let request = delete(b"b".to_vec(), b"e".to_vec());
            state.apply_etcd(index, &request).await.unwrap();
        }
        7 | 8 => {
            state.apply_etcd(index, &txn(random)).await.unwrap();
        }
        9 => {
            let command = if *lease == 0 {
                *lease = 1 + random.next(1000) as i64;
                Lease::Grant {
                    id: *lease,
                    ttl: 60,
                }
            } else {
                // Revoke deletes the attached keys.
                let id = std::mem::take(lease);
                Lease::Revoke { id }
            };
            state.apply_lease(index, &command).await.unwrap();
        }
        10 => {
            let request = CompactionRequest {
                revision: state.compact_revision.max(0) + 1 + random.next(3) as i64,
                physical: true,
            };
            state.apply_compaction(index, &request).await.unwrap();
        }
        _ => {
            state.apply(index, None).await.unwrap();
        }
    }
}

/// The same random writes and reads reach a state with the row cache and one without it.
/// Reads run concurrently through new and older pinned views while later writes apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_reads_match_uncached_reads() {
    for seed in 1..=6u64 {
        let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let small = StateCache {
            // Small enough for evictions, large enough to hold rows.
            row_bytes: 64 * 1024,
            ..StateCache::default()
        };
        let mut cached = open(dirs[0].path(), small).await;
        let off = StateCache {
            row_bytes: 0,
            ..StateCache::default()
        };
        let mut plain = open(dirs[1].path(), off).await;
        let (mut cached_lease, mut plain_lease) = (0, 0);
        let mut views: Vec<(Arc<ReadView>, Arc<ReadView>)> = vec![];
        let mut random = Random(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        for step in 0..300 {
            let state_seed = random.next(u64::MAX);
            write(&mut cached, &mut Random(state_seed | 1), &mut cached_lease).await;
            write(&mut plain, &mut Random(state_seed | 1), &mut plain_lease).await;
            assert_eq!(cached.applied, plain.applied);
            views.push((cached.read_view().unwrap(), plain.read_view().unwrap()));
            if views.len() > 8 {
                views.remove(random.next(views.len() as u64) as usize);
            }
            let mut reads = tokio::task::JoinSet::new();
            for _ in 0..6 {
                let (with, without) = views[random.next(views.len() as u64) as usize].clone();
                let request = read(&mut random, with.revision);
                reads.spawn(async move {
                    let expected = outcome_text(without.read(&request, None).await.unwrap());
                    // Read twice: the first read may fill, the second may hit.
                    for _ in 0..2 {
                        let actual = outcome_text(with.read(&request, None).await.unwrap());
                        assert_eq!(actual, expected, "seed {seed} step {step} {request:?}");
                    }
                });
            }
            // Later writes race with the reads of older views.
            write(&mut cached, &mut Random(state_seed ^ 7), &mut cached_lease).await;
            write(&mut plain, &mut Random(state_seed ^ 7), &mut plain_lease).await;
            while let Some(result) = reads.join_next().await {
                result.unwrap();
            }
        }
        // Every current row of the cached state matches a read without the cache.
        let (with, without) = (cached.read_view().unwrap(), plain.read_view().unwrap());
        for key in KEYS {
            let request = get(key.to_vec(), 0);
            assert_eq!(
                outcome_text(with.read(&request, None).await.unwrap()),
                outcome_text(without.read(&request, None).await.unwrap())
            );
        }
        // The reads above filled the cache, so the comparisons covered hits.
        let applied = cached.applied;
        assert!(KEYS
            .iter()
            .any(|key| cached.rows.get(key, applied, |_| ()).is_some()));
        drop(views);
        for state in [cached, plain] {
            state.close().await.unwrap();
        }
    }
}

/// Model check of fill and invalidation races under real threads.
///
/// One writer follows the apply order: invalidate, store a new version, then publish `applied`.
/// Readers pin a sequence at or below `applied`, look up the cache, and on a miss read the model
/// at that sequence and fill. A hit must equal the model row at the reader's sequence.
#[test]
fn concurrent_fills_and_invalidations_match_the_model() {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::RwLock;

    use prost::Message;

    type History = HashMap<Vec<u8>, Vec<(u64, Option<Vec<u8>>)>>;
    let row = |key: &[u8], seq: u64| {
        crate::proto::mvccpb::KeyValue {
            key: key.to_vec(),
            value: seq.to_be_bytes().to_vec(),
            mod_revision: seq as i64,
            ..Default::default()
        }
        .encode_to_vec()
    };
    let at = |history: &History, key: &[u8], seq: u64| {
        history.get(key).and_then(|versions| {
            versions
                .iter()
                .rev()
                .find(|(written, _)| *written <= seq)
                .and_then(|(_, row)| row.clone())
        })
    };
    // A small table forces evictions and set collisions.
    let cache = rows::RowCache::new(64 * 1024);
    let history = RwLock::new(History::new());
    let applied = AtomicU64::new(0);
    let done = AtomicBool::new(false);
    let hits = AtomicU64::new(0);
    std::thread::scope(|scope| {
        for reader in 0..6u64 {
            let (cache, history, applied, done, hits) = (&cache, &history, &applied, &done, &hits);
            scope.spawn(move || {
                let mut random = Random(reader + 11);
                while !done.load(Ordering::Acquire) {
                    let head = applied.load(Ordering::Acquire);
                    // Pinned views may be several applies old.
                    let seq = head.saturating_sub(random.next(4));
                    let key = 32u64.min(random.next(40)).to_be_bytes();
                    let found = cache.get(&key, seq, |row| row.to_vec());
                    let expected = at(&history.read().unwrap(), &key, seq);
                    match found {
                        Some(found) => {
                            assert_eq!(Some(found), expected, "key {key:?} seq {seq}");
                            hits.fetch_add(1, Ordering::Relaxed);
                        }
                        None => {
                            if let Some(row) = expected {
                                std::thread::yield_now();
                                cache.fill(&key, &row, seq);
                            }
                        }
                    }
                }
            });
        }
        let mut random = Random(5);
        for index in 1..=200_000u64 {
            let key = 32u64.min(random.next(40)).to_be_bytes();
            let value = (random.next(5) != 0).then(|| row(&key, index));
            cache.invalidate(&key, index);
            if random.next(8) == 0 {
                std::thread::yield_now();
            }
            history
                .write()
                .unwrap()
                .entry(key.to_vec())
                .or_default()
                .push((index, value));
            applied.store(index, Ordering::Release);
        }
        done.store(true, Ordering::Release);
    });
    assert!(hits.load(Ordering::Relaxed) > 0);
}
