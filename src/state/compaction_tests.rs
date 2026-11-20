//! Compare stepped physical compaction with the former single-batch compaction.
//!
//! The reference follows the single-batch path from upstream main, including its hash rules.

use super::*;
use crate::maintenance::HASH;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::*;
use prost::Message;

/// The former apply path. It removes all obsolete rows in one batch.
async fn reference_compaction(state: &mut StateMachine, request: &CompactionRequest) -> bool {
    let index = state.applied + 1;
    if mvcc::check_compaction(state.revision, state.compact_revision, request).is_err() {
        state.apply(index, None).await.unwrap();
        return false;
    }
    let db = state.db.clone();
    let mut batch = WriteBatch::new();
    let mut scan = db.scan_prefix(mvcc::HISTORY, ..).await.unwrap();
    let mut previous: Option<(Vec<u8>, bytes::Bytes, usize)> = None;
    while let Some(row) = scan.next().await.unwrap() {
        let len = row.value.len();
        let version = crate::proto::state::Version::decode(row.value).unwrap();
        if version.revision <= request.revision {
            if let Some((key, history_key, old_len)) = previous.take() {
                if key == version.key {
                    batch.delete_known(history_key, old_len);
                }
            }
            previous = Some((version.key, row.key, len));
        }
    }
    let mut scan = db.scan_prefix(HASH, ..).await.unwrap();
    let mut latest = std::collections::BTreeMap::new();
    while let Some(row) = scan.next().await.unwrap() {
        let main = i64::from_be_bytes(row.key[HASH.len()..HASH.len() + 8].try_into().unwrap());
        if main > request.revision {
            break;
        }
        let kv = crate::proto::mvccpb::KeyValue::decode(row.value.clone()).unwrap();
        let len = row.value.len();
        if let Some((old, old_len)) = latest.insert(kv.key, (row.key.clone(), len)) {
            batch.delete_known(old, old_len);
        }
        if row.key.len() == HASH.len() + 18 && main < request.revision {
            batch.delete_known(row.key, len);
        }
    }
    let mut scan = db
        .scan(crate::watch::EVENTS.to_vec()..crate::watch::key(request.revision))
        .await
        .unwrap();
    while let Some(row) = scan.next().await.unwrap() {
        batch.delete_known(row.key, row.value.len());
    }
    // The upstream apply path hashes the rows that the deletion batch retains.
    let hash = state
        .hash_retained(
            request.revision,
            |row| batch.get(row).is_none(),
            |_, _| true,
        )
        .await
        .unwrap();
    let mut hashes = state.hash_cache().await.unwrap();
    hashes.push(hash);
    if hashes.len() > 10 {
        hashes.remove(0);
    }
    batch.put(b"etcd/hash-cache", serde_json::to_vec(&hashes).unwrap());
    batch.put(mvcc::COMPACT_REVISION, request.revision.to_be_bytes());
    batch.put(APPLIED, index.to_be_bytes());
    state.write_batch(batch, index).await.unwrap();
    state.applied = index;
    state.compact_revision = request.revision;
    true
}

pub(super) async fn finish(state: &mut StateMachine) {
    while state.compaction_pending() {
        state
            .apply_compaction_step(state.applied + 1)
            .await
            .unwrap();
    }
}

async fn rows(state: &StateMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut scan = state.db.scan_prefix(b"etcd/", ..).await.unwrap();
    let mut rows = vec![];
    while let Some(row) = scan.next().await.unwrap() {
        // Snapshots store the in-memory member list. This test covers MVCC rows.
        if row.key.as_ref() != crate::cluster::KEY {
            rows.push((row.key.to_vec(), row.value.to_vec()));
        }
    }
    rows
}

async fn count(state: &StateMachine, prefix: &[u8]) -> usize {
    let mut scan = state.db.scan_prefix(prefix, ..).await.unwrap();
    let mut count = 0;
    while scan.next().await.unwrap().is_some() {
        count += 1;
    }
    count
}

fn put(key: &str, value: &str) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestPut(PutRequest {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
            ..Default::default()
        })),
    }
}

fn delete(key: &str, end: &str) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
            key: key.as_bytes().to_vec(),
            range_end: end.as_bytes().to_vec(),
            ..Default::default()
        })),
    }
}

fn range(revision: i64) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestRange(RangeRequest {
            key: vec![0],
            range_end: vec![0],
            revision,
            ..Default::default()
        })),
    }
}

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

fn request(random: &mut Random) -> RequestOp {
    let key = |random: &mut Random| ["a", "b", "c", "d", "e"][random.next(5) as usize];
    match random.next(10) {
        0..=5 => put(key(random), &random.next(1000).to_string()),
        6 => delete(key(random), ""),
        7 => delete("b", "d"),
        _ => RequestOp {
            request: Some(Op::RequestTxn(TxnRequest {
                success: vec![put("a", "txn"), put("e", "txn"), delete("c", "")],
                ..Default::default()
            })),
        },
    }
}

/// Compare reads and HashKV results with the reference after every change.
async fn compare(state: &StateMachine, reference: &StateMachine) {
    assert_eq!(state.revision, reference.revision);
    assert_eq!(state.compact_revision, reference.compact_revision);
    let pending: Vec<i64> = state
        .compaction
        .iter()
        .flat_map(|progress| progress.targets.iter().map(|target| target.revision))
        .collect();
    for revision in (state.compact_revision - 1).max(0)..=state.revision + 1 {
        // Before its pass ends, etcd also computes a pending boundary hash from current rows.
        if !pending.contains(&revision) {
            assert_eq!(
                serde_json::to_value(state.hash_kv(revision).await).unwrap(),
                serde_json::to_value(reference.hash_kv(revision).await).unwrap(),
                "HashKV revision {revision}"
            );
        }
        assert_eq!(
            serde_json::to_value(state.read_etcd(&range(revision)).await.unwrap()).unwrap(),
            serde_json::to_value(reference.read_etcd(&range(revision)).await.unwrap()).unwrap(),
            "Range revision {revision}"
        );
    }
}

#[test]
fn hash_parts_combine_into_one_sequential_hash() {
    let start = crc32c::crc32c(b"key");
    let first = crc32c::crc32c_append(start, b"older rows");
    let second = crc32c::crc32c_append(0, b"newer rows");
    assert_eq!(
        crc32c::crc32c_combine(first, second, b"newer rows".len()),
        crc32c::crc32c_append(first, b"newer rows")
    );
    assert_eq!(crc32c::crc32c_combine(first, 0, 0), first);
}

#[tokio::test]
async fn stepped_compaction_matches_single_batch_compaction() {
    for seed in 0..4_u64 {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(&dir.path().join("stepped"))
            .await
            .unwrap();
        let mut reference = StateMachine::open(&dir.path().join("reference"))
            .await
            .unwrap();
        let mut random = Random(seed);
        for operation in 0..120 {
            if operation % 23 == 22 {
                let span = (state.revision - state.compact_revision.max(0)).max(1) as u64;
                let request = CompactionRequest {
                    revision: state.compact_revision.max(0) + random.next(span) as i64 + 1,
                    physical: false,
                };
                let expected = reference_compaction(&mut reference, &request).await;
                let outcome = state
                    .apply_compaction(state.applied + 1, &request)
                    .await
                    .unwrap();
                assert_eq!(outcome.result.is_ok(), expected);
                // Leave some passes unfinished across later writes and boundaries.
                for _ in 0..random.next(3) {
                    state
                        .apply_compaction_step(state.applied + 1)
                        .await
                        .unwrap();
                }
            } else {
                let request = request(&mut random);
                let expected = reference
                    .apply_etcd(reference.applied + 1, &request)
                    .await
                    .unwrap();
                let actual = state.apply_etcd(state.applied + 1, &request).await.unwrap();
                assert_eq!(
                    serde_json::to_value(actual).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
            }
            if operation % 7 == 0 || operation % 23 == 22 {
                compare(&state, &reference).await;
            }
        }
        finish(&mut state).await;
        compare(&state, &reference).await;
        for revision in reference.hash_cache().await.unwrap() {
            let revision = revision["hash_revision"].as_i64().unwrap();
            if revision >= state.compact_revision {
                assert_eq!(
                    state.hash_kv(revision).await.unwrap(),
                    reference.hash_kv(revision).await.unwrap()
                );
            }
        }
        assert_eq!(rows(&state).await, rows(&reference).await, "seed {seed}");
        assert_eq!(state.logical_bytes, reference.logical_bytes);
        assert_eq!(
            state.logical_bytes,
            maintenance::count_logical_size(&state.db).await.unwrap()
        );
        state.close().await.unwrap();
        reference.close().await.unwrap();
    }
}

#[tokio::test]
async fn compact_applies_without_deleting_a_large_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    for n in 0..20_000 {
        let request = put(&format!("key-{}", n % 1000), &n.to_string());
        state
            .apply_etcd(state.applied + 1, &request)
            .await
            .unwrap()
            .result
            .unwrap();
    }
    let history = count(&state, mvcc::HISTORY).await;
    let started = std::time::Instant::now();
    state
        .apply_compaction(
            state.applied + 1,
            &CompactionRequest {
                revision: state.revision - 100,
                physical: true,
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
    assert_eq!(count(&state, mvcc::HISTORY).await, history);
    assert!(state.physical_revision() < state.compact_revision);
    let compacted = state.read_etcd(&range(state.revision - 101)).await.unwrap();
    assert_eq!(
        compacted.result.unwrap_err().code,
        tonic::Code::OutOfRange as i32
    );
    let mut steps = 0;
    let mut flush = None;
    while state.compaction_pending() {
        assert_eq!(flush, None);
        flush = state
            .apply_compaction_step(state.applied + 1)
            .await
            .unwrap();
        steps += 1;
    }
    // Several bounded steps share the work.
    assert!(steps > 10, "{steps}");
    assert_eq!(state.physical_revision(), state.compact_revision);
    // The last step asks the node to flush the physical boundary outside ordered apply.
    assert_eq!(flush, Some(state.compact_revision));
    assert!(count(&state, mvcc::HISTORY).await < 1200);
    assert!(count(&state, HASH).await < 1200);
    assert!(count(&state, crate::watch::EVENTS).await <= 101);
    assert!(state.db.get(compaction::PENDING).await.unwrap().is_none());
    assert_eq!(
        state.logical_bytes,
        maintenance::count_logical_size(&state.db).await.unwrap()
    );
    state.close().await.unwrap();
}

#[derive(Clone)]
enum Entry {
    Kv(RequestOp),
    Compact(i64),
    Step,
}

async fn apply(state: &mut StateMachine, entry: &Entry) {
    let index = state.applied + 1;
    match entry {
        Entry::Kv(request) => {
            state.apply_etcd(index, request).await.unwrap();
        }
        Entry::Compact(revision) => {
            state
                .apply_compaction(
                    index,
                    &CompactionRequest {
                        revision: *revision,
                        physical: false,
                    },
                )
                .await
                .unwrap();
        }
        Entry::Step => {
            state.apply_compaction_step(index).await.unwrap();
        }
    }
}

#[tokio::test]
async fn restart_resumes_compaction_from_the_durable_step() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state");
    let mut log = vec![];
    for n in 0..3000 {
        log.push(Entry::Kv(put(&format!("key-{}", n % 300), &n.to_string())));
    }
    log.push(Entry::Compact(2500));
    log.extend(std::iter::repeat_n(Entry::Step, 3));
    log.push(Entry::Kv(delete("key-1", "key-2")));
    log.push(Entry::Compact(2800));
    log.extend(std::iter::repeat_n(Entry::Step, 4));
    let mut reference = StateMachine::open(&dir.path().join("reference"))
        .await
        .unwrap();
    for entry in &log {
        match entry {
            Entry::Kv(request) => {
                reference
                    .apply_etcd(reference.applied + 1, request)
                    .await
                    .unwrap();
            }
            Entry::Compact(revision) => {
                assert!(
                    reference_compaction(
                        &mut reference,
                        &CompactionRequest {
                            revision: *revision,
                            physical: false,
                        },
                    )
                    .await
                );
            }
            Entry::Step => {}
        }
    }
    let mut state = StateMachine::open(&path).await.unwrap();
    for (position, entry) in log.iter().enumerate() {
        apply(&mut state, entry).await;
        if position == 3002 {
            // Make a prefix that contains the first steps durable.
            state.db.flush().await.unwrap();
        }
    }
    assert!(state.compaction_pending());
    // Unflushed steps are lost. Raft replays them from the log after a restart.
    state.close().await.unwrap();
    drop(state);
    let mut state = StateMachine::open(&path).await.unwrap();
    assert!(state.applied >= 3003);
    assert!(state.compaction_pending());
    for entry in &log[state.applied as usize..] {
        apply(&mut state, entry).await;
    }
    finish(&mut state).await;
    for revision in [0, 2799, 2800, 2900] {
        assert_eq!(
            serde_json::to_value(state.hash_kv(revision).await).unwrap(),
            serde_json::to_value(reference.hash_kv(revision).await).unwrap()
        );
    }
    assert_eq!(rows(&state).await, rows(&reference).await);
    assert_eq!(state.logical_bytes, reference.logical_bytes);
    // The node flushes outside ordered apply. After that flush, the finished pass survives.
    state.db.flush().await.unwrap();
    state.close().await.unwrap();
    drop(state);
    let state = StateMachine::open(&path).await.unwrap();
    assert!(!state.compaction_pending());
    assert_eq!(state.physical_revision(), 2800);
    state.close().await.unwrap();
    reference.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_install_carries_a_pending_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = StateMachine::open(&dir.path().join("source"))
        .await
        .unwrap();
    for n in 0..3000 {
        source
            .apply_etcd(source.applied + 1, &put(&format!("k{}", n % 50), "v"))
            .await
            .unwrap();
    }
    apply(&mut source, &Entry::Compact(2900)).await;
    apply(&mut source, &Entry::Step).await;
    assert!(source.compaction_pending());
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = source.applied;
    snapshot.set_data(source.snapshot_data().await.unwrap().into());
    let mut target = StateMachine::open(&dir.path().join("target"))
        .await
        .unwrap();
    target.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(target.compaction, source.compaction);
    finish(&mut source).await;
    finish(&mut target).await;
    assert_eq!(rows(&source).await, rows(&target).await);
    for state in [&source, &target] {
        assert_eq!(
            state.logical_bytes,
            maintenance::count_logical_size(&state.db).await.unwrap()
        );
    }
    source.close().await.unwrap();
    target.close().await.unwrap();
}

#[tokio::test]
async fn boundary_tombstones_select_the_retained_hash() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(&dir.path().join("stepped"))
        .await
        .unwrap();
    let mut reference = StateMachine::open(&dir.path().join("reference"))
        .await
        .unwrap();
    // Only tombstones remain at the second boundary. etcd then skips the older records.
    let log = [
        Entry::Kv(put("a", "1")),
        Entry::Kv(put("b", "2")),
        Entry::Compact(2),
        Entry::Kv(put("a", "3")),
        Entry::Kv(delete("a", "z")),
        Entry::Kv(put("a", "new generation")),
        Entry::Compact(4),
    ];
    for entry in &log {
        apply(&mut state, entry).await;
        finish(&mut state).await;
        match entry {
            Entry::Kv(request) => {
                reference
                    .apply_etcd(reference.applied + 1, request)
                    .await
                    .unwrap();
            }
            Entry::Compact(revision) => {
                let request = CompactionRequest {
                    revision: *revision,
                    physical: false,
                };
                assert!(reference_compaction(&mut reference, &request).await);
            }
            Entry::Step => unreachable!(),
        }
    }
    assert_eq!(
        state.hash_cache().await.unwrap(),
        reference.hash_cache().await.unwrap()
    );
    compare(&state, &reference).await;
    assert_eq!(rows(&state).await, rows(&reference).await);
    state.close().await.unwrap();
    reference.close().await.unwrap();
}
