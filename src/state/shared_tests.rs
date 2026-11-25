//! Compare shared-mode apply with replicated apply, and check bases, claims, and reopen.

use super::*;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::*;
use slatedb::config::{CheckpointOptions, CheckpointScope, ScanOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

fn options() -> shared::Options {
    shared::Options {
        quota: 0,
        voters: vec![1],
        join: false,
        empty_log: false,
        pin_max: Duration::from_secs(300),
        restore: None,
        record: None,
    }
}

async fn open_shared(store: Arc<dyn ObjectStore>) -> StateMachine {
    StateMachine::open_shared(
        store,
        "cluster",
        "test",
        StateCache::default(),
        &options(),
        transfer::Backend::Local,
    )
    .await
    .unwrap()
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

/// The request for step `n` of a fixed workload with overwrites, deletes, and transactions.
fn request(n: u64) -> RequestOp {
    let key = ["a", "b", "c", "d", "e"][(n * 7 % 5) as usize];
    match n % 9 {
        0..=4 => put(key, &format!("value-{n}")),
        5 => delete(key, ""),
        6 => delete("b", "d"),
        _ => RequestOp {
            request: Some(Op::RequestTxn(TxnRequest {
                success: vec![put("a", "txn"), put("e", &n.to_string()), delete("c", "")],
                ..Default::default()
            })),
        },
    }
}

async fn rows(state: &StateMachine, prefix: &[u8]) -> Vec<Row> {
    source::rows(state.applied_state(), prefix).await
}

async fn compare(state: &StateMachine, reference: &StateMachine) {
    assert_eq!(state.revision, reference.revision);
    assert_eq!(state.compact_revision, reference.compact_revision);
    assert_eq!(rows(state, b"etcd/").await, rows(reference, b"etcd/").await);
    for revision in state.compact_revision.max(1)..=state.revision {
        assert_eq!(
            serde_json::to_value(state.hash_kv(revision).await).unwrap(),
            serde_json::to_value(reference.hash_kv(revision).await).unwrap(),
            "HashKV revision {revision}"
        );
        assert_eq!(
            serde_json::to_value(state.read_etcd(&range(revision)).await.unwrap()).unwrap(),
            serde_json::to_value(reference.read_etcd(&range(revision)).await.unwrap()).unwrap(),
            "Range revision {revision}"
        );
    }
}

/// Apply the same workload to `state` and `reference`. Before each compaction step, `inline`
/// drops the background plan of `state`, so the step reads the state inline.
async fn run(state: &mut StateMachine, reference: &mut StateMachine, inline: bool) {
    for n in 0..80_u64 {
        if n % 20 == 19 {
            let request = CompactionRequest {
                revision: state.revision - 3,
                physical: true,
            };
            let expected = reference
                .apply_compaction(reference.applied + 1, &request)
                .await
                .unwrap();
            let actual = state
                .apply_compaction(state.applied + 1, &request)
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(actual).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
            while state.compaction_pending() {
                if inline {
                    state.compaction_plan.clear();
                }
                state
                    .apply_compaction_step(state.applied + 1)
                    .await
                    .unwrap();
                reference
                    .apply_compaction_step(reference.applied + 1)
                    .await
                    .unwrap();
            }
            assert!(!reference.compaction_pending());
        } else {
            let request = request(n);
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
        if n % 10 == 9 {
            compare(state, reference).await;
        }
    }
    compare(state, reference).await;
    assert_eq!(state.logical_bytes, reference.logical_bytes);
}

#[tokio::test]
async fn shared_apply_matches_replicated_apply_from_the_overlay() {
    let dir = tempfile::tempdir().unwrap();
    let mut reference = StateMachine::open(dir.path()).await.unwrap();
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    run(&mut state, &mut reference, false).await;
    // The rows exist only in the overlay. A member has no database of its own.
    assert!(state.database().is_none());
    assert!(state.write_batches > 0);
    // Each batch stores the logical size, so a later base needs no full scan.
    let stored = state.applied_state().get(shared::LOGICAL_BYTES).await;
    assert_eq!(
        stored.unwrap().unwrap().as_ref(),
        state.logical_bytes.total().to_be_bytes()
    );
    assert_eq!(
        stored_counter(&state).await,
        (state.applied, Some(state.logical_bytes))
    );
    assert_eq!(
        state.logical_bytes,
        maintenance::count_logical_usage(state.applied_state())
            .await
            .unwrap()
    );
    state.close().await.unwrap();
    reference.close().await.unwrap();
}

#[tokio::test]
async fn background_and_inline_compaction_steps_read_the_same_overlay_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut planned = open_shared(Arc::new(InMemory::new())).await;
    let mut inline = open_shared(Arc::new(InMemory::new())).await;
    let mut first = StateMachine::open(&dir.path().join("first")).await.unwrap();
    let mut second = StateMachine::open(&dir.path().join("second"))
        .await
        .unwrap();
    run(&mut planned, &mut first, false).await;
    run(&mut inline, &mut second, true).await;
    // The planner read a pinned view of the overlay for the steps.
    assert!(planned.compaction_plan.planned > 0);
    assert_eq!(inline.compaction_plan.planned, 0);
    assert_eq!(
        rows(&planned, b"etcd/").await,
        rows(&inline, b"etcd/").await
    );
    for revision in [0, planned.revision, planned.compact_revision] {
        assert_eq!(
            planned.hash_kv(revision).await.unwrap(),
            inline.hash_kv(revision).await.unwrap()
        );
    }
    for state in [planned, inline, first, second] {
        state.close().await.unwrap();
    }
}

/// The first entry writes no batch, and the bootstrap membership stays in memory. The seed is
/// the first batch, so the first checkpoint holds the membership. Later no-op entries write no
/// batch.
#[tokio::test]
async fn no_op_entries_write_no_batch_after_the_membership_seed() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    state.apply(1, None).await.unwrap();
    let queue = state.shared.as_ref().unwrap().overlay.queue();
    assert_eq!((state.write_batches, queue.pushed()), (0, 0));
    let members = state.applied_state().get(crate::cluster::KEY).await;
    assert!(members.unwrap().is_none());
    state.seed_for_test(2).await.unwrap();
    assert_eq!((state.write_batches, queue.pushed()), (1, 2));
    let members = state.applied_state().get(crate::cluster::KEY).await;
    assert!(members.unwrap().is_some());
    state.apply(3, None).await.unwrap();
    state
        .apply_view_checkpoint(4, &uuid::Uuid::new_v4().to_string(), 0)
        .await
        .unwrap();
    assert_eq!(state.applied, 4);
    assert_eq!((state.write_batches, queue.pushed()), (1, 2));
    // The view at the applied index equals the state of the last batch.
    let view = state.read_view().unwrap();
    assert_eq!(
        view.state().get(APPLIED).await.unwrap().unwrap().as_ref(),
        2_u64.to_be_bytes()
    );
    // The first checkpoint covers the seed batch and holds the membership.
    let mut written = 0;
    let db = writer(&state).await;
    let base = checkpoint(&state, &db, &mut written).await;
    assert_eq!(base.applied, 2);
    assert!(base.get(crate::cluster::KEY).await.unwrap().is_some());
    db.close().await.unwrap();
    state.apply_etcd(5, &put("k", "v")).await.unwrap();
    assert_eq!((state.write_batches, queue.pushed()), (2, 5));
    // The view of index 4 does not see the later write.
    assert_eq!(view.state().get(b"etcd/current/k").await.unwrap(), None);
    state.close().await.unwrap();
}

/// Open the shared writer, as the leader task does.
async fn writer(state: &StateMachine) -> slatedb::Db {
    let source = state.shared.as_ref().unwrap().source.clone();
    source.open_writer().await.unwrap()
}

/// Write the overlay batches into the shared database and flush it.
async fn flush(state: &StateMachine, writer: &slatedb::Db, written: &mut u64) {
    let queue = state.shared.as_ref().unwrap().overlay.queue();
    for (seq, batch) in queue.after(*written, usize::MAX) {
        writer
            .write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
            .await
            .unwrap();
        *written = seq;
    }
    writer.flush().await.unwrap();
}

/// Write the overlay batches into the shared database, flush, and open a checkpoint of it.
async fn checkpoint(
    state: &StateMachine,
    writer: &slatedb::Db,
    written: &mut u64,
) -> Arc<view::Base> {
    flush(state, writer, written).await;
    let id = writer
        .create_checkpoint(
            CheckpointScope::Durable,
            &CheckpointOptions {
                name: Some(shared::view_name(*written)),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .id;
    let source = state.shared.as_ref().unwrap().source.clone();
    source.open_base(id).await.unwrap()
}

#[tokio::test]
async fn bases_switch_monotonically_and_release_covered_segments() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut state = open_shared(store.clone()).await;
    let mut written = 0;
    let writer = writer(&state).await;
    // A reopen needs a checkpoint with the membership.
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    state.seed_for_test(1).await.unwrap();
    for n in 0..9 {
        state
            .apply_etcd(state.applied + 1, &put(&format!("k{n}"), "one"))
            .await
            .unwrap();
    }
    let older = checkpoint(&state, &writer, &mut written).await;
    assert_eq!(older.applied, 10);
    state
        .apply_view_checkpoint(state.applied + 1, &uuid::Uuid::new_v4().to_string(), 0)
        .await
        .unwrap();
    for n in 0..10 {
        state
            .apply_etcd(state.applied + 1, &put(&format!("k{n}"), "two"))
            .await
            .unwrap();
    }
    let newer = checkpoint(&state, &writer, &mut written).await;
    assert_eq!(newer.applied, 21);
    state
        .apply_view_checkpoint(state.applied + 1, &uuid::Uuid::new_v4().to_string(), 0)
        .await
        .unwrap();
    state
        .apply_etcd(state.applied + 1, &delete("k0", ""))
        .await
        .unwrap();
    let before = state.read_view().unwrap();
    let expected = rows(&state, b"etcd/").await;
    let bytes = state.shared.as_ref().unwrap().overlay.bytes();

    // The newer open finishes first. The older one must not lower the base.
    let shared = state.shared.as_ref().unwrap();
    shared.offer(newer.clone());
    shared.offer(older.clone());
    state.switch_base();
    assert_eq!(state.shared.as_ref().unwrap().base.applied, 21);
    state.shared.as_ref().unwrap().offer(older);
    state.switch_base();
    assert_eq!(state.shared.as_ref().unwrap().base.applied, 21);
    // The two closed segments are covered. The switch also closed the segment with the delete,
    // which is above the base, and opened an empty one.
    let overlay = &state.shared.as_ref().unwrap().overlay;
    let segments = overlay.segments();
    assert_eq!(segments.len(), 2);
    assert_eq!((segments[0].last(), segments[1].last()), (23, 0));
    assert!(overlay.bytes() < bytes);
    assert_eq!(rows(&state, b"etcd/").await, expected);
    assert_eq!(
        source::rows(state.read_view().unwrap().state(), b"etcd/").await,
        source::rows(before.state(), b"etcd/").await
    );
    // The view taken before the switch still reads its released segments.
    let old_value = before.state().get(b"etcd/current/k1").await.unwrap();
    assert!(old_value.is_some());
    assert_eq!(
        old_value,
        state.applied_state().get(b"etcd/current/k1").await.unwrap()
    );

    // A reopen starts from the newest checkpoint and rebuilds the same derived state.
    state
        .shared
        .as_ref()
        .unwrap()
        .source
        .mark_bootstrapped(uuid::Uuid::nil())
        .await
        .unwrap();
    let revision = state.revision;
    state.close().await.unwrap();
    let reopened = open_shared(store.clone()).await;
    assert_eq!(reopened.applied, 21);
    assert_eq!(reopened.revision, revision - 1);
    assert_eq!(reopened.shared.as_ref().unwrap().base.applied, 21);
    let k0 = reopened.applied_state().get(b"etcd/current/k0").await;
    assert!(k0.unwrap().is_some());
    let admin = reopened.admin.clone();
    reopened.close().await.unwrap();
    // After the bootstrap marker exists, a member never starts from an empty base.
    for (_, checkpoint) in shared::view_checkpoints(&admin).await.unwrap() {
        admin.delete_checkpoint(checkpoint.id).await.unwrap();
    }
    let error = StateMachine::open_shared(
        store,
        "cluster",
        "test",
        StateCache::default(),
        &options(),
        transfer::Backend::Local,
    )
    .await
    .err()
    .unwrap();
    assert!(error.to_string().contains("no view checkpoint"), "{error}");
}

#[tokio::test]
async fn the_cluster_claim_fixes_the_mode_and_the_quota() {
    let store = InMemory::new();
    assert_eq!(
        shared::claim(&store, "root", "one", 5, &[1]).await.unwrap(),
        5
    );
    // A later member uses the claimed quota.
    assert_eq!(
        shared::claim(&store, "root", "one", 7, &[1]).await.unwrap(),
        5
    );
    assert!(shared::claim(&store, "root", "two", 5, &[1]).await.is_err());
    // A separate one-voter bootstrap in the same namespace refuses before it opens the writer.
    assert!(shared::claim(&store, "root", "one", 5, &[2]).await.is_err());
    shared::claim_mode(&store, "root", "one", StateMode::Shared)
        .await
        .unwrap();
    assert!(
        shared::claim_mode(&store, "root", "one", StateMode::Replicated)
            .await
            .is_err()
    );
}

/// Shared mode keeps the claim quota. A `SetQuota` entry stores only the apply position, so the
/// overlay gets no batch and the next checkpoint holds no quota row.
#[tokio::test]
async fn shared_mode_ignores_a_set_quota_entry() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    state.quota = 500;
    state.set_quota_flag(1 << 20);
    state.apply(1, None).await.unwrap();
    let batches = state.write_batches;
    let request = quota::ClusterQuota {
        bytes: 7,
        requested_at: 1,
        member: 1,
    };
    state.apply_set_quota(2, request, false).await.unwrap();
    assert_eq!(state.applied, 2);
    assert_eq!(state.write_batches, batches);
    assert_eq!((state.quota, state.quota_bytes()), (500, 500));
    assert_eq!(state.cluster_quota(), None);
    assert!(state.quota_mismatch());
    assert!(state
        .applied_state()
        .get(quota::KEY)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn a_restart_writes_a_missing_bootstrap_marker() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut state = open_shared(store.clone()).await;
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    state.seed_for_test(1).await.unwrap();
    // The publisher stopped after its first checkpoint and before the marker.
    let mut written = 0;
    let writer = writer(&state).await;
    checkpoint(&state, &writer, &mut written).await;
    let source = state.shared.as_ref().unwrap().source.clone();
    assert!(!source.bootstrapped().await.unwrap());
    state.close().await.unwrap();
    // A restart with no client write finds the checkpoint and writes the marker.
    let state = open_shared(store).await;
    assert!(state
        .shared
        .as_ref()
        .unwrap()
        .source
        .bootstrapped()
        .await
        .unwrap());
    assert_eq!(state.shared.as_ref().unwrap().base.applied, 1);
    state.close().await.unwrap();
}

#[test]
fn retention_keeps_every_checkpoint_from_the_oldest_pinned_one() {
    let ids: Vec<_> = (0..5).map(|_| uuid::Uuid::new_v4()).collect();
    let views: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(n, id)| (n as u64 * 10, *id))
        .collect();
    let none = std::collections::HashSet::new();
    assert_eq!(shared::retired(&views, &none, &none, 2), ids[..3]);
    let pinned = [ids[1]].into_iter().collect();
    assert_eq!(shared::retired(&views, &pinned, &none, 2), ids[..1]);
    // A pin on a checkpoint that the publisher keeps anyway changes nothing.
    let pinned = [ids[4]].into_iter().collect();
    assert_eq!(shared::retired(&views, &pinned, &none, 2), ids[..3]);
    // A checkpoint that another member reported survives. Older unreported ones do not.
    let reported = [ids[1]].into_iter().collect();
    assert_eq!(
        shared::retired(&views, &none, &reported, 2),
        [ids[0], ids[2]]
    );
}

#[tokio::test]
async fn retention_keeps_the_checkpoint_of_a_live_base() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    let mut written = 0;
    let writer = writer(&state).await;
    let mut bases = vec![];
    for n in 0..5 {
        state
            .apply_etcd(state.applied + 1, &put(&format!("k{n}"), "v"))
            .await
            .unwrap();
        bases.push(checkpoint(&state, &writer, &mut written).await);
    }
    let source = state.shared.as_ref().unwrap().source.clone();
    let count = || async { shared::view_checkpoints(&state.admin).await.unwrap().len() };
    // A slow open still uses the second checkpoint. Retention keeps it and all newer ones.
    let pinned = bases.remove(1);
    drop(bases);
    shared::retain(&state.admin, &source, &Default::default(), 2, None)
        .await
        .unwrap();
    assert_eq!(count().await, 4);
    assert_eq!(
        pinned.get(b"meta/applied").await.unwrap().unwrap().as_ref(),
        2_u64.to_be_bytes()
    );
    drop(pinned);
    shared::retain(&state.admin, &source, &Default::default(), 2, None)
        .await
        .unwrap();
    assert_eq!(count().await, 2);
    state.close().await.unwrap();
}

#[tokio::test]
async fn a_shared_export_keeps_the_durable_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut shared_state = open_shared(Arc::new(InMemory::new())).await;
    let mut replicated = StateMachine::open(dir.path()).await.unwrap();
    for state in [&mut shared_state, &mut replicated] {
        state.apply_etcd(1, &put("key", "value")).await.unwrap();
        assert_eq!(state.durable_applied, 0);
        state.snapshot_data().await.unwrap();
    }
    // The view scan flushed nothing.
    assert_eq!(shared_state.durable_applied, 0);
    // A replicated export flushes the database through the applied index.
    assert_eq!(replicated.durable_applied, 1);
    shared_state.close().await.unwrap();
    replicated.close().await.unwrap();
}

/// A shared-mode backup restores into a new replicated cluster, and the previous format 4 reader
/// accepts it. The restored quota accounting equals a restore of the same replicated data.
#[tokio::test]
async fn a_shared_backup_restores_into_a_replicated_cluster() {
    let dir = tempfile::tempdir().unwrap();
    let mut reference = StateMachine::open(&dir.path().join("reference"))
        .await
        .unwrap();
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    // A small quota raises the space alarm, so the backups also hold an alarm row.
    for machine in [&mut state, &mut reference] {
        machine.quota = 400;
        for n in 0..12_u64 {
            let index = machine.applied + 1;
            machine
                .apply_etcd_quota(index, &request(n), 1)
                .await
                .unwrap();
        }
        assert!(!machine.alarms.is_empty());
    }
    let shared_image = state.snapshot_data().await.unwrap();
    let data: SnapshotData = crate::codec::decode(&shared_image).unwrap();
    assert!(data.rows.iter().all(|(key, _)| !derived_key(key)));
    super::tests::check_format_4(&data);
    let shared_backup = super::tests::backup_bytes(shared_image);
    super::tests::check_format_4_backup(&shared_backup);
    // A Snapshot RPC in shared mode sends the same image of the pinned view. It pads the
    // membership JSON, so `etcdctl snapshot save` accepts the length: 512 * n + the digest.
    let streamed = super::tests::streamed_backup(&state).await;
    assert_eq!(
        streamed.len() % 512,
        32,
        "etcdctl snapshot save checks this size"
    );
    super::tests::check_format_4_backup(&streamed);
    let mut padded: SnapshotData = crate::codec::decode(&streamed[..streamed.len() - 32]).unwrap();
    // The backup ends with the membership in memory. The Raft image sends only a stored row.
    let (key, members) = padded.rows.pop().unwrap();
    assert_eq!(key, crate::cluster::KEY);
    assert_eq!(
        members.trim_ascii_end(),
        serde_json::to_vec(state.members.as_ref()).unwrap()
    );
    let mut stored = data.rows.clone();
    stored.retain(|(key, _)| key != crate::cluster::KEY);
    assert_eq!(padded.rows, stored);
    // A shared image above the limit returns ResourceExhausted, as before streaming backups.
    let error = state.view_backup(1024).await.err().unwrap();
    let error = super::backup::backup_error(error);
    assert_eq!(
        error.code,
        tonic::Code::ResourceExhausted as i32,
        "{error:?}"
    );
    let reference_backup = super::tests::backup_bytes(reference.snapshot_data().await.unwrap());

    let restored = {
        let path = dir.path().join("from-shared");
        std::fs::create_dir(&path).unwrap();
        super::tests::restore(&path, streamed).await
    };
    let expected = {
        let path = dir.path().join("from-replicated");
        std::fs::create_dir(&path).unwrap();
        super::tests::restore(&path, reference_backup).await
    };
    assert_eq!(rows(&restored, b"").await, rows(&expected, b"").await);
    assert_eq!(restored.logical_size(), expected.logical_size());
    assert_eq!(
        restored.logical_usage(),
        maintenance::count_logical_usage(restored.applied_state())
            .await
            .unwrap()
    );
    assert_eq!(restored.alarms, expected.alarms);
    assert_eq!(
        restored
            .applied_state()
            .get(shared::LOGICAL_BYTES)
            .await
            .unwrap(),
        None
    );
    for revision in [0, state.revision] {
        assert_eq!(
            serde_json::to_value(restored.read_etcd(&range(revision)).await.unwrap()).unwrap(),
            serde_json::to_value(state.read_etcd(&range(revision)).await.unwrap()).unwrap()
        );
    }
    for machine in [state, reference, restored, expected] {
        machine.close().await.unwrap();
    }
}

/// An image from an exporter that kept the logical size row still installs and restores.
#[tokio::test]
async fn readers_drop_a_logical_size_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = StateMachine::open(&dir.path().join("source"))
        .await
        .unwrap();
    source.apply_etcd(1, &put("key", "value")).await.unwrap();
    let mut data: SnapshotData =
        crate::codec::decode(&source.snapshot_data().await.unwrap()).unwrap();
    data.rows
        .push((shared::LOGICAL_BYTES.to_vec(), 7_u64.to_be_bytes().to_vec()));
    let image = crate::codec::encode(&data).unwrap();
    let path = dir.path().join("restore");
    std::fs::create_dir(&path).unwrap();
    let restored = super::tests::restore(&path, super::tests::backup_bytes(image)).await;
    assert_eq!(
        restored
            .applied_state()
            .get(shared::LOGICAL_BYTES)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        restored.logical_usage(),
        maintenance::count_logical_usage(restored.applied_state())
            .await
            .unwrap()
    );
    restored.close().await.unwrap();
    source.close().await.unwrap();
}

/// The applied index and the split in the counter row of `state`.
async fn stored_counter(state: &StateMachine) -> (u64, Option<crate::batch::Usage>) {
    use prost::Message;
    let row = state
        .applied_state()
        .get(LOGICAL_SIZE)
        .await
        .unwrap()
        .unwrap();
    let counter = crate::proto::state::LogicalSize::decode(row).unwrap();
    (
        counter.applied,
        stored_usage(&counter, counter.applied).unwrap(),
    )
}

/// A checkpoint from a binary before the usage split holds only the eight-byte total and no
/// counter row. A reopen counts the split, the next batch stores it, and the quota uses it.
#[tokio::test]
async fn a_checkpoint_with_an_eight_byte_total_reopens() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut state = open_shared(store.clone()).await;
    // A reopen needs a checkpoint with the membership.
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    state.seed_for_test(1).await.unwrap();
    for n in 0..10 {
        state
            .apply_etcd(state.applied + 1, &put(&format!("k{n}"), "one"))
            .await
            .unwrap();
    }
    state
        .apply_etcd(state.applied + 1, &put("k0", "two"))
        .await
        .unwrap();
    let size = state.logical_bytes;
    assert!(size.current > 0 && size.history > 0 && size.other > 0);
    // Write the batches the way the older binary did: the total only, and no counter row.
    let writer = writer(&state).await;
    let queue = state.shared.as_ref().unwrap().overlay.queue();
    let mut written = 0;
    for (seq, batch) in queue.after(0, usize::MAX) {
        let mut batch = (*batch).clone();
        if batch.contains(LOGICAL_SIZE) {
            batch.delete(LOGICAL_SIZE);
        }
        writer
            .write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
            .await
            .unwrap();
        written = seq;
    }
    writer.flush().await.unwrap();
    writer
        .create_checkpoint(
            CheckpointScope::Durable,
            &CheckpointOptions {
                name: Some(shared::view_name(written)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let stored = writer.get(shared::LOGICAL_BYTES).await.unwrap().unwrap();
    assert_eq!(stored.as_ref(), size.total().to_be_bytes());
    assert_eq!(writer.get(LOGICAL_SIZE).await.unwrap(), None);
    writer.close().await.unwrap();
    state
        .shared
        .as_ref()
        .unwrap()
        .source
        .mark_bootstrapped(uuid::Uuid::nil())
        .await
        .unwrap();
    state.close().await.unwrap();

    let mut state = open_shared(store).await;
    assert_eq!(state.shared.as_ref().unwrap().base.applied, written);
    assert_eq!(state.logical_bytes, size);
    // The next write stores the split, and the quota checks the updated total.
    state.quota = size.total() + 1;
    let index = state.applied + 1;
    let outcome = state.apply_etcd_quota(index, &put("k1", "two"), 1).await;
    assert!(outcome.unwrap().result.is_ok());
    assert!(state.logical_size() > state.quota);
    let stored = state.applied_state().get(shared::LOGICAL_BYTES).await;
    assert_eq!(
        stored.unwrap().unwrap().as_ref(),
        state.logical_bytes.total().to_be_bytes()
    );
    assert_eq!(
        stored_counter(&state).await,
        (state.applied, Some(state.logical_bytes))
    );
    assert_eq!(
        state.logical_bytes,
        maintenance::count_logical_usage(state.applied_state())
            .await
            .unwrap()
    );
    let index = state.applied + 1;
    let outcome = state.apply_etcd_quota(index, &put("k2", "two"), 1).await;
    let error = outcome.unwrap().result.err().unwrap();
    assert!(
        format!("{error:?}").contains("database space exceeded"),
        "{error:?}"
    );
    assert!(state.alarms.iter().any(|alarm| alarm.alarm == 1));
    state.close().await.unwrap();
}

/// A member behind a checkpoint from a binary before the usage split installs it. The install
/// counts the split from the rows, and the quota uses it on the next write.
#[tokio::test]
async fn a_member_installs_a_checkpoint_with_an_eight_byte_total() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut ahead = open_shared(store.clone()).await;
    let mut behind = open_shared(store).await;
    for state in [&mut ahead, &mut behind] {
        state.entry_term = 4;
    }
    for n in 1..=10 {
        ahead
            .apply_etcd(n, &put(&format!("k{n}"), "one"))
            .await
            .unwrap();
    }
    ahead.apply_etcd(11, &put("k1", "two")).await.unwrap();
    let size = ahead.logical_bytes;
    assert!(size.current > 0 && size.history > 0 && size.other > 0);
    behind.apply_etcd(1, &put("k1", "one")).await.unwrap();
    // Write the batches the way the older binary did: the total only, and no counter row.
    let writer = writer(&ahead).await;
    let queue = ahead.shared.as_ref().unwrap().overlay.queue();
    let mut written = 0;
    for (seq, batch) in queue.after(0, usize::MAX) {
        let mut batch = (*batch).clone();
        if batch.contains(LOGICAL_SIZE) {
            batch.delete(LOGICAL_SIZE);
        }
        writer
            .write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
            .await
            .unwrap();
        written = seq;
    }
    writer.flush().await.unwrap();
    let id = writer
        .create_checkpoint(
            CheckpointScope::Durable,
            &CheckpointOptions {
                name: Some(shared::view_name(written)),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .id;
    let stored = writer.get(shared::LOGICAL_BYTES).await.unwrap().unwrap();
    assert_eq!(stored.as_ref(), size.total().to_be_bytes());
    assert_eq!(writer.get(LOGICAL_SIZE).await.unwrap(), None);
    let source = behind.shared.as_ref().unwrap().source.clone();
    let base = source.open_base(id).await.unwrap();
    assert_eq!(base.applied, 11);

    behind.adopt_opened(base, 4).await.unwrap();
    assert_eq!(behind.applied, 11);
    assert_eq!(behind.logical_bytes, size);
    // The next write stores the split, and the quota checks the updated total.
    behind.quota = size.total() + 1;
    let outcome = behind.apply_etcd_quota(12, &put("k2", "two"), 1).await;
    assert!(outcome.unwrap().result.is_ok());
    assert!(behind.logical_size() > behind.quota);
    assert_eq!(
        stored_counter(&behind).await,
        (behind.applied, Some(behind.logical_bytes))
    );
    assert_eq!(
        behind.logical_bytes,
        maintenance::count_logical_usage(behind.applied_state())
            .await
            .unwrap()
    );
    let outcome = behind.apply_etcd_quota(13, &put("k3", "two"), 1).await;
    let error = outcome.unwrap().result.err().unwrap();
    assert!(
        format!("{error:?}").contains("database space exceeded"),
        "{error:?}"
    );
    writer.close().await.unwrap();
    for state in [ahead, behind] {
        state.close().await.unwrap();
    }
}

/// Five checkpoints with no live base, and their `(seq, id)` list.
async fn five_checkpoints(state: &mut StateMachine) -> Vec<(u64, uuid::Uuid)> {
    let mut written = 0;
    let writer = writer(state).await;
    for n in 0..5 {
        state
            .apply_etcd(state.applied + 1, &put(&format!("k{n}"), "v"))
            .await
            .unwrap();
        drop(checkpoint(state, &writer, &mut written).await);
    }
    shared::view_checkpoints(&state.admin)
        .await
        .unwrap()
        .into_iter()
        .map(|(seq, checkpoint)| (seq, checkpoint.id))
        .collect()
}

/// An open that starts after retention selected its checkpoint fails. The member never holds a
/// live base whose checkpoint retention deletes.
#[tokio::test]
async fn an_open_after_the_retention_choice_fails() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    let views = five_checkpoints(&mut state).await;
    let source = state.shared.as_ref().unwrap().source.clone();
    let oldest = views[0].1;
    let candidates = source.begin_retire(&views, &Default::default(), 2);
    assert_eq!(candidates, [views[0].1, views[1].1, views[2].1]);
    // The open runs between the choice and the deletion.
    let error = source.open_base(oldest).await.err().unwrap();
    assert!(error.to_string().contains("being deleted"), "{error}");
    assert!(!source.pinned().contains(&oldest));
    for id in &candidates {
        state.admin.delete_checkpoint(*id).await.unwrap();
    }
    source.end_retire(&candidates);
    assert_eq!(
        shared::view_checkpoints(&state.admin).await.unwrap().len(),
        2
    );
    state.close().await.unwrap();
}

/// An open in flight before retention collects the pins keeps its checkpoint.
#[tokio::test]
async fn an_open_in_flight_keeps_its_checkpoint() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    let views = five_checkpoints(&mut state).await;
    let source = state.shared.as_ref().unwrap().source.clone();
    // The open reserved its checkpoint and waits for storage.
    let opening = source.reserve_open(views[0].1).unwrap();
    shared::retain(&state.admin, &source, &Default::default(), 2, None)
        .await
        .unwrap();
    assert_eq!(
        shared::view_checkpoints(&state.admin).await.unwrap().len(),
        5
    );
    drop(opening);
    shared::retain(&state.admin, &source, &Default::default(), 2, None)
        .await
        .unwrap();
    assert_eq!(
        shared::view_checkpoints(&state.admin).await.unwrap().len(),
        2
    );
    state.close().await.unwrap();
}

/// A read on a view that is older than the pin limit fails with a retryable error.
#[tokio::test]
async fn an_old_view_fails_reads_with_a_retryable_error() {
    let mut state = StateMachine::open_shared(
        Arc::new(InMemory::new()),
        "cluster",
        "test",
        StateCache::default(),
        &shared::Options {
            pin_max: Duration::from_millis(50),
            ..options()
        },
        transfer::Backend::Local,
    )
    .await
    .unwrap();
    state.apply_etcd(1, &put("key", "value")).await.unwrap();
    let get = RequestOp {
        request: Some(Op::RequestRange(RangeRequest {
            key: b"key".to_vec(),
            ..Default::default()
        })),
    };
    let view = state.read_view().unwrap();
    assert!(view.read(&get, None).await.unwrap().result.is_ok());
    // An idle view stays usable. Each read starts its own deadline.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(view.read(&get, None).await.unwrap().result.is_ok());
    // One read that holds its pin past the limit fails at its next get or scan step.
    let held = view.read_state();
    let mut scan = held
        .scan_under(b"etcd/", &ScanOptions::default())
        .await
        .unwrap();
    assert!(scan.next().await.unwrap().is_some());
    tokio::time::sleep(Duration::from_millis(80)).await;
    let error = scan.next().await.err().unwrap();
    assert!(view::expired(&error.to_string()), "{error}");
    let error = held.get(b"etcd/current/key").await.unwrap_err();
    assert!(view::expired(&error.to_string()), "{error}");
    assert!(view.read(&get, None).await.unwrap().result.is_ok());
    state.close().await.unwrap();
}

/// A format 6 snapshot at the applied index must have the applied term.
#[tokio::test]
async fn an_equal_index_snapshot_with_another_term_fails() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    state.entry_term = 2;
    state.apply_etcd(1, &put("key", "value")).await.unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 1;
    snapshot.mut_metadata().term = 3;
    snapshot.data = transfer::ViewDescriptor::new(uuid::Uuid::new_v4())
        .encode()
        .unwrap();
    let error = state.install_snapshot(&snapshot).await.unwrap_err();
    assert!(error.to_string().contains("differs"), "{error}");
    snapshot.mut_metadata().term = 2;
    state.install_snapshot(&snapshot).await.unwrap();
    state.close().await.unwrap();
}

/// A received snapshot that applies nothing and is not above the base releases its checkpoint.
/// One above the base raises the base there, because the checkpoint holds that state.
#[tokio::test]
async fn a_skipped_snapshot_releases_its_receipt() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    state.apply_etcd(1, &put("a", "1")).await.unwrap();
    let mut written = 0;
    let writer = writer(&state).await;
    let first = checkpoint(&state, &writer, &mut written).await;
    let x = first.checkpoint.unwrap();
    let second = writer
        .create_checkpoint(
            CheckpointScope::Durable,
            &CheckpointOptions {
                name: Some(shared::view_name(1)),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .id;
    state.apply_etcd(2, &put("b", "2")).await.unwrap();
    let source = state.shared.as_ref().unwrap().source.clone();
    let install = |id: uuid::Uuid| {
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 1;
        snapshot.data = transfer::ViewDescriptor::new(id).encode().unwrap();
        snapshot
    };
    // Above the base at 0: the base moves to X at 1.
    source.receive(x).await.unwrap();
    state.install_snapshot(&install(x)).await.unwrap();
    assert_eq!(state.applied, 2);
    state.switch_base();
    assert_eq!(state.base_checkpoint(), Some(x));
    assert_eq!(state.view_status().unwrap().0, 1);
    // At the base index: nothing changes, and the receipt goes.
    source.receive(second).await.unwrap();
    assert!(source.pinned().contains(&second));
    state.install_snapshot(&install(second)).await.unwrap();
    state.switch_base();
    assert_eq!(state.base_checkpoint(), Some(x));
    assert!(!source.pinned().contains(&second));
    drop(first);
    state.close().await.unwrap();
}

/// A shared state whose base holds 64 rows of 1 KiB, behind a store that can delay SST reads.
/// Reads hold their view for at most 20 ms.
async fn slow_base() -> (StateMachine, Arc<crate::object::faults::FaultStore>) {
    use crate::object::faults::FaultStore;
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = faults.clone();
    let mut state = StateMachine::open_shared(
        store,
        "cluster",
        "test",
        // A tiny cache, so the read goes to the store.
        StateCache {
            block_bytes: 1,
            meta_bytes: 1,
            row_bytes: 0,
        },
        &shared::Options {
            pin_max: Duration::from_millis(20),
            ..options()
        },
        transfer::Backend::Local,
    )
    .await
    .unwrap();
    for n in 0..64 {
        state
            .apply_etcd(n + 1, &put(&format!("k{n}"), &"v".repeat(1024)))
            .await
            .unwrap();
    }
    let writer = writer(&state).await;
    let mut written = 0;
    let base = checkpoint(&state, &writer, &mut written).await;
    state.shared.as_ref().unwrap().offer(base);
    state.switch_base();
    assert_eq!(state.shared.as_ref().unwrap().base.applied, 64);
    (state, faults)
}

/// Assert that a read failed at its deadline with a retryable error, long before a slow
/// object read would end.
fn assert_expired(result: Result<(), slatedb::Error>, started: std::time::Instant) {
    let error = result.unwrap_err();
    assert!(view::expired(&error.to_string()), "{error}");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
}

/// A slow base read ends at the read's deadline with a retryable error.
#[tokio::test]
async fn the_deadline_bounds_a_slow_base_read() {
    let (state, faults) = slow_base().await;
    let view = state.read_view().unwrap();
    // Each SST read takes 1 s, far above the 20 ms pin limit. A loaded host still ends the
    // read well below one SST read.
    faults.set_delay(Duration::from_secs(1));
    faults.set(crate::object::faults::DELAY_SST_GET);
    // A flush caches current rows, so the read scans history rows, which only the store has.
    let held = view.read_state();
    let started = std::time::Instant::now();
    let scanned = async {
        let mut scan = held
            .scan_under(b"etcd/history/", &ScanOptions::default())
            .await?;
        while scan.next().await?.is_some() {}
        Ok::<_, slatedb::Error>(())
    };
    assert_expired(scanned.await, started);
    faults.set(0);
    state.close().await.unwrap();
}

/// A slow base read during a seek ends at the read's deadline with a retryable error.
#[tokio::test]
async fn the_deadline_bounds_a_slow_base_seek() {
    let (state, faults) = slow_base().await;
    let view = state.read_view().unwrap();
    let held = view.read_state();
    let mut scan = held
        .scan_under(b"etcd/history/", &ScanOptions::default())
        .await
        .unwrap();
    faults.set_delay(Duration::from_secs(1));
    faults.set(crate::object::faults::DELAY_SST_GET);
    let started = std::time::Instant::now();
    assert_expired(
        scan.seek(&crate::mvcc::history_key(b"k9", 0)).await,
        started,
    );
    faults.set(0);
    state.close().await.unwrap();
}

/// Range the single key `key` at `revision` through a pinned view and the live state, and
/// return the value.
async fn value_at(state: &StateMachine, key: &str, revision: i64) -> Option<String> {
    let request = RequestOp {
        request: Some(Op::RequestRange(RangeRequest {
            key: key.as_bytes().to_vec(),
            revision,
            ..Default::default()
        })),
    };
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
    let Ok(ResponseOp {
        response: Some(response_op::Response::ResponseRange(reply)),
    }) = live.result
    else {
        panic!("unexpected response {:?}", live.result);
    };
    reply
        .kvs
        .first()
        .map(|kv| String::from_utf8(kv.value.clone()).unwrap())
}

/// A historical point read returns the newest version at its revision, from the overlay alone
/// and from history split between a checkpoint and the overlay.
#[tokio::test]
async fn historical_point_reads_return_the_version_at_the_revision() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    let writer = writer(&state).await;
    let mut written = 0;
    // Revisions 2, 3, and 4, all in the overlay.
    for value in ["old", "middle", "new"] {
        state
            .apply_etcd(state.applied + 1, &put("a", value))
            .await
            .unwrap();
    }
    let expected = [
        (1, None),
        (2, Some("old")),
        (3, Some("middle")),
        (4, Some("new")),
    ];
    for (revision, value) in expected {
        assert_eq!(
            value_at(&state, "a", revision).await.as_deref(),
            value,
            "overlay, revision {revision}"
        );
    }
    // Revisions 5 and 6 of `b` go into a checkpoint, and 7 and 8 stay in the overlay.
    for value in ["b1", "b2"] {
        state
            .apply_etcd(state.applied + 1, &put("b", value))
            .await
            .unwrap();
    }
    let base = checkpoint(&state, &writer, &mut written).await;
    state.shared.as_ref().unwrap().offer(base);
    state.switch_base();
    assert_eq!(state.shared.as_ref().unwrap().base.applied, 5);
    for value in ["b3", "b4"] {
        state
            .apply_etcd(state.applied + 1, &put("b", value))
            .await
            .unwrap();
    }
    let expected = [
        (4, None),
        (5, Some("b1")),
        (6, Some("b2")),
        (7, Some("b3")),
        (8, Some("b4")),
    ];
    for (revision, value) in expected {
        assert_eq!(
            value_at(&state, "b", revision).await.as_deref(),
            value,
            "checkpoint and overlay, revision {revision}"
        );
    }
    assert_eq!(value_at(&state, "a", 3).await.as_deref(), Some("middle"));
    state.close().await.unwrap();
}

async fn announce(state: &mut StateMachine, base: &view::Base) {
    let id = base.checkpoint.unwrap().to_string();
    let index = state.applied + 1;
    state.apply_view_checkpoint(index, &id, 0).await.unwrap();
}

/// A deposed leader's `ViewCheckpoint` commits after a newer one. The base does not go back.
#[tokio::test]
async fn a_late_older_announcement_keeps_the_base() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    let writer = writer(&state).await;
    let mut written = 0;
    state.apply_etcd(1, &put("a", "1")).await.unwrap();
    let older = checkpoint(&state, &writer, &mut written).await;
    state.apply_etcd(2, &put("b", "2")).await.unwrap();
    let newer = checkpoint(&state, &writer, &mut written).await;
    let opened = state.shared_handles().unwrap().opened;
    announce(&mut state, &newer).await;
    for _ in 0..100 {
        state.switch_base();
        if state.shared.as_ref().unwrap().base.applied == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(state.shared.as_ref().unwrap().base.applied, 2);
    announce(&mut state, &older).await;
    for _ in 0..100 {
        if opened.newest() == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(opened.newest(), Some(1));
    state.switch_base();
    assert_eq!(state.shared.as_ref().unwrap().base.applied, 2);
    state.close().await.unwrap();
}

/// A member behind a checkpoint adopts it in process when its log commits the checkpoint index.
#[tokio::test]
async fn a_member_behind_a_checkpoint_adopts_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut ahead = open_shared(store.clone()).await;
    let mut behind = open_shared(store).await;
    for state in [&mut ahead, &mut behind] {
        state.entry_term = 4;
    }
    let writer = writer(&ahead).await;
    let mut written = 0;
    for n in 1..=4 {
        ahead
            .apply_etcd(n, &put(&format!("k{n}"), "v"))
            .await
            .unwrap();
    }
    let at_four = checkpoint(&ahead, &writer, &mut written).await;
    let rows_at_four = rows(&ahead, b"etcd/").await;
    for n in 5..=6 {
        ahead
            .apply_etcd(n, &put(&format!("k{n}"), "v"))
            .await
            .unwrap();
    }
    let at_six = checkpoint(&ahead, &writer, &mut written).await;
    for n in 1..=2 {
        behind
            .apply_etcd(n, &put(&format!("k{n}"), "v"))
            .await
            .unwrap();
    }
    let opened = behind.shared_handles().unwrap().opened;
    opened.offer(at_six.clone(), 0);
    // A switch cannot use a base above the applied index.
    behind.switch_base();
    assert_eq!(behind.applied, 2);
    // The node verified checkpoint 4. A newer open in the slot does not replace it.
    behind.adopt_opened(at_four, 4).await.unwrap();
    assert_eq!(behind.applied, 4);
    assert_eq!(rows(&behind, b"etcd/").await, rows_at_four);
    assert_eq!(opened.newest(), Some(6));
    behind
        .adopt_opened(opened.take().unwrap().0, 4)
        .await
        .unwrap();
    assert_eq!(behind.applied, 6);
    assert_eq!(behind.revision, ahead.revision);
    assert_eq!(rows(&behind, b"etcd/").await, rows(&ahead, b"etcd/").await);
    // A base with another term at its index fails.
    let mut wrong = open_shared(Arc::new(InMemory::new())).await;
    wrong.apply_etcd(1, &put("x", "y")).await.unwrap();
    assert!(wrong.adopt_opened(at_six, 5).await.is_err());
    for state in [ahead, behind, wrong] {
        state.close().await.unwrap();
    }
}

/// A deposed writer cannot delete checkpoints. Retention reads the writer epoch before each
/// deletion and stops when a newer writer holds the shared state.
#[tokio::test]
async fn a_fenced_writer_deletes_no_checkpoint() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    let views = five_checkpoints(&mut state).await;
    let source = state.shared.as_ref().unwrap().source.clone();
    let old = writer(&state).await;
    let old_epoch = old.manifest().writer_epoch();
    // A successor opens the writer. Its members pin the oldest checkpoint.
    let successor = writer(&state).await;
    let new_epoch = successor.manifest().writer_epoch();
    assert!(new_epoch > old_epoch);
    let pinned = std::collections::HashSet::from([views[0].1]);
    // The deposed publisher resumes its retention. It deletes nothing.
    shared::retain(
        &state.admin,
        &source,
        &Default::default(),
        2,
        Some(old_epoch),
    )
    .await
    .unwrap();
    assert_eq!(
        shared::view_checkpoints(&state.admin).await.unwrap().len(),
        5
    );
    // The successor keeps the reported pin and deletes the others.
    shared::retain(&state.admin, &source, &pinned, 2, Some(new_epoch))
        .await
        .unwrap();
    let left: Vec<_> = shared::view_checkpoints(&state.admin)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, checkpoint)| checkpoint.id)
        .collect();
    assert_eq!(left, [views[0].1, views[3].1, views[4].1]);
    state.close().await.unwrap();
}

fn restore(id: &str) -> shared::Restore {
    let members = crate::cluster::Store::initial(&[(1, "127.0.0.1:1".to_owned())].into());
    shared::Restore::new(
        vec![
            (APPLIED.to_vec(), 1_u64.to_be_bytes().to_vec()),
            (
                crate::cluster::KEY.to_vec(),
                serde_json::to_vec(&members).unwrap(),
            ),
            (b"etcd/restored".to_vec(), id.as_bytes().to_vec()),
        ],
        id.to_owned(),
    )
}

fn restore_source(store: Arc<dyn ObjectStore>) -> (Arc<shared::Source>, slatedb::admin::Admin) {
    let source = shared::offline_source(store.clone(), "cluster", StateCache::default()).unwrap();
    let admin = slatedb::admin::Admin::builder(source.state().as_str(), store).build();
    (source, admin)
}

/// The bootstrap claim names the snapshot. The same snapshot restores again with success. Another
/// snapshot fails, and so does a shared state that an older ordinary start created.
#[tokio::test]
async fn a_restore_claim_names_its_snapshot() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (source, admin) = restore_source(store.clone());
    shared::seed(&source, &admin, &restore("a")).await.unwrap();
    shared::seed(&source, &admin, &restore("a")).await.unwrap();
    let error = shared::seed(&source, &admin, &restore("b"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("new cluster prefix"),
        "{error:#}"
    );
    let (id, _) = shared::view_checkpoints(&admin)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(id, 1);
    // A normal first leader created this shared state. It has no restore claim.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (source, admin) = restore_source(store);
    let db = source.open_writer().await.unwrap();
    db.put(b"etcd/other", b"value").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();
    let error = shared::seed(&source, &admin, &restore("a"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("new cluster prefix"),
        "{error:#}"
    );
}

/// A restore stopped after its rows reached the shared database and before its checkpoint and
/// marker. The next restore of the same snapshot completes the checkpoint and the marker.
#[tokio::test]
async fn a_restore_completes_after_a_failure_before_its_marker() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (source, admin) = restore_source(store);
    let partial = restore("a");
    shared::seed_rows_for_test(&source, &partial).await.unwrap();
    assert!(!source.bootstrapped().await.unwrap());
    assert!(admin.read_manifest(None).await.unwrap().is_some());
    shared::seed(&source, &admin, &partial).await.unwrap();
    assert!(source.bootstrapped().await.unwrap());
    let (seq, checkpoint) = shared::view_checkpoints(&admin)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(seq, 1);
    let base = source.open_base(checkpoint.id).await.unwrap();
    assert_eq!(base.applied, 1);
    assert_eq!(
        base.get(b"etcd/restored").await.unwrap().as_deref(),
        Some(b"a".as_slice())
    );
    // Another snapshot cannot take over the state of this one.
    let error = shared::seed(&source, &admin, &restore("b"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("new cluster prefix"),
        "{error:#}"
    );
}

/// Defragment from sorted runs only. SlateDB can reuse the destination run ID, so the check
/// compares SST views: a checkpoint after `compact_all` has one run and none of the input views.
#[tokio::test]
async fn defragment_replaces_every_input_sorted_run() {
    use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut settings = slatedb_settings();
    if let Some(compactor) = settings.compactor_options.as_mut() {
        compactor
            .scheduler_options
            .insert("min_compaction_sources".into(), "1000000".into());
    }
    let db = slatedb::Db::builder("db", store.clone())
        .with_settings(settings)
        .build()
        .await
        .unwrap();
    let admin = slatedb::admin::Admin::builder("db", store).build();
    // Two sorted runs and no L0 SST.
    for (n, destination) in [(0_u32, 0_u32), (1, 1)] {
        db.put(format!("k{n}").as_bytes(), b"v").await.unwrap();
        db.flush().await.unwrap();
        let manifest = admin.read_manifest(None).await.unwrap().unwrap();
        let sources = manifest
            .l0()
            .iter()
            .map(|view| SourceId::SstView(view.id))
            .collect();
        let job = admin
            .submit_compaction(CompactionSpec::new(sources, destination))
            .await
            .unwrap();
        for _ in 0..500 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let done = matches!(
                admin
                    .read_compaction(job.id(), None)
                    .await
                    .unwrap()
                    .map(|j| j.status()),
                Some(CompactionStatus::Completed)
            );
            if done
                && admin
                    .read_manifest(None)
                    .await
                    .unwrap()
                    .unwrap()
                    .l0()
                    .is_empty()
            {
                break;
            }
        }
    }
    let manifest = admin.read_manifest(None).await.unwrap().unwrap();
    assert!(manifest.l0().is_empty());
    assert_eq!(manifest.compacted().len(), 2);
    let input: std::collections::HashSet<_> =
        manifest.all_sst_views().map(|view| view.id).collect();
    maintenance::compact_all(&admin).await.unwrap();
    let checkpoint = db
        .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
        .await
        .unwrap();
    let manifest = admin
        .read_manifest(Some(checkpoint.manifest_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.compacted().len(), 1);
    assert!(!manifest
        .all_sst_views()
        .any(|view| input.contains(&view.id)));
    db.close().await.unwrap();
}

/// A writer that caches nothing at a flush, so the member cache holds no block of a new SST.
async fn cold_writer(state: &StateMachine) -> slatedb::Db {
    let source = state.shared.as_ref().unwrap().source.clone();
    slatedb::Db::builder(source.state(), source.store.clone())
        .with_db_cache(source.cache.clone(), 0)
        .with_settings(slatedb_settings())
        .with_block_cache_policy(slatedb::BlockCachePolicy::default().with_flush_targets(&[]))
        .build()
        .await
        .unwrap()
}

/// Write the overlay into `writer`, flush, and create a view checkpoint. A read of the apply
/// marker caches what a base open reads, but no current-row block.
async fn cold_checkpoint(
    state: &StateMachine,
    writer: &slatedb::Db,
    written: &mut u64,
) -> uuid::Uuid {
    cold_flush(state, writer, written).await;
    writer
        .create_checkpoint(
            CheckpointScope::Durable,
            &CheckpointOptions {
                name: Some(shared::view_name(*written)),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .id
}

/// Write the overlay into `writer` and flush it to a new L0 SST. A read of the apply marker
/// caches what a base open reads, but no current-row block.
async fn cold_flush(state: &StateMachine, writer: &slatedb::Db, written: &mut u64) {
    let queue = state.shared.as_ref().unwrap().overlay.queue();
    for (seq, batch) in queue.after(*written, usize::MAX) {
        writer
            .write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
            .await
            .unwrap();
        *written = seq;
    }
    writer.flush().await.unwrap();
    writer.get(APPLIED).await.unwrap();
}

/// Put `count` keys from `first` with 200 B values, so the rows fill many blocks.
async fn put_rows(state: &mut StateMachine, first: usize, count: usize) {
    for n in first..first + count {
        state
            .apply_etcd(
                state.applied + 1,
                &put(&format!("k{n:04}"), &"v".repeat(200)),
            )
            .await
            .unwrap();
    }
}

fn current(key: &str) -> Vec<u8> {
    [crate::mvcc::CURRENT, key.as_bytes()].concat()
}

/// Apply the announcement of `id` and switch to its base once the background open offers it.
async fn adopt(state: &mut StateMachine, id: uuid::Uuid) {
    state
        .apply_view_checkpoint(state.applied + 1, &id.to_string(), 0)
        .await
        .unwrap();
    state.settle_opens().await;
    state.switch_base();
    assert_eq!(state.base_checkpoint(), Some(id));
}

async fn fault_state() -> (Arc<crate::object::faults::FaultStore>, StateMachine) {
    let faults = Arc::new(crate::object::faults::FaultStore::new(Arc::new(
        InMemory::new(),
    )));
    let state = open_shared(faults.clone()).await;
    (faults, state)
}

/// A publication, a snapshot receipt, and a restart open a base without a cache warm. Slow SST
/// reads do not delay them.
#[tokio::test]
async fn slow_sst_reads_do_not_delay_metadata_opens() {
    use crate::object::faults::DELAY_SST_GET;
    let (faults, mut state) = fault_state().await;
    put_rows(&mut state, 0, 300).await;
    let writer = cold_writer(&state).await;
    let id = cold_checkpoint(&state, &writer, &mut 0).await;
    let source = state.shared.as_ref().unwrap().source.clone();
    faults.set_delay(Duration::from_secs(3));
    faults.set(DELAY_SST_GET);
    let limit = Duration::from_secs(1);
    let base = tokio::time::timeout(limit, source.open_base(id))
        .await
        .expect("an open waited for SST reads")
        .unwrap();
    tokio::time::timeout(limit, source.receive(id))
        .await
        .expect("a receipt waited for SST reads")
        .unwrap();
    assert!(source.pinned().contains(&id));
    assert!(source.warmed().0.is_empty());
    drop(base);
    faults.set(0);
    writer.close().await.unwrap();
    state.close().await.unwrap();
}

/// A follower adoption warms for at most `WARM_MAX`, then switches to the base.
#[tokio::test]
async fn an_adoption_switches_within_the_warm_limit() {
    use crate::object::faults::DELAY_SST_GET;
    let (faults, mut state) = fault_state().await;
    put_rows(&mut state, 0, 300).await;
    let writer = cold_writer(&state).await;
    let id = cold_checkpoint(&state, &writer, &mut 0).await;
    faults.set_delay(Duration::from_secs(3));
    faults.set(DELAY_SST_GET);
    let started = std::time::Instant::now();
    state
        .apply_view_checkpoint(state.applied + 1, &id.to_string(), 0)
        .await
        .unwrap();
    while state.base_checkpoint() != Some(id) && started.elapsed() < Duration::from_secs(3) {
        tokio::time::sleep(Duration::from_millis(10)).await;
        state.switch_base();
    }
    assert_eq!(state.base_checkpoint(), Some(id));
    let elapsed = started.elapsed();
    assert!(elapsed >= shared::WARM_MAX, "{elapsed:?}");
    assert!(elapsed < shared::WARM_MAX * 3, "{elapsed:?}");
    faults.set(0);
    writer.close().await.unwrap();
    state.close().await.unwrap();
}

/// An SST whose warm failed stays cold in the warm set, and the next adoption loads it.
#[tokio::test]
async fn a_failed_warm_is_retried_on_a_later_base() {
    use crate::object::faults::FAIL_SST_GET;
    let (faults, mut state) = fault_state().await;
    put_rows(&mut state, 0, 300).await;
    let writer = cold_writer(&state).await;
    let mut written = 0;
    let first = cold_checkpoint(&state, &writer, &mut written).await;
    faults.set(FAIL_SST_GET);
    adopt(&mut state, first).await;
    faults.set(0);
    let source = state.shared.as_ref().unwrap().source.clone();
    assert!(source.warmed().0.is_empty());
    put_rows(&mut state, 300, 10).await;
    let second = cold_checkpoint(&state, &writer, &mut written).await;
    adopt(&mut state, second).await;
    faults.set(FAIL_SST_GET);
    let base = state.shared.as_ref().unwrap().base.clone();
    assert!(base.get(&current("k0001")).await.unwrap().is_some());
    faults.set(0);
    writer.close().await.unwrap();
    state.close().await.unwrap();
}

/// Two warms at the same time both add their SSTs.
#[tokio::test]
async fn concurrent_warms_keep_both_results() {
    let (_faults, mut state) = fault_state().await;
    put_rows(&mut state, 0, 50).await;
    let writer = cold_writer(&state).await;
    let mut written = 0;
    let first = cold_checkpoint(&state, &writer, &mut written).await;
    put_rows(&mut state, 50, 50).await;
    let second = cold_checkpoint(&state, &writer, &mut written).await;
    let source = state.shared.as_ref().unwrap().source.clone();
    let (a, b) = (
        source.open_base(first).await.unwrap(),
        source.open_base(second).await.unwrap(),
    );
    tokio::join!(source.warm(&b), source.warm(&a));
    let all: std::collections::HashSet<_> =
        a.ssts().iter().chain(b.ssts()).map(|(id, _)| *id).collect();
    assert_eq!(all.len(), 2);
    assert_eq!(source.warmed().0, all);
    writer.close().await.unwrap();
    state.close().await.unwrap();
}

/// The warm set keeps only SSTs of live bases, so it does not grow with each compaction.
#[tokio::test]
async fn the_warm_set_stays_bounded_across_base_switches() {
    let (_faults, mut state) = fault_state().await;
    let writer = cold_writer(&state).await;
    let source = state.shared.as_ref().unwrap().source.clone();
    let admin = slatedb::admin::Admin::builder(source.state(), source.store.clone()).build();
    let mut written = 0;
    let mut seen = std::collections::HashSet::new();
    for round in 0..6 {
        put_rows(&mut state, round * 20, 20).await;
        let id = cold_checkpoint(&state, &writer, &mut written).await;
        adopt(&mut state, id).await;
        let base = state.shared.as_ref().unwrap().base.clone();
        let current: Vec<_> = base.ssts().iter().map(|(id, _)| *id).collect();
        // Only SSTs of live bases stay. Another task, such as a read, can still hold an older
        // base, so the bound is the set of live bases, not the last two.
        let (warmed, live) = source.warmed();
        assert!(
            warmed.is_subset(&live),
            "round {round}: {} warm SSTs, {} retired",
            warmed.len(),
            warmed.difference(&live).count()
        );
        seen.extend(current.iter().copied());
        crate::state::maintenance::compact_all(&admin)
            .await
            .unwrap();
    }
    // Each round adds an L0 SST and new compacted SSTs. Without the pruning, all would stay.
    let warmed = source.warmed().0.len();
    assert!(seen.len() >= 12, "{warmed} of {}", seen.len());
    writer.close().await.unwrap();
    state.close().await.unwrap();
}

/// Adoptions on a slow store each warm some SSTs and then reach the time limit. The warm set
/// still keeps only SSTs of live bases across compactions.
#[tokio::test]
async fn timed_out_warms_keep_the_warm_set_bounded() {
    use crate::object::faults::DELAY_SST_GET;
    let (faults, mut state) = fault_state().await;
    let writer = cold_writer(&state).await;
    let source = state.shared.as_ref().unwrap().source.clone();
    let admin = slatedb::admin::Admin::builder(source.state(), source.store.clone()).build();
    faults.set_delay(Duration::from_millis(150));
    let mut written = 0;
    let mut partial = 0;
    for round in 0..6 {
        // Four cold L0 SSTs. Each data read takes 150 ms, so a warm loads about three of them.
        for flush in 0..4 {
            put_rows(&mut state, (round * 4 + flush) * 20, 20).await;
            cold_flush(&state, &writer, &mut written).await;
        }
        let id = cold_checkpoint(&state, &writer, &mut written).await;
        faults.set(DELAY_SST_GET);
        adopt(&mut state, id).await;
        faults.set(0);
        let base = state.shared.as_ref().unwrap().base.clone();
        let current: Vec<_> = base.ssts().iter().map(|(id, _)| *id).collect();
        // Only SSTs of live bases stay. Another task, such as a read, can still hold an older
        // base, so the bound is the set of live bases, not the last two.
        let (warmed, live) = source.warmed();
        assert!(
            warmed.is_subset(&live),
            "round {round}: {} warm SSTs, {} retired",
            warmed.len(),
            warmed.difference(&live).count()
        );
        let cold = base
            .ssts()
            .iter()
            .filter(|(id, l0)| *l0 && !warmed.contains(id))
            .count();
        if cold > 0 && warmed.iter().any(|id| current.contains(id)) {
            partial += 1;
        }
        crate::state::maintenance::compact_all(&admin)
            .await
            .unwrap();
    }
    assert!(
        partial >= 3,
        "only {partial} warms loaded some SSTs and then stopped"
    );
    writer.close().await.unwrap();
    state.close().await.unwrap();
}

/// SlateDB advances its GC boundary files with conditional updates. Both local store layouts
/// must accept them after a writer change, or manifest GC stops deleting files.
#[tokio::test]
async fn manifest_gc_continues_on_a_local_store_after_a_writer_change() {
    use futures::StreamExt;
    use slatedb::config::{GarbageCollectorDirectoryOptions, GarbageCollectorOptions};
    let dir = tempfile::tempdir().unwrap();
    let gc = GarbageCollectorOptions {
        manifest_options: Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::ZERO,
            dry_run: false,
        }),
        wal_options: None,
        wal_fence_options: None,
        compacted_options: None,
        compactions_options: None,
        detach_options: None,
        boundary_files_enabled: true,
        ..Default::default()
    };
    for config in [
        crate::object::Config::Local,
        crate::object::Config::SharedDirectory(dir.path().join("shared")),
    ] {
        let data = tempfile::tempdir_in(dir.path()).unwrap();
        let mut state = config
            .open(
                data.path(),
                "cluster",
                1,
                StateCache::default(),
                StateMode::Shared,
                &options(),
            )
            .await
            .unwrap();
        state.members = Arc::new(crate::cluster::Store::initial(
            &[(1, "127.0.0.1:1".to_owned())].into(),
        ));
        state.seed_for_test(1).await.unwrap();
        let source = state.shared.as_ref().unwrap().source.clone();
        let prefix =
            slatedb::object_store::path::Path::from(format!("{}/manifest", source.state()));
        let mut written = 0;
        let mut counts = vec![];
        // Each round opens a new writer, which fences the previous one.
        for _ in 0..2 {
            let writer = writer(&state).await;
            for n in 0..4 {
                state
                    .apply_etcd(state.applied + 1, &put("k", &n.to_string()))
                    .await
                    .unwrap();
                flush(&state, &writer, &mut written).await;
            }
            writer.close().await.unwrap();
            let before = source.store.list(Some(&prefix)).count().await;
            state.admin.run_gc_once(gc.clone()).await.unwrap();
            let after = source.store.list(Some(&prefix)).count().await;
            counts.push((before, after));
        }
        assert!(
            counts.iter().all(|(before, after)| after < before),
            "{config:?}: manifest files before and after each GC run: {counts:?}"
        );
        state.close().await.unwrap();
    }
}
