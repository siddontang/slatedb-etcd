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

/// Every row under `prefix` except the membership, which only shared mode stores at entry 1.
async fn rows(state: &StateMachine, prefix: &[u8]) -> Vec<Row> {
    source::rows(state.applied_state(), prefix)
        .await
        .into_iter()
        .filter(|row| row.key.as_ref() != crate::cluster::KEY)
        .collect()
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
    // Shared mode stores the membership with the first entry. Let the reference do the same.
    reference.members_unsaved = true;
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
        state.logical_bytes.to_be_bytes()
    );
    assert_eq!(
        state.logical_bytes,
        maintenance::count_logical_size(state.applied_state())
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

#[tokio::test]
async fn no_op_entries_write_no_batch_after_the_membership_entry() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    // The first entry stores the bootstrap membership on every member, even when it is empty.
    state.apply(1, None).await.unwrap();
    assert_eq!(state.write_batches, 1);
    let members = state.applied_state().get(crate::cluster::KEY).await;
    assert!(members.unwrap().is_some());
    let queue = state.shared.as_ref().unwrap().overlay.queue();
    assert_eq!(queue.pushed(), 1);
    state.apply(2, None).await.unwrap();
    state
        .apply_view_checkpoint(3, &uuid::Uuid::new_v4().to_string())
        .await
        .unwrap();
    assert_eq!(state.applied, 3);
    assert_eq!(state.write_batches, 1);
    assert_eq!(queue.pushed(), 1);
    // The view at the applied index equals the state of the last batch.
    let view = state.read_view().unwrap();
    assert_eq!(
        view.state().get(APPLIED).await.unwrap().unwrap().as_ref(),
        1_u64.to_be_bytes()
    );
    state.apply_etcd(4, &put("k", "v")).await.unwrap();
    assert_eq!(state.write_batches, 2);
    assert_eq!(queue.pushed(), 4);
    // The view of index 3 does not see the later write.
    assert_eq!(view.state().get(b"etcd/current/k").await.unwrap(), None);
    state.close().await.unwrap();
}

/// Open the shared writer, as the leader task does.
async fn writer(state: &StateMachine) -> slatedb::Db {
    let source = state.shared.as_ref().unwrap().source.clone();
    source.open_writer().await.unwrap()
}

/// Write the overlay batches into the shared database, flush, and open a checkpoint of it.
async fn checkpoint(
    state: &StateMachine,
    writer: &slatedb::Db,
    written: &mut u64,
) -> Arc<view::Base> {
    let queue = state.shared.as_ref().unwrap().overlay.queue();
    for (seq, batch) in queue.after(*written, usize::MAX) {
        writer
            .write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
            .await
            .unwrap();
        *written = seq;
    }
    writer.flush().await.unwrap();
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
    for n in 0..10 {
        state
            .apply_etcd(state.applied + 1, &put(&format!("k{n}"), "one"))
            .await
            .unwrap();
    }
    let older = checkpoint(&state, &writer, &mut written).await;
    assert_eq!(older.applied, 10);
    state
        .apply_view_checkpoint(state.applied + 1, &uuid::Uuid::new_v4().to_string())
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
        .apply_view_checkpoint(state.applied + 1, &uuid::Uuid::new_v4().to_string())
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

#[tokio::test]
async fn a_restart_writes_a_missing_bootstrap_marker() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut state = open_shared(store.clone()).await;
    state.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    state.apply(1, None).await.unwrap();
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
    reference.members_unsaved = true;
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
    let reference_backup = super::tests::backup_bytes(reference.snapshot_data().await.unwrap());

    let restored = {
        let path = dir.path().join("from-shared");
        std::fs::create_dir(&path).unwrap();
        super::tests::restore(&path, shared_backup).await.0
    };
    let expected = {
        let path = dir.path().join("from-replicated");
        std::fs::create_dir(&path).unwrap();
        super::tests::restore(&path, reference_backup).await.0
    };
    assert_eq!(rows(&restored, b"").await, rows(&expected, b"").await);
    assert_eq!(restored.logical_size(), expected.logical_size());
    assert_eq!(
        restored.logical_size(),
        maintenance::count_logical_size(restored.applied_state())
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
    let (restored, _) = super::tests::restore(&path, super::tests::backup_bytes(image)).await;
    assert_eq!(
        restored
            .applied_state()
            .get(shared::LOGICAL_BYTES)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        restored.logical_size(),
        maintenance::count_logical_size(restored.applied_state())
            .await
            .unwrap()
    );
    restored.close().await.unwrap();
    source.close().await.unwrap();
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

/// A received snapshot that applies nothing releases its checkpoint.
#[tokio::test]
async fn a_skipped_snapshot_releases_its_receipt() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    state.apply_etcd(1, &put("a", "1")).await.unwrap();
    let mut written = 0;
    let writer = writer(&state).await;
    let id = checkpoint(&state, &writer, &mut written)
        .await
        .checkpoint
        .unwrap();
    state.apply_etcd(2, &put("b", "2")).await.unwrap();
    let source = state.shared.as_ref().unwrap().source.clone();
    source.receive(id).await.unwrap();
    assert!(source.pinned().contains(&id));
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 1;
    snapshot.data = transfer::ViewDescriptor::new(id).encode().unwrap();
    state.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(state.applied, 2);
    assert!(!source.pinned().contains(&id));
    state.close().await.unwrap();
}

/// A slow base read ends at the read's deadline with a retryable error.
#[tokio::test]
async fn the_deadline_bounds_a_slow_base_read() {
    use crate::object::faults::{FaultStore, DELAY_SST_GET};
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
    let view = state.read_view().unwrap();
    // Each SST read takes 1 s, far above the 20 ms pin limit. A loaded host still ends the
    // read well below one SST read.
    faults.set_delay(Duration::from_secs(1));
    faults.set(DELAY_SST_GET);
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
    let error = scanned.await.unwrap_err();
    assert!(view::expired(&error.to_string()), "{error}");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
    faults.set(0);
    state.close().await.unwrap();
}

async fn announce(state: &mut StateMachine, base: &view::Base) {
    let id = base.checkpoint.unwrap().to_string();
    let index = state.applied + 1;
    state.apply_view_checkpoint(index, &id).await.unwrap();
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
    opened.offer(at_six.clone());
    // A switch cannot use a base above the applied index.
    behind.switch_base();
    assert_eq!(behind.applied, 2);
    // The node verified checkpoint 4. A newer open in the slot does not replace it.
    behind.adopt_opened(at_four, 4).await.unwrap();
    assert_eq!(behind.applied, 4);
    assert_eq!(rows(&behind, b"etcd/").await, rows_at_four);
    assert_eq!(opened.newest(), Some(6));
    behind
        .adopt_opened(opened.take().unwrap(), 4)
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
