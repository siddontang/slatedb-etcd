//! Compare shared-mode apply with replicated apply, and check bases, claims, and reopen.

use super::*;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::*;
use slatedb::config::{CheckpointOptions, CheckpointScope};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

async fn open_shared(store: Arc<dyn ObjectStore>) -> StateMachine {
    StateMachine::open_shared(
        store,
        "cluster",
        "test",
        StateCache::default(),
        0,
        &[1],
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
    // The rows exist only in the overlay. Apply never wrote the shared database.
    assert_eq!(state.db.get(APPLIED).await.unwrap(), None);
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

/// Write the overlay batches into the shared database, flush, and open a checkpoint of it.
async fn checkpoint(state: &StateMachine, written: &mut u64) -> Arc<view::Base> {
    let queue = state.shared.as_ref().unwrap().overlay.queue();
    for (seq, batch) in queue.after(*written, usize::MAX) {
        state
            .db
            .write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
            .await
            .unwrap();
        *written = seq;
    }
    state.db.flush().await.unwrap();
    let id = state
        .db
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
    for n in 0..10 {
        state
            .apply_etcd(state.applied + 1, &put(&format!("k{n}"), "one"))
            .await
            .unwrap();
    }
    let older = checkpoint(&state, &mut written).await;
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
    let newer = checkpoint(&state, &mut written).await;
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
    // The two closed segments are covered. The open segment holds the delete.
    let overlay = &state.shared.as_ref().unwrap().overlay;
    assert_eq!(overlay.segments().len(), 1);
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
        0,
        &[1],
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
    checkpoint(&state, &mut written).await;
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
    assert_eq!(shared::retired(&views, &none, 2), ids[..3]);
    let pinned = [ids[1]].into_iter().collect();
    assert_eq!(shared::retired(&views, &pinned, 2), ids[..1]);
    // A pin on a checkpoint that the publisher keeps anyway changes nothing.
    let pinned = [ids[4]].into_iter().collect();
    assert_eq!(shared::retired(&views, &pinned, 2), ids[..3]);
}

#[tokio::test]
async fn retention_keeps_the_checkpoint_of_a_live_base() {
    let mut state = open_shared(Arc::new(InMemory::new())).await;
    let mut written = 0;
    let mut bases = vec![];
    for n in 0..5 {
        state
            .apply_etcd(state.applied + 1, &put(&format!("k{n}"), "v"))
            .await
            .unwrap();
        bases.push(checkpoint(&state, &mut written).await);
    }
    let source = state.shared.as_ref().unwrap().source.clone();
    let count = || async { shared::view_checkpoints(&state.admin).await.unwrap().len() };
    // A slow open still uses the second checkpoint. Retention keeps it and all newer ones.
    let pinned = bases.remove(1);
    drop(bases);
    shared::retain(&state.admin, &source, 2).await.unwrap();
    assert_eq!(count().await, 4);
    assert_eq!(
        pinned.get(b"meta/applied").await.unwrap().unwrap().as_ref(),
        2_u64.to_be_bytes()
    );
    drop(pinned);
    shared::retain(&state.admin, &source, 2).await.unwrap();
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
    // The view scan flushed nothing. The shared database is still empty.
    assert_eq!(shared_state.durable_applied, 0);
    assert_eq!(shared_state.db.snapshot().unwrap().seq(), 0);
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
    for n in 0..5 {
        state
            .apply_etcd(state.applied + 1, &put(&format!("k{n}"), "v"))
            .await
            .unwrap();
        drop(checkpoint(state, &mut written).await);
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
    let candidates = source.begin_retire(&views, 2);
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
    shared::retain(&state.admin, &source, 2).await.unwrap();
    assert_eq!(
        shared::view_checkpoints(&state.admin).await.unwrap().len(),
        5
    );
    drop(opening);
    shared::retain(&state.admin, &source, 2).await.unwrap();
    assert_eq!(
        shared::view_checkpoints(&state.admin).await.unwrap().len(),
        2
    );
    state.close().await.unwrap();
}
