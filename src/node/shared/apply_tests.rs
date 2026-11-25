//! Shared-mode apply tests: a lost checkpoint object, and members that group entries in other
//! ways. See `docs/shared-state.md`.

use super::*;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::response_op::Response as Resp;
use crate::proto::etcdserverpb::*;

/// A shared-mode apply read loses its SST. Apply rebases on the newest checkpoint, replays
/// the local log, and applies the job again with the same results.
#[tokio::test]
async fn a_lost_checkpoint_object_rebases_apply() {
    lose_a_checkpoint_object(false).await;
}

/// The base row is 19, and an announcement certified the checkpoint through the empty
/// entry 20. The member truncated its log through 20. The rebase opens the same checkpoint
/// at 20 with the record term, and the job reads the log from 21.
#[tokio::test]
async fn a_lost_checkpoint_object_rebases_apply_above_a_truncated_row() {
    lose_a_checkpoint_object(true).await;
}

async fn lose_a_checkpoint_object(truncated: bool) {
    use crate::object::faults::{FaultStore, LOSE_SST_TWICE};
    use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions};
    use slatedb::object_store::ObjectStore;
    let faults = std::sync::Arc::new(FaultStore::new(std::sync::Arc::new(
        slatedb::object_store::memory::InMemory::new(),
    )));
    let dir = tempfile::tempdir().unwrap();
    let mut storage = RaftStorage::open(dir.path(), 1, "test", vec![1]).unwrap();
    let command = |index: u64, request| {
        RaftCommand::Etcd {
            id: index.to_string(),
            origin: 1,
            etcd: RequestOp {
                request: Some(request),
            },
        }
        .encode()
        .unwrap()
    };
    let put = |index: u64| {
        command(
            index,
            Op::RequestPut(PutRequest {
                key: format!("k{}", index % 4).into_bytes(),
                // Large values give the SST many blocks. The read after the fault then
                // needs a block that no earlier read cached.
                value: format!("v{index}-{}", "x".repeat(2048)).into_bytes(),
                ..Default::default()
            }),
        )
    };
    let mut entries: Vec<Entry> = (1..=24)
        .map(|index| Entry {
            index,
            term: 1,
            data: put(index),
            ..Default::default()
        })
        .collect();
    // A rebase opens only a checkpoint with the membership, so entry 1 seeds it.
    entries[0].data = RaftCommand::SeedMembers(crate::cluster::Seed {
        id: "1".into(),
        origin: 1,
        members: serde_json::to_string(&crate::cluster::Store::initial(
            &[(1, "127.0.0.1:1".to_owned())].into(),
        ))
        .unwrap(),
        digest: None,
        applied: 0,
    })
    .encode()
    .unwrap();
    // Entry 20 changes no state, so the base row is 19.
    if truncated {
        entries[19].data.clear();
    }
    // Entry 21 is a physical Compact, and entry 22 adds a learner. Their completions come
    // before the lost read of entry 23 in the same job.
    entries[20].data = RaftCommand::Compact {
        id: "21".into(),
        compact: CompactionRequest {
            revision: 3,
            physical: true,
        },
    }
    .encode()
    .unwrap();
    let request = crate::cluster::Command::Add(Member {
        id: 9,
        peer_ur_ls: vec!["http://127.0.0.1:9".into()],
        is_learner: true,
        ..Default::default()
    });
    let mut change = request.change();
    change.context = RaftCommand::Cluster {
        id: "22".into(),
        request,
        credentials: Default::default(),
    }
    .encode()
    .unwrap();
    entries[21].entry_type = EntryType::EntryConfChange as i32;
    entries[21].data = change.encode_to_vec();
    // Entry 23 reads an old revision. Only the base holds that history row.
    entries[22].data = command(
        23,
        Op::RequestTxn(TxnRequest {
            success: vec![RequestOp {
                request: Some(Op::RequestRange(RangeRequest {
                    key: b"k0".to_vec(),
                    revision: 5,
                    ..Default::default()
                })),
            }],
            ..Default::default()
        }),
    );
    let hs = raft::eraftpb::HardState {
        term: 1,
        vote: 1,
        commit: 24,
    };
    storage.persist(&entries, Some(&hs), None).unwrap();
    let options = crate::state::shared::Options {
        quota: 0,
        voters: vec![1],
        join: false,
        empty_log: false,
        pin_max: Duration::from_secs(300),
        restore: None,
        record: None,
    };
    let store: std::sync::Arc<dyn ObjectStore> = faults.clone();
    let mut state = StateMachine::open_shared(
        store.clone(),
        "root",
        "test",
        // A tiny cache keeps few blocks, so the read after the fault reaches the store.
        crate::state::StateCache {
            block_bytes: 1,
            meta_bytes: 1,
            row_bytes: 0,
        },
        &options,
        crate::state::transfer::Backend::Local,
    )
    .await
    .unwrap();
    let header = Header {
        cluster: 0,
        member: 1,
        term: 1,
    };
    apply_entries(&mut state, entries[..20].to_vec(), header)
        .await
        .unwrap();
    // The leader writer checkpoints entry 20 with its own cache. The member switches to it.
    let handles = state.shared_handles().unwrap();
    let writer = slatedb::Db::builder(handles.source.state(), store)
        .with_settings(crate::state::slatedb_settings())
        .build()
        .await
        .unwrap();
    for (seq, batch) in handles.queue.after(0, usize::MAX) {
        writer
            .write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
            .await
            .unwrap();
    }
    writer.flush().await.unwrap();
    let row = if truncated { 19 } else { 20 };
    let options = CheckpointOptions {
        name: Some(crate::state::shared::view_name(row)),
        ..Default::default()
    };
    let id = writer
        .create_checkpoint(CheckpointScope::Durable, &options)
        .await
        .unwrap()
        .id;
    let base = handles.source.open_base(id).await.unwrap();
    assert_eq!(base.applied, row);
    handles.opened.offer(base, 20);
    state.switch_base();
    assert_eq!(*handles.based.borrow(), 20);
    if truncated {
        // The member records the base at 20 and drops the log through it.
        let data = crate::state::transfer::ViewDescriptor::new(id)
            .encode()
            .unwrap();
        storage.checkpoint(20, data).unwrap();
        assert!(storage.reader().entries(20, 21).is_err());
    }

    let (events, _) = mpsc::channel(8);
    let mut executor = Executor {
        state,
        events,
        snapshot_task: None,
        inline_checkpoints: true,
        log: Some(storage.reader()),
    };
    // The block cache load and its direct read both lose the object, so the job fails.
    faults.set(LOSE_SST_TWICE);
    let completion = executor
        .execute(Work {
            job: Job::Entries(entries[20..].to_vec()),
            header,
            delay: Duration::ZERO,
        })
        .await
        .unwrap();
    // The read lost its object. The rebase restarted the job, and every entry of the job
    // completes exactly once, with the Compact notice and the membership change.
    assert_eq!(faults.entered.available_permits(), 2);
    assert_eq!(completion.applied, 24);
    let Output::Entries(replies) = completion.output else {
        panic!("expected entries");
    };
    let ids: Vec<_> = replies.iter().map(|reply| reply.id.as_str()).collect();
    assert_eq!(ids, ["21", "22", "23", "24"]);
    assert!(matches!(replies[0].reply, Reply::PhysicalCompact { .. }));
    assert_eq!(replies[1].change.as_ref().unwrap().node_id, 9);
    let Reply::Kv(outcome) = &replies[2].reply else {
        panic!("expected a KV reply");
    };
    let Some(Resp::ResponseTxn(txn)) = &outcome.result.as_ref().unwrap().response else {
        panic!("expected a transaction");
    };
    // At revision 5, `k0` has the put of entry 4.
    let Some(Resp::ResponseRange(range)) = &txn.responses[0].response else {
        panic!("expected a range");
    };
    assert!(range.kvs[0].value.starts_with(b"v4-"));
    // The rebase opened the same checkpoint again. It holds the state through entry 20.
    assert_eq!(executor.state.base_checkpoint(), Some(id));
    executor.state.close().await.unwrap();
    writer.close().await.unwrap();
}

/// Two shared members on one store, and the entries of #96: a Put, then an update of a
/// missing key with `ignore_value`. The update fails, so it changes no state.
async fn grouping_members() -> (StateMachine, StateMachine, Vec<Entry>) {
    let store: std::sync::Arc<dyn slatedb::object_store::ObjectStore> =
        std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    let options = crate::state::shared::Options {
        quota: 0,
        voters: vec![1],
        join: false,
        empty_log: false,
        pin_max: Duration::from_secs(300),
        restore: None,
        record: None,
    };
    let mut members = vec![];
    for _ in 0..2 {
        let state = StateMachine::open_shared(
            store.clone(),
            "root",
            "test",
            Default::default(),
            &options,
            crate::state::transfer::Backend::Local,
        )
        .await
        .unwrap();
        members.push(state);
    }
    let entry = |index: u64, put: PutRequest| Entry {
        index,
        term: 1,
        data: RaftCommand::Etcd {
            id: index.to_string(),
            origin: 1,
            etcd: RequestOp {
                request: Some(Op::RequestPut(put)),
            },
        }
        .encode()
        .unwrap(),
        ..Default::default()
    };
    let entries = vec![
        entry(
            1,
            PutRequest {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            },
        ),
        entry(
            2,
            PutRequest {
                key: b"missing".to_vec(),
                ignore_value: true,
                ..Default::default()
            },
        ),
    ];
    let second = members.pop().unwrap();
    (members.pop().unwrap(), second, entries)
}

const HEADER: Header = Header {
    cluster: 0,
    member: 1,
    term: 1,
};

/// Write the overlay batches of `state` to the shared database and create a checkpoint.
async fn publish_batches(
    state: &StateMachine,
) -> (slatedb::Db, std::sync::Arc<crate::state::view::Base>) {
    let handles = state.shared_handles().unwrap();
    let writer = handles.source.open_writer().await.unwrap();
    for (seq, batch) in handles.queue.after(0, usize::MAX) {
        writer
            .write_with_options(
                batch.to_storage(),
                &slatedb::config::WriteOptions { seqnum: seq },
            )
            .await
            .unwrap();
    }
    writer.flush().await.unwrap();
    let base = checkpoint_of(&handles.source, &writer).await;
    (writer, base)
}

async fn checkpoint_of(
    source: &crate::state::shared::Source,
    writer: &slatedb::Db,
) -> std::sync::Arc<crate::state::view::Base> {
    let options = slatedb::config::CheckpointOptions {
        name: Some(crate::state::shared::view_name(
            writer.manifest().last_l0_seq(),
        )),
        ..Default::default()
    };
    let id = writer
        .create_checkpoint(slatedb::config::CheckpointScope::Durable, &options)
        .await
        .unwrap()
        .id;
    source.open_base(id).await.unwrap()
}

/// #96: one member groups the Put and the failed update into one batch at 2. The other
/// applies them apart and stores its last batch at 1. It still adopts the checkpoint at 2,
/// because it applied through 2, and it drops its overlay.
#[tokio::test]
async fn a_member_that_applies_entries_apart_adopts_a_grouped_checkpoint() {
    use crate::state::StateRead;
    let (mut grouped, mut apart, entries) = grouping_members().await;
    apply_entries(&mut grouped, entries.clone(), HEADER)
        .await
        .unwrap();
    for entry in &entries {
        apply_entries(&mut apart, vec![entry.clone()], HEADER)
            .await
            .unwrap();
    }
    let queue = |state: &StateMachine| state.shared_handles().unwrap().queue;
    assert_eq!((grouped.applied, queue(&grouped).pushed()), (2, 2));
    assert_eq!((apart.applied, queue(&apart).pushed()), (2, 1));
    let (writer, base) = publish_batches(&grouped).await;
    assert_eq!(base.applied, 2);
    apart.shared.as_ref().unwrap().offer(base);
    apart.switch_base();
    let handles = apart.shared_handles().unwrap();
    assert_eq!(*handles.based.borrow(), 2);
    assert_eq!(handles.queue.len(), 0);
    assert_eq!(apart.view_status().unwrap().2, 0);
    let view = apart.read_view().unwrap();
    let value = view.state().get(b"etcd/current/k").await.unwrap();
    assert!(value.is_some());
    writer.close().await.unwrap();
}

/// The reverse order: the leader stores the Put at 1 and no batch at 2. A follower groups
/// both entries into one batch at 2. The checkpoint row is 1, so only an announcement that
/// certifies the checkpoint through 2 lets the follower drop its batch at 2. Another
/// checkpoint at the same row does not get that coverage.
#[tokio::test]
async fn a_certified_checkpoint_covers_a_batch_of_another_grouping() {
    use crate::state::StateRead;
    let (mut grouped, mut apart, entries) = grouping_members().await;
    for entry in &entries {
        apply_entries(&mut apart, vec![entry.clone()], HEADER)
            .await
            .unwrap();
    }
    apply_entries(&mut grouped, entries, HEADER).await.unwrap();
    let (writer, base) = publish_batches(&apart).await;
    assert_eq!(base.applied, 1);
    let handles = grouped.shared_handles().unwrap();
    // The row alone covers only the batch at 1, which this member does not have.
    handles.opened.offer(base.clone(), 0);
    grouped.switch_base();
    assert_eq!(*handles.based.borrow(), 1);
    assert_eq!(handles.queue.len(), 1);
    // The announcement at 3 certifies the current base through 2. It opens nothing.
    let id = base.checkpoint.unwrap();
    grouped
        .apply_view_checkpoint(3, &id.to_string(), 2)
        .await
        .unwrap();
    grouped.switch_base();
    assert_eq!(*handles.based.borrow(), 2);
    assert_eq!(handles.queue.len(), 0);
    assert_eq!(grouped.view_status().unwrap().2, 0);
    let value = grouped.applied_state().get(b"etcd/current/k").await;
    assert!(value.unwrap().is_some());
    // A checkpoint at the same row with no certified coverage does not replace it.
    let other = checkpoint_of(&handles.source, &writer).await;
    assert_eq!(other.applied, 1);
    assert_ne!(other.checkpoint, base.checkpoint);
    handles.opened.offer(other, 0);
    grouped.switch_base();
    assert_eq!(*handles.based.borrow(), 2);
    assert_eq!(grouped.base_checkpoint(), Some(id));
    assert_eq!(grouped.view_status().unwrap().0, 2);
    writer.close().await.unwrap();
}
