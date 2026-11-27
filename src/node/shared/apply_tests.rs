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
    lose_a_checkpoint_object(Lose::AtRow).await;
}

/// The base row is 19, and an announcement certified the checkpoint through the empty
/// entry 20. The member truncated its log through 20. The rebase opens the same checkpoint
/// at 20 with the record term, and the job reads the log from 21. A reader of the base index
/// never sees 19.
#[tokio::test]
async fn a_lost_checkpoint_object_rebases_apply_above_a_truncated_row() {
    lose_a_checkpoint_object(Lose::AboveRow).await;
}

/// Retention deleted the base at row 20, and the member truncated its log through 20. Only an
/// older checkpoint at row 10 is left. The base index never goes down, so the rebase does not
/// take it. Apply holds, and a covering snapshot ends the job with its prefix.
#[tokio::test]
async fn a_rebase_without_its_base_holds_above_an_older_checkpoint() {
    lose_a_checkpoint_object(Lose::Deleted).await;
}

/// The same with a base at row 19 that an announcement certified through 20. The rebase does
/// not find the base, and no checkpoint has its row in [19, 20]. It fails with a missing object,
/// and apply holds.
#[tokio::test]
async fn a_rebase_above_the_row_without_its_base_fails_with_a_missing_object() {
    lose_a_checkpoint_object(Lose::DeletedAboveRow).await;
}

/// The same, but the log still holds the entries above row 10. A replay from the older
/// checkpoint would succeed, but it would move the base index down from 20 to 10.
#[tokio::test]
async fn a_rebase_never_moves_the_base_down() {
    lose_a_checkpoint_object(Lose::DeletedInLog).await;
}

/// Retention deleted the base at row 20. The only checkpoint left has row 24, above the
/// applied index, so no local rebuild can use it. Apply holds instead of stopping. A snapshot
/// covers the job, and its install adopts the checkpoint at 24.
#[tokio::test]
async fn a_rebase_with_only_a_newer_checkpoint_holds_until_a_snapshot() {
    lose_a_checkpoint_object(Lose::Newer).await;
}

/// A shared-mode apply read fails with a transient store error after a lease grant and a
/// membership change applied in the same job. Apply keeps that prefix and its replies. After
/// the store recovers, it applies the rest, and each entry completes exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_apply_read_keeps_the_applied_prefix() {
    lose_a_checkpoint_object(Lose::Unavailable).await;
}

/// A read loses its object, and then every read of the store fails. The rebase cannot find its
/// replacement checkpoint, and it retries. A covering snapshot ends the job with its prefix.
/// The install of that snapshot fails on the store too, and a newer snapshot supersedes it.
/// After the store recovers, the newer snapshot installs, and apply goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn covering_snapshots_preempt_a_rebase_and_an_install_that_retry() {
    lose_a_checkpoint_object(Lose::Stuck).await;
}

/// A read loses its object. The rebase adopts its checkpoint, but its replay then fails on the
/// store. A covering snapshot ends the job. The node gets the prefix boundary and no state
/// below it. A queued job behind it also reports the boundary, the end of the prefix, and not
/// the index where the replay stopped. Then the snapshot installs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_covering_snapshot_preempts_a_failed_rebase_replay() {
    lose_a_checkpoint_object(Lose::Replay).await;
}

/// The base has row 19, and an announcement certified it through 20. The membership row is in
/// an older SST, and its read for the Raft snapshot record fails on the store. The Checkpoint
/// job fails, but the worker goes on, and the next Checkpoint job succeeds.
#[tokio::test]
async fn a_failed_checkpoint_read_keeps_the_worker() {
    lose_a_checkpoint_object(Lose::Checkpoint).await;
}

/// The base of `lose_a_checkpoint_object`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lose {
    /// The base has row 20, and its SST reads fail until the store recovers.
    Unavailable,
    /// The base has row 20.
    AtRow,
    /// The base has row 19, and an announcement certified it through the empty entry 20.
    AboveRow,
    /// The base has row 20 and is deleted. An older checkpoint has row 10.
    Deleted,
    /// As `Deleted`, but the member keeps its log.
    DeletedInLog,
    /// As `Deleted`, but the base has row 19, and an announcement certified it through 20.
    DeletedAboveRow,
    /// The base has row 20 and is deleted. A newer checkpoint has row 24.
    Newer,
    /// The base has row 20. After the lost read every read of the store fails.
    Stuck,
    /// The base has row 20. The first read of the rebase replay fails.
    Replay,
    /// As `AboveRow`, with a flush at 10. The read of the membership for a Checkpoint job
    /// fails on the store.
    Checkpoint,
}

async fn lose_a_checkpoint_object(lose: Lose) {
    let truncated = !matches!(
        lose,
        Lose::AtRow | Lose::Unavailable | Lose::Stuck | Lose::Replay | Lose::DeletedInLog
    );
    let deleted = matches!(
        lose,
        Lose::Deleted | Lose::DeletedInLog | Lose::DeletedAboveRow | Lose::Newer
    );
    let above = matches!(
        lose,
        Lose::AboveRow | Lose::Checkpoint | Lose::DeletedAboveRow
    );
    use crate::object::faults::{
        FaultStore, FAIL_SST_GET, LOSE_SST_TWICE, LOSE_SST_TWICE_THEN_FAIL,
    };
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
    if above {
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
    // A lease grant instead of the Compact starts no background compaction read, which could
    // take the lost reads.
    if matches!(lose, Lose::Unavailable | Lose::Replay | Lose::Stuck) {
        entries[20].data = RaftCommand::Lease {
            id: "21".into(),
            lease: LeaseCommand::Grant { id: 77, ttl: 60 },
            origin: 1,
        }
        .encode()
        .unwrap();
    }
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
    let state_options = crate::state::shared::Options {
        quota: 0,
        voters: vec![1],
        join: false,
        empty_log: false,
        pin_max: Duration::from_secs(300),
        overlay_max_bytes: 1 << 30,
        restore: None,
        record: None,
        store_id: None,
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
        &state_options,
    )
    .await
    .unwrap();
    let header = Header {
        cluster: 0,
        member: 1,
        term: 1,
    };
    // Two jobs, so a batch ends at 10 for the older checkpoint of `Lose::Deleted`.
    for range in [0..10, 10..20] {
        apply_entries(&mut state, entries[range].to_vec(), header)
            .await
            .unwrap();
    }
    // The leader writer checkpoints entry 20 with its own cache. The member switches to it.
    let handles = state.shared_handles();
    let writer = slatedb::Db::builder(handles.source.state(), store.clone())
        .with_settings(crate::state::slatedb_settings())
        .build()
        .await
        .unwrap();
    let mut older = None;
    for (seq, batch) in handles.queue.after(0, usize::MAX) {
        writer
            .write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
            .await
            .unwrap();
        let older_sst = matches!(
            lose,
            Lose::Deleted | Lose::DeletedInLog | Lose::DeletedAboveRow | Lose::Checkpoint
        );
        if seq == 10 && older_sst {
            writer.flush().await.unwrap();
            let options = CheckpointOptions {
                name: Some(crate::state::shared::view_name(seq)),
                ..Default::default()
            };
            let checkpoint = writer
                .create_checkpoint(CheckpointScope::Durable, &options)
                .await
                .unwrap();
            older = Some(checkpoint.id);
        }
    }
    writer.flush().await.unwrap();
    let row = if above { 19 } else { 20 };
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
    // Another member opens the base at 20, applies the job, and the writer checkpoints 24.
    let mut newer = None;
    if lose == Lose::Newer {
        let cache = crate::state::StateCache::default();
        let mut twin =
            StateMachine::open_shared(store.clone(), "root", "test", cache, &state_options)
                .await
                .unwrap();
        assert_eq!(twin.applied, 20);
        apply_entries(&mut twin, entries[20..].to_vec(), header)
            .await
            .unwrap();
        for (seq, batch) in twin.shared_handles().queue.after(20, usize::MAX) {
            writer
                .write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
                .await
                .unwrap();
        }
        writer.flush().await.unwrap();
        let options = CheckpointOptions {
            name: Some(crate::state::shared::view_name(24)),
            ..Default::default()
        };
        let checkpoint = writer
            .create_checkpoint(CheckpointScope::Durable, &options)
            .await
            .unwrap();
        twin.close().await.unwrap();
        newer = Some(checkpoint.id);
    }
    if truncated {
        // The member records the base at 20 and drops the log through it.
        let data = crate::state::transfer::ViewDescriptor::new(id)
            .encode()
            .unwrap();
        storage.checkpoint(20, data).unwrap();
        assert!(storage.reader().entries(20, 21).is_err());
    }

    let (events, _) = mpsc::channel(8);
    let (control, signals) = super::retry::signals();
    let mut executor = Executor {
        state,
        events,
        snapshot_task: None,
        inline_checkpoints: true,
        log: Some(storage.reader()),
        signals,
        reported: None,
        before_replay: None,
    };
    if deleted {
        executor.state.admin.delete_checkpoint(id).await.unwrap();
    }
    if lose == Lose::Checkpoint {
        let images = tempfile::tempdir().unwrap();
        let checkpoint = || Work {
            job: Job::Checkpoint {
                delay: Duration::ZERO,
                dir: images.path().to_owned(),
            },
            header,
            delay: Duration::ZERO,
        };
        // No read cached the SST of the flush at 10, so the membership read reaches the store.
        faults.set(FAIL_SST_GET);
        let failed = executor.execute(checkpoint()).await.unwrap();
        let Output::Checkpoint(Some(built)) = failed.output else {
            panic!("expected a checkpoint result");
        };
        let Err(error) = built.result else {
            panic!("the checkpoint did not fail");
        };
        assert_eq!(
            crate::storage_fault::classify(&error),
            Some(crate::storage_fault::StorageFault::Transient)
        );
        // The store recovers. The next Checkpoint job records the base at 20.
        faults.set(0);
        let done = executor.execute(checkpoint()).await.unwrap();
        let Output::Checkpoint(Some(built)) = done.output else {
            panic!("expected a checkpoint result");
        };
        assert_eq!(built.index, 20);
        assert!(matches!(built.result, Ok(Some(_))));
        executor.state.close().await.unwrap();
        writer.close().await.unwrap();
        return;
    }
    // The block cache load and its direct read both lose the object, so the job fails.
    if lose == Lose::Unavailable {
        faults.set(FAIL_SST_GET);
        let faults = faults.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            faults.set(0);
        });
    } else if lose == Lose::Stuck {
        faults.set(LOSE_SST_TWICE_THEN_FAIL);
    } else if lose == Lose::Replay {
        faults.set(LOSE_SST_TWICE);
        // The first replay read fails on the store, after the rebase adopted the base at 20.
        executor.before_replay = Some(Box::new(|| {
            Err(anyhow::Error::new(crate::storage_fault::Classified {
                fault: crate::storage_fault::StorageFault::Transient,
                message: "the store failed during the replay".into(),
            }))
        }));
    } else {
        faults.set(LOSE_SST_TWICE);
    }
    // Raft accepts a snapshot that covers entry 23 once the failed rebase waits on it. A
    // deleted base can lose the read of entry 22 first. The task returns the entry that apply
    // waited on, and whether it held there.
    let (preempt, mut paused) = control.handles();
    let stuck = deleted || matches!(lose, Lose::Stuck | Lose::Replay);
    let covering = tokio::spawn(async move {
        if !stuck {
            return None;
        }
        let waits = paused.wait_for(|paused| {
            paused
                .as_ref()
                .is_some_and(|paused| deleted || paused.index == 23)
        });
        let timed = tokio::time::timeout(Duration::from_secs(5), waits).await;
        let waited = timed
            .ok()?
            .ok()?
            .as_ref()
            .map(|paused| (paused.index, paused.held))?;
        preempt.send_replace(24);
        Some(waited)
    });
    // A reader on another thread records each base index that it sees during the job.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader = (lose == Lose::AboveRow).then(|| {
        let handles = executor.state.shared_handles();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut seen = vec![];
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let index = *handles.based.borrow();
                let base = handles.base_id.borrow().map(|base| base.index);
                if seen.last() != Some(&(index, base)) {
                    seen.push((index, base));
                }
            }
            seen
        })
    });
    let job = executor.execute(Work {
        job: Job::Entries(entries[20..].to_vec()),
        header,
        delay: Duration::ZERO,
    });
    let result = tokio::time::timeout(Duration::from_secs(10), job)
        .await
        .expect("no snapshot ended the waiting job");
    let completion = result.unwrap();
    let waited = covering.await.unwrap();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    if let Some(reader) = reader {
        // The rebase installs row 19 at its base index 20 at once. No reader sees 19.
        assert_eq!(reader.join().unwrap(), [(20, Some(20))]);
    }
    if deleted {
        assert_eq!(older.is_some(), lose != Lose::Newer);
        // The base did not move down to the older checkpoint.
        assert_eq!(executor.state.base_checkpoint(), Some(id));
        assert_eq!(executor.state.view_status().0, 20);
        // No checkpoint was between the base index and the applied index. Apply held, so a
        // follower asks its leader for a snapshot. The snapshot ended the job with its prefix.
        let (index, held) = waited.expect("apply did not wait");
        assert!(held);
        assert!(completion.early);
        assert_eq!(completion.applied, index - 1);
        // The rebase reports a missing object, not corrupt data.
        let error = executor
            .state
            .rebase(completion.applied, |_| Ok(1))
            .await
            .unwrap_err();
        assert_eq!(
            crate::storage_fault::classify(&error),
            Some(crate::storage_fault::StorageFault::Missing),
            "{error:#}"
        );
        let Output::Entries(replies) = completion.output else {
            panic!("expected entries");
        };
        let ids: Vec<_> = replies.iter().map(|reply| reply.id.clone()).collect();
        let prefix: Vec<_> = (21..index).map(|index| index.to_string()).collect();
        assert_eq!(ids, prefix);
        if let Some(newer) = newer {
            // The install adopts the newer checkpoint ahead of the applied index.
            let mut snapshot = Snapshot::default();
            snapshot.mut_metadata().index = 24;
            snapshot.mut_metadata().term = 1;
            snapshot.data = crate::state::transfer::ViewDescriptor::new(newer)
                .encode()
                .unwrap();
            let installed = executor
                .execute(Work {
                    job: Job::Snapshot(snapshot),
                    header,
                    delay: Duration::ZERO,
                })
                .await
                .unwrap();
            assert!(!installed.early);
            assert_eq!(installed.applied, 24);
            assert_eq!(executor.state.base_checkpoint(), Some(newer));
        }
        assert_eq!(control.paused(), None);
        executor.state.close().await.unwrap();
        writer.close().await.unwrap();
        return;
    }
    if matches!(lose, Lose::Stuck | Lose::Replay) {
        // The rebase failed and waited. The snapshot ended the job at its prefix boundary.
        assert!(completion.early);
        assert_eq!(completion.applied, 22);
        // A replay that failed sends no state below the prefix.
        assert_eq!(completion.state.is_none(), lose == Lose::Replay);
        let Output::Entries(replies) = completion.output else {
            panic!("expected entries");
        };
        let ids: Vec<_> = replies.iter().map(|reply| reply.id.as_str()).collect();
        assert_eq!(ids, ["21", "22"]);
        // The record names checkpoint 20 above its row. The test checks only positions.
        let record = |index: u64| {
            let mut snapshot = Snapshot::default();
            snapshot.mut_metadata().index = index;
            snapshot.mut_metadata().term = 1;
            snapshot.data = crate::state::transfer::ViewDescriptor::above_row(id, 20)
                .encode()
                .unwrap();
            Work {
                job: Job::Snapshot(snapshot),
                header,
                delay: Duration::ZERO,
            }
        };
        if lose == Lose::Replay {
            // The replay left the state at 20. The snapshot replaces it.
            assert_eq!(executor.state.applied, 20);
            // The job reported 22, the end of its prefix. A queued job that the snapshot
            // supersedes reports 22 too. The 20 where the replay stopped would stop the node on
            // a regressed apply position.
            let queued = executor
                .execute(Work {
                    job: Job::Entries(entries[22..].to_vec()),
                    header,
                    delay: Duration::ZERO,
                })
                .await
                .unwrap();
            assert!(queued.early);
            assert_eq!(queued.applied, 22);
            assert!(queued.state.is_none());
            let installed = executor.execute(record(24)).await.unwrap();
            assert!(!installed.early);
            assert_eq!(installed.applied, 24);
            assert!(installed.state.is_some());
        } else {
            let (preempt, mut paused) = control.handles();
            let newer = tokio::spawn(async move {
                let waits =
                    paused.wait_for(|paused| paused.as_ref().is_some_and(|p| p.index == 25));
                tokio::time::timeout(Duration::from_secs(5), waits)
                    .await
                    .expect("the install did not retry")
                    .unwrap();
                preempt.send_replace(30);
            });
            let install = executor.execute(record(24));
            let superseded = tokio::time::timeout(Duration::from_secs(10), install)
                .await
                .expect("no snapshot ended the waiting install")
                .unwrap();
            newer.await.unwrap();
            assert!(superseded.early);
            assert_eq!(superseded.applied, 22);
            faults.set(0);
            let installed = executor.execute(record(30)).await.unwrap();
            assert!(!installed.early);
            assert_eq!(installed.applied, 30);
        }
        assert_eq!(control.paused(), None);
        executor.state.close().await.unwrap();
        writer.close().await.unwrap();
        return;
    }
    if lose == Lose::Unavailable {
        // No rebase ran. The prefix kept its lease grant and its membership change.
        assert!(faults.entered.available_permits() > 0, "no read failed");
        assert_eq!(completion.applied, 24);
        let Output::Entries(replies) = completion.output else {
            panic!("expected entries");
        };
        let ids: Vec<_> = replies.iter().map(|reply| reply.id.as_str()).collect();
        assert_eq!(ids, ["21", "22", "23", "24"]);
        assert!(matches!(
            replies[0].lease,
            Some(LeaseCommand::Grant { id: 77, .. })
        ));
        assert_eq!(replies[1].change.as_ref().unwrap().node_id, 9);
        assert!(replies[2..].iter().all(|reply| reply.change.is_none()));
        assert!(executor.state.members.members.contains_key(&9));
        assert_eq!(executor.state.base_checkpoint(), Some(id));
        executor.state.close().await.unwrap();
        writer.close().await.unwrap();
        return;
    }
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
        overlay_max_bytes: 1 << 30,
        restore: None,
        record: None,
        store_id: None,
    };
    let mut members = vec![];
    for _ in 0..2 {
        let state =
            StateMachine::open_shared(store.clone(), "root", "test", Default::default(), &options)
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
    let handles = state.shared_handles();
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
    let queue = |state: &StateMachine| state.shared_handles().queue;
    assert_eq!((grouped.applied, queue(&grouped).pushed()), (2, 2));
    assert_eq!((apart.applied, queue(&apart).pushed()), (2, 1));
    let (writer, base) = publish_batches(&grouped).await;
    assert_eq!(base.applied, 2);
    apart.shared.offer(base);
    apart.switch_base();
    let handles = apart.shared_handles();
    assert_eq!(*handles.based.borrow(), 2);
    assert_eq!(handles.queue.len(), 0);
    assert_eq!(apart.view_status().2, 0);
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
    let handles = grouped.shared_handles();
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
    assert_eq!(grouped.view_status().2, 0);
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
    assert_eq!(grouped.view_status().0, 2);
    writer.close().await.unwrap();
}
