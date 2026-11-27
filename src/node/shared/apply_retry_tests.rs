//! A storage error of an apply read leaves the entry unapplied, and apply runs it again.

use super::*;
use crate::object::faults::{FaultStore, FAIL_SST_GET};
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::*;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use std::sync::Arc;

/// No block, metadata, or row cache, so each read of a flushed row reads the store.
const COLD: crate::state::StateCache = crate::state::StateCache {
    block_bytes: 1,
    meta_bytes: 1,
    row_bytes: 0,
};

const HEADER: Header = Header {
    cluster: 1,
    member: 1,
    term: 1,
};

/// A large value fills its own SST block, so a read of it after a restart reads the store.
const VALUE: [u8; 8 << 10] = [b'v'; 8 << 10];

fn put(key: &str) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestPut(PutRequest {
            key: key.as_bytes().to_vec(),
            value: VALUE.to_vec(),
            ..Default::default()
        })),
    }
}

/// A Txn that compares the value of `k1` and then puts `k2`. The compare reads the row of `k1`.
fn txn_entry(index: u64) -> Entry {
    let txn = TxnRequest {
        compare: vec![Compare {
            key: b"k1".to_vec(),
            target: compare::CompareTarget::Value as i32,
            target_union: Some(compare::TargetUnion::Value(VALUE.to_vec())),
            ..Default::default()
        }],
        success: vec![put("k2")],
        ..Default::default()
    };
    Entry {
        index,
        term: 1,
        data: RaftCommand::Etcd {
            id: index.to_string(),
            origin: 1,
            etcd: RequestOp {
                request: Some(Op::RequestTxn(txn)),
            },
        }
        .encode()
        .unwrap(),
        ..Default::default()
    }
}

/// A state on `store` with the membership at index 1, and `k0` to `k2` through index 4. See
/// `reopen_cold`.
async fn cold(store: Arc<dyn ObjectStore>) -> StateMachine {
    let mut state = open_cold(store).await;
    // A restart opens only a base that holds the membership.
    state.seed_for_test(1).await.unwrap();
    for index in 2..=4 {
        let key = format!("k{}", index - 2);
        state.apply_etcd(index, &put(&key)).await.unwrap();
    }
    reopen_cold(state).await
}

/// Open a state on `store` with empty caches.
async fn open_cold(store: Arc<dyn ObjectStore>) -> StateMachine {
    let options = crate::state::shared::tests::options();
    StateMachine::open_shared(store, "root", "test", COLD, &options)
        .await
        .unwrap()
}

/// Write the overlay of `state` into a shared checkpoint, and open the state again on it with
/// empty caches. The rows are then in SSTs, so a read of them reads the store.
async fn reopen_cold(state: StateMachine) -> StateMachine {
    use crate::state::shared::tests::{checkpoint, writer};
    let writer = writer(&state).await;
    drop(checkpoint(&state, &writer, &mut 0).await);
    writer.close().await.unwrap();
    state.close().await.unwrap();
    open_cold(state.shared.as_ref().unwrap().source.store.clone()).await
}

async fn hash(state: &mut StateMachine) -> Value {
    let command = crate::maintenance::Command::Hash(0);
    let outcome = state
        .maintenance(&command, &Default::default(), Default::default())
        .await
        .unwrap();
    serde_json::to_value(outcome.result.unwrap()).unwrap()
}

/// A storage error of a Txn compare is no result of the entry: the entry stays unapplied until
/// the read succeeds. Then it applies once, and the state matches a member with no fault.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_read_leaves_the_entry_unapplied() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let state = cold(faults.clone()).await;
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(state, events, true, None);
    worker
        .submit(Work {
            job: Job::Entries(vec![txn_entry(5)]),
            header: HEADER,
            delay: Duration::ZERO,
        })
        .unwrap();
    faults.set(FAIL_SST_GET);
    let recover = {
        let faults = faults.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            faults.set(0);
        })
    };
    let completion = worker.step().await.unwrap().unwrap();
    recover.await.unwrap();
    assert!(faults.entered.available_permits() > 0, "no read failed");
    assert_eq!(completion.applied, 5);
    let Output::Entries(replies) = completion.output else {
        panic!("expected entries");
    };
    let [AppliedCommand {
        reply: Reply::Kv(outcome),
        ..
    }] = &replies[..]
    else {
        panic!("expected one KV reply");
    };
    let Some(response_op::Response::ResponseTxn(txn)) = &outcome.result.as_ref().unwrap().response
    else {
        panic!("expected a Txn reply");
    };
    assert!(txn.succeeded);
    let mut twin = cold(Arc::new(InMemory::new())).await;
    apply_entries(&mut twin, vec![txn_entry(5)], HEADER)
        .await
        .unwrap();
    assert_eq!(hash(worker.state_mut()).await, hash(&mut twin).await);
    twin.close().await.unwrap();
}

/// A shutdown ends an apply job that retries a failed read. It does not wait for the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shutdown_ends_a_retrying_apply_at_once() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let state = cold(faults.clone()).await;
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(state, events, false, None);
    faults.set(FAIL_SST_GET);
    worker
        .submit(Work {
            job: Job::Entries(vec![txn_entry(5)]),
            header: HEADER,
            delay: Duration::ZERO,
        })
        .unwrap();
    // The read keeps failing, so the job does not complete.
    let pending = tokio::time::timeout(Duration::from_millis(500), worker.next()).await;
    assert!(pending.is_err(), "the job ended while its read failed");
    tokio::time::timeout(Duration::from_secs(2), worker.shutdown())
        .await
        .expect("the shutdown waited for the store")
        .unwrap();
    faults.set(0);
}

/// An error of the member's own invariants still stops apply. Only a storage error of a read
/// and a missing object retry, and only corrupt stored data holds.
#[tokio::test]
async fn an_invariant_error_still_stops_apply() {
    let dir = tempfile::tempdir().unwrap();
    let state = StateMachine::open_test(dir.path()).await.unwrap();
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(state, events, true, None);
    worker
        .submit(Work {
            job: Job::Entries(vec![txn_entry(5)]),
            header: HEADER,
            delay: Duration::ZERO,
        })
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), worker.step())
        .await
        .expect("apply retried an invariant error")
        .err()
        .expect("apply accepted an index gap");
    assert!(format!("{error:#}").contains("index gap"), "{error:#}");
    let corrupt = anyhow::Error::new(crate::mvcc::storage_fault::Classified {
        fault: crate::mvcc::storage_fault::StorageFault::Corrupt,
        message: "short history key".into(),
    });
    assert_eq!(retryable(&corrupt), Some(StorageFault::Corrupt));
    let invariant = anyhow::anyhow!("the base checkpoint has no applied term");
    assert_eq!(retryable(&invariant), None);
    worker.state_mut().close().await.unwrap();
}

/// An entry with no command, such as the first entry of a new leader.
fn empty(index: u64, term: u64) -> Entry {
    Entry {
        index,
        term,
        ..Default::default()
    }
}

/// Recover the store after 300 ms.
fn recover_soon(faults: &Arc<FaultStore>) -> tokio::task::JoinHandle<()> {
    let faults = faults.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        faults.set(0);
    })
}

fn entries(job: Vec<Entry>) -> Work {
    Work {
        job: Job::Entries(job),
        header: HEADER,
        delay: Duration::ZERO,
    }
}

/// An early Put evaluation can run before its entries commit, and a new leader can replace
/// them. A read of it that keeps failing ends the job at once and frees the worker. The next
/// entry of the new leader then applies while the store still fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_early_put_evaluation_frees_the_worker() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let state = cold(faults.clone()).await;
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(state, events, true, None);
    // The previous value of `k1` is in an SST, so the evaluation reads the store.
    let command = RaftCommand::Etcd {
        id: "5".into(),
        origin: 1,
        etcd: RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: b"k1".to_vec(),
                value: b"new".to_vec(),
                prev_kv: true,
                ..Default::default()
            })),
        },
    };
    faults.set(FAIL_SST_GET);
    worker
        .submit(Work {
            job: Job::PreparePuts {
                commands: vec![(5, command)],
                term: 1,
            },
            header: HEADER,
            delay: Duration::ZERO,
        })
        .unwrap();
    let completion = tokio::time::timeout(Duration::from_secs(2), worker.step())
        .await
        .expect("the early evaluation held the worker")
        .unwrap()
        .unwrap();
    assert!(faults.entered.available_permits() > 0, "no read failed");
    assert!(matches!(completion.output, Output::Unprepared(5)));
    assert_eq!(completion.applied, 4);
    worker.submit(entries(vec![empty(5, 2)])).unwrap();
    let completion = tokio::time::timeout(Duration::from_secs(2), worker.step())
        .await
        .expect("the entry of the new leader waited")
        .unwrap()
        .unwrap();
    assert_eq!(completion.applied, 5);
    faults.set(0);
    worker.state_mut().close().await.unwrap();
}

/// A job that overlaps the applied index still fails, at its start and in its middle. In the
/// middle case a failed read retries the job first, so the retry must keep the index check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_overlapping_job_still_fails() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let state = cold(faults.clone()).await;
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(state, events, true, None);
    worker.submit(entries(vec![empty(4, 1)])).unwrap();
    let error = worker
        .step()
        .await
        .err()
        .expect("a leading overlap applied");
    assert!(format!("{error:#}").contains("index gap"), "{error:#}");
    // Entry 6 reads `k1` and fails once. The job then repeats index 5.
    worker
        .submit(entries(vec![empty(5, 1), txn_entry(6), empty(5, 1)]))
        .unwrap();
    faults.set(FAIL_SST_GET);
    let recover = recover_soon(&faults);
    let error = worker
        .step()
        .await
        .err()
        .expect("an overlap in the middle applied");
    recover.await.unwrap();
    assert!(faults.entered.available_permits() > 0, "no read failed");
    assert!(format!("{error:#}").contains("index gap"), "{error:#}");
    assert_eq!(worker.state_mut().applied, 6);
    worker.state_mut().close().await.unwrap();
}

/// A membership change reads the members row once. The size count takes the length of that
/// row, so no later read can fail after the change is evaluated. The counted size still
/// matches a full count.
#[tokio::test]
async fn a_membership_change_reads_its_row_once() {
    use crate::cluster::Command as C;
    let voter = |id: u64| {
        C::Add(crate::proto::etcdserverpb::Member {
            id,
            peer_ur_ls: vec![format!("http://127.0.0.1:{id}")],
            ..Default::default()
        })
    };
    let admin = crate::auth::Credentials::default();
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
    state.apply_cluster(1, &voter(7), &admin).await.unwrap();
    let reads = || crate::batch::SIZE_READS.with(std::cell::Cell::get);
    let before = reads();
    let outcome = state.apply_cluster(2, &voter(8), &admin).await.unwrap();
    outcome.result.unwrap();
    assert_eq!(reads(), before, "the size count read the members row again");
    assert!(state.members.members.contains_key(&8));
    let counted = crate::state::maintenance::count_logical_usage(state.applied_state())
        .await
        .unwrap();
    assert_eq!(state.logical_size(), counted.total());
    state.close().await.unwrap();
}

fn entry(index: u64, command: RaftCommand) -> Entry {
    Entry {
        index,
        term: 1,
        data: command.encode().unwrap(),
        ..Default::default()
    }
}

/// Run `job` while the store fails for 300 ms. Return its replies.
async fn retried(worker: &mut Worker, faults: &Arc<FaultStore>, job: Vec<Entry>) -> Completion {
    let failed = faults.entered.available_permits();
    worker.submit(entries(job)).unwrap();
    faults.set(FAIL_SST_GET);
    let recover = recover_soon(faults);
    let completion = worker.step().await.unwrap().unwrap();
    recover.await.unwrap();
    assert!(
        faults.entered.available_permits() > failed,
        "no read failed"
    );
    completion
}

/// A committed diagnostic Get and a compaction step read before they change state. A storage
/// error of the read leaves the entry unapplied, and apply runs it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_diagnostic_or_compaction_read_retries() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let mut state = cold(faults.clone()).await;
    let value = "d".repeat(8 << 10);
    let diagnostic = |index: u64, operation| {
        RaftCommand::Diagnostic(crate::Command {
            id: index.to_string(),
            request: crate::Request {
                operations: vec![operation],
            },
        })
    };
    let store = crate::Operation::Put {
        key: "d".into(),
        value: value.clone(),
    };
    let command = crate::Command {
        id: "5".into(),
        request: crate::Request {
            operations: vec![store],
        },
    };
    state.apply(5, Some(&command)).await.unwrap();
    // Two more versions of `k0`, so a physical compaction has history rows to remove.
    state.apply_etcd(6, &put("k0")).await.unwrap();
    state.apply_etcd(7, &put("k0")).await.unwrap();
    let compact = crate::proto::etcdserverpb::CompactionRequest {
        revision: state.revision,
        physical: true,
    };
    state
        .apply_compaction(8, &compact)
        .await
        .unwrap()
        .result
        .unwrap();
    let state = reopen_cold(state).await;
    assert!(state.compaction_pending());
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(state, events, true, None);
    let get = crate::Operation::Get { key: "d".into() };
    let completion = retried(&mut worker, &faults, vec![entry(9, diagnostic(9, get))]).await;
    let Output::Entries(replies) = completion.output else {
        panic!("expected entries");
    };
    let Reply::Encoded(reply) = &replies[0].reply else {
        panic!("expected a diagnostic reply");
    };
    assert_eq!(reply["values"][0], value.as_str());
    // A planned step read before the fault would serve the step. Plan it again.
    faults.set(FAIL_SST_GET);
    worker.state_mut().plan_compaction();
    let step = RaftCommand::CompactStep { id: "10".into() };
    let completion = retried(&mut worker, &faults, vec![entry(10, step)]).await;
    assert_eq!(completion.applied, 10);
    worker.state_mut().close().await.unwrap();
}

/// A Put entry with the reply ID `index`.
fn put_entry(index: u64, key: &str) -> Entry {
    let etcd = RaftCommand::Etcd {
        id: index.to_string(),
        origin: 1,
        etcd: put(key),
    };
    entry(index, etcd)
}

/// A diagnostic Get of `key`.
fn get_entry(index: u64, key: &str) -> Entry {
    let get = RaftCommand::Diagnostic(crate::Command {
        id: index.to_string(),
        request: crate::Request {
            operations: vec![crate::Operation::Get { key: key.into() }],
        },
    });
    entry(index, get)
}

/// Wait until apply holds on `index`, and then preempt it with a snapshot through `covers`.
fn preempt_when_held(worker: &Worker, index: u64, covers: u64) -> tokio::task::JoinHandle<()> {
    let (preempt, mut paused) = worker.control.handles();
    tokio::spawn(async move {
        let held = paused.wait_for(|paused| {
            paused
                .as_ref()
                .is_some_and(|paused| paused.held && paused.index == index)
        });
        tokio::time::timeout(Duration::from_secs(5), held)
            .await
            .expect("apply did not hold")
            .unwrap();
        // The hold checks the entry again and stays.
        tokio::time::sleep(Duration::from_millis(500)).await;
        preempt.send_replace(covers);
    })
}

/// Corrupt stored data holds apply on its entry instead of stopping it. The hold reports the
/// entry, and it checks the entry again. A snapshot that covers the entry ends the job with
/// its applied prefix, whose reply goes out once. The queued job behind it is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_data_holds_apply_until_a_snapshot_covers_it() {
    let mut state = cold(Arc::new(InMemory::new())).await;
    // The direct write takes one sequence number. A view pins the state at the applied index.
    state.write_raw(&[(b"data/bad", b"\xff\xfe")]);
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(state, events, true, None);
    worker
        .submit(entries(vec![put_entry(6, "k9"), get_entry(7, "bad")]))
        .unwrap();
    worker.submit(entries(vec![put_entry(8, "k8")])).unwrap();
    let preempt = preempt_when_held(&worker, 7, 9);
    let held = worker.step().await.unwrap().unwrap();
    preempt.await.unwrap();
    assert!(held.early);
    assert_eq!(held.applied, 6);
    let Output::Entries(replies) = held.output else {
        panic!("expected entries");
    };
    let ids: Vec<_> = replies.iter().map(|reply| reply.id.as_str()).collect();
    assert_eq!(ids, ["6"]);
    let dropped = worker.step().await.unwrap().unwrap();
    assert!(dropped.early);
    assert_eq!(dropped.applied, 6);
    assert!(matches!(dropped.output, Output::Entries(replies) if replies.is_empty()));
    assert_eq!(worker.control.paused(), None);
    worker.state_mut().close().await.unwrap();
}

/// A stored members row that does not decode is corrupt data. A membership change holds on
/// it instead of stopping apply.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_corrupt_members_row_holds_apply() {
    let mut state = cold(Arc::new(InMemory::new())).await;
    state.write_raw(&[(crate::cluster::KEY, b"{not a members row")]);
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(state, events, true, None);
    let change = RaftCommand::Cluster {
        id: "6".into(),
        request: crate::cluster::Command::Remove(9),
        credentials: Default::default(),
    };
    worker.submit(entries(vec![entry(6, change)])).unwrap();
    let preempt = preempt_when_held(&worker, 6, 6);
    let held = worker.step().await.unwrap().unwrap();
    preempt.await.unwrap();
    assert!(held.early);
    assert_eq!(held.applied, 5);
    worker.state_mut().close().await.unwrap();
}

/// How the checkpoint of `base_hold` fails to load.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BaseFault {
    /// Its SST is missing.
    Missing,
    /// Its members row does not decode.
    Corrupt,
}

/// A shared member at index 1 gets a snapshot of index 4 whose checkpoint does not load. Apply
/// holds instead of stopping, and it reports the hold. A newer snapshot ends the job, and the
/// state stays as it was.
async fn base_hold(fault: BaseFault) {
    use crate::state::shared::tests::{checkpoint, options, put, writer};
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let open = |cache| {
        let store: Arc<dyn ObjectStore> = faults.clone();
        async move {
            StateMachine::open_shared(store, "cluster", "test", cache, &options())
                .await
                .unwrap()
        }
    };
    let large = |key: &str| put(key, &"v".repeat(8 << 10));
    let mut ahead = open(Default::default()).await;
    let mut behind = open(COLD).await;
    for state in [&mut ahead, &mut behind] {
        state.entry_term = 4;
    }
    let writer = writer(&ahead).await;
    let mut written = 0;
    for n in 1..=4 {
        ahead.apply_etcd(n, &large(&format!("k{n}"))).await.unwrap();
    }
    let mut base = checkpoint(&ahead, &writer, &mut written).await;
    if fault == BaseFault::Corrupt {
        // The checkpoint keeps index 4, with a members row that does not decode.
        writer
            .put(crate::cluster::KEY, b"{not a members row")
            .await
            .unwrap();
        base = checkpoint(&ahead, &writer, &mut written).await;
        assert_eq!(base.applied, 4);
    }
    behind.apply_etcd(1, &large("k1")).await.unwrap();
    let id = base.checkpoint.unwrap();
    drop(base);
    let source = behind.shared.as_ref().unwrap().source.clone();
    source.receive(id).await.unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 4;
    snapshot.mut_metadata().term = 4;
    snapshot.data = crate::state::transfer::ViewDescriptor::new(id)
        .encode()
        .unwrap();
    let job = Job::Snapshot(snapshot);
    if fault == BaseFault::Missing {
        faults.set(crate::object::faults::LOSE_SST_TWICE);
    }
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(behind, events, true, None);
    worker
        .submit(Work {
            job,
            header: HEADER,
            delay: Duration::ZERO,
        })
        .unwrap();
    // A newer snapshot comes once apply holds on the entry after the checkpoint.
    let (preempt, mut paused) = worker.control.handles();
    let newer = tokio::spawn(async move {
        let held = paused.wait_for(|paused| {
            paused
                .as_ref()
                .is_some_and(|paused| paused.held && paused.index == 5)
        });
        tokio::time::timeout(Duration::from_secs(5), held)
            .await
            .expect("apply did not hold")
            .unwrap();
        preempt.send_replace(10);
    });
    let held = tokio::time::timeout(Duration::from_secs(10), worker.step())
        .await
        .expect("no snapshot ended the hold")
        .unwrap()
        .unwrap();
    newer.await.unwrap();
    assert!(held.early);
    assert_eq!(held.applied, 1);
    assert_eq!(worker.state().applied, 1);
    assert_eq!(worker.control.paused(), None);
    faults.set(0);
    worker.state_mut().close().await.unwrap();
    ahead.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_object_holds_an_install() {
    base_hold(BaseFault::Missing).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_data_holds_an_install() {
    base_hold(BaseFault::Corrupt).await;
}

/// A shared member at index 1 opens a base of index 4 that does not load. The adoption drops
/// the base instead of holding: the member keeps its base and overlay, and it applies the next
/// entry. A later base that loads is adopted.
async fn base_drop(fault: BaseFault) {
    use crate::state::shared::tests::{checkpoint, options, put, writer};
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let open = |cache| {
        let store: Arc<dyn ObjectStore> = faults.clone();
        async move {
            StateMachine::open_shared(store, "cluster", "test", cache, &options())
                .await
                .unwrap()
        }
    };
    let large = |key: &str| put(key, &"v".repeat(8 << 10));
    let mut ahead = open(Default::default()).await;
    let mut behind = open(COLD).await;
    for state in [&mut ahead, &mut behind] {
        state.entry_term = 4;
    }
    let writer = writer(&ahead).await;
    let mut written = 0;
    for n in 1..=4 {
        ahead.apply_etcd(n, &large(&format!("k{n}"))).await.unwrap();
    }
    let mut base = checkpoint(&ahead, &writer, &mut written).await;
    if fault == BaseFault::Corrupt {
        // The checkpoint keeps index 4, with a members row that does not decode.
        writer
            .put(crate::cluster::KEY, b"{not a members row")
            .await
            .unwrap();
        base = checkpoint(&ahead, &writer, &mut written).await;
    }
    behind.apply_etcd(1, &large("k1")).await.unwrap();
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(behind, events, true, None);
    let adopt = |base| Work {
        job: Job::AdoptBase { base, term: 4 },
        header: HEADER,
        delay: Duration::ZERO,
    };
    if fault == BaseFault::Missing {
        faults.set(crate::object::faults::LOSE_SST_TWICE);
    }
    worker.submit(adopt(base.clone())).unwrap();
    let dropped = tokio::time::timeout(Duration::from_secs(5), worker.step())
        .await
        .expect("the adoption waited")
        .unwrap()
        .unwrap();
    faults.set(0);
    assert!(dropped.early);
    assert_eq!(dropped.applied, 1);
    let Output::RejectedBase(rejected) = dropped.output else {
        panic!("the adoption did not drop its base");
    };
    assert_eq!(rejected.checkpoint, base.checkpoint);
    assert_eq!(rejected.index, 4);
    let class = match fault {
        BaseFault::Missing => crate::mvcc::storage_fault::StorageFault::Missing,
        BaseFault::Corrupt => crate::mvcc::storage_fault::StorageFault::Corrupt,
    };
    assert_eq!(rejected.fault, class);
    assert_eq!(worker.control.paused(), None);
    // The member keeps applying on its own base.
    worker.submit(entries(vec![put_entry(2, "k2")])).unwrap();
    assert_eq!(worker.step().await.unwrap().unwrap().applied, 2);
    if fault == BaseFault::Corrupt {
        // A newer checkpoint at the same index has no members row, so it loads.
        writer.delete(crate::cluster::KEY).await.unwrap();
        base = checkpoint(&ahead, &writer, &mut written).await;
    }
    worker.submit(adopt(base.clone())).unwrap();
    let adopted = worker.step().await.unwrap().unwrap();
    assert!(matches!(adopted.output, Output::Adopted(_)));
    assert_eq!(adopted.applied, 4);
    assert_eq!(worker.state().base_checkpoint(), base.checkpoint);
    drop(base);
    worker.state_mut().close().await.unwrap();
    ahead.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adoption_drops_a_base_with_a_missing_object() {
    base_drop(BaseFault::Missing).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adoption_drops_a_base_with_corrupt_data() {
    base_drop(BaseFault::Corrupt).await;
}

/// How `install_keeps_its_base` ends the install job.
#[derive(Clone, Copy, PartialEq, Eq)]
enum InstallEnd {
    /// A stale receipt replaces the receipt slot, and the manifests stop decoding. The retry
    /// still installs from the base that the job keeps.
    Installs,
    /// A newer snapshot that Raft accepted ends the job.
    Preempted,
    /// The worker stops.
    Stopped,
}

/// An install at 4 takes the base of its receipt, and its first load fails. The job keeps that
/// base across its tries, and it drops it when the job ends.
async fn install_keeps_its_base(end: InstallEnd) {
    use crate::object::faults::{CORRUPT_MANIFEST, LOSE_SST_TWICE};
    use crate::state::shared::tests::{checkpoint, options, put, writer};
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let open = |cache| {
        let store: Arc<dyn ObjectStore> = faults.clone();
        async move {
            StateMachine::open_shared(store, "cluster", "test", cache, &options())
                .await
                .unwrap()
        }
    };
    let large = |key: &str| put(key, &"v".repeat(8 << 10));
    let mut ahead = open(Default::default()).await;
    let mut behind = open(COLD).await;
    for state in [&mut ahead, &mut behind] {
        state.entry_term = 4;
    }
    let writer = writer(&ahead).await;
    let mut written = 0;
    for n in 1..=2 {
        ahead.apply_etcd(n, &large(&format!("k{n}"))).await.unwrap();
    }
    let stale = checkpoint(&ahead, &writer, &mut written)
        .await
        .checkpoint
        .unwrap();
    for n in 3..=4 {
        ahead.apply_etcd(n, &large(&format!("k{n}"))).await.unwrap();
    }
    let id = checkpoint(&ahead, &writer, &mut written)
        .await
        .checkpoint
        .unwrap();
    behind.apply_etcd(1, &large("k1")).await.unwrap();
    let source = behind.shared.as_ref().unwrap().source.clone();
    source.receive(id).await.unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 4;
    snapshot.mut_metadata().term = 4;
    snapshot.data = crate::state::transfer::ViewDescriptor::new(id)
        .encode()
        .unwrap();
    // The first load loses an SST, and the install holds. Later reads find it again.
    faults.set(LOSE_SST_TWICE);
    let (events, _) = mpsc::channel(8);
    let mut worker = Worker::start(behind, events, true, None);
    worker
        .submit(Work {
            job: Job::Snapshot(snapshot),
            header: HEADER,
            delay: Duration::ZERO,
        })
        .unwrap();
    let (preempt, mut paused) = worker.control.handles();
    let stopper = worker.control.stopper();
    let during = {
        let source = source.clone();
        let faults = faults.clone();
        tokio::spawn(async move {
            let held = paused.wait_for(|paused| {
                paused
                    .as_ref()
                    .is_some_and(|paused| paused.held && paused.index == 5)
            });
            tokio::time::timeout(Duration::from_secs(5), held)
                .await
                .expect("the install did not hold")
                .unwrap();
            match end {
                InstallEnd::Installs => {
                    // Raft rejects the stale snapshot later. Its receipt replaces the slot.
                    source.receive(stale).await.unwrap();
                    faults.set(CORRUPT_MANIFEST);
                }
                InstallEnd::Preempted => drop(preempt.send_replace(10)),
                InstallEnd::Stopped => drop(stopper.send_replace(true)),
            }
        })
    };
    let result = tokio::time::timeout(Duration::from_secs(10), worker.step())
        .await
        .expect("the install did not end");
    during.await.unwrap();
    faults.set(0);
    match end {
        InstallEnd::Installs => {
            let installed = result.unwrap().unwrap();
            assert!(!installed.early);
            assert_eq!(installed.applied, 4);
            assert_eq!(worker.state().base_checkpoint(), Some(id));
        }
        InstallEnd::Preempted => {
            let ended = result.unwrap().unwrap();
            assert!(ended.early);
            assert_eq!(ended.applied, 1);
            assert!(!source.pinned().contains(&id), "the job kept its base");
        }
        InstallEnd::Stopped => {
            assert!(result.is_err(), "the stopped install did not fail");
            assert!(!source.pinned().contains(&id), "the job kept its base");
        }
    }
    worker.state_mut().close().await.unwrap();
    ahead.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_install_keeps_its_base_when_a_stale_receipt_comes() {
    install_keeps_its_base(InstallEnd::Installs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_newer_snapshot_releases_the_base_of_an_install() {
    install_keeps_its_base(InstallEnd::Preempted).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_releases_the_base_of_an_install() {
    install_keeps_its_base(InstallEnd::Stopped).await;
}
