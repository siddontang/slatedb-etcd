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

/// A replicated state on `store` with `k0` to `k3` through index 4. The rows are in SSTs, and
/// the state opens again with empty caches.
async fn cold(store: Arc<dyn ObjectStore>) -> StateMachine {
    let open = || StateMachine::open_store_with_cache(store.clone(), "root", COLD);
    let mut state = open().await.unwrap();
    for index in 1..=4 {
        let key = format!("k{}", index - 1);
        state.apply_etcd(index, &put(&key)).await.unwrap();
    }
    state.metrics.flush_db(state.db()).await.unwrap();
    state.close().await.unwrap();
    open().await.unwrap()
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

/// An error of the member's own invariants still stops apply. A storage error of a read and a
/// missing object are the only errors that apply runs again.
#[tokio::test]
async fn an_invariant_error_still_stops_apply() {
    let dir = tempfile::tempdir().unwrap();
    let state = StateMachine::open(dir.path()).await.unwrap();
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
    assert_eq!(retryable(&corrupt), None);
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
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    state.apply_cluster(1, &voter(7), &admin).await.unwrap();
    let reads = || crate::batch::SIZE_READS.with(std::cell::Cell::get);
    let before = reads();
    let outcome = state.apply_cluster(2, &voter(8), &admin).await.unwrap();
    outcome.result.unwrap();
    assert_eq!(reads(), before, "the size count read the members row again");
    assert!(state.members.members.contains_key(&8));
    let counted = crate::state::maintenance::count_logical_usage(state.db().as_ref())
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
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    state.db().flush_with_options(memtable).await.unwrap();
    state.close().await.unwrap();
    let state = StateMachine::open_store_with_cache(faults.clone(), "root", COLD)
        .await
        .unwrap();
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
