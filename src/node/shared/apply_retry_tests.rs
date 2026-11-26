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
