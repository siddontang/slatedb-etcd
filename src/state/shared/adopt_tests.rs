//! A failed load of a new base leaves the current state, and an install keeps its base for a
//! retry.

use super::*;
use crate::object::faults::{FaultStore, FAIL_SST_GET};
use crate::state::shared::tests::{checkpoint, options, put, writer};
use crate::state::source::rows;
use slatedb::object_store::memory::InMemory;

/// No block, metadata, or row cache, so a load of a base reads the store.
const COLD: StateCache = StateCache {
    block_bytes: 1,
    meta_bytes: 1,
    row_bytes: 0,
};

/// A large value fills its own SST block, so a load of the base reads several blocks.
fn large(key: &str) -> crate::proto::etcdserverpb::RequestOp {
    put(key, &"v".repeat(8 << 10))
}

/// A read of the candidate base fails after the candidate opened. The member keeps its state,
/// its overlay batches, and its materializer queue, and it still serves reads. A retry adopts
/// the base.
#[tokio::test]
async fn a_failed_base_load_keeps_the_state() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let options = options();
    let open = |cache| {
        let store: Arc<dyn ObjectStore> = faults.clone();
        let options = &options;
        async move { StateMachine::open_shared(store, "cluster", "test", cache, options).await }
    };
    let mut ahead = open(StateCache::default()).await.unwrap();
    let mut behind = open(COLD).await.unwrap();
    for state in [&mut ahead, &mut behind] {
        state.entry_term = 4;
    }
    let writer = writer(&ahead).await;
    let mut written = 0;
    for n in 1..=4 {
        ahead.apply_etcd(n, &large(&format!("k{n}"))).await.unwrap();
    }
    let at_four = checkpoint(&ahead, &writer, &mut written).await;
    for n in 1..=2 {
        behind
            .apply_etcd(n, &large(&format!("k{n}")))
            .await
            .unwrap();
    }
    let shared = &behind.shared;
    let queue = shared.overlay.queue();
    let (pushed, batches, old_base) = (queue.pushed(), queue.len(), shared.base.checkpoint);
    assert!(batches > 0, "the member has no overlay batch");
    let before = rows(behind.applied_state(), b"etcd/").await;
    let candidate = shared
        .source
        .open_base(at_four.checkpoint.unwrap())
        .await
        .unwrap();
    faults.set(FAIL_SST_GET);
    let error = behind.adopt_opened(candidate.clone(), 4).await.unwrap_err();
    faults.set(0);
    assert!(faults.entered.available_permits() > 0, "no read failed");
    assert_eq!(
        crate::mvcc::storage_fault::classify(&error),
        Some(crate::mvcc::storage_fault::StorageFault::Transient),
        "{error:#}"
    );
    assert_eq!(behind.applied, 2);
    let shared = &behind.shared;
    assert_eq!(shared.base.checkpoint, old_base);
    assert_eq!((queue.pushed(), queue.len()), (pushed, batches));
    assert_eq!(rows(behind.applied_state(), b"etcd/").await, before);
    behind.adopt_opened(candidate, 4).await.unwrap();
    assert_eq!(behind.applied, 4);
    assert_eq!(
        rows(behind.applied_state(), b"etcd/").await,
        rows(ahead.applied_state(), b"etcd/").await
    );
    for state in [ahead, behind] {
        state.close().await.unwrap();
    }
    writer.close().await.unwrap();
}

/// The install of a received snapshot fails on a read of its base. The install keeps the base,
/// which pins the checkpoint, so retention cannot delete it before the retry. The retry installs
/// the snapshot, and then the install holds no base.
#[tokio::test]
async fn a_failed_install_keeps_its_base() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let options = options();
    let open = |cache| {
        let store: Arc<dyn ObjectStore> = faults.clone();
        let options = &options;
        async move {
            StateMachine::open_shared(store, "cluster", "test", cache, options)
                .await
                .unwrap()
        }
    };
    let mut ahead = open(StateCache::default()).await;
    let mut behind = open(COLD).await;
    for state in [&mut ahead, &mut behind] {
        state.entry_term = 4;
    }
    let writer = writer(&ahead).await;
    let mut written = 0;
    for n in 1..=4 {
        ahead.apply_etcd(n, &large(&format!("k{n}"))).await.unwrap();
    }
    let id = checkpoint(&ahead, &writer, &mut written)
        .await
        .checkpoint
        .unwrap();
    behind.apply_etcd(1, &large("k1")).await.unwrap();
    let source = behind.shared.source.clone();
    source.receive(id).await.unwrap();
    let mut snapshot = raft::eraftpb::Snapshot::default();
    snapshot.mut_metadata().index = 4;
    snapshot.mut_metadata().term = 4;
    snapshot.data = transfer::ViewDescriptor::new(id).encode().unwrap();
    let mut reader = None;
    faults.set(FAIL_SST_GET);
    let failed = behind.install_snapshot_with(&snapshot, &mut reader).await;
    faults.set(0);
    failed.unwrap_err();
    assert!(faults.entered.available_permits() > 0, "no read failed");
    assert_eq!(behind.applied, 1);
    assert!(reader.is_some(), "the failed install dropped its base");
    assert!(
        source.pinned().contains(&id),
        "the failed install lost the pin"
    );
    behind
        .install_snapshot_with(&snapshot, &mut reader)
        .await
        .unwrap();
    assert!(reader.is_none(), "the install kept its base");
    assert_eq!(behind.applied, 4);
    assert_eq!(behind.base_checkpoint(), Some(id));
    assert!(source.take_received(id).is_none(), "the receipt stayed");
    for state in [ahead, behind] {
        state.close().await.unwrap();
    }
    writer.close().await.unwrap();
}
