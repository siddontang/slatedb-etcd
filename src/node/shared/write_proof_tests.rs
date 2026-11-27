//! The write probe of a member.

use super::*;
use crate::object::faults::{FaultStore, FAIL_STATE_DELETE, LOSE_STATE_PUT_REPLY, READ_ONLY_STATE};
use futures::TryStreamExt;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

/// A prober of member 2 on `faults`, and the scratch objects under its shared state.
fn prober(faults: &Arc<FaultStore>) -> Prober {
    let store: Arc<dyn ObjectStore> = faults.clone();
    let cache = crate::state::StateCache::default();
    let source = crate::state::shared::offline_source(store, "root", cache).unwrap();
    Prober::new(2, source)
}

async fn scratch(store: &dyn ObjectStore) -> Vec<String> {
    let prefix = Path::from("root/shared/state/.probe");
    let listed: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
    listed
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect()
}

/// A successful probe creates its scratch object in the shared state, with its member and
/// process in the name, and deletes it.
#[tokio::test]
async fn a_successful_probe_leaves_no_scratch_object() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    prober(&faults).probe(11).await.unwrap();
    let name = format!(
        "root/shared/state/.probe/2-{}-11",
        crate::peer::process_id()
    );
    assert!(faults.written.lock().unwrap().contains(&name));
    assert!(scratch(faults.as_ref()).await.is_empty());
}

/// A read-only member fails each probe at the create. Its refused delete of a scratch object
/// that does not exist does not hide that cause.
#[tokio::test]
async fn a_read_only_member_fails_each_probe_at_the_create() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    faults.set(READ_ONLY_STATE);
    let prober = prober(&faults);
    for nonce in 1..=3 {
        let error = prober.probe(nonce).await.unwrap_err();
        assert!(format!("{error:#}").contains("cannot create"), "{error:#}");
    }
    assert!(scratch(faults.as_ref()).await.is_empty());
}

/// A member that can create but not delete keeps at most one scratch object over many probes.
/// After the delete works again, the next probe removes it.
#[tokio::test]
async fn a_refused_delete_leaves_at_most_one_scratch_object() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    faults.set(FAIL_STATE_DELETE);
    let prober = prober(&faults);
    for nonce in 1..=5 {
        let error = prober.probe(nonce).await.unwrap_err();
        assert!(format!("{error:#}").contains("cannot delete"), "{error:#}");
    }
    assert_eq!(scratch(faults.as_ref()).await.len(), 1);
    faults.set(0);
    prober.probe(6).await.unwrap();
    assert!(scratch(faults.as_ref()).await.is_empty());
}

/// A create that reached the store but lost its reply leaves a scratch object. The next probe
/// deletes it before it creates its own.
#[tokio::test]
async fn a_lost_create_reply_is_cleaned_by_the_next_probe() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    faults.set(LOSE_STATE_PUT_REPLY);
    let prober = prober(&faults);
    prober.probe(1).await.unwrap_err();
    assert_eq!(scratch(faults.as_ref()).await.len(), 1);
    prober.probe(2).await.unwrap();
    assert!(scratch(faults.as_ref()).await.is_empty());
}
