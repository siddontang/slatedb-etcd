use std::sync::Arc;

use raft::eraftpb::Snapshot;
use slatedb::object_store::path::Path;
use slatedb::object_store::ObjectStoreExt;

use super::*;
use crate::state::{StateMachine, StateRead};
use crate::{Command, Operation};

async fn put(state: &mut StateMachine, index: u64, key: &str, value: &str) {
    let operations = vec![Operation::Put {
        key: key.to_owned(),
        value: value.to_owned(),
    }];
    state
        .apply(
            index,
            Some(&Command {
                id: index.to_string(),
                request: crate::Request { operations },
            }),
        )
        .await
        .unwrap();
}

async fn open(dir: &std::path::Path, member: u64) -> StateMachine {
    let state = StateMachine::open(dir).await.unwrap();
    state.location.bind(member, "transfer-test", None, None);
    state
}

async fn image(state: &StateMachine) -> Snapshot {
    let data = state
        .checkpoint_job(true)
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = Descriptor::decode(&data).unwrap().unwrap().applied;
    snapshot.data = data;
    snapshot
}

async fn exists(state: &StateMachine, path: &str) -> bool {
    state.location.store().head(&Path::from(path)).await.is_ok()
}

#[tokio::test]
async fn a_follower_installs_copied_checkpoint_objects() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader = open(leader_dir.path(), 1).await;
    leader.entry_term = 7;
    for index in 1..=50 {
        put(
            &mut leader,
            index,
            &format!("key-{index}"),
            &format!("value-{index}"),
        )
        .await;
    }
    let job = leader.checkpoint_job(true).await.unwrap();
    // Entries after the pinned index stay out of the checkpoint.
    put(&mut leader, 51, "key-1", "changed").await;
    let data = job.await.unwrap().unwrap();
    let descriptor = Descriptor::decode(&data).unwrap().unwrap();
    assert_eq!((descriptor.applied, descriptor.member), (50, 1));
    assert!(!descriptor.ssts.is_empty());
    assert!(sendable(&data, 1));
    assert!(!sendable(&data, 2));

    let mut follower = open(follower_dir.path(), 2).await;
    put(&mut follower, 1, "stale", "row").await;
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 50;
    snapshot.data = data;
    follower.install_snapshot(&snapshot).await.unwrap();
    // The copied checkpoint holds the term row of its last entry.
    assert_eq!((follower.applied, follower.applied_term), (50, 7));
    assert_eq!(follower.location.root(), "state-1");
    assert_eq!(
        follower.local_value("key-1").await.unwrap().as_deref(),
        Some("value-1")
    );
    assert_eq!(follower.local_value("stale").await.unwrap(), None);
    assert!(follower.members_unsaved);
    // The installed database accepts the next entry and stores the membership with it.
    put(&mut follower, 51, "key-1", "changed").await;
    assert!(!follower.members_unsaved);
    assert!(follower
        .db
        .get(crate::cluster::KEY)
        .await
        .unwrap()
        .is_some());
    // Raft replays unflushed entries after a crash. Flush to keep this test local.
    follower.db.flush().await.unwrap();
    follower.close().await.unwrap();
    drop(follower);

    // The pointer selects the installed database after a restart.
    let follower = open(follower_dir.path(), 2).await;
    assert_eq!(follower.location.root(), "state-1");
    assert_eq!(follower.applied, 51);
    assert_eq!(
        follower.local_value("key-1").await.unwrap().as_deref(),
        Some("changed")
    );
    // Startup deletes the replaced database. The leader namespace keeps its objects.
    assert!(!exists(&follower, "state/manifest/00000000000000000001.manifest").await);
    assert!(
        exists(&leader, &format!("state/{}", descriptor.ssts[0])).await,
        "the copy must not move leader objects"
    );
    follower.close().await.unwrap();
    leader.close().await.unwrap();
}

#[tokio::test]
async fn a_retry_keeps_copied_ssts_and_replaces_the_manifest() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader = open(leader_dir.path(), 1).await;
    for index in 1..=20 {
        put(&mut leader, index, &format!("key-{index}"), "first").await;
    }
    let first = image(&leader).await;
    let first_descriptor = Descriptor::decode(&first.data).unwrap().unwrap();
    for index in 21..=40 {
        put(&mut leader, index, &format!("key-{index}"), "second").await;
    }
    let second = image(&leader).await;

    let mut follower = open(follower_dir.path(), 2).await;
    // An earlier transfer copied an older checkpoint, but Raft did not accept it.
    assert_eq!(
        follower.location.prepare(&first_descriptor).await.unwrap(),
        "state-1"
    );
    assert!(exists(&follower, "state-1.ready").await);
    follower.install_snapshot(&second).await.unwrap();
    assert_eq!(follower.applied, 40);
    assert_eq!(
        follower.local_value("key-40").await.unwrap().as_deref(),
        Some("second")
    );
    let admin = slatedb::admin::Admin::builder("state-1", follower.location.store()).build();
    assert!(admin.list_checkpoints(None).await.unwrap().is_empty());
    follower.close().await.unwrap();
    leader.close().await.unwrap();
}

#[tokio::test]
async fn a_restart_restores_the_membership_of_an_installed_snapshot() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader = open(leader_dir.path(), 1).await;
    put(&mut leader, 1, "key", "value").await;
    // A bootstrap membership exists only in memory.
    let mut peers = std::collections::BTreeMap::new();
    peers.insert(1, "127.0.0.1:1".to_owned());
    peers.insert(2, "127.0.0.1:2".to_owned());
    leader.members = Arc::new(crate::cluster::Store::initial(&peers));
    // A member update changed the peer URL after startup. The applied membership wins.
    leader
        .location
        .bind(1, "transfer-test", Some("http://127.0.0.1:9".into()), None);
    let snapshot = image(&leader).await;
    let descriptor = Descriptor::decode(&snapshot.data).unwrap().unwrap();
    assert_eq!(descriptor.peer_url.as_deref(), Some("http://127.0.0.1:1"));
    let mut follower = open(follower_dir.path(), 2).await;
    follower.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(follower.members.members.len(), 2);
    follower.close().await.unwrap();
    drop(follower);
    let mut follower = open(follower_dir.path(), 2).await;
    assert!(follower.members.members.is_empty());
    // Node startup installs its recovery snapshot again. The index is already applied.
    follower.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(follower.members.members.len(), 2);
    assert!(follower.members_unsaved);
    follower.close().await.unwrap();
    leader.close().await.unwrap();
}

#[tokio::test]
async fn a_member_serves_only_checkpoint_objects_of_its_current_database() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = open(dir.path(), 1).await;
    put(&mut state, 1, "key", "value").await;
    let snapshot = image(&state).await;
    let descriptor = Descriptor::decode(&snapshot.data).unwrap().unwrap();
    let location = &state.location;
    assert!(location
        .serve("state", &descriptor.ssts[0])
        .await
        .unwrap()
        .is_some());
    assert!(location
        .serve("state", &descriptor.manifest_path())
        .await
        .unwrap()
        .is_some());
    for (root, path) in [
        ("state-1", descriptor.ssts[0].as_str()),
        ("state", "../object-store.json"),
        ("state", "compacted/../../x.sst"),
        ("state", "../state.current"),
    ] {
        assert!(location.serve(root, path).await.unwrap().is_none());
    }
    state.close().await.unwrap();
}

#[test]
fn only_fresh_own_descriptors_are_transferable() {
    let mut descriptor = Descriptor {
        version: OBJECT_SNAPSHOT_FORMAT,
        applied: 7,
        member: 3,
        namespace: "namespace".into(),
        backend: Backend::Local,
        peer_url: None,
        root: "state".into(),
        checkpoint: uuid::Uuid::new_v4().to_string(),
        manifest: 4,
        ssts: vec!["compacted/01ARZ3NDEKTSV4RRFFQ69G5FAV.sst".into()],
        members: vec![],
        created_ms: now_ms(),
    };
    assert!(sendable(&descriptor.encode().unwrap(), 3));
    assert!(!sendable(&descriptor.encode().unwrap(), 4));
    descriptor.created_ms -= IMAGE_REUSE.as_millis() as u64;
    assert!(!sendable(&descriptor.encode().unwrap(), 3));
    descriptor.ssts = vec!["compacted/../x.sst".into()];
    assert!(Descriptor::decode(&descriptor.encode().unwrap()).is_err());
    assert!(!sendable(&[], 3));
    // A row image of format 4 stays transferable.
    let rows = crate::codec::encode(&(crate::SNAPSHOT_FORMAT, 7_u64, 0_u64)).unwrap();
    assert!(sendable(&rows, 3));
    assert_eq!(generation("state", "state"), Some(0));
    assert_eq!(generation("state", "state-12"), Some(12));
    assert_eq!(generation("state", "state-01"), None);
    assert_eq!(generation("state", "state-+1"), None);
    assert_eq!(generation("state", "states"), None);
}

#[tokio::test]
async fn an_accepted_snapshot_keeps_its_staging_copy_when_a_newer_copy_starts() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader = open(leader_dir.path(), 1).await;
    for index in 1..=20 {
        put(&mut leader, index, &format!("key-{index}"), "first").await;
    }
    let accepted = image(&leader).await;
    let accepted_descriptor = Descriptor::decode(&accepted.data).unwrap().unwrap();
    for index in 21..=40 {
        put(&mut leader, index, &format!("key-{index}"), "second").await;
    }
    let newer = Descriptor::decode(&image(&leader).await.data)
        .unwrap()
        .unwrap();

    let mut follower = open(follower_dir.path(), 2).await;
    let staging = follower
        .location
        .prepare(&accepted_descriptor)
        .await
        .unwrap();
    // Raft accepts the first snapshot. A newer transfer then starts before the install.
    assert_eq!(
        follower
            .location
            .reserve(20, &accepted_descriptor.checkpoint),
        Some(true)
    );
    let other = follower.location.prepare(&newer).await.unwrap();
    assert_ne!(staging, other);
    assert!(exists(&follower, &format!("{staging}.ready")).await);

    // The install must not need the source again.
    leader.close().await.unwrap();
    drop(leader);
    follower.install_snapshot(&accepted).await.unwrap();
    assert_eq!(follower.applied, 20);
    assert_eq!(follower.location.root(), staging);
    assert_eq!(
        follower.local_value("key-20").await.unwrap().as_deref(),
        Some("first")
    );
    assert_eq!(follower.local_value("key-21").await.unwrap(), None);
    follower.close().await.unwrap();
}

#[tokio::test]
async fn cleanup_keeps_a_sibling_database_that_this_member_does_not_own() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader = open(leader_dir.path(), 1).await;
    put(&mut leader, 1, "key", "leader").await;
    let snapshot = image(&leader).await;

    let mut follower = open(follower_dir.path(), 2).await;
    // An independent database uses a name that looks like a generation of this family.
    let store = follower.location.store();
    let mut sibling = StateMachine::open_store(store.clone(), "state-5")
        .await
        .unwrap();
    put(&mut sibling, 1, "sibling", "kept").await;
    sibling.db.flush().await.unwrap();
    sibling.close().await.unwrap();
    drop(sibling);

    follower.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(follower.location.root(), "state-6");
    follower.close().await.unwrap();
    drop(follower);
    let follower = open(follower_dir.path(), 2).await;
    assert_eq!(follower.location.root(), "state-6");
    assert!(!exists(&follower, "state.owner").await);
    follower.close().await.unwrap();

    // The sibling opens as its own family and keeps its data.
    let sibling = StateMachine::open_store(store, "state-5").await.unwrap();
    assert_eq!(sibling.location.root(), "state-5");
    assert_eq!(
        sibling.local_value("sibling").await.unwrap().as_deref(),
        Some("kept")
    );
    sibling.close().await.unwrap();
    leader.close().await.unwrap();
}

#[tokio::test]
async fn a_read_view_keeps_the_replaced_database_open() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader = open(leader_dir.path(), 1).await;
    for index in 1..=5 {
        put(&mut leader, index, "key", "leader").await;
    }
    let snapshot = image(&leader).await;
    let mut follower = open(follower_dir.path(), 2).await;
    put(&mut follower, 1, "key", "follower").await;
    let view = follower.read_view().unwrap();
    follower.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(follower.location.root(), "state-1");

    // The retire task waits for the view. The pinned snapshot stays readable.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(view
        .state()
        .get(crate::state::APPLIED)
        .await
        .unwrap()
        .is_some());
    assert!(exists(&follower, "state.owner").await);
    drop(view);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while exists(&follower, "state.owner").await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "old database was not deleted"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    follower.close().await.unwrap();
    leader.close().await.unwrap();
}

#[tokio::test]
async fn a_staging_copy_that_a_new_copy_claims_cannot_be_reserved() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader = open(leader_dir.path(), 1).await;
    put(&mut leader, 1, "key", "first").await;
    let first = Descriptor::decode(&image(&leader).await.data)
        .unwrap()
        .unwrap();
    put(&mut leader, 2, "key", "second").await;
    let second = Descriptor::decode(&image(&leader).await.data)
        .unwrap()
        .unwrap();
    let follower = open(follower_dir.path(), 2).await;
    let staging = follower.location.prepare(&first).await.unwrap();
    // The newer copy claims the unreserved staging database of the first checkpoint.
    assert_eq!(follower.location.prepare(&second).await.unwrap(), staging);
    // Raft then cannot reserve the first checkpoint. The node copies it again instead.
    assert_eq!(follower.location.reserve(1, &first.checkpoint), None);
    assert_eq!(follower.location.reserve(2, &second.checkpoint), Some(true));
    assert_eq!(
        follower.location.reserve(2, &second.checkpoint),
        Some(false)
    );
    follower.close().await.unwrap();
    leader.close().await.unwrap();
}

#[tokio::test]
async fn a_copy_never_claims_a_database_that_an_install_made_current() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader = open(leader_dir.path(), 1).await;
    put(&mut leader, 1, "key", "first").await;
    let first = Descriptor::decode(&image(&leader).await.data)
        .unwrap()
        .unwrap();
    put(&mut leader, 2, "key", "second").await;
    let second = Descriptor::decode(&image(&leader).await.data)
        .unwrap()
        .unwrap();

    let follower = open(follower_dir.path(), 2).await;
    let location = follower.location.clone();
    let staging = location.prepare(&first).await.unwrap();
    assert_eq!(location.reserve(1, &first.checkpoint), Some(true));
    // The second copy scans while the first snapshot is still a reserved staging database.
    let reached = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *location.pause_after_scan.lock().unwrap() = Some((reached.clone(), resume.clone()));
    let copy = tokio::spawn({
        let location = location.clone();
        async move { location.prepare(&second).await }
    });
    reached.notified().await;
    // The install of the first snapshot finishes during the pause.
    location.switch(&staging, 1).await.unwrap();
    resume.notify_one();
    let other = copy.await.unwrap().unwrap();
    assert_ne!(other, staging, "the copy claimed the current database");
    follower.close().await.unwrap();
    drop(follower);
    drop(location);

    // The published files hold the first snapshot only.
    let follower = open(follower_dir.path(), 2).await;
    assert_eq!(follower.location.root(), staging);
    assert_eq!(follower.applied, 1);
    assert_eq!(
        follower.local_value("key").await.unwrap().as_deref(),
        Some("first")
    );
    follower.close().await.unwrap();
    leader.close().await.unwrap();
}
