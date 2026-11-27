//! Restart and install of a shared base whose Raft record is above the checkpoint row.

use super::*;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

/// A member installed checkpoint X at row 2 with a Raft record at 5, in a later term, and
/// truncated its log through 5. A Defragment then made checkpoint Y at the same row, so Y
/// is the newest. The restart opens X by the ID in the record, at 5 with the record term,
/// before `align_log` checks the log. Entry 6 then applies once. A fresh member installs
/// the same record.
#[tokio::test]
async fn a_restart_opens_the_record_checkpoint_above_its_row() {
    use crate::state::shared::Record;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let open = |record: Option<Record>| {
        let store = store.clone();
        async move {
            let options = crate::state::shared::Options {
                quota: 0,
                voters: vec![1],
                join: false,
                empty_log: false,
                pin_max: Duration::from_secs(300),
                overlay_max_bytes: 1 << 30,
                restore: None,
                record,
                store_id: None,
            };
            StateMachine::open_shared(
                store,
                "root",
                "test",
                crate::state::StateCache::default(),
                &options,
            )
            .await
        }
    };
    let put = |index: u64, key: &str| Entry {
        index,
        term: 2,
        data: RaftCommand::Etcd {
            id: index.to_string(),
            origin: 1,
            etcd: crate::proto::etcdserverpb::RequestOp {
                request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                    crate::proto::etcdserverpb::PutRequest {
                        key: key.as_bytes().to_vec(),
                        value: b"v".to_vec(),
                        ..Default::default()
                    },
                )),
            },
        }
        .encode()
        .unwrap(),
        ..Default::default()
    };
    let header = apply::Header {
        cluster: 0,
        member: 1,
        term: 2,
    };
    let mut leader = open(None).await.unwrap();
    leader.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    leader.entry_term = 1;
    leader.seed_for_test(1).await.unwrap();
    let mut first = put(2, "a");
    first.term = 1;
    apply::apply_entries(&mut leader, vec![first], header)
        .await
        .unwrap();
    let handles = leader.shared_handles().unwrap();
    let db = handles.source.open_writer().await.unwrap();
    for (seq, batch) in handles.queue.after(0, usize::MAX) {
        db.write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
            .await
            .unwrap();
    }
    db.flush().await.unwrap();
    let options = CheckpointOptions {
        name: Some(view_name(2)),
        ..Default::default()
    };
    let x = db
        .create_checkpoint(CheckpointScope::Durable, &options)
        .await
        .unwrap()
        .id;
    let y = db
        .create_checkpoint(CheckpointScope::Durable, &options)
        .await
        .unwrap()
        .id;
    db.close().await.unwrap();
    handles.source.mark_bootstrapped(x).await.unwrap();
    // An election, then entries 3 to 5 that change no state.
    leader.entry_term = 2;
    for index in 3..=5 {
        leader.apply(index, None).await.unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let mut storage = RaftStorage::open(dir.path(), 1, "test", vec![1]).unwrap();
    let mut snapshot = raft::eraftpb::Snapshot::default();
    snapshot.mut_metadata().index = 5;
    snapshot.mut_metadata().term = 2;
    snapshot
        .mut_metadata()
        .set_conf_state(leader.members.conf());
    snapshot.data = crate::state::transfer::ViewDescriptor::new(x)
        .encode()
        .unwrap();
    storage.adopt(&snapshot).unwrap();
    let hs = raft::eraftpb::HardState {
        term: 2,
        vote: 1,
        commit: 6,
    };
    storage.persist(&[put(6, "b")], Some(&hs), None).unwrap();

    // Without the record, the newest checkpoint Y starts the member at 2, below the log.
    let plain = open(None).await.unwrap();
    assert_eq!(plain.base_checkpoint(), Some(y));
    assert_eq!(plain.applied, 2);
    assert!(Node::align_log(&mut storage, &plain).is_err());
    plain.close().await.unwrap();
    // A record at the checkpoint row extends nothing.
    let at_row = Record {
        checkpoint: x,
        index: 2,
        term: 1,
        row: None,
    };
    let upgraded = open(Some(at_row)).await.unwrap();
    assert_eq!((upgraded.applied, upgraded.base_checkpoint()), (2, Some(y)));
    upgraded.close().await.unwrap();

    let record = Record::of(storage.recovery_snapshot()).unwrap().unwrap();
    assert_eq!(record.index, 5);
    let mut member = open(Some(record)).await.unwrap();
    assert_eq!(member.base_checkpoint(), Some(x));
    assert_eq!((member.applied, member.applied_term), (5, 2));
    assert_eq!(member.view_status().unwrap().0, 5);
    Node::align_log(&mut storage, &member).unwrap();
    member
        .install_snapshot(storage.recovery_snapshot())
        .await
        .unwrap();
    assert_eq!(member.applied, 5);
    let entries = storage
        .entries(6, 7, None, raft::GetEntriesContext::empty(false))
        .unwrap();
    apply::apply_entries(&mut member, entries.clone(), header)
        .await
        .unwrap();
    apply::apply_entries(&mut leader, entries, header)
        .await
        .unwrap();
    assert_eq!((member.applied, member.revision), (6, leader.revision));
    assert_eq!(leader.revision, 3);

    // A member on Y at 2 installs the record at 5.
    let mut fresh = open(None).await.unwrap();
    assert_eq!(fresh.applied, 2);
    fresh
        .install_snapshot(storage.recovery_snapshot())
        .await
        .unwrap();
    assert_eq!((fresh.applied, fresh.applied_term), (5, 2));
    assert_eq!(fresh.base_checkpoint(), Some(x));
    assert_eq!(fresh.revision, 2);
    for state in [leader, member, fresh] {
        state.close().await.unwrap();
    }
}

/// The shared state of `retired`: checkpoint Z at row 1, then X and Y at row 2, and a leader
/// that applied entries 3 to 5 in term 2. W has row 3: the leader stored a batch at 3 and
/// published it.
struct Retired {
    store: Arc<dyn ObjectStore>,
    leader: StateMachine,
    x: uuid::Uuid,
    y: uuid::Uuid,
    w: uuid::Uuid,
}

async fn open_at(
    store: &Arc<dyn ObjectStore>,
    record: Option<crate::state::shared::Record>,
) -> Result<StateMachine> {
    let options = crate::state::shared::Options {
        quota: 0,
        voters: vec![1],
        join: false,
        empty_log: false,
        pin_max: Duration::from_secs(300),
        overlay_max_bytes: 1 << 30,
        restore: None,
        record,
        store_id: None,
    };
    StateMachine::open_shared(
        store.clone(),
        "root",
        "test",
        crate::state::StateCache::default(),
        &options,
    )
    .await
}

async fn retired() -> Retired {
    retired_in(Arc::new(InMemory::new())).await
}

async fn retired_in(store: Arc<dyn ObjectStore>) -> Retired {
    let mut leader = open_at(&store, None).await.unwrap();
    leader.members = Arc::new(crate::cluster::Store::initial(
        &[(1, "127.0.0.1:1".to_owned())].into(),
    ));
    leader.entry_term = 1;
    leader.seed_for_test(1).await.unwrap();
    let handles = leader.shared_handles().unwrap();
    let db = handles.source.open_writer().await.unwrap();
    let checkpoint = |row| CheckpointOptions {
        name: Some(view_name(row)),
        ..Default::default()
    };
    let mut written = 0;
    let mut materialize = async |leader: &StateMachine| {
        let queue = leader.shared_handles().unwrap().queue;
        for (seq, batch) in queue.after(written, usize::MAX) {
            db.write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
                .await
                .unwrap();
            written = seq;
        }
        db.flush().await.unwrap();
    };
    materialize(&leader).await;
    db.create_checkpoint(CheckpointScope::Durable, &checkpoint(1))
        .await
        .unwrap();
    handles
        .source
        .mark_bootstrapped(uuid::Uuid::nil())
        .await
        .unwrap();
    let put = crate::proto::etcdserverpb::RequestOp {
        request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
            crate::proto::etcdserverpb::PutRequest {
                key: b"a".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            },
        )),
    };
    leader.apply_etcd(2, &put).await.unwrap();
    materialize(&leader).await;
    let x = db
        .create_checkpoint(CheckpointScope::Durable, &checkpoint(2))
        .await
        .unwrap()
        .id;
    let y = db
        .create_checkpoint(CheckpointScope::Durable, &checkpoint(2))
        .await
        .unwrap()
        .id;
    leader.entry_term = 2;
    // A batch at 3 puts the row of the next checkpoint there.
    let row = crate::Command {
        id: "row".into(),
        request: crate::Request {
            operations: vec![crate::Operation::Put {
                key: "row".into(),
                value: "3".into(),
            }],
        },
    };
    leader.apply(3, Some(&row)).await.unwrap();
    materialize(&leader).await;
    let w = db
        .create_checkpoint(CheckpointScope::Durable, &checkpoint(3))
        .await
        .unwrap()
        .id;
    db.close().await.unwrap();
    for index in 4..=5 {
        leader.apply(index, None).await.unwrap();
    }
    Retired {
        store,
        leader,
        x,
        y,
        w,
    }
}

/// A record of X at 5 that names its row 2.
fn record_of(x: uuid::Uuid) -> crate::state::shared::Record {
    crate::state::shared::Record {
        checkpoint: x,
        index: 5,
        term: 2,
        row: Some(2),
    }
}

/// Delete the manifest of checkpoint `id`. The checkpoint stays listed, but it cannot open, as
/// when retention deletes it after the listing.
async fn lose_manifest(
    store: &Arc<dyn ObjectStore>,
    admin: &slatedb::admin::Admin,
    id: uuid::Uuid,
) {
    let views = crate::state::shared::view_checkpoints(admin).await.unwrap();
    let manifest = views
        .iter()
        .find(|(_, c)| c.id == id)
        .unwrap()
        .1
        .manifest_id;
    let path = format!("root/shared/state/manifest/{manifest:020}.manifest");
    let path = slatedb::object_store::path::Path::from(path);
    slatedb::object_store::ObjectStoreExt::delete(store.as_ref(), &path)
        .await
        .unwrap();
}

/// Retention deleted X, the checkpoint of a record at 5 above its row 2. A restart loads W, the
/// newest checkpoint of the same shared state with its row in [2, 5], at row 3, and raises it
/// to 5. If W is gone too, the restart skips it and loads Y at row 2. If only Z at row 1 is
/// left, below the row of X, the restart fails closed with an error that says how to recover.
/// A record that names no row needs X itself.
#[tokio::test]
async fn a_restart_without_the_record_checkpoint_uses_one_in_its_range() {
    let Retired {
        store,
        leader,
        x,
        y,
        w,
    } = retired().await;
    let admin = leader.admin.clone();
    admin.delete_checkpoint(x).await.unwrap();
    let mut member = open_at(&store, Some(record_of(x))).await.unwrap();
    assert_eq!(member.base_checkpoint(), Some(w));
    assert_eq!(member.shared_handles().unwrap().opened.newest(), None);
    assert_eq!((member.applied, member.applied_term), (5, 2));
    assert_eq!(member.extensions(), 1);
    assert_eq!(member.loaded_fields(), leader.loaded_fields());
    member.apply_etcd(6, &put_b()).await.unwrap();
    assert_eq!(member.revision, 3);
    member.close().await.unwrap();
    lose_manifest(&store, &admin, w).await;
    let member = open_at(&store, Some(record_of(x))).await.unwrap();
    assert_eq!(member.base_checkpoint(), Some(y));
    assert_eq!((member.applied, member.applied_term), (5, 2));
    assert_eq!(member.loaded_fields(), leader.loaded_fields());
    member.close().await.unwrap();
    let exact = crate::state::shared::Record {
        row: None,
        ..record_of(x)
    };
    let error = open_at(&store, Some(exact)).await.err().unwrap();
    assert!(
        format!("{error:#}").contains("no other view checkpoint"),
        "{error:#}"
    );
    admin.delete_checkpoint(y).await.unwrap();
    let error = open_at(&store, Some(record_of(x))).await.err().unwrap();
    let message = format!("{error:#}");
    assert!(message.contains("has a row from 2 to 5"), "{message}");
    assert!(message.contains("empty data directory"), "{message}");
    leader.close().await.unwrap();
}

/// The same for a rebase: the member's base X at 5 above its row 2 lost an object, and
/// retention deleted X. The rebase loads W in [2, 5] and raises it to 5 with the log term. Then
/// W is gone too. The base row is now 3, and Y at row 2 is below it, so the rebase fails
/// closed.
#[tokio::test]
async fn a_rebase_without_its_base_checkpoint_uses_one_in_its_range() {
    let Retired {
        store,
        leader,
        x,
        y,
        w,
    } = retired().await;
    let admin = leader.admin.clone();
    let mut member = open_at(&store, Some(record_of(x))).await.unwrap();
    assert_eq!((member.base_checkpoint(), member.applied), (Some(x), 5));
    admin.delete_checkpoint(x).await.unwrap();
    member.rebase(5, |index| Ok(index - 3)).await.unwrap();
    assert_eq!(member.base_checkpoint(), Some(w));
    assert_eq!((member.applied, member.applied_term), (5, 2));
    assert_eq!(member.loaded_fields(), leader.loaded_fields());
    lose_manifest(&store, &admin, w).await;
    let error = member.rebase(5, |index| Ok(index - 3)).await.err().unwrap();
    assert!(
        format!("{error:#}").contains("has a row from 3 to 5"),
        "{error:#}"
    );
    assert_ne!(member.base_checkpoint(), Some(y));
    leader.close().await.unwrap();
}

/// Only a checkpoint that is gone is skipped. Another storage error stops the search, so a
/// transient error never selects another base.
#[tokio::test]
async fn only_a_gone_checkpoint_is_skipped() {
    use crate::object::faults::{FaultStore, FAIL_GET};
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let Retired {
        store,
        leader,
        x,
        w,
        ..
    } = retired_in(faults.clone()).await;
    let source = leader.shared_handles().unwrap().source;
    let admin = leader.admin.clone();
    admin.delete_checkpoint(x).await.unwrap();
    assert!(crate::state::shared::open_present(&source, x)
        .await
        .unwrap()
        .is_none());
    lose_manifest(&store, &admin, w).await;
    assert!(crate::state::shared::open_present(&source, w)
        .await
        .unwrap()
        .is_none());
    let y = crate::state::shared::view_checkpoints(&admin)
        .await
        .unwrap();
    let y = y.iter().rev().find(|(seq, _)| *seq == 2).unwrap().1.id;
    faults.set(FAIL_GET);
    let error = crate::state::shared::open_present(&source, y).await.err();
    assert!(error.is_some_and(|error| !crate::state::shared::gone(&error)));
    faults.set(0);
    leader.close().await.unwrap();
}

fn put_b() -> crate::proto::etcdserverpb::RequestOp {
    crate::proto::etcdserverpb::RequestOp {
        request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
            crate::proto::etcdserverpb::PutRequest {
                key: b"b".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            },
        )),
    }
}
