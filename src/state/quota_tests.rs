use super::*;
use crate::auth::{Command as Auth, Credentials};
use crate::lease::LeaseCommand as Lease;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::*;

async fn check(state: &StateMachine) {
    assert_eq!(
        state.logical_size(),
        maintenance::count_logical_size(&state.db).await.unwrap()
    );
}

fn put(key: &[u8], value: &[u8], lease: i64) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestPut(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            lease,
            ..Default::default()
        })),
    }
}

#[tokio::test]
async fn quota_tracks_final_mutations_and_failed_storage() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"etcd/test", b"old value");
    batch.put(b"data/excluded", b"not counted");
    state.write_batch(batch, 1).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes, 18);
    let mut batch = WriteBatch::new();
    batch.put(b"etcd/test", b"temporary value");
    batch.delete(b"etcd/test");
    batch.put(b"etcd/test", b"x");
    batch.delete(b"etcd/absent");
    batch.delete(b"etcd/absent");
    state.write_batch(batch, 2).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes, 10);
    let mut batch = WriteBatch::new();
    batch.delete(b"etcd/test");
    state.write_batch(batch, 3).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes, 0);
    state.close().await.unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"etcd/test", b"must fail");
    assert!(state.write_batch(batch, 4).await.is_err());
    assert_eq!(state.logical_bytes, 0);
}

#[tokio::test]
async fn quota_matches_scan_across_state_changes_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    let credentials = Credentials::default();
    check(&state).await;
    for request in [put(b"a", b"long value", 0), put(b"a", b"x", 0)] {
        state
            .apply_etcd(state.applied + 1, &request)
            .await
            .unwrap()
            .result
            .unwrap();
        check(&state).await;
    }
    let transaction = RequestOp {
        request: Some(Op::RequestTxn(TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(Op::RequestTxn(TxnRequest {
                    success: vec![put(b"b", b"nested", 0)],
                    ..Default::default()
                })),
            }],
            failure: vec![],
        })),
    };
    state
        .apply_etcd(state.applied + 1, &transaction)
        .await
        .unwrap()
        .result
        .unwrap();
    check(&state).await;
    let size = state.logical_bytes;
    assert!(state
        .apply_etcd(state.applied + 1, &put(b"invalid", b"x", 999))
        .await
        .unwrap()
        .result
        .is_err());
    check(&state).await;
    assert_eq!(state.logical_bytes, size);
    for id in [7, 8] {
        state
            .apply_lease(state.applied + 1, &Lease::Grant { id, ttl: 60 })
            .await
            .unwrap()
            .result
            .unwrap();
        check(&state).await;
        state
            .apply_etcd(state.applied + 1, &put(&[id as u8], b"leased", id))
            .await
            .unwrap()
            .result
            .unwrap();
        check(&state).await;
        state
            .apply_lease(state.applied + 1, &Lease::KeepAlive { id })
            .await
            .unwrap()
            .result
            .unwrap();
        check(&state).await;
        let command = if id == 7 {
            Lease::Revoke { id }
        } else {
            Lease::Expire {
                id,
                generation: state.leases[&id].generation,
            }
        };
        state
            .apply_lease(state.applied + 1, &command)
            .await
            .unwrap()
            .result
            .unwrap();
        check(&state).await;
    }
    for command in [
        Auth::UserAdd(AuthUserAddRequest {
            name: "user".into(),
            options: Some(crate::proto::authpb::UserAddOptions { no_password: true }),
            ..Default::default()
        }),
        Auth::RoleAdd(AuthRoleAddRequest {
            name: "role".into(),
        }),
        Auth::RoleDelete(AuthRoleDeleteRequest {
            role: "role".into(),
        }),
        Auth::UserDelete(AuthUserDeleteRequest {
            name: "user".into(),
        }),
    ] {
        state
            .apply_auth(state.applied + 1, &credentials, &command)
            .await
            .unwrap()
            .result
            .unwrap();
        check(&state).await;
    }
    state
        .apply_cluster(
            state.applied + 1,
            &crate::cluster::Command::Add(Member {
                id: 1,
                peer_ur_ls: vec!["http://127.0.0.1:2380".into()],
                ..Default::default()
            }),
            &credentials,
        )
        .await
        .unwrap()
        .result
        .unwrap();
    check(&state).await;
    state
        .apply_cluster(
            state.applied + 1,
            &crate::cluster::Command::Publish {
                id: 1,
                client_urls: vec!["http://127.0.0.1:2379".into()],
            },
            &credentials,
        )
        .await
        .unwrap()
        .result
        .unwrap();
    check(&state).await;
    state.quota = 1;
    assert!(state
        .apply_etcd_quota(state.applied + 1, &put(b"full", b"x", 0), 1)
        .await
        .unwrap()
        .result
        .is_err());
    check(&state).await;
    state.quota = 0;
    state
        .apply_maintenance(
            state.applied + 1,
            &crate::maintenance::Command::Alarm(AlarmRequest {
                action: alarm_request::AlarmAction::Deactivate as i32,
                member_id: 0,
                alarm: 0,
            }),
            &credentials,
        )
        .await
        .unwrap()
        .result
        .unwrap();
    check(&state).await;
    state
        .apply_etcd(
            state.applied + 1,
            &RequestOp {
                request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
                    key: b"a".to_vec(),
                    ..Default::default()
                })),
            },
        )
        .await
        .unwrap()
        .result
        .unwrap();
    check(&state).await;
    for physical in [false, true] {
        state
            .apply_etcd(state.applied + 1, &put(b"b", b"compact", 0))
            .await
            .unwrap()
            .result
            .unwrap();
        state
            .apply_compaction(
                state.applied + 1,
                &CompactionRequest {
                    revision: state.revision,
                    physical,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        check(&state).await;
    }
    let mut target = StateMachine::open(other.path()).await.unwrap();
    target
        .apply_etcd(1, &put(b"removed-by-snapshot", b"old", 0))
        .await
        .unwrap()
        .result
        .unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = state.applied;
    snapshot.set_data(state.snapshot_data().await.unwrap().into());
    target.install_snapshot(&snapshot).await.unwrap();
    check(&target).await;
    assert_eq!(target.logical_bytes, state.logical_bytes);
    target.close().await.unwrap();
    state.db.flush().await.unwrap();
    let expected = state.logical_bytes;
    state.close().await.unwrap();
    drop(state);
    let state = StateMachine::open(dir.path()).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes, expected);
    state.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_import_requires_the_current_format() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(&dir.path().join("state")).await.unwrap();
    let data: SnapshotData = SnapshotData {
        version: 1,
        applied: 1,
        rows: vec![],
    };
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 1;
    let mut bytes = bincode::serialize(&data).unwrap();
    snapshot.set_data(bytes.clone().into());
    assert!(state.install_snapshot(&snapshot).await.is_err());
    assert_eq!(state.applied, 0);
    check(&state).await;
    bytes.extend_from_slice(&Sha256::digest(&bytes));
    let path = dir.path().join("backup");
    std::fs::write(&path, bytes).unwrap();
    let members = crate::cluster::Store::initial(&std::collections::BTreeMap::from([(
        1,
        "http://127.0.0.1:2380".into(),
    )]));
    let error =
        StateMachine::restore_backup(&path, &dir.path().join("restore"), 1, "test", &members)
            .unwrap_err();
    assert!(error.to_string().contains("unsupported snapshot version"));
    state.close().await.unwrap();
}

#[tokio::test]
async fn warm_reads_use_the_state_cache_and_observe_new_versions() {
    use crate::object::faults::{FaultStore, FAIL_SST_GET};
    use crate::proto::etcdserverpb::response_op::Response as Resp;
    use slatedb::object_store::memory::InMemory;
    let store = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let mut state = StateMachine::open_store(store.clone(), "cached")
        .await
        .unwrap();
    state
        .apply_etcd(1, &put(b"cached", b"first", 0))
        .await
        .unwrap()
        .result
        .unwrap();
    state.db.flush().await.unwrap();
    let request = RequestOp {
        request: Some(Op::RequestRange(RangeRequest {
            key: b"cached".to_vec(),
            ..Default::default()
        })),
    };
    let first = state.read_etcd(&request).await.unwrap().result.unwrap();
    store.set(FAIL_SST_GET);
    for _ in 0..3 {
        assert_eq!(
            state.read_etcd(&request).await.unwrap().result.unwrap(),
            first
        );
    }
    store.set(0);
    state
        .apply_etcd(2, &put(b"cached", b"second", 0))
        .await
        .unwrap()
        .result
        .unwrap();
    state.db.flush().await.unwrap();
    let Some(Resp::ResponseRange(response)) = state
        .read_etcd(&request)
        .await
        .unwrap()
        .result
        .unwrap()
        .response
    else {
        panic!("expected range response");
    };
    assert_eq!(response.kvs[0].value, b"second");
    check(&state).await;
    state.close().await.unwrap();
}

#[tokio::test]
async fn append_accounting_keeps_original_absence_through_batch_rewrites() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    let mut batch = WriteBatch::new();
    batch.put_new(b"etcd/new", b"temporary");
    batch.delete(b"etcd/new");
    batch.put(b"etcd/new", b"final");
    batch.put_new(b"etcd/deleted", b"gone");
    batch.delete(b"etcd/deleted");
    state.write_batch(batch, 1).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes, 13);
    let mut batch = WriteBatch::new();
    batch.put(b"etcd/new", b"replace");
    batch.delete(b"etcd/new");
    state.write_batch(batch, 2).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes, 0);
    state.close().await.unwrap();
}
