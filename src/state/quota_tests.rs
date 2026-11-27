use super::*;
use crate::auth::{Command as Auth, Credentials};
use crate::lease::LeaseCommand as Lease;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::*;

async fn check(state: &StateMachine) {
    assert_eq!(
        state.logical_usage(),
        maintenance::count_logical_usage(state.applied_state())
            .await
            .unwrap()
    );
    assert!(state
        .key_index
        .matches(state.applied_state())
        .await
        .unwrap());
}

pub(super) fn put(key: &[u8], value: &[u8], lease: i64) -> RequestOp {
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
async fn quota_tracks_final_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"etcd/test", b"old value");
    batch.put(b"data/excluded", b"not counted");
    state.write_batch(batch, 1).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes.total(), 18);
    let mut batch = WriteBatch::new();
    batch.put(b"etcd/test", b"temporary value");
    batch.delete(b"etcd/test");
    batch.put(b"etcd/test", b"x");
    batch.delete(b"etcd/absent");
    batch.delete(b"etcd/absent");
    state.write_batch(batch, 2).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes.total(), 10);
    let mut batch = WriteBatch::new();
    batch.delete(b"etcd/test");
    state.write_batch(batch, 3).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes.total(), 0);
    state.close().await.unwrap();
}

#[tokio::test]
async fn quota_matches_scan_across_state_changes() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
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
        super::compaction_tests::finish(&mut state).await;
        check(&state).await;
    }
    state.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_import_requires_the_current_format() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(&dir.path().join("state"))
        .await
        .unwrap();
    let data: SnapshotData = SnapshotData {
        version: 1,
        applied: 1,
        rows: vec![],
    };
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 1;
    let mut bytes = crate::codec::encode(&data).unwrap();
    snapshot.set_data(bytes.clone());
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
    let error = StateMachine::restore_shared(&path, 1, &members)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("unsupported snapshot version"));
    state.close().await.unwrap();
}

#[tokio::test]
async fn append_accounting_keeps_original_absence_through_batch_rewrites() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open_test(dir.path()).await.unwrap();
    let mut batch = WriteBatch::new();
    batch.put_new("etcd/new", "temporary");
    batch.delete(b"etcd/new");
    batch.put(b"etcd/new", b"final");
    batch.put_new("etcd/deleted", "gone");
    batch.delete(b"etcd/deleted");
    state.write_batch(batch, 1).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes.total(), 13);
    let mut batch = WriteBatch::new();
    batch.put(b"etcd/new", b"replace");
    batch.delete(b"etcd/new");
    state.write_batch(batch, 2).await.unwrap();
    check(&state).await;
    assert_eq!(state.logical_bytes.total(), 0);
    state.close().await.unwrap();
}
