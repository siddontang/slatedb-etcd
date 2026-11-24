//! The lease key sets of the key index and the stored logical size counter.

use super::*;
use crate::lease::{LeaseCommand, LeaseReply};
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::*;
use crate::proto::mvccpb::KeyValue;
use prost::Message;

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

fn keep_lease(key: &[u8], value: &[u8]) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestPut(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ignore_lease: true,
            ..Default::default()
        })),
    }
}

fn delete(key: &[u8], end: &[u8]) -> RequestOp {
    RequestOp {
        request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
            key: key.to_vec(),
            range_end: end.to_vec(),
            ..Default::default()
        })),
    }
}

fn txn(success: Vec<RequestOp>, failure: Vec<RequestOp>, key: &[u8], exists: bool) -> RequestOp {
    // `version > 0` holds for an existing key.
    let compare = Compare {
        key: key.to_vec(),
        target: 0,
        result: if exists { 1 } else { 0 },
        target_union: Some(compare::TargetUnion::Version(0)),
        ..Default::default()
    };
    RequestOp {
        request: Some(Op::RequestTxn(TxnRequest {
            compare: vec![compare],
            success,
            failure,
        })),
    }
}

async fn lease(state: &mut StateMachine, command: LeaseCommand) -> Option<LeaseReply> {
    let index = state.applied + 1;
    state
        .apply_lease(index, &command)
        .await
        .unwrap()
        .result
        .ok()
}

async fn etcd(state: &mut StateMachine, request: &RequestOp) {
    let index = state.applied + 1;
    // Semantic errors, such as a put to a missing lease, still advance the apply index.
    let _ = state.apply_etcd(index, request).await.unwrap();
}

/// Check the lease sets against a rebuild from storage and a full scan for every lease.
async fn check(state: &StateMachine, leases: &[i64]) {
    assert!(state.key_index.matches(state.db()).await.unwrap());
    for &id in leases {
        let scanned = crate::mvcc::scan_lease_keys(state.db().as_ref(), id)
            .await
            .unwrap();
        assert_eq!(state.key_index.lease_keys(id), scanned, "lease {id}");
    }
    assert_eq!(
        state.logical_bytes,
        maintenance::count_logical_size(state.db().as_ref())
            .await
            .unwrap()
    );
}

async fn ttl_keys(state: &mut StateMachine, id: i64) -> Vec<Vec<u8>> {
    match lease(state, LeaseCommand::TimeToLive { id, keys: true }).await {
        Some(LeaseReply::TimeToLive(reply)) => reply.keys,
        other => panic!("unexpected reply {other:?}"),
    }
}

/// A small deterministic generator. Tests must not depend on a seedless source.
struct Rng(u64);

impl Rng {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

#[tokio::test]
async fn lease_sets_match_a_rebuild_after_random_operations_restart_and_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    let ids = [1_i64, 2, 3, 4];
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let key = |n: u64| format!("k{n:02}").into_bytes();
    for step in 0..600 {
        let k = key(rng.next(24));
        let id = ids[rng.next(ids.len() as u64) as usize];
        // Lease zero means no lease. Some puts name a lease that does not exist.
        let any_lease = [0, 0, id, id, 99][rng.next(5) as usize];
        match rng.next(10) {
            0..=2 => etcd(&mut state, &put(&k, b"v", any_lease)).await,
            3 => etcd(&mut state, &keep_lease(&k, b"kept")).await,
            4 => etcd(&mut state, &delete(&k, b"")).await,
            5 => etcd(&mut state, &delete(&k, &key(rng.next(24) + 1))).await,
            6 => {
                let other = key(rng.next(24));
                let request = txn(
                    vec![put(&k, b"t", id), delete(&other, b"")],
                    vec![put(&other, b"f", any_lease), keep_lease(&k, b"f")],
                    &k,
                    true,
                );
                etcd(&mut state, &request).await;
            }
            7 => {
                lease(&mut state, LeaseCommand::Grant { id, ttl: 60 }).await;
            }
            8 => {
                lease(&mut state, LeaseCommand::Revoke { id }).await;
                assert!(state.key_index.lease_keys(id).is_empty());
            }
            _ => {
                let keys = ttl_keys(&mut state, id).await;
                if state.leases.contains_key(&id) {
                    assert_eq!(
                        keys,
                        crate::mvcc::scan_lease_keys(state.db().as_ref(), id)
                            .await
                            .unwrap()
                    );
                }
            }
        }
        if step % 25 == 0 {
            check(&state, &ids).await;
        }
    }
    check(&state, &ids).await;
    assert!(
        ids.iter()
            .any(|&id| !state.key_index.lease_keys(id).is_empty()),
        "the sequence must leave leased keys"
    );

    // A native snapshot replaces every row of a target with other leased keys.
    let mut target = StateMachine::open(other.path()).await.unwrap();
    lease(&mut target, LeaseCommand::Grant { id: 1, ttl: 60 }).await;
    etcd(&mut target, &put(b"stale", b"x", 1)).await;
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = state.applied;
    snapshot.set_data(state.snapshot_data().await.unwrap());
    target.install_snapshot(&snapshot).await.unwrap();
    check(&target, &ids).await;
    for id in ids {
        assert_eq!(
            target.key_index.lease_keys(id),
            state.key_index.lease_keys(id)
        );
    }
    target.close().await.unwrap();

    // A restart rebuilds the sets and reads the stored logical size.
    state.db().flush().await.unwrap();
    let expected: Vec<_> = ids
        .iter()
        .map(|&id| state.key_index.lease_keys(id))
        .collect();
    let size = state.logical_bytes;
    state.close().await.unwrap();
    drop(state);
    let state = StateMachine::open(dir.path()).await.unwrap();
    check(&state, &ids).await;
    let reopened: Vec<_> = ids
        .iter()
        .map(|&id| state.key_index.lease_keys(id))
        .collect();
    assert_eq!(reopened, expected);
    assert_eq!(state.logical_bytes, size);
    state.close().await.unwrap();
}

#[tokio::test]
async fn a_checkpoint_install_rebuilds_the_lease_sets_and_keeps_the_size() {
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let mut leader = StateMachine::open(leader_dir.path()).await.unwrap();
    leader.location.bind(1, "lease-index-test", None, None);
    lease(&mut leader, LeaseCommand::Grant { id: 5, ttl: 60 }).await;
    for n in 0..40 {
        let id = if n % 2 == 0 { 5 } else { 0 };
        etcd(&mut leader, &put(format!("key-{n}").as_bytes(), b"v", id)).await;
    }
    let data = leader
        .checkpoint_job(true)
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = transfer::Descriptor::decode(&data)
        .unwrap()
        .unwrap()
        .applied;
    snapshot.data = data;
    let mut follower = StateMachine::open(follower_dir.path()).await.unwrap();
    follower.location.bind(2, "lease-index-test", None, None);
    lease(&mut follower, LeaseCommand::Grant { id: 5, ttl: 60 }).await;
    etcd(&mut follower, &put(b"stale", b"x", 5)).await;
    follower.install_snapshot(&snapshot).await.unwrap();
    check(&follower, &[5]).await;
    assert_eq!(follower.key_index.lease_keys(5).len(), 20);
    assert_eq!(follower.logical_bytes, leader.logical_bytes);
    follower.close().await.unwrap();
    leader.close().await.unwrap();
}

#[tokio::test]
async fn revoking_a_large_lease_deletes_only_its_keys_in_one_revision() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    lease(&mut state, LeaseCommand::Grant { id: 7, ttl: 60 }).await;
    lease(&mut state, LeaseCommand::Grant { id: 8, ttl: 60 }).await;
    let count = 2_000;
    for n in 0..count {
        let id = if n % 4 == 0 { 8 } else { 7 };
        etcd(&mut state, &put(format!("key-{n:05}").as_bytes(), b"v", id)).await;
    }
    etcd(&mut state, &put(b"free", b"v", 0)).await;
    // Overwrite moves a key to the other lease. A plain put detaches a key.
    etcd(&mut state, &put(b"key-00001", b"moved", 8)).await;
    etcd(&mut state, &put(b"key-00002", b"detached", 0)).await;
    etcd(&mut state, &keep_lease(b"key-00003", b"same lease")).await;
    check(&state, &[7, 8]).await;
    let on_seven = state.key_index.lease_keys(7);
    assert_eq!(on_seven.len(), count * 3 / 4 - 2);
    assert_eq!(ttl_keys(&mut state, 7).await, on_seven);
    let before = state.revision;
    lease(&mut state, LeaseCommand::Revoke { id: 7 })
        .await
        .unwrap();
    assert_eq!(state.revision, before + 1);
    check(&state, &[7, 8]).await;
    assert!(state.key_index.lease_keys(7).is_empty());
    assert_eq!(state.key_index.lease_keys(8).len(), count / 4 + 1);
    let page = state
        .watch(
            &WatchCreateRequest {
                key: vec![0],
                range_end: vec![0],
                start_revision: before + 1,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    let deleted: Vec<_> = page
        .responses
        .iter()
        .flat_map(|response| &response.events)
        .map(|event| event.kv.as_ref().unwrap().key.clone())
        .collect();
    assert_eq!(deleted, on_seven);
    for key in [&b"free"[..], b"key-00001", b"key-00002", b"key-00004"] {
        assert!(state.key_index.get(key).is_some());
    }
    state.close().await.unwrap();
}

#[tokio::test]
async fn auth_checks_lease_keys_from_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    lease(&mut state, LeaseCommand::Grant { id: 3, ttl: 60 }).await;
    etcd(&mut state, &put(b"secret/a", b"v", 3)).await;
    etcd(&mut state, &put(b"public/a", b"v", 0)).await;
    let admin = crate::auth::Credentials::default();
    let user = |name: &str| crate::auth::Credentials {
        user: Some(name.into()),
        ..Default::default()
    };
    let add = |name: &str| {
        crate::auth::Command::UserAdd(AuthUserAddRequest {
            name: name.into(),
            options: Some(crate::proto::authpb::UserAddOptions { no_password: true }),
            ..Default::default()
        })
    };
    let grant = |user: &str, role: &str| {
        crate::auth::Command::UserGrantRole(AuthUserGrantRoleRequest {
            user: user.into(),
            role: role.into(),
        })
    };
    for command in [
        add("root"),
        grant("root", "root"),
        add("u"),
        crate::auth::Command::RoleAdd(AuthRoleAddRequest { name: "r".into() }),
        crate::auth::Command::RoleGrantPermission(AuthRoleGrantPermissionRequest {
            name: "r".into(),
            perm: Some(crate::proto::authpb::Permission {
                perm_type: 2,
                key: b"public/".to_vec(),
                range_end: b"public0".to_vec(),
            }),
        }),
        grant("u", "r"),
        crate::auth::Command::Enable(AuthEnableRequest {}),
    ] {
        let index = state.applied + 1;
        let credentials = if state.auth.enabled {
            user("root")
        } else {
            admin.clone()
        };
        state
            .apply_auth(index, &credentials, &command)
            .await
            .unwrap()
            .result
            .unwrap();
    }
    let user = user("u");
    // The lease holds a key outside the user's permission.
    assert!(state
        .authorize_kv(&user, &put(b"public/b", b"v", 3))
        .await
        .is_err());
    assert!(state
        .authorize_kv(&user, &put(b"public/b", b"v", 0))
        .await
        .is_ok());
    let revoke = crate::RaftCommand::Lease {
        id: "r".into(),
        lease: LeaseCommand::Revoke { id: 3 },
        origin: 1,
    };
    assert!(state.authorize(&user, &revoke).await.is_err());
    // Without the secret key, the lease holds only permitted keys.
    etcd(&mut state, &delete(b"secret/a", b"")).await;
    assert!(state
        .authorize_kv(&user, &put(b"public/b", b"v", 3))
        .await
        .is_ok());
    assert!(state.authorize(&user, &revoke).await.is_ok());
    state.close().await.unwrap();
}

#[tokio::test]
async fn the_logical_size_is_counted_when_the_counter_is_missing_or_stale() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    for n in 0..20 {
        etcd(&mut state, &put(format!("key-{n}").as_bytes(), b"value", 0)).await;
    }
    etcd(&mut state, &delete(b"key-1", b"key-2")).await;
    let size = state.logical_bytes;
    let stored = state.db().get(LOGICAL_SIZE).await.unwrap().unwrap();
    let counter = crate::proto::state::LogicalSize::decode(stored).unwrap();
    assert_eq!((counter.applied, counter.bytes), (state.applied, size));
    let applied = state.applied;

    // An older binary has no counter row.
    let options = slatedb::config::WriteOptions {
        seqnum: applied + 1,
    };
    let mut batch = slatedb::WriteBatch::new();
    batch.delete(LOGICAL_SIZE);
    batch.put(APPLIED, (applied + 1).to_be_bytes());
    state
        .db()
        .write_with_options(batch, &options)
        .await
        .unwrap();
    state.db().flush().await.unwrap();
    state.close().await.unwrap();
    drop(state);
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    assert_eq!(state.logical_bytes, size);

    // An older binary leaves a counter behind the apply marker. The value is wrong on purpose.
    etcd(&mut state, &put(b"new", b"value", 0)).await;
    let size = state.logical_bytes;
    let applied = state.applied;
    let options = slatedb::config::WriteOptions {
        seqnum: applied + 1,
    };
    let mut batch = slatedb::WriteBatch::new();
    batch.put(
        LOGICAL_SIZE,
        crate::proto::state::LogicalSize { applied, bytes: 1 }.encode_to_vec(),
    );
    batch.put(APPLIED, (applied + 1).to_be_bytes());
    state
        .db()
        .write_with_options(batch, &options)
        .await
        .unwrap();
    state.db().flush().await.unwrap();
    state.close().await.unwrap();
    drop(state);
    let state = StateMachine::open(dir.path()).await.unwrap();
    assert_eq!(state.logical_bytes, size);
    // The native snapshot leaves the counter out. Older receivers reject unknown keys.
    let data: SnapshotData =
        crate::codec::decode(&state.snapshot_job().unwrap().await.unwrap()).unwrap();
    assert!(data.rows.iter().all(|(key, _)| key != LOGICAL_SIZE));
    assert!(data
        .rows
        .iter()
        .any(|(key, value)| key.starts_with(crate::mvcc::CURRENT)
            && KeyValue::decode(value.as_slice()).is_ok()));
    // A snapshot from an older binary may hold a stale counter. The install ignores it.
    let mut data = data;
    data.rows.push((
        LOGICAL_SIZE.to_vec(),
        crate::proto::state::LogicalSize {
            applied: data.applied,
            bytes: 1,
        }
        .encode_to_vec(),
    ));
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = data.applied;
    snapshot.set_data(crate::codec::encode(&data).unwrap());
    let other = tempfile::tempdir().unwrap();
    let mut target = StateMachine::open(other.path()).await.unwrap();
    target.install_snapshot(&snapshot).await.unwrap();
    // The snapshot also adds the membership row, which the source has not stored.
    let installed = target.logical_bytes;
    assert_eq!(
        installed,
        maintenance::count_logical_size(target.db().as_ref())
            .await
            .unwrap()
    );
    target.db().flush().await.unwrap();
    target.close().await.unwrap();
    drop(target);
    let target = StateMachine::open(other.path()).await.unwrap();
    assert_eq!(target.logical_bytes, installed);
    target.close().await.unwrap();
    state.close().await.unwrap();
}
