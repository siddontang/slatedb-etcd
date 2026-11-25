use super::*;
use crate::auth::{Command as Auth, Credentials};
use crate::lease::LeaseCommand as Lease;
use crate::proto::etcdserverpb::request_op::Request as Op;
use crate::proto::etcdserverpb::*;

async fn check(state: &StateMachine) {
    assert_eq!(
        state.logical_usage(),
        maintenance::count_logical_usage(state.db().as_ref())
            .await
            .unwrap()
    );
    assert!(state.key_index.matches(state.db()).await.unwrap());
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
async fn quota_tracks_final_mutations_and_failed_storage() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
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
    let mut batch = WriteBatch::new();
    batch.put(b"etcd/test", b"must fail");
    assert!(state.write_batch(batch, 4).await.is_err());
    assert_eq!(state.logical_bytes.total(), 0);
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
        super::compaction_tests::finish(&mut state).await;
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
    snapshot.set_data(state.snapshot_data().await.unwrap());
    target.install_snapshot(&snapshot).await.unwrap();
    check(&target).await;
    assert_eq!(target.logical_bytes, state.logical_bytes);
    target.close().await.unwrap();
    state.db().flush().await.unwrap();
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
    let error = StateMachine::restore_backup(
        &path,
        &dir.path().join("restore"),
        1,
        "test",
        &members,
        &crate::object::Config::Local,
        StateCache::default(),
    )
    .await
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
    state.db().flush().await.unwrap();
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
    state.db().flush().await.unwrap();
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
async fn scans_keep_new_sst_metadata_after_blocks_fill_the_cache() {
    use crate::object::faults::{FaultStore, FAIL_SST_GET};
    use slatedb::object_store::memory::InMemory;
    const BUDGET: u64 = 1024 * 1024;
    let store = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    // A single shared cache with twice this budget fails this test.
    let cache = StateCache {
        block_bytes: BUDGET,
        meta_bytes: BUDGET,
        row_bytes: BUDGET,
    };
    let mut state = StateMachine::open_store_with_cache(store.clone(), "meta", cache)
        .await
        .unwrap();
    // Each SST index weighs about 50 data blocks, but fits easily in either budget.
    let key = |set: u8, i: usize| format!("{}/{i:0>100}", set as char).into_bytes();
    let range = |set: u8, end: usize, limit: i64| RequestOp {
        request: Some(Op::RequestRange(RangeRequest {
            key: key(set, 0),
            range_end: key(set, end),
            limit,
            ..Default::default()
        })),
    };
    let mut index = 0;
    for set in [b'a', b'b'] {
        for chunk in 0..if set == b'a' { 64 } else { 32 } {
            let requests: Vec<_> = (0..256)
                .map(|i| put(&key(set, chunk * 256 + i), b"value", 0))
                .collect();
            let puts: Vec<_> = requests
                .iter()
                .map(|request| {
                    index += 1;
                    put_batch::KvEntry {
                        index,
                        request,
                        credentials: None,
                        origin: 1,
                    }
                })
                .collect();
            state.apply_puts(&puts).await.unwrap();
        }
        state.db().flush().await.unwrap();
        if set == b'a' {
            // Count every row twice. Set a's blocks now fill the block budget.
            for _ in 0..2 {
                let view = state.read_view().unwrap();
                view.read(&range(b'a', 16384, 1), None)
                    .await
                    .unwrap()
                    .result
                    .unwrap();
            }
        }
    }
    // Set b's SST and its index arrive while the cache is full of set a's blocks.
    let view = state.read_view().unwrap();
    let small = range(b'b', 10, 0);
    let expected = view.read(&small, None).await.unwrap().result.unwrap();
    // Moka decides admission in its maintenance runs. Let several runs happen between reads.
    for _ in 0..5 {
        for _ in 0..4 {
            assert_eq!(
                view.read(&small, None).await.unwrap().result.unwrap(),
                expected
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    }
    store.set(FAIL_SST_GET);
    let cached = view.read(&small, None).await.unwrap().result;
    store.set(0);
    assert_eq!(cached.unwrap(), expected);
    drop(view);
    state.close().await.unwrap();
}

#[tokio::test]
async fn append_accounting_keeps_original_absence_through_batch_rewrites() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
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

/// Apply `count` Puts of about 70 logical bytes each. Return the failed ones.
async fn put_many(state: &mut StateMachine, count: u64) -> usize {
    let mut failed = 0;
    for _ in 0..count {
        let index = state.applied + 1;
        let request = put(format!("key-{index}").as_bytes(), &[b'v'; 16], 0);
        let outcome = state.apply_etcd_quota(index, &request, 1).await.unwrap();
        failed += usize::from(outcome.result.is_err());
    }
    failed
}

/// A request of `member` at `requested_at`. Zero time marks a seed.
fn request(bytes: i64, requested_at: u64, member: u64) -> quota::ClusterQuota {
    quota::ClusterQuota {
        bytes,
        requested_at,
        member,
    }
}

/// The value of the cluster quota row.
fn cluster_bytes(state: &StateMachine) -> Option<i64> {
    state.cluster_quota().map(|quota| quota.bytes)
}

async fn same_state(left: &StateMachine, right: &StateMachine) {
    assert_eq!(left.applied, right.applied);
    assert_eq!(left.revision, right.revision);
    assert_eq!(left.alarms.len(), right.alarms.len());
    assert_eq!(left.quota, right.quota);
    assert_eq!(left.cluster_quota(), right.cluster_quota());
    assert_eq!(left.logical_size(), right.logical_size());
    check(left).await;
    check(right).await;
}

/// Members with different flags apply the same entries alike after a `SetQuota` entry.
#[tokio::test]
async fn members_with_different_flags_apply_one_cluster_quota() {
    let dir = tempfile::tempdir().unwrap();
    let mut small = StateMachine::open(&dir.path().join("small")).await.unwrap();
    let mut large = StateMachine::open(&dir.path().join("large")).await.unwrap();
    small.set_quota_flag(300);
    large.set_quota_flag(1 << 20);
    let mut failed = vec![];
    for state in [&mut small, &mut large] {
        assert_eq!(state.cluster_quota(), None);
        state
            .apply_set_quota(1, request(2000, 0, 1), true)
            .await
            .unwrap();
        assert_eq!((state.quota, state.quota_bytes()), (2000, 2000));
        // A later seed changes nothing. A leader with an old view proposes it.
        state
            .apply_set_quota(2, request(9, 0, 2), true)
            .await
            .unwrap();
        assert_eq!(cluster_bytes(state), Some(2000));
        failed.push(put_many(state, 40).await);
        assert_eq!(state.alarms.len(), 1);
    }
    assert_eq!(failed[0], failed[1]);
    assert!(failed[0] > 0 && failed[0] < 40, "{failed:?}");
    assert!(small.quota_mismatch() && large.quota_mismatch());
    same_state(&small, &large).await;
    small.close().await.unwrap();
    large.close().await.unwrap();
}

/// A non-positive cluster quota disables the check, whatever the flag of the member.
#[tokio::test]
async fn a_non_positive_cluster_quota_disables_the_check() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    state.set_quota_flag(100);
    state
        .apply_set_quota(1, request(-1, 5, 1), false)
        .await
        .unwrap();
    assert_eq!((state.quota, state.quota_bytes()), (0, -1));
    assert_eq!(put_many(&mut state, 10).await, 0);
    assert!(state.alarms.is_empty());
    // The row counts toward the logical size, like every `etcd/` row.
    check(&state).await;
    state.snapshot_data().await.unwrap();
    state.close().await.unwrap();
    let mut state = StateMachine::open(dir.path()).await.unwrap();
    assert_eq!(state.cluster_quota(), Some(request(-1, 5, 1)));
    state.set_quota_flag(100);
    assert_eq!(state.quota, 0);
    state.close().await.unwrap();
}

/// A Raft snapshot image and a native backup carry the cluster quota. Entries above the
/// installed index apply under it, not under the flag of the new member.
#[tokio::test]
async fn images_and_backups_carry_the_cluster_quota() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = StateMachine::open(&dir.path().join("source"))
        .await
        .unwrap();
    source.set_quota_flag(1 << 20);
    source
        .apply_set_quota(1, request(2000, 5, 1), false)
        .await
        .unwrap();
    assert_eq!(put_many(&mut source, 10).await, 0);
    let image = source.snapshot_data().await.unwrap();
    let data: SnapshotData = crate::codec::decode(&image).unwrap();
    super::tests::check_format_4(&data);
    assert!(data.rows.iter().any(|(key, _)| key == quota::KEY));

    let mut follower = StateMachine::open(&dir.path().join("follower"))
        .await
        .unwrap();
    follower.set_quota_flag(1 << 20);
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = source.applied;
    snapshot.mut_metadata().term = 1;
    snapshot.set_data(image.clone());
    follower.install_snapshot(&snapshot).await.unwrap();
    assert_eq!(follower.cluster_quota(), Some(request(2000, 5, 1)));
    assert_eq!(follower.quota, 2000);

    let backup = super::tests::backup_bytes(image);
    super::tests::check_format_4_backup(&backup);
    let path = dir.path().join("restore");
    std::fs::create_dir(&path).unwrap();
    let mut restored = super::tests::restore(&path, backup).await;
    restored.set_quota_flag(1 << 20);
    assert_eq!(restored.cluster_quota(), Some(request(2000, 5, 1)));
    assert_eq!(restored.quota, 2000);

    let failed = put_many(&mut source, 30).await;
    assert!(failed > 0);
    for state in [&mut follower, &mut restored] {
        assert_eq!(put_many(state, 30).await, failed);
        assert_eq!(state.revision, source.revision);
        assert_eq!(state.alarms.len(), source.alarms.len());
        check(state).await;
    }
    for state in [source, follower, restored] {
        state.close().await.unwrap();
    }
}

/// A leader that is behind on apply can append an older request after a newer one commits.
/// Every member keeps the newer value. Equal times compare the member ID, then the value.
#[tokio::test]
async fn an_older_request_does_not_replace_a_newer_cluster_quota() {
    let dir = tempfile::tempdir().unwrap();
    let mut members = vec![];
    for (name, flag) in [("one", 100), ("two", 1 << 20)] {
        let mut state = StateMachine::open(&dir.path().join(name)).await.unwrap();
        state.set_quota_flag(flag);
        members.push(state);
    }
    let entries = [
        (request(300, 50, 2), false, 300),
        (request(200, 40, 1), false, 300),
        (request(9, 0, 3), true, 300),
        (request(250, 50, 1), false, 300),
        (request(260, 50, 3), false, 260),
        (request(270, 50, 3), false, 270),
    ];
    for state in &mut members {
        for (index, (quota, seed, expected)) in (1..).zip(entries) {
            state.apply_set_quota(index, quota, seed).await.unwrap();
            assert_eq!(cluster_bytes(state), Some(expected), "entry {index}");
        }
        assert_eq!(state.applied, 6);
        check(state).await;
    }
    same_state(&members[0], &members[1]).await;
    for state in members {
        state.close().await.unwrap();
    }
}
