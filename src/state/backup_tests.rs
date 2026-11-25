use super::backup::{Stream, MAX_ROW_BYTES, RESTORE_MARKER};
use super::*;
use crate::{Command, Operation};
use sha2::{Digest, Sha256};

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

/// Collect a stream and check its chunk sizes against `remaining_bytes`.
async fn collect(mut stream: Stream) -> Vec<u8> {
    let mut data = vec![];
    let mut last = None;
    while let Some(chunk) = stream.chunks.recv().await {
        let chunk = chunk.unwrap();
        assert!(!chunk.blob.is_empty() && chunk.blob.len() <= 64 * 1024);
        if let Some(last) = last {
            assert_eq!(last, chunk.remaining + chunk.blob.len() as u64);
        }
        last = Some(chunk.remaining);
        data.extend_from_slice(&chunk.blob);
    }
    assert_eq!(last, Some(0));
    data
}

async fn rows(state: &StateMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = vec![];
    let mut scan = state.db().scan_prefix(b"", ..).await.unwrap();
    while let Some(row) = scan.next().await.unwrap() {
        // The image leaves out the derived meta rows, and writes the membership row last.
        if row.key.as_ref() != crate::cluster::KEY && !derived_key(&row.key) {
            rows.push((row.key.to_vec(), row.value.to_vec()));
        }
    }
    rows
}

fn members() -> crate::cluster::Store {
    crate::cluster::Store::initial(&std::collections::BTreeMap::from([(
        1,
        "http://127.0.0.1:2380".into(),
    )]))
}

async fn restore(path: &std::path::Path, dir: &std::path::Path) -> Result<()> {
    StateMachine::restore_backup(
        path,
        dir,
        1,
        "backup-test",
        &members(),
        &crate::object::Config::Local,
        StateCache::default(),
    )
    .await
}

async fn backup_checkpoints(state: &StateMachine) -> usize {
    let checkpoints = state.admin.list_checkpoints(None).await.unwrap();
    checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.name.as_deref() == Some("native-backup"))
        .count()
}

async fn released(state: &StateMachine) {
    for _ in 0..200 {
        if backup_checkpoints(state).await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the backup checkpoint was not deleted");
}

#[tokio::test]
async fn a_backup_streams_its_applied_boundary_while_writes_continue() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(&dir.path().join("state")).await.unwrap();
    let value = "v".repeat(2048);
    for index in 1..=300 {
        put(&mut state, index, &format!("key-{index}"), &value).await;
    }
    // etcd keys give the source a logical size.
    for index in 301..=320 {
        let key = format!("etcd-{index}");
        let request = super::quota_tests::put(key.as_bytes(), value.as_bytes(), 0);
        state
            .apply_etcd(index, &request)
            .await
            .unwrap()
            .result
            .unwrap();
    }
    assert!(state.logical_bytes > 0);
    let boundary = state.applied;
    let size = state.logical_bytes;
    let expected = rows(&state).await;
    let stream = state.backup().await.unwrap();
    assert_eq!(stream.header.unwrap().revision, state.revision);
    // Later writes overwrite and add rows while the image streams.
    for index in 321..=420 {
        put(&mut state, index, &format!("key-{}", index - 320), "new").await;
    }
    let data = collect(stream).await;
    assert_eq!(
        data.len() % 512,
        32,
        "etcdctl snapshot save checks this size"
    );
    let (payload, checksum) = data.split_at(data.len() - 32);
    assert_eq!(Sha256::digest(payload).as_slice(), checksum);
    let image: SnapshotData = crate::codec::decode(payload).unwrap();
    assert_eq!(image.version, crate::SNAPSHOT_FORMAT);
    assert_eq!(image.applied, boundary);
    let (last, copied) = image.rows.split_last().unwrap();
    assert_eq!(copied, expected.as_slice());
    assert_eq!(last.0, crate::cluster::KEY);
    let saved: crate::cluster::Store = serde_json::from_slice(&last.1).unwrap();
    assert_eq!(saved.members.len(), state.members.members.len());
    released(&state).await;

    let path = dir.path().join("native.snapshot");
    std::fs::write(&path, &data).unwrap();
    let target = dir.path().join("restored");
    restore(&path, &target).await.unwrap();
    assert!(!target.join(RESTORE_MARKER).exists());
    let copied = expected
        .iter()
        .filter(|(key, _)| key.as_slice() != APPLIED)
        .count() as u64;
    let storage =
        crate::storage::RaftStorage::open(&target.join("raft"), 1, "backup-test", vec![1]).unwrap();
    assert_eq!(storage.snapshot_index(), copied + 1);
    assert_eq!(storage.commit_index(), copied + 1);
    assert!(storage.recovery_snapshot().data.is_empty());
    drop(storage);
    let restored = StateMachine::open(&target.join("objects")).await.unwrap();
    assert_eq!(restored.applied, copied + 1);
    assert_eq!(
        restored.local_value("key-1").await.unwrap().as_deref(),
        Some(value.as_str())
    );
    assert_eq!(
        restored.local_value("key-300").await.unwrap().as_deref(),
        Some(value.as_str())
    );
    assert_eq!(restored.members.members.len(), 1);
    // The restored size is the source size at the boundary plus the new membership row, which
    // the source keeps only in memory.
    let members = crate::cluster::KEY.len() + serde_json::to_vec(&members()).unwrap().len();
    assert_eq!(restored.logical_bytes, size + members as u64);
    restored.close().await.unwrap();
    state.close().await.unwrap();
}

#[tokio::test]
async fn an_interrupted_backup_releases_its_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(&dir.path().join("state")).await.unwrap();
    let value = "v".repeat(16 * 1024);
    for index in 1..=200 {
        put(&mut state, index, &format!("key-{index}"), &value).await;
    }
    let mut stream = state.backup().await.unwrap();
    let first = stream.chunks.recv().await.unwrap().unwrap();
    assert!(first.remaining > 0);
    drop(stream);
    released(&state).await;

    // A truncated file fails its checksum and creates no directory.
    let data = collect(state.backup().await.unwrap()).await;
    let path = dir.path().join("partial.snapshot");
    std::fs::write(&path, &data[..data.len() / 2]).unwrap();
    let target = dir.path().join("restored");
    let error = restore(&path, &target).await.unwrap_err();
    assert!(error.to_string().contains("checksum mismatch"), "{error:#}");
    assert!(!target.exists());
    state.close().await.unwrap();
}

#[tokio::test]
async fn a_backup_stalled_in_an_object_read_ends_when_its_client_goes_away() {
    use crate::object::faults::{FaultStore, DELAY_SST_GET};
    let store = Arc::new(FaultStore::new(Arc::new(
        slatedb::object_store::memory::InMemory::new(),
    )));
    let mut state = StateMachine::open_store(store.clone(), "stalled")
        .await
        .unwrap();
    let value = "v".repeat(16 * 1024);
    for index in 1..=50 {
        put(&mut state, index, &format!("key-{index}"), &value).await;
    }
    state.db().flush().await.unwrap();
    // Every SST read of the stream now waits, like a read that keeps retrying.
    store.set_delay(Duration::from_secs(3600));
    store.set(DELAY_SST_GET);
    let mut stream = state.backup().await.unwrap();
    let stalled = tokio::time::timeout(Duration::from_millis(500), stream.chunks.recv()).await;
    assert!(stalled.is_err(), "the stream did not stall");
    drop(stream);
    released(&state).await;
    store.set(0);
    state.close().await.unwrap();
}

#[tokio::test]
async fn a_restore_reads_legacy_images_and_rejects_unordered_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(&dir.path().join("state")).await.unwrap();
    for index in 1..=20 {
        put(&mut state, index, &format!("key-{index}"), "legacy").await;
    }
    // Earlier binaries wrote the image without padding.
    let mut legacy = state.snapshot_data().await.unwrap();
    legacy.extend_from_slice(&Sha256::digest(&legacy));
    let path = dir.path().join("legacy.snapshot");
    std::fs::write(&path, &legacy).unwrap();
    let target = dir.path().join("legacy");
    restore(&path, &target).await.unwrap();
    let restored = StateMachine::open(&target.join("objects")).await.unwrap();
    assert_eq!(
        restored.local_value("key-20").await.unwrap().as_deref(),
        Some("legacy")
    );
    restored.close().await.unwrap();

    let mut unordered = crate::codec::encode(&SnapshotData {
        version: crate::SNAPSHOT_FORMAT,
        applied: 2,
        rows: vec![
            (b"etcd/b".to_vec(), b"1".to_vec()),
            (b"etcd/a".to_vec(), b"2".to_vec()),
        ],
    })
    .unwrap();
    unordered.extend_from_slice(&Sha256::digest(&unordered));
    std::fs::write(&path, &unordered).unwrap();
    let target = dir.path().join("unordered");
    let error = restore(&path, &target).await.unwrap_err();
    assert!(error.to_string().contains("unordered"), "{error:#}");
    assert!(!target.exists());
    state.close().await.unwrap();
}

#[tokio::test]
async fn a_restore_rejects_an_image_replaced_after_its_check() {
    let dir = tempfile::tempdir().unwrap();
    let image = |value: &[u8]| {
        let mut bytes = crate::codec::encode(&SnapshotData {
            version: crate::SNAPSHOT_FORMAT,
            applied: 2,
            rows: vec![
                (b"etcd/a".to_vec(), b"1".to_vec()),
                (b"etcd/b".to_vec(), value.to_vec()),
            ],
        })
        .unwrap();
        bytes.extend_from_slice(&Sha256::digest(&bytes));
        bytes
    };
    let path = dir.path().join("native.snapshot");
    std::fs::write(&path, image(b"checked")).unwrap();
    let checked = super::backup::check(&path).unwrap();
    // Between the passes, a valid image with as many rows replaces the checked file.
    std::fs::write(&path, image(b"replaced")).unwrap();
    let target = dir.path().join("restored");
    let error = StateMachine::restore_checked(
        &checked,
        &target,
        1,
        "backup-test",
        &members(),
        &crate::object::Config::Local,
        StateCache::default(),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("snapshot changed during restore"),
        "{error:#}"
    );
    // Startup rejects the directory of the failed restore.
    assert!(target.join(RESTORE_MARKER).exists());
}

#[tokio::test]
async fn a_paused_client_keeps_the_backup_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = StateMachine::open(&dir.path().join("state")).await.unwrap();
    let value = "v".repeat(16 * 1024);
    for index in 1..=200 {
        put(&mut state, index, &format!("key-{index}"), &value).await;
    }
    let lifetime = Duration::from_secs(2);
    let mut stream = state.backup_with_lifetime(lifetime).await.unwrap();
    let first = stream.chunks.recv().await.unwrap().unwrap();
    // The client stops reading while the image is larger than the chunks buffered ahead.
    tokio::time::sleep(lifetime * 3).await;
    let checkpoints = state
        .admin
        .list_checkpoints(Some("native-backup"))
        .await
        .unwrap();
    let expires = checkpoints
        .first()
        .and_then(|checkpoint| checkpoint.expire_time)
        .expect("the backup checkpoint is gone");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    assert!(
        expires.timestamp_millis() > now.as_millis() as i64,
        "the backup checkpoint expired"
    );
    // The client resumes and gets the whole image.
    let mut data = first.blob;
    data.extend_from_slice(&collect(stream).await);
    let (payload, checksum) = data.split_at(data.len() - 32);
    assert_eq!(Sha256::digest(payload).as_slice(), checksum);
    released(&state).await;
    state.close().await.unwrap();
}

/// Write a checksummed image of `rows` to `path`.
fn write_image(path: &std::path::Path, rows: Vec<(Vec<u8>, Vec<u8>)>) {
    let mut bytes = crate::codec::encode(&SnapshotData {
        version: crate::SNAPSHOT_FORMAT,
        applied: rows.len() as u64,
        rows,
    })
    .unwrap();
    bytes.extend_from_slice(&Sha256::digest(&bytes));
    std::fs::write(path, bytes).unwrap();
}

async fn restore_checked(
    path: &std::path::Path,
    dir: &std::path::Path,
    cluster: &str,
    objects: &crate::object::Config,
) -> Result<()> {
    let checked = super::backup::check(path)?;
    StateMachine::restore_checked(
        &checked,
        dir,
        1,
        cluster,
        &members(),
        objects,
        StateCache::default(),
    )
    .await
}

/// A backup accepts only rows that a restore decodes. The auth row holds every user and role,
/// so many small commands can grow it beyond one Raft entry.
#[tokio::test]
async fn a_backup_rejects_a_row_that_restore_cannot_read() {
    let dir = tempfile::tempdir().unwrap();
    // The largest accepted row restores.
    let key = b"etcd/large".to_vec();
    let value = vec![b'v'; MAX_ROW_BYTES - 16 - key.len()];
    assert_eq!(
        super::backup::row_size(&key, &value).unwrap(),
        MAX_ROW_BYTES as u64
    );
    let mut larger = value.clone();
    larger.push(b'v');
    let error = super::backup::row_size(&key, &larger).unwrap_err();
    let error = super::backup::backup_error(error, crate::mvcc::KvError::storage);
    assert_eq!(error.code, tonic::Code::ResourceExhausted as i32);
    let path = dir.path().join("largest.snapshot");
    write_image(&path, vec![(key, value)]);
    restore(&path, &dir.path().join("largest")).await.unwrap();

    // A backup of a state with a larger row fails before it sends the image.
    let mut state = StateMachine::open(&dir.path().join("state")).await.unwrap();
    let large = "v".repeat(MAX_ROW_BYTES);
    put(&mut state, 1, "large", &large).await;
    drop(large);
    let mut stream = state.backup().await.unwrap();
    let error = match stream.chunks.recv().await.unwrap() {
        Ok(_) => panic!("the backup sent a row that restore rejects"),
        Err(error) => error,
    };
    let error = super::backup::backup_error(error, crate::mvcc::KvError::storage);
    assert_eq!(
        error.code,
        tonic::Code::ResourceExhausted as i32,
        "{error:?}"
    );
    assert!(stream.chunks.recv().await.is_none());
    released(&state).await;
    state.close().await.unwrap();
}

/// A restore that passed the empty-directory check and then lost the directory to a started
/// member, or to another restore, leaves that directory unchanged.
#[tokio::test]
async fn a_restore_that_loses_its_directory_leaves_no_marker() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.snapshot");
    write_image(&path, vec![(b"etcd/a".to_vec(), b"1".to_vec())]);
    let target = dir.path().join("member");
    restore(&path, &target).await.unwrap();
    let objects = crate::object::Config::Local;
    // The restored member runs and holds the Raft directory lock.
    let running =
        crate::storage::RaftStorage::open(&target.join("raft"), 1, "backup-test", vec![1]).unwrap();
    assert!(restore_checked(&path, &target, "backup-test", &objects)
        .await
        .is_err());
    assert!(!target.join(RESTORE_MARKER).exists());
    drop(running);
    // The member stopped. Its directory is no longer empty.
    let error = restore_checked(&path, &target, "backup-test", &objects)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("empty data directory"),
        "{error:#}"
    );
    assert!(!target.join(RESTORE_MARKER).exists());
    let storage = crate::storage::RaftStorage::open_existing(&target.join("raft")).unwrap();
    assert_eq!(storage.snapshot_index(), 2);
    drop(storage);

    // Two restores race for one empty directory. The winner holds the lock, so the loser fails
    // without a marker, and the winner completes.
    let target = dir.path().join("race");
    std::fs::create_dir_all(&target).unwrap();
    let winner =
        crate::storage::RaftStorage::open(&target.join("raft"), 1, "backup-test", vec![1]).unwrap();
    assert!(restore_checked(&path, &target, "backup-test", &objects)
        .await
        .is_err());
    assert!(!target.join(RESTORE_MARKER).exists());
    drop(winner);
    restore_checked(&path, &target, "backup-test", &objects)
        .await
        .unwrap();
}

/// A restore writes its rows without the query indexes. Startup builds them from the rows.
#[tokio::test]
async fn a_restore_does_not_index_the_copied_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = StateMachine::open(&dir.path().join("source"))
        .await
        .unwrap();
    for index in 1..=2000 {
        let key = format!("key-{index}");
        let request = super::quota_tests::put(key.as_bytes(), b"small", 0);
        source
            .apply_etcd(index, &request)
            .await
            .unwrap()
            .result
            .unwrap();
    }
    assert_eq!(source.key_index.len(), 2000);
    let data = collect(source.backup().await.unwrap()).await;
    released(&source).await;
    let path = dir.path().join("native.snapshot");
    std::fs::write(&path, &data).unwrap();
    let checked = super::backup::check(&path).unwrap();
    let mut state = StateMachine::open(&dir.path().join("restore"))
        .await
        .unwrap();
    let index = state.restore_rows(&checked, &members()).await.unwrap();
    assert_eq!(state.key_index.len(), 0, "the copy built a key index");
    state.close().await.unwrap();
    let restored = StateMachine::open(&dir.path().join("restore"))
        .await
        .unwrap();
    assert_eq!(restored.applied, index);
    assert_eq!(restored.key_index.len(), source.key_index.len());
    assert!(restored
        .key_index
        .get_row(b"etcd/current/key-2000")
        .is_some());
    // The copy counted the logical size that startup reads.
    assert_eq!(
        restored.logical_bytes,
        super::maintenance::count_logical_size(restored.applied_state())
            .await
            .unwrap()
    );
    restored.close().await.unwrap();
    source.close().await.unwrap();
}

/// An interrupted restore with S3 objects keeps its claim on the member namespace. Removing the
/// local directory is not enough: the documented retry also deletes the namespace objects or
/// uses a new cluster ID.
#[tokio::test]
async fn an_interrupted_s3_restore_retries_after_its_namespace_is_cleared() {
    use futures::StreamExt;
    use slatedb::object_store::{ObjectStore, ObjectStoreExt};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.snapshot");
    let rows = |value: &[u8]| {
        vec![
            (b"etcd/a".to_vec(), b"1".to_vec()),
            (b"etcd/b".to_vec(), value.to_vec()),
        ]
    };
    let store = Arc::new(slatedb::object_store::memory::InMemory::new());
    let s3 = crate::object::S3 {
        bucket: "test-bucket".into(),
        region: "us-east-1".into(),
        endpoint: Some("http://127.0.0.1:9000".into()),
        prefix: "tests".into(),
        allow_http: true,
    };
    let objects = crate::object::Config::TestS3(store.clone(), s3.clone());
    // The image changes during the restore, after the member claimed its namespace.
    write_image(&path, rows(b"checked"));
    let checked = super::backup::check(&path).unwrap();
    write_image(&path, rows(b"replaced"));
    let target = dir.path().join("member");
    let error = StateMachine::restore_checked(
        &checked,
        &target,
        1,
        "backup-test",
        &members(),
        &objects,
        StateCache::default(),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("changed during restore"),
        "{error:#}"
    );
    assert!(target.join(RESTORE_MARKER).exists());
    let prefix = s3.member_prefix("backup-test", 1);
    let namespace = || async {
        store
            .list(Some(&prefix.as_str().into()))
            .map(|meta| meta.unwrap().location)
            .collect::<Vec<_>>()
            .await
    };
    assert!(!namespace().await.is_empty());

    // Removing the directory alone loses the namespace owner.
    std::fs::remove_dir_all(&target).unwrap();
    let error = restore_checked(&path, &target, "backup-test", &objects)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("belongs to another data directory"),
        "{error:#}"
    );
    // A new cluster ID uses an unused namespace.
    restore_checked(
        &path,
        &dir.path().join("new-cluster"),
        "backup-test-2",
        &objects,
    )
    .await
    .unwrap();
    // Or delete the namespace objects, then restore with the same settings.
    std::fs::remove_dir_all(&target).unwrap();
    for location in namespace().await {
        store.delete(&location).await.unwrap();
    }
    restore_checked(&path, &target, "backup-test", &objects)
        .await
        .unwrap();
    assert!(!target.join(RESTORE_MARKER).exists());
}
