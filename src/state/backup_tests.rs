use super::backup::{Stream, RESTORE_MARKER};
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
