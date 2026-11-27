use super::{write_image, MAX_ROW_BYTES};
use crate::state::*;
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

fn members() -> crate::cluster::Store {
    crate::cluster::Store::initial(&std::collections::BTreeMap::from([(
        1,
        "http://127.0.0.1:2380".into(),
    )]))
}

/// Check a native backup for a restore into a new cluster.
async fn restore(path: &std::path::Path) -> Result<()> {
    StateMachine::restore_shared(path, 1, &members())
        .await
        .map(drop)
}

/// A backup that fails to read storage, at its start or during the stream, returns
/// Unavailable, as a failed maintenance read does, so clients retry it. A shared backup reads
/// its pinned view before it returns the stream. Only a size limit returns ResourceExhausted.
#[test]
fn a_failed_backup_read_is_retryable() {
    let read = anyhow::Error::from(std::io::Error::other("an SST went away"));
    let error = super::backup_error(read);
    assert_eq!(error.code, tonic::Code::Unavailable as i32, "{error:?}");
    let error = super::backup_error(super::row_size(b"k", &vec![0; MAX_ROW_BYTES]).unwrap_err());
    assert_eq!(
        error.code,
        tonic::Code::ResourceExhausted as i32,
        "{error:?}"
    );
}

/// A backup accepts only rows that a restore decodes. The auth row holds every user and role,
/// so many small commands can grow it beyond one Raft entry.
#[tokio::test]
async fn a_backup_rejects_a_row_that_restore_cannot_read() {
    let dir = tempfile::tempdir().unwrap();
    // The largest accepted row restores.
    let key = b"etcd/large".to_vec();
    let value = vec![b'v'; MAX_ROW_BYTES - 16 - key.len()];
    assert_eq!(super::row_size(&key, &value).unwrap(), MAX_ROW_BYTES as u64);
    let mut larger = value.clone();
    larger.push(b'v');
    let error = super::row_size(&key, &larger).unwrap_err();
    let error = super::backup_error(error);
    assert_eq!(error.code, tonic::Code::ResourceExhausted as i32);
    let path = dir.path().join("largest.snapshot");
    write_image(&path, vec![(key, value)]);
    restore(&path).await.unwrap();

    // A backup of a state with a larger row fails before it sends the image.
    let mut state = StateMachine::open_test(&dir.path().join("state"))
        .await
        .unwrap();
    let large = "v".repeat(MAX_ROW_BYTES);
    put(&mut state, 1, "large", &large).await;
    drop(large);
    let mut stream = state.backup().await.unwrap();
    let error = match stream.recv().await.unwrap() {
        Ok(_) => panic!("the backup sent a row that restore rejects"),
        Err(error) => error,
    };
    let error = super::backup_error(error);
    assert_eq!(
        error.code,
        tonic::Code::ResourceExhausted as i32,
        "{error:?}"
    );
    assert!(stream.recv().await.is_none());
    state.close().await.unwrap();
}

/// `Server::start` restores on the runtime that serves other embedded members. The check reads
/// the whole image, so it runs in the blocking pool, and a timer keeps firing meanwhile.
#[tokio::test(flavor = "current_thread")]
async fn a_restore_check_does_not_stall_a_single_worker_runtime() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.snapshot");
    let rows = (0..256)
        .map(|n| (format!("etcd/{n:04}").into_bytes(), vec![b'v'; 64 * 1024]))
        .collect();
    write_image(&path, rows);
    // A wrong digest fails the check after its full pass, before the restore writes anything.
    let mut bytes = std::fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(&path, bytes).unwrap();
    let started = std::time::Instant::now();
    assert!(super::check(&path, super::replaced).is_err());
    let pass = started.elapsed();

    let gap = Arc::new(AtomicU64::new(0));
    let ticker = tokio::spawn({
        let gap = gap.clone();
        async move {
            let mut last = std::time::Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let now = std::time::Instant::now();
                gap.fetch_max((now - last).as_micros() as u64, Ordering::Relaxed);
                last = now;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let error = restore(&path).await.unwrap_err();
    assert!(error.to_string().contains("checksum mismatch"), "{error:#}");
    tokio::time::sleep(Duration::from_millis(5)).await;
    ticker.abort();
    let gap = Duration::from_micros(gap.load(Ordering::Relaxed));
    assert!(
        gap < pass / 4,
        "the timer paused {gap:?} for a {pass:?} check"
    );
}
