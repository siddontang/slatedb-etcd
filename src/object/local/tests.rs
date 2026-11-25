use super::*;
use futures::TryStreamExt;
use slatedb::object_store::{ObjectStoreExt, UpdateVersion};

fn update_to(e_tag: Option<String>) -> PutOptions {
    PutMode::Update(UpdateVersion {
        e_tag,
        version: None,
    })
    .into()
}

/// The names of the staging files in `dir`. The lock file `#0` is not one.
fn staged(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains('#') && !name.ends_with("#0"))
        .collect()
}

/// A runtime with one blocking thread, and a task that holds it until `release` sends.
/// Blocking work then waits, like work behind a hung filesystem call.
fn blocked_runtime() -> (tokio::runtime::Runtime, std::sync::mpsc::Sender<()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (release, held) = std::sync::mpsc::channel::<()>();
    runtime.spawn_blocking(move || held.recv());
    (runtime, release)
}

#[tokio::test]
async fn update_replaces_only_the_expected_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalStore::open(dir.path()).unwrap();
    let path = Path::from("gc/manifest.boundary");
    let missing = store.put_opts(&path, "1".into(), update_to(None)).await;
    assert!(
        matches!(missing, Err(Error::Precondition { .. })),
        "{missing:?}"
    );

    let created = store
        .put_opts(&path, "1".into(), PutMode::Create.into())
        .await
        .unwrap();
    let again = store
        .put_opts(&path, "2".into(), PutMode::Create.into())
        .await;
    assert!(
        matches!(again, Err(Error::AlreadyExists { .. })),
        "{again:?}"
    );

    // Equal sizes keep the e-tags apart only by inode and mtime.
    let mut e_tags = vec![created.e_tag.clone().unwrap()];
    for n in 2..10 {
        let previous = e_tags.last().cloned();
        let updated = store
            .put_opts(&path, n.to_string().into(), update_to(previous))
            .await
            .unwrap();
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.e_tag, updated.e_tag);
        assert!(!e_tags.contains(meta.e_tag.as_ref().unwrap()), "{e_tags:?}");
        e_tags.push(meta.e_tag.unwrap());
    }
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), b"9");

    let stale = store
        .put_opts(&path, "10".into(), update_to(created.e_tag))
        .await;
    assert!(
        matches!(stale, Err(Error::Precondition { .. })),
        "{stale:?}"
    );
    // The lock file and staging files stay out of listings.
    let listed: Vec<_> = store.list(None).try_collect().await.unwrap();
    let listed: Vec<_> = listed.into_iter().map(|meta| meta.location).collect();
    assert_eq!(listed, vec![path]);
}

/// Set the mtime rounded down to a whole second, like HFS+.
fn whole_seconds(file: &File, time: SystemTime) -> std::io::Result<()> {
    let seconds = time.duration_since(UNIX_EPOCH).unwrap().as_secs();
    file.set_modified(UNIX_EPOCH + Duration::from_secs(seconds))
}

/// Set nothing, like a filesystem that keeps one mtime.
fn ignore_mtime(_: &File, _: SystemTime) -> std::io::Result<()> {
    Ok(())
}

#[tokio::test]
async fn a_coarse_mtime_gets_a_larger_step_or_fails_the_update() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalStore::open(dir.path()).unwrap();
    let path = Path::from("boundary");
    let file = dir.path().join("boundary");
    store
        .put_opts(&path, "1".into(), PutMode::Create.into())
        .await
        .unwrap();
    // The current version has a whole-second mtime after the clock. A new file gets an
    // earlier mtime, and a 1 µs or 1 ms step rounds back to the current one.
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let ahead = UNIX_EPOCH + Duration::from_secs(now.as_secs() + 5);
    File::open(&file).unwrap().set_modified(ahead).unwrap();
    let current = store.head(&path).await.unwrap();

    store.set_mtime = ignore_mtime;
    let kept = store
        .put_opts(&path, "2".into(), update_to(current.e_tag.clone()))
        .await;
    let kept = kept.unwrap_err().to_string();
    assert!(kept.contains("keeps the mtime"), "{kept}");
    assert_eq!(store.head(&path).await.unwrap().e_tag, current.e_tag);
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), b"1");
    assert!(staged(dir.path()).is_empty(), "{:?}", staged(dir.path()));

    store.set_mtime = whole_seconds;
    let updated = store
        .put_opts(&path, "2".into(), update_to(current.e_tag.clone()))
        .await
        .unwrap();
    let meta = store.head(&path).await.unwrap();
    assert_eq!(meta.e_tag, updated.e_tag);
    assert_ne!(meta.e_tag, current.e_tag);
    let mtime = std::fs::metadata(&file).unwrap().modified().unwrap();
    assert_eq!(mtime, ahead + Duration::from_secs(1));
}

/// Equal-size updates on an HFS+ disk image, a filesystem with whole-second mtimes. The test
/// does nothing where `hdiutil` cannot make and attach the image.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn equal_size_updates_on_hfs_plus_get_new_e_tags() {
    let dir = tempfile::tempdir().unwrap();
    let image = dir.path().join("hfs.dmg");
    let mount = dir.path().join("mount");
    let hdiutil = |args: &[&std::ffi::OsStr]| {
        std::process::Command::new("hdiutil")
            .args(args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    };
    let made = hdiutil(&[
        "create".as_ref(),
        "-size".as_ref(),
        "4m".as_ref(),
        "-fs".as_ref(),
        "HFS+".as_ref(),
        "-volname".as_ref(),
        "boundary".as_ref(),
        image.as_os_str(),
    ]);
    std::fs::create_dir(&mount).unwrap();
    if !made
        || !hdiutil(&[
            "attach".as_ref(),
            "-nobrowse".as_ref(),
            "-mountpoint".as_ref(),
            mount.as_os_str(),
            image.as_os_str(),
        ])
    {
        eprintln!("skipped: hdiutil cannot make an HFS+ image here");
        return;
    }
    let result = async {
        let store = LocalStore::open(&mount.join("objects")).unwrap();
        let path = Path::from("boundary");
        let created = store
            .put_opts(&path, "0".into(), PutMode::Create.into())
            .await
            .unwrap();
        let mut e_tags = vec![created.e_tag.unwrap()];
        let mut mtime = store.head(&path).await.unwrap().last_modified;
        for n in 1..4 {
            let previous = e_tags.last().cloned();
            let updated = store
                .put_opts(&path, n.to_string().into(), update_to(previous))
                .await
                .unwrap();
            let meta = store.head(&path).await.unwrap();
            assert_eq!(meta.e_tag, updated.e_tag);
            assert!(!e_tags.contains(updated.e_tag.as_ref().unwrap()));
            // Each version has a later mtime, so a reused inode cannot repeat an e-tag.
            assert!(meta.last_modified > mtime, "{} {mtime}", meta.last_modified);
            mtime = meta.last_modified;
            e_tags.push(updated.e_tag.unwrap());
        }
    };
    let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(result)).await;
    assert!(hdiutil(&[
        "detach".as_ref(),
        "-force".as_ref(),
        mount.as_os_str()
    ]));
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn an_update_waits_for_a_held_lock_only_until_the_lock_wait() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = LocalStore::open(dir.path()).unwrap();
    store.lock_wait = Duration::from_millis(200);
    let path = Path::from("boundary");
    let created = store
        .put_opts(&path, "1".into(), PutMode::Create.into())
        .await
        .unwrap();
    let held = lock(&dir.path().join("boundary"), Instant::now()).unwrap();
    let started = Instant::now();
    let blocked = store
        .put_opts(&path, "2".into(), update_to(created.e_tag.clone()))
        .await;
    let blocked = blocked.unwrap_err().to_string();
    assert!(blocked.contains("timed out"), "{blocked}");
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(held);
    store
        .put_opts(&path, "2".into(), update_to(created.e_tag))
        .await
        .unwrap();
}

/// `LocalFileSystem` makes the staging file of a multipart upload when the future is first
/// polled. `LocalStore` does it on a blocking thread, so a hung filesystem cannot stop a
/// runtime thread.
#[test]
fn a_multipart_create_does_no_work_on_the_runtime_thread() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalStore::open(dir.path()).unwrap();
    let (runtime, release) = blocked_runtime();
    let path = Path::from("sst");
    let mut create = store.put_multipart_opts(&path, Default::default());
    let first = runtime.block_on(async { futures::poll!(create.as_mut()) });
    assert!(first.is_pending());
    assert!(staged(dir.path()).is_empty(), "{:?}", staged(dir.path()));
    release.send(()).unwrap();
    let mut upload = runtime.block_on(create).unwrap();
    runtime.block_on(upload.abort()).unwrap();
}

#[tokio::test]
async fn a_body_reads_its_range_in_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalStore::open(dir.path()).unwrap();
    let path = Path::from("large");
    let data: Vec<u8> = (0..READ_CHUNK * 2 + 7).map(|n| n as u8).collect();
    store.put(&path, data.clone().into()).await.unwrap();
    let result = store.get(&path).await.unwrap();
    assert_eq!(result.meta.size, data.len() as u64);
    let chunks: Vec<Bytes> = result.into_stream().try_collect().await.unwrap();
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks.concat(), data);
    let start = READ_CHUNK - 3;
    let range = store.get_range(&path, start..start + 10).await.unwrap();
    assert_eq!(range.as_ref(), &data[start as usize..start as usize + 10]);
}

/// The environment variables of a racing child process.
const RACE_DIR: &str = "SLATEDB_ETCD_TEST_RACE_DIR";
const RACE_ETAG: &str = "SLATEDB_ETCD_TEST_RACE_ETAG";

/// Two processes update one object with the same expected e-tag. Exactly one wins.
#[test]
fn one_of_two_racing_processes_updates() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let path = Path::from("gc/manifest.boundary");
    if let Ok(dir) = std::env::var(RACE_DIR) {
        // A child waits for the start file, then tries its update once.
        let dir = std::path::Path::new(&dir);
        while !dir.join("start").exists() {
            std::thread::sleep(Duration::from_micros(100));
        }
        let store = LocalStore::open(&dir.join("objects")).unwrap();
        let id = std::process::id().to_string();
        let e_tag = std::env::var(RACE_ETAG).ok();
        let result = runtime.block_on(store.put_opts(&path, id.into(), update_to(e_tag)));
        match result {
            Ok(_) => std::process::exit(0),
            Err(Error::Precondition { .. }) => std::process::exit(3),
            Err(error) => panic!("{error}"),
        }
    }
    let test = module_path!().split_once("::").unwrap().1;
    let test = format!("{test}::one_of_two_racing_processes_updates");
    for round in 0..8 {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(&dir.path().join("objects")).unwrap();
        let e_tag = runtime
            .block_on(store.put_opts(&path, "0".into(), PutMode::Create.into()))
            .unwrap()
            .e_tag
            .unwrap();
        let children: Vec<_> = (0..2)
            .map(|_| {
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args([test.as_str(), "--exact", "--test-threads=1"])
                    .env(RACE_DIR, dir.path())
                    .env(RACE_ETAG, &e_tag)
                    .stdout(std::process::Stdio::null())
                    .spawn()
                    .unwrap()
            })
            .collect();
        std::fs::write(dir.path().join("start"), b"").unwrap();
        let mut winners = vec![];
        for mut child in children {
            let id = child.id();
            match child.wait().unwrap().code() {
                Some(0) => winners.push(id),
                Some(3) => {}
                code => panic!("round {round}: child exited with {code:?}"),
            }
        }
        assert_eq!(winners.len(), 1, "round {round}");
        let bytes = runtime
            .block_on(async { store.get(&path).await.unwrap().bytes().await })
            .unwrap();
        assert_eq!(bytes.as_ref(), winners[0].to_string().as_bytes());
    }
}
