use super::*;
use crate::object::local::LocalStore;
use std::sync::atomic::AtomicBool;

fn limiter(max: usize, timeout: Duration) -> Arc<Limiter> {
    Arc::new(Limiter::new(max, timeout))
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

/// Wait until `check` holds, for at most five seconds.
async fn eventually(what: &str, check: impl Fn() -> bool) {
    let started = std::time::Instant::now();
    while !check() {
        assert!(started.elapsed() < Duration::from_secs(5), "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn assert_timed_out<T: fmt::Debug>(result: Result<T>, started: std::time::Instant) {
    let error = result.unwrap_err().to_string();
    assert!(error.contains("timed out"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2), "{error}");
}

/// The read of a regular file body has its own deadline. SlateDB reads a GC boundary with
/// `get_opts` and then `bytes`. Only the body read has the short deadline.
#[test]
fn a_body_read_fails_at_its_deadline() {
    use futures::TryStreamExt;
    let dir = tempfile::tempdir().unwrap();
    let local = LocalStore::open(dir.path()).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let path = Path::from("gc/manifest.boundary");
    let result = runtime.block_on(async {
        local.put(&path, "7".into()).await.unwrap();
        local.get(&path).await.unwrap()
    });
    let GetResultPayload::Stream(stream) = result.payload else {
        panic!("a local body is a stream");
    };
    let limits = limiter(MAX_CALLS, Duration::from_millis(60));
    let reads = body(limits.clone(), stream, result.meta.size);
    let (release, held) = std::sync::mpsc::channel::<()>();
    runtime.spawn_blocking(move || held.recv());
    let started = std::time::Instant::now();
    let read = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), reads.try_collect::<Vec<_>>())
            .await
            .expect("the body read waited past its deadline")
    });
    assert_timed_out(read, started);
    release.send(()).unwrap();
    runtime.block_on(eventually("the late read returns its slot", || {
        limits.held.get() == 0
    }));
}

/// A body ends after its last byte. Its end then takes no call.
#[tokio::test]
async fn a_body_ends_after_its_last_byte() {
    use futures::TryStreamExt;
    let limits = limiter(MAX_CALLS, Duration::from_secs(30));
    let chunks = [
        Ok(Bytes::from("ab")),
        Ok(Bytes::from("c")),
        Err(error("polled after the last byte".into())),
    ];
    let reads = body(limits, futures::stream::iter(chunks).boxed(), 3);
    let read: Vec<Bytes> = reads.try_collect().await.unwrap();
    assert_eq!(read.concat(), b"abc");
}

/// A GC run whose boundary read hangs returns, deletes nothing, and a later run deletes.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_gc_run_with_a_hung_boundary_read_returns() {
    use slatedb::config::{GarbageCollectorDirectoryOptions, GarbageCollectorOptions};
    let dir = tempfile::tempdir().unwrap();
    // The fixture uses its own store. Only the GC run under test has the short deadline.
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::open(dir.path()).unwrap());
    let limits = limiter(MAX_CALLS, Duration::from_millis(500));
    let bounded: Arc<dyn ObjectStore> = Arc::new(Bounded::with_limiter(
        LocalStore::open(dir.path()).unwrap(),
        limits.clone(),
    ));
    let gc = GarbageCollectorOptions {
        manifest_options: Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::ZERO,
            dry_run: false,
        }),
        wal_options: None,
        wal_fence_options: None,
        compacted_options: None,
        compactions_options: None,
        detach_options: None,
        boundary_files_enabled: true,
        object_store_max_retries: Some(0),
        ..Default::default()
    };
    let admin = slatedb::admin::Admin::builder("db", store.clone()).build();
    let manifests = || async {
        let prefix = Path::from("db/manifest");
        store.list(Some(&prefix)).count().await
    };
    let flushes = || async {
        let db = slatedb::Db::builder("db", store.clone())
            .build()
            .await
            .unwrap();
        for n in 0..3 {
            db.put(b"k", n.to_string()).await.unwrap();
            db.flush().await.unwrap();
        }
        db.close().await.unwrap();
    };
    flushes().await;
    admin.run_gc_once(gc.clone()).await.unwrap();
    flushes().await;
    let before = manifests().await;

    let boundary = dir.path().join("db/gc/manifest.boundary");
    let saved = std::fs::read(&boundary).unwrap();
    std::fs::remove_file(&boundary).unwrap();
    let mut fifo = Fifo::new(boundary.clone());
    let started = std::time::Instant::now();
    let hung = slatedb::admin::Admin::builder("db", bounded).build();
    hung.run_gc_once(gc.clone()).await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(manifests().await, before);

    // Release the blocked reads, then restore the boundary.
    let writer = fifo.release();
    eventually("the late reads return their slots", || {
        limits.held.get() == 0
    })
    .await;
    drop(writer);
    std::fs::remove_file(&boundary).unwrap();
    std::fs::write(&boundary, saved).unwrap();
    admin.run_gc_once(gc).await.unwrap();
    assert!(manifests().await < before);
}

/// A FIFO. An open for reading waits for a writer, like a call on a hung filesystem.
#[cfg(unix)]
struct Fifo {
    path: std::path::PathBuf,
    released: bool,
}

#[cfg(unix)]
impl Fifo {
    fn new(path: std::path::PathBuf) -> Self {
        let made = std::process::Command::new("mkfifo").arg(&path).status();
        assert!(made.unwrap().success());
        Self {
            path,
            released: false,
        }
    }

    /// Open the writer end, which releases the waiting opens. Keep it open while late calls
    /// finish.
    fn release(&mut self) -> std::fs::File {
        self.released = true;
        let path = self.path.clone();
        let writer = std::thread::spawn(move || std::fs::OpenOptions::new().write(true).open(path));
        writer.join().unwrap().unwrap()
    }
}

#[cfg(unix)]
impl Drop for Fifo {
    /// A failed test still releases the waiting opens, so that its runtime can stop.
    fn drop(&mut self) {
        if !self.released {
            let path = self.path.clone();
            std::thread::spawn(move || {
                let _writer = std::fs::OpenOptions::new().write(true).open(path);
                std::thread::sleep(Duration::from_secs(5));
            });
        }
    }
}

/// A multipart create that finishes after its deadline aborts under a call slot. No cleanup
/// runs outside the slots, and the staging file goes away.
#[test]
fn a_late_multipart_create_aborts_under_a_slot() {
    let dir = tempfile::tempdir().unwrap();
    let limits = limiter(MAX_CALLS, Duration::from_millis(60));
    let store = Bounded::with_limiter(LocalStore::open(dir.path()).unwrap(), limits.clone());
    let (runtime, release) = blocked_runtime();
    let started = std::time::Instant::now();
    let path = Path::from("sst");
    let create = runtime.block_on(store.put_multipart_opts(&path, Default::default()));
    assert_timed_out(create, started);
    assert_eq!(limits.held.get(), 1);
    release.send(()).unwrap();
    runtime.block_on(eventually("the late create returns its slot", || {
        limits.held.get() == 0
            && std::fs::read_dir(dir.path())
                .unwrap()
                .all(|entry| !entry.unwrap().file_name().to_string_lossy().contains('#'))
    }));
}

/// An upload that records the slots in use when it aborts.
#[derive(Debug)]
struct Recorded {
    limiter: Arc<Limiter>,
    held_at_abort: Arc<std::sync::Mutex<Option<i64>>>,
}

#[async_trait]
impl MultipartUpload for Recorded {
    fn put_part(&mut self, _: PutPayload) -> UploadPart {
        unimplemented!()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        unimplemented!()
    }

    async fn abort(&mut self) -> Result<()> {
        *self.held_at_abort.lock().unwrap() = Some(self.limiter.held.get());
        Ok(())
    }
}

#[tokio::test]
async fn a_dropped_upload_aborts_under_a_slot() {
    let limits = limiter(MAX_CALLS, Duration::from_secs(30));
    let held_at_abort = Arc::new(std::sync::Mutex::new(None));
    let recorded = Recorded {
        limiter: limits.clone(),
        held_at_abort: held_at_abort.clone(),
    };
    drop(Upload::new(Box::new(recorded), limits.clone()));
    eventually("the dropped upload aborts", || {
        held_at_abort.lock().unwrap().is_some()
    })
    .await;
    assert_eq!(*held_at_abort.lock().unwrap(), Some(1));
    eventually("the abort returns its slot", || limits.held.get() == 0).await;
}

/// Start a multipart upload with one part, and return it with the name of its staging file.
async fn started_upload(
    store: &Bounded<LocalStore>,
    dir: &std::path::Path,
) -> (Box<dyn MultipartUpload>, std::path::PathBuf) {
    let mut upload = store
        .put_multipart_opts(&Path::from("sst"), Default::default())
        .await
        .unwrap();
    upload.put_part("data".into()).await.unwrap();
    (upload, dir.join("sst#1"))
}

/// Take every slot of `limits`. Late slots make the store look stuck.
async fn take_slots(limits: &Arc<Limiter>, late: bool) -> Vec<Slot> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut slots = vec![];
    for _ in 0..limits.max {
        let mut slot = limits.acquire(deadline).await.unwrap();
        if late {
            slot.mark_late();
        }
        slots.push(slot);
    }
    slots
}

/// Check that a dropped upload keeps its staging file while no slot is free, and removes it
/// under a slot after the slots return.
async fn assert_cleanup_waits_for_a_slot(
    limits: &Arc<Limiter>,
    slots: Vec<Slot>,
    staged: &std::path::Path,
) {
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(staged.exists(), "cleanup ran outside the call slots");
    assert_eq!(limits.held.get(), limits.max as i64);
    drop(slots);
    eventually("the cleanup removes the staging file", || !staged.exists()).await;
    eventually("the cleanup returns its slot", || limits.held.get() == 0).await;
}

/// `complete` and `abort` that get no slot leave the upload unfinished. Its drop then
/// aborts it under a slot.
#[tokio::test(flavor = "multi_thread")]
async fn an_upload_whose_finish_gets_no_slot_aborts_under_a_slot() {
    let dir = tempfile::tempdir().unwrap();
    let limits = limiter(MAX_CALLS, Duration::from_secs(30));
    let store = Bounded::with_limiter(LocalStore::open(dir.path()).unwrap(), limits.clone());
    let (mut upload, staged) = started_upload(&store, dir.path()).await;
    assert!(staged.exists());
    let slots = take_slots(&limits, true).await;
    for error in [upload.complete().await.map(drop), upload.abort().await] {
        let error = error.unwrap_err().to_string();
        assert!(error.contains("call slots"), "{error}");
    }
    drop(upload);
    assert_cleanup_waits_for_a_slot(&limits, slots, &staged).await;
}

/// A `complete` that its caller drops while it waits for a slot leaves the upload unfinished.
#[tokio::test(flavor = "multi_thread")]
async fn an_upload_whose_finish_is_cancelled_aborts_under_a_slot() {
    let dir = tempfile::tempdir().unwrap();
    let limits = limiter(MAX_CALLS, Duration::from_secs(30));
    let store = Bounded::with_limiter(LocalStore::open(dir.path()).unwrap(), limits.clone());
    let (mut upload, staged) = started_upload(&store, dir.path()).await;
    let slots = take_slots(&limits, false).await;
    let waiting = tokio::time::timeout(Duration::from_millis(20), upload.complete()).await;
    assert!(waiting.is_err());
    drop(upload);
    assert_cleanup_waits_for_a_slot(&limits, slots, &staged).await;
}

/// A drop on a thread outside the runtime still aborts under a slot of that runtime.
#[tokio::test(flavor = "multi_thread")]
async fn an_upload_dropped_outside_the_runtime_aborts_under_a_slot() {
    let dir = tempfile::tempdir().unwrap();
    let limits = limiter(MAX_CALLS, Duration::from_secs(30));
    let store = Bounded::with_limiter(LocalStore::open(dir.path()).unwrap(), limits.clone());
    let (upload, staged) = started_upload(&store, dir.path()).await;
    let slots = take_slots(&limits, false).await;
    std::thread::spawn(move || drop(upload)).join().unwrap();
    assert_cleanup_waits_for_a_slot(&limits, slots, &staged).await;
}

/// An upload whose abort waits until the gate opens.
#[derive(Debug)]
struct HungAbort(tokio::sync::watch::Receiver<bool>);

#[async_trait]
impl MultipartUpload for HungAbort {
    fn put_part(&mut self, _: PutPayload) -> UploadPart {
        unimplemented!()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        unimplemented!()
    }

    async fn abort(&mut self) -> Result<()> {
        let _ = self.0.wait_for(|open| *open).await;
        Ok(())
    }
}

/// A cleanup abort past its deadline counts as a late call. When such cleanups hold every
/// slot, a new call fails at once.
#[tokio::test(flavor = "multi_thread")]
async fn late_cleanups_make_new_calls_fail_at_once() {
    let limits = limiter(MAX_CALLS, Duration::from_secs(1));
    let (open, gate) = tokio::sync::watch::channel(false);
    for _ in 0..MAX_CALLS {
        drop(Upload::new(
            Box::new(HungAbort(gate.clone())),
            limits.clone(),
        ));
    }
    eventually("every cleanup is late", || {
        limits.late.load(Ordering::SeqCst) == MAX_CALLS
    })
    .await;
    let started = std::time::Instant::now();
    let next = run(&limits, async { Ok(()) }).await;
    let error = next.unwrap_err().to_string();
    assert!(error.contains("call slots"), "{error}");
    assert!(started.elapsed() < Duration::from_millis(500));
    open.send(true).unwrap();
    eventually("the cleanups return their slots", || limits.held.get() == 0).await;
    assert_eq!(limits.late.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn many_reads_on_a_healthy_store_all_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let limits = limiter(MAX_CALLS, Duration::from_secs(30));
    let store = Arc::new(Bounded::with_limiter(
        LocalStore::open(dir.path()).unwrap(),
        limits.clone(),
    ));
    let path = Path::from("object");
    store.put(&path, "value".into()).await.unwrap();
    let reads = (0..200).map(|_| {
        let store = store.clone();
        let path = path.clone();
        tokio::spawn(async move { store.get(&path).await?.bytes().await })
    });
    for read in futures::future::join_all(reads).await {
        assert_eq!(read.unwrap().unwrap().as_ref(), b"value");
    }
    assert_eq!(limits.held.get(), 0);
}

/// With every slot held by a late call, a new call fails at once. Other blocking work of the
/// process still runs.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_stuck_store_fails_new_calls_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let limits = limiter(MAX_CALLS, Duration::from_secs(1));
    let local = LocalStore::open(dir.path()).unwrap();
    local.put(&Path::from("object"), "1".into()).await.unwrap();
    let store = Arc::new(Bounded::with_limiter(local, limits.clone()));
    let mut fifo = Fifo::new(dir.path().join("fifo"));
    let hung = (0..MAX_CALLS).map(|_| {
        let store = store.clone();
        tokio::spawn(async move { store.get(&Path::from("fifo")).await.map(drop) })
    });
    for call in futures::future::join_all(hung).await {
        assert!(call.unwrap().is_err());
    }
    eventually("every hung call is late", || {
        limits.late.load(Ordering::SeqCst) == MAX_CALLS
    })
    .await;
    assert_eq!(limits.held.get(), MAX_CALLS as i64);

    let started = std::time::Instant::now();
    let next = store.get(&Path::from("object")).await.map(drop);
    let error = next.unwrap_err().to_string();
    assert!(error.contains("call slots"), "{error}");
    // A wait for a slot would last the whole 1 s deadline.
    assert!(started.elapsed() < Duration::from_millis(500));
    let other = tokio::task::spawn_blocking(|| 7);
    let other = tokio::time::timeout(Duration::from_secs(1), other).await;
    assert_eq!(other.unwrap().unwrap(), 7);

    let writer = fifo.release();
    eventually("the late calls return their slots", || {
        limits.held.get() == 0
    })
    .await;
    drop(writer);
    assert_eq!(limits.late.load(Ordering::SeqCst), 0);
    store.get(&Path::from("object")).await.unwrap();
}

/// A store whose deletes block on a gate, ten at a time like `LocalFileSystem`. It counts
/// the deletes in progress.
#[derive(Debug, Default)]
struct GatedDeletes {
    open: Arc<AtomicBool>,
    running: Arc<AtomicUsize>,
}

impl fmt::Display for GatedDeletes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GatedDeletes")
    }
}

#[async_trait]
impl ObjectStore for GatedDeletes {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        unimplemented!()
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        unimplemented!()
    }

    async fn get_opts(&self, _: &Path, _: GetOptions) -> Result<GetResult> {
        unimplemented!()
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let (open, running) = (self.open.clone(), self.running.clone());
        locations
            .map(move |location| {
                let (open, running) = (open.clone(), running.clone());
                async move {
                    let location = location?;
                    tokio::task::spawn_blocking(move || {
                        running.fetch_add(1, Ordering::SeqCst);
                        while !open.load(Ordering::SeqCst) {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        running.fetch_sub(1, Ordering::SeqCst);
                    })
                    .await?;
                    Ok(location)
                }
            })
            .buffered(DELETES)
            .boxed()
    }

    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        unimplemented!()
    }

    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        unimplemented!()
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        unimplemented!()
    }
}

/// Open a gate when dropped.
struct Gate(Arc<AtomicBool>);

impl Drop for Gate {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Each blocking delete of a delete stream holds its own slot.
#[tokio::test(flavor = "multi_thread")]
async fn each_delete_of_a_stream_holds_a_slot() {
    let limits = limiter(MAX_CALLS, Duration::from_secs(30));
    let inner = GatedDeletes::default();
    let (open, running) = (inner.open.clone(), inner.running.clone());
    // A failed assertion still opens the gate, so that the runtime can stop.
    let _gate = Gate(open.clone());
    let store = Bounded::with_limiter(inner, limits.clone());
    let paths = (0..30).map(|n| Ok(Path::from(format!("sst-{n}"))));
    let deletes = store.delete_stream(futures::stream::iter(paths).boxed());
    let deleted = tokio::spawn(deletes.collect::<Vec<_>>());
    eventually("ten deletes block", || {
        running.load(Ordering::SeqCst) == DELETES
    })
    .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(running.load(Ordering::SeqCst), DELETES);
    assert_eq!(limits.held.get(), DELETES as i64);
    open.store(true, Ordering::SeqCst);
    let deleted = deleted.await.unwrap();
    assert_eq!(deleted.len(), 30);
    assert!(deleted.iter().all(Result::is_ok));
    assert_eq!(limits.held.get(), 0);
}
