//! Inject faults at the ObjectStore boundary without changing SlateDB.

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use slatedb::object_store::path::Path;
use slatedb::object_store::*;
use tokio::sync::Semaphore;

#[derive(Debug)]
pub(crate) struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    mode: AtomicU8,
    pub entered: Semaphore,
    pub release: Semaphore,
    /// Wakes stalled puts after a mode change.
    resumed: tokio::sync::Notify,
    /// The delay of `DELAY_SST_GET` and `DELAY_SHARED`, in milliseconds.
    delay_ms: std::sync::atomic::AtomicU64,
    /// The paths of puts and copy targets, and of reads, for access tests.
    pub written: std::sync::Mutex<std::collections::BTreeSet<String>>,
    pub read: std::sync::Mutex<std::collections::BTreeSet<String>>,
    /// The put that `DETACH_STATE_PUT` kept.
    detached: std::sync::Mutex<Option<(Path, PutPayload, PutOptions)>>,
}

pub(crate) const FAIL_PUT: u8 = 1;
pub(crate) const PAUSE_PUT: u8 = 2;
pub(crate) const LOSE_PUT_REPLY: u8 = 3;
pub(crate) const FAIL_GET: u8 = 4;
pub(crate) const FAIL_SST_GET: u8 = 5;
/// Hold every put until the mode changes, like a stalled object store.
pub(crate) const STALL_PUT: u8 = 6;
/// Hold every put of the shared database, but not the claims beside it.
pub(crate) const STALL_STATE_PUT: u8 = 8;
/// Delay every SST read, by 120 ms unless `set_delay` changes it, like a slow object store.
pub(crate) const DELAY_SST_GET: u8 = 9;
/// Report the next SST read as missing, like an SST that GC deleted.
pub(crate) const LOSE_SST_ONCE: u8 = 7;
/// Fail every put of the shared database, like a bucket that denies writes. Claims beside it
/// still succeed.
pub(crate) const FAIL_STATE_PUT: u8 = 11;
/// Return manifests of the shared database that do not decode, like a newer manifest format.
pub(crate) const CORRUPT_MANIFEST: u8 = 12;
/// Hold every read of the bootstrap marker and claim until the mode changes.
pub(crate) const STALL_BOOTSTRAP_READ: u8 = 13;
/// Hold every read of a manifest of the shared database until the mode changes.
pub(crate) const STALL_MANIFEST_READ: u8 = 14;
/// Hold the next read of a manifest of the shared database until `release`.
pub(crate) const PAUSE_MANIFEST_READ: u8 = 15;
/// Delay every read and put of the shared objects, by 120 ms unless `set_delay` changes it, like
/// the latency of S3.
pub(crate) const DELAY_SHARED: u8 = 16;
/// Report the next two SST reads as missing. A failed cache load in SlateDB falls back to one
/// direct read, so only this mode fails a read that goes through the block cache.
pub(crate) const LOSE_SST_TWICE: u8 = 10;
/// Fail every SST read with a retryable error, like a local file with a persistent I/O error.
pub(crate) const RETRYABLE_SST_GET: u8 = 17;
/// Report the next two SST reads as missing, like `LOSE_SST_TWICE`. Then fail every read, like
/// `FAIL_GET`.
pub(crate) const LOSE_SST_TWICE_THEN_FAIL: u8 = 18;
/// The second step of `LOSE_SST_TWICE_THEN_FAIL`.
const LOSE_SST_ONCE_THEN_FAIL: u8 = 19;
/// Fail every delete in the shared database, like a credential that can create but not delete
/// objects there.
pub(crate) const FAIL_STATE_DELETE: u8 = 20;
/// Store the next put in the shared database, then report a timeout, like a reply that the
/// network lost.
pub(crate) const LOSE_STATE_PUT_REPLY: u8 = 21;
/// Fail every put and every delete in the shared database, like a read-only credential.
pub(crate) const READ_ONLY_STATE: u8 = 22;
/// Report a timeout for the next put in the shared database, and keep the put. `land` writes it
/// later, like a request that reaches the store after its caller stopped waiting.
pub(crate) const DETACH_STATE_PUT: u8 = 23;
/// Delete each object in the shared database, then report a refusal, like a refused delete of
/// an object that another client removed.
pub(crate) const GONE_STATE_DELETE: u8 = 24;
/// Delete each object in the shared database, then report it as not found.
pub(crate) const NOT_FOUND_STATE_DELETE: u8 = 25;

impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            mode: AtomicU8::new(0),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            resumed: tokio::sync::Notify::new(),
            delay_ms: std::sync::atomic::AtomicU64::new(120),
            written: Default::default(),
            read: Default::default(),
            detached: Default::default(),
        }
    }

    /// Write the put that `DETACH_STATE_PUT` kept.
    pub async fn land(&self) -> Result<PutResult> {
        let detached = self.detached.lock().unwrap().take();
        let (path, payload, options) = detached.expect("no detached put");
        self.inner.put_opts(&path, payload, options).await
    }

    pub fn set(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
        self.resumed.notify_waiters();
    }

    pub fn set_delay(&self, delay: std::time::Duration) {
        self.delay_ms
            .store(delay.as_millis() as u64, Ordering::SeqCst);
    }

    /// Wait for the delay of `DELAY_SHARED` on a shared object.
    async fn lag(&self, path: &Path) {
        if self.mode.load(Ordering::SeqCst) == DELAY_SHARED && path.as_ref().contains("shared/") {
            let delay = self.delay_ms.load(Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
    }

    /// Wait while the mode is `mode`.
    async fn hold(&self, mode: u8) {
        loop {
            let resumed = self.resumed.notified();
            if self.mode.load(Ordering::SeqCst) != mode {
                return;
            }
            resumed.await;
        }
    }

    async fn stall(&self, path: &Path) {
        loop {
            let resumed = self.resumed.notified();
            match self.mode.load(Ordering::SeqCst) {
                STALL_PUT => {}
                STALL_STATE_PUT if state(path) => {}
                _ => return,
            }
            resumed.await;
        }
    }
}

/// Return true for an object of the shared database.
fn state(path: &Path) -> bool {
    path.as_ref().contains("shared/state/")
}

fn error() -> Error {
    Error::NotSupported {
        source: std::io::Error::new(std::io::ErrorKind::Unsupported, "injected failure").into(),
    }
}

impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.written.lock().unwrap().insert(path.to_string());
        self.stall(path).await;
        self.lag(path).await;
        if matches!(
            self.mode.load(Ordering::SeqCst),
            FAIL_STATE_PUT | READ_ONLY_STATE
        ) && state(path)
        {
            self.entered.add_permits(1);
            return Err(error());
        }
        let order = Ordering::SeqCst;
        if state(path)
            && self
                .mode
                .compare_exchange(DETACH_STATE_PUT, 0, order, order)
                .is_ok()
        {
            *self.detached.lock().unwrap() = Some((path.clone(), payload, options));
            return Err(Error::Generic {
                store: "injected",
                source: std::io::Error::new(std::io::ErrorKind::TimedOut, "detached put").into(),
            });
        }
        let order = Ordering::SeqCst;
        if state(path)
            && self
                .mode
                .compare_exchange(LOSE_STATE_PUT_REPLY, 0, order, order)
                .is_ok()
        {
            self.inner.put_opts(path, payload, options).await?;
            self.entered.add_permits(1);
            return Err(Error::Generic {
                store: "injected",
                source: std::io::Error::new(std::io::ErrorKind::TimedOut, "lost reply").into(),
            });
        }
        if path.as_ref().ends_with(".sst") {
            match self.mode.load(Ordering::SeqCst) {
                FAIL_PUT => {
                    self.entered.add_permits(1);
                    return Err(error());
                }
                PAUSE_PUT => {
                    self.mode.store(0, Ordering::SeqCst);
                    self.entered.add_permits(1);
                    self.release.acquire().await.unwrap().forget();
                }
                LOSE_PUT_REPLY => {
                    self.mode.store(0, Ordering::SeqCst);
                    self.inner.put_opts(path, payload, options).await?;
                    self.entered.add_permits(1);
                    return Err(Error::Generic {
                        store: "injected",
                        source: std::io::Error::new(std::io::ErrorKind::TimedOut, "lost reply")
                            .into(),
                    });
                }
                _ => {}
            }
        }
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.stall(path).await;
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        self.read.lock().unwrap().insert(path.to_string());
        self.lag(path).await;
        if path.as_ref().contains("shared/bootstrap") {
            self.hold(STALL_BOOTSTRAP_READ).await;
        }
        if state(path) && path.as_ref().ends_with(".manifest") {
            self.hold(STALL_MANIFEST_READ).await;
            let pause = Ordering::SeqCst;
            if self
                .mode
                .compare_exchange(PAUSE_MANIFEST_READ, 0, pause, pause)
                .is_ok()
            {
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
        }
        let mode = self.mode.load(Ordering::SeqCst);
        let losing = [
            LOSE_SST_ONCE,
            LOSE_SST_TWICE,
            LOSE_SST_TWICE_THEN_FAIL,
            LOSE_SST_ONCE_THEN_FAIL,
        ];
        if losing.contains(&mode) && path.as_ref().ends_with(".sst") {
            let next = match mode {
                LOSE_SST_TWICE => LOSE_SST_ONCE,
                LOSE_SST_TWICE_THEN_FAIL => LOSE_SST_ONCE_THEN_FAIL,
                LOSE_SST_ONCE_THEN_FAIL => FAIL_GET,
                _ => 0,
            };
            self.mode.store(next, Ordering::SeqCst);
            self.entered.add_permits(1);
            return Err(Error::NotFound {
                path: path.to_string(),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "injected loss").into(),
            });
        }
        if mode == DELAY_SST_GET && path.as_ref().ends_with(".sst") {
            let delay = self.delay_ms.load(Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        if mode == RETRYABLE_SST_GET && path.as_ref().ends_with(".sst") {
            return Err(Error::Generic {
                store: "injected",
                source: std::io::Error::other("injected I/O error").into(),
            });
        }
        if mode == FAIL_GET || (mode == FAIL_SST_GET && path.as_ref().ends_with(".sst")) {
            self.entered.add_permits(1);
            return Err(error());
        }
        if mode == CORRUPT_MANIFEST && state(path) && path.as_ref().ends_with(".manifest") {
            let mut result = self.inner.get_opts(path, options).await?;
            let garbage = bytes::Bytes::from_static(b"not a manifest");
            result.range = 0..garbage.len() as u64;
            result.meta.size = garbage.len() as u64;
            result.payload =
                GetResultPayload::Stream(Box::pin(futures::stream::once(async { Ok(garbage) })));
            return Ok(result);
        }
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let mode = self.mode.load(Ordering::SeqCst);
        let faulty = [
            FAIL_STATE_DELETE,
            READ_ONLY_STATE,
            GONE_STATE_DELETE,
            NOT_FOUND_STATE_DELETE,
        ];
        if !faulty.contains(&mode) {
            return self.inner.delete_stream(paths);
        }
        use futures::StreamExt;
        let inner = self.inner.clone();
        paths
            .then(move |path| {
                let inner = inner.clone();
                async move {
                    let path = path?;
                    if !state(&path) {
                        return inner.delete(&path).await.map(|()| path);
                    }
                    if mode == GONE_STATE_DELETE || mode == NOT_FOUND_STATE_DELETE {
                        inner.delete(&path).await?;
                    }
                    if mode == NOT_FOUND_STATE_DELETE {
                        return Err(Error::NotFound {
                            path: path.to_string(),
                            source: "injected".into(),
                        });
                    }
                    Err(error())
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.written.lock().unwrap().insert(to.to_string());
        self.read.lock().unwrap().insert(from.to_string());
        self.inner.copy_opts(from, to, options).await
    }
}
