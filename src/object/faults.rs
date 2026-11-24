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
    /// The delay of `DELAY_SST_GET`, in milliseconds.
    delay_ms: std::sync::atomic::AtomicU64,
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

impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            mode: AtomicU8::new(0),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            resumed: tokio::sync::Notify::new(),
            delay_ms: std::sync::atomic::AtomicU64::new(120),
        }
    }

    pub fn set(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
        self.resumed.notify_waiters();
    }

    pub fn set_delay(&self, delay: std::time::Duration) {
        self.delay_ms
            .store(delay.as_millis() as u64, Ordering::SeqCst);
    }

    async fn stall(&self, path: &Path) {
        loop {
            let resumed = self.resumed.notified();
            match self.mode.load(Ordering::SeqCst) {
                STALL_PUT => {}
                STALL_STATE_PUT if path.as_ref().contains("shared/state/") => {}
                _ => return,
            }
            resumed.await;
        }
    }
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
        self.stall(path).await;
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
        let mode = self.mode.load(Ordering::SeqCst);
        if mode == LOSE_SST_ONCE && path.as_ref().ends_with(".sst") {
            self.mode.store(0, Ordering::SeqCst);
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
        if mode == FAIL_GET || (mode == FAIL_SST_GET && path.as_ref().ends_with(".sst")) {
            self.entered.add_permits(1);
            return Err(error());
        }
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
