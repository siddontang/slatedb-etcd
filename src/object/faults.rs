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
}

pub(crate) const FAIL_PUT: u8 = 1;
pub(crate) const PAUSE_PUT: u8 = 2;
pub(crate) const LOSE_PUT_REPLY: u8 = 3;
pub(crate) const FAIL_GET: u8 = 4;

impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            mode: AtomicU8::new(0),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }

    pub fn set(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
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
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        if self.mode.load(Ordering::SeqCst) == FAIL_GET {
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
