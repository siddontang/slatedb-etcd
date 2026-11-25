//! Bound the calls of an object store in time and in number.
//!
//! A hung filesystem call holds a blocking thread in the kernel. A deadline alone returns the
//! caller but not the thread, and Tokio has 512 blocking threads for the whole process.
//! `Bounded` therefore runs each call in a task that holds a call slot until its work ends,
//! also after the deadline. At most `MAX_CALLS` calls hold a slot. A new call waits for a slot
//! within its own deadline. When every slot belongs to a call past its deadline, the store is
//! stuck, and a new call fails at once.

use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result, UploadPart,
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

/// The deadline of one call, like the request timeout of the S3 client.
const TIMEOUT: Duration = Duration::from_secs(30);
/// The call slots of the process. Late calls keep their slots, so at most this many blocked
/// filesystem calls can finish after their deadline.
const MAX_CALLS: usize = 64;
/// The deletes of one delete stream that run at once, as in `LocalFileSystem`.
const DELETES: usize = 10;

/// The call slots that all bounded stores of the process share.
pub(crate) static LIMITER: LazyLock<Arc<Limiter>> =
    LazyLock::new(|| Arc::new(Limiter::new(MAX_CALLS, TIMEOUT)));

/// Call slots and the deadline of each call.
#[derive(Debug)]
pub(crate) struct Limiter {
    slots: Arc<Semaphore>,
    max: usize,
    timeout: Duration,
    /// Calls that hold a slot after their deadline.
    late: AtomicUsize,
    held: prometheus::IntGauge,
}

impl Limiter {
    fn new(max: usize, timeout: Duration) -> Self {
        let held = prometheus::IntGauge::new(
            "slatedb_etcd_local_store_calls_in_flight",
            "Local object store calls that hold a call slot, also calls after their deadline.",
        )
        .expect("the gauge options are valid");
        Self {
            slots: Arc::new(Semaphore::new(max)),
            max,
            timeout,
            late: AtomicUsize::new(0),
            held,
        }
    }

    /// Add the call slot gauge to a member registry.
    pub(crate) fn register(&self, registry: &prometheus::Registry) -> anyhow::Result<()> {
        registry.register(Box::new(self.held.clone()))?;
        Ok(())
    }

    /// Take a slot before `deadline`. Fail at once if every slot belongs to a late call.
    async fn acquire(self: &Arc<Self>, deadline: Instant) -> Result<Slot> {
        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) if self.late.load(Ordering::SeqCst) >= self.max => {
                return Err(error(format!(
                    "all {} call slots belong to calls past their deadline",
                    self.max
                )));
            }
            Err(_) => {
                match tokio::time::timeout_at(deadline, self.slots.clone().acquire_owned()).await {
                    Ok(permit) => permit.expect("the call slots stay open"),
                    Err(_) => return Err(self.timed_out()),
                }
            }
        };
        Ok(self.slot(permit))
    }

    /// Wait for a slot without a deadline. Cleanup that must not leave untracked work uses it.
    async fn wait(self: &Arc<Self>) -> Slot {
        let permit = self.slots.clone().acquire_owned().await;
        self.slot(permit.expect("the call slots stay open"))
    }

    fn slot(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> Slot {
        self.held.inc();
        Slot {
            _permit: permit,
            limiter: self.clone(),
            late: false,
        }
    }

    fn timed_out(&self) -> Error {
        error(format!(
            "the object store call timed out after {:?}",
            self.timeout
        ))
    }
}

fn error(message: String) -> Error {
    Error::Generic {
        store: "Bounded",
        source: message.into(),
    }
}

/// The call slot of one call. It returns when the work of the call ends.
struct Slot {
    _permit: OwnedSemaphorePermit,
    limiter: Arc<Limiter>,
    late: bool,
}

impl Slot {
    fn mark_late(&mut self) {
        self.late = true;
        self.limiter.late.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.limiter.held.dec();
        if self.late {
            self.limiter.late.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Run `work` in a task that holds a call slot. Return its result, or an error at the deadline.
/// After the deadline the task keeps the slot until `work` ends.
async fn run<T: Send + 'static>(
    limiter: &Arc<Limiter>,
    work: impl Future<Output = Result<T>> + Send + 'static,
) -> Result<T> {
    let deadline = Instant::now() + limiter.timeout;
    let slot = limiter.acquire(deadline).await?;
    let task = tokio::spawn(hold(slot, deadline, work));
    match tokio::time::timeout_at(deadline, task).await {
        Ok(result) => result?,
        Err(_) => Err(limiter.timed_out()),
    }
}

/// Run `work` with `slot`. Mark the slot late when `work` passes `deadline`.
async fn hold<T>(mut slot: Slot, deadline: Instant, work: impl Future<Output = T>) -> T {
    let mut work = std::pin::pin!(work);
    if let Ok(result) = tokio::time::timeout_at(deadline, &mut work).await {
        return result;
    }
    slot.mark_late();
    work.await
}

type Items<T> = BoxStream<'static, Result<T>>;

/// Pull the next item of `stream` as one call.
async fn pull<T: Send + 'static>(
    limiter: &Arc<Limiter>,
    mut stream: Items<T>,
) -> Result<(Option<Result<T>>, Items<T>)> {
    run(limiter, async move { Ok((stream.next().await, stream)) }).await
}

/// Pull each item of `stream` as one call. A late item ends the stream with an error.
fn items<T: Send + 'static>(limiter: Arc<Limiter>, stream: Items<T>) -> Items<T> {
    futures::stream::unfold(Some(stream), move |stream| {
        let limiter = limiter.clone();
        async move {
            match pull(&limiter, stream?).await {
                Ok((Some(item), stream)) => Some((item, Some(stream))),
                Ok((None, _)) => None,
                Err(error) => Some((Err(error), None)),
            }
        }
    })
    .boxed()
}

/// Pull each chunk of a body of `len` bytes as one call. The stream ends after the last byte,
/// so its end takes no call.
fn body(limiter: Arc<Limiter>, stream: Items<Bytes>, len: u64) -> Items<Bytes> {
    futures::stream::unfold((Some(stream), len), move |(stream, left)| {
        let limiter = limiter.clone();
        async move {
            let stream = stream.filter(|_| left > 0)?;
            match pull(&limiter, stream).await {
                Ok((Some(Ok(chunk)), stream)) => {
                    let left = left.saturating_sub(chunk.len() as u64);
                    Some((Ok(chunk), (Some(stream), left)))
                }
                Ok((Some(Err(error)), stream)) => Some((Err(error), (Some(stream), left))),
                Ok((None, _)) => None,
                Err(error) => Some((Err(error), (None, left))),
            }
        }
    })
    .boxed()
}

/// An object store whose calls have a deadline and share a limited number of call slots.
///
/// Each call runs in its own task. A stream takes a slot for each item, and each read of an
/// object body has its own deadline. A delete stream takes a slot for each delete.
#[derive(Debug)]
pub(crate) struct Bounded<S> {
    inner: Arc<S>,
    limiter: Arc<Limiter>,
}

impl<S: ObjectStore> Bounded<S> {
    /// Bound `inner` with the call slots of the process.
    pub(crate) fn new(inner: S) -> Self {
        Self::with_limiter(inner, LIMITER.clone())
    }

    fn with_limiter(inner: S, limiter: Arc<Limiter>) -> Self {
        Self {
            inner: Arc::new(inner),
            limiter,
        }
    }

    async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(Arc<S>) -> futures::future::BoxFuture<'static, Result<T>>,
    ) -> Result<T> {
        run(&self.limiter, work(self.inner.clone())).await
    }
}

impl<S: ObjectStore> fmt::Display for Bounded<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bounded({})", self.inner)
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for Bounded<S> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let location = location.clone();
        self.run(|inner| Box::pin(async move { inner.put_opts(&location, payload, opts).await }))
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let location = location.clone();
        let limiter = self.limiter.clone();
        // The task wraps the upload. An upload that arrives after the deadline then aborts
        // under a call slot when the task drops it.
        self.run(|inner| {
            Box::pin(async move {
                let upload = inner.put_multipart_opts(&location, opts).await?;
                Ok(Box::new(Upload::new(upload, limiter)) as Box<dyn MultipartUpload>)
            })
        })
        .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let location = location.clone();
        let mut result = self
            .run(|inner| Box::pin(async move { inner.get_opts(&location, options).await }))
            .await?;
        let empty = GetResultPayload::Stream(futures::stream::empty().boxed());
        let stream = match std::mem::replace(&mut result.payload, empty) {
            GetResultPayload::Stream(stream) => stream,
            file => GetResult {
                payload: file,
                meta: result.meta.clone(),
                range: result.range.clone(),
                attributes: Default::default(),
                extensions: Default::default(),
            }
            .into_stream(),
        };
        let len = result.range.end - result.range.start;
        result.payload = GetResultPayload::Stream(body(self.limiter.clone(), stream, len));
        Ok(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let location = location.clone();
        let ranges = ranges.to_vec();
        self.run(|inner| Box::pin(async move { inner.get_ranges(&location, &ranges).await }))
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        let limiter = self.limiter.clone();
        locations
            .map(move |location| {
                let inner = inner.clone();
                let limiter = limiter.clone();
                async move {
                    let location = location?;
                    run(&limiter, async move {
                        inner.delete(&location).await?;
                        Ok(location)
                    })
                    .await
                }
            })
            .buffered(DELETES)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        items(self.limiter.clone(), self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        items(
            self.limiter.clone(),
            self.inner.list_with_offset(prefix, offset),
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.cloned();
        self.run(|inner| Box::pin(async move { inner.list_with_delimiter(prefix.as_ref()).await }))
            .await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let (from, to) = (from.clone(), to.clone());
        self.run(|inner| Box::pin(async move { inner.copy_opts(&from, &to, options).await }))
            .await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let (from, to) = (from.clone(), to.clone());
        self.run(|inner| Box::pin(async move { inner.rename_opts(&from, &to, options).await }))
            .await
    }
}

/// A multipart upload whose calls hold call slots.
///
/// The inner upload finishes only in a task that holds a slot: in `complete`, in `abort`, or
/// after a drop. A call that gets no slot leaves the upload unfinished, so a drop still aborts
/// it under a slot. The inner upload then does no cleanup of its own outside the slots.
///
/// A drop spawns the abort on the runtime that created the upload, also from a thread outside
/// it. If that runtime has stopped, the inner upload removes its staging file on its own.
#[derive(Debug)]
struct Upload {
    shared: Arc<Shared>,
    limiter: Arc<Limiter>,
    runtime: tokio::runtime::Handle,
}

#[derive(Debug)]
struct Shared {
    upload: Mutex<Box<dyn MultipartUpload>>,
    /// Set under the lock by the task that completes or aborts the upload.
    finished: AtomicBool,
}

impl Upload {
    fn new(upload: Box<dyn MultipartUpload>, limiter: Arc<Limiter>) -> Self {
        let shared = Shared {
            upload: Mutex::new(upload),
            finished: AtomicBool::new(false),
        };
        Self {
            shared: Arc::new(shared),
            limiter,
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

#[async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part = match self.shared.upload.try_lock() {
            Ok(mut upload) => upload.put_part(data),
            Err(_) => {
                let busy = error("the upload is completing or aborting".into());
                return Box::pin(futures::future::ready(Err(busy)));
            }
        };
        let limiter = self.limiter.clone();
        Box::pin(async move { run(&limiter, part).await })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let shared = self.shared.clone();
        run(&self.limiter, async move {
            let mut upload = shared.upload.lock().await;
            shared.finished.store(true, Ordering::SeqCst);
            upload.complete().await
        })
        .await
    }

    async fn abort(&mut self) -> Result<()> {
        let shared = self.shared.clone();
        run(&self.limiter, async move {
            let mut upload = shared.upload.lock().await;
            shared.finished.store(true, Ordering::SeqCst);
            upload.abort().await
        })
        .await
    }
}

impl Drop for Upload {
    fn drop(&mut self) {
        if self.shared.finished.load(Ordering::SeqCst) {
            return;
        }
        let shared = self.shared.clone();
        let limiter = self.limiter.clone();
        self.runtime.spawn(async move {
            let slot = limiter.wait().await;
            let deadline = Instant::now() + limiter.timeout;
            hold(slot, deadline, async move {
                let mut upload = shared.upload.lock().await;
                if !shared.finished.swap(true, Ordering::SeqCst) {
                    let _ = upload.abort().await;
                }
            })
            .await;
        });
    }
}

#[cfg(test)]
mod tests;
