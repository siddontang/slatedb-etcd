//! Write local SSTs durably without flooding the disk queue.
//!
//! A synced `LocalFileSystem` writes a whole SST to the page cache and then calls `fsync`. A
//! 64 MiB L0 SST then reaches the disk in one burst, and Raft log syncs queue behind it. This
//! store starts writeback one window at a time, as RocksDB `bytes_per_sync` does. A window is
//! normally one SST block, because SlateDB passes each block as its own chunk. A Raft log sync
//! then waits behind only a few blocks. A final sync, rename, and directory sync then publish the
//! SST. Other objects, such as manifests, use the synced inner store and keep its conditional
//! writes.
//!
//! Block-sized windows cost one disk round trip per block. After a bulk load, flushes and
//! compaction then fell behind: unflushed memtables reached `max_unflushed_bytes`, and SlateDB
//! held the apply pipeline for seconds. So while SlateDB reports unflushed memtable bytes of at
//! least [`BACKLOG_PERCENT`] of that limit, each window fills `PACE_BYTES`. Raft log syncs then
//! wait longer, but writers no longer wait for seconds of backpressure.
//!
//! SlateDB uploads a large SST as 10 MiB multipart parts, up to 8 at a time, and flushes and
//! compactions run concurrently. Pacing each writer alone still left dozens of windows in
//! writeback at once. The block layer's writeback throttling then blocked SST writers and the
//! Raft log thread alike, and every Raft `fdatasync` waited behind SST pages. So all SST writers
//! in the process share one [`Lane`] of `MAX_WINDOWS` windows. Compaction output yields to
//! waiting flushes, which writers wait on when memory fills up. Its share is an admission rule:
//! while a memtable flush runs, a compaction window is granted only while compaction holds
//! fewer than `MAX_COMPACTION_WINDOWS`, and with no flush running, fewer than
//! `MAX_IDLE_COMPACTION_WINDOWS`. Windows already held are not taken back when a flush starts;
//! each is released after one `PACE_BYTES` window of writeback. SlateDB stops L0 flushes when
//! L0 is full, and only compaction frees L0 then.
//!
//! A Raft log `fdatasync` waits for the SST windows in writeback ahead of it. While one runs, the
//! lane grants no new window ([`raft_sync`]), so a sync does not also wait for windows granted
//! during it. A writer yields to at most one sync, so back-to-back syncs cannot starve flushes.

use std::fmt;
use std::fs::File;
use std::io::ErrorKind;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use async_trait::async_trait;
use futures::stream::BoxStream;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path;
use slatedb::object_store::*;

use super::sst_reads::{self, SstReads};

/// The largest window. Windows are this large only during a write backlog.
const PACE_BYTES: u64 = 1024 * 1024;
/// Unflushed memtable bytes, in percent of `max_unflushed_bytes`, that start backlog windows.
///
/// A bulk load reached the limit within 5 s and then stalled apply for seconds. Short write
/// bursts, such as 100,000 puts of 4 KiB values, came to about 70 % on a slow disk. Backlog
/// windows during such a burst moved more SST writeback in front of Raft log syncs and cost
/// 10-15 % throughput.
pub(crate) const BACKLOG_PERCENT: usize = 85;
/// SST windows in writeback across the process.
const MAX_WINDOWS: usize = 4;
/// Windows compaction output may hold. One keeps compaction moving while memtable flushes get
/// the rest; with two, flush fell behind under 4 KiB-value writes and immutable memtables piled
/// up in memory.
const MAX_COMPACTION_WINDOWS: usize = 1;
/// Windows compaction output may hold while no memtable flush runs.
///
/// A full L0 stops every flush until compaction frees L0. With one window, compaction wrote
/// about 20-40 MB/s after a bulk load. Immutable memtables then filled `max_unflushed_bytes`,
/// and SlateDB held the apply pipeline for seconds. One window stays free, so the first window
/// a flush asks for on an idle lane comes without a wait. Further concurrent windows of that
/// flush may wait for compaction windows to finish their writeback.
const MAX_IDLE_COMPACTION_WINDOWS: usize = MAX_WINDOWS - 1;
const _: () = assert!(
    MAX_COMPACTION_WINDOWS < MAX_IDLE_COMPACTION_WINDOWS
        && MAX_IDLE_COMPACTION_WINDOWS < MAX_WINDOWS
);

#[derive(Debug)]
pub(crate) struct PacedStore {
    inner: Arc<LocalFileSystem>,
    reads: Arc<SstReads>,
    lane: &'static Lane,
    /// SlateDB of this store has a write backlog, so its windows fill `PACE_BYTES`. Each store
    /// has its own flag: embedded members share the lane but not their memtables.
    backlog: Arc<AtomicBool>,
}

impl PacedStore {
    /// `inner` must sync its own writes. Only SSTs bypass it.
    pub fn new(inner: LocalFileSystem) -> Self {
        Self {
            inner: Arc::new(inner),
            reads: Arc::default(),
            lane: &LANE,
            backlog: Arc::default(),
        }
    }

    /// A store that writes SSTs through its own lane, so a test can watch the lane alone.
    #[cfg(test)]
    fn with_lane(inner: LocalFileSystem, lane: &'static Lane) -> Self {
        Self {
            lane,
            ..Self::new(inner)
        }
    }

    fn paced(location: &Path, mode: &PutMode) -> bool {
        location.as_ref().ends_with(".sst") && matches!(mode, PutMode::Overwrite)
    }

    async fn result(&self, location: &Path) -> Result<PutResult> {
        let meta = self.inner.head(location).await?;
        Ok(PutResult {
            e_tag: meta.e_tag,
            version: meta.version,
            extensions: Default::default(),
        })
    }
}

impl fmt::Display for PacedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PacedStore({})", self.inner)
    }
}

fn io_error(source: std::io::Error, path: &FsPath) -> Error {
    Error::Generic {
        store: "PacedStore",
        source: format!("{}: {source}", path.display()).into(),
    }
}

fn join_error(source: tokio::task::JoinError) -> Error {
    Error::Generic {
        store: "PacedStore",
        source: source.into(),
    }
}

/// Open a new staged file next to `dest`. The inner store's listings skip `#<digits>` names.
fn stage(dest: &FsPath) -> Result<(File, PathBuf)> {
    super::local::stage(dest).map_err(|e| io_error(e, dest))
}

fn sync_range(file: &File, offset: u64, len: u64, flags: libc::c_uint) -> std::io::Result<()> {
    // SAFETY: the descriptor stays open for the call and the range is plain integers.
    let rc = unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            offset as libc::off64_t,
            len as libc::off64_t,
            flags,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Which SlateDB component writes an SST.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Writer {
    Flush,
    Compaction,
}

impl Writer {
    fn of(extensions: &Extensions) -> Self {
        use slatedb::object_store_tag::{ObjectStoreCallTag, TableStoreKind};
        match ObjectStoreCallTag::from_extensions(extensions) {
            Some(tag) if tag.kind == TableStoreKind::Compactor => Self::Compaction,
            _ => Self::Flush,
        }
    }
}

/// Process-wide budget of SST windows in writeback.
///
/// A writer holds at most one window at a time, so waiting for one never blocks a holder.
#[derive(Debug)]
struct Lane {
    state: Mutex<LaneState>,
    freed: Condvar,
}

#[derive(Debug, Default)]
struct LaneState {
    used: usize,
    compaction: usize,
    flush_waiting: usize,
    /// Memtable flush SSTs that are being written.
    flushes: usize,
    /// Raft log syncs that are running.
    syncs: usize,
    /// Raft log syncs that have finished.
    synced: u64,
    #[cfg(test)]
    max_used: usize,
    #[cfg(test)]
    max_compaction: usize,
    #[cfg(test)]
    windows: usize,
}

static LANE: Lane = Lane::new();

impl Lane {
    const fn new() -> Self {
        Self {
            state: Mutex::new(LaneState {
                used: 0,
                compaction: 0,
                flush_waiting: 0,
                flushes: 0,
                syncs: 0,
                synced: 0,
                #[cfg(test)]
                max_used: 0,
                #[cfg(test)]
                max_compaction: 0,
                #[cfg(test)]
                windows: 0,
            }),
            freed: Condvar::new(),
        }
    }

    fn acquire(&self, writer: Writer) -> Window<'_> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if writer == Writer::Flush {
            st.flush_waiting += 1;
        }
        let mut yielded = None;
        while !st.admits(writer) || st.yields_to_sync(&mut yielded) {
            st = self.freed.wait(st).unwrap_or_else(|e| e.into_inner());
        }
        self.grant(st, writer)
    }

    /// [`Lane::acquire`] that gives up after `timeout`, so a test cannot hang on a wrong policy.
    #[cfg(test)]
    fn try_acquire(&self, writer: Writer, timeout: std::time::Duration) -> Option<Window<'_>> {
        let deadline = std::time::Instant::now() + timeout;
        let mut st = self.state.lock().unwrap();
        if writer == Writer::Flush {
            st.flush_waiting += 1;
        }
        let mut yielded = None;
        while !st.admits(writer) || st.yields_to_sync(&mut yielded) {
            let Some(left) = deadline.checked_duration_since(std::time::Instant::now()) else {
                if writer == Writer::Flush {
                    st.flush_waiting -= 1;
                }
                return None;
            };
            st = self.freed.wait_timeout(st, left).unwrap().0;
        }
        Some(self.grant(st, writer))
    }

    /// Take a window that [`LaneState::admits`] allowed, under the same lock.
    fn grant<'a>(
        &'a self,
        mut st: std::sync::MutexGuard<'_, LaneState>,
        writer: Writer,
    ) -> Window<'a> {
        match writer {
            Writer::Flush => st.flush_waiting -= 1,
            Writer::Compaction => st.compaction += 1,
        }
        st.used += 1;
        #[cfg(test)]
        {
            st.max_used = st.max_used.max(st.used);
            st.max_compaction = st.max_compaction.max(st.compaction);
            st.windows += 1;
        }
        Window { lane: self, writer }
    }

    /// Hold new windows back until the guard drops. See [`raft_sync`].
    fn sync(&self) -> RaftSync<'_> {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).syncs += 1;
        RaftSync { lane: self }
    }

    /// Mark one SST of `writer` as being written until the guard drops.
    fn start(&self, writer: Writer) -> Active<'_> {
        if writer == Writer::Flush {
            self.state.lock().unwrap_or_else(|e| e.into_inner()).flushes += 1;
        }
        Active { lane: self, writer }
    }
}

impl LaneState {
    /// Whether `writer` may take a window now. A waiting flush counts itself in
    /// `flush_waiting`, and compaction waits for every waiting flush.
    fn admits(&self, writer: Writer) -> bool {
        self.used < MAX_WINDOWS
            && match writer {
                Writer::Flush => true,
                Writer::Compaction => {
                    self.compaction < self.compaction_windows() && self.flush_waiting == 0
                }
            }
    }

    /// Whether a writer that [`LaneState::admits`] must still wait for a running Raft log sync.
    /// `yielded` holds the `synced` count when this writer first waited for a sync. Once any
    /// sync has finished since then, it waits for no further sync.
    fn yields_to_sync(&self, yielded: &mut Option<u64>) -> bool {
        if self.syncs == 0 {
            return false;
        }
        match *yielded {
            Some(synced) if synced != self.synced => false,
            Some(_) => true,
            None => {
                *yielded = Some(self.synced);
                true
            }
        }
    }

    fn compaction_windows(&self) -> usize {
        if self.flushes > 0 {
            MAX_COMPACTION_WINDOWS
        } else {
            MAX_IDLE_COMPACTION_WINDOWS
        }
    }
}

/// Mark a Raft log sync as running until the guard drops. While it runs, the SST lane grants
/// no new window; windows already in writeback finish. The sync itself is not delayed or
/// skipped, so durability does not change.
pub(crate) fn raft_sync() -> RaftSync<'static> {
    LANE.sync()
}

/// A running Raft log sync.
pub(crate) struct RaftSync<'a> {
    lane: &'a Lane,
}

impl Drop for RaftSync<'_> {
    fn drop(&mut self) {
        let mut st = self.lane.state.lock().unwrap_or_else(|e| e.into_inner());
        st.syncs -= 1;
        st.synced += 1;
        drop(st);
        self.lane.freed.notify_all();
    }
}

/// One SST that is being written. A flush lowers the compaction share until it drops.
struct Active<'a> {
    lane: &'a Lane,
    writer: Writer,
}

impl fmt::Debug for Active<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Active")
            .field("writer", &self.writer)
            .finish()
    }
}

impl Drop for Active<'_> {
    fn drop(&mut self) {
        if self.writer != Writer::Flush {
            return;
        }
        self.lane
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .flushes -= 1;
        // A waiting compaction may now take a larger share.
        self.lane.freed.notify_all();
    }
}

/// One window of writeback budget, returned on drop.
struct Window<'a> {
    lane: &'a Lane,
    writer: Writer,
}

impl Drop for Window<'_> {
    fn drop(&mut self) {
        let mut st = self.lane.state.lock().unwrap_or_else(|e| e.into_inner());
        st.used -= 1;
        if self.writer == Writer::Compaction {
            st.compaction -= 1;
        }
        drop(st);
        // Flush and compaction waiters wait for different conditions.
        self.lane.freed.notify_all();
    }
}

/// The window in writeback of one writer.
struct Pacer<'a> {
    file: &'a File,
    path: &'a FsPath,
    lane: &'a Lane,
    writer: Writer,
    previous: Option<(u64, u64, Window<'a>)>,
}

impl<'a> Pacer<'a> {
    /// Wait for the previous window, then start writeback of `len` bytes at `start`.
    fn submit(&mut self, start: u64, len: u64) -> Result<()> {
        self.wait()?;
        let window = self.lane.acquire(self.writer);
        sync_range(self.file, start, len, libc::SYNC_FILE_RANGE_WRITE)
            .map_err(|e| io_error(e, self.path))?;
        self.previous = Some((start, len, window));
        Ok(())
    }

    /// Wait until the previous window is on the device, then release it.
    fn wait(&mut self) -> Result<()> {
        if let Some((start, size, _window)) = self.previous.take() {
            sync_range(
                self.file,
                start,
                size,
                libc::SYNC_FILE_RANGE_WAIT_BEFORE
                    | libc::SYNC_FILE_RANGE_WRITE
                    | libc::SYNC_FILE_RANGE_WAIT_AFTER,
            )
            .map_err(|e| io_error(e, self.path))?;
        }
        Ok(())
    }
}

/// Write `data` at `offset`, one window in writeback at a time.
///
/// A window ends with each chunk of `data`, or at `PACE_BYTES` during a write backlog. The next
/// window is copied into the page cache while the previous one is on the device.
fn write_paced(
    file: &File,
    mut offset: u64,
    data: &PutPayload,
    path: &FsPath,
    lane: &Lane,
    writer: Writer,
    backlog: &AtomicBool,
) -> Result<()> {
    let mut pacer = Pacer {
        file,
        path,
        lane,
        writer,
        previous: None,
    };
    let mut start = offset;
    for chunk in data.iter() {
        let mut chunk = chunk.as_ref();
        while !chunk.is_empty() {
            let len = chunk.len().min((PACE_BYTES - (offset - start)) as usize);
            file.write_all_at(&chunk[..len], offset)
                .map_err(|e| io_error(e, path))?;
            offset += len as u64;
            chunk = &chunk[len..];
            if offset - start == PACE_BYTES || !backlog.load(Ordering::Relaxed) {
                pacer.submit(start, offset - start)?;
                start = offset;
            }
        }
    }
    if offset > start {
        pacer.submit(start, offset - start)?;
    }
    pacer.wait()
}

/// Sync the staged file, then atomically publish it and sync its directory.
fn publish(file: File, staged: &FsPath, dest: &FsPath) -> Result<()> {
    super::local::publish(file, staged, dest).map_err(|e| io_error(e, dest))
}

#[async_trait]
impl ObjectStore for PacedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        // A put may replace an SST that reads have open.
        self.reads.evict(location);
        let result = self.put_opts_inner(location, payload, opts).await;
        self.reads.evict(location);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.reads.evict(location);
        self.put_multipart_opts_inner(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if !sst_reads::serves(location, &options) {
            return self.inner.get_opts(location, options).await;
        }
        let path = self.inner.path_to_filesystem(location)?;
        self.reads.get(location, path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        use futures::StreamExt;
        // Close descriptors before the delete and again after it, so a read that opened the
        // path in between cannot keep a deleted SST open.
        let (before, after) = (self.reads.clone(), self.reads.clone());
        let paths = paths
            .inspect(move |path| {
                if let Ok(path) = path {
                    before.evict(path);
                }
            })
            .boxed();
        self.inner
            .delete_stream(paths)
            .inspect(move |path| {
                if let Ok(path) = path {
                    after.evict(path);
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
        self.reads.evict(to);
        let result = self.inner.copy_opts(from, to, options).await;
        self.reads.evict(to);
        result
    }
}

impl PacedStore {
    async fn put_opts_inner(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if !Self::paced(location, &opts.mode) {
            return self.inner.put_opts(location, payload, opts).await;
        }
        let dest = self.inner.path_to_filesystem(location)?;
        let writer = Writer::of(&opts.extensions);
        let (lane, backlog) = (self.lane, self.backlog.clone());
        let active = lane.start(writer);
        tokio::task::spawn_blocking(move || {
            let _active = active;
            let (file, staged) = stage(&dest)?;
            let result = write_paced(&file, 0, &payload, &staged, lane, writer, &backlog)
                .and_then(|_| publish(file, &staged, &dest));
            if result.is_err() {
                let _ = std::fs::remove_file(&staged);
            }
            result
        })
        .await
        .map_err(join_error)??;
        self.result(location).await
    }

    async fn put_multipart_opts_inner(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        if !location.as_ref().ends_with(".sst") {
            return self.inner.put_multipart_opts(location, opts).await;
        }
        let dest = self.inner.path_to_filesystem(location)?;
        let writer = Writer::of(&opts.extensions);
        let (file, staged) = {
            let dest = dest.clone();
            tokio::task::spawn_blocking(move || stage(&dest))
                .await
                .map_err(join_error)??
        };
        Ok(Box::new(PacedUpload {
            active: Some(Arc::new(self.lane.start(writer))),
            lane: self.lane,
            backlog: self.backlog.clone(),
            store: self.inner.clone(),
            reads: self.reads.clone(),
            location: location.clone(),
            dest,
            staged,
            file: Arc::new(Mutex::new(Some(Arc::new(file)))),
            offset: 0,
            writer,
        }))
    }
}

#[derive(Debug)]
struct PacedUpload {
    /// Held until the upload completes, aborts, or drops, and by each part until its blocking
    /// write exits. A cancelled part's write keeps running after its task is gone.
    active: Option<Arc<Active<'static>>>,
    lane: &'static Lane,
    backlog: Arc<AtomicBool>,
    store: Arc<LocalFileSystem>,
    reads: Arc<SstReads>,
    location: Path,
    dest: PathBuf,
    staged: PathBuf,
    file: Arc<Mutex<Option<Arc<File>>>>,
    // Parts can finish out of order, so each one reserves its offset when it starts.
    offset: u64,
    writer: Writer,
}

impl PacedUpload {
    fn file(&self) -> Result<Arc<File>> {
        self.file
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| Error::Generic {
                store: "PacedStore",
                source: "upload already finished".into(),
            })
    }
}

#[async_trait]
impl MultipartUpload for PacedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let offset = self.offset;
        self.offset += data.content_length() as u64;
        let file = self.file();
        let staged = self.staged.clone();
        let (lane, writer, backlog) = (self.lane, self.writer, self.backlog.clone());
        let active = self.active.clone();
        Box::pin(async move {
            let file = file?;
            tokio::task::spawn_blocking(move || {
                let _active = active;
                write_paced(&file, offset, &data, &staged, lane, writer, &backlog)
            })
            .await
            .map_err(join_error)?
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let file = self.file.lock().unwrap().take();
        let file = file.ok_or_else(|| Error::Generic {
            store: "PacedStore",
            source: "upload already finished".into(),
        })?;
        let file = Arc::try_unwrap(file).map_err(|_| Error::Generic {
            store: "PacedStore",
            source: "upload completed before its parts".into(),
        })?;
        let (staged, dest) = (self.staged.clone(), self.dest.clone());
        let published = tokio::task::spawn_blocking(move || publish(file, &staged, &dest))
            .await
            .map_err(join_error)
            .and_then(|r| r);
        self.active = None;
        self.reads.evict(&self.location);
        published?;
        let meta = self.store.head(&self.location).await?;
        Ok(PutResult {
            e_tag: meta.e_tag,
            version: meta.version,
            extensions: Default::default(),
        })
    }

    async fn abort(&mut self) -> Result<()> {
        self.file.lock().unwrap().take();
        self.active = None;
        let staged = self.staged.clone();
        tokio::task::spawn_blocking(move || match std::fs::remove_file(&staged) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error(e, &staged)),
        })
        .await
        .map_err(join_error)?
    }
}

impl PacedStore {
    /// A SlateDB metrics recorder that turns backlog windows of this store on and off.
    ///
    /// SlateDB sets its unflushed memtable gauge before each write. The recorder compares it
    /// with [`BACKLOG_PERCENT`] of `max_unflushed_bytes`. It drops every other metric.
    pub(crate) fn backlog_recorder(
        &self,
        max_unflushed_bytes: usize,
    ) -> Arc<dyn slatedb_common::metrics::MetricsRecorder> {
        Arc::new(BacklogRecorder {
            gauge: Arc::new(UnflushedGauge {
                backlog: self.backlog.clone(),
                threshold: (max_unflushed_bytes / 100 * BACKLOG_PERCENT) as i64,
            }),
            noop: slatedb_common::metrics::NoopMetricsRecorder::new(),
        })
    }
}

struct BacklogRecorder {
    gauge: Arc<UnflushedGauge>,
    noop: slatedb_common::metrics::NoopMetricsRecorder,
}

struct UnflushedGauge {
    backlog: Arc<AtomicBool>,
    threshold: i64,
}

impl slatedb_common::metrics::GaugeFn for UnflushedGauge {
    fn set(&self, value: i64) {
        self.backlog
            .store(value >= self.threshold, Ordering::Relaxed);
    }
}

impl slatedb_common::metrics::MetricsRecorder for BacklogRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn slatedb_common::metrics::CounterFn> {
        self.noop.register_counter(name, description, labels)
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn slatedb_common::metrics::GaugeFn> {
        if name == slatedb::db_stats::TOTAL_MEM_SIZE_BYTES {
            return self.gauge.clone();
        }
        self.noop.register_gauge(name, description, labels)
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn slatedb_common::metrics::UpDownCounterFn> {
        self.noop
            .register_up_down_counter(name, description, labels)
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        boundaries: &[f64],
    ) -> Arc<dyn slatedb_common::metrics::HistogramFn> {
        self.noop
            .register_histogram(name, description, labels, boundaries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn paced_ssts_publish_whole_objects_and_hide_staged_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = PacedStore::new(
            LocalFileSystem::new_with_prefix(dir.path())
                .unwrap()
                .with_fsync(true),
        );
        let big: Vec<u8> = (0..(5 * PACE_BYTES / 2) as usize)
            .map(|i| (i % 251) as u8)
            .collect();
        let single = Path::from("state/compacted/a.sst");
        store.put(&single, big.clone().into()).await.unwrap();
        assert_eq!(
            store.get(&single).await.unwrap().bytes().await.unwrap(),
            big
        );

        let multi = Path::from("state/compacted/b.sst");
        let mut upload = store.put_multipart(&multi).await.unwrap();
        let (first, second) = big.split_at(PACE_BYTES as usize + 7);
        let p1 = upload.put_part(first.to_vec().into());
        let p2 = upload.put_part(second.to_vec().into());
        // Parts may finish in either order.
        futures::try_join!(p2, p1).unwrap();
        upload.complete().await.unwrap();
        assert_eq!(store.get(&multi).await.unwrap().bytes().await.unwrap(), big);

        // Compaction output takes the compaction share of the lane.
        let compacted = Path::from("state/compacted/d.sst");
        let tag = slatedb::object_store_tag::ObjectStoreCallTag::new(
            slatedb::object_store_tag::TableStoreKind::Compactor,
            slatedb::object_store_tag::SstType::Compacted,
        );
        let opts = PutMultipartOptions {
            extensions: tag.into(),
            ..Default::default()
        };
        let mut upload = store.put_multipart_opts(&compacted, opts).await.unwrap();
        let parts: Vec<_> = big
            .chunks(PACE_BYTES as usize / 2 + 3)
            .map(|part| upload.put_part(part.to_vec().into()))
            .collect();
        futures::future::try_join_all(parts).await.unwrap();
        upload.complete().await.unwrap();
        assert_eq!(
            store.get(&compacted).await.unwrap().bytes().await.unwrap(),
            big
        );

        let aborted = Path::from("state/compacted/c.sst");
        let mut upload = store.put_multipart(&aborted).await.unwrap();
        upload.put_part(vec![1; 10].into()).await.unwrap();
        upload.abort().await.unwrap();

        // Other objects keep the inner store's conditional writes.
        let manifest = Path::from("state/manifest/1.manifest");
        store
            .put_opts(&manifest, vec![1].into(), PutMode::Create.into())
            .await
            .unwrap();
        assert!(store
            .put_opts(&manifest, vec![2].into(), PutMode::Create.into())
            .await
            .is_err());

        let mut names: Vec<_> = store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .unwrap();
        names.sort();
        assert_eq!(
            names,
            [
                "state/compacted/a.sst",
                "state/compacted/b.sst",
                "state/compacted/d.sst",
                "state/manifest/1.manifest"
            ]
        );
        let staged: Vec<_> = std::fs::read_dir(dir.path().join("state/compacted"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains('#'))
            .collect();
        assert!(staged.is_empty(), "{staged:?}");
    }

    /// A part keeps its flush marked as running until its blocking write exits, even after its
    /// task is cancelled and the upload aborted. Otherwise compaction could take the window
    /// that a new flush needs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_flush_part_keeps_the_flush_active_until_it_exits() {
        let dir = tempfile::tempdir().unwrap();
        let lane: &'static Lane = Box::leak(Box::new(Lane::new()));
        let store =
            PacedStore::with_lane(LocalFileSystem::new_with_prefix(dir.path()).unwrap(), lane);
        // Every window is taken, so the part waits in the lane.
        let held: Vec<_> = (0..MAX_WINDOWS)
            .map(|_| lane.acquire(Writer::Flush))
            .collect();
        let path = Path::from("state/compacted/f.sst");
        let mut upload = store.put_multipart(&path).await.unwrap();
        let part = tokio::spawn(upload.put_part(vec![7; 10 << 20].into()));
        let started = std::time::Instant::now();
        while lane.state.lock().unwrap().flush_waiting == 0 {
            assert!(
                started.elapsed().as_secs() < 30,
                "part never reached the lane"
            );
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        part.abort();
        assert!(part.await.unwrap_err().is_cancelled());
        upload.abort().await.unwrap();
        drop(upload);
        // The detached write still runs, so the flush still counts.
        assert_eq!(lane.state.lock().unwrap().flushes, 1);
        assert_eq!(
            lane.state.lock().unwrap().compaction_windows(),
            MAX_COMPACTION_WINDOWS
        );

        drop(held);
        let started = std::time::Instant::now();
        loop {
            let counts = {
                let st = lane.state.lock().unwrap();
                (st.used, st.flush_waiting, st.flushes)
            };
            if counts == (0, 0, 0) {
                break;
            }
            assert!(started.elapsed().as_secs() < 30, "the part never finished");
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// Cached SST descriptors serve the bytes the path holds now, through overwrites, renames,
    /// and deletes, for inline, blocking, partial, and whole-object reads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sst_reads_follow_the_current_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = PacedStore::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let data = |seed: u8, len: usize| -> Vec<u8> {
            (0..len).map(|i| (i % 251) as u8 ^ seed).collect()
        };
        let path = Path::from("state/compacted/x.sst");
        let first = data(1, 3 * 1024 * 1024);
        store.put(&path, first.clone().into()).await.unwrap();
        for _ in 0..2 {
            // The first read opens the file; the second uses the cached descriptor.
            for range in [0..4096, 10..10_000, 5000..(1 << 20), 0..first.len() as u64] {
                let got = store.get_range(&path, range.clone()).await.unwrap();
                assert_eq!(&got[..], &first[range.start as usize..range.end as usize]);
            }
            let whole = store.get(&path).await.unwrap();
            assert_eq!(whole.meta.size, first.len() as u64);
            assert_eq!(whole.bytes().await.unwrap(), first);
        }
        // Ranges past the end behave as in the inner store: clamped, or an error from the end on.
        let len = first.len() as u64;
        assert_eq!(store.get_range(&path, 0..len + 1).await.unwrap(), first);
        let inner = store.inner.get_range(&path, 0..len + 1).await.unwrap();
        assert_eq!(inner, first);
        assert!(store.get_range(&path, len..len + 1).await.is_err());
        assert!(store.inner.get_range(&path, len..len + 1).await.is_err());

        // A replaced SST is read from its new file, through both put paths.
        let second = data(2, 8192);
        store.put(&path, second.clone().into()).await.unwrap();
        assert_eq!(store.get_range(&path, 0..8192).await.unwrap(), second);
        let third = data(3, 8192);
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload.put_part(third.clone().into()).await.unwrap();
        upload.complete().await.unwrap();
        assert_eq!(store.get_range(&path, 0..8192).await.unwrap(), third);

        // A copy or rename onto a cached path replaces it too.
        let other = Path::from("state/compacted/y.sst");
        let fourth = data(4, 8192);
        store.put(&other, fourth.clone().into()).await.unwrap();
        assert_eq!(store.get_range(&other, 0..8192).await.unwrap(), fourth);
        store.copy(&other, &path).await.unwrap();
        assert_eq!(store.get_range(&path, 0..8192).await.unwrap(), fourth);
        store.put(&other, second.clone().into()).await.unwrap();
        store.rename(&other, &path).await.unwrap();
        assert_eq!(store.get_range(&path, 0..8192).await.unwrap(), second);
        assert!(matches!(
            store.get_range(&other, 0..1).await,
            Err(Error::NotFound { .. })
        ));

        // A deleted SST is gone, and its descriptor is closed.
        store.delete(&path).await.unwrap();
        assert!(matches!(
            store.get_range(&path, 0..1).await,
            Err(Error::NotFound { .. })
        ));
        assert!(!store.reads.is_open(&path));

        // Preconditioned and head requests keep the inner store's handling.
        store.put(&path, first.clone().into()).await.unwrap();
        let meta = store.head(&path).await.unwrap();
        let opts = GetOptions {
            if_none_match: meta.e_tag.clone(),
            ..Default::default()
        };
        assert!(matches!(
            store.get_opts(&path, opts).await,
            Err(Error::NotModified { .. })
        ));
        // The cached metadata matches what the inner store reports.
        let cached = store.get(&path).await.unwrap().meta;
        assert_eq!((cached.size, &cached.e_tag), (meta.size, &meta.e_tag));
    }

    /// A read that opened a path before it changed does not cache its descriptor.
    #[test]
    fn sst_reads_drop_descriptors_opened_before_a_change() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("a.sst");
        std::fs::write(&file_path, b"old").unwrap();
        let reads = SstReads::default();
        let path = Path::from("a.sst");
        let generation = reads.generation();
        let file = File::open(&file_path).unwrap();
        let meta = ObjectMeta {
            location: path.clone(),
            last_modified: Default::default(),
            size: 3,
            e_tag: None,
            version: None,
        };
        reads.evict(&path);
        reads.insert_for_test(&path, file, meta, generation);
        assert!(!reads.is_open(&path));
    }

    #[test]
    fn lane_caps_windows_and_compaction_yields_to_flushes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let lane = Lane::new();
        // While a flush runs, compaction gets only its share.
        let flush = lane.start(Writer::Flush);
        let mut compactions: Vec<_> = (0..MAX_COMPACTION_WINDOWS)
            .map(|_| lane.acquire(Writer::Compaction))
            .collect();
        let extra_compaction = AtomicUsize::new(0);
        let flushed = AtomicUsize::new(0);
        let release = AtomicUsize::new(0);
        std::thread::scope(|s| {
            s.spawn(|| {
                let _w = lane.acquire(Writer::Compaction);
                extra_compaction.store(1, Ordering::SeqCst);
            });
            std::thread::sleep(Duration::from_millis(50));
            assert_eq!(
                extra_compaction.load(Ordering::SeqCst),
                0,
                "compaction exceeded its share"
            );
            // Flushes still get the rest of the lane.
            let flushes: Vec<_> = (MAX_COMPACTION_WINDOWS..MAX_WINDOWS)
                .map(|_| lane.acquire(Writer::Flush))
                .collect();
            // A waiting flush goes before the waiting compaction.
            s.spawn(|| {
                let _w = lane.acquire(Writer::Flush);
                flushed.store(1, Ordering::SeqCst);
                // A failed assertion of the main thread never sets `release`. The scope then
                // waits for this thread, so it stops after a bounded time.
                let started = std::time::Instant::now();
                while release.load(Ordering::SeqCst) == 0 && started.elapsed().as_secs() < 30 {
                    std::thread::sleep(Duration::from_millis(5));
                }
            });
            // Wait until the flush waits. A fixed sleep fails on a loaded host: the compaction
            // then takes the freed window first.
            let started = std::time::Instant::now();
            while lane.state.lock().unwrap().flush_waiting == 0 {
                assert!(started.elapsed().as_secs() < 30, "the flush never waited");
                std::thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(
                flushed.load(Ordering::SeqCst),
                0,
                "lane exceeded MAX_WINDOWS"
            );
            drop(compactions.pop());
            while flushed.load(Ordering::SeqCst) == 0 {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(
                extra_compaction.load(Ordering::SeqCst),
                0,
                "compaction took a window a flush was waiting for"
            );
            release.store(1, Ordering::SeqCst);
            drop((compactions, flushes));
        });
        drop(flush);
        let st = lane.state.lock().unwrap();
        assert_eq!(
            (st.used, st.compaction, st.flush_waiting, st.flushes),
            (0, 0, 0, 0)
        );
        drop(st);

        // Many writers of both kinds never exceed the caps and all finish.
        let lane = Lane::new();
        let flush = lane.start(Writer::Flush);
        std::thread::scope(|s| {
            for i in 0..16 {
                let writer = if i % 2 == 0 {
                    Writer::Flush
                } else {
                    Writer::Compaction
                };
                let lane = &lane;
                s.spawn(move || {
                    for _ in 0..50 {
                        let _w = lane.acquire(writer);
                        std::thread::yield_now();
                    }
                });
            }
        });
        drop(flush);
        let st = lane.state.lock().unwrap();
        assert_eq!((st.used, st.compaction, st.flush_waiting), (0, 0, 0));
        assert!(st.max_used <= MAX_WINDOWS, "{}", st.max_used);
        assert!(
            st.max_compaction <= MAX_COMPACTION_WINDOWS,
            "{}",
            st.max_compaction
        );
    }

    /// A full L0 stops every flush. Compaction then gets a larger share, and a flush that starts
    /// still gets a window without a wait.
    #[test]
    fn compaction_takes_the_idle_lane_and_a_new_flush_gets_a_window() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let lane = Lane::new();
        let mut compactions: Vec<_> = (0..MAX_IDLE_COMPACTION_WINDOWS)
            .map(|_| {
                lane.try_acquire(Writer::Compaction, Duration::from_secs(30))
                    .expect("idle compaction share")
            })
            .collect();
        // Even with no flush running, compaction leaves one window free.
        assert!(lane
            .try_acquire(Writer::Compaction, Duration::from_millis(50))
            .is_none());
        let flush = lane.start(Writer::Flush);
        // The flush gets the free window at once.
        let window = lane
            .try_acquire(Writer::Flush, Duration::from_millis(50))
            .expect("the reserved window");
        let extra_compaction = AtomicUsize::new(0);
        std::thread::scope(|s| {
            s.spawn(|| {
                let _w = lane.acquire(Writer::Compaction);
                extra_compaction.store(1, Ordering::SeqCst);
            });
            // Released compaction windows do not go back to compaction while the flush runs.
            while compactions.len() > MAX_COMPACTION_WINDOWS {
                drop(compactions.pop());
            }
            drop(window);
            std::thread::sleep(Duration::from_millis(50));
            assert_eq!(
                extra_compaction.load(Ordering::SeqCst),
                0,
                "compaction exceeded its share during a flush"
            );
            // When the flush ends, the waiting compaction gets the idle share.
            drop(flush);
            let started = std::time::Instant::now();
            while extra_compaction.load(Ordering::SeqCst) == 0 {
                assert!(started.elapsed().as_secs() < 30, "compaction never resumed");
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        drop(compactions);
        let st = lane.state.lock().unwrap();
        assert_eq!(
            (st.used, st.compaction, st.flush_waiting, st.flushes),
            (0, 0, 0, 0)
        );
        assert_eq!(st.max_compaction, MAX_IDLE_COMPACTION_WINDOWS);
    }

    /// Windows end with each chunk, so a Raft log sync waits behind a few SST blocks. During a
    /// write backlog they fill `PACE_BYTES`, across chunk and part boundaries.
    #[tokio::test]
    async fn backlog_windows_fill_pace_bytes() {
        use slatedb_common::metrics::MetricsRecorder;

        let dir = tempfile::tempdir().unwrap();
        let lane: &'static Lane = Box::leak(Box::new(Lane::new()));
        let store =
            PacedStore::with_lane(LocalFileSystem::new_with_prefix(dir.path()).unwrap(), lane);
        let block = 4096;
        let data: Vec<u8> = (0..(5 * PACE_BYTES / 2) as usize)
            .map(|i| (i % 251) as u8)
            .collect();
        let payload = || {
            data.chunks(block)
                .map(bytes::Bytes::copy_from_slice)
                .collect::<PutPayload>()
        };
        let windows = |lane: &Lane| std::mem::take(&mut lane.state.lock().unwrap().windows);

        let path = Path::from("state/compacted/a.sst");
        store.put(&path, payload()).await.unwrap();
        assert_eq!(windows(lane), data.len().div_ceil(block));

        // SlateDB reports its unflushed bytes through the recorder.
        let recorder = BacklogRecorder {
            gauge: Arc::new(UnflushedGauge {
                backlog: store.backlog.clone(),
                threshold: 100,
            }),
            noop: slatedb_common::metrics::NoopMetricsRecorder::new(),
        };
        let gauge = recorder.register_gauge(slatedb::db_stats::TOTAL_MEM_SIZE_BYTES, "", &[]);
        gauge.set(99);
        assert!(!store.backlog.load(Ordering::Relaxed));
        gauge.set(100);
        assert!(store.backlog.load(Ordering::Relaxed));

        store.put(&path, payload()).await.unwrap();
        assert_eq!(windows(lane), 3);
        assert_eq!(store.get(&path).await.unwrap().bytes().await.unwrap(), data);

        // Each multipart part paces its own range. Parts hold block-sized chunks and end off a
        // block boundary, as SlateDB parts do.
        let (first, second) = data.split_at(PACE_BYTES as usize + 7);
        let multipart = || async {
            let mut upload = store.put_multipart(&path).await.unwrap();
            for part in [first, second] {
                let part: PutPayload = part
                    .chunks(block)
                    .map(bytes::Bytes::copy_from_slice)
                    .collect();
                upload.put_part(part).await.unwrap();
            }
            upload.complete().await.unwrap();
            assert_eq!(store.get(&path).await.unwrap().bytes().await.unwrap(), data);
        };
        multipart().await;
        assert_eq!(windows(lane), 2 + 2);

        gauge.set(0);
        store.put(&path, payload()).await.unwrap();
        assert_eq!(windows(lane), data.len().div_ceil(block));
        multipart().await;
        assert_eq!(
            windows(lane),
            first.len().div_ceil(block) + second.len().div_ceil(block)
        );
    }

    /// Embedded members share one process and the lane. A small write on member B must not
    /// turn off the backlog windows of member A while A waits for its flush.
    #[tokio::test]
    async fn backlog_of_one_store_survives_another_stores_gauge() {
        let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let lane: &'static Lane = Box::leak(Box::new(Lane::new()));
        let store = |dir: &tempfile::TempDir| {
            PacedStore::with_lane(LocalFileSystem::new_with_prefix(dir.path()).unwrap(), lane)
        };
        let (a, b) = (store(&dir_a), store(&dir_b));
        let max = 100 << 20;
        let gauge = |store: &PacedStore| {
            store.backlog_recorder(max).register_gauge(
                slatedb::db_stats::TOTAL_MEM_SIZE_BYTES,
                "",
                &[],
            )
        };
        gauge(&a).set(max as i64 / 100 * 90);
        gauge(&b).set(max as i64 / 100 * 10);
        let data = vec![3_u8; PACE_BYTES as usize];
        let payload = || -> PutPayload {
            data.chunks(4096)
                .map(bytes::Bytes::copy_from_slice)
                .collect()
        };
        a.put(&Path::from("state/compacted/a.sst"), payload())
            .await
            .unwrap();
        assert_eq!(std::mem::take(&mut lane.state.lock().unwrap().windows), 1);
        // B still writes one block per window.
        b.put(&Path::from("state/compacted/b.sst"), payload())
            .await
            .unwrap();
        assert_eq!(lane.state.lock().unwrap().windows, data.len() / 4096);
    }

    /// A running Raft log sync holds back new windows. A writer waits for at most one sync, so
    /// back-to-back syncs cannot starve it.
    #[test]
    fn new_windows_wait_for_one_raft_sync() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::{Duration, Instant};

        let lane = Lane::new();
        let held = lane.acquire(Writer::Compaction);
        let sync = lane.sync();
        // A window already in writeback is untouched; a new one waits for the sync.
        assert!(lane
            .try_acquire(Writer::Flush, Duration::from_millis(50))
            .is_none());
        assert_eq!(lane.state.lock().unwrap().flush_waiting, 0);
        drop(held);
        let granted = AtomicUsize::new(0);
        std::thread::scope(|s| {
            s.spawn(|| {
                let _w = lane.acquire(Writer::Flush);
                granted.store(1, Ordering::SeqCst);
            });
            let started = Instant::now();
            while lane.state.lock().unwrap().flush_waiting == 0 {
                assert!(started.elapsed().as_secs() < 30, "the flush never waited");
                std::thread::sleep(Duration::from_millis(1));
            }
            std::thread::sleep(Duration::from_millis(20));
            assert_eq!(
                granted.load(Ordering::SeqCst),
                0,
                "window granted during a sync"
            );
            // The next sync starts before the waiter runs. It still gets its window.
            let next = lane.sync();
            drop(sync);
            while granted.load(Ordering::SeqCst) == 0 {
                assert!(started.elapsed().as_secs() < 30, "the writer starved");
                std::thread::sleep(Duration::from_millis(1));
            }
            drop(next);
        });
        // With no sync running, windows come at once.
        drop(
            lane.try_acquire(Writer::Flush, Duration::from_millis(50))
                .expect("window"),
        );
        let st = lane.state.lock().unwrap();
        assert_eq!(
            (st.used, st.flush_waiting, st.syncs, st.synced),
            (0, 0, 0, 2)
        );
    }

    #[test]
    fn lane_releases_windows_when_a_writer_panics() {
        let lane = Lane::new();
        std::thread::scope(|s| {
            for writer in [Writer::Flush, Writer::Compaction] {
                let lane = &lane;
                let r = s
                    .spawn(move || {
                        let _w = lane.acquire(writer);
                        panic!("writer failed mid-window");
                    })
                    .join();
                assert!(r.is_err());
            }
        });
        let st = lane.state.lock().unwrap();
        assert_eq!((st.used, st.compaction, st.flush_waiting), (0, 0, 0));
        drop(st);
        // The whole lane is usable again.
        let _all: Vec<_> = (0..MAX_WINDOWS)
            .map(|_| lane.acquire(Writer::Flush))
            .collect();
    }
}
