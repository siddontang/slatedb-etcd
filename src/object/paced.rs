//! Write local SSTs durably without flooding the disk queue.
//!
//! A synced `LocalFileSystem` writes a whole SST to the page cache and then calls `fsync`. A
//! 64 MiB L0 SST then reaches the disk in one burst, and Raft log syncs queue behind it. This
//! store starts writeback one `PACE_BYTES` window at a time, as RocksDB `bytes_per_sync` does.
//! A final sync, rename, and directory sync then publish the SST. Other objects, such as
//! manifests, use the synced inner store and keep its conditional writes.
//!
//! SlateDB uploads a large SST as 10 MiB multipart parts, up to 8 at a time, and flushes and
//! compactions run concurrently. Pacing each writer alone still left dozens of windows in
//! writeback at once. The block layer's writeback throttling then blocked SST writers and the
//! Raft log thread alike, and every Raft `fdatasync` waited behind SST pages. So all SST writers
//! in the process share one [`Lane`] of `MAX_WINDOWS` windows. Compaction output holds at most
//! `MAX_COMPACTION_WINDOWS` of them and yields to waiting memtable flushes, which writers wait
//! on when memory fills up.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use async_trait::async_trait;
use futures::stream::BoxStream;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path;
use slatedb::object_store::*;

const PACE_BYTES: u64 = 1024 * 1024;
/// SST windows in writeback across the process. About 4 MiB in flight keeps a SATA SSD busy
/// with SST data while a Raft log sync waits behind only a few milliseconds of it.
const MAX_WINDOWS: usize = 4;
/// Windows compaction output may hold. One keeps compaction moving while memtable flushes get
/// the rest; with two, flush fell behind under 4 KiB-value writes and immutable memtables piled
/// up in memory.
const MAX_COMPACTION_WINDOWS: usize = 1;

#[derive(Debug)]
pub(crate) struct PacedStore {
    inner: Arc<LocalFileSystem>,
}

impl PacedStore {
    /// `inner` must sync its own writes. Only SSTs bypass it.
    pub fn new(inner: LocalFileSystem) -> Self {
        Self {
            inner: Arc::new(inner),
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
    let mut id = 1_u64;
    loop {
        let mut name = dest.as_os_str().to_owned();
        name.push(format!("#{id}"));
        let path = PathBuf::from(name);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(source) => match source.kind() {
                ErrorKind::AlreadyExists => id += 1,
                ErrorKind::NotFound => {
                    let parent = dest.parent().expect("object path has a parent");
                    std::fs::create_dir_all(parent).map_err(|e| io_error(e, parent))?;
                    sync_dir(parent)?;
                    if let Some(grand) = parent.parent() {
                        sync_dir(grand)?;
                    }
                }
                _ => return Err(io_error(source, &path)),
            },
        }
    }
}

fn sync_dir(dir: &FsPath) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_error(e, dir))
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
struct Lane {
    state: Mutex<LaneState>,
    freed: Condvar,
}

#[derive(Default)]
struct LaneState {
    used: usize,
    compaction: usize,
    flush_waiting: usize,
    #[cfg(test)]
    max_used: usize,
    #[cfg(test)]
    max_compaction: usize,
}

static LANE: Lane = Lane::new();

impl Lane {
    const fn new() -> Self {
        Self {
            state: Mutex::new(LaneState {
                used: 0,
                compaction: 0,
                flush_waiting: 0,
                #[cfg(test)]
                max_used: 0,
                #[cfg(test)]
                max_compaction: 0,
            }),
            freed: Condvar::new(),
        }
    }

    fn acquire(&self, writer: Writer) -> Window<'_> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match writer {
            Writer::Flush => {
                st.flush_waiting += 1;
                while st.used >= MAX_WINDOWS {
                    st = self.freed.wait(st).unwrap_or_else(|e| e.into_inner());
                }
                st.flush_waiting -= 1;
            }
            Writer::Compaction => {
                while st.used >= MAX_WINDOWS
                    || st.compaction >= MAX_COMPACTION_WINDOWS
                    || st.flush_waiting > 0
                {
                    st = self.freed.wait(st).unwrap_or_else(|e| e.into_inner());
                }
                st.compaction += 1;
            }
        }
        st.used += 1;
        #[cfg(test)]
        {
            st.max_used = st.max_used.max(st.used);
            st.max_compaction = st.max_compaction.max(st.compaction);
        }
        Window { lane: self, writer }
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

/// Write `data` at `offset`, one `PACE_BYTES` window in writeback at a time.
///
/// The next window is copied into the page cache while the previous one is on the device.
fn write_paced(
    file: &File,
    mut offset: u64,
    data: &PutPayload,
    path: &FsPath,
    writer: Writer,
) -> Result<()> {
    let mut previous: Option<(u64, u64, Window<'_>)> = None;
    let wait = |previous: &mut Option<(u64, u64, Window<'_>)>| -> Result<()> {
        if let Some((start, size, _window)) = previous.take() {
            sync_range(
                file,
                start,
                size,
                libc::SYNC_FILE_RANGE_WAIT_BEFORE
                    | libc::SYNC_FILE_RANGE_WRITE
                    | libc::SYNC_FILE_RANGE_WAIT_AFTER,
            )
            .map_err(|e| io_error(e, path))?;
        }
        Ok(())
    };
    for chunk in data.iter() {
        let mut chunk = chunk.as_ref();
        while !chunk.is_empty() {
            let len = chunk.len().min(PACE_BYTES as usize);
            file.write_all_at(&chunk[..len], offset)
                .map_err(|e| io_error(e, path))?;
            wait(&mut previous)?;
            let window = LANE.acquire(writer);
            sync_range(file, offset, len as u64, libc::SYNC_FILE_RANGE_WRITE)
                .map_err(|e| io_error(e, path))?;
            previous = Some((offset, len as u64, window));
            offset += len as u64;
            chunk = &chunk[len..];
        }
    }
    wait(&mut previous)
}

/// Sync the staged file, then atomically publish it and sync its directory.
fn publish(file: File, staged: &FsPath, dest: &FsPath) -> Result<()> {
    file.sync_all().map_err(|e| io_error(e, staged))?;
    drop(file);
    std::fs::rename(staged, dest).map_err(|e| io_error(e, dest))?;
    sync_dir(dest.parent().expect("object path has a parent"))
}

#[async_trait]
impl ObjectStore for PacedStore {
    async fn put_opts(
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
        tokio::task::spawn_blocking(move || {
            let (file, staged) = stage(&dest)?;
            let result = write_paced(&file, 0, &payload, &staged, writer)
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

    async fn put_multipart_opts(
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
            store: self.inner.clone(),
            location: location.clone(),
            dest,
            staged,
            file: Arc::new(Mutex::new(Some(Arc::new(file)))),
            offset: 0,
            writer,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
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

#[derive(Debug)]
struct PacedUpload {
    store: Arc<LocalFileSystem>,
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
        let writer = self.writer;
        Box::pin(async move {
            let file = file?;
            tokio::task::spawn_blocking(move || write_paced(&file, offset, &data, &staged, writer))
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
        tokio::task::spawn_blocking(move || publish(file, &staged, &dest))
            .await
            .map_err(join_error)??;
        let meta = self.store.head(&self.location).await?;
        Ok(PutResult {
            e_tag: meta.e_tag,
            version: meta.version,
            extensions: Default::default(),
        })
    }

    async fn abort(&mut self) -> Result<()> {
        self.file.lock().unwrap().take();
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

    #[test]
    fn lane_caps_windows_and_compaction_yields_to_flushes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let lane = Lane::new();
        // Compaction alone gets only its share.
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
                while release.load(Ordering::SeqCst) == 0 {
                    std::thread::sleep(Duration::from_millis(5));
                }
            });
            std::thread::sleep(Duration::from_millis(50));
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
        let st = lane.state.lock().unwrap();
        assert_eq!((st.used, st.compaction, st.flush_waiting), (0, 0, 0));
        drop(st);

        // Many writers of both kinds never exceed the caps and all finish.
        let lane = Lane::new();
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
        let st = lane.state.lock().unwrap();
        assert_eq!((st.used, st.compaction, st.flush_waiting), (0, 0, 0));
        assert!(st.max_used <= MAX_WINDOWS, "{}", st.max_used);
        assert!(
            st.max_compaction <= MAX_COMPACTION_WINDOWS,
            "{}",
            st.max_compaction
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
