//! Read local SST ranges without a blocking-pool round trip.
//!
//! A SlateDB block cache miss reads one range of an SST through `get_opts` and then
//! `GetResult::bytes`. The local store runs each of them on the blocking pool: the first opens and
//! stats the file, the second reads it, and the file is opened again on the next miss. In a point
//! read benchmark over a data set larger than the block cache, these two hand-offs and their futex
//! wake-ups took about a third of the server CPU.
//!
//! SSTs are immutable once published, so this keeps their open descriptors and metadata. A range
//! read first tries `preadv2(RWF_NOWAIT)` on the calling task. That flag makes the kernel return
//! `EAGAIN` instead of waiting for the device, so a read is inline only when the page cache already
//! holds it. Other reads, and the first read of each SST, go to the blocking pool as before.
//!
//! Descriptors must not outlive the paths they were opened from. Every write, copy, or delete of an
//! SST path evicts it before and after the operation, and bumps a generation. A read that opened the
//! file during that window does not cache its descriptor, because the generation moved. A later
//! read opens the path again and sees what the inner store would see.
//!
//! Cached descriptors count against the open file limit, which the server shares with sockets,
//! Raft log files, and SlateDB writes. The cache holds at most a quarter of the soft limit. If an
//! open still fails because no descriptor is free, the cache closes its descriptors, halves its
//! cap, and opens the file again.

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    Attributes, Error, GetOptions, GetResult, GetResultPayload, ObjectMeta, Result,
};

/// Most open SSTs kept per store. A sorted run has a few dozen SSTs at most; past the cap, an
/// arbitrary descriptor is closed and reopened on its next read. [`max_open`] lowers the cap
/// under a low open file limit.
const MAX_OPEN: usize = 1024;
/// The cap never drops below this after descriptors run out.
const MIN_OPEN: usize = 16;
/// Longest range read on the calling task. Compaction read-ahead and whole-object reads copy more
/// and go to the blocking pool.
const MAX_INLINE: u64 = 256 * 1024;

pub(crate) fn is_sst(location: &Path) -> bool {
    location.as_ref().ends_with(".sst")
}

/// The reads this cache serves: plain range or whole-object reads without preconditions.
pub(crate) fn serves(location: &Path, options: &GetOptions) -> bool {
    is_sst(location)
        && !options.head
        && options.if_match.is_none()
        && options.if_none_match.is_none()
        && options.if_modified_since.is_none()
        && options.if_unmodified_since.is_none()
        && options.version.is_none()
}

struct Open {
    file: File,
    meta: ObjectMeta,
}

#[derive(Default)]
struct Files {
    generation: u64,
    open: HashMap<Path, Arc<Open>>,
}

pub(crate) struct SstReads {
    files: RwLock<Files>,
    max_open: AtomicUsize,
}

impl Default for SstReads {
    fn default() -> Self {
        Self::with_max_open(max_open(crate::fdlimit::soft()))
    }
}

/// The cache cap for a soft open file limit: a quarter of it, at most [`MAX_OPEN`].
fn max_open(soft_limit: Option<u64>) -> usize {
    match soft_limit {
        Some(limit) => (limit / 4).clamp(1, MAX_OPEN as u64) as usize,
        None => MAX_OPEN,
    }
}

impl std::fmt::Debug for SstReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstReads").finish_non_exhaustive()
    }
}

impl SstReads {
    fn with_max_open(max_open: usize) -> Self {
        Self {
            files: RwLock::default(),
            max_open: AtomicUsize::new(max_open),
        }
    }

    /// Forget `location`. Call before and after any change to that path.
    pub(crate) fn evict(&self, location: &Path) {
        if !is_sst(location) {
            return;
        }
        let mut files = self.files.write().unwrap_or_else(|e| e.into_inner());
        files.generation += 1;
        files.open.remove(location);
    }

    fn lookup(&self, location: &Path) -> (Option<Arc<Open>>, u64) {
        let files = self.files.read().unwrap_or_else(|e| e.into_inner());
        (files.open.get(location).cloned(), files.generation)
    }

    fn insert(&self, location: &Path, open: Arc<Open>, generation: u64) {
        let mut files = self.files.write().unwrap_or_else(|e| e.into_inner());
        if files.generation != generation {
            return;
        }
        let max_open = self.max_open.load(Ordering::Relaxed);
        while files.open.len() >= max_open && !files.open.contains_key(location) {
            let victim = files.open.keys().next().cloned();
            match victim {
                Some(victim) => files.open.remove(&victim),
                None => break,
            };
        }
        files.open.insert(location.clone(), open);
    }

    /// Open `path`. If no descriptor is free, close the cached ones, halve the cap, and try once
    /// more. Reads in flight keep their descriptors until they finish.
    fn open(
        &self,
        path: &std::path::Path,
        open: impl Fn(&std::path::Path) -> std::io::Result<File>,
    ) -> std::io::Result<File> {
        match open(path) {
            Err(error) if crate::fdlimit::exhausted(&error) => {
                let closed = self.shed();
                tracing::warn!(
                    %error,
                    closed,
                    max_open = self.max_open.load(Ordering::Relaxed),
                    "No free file descriptor for an SST read; closed cached SST descriptors"
                );
                open(path)
            }
            result => result,
        }
    }

    /// Close every cached descriptor and halve the cap. Returns how many closed.
    fn shed(&self) -> usize {
        let mut files = self.files.write().unwrap_or_else(|e| e.into_inner());
        let closed = files.open.len();
        files.open.clear();
        let _ = self
            .max_open
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |max| {
                Some((max / 2).max(MIN_OPEN).min(max))
            });
        closed
    }

    #[cfg(test)]
    pub(crate) fn is_open(&self, location: &Path) -> bool {
        self.lookup(location).0.is_some()
    }

    #[cfg(test)]
    pub(crate) fn open_count(&self) -> usize {
        self.files
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .open
            .len()
    }

    #[cfg(test)]
    pub(crate) fn generation(&self) -> u64 {
        self.lookup(&Path::from("")).1
    }

    #[cfg(test)]
    pub(crate) fn insert_for_test(
        &self,
        location: &Path,
        file: File,
        meta: ObjectMeta,
        generation: u64,
    ) {
        self.insert(location, Arc::new(Open { file, meta }), generation);
    }

    /// Serve a read that [`serves`] accepted. `path` is the file behind `location`.
    pub(crate) async fn get(
        self: &Arc<Self>,
        location: &Path,
        path: std::path::PathBuf,
        options: GetOptions,
    ) -> Result<GetResult> {
        let (open, generation) = self.lookup(location);
        if let Some(open) = open {
            let range = range(&options, &open.meta)?;
            let len = range.end - range.start;
            let mut buf = vec![0; len as usize];
            let mut filled = 0;
            if len <= MAX_INLINE {
                filled = read_cached(&open.file, &mut buf, range.start)
                    .map_err(|e| read_error(e, location))?;
            }
            if filled < buf.len() {
                let (open, location) = (open.clone(), location.clone());
                buf = tokio::task::spawn_blocking(move || {
                    open.file
                        .read_exact_at(&mut buf[filled..], range.start + filled as u64)
                        .map(|_| buf)
                        .map_err(|e| read_error(e, &location))
                })
                .await
                .map_err(join_error)??;
            }
            return Ok(result(open.meta.clone(), range, buf));
        }
        let reads = self.clone();
        let location = location.clone();
        tokio::task::spawn_blocking(move || {
            let file = reads
                .open(&path, |path| File::open(path))
                .map_err(|e| open_error(e, &location))?;
            let metadata = file.metadata().map_err(|e| read_error(e, &location))?;
            let meta = convert_metadata(&metadata, location.clone());
            let range = range(&options, &meta)?;
            let mut buf = vec![0; (range.end - range.start) as usize];
            file.read_exact_at(&mut buf, range.start)
                .map_err(|e| read_error(e, &location))?;
            reads.insert(
                &location,
                Arc::new(Open {
                    file,
                    meta: meta.clone(),
                }),
                generation,
            );
            Ok(result(meta, range, buf))
        })
        .await
        .map_err(join_error)?
    }
}

fn range(options: &GetOptions, meta: &ObjectMeta) -> Result<std::ops::Range<u64>> {
    match &options.range {
        Some(r) => r.as_range(meta.size).map_err(|source| Error::Generic {
            store: "PacedStore",
            source: format!("{}: {source}", meta.location).into(),
        }),
        None => Ok(0..meta.size),
    }
}

fn result(meta: ObjectMeta, range: std::ops::Range<u64>, buf: Vec<u8>) -> GetResult {
    let bytes = Bytes::from(buf);
    GetResult {
        payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
            async move { Ok(bytes) },
        ))),
        attributes: Attributes::default(),
        range,
        meta,
        extensions: Default::default(),
    }
}

/// Read what the page cache holds of `buf` at `offset`, without waiting for the device.
/// Returns the bytes read; fewer than `buf.len()` means the rest must be read with blocking IO.
#[cfg(target_env = "gnu")]
fn read_cached(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::fd::AsRawFd;
    let mut filled = 0;
    while filled < buf.len() {
        let rest = &mut buf[filled..];
        let iov = libc::iovec {
            iov_base: rest.as_mut_ptr().cast(),
            iov_len: rest.len(),
        };
        // SAFETY: `iov` points into `buf`, which outlives the call, and the descriptor is open.
        let n = unsafe {
            libc::preadv2(
                file.as_raw_fd(),
                &iov,
                1,
                (offset + filled as u64) as libc::off_t,
                libc::RWF_NOWAIT,
            )
        };
        if n > 0 {
            filled += n as usize;
            continue;
        }
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let error = std::io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => continue,
            // Not cached, or the kernel or file system does not support the flag.
            Some(libc::EAGAIN)
            | Some(libc::EOPNOTSUPP)
            | Some(libc::ENOSYS)
            | Some(libc::EINVAL) => return Ok(filled),
            _ => return Err(error),
        }
    }
    Ok(filled)
}

#[cfg(not(target_env = "gnu"))]
fn read_cached(_: &File, _: &mut [u8], _: u64) -> std::io::Result<usize> {
    Ok(0)
}

/// The metadata `LocalFileSystem` reports for a file.
fn convert_metadata(metadata: &std::fs::Metadata, location: Path) -> ObjectMeta {
    let modified = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .unwrap_or_default();
    let last_modified =
        chrono::DateTime::from_timestamp(modified.as_secs() as i64, modified.subsec_nanos())
            .unwrap_or_default();
    let size = metadata.len();
    ObjectMeta {
        location,
        last_modified,
        size,
        e_tag: Some(format!(
            "\"{:x}-{:x}-{:x}\"",
            metadata.ino(),
            modified.as_micros(),
            size
        )),
        version: None,
    }
}

fn open_error(source: std::io::Error, location: &Path) -> Error {
    if source.kind() == std::io::ErrorKind::NotFound {
        Error::NotFound {
            path: location.to_string(),
            source: source.into(),
        }
    } else {
        read_error(source, location)
    }
}

fn read_error(source: std::io::Error, location: &Path) -> Error {
    Error::Generic {
        store: "PacedStore",
        source: format!("{location}: {source}").into(),
    }
}

fn join_error(source: tokio::task::JoinError) -> Error {
    Error::Generic {
        store: "PacedStore",
        source: source.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_open_is_a_quarter_of_the_limit() {
        assert_eq!(max_open(Some(1024)), 256);
        assert_eq!(max_open(Some(256)), 64);
        assert_eq!(max_open(Some(2)), 1);
        assert_eq!(max_open(Some(65536)), MAX_OPEN);
        assert_eq!(max_open(Some(u64::MAX)), MAX_OPEN);
        assert_eq!(max_open(None), MAX_OPEN);
    }

    /// The cache stays under its cap. When no descriptor is free, an open closes the cached
    /// descriptors, halves the cap, and tries again instead of failing the read.
    #[test]
    fn sst_reads_shed_descriptors_when_none_are_free() {
        let dir = tempfile::tempdir().unwrap();
        let reads = SstReads::with_max_open(64);
        let meta = |path: &Path| ObjectMeta {
            location: path.clone(),
            last_modified: Default::default(),
            size: 1,
            e_tag: None,
            version: None,
        };
        for i in 0..100 {
            let file_path = dir.path().join(format!("{i}.sst"));
            std::fs::write(&file_path, b"x").unwrap();
            let path = Path::from(format!("{i}.sst"));
            let file = File::open(&file_path).unwrap();
            reads.insert_for_test(&path, file, meta(&path), reads.generation());
        }
        assert_eq!(reads.open_count(), 64);

        // The first open fails with EMFILE, the retry succeeds.
        let file_path = dir.path().join("0.sst");
        let failed = std::sync::atomic::AtomicBool::new(false);
        let file = reads.open(&file_path, |path| {
            if !failed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                return Err(std::io::Error::from_raw_os_error(libc::EMFILE));
            }
            File::open(path)
        });
        assert!(file.is_ok());
        assert_eq!(reads.open_count(), 0);
        assert_eq!(reads.max_open.load(Ordering::Relaxed), 32);

        // Other errors pass through and keep the cache.
        let path = Path::from("a.sst");
        reads.insert_for_test(
            &path,
            File::open(&file_path).unwrap(),
            meta(&path),
            reads.generation(),
        );
        let missing = reads.open(&dir.path().join("missing.sst"), |p| File::open(p));
        assert_eq!(missing.unwrap_err().kind(), std::io::ErrorKind::NotFound);
        assert_eq!(reads.open_count(), 1);

        // Repeated exhaustion stops at the floor, and a second EMFILE reaches the caller.
        for _ in 0..10 {
            let err = reads
                .open(&file_path, |_| {
                    Err(std::io::Error::from_raw_os_error(libc::EMFILE))
                })
                .unwrap_err();
            assert_eq!(err.raw_os_error(), Some(libc::EMFILE));
        }
        assert_eq!(reads.max_open.load(Ordering::Relaxed), 16);
    }
}
