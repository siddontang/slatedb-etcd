//! A local object store with conditional updates, and the staging rules of local files.
//!
//! `LocalFileSystem` rejects `PutMode::Update`, and SlateDB advances its GC boundary files with
//! it. `LocalStore` implements the update with an `flock` lock and an atomic rename. Processes
//! that share one POSIX filesystem then see one order of updates. NFS is not supported.
//!
//! `LocalStore` does its filesystem work on blocking threads only. `bounded::Bounded` adds the
//! deadline and the limit of calls in flight.

use std::fmt;
use std::fs::{File, Metadata, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result,
};

/// The longest wait of an update for the lock of its object.
const LOCK_WAIT: Duration = Duration::from_secs(30);
/// The largest read of an object body on one blocking thread.
const READ_CHUNK: u64 = 4 << 20;
/// The mtime steps after the previous version. A coarse filesystem can round a small step back
/// to the previous mtime. HFS+ keeps whole seconds, and FAT keeps two seconds.
const MTIME_STEPS: [Duration; 4] = [
    Duration::from_micros(1),
    Duration::from_millis(1),
    Duration::from_secs(1),
    Duration::from_secs(2),
];

/// Set the mtime of a file.
type SetMtime = fn(&File, SystemTime) -> std::io::Result<()>;

/// A synced `LocalFileSystem` that also accepts `PutMode::Update`.
///
/// An update holds an exclusive `flock` on the lock file of its object while it compares the
/// e-tag and renames a new file over the object. `PutMode::Create` needs no lock: its hard link
/// fails while the object exists, and an update needs the object to exist. Deletes and
/// overwrites are not ordered against updates. SlateDB does neither on its boundary files.
#[derive(Debug)]
pub(crate) struct LocalStore {
    inner: LocalFileSystem,
    lock_wait: Duration,
    /// Tests replace it to model a filesystem with coarse timestamps.
    set_mtime: SetMtime,
}

impl LocalStore {
    /// Open a store at `root`, and create the directory if it is missing.
    pub(crate) fn open(root: &std::path::Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(root)?;
        let inner = LocalFileSystem::new_with_prefix(root)?.with_fsync(true);
        Ok(Self {
            inner,
            lock_wait: LOCK_WAIT,
            set_mtime: File::set_modified,
        })
    }
}

impl fmt::Display for LocalStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let PutMode::Update(version) = opts.mode else {
            return self.inner.put_opts(location, payload, opts).await;
        };
        if !opts.attributes.is_empty() {
            return Err(Error::NotImplemented {
                operation: "`put_opts` with `opts.attributes` specified".into(),
                implementer: self.to_string(),
            });
        }
        let target = self.inner.path_to_filesystem(location)?;
        let location = location.to_string();
        let deadline = Instant::now() + self.lock_wait;
        let set_mtime = self.set_mtime;
        tokio::task::spawn_blocking(move || {
            let expected = version.e_tag.as_deref();
            match update(&target, &payload, expected, deadline, set_mtime) {
                Ok(e_tag) => Ok(PutResult {
                    e_tag: Some(e_tag),
                    version: None,
                    extensions: Default::default(),
                }),
                Err(Update::Changed(reason)) => Err(Error::Precondition {
                    path: location,
                    source: reason.into(),
                }),
                Err(Update::Io(source)) => Err(io_error(source, &target)),
            }
        })
        .await?
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // `LocalFileSystem` creates the staging file when its future is first polled. Run that
        // on a blocking thread, not on a runtime thread.
        let inner = self.inner.clone();
        let location = location.clone();
        tokio::task::spawn_blocking(move || {
            futures::executor::block_on(inner.put_multipart_opts(&location, opts))
        })
        .await?
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let mut result = self.inner.get_opts(location, options).await?;
        let empty = GetResultPayload::Stream(futures::stream::empty().boxed());
        result.payload = match std::mem::replace(&mut result.payload, empty) {
            GetResultPayload::File(file, path) => {
                GetResultPayload::Stream(body(file, path, result.range.clone()))
            }
            stream => stream,
        };
        Ok(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

fn io_error(source: std::io::Error, path: &std::path::Path) -> Error {
    Error::Generic {
        store: "LocalStore",
        source: format!("{}: {source}", path.display()).into(),
    }
}

/// Read `range` of `file` in chunks. Each chunk is one read on a blocking thread.
fn body(file: File, path: PathBuf, range: Range<u64>) -> BoxStream<'static, Result<Bytes>> {
    futures::stream::try_unfold((file, path, range), |(mut file, path, range)| async move {
        if range.is_empty() {
            return Ok(None);
        }
        let end = range.end.min(range.start + READ_CHUNK);
        let (chunk, file, path) =
            tokio::task::spawn_blocking(move || (read(&mut file, range.start..end), file, path))
                .await?;
        let chunk = chunk.map_err(|source| io_error(source, &path))?;
        Ok(Some((chunk, (file, path, end..range.end))))
    })
    .boxed()
}

fn read(file: &mut File, range: Range<u64>) -> std::io::Result<Bytes> {
    let len = range.end - range.start;
    file.seek(SeekFrom::Start(range.start))?;
    let mut buffer = Vec::with_capacity(len.try_into().unwrap_or_default());
    file.take(len).read_to_end(&mut buffer)?;
    if buffer.len() as u64 != len {
        return Err(std::io::Error::new(
            ErrorKind::UnexpectedEof,
            "the file ended inside the range",
        ));
    }
    Ok(buffer.into())
}

/// The reason that an update failed.
enum Update {
    /// The object is missing or has another e-tag.
    Changed(String),
    Io(std::io::Error),
}

impl From<std::io::Error> for Update {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Replace `target` with `payload` if its e-tag is `expected`, and return the new e-tag.
fn update(
    target: &std::path::Path,
    payload: &PutPayload,
    expected: Option<&str>,
    deadline: Instant,
    set_mtime: SetMtime,
) -> std::result::Result<String, Update> {
    let _lock = match lock(target, deadline) {
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(Update::Changed("the object does not exist".into()))
        }
        lock => lock?,
    };
    let current = match std::fs::metadata(target) {
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(Update::Changed("the object does not exist".into()))
        }
        current => current?,
    };
    let found = e_tag(&current);
    if expected != Some(found.as_str()) {
        return Err(Update::Changed(format!(
            "{found} does not match {expected:?}"
        )));
    }
    let (mut file, staged) = stage(target)?;
    let replaced = (|| -> std::io::Result<String> {
        for chunk in payload.iter() {
            file.write_all(chunk)?;
        }
        follow(&file, micros(&current), set_mtime)?;
        let e_tag = e_tag(&file.metadata()?);
        publish(file, &staged, target)?;
        Ok(e_tag)
    })();
    if replaced.is_err() {
        let _ = std::fs::remove_file(&staged);
    }
    Ok(replaced?)
}

/// Give `file` an mtime after `previous`, in microseconds. A stale e-tag then cannot match a
/// later version, even when the new file reuses an old inode. Fail if every step of
/// `MTIME_STEPS` reads back at or before `previous`. The old object then stays.
fn follow(file: &File, previous: u128, set_mtime: SetMtime) -> std::io::Result<()> {
    if micros(&file.metadata()?) > previous {
        return Ok(());
    }
    let base = UNIX_EPOCH + Duration::from_micros(u64::try_from(previous).unwrap_or(u64::MAX));
    for step in MTIME_STEPS {
        set_mtime(file, base + step)?;
        if micros(&file.metadata()?) > previous {
            return Ok(());
        }
    }
    Err(std::io::Error::other(
        "the filesystem keeps the mtime of the previous version",
    ))
}

/// Take the exclusive lock of `target`, or fail at `deadline`. The lock file name ends in `#0`.
/// `LocalFileSystem` lists no file whose name ends in `#` and digits, and staging names start
/// at `#1`.
fn lock(target: &std::path::Path, deadline: Instant) -> std::io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(sibling(target, 0))?;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(std::io::Error::new(
                    ErrorKind::TimedOut,
                    "timed out: another update holds the lock",
                ));
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        }
    }
}

/// Create a new staging file beside `target`, with the naming of `LocalFileSystem`. Create and
/// sync missing parent directories.
pub(crate) fn stage(target: &std::path::Path) -> std::io::Result<(File, PathBuf)> {
    let mut n = 1;
    loop {
        let path = sibling(target, n);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => n += 1,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let parent = target.parent().ok_or(error)?;
                std::fs::create_dir_all(parent)?;
                sync_dir(parent)?;
                if let Some(grand) = parent.parent() {
                    sync_dir(grand)?;
                }
            }
            Err(error) => return Err(error),
        }
    }
}

/// Sync the staged file, then rename it over `target` and sync the directory.
pub(crate) fn publish(
    file: File,
    staged: &std::path::Path,
    target: &std::path::Path,
) -> std::io::Result<()> {
    file.sync_all()?;
    drop(file);
    std::fs::rename(staged, target)?;
    match target.parent() {
        Some(parent) => sync_dir(parent),
        None => Ok(()),
    }
}

fn sync_dir(dir: &std::path::Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

fn sibling(target: &std::path::Path, n: u64) -> PathBuf {
    let mut path = target.as_os_str().to_owned();
    path.push(format!("#{n}"));
    path.into()
}

/// The e-tag that `LocalFileSystem` 0.14 reports: the inode, the mtime in microseconds, and
/// the size.
fn e_tag(metadata: &Metadata) -> String {
    #[cfg(unix)]
    let inode = std::os::unix::fs::MetadataExt::ino(metadata);
    #[cfg(not(unix))]
    let inode = 0;
    format!("\"{inode:x}-{:x}-{:x}\"", micros(metadata), metadata.len())
}

fn micros(metadata: &Metadata) -> u128 {
    metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(SystemTime::UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros()
}

#[cfg(test)]
mod tests;
