//! A local object store with conditional updates.
//!
//! `LocalFileSystem` rejects `PutMode::Update`, and SlateDB advances its GC boundary files with
//! it. `LocalStore` implements the update with an `flock` lock and an atomic rename. Processes
//! that share one POSIX filesystem then see one order of updates. NFS is not supported.
//!
//! Each call returns an error after `TIMEOUT`, like an S3 request. A hung filesystem call then
//! fails a SlateDB task and does not stop it forever.

use std::fmt;
use std::fs::{File, Metadata, OpenOptions};
use std::future::Future;
use std::io::{ErrorKind, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result, UploadPart,
};

/// The limit of one call. A synced SST write takes far less on a working disk.
const TIMEOUT: Duration = Duration::from_secs(30);

/// A synced `LocalFileSystem` that also accepts `PutMode::Update`.
///
/// An update holds an exclusive `flock` on the lock file of its object while it compares the
/// e-tag and renames a new file over the object. `PutMode::Create` needs no lock: its hard link
/// fails while the object exists, and an update needs the object to exist. Deletes and
/// overwrites are not ordered against updates. SlateDB does neither on its boundary files.
///
/// A call that passes `timeout` returns an error. Its blocking filesystem work can continue.
#[derive(Debug)]
pub(crate) struct LocalStore {
    inner: LocalFileSystem,
    timeout: Duration,
}

impl LocalStore {
    /// Open a store at `root`, and create the directory if it is missing.
    pub(crate) fn open(root: &std::path::Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(root)?;
        let inner = LocalFileSystem::new_with_prefix(root)?.with_fsync(true);
        Ok(Self {
            inner,
            timeout: TIMEOUT,
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
            return bound(self.timeout, self.inner.put_opts(location, payload, opts)).await;
        };
        if !opts.attributes.is_empty() {
            return Err(Error::NotImplemented {
                operation: "`put_opts` with `opts.attributes` specified".into(),
                implementer: self.to_string(),
            });
        }
        let target = self.inner.path_to_filesystem(location)?;
        let location = location.to_string();
        let deadline = Instant::now() + self.timeout;
        // The blocking task owns the lock, so a dropped future cannot release it early.
        let update = tokio::task::spawn_blocking(move || {
            update(&target, &payload, version.e_tag.as_deref(), deadline).map_err(|error| {
                match error {
                    Update::Changed(reason) => Error::Precondition {
                        path: location,
                        source: reason.into(),
                    },
                    Update::Io(source) => Error::Generic {
                        store: "LocalStore",
                        source: Box::new(source),
                    },
                }
            })
        });
        bound(self.timeout, async { update.await? }).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let inner = bound(self.timeout, self.inner.put_multipart_opts(location, opts)).await?;
        Ok(Box::new(Upload {
            inner,
            timeout: self.timeout,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        bound(self.timeout, self.inner.get_opts(location, options)).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        bound(self.timeout, self.inner.get_ranges(location, ranges)).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        bound_stream(self.timeout, self.inner.delete_stream(locations))
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        bound_stream(self.timeout, self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        bound_stream(self.timeout, self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        bound(self.timeout, self.inner.list_with_delimiter(prefix)).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        bound(self.timeout, self.inner.copy_opts(from, to, options)).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        bound(self.timeout, self.inner.rename_opts(from, to, options)).await
    }
}

/// A multipart upload whose calls have a timeout.
#[derive(Debug)]
struct Upload {
    inner: Box<dyn MultipartUpload>,
    timeout: Duration,
}

#[async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        Box::pin(bound(self.timeout, self.inner.put_part(data)))
    }

    async fn complete(&mut self) -> Result<PutResult> {
        bound(self.timeout, self.inner.complete()).await
    }

    async fn abort(&mut self) -> Result<()> {
        bound(self.timeout, self.inner.abort()).await
    }
}

/// Run `call`, or fail when it passes `limit`.
async fn bound<T>(limit: Duration, call: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(limit, call)
        .await
        .unwrap_or_else(|_| Err(timed_out(limit)))
}

/// Give each item of `stream` the time `limit`. A late item ends the stream with an error.
fn bound_stream<T: Send + 'static>(
    limit: Duration,
    stream: BoxStream<'static, Result<T>>,
) -> BoxStream<'static, Result<T>> {
    futures::stream::unfold(Some(stream), move |stream| async move {
        let mut stream = stream?;
        match tokio::time::timeout(limit, stream.next()).await {
            Ok(item) => item.map(|item| (item, Some(stream))),
            Err(_) => Some((Err(timed_out(limit)), None)),
        }
    })
    .boxed()
}

fn timed_out(limit: Duration) -> Error {
    Error::Generic {
        store: "LocalStore",
        source: format!("the local filesystem call timed out after {limit:?}").into(),
    }
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
) -> std::result::Result<PutResult, Update> {
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
    let (file, staged) = stage(target)?;
    // A later version gets a later mtime. A stale e-tag then cannot match again, even when a new
    // file reuses an old inode within one clock tick.
    let e_tag =
        publish(file, &staged, target, payload, micros(&current) + 1).inspect_err(|_| {
            let _ = std::fs::remove_file(&staged);
        })?;
    Ok(PutResult {
        e_tag: Some(e_tag),
        version: None,
        extensions: Default::default(),
    })
}

/// Write `payload` to the staging file, give it an mtime of at least `floor` microseconds,
/// sync it, and rename it over `target`. Return its e-tag.
fn publish(
    mut file: File,
    staged: &std::path::Path,
    target: &std::path::Path,
    payload: &PutPayload,
    floor: u128,
) -> std::io::Result<String> {
    for chunk in payload.iter() {
        file.write_all(chunk)?;
    }
    if micros(&file.metadata()?) < floor {
        let floor = u64::try_from(floor).unwrap_or(u64::MAX);
        file.set_modified(UNIX_EPOCH + Duration::from_micros(floor))?;
    }
    file.sync_all()?;
    let e_tag = e_tag(&file.metadata()?);
    std::fs::rename(staged, target)?;
    if let Some(parent) = target.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(e_tag)
}

/// Take the exclusive lock of `target`, or fail at `deadline`. The lock file name ends in `#0`.
/// `LocalFileSystem` lists no file whose name ends in `#` and digits, and it stages uploads from
/// `#1`.
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

/// Create a new staging file beside `target`, with the naming of `LocalFileSystem`.
fn stage(target: &std::path::Path) -> std::io::Result<(File, PathBuf)> {
    for n in 1.. {
        let path = sibling(target, n);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    unreachable!("the staging names are unbounded")
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
mod tests {
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

    /// A hung read and a held update lock both fail at the timeout.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_hung_call_fails_at_the_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LocalStore::open(dir.path()).unwrap();
        store.timeout = Duration::from_millis(200);
        let fails_in_time = |result: Result<()>, started: Instant| {
            let error = result.unwrap_err();
            assert!(error.to_string().contains("timed out"), "{error}");
            assert!(started.elapsed() < Duration::from_secs(2));
        };
        // An open of a FIFO waits for a writer, like a hung filesystem.
        let fifo = dir.path().join("fifo");
        let made = std::process::Command::new("mkfifo").arg(&fifo).status();
        assert!(made.unwrap().success());
        let started = Instant::now();
        fails_in_time(store.get(&Path::from("fifo")).await.map(drop), started);
        // Release the blocked open, so that the runtime can stop.
        drop(OpenOptions::new().write(true).open(&fifo).unwrap());

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
        fails_in_time(blocked.map(drop), started);
        drop(held);
        store
            .put_opts(&path, "2".into(), update_to(created.e_tag))
            .await
            .unwrap();
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
}
