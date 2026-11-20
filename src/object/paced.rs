//! Write local SSTs durably without one large writeback burst.
//!
//! A synced `LocalFileSystem` writes a whole SST to the page cache and then calls `fsync`. A
//! 64 MiB L0 SST then reaches the disk in one burst, and Raft log syncs queue behind it. This
//! store starts writeback every `PACE_BYTES` and waits for the previous range, as RocksDB
//! `bytes_per_sync` does. A final sync, rename, and directory sync then publish the SST. Other
//! objects, such as manifests, use the synced inner store and keep its conditional writes.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream::BoxStream;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path;
use slatedb::object_store::*;

const PACE_BYTES: u64 = 1024 * 1024;

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

/// Write `data` at `offset` and keep at most two `PACE_BYTES` ranges in writeback.
fn write_paced(file: &File, mut offset: u64, data: &PutPayload, path: &FsPath) -> Result<()> {
    let mut previous: Option<(u64, u64)> = None;
    for chunk in data.iter() {
        let mut chunk = chunk.as_ref();
        while !chunk.is_empty() {
            let len = chunk.len().min(PACE_BYTES as usize);
            file.write_all_at(&chunk[..len], offset)
                .map_err(|e| io_error(e, path))?;
            sync_range(file, offset, len as u64, libc::SYNC_FILE_RANGE_WRITE)
                .map_err(|e| io_error(e, path))?;
            if let Some((start, size)) = previous {
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
            previous = Some((offset, len as u64));
            offset += len as u64;
            chunk = &chunk[len..];
        }
    }
    Ok(())
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
        tokio::task::spawn_blocking(move || {
            let (file, staged) = stage(&dest)?;
            let result = write_paced(&file, 0, &payload, &staged)
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
        Box::pin(async move {
            let file = file?;
            tokio::task::spawn_blocking(move || write_paced(&file, offset, &data, &staged))
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
}
