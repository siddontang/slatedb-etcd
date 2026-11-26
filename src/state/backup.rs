//! Native backups: stream a pinned state as a native image, and restore an image with bounded
//! memory. See `docs/administration.md#maintenance-and-storage`.
//!
//! The image is the legacy Raft snapshot format, `SnapshotData` in `codec` encoding, followed by
//! its SHA-256 digest. Its rows are in key order, and the membership row comes last.
//!
//! A replicated backup pins a SlateDB checkpoint. A shared-mode backup pins a view. See `view`.

use super::*;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use slatedb::config::{CheckpointOptions, CheckpointScope, DbReaderOptions, ScanOptions};
use slatedb::{DbReader, DbReaderMode};
use std::io::{BufReader, Read};
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

#[cfg(test)]
mod tests;
mod view;
#[cfg(test)]
mod view_tests;

/// The bytes of one Snapshot response message.
const CHUNK_BYTES: usize = 64 * 1024;
/// The messages buffered ahead of a slow client.
const CHUNKS_AHEAD: usize = 16;
/// A checkpoint of a failed process expires after this time. A running backup refreshes it.
const CHECKPOINT_LIFETIME: Duration = Duration::from_secs(10 * 60);
const CHECKPOINT_NAME: &str = "native-backup";
/// `etcdctl snapshot save` accepts a file of a multiple of this size plus the digest.
const ALIGN: u64 = 512;
const DIGEST: u64 = 32;
/// The largest image that a shared-mode restore reads into memory.
const MAX_SHARED_RESTORE_BYTES: usize = 64 * 1024 * 1024;
/// The largest encoded row of an image. A restore decodes rows up to this size, and a backup
/// rejects a larger row before it publishes the image. A Raft entry holds at most 8 MiB, but
/// the auth row holds every user and role and can grow beyond that.
pub(super) const MAX_ROW_BYTES: usize = 64 * 1024 * 1024;
/// The row bytes of one restore write batch.
const RESTORE_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// The row bytes a restore writes between flushes. Without a flush, SlateDB keeps up to
/// `max_unflushed_bytes` in memtables. This matches the L0 SST size.
const RESTORE_FLUSH_BYTES: usize = 64 * 1024 * 1024;
/// A data directory with this file holds an interrupted restore. Startup rejects it.
pub(crate) const RESTORE_MARKER: &str = "restore-incomplete";

/// One part of a native image, and the image bytes after it.
pub struct Chunk {
    pub blob: Vec<u8>,
    pub remaining: u64,
}

/// A backup above a limit: the row size, or the holder limits of `overlay::Holders`. The
/// Snapshot RPC returns ResourceExhausted for it.
#[derive(Debug)]
pub(crate) struct TooLarge(String);

impl std::fmt::Display for TooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TooLarge {}

/// The status of a failed backup: ResourceExhausted above a size limit. Other errors are
/// storage reads, at the start or during the stream. They return Unavailable, as maintenance
/// reads do, so clients retry them.
pub(crate) fn backup_error(error: anyhow::Error) -> crate::mvcc::KvError {
    match error.downcast_ref::<TooLarge>() {
        Some(TooLarge(message)) => {
            crate::mvcc::KvError::new(tonic::Code::ResourceExhausted, message)
        }
        None => crate::mvcc::KvError::new(
            tonic::Code::Unavailable,
            &format!("snapshot failed: {error}"),
        ),
    }
}

/// Pad the membership JSON with spaces so that `etcdctl snapshot save` accepts the image. It
/// requires a multiple of 512 bytes plus the digest. `before` is the image size before the row.
pub(super) fn pad_members(members: &mut Vec<u8>, before: u64) -> Result<()> {
    let size = before + encoded_size(&(crate::cluster::KEY, &*members))?;
    let padding = (ALIGN - size % ALIGN) % ALIGN;
    members.resize(members.len() + padding as usize, b' ');
    Ok(())
}

/// A started backup: the response header of its applied boundary and the image chunks.
pub struct Stream {
    pub header: Option<crate::proto::etcdserverpb::ResponseHeader>,
    chunks: mpsc::Receiver<Chunk>,
    /// The end of the image: `Ok` after its last chunk, or the error that ended it early. The
    /// sender sets it before it closes `chunks`. `None` after a successful end arrived.
    end: Option<oneshot::Receiver<Result<()>>>,
    ended: bool,
}

/// The sending side of a `Stream`.
type Sender = (mpsc::Sender<Chunk>, oneshot::Sender<Result<()>>);

impl Stream {
    fn new(header: Option<crate::proto::etcdserverpb::ResponseHeader>) -> (Self, Sender) {
        let (tx, chunks) = mpsc::channel(CHUNKS_AHEAD);
        let (end_tx, end) = oneshot::channel();
        let stream = Self {
            header,
            chunks,
            end: Some(end),
            ended: false,
        };
        (stream, (tx, end_tx))
    }

    /// Return the next chunk, the error that ended the stream, or `None` after the image.
    ///
    /// An error comes before the chunks that are still buffered. So a stream that ended early
    /// never looks like a complete image, also to a client that reads it later. A sender that
    /// went away without an end also ended the stream early.
    pub async fn recv(&mut self) -> Option<Result<Chunk>> {
        if self.ended {
            return None;
        }
        let next = match &mut self.end {
            Some(end) => tokio::select! {
                biased;
                end = end => match end {
                    Ok(Ok(())) => {
                        self.end = None;
                        self.chunks.recv().await
                    }
                    end => return Some(Err(self.fail(end))),
                },
                chunk = self.chunks.recv() => chunk,
            },
            None => self.chunks.recv().await,
        };
        if let Some(chunk) = next {
            return Some(Ok(chunk));
        }
        self.ended = true;
        match self.end.take().map(|mut end| end.try_recv()) {
            None | Some(Ok(Ok(()))) => None,
            Some(Ok(Err(error))) => Some(Err(error)),
            Some(Err(_)) => Some(Err(anyhow::anyhow!("the backup stream stopped early"))),
        }
    }

    fn fail(&mut self, end: Result<Result<()>, oneshot::error::RecvError>) -> anyhow::Error {
        self.ended = true;
        self.end = None;
        self.chunks.close();
        match end {
            Ok(Err(error)) => error,
            _ => anyhow::anyhow!("the backup stream stopped early"),
        }
    }
}

impl StateMachine {
    /// Pin the applied prefix in a SlateDB checkpoint, then stream its image from a task.
    ///
    /// Call it between apply jobs. Only the checkpoint start runs here. Writes that apply later
    /// do not reach the checkpoint, so the image holds one applied state boundary.
    pub(crate) async fn backup(&self) -> Result<Stream> {
        self.backup_with_lifetime(CHECKPOINT_LIFETIME).await
    }

    /// Like `backup`, with a checkpoint that expires after `lifetime` unless refreshed.
    pub(super) async fn backup_with_lifetime(&self, lifetime: Duration) -> Result<Stream> {
        if let Some(shared) = &self.shared {
            return self.shared_backup(shared);
        }
        ensure!(
            self.db().snapshot()?.seq() == self.applied,
            "backup does not match the apply marker"
        );
        // The membership row can exist only in memory. See `members_unsaved`.
        let members = serde_json::to_vec(self.members.as_ref())?;
        let handle = self
            .db()
            .begin_checkpoint(
                CheckpointScope::All,
                &CheckpointOptions {
                    lifetime: Some(lifetime),
                    name: Some(CHECKPOINT_NAME.to_owned()),
                    ..Default::default()
                },
            )
            .await?;
        let timer = self.metrics.snapshot.start_timer();
        let source = Source {
            _db: self.db().clone(),
            admin: self.admin.clone(),
            store: self.location.store(),
            root: self.location.root(),
            image: Rows {
                applied: self.applied,
                members,
                omit: omitted,
            },
        };
        let (stream, (tx, end)) = Stream::new(mvcc::header(self.revision));
        tokio::spawn(async move {
            let checkpoint = match handle.wait().await {
                Ok(checkpoint) => checkpoint.id,
                Err(error) => {
                    let _ = end.send(Err(error.into()));
                    return;
                }
            };
            // A client that stops reading blocks the stream, so refresh the checkpoint apart
            // from it until the stream ends. A client that goes away ends the stream, even
            // while an object read retries.
            let result = tokio::select! {
                result = source.stream(checkpoint, &tx) => result,
                error = keep(&source.admin, checkpoint, lifetime) => Err(error),
                () = tx.closed() => Err(anyhow::anyhow!("the snapshot client went away")),
            };
            // Like etcd, the backend snapshot duration counts completed streams only.
            if result.is_ok() {
                timer.observe_duration();
            } else {
                timer.stop_and_discard();
            }
            // End the response before the cleanup. The checkpoint expires by itself after
            // `lifetime`, so the deletion stops retrying then.
            let _ = end.send(result);
            drop(tx);
            match tokio::time::timeout(lifetime, source.admin.delete_checkpoint(checkpoint)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "cannot delete a backup checkpoint"),
                Err(_) => tracing::warn!("timed out deleting a backup checkpoint"),
            }
        });
        Ok(stream)
    }
}

struct Source {
    /// A replaced database keeps its objects while this handle exists. See `Location::retire`.
    _db: Arc<Db>,
    admin: Arc<slatedb::admin::Admin>,
    store: Arc<dyn slatedb::object_store::ObjectStore>,
    root: String,
    image: Rows,
}

impl Source {
    async fn stream(&self, checkpoint: uuid::Uuid, tx: &mpsc::Sender<Chunk>) -> Result<()> {
        let reader = DbReader::builder(self.root.as_str(), self.store.clone())
            .with_reader_mode(DbReaderMode::Checkpoint(checkpoint))
            .with_options(DbReaderOptions {
                skip_wal_replay: true,
                // Like the member's reads, end the stream on a persistent read error. The
                // default retries forever while the checkpoint stays pinned.
                object_store_max_retries: slatedb_settings().object_store_max_retries,
                ..Default::default()
            })
            // One sequential pass does not reuse blocks. Keep the member caches for its reads.
            .with_db_cache_disabled()
            .build()
            .await?;
        let result = self.image.copy(&reader, tx, None).await;
        let _ = reader.close().await;
        result
    }
}

/// The parts of an image that do not come from the stored rows.
struct Rows {
    applied: u64,
    /// The membership JSON in memory. The image writes it as its last row.
    members: Vec<u8>,
    /// Return true for a stored row that the image leaves out.
    omit: fn(&[u8]) -> bool,
}

impl Rows {
    /// Send the image of `state` in two passes. The row count precedes the rows, so the first
    /// pass counts them and checks their sizes before the first chunk. The caller pins `state`,
    /// so it does not change. With `idle`, the stream ends when a chunk waits that long for the
    /// client.
    async fn copy(
        &self,
        state: &impl StateRead,
        tx: &mpsc::Sender<Chunk>,
        idle: Option<Duration>,
    ) -> Result<()> {
        let header = (crate::SNAPSHOT_FORMAT, self.applied);
        let mut rows = 0_u64;
        let mut size = encoded_size(&header)? + encoded_size(&0_u64)?;
        let mut scan = rows_of(state).await?;
        let mut read = 0_u64;
        while let Some(row) = scan.next().await? {
            read += 1;
            cooperate(read).await;
            if !(self.omit)(&row.key) {
                rows += 1;
                size += row_size(&row.key, &row.value)?;
            }
        }
        let mut members = self.members.clone();
        pad_members(&mut members, size)?;
        size += row_size(crate::cluster::KEY, &members)?;
        let mut image = Image {
            tx,
            idle,
            buffer: Vec::with_capacity(2 * CHUNK_BYTES),
            hash: Sha256::new(),
            remaining: size + DIGEST,
        };
        image.write(&header).await?;
        image.write(&(rows + 1)).await?;
        let mut scan = rows_of(state).await?;
        let mut written = 0_u64;
        let mut read = 0_u64;
        while let Some(row) = scan.next().await? {
            read += 1;
            cooperate(read).await;
            if !(self.omit)(&row.key) {
                written += 1;
                image.write(&(row.key, row.value)).await?;
            }
        }
        ensure!(written == rows, "backup state changed during the stream");
        image.write(&(crate::cluster::KEY, members)).await?;
        image.finish().await
    }
}

/// Yield to the runtime every 1024 rows. Rows that come from memory or cached blocks never wait,
/// so without this a pass would hold the thread, and a cancelled stream would run on.
async fn cooperate(rows: u64) {
    if rows.is_multiple_of(1024) {
        tokio::task::yield_now().await;
    }
}

async fn rows_of(state: &impl StateRead) -> Result<impl StateScan + '_> {
    let options = ScanOptions {
        cache_blocks: false,
        ..Default::default()
    };
    Ok(state.scan_under(b"", &options).await?)
}

/// Return true for the rows that the image leaves out. The membership row is written last, and
/// format 4 readers reject the derived meta rows. See `derived_key`.
fn omitted(key: &[u8]) -> bool {
    key == crate::cluster::KEY || derived_key(key)
}

/// The encoded size of a row. Reject a row that a restore cannot decode.
pub(super) fn row_size(key: &[u8], value: &[u8]) -> Result<u64> {
    let size = encoded_size(&(key, value))?;
    if size > MAX_ROW_BYTES as u64 {
        return Err(TooLarge(format!(
            "native snapshot row {:?} has {size} bytes, above the restore limit of {} MiB",
            String::from_utf8_lossy(&key[..key.len().min(64)]),
            MAX_ROW_BYTES >> 20
        ))
        .into());
    }
    Ok(size)
}

fn encoded_size(value: &impl serde::Serialize) -> Result<u64> {
    Ok(crate::codec::write(&mut std::io::sink(), value)? as u64)
}

/// Refresh the backup checkpoint every tenth of its lifetime. Return only on an error.
async fn keep(
    admin: &slatedb::admin::Admin,
    checkpoint: uuid::Uuid,
    lifetime: Duration,
) -> anyhow::Error {
    loop {
        tokio::time::sleep(lifetime / 10).await;
        if let Err(error) = admin.refresh_checkpoint(checkpoint, Some(lifetime)).await {
            return error.into();
        }
    }
}

/// Encode the image into chunks, then send its digest.
struct Image<'a> {
    tx: &'a mpsc::Sender<Chunk>,
    /// The longest time that a chunk waits for the client, if any.
    idle: Option<Duration>,
    buffer: Vec<u8>,
    hash: Sha256,
    remaining: u64,
}

impl Image<'_> {
    async fn write(&mut self, value: &impl serde::Serialize) -> Result<()> {
        let start = self.buffer.len();
        crate::codec::write(&mut self.buffer, value)?;
        self.hash.update(&self.buffer[start..]);
        let mut sent = 0;
        while self.buffer.len() - sent >= CHUNK_BYTES {
            let blob = self.buffer[sent..sent + CHUNK_BYTES].to_vec();
            sent += CHUNK_BYTES;
            self.send(blob).await?;
        }
        self.buffer.drain(..sent);
        Ok(())
    }

    async fn send(&mut self, blob: Vec<u8>) -> Result<()> {
        self.remaining = self
            .remaining
            .checked_sub(blob.len() as u64)
            .context("backup image exceeds its counted size")?;
        let chunk = Chunk {
            blob,
            remaining: self.remaining,
        };
        let send = self.tx.send(chunk);
        let sent = match self.idle {
            Some(idle) => tokio::time::timeout(idle, send).await.map_err(|_| {
                anyhow::anyhow!("the snapshot client took no chunk for {idle:?}; backup too slow")
            })?,
            None => send.await,
        };
        sent.map_err(|_| anyhow::anyhow!("the snapshot client went away"))
    }

    async fn finish(mut self) -> Result<()> {
        let mut blob = std::mem::take(&mut self.buffer);
        blob.extend_from_slice(&self.hash.clone().finalize());
        for part in blob.chunks(CHUNK_BYTES) {
            self.send(part.to_vec()).await?;
        }
        ensure!(
            self.remaining == 0,
            "backup image is shorter than its counted size"
        );
        Ok(())
    }
}

/// Read a native image file row by row, and check its digest at the end.
struct ImageFile {
    payload: Hashing<std::io::Take<BufReader<std::fs::File>>>,
    version: u32,
    rows: u64,
    read: u64,
}

impl ImageFile {
    fn open(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        ensure!(len > DIGEST, "snapshot is missing its checksum");
        let payload = Hashing {
            inner: BufReader::with_capacity(1024 * 1024, file).take(len - DIGEST),
            hash: Sha256::new(),
        };
        Ok(Self {
            payload,
            version: 0,
            rows: 0,
            read: 0,
        })
    }

    fn header(&mut self) -> Result<()> {
        let (version, _applied, rows): (u32, u64, u64) =
            crate::codec::read::<_, MAX_ROW_BYTES>(&mut self.payload)
                .context("invalid native snapshot")?;
        ensure!(
            crate::readable_snapshot(version),
            "unsupported snapshot version"
        );
        self.version = version;
        self.rows = rows;
        Ok(())
    }

    /// Decode a row. `Bytes` reads each field at once, where `Vec<u8>` reads it bytewise.
    fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        if self.read == self.rows {
            return Ok(None);
        }
        self.read += 1;
        let row = crate::codec::read::<_, MAX_ROW_BYTES>(&mut self.payload)
            .context("invalid native snapshot")?;
        Ok(Some(row))
    }

    /// Read the rest of the file. Check its digest, then that the rows filled the payload.
    /// Return the digest.
    fn finish(mut self, rows: Result<()>) -> Result<[u8; DIGEST as usize]> {
        let trailing = std::io::copy(&mut self.payload, &mut std::io::sink())?;
        let mut checksum = [0; DIGEST as usize];
        self.payload.inner.into_inner().read_exact(&mut checksum)?;
        ensure!(
            self.payload.hash.finalize().as_slice() == checksum,
            "snapshot checksum mismatch"
        );
        rows?;
        ensure!(trailing == 0, "trailing native snapshot data");
        Ok(checksum)
    }
}

struct Hashing<R> {
    inner: R,
    hash: Sha256,
}

impl<R: Read> Read for Hashing<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hash.update(&buf[..n]);
        Ok(n)
    }
}

/// Return true for the rows that a restore replaces instead of copying.
fn replaced(key: &[u8]) -> bool {
    key == APPLIED || derived_key(key) || key == crate::cluster::KEY
}

/// A native image file that passed `check`.
pub(super) struct Checked {
    path: PathBuf,
    /// The number of rows that a restore copies.
    copied: u64,
    digest: [u8; DIGEST as usize],
}

/// Check every row in one pass.
pub(super) fn check(path: &Path) -> Result<Checked> {
    let mut image = ImageFile::open(path)?;
    let mut copied = 0;
    let rows = (|| {
        image.header()?;
        let mut last: Option<Bytes> = None;
        while let Some((key, value)) = image.next()? {
            ensure!(snapshot_key(&key), "invalid snapshot key");
            if replaced(&key) {
                continue;
            }
            // Images hold rows in key order, so order also rules out duplicates.
            ensure!(
                last.as_ref().is_none_or(|last| *last < key),
                "duplicate or unordered snapshot key"
            );
            restored(&key, value, 0)?;
            last = Some(key);
            copied += 1;
        }
        Ok(())
    })();
    let digest = image.finish(rows)?;
    Ok(Checked {
        path: path.to_owned(),
        copied,
        digest,
    })
}

/// The rows of one restore batch, with the image to read on or, at its end, its digest.
type Batch = (
    Vec<(Bytes, Bytes)>,
    std::result::Result<ImageFile, [u8; DIGEST as usize]>,
);

/// Read the next copied rows of an image, about `RESTORE_BATCH_BYTES` of them. Each call
/// holds a blocking thread only while it reads, never while the writer catches up: a SlateDB
/// flush can need the same blocking pool, as the local object store does.
fn read_batch(mut image: ImageFile) -> Result<Batch> {
    let mut rows = vec![];
    let mut bytes = 0;
    let end = (|| {
        // The first batch reads the header, which sets a readable version.
        if image.version == 0 {
            image.header()?;
        }
        while bytes < RESTORE_BATCH_BYTES {
            let Some((key, value)) = image.next()? else {
                return Ok(true);
            };
            if replaced(&key) {
                continue;
            }
            bytes += key.len() + value.len();
            rows.push((key, value));
        }
        Ok(false)
    })();
    // At a read error, `finish` prefers the checksum error, as `check` does.
    if let Ok(false) = end {
        return Ok((rows, Ok(image)));
    }
    let digest = image.finish(end.map(|_| ()))?;
    Ok((rows, Err(digest)))
}

/// The value that a restored cluster stores for `key`.
fn restored(key: &[u8], value: Bytes, member: u64) -> Result<Bytes> {
    if key == crate::auth::KEY {
        let mut auth: crate::auth::Store = serde_json::from_slice(&value)?;
        auth.reset_tokens();
        return Ok(serde_json::to_vec(&auth)?.into());
    }
    if key == crate::maintenance::ALARMS {
        let mut alarms: Vec<crate::proto::etcdserverpb::AlarmMember> =
            serde_json::from_slice(&value)?;
        // Keep alarm types, but attach them to a member of the restored cluster.
        for alarm in &mut alarms {
            alarm.member_id = member;
        }
        alarms.sort_by_key(|alarm| alarm.alarm);
        alarms.dedup();
        return Ok(serde_json::to_vec(&alarms)?.into());
    }
    Ok(value)
}

impl StateMachine {
    /// Restore a checked native backup with a new Raft identity and membership.
    ///
    /// The rows go to the state database in batches. Row `n` of the copied rows is written at
    /// sequence `n` or later, and the apply marker follows at `rows + 1`. The
    /// restored index therefore depends only on the backup, so every member restores the same
    /// Raft position. The Raft directory records a snapshot without an image at that index.
    pub async fn restore_backup(
        backup: &Path,
        dir: &Path,
        id: u64,
        cluster_id: &str,
        members: &crate::cluster::Store,
        objects: &crate::object::Config,
        cache: StateCache,
    ) -> Result<()> {
        check_restore_target(dir, id, members)?;
        let started = std::time::Instant::now();
        // A large image reads for long. Keep the reads off the runtime that serves other members.
        let path = backup.to_owned();
        let checked = tokio::task::spawn_blocking(move || check(&path)).await??;
        tracing::info!(rows = checked.copied, elapsed = ?started.elapsed(), "checked the snapshot to restore");
        Self::restore_checked(&checked, dir, id, cluster_id, members, objects, cache).await
    }

    /// Write the rows of a checked image. See `restore_backup`.
    pub(super) async fn restore_checked(
        checked: &Checked,
        dir: &Path,
        id: u64,
        cluster_id: &str,
        members: &crate::cluster::Store,
        objects: &crate::object::Config,
        cache: StateCache,
    ) -> Result<()> {
        let started = std::time::Instant::now();
        std::fs::create_dir_all(dir)?;
        // The Raft directory lock reserves the data directory, as at startup. Check the target
        // again under it: another restore or a started member can have taken it since the check
        // in `restore_backup`. Only then create the marker, so a losing restore leaves no marker.
        // The identity follows the marker, so a crash before the marker leaves no identity.
        let mut storage = crate::storage::RaftStorage::open_for_restore(
            &dir.join("raft"),
            id,
            cluster_id,
            members.conf().voters,
        )?;
        let mut others = std::fs::read_dir(dir)?.filter(|entry| {
            entry
                .as_ref()
                .map_or(true, |entry| entry.file_name() != "raft")
        });
        ensure!(
            storage.is_new() && others.next().is_none(),
            "snapshot restore requires a new, empty data directory"
        );
        let marker = dir.join(RESTORE_MARKER);
        std::fs::File::create(&marker)?.sync_all()?;
        sync_dir(dir)?;
        // A shared cluster restores with `restore_shared`, so the restored state is replicated.
        let mut state = objects
            .open(
                dir,
                cluster_id,
                id,
                cache,
                StateMode::Replicated,
                // Replicated mode ignores the shared-mode settings.
                &crate::state::shared::Options {
                    quota: 0,
                    voters: members.conf().voters.clone(),
                    join: false,
                    empty_log: false,
                    pin_max: std::time::Duration::from_secs(300),
                    overlay_max_bytes: u64::MAX,
                    restore: None,
                    record: None,
                },
            )
            .await?;
        state.row_marker = Some(storage.row_marker());
        let result = state.restore_rows(checked, members).await;
        if let Ok(index) = &result {
            tracing::info!(index, elapsed = ?started.elapsed(), "restored the snapshot rows");
        }
        state.close().await?;
        storage.restore(result?, members.conf())?;
        std::fs::remove_file(&marker)?;
        sync_dir(dir)
    }

    /// Copy the rows of a checked image into this empty state, then store the apply marker.
    /// `members` gives the alarm owner. Return the restored index, and flush the state at it.
    pub(super) async fn restore_rows(
        &mut self,
        checked: &Checked,
        members: &crate::cluster::Store,
    ) -> Result<u64> {
        let member = *members.members.keys().next().unwrap();
        // Read the image in the blocking pool, as `check` does, one batch ahead of the writes.
        // A failed write drops the read in flight, which stops the reader.
        let image = ImageFile::open(&checked.path)?;
        let mut next = Some(tokio::task::spawn_blocking(move || read_batch(image)));
        let mut digest = None;
        let mut written = 0;
        let mut unflushed = 0;
        while let Some(read) = next.take() {
            let (rows, rest) = read.await??;
            match rest {
                Ok(image) => next = Some(tokio::task::spawn_blocking(move || read_batch(image))),
                Err(end) => digest = Some(end),
            }
            if rows.is_empty() {
                continue;
            }
            let mut batch = WriteBatch::new();
            for (key, value) in rows {
                let value = restored(&key, value, member)?;
                unflushed += key.len() + value.len();
                // The database starts empty, and the check rules out duplicate keys.
                batch.put_new(key, value);
                written += 1;
            }
            self.store_restored(batch, written).await?;
            if unflushed >= RESTORE_FLUSH_BYTES {
                self.metrics.flush_db(self.db()).await?;
                unflushed = 0;
            }
        }
        // The file can change after the check. Its digest must match the checked one before
        // the marker goes away.
        ensure!(
            digest == Some(checked.digest),
            "snapshot changed during restore"
        );
        let index = written + 1;
        let mut batch = WriteBatch::new();
        // Like `restored_data`, store no membership row. Each member restores with its own
        // flags, so the first leader of the new cluster seeds the row.
        // The Raft directory records the snapshot at `index` with term 1.
        mark_applied(&mut batch, index, 1);
        self.write_batch(batch, index).await?;
        self.metrics.flush_db(self.db()).await?;
        Ok(index)
    }

    /// Store new restored rows at `index` and count their logical size. Unlike `write_batch`,
    /// do not publish them to the key index and the row caches: the restoring state closes
    /// after the copy, and startup builds them from the stored rows.
    async fn store_restored(&mut self, mut batch: WriteBatch, index: u64) -> Result<()> {
        ensure!(!self.is_shared(), "a restore writes a replicated state");
        // Every row is new, so the size needs no reads.
        let size = batch
            .logical_size(self.applied_state(), &self.key_index, self.logical_bytes)
            .await?;
        self.mark_format_4(&batch)?;
        self.put_logical_size(&mut batch, index, size);
        self.db()
            .write_with_options(batch.to_storage(), &WriteOptions { seqnum: index })
            .await?;
        self.logical_bytes = size;
        Ok(())
    }

    /// Check and read a native snapshot for a new shared cluster. The shared database gets its
    /// rows at index 1. See `shared::Restore`.
    pub fn restore_shared(
        backup: &Path,
        dir: &Path,
        id: u64,
        members: &crate::cluster::Store,
    ) -> Result<shared::Restore> {
        check_restore_target(dir, id, members)?;
        let (mut data, checksum) = restored_data(backup, members)?;
        // The cluster claim sets the quota of shared mode.
        data.rows.retain(|(key, _)| key != quota::KEY);
        // The first view checkpoint must hold the membership. See `restored_data`.
        data.rows
            .push((crate::cluster::KEY.to_vec(), serde_json::to_vec(members)?));
        Ok(shared::Restore::new(data.rows, checksum))
    }
}

/// A restore needs a member of the new cluster and a new, empty data directory.
fn check_restore_target(dir: &Path, id: u64, members: &crate::cluster::Store) -> Result<()> {
    ensure!(
        members.members.contains_key(&id),
        "restore member is missing"
    );
    ensure!(
        !dir.exists() || std::fs::read_dir(dir)?.next().is_none(),
        "snapshot restore requires a new, empty data directory"
    );
    Ok(())
}

/// Read and check a native snapshot, and rewrite it for a new cluster with `members`. The rows
/// are at index 1, with new auth tokens and the alarms on a member of the new cluster.
/// Also return the snapshot checksum in hexadecimal. It identifies the snapshot.
///
/// A replicated restore drops the membership row of the backup. Each member restores with its
/// own flags, so the first leader of the new cluster seeds the row. A shared restore writes the
/// rows once for all members, so it stores the membership of the restoring member.
fn restored_data(backup: &Path, members: &crate::cluster::Store) -> Result<(SnapshotData, String)> {
    let mut bytes = vec![];
    std::fs::File::open(backup)?
        .take(MAX_SHARED_RESTORE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= MAX_SHARED_RESTORE_BYTES,
        "snapshot exceeds 64 MiB"
    );
    ensure!(bytes.len() > 32, "snapshot is missing its checksum");
    let (payload, checksum) = bytes.split_at(bytes.len() - 32);
    ensure!(
        Sha256::digest(payload).as_slice() == checksum,
        "snapshot checksum mismatch"
    );
    let (mut data, consumed): (SnapshotData, _) = bincode::serde::decode_from_slice(
        payload,
        bincode::config::legacy().with_limit::<MAX_SHARED_RESTORE_BYTES>(),
    )
    .context("invalid native snapshot")?;
    ensure!(consumed == payload.len(), "trailing native snapshot data");
    ensure!(
        crate::readable_snapshot(data.version),
        "unsupported snapshot version"
    );
    let mut seen = std::collections::BTreeSet::new();
    for (key, value) in &mut data.rows {
        ensure!(snapshot_key(key), "invalid snapshot key");
        ensure!(seen.insert(key.clone()), "duplicate snapshot key");
        if key == crate::auth::KEY {
            let mut auth: crate::auth::Store = serde_json::from_slice(value)?;
            auth.reset_tokens();
            *value = serde_json::to_vec(&auth)?;
        }
        if key == crate::maintenance::ALARMS {
            let mut alarms: Vec<crate::proto::etcdserverpb::AlarmMember> =
                serde_json::from_slice(value)?;
            // Keep alarm types, but attach them to a member of the restored cluster.
            for alarm in &mut alarms {
                alarm.member_id = *members.members.keys().next().unwrap();
            }
            alarms.sort_by_key(|alarm| alarm.alarm);
            alarms.dedup();
            *value = serde_json::to_vec(&alarms)?;
        }
    }
    // The restored log starts at (1, 1). Install stores that term from the snapshot.
    data.rows
        .retain(|(key, _)| key != APPLIED && !derived_key(key) && key != crate::cluster::KEY);
    data.rows
        .push((APPLIED.to_vec(), 1_u64.to_be_bytes().to_vec()));
    data.applied = 1;
    data.version = crate::SNAPSHOT_FORMAT;
    let checksum = checksum.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok((data, checksum))
}

fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}
