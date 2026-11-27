//! Native backups: stream a pinned state as a native image, and restore an image with bounded
//! memory. See `docs/administration.md#maintenance-and-storage`.
//!
//! The image is the legacy Raft snapshot format, `SnapshotData` in `codec` encoding, followed by
//! its SHA-256 digest. Its rows are in key order, and the membership row comes last.
//!
//! A backup pins a view. See `view`.

use super::*;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use slatedb::config::ScanOptions;
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
/// `etcdctl snapshot save` accepts a file of a multiple of this size plus the digest.
const ALIGN: u64 = 512;
const DIGEST: u64 = 32;
/// The largest encoded row of an image. A restore decodes rows up to this size, and a backup
/// rejects a larger row before it publishes the image. A Raft entry holds at most 8 MiB, but
/// the auth row holds every user and role and can grow beyond that.
pub(super) const MAX_ROW_BYTES: usize = 64 * 1024 * 1024;
/// The row bytes of one restore write batch.
pub(super) const RESTORE_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// The row bytes a restore writes between flushes. Without a flush, SlateDB keeps up to
/// `max_unflushed_bytes` in memtables. This matches the L0 SST size.
pub(super) const RESTORE_FLUSH_BYTES: usize = 64 * 1024 * 1024;

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
    /// Pin the applied view, then stream its image from a task.
    ///
    /// Call it between apply jobs. Only the pin runs here. Writes that apply later do not reach
    /// the view, so the image holds one applied state boundary.
    pub(crate) async fn backup(&self) -> Result<Stream> {
        self.shared_backup(&self.shared)
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
pub(super) struct ImageFile {
    payload: Hashing<std::io::Take<BufReader<std::fs::File>>>,
    version: u32,
    rows: u64,
    read: u64,
    /// Return true for a row that the restore does not copy. `next` skips it.
    skip: fn(&[u8]) -> bool,
    /// The digest state after the last copied row.
    copied: Sha256,
}

impl ImageFile {
    pub(super) fn open(path: &Path, skip: fn(&[u8]) -> bool) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        ensure!(len > DIGEST, "snapshot is missing its checksum");
        let payload = Hashing {
            inner: BufReader::with_capacity(1024 * 1024, file).take(len - DIGEST),
            hash: Sha256::new(),
            bytes: 0,
            progress: None,
            #[cfg(test)]
            throttle: None,
        };
        Ok(Self {
            payload,
            version: 0,
            rows: 0,
            read: 0,
            skip,
            copied: Sha256::new(),
        })
    }

    /// Store the number of file bytes read in `progress` after each read. A slow read of a large
    /// row then still shows progress, and a read that hangs shows none.
    pub(super) fn count_in(&mut self, progress: Arc<std::sync::atomic::AtomicU64>) {
        self.payload.progress = Some(progress);
    }

    /// Return at most `bytes` from each read, and wait `delay` before it, as a slow disk does.
    #[cfg(test)]
    pub(super) fn throttle(&mut self, bytes: usize, delay: Duration) {
        self.payload.throttle = Some((bytes, delay));
    }

    pub(super) fn header(&mut self) -> Result<()> {
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

    /// Decode the next copied row. `Bytes` reads each field at once, where `Vec<u8>` reads it
    /// bytewise.
    pub(super) fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        while self.read < self.rows {
            self.read += 1;
            let (key, value): (Bytes, Bytes) =
                crate::codec::read::<_, MAX_ROW_BYTES>(&mut self.payload)
                    .context("invalid native snapshot")?;
            if !(self.skip)(&key) {
                self.copied = self.payload.hash.clone();
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }

    /// The SHA-256 of the file bytes through the last copied row.
    pub(super) fn prefix(&self) -> [u8; DIGEST as usize] {
        self.copied.clone().finalize().into()
    }

    /// Read the rest of the file. Check its digest, then that the rows filled the payload.
    /// Return the digest.
    pub(super) fn finish(mut self, rows: Result<()>) -> Result<[u8; DIGEST as usize]> {
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
    bytes: u64,
    /// Gets `bytes` after each read, if set.
    progress: Option<Arc<std::sync::atomic::AtomicU64>>,
    #[cfg(test)]
    throttle: Option<(usize, Duration)>,
}

impl<R: Read> Read for Hashing<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(test)]
        let buf = match self.throttle {
            Some((bytes, delay)) => {
                std::thread::sleep(delay);
                let len = buf.len().min(bytes);
                &mut buf[..len]
            }
            None => buf,
        };
        let n = self.inner.read(buf)?;
        self.hash.update(&buf[..n]);
        self.bytes += n as u64;
        if let Some(progress) = &self.progress {
            progress.store(self.bytes, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(n)
    }
}

/// Return true for the rows that a restore replaces instead of copying.
pub(super) fn replaced(key: &[u8]) -> bool {
    key == APPLIED || derived_key(key) || key == crate::cluster::KEY
}

/// A native image file that passed `check`.
pub(super) struct Checked {
    pub(super) path: PathBuf,
    /// The number of rows that a restore copies.
    pub(super) copied: u64,
    pub(super) digest: [u8; DIGEST as usize],
}

/// Check every row in one pass. `skip` returns true for the rows that the restore does not copy.
pub(super) fn check(path: &Path, skip: fn(&[u8]) -> bool) -> Result<Checked> {
    let mut image = ImageFile::open(path, skip)?;
    let mut copied = 0;
    let rows = (|| {
        image.header()?;
        let mut last: Option<Bytes> = None;
        while let Some((key, value)) = image.next()? {
            ensure!(snapshot_key(&key), "invalid snapshot key");
            // Images hold rows in key order, so order also rules out duplicates.
            ensure!(
                last.as_ref().is_none_or(|last| *last < key),
                "duplicate or unordered snapshot key"
            );
            // The restored value of the auth row or the alarm row can have another size.
            row_size(&key, &restored(&key, value, 0)?)?;
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

/// The copied rows of one restore batch, the SHA-256 of the file through its last row, and the
/// image to read on or, at its end, its digest.
pub(super) struct Batch {
    pub(super) rows: Vec<(Bytes, Bytes)>,
    pub(super) prefix: [u8; DIGEST as usize],
    pub(super) rest: std::result::Result<ImageFile, [u8; DIGEST as usize]>,
}

/// Read the next copied rows of an image, about `limit` bytes of them. Each call holds a
/// blocking thread only while it reads, never while the writer catches up: a SlateDB flush can
/// need the same blocking pool, as the local object store does.
pub(super) fn read_batch(mut image: ImageFile, limit: usize) -> Result<Batch> {
    let mut rows = vec![];
    let mut bytes = 0;
    let end = (|| {
        // The first batch reads the header, which sets a readable version.
        if image.version == 0 {
            image.header()?;
        }
        while bytes < limit {
            let Some((key, value)) = image.next()? else {
                return Ok(true);
            };
            bytes += key.len() + value.len();
            rows.push((key, value));
        }
        Ok(false)
    })();
    let prefix = image.prefix();
    // At a read error, `finish` prefers the checksum error, as `check` does.
    if let Ok(false) = end {
        return Ok(Batch {
            rows,
            prefix,
            rest: Ok(image),
        });
    }
    let digest = image.finish(end.map(|_| ()))?;
    Ok(Batch {
        rows,
        prefix,
        rest: Err(digest),
    })
}

/// The value that a restored cluster stores for `key`. A shared restore records this rule in
/// its input with `shared::TRANSFORM`, so change that version with it.
pub(super) fn restored(key: &[u8], value: Bytes, member: u64) -> Result<Bytes> {
    if key == crate::auth::KEY {
        let mut auth: crate::auth::Store = serde_json::from_slice(&value)?;
        auth.reset_tokens();
        return Ok(serde_json::to_vec(&auth)?.into());
    }
    if let Some(lease) = crate::lease::restored(key, &value) {
        return Ok(lease?.into());
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
    /// Check a native backup for a new shared cluster in one pass. The member writes its rows
    /// into the shared database when it opens. See `shared::Restore`.
    pub async fn restore_shared(
        backup: &Path,
        id: u64,
        members: &crate::cluster::Store,
    ) -> Result<shared::Restore> {
        ensure!(
            members.members.contains_key(&id),
            "restore member is missing"
        );
        let started = std::time::Instant::now();
        // A large image reads for long. Keep the reads off the runtime.
        let path = backup.to_owned();
        let checked =
            tokio::task::spawn_blocking(move || check(&path, shared::not_restored)).await??;
        tracing::info!(rows = checked.copied, elapsed = ?started.elapsed(), "checked the snapshot to restore");
        shared::Restore::new(checked, id, members)
    }
}

/// Write a checksummed image of `rows` to `path`.
#[cfg(test)]
pub(crate) fn write_image(path: &Path, rows: Vec<(Vec<u8>, Vec<u8>)>) {
    let mut bytes = crate::codec::encode(&SnapshotData {
        version: crate::SNAPSHOT_FORMAT,
        applied: rows.len() as u64,
        rows,
    })
    .unwrap();
    bytes.extend_from_slice(&Sha256::digest(&bytes));
    std::fs::write(path, bytes).unwrap();
}
