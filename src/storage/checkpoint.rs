//! Publish checkpoint bytes before the WAL can reference them or discard covered entries.

mod encoding;

use super::{Engine, LogBatch, LogFileSystem, Snapshot, GROUP, SNAPSHOT};
use anyhow::{ensure, Context, Result};
use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

const REFERENCE: &[u8] = b"checkpoint-file";
const PENDING: &str = "checkpoint.pending";

const STAGING_PREFIX: &str = "checkpoint-stage-";
const STAGING_SUFFIX: &str = ".tmp";
const RANDOM_CHARS: usize = 12;

pub(super) fn path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("checkpoint-{index:016x}.data"))
}

fn remove(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        result => Ok(result?),
    }
}

/// A synced image that the WAL owner can publish or discard.
pub struct PreparedCheckpoint {
    pub(crate) data: bytes::Bytes,
    file: tempfile::NamedTempFile,
    checksum: u32,
    compressed_len: Option<u64>,
}

impl PreparedCheckpoint {
    pub(crate) fn write(dir: &Path, data: bytes::Bytes) -> Result<Self> {
        let mut file = tempfile::Builder::new()
            .prefix(STAGING_PREFIX)
            .suffix(STAGING_SUFFIX)
            .rand_bytes(RANDOM_CHARS)
            .tempfile_in(dir)?;
        let compressed_len = encoding::write(file.as_file_mut(), &data)?;
        file.as_file().sync_all()?;
        let checksum = crc32c::crc32c(&data);
        crate::failpoint::hit("checkpoint-after-file-sync");
        Ok(Self {
            data,
            file,
            checksum,
            compressed_len,
        })
    }

    pub(super) fn publish(
        self,
        dir: &Path,
        snapshot: &Snapshot,
        batch: &mut LogBatch,
    ) -> Result<()> {
        self.file
            .persist(path(dir, snapshot.get_metadata().index))?;
        File::open(dir)?.sync_all()?;
        crate::failpoint::hit("checkpoint-file-before-reference");

        let mut reference = (self.data.len() as u64).to_le_bytes().to_vec();
        reference.extend_from_slice(&self.checksum.to_le_bytes());
        if let Some(len) = self.compressed_len {
            reference.extend_from_slice(&len.to_le_bytes());
        }
        let mut metadata = snapshot.clone();
        metadata.data = bytes::Bytes::new();
        batch.put_message(GROUP, SNAPSHOT.to_vec(), &metadata)?;
        batch.put(GROUP, REFERENCE.to_vec(), reference)?;
        Ok(())
    }
}

/// Publish snapshot metadata without an image. Flushed SlateDB state covers the removed log.
pub(super) fn publish_metadata(snapshot: &Snapshot, batch: &mut LogBatch) -> Result<()> {
    ensure!(snapshot.data.is_empty(), "metadata snapshot has data");
    batch.put_message(GROUP, SNAPSHOT.to_vec(), snapshot)?;
    batch.delete(GROUP, REFERENCE.to_vec());
    Ok(())
}

fn staged_name(name: &str) -> bool {
    name.strip_prefix(STAGING_PREFIX)
        .and_then(|name| name.strip_suffix(STAGING_SUFFIX))
        .is_some_and(|id| id.len() == RANDOM_CHARS && id.bytes().all(|b| b.is_ascii_alphanumeric()))
}

pub(super) fn load(
    dir: &Path,
    engine: &Engine<LogFileSystem>,
    snapshot: &mut Snapshot,
) -> Result<Option<u64>> {
    if let Some(reference) = engine.get(GROUP, REFERENCE) {
        ensure!(
            matches!(reference.len(), 12 | 20) && snapshot.data.is_empty(),
            "invalid checkpoint reference"
        );
        let len = u64::from_le_bytes(reference[..8].try_into()?);
        let checksum = u32::from_le_bytes(reference[8..12].try_into()?);
        let compressed_len = reference[12..].try_into().ok().map(u64::from_le_bytes);
        let file = File::open(path(dir, snapshot.get_metadata().index))
            .context("missing checkpoint file")?;
        let data = encoding::read(file, len, compressed_len)?;
        ensure!(
            crc32c::crc32c(&data) == checksum,
            "checkpoint checksum mismatch"
        );
        snapshot.data = data.into();
        return Ok(Some(snapshot.get_metadata().index));
    }
    Ok(None)
}

pub(super) fn retire(dir: &Path, index: u64) -> Result<()> {
    if index != 0 {
        remove(&path(dir, index))?;
    }
    Ok(())
}

pub(super) fn remove_orphans(dir: &Path, current: Option<u64>) -> Result<()> {
    remove(&dir.join(PENDING))?;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_str().is_some_and(staged_name) {
            remove(&entry.path())?;
            continue;
        }
        let Some(index) = name
            .to_str()
            .and_then(|s| s.strip_prefix("checkpoint-"))
            .and_then(|s| s.strip_suffix(".data"))
            .and_then(|s| u64::from_str_radix(s, 16).ok())
        else {
            continue;
        };
        if Some(index) != current && entry.path() == path(dir, index) {
            remove(&entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_images_cannot_replace_the_published_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let image = |bytes| {
            PreparedCheckpoint::write(dir.path(), bytes::Bytes::from_static(bytes)).unwrap()
        };
        let older = image(b"old local image");
        let orphan = older.file.path().to_owned();
        let incoming = image(b"from leader");
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 11;
        incoming
            .publish(dir.path(), &snapshot, &mut LogBatch::default())
            .unwrap();
        drop(older);
        assert!(!orphan.exists());
        assert_eq!(fs::read(path(dir.path(), 11)).unwrap(), b"from leader");

        let abandoned = image(b"synced before process exit");
        let (_, abandoned) = abandoned.file.keep().unwrap();
        let unrelated = dir.path().join("checkpoint-stage-personal.tmp");
        fs::write(&unrelated, b"other file").unwrap();
        remove_orphans(dir.path(), Some(11)).unwrap();
        assert!(!abandoned.exists());
        assert!(unrelated.exists());
        assert_eq!(fs::read(path(dir.path(), 11)).unwrap(), b"from leader");
    }
}
