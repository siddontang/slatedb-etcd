//! Keep final mutations so quota accounting sees each key once.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use prost::bytes::Bytes;
use slatedb::Db;

use crate::state::key_index::KeyIndex;

/// An atomic state batch. The last mutation for a key replaces earlier mutations.
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    changes: BTreeMap<Bytes, Mutation>,
}

#[derive(Clone, Debug)]
struct Mutation {
    value: Option<Bytes>,
    original: Original,
}

#[derive(Clone, Copy, Debug)]
enum Original {
    Read,
    Known(Option<usize>),
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.change(
            key.as_ref(),
            Some(Bytes::copy_from_slice(value.as_ref())),
            Original::Read,
        );
    }

    /// Add a put without copying the key or value. Quota accounting reads the old length.
    pub(crate) fn put_bytes(&mut self, key: Bytes, value: Bytes) {
        self.insert(key, Some(value), Original::Read);
    }

    /// Add a key that the current revision has not stored before.
    pub(crate) fn put_new(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.insert(key.into(), Some(value.into()), Original::Known(None));
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) {
        self.change(key.as_ref(), None, Original::Read);
    }

    /// Delete a row whose stored length is known from the compaction scan.
    pub(crate) fn delete_known(&mut self, key: Bytes, old_len: usize) {
        self.insert(key, None, Original::Known(Some(old_len)));
    }

    /// Keep a known old row length for quota accounting.
    pub(crate) fn put_known(
        &mut self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        old_len: Option<usize>,
    ) {
        self.insert(key.into(), Some(value.into()), Original::Known(old_len));
    }

    /// Update the quota total before the next put in this batch.
    pub(crate) fn put_counted(
        &mut self,
        key: Bytes,
        value: Bytes,
        old_len: Option<usize>,
        size: &mut u64,
    ) -> Result<()> {
        *size = row_size_after(*size, key.len(), old_len, Some(value.len()))?;
        self.insert(key, Some(value), Original::Known(old_len));
        Ok(())
    }

    fn change(&mut self, key: &[u8], value: Option<Bytes>, original: Original) {
        self.insert(Bytes::copy_from_slice(key), value, original);
    }

    fn insert(&mut self, key: Bytes, value: Option<Bytes>, original: Original) {
        // Keep the first mutation's knowledge of storage across later batch mutations.
        match self.changes.entry(key) {
            std::collections::btree_map::Entry::Occupied(mut old) => old.get_mut().value = value,
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(Mutation { value, original });
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<Option<Bytes>> {
        self.changes.get(key).map(|change| change.value.clone())
    }

    pub(crate) fn extend(&mut self, other: Self) {
        for (key, change) in other.changes {
            self.insert(key, change.value, change.original);
        }
    }

    pub(crate) async fn logical_size(&self, db: &Db, index: &KeyIndex, size: u64) -> Result<u64> {
        self.logical_size_after(db, index, None, size).await
    }

    /// Update the logical size. The key index supplies old current-row lengths without a read.
    pub(crate) async fn logical_size_after(
        &self,
        db: &Db,
        index: &KeyIndex,
        previous: Option<&Self>,
        mut size: u64,
    ) -> Result<u64> {
        for (key, change) in &self.changes {
            if !key.starts_with(b"etcd/") {
                continue;
            }
            let old_len = match change.original {
                Original::Known(length) => length,
                Original::Read => match previous.and_then(|batch| batch.get(key)) {
                    Some(value) => value.map(|v| v.len()),
                    None if key.starts_with(crate::mvcc::CURRENT) => {
                        index.get_row(key).map(|meta| meta.row_len)
                    }
                    None => db.get(key).await?.map(|v| v.len()),
                },
            };
            size = row_size_after(
                size,
                key.len(),
                old_len,
                change.value.as_ref().map(Bytes::len),
            )?;
        }
        Ok(size)
    }

    pub(crate) fn into_rows(self) -> impl Iterator<Item = (Bytes, Option<Bytes>)> {
        self.changes
            .into_iter()
            .map(|(key, change)| (key, change.value))
    }

    pub(crate) fn to_storage(&self) -> slatedb::WriteBatch {
        let mut batch = slatedb::WriteBatch::new();
        for (key, change) in &self.changes {
            if let Some(value) = &change.value {
                batch.put_bytes(key.clone(), value.clone());
            } else {
                batch.delete(key);
            }
        }
        batch
    }
}

/// Rows that share one buffer.
///
/// SlateDB keeps each memtable key and value as its own `Bytes` until the memtable is flushed. A
/// Put writes four rows. Built separately, they cost eight heap buffers, and cloning them into
/// the SlateDB batch adds one shared reference count allocation each. All rows of one write enter
/// the same memtable and leave it with the same flush, so one buffer frees no later than eight.
#[derive(Default)]
pub(crate) struct RowPack {
    buf: Vec<u8>,
    rows: Vec<PackedRow>,
}

struct PackedRow {
    key: std::ops::Range<usize>,
    value: std::ops::Range<usize>,
    old_len: Option<usize>,
}

impl RowPack {
    /// Reserve room for `bytes` more bytes, so that the buffer is allocated once.
    pub(crate) fn reserve(&mut self, bytes: usize) {
        self.buf.reserve_exact(bytes);
    }

    /// Append the bytes that `write` adds to the buffer and return their range.
    pub(crate) fn write(&mut self, write: impl FnOnce(&mut Vec<u8>)) -> std::ops::Range<usize> {
        let start = self.buf.len();
        write(&mut self.buf);
        start..self.buf.len()
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.buf
    }

    pub(crate) fn append(&mut self, bytes: &[u8]) -> std::ops::Range<usize> {
        self.write(|buf| buf.extend_from_slice(bytes))
    }

    /// Add a row from ranges of the buffer. Rows may share bytes.
    pub(crate) fn row(
        &mut self,
        key: std::ops::Range<usize>,
        value: std::ops::Range<usize>,
        old_len: Option<usize>,
    ) {
        self.rows.push(PackedRow {
            key,
            value,
            old_len,
        });
    }

    /// Add the rows to `batch` in order, as `WriteBatch::put_counted` does.
    pub(crate) fn put_into(mut self, batch: &mut WriteBatch, size: &mut u64) -> Result<()> {
        // An exact buffer converts to `Bytes` without a copy and wastes no capacity.
        self.buf.shrink_to_fit();
        let buf = Bytes::from(self.buf);
        for row in self.rows {
            batch.put_counted(buf.slice(row.key), buf.slice(row.value), row.old_len, size)?;
        }
        Ok(())
    }
}

fn row_size_after(
    size: u64,
    key_len: usize,
    old: Option<usize>,
    new: Option<usize>,
) -> Result<u64> {
    let bytes = |len| (key_len + len) as u64;
    size.checked_sub(old.map_or(0, bytes))
        .context("logical size counter underflow")?
        .checked_add(new.map_or(0, bytes))
        .context("logical size counter overflow")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_rows_match_separate_rows_and_share_one_buffer() {
        let rows: [(&[u8], &[u8], Option<usize>); 3] = [
            (b"etcd/a", b"value-a", Some(9)),
            (b"etcd/b", b"", None),
            (b"etcd/c", b"value-c", None),
        ];
        let mut separate = WriteBatch::new();
        let mut separate_size = 100;
        for (key, value, old) in rows {
            separate
                .put_counted(
                    Bytes::copy_from_slice(key),
                    Bytes::copy_from_slice(value),
                    old,
                    &mut separate_size,
                )
                .unwrap();
        }
        let mut pack = RowPack::default();
        pack.reserve(64);
        for (key, value, old) in rows {
            let key = pack.append(key);
            let value = pack.append(value);
            pack.row(key, value, old);
        }
        // A row may also reuse bytes of an earlier row.
        let shared = 6..12;
        let key = pack.append(b"etcd/d");
        pack.row(key, shared, None);
        let mut packed = WriteBatch::new();
        let mut packed_size = 100;
        pack.put_into(&mut packed, &mut packed_size).unwrap();
        separate
            .put_counted(
                Bytes::from_static(b"etcd/d"),
                Bytes::from_static(b"value-"),
                None,
                &mut separate_size,
            )
            .unwrap();
        assert_eq!(packed_size, separate_size);
        let packed: Vec<_> = packed.into_rows().collect();
        assert_eq!(packed, separate.into_rows().collect::<Vec<_>>());
        let base = packed[0].0.as_ptr() as usize;
        for (key, _) in &packed {
            assert!((key.as_ptr() as usize - base) < 64);
        }
    }
}
