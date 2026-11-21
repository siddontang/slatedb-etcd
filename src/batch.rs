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
