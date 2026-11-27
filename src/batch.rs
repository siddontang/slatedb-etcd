//! Keep final mutations so quota accounting sees each key once.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use prost::bytes::Bytes;

use crate::state::key_index::KeyIndex;
use crate::state::StateRead;

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

#[cfg(test)]
thread_local! {
    /// The old rows that the size count read on this thread.
    pub(crate) static SIZE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
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
        size: &mut Usage,
    ) -> Result<()> {
        size.replace_row(&key, old_len, Some(value.len()))?;
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

    pub(crate) async fn logical_size(
        &self,
        db: &impl StateRead,
        index: &KeyIndex,
        size: Usage,
    ) -> Result<Usage> {
        self.logical_size_after(db, index, None, size).await
    }

    /// Update the logical size. The key index supplies old current-row lengths without a read.
    pub(crate) async fn logical_size_after(
        &self,
        db: &impl StateRead,
        index: &KeyIndex,
        previous: Option<&Self>,
        mut size: Usage,
    ) -> Result<Usage> {
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
                    None => {
                        #[cfg(test)]
                        SIZE_READS.with(|reads| reads.set(reads.get() + 1));
                        db.get(key).await?.map(|v| v.len())
                    }
                },
            };
            size.replace_row(key, old_len, change.value.as_ref().map(Bytes::len))?;
        }
        Ok(size)
    }

    /// Return true when the batch puts a row whose key starts with `prefix`.
    pub(crate) fn puts_under(&self, prefix: &[u8]) -> bool {
        use std::ops::Bound;
        self.changes
            .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .any(|(_, change)| change.value.is_some())
    }

    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        self.changes.contains_key(key)
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.changes.keys()
    }

    /// Return the final mutation of each key in key order. `None` is a delete.
    /// The values are the batch's own `Bytes`, so a caller can keep them without a copy.
    pub(crate) fn rows(&self) -> impl Iterator<Item = (&Bytes, Option<&Bytes>)> {
        self.changes
            .iter()
            .map(|(key, change)| (key, change.value.as_ref()))
    }

    #[cfg(test)]
    pub(crate) fn into_rows(self) -> impl Iterator<Item = (Bytes, Option<Bytes>)> {
        self.changes
            .into_iter()
            .map(|(key, change)| (key, change.value))
    }

    pub(crate) fn to_storage(&self) -> slatedb::WriteBatch {
        let mut batch = slatedb::WriteBatch::new();
        for (key, value) in self.rows() {
            if let Some(value) = value {
                batch.put_bytes(key.clone(), value.clone());
            } else {
                batch.delete(key);
            }
        }
        batch
    }
}

/// Logical bytes of `etcd/` rows by kind. The quota limits the total.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    /// Current rows of live keys.
    pub current: u64,
    /// History versions and their hash digests. Compaction removes them.
    pub history: u64,
    /// Watch journal rows. Compaction removes them.
    pub journal: u64,
    /// Metadata rows: auth, members, leases, alarms, revision and compaction records.
    pub other: u64,
}

impl Usage {
    pub fn total(&self) -> u64 {
        self.current + self.history + self.journal + self.other
    }

    /// The total, or `None` when a corrupt split does not fit in a `u64`.
    pub fn checked_total(&self) -> Option<u64> {
        self.current
            .checked_add(self.history)?
            .checked_add(self.journal)?
            .checked_add(self.other)
    }

    fn kind(&mut self, key: &[u8]) -> &mut u64 {
        if key.starts_with(crate::mvcc::CURRENT) {
            &mut self.current
        } else if key.starts_with(crate::mvcc::HISTORY) || key.starts_with(crate::maintenance::HASH)
        {
            &mut self.history
        } else if key.starts_with(crate::watch::EVENTS) {
            &mut self.journal
        } else {
            &mut self.other
        }
    }

    /// Count one stored row. Callers pass only `etcd/` keys.
    pub(crate) fn add_row(&mut self, key: &[u8], len: usize) -> Result<()> {
        self.replace_row(key, None, Some(len))
    }

    /// Replace the old row length with the new one. `None` means that no row exists.
    pub(crate) fn replace_row(
        &mut self,
        key: &[u8],
        old: Option<usize>,
        new: Option<usize>,
    ) -> Result<()> {
        let bytes = |len| (key.len() + len) as u64;
        let slot = self.kind(key);
        *slot = slot
            .checked_sub(old.map_or(0, bytes))
            .context("logical size counter underflow")?
            .checked_add(new.map_or(0, bytes))
            .context("logical size counter overflow")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_hold_the_final_mutations_without_a_copy() {
        let mut batch = WriteBatch::new();
        let value = Bytes::from(vec![7; 64]);
        batch.put(b"b", b"first");
        batch.put_bytes(Bytes::from_static(b"b"), value.clone());
        batch.put(b"a", b"kept");
        batch.put(b"c", b"removed");
        batch.delete(b"c");
        batch.delete(b"d");
        let rows: Vec<_> = batch.rows().collect();
        assert_eq!(
            rows,
            [
                (
                    &Bytes::from_static(b"a"),
                    Some(&Bytes::from_static(b"kept"))
                ),
                (&Bytes::from_static(b"b"), Some(&value)),
                (&Bytes::from_static(b"c"), None),
                (&Bytes::from_static(b"d"), None),
            ]
        );
        // The row shares the buffer of the put value.
        assert_eq!(rows[1].1.unwrap().as_ptr(), value.as_ptr());
        let owned: Vec<_> = batch
            .rows()
            .map(|(key, value)| (key.clone(), value.cloned()))
            .collect();
        assert_eq!(batch.into_rows().collect::<Vec<_>>(), owned);
    }
}
