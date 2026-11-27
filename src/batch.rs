//! Keep final mutations so quota accounting sees each key once.

use std::collections::BTreeMap;

use prost::bytes::Bytes;

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

/// What a batch knows of the stored row that a mutation replaces.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Original {
    /// A reader must look up the old row.
    Read,
    /// The length of the old row. `None` means that no row exists.
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

    /// Return the final mutation of each key in key order, with what the batch knows of the old
    /// row. `None` is a delete.
    pub(crate) fn changes(&self) -> impl Iterator<Item = (&Bytes, Option<&Bytes>, Original)> {
        self.changes
            .iter()
            .map(|(key, change)| (key, change.value.as_ref(), change.original))
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
