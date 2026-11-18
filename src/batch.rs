//! Keep final mutations so quota accounting sees each key once.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use prost::bytes::Bytes;
use slatedb::Db;

/// An atomic state batch. The last mutation for a key replaces earlier mutations.
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    changes: BTreeMap<Bytes, Mutation>,
}

#[derive(Clone, Debug)]
struct Mutation {
    value: Option<Bytes>,
    absent: bool,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.change(
            key.as_ref(),
            Some(Bytes::copy_from_slice(value.as_ref())),
            false,
        );
    }

    /// Add a key that the current revision has not stored before.
    pub(crate) fn put_new(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.change(
            key.as_ref(),
            Some(Bytes::copy_from_slice(value.as_ref())),
            true,
        );
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) {
        self.change(key.as_ref(), None, false);
    }

    fn change(&mut self, key: &[u8], value: Option<Bytes>, absent: bool) {
        // Keep the first mutation's knowledge of storage across later batch mutations.
        self.changes
            .entry(Bytes::copy_from_slice(key))
            .and_modify(|change| change.value = value.clone())
            .or_insert(Mutation { value, absent });
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub(crate) async fn logical_size(&self, db: &Db, mut size: u64) -> Result<u64> {
        for (key, change) in &self.changes {
            if !key.starts_with(b"etcd/") {
                continue;
            }
            if let Some(old) = if change.absent {
                None
            } else {
                db.get(key).await?
            } {
                size = size
                    .checked_sub((key.len() + old.len()) as u64)
                    .context("logical size counter underflow")?;
            }
            if let Some(value) = &change.value {
                size = size
                    .checked_add((key.len() + value.len()) as u64)
                    .context("logical size counter overflow")?;
            }
        }
        Ok(size)
    }

    pub(crate) fn into_storage(self) -> slatedb::WriteBatch {
        let mut batch = slatedb::WriteBatch::new();
        for (key, change) in self.changes {
            if let Some(value) = change.value {
                batch.put_bytes(key, value);
            } else {
                batch.delete(key);
            }
        }
        batch
    }
}
