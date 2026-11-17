//! Keep final mutations so quota accounting sees each key once.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use prost::bytes::Bytes;
use slatedb::Db;

/// An atomic state batch. The last mutation for a key replaces earlier mutations.
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    changes: BTreeMap<Bytes, Option<Bytes>>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.changes.insert(
            Bytes::copy_from_slice(key.as_ref()),
            Some(Bytes::copy_from_slice(value.as_ref())),
        );
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) {
        self.changes
            .insert(Bytes::copy_from_slice(key.as_ref()), None);
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub(crate) async fn logical_size(&self, db: &Db, mut size: u64) -> Result<u64> {
        for (key, value) in &self.changes {
            if !key.starts_with(b"etcd/") {
                continue;
            }
            if let Some(old) = db.get(key).await? {
                size = size
                    .checked_sub((key.len() + old.len()) as u64)
                    .context("logical size counter underflow")?;
            }
            if let Some(value) = value {
                size = size
                    .checked_add((key.len() + value.len()) as u64)
                    .context("logical size counter overflow")?;
            }
        }
        Ok(size)
    }

    pub(crate) fn into_storage(self) -> slatedb::WriteBatch {
        let mut batch = slatedb::WriteBatch::new();
        for (key, value) in self.changes {
            if let Some(value) = value {
                batch.put_bytes(key, value);
            } else {
                batch.delete(key);
            }
        }
        batch
    }
}
