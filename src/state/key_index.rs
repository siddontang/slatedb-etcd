//! Keep revision metadata for every current etcd key in apply-owner memory.
//!
//! The index follows etcd's `treeIndex`. Put, transaction, and quota paths use it instead of a
//! SlateDB point read when they do not need the old value. Every state write updates the index
//! after SlateDB accepts the batch, so the index always matches the applied `etcd/current/` rows.
//!
//! Each hash table slot uses 57 bytes on 64-bit hosts: a boxed key, five integer fields, and a
//! control byte. Slots are 44 to 88 percent full, so a key costs 65 to 130 bytes plus its key
//! allocation.

use std::collections::HashMap;

use anyhow::{Context, Result};
use prost::Message;
use slatedb::Db;

use crate::mvcc::CURRENT;

/// The revision metadata of one current key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct KeyMeta {
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub lease: i64,
    /// Length of the encoded `etcd/current/` row value.
    pub row_len: usize,
}

/// Decode the integer fields of `mvccpb.KeyValue` and skip its key and value bytes.
#[derive(Clone, PartialEq, Message)]
struct MetaFields {
    #[prost(int64, tag = "2")]
    create_revision: i64,
    #[prost(int64, tag = "3")]
    mod_revision: i64,
    #[prost(int64, tag = "4")]
    version: i64,
    #[prost(int64, tag = "6")]
    lease: i64,
}

impl KeyMeta {
    /// Read the metadata of an encoded current row.
    pub(crate) fn decode(row: &[u8]) -> Result<Self> {
        let fields = MetaFields::decode(row).context("invalid current row")?;
        Ok(Self {
            create_revision: fields.create_revision,
            mod_revision: fields.mod_revision,
            version: fields.version,
            lease: fields.lease,
            row_len: row.len(),
        })
    }

    /// Build a value without key and value bytes for revision and lease evaluation.
    pub(crate) fn stub(&self) -> crate::proto::mvccpb::KeyValue {
        crate::proto::mvccpb::KeyValue {
            create_revision: self.create_revision,
            mod_revision: self.mod_revision,
            version: self.version,
            lease: self.lease,
            ..Default::default()
        }
    }
}

#[derive(Default)]
pub(crate) struct KeyIndex {
    keys: HashMap<Box<[u8]>, KeyMeta>,
}

impl KeyIndex {
    /// Scan all current rows once.
    pub(crate) async fn load(db: &Db) -> Result<Self> {
        let mut index = Self::default();
        let mut scan = db.scan_prefix(CURRENT, ..).await?;
        while let Some(row) = scan.next().await? {
            index.publish(&row.key, Some(&row.value))?;
        }
        Ok(index)
    }

    /// Return the metadata for a user key.
    pub(crate) fn get(&self, key: &[u8]) -> Option<&KeyMeta> {
        self.keys.get(key)
    }

    /// Return the metadata for a full `etcd/current/` storage key.
    pub(crate) fn get_row(&self, row_key: &[u8]) -> Option<&KeyMeta> {
        self.get(row_key.strip_prefix(CURRENT)?)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Apply one committed storage mutation. Other key prefixes are ignored.
    pub(crate) fn publish(&mut self, row_key: &[u8], value: Option<&bytes::Bytes>) -> Result<()> {
        let Some(key) = row_key.strip_prefix(CURRENT) else {
            return Ok(());
        };
        match value {
            Some(value) => {
                let meta = KeyMeta::decode(value)?;
                if let Some(old) = self.keys.get_mut(key) {
                    *old = meta;
                } else {
                    self.keys.insert(key.into(), meta);
                }
            }
            None => {
                self.keys.remove(key);
            }
        }
        Ok(())
    }

    /// Compare the index with a full current-row scan.
    #[cfg(test)]
    pub(crate) async fn matches(&self, db: &Db) -> Result<bool> {
        let fresh = Self::load(db).await?;
        Ok(fresh.keys == self.keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::mvccpb::KeyValue;

    #[test]
    fn metadata_matches_the_full_decoder_and_deletes_remove_keys() {
        let kv = KeyValue {
            key: b"key".to_vec(),
            create_revision: 3,
            mod_revision: 9,
            version: 4,
            value: vec![7; 300],
            lease: 11,
        };
        let row = bytes::Bytes::from(kv.encode_to_vec());
        let meta = KeyMeta::decode(&row).unwrap();
        let stub = meta.stub();
        assert_eq!(
            (
                stub.create_revision,
                stub.mod_revision,
                stub.version,
                stub.lease
            ),
            (3, 9, 4, 11)
        );
        assert_eq!(meta.row_len, row.len());
        let mut index = KeyIndex::default();
        let row_key = [CURRENT, b"key"].concat();
        index.publish(&row_key, Some(&row)).unwrap();
        index.publish(b"etcd/history/key", None).unwrap();
        assert_eq!(index.get(b"key"), Some(&meta));
        assert_eq!(index.get_row(&row_key), Some(&meta));
        index.publish(&row_key, None).unwrap();
        assert_eq!(index.len(), 0);
    }
}
