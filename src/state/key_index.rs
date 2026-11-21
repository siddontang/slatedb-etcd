//! Keep revision metadata for every current etcd key in apply-owner memory.
//!
//! The index follows etcd's `treeIndex`. Put, transaction, and quota paths use it instead of a
//! SlateDB point read when they do not need the old value. Every state write updates the index
//! after SlateDB accepts the batch, so the index always matches the applied `etcd/current/` rows.
//!
//! Each hash table slot uses 57 bytes on 64-bit hosts: a boxed key, five integer fields, and a
//! control byte. Slots are 44 to 88 percent full, so a key costs 65 to 130 bytes plus its key
//! allocation.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Bound;
use std::sync::{Arc, Mutex, RwLock};

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
    keys: HashMap<Arc<[u8]>, KeyMeta>,
    /// Key creations and deletions of the batch being published.
    changes: Vec<(Arc<[u8]>, bool)>,
    order: Arc<KeyOrder>,
    /// The key order holds changes that a later commit may fold into its base state.
    history: bool,
}

impl KeyIndex {
    /// Scan all current rows once.
    pub(crate) async fn load(db: &Db) -> Result<Self> {
        let mut index = Self::default();
        let mut scan = db.scan_prefix(CURRENT, ..).await?;
        while let Some(row) = scan.next().await? {
            index.publish(&row.key, Some(&row.value))?;
        }
        index.commit(0);
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
                    let key: Arc<[u8]> = key.into();
                    self.keys.insert(key.clone(), meta);
                    self.changes.push((key, true));
                }
            }
            None => {
                if let Some((key, _)) = self.keys.remove_entry(key) {
                    self.changes.push((key, false));
                }
            }
        }
        Ok(())
    }

    /// Make the creations and deletions published since the last commit visible to read views
    /// pinned at `seq` or later. `seq` is the SlateDB sequence number of the batch.
    pub(crate) fn commit(&mut self, seq: u64) {
        // Overwrites of current keys leave the order unchanged.
        if self.changes.is_empty() && !self.history {
            return;
        }
        self.history = self.order.apply(seq, std::mem::take(&mut self.changes));
    }

    /// Pin the key order at an applied SlateDB sequence number for one read view.
    pub(crate) fn pin(&self, seq: u64) -> KeyPin {
        self.order.pin(seq)
    }

    /// Compare the index with a full current-row scan.
    #[cfg(test)]
    pub(crate) async fn matches(&self, db: &Db) -> Result<bool> {
        let fresh = Self::load(db).await?;
        let latest = self.pin(u64::MAX);
        let ordered = self.order.keys.read().unwrap();
        let visible: Vec<&[u8]> = ordered
            .keys
            .iter()
            .filter(|(_, presence)| presence.visible(latest.seq))
            .map(|(key, _)| key.as_ref())
            .collect();
        let mut current: Vec<&[u8]> = self.keys.keys().map(AsRef::as_ref).collect();
        current.sort_unstable();
        Ok(fresh.keys == self.keys && visible == current && self.changes.is_empty())
    }
}

/// The ordered set of current keys, with recent creations and deletions by SlateDB sequence.
///
/// A pinned read view counts the keys of a Range here instead of scanning every row of the
/// interval. The apply owner records each key creation and deletion with the sequence number of
/// its SlateDB batch, so a view pinned at sequence `s` sees exactly the keys of the
/// `etcd/current/` rows that its SlateDB snapshot at `s` holds. Overwrites of a current key do not
/// change the set. History older than every pinned view is folded into the base state.
#[derive(Default)]
pub(crate) struct KeyOrder {
    keys: RwLock<Ordered>,
    /// Pinned sequence numbers and their view counts.
    pins: Mutex<BTreeMap<u64, usize>>,
}

#[derive(Default)]
struct Ordered {
    keys: BTreeMap<Arc<[u8]>, Presence>,
    /// Keys with history, by the sequence number of the change.
    history: VecDeque<(u64, Arc<[u8]>)>,
}

enum Presence {
    /// Present at every sequence that a pinned view can use.
    Live,
    /// `(seq, present)` changes in sequence order. The key is absent before the first change.
    History(Vec<(u64, bool)>),
}

impl Presence {
    fn visible(&self, seq: u64) -> bool {
        match self {
            Self::Live => true,
            Self::History(changes) => changes
                .iter()
                .rev()
                .find(|(at, _)| *at <= seq)
                .is_some_and(|(_, present)| *present),
        }
    }
}

impl KeyOrder {
    fn pin(self: &Arc<Self>, seq: u64) -> KeyPin {
        *self.pins.lock().unwrap().entry(seq).or_default() += 1;
        KeyPin {
            order: self.clone(),
            seq,
        }
    }

    fn unpin(&self, seq: u64) {
        let mut pins = self.pins.lock().unwrap();
        if let Some(count) = pins.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&seq);
            }
        }
    }

    /// Record one batch and return whether history remains to fold.
    fn apply(&self, seq: u64, changes: Vec<(Arc<[u8]>, bool)>) -> bool {
        // A view pinned later than this point sees this batch. Earlier views cannot drop below it.
        let oldest = self.pins.lock().unwrap().keys().next().copied();
        let mut ordered = self.keys.write().unwrap();
        for (key, present) in changes {
            // Without an older pinned view, the change is the base state.
            if oldest.is_none_or(|oldest| oldest >= seq) {
                if present {
                    ordered.keys.insert(key, Presence::Live);
                } else {
                    ordered.keys.remove(&key);
                }
                continue;
            }
            match ordered.keys.get_mut(&key) {
                Some(Presence::History(changes)) => match changes.last_mut() {
                    Some(last) if last.0 == seq => last.1 = present,
                    _ => changes.push((seq, present)),
                },
                Some(presence @ Presence::Live) => {
                    *presence = Presence::History(vec![(0, true), (seq, present)]);
                }
                None => {
                    ordered
                        .keys
                        .insert(key.clone(), Presence::History(vec![(seq, present)]));
                }
            }
            ordered.history.push_back((seq, key));
        }
        ordered.prune(oldest.unwrap_or(u64::MAX));
        !ordered.history.is_empty()
    }
}

impl Ordered {
    /// Fold changes at or below `oldest` into the base state. No view reads an older sequence.
    fn prune(&mut self, oldest: u64) {
        while self.history.front().is_some_and(|(seq, _)| *seq <= oldest) {
            let (_, key) = self.history.pop_front().unwrap();
            let Some(Presence::History(changes)) = self.keys.get_mut(&key) else {
                continue;
            };
            let Some(base) = changes.iter().rposition(|(seq, _)| *seq <= oldest) else {
                continue;
            };
            changes.drain(..base);
            changes[0].0 = 0;
            if changes.len() == 1 {
                if changes[0].1 {
                    self.keys.insert(key, Presence::Live);
                } else {
                    self.keys.remove(&key);
                }
            }
        }
    }
}

/// A read view's pin on the key order at one SlateDB sequence number.
pub(crate) struct KeyPin {
    order: Arc<KeyOrder>,
    seq: u64,
}

impl KeyPin {
    /// Count the current keys in `[begin, end)` at the pinned sequence. `end == [0]` means no
    /// upper bound. The interval follows `mvcc::current_bounds`.
    pub(crate) fn count(&self, begin: &[u8], end: &[u8]) -> i64 {
        let upper = if end == [0] {
            Bound::Unbounded
        } else if begin >= end {
            return 0;
        } else {
            Bound::Excluded(end)
        };
        let ordered = self.order.keys.read().unwrap();
        ordered
            .keys
            .range::<[u8], _>((Bound::Included(begin), upper))
            .filter(|(_, presence)| presence.visible(self.seq))
            .count() as i64
    }
}

impl Drop for KeyPin {
    fn drop(&mut self) {
        self.order.unpin(self.seq);
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

    fn row(key: &[u8]) -> (Vec<u8>, bytes::Bytes) {
        let kv = KeyValue {
            key: key.to_vec(),
            create_revision: 1,
            mod_revision: 1,
            version: 1,
            ..Default::default()
        };
        ([CURRENT, key].concat(), kv.encode_to_vec().into())
    }

    fn put(index: &mut KeyIndex, key: &[u8]) {
        let (key, value) = row(key);
        index.publish(&key, Some(&value)).unwrap();
    }

    fn delete(index: &mut KeyIndex, key: &[u8]) {
        index.publish(&[CURRENT, key].concat(), None).unwrap();
    }

    fn history_len(index: &KeyIndex) -> usize {
        let ordered = index.order.keys.read().unwrap();
        ordered
            .keys
            .values()
            .filter(|presence| matches!(presence, Presence::History(_)))
            .count()
    }

    #[test]
    fn pinned_counts_keep_their_sequence_until_the_pin_drops() {
        let mut index = KeyIndex::default();
        for key in [b"a", b"b", b"c"] {
            put(&mut index, key);
        }
        index.commit(1);
        let first = index.pin(1);
        assert_eq!(first.count(b"a", &[0]), 3);
        // Overwrites, a deletion, and a creation in one batch.
        put(&mut index, b"a");
        delete(&mut index, b"b");
        put(&mut index, b"d");
        index.commit(2);
        let second = index.pin(2);
        // A key deleted and created again, and a key created and then deleted.
        delete(&mut index, b"a");
        index.commit(3);
        put(&mut index, b"a");
        put(&mut index, b"e");
        index.commit(4);
        delete(&mut index, b"e");
        index.commit(5);
        let third = index.pin(5);
        for (pin, all, from_b, to_c, empty) in [
            (&first, 3, 2, 2, 0),
            (&second, 3, 2, 1, 0),
            (&third, 3, 2, 1, 0),
        ] {
            assert_eq!(pin.count(b"a", &[0]), all);
            assert_eq!(pin.count(b"b", &[0]), from_b);
            assert_eq!(pin.count(b"a", b"c"), to_c);
            assert_eq!(pin.count(b"c", b"a"), empty);
            assert_eq!(pin.count(b"c", b"c"), empty);
        }
        assert_eq!(second.count(b"a", b"b"), 1);
        assert_eq!(third.count(b"d", b"f"), 1);
        assert_eq!(index.pin(3).count(b"a", &[0]), 2);
        assert_eq!(index.pin(4).count(b"e", &[0]), 1);
        assert!(history_len(&index) > 0);
        drop((first, second));
        // The next batch folds history at or below the oldest pin into the base state.
        put(&mut index, b"f");
        index.commit(6);
        assert_eq!(history_len(&index), 1);
        assert_eq!(third.count(b"a", &[0]), 3);
        assert_eq!(index.pin(6).count(b"a", &[0]), 4);
        drop(third);
        delete(&mut index, b"f");
        index.commit(7);
        assert_eq!(history_len(&index), 0);
        assert_eq!(index.order.keys.read().unwrap().keys.len(), 3);
        assert!(index.order.pins.lock().unwrap().is_empty());
    }
}
