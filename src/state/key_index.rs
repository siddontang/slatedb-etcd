//! Keep revision metadata for every current etcd key in apply-owner memory.
//!
//! The index follows etcd's `treeIndex`. Put, transaction, and quota paths use it instead of a
//! SlateDB point read when they do not need the old value. Every state write updates the index
//! after SlateDB accepts the batch, so the index always matches the applied `etcd/current/` rows.
//!
//! Each hash table slot uses 57 bytes on 64-bit hosts: a boxed key, five integer fields, and a
//! control byte. Slots are 44 to 88 percent full, so a key costs 65 to 130 bytes plus its key
//! allocation.
//!
//! The index also groups the keys of each lease, like etcd's lessor item sets. Lease revocation,
//! `LeaseTimeToLive` with keys, and Auth lease checks then read only the keys of one lease. A
//! leased key adds one 17-byte set slot that shares the key allocation, so it costs 19 to 39 bytes
//! more. Each lease with keys adds a 57-byte map slot and its set.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{Context, Result};
use prost::Message;

use crate::mvcc::CURRENT;
use crate::state::view::StateView;
use crate::state::{StateRead, StateScan, CURRENT_END};

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

/// The most parallel scans that load the index: one per CPU of this process, at most 8.
fn scan_parts() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get().min(8))
}

/// Split `etcd/current/` into at most `parts` ascending, disjoint ranges that cover it. The
/// split keys come from `firsts`, the first keys of the compacted SSTs, so each part reads about
/// whole SSTs. Any split keys give the same rows; the SSTs only balance the parts.
fn scan_bounds<'a>(firsts: impl IntoIterator<Item = &'a [u8]>, parts: usize) -> Vec<Vec<u8>> {
    let mut firsts: Vec<&[u8]> = firsts
        .into_iter()
        .filter(|key| *key > CURRENT && *key < CURRENT_END)
        .collect();
    firsts.sort_unstable();
    firsts.dedup();
    let mut bounds = vec![CURRENT.to_vec()];
    // Pick `parts - 1` split keys spread over the candidates, each above the last one.
    for part in 1..parts.max(1) {
        if let Some(key) = firsts.get(part * firsts.len() / parts) {
            if bounds.last().is_some_and(|last| last.as_slice() < *key) {
                bounds.push(key.to_vec());
            }
        }
    }
    bounds.push(CURRENT_END.to_vec());
    bounds
}

/// Scan the ranges between consecutive `bounds` of one snapshot in parallel tasks, and return
/// the rows of each range in order.
async fn scan_parts_of(
    snapshot: Arc<slatedb::DbSnapshot>,
    bounds: &[Vec<u8>],
) -> Result<Vec<Vec<(Arc<[u8]>, KeyMeta)>>> {
    let mut tasks = tokio::task::JoinSet::new();
    for (part, range) in bounds.windows(2).enumerate() {
        let (snapshot, range) = (snapshot.clone(), range[0].clone()..range[1].clone());
        tasks.spawn(async move { (part, scan_meta(snapshot.as_ref(), range).await) });
    }
    let mut parts = vec![Vec::new(); bounds.len().saturating_sub(1)];
    while let Some(done) = tasks.join_next().await {
        let (part, rows) = done.context("key index scan task failed")?;
        parts[part] = rows?;
    }
    Ok(parts)
}

/// Read the metadata of the current rows in `range`, in key order.
async fn scan_meta(
    db: &impl StateRead,
    range: std::ops::Range<Vec<u8>>,
) -> Result<Vec<(Arc<[u8]>, KeyMeta)>> {
    let options = slatedb::config::ScanOptions {
        read_ahead_bytes: 1024 * 1024,
        max_fetch_tasks: 4,
        ..Default::default()
    };
    let mut scan = db.scan_range(range, &options).await?;
    let mut rows = Vec::new();
    while let Some(row) = scan.next().await? {
        let Some(key) = row.key.strip_prefix(CURRENT) else {
            continue;
        };
        rows.push((key.into(), KeyMeta::decode(&row.value)?));
    }
    Ok(rows)
}

#[derive(Default)]
pub(crate) struct KeyIndex {
    keys: HashMap<Arc<[u8]>, KeyMeta>,
    /// The current keys of each lease. Entries share the key allocations of `keys`.
    leases: HashMap<i64, HashSet<Arc<[u8]>>>,
    /// Key creations and deletions of the batch being published.
    changes: Vec<(Arc<[u8]>, bool)>,
    order: Arc<KeyOrder>,
    /// The key order holds changes that a later commit may fold into its base state.
    history: bool,
}

impl KeyIndex {
    /// Scan all current rows once.
    ///
    /// Startup time grows with this scan: it reads every current value to decode a few integer
    /// fields. A live database splits the key range at SST boundaries and scans the parts of one
    /// snapshot in parallel tasks. The index is then built from the parts in key order, and the
    /// key order in bulk from the sorted rows instead of one tree insertion per key.
    pub(crate) async fn load(reads: &StateView) -> Result<Self> {
        let started = std::time::Instant::now();
        let parts = match reads {
            StateView::Live(db) => {
                let manifest = db.manifest();
                let firsts = manifest
                    .compacted()
                    .iter()
                    .flat_map(|run| run.sst_views())
                    .filter_map(|view| view.sst.info.first_entry.as_deref());
                let bounds = scan_bounds(firsts, scan_parts());
                // One snapshot gives every part the same sequence.
                scan_parts_of(db.snapshot()?, &bounds).await?
            }
            _ => vec![scan_meta(reads, CURRENT.to_vec()..CURRENT_END.to_vec()).await?],
        };
        let scans = parts.len();
        let mut index = Self::default();
        let mut sorted = Vec::with_capacity(parts.iter().map(Vec::len).sum());
        for (key, meta) in parts.into_iter().flatten() {
            index.link(meta.lease, key.clone());
            index.keys.insert(key.clone(), meta);
            sorted.push((key, Presence::Live));
        }
        // The parts are disjoint and ascending, and each returns its keys once in ascending
        // order, so `collect` builds the tree in bulk.
        let keys = sorted.into_iter().collect();
        index.order = Arc::new(KeyOrder {
            keys: RwLock::new(Ordered {
                keys,
                history: VecDeque::new(),
            }),
            pins: Mutex::default(),
        });
        tracing::info!(
            keys = index.keys.len(),
            scans,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "loaded key index"
        );
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
                    let old_lease = std::mem::replace(old, meta).lease;
                    if old_lease != meta.lease {
                        let shared = self.keys.get_key_value(key).unwrap().0.clone();
                        self.unlink(old_lease, &shared);
                        self.link(meta.lease, shared);
                    }
                } else {
                    let key: Arc<[u8]> = key.into();
                    self.link(meta.lease, key.clone());
                    self.keys.insert(key.clone(), meta);
                    self.changes.push((key, true));
                }
            }
            None => {
                if let Some((key, meta)) = self.keys.remove_entry(key) {
                    self.unlink(meta.lease, &key);
                    self.changes.push((key, false));
                }
            }
        }
        Ok(())
    }

    fn link(&mut self, lease: i64, key: Arc<[u8]>) {
        if lease != 0 {
            self.leases.entry(lease).or_default().insert(key);
        }
    }

    fn unlink(&mut self, lease: i64, key: &[u8]) {
        if lease == 0 {
            return;
        }
        if let Some(keys) = self.leases.get_mut(&lease) {
            keys.remove(key);
            if keys.is_empty() {
                self.leases.remove(&lease);
            }
        }
    }

    /// Visit the current keys attached to a lease in no particular order.
    pub(crate) fn lease_keys_unordered(&self, lease: i64) -> impl Iterator<Item = &[u8]> {
        self.leases
            .get(&lease)
            .filter(|_| lease != 0)
            .into_iter()
            .flatten()
            .map(AsRef::as_ref)
    }

    /// Return the current keys attached to a lease in key order.
    pub(crate) fn lease_keys(&self, lease: i64) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = match self.leases.get(&lease) {
            Some(keys) if lease != 0 => keys.iter().map(|key| key.to_vec()).collect(),
            _ => return vec![],
        };
        keys.sort_unstable();
        keys
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
    pub(crate) async fn matches(&self, db: &Arc<slatedb::Db>) -> Result<bool> {
        let fresh = Self::load(&StateView::Live(db.clone())).await?;
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
        Ok(fresh.keys == self.keys
            && fresh.leases == self.leases
            && visible == current
            && self.changes.is_empty())
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
    /// The SlateDB sequence number of the pinned view.
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

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

    /// The startup load reads rows from several SSTs and the memtable with read-ahead. It must
    /// match an index that applied the same mutations one by one.
    #[tokio::test]
    async fn load_matches_incremental_publish() {
        use slatedb::object_store::memory::InMemory;
        // Without the block cache, the load reads the flushed blocks from the object store.
        let db = slatedb::Db::builder("db", Arc::new(InMemory::new()))
            .with_db_cache_disabled()
            .build()
            .await
            .unwrap();
        let db = Arc::new(db);
        let mut expected = KeyIndex::default();
        // `step` makes an overwrite differ from the flushed row in its revisions and lease.
        let write = |batch: &mut slatedb::WriteBatch, index: &mut KeyIndex, i: u32, step: i64| {
            let key = format!("key/{i:05}").into_bytes();
            let kv = KeyValue {
                key: key.clone(),
                create_revision: i64::from(i) + 1,
                mod_revision: i64::from(i) + 2 + step,
                version: 1 + step,
                lease: (i64::from(i) + step) % 7,
                value: vec![1; 2048],
            };
            let (row_key, value) = ([CURRENT, &key].concat(), kv.encode_to_vec().into());
            batch.put(&row_key, &value);
            index.publish(&row_key, Some(&value)).unwrap();
        };
        // Two L0 SSTs of about 4 MiB each, larger than one read-ahead request.
        let memtable = slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        };
        for part in 0..2 {
            let mut batch = slatedb::WriteBatch::new();
            for i in (part..4000).step_by(2) {
                write(&mut batch, &mut expected, i, 0);
            }
            db.write(batch).await.unwrap();
            db.flush_with_options(memtable.clone()).await.unwrap();
        }
        assert_eq!(db.manifest().l0().len(), 2);
        // Unflushed changes: deletes, overwrites, and rows next to the prefix.
        let mut batch = slatedb::WriteBatch::new();
        for i in (0..4000).step_by(3) {
            let row_key = [CURRENT, format!("key/{i:05}").as_bytes()].concat();
            batch.delete(&row_key);
            expected.publish(&row_key, None).unwrap();
        }
        for i in (1..4000).step_by(9) {
            write(&mut batch, &mut expected, i, 1);
        }
        batch.put(b"etcd/curren", b"x");
        batch.put(b"etcd/current0", b"x");
        db.write(batch).await.unwrap();
        expected.commit(0);

        assert!(expected.matches(&db).await.unwrap());
        let loaded = KeyIndex::load(&StateView::Live(db.clone())).await.unwrap();
        assert_eq!(loaded.len(), expected.len());
        assert_eq!(loaded.pin(u64::MAX).count(b"", &[0]), loaded.len() as i64);
        // The ordered user keys, not only their number, match the visible keys.
        let visible = |index: &KeyIndex| -> Vec<Arc<[u8]>> {
            let ordered = index.order.keys.read().unwrap();
            ordered
                .keys
                .iter()
                .filter(|(_, presence)| presence.visible(u64::MAX))
                .map(|(key, _)| key.clone())
                .collect()
        };
        assert_eq!(visible(&loaded), visible(&expected));
        assert_eq!(loaded.lease_keys(3), expected.lease_keys(3));

        // Parallel parts return the rows of one full scan, with split keys on a row, between
        // rows, on a deleted row, and past the last row.
        let full = scan_meta(db.as_ref(), CURRENT.to_vec()..CURRENT_END.to_vec())
            .await
            .unwrap();
        let splits: [&[u8]; 4] = [b"key/00001", b"key/01000x", b"key/02001", b"zzz"];
        let row_keys: Vec<Vec<u8>> = splits.iter().map(|key| [CURRENT, key].concat()).collect();
        let bounds = scan_bounds(row_keys.iter().map(Vec::as_slice), 8);
        assert_eq!(bounds.len(), 6);
        let parts = scan_parts_of(db.snapshot().unwrap(), &bounds)
            .await
            .unwrap();
        assert!(parts[1..4].iter().all(|part| !part.is_empty()));
        assert_eq!(parts.concat(), full);
        db.close().await.unwrap();
    }

    #[test]
    fn scan_bounds_cover_the_current_prefix() {
        let key = |name: &str| [CURRENT, name.as_bytes()].concat();
        let whole = vec![CURRENT.to_vec(), CURRENT_END.to_vec()];
        // No SSTs, or one part: a single scan.
        assert_eq!(scan_bounds([], 4), whole);
        assert_eq!(scan_bounds([key("b").as_slice()], 1), whole);
        // Keys outside the prefix and the prefix itself are no split keys.
        let outside = [
            b"etcd/apply".as_slice(),
            CURRENT,
            CURRENT_END,
            b"etcd/hash/1",
        ];
        assert_eq!(scan_bounds(outside, 4), whole);
        // Duplicate and unsorted first keys give ascending, distinct bounds.
        let (b, c, d) = (key("b"), key("c"), key("d"));
        let firsts = [
            d.as_slice(),
            b.as_slice(),
            c.as_slice(),
            b.as_slice(),
            d.as_slice(),
        ];
        let expected = vec![
            CURRENT.to_vec(),
            b.clone(),
            c.clone(),
            d.clone(),
            CURRENT_END.to_vec(),
        ];
        assert_eq!(scan_bounds(firsts, 8), expected);
        // Fewer parts than first keys spread the split keys.
        assert_eq!(
            scan_bounds(firsts, 2),
            vec![CURRENT.to_vec(), c.clone(), CURRENT_END.to_vec()]
        );
    }
}
