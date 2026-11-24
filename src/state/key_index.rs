//! Keep revision metadata for every current etcd key in apply-owner memory.
//!
//! The index follows etcd's `treeIndex`. Put, transaction, and quota paths use it instead of a
//! SlateDB point read when they do not need the old value. Every state write updates the index
//! after SlateDB accepts the batch, so the index always matches the applied `etcd/current/` rows.
//!
//! Each key has one heap allocation of 48 bytes plus the key, rounded up to the allocator size
//! class. It holds the metadata. The hash set stores one 8-byte pointer per key, and the ordered
//! map stores the pointer and an 8-byte presence slot. Hash slots are 44 to 88 percent full and
//! tree nodes are half full or more, so a key costs about 30 to 55 bytes plus its allocation. A
//! 16-byte key uses about 115 bytes with jemalloc.
//!
//! The index also groups the keys of each lease, like etcd's lessor item sets. Lease revocation,
//! `LeaseTimeToLive` with keys, and Auth lease checks then read only the keys of one lease. A
//! leased key adds one 9-byte set slot that shares the key allocation, so it costs 10 to 20 bytes
//! more. Each lease with keys adds a 57-byte map slot and its set.

mod key;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{Context, Result};
use prost::Message;

use crate::mvcc::CURRENT;
use crate::state::{StateRead, StateScan};
use key::Key;

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
    /// The current keys. Each key allocation holds the metadata of the key.
    keys: HashSet<Key>,
    /// The current keys of each lease. Entries share the key allocations of `keys`.
    leases: HashMap<i64, HashSet<Key>>,
    /// Key creations and deletions of the batch being published.
    changes: Vec<(Key, bool)>,
    order: Arc<KeyOrder>,
    /// The key order holds changes that a later commit may fold into its base state.
    history: bool,
}

impl KeyIndex {
    /// Scan all current rows once.
    pub(crate) async fn load(db: &impl StateRead) -> Result<Self> {
        let mut index = Self::default();
        let mut scan = db.scan_under(CURRENT, &Default::default()).await?;
        while let Some(row) = scan.next().await? {
            index.publish(&row.key, Some(&row.value))?;
        }
        // The scan returns keys in order, so a bulk build fills the tree nodes.
        let keys = std::mem::take(&mut index.changes)
            .into_iter()
            .map(|(key, _)| (key, None))
            .collect();
        index.order = Arc::new(KeyOrder {
            keys: RwLock::new(Ordered {
                keys,
                ..Default::default()
            }),
            ..Default::default()
        });
        Ok(index)
    }

    /// Return the metadata for a user key.
    pub(crate) fn get(&self, key: &[u8]) -> Option<&KeyMeta> {
        // Only `publish` writes the metadata, and it needs `&mut self`. `KeyOrder` never reads it.
        self.keys.get(key).map(|key| unsafe { key.meta() })
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
                if let Some(old) = self.keys.get(key) {
                    // `&mut self` excludes every reference that `get` returned.
                    let old_lease = unsafe { old.meta() }.lease;
                    unsafe { old.set_meta(meta) };
                    if old_lease != meta.lease {
                        let shared = old.clone();
                        self.unlink(old_lease, shared.as_ref());
                        self.link(meta.lease, shared);
                    }
                } else {
                    let key = Key::new(key, meta)?;
                    self.link(meta.lease, key.clone());
                    self.keys.insert(key.clone());
                    self.changes.push((key, true));
                }
            }
            None => {
                if let Some(key) = self.keys.take(key) {
                    // `&mut self` excludes every reference that `get` returned.
                    let lease = unsafe { key.meta() }.lease;
                    self.unlink(lease, key.as_ref());
                    self.changes.push((key, false));
                }
            }
        }
        Ok(())
    }

    fn link(&mut self, lease: i64, key: Key) {
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
            Some(keys) if lease != 0 => keys.iter().map(|key| key.as_ref().to_vec()).collect(),
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
    pub(crate) async fn matches(&self, db: &slatedb::Db) -> Result<bool> {
        let fresh = Self::load(db).await?;
        let latest = self.pin(u64::MAX);
        let ordered = self.order.keys.read().unwrap();
        let visible: Vec<&[u8]> = ordered
            .keys
            .iter()
            .filter(|(_, slot)| visible(slot, latest.seq))
            .map(|(key, _)| key.as_ref())
            .collect();
        let mut current: Vec<&[u8]> = self.keys.iter().map(AsRef::as_ref).collect();
        current.sort_unstable();
        let metas = fresh.keys.len() == self.keys.len()
            && fresh
                .keys
                .iter()
                .all(|key| fresh.get(key.as_ref()) == self.get(key.as_ref()));
        Ok(metas && fresh.leases == self.leases && visible == current && self.changes.is_empty())
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
    keys: BTreeMap<Key, Slot>,
    /// Keys with history, by the sequence number of the change.
    history: VecDeque<(u64, Key)>,
}

/// The presence of a key in `Ordered::keys`. `None` means `Presence::Live`, the common state. The
/// other states are boxed, so each entry uses 8 bytes.
type Slot = Option<Box<Presence>>;

fn visible(slot: &Slot, seq: u64) -> bool {
    slot.as_deref().is_none_or(|presence| presence.visible(seq))
}

/// The presence of a key over the sequences that pinned views can use.
#[derive(Debug, PartialEq)]
enum Presence {
    /// Present at every sequence that a pinned view can use.
    Live,
    /// Absent before the sequence and present from it.
    Created(u64),
    /// Present before the sequence and absent from it.
    Deleted(u64),
    /// `(seq, present)` changes in sequence order. The key is absent before the first change.
    History(Vec<(u64, bool)>),
}

impl Presence {
    fn visible(&self, seq: u64) -> bool {
        match self {
            Self::Live => true,
            Self::Created(at) => seq >= *at,
            Self::Deleted(at) => seq < *at,
            Self::History(changes) => changes
                .iter()
                .rev()
                .find(|(at, _)| *at <= seq)
                .is_some_and(|(_, present)| *present),
        }
    }

    fn changes(&self) -> Vec<(u64, bool)> {
        match self {
            Self::Live => vec![(0, true)],
            Self::Created(at) => vec![(*at, true)],
            Self::Deleted(at) => vec![(0, true), (*at, false)],
            Self::History(changes) => changes.to_vec(),
        }
    }

    fn from_changes(changes: Vec<(u64, bool)>) -> Self {
        match changes[..] {
            [(0, true)] => Self::Live,
            [(at, true)] => Self::Created(at),
            [(0, true), (at, false)] => Self::Deleted(at),
            _ => Self::History(changes),
        }
    }

    fn into_slot(self) -> Slot {
        match self {
            Self::Live => None,
            presence => Some(Box::new(presence)),
        }
    }

    /// Record a change at `seq`, the newest sequence so far. The result is never `Live`.
    fn push(&mut self, seq: u64, present: bool) {
        *self = match (&*self, present) {
            (Self::Live, false) => Self::Deleted(seq),
            _ => {
                let mut changes = self.changes();
                match changes.last_mut() {
                    Some(last) if last.0 == seq => last.1 = present,
                    _ => changes.push((seq, present)),
                }
                Self::from_changes(changes)
            }
        };
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
    fn apply(&self, seq: u64, changes: Vec<(Key, bool)>) -> bool {
        // A view pinned later than this point sees this batch. Earlier views cannot drop below it.
        let oldest = self.pins.lock().unwrap().keys().next().copied();
        let mut ordered = self.keys.write().unwrap();
        for (key, present) in changes {
            // Without an older pinned view, the change is the base state.
            if oldest.is_none_or(|oldest| oldest >= seq) {
                if present {
                    ordered.keys.insert(key, None);
                } else {
                    ordered.keys.remove(&key);
                }
                continue;
            }
            match ordered.keys.get_mut(&key) {
                Some(Some(presence)) => presence.push(seq, present),
                Some(slot @ None) => {
                    let mut presence = Presence::Live;
                    presence.push(seq, present);
                    *slot = presence.into_slot();
                }
                None => {
                    let presence = match present {
                        true => Presence::Created(seq),
                        false => Presence::History(vec![(seq, false)]),
                    };
                    ordered.keys.insert(key.clone(), presence.into_slot());
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
            let Some(slot) = self.keys.get_mut(&key) else {
                continue;
            };
            let Some(presence) = slot else {
                continue;
            };
            let folded = match &mut **presence {
                Presence::Live => continue,
                Presence::Created(at) if *at <= oldest => Presence::Live,
                Presence::Deleted(at) if *at <= oldest => {
                    self.keys.remove(&key);
                    continue;
                }
                Presence::Created(_) | Presence::Deleted(_) => continue,
                Presence::History(changes) => {
                    let Some(base) = changes.iter().rposition(|(seq, _)| *seq <= oldest) else {
                        continue;
                    };
                    let mut changes = std::mem::take(changes);
                    changes.drain(..base);
                    changes[0].0 = 0;
                    if changes[..] == [(0, false)] {
                        self.keys.remove(&key);
                        continue;
                    }
                    Presence::from_changes(changes)
                }
            };
            *slot = folded.into_slot();
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
            .filter(|(_, slot)| visible(slot, self.seq))
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
        ordered.keys.values().filter(|slot| slot.is_some()).count()
    }

    #[test]
    fn pinned_counts_keep_their_sequence_until_the_pin_drops() {
        assert_eq!(size_of::<Slot>(), 8);
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

    #[test]
    fn pinned_counts_match_a_model_under_random_batches() {
        use std::collections::BTreeSet;
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };
        let mut index = KeyIndex::default();
        let mut current = BTreeSet::new();
        // Each pin with the key set at its sequence.
        let mut pins: Vec<(KeyPin, BTreeSet<Vec<u8>>)> = vec![];
        for seq in 1..2000 {
            for _ in 0..=next(4) {
                let key = vec![b'a' + next(16) as u8];
                if next(3) == 0 {
                    delete(&mut index, &key);
                    current.remove(&key);
                } else {
                    put(&mut index, &key);
                    current.insert(key);
                }
            }
            index.commit(seq);
            if next(3) == 0 {
                pins.push((index.pin(seq), current.clone()));
            }
            if !pins.is_empty() && next(3) == 0 {
                pins.swap_remove(next(pins.len() as u64) as usize);
            }
            for (pin, keys) in &pins {
                let (begin, end) = (vec![b'a' + next(16) as u8], vec![b'a' + next(17) as u8]);
                let expected = keys
                    .iter()
                    .filter(|key| **key >= begin && (end == b"q" || **key < end))
                    .count();
                let end = if end == b"q" { vec![0] } else { end };
                assert_eq!(pin.count(&begin, &end), expected as i64, "seq {seq}");
            }
        }
        drop(pins);
        index.commit(2000);
        let ordered = index.order.keys.read().unwrap();
        assert!(ordered.history.is_empty());
        assert!(ordered.keys.values().all(Option::is_none));
        let live: Vec<&[u8]> = ordered.keys.keys().map(AsRef::as_ref).collect();
        let current: Vec<&[u8]> = current.iter().map(Vec::as_slice).collect();
        assert_eq!(live, current);
    }

    /// Heap bytes in use, from the glibc allocator of the test binary.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn heap_in_use() -> usize {
        // mallinfo2 only reads allocator counters.
        let info = unsafe { libc::mallinfo2() };
        info.uordblks + info.hblkhd
    }

    /// Report the heap bytes per key of the index. Run it alone:
    /// `cargo test --lib key_index_memory -- --ignored --nocapture --test-threads=1`.
    #[test]
    #[ignore]
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn key_index_memory() {
        // `KEY_INDEX_MEMORY_KEYS` sets the key count. The default is one million.
        let keys_env = std::env::var("KEY_INDEX_MEMORY_KEYS").ok();
        let keys_env = keys_env.and_then(|keys| keys.parse().ok());
        let total: usize = keys_env.unwrap_or(1_000_000);
        let key = |i: usize, len: usize| {
            // Spread keys so that incremental inserts land in random tree positions.
            let mut key = format!("{:016x}", (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
            key.extend(std::iter::repeat_n('k', len.saturating_sub(16)));
            key.into_bytes()
        };
        for len in [16, 64, 256] {
            for sorted in [true, false] {
                let mut keys: Vec<Vec<u8>> = (0..total).map(|i| key(i, len)).collect();
                if sorted {
                    keys.sort_unstable();
                }
                let before = heap_in_use();
                let mut index = KeyIndex::default();
                let start = std::time::Instant::now();
                // Each applied batch publishes a read view, so the previous one stays pinned.
                let mut pinned = None;
                for (i, key) in keys.iter().enumerate() {
                    let kv = KeyValue {
                        key: key.clone(),
                        create_revision: i as i64 + 1,
                        mod_revision: i as i64 + 1,
                        version: 1,
                        value: vec![0; 8],
                        lease: 0,
                    };
                    let value = bytes::Bytes::from(kv.encode_to_vec());
                    index
                        .publish(&[CURRENT, key].concat(), Some(&value))
                        .unwrap();
                    // The startup load commits once. Writes commit each Raft batch.
                    if !sorted && i % 32 == 31 {
                        index.commit(i as u64);
                        pinned = Some(index.pin(i as u64));
                    }
                }
                drop(pinned);
                index.commit(total as u64);
                let write_ns = start.elapsed().as_nanos() as f64 / total as f64;
                let used = heap_in_use() - before;
                assert_eq!(index.len(), total);
                // Time point lookups in insertion order and one full count.
                let start = std::time::Instant::now();
                let mut found = 0;
                for key in &keys {
                    found += usize::from(index.get(std::hint::black_box(key)).is_some());
                }
                let get_ns = start.elapsed().as_nanos() as f64 / total as f64;
                let start = std::time::Instant::now();
                let counted = index.pin(u64::MAX).count(b"", &[0]);
                let count_ns = start.elapsed().as_nanos() as f64 / total as f64;
                assert_eq!((found, counted), (total, total as i64));
                println!(
                    "key_len={len} order={} bytes_per_key={:.1} write_ns={write_ns:.0} get_ns={get_ns:.0} \
                     count_ns_per_key={count_ns:.1}",
                    if sorted {
                        "sorted-load"
                    } else {
                        "random-writes"
                    },
                    used as f64 / total as f64,
                );
                drop(index);
            }
        }
    }

    /// The CPU time of this thread, so that other load on the host does not count.
    fn cpu_now() -> std::time::Duration {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // The kernel fills the timespec.
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) },
            0
        );
        std::time::Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    }

    /// Time `publish` and `commit` in the txn-put-ops1 pattern: random 32-byte keys from a
    /// 100,000-key space, 256 keys per batch, and the previous batch's view pinned. Run it alone:
    /// `cargo test --release --lib key_index_apply_cost -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn key_index_apply_cost() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for preload in [0_usize, 1_000_000] {
            let mut best = f64::MAX;
            let mut best_publish = 0.0;
            for _ in 0..15 {
                let rows: Vec<(Vec<u8>, bytes::Bytes)> = (0..50_000)
                    .map(|_| {
                        let key = format!("{:032}", next() % 100_000).into_bytes();
                        let kv = KeyValue {
                            key: key.clone(),
                            create_revision: 1,
                            mod_revision: 1,
                            version: 1,
                            value: vec![0; 256],
                            lease: 0,
                        };
                        ([CURRENT, &key].concat(), kv.encode_to_vec().into())
                    })
                    .collect();
                let mut index = KeyIndex::default();
                for i in 0..preload {
                    let (key, value) = row(format!("p{i:031}").as_bytes());
                    index.publish(&key, Some(&value)).unwrap();
                }
                index.commit(1);
                let mut pinned = Some(index.pin(1));
                let start = cpu_now();
                let mut publish = std::time::Duration::ZERO;
                for (batch, rows) in rows.chunks(256).enumerate() {
                    let at = cpu_now();
                    for (key, value) in rows {
                        index.publish(key, Some(value)).unwrap();
                    }
                    publish += cpu_now() - at;
                    let seq = batch as u64 + 2;
                    index.commit(seq);
                    pinned = Some(index.pin(seq));
                }
                drop(pinned);
                let total = (cpu_now() - start).as_nanos() as f64 / rows.len() as f64;
                if total < best {
                    best = total;
                    best_publish = publish.as_nanos() as f64 / rows.len() as f64;
                }
            }
            println!(
                "apply_cost preload={preload} best_ns_per_put={best:.0} publish={best_publish:.0}"
            );
        }
    }
}
