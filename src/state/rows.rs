//! Share current rows between linearizable point reads.
//!
//! A SlateDB point lookup builds a merged iterator over memtables, L0 SSTs, and sorted runs, even
//! when the key is in the block cache. This cache sits in front of it for `etcd/current/` rows read
//! from pinned read views.
//!
//! Every entry records the sequence `written` at which its row was read. Each set records a `fence`:
//! the highest apply index that changed or may change one of its keys. These rules keep a hit equal to
//! the SlateDB row at the reader's sequence `seq`:
//!
//! - The apply worker calls `invalidate` for every key of a state batch before SlateDB receives the
//!   batch. It raises the fence and drops the entry. Readers at `seq >= index` exist only after the
//!   write succeeded, so no reader can see a cached row older than a write it must observe.
//! - A reader fills an entry only when `fence <= seq`, under the same lock as invalidation. Then no
//!   write after `seq` touched the key, and the row read at `seq` is still the latest row.
//! - A hit requires `written <= seq`. The row did not change between `written` and the present, so it
//!   is the row at `seq` too. A reader with an older pinned view falls back to storage.
//!
//! Writes only invalidate. Only reads fill, so write-only workloads keep no rows here.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::Mutex;

const SHARDS: usize = 64;
const WAYS: usize = 8;
/// Larger rows go to storage each time. They would take budget from many small rows.
const MAX_ROW: usize = 4096;
/// Charge for the entry slot and the allocator, in addition to the row bytes.
const ENTRY_OVERHEAD: usize = 48;
/// Entry size used to size the table. Larger rows reach the byte budget before the table fills.
const SIZING_ENTRY: usize = 512;

/// Return the key of an encoded `mvccpb.KeyValue`. Prost writes field 1 first.
fn row_key(mut row: &[u8]) -> Option<&[u8]> {
    if row.first() != Some(&0x0a) {
        return None;
    }
    row = &row[1..];
    let len = usize::try_from(prost::encoding::decode_varint(&mut row).ok()?).ok()?;
    row.get(..len)
}

struct Entry {
    /// The encoded row. It holds its key, so the cache stores no separate copy.
    row: Box<[u8]>,
    written: u64,
}

impl Entry {
    fn holds(&self, key: &[u8]) -> bool {
        row_key(&self.row) == Some(key)
    }

    fn cost(&self) -> usize {
        self.row.len() + ENTRY_OVERHEAD
    }
}

#[derive(Default)]
struct Set {
    fence: u64,
    hand: u8,
    /// One bit per way: read since the last CLOCK pass.
    referenced: u8,
    ways: [Option<Entry>; WAYS],
}

#[derive(Default)]
struct Shard {
    /// Allocated on the first fill. Until then `fence` covers every set.
    sets: Vec<Set>,
    fence: u64,
    bytes: usize,
}

pub(crate) struct RowCache {
    hash: RandomState,
    shards: Box<[Mutex<Shard>]>,
    sets: usize,
    shard_budget: usize,
}

impl RowCache {
    /// A budget of zero disables the cache.
    pub(crate) fn new(budget: u64) -> Self {
        let budget = usize::try_from(budget).unwrap_or(usize::MAX);
        let sets = if budget == 0 {
            0
        } else {
            (budget / SIZING_ENTRY / WAYS / SHARDS).max(1)
        };
        Self {
            hash: RandomState::new(),
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
            sets,
            shard_budget: budget / SHARDS,
        }
    }

    fn enabled(&self) -> bool {
        self.sets > 0
    }

    fn locate(&self, key: &[u8]) -> (usize, usize) {
        let hash = self.hash.hash_one(key) as usize;
        (hash % SHARDS, (hash / SHARDS) % self.sets)
    }

    fn lock(&self, shard: usize) -> std::sync::MutexGuard<'_, Shard> {
        // A panic under this lock cannot leave a stale row: entries change only by removal or by
        // a complete insert.
        self.shards[shard]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Apply `read` to the current row of `key` at sequence `seq`, if the cache can prove the row.
    pub(crate) fn get<R>(&self, key: &[u8], seq: u64, read: impl FnOnce(&[u8]) -> R) -> Option<R> {
        if !self.enabled() {
            return None;
        }
        let (shard, set) = self.locate(key);
        let mut shard = self.lock(shard);
        let set = shard.sets.get_mut(set)?;
        let way = set
            .ways
            .iter()
            .position(|entry| entry.as_ref().is_some_and(|entry| entry.holds(key)))?;
        let entry = set.ways[way].as_ref()?;
        if entry.written > seq {
            return None;
        }
        set.referenced |= 1 << way;
        Some(read(&entry.row))
    }

    /// Remember `row`, the current row of `key` read at sequence `seq`.
    pub(crate) fn fill(&self, key: &[u8], row: &[u8], seq: u64) {
        if !self.enabled() || row.len() > MAX_ROW || row_key(row) != Some(key) {
            return;
        }
        let (shard, index) = self.locate(key);
        let mut shard = self.lock(shard);
        if shard.sets.is_empty() {
            let fence = shard.fence;
            shard.sets = (0..self.sets)
                .map(|_| Set {
                    fence,
                    ..Set::default()
                })
                .collect();
        }
        let Shard { sets, bytes, .. } = &mut *shard;
        let set = &mut sets[index];
        if set.fence > seq || set.ways.iter().flatten().any(|entry| entry.holds(key)) {
            return;
        }
        // CLOCK over the ways: take an empty way, or the first one not read since the last pass.
        let way = match set.ways.iter().position(Option::is_none) {
            Some(way) => way,
            None => loop {
                let way = usize::from(set.hand);
                set.hand = ((way + 1) % WAYS) as u8;
                if set.referenced & (1 << way) == 0 {
                    break way;
                }
                set.referenced &= !(1 << way);
            },
        };
        let freed = set.ways[way].as_ref().map_or(0, Entry::cost);
        let cost = row.len() + ENTRY_OVERHEAD;
        if *bytes - freed + cost > self.shard_budget {
            return;
        }
        *bytes = *bytes - freed + cost;
        // Own only this row. A slice of a SlateDB block would retain the whole block.
        set.ways[way] = Some(Entry {
            row: row.into(),
            written: seq,
        });
        set.referenced &= !(1 << way);
    }

    /// Drop `key` before the apply worker stores a change to it at `index`.
    pub(crate) fn invalidate(&self, key: &[u8], index: u64) {
        if !self.enabled() {
            return;
        }
        let (shard, set) = self.locate(key);
        let mut shard = self.lock(shard);
        let Shard { sets, fence, bytes } = &mut *shard;
        let Some(set) = sets.get_mut(set) else {
            *fence = (*fence).max(index);
            return;
        };
        set.fence = set.fence.max(index);
        for way in &mut set.ways {
            if way.as_ref().is_some_and(|entry| entry.holds(key)) {
                *bytes -= way.take().map_or(0, |entry| entry.cost());
            }
        }
    }

    #[cfg(test)]
    fn bytes(&self) -> usize {
        (0..SHARDS).map(|shard| self.lock(shard).bytes).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    fn row(key: &[u8], value: &[u8]) -> Vec<u8> {
        crate::proto::mvccpb::KeyValue {
            key: key.to_vec(),
            value: value.to_vec(),
            mod_revision: 2,
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn value(cache: &RowCache, key: &[u8], seq: u64) -> Option<Vec<u8>> {
        cache.get(key, seq, |row| {
            crate::proto::mvccpb::KeyValue::decode(row).unwrap().value
        })
    }

    #[test]
    fn hits_need_a_row_no_newer_than_the_reader() {
        let cache = RowCache::new(1 << 20);
        cache.fill(b"a", &row(b"a", b"v5"), 5);
        assert_eq!(value(&cache, b"a", 5).unwrap(), b"v5");
        assert_eq!(value(&cache, b"a", 9).unwrap(), b"v5");
        // A reader pinned before the fill cannot prove the row and reads storage.
        assert!(value(&cache, b"a", 4).is_none());
        assert!(value(&cache, b"b", 9).is_none());
        // A row must hold the key it is filled under.
        cache.fill(b"b", &row(b"c", b"v5"), 5);
        assert!(value(&cache, b"b", 9).is_none());
        assert!(value(&cache, b"c", 9).is_none());
    }

    #[test]
    fn invalidation_drops_rows_and_fences_late_fills() {
        let cache = RowCache::new(1 << 20);
        cache.fill(b"a", &row(b"a", b"old"), 5);
        cache.invalidate(b"a", 7);
        assert!(value(&cache, b"a", 9).is_none());
        // A reader at 6 read the row before the write at 7. It must not install it.
        cache.fill(b"a", &row(b"a", b"old"), 6);
        assert!(value(&cache, b"a", 9).is_none());
        cache.fill(b"a", &row(b"a", b"new"), 7);
        assert_eq!(value(&cache, b"a", 7).unwrap(), b"new");
    }

    #[test]
    fn invalidation_before_the_first_fill_fences_new_sets() {
        let cache = RowCache::new(1 << 20);
        cache.invalidate(b"a", 7);
        cache.fill(b"a", &row(b"a", b"old"), 6);
        assert!(value(&cache, b"a", 9).is_none());
        cache.fill(b"a", &row(b"a", b"new"), 8);
        assert_eq!(value(&cache, b"a", 8).unwrap(), b"new");
    }

    #[test]
    fn zero_budget_disables_and_large_rows_are_skipped() {
        let off = RowCache::new(0);
        off.invalidate(b"a", 1);
        off.fill(b"a", &row(b"a", b"v"), 1);
        assert!(value(&off, b"a", 1).is_none());
        let cache = RowCache::new(1 << 20);
        cache.fill(b"a", &row(b"a", &vec![0; MAX_ROW]), 1);
        assert!(value(&cache, b"a", 1).is_none());
    }

    #[test]
    fn budget_bounds_bytes_and_eviction_keeps_accounting() {
        let budget = SHARDS * 4096;
        let cache = RowCache::new(budget as u64);
        for i in 0..10_000u32 {
            let key = i.to_be_bytes();
            cache.fill(&key, &row(&key, &[7; 1000]), 1);
            assert!(cache.bytes() <= budget);
        }
        let cached: Vec<u32> = (0..10_000u32)
            .filter(|i| value(&cache, &i.to_be_bytes(), 1).is_some())
            .collect();
        assert!(!cached.is_empty());
        for i in &cached {
            cache.invalidate(&i.to_be_bytes(), 2);
        }
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn clock_keeps_recently_read_rows() {
        let cache = RowCache::new((SHARDS * SIZING_ENTRY * WAYS) as u64);
        assert_eq!(cache.sets, 1);
        // Fill one set past its ways while one key is read before each new fill.
        let hot = 0u32.to_be_bytes();
        let (hot_shard, _) = cache.locate(&hot);
        cache.fill(&hot, &row(&hot, b"v"), 1);
        let mut filled = 0;
        for i in 1..100_000u32 {
            let key = i.to_be_bytes();
            if cache.locate(&key).0 != hot_shard {
                continue;
            }
            assert!(value(&cache, &hot, 1).is_some());
            cache.fill(&key, &row(&key, b"v"), 1);
            filled += 1;
            if filled == 4 * WAYS {
                break;
            }
        }
        assert!(value(&cache, &hot, 1).is_some());
    }
}
