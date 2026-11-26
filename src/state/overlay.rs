//! Keep the applied batches above the base checkpoint of a shared-mode member.
//!
//! See `docs/shared-state.md`. The overlay keeps each apply batch twice, and both copies share
//! the batch's `Bytes`:
//!
//! - The queue holds whole batches in sequence order. The leader materializer writes them into
//!   the shared `Db`.
//! - Segments hold the final row of each key at each batch sequence. Views read them.
//!
//! Apply is the only writer. A `ViewCheckpoint` entry closes the open segment. A base switch
//! releases the closed segments that the new base covers. Views keep `Arc` references, so a
//! released segment stays in memory until the last view that uses it drops.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;

use crate::WriteBatch;

/// The versions of one key in a segment, in ascending sequence order. `None` is a delete.
type Versions = Vec<(u64, Option<Bytes>)>;

/// The rows of consecutive batches. Only the open segment receives new batches.
#[derive(Default)]
pub(crate) struct Segment {
    rows: RwLock<BTreeMap<Bytes, Versions>>,
    /// The sequence of the newest batch. Zero means that the segment is empty.
    last: AtomicU64,
    /// Key and value bytes of all versions.
    bytes: AtomicU64,
}

/// The upper end of a scan.
pub(crate) enum End<'a> {
    /// Keys below this key.
    Before(&'a [u8]),
    /// Keys that start with this prefix.
    Prefix(&'a [u8]),
}

impl End<'_> {
    fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::Before(end) => key < *end,
            Self::Prefix(prefix) => key.starts_with(prefix),
        }
    }
}

impl Segment {
    fn insert(&self, seq: u64, batch: &WriteBatch) {
        let mut rows = self.rows.write().unwrap();
        let mut added = 0;
        for (key, value) in batch.rows() {
            added += (key.len() + value.map_or(0, Bytes::len)) as u64;
            rows.entry(key.clone())
                .or_default()
                .push((seq, value.cloned()));
        }
        self.bytes.fetch_add(added, Ordering::Relaxed);
        self.last.store(seq, Ordering::Release);
    }

    /// Key and value bytes of all versions.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// The sequence of the newest batch, or zero for an empty segment.
    pub(crate) fn last(&self) -> u64 {
        self.last.load(Ordering::Acquire)
    }

    /// Return the newest version of `key` at or below `seq`, if the segment has one.
    pub(crate) fn get(&self, key: &[u8], seq: u64) -> Option<Option<Bytes>> {
        let rows = self.rows.read().unwrap();
        newest(rows.get(key)?, seq)
    }

    /// Return the first key after `lower` that has a version at or below `seq`, with that
    /// version. Keys at or after `limit` are not useful to the caller.
    pub(crate) fn first_after(
        &self,
        lower: Bound<&[u8]>,
        end: &End<'_>,
        seq: u64,
        limit: Option<&[u8]>,
    ) -> Option<(Bytes, Option<Bytes>)> {
        let rows = self.rows.read().unwrap();
        for (key, versions) in rows.range::<[u8], _>((lower, Bound::Unbounded)) {
            if !end.contains(key) || limit.is_some_and(|limit| key.as_ref() >= limit) {
                return None;
            }
            if let Some(value) = newest(versions, seq) {
                return Some((key.clone(), value));
            }
        }
        None
    }

    /// Return the last key in `[start, upper)` within `end` that has a version at or below
    /// `seq`, with that version. Keys at or before `limit` are not useful to the caller.
    pub(crate) fn last_before(
        &self,
        start: &[u8],
        upper: Bound<&[u8]>,
        end: &End<'_>,
        seq: u64,
        limit: Option<&[u8]>,
    ) -> Option<(Bytes, Option<Bytes>)> {
        let rows = self.rows.read().unwrap();
        for (key, versions) in rows.range::<[u8], _>((Bound::Included(start), upper)).rev() {
            if limit.is_some_and(|limit| key.as_ref() <= limit) {
                return None;
            }
            if !end.contains(key) {
                continue;
            }
            if let Some(value) = newest(versions, seq) {
                return Some((key.clone(), value));
            }
        }
        None
    }
}

fn newest(versions: &Versions, seq: u64) -> Option<Option<Bytes>> {
    versions
        .iter()
        .rev()
        .find(|(version, _)| *version <= seq)
        .map(|(_, value)| value.clone())
}

/// The apply position of an overlay.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Position {
    /// The sequence of the newest batch. Before any batch it is the base index.
    pub pushed: u64,
    /// The applied index through which the stored batches are the whole state. Entries after
    /// the newest batch that change no state advance it. It equals `applied` after each entry.
    pub covered: u64,
}

/// Whole batches in sequence order for the materializer.
pub(crate) struct Queue {
    batches: Mutex<VecDeque<(u64, Arc<WriteBatch>)>>,
    /// One channel sends both values, so a reader gets them from the same applied state.
    position: tokio::sync::watch::Sender<Position>,
    /// The base index of the last release. The base holds every batch at or below it.
    released: AtomicU64,
}

impl Queue {
    fn new(base: u64) -> Self {
        Self {
            batches: Mutex::new(VecDeque::new()),
            position: tokio::sync::watch::channel(Position {
                pushed: base,
                covered: base,
            })
            .0,
            released: AtomicU64::new(base),
        }
    }

    /// Return up to `limit` batches above `seq`, in order.
    pub(crate) fn after(&self, seq: u64, limit: usize) -> Vec<(u64, Arc<WriteBatch>)> {
        let batches = self.batches.lock().unwrap();
        let start = batches.partition_point(|(batch, _)| *batch <= seq);
        batches.range(start..).take(limit).cloned().collect()
    }

    /// The sequence of the newest batch. Before any batch it is the base index.
    pub(crate) fn pushed(&self) -> u64 {
        self.position.borrow().pushed
    }

    /// The applied index through which the stored batches are the whole state.
    pub(crate) fn covered(&self) -> u64 {
        self.position.borrow().covered
    }

    /// The newest batch and the covered index, from one applied state.
    pub(crate) fn position(&self) -> Position {
        *self.position.borrow()
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<Position> {
        self.position.subscribe()
    }

    fn push(&self, seq: u64, batch: Arc<WriteBatch>) {
        self.batches.lock().unwrap().push_back((seq, batch));
        self.position.send_modify(|position| {
            position.pushed = seq;
            position.covered = position.covered.max(seq);
        });
    }

    fn cover(&self, index: u64) {
        self.position.send_if_modified(|position| {
            let advanced = index > position.covered;
            position.covered = position.covered.max(index);
            advanced
        });
    }

    /// Drop every batch and start again after `base`.
    fn reset(&self, base: u64) {
        let mut batches = self.batches.lock().unwrap();
        batches.clear();
        self.released.store(base, Ordering::Release);
        drop(batches);
        self.position.send_replace(Position {
            pushed: base,
            covered: base,
        });
    }

    fn release(&self, through: u64) {
        let mut batches = self.batches.lock().unwrap();
        while batches.front().is_some_and(|(seq, _)| *seq <= through) {
            batches.pop_front();
        }
        self.released.fetch_max(through, Ordering::AcqRel);
    }

    /// The base index of the last release. A base holds every batch at or below it, so the
    /// materializer has nothing to write there, even above the physical sequence of the `Db`.
    pub(crate) fn released(&self) -> u64 {
        self.released.load(Ordering::Acquire)
    }

    pub(crate) fn len(&self) -> usize {
        self.batches.lock().unwrap().len()
    }
}

/// The apply-owned overlay of one member.
pub(crate) struct Overlay {
    /// Oldest first. The last segment is open.
    segments: Vec<Arc<Segment>>,
    /// The same segments for new views. It changes only when a segment opens or is released.
    shared: Arc<[Arc<Segment>]>,
    queue: Arc<Queue>,
}

impl Overlay {
    /// Start an empty overlay above a base that covers `base`.
    pub(crate) fn new(base: u64) -> Self {
        let segments = vec![Arc::new(Segment::default())];
        Self {
            shared: segments.clone().into(),
            segments,
            queue: Arc::new(Queue::new(base)),
        }
    }

    /// Add the batch of the group that ends at `seq`.
    pub(crate) fn push(&mut self, seq: u64, batch: Arc<WriteBatch>) {
        debug_assert!(seq > self.queue.pushed(), "overlay batches must be ordered");
        self.segments.last().unwrap().insert(seq, &batch);
        self.queue.push(seq, batch);
    }

    /// Record that the entry at `index` stored no batch. The state at `index` is the state after
    /// the newest batch.
    pub(crate) fn cover(&mut self, index: u64) {
        self.queue.cover(index);
    }

    /// Close the open segment and open a new one. An empty open segment stays open.
    pub(crate) fn close_segment(&mut self) {
        if self.segments.last().unwrap().last() == 0 {
            return;
        }
        self.segments.push(Arc::new(Segment::default()));
        self.shared = self.segments.clone().into();
    }

    /// Release the closed segments and the batches that a base at `through` covers.
    /// A segment that spans `through` stays until a later base covers it.
    pub(crate) fn release(&mut self, through: u64) {
        let open = self.segments.pop().unwrap();
        let before = self.segments.len();
        self.segments.retain(|segment| segment.last() > through);
        self.segments.push(open);
        if self.segments.len() != before + 1 {
            self.shared = self.segments.clone().into();
        }
        self.queue.release(through);
    }

    /// Drop all segments and batches. The new base at `base` holds their state. The queue stays
    /// the same object, so the materializer keeps its handle.
    pub(crate) fn reset(&mut self, base: u64) {
        self.segments = vec![Arc::new(Segment::default())];
        self.shared = self.segments.clone().into();
        self.queue.reset(base);
    }

    pub(crate) fn segments(&self) -> Arc<[Arc<Segment>]> {
        self.shared.clone()
    }

    pub(crate) fn queue(&self) -> Arc<Queue> {
        self.queue.clone()
    }

    /// Key and value bytes in the segments that the overlay still holds.
    pub(crate) fn bytes(&self) -> u64 {
        self.segments.iter().map(|segment| segment.bytes()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(rows: &[(&str, Option<&str>)]) -> Arc<WriteBatch> {
        let mut batch = WriteBatch::new();
        for (key, value) in rows {
            match value {
                Some(value) => batch.put(key, value),
                None => batch.delete(key),
            }
        }
        Arc::new(batch)
    }

    #[test]
    fn segments_return_the_newest_version_at_a_sequence() {
        let mut overlay = Overlay::new(0);
        overlay.push(2, batch(&[("a", Some("one")), ("b", Some("x"))]));
        overlay.push(5, batch(&[("a", Some("two")), ("b", None)]));
        let segment = overlay.segments()[0].clone();
        assert_eq!(segment.get(b"a", 1), None);
        assert_eq!(segment.get(b"a", 4), Some(Some(Bytes::from("one"))));
        assert_eq!(segment.get(b"a", 5), Some(Some(Bytes::from("two"))));
        assert_eq!(segment.get(b"b", 9), Some(None));
        let lower = Bound::Excluded(&b"a"[..]);
        let end = End::Prefix(b"");
        assert_eq!(
            segment.first_after(lower, &end, 4, None),
            Some((Bytes::from("b"), Some(Bytes::from("x"))))
        );
        // A key with only newer versions is skipped.
        overlay.push(7, batch(&[("c", Some("late"))]));
        assert_eq!(segment.first_after(lower, &end, 5, None).unwrap().1, None);
        let after_b = Bound::Excluded(&b"b"[..]);
        assert_eq!(segment.first_after(after_b, &end, 5, None), None);
        assert_eq!(overlay.queue().after(2, 10).len(), 2);
        assert_eq!(overlay.queue().pushed(), 7);
    }

    #[test]
    fn entries_without_a_batch_advance_only_the_covered_index() {
        let mut overlay = Overlay::new(3);
        let position = |overlay: &Overlay| {
            let position = overlay.queue().position();
            (position.pushed, position.covered)
        };
        assert_eq!(position(&overlay), (3, 3));
        overlay.cover(5);
        assert_eq!(position(&overlay), (3, 5));
        overlay.push(6, batch(&[("a", Some("one"))]));
        assert_eq!(position(&overlay), (6, 6));
        overlay.cover(8);
        overlay.cover(7);
        assert_eq!(position(&overlay), (6, 8));
        overlay.reset(10);
        assert_eq!(position(&overlay), (10, 10));
    }

    #[test]
    fn release_keeps_a_spanning_segment_and_views_keep_released_segments() {
        let mut overlay = Overlay::new(0);
        overlay.push(2, batch(&[("a", Some("one"))]));
        overlay.close_segment();
        overlay.push(4, batch(&[("a", Some("two"))]));
        overlay.push(6, batch(&[("b", Some("x"))]));
        overlay.close_segment();
        overlay.push(8, batch(&[("c", Some("y"))]));
        let pinned = overlay.segments();
        let first = Arc::downgrade(&pinned[0]);
        // The second segment spans 5, so it stays. The first one is covered.
        overlay.release(5);
        assert_eq!(overlay.segments().len(), 2);
        assert_eq!(overlay.segments()[0].last(), 6);
        assert_eq!(overlay.queue().after(0, 10)[0].0, 6);
        // The older view still holds the released segment.
        assert!(first.upgrade().is_some());
        drop(pinned);
        assert!(first.upgrade().is_none());
        // The open segment is never released, even when the base covers it.
        overlay.release(100);
        assert_eq!(overlay.segments().len(), 1);
        assert_eq!(overlay.segments()[0].last(), 8);
        assert_eq!(overlay.queue().len(), 0);
    }
}
