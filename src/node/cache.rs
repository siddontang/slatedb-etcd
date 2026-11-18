use super::*;
use std::cell::RefCell;

use raft::eraftpb::{HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, StorageError};

// Charges include encoded payloads and a fixed allowance for each entry or term run.
pub(super) const ENTRY_OVERHEAD: usize = 256;
pub(super) const MAX_ENTRY_BYTES: usize = 8 * 1024 * 1024;
const TERM_BYTES: usize = 64;
const CONTROL_RESERVE: usize = 64 * 1024;
pub(super) const FETCH_ENTRIES: u64 = 128;
pub(super) const FETCH_BYTES: u64 = 512 * 1024;

#[derive(Clone, Copy)]
pub struct LogMemory {
    pub cache_bytes: usize,
    pub pending_bytes: usize,
    pub read_bytes: usize,
    pub term_bytes: usize,
}

impl Default for LogMemory {
    fn default() -> Self {
        Self {
            cache_bytes: 8 * 1024 * 1024,
            pending_bytes: 32 * 1024 * 1024,
            read_bytes: 16 * 1024 * 1024,
            term_bytes: 1024 * 1024,
        }
    }
}

impl LogMemory {
    pub fn validate(self) -> Result<()> {
        ensure!(
            self.cache_bytes >= ENTRY_OVERHEAD,
            "log cache quota is too small"
        );
        ensure!(
            self.pending_bytes > MAX_ENTRY_BYTES + CONTROL_RESERVE,
            "pending log quota must exceed the entry limit and control reserve"
        );
        ensure!(
            self.read_bytes >= MAX_ENTRY_BYTES + ENTRY_OVERHEAD,
            "log read quota must hold one maximum entry"
        );
        ensure!(self.term_bytes >= TERM_BYTES, "log term quota is too small");
        Ok(())
    }
}

pub(super) fn charge(entry: &Entry) -> usize {
    entry.compute_size() as usize + ENTRY_OVERHEAD
}

#[derive(Debug)]
pub(super) struct PendingQuota;
impl std::fmt::Display for PendingQuota {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("pending log memory quota exhausted")
    }
}
impl std::error::Error for PendingQuota {}

pub(super) struct Fetch {
    pub epoch: u64,
    pub low: u64,
    pub high: u64,
    pub max_bytes: u64,
    pub context: GetEntriesContext,
}

#[derive(Default)]
struct History {
    entries: BTreeMap<u64, Entry>,
    bytes: usize,
    window: BTreeMap<u64, Entry>,
    window_bytes: usize,
    request: Option<Fetch>,
    fetching: bool,
    hits: u64,
    misses: u64,
    evictions: u64,
    reads: u64,
    stale: u64,
}

impl History {
    fn insert(&mut self, entry: Entry, limit: usize) {
        let bytes = charge(&entry);
        if bytes > limit {
            return;
        }
        if let Some(old) = self.entries.remove(&entry.index) {
            self.bytes -= charge(&old);
        }
        while self.bytes + bytes > limit {
            let (_, old) = self.entries.pop_first().unwrap();
            self.bytes -= charge(&old);
            self.evictions += 1;
        }
        self.bytes += bytes;
        self.entries.insert(entry.index, entry);
    }
    fn clear(&mut self) {
        self.evictions += self.entries.len() as u64;
        self.entries.clear();
        self.bytes = 0;
        self.window.clear();
        self.window_bytes = 0;
    }
}

// Raft needs synchronous access to every unapplied entry and every retained term.
pub(super) struct Cache {
    pending: BTreeMap<u64, Entry>,
    pending_bytes: usize,
    terms: BTreeMap<u64, u64>,
    last: u64,
    applied: u64,
    epoch: u64,
    history: RefCell<History>,
    limits: LogMemory,
    snapshot: Snapshot,
    pub initial_conf: Option<raft::eraftpb::ConfState>,
    hard_state: HardState,
    pub durable: log::Durable,
}

impl Cache {
    pub fn load(storage: &RaftStorage, applied: u64, limits: LogMemory) -> Result<Self> {
        limits.validate()?;
        let durable = log::Durable::read(storage)?;
        let mut cache = Self {
            pending: BTreeMap::new(),
            pending_bytes: 0,
            terms: BTreeMap::new(),
            last: durable.snapshot.get_metadata().index,
            applied,
            epoch: 0,
            history: RefCell::new(History::default()),
            limits,
            snapshot: durable.snapshot.clone(),
            initial_conf: None,
            hard_state: durable.hard_state.clone(),
            durable,
        };
        let mut low = storage.first_index()?;
        let end = storage.last_index()? + 1;
        while low < end {
            let entries = storage.entries(
                low,
                (low + FETCH_ENTRIES).min(end),
                Some(FETCH_BYTES),
                GetEntriesContext::empty(false),
            )?;
            ensure!(!entries.is_empty(), "empty log recovery batch");
            low = entries.last().unwrap().index + 1;
            cache.stage(&entries, None, None)?;
        }
        Ok(cache)
    }

    pub fn stage(
        &mut self,
        entries: &[Entry],
        hs: Option<&HardState>,
        snapshot: Option<&Snapshot>,
    ) -> Result<()> {
        if let Some(snapshot) = snapshot {
            ensure!(
                snapshot.get_metadata().index > self.snapshot.get_metadata().index,
                "stale cached snapshot"
            );
            self.snapshot = snapshot.clone();
            self.last = snapshot.get_metadata().index;
            self.pending.clear();
            self.pending_bytes = 0;
            self.terms.clear();
            self.invalidate();
        }
        if let Some(first) = entries.first() {
            ensure!(
                first.index >= self.first_index()? && first.index <= self.last + 1,
                "cached log gap"
            );
            if first.index <= self.last {
                self.invalidate();
                self.pending.split_off(&first.index);
                self.pending_bytes = self.pending.values().map(charge).sum();
                self.terms.split_off(&first.index);
                self.last = first.index - 1;
            }
            for entry in entries {
                ensure!(entry.index == self.last + 1, "cached log gap");
                ensure!(
                    entry.compute_size() as usize <= MAX_ENTRY_BYTES,
                    "Raft entry exceeds size limit"
                );
                if self.terms.last_key_value().map(|(_, term)| *term) != Some(entry.term) {
                    ensure!(
                        (self.terms.len() + 1) * TERM_BYTES <= self.limits.term_bytes,
                        "Raft term memory quota exhausted"
                    );
                    self.terms.insert(entry.index, entry.term);
                }
                if entry.index > self.applied {
                    if self.pending_bytes + charge(entry) > self.limits.pending_bytes {
                        anyhow::bail!(PendingQuota);
                    }
                    self.pending_bytes += charge(entry);
                    self.pending.insert(entry.index, entry.clone());
                } else {
                    self.history
                        .get_mut()
                        .insert(entry.clone(), self.limits.cache_bytes);
                }
                self.last = entry.index;
            }
        }
        if let Some(hs) = hs {
            self.hard_state = hs.clone();
        }
        Ok(())
    }

    fn invalidate(&mut self) {
        self.epoch += 1;
        self.history.get_mut().clear();
    }

    pub fn applied_to(&mut self, index: u64) {
        self.applied = self.applied.max(index);
        while self
            .pending
            .first_key_value()
            .is_some_and(|(i, _)| *i <= self.applied)
        {
            let (_, entry) = self.pending.pop_first().unwrap();
            self.pending_bytes -= charge(&entry);
            self.history
                .get_mut()
                .insert(entry, self.limits.cache_bytes);
        }
    }

    pub fn complete(&mut self, durable: log::Durable) -> Result<()> {
        ensure!(
            durable.hard_state.commit >= self.commit_index(),
            "durable commit regressed"
        );
        if durable.snapshot.get_metadata().index > self.snapshot.get_metadata().index {
            let end = durable.snapshot.get_metadata().index + 1;
            let term = self.terms.range(..=end).next_back().map(|(_, t)| *t);
            self.pending = self.pending.split_off(&end);
            self.pending_bytes = self.pending.values().map(charge).sum();
            self.terms = self.terms.split_off(&end);
            if end <= self.last {
                self.terms
                    .entry(end)
                    .or_insert(term.context("missing log term")?);
            }
            self.snapshot = durable.snapshot.clone();
            self.invalidate();
        }
        self.durable = durable;
        Ok(())
    }

    pub fn admits_message(&self, unstable: &[Entry], message: &Message) -> bool {
        if message.get_msg_type() == MessageType::MsgPropose {
            // Raft assigns new indexes to forwarded proposals. They cannot replace a suffix.
            message
                .entries
                .iter()
                .all(|e| e.compute_size() as usize <= MAX_ENTRY_BYTES)
                && self.admits(unstable, &[], message.entries.iter().map(charge).sum())
        } else {
            self.admits(unstable, &message.entries, 0)
        }
    }

    pub fn admits(&self, unstable: &[Entry], incoming: &[Entry], proposal_bytes: usize) -> bool {
        if incoming
            .iter()
            .any(|entry| entry.compute_size() as usize > MAX_ENTRY_BYTES)
        {
            return false;
        }
        let incoming_bytes: usize = incoming.iter().map(charge).sum();
        // Count the full suffix first. Overwrites can only reduce this upper bound.
        let upper = self
            .pending_bytes
            .saturating_add(unstable.iter().map(charge).sum())
            .saturating_add(incoming_bytes)
            .saturating_add(proposal_bytes);
        if upper <= self.limits.pending_bytes - CONTROL_RESERVE {
            return true;
        }
        let overwrite = incoming.first().map_or(u64::MAX, |e| e.index);
        let unstable_start = unstable.first().map_or(u64::MAX, |e| e.index);
        let retained: usize = self
            .pending
            .range(..overwrite.min(unstable_start))
            .map(|(_, e)| charge(e))
            .sum();
        let unstable_bytes: usize = unstable
            .iter()
            .filter(|e| e.index < overwrite)
            .map(charge)
            .sum();
        let data_charge = |entry: &Entry| {
            if entry.data.is_empty()
                && entry.context.is_empty()
                && entry.get_entry_type() == EntryType::EntryNormal
            {
                0
            } else {
                charge(entry)
            }
        };
        let current_data = self
            .pending
            .range(..unstable_start)
            .map(|(_, e)| data_charge(e))
            .sum::<usize>()
            + unstable.iter().map(data_charge).sum::<usize>();
        let projected_data = self
            .pending
            .range(..overwrite.min(unstable_start))
            .map(|(_, e)| data_charge(e))
            .sum::<usize>()
            + unstable
                .iter()
                .filter(|e| e.index < overwrite)
                .map(data_charge)
                .sum::<usize>()
            + incoming.iter().map(data_charge).sum::<usize>()
            + proposal_bytes;
        let projected = retained
            .saturating_add(unstable_bytes)
            .saturating_add(incoming_bytes)
            .saturating_add(proposal_bytes);
        projected <= self.limits.pending_bytes
            && projected_data <= current_data.max(self.limits.pending_bytes - CONTROL_RESERVE)
    }

    pub fn take_fetch(&mut self) -> Option<Fetch> {
        self.history.get_mut().request.take()
    }

    pub fn fetched(&mut self, fetch: &Fetch, entries: raft::Result<Vec<Entry>>) -> Result<()> {
        let history = self.history.get_mut();
        history.fetching = false;
        history.reads += 1;
        if fetch.epoch != self.epoch {
            history.stale += 1;
            return Ok(());
        }
        let entries = entries?;
        ensure!(!entries.is_empty(), "empty asynchronous log read");
        let bytes: usize = entries.iter().map(charge).sum();
        ensure!(
            bytes <= self.limits.read_bytes,
            "log read memory quota exceeded"
        );
        history.window.clear();
        history.window_bytes = bytes;
        for entry in entries {
            ensure!(
                entry.index >= fetch.low && entry.index < fetch.high,
                "asynchronous log read has a wrong index"
            );
            history.insert(entry.clone(), self.limits.cache_bytes);
            history.window.insert(entry.index, entry);
        }
        Ok(())
    }

    pub fn stats(&self) -> Value {
        let h = self.history.borrow();
        json!({"cache_bytes": h.bytes, "cache_limit": self.limits.cache_bytes,
            "pending_bytes": self.pending_bytes, "pending_limit": self.limits.pending_bytes,
            "term_bytes": self.terms.len() * TERM_BYTES, "term_limit": self.limits.term_bytes,
            "read_bytes": h.window_bytes, "read_reserved": if h.fetching {self.limits.read_bytes} else {0},
            "read_limit": self.limits.read_bytes, "hits": h.hits, "misses": h.misses,
            "evictions": h.evictions, "disk_reads": h.reads, "stale_reads": h.stale})
    }
    pub fn snapshot_pending(&self) -> bool {
        self.snapshot.get_metadata().index > self.snapshot_index()
    }
    pub fn commit_index(&self) -> u64 {
        self.durable.hard_state.commit
    }
    pub fn snapshot_index(&self) -> u64 {
        self.durable.snapshot.get_metadata().index
    }
    pub fn recovery_snapshot(&self) -> &Snapshot {
        &self.durable.snapshot
    }
}

impl Storage for Cache {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state.clone(),
            conf_state: self
                .initial_conf
                .clone()
                .unwrap_or_else(|| self.snapshot.get_metadata().get_conf_state().clone()),
        })
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low < self.first_index()? {
            return Err(StorageError::Compacted.into());
        }
        if high > self.last + 1 || high < low {
            return Err(StorageError::Unavailable.into());
        }
        let limit = max_size.into().unwrap_or(u64::MAX);
        let mut history = self.history.borrow_mut();
        let mut result = vec![];
        let mut bytes = 0;
        for index in low..high {
            let entry = self
                .pending
                .get(&index)
                .or_else(|| history.entries.get(&index))
                .or_else(|| history.window.get(&index));
            let Some(entry) = entry else {
                if !result.is_empty() {
                    break;
                }
                history.misses += 1;
                if context.can_async() {
                    if !history.fetching {
                        history.window.clear();
                        history.window_bytes = 0;
                        history.fetching = true;
                        history.request = Some(Fetch {
                            epoch: self.epoch,
                            low,
                            high: high.min(low + FETCH_ENTRIES),
                            max_bytes: limit.min(FETCH_BYTES),
                            context,
                        });
                    }
                    return Err(StorageError::LogTemporarilyUnavailable.into());
                }
                return Err(StorageError::Unavailable.into());
            };
            let size = u64::from(entry.compute_size());
            if !result.is_empty() && bytes + size > limit {
                break;
            }
            result.push(entry.clone());
            bytes += size;
        }
        history.hits += 1;
        Ok(result)
    }
    fn term(&self, index: u64) -> raft::Result<u64> {
        if index == self.snapshot.get_metadata().index {
            return Ok(self.snapshot.get_metadata().term);
        }
        if index < self.first_index()? {
            return Err(StorageError::Compacted.into());
        }
        if index > self.last {
            return Err(StorageError::Unavailable.into());
        }
        self.terms
            .range(..=index)
            .next_back()
            .map(|(_, term)| *term)
            .ok_or_else(|| StorageError::Unavailable.into())
    }
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.snapshot.get_metadata().index + 1)
    }
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last)
    }
    fn snapshot(&self, request_index: u64, _: u64) -> raft::Result<Snapshot> {
        if self.snapshot_index() == 0 || self.snapshot_index() < request_index {
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }
        Ok(self.durable.snapshot.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            data: vec![index as u8; 1024].into(),
            ..Default::default()
        }
    }

    fn stored() -> (tempfile::TempDir, RaftStorage, Cache) {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = RaftStorage::open(dir.path(), 1, "cache-test", vec![1, 2, 3]).unwrap();
        let entries: Vec<_> = (1..=8).map(|index| entry(index, 1)).collect();
        storage
            .persist(
                &entries,
                Some(&HardState {
                    term: 1,
                    commit: 8,
                    ..Default::default()
                }),
                None,
            )
            .unwrap();
        let cache = Cache::load(
            &storage,
            8,
            LogMemory {
                cache_bytes: 2048,
                ..LogMemory::default()
            },
        )
        .unwrap();
        (dir, storage, cache)
    }

    fn miss(cache: &mut Cache, low: u64) -> Fetch {
        assert!(matches!(
            cache.entries(low, low + 1, None, GetEntriesContext::empty(true)),
            Err(raft::Error::Store(StorageError::LogTemporarilyUnavailable))
        ));
        cache.take_fetch().unwrap()
    }

    #[test]
    fn evicted_payloads_use_one_bounded_read_and_keep_terms() {
        let (_dir, storage, mut cache) = stored();
        assert!(cache.stats()["evictions"].as_u64().unwrap() > 0);
        for index in 1..=8 {
            assert_eq!(cache.term(index).unwrap(), 1);
        }
        let fetch = miss(&mut cache, 1);
        assert!(cache
            .entries(2, 3, None, GetEntriesContext::empty(true))
            .is_err());
        assert!(
            cache.take_fetch().is_none(),
            "duplicate read exceeded one request"
        );
        let entries = storage.entries(fetch.low, fetch.high, None, GetEntriesContext::empty(false));
        cache.fetched(&fetch, entries).unwrap();
        assert_eq!(
            cache
                .entries(1, 2, None, GetEntriesContext::empty(false))
                .unwrap()[0],
            entry(1, 1)
        );
        assert!(cache.stats()["cache_bytes"].as_u64().unwrap() <= 2048);
        assert_eq!(cache.stats()["read_reserved"], 0);
    }

    #[test]
    fn an_entry_larger_than_the_cache_uses_the_read_pool() {
        let (_dir, storage, _) = stored();
        let mut cache = Cache::load(
            &storage,
            8,
            LogMemory {
                cache_bytes: 256,
                ..LogMemory::default()
            },
        )
        .unwrap();
        let fetch = miss(&mut cache, 1);
        cache
            .fetched(
                &fetch,
                storage.entries(1, 2, Some(0), GetEntriesContext::empty(false)),
            )
            .unwrap();
        assert_eq!(cache.stats()["cache_bytes"], 0);
        assert!(cache.stats()["read_bytes"].as_u64().unwrap() > 256);
        assert_eq!(
            cache
                .entries(1, 2, Some(0), GetEntriesContext::empty(false))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn overwrite_compaction_and_snapshot_discard_stale_reads() {
        let (_dir, mut storage, mut cache) = stored();
        let fetch = miss(&mut cache, 1);
        let old = storage.entries(1, 2, None, GetEntriesContext::empty(false));
        cache.stage(&[entry(8, 2)], None, None).unwrap();
        cache.fetched(&fetch, old).unwrap();
        assert_eq!(cache.stats()["stale_reads"], 1);
        assert_eq!(cache.term(7).unwrap(), 1);
        assert_eq!(cache.term(8).unwrap(), 2);
        let fetch = miss(&mut cache, 1);
        storage.checkpoint(4, vec![]).unwrap();
        cache
            .complete(log::Durable::read(&storage).unwrap())
            .unwrap();
        cache
            .fetched(&fetch, Err(StorageError::Compacted.into()))
            .unwrap();
        assert_eq!(cache.first_index().unwrap(), 5);
        assert_eq!(cache.term(4).unwrap(), 1);
        assert_eq!(cache.term(5).unwrap(), 1);
        let fetch = miss(&mut cache, 5);
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 10;
        snapshot.mut_metadata().term = 3;
        cache.stage(&[], None, Some(&snapshot)).unwrap();
        cache.fetched(&fetch, Ok(vec![entry(5, 1)])).unwrap();
        assert_eq!(cache.stats()["stale_reads"], 3);
        assert_eq!(cache.last_index().unwrap(), 10);
        assert!(cache.snapshot_pending());
    }

    #[test]
    fn pending_and_term_quotas_fail_without_exceeding_their_limits() {
        let (_dir, storage, _) = stored();
        let mut cache = Cache::load(&storage, 0, LogMemory::default()).unwrap();
        cache.limits.pending_bytes = cache.pending_bytes;
        assert!(cache
            .stage(&[entry(9, 1)], None, None)
            .unwrap_err()
            .is::<PendingQuota>());
        assert_eq!(cache.pending_bytes, cache.limits.pending_bytes);
        cache.limits.pending_bytes += 4096;
        cache.limits.term_bytes = TERM_BYTES;
        assert!(cache
            .stage(&[entry(9, 2)], None, None)
            .unwrap_err()
            .to_string()
            .contains("term memory"));
        assert_eq!(cache.terms.len(), 1);
        cache.applied_to(8);
        assert_eq!(cache.pending_bytes, 0);
    }

    #[test]
    fn admission_releases_conflicting_suffix_and_checks_unstable_entries() {
        let (_dir, storage, _) = stored();
        let mut cache = Cache::load(&storage, 0, LogMemory::default()).unwrap();
        cache.limits.pending_bytes = cache.pending_bytes + CONTROL_RESERVE;
        assert!(!cache.admits(&[], &[], 1));
        assert!(cache.admits(&[], &[entry(5, 2)], 0));
        let proposal = Message {
            msg_type: MessageType::MsgPropose,
            entries: vec![entry(5, 2)].into(),
            ..Default::default()
        };
        assert!(!cache.admits_message(&[], &proposal));
        assert!(cache.admits(&[entry(9, 1)], &[], 0));
        assert!(!cache.admits(&[entry(9, 1)], &[], 1));
        assert!(cache.admits(&[entry(5, 2)], &[entry(5, 3)], 0));
    }

    #[test]
    fn a_current_read_error_fails_instead_of_returning_old_data() {
        let (_dir, _storage, mut cache) = stored();
        let fetch = miss(&mut cache, 1);
        assert!(cache
            .fetched(&fetch, Err(StorageError::Unavailable.into()))
            .is_err());
        assert_eq!(cache.stats()["read_reserved"], 0);
    }

    #[tokio::test]
    async fn full_pending_pool_preserves_raft_control_progress() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage =
            RaftStorage::open(&dir.path().join("raft"), 1, "quota", vec![1, 2, 3]).unwrap();
        let entries: Vec<_> = (1..=8).map(|index| entry(index, 1)).collect();
        storage
            .persist(
                &entries,
                Some(&HardState {
                    term: 1,
                    ..Default::default()
                }),
                None,
            )
            .unwrap();
        drop(storage);
        let (events, _) = mpsc::channel(8);
        let (peer, mut replies) = mpsc::channel(8);
        let mut node = Node::open_with_transport(
            NodeOptions {
                client_peers: BTreeMap::new(),
                transport: crate::tls::Transport::default(),
                require_peer_tls: false,
                object_store: crate::object::Config::Local,
                quota_bytes: 0,
                strict_reconfiguration: false,
                join: false,
                id: 1,
                cluster_id: "quota".into(),
                dir: dir.path().to_owned(),
                peers: (1..=3).map(|id| (id, format!("127.0.0.1:{id}"))).collect(),
                snapshot_every: 0,
                log_memory: LogMemory::default(),
            },
            events,
            Some(BTreeMap::from([(2, peer)])),
        )
        .await
        .unwrap();
        let cache = node.raw.mut_store();
        cache.limits.pending_bytes = cache.pending_bytes + CONTROL_RESERVE - 1;
        let used = cache.pending_bytes;
        for (index, term, wire_term, next, accepted, rejected) in [
            (20, 2, 2, entry(21, 2), true, true),
            (7, 1, 2, entry(8, 1), true, false),
            (8, 1, 2, entry(9, 2), false, false),
            (8, 1, 0, entry(9, 2), false, false),
        ] {
            let message = Message {
                msg_type: MessageType::MsgAppend,
                from: 2,
                to: 1,
                term: wire_term,
                index,
                log_term: term,
                entries: vec![next].into(),
                ..Default::default()
            };
            let (tx, rx) = oneshot::channel();
            node.handle(Event::Step(message, tx), &mut vec![])
                .await
                .unwrap();
            assert_eq!(rx.await.unwrap().is_ok(), accepted);
            node.drive_ready().await.unwrap();
            node.drain_log().await.unwrap();
            if accepted {
                let reply = replies.try_recv().unwrap();
                assert_eq!(reply.get_msg_type(), MessageType::MsgAppendResponse);
                assert_eq!(reply.reject, rejected);
            } else {
                assert!(replies.try_recv().is_err());
            }
            assert_eq!(node.raw.store().last_index().unwrap(), 8);
            assert_eq!(node.raw.store().pending_bytes, used);
        }
        let control = Entry {
            index: 9,
            term: 2,
            ..Default::default()
        };
        let extra = charge(&control);
        let message = Message {
            msg_type: MessageType::MsgAppend,
            from: 2,
            to: 1,
            term: 2,
            index: 8,
            log_term: 1,
            entries: vec![control].into(),
            ..Default::default()
        };
        let (tx, rx) = oneshot::channel();
        node.handle(Event::Step(message, tx), &mut vec![])
            .await
            .unwrap();
        assert!(rx.await.unwrap().is_ok());
        node.drive_ready().await.unwrap();
        node.drain_log().await.unwrap();
        let reply = replies.try_recv().unwrap();
        assert!(!reply.reject);
        assert_eq!(reply.index, 9);
        assert_eq!(node.raw.store().pending_bytes, used + extra);
    }
}
