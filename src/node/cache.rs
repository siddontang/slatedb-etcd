use super::*;
use std::cell::{Cell, RefCell};

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
    entry.encoded_len() + ENTRY_OVERHEAD
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
    // Raft asked for a transferable image that the durable snapshot does not contain.
    image_wanted: Cell<bool>,
    // The snapshot index that each requesting follower needs, pinned at its first request. See
    // `Storage::snapshot`.
    wanted: RefCell<BTreeMap<u64, u64>>,
    // The leader term of the requests in `wanted`.
    wanted_term: Cell<u64>,
    // The process of each peer, for the coverage capability. See `PeerProcess`.
    peers: RefCell<BTreeMap<u64, PeerProcess>>,
    // A member without the coverage capability needs a record at its checkpoint row.
    exact_wanted: Cell<bool>,
    // Shared mode: the checkpoint and the index of this member's base.
    base: Cell<Option<(uuid::Uuid, u64)>>,
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
            hard_state: durable.hard_state,
            image_wanted: Cell::new(false),
            wanted: RefCell::default(),
            wanted_term: Cell::new(0),
            peers: RefCell::default(),
            exact_wanted: Cell::new(false),
            base: Cell::new(None),
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
                    entry.encoded_len() <= MAX_ENTRY_BYTES,
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
            self.hard_state = *hs;
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
        // An image at the same index can replace a record that is no longer transferable.
        let new_image = !durable.snapshot.data.is_empty()
            && (durable.snapshot.get_metadata().index > self.snapshot_index()
                || durable.snapshot.data != self.durable.snapshot.data);
        if new_image {
            self.image_wanted.set(false);
            self.exact_wanted.set(false);
        }
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
                .all(|e| e.encoded_len() <= MAX_ENTRY_BYTES)
                && self.admits(unstable, &[], message.entries.iter().map(charge).sum())
        } else {
            self.admits(unstable, &message.entries, 0)
        }
    }

    /// Return true when unapplied entries, with unstable entries not staged yet and
    /// `proposal_bytes`, use half or more of the pending quota. Writers wait then, so the
    /// quota does not fail a write that is held until a quorum or apply returns.
    pub fn pending_half_full(&self, unstable: &[Entry], proposal_bytes: usize) -> bool {
        let unstaged: usize = unstable
            .iter()
            .filter(|entry| entry.index > self.last)
            .map(charge)
            .sum();
        self.pending_bytes + unstaged + proposal_bytes >= self.limits.pending_bytes / 2
    }

    /// Return true when the unapplied entries leave the control reserve free, so the
    /// term-start entries of a new leader fit.
    pub fn has_control_room(&self) -> bool {
        self.pending_bytes + CONTROL_RESERVE <= self.limits.pending_bytes
    }

    pub fn admits(&self, unstable: &[Entry], incoming: &[Entry], proposal_bytes: usize) -> bool {
        if incoming
            .iter()
            .any(|entry| entry.encoded_len() > MAX_ENTRY_BYTES)
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
                && entry.entry_type == EntryType::EntryNormal as i32
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
    /// The index of the newest staged snapshot record, durable or not.
    pub fn staged_snapshot_index(&self) -> u64 {
        self.snapshot.get_metadata().index
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
    /// Return true after Raft asked for an image that the durable snapshot cannot supply.
    pub fn image_wanted(&self) -> bool {
        self.image_wanted.get()
    }
    /// The highest snapshot index that a requesting follower needs above the durable record.
    /// Zero when the record serves every request. A peer keeps its pinned target until it gets
    /// the snapshot, but a record that reaches the target ends the demand.
    pub fn wanted_index(&self) -> u64 {
        let record = self.snapshot_index();
        self.wanted
            .borrow()
            .values()
            .copied()
            .filter(|target| *target > record)
            .max()
            .unwrap_or(0)
    }
    /// Drop the requests at a term change. Raft sends a current request again.
    pub fn clear_wanted(&self) {
        self.wanted.borrow_mut().clear();
    }
    /// Note the term of this leader. A new term drops the requests of the old one.
    pub fn lead_term(&self, term: u64) {
        if self.wanted_term.replace(term) != term {
            self.clear_wanted();
        }
    }
    /// Note the process that sent the last transport frame of `peer`. A new process loses the
    /// coverage capability of the old one.
    pub fn note_process(&self, peer: u64, process: Option<u64>) {
        let mut peers = self.peers.borrow_mut();
        let entry = peers.entry(peer).or_default();
        if entry.process != process {
            *entry = PeerProcess {
                process,
                advertised: None,
            };
        }
    }
    /// Note that process `process` of `peer` installs a base above its checkpoint row. A report
    /// without a process, from an earlier binary, removes the capability.
    pub fn note_coverage(&self, peer: u64, process: Option<u64>) {
        self.peers.borrow_mut().entry(peer).or_default().advertised = process;
    }
    /// `peer` refused a snapshot that named process `target`. That process loses the capability
    /// until it reports again. A refusal that comes late, for an earlier process, changes
    /// nothing. The process of the stream stays, because a stream names it only in its first
    /// frame. The next report can thus restore the capability in the middle of a stream.
    pub fn note_refused(&self, peer: u64, target: u64) {
        let mut peers = self.peers.borrow_mut();
        if let Some(entry) = peers.get_mut(&peer) {
            if entry.advertised == Some(target) {
                entry.advertised = None;
            }
        }
    }
    /// The process of the last transport frame of `peer`.
    pub fn process(&self, peer: u64) -> Option<u64> {
        self.peers.borrow().get(&peer)?.process
    }
    /// Return true when the current process of `peer` advertised the coverage capability.
    pub fn coverage_capable(&self, peer: u64) -> bool {
        self.peers
            .borrow()
            .get(&peer)
            .is_some_and(|entry| entry.process.is_some() && entry.advertised == entry.process)
    }
    /// The process of `peer` that advertised the coverage capability, while it runs.
    pub fn capable_process(&self, peer: u64) -> Option<u64> {
        self.coverage_capable(peer)
            .then(|| self.peers.borrow()[&peer].process)
            .flatten()
    }
    /// Return true after a member without the capability needed a record at its row.
    pub fn exact_wanted(&self) -> bool {
        self.exact_wanted.get()
    }
    /// Note the checkpoint and the index of this member's base.
    pub fn note_base(&self, base: Option<(uuid::Uuid, u64)>) {
        self.base.set(base);
    }
    /// Return true when the durable snapshot holds an image that this member can send.
    ///
    /// A view record is sendable only while it names the checkpoint of the base, at any index.
    /// Only a live base pins its checkpoint, so retention can delete the checkpoint of the record
    /// after a base switch, and a restart or a rebase can use another checkpoint for that
    /// reason. A member with no base checkpoint yet, such as a new member before its install,
    /// pins none. A received record can also be above the base. The owner records the base once
    /// the base index reaches the record. See `Node::certify_base`.
    pub fn has_image(&self) -> bool {
        let data = &self.durable.snapshot.data;
        let stale = match crate::state::transfer::ViewDescriptor::decode(data) {
            Ok(Some(view)) => self
                .base
                .get()
                .is_none_or(|(checkpoint, _)| view.checkpoint().ok() != Some(checkpoint)),
            _ => false,
        };
        crate::state::transfer::sendable(data) && !stale
    }
}

/// The process of a peer. A member sends a random process ID with its transport frames and its
/// `ViewAck` reports. An earlier binary sends none. The coverage capability holds only for the
/// process that advertised it, so a restart on an earlier binary with the same member ID loses
/// it at its first frame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerProcess {
    /// The process of the last transport frame.
    process: Option<u64>,
    /// The process whose `ViewAck` advertised the capability.
    advertised: Option<u64>,
}

impl Storage for Cache {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state,
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
            let size = entry.encoded_len() as u64;
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
    /// Return the durable record for a follower, or ask the owner for one.
    ///
    /// A follower's request can name an index that this leader never has: raft-rs keeps a
    /// pending request across a term change, and the follower sends it again to each leader. The
    /// leader therefore serves `min(request, last)`, with its last log index, and pins that
    /// target at the first request of the peer. Raft repeats the original index at each retry,
    /// and the pinned target does not follow the log, so the entries that serve it do not move
    /// it. The target ends when the peer gets a snapshot, or at a term change.
    ///
    /// Raft accepts any snapshot at or above the follower's commit index, and that clears the
    /// follower's pending request. The leader's log holds every committed entry, so the target
    /// is at or above that index.
    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        // Raft calls this as soon as it leads, before the node sees the new role. The staged
        // HardState already has the term, because a candidate persists its term before it asks
        // for votes. A target of an earlier term goes.
        self.lead_term(self.hard_state.term);
        let pinned = self.wanted.borrow().get(&to).copied();
        let target = pinned.unwrap_or_else(|| request_index.min(self.last));
        // A record without an image only truncates the local log. Raft retries after the owner
        // builds and publishes a complete image.
        if self.snapshot_index() == 0 || self.snapshot_index() < target || !self.has_image() {
            self.image_wanted.set(true);
            if target > self.snapshot_index() {
                self.wanted.borrow_mut().insert(to, target);
                // An earlier binary gets only a record at its row.
                if !self.coverage_capable(to) {
                    self.exact_wanted.set(true);
                }
            }
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }
        // An earlier binary installs a view record only at its checkpoint row.
        if let Ok(Some(view)) =
            crate::state::transfer::ViewDescriptor::decode(&self.durable.snapshot.data)
        {
            if view.row.is_some_and(|row| row < self.snapshot_index()) && !self.coverage_capable(to)
            {
                self.image_wanted.set(true);
                self.exact_wanted.set(true);
                return Err(StorageError::SnapshotTemporarilyUnavailable.into());
            }
        }
        self.wanted.borrow_mut().remove(&to);
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
            data: vec![index as u8; 1024],
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

    /// A request above the last log index pins `min(request, last)` at its first call. Raft
    /// repeats the original index, and the pin holds while the log grows. A record at the target
    /// ends the demand but not the pin, so the next retry gets that record. A later term drops
    /// the requests, also at a call before the node sees its new role.
    #[test]
    fn a_snapshot_request_pins_its_target_until_the_peer_gets_it() {
        let (_dir, mut storage, mut cache) = stored();
        assert_eq!(cache.last_index().unwrap(), 8);
        assert!(cache.snapshot(34, 2).is_err());
        assert_eq!(cache.wanted_index(), 8);
        // The log grows, and Raft asks again with the same index.
        cache.stage(&[entry(9, 1)], None, None).unwrap();
        assert!(cache.snapshot(34, 2).is_err());
        assert_eq!(cache.wanted_index(), 8);
        // The record at 8 of the base ends the demand. The retry then gets it.
        let id = uuid::Uuid::new_v4();
        let data = crate::state::transfer::ViewDescriptor::new(id)
            .encode()
            .unwrap();
        storage.checkpoint(8, data).unwrap();
        cache.note_base(Some((id, 8)));
        cache
            .complete(log::Durable::read(&storage).unwrap())
            .unwrap();
        assert_eq!(cache.wanted_index(), 0);
        assert_eq!(cache.snapshot(34, 2).unwrap().get_metadata().index, 8);
        // The next request starts again at the last index.
        assert!(cache.snapshot(34, 2).is_err());
        assert_eq!(cache.wanted_index(), 9);
        // The member leads again in a later term. Raft asks before the node sees the role. The
        // staged HardState has the new term, so the old target goes, and the request pins the
        // last index of now.
        let hs = HardState {
            term: 2,
            commit: 8,
            ..Default::default()
        };
        cache.stage(&[entry(10, 2)], Some(&hs), None).unwrap();
        assert!(cache.snapshot(34, 2).is_err());
        assert_eq!(cache.wanted_index(), 10);
        // The node also drops the targets when it sees a new term.
        cache.lead_term(3);
        assert_eq!(cache.wanted_index(), 0);
    }

    /// A record above its checkpoint row goes only to a peer whose current process advertised
    /// the capability. A new process of the same member, of an earlier binary, loses it.
    #[test]
    fn a_record_above_its_row_needs_a_capable_process() {
        let (_dir, mut storage, mut cache) = stored();
        cache.applied_to(8);
        let id = uuid::Uuid::new_v4();
        let data = crate::state::transfer::ViewDescriptor::above_row(id, 3)
            .encode()
            .unwrap();
        storage.checkpoint(6, data).unwrap();
        cache
            .complete(log::Durable::read(&storage).unwrap())
            .unwrap();
        // Before a matching base opens, the record pins nothing, so it is not sendable.
        assert!(!cache.has_image());
        cache.note_base(Some((uuid::Uuid::new_v4(), 6)));
        assert!(!cache.has_image());
        cache.note_base(Some((id, 6)));
        assert!(cache.has_image());
        assert!(cache.snapshot(6, 2).is_err());
        assert!(cache.exact_wanted());
        cache.note_process(2, Some(7));
        cache.note_coverage(2, Some(7));
        assert!(cache.coverage_capable(2));
        assert_eq!(cache.snapshot(6, 2).unwrap().get_metadata().index, 6);
        // The same member starts again on an earlier binary.
        cache.note_process(2, None);
        assert!(!cache.coverage_capable(2));
        assert!(cache.snapshot(6, 2).is_err());
        // A new process of this binary reports the capability. Its stream named it first.
        cache.note_process(2, Some(9));
        assert!(!cache.coverage_capable(2));
        cache.note_coverage(2, Some(9));
        assert!(cache.coverage_capable(2));
        assert_eq!(cache.capable_process(2), Some(9));
        // A late 409 for an earlier process leaves the new one capable.
        cache.note_refused(2, 7);
        assert_eq!(cache.capable_process(2), Some(9));
        // A refusal for the current process ends the capability until it reports again. Its
        // report restores the match also in the middle of its stream.
        cache.note_refused(2, 9);
        assert!(!cache.coverage_capable(2));
        cache.note_coverage(2, Some(9));
        assert!(cache.coverage_capable(2));
        // A late report of an earlier process does not count for the current one.
        cache.note_coverage(2, Some(7));
        assert!(!cache.coverage_capable(2));
        cache.note_coverage(2, Some(9));
        assert!(cache.coverage_capable(2));
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
            msg_type: MessageType::MsgPropose as i32,
            entries: vec![entry(5, 2)],
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
                view_max_age: std::time::Duration::from_secs(10),
                view_pin_max: std::time::Duration::from_secs(300),
                overlay_max_bytes: 1 << 30,
                max_clock_skew: std::time::Duration::from_secs(5),
                view_retention: Default::default(),
                restore: None,
                object_store: crate::object::Config::Test(
                    crate::object::test_store(dir.path()),
                    "root".into(),
                ),
                quota_bytes: 0,
                quota_warning_fraction: 0.0,
                auto_compaction: None,
                strict_reconfiguration: false,
                join: false,
                id: 1,
                cluster_id: "quota".into(),
                dir: dir.path().to_owned(),
                peers: (1..=3).map(|id| (id, format!("127.0.0.1:{id}"))).collect(),
                snapshot_every: 0,
                snapshot_min_bytes: 0,
                log_file_bytes: 256 * 1024,
                log_memory: LogMemory::default(),
                state_cache: Default::default(),
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
                msg_type: MessageType::MsgAppend as i32,
                from: 2,
                to: 1,
                term: wire_term,
                index,
                log_term: term,
                entries: vec![next],
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
            msg_type: MessageType::MsgAppend as i32,
            from: 2,
            to: 1,
            term: 2,
            index: 8,
            log_term: 1,
            entries: vec![control],
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
