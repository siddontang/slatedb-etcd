//! Remove compacted MVCC rows in bounded steps after the boundary is applied.
//!
//! Compact stores only the boundary and a progress record. The leader then proposes
//! `CompactStep` entries. Each replica applies one bounded step per entry, so every replica
//! deletes the same rows at the same index. The progress record shares the write batch with
//! the apply marker, so a restart resumes from the last applied step.

use std::collections::VecDeque;

use super::*;
use crate::maintenance::HASH;
use crate::proto::state::Version;
use bytes::Bytes;
use prost::Message;
use serde_json::{json, Value};

pub(crate) const PENDING: &[u8] = b"etcd/compact-pending";
const HASH_CACHE: &[u8] = b"etcd/hash-cache";
/// The scan cost that one step may use. One scanned row costs one unit.
const STEP_COST: usize = 2048;
/// A hash row lookup costs about as much as 12 sequential rows.
const LOOKUP_COST: usize = 12;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Phase {
    /// Hash the retained rows before this boundary removes any of them.
    Hash,
    /// Remove obsolete history versions and their hash records in key order.
    History,
    /// Remove Watch events below the boundary.
    Events,
    /// Hash the rows at or below the previous boundary that survive this boundary.
    Keep,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Target {
    pub revision: i64,
    /// The logical boundary before this compaction. It selects the hash filter.
    pub previous: i64,
    /// The store revision when Compact applied. The hash cache records it.
    pub current: i64,
    /// Members flush SlateDB when a physical target finishes.
    pub physical: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Progress {
    /// The first target is active. Later targets wait in request order.
    pub targets: VecDeque<Target>,
    /// The last boundary that has no obsolete rows.
    pub done: i64,
    phase: Phase,
    /// The next key that the active phase reads.
    cursor: Vec<u8>,
    crc_all: u32,
    crc_new: u32,
    len_new: u64,
    crc_keep: u32,
    /// Some record at or below the boundary survives this pass.
    retained: bool,
}

impl Progress {
    fn new(done: i64) -> Self {
        Self {
            targets: VecDeque::new(),
            done,
            phase: Phase::Hash,
            cursor: vec![],
            crc_all: crc32c::crc32c(b"key"),
            crc_new: 0,
            len_new: 0,
            crc_keep: crc32c::crc32c(b"key"),
            retained: false,
        }
    }

    fn next_target(&mut self) {
        let targets = std::mem::take(&mut self.targets);
        *self = Self {
            targets,
            ..Self::new(self.done)
        };
    }

    fn next_phase(&mut self, phase: Phase) {
        self.phase = phase;
        self.cursor.clear();
    }
}

pub(crate) async fn load(db: &impl StateRead) -> Result<Option<Progress>> {
    Ok(match db.get(PENDING).await? {
        Some(bytes) => Some(serde_json::from_slice(&bytes).context("invalid compaction progress")?),
        None => None,
    })
}

/// Sequential compaction scans read several blocks per request.
fn scan_options() -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        read_ahead_bytes: 256 * 1024,
        max_fetch_tasks: 2,
        ..Default::default()
    }
}

fn revision_bound(prefix: &[u8], revision: i64) -> Vec<u8> {
    // Revisions are positive here, so big-endian bytes sort like numbers.
    [prefix, &revision.saturating_add(1).max(0).to_be_bytes()].concat()
}

fn main_revision(key: &[u8]) -> Result<i64> {
    Ok(i64::from_be_bytes(
        key[HASH.len()..HASH.len() + 8]
            .try_into()
            .context("invalid hash key")?,
    ))
}

struct HistoryRow {
    key: Bytes,
    len: usize,
    user_key: Vec<u8>,
    revision: i64,
    tombstone: bool,
}

/// The result of one bounded step.
struct Step {
    progress: Progress,
    batch: WriteBatch,
    finished: bool,
}

/// A step that a background task reads before its `CompactStep` entry applies.
struct Plan {
    input: Progress,
    task: tokio::task::JoinHandle<Result<Step>>,
}

impl Drop for Plan {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Read the next step outside ordered apply.
///
/// A step reads only rows at or below its boundary: hash records, history versions, and Watch
/// events. Later entries add rows only above the boundary, and only compaction steps remove
/// rows. So a step that starts after the previous step is written reads the same rows as an
/// inline step at its own index. Apply uses a plan only when its input equals the current
/// progress, and it reads the step inline otherwise.
pub(crate) struct Planner {
    plan: Option<Plan>,
    generation: u64,
    /// Steps that applied a background read.
    pub planned: u64,
    /// The newest generation whose step is ready. The leader proposes a step after this.
    ready: Arc<tokio::sync::watch::Sender<u64>>,
}

impl Default for Planner {
    fn default() -> Self {
        Self {
            plan: None,
            generation: 0,
            planned: 0,
            ready: Arc::new(tokio::sync::watch::channel(0).0),
        }
    }
}

impl Planner {
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.ready.subscribe()
    }

    /// The generation that the next step needs. Zero means that no step is pending.
    pub fn generation(&self) -> u64 {
        self.plan.as_ref().map_or(0, |_| self.generation)
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.plan = None;
    }

    fn start(&mut self, state: StateView, progress: Option<&Progress>) {
        let Some(progress) = progress else {
            self.plan = None;
            return;
        };
        if self
            .plan
            .as_ref()
            .is_some_and(|plan| &plan.input == progress)
        {
            return;
        }
        self.generation += 1;
        let generation = self.generation;
        let ready = self.ready.clone();
        let input = progress.clone();
        let task = tokio::spawn(async move {
            // Publish also after a failure or panic, so the leader proposes the step and reads
            // it inline.
            let _ready = Ready(ready, generation);
            read_step(&state, input).await
        });
        self.plan = Some(Plan {
            input: progress.clone(),
            task,
        });
    }

    /// Take the planned step for `progress`, if one exists.
    async fn take(&mut self, progress: &Progress) -> Option<Step> {
        let mut plan = self.plan.take().filter(|plan| &plan.input == progress)?;
        match (&mut plan.task).await {
            Ok(Ok(step)) => {
                self.planned += 1;
                Some(step)
            }
            Ok(Err(error)) => {
                tracing::warn!(%error, "planned compaction step failed; reading it inline");
                None
            }
            Err(error) => {
                tracing::warn!(%error, "planned compaction step stopped; reading it inline");
                None
            }
        }
    }
}

struct Ready(Arc<tokio::sync::watch::Sender<u64>>, u64);

impl Drop for Ready {
    fn drop(&mut self) {
        let generation = self.1;
        self.0.send_if_modified(|old| {
            let newer = *old < generation;
            *old = (*old).max(generation);
            newer
        });
    }
}

async fn read_step(db: &impl StateRead, mut progress: Progress) -> Result<Step> {
    let mut batch = WriteBatch::new();
    let finished = Reader { db }.work(&mut progress, &mut batch).await?;
    Ok(Step {
        progress,
        batch,
        finished,
    })
}

/// Read-only compaction work. It reads storage and records deletes in the step batch.
struct Reader<'a, R> {
    db: &'a R,
}

impl StateMachine {
    /// The highest boundary whose obsolete rows are physically removed.
    pub fn physical_revision(&self) -> i64 {
        self.compaction
            .as_ref()
            .map_or(self.compact_revision, |progress| progress.done)
    }

    pub fn compaction_pending(&self) -> bool {
        self.compaction.is_some()
    }

    pub async fn apply_compaction(
        &mut self,
        index: u64,
        request: &CompactionRequest,
    ) -> Result<Outcome<CompactionResponse>> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        if self.alarms.iter().any(|alarm| alarm.alarm == 2) {
            self.apply(index, None).await?;
            return Ok(Outcome {
                result: Err(mvcc::KvError::new(tonic::Code::DataLoss, "corrupt cluster")),
            });
        }
        let result = mvcc::check_compaction(self.revision, self.compact_revision, request);
        let outcome = Outcome { result };
        let mut batch = WriteBatch::new();
        let mut progress = self.compaction.clone();
        let retry =
            outcome.result.is_err() && self.physical_compact_revision(request, &outcome).is_some();
        if let Some(target) = progress
            .as_mut()
            .and_then(|state| state.targets.back_mut())
            .filter(|target| retry && !target.physical)
        {
            // A physical retry waits for the pending boundary. Its end must reach storage.
            target.physical = true;
            batch.put(PENDING, serde_json::to_vec(&progress)?);
        }
        let result = outcome.result;
        if result.is_ok() {
            // Reads use the new boundary at once. Physical removal follows in later entries.
            progress
                .get_or_insert_with(|| Progress::new(self.compact_revision))
                .targets
                .push_back(Target {
                    revision: request.revision,
                    previous: self.compact_revision,
                    current: self.revision,
                    physical: request.physical,
                });
            batch.put(mvcc::COMPACT_REVISION, request.revision.to_be_bytes());
            batch.put(PENDING, serde_json::to_vec(&progress)?);
        }
        crate::failpoint::hit("compact-before-apply");
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        self.applied = index;
        if result.is_ok() || retry {
            self.compaction = progress;
        }
        if result.is_ok() {
            self.compact_revision = request.revision;
        }
        self.plan_compaction();
        crate::failpoint::hit("compact-after-apply");
        Ok(Outcome { result })
    }

    /// Apply one bounded part of the pending physical compaction.
    ///
    /// Return the new physical boundary when this step finished a physical target. The caller
    /// flushes SlateDB outside ordered apply before physical Compact replies.
    pub async fn apply_compaction_step(&mut self, index: u64) -> Result<Option<i64>> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let mut batch = WriteBatch::new();
        let mut progress = None;
        let mut finished = false;
        if let Some(current) = self.compaction.clone() {
            let step = match self.compaction_plan.take(&current).await {
                Some(step) => step,
                // The step reads before it changes state, so a storage error of a read
                // leaves the entry unapplied.
                None => read_step(self.applied_state(), current)
                    .await
                    .map_err(crate::mvcc::storage_fault::read_cause)?,
            };
            batch = step.batch;
            finished = step.finished;
            if step.progress.targets.is_empty() {
                batch.delete(PENDING);
            } else {
                batch.put(PENDING, serde_json::to_vec(&step.progress)?);
            }
            progress = Some(step.progress);
        }
        crate::failpoint::hit("compact-step-before-apply");
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        self.applied = index;
        self.compaction = progress.filter(|state| !state.targets.is_empty());
        self.plan_compaction();
        Ok(finished.then(|| self.physical_revision()))
    }

    /// Start reading the next step for the current progress in the background.
    pub(crate) fn plan_compaction(&mut self) {
        // The planner reads a pinned view in the background while apply continues.
        let state = self.pinned_state();
        self.compaction_plan.start(state, self.compaction.as_ref());
    }
}

impl<R: StateRead> Reader<'_, R> {
    /// Return true when at least one physical target is complete.
    async fn work(&self, state: &mut Progress, batch: &mut WriteBatch) -> Result<bool> {
        let mut budget = STEP_COST;
        let mut finished = false;
        let mut cache: Option<Vec<Value>> = None;
        while budget > 0 {
            let Some(target) = state.targets.front().copied() else {
                break;
            };
            match state.phase {
                Phase::Hash => {
                    if self.hash_phase(target, state, &mut budget).await? {
                        state.next_phase(Phase::History);
                    }
                }
                Phase::History => {
                    if self
                        .history_phase(target, state, batch, &mut budget)
                        .await?
                    {
                        state.next_phase(Phase::Events);
                        // Later phases read hash rows. Let this batch reach storage first.
                        break;
                    }
                }
                Phase::Events => {
                    if self.events_phase(target, state, batch, &mut budget).await? {
                        state.next_phase(Phase::Keep);
                    }
                }
                Phase::Keep => {
                    // Without a retained record, etcd hashes every older record.
                    if state.retained && !self.keep_phase(target, state, &mut budget).await? {
                        continue;
                    }
                    let part = if state.retained {
                        state.crc_keep
                    } else {
                        state.crc_all
                    };
                    let hash = crate::crc::combine(part, state.crc_new, state.len_new as usize);
                    let hashes = match &mut cache {
                        Some(hashes) => hashes,
                        None => cache.insert(self.hash_cache().await?),
                    };
                    hashes.push(json!({"header": mvcc::header(target.current), "hash": hash,
                        "compact_revision": target.previous, "hash_revision": target.revision}));
                    if hashes.len() > 10 {
                        hashes.remove(0);
                    }
                    batch.put(HASH_CACHE, serde_json::to_vec(hashes)?);
                    state.done = target.revision;
                    state.targets.pop_front();
                    state.next_target();
                    finished |= target.physical;
                }
            }
        }
        Ok(finished)
    }

    async fn hash_cache(&self) -> Result<Vec<Value>> {
        match self.db.get(HASH_CACHE).await? {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Ok(vec![]),
        }
    }

    fn start(state: &Progress, prefix: &[u8]) -> Vec<u8> {
        if state.cursor.is_empty() {
            prefix.to_vec()
        } else {
            state.cursor.clone()
        }
    }

    async fn hash_phase(
        &self,
        target: Target,
        state: &mut Progress,
        budget: &mut usize,
    ) -> Result<bool> {
        let end = revision_bound(HASH, target.revision);
        let mut scan = self
            .db
            .scan_range(Self::start(state, HASH)..end, &scan_options())
            .await?;
        while let Some(row) = scan.next().await? {
            if *budget == 0 {
                state.cursor = row.key.to_vec();
                return Ok(false);
            }
            *budget -= 1;
            let key = &row.key[HASH.len()..];
            let main = main_revision(&row.key)?;
            if main <= target.previous {
                // The previous pass left one retained record for each key.
                if !(main == target.previous && key.len() == 18) {
                    let crc = crc32c::crc32c_append(state.crc_all, key);
                    state.crc_all = maintenance::hash_append(crc, &row.value)?.0;
                }
            } else {
                let crc = crc32c::crc32c_append(state.crc_new, key);
                let (crc, len) = maintenance::hash_append(crc, &row.value)?;
                state.crc_new = crc;
                state.len_new += key.len() as u64 + len;
            }
        }
        Ok(true)
    }

    async fn keep_phase(
        &self,
        target: Target,
        state: &mut Progress,
        budget: &mut usize,
    ) -> Result<bool> {
        let end = revision_bound(HASH, target.previous);
        let mut scan = self
            .db
            .scan_range(Self::start(state, HASH)..end, &scan_options())
            .await?;
        while let Some(row) = scan.next().await? {
            if *budget == 0 {
                state.cursor = row.key.to_vec();
                return Ok(false);
            }
            *budget -= 1;
            // History pruning left only records that are live at the new boundary.
            let crc = crc32c::crc32c_append(state.crc_keep, &row.key[HASH.len()..]);
            state.crc_keep = maintenance::hash_append(crc, &row.value)?.0;
        }
        Ok(true)
    }

    async fn events_phase(
        &self,
        target: Target,
        state: &mut Progress,
        batch: &mut WriteBatch,
        budget: &mut usize,
    ) -> Result<bool> {
        let start = Self::start(state, crate::watch::EVENTS);
        let mut scan = self
            .db
            .scan_range(start..crate::watch::key(target.revision), &scan_options())
            .await?;
        while let Some(row) = scan.next().await? {
            if *budget == 0 {
                state.cursor = row.key.to_vec();
                return Ok(false);
            }
            *budget -= 1;
            batch.delete_known(row.key, row.value.len());
        }
        Ok(true)
    }

    async fn history_phase(
        &self,
        target: Target,
        state: &mut Progress,
        batch: &mut WriteBatch,
        budget: &mut usize,
    ) -> Result<bool> {
        let mut end = mvcc::HISTORY.to_vec();
        *end.last_mut().unwrap() += 1;
        let mut scan = self
            .db
            .scan_range(Self::start(state, mvcc::HISTORY)..end, &scan_options())
            .await?;
        let mut group: Vec<HistoryRow> = vec![];
        let mut prefix: Option<Bytes> = None;
        while let Some(row) = scan.next().await? {
            ensure!(
                row.key.len() >= mvcc::HISTORY.len() + 9,
                "invalid history key"
            );
            let split = row.key.len() - 8;
            let revision = i64::from_be_bytes(row.key[split..].try_into()?);
            if revision > target.revision {
                // Later entries add these rows while a planned step reads. They must not change
                // the step budget or its cursor.
                continue;
            }
            if prefix
                .as_ref()
                .is_some_and(|prefix| prefix[..] != row.key[..split])
            {
                // Each key's versions change in one batch, so reads never see a partial key.
                self.prune_versions(target, state, &group, batch, budget)
                    .await?;
                group.clear();
                if *budget == 0 {
                    state.cursor = row.key.to_vec();
                    return Ok(false);
                }
            }
            prefix = Some(row.key.slice(..split));
            *budget = budget.saturating_sub(1);
            let len = row.value.len();
            let version = Version::decode(row.value)?;
            group.push(HistoryRow {
                key: row.key,
                len,
                user_key: version.key,
                revision,
                tombstone: version.value.is_none(),
            });
        }
        self.prune_versions(target, state, &group, batch, budget)
            .await?;
        Ok(true)
    }

    /// Keep the latest version at or before the boundary, including tombstones.
    async fn prune_versions(
        &self,
        target: Target,
        state: &mut Progress,
        versions: &[HistoryRow],
        batch: &mut WriteBatch,
        budget: &mut usize,
    ) -> Result<()> {
        let Some((latest, older)) = versions.split_last() else {
            return Ok(());
        };
        // A tombstone at the boundary remains, so it also selects the retained-record hash.
        state.retained |= !latest.tombstone || latest.revision == target.revision;
        for version in older {
            batch.delete_known(version.key.clone(), version.len);
            self.delete_hash(version, batch, budget).await?;
        }
        // The hash keeps a tombstone only at the boundary revision.
        if latest.tombstone && latest.revision < target.revision {
            self.delete_hash(latest, batch, budget).await?;
        }
        Ok(())
    }

    async fn delete_hash(
        &self,
        version: &HistoryRow,
        batch: &mut WriteBatch,
        budget: &mut usize,
    ) -> Result<()> {
        *budget = budget.saturating_sub(LOOKUP_COST);
        let matches = |value: &Bytes| -> Result<bool> {
            Ok(maintenance::hash_record_key(value)? == version.user_key)
        };
        let key = maintenance::hash_key(version.revision, 0, version.tombstone);
        if let Some(value) = self.db.get(&key).await? {
            if matches(&value)? {
                batch.delete_known(key.into(), value.len());
                return Ok(());
            }
        }
        // Transactions record later changes at higher sub-revisions.
        let mut prefix = HASH.to_vec();
        prefix.extend_from_slice(&version.revision.to_be_bytes());
        prefix.push(b'_');
        let mut scan = self.db.scan_under(&prefix, &Default::default()).await?;
        while let Some(row) = scan.next().await? {
            if (row.key.len() == HASH.len() + 18) == version.tombstone && matches(&row.value)? {
                batch.delete_known(row.key, row.value.len());
                break;
            }
        }
        Ok(())
    }
}
