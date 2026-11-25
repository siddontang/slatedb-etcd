//! Leader proposals of the replicated cluster quota. See `state::quota`.
//!
//! The `--quota-backend-bytes` flag seeds the cluster quota and requests changes to it. A member
//! keeps a local record of its flag in the data directory. Raft, snapshots, and backups do not
//! carry that record. A restart with a changed flag, or with `--quota-reassert`, makes a pending
//! request with the wall-clock time. The member proposes it whenever it leads, until the cluster
//! quota equals it or holds a newer request. The newest request wins, so a member with an
//! unchanged old flag does not revert the cluster quota when it leads.
//!
//! A state that this binary created empty never applies a quota-checked entry under its flag.
//! Such an entry before the first `SetQuota` entry comes from an earlier binary. The member
//! finds it in its log at open and as entries arrive. Apply then stops before it, and the member
//! asks the leader for a snapshot. Until then it does not lead: it skips elections while its
//! leader is live, ignores leadership transfers, and hands over leadership if it wins. It still
//! votes. A member also checkpoints through a first cluster quota that follows such entries, so
//! the leader sends later members a snapshot. See `StateMachine::quota_boundary`.
//!
//! The same guard keeps such a state from an entry of an earlier binary before the first
//! `SeedMembers` entry, in both state modes; see `state::cluster`. A local origin record in the
//! data directory, kept in both modes, says which guards apply. See `Origin`.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::*;
use crate::state::quota::ClusterQuota;

/// The file of the local flag record in the data directory.
const RECORD: &str = "quota-flag.json";

/// The file of the local record of the start that created the data directory. Both state modes
/// keep it. Raft, snapshots, and backups do not carry it.
const ORIGIN: &str = "state-origin.json";

/// The guards that apply to a data directory, by the start that created it. See `Guards`.
///
/// The first start of this binary writes the record. Without an earlier record, a guard applies
/// when that start found an empty Raft log and an empty state. #116 kept the quota mark in its
/// flag record, so the quota guard keeps that mark. A directory with history at that start
/// holds decisions of an earlier binary, which applied it without seeds. So the membership guard
/// does not apply there, and replay applies the entries as that binary did.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Origin {
    /// The state was created empty by this binary, or by #116 before it.
    quota_created_empty: bool,
    /// The state was created empty by this binary.
    members_created_empty: bool,
}

/// An operator request of this member: a flag value and the wall-clock time of the start that
/// saw it, in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Request {
    bytes: i64,
    requested_at: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    /// The last flag value that needs no proposal: the first flag of this directory, or a flag
    /// whose request the cluster took or replaced.
    acknowledged: i64,
    /// A request that this member must propose.
    pending: Option<Request>,
}

/// The options of `Quota::open`.
pub(super) struct Startup {
    /// The member ID. It orders requests with equal times.
    pub member: u64,
    /// The effective flag value. A non-positive value disables the quota.
    pub flag: i64,
    pub shared: bool,
    /// The Raft log and the state are empty.
    pub empty: bool,
    /// `--quota-reassert`: request the flag again with a new time.
    pub reassert: bool,
    /// The wall-clock time in milliseconds.
    pub now: u64,
}

/// The cluster state that a state created empty still lacks. While it lacks an input, an
/// entry of an earlier binary can need it. A `SetQuota` entry gives the cluster quota, and a
/// `SeedMembers` entry gives the membership row. See `state::cluster`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Guards {
    /// A replicated state has no cluster quota. Shared mode takes it from the cluster claim.
    pub quota: bool,
    /// The state has no membership row.
    pub members: bool,
}

impl Guards {
    fn new(shared: bool, cluster_quota: bool, members_row: bool) -> Self {
        Self {
            quota: !shared && !cluster_quota,
            members: !members_row,
        }
    }

    /// The guards of `state` that apply to a directory with origin `origin`.
    pub(super) fn of(state: &StateMachine, origin: Self) -> Self {
        Self::new(
            state.is_shared(),
            state.cluster_quota().is_some(),
            state.members_row.is_some(),
        )
        .and(origin)
    }

    pub(super) fn any(self) -> bool {
        self.quota || self.members
    }

    /// The guards that both `self` and `other` still hold.
    fn and(self, other: Self) -> Self {
        Self {
            quota: self.quota && other.quota,
            members: self.members && other.members,
        }
    }
}

/// The first entry that decides whether a state created empty can apply its log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Gate {
    /// The `SetQuota` or `SeedMembers` entry that gives the last missing input. The entries
    /// after it apply under cluster state.
    Passed(u64),
    /// An entry of an earlier binary that needs a missing input: a quota-checked entry before
    /// any `SetQuota` entry, or another entry than these two before any `SeedMembers` entry.
    /// Other members applied it under their own flags.
    Checked(u64),
}

/// Return the first gate in `entries`, by Raft index. `guards` holds the inputs that the state
/// lacks before `entries`, and the inputs that it still lacks after them.
pub(super) fn first_gate(entries: &[Entry], guards: &mut Guards) -> Result<Option<Gate>> {
    for entry in entries {
        if entry.data.is_empty() {
            continue;
        }
        let command = match EntryType::try_from(entry.entry_type)? {
            EntryType::EntryNormal => RaftCommand::decode(&entry.data)?,
            // A configuration change of an earlier binary changes the membership row.
            EntryType::EntryConfChange if guards.members => {
                return Ok(Some(Gate::Checked(entry.index)))
            }
            _ => continue,
        };
        let passed = match command {
            RaftCommand::SetQuota { .. } => std::mem::take(&mut guards.quota),
            RaftCommand::SeedMembers(_) => std::mem::take(&mut guards.members),
            _ => false,
        };
        if passed && !guards.any() {
            return Ok(Some(Gate::Passed(entry.index)));
        }
        if (guards.quota && crate::state::quota::checks_quota(&command))
            || (guards.members && crate::state::cluster::needs_members(&command))
        {
            return Ok(Some(Gate::Checked(entry.index)));
        }
    }
    Ok(None)
}

/// Keep only the entries that a recovery replay can apply. A state of a directory with origin
/// `origin` stops before a `Gate::Checked` entry. Return true when no entry is left: the member
/// then waits for a snapshot.
pub(super) fn guard_replay(
    state: &StateMachine,
    origin: Guards,
    entries: &mut Vec<Entry>,
) -> Result<bool> {
    let mut guards = Guards::of(state, origin);
    if !guards.any() {
        return Ok(false);
    }
    if let Some(Gate::Checked(index)) = first_gate(entries, &mut guards)? {
        entries.retain(|entry| entry.index < index);
    }
    Ok(entries.is_empty())
}

/// The quota flag of this member and its proposal state.
pub(super) struct Quota {
    member: u64,
    flag: i64,
    /// The record file and its content. `None` in shared mode, which never proposes.
    record: Option<(PathBuf, Record)>,
    /// The guards that apply to this directory. See `Origin`.
    origin: Guards,
    /// The index and term of the last `SetQuota` proposal of this member.
    proposed: Option<(u64, u64)>,
    /// The cluster quota that `observe_quota` last handled.
    observed: Option<Option<ClusterQuota>>,
    /// The first gate in the log above the applied index, while a state created empty has no
    /// cluster quota. A `Gate::Checked` entry waits for a snapshot.
    gate: Option<Gate>,
    /// The last log index that the gate scan covered.
    scanned: u64,
    /// The inputs that the state still lacks after `scanned`, when it is above the applied
    /// index.
    open: Guards,
    /// Ticks since the last message of the current leader. Raft keeps the ID of a lost leader
    /// while the timer cap of `tick_raft` holds, so the ID is not a liveness signal.
    silent: usize,
    /// Simulation: the member proposes no `SetQuota`, like an earlier binary.
    #[cfg(test)]
    pub(super) hold: bool,
}

/// Return the `SetQuota` request and seed mark that a leader proposes, or `None`.
///
/// This is the "operator-change detection" policy. Without a cluster quota, a leader seeds it
/// from its flag, or sets its pending request. Otherwise it proposes only its own pending
/// request, and only when that request is newer than the request of the cluster quota.
fn proposal(
    cluster: Option<ClusterQuota>,
    flag: i64,
    pending: Option<Request>,
    member: u64,
) -> Option<(ClusterQuota, bool)> {
    let request = |request: Request| ClusterQuota {
        bytes: request.bytes,
        requested_at: request.requested_at,
        member,
    };
    match (cluster, pending) {
        (None, None) => Some((
            ClusterQuota {
                bytes: flag,
                requested_at: 0,
                member,
            },
            true,
        )),
        (None, Some(pending)) => Some((request(pending), false)),
        // A newer request with the same value still moves the order of the row forward.
        (Some(row), Some(pending)) => {
            let request = request(pending);
            (request.key() > row.key()).then_some((request, false))
        }
        (Some(_), None) => None,
    }
}

impl Quota {
    /// Load the record of `dir` and update it for this start.
    ///
    /// A member without a record acknowledges its flag, so a new member requests no change. A
    /// flag that differs from the acknowledged value makes a request. A request for the same
    /// flag keeps its time, unless `reassert` asks for a new one.
    pub(super) fn open(dir: &Path, startup: Startup) -> Result<Self> {
        let mut quota = Self {
            member: startup.member,
            flag: startup.flag,
            record: None,
            origin: open_origin(dir, startup.empty)?,
            proposed: None,
            observed: None,
            gate: None,
            scanned: 0,
            open: Guards {
                quota: true,
                members: true,
            },
            silent: 0,
            #[cfg(test)]
            hold: false,
        };
        if startup.shared {
            return Ok(quota);
        }
        let path = dir.join(RECORD);
        let stored = match std::fs::read(&path) {
            Ok(data) => {
                Some(serde_json::from_slice::<Record>(&data).context("invalid quota record")?)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let flag = startup.flag;
        let acknowledged = stored.map_or(flag, |record| record.acknowledged);
        let request = Request {
            bytes: flag,
            requested_at: startup.now,
        };
        let pending = match stored.and_then(|record| record.pending) {
            _ if startup.reassert => Some(request),
            Some(pending) if pending.bytes == flag => Some(pending),
            _ => (flag != acknowledged).then_some(request),
        };
        let record = Record {
            acknowledged,
            pending,
        };
        if stored != Some(record) {
            write_record(&path, &record)?;
        }
        quota.record = Some((path, record));
        Ok(quota)
    }

    fn pending(&self) -> Option<Request> {
        self.record.as_ref().and_then(|(_, record)| record.pending)
    }

    /// This binary created the data directory empty.
    #[cfg(test)]
    pub(super) fn created_empty(&self) -> bool {
        self.origin.any()
    }

    /// The guards that apply to this directory. See `Origin`.
    pub(super) fn origin(&self) -> Guards {
        self.origin
    }

    /// The quota-checked entry that waits for a snapshot, if any.
    pub(super) fn blocked(&self) -> Option<u64> {
        match self.gate {
            Some(Gate::Checked(index)) => Some(index),
            _ => None,
        }
    }

    /// Record the gate that an open scan of the durable log found. See `scan_storage`.
    pub(super) fn set_gate(&mut self, gate: Option<Gate>, scanned: u64) {
        self.gate = gate;
        self.scanned = scanned;
    }

    /// Record a scan through `scanned` that found `gate`, and the inputs that remain open.
    pub(super) fn set_scan(&mut self, gate: Option<Gate>, scanned: u64, open: Guards) {
        self.set_gate(gate, scanned);
        self.open = open;
    }

    /// Clear the pending request once the cluster quota holds it or a newer request. This needs
    /// only the order of the current row, so batched apply, restarts, and snapshot installs do
    /// not lose an acknowledgment. An equal value does not clear it: an older request with that
    /// value can precede newer requests in the log that is not applied yet.
    fn acknowledge(&mut self, cluster: Option<ClusterQuota>) -> Result<()> {
        let member = self.member;
        let Some((path, record)) = &mut self.record else {
            return Ok(());
        };
        let (Some(pending), Some(row)) = (record.pending, cluster) else {
            return Ok(());
        };
        if (row.requested_at, row.member) >= (pending.requested_at, member) {
            *record = Record {
                acknowledged: self.flag,
                pending: None,
            };
            write_record(path, record)?;
        }
        Ok(())
    }
}

/// Find the first gate in the durable log above `applied`, in bounded batches, so the scan
/// holds no backlog. Return the gate, the last index of the log, and the open guards.
pub(super) fn scan_storage(
    storage: &RaftStorage,
    applied: u64,
    mut guards: Guards,
) -> Result<(Option<Gate>, u64, Guards)> {
    let last = storage.last_index()?;
    let mut low = applied.max(storage.first_index()? - 1) + 1;
    while low <= last {
        let entries = storage.entries(
            low,
            (low + 256).min(last + 1),
            Some(1 << 20),
            raft::GetEntriesContext::empty(false),
        )?;
        let Some(end) = entries.last().map(|entry| entry.index) else {
            anyhow::bail!("empty quota scan batch");
        };
        if let Some(gate) = first_gate(&entries, &mut guards)? {
            return Ok((Some(gate), last, guards));
        }
        low = end + 1;
    }
    Ok((None, last, guards))
}

/// Return `conf` with `id` as a learner, not a voter.
pub(super) fn without_vote(
    mut conf: raft::eraftpb::ConfState,
    id: u64,
) -> raft::eraftpb::ConfState {
    conf.voters.retain(|&voter| voter != id);
    if !conf.learners.contains(&id) {
        conf.learners.push(id);
    }
    conf
}

/// Read the origin record of `dir`. The first start writes it: `empty` is true when this start
/// found an empty Raft log and an empty state.
fn open_origin(dir: &Path, empty: bool) -> Result<Guards> {
    let path = dir.join(ORIGIN);
    let origin = match read_json::<Origin>(&path).context("invalid origin record")? {
        Some(origin) => origin,
        None => {
            // #116 kept the quota mark in its flag record. A member that waits for a snapshot
            // must keep waiting after the upgrade.
            let earlier = read_json::<serde_json::Value>(&dir.join(RECORD))
                .context("invalid quota record")?
                .and_then(|record| record["created_empty"].as_bool());
            let origin = Origin {
                quota_created_empty: earlier.unwrap_or(empty),
                members_created_empty: empty,
            };
            write_record(&path, &origin)?;
            origin
        }
    };
    Ok(Guards {
        quota: origin.quota_created_empty,
        members: origin.members_created_empty,
    })
}

/// Read a JSON file, or `None` when it does not exist.
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match std::fs::read(path) {
        Ok(data) => Ok(Some(serde_json::from_slice(&data)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Replace the record file atomically.
fn write_record(path: &Path, record: &impl Serialize) -> Result<()> {
    let dir = path.parent().context("quota record has no directory")?;
    let temporary = path.with_extension("json.pending");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(record)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// The wall-clock time in milliseconds.
pub(super) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| time.as_millis() as u64)
}

impl Node {
    /// Propose the cluster quota when this member leads and `proposal` asks for it.
    ///
    /// The node calls this right after each Raft step and tick, before it takes other work. So
    /// the entry follows the empty entry of a new leader, before every client write of the term.
    /// One proposal of a term stays in the log, so the next one waits until it applies.
    pub(super) fn propose_quota(&mut self) -> Result<()> {
        if self.raw.raft.state != StateRole::Leader || self.quota.record.is_none() {
            return Ok(());
        }
        #[cfg(test)]
        if self.quota.hold {
            return Ok(());
        }
        let term = self.raw.raft.term;
        if let Some((index, proposed)) = self.quota.proposed {
            if proposed == term && self.state.applied < index {
                return Ok(());
            }
        }
        let Some((request, seed)) = proposal(
            self.state.cluster_quota,
            self.quota.flag,
            self.quota.pending(),
            self.quota.member,
        ) else {
            return Ok(());
        };
        let data = RaftCommand::SetQuota {
            id: uuid::Uuid::new_v4().to_string(),
            bytes: request.bytes,
            seed,
            requested_at: request.requested_at,
            member: request.member,
        }
        .encode()?;
        // The entry is small. It skips the memory admission check, so no client write goes
        // first. Raft drops it only during a leadership transfer; the next call tries again.
        if self.raw.propose(vec![], data).is_ok() {
            self.quota.proposed = Some((self.raw.raft.raft_log.last_index(), term));
            tracing::info!(
                bytes = request.bytes,
                requested_at = request.requested_at,
                seed,
                term,
                "propose the cluster quota"
            );
        }
        Ok(())
    }

    /// Update the local record and the gate after apply, and warn when the flag differs from
    /// the quota that apply uses.
    pub(super) fn observe_quota(&mut self) -> Result<()> {
        // A snapshot install moved the state past the waiting entry. Scan the rest again.
        if self
            .quota
            .blocked()
            .is_some_and(|index| self.state.applied >= index)
        {
            self.quota.set_gate(None, self.state.applied);
        }
        self.scan_quota_log(None)?;
        let cluster = self.state.cluster_quota;
        if self.quota.observed == Some(cluster) {
            return Ok(());
        }
        self.quota.observed = Some(cluster);
        self.quota.acknowledge(cluster)?;
        // Shared mode warns when it reads the cluster claim.
        if self.state.quota_mismatch && self.quota.record.is_some() {
            tracing::warn!(
                configured = self.quota.flag,
                cluster = self.state.quota_bytes,
                pending = self.quota.pending().is_some(),
                "the cluster state sets the quota"
            );
        }
        Ok(())
    }

    /// The inputs that the applied state lacks, for the guards of this directory. See `Guards`.
    fn guards(&self) -> Guards {
        Guards::new(
            !self.options.state_mode.is_replicated(),
            self.state.cluster_quota.is_some(),
            self.state.members_row.is_some(),
        )
        .and(self.quota.origin())
    }

    /// Return true while a guard of this directory applies: a state created empty lacks the
    /// cluster quota or the membership row. Apply then runs one batch at a time and stops before
    /// a `Gate::Checked` entry.
    pub(super) fn quota_guarded(&self) -> bool {
        self.guards().any()
    }

    /// Find the first gate in the cached log above the applied index. `replaced` is the first
    /// index of entries that replace a log suffix. Run after Raft entries are staged and after
    /// apply.
    pub(super) fn scan_quota_log(&mut self, replaced: Option<u64>) -> Result<()> {
        if !self.quota_guarded() {
            self.quota.set_gate(None, 0);
            return Ok(());
        }
        // Replaced entries can change only the gate at or after their first index. A guard on
        // an earlier entry stays. A snapshot keeps it too, until the install passes it.
        if let Some(first) = replaced {
            let index = match self.quota.gate {
                Some(Gate::Passed(index) | Gate::Checked(index)) => index,
                None => u64::MAX,
            };
            // A guard on an earlier entry stays. Without one, scan again from the applied
            // index: the replaced suffix may have given an input that `open` holds.
            let gate = self.quota.gate.filter(|_| index < first);
            let scanned = match gate {
                Some(_) => self.quota.scanned.min(first - 1),
                None => 0,
            };
            self.quota.set_gate(gate, scanned);
        }
        if self.quota.gate.is_some() {
            return Ok(());
        }
        let first = self.raw.store().first_index()?;
        let low = self.quota.scanned.max(self.state.applied).max(first - 1) + 1;
        let last = self.raw.store().last_index()?;
        if low > last {
            return Ok(());
        }
        // Entries above the applied index are pending in the cache.
        let entries =
            self.raw
                .store()
                .entries(low, last + 1, None, raft::GetEntriesContext::empty(false))?;
        let mut guards = self.guards();
        if self.quota.scanned > self.state.applied {
            guards = guards.and(self.quota.open);
        }
        let gate = first_gate(&entries, &mut guards)?;
        if let Some(Gate::Checked(index)) = gate {
            tracing::error!(
                index,
                "entry needs the cluster quota or membership; apply waits for a snapshot from the \
                 leader"
            );
        }
        self.quota.set_scan(gate, last, guards);
        Ok(())
    }

    /// Limit an apply batch that ends before `high` to the entries before a waiting entry. Ask
    /// the leader for a snapshot when no entry is left.
    pub(super) fn quota_apply_limit(&mut self, low: u64, high: u64) -> u64 {
        let Some(blocked) = self.quota.blocked() else {
            return high;
        };
        if blocked <= low {
            self.request_quota_snapshot();
        }
        high.min(blocked)
    }

    /// Ask the leader for a snapshot that replaces the entries before the cluster quota. Raft
    /// drops the request on a leader, without a leader, or with a request in flight. The next
    /// tick asks again. In shared mode an idle leader cannot yet offer a base above its last
    /// batch, so the recovery of an idle cluster depends on #96.
    fn request_quota_snapshot(&mut self) {
        if self.quota.blocked() == Some(self.state.applied + 1) {
            let _ = self.raw.request_snapshot();
        }
    }

    /// Tick Raft. A member that waits for a snapshot does not start an election while its
    /// leader is live, so the group does not elect a member that cannot apply.
    ///
    /// After three election timeouts without a message of the leader, it may campaign, so it
    /// never holds up the group. If it wins, `acts_as_leader` hands leadership over at once. Raft
    /// itself refuses the campaign while a committed configuration change is not applied, and
    /// apply stops before the waiting entry, so such a change stays unapplied. A member that
    /// skipped its backlog at open is a learner until the snapshot, so it never campaigns.
    pub(super) fn tick_raft(&mut self) -> Result<()> {
        if self.quota.blocked().is_some() {
            self.quota.silent += 1;
            let raft = &mut self.raw.raft;
            if self.quota.silent < 3 * raft.election_timeout() {
                // Keep the timer past the leader lease, so the member still votes, but below
                // the largest randomized timeout, so it does not campaign.
                let timeout = 2 * raft.election_timeout() - 1;
                raft.set_randomized_election_timeout(timeout);
                raft.election_elapsed = raft.election_elapsed.min(timeout - 2);
            }
            self.request_quota_snapshot();
        } else {
            self.quota.silent = 0;
        }
        self.raw.tick();
        self.propose_term_start()
    }

    /// Step a peer message. A member that waits for a snapshot ignores a leadership transfer.
    /// A new leader proposes the cluster quota before the next client write.
    pub(super) fn step_peer(&mut self, message: Message) -> raft::Result<()> {
        if message.get_msg_type() == MessageType::MsgTimeoutNow && self.quota.blocked().is_some() {
            tracing::info!(
                from = message.from,
                "ignore a leadership transfer before a snapshot"
            );
            return Ok(());
        }
        let from = message.from;
        let leader_message = matches!(
            message.get_msg_type(),
            MessageType::MsgAppend | MessageType::MsgHeartbeat | MessageType::MsgSnapshot
        );
        self.raw.step(message)?;
        if leader_message && from == self.raw.raft.leader_id {
            self.quota.silent = 0;
        }
        if let Err(error) = self.propose_term_start() {
            tracing::warn!(%error, "cannot propose the term-start entries");
        }
        Ok(())
    }

    /// Return true when this member leads and acts as leader. A leader that waits for a
    /// snapshot cannot apply, so it proposes nothing and hands leadership over.
    pub(super) fn acts_as_leader(&mut self) -> bool {
        if self.raw.raft.state != StateRole::Leader {
            return false;
        }
        if self.quota.blocked().is_some() {
            self.yield_leadership();
            return false;
        }
        true
    }

    /// Hand leadership to the voter with the longest matched log.
    fn yield_leadership(&mut self) {
        if self.raw.raft.lead_transferee.is_some() {
            return;
        }
        let id = self.options.id;
        let target = self
            .raw
            .raft
            .prs()
            .iter()
            .filter(|(&peer, _)| peer != id)
            .filter(|(&peer, _)| self.raw.raft.prs().conf().voters().contains(peer))
            .max_by_key(|(_, progress)| progress.matched)
            .map(|(&peer, _)| peer);
        if let Some(target) = target {
            tracing::warn!(
                target,
                "hand over leadership while apply waits for a snapshot"
            );
            self.raw.transfer_leader(target);
        }
    }

    /// The quota fields of `/status`.
    pub(super) fn quota_status(&self) -> Value {
        json!({
            "bytes": self.state.quota_bytes,
            "cluster": self.state.cluster_quota,
            "flag_bytes": self.quota.flag,
            "flag_mismatch": self.state.quota_mismatch,
            "flag_pending": self.quota.pending(),
            "blocked_index": self.quota.blocked(),
        })
    }

    /// The `db_size_quota` of Maintenance Status. Like etcd, it is the configured value, unless
    /// the quota that apply uses differs from it.
    pub(super) fn status_quota(&self) -> i64 {
        if self.state.quota_mismatch {
            self.state.quota_bytes
        } else {
            self.options.quota_bytes
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::super::tests::options;
    use super::*;
    use raft::eraftpb::HardState;

    pub(in crate::node) fn row(bytes: i64, requested_at: u64, member: u64) -> ClusterQuota {
        ClusterQuota {
            bytes,
            requested_at,
            member,
        }
    }

    pub(in crate::node) fn set_quota(bytes: i64, requested_at: u64, member: u64) -> RaftCommand {
        RaftCommand::SetQuota {
            id: format!("set-{bytes}-{requested_at}"),
            bytes,
            seed: requested_at == 0,
            requested_at,
            member,
        }
    }

    fn startup(flag: i64, now: u64, empty: bool, reassert: bool) -> Startup {
        Startup {
            member: 1,
            flag,
            shared: false,
            empty,
            reassert,
            now,
        }
    }

    #[test]
    fn a_leader_seeds_or_proposes_only_a_newer_request() {
        let pending = |bytes, requested_at| {
            Some(Request {
                bytes,
                requested_at,
            })
        };
        // No cluster quota: seed it, or set the pending request.
        assert_eq!(proposal(None, 5, None, 1), Some((row(5, 0, 1), true)));
        assert_eq!(
            proposal(None, 5, pending(5, 9), 1),
            Some((row(5, 9, 1), false))
        );
        // An unchanged old flag does not replace the cluster quota.
        assert_eq!(proposal(Some(row(9, 0, 2)), 5, None, 1), None);
        // The newer request wins. Equal times compare the member ID.
        assert_eq!(
            proposal(Some(row(9, 3, 2)), 5, pending(5, 4), 1),
            Some((row(5, 4, 1), false))
        );
        assert_eq!(proposal(Some(row(9, 5, 2)), 5, pending(5, 4), 1), None);
        assert_eq!(proposal(Some(row(9, 4, 2)), 5, pending(5, 4), 1), None);
        assert_eq!(
            proposal(Some(row(9, 4, 2)), 5, pending(5, 4), 3),
            Some((row(5, 4, 3), false))
        );
        // A newer request with the same value still moves the row forward.
        assert_eq!(
            proposal(Some(row(5, 1, 2)), 5, pending(5, 4), 1),
            Some((row(5, 4, 1), false))
        );
        // A disabled quota is a value like any other.
        assert_eq!(
            proposal(Some(row(9, 3, 2)), -1, pending(-1, 4), 1),
            Some((row(-1, 4, 1), false))
        );
    }

    #[test]
    fn a_flag_request_keeps_its_time_until_the_cluster_takes_it() {
        let dir = tempfile::tempdir().unwrap();
        let open = |flag, now, empty, reassert| {
            Quota::open(dir.path(), startup(flag, now, empty, reassert)).unwrap()
        };
        let request = |bytes, requested_at| {
            Some(Request {
                bytes,
                requested_at,
            })
        };
        // A member without a record requests no change.
        let quota = open(100, 10, true, false);
        assert_eq!(quota.pending(), None);
        assert!(quota.created_empty());
        // Restarts before the member leads keep the latest flag and the time of its first start.
        assert_eq!(open(200, 20, false, false).pending(), request(200, 20));
        assert_eq!(open(200, 30, false, false).pending(), request(200, 20));
        let mut quota = open(300, 40, false, false);
        assert_eq!(quota.pending(), request(300, 40));
        assert!(quota.created_empty());
        // An older request with another value does not clear it. A newer one does.
        quota.acknowledge(Some(row(100, 39, 2))).unwrap();
        assert_eq!(quota.pending(), request(300, 40));
        quota.acknowledge(Some(row(250, 41, 2))).unwrap();
        assert_eq!(quota.pending(), None);
        assert_eq!(open(300, 50, false, false).pending(), None);
        // `--quota-reassert` requests the same flag again with a new time.
        assert_eq!(open(300, 60, false, true).pending(), request(300, 60));
        // Back to the acknowledged value: nothing is pending.
        assert_eq!(open(100, 70, false, false).pending(), request(100, 70));
        assert_eq!(open(300, 80, false, false).pending(), None);
        // Shared mode keeps no flag record. It keeps the origin record.
        let shared = tempfile::tempdir().unwrap();
        let mut startup = startup(1, 1, true, false);
        startup.shared = true;
        let quota = Quota::open(shared.path(), startup).unwrap();
        assert!(quota.record.is_none() && quota.created_empty());
        assert!(!shared.path().join(RECORD).exists());
        assert!(shared.path().join(ORIGIN).exists());
    }

    fn quota_put(n: u64) -> RaftCommand {
        RaftCommand::Etcd {
            id: format!("put-{n}"),
            origin: 1,
            etcd: RequestOp {
                request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                    crate::proto::etcdserverpb::PutRequest {
                        key: format!("key-{n}").into_bytes(),
                        value: vec![b'v'; 32],
                        ..Default::default()
                    },
                )),
            },
        }
    }

    /// Append committed entries `first..` with `commands` to the Raft log of `dir`.
    pub(in crate::node) fn append_committed(
        dir: &std::path::Path,
        first: u64,
        commands: &[RaftCommand],
    ) {
        let mut storage =
            RaftStorage::open(&dir.join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
        let entries: Vec<_> = (first..)
            .zip(commands)
            .map(|(index, command)| Entry {
                index,
                term: 1,
                data: command.encode().unwrap(),
                ..Default::default()
            })
            .collect();
        let commit = first + commands.len() as u64 - 1;
        storage
            .persist(
                &entries,
                Some(&HardState {
                    term: 1,
                    vote: 1,
                    commit,
                }),
                None,
            )
            .unwrap();
    }

    /// Apply every committed entry of a follower node.
    pub(in crate::node) async fn apply_committed(node: &mut Node) {
        for _ in 0..10 {
            node.drive_ready().await.unwrap();
            node.drain_log().await.unwrap();
            node.drain_apply().await.unwrap();
        }
        assert_eq!(node.state.applied, node.raw.store().commit_index());
    }

    /// A directory of an earlier binary has no cluster quota. After an upgrade, a member replays its
    /// unflushed tail under its flag, as the earlier binary applied it.
    #[tokio::test]
    async fn an_upgraded_member_replays_its_unflushed_tail_under_its_flag() {
        let commands: Vec<_> = (1..=40).map(quota_put).collect();
        let mut results = vec![];
        for flushed in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            append_committed(dir.path(), 1, &commands);
            if flushed {
                // The earlier binary applied and flushed every entry under its flag. It stored
                // its bootstrap membership with its first batch.
                let mut state = StateMachine::open(&dir.path().join("objects"))
                    .await
                    .unwrap();
                state.quota = 3000;
                let peers = options(dir.path()).peers;
                state.members = std::sync::Arc::new(crate::cluster::Store::initial(&peers));
                state.members_unsaved = true;
                for (index, command) in (1..).zip(&commands) {
                    let RaftCommand::Etcd { etcd, origin, .. } = command else {
                        unreachable!()
                    };
                    state.apply_etcd_quota(index, etcd, *origin).await.unwrap();
                }
                state.snapshot_data().await.unwrap();
                state.close().await.unwrap();
            }
            let mut config = options(dir.path());
            config.quota_bytes = 3000;
            let (events, _) = mpsc::channel(256);
            let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
                .await
                .unwrap();
            assert_eq!(node.state.applied, if flushed { 40 } else { 0 });
            apply_committed(&mut node).await;
            assert_eq!(node.state.cluster_quota, None);
            let state = node.worker.state();
            results.push((node.state.revision, state.alarms.len(), state.quota));
        }
        assert_eq!(results[0], results[1]);
        let (revision, alarms, quota) = results[0];
        assert!(revision > 1 && revision < 41, "revision {revision}");
        assert_eq!((alarms, quota), (1, 3000));
    }

    /// A restored backup keeps its cluster quota. The flag of the new member does not replace it,
    /// and the entries above the restored index apply under it.
    #[tokio::test]
    async fn a_restored_cluster_quota_wins_over_the_flag() {
        let dir = tempfile::tempdir().unwrap();
        let mut source = StateMachine::open(&dir.path().join("source"))
            .await
            .unwrap();
        source.set_quota_flag(1 << 20);
        source
            .apply_set_quota(1, row(3000, 5, 2), false)
            .await
            .unwrap();
        let image = source.snapshot_data().await.unwrap();
        let mut backup = image.clone();
        use sha2::Digest as _;
        backup.extend_from_slice(&sha2::Sha256::digest(&image));
        let path = dir.path().join("backup");
        std::fs::write(&path, backup).unwrap();
        let target = dir.path().join("member");
        let config = options(&target);
        let members = crate::cluster::Store::initial(&config.peers);
        StateMachine::restore_backup(&path, &target, 1, "worker-test", &members).unwrap();
        // The restored log starts at index 1. Later entries follow it.
        let commands: Vec<_> = (1..=40).map(quota_put).collect();
        append_committed(&target, 2, &commands);
        let mut config = options(&target);
        config.quota_bytes = 1 << 20;
        let (events, _) = mpsc::channel(256);
        let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap();
        apply_committed(&mut node).await;
        assert_eq!(node.state.cluster_quota, Some(row(3000, 5, 2)));
        assert!(node.state.quota_mismatch);
        assert_eq!(node.status_quota(), 3000);
        assert_eq!(node.quota_status()["flag_bytes"], 1 << 20);
        // The flag would accept every Put. The restored quota rejects the later ones.
        assert!(node.state.revision > 1 && node.state.revision < 41);
        assert_eq!(node.worker.state().alarms.len(), 1);
        assert_eq!(node.worker.state().quota, 3000);
        source.close().await.unwrap();
    }

    /// A new leader proposes the cluster quota right after its election. A client write that
    /// arrives next follows it in the log.
    #[tokio::test]
    async fn a_new_leader_proposes_the_cluster_quota_before_client_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = options(dir.path());
        config.quota_bytes = -1;
        let (events, _) = mpsc::channel(256);
        let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap();
        node.raw.campaign().unwrap();
        for kind in [
            MessageType::MsgRequestPreVoteResponse,
            MessageType::MsgRequestVoteResponse,
        ] {
            let mut vote = Message::default();
            vote.set_msg_type(kind);
            vote.from = 2;
            vote.to = 1;
            vote.term =
                node.raw.raft.term + u64::from(kind == MessageType::MsgRequestPreVoteResponse);
            let (tx, _reply) = oneshot::channel();
            node.handle(Event::Step(vote, tx), &mut vec![])
                .await
                .unwrap();
        }
        assert_eq!(node.raw.raft.state, StateRole::Leader);
        let (tx, _reply) = oneshot::channel();
        node.propose(quota_put(1), ClientResponse::Kv(tx)).unwrap();
        node.drive_ready().await.unwrap();
        let last = node.raw.raft.raft_log.last_index();
        let entries = node
            .raw
            .raft
            .raft_log
            .entries(1, u64::MAX, raft::GetEntriesContext::empty(false))
            .unwrap();
        let commands: Vec<_> = entries
            .iter()
            .filter(|entry| !entry.data.is_empty())
            .map(|entry| RaftCommand::decode(&entry.data).unwrap())
            .collect();
        // The empty entry, `SeedMembers`, `SetQuota`, and the Put.
        assert_eq!(last, 4);
        assert!(matches!(commands[0], RaftCommand::SeedMembers(_)));
        assert!(matches!(
            commands[1],
            RaftCommand::SetQuota {
                bytes: -1,
                seed: true,
                ..
            }
        ));
        assert!(matches!(commands[2], RaftCommand::Etcd { .. }));
        // One proposal of a term is enough until it applies.
        node.propose_quota().unwrap();
        assert_eq!(node.raw.raft.raft_log.last_index(), 4);
    }

    async fn open_node(dir: &Path, flag: i64, reassert: bool) -> Node {
        let mut config = options(dir);
        config.quota_bytes = flag;
        config.quota_reassert = reassert;
        let (events, _) = mpsc::channel(256);
        Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap()
    }

    /// The request that member 1 would propose when it leads.
    fn leader_request(node: &Node) -> Option<(ClusterQuota, bool)> {
        proposal(
            node.state.cluster_quota,
            node.quota.flag,
            node.quota.pending(),
            1,
        )
    }

    /// A follower requested 200 and then applies 100, 200, and 300 in one batch. The newer 300
    /// replaces its request, so it does not propose 200 when it leads.
    #[tokio::test]
    async fn a_batched_newer_quota_replaces_a_pending_request() {
        let dir = tempfile::tempdir().unwrap();
        Quota::open(dir.path(), startup(100, 10, false, false)).unwrap();
        let pending = Quota::open(dir.path(), startup(200, 20, false, false)).unwrap();
        assert!(pending.pending().is_some());
        let entries = [
            set_quota(100, 0, 2),
            set_quota(200, 20, 1),
            set_quota(300, 30, 2),
        ];
        append_committed(dir.path(), 1, &entries);
        let mut node = open_node(dir.path(), 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(node.state.cluster_quota, Some(row(300, 30, 2)));
        assert_eq!(node.quota.pending(), None);
        assert_eq!(leader_request(&node), None);
    }

    /// The cluster took 300 while a member was offline. The member then restarts with 200. Its
    /// request is newer, so it stays pending through the replay of 300.
    #[tokio::test]
    async fn a_newer_request_survives_the_replay_of_an_older_quota() {
        let dir = tempfile::tempdir().unwrap();
        Quota::open(dir.path(), startup(100, 10, false, false)).unwrap();
        append_committed(
            dir.path(),
            1,
            &[set_quota(100, 0, 2), set_quota(300, 30, 2)],
        );
        Quota::open(dir.path(), startup(200, 40, false, false)).unwrap();
        let mut node = open_node(dir.path(), 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(node.state.cluster_quota, Some(row(300, 30, 2)));
        assert_eq!(leader_request(&node), Some((row(200, 40, 1), false)));
    }

    /// Restore a native backup of a state with the cluster quota `quota` into `target`.
    pub(in crate::node) async fn restore_with(dir: &Path, target: &Path, quota: ClusterQuota) {
        let mut source = StateMachine::open(&dir.join("source")).await.unwrap();
        source.apply_set_quota(1, quota, false).await.unwrap();
        let image = source.snapshot_data().await.unwrap();
        source.close().await.unwrap();
        let mut backup = image.clone();
        use sha2::Digest as _;
        backup.extend_from_slice(&sha2::Sha256::digest(&image));
        let path = dir.join("backup");
        std::fs::write(&path, backup).unwrap();
        let members = crate::cluster::Store::initial(&options(target).peers);
        StateMachine::restore_backup(&path, target, 1, "worker-test", &members).unwrap();
    }

    /// A snapshot install moves a member past its pending request. A newer quota in the snapshot
    /// replaces the request. An older one does not.
    #[tokio::test]
    async fn a_snapshot_install_resolves_a_pending_request_by_time() {
        for (installed, pending) in [(row(300, 30, 2), false), (row(300, 10, 2), true)] {
            let dir = tempfile::tempdir().unwrap();
            let target = dir.path().join("member");
            restore_with(dir.path(), &target, installed).await;
            Quota::open(&target, startup(100, 5, false, false)).unwrap();
            Quota::open(&target, startup(200, 20, false, false)).unwrap();
            let mut node = open_node(&target, 200, false).await;
            apply_committed(&mut node).await;
            assert_eq!(node.state.cluster_quota, Some(installed));
            assert_eq!(node.quota.pending().is_some(), pending, "{installed:?}");
            assert_eq!(leader_request(&node).is_some(), pending, "{installed:?}");
        }
    }

    /// An operator retries a request with the same flag and data directory. Only
    /// `--quota-reassert` makes a new request.
    #[tokio::test]
    async fn quota_reassert_requests_the_same_flag_again() {
        let dir = tempfile::tempdir().unwrap();
        Quota::open(dir.path(), startup(200, 10, false, false)).unwrap();
        append_committed(
            dir.path(),
            1,
            &[set_quota(200, 0, 1), set_quota(300, 50, 2)],
        );
        let mut node = open_node(dir.path(), 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(leader_request(&node), None);
        drop(node);
        let mut node = open_node(dir.path(), 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(leader_request(&node), None);
        drop(node);
        let mut node = open_node(dir.path(), 200, true).await;
        apply_committed(&mut node).await;
        let (request, seed) = leader_request(&node).unwrap();
        assert_eq!((request.bytes, seed), (200, false));
        assert!(request.key() > row(300, 50, 2).key());
    }

    fn txn_put(n: u64) -> RaftCommand {
        let RaftCommand::Etcd { etcd, .. } = quota_put(n) else {
            unreachable!()
        };
        RaftCommand::Etcd {
            id: format!("txn-{n}"),
            origin: 1,
            etcd: RequestOp {
                request: Some(crate::proto::etcdserverpb::request_op::Request::RequestTxn(
                    crate::proto::etcdserverpb::TxnRequest {
                        success: vec![etcd],
                        ..Default::default()
                    },
                )),
            },
        }
    }

    fn lease_grant(n: u64) -> RaftCommand {
        RaftCommand::Lease {
            id: format!("grant-{n}"),
            origin: 1,
            lease: LeaseCommand::Grant {
                id: n as i64,
                ttl: 60,
            },
        }
    }

    /// Open a member that starts empty and then holds `entries` of an earlier binary.
    async fn waiting_member(dir: &Path, entries: &[RaftCommand], pending: usize) -> Node {
        drop(open_node(dir, 3000, false).await);
        append_committed(dir, 1, entries);
        let mut config = options(dir);
        config.quota_bytes = 3000;
        config.log_memory.pending_bytes = pending;
        let (events, _) = mpsc::channel(256);
        Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap()
    }

    async fn drive(node: &mut Node) {
        for _ in 0..5 {
            node.drive_ready().await.unwrap();
            node.drain_log().await.unwrap();
            node.drain_apply().await.unwrap();
        }
    }

    /// A snapshot of another member that applied `entries` under the cluster decisions.
    /// Return it and its revision.
    async fn covering_snapshot(
        dir: &Path,
        entries: &[RaftCommand],
    ) -> (raft::eraftpb::Snapshot, i64) {
        let mut source = StateMachine::open(&dir.join("source")).await.unwrap();
        source.set_quota_flag(1 << 20);
        let members = crate::cluster::Store::initial(&options(dir).peers);
        source.members = std::sync::Arc::new(members.clone());
        source.members_unsaved = true;
        let log: Vec<_> = (1..)
            .zip(entries)
            .map(|(index, command)| Entry {
                index,
                term: 1,
                data: command.encode().unwrap(),
                ..Default::default()
            })
            .collect();
        let header = super::super::apply::Header {
            cluster: 0,
            member: 2,
            term: 1,
        };
        super::super::apply::apply_entries(&mut source, log, header)
            .await
            .unwrap();
        let mut snapshot = raft::eraftpb::Snapshot {
            data: source.snapshot_data().await.unwrap(),
            ..Default::default()
        };
        snapshot.mut_metadata().index = entries.len() as u64;
        snapshot.mut_metadata().term = 1;
        snapshot.mut_metadata().set_conf_state(members.conf());
        let revision = source.revision;
        source.close().await.unwrap();
        (snapshot, revision)
    }

    fn message(kind: MessageType, from: u64, term: u64) -> Message {
        let mut message = Message::default();
        message.set_msg_type(kind);
        message.from = from;
        message.to = 1;
        message.term = term;
        message
    }

    /// Member 2 leads. Member 1 asks it for a snapshot and installs `snapshot`.
    async fn install_requested(node: &mut Node, snapshot: raft::eraftpb::Snapshot) {
        let index = snapshot.get_metadata().index;
        let term = node.raw.raft.term.max(1);
        node.step_peer(message(MessageType::MsgHeartbeat, 2, term))
            .unwrap();
        node.tick_raft().unwrap();
        drive(node).await;
        assert_eq!(node.raw.raft.pending_request_snapshot, index);
        let mut install = message(MessageType::MsgSnapshot, 2, term);
        install.set_snapshot(snapshot);
        node.step_peer(install).unwrap();
        apply_committed(node).await;
        assert_eq!(node.state.applied, index);
        assert_eq!(node.quota.blocked(), None);
    }

    /// The first start with this binary records whether the Raft log was empty and nothing had
    /// applied. An upgraded member that never flushed still has its log, so it is not new.
    #[tokio::test]
    async fn only_a_start_with_an_empty_log_and_state_creates_the_state_empty() {
        let upgraded = tempfile::tempdir().unwrap();
        append_committed(upgraded.path(), 1, &[publish(), quota_put(1)]);
        let node = open_node(upgraded.path(), 3000, false).await;
        assert_eq!(node.state.applied, 0);
        assert!(!node.quota.created_empty());
        drop(node);
        assert!(!open_node(upgraded.path(), 3000, false)
            .await
            .quota
            .created_empty());
        let fresh = tempfile::tempdir().unwrap();
        drop(open_node(fresh.path(), 3000, false).await);
        // Later entries do not change the record of the first start.
        append_committed(fresh.path(), 1, &[publish()]);
        assert!(open_node(fresh.path(), 3000, false)
            .await
            .quota
            .created_empty());
    }

    /// The flag record that #116 wrote when it first opened a member.
    fn write_116_record(dir: &Path, created_empty: bool) {
        let record =
            format!(r#"{{"acknowledged":3000,"pending":null,"created_empty":{created_empty}}}"#);
        std::fs::write(dir.join(RECORD), record).unwrap();
    }

    /// #116 created this member empty, and it waits to replace the Puts of an earlier binary.
    /// After the upgrade, the quota guard keeps the member waiting: the origin record takes the
    /// mark of #116, not the log that the member holds now.
    #[tokio::test]
    async fn an_upgrade_from_116_keeps_the_quota_guard_of_a_waiting_member() {
        let dir = tempfile::tempdir().unwrap();
        write_116_record(dir.path(), true);
        let mut commands: Vec<_> = (0..40).map(quota_put).collect();
        commands.push(set_quota(-1, 0, 2));
        append_committed(dir.path(), 1, &commands);
        let mut node = open_node(dir.path(), 3000, false).await;
        drive(&mut node).await;
        assert_eq!(
            (
                node.state.applied,
                node.quota.blocked(),
                node.state.revision
            ),
            (0, Some(1), 1),
        );
        assert!(!node.quota.origin().members);
    }

    /// #116 has no seeds. A #116 member with history before the first start of this binary
    /// applied its membership entries without one, so the membership guard does not apply. It
    /// replays its unflushed log as #116 did. A #116 member with an empty log and state has no
    /// such history, so the membership guard applies to it.
    #[tokio::test]
    async fn an_upgrade_from_116_guards_the_membership_only_without_history() {
        let dir = tempfile::tempdir().unwrap();
        write_116_record(dir.path(), true);
        let commands = [set_quota(1 << 20, 0, 2), publish(), quota_put(1)];
        append_committed(dir.path(), 1, &commands);
        let mut node = open_node(dir.path(), 3000, false).await;
        assert_eq!(
            node.quota.origin(),
            Guards {
                quota: true,
                members: false
            }
        );
        apply_committed(&mut node).await;
        assert_eq!((node.quota.blocked(), node.state.revision), (None, 2));
        assert!(node.state.members_row.is_some());

        let dir = tempfile::tempdir().unwrap();
        write_116_record(dir.path(), true);
        drop(open_node(dir.path(), 3000, false).await);
        append_committed(dir.path(), 1, &[publish()]);
        let mut node = open_node(dir.path(), 3000, false).await;
        assert_eq!(
            node.quota.origin(),
            Guards {
                quota: true,
                members: true
            }
        );
        drive(&mut node).await;
        assert_eq!((node.state.applied, node.quota.blocked()), (0, Some(1)));
    }

    /// An earlier binary stored the flags of the publishing member before this entry.
    fn publish() -> RaftCommand {
        RaftCommand::Cluster {
            id: "publish-2".into(),
            request: crate::cluster::Command::Publish {
                id: 2,
                client_urls: vec!["http://127.0.0.1:2382".into()],
            },
            credentials: Default::default(),
        }
    }

    /// Entries of an earlier binary: `first`, 39 Puts, and the seed at index 41.
    fn earlier_entries(first: RaftCommand) -> Vec<RaftCommand> {
        let mut entries = vec![first];
        entries.extend((2..=40).map(quota_put));
        entries.push(set_quota(1 << 20, 0, 2));
        entries
    }

    /// A state that this binary created empty must not apply the entries of an earlier binary
    /// under its own flag: a Put, a transaction with a Put, or a lease grant. Nor may it store
    /// its own membership before a cluster command. It knows the waiting entry at open, waits
    /// for a snapshot, and catches up after the install.
    #[tokio::test]
    async fn a_new_member_waits_for_a_snapshot_before_entries_of_an_earlier_binary() {
        for first in [quota_put(1), txn_put(1), lease_grant(1), publish()] {
            let dir = tempfile::tempdir().unwrap();
            let entries = earlier_entries(first);
            let mut node = waiting_member(dir.path(), &entries, 32 << 20).await;
            assert!(node.quota.created_empty());
            assert_eq!(node.quota.blocked(), Some(1));
            drive(&mut node).await;
            assert_eq!(node.state.applied, 0);
            let (snapshot, revision) = covering_snapshot(dir.path(), &entries).await;
            install_requested(&mut node, snapshot).await;
            assert_eq!(node.state.cluster_quota, Some(row(1 << 20, 0, 2)));
            assert_eq!(node.state.revision, revision);
            assert!(node.worker.state().alarms.is_empty());
        }
    }

    /// A waiting member knows its waiting entry before apply reaches it. It does not start an
    /// election while it knows a leader, ignores a leadership transfer, and still votes.
    #[tokio::test]
    async fn a_waiting_member_does_not_campaign_but_votes() {
        let dir = tempfile::tempdir().unwrap();
        let entries = earlier_entries(quota_put(1));
        let mut node = waiting_member(dir.path(), &entries, 32 << 20).await;
        assert_eq!(node.quota.blocked(), Some(1));
        for _ in 0..60 {
            node.step_peer(message(MessageType::MsgHeartbeat, 2, 1))
                .unwrap();
            node.tick_raft().unwrap();
        }
        assert_eq!(node.raw.raft.state, StateRole::Follower);
        assert_eq!(node.raw.raft.term, 1);
        node.step_peer(message(MessageType::MsgTimeoutNow, 2, 1))
            .unwrap();
        assert_eq!(node.raw.raft.state, StateRole::Follower);
        assert_eq!(node.raw.raft.term, 1);
        // The leader stops. After the lease, but before three election timeouts, the member
        // still does not campaign, and it votes for another candidate.
        for _ in 0..15 {
            node.tick_raft().unwrap();
        }
        assert_eq!(node.raw.raft.state, StateRole::Follower);
        let mut vote = message(MessageType::MsgRequestVote, 3, 2);
        vote.index = 41;
        vote.log_term = 1;
        node.step_peer(vote).unwrap();
        assert_eq!(node.raw.raft.vote, 3);
    }

    /// A member of a new empty cluster has no waiting entry, so it campaigns as usual.
    #[tokio::test]
    async fn a_member_of_a_new_cluster_campaigns() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = open_node(dir.path(), 3000, false).await;
        assert!(node.quota.created_empty());
        assert_eq!(node.quota.blocked(), None);
        for _ in 0..30 {
            node.tick_raft().unwrap();
        }
        assert_ne!(node.raw.raft.state, StateRole::Follower);
    }

    /// A waiting member that still wins an election hands leadership to another voter.
    #[tokio::test]
    async fn a_waiting_member_that_wins_hands_over_leadership() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = waiting_member(
            dir.path(),
            &[quota_put(1), set_quota(10000, 0, 2)],
            32 << 20,
        )
        .await;
        drive(&mut node).await;
        assert_eq!(node.quota.blocked(), Some(1));
        node.raw.campaign().unwrap();
        for kind in [
            MessageType::MsgRequestPreVoteResponse,
            MessageType::MsgRequestVoteResponse,
        ] {
            let term =
                node.raw.raft.term + u64::from(kind == MessageType::MsgRequestPreVoteResponse);
            let (tx, _) = oneshot::channel();
            node.handle(Event::Step(message(kind, 2, term), tx), &mut vec![])
                .await
                .unwrap();
        }
        drive(&mut node).await;
        assert_eq!(node.raw.raft.state, StateRole::Leader);
        assert!(matches!(node.raw.raft.lead_transferee, Some(2 | 3)));
    }

    /// A member that waits on an earlier binary's membership entry and still wins hands
    /// leadership over. It appends neither `SeedMembers` nor `SetQuota` in its term.
    #[tokio::test]
    async fn a_member_waiting_on_a_membership_entry_hands_over_without_seeds() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = waiting_member(dir.path(), &[publish()], 32 << 20).await;
        drive(&mut node).await;
        assert_eq!(node.quota.blocked(), Some(1));
        node.raw.campaign().unwrap();
        for kind in [
            MessageType::MsgRequestPreVoteResponse,
            MessageType::MsgRequestVoteResponse,
        ] {
            let term =
                node.raw.raft.term + u64::from(kind == MessageType::MsgRequestPreVoteResponse);
            let (tx, _) = oneshot::channel();
            node.handle(Event::Step(message(kind, 2, term), tx), &mut vec![])
                .await
                .unwrap();
        }
        drive(&mut node).await;
        node.tick().unwrap();
        assert_eq!(node.raw.raft.state, StateRole::Leader);
        assert!(matches!(node.raw.raft.lead_transferee, Some(2 | 3)));
        let entries = node
            .raw
            .raft
            .raft_log
            .entries(2, u64::MAX, raft::GetEntriesContext::empty(false))
            .unwrap();
        assert!(!entries.is_empty());
        assert!(entries.iter().all(|entry| entry.data.is_empty()));
    }

    /// The row keeps the index of a first cluster quota after an earlier binary. A restart
    /// before the Raft checkpoint still truncates the log through it.
    #[tokio::test]
    async fn a_restart_retries_the_checkpoint_through_the_first_quota() {
        let dir = tempfile::tempdir().unwrap();
        let commands = [quota_put(1), set_quota(10000, 0, 2)];
        append_committed(dir.path(), 1, &commands);
        let mut state = StateMachine::open(&dir.path().join("objects"))
            .await
            .unwrap();
        let log = (1..)
            .zip(&commands)
            .map(|(index, command)| Entry {
                index,
                term: 1,
                data: command.encode().unwrap(),
                ..Default::default()
            })
            .collect();
        let header = super::super::apply::Header {
            cluster: 0,
            member: 1,
            term: 1,
        };
        super::super::apply::apply_entries(&mut state, log, header)
            .await
            .unwrap();
        assert_eq!(state.quota_boundary, 2);
        state.snapshot_data().await.unwrap();
        state.close().await.unwrap();
        let mut node = open_node(dir.path(), 10000, false).await;
        apply_committed(&mut node).await;
        assert_eq!(node.state.applied, 2);
        assert!(node.raw.store().snapshot_index() >= 2);
    }

    /// Puts of `size` bytes, then the seed.
    fn large_entries(count: u64, size: usize) -> Vec<RaftCommand> {
        let mut entries: Vec<_> = (1..=count)
            .map(|n| {
                let mut command = quota_put(n);
                if let RaftCommand::Etcd { etcd, .. } = &mut command {
                    if let Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(put)) =
                        &mut etcd.request
                    {
                        put.value = vec![1; size];
                    }
                }
                command
            })
            .collect();
        entries.push(set_quota(1 << 20, 0, 2));
        entries
    }

    /// A waiting member with a backlog above its pending limit starts, and does not hold the
    /// backlog.
    #[tokio::test]
    async fn a_waiting_member_starts_with_a_large_backlog() {
        let dir = tempfile::tempdir().unwrap();
        let entries = large_entries(36, 1 << 20);
        let node = waiting_member(dir.path(), &entries, 32 << 20).await;
        assert_eq!(node.quota.blocked(), Some(1));
        assert_eq!(node.state.applied, 0);
        assert_eq!(node.raw.raft.raft_log.applied, 37);
    }

    /// A restart lowers the pending limit below the backlog of a waiting member. The member
    /// installs the snapshot within the limit and catches up.
    #[tokio::test]
    async fn a_waiting_member_recovers_within_a_lower_pending_limit() {
        let dir = tempfile::tempdir().unwrap();
        let entries = large_entries(12, 1 << 20);
        let mut node = waiting_member(dir.path(), &entries, 9 << 20).await;
        assert_eq!(node.raw.raft.raft_log.applied, 13);
        drive(&mut node).await;
        assert_eq!(node.state.applied, 0);
        let (snapshot, revision) = covering_snapshot(dir.path(), &entries).await;
        install_requested(&mut node, snapshot).await;
        assert_eq!(node.state.revision, revision);
        assert_eq!(node.state.cluster_quota, Some(row(1 << 20, 0, 2)));
        assert!(node.raw.store().stats()["pending_bytes"].as_u64().unwrap() <= 9 << 20);
    }

    /// The durable row holds 200 at time 10, the committed tail 300 at time 30, and the member a
    /// request for 200 at time 40. The request is newer than both, so it survives the replay,
    /// and its proposal sets 200 at time 40.
    #[tokio::test]
    async fn a_newer_request_with_an_older_value_survives_the_replay() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("member");
        restore_with(temp.path(), &dir, row(200, 10, 2)).await;
        append_committed(&dir, 2, &[set_quota(300, 30, 2)]);
        Quota::open(&dir, startup(100, 10, false, false)).unwrap();
        Quota::open(&dir, startup(200, 40, false, false)).unwrap();
        let mut node = open_node(&dir, 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(node.state.cluster_quota, Some(row(300, 30, 2)));
        let (request, seed) = leader_request(&node).unwrap();
        assert_eq!((request, seed), (row(200, 40, 1), false));
        drop(node);
        append_committed(&dir, 3, &[set_quota(200, 40, 1)]);
        let mut node = open_node(&dir, 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(node.state.cluster_quota, Some(row(200, 40, 1)));
        assert_eq!(node.quota.pending(), None);
        assert_eq!(leader_request(&node), None);
    }

    /// A newer request with the value of the row still moves the order of the row forward, once.
    #[tokio::test]
    async fn a_newer_request_with_the_same_value_advances_the_row_once() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("member");
        restore_with(temp.path(), &dir, row(200, 10, 2)).await;
        Quota::open(&dir, startup(100, 5, false, false)).unwrap();
        Quota::open(&dir, startup(200, 40, false, false)).unwrap();
        let mut node = open_node(&dir, 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(leader_request(&node), Some((row(200, 40, 1), false)));
        drop(node);
        append_committed(&dir, 2, &[set_quota(200, 40, 1)]);
        let mut node = open_node(&dir, 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(node.state.cluster_quota, Some(row(200, 40, 1)));
        assert_eq!(leader_request(&node), None);
    }

    /// A follower holds 40 committed Puts of an earlier binary and an uncommitted Put at 41. A
    /// new leader replaces only entry 41 with its seed. The guard at 1 stays.
    #[tokio::test]
    async fn a_replaced_suffix_after_the_guard_keeps_the_guard() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_node(dir.path(), 3000, false).await);
        let mut commands: Vec<_> = (1..=40).map(quota_put).collect();
        {
            let mut storage =
                RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3])
                    .unwrap();
            let mut log: Vec<_> = (1..)
                .zip(&commands)
                .map(|(index, command)| Entry {
                    index,
                    term: 1,
                    data: command.encode().unwrap(),
                    ..Default::default()
                })
                .collect();
            log.push(Entry {
                index: 41,
                term: 1,
                data: quota_put(41).encode().unwrap(),
                ..Default::default()
            });
            let state = HardState {
                term: 1,
                vote: 1,
                commit: 40,
            };
            storage.persist(&log, Some(&state), None).unwrap();
        }
        let mut node = open_node(dir.path(), 3000, false).await;
        assert_eq!(node.quota.blocked(), Some(1));
        let seed = set_quota(1 << 20, 0, 2);
        let mut append = message(MessageType::MsgAppend, 2, 2);
        append.index = 40;
        append.log_term = 1;
        append.commit = 41;
        append.entries.push(Entry {
            index: 41,
            term: 2,
            data: seed.encode().unwrap(),
            ..Default::default()
        });
        node.step_peer(append).unwrap();
        drive(&mut node).await;
        commands.push(seed);
        let (snapshot, revision) = covering_snapshot(dir.path(), &commands).await;
        assert_eq!(node.state.applied, 0);
        assert_eq!(node.quota.blocked(), Some(1));
        // The snapshot passes the guard.
        install_requested(&mut node, snapshot).await;
        assert_eq!(node.state.revision, revision);
    }

    /// The membership row of a seed with the default peers.
    fn seed_members() -> RaftCommand {
        let members = crate::cluster::Store::initial(&options(Path::new("")).peers);
        RaftCommand::SeedMembers(crate::cluster::Seed {
            id: "seed".into(),
            origin: 2,
            members: serde_json::to_string(&members).unwrap(),
            digest: None,
            applied: 0,
        })
    }

    /// Persist `committed` from index 1, and `uncommitted` after it, on a member that started
    /// empty.
    fn persist_log(dir: &Path, committed: &[Entry], uncommitted: &[Entry]) {
        let mut storage =
            RaftStorage::open(&dir.join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
        let log: Vec<_> = committed.iter().chain(uncommitted).cloned().collect();
        let state = HardState {
            term: 1,
            vote: 1,
            commit: committed.len() as u64,
        };
        storage.persist(&log, Some(&state), None).unwrap();
    }

    fn entry(index: u64, term: u64, command: Option<&RaftCommand>) -> Entry {
        Entry {
            index,
            term,
            data: command.map_or(vec![], |command| command.encode().unwrap()),
            ..Default::default()
        }
    }

    /// The rules of the quota guard also hold for an index of the membership guard. The log of
    /// an earlier binary holds only membership entries. A new leader replaces only the uncommitted
    /// entry 4 with its seed, and the guard at 1 stays until a snapshot passes it. A replacement
    /// at the guard computes the gate again.
    #[tokio::test]
    async fn a_membership_guard_keeps_the_rules_of_the_quota_guard() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_node(dir.path(), 3000, false).await);
        let committed: Vec<_> = (1..=3)
            .map(|index| entry(index, 1, Some(&publish())))
            .collect();
        persist_log(dir.path(), &committed, &[entry(4, 1, Some(&publish()))]);
        let mut node = open_node(dir.path(), 3000, false).await;
        assert_eq!(node.quota.blocked(), Some(1));
        let mut append = message(MessageType::MsgAppend, 2, 2);
        append.index = 3;
        append.log_term = 1;
        append.commit = 4;
        append.entries.push(entry(4, 2, Some(&seed_members())));
        node.step_peer(append).unwrap();
        drive(&mut node).await;
        assert_eq!((node.state.applied, node.quota.blocked()), (0, Some(1)));
        let commands = [publish(), publish(), publish(), seed_members()];
        let (snapshot, revision) = covering_snapshot(dir.path(), &commands).await;
        install_requested(&mut node, snapshot).await;
        assert_eq!(node.state.revision, revision);
        assert!(node.state.members_row.is_some());

        let dir = tempfile::tempdir().unwrap();
        drop(open_node(dir.path(), 3000, false).await);
        let committed: Vec<_> = (1..=3).map(|index| entry(index, 1, None)).collect();
        persist_log(dir.path(), &committed, &[entry(4, 1, Some(&publish()))]);
        let mut node = open_node(dir.path(), 3000, false).await;
        assert_eq!(node.quota.blocked(), Some(4));
        let mut append = message(MessageType::MsgAppend, 2, 2);
        append.index = 3;
        append.log_term = 1;
        append.commit = 4;
        append.entries.push(entry(4, 2, Some(&seed_members())));
        node.step_peer(append).unwrap();
        drive(&mut node).await;
        assert_eq!((node.state.applied, node.quota.blocked()), (4, None));
        assert!(node.state.members_row.is_some());
    }

    /// A member that waits on a membership entry before a backlog above its pending limit
    /// starts as a learner, like a member that waits on a quota-checked entry.
    #[tokio::test]
    async fn a_member_waiting_on_a_membership_entry_skips_its_backlog_as_a_learner() {
        let dir = tempfile::tempdir().unwrap();
        let mut entries = vec![publish()];
        entries.extend(large_entries(12, 1 << 20));
        let node = waiting_member(dir.path(), &entries, 9 << 20).await;
        assert_eq!(node.quota.blocked(), Some(1));
        assert_eq!(node.state.applied, 0);
        assert_eq!(node.raw.raft.raft_log.applied, 14);
        assert!(!node.raw.raft.prs().conf().voters().contains(1));
    }

    /// A replacement at or before the guard computes the gate again.
    #[tokio::test]
    async fn a_replaced_guard_entry_is_scanned_again() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_node(dir.path(), 3000, false).await);
        {
            let mut storage =
                RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3])
                    .unwrap();
            let mut log: Vec<_> = (1..=3)
                .map(|index| Entry {
                    index,
                    term: 1,
                    ..Default::default()
                })
                .collect();
            log.push(Entry {
                index: 4,
                term: 1,
                data: quota_put(4).encode().unwrap(),
                ..Default::default()
            });
            let state = HardState {
                term: 1,
                vote: 1,
                commit: 3,
            };
            storage.persist(&log, Some(&state), None).unwrap();
        }
        let mut node = open_node(dir.path(), 3000, false).await;
        assert_eq!(node.quota.blocked(), Some(4));
        let mut append = message(MessageType::MsgAppend, 2, 2);
        append.index = 3;
        append.log_term = 1;
        append.commit = 4;
        append.entries.push(Entry {
            index: 4,
            term: 2,
            data: set_quota(1 << 20, 0, 2).encode().unwrap(),
            ..Default::default()
        });
        node.step_peer(append).unwrap();
        drive(&mut node).await;
        assert_eq!(node.quota.blocked(), None);
        assert_eq!(node.state.applied, 4);
        assert_eq!(node.state.cluster_quota, Some(row(1 << 20, 0, 2)));
    }

    /// The leader of a waiting member goes silent. Raft keeps its ID, but the member campaigns
    /// after three election timeouts.
    #[tokio::test]
    async fn a_waiting_member_campaigns_after_its_leader_goes_silent() {
        let dir = tempfile::tempdir().unwrap();
        let entries = earlier_entries(quota_put(1));
        let mut node = waiting_member(dir.path(), &entries, 32 << 20).await;
        node.step_peer(message(MessageType::MsgHeartbeat, 2, 1))
            .unwrap();
        for _ in 0..200 {
            node.tick_raft().unwrap();
        }
        assert_ne!(node.raw.raft.state, StateRole::Follower);
    }

    /// A configuration change that adds voter `id`, as the log stores it.
    fn add_voter(id: u64) -> raft::eraftpb::ConfChange {
        raft::eraftpb::ConfChange {
            change_type: raft::eraftpb::ConfChangeType::AddNode as i32,
            node_id: id,
            ..Default::default()
        }
    }

    /// A member that skips a backlog above its pending limit starts as a learner in memory. The
    /// backlog adds voters 4 and 5, which its start does not see. It never campaigns, ignores a
    /// transfer, still votes, and is a voter again after the snapshot. Nothing stores the
    /// learner configuration.
    #[tokio::test]
    async fn a_member_that_skips_its_backlog_cannot_campaign_until_the_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_node(dir.path(), 3000, false).await);
        let commands = large_entries(12, 1 << 20);
        {
            let mut storage =
                RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3])
                    .unwrap();
            let mut log: Vec<_> = (1..)
                .zip(&commands)
                .map(|(index, command)| Entry {
                    index,
                    term: 1,
                    data: command.encode().unwrap(),
                    ..Default::default()
                })
                .collect();
            for (index, id) in [(14, 4), (15, 5)] {
                log.push(Entry {
                    index,
                    term: 1,
                    entry_type: EntryType::EntryConfChange as i32,
                    data: add_voter(id).encode_to_vec(),
                    ..Default::default()
                });
            }
            let state = HardState {
                term: 1,
                vote: 1,
                commit: 15,
            };
            storage.persist(&log, Some(&state), None).unwrap();
        }
        let mut config = options(dir.path());
        config.quota_bytes = 3000;
        config.log_memory.pending_bytes = 9 << 20;
        let (events, _) = mpsc::channel(256);
        let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap();
        assert_eq!(node.raw.raft.raft_log.applied, 15);
        assert!(!node.raw.raft.promotable());
        // No leader for many election timeouts: no campaign, so no win with two of three.
        for _ in 0..500 {
            node.tick_raft().unwrap();
        }
        assert_eq!(node.raw.raft.state, StateRole::Follower);
        node.raw
            .step(message(MessageType::MsgTimeoutNow, 2, 1))
            .unwrap();
        assert_eq!(node.raw.raft.state, StateRole::Follower);
        assert_eq!(node.raw.raft.term, 1);
        let mut vote = message(MessageType::MsgRequestVote, 3, 2);
        vote.index = 15;
        vote.log_term = 1;
        node.step_peer(vote).unwrap();
        assert_eq!(node.raw.raft.vote, 3);
        // The durable Raft state keeps the bootstrap voters.
        let stored = node
            .log_worker
            .storage()
            .initial_state()
            .unwrap()
            .conf_state;
        assert!(stored.voters.contains(&1));
        drop(node);
        assert!(
            RaftStorage::open(&dir.path().join("raft"), 1, "worker-test", vec![1, 2, 3])
                .unwrap()
                .initial_state()
                .unwrap()
                .conf_state
                .voters
                .contains(&1)
        );
    }

    /// After the snapshot install, a member that skipped its backlog is a voter again.
    #[tokio::test]
    async fn a_member_that_skipped_its_backlog_votes_again_after_the_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let entries = large_entries(12, 1 << 20);
        let mut node = waiting_member(dir.path(), &entries, 9 << 20).await;
        assert!(!node.raw.raft.promotable());
        let (snapshot, _) = covering_snapshot(dir.path(), &entries).await;
        install_requested(&mut node, snapshot).await;
        assert!(node.raw.raft.promotable());
        assert!(node.raw.raft.prs().conf().voters().contains(1));
    }
}
