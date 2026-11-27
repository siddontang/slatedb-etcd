//! The membership guard of a state created empty, and the quota fields of the status.
//!
//! A state that this binary created empty never applies an entry of an earlier binary before
//! the first `SeedMembers` entry; see `state::cluster`. The member finds such an entry in its
//! log at open and as entries arrive. Apply then stops before it, and the member asks the leader
//! for a snapshot. Until then it does not lead: it skips elections while its leader is live,
//! ignores leadership transfers, and hands over leadership if it wins. It still votes. A local
//! origin record in the data directory says whether the guard applies. See `Origin`.

use std::io::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::*;

/// The file of the local record of the start that created the data directory. Raft, snapshots,
/// and backups do not carry it.
const ORIGIN: &str = "state-origin.json";

/// The guards that apply to a data directory, by the start that created it. See `Guards`.
///
/// The first start of this binary writes the record. Without an earlier record, the guard
/// applies when that start found an empty Raft log and an empty state. A directory with history
/// at that start holds decisions of an earlier binary, which applied it without seeds. So the
/// membership guard does not apply there, and replay applies the entries as that binary did.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Origin {
    /// The state was created empty by this binary.
    members_created_empty: bool,
}

/// The options of `Quota::open`.
pub(super) struct Startup {
    /// The effective flag value. A non-positive value disables the quota.
    pub flag: i64,
    /// The Raft log and the state are empty.
    pub empty: bool,
}

/// The cluster state that a state created empty still lacks. While it lacks an input, an
/// entry of an earlier binary can need it. A `SeedMembers` entry gives the membership row. See
/// `state::cluster`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Guards {
    /// The state has no membership row.
    pub members: bool,
}

impl Guards {
    fn new(members_row: bool) -> Self {
        Self {
            members: !members_row,
        }
    }

    /// The guards of `state` that apply to a directory with origin `origin`.
    pub(super) fn of(state: &StateMachine, origin: Self) -> Self {
        Self::new(state.members_row.is_some()).and(origin)
    }

    pub(super) fn any(self) -> bool {
        self.members
    }

    /// The guards that both `self` and `other` still hold.
    fn and(self, other: Self) -> Self {
        Self {
            members: self.members && other.members,
        }
    }
}

/// The first entry that decides whether a state created empty can apply its log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Gate {
    /// The `SeedMembers` entry that gives the last missing input. The entries after it apply
    /// under cluster state.
    Passed(u64),
    /// An entry of an earlier binary that needs a missing input: another entry than a
    /// `SeedMembers` entry before any `SeedMembers` entry. Other members applied it under their
    /// own flags.
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
            RaftCommand::SeedMembers(_) => std::mem::take(&mut guards.members),
            _ => false,
        };
        if passed && !guards.any() {
            return Ok(Some(Gate::Passed(entry.index)));
        }
        if guards.members && crate::state::cluster::needs_members(&command) {
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

/// The quota flag of this member and the membership guard of its directory.
pub(super) struct Quota {
    flag: i64,
    /// The guards that apply to this directory. See `Origin`.
    origin: Guards,
    /// The first gate in the log above the applied index, while a state created empty has no
    /// membership row. A `Gate::Checked` entry waits for a snapshot.
    gate: Option<Gate>,
    /// The last log index that the gate scan covered.
    scanned: u64,
    /// The inputs that the state still lacks after `scanned`, when it is above the applied
    /// index.
    open: Guards,
    /// Ticks since the last message of the current leader. Raft keeps the ID of a lost leader
    /// while the timer cap of `tick_raft` holds, so the ID is not a liveness signal.
    silent: usize,
}

impl Quota {
    /// Load the origin record of `dir` for this start.
    pub(super) fn open(dir: &Path, startup: Startup) -> Result<Self> {
        Ok(Self {
            flag: startup.flag,
            origin: open_origin(dir, startup.empty)?,
            gate: None,
            scanned: 0,
            open: Guards { members: true },
            silent: 0,
        })
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

    /// The entry that waits for a snapshot, if any.
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
            let origin = Origin {
                members_created_empty: empty,
            };
            write_record(&path, &origin)?;
            origin
        }
    };
    Ok(Guards {
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

impl Node {
    /// Update the gate after apply.
    pub(super) fn observe_quota(&mut self) -> Result<()> {
        // A snapshot install moved the state past the waiting entry. Scan the rest again.
        if self
            .quota
            .blocked()
            .is_some_and(|index| self.state.applied >= index)
        {
            self.quota.set_gate(None, self.state.applied);
        }
        self.scan_quota_log(None)
    }

    /// The inputs that the applied state lacks, for the guards of this directory. See `Guards`.
    fn guards(&self) -> Guards {
        Guards::new(self.state.members_row.is_some()).and(self.quota.origin())
    }

    /// Return true while a guard of this directory applies: a state created empty lacks the
    /// membership row. Apply then runs one batch at a time and stops before a `Gate::Checked`
    /// entry.
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
                "entry needs the membership; apply waits for a snapshot from the leader"
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

    /// Ask the leader for a snapshot that replaces the entries before the membership row. Raft
    /// drops the request on a leader, without a leader, or with a request in flight. The next
    /// tick asks again. The request is for the last index of this member. An idle leader serves
    /// it with its base, certified through that index. See `certify_base`.
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
            if self.quota.silent < 3 * self.raw.raft.election_timeout() {
                self.hold_campaign();
            }
            self.request_quota_snapshot();
        } else {
            self.quota.silent = 0;
        }
        // Between an accepted snapshot and its install, Raft scans the unapplied entries from its
        // applied index at a campaign. The log starts above that index, so the scan fails and
        // Raft stops the process. The install is local, so the member waits for it.
        if self.installing() {
            self.hold_campaign();
        }
        self.raw.tick();
        self.propose_term_start()
    }

    /// Step a peer message. A member that waits for a snapshot ignores a leadership transfer.
    pub(super) fn step_peer(&mut self, message: Message) -> raft::Result<()> {
        if message.get_msg_type() == MessageType::MsgTimeoutNow
            && (self.quota.blocked().is_some() || self.installing())
        {
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

    /// Hand leadership to the voter with the longest matched log. A voter that can take over
    /// the writer comes first. See `Node::rank_voters`.
    fn yield_leadership(&mut self) {
        if self.raw.raft.lead_transferee.is_some() {
            return;
        }
        if let Some(target) = self.rank_voters() {
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
            "flag_bytes": self.quota.flag,
            "flag_mismatch": self.state.quota_mismatch,
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
