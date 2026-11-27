//! The clock checks of the shared writer. See "Clocks" in `docs/shared-state.md`.
//!
//! SlateDB stamps each write with the clock of its writer, and a new writer starts its clock at
//! the manifest's `last_l0_clock_tick`. Two checks keep that tick near the clocks of the voters:
//!
//! - **Behind:** a new writer waits until its clock passes the manifest tick. Over the skew
//!   limit it does not start. See `Clock::pass`.
//! - **Ahead:** the leader probes the clock of each voter. The bound of a tick is the `m`-th
//!   largest clock of all voters plus the skew limit, where `m` is a majority. The leader reads
//!   its own clock fresh at each check. A skew is measured only when no majority can support
//!   the leader clock, also with each missing voter on the leader's side. Otherwise it is
//!   uncertain, and ticks stay at the bound. See `Clock::verdict`.
//!
//! `Gate` is the SlateDB clock of one writer `Db`. It belongs to the term of its open, and it
//! freezes when that term ends. See `Gate`.
//!
//! Lock order: `Clock::voters`, then `Gate::ticks`. `configure` and `sample` take only
//! `voters`, so a refresh of the voters or samples runs wholly before or after each tick
//! decision. See `Clock::with_verdict`.

use super::*;
use serde::{Deserialize, Serialize};
use slatedb_common::clock::{DefaultSystemClock, SystemClock, SystemClockTicker};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

/// Milliseconds of a clock: wall time since the Unix epoch, or monotonic time since a start.
pub(crate) type Millis = Arc<dyn Fn() -> i64 + Send + Sync>;

/// The peer route that returns the clock of a member.
pub(crate) const PROBE_ROUTE: &str = "/raft/clock";
/// Ticks between two clock probes of the leader: 500 ms.
pub(super) const PROBE_TICKS: u64 = 5;
/// The longest wait for the reply to a probe.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
/// A sample counts for this time after its reply: eight probe intervals. The error of an old
/// sample grows only with the rate difference of two clocks, some milliseconds per minute. A
/// lost reply must not stop the writer, so several replies in a row can be lost.
const FRESH_MS: i64 = 4_000;
/// The longest wait of a writer before it reads the samples again.
const RECHECK: Duration = Duration::from_millis(100);

/// The body of a clock probe.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct ProbeRequest {
    pub nonce: u64,
}

/// The reply of a member to a clock probe.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct ProbeReply {
    /// The nonce of the probe.
    pub nonce: u64,
    /// The process of the member. See `crate::peer::process_id`.
    pub process: u64,
    /// The wall clock of the member, in milliseconds since the Unix epoch.
    pub clock_ms: i64,
}

/// A probe that the leader sends to one voter.
#[derive(Clone, Copy, Debug)]
pub(in crate::node) struct Probe {
    generation: u64,
    nonce: u64,
}

/// Tests: the nonce of `probe`.
#[cfg(test)]
pub(in crate::node) fn probe_nonce(probe: &Probe) -> u64 {
    probe.nonce
}

/// The clock of one voter, as one probe measured it. The voter read its clock at a time in the
/// probe window from the send to the reply. Its offset to the leader's monotonic clock is thus
/// between `low` and `high`. Each projection of the offset advances at the leader's monotonic
/// rate.
#[derive(Clone, Copy, Debug)]
struct Sample {
    nonce: u64,
    /// The voter clock minus the monotonic time of the reply.
    low: i64,
    /// The voter clock minus the monotonic time of the send.
    high: i64,
    /// The monotonic time of the reply.
    at: i64,
}

/// The voters of this leader's term and their samples.
#[derive(Default)]
struct Voters {
    /// Goes up at each change of the term or of the voters. A probe of an earlier generation
    /// gives no sample.
    generation: u64,
    term: u64,
    me: u64,
    /// The voters, each with the process of its last transport frame. Empty while this member
    /// does not lead.
    voters: BTreeMap<u64, Option<u64>>,
    /// The node configured this clock at least once. A clock that no node drives, as in a test
    /// of the writer alone, has no checks.
    managed: bool,
    nonce: u64,
    samples: HashMap<u64, Sample>,
}

/// The bound of a tick at one time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Bound {
    /// The `m`-th largest voter clock plus the skew limit. A tick at or below it is at most the
    /// skew limit above a majority of the voters.
    pub cap: i64,
    /// The leader clock now.
    pub real: i64,
}

/// The decision of the ahead check at one time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Verdict {
    /// This member does not lead. No tick is authorized.
    Off,
    /// This member is the only voter. Its own clock is the majority.
    Single(Bound),
    /// Too few voters have a fresh sample to name the `m`-th largest clock.
    Waiting,
    /// The leader clock is above the bound, but missing voters could still form a majority
    /// that supports it. This is no measured skew: ticks stay at the cap, and no alarm rises.
    Uncertain(Bound),
    /// The leader clock is above the bound also at the high edge of each window, and also when
    /// each missing voter agrees with the leader. It is `by` milliseconds ahead.
    Ahead { by: i64, bound: Bound },
    /// The leader clock is within the bound.
    Admit(Bound),
}

/// One voter in the order of the clocks: its offset to the leader's monotonic clock at the low
/// and the high edge of its window. The leader's own entry has equal edges.
#[derive(Clone, Copy, Debug)]
struct Entry {
    id: u64,
    low: i64,
    high: i64,
}

/// The clocks of the voters at one monotonic time `now`: the leader read now, and each peer
/// with a fresh sample.
struct Order {
    me: u64,
    majority: usize,
    entries: Vec<Entry>,
    /// Voters other than the leader without a fresh sample.
    missing: usize,
    now: i64,
}

impl Order {
    fn new(held: &Voters, now: i64, real: i64) -> Self {
        let mut entries = vec![];
        if held.voters.contains_key(&held.me) {
            let own = real.saturating_sub(now);
            entries.push(Entry {
                id: held.me,
                low: own,
                high: own,
            });
        }
        let mut missing = 0;
        for &id in held.voters.keys().filter(|id| **id != held.me) {
            let fresh = held
                .samples
                .get(&id)
                .filter(|sample| now - sample.at < FRESH_MS);
            match fresh {
                Some(sample) => entries.push(Entry {
                    id,
                    low: sample.low,
                    high: sample.high,
                }),
                None => missing += 1,
            }
        }
        Self {
            me: held.me,
            majority: held.voters.len() / 2 + 1,
            entries,
            missing,
            now,
        }
    }

    /// The `m`-th largest projected clock at `edge`, with `top` more voters above all others.
    /// `None` when the order has fewer than `m` voters.
    fn mth(&self, edge: fn(&Entry) -> i64, top: usize) -> Option<i64> {
        let Some(rank) = self.majority.checked_sub(top + 1) else {
            return Some(i64::MAX);
        };
        let mut edges: Vec<i64> = self.entries.iter().map(edge).collect();
        edges.sort_unstable_by(|a, b| b.cmp(a));
        edges
            .get(rank)
            .map(|offset| offset.saturating_add(self.now))
    }
}

/// The clock checks of this member's writer.
#[derive(Clone)]
pub(in crate::node) struct Clock {
    /// Milliseconds since the Unix epoch. Tests skew it.
    pub now_ms: Millis,
    /// Monotonic milliseconds. The probe windows and the sample age use it. Tests move it.
    pub mono_ms: Millis,
    pub max_skew: Duration,
    /// The last measured skew: `last_l0_clock_tick` minus the local time, at least zero.
    pub skew: Arc<AtomicI64>,
    /// Set while the skew is over the limit and the writer does not start.
    pub alarm: Arc<AtomicBool>,
    pub gauge: Option<prometheus::IntGauge>,
    voters: Arc<Mutex<Voters>>,
    /// Milliseconds that the leader clock is ahead of a voter majority over the skew limit, also
    /// at the favourable edge of each window. Zero when it is not.
    ahead: Arc<AtomicI64>,
    /// Set while the writer waits for samples or for its bound to pass the last tick.
    waiting: Arc<AtomicBool>,
    /// Set while the leader clock is above the bound, but missing voters could support it.
    uncertain: Arc<AtomicBool>,
    /// A new sample wakes a writer that waits.
    sampled: Arc<tokio::sync::Notify>,
}

impl Clock {
    pub fn system(max_skew: Duration, gauge: Option<prometheus::IntGauge>) -> Self {
        let start = Instant::now();
        Self {
            now_ms: Arc::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |since| since.as_millis() as i64)
            }),
            mono_ms: Arc::new(move || start.elapsed().as_millis() as i64),
            max_skew,
            skew: Arc::default(),
            alarm: Arc::default(),
            gauge,
            voters: Arc::default(),
            ahead: Arc::default(),
            waiting: Arc::default(),
            uncertain: Arc::default(),
            sampled: Arc::default(),
        }
    }

    /// Wait until the local clock passes `tick`. Return false, and raise the alarm, when the
    /// local clock is behind by more than the limit.
    pub async fn pass(&self, tick: i64) -> bool {
        // A new database has the minimum tick, so the subtraction saturates.
        let skew = tick.saturating_sub((self.now_ms)()).max(0);
        self.skew.store(skew, Ordering::Relaxed);
        if let Some(gauge) = &self.gauge {
            gauge.set(skew);
        }
        if skew > self.max_skew.as_millis() as i64 {
            if !self.alarm.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    skew_ms = skew,
                    "the local clock is behind the shared writer"
                );
            }
            return false;
        }
        self.alarm.store(false, Ordering::Relaxed);
        loop {
            let behind = tick.saturating_sub((self.now_ms)());
            if behind < 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis((behind as u64 + 1).min(100))).await;
        }
    }

    /// Set the term and the voters of this leader. Each voter has the process of its last
    /// transport frame. A member that does not lead gives no voters. A new term or another set
    /// of voters drops every sample. A new process of a voter drops the sample of that voter.
    pub(in crate::node) fn configure(
        &self,
        term: u64,
        me: u64,
        voters: BTreeMap<u64, Option<u64>>,
    ) {
        let mut held = self.voters.lock().unwrap();
        if held.term != term || held.me != me || !held.voters.keys().eq(voters.keys()) {
            held.generation += 1;
            held.samples.clear();
        } else {
            for (id, process) in &voters {
                if held.voters.get(id) != Some(process) {
                    held.samples.remove(id);
                }
            }
        }
        held.term = term;
        held.me = me;
        held.voters = voters;
        held.managed = true;
    }

    /// Start a probe of `peer`.
    pub(in crate::node) fn probe(&self) -> Probe {
        let mut held = self.voters.lock().unwrap();
        held.nonce += 1;
        Probe {
            generation: held.generation,
            nonce: held.nonce,
        }
    }

    /// Record the reply of `peer` to `probe`, sent and received at the monotonic times `sent`
    /// and `received`. Return false when the reply gives no sample: the probe is of an earlier
    /// term or set of voters, the reply comes from another process than the voter's current
    /// one, or a newer probe already gave a sample.
    pub(in crate::node) fn sample(
        &self,
        peer: u64,
        probe: Probe,
        reply: ProbeReply,
        sent: i64,
        received: i64,
    ) -> bool {
        let mut held = self.voters.lock().unwrap();
        if probe.generation != held.generation || reply.nonce != probe.nonce || peer == held.me {
            return false;
        }
        if held.voters.get(&peer) != Some(&Some(reply.process)) {
            return false;
        }
        // A delayed reply can come after the reply to a later probe. The later one holds.
        if held
            .samples
            .get(&peer)
            .is_some_and(|sample| sample.nonce >= probe.nonce)
        {
            return false;
        }
        let sample = Sample {
            nonce: probe.nonce,
            low: reply.clock_ms.saturating_sub(received),
            high: reply.clock_ms.saturating_sub(sent),
            at: received,
        };
        held.samples.insert(peer, sample);
        drop(held);
        self.sampled.notify_waiters();
        true
    }

    /// Decide whether the leader can write a tick now.
    ///
    /// The bound is the `m`-th largest clock of all voters plus the skew limit, where `m` is a
    /// majority. The leader reads its own clock now. `cap` uses the low edge of each peer
    /// window, so a tick at or below it is never too far ahead of a majority. A measured skew
    /// uses the high edges and counts each missing voter as one that agrees with the leader, so
    /// neither a slow reply nor a missing one alone refuses.
    pub(super) fn verdict(&self) -> Verdict {
        self.decide().2
    }

    /// The verdict, whether a node manages this clock, and the term that this member leads.
    fn decide(&self) -> (bool, Option<u64>, Verdict) {
        self.with_verdict(|managed, term, verdict| (managed, term, verdict))
    }

    /// Run `then` with the verdict while the lock of the voters is still held. A tick decision
    /// in `then` thus uses a proof that no refresh can change before the decision ends. `then`
    /// may take `Gate::ticks`, but nothing else.
    fn with_verdict<T>(&self, then: impl FnOnce(bool, Option<u64>, Verdict) -> T) -> T {
        let held = self.voters.lock().unwrap();
        let (managed, term, verdict) = self.decide_held(&held);
        then(managed, term, verdict)
    }

    /// The verdict from the voters `held`.
    fn decide_held(&self, held: &Voters) -> (bool, Option<u64>, Verdict) {
        if held.voters.is_empty() {
            return (held.managed, None, Verdict::Off);
        }
        let (now, real) = ((self.mono_ms)(), (self.now_ms)());
        let order = Order::new(held, now, real);
        let (managed, term) = (held.managed, Some(held.term));
        let single = held.voters.len() == 1 && held.voters.contains_key(&held.me);
        let skew = self.max_skew.as_millis() as i64;
        let Some(low) = order.mth(|entry| entry.low, 0) else {
            return (managed, term, Verdict::Waiting);
        };
        let bound = Bound {
            cap: low.saturating_add(skew),
            real,
        };
        if single {
            return (managed, term, Verdict::Single(bound));
        }
        let measured = order.mth(|entry| entry.high, 0).unwrap_or(i64::MAX);
        if real <= measured.saturating_add(skew) {
            return (managed, term, Verdict::Admit(bound));
        }
        let limit = order
            .mth(|entry| entry.high, order.missing)
            .unwrap_or(i64::MAX)
            .saturating_add(skew);
        let verdict = match real > limit {
            true => Verdict::Ahead {
                by: real - limit,
                bound,
            },
            false => Verdict::Uncertain(bound),
        };
        (managed, term, verdict)
    }

    /// Record a measured skew ahead of a voter majority, or its end. Log the start of one.
    pub(super) fn note_ahead(&self, by: Option<i64>) {
        let by = by.unwrap_or(0);
        if self.ahead.swap(by, Ordering::Relaxed) == 0 && by > 0 {
            tracing::error!(
                ahead_ms = by,
                "the local clock is ahead of a voter majority over the skew limit"
            );
        }
    }

    /// Record the decision of a check for `/status`: a measured skew, an uncertain one, or
    /// neither.
    fn note(&self, verdict: &Verdict) {
        let (ahead, uncertain) = match verdict {
            Verdict::Ahead { by, .. } => (Some(*by), false),
            Verdict::Uncertain(_) => (None, true),
            Verdict::Off | Verdict::Single(_) | Verdict::Admit(_) => (None, false),
            // Missing samples keep the last decision.
            Verdict::Waiting => return,
        };
        self.note_ahead(ahead);
        self.uncertain.store(uncertain, Ordering::Relaxed);
    }

    /// Check the leader clock against the samples, for `/status` and the alarm gauge.
    pub(in crate::node) fn observe(&self) {
        self.note(&self.verdict());
    }

    /// This member stopped leading. Its samples and its alarms of the ahead check end. A writer
    /// `Db` of the ended term keeps its last tick. See `Gate`.
    pub(in crate::node) fn stand_down(&self) {
        self.configure(0, 0, BTreeMap::new());
        self.note_ahead(None);
        self.uncertain.store(false, Ordering::Relaxed);
        self.waiting.store(false, Ordering::Relaxed);
    }

    /// The voters that the ahead check would refuse as leader: their clock is above the `m`-th
    /// largest voter clock plus the skew limit. The order is the one of `verdict`, with the
    /// leader's own clock read now. A voter without a fresh sample is not in the set.
    pub(in crate::node) fn above(&self) -> HashSet<u64> {
        let held = self.voters.lock().unwrap();
        let order = Order::new(&held, (self.mono_ms)(), (self.now_ms)());
        drop(held);
        let Some(mth) = order.mth(|entry| entry.low, 0) else {
            return HashSet::new();
        };
        let limit = mth.saturating_add(self.max_skew.as_millis() as i64);
        order
            .entries
            .iter()
            .filter(|entry| entry.id != order.me && entry.low.saturating_add(order.now) > limit)
            .map(|entry| entry.id)
            .collect()
    }

    /// The alarm of the clock checks: `behind`, `ahead`, or none.
    pub(in crate::node) fn alarm_name(&self) -> Option<&'static str> {
        if self.alarm.load(Ordering::Relaxed) {
            Some("behind")
        } else if self.ahead.load(Ordering::Relaxed) > 0 {
            Some("ahead")
        } else {
            None
        }
    }

    pub(in crate::node) fn waiting(&self) -> bool {
        self.waiting.load(Ordering::Relaxed)
    }

    pub(in crate::node) fn uncertain(&self) -> bool {
        self.uncertain.load(Ordering::Relaxed)
    }

    /// Set or clear the wait of the writer. Log the start of a wait.
    fn set_waiting(&self, reason: Option<&str>) {
        let was = self.waiting.swap(reason.is_some(), Ordering::Relaxed);
        if let (false, Some(reason)) = (was, reason) {
            // A new leader waits for its first samples. `/status` shows a longer wait.
            tracing::info!(reason, "the shared writer waits for its clock bound");
        }
    }

    /// Wait for a new sample, at most `most`.
    async fn wait(&self, reason: &str, most: Duration) {
        let sampled = self.sampled.notified();
        self.set_waiting(Some(reason));
        let _ = tokio::time::timeout(most, sampled).await;
    }

    /// Wait until the ahead check can decide, and return the SlateDB clock of a new writer.
    ///
    /// The gate belongs to the term that this member leads now. A leader of several voters
    /// waits for the samples of a majority before its open, and its `Db` never reads a time
    /// without a bound. A lone voter gets a gate without a check. Only a clock that no node
    /// manages, in a test, gets a gate with no owner.
    pub(in crate::node) async fn gate(&self) -> Arc<Gate> {
        loop {
            let (managed, term, verdict) = self.decide();
            if !managed {
                return Arc::new(Gate::new(Some(self.clone()), Mode::Test, None));
            }
            // A member that stopped leading waits until the node stops its writer task.
            let Some(term) = term else {
                tokio::time::sleep(RECHECK).await;
                continue;
            };
            let (guarded, start) = match verdict {
                Verdict::Off => {
                    tokio::time::sleep(RECHECK).await;
                    continue;
                }
                Verdict::Single(_) => (false, None),
                Verdict::Waiting => {
                    self.wait(NO_SAMPLES, RECHECK).await;
                    continue;
                }
                Verdict::Ahead { bound, .. }
                | Verdict::Uncertain(bound)
                | Verdict::Admit(bound) => (true, Some(bound.real.min(bound.cap))),
            };
            self.set_waiting(None);
            let mode = Mode::Owned {
                term,
                guarded: AtomicBool::new(guarded),
            };
            return Arc::new(Gate::new(Some(self.clone()), mode, start));
        }
    }

    pub(in crate::node) fn status(&self) -> Value {
        let samples = self.voters.lock().unwrap().samples.len();
        json!({
            "alarm": self.alarm_name(),
            "behind_ms": self.skew.load(Ordering::Relaxed),
            "ahead_ms": self.ahead.load(Ordering::Relaxed),
            "waiting": self.waiting() || self.uncertain(),
            "uncertain": self.uncertain(),
            "samples": samples,
        })
    }
}

/// The lowest time of a new writer on `manifest`: its `last_l0_clock_tick`, or the time in the
/// newest L0 SST ID when that is later. SlateDB 0.18.1 takes SST IDs from its clock, and it
/// refuses a new L0 SST whose ID time is below the newest one (`manifest/invariants.rs:51-83`).
/// A writer whose clock is behind that time thus cannot flush.
pub(super) fn floor(manifest: &slatedb::VersionedManifest) -> i64 {
    let newest = match manifest.l0().is_empty() {
        true => manifest.last_compacted_l0_sst_view_id(),
        false => manifest.l0().iter().map(|view| view.sst.id.value()).max(),
    };
    let newest = newest.map_or(i64::MIN, |id| id.timestamp_ms() as i64);
    manifest.last_l0_clock_tick().max(newest)
}

/// The floor of the stored state before a writer opens: `floor` of the latest manifest, or the
/// time of the newest compaction job ID when that is later.
///
/// SlateDB 0.18.1 refuses a new compaction job whose ID time is below the newest job
/// (`compactions_store/invariants.rs:12-31`). The embedded compactor of a writer takes job IDs
/// from its clock at once, and a refused job fails the compactor task, which closes the whole
/// `Db` (`dispatcher.rs`, `TaskMonitor::run`). So the gate starts at this floor before the
/// open.
pub(super) async fn stored_floor(admin: &slatedb::admin::Admin) -> Result<i64> {
    let manifest = admin.read_manifest(None).await?;
    let compactions = admin.read_compactions(None).await?;
    let jobs = compactions
        .iter()
        .flat_map(|compactions| compactions.recent_compactions())
        .map(|job| job.id().timestamp_ms() as i64)
        .max()
        .unwrap_or(i64::MIN);
    Ok(manifest.as_ref().map_or(i64::MIN, floor).max(jobs))
}

/// The wait reason of a writer without the samples of a voter majority.
const NO_SAMPLES: &str = "too few voters have a fresh clock sample";
/// The wait reason of a writer whose last tick is above the bound.
const ABOVE_CAP: &str = "the last tick is above the clock bound of a voter majority";

/// A batch refusal: the leader clock is ahead of a voter majority over the skew limit. The
/// writer reports it as a `Fault::Clock`.
#[derive(Debug)]
pub(in crate::node) struct ClockAhead {
    pub by_ms: i64,
}

impl std::fmt::Display for ClockAhead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the local clock is {} ms ahead of a voter majority over the skew limit",
            self.by_ms
        )
    }
}

impl std::error::Error for ClockAhead {}

/// The SlateDB clock of one writer `Db`.
///
/// `admit` fixes the tick of each batch before the materializer submits it:
/// `max(last, min(now, cap))`. The tick must be at or below the cap, so a retained `last` that
/// a backward step left above the bound waits. Until the batch completes, `now` returns exactly
/// that tick, so the batch writer of SlateDB stamps the batch with it. A later step of the wall
/// clock or a change of the samples cannot change it.
///
/// Flushes, checkpoints, the compactor, and GC read `now` too. They take the SST IDs, the
/// checkpoint times, and the sequence times from it. Between batches it returns
/// `max(last, min(now, cap))`, and without fresh samples it stays at `last`. So it never steps
/// back. Sleeps and tickers use SlateDB's default clock, which follows the monotonic clock. A
/// step of the wall clock thus does not move them.
///
/// A gate belongs to the term of its open. When that term ends, `now` stays at `last` and no
/// batch gets a tick again. A lone voter's gate reads SlateDB's default clock. It takes the
/// check when a voter joins in the term, and it never drops the check again.
pub(in crate::node) struct Gate {
    clock: Option<Clock>,
    mode: Mode,
    default: DefaultSystemClock,
    ticks: Mutex<Ticks>,
    /// Tests: each admitted batch notifies the first and waits for the second before SlateDB
    /// gets it.
    #[cfg(test)]
    pub(super) hold: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    /// Tests: the next `read` takes the pair. It sends on the first and waits on the second
    /// between its verdict and its advance of `last`.
    #[cfg(test)]
    pub(super) read_hold: Mutex<Option<ReadHold>>,
}

#[cfg(test)]
pub(super) type ReadHold = (
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
);

/// The owner and the check of a gate. The open of the writer decides it.
#[derive(Debug)]
enum Mode {
    /// No node manages the clock, as in a test of the writer alone. No check applies.
    Test,
    /// The writer of leader term `term`. Without `guarded`, the open saw one voter.
    Owned { term: u64, guarded: AtomicBool },
}

/// One decision of `Gate::admit`.
enum Step {
    /// The batch goes on without a pinned tick: a test gate or a lone voter's gate.
    Unpinned,
    /// The batch has its pinned tick.
    Pinned(Verdict),
    /// Wait for a sample, at most the time given, then decide again.
    Wait(&'static str, Duration, Option<Verdict>),
    /// The leader clock is ahead of a voter majority by the given milliseconds.
    Refuse(Verdict, i64),
    /// The term of the gate ended.
    Ended,
}

/// What `now` returns between batches.
enum Reading {
    /// SlateDB's default clock.
    Default,
    /// The last time: the term ended, or the samples are missing.
    Frozen,
    /// A time at or below the bound.
    Capped(i64),
}

#[derive(Debug)]
struct Ticks {
    /// The highest time that `now` returned. It is at or above the tick of each batch.
    last: i64,
    /// The tick of the batch that SlateDB writes now.
    admitted: Option<i64>,
}

impl std::fmt::Debug for Gate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gate")
            .field("mode", &self.mode)
            .field("ticks", &*self.ticks.lock().unwrap())
            .finish_non_exhaustive()
    }
}

impl Gate {
    fn new(clock: Option<Clock>, mode: Mode, start: Option<i64>) -> Self {
        let default = DefaultSystemClock::new();
        let last = start.unwrap_or_else(|| default.now().timestamp_millis());
        Self {
            clock,
            mode,
            default,
            ticks: Mutex::new(Ticks {
                last,
                admitted: None,
            }),
            #[cfg(test)]
            hold: Mutex::default(),
            #[cfg(test)]
            read_hold: Mutex::default(),
        }
    }

    /// A clock with no ahead check, for tests that drive the materializer alone.
    #[cfg(test)]
    pub(in crate::node) fn unchecked() -> Arc<Self> {
        Arc::new(Self::new(None, Mode::Test, None))
    }

    /// The writer opened at the manifest tick `tick`. Each later tick is at or above it.
    pub(super) fn floor(&self, tick: i64) {
        let mut ticks = self.ticks.lock().unwrap();
        ticks.last = ticks.last.max(tick);
    }

    /// The verdict for this gate's term from `lead` and `verdict`, or `None` when the term
    /// ended. A verdict with more than one voter turns on the check of a lone voter's gate for
    /// good.
    fn owned(&self, lead: Option<u64>, verdict: Verdict) -> Option<(Verdict, bool)> {
        let Mode::Owned { term, guarded } = &self.mode else {
            return Some((Verdict::Off, false));
        };
        if lead != Some(*term) || verdict == Verdict::Off {
            return None;
        }
        if !matches!(verdict, Verdict::Single(_)) {
            guarded.store(true, Ordering::Relaxed);
        }
        Some((verdict, guarded.load(Ordering::Relaxed)))
    }

    /// The time that SlateDB reads now, in milliseconds. The pinned tick of a batch returns at
    /// once. Otherwise the advance of `last` runs inside `Clock::with_verdict`.
    fn read(&self) -> i64 {
        if let Some(tick) = self.ticks.lock().unwrap().admitted {
            return tick;
        }
        let Some(clock) = &self.clock else {
            return self.advance(Reading::Default);
        };
        clock.with_verdict(|_, lead, verdict| {
            let reading = match self.owned(lead, verdict) {
                None => Reading::Frozen,
                Some((Verdict::Off, _)) => Reading::Default,
                Some((Verdict::Single(_), false)) => Reading::Default,
                Some((Verdict::Waiting, _)) => Reading::Frozen,
                Some((Verdict::Single(bound), true))
                | Some((Verdict::Uncertain(bound), _))
                | Some((Verdict::Ahead { bound, .. }, _))
                | Some((Verdict::Admit(bound), _)) => Reading::Capped(bound.real.min(bound.cap)),
            };
            #[cfg(test)]
            {
                let hold = self.read_hold.lock().unwrap().take();
                if let Some((reached, resume)) = hold {
                    let _ = reached.send(());
                    let _ = resume.recv();
                }
            }
            self.advance(reading)
        })
    }

    /// Move `last` to `reading`, never back, and return it. A batch that SlateDB writes keeps
    /// its pinned tick.
    fn advance(&self, reading: Reading) -> i64 {
        let mut ticks = self.ticks.lock().unwrap();
        if let Some(tick) = ticks.admitted {
            return tick;
        }
        let next = match reading {
            Reading::Default => self.default.now().timestamp_millis(),
            Reading::Frozen => ticks.last,
            Reading::Capped(time) => time,
        };
        ticks.last = ticks.last.max(next);
        ticks.last
    }

    /// Fix the tick of the next batch. Wait while too few voters have a fresh sample, or while
    /// the cap is below the last tick, as after an open above the cap or a backward step.
    /// Refuse the batch with a `ClockAhead` error when the leader clock is ahead of a voter
    /// majority. The batch then stays in the overlay, and the next writer writes it again.
    /// After the term of the gate ends, no batch gets a tick: the call waits until the node
    /// stops the writer task. The verdict and the pin of the tick run in one
    /// `Clock::with_verdict`.
    pub(in crate::node) async fn admit(&self) -> Result<Admitted<'_>> {
        let Some(clock) = &self.clock else {
            return Ok(Admitted { gate: self });
        };
        loop {
            let step = clock.with_verdict(|_, lead, verdict| self.step(lead, verdict));
            match step {
                Step::Unpinned => {
                    clock.set_waiting(None);
                    return Ok(Admitted { gate: self });
                }
                Step::Pinned(verdict) => {
                    clock.note(&verdict);
                    clock.set_waiting(None);
                    return Ok(Admitted { gate: self });
                }
                Step::Wait(reason, wait, verdict) => {
                    if let Some(verdict) = verdict {
                        clock.note(&verdict);
                    }
                    clock.wait(reason, wait).await;
                }
                Step::Refuse(verdict, by) => {
                    clock.set_waiting(None);
                    clock.note(&verdict);
                    return Err(ClockAhead { by_ms: by }.into());
                }
                Step::Ended => {
                    clock.set_waiting(None);
                    return std::future::pending().await;
                }
            }
        }
    }

    /// One decision of `admit`, with the lock of the voters held. A pin happens here.
    fn step(&self, lead: Option<u64>, verdict: Verdict) -> Step {
        let Some((verdict, guarded)) = self.owned(lead, verdict) else {
            return Step::Ended;
        };
        let bound = match verdict {
            // A test gate, or a lone voter's gate that never saw a second voter.
            Verdict::Off | Verdict::Single(_) if !guarded => return Step::Unpinned,
            Verdict::Off => return Step::Ended,
            Verdict::Waiting => return Step::Wait(NO_SAMPLES, RECHECK, None),
            Verdict::Ahead { by, .. } => return Step::Refuse(verdict, by),
            Verdict::Single(bound) | Verdict::Uncertain(bound) | Verdict::Admit(bound) => bound,
        };
        let mut ticks = self.ticks.lock().unwrap();
        let tick = ticks.last.max(bound.real.min(bound.cap));
        if tick > bound.cap {
            let wait = Duration::from_millis(tick.saturating_sub(bound.cap) as u64).min(RECHECK);
            return Step::Wait(ABOVE_CAP, wait, Some(verdict));
        }
        ticks.last = tick;
        ticks.admitted = Some(tick);
        Step::Pinned(verdict)
    }

    /// Tests: hold an admitted batch before SlateDB gets it.
    #[cfg(test)]
    pub(super) async fn held(&self) {
        let hold = self.hold.lock().unwrap().clone();
        if let Some((reached, resume)) = hold {
            reached.notify_one();
            resume.notified().await;
        }
    }
}

/// The tick of one batch. A batch that does not complete keeps its tick: its writer stops,
/// and its clock does not move again.
pub(in crate::node) struct Admitted<'a> {
    gate: &'a Gate,
}

impl Admitted<'_> {
    /// SlateDB wrote the batch. Later reads follow the clock again.
    pub(in crate::node) fn done(self) {
        self.gate.ticks.lock().unwrap().admitted = None;
    }
}

impl SystemClock for Gate {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(self.read()).unwrap_or_else(|| self.default.now())
    }

    fn sleep<'a>(
        &'a self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        self.default.sleep(duration)
    }

    fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
        SystemClockTicker::new(&self.default, duration)
    }
}

impl Node {
    /// The voters, each with the process of its last transport frame.
    fn clock_voters(&self) -> BTreeMap<u64, Option<u64>> {
        let id = self.options.id;
        self.raw
            .raft
            .prs()
            .conf()
            .voters()
            .ids()
            .iter()
            .map(|voter| {
                let process = match voter == id {
                    true => Some(crate::peer::process_id()),
                    false => self.raw.store().process(voter),
                };
                (voter, process)
            })
            .collect()
    }

    /// Give the clock check the term and the voters of this leader. With `probe`, send a clock
    /// probe to each other voter. A manual transport has no peer routes, so the check is off.
    /// The node also calls it at once when Raft applies a configuration change or a voter
    /// process changes, so the next batch admission sees the new voters.
    pub(in crate::node) fn tick_clock(&mut self, probe: bool) {
        let Some(writer) = &self.writer else {
            return;
        };
        #[cfg(test)]
        let manual = self.manual_transport && !self.clock_in_manual;
        #[cfg(not(test))]
        let manual = self.manual_transport;
        if manual {
            return;
        }
        let leader = self.raw.raft.state == StateRole::Leader;
        if !leader {
            writer.clock.stand_down();
            return;
        }
        let voters = self.clock_voters();
        writer
            .clock
            .configure(self.raw.raft.term, self.options.id, voters.clone());
        writer.clock.observe();
        if !probe || self.manual_transport {
            return;
        }
        for &peer in voters.keys().filter(|peer| **peer != self.options.id) {
            let Some(address) = self
                .state
                .members
                .members
                .get(&peer)
                .and_then(|member| member.peer_ur_ls.first())
            else {
                continue;
            };
            let url = format!("{}{PROBE_ROUTE}", crate::cluster::url(address));
            let request = self.peer.headers(self.options.transport.http.post(url));
            let clock = writer.clock.clone();
            let probe = clock.probe();
            tokio::spawn(async move {
                let sent = (clock.mono_ms)();
                let replied = async {
                    let body = ProbeRequest { nonce: probe.nonce };
                    let response = request.timeout(PROBE_TIMEOUT).json(&body).send().await?;
                    response.error_for_status()?.json::<ProbeReply>().await
                };
                match replied.await {
                    Ok(reply) => {
                        let received = (clock.mono_ms)();
                        clock.sample(peer, probe, reply, sent, received);
                    }
                    Err(error) => tracing::debug!(%error, peer, "a clock probe failed"),
                }
            });
        }
    }
}

#[cfg(test)]
#[path = "clock_tests.rs"]
mod tests;
