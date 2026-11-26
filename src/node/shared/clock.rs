//! The clock checks of the shared writer. See "Clocks" in `docs/shared-state.md`.
//!
//! SlateDB stamps each write with the clock of its writer, and a new writer starts its clock at
//! the manifest's `last_l0_clock_tick`. Two checks keep that tick near the clocks of the voters:
//!
//! - **Behind:** a new writer waits until its clock passes the manifest tick. Over the skew
//!   limit it does not start. See `Clock::pass`.
//! - **Ahead:** the leader probes the clock of each voter. A majority of `m` voters holds the
//!   leader and the `m - 1` peers with the largest clocks. The `(m - 1)`-th largest peer clock
//!   plus the skew limit is the bound of a write. See `Clock::verdict`.

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
    nonce: u64,
    samples: HashMap<u64, Sample>,
}

/// The decision of the ahead check at one time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Verdict {
    /// One voter, or this member does not lead. No check applies.
    Off,
    /// Fewer than `m - 1` voters have a fresh sample.
    Waiting,
    /// The leader clock `real` is above the bound also at the favourable edge of each window,
    /// by `by` milliseconds. `cap` is the bound at the unfavourable edge.
    Ahead { by: i64, cap: i64, real: i64 },
    /// A tick at or below `cap` is at most the skew limit above a majority of the voters. The
    /// leader clock reads `real`.
    Admit { cap: i64, real: i64 },
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
    /// The bound is the `(m - 1)`-th largest projected peer clock plus the skew limit. `cap`
    /// uses the low edge of each window, so a tick at or below it is never too far ahead of a
    /// majority. The refusal uses the high edge, so a slow reply alone never refuses. The
    /// leader's own clock is read now, and it counts once as the last member of the majority.
    pub(super) fn verdict(&self) -> Verdict {
        let held = self.voters.lock().unwrap();
        let mine = usize::from(held.voters.contains_key(&held.me));
        let need = (held.voters.len() / 2 + 1).saturating_sub(mine);
        if held.voters.is_empty() || need == 0 {
            return Verdict::Off;
        }
        let now = (self.mono_ms)();
        let fresh: Vec<Sample> = held
            .samples
            .iter()
            .filter(|(id, sample)| {
                **id != held.me && held.voters.contains_key(id) && now - sample.at < FRESH_MS
            })
            .map(|(_, sample)| *sample)
            .collect();
        drop(held);
        if fresh.len() < need {
            return Verdict::Waiting;
        }
        let skew = self.max_skew.as_millis() as i64;
        let bound = |edge: fn(&Sample) -> i64| {
            let mut edges: Vec<i64> = fresh.iter().map(edge).collect();
            edges.sort_unstable_by(|a, b| b.cmp(a));
            edges[need - 1].saturating_add(now).saturating_add(skew)
        };
        let cap = bound(|sample| sample.low);
        let limit = bound(|sample| sample.high);
        let real = (self.now_ms)();
        if real > limit {
            Verdict::Ahead {
                by: real - limit,
                cap,
                real,
            }
        } else {
            Verdict::Admit { cap, real }
        }
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

    /// Check the leader clock against the samples, for `/status` and the alarm gauge.
    pub(in crate::node) fn observe(&self) {
        match self.verdict() {
            Verdict::Ahead { by, .. } => self.note_ahead(Some(by)),
            Verdict::Off | Verdict::Admit { .. } => self.note_ahead(None),
            // Missing samples keep the last decision.
            Verdict::Waiting => {}
        }
    }

    /// This member stopped leading. Its samples and its ahead alarm end.
    pub(in crate::node) fn stand_down(&self) {
        self.configure(0, 0, BTreeMap::new());
        self.note_ahead(None);
        self.waiting.store(false, Ordering::Relaxed);
    }

    /// The voters whose clock is above the `m`-th largest clock of the voters. The leader's own
    /// clock is read now. A voter without a fresh sample is not in the set.
    pub(in crate::node) fn above(&self) -> HashSet<u64> {
        let held = self.voters.lock().unwrap();
        let majority = held.voters.len() / 2 + 1;
        let now = (self.mono_ms)();
        let mut offsets: Vec<(u64, i64)> = held
            .samples
            .iter()
            .filter(|(id, sample)| {
                **id != held.me && held.voters.contains_key(id) && now - sample.at < FRESH_MS
            })
            .map(|(id, sample)| (*id, sample.low))
            .collect();
        if held.voters.contains_key(&held.me) {
            offsets.push((held.me, (self.now_ms)() - now));
        }
        let me = held.me;
        drop(held);
        if offsets.len() < majority {
            return HashSet::new();
        }
        let mut sorted: Vec<i64> = offsets.iter().map(|(_, offset)| *offset).collect();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        let mth = sorted[majority - 1];
        offsets
            .into_iter()
            .filter(|(id, offset)| *id != me && *offset > mth)
            .map(|(id, _)| id)
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

    /// Wait until the ahead check can decide, and return the SlateDB clock of a new writer. A
    /// leader of several voters thus waits for the samples of a majority before its open, and
    /// its `Db` never reads a time without a bound. Missing samples are no measured skew, so
    /// the wait is no writer fault.
    pub(super) async fn gate(&self) -> Arc<Gate> {
        loop {
            let start = match self.verdict() {
                Verdict::Off => None,
                Verdict::Waiting => {
                    self.wait(NO_SAMPLES, RECHECK).await;
                    continue;
                }
                Verdict::Ahead { cap, real, .. } | Verdict::Admit { cap, real } => {
                    Some(real.min(cap))
                }
            };
            self.set_waiting(None);
            return Arc::new(Gate::new(Some(self.clone()), start));
        }
    }

    pub(in crate::node) fn status(&self) -> Value {
        let samples = self.voters.lock().unwrap().samples.len();
        json!({
            "alarm": self.alarm_name(),
            "behind_ms": self.skew.load(Ordering::Relaxed),
            "ahead_ms": self.ahead.load(Ordering::Relaxed),
            "waiting": self.waiting(),
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

/// The wait reason of a writer without the samples of a voter majority.
const NO_SAMPLES: &str = "too few voters have a fresh clock sample";

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
/// `max(last, min(now, cap))`. Until the batch completes, `now` returns exactly that tick, so
/// the batch writer of SlateDB stamps the batch with it. A later step of the wall clock or a
/// change of the samples cannot change it.
///
/// Flushes, checkpoints, the compactor, and GC read `now` too. Between batches it returns
/// `max(last, min(now, cap))`, and without fresh samples it stays at `last`. So it never steps
/// back. Sleeps and tickers use SlateDB's default clock, which follows the monotonic clock. A
/// step of the wall clock thus does not move them. With one voter, `now` is the default clock.
pub(in crate::node) struct Gate {
    clock: Option<Clock>,
    default: DefaultSystemClock,
    ticks: Mutex<Ticks>,
    /// Tests: each admitted batch notifies the first and waits for the second before SlateDB
    /// gets it.
    #[cfg(test)]
    pub(super) hold: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
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
            .field("ticks", &*self.ticks.lock().unwrap())
            .finish_non_exhaustive()
    }
}

impl Gate {
    fn new(clock: Option<Clock>, start: Option<i64>) -> Self {
        let default = DefaultSystemClock::new();
        let last = start.unwrap_or_else(|| default.now().timestamp_millis());
        Self {
            clock,
            default,
            ticks: Mutex::new(Ticks {
                last,
                admitted: None,
            }),
            #[cfg(test)]
            hold: Mutex::default(),
        }
    }

    /// A clock with no ahead check, for tests that drive the materializer alone.
    #[cfg(test)]
    pub(in crate::node) fn unchecked() -> Arc<Self> {
        Arc::new(Self::new(None, None))
    }

    /// The writer opened at the manifest tick `tick`. Each later tick is at or above it.
    pub(super) fn floor(&self, tick: i64) {
        let mut ticks = self.ticks.lock().unwrap();
        ticks.last = ticks.last.max(tick);
    }

    /// The time that SlateDB reads now, in milliseconds.
    fn read(&self) -> i64 {
        let mut ticks = self.ticks.lock().unwrap();
        if let Some(tick) = ticks.admitted {
            return tick;
        }
        let verdict = self.clock.as_ref().map_or(Verdict::Off, Clock::verdict);
        let next = match verdict {
            Verdict::Off => self.default.now().timestamp_millis(),
            Verdict::Waiting => ticks.last,
            Verdict::Ahead { cap, real, .. } | Verdict::Admit { cap, real } => real.min(cap),
        };
        ticks.last = ticks.last.max(next);
        ticks.last
    }

    /// Fix the tick of the next batch. Wait while too few voters have a fresh sample, or while
    /// the cap is below the last tick, as after an open above the cap. Refuse the batch with a
    /// `ClockAhead` error when the leader clock is ahead of a voter majority. The batch then
    /// stays in the overlay, and the next writer writes it again.
    pub(super) async fn admit(&self) -> Result<Admitted<'_>> {
        let Some(clock) = &self.clock else {
            return Ok(Admitted { gate: self });
        };
        loop {
            let (cap, real) = match clock.verdict() {
                Verdict::Off => {
                    clock.set_waiting(None);
                    return Ok(Admitted { gate: self });
                }
                Verdict::Waiting => {
                    clock.wait(NO_SAMPLES, RECHECK).await;
                    continue;
                }
                Verdict::Ahead { by, .. } => {
                    clock.set_waiting(None);
                    clock.note_ahead(Some(by));
                    return Err(ClockAhead { by_ms: by }.into());
                }
                Verdict::Admit { cap, real } => (cap, real),
            };
            clock.note_ahead(None);
            // The lock guard must not live across an await.
            let above = {
                let mut ticks = self.ticks.lock().unwrap();
                let tick = ticks.last.max(real.min(cap));
                if tick <= cap {
                    ticks.last = tick;
                    ticks.admitted = Some(tick);
                }
                tick.saturating_sub(cap)
            };
            if above > 0 {
                let wait = Duration::from_millis(above as u64).min(RECHECK);
                clock
                    .wait("the last tick is above the clock bound", wait)
                    .await;
                continue;
            }
            clock.set_waiting(None);
            return Ok(Admitted { gate: self });
        }
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
pub(super) struct Admitted<'a> {
    gate: &'a Gate,
}

impl Admitted<'_> {
    /// SlateDB wrote the batch. Later reads follow the clock again.
    pub(super) fn done(self) {
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
    pub(super) fn tick_clock(&mut self, probe: bool) {
        let Some(writer) = &self.writer else {
            return;
        };
        if self.manual_transport {
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
        if !probe {
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
