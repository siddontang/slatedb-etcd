//! Classify shared writer errors, keep the writer health record, and pick a handoff target.
//!
//! See "Failure cases" in `docs/shared-state.md`. A fence reopens the writer at once. A storage
//! error or a clock refusal keeps the member up, and the writer task opens again after a
//! backoff. A data error halts the writer for a longer backoff. Only an error of this member's
//! own invariants stops the member.
//!
//! The leader hands its leadership to a healthy voter after three failures in a row, after one
//! data error, or when due storage work makes no progress for `STALL`. Each member that met a
//! writer fault keeps a mark until its own writer publishes again, and it reports the mark in
//! `ViewAck`. A marked voter is not a target. So a fault on one member's path moves leadership
//! once, and a fault of the whole backend moves it at most once per member.

use super::*;
use serde::{Deserialize, Serialize};

/// The first wait after a storage error or a clock refusal. It doubles up to `RETRY_CAP`.
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_CAP: Duration = Duration::from_secs(30);
/// The first wait after a data error. It doubles up to `DATA_RETRY_CAP`.
const DATA_RETRY_FIRST: Duration = Duration::from_secs(30);
const DATA_RETRY_CAP: Duration = Duration::from_secs(5 * 60);
/// Failures in a row after which the leader hands over its leadership.
const HANDOFF_FAILURES: u32 = 3;
/// Due storage work that makes no durable progress for this time is a fault.
pub(super) const STALL: Duration = Duration::from_secs(30);
/// A failed writer that takes this long to close logs a warning.
const SLOW_CLOSE: Duration = Duration::from_secs(30);
/// Election timeouts that a handoff can take before the leader tries another voter.
const HANDOFF_TIMEOUTS: u32 = 2;

/// The class of a writer fault.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node) enum Fault {
    /// A newer writer fenced this one, or two opens raced for one manifest version.
    Fenced,
    /// The object store failed or timed out.
    Unavailable,
    /// SlateDB cannot use the shared database, for example an object is missing or does not
    /// decode. Each later leader can meet the same error.
    Data,
    /// The local clock is too far behind the clock of the shared writer.
    Clock,
    /// Due storage work made no durable progress for `STALL`.
    Stalled,
    /// An error of this member's own invariants. Its applied state is in doubt.
    Invariant,
}

impl Fault {
    fn name(self) -> &'static str {
        match self {
            Fault::Fenced => "fenced",
            Fault::Unavailable => "unavailable",
            Fault::Data => "data",
            Fault::Clock => "clock",
            Fault::Stalled => "stalled",
            Fault::Invariant => "invariant",
        }
    }
}

/// Classify a writer error. The outermost SlateDB error decides, so a missing object that
/// SlateDB reports as a data error is not a storage error. A stored object that does not decode
/// is a data error too. An object store error with no SlateDB error around it comes from this
/// crate's own storage calls.
pub(in crate::node) fn classify(error: &anyhow::Error) -> Fault {
    if fenced(error) || open_conflict(error) {
        return Fault::Fenced;
    }
    if error
        .chain()
        .any(|cause| cause.is::<crate::state::StoredData>())
    {
        return Fault::Data;
    }
    if let Some(error) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<slatedb::Error>())
    {
        return match error.kind() {
            slatedb::ErrorKind::Unavailable => Fault::Unavailable,
            _ => Fault::Data,
        };
    }
    if error
        .chain()
        .any(|cause| cause.is::<slatedb::object_store::Error>())
    {
        return Fault::Unavailable;
    }
    Fault::Invariant
}

/// Return true when `error` is a lost manifest race: another writer wrote the manifest version
/// that this open tried to write. SlateDB reports it as a data error, so the check uses its
/// message.
pub(in crate::node) fn open_conflict(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<slatedb::Error>().is_some_and(|error| {
            error.kind() == slatedb::ErrorKind::Data
                && error.to_string().contains("version already exists")
        })
    })
}

/// Return true when `error` comes from a writer that a newer writer fenced.
pub(in crate::node) fn fenced(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<slatedb::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
            )
        })
    })
}

/// Return the stored error of a writer that SlateDB closed. A failed background flush closes
/// the `Db`, and the status then reports `Panic` for every kind of error. A call on the closed
/// `Db` returns the stored error, which keeps its kind.
pub(super) async fn closed(
    db: &slatedb::Db,
    status: &tokio::sync::watch::Receiver<slatedb::DbStatus>,
) -> Result<()> {
    let Some(reason) = status.borrow().close_reason else {
        return Ok(());
    };
    db.flush().await?;
    Err(slatedb::Error::closed("the shared writer closed".into(), reason).into())
}

/// Decide what the writer task does after `error` while this member leads: open the writer
/// again after the returned delay, or stop the member with the error.
pub(super) fn recover(
    error: anyhow::Error,
    term: u64,
    health: &TaskHealth,
    reopens: &std::sync::atomic::AtomicU64,
) -> Result<Duration> {
    match classify(&error) {
        Fault::Fenced => {
            // Two quick elections can open writers in reverse term order. The step timer keeps
            // running across this reopen on purpose, also across the bootstrap check of a
            // fenced open: fences for 30 s are no durable progress. See `lead`.
            tracing::warn!(term, %error, "the shared writer was fenced; open it again");
            reopens.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(Duration::ZERO)
        }
        Fault::Invariant => Err(error),
        fault => Ok(health.failed(fault, &format!("{error:#}"))),
    }
}

/// Record a bootstrap claim that the writer task cannot read, and return the wait before the
/// next try. The claim describes the shared store, not this member's state, so the member
/// stays up also when the claim does not decode.
pub(super) fn unreadable_claim(error: &anyhow::Error, health: &TaskHealth) -> Duration {
    let fault = match classify(error) {
        Fault::Unavailable => Fault::Unavailable,
        Fault::Fenced | Fault::Data | Fault::Clock | Fault::Stalled | Fault::Invariant => {
            Fault::Data
        }
    };
    health.failed(
        fault,
        &format!("cannot read the bootstrap claim: {error:#}"),
    )
}

/// The health of this member's writer. The node keeps it, and each writer task updates it
/// through a `TaskHealth`.
#[derive(Clone)]
pub(in crate::node) struct Health(Arc<Shared>);

struct Shared {
    record: Mutex<Record>,
    /// The close of a failed writer. See `TaskHealth::abandon`.
    close: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Closes of failed writers that a newer one aborted.
    aborted_closes: std::sync::atomic::AtomicU64,
    /// The node asks the running attempt of the writer task to stop. See `TaskHealth::restart`.
    restart: tokio::sync::watch::Sender<u64>,
}

impl Default for Health {
    fn default() -> Self {
        Self(Arc::new(Shared {
            record: Mutex::default(),
            close: Mutex::default(),
            aborted_closes: Default::default(),
            restart: tokio::sync::watch::channel(0).0,
        }))
    }
}

#[derive(Default)]
struct Record {
    /// The writer task that may change this record. A start or a stop of a task changes it.
    generation: u64,
    /// Failures since the last publication of this member's writer.
    consecutive: u32,
    /// The first failure of those.
    since: Option<Instant>,
    /// The newest failure, its class, and its error.
    last: Option<(Instant, Fault, String)>,
    /// Faults since the start.
    total: u64,
    /// The writer is open and did not fail since this time.
    running: Option<Instant>,
    /// Storage work is due, and its last durable step ended at this time.
    due: Option<Instant>,
    /// The time when the due work above passed `STALL`, until it ends.
    stalled: Option<Instant>,
    /// The start of the first due work that stalled since the last publication.
    stall_began: Option<Instant>,
    /// The node asked the task to drop its stalled step. The end of the stall clears it.
    restart: bool,
    /// The time of the newest fault, until this member's writer publishes again.
    mark: Option<Instant>,
    /// The time when this member's writer last created and announced a checkpoint.
    published: Option<Instant>,
}

impl Record {
    /// The due storage work ended, or no work is due.
    fn done(&mut self) {
        self.due = None;
        self.stalled = None;
        self.restart = false;
    }

    /// Count a failure and return the wait before the next try. Only a publication starts a
    /// new count: a writer that opens and then fails each flush is still down. The failed step
    /// ended, so the watchdog stops until the next try starts its I/O. A wait that is longer
    /// than `STALL` is thus no stall.
    fn fail(&mut self, fault: Fault) -> Duration {
        self.running = None;
        self.done();
        self.consecutive += 1;
        self.since.get_or_insert_with(Instant::now);
        let (first, cap) = match fault {
            Fault::Data => (DATA_RETRY_FIRST, DATA_RETRY_CAP),
            _ => (RETRY_FIRST, RETRY_CAP),
        };
        first
            .saturating_mul(1 << (self.consecutive - 1).min(16))
            .min(cap)
    }
}

impl Health {
    fn record(&self) -> std::sync::MutexGuard<'_, Record> {
        self.0.record.lock().unwrap()
    }

    /// Start a writer task. The count starts again, so an earlier term's failures do not hand
    /// over this one before its writer tries. The mark stays until a publication.
    pub(super) fn begin(&self) -> TaskHealth {
        let mut record = self.record();
        record.generation += 1;
        record.consecutive = 0;
        record.since = None;
        record.stall_began = None;
        record.done();
        TaskHealth {
            health: self.clone(),
            generation: record.generation,
            restart: self.0.restart.subscribe(),
        }
    }

    /// Stop the writer task. A late result of it changes nothing.
    pub(super) fn end(&self) {
        let mut record = self.record();
        record.generation += 1;
        record.running = None;
        record.done();
    }

    /// Ask the writer task to drop its stalled attempt and open the writer again. The request
    /// holds until the stall ends, so a task between two steps sees it at its next step. A
    /// request after the stall ended does nothing: the writer made progress since the tick.
    pub(super) fn restart(&self) {
        let mut record = self.record();
        if record.stalled.is_none() {
            return;
        }
        record.restart = true;
        drop(record);
        self.0.restart.send_modify(|restarts| *restarts += 1);
    }

    /// Return true when due work just passed `STALL`.
    ///
    /// A running step of the writer task has its own timer, which each durable step restarts.
    /// Only without one, `waiting` counts: the time since work that the publisher has not
    /// started became due. See `Writer::waiting`.
    pub(super) fn check(&self, waiting: Option<Instant>) -> bool {
        let mut record = self.record();
        let waiting = waiting.filter(|at| *at <= Instant::now());
        let Some(due) = record.due.or(waiting) else {
            return false;
        };
        if record.stalled.is_some() || due.elapsed() < STALL {
            return false;
        }
        let now = Instant::now();
        record.stalled = Some(now);
        record.stall_began.get_or_insert(due);
        record.total += 1;
        record.mark = Some(now);
        let error = format!("due storage work made no durable progress for {STALL:?}");
        tracing::error!(%error, "the shared writer stalled");
        record.last = Some((now, Fault::Stalled, error));
        true
    }

    /// Return true when the leader must hand over its leadership: the writer is down after
    /// `HANDOFF_FAILURES` failures in a row or after a data error, or due work stalled.
    pub(super) fn handoff_due(&self) -> bool {
        let record = self.record();
        let data = record
            .last
            .as_ref()
            .is_some_and(|(_, fault, _)| *fault == Fault::Data);
        let down = record.running.is_none()
            && (record.consecutive >= HANDOFF_FAILURES || data && record.consecutive > 0);
        down || record.stalled.is_some()
    }

    /// Return true while due work is past `STALL`.
    pub(super) fn stalled(&self) -> bool {
        self.record().stalled.is_some()
    }

    /// The start of the current incident: the first failure since the last publication, or
    /// the start of the first work that stalled. A voter that reported after it was alive while
    /// this writer failed.
    fn incident(&self) -> Option<Instant> {
        let record = self.record();
        record.since.into_iter().chain(record.stall_began).min()
    }

    /// The report of this member in `ViewAck`, in Raft term `term`.
    pub(in crate::node) fn report(&self, term: u64) -> WriterReport {
        WriterReport {
            failed_secs: self.record().mark.map(|at| at.elapsed().as_secs()),
            term,
        }
    }

    /// 1 while the close of a failed writer is pending.
    pub(super) fn pending_closes(&self) -> u64 {
        let close = self.0.close.lock().unwrap();
        close
            .as_ref()
            .is_some_and(|close| !close.is_finished())
            .into()
    }

    pub(super) fn aborted_closes(&self) -> u64 {
        self.0
            .aborted_closes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Take the pending close of a failed writer, for a shutdown.
    pub(super) fn take_close(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.0.close.lock().unwrap().take()
    }

    pub(super) fn consecutive(&self) -> u32 {
        self.record().consecutive
    }

    pub(super) fn total(&self) -> u64 {
        self.record().total
    }

    fn published_at(&self) -> Option<Instant> {
        self.record().published
    }

    pub(super) fn status(&self) -> Value {
        let record = self.record();
        let last = record.last.as_ref();
        json!({
            "consecutive_failures": record.consecutive,
            "failing_ms": record.since.map(|at| at.elapsed().as_millis() as u64),
            "last_error_class": last.map(|(_, fault, _)| fault.name()),
            "last_error": last.map(|(_, _, error)| error.clone()),
            "last_failure_ms_ago": last.map(|(at, _, _)| at.elapsed().as_millis() as u64),
            "faults": record.total,
            "stalled": record.stalled.is_some(),
            "marked": record.mark.is_some(),
            "pending_closes": self.pending_closes(),
            "aborted_closes": self.aborted_closes(),
        })
    }
}

/// The health record as one writer task sees it. After the task's term ends, its updates
/// change nothing, so a late result cannot touch a newer term.
#[derive(Clone)]
pub(super) struct TaskHealth {
    health: Health,
    generation: u64,
    restart: tokio::sync::watch::Receiver<u64>,
}

impl TaskHealth {
    /// Change the record if this task still owns it.
    fn update<T>(&self, change: impl FnOnce(&mut Record) -> T) -> Option<T> {
        let mut record = self.health.record();
        (record.generation == self.generation).then(|| change(&mut record))
    }

    /// Record a failure and return the wait before the next open.
    pub(super) fn failed(&self, fault: Fault, error: &str) -> Duration {
        let failed = self.update(|record| {
            let delay = record.fail(fault);
            let now = Instant::now();
            record.last = Some((now, fault, error.to_owned()));
            record.total += 1;
            record.mark = Some(now);
            (delay, record.consecutive)
        });
        let Some((delay, failures)) = failed else {
            return RETRY_FIRST;
        };
        let class = fault.name();
        if failures == 1 {
            tracing::error!(
                class,
                error,
                ?delay,
                "the shared writer failed; open it again"
            );
        } else {
            tracing::warn!(
                class,
                failures,
                error,
                ?delay,
                "the shared writer failed again"
            );
        }
        delay
    }

    /// The node dropped a stalled attempt. The stall is already a fault. Return the wait
    /// before the next open. The watchdog starts again with the I/O of the next attempt.
    pub(super) fn restarted(&self) -> Duration {
        let delay = self.update(|record| record.fail(Fault::Stalled));
        tracing::warn!(
            ?delay,
            "drop the stalled shared writer attempt and open it again"
        );
        delay.unwrap_or(RETRY_FIRST)
    }

    /// The writer opened. The publisher ends the due work after its first reads.
    pub(super) fn opened(&self) {
        self.update(|record| record.running = Some(Instant::now()));
    }

    /// Close a writer that failed or stalled, without waiting. A close joins the SST uploads,
    /// and a stuck upload can hold it for a long time. The next open fences this writer, so its
    /// late writes cannot change the manifest.
    ///
    /// At most one such close is pending. A newer one aborts the older close, whose writer
    /// later epochs fenced. `Writer::stop` waits for the pending close within its limit.
    pub(super) fn abandon(&self, db: Arc<slatedb::Db>) {
        let close = tokio::spawn(async move {
            let options = slatedb::config::CloseOptions { flush_type: None };
            let close = db.close_with_options(options);
            tokio::pin!(close);
            let closed = tokio::select! {
                closed = &mut close => closed,
                _ = tokio::time::sleep(SLOW_CLOSE) => {
                    tracing::warn!("a failed shared writer still closes; its I/O waits");
                    close.await
                }
            };
            if let Err(error) = closed {
                tracing::info!(%error, "a failed shared writer closed with an error");
            }
        });
        let older = self.health.0.close.lock().unwrap().replace(close);
        if let Some(older) = older.filter(|older| !older.is_finished()) {
            tracing::warn!("abort the close of an older failed shared writer");
            older.abort();
            self.health
                .0
                .aborted_closes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Storage work is due: an open, a flush, or a checkpoint. The watchdog starts, unless due
    /// work already runs.
    pub(super) fn due(&self) {
        self.update(|record| {
            record.due.get_or_insert_with(Instant::now);
        });
    }

    /// A durable step of the due work ended. The watchdog starts again for the next step.
    pub(super) fn progressed(&self) {
        self.update(|record| {
            record.due = Some(Instant::now());
            record.stalled = None;
            record.restart = false;
        });
    }

    /// The due storage work ended, or no work is due.
    pub(super) fn done(&self) {
        self.update(Record::done);
    }

    /// The writer created a checkpoint and sent its announcement. This proves the storage path,
    /// also when Raft drops the proposal, so the mark ends and a new count starts.
    pub(super) fn published(&self) {
        self.update(|record| {
            record.published = Some(Instant::now());
            record.mark = None;
            record.consecutive = 0;
            record.since = None;
            record.stall_began = None;
            record.done();
        });
    }

    /// Wait until the node asks this task to drop its stalled attempt. A request that the end
    /// of the stall cleared does not count.
    pub(super) async fn restart_requested(&mut self) {
        loop {
            self.restart.borrow_and_update();
            if self.update(|record| record.restart) == Some(true) {
                return;
            }
            if self.restart.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// The writer health that a member reports in `ViewAck`. An earlier binary sends none.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterReport {
    /// Seconds since the newest fault of the member's writer, while its mark holds.
    pub failed_secs: Option<u64>,
    /// The Raft term that the member saw when it sent the report.
    #[serde(default)]
    pub term: u64,
}

/// A writer report as the leader received it.
#[derive(Clone, Copy, Debug)]
struct Received {
    process: u64,
    at: Instant,
    report: WriterReport,
}

/// A voter that can take over the leadership.
#[derive(Clone, Copy, Debug)]
pub(in crate::node) struct Candidate {
    pub id: u64,
    pub matched: u64,
    /// The process of the voter's last transport frame.
    pub process: Option<u64>,
}

/// How well a voter suits a writer fault handoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Suit {
    /// No current report from this term, an earlier binary, or a mark from this incident.
    No,
    /// A mark older than this leader's last publication. Another writer published after the
    /// voter's fault, so the fault can be over.
    Stale,
    /// A current report without a mark.
    Healthy,
}

/// The leader's handoff state of one writer incident.
#[derive(Default)]
pub(in crate::node) struct Handoff {
    reports: HashMap<u64, Received>,
    /// This member leads since this time. Only reports after it count.
    led: Option<Instant>,
    /// The voters that this incident tried. Each one is tried once.
    tried: HashSet<u64>,
    /// The handoff in flight, with its start.
    pending: Option<(u64, Instant)>,
    /// This incident found no target, and the leader logged it.
    exhausted: bool,
    /// The last publication of this member's writer that the handoff saw. A new one ends the
    /// incident.
    published: Option<Instant>,
}

/// The state of one tick that a writer fault handoff uses.
pub(super) struct Tick<'a> {
    /// The writer is down or stalled. See `Health::handoff_due`.
    pub due: bool,
    pub stalled: bool,
    /// The target of the Raft leader transfer in flight.
    pub transferee: Option<u64>,
    /// The longest time that a handoff can take.
    pub wait: Duration,
    pub candidates: &'a [Candidate],
    /// The last publication of this member's writer.
    pub published: Option<Instant>,
    /// The start of the incident. Only a report from after it counts.
    pub incident: Option<Instant>,
    pub term: u64,
    /// The targets of MoveLeader requests in flight. The handoff never aborts their transfer.
    pub moves: &'a [u64],
}

/// What the node does after a tick of a writer fault handoff.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Decision {
    /// Abort the Raft leader transfer that did not complete.
    pub abort: bool,
    /// Transfer the leadership to this voter.
    pub transfer: Option<u64>,
    /// Drop the stalled writer attempt and open the writer again.
    pub restart: bool,
}

impl Handoff {
    /// Note the writer report of `member` from process `process`. An earlier binary sends no
    /// report, and its process is not a target.
    pub(super) fn note(&mut self, member: u64, process: Option<u64>, report: Option<WriterReport>) {
        match (process, report) {
            (Some(process), Some(report)) => {
                let at = Instant::now();
                self.reports.insert(
                    member,
                    Received {
                        process,
                        at,
                        report,
                    },
                );
            }
            _ => {
                self.reports.remove(&member);
            }
        }
    }

    /// This member starts or stops leading. Raft drops a transfer at a role change, so the
    /// incident ends.
    pub(super) fn lead(&mut self, leading: bool) {
        self.led = leading.then(Instant::now);
        self.tried.clear();
        self.pending = None;
        self.exhausted = false;
    }

    /// How a voter suits a handoff: its report came in this leadership, from its current
    /// process, and holds no mark from after this member's last publication.
    fn suit(&self, candidate: &Candidate, published: Option<Instant>) -> Suit {
        let Some(received) = self.reports.get(&candidate.id) else {
            return Suit::No;
        };
        let current = candidate.process == Some(received.process);
        if !current || self.led.is_none_or(|led| received.at < led) {
            return Suit::No;
        }
        let Some(failed) = received.report.failed_secs else {
            return Suit::Healthy;
        };
        // The latest time of the fault. A delayed report makes it later, never earlier.
        let fault = received.at.checked_sub(Duration::from_secs(failed));
        match (fault, published) {
            (Some(fault), Some(published)) if fault < published => Suit::Stale,
            _ => Suit::No,
        }
    }

    /// Pick the target of a writer fault handoff: a healthy voter before a stale mark, and then
    /// the longest matched log. The voter's report must come from after the start of the
    /// incident and from the current Raft term, so a voter that stopped or faulted since its
    /// last report is not a target. A voter that this incident tried is not a target either.
    fn pick(&self, tick: &Tick) -> Option<u64> {
        let fresh = |id: &u64| {
            self.reports.get(id).is_some_and(|received| {
                received.report.term == tick.term
                    && tick.incident.is_none_or(|incident| received.at >= incident)
            })
        };
        tick.candidates
            .iter()
            .filter(|candidate| !self.tried.contains(&candidate.id) && fresh(&candidate.id))
            .map(|candidate| (self.suit(candidate, tick.published), candidate))
            .filter(|(suit, _)| *suit != Suit::No)
            .max_by_key(|(suit, candidate)| (*suit, candidate.matched))
            .map(|(_, candidate)| candidate.id)
    }

    /// Decide the handoff of one tick on the leader.
    ///
    /// Only a publication of this member's writer ends an incident, so a writer that opens and
    /// fails again does not try a voter twice. The handoff aborts only its own transfer that
    /// Raft still holds after `tick.wait`, so Raft takes proposals again. With no target left,
    /// a stalled writer drops its attempt and opens again.
    pub(super) fn step(&mut self, tick: &Tick) -> Decision {
        let mut decision = Decision::default();
        if self.published != tick.published {
            self.published = tick.published;
            self.tried.clear();
            self.exhausted = false;
        }
        // Raft ends a transfer that does not complete within an election timeout. The record
        // follows Raft, so a later transfer to the same voter is not this one.
        if let Some((target, started)) = self.pending {
            if tick.transferee != Some(target) || tick.moves.contains(&target) {
                self.pending = None;
            } else if started.elapsed() < tick.wait {
                return decision;
            } else {
                tracing::warn!(target, "the writer handoff did not complete");
                decision.abort = true;
                self.pending = None;
            }
        }
        if !tick.due {
            return decision;
        }
        // A MoveLeader or a snapshot wait can hand over first.
        if tick.transferee.is_some() && !decision.abort {
            return decision;
        }
        match self.pick(tick) {
            Some(target) => {
                tracing::warn!(target, "hand over leadership after a shared writer fault");
                self.tried.insert(target);
                self.pending = Some((target, Instant::now()));
                decision.transfer = Some(target);
            }
            None => {
                if !self.exhausted {
                    self.exhausted = true;
                    tracing::error!(
                        "no healthy voter can take over the shared writer; keep the lead and retry"
                    );
                }
                decision.restart = tick.stalled;
            }
        }
        decision
    }

    pub(super) fn status(&self) -> Value {
        let mut tried: Vec<_> = self.tried.iter().copied().collect();
        tried.sort_unstable();
        json!({
            "target": self.pending.map(|(target, _)| target),
            "tried": tried,
        })
    }
}

/// Order voters for a handoff that is not a writer fault. A voter that suits a writer handoff
/// comes first, then the longest matched log.
fn rank(
    handoff: Option<&Handoff>,
    published: Option<Instant>,
    candidates: &[Candidate],
) -> Option<u64> {
    candidates
        .iter()
        .max_by_key(|candidate| {
            let suit = handoff.map_or(Suit::No, |handoff| handoff.suit(candidate, published));
            (suit, candidate.matched)
        })
        .map(|candidate| candidate.id)
}

impl Writer {
    /// The time since work that the publisher has not started became due: the oldest unflushed
    /// write at its age limit, or an explicit publication request. A writer that is not open
    /// holds the progress of an earlier one, so only an open writer counts.
    pub(super) fn waiting(&self) -> Option<Instant> {
        if self.open.lock().unwrap().is_none() {
            return None;
        }
        let aged = self.progress.deadline(self.max_age);
        aged.into_iter().chain(self.progress.requested()).min()
    }
}

impl Node {
    /// The voters other than this member, with their matched log and transport process.
    pub(in crate::node) fn handoff_candidates(&self) -> Vec<Candidate> {
        let id = self.options.id;
        let voters = self.raw.raft.prs().conf().voters();
        self.raw
            .raft
            .prs()
            .iter()
            .filter(|(&peer, _)| peer != id && voters.contains(peer))
            .map(|(&peer, progress)| Candidate {
                id: peer,
                matched: progress.matched,
                process: self.raw.store().process(peer),
            })
            .collect()
    }

    /// Pick the target of a handoff that is not a writer fault: a voter that suits a writer
    /// handoff first, then the longest matched log. No voter is excluded, so replicated mode and
    /// a cluster of earlier binaries keep the longest matched log.
    pub(in crate::node) fn rank_voters(&self) -> Option<u64> {
        let candidates = self.handoff_candidates();
        let writer = self.writer.as_ref();
        let published = writer.and_then(|writer| writer.health.published_at());
        rank(writer.map(|writer| &writer.handoff), published, &candidates)
    }

    /// Check the writer health on each tick. A leader whose writer is down hands over its
    /// leadership to a healthy voter, once per voter and incident. With no target left, it stays
    /// leader and its writer keeps retrying. See `Handoff::step`.
    pub(super) fn tick_writer_health(&mut self) {
        let leader = self.raw.raft.state == StateRole::Leader;
        let candidates = if leader {
            self.handoff_candidates()
        } else {
            vec![]
        };
        let wait = TICK * HANDOFF_TIMEOUTS * self.raw.raft.election_timeout() as u32;
        let (term, transferee) = (self.raw.raft.term, self.raw.raft.lead_transferee);
        let mut moves: Vec<u64> = self.transfers.iter().map(|(target, ..)| *target).collect();
        let Some(writer) = &mut self.writer else {
            return;
        };
        moves.extend(writer.transfer.map(|(target, _)| target));
        if leader {
            writer.health.check(writer.waiting());
        }
        if let Some(gauges) = &writer.gauges {
            let health = &writer.health;
            gauges.failures.set(health.consecutive().into());
            gauges.pending_closes.set(health.pending_closes() as i64);
            let counters = [
                (&gauges.faults, health.total()),
                (&gauges.aborted_closes, health.aborted_closes()),
            ];
            for (counter, total) in counters {
                counter.inc_by(total.saturating_sub(counter.get()));
            }
        }
        if !leader {
            return;
        }
        let health = &writer.health;
        let decision = writer.handoff.step(&Tick {
            due: health.handoff_due(),
            stalled: health.stalled(),
            transferee,
            wait,
            candidates: &candidates,
            published: health.published_at(),
            incident: health.incident(),
            term,
            moves: &moves,
        });
        if decision.restart {
            health.restart();
        }
        if decision.abort {
            self.raw.raft.abort_leader_transfer();
        }
        if let Some(target) = decision.transfer {
            self.raw.transfer_leader(target);
        }
    }
}

#[cfg(test)]
#[path = "fault_tests.rs"]
mod tests;
