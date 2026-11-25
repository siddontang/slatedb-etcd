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
/// A writer that runs this long without a failure starts a new count of failures.
const HEALTHY_RUN: Duration = Duration::from_secs(60);
/// Failures in a row after which the leader hands over its leadership.
const HANDOFF_FAILURES: u32 = 3;
/// Due storage work that makes no durable progress for this time is a fault.
pub(super) const STALL: Duration = Duration::from_secs(30);
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
/// SlateDB reports as a data error is not a storage error. An object store error with no
/// SlateDB error around it comes from this crate's own storage calls.
pub(in crate::node) fn classify(error: &anyhow::Error) -> Fault {
    if fenced(error) || open_conflict(error) {
        return Fault::Fenced;
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
    health: &Health,
    reopens: &std::sync::atomic::AtomicU64,
) -> Result<Duration> {
    match classify(&error) {
        Fault::Fenced => {
            // Two quick elections can open writers in reverse term order.
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
pub(super) fn unreadable_claim(error: &anyhow::Error, health: &Health) -> Duration {
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

/// The health of this member's writer. The writer task and the node share it.
#[derive(Clone, Default)]
pub(in crate::node) struct Health(Arc<Mutex<Record>>);

#[derive(Default)]
struct Record {
    /// Failures since the writer last ran for `HEALTHY_RUN`.
    consecutive: u32,
    /// The first failure of those.
    since: Option<Instant>,
    /// The newest failure, its class, and its error.
    last: Option<(Instant, Fault, String)>,
    /// Faults since the start.
    total: u64,
    /// The writer is open and did not fail since this time.
    running: Option<Instant>,
    /// Storage work is due and made no durable progress since this time.
    due: Option<Instant>,
    /// The due work above made no progress for `STALL`.
    stalled: bool,
    /// The time of the newest fault, until this member's writer publishes again.
    mark: Option<Instant>,
    /// The time when this member's writer last created and announced a checkpoint.
    published: Option<Instant>,
}

impl Health {
    fn record(&self) -> std::sync::MutexGuard<'_, Record> {
        self.0.lock().unwrap()
    }

    /// Record a failure and return the wait before the next open.
    pub(super) fn failed(&self, fault: Fault, error: &str) -> Duration {
        let mut record = self.record();
        let now = Instant::now();
        if record
            .running
            .take()
            .is_some_and(|at| at.elapsed() >= HEALTHY_RUN)
        {
            record.consecutive = 0;
            record.since = None;
        }
        record.consecutive += 1;
        record.since.get_or_insert(now);
        record.last = Some((now, fault, error.to_owned()));
        record.total += 1;
        record.mark = Some(now);
        let (first, cap) = match fault {
            Fault::Data => (DATA_RETRY_FIRST, DATA_RETRY_CAP),
            _ => (RETRY_FIRST, RETRY_CAP),
        };
        let delay = first
            .saturating_mul(1 << (record.consecutive - 1).min(16))
            .min(cap);
        let (class, failures) = (fault.name(), record.consecutive);
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

    /// The writer opened. The publisher ends the due work after its first reads.
    pub(super) fn opened(&self) {
        self.record().running = Some(Instant::now());
    }

    /// Storage work is due: an open, a flush, or a checkpoint. The watchdog starts.
    pub(super) fn due(&self) {
        self.record().due.get_or_insert_with(Instant::now);
    }

    /// The due storage work ended, or no work is due.
    pub(super) fn done(&self) {
        let mut record = self.record();
        record.due = None;
        record.stalled = false;
    }

    /// The writer created and announced a checkpoint. Its mark ends.
    pub(super) fn published(&self) {
        let mut record = self.record();
        record.published = Some(Instant::now());
        record.mark = None;
        drop(record);
        self.done();
    }

    /// Start a new count after a healthy run. Return true when due work just passed `STALL`.
    ///
    /// `overdue` is the time when the oldest unflushed write of the open writer reached its age
    /// limit. It covers a publisher that waits elsewhere and has not started the flush.
    pub(super) fn check(&self, overdue: Option<Instant>) -> bool {
        let mut record = self.record();
        if record.running.is_some_and(|at| at.elapsed() >= HEALTHY_RUN) {
            record.consecutive = 0;
            record.since = None;
        }
        let overdue = overdue.filter(|at| *at <= Instant::now());
        let Some(due) = record.due.into_iter().chain(overdue).min() else {
            return false;
        };
        if record.stalled || due.elapsed() < STALL {
            return false;
        }
        let now = Instant::now();
        record.stalled = true;
        record.total += 1;
        record.mark = Some(now);
        let error = format!("due storage work made no progress for {STALL:?}");
        tracing::error!(%error, "the shared writer stalled");
        record.last = Some((now, Fault::Stalled, error));
        true
    }

    /// Return true when the leader must hand over its leadership: the writer is down after
    /// `HANDOFF_FAILURES` failures in a row or after a data error, or due work stalled.
    pub(super) fn handoff_due(&self) -> bool {
        let record = self.record();
        let down = record.running.is_none()
            && (record.consecutive >= HANDOFF_FAILURES
                || record
                    .last
                    .as_ref()
                    .is_some_and(|(_, fault, _)| *fault == Fault::Data && record.consecutive > 0));
        down || record.stalled
    }

    /// The report of this member in `ViewAck`.
    pub(in crate::node) fn report(&self) -> WriterReport {
        WriterReport {
            failed_secs: self.record().mark.map(|at| at.elapsed().as_secs()),
        }
    }

    /// Clear the watchdog when this member stops leading.
    pub(super) fn reset_watch(&self) {
        self.done();
    }

    /// Start the count of a new term. An earlier term's failures do not hand over this one
    /// before its writer tries. The mark stays until a publication.
    pub(super) fn new_term(&self) {
        let mut record = self.record();
        record.consecutive = 0;
        record.since = None;
        drop(record);
        self.done();
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
            "stalled": record.stalled,
            "marked": record.mark.is_some(),
        })
    }
}

/// The writer health that a member reports in `ViewAck`. An earlier binary sends none.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterReport {
    /// Seconds since the newest fault of the member's writer, while its mark holds.
    pub failed_secs: Option<u64>,
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

    /// This member starts or stops leading. A new term starts a new incident.
    pub(super) fn lead(&mut self, leading: bool) {
        self.led = leading.then(Instant::now);
        self.end();
    }

    /// The incident ended.
    fn end(&mut self) {
        self.tried.clear();
        self.pending = None;
        self.exhausted = false;
    }

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
    /// the longest matched log. A voter that this incident tried is not a target.
    fn pick(&self, candidates: &[Candidate], published: Option<Instant>) -> Option<u64> {
        candidates
            .iter()
            .filter(|candidate| !self.tried.contains(&candidate.id))
            .map(|candidate| (self.suit(candidate, published), candidate))
            .filter(|(suit, _)| *suit != Suit::No)
            .max_by_key(|(suit, candidate)| (*suit, candidate.matched))
            .map(|(_, candidate)| candidate.id)
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
    /// leader and its writer keeps retrying.
    pub(super) fn tick_writer_health(&mut self) {
        let leader = self.raw.raft.state == StateRole::Leader;
        let candidates = if leader {
            self.handoff_candidates()
        } else {
            vec![]
        };
        let wait = TICK * HANDOFF_TIMEOUTS * self.raw.raft.election_timeout() as u32;
        let Some(writer) = &mut self.writer else {
            return;
        };
        // A writer that is not open holds the progress of an earlier one.
        let open = writer.open.lock().unwrap().is_some();
        let overdue = open
            .then(|| writer.progress.deadline(writer.max_age))
            .flatten();
        let stalled = leader && writer.health.check(overdue);
        if let Some(gauges) = &writer.gauges {
            gauges.failures.set(writer.health.consecutive().into());
            let total = writer.health.total();
            gauges
                .faults
                .inc_by(total.saturating_sub(gauges.faults.get()));
        }
        if !leader {
            return;
        }
        let handoff = &mut writer.handoff;
        if !stalled && !writer.health.handoff_due() {
            handoff.end();
            return;
        }
        if let Some((target, started)) = handoff.pending {
            if started.elapsed() < wait {
                return;
            }
            tracing::warn!(
                target,
                "the writer handoff did not complete; try another voter"
            );
            if self.raw.raft.lead_transferee == Some(target) {
                self.raw.raft.abort_leader_transfer();
            }
            handoff.pending = None;
        }
        // A MoveLeader or a snapshot wait can hand over first.
        if self.raw.raft.lead_transferee.is_some() {
            return;
        }
        match handoff.pick(&candidates, writer.health.published_at()) {
            Some(target) => {
                tracing::warn!(target, "hand over leadership after a shared writer fault");
                handoff.tried.insert(target);
                handoff.pending = Some((target, Instant::now()));
                self.raw.transfer_leader(target);
            }
            None if !handoff.exhausted => {
                handoff.exhausted = true;
                tracing::error!(
                    "no healthy voter can take over the shared writer; keep the lead and retry"
                );
            }
            None => {}
        }
    }
}

#[cfg(test)]
#[path = "fault_tests.rs"]
mod tests;
