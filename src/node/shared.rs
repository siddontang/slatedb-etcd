//! Run the shared-mode leader tasks: the materializer and the checkpoint publisher.
//!
//! See `docs/shared-state.md`. The node starts both tasks when it becomes the leader and stops
//! them when it steps down. Phase 1 keeps the writer `Db` open across terms, because a one-voter
//! cluster has no other writer. Phase 3 opens the writer at each leader change and adds fencing.

use super::*;
use std::sync::{Arc, Mutex};

use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions};

use std::collections::HashSet;

use crate::mvcc::{KvError, Outcome};
use crate::state::overlay::Queue;
use crate::state::shared::{view_checkpoints, view_name, Handles, Source};

/// The newest checkpoints that the publisher keeps. It also keeps every newer checkpoint than
/// the oldest one that a live base pins.
const KEEP_CHECKPOINTS: usize = 2;
/// Batches that the materializer takes from the queue at once.
const MATERIALIZE_BATCHES: usize = 64;

/// Progress that the materializer, the publisher, and flush waiters share.
pub(super) struct Progress {
    inner: Mutex<Inner>,
    /// The sequence of the newest batch in the shared `Db`.
    written: tokio::sync::watch::Sender<u64>,
    wake: tokio::sync::Notify,
}

#[derive(Default)]
struct Inner {
    /// The time of the oldest write that no flush covers.
    dirty_since: Option<Instant>,
    /// Publish a checkpoint after the materializer writes this sequence.
    publish_through: Option<u64>,
    /// Publish a new checkpoint even at an unchanged sequence. Defragment sets this.
    republish: bool,
    /// The time of the oldest request that no publication covered yet.
    requested: Option<Instant>,
    /// The time of the oldest republish that no publication served yet.
    republished: Option<Instant>,
}

impl Progress {
    pub(super) fn new(written: u64) -> Self {
        Self {
            inner: Mutex::default(),
            written: tokio::sync::watch::channel(written).0,
            wake: tokio::sync::Notify::new(),
        }
    }

    pub fn written(&self) -> u64 {
        *self.written.borrow()
    }

    fn wrote(&self, seq: u64) {
        self.written.send_replace(seq);
        let mut inner = self.inner.lock().unwrap();
        let first = inner.dirty_since.is_none();
        inner.dirty_since.get_or_insert_with(Instant::now);
        let requested = inner.publish_through.is_some_and(|through| seq >= through);
        drop(inner);
        // The publisher sleeps until the oldest write reaches its age.
        if first || requested {
            self.wake.notify_one();
        }
    }

    /// Start again after a writer open at `written`. The writes that the age limit tracked
    /// were in the old writer. A publication request and a republish stay: a manual checkpoint
    /// or a Defragment can ask before the new writer opens, or a failed pass gives them back.
    fn reset(&self, written: u64) {
        self.written.send_replace(written);
        self.inner.lock().unwrap().dirty_since = None;
    }

    /// Publish a new checkpoint at the current sequence, for a changed layout.
    fn republish(&self) {
        self.give_back(None, true);
        self.wake.notify_one();
    }

    fn take_republish(&self) -> bool {
        std::mem::take(&mut self.inner.lock().unwrap().republish)
    }

    fn request(&self, through: u64) {
        self.give_back(Some(through), false);
        self.wake.notify_one();
    }

    /// Add a request and a republish without a wake. Each keeps the wait of an earlier one.
    fn give_back(&self, through: Option<u64>, republish: bool) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(through) = through {
            let through = inner
                .publish_through
                .map_or(through, |old| old.max(through));
            inner.publish_through = Some(through);
            inner.requested.get_or_insert_with(Instant::now);
        }
        if republish {
            inner.republish = true;
            inner.republished.get_or_insert_with(Instant::now);
        }
    }

    /// The time of the oldest request or republish that no publication served yet.
    fn requested(&self) -> Option<Instant> {
        let inner = self.inner.lock().unwrap();
        inner.requested.into_iter().chain(inner.republished).min()
    }

    /// A pass ended. A request and a republish keep separate waits. A wait starts again only
    /// when the pass covered that kind of demand and a newer one waits. A pass that covered no
    /// request, for example one above the written batch, keeps the wait of the request.
    fn served(&self, covered: bool, republished: bool) {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        if inner.publish_through.is_none() {
            inner.requested = None;
        } else if covered {
            inner.requested = Some(now);
        }
        if !inner.republish {
            inner.republished = None;
        } else if republished {
            inner.republished = Some(now);
        }
    }

    /// A new writer starts the wait of the pending requests, so it has a whole interval to
    /// reach them.
    fn rearm(&self) {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        inner.requested = inner.publish_through.map(|_| now);
        inner.republished = inner.republish.then_some(now);
    }

    /// Wait until the shared `Db` holds every batch through `seq`.
    #[cfg(test)]
    pub async fn wait_written(&self, seq: u64) -> Result<()> {
        let mut written = self.written.subscribe();
        written.wait_for(|written| *written >= seq).await?;
        Ok(())
    }

    /// The time when the oldest unflushed write reaches `max_age`.
    fn deadline(&self, max_age: Duration) -> Option<Instant> {
        Some(self.inner.lock().unwrap().dirty_since? + max_age)
    }

    /// Return whether a flush is due, and the request that is one of its causes. Clear the
    /// causes that it covers.
    fn take_flush(&self, max_age: Duration) -> (bool, Option<u64>) {
        let written = self.written();
        let mut inner = self.inner.lock().unwrap();
        let requested = inner.publish_through.filter(|through| written >= *through);
        let aged = inner
            .dirty_since
            .is_some_and(|since| since.elapsed() >= max_age);
        if requested.is_some() {
            inner.publish_through = None;
        }
        if requested.is_some() || aged {
            inner.dirty_since = None;
        }
        (requested.is_some() || aged, requested)
    }
}

/// The publication demand that one publisher pass took: a request and a republish. A pass that
/// fails or stops before its publication gives the demand back, so the next writer serves it.
struct Demand<'a> {
    progress: &'a Progress,
    through: Option<u64>,
    republish: bool,
}

impl Demand<'_> {
    /// The pass published, or found nothing to publish.
    fn served(mut self) {
        let covered = self.through.take().is_some();
        let republished = std::mem::take(&mut self.republish);
        self.progress.served(covered, republished);
    }

    /// Give the demand back without a wake, for a later pass at the retry deadline.
    fn requeue(mut self) {
        let through = self.through.take();
        let republish = std::mem::take(&mut self.republish);
        self.progress.give_back(through, republish);
    }
}

impl Drop for Demand<'_> {
    fn drop(&mut self) {
        if let Some(through) = self.through {
            self.progress.request(through);
        }
        if self.republish {
            self.progress.republish();
        }
    }
}

/// Reports of pinned checkpoints by member. Each one protects its checkpoints from retention
/// for `REPORT_LIFETIME`.
type Reports = Arc<Mutex<HashMap<u64, (Instant, HashSet<uuid::Uuid>)>>>;

/// A new leader deletes no checkpoint for this time. Each member reports twice in it, so the
/// leader knows the pins of the members before it deletes.
const RETENTION_GRACE: Duration = Duration::from_secs(2 * REPORT_TICKS * 100 / 1000);

/// A member's report protects its checkpoints for this time after it arrives.
const REPORT_LIFETIME: Duration = Duration::from_secs(10 * 60);

/// The shared-mode tasks of one member. Only the leader runs the writer.
pub(super) struct Writer {
    pub(super) admin: Arc<slatedb::admin::Admin>,
    queue: Arc<Queue>,
    pub(super) source: Arc<Source>,
    progress: Arc<Progress>,
    max_age: Duration,
    /// Set while this member leads. Sending `true` closes the writer.
    stop: Option<tokio::sync::watch::Sender<bool>>,
    /// Writer tasks with the term that started each one.
    tasks: tokio::task::JoinSet<(u64, Result<()>)>,
    reports: Reports,
    /// `ViewCheckpoint` entries that this node proposed.
    pub announced: u64,
    /// The applied base and announcement. The publisher announces a stranded checkpoint.
    applied: Applied,
    /// The `F` of the newest base that opened. A change starts a switch job.
    opened: tokio::sync::watch::Receiver<u64>,
    opened_slot: Arc<crate::state::shared::Opened>,
    /// A switch job is due.
    pub switch_due: bool,
    /// Apply waits while the overlay is over the hard limit.
    pub paused: bool,
    /// The last opened base that an adoption dropped. See `rejected`.
    rejected: rejected::Rejected,
    /// A paused member polls the checkpoint list. Only one poll runs at a time.
    polling: Arc<std::sync::atomic::AtomicBool>,
    ticks: u64,
    /// The role and term of this member. The writer task checks them after its open.
    leadership: tokio::sync::watch::Sender<(bool, u64)>,
    /// The term of the running writer task.
    term: u64,
    pub(super) clock: Clock,
    /// A leader transfer waits for a checkpoint that covers this batch sequence.
    pub(super) transfer: Option<(u64, u64)>,
    /// Writer opens after a fence while this member still led.
    reopens: Arc<std::sync::atomic::AtomicU64>,
    pub(super) retention: crate::state::ViewRetention,
    pub(super) retained: Option<prometheus::IntGauge>,
    /// The writer `Db` while this member leads.
    open: OpenWriter,
    /// Tests: the writer task notifies the first and waits for the second after it publishes
    /// an open writer.
    #[cfg(test)]
    pub(super) open_hold: Hold,
    pub(super) gauges: Option<Gauges>,
    /// The highest manual checkpoint index that requested a publication after apply.
    checkpoint_requested: u64,
    /// A follower's manual checkpoint index that waits for apply on this leader.
    remote_checkpoint: u64,
    /// The highest index that a follower's manual checkpoint needs a base to cover.
    demand: u64,
    /// The coverage and the tick of the last certification that this leader proposed.
    certified: Option<(u64, u64)>,
    /// The batch through which this leader last asked for a publication to serve a demand.
    demand_publication: u64,
    /// Ticks of `certify_base`.
    certify_ticks: u64,
    /// The leader term of the demand above. A new term drops the demand of the old one.
    demand_term: u64,
    /// The applied index through which this leader asked for a checkpoint at its row.
    exact_through: u64,
    /// Apply stores the apply marker at the applied index, for a checkpoint at its row.
    mark_row_due: bool,
    /// The highest record index whose checkpoint apply offered as the base. See `take_raise`.
    raised_record: u64,
    /// DST: skip that offer, as when the checkpoint cannot open.
    #[cfg(test)]
    pub(super) skip_raise: bool,
    /// The health of this member's writer. See `fault`.
    pub(super) health: fault::Health,
    /// A batch waits for the clock check since this time. See `Writer::note_clock_wait`.
    pub(super) clock_due: Option<Instant>,
    /// The writer reports of the voters, and the handoff of a writer incident.
    pub(super) handoff: fault::Handoff,
    /// The write proofs of the learners that this leader promotes. See `write_proof`.
    pub(super) proofs: write_proof::Proofs,
    /// The write probes of this member. The node sets it at its open.
    pub(super) prober: Option<Arc<Prober>>,
}

/// The shared-mode gauges of one member. The node sets them on each tick.
pub(super) struct Gauges {
    base: prometheus::IntGauge,
    overlay: prometheus::IntGauge,
    paused: prometheus::IntGauge,
    leading: prometheus::IntGauge,
    reopens: prometheus::IntGauge,
    clock_alarm: prometheus::IntGauge,
    clock_waiting: prometheus::IntGauge,
    pub(super) failures: prometheus::IntGauge,
    pub(super) faults: prometheus::IntCounter,
    pub(super) pending_closes: prometheus::IntGauge,
    pub(super) aborted_closes: prometheus::IntCounter,
}

impl Gauges {
    pub(super) fn register(registry: &prometheus::Registry) -> Result<Self> {
        let gauge = |name: &str, help: &str| -> Result<prometheus::IntGauge> {
            let gauge = prometheus::IntGauge::new(name, help)?;
            registry.register(Box::new(gauge.clone()))?;
            Ok(gauge)
        };
        let counter = |name: &str, help: &str| -> Result<prometheus::IntCounter> {
            let counter = prometheus::IntCounter::new(name, help)?;
            registry.register(Box::new(counter.clone()))?;
            Ok(counter)
        };
        Ok(Self {
            base: gauge(
                "slatedb_etcd_shared_base_applied_index",
                "Raft index of the view checkpoint that this member reads as its base.",
            )?,
            overlay: gauge(
                "slatedb_etcd_shared_overlay_bytes",
                "Bytes of applied batches in the overlay above the base.",
            )?,
            paused: gauge(
                "slatedb_etcd_shared_apply_paused",
                "1 while apply waits for a base switch above twice the overlay limit.",
            )?,
            leading: gauge(
                "slatedb_etcd_shared_writer_running",
                "1 while the shared writer database of this member is open.",
            )?,
            clock_alarm: gauge(
                "slatedb_etcd_shared_writer_clock_alarm",
                "1 while the local clock is too far behind the writer clock or ahead of a voter \
                 majority.",
            )?,
            clock_waiting: gauge(
                "slatedb_etcd_shared_writer_clock_waiting",
                "1 while the writer waits for the clock samples of a voter majority or for its \
                 clock bound.",
            )?,
            reopens: gauge(
                "slatedb_etcd_shared_writer_reopens",
                "Writer opens after a fence or a lost manifest race while this member led.",
            )?,
            failures: gauge(
                "slatedb_etcd_shared_writer_consecutive_failures",
                "Shared writer failures in a row on this member, other than fences.",
            )?,
            faults: counter(
                "slatedb_etcd_shared_writer_faults_total",
                "Shared writer faults on this member: errors, clock refusals, and stalls.",
            )?,
            pending_closes: gauge(
                "slatedb_etcd_shared_writer_pending_closes",
                "1 while the close of a failed shared writer waits for its I/O.",
            )?,
            aborted_closes: counter(
                "slatedb_etcd_shared_writer_aborted_closes_total",
                "Closes of failed shared writers that the close of a newer one aborted.",
            )?,
        })
    }
}

impl Writer {
    pub fn new(handles: &Handles, max_age: Duration, clock: Clock) -> Self {
        Self {
            leadership: tokio::sync::watch::channel((false, 0)).0,
            term: 0,
            clock,
            transfer: None,
            reopens: Arc::default(),
            retention: crate::state::ViewRetention::Off,
            retained: None,
            open: OpenWriter::default(),
            #[cfg(test)]
            open_hold: Hold::default(),
            gauges: None,
            checkpoint_requested: 0,
            remote_checkpoint: 0,
            demand: 0,
            certified: None,
            demand_publication: 0,
            certify_ticks: 0,
            demand_term: 0,
            exact_through: 0,
            mark_row_due: false,
            raised_record: 0,
            #[cfg(test)]
            skip_raise: false,
            health: fault::Health::default(),
            clock_due: None,
            handoff: fault::Handoff::default(),
            proofs: write_proof::Proofs::default(),
            prober: None,
            progress: Arc::new(Progress::new(0)),
            admin: handles.admin.clone(),
            queue: handles.queue.clone(),
            source: handles.source.clone(),
            max_age,
            stop: None,
            tasks: tokio::task::JoinSet::new(),
            reports: Reports::default(),
            announced: 0,
            applied: Applied {
                based: handles.based.clone(),
                announced: handles.announced.clone(),
                base_id: handles.base_id.clone(),
                #[cfg(test)]
                failures: Arc::default(),
            },
            opened: handles.opened.subscribe(),
            opened_slot: handles.opened.clone(),
            switch_due: false,
            paused: false,
            rejected: Default::default(),
            polling: Arc::default(),
            ticks: 0,
        }
    }

    pub fn running(&self) -> bool {
        self.stop.is_some()
    }

    /// Open the writer on the leader of `term`. Close it on any other member.
    ///
    /// The open fences an older writer. A task of an older term stops before the new one starts.
    pub fn sync(&mut self, leader: bool, term: u64, events: &mpsc::Sender<Event>) {
        self.leadership.send_replace((leader, term));
        if !leader || self.term != term {
            if let Some(stop) = self.stop.take() {
                stop.send_replace(true);
            }
            self.health.end();
            self.handoff.lead(false);
            self.clock_due = None;
            if !leader {
                self.clock.stand_down();
            }
            self.transfer = None;
            self.demand = 0;
            self.certified = None;
            self.demand_publication = 0;
        }
        if !leader || self.running() {
            return;
        }
        self.term = term;
        let health = self.health.begin();
        self.handoff.lead(true);
        let (stop, stopped) = tokio::sync::watch::channel(false);
        self.stop = Some(stop);
        let task = lead(
            Publisher {
                admin: self.admin.clone(),
                source: self.source.clone(),
                progress: self.progress.clone(),
                max_age: self.max_age,
                reports: self.reports.clone(),
                retention: self.retention,
                retained: self.retained.clone(),
                applied: self.applied.clone(),
                health: health.clone(),
            },
            Leading {
                queue: self.queue.clone(),
                events: events.clone(),
                stopped,
                term,
                leadership: self.leadership.subscribe(),
                clock: self.clock.clone(),
                reopens: self.reopens.clone(),
                open: self.open.clone(),
                #[cfg(test)]
                open_hold: self.open_hold.clone(),
                health,
            },
        );
        self.tasks.spawn(async move { (term, task.await) });
    }

    pub async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.send_replace(true);
        }
        // A clean stop closes the writer. A stuck close must not hold the shutdown, also not
        // the close of a failed writer. One deadline covers both.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let joined = async { while self.tasks.join_next().await.is_some() {} };
        if tokio::time::timeout_at(deadline, joined).await.is_err() {
            self.tasks.shutdown().await;
        }
        // A task can abandon a failed writer until it ends, so the close comes after the join.
        if let Some(mut failed) = self.health.take_close() {
            if tokio::time::timeout_at(deadline, &mut failed)
                .await
                .is_err()
            {
                failed.abort();
            }
        }
    }

    /// Request the publication for a manual checkpoint through `index` once apply reaches it.
    /// Return true when this call requested it. Each index is requested once.
    pub fn request_checkpoint(&mut self, index: u64, applied: u64) -> bool {
        if index > applied || index <= self.checkpoint_requested {
            return false;
        }
        self.checkpoint_requested = index;
        self.request_publication(index);
        true
    }

    /// Publish a checkpoint that holds the applied state through `index`.
    pub fn request_publication(&self, index: u64) {
        self.progress.request(self.covering(index));
    }

    /// DST: take the batch through which a publication is due. The DST writer steps publish
    /// without it.
    #[cfg(test)]
    pub(super) fn take_publish_through(&self) -> Option<u64> {
        self.progress.inner.lock().unwrap().publish_through.take()
    }

    /// The batch sequence that holds the state through `index`. A later no-op entry has no batch,
    /// and a batch that the base released holds no change above the base row.
    pub fn covering(&self, index: u64) -> u64 {
        let base = *self.applied.base_id.borrow();
        index.min(demand::last_change(base, self.queue.position()))
    }

    /// Record the checkpoints that `member` pins. The publisher keeps them.
    pub fn report(&self, member: u64, checkpoints: HashSet<uuid::Uuid>) {
        self.reports
            .lock()
            .unwrap()
            .insert(member, (Instant::now(), checkpoints));
    }

    pub fn status(&self) -> Value {
        json!({
            "written": self.progress.written(),
            "queued_batches": self.queue.len(),
            "announced": self.announced,
            "running": self.running(),
            "paused": self.paused,
            "rejected_base": self.rejected.status(),
            "clock_skew_ms": self.clock.skew.load(std::sync::atomic::Ordering::Relaxed),
            "clock_alarm": self.clock.alarm_name(),
            "clock": self.clock.status(),
            "reopens": self.reopens.load(std::sync::atomic::Ordering::Relaxed),
            "reports": self.reports.lock().unwrap().len(),
            "health": self.health.status(),
            "handoff": self.handoff.status(),
            "write_proofs": self.proofs.status(Instant::now()),
        })
    }
}

/// The checkpoints that recent reports pin.
fn reported(reports: &Reports) -> HashSet<uuid::Uuid> {
    let mut reports = reports.lock().unwrap();
    reports.retain(|_, (at, _)| at.elapsed() < REPORT_LIFETIME);
    reports
        .values()
        .flat_map(|(_, ids)| ids.iter().copied())
        .collect()
}

/// An event of the shared-mode tasks.
pub(super) enum WriterEvent {
    /// A writer task failed on an invariant of this member. The node stops.
    Failed(anyhow::Error),
    /// A newer base opened. A switch job is due.
    Opened,
}

/// Wait for the next writer event. A member without shared state waits forever. A stop that
/// the member requested is not a failure.
pub(super) async fn next(writer: &mut Option<Writer>) -> WriterEvent {
    let Some(writer) = writer else {
        return std::future::pending().await;
    };
    let leading = writer.stop.is_some();
    let current = writer.term;
    let failed = async {
        loop {
            match writer.tasks.join_next().await {
                // A task of an older term, or of a term that this member left, is obsolete. A
                // newer writer can fence it before it sees its stop.
                Some(Ok((term, result))) if !leading || term != current => {
                    if let Err(error) = result {
                        tracing::warn!(%error, term, "a stopped shared writer failed");
                    }
                    continue;
                }
                Some(Ok((_, Ok(())))) => {
                    return anyhow::anyhow!("a shared state writer task stopped")
                }
                Some(Ok((_, Err(error)))) => return error.context("shared state writer failed"),
                Some(Err(error)) if error.is_cancelled() => continue,
                Some(Err(error)) => {
                    return anyhow::Error::from(error).context("shared state writer stopped")
                }
                None => return std::future::pending().await,
            }
        }
    };
    tokio::select! {
        error = failed => WriterEvent::Failed(error),
        changed = writer.opened.changed() => {
            if changed.is_err() {
                return std::future::pending().await;
            }
            writer.switch_due = true;
            WriterEvent::Opened
        }
    }
}

/// The inputs of one leader term's writer task.
struct Leading {
    queue: Arc<Queue>,
    events: mpsc::Sender<Event>,
    stopped: tokio::sync::watch::Receiver<bool>,
    term: u64,
    leadership: tokio::sync::watch::Receiver<(bool, u64)>,
    clock: Clock,
    reopens: Arc<std::sync::atomic::AtomicU64>,
    open: OpenWriter,
    #[cfg(test)]
    open_hold: Hold,
    health: fault::TaskHealth,
}

/// The writer `Db` while the writer task runs, with an admin on its SlateDB clock. Defragment
/// flushes the `Db` and submits its compaction through the admin.
///
/// SlateDB refuses a compaction job whose ID time is below the newest job, and an output SST
/// whose ID time is below its job. The compactor of the writer takes both times from the gate
/// of the writer, so the job takes its ID from that gate too. One slot holds the pair, so no
/// reader sees a `Db` without its admin.
#[derive(Clone)]
pub(super) struct Open {
    pub(super) db: Arc<slatedb::Db>,
    pub(super) admin: Arc<slatedb::admin::Admin>,
    pub(super) gate: Arc<clock::Gate>,
}

type OpenWriter = Arc<Mutex<Option<Open>>>;

/// Tests: a pause of the writer task. See `Writer::open_hold`.
#[cfg(test)]
pub(super) type Hold = Arc<Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>;

/// Wait until the member asks the writer task to stop. The result holds no borrow, so the task
/// stays `Send` across later waits.
async fn stop_requested(stopped: &mut tokio::sync::watch::Receiver<bool>) {
    let _ = stopped.wait_for(|stop| *stop).await;
}

/// The end of one step of the writer task.
enum Attempt<T> {
    Done(T),
    /// The member asked the writer task to stop.
    Stopped,
    /// The node dropped the step after a stall with no handoff target.
    Restarted,
}

/// Run `work` until it ends, the member stops the writer task, or the node restarts the stalled
/// step. A dropped step can leave I/O in flight. The next open fences it.
async fn attempt<T>(
    work: impl std::future::Future<Output = T>,
    stopped: &mut tokio::sync::watch::Receiver<bool>,
    health: &mut fault::TaskHealth,
) -> Attempt<T> {
    tokio::select! {
        value = work => Attempt::Done(value),
        _ = stop_requested(stopped) => Attempt::Stopped,
        _ = health.restart_requested() => Attempt::Restarted,
    }
}

/// Open the writer, then materialize and publish until the member steps down.
///
/// 1. The open increments the writer epoch, which fences an older writer.
/// 2. After the open, the member must still lead the same term. Otherwise it closes the writer.
/// 3. The local clock must pass the manifest's `last_l0_clock_tick` and the time of its newest
///    L0 SST ID. Over the skew limit the writer does not start, and the task tries again after
///    a backoff. Before the open, a leader of several voters waits for the clock samples of a
///    majority. Each batch then gets a tick at or below the bound of `clock::Gate`. A leader
///    clock ahead of a majority fails the materializer with a clock fault.
/// 4. The materializer and the publisher start. After an error while this member still leads,
///    `fault::recover` decides. A fence opens the writer again at once. A storage or data error
///    abandons the writer, and the task opens it again after a backoff. The overlay keeps every
///    batch, so the new writer writes them again. Apply, replies, and reads continue. Only an
///    error of this member's own invariants stops the member.
///
/// Each step that reads or writes storage also ends when the node restarts a stalled writer.
/// The writer then opens again after `fault::STALL` and the backoff, also when the stalled I/O
/// never returns.
async fn lead(publisher: Publisher, leading: Leading) -> Result<()> {
    let Leading {
        queue,
        events,
        mut stopped,
        term,
        leadership,
        clock,
        reopens,
        open,
        #[cfg(test)]
        open_hold,
        mut health,
    } = leading;
    let current = || *leadership.borrow() == (true, term);
    let mut delay = Duration::ZERO;
    // An open that a fence or a manifest race failed. The open step keeps its timer until an
    // open succeeds, because these retries are no durable progress.
    let mut fenced = false;
    loop {
        if !delay.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = stop_requested(&mut stopped) => return Ok(()),
            }
        }
        // A leader of several voters waits for the clock samples of a majority before its
        // open. The wait is no writer fault, so no step timer runs. See `Clock::gate`.
        let gate = match attempt(clock.gate(), &mut stopped, &mut health).await {
            Attempt::Done(gate) => gate,
            Attempt::Stopped => return Ok(()),
            Attempt::Restarted => {
                delay = health.restarted();
                continue;
            }
        };
        // An ordinary start bootstraps an empty state only after it claims the one bootstrap
        // operation. A pending snapshot restore holds the claim, so the leader waits for it.
        // Each member meets the same wait, so it is not a writer fault. The reads are.
        health.due();
        let checked = attempt(bootstrap_wait(&publisher.source), &mut stopped, &mut health);
        match checked.await {
            // The open is a new step with its own timer, unless it retries a fenced open.
            Attempt::Done(Ok(None)) if !fenced => health.progressed(),
            Attempt::Done(Ok(None)) => {}
            Attempt::Done(Ok(Some(wait))) => {
                health.done();
                tracing::warn!(reason = %wait, "the shared writer waits for the bootstrap");
                delay = Duration::from_secs(1);
                continue;
            }
            Attempt::Done(Err(error)) => {
                delay = fault::unreadable_claim(&error, &health);
                continue;
            }
            Attempt::Stopped => return Ok(()),
            Attempt::Restarted => {
                delay = health.restarted();
                continue;
            }
        }
        // The gate starts at the floor of the stored state, so the compactor and the flushes of
        // the new `Db` take no ID time below it. See `clock::stored_floor`. The behind check of
        // that floor comes first, so a floor far ahead never reaches the gate or SlateDB.
        let opening = async {
            let stored = clock::stored_floor(&publisher.admin).await?;
            if !clock.pass(stored).await {
                return Ok(None);
            }
            gate.floor(stored);
            let db = publisher.source.open_writer_on(gate.clone()).await?;
            anyhow::Ok(Some((db, stored)))
        };
        let opened = attempt(opening, &mut stopped, &mut health);
        let (db, stored) = match opened.await {
            Attempt::Done(Ok(None)) => {
                delay = health.failed(
                    fault::Fault::Clock,
                    "the local clock is behind the stored writer clock",
                );
                continue;
            }
            Attempt::Done(Ok(Some((db, stored)))) => {
                // The open wrote the manifest. The next step has its own timer.
                health.progressed();
                fenced = false;
                (Arc::new(db), stored)
            }
            // A newer open fenced this one while the member still leads the same term.
            // Two opens can also race for the same manifest version. The open that loses must
            // read the new manifest and try again.
            Attempt::Done(Err(error)) if current() && !*stopped.borrow() => {
                fenced = fault::classify(&error) == fault::Fault::Fenced;
                delay = fault::recover(error, term, &health, &reopens)?;
                continue;
            }
            Attempt::Done(Err(error)) => return Err(error),
            Attempt::Stopped => return Ok(()),
            Attempt::Restarted => {
                delay = health.restarted();
                continue;
            }
        };
        let close = |db: Arc<slatedb::Db>| async move {
            // A writer that a newer writer fenced cannot close cleanly. GC closes with it.
            if let Err(error) = db
                .close_with_options(slatedb::config::CloseOptions { flush_type: None })
                .await
            {
                tracing::warn!(%error, "cannot close the shared writer");
            }
        };
        // Apply and replies continued during the open. A later election ends this term.
        if !current() {
            close(db).await;
            return Ok(());
        }
        // The floor also holds the time of the newest L0 SST ID. See `clock::floor`.
        let tick = clock::floor(&db.manifest()).max(stored);
        let passed = match attempt(clock.pass(tick), &mut stopped, &mut health).await {
            Attempt::Done(passed) => passed,
            Attempt::Stopped => {
                close(db).await;
                return Ok(());
            }
            Attempt::Restarted => {
                health.abandon(db);
                delay = health.restarted();
                continue;
            }
        };
        if !passed {
            // The fault counts at once. The close of this writer can wait on its I/O.
            delay = health.failed(
                fault::Fault::Clock,
                "the local clock is behind the shared writer clock",
            );
            health.abandon(db);
            continue;
        }
        gate.floor(tick);
        health.opened();
        // The open starts at `last_l0_seq`. Unflushed writes of an older writer are lost, and
        // the overlay holds them.
        publisher.progress.reset(db.manifest().last_l0_seq());
        publisher.progress.rearm();
        let admin = slatedb::admin::Admin::builder(
            publisher.source.state(),
            publisher.source.store.clone(),
        )
        .with_system_clock(gate.clone())
        .build();
        *open.lock().unwrap() = Some(Open {
            db: db.clone(),
            admin: Arc::new(admin),
            gate: gate.clone(),
        });
        #[cfg(test)]
        {
            let hold = open_hold.lock().unwrap().clone();
            if let Some((reached, resume)) = hold {
                reached.notify_one();
                resume.notified().await;
            }
        }
        let progress = publisher.progress.clone();
        let running = async {
            tokio::select! {
                result = materialize(db.clone(), queue.clone(), progress, gate) => result,
                result = publish(publisher.clone(), db.clone(), events.clone()) => result,
            }
        };
        let outcome = attempt(running, &mut stopped, &mut health).await;
        // A later task can own the slot already. Clear only this task's writer.
        {
            let mut slot = open.lock().unwrap();
            if slot.as_ref().is_some_and(|open| Arc::ptr_eq(&open.db, &db)) {
                *slot = None;
            }
        }
        match outcome {
            Attempt::Done(Err(error)) if current() && !*stopped.borrow() => {
                health.abandon(db);
                delay = fault::recover(error, term, &health, &reopens)?;
            }
            Attempt::Done(result) => {
                close(db).await;
                return result;
            }
            Attempt::Stopped => {
                close(db).await;
                return Ok(());
            }
            Attempt::Restarted => {
                health.abandon(db);
                delay = health.restarted();
            }
        }
    }
}

/// Return why the writer cannot open yet: a pending snapshot restore. An unreadable claim is an
/// error.
async fn bootstrap_wait(source: &Source) -> Result<Option<String>> {
    if source.bootstrapped().await? {
        return Ok(None);
    }
    let op = crate::state::shared::BootstrapOp::Empty;
    Ok(
        match crate::state::shared::claim_bootstrap(source, &op).await? {
            crate::state::shared::BootstrapOp::Empty => None,
            crate::state::shared::BootstrapOp::Restore { snapshot } => {
                Some(format!("the restore of snapshot {snapshot} is pending"))
            }
        },
    )
}

/// Copy overlay batches into the shared `Db` in order, each with its own sequence.
///
/// The writer opened at `last_l0_seq`. The batches at or below the written sequence are already
/// in the `Db`. A batch that spans it is written whole: its rows are final values, so the result
/// is the state at its sequence.
///
/// The task wakes past `next`, not past the physical sequence. A base can hold a released batch
/// above that sequence: a member that grouped entries in another way stored a batch where the
/// checkpoint row is lower, and a certified announcement released it. Such a batch changes no
/// state in the base, so the task skips it and waits for a later batch. The physical sequence,
/// which publications use as the checkpoint row, stays where the writes left it.
pub(super) async fn materialize(
    db: Arc<slatedb::Db>,
    queue: Arc<Queue>,
    progress: Arc<Progress>,
    gate: Arc<clock::Gate>,
) -> Result<()> {
    let mut next = progress.written();
    let mut pushed = queue.subscribe();
    loop {
        next = next.max(queue.released());
        pushed.wait_for(|position| position.pushed > next).await?;
        let batches = queue.after(next, MATERIALIZE_BATCHES);
        if batches.is_empty() {
            // A release took the batches after the wait. The next pass reads its index.
            continue;
        }
        for (seq, batch) in batches {
            // The tick of the batch is fixed before SlateDB gets it. See `clock::Gate`.
            let admitted = gate.admit().await;
            #[cfg(test)]
            gate.held().await;
            db.write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
                .await?;
            admitted.done();
            next = seq;
            progress.wrote(seq);
        }
    }
}

#[derive(Clone)]
struct Publisher {
    admin: Arc<slatedb::admin::Admin>,
    source: Arc<Source>,
    progress: Arc<Progress>,
    max_age: Duration,
    reports: Reports,
    retention: crate::state::ViewRetention,
    /// The number of `view-` checkpoints after the last publication.
    retained: Option<prometheus::IntGauge>,
    applied: Applied,
    health: fault::TaskHealth,
}

/// The applied state that a publisher checks before it announces a checkpoint again.
#[derive(Clone)]
struct Applied {
    /// The index through which the current base holds the whole state.
    based: tokio::sync::watch::Receiver<u64>,
    /// The checkpoint of the newest applied `ViewCheckpoint` entry.
    announced: tokio::sync::watch::Receiver<Option<uuid::Uuid>>,
    /// The checkpoint of the current base, its row `F`, and its base index.
    base_id: tokio::sync::watch::Receiver<Option<crate::state::shared::BaseRef>>,
    /// Checks that fail before they read storage, for tests.
    #[cfg(test)]
    failures: Arc<std::sync::atomic::AtomicU32>,
}

/// The first wait before the publisher checks an announcement again.
const RETRY_MIN: Duration = Duration::from_millis(100);
/// The longest wait between two checks.
const RETRY_MAX: Duration = Duration::from_secs(2);

/// The check of announcements that the publisher sent. An announcement can be lost: the node
/// can drop its proposal, for example during a leader transfer, or a writer can stop before it
/// commits. A sent checkpoint counts only after applied state shows it. Until then, the
/// publisher checks again with a backoff from `RETRY_MIN` to `RETRY_MAX`, also on an idle
/// cluster.
struct Retry {
    /// The checkpoint that the publisher announced last, with its row `F`.
    awaiting: Option<(uuid::Uuid, u64)>,
    /// The time of the next check, if one is due.
    at: Option<Instant>,
    backoff: Duration,
}

impl Retry {
    /// A writer checks for a stranded checkpoint at its open.
    fn new() -> Self {
        Self {
            awaiting: None,
            at: Some(Instant::now()),
            backoff: RETRY_MIN,
        }
    }

    /// Check at once, for an explicit request.
    fn now(&mut self) {
        self.at = Some(Instant::now());
    }

    fn due(&self) -> bool {
        self.at.is_some_and(|at| at <= Instant::now())
    }

    /// Check again after the backoff, and double it.
    fn again(&mut self) {
        self.at = Some(Instant::now() + self.backoff);
        self.backoff = (self.backoff * 2).min(RETRY_MAX);
    }

    /// The publisher sent checkpoint `id`. Raft took the proposal if `accepted`. An entry in
    /// the leader's log commits in its term, so the next check waits `RETRY_MAX`. A dropped
    /// proposal is sent again after the backoff, which grows while the same checkpoint waits.
    fn sent(&mut self, id: (uuid::Uuid, u64), accepted: bool) {
        if accepted {
            self.at = Some(Instant::now() + RETRY_MAX);
        } else {
            if self.awaiting != Some(id) {
                self.backoff = RETRY_MIN;
            }
            self.again();
        }
        self.awaiting = Some(id);
    }

    fn done(&mut self) {
        self.awaiting = None;
        self.at = None;
        self.backoff = RETRY_MIN;
    }
}

/// Find a stranded checkpoint: the newest view checkpoint, when no applied announcement covers
/// it. A writer that stops after a checkpoint and before its announcement commits leaves one.
/// With no more writes, no newer checkpoint replaces it, and members wait for its base.
///
/// The newest checkpoint is the last in (index, manifest) order. It is not stranded when it is
/// the newest announced checkpoint or the current base, or when its `F` is below the base.
/// After a Defragment a newer checkpoint at the base index is stranded until it applies. Thus an
/// adopted checkpoint never causes a new announcement, and announcements cannot loop. A
/// checkpoint without membership is not valid and is not stranded.
async fn stranded(
    admin: &slatedb::admin::Admin,
    source: &Source,
    applied: &Applied,
) -> Result<Option<(uuid::Uuid, u64)>> {
    #[cfg(test)]
    if applied
        .failures
        .fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |left| left.checked_sub(1),
        )
        .is_ok()
    {
        anyhow::bail!("an injected storage error");
    }
    // The newest checkpoint in (index, manifest) order. A Defragment adds a newer one at the
    // same index, so the identity counts, not only the index.
    let views = view_checkpoints(admin).await?;
    let Some((_, checkpoint)) = views.last() else {
        return Ok(None);
    };
    if *applied.announced.borrow() == Some(checkpoint.id)
        || applied.base_id.borrow().map(|base| base.checkpoint) == Some(checkpoint.id)
    {
        return Ok(None);
    }
    let base = source.open_base(checkpoint.id).await?;
    if base.applied < applied.base_id.borrow().map_or(0, |base| base.row) {
        return Ok(None);
    }
    if base.get(crate::cluster::KEY).await?.is_none() {
        return Ok(None);
    }
    Ok(Some((checkpoint.id, base.applied)))
}

/// Flush the shared `Db` when its oldest write reaches the age limit or on request. Create one
/// checkpoint for each advance of `last_l0_seq`, and announce it through Raft.
///
/// Only state changes write batches, so an announcement never causes the next one. A request
/// stays until its publication: a failed pass gives it back to the next writer. A `Db` that
/// SlateDB closed, also an idle one, ends the task with its stored error. A retention error
/// only skips that retention.
async fn publish(
    publisher: Publisher,
    db: Arc<slatedb::Db>,
    events: mpsc::Sender<Event>,
) -> Result<()> {
    let Publisher {
        admin,
        source,
        progress,
        max_age,
        reports,
        retention,
        retained,
        applied,
        health,
    } = publisher;
    let led = Instant::now();
    let epoch = db.manifest().writer_epoch();
    let mut published = view_checkpoints(&admin)
        .await?
        .last()
        .map_or(0, |(seq, _)| *seq);
    let mut bootstrapped = source.bootstrapped().await?;
    if !bootstrapped {
        // The first leader publishes the first checkpoint at once. It holds `etcd/members`.
        progress.request(1);
    }
    // The open and the reads above are done.
    health.done();
    // A writer that stopped before its announcement committed can leave a stranded
    // checkpoint. A new writer announces it, so the members do not wait for more writes.
    let mut retry = Retry::new();
    // A stranded check failed. The requests that wait for it retry at the backoff.
    let mut rechecking = false;
    // The `Db` can close before the subscription, so each pass reads the current status.
    let mut status = db.subscribe();
    loop {
        fault::closed(&db, &status).await?;
        let deadline = [progress.deadline(max_age), retry.at]
            .into_iter()
            .flatten()
            .min();
        match deadline {
            Some(deadline) => tokio::select! {
                _ = progress.wake.notified() => {}
                _ = tokio::time::sleep_until(deadline.into()) => {}
                changed = status.changed() => changed?,
            },
            None => tokio::select! {
                _ = progress.wake.notified() => {}
                changed = status.changed() => changed?,
            },
        }
        fault::closed(&db, &status).await?;
        let (flush, requested) = progress.take_flush(max_age);
        let mut demand = Demand {
            progress: &progress,
            through: requested,
            republish: false,
        };
        if flush {
            health.due();
            db.flush().await?;
            health.progressed();
        }
        // SlateDB also flushes a full memtable by itself. Each advance gets one checkpoint.
        let last = db.manifest().last_l0_seq();
        demand.republish = progress.take_republish();
        if last <= published && !demand.republish {
            health.done();
            // A request with no new checkpoint can wait for a stranded one. The request and
            // its timer stay until the check finds the checkpoint or rules it out, so a hung or
            // failing read of the check counts against the request. After a failed check, the
            // retry keeps its backoff.
            if requested.is_some() && !rechecking {
                retry.now();
            }
            if !retry.due() && rechecking {
                demand.requeue();
                continue;
            }
            if retry.due() {
                let checked = reannounce(
                    &admin,
                    &source,
                    &applied,
                    &mut retry,
                    &mut bootstrapped,
                    &events,
                );
                match checked.await? {
                    Recheck::Done => rechecking = false,
                    Recheck::Failed => {
                        rechecking = true;
                        demand.requeue();
                        continue;
                    }
                    Recheck::Closed => return Ok(()),
                }
            }
            demand.served();
            continue;
        }
        health.due();
        let options = CheckpointOptions {
            name: Some(view_name(last)),
            ..Default::default()
        };
        let checkpoint = db
            .create_checkpoint(CheckpointScope::Durable, &options)
            .await?;
        health.progressed();
        published = last;
        // A pending L0 commit can land first, so `F` comes from the checkpoint itself.
        let base = source.open_base(checkpoint.id).await?;
        health.progressed();
        if base.get(crate::cluster::KEY).await?.is_none() {
            tracing::error!(checkpoint = %checkpoint.id, "a view checkpoint has no membership");
            admin.delete_checkpoint(checkpoint.id).await?;
            continue;
        }
        tracing::debug!(checkpoint = %checkpoint.id, applied = base.applied, "publish view");
        let row = base.applied;
        drop(base);
        // The marker comes before the first announcement. The checkpoint already exists, so a
        // restart after the marker still finds a base.
        if !bootstrapped {
            source.mark_bootstrapped(checkpoint.id).await?;
            health.progressed();
            bootstrapped = true;
        }
        let Some(accepted) = announce(&events, (checkpoint.id, row)).await else {
            return Ok(());
        };
        demand.served();
        health.published();
        retry.sent((checkpoint.id, row), accepted);
        // A new leader has no reports yet. It waits until each member has reported twice.
        let retain =
            retention == crate::state::ViewRetention::Fenced && led.elapsed() >= RETENTION_GRACE;
        let kept = async {
            let views = view_checkpoints(&admin).await?.len();
            if let Some(gauge) = &retained {
                gauge.set(views as i64);
            }
            if retain {
                let reported = reported(&reports);
                crate::state::shared::retain(
                    &admin,
                    &source,
                    &reported,
                    KEEP_CHECKPOINTS,
                    Some(epoch),
                )
                .await?;
            }
            anyhow::Ok(())
        };
        // The publication is done. A later pass deletes what this one keeps.
        if let Err(error) = kept.await {
            tracing::warn!(%error, "cannot retain view checkpoints; skip this round");
        }
    }
}

/// The end of a `reannounce`.
enum Recheck {
    /// The check found and announced a checkpoint, or ruled one out.
    Done,
    /// The storage read failed. The retry stays due.
    Failed,
    /// The node no longer takes events.
    Closed,
}

/// Check the last announcement, and announce a stranded checkpoint.
///
/// A failed check keeps the retry due. A sent announcement stays due until applied state
/// shows it, or until its base covers it.
async fn reannounce(
    admin: &slatedb::admin::Admin,
    source: &Source,
    applied: &Applied,
    retry: &mut Retry,
    bootstrapped: &mut bool,
    events: &mpsc::Sender<Event>,
) -> Result<Recheck> {
    // A sent announcement counts when applied state shows it. Until then this publisher sends
    // its own checkpoint again. The usual case reads no storage.
    if let Some(id) = retry.awaiting {
        if *applied.announced.borrow() == Some(id.0) {
            retry.done();
            return Ok(Recheck::Done);
        }
        let Some(accepted) = announce(events, id).await else {
            return Ok(Recheck::Closed);
        };
        retry.sent(id, accepted);
        return Ok(Recheck::Done);
    }
    let id = match stranded(admin, source, applied).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            retry.done();
            return Ok(Recheck::Done);
        }
        Err(error) => {
            tracing::warn!(%error, "cannot check for a stranded view checkpoint; retry");
            retry.again();
            return Ok(Recheck::Failed);
        }
    };
    tracing::info!(checkpoint = %id.0, "announce a stranded view checkpoint");
    if !*bootstrapped {
        source.mark_bootstrapped(id.0).await?;
        *bootstrapped = true;
    }
    let Some(accepted) = announce(events, id).await else {
        return Ok(Recheck::Closed);
    };
    retry.sent(id, accepted);
    Ok(Recheck::Done)
}

/// Send checkpoint `id` with its row `F` to the node for its announcement. Return whether Raft
/// took the proposal, or `None` when the node no longer takes events.
async fn announce(events: &mpsc::Sender<Event>, (id, row): (uuid::Uuid, u64)) -> Option<bool> {
    let (reply, accepted) = tokio::sync::oneshot::channel();
    events
        .send(Event::ViewCheckpoint(id, row, Some(reply)))
        .await
        .ok()?;
    Some(accepted.await.unwrap_or(false))
}

/// Bring Raft's voters and learners to `target` with single membership changes.
///
/// An in-process adopt skips the log entries up to the checkpoint, and those entries can hold
/// membership changes. Raft changes its configuration only through `apply_conf_change`, so this
/// applies the difference: new voters and promotions, new learners, then removals.
pub(super) fn reconcile_conf(
    raw: &mut RawNode<log::Cache>,
    target: &raft::eraftpb::ConfState,
) -> Result<()> {
    use raft::eraftpb::{ConfChange, ConfChangeType};
    let current = raw.raft.prs().conf().to_conf_state();
    let mut changes = vec![];
    for &id in &target.voters {
        if !current.voters.contains(&id) {
            changes.push((id, ConfChangeType::AddNode));
        }
    }
    for &id in &target.learners {
        if !current.learners.contains(&id) && !current.voters.contains(&id) {
            changes.push((id, ConfChangeType::AddLearnerNode));
        }
    }
    for &id in current.voters.iter().chain(&current.learners) {
        if !target.voters.contains(&id) && !target.learners.contains(&id) {
            changes.push((id, ConfChangeType::RemoveNode));
        }
    }
    for (node_id, change_type) in changes {
        raw.apply_conf_change(&ConfChange {
            node_id,
            change_type: change_type as i32,
            ..Default::default()
        })?;
    }
    Ok(())
}

/// Ticks between two `ViewAck` reports.
const REPORT_TICKS: u64 = 50;
/// Ticks between two publication requests of a follower's waiting manual checkpoint.
const RETRY_TICKS: u64 = 5;
/// Ticks between two checkpoint polls of a paused member.
const POLL_TICKS: u64 = 10;
impl Node {
    /// Align the Raft log with the base that the state opened at restart.
    ///
    /// If the log holds `F` with the checkpoint term, apply replays the entries above it: a local
    /// rebuild. Otherwise the member adopts the checkpoint as its snapshot record.
    pub(super) fn align_log(storage: &mut RaftStorage, state: &StateMachine) -> Result<()> {
        let index = state.applied;
        if index == 0 {
            return Ok(());
        }
        if index == storage.snapshot_index() {
            // Known terms at the same index must agree.
            let term = storage.recovery_snapshot().get_metadata().term;
            ensure!(
                term == 0 || state.applied_term == 0 || term == state.applied_term,
                "base term {} differs from snapshot term {term} at {index}",
                state.applied_term
            );
            return Ok(());
        }
        ensure!(
            index > storage.snapshot_index(),
            "the base checkpoint is below the Raft snapshot"
        );
        let term = state.applied_term;
        ensure!(term != 0, "the base checkpoint has no applied term");
        if index <= storage.last_index()? && storage.term(index).ok() == Some(term) {
            return Ok(());
        }
        let mut snapshot = raft::eraftpb::Snapshot::default();
        snapshot.mut_metadata().index = index;
        snapshot.mut_metadata().term = term;
        snapshot.mut_metadata().set_conf_state(state.members.conf());
        let shared = state.shared.as_ref().context("shared state has no base")?;
        let checkpoint = shared
            .base
            .checkpoint
            .context("the base has no checkpoint")?;
        snapshot.data = crate::state::transfer::ViewDescriptor::new(checkpoint).encode()?;
        tracing::info!(index, term, "adopt the base checkpoint");
        storage.adopt(&snapshot)
    }

    /// Start or stop the writer tasks after a role change.
    pub(super) fn sync_writer(&mut self) {
        let leader = self.raw.raft.state == StateRole::Leader;
        let term = self.raw.raft.term;
        // A new leader probes the voters at once. Its first write waits for their samples.
        self.tick_clock(true);
        if let Some(writer) = &mut self.writer {
            writer.proofs.lead(leader.then_some(term));
            writer.sync(leader, term, &self.events);
        }
    }

    /// Defragment the shared database. Only the leader has the writer and its compactor, so a
    /// follower returns "not leader" and the gRPC layer forwards the request. The leader flushes
    /// the writer, compacts every SST into one sorted run, and requests a checkpoint, so the
    /// next base reads the compacted run.
    pub(super) fn defragment_shared(
        &mut self,
        credentials: crate::auth::Credentials,
        response: Response,
    ) -> Result<()> {
        if let Err(error) = self.state.auth.admin(&credentials) {
            let _ = response.send(Ok(serde_json::to_value(Outcome::<Value> {
                result: Err(error),
            })?));
            return Ok(());
        }
        if self.raw.raft.state != StateRole::Leader {
            let _ = response.send(Err(self.error("not leader")));
            return Ok(());
        }
        let writer = self.writer.as_ref().context("shared mode has no writer")?;
        // The `Db` and the admin on its clock come as one pair, or the writer is not open.
        let Some(Open { db, admin, gate }) = writer.open.lock().unwrap().clone() else {
            let _ = response.send(Err(self.error("the shared writer is not open yet")));
            return Ok(());
        };
        let defragment = Defragment {
            db,
            admin,
            owner: Some(gate),
            progress: writer.progress.clone(),
            announced: writer.applied.announced.clone(),
            leadership: writer.leadership.subscribe(),
            term: self.raw.raft.term,
            pause: None,
        };
        let mut header = ResponseHeader {
            revision: self.state.revision,
            ..Default::default()
        };
        self.fill_header(Some(&mut header));
        tokio::spawn(async move {
            let outcome = match defragment.run().await {
                Ok(()) => Outcome {
                    result: Ok(json!({ "header": header })),
                },
                Err(error) => Outcome { result: Err(error) },
            };
            if let Ok(value) = serde_json::to_value(outcome) {
                let _ = response.send(Ok(value));
            }
        });
        Ok(())
    }

    /// Propose a checkpoint with row `row` that the publisher created. Only the leader
    /// announces. Return true when Raft took the proposal.
    ///
    /// The coverage stays at the row unless every other member installs a base above its row.
    pub(super) fn announce_view(&mut self, checkpoint: uuid::Uuid, row: u64) -> Result<bool> {
        let extend = self.extension_allowed();
        let Some(writer) = &self.writer else {
            return Ok(false);
        };
        let base = *writer.applied.base_id.borrow();
        let covers = match extend {
            true => demand::coverage(row, base, writer.queue.position()),
            false => row,
        };
        self.propose_view(checkpoint, covers)
    }

    /// Propose a `ViewCheckpoint` entry that certifies `checkpoint` through `covers`.
    fn propose_view(&mut self, checkpoint: uuid::Uuid, covers: u64) -> Result<bool> {
        let Some(writer) = &mut self.writer else {
            return Ok(false);
        };
        if self.raw.raft.state != StateRole::Leader {
            return Ok(false);
        }
        let data = RaftCommand::ViewCheckpoint {
            id: uuid::Uuid::new_v4().to_string(),
            checkpoint: checkpoint.to_string(),
            covers,
        }
        .encode()?;
        // The entry frees memory, so it bypasses the client admission limits. Raft can drop the
        // proposal, for example during a leader transfer. The publisher then announces again.
        match self.raw.propose(vec![], data) {
            Ok(()) => {
                writer.announced += 1;
                Ok(true)
            }
            Err(error) => {
                tracing::info!(%error, %checkpoint, "a view checkpoint proposal was dropped");
                Ok(false)
            }
        }
    }

    /// The checkpoints that this member pins: live bases, received snapshots, and the
    /// checkpoint of its Raft snapshot record.
    fn pinned_checkpoints(&self) -> HashSet<uuid::Uuid> {
        let Some(writer) = &self.writer else {
            return HashSet::new();
        };
        let mut pinned = writer.source.pinned();
        let record = self.raw.store().recovery_snapshot().get_data();
        if let Ok(Some(descriptor)) = crate::state::transfer::ViewDescriptor::decode(record) {
            if let Ok(id) = descriptor.checkpoint() {
                pinned.insert(id);
            }
        }
        pinned
    }

    /// Record a `ViewAck` from `member`. Only the leader keeps reports.
    pub(super) fn view_ack(
        &mut self,
        member: u64,
        checkpoints: HashSet<uuid::Uuid>,
        publish: Option<u64>,
        coverage: Option<u64>,
        health: Option<fault::WriterReport>,
    ) {
        self.raw.store().note_coverage(member, coverage);
        if let Some(writer) = &mut self.writer {
            writer.report(member, checkpoints);
            writer.handoff.note(member, coverage, health);
            // A follower's manual checkpoint. The publication is due once apply reaches it.
            // The follower can group entries in other ways, so its base must reach the index.
            if let Some(index) = publish {
                writer.remote_checkpoint = writer.remote_checkpoint.max(index);
                writer.demand = writer.demand.max(index);
            }
        }
    }

    /// Run the periodic shared-mode work: `ViewAck` reports, the paused checkpoint poll, and the
    /// leader's demand for a base.
    pub(super) fn tick_shared(&mut self) -> Result<()> {
        self.certify_base()?;
        let probe = self
            .writer
            .as_ref()
            .is_some_and(|writer| (writer.ticks + 1) % clock::PROBE_TICKS == 0);
        self.tick_clock(probe);
        self.tick_writer_health();
        let Some(writer) = &mut self.writer else {
            return Ok(());
        };
        writer.ticks += 1;
        let ticks = writer.ticks;
        if let (Some(gauges), Some((base, _, bytes))) = (&writer.gauges, self.state.view) {
            gauges.base.set(base as i64);
            gauges.overlay.set(bytes as i64);
            gauges.paused.set(writer.paused.into());
            gauges
                .leading
                .set(writer.open.lock().unwrap().is_some().into());
            gauges
                .clock_alarm
                .set(writer.clock.alarm_name().is_some().into());
            let waiting = writer.clock.waiting() || writer.clock.uncertain();
            gauges.clock_waiting.set(waiting.into());
            gauges
                .reopens
                .set(writer.reopens.load(std::sync::atomic::Ordering::Relaxed) as i64);
        }
        let leader = self.raw.raft.state == StateRole::Leader;
        if leader && writer.remote_checkpoint > 0 {
            let index = writer.remote_checkpoint;
            writer.request_checkpoint(index, self.state.applied);
            if index <= writer.checkpoint_requested {
                writer.remote_checkpoint = 0;
            }
        }
        if ticks % POLL_TICKS == 0 && writer.paused {
            self.poll_checkpoints();
        }
        // A follower repeats the request of a waiting manual checkpoint.
        if !leader && ticks % RETRY_TICKS == 0 {
            if let Some(index) = self
                .checkpoint_waiters
                .iter()
                .map(|(index, _)| *index)
                .max()
            {
                self.send_view_ack(Some(index));
            }
        }
        if ticks % REPORT_TICKS != 0 {
            return Ok(());
        }
        self.send_view_ack(None);
        Ok(())
    }

    /// Report the pinned checkpoints to the leader. `publish` asks the leader to publish through
    /// an index for a manual checkpoint on this member.
    pub(super) fn send_view_ack(&mut self, publish: Option<u64>) {
        let pinned = self.pinned_checkpoints();
        let leader = self.raw.raft.leader_id;
        if leader == self.options.id {
            if let Some(writer) = &self.writer {
                writer.report(leader, pinned);
            }
            return;
        }
        if leader == 0 || self.manual_transport {
            return;
        }
        let Some(address) = self
            .state
            .members
            .members
            .get(&leader)
            .and_then(|member| member.peer_ur_ls.first())
        else {
            return;
        };
        let url = format!("{}/raft/view-ack", crate::cluster::url(address));
        // The member installs a base above its checkpoint row. See `node::shared::demand`.
        // The writer report tells the leader whether this member can take over the writer.
        let term = self.raw.raft.term;
        let health = self
            .writer
            .as_ref()
            .map(|writer| writer.health.report(term, self.apply_held_at));
        let body = json!({"member": self.options.id, "checkpoints": pinned, "publish": publish,
            "coverage": crate::peer::process_id(), "writer": health});
        let client = self.options.transport.http.clone();
        let identity = self.peer.clone();
        tokio::spawn(async move {
            let sent = identity
                .headers(client.post(url))
                .timeout(Duration::from_secs(2))
                .json(&body)
                .send()
                .await;
            if let Err(error) = sent {
                tracing::debug!(%error, "cannot send a view report");
            }
        });
    }

    /// While apply waits, open the newest checkpoint above the base out of band. A switch job
    /// then releases the segments that it covers, including the open one.
    fn poll_checkpoints(&mut self) {
        let Some(writer) = &self.writer else {
            return;
        };
        if writer
            .polling
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let polling = writer.polling.clone();
        let admin = writer.admin.clone();
        let source = writer.source.clone();
        let opened = writer.opened_slot.clone();
        let base = self.state.view.map_or(0, |(base, _, _)| base);
        tokio::spawn(async move {
            let result = async {
                let Some((seq, checkpoint)) = view_checkpoints(&admin).await?.pop() else {
                    return anyhow::Ok(());
                };
                if seq > base {
                    opened.offer(source.open_base(checkpoint.id).await?, 0);
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "cannot poll view checkpoints");
            }
            polling.store(false, std::sync::atomic::Ordering::Release);
        });
    }

    /// Return true when the overlay is over the soft limit. The leader then rejects client
    /// proposals. `ViewCheckpoint` entries are exempt.
    pub(super) fn overlay_full(&self) -> bool {
        self.state
            .view
            .is_some_and(|(_, _, bytes)| bytes > self.options.overlay_max_bytes)
    }

    /// Return true while apply waits for a base switch. Apply waits above twice the soft limit
    /// and resumes below the soft limit. Raft still persists and acknowledges entries.
    pub(super) fn apply_paused(&mut self) -> bool {
        let Some((_, _, bytes)) = self.state.view else {
            return false;
        };
        let limit = self.options.overlay_max_bytes;
        let Some(writer) = &mut self.writer else {
            return false;
        };
        if bytes > limit.saturating_mul(2) {
            if !writer.paused {
                tracing::warn!(
                    bytes,
                    limit,
                    "pause apply until a base switch frees the overlay"
                );
            }
            writer.paused = true;
        } else if bytes < limit {
            writer.paused = false;
            // A dropped base explains a pause only. Any way out of the pause ends its report.
            writer.rejected.clear();
        }
        writer.paused
    }

    /// Return the index and log term of an opened base that a paused member can adopt: the
    /// base is above the applied index, and the delivered commit covers it.
    ///
    /// The node takes the base from the slot and verifies that one. A newer open cannot replace
    /// it before the job runs. A base that fails the check goes back to the slot.
    pub(super) fn adoptable(&self) -> Option<(Arc<crate::state::view::Base>, u64)> {
        let writer = self.writer.as_ref()?;
        if !writer.paused || self.prepared_puts.is_some() || self.held_puts.is_some() {
            return None;
        }
        let (base, covers) = writer.opened_slot.take()?;
        let index = base.applied;
        let term = (index > self.state.applied && index <= self.committed_ready)
            .then(|| self.raw.raft.raft_log.term(index).ok())
            .flatten();
        match term {
            Some(term) => Some((base, term)),
            None => {
                writer.opened_slot.offer(base, covers);
                None
            }
        }
    }

    /// Mark a switch due when a base was offered since the last call. A manual apply, such as
    /// DST, has no event loop that watches the slot.
    #[cfg(test)]
    pub(super) fn note_offers(&mut self) {
        if let Some(writer) = &mut self.writer {
            if writer.opened.has_changed().unwrap_or(false) {
                writer.opened.borrow_and_update();
                writer.switch_due = true;
            }
        }
    }

    /// Take a due switch job.
    pub(super) fn take_switch(&mut self) -> bool {
        self.writer
            .as_mut()
            .is_some_and(|writer| std::mem::take(&mut writer.switch_due))
    }
}

/// A writer that DST drives one step at a time. Each step follows the rules of `lead` and
/// `publish`, so a schedule can stop, fence, or crash a writer between any two steps.
#[cfg(test)]
#[derive(Default)]
pub(super) struct DstWriter {
    pub db: Option<Arc<slatedb::Db>>,
    pub term: u64,
    pub written: u64,
    pub epoch: u64,
    published: u64,
    bootstrapped: bool,
    /// A checkpoint with its row that the next announce step proposes.
    pub pending: Option<(uuid::Uuid, u64)>,
}

/// Compact every L0 SST and sorted run into one run when the L0 has `limit` SSTs or more, and
/// wait until the compaction ends.
#[cfg(test)]
async fn dst_compact(admin: &slatedb::admin::Admin, limit: usize) -> Result<()> {
    use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
    let manifest = admin
        .read_manifest(None)
        .await?
        .context("missing manifest")?;
    if manifest.l0().len() < limit {
        return Ok(());
    }
    let mut sources: Vec<_> = manifest
        .l0()
        .iter()
        .map(|view| SourceId::SstView(view.id))
        .collect();
    sources.extend(
        manifest
            .compacted()
            .iter()
            .map(|run| SourceId::SortedRun(run.id)),
    );
    let destination = manifest
        .compacted()
        .iter()
        .map(|run| run.id)
        .min()
        .unwrap_or(0);
    let job = admin
        .submit_compaction(CompactionSpec::new(sources, destination))
        .await?;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        match admin.read_compaction(job.id(), None).await? {
            Some(job) if matches!(job.status(), CompactionStatus::Completed) => {
                // The compactor commits the manifest after the job. Wait for the new L0.
                let manifest = admin
                    .read_manifest(None)
                    .await?
                    .context("missing manifest")?;
                if manifest.l0().len() < limit {
                    return Ok(());
                }
            }
            Some(job) if matches!(job.status(), CompactionStatus::Failed) => {
                anyhow::bail!("a DST compaction failed")
            }
            _ => {}
        }
    }
    anyhow::bail!("a DST compaction timed out")
}

#[cfg(test)]
impl DstWriter {
    pub async fn close(&mut self) {
        if let Some(db) = self.db.take() {
            let _ = db
                .close_with_options(slatedb::config::CloseOptions { flush_type: None })
                .await;
        }
    }
}

#[cfg(test)]
impl Node {
    /// Open the writer on the leader. With `deposed`, a member that does not lead opens it, as
    /// a slow open of an older term does. Return false when the open did not start a writer.
    pub(super) async fn dst_open(&mut self, dst: &mut DstWriter, deposed: bool) -> Result<bool> {
        let leader = self.raw.raft.state == StateRole::Leader;
        if leader == deposed {
            return Ok(false);
        }
        let term = self.raw.raft.term;
        let writer = self.writer.as_ref().context("shared state mode")?;
        dst.close().await;
        // The compactor runs only on request, so GC removes the same SSTs on every replay.
        let mut settings = crate::state::slatedb_settings();
        if let Some(compactor) = settings.compactor_options.as_mut() {
            compactor
                .scheduler_options
                .insert("min_compaction_sources".into(), "1000000".into());
        }
        let db = Arc::new(writer.source.open_writer_with(settings).await?);
        // The lead must be unchanged after the open, and the clock must pass the manifest.
        if (!deposed && self.raw.raft.term != term)
            || !writer.clock.pass(db.manifest().last_l0_clock_tick()).await
        {
            let _ = db
                .close_with_options(slatedb::config::CloseOptions { flush_type: None })
                .await;
            return Ok(false);
        }
        dst.written = db.manifest().last_l0_seq();
        dst.epoch = db.manifest().writer_epoch();
        dst.term = term;
        dst.published = view_checkpoints(&writer.admin)
            .await?
            .last()
            .map_or(0, |(seq, _)| *seq);
        dst.bootstrapped = writer.source.bootstrapped().await?;
        // A new writer announces a stranded checkpoint, as `publish` does.
        dst.pending = stranded(&writer.admin, &writer.source, &writer.applied).await?;
        if let (Some((id, _)), false) = (dst.pending, dst.bootstrapped) {
            writer.source.mark_bootstrapped(id).await?;
            dst.bootstrapped = true;
        }
        dst.db = Some(db);
        Ok(true)
    }

    /// Write the overlay batches above the written sequence, each with its own sequence.
    pub(super) async fn dst_materialize(&mut self, dst: &mut DstWriter) -> Result<usize> {
        let (Some(db), Some(writer)) = (&dst.db, &self.writer) else {
            return Ok(0);
        };
        let mut count = 0;
        for (seq, batch) in writer.queue.after(dst.written, usize::MAX) {
            db.write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
                .await?;
            dst.written = seq;
            count += 1;
        }
        Ok(count)
    }

    /// Flush, then create one checkpoint for an advance of `last_l0_seq`. The bootstrap marker
    /// comes before the first announcement.
    pub(super) async fn dst_checkpoint(
        &mut self,
        dst: &mut DstWriter,
    ) -> Result<Option<uuid::Uuid>> {
        let (Some(db), Some(writer)) = (&dst.db, &self.writer) else {
            return Ok(None);
        };
        db.flush().await?;
        // Compact the L0 SSTs on request before SlateDB would stop writes for them.
        dst_compact(&writer.admin, 4).await?;
        let last = db.manifest().last_l0_seq();
        if last <= dst.published {
            return Ok(None);
        }
        let options = CheckpointOptions {
            name: Some(view_name(last)),
            ..Default::default()
        };
        let id = db
            .create_checkpoint(CheckpointScope::Durable, &options)
            .await?
            .id;
        dst.published = last;
        let base = writer.source.open_base(id).await?;
        if base.get(crate::cluster::KEY).await?.is_none() {
            writer.admin.delete_checkpoint(id).await?;
            return Ok(None);
        }
        if !dst.bootstrapped {
            writer.source.mark_bootstrapped(id).await?;
            dst.bootstrapped = true;
        }
        dst.pending = Some((id, base.applied));
        Ok(Some(id))
    }

    /// Propose the pending checkpoint. Only the leader proposes.
    pub(super) fn dst_announce(&mut self, dst: &mut DstWriter) -> Result<()> {
        match dst.pending.take() {
            Some((id, row)) => self.announce_view(id, row).map(drop),
            None => Ok(()),
        }
    }

    /// Delete old checkpoints that no member reported, with the epoch check of this writer.
    pub(super) async fn dst_retain(
        &mut self,
        dst: &mut DstWriter,
        reported: &HashSet<uuid::Uuid>,
    ) -> Result<()> {
        let (Some(_), Some(writer)) = (&dst.db, &self.writer) else {
            return Ok(());
        };
        crate::state::shared::retain(
            &writer.admin,
            &writer.source,
            reported,
            KEEP_CHECKPOINTS,
            Some(dst.epoch),
        )
        .await
    }

    /// Return true when a newer writer holds the shared database.
    pub(super) async fn dst_fenced(&self, dst: &DstWriter) -> bool {
        let Some(writer) = &self.writer else {
            return false;
        };
        match writer.admin.read_manifest(None).await {
            Ok(Some(manifest)) => manifest.writer_epoch() != dst.epoch,
            _ => false,
        }
    }

    /// The checkpoints that this member pins, as its `ViewAck` reports them.
    pub(super) fn dst_pins(&self) -> HashSet<uuid::Uuid> {
        self.pinned_checkpoints()
    }
}

mod clock;
mod defragment;
mod demand;
mod fault;
mod rejected;
mod write_proof;
pub(super) use clock::Clock;
#[cfg(test)]
pub(super) use clock::{probe_nonce, Gate};
pub(crate) use clock::{Millis, ProbeReply, ProbeRequest, PROBE_ROUTE};
use defragment::Defragment;
#[cfg(test)]
use defragment::DefragmentStep;
#[cfg(test)]
pub(super) use fault::fenced;
pub(crate) use fault::WriterReport;
pub(crate) use write_proof::{Prober, WriteProbeReply, WriteProbeRequest, WRITE_PROBE_ROUTE};
#[cfg(test)]
mod materialize_tests;
#[cfg(test)]
mod record_tests;

#[cfg(test)]
mod tests;
