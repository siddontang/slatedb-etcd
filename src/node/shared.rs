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

    /// Start again after a writer open at `written`. A publication request stays: a manual
    /// checkpoint can ask for it before the new writer opens.
    fn reset(&self, written: u64) {
        self.written.send_replace(written);
        let mut inner = self.inner.lock().unwrap();
        inner.dirty_since = None;
        inner.republish = false;
    }

    /// Publish a new checkpoint at the current sequence, for a changed layout.
    fn republish(&self) {
        self.inner.lock().unwrap().republish = true;
        self.wake.notify_one();
    }

    fn take_republish(&self) -> bool {
        std::mem::take(&mut self.inner.lock().unwrap().republish)
    }

    fn request(&self, through: u64) {
        let mut inner = self.inner.lock().unwrap();
        let through = inner
            .publish_through
            .map_or(through, |old| old.max(through));
        inner.publish_through = Some(through);
        drop(inner);
        self.wake.notify_one();
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

    /// Return whether a flush is due, and whether a request is one of its causes. Clear the
    /// causes that it covers.
    fn take_flush(&self, max_age: Duration) -> (bool, bool) {
        let written = self.written();
        let mut inner = self.inner.lock().unwrap();
        let requested = inner
            .publish_through
            .is_some_and(|through| written >= through);
        let aged = inner
            .dirty_since
            .is_some_and(|since| since.elapsed() >= max_age);
        if requested {
            inner.publish_through = None;
        }
        if requested || aged {
            inner.dirty_since = None;
        }
        (requested || aged, requested)
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
}

/// The shared-mode gauges of one member. The node sets them on each tick.
pub(super) struct Gauges {
    base: prometheus::IntGauge,
    overlay: prometheus::IntGauge,
    paused: prometheus::IntGauge,
    leading: prometheus::IntGauge,
    reopens: prometheus::IntGauge,
    clock_alarm: prometheus::IntGauge,
}

impl Gauges {
    pub(super) fn register(registry: &prometheus::Registry) -> Result<Self> {
        let gauge = |name: &str, help: &str| -> Result<prometheus::IntGauge> {
            let gauge = prometheus::IntGauge::new(name, help)?;
            registry.register(Box::new(gauge.clone()))?;
            Ok(gauge)
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
                "1 while the writer does not start because the local clock is too far behind.",
            )?,
            reopens: gauge(
                "slatedb_etcd_shared_writer_reopens",
                "Writer opens after a fence or a lost manifest race while this member led.",
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
            gauges: None,
            checkpoint_requested: 0,
            remote_checkpoint: 0,
            demand: 0,
            certified: None,
            demand_publication: 0,
            certify_ticks: 0,
            demand_term: 0,
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
            self.transfer = None;
            self.demand = 0;
            self.certified = None;
            self.demand_publication = 0;
        }
        if !leader || self.running() {
            return;
        }
        self.term = term;
        let (stop, stopped) = tokio::sync::watch::channel(false);
        self.stop = Some(stop);
        let task = lead(
            Publisher {
                db: None,
                admin: self.admin.clone(),
                source: self.source.clone(),
                progress: self.progress.clone(),
                max_age: self.max_age,
                reports: self.reports.clone(),
                retention: self.retention,
                retained: self.retained.clone(),
                applied: self.applied.clone(),
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
            },
        );
        self.tasks.spawn(async move { (term, task.await) });
    }

    pub async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.send_replace(true);
        }
        // A clean stop closes the writer. A stuck close must not hold the shutdown.
        let closing = async { while self.tasks.join_next().await.is_some() {} };
        if tokio::time::timeout(Duration::from_secs(5), closing)
            .await
            .is_err()
        {
            self.tasks.shutdown().await;
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
            "clock_skew_ms": self.clock.skew.load(std::sync::atomic::Ordering::Relaxed),
            "clock_alarm": self.clock.alarm.load(std::sync::atomic::Ordering::Relaxed),
            "reopens": self.reopens.load(std::sync::atomic::Ordering::Relaxed),
            "reports": self.reports.lock().unwrap().len(),
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
    /// A writer task failed. The node stops.
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

/// The clock check of a new writer. SlateDB starts a writer's clock at the manifest's
/// `last_l0_clock_tick`, and a lagging clock makes each write wait and then fail.
#[derive(Clone)]
pub(super) struct Clock {
    /// Milliseconds since the Unix epoch. Tests skew it.
    pub now_ms: Arc<dyn Fn() -> i64 + Send + Sync>,
    pub max_skew: Duration,
    /// The last measured skew: `last_l0_clock_tick` minus the local time, at least zero.
    pub skew: Arc<std::sync::atomic::AtomicI64>,
    /// Set while the skew is over the limit and the writer does not start.
    pub alarm: Arc<std::sync::atomic::AtomicBool>,
    pub gauge: Option<prometheus::IntGauge>,
}

impl Clock {
    pub fn system(max_skew: Duration, gauge: Option<prometheus::IntGauge>) -> Self {
        Self {
            now_ms: Arc::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |since| since.as_millis() as i64)
            }),
            max_skew,
            skew: Arc::default(),
            alarm: Arc::default(),
            gauge,
        }
    }

    /// Wait until the local clock passes `tick`. Return false, and raise the alarm, when the
    /// local clock is behind by more than the limit.
    pub async fn pass(&self, tick: i64) -> bool {
        use std::sync::atomic::Ordering;
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
}

/// The writer `Db` while the writer task runs. Defragment flushes and compacts it.
type OpenWriter = Arc<Mutex<Option<Arc<slatedb::Db>>>>;

/// Wait until the member asks the writer task to stop. The result holds no borrow, so the task
/// stays `Send` across later waits.
async fn stop_requested(stopped: &mut tokio::sync::watch::Receiver<bool>) {
    let _ = stopped.wait_for(|stop| *stop).await;
}

/// Return true when `error` is a lost manifest race: another writer wrote the manifest version
/// that this open tried to write. SlateDB reports it as a data error, so the check uses its
/// message.
pub(super) fn open_conflict(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<slatedb::Error>().is_some_and(|error| {
            error.kind() == slatedb::ErrorKind::Data
                && error.to_string().contains("version already exists")
        })
    })
}

/// Return true when `error` comes from a writer that a newer writer fenced.
pub(super) fn fenced(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<slatedb::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
            )
        })
    })
}

/// Open the writer, then materialize and publish until the member steps down.
///
/// 1. The open increments the writer epoch, which fences an older writer.
/// 2. After the open, the member must still lead the same term. Otherwise it closes the writer.
/// 3. The local clock must pass the manifest's `last_l0_clock_tick`. Over the skew limit the
///    writer does not start, and the task tries again later.
/// 4. The materializer and the publisher start. A fence while this member still leads is not
///    a storage failure: the task opens the writer again. Every other error stops the member.
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
    } = leading;
    let current = || *leadership.borrow() == (true, term);
    loop {
        // An ordinary start bootstraps an empty state only after it claims the one bootstrap
        // operation. A pending snapshot restore holds the claim, so the leader waits for it.
        if let Some(wait) = bootstrap_wait(&publisher.source).await {
            tracing::warn!(reason = %wait, "the shared writer waits for the bootstrap");
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                _ = stop_requested(&mut stopped) => return Ok(()),
            }
        }
        let opened = tokio::select! {
            opened = publisher.source.open_writer() => opened,
            _ = stop_requested(&mut stopped) => return Ok(()),
        };
        let db = match opened {
            Ok(db) => Arc::new(db),
            // A newer open fenced this one while the member still leads the same term.
            // Two opens can also race for the same manifest version. The open that loses must
            // read the new manifest and try again.
            Err(error)
                if (fenced(&error) || open_conflict(&error)) && current() && !*stopped.borrow() =>
            {
                tracing::warn!(term, %error, "the shared writer open was fenced; open it again");
                reopens.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                continue;
            }
            Err(error) => return Err(error),
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
        let passed = tokio::select! {
            passed = clock.pass(db.manifest().last_l0_clock_tick()) => passed,
            _ = stop_requested(&mut stopped) => {
                close(db).await;
                return Ok(());
            }
        };
        if !passed {
            close(db).await;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                _ = stop_requested(&mut stopped) => return Ok(()),
            }
        }
        // The open starts at `last_l0_seq`. Unflushed writes of an older writer are lost, and
        // the overlay holds them.
        let mut publisher = publisher.clone();
        publisher.progress.reset(db.manifest().last_l0_seq());
        publisher.db = Some(db.clone());
        *open.lock().unwrap() = Some(db.clone());
        let result = tokio::select! {
            result = materialize(db.clone(), queue.clone(), publisher.progress.clone()) => result,
            result = publish(publisher, events.clone()) => result,
            _ = stop_requested(&mut stopped) => Ok(()),
        };
        // A later task can own the slot already. Clear only this task's writer.
        {
            let mut slot = open.lock().unwrap();
            if slot.as_ref().is_some_and(|open| Arc::ptr_eq(open, &db)) {
                *slot = None;
            }
        }
        close(db).await;
        match result {
            // Two quick elections can open writers in reverse term order.
            Err(error) if fenced(&error) && current() && !*stopped.borrow() => {
                tracing::warn!(term, "the shared writer was fenced; open it again");
                reopens.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            result => return result,
        }
    }
}

/// Return why the writer cannot open yet: a pending snapshot restore, or an unreadable claim.
async fn bootstrap_wait(source: &Source) -> Option<String> {
    let checked = async {
        if source.bootstrapped().await? {
            return anyhow::Ok(None);
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
    };
    match checked.await {
        Ok(wait) => wait,
        Err(error) => Some(error.to_string()),
    }
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
            db.write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
                .await?;
            next = seq;
            progress.wrote(seq);
        }
    }
}

#[derive(Clone)]
struct Publisher {
    db: Option<Arc<slatedb::Db>>,
    admin: Arc<slatedb::admin::Admin>,
    source: Arc<Source>,
    progress: Arc<Progress>,
    max_age: Duration,
    reports: Reports,
    retention: crate::state::ViewRetention,
    /// The number of `view-` checkpoints after the last publication.
    retained: Option<prometheus::IntGauge>,
    applied: Applied,
}

/// One Defragment on the leader writer.
pub(super) struct Defragment {
    db: Arc<slatedb::Db>,
    admin: Arc<slatedb::admin::Admin>,
    progress: Arc<Progress>,
    /// The newest applied announcement on this member.
    announced: tokio::sync::watch::Receiver<Option<uuid::Uuid>>,
    leadership: tokio::sync::watch::Receiver<(bool, u64)>,
    term: u64,
    /// A test pause before a step: (step, reached, resume).
    pause: Option<(
        DefragmentStep,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    )>,
}

/// The steps of a Defragment where a test can pause it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum DefragmentStep {
    Flush,
    Compact,
    Publish,
}

/// The longest time that a Defragment waits for its compacted checkpoint.
const DEFRAGMENT_WAIT: Duration = Duration::from_secs(20);

impl Defragment {
    /// Flush, compact every SST into one sorted run, and wait until this member applied the
    /// announcement of a checkpoint at or beyond the completed compaction manifest. An
    /// older checkpoint that applies late does not count. Members then adopt the compacted one.
    ///
    /// A leader change before that returns a retryable error, also when it closes the writer
    /// during the flush or the compaction. The new leader can then run the Defragment again.
    pub(super) async fn run(mut self) -> std::result::Result<(), KvError> {
        let lost = || {
            KvError::new(
                tonic::Code::Unavailable,
                "etcdserver: the leader changed during Defragment; retry",
            )
        };
        let term = self.term;
        let leads = |leadership: &tokio::sync::watch::Receiver<(bool, u64)>| {
            *leadership.borrow() == (true, term)
        };
        // A step down closes the writer, so a storage error then means a lost lead.
        let failed = |leadership: &tokio::sync::watch::Receiver<(bool, u64)>,
                      error: anyhow::Error| {
            if leads(leadership) {
                KvError::storage(error)
            } else {
                lost()
            }
        };
        self.pause_at(DefragmentStep::Flush).await;
        if let Err(error) = self.db.flush().await {
            return Err(failed(&self.leadership, error.into()));
        }
        self.pause_at(DefragmentStep::Compact).await;
        let compacted_manifest_id = match crate::state::maintenance::compact_all(&self.admin).await
        {
            Ok(id) => id,
            Err(error) => return Err(failed(&self.leadership, error)),
        };
        self.pause_at(DefragmentStep::Publish).await;
        if !leads(&self.leadership) {
            return Err(lost());
        }
        // The next checkpoint, also at the same index, reads the compacted layout.
        self.progress.republish();
        let deadline = tokio::time::Instant::now() + DEFRAGMENT_WAIT;
        let timeout = || {
            KvError::new(
                tonic::Code::Unavailable,
                "etcdserver: Defragment did not publish its checkpoint in time; retry",
            )
        };
        loop {
            let applied = *self.announced.borrow_and_update();
            if let Some(checkpoint) = applied {
                match checkpoint_covers_compaction(&self.admin, checkpoint, compacted_manifest_id)
                    .await
                {
                    Ok(true) => return Ok(()),
                    Ok(false) => {}
                    Err(error) => return Err(failed(&self.leadership, error)),
                }
            }
            tokio::select! {
                changed = self.announced.changed() => changed.map_err(|_| lost())?,
                changed = self.leadership.changed() => {
                    if changed.is_err() || !leads(&self.leadership) {
                        return Err(lost());
                    }
                }
                _ = tokio::time::sleep_until(deadline) => return Err(timeout()),
            }
        }
    }

    async fn pause_at(&mut self, step: DefragmentStep) {
        if self.pause.as_ref().is_some_and(|(at, ..)| *at == step) {
            let (_, reached, resume) = self.pause.take().unwrap();
            reached.notify_one();
            resume.notified().await;
        }
    }
}

/// Manifest versions increase across both compactions and checkpoint publications.
/// An older checkpoint may also omit the inputs if it predates an earlier compaction.
async fn checkpoint_covers_compaction(
    admin: &slatedb::admin::Admin,
    checkpoint: uuid::Uuid,
    compacted_manifest_id: u64,
) -> Result<bool> {
    Ok(admin
        .list_checkpoints(None)
        .await?
        .iter()
        .any(|listed| listed.id == checkpoint && listed.manifest_id >= compacted_manifest_id))
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
/// Only state changes write batches, so an announcement never causes the next one.
async fn publish(publisher: Publisher, events: mpsc::Sender<Event>) -> Result<()> {
    let Publisher {
        db,
        admin,
        source,
        progress,
        max_age,
        reports,
        retention,
        retained,
        applied,
    } = publisher;
    let db = db.context("the publisher needs the writer")?;
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
    // A writer that stopped before its announcement committed can leave a stranded
    // checkpoint. A new writer announces it, so the members do not wait for more writes.
    let mut retry = Retry::new();
    let mut status = db.subscribe();
    loop {
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
        let (flush, requested) = progress.take_flush(max_age);
        if flush {
            db.flush().await?;
        }
        // SlateDB also flushes a full memtable by itself. Each advance gets one checkpoint.
        let last = db.manifest().last_l0_seq();
        let republish = progress.take_republish();
        if last <= published && !republish {
            // A request with no new checkpoint can wait for a stranded one.
            if requested {
                retry.now();
            }
            if retry.due()
                && !reannounce(
                    &admin,
                    &source,
                    &applied,
                    &mut retry,
                    &mut bootstrapped,
                    &events,
                )
                .await?
            {
                return Ok(());
            }
            continue;
        }
        let options = CheckpointOptions {
            name: Some(view_name(last)),
            ..Default::default()
        };
        let checkpoint = db
            .create_checkpoint(CheckpointScope::Durable, &options)
            .await?;
        published = last;
        // A pending L0 commit can land first, so `F` comes from the checkpoint itself.
        let base = source.open_base(checkpoint.id).await?;
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
            bootstrapped = true;
        }
        let Some(accepted) = announce(&events, (checkpoint.id, row)).await else {
            return Ok(());
        };
        retry.sent((checkpoint.id, row), accepted);
        let views = view_checkpoints(&admin).await?.len();
        if let Some(gauge) = &retained {
            gauge.set(views as i64);
        }
        // A new leader has no reports yet. It waits until each member has reported twice.
        if retention == crate::state::ViewRetention::Fenced && led.elapsed() >= RETENTION_GRACE {
            let reported = reported(&reports);
            crate::state::shared::retain(&admin, &source, &reported, KEEP_CHECKPOINTS, Some(epoch))
                .await?;
        }
    }
}

/// Check the last announcement, and announce a stranded checkpoint. Return false when the node
/// no longer takes events.
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
) -> Result<bool> {
    // A sent announcement counts when applied state shows it. Until then this publisher sends
    // its own checkpoint again. The usual case reads no storage.
    if let Some(id) = retry.awaiting {
        if *applied.announced.borrow() == Some(id.0) {
            retry.done();
            return Ok(true);
        }
        let Some(accepted) = announce(events, id).await else {
            return Ok(false);
        };
        retry.sent(id, accepted);
        return Ok(true);
    }
    let id = match stranded(admin, source, applied).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            retry.done();
            return Ok(true);
        }
        Err(error) => {
            tracing::warn!(%error, "cannot check for a stranded view checkpoint; retry");
            retry.again();
            return Ok(true);
        }
    };
    tracing::info!(checkpoint = %id.0, "announce a stranded view checkpoint");
    if !*bootstrapped {
        source.mark_bootstrapped(id.0).await?;
        *bootstrapped = true;
    }
    let Some(accepted) = announce(events, id).await else {
        return Ok(false);
    };
    retry.sent(id, accepted);
    Ok(true)
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
        if !state.is_shared() || index == 0 {
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
        if let Some(writer) = &mut self.writer {
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
        let Some(db) = writer.open.lock().unwrap().clone() else {
            let _ = response.send(Err(self.error("the shared writer is not open yet")));
            return Ok(());
        };
        let defragment = Defragment {
            db,
            admin: writer.admin.clone(),
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
    pub(super) fn announce_view(&mut self, checkpoint: uuid::Uuid, row: u64) -> Result<bool> {
        let Some(writer) = &self.writer else {
            return Ok(false);
        };
        let base = *writer.applied.base_id.borrow();
        let covers = demand::coverage(row, base, writer.queue.position());
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
    ) {
        if let Some(writer) = &mut self.writer {
            writer.report(member, checkpoints);
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
            gauges.clock_alarm.set(
                writer
                    .clock
                    .alarm
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .into(),
            );
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
        let body = json!({"member": self.options.id, "checkpoints": pinned, "publish": publish});
        let client = self.options.transport.http.clone();
        let cluster = self.options.cluster_id.clone();
        tokio::spawn(async move {
            let sent = client
                .post(url)
                .header("x-cluster-id", cluster)
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

mod demand;
#[cfg(test)]
mod materialize_tests;
#[cfg(test)]
mod record_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::overlay::Overlay;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;

    fn batch(seq: u64, rows: &[(&str, &str)]) -> Arc<crate::WriteBatch> {
        let mut batch = crate::WriteBatch::new();
        for (key, value) in rows {
            batch.put(key, value);
        }
        batch.put(b"meta/applied", seq.to_be_bytes());
        Arc::new(batch)
    }

    async fn value(db: &slatedb::Db, key: &str) -> Option<bytes::Bytes> {
        db.get(key.as_bytes()).await.unwrap()
    }

    #[tokio::test]
    async fn the_materializer_writes_whole_batches_with_their_sequences() {
        let db = Arc::new(
            slatedb::Db::builder("state", Arc::new(InMemory::new()))
                .with_settings(crate::state::slatedb_settings())
                .build()
                .await
                .unwrap(),
        );
        // Entries 1 and 2 reached L0 before a restart.
        let mut first = Overlay::new(0);
        first.push(1, batch(1, &[("a", "1")]));
        first.push(2, batch(2, &[("b", "2")]));
        for (seq, batch) in first.queue().after(0, 10) {
            db.write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
                .await
                .unwrap();
        }
        db.flush().await.unwrap();
        assert_eq!(db.manifest().last_l0_seq(), 2);

        // The replay grouped entries 2..=4 into one batch. It spans `last_l0_seq` and holds
        // final values, so the materializer writes it whole at sequence 4.
        let mut replay = Overlay::new(1);
        replay.push(4, batch(4, &[("b", "2"), ("c", "3"), ("d", "4")]));
        let progress = Arc::new(Progress::new(db.manifest().last_l0_seq()));
        let task = tokio::spawn(materialize(db.clone(), replay.queue(), progress.clone()));
        progress.wait_written(4).await.unwrap();
        assert_eq!(db.snapshot().unwrap().seq(), 4);
        assert_eq!(value(&db, "c").await, Some("3".into()));
        assert_eq!(
            value(&db, "meta/applied").await.unwrap().as_ref(),
            4_u64.to_be_bytes()
        );
        // A later batch keeps its own sequence.
        replay.push(7, batch(7, &[("a", "7")]));
        progress.wait_written(7).await.unwrap();
        assert_eq!(db.snapshot().unwrap().seq(), 7);
        assert_eq!(value(&db, "a").await, Some("7".into()));
        assert_eq!(value(&db, "b").await, Some("2".into()));
        assert!(progress.deadline(Duration::from_secs(1)).is_some());
        assert!(!task.is_finished());
        task.abort();
        db.close().await.unwrap();
    }

    #[test]
    fn flushes_follow_the_oldest_write_and_publication_requests() {
        let progress = Progress::new(0);
        let long = Duration::from_secs(60);
        // An idle writer has no deadline and no flush.
        assert_eq!(progress.deadline(long), None);
        assert_eq!(progress.take_flush(Duration::ZERO), (false, false));
        progress.wrote(1);
        let deadline = progress.deadline(long).unwrap();
        progress.wrote(2);
        // A later write keeps the deadline of the oldest unflushed write.
        assert_eq!(progress.deadline(long), Some(deadline));
        assert_eq!(progress.take_flush(long), (false, false));
        assert_eq!(progress.take_flush(Duration::ZERO), (true, false));
        assert_eq!(progress.take_flush(Duration::ZERO), (false, false));
        assert_eq!(progress.deadline(long), None);
        // A request waits until the materializer writes its sequence.
        progress.request(3);
        assert_eq!(progress.take_flush(long), (false, false));
        progress.wrote(3);
        assert_eq!(progress.take_flush(long), (true, true));
        assert_eq!(progress.take_flush(long), (false, false));
        // A request that the written sequence already covers is due at once.
        progress.request(2);
        assert_eq!(progress.take_flush(long), (true, true));
    }

    /// Read `/status` from the client listener.
    async fn status(server: &crate::server::Server) -> Value {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(server.client_addr())
            .await
            .unwrap();
        let request = "GET /status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        let body = response.split_once("\r\n\r\n").unwrap().1;
        let body = &body[body.find('{').unwrap()..=body.rfind('}').unwrap()];
        serde_json::from_str(body).unwrap()
    }

    async fn eventually(what: &str, mut check: impl std::ops::AsyncFnMut() -> bool) {
        for _ in 0..600 {
            if check().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// Stall S3 on one voter until apply pauses, then recover S3 and send no new request. The
    /// leader still publishes a checkpoint at or below its applied index, and the paused member
    /// switches to it out of band. With `early`, the stall starts before the first announcement,
    /// so the switch must release the open segment.
    async fn stall_and_recover(early: bool) {
        use crate::object::faults::{FaultStore, STALL_PUT};
        use crate::proto::etcdserverpb::{kv_client::KvClient, PutRequest};
        let limit = 64 * 1024;
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::server::ServerConfig::single_node(dir.path());
        config.state_mode = crate::state::StateMode::Shared;
        config.object_store = crate::object::Config::Test(faults.clone(), "root".into());
        config.view_max_age = Duration::from_millis(100);
        config.overlay_max_bytes = limit;
        let server = crate::server::Server::start(config).await.unwrap();
        if early {
            faults.set(STALL_PUT);
        }
        server.wait_ready(Duration::from_secs(15)).await.unwrap();
        let shared = |status: &Value, field: &str| status["shared_state"][field].clone();
        if !early {
            eventually("the first announcement", async || {
                shared(&status(&server).await, "applied_view_checkpoints") == 1
            })
            .await;
            faults.set(STALL_PUT);
        }
        let channel = tonic::transport::Endpoint::from_shared(server.client_url())
            .unwrap()
            .connect()
            .await
            .unwrap();
        // Each write alone applies above twice the soft limit, so the pause does not depend on
        // how many writes the admission check lets in. No checkpoint can appear.
        let puts = (0..8_u8).map(|n| {
            let mut kv = KvClient::new(channel.clone());
            async move {
                kv.put(PutRequest {
                    key: vec![b'k', n],
                    value: vec![n; 96 * 1024],
                    ..Default::default()
                })
                .await
            }
        });
        let written = futures::future::join_all(puts).await;
        assert!(written.iter().any(Result::is_ok));
        eventually("the apply pause", async || {
            status(&server).await["shared_state"]["writer"]["paused"] == true
        })
        .await;
        let paused = status(&server).await;
        assert!(shared(&paused, "overlay_bytes").as_u64().unwrap() > 2 * limit);
        // S3 recovers. No client sends a request.
        faults.set(0);
        eventually("the resumed apply", async || {
            let status = status(&server).await;
            status["shared_state"]["writer"]["paused"] == false
                && shared(&status, "overlay_bytes").as_u64().unwrap() < limit
                && status["applied_index"] == status["commit_index"]
        })
        .await;
        let mut kv = KvClient::new(channel);
        let reply = kv
            .range(crate::proto::etcdserverpb::RangeRequest {
                key: vec![b'k'],
                range_end: vec![b'l'],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        // The admitted writes are readable. The admission check rejected the others.
        assert!(!reply.kvs.is_empty());
        server.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stalled_store_pauses_apply_until_an_out_of_band_switch() {
        stall_and_recover(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stall_before_the_first_announcement_releases_the_open_segment() {
        stall_and_recover(true).await;
    }

    /// Status answers while the first leader cannot create the shared manifest, and after.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn status_answers_before_the_writer_creates_the_manifest() {
        use crate::object::faults::{FaultStore, STALL_STATE_PUT};
        use crate::proto::etcdserverpb::{
            kv_client::KvClient, maintenance_client::MaintenanceClient, PutRequest, StatusRequest,
        };
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        // The claims succeed. The writer cannot create the shared manifest.
        faults.set(STALL_STATE_PUT);
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::server::ServerConfig::single_node(dir.path());
        config.state_mode = crate::state::StateMode::Shared;
        config.object_store = crate::object::Config::Test(faults.clone(), "root".into());
        let server = crate::server::Server::start(config).await.unwrap();
        server.wait_ready(Duration::from_secs(15)).await.unwrap();
        let channel = tonic::transport::Endpoint::from_shared(server.client_url())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut maintenance = MaintenanceClient::new(channel.clone());
        for _ in 0..3 {
            // No SST exists yet, so the size is the logical size of the overlay.
            let reply = maintenance
                .status(StatusRequest {})
                .await
                .unwrap()
                .into_inner();
            assert_eq!(reply.db_size, reply.db_size_in_use);
        }
        faults.set(0);
        let mut kv = KvClient::new(channel);
        kv.put(PutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
        eventually("the first checkpoint", async || {
            status(&server).await["shared_state"]["applied_view_checkpoints"]
                .as_u64()
                .unwrap()
                > 0
        })
        .await;
        maintenance.status(StatusRequest {}).await.unwrap();
        server.shutdown().await.unwrap();
    }

    /// A restart whose base and snapshot record share an index must agree on its term.
    #[tokio::test]
    async fn a_restart_refuses_a_base_with_another_term_at_the_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = RaftStorage::open(dir.path(), 1, "test", vec![1]).unwrap();
        let mut snapshot = raft::eraftpb::Snapshot::default();
        snapshot.mut_metadata().index = 1;
        snapshot.mut_metadata().term = 3;
        snapshot.data = crate::state::transfer::ViewDescriptor::new(uuid::Uuid::new_v4())
            .encode()
            .unwrap();
        storage.adopt(&snapshot).unwrap();
        let options = crate::state::shared::Options {
            quota: 0,
            voters: vec![1],
            join: false,
            empty_log: false,
            pin_max: Duration::from_secs(300),
            restore: None,
            record: None,
        };
        let mut state = StateMachine::open_shared(
            Arc::new(InMemory::new()),
            "root",
            "test",
            crate::state::StateCache::default(),
            &options,
            crate::state::transfer::Backend::Local,
        )
        .await
        .unwrap();
        state.entry_term = 2;
        state.apply(1, None).await.unwrap();
        let error = Node::align_log(&mut storage, &state).unwrap_err();
        assert!(error.to_string().contains("differs"), "{error}");
        state.entry_term = 3;
        state.applied_term = 3;
        Node::align_log(&mut storage, &state).unwrap();
        state.close().await.unwrap();
    }

    /// One key changes 16 times. A Watch with prev_kv catches up from the base with a short pin
    /// and slow object reads. It receives every revision once, each with its previous value.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_slow_prev_kv_catch_up_makes_progress() {
        use crate::object::faults::{FaultStore, DELAY_SST_GET};
        use crate::proto::etcdserverpb::{
            kv_client::KvClient, watch_client::WatchClient, watch_request, PutRequest,
            WatchCreateRequest, WatchRequest,
        };
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::server::ServerConfig::single_node(dir.path());
        config.state_mode = crate::state::StateMode::Shared;
        config.object_store = crate::object::Config::Test(faults.clone(), "root".into());
        config.view_max_age = Duration::from_millis(100);
        config.view_pin_max = Duration::from_millis(24);
        // A tiny cache, so history reads go to the store.
        config.state_cache = crate::state::StateCache {
            block_bytes: 1,
            meta_bytes: 1,
            row_bytes: 0,
        };
        let server = crate::server::Server::start(config).await.unwrap();
        server.wait_ready(Duration::from_secs(15)).await.unwrap();
        let channel = tonic::transport::Endpoint::from_shared(server.client_url())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut kv = KvClient::new(channel.clone());
        let mut revisions = vec![];
        for n in 0..16 {
            let reply = kv
                .put(PutRequest {
                    key: b"key".to_vec(),
                    value: format!("v{n}").into_bytes(),
                    ..Default::default()
                })
                .await
                .unwrap();
            revisions.push(reply.into_inner().header.unwrap().revision);
        }
        // The base covers every write when the overlay holds no batch. The applied index can be
        // higher: a later `ViewCheckpoint` entry changes no state, so no base reaches it.
        eventually("a base that covers the writes", async || {
            let status = status(&server).await;
            status["shared_state"]["overlay_bytes"].as_u64() == Some(0)
                && status["shared_state"]["base_applied_index"].as_u64() > Some(0)
        })
        .await;
        faults.set_delay(Duration::from_millis(4));
        faults.set(DELAY_SST_GET);
        let (send, receive) = mpsc::channel(1);
        send.send(WatchRequest {
            request_union: Some(watch_request::RequestUnion::CreateRequest(
                WatchCreateRequest {
                    key: b"key".to_vec(),
                    start_revision: revisions[0],
                    prev_kv: true,
                    ..Default::default()
                },
            )),
        })
        .await
        .unwrap();
        let mut stream = WatchClient::new(channel)
            .watch(tokio_stream::wrappers::ReceiverStream::new(receive))
            .await
            .unwrap()
            .into_inner();
        let mut events = vec![];
        while events.len() < revisions.len() {
            let message = tokio::time::timeout(Duration::from_secs(20), stream.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            events.extend(message.events);
        }
        let seen: Vec<_> = events
            .iter()
            .map(|event| event.kv.as_ref().unwrap().mod_revision)
            .collect();
        assert_eq!(seen, revisions);
        for (n, event) in events.iter().enumerate().skip(1) {
            let previous = event.prev_kv.as_ref().expect("prev_kv");
            assert_eq!(previous.value, format!("v{}", n - 1).into_bytes());
        }
        faults.set(0);
        drop(send);
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn a_new_writer_waits_for_its_clock_and_refuses_a_large_skew() {
        use std::sync::atomic::Ordering;
        let clock = Clock::system(Duration::from_secs(5), None);
        // The manifest clock is 300 ms ahead. The writer waits until the local clock passes it.
        let started = Instant::now();
        assert!(clock.pass((clock.now_ms)() + 300).await);
        assert!(started.elapsed() >= Duration::from_millis(250));
        assert!(!clock.alarm.load(Ordering::Relaxed));
        // A clock 10 s behind does not start the writer, and it raises the alarm.
        let started = Instant::now();
        assert!(!clock.pass((clock.now_ms)() + 10_000).await);
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(clock.alarm.load(Ordering::Relaxed));
        assert!(clock.skew.load(Ordering::Relaxed) >= 9_000);
        // A later check within the limit clears the alarm.
        assert!(clock.pass((clock.now_ms)() - 1).await);
        assert!(!clock.alarm.load(Ordering::Relaxed));
    }

    /// The open of an older term finishes after the current leader's open and fences it. The
    /// current leader opens its writer again, and its overlay batches still reach a checkpoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_fenced_leader_opens_its_writer_again() {
        let options = crate::state::shared::Options {
            quota: 0,
            voters: vec![1],
            join: false,
            empty_log: false,
            pin_max: Duration::from_secs(300),
            restore: None,
            record: None,
        };
        let mut state = StateMachine::open_shared(
            Arc::new(InMemory::new()),
            "root",
            "test",
            crate::state::StateCache::default(),
            &options,
            crate::state::transfer::Backend::Local,
        )
        .await
        .unwrap();
        state.members = Arc::new(crate::cluster::Store::initial(
            &[(1, "127.0.0.1:1".to_owned())].into(),
        ));
        state.seed_for_test(1).await.unwrap();
        let handles = state.shared_handles().unwrap();
        let (events, mut announced) = mpsc::channel(64);
        let clock = Clock::system(Duration::from_secs(5), None);
        let mut writer = Writer::new(&handles, Duration::from_millis(50), clock);
        writer.sync(true, 2, &events);
        writer.progress.wait_written(1).await.unwrap();
        // The leader of term 1 opens late. Its open fences the writer of term 2. The current
        // leader can open again before this open ends, which fails the late open instead.
        let old = handles.source.open_writer().await.ok();
        let put = |key: &str| crate::proto::etcdserverpb::RequestOp {
            request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                crate::proto::etcdserverpb::PutRequest {
                    key: key.as_bytes().to_vec(),
                    value: b"value".to_vec(),
                    ..Default::default()
                },
            )),
        };
        state.apply_etcd(2, &put("after-fence")).await.unwrap();
        // A checkpoint above the fence appears. The writer task did not stop the member.
        let covered = tokio::time::timeout(Duration::from_secs(20), async {
            while let Some(event) = announced.recv().await {
                if let Event::ViewCheckpoint(id, _, reply) = event {
                    if let Some(reply) = reply {
                        let _ = reply.send(true);
                    }
                    let base = handles.source.open_base(id).await.unwrap();
                    if base.applied >= 2 {
                        return base;
                    }
                }
            }
            panic!("the writer stopped");
        })
        .await
        .unwrap();
        let value = covered.get(b"etcd/current/after-fence").await.unwrap();
        assert!(value.is_some());
        assert!(writer.tasks.try_join_next().is_none());
        // The current leader saw the fence and opened its writer again.
        assert!(writer.reopens.load(std::sync::atomic::Ordering::Relaxed) >= 1);
        // The writer of term 1 is fenced now. Its next flush cannot commit a manifest.
        if let Some(old) = old {
            let flushed = async {
                old.put(b"orphan", b"value").await?;
                old.flush().await
            };
            let error = flushed.await.unwrap_err();
            assert!(fenced(&anyhow::Error::from(error)));
        }
        writer.sync(false, 2, &events);
        writer.stop().await;
        state.close().await.unwrap();
    }

    /// The announcement events of a publisher. The test answers for the node.
    struct Announcements(mpsc::Receiver<Event>);

    impl Announcements {
        /// Wait for the next announcement and reply whether Raft took it.
        async fn next(&mut self, wait: Duration, accepted: bool) -> uuid::Uuid {
            tokio::time::timeout(wait, async {
                loop {
                    if let Some(Event::ViewCheckpoint(id, _, reply)) = self.0.recv().await {
                        if let Some(reply) = reply {
                            let _ = reply.send(accepted);
                        }
                        return id;
                    }
                }
            })
            .await
            .expect("an announcement")
        }

        /// Wait `wait` and fail on any announcement in it.
        async fn none(&mut self, wait: Duration) {
            let next = tokio::time::timeout(wait, self.0.recv()).await;
            assert!(
                !matches!(next, Ok(Some(Event::ViewCheckpoint(..)))),
                "an unexpected announcement"
            );
        }
    }

    /// Open a one-voter shared state. A first writer publishes a checkpoint at index 1, and its
    /// announcement never reaches Raft. Return the state, its handles, the event sender, the
    /// announcements, and the stranded checkpoint.
    async fn stranded_checkpoint() -> (
        StateMachine,
        Handles,
        mpsc::Sender<Event>,
        Announcements,
        uuid::Uuid,
    ) {
        let options = crate::state::shared::Options {
            quota: 0,
            voters: vec![1],
            join: false,
            empty_log: false,
            pin_max: Duration::from_secs(300),
            restore: None,
            record: None,
        };
        let mut state = StateMachine::open_shared(
            Arc::new(InMemory::new()),
            "root",
            "test",
            crate::state::StateCache::default(),
            &options,
            crate::state::transfer::Backend::Local,
        )
        .await
        .unwrap();
        state.members = Arc::new(crate::cluster::Store::initial(
            &[(1, "127.0.0.1:1".to_owned())].into(),
        ));
        state.seed_for_test(1).await.unwrap();
        let handles = state.shared_handles().unwrap();
        let (events, received) = mpsc::channel(64);
        let mut announcements = Announcements(received);
        let mut first = Writer::new(&handles, STRANDED_AGE, clock());
        first.sync(true, 2, &events);
        let lost = announcements.next(Duration::from_secs(20), true).await;
        first.sync(false, 2, &events);
        first.stop().await;
        (state, handles, events, announcements, lost)
    }

    const STRANDED_AGE: Duration = Duration::from_millis(50);

    fn clock() -> Clock {
        Clock::system(Duration::from_secs(5), None)
    }

    /// The next writer has no writes. It announces the stranded checkpoint, and Raft drops the
    /// proposal, as during a leader transfer. On the idle cluster the publisher announces it
    /// again within the backoff limit. The base advances and a physical compaction waiter ends.
    /// The adopted checkpoint causes no further announcement, also from a later writer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_new_writer_announces_a_stranded_checkpoint() {
        let (mut state, handles, events, mut announcements, lost) = stranded_checkpoint().await;
        // A physical compaction waits for a base that covers index 1.
        let mut based = handles.based.clone();
        assert_eq!(*based.borrow(), 0);
        let waiter = tokio::spawn(async move { based.wait_for(|base| *base >= 1).await.is_ok() });
        let mut second = Writer::new(&handles, STRANDED_AGE, clock());
        second.sync(true, 3, &events);
        assert_eq!(
            announcements.next(Duration::from_secs(2), false).await,
            lost
        );
        // No write and no request follows. The retry timer sends it again.
        let limit = RETRY_MAX + Duration::from_millis(500);
        assert_eq!(announcements.next(limit, true).await, lost);
        state
            .apply_view_checkpoint(2, &lost.to_string(), 0)
            .await
            .unwrap();
        state.settle_opens().await;
        state.switch_base();
        assert!(tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap());
        // Requests and retries over several flush ages do not announce it again.
        for _ in 0..4 {
            second.request_publication(1);
            announcements.none(STRANDED_AGE).await;
        }
        announcements.none(limit).await;
        second.sync(false, 3, &events);
        second.stop().await;
        // A later writer does not announce it either.
        let mut third = Writer::new(&handles, STRANDED_AGE, clock());
        third.sync(true, 4, &events);
        third.progress.wait_written(1).await.unwrap();
        third.request_publication(1);
        announcements.none(limit).await;
        assert!(third.tasks.try_join_next().is_none());
        third.sync(false, 4, &events);
        third.stop().await;
        state.close().await.unwrap();
    }

    /// A storage error in the stranded check does not drop it. The publisher checks again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_stranded_check_is_retried() {
        let (state, handles, events, mut announcements, lost) = stranded_checkpoint().await;
        let mut second = Writer::new(&handles, STRANDED_AGE, clock());
        let failures = second.applied.failures.clone();
        failures.store(3, std::sync::atomic::Ordering::Relaxed);
        second.sync(true, 3, &events);
        let limit = RETRY_MAX + Duration::from_secs(1);
        assert_eq!(announcements.next(limit, true).await, lost);
        assert_eq!(failures.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert!(second.tasks.try_join_next().is_none());
        second.sync(false, 3, &events);
        second.stop().await;
        state.close().await.unwrap();
    }

    /// The announcement of a checkpoint applies, and the member cannot read the checkpoint.
    /// After the store recovers, the member opens it and switches its base. No client write
    /// and no new Raft entry is needed, and a physical compaction waiter ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_base_open_is_retried_on_an_idle_member() {
        use crate::object::faults::{FaultStore, FAIL_GET};
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let options = crate::state::shared::Options {
            quota: 0,
            voters: vec![1],
            join: false,
            empty_log: false,
            pin_max: Duration::from_secs(300),
            restore: None,
            record: None,
        };
        // A tiny cache, so the open reads the store.
        let cache = crate::state::StateCache {
            block_bytes: 1,
            meta_bytes: 1,
            row_bytes: 0,
        };
        let mut state = StateMachine::open_shared(
            faults.clone(),
            "root",
            "test",
            cache,
            &options,
            crate::state::transfer::Backend::Local,
        )
        .await
        .unwrap();
        state.members = Arc::new(crate::cluster::Store::initial(
            &[(1, "127.0.0.1:1".to_owned())].into(),
        ));
        state.seed_for_test(1).await.unwrap();
        let handles = state.shared_handles().unwrap();
        let (events, received) = mpsc::channel(64);
        let mut announcements = Announcements(received);
        let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
        writer.sync(true, 2, &events);
        let id = announcements.next(Duration::from_secs(20), true).await;
        writer.sync(false, 2, &events);
        writer.stop().await;
        let mut based = handles.based.clone();
        let waiter = tokio::spawn(async move { based.wait_for(|base| *base >= 1).await.is_ok() });
        // The announcement applies while every read fails.
        faults.set(FAIL_GET);
        state
            .apply_view_checkpoint(2, &id.to_string(), 0)
            .await
            .unwrap();
        let applied = state.applied;
        faults.entered.acquire().await.unwrap().forget();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(handles.opened.newest(), None);
        assert_eq!(*handles.based.borrow(), 0);
        // The store recovers. Only the retry of the open runs.
        faults.set(0);
        let mut ready = handles.opened.subscribe();
        tokio::time::timeout(
            RETRY_MAX + Duration::from_secs(2),
            ready.wait_for(|ready| *ready >= 1),
        )
        .await
        .unwrap()
        .unwrap();
        state.switch_base();
        assert!(tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap());
        assert_eq!(state.applied, applied);
        state.close().await.unwrap();
    }

    /// Announcements arrive faster than one open takes. A started open finishes, so the base
    /// advances while announcements still arrive, and it reaches the newest checkpoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_base_advances_under_a_stream_of_announcements() {
        use crate::object::faults::{FaultStore, DELAY_SST_GET};
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let options = crate::state::shared::Options {
            quota: 0,
            voters: vec![1],
            join: false,
            empty_log: false,
            pin_max: Duration::from_secs(300),
            restore: None,
            record: None,
        };
        // A tiny cache, so each open reads the store.
        let cache = crate::state::StateCache {
            block_bytes: 1,
            meta_bytes: 1,
            row_bytes: 0,
        };
        let mut state = StateMachine::open_shared(
            faults.clone(),
            "root",
            "test",
            cache,
            &options,
            crate::state::transfer::Backend::Local,
        )
        .await
        .unwrap();
        state.members = Arc::new(crate::cluster::Store::initial(
            &[(1, "127.0.0.1:1".to_owned())].into(),
        ));
        state.seed_for_test(1).await.unwrap();
        let handles = state.shared_handles().unwrap();
        let (events, received) = mpsc::channel(64);
        let mut announcements = Announcements(received);
        let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
        writer.sync(true, 2, &events);
        // Sixteen checkpoints, each after one more write. Raft takes none of them yet.
        let mut checkpoints = vec![announcements.next(Duration::from_secs(20), true).await];
        for n in 0..15 {
            let put = crate::proto::etcdserverpb::RequestOp {
                request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                    crate::proto::etcdserverpb::PutRequest {
                        key: format!("key-{n}").into_bytes(),
                        value: b"value".to_vec(),
                        ..Default::default()
                    },
                )),
            };
            let index = state.applied + 1;
            state.apply_etcd(index, &put).await.unwrap();
            loop {
                let id = announcements.next(Duration::from_secs(20), true).await;
                let base = handles.source.open_base(id).await.unwrap();
                if base.applied == index {
                    checkpoints.push(id);
                    break;
                }
            }
        }
        writer.sync(false, 2, &events);
        writer.stop().await;
        let newest = state.applied;
        // Each object read takes 70 ms. The announcements come 40 ms apart.
        faults.set_delay(Duration::from_millis(70));
        faults.set(DELAY_SST_GET);
        let mut advanced = 0;
        for id in &checkpoints {
            let index = state.applied + 1;
            state
                .apply_view_checkpoint(index, &id.to_string(), 0)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(40)).await;
            state.switch_base();
            advanced = advanced.max(*handles.based.borrow());
        }
        assert!(
            advanced >= 1,
            "the base did not advance during the announcements"
        );
        let mut ready = handles.opened.subscribe();
        tokio::time::timeout(
            Duration::from_secs(20),
            ready.wait_for(|ready| *ready >= newest),
        )
        .await
        .unwrap()
        .unwrap();
        state.switch_base();
        assert_eq!(*handles.based.borrow(), newest);
        faults.set(0);
        state.close().await.unwrap();
    }

    /// A manual checkpoint arrives after its entry commits and before the entry applies. The
    /// publication request waits for apply, so it covers the batch of that entry.
    #[tokio::test]
    async fn a_manual_checkpoint_requests_its_publication_after_apply() {
        let (mut state, handles, _events, _announcements, _) = stranded_checkpoint().await;
        let mut writer = Writer::new(&handles, Duration::from_secs(60), clock());
        let index = state.applied + 1;
        // Committed at `index`, applied below it: no request yet.
        assert!(!writer.request_checkpoint(index, index - 1));
        let put = crate::proto::etcdserverpb::RequestOp {
            request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                crate::proto::etcdserverpb::PutRequest {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                    ..Default::default()
                },
            )),
        };
        state.apply_etcd(index, &put).await.unwrap();
        assert!(writer.request_checkpoint(index, index));
        assert_eq!(
            writer.progress.inner.lock().unwrap().publish_through,
            Some(index)
        );
        assert!(!writer.request_checkpoint(index, index));
        state.close().await.unwrap();
    }

    /// Apply announcement `id` at the next index and switch to its base.
    async fn adopt_announced(state: &mut StateMachine, id: uuid::Uuid) {
        let index = state.applied + 1;
        state
            .apply_view_checkpoint(index, &id.to_string(), 0)
            .await
            .unwrap();
        state.settle_opens().await;
        state.switch_base();
        assert_eq!(state.base_checkpoint(), Some(id));
    }

    /// A Defragment publishes a newer checkpoint at the base index, and the leader changes
    /// before its announcement applies. The next writer announces it, because the identity of
    /// the newest checkpoint differs from the base, and the members adopt it with no write.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_new_writer_announces_a_same_index_checkpoint() {
        let (mut state, handles, events, mut announcements, first) = stranded_checkpoint().await;
        adopt_announced(&mut state, first).await;
        let mut second = Writer::new(&handles, STRANDED_AGE, clock());
        second.sync(true, 3, &events);
        second.progress.wait_written(1).await.unwrap();
        announcements.none(Duration::from_millis(300)).await;
        // The Defragment republish. Its announcement does not apply before the change.
        second.progress.republish();
        let compacted = announcements.next(Duration::from_secs(5), true).await;
        assert_ne!(compacted, first);
        let base = handles.source.open_base(compacted).await.unwrap();
        assert_eq!(base.applied, state.shared.as_ref().unwrap().base.applied);
        drop(base);
        second.sync(false, 3, &events);
        second.stop().await;
        let mut third = Writer::new(&handles, STRANDED_AGE, clock());
        third.sync(true, 4, &events);
        assert_eq!(
            announcements.next(Duration::from_secs(2), true).await,
            compacted
        );
        adopt_announced(&mut state, compacted).await;
        announcements
            .none(RETRY_MAX + Duration::from_millis(500))
            .await;
        third.sync(false, 4, &events);
        third.stop().await;
        state.close().await.unwrap();
    }

    /// A manual checkpoint requests its publication after apply and before the newly elected
    /// writer opens. The open keeps the request, so the writer publishes and announces the
    /// covering checkpoint at once, not after the flush age.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_checkpoint_request_survives_the_writer_open() {
        let (mut state, handles, events, mut announcements, first) = stranded_checkpoint().await;
        adopt_announced(&mut state, first).await;
        let put = crate::proto::etcdserverpb::RequestOp {
            request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                crate::proto::etcdserverpb::PutRequest {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                    ..Default::default()
                },
            )),
        };
        let index = state.applied + 1;
        state.apply_etcd(index, &put).await.unwrap();
        let mut writer = Writer::new(&handles, Duration::from_secs(90), clock());
        assert!(writer.request_checkpoint(index, index));
        writer.sync(true, 3, &events);
        let id = announcements.next(Duration::from_secs(2), true).await;
        assert_eq!(handles.source.open_base(id).await.unwrap().applied, index);
        writer.sync(false, 3, &events);
        writer.stop().await;
        state.close().await.unwrap();
    }

    /// A one-voter shared state on `store`, opened without a restore, with its handles.
    async fn ordinary(store: Arc<dyn ObjectStore>) -> (StateMachine, Handles) {
        let options = crate::state::shared::Options {
            quota: 0,
            voters: vec![1],
            join: false,
            empty_log: true,
            pin_max: Duration::from_secs(300),
            restore: None,
            record: None,
        };
        let mut state = StateMachine::open_shared(
            store,
            "root",
            "test",
            crate::state::StateCache::default(),
            &options,
            crate::state::transfer::Backend::Local,
        )
        .await
        .unwrap();
        state.members = Arc::new(crate::cluster::Store::initial(
            &[(1, "127.0.0.1:1".to_owned())].into(),
        ));
        let handles = state.shared_handles().unwrap();
        (state, handles)
    }

    fn snapshot(id: &str) -> crate::state::shared::Restore {
        let members = crate::cluster::Store::initial(&[(1, "127.0.0.1:1".to_owned())].into());
        crate::state::shared::Restore::new(
            vec![
                (crate::state::APPLIED.to_vec(), 1_u64.to_be_bytes().to_vec()),
                (
                    crate::cluster::KEY.to_vec(),
                    serde_json::to_vec(&members).unwrap(),
                ),
                (b"etcd/restored".to_vec(), id.as_bytes().to_vec()),
            ],
            id.to_owned(),
        )
    }

    /// A restore claims the bootstrap and stops before its marker. An ordinary member then
    /// opens without the restore flag. It does not select an empty base: it waits. The restore
    /// completes, and the ordinary member opens on the restored checkpoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_ordinary_start_waits_for_a_pending_restore() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let source = crate::state::shared::offline_source(
            store.clone(),
            "root",
            crate::state::StateCache::default(),
        )
        .unwrap();
        let restore = snapshot("a");
        crate::state::shared::seed_rows_for_test(&source, &restore)
            .await
            .unwrap();
        let opening = tokio::spawn(ordinary(store.clone()));
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(!opening.is_finished());
        let admin = slatedb::admin::Admin::builder(source.state().as_str(), store).build();
        crate::state::shared::seed(&source, &admin, &restore)
            .await
            .unwrap();
        let (state, _handles) = opening.await.unwrap();
        let base = state.shared.as_ref().unwrap().base.clone();
        assert_eq!(base.applied, 1);
        assert_eq!(
            base.get(b"etcd/restored").await.unwrap().as_deref(),
            Some(b"a".as_slice())
        );
        drop(base);
        state.close().await.unwrap();
    }

    /// An ordinary leader bootstraps first. A later restore into that cluster fails, and tells
    /// the operator that a restore needs a new cluster prefix.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restore_after_an_ordinary_bootstrap_fails() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (mut state, handles) = ordinary(store.clone()).await;
        state.seed_for_test(1).await.unwrap();
        let (events, received) = mpsc::channel(64);
        let mut announcements = Announcements(received);
        let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
        writer.sync(true, 2, &events);
        announcements.next(Duration::from_secs(20), true).await;
        assert!(handles.source.bootstrapped().await.unwrap());
        let admin =
            slatedb::admin::Admin::builder(handles.source.state().as_str(), store.clone()).build();
        let error = crate::state::shared::seed(&handles.source, &admin, &snapshot("a"))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("new cluster prefix"),
            "{error:#}"
        );
        writer.sync(false, 2, &events);
        writer.stop().await;
        state.close().await.unwrap();
    }

    /// A restore stops after its rows, or after its checkpoint, before its marker. An ordinary
    /// server start never serves the empty base while the restore claim is pending. After the
    /// rows it fails after its wait. After the checkpoint it completes the marker with the
    /// snapshot checksum and starts. A retry of the same snapshot then succeeds, and an ordinary
    /// start follows the restore.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_ordinary_start_resolves_a_stopped_restore() {
        for checkpointed in [false, true] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let source = crate::state::shared::offline_source(
                store.clone(),
                "root",
                crate::state::StateCache::default(),
            )
            .unwrap();
            let admin =
                slatedb::admin::Admin::builder(source.state().as_str(), store.clone()).build();
            let restore = snapshot("a");
            if checkpointed {
                crate::state::shared::seed_checkpoint_for_test(&source, &restore)
                    .await
                    .unwrap();
            } else {
                crate::state::shared::seed_rows_for_test(&source, &restore)
                    .await
                    .unwrap();
            }
            let dir = tempfile::tempdir().unwrap();
            let mut config = crate::server::ServerConfig::single_node(dir.path());
            config.state_mode = crate::state::StateMode::Shared;
            config.object_store = crate::object::Config::Test(store.clone(), "root".into());
            let started = tokio::time::timeout(
                Duration::from_secs(30),
                crate::server::Server::start(config.clone()),
            )
            .await
            .unwrap();
            if checkpointed {
                let server = started.unwrap();
                server.wait_ready(Duration::from_secs(15)).await.unwrap();
                server.shutdown().await.unwrap();
                assert_eq!(
                    source.restored_snapshot().await.unwrap().as_deref(),
                    Some("a")
                );
            } else {
                let error = started.err().unwrap();
                assert!(format!("{error:#}").contains("pending"), "{error:#}");
                assert!(!source.bootstrapped().await.unwrap());
            }
            // The same snapshot restores with success at either boundary.
            crate::state::shared::seed(&source, &admin, &restore)
                .await
                .unwrap();
            assert_eq!(
                source.restored_snapshot().await.unwrap().as_deref(),
                Some("a")
            );
            let dir = tempfile::tempdir().unwrap();
            let mut config = crate::server::ServerConfig::single_node(dir.path());
            config.state_mode = crate::state::StateMode::Shared;
            config.object_store = crate::object::Config::Test(store.clone(), "root".into());
            let server = crate::server::Server::start(config).await.unwrap();
            server.wait_ready(Duration::from_secs(15)).await.unwrap();
            server.shutdown().await.unwrap();
        }
    }

    /// A MoveLeader requests its publication while the new writer is still opening, after
    /// apply passed the last published index. The open keeps the request, so the writer
    /// publishes through the requested index at once, well before the 3 s transfer fallback.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_transfer_request_during_the_writer_open_is_published() {
        let (mut state, handles, events, mut announcements, first) = stranded_checkpoint().await;
        adopt_announced(&mut state, first).await;
        for n in 0..5 {
            let put = crate::proto::etcdserverpb::RequestOp {
                request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                    crate::proto::etcdserverpb::PutRequest {
                        key: format!("key-{n}").into_bytes(),
                        value: b"value".to_vec(),
                        ..Default::default()
                    },
                )),
            };
            let index = state.applied + 1;
            state.apply_etcd(index, &put).await.unwrap();
        }
        let applied = state.applied;
        let mut writer = Writer::new(&handles, Duration::from_secs(90), clock());
        writer.sync(true, 3, &events);
        // MoveLeader calls this with the applied index. The writer has not opened yet.
        writer.request_publication(applied);
        let started = Instant::now();
        let id = announcements.next(Duration::from_secs(2), true).await;
        assert!(started.elapsed() < Duration::from_secs(3));
        assert_eq!(handles.source.open_base(id).await.unwrap().applied, applied);
        writer.sync(false, 3, &events);
        writer.stop().await;
        state.close().await.unwrap();
    }

    /// The leader changes after the Defragment compaction and before the republish. The call
    /// returns a retryable error. A retry on the new leader publishes the compacted checkpoint,
    /// returns after its announcement applies, and the base then has one run and no L0 SST.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_defragment_that_loses_the_lead_returns_a_retryable_error() {
        let (mut state, handles, events, mut announcements, first) = stranded_checkpoint().await;
        adopt_announced(&mut state, first).await;
        let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
        writer.sync(true, 3, &events);
        writer.progress.wait_written(1).await.unwrap();
        for n in 0..5 {
            let put = crate::proto::etcdserverpb::RequestOp {
                request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                    crate::proto::etcdserverpb::PutRequest {
                        key: format!("key-{n}").into_bytes(),
                        value: b"value".to_vec(),
                        ..Default::default()
                    },
                )),
            };
            let index = state.applied + 1;
            state.apply_etcd(index, &put).await.unwrap();
        }
        // The aged flush publishes a checkpoint with an L0 SST. The members adopt it.
        let id = announcements.next(Duration::from_secs(10), true).await;
        adopt_announced(&mut state, id).await;
        let db = writer.open.lock().unwrap().clone().unwrap();
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let defragment = Defragment {
            db,
            admin: handles.admin.clone(),
            progress: writer.progress.clone(),
            announced: handles.announced.clone(),
            leadership: writer.leadership.subscribe(),
            term: 3,
            pause: Some((DefragmentStep::Publish, reached.clone(), resume.clone())),
        };
        let running = tokio::spawn(defragment.run());
        reached.notified().await;
        writer.sync(false, 3, &events);
        writer.stop().await;
        resume.notify_one();
        let error = running.await.unwrap().unwrap_err();
        assert_eq!(error.code, tonic::Code::Unavailable as i32, "{error:?}");
        // The retry on the next leader.
        let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
        writer.sync(true, 4, &events);
        let db = loop {
            if let Some(db) = writer.open.lock().unwrap().clone() {
                break db;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let defragment = Defragment {
            db,
            admin: handles.admin.clone(),
            progress: writer.progress.clone(),
            announced: handles.announced.clone(),
            leadership: writer.leadership.subscribe(),
            term: 4,
            pause: None,
        };
        let running = tokio::spawn(defragment.run());
        let compacted = announcements.next(Duration::from_secs(20), true).await;
        adopt_announced(&mut state, compacted).await;
        running.await.unwrap().unwrap();
        let checkpoint = handles
            .admin
            .list_checkpoints(None)
            .await
            .unwrap()
            .into_iter()
            .find(|checkpoint| Some(checkpoint.id) == state.base_checkpoint())
            .unwrap();
        let manifest = handles
            .admin
            .read_manifest(Some(checkpoint.manifest_id))
            .await
            .unwrap()
            .unwrap();
        assert!(manifest.l0().is_empty());
        assert_eq!(manifest.compacted().len(), 1);
        writer.sync(false, 4, &events);
        writer.stop().await;
        state.close().await.unwrap();
    }

    /// A leader writer at term 3 with five more writes after the adopted first checkpoint.
    async fn defragment_setup() -> (
        StateMachine,
        Handles,
        mpsc::Sender<Event>,
        Announcements,
        Writer,
    ) {
        let (mut state, handles, events, mut announcements, first) = stranded_checkpoint().await;
        adopt_announced(&mut state, first).await;
        let mut writer = Writer::new(&handles, STRANDED_AGE, clock());
        writer.sync(true, 3, &events);
        writer.progress.wait_written(1).await.unwrap();
        for n in 0..5 {
            let put = crate::proto::etcdserverpb::RequestOp {
                request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                    crate::proto::etcdserverpb::PutRequest {
                        key: format!("key-{n}").into_bytes(),
                        value: b"value".to_vec(),
                        ..Default::default()
                    },
                )),
            };
            let index = state.applied + 1;
            state.apply_etcd(index, &put).await.unwrap();
        }
        let _ = &mut announcements;
        (state, handles, events, announcements, writer)
    }

    fn defragment(
        handles: &Handles,
        writer: &Writer,
        term: u64,
        pause: Option<(
            DefragmentStep,
            Arc<tokio::sync::Notify>,
            Arc<tokio::sync::Notify>,
        )>,
    ) -> Defragment {
        Defragment {
            db: writer.open.lock().unwrap().clone().unwrap(),
            admin: handles.admin.clone(),
            progress: writer.progress.clone(),
            announced: handles.announced.clone(),
            leadership: writer.leadership.subscribe(),
            term,
            pause,
        }
    }

    /// The announcement of a checkpoint from before the Defragment waits in Raft. It applies
    /// after the compaction. Defragment does not count it, because its layout still holds the
    /// input SSTs. It returns only after the compacted checkpoint applies.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_defragment_ignores_an_older_checkpoint_that_applies_late() {
        check_delayed_checkpoint(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_defragment_ignores_a_checkpoint_from_before_a_prior_compaction() {
        check_delayed_checkpoint(true).await;
    }

    async fn check_delayed_checkpoint(compact_before_defragment: bool) {
        let (mut state, handles, events, mut announcements, mut writer) = defragment_setup().await;
        // Hold the announcement of the aged flush: no reply, no apply.
        let (old, reply) = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(Event::ViewCheckpoint(id, _, reply)) = announcements.0.recv().await {
                    return (id, reply.unwrap());
                }
            }
        })
        .await
        .unwrap();
        if compact_before_defragment {
            // Rewrite the SSTs without advancing the applied announcement. The delayed
            // checkpoint now shares no input SSTs with the upcoming Defragment.
            crate::state::maintenance::compact_all(&handles.admin)
                .await
                .unwrap();
        }
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let running = tokio::spawn(
            defragment(
                &handles,
                &writer,
                3,
                Some((DefragmentStep::Publish, reached.clone(), resume.clone())),
            )
            .run(),
        );
        reached.notified().await;
        resume.notify_one();
        let _ = reply.send(true);
        adopt_announced(&mut state, old).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !running.is_finished(),
            "Defragment counted the older checkpoint"
        );
        let compacted = announcements.next(Duration::from_secs(10), true).await;
        assert_ne!(compacted, old);
        adopt_announced(&mut state, compacted).await;
        running.await.unwrap().unwrap();
        let checkpoint = handles
            .admin
            .list_checkpoints(None)
            .await
            .unwrap()
            .into_iter()
            .find(|checkpoint| checkpoint.id == compacted)
            .unwrap();
        let manifest = handles
            .admin
            .read_manifest(Some(checkpoint.manifest_id))
            .await
            .unwrap()
            .unwrap();
        assert!(manifest.l0().is_empty());
        assert_eq!(manifest.compacted().len(), 1);
        writer.sync(false, 3, &events);
        writer.stop().await;
        state.close().await.unwrap();
    }

    /// A step down during the flush or the compaction closes the writer. The storage error
    /// then becomes the retryable Unavailable error, not an internal error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_step_down_during_defragment_storage_work_is_retryable() {
        for step in [DefragmentStep::Flush, DefragmentStep::Compact] {
            let (state, handles, events, mut announcements, mut writer) = defragment_setup().await;
            // The aged flush leaves an L0 SST for the compaction.
            announcements.next(Duration::from_secs(10), true).await;
            let reached = Arc::new(tokio::sync::Notify::new());
            let resume = Arc::new(tokio::sync::Notify::new());
            let running = tokio::spawn(
                defragment(
                    &handles,
                    &writer,
                    3,
                    Some((step, reached.clone(), resume.clone())),
                )
                .run(),
            );
            reached.notified().await;
            writer.sync(false, 3, &events);
            writer.stop().await;
            resume.notify_one();
            let error = running.await.unwrap().unwrap_err();
            assert_eq!(
                error.code,
                tonic::Code::Unavailable as i32,
                "{step:?}: {error:?}"
            );
            state.close().await.unwrap();
        }
    }

    /// An adopt across membership changes leaves Raft with the checkpoint's ConfState, also
    /// after an election.
    #[tokio::test]
    async fn an_adopt_takes_the_checkpoint_membership() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = super::super::tests::leader_node(dir.path()).await;
        // The skipped entries added learner 4, promoted it, added learner 5, and removed 3.
        let target = raft::eraftpb::ConfState::from((vec![1, 2, 4], vec![5]));
        reconcile_conf(&mut node.raw, &target).unwrap();
        let conf = |node: &Node| {
            let mut conf = node.raw.raft.prs().conf().to_conf_state();
            conf.voters.sort_unstable();
            conf.learners.sort_unstable();
            conf
        };
        assert_eq!(conf(&node), target);
        // A new election asks the new voters, not the removed one or the learner.
        let term = node.raw.raft.term;
        node.raw.raft.become_follower(term + 1, raft::INVALID_ID);
        node.raw.raft.msgs.clear();
        node.raw.campaign().unwrap();
        assert_eq!(conf(&node), target);
        let mut asked: Vec<_> = node
            .raw
            .raft
            .msgs
            .iter()
            .map(|message| message.to)
            .collect();
        asked.sort_unstable();
        asked.dedup();
        assert_eq!(asked, [2, 4]);
        // A second reconcile changes nothing.
        reconcile_conf(&mut node.raw, &target).unwrap();
        assert_eq!(conf(&node), target);
    }

    /// A member steps down and leads again before it collects the old task. The old term's
    /// normal end is not a failure of the new term.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_old_term_task_end_is_not_a_failure() {
        let options = crate::state::shared::Options {
            quota: 0,
            voters: vec![1],
            join: false,
            empty_log: false,
            pin_max: Duration::from_secs(300),
            restore: None,
            record: None,
        };
        let mut state = StateMachine::open_shared(
            Arc::new(InMemory::new()),
            "root",
            "test",
            crate::state::StateCache::default(),
            &options,
            crate::state::transfer::Backend::Local,
        )
        .await
        .unwrap();
        let handles = state.shared_handles().unwrap();
        let (events, _announced) = mpsc::channel(64);
        let clock = Clock::system(Duration::from_secs(5), None);
        let mut writer = Some(Writer::new(&handles, Duration::from_millis(50), clock));
        let leader = writer.as_mut().unwrap();
        leader.sync(true, 2, &events);
        leader.sync(false, 2, &events);
        leader.sync(true, 3, &events);
        let failure = tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                if let WriterEvent::Failed(error) = next(&mut writer).await {
                    return error;
                }
            }
        })
        .await;
        assert!(failure.is_err(), "{failure:?}");
        let mut leader = writer.unwrap();
        leader.sync(false, 3, &events);
        leader.stop().await;
        state.apply(1, None).await.unwrap();
        state.close().await.unwrap();
    }
}
