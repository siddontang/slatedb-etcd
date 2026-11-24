//! Run the shared-mode leader tasks: the materializer and the checkpoint publisher.
//!
//! See `docs/shared-state.md`. The node starts both tasks when it becomes the leader and stops
//! them when it steps down. Phase 1 keeps the writer `Db` open across terms, because a one-voter
//! cluster has no other writer. Phase 3 opens the writer at each leader change and adds fencing.

use super::*;
use std::sync::{Arc, Mutex};

use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions};

use std::collections::HashSet;

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
}

impl Progress {
    fn new(written: u64) -> Self {
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

    /// Start again after a writer open at `written`.
    fn reset(&self, written: u64) {
        self.written.send_replace(written);
        *self.inner.lock().unwrap() = Inner::default();
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

    /// Return true when a flush is due, and clear the causes that it covers.
    fn take_flush(&self, max_age: Duration) -> bool {
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
        requested || aged
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
            progress: Arc::new(Progress::new(0)),
            admin: handles.admin.clone(),
            queue: handles.queue.clone(),
            source: handles.source.clone(),
            max_age,
            stop: None,
            tasks: tokio::task::JoinSet::new(),
            reports: Reports::default(),
            announced: 0,
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
            },
            Leading {
                queue: self.queue.clone(),
                events: events.clone(),
                stopped,
                term,
                leadership: self.leadership.subscribe(),
                clock: self.clock.clone(),
                reopens: self.reopens.clone(),
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

    /// Publish a checkpoint that holds the applied state through `index`.
    pub fn request_publication(&self, index: u64) {
        self.progress.request(self.covering(index));
    }

    /// The batch sequence that holds the state through `index`. A later no-op entry has no batch.
    pub fn covering(&self, index: u64) -> u64 {
        index.min(self.queue.pushed())
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
}

/// Wait until the member asks the writer task to stop. The result holds no borrow, so the task
/// stays `Send` across later waits.
async fn stop_requested(stopped: &mut tokio::sync::watch::Receiver<bool>) {
    let _ = stopped.wait_for(|stop| *stop).await;
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
    } = leading;
    let current = || *leadership.borrow() == (true, term);
    loop {
        let opened = tokio::select! {
            opened = publisher.source.open_writer() => opened,
            _ = stop_requested(&mut stopped) => return Ok(()),
        };
        let db = match opened {
            Ok(db) => Arc::new(db),
            // A newer open fenced this one while the member still leads the same term.
            Err(error) if fenced(&error) && current() && !*stopped.borrow() => {
                tracing::warn!(term, "the shared writer open was fenced; open it again");
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
        let result = tokio::select! {
            result = materialize(db.clone(), queue.clone(), publisher.progress.clone()) => result,
            result = publish(publisher, events.clone()) => result,
            _ = stop_requested(&mut stopped) => Ok(()),
        };
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

/// Copy overlay batches into the shared `Db` in order, each with its own sequence.
///
/// The writer opened at `last_l0_seq`. The batches at or below the written sequence are already
/// in the `Db`. A batch that spans it is written whole: its rows are final values, so the result
/// is the state at its sequence.
pub(super) async fn materialize(
    db: Arc<slatedb::Db>,
    queue: Arc<Queue>,
    progress: Arc<Progress>,
) -> Result<()> {
    let mut written = progress.written();
    let mut pushed = queue.subscribe();
    loop {
        pushed.wait_for(|pushed| *pushed > written).await?;
        for (seq, batch) in queue.after(written, MATERIALIZE_BATCHES) {
            db.write_with_options(batch.to_storage(), &WriteOptions { seqnum: seq })
                .await?;
            written = seq;
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
    let mut status = db.subscribe();
    loop {
        match progress.deadline(max_age) {
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
        if progress.take_flush(max_age) {
            db.flush().await?;
        }
        // SlateDB also flushes a full memtable by itself. Each advance gets one checkpoint.
        let last = db.manifest().last_l0_seq();
        if last <= published {
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
        drop(base);
        // The marker comes before the first announcement. The checkpoint already exists, so a
        // restart after the marker still finds a base.
        if !bootstrapped {
            source.mark_bootstrapped(checkpoint.id).await?;
            bootstrapped = true;
        }
        if events
            .send(Event::ViewCheckpoint(checkpoint.id))
            .await
            .is_err()
        {
            return Ok(());
        }
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

    /// Propose a checkpoint that the publisher created. Only the leader announces.
    pub(super) fn announce_view(&mut self, checkpoint: uuid::Uuid) -> Result<()> {
        let Some(writer) = &mut self.writer else {
            return Ok(());
        };
        if self.raw.raft.state != StateRole::Leader {
            return Ok(());
        }
        let data = RaftCommand::ViewCheckpoint {
            id: uuid::Uuid::new_v4().to_string(),
            checkpoint: checkpoint.to_string(),
        }
        .encode()?;
        // The entry frees memory, so it bypasses the client admission limits.
        if self.raw.propose(vec![], data).is_ok() {
            writer.announced += 1;
        }
        Ok(())
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
    pub(super) fn view_ack(&mut self, member: u64, checkpoints: HashSet<uuid::Uuid>) {
        if let Some(writer) = &self.writer {
            writer.report(member, checkpoints);
        }
    }

    /// Run the periodic shared-mode work: `ViewAck` reports and the paused checkpoint poll.
    pub(super) fn tick_shared(&mut self) {
        let Some(writer) = &mut self.writer else {
            return;
        };
        writer.ticks += 1;
        let ticks = writer.ticks;
        if ticks % POLL_TICKS == 0 && writer.paused {
            self.poll_checkpoints();
        }
        if ticks % REPORT_TICKS != 0 {
            return;
        }
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
        let body = json!({"member": self.options.id, "checkpoints": pinned});
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
                    opened.offer(source.open_base(checkpoint.id).await?);
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
        let base = writer.opened_slot.take()?;
        let index = base.applied;
        let term = (index > self.state.applied && index <= self.committed_ready)
            .then(|| self.raw.raft.raft_log.term(index).ok())
            .flatten();
        match term {
            Some(term) => Some((base, term)),
            None => {
                writer.opened_slot.offer(base);
                None
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
    /// A checkpoint that the next announce step proposes.
    pub pending: Option<uuid::Uuid>,
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
        dst.pending = None;
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
        dst.pending = Some(id);
        Ok(Some(id))
    }

    /// Propose the pending checkpoint. Only the leader proposes.
    pub(super) fn dst_announce(&mut self, dst: &mut DstWriter) -> Result<()> {
        match dst.pending.take() {
            Some(id) => self.announce_view(id),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::overlay::Overlay;
    use slatedb::object_store::memory::InMemory;

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
        assert!(!progress.take_flush(Duration::ZERO));
        progress.wrote(1);
        let deadline = progress.deadline(long).unwrap();
        progress.wrote(2);
        // A later write keeps the deadline of the oldest unflushed write.
        assert_eq!(progress.deadline(long), Some(deadline));
        assert!(!progress.take_flush(long));
        assert!(progress.take_flush(Duration::ZERO));
        assert!(!progress.take_flush(Duration::ZERO));
        assert_eq!(progress.deadline(long), None);
        // A request waits until the materializer writes its sequence.
        progress.request(3);
        assert!(!progress.take_flush(long));
        progress.wrote(3);
        assert!(progress.take_flush(long));
        assert!(!progress.take_flush(long));
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
        let applied = status(&server).await["applied_index"].as_u64().unwrap();
        eventually("a base that covers the writes", async || {
            status(&server).await["shared_state"]["base_applied_index"]
                .as_u64()
                .unwrap()
                >= applied
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
        state.apply(1, None).await.unwrap();
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
                if let Event::ViewCheckpoint(id) = event {
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
