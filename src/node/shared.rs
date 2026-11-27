//! Run the shared-mode leader tasks: the materializer and the checkpoint publisher.
//!
//! See `docs/shared-state.md`. The node starts both tasks when it becomes the leader and stops
//! them when it steps down. Phase 1 keeps the writer `Db` open across terms, because a one-voter
//! cluster has no other writer. Phase 3 opens the writer at each leader change and adds fencing.

use super::*;
use std::sync::{Arc, Mutex};

use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions};

use std::collections::HashSet;

use crate::mvcc::storage_fault::{StorageFault, CORRUPT_READS};
use crate::mvcc::{KvError, Outcome};
use crate::state::overlay::Queue;
use crate::state::shared::{newest_base_between, view_checkpoints, view_name, Handles, Source};

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
    /// Tests: the next `wrote` takes the pair. It sends on the first and waits on the second
    /// before it changes the progress.
    #[cfg(test)]
    hold: Mutex<Option<clock::ReadHold>>,
    /// Tests: the next writer task takes the pair before its reset. It notifies the first and
    /// waits for the second.
    #[cfg(test)]
    reset_hold: Hold,
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
    /// The writer opens so far. Only the materializer of the newest open moves `written`. A
    /// stopped materializer of an older open can end after the newer open.
    owner: u64,
}

impl Progress {
    pub(super) fn new(written: u64) -> Self {
        Self {
            inner: Mutex::default(),
            written: tokio::sync::watch::channel(written).0,
            wake: tokio::sync::Notify::new(),
            #[cfg(test)]
            hold: Mutex::default(),
            #[cfg(test)]
            reset_hold: Hold::default(),
        }
    }

    pub fn written(&self) -> u64 {
        *self.written.borrow()
    }

    /// The materializer of open `owner` wrote `seq`.
    fn wrote(&self, owner: u64, seq: u64) {
        #[cfg(test)]
        {
            let hold = self.hold.lock().unwrap().take();
            if let Some((reached, resume)) = hold {
                let _ = reached.send(());
                let _ = resume.recv();
            }
        }
        let mut inner = self.inner.lock().unwrap();
        if inner.owner != owner {
            return;
        }
        self.written.send_replace(seq);
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
    /// Return the owner of the new open. The writer task calls this only while it still leads
    /// its term. See `lead`.
    fn reset(&self, written: u64) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        inner.owner += 1;
        inner.dirty_since = None;
        self.written.send_replace(written);
        inner.owner
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
        let admin = slatedb::admin::Admin::builder(
            publisher.source.state(),
            publisher.source.store.clone(),
        )
        .with_system_clock(gate.clone())
        .build();
        #[cfg(test)]
        {
            let hold = publisher.progress.reset_hold.lock().unwrap().take();
            if let Some((reached, resume)) = hold {
                reached.notify_one();
                resume.notified().await;
            }
        }
        // The open starts at `last_l0_seq`. Unflushed writes of an older writer are lost, and
        // the overlay holds them.
        //
        // The task of a newer term can open while this task is here. `Writer::sync` changes
        // the leadership before it starts that task. The read guard holds the leadership from
        // the check through the reset and the slot, so a newer term comes before the check or
        // after the slot. An older task then cannot move the progress back or replace the
        // newer writer.
        let owner = {
            let leading = leadership.borrow();
            (*leading == (true, term)).then(|| {
                let owner = publisher.progress.reset(db.manifest().last_l0_seq());
                publisher.progress.rearm();
                *open.lock().unwrap() = Some(Open {
                    db: db.clone(),
                    admin: Arc::new(admin),
                    gate: gate.clone(),
                });
                owner
            })
        };
        let Some(owner) = owner else {
            close(db).await;
            return Ok(());
        };
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
                result = materialize(db.clone(), queue.clone(), progress, gate, owner) => result,
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

/// Copy overlay batches into the shared `Db` in order, each with its own sequence. `owner` is
/// the open of `db`. See `Progress::reset`.
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
    owner: u64,
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
            progress.wrote(owner, seq);
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
        let checkpoint = state
            .shared
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
        self.writer.lead_proofs(leader.then_some(term));
        self.writer.sync(leader, term, &self.events);
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
        let Some(defragment) = self.writer.defragment(self.raw.raft.term) else {
            let _ = response.send(Err(self.error("the shared writer is not open yet")));
            return Ok(());
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
        let covers = self.writer.coverage(row);
        self.propose_view(checkpoint, covers)
    }

    /// Propose a `ViewCheckpoint` entry that certifies `checkpoint` through `covers`.
    fn propose_view(&mut self, checkpoint: uuid::Uuid, covers: u64) -> Result<bool> {
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
                self.writer.note_announced();
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
        let mut pinned = self.writer.source().pinned();
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
        self.writer
            .note_view_ack(member, checkpoints, publish, coverage, health);
    }

    /// Run the periodic shared-mode work: `ViewAck` reports, the paused checkpoint poll, and the
    /// leader's demand for a base.
    pub(super) fn tick_shared(&mut self) -> Result<()> {
        self.certify_base()?;
        let probe = self.writer.probe_due();
        self.tick_clock(probe);
        self.tick_writer_health();
        let (base, _, bytes) = self.state.view;
        let leader = self.raw.raft.state == StateRole::Leader;
        let ticks = self.writer.tick(
            leader,
            self.state.applied,
            self.committed_ready,
            base,
            self.state.base_checkpoint,
            bytes,
        );
        // A follower repeats the request of a waiting manual checkpoint.
        if !leader && ticks.is_multiple_of(RETRY_TICKS) {
            if let Some(index) = self
                .checkpoint_waiters
                .iter()
                .map(|(index, _)| *index)
                .max()
            {
                self.send_view_ack(Some(index));
            }
        }
        if !ticks.is_multiple_of(REPORT_TICKS) {
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
            self.writer.report(leader, pinned);
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
        let health = self.writer.health_report(term, self.apply_held_at);
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

    /// Return true when the overlay is over the soft limit. The leader then rejects client
    /// proposals. `ViewCheckpoint` entries are exempt.
    pub(super) fn overlay_full(&self) -> bool {
        let (_, _, bytes) = self.state.view;
        bytes > self.options.overlay_max_bytes
    }

    /// Return true while apply waits for a base switch. Apply waits above twice the soft limit
    /// and resumes below the soft limit. Raft still persists and acknowledges entries.
    pub(super) fn apply_paused(&mut self) -> bool {
        let (_, _, bytes) = self.state.view;
        let limit = self.options.overlay_max_bytes;
        self.writer.pause_apply(bytes, limit)
    }

    /// Return the index and log term of an opened base that a paused member can adopt: the
    /// base is above the applied index, and the delivered commit covers it.
    ///
    /// The node takes the base from the slot and verifies that one. A newer open cannot replace
    /// it before the job runs. A base that fails the check goes back to the slot.
    pub(super) fn adoptable(&self) -> Option<(Arc<crate::state::view::Base>, u64)> {
        if self.prepared_puts.is_some() || self.held_puts.is_some() {
            return None;
        }
        let (base, covers) = self.writer.take_opened()?;
        let index = base.applied;
        let term = (index > self.state.applied && index <= self.committed_ready)
            .then(|| self.raw.raft.raft_log.term(index).ok())
            .flatten();
        match term {
            Some(term) => Some((base, term)),
            None => {
                self.writer.offer_opened(base, covers);
                None
            }
        }
    }
}

impl Node {
    /// Serve a demand for a base above the base index of this leader: a follower's snapshot
    /// request at its last index, a follower's manual checkpoint, or the leader's own Raft
    /// snapshot record above its base. See `base_below_record`.
    ///
    /// The base holds every batch through its row. If no unreleased batch of this leader is
    /// above the row, the leader announces the same checkpoint again with its covered index.
    /// That costs one small entry and no checkpoint. The members then raise their base index,
    /// and the leader records the base at the new index for the snapshot. If a batch is above
    /// the row, the leader asks for a publication that holds it, and serves the demand with
    /// that checkpoint.
    ///
    /// The base comes first, then one position read gives the last batch and the covered
    /// index. A switch between the reads makes the check stricter, not weaker. One announcement
    /// serves the demand at a time. A lost proposal is sent again after `CERTIFY_TICKS`. The
    /// demand ends when the base index reaches it, so an idle cluster stops announcing. The log
    /// cache sends a record above its row only to a peer whose current process advertised that
    /// it installs such a base.
    pub(in crate::node) fn certify_base(&mut self) -> Result<()> {
        self.note_base();
        if self.raw.raft.state != StateRole::Leader {
            return Ok(());
        }
        // A request of an earlier term can name a suffix that this leader replaced.
        let term = self.raw.raft.term;
        self.raw.store().lead_term(term);
        self.writer.lead_demand(term);
        // A member does not lead before its install. See `tick_raft`. If it did, the install
        // would raise the base, and the state below the record could serve nothing.
        if self.installing() {
            return Ok(());
        }
        let record = self.raw.store().snapshot_index();
        let wanted = self.raw.store().wanted_index();
        // A base below the record cannot replace the record, so the record index is a demand.
        let record = match self.base_below_record() {
            true => record,
            false => 0,
        };
        let Some((checkpoint, covers, ticks)) = self.writer.certify(wanted, record) else {
            return Ok(());
        };
        if self.propose_view(checkpoint, covers)? {
            self.writer.certified(covers, ticks);
        }
        Ok(())
    }

    /// Tell the log cache which checkpoint and index the base has. See `Cache::has_image`.
    pub(in crate::node) fn note_base(&self) {
        self.raw.store().note_base(self.writer.base());
    }

    /// Return true while the base index of this shared member is below its Raft snapshot record.
    /// That happens after Raft persists a received snapshot that apply had already passed. No
    /// checkpoint at the base can replace the record then, so the checkpoint jobs wait, and
    /// `certify_base` raises the base through the record index.
    pub(in crate::node) fn base_below_record(&self) -> bool {
        let (base, _, _) = self.state.view;
        base < self.raw.store().snapshot_index()
    }

    /// Take a due job that offers the checkpoint of a record that apply passed as the base, once
    /// for each record. This is the fast path. If the offer fails, `certify_base` still raises
    /// the base.
    pub(in crate::node) fn take_raise(&mut self) -> bool {
        let record = self.raw.store().snapshot_index();
        if !self.base_below_record() || record > self.state.applied {
            return false;
        }
        self.writer.take_raise(record)
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
        #[cfg(test)]
        let manual = self.manual_transport && !self.clock_in_manual;
        #[cfg(not(test))]
        let manual = self.manual_transport;
        if manual {
            return;
        }
        let leader = self.raw.raft.state == StateRole::Leader;
        if !leader {
            self.writer.clock().stand_down();
            return;
        }
        let voters = self.clock_voters();
        self.writer
            .clock()
            .configure(self.raw.raft.term, self.options.id, voters.clone());
        self.writer.clock().observe();
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
            let clock = self.writer.clock().clone();
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

    /// Check the writer health on each tick. A leader whose writer is down hands over its
    /// leadership to a healthy voter, once per voter and incident. With no target left, it stays
    /// leader and its writer keeps retrying. See `Handoff::step`.
    pub(super) fn tick_writer_health(&mut self) {
        let leader = self.raw.raft.state == StateRole::Leader;
        let wait = TICK * HANDOFF_TIMEOUTS * self.raw.raft.election_timeout() as u32;
        let paused = self.worker.control.paused();
        let held = self.apply_held_at.is_some() || paused.is_some();
        let (term, transferee) = (self.raw.raft.term, self.raw.raft.lead_transferee);
        let moves: Vec<u64> = self.transfers.iter().map(|(target, ..)| *target).collect();
        self.writer.tick_health(leader);
        if !leader {
            return;
        }
        // `Handoff::step` reads the candidates only when a handoff is due.
        let due = self.writer.handoff_due(paused.as_ref());
        let candidates = match due {
            true => self.handoff_candidates(),
            false => vec![],
        };
        let decision =
            self.writer
                .step_handoff(due, transferee, wait, &candidates, term, moves, held);
        if decision.abort {
            self.raw.raft.abort_leader_transfer();
        }
        if let Some(target) = decision.transfer {
            self.raw.transfer_leader(target);
        }
    }
}

impl Node {
    /// Report a base that an adoption dropped.
    pub(in crate::node) fn reject_base(&mut self, rejected: apply::RejectedBase) {
        if rejected.fault == StorageFault::Corrupt {
            CORRUPT_READS.inc();
        }
        if self.writer.note_rejected(&rejected) {
            tracing::error!(
                checkpoint = ?rejected.checkpoint,
                index = rejected.index,
                fault = ?rejected.fault,
                reason = rejected.reason,
                "drop a base that does not load; keep the current base"
            );
        }
    }
}

impl Node {
    /// Shared mode: refuse the promotion of a learner without a valid write proof. The refusal
    /// is retryable. It names a missing, failed, or expired proof, and it can start a probe.
    pub(in crate::node) fn check_write_proof(
        &mut self,
        request: &crate::cluster::Command,
    ) -> std::result::Result<(), KvError> {
        let crate::cluster::Command::Promote(member) = *request else {
            return Ok(());
        };
        #[cfg(test)]
        if self.skip_write_proof {
            return Ok(());
        }
        let process = self.raw.store().process(member);
        let term = self.raw.raft.term;
        let refusal = match self
            .writer
            .check_proof(Instant::now(), term, member, process)
        {
            Ok(()) => return Ok(()),
            Err(refusal) => refusal,
        };
        if let Some(nonce) = refusal.probe {
            self.send_write_probe(member, nonce);
        }
        Err(KvError::new(
            tonic::Code::FailedPrecondition,
            &refusal.message,
        ))
    }

    /// Drop the write proofs of members that left the Raft configuration.
    pub(in crate::node) fn prune_write_proofs(&mut self) {
        let conf = self.raw.raft.prs().conf().to_conf_state();
        let members: std::collections::HashSet<u64> = [
            &conf.voters,
            &conf.learners,
            &conf.voters_outgoing,
            &conf.learners_next,
        ]
        .into_iter()
        .flatten()
        .copied()
        .collect();
        self.writer.retain_proofs(&members);
    }

    /// Send the write probe `nonce` to `member`. Its reply comes back as `Event::WriteProbed`.
    /// A manual transport has no peer routes, so the test gives the reply itself.
    fn send_write_probe(&self, member: u64, nonce: u64) {
        if self.manual_transport {
            return;
        }
        let Some(address) = self
            .state
            .members
            .members
            .get(&member)
            .and_then(|member| member.peer_ur_ls.first())
        else {
            return;
        };
        let url = format!("{}{WRITE_PROBE_ROUTE}", crate::cluster::url(address));
        let request = self.peer.headers(self.options.transport.http.post(url));
        let events = self.events.clone();
        tokio::spawn(async move {
            let replied = async {
                let body = WriteProbeRequest { member, nonce };
                let response = request.timeout(REPLY_WAIT).json(&body).send().await?;
                response.error_for_status()?.json::<WriteProbeReply>().await
            };
            let reply = replied.await.map_err(|error| error.to_string());
            let _ = events
                .send(Event::WriteProbed {
                    member,
                    nonce,
                    reply,
                })
                .await;
        });
    }

    /// Record the reply of `member` to the write probe `nonce`.
    pub(in crate::node) fn write_probed(
        &mut self,
        member: u64,
        nonce: u64,
        reply: std::result::Result<WriteProbeReply, String>,
    ) {
        let replied = match reply {
            Ok(reply) if reply.nonce != nonce => return,
            Ok(reply) if reply.store != self.peer.store => Replied {
                nonce,
                process: Some(reply.process),
                outcome: Err(format!(
                    "the member uses the shared store {:?}",
                    reply.store
                )),
            },
            Ok(reply) => Replied {
                nonce,
                process: Some(reply.process),
                outcome: reply.error.map_or(Ok(()), Err),
            },
            Err(error) => Replied {
                nonce,
                process: None,
                outcome: Err(error),
            },
        };
        let process = self.raw.store().process(member);
        let term = self.raw.raft.term;
        let failure = replied.outcome.clone().err();
        if !self
            .writer
            .complete_proof(Instant::now(), term, member, process, replied)
        {
            return;
        }
        match failure {
            None => tracing::info!(
                member,
                "the learner proved write access to the shared state"
            ),
            Some(error) => tracing::warn!(
                member,
                %error,
                "the learner has no write access to the shared state"
            ),
        }
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
        let writer = &self.writer;
        dst.close().await;
        // The compactor runs only on request, so GC removes the same SSTs on every replay.
        let mut settings = crate::state::slatedb_settings();
        if let Some(compactor) = settings.compactor_options.as_mut() {
            compactor
                .scheduler_options
                .insert("min_compaction_sources".into(), "1000000".into());
        }
        let db = Arc::new(writer.source().open_writer_with(settings).await?);
        // The lead must be unchanged after the open, and the clock must pass the manifest.
        if (!deposed && self.raw.raft.term != term)
            || !writer
                .clock()
                .pass(db.manifest().last_l0_clock_tick())
                .await
        {
            let _ = db
                .close_with_options(slatedb::config::CloseOptions { flush_type: None })
                .await;
            return Ok(false);
        }
        dst.written = db.manifest().last_l0_seq();
        dst.epoch = db.manifest().writer_epoch();
        dst.term = term;
        dst.published = view_checkpoints(writer.admin())
            .await?
            .last()
            .map_or(0, |(seq, _)| *seq);
        dst.bootstrapped = writer.source().bootstrapped().await?;
        // A new writer announces a stranded checkpoint, as `publish` does.
        dst.pending = writer.stranded().await?;
        if let (Some((id, _)), false) = (dst.pending, dst.bootstrapped) {
            writer.source().mark_bootstrapped(id).await?;
            dst.bootstrapped = true;
        }
        dst.db = Some(db);
        Ok(true)
    }

    /// Write the overlay batches above the written sequence, each with its own sequence.
    pub(super) async fn dst_materialize(&mut self, dst: &mut DstWriter) -> Result<usize> {
        let Some(db) = &dst.db else {
            return Ok(0);
        };
        let writer = &self.writer;
        let mut count = 0;
        for (seq, batch) in writer.queue().after(dst.written, usize::MAX) {
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
        let Some(db) = &dst.db else {
            return Ok(None);
        };
        let writer = &self.writer;
        db.flush().await?;
        // Compact the L0 SSTs on request before SlateDB would stop writes for them.
        dst_compact(writer.admin(), 4).await?;
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
        let base = writer.source().open_base(id).await?;
        if base.get(crate::cluster::KEY).await?.is_none() {
            writer.admin().delete_checkpoint(id).await?;
            return Ok(None);
        }
        if !dst.bootstrapped {
            writer.source().mark_bootstrapped(id).await?;
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
        if dst.db.is_none() {
            return Ok(());
        }
        let writer = &self.writer;
        crate::state::shared::retain(
            writer.admin(),
            writer.source(),
            reported,
            KEEP_CHECKPOINTS,
            Some(dst.epoch),
        )
        .await
    }

    /// Return true when a newer writer holds the shared database.
    pub(super) async fn dst_fenced(&self, dst: &DstWriter) -> bool {
        match self.writer.admin().read_manifest(None).await {
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
mod writer;
pub(super) use clock::Clock;
use clock::PROBE_TIMEOUT;
#[cfg(test)]
pub(super) use clock::{probe_nonce, Gate};
pub(crate) use clock::{Millis, ProbeReply, ProbeRequest, PROBE_ROUTE};
use defragment::Defragment;
#[cfg(test)]
use defragment::DefragmentStep;
#[cfg(test)]
pub(super) use fault::fenced;
pub(crate) use fault::WriterReport;
use fault::{Candidate, HANDOFF_TIMEOUTS};
pub(crate) use write_proof::{Prober, WriteProbeReply, WriteProbeRequest, WRITE_PROBE_ROUTE};
use write_proof::{Replied, REPLY_WAIT};
pub(super) use writer::{next, Gauges, Writer};
#[cfg(test)]
mod materialize_tests;
#[cfg(test)]
mod record_tests;

#[cfg(test)]
mod tests;
