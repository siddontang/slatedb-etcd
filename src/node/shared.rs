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

/// A member's report protects its checkpoints for this time after it arrives.
const REPORT_LIFETIME: Duration = Duration::from_secs(10 * 60);

/// The shared-mode tasks of one member. Only the leader runs the writer.
pub(super) struct Writer {
    admin: Arc<slatedb::admin::Admin>,
    queue: Arc<Queue>,
    pub(super) source: Arc<Source>,
    progress: Arc<Progress>,
    max_age: Duration,
    /// Set while this member leads. Sending `true` closes the writer.
    stop: Option<tokio::sync::watch::Sender<bool>>,
    tasks: tokio::task::JoinSet<Result<()>>,
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
}

impl Writer {
    pub fn new(handles: &Handles, max_age: Duration) -> Self {
        Self {
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

    /// Open the writer on the leader. Close it on any other member.
    ///
    /// The open fences an older writer. Phase 3 adds the term check after the open, the clock
    /// check, and the reopen after a fence. Phase 2 rejects leader transfer instead.
    pub fn sync(&mut self, leader: bool, events: &mpsc::Sender<Event>) {
        if !leader {
            if let Some(stop) = self.stop.take() {
                stop.send_replace(true);
            }
            return;
        }
        if self.running() {
            return;
        }
        let (stop, stopped) = tokio::sync::watch::channel(false);
        self.stop = Some(stop);
        self.tasks.spawn(lead(
            Publisher {
                db: None,
                admin: self.admin.clone(),
                source: self.source.clone(),
                progress: self.progress.clone(),
                max_age: self.max_age,
                reports: self.reports.clone(),
            },
            self.queue.clone(),
            events.clone(),
            stopped,
        ));
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
    let failed = async {
        loop {
            match writer.tasks.join_next().await {
                Some(Ok(Ok(()))) if !leading => continue,
                // A newer writer can fence the old one before it sees the stop.
                Some(Ok(Err(error))) if !leading => {
                    tracing::warn!(%error, "the stopped shared writer failed");
                    continue;
                }
                Some(Ok(Ok(()))) => return anyhow::anyhow!("a shared state writer task stopped"),
                Some(Ok(Err(error))) => return error.context("shared state writer failed"),
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

/// Open the writer, then materialize and publish until the member steps down.
async fn lead(
    mut publisher: Publisher,
    queue: Arc<Queue>,
    events: mpsc::Sender<Event>,
    mut stopped: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let db = tokio::select! {
        db = publisher.source.open_writer() => Arc::new(db?),
        _ = stopped.wait_for(|stop| *stop) => return Ok(()),
    };
    // The open starts at `last_l0_seq`. Unflushed writes of an older writer are lost, and the
    // overlay holds them.
    publisher.progress.reset(db.manifest().last_l0_seq());
    publisher.db = Some(db.clone());
    let result = tokio::select! {
        result = materialize(db.clone(), queue, publisher.progress.clone()) => result,
        result = publish(publisher, events) => result,
        _ = stopped.wait_for(|stop| *stop) => Ok(()),
    };
    let closed = db
        .close_with_options(slatedb::config::CloseOptions { flush_type: None })
        .await;
    result?;
    // A writer that a newer leader fenced cannot close cleanly. Phase 3 reopens it.
    if let Err(error) = closed {
        tracing::warn!(%error, "cannot close the shared writer");
    }
    Ok(())
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

struct Publisher {
    db: Option<Arc<slatedb::Db>>,
    admin: Arc<slatedb::admin::Admin>,
    source: Arc<Source>,
    progress: Arc<Progress>,
    max_age: Duration,
    reports: Reports,
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
    } = publisher;
    let db = db.context("the publisher needs the writer")?;
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
        if events
            .send(Event::ViewCheckpoint(checkpoint.id))
            .await
            .is_err()
        {
            return Ok(());
        }
        if !bootstrapped {
            source.mark_bootstrapped(checkpoint.id).await?;
            bootstrapped = true;
        }
        crate::state::shared::retain(&admin, &source, &reported(&reports), KEEP_CHECKPOINTS)
            .await?;
    }
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
        if let Some(writer) = &mut self.writer {
            writer.sync(leader, &self.events);
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

    /// Take a due switch job.
    pub(super) fn take_switch(&mut self) -> bool {
        self.writer
            .as_mut()
            .is_some_and(|writer| std::mem::take(&mut writer.switch_due))
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
        // These writes apply above twice the soft limit, and no checkpoint can appear.
        let puts = (0..16_u8).map(|n| {
            let mut kv = KvClient::new(channel.clone());
            async move {
                kv.put(PutRequest {
                    key: vec![b'k', n],
                    value: vec![n; 16 * 1024],
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
                key: vec![b'k', 3],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.kvs[0].value, vec![3; 16 * 1024]);
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
}
