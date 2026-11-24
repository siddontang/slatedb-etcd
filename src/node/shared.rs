//! Run the shared-mode leader tasks: the materializer and the checkpoint publisher.
//!
//! See `docs/shared-state.md`. The node starts both tasks when it becomes the leader and stops
//! them when it steps down. Phase 1 keeps the writer `Db` open across terms, because a one-voter
//! cluster has no other writer. Phase 3 opens the writer at each leader change and adds fencing.

use super::*;
use std::sync::{Arc, Mutex};

use slatedb::config::{CheckpointOptions, CheckpointScope, WriteOptions};

use crate::state::overlay::Queue;
use crate::state::shared::{view_checkpoints, view_name, Source, WriterParts};

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

/// The leader writer of shared state mode.
pub(super) struct Writer {
    db: Arc<slatedb::Db>,
    admin: Arc<slatedb::admin::Admin>,
    queue: Arc<Queue>,
    source: Arc<Source>,
    progress: Arc<Progress>,
    max_age: Duration,
    tasks: tokio::task::JoinSet<Result<()>>,
    /// `ViewCheckpoint` entries that this node proposed.
    pub announced: u64,
}

impl Writer {
    pub fn new(parts: WriterParts, max_age: Duration) -> Self {
        Self {
            progress: Arc::new(Progress::new(parts.written)),
            db: parts.db,
            admin: parts.admin,
            queue: parts.queue,
            source: parts.source,
            max_age,
            tasks: tokio::task::JoinSet::new(),
            announced: 0,
        }
    }

    pub fn running(&self) -> bool {
        !self.tasks.is_empty()
    }

    /// Start the tasks on the leader. Stop them on any other member.
    pub fn sync(&mut self, leader: bool, events: &mpsc::Sender<Event>) {
        if !leader {
            // Dropping the set aborts the tasks. The `Db` stays open for the next term.
            self.tasks = tokio::task::JoinSet::new();
            return;
        }
        if self.running() {
            return;
        }
        self.tasks.spawn(materialize(
            self.db.clone(),
            self.queue.clone(),
            self.progress.clone(),
        ));
        self.tasks.spawn(publish(
            Publisher {
                db: self.db.clone(),
                admin: self.admin.clone(),
                source: self.source.clone(),
                progress: self.progress.clone(),
                max_age: self.max_age,
            },
            events.clone(),
        ));
    }

    pub async fn stop(&mut self) {
        self.tasks.shutdown().await;
    }

    /// Publish a checkpoint that holds the applied state through `index`.
    pub fn request_publication(&self, index: u64) {
        self.progress.request(self.covering(index));
    }

    /// The batch sequence that holds the state through `index`. A later no-op entry has no batch.
    pub fn covering(&self, index: u64) -> u64 {
        index.min(self.queue.pushed())
    }

    pub fn progress(&self) -> Arc<Progress> {
        self.progress.clone()
    }

    pub fn status(&self) -> Value {
        json!({
            "written": self.progress.written(),
            "queued_batches": self.queue.len(),
            "announced": self.announced,
            "running": self.running(),
        })
    }
}

/// Wait until a writer task fails. A member without running tasks waits forever.
pub(super) async fn failed(writer: &mut Option<Writer>) -> anyhow::Error {
    let Some(writer) = writer else {
        return std::future::pending().await;
    };
    match writer.tasks.join_next().await {
        Some(Ok(Ok(()))) => anyhow::anyhow!("a shared state writer task stopped"),
        Some(Ok(Err(error))) => error.context("shared state writer failed"),
        Some(Err(error)) => anyhow::Error::from(error).context("shared state writer stopped"),
        None => std::future::pending().await,
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

struct Publisher {
    db: Arc<slatedb::Db>,
    admin: Arc<slatedb::admin::Admin>,
    source: Arc<Source>,
    progress: Arc<Progress>,
    max_age: Duration,
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
    } = publisher;
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
        crate::state::shared::retain(&admin, &source, KEEP_CHECKPOINTS).await?;
    }
}

impl Node {
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
}
