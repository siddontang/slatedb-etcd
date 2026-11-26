//! Defragment the shared database on the leader writer.

use super::*;

/// One Defragment on the leader writer.
pub(in crate::node) struct Defragment {
    pub(super) db: Arc<slatedb::Db>,
    pub(super) admin: Arc<slatedb::admin::Admin>,
    pub(super) progress: Arc<Progress>,
    /// The newest applied announcement on this member.
    pub(super) announced: tokio::sync::watch::Receiver<Option<uuid::Uuid>>,
    pub(super) leadership: tokio::sync::watch::Receiver<(bool, u64)>,
    pub(super) term: u64,
    /// A test pause before a step: (step, reached, resume).
    pub(super) pause: Option<(
        DefragmentStep,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    )>,
}

/// The steps of a Defragment where a test can pause it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::node) enum DefragmentStep {
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
    pub(in crate::node) async fn run(mut self) -> std::result::Result<(), KvError> {
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
