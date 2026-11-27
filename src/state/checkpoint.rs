//! Make an applied prefix durable for local log truncation.

use super::*;

type CheckpointJob =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Vec<u8>>>> + Send>>;

/// The index, the ConfState, and the job of one checkpoint.
pub(crate) type CheckpointParts = (u64, raft::eraftpb::ConfState, CheckpointJob);

impl StateMachine {
    /// Start a checkpoint of the applied prefix. The state takes its base: the record is a
    /// format 6 descriptor at `F`, with the ConfState of the checkpoint. It needs no flush.
    pub(crate) async fn checkpoint_parts(&self) -> Result<CheckpointParts> {
        let (index, data, conf) = self
            .base_snapshot()
            .await?
            .context("shared state has no checkpoint yet")?;
        Ok((index, conf, Box::pin(async move { Ok(Some(data)) })))
    }
}
