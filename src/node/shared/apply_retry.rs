//! Apply a job again after a storage error of a read. See "Read faults" in
//! `docs/shared-state.md`.
//!
//! Each member must reach the same result for each entry. A read that failed on this member is
//! no result, so the entry stays unapplied. Apply keeps the entries that it applied before the
//! error, with their replies, and runs the rest again after a backoff.

use super::*;
use crate::mvcc::storage_fault::{Classified, StorageFault};

/// The first wait after a storage error. It doubles up to `RETRY_CAP`.
const RETRY_FIRST: Duration = Duration::from_millis(100);
const RETRY_CAP: Duration = Duration::from_secs(5);
/// Tries between two warnings while a read keeps failing.
const WARN_EVERY: u32 = 20;

/// The tries of one job after storage errors.
#[derive(Default)]
pub(super) struct Retry {
    tries: u32,
}

impl Retry {
    /// Wait before the next try after `error`. Fail at once when the worker stops.
    pub(super) async fn wait(
        &mut self,
        stop: &mut tokio::sync::watch::Receiver<bool>,
        error: &anyhow::Error,
    ) -> Result<()> {
        self.tries += 1;
        let delay = RETRY_FIRST
            .saturating_mul(1 << (self.tries - 1).min(16))
            .min(RETRY_CAP);
        if self.tries == 1 {
            tracing::warn!(
                error = format!("{error:#}"),
                ?delay,
                "an apply read failed; retry"
            );
        } else if self.tries.is_multiple_of(WARN_EVERY) {
            let tries = self.tries;
            tracing::warn!(
                tries,
                error = format!("{error:#}"),
                "an apply read still fails"
            );
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => Ok(()),
            _ = stopped(stop) => {
                anyhow::bail!("apply stopped while it retried a storage read: {error:#}")
            }
        }
    }
}

/// Wait until the worker stops. A worker without a stop sender runs until the task ends.
async fn stopped(stop: &mut tokio::sync::watch::Receiver<bool>) {
    if stop.wait_for(|stop| *stop).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// The class of a storage error that apply can run again, or `None` when apply must stop.
///
/// A transient error must come from a read: the error crossed a `KvError` or a read helper, so
/// no state changed. A missing object can come from any read. Apply rebases on it first.
pub(super) fn retryable(error: &anyhow::Error) -> Option<StorageFault> {
    let read = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<Classified>())
        .map(|classified| classified.fault);
    match (read, crate::mvcc::storage_fault::classify(error)) {
        (_, Some(StorageFault::Missing)) => Some(StorageFault::Missing),
        (Some(StorageFault::Transient), _) => Some(StorageFault::Transient),
        _ => None,
    }
}

impl Executor {
    /// Apply committed entries. After a storage error of a read, the applied prefix keeps its
    /// replies, its membership changes, and its lease commands. The rest runs again from
    /// `applied + 1`, so each entry completes exactly once. In shared mode a missing object
    /// rebases the state at the applied index first. Other errors stop apply.
    pub(super) async fn apply_retrying(
        &mut self,
        entries: Vec<Entry>,
        header: Header,
    ) -> Result<Vec<AppliedCommand>> {
        let mut replies = vec![];
        let mut retry = Retry::default();
        // Each try starts at the first entry of the job that did not apply. Every applied entry
        // raises the applied index by one, so `apply_into` still checks each index of the job
        // and rejects an overlap.
        let mut next = 0;
        loop {
            let before = self.state.applied;
            let rest = entries[next..].to_vec();
            let tried = Box::pin(apply_into(&mut self.state, rest, header, &mut replies)).await;
            let Err(error) = tried else {
                return Ok(replies);
            };
            next += (self.state.applied - before) as usize;
            match retryable(&error) {
                Some(StorageFault::Missing) if self.log.is_some() => {
                    tracing::warn!(%error, "an apply read lost its checkpoint object");
                    Box::pin(self.rebase(header, self.state.applied)).await?;
                }
                Some(StorageFault::Transient) => {}
                _ => return Err(error),
            }
            retry.wait(&mut self.stop, &error).await?;
        }
    }

    /// Evaluate a group of Puts before their state write. The entries can still be uncommitted,
    /// and a new leader can replace them. So a storage error of a read does not retry here,
    /// where it would hold the worker. The group returns `None`, and its entries apply on the
    /// committed path, which retries and rebases.
    pub(super) async fn prepare_or_skip(
        &mut self,
        commands: &[(u64, RaftCommand)],
        term: u64,
        header: Header,
    ) -> Result<Option<(crate::state::put_batch::PreparedPuts, Vec<AppliedCommand>)>> {
        match super::quorum_put::prepare(&self.state, commands, term, header).await {
            Err(error) if retryable(&error).is_some() => {
                tracing::warn!(
                    error = format!("{error:#}"),
                    "an early Put evaluation failed; apply the entries after commit"
                );
                Ok(None)
            }
            prepared => prepared.map(Some),
        }
    }
}

#[cfg(test)]
#[path = "apply_retry_tests.rs"]
mod tests;
