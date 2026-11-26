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
        loop {
            let applied = self.state.applied;
            let rest: Vec<_> = entries
                .iter()
                .filter(|entry| entry.index > applied)
                .cloned()
                .collect();
            let tried = Box::pin(apply_into(&mut self.state, rest, header, &mut replies)).await;
            let Err(error) = tried else {
                return Ok(replies);
            };
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

    /// Evaluate a group of committed Puts. The evaluation changes no state, so a transient
    /// storage error of a read evaluates the whole group again.
    pub(super) async fn prepare_retrying(
        &mut self,
        commands: &[(u64, RaftCommand)],
        term: u64,
        header: Header,
    ) -> Result<(crate::state::put_batch::PreparedPuts, Vec<AppliedCommand>)> {
        let mut retry = Retry::default();
        loop {
            match super::quorum_put::prepare(&self.state, commands, term, header).await {
                Err(error) if retryable(&error) == Some(StorageFault::Transient) => {
                    retry.wait(&mut self.stop, &error).await?;
                }
                prepared => return prepared,
            }
        }
    }
}

#[cfg(test)]
#[path = "apply_retry_tests.rs"]
mod tests;
