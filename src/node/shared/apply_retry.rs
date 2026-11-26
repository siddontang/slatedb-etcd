//! Apply a job again after a storage error of a read. See "Read faults" in
//! `docs/shared-state.md`.
//!
//! Each member must reach the same result for each entry. A read that failed on this member is
//! no result, so the entry stays unapplied. Apply keeps the entries that it applied before the
//! error, with their replies, and runs the rest again after a backoff. Corrupt stored data
//! holds apply on its entry, and apply checks it again every `HOLD_RECHECK`. A snapshot that
//! covers the entry ends the wait early.

use super::*;
use crate::mvcc::storage_fault::{Classified, StorageFault, StoredData};
use std::sync::Arc;
use tokio::sync::watch;

/// The first wait after a storage error. It doubles up to `RETRY_CAP`.
const RETRY_FIRST: Duration = Duration::from_millis(100);
const RETRY_CAP: Duration = Duration::from_secs(5);
/// The wait between two tries of an entry that met corrupt stored data.
#[cfg(not(test))]
const HOLD_RECHECK: Duration = Duration::from_secs(30);
#[cfg(test)]
const HOLD_RECHECK: Duration = Duration::from_millis(200);
/// Tries between two warnings while a read keeps failing.
const WARN_EVERY: u32 = 20;

/// Why apply waits on an entry. The node reads it for `/status`, the metrics, the read fences,
/// and the leader handoff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::node) struct Paused {
    /// The entry that apply cannot apply yet.
    pub index: u64,
    /// True for corrupt stored data: apply holds. False for a transient error: apply retries.
    pub held: bool,
    pub reason: String,
    /// The first failure on this entry.
    pub since: Instant,
}

/// The node side of the signals of an apply job that waits on a storage error.
pub(in crate::node) struct Control {
    stop: watch::Sender<bool>,
    preempt: Arc<watch::Sender<u64>>,
    paused: watch::Receiver<Option<Paused>>,
    /// The sender of the pause report, so a node test can set it without a job.
    #[cfg(test)]
    report: Arc<watch::Sender<Option<Paused>>>,
}

impl Control {
    /// End a waiting job at once. The worker stops.
    pub(super) fn stop(&self) {
        self.stop.send_replace(true);
    }

    /// Raft accepted a snapshot through `index`. A job that waits on an entry at or below it
    /// ends with its applied prefix, and queued work below it is dropped.
    pub(in crate::node) fn preempt(&self, index: u64) {
        self.preempt.send_if_modified(|covered| {
            let raise = index > *covered;
            *covered = (*covered).max(index);
            raise
        });
    }

    /// A handle that preempts from another task, and the pause reports, for tests.
    #[cfg(test)]
    pub(in crate::node) fn handles(
        &self,
    ) -> (Arc<watch::Sender<u64>>, watch::Receiver<Option<Paused>>) {
        (self.preempt.clone(), self.paused.clone())
    }

    /// The entry that apply waits on, if any.
    pub(in crate::node) fn paused(&self) -> Option<Paused> {
        self.paused.borrow().clone()
    }

    /// Set the pause report as a waiting job does.
    #[cfg(test)]
    pub(in crate::node) fn set_paused(&self, paused: Option<Paused>) {
        self.report.send_replace(paused);
    }
}

/// The apply side of the signals.
pub(super) struct Signals {
    stop: watch::Receiver<bool>,
    preempt: watch::Receiver<u64>,
    paused: Arc<watch::Sender<Option<Paused>>>,
}

impl Signals {
    /// Return true when a covering snapshot superseded a job that starts at `first`. Only a job
    /// that cannot apply in order can be superseded: one behind a job that ended early.
    pub(super) fn superseded(&self, first: u64, applied: u64) -> bool {
        first > applied + 1 && *self.preempt.borrow() >= first
    }
}

pub(super) fn signals() -> (Control, Signals) {
    let (stop, stopping) = watch::channel(false);
    let (preempt, preempted) = watch::channel(0);
    let (paused, pauses) = watch::channel(None);
    let paused = Arc::new(paused);
    let control = Control {
        stop,
        preempt: Arc::new(preempt),
        paused: pauses,
        #[cfg(test)]
        report: paused.clone(),
    };
    let signals = Signals {
        stop: stopping,
        preempt: preempted,
        paused,
    };
    (control, signals)
}

/// How a wait after a storage error ended.
#[derive(PartialEq, Eq)]
pub(super) enum Waited {
    /// Try again.
    Again,
    /// A snapshot covers the entry. The job ends with its applied prefix.
    Covered,
}

/// The tries of one job after storage errors.
#[derive(Default)]
pub(super) struct Retry {
    tries: u32,
    since: Option<Instant>,
}

impl Retry {
    /// Wait before the next try of entry `index` after `error`. Fail at once when the worker
    /// stops. Corrupt data holds the entry, and the next try runs after `HOLD_RECHECK`.
    pub(super) async fn wait(
        &mut self,
        signals: &mut Signals,
        index: u64,
        fault: StorageFault,
        error: &anyhow::Error,
    ) -> Result<Waited> {
        self.tries += 1;
        let held = fault == StorageFault::Corrupt;
        let delay = match held {
            true => HOLD_RECHECK,
            false => RETRY_FIRST
                .saturating_mul(1 << (self.tries - 1).min(16))
                .min(RETRY_CAP),
        };
        let error_text = format!("{error:#}");
        if self.tries == 1 && held {
            tracing::error!(
                index,
                error = error_text,
                "apply holds on corrupt stored data; the member serves its earlier state"
            );
        } else if self.tries == 1 {
            tracing::warn!(
                index,
                error = error_text,
                ?delay,
                "an apply read failed; retry"
            );
        } else if self.tries.is_multiple_of(WARN_EVERY) || held {
            let tries = self.tries;
            tracing::warn!(
                index,
                tries,
                error = error_text,
                "an apply read still fails"
            );
        }
        let since = *self.since.get_or_insert_with(Instant::now);
        signals.paused.send_replace(Some(Paused {
            index,
            held,
            reason: error_text,
            since,
        }));
        let waited = tokio::select! {
            _ = tokio::time::sleep(delay) => Ok(Waited::Again),
            _ = covered(&mut signals.preempt, index) => Ok(Waited::Covered),
            _ = stopped(&mut signals.stop) => {
                Err(anyhow::anyhow!("apply stopped while it retried a storage read: {error:#}"))
            }
        };
        if waited
            .as_ref()
            .is_ok_and(|waited| *waited == Waited::Covered)
        {
            tracing::warn!(
                index,
                "a snapshot covers the waiting entry; end the job early"
            );
        }
        waited
    }

    /// The job ended. Apply waits on no entry.
    fn done(&self, signals: &Signals) {
        signals
            .paused
            .send_if_modified(|paused| paused.take().is_some());
    }
}

/// Wait until the worker stops. A worker without a stop sender runs until the task ends.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    if stop.wait_for(|stop| *stop).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Wait until a snapshot that Raft accepted covers `index`.
async fn covered(preempt: &mut watch::Receiver<u64>, index: u64) {
    if preempt.wait_for(|covered| *covered >= index).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// The class of a storage error that apply can run again or hold on, or `None` when apply
/// must stop.
///
/// A transient or corrupt error must come from a read: the error crossed a `KvError` or a read
/// helper, or stored data did not decode. So no state changed. A missing object can come from
/// any read. Apply rebases on it first.
pub(super) fn retryable(error: &anyhow::Error) -> Option<StorageFault> {
    let read = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<Classified>())
        .map(|classified| classified.fault);
    let stored = error
        .chain()
        .any(|cause| cause.is::<StoredData>() || cause.is::<prost::DecodeError>());
    match (read, crate::mvcc::storage_fault::classify(error)) {
        (_, Some(StorageFault::Missing)) => Some(StorageFault::Missing),
        (Some(fault), _) => Some(fault),
        _ if stored => Some(StorageFault::Corrupt),
        _ => None,
    }
}

impl Executor {
    /// Apply committed entries. After a storage error of a read, the applied prefix keeps its
    /// replies, its membership changes, and its lease commands. The rest runs again from
    /// `applied + 1`, so each entry completes exactly once. In shared mode a missing object
    /// rebases the state at the applied index first. Corrupt data holds apply on its entry.
    /// Other errors stop apply.
    ///
    /// The flag is true when a covering snapshot ended the job early, at its prefix boundary.
    pub(super) async fn apply_retrying(
        &mut self,
        entries: Vec<Entry>,
        header: Header,
    ) -> Result<(Vec<AppliedCommand>, bool)> {
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
                retry.done(&self.signals);
                return Ok((replies, false));
            };
            next += (self.state.applied - before) as usize;
            let fault = match retryable(&error) {
                Some(StorageFault::Missing) if self.log.is_some() => {
                    tracing::warn!(%error, "an apply read lost its checkpoint object");
                    self.rebase_retrying(header, self.state.applied).await?;
                    StorageFault::Missing
                }
                Some(fault @ (StorageFault::Transient | StorageFault::Corrupt)) => fault,
                _ => return Err(error),
            };
            let index = self.state.applied + 1;
            if retry.wait(&mut self.signals, index, fault, &error).await? == Waited::Covered {
                retry.done(&self.signals);
                return Ok((replies, true));
            }
        }
    }

    /// Rebase the state at `applied`. A failed load leaves the state as it was, and a failed
    /// replay leaves a state below `applied`. Both run the rebase again after a storage error
    /// of a read. See `StateMachine::adopt_base`.
    async fn rebase_retrying(&mut self, header: Header, applied: u64) -> Result<()> {
        let mut retry = Retry::default();
        loop {
            let Err(error) = Box::pin(self.rebase(header, applied)).await else {
                retry.done(&self.signals);
                return Ok(());
            };
            let Some(fault) = retryable(&error) else {
                return Err(error);
            };
            // A rebase is no entry, so no snapshot preempts it.
            retry
                .wait(&mut self.signals, u64::MAX, fault, &error)
                .await?;
        }
    }

    /// Adopt an opened base. A transient storage error of the load leaves the state as it was,
    /// so the adoption runs again.
    pub(super) async fn adopt_retrying(
        &mut self,
        base: std::sync::Arc<crate::state::view::Base>,
        term: u64,
    ) -> Result<()> {
        let mut retry = Retry::default();
        loop {
            match self.state.adopt_opened(base.clone(), term).await {
                Err(error) if retryable(&error) == Some(StorageFault::Transient) => {
                    let fault = StorageFault::Transient;
                    retry
                        .wait(&mut self.signals, u64::MAX, fault, &error)
                        .await?;
                }
                adopted => {
                    retry.done(&self.signals);
                    return adopted;
                }
            }
        }
    }

    /// Install a snapshot. In shared mode a transient storage error of the base load leaves the
    /// state as it was, so the install runs again. A replicated install is not staged, so its
    /// errors stop apply.
    pub(super) async fn install_retrying(&mut self, snapshot: &Snapshot) -> Result<()> {
        let mut retry = Retry::default();
        loop {
            match Box::pin(self.state.install_snapshot(snapshot)).await {
                Err(error)
                    if self.state.is_shared()
                        && retryable(&error) == Some(StorageFault::Transient) =>
                {
                    let fault = StorageFault::Transient;
                    retry
                        .wait(&mut self.signals, u64::MAX, fault, &error)
                        .await?;
                }
                installed => {
                    retry.done(&self.signals);
                    return installed;
                }
            }
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
