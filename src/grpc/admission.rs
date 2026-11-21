//! Bound client reads before they enter the Raft owner's ReadIndex table.

use tokio::sync::{Semaphore, SemaphorePermit};
use tonic::Status;

pub(super) struct ReadAdmission {
    waiting: Semaphore,
    active: Semaphore,
}

/// Client reads that may wait for an active slot. A waiter holds only its decoded
/// request, so this bounds memory while a burst of clients queues instead of failing.
const MAX_WAITING_READS: usize = 8192;
/// How long a read may wait for an active slot before the node reports overload.
const ADMISSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

impl ReadAdmission {
    /// Linearizable reads entering the ReadIndex table.
    pub fn reads() -> Self {
        Self {
            waiting: Semaphore::new(MAX_WAITING_READS),
            // Leave slots for health checks, authorization, and local barriers.
            active: Semaphore::new(crate::node::MAX_PENDING_READS * 3 / 4),
        }
    }

    /// Follower reads waiting for the local apply barrier.
    pub fn barriers() -> Self {
        Self {
            waiting: Semaphore::new(512),
            active: Semaphore::new(crate::node::MAX_APPLY_BARRIERS * 3 / 4),
        }
    }

    /// Wait for an active slot. Dropping the future (client cancel or deadline) releases
    /// the waiting slot. A full waiting queue or a timeout means real overload.
    pub async fn acquire(&self) -> Result<SemaphorePermit<'_>, Status> {
        let _queue = self
            .waiting
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("etcdserver: too many queued reads"))?;
        tokio::time::timeout(ADMISSION_TIMEOUT, self.active.acquire())
            .await
            // etcd reports an expired server-side request wait as ErrGRPCTimeout.
            .map_err(|_| Status::unavailable("etcdserver: request timed out"))?
            .map_err(|_| Status::unavailable("etcdserver: node unavailable"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queued_reads_resume_and_cancellation_releases_the_waiter() {
        let admission = ReadAdmission {
            waiting: Semaphore::new(1),
            active: Semaphore::new(1),
        };
        let active = admission.acquire().await.unwrap();
        let mut waiting = Box::pin(admission.acquire());
        assert!(futures::poll!(&mut waiting).is_pending());
        assert_eq!(
            admission.acquire().await.unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        drop(waiting);
        let mut next = Box::pin(admission.acquire());
        assert!(futures::poll!(&mut next).is_pending());
        drop(active);
        assert!(next.await.is_ok());
    }

    #[tokio::test]
    async fn a_burst_of_clients_waits_for_read_slots_instead_of_failing() {
        let admission = ReadAdmission::reads();
        let mut active = vec![];
        for _ in 0..crate::node::MAX_PENDING_READS * 3 / 4 {
            active.push(admission.acquire().await.unwrap());
        }
        // A thousand concurrent clients wait; none is rejected while slots are busy.
        let mut waiting: Vec<_> = (0..1000).map(|_| Box::pin(admission.acquire())).collect();
        for waiter in &mut waiting {
            assert!(futures::poll!(waiter).is_pending());
        }
        drop(active);
        for waiter in waiting {
            drop(waiter.await.unwrap());
        }
    }
}
