//! Bound client reads before they enter the Raft owner's ReadIndex table.

use tokio::sync::{Semaphore, SemaphorePermit};
use tonic::Status;

pub(super) struct ReadAdmission {
    waiting: Semaphore,
    active: Semaphore,
}

impl Default for ReadAdmission {
    fn default() -> Self {
        Self {
            waiting: Semaphore::new(512),
            // Leave slots for health checks, authorization, and local barriers.
            active: Semaphore::new(crate::node::MAX_PENDING_READS * 3 / 4),
        }
    }
}

impl ReadAdmission {
    pub async fn acquire(&self) -> Result<SemaphorePermit<'_>, Status> {
        let _queue = self
            .waiting
            .try_acquire()
            .map_err(|_| Status::resource_exhausted("etcdserver: too many queued reads"))?;
        tokio::time::timeout(std::time::Duration::from_secs(10), self.active.acquire())
            .await
            .map_err(|_| Status::deadline_exceeded("etcdserver: read admission timed out"))?
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
}
