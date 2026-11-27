//! The write probe of a member: a create and a delete of one scratch object in the shared
//! state, with the store client of the member's own writer.
//!
//! The member creates `shared/state/.probe/<member>-<process>-<nonce>` with `PutMode::Create`
//! and deletes it. SlateDB 0.18.1 writes each manifest version with `PutMode::Create`
//! (`slatedb-txn-obj-0.18.1/src/object_store.rs:383`), so a create and a delete show the access
//! that the writer needs.

use super::*;
use serde::{Deserialize, Serialize};
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};

/// The peer route that runs a write probe on a member.
pub(crate) const WRITE_PROBE_ROUTE: &str = "/raft/write-probe";
/// The prefix of the scratch objects under the shared state.
const SCRATCH: &str = ".probe";
/// The longest time of one probe on the member.
const PROBE_TIME: Duration = Duration::from_secs(10);

/// The body of a write probe.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct WriteProbeRequest {
    /// The member that the leader probes.
    pub member: u64,
    pub nonce: u64,
}

/// The reply of a member to a write probe.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteProbeReply {
    /// The nonce of the probe.
    pub nonce: u64,
    /// The process of the member. See `crate::peer::process_id`.
    pub process: u64,
    /// The store ID of the member's shared store.
    pub store: Option<String>,
    /// The error of a failed probe. `None` when the create and the delete succeeded.
    pub error: Option<String>,
}

/// The write probes of this member, on its own writer's store client.
pub(crate) struct Prober {
    member: u64,
    source: Arc<Source>,
    /// The scratch object of an earlier probe that can still exist: its create or its delete
    /// failed. A probe removes it before it creates another, so this member leaves at most one
    /// scratch object. The lock also lets only one probe run at a time.
    leftover: tokio::sync::Mutex<Option<Path>>,
}

impl Prober {
    pub(in crate::node) fn new(member: u64, source: Arc<Source>) -> Self {
        Self {
            member,
            source,
            leftover: Default::default(),
        }
    }

    /// The member that this prober belongs to.
    pub(crate) fn member(&self) -> u64 {
        self.member
    }

    /// Create and delete one scratch object for `nonce`, in at most `PROBE_TIME`.
    pub(crate) async fn probe(&self, nonce: u64) -> Result<()> {
        tokio::time::timeout(PROBE_TIME, self.run(nonce))
            .await
            .map_err(|_| anyhow::anyhow!("the write probe timed out"))?
    }

    async fn run(&self, nonce: u64) -> Result<()> {
        let mut leftover = self.leftover.lock().await;
        if let Some(path) = leftover.clone() {
            self.remove(&path)
                .await
                .with_context(|| format!("cannot delete {path} of an earlier write probe"))?;
            *leftover = None;
        }
        let name = format!("{}-{}-{nonce}", self.member, crate::peer::process_id());
        let path = Path::from(format!("{}/{SCRATCH}/{name}", self.source.state()));
        // A create that fails or times out can still reach the store.
        *leftover = Some(path.clone());
        let payload = PutPayload::from_static(b"write probe\n");
        self.source
            .store
            .put_opts(&path, payload, PutOptions::from(PutMode::Create))
            .await
            .with_context(|| format!("cannot create {path}"))?;
        self.remove(&path)
            .await
            .with_context(|| format!("cannot delete {path}"))?;
        *leftover = None;
        Ok(())
    }

    /// Delete `path`. A refused delete of an object that does not exist is no leak: a learner
    /// with read-only access cannot delete, and its create stored nothing.
    async fn remove(&self, path: &Path) -> Result<()> {
        use slatedb::object_store::Error::NotFound;
        let error = match self.source.store.delete(path).await {
            Ok(()) | Err(NotFound { .. }) => return Ok(()),
            Err(error) => error,
        };
        match self.source.store.head(path).await {
            Err(NotFound { .. }) => Ok(()),
            _ => Err(error.into()),
        }
    }
}

#[cfg(test)]
#[path = "write_proof_tests.rs"]
mod tests;
