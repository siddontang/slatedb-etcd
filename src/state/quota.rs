//! The logical quota of replicated mode as cluster state.
//!
//! A `SetQuota` entry stores the quota in `etcd/quota`, and apply then reads it only from there.
//! So every member evaluates an entry under the same value. Until the first `SetQuota` entry
//! applies, a member uses its own flag, as earlier binaries did. That flag is in memory only.
//!
//! An operator request carries its wall-clock time. The newest request wins; see
//! `ClusterQuota`.
//!
//! The row is under `etcd/`, so format 4 images and backups carry it, and older readers accept
//! it. Like the alarms row, it counts toward the logical size on every member.
//!
//! Shared mode takes the quota from the cluster claim. It ignores the row, and a `SetQuota`
//! entry stores only the apply position there.

use super::*;

/// The cluster quota row. See `ClusterQuota::encode`.
pub(crate) const KEY: &[u8] = b"etcd/quota";

/// The cluster quota and the operator request that set it.
///
/// Requests are ordered by `(requested_at, member, bytes)`, so every member resolves two
/// requests the same way. `requested_at` is the wall clock of the requesting member in
/// milliseconds. A seed has zero, so every operator request is newer than a seed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ClusterQuota {
    /// A non-positive value disables the check.
    pub bytes: i64,
    pub requested_at: u64,
    /// The member that requested the value.
    pub member: u64,
}

impl ClusterQuota {
    /// The order of two requests. A later key wins.
    pub(crate) fn key(&self) -> (u64, u64, i64) {
        (self.requested_at, self.member, self.bytes)
    }

    /// Four big-endian values: `bytes`, `requested_at`, `member`, and the boundary of
    /// `StateMachine::quota_boundary`.
    fn encode(&self, boundary: u64) -> [u8; 32] {
        let mut row = [0; 32];
        row[..8].copy_from_slice(&self.bytes.to_be_bytes());
        row[8..16].copy_from_slice(&self.requested_at.to_be_bytes());
        row[16..24].copy_from_slice(&self.member.to_be_bytes());
        row[24..].copy_from_slice(&boundary.to_be_bytes());
        row
    }

    fn decode(row: &[u8]) -> Result<(Self, u64)> {
        let row: &[u8; 32] = row.try_into().context("invalid cluster quota")?;
        let quota = Self {
            bytes: i64::from_be_bytes(row[..8].try_into()?),
            requested_at: u64::from_be_bytes(row[8..16].try_into()?),
            member: u64::from_be_bytes(row[16..24].try_into()?),
        };
        Ok((quota, u64::from_be_bytes(row[24..].try_into()?)))
    }
}

/// Read the cluster quota of a replicated state and its boundary. `None` means that no
/// `SetQuota` entry applied.
pub(super) async fn load(reads: &impl StateRead) -> Result<(Option<ClusterQuota>, u64)> {
    Ok(match reads.get(KEY).await? {
        Some(row) => {
            let (quota, boundary) = ClusterQuota::decode(&row)?;
            (Some(quota), boundary)
        }
        None => (None, 0),
    })
}

/// Return true when apply checks the quota for `command`: a Put, a transaction with a Put, or a
/// lease grant.
pub(crate) fn checks_quota(command: &crate::RaftCommand) -> bool {
    use crate::RaftCommand as C;
    match command {
        C::Etcd { etcd, .. } => crate::maintenance::has_put(etcd),
        C::Lease { lease, .. } => matches!(lease, crate::lease::LeaseCommand::Grant { .. }),
        C::Secured { command, .. } => checks_quota(command),
        _ => false,
    }
}

impl StateMachine {
    /// Set the effective `--quota-backend-bytes` value of this member. Replicated apply uses it
    /// only while the state has no cluster quota. A non-positive value disables the check.
    pub(crate) fn set_quota_flag(&mut self, flag: i64) {
        self.quota_flag = flag;
        self.refresh_quota();
    }

    /// The effective flag value of this member.
    pub(crate) fn quota_flag(&self) -> i64 {
        self.quota_flag
    }

    /// The quota of the last applied `SetQuota` entry. Always `None` in shared mode.
    pub(crate) fn cluster_quota(&self) -> Option<ClusterQuota> {
        self.cluster_quota
    }

    /// The quota that apply uses, as a signed value. A non-positive value means no limit.
    pub(crate) fn quota_bytes(&self) -> i64 {
        match self.cluster_quota {
            _ if self.is_shared() => self.quota as i64,
            Some(quota) => quota.bytes,
            None => self.quota_flag,
        }
    }

    /// Return true when the flag of this member differs from the quota that apply uses.
    pub(crate) fn quota_mismatch(&self) -> bool {
        if self.is_shared() {
            // The claim stores a disabled quota as zero.
            return self.quota_flag.max(0) as u64 != self.quota;
        }
        self.quota_bytes() != self.quota_flag
    }

    /// Apply a `SetQuota` entry. A seed sets the quota only when the state has none. Another
    /// request sets it only when it is newer than the request of the row. So a leader that is
    /// behind on apply cannot replace a newer value with an older request.
    pub(crate) async fn apply_set_quota(
        &mut self,
        index: u64,
        request: ClusterQuota,
        seed: bool,
    ) -> Result<()> {
        let newer = match self.cluster_quota {
            None => true,
            Some(row) => !seed && request.key() > row.key(),
        };
        if self.is_shared() || !newer {
            self.apply(index, None).await?;
            return Ok(());
        }
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        if self.cluster_quota.is_none()
            && (self.revision > 1 || !self.leases.is_empty() || !self.alarms.is_empty())
        {
            // Entries before this one applied under member flags. A fresh cluster has none.
            self.quota_boundary = index;
        }
        let mut batch = WriteBatch::new();
        batch.put(KEY, request.encode(self.quota_boundary));
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        self.applied = index;
        self.cluster_quota = Some(request);
        self.refresh_quota();
        Ok(())
    }

    /// Read the cluster quota again after the rows of the state changed.
    pub(super) async fn reload_quota(&mut self) -> Result<()> {
        (self.cluster_quota, self.quota_boundary) = load(self.applied_state()).await?;
        self.refresh_quota();
        Ok(())
    }

    /// Set the quota of replicated apply: the cluster quota, or the flag without one.
    /// Shared mode keeps the quota of the claim.
    fn refresh_quota(&mut self) {
        if !self.is_shared() {
            self.quota = self.quota_bytes().max(0) as u64;
        }
    }
}
