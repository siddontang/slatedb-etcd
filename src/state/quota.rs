//! The logical quota of replicated mode as cluster state.
//!
//! A `SetQuota` entry stores the quota in `etcd/quota`, and apply then reads it only from there.
//! So every member evaluates an entry under the same value. Until the first `SetQuota` entry
//! applies, a member uses its own flag, as earlier binaries did. That flag is in memory only.
//!
//! The row is under `etcd/`, so format 4 images and backups carry it, and older readers accept
//! it. Like the alarms row, it counts toward the logical size on every member.
//!
//! Shared mode takes the quota from the cluster claim. It ignores the row, and a `SetQuota`
//! entry stores only the apply position there.

use super::*;

/// The cluster quota: one big-endian `i64`. A non-positive value disables the check.
pub(crate) const KEY: &[u8] = b"etcd/quota";

/// Read the cluster quota of a replicated state. `None` means that no `SetQuota` entry applied.
pub(super) async fn load(reads: &impl StateRead) -> Result<Option<i64>> {
    reads
        .get(KEY)
        .await?
        .map(|row| {
            let row = row.as_ref().try_into().context("invalid cluster quota")?;
            Ok(i64::from_be_bytes(row))
        })
        .transpose()
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
    pub(crate) fn cluster_quota(&self) -> Option<i64> {
        self.cluster_quota
    }

    /// The quota that apply uses, as a signed value. A non-positive value means no limit.
    pub(crate) fn quota_bytes(&self) -> i64 {
        match self.cluster_quota {
            _ if self.is_shared() => self.quota as i64,
            Some(bytes) => bytes,
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

    /// Apply a `SetQuota` entry. A seed entry changes nothing when the state has a cluster
    /// quota, so a leader with an old view cannot replace a newer value.
    pub(crate) async fn apply_set_quota(
        &mut self,
        index: u64,
        bytes: i64,
        seed: bool,
    ) -> Result<()> {
        if self.is_shared() || (seed && self.cluster_quota.is_some()) {
            self.apply(index, None).await?;
            return Ok(());
        }
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let mut batch = WriteBatch::new();
        batch.put(KEY, bytes.to_be_bytes());
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        self.applied = index;
        self.cluster_quota = Some(bytes);
        self.refresh_quota();
        Ok(())
    }

    /// Read the cluster quota again after the rows of the state changed.
    pub(super) async fn reload_quota(&mut self) -> Result<()> {
        self.cluster_quota = load(self.applied_state()).await?;
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
