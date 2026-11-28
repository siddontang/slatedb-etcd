//! The logical quota and the logical size that it limits. Every member takes the quota from the
//! cluster claim.

use super::*;
use crate::batch::Original;
use bytes::Bytes;

#[cfg(test)]
thread_local! {
    /// The old rows that the size count read on this thread.
    pub(crate) static SIZE_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

impl StateMachine {
    /// Set the effective `--quota-backend-bytes` value of this member. Apply uses the quota of
    /// the cluster claim. The flag only shows a difference from it.
    pub(crate) fn set_quota_flag(&mut self, flag: i64) {
        self.quota_flag = flag;
    }

    /// The quota that apply uses, as a signed value. A non-positive value means no limit.
    pub(crate) fn quota_bytes(&self) -> i64 {
        self.quota as i64
    }

    /// Return true when the flag of this member differs from the quota that apply uses.
    pub(crate) fn quota_mismatch(&self) -> bool {
        // The claim stores a disabled quota as zero.
        self.quota_flag.max(0) as u64 != self.quota
    }

    /// Update the logical size `size` by the mutations of `batch`, which apply after
    /// `previous`. The key index supplies old current-row lengths without a read.
    pub(crate) async fn logical_size_after(
        &self,
        batch: &WriteBatch,
        previous: Option<&WriteBatch>,
        mut size: Usage,
    ) -> Result<Usage> {
        for (key, value, original) in batch.changes() {
            if !key.starts_with(b"etcd/") {
                continue;
            }
            let old_len = match original {
                Original::Known(length) => length,
                Original::Read => match previous.and_then(|earlier| earlier.get(key)) {
                    Some(old) => old.map(|v| v.len()),
                    None if key.starts_with(crate::mvcc::CURRENT) => {
                        self.key_index.get_row(key).map(|meta| meta.row_len)
                    }
                    None => {
                        #[cfg(test)]
                        SIZE_READS.with(|reads| reads.set(reads.get() + 1));
                        self.applied_state().get(key).await?.map(|v| v.len())
                    }
                },
            };
            size.replace_row(key, old_len, value.map(Bytes::len))?;
        }
        Ok(size)
    }
}

/// Logical bytes of `etcd/` rows by kind. The quota limits the total.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    /// Current rows of live keys.
    pub current: u64,
    /// History versions and their hash digests. Compaction removes them.
    pub history: u64,
    /// Watch journal rows. Compaction removes them.
    pub journal: u64,
    /// Metadata rows: auth, members, leases, alarms, revision and compaction records.
    pub other: u64,
}

impl Usage {
    pub fn total(&self) -> u64 {
        self.current + self.history + self.journal + self.other
    }

    /// The total, or `None` when a corrupt split does not fit in a `u64`.
    pub fn checked_total(&self) -> Option<u64> {
        self.current
            .checked_add(self.history)?
            .checked_add(self.journal)?
            .checked_add(self.other)
    }

    fn kind(&mut self, key: &[u8]) -> &mut u64 {
        if key.starts_with(crate::mvcc::CURRENT) {
            &mut self.current
        } else if key.starts_with(crate::mvcc::HISTORY) || key.starts_with(crate::maintenance::HASH)
        {
            &mut self.history
        } else if key.starts_with(crate::watch::EVENTS) {
            &mut self.journal
        } else {
            &mut self.other
        }
    }

    /// Count one stored row. Callers pass only `etcd/` keys.
    pub(crate) fn add_row(&mut self, key: &[u8], len: usize) -> Result<()> {
        self.replace_row(key, None, Some(len))
    }

    /// Replace the old row length with the new one. `None` means that no row exists.
    pub(crate) fn replace_row(
        &mut self,
        key: &[u8],
        old: Option<usize>,
        new: Option<usize>,
    ) -> Result<()> {
        let bytes = |len| (key.len() + len) as u64;
        let slot = self.kind(key);
        *slot = slot
            .checked_sub(old.map_or(0, bytes))
            .context("logical size counter underflow")?
            .checked_add(new.map_or(0, bytes))
            .context("logical size counter overflow")?;
        Ok(())
    }
}
