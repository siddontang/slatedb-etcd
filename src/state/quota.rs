//! The logical quota. Every member takes it from the cluster claim.

use super::*;

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
}
