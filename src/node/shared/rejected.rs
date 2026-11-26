//! Report the bases that an adoption dropped. See `Executor::adopt_retrying`.
//!
//! A paused member opens the newest checkpoint at each poll, so the same base can fail again.
//! Only the first failure of each checkpoint logs. Each failure of corrupt data counts as a
//! corrupt read. `/status` shows the last dropped base until the pause ends.

use super::*;
use crate::mvcc::storage_fault::{StorageFault, CORRUPT_READS};

/// The last base that an adoption dropped, with its failure count.
#[derive(Default)]
pub(super) struct Rejected(Option<(apply::RejectedBase, u64)>);

impl Rejected {
    /// Count a failure of `rejected`. Return true for the first failure of its checkpoint.
    fn note(&mut self, rejected: &apply::RejectedBase) -> bool {
        let (failures, first) = match &self.0 {
            Some((last, failures)) if last.checkpoint == rejected.checkpoint => {
                (failures + 1, false)
            }
            _ => (1, true),
        };
        self.0 = Some((rejected.clone(), failures));
        first
    }

    /// A dropped base explains a pause only. The end of the pause ends the report.
    pub(super) fn clear(&mut self) {
        self.0 = None;
    }

    /// The `rejected_base` field of the writer status.
    pub(super) fn status(&self) -> Value {
        match &self.0 {
            Some((rejected, failures)) => json!({
                "checkpoint": rejected.checkpoint.map(|id| id.to_string()),
                "index": rejected.index,
                "class": format!("{:?}", rejected.fault).to_lowercase(),
                "reason": rejected.reason,
                "failures": failures,
            }),
            None => Value::Null,
        }
    }
}

impl Node {
    /// Report a base that an adoption dropped.
    pub(in crate::node) fn reject_base(&mut self, rejected: apply::RejectedBase) {
        if rejected.fault == StorageFault::Corrupt {
            CORRUPT_READS.inc();
        }
        let Some(writer) = &mut self.writer else {
            return;
        };
        if writer.rejected.note(&rejected) {
            tracing::error!(
                checkpoint = ?rejected.checkpoint,
                index = rejected.index,
                fault = ?rejected.fault,
                reason = rejected.reason,
                "drop a base that does not load; keep the current base"
            );
        }
    }
}
