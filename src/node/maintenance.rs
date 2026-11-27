use super::*;
use crate::mvcc::{KvError, Outcome};
use tonic::Code;

impl Node {
    /// The quota fields of `/status`.
    pub(super) fn quota_status(&self) -> Value {
        json!({
            "bytes": self.state.quota_bytes,
            "flag_bytes": self.options.effective_quota(),
            "flag_mismatch": self.state.quota_mismatch,
        })
    }

    /// The `db_size_quota` of Maintenance Status. Like etcd, it is the configured value, unless
    /// the quota that apply uses differs from it.
    pub(super) fn status_quota(&self) -> i64 {
        if self.state.quota_mismatch {
            self.state.quota_bytes
        } else {
            self.options.quota_bytes
        }
    }

    pub(super) fn move_leader(
        &mut self,
        target: u64,
        credentials: crate::auth::Credentials,
        response: Response,
    ) -> Result<()> {
        let checked = self.state.auth.admin(&credentials).and_then(|()| {
            let member = self
                .state
                .members
                .members
                .get(&target)
                .ok_or_else(|| KvError::new(Code::NotFound, "bad leader transferee"))?;
            if member.is_learner {
                return Err(KvError::new(
                    Code::FailedPrecondition,
                    "bad leader transferee",
                ));
            }
            Ok(())
        });
        if let Err(error) = checked {
            let _ = response.send(Ok(serde_json::to_value(Outcome::<Value> {
                result: Err(error),
            })?));
            return Ok(());
        }
        if self.raw.raft.state != StateRole::Leader {
            let _ = response.send(Err(self.error("not leader")));
            return Ok(());
        }
        if self.transfers.len() >= 64 {
            let _ = response.send(Err(self.error("too many leader transfers")));
            return Ok(());
        }
        self.transfers.push((target, Instant::now(), response));
        // A shared-mode leader first publishes a checkpoint of its applied state. The members
        // then hold small overlays, and the new leader materializes little.
        let applied = self.state.applied;
        let writer = &mut self.writer;
        writer.transfer = Some((target, writer.covering(applied)));
        writer.request_publication(applied);
        self.finish_transfers()
    }

    pub(super) fn finish_transfers(&mut self) -> Result<()> {
        let (base, _, _) = self.state.view;
        // Transfer once a base covers the applied state, or after a bounded wait.
        let waited = self
            .transfers
            .iter()
            .all(|(_, started, _)| started.elapsed() >= Duration::from_secs(3));
        if let Some((target, covering)) = self.writer.transfer {
            if base >= covering || waited {
                self.writer.transfer = None;
                self.raw.transfer_leader(target);
            }
        }
        let limit = Duration::from_secs(10);
        let mut pending = vec![];
        for (target, started, response) in std::mem::take(&mut self.transfers) {
            if self.raw.raft.leader_id == target {
                let mut header = ResponseHeader {
                    revision: self.state.revision,
                    ..Default::default()
                };
                self.fill_header(Some(&mut header));
                let _ = response.send(Ok(serde_json::to_value(Outcome {
                    result: Ok(json!({"header": header})),
                })?));
            } else if started.elapsed() >= limit {
                let _ = response.send(Ok(serde_json::to_value(Outcome::<Value> {
                    result: Err(KvError::new(
                        Code::DeadlineExceeded,
                        "leader transfer timed out",
                    )),
                })?));
            } else if !response.is_closed() {
                pending.push((target, started, response));
            }
        }
        self.transfers = pending;
        Ok(())
    }
}
