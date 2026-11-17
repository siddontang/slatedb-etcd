use super::*;
use crate::mvcc::{KvError, Outcome};
use tonic::Code;

impl Node {
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
        self.raw.transfer_leader(target);
        self.finish_transfers()
    }

    pub(super) fn finish_transfers(&mut self) -> Result<()> {
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
            } else if started.elapsed() >= Duration::from_secs(5) {
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
