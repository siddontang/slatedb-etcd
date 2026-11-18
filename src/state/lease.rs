use super::*;

impl StateMachine {
    pub async fn apply_lease(
        &mut self,
        index: u64,
        command: &crate::lease::LeaseCommand,
    ) -> Result<Outcome<crate::lease::LeaseReply>> {
        self.apply_lease_from(index, command, 0).await
    }

    pub async fn apply_lease_from(
        &mut self,
        index: u64,
        command: &crate::lease::LeaseCommand,
        origin: u64,
    ) -> Result<Outcome<crate::lease::LeaseReply>> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        use crate::lease::LeaseCommand as L;
        if matches!(command, L::Expire { .. }) {
            crate::failpoint::hit("lease-before-expire");
        }

        let corrupt = self.alarms.iter().any(|a| a.alarm == 2)
            && matches!(
                command,
                L::Grant { .. } | L::Revoke { .. } | L::Expire { .. }
            );
        let full = matches!(command, L::Grant { .. })
            && (self.alarms.iter().any(|a| a.alarm == 1)
                || (self.quota > 0 && self.logical_size() >= self.quota));
        if corrupt || full {
            if full && !self.alarms.iter().any(|a| a.alarm == 1) {
                self.alarms.push(crate::proto::etcdserverpb::AlarmMember {
                    member_id: origin,
                    alarm: 1,
                });
            }
            let mut batch = WriteBatch::new();
            batch.put(
                crate::maintenance::ALARMS,
                serde_json::to_vec(&self.alarms)?,
            );
            batch.put(APPLIED, index.to_be_bytes());
            self.write_batch(batch, index).await?;
            self.applied = index;
            return Ok(Outcome {
                result: Err(if corrupt {
                    mvcc::KvError::new(tonic::Code::DataLoss, "corrupt cluster")
                } else {
                    mvcc::KvError::new(
                        tonic::Code::ResourceExhausted,
                        "mvcc: database space exceeded",
                    )
                }),
            });
        }
        let (result, mut batch, revision, change) = match crate::lease::prepare(
            &self.db,
            self.revision,
            self.compact_revision,
            index,
            &self.leases,
            command,
        )
        .await
        {
            Ok((reply, batch, revision, change)) => (Ok(reply), batch, revision, change),
            Err(error) => {
                ensure!(
                    error.code != tonic::Code::Internal as i32,
                    "{}",
                    error.message
                );
                (Err(error), WriteBatch::new(), self.revision, None)
            }
        };
        batch.put(APPLIED, index.to_be_bytes());
        self.write_batch(batch, index).await?;
        self.applied = index;
        self.revision = revision;
        if let Some((id, record)) = change {
            let leases = Arc::make_mut(&mut self.leases);
            if let Some(record) = record {
                leases.insert(id, record);
            } else {
                leases.remove(&id);
            }
        }
        Ok(Outcome { result })
    }
}
