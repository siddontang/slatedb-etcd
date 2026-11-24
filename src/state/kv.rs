use super::*;

impl StateMachine {
    pub async fn read_etcd(&self, request: &RequestOp) -> Result<Outcome> {
        let result = mvcc::read(
            self.applied_state(),
            self.revision,
            self.compact_revision,
            request,
            &self.leases,
        )
        .await;
        if let Err(error) = &result {
            ensure!(
                error.code != tonic::Code::Internal as i32,
                "{}",
                error.message
            );
        }
        Ok(Outcome { result })
    }

    pub async fn apply_etcd_quota(
        &mut self,
        index: u64,
        request: &RequestOp,
        origin: u64,
    ) -> Result<Outcome> {
        let corrupt = self.alarms.iter().any(|a| a.alarm == 2);
        let full = crate::maintenance::has_put(request)
            && (self.alarms.iter().any(|a| a.alarm == 1)
                || (self.quota > 0 && self.logical_size() >= self.quota));
        if corrupt || full {
            let mut batch = WriteBatch::new();
            if full && !self.alarms.iter().any(|a| a.alarm == 1) {
                self.alarms.push(crate::proto::etcdserverpb::AlarmMember {
                    member_id: origin,
                    alarm: 1,
                });
            }
            batch.put(
                crate::maintenance::ALARMS,
                serde_json::to_vec(&self.alarms)?,
            );
            self.mark_applied(&mut batch, index);
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
        self.apply_etcd(index, request).await
    }

    pub async fn apply_etcd(&mut self, index: u64, request: &RequestOp) -> Result<Outcome> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let (result, mut batch, revision, counts) = match mvcc::prepare_with_counts(
            self.applied_state(),
            Some(&self.key_index),
            self.revision,
            self.compact_revision,
            request,
            &self.leases,
        )
        .await
        {
            Ok((response, batch, revision, counts)) => (Ok(response), batch, revision, counts),
            Err(error) => {
                ensure!(
                    error.code != tonic::Code::Internal as i32,
                    "{}",
                    error.message
                );
                (
                    Err(error),
                    WriteBatch::new(),
                    self.revision,
                    Default::default(),
                )
            }
        };
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        self.applied = index;
        self.revision = revision;
        counts.publish(&self.metrics);
        Ok(Outcome { result })
    }

    pub(crate) fn physical_compact_revision(
        &self,
        request: &CompactionRequest,
        outcome: &Outcome<CompactionResponse>,
    ) -> Option<i64> {
        // A physical retry also waits for an earlier logical compaction to reach storage.
        let compacted = request.revision <= self.compact_revision
            && outcome
                .result
                .as_ref()
                .is_err_and(|e| e.code == tonic::Code::OutOfRange as i32);
        (request.physical && (outcome.result.is_ok() || compacted)).then_some(self.compact_revision)
    }

    #[cfg(test)]
    pub async fn watch(
        &self,
        request: &crate::proto::etcdserverpb::WatchCreateRequest,
        through: Option<i64>,
    ) -> Result<crate::watch::WatchPage> {
        crate::watch::read(
            &*self.db.snapshot()?,
            through.unwrap_or(self.revision).min(self.revision),
            self.compact_revision,
            request,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.message))
    }
}
