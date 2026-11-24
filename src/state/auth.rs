use super::*;

impl StateMachine {
    pub async fn apply_auth(
        &mut self,
        index: u64,
        credentials: &crate::auth::Credentials,
        command: &crate::auth::Command,
    ) -> Result<Outcome<serde_json::Value>> {
        ensure!(index == self.applied + 1, "non-contiguous auth apply");
        let mut next = self.auth.as_ref().clone();
        let result = next.execute(credentials, command, self.revision);
        let mut batch = WriteBatch::new();
        if result.is_ok() {
            batch.put(crate::auth::KEY, serde_json::to_vec(&next)?);
        }
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        self.applied = index;
        if result.is_ok() {
            self.auth = Arc::new(next);
        }
        Ok(Outcome { result })
    }

    pub async fn read_etcd_authorized(
        &self,
        request: &RequestOp,
        credentials: Option<&crate::auth::Credentials>,
    ) -> Result<Outcome> {
        if let Some(credentials) = credentials {
            if let Err(error) = self.auth.kv(credentials, request) {
                return Ok(Outcome { result: Err(error) });
            }
        }
        self.read_etcd(request).await
    }

    pub async fn authorize(
        &self,
        auth: &crate::auth::Credentials,
        command: &crate::RaftCommand,
    ) -> std::result::Result<(), mvcc::KvError> {
        use crate::lease::LeaseCommand as L;
        match command {
            crate::RaftCommand::Etcd { etcd, .. } => self.authorize_kv(auth, etcd).await,
            crate::RaftCommand::Lease { lease, .. } => {
                self.auth.authenticated(auth)?;
                let (id, write) = match lease {
                    L::KeepAlive { id } | L::Revoke { id } => (*id, true),
                    L::TimeToLive { id, keys: true } => (*id, false),
                    L::TimeToLive { keys: false, .. } => return Ok(()),
                    L::Grant { .. } | L::List => return Ok(()),
                    L::Expire { .. } => return Err(crate::auth::denied()),
                };
                if self.auth.enabled {
                    let mut scan = self
                        .applied_state()
                        .scan_under(mvcc::CURRENT, &Default::default())
                        .await
                        .map_err(mvcc::KvError::storage)?;
                    use prost::Message;
                    while let Some(row) = scan.next().await.map_err(mvcc::KvError::storage)? {
                        let kv = crate::proto::mvccpb::KeyValue::decode(row.value)
                            .map_err(mvcc::KvError::storage)?;
                        if kv.lease == id {
                            self.auth.range(auth, &kv.key, &[], write)?;
                        }
                    }
                }
                Ok(())
            }
            crate::RaftCommand::Compact { .. } => self.auth.admin(auth),
            _ => Err(crate::auth::denied()),
        }
    }

    /// Check a KV request. Leased puts also need write access to keys on that lease.
    pub(crate) async fn authorize_kv(
        &self,
        auth: &crate::auth::Credentials,
        etcd: &RequestOp,
    ) -> std::result::Result<(), mvcc::KvError> {
        self.auth.kv(auth, etcd)?;
        let mut requests = vec![etcd];
        let mut leases = std::collections::BTreeSet::new();
        while let Some(request) = requests.pop() {
            use crate::proto::etcdserverpb::request_op::Request as Op;
            match &request.request {
                Some(Op::RequestPut(put)) if put.lease != 0 => {
                    leases.insert(put.lease);
                }
                Some(Op::RequestTxn(txn)) => {
                    requests.extend(txn.success.iter().chain(&txn.failure));
                }
                _ => (),
            }
        }
        if self.auth.enabled && !leases.is_empty() {
            use prost::Message;
            let mut scan = self
                .applied_state()
                .scan_under(mvcc::CURRENT, &Default::default())
                .await
                .map_err(mvcc::KvError::storage)?;
            while let Some(row) = scan.next().await.map_err(mvcc::KvError::storage)? {
                let kv = crate::proto::mvccpb::KeyValue::decode(row.value)
                    .map_err(mvcc::KvError::storage)?;
                if leases.contains(&kv.lease) {
                    self.auth.range(auth, &kv.key, &[], true)?;
                }
            }
        }
        Ok(())
    }
}
