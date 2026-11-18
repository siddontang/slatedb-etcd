use super::*;

/// Pin data and authorization metadata at one applied position.
pub(crate) struct ReadView {
    metrics: Arc<crate::metrics::StateMeters>,
    pub snapshot: Arc<slatedb::DbSnapshot>,
    pub revision: i64,
    pub compact_revision: i64,
    pub auth: Arc<crate::auth::Store>,
    pub leases: Arc<std::collections::BTreeMap<i64, crate::lease::LeaseRecord>>,
}

impl StateMachine {
    pub(crate) fn read_view(&self) -> Result<Arc<ReadView>> {
        let snapshot = self.db.snapshot()?;
        ensure!(
            snapshot.seq() == self.applied,
            "read view does not match applied state"
        );
        Ok(Arc::new(ReadView {
            metrics: self.metrics.clone(),
            snapshot,
            revision: self.revision,
            compact_revision: self.compact_revision,
            auth: self.auth.clone(),
            leases: self.leases.clone(),
        }))
    }
}

impl ReadView {
    pub async fn read(
        &self,
        request: &RequestOp,
        credentials: Option<&crate::auth::Credentials>,
    ) -> Result<Outcome> {
        if let Some(credentials) = credentials {
            if let Err(error) = self.auth.kv(credentials, request) {
                return Ok(Outcome { result: Err(error) });
            }
        }
        let start = matches!(
            request.request,
            Some(crate::proto::etcdserverpb::request_op::Request::RequestRange(_))
        )
        .then(std::time::Instant::now);
        let result = mvcc::read_snapshot(
            &self.snapshot,
            self.revision,
            self.compact_revision,
            request,
            &self.leases,
        )
        .await;
        if let Some(start) = start {
            let histogram = if result.is_ok() {
                &self.metrics.range_ok
            } else {
                &self.metrics.range_error
            };
            histogram.observe(start.elapsed().as_secs_f64());
        }
        if let Err(error) = &result {
            ensure!(
                error.code != tonic::Code::Internal as i32,
                "{}",
                error.message
            );
        }
        Ok(Outcome { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::etcdserverpb::request_op::Request as Op;
    use crate::proto::etcdserverpb::response_op::Response as Resp;
    use crate::proto::etcdserverpb::*;

    #[tokio::test]
    async fn pinned_read_survives_snapshot_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        let mut source = StateMachine::open(source_dir.path()).await.unwrap();
        let put = |value: &[u8]| RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: b"key".to_vec(),
                value: value.to_vec(),
                ..Default::default()
            })),
        };
        state.apply_etcd(1, &put(b"old")).await.unwrap();
        let old = state.read_view().unwrap();
        source.apply_etcd(1, &put(b"source")).await.unwrap();
        source.apply_etcd(2, &put(b"new")).await.unwrap();
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 2;
        snapshot.data = source.snapshot_job().unwrap().await.unwrap().into();
        state.install_snapshot(&snapshot).await.unwrap();
        let current = state.read_view().unwrap();
        let request = RequestOp {
            request: Some(Op::RequestRange(RangeRequest {
                key: b"key".to_vec(),
                ..Default::default()
            })),
        };
        for (view, revision, value) in [(old, 2, b"old"), (current, 3, b"new")] {
            let Some(Resp::ResponseRange(reply)) = view
                .read(&request, None)
                .await
                .unwrap()
                .result
                .unwrap()
                .response
            else {
                panic!("expected range");
            };
            assert_eq!(reply.header.unwrap().revision, revision);
            assert_eq!(reply.kvs[0].value, value);
        }
        state.close().await.unwrap();
        source.close().await.unwrap();
    }

    #[tokio::test]
    async fn pinned_reads_keep_data_revision_compaction_and_auth_together() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        let put = |value: &[u8]| RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: b"a".to_vec(),
                value: value.to_vec(),
                ..Default::default()
            })),
        };
        state
            .apply_etcd(1, &put(b"old"))
            .await
            .unwrap()
            .result
            .unwrap();
        let old = state.read_view().unwrap();
        state
            .apply_etcd(2, &put(b"new"))
            .await
            .unwrap()
            .result
            .unwrap();
        state
            .apply_compaction(
                3,
                &CompactionRequest {
                    revision: 3,
                    physical: true,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        let credentials = crate::auth::Credentials::default();
        for command in [
            crate::auth::Command::UserAdd(AuthUserAddRequest {
                name: "root".into(),
                options: Some(crate::proto::authpb::UserAddOptions { no_password: true }),
                ..Default::default()
            }),
            crate::auth::Command::UserGrantRole(AuthUserGrantRoleRequest {
                user: "root".into(),
                role: "root".into(),
            }),
            crate::auth::Command::Enable(AuthEnableRequest {}),
        ] {
            state
                .apply_auth(state.applied + 1, &credentials, &command)
                .await
                .unwrap()
                .result
                .unwrap();
        }
        let current = state.read_view().unwrap();
        for revision in [0, 2] {
            let read = RequestOp {
                request: Some(Op::RequestRange(RangeRequest {
                    key: b"a".to_vec(),
                    revision,
                    ..Default::default()
                })),
            };
            let Some(Resp::ResponseRange(reply)) = old
                .read(&read, Some(&credentials))
                .await
                .unwrap()
                .result
                .unwrap()
                .response
            else {
                panic!("expected range");
            };
            assert_eq!(reply.header.unwrap().revision, 2);
            assert_eq!(reply.kvs[0].value, b"old");
            assert!(current
                .read(&read, Some(&credentials))
                .await
                .unwrap()
                .result
                .is_err());
            if revision == 2 {
                assert!(current.read(&read, None).await.unwrap().result.is_err());
            }
        }
        drop(old);
        drop(current);
        state.close().await.unwrap();
    }
}
