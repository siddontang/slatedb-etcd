use super::*;

/// Pin data and authorization metadata at one applied position.
pub(crate) struct ReadView {
    metrics: Arc<crate::metrics::StateMeters>,
    pub snapshot: Arc<slatedb::DbSnapshot>,
    pub revision: i64,
    pub compact_revision: i64,
    pub auth: Arc<crate::auth::Store>,
    pub leases: Arc<std::collections::BTreeMap<i64, crate::lease::LeaseRecord>>,
    /// Current keys at the snapshot's sequence number, for Range counts.
    keys: key_index::KeyPin,
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
            keys: self.key_index.pin(self.applied),
        }))
    }
}

impl ReadView {
    pub async fn read(
        &self,
        request: &RequestOp,
        credentials: Option<&crate::auth::Credentials>,
    ) -> Result<Outcome> {
        let outcome = self.read_within(request, credentials, None).await?;
        Ok(outcome.expect("an unbounded read always completes"))
    }

    /// Read with an optional budget for materialized Range rows.
    /// `Ok(None)` means the rows exceed `budget`; the same read without a budget returns the result.
    pub async fn read_within(
        &self,
        request: &RequestOp,
        credentials: Option<&crate::auth::Credentials>,
        budget: Option<usize>,
    ) -> Result<Option<Outcome>> {
        if let Some(credentials) = credentials {
            if let Err(error) = self.auth.kv(credentials, request) {
                return Ok(Some(Outcome { result: Err(error) }));
            }
        }
        let start = matches!(
            request.request,
            Some(crate::proto::etcdserverpb::request_op::Request::RequestRange(_))
        )
        .then(std::time::Instant::now);
        let result = match budget {
            Some(budget) => match mvcc::read_snapshot_within(
                &self.snapshot,
                self.revision,
                self.compact_revision,
                request,
                &self.leases,
                &self.keys,
                budget,
            )
            .await
            {
                Some(result) => result,
                None => return Ok(None),
            },
            None => {
                mvcc::read_snapshot(
                    &self.snapshot,
                    self.revision,
                    self.compact_revision,
                    request,
                    &self.leases,
                    &self.keys,
                )
                .await
            }
        };
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
        Ok(Some(Outcome { result }))
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

    #[tokio::test]
    async fn pinned_range_counts_follow_their_snapshot_after_later_writes() {
        use crate::proto::etcdserverpb::{DeleteRangeRequest, TxnRequest};
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        let put = |key: &[u8]| RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: key.to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            })),
        };
        let delete = |key: &[u8], end: &[u8]| RequestOp {
            request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
                key: key.to_vec(),
                range_end: end.to_vec(),
                ..Default::default()
            })),
        };
        let mut index = 0;
        let mut apply = async |state: &mut StateMachine, request: RequestOp| {
            index += 1;
            state
                .apply_etcd(index, &request)
                .await
                .unwrap()
                .result
                .unwrap();
        };
        for key in [b"a", b"b", b"c", b"d"] {
            apply(&mut state, put(key)).await;
        }
        let first = state.read_view().unwrap();
        apply(&mut state, put(b"b")).await;
        apply(&mut state, delete(b"b", b"d")).await;
        apply(&mut state, put(b"e")).await;
        let second = state.read_view().unwrap();
        apply(&mut state, put(b"c")).await;
        apply(&mut state, delete(b"a", b"")).await;
        let third = state.read_view().unwrap();
        // (key, end, limit) -> (count, rows, more) at each view.
        // (count, rows, more) at the first, second, and third view.
        type Expected = [(i64, usize, bool); 3];
        let cases: [(&[u8], &[u8], i64, Expected); 5] = [
            (&[0], &[0], 2, [(4, 2, true), (3, 2, true), (3, 2, true)]),
            (b"b", &[0], 0, [(3, 3, false), (2, 2, false), (3, 3, false)]),
            (b"a", b"d", 1, [(3, 1, true), (1, 1, false), (1, 1, false)]),
            (b"c", b"e", 5, [(2, 2, false), (1, 1, false), (2, 2, false)]),
            (b"f", &[0], 1, [(0, 0, false), (0, 0, false), (0, 0, false)]),
        ];
        for (key, end, limit, expected) in cases {
            for count_only in [false, true] {
                let range = RequestOp {
                    request: Some(Op::RequestRange(RangeRequest {
                        key: key.to_vec(),
                        range_end: end.to_vec(),
                        limit,
                        count_only,
                        ..Default::default()
                    })),
                };
                let txn = RequestOp {
                    request: Some(Op::RequestTxn(TxnRequest {
                        success: vec![range.clone()],
                        ..Default::default()
                    })),
                };
                for (view, (count, rows, more)) in
                    [&first, &second, &third].into_iter().zip(expected)
                {
                    for request in [&range, &txn] {
                        let mut response = view.read(request, None).await.unwrap().result.unwrap();
                        if let Some(Resp::ResponseTxn(txn)) = response.response {
                            response = txn.responses.into_iter().next().unwrap();
                        }
                        let Some(Resp::ResponseRange(reply)) = response.response else {
                            panic!("expected range");
                        };
                        let rows = if count_only { 0 } else { rows };
                        let more = more && !count_only;
                        assert_eq!(
                            (reply.count, reply.kvs.len(), reply.more),
                            (count, rows, more),
                            "{key:?}..{end:?} limit {limit} count_only {count_only}"
                        );
                    }
                }
            }
        }
        drop((first, second, third));
        apply(&mut state, put(b"z")).await;
        assert!(state.key_index.matches(&state.db).await.unwrap());
        state.close().await.unwrap();
    }
}
