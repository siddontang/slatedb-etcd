use super::*;

/// Pin data and authorization metadata at one applied position.
pub(crate) struct ReadView {
    metrics: Arc<crate::metrics::StateMeters>,
    pub snapshot: Arc<slatedb::DbSnapshot>,
    rows: Arc<super::rows::RowCache>,
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
            rows: self.rows.clone(),
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
                &self.rows,
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
                    &self.rows,
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
    async fn shared_rows_match_each_pinned_view() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        let put = |value: &[u8]| RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: b"key".to_vec(),
                value: value.to_vec(),
                ..Default::default()
            })),
        };
        let get = RequestOp {
            request: Some(Op::RequestRange(RangeRequest {
                key: b"key".to_vec(),
                ..Default::default()
            })),
        };
        let value = |view: Arc<ReadView>| {
            let get = get.clone();
            async move {
                let Some(Resp::ResponseRange(reply)) = view
                    .read(&get, None)
                    .await
                    .unwrap()
                    .result
                    .unwrap()
                    .response
                else {
                    panic!("expected range");
                };
                reply.kvs.first().map(|kv| kv.value.clone())
            }
        };
        state.apply_etcd(1, &put(b"v1")).await.unwrap();
        let first = state.read_view().unwrap();
        // Fill the shared row, then hit it.
        for _ in 0..2 {
            assert_eq!(value(first.clone()).await.unwrap(), b"v1");
        }
        assert!(state.rows.get(b"key", state.applied, |_| ()).is_some());
        state.apply_etcd(2, &put(b"v2")).await.unwrap();
        assert!(state.rows.get(b"key", state.applied, |_| ()).is_none());
        let second = state.read_view().unwrap();
        // The older view cannot install its row after the newer write.
        assert_eq!(value(first.clone()).await.unwrap(), b"v1");
        assert!(state.rows.get(b"key", state.applied, |_| ()).is_none());
        for _ in 0..2 {
            assert_eq!(value(second.clone()).await.unwrap(), b"v2");
        }
        assert!(state.rows.get(b"key", state.applied, |_| ()).is_some());
        // A cached newer row does not answer the older view.
        assert_eq!(value(first.clone()).await.unwrap(), b"v1");
        let delete = RequestOp {
            request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
                key: b"key".to_vec(),
                ..Default::default()
            })),
        };
        state.apply_etcd(3, &delete).await.unwrap();
        assert!(value(state.read_view().unwrap()).await.is_none());
        assert_eq!(value(second).await.unwrap(), b"v2");
        state.close().await.unwrap();
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

    /// Range answers of one pinned view against the key set it should hold.
    async fn check_view(
        view: &ReadView,
        model: &std::collections::BTreeSet<Vec<u8>>,
        random: &mut impl FnMut(u64) -> u64,
    ) {
        use crate::proto::etcdserverpb::TxnRequest;
        for _ in 0..3 {
            let a = format!("k{:02}", random(26)).into_bytes();
            let (key, end) = match random(4) {
                0 => (vec![0], vec![0]),
                1 => (a, vec![0]),
                _ => (a, format!("k{:02}", random(26)).into_bytes()),
            };
            let limit = random(4) as i64;
            let count_only = random(5) == 0;
            let keys_only = random(3) == 0;
            let expected: Vec<&Vec<u8>> = model
                .iter()
                .filter(|k| crate::mvcc::contains(k, &key, &end))
                .collect();
            let range = RequestOp {
                request: Some(Op::RequestRange(RangeRequest {
                    key: key.clone(),
                    range_end: end.clone(),
                    limit,
                    count_only,
                    keys_only,
                    ..Default::default()
                })),
            };
            let request = if random(2) == 0 {
                range
            } else {
                RequestOp {
                    request: Some(Op::RequestTxn(TxnRequest {
                        success: vec![range],
                        ..Default::default()
                    })),
                }
            };
            let mut response = view.read(&request, None).await.unwrap().result.unwrap();
            if let Some(Resp::ResponseTxn(txn)) = response.response {
                response = txn.responses.into_iter().next().unwrap();
            }
            let Some(Resp::ResponseRange(reply)) = response.response else {
                panic!("expected range");
            };
            let count = expected.len() as i64;
            let rows = if count_only {
                0
            } else if limit > 0 {
                count.min(limit)
            } else {
                count
            };
            let context = format!("{key:?}..{end:?} limit {limit} count_only {count_only}");
            assert_eq!(reply.count, count, "{context}");
            assert_eq!(reply.kvs.len() as i64, rows, "{context}");
            assert_eq!(
                reply.more,
                !count_only && limit > 0 && count > limit,
                "{context}"
            );
            assert_eq!(reply.header.unwrap().revision, view.revision, "{context}");
            for (kv, key) in reply.kvs.iter().zip(&expected) {
                assert_eq!(&&kv.key, key, "{context}");
                assert_eq!(kv.value.is_empty(), keys_only, "{context}");
            }
        }
    }

    /// Random writes, lease revocation and expiry, compaction, snapshot installation, and restart,
    /// with read views pinned at random points. Every Range count must match the key set of its
    /// view. Debug builds also check each count against a full scan of the same snapshot.
    #[tokio::test]
    async fn random_pinned_range_counts_match_their_snapshots() {
        use crate::lease::LeaseCommand;
        use crate::proto::etcdserverpb::{DeleteRangeRequest, TxnRequest};
        use std::collections::{BTreeMap, BTreeSet};
        for seed in 1..=3u64 {
            let mut x = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
            let mut random = move |n: u64| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x % n
            };
            let dir = tempfile::tempdir().unwrap();
            let mirror_dir = tempfile::tempdir().unwrap();
            let mut state = StateMachine::open(dir.path()).await.unwrap();
            let mut mirror = StateMachine::open(mirror_dir.path()).await.unwrap();
            let mut model = BTreeSet::new();
            let mut leases: BTreeMap<i64, BTreeSet<Vec<u8>>> = BTreeMap::new();
            let mut next_lease = 1;
            let mut views: Vec<(Arc<ReadView>, BTreeSet<Vec<u8>>)> = vec![];
            let mut mirror_views: Vec<(Arc<ReadView>, BTreeSet<Vec<u8>>)> = vec![];
            for _ in 0..150 {
                let key =
                    |random: &mut dyn FnMut(u64) -> u64| format!("k{:02}", random(24)).into_bytes();
                let index = state.applied + 1;
                match random(16) {
                    0..=5 => {
                        let k = key(&mut random);
                        let lease = match leases.keys().copied().collect::<Vec<_>>() {
                            live if !live.is_empty() && random(2) == 0 => {
                                live[random(live.len() as u64) as usize]
                            }
                            _ => 0,
                        };
                        let put = RequestOp {
                            request: Some(Op::RequestPut(PutRequest {
                                key: k.clone(),
                                value: b"v".to_vec(),
                                lease,
                                ..Default::default()
                            })),
                        };
                        state.apply_etcd(index, &put).await.unwrap().result.unwrap();
                        for keys in leases.values_mut() {
                            keys.remove(&k);
                        }
                        if lease != 0 {
                            leases.get_mut(&lease).unwrap().insert(k.clone());
                        }
                        model.insert(k);
                    }
                    6 | 7 => {
                        let (a, b) = (key(&mut random), key(&mut random));
                        let end = if random(4) == 0 { vec![] } else { b };
                        let delete = RequestOp {
                            request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
                                key: a.clone(),
                                range_end: end.clone(),
                                ..Default::default()
                            })),
                        };
                        state
                            .apply_etcd(index, &delete)
                            .await
                            .unwrap()
                            .result
                            .unwrap();
                        model.retain(|k| !crate::mvcc::contains(k, &a, &end));
                        for keys in leases.values_mut() {
                            keys.retain(|k| !crate::mvcc::contains(k, &a, &end));
                        }
                    }
                    8 => {
                        // One transaction creates a key and deletes another.
                        let (a, b) = (key(&mut random), key(&mut random));
                        let txn = RequestOp {
                            request: Some(Op::RequestTxn(TxnRequest {
                                success: vec![
                                    RequestOp {
                                        request: Some(Op::RequestPut(PutRequest {
                                            key: a.clone(),
                                            value: b"t".to_vec(),
                                            ..Default::default()
                                        })),
                                    },
                                    RequestOp {
                                        request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
                                            key: b.clone(),
                                            ..Default::default()
                                        })),
                                    },
                                ],
                                ..Default::default()
                            })),
                        };
                        // Etcd rejects a put and a delete of the same key in one transaction.
                        let outcome = state.apply_etcd(index, &txn).await.unwrap();
                        if outcome.result.is_ok() {
                            model.insert(a.clone());
                            model.remove(&b);
                            for keys in leases.values_mut() {
                                keys.remove(&a);
                                keys.remove(&b);
                            }
                        }
                    }
                    9 => {
                        let command = LeaseCommand::Grant {
                            id: next_lease,
                            ttl: 60,
                        };
                        state
                            .apply_lease(index, &command)
                            .await
                            .unwrap()
                            .result
                            .unwrap();
                        leases.insert(next_lease, BTreeSet::new());
                        next_lease += 1;
                    }
                    10 => {
                        let Some(&id) = leases.keys().next() else {
                            continue;
                        };
                        let command = if random(2) == 0 {
                            LeaseCommand::Revoke { id }
                        } else {
                            let generation = state.leases[&id].generation;
                            LeaseCommand::Expire { id, generation }
                        };
                        state
                            .apply_lease(index, &command)
                            .await
                            .unwrap()
                            .result
                            .unwrap();
                        for k in leases.remove(&id).unwrap() {
                            model.remove(&k);
                        }
                    }
                    11 if state.revision > state.compact_revision.max(1) => {
                        let request = CompactionRequest {
                            revision: state.revision,
                            physical: false,
                        };
                        state
                            .apply_compaction(index, &request)
                            .await
                            .unwrap()
                            .result
                            .unwrap();
                    }
                    12 => {
                        // The mirror installs a snapshot of the current state over its old one.
                        let mut snapshot = Snapshot::default();
                        snapshot.mut_metadata().index = state.applied;
                        snapshot.data = state.snapshot_job().unwrap().await.unwrap().into();
                        if state.applied > mirror.applied {
                            mirror.install_snapshot(&snapshot).await.unwrap();
                            mirror_views.push((mirror.read_view().unwrap(), model.clone()));
                        }
                    }
                    13 if random(4) == 0 => {
                        // Without a SlateDB WAL, unflushed rows return through Raft replay. Flush
                        // so the reopened state holds every applied row.
                        views.clear();
                        state.db.flush().await.unwrap();
                        state.close().await.unwrap();
                        drop(state);
                        state = StateMachine::open(dir.path()).await.unwrap();
                    }
                    _ => {
                        views.push((state.read_view().unwrap(), model.clone()));
                        if views.len() > 4 {
                            views.remove(random(views.len() as u64) as usize);
                        }
                    }
                }
                for (view, expected) in views.iter().chain(&mirror_views) {
                    check_view(view, expected, &mut random).await;
                }
                check_view(&state.read_view().unwrap(), &model, &mut random).await;
                if mirror_views.len() > 3 {
                    mirror_views.remove(0);
                }
            }
            views.clear();
            mirror_views.clear();
            // One more batch folds the remaining history.
            let index = state.applied + 1;
            state
                .apply_etcd(
                    index,
                    &RequestOp {
                        request: Some(Op::RequestPut(PutRequest {
                            key: b"k00".to_vec(),
                            ..Default::default()
                        })),
                    },
                )
                .await
                .unwrap()
                .result
                .unwrap();
            assert!(state.key_index.matches(&state.db).await.unwrap());
            assert!(mirror.key_index.matches(&mirror.db).await.unwrap());
            state.close().await.unwrap();
            mirror.close().await.unwrap();
        }
    }
}
