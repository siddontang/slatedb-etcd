use super::*;

/// One KV entry in an apply group.
pub(crate) struct KvEntry<'a> {
    pub index: u64,
    pub request: &'a RequestOp,
    pub credentials: Option<&'a crate::auth::Credentials>,
    pub origin: u64,
}

pub(crate) struct PreparedPuts {
    counts: crate::metrics::Writes,
    first: u64,
    pub last: u64,
    /// The Raft term of the entry at `last`.
    term: u64,
    writes: WriteBatch,
    revision: i64,
    size: crate::batch::Usage,
    alarms: Vec<crate::proto::etcdserverpb::AlarmMember>,
}

impl StateMachine {
    /// Commit KV entries together. Each request keeps its own result and revision.
    #[cfg(test)]
    pub(crate) async fn apply_puts(&mut self, puts: &[KvEntry<'_>]) -> Result<Vec<Outcome>> {
        let (batch, replies) = self.prepare_puts(puts, self.entry_term).await?;
        self.publish_puts(batch).await?;
        Ok(replies)
    }

    /// Evaluate a group of KV entries. `term` is the Raft term of the last entry.
    pub(crate) async fn prepare_puts(
        &self,
        puts: &[KvEntry<'_>],
        term: u64,
    ) -> Result<(PreparedPuts, Vec<Outcome>)> {
        ensure!(!puts.is_empty(), "empty put batch");
        let mut batch = WriteBatch::new();
        let mut revision = self.revision;
        let mut size = self.logical_bytes;
        let mut alarms = self.alarms.clone();
        let mut replies = Vec::with_capacity(puts.len());
        let mut counts = crate::metrics::Writes::default();
        for (offset, put) in puts.iter().enumerate() {
            ensure!(
                put.index == self.applied + offset as u64 + 1,
                "put batch has an index gap"
            );
            let secured = put.credentials.is_some();
            ensure!(
                offset == 0 || !mvcc::needs_scan(put.request, secured),
                "a scan follows earlier group writes"
            );
            crate::failpoint::hit("apply-before-entry");
            let permission = match put.credentials {
                Some(auth) if mvcc::batchable_put(put.request) => self.auth.kv(auth, put.request),
                Some(auth) => self.authorize_kv(auth, put.request).await,
                None => Ok(()),
            };
            let result = if let Err(error) = permission {
                Err(error)
            } else {
                let corrupt = alarms.iter().any(|a| a.alarm == 2);
                let full = crate::maintenance::has_put(put.request)
                    && (alarms.iter().any(|a| a.alarm == 1)
                        || (self.quota > 0 && size.total() >= self.quota));
                if corrupt || full {
                    if full && !alarms.iter().any(|a| a.alarm == 1) {
                        alarms.push(crate::proto::etcdserverpb::AlarmMember {
                            member_id: put.origin,
                            alarm: 1,
                        });
                    }
                    let mut alarm_write = WriteBatch::new();
                    alarm_write.put(crate::maintenance::ALARMS, serde_json::to_vec(&alarms)?);
                    size = alarm_write
                        .logical_size_after(
                            self.applied_state(),
                            &self.key_index,
                            Some(&batch),
                            size,
                        )
                        .await
                        .map_err(crate::mvcc::storage_fault::read_cause)?;
                    batch.extend(alarm_write);
                    Err(if corrupt {
                        mvcc::KvError::new(tonic::Code::DataLoss, "corrupt cluster")
                    } else {
                        mvcc::KvError::new(
                            tonic::Code::ResourceExhausted,
                            "mvcc: database space exceeded",
                        )
                    })
                } else {
                    let view = mvcc::CurrentView {
                        db: self.applied_state(),
                        rows: &self.current,
                        index: &self.key_index,
                    };
                    let prepared = if mvcc::batchable_put(put.request) {
                        mvcc::prepare_put(&view, revision, put.request, &mut batch, &mut size)
                            .await
                            .map(|(reply, next)| {
                                if let Some(
                                    crate::proto::etcdserverpb::request_op::Request::RequestPut(p),
                                ) = &put.request.request
                                {
                                    counts.put(&p.key, &p.value);
                                }
                                (reply, next)
                            })
                    } else {
                        let direct = if mvcc::batchable_txn(put.request) {
                            mvcc::prepare_txn_puts(
                                &view,
                                revision,
                                self.compact_revision,
                                put.request,
                                &self.leases,
                                &mut batch,
                                &mut size,
                                &mut counts,
                            )
                            .await
                        } else {
                            Ok(None)
                        };
                        match direct {
                            Ok(Some(prepared)) => Ok(prepared),
                            Err(error) => Err(error),
                            Ok(None) => match mvcc::prepare_grouped(
                                &view,
                                &batch,
                                revision,
                                self.compact_revision,
                                put.request,
                                &self.leases,
                            )
                            .await
                            {
                                Ok((reply, writes, next, count)) => {
                                    size = writes
                                        .logical_size_after(
                                            self.applied_state(),
                                            &self.key_index,
                                            Some(&batch),
                                            size,
                                        )
                                        .await
                                        .map_err(crate::mvcc::storage_fault::read_cause)?;
                                    counts.merge(count);
                                    batch.extend(writes);
                                    Ok((reply, next))
                                }
                                Err(error) => Err(error),
                            },
                        }
                    };
                    match prepared {
                        Ok((reply, next)) => {
                            revision = next;
                            Ok(reply)
                        }
                        Err(error) => {
                            if let Some(error) = error.unapplied() {
                                return Err(error);
                            }
                            Err(error)
                        }
                    }
                }
            };
            replies.push(Outcome { result });
        }
        if revision != self.revision {
            batch.put(mvcc::REVISION, revision.to_be_bytes());
        }
        let last = puts.last().unwrap().index;
        mark_applied(&mut batch, last, term);
        Ok((
            PreparedPuts {
                counts,
                first: puts[0].index,
                last,
                term,
                writes: batch,
                revision,
                size,
                alarms,
            },
            replies,
        ))
    }

    pub(crate) async fn publish_puts(&mut self, prepared: PreparedPuts) -> Result<()> {
        let PreparedPuts {
            counts,
            first,
            last,
            term,
            writes,
            revision,
            size,
            alarms,
        } = prepared;
        ensure!(first == self.applied + 1, "prepared puts lost their base");
        if first < last {
            crate::failpoint::hit("apply-batch-before-write");
        }
        let size = self.store_batch(writes, last, size).await?;
        if first < last {
            crate::failpoint::hit("apply-batch-after-write");
        }
        self.applied = last;
        self.applied_term = term;
        self.revision = revision;
        self.logical_bytes = size;
        counts.publish(&self.metrics);
        self.alarms = alarms;
        for _ in first..=last {
            crate::failpoint::hit("apply-after-entry");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::etcdserverpb::request_op::Request as Op;
    use crate::proto::etcdserverpb::PutRequest;

    fn request(key: &[u8], value: &[u8]) -> RequestOp {
        RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                prev_kv: true,
                ..Default::default()
            })),
        }
    }

    async fn rows(state: &StateMachine) -> Vec<(bytes::Bytes, bytes::Bytes)> {
        let mut scan = state.db().scan_prefix(b"", ..).await.unwrap();
        let mut rows = vec![];
        while let Some(row) = scan.next().await.unwrap() {
            rows.push((row.key, row.value));
        }
        rows
    }

    #[tokio::test]
    async fn grouped_puts_match_individual_revisions_watch_errors_quota_and_reopen() {
        for quota in [0, 1, 700, 10000] {
            let left = tempfile::tempdir().unwrap();
            let right = tempfile::tempdir().unwrap();
            let mut grouped = StateMachine::open(left.path()).await.unwrap();
            let mut separate = StateMachine::open(right.path()).await.unwrap();
            grouped.quota = quota;
            separate.quota = quota;
            let mut requests = [
                request(b"a", b"old"),
                request(b"", b"invalid"),
                request(b"a", b"new"),
                request(b"b", b"value"),
                request(b"a", b"last"),
                request(&[0, 255], &[0; 4096]),
                request(&[0, 255], b""),
            ];
            for (i, op) in requests.iter_mut().enumerate() {
                let Some(Op::RequestPut(put)) = op.request.as_mut() else {
                    unreachable!()
                };
                put.prev_kv = i % 2 == 1;
            }
            let puts: Vec<_> = requests
                .iter()
                .enumerate()
                .map(|(i, request)| KvEntry {
                    index: i as u64 + 1,
                    request,
                    origin: 9,
                    credentials: None,
                })
                .collect();
            let replies = grouped.apply_puts(&puts).await.unwrap();
            for (put, reply) in puts.iter().zip(replies) {
                let expected = separate
                    .apply_etcd_quota(put.index, put.request, put.origin)
                    .await
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(reply).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
            }
            assert_eq!(grouped.write_batches, 1);
            assert_eq!(separate.write_batches, requests.len() as u64);
            assert_eq!(rows(&grouped).await, rows(&separate).await);
            assert_eq!(grouped.logical_bytes, separate.logical_bytes);
            assert_eq!(
                grouped.logical_bytes,
                maintenance::count_logical_usage(grouped.db().as_ref())
                    .await
                    .unwrap()
            );
            assert_eq!(
                grouped.db().snapshot().unwrap().seq(),
                requests.len() as u64
            );
            grouped.db().flush().await.unwrap();
            grouped.close().await.unwrap();
            drop(grouped);
            let reopened = StateMachine::open(left.path()).await.unwrap();
            assert_eq!(rows(&reopened).await, rows(&separate).await);
            assert_eq!(reopened.applied, requests.len() as u64);
            reopened.close().await.unwrap();
            separate.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn failed_batch_write_does_not_publish_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        state.close().await.unwrap();
        let request = request(b"a", b"value");
        assert!(state
            .apply_puts(&[KvEntry {
                index: 1,
                request: &request,
                origin: 1,
                credentials: None
            }])
            .await
            .is_err());
        assert_eq!(
            (
                state.applied,
                state.revision,
                state.logical_bytes.total(),
                state.write_batches
            ),
            (0, 1, 0, 0)
        );
    }
}
