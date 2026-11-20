use super::*;
pub(super) mod executor;

pub(super) struct PendingRead {
    request: Option<RequestOp>,
    credentials: Option<crate::auth::Credentials>,
    response: ClientResponse,
    term: u64,
    leader: u64,
    role: StateRole,
    started: Instant,
    index: Option<u64>,
    context: Option<u64>,
    retry_ticks: u8,
}

impl Node {
    pub(super) fn wait_revision(&mut self, revision: i64, response: Response) {
        self.finish_revision_waiters();
        if self.state.revision >= revision {
            let _ = response.send(Ok(Value::Null));
        } else if self.revision_waiters.len() >= apply::MAX_QUERIES {
            let _ = response.send(Err(self.error("too many pending apply barriers")));
        } else {
            self.revision_waiters.push_back(RevisionWaiter {
                revision,
                response,
                started: Instant::now(),
            });
        }
    }

    pub(super) fn finish_revision_waiters(&mut self) {
        for _ in 0..self.revision_waiters.len() {
            let waiter = self.revision_waiters.pop_front().unwrap();
            if waiter.response.is_closed() {
                continue;
            }
            if waiter.started.elapsed() > Duration::from_secs(5) {
                let _ = waiter
                    .response
                    .send(Err(self.error("local apply timed out")));
            } else if self.state.revision >= waiter.revision {
                let _ = waiter.response.send(Ok(Value::Null));
            } else {
                self.revision_waiters.push_back(waiter);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn linear_read(&mut self, request: RequestOp, response: Response) {
        self.linear_read_auth(request, None, response);
    }

    pub(super) fn linear_read_auth(
        &mut self,
        request: RequestOp,
        credentials: Option<crate::auth::Credentials>,
        response: impl Into<ClientResponse>,
    ) {
        let response = response.into();
        if !crate::mvcc::read_only(&request, false) {
            response.error(self.error("read index request contains a write"));
        } else if self.raw.raft.leader_id == 0 {
            response.error(self.error("not leader"));
        } else if self.reads.len() >= MAX_PENDING_READS {
            response.error(self.error("too many pending read indexes"));
        } else {
            let Some(id) = self.read_sequence.checked_add(1) else {
                response.error(self.error("read sequence exhausted"));
                return;
            };
            self.read_sequence = id;
            self.reads.insert(
                id,
                PendingRead {
                    request: Some(request),
                    credentials,
                    response,
                    term: self.raw.raft.term,
                    leader: self.raw.raft.leader_id,
                    role: self.raw.raft.state,
                    started: Instant::now(),
                    index: None,
                    context: None,
                    retry_ticks: 2,
                },
            );
        }
    }

    pub(super) fn read_context(&self, id: u64) -> Vec<u8> {
        [self.read_nonce.as_slice(), &id.to_be_bytes()].concat()
    }

    pub(super) fn issue_reads(&mut self) {
        if self
            .reads
            .values()
            .any(|read| read.context.is_some() && read.index.is_none())
        {
            return;
        }
        let mut context = None;
        for (id, read) in &mut self.reads {
            if read.context.is_none() {
                let context = *context.get_or_insert(*id);
                read.context = Some(context);
                read.retry_ticks = 2;
            }
        }
        // Freeze the batch before asking the quorum. Later requests need a new proof.
        if let Some(context) = context {
            self.raw.read_index(self.read_context(context));
        }
    }

    pub(super) fn retry_reads(&mut self) {
        self.expire_reads();
        let mut retry = BTreeSet::new();
        for read in self.reads.values_mut() {
            if read.index.is_some() || read.context.is_none() {
                continue;
            }
            read.retry_ticks -= 1;
            if read.retry_ticks == 0 {
                // Raft can drop reads before the new leader commits its first entry.
                retry.insert(read.context.unwrap());
                read.retry_ticks = 2;
            }
        }
        for context in retry {
            self.raw.read_index(self.read_context(context));
        }
        self.retry_renewals();
    }

    pub(super) fn confirm_reads(&mut self, reads: Vec<raft::ReadState>) {
        for state in reads {
            let context: &[u8] = state.request_ctx.as_ref();
            if context.len() != 24 || context[..16] != self.read_nonce {
                continue;
            }
            let id = u64::from_be_bytes(context[16..].try_into().unwrap());
            self.confirm_renewal(id, state.index);
            for read in self.reads.values_mut() {
                if read.context == Some(id)
                    && read.index.is_none()
                    && read.term == self.raw.raft.term
                    && read.leader == self.raw.raft.leader_id
                    && read.role == self.raw.raft.state
                {
                    read.index = Some(state.index);
                }
            }
        }
    }

    pub(super) fn schedule_reads(&mut self) {
        if self.state.applied < self.quorum_reply_index {
            return;
        }
        for (id, read) in &mut self.reads {
            if self.read_queries.len() >= apply::MAX_QUERIES {
                break;
            }
            if read.index.is_some_and(|index| index <= self.state.applied) {
                if let Some(request) = read.request.take() {
                    self.read_queries.push_back(Query::Linear(
                        request,
                        read.credentials.take(),
                        *id,
                    ));
                }
            }
        }
        while self.read_executor.has_capacity() {
            // Point reads can pass queued wide reads. Wide reads start in queue order,
            // so shared scans cannot starve an older exclusive read.
            let mut first_wide = true;
            let Some(position) = self.read_queries.iter().position(|query| {
                if query.is_point_read() {
                    return self.read_executor.can_submit(query);
                }
                std::mem::take(&mut first_wide) && self.read_executor.can_submit(query)
            }) else {
                break;
            };
            let first = self.read_queries.remove(position).unwrap();
            let point = first.is_point_read();
            let mut queries = vec![first];
            while queries.len() < apply::READ_BATCH
                && self.read_queries.get(position).is_some_and(|next| {
                    if point {
                        next.is_point_read()
                    } else {
                        queries[0].same_linear_read(next)
                    }
                })
            {
                queries.push(self.read_queries.remove(position).unwrap());
            }
            self.read_executor
                .submit(self.state.read.clone(), queries, self.header());
        }
    }

    pub(super) fn complete_read(&mut self, output: Output) -> Result<()> {
        self.expire_reads();
        self.finish_output(output)
    }

    pub(super) fn finish_shared_reads(
        &mut self,
        ids: Vec<u64>,
        outcome: std::sync::Arc<crate::mvcc::Outcome>,
    ) -> Result<()> {
        for id in ids {
            if let Some(read) = self.reads.remove(&id) {
                read.response.shared_kv(outcome.clone())?;
            }
        }
        Ok(())
    }

    pub(super) fn finish_read(&mut self, id: u64, outcome: crate::mvcc::Outcome) -> Result<()> {
        if let Some(read) = self.reads.remove(&id) {
            read.response.kv(outcome)?;
        }
        Ok(())
    }

    pub(super) fn expire_reads(&mut self) {
        let now = Instant::now();
        let term = self.raw.raft.term;
        let leader = self.raw.raft.leader_id;
        let role = self.raw.raft.state;
        let expired: Vec<_> = self
            .reads
            .iter()
            .filter(|(_, read)| {
                leader == 0
                    || read.leader != leader
                    || read.role != role
                    || read.term != term
                    || read.response.is_closed()
                    || now.duration_since(read.started) > Duration::from_secs(5)
            })
            .map(|(id, _)| *id)
            .collect();
        let mut failed = BTreeSet::new();
        for id in expired {
            if let Some(read) = self.reads.remove(&id) {
                if read.index.is_none() && !read.response.is_closed() {
                    failed.extend(read.context);
                }
                read.response
                    .error(self.error("read cancelled: leadership changed or request timed out"));
            }
        }
        if !failed.is_empty() {
            self.metrics.failed_read_indexes.inc_by(failed.len() as u64);
        }
    }
}
