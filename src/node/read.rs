use super::*;

pub(super) struct PendingRead {
    request: Option<RequestOp>,
    credentials: Option<crate::auth::Credentials>,
    response: ClientResponse,
    term: u64,
    leader: u64,
    role: StateRole,
    started: Instant,
    index: Option<u64>,
    context: Option<Vec<u8>>,
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
        } else if self.reads.len() >= apply::MAX_QUERIES {
            response.error(self.error("too many pending read indexes"));
        } else {
            self.read_sequence += 1;
            let id = [
                self.read_nonce.as_slice(),
                &self.read_sequence.to_be_bytes(),
            ]
            .concat();
            self.reads.insert(
                id.clone(),
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
                let context = context.get_or_insert_with(|| id.clone());
                read.context = Some(context.clone());
                read.retry_ticks = 2;
            }
        }
        // Freeze the batch before asking the quorum. Later requests need a new proof.
        if let Some(context) = context {
            self.raw.read_index(context);
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
                retry.insert(read.context.as_ref().unwrap().clone());
                read.retry_ticks = 2;
            }
        }
        for context in retry {
            self.raw.read_index(context);
        }
    }

    pub(super) fn confirm_reads(&mut self, reads: Vec<raft::ReadState>) {
        for state in reads {
            for read in self.reads.values_mut() {
                if read.context.as_deref() == Some(state.request_ctx.as_ref())
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
        for (id, read) in &mut self.reads {
            if self.queries.len() >= apply::MAX_QUERIES {
                break;
            }
            if read.index.is_some_and(|index| index <= self.state.applied) {
                if let Some(request) = read.request.take() {
                    self.queries.push_back(Query::Linear(
                        request,
                        read.credentials.take(),
                        id.clone(),
                    ));
                }
            }
        }
    }

    pub(super) fn finish_read(&mut self, id: Vec<u8>, outcome: crate::mvcc::Outcome) -> Result<()> {
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
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            if let Some(read) = self.reads.remove(&id) {
                read.response
                    .error(self.error("read cancelled: leadership changed or request timed out"));
            }
        }
    }
}
