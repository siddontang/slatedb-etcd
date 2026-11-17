use super::*;

pub(super) struct PendingRead {
    request: Option<RequestOp>,
    response: Response,
    term: u64,
    started: Instant,
    index: Option<u64>,
}

impl Node {
    pub(super) fn linear_read(&mut self, request: RequestOp, response: Response) {
        if !crate::mvcc::read_only(&request, false) {
            let _ = response.send(Err(self.error("read index request contains a write")));
        } else if self.raw.raft.state != StateRole::Leader {
            let _ = response.send(Err(self.error("not leader")));
        } else if self.reads.len() >= apply::MAX_QUERIES {
            let _ = response.send(Err(self.error("too many pending read indexes")));
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
                    response,
                    term: self.raw.raft.term,
                    started: Instant::now(),
                    index: None,
                },
            );
            self.raw.read_index(id);
        }
    }

    pub(super) fn confirm_reads(&mut self, reads: Vec<raft::ReadState>) {
        for state in reads {
            if let Some(read) = self.reads.get_mut::<[u8]>(state.request_ctx.as_ref()) {
                if read.term == self.raw.raft.term && self.raw.raft.state == StateRole::Leader {
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
                    self.queries.push_back(Query::Linear(request, id.clone()));
                }
            }
        }
    }

    pub(super) fn finish_read(&mut self, id: Vec<u8>, value: Value) {
        self.expire_reads();
        if let Some(read) = self.reads.remove(&id) {
            let _ = read.response.send(Ok(value));
        }
    }

    pub(super) fn expire_reads(&mut self) {
        let error = self.error("read cancelled: leadership changed or request timed out");
        let term = self.raw.raft.term;
        let leader = self.raw.raft.state == StateRole::Leader;
        let expired: Vec<_> = self
            .reads
            .iter()
            .filter(|(_, read)| {
                !leader
                    || read.term != term
                    || read.response.is_closed()
                    || read.started.elapsed() > Duration::from_secs(5)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            if let Some(read) = self.reads.remove(&id) {
                let _ = read.response.send(Err(error.clone()));
            }
        }
    }
}
