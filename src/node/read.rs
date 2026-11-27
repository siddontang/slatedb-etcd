use super::*;
pub(super) mod executor;
#[cfg(test)]
pub(super) mod fault_tests;
pub(super) mod table;

impl Node {
    pub(super) fn wait_revision(&mut self, revision: i64, response: Response) {
        self.reads.wait_revision(
            revision,
            response,
            self.state.revision,
            self.raw.raft.leader_id,
        );
    }

    pub(super) fn finish_revision_waiters(&mut self) {
        self.reads
            .finish_revision_waiters(self.state.revision, self.raw.raft.leader_id);
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
        } else {
            self.reads.add(
                request,
                credentials,
                response,
                self.raw.raft.term,
                self.raw.raft.leader_id,
                self.raw.raft.state,
            );
        }
    }

    pub(super) fn issue_reads(&mut self) {
        // Freeze the batch before asking the quorum. Later requests need a new proof.
        if let Some(context) = self.reads.issue() {
            self.raw.read_index(self.reads.context(context));
        }
    }

    pub(super) fn retry_reads(&mut self) {
        self.expire_reads();
        for context in self.reads.retry() {
            self.raw.read_index(self.reads.context(context));
        }
        self.retry_renewals();
    }

    pub(super) fn confirm_reads(&mut self, reads: Vec<raft::ReadState>) {
        for state in reads {
            let Some(id) = self.reads.context_id(state.request_ctx.as_ref()) else {
                continue;
            };
            self.confirm_renewal(id, state.index);
            let raft = &self.raw.raft;
            self.reads
                .confirm(id, state.index, raft.term, raft.leader_id, raft.state);
        }
        self.fence_reads();
    }

    pub(super) fn schedule_reads(&mut self) {
        if !self.early_puts.unblock_reads(self.state.applied) {
            return;
        }
        self.reads.queue_confirmed(self.state.applied);
        while self.read_executor.has_capacity() {
            let Some(queries) = self.reads.next_batch(&self.read_executor) else {
                break;
            };
            self.read_executor
                .submit(self.state.read.clone(), queries, self.header());
        }
    }

    pub(super) fn complete_read(&mut self, output: Output) -> Result<()> {
        self.expire_reads();
        self.finish_output(output)
    }

    pub(super) fn expire_reads(&mut self) {
        let raft = &self.raw.raft;
        let failed = self.reads.expire(raft.term, raft.leader_id, raft.state);
        if failed > 0 {
            self.metrics.failed_read_indexes.inc_by(failed as u64);
        }
    }
}
