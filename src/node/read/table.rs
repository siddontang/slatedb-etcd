//! Keep the linearizable reads that wait for their ReadIndex proof, the reads that wait for the
//! executor, and the local waits for an applied revision.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

use anyhow::Result;
use raft::StateRole;
use serde_json::Value;

use super::super::apply::{self, Query};
use super::super::response::ClientResponse;
use super::super::Response;
use super::executor::Executor;
use crate::proto::etcdserverpb::RequestOp;
use crate::ApiError;

/// ReadIndex reads that may wait in the Raft owner's table. One quorum round confirms
/// every read in a batch, so a larger table lets one round cover more clients.
pub(crate) const MAX_PENDING_READS: usize = 256;

struct PendingRead {
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

struct RevisionWaiter {
    revision: i64,
    response: Response,
    started: Instant,
}

/// The read side of one member.
///
/// A linearizable read waits here for its ReadIndex proof, then for the local apply to reach
/// the proven index. Then it moves to the queue of the read executor, with the serializable
/// reads. A read batch has the id of its first read as its ReadIndex context. Lease renewals
/// take their contexts from the same sequence. `Node` gives the Raft state to each method.
pub(in crate::node) struct ReadIndexTable {
    /// Linearizable reads by id, in arrival order.
    reads: BTreeMap<u64, PendingRead>,
    /// Reads that wait for the executor.
    queries: VecDeque<Query>,
    /// Local waits until the applied revision reaches a revision.
    revision_waiters: VecDeque<RevisionWaiter>,
    /// The process nonce in each ReadIndex context. A context of an earlier process does not
    /// match.
    nonce: [u8; 16],
    /// The last read or renewal id.
    sequence: u64,
}

fn error(message: &str, leader_id: u64) -> ApiError {
    ApiError {
        error: message.to_owned(),
        leader_id,
    }
}

impl ReadIndexTable {
    pub(in crate::node) fn new() -> Self {
        Self {
            reads: BTreeMap::new(),
            queries: VecDeque::new(),
            revision_waiters: VecDeque::new(),
            nonce: *uuid::Uuid::new_v4().as_bytes(),
            sequence: 0,
        }
    }

    /// The number of linearizable reads in the table.
    pub(in crate::node) fn pending(&self) -> usize {
        self.reads.len()
    }

    /// The number of reads that wait for the executor.
    pub(in crate::node) fn queued(&self) -> usize {
        self.queries.len()
    }

    /// The number of local waits for a revision.
    pub(in crate::node) fn revision_waiters(&self) -> usize {
        self.revision_waiters.len()
    }

    /// Take the next read or renewal id. `None` if the sequence is exhausted.
    pub(in crate::node) fn next_id(&mut self) -> Option<u64> {
        let id = self.sequence.checked_add(1)?;
        self.sequence = id;
        Some(id)
    }

    /// The ReadIndex context of the read or renewal `id`.
    pub(in crate::node) fn context(&self, id: u64) -> Vec<u8> {
        [self.nonce.as_slice(), &id.to_be_bytes()].concat()
    }

    /// The id in a ReadIndex context of this process.
    pub(in crate::node) fn context_id(&self, context: &[u8]) -> Option<u64> {
        if context.len() != 24 || context[..16] != self.nonce {
            return None;
        }
        Some(u64::from_be_bytes(context[16..].try_into().unwrap()))
    }

    /// Answer the request when the applied revision reaches `revision`.
    pub(in crate::node) fn wait_revision(
        &mut self,
        revision: i64,
        response: Response,
        applied: i64,
        leader_id: u64,
    ) {
        self.finish_revision_waiters(applied, leader_id);
        if applied >= revision {
            let _ = response.send(Ok(Value::Null));
        } else if self.revision_waiters.len() >= apply::MAX_QUERIES {
            let _ = response.send(Err(error("too many pending apply barriers", leader_id)));
        } else {
            self.revision_waiters.push_back(RevisionWaiter {
                revision,
                response,
                started: Instant::now(),
            });
        }
    }

    /// Answer the waits that `applied` reaches, and fail the waits that timed out.
    pub(in crate::node) fn finish_revision_waiters(&mut self, applied: i64, leader_id: u64) {
        for _ in 0..self.revision_waiters.len() {
            let waiter = self.revision_waiters.pop_front().unwrap();
            if waiter.response.is_closed() {
                continue;
            }
            if waiter.started.elapsed() > Duration::from_secs(5) {
                let _ = waiter
                    .response
                    .send(Err(error("local apply timed out", leader_id)));
            } else if applied >= waiter.revision {
                let _ = waiter.response.send(Ok(Value::Null));
            } else {
                self.revision_waiters.push_back(waiter);
            }
        }
    }

    /// Queue a read for the executor, or fail it if the queue is full.
    pub(in crate::node) fn queue(&mut self, query: Query, leader_id: u64) {
        if self.queries.len() >= apply::MAX_QUERIES {
            query.reject(error("too many queued reads", leader_id));
        } else {
            self.queries.push_back(query);
        }
    }

    /// Add a linearizable read under the leadership that `term`, `leader`, and `role` give.
    pub(in crate::node) fn add(
        &mut self,
        request: RequestOp,
        credentials: Option<crate::auth::Credentials>,
        response: ClientResponse,
        term: u64,
        leader: u64,
        role: StateRole,
    ) {
        if self.reads.len() >= MAX_PENDING_READS {
            response.error(error("too many pending read indexes", leader));
            return;
        }
        let Some(id) = self.next_id() else {
            response.error(error("read sequence exhausted", leader));
            return;
        };
        self.reads.insert(
            id,
            PendingRead {
                request: Some(request),
                credentials,
                response,
                term,
                leader,
                role,
                started: Instant::now(),
                index: None,
                context: None,
                retry_ticks: 2,
            },
        );
    }

    /// Put the reads without a batch into a new batch. Return its context id, which the caller
    /// sends to the quorum. Nothing starts while an earlier batch waits for its proof.
    pub(in crate::node) fn issue(&mut self) -> Option<u64> {
        if self
            .reads
            .values()
            .any(|read| read.context.is_some() && read.index.is_none())
        {
            return None;
        }
        let mut context = None;
        for (id, read) in &mut self.reads {
            if read.context.is_none() {
                let context = *context.get_or_insert(*id);
                read.context = Some(context);
                read.retry_ticks = 2;
            }
        }
        context
    }

    /// Count one tick for each batch that waits for its proof. Return the context ids that the
    /// caller sends to the quorum again.
    pub(in crate::node) fn retry(&mut self) -> BTreeSet<u64> {
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
        retry
    }

    /// The quorum confirmed the batch `context` at `index`. Only reads of the same leadership
    /// take the proof.
    pub(in crate::node) fn confirm(
        &mut self,
        context: u64,
        index: u64,
        term: u64,
        leader: u64,
        role: StateRole,
    ) {
        for read in self.reads.values_mut() {
            if read.context == Some(context)
                && read.index.is_none()
                && read.term == term
                && read.leader == leader
                && read.role == role
            {
                read.index = Some(index);
            }
        }
    }

    /// Queue each read whose proven index `applied` reaches. `MAX_PENDING_READS` bounds these
    /// reads, so they do not wait for room in the queue: a full queue of wide reads must not
    /// hold back a point read, such as a health check, while an executor task is free.
    pub(in crate::node) fn queue_confirmed(&mut self, applied: u64) {
        for (id, read) in &mut self.reads {
            if read.index.is_some_and(|index| index <= applied) {
                if let Some(request) = read.request.take() {
                    self.queries
                        .push_back(Query::Linear(request, read.credentials.take(), *id));
                }
            }
        }
    }

    /// Take the next batch of queued reads that `executor` can run now.
    pub(in crate::node) fn next_batch(&mut self, executor: &Executor) -> Option<Vec<Query>> {
        // Point reads can pass queued wide reads. Wide reads start in queue order,
        // so shared scans cannot starve an older exclusive read.
        let mut first_wide = true;
        let position = self.queries.iter().position(|query| {
            if query.is_point_read() {
                return executor.can_submit(query);
            }
            std::mem::take(&mut first_wide) && executor.can_submit(query)
        })?;
        let first = self.queries.remove(position).unwrap();
        let point = first.is_point_read();
        let mut queries = vec![first];
        while queries.len() < apply::READ_BATCH
            && self.queries.get(position).is_some_and(|next| {
                if point {
                    next.is_point_read()
                } else {
                    queries[0].same_linear_read(next)
                }
            })
        {
            queries.push(self.queries.remove(position).unwrap());
        }
        Some(queries)
    }

    /// Answer the reads `ids` with one shared outcome.
    pub(in crate::node) fn finish_shared(
        &mut self,
        ids: Vec<u64>,
        outcome: crate::mvcc::Outcome,
    ) -> Result<()> {
        let outcome = std::sync::Arc::new(outcome);
        for id in ids {
            if let Some(read) = self.reads.remove(&id) {
                read.response.shared_kv(outcome.clone())?;
            }
        }
        Ok(())
    }

    /// Answer the read `id`.
    pub(in crate::node) fn finish(&mut self, id: u64, outcome: crate::mvcc::Outcome) -> Result<()> {
        if let Some(read) = self.reads.remove(&id) {
            read.response.kv(outcome)?;
        }
        Ok(())
    }

    /// Fail the reads whose caller left, and the reads that the quorum has not confirmed within
    /// 5 s or under the same leader and term. Return the number of batches that lost their proof.
    ///
    /// As in etcd, only the ReadIndex wait has this limit. A confirmed read is linearizable at
    /// its read index whatever happens to leadership later, because the applied index only grows.
    /// It then waits for apply and an executor slot, and runs up to the executor limit. The gRPC
    /// handler bounds the whole request; when it gives up, the closed reply drops the read here.
    /// A held apply fails it earlier; see `fence`.
    pub(in crate::node) fn expire(&mut self, term: u64, leader: u64, role: StateRole) -> usize {
        let now = Instant::now();
        let expired: Vec<_> = self
            .reads
            .iter()
            .filter(|(_, read)| {
                read.response.is_closed()
                    || (read.index.is_none()
                        && (leader == 0
                            || read.leader != leader
                            || read.role != role
                            || read.term != term
                            || now.duration_since(read.started) > Duration::from_secs(5)))
            })
            .map(|(id, _)| *id)
            .collect();
        let mut failed = BTreeSet::new();
        for id in expired {
            if let Some(read) = self.reads.remove(&id) {
                if read.index.is_none() && !read.response.is_closed() {
                    failed.extend(read.context);
                }
                read.response.error(error(
                    "read cancelled: leadership changed or request timed out",
                    leader,
                ));
            }
        }
        // A queued read whose caller already got an error or left must not take an executor slot.
        let reads = &self.reads;
        self.queries.retain(|query| match query {
            Query::Linear(_, _, id) => reads.contains_key(id),
            Query::Read(_, _, response) => !response.is_closed(),
            _ => true,
        });
        failed.len()
    }

    /// The caller of `query` still waits for its result.
    pub(in crate::node) fn live(&self, query: &Query) -> bool {
        match query {
            Query::Linear(_, _, id) => self
                .reads
                .get(id)
                .is_some_and(|read| !read.response.is_closed()),
            Query::Read(_, _, response) => !response.is_closed(),
            _ => true,
        }
    }

    /// Fail each read whose proven index is above `applied`.
    pub(in crate::node) fn fence(&mut self, applied: u64, message: &str, leader_id: u64) {
        let fenced: Vec<_> = self
            .reads
            .iter()
            .filter(|(_, read)| read.index.is_some_and(|index| index > applied))
            .map(|(id, _)| *id)
            .collect();
        for id in fenced {
            if let Some(read) = self.reads.remove(&id) {
                read.response.error(error(message, leader_id));
            }
        }
    }
}

#[cfg(test)]
impl ReadIndexTable {
    /// The ids of the linearizable reads, in arrival order.
    pub(in crate::node) fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.reads.keys().copied()
    }

    pub(in crate::node) fn queries(&self) -> &VecDeque<Query> {
        &self.queries
    }

    /// Set the start of the read `id`, or of every read.
    pub(in crate::node) fn set_started(&mut self, id: Option<u64>, started: Instant) {
        for (_, read) in self
            .reads
            .iter_mut()
            .filter(|(key, _)| id.is_none_or(|id| **key == id))
        {
            read.started = started;
        }
    }

    /// Set the start of each revision wait.
    pub(in crate::node) fn set_revision_waiters_started(&mut self, started: Instant) {
        for waiter in &mut self.revision_waiters {
            waiter.started = started;
        }
    }
}
