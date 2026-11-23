//! Send consecutive ordinary puts through Raft in one proposal message, and hold client
//! writes before proposal while apply or log I/O is behind.

use super::*;

const MAX_WAITING: usize = 256;
const MAX_WAITING_BYTES: usize = 4 * 1024 * 1024;
const WAIT_LIMIT: Duration = Duration::from_secs(5);
/// etcd returns ErrTooManyRequests when apply falls too far behind. A deferred
/// proposal that expires was never proposed, so its outcome is known.
pub(super) const TOO_MANY_REQUESTS: &str = "too many requests";

/// A client reply for a write that has not been proposed yet.
enum Deferred {
    /// An ordinary put that joins the shared proposal group.
    Put(KvResponse),
    /// Any other client write, proposed as its own entry.
    Command(ClientResponse),
}

impl Deferred {
    fn is_closed(&self) -> bool {
        match self {
            Self::Put(response) => response.is_closed(),
            Self::Command(response) => response.is_closed(),
        }
    }

    fn error(self, error: ApiError) {
        match self {
            Self::Put(response) => {
                let _ = response.send(Err(error));
            }
            Self::Command(response) => response.error(error),
        }
    }
}

struct WaitingProposal {
    id: String,
    data: Vec<u8>,
    response: Deferred,
    term: u64,
    expires: Instant,
}

/// Client writes held before proposal while apply or log I/O is behind.
///
/// One FIFO keeps log order equal to admission order across puts and other writes.
#[derive(Default)]
pub(super) struct Waiting {
    requests: VecDeque<WaitingProposal>,
    bytes: usize,
}

impl Waiting {
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.requests.len()
    }
}

#[derive(Default)]
pub(super) struct Group {
    entries: Vec<Entry>,
    clients: Vec<(String, KvResponse)>,
    bytes: usize,
}

impl Group {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Node {
    pub(super) fn queue_put(
        &mut self,
        request: RequestOp,
        credentials: Option<crate::auth::Credentials>,
        response: KvResponse,
    ) -> Result<()> {
        if self.raw.raft.state != StateRole::Leader {
            let _ = response.send(Err(self.error("not leader")));
            return Ok(());
        }
        if self.pending.len() + self.put_proposals.clients.len() >= 1024 {
            let _ = response.send(Err(self.error("too many pending requests")));
            return Ok(());
        }
        let id = uuid::Uuid::new_v4().to_string();
        let data = RaftCommand::encode_kv(id.clone(), self.options.id, request, credentials);
        if self.must_wait() {
            self.defer(id, data, Deferred::Put(response));
        } else {
            self.append_put(id, data, response);
        }
        Ok(())
    }

    /// A new write waits behind earlier waiting writes or while capacity is short.
    pub(super) fn must_wait(&mut self) -> bool {
        !self.waiting_proposals.is_empty() || self.backlogged()
    }

    /// Hold an encoded write until capacity returns. Nothing reaches Raft here, so
    /// every error sent later means the write was not proposed.
    pub(super) fn defer_command(&mut self, id: String, data: Vec<u8>, response: ClientResponse) {
        self.defer(id, data, Deferred::Command(response));
    }

    fn defer(&mut self, id: String, data: Vec<u8>, response: Deferred) {
        let waiting = &mut self.waiting_proposals;
        if waiting.requests.len() >= MAX_WAITING
            || waiting.bytes + data.capacity() > MAX_WAITING_BYTES
        {
            response.error(self.error("state apply backlog is full"));
            return;
        }
        waiting.bytes += data.capacity();
        waiting.requests.push_back(WaitingProposal {
            id,
            data,
            response,
            term: self.raw.raft.term,
            expires: Instant::now() + WAIT_LIMIT,
        });
    }

    /// Propose waiting writes in order while capacity allows. Fail stale, expired, and
    /// cancelled writes without proposing them.
    pub(super) fn resume_proposals(&mut self) {
        for _ in 0..apply::BATCH_ENTRIES {
            let Some(first) = self.waiting_proposals.requests.front() else {
                break;
            };
            let current =
                self.raw.raft.state == StateRole::Leader && first.term == self.raw.raft.term;
            let expired = Instant::now() >= first.expires;
            if current
                && !expired
                && !first.response.is_closed()
                && (self.backlogged()
                    || self.pending.len() + self.put_proposals.clients.len() >= 1024)
            {
                break;
            }
            let waiting = self.waiting_proposals.requests.pop_front().unwrap();
            self.waiting_proposals.bytes -= waiting.data.capacity();
            if waiting.response.is_closed() {
                continue;
            }
            if !current {
                waiting.response.error(self.error("not leader"));
            } else if expired {
                waiting.response.error(self.error(TOO_MANY_REQUESTS));
            } else {
                match waiting.response {
                    Deferred::Put(response) => self.append_put(waiting.id, waiting.data, response),
                    Deferred::Command(response) => {
                        // Earlier puts in the group must reach the log first.
                        self.flush_puts();
                        self.propose_data(waiting.id, waiting.data, response);
                    }
                }
            }
        }
    }

    fn append_put(&mut self, id: String, data: Vec<u8>, response: KvResponse) {
        let charge = data.len() + cache::ENTRY_OVERHEAD + 64;
        if self.put_proposals.bytes + charge > apply::BATCH_BYTES as usize {
            self.flush_puts();
        }
        if data.len() + 64 > cache::MAX_ENTRY_BYTES
            || !self.raw.store().admits(
                self.raw.raft.raft_log.unstable_entries(),
                &[],
                self.put_proposals.bytes + charge,
            )
        {
            let _ = response.send(Err(self.error("pending log memory quota is full")));
            return;
        }
        self.put_proposals.entries.push(Entry {
            data,
            ..Default::default()
        });
        self.put_proposals.clients.push((id, response));
        self.put_proposals.bytes += charge;
        if self.put_proposals.entries.len() >= apply::BATCH_ENTRIES as usize {
            self.flush_puts();
        }
    }

    pub(super) fn flush_puts(&mut self) {
        if self.put_proposals.is_empty() {
            return;
        }
        let group = std::mem::take(&mut self.put_proposals);
        // This is the local MsgPropose that RawNode::propose builds for one entry.
        let mut proposal = Message {
            msg_type: MessageType::MsgPropose as i32,
            from: self.options.id,
            ..Default::default()
        };
        proposal.set_entries(group.entries);
        let result = self.raw.raft.step(proposal);
        let started = Instant::now();
        for (id, response) in group.clients {
            if let Err(ref error) = result {
                self.metrics.failed_proposals.inc();
                let _ = response.send(Err(self.error(&error.to_string())));
                continue;
            }
            self.pending.insert(
                id,
                Pending {
                    response: ClientResponse::Kv(response),
                    started,
                    term: self.raw.raft.term,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::etcdserverpb::{
        request_op::Request, response_op::Response, PutRequest, TxnRequest,
    };

    fn put_op(key: &[u8], value: Vec<u8>) -> RequestOp {
        RequestOp {
            request: Some(Request::RequestPut(PutRequest {
                key: key.to_vec(),
                value,
                ..Default::default()
            })),
        }
    }

    fn txn_op(key: &[u8], value: Vec<u8>) -> RequestOp {
        RequestOp {
            request: Some(Request::RequestTxn(TxnRequest {
                success: vec![put_op(key, value)],
                ..Default::default()
            })),
        }
    }

    async fn send(node: &mut Node, op: RequestOp) -> oneshot::Receiver<Result<KvReply, ApiError>> {
        let (tx, rx) = oneshot::channel();
        node.handle(Event::Kv(op, tx), &mut vec![]).await.unwrap();
        rx
    }

    /// Commit everything the leader has appended through the second voter's ack.
    async fn commit_all(node: &mut Node) {
        node.drive_ready().await.unwrap();
        node.drain_log().await.unwrap();
        let index = node.raw.raft.raft_log.last_index();
        node.raw
            .step(Message {
                msg_type: MessageType::MsgAppendResponse as i32,
                from: 2,
                to: 1,
                term: node.raw.raft.term,
                index,
                ..Default::default()
            })
            .unwrap();
        node.drive_ready().await.unwrap();
        node.drain_log().await.unwrap();
        node.drain_apply().await.unwrap();
    }

    fn revision(reply: Result<KvReply, ApiError>) -> i64 {
        let result = reply.unwrap().into_outcome().result.unwrap();
        match result.response {
            Some(Response::ResponsePut(put)) => put.header.unwrap().revision,
            Some(Response::ResponseTxn(txn)) => {
                assert!(txn.succeeded);
                txn.header.unwrap().revision
            }
            other => panic!("unexpected response {other:?}"),
        }
    }

    #[tokio::test]
    async fn backlog_defers_txns_behind_puts_and_proposes_them_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = super::super::tests::leader_node(dir.path()).await;
        let last = node.raw.raft.raft_log.last_index();
        let start = node.state.revision;
        let previous_bytes = node.log_bytes;
        node.log_bytes = log::MAX_BYTES;
        let mut first = send(&mut node, put_op(b"a", vec![1])).await;
        let mut txn = send(&mut node, txn_op(b"b", vec![2])).await;
        let mut second = send(&mut node, put_op(b"a", vec![3])).await;
        node.drive_ready().await.unwrap();
        // While the log is full, every write waits instead of failing.
        for rx in [&mut first, &mut txn, &mut second] {
            assert!(matches!(
                rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        assert_eq!(node.waiting_proposals.requests.len(), 3);
        assert_eq!(node.raw.raft.raft_log.last_index(), last);
        // A txn arriving with free capacity still queues behind the waiting writes.
        node.log_bytes = previous_bytes;
        let mut late = send(&mut node, txn_op(b"c", vec![4])).await;
        assert_eq!(node.waiting_proposals.requests.len(), 4);
        assert_eq!(node.raw.raft.raft_log.last_index(), last);
        assert!(matches!(
            late.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        commit_all(&mut node).await;
        assert!(node.waiting_proposals.is_empty());
        assert_eq!(node.waiting_proposals.bytes, 0);
        assert_eq!(node.raw.raft.raft_log.last_index(), last + 4);
        assert_eq!(revision(first.await.unwrap()), start + 1);
        assert_eq!(revision(txn.await.unwrap()), start + 2);
        assert_eq!(revision(second.await.unwrap()), start + 3);
        assert_eq!(revision(late.await.unwrap()), start + 4);
    }

    #[tokio::test]
    async fn waiting_txns_fail_unproposed_on_timeout_cancel_and_leader_change() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = super::super::tests::leader_node(dir.path()).await;
        let last = node.raw.raft.raft_log.last_index();
        let pending = node.pending.len();
        node.log_bytes = log::MAX_BYTES;
        let expired = send(&mut node, txn_op(b"t", vec![1])).await;
        node.waiting_proposals.requests.front_mut().unwrap().expires = Instant::now();
        drop(send(&mut node, txn_op(b"t", vec![2])).await);
        let (tx, deleted) = oneshot::channel();
        node.handle(
            Event::EtcdCompact(
                CompactionRequest {
                    revision: 1,
                    ..Default::default()
                },
                tx,
            ),
            &mut vec![],
        )
        .await
        .unwrap();
        let mut stale = send(&mut node, txn_op(b"t", vec![3])).await;
        node.drive_ready().await.unwrap();
        let error = expired.await.unwrap().unwrap_err();
        assert_eq!(error.error, TOO_MANY_REQUESTS);
        // The cancelled txn left the queue; the compaction and the last txn still wait.
        assert_eq!(node.waiting_proposals.requests.len(), 2);
        assert!(matches!(
            stale.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        node.raw.raft.become_follower(node.raw.raft.term + 1, 2);
        node.resume_proposals();
        assert_eq!(deleted.await.unwrap().unwrap_err().error, "not leader");
        assert_eq!(stale.await.unwrap().unwrap_err().error, "not leader");
        assert!(node.waiting_proposals.is_empty());
        assert_eq!(node.waiting_proposals.bytes, 0);
        // No failed write reached Raft.
        assert_eq!(node.raw.raft.raft_log.last_index(), last);
        assert_eq!(node.pending.len(), pending);
    }

    #[tokio::test]
    async fn a_full_wait_queue_rejects_txns_without_growing() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = super::super::tests::leader_node(dir.path()).await;
        node.log_bytes = log::MAX_BYTES;
        let mut held = Vec::new();
        for n in 0..MAX_WAITING {
            held.push(send(&mut node, txn_op(b"q", vec![n as u8])).await);
        }
        let rejected = send(&mut node, txn_op(b"q", vec![])).await;
        assert!(rejected
            .await
            .unwrap()
            .unwrap_err()
            .error
            .contains("backlog"));
        assert_eq!(node.waiting_proposals.requests.len(), MAX_WAITING);
    }

    fn waiting_put(
        node: &mut Node,
        value: Vec<u8>,
    ) -> oneshot::Receiver<Result<KvReply, ApiError>> {
        let (tx, rx) = oneshot::channel();
        let put = PutRequest {
            key: b"waiting".to_vec(),
            value,
            ..Default::default()
        };
        node.queue_put(
            RequestOp {
                request: Some(Request::RequestPut(put)),
            },
            None,
            tx,
        )
        .unwrap();
        rx
    }

    #[tokio::test]
    async fn log_pressure_defers_puts_until_progress_and_drops_stale_requests() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = super::super::tests::leader_node(dir.path()).await;
        let last = node.raw.raft.raft_log.last_index();
        let previous_bytes = node.log_bytes;
        node.log_bytes = log::MAX_BYTES;
        let expired = waiting_put(&mut node, vec![]);
        node.waiting_proposals.requests.front_mut().unwrap().expires = Instant::now();
        drop(waiting_put(&mut node, vec![]));
        let mut live = waiting_put(&mut node, vec![1]);
        let large = waiting_put(&mut node, vec![0; MAX_WAITING_BYTES]);
        assert!(large.await.unwrap().unwrap_err().error.contains("backlog"));
        node.resume_proposals();
        assert!(expired
            .await
            .unwrap()
            .unwrap_err()
            .error
            .contains(TOO_MANY_REQUESTS));
        assert_eq!(node.waiting_proposals.requests.len(), 1);
        assert!(matches!(
            live.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(node.raw.raft.raft_log.last_index(), last);
        node.log_bytes = previous_bytes;
        node.drive_ready().await.unwrap();
        assert_eq!(node.raw.raft.raft_log.last_index(), last + 1);
        assert_eq!(node.waiting_proposals.bytes, 0);
        node.log_bytes = log::MAX_BYTES;
        let stale = waiting_put(&mut node, vec![2]);
        node.raw.raft.become_follower(node.raw.raft.term + 1, 2);
        node.resume_proposals();
        assert_eq!(stale.await.unwrap().unwrap_err().error, "not leader");
        assert_eq!(node.waiting_proposals.bytes, 0);
        assert_eq!(node.raw.raft.raft_log.last_index(), last + 1);
    }

    #[tokio::test]
    async fn consecutive_puts_share_replication_and_keep_individual_results() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = super::super::tests::leader_node(dir.path()).await;
        let previous = node.state.applied;
        let revision = node.state.revision;
        let mut replies = Vec::new();
        for n in 0..4 {
            let (tx, rx) = oneshot::channel();
            node.handle(
                Event::Kv(
                    RequestOp {
                        request: Some(Request::RequestPut(PutRequest {
                            key: b"batched".to_vec(),
                            value: vec![n],
                            prev_kv: true,
                            ..Default::default()
                        })),
                    },
                    tx,
                ),
                &mut vec![],
            )
            .await
            .unwrap();
            replies.push(rx);
        }
        assert_eq!(node.raw.raft.raft_log.last_index(), previous);
        let (tx, status) = oneshot::channel();
        node.handle(Event::Status(tx), &mut vec![]).await.unwrap();
        assert!(status.await.unwrap().is_ok());
        assert_eq!(node.raw.raft.raft_log.last_index(), previous + 4);
        let messages: Vec<_> = node
            .raw
            .raft
            .msgs
            .iter()
            .filter(|m| {
                m.to == 2 && m.get_msg_type() == MessageType::MsgAppend && !m.entries.is_empty()
            })
            .collect();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].entries.len(), 4);
        node.drive_ready().await.unwrap();
        node.drain_log().await.unwrap();
        node.raw
            .step(Message {
                msg_type: MessageType::MsgAppendResponse as i32,
                from: 2,
                to: 1,
                term: node.raw.raft.term,
                index: previous + 4,
                ..Default::default()
            })
            .unwrap();
        node.drive_ready().await.unwrap();
        node.drain_log().await.unwrap();
        node.drain_apply().await.unwrap();
        for (n, reply) in replies.into_iter().enumerate() {
            let result = reply.await.unwrap().unwrap().into_outcome().result.unwrap();
            let Some(Response::ResponsePut(put)) = result.response else {
                panic!("expected Put");
            };
            assert_eq!(put.header.unwrap().revision, revision + n as i64 + 1);
            assert_eq!(
                put.prev_kv.map(|kv| kv.value),
                n.checked_sub(1).map(|i| vec![i as u8])
            );
        }
    }
}
