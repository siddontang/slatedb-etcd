//! Send consecutive ordinary puts through Raft in one proposal message.

use super::*;

const MAX_WAITING_PUTS: usize = 256;
const MAX_WAITING_BYTES: usize = 4 * 1024 * 1024;

struct WaitingPut {
    id: String,
    data: Vec<u8>,
    response: KvResponse,
    term: u64,
    expires: Instant,
}

#[derive(Default)]
pub(super) struct Waiting {
    requests: VecDeque<WaitingPut>,
    bytes: usize,
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
        if !self.waiting_puts.requests.is_empty() || self.backlogged() {
            let waiting = &mut self.waiting_puts;
            if waiting.requests.len() >= MAX_WAITING_PUTS
                || waiting.bytes + data.capacity() > MAX_WAITING_BYTES
            {
                let _ = response.send(Err(self.error("state apply backlog is full")));
                return Ok(());
            }
            waiting.bytes += data.capacity();
            waiting.requests.push_back(WaitingPut {
                id,
                data,
                response,
                term: self.raw.raft.term,
                expires: Instant::now() + Duration::from_secs(5),
            });
        } else {
            self.append_put(id, data, response);
        }
        Ok(())
    }

    pub(super) fn resume_puts(&mut self) {
        for _ in 0..apply::BATCH_ENTRIES {
            let Some(first) = self.waiting_puts.requests.front() else {
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
            let put = self.waiting_puts.requests.pop_front().unwrap();
            self.waiting_puts.bytes -= put.data.capacity();
            if put.response.is_closed() {
                continue;
            }
            if !current {
                let _ = put.response.send(Err(self.error("not leader")));
            } else if expired {
                let _ = put
                    .response
                    .send(Err(self.error("proposal admission timed out")));
            } else {
                self.append_put(put.id, put.data, put.response);
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
            data: data.into(),
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
    use crate::proto::etcdserverpb::{request_op::Request, response_op::Response, PutRequest};

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
        node.waiting_puts.requests.front_mut().unwrap().expires = Instant::now();
        drop(waiting_put(&mut node, vec![]));
        let mut live = waiting_put(&mut node, vec![1]);
        let large = waiting_put(&mut node, vec![0; MAX_WAITING_BYTES]);
        assert!(large.await.unwrap().unwrap_err().error.contains("backlog"));
        node.resume_puts();
        assert!(expired
            .await
            .unwrap()
            .unwrap_err()
            .error
            .contains("timed out"));
        assert_eq!(node.waiting_puts.requests.len(), 1);
        assert!(matches!(
            live.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(node.raw.raft.raft_log.last_index(), last);
        node.log_bytes = previous_bytes;
        node.drive_ready().await.unwrap();
        assert_eq!(node.raw.raft.raft_log.last_index(), last + 1);
        assert_eq!(node.waiting_puts.bytes, 0);
        node.log_bytes = log::MAX_BYTES;
        let stale = waiting_put(&mut node, vec![2]);
        node.raw.raft.become_follower(node.raw.raft.term + 1, 2);
        node.resume_puts();
        assert_eq!(stale.await.unwrap().unwrap_err().error, "not leader");
        assert_eq!(node.waiting_puts.bytes, 0);
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
