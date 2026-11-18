//! Send consecutive ordinary puts through Raft in one proposal message.

use super::*;

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
        if self.backlogged() {
            let _ = response.send(Err(self.error("state apply backlog is full")));
            return Ok(());
        }
        if self.pending.len() + self.put_proposals.clients.len() >= 1024 {
            let _ = response.send(Err(self.error("too many pending requests")));
            return Ok(());
        }
        let id = uuid::Uuid::new_v4().to_string();
        let data = RaftCommand::Etcd {
            origin: self.options.id,
            id: id.clone(),
            etcd: request,
        }
        .secure(credentials)
        .encode()?;
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
            return Ok(());
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
        Ok(())
    }

    pub(super) fn flush_puts(&mut self) {
        if self.put_proposals.is_empty() {
            return;
        }
        let group = std::mem::take(&mut self.put_proposals);
        // This is the local MsgPropose that RawNode::propose builds for one entry.
        let mut proposal = Message {
            msg_type: MessageType::MsgPropose,
            from: self.options.id,
            ..Default::default()
        };
        proposal.set_entries(group.entries.into());
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
                msg_type: MessageType::MsgAppendResponse,
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
            let result = reply.await.unwrap().unwrap().result.unwrap();
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
