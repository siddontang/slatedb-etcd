//! Prepare a committed Put reply while the leader waits for its local log device.

use super::apply::AppliedCommand;
use super::*;
use crate::state::put_batch::{PreparedPuts, Put};

impl Node {
    pub(super) fn prepare_quorum_puts(&mut self) -> Result<bool> {
        let raft = &self.raw.raft;
        let low = self.state.applied + 1;
        // Raft counts the leader's own replica for an entry only after local persistence.
        // A commit above the persisted index therefore has a durable quorum on other voters.
        if raft.state != StateRole::Leader
            || self.pending.is_empty()
            || low <= raft.raft_log.persisted
            || low > raft.raft_log.committed
            || self.state.applied < self.quorum_reply_index
            || raft.pending_conf_index > self.state.applied
            || self.raw.store().durable.hard_state.term != raft.term
        {
            return Ok(false);
        }
        let high = (low + apply::BATCH_ENTRIES).min(raft.raft_log.committed + 1);
        let high = high.min(self.raw.store().last_index()? + 1);
        if high <= low {
            return Ok(false);
        }
        let entries = self.raw.store().entries(
            low,
            high,
            Some(apply::BATCH_BYTES),
            raft::GetEntriesContext::empty(false),
        )?;
        let mut commands = Vec::new();
        for entry in entries {
            if entry.term != raft.term
                || entry.get_entry_type() != EntryType::EntryNormal
                || entry.data.is_empty()
                || u64::from(entry.compute_size()) > apply::BATCH_BYTES
            {
                break;
            }
            let command = RaftCommand::decode(&entry.data)?;
            if put_command(&command).is_none() {
                break;
            }
            commands.push((entry.index, command));
        }
        if !commands
            .iter()
            .any(|(_, command)| self.pending.contains_key(command.id()))
        {
            return Ok(false);
        }
        self.submit_apply(Job::PreparePuts(commands), self.state.applied)?;
        Ok(true)
    }
}

pub(super) fn put_command(
    command: &RaftCommand,
) -> Option<(&RequestOp, Option<&crate::auth::Credentials>, u64)> {
    let (command, credentials) = match command {
        RaftCommand::Secured {
            command,
            credentials,
            ..
        } => (command.as_ref(), Some(credentials)),
        command => (command, None),
    };
    match command {
        RaftCommand::Etcd { etcd, origin, .. } if crate::mvcc::batchable_put(etcd) => {
            Some((etcd, credentials, *origin))
        }
        _ => None,
    }
}

pub(super) async fn prepare(
    state: &StateMachine,
    commands: &[(u64, RaftCommand)],
    header: Header,
) -> Result<(PreparedPuts, Vec<AppliedCommand>)> {
    let puts = commands
        .iter()
        .map(|(index, command)| {
            let (request, credentials, origin) =
                put_command(command).context("expected an ordinary put")?;
            Ok(Put {
                index: *index,
                request,
                credentials,
                origin,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let (batch, outcomes) = state.prepare_puts(&puts).await?;
    let replies = commands
        .iter()
        .zip(outcomes)
        .map(|((_, command), mut outcome)| {
            header.outcome(&mut outcome);
            AppliedCommand {
                id: command.id().to_owned(),
                change: None,
                lease: None,
                reply: Reply::Kv(outcome),
            }
        })
        .collect();
    Ok((batch, replies))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::etcdserverpb::{request_op::Request, PutRequest};

    #[tokio::test]
    async fn only_a_durable_quorum_can_prepare_the_reply() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = super::super::tests::leader_node(dir.path()).await;
        let applied = node.state.applied;
        let (tx, mut reply) = oneshot::channel();
        node.propose(
            RaftCommand::Etcd {
                id: "before-sync".into(),
                origin: 1,
                etcd: RequestOp {
                    request: Some(Request::RequestPut(PutRequest {
                        key: b"slow".to_vec(),
                        value: b"disk".to_vec(),
                        ..Default::default()
                    })),
                },
            },
            ClientResponse::Kv(tx),
        )
        .unwrap();
        node.drive_ready().await.unwrap();
        let index = node.raw.store().last_index().unwrap();
        let mut ack = Message::default();
        ack.set_msg_type(MessageType::MsgAppendResponse);
        ack.to = 1;
        ack.term = node.raw.raft.term;
        ack.index = index;
        for peer in [2, 2, 3] {
            ack.from = peer;
            node.raw.step(ack.clone()).unwrap();
            node.drive_ready().await.unwrap();
            node.drain_apply().await.unwrap();
            assert!(node.log_worker.storage().last_index().unwrap() < index);
            assert_eq!(node.state.applied, applied);
            if peer == 2 {
                assert!(reply.try_recv().is_err());
                assert!(node.raw.raft.raft_log.committed < index);
            }
        }
        assert!(reply.try_recv().unwrap().unwrap().result.is_ok());
        assert_eq!(node.quorum_reply_index, index);
        let (tx, mut read) = oneshot::channel();
        node.enqueue_query(Query::Read(
            super::super::tests::range_request(),
            None,
            ClientResponse::Kv(tx),
        ));
        node.drive_ready().await.unwrap();
        node.drain_reads().await.unwrap();
        assert!(read.try_recv().is_err());
        node.drain_log().await.unwrap();
        node.drain_apply().await.unwrap();
        assert_eq!(node.state.applied, index);
        assert!(read.try_recv().unwrap().unwrap().result.is_ok());
        assert!(node.prepared_puts.is_none());
    }
}
