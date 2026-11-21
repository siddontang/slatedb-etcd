//! Prepare a committed Put reply before its state write.

use super::apply::AppliedCommand;
use super::*;
use crate::state::put_batch::{KvEntry, PreparedPuts};

impl Node {
    pub(super) fn prepare_quorum_puts(&mut self) -> Result<bool> {
        let raft = &self.raw.raft;
        let low = self.state.applied + 1;
        // Raft counts the leader's own replica for an entry only after local persistence.
        // A commit above the persisted index therefore has a durable quorum on other voters.
        if low <= raft.raft_log.persisted || low > raft.raft_log.committed {
            return Ok(false);
        }
        let high = (low + apply::BATCH_ENTRIES).min(raft.raft_log.committed + 1);
        let high = high.min(self.raw.store().last_index()? + 1);
        self.prepare_puts(high)
    }

    /// Reply to a small committed group of local Puts before the state write.
    ///
    /// Raft delivered these entries, so they are committed and durable in the local log. The
    /// reply needs only the evaluated outcome. Reads still wait for the published state.
    pub(super) fn prepare_local_puts(&mut self) -> Result<bool> {
        let low = self.state.applied + 1;
        if self.committed_ready < low || self.committed_ready - self.state.applied > QUICK_REPLY {
            return Ok(false);
        }
        self.prepare_puts(self.committed_ready + 1)
    }

    /// Evaluate the Puts in `[applied + 1, high)` for an early reply.
    fn prepare_puts(&mut self, high: u64) -> Result<bool> {
        let raft = &self.raw.raft;
        let low = self.state.applied + 1;
        if raft.state != StateRole::Leader
            || self.pending.is_empty()
            || high <= low
            || self.state.applied < self.quorum_reply_index
            || raft.pending_conf_index > self.state.applied
            || self.raw.store().durable.hard_state.term != raft.term
        {
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
                || entry.entry_type != EntryType::EntryNormal as i32
                || entry.data.is_empty()
                || (entry.encoded_len() as u64) > apply::BATCH_BYTES
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

/// Use the early reply for at most this many committed entries. A larger backlog gains more
/// from one pipelined apply job than from a split prepare and publish.
const QUICK_REPLY: u64 = 8;

pub(super) fn put_command(
    command: &RaftCommand,
) -> Option<(&RequestOp, Option<&crate::auth::Credentials>, u64)> {
    kv_command(command).filter(|(etcd, ..)| crate::mvcc::batchable_put(etcd))
}

/// Return an etcd KV request that can join an apply group.
pub(super) fn kv_command(
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
        RaftCommand::Etcd { etcd, origin, .. } => Some((etcd, credentials, *origin)),
        _ => None,
    }
}

/// Prepare one apply group. Callers place any member that needs a scan first.
pub(super) async fn prepare(
    state: &StateMachine,
    commands: &[(u64, RaftCommand)],
    header: Header,
) -> Result<(PreparedPuts, Vec<AppliedCommand>)> {
    let puts = commands
        .iter()
        .map(|(index, command)| {
            let (request, credentials, origin) =
                kv_command(command).context("expected an etcd KV request")?;
            Ok(KvEntry {
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
        let before = node.forwarding.local().expect("applied view published");
        assert_eq!(before.applied_revision(), node.state.revision);
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
        assert!(reply
            .try_recv()
            .unwrap()
            .unwrap()
            .into_outcome()
            .result
            .is_ok());
        assert_eq!(node.quorum_reply_index, index);
        // The Put reply left before the local apply. Serializable reads outside the owner must
        // not use the older view, or a client that saw the reply could miss its write here.
        assert!(node.forwarding.local().is_none());
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
        assert!(read
            .try_recv()
            .unwrap()
            .unwrap()
            .into_outcome()
            .result
            .is_ok());
        assert!(node.prepared_puts.is_none());
        let after = node
            .forwarding
            .local()
            .expect("applied view published again");
        assert_eq!(after.applied_revision(), node.state.revision);
        assert!(after.applied_revision() > before.applied_revision());
        let get = RequestOp {
            request: Some(Request::RequestRange(
                crate::proto::etcdserverpb::RangeRequest {
                    key: b"slow".to_vec(),
                    serializable: true,
                    ..Default::default()
                },
            )),
        };
        let Some(crate::proto::etcdserverpb::response_op::Response::ResponseRange(range)) = after
            .read(&get, None)
            .await
            .unwrap()
            .result
            .unwrap()
            .response
        else {
            panic!("expected range");
        };
        assert_eq!(range.kvs[0].value, b"disk");
        let header = range.header.unwrap();
        assert_eq!(header.revision, node.state.revision);
        assert_eq!(header.member_id, node.options.id);
        assert_eq!(header.raft_term, node.raw.raft.term);
        // The view taken before the write still answers from its own snapshot.
        let Some(crate::proto::etcdserverpb::response_op::Response::ResponseRange(old)) = before
            .read(&get, None)
            .await
            .unwrap()
            .result
            .unwrap()
            .response
        else {
            panic!("expected range");
        };
        assert!(old.kvs.is_empty());
    }
    #[tokio::test]
    async fn a_locally_durable_put_replies_before_its_state_write_and_commit_marker() {
        let dir = tempfile::tempdir().unwrap();
        let mut node = super::super::tests::leader_node(dir.path()).await;
        let applied = node.state.applied;
        let (tx, mut reply) = oneshot::channel();
        node.propose(
            RaftCommand::Etcd {
                id: "local".into(),
                origin: 1,
                etcd: RequestOp {
                    request: Some(Request::RequestPut(PutRequest {
                        key: b"key".to_vec(),
                        value: b"value".to_vec(),
                        ..Default::default()
                    })),
                },
            },
            ClientResponse::Kv(tx),
        )
        .unwrap();
        node.drive_ready().await.unwrap();
        let index = node.raw.store().last_index().unwrap();
        // Persist the entry locally. One peer ack then makes a quorum.
        let persisted = node.log_worker.step().unwrap().unwrap();
        node.complete_log(persisted).unwrap();
        node.drive_ready().await.unwrap();
        assert!(node.raw.raft.raft_log.persisted >= index);
        let mut ack = Message::default();
        ack.set_msg_type(MessageType::MsgAppendResponse);
        ack.from = 2;
        ack.to = 1;
        ack.term = node.raw.raft.term;
        ack.index = index;
        node.raw.step(ack).unwrap();
        node.drive_ready().await.unwrap();
        assert!(node.raw.raft.raft_log.committed >= index);
        // The log thread has not written the commit marker yet.
        assert!(node.log_worker.storage().commit_index() < index);
        let completed = node.worker.step().await.unwrap().unwrap();
        node.complete_apply(completed).unwrap();
        assert!(reply
            .try_recv()
            .unwrap()
            .unwrap()
            .into_outcome()
            .result
            .is_ok());
        assert_eq!(node.state.applied, applied);
        assert_eq!(node.quorum_reply_index, index);
        // A read must not see the state before the prepared write is published.
        let (tx, mut read) = oneshot::channel();
        node.enqueue_query(Query::Read(
            super::super::tests::range_request(),
            None,
            ClientResponse::Kv(tx),
        ));
        node.drive_ready().await.unwrap();
        node.drain_reads().await.unwrap();
        assert!(read.try_recv().is_err());
        node.drain_apply().await.unwrap();
        assert_eq!(node.state.applied, index);
        assert!(node.log_worker.storage().commit_index() < index);
        let outcome = read.try_recv().unwrap().unwrap().into_outcome();
        let Some(crate::proto::etcdserverpb::response_op::Response::ResponseRange(range)) =
            outcome.result.unwrap().response
        else {
            panic!("expected a Range reply");
        };
        assert_eq!(range.kvs[0].value, b"value");
        node.drain_log().await.unwrap();
        assert!(node.log_worker.storage().commit_index() >= index);
    }
}
