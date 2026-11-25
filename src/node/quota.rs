//! Leader proposals of the replicated cluster quota. See `state::quota`.
//!
//! The `--quota-backend-bytes` flag seeds the cluster quota and requests changes to it. A member
//! keeps a local record of its flag in the data directory. Raft, snapshots, and backups do not
//! carry that record. A restart with a changed flag marks a pending change. The member proposes
//! it whenever it leads, until the cluster quota equals its flag. So a member with an unchanged
//! old flag does not revert the cluster quota when it leads.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::*;

/// The file of the local flag record in the data directory.
const RECORD: &str = "quota-flag.json";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    /// The last flag value that needs no proposal: the first flag of this directory, or a flag
    /// that the cluster quota took.
    acknowledged: i64,
    /// A changed flag value that this member must propose.
    pending: Option<i64>,
}

/// The quota flag of this member and its proposal state.
pub(super) struct Quota {
    /// The effective flag value. A non-positive value disables the quota.
    flag: i64,
    /// The record file and its content. `None` in shared mode, which never proposes.
    record: Option<(PathBuf, Record)>,
    /// The index and term of the last `SetQuota` proposal of this member.
    proposed: Option<(u64, u64)>,
    /// The cluster quota that `observe_quota` last handled.
    observed: Option<Option<i64>>,
}

/// Return the `SetQuota` value and seed mark that a leader proposes, or `None`.
///
/// This is the "operator-change detection" policy. Without a cluster quota, a leader seeds it
/// from its flag. Otherwise it proposes only its own pending flag change.
fn proposal(cluster: Option<i64>, flag: i64, pending: bool) -> Option<(i64, bool)> {
    match cluster {
        None => Some((flag, !pending)),
        Some(value) if pending && value != flag => Some((flag, false)),
        Some(_) => None,
    }
}

impl Quota {
    /// Load the record of `dir`. A flag that differs from the acknowledged value is pending.
    /// A member without a record acknowledges its flag, so a new member requests no change.
    pub(super) fn open(dir: &Path, flag: i64, shared: bool) -> Result<Self> {
        let mut quota = Self {
            flag,
            record: None,
            proposed: None,
            observed: None,
        };
        if shared {
            return Ok(quota);
        }
        let path = dir.join(RECORD);
        let stored = match std::fs::read(&path) {
            Ok(data) => {
                Some(serde_json::from_slice::<Record>(&data).context("invalid quota record")?)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let acknowledged = stored.map_or(flag, |record| record.acknowledged);
        let record = Record {
            acknowledged,
            pending: (flag != acknowledged).then_some(flag),
        };
        if stored != Some(record) {
            write_record(&path, &record)?;
        }
        quota.record = Some((path, record));
        Ok(quota)
    }

    fn pending(&self) -> bool {
        self.record
            .as_ref()
            .is_some_and(|(_, record)| record.pending.is_some())
    }

    /// Clear a pending change once the cluster quota equals the flag.
    fn acknowledge(&mut self, cluster: Option<i64>) -> Result<()> {
        let Some((path, record)) = &mut self.record else {
            return Ok(());
        };
        if record.pending.is_some() && cluster == Some(self.flag) {
            *record = Record {
                acknowledged: self.flag,
                pending: None,
            };
            write_record(path, record)?;
        }
        Ok(())
    }
}

/// Replace the record file atomically.
fn write_record(path: &Path, record: &Record) -> Result<()> {
    let dir = path.parent().context("quota record has no directory")?;
    let temporary = path.with_extension("json.pending");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(record)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

impl Node {
    /// Propose the cluster quota when this member leads and `proposal` asks for it.
    ///
    /// The node calls this right after each Raft step and tick, before it takes other work. So
    /// the entry follows the empty entry of a new leader, before every client write of the term.
    /// One proposal of a term stays in the log, so the next one waits until it applies.
    pub(super) fn propose_quota(&mut self) -> Result<()> {
        if self.raw.raft.state != StateRole::Leader || self.quota.record.is_none() {
            return Ok(());
        }
        let term = self.raw.raft.term;
        if let Some((index, proposed)) = self.quota.proposed {
            if proposed == term && self.state.applied < index {
                return Ok(());
            }
        }
        let Some((bytes, seed)) = proposal(
            self.state.cluster_quota,
            self.quota.flag,
            self.quota.pending(),
        ) else {
            return Ok(());
        };
        let data = RaftCommand::SetQuota {
            id: uuid::Uuid::new_v4().to_string(),
            bytes,
            seed,
        }
        .encode()?;
        // The entry is small. It skips the memory admission check, so no client write goes
        // first. Raft drops it only during a leadership transfer; the next call tries again.
        if self.raw.propose(vec![], data).is_ok() {
            self.quota.proposed = Some((self.raw.raft.raft_log.last_index(), term));
            tracing::info!(bytes, seed, term, "propose the cluster quota");
        }
        Ok(())
    }

    /// Update the local record after the applied cluster quota changed, and warn when the flag
    /// differs from the quota that apply uses.
    pub(super) fn observe_quota(&mut self) -> Result<()> {
        let cluster = self.state.cluster_quota;
        if self.quota.observed == Some(cluster) {
            return Ok(());
        }
        self.quota.observed = Some(cluster);
        self.quota.acknowledge(cluster)?;
        // Shared mode warns when it reads the cluster claim.
        if self.state.quota_mismatch && self.quota.record.is_some() {
            tracing::warn!(
                configured = self.quota.flag,
                cluster = self.state.quota_bytes,
                pending = self.quota.pending(),
                "the cluster state sets the quota"
            );
        }
        Ok(())
    }

    /// The quota fields of `/status`.
    pub(super) fn quota_status(&self) -> Value {
        json!({
            "bytes": self.state.quota_bytes,
            "cluster_bytes": self.state.cluster_quota,
            "flag_bytes": self.quota.flag,
            "flag_mismatch": self.state.quota_mismatch,
            "flag_pending": self.quota.pending(),
        })
    }

    /// The `db_size_quota` of Maintenance Status. Like etcd, it is the configured value, unless
    /// the quota that apply uses differs from it.
    pub(super) fn status_quota(&self) -> i64 {
        if self.state.quota_mismatch {
            self.state.quota_bytes
        } else {
            self.options.quota_bytes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::options;
    use super::*;
    use raft::eraftpb::HardState;

    #[test]
    fn a_leader_seeds_or_proposes_only_its_own_change() {
        // No cluster quota: seed it, or set it when the operator changed the flag.
        assert_eq!(proposal(None, 5, false), Some((5, true)));
        assert_eq!(proposal(None, 5, true), Some((5, false)));
        // An unchanged old flag does not replace the cluster quota.
        assert_eq!(proposal(Some(9), 5, false), None);
        assert_eq!(proposal(Some(9), 5, true), Some((5, false)));
        assert_eq!(proposal(Some(5), 5, true), None);
        // A disabled quota is a value like any other.
        assert_eq!(proposal(Some(9), -1, true), Some((-1, false)));
    }

    #[test]
    fn a_changed_flag_stays_pending_across_restarts_until_the_cluster_takes_it() {
        let dir = tempfile::tempdir().unwrap();
        // A member without a record requests no change.
        let quota = Quota::open(dir.path(), 100, false).unwrap();
        assert!(!quota.pending());
        // Two restarts with a new flag before the member leads keep the latest flag pending.
        assert!(Quota::open(dir.path(), 200, false).unwrap().pending());
        let mut quota = Quota::open(dir.path(), 300, false).unwrap();
        assert!(quota.pending());
        quota.acknowledge(Some(100)).unwrap();
        assert!(quota.pending());
        quota.acknowledge(Some(300)).unwrap();
        assert!(!quota.pending());
        assert!(!Quota::open(dir.path(), 300, false).unwrap().pending());
        // Back to the acknowledged value: nothing is pending.
        assert!(Quota::open(dir.path(), 400, false).unwrap().pending());
        assert!(!Quota::open(dir.path(), 300, false).unwrap().pending());
        // Shared mode keeps no record.
        let shared = tempfile::tempdir().unwrap();
        assert!(Quota::open(shared.path(), 1, true)
            .unwrap()
            .record
            .is_none());
        assert!(!shared.path().join(RECORD).exists());
    }
    fn quota_put(n: u64) -> RaftCommand {
        RaftCommand::Etcd {
            id: format!("put-{n}"),
            origin: 1,
            etcd: RequestOp {
                request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                    crate::proto::etcdserverpb::PutRequest {
                        key: format!("key-{n}").into_bytes(),
                        value: vec![b'v'; 32],
                        ..Default::default()
                    },
                )),
            },
        }
    }

    /// Append committed entries `first..` with `commands` to the Raft log of `dir`.
    fn append_committed(dir: &std::path::Path, first: u64, commands: &[RaftCommand]) {
        let mut storage =
            RaftStorage::open(&dir.join("raft"), 1, "worker-test", vec![1, 2, 3]).unwrap();
        let entries: Vec<_> = (first..)
            .zip(commands)
            .map(|(index, command)| Entry {
                index,
                term: 1,
                data: command.encode().unwrap(),
                ..Default::default()
            })
            .collect();
        let commit = first + commands.len() as u64 - 1;
        storage
            .persist(
                &entries,
                Some(&HardState {
                    term: 1,
                    vote: 1,
                    commit,
                }),
                None,
            )
            .unwrap();
    }

    /// Apply every committed entry of a follower node.
    async fn apply_committed(node: &mut Node) {
        for _ in 0..10 {
            node.drive_ready().await.unwrap();
            node.drain_log().await.unwrap();
            node.drain_apply().await.unwrap();
        }
        assert_eq!(node.state.applied, node.raw.store().commit_index());
    }

    /// A directory of an earlier binary has no cluster quota. After an upgrade, a member replays its
    /// unflushed tail under its flag, as the earlier binary applied it.
    #[tokio::test]
    async fn an_upgraded_member_replays_its_unflushed_tail_under_its_flag() {
        let commands: Vec<_> = (1..=40).map(quota_put).collect();
        let mut results = vec![];
        for flushed in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            append_committed(dir.path(), 1, &commands);
            if flushed {
                // The earlier binary applied and flushed every entry under its flag.
                let mut state = StateMachine::open(&dir.path().join("objects"))
                    .await
                    .unwrap();
                state.quota = 3000;
                for (index, command) in (1..).zip(&commands) {
                    let RaftCommand::Etcd { etcd, origin, .. } = command else {
                        unreachable!()
                    };
                    state.apply_etcd_quota(index, etcd, *origin).await.unwrap();
                }
                state.snapshot_data().await.unwrap();
                state.close().await.unwrap();
            }
            let mut config = options(dir.path());
            config.quota_bytes = 3000;
            let (events, _) = mpsc::channel(256);
            let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
                .await
                .unwrap();
            assert_eq!(node.state.applied, if flushed { 40 } else { 0 });
            apply_committed(&mut node).await;
            assert_eq!(node.state.cluster_quota, None);
            let state = node.worker.state();
            results.push((node.state.revision, state.alarms.len(), state.quota));
        }
        assert_eq!(results[0], results[1]);
        let (revision, alarms, quota) = results[0];
        assert!(revision > 1 && revision < 41, "revision {revision}");
        assert_eq!((alarms, quota), (1, 3000));
    }

    /// A restored backup keeps its cluster quota. The flag of the new member does not replace it,
    /// and the entries above the restored index apply under it.
    #[tokio::test]
    async fn a_restored_cluster_quota_wins_over_the_flag() {
        let dir = tempfile::tempdir().unwrap();
        let mut source = StateMachine::open(&dir.path().join("source"))
            .await
            .unwrap();
        source.set_quota_flag(1 << 20);
        source.apply_set_quota(1, 3000, false).await.unwrap();
        let image = source.snapshot_data().await.unwrap();
        let mut backup = image.clone();
        use sha2::Digest as _;
        backup.extend_from_slice(&sha2::Sha256::digest(&image));
        let path = dir.path().join("backup");
        std::fs::write(&path, backup).unwrap();
        let target = dir.path().join("member");
        let config = options(&target);
        let members = crate::cluster::Store::initial(&config.peers);
        StateMachine::restore_backup(&path, &target, 1, "worker-test", &members).unwrap();
        // The restored log starts at index 1. Later entries follow it.
        let commands: Vec<_> = (1..=40).map(quota_put).collect();
        append_committed(&target, 2, &commands);
        let mut config = options(&target);
        config.quota_bytes = 1 << 20;
        let (events, _) = mpsc::channel(256);
        let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap();
        apply_committed(&mut node).await;
        assert_eq!(node.state.cluster_quota, Some(3000));
        assert!(node.state.quota_mismatch);
        assert_eq!(node.status_quota(), 3000);
        assert_eq!(node.quota_status()["flag_bytes"], 1 << 20);
        // The flag would accept every Put. The restored quota rejects the later ones.
        assert!(node.state.revision > 1 && node.state.revision < 41);
        assert_eq!(node.worker.state().alarms.len(), 1);
        assert_eq!(node.worker.state().quota, 3000);
        source.close().await.unwrap();
    }

    /// A new leader proposes the cluster quota right after its election. A client write that
    /// arrives next follows it in the log.
    #[tokio::test]
    async fn a_new_leader_proposes_the_cluster_quota_before_client_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = options(dir.path());
        config.quota_bytes = -1;
        let (events, _) = mpsc::channel(256);
        let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap();
        node.raw.campaign().unwrap();
        for kind in [
            MessageType::MsgRequestPreVoteResponse,
            MessageType::MsgRequestVoteResponse,
        ] {
            let mut vote = Message::default();
            vote.set_msg_type(kind);
            vote.from = 2;
            vote.to = 1;
            vote.term =
                node.raw.raft.term + u64::from(kind == MessageType::MsgRequestPreVoteResponse);
            let (tx, _reply) = oneshot::channel();
            node.handle(Event::Step(vote, tx), &mut vec![])
                .await
                .unwrap();
        }
        assert_eq!(node.raw.raft.state, StateRole::Leader);
        let (tx, _reply) = oneshot::channel();
        node.propose(quota_put(1), ClientResponse::Kv(tx)).unwrap();
        node.drive_ready().await.unwrap();
        let last = node.raw.raft.raft_log.last_index();
        let entries = node
            .raw
            .raft
            .raft_log
            .entries(1, u64::MAX, raft::GetEntriesContext::empty(false))
            .unwrap();
        let commands: Vec<_> = entries
            .iter()
            .filter(|entry| !entry.data.is_empty())
            .map(|entry| RaftCommand::decode(&entry.data).unwrap())
            .collect();
        assert_eq!(last, 3);
        assert!(matches!(
            commands[0],
            RaftCommand::SetQuota {
                bytes: -1,
                seed: true,
                ..
            }
        ));
        assert!(matches!(commands[1], RaftCommand::Etcd { .. }));
        // One proposal of a term is enough until it applies.
        node.propose_quota().unwrap();
        assert_eq!(node.raw.raft.raft_log.last_index(), 3);
    }
}
