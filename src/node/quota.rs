//! Leader proposals of the replicated cluster quota. See `state::quota`.
//!
//! The `--quota-backend-bytes` flag seeds the cluster quota and requests changes to it. A member
//! keeps a local record of its flag in the data directory. Raft, snapshots, and backups do not
//! carry that record. A restart with a changed flag, or with `--quota-reassert`, makes a pending
//! request with the wall-clock time. The member proposes it whenever it leads, until the cluster
//! quota equals it or holds a newer request. The newest request wins, so a member with an
//! unchanged old flag does not revert the cluster quota when it leads.
//!
//! A state that this binary created empty never applies a quota-checked entry under its flag.
//! Such an entry before the first `SetQuota` entry comes from an earlier binary. The member then
//! stops apply and asks the leader for a snapshot. A member also checkpoints through a first
//! cluster quota that follows such entries, so the leader sends later members a snapshot.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::*;
use crate::state::quota::ClusterQuota;

/// The file of the local flag record in the data directory.
const RECORD: &str = "quota-flag.json";

/// An operator request of this member: a flag value and the wall-clock time of the start that
/// saw it, in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Request {
    bytes: i64,
    requested_at: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    /// The last flag value that needs no proposal: the first flag of this directory, or a flag
    /// whose request the cluster took or replaced.
    acknowledged: i64,
    /// A request that this member must propose.
    pending: Option<Request>,
    /// This binary created the data directory empty. See `guard_quota`.
    created_empty: bool,
}

/// The options of `Quota::open`.
pub(super) struct Startup {
    /// The member ID. It orders requests with equal times.
    pub member: u64,
    /// The effective flag value. A non-positive value disables the quota.
    pub flag: i64,
    pub shared: bool,
    /// The Raft log and the state are empty.
    pub empty: bool,
    /// `--quota-reassert`: request the flag again with a new time.
    pub reassert: bool,
    /// The wall-clock time in milliseconds.
    pub now: u64,
}

/// The quota flag of this member and its proposal state.
pub(super) struct Quota {
    member: u64,
    flag: i64,
    /// The record file and its content. `None` in shared mode, which never proposes.
    record: Option<(PathBuf, Record)>,
    /// The index and term of the last `SetQuota` proposal of this member.
    proposed: Option<(u64, u64)>,
    /// The cluster quota that `observe_quota` last handled.
    observed: Option<Option<ClusterQuota>>,
    /// The applied index when the first cluster quota appeared. A checkpoint through it removes
    /// the entries of earlier binaries from the log.
    checkpoint: u64,
    /// The entry that waits for a snapshot. See `guard_quota`.
    blocked: Option<u64>,
    /// Simulation: the member proposes no `SetQuota`, like an earlier binary.
    #[cfg(test)]
    pub(super) hold: bool,
}

/// Return the `SetQuota` request and seed mark that a leader proposes, or `None`.
///
/// This is the "operator-change detection" policy. Without a cluster quota, a leader seeds it
/// from its flag, or sets its pending request. Otherwise it proposes only its own pending
/// request, and only when that request is newer than the cluster quota.
fn proposal(
    cluster: Option<ClusterQuota>,
    flag: i64,
    pending: Option<Request>,
    member: u64,
) -> Option<(ClusterQuota, bool)> {
    let request = |request: Request| ClusterQuota {
        bytes: request.bytes,
        requested_at: request.requested_at,
        member,
    };
    match (cluster, pending) {
        (None, None) => Some((
            ClusterQuota {
                bytes: flag,
                requested_at: 0,
                member,
            },
            true,
        )),
        (None, Some(pending)) => Some((request(pending), false)),
        (Some(row), Some(pending)) => {
            let request = request(pending);
            (row.bytes != request.bytes && request.key() > row.key()).then_some((request, false))
        }
        (Some(_), None) => None,
    }
}

impl Quota {
    /// Load the record of `dir` and update it for this start.
    ///
    /// A member without a record acknowledges its flag, so a new member requests no change. A
    /// flag that differs from the acknowledged value makes a request. A request for the same
    /// flag keeps its time, unless `reassert` asks for a new one.
    pub(super) fn open(dir: &Path, startup: Startup) -> Result<Self> {
        let mut quota = Self {
            member: startup.member,
            flag: startup.flag,
            record: None,
            proposed: None,
            observed: None,
            checkpoint: 0,
            blocked: None,
            #[cfg(test)]
            hold: false,
        };
        if startup.shared {
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
        let flag = startup.flag;
        let acknowledged = stored.map_or(flag, |record| record.acknowledged);
        let request = Request {
            bytes: flag,
            requested_at: startup.now,
        };
        let pending = match stored.and_then(|record| record.pending) {
            _ if startup.reassert => Some(request),
            Some(pending) if pending.bytes == flag => Some(pending),
            _ => (flag != acknowledged).then_some(request),
        };
        let record = Record {
            acknowledged,
            pending,
            created_empty: stored.map_or(startup.empty, |record| record.created_empty),
        };
        if stored != Some(record) {
            write_record(&path, &record)?;
        }
        quota.record = Some((path, record));
        Ok(quota)
    }

    fn pending(&self) -> Option<Request> {
        self.record.as_ref().and_then(|(_, record)| record.pending)
    }

    /// This binary created the data directory empty.
    pub(super) fn created_empty(&self) -> bool {
        self.record
            .as_ref()
            .is_some_and(|(_, record)| record.created_empty)
    }

    /// The index that a checkpoint must cover, or zero.
    pub(super) fn checkpoint(&self) -> u64 {
        self.checkpoint
    }

    /// Clear the pending request once the cluster quota equals it or holds a newer request.
    /// This needs only the current cluster quota, so batched apply and snapshot installs do not
    /// lose an acknowledgment.
    fn acknowledge(&mut self, cluster: Option<ClusterQuota>) -> Result<()> {
        let member = self.member;
        let Some((path, record)) = &mut self.record else {
            return Ok(());
        };
        let (Some(pending), Some(row)) = (record.pending, cluster) else {
            return Ok(());
        };
        let request = (pending.requested_at, member, pending.bytes);
        if row.bytes == pending.bytes || row.key() >= request {
            *record = Record {
                acknowledged: self.flag,
                pending: None,
                created_empty: record.created_empty,
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

/// The wall-clock time in milliseconds.
pub(super) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| time.as_millis() as u64)
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
        #[cfg(test)]
        if self.quota.hold {
            return Ok(());
        }
        let term = self.raw.raft.term;
        if let Some((index, proposed)) = self.quota.proposed {
            if proposed == term && self.state.applied < index {
                return Ok(());
            }
        }
        let Some((request, seed)) = proposal(
            self.state.cluster_quota,
            self.quota.flag,
            self.quota.pending(),
            self.quota.member,
        ) else {
            return Ok(());
        };
        let data = RaftCommand::SetQuota {
            id: uuid::Uuid::new_v4().to_string(),
            bytes: request.bytes,
            seed,
            requested_at: request.requested_at,
            member: request.member,
        }
        .encode()?;
        // The entry is small. It skips the memory admission check, so no client write goes
        // first. Raft drops it only during a leadership transfer; the next call tries again.
        if self.raw.propose(vec![], data).is_ok() {
            self.quota.proposed = Some((self.raw.raft.raft_log.last_index(), term));
            tracing::info!(
                bytes = request.bytes,
                requested_at = request.requested_at,
                seed,
                term,
                "propose the cluster quota"
            );
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
        // The first cluster quota follows entries of an earlier binary. Truncate them.
        if self.quota.observed == Some(None) && cluster.is_some() && self.state.seeded_late {
            self.quota.checkpoint = self.state.applied;
        }
        self.quota.observed = Some(cluster);
        self.quota.acknowledge(cluster)?;
        if cluster.is_some() && self.quota.blocked.take().is_some() {
            tracing::info!(
                applied = self.state.applied,
                "apply resumes with the cluster quota"
            );
        }
        // Shared mode warns when it reads the cluster claim.
        if self.state.quota_mismatch && self.quota.record.is_some() {
            tracing::warn!(
                configured = self.quota.flag,
                cluster = self.state.quota_bytes,
                pending = self.quota.pending().is_some(),
                "the cluster state sets the quota"
            );
        }
        Ok(())
    }

    /// Return true while a state created empty has no cluster quota. Apply then checks each
    /// batch with `guard_quota`, and runs one batch at a time.
    pub(super) fn quota_guarded(&self) -> bool {
        self.quota.created_empty() && self.state.cluster_quota.is_none()
    }

    /// Keep only the entries before the first quota-checked entry that precedes a `SetQuota`
    /// entry. A state created empty must not apply it under its own flag: an earlier binary
    /// wrote it, and other members applied it under their flags. Then ask the leader for a
    /// snapshot. The installed state replaces the decisions of that prefix.
    pub(super) fn guard_quota(&mut self, entries: &mut Vec<Entry>) -> Result<()> {
        let position = match self.quota_guarded() {
            true => crate::state::quota::unguarded(entries)?,
            false => None,
        };
        let Some(position) = position.filter(|&position| position == 0) else {
            if let Some(position) = position {
                entries.truncate(position);
            }
            self.quota.blocked = None;
            return Ok(());
        };
        let index = entries[position].index;
        entries.clear();
        if self.quota.blocked != Some(index) {
            self.quota.blocked = Some(index);
            tracing::error!(
                index,
                "entry needs the cluster quota; apply waits for a snapshot from the leader"
            );
        }
        // Raft drops the request without a leader or with a request in flight. The next
        // schedule asks again.
        let _ = self.raw.request_snapshot();
        Ok(())
    }

    /// The quota fields of `/status`.
    pub(super) fn quota_status(&self) -> Value {
        json!({
            "bytes": self.state.quota_bytes,
            "cluster": self.state.cluster_quota,
            "flag_bytes": self.quota.flag,
            "flag_mismatch": self.state.quota_mismatch,
            "flag_pending": self.quota.pending(),
            "blocked_index": self.quota.blocked,
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
pub(super) mod tests {
    use super::super::tests::options;
    use super::*;
    use raft::eraftpb::HardState;

    pub(in crate::node) fn row(bytes: i64, requested_at: u64, member: u64) -> ClusterQuota {
        ClusterQuota {
            bytes,
            requested_at,
            member,
        }
    }

    pub(in crate::node) fn set_quota(bytes: i64, requested_at: u64, member: u64) -> RaftCommand {
        RaftCommand::SetQuota {
            id: format!("set-{bytes}-{requested_at}"),
            bytes,
            seed: requested_at == 0,
            requested_at,
            member,
        }
    }

    fn startup(flag: i64, now: u64, empty: bool, reassert: bool) -> Startup {
        Startup {
            member: 1,
            flag,
            shared: false,
            empty,
            reassert,
            now,
        }
    }

    #[test]
    fn a_leader_seeds_or_proposes_only_a_newer_request() {
        let pending = |bytes, requested_at| {
            Some(Request {
                bytes,
                requested_at,
            })
        };
        // No cluster quota: seed it, or set the pending request.
        assert_eq!(proposal(None, 5, None, 1), Some((row(5, 0, 1), true)));
        assert_eq!(
            proposal(None, 5, pending(5, 9), 1),
            Some((row(5, 9, 1), false))
        );
        // An unchanged old flag does not replace the cluster quota.
        assert_eq!(proposal(Some(row(9, 0, 2)), 5, None, 1), None);
        // The newer request wins. Equal times compare the member ID.
        assert_eq!(
            proposal(Some(row(9, 3, 2)), 5, pending(5, 4), 1),
            Some((row(5, 4, 1), false))
        );
        assert_eq!(proposal(Some(row(9, 5, 2)), 5, pending(5, 4), 1), None);
        assert_eq!(proposal(Some(row(9, 4, 2)), 5, pending(5, 4), 1), None);
        assert_eq!(
            proposal(Some(row(9, 4, 2)), 5, pending(5, 4), 3),
            Some((row(5, 4, 3), false))
        );
        assert_eq!(proposal(Some(row(5, 1, 2)), 5, pending(5, 4), 1), None);
        // A disabled quota is a value like any other.
        assert_eq!(
            proposal(Some(row(9, 3, 2)), -1, pending(-1, 4), 1),
            Some((row(-1, 4, 1), false))
        );
    }

    #[test]
    fn a_flag_request_keeps_its_time_until_the_cluster_takes_it() {
        let dir = tempfile::tempdir().unwrap();
        let open = |flag, now, empty, reassert| {
            Quota::open(dir.path(), startup(flag, now, empty, reassert)).unwrap()
        };
        let request = |bytes, requested_at| {
            Some(Request {
                bytes,
                requested_at,
            })
        };
        // A member without a record requests no change.
        let quota = open(100, 10, true, false);
        assert_eq!(quota.pending(), None);
        assert!(quota.created_empty());
        // Restarts before the member leads keep the latest flag and the time of its first start.
        assert_eq!(open(200, 20, false, false).pending(), request(200, 20));
        assert_eq!(open(200, 30, false, false).pending(), request(200, 20));
        let mut quota = open(300, 40, false, false);
        assert_eq!(quota.pending(), request(300, 40));
        assert!(quota.created_empty());
        // An older request with another value does not clear it. A newer one does.
        quota.acknowledge(Some(row(100, 39, 2))).unwrap();
        assert_eq!(quota.pending(), request(300, 40));
        quota.acknowledge(Some(row(250, 41, 2))).unwrap();
        assert_eq!(quota.pending(), None);
        assert_eq!(open(300, 50, false, false).pending(), None);
        // `--quota-reassert` requests the same flag again with a new time.
        assert_eq!(open(300, 60, false, true).pending(), request(300, 60));
        // Back to the acknowledged value: nothing is pending.
        assert_eq!(open(100, 70, false, false).pending(), request(100, 70));
        assert_eq!(open(300, 80, false, false).pending(), None);
        // Shared mode keeps no record.
        let shared = tempfile::tempdir().unwrap();
        let mut startup = startup(1, 1, true, false);
        startup.shared = true;
        assert!(Quota::open(shared.path(), startup)
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
    pub(in crate::node) fn append_committed(
        dir: &std::path::Path,
        first: u64,
        commands: &[RaftCommand],
    ) {
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
    pub(in crate::node) async fn apply_committed(node: &mut Node) {
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
        source
            .apply_set_quota(1, row(3000, 5, 2), false)
            .await
            .unwrap();
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
        assert_eq!(node.state.cluster_quota, Some(row(3000, 5, 2)));
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

    async fn open_node(dir: &Path, flag: i64, reassert: bool) -> Node {
        let mut config = options(dir);
        config.quota_bytes = flag;
        config.quota_reassert = reassert;
        let (events, _) = mpsc::channel(256);
        Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap()
    }

    /// The request that member 1 would propose when it leads.
    fn leader_request(node: &Node) -> Option<(ClusterQuota, bool)> {
        proposal(
            node.state.cluster_quota,
            node.quota.flag,
            node.quota.pending(),
            1,
        )
    }

    /// A follower requested 200 and then applies 100, 200, and 300 in one batch. The newer 300
    /// replaces its request, so it does not propose 200 when it leads.
    #[tokio::test]
    async fn a_batched_newer_quota_replaces_a_pending_request() {
        let dir = tempfile::tempdir().unwrap();
        Quota::open(dir.path(), startup(100, 10, false, false)).unwrap();
        let pending = Quota::open(dir.path(), startup(200, 20, false, false)).unwrap();
        assert!(pending.pending().is_some());
        let entries = [
            set_quota(100, 0, 2),
            set_quota(200, 20, 1),
            set_quota(300, 30, 2),
        ];
        append_committed(dir.path(), 1, &entries);
        let mut node = open_node(dir.path(), 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(node.state.cluster_quota, Some(row(300, 30, 2)));
        assert_eq!(node.quota.pending(), None);
        assert_eq!(leader_request(&node), None);
    }

    /// The cluster took 300 while a member was offline. The member then restarts with 200. Its
    /// request is newer, so it stays pending through the replay of 300.
    #[tokio::test]
    async fn a_newer_request_survives_the_replay_of_an_older_quota() {
        let dir = tempfile::tempdir().unwrap();
        Quota::open(dir.path(), startup(100, 10, false, false)).unwrap();
        append_committed(
            dir.path(),
            1,
            &[set_quota(100, 0, 2), set_quota(300, 30, 2)],
        );
        Quota::open(dir.path(), startup(200, 40, false, false)).unwrap();
        let mut node = open_node(dir.path(), 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(node.state.cluster_quota, Some(row(300, 30, 2)));
        assert_eq!(leader_request(&node), Some((row(200, 40, 1), false)));
    }

    /// Restore a native backup of a state with the cluster quota `quota` into `target`.
    pub(in crate::node) async fn restore_with(dir: &Path, target: &Path, quota: ClusterQuota) {
        let mut source = StateMachine::open(&dir.join("source")).await.unwrap();
        source.apply_set_quota(1, quota, false).await.unwrap();
        let image = source.snapshot_data().await.unwrap();
        source.close().await.unwrap();
        let mut backup = image.clone();
        use sha2::Digest as _;
        backup.extend_from_slice(&sha2::Sha256::digest(&image));
        let path = dir.join("backup");
        std::fs::write(&path, backup).unwrap();
        let members = crate::cluster::Store::initial(&options(target).peers);
        StateMachine::restore_backup(&path, target, 1, "worker-test", &members).unwrap();
    }

    /// A snapshot install moves a member past its pending request. A newer quota in the snapshot
    /// replaces the request. An older one does not.
    #[tokio::test]
    async fn a_snapshot_install_resolves_a_pending_request_by_time() {
        for (installed, pending) in [(row(300, 30, 2), false), (row(300, 10, 2), true)] {
            let dir = tempfile::tempdir().unwrap();
            let target = dir.path().join("member");
            restore_with(dir.path(), &target, installed).await;
            Quota::open(&target, startup(100, 5, false, false)).unwrap();
            Quota::open(&target, startup(200, 20, false, false)).unwrap();
            let mut node = open_node(&target, 200, false).await;
            apply_committed(&mut node).await;
            assert_eq!(node.state.cluster_quota, Some(installed));
            assert_eq!(node.quota.pending().is_some(), pending, "{installed:?}");
            assert_eq!(leader_request(&node).is_some(), pending, "{installed:?}");
        }
    }

    /// An operator retries a request with the same flag and data directory. Only
    /// `--quota-reassert` makes a new request.
    #[tokio::test]
    async fn quota_reassert_requests_the_same_flag_again() {
        let dir = tempfile::tempdir().unwrap();
        Quota::open(dir.path(), startup(200, 10, false, false)).unwrap();
        append_committed(
            dir.path(),
            1,
            &[set_quota(200, 0, 1), set_quota(300, 50, 2)],
        );
        let mut node = open_node(dir.path(), 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(leader_request(&node), None);
        drop(node);
        let mut node = open_node(dir.path(), 200, false).await;
        apply_committed(&mut node).await;
        assert_eq!(leader_request(&node), None);
        drop(node);
        let mut node = open_node(dir.path(), 200, true).await;
        apply_committed(&mut node).await;
        let (request, seed) = leader_request(&node).unwrap();
        assert_eq!((request.bytes, seed), (200, false));
        assert!(request.key() > row(300, 50, 2).key());
    }

    fn txn_put(n: u64) -> RaftCommand {
        let RaftCommand::Etcd { etcd, .. } = quota_put(n) else {
            unreachable!()
        };
        RaftCommand::Etcd {
            id: format!("txn-{n}"),
            origin: 1,
            etcd: RequestOp {
                request: Some(crate::proto::etcdserverpb::request_op::Request::RequestTxn(
                    crate::proto::etcdserverpb::TxnRequest {
                        success: vec![etcd],
                        ..Default::default()
                    },
                )),
            },
        }
    }

    fn lease_grant(n: u64) -> RaftCommand {
        RaftCommand::Lease {
            id: format!("grant-{n}"),
            origin: 1,
            lease: LeaseCommand::Grant {
                id: n as i64,
                ttl: 60,
            },
        }
    }

    /// A state that this binary created empty must not apply the entries of an earlier binary
    /// under its own flag: a Put, a transaction with a Put, or a lease grant. It waits for a
    /// snapshot, and catches up after the install.
    #[tokio::test]
    async fn a_new_member_waits_for_a_snapshot_before_entries_of_an_earlier_binary() {
        for first in [quota_put(1), txn_put(1), lease_grant(1)] {
            let dir = tempfile::tempdir().unwrap();
            // The member starts empty, then receives entries from before the first seed.
            drop(open_node(dir.path(), 3000, false).await);
            let mut entries = vec![first];
            entries.extend((2..=40).map(quota_put));
            entries.push(set_quota(1 << 20, 0, 2));
            append_committed(dir.path(), 1, &entries);
            let mut node = open_node(dir.path(), 3000, false).await;
            assert!(node.quota.created_empty());
            for _ in 0..5 {
                node.drive_ready().await.unwrap();
                node.drain_log().await.unwrap();
                node.drain_apply().await.unwrap();
            }
            assert_eq!(node.state.applied, 0);
            assert_eq!(node.quota.blocked, Some(1));

            // Another member applied the same entries under the decisions of the cluster.
            let mut source = StateMachine::open(&dir.path().join("source"))
                .await
                .unwrap();
            source.set_quota_flag(1 << 20);
            let members = crate::cluster::Store::initial(&options(dir.path()).peers);
            source.members = std::sync::Arc::new(members.clone());
            source.members_unsaved = true;
            let log: Vec<_> = (1..)
                .zip(&entries)
                .map(|(index, command)| Entry {
                    index,
                    term: 1,
                    data: command.encode().unwrap(),
                    ..Default::default()
                })
                .collect();
            let header = super::super::apply::Header {
                cluster: 0,
                member: 2,
                term: 1,
            };
            super::super::apply::apply_entries(&mut source, log, header)
                .await
                .unwrap();
            let mut snapshot = raft::eraftpb::Snapshot {
                data: source.snapshot_data().await.unwrap(),
                ..Default::default()
            };
            snapshot.mut_metadata().index = 41;
            snapshot.mut_metadata().term = 1;
            snapshot.mut_metadata().set_conf_state(members.conf());
            let revision = source.revision;
            source.close().await.unwrap();

            // The leader appears. The member asks it for a snapshot and installs it.
            let mut heartbeat = Message::default();
            heartbeat.set_msg_type(MessageType::MsgHeartbeat);
            heartbeat.from = 2;
            heartbeat.to = 1;
            heartbeat.term = 1;
            node.raw.step(heartbeat).unwrap();
            node.drive_ready().await.unwrap();
            assert_eq!(node.raw.raft.pending_request_snapshot, 41);
            let mut message = Message::default();
            message.set_msg_type(MessageType::MsgSnapshot);
            message.from = 2;
            message.to = 1;
            message.term = 1;
            message.set_snapshot(snapshot);
            node.raw.step(message).unwrap();
            apply_committed(&mut node).await;
            assert_eq!(node.state.applied, 41);
            assert_eq!(node.state.cluster_quota, Some(row(1 << 20, 0, 2)));
            assert_eq!(node.state.revision, revision);
            assert!(node.worker.state().alarms.is_empty());
            assert_eq!(node.quota.blocked, None);
        }
    }
}
