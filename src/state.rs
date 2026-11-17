use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use raft::eraftpb::Snapshot;
use serde::{Deserialize, Serialize};
use slatedb::config::{Settings, WriteOptions};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::{Db, WriteBatch};

use crate::mvcc::{self, Outcome};
use crate::proto::etcdserverpb::{CompactionRequest, CompactionResponse, RequestOp};
use crate::{Command, Operation, Reply};

mod backup;
pub(crate) mod maintenance;

const APPLIED: &[u8] = b"meta/applied";
const DATA: &[u8] = b"data/";

#[derive(Deserialize, Serialize)]
struct SnapshotData {
    version: u32,
    applied: u64,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct StateMachine {
    pub(crate) db: Arc<Db>,
    pub auth: crate::auth::Store,
    pub members: crate::cluster::Store,
    admin: slatedb::admin::Admin,
    pub quota: u64,
    pub alarms: Vec<crate::proto::etcdserverpb::AlarmMember>,
    pub applied: u64,
    pub durable_applied: u64,
    pub revision: i64,
    pub compact_revision: i64,
    pub leases: std::collections::BTreeMap<i64, crate::lease::LeaseRecord>,
}

impl StateMachine {
    pub async fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(dir)?);
        Self::open_store(object_store, "state").await
    }

    pub async fn open_store(
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
        prefix: &str,
    ) -> Result<Self> {
        let mut settings = Settings {
            wal_enabled: false,
            flush_interval: None,
            object_store_max_retries: Some(3),
            manifest_poll_interval: Duration::from_millis(100),
            ..Settings::default()
        };
        // Frequent checkpoints need prompt L0 compaction and manifest refreshes.
        if let Some(compactor) = settings.compactor_options.as_mut() {
            compactor.poll_interval = Duration::from_millis(100);
            compactor.commit_compacted_interval = Duration::from_millis(100);
            if let Some(worker) = compactor.worker.as_mut() {
                worker.compactions_poll_interval = Duration::from_millis(100);
            }
        }
        let admin = slatedb::admin::Admin::builder(prefix, object_store.clone()).build();
        let db = Db::builder(prefix, object_store)
            .with_settings(settings)
            .build()
            .await?;
        let db = Arc::new(db);
        let result = async {
            let applied = match db.get(APPLIED).await? {
                Some(bytes) => {
                    u64::from_be_bytes(bytes.as_ref().try_into().context("invalid apply marker")?)
                }
                None => 0,
            };
            ensure!(
                db.snapshot()?.seq() == applied,
                "SlateDB sequence and apply marker differ"
            );
            let revision = read_revision(&db).await?;
            let compact_revision = read_compact_revision(&db).await?;
            ensure!(
                compact_revision <= revision,
                "compaction exceeds current revision"
            );
            let leases = crate::lease::load(&db).await?;
            let auth = match db.get(crate::auth::KEY).await? {
                Some(data) => serde_json::from_slice(&data)?,
                None => crate::auth::Store::default(),
            };
            let alarms = match db.get(crate::maintenance::ALARMS).await? {
                Some(data) => serde_json::from_slice(&data)?,
                None => vec![],
            };
            let members = match db.get(crate::cluster::KEY).await? {
                Some(data) => serde_json::from_slice(&data)?,
                None => crate::cluster::Store::default(),
            };
            Ok(Self {
                members,
                admin,
                quota: 0,
                alarms,
                auth,
                db: Arc::clone(&db),
                applied,
                durable_applied: applied,
                revision,
                compact_revision,
                leases,
            })
        }
        .await;
        if result.is_err() {
            let _ = db
                .close_with_options(slatedb::config::CloseOptions { flush_type: None })
                .await;
        }
        result
    }

    pub(crate) async fn close(&self) -> Result<()> {
        // Raft keeps acknowledged writes durable. Shutdown does not require a new checkpoint.
        self.db
            .close_with_options(slatedb::config::CloseOptions { flush_type: None })
            .await?;
        Ok(())
    }

    pub fn snapshot_conf(bytes: &[u8]) -> Result<Option<raft::eraftpb::ConfState>> {
        let data: SnapshotData = bincode::deserialize(bytes)?;
        for (key, value) in data.rows {
            if key == crate::cluster::KEY {
                let members: crate::cluster::Store = serde_json::from_slice(&value)?;
                return Ok(Some(members.conf()));
            }
        }
        Ok(None)
    }

    pub async fn apply_cluster(
        &mut self,
        index: u64,
        request: &crate::cluster::Command,
        credentials: &crate::auth::Credentials,
    ) -> Result<Outcome<serde_json::Value>> {
        ensure!(index == self.applied + 1, "non-contiguous cluster apply");
        let allowed = if matches!(
            request,
            crate::cluster::Command::List(_) | crate::cluster::Command::Publish { .. }
        ) {
            Ok(())
        } else {
            self.auth.admin(credentials)
        };
        let mut members = self.members.clone();
        let result = allowed.and_then(|()| members.validate(request)).map(|()| {
            members.apply(request);
            let mut response = serde_json::json!({"header": mvcc::header(self.revision),
                "members": members.members.values().collect::<Vec<_>>()});
            if let crate::cluster::Command::Add(member) = request {
                response["member"] = serde_json::json!(member);
            }
            response
        });
        let mut batch = WriteBatch::new();
        batch.put(crate::cluster::KEY, serde_json::to_vec(&members)?);
        batch.put(APPLIED, index.to_be_bytes());
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        self.members = members;
        self.applied = index;
        Ok(Outcome { result })
    }

    pub async fn apply_auth(
        &mut self,
        index: u64,
        credentials: &crate::auth::Credentials,
        command: &crate::auth::Command,
    ) -> Result<Outcome<serde_json::Value>> {
        ensure!(index == self.applied + 1, "non-contiguous auth apply");
        let mut next = self.auth.clone();
        let result = next.execute(credentials, command, self.revision);
        let mut batch = WriteBatch::new();
        if result.is_ok() {
            batch.put(crate::auth::KEY, serde_json::to_vec(&next)?);
        }
        batch.put(APPLIED, index.to_be_bytes());
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        self.applied = index;
        if result.is_ok() {
            self.auth = next;
        }
        Ok(Outcome { result })
    }

    pub async fn read_etcd_authorized(
        &self,
        request: &RequestOp,
        credentials: Option<&crate::auth::Credentials>,
    ) -> Result<Outcome> {
        if let Some(credentials) = credentials {
            if let Err(error) = self.auth.kv(credentials, request) {
                return Ok(Outcome { result: Err(error) });
            }
        }
        self.read_etcd(request).await
    }

    pub async fn authorize(
        &self,
        auth: &crate::auth::Credentials,
        command: &crate::RaftCommand,
    ) -> std::result::Result<(), mvcc::KvError> {
        use crate::lease::LeaseCommand as L;
        match command {
            crate::RaftCommand::Etcd { etcd, .. } => {
                self.auth.kv(auth, etcd)?;
                let mut requests = vec![etcd];
                let mut leases = std::collections::BTreeSet::new();
                while let Some(request) = requests.pop() {
                    use crate::proto::etcdserverpb::request_op::Request as Op;
                    match &request.request {
                        Some(Op::RequestPut(put)) if put.lease != 0 => {
                            leases.insert(put.lease);
                        }
                        Some(Op::RequestTxn(txn)) => {
                            requests.extend(txn.success.iter().chain(&txn.failure));
                        }
                        _ => (),
                    }
                }
                if self.auth.enabled && !leases.is_empty() {
                    use prost::Message;
                    let mut scan = self
                        .db
                        .scan_prefix(b"etcd/current/", ..)
                        .await
                        .map_err(mvcc::KvError::storage)?;
                    while let Some(row) = scan.next().await.map_err(mvcc::KvError::storage)? {
                        let kv = crate::proto::mvccpb::KeyValue::decode(row.value)
                            .map_err(mvcc::KvError::storage)?;
                        if leases.contains(&kv.lease) {
                            self.auth.range(auth, &kv.key, &[], true)?;
                        }
                    }
                }
                Ok(())
            }
            crate::RaftCommand::Lease { lease, .. } => {
                self.auth.authenticated(auth)?;
                let (id, write) = match lease {
                    L::KeepAlive { id } | L::Revoke { id } => (*id, true),
                    L::TimeToLive { id, keys: true } => (*id, false),
                    L::TimeToLive { keys: false, .. } => return Ok(()),
                    L::Grant { .. } | L::List => return Ok(()),
                    L::Expire { .. } => return Err(crate::auth::denied()),
                };
                if self.auth.enabled {
                    let mut scan = self
                        .db
                        .scan_prefix(b"etcd/current/", ..)
                        .await
                        .map_err(mvcc::KvError::storage)?;
                    use prost::Message;
                    while let Some(row) = scan.next().await.map_err(mvcc::KvError::storage)? {
                        let kv = crate::proto::mvccpb::KeyValue::decode(row.value)
                            .map_err(mvcc::KvError::storage)?;
                        if kv.lease == id {
                            self.auth.range(auth, &kv.key, &[], write)?;
                        }
                    }
                }
                Ok(())
            }
            crate::RaftCommand::Compact { .. } => self.auth.admin(auth),
            _ => Err(crate::auth::denied()),
        }
    }

    pub async fn apply(&mut self, index: u64, command: Option<&Command>) -> Result<Reply> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let mut batch = WriteBatch::new();
        let mut values = vec![];
        // A small overlay preserves request order within an atomic multi-operation batch.
        let mut overlay = std::collections::BTreeMap::<String, Option<String>>::new();
        if let Some(command) = command {
            for operation in &command.request.operations {
                match operation {
                    Operation::Put { key, value } => {
                        batch.put(data_key(key), value.as_bytes());
                        overlay.insert(key.clone(), Some(value.clone()));
                        values.push(None);
                    }
                    Operation::Delete { key } => {
                        batch.delete(data_key(key));
                        overlay.insert(key.clone(), None);
                        values.push(None);
                    }
                    Operation::Get { key } => {
                        let value = if let Some(value) = overlay.get(key) {
                            value.clone()
                        } else {
                            self.db
                                .get(data_key(key))
                                .await?
                                .map(|v| String::from_utf8(v.to_vec()))
                                .transpose()?
                        };
                        values.push(value);
                    }
                }
            }
        }
        batch.put(APPLIED, index.to_be_bytes());
        // Raft already made this command durable. Waiting for SlateDB here would wait for L0.
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        self.applied = index;
        Ok(Reply { index, values })
    }

    pub async fn snapshot_data(&mut self) -> Result<Vec<u8>> {
        let data = self.snapshot_job()?.await?;
        self.durable_applied = self.applied;
        Ok(data)
    }

    pub async fn read_etcd(&self, request: &RequestOp) -> Result<Outcome> {
        let result = mvcc::prepare(
            &self.db,
            self.revision,
            self.compact_revision,
            request,
            &self.leases,
        )
        .await;
        if let Err(error) = &result {
            ensure!(
                error.code != tonic::Code::Internal as i32,
                "{}",
                error.message
            );
        }
        Ok(Outcome {
            result: result.map(|(response, _, _)| response),
        })
    }

    pub async fn apply_etcd_quota(
        &mut self,
        index: u64,
        request: &RequestOp,
        origin: u64,
    ) -> Result<Outcome> {
        let corrupt = self.alarms.iter().any(|a| a.alarm == 2);
        let full = crate::maintenance::has_put(request)
            && (self.alarms.iter().any(|a| a.alarm == 1)
                || (self.quota > 0 && self.logical_size().await? >= self.quota));
        if corrupt || full {
            let mut batch = WriteBatch::new();
            if full && !self.alarms.iter().any(|a| a.alarm == 1) {
                self.alarms.push(crate::proto::etcdserverpb::AlarmMember {
                    member_id: origin,
                    alarm: 1,
                });
            }
            batch.put(
                crate::maintenance::ALARMS,
                serde_json::to_vec(&self.alarms)?,
            );
            batch.put(APPLIED, index.to_be_bytes());
            self.db
                .write_with_options(batch, &WriteOptions { seqnum: index })
                .await?;
            self.applied = index;
            return Ok(Outcome {
                result: Err(if corrupt {
                    mvcc::KvError::new(tonic::Code::DataLoss, "corrupt cluster")
                } else {
                    mvcc::KvError::new(
                        tonic::Code::ResourceExhausted,
                        "mvcc: database space exceeded",
                    )
                }),
            });
        }
        self.apply_etcd(index, request).await
    }

    pub async fn apply_etcd(&mut self, index: u64, request: &RequestOp) -> Result<Outcome> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let (result, mut batch, revision) = match mvcc::prepare(
            &self.db,
            self.revision,
            self.compact_revision,
            request,
            &self.leases,
        )
        .await
        {
            Ok((response, batch, revision)) => (Ok(response), batch, revision),
            Err(error) => {
                ensure!(
                    error.code != tonic::Code::Internal as i32,
                    "{}",
                    error.message
                );
                (Err(error), WriteBatch::new(), self.revision)
            }
        };
        batch.put(APPLIED, index.to_be_bytes());
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        self.applied = index;
        self.revision = revision;
        Ok(Outcome { result })
    }

    pub async fn apply_compaction(
        &mut self,
        index: u64,
        request: &CompactionRequest,
    ) -> Result<Outcome<CompactionResponse>> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        if self.alarms.iter().any(|alarm| alarm.alarm == 2) {
            self.apply(index, None).await?;
            return Ok(Outcome {
                result: Err(mvcc::KvError::new(tonic::Code::DataLoss, "corrupt cluster")),
            });
        }
        let (result, mut batch) =
            match mvcc::prepare_compaction(&self.db, self.revision, self.compact_revision, request)
                .await
            {
                Ok((response, batch)) => (Ok(response), batch),
                Err(error) => {
                    ensure!(
                        error.code != tonic::Code::Internal as i32,
                        "{}",
                        error.message
                    );
                    (Err(error), WriteBatch::new())
                }
            };
        if result.is_ok() {
            if let Ok(hash) = self.hash_kv(request.revision).await {
                let mut hashes = self.hash_cache().await?;
                hashes.push(hash);
                if hashes.len() > 10 {
                    hashes.remove(0);
                }
                batch.put(b"etcd/hash-cache", serde_json::to_vec(&hashes)?);
            }
        }
        crate::failpoint::hit("compact-before-apply");
        batch.put(APPLIED, index.to_be_bytes());
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        self.applied = index;
        if result.is_ok() {
            self.compact_revision = request.revision;
            if request.physical {
                self.db.flush().await?;
                self.durable_applied = self.applied;
            }
        }
        crate::failpoint::hit("compact-after-apply");
        Ok(Outcome { result })
    }

    pub async fn apply_lease(
        &mut self,
        index: u64,
        command: &crate::lease::LeaseCommand,
    ) -> Result<Outcome<crate::lease::LeaseReply>> {
        self.apply_lease_from(index, command, 0).await
    }

    pub async fn apply_lease_from(
        &mut self,
        index: u64,
        command: &crate::lease::LeaseCommand,
        origin: u64,
    ) -> Result<Outcome<crate::lease::LeaseReply>> {
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        use crate::lease::LeaseCommand as L;
        if matches!(command, L::Expire { .. }) {
            crate::failpoint::hit("lease-before-expire");
        }

        let corrupt = self.alarms.iter().any(|a| a.alarm == 2)
            && matches!(
                command,
                L::Grant { .. } | L::Revoke { .. } | L::Expire { .. }
            );
        let full = matches!(command, L::Grant { .. })
            && (self.alarms.iter().any(|a| a.alarm == 1)
                || (self.quota > 0 && self.logical_size().await? >= self.quota));
        if corrupt || full {
            if full && !self.alarms.iter().any(|a| a.alarm == 1) {
                self.alarms.push(crate::proto::etcdserverpb::AlarmMember {
                    member_id: origin,
                    alarm: 1,
                });
            }
            let mut batch = WriteBatch::new();
            batch.put(
                crate::maintenance::ALARMS,
                serde_json::to_vec(&self.alarms)?,
            );
            batch.put(APPLIED, index.to_be_bytes());
            self.db
                .write_with_options(batch, &WriteOptions { seqnum: index })
                .await?;
            self.applied = index;
            return Ok(Outcome {
                result: Err(if corrupt {
                    mvcc::KvError::new(tonic::Code::DataLoss, "corrupt cluster")
                } else {
                    mvcc::KvError::new(
                        tonic::Code::ResourceExhausted,
                        "mvcc: database space exceeded",
                    )
                }),
            });
        }
        let (result, mut batch, revision, change) = match crate::lease::prepare(
            &self.db,
            self.revision,
            self.compact_revision,
            index,
            &self.leases,
            command,
        )
        .await
        {
            Ok((reply, batch, revision, change)) => (Ok(reply), batch, revision, change),
            Err(error) => {
                ensure!(
                    error.code != tonic::Code::Internal as i32,
                    "{}",
                    error.message
                );
                (Err(error), WriteBatch::new(), self.revision, None)
            }
        };
        batch.put(APPLIED, index.to_be_bytes());
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        self.applied = index;
        self.revision = revision;
        if let Some((id, record)) = change {
            if let Some(record) = record {
                self.leases.insert(id, record);
            } else {
                self.leases.remove(&id);
            }
        }
        Ok(Outcome { result })
    }

    pub async fn watch(
        &self,
        request: &crate::proto::etcdserverpb::WatchCreateRequest,
        through: Option<i64>,
    ) -> Result<crate::watch::WatchPage> {
        crate::watch::read(
            &self.db,
            through.unwrap_or(self.revision).min(self.revision),
            self.compact_revision,
            request,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.message))
    }

    /// Pin the applied prefix now, then flush and serialize it outside the Raft loop.
    pub fn snapshot_job(
        &self,
    ) -> Result<impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'static> {
        let snapshot = self.db.snapshot()?;
        let db = Arc::clone(&self.db);
        let applied = self.applied;
        let members = serde_json::to_vec(&self.members)?;
        ensure!(
            snapshot.seq() == applied,
            "snapshot does not match the apply marker"
        );
        Ok(async move {
            db.flush().await?;
            let mut rows = vec![];
            let mut iterator = snapshot.scan_prefix(b"", ..).await?;
            while let Some(row) = iterator.next().await? {
                rows.push((row.key.to_vec(), row.value.to_vec()));
            }
            rows.retain(|(key, _)| key != crate::cluster::KEY);
            rows.push((crate::cluster::KEY.to_vec(), members));
            Ok(bincode::serialize(&SnapshotData {
                version: 2,
                applied,
                rows,
            })?)
        })
    }

    pub async fn install_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let index = snapshot.get_metadata().index;
        if index <= self.applied {
            return Ok(());
        }
        let data: SnapshotData = bincode::deserialize(snapshot.get_data())?;
        ensure!(
            (data.version == 1 || data.version == 2) && data.applied == index,
            "invalid snapshot metadata"
        );
        let mut batch = WriteBatch::new();
        let mut iterator = self.db.scan_prefix(b"", ..).await?;
        while let Some(row) = iterator.next().await? {
            batch.delete(row.key);
        }
        for (key, value) in data.rows {
            ensure!(
                key.starts_with(DATA) || key.starts_with(b"etcd/") || key == APPLIED,
                "invalid snapshot key"
            );
            batch.put(key, value);
        }
        crate::failpoint::hit("snapshot-before-install");
        batch.put(APPLIED, index.to_be_bytes());
        self.db
            .write_with_options(batch, &WriteOptions { seqnum: index })
            .await?;
        crate::failpoint::hit("snapshot-after-install-write");
        // The durable Raft snapshot lets a restart repeat this atomic replacement.
        self.db.flush().await?;
        crate::failpoint::hit("snapshot-after-install-flush");
        self.applied = index;
        self.durable_applied = index;
        self.revision = read_revision(&self.db).await?;
        self.compact_revision = read_compact_revision(&self.db).await?;
        self.leases = crate::lease::load(&self.db).await?;
        self.alarms = match self.db.get(crate::maintenance::ALARMS).await? {
            Some(data) => serde_json::from_slice(&data)?,
            None => vec![],
        };
        self.members = match self.db.get(crate::cluster::KEY).await? {
            Some(data) => serde_json::from_slice(&data)?,
            None => crate::cluster::Store::default(),
        };
        self.auth = match self.db.get(crate::auth::KEY).await? {
            Some(data) => serde_json::from_slice(&data)?,
            None => crate::auth::Store::default(),
        };
        ensure!(
            self.compact_revision <= self.revision,
            "compaction exceeds current revision"
        );
        Ok(())
    }

    pub async fn local_value(&self, key: &str) -> Result<Option<String>> {
        self.db
            .get(data_key(key))
            .await?
            .map(|value| String::from_utf8(value.to_vec()).map_err(Into::into))
            .transpose()
    }
}

async fn read_revision(db: &Db) -> Result<i64> {
    Ok(match db.get(mvcc::REVISION).await? {
        Some(bytes) => i64::from_be_bytes(bytes.as_ref().try_into().context("invalid revision")?),
        None => 1,
    })
}

async fn read_compact_revision(db: &Db) -> Result<i64> {
    Ok(match db.get(mvcc::COMPACT_REVISION).await? {
        Some(bytes) => i64::from_be_bytes(
            bytes
                .as_ref()
                .try_into()
                .context("invalid compact revision")?,
        ),
        None => -1,
    })
}

fn data_key(key: &str) -> Vec<u8> {
    [DATA, key.as_bytes()].concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::etcdserverpb::request_op::Request as Op;
    use crate::proto::etcdserverpb::response_op::Response as Resp;
    use crate::proto::etcdserverpb::{DeleteRangeRequest, PutRequest, RangeRequest};

    fn put(key: &[u8], value: &[u8]) -> RequestOp {
        RequestOp {
            request: Some(Op::RequestPut(PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                ..Default::default()
            })),
        }
    }

    async fn read(state: &StateMachine, key: &[u8], revision: i64) -> Outcome {
        state
            .read_etcd(&RequestOp {
                request: Some(Op::RequestRange(RangeRequest {
                    key: key.to_vec(),
                    revision,
                    ..Default::default()
                })),
            })
            .await
            .unwrap()
    }

    async fn history_len(state: &StateMachine) -> usize {
        let mut scan = state.db.scan_prefix(b"etcd/history/", ..).await.unwrap();
        let mut count = 0;
        while scan.next().await.unwrap().is_some() {
            count += 1;
        }
        count
    }

    #[tokio::test]
    async fn compaction_removes_history_and_preserves_boundary_versions() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        let delete = RequestOp {
            request: Some(Op::RequestDeleteRange(DeleteRangeRequest {
                key: b"a".to_vec(),
                ..Default::default()
            })),
        };
        for (index, request) in [
            put(b"a", b"first"),
            put(b"b", b"stable"),
            put(b"a", b"second"),
            delete,
            put(b"a", b"third"),
            put(b"a", b"fourth"),
        ]
        .iter()
        .enumerate()
        {
            state
                .apply_etcd(index as u64 + 1, request)
                .await
                .unwrap()
                .result
                .unwrap();
        }
        assert_eq!(history_len(&state).await, 6);
        state
            .apply_compaction(
                7,
                &CompactionRequest {
                    revision: 5,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        assert_eq!(history_len(&state).await, 4);
        let deleted = read(&state, b"a", 5).await.result.unwrap();
        assert!(matches!(deleted.response, Some(Resp::ResponseRange(r)) if r.kvs.is_empty()));
        let stable = read(&state, b"b", 5).await.result.unwrap();
        assert!(matches!(stable.response, Some(Resp::ResponseRange(r))
            if r.kvs[0].value == b"stable" && r.kvs[0].mod_revision == 3));
        let compacted = read(&state, b"a", 4).await.result.unwrap_err();
        assert_eq!(compacted.code, tonic::Code::OutOfRange as i32);
        let future = state
            .apply_compaction(
                8,
                &CompactionRequest {
                    revision: 8,
                    physical: false,
                },
            )
            .await
            .unwrap();
        assert!(future.result.is_err());
        assert_eq!(state.compact_revision, 5);
        assert_eq!(history_len(&state).await, 4);
        state
            .apply_compaction(
                9,
                &CompactionRequest {
                    revision: 7,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        assert_eq!(state.revision, 7);
        assert_eq!(history_len(&state).await, 2);
        let current = read(&state, b"a", 7).await.result.unwrap();
        assert!(matches!(current.response, Some(Resp::ResponseRange(r))
            if r.kvs[0].value == b"fourth" && r.kvs[0].create_revision == 6));
        state.db.flush().await.unwrap();
        state.db.close().await.unwrap();
        drop(state);
        let state = StateMachine::open(dir.path()).await.unwrap();
        assert_eq!(
            (state.applied, state.revision, state.compact_revision),
            (9, 7, 7)
        );
        assert_eq!(history_len(&state).await, 2);
        assert!(read(&state, b"a", 6).await.result.is_err());
        assert!(read(&state, b"a", 7).await.result.is_ok());
    }

    #[tokio::test]
    async fn snapshot_keeps_the_boundary_at_its_pinned_apply_position() {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        state
            .apply_etcd(1, &put(b"key", b"first"))
            .await
            .unwrap()
            .result
            .unwrap();
        state
            .apply_etcd(2, &put(b"key", b"second"))
            .await
            .unwrap()
            .result
            .unwrap();
        state
            .apply_compaction(
                3,
                &CompactionRequest {
                    revision: 2,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        let snapshot_job = state.snapshot_job().unwrap();
        state
            .apply_compaction(
                4,
                &CompactionRequest {
                    revision: 3,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 3;
        snapshot.data = snapshot_job.await.unwrap().into();
        let mut restored = StateMachine::open(other.path()).await.unwrap();
        restored.install_snapshot(&snapshot).await.unwrap();
        assert_eq!((restored.applied, restored.compact_revision), (3, 2));
        assert_eq!(history_len(&restored).await, 2);
        assert_eq!(history_len(&state).await, 1);
        assert!(read(&restored, b"key", 2).await.result.is_ok());
        assert!(read(&state, b"key", 2).await.result.is_err());
        restored
            .apply_compaction(
                4,
                &CompactionRequest {
                    revision: 3,
                    physical: false,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        assert_eq!(history_len(&restored).await, 1);
        assert!(read(&restored, b"key", 2).await.result.is_err());
    }

    #[tokio::test]
    async fn renewed_lease_and_watch_history_survive_snapshot_and_stale_expiry() {
        use crate::lease::LeaseCommand;
        use crate::proto::etcdserverpb::{TxnRequest, WatchCreateRequest};
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        state
            .apply_lease(1, &LeaseCommand::Grant { id: 7, ttl: 30 })
            .await
            .unwrap()
            .result
            .unwrap();
        let mut writes = vec![];
        for key in [b"z", b"a"] {
            let mut request = put(key, key);
            if let Some(Op::RequestPut(p)) = &mut request.request {
                p.lease = 7;
            }
            writes.push(request);
        }
        state
            .apply_etcd(
                2,
                &RequestOp {
                    request: Some(Op::RequestTxn(TxnRequest {
                        success: writes,
                        ..Default::default()
                    })),
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        state
            .apply_lease(3, &LeaseCommand::KeepAlive { id: 7 })
            .await
            .unwrap()
            .result
            .unwrap();
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 3;
        snapshot.data = state.snapshot_data().await.unwrap().into();
        let mut restored = StateMachine::open(other.path()).await.unwrap();
        restored.install_snapshot(&snapshot).await.unwrap();
        assert_eq!(restored.leases[&7].generation, 3);
        restored
            .apply_lease(
                4,
                &LeaseCommand::Expire {
                    id: 7,
                    generation: 1,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        assert_eq!(restored.revision, 2);
        assert!(restored.leases.contains_key(&7));
        restored
            .apply_lease(
                5,
                &LeaseCommand::Expire {
                    id: 7,
                    generation: 3,
                },
            )
            .await
            .unwrap()
            .result
            .unwrap();
        assert_eq!(restored.revision, 3);
        assert!(restored.leases.is_empty());
        let request = WatchCreateRequest {
            key: vec![0],
            range_end: vec![0],
            start_revision: 1,
            prev_kv: true,
            ..Default::default()
        };
        let page = restored.watch(&request, None).await.unwrap();
        assert_eq!(page.responses.len(), 2);
        let put_events = &page.responses[0].events;
        assert_eq!(put_events[0].kv.as_ref().unwrap().key, b"z");
        assert_eq!(put_events[1].kv.as_ref().unwrap().key, b"a");
        let deletes = &page.responses[1].events;
        assert_eq!(deletes.len(), 2);
        for event in deletes {
            assert_eq!(event.r#type, 1);
            assert_eq!(event.kv.as_ref().unwrap().mod_revision, 3);
            assert_eq!(event.prev_kv.as_ref().unwrap().lease, 7);
            let key = &event.kv.as_ref().unwrap().key;
            assert!(
                matches!(read(&restored, key, 0).await.result.unwrap().response,
                Some(Resp::ResponseRange(r)) if r.kvs.is_empty())
            );
        }
        restored.db.flush().await.unwrap();
        restored.db.close().await.unwrap();
        drop(restored);
        let restored = StateMachine::open(other.path()).await.unwrap();
        assert_eq!(restored.applied, 5);
        assert!(restored.leases.is_empty());
        assert_eq!(
            restored.watch(&request, None).await.unwrap().responses,
            page.responses
        );
    }

    #[tokio::test]
    async fn lease_revoke_deletes_more_keys_than_the_user_transaction_limit() {
        use crate::lease::LeaseCommand;
        use crate::proto::etcdserverpb::WatchCreateRequest;
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        state
            .apply_lease(1, &LeaseCommand::Grant { id: 9, ttl: 30 })
            .await
            .unwrap()
            .result
            .unwrap();
        for n in 0..130 {
            let mut request = put(format!("key-{n}").as_bytes(), b"value");
            if let Some(Op::RequestPut(p)) = &mut request.request {
                p.lease = 9;
            }
            state
                .apply_etcd(n + 2, &request)
                .await
                .unwrap()
                .result
                .unwrap();
        }
        state
            .apply_lease(132, &LeaseCommand::Revoke { id: 9 })
            .await
            .unwrap()
            .result
            .unwrap();
        let page = state
            .watch(
                &WatchCreateRequest {
                    key: vec![0],
                    range_end: vec![0],
                    start_revision: 132,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(page.responses.len(), 1);
        assert_eq!(page.responses[0].events.len(), 130);
        assert!(crate::mvcc::lease_keys(&state.db, 9)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn legacy_state_rejects_missing_watch_history_and_records_new_events() {
        use crate::proto::etcdserverpb::WatchCreateRequest;
        let dir = tempfile::tempdir().unwrap();
        let mut state = StateMachine::open(dir.path()).await.unwrap();
        state
            .apply_etcd(1, &put(b"key", b"old"))
            .await
            .unwrap()
            .result
            .unwrap();
        // Remove the journal to reproduce a database from a binary without Watch.
        let mut batch = WriteBatch::new();
        batch.delete(b"etcd/watch-floor");
        batch.delete([b"etcd/events/".as_slice(), &2_i64.to_be_bytes()].concat());
        batch.put(APPLIED, 2_u64.to_be_bytes());
        state
            .db
            .write_with_options(batch, &WriteOptions { seqnum: 2 })
            .await
            .unwrap();
        state.applied = 2;
        let mut request = WatchCreateRequest {
            key: b"key".to_vec(),
            start_revision: 1,
            prev_kv: true,
            ..Default::default()
        };
        assert_eq!(
            state.watch(&request, None).await.unwrap().compact_revision,
            3
        );
        state
            .apply_etcd(3, &put(b"key", b"new"))
            .await
            .unwrap()
            .result
            .unwrap();
        request.start_revision = 3;
        let page = state.watch(&request, None).await.unwrap();
        assert_eq!(page.compact_revision, 0);
        assert_eq!(
            page.responses[0].events[0].prev_kv.as_ref().unwrap().value,
            b"old"
        );
    }
}
