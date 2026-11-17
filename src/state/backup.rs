use super::*;
use bincode::Options;
use sha2::{Digest, Sha256};
use std::io::Read;

const MAX_BACKUP_BYTES: u64 = 64 * 1024 * 1024;

impl StateMachine {
    /// Restore a checked native snapshot with a new Raft identity and membership.
    pub fn restore_backup(
        backup: &Path,
        dir: &Path,
        id: u64,
        cluster_id: &str,
        members: &crate::cluster::Store,
    ) -> Result<()> {
        ensure!(
            members.members.contains_key(&id),
            "restore member is missing"
        );
        ensure!(
            !dir.exists() || std::fs::read_dir(dir)?.next().is_none(),
            "snapshot restore requires a new, empty data directory"
        );
        let mut bytes = vec![];
        std::fs::File::open(backup)?
            .take(MAX_BACKUP_BYTES + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_BACKUP_BYTES,
            "snapshot exceeds 64 MiB"
        );
        ensure!(bytes.len() > 32, "snapshot is missing its checksum");
        let (payload, checksum) = bytes.split_at(bytes.len() - 32);
        ensure!(
            Sha256::digest(payload).as_slice() == checksum,
            "snapshot checksum mismatch"
        );
        let mut data: SnapshotData = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .reject_trailing_bytes()
            .with_limit(MAX_BACKUP_BYTES)
            .deserialize(payload)
            .context("invalid native snapshot")?;
        ensure!(
            data.version == 1 || data.version == 2,
            "unsupported snapshot version"
        );
        let mut seen = std::collections::BTreeSet::new();
        for (key, value) in &mut data.rows {
            ensure!(
                key.starts_with(DATA) || key.starts_with(b"etcd/") || key == APPLIED,
                "invalid snapshot key"
            );
            ensure!(seen.insert(key.clone()), "duplicate snapshot key");
            if key == crate::auth::KEY {
                let mut auth: crate::auth::Store = serde_json::from_slice(value)?;
                auth.reset_tokens();
                *value = serde_json::to_vec(&auth)?;
            }
            if key == crate::maintenance::ALARMS {
                let mut alarms: Vec<crate::proto::etcdserverpb::AlarmMember> =
                    serde_json::from_slice(value)?;
                // Keep alarm types, but attach them to a member of the restored cluster.
                for alarm in &mut alarms {
                    alarm.member_id = *members.members.keys().next().unwrap();
                }
                alarms.sort_by_key(|alarm| alarm.alarm);
                alarms.dedup();
                *value = serde_json::to_vec(&alarms)?;
            }
        }
        data.rows
            .retain(|(key, _)| key != APPLIED && key != crate::cluster::KEY);
        data.rows
            .push((APPLIED.to_vec(), 1_u64.to_be_bytes().to_vec()));
        data.rows
            .push((crate::cluster::KEY.to_vec(), serde_json::to_vec(members)?));
        data.applied = 1;
        data.version = 2;
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 1;
        snapshot.mut_metadata().term = 1;
        snapshot.mut_metadata().set_conf_state(members.conf());
        snapshot.data = bincode::serialize(&data)?.into();
        std::fs::create_dir_all(dir)?;
        let mut storage = crate::storage::RaftStorage::open(
            &dir.join("raft"),
            id,
            cluster_id,
            members.conf().voters,
        )?;
        storage.persist(
            &[],
            Some(&raft::eraftpb::HardState {
                term: 1,
                commit: 1,
                ..Default::default()
            }),
            Some(&snapshot),
        )?;
        Ok(())
    }
}
