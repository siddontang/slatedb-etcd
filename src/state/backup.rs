use super::*;
use sha2::{Digest, Sha256};
use std::io::Read;

const MAX_BACKUP_BYTES: usize = 64 * 1024 * 1024;

impl StateMachine {
    /// Restore a checked native snapshot with a new Raft identity and membership.
    pub fn restore_backup(
        backup: &Path,
        dir: &Path,
        id: u64,
        cluster_id: &str,
        members: &crate::cluster::Store,
    ) -> Result<()> {
        check_restore_target(dir, id, members)?;
        let (data, _) = restored_data(backup, members)?;
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = 1;
        snapshot.mut_metadata().term = 1;
        snapshot.mut_metadata().set_conf_state(members.conf());
        snapshot.data = crate::codec::encode(&data)?;
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

    /// Check and read a native snapshot for a new shared cluster. The shared database gets its
    /// rows at index 1. See `shared::Restore`.
    pub fn restore_shared(
        backup: &Path,
        dir: &Path,
        id: u64,
        members: &crate::cluster::Store,
    ) -> Result<shared::Restore> {
        check_restore_target(dir, id, members)?;
        let (mut data, checksum) = restored_data(backup, members)?;
        // The cluster claim sets the quota of shared mode.
        data.rows.retain(|(key, _)| key != quota::KEY);
        Ok(shared::Restore::new(data.rows, checksum))
    }
}

/// A restore needs a member of the new cluster and a new, empty data directory.
fn check_restore_target(dir: &Path, id: u64, members: &crate::cluster::Store) -> Result<()> {
    ensure!(
        members.members.contains_key(&id),
        "restore member is missing"
    );
    ensure!(
        !dir.exists() || std::fs::read_dir(dir)?.next().is_none(),
        "snapshot restore requires a new, empty data directory"
    );
    Ok(())
}

/// Read and check a native snapshot, and rewrite it for a new cluster with `members`. The rows
/// are at index 1, with new auth tokens and the alarms on a member of the new cluster.
/// Also return the snapshot checksum in hexadecimal. It identifies the snapshot.
fn restored_data(backup: &Path, members: &crate::cluster::Store) -> Result<(SnapshotData, String)> {
    let mut bytes = vec![];
    std::fs::File::open(backup)?
        .take(MAX_BACKUP_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= MAX_BACKUP_BYTES, "snapshot exceeds 64 MiB");
    ensure!(bytes.len() > 32, "snapshot is missing its checksum");
    let (payload, checksum) = bytes.split_at(bytes.len() - 32);
    ensure!(
        Sha256::digest(payload).as_slice() == checksum,
        "snapshot checksum mismatch"
    );
    let (mut data, consumed): (SnapshotData, _) = bincode::serde::decode_from_slice(
        payload,
        bincode::config::legacy().with_limit::<MAX_BACKUP_BYTES>(),
    )
    .context("invalid native snapshot")?;
    ensure!(consumed == payload.len(), "trailing native snapshot data");
    ensure!(
        crate::readable_snapshot(data.version),
        "unsupported snapshot version"
    );
    let mut seen = std::collections::BTreeSet::new();
    for (key, value) in &mut data.rows {
        ensure!(snapshot_key(key), "invalid snapshot key");
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
    // The restored log starts at (1, 1). Install stores that term from the snapshot.
    data.rows
        .retain(|(key, _)| key != APPLIED && !derived_key(key) && key != crate::cluster::KEY);
    data.rows
        .push((APPLIED.to_vec(), 1_u64.to_be_bytes().to_vec()));
    data.rows
        .push((crate::cluster::KEY.to_vec(), serde_json::to_vec(members)?));
    data.applied = 1;
    data.version = crate::SNAPSHOT_FORMAT;
    let checksum = checksum.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok((data, checksum))
}
