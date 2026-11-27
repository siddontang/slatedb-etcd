//! The `etcd/members` row.
//!
//! The logical size counts the row, and the quota check reads that size. So every member must
//! store the same bytes at each index. Apply writes the row only from replicated data:
//!
//! - A `SeedMembers` entry stores the membership of its leader when the state has no row.
//! - A cluster command changes the stored row. A rejected command writes no row.
//! - A Raft snapshot carries the row of the member that made it.
//!
//! The bootstrap membership from the flags of a member stays in memory until the seed applies.
//! The transport and the member API use it. It does not count toward the logical size.
//!
//! A leader proposes `SeedMembers` right after its election, before any other entry of its term.
//!
//! A `SeedMembers` entry also carries the digest of the row of its leader at the applied index of
//! the leader. A member whose row did not change after that index compares the digests. A
//! difference means that the rows diverged. Apply logs a warning and keeps the row.

use super::*;
use crate::cluster::Store;

/// The digest of the stored `etcd/members` row, and the last index that can have changed it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MembersRow {
    pub digest: String,
    /// The index of the last write of the row. After a load or an install it is the applied
    /// index, which is not lower.
    pub written: u64,
}

impl MembersRow {
    pub(super) fn new(row: &[u8], written: u64) -> Self {
        Self {
            digest: crate::cluster::digest(row),
            written,
        }
    }
}

/// Read the stored membership and its digest. A state without the row has no membership.
pub(super) async fn load(
    reads: &impl StateRead,
    applied: u64,
) -> Result<(Store, Option<MembersRow>)> {
    Ok(match reads.get(crate::cluster::KEY).await? {
        Some(row) => (
            serde_json::from_slice(&row)?,
            Some(MembersRow::new(&row, applied)),
        ),
        None => (Store::default(), None),
    })
}

impl StateMachine {
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
        // The stored row is the base. Without it, the base is the in-memory membership.
        let stored = self
            .applied_state()
            .get(crate::cluster::KEY)
            .await
            .map_err(crate::mvcc::storage_fault::read)?;
        // A stored row that does not decode is corrupt data, so apply holds on the entry.
        let mut members = match &stored {
            Some(row) => serde_json::from_slice(row).map_err(|error| {
                crate::mvcc::storage_fault::stored(format!("invalid members row: {error}"))
            })?,
            None => self.members.as_ref().clone(),
        };
        let result = allowed.and_then(|()| members.validate(request)).map(|()| {
            members.apply(request);
            let mut response = serde_json::json!({"header": mvcc::header(self.revision),
                "members": members.members.values().collect::<Vec<_>>()});
            if let crate::cluster::Command::Add(member) = request {
                response["member"] = serde_json::json!(member);
            }
            response
        });
        // Stage the membership until the batch is stored. A failed store then leaves the
        // membership of the applied prefix. The size count takes the length of the row read
        // above, so it reads nothing.
        let mut batch = WriteBatch::new();
        let mut staged = None;
        if result.is_ok() {
            let value = serde_json::to_vec(&members)?;
            let row = (stored.as_deref() != Some(value.as_slice()))
                .then(|| MembersRow::new(&value, index));
            if row.is_some() {
                let old_len = stored.as_ref().map(|row| row.len());
                batch.put_known(crate::cluster::KEY, value, old_len);
            }
            staged = Some((members, row));
        }
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        self.applied = index;
        if let Some((members, row)) = staged {
            if row.is_some() {
                self.members_row = row;
            }
            self.members = Arc::new(members);
        }
        Ok(Outcome { result })
    }

    /// Apply a `SeedMembers` entry. Store the row of the leader when the state has no row.
    /// Otherwise keep the row, and compare it with the digest of the row of the leader.
    pub(crate) async fn apply_seed_members(
        &mut self,
        index: u64,
        seed: &crate::cluster::Seed,
    ) -> Result<()> {
        let crate::cluster::Seed {
            origin,
            ref members,
            ref digest,
            applied,
            ..
        } = *seed;
        ensure!(
            index == self.applied + 1,
            "non-contiguous state machine apply"
        );
        let mut batch = WriteBatch::new();
        // Stage the seeded membership until the batch is stored, as `apply_cluster` does.
        let mut staged = None;
        match &self.members_row {
            None => {
                staged = Some((
                    Arc::new(serde_json::from_str(members)?),
                    MembersRow::new(members.as_bytes(), index),
                ));
                batch.put(crate::cluster::KEY, members.as_bytes());
            }
            Some(row) => {
                // A row that changed after `applied` can differ from the row of the leader.
                if let Some(digest) = digest.as_deref().filter(|_| row.written <= applied) {
                    self.members_checked = Some((self.entry_term, index));
                    if row.digest == digest {
                        self.members_mismatch = None;
                    } else {
                        tracing::warn!(
                            leader = origin,
                            leader_digest = digest,
                            digest = %row.digest,
                            index = applied,
                            "the members row differs from the row of the leader; install a \
                             snapshot from a chosen member to repair it"
                        );
                        self.members_mismatch = Some(origin);
                    }
                }
            }
        }
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        self.applied = index;
        if let Some((members, row)) = staged {
            self.members = members;
            self.members_row = Some(row);
        }
        Ok(())
    }

    /// Apply a seed of the in-memory membership at `index`, as a first leader does.
    #[cfg(test)]
    pub(crate) async fn seed_for_test(&mut self, index: u64) -> Result<()> {
        let seed = crate::cluster::Seed {
            id: String::new(),
            origin: 1,
            members: serde_json::to_string(self.members.as_ref())?,
            digest: None,
            applied: index - 1,
        };
        self.apply_seed_members(index, &seed).await
    }
}
