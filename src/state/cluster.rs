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
//! A leader proposes `SeedMembers` right after its election, before any other entry of its term
//! except `SetQuota`. An earlier binary has no seed, and it stored the bootstrap membership with
//! its first batch. So another entry that finds no row comes from a log of an earlier binary.
//! The batch of that entry then stores the in-memory membership, as that binary did, and the
//! quota check of that entry already counts it. An upgraded member that replays such a log gets
//! the row and the decisions of the members that flushed it. A state that this binary created empty never
//! applies such an entry. The quota guard stops it there and asks for a snapshot; see
//! `node::quota`.
//!
//! A `SeedMembers` entry also carries the digest of the row of its leader at the applied index of
//! the leader. A member whose row did not change after that index compares the digests. A
//! difference means that the rows diverged, for example under an earlier binary. Apply logs a
//! warning and keeps the row.

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

/// Return true when an earlier binary stored its bootstrap membership before `command`: for
/// every entry except the term-start entries `SeedMembers` and `SetQuota`.
pub(crate) fn needs_members(command: &crate::RaftCommand) -> bool {
    !matches!(
        command,
        crate::RaftCommand::SeedMembers(_) | crate::RaftCommand::SetQuota { .. }
    )
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
        // The stored row is the base. Only a log of an earlier binary applies a cluster command
        // without it. That binary used the in-memory membership, and `before_entry` stores it.
        let stored = self.applied_state().get(crate::cluster::KEY).await?;
        let mut members = match &stored {
            Some(row) => serde_json::from_slice(row)?,
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
        let mut batch = WriteBatch::new();
        if result.is_ok() {
            let value = serde_json::to_vec(&members)?;
            if stored.as_deref() != Some(value.as_slice()) {
                self.members_row = Some(MembersRow::new(&value, index));
                batch.put(crate::cluster::KEY, value);
            }
            self.members = Arc::new(members);
        }
        self.mark_applied(&mut batch, index);
        self.write_batch(batch, index).await?;
        self.applied = index;
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
        match &self.members_row {
            None => {
                self.members = Arc::new(serde_json::from_str(members)?);
                self.members_row = Some(MembersRow::new(members.as_bytes(), index));
                batch.put(crate::cluster::KEY, members.as_bytes());
            }
            Some(row) => {
                // A row that changed after `applied` can differ from the row of the leader.
                if let Some(digest) = digest.as_deref().filter(|_| row.written <= applied) {
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
        Ok(())
    }

    /// Prepare the state for the next entry, `command`. An entry that `needs_members` selects
    /// and that finds no row comes from a log of an earlier binary. Its batch then stores the
    /// in-memory membership, as that binary did. See the module comment.
    ///
    /// That binary counted the row before this entry. So the logical size counts it now, before
    /// the entry checks the quota, wherever the entry falls in an apply batch.
    pub(crate) fn before_entry(&mut self, command: &crate::RaftCommand) -> Result<()> {
        if self.members_row.is_none()
            && !self.members_unsaved
            && !self.members.members.is_empty()
            && needs_members(command)
        {
            let len = serde_json::to_vec(self.members.as_ref())?.len();
            self.logical_bytes
                .replace_row(crate::cluster::KEY, None, Some(len))?;
            self.members_counted = Some(len);
            self.members_unsaved = true;
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

    /// Read the row again after a snapshot install replaced the rows. Without the row, keep the
    /// in-memory membership. It is the bootstrap membership or the membership of a snapshot.
    pub(super) async fn reload_members(&mut self) -> Result<()> {
        let (members, row) = load(self.applied_state(), self.applied).await?;
        if row.is_some() {
            self.members = Arc::new(members);
        }
        self.members_row = row;
        self.members_unsaved = false;
        self.members_mismatch = None;
        Ok(())
    }
}
