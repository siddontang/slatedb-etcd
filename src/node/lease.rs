use super::*;

impl Node {
    pub(super) fn sync_lease_deadlines(&mut self) {
        if self.raw.raft.state != StateRole::Leader || self.lease_term != self.raw.raft.term {
            self.lease_deadlines.clear();
            self.lease_expiring.clear();
            self.lease_term = self.raw.raft.term;
        }
        if self.raw.raft.state != StateRole::Leader {
            return;
        }
        self.lease_deadlines
            .retain(|id, _| self.state.leases.contains_key(id));
        for (&id, lease) in self.state.leases.iter() {
            let entry = self
                .lease_deadlines
                .entry(id)
                .or_insert((0, Instant::now()));
            if entry.0 != lease.generation {
                *entry = (
                    lease.generation,
                    Instant::now() + Duration::from_secs(lease.ttl as u64),
                );
            }
        }
    }

    pub(super) fn expire_leases(&mut self) -> Result<()> {
        self.sync_lease_deadlines();
        if self
            .raw
            .store()
            .commit_index()
            .max(self.raw.raft.raft_log.committed)
            .saturating_sub(self.state.applied)
            >= apply::MAX_APPLY_LAG
            || self.log_queue.len() >= log::MAX_JOBS
            || self.log_bytes >= log::MAX_BYTES
        {
            return Ok(());
        }
        let now = Instant::now();
        for (&id, &(generation, deadline)) in &self.lease_deadlines {
            if deadline > now || self.lease_expiring.contains(&(id, generation)) {
                continue;
            }
            let command = RaftCommand::Lease {
                origin: self.options.id,
                id: uuid::Uuid::new_v4().to_string(),
                lease: LeaseCommand::Expire { id, generation },
            };
            let data = command.encode()?;
            if !self.raw.store().admits(
                self.raw.raft.raft_log.unstable_entries(),
                &[],
                data.len() + cache::ENTRY_OVERHEAD + 64,
            ) {
                break;
            }
            if self.raw.propose(vec![], data).is_ok() {
                self.lease_expiring.insert((id, generation));
            }
        }
        Ok(())
    }
}
