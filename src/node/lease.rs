use super::*;
use std::sync::Arc;

/// Bound the renewals that wait for one leadership proof.
pub(super) const MAX_RENEWALS: usize = 4096;
const RENEWAL_CANCELLED: &str = "lease renewal cancelled: leadership changed or request timed out";

pub(super) struct Renewal {
    pub(super) id: i64,
    pub(super) credentials: Option<crate::auth::Credentials>,
    pub(super) response: Response,
    pub(super) started: Instant,
}

/// A group of renewals that share one ReadIndex proof.
pub(super) struct RenewBatch {
    pub(super) context: u64,
    pub(super) term: u64,
    pub(super) index: Option<u64>,
    pub(super) retry_ticks: u8,
    pub(super) renewals: Vec<Renewal>,
}

/// Renew requests that the leader serves without a Raft entry.
///
/// A renewal waits for a ReadIndex proof in the current term. It then waits until the applied
/// index reaches the proof index. Only then does the leader extend its in-memory deadline.
#[derive(Default)]
pub(super) struct Renewals {
    pub(super) waiting: Vec<Renewal>,
    pub(super) batches: VecDeque<RenewBatch>,
}

impl Renewals {
    pub fn len(&self) -> usize {
        self.waiting.len() + self.batches.iter().map(|b| b.renewals.len()).sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.waiting.is_empty() && self.batches.is_empty()
    }
}

impl Node {
    pub(super) fn sync_lease_deadlines(&mut self) {
        if self.raw.raft.state != StateRole::Leader || self.lease_term != self.raw.raft.term {
            self.lease_deadlines.clear();
            self.lease_expiring.clear();
            self.lease_view = None;
            self.lease_term = self.raw.raft.term;
        }
        if self.raw.raft.state != StateRole::Leader {
            return;
        }
        // The node holds the previous map, so apply must copy it before a change.
        // An equal pointer therefore proves that the lease set did not change.
        if self
            .lease_view
            .as_ref()
            .is_some_and(|view| Arc::ptr_eq(view, &self.state.leases))
        {
            return;
        }
        self.lease_deadlines
            .retain(|id, _| self.state.leases.contains_key(id));
        let now = Instant::now();
        for (&id, lease) in self.state.leases.iter() {
            let entry = self.lease_deadlines.entry(id).or_insert((0, now));
            if entry.0 != lease.generation {
                *entry = (
                    lease.generation,
                    now + Duration::from_secs(lease.ttl as u64),
                );
            }
        }
        self.lease_view = Some(self.state.leases.clone());
    }

    pub(super) fn expire_leases(&mut self) -> Result<()> {
        self.sync_lease_deadlines();
        if self.backlogged() {
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

    /// Queue a KeepAlive request. A follower returns `not leader`, so the caller forwards it.
    pub(super) fn keep_alive(
        &mut self,
        id: i64,
        credentials: Option<crate::auth::Credentials>,
        response: Response,
    ) -> Result<()> {
        if self.raw.raft.state != StateRole::Leader {
            let _ = response.send(Err(self.error("not leader")));
        } else if self.state.auth.enabled {
            self.propose_keep_alive(id, credentials, response)?;
        } else if self.renewals.len() >= MAX_RENEWALS {
            let _ = response.send(Err(self.error("too many pending lease renewals")));
        } else {
            self.renewals.waiting.push(Renewal {
                id,
                credentials,
                response,
                started: Instant::now(),
            });
        }
        Ok(())
    }

    /// Use the Raft path when Auth is enabled. Apply checks write permission on attached keys.
    fn propose_keep_alive(
        &mut self,
        id: i64,
        credentials: Option<crate::auth::Credentials>,
        response: Response,
    ) -> Result<()> {
        self.propose(
            RaftCommand::Lease {
                origin: self.options.id,
                id: uuid::Uuid::new_v4().to_string(),
                lease: LeaseCommand::KeepAlive { id },
            }
            .secure(credentials),
            response,
        )
    }

    /// Ask the quorum to confirm leadership for all waiting renewals.
    pub(super) fn issue_renewals(&mut self) {
        if self.renewals.waiting.is_empty()
            || self.renewals.batches.iter().any(|b| b.index.is_none())
        {
            return;
        }
        if self.raw.raft.state != StateRole::Leader {
            for renewal in std::mem::take(&mut self.renewals.waiting) {
                let _ = renewal.response.send(Err(self.error("not leader")));
            }
            return;
        }
        let Some(context) = self.read_sequence.checked_add(1) else {
            for renewal in std::mem::take(&mut self.renewals.waiting) {
                let _ = renewal
                    .response
                    .send(Err(self.error("read sequence exhausted")));
            }
            return;
        };
        self.read_sequence = context;
        self.renewals.batches.push_back(RenewBatch {
            context,
            term: self.raw.raft.term,
            index: None,
            retry_ticks: 2,
            renewals: std::mem::take(&mut self.renewals.waiting),
        });
        self.raw.read_index(self.read_context(context));
    }

    pub(super) fn retry_renewals(&mut self) {
        let mut retry = vec![];
        for batch in &mut self.renewals.batches {
            if batch.index.is_some() {
                continue;
            }
            batch.retry_ticks -= 1;
            if batch.retry_ticks == 0 {
                // Raft can drop reads before the new leader commits its first entry.
                retry.push(batch.context);
                batch.retry_ticks = 2;
            }
        }
        for context in retry {
            self.raw.read_index(self.read_context(context));
        }
    }

    pub(super) fn confirm_renewal(&mut self, context: u64, index: u64) {
        let term = self.raw.raft.term;
        let leader = self.raw.raft.state == StateRole::Leader;
        for batch in &mut self.renewals.batches {
            if batch.context == context && batch.index.is_none() && batch.term == term && leader {
                batch.index = Some(index);
            }
        }
    }

    /// Cancel renewals after a leadership change, a timeout, or a closed client.
    pub(super) fn expire_renewals(&mut self) {
        if self.renewals.is_empty() {
            return;
        }
        let now = Instant::now();
        let term = self.raw.raft.term;
        let leader = self.raw.raft.state == StateRole::Leader;
        let mut cancelled = vec![];
        let mut keep = |renewals: Vec<Renewal>, stale: bool| {
            let mut kept = Vec::with_capacity(renewals.len());
            for renewal in renewals {
                if renewal.response.is_closed() {
                    continue;
                }
                if stale || now.duration_since(renewal.started) > Duration::from_secs(5) {
                    cancelled.push(renewal.response);
                } else {
                    kept.push(renewal);
                }
            }
            kept
        };
        self.renewals.waiting = keep(std::mem::take(&mut self.renewals.waiting), false);
        for mut batch in std::mem::take(&mut self.renewals.batches) {
            batch.renewals = keep(batch.renewals, !leader || batch.term != term);
            if !batch.renewals.is_empty() {
                self.renewals.batches.push_back(batch);
            }
        }
        for response in cancelled {
            let _ = response.send(Err(self.error(RENEWAL_CANCELLED)));
        }
    }

    /// Renew leases whose leadership proof the local state has applied.
    pub(super) fn finish_renewals(&mut self) -> Result<()> {
        while self
            .renewals
            .batches
            .front()
            .and_then(|batch| batch.index)
            .is_some_and(|index| index <= self.state.applied)
        {
            let batch = self.renewals.batches.pop_front().unwrap();
            if batch.term != self.raw.raft.term || self.raw.raft.state != StateRole::Leader {
                for renewal in batch.renewals {
                    let _ = renewal.response.send(Err(self.error(RENEWAL_CANCELLED)));
                }
                continue;
            }
            for renewal in batch.renewals {
                if renewal.response.is_closed() {
                    continue;
                }
                if self.state.auth.enabled {
                    self.propose_keep_alive(renewal.id, renewal.credentials, renewal.response)?;
                    continue;
                }
                let ttl = self.renew_lease(renewal.id);
                let mut header = crate::mvcc::header(self.state.revision);
                self.fill_header(header.as_mut());
                let reply = crate::mvcc::Outcome {
                    result: Ok(LeaseReply::KeepAlive(
                        crate::proto::etcdserverpb::LeaseKeepAliveResponse {
                            header,
                            id: renewal.id,
                            ttl,
                        },
                    )),
                };
                let _ = renewal.response.send(Ok(serde_json::to_value(reply)?));
            }
        }
        Ok(())
    }

    /// Extend one lease deadline on the current leader and return its TTL.
    ///
    /// A lease past its deadline is not renewed, so an Expire proposal from this term stays
    /// valid. The reply then reports TTL 0, as etcd does for a missing lease.
    pub(super) fn renew_lease(&mut self, id: i64) -> i64 {
        self.sync_lease_deadlines();
        let Some(lease) = self.state.leases.get(&id) else {
            return 0;
        };
        let Some((_, deadline)) = self.lease_deadlines.get_mut(&id) else {
            return 0;
        };
        let now = Instant::now();
        if *deadline <= now {
            return 0;
        }
        *deadline = now + Duration::from_secs(lease.ttl as u64);
        lease.ttl
    }
}
