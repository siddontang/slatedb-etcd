use super::*;
pub(super) mod clock;

/// Bound the renewals that wait for one leadership proof.
pub(super) const MAX_RENEWALS: usize = 4096;
const RENEWAL_CANCELLED: &str = "lease renewal cancelled: leadership changed or request timed out";

pub(super) struct Renewal {
    pub(super) id: i64,
    pub(super) credentials: Option<crate::auth::Credentials>,
    pub(super) response: ClientResponse,
    pub(super) started: Instant,
}

/// A group of renewals that share one ReadIndex proof.
pub(super) struct RenewBatch {
    pub(super) context: u64,
    /// When this leader first asked the quorum for the proof.
    pub(super) issued: Instant,
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
        let leader = self.raw.raft.state == StateRole::Leader;
        if self
            .lease_clock
            .sync_term(self.raw.raft.term, leader, &self.state.leases)
        {
            let extend = self.promote_extend();
            self.lease_clock.rebuild(&self.state.leases, extend);
        }
    }

    /// The longest randomized election timeout.
    ///
    /// An old leader renews in memory after its ReadIndex proof, so the next leader does not see the
    /// renewal. The followers that confirmed the proof reject votes for one election timeout. The
    /// next leader then adds this extension to each inherited deadline. A renewal is cancelled when
    /// it finishes more than this extension after its proof, so it cannot outlive that deadline.
    pub(super) fn promote_extend(&self) -> Duration {
        TICK * (2 * self.raw.raft.election_timeout()) as u32
    }

    pub(super) fn expire_leases(&mut self) -> Result<()> {
        self.sync_lease_deadlines();
        if self.backlogged() {
            return Ok(());
        }
        let mut due = self.lease_clock.due(Instant::now());
        while let Some((id, generation)) = due.next() {
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
                due.proposed(id, generation);
            }
        }
        Ok(())
    }

    /// Queue a KeepAlive request. A follower returns `not leader`, so the caller forwards it.
    pub(super) fn keep_alive(
        &mut self,
        id: i64,
        credentials: Option<crate::auth::Credentials>,
        response: impl Into<ClientResponse>,
    ) -> Result<()> {
        let response = response.into();
        if self.raw.raft.state != StateRole::Leader {
            response.error(self.error("not leader"));
        } else if self.state.auth.enabled {
            self.propose_keep_alive(id, credentials, response)?;
        } else if self.renewals.len() >= MAX_RENEWALS {
            response.error(self.error("too many pending lease renewals"));
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
        response: ClientResponse,
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
                renewal.response.error(self.error("not leader"));
            }
            return;
        }
        let Some(context) = self.reads.next_id() else {
            for renewal in std::mem::take(&mut self.renewals.waiting) {
                renewal
                    .response
                    .error(self.error("read sequence exhausted"));
            }
            return;
        };
        self.renewals.batches.push_back(RenewBatch {
            context,
            issued: Instant::now(),
            term: self.raw.raft.term,
            index: None,
            retry_ticks: 2,
            renewals: std::mem::take(&mut self.renewals.waiting),
        });
        self.raw.read_index(self.reads.context(context));
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
            self.raw.read_index(self.reads.context(context));
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
            response.error(self.error(RENEWAL_CANCELLED));
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
            // A late apply would renew after the next leader could start its deadline.
            let stale = batch.issued.elapsed() > self.promote_extend();
            if stale || batch.term != self.raw.raft.term || self.raw.raft.state != StateRole::Leader
            {
                for renewal in batch.renewals {
                    renewal.response.error(self.error(RENEWAL_CANCELLED));
                }
                continue;
            }
            // Every reply in the batch reports the same applied revision and member.
            let mut header = crate::mvcc::header(self.state.revision);
            self.fill_header(header.as_mut());
            for renewal in batch.renewals {
                if renewal.response.is_closed() {
                    continue;
                }
                if self.state.auth.enabled {
                    self.propose_keep_alive(renewal.id, renewal.credentials, renewal.response)?;
                    continue;
                }
                let ttl = self.renew_lease(renewal.id);
                let reply = crate::mvcc::Outcome {
                    result: Ok(LeaseReply::KeepAlive(
                        crate::proto::etcdserverpb::LeaseKeepAliveResponse {
                            header,
                            id: renewal.id,
                            ttl,
                        },
                    )),
                };
                renewal.response.lease(reply)?;
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
        if !self.lease_clock.renew(id, lease.ttl) {
            return 0;
        }
        lease.ttl
    }
}
