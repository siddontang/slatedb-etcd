use super::*;
use crate::cluster::Command as C;
use crate::mvcc::{KvError, Outcome};
use tonic::Code;

impl Node {
    pub(super) fn publish_member(&mut self) -> Result<()> {
        if self.raw.raft.leader_id == 0
            || self.state.members.members.is_empty()
            || self
                .published_at
                .is_some_and(|time| time.elapsed() < Duration::from_secs(1))
        {
            return Ok(());
        }
        let id = self.options.id;
        let Some(member) = self.state.members.members.get(&id) else {
            return Ok(());
        };
        let Some(address) = self
            .options
            .client_peers
            .get(&id)
            .or(self.options.peers.get(&id))
        else {
            return Ok(());
        };
        let client_urls = vec![crate::cluster::url(address)];
        if member.client_ur_ls == client_urls && !member.name.is_empty() {
            return Ok(());
        }
        let command = RaftCommand::Cluster {
            id: format!("publish-{id}"),
            request: C::Publish { id, client_urls },
            credentials: crate::auth::Credentials::default(),
        };
        let data = command.encode()?;
        if self.raw.store().admits(
            self.raw.raft.raft_log.unstable_entries(),
            &[],
            data.len() + cache::ENTRY_OVERHEAD + 64,
        ) {
            self.published_at = Some(Instant::now());
            // Raft forwards this trusted peer update to the current leader.
            let _ = self.raw.propose(vec![], data);
        }
        Ok(())
    }

    pub(super) fn cluster_request(
        &mut self,
        request: C,
        credentials: crate::auth::Credentials,
        response: Response,
    ) -> Result<()> {
        if matches!(request, C::List(_)) {
            let mut header = ResponseHeader {
                revision: self.state.revision,
                ..Default::default()
            };
            self.fill_header(Some(&mut header));
            let _ = response.send(Ok(serde_json::to_value(Outcome {
                result: Ok(json!({"header": header,
                    "members": self.state.members.members.values().collect::<Vec<_>>()})),
            })?));
            return Ok(());
        }
        if self.raw.raft.state != StateRole::Leader {
            let _ = response.send(Err(self.error("not leader")));
            return Ok(());
        }
        let permission = if matches!(request, C::List(_)) {
            Ok(())
        } else {
            self.state.auth.admin(&credentials)
        };
        let checked = permission
            .and_then(|()| self.state.members.validate(&request))
            .and_then(|()| self.check_membership(&request));
        if let Err(error) = checked {
            let _ = response.send(Ok(serde_json::to_value(Outcome::<Value> {
                result: Err(error),
            })?));
            return Ok(());
        }
        self.propose(
            RaftCommand::Cluster {
                id: uuid::Uuid::new_v4().to_string(),
                request,
                credentials,
            },
            response,
        )
    }

    fn check_membership(&self, request: &C) -> std::result::Result<(), KvError> {
        let urls = match request {
            C::Add(member) => &member.peer_ur_ls[..],
            C::Update(member) => &member.peer_ur_ls[..],
            _ => &[],
        };
        if self.options.require_peer_tls {
            for url in urls {
                crate::tls::validate_url(url, true, true).map_err(|_| {
                    KvError::new(Code::InvalidArgument, "remote peers require an HTTPS URL")
                })?;
            }
        }
        if matches!(request, C::List(_)) {
            return Ok(());
        }
        if self.raw.raft.pending_conf_index > self.state.applied {
            return Err(KvError::new(
                Code::Unavailable,
                "reconfiguration is in progress",
            ));
        }
        if let C::Promote(id) = request {
            let caught_up = self
                .raw
                .raft
                .prs()
                .get(*id)
                .is_some_and(|p| p.matched >= self.raw.raft.raft_log.committed);
            if !caught_up {
                return Err(KvError::new(
                    Code::FailedPrecondition,
                    "can only promote a learner member which is in sync with leader",
                ));
            }
        }
        if self.options.strict_reconfiguration && self.state.members.members.len() > 1 {
            let voters: Vec<_> = self
                .state
                .members
                .members
                .values()
                .filter(|m| !m.is_learner)
                .collect();
            let connected = |id: u64| {
                id == self.options.id
                    || self
                        .peer_health
                        .get(&id)
                        .is_some_and(|since| since.elapsed() >= Duration::from_secs(5))
            };
            let healthy = match request {
                C::Add(_) => voters.iter().all(|m| connected(m.id)),
                C::Remove(id) if self.state.members.members[id].is_learner => true,
                C::Remove(id) => {
                    voters
                        .iter()
                        .filter(|m| m.id != *id && connected(m.id))
                        .count()
                        >= voters.len().div_ceil(2)
                }
                C::Promote(_) => {
                    voters.iter().filter(|m| connected(m.id)).count() >= voters.len().div_ceil(2)
                }
                C::Update(_) | C::List(_) | C::Publish { .. } => true,
            };
            if !healthy {
                return Err(KvError::new(Code::Unavailable, "unhealthy cluster"));
            }
        }
        Ok(())
    }

    pub(super) fn sync_members(&mut self) {
        if self.state.members.members.is_empty() {
            return;
        }
        let peers: BTreeMap<_, _> = self
            .state
            .members
            .members
            .values()
            .filter_map(|m| m.peer_ur_ls.first().map(|url| (m.id, url.clone())))
            .collect();
        self.forwarding.update_peers(&peers);
        if self.manual_transport {
            // DST controls the senders but uses the same membership admission checks.
            self.options.peers = peers;
            return;
        }
        while self.transports.try_join_next().is_some() {}
        self.traffic.retain(|id, _| peers.contains_key(id));
        self.senders.retain(|id, _| peers.contains_key(id));
        self.peer_addresses.retain(|id, _| peers.contains_key(id));
        for (&id, address) in &peers {
            if id == self.options.id {
                continue;
            }
            if self.senders.contains_key(&id) {
                // A running sender keeps its connection and dials the new URL after a failure.
                if let Some(sender) = self.peer_addresses.get(&id) {
                    sender.send_if_modified(|old| {
                        let changed = crate::cluster::url(old) != crate::cluster::url(address);
                        if changed {
                            *old = address.clone();
                        }
                        changed
                    });
                    continue;
                }
            }
            let (tx, rx) = mpsc::channel(256);
            self.senders.insert(id, tx);
            self.peer_health.remove(&id);
            let (address_tx, address_rx) = tokio::sync::watch::channel(address.clone());
            self.peer_addresses.insert(id, address_tx);
            self.transports.spawn(transport_to(
                id,
                address_rx,
                self.options.cluster_id.clone(),
                rx,
                self.events.clone(),
                self.options.transport.clone(),
                self.traffic.entry(id).or_default().clone(),
            ));
        }
        self.options.peers = peers;
    }
}
