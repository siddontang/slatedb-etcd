use super::*;
use crate::cluster::Command as C;
use crate::mvcc::{KvError, Outcome};
use tonic::Code;

impl Node {
    /// Propose `SeedMembers` at the start of each term of this leader. It seeds a state without
    /// the membership row. Otherwise it lets every member compare its row with the row of this
    /// leader.
    ///
    /// A member compares only when its row did not change after the applied index of the
    /// leader. After a restart, a member knows only that its row changed at or before its index
    /// at the start. That index is at most the last entry of an earlier term. A new leader that
    /// has not applied that entry yet, or has no row, proposes its digest once more after the
    /// first seed of its term applies. A new cluster pays one small entry for this.
    pub(super) fn propose_members(&mut self) -> Result<()> {
        let term = self.raw.raft.term;
        if self.raw.raft.state != StateRole::Leader || self.members_rechecked == term {
            return Ok(());
        }
        let row = self.state.members_row.as_ref();
        let recheck = match self.members_proposed {
            Some((proposed, index)) if proposed == term => {
                if self.state.applied < index || row.is_none() {
                    return Ok(());
                }
                true
            }
            _ => false,
        };
        // A joining member has no bootstrap membership. It gets the row from the cluster.
        if row.is_none() && (self.options.join || self.state.members.members.is_empty()) {
            return Ok(());
        }
        let data = RaftCommand::SeedMembers(crate::cluster::Seed {
            id: uuid::Uuid::new_v4().to_string(),
            origin: self.options.id,
            members: serde_json::to_string(self.state.members.as_ref())?,
            digest: row.map(|row| row.digest.clone()),
            applied: self.state.applied,
        })
        .encode()?;
        // Like `SetQuota`, the small entry skips the memory admission check. Raft drops it only
        // during a leadership transfer, with every other proposal. The next call tries again.
        // The entries of this term start at the empty entry of the leader. A seed without a
        // digest compares nothing, so the leader sends its digest once it has a row.
        let covered =
            row.is_some() && self.state.applied + 1 >= self.raw.raft.raft_log.last_index();
        if self.raw.propose(vec![], data).is_ok() {
            if recheck || covered {
                self.members_rechecked = term;
            }
            self.members_proposed = Some((term, self.raw.raft.raft_log.last_index()));
        }
        Ok(())
    }

    /// Warn once when the first stored row that this node sees differs from the bootstrap
    /// membership of its flags. The stored row wins.
    pub(super) fn observe_members(&mut self) {
        if self.members_observed || self.state.members_row.is_none() {
            return;
        }
        self.members_observed = true;
        if self.options.join {
            return;
        }
        let flags =
            crate::cluster::Store::bootstrap(&self.options.peers, &self.options.client_peers);
        let differences = flags.differences(&self.state.members);
        if !differences.is_empty() {
            tracing::warn!(
                members = ?differences,
                "the stored membership differs from the --peer and --client-peer flags; the \
                 stored membership applies"
            );
        }
    }

    /// The membership row fields of `/status`.
    pub(super) fn members_status(&self) -> Value {
        let row = self.state.members_row.as_ref();
        json!({
            "digest": row.map(|row| row.digest.clone()),
            "written": row.map(|row| row.written),
            "mismatch_leader": self.state.members_mismatch,
            "checked": self.state.members_checked.map(|(term, index)| json!({
                "term": term,
                "index": index,
            })),
        })
    }

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
            .and_then(|()| self.check_membership(&request))
            .and_then(|()| self.check_write_proof(&request));
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
                self.peer.clone(),
                rx,
                self.events.clone(),
                self.options.transport.clone(),
                self.traffic.entry(id).or_default().clone(),
            ));
        }
        self.options.peers = peers;
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{commit_with_peer, leader_node_with, options};
    use super::*;
    use crate::state::StateRead as _;
    use raft::eraftpb::HardState;

    /// A host name that makes each client URL about 200 bytes longer.
    fn long_host() -> String {
        format!("{}example", "a.".repeat(100))
    }

    /// The client URLs of members 1 to 3, with the host text `host`.
    fn client_urls(host: &str) -> BTreeMap<u64, String> {
        (1..=3)
            .map(|id| (id, format!("http://{host}:{}", 2379 + id)))
            .collect()
    }

    fn put(n: usize) -> RaftCommand {
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

    /// The committed log of a leader with the quota flag `quota` and then `puts` client Puts.
    async fn leader_log(quota: i64, puts: usize) -> Vec<Entry> {
        let dir = tempfile::tempdir().unwrap();
        let mut leader = leader_node_with(dir.path(), |config| {
            config.quota_bytes = quota;
            config.client_peers = client_urls("127.0.0.1");
        })
        .await;
        for n in 0..puts {
            let (tx, _reply) = oneshot::channel();
            leader.propose(put(n), ClientResponse::Kv(tx)).unwrap();
            commit_with_peer(&mut leader).await;
        }
        let commit = leader.raw.store().commit_index();
        leader
            .raw
            .raft
            .raft_log
            .entries(1, u64::MAX, raft::GetEntriesContext::empty(false))
            .unwrap()
            .into_iter()
            .filter(|entry| entry.index <= commit)
            .collect()
    }

    /// Append `entries` as committed entries to the log of member `id` in `dir`. Then open the
    /// member with `configure` and apply them.
    async fn replay(
        dir: &std::path::Path,
        id: u64,
        entries: &[Entry],
        configure: impl FnOnce(&mut NodeOptions),
    ) -> Node {
        let mut config = options(dir);
        config.id = id;
        configure(&mut config);
        let voters = if config.join { vec![] } else { vec![1, 2, 3] };
        let mut storage = RaftStorage::open(&dir.join("raft"), id, "worker-test", voters).unwrap();
        let last = entries.last().unwrap();
        storage
            .persist(
                entries,
                Some(&HardState {
                    term: last.term,
                    vote: 1,
                    commit: last.index,
                }),
                None,
            )
            .unwrap();
        drop(storage);
        let (events, _) = mpsc::channel(256);
        let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap();
        for _ in 0..10 {
            node.drive_ready().await.unwrap();
            node.drain_log().await.unwrap();
            node.drain_apply().await.unwrap();
        }
        assert_eq!(node.state.applied, last.index);
        node
    }

    /// The apply results that must not depend on the member: index, revision, logical size,
    /// alarms, and the bytes of the membership row.
    type Applied = (u64, i64, u64, usize, Option<Vec<u8>>);

    async fn applied(state: &StateMachine) -> Applied {
        let row = state
            .applied_state()
            .get(crate::cluster::KEY)
            .await
            .unwrap();
        (
            state.applied,
            state.revision,
            state.logical_size(),
            state.alarms.len(),
            row.map(|row| row.to_vec()),
        )
    }

    /// Shared mode: two members on one store apply the same log alike. Only their bootstrap
    /// client URL text differs. Earlier, the first entry stored the flags of each member.
    #[tokio::test]
    async fn client_url_text_changes_no_shared_apply_result() {
        let entries = leader_log(-1, 40).await;
        let store = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
        let shared = crate::state::shared::Options {
            quota: 3000,
            voters: vec![1, 2, 3],
            join: false,
            empty_log: true,
            pin_max: Duration::from_secs(300),
            overlay_max_bytes: 1 << 30,
            restore: None,
            record: None,
            store_id: None,
        };
        let mut results = vec![];
        for host in ["127.0.0.1".to_owned(), long_host()] {
            let mut state = StateMachine::open_shared(
                store.clone(),
                "root",
                "test",
                crate::state::StateCache::default(),
                &shared,
            )
            .await
            .unwrap();
            // Node startup builds this membership from the flags.
            let mut members =
                crate::cluster::Store::initial(&options(std::path::Path::new("")).peers);
            for (id, url) in client_urls(&host) {
                members.members.get_mut(&id).unwrap().client_ur_ls = vec![url];
            }
            state.members = std::sync::Arc::new(members);
            let header = apply::Header {
                cluster: 1,
                member: 1,
                term: 1,
            };
            apply::apply_entries(&mut state, entries.clone(), header)
                .await
                .unwrap();
            results.push(applied(&state).await);
            state.close().await.unwrap();
        }
        assert_eq!(results[0], results[1]);
        let (_, revision, _, alarms, row) = &results[0];
        assert!(*revision > 1 && *revision < 41, "revision {revision}");
        assert_eq!(*alarms, 1);
        assert!(row.is_some());
    }

    /// A member that restarts before the seed applies keeps its bootstrap membership in memory
    /// only. It then applies the seed like any other member. A joining member gets the row from
    /// the log.
    #[tokio::test]
    async fn the_row_comes_from_the_seed_after_a_restart_or_a_join() {
        let entries = leader_log(3000, 40).await;
        let seed = entries
            .iter()
            .position(|entry| {
                matches!(
                    RaftCommand::decode(&entry.data),
                    Ok(RaftCommand::SeedMembers(_))
                )
            })
            .unwrap();
        let reference_dir = tempfile::tempdir().unwrap();
        let reference = replay(reference_dir.path(), 2, &entries, |_| {}).await;
        let expected = applied(reference.worker.state()).await;

        let dir = tempfile::tempdir().unwrap();
        let host = long_host();
        let node = replay(dir.path(), 3, &entries[..seed], |config| {
            config.client_peers = client_urls(&host);
        })
        .await;
        assert!(node.state.members_row.is_none());
        assert_eq!(node.state.usage.total(), 0);
        assert_eq!(
            node.state.members.members[&1].client_ur_ls,
            [format!("http://{host}:2380")]
        );
        drop(node);
        let node = replay(dir.path(), 3, &entries[seed..], |config| {
            config.client_peers = client_urls(&host);
        })
        .await;
        assert_eq!(applied(node.worker.state()).await, expected);
        // The stored row replaces the bootstrap membership in memory.
        assert_eq!(
            node.state.members.members[&1].client_ur_ls,
            ["http://127.0.0.1:2380"]
        );

        let dir = tempfile::tempdir().unwrap();
        // A joining member needs the cluster claim on the store of the cluster.
        let store = crate::object::test_store(reference_dir.path());
        let node = replay(dir.path(), 3, &entries, |config| {
            config.join = true;
            config.object_store = crate::object::Config::Test(store, "root".into());
        })
        .await;
        assert_eq!(applied(node.worker.state()).await, expected);
    }

    /// Open a member of a one-voter cluster and tick it until it leads. Return the commands of
    /// its log.
    async fn lead_alone(configure: impl FnOnce(&mut Node)) -> (Node, Vec<RaftCommand>) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = options(dir.path());
        config.peers = BTreeMap::from([(1, "127.0.0.1:1".to_owned())]);
        let (events, _) = mpsc::channel(256);
        let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap();
        configure(&mut node);
        while node.raw.raft.state != StateRole::Leader {
            node.tick().unwrap();
        }
        let commands = node
            .raw
            .raft
            .raft_log
            .entries(1, u64::MAX, raft::GetEntriesContext::empty(false))
            .unwrap()
            .iter()
            .filter(|entry| !entry.data.is_empty())
            .map(|entry| RaftCommand::decode(&entry.data).unwrap())
            .collect();
        (node, commands)
    }

    /// Drive a one-voter shared node until its log and apply are idle.
    async fn settle(node: &mut Node) {
        for _ in 0..5 {
            node.drive_ready().await.unwrap();
            node.drain_log().await.unwrap();
            node.drain_apply().await.unwrap();
        }
    }

    /// Propose a membership change on a one-voter node and apply it.
    async fn change_members(node: &mut Node, request: C) {
        let (tx, _reply) = oneshot::channel();
        let command = RaftCommand::Cluster {
            id: uuid::Uuid::new_v4().to_string(),
            request,
            credentials: Default::default(),
        };
        node.propose(command, tx).unwrap();
        settle(node).await;
    }

    /// The reply of voter 2 from `process` to a new probe, with a correct clock.
    fn sample(clock: &shared::Clock, process: u64) -> bool {
        let probe = clock.probe();
        let now = (clock.mono_ms)();
        let reply = crate::node::ProbeReply {
            nonce: shared::probe_nonce(&probe),
            process,
            clock_ms: (clock.now_ms)(),
        };
        clock.sample(2, probe, reply, now, now)
    }

    /// The node gives the clock check its voters at the moment they change, with no tick
    /// between: a promoted learner, a restarted voter process, and an adopted configuration of
    /// one voter. Each next batch admission follows the new voters.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_clock_check_follows_each_voter_change_before_the_next_batch() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = options(dir.path());
        config.peers = BTreeMap::from([(1, "127.0.0.1:1".to_owned())]);
        let store = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
        config.object_store = crate::object::Config::Test(store, "root".into());
        let (events, _) = mpsc::channel(256);
        let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
            .await
            .unwrap();
        node.clock_in_manual = true;
        // Learner 2 has no process to run a write probe. See `write_proof` for those tests.
        node.skip_write_proof = true;
        while node.raw.raft.state != StateRole::Leader {
            node.tick().unwrap();
        }
        settle(&mut node).await;
        let clock = node.writer.clock().clone();
        node.tick_clock(false);
        let gate = clock.gate().await;
        let admit = |gate: &std::sync::Arc<shared::Gate>| {
            let gate = gate.clone();
            async move {
                let admitted = tokio::time::timeout(Duration::from_millis(300), gate.admit());
                admitted.await.map(|admitted| admitted.done()).is_ok()
            }
        };
        assert!(admit(&gate).await, "a lone voter waited");

        // Promote a learner. Its sample is missing, so the next batch waits.
        let member = |is_learner| crate::proto::etcdserverpb::Member {
            id: 2,
            name: "node-2".into(),
            peer_ur_ls: vec!["http://127.0.0.1:2".into()],
            client_ur_ls: vec!["http://127.0.0.1:2".into()],
            is_learner,
        };
        change_members(&mut node, C::Add(member(true))).await;
        assert!(admit(&gate).await, "a learner is no voter");
        change_members(&mut node, C::Promote(2)).await;
        assert!(node.raw.raft.prs().conf().voters().contains(2));
        assert!(
            !admit(&gate).await,
            "a batch was admitted without the new voter"
        );

        // The voter's process becomes known, and its sample admits the batch.
        let mut responses = vec![];
        let process = |process| Event::PeerProcess {
            member: 2,
            process: Some(process),
        };
        node.handle(process(7), &mut responses).await.unwrap();
        assert!(sample(&clock, 7));
        assert!(
            admit(&gate).await,
            "the voter's sample did not admit the batch"
        );

        // The voter restarts. Its old sample and a late reply of its old process count no more.
        node.handle(process(8), &mut responses).await.unwrap();
        assert!(!sample(&clock, 7));
        assert!(
            !admit(&gate).await,
            "a batch was admitted on the sample of an old process"
        );

        // An adopted base holds one voter again. The gate keeps its check, with the leader's
        // own clock as the majority.
        let conf = raft::eraftpb::ConfState {
            voters: vec![1],
            ..Default::default()
        };
        node.finish_output(Output::Adopted(conf)).unwrap();
        assert!(!node.raw.raft.prs().conf().voters().contains(2));
        assert!(
            admit(&gate).await,
            "the adopted voters did not reach the gate"
        );
    }

    /// A leader proposes `SeedMembers` before the `Publish` of the tick that elects it. A joining
    /// member without the row proposes no seed.
    #[tokio::test]
    async fn a_new_leader_seeds_the_members_before_other_entries() {
        let (mut node, commands) = lead_alone(|node| {
            // A client URL that differs from the flag makes the member publish at once.
            let mut members = node.state.members.as_ref().clone();
            members.members.get_mut(&1).unwrap().client_ur_ls = vec!["http://other:1".into()];
            node.state.members = std::sync::Arc::new(members);
        })
        .await;
        assert!(
            matches!(
                commands.as_slice(),
                [
                    RaftCommand::SeedMembers(crate::cluster::Seed { digest: None, .. }),
                    RaftCommand::Cluster {
                        request: C::Publish { .. },
                        ..
                    },
                ]
            ),
            "{commands:?}"
        );
        // One seed in each term.
        node.propose_term_start().unwrap();
        assert_eq!(node.raw.raft.raft_log.last_index(), 3);

        let (_, commands) = lead_alone(|node| node.options.join = true).await;
        assert!(commands.is_empty(), "{commands:?}");
    }

    /// Shared mode: open a member of a new cluster, which starts from an empty base. Then give
    /// it `commands` as committed entries from index 2, after the empty entry of the leader, and
    /// open it again with the pending log limit `pending`.
    async fn open_new_shared_member(
        dir: &std::path::Path,
        commands: &[RaftCommand],
        pending: usize,
    ) -> Result<Node> {
        let store = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
        let config = || {
            let mut config = options(dir);
            config.object_store = crate::object::Config::Test(store.clone(), "root".into());
            config.client_peers.insert(2, "local-only:8888".into());
            config.log_memory.pending_bytes = pending;
            config
        };
        let open = |config| async move {
            let (events, _) = mpsc::channel(256);
            Node::open_with_transport(config, events, Some(BTreeMap::new())).await
        };
        drop(open(config()).await?);
        let mut storage = RaftStorage::open(&dir.join("raft"), 1, "worker-test", vec![1, 2, 3])?;
        let entries: Vec<_> = std::iter::once(Entry {
            index: 1,
            term: 1,
            ..Default::default()
        })
        .chain((2..).zip(commands).map(|(index, command)| Entry {
            index,
            term: 1,
            data: command.encode().unwrap(),
            ..Default::default()
        }))
        .collect();
        let hs = HardState {
            term: 1,
            vote: 2,
            commit: entries.len() as u64,
        };
        storage.persist(&entries, Some(&hs), None)?;
        drop(storage);
        open(config()).await
    }

    /// Like `open_new_shared_member` with the default pending limit. Then drive apply.
    async fn new_shared_member(dir: &std::path::Path, commands: &[RaftCommand]) -> Node {
        let pending = LogMemory::default().pending_bytes;
        let mut node = open_new_shared_member(dir, commands, pending)
            .await
            .unwrap();
        for _ in 0..5 {
            node.drive_ready().await.unwrap();
            node.drain_log().await.unwrap();
            node.drain_apply().await.unwrap();
        }
        node
    }

    /// A new member applies the seed and then a Put. The cluster claim sets the quota.
    #[tokio::test]
    async fn a_new_shared_member_applies_a_put_after_the_seed() {
        let peers = options(std::path::Path::new("")).peers;
        let seed = RaftCommand::SeedMembers(crate::cluster::Seed {
            id: "seed".into(),
            origin: 2,
            members: serde_json::to_string(&crate::cluster::Store::initial(&peers)).unwrap(),
            digest: None,
            applied: 1,
        });
        let dir = tempfile::tempdir().unwrap();
        let node = new_shared_member(dir.path(), &[seed, put(1)]).await;
        assert_eq!((node.state.applied, node.state.revision), (3, 2));
        assert!(node.state.members_row.is_some());
    }
}
