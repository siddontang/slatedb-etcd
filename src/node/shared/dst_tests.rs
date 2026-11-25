//! Shared-mode simulations of #96: coverage of a base above its row, the leader's demand for a
//! base, and restarts at a record above the row.

use super::*;

impl Simulation {
    /// The end of a shared schedule. A follower asks for a snapshot at its last index, above the
    /// leader's base. The leader certifies its base and records it above the row. Then each
    /// member restarts once. A record above its checkpoint row opens that checkpoint at its row
    /// and raises it.
    pub(super) async fn restart_above_rows(&mut self) {
        if self.leader().is_some() {
            let follower = self.follower(0);
            let node = self.nodes.get_mut(&follower).unwrap();
            if node.raw.request_snapshot().is_ok() {
                self.trace.push(format!("request snapshot {follower}"));
            }
            self.settle(10).await;
        }
        for id in 1..=3 {
            self.stop(id);
            self.start(id).await;
            self.settle(4).await;
        }
        self.settle(10).await;
    }

    /// The process ID that member `id` sends with its traffic. An earlier binary sends none.
    pub(super) fn process(&self, id: u64) -> Option<u64> {
        match self.old.contains(&id) {
            true => None,
            false => self.processes.get(&id).copied(),
        }
    }

    /// Member `id` sends its `ViewAck` to the leader, as the peer stream does. DST has no peer
    /// stream, so each tick of a follower delivers one.
    pub(super) fn report_view(&mut self, id: u64) {
        if self.shared.is_none() || Some(id) == self.isolated {
            return;
        }
        let Some(leader) = self.leader() else {
            return;
        };
        if leader == id || !self.nodes.contains_key(&id) {
            return;
        }
        let pins = self.nodes[&id].dst_pins();
        let coverage = self.process(id);
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .view_ack(id, pins, None, coverage);
    }

    /// The HTTP routes of a snapshot. A view snapshot above its row goes to its own route with
    /// the receiver process that the leader named. An earlier binary has no such route, and
    /// another process refuses it. Then the transport tells the leader that the named process is
    /// not capable, and reports the snapshot as failed. Return the message that Raft gets, if
    /// any.
    pub(super) async fn receive_snapshot(&mut self, mut message: Message) -> Option<Message> {
        let (from, to) = (message.from, message.to);
        if crate::state::transfer::above_row(message.get_snapshot()) {
            let target = <[u8; 8]>::try_from(message.context.as_slice())
                .ok()
                .map(u64::from_le_bytes);
            let receiver = self.process(to);
            if receiver.is_none() || target != receiver {
                // The node names a target for every snapshot above its row.
                assert!(target.is_some());
                self.refused_snapshots += 1;
                self.trace.push(format!("refuse snapshot {from} {to}"));
                if let Some(sender) = self.nodes.get_mut(&from) {
                    sender.raw.store().note_refused(to, target.unwrap());
                    sender.raw.report_snapshot(to, SnapshotStatus::Failure);
                }
                return None;
            }
            message.context.clear();
        }
        if let Ok(Some(view)) =
            crate::state::transfer::ViewDescriptor::decode(message.get_snapshot().get_data())
        {
            self.delivered_views.push(view.checkpoint().unwrap());
        }
        if self.old.contains(&to) {
            self.check_old_install(&message).await;
        }
        Some(message)
    }

    /// An earlier binary installs a view snapshot only when the checkpoint row equals the
    /// snapshot index. Check every view snapshot that such a member receives.
    async fn check_old_install(&mut self, message: &Message) {
        let snapshot = message.get_snapshot();
        let Ok(Some(view)) = crate::state::transfer::ViewDescriptor::decode(snapshot.get_data())
        else {
            return;
        };
        let source = self.nodes[&message.from].shared_source().unwrap();
        let base = source.open_base(view.checkpoint().unwrap()).await.unwrap();
        assert_eq!(
            base.applied,
            snapshot.get_metadata().index,
            "seed {}: an earlier binary got a view snapshot above its row",
            self.seed
        );
        self.old_installs += 1;
    }

    /// A member sends only a record whose checkpoint exists. A sendable record names the live
    /// base, which pins its checkpoint. The base index itself can stay below a received record
    /// when an open fails, so this checks the record that a member can send, not the base.
    pub(super) async fn assert_records_open(&self) {
        let Some(admin) = self
            .nodes
            .values()
            .find_map(|node| node.writer.as_ref().map(|writer| writer.admin.clone()))
        else {
            return;
        };
        let views = crate::state::shared::view_checkpoints(&admin)
            .await
            .unwrap();
        for (&id, node) in &self.nodes {
            let record = node.raw.store().recovery_snapshot();
            let Ok(Some(view)) = crate::state::transfer::ViewDescriptor::decode(record.get_data())
            else {
                continue;
            };
            let checkpoint = view.checkpoint().unwrap();
            if node.raw.store().has_image() {
                assert!(
                    views.iter().any(|(_, view)| view.id == checkpoint),
                    "seed {}: member {id} can send a record of {checkpoint}, which is gone",
                    self.seed
                );
            }
        }
    }

    /// Shared mode: compare the loaded fields of each member with the first member seen at its
    /// index. A member that opens or installs a base above its row must equal one that applied.
    pub(super) fn check_loaded(&mut self) {
        if self.shared.is_none() {
            return;
        }
        for (&id, node) in &self.nodes {
            let state = node.worker.state();
            let fields = state.loaded_fields();
            let first = self.loaded.entry(state.applied).or_insert(fields.clone());
            assert_eq!(
                *first, fields,
                "seed {}: member {id} loaded other fields at {}",
                self.seed, state.applied
            );
        }
    }

    /// Propose a Put on the leader that no client waits for. With `ignore_value` on a missing
    /// key it fails, so it changes no state and stores no batch.
    pub(super) async fn propose_put(&mut self, put: PutRequest) {
        let Some(leader) = self.leader() else {
            return;
        };
        self.next_request += 1;
        let (tx, _) = oneshot::channel();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(
                RaftCommand::Etcd {
                    origin: 1,
                    id: format!("put-{}", self.next_request),
                    etcd: RequestOp {
                        request: Some(Op::RequestPut(put)),
                    },
                },
                ClientResponse::Kv(tx),
            )
            .unwrap();
        // The entry goes to the log before any later proposal.
        self.nodes.get_mut(&leader).unwrap().flush_puts();
        self.trace.push(format!("propose put {leader}"));
        self.drive().await;
    }

    /// Propose a Put that fails and changes no state.
    pub(super) async fn propose_noop(&mut self) {
        self.propose_put(PutRequest {
            key: b"missing".to_vec(),
            ignore_value: true,
            ..Default::default()
        })
        .await;
    }

    /// The committed `ViewCheckpoint` entries: index, checkpoint, and coverage.
    fn announcements(&self) -> Vec<(u64, uuid::Uuid, u64)> {
        self.committed_entries
            .iter()
            .filter_map(|(&index, data)| {
                let entry = Entry::decode(data.as_slice()).unwrap();
                match RaftCommand::decode(&entry.data) {
                    Ok(RaftCommand::ViewCheckpoint {
                        checkpoint, covers, ..
                    }) => Some((index, checkpoint.parse().unwrap(), covers)),
                    _ => None,
                }
            })
            .collect()
    }

    /// Open, materialize, checkpoint, and announce on the leader.
    async fn publish_view(&mut self, writers: &mut BTreeMap<u64, shared::DstWriter>) {
        let leader = self.leader().unwrap();
        for step in [
            WriterStep::Open,
            WriterStep::Materialize,
            WriterStep::Checkpoint,
            WriterStep::Announce,
        ] {
            let open = writers
                .get(&leader)
                .is_some_and(|writer| writer.db.is_some());
            if matches!(step, WriterStep::Open) && open {
                continue;
            }
            self.writer_step(writers, leader, step).await;
        }
    }

    /// Complete the manual checkpoints of member `id` that its base allows, as the run loop
    /// does after each event.
    fn finish_checkpoints(&mut self, id: u64) {
        let node = self.nodes.get_mut(&id).unwrap();
        node.finish_checkpoint_waiters().unwrap();
    }

    /// The newest batch on member `id`.
    fn pushed(&self, id: u64) -> u64 {
        self.nodes[&id]
            .worker
            .state()
            .shared_handles()
            .unwrap()
            .queue
            .pushed()
    }

    /// The base index, the checkpoint, and the overlay bytes of member `id`.
    fn base(&self, id: u64) -> (u64, Option<uuid::Uuid>, u64) {
        let state = self.nodes[&id].worker.state();
        let (index, _, bytes) = state.view_status().unwrap();
        (index, state.base_checkpoint(), bytes)
    }
}

/// Critic's probe for #117 and #96: a follower of an idle shared leader asks for a snapshot at
/// its last index, above the base. The leader announces its base again through its covered
/// index, records it there, and sends it. Raft repeats the request, and the follower asks
/// twice, but one announcement serves them. The idle cluster then stops appending.
///
/// The follower applied through its last index, so it saves the record and installs no state.
#[tokio::test]
async fn an_idle_shared_leader_serves_a_snapshot_request_at_the_last_index() {
    let mut sim = Simulation::with_mode(9, true).await;
    sim.settle(4).await;
    let leader = sim.leader().unwrap();
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let (base, _, _) = sim.base(leader);
    assert!(base > 0);
    let announced = sim.announcements().len();
    let follower = sim.follower(0);
    let node = sim.nodes.get_mut(&follower).unwrap();
    node.raw.request_snapshot().unwrap();
    let requested = node.raw.raft.pending_request_snapshot;
    assert!(requested > base, "requested {requested} at base {base}");
    // Raft drops a second request while the first waits.
    assert!(node.raw.request_snapshot().is_err());
    sim.settle(30).await;
    assert_eq!(sim.leader(), Some(leader));
    let node = &sim.nodes[&follower];
    assert_eq!(node.raw.raft.pending_request_snapshot, 0);
    assert!(node.raw.store().snapshot_index() >= requested);
    let record = Record::of(node.raw.store().recovery_snapshot()).unwrap();
    assert_eq!(record.map(|record| record.checkpoint), sim.base(leader).1);
    assert_eq!(node.state.applied, sim.nodes[&leader].state.applied);
    let announcements = sim.announcements();
    assert_eq!(announcements.len(), announced + 1);
    let (_, checkpoint, covers) = *announcements.last().unwrap();
    assert_eq!(Some(checkpoint), sim.base(leader).1);
    assert!(covers >= requested);
    let last = sim.nodes[&leader].raw.raft.raft_log.last_index();
    sim.settle(60).await;
    assert_eq!(sim.nodes[&leader].raw.raft.raft_log.last_index(), last);
    assert_eq!(sim.announcements().len(), announced + 1);
    sim.assert_records_open().await;
}

/// #117 stops a new member at an earlier binary's `Publish` until a snapshot arrives. A failed
/// update follows the `Publish`, so the last index is above the last batch. In this idle shared
/// cluster the leader's announcement covers that index, and the member recovers by snapshot.
#[tokio::test]
async fn a_guarded_shared_member_recovers_in_an_idle_cluster() {
    guarded_member(false).await;
}

/// The same with the leader's apply behind when the member first asks. The target is the request
/// itself, so the leader serves it once its apply reaches it. The member never applies the
/// blocked entry.
#[tokio::test]
async fn a_guarded_shared_member_recovers_from_a_clamped_target() {
    guarded_member(true).await;
}

async fn guarded_member(lagging: bool) {
    let mut sim = Simulation::with_holds(23, true, BTreeMap::new(), false, true, true).await;
    sim.settle(3).await;
    let publish = RaftCommand::Cluster {
        id: "publish-2".into(),
        request: crate::cluster::Command::Publish {
            id: 2,
            client_urls: vec!["http://localhost:2381".into()],
        },
        credentials: Default::default(),
    };
    sim.admin_command(publish).await.result.unwrap();
    sim.propose_noop().await;
    sim.settle(2).await;
    let waiting = sim.follower(0);
    sim.replace(waiting, 0).await;
    sim.until_waiting(waiting).await;
    assert!(sim.nodes[&waiting].state.members_row.is_none());
    let blocked = sim.nodes[&waiting].quota.blocked().unwrap();
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    let leader = sim.leader().unwrap();
    if lagging {
        sim.paused_apply = Some(leader);
        sim.propose_noop().await;
        for _ in 0..3 {
            sim.settle(1).await;
            assert!(sim.nodes[&waiting].state.applied < blocked);
        }
        sim.paused_apply = None;
    }
    sim.settle(30).await;
    let node = &sim.nodes[&waiting];
    assert_eq!(node.quota.blocked(), None);
    assert!(node.installed_snapshots > 0);
    assert_eq!(node.state.applied, sim.nodes[&leader].state.applied);
    assert!(node.state.members_row.is_some());
}

/// #96 in a cluster. Member `grouped` misses two commits and applies a Put and a failed update
/// in one batch. Member `apart` applies them one by one, so its last batch is one below its
/// applied index. `grouped` then leads and publishes its batch.
///
/// `apart` is over its overlay limit. It adopts the checkpoint, drops its overlay, truncates its
/// log, and completes a manual checkpoint. After a failover it accepts a write.
#[tokio::test]
async fn a_member_that_applied_entries_apart_adopts_the_base_and_leads() {
    let mut sim = Simulation::with_mode(29, true).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let grouped = sim.follower(0);
    let apart = sim.follower(grouped);
    sim.isolated = Some(grouped);
    sim.propose().await;
    sim.settle(1).await;
    sim.propose_noop().await;
    sim.settle(1).await;
    let index = sim.nodes[&apart].state.applied;
    assert_eq!(sim.pushed(apart), index - 1);
    sim.isolated = None;
    sim.settle(2).await;
    assert_eq!(sim.nodes[&grouped].state.applied, index);
    assert_eq!(sim.pushed(grouped), index);
    sim.lead(grouped).await;
    let (_, _, bytes) = sim.base(apart);
    assert!(bytes > 0);
    sim.nodes.get_mut(&apart).unwrap().options.overlay_max_bytes = bytes * 2 / 3;
    assert!(sim.nodes[&apart].overlay_full());
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let (_, checkpoint, _) = *sim.announcements().last().unwrap();
    let (base, adopted, bytes) = sim.base(apart);
    assert!(base >= index, "base {base} below {index}");
    assert_eq!((adopted, bytes), (Some(checkpoint), 0));
    assert!(!sim.nodes[&apart].overlay_full());
    assert!(sim.nodes[&apart].raw.store().snapshot_index() >= index);
    let (tx, mut rx) = oneshot::channel();
    let node = sim.nodes.get_mut(&apart).unwrap();
    node.handle(Event::Checkpoint(tx), &mut vec![])
        .await
        .unwrap();
    sim.settle(1).await;
    sim.finish_checkpoints(apart);
    assert!(rx.try_recv().unwrap().is_ok());
    sim.lead(apart).await;
    let before = sim.nodes[&apart].state.revision;
    sim.propose().await;
    sim.settle(2).await;
    let key = format!("key-{}", sim.next_request);
    assert!(sim.acknowledged.contains_key(&key));
    assert!(sim.nodes[&apart].state.revision > before);
}

/// The reverse order of #96. The leader stores the Put at `j` and no batch at `j + 1`. It
/// announces its checkpoint at `j` before it applies `j + 1`, so the coverage is `j`. Member
/// `grouped` stores one batch at `j + 1`, so its manual checkpoint needs a base through `j + 1`.
/// The leader certifies the same checkpoint through its covered index. No new checkpoint and no
/// client write are needed.
#[tokio::test]
async fn a_follower_manual_checkpoint_completes_with_a_certified_base() {
    let mut sim = Simulation::with_mode(31, true).await;
    // No member truncates, so `grouped` gets the entries and no snapshot.
    for node in sim.nodes.values_mut() {
        node.options.snapshot_every = 0;
        node.next_checkpoint = None;
    }
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let leader = sim.leader().unwrap();
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let grouped = sim.follower(0);
    sim.isolated = Some(grouped);
    sim.propose().await;
    sim.settle(1).await;
    let j = sim.nodes[&leader].state.applied;
    assert_eq!(sim.pushed(leader), j);
    // The failed update is in the log but not committed when the leader announces.
    sim.propose_noop().await;
    for step in [
        WriterStep::Materialize,
        WriterStep::Checkpoint,
        WriterStep::Announce,
    ] {
        sim.writer_step(&mut writers, leader, step).await;
    }
    // Both entries commit first. `grouped` then gets them with the commit at once.
    sim.settle(1).await;
    sim.isolated = None;
    sim.settle(4).await;
    let (_, checkpoint, covers) = *sim.announcements().last().unwrap();
    assert_eq!(covers, j);
    assert_eq!(sim.pushed(grouped), j + 1);
    assert_eq!(sim.base(grouped).0, j);
    let admin = sim.nodes[&leader].writer.as_ref().unwrap().admin.clone();
    let views = crate::state::shared::view_checkpoints(&admin)
        .await
        .unwrap()
        .len();
    let (tx, mut rx) = oneshot::channel();
    let node = sim.nodes.get_mut(&grouped).unwrap();
    node.handle(Event::Checkpoint(tx), &mut vec![])
        .await
        .unwrap();
    let index = *node
        .checkpoint_waiters
        .iter()
        .map(|(i, _)| i)
        .max()
        .unwrap();
    sim.settle(2).await;
    sim.finish_checkpoints(grouped);
    assert!(rx.try_recv().is_err());
    // DST has no peer stream, so the test delivers the follower's report.
    let pins = sim.nodes[&grouped].dst_pins();
    let coverage = sim.process(grouped);
    let node = sim.nodes.get_mut(&leader).unwrap();
    node.view_ack(grouped, pins, Some(index), coverage);
    sim.settle(4).await;
    sim.finish_checkpoints(grouped);
    assert!(rx.try_recv().unwrap().is_ok());
    let (_, certified, covers) = *sim.announcements().last().unwrap();
    assert_eq!(certified, checkpoint);
    assert!(covers >= index);
    assert!(sim.base(grouped).0 >= index);
    assert_eq!(
        crate::state::shared::view_checkpoints(&admin)
            .await
            .unwrap()
            .len(),
        views
    );

    // Critic's probe: `grouped` released its batch at `j + 1`, and now it leads. Its writer
    // opens below that batch, at `j`.
    sim.lead(grouped).await;
    sim.writer_step(&mut writers, grouped, WriterStep::Open)
        .await;
    let writer = &writers[&grouped];
    assert_eq!(writer.written, j);
    let queue = sim.nodes[&grouped]
        .worker
        .state()
        .shared_handles()
        .unwrap()
        .queue;
    assert!(queue.after(writer.written, 1).is_empty());
    // The idle materializer waits for the next batch. The timer goes first, so a busy future
    // cannot starve it.
    use std::future::Future;
    let progress = std::sync::Arc::new(shared::Progress::new(writer.written));
    let mut materializer = Box::pin(shared::materialize(
        writer.db.as_ref().unwrap().clone(),
        queue,
        progress,
    ));
    let mut polls = 0;
    tokio::select! {
        biased;
        _ = tokio::time::sleep(Duration::from_millis(30)) => {}
        result = std::future::poll_fn(|cx| {
            polls += 1;
            materializer.as_mut().poll(cx)
        }) => panic!("idle materializer returned: {result:?}"),
    }
    drop(materializer);
    // A new request extends the same checkpoint without a client write: the released batch
    // holds no change above the row.
    sim.settle(2).await;
    let covered = sim.base(grouped).0;
    let wanted = sim.nodes[&grouped].state.applied;
    assert!(wanted > covered);
    let receiver = sim.follower(grouped);
    let store = sim.nodes[&grouped].raw.store();
    assert!(store.snapshot(wanted, receiver).is_err());
    assert_eq!(store.wanted_index(), wanted);
    let announced = sim.announcements().len();
    sim.settle(30).await;
    let store = sim.nodes[&grouped].raw.store();
    assert_eq!(
        ((1..=4).contains(&polls), store.wanted_index()),
        (true, 0),
        "idle materializer polled {polls} times; durable snapshot at {}",
        store.snapshot_index()
    );
    assert!(store.snapshot_index() >= wanted);
    assert_eq!(sim.announcements().len(), announced + 1);
    assert_eq!(
        crate::state::shared::view_checkpoints(&admin)
            .await
            .unwrap()
            .len(),
        views
    );
    // The new leader publishes, accepts a write, and stops cleanly.
    sim.propose().await;
    sim.settle(2).await;
    let key = format!("key-{}", sim.next_request);
    assert!(sim.acknowledged.contains_key(&key));
    sim.publish_view(&mut writers).await;
    sim.settle(4).await;
    assert!(sim.base(grouped).0 > wanted);
    if let Some(mut writer) = writers.remove(&grouped) {
        writer.close().await;
    }
    sim.stop(grouped);
}

/// Condition (a): a Put at `b + 1` is applied but not materialized, and a failed update follows.
/// The checkpoint at `b` then covers only `b`, also for a snapshot request. A publication that
/// holds the Put serves the request, and the receiver reads the Put.
#[tokio::test]
async fn a_buffered_put_keeps_the_coverage_at_the_checkpoint_row() {
    let mut sim = Simulation::with_mode(37, true).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(3).await;
    let leader = sim.leader().unwrap();
    let mut writers = BTreeMap::new();
    for step in [
        WriterStep::Open,
        WriterStep::Materialize,
        WriterStep::Checkpoint,
    ] {
        sim.writer_step(&mut writers, leader, step).await;
    }
    let b = sim.pushed(leader);
    // The follower stores the next entries but applies none, so the snapshot installs state.
    let follower = sim.follower(0);
    sim.paused_apply = Some(follower);
    sim.propose().await;
    let key = format!("key-{}", sim.next_request);
    sim.propose_noop().await;
    sim.settle(2).await;
    assert_eq!(sim.pushed(leader), b + 1);
    sim.writer_step(&mut writers, leader, WriterStep::Announce)
        .await;
    sim.settle(4).await;
    let (_, checkpoint, covers) = *sim.announcements().last().unwrap();
    assert_eq!(covers, b);
    assert_eq!(sim.base(leader), (b, Some(checkpoint), sim.base(leader).2));
    let announced = sim.announcements().len();
    let node = sim.nodes.get_mut(&follower).unwrap();
    node.raw.request_snapshot().unwrap();
    let requested = node.raw.raft.pending_request_snapshot;
    sim.settle(10).await;
    assert_eq!(sim.announcements().len(), announced);
    assert!(sim.nodes[&follower].raw.store().snapshot_index() < requested);
    for step in [
        WriterStep::Materialize,
        WriterStep::Checkpoint,
        WriterStep::Announce,
    ] {
        sim.writer_step(&mut writers, leader, step).await;
    }
    sim.settle(20).await;
    // The served checkpoint is the new publication, and it holds the Put.
    let record = sim.nodes[&follower].raw.store().recovery_snapshot().clone();
    let record = Record::of(&record).unwrap().unwrap();
    assert!(record.index >= requested);
    assert_ne!(record.checkpoint, checkpoint);
    let source = sim.nodes[&leader].writer.as_ref().unwrap().source.clone();
    let served = source.open_base(record.checkpoint).await.unwrap();
    assert!(served.applied > b);
    let current = format!("etcd/current/{key}");
    assert!(served.get(current.as_bytes()).await.unwrap().is_some());
    sim.paused_apply = None;
    sim.settle(10).await;
    let node = &sim.nodes[&follower];
    assert_eq!(node.state.applied, sim.nodes[&leader].state.applied);
    let row = node
        .state
        .read
        .state()
        .get(format!("etcd/current/{key}").as_bytes())
        .await
        .unwrap();
    assert!(row.is_some());
}

/// Condition (b): checkpoint X has row `b` in term 1. After an election, the leader certifies
/// X through `c` in term 2 for a snapshot request. The follower saves the record and stops
/// before the install. A Defragment then adds checkpoint Y at row `b`, so Y is the newest. The
/// restart opens X by the record ID at `c` with term 2. It loads the same fields as a member
/// that applied through `c`, and it applies the later entries once.
#[tokio::test]
async fn a_member_restarts_at_a_record_above_its_checkpoint_row() {
    let mut sim = Simulation::with_mode(41, true).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(3).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let first = sim.leader().unwrap();
    let (b, x, _) = sim.base(first);
    let x = x.unwrap();
    let term = sim.nodes[&first].raw.raft.term;
    let leader = sim.follower(0);
    sim.lead(leader).await;
    let term2 = sim.nodes[&leader].raw.raft.term;
    assert!(term2 > term);
    // The member stores the next entry but does not apply it.
    let member = sim.follower(0);
    sim.paused_apply = Some(member);
    sim.propose_noop().await;
    sim.settle(2).await;
    sim.nodes
        .get_mut(&member)
        .unwrap()
        .raw
        .request_snapshot()
        .unwrap();
    for _ in 0..40 {
        if sim.nodes[&member].raw.store().snapshot_index() > b {
            break;
        }
        sim.settle(1).await;
    }
    let record = sim.nodes[&member].raw.store().recovery_snapshot().clone();
    let record = Record::of(&record).unwrap().unwrap();
    assert_eq!((record.checkpoint, record.term), (x, term2));
    let c = record.index;
    assert!(c > b);
    assert!(sim.nodes[&member].state.applied < c);
    sim.stop(member);
    sim.writer_step(&mut writers, leader, WriterStep::Open)
        .await;
    let db = writers[&leader].db.clone().unwrap();
    assert_eq!(db.manifest().last_l0_seq(), b);
    let options = slatedb::config::CheckpointOptions {
        name: Some(crate::state::shared::view_name(b)),
        ..Default::default()
    };
    let y = db
        .create_checkpoint(slatedb::config::CheckpointScope::Durable, &options)
        .await
        .unwrap()
        .id;
    let admin = sim.nodes[&leader].writer.as_ref().unwrap().admin.clone();
    let views = crate::state::shared::view_checkpoints(&admin)
        .await
        .unwrap();
    assert_eq!(views.last().unwrap().1.id, y);
    sim.paused_apply = None;
    sim.start(member).await;
    let state = sim.nodes[&member].worker.state();
    assert_eq!((state.applied, state.applied_term), (c, term2));
    assert_eq!(state.base_checkpoint(), Some(x));
    assert_eq!(Some(&state.loaded_fields()), sim.loaded.get(&c));
    sim.settle(4).await;
    sim.propose().await;
    sim.settle(4).await;
    let node = &sim.nodes[&member];
    assert_eq!(node.state.applied, sim.nodes[&leader].state.applied);
    assert_eq!(node.state.revision, sim.nodes[&leader].state.revision);
    sim.assert_acknowledged(member).await;
}

impl Simulation {
    /// Deliver messages inside the groups `{1, 2}` and `{3, 4, 5}` only. Member 2 also loses
    /// its acceptances to member 1, so member 1 cannot commit the suffix that member 2 holds,
    /// and it loses the snapshots of member 1, so its request outlives the term.
    async fn pump_partition(&mut self) {
        self.drive().await;
        for _ in 0..150 {
            self.messages.retain(|m| {
                (m.from <= 2) == (m.to <= 2)
                    && !(m.from == 2
                        && m.to == 1
                        && m.get_msg_type() == MessageType::MsgAppendResponse
                        && !m.reject)
                    && !(m.from == 1 && m.get_msg_type() == MessageType::MsgSnapshot)
            });
            if self.messages.is_empty() {
                break;
            }
            self.deliver(false).await;
        }
    }
}

/// Sig-Codex's five-voter probe. Member 2 gets an uncommitted suffix through `high` from member
/// 1 and asks for a snapshot there. Members 3 to 5 elect a leader that replaces the suffix.
///
/// raft-rs keeps the pending request across the term change, and member 2 sends `high` again to
/// each leader. A leader serves `min(high, last)` and pins that target, so member 2 restores a
/// snapshot below `high`, which clears its request, and then takes appends. With `restart`,
/// member 2 restarts instead, and the old request of the earlier term must not hold back
/// member 4's request after leadership goes back to member 1. Both state modes use this rule.
async fn stale_request(shared: bool, restart: bool) {
    let mut sim = Simulation::with_size(9, shared, 5).await;
    sim.settle(5).await;
    let first = sim.leader().unwrap();
    assert_eq!(first, 1);
    let mut writers = BTreeMap::new();
    if shared {
        sim.publish_view(&mut writers).await;
    }
    sim.settle(10).await;
    for _ in 0..30 {
        sim.propose_noop().await;
    }
    let high = sim.nodes[&first].raw.raft.raft_log.last_index();
    assert!(high > sim.nodes[&first].raw.raft.raft_log.committed + 20);
    sim.pump_partition().await;
    assert_eq!(sim.nodes[&2].raw.raft.raft_log.last_index(), high);
    sim.nodes
        .get_mut(&2)
        .unwrap()
        .raw
        .request_snapshot()
        .unwrap();
    sim.pump_partition().await;
    // Member 1 holds the suffix, so the request is within its log.
    assert_eq!(sim.nodes[&first].raw.store().wanted_index(), high);
    if restart {
        sim.stop(2);
    }
    for _ in 0..65 {
        for id in 1..=5 {
            sim.tick(id).await;
        }
        sim.pump_partition().await;
    }
    let next = sim.leader().unwrap();
    assert!(next > 2);
    if restart {
        sim.start(2).await;
    } else {
        assert_eq!(sim.nodes[&2].raw.raft.pending_request_snapshot, high);
    }
    sim.settle(20).await;
    if !restart {
        // Member 2 restored a snapshot below `high` and then took the appends of `next`.
        let node = &sim.nodes[&2];
        assert_eq!(node.raw.raft.pending_request_snapshot, 0);
        assert!(node.raw.store().snapshot_index() < high);
        let leader = &sim.nodes[&next];
        assert_eq!(node.state.applied, leader.state.applied);
        assert_eq!(leader.raw.store().wanted_index(), 0);
        // No target chases the announcements: the log of the idle cluster stays put.
        let last = leader.raw.raft.raft_log.last_index();
        sim.settle(60).await;
        assert_eq!(sim.nodes[&next].raw.raft.raft_log.last_index(), last);
    }
    assert!(sim.nodes[&first].raw.raft.raft_log.last_index() < high);
    sim.nodes.get_mut(&next).unwrap().raw.transfer_leader(first);
    sim.settle(35).await;
    assert_eq!(sim.leader(), Some(first));
    assert!(sim.nodes[&first].raw.store().wanted_index() < high);
    let follower = 4;
    sim.nodes
        .get_mut(&follower)
        .unwrap()
        .raw
        .request_snapshot()
        .unwrap();
    let current = sim.nodes[&follower].raw.raft.pending_request_snapshot;
    assert!(current < high);
    sim.settle(60).await;
    assert_eq!(sim.nodes[&follower].raw.raft.pending_request_snapshot, 0);
    for id in 1..=5 {
        let node = &sim.nodes[&id];
        assert_eq!(node.raw.raft.pending_request_snapshot, 0, "member {id}");
        assert_eq!(
            node.state.applied, sim.nodes[&first].state.applied,
            "member {id}"
        );
    }
}

#[tokio::test]
async fn an_earlier_term_request_does_not_hold_back_a_current_one() {
    stale_request(true, true).await;
}

#[tokio::test]
async fn a_requester_of_an_earlier_term_takes_a_snapshot_below_its_request() {
    stale_request(true, false).await;
}

#[tokio::test]
async fn a_replicated_requester_of_an_earlier_term_recovers() {
    stale_request(false, false).await;
}

/// Rolling upgrade: member `old` runs an earlier binary. It installs a view snapshot only at its
/// checkpoint row, and it does not advertise the coverage capability. The leader then never
/// certifies above a row, and every snapshot of the old member is at its row. After the member
/// runs this binary and reports, announcements cover the no-op entries again.
#[tokio::test]
async fn a_leader_with_an_earlier_binary_follower_announces_at_the_row() {
    let mut sim = Simulation::with_mode(43, true).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    let leader = sim.leader().unwrap();
    let old = sim.follower(0);
    sim.old.insert(old);
    sim.settle(2).await;
    assert!(!sim.nodes[&leader].extension_allowed());
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(4).await;
    // The old member falls behind the truncated log.
    sim.stop(old);
    for _ in 0..3 {
        sim.write(4).await;
        sim.propose_noop().await;
        sim.publish_view(&mut writers).await;
        sim.settle(4).await;
    }
    // Each announcement covers exactly its checkpoint row.
    let source = sim.nodes[&leader].shared_source().unwrap();
    for (index, checkpoint, covers) in sim.announcements() {
        let row = source.open_base(checkpoint).await.unwrap().applied;
        assert_eq!(covers, row, "the announcement at {index}");
    }
    let record = sim.nodes[&leader].raw.store().recovery_snapshot().clone();
    let record = Record::of(&record).unwrap().unwrap();
    assert_eq!(record.row, None, "the leader recorded {record:?}");
    sim.start(old).await;
    sim.settle(20).await;
    assert!(sim.old_installs > 0);
    assert_eq!(
        sim.nodes[&old].state.applied,
        sim.nodes[&leader].state.applied
    );
    // The member upgrades: its next process reports the capability.
    sim.old.remove(&old);
    sim.stop(old);
    sim.start(old).await;
    sim.settle(2).await;
    assert!(sim.nodes[&leader].extension_allowed());
    sim.write(1).await;
    for step in [WriterStep::Materialize, WriterStep::Checkpoint] {
        sim.writer_step(&mut writers, leader, step).await;
    }
    sim.propose_noop().await;
    sim.settle(1).await;
    sim.writer_step(&mut writers, leader, WriterStep::Announce)
        .await;
    sim.settle(4).await;
    let (_, _, covers) = *sim.announcements().last().unwrap();
    let (base, checkpoint, _) = sim.base(leader);
    let source = sim.nodes[&leader].shared_source().unwrap();
    let row = source.open_base(checkpoint.unwrap()).await.unwrap().applied;
    assert!(
        covers > row && base > row,
        "covers {covers}, base {base}, row {row}"
    );
}

/// The leader recorded its base above the row while every member advertised the capability.
/// Then a member is replaced by an earlier binary with the same ID, in the same term, or after
/// the leader restarts. Its first frame names no process, so it loses the capability. The leader
/// does not send the record above the row. It stores the apply marker at its applied index,
/// publishes a checkpoint with its row there, and sends that one.
async fn replaced_by_an_earlier_binary(restart_leader: bool) {
    let mut sim = Simulation::with_mode(47, true).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let leader = sim.leader().unwrap();
    let requester = sim.follower(0);
    sim.nodes
        .get_mut(&requester)
        .unwrap()
        .raw
        .request_snapshot()
        .unwrap();
    sim.settle(20).await;
    let record = sim.nodes[&leader].raw.store().recovery_snapshot().clone();
    let record = Record::of(&record).unwrap().unwrap();
    assert!(
        record.row.is_some_and(|row| row < record.index),
        "{record:?}"
    );
    let term = sim.nodes[&leader].raw.raft.term;
    let mut leader = leader;
    if restart_leader {
        if let Some(mut writer) = writers.remove(&leader) {
            writer.close().await;
        }
        sim.stop(leader);
        sim.start(leader).await;
        sim.settle(30).await;
        leader = sim.leader().unwrap();
        let record = sim.nodes[&leader].raw.store().recovery_snapshot().clone();
        let record = Record::of(&record).unwrap().unwrap();
        assert!(record.row.is_some_and(|row| row < record.index));
    }
    let replaced = sim.follower(0);
    let installs = sim.old_installs;
    sim.old.insert(replaced);
    sim.replace(replaced, 0).await;
    sim.settle(10).await;
    assert_eq!(sim.old_installs, installs);
    assert!(sim.nodes[&leader].raw.store().exact_wanted());
    sim.publish_view(&mut writers).await;
    sim.settle(30).await;
    if !restart_leader {
        assert_eq!(sim.nodes[&leader].raw.raft.term, term);
    }
    assert!(sim.old_installs > installs);
    assert_eq!(
        sim.nodes[&replaced].state.applied,
        sim.nodes[&leader].state.applied
    );
    sim.assert_acknowledged(replaced).await;
}

#[tokio::test]
async fn a_member_replaced_by_an_earlier_binary_gets_a_snapshot_at_its_row() {
    replaced_by_an_earlier_binary(false).await;
}

#[tokio::test]
async fn a_restarted_leader_sends_an_earlier_binary_a_snapshot_at_its_row() {
    replaced_by_an_earlier_binary(true).await;
}

impl Simulation {
    /// Deliver messages until a view snapshot above its row waits for member `to`. Return false
    /// when none comes.
    async fn until_covered_snapshot(&mut self, to: u64) -> bool {
        for _ in 0..3000 {
            let waiting = self.messages.iter().any(|message| {
                message.to == to
                    && message.get_msg_type() == MessageType::MsgSnapshot
                    && crate::state::transfer::above_row(message.get_snapshot())
            });
            if waiting {
                return true;
            }
            if self.messages.is_empty() {
                let leader = self.leader().unwrap();
                self.tick(leader).await;
            }
            self.deliver(false).await;
        }
        false
    }

    /// Member `requester` asks for a snapshot at its last index, and the leader serves it with a
    /// checkpoint at its row, published by `writers`. Both members then hold a view record.
    /// Return the record of `id`.
    async fn record_by_request(
        &mut self,
        writers: &mut BTreeMap<u64, shared::DstWriter>,
        requester: u64,
        id: u64,
    ) -> crate::state::shared::Record {
        let node = self.nodes.get_mut(&requester).unwrap();
        node.raw.request_snapshot().unwrap();
        self.settle(10).await;
        self.publish_view(writers).await;
        self.settle(20).await;
        assert_eq!(self.nodes[&requester].raw.raft.pending_request_snapshot, 0);
        let record = self.nodes[&id].raw.store().recovery_snapshot().clone();
        Record::of(&record).unwrap().unwrap()
    }

    /// Make the record of the leader above its row: a follower asks for a snapshot at its last
    /// index, and the leader certifies its base through it.
    async fn certify_for(&mut self, follower: u64) -> crate::state::shared::Record {
        let node = self.nodes.get_mut(&follower).unwrap();
        node.raw.request_snapshot().unwrap();
        self.settle(20).await;
        let leader = self.leader().unwrap();
        let record = self.nodes[&leader].raw.store().recovery_snapshot().clone();
        Record::of(&record).unwrap().unwrap()
    }
}

/// Critic's send race. The leader builds a snapshot above its row for the process that
/// advertised the capability. Before it arrives, the member restarts on an earlier binary. The
/// earlier binary has no route for it and refuses it before Raft, so it never stores it. The
/// leader then treats the member as an earlier binary, and the member catches up.
#[tokio::test]
async fn a_snapshot_above_its_row_to_a_restarted_earlier_binary_is_refused() {
    let mut sim = Simulation::with_mode(53, true).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let leader = sim.leader().unwrap();
    let member = sim.follower(0);
    // The member falls behind, then asks for a snapshot above the base.
    sim.propose_noop().await;
    sim.settle(4).await;
    sim.nodes
        .get_mut(&member)
        .unwrap()
        .raw
        .request_snapshot()
        .unwrap();
    assert!(sim.until_covered_snapshot(member).await);
    // The same member starts again on an earlier binary before the snapshot arrives.
    sim.stop(member);
    sim.old.insert(member);
    sim.start(member).await;
    let refused = sim.refused_snapshots;
    sim.settle(20).await;
    assert!(sim.refused_snapshots > refused);
    assert!(!sim.nodes[&leader].raw.store().coverage_capable(member));
    sim.publish_view(&mut writers).await;
    sim.settle(30).await;
    assert_eq!(sim.leader(), Some(leader));
    let record = Record::of(sim.nodes[&member].raw.store().recovery_snapshot()).unwrap();
    assert!(record.is_none_or(|record| record.row.is_none()));
    assert_eq!(
        sim.nodes[&member].state.applied,
        sim.nodes[&leader].state.applied
    );
    sim.assert_acknowledged(member).await;
}

/// Critic's stall. An earlier binary asks for a snapshot above the record: one above its row,
/// or one at its row. Its request feeds the publication of a checkpoint at its row.
async fn an_earlier_binary_request(above_row: bool) {
    let mut sim = Simulation::with_mode(59, true).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let leader = sim.leader().unwrap();
    let old = sim.follower(0);
    if above_row {
        let record = sim.certify_for(sim.follower(old)).await;
        assert!(record.row.is_some_and(|row| row < record.index));
    }
    sim.old.insert(old);
    sim.propose_noop().await;
    sim.settle(4).await;
    let record = sim.nodes[&leader].raw.store().snapshot_index();
    let node = sim.nodes.get_mut(&old).unwrap();
    node.raw.request_snapshot().unwrap();
    let requested = node.raw.raft.pending_request_snapshot;
    assert!(requested > record);
    sim.settle(10).await;
    assert!(sim.nodes[&leader].raw.store().exact_wanted());
    sim.publish_view(&mut writers).await;
    sim.settle(30).await;
    let node = &sim.nodes[&old];
    assert_eq!(node.raw.raft.pending_request_snapshot, 0);
    let served = Record::of(node.raw.store().recovery_snapshot()).unwrap();
    assert_eq!(served.map(|record| record.row), Some(None));
    assert!(node.raw.store().snapshot_index() >= requested);
    assert!(sim.old_installs > 0);
}

#[tokio::test]
async fn an_earlier_binary_request_above_a_record_above_its_row_is_served() {
    an_earlier_binary_request(true).await;
}

#[tokio::test]
async fn an_earlier_binary_request_above_a_record_at_its_row_is_served() {
    an_earlier_binary_request(false).await;
}

/// Sig-Codex's inverse case. A member of this binary asks above the record while another voter
/// runs an earlier binary, so no certification can serve it. The leader publishes a checkpoint
/// at its row instead, with no client write, and the requester recovers.
#[tokio::test]
async fn a_request_beside_an_earlier_binary_voter_gets_a_snapshot_at_its_row() {
    let mut sim = Simulation::with_mode(61, true).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let leader = sim.leader().unwrap();
    let old = sim.follower(0);
    let member = sim.follower(old);
    sim.old.insert(old);
    sim.propose_noop().await;
    sim.settle(4).await;
    assert!(!sim.nodes[&leader].extension_allowed());
    let record = sim.nodes[&leader].raw.store().snapshot_index();
    let node = sim.nodes.get_mut(&member).unwrap();
    node.raw.request_snapshot().unwrap();
    let requested = node.raw.raft.pending_request_snapshot;
    assert!(requested > record);
    let acknowledged = sim.acknowledged.len();
    sim.settle(10).await;
    sim.publish_view(&mut writers).await;
    sim.settle(30).await;
    assert_eq!(sim.acknowledged.len(), acknowledged);
    let node = &sim.nodes[&member];
    assert_eq!(node.raw.raft.pending_request_snapshot, 0);
    assert!(node.raw.store().snapshot_index() >= requested);
    let served = Record::of(node.raw.store().recovery_snapshot()).unwrap();
    assert_eq!(served.map(|record| record.row), Some(None));
}

/// Critic's stale descriptor. Retention deleted checkpoint X of the leader's record above its
/// row, and the restart opened Y at the record index instead. A new member then needs a
/// snapshot. The leader records Y at that index and sends it, and it never sends X.
#[tokio::test]
async fn a_restart_on_another_checkpoint_sends_that_checkpoint() {
    let mut sim = Simulation::with_mode(67, true).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let leader = sim.leader().unwrap();
    let record = sim.certify_for(sim.follower(0)).await;
    let x = record.checkpoint;
    let row = record.row.unwrap();
    assert!(row < record.index);
    // Y has the same row, and retention deletes X.
    let db = writers[&leader].db.clone().unwrap();
    let options = slatedb::config::CheckpointOptions {
        name: Some(crate::state::shared::view_name(row)),
        ..Default::default()
    };
    let y = db
        .create_checkpoint(slatedb::config::CheckpointScope::Durable, &options)
        .await
        .unwrap()
        .id;
    let admin = sim.nodes[&leader].writer.as_ref().unwrap().admin.clone();
    admin.delete_checkpoint(x).await.unwrap();
    if let Some(mut writer) = writers.remove(&leader) {
        writer.close().await;
    }
    // Only the leader restarts. The others keep the term, so it leads again.
    sim.stop(leader);
    sim.start(leader).await;
    let restarted = sim.delivered_views.len();
    let node = &sim.nodes[&leader];
    assert_eq!(node.worker.state().base_checkpoint(), Some(y));
    assert_eq!(node.state.applied, record.index);
    sim.settle(30).await;
    let now = sim.leader().unwrap();
    let joining = sim.follower(0);
    sim.replace(joining, 0).await;
    sim.settle(40).await;
    let delivered = &sim.delivered_views[restarted..];
    assert!(!delivered.contains(&x), "{delivered:?}");
    assert!(delivered.contains(&y), "{delivered:?}");
    let node = &sim.nodes[&joining];
    assert_eq!(node.state.applied, sim.nodes[&now].state.applied);
    if now == leader {
        let record = Record::of(sim.nodes[&leader].raw.store().recovery_snapshot());
        assert_ne!(record.unwrap().unwrap().checkpoint, x);
    }
}

/// Sig-Codex: the reverse grouping case beside a voter of an earlier binary. No certification
/// can serve the follower's manual checkpoint, and the leader's base is at its row below the
/// demand. The leader publishes a checkpoint at its applied index, and the manual checkpoint
/// completes. Automatic truncation is off, so only that publication can serve it.
#[tokio::test]
async fn a_follower_manual_checkpoint_beside_an_earlier_binary_gets_a_row_checkpoint() {
    let mut sim = Simulation::with_mode(31, true).await;
    for node in sim.nodes.values_mut() {
        node.options.snapshot_every = 0;
        node.next_checkpoint = None;
    }
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let leader = sim.leader().unwrap();
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let grouped = sim.follower(0);
    let old = sim.follower(grouped);
    sim.old.insert(old);
    sim.isolated = Some(grouped);
    sim.propose().await;
    sim.settle(1).await;
    let j = sim.nodes[&leader].state.applied;
    sim.propose_noop().await;
    for step in [
        WriterStep::Materialize,
        WriterStep::Checkpoint,
        WriterStep::Announce,
    ] {
        sim.writer_step(&mut writers, leader, step).await;
    }
    sim.settle(1).await;
    sim.isolated = None;
    sim.settle(4).await;
    assert!(!sim.nodes[&leader].extension_allowed());
    assert_eq!(sim.pushed(grouped), j + 1);
    assert_eq!(sim.base(grouped).0, j);
    let (tx, mut rx) = oneshot::channel();
    let node = sim.nodes.get_mut(&grouped).unwrap();
    node.handle(Event::Checkpoint(tx), &mut vec![])
        .await
        .unwrap();
    let index = *node
        .checkpoint_waiters
        .iter()
        .map(|(i, _)| i)
        .max()
        .unwrap();
    let pins = sim.nodes[&grouped].dst_pins();
    let coverage = sim.process(grouped);
    let node = sim.nodes.get_mut(&leader).unwrap();
    node.view_ack(grouped, pins, Some(index), coverage);
    sim.settle(4).await;
    sim.finish_checkpoints(grouped);
    assert!(rx.try_recv().is_err());
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    sim.finish_checkpoints(grouped);
    assert!(rx.try_recv().unwrap().is_ok());
    let (base, checkpoint, _) = sim.base(grouped);
    let source = sim.nodes[&leader].shared_source().unwrap();
    let row = source.open_base(checkpoint.unwrap()).await.unwrap().applied;
    assert!(base >= index && row >= index, "base {base}, row {row}");
}

/// Sig-Codex: a 409 for an earlier process of a member arrives after its new process reported
/// the capability. The new process stays capable, and the leader certifies its base again with
/// no new checkpoint.
#[tokio::test]
async fn a_late_refusal_for_an_earlier_process_keeps_the_capability() {
    let mut sim = Simulation::with_mode(9, true).await;
    sim.settle(4).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let leader = sim.leader().unwrap();
    let follower = sim.follower(0);
    let process = sim.process(follower).unwrap();
    sim.nodes[&leader]
        .raw
        .store()
        .note_refused(follower, process + 1000);
    assert!(sim.nodes[&leader].raw.store().coverage_capable(follower));
    let admin = sim.nodes[&leader].writer.as_ref().unwrap().admin.clone();
    let views = crate::state::shared::view_checkpoints(&admin)
        .await
        .unwrap()
        .len();
    let announced = sim.announcements().len();
    let node = sim.nodes.get_mut(&follower).unwrap();
    node.raw.request_snapshot().unwrap();
    sim.settle(30).await;
    assert_eq!(sim.nodes[&follower].raw.raft.pending_request_snapshot, 0);
    assert_eq!(sim.announcements().len(), announced + 1);
    assert_eq!(
        crate::state::shared::view_checkpoints(&admin)
            .await
            .unwrap()
            .len(),
        views
    );
}

/// Architecture: the leader's base moves from X to a newer checkpoint in process, and its record
/// still names X at a lower index. Retention deletes X. A lagging member then gets a record of
/// the base, never X.
#[tokio::test]
async fn a_record_of_an_earlier_base_is_rebuilt_from_the_base() {
    let mut sim = Simulation::with_size(73, true, 5).await;
    for node in sim.nodes.values_mut() {
        node.options.snapshot_every = 0;
        node.next_checkpoint = None;
    }
    sim.settle(5).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    let leader = sim.leader().unwrap();
    let member = sim.follower(0);
    let lagging = sim.follower(member);
    sim.isolated = Some(lagging);
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let record = sim.record_by_request(&mut writers, member, leader).await;
    let x = record.checkpoint;
    sim.write(3).await;
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let (index, base, _) = sim.base(leader);
    assert!(index > record.index && base != Some(x));
    let kept = Record::of(sim.nodes[&leader].raw.store().recovery_snapshot());
    assert_eq!(kept.unwrap().map(|record| record.checkpoint), Some(x));
    let admin = sim.nodes[&leader].writer.as_ref().unwrap().admin.clone();
    admin.delete_checkpoint(x).await.unwrap();
    assert!(!sim.nodes[&leader].raw.store().has_image());
    let delivered = sim.delivered_views.len();
    sim.isolated = None;
    sim.settle(40).await;
    let views = &sim.delivered_views[delivered..];
    assert!(!views.is_empty() && !views.contains(&x), "{views:?}");
    assert_eq!(
        sim.nodes[&lagging].state.applied,
        sim.nodes[&leader].state.applied
    );
}

/// Critic: a member's record names X at 15, and a newer checkpoint has a higher row. Retention
/// deletes X, and the member restarts on the newer checkpoint. The record is not sendable at
/// once, before any tick. The member then leads, and a lagging member gets a record of the base,
/// never X.
#[tokio::test]
async fn a_restart_above_its_record_does_not_send_a_deleted_checkpoint() {
    let mut sim = Simulation::with_size(79, true, 5).await;
    for node in sim.nodes.values_mut() {
        node.options.snapshot_every = 0;
        node.next_checkpoint = None;
    }
    sim.settle(5).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    let member = sim.follower(0);
    let lagging = sim.follower(member);
    sim.isolated = Some(lagging);
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let record = sim.record_by_request(&mut writers, member, member).await;
    let x = record.checkpoint;
    sim.write(3).await;
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    assert_ne!(sim.base(member).1, Some(x));
    sim.stop(member);
    let leader = sim.leader().unwrap();
    let admin = sim.nodes[&leader].writer.as_ref().unwrap().admin.clone();
    admin.delete_checkpoint(x).await.unwrap();
    sim.start(member).await;
    let node = &sim.nodes[&member];
    assert!(node.state.view.unwrap().0 > record.index);
    assert_ne!(node.worker.state().base_checkpoint(), Some(x));
    assert_eq!(node.raw.store().snapshot_index(), record.index);
    assert!(!node.raw.store().has_image());
    if let Some(mut writer) = writers.remove(&leader) {
        writer.close().await;
    }
    sim.lead(member).await;
    let delivered = sim.delivered_views.len();
    sim.isolated = None;
    sim.settle(40).await;
    let views = &sim.delivered_views[delivered..];
    assert!(!views.is_empty() && !views.contains(&x), "{views:?}");
    assert_eq!(
        sim.nodes[&lagging].state.applied,
        sim.nodes[&member].state.applied
    );
    sim.assert_records_open().await;
}

impl Simulation {
    /// Member `requester` asks for a snapshot at its applied index, which is its commit index,
    /// above its base. Deliver until Raft stores the record. The leader's announcement of the
    /// record checkpoint has not applied there yet. Return the record.
    async fn record_at_applied(&mut self, requester: u64) -> crate::state::shared::Record {
        let node = &self.nodes[&requester];
        assert_eq!(node.state.applied, node.raw.raft.raft_log.committed);
        assert!(self.base(requester).0 < node.state.applied);
        let node = self.nodes.get_mut(&requester).unwrap();
        node.raw.request_snapshot().unwrap();
        let mut writers = BTreeMap::new();
        for round in 0..30 {
            self.settle(1).await;
            if round % 5 == 4 {
                self.publish_view(&mut writers).await;
            }
            let node = &self.nodes[&requester];
            if node.raw.raft.pending_request_snapshot == 0 {
                let record = Record::of(node.raw.store().recovery_snapshot());
                return record.unwrap().unwrap();
            }
        }
        panic!("no record for member {requester}");
    }
}

/// Critic's gap, through the Raft and apply schedulers. A member asks for a snapshot at its
/// applied index, and Raft stores the record there, so apply installs nothing. Retention then
/// deletes the record checkpoint, so the announcement of it cannot open. The member still
/// raises its base to the record from the received checkpoint, and records nothing below it.
#[tokio::test]
async fn a_record_at_the_applied_index_raises_the_base() {
    let mut sim = Simulation::with_mode(83, true).await;
    for node in sim.nodes.values_mut() {
        node.options.snapshot_every = 0;
        node.next_checkpoint = None;
    }
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let leader = sim.leader().unwrap();
    let member = sim.follower(0);
    sim.write(2).await;
    sim.settle(4).await;
    let images = sim.nodes[&member].snapshot_images;
    let record = sim.record_at_applied(member).await;
    let admin = sim.nodes[&leader].writer.as_ref().unwrap().admin.clone();
    admin.delete_checkpoint(record.checkpoint).await.unwrap();
    sim.settle(10).await;
    let (index, base, _) = sim.base(member);
    assert!(
        index >= record.index,
        "base {index}, record {}",
        record.index
    );
    assert_eq!(base, Some(record.checkpoint));
    assert_eq!(sim.nodes[&member].snapshot_images, images);
}

/// Architecture: correctness does not depend on that raise. The raise fails, as when the
/// checkpoint cannot open, and retention deletes the record checkpoint. While the base is
/// below the record, no checkpoint job starts and the flush counter keeps its bytes. The
/// member then leads, and its record index is a demand: it certifies or publishes through it.
/// The next image replaces the record, and the lagging member never gets the deleted
/// checkpoint. With an earlier binary present, the exact path publishes at a row.
async fn a_failed_raise(old: bool) {
    let mut sim = Simulation::with_mode(83, true).await;
    for node in sim.nodes.values_mut() {
        node.options.snapshot_every = 0;
        node.next_checkpoint = None;
    }
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let leader = sim.leader().unwrap();
    let member = sim.follower(0);
    let other = sim.follower(member);
    if old {
        sim.old.insert(other);
    }
    sim.write(2).await;
    sim.settle(4).await;
    let node = sim.nodes.get_mut(&member).unwrap();
    node.writer.as_mut().unwrap().skip_raise = true;
    let record = sim.record_at_applied(member).await;
    let admin = sim.nodes[&leader].writer.as_ref().unwrap().admin.clone();
    admin.delete_checkpoint(record.checkpoint).await.unwrap();
    sim.settle(10).await;
    let (index, base, _) = sim.base(member);
    assert!(index < record.index && base != Some(record.checkpoint));
    let node = sim.nodes.get_mut(&member).unwrap();
    assert!(!node.raw.store().has_image());
    // No checkpoint job starts below the record, and none resets the flush counter.
    node.appended_bytes = 7;
    let images = node.snapshot_images;
    assert_eq!(node.checkpoint_due(node.state.applied), None);
    node.start_checkpoint(true).unwrap();
    assert!(!node.checkpoint_running);
    assert_eq!((node.appended_bytes, node.snapshot_images), (7, images));
    if let Some(mut writer) = writers.remove(&leader) {
        writer.close().await;
    }
    let writer = sim.nodes[&member].writer.as_ref().unwrap();
    writer.take_publish_through();
    sim.lead(member).await;
    // No peer asks for a snapshot yet. The record index alone asks for a publication: the
    // batches above the base row, or a checkpoint at a row for the earlier binary.
    sim.settle(2).await;
    let writer = sim.nodes[&member].writer.as_ref().unwrap();
    assert!(writer.take_publish_through().is_some());
    let delivered = sim.delivered_views.len();
    let node = sim.nodes.get_mut(&other).unwrap();
    node.raw.request_snapshot().unwrap();
    for round in 0..40 {
        sim.settle(1).await;
        if round % 5 == 4 {
            sim.publish_view(&mut writers).await;
        }
    }
    assert_eq!(sim.nodes[&other].raw.raft.pending_request_snapshot, 0);
    let views = &sim.delivered_views[delivered..];
    assert!(
        !views.is_empty() && !views.contains(&record.checkpoint),
        "{views:?}"
    );
    let (index, base, _) = sim.base(member);
    assert!(index >= record.index);
    let kept = Record::of(sim.nodes[&member].raw.store().recovery_snapshot());
    assert_eq!(kept.unwrap().map(|record| record.checkpoint), base);
    assert!(sim.nodes[&member].snapshot_images <= images + 2);
    assert_eq!(
        sim.nodes[&other].state.applied,
        sim.nodes[&member].state.applied
    );
    sim.assert_records_open().await;
}

#[tokio::test]
async fn a_failed_raise_is_served_by_a_demand_at_the_record() {
    a_failed_raise(false).await;
}

#[tokio::test]
async fn a_failed_raise_beside_an_earlier_binary_is_served_at_a_row() {
    a_failed_raise(true).await;
}

/// Architecture: a follower stores a received record above its applied index, and a leadership
/// transfer or an election timeout comes before its install job runs. Raft would scan the
/// unapplied entries from its applied index, below the start of its log, and stop the process.
/// The member does not campaign before its install, but it still votes. After the install it
/// leads, and certifies or announces nothing above its applied index.
#[tokio::test]
async fn a_member_does_not_lead_before_its_install() {
    let mut sim = Simulation::with_mode(89, true).await;
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(20).await;
    let leader = sim.leader().unwrap();
    let member = sim.follower(0);
    sim.paused_apply = Some(member);
    sim.paused_reads = Some(member);
    sim.write(2).await;
    sim.settle(4).await;
    let node = sim.nodes.get_mut(&member).unwrap();
    node.raw.request_snapshot().unwrap();
    for round in 0..30 {
        sim.settle(1).await;
        if round % 5 == 4 {
            sim.publish_view(&mut writers).await;
        }
        if sim.nodes[&member].raw.raft.pending_request_snapshot == 0 {
            break;
        }
    }
    let node = &sim.nodes[&member];
    let record = Record::of(node.raw.store().recovery_snapshot());
    let record = record.unwrap().unwrap();
    assert!(node.state.applied < record.index);
    // Transfers are ignored, and election timeouts start no campaign.
    for _ in 0..40 {
        let node = sim.nodes.get_mut(&leader).unwrap();
        node.raw.transfer_leader(member);
        for _ in 0..30 {
            sim.deliver(false).await;
        }
        for id in 1..=3 {
            sim.tick(id).await;
        }
    }
    assert_eq!(sim.leader(), Some(leader));
    // The leader goes silent. The member votes for the other member, but does not campaign.
    if let Some(mut writer) = writers.remove(&leader) {
        writer.close().await;
    }
    let other = sim.follower(member);
    sim.isolated = Some(leader);
    for _ in 0..60 {
        for id in 1..=3 {
            sim.tick(id).await;
        }
        for _ in 0..30 {
            sim.deliver(false).await;
        }
    }
    assert_eq!(sim.nodes[&other].raw.raft.state, StateRole::Leader);
    assert_ne!(sim.nodes[&member].raw.raft.state, StateRole::Leader);
    sim.isolated = None;
    // The install raises the base, and the member then leads.
    sim.paused_apply = None;
    sim.paused_reads = None;
    sim.settle(10).await;
    assert!(sim.base(member).0 >= record.index);
    sim.lead(member).await;
    sim.settle(10).await;
    let announced = sim.announcements();
    let applied = sim.nodes[&member].state.applied;
    assert!(announced.iter().all(|(_, _, covers)| *covers <= applied));
    sim.assert_records_open().await;
}

/// Sig-Codex and Architecture: a new member applies the log from index 1, and no announced
/// checkpoint opens there, so its base stays empty. It asks for a snapshot at its applied index,
/// and the raise to the received checkpoint fails too. Retention deletes that checkpoint, and
/// the member leads. (1) While its base is empty, a lagging member gets nothing. (2) The new
/// writer announces the newest checkpoint at its open, as a writer does with no client write.
/// The record index is then a demand above that base, so the leader publishes, and the lagging
/// member gets a record of the base.
#[tokio::test]
async fn an_empty_base_sends_a_record_only_after_its_first_base() {
    let mut sim = Simulation::with_mode(89, true).await;
    for node in sim.nodes.values_mut() {
        node.options.snapshot_every = 0;
        node.next_checkpoint = None;
    }
    sim.settle(4).await;
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    sim.write(10).await;
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(20).await;
    let leader = sim.leader().unwrap();
    let joining = sim.follower(0);
    let other = sim.follower(joining);
    sim.joining = Some(joining);
    sim.replace(joining, 0).await;
    let source = sim.nodes[&joining].shared_source().unwrap();
    source
        .fail_opens
        .store(true, std::sync::atomic::Ordering::Relaxed);
    sim.settle(20).await;
    let node = &sim.nodes[&joining];
    assert_eq!(node.state.applied, sim.nodes[&leader].state.applied);
    assert_eq!(sim.base(joining).1, None);
    source
        .fail_opens
        .store(false, std::sync::atomic::Ordering::Relaxed);
    sim.write(2).await;
    sim.settle(4).await;
    let node = sim.nodes.get_mut(&joining).unwrap();
    node.writer.as_mut().unwrap().skip_raise = true;
    let record = sim.record_at_applied(joining).await;
    let admin = sim.nodes[&leader].writer.as_ref().unwrap().admin.clone();
    admin.delete_checkpoint(record.checkpoint).await.unwrap();
    sim.settle(10).await;
    assert_eq!(sim.base(joining).1, None);
    assert!(!sim.nodes[&joining].raw.store().has_image());
    if let Some(mut writer) = writers.remove(&leader) {
        writer.close().await;
    }
    sim.lead(joining).await;
    let delivered = sim.delivered_views.len();
    let node = sim.nodes.get_mut(&other).unwrap();
    node.raw.request_snapshot().unwrap();
    sim.settle(20).await;
    // (1) The base is empty, so nothing goes out.
    assert_eq!(sim.leader(), Some(joining));
    assert_eq!(sim.base(joining).1, None);
    assert_eq!(sim.delivered_views.len(), delivered);
    // (2) The writer opens, as at a new lead, and publishes only on request after that.
    sim.publish_view(&mut writers).await;
    for _ in 0..40 {
        sim.settle(1).await;
        let writer = sim.nodes[&joining].writer.as_ref().unwrap();
        if writer.take_publish_through().is_some() {
            sim.publish_view(&mut writers).await;
        }
    }
    assert_eq!(sim.nodes[&other].raw.raft.pending_request_snapshot, 0);
    let views = &sim.delivered_views[delivered..];
    assert!(
        !views.is_empty() && !views.contains(&record.checkpoint),
        "{views:?}"
    );
    let (index, base, _) = sim.base(joining);
    assert!(index >= record.index);
    let kept = Record::of(sim.nodes[&joining].raw.store().recovery_snapshot());
    assert_eq!(kept.unwrap().map(|record| record.checkpoint), base);
    assert_eq!(
        sim.nodes[&other].state.applied,
        sim.nodes[&joining].state.applied
    );
    sim.assert_records_open().await;
}

impl Simulation {
    /// Deliver until the leader sends member `to` a snapshot, and take that message out.
    async fn take_snapshot_to(
        &mut self,
        to: u64,
        writers: &mut BTreeMap<u64, shared::DstWriter>,
    ) -> Message {
        for round in 0..3000 {
            let found = self.messages.iter().position(|message| {
                message.to == to && message.get_msg_type() == MessageType::MsgSnapshot
            });
            if let Some(position) = found {
                return self.messages.remove(position);
            }
            if self.messages.is_empty() {
                let leader = self.leader().unwrap();
                self.tick(leader).await;
                if round % 50 == 49 {
                    self.publish_view(writers).await;
                }
            }
            self.deliver(false).await;
        }
        panic!("no snapshot for member {to}");
    }

    /// A follower in snapshot install: its apply is paused, and it asks for a snapshot above
    /// its applied index. Return the leader, the member, and the third member.
    async fn installing_member(
        seed: u64,
        writers: &mut BTreeMap<u64, shared::DstWriter>,
    ) -> (Self, u64, u64, u64) {
        let mut sim = Simulation::with_mode(seed, true).await;
        sim.settle(4).await;
        sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
        sim.write(10).await;
        sim.publish_view(writers).await;
        sim.settle(20).await;
        let leader = sim.leader().unwrap();
        let member = sim.follower(0);
        let other = sim.follower(member);
        sim.paused_apply = Some(member);
        sim.paused_reads = Some(member);
        sim.write(2).await;
        sim.settle(4).await;
        let node = sim.nodes.get_mut(&member).unwrap();
        node.raw.request_snapshot().unwrap();
        (sim, leader, member, other)
    }
}

/// Sig-Codex and Critic: Raft accepted the snapshot and the log cache staged it, but its write
/// waits. Raft already dropped its unstable snapshot, and the durable record is the old one.
/// Election timeouts start no campaign and stop nothing. Once the write ends and the install
/// runs, leadership works as usual.
#[tokio::test]
async fn a_member_does_not_campaign_while_its_snapshot_is_staged() {
    let mut writers = BTreeMap::new();
    let (mut sim, leader, member, other) = Simulation::installing_member(97, &mut writers).await;
    let message = sim.take_snapshot_to(member, &mut writers).await;
    let index = message.get_snapshot().get_metadata().index;
    sim.paused_log = Some(member);
    let accepted = sim.receive_snapshot(message).await.unwrap();
    let node = sim.nodes.get_mut(&member).unwrap();
    node.step_peer(accepted).unwrap();
    let node = sim.nodes.get_mut(&leader).unwrap();
    node.raw.report_snapshot(member, SnapshotStatus::Finish);
    sim.drive().await;
    let node = &sim.nodes[&member];
    assert!(node.raw.raft.raft_log.unstable.snapshot.is_none());
    assert_eq!(node.raw.store().staged_snapshot_index(), index);
    assert!(node.raw.store().snapshot_index() < index);
    // The leader goes silent, and the election timer of the member runs out many times.
    if let Some(mut writer) = writers.remove(&leader) {
        writer.close().await;
    }
    sim.isolated = Some(leader);
    for _ in 0..60 {
        for id in 1..=3 {
            sim.tick(id).await;
        }
        for _ in 0..30 {
            sim.deliver(false).await;
        }
    }
    let node = &sim.nodes[&member];
    assert_eq!(node.raw.raft.state, StateRole::Follower);
    assert_eq!(node.raw.store().staged_snapshot_index(), index);
    // The write ends, and the install runs. The member then leads.
    sim.paused_log = None;
    sim.paused_apply = None;
    sim.paused_reads = None;
    sim.isolated = None;
    sim.settle(30).await;
    assert!(sim.nodes[&member].state.applied >= index);
    sim.lead(member).await;
    sim.settle(10).await;
    assert_eq!(
        sim.nodes[&member].state.applied,
        sim.nodes[&other].state.applied
    );
}

/// Sig-Codex: a snapshot and a leadership transfer arrive in the same event batch. Raft holds
/// the snapshot as unstable when the transfer comes, and the member ignores the transfer.
#[tokio::test]
async fn a_transfer_after_a_snapshot_in_one_batch_is_ignored() {
    let mut writers = BTreeMap::new();
    let (mut sim, leader, member, other) = Simulation::installing_member(101, &mut writers).await;
    let message = sim.take_snapshot_to(member, &mut writers).await;
    let index = message.get_snapshot().get_metadata().index;
    let term = message.term;
    let accepted = sim.receive_snapshot(message).await.unwrap();
    let mut transfer = Message::default();
    transfer.set_msg_type(MessageType::MsgTimeoutNow);
    transfer.from = leader;
    transfer.to = member;
    transfer.term = term;
    let node = sim.nodes.get_mut(&member).unwrap();
    node.step_peer(accepted).unwrap();
    assert!(node.raw.raft.raft_log.unstable.snapshot.is_some());
    node.step_peer(transfer).unwrap();
    assert_eq!(node.raw.raft.state, StateRole::Follower);
    assert_eq!(node.raw.raft.term, term);
    // The transport reports the sent snapshot, as `deliver` does.
    let node = sim.nodes.get_mut(&leader).unwrap();
    node.raw.report_snapshot(member, SnapshotStatus::Finish);
    // The install runs, and the member then leads.
    sim.paused_apply = None;
    sim.paused_reads = None;
    sim.settle(30).await;
    assert!(sim.nodes[&member].state.applied >= index);
    if let Some(mut writer) = writers.remove(&leader) {
        writer.close().await;
    }
    sim.lead(member).await;
    sim.settle(10).await;
    assert_eq!(
        sim.nodes[&member].state.applied,
        sim.nodes[&other].state.applied
    );
}
