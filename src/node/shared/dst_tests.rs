//! Shared-mode simulations of #96: coverage of a base above its row, the leader's demand for a
//! base, and restarts at a record above the row.

use super::*;

impl Simulation {
    /// Propose a Put on the leader that no client waits for. With `ignore_value` on a missing
    /// key it fails, so it changes no state and stores no batch.
    async fn propose_put(&mut self, put: PutRequest) {
        let leader = self.leader().unwrap();
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
    async fn propose_noop(&mut self) {
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
}

/// #117 stops a new member at an earlier binary's `Publish` until a snapshot arrives. A failed
/// update follows the `Publish`, so the last index is above the last batch. In this idle shared
/// cluster the leader's announcement covers that index, and the member recovers by snapshot.
#[tokio::test]
async fn a_guarded_shared_member_recovers_in_an_idle_cluster() {
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
    let mut writers = BTreeMap::new();
    sim.publish_view(&mut writers).await;
    sim.settle(30).await;
    let leader = sim.leader().unwrap();
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
    let node = sim.nodes.get_mut(&leader).unwrap();
    node.view_ack(grouped, pins, Some(index));
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
/// restart opens X by the record ID at `c` with term 2, and it applies the later entries once.
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
    sim.settle(4).await;
    sim.propose().await;
    sim.settle(4).await;
    let node = &sim.nodes[&member];
    assert_eq!(node.state.applied, sim.nodes[&leader].state.applied);
    assert_eq!(node.state.revision, sim.nodes[&leader].state.revision);
    sim.assert_acknowledged(member).await;
}
