//! The shared-mode writer of one member: the writer tasks of the leader and the state that only
//! the writer path uses. The node calls its methods and reads none of its fields.

use super::*;

/// The shared-mode tasks of one member. Only the leader runs the writer.
pub(in crate::node) struct Writer {
    admin: Arc<slatedb::admin::Admin>,
    queue: Arc<Queue>,
    source: Arc<Source>,
    progress: Arc<Progress>,
    max_age: Duration,
    /// Set while this member leads. Sending `true` closes the writer.
    stop: Option<tokio::sync::watch::Sender<bool>>,
    /// Writer tasks with the term that started each one.
    tasks: tokio::task::JoinSet<(u64, Result<()>)>,
    reports: Reports,
    /// `ViewCheckpoint` entries that this node proposed.
    announced: u64,
    /// The applied base and announcement. The publisher announces a stranded checkpoint.
    applied: Applied,
    /// The `F` of the newest base that opened. A change starts a switch job.
    opened: tokio::sync::watch::Receiver<u64>,
    opened_slot: Arc<crate::state::shared::Opened>,
    /// A switch job is due.
    switch_due: bool,
    /// Apply waits while the overlay is over the hard limit.
    paused: bool,
    /// The last opened base that an adoption dropped. See `rejected`.
    rejected: rejected::Rejected,
    /// A paused member polls the checkpoint list. Only one poll runs at a time.
    polling: Arc<std::sync::atomic::AtomicBool>,
    ticks: u64,
    /// The role and term of this member. The writer task checks them after its open.
    leadership: tokio::sync::watch::Sender<(bool, u64)>,
    /// The term of the running writer task.
    term: u64,
    clock: Clock,
    /// A leader transfer waits for a checkpoint that covers this batch sequence.
    transfer: Option<(u64, u64)>,
    /// Writer opens after a fence while this member still led.
    reopens: Arc<std::sync::atomic::AtomicU64>,
    retention: crate::state::ViewRetention,
    retained: Option<prometheus::IntGauge>,
    /// The writer `Db` while this member leads.
    open: OpenWriter,
    /// Tests: the writer task notifies the first and waits for the second after it publishes
    /// an open writer.
    #[cfg(test)]
    open_hold: Hold,
    gauges: Option<Gauges>,
    /// The highest manual checkpoint index that requested a publication after apply.
    checkpoint_requested: u64,
    /// A follower's manual checkpoint index that waits for apply on this leader.
    remote_checkpoint: u64,
    /// The highest index that a follower's manual checkpoint needs a base to cover.
    demand: u64,
    /// The coverage and the tick of the last certification that this leader proposed.
    certified: Option<(u64, u64)>,
    /// The batch through which this leader last asked for a publication to serve a demand.
    demand_publication: u64,
    /// Ticks of `certify_base`.
    certify_ticks: u64,
    /// The leader term of the demand above. A new term drops the demand of the old one.
    demand_term: u64,
    /// The highest record index whose checkpoint apply offered as the base. See `take_raise`.
    raised_record: u64,
    /// DST: skip that offer, as when the checkpoint cannot open.
    #[cfg(test)]
    skip_raise: bool,
    /// The health of this member's writer. See `fault`.
    health: fault::Health,
    /// A batch waits for the clock check since this time. See `Writer::note_clock_wait`.
    clock_due: Option<Instant>,
    /// The writer reports of the voters, and the handoff of a writer incident.
    handoff: fault::Handoff,
    /// The write proofs of the learners that this leader promotes. See `write_proof`.
    proofs: write_proof::Proofs,
    /// The write probes of this member. The node gives them at its open.
    prober: Option<Arc<Prober>>,
}

/// The shared-mode gauges of one member. The writer sets them on each tick of the node.
pub(in crate::node) struct Gauges {
    base: prometheus::IntGauge,
    overlay: prometheus::IntGauge,
    paused: prometheus::IntGauge,
    leading: prometheus::IntGauge,
    reopens: prometheus::IntGauge,
    clock_alarm: prometheus::IntGauge,
    clock_waiting: prometheus::IntGauge,
    failures: prometheus::IntGauge,
    faults: prometheus::IntCounter,
    pending_closes: prometheus::IntGauge,
    aborted_closes: prometheus::IntCounter,
}

impl Gauges {
    pub(in crate::node) fn register(registry: &prometheus::Registry) -> Result<Self> {
        let gauge = |name: &str, help: &str| -> Result<prometheus::IntGauge> {
            let gauge = prometheus::IntGauge::new(name, help)?;
            registry.register(Box::new(gauge.clone()))?;
            Ok(gauge)
        };
        let counter = |name: &str, help: &str| -> Result<prometheus::IntCounter> {
            let counter = prometheus::IntCounter::new(name, help)?;
            registry.register(Box::new(counter.clone()))?;
            Ok(counter)
        };
        Ok(Self {
            base: gauge(
                "slatedb_etcd_shared_base_applied_index",
                "Raft index of the view checkpoint that this member reads as its base.",
            )?,
            overlay: gauge(
                "slatedb_etcd_shared_overlay_bytes",
                "Bytes of applied batches in the overlay above the base.",
            )?,
            paused: gauge(
                "slatedb_etcd_shared_apply_paused",
                "1 while apply waits for a base switch above twice the overlay limit.",
            )?,
            leading: gauge(
                "slatedb_etcd_shared_writer_running",
                "1 while the shared writer database of this member is open.",
            )?,
            clock_alarm: gauge(
                "slatedb_etcd_shared_writer_clock_alarm",
                "1 while the local clock is too far behind the writer clock or ahead of a voter \
                 majority.",
            )?,
            clock_waiting: gauge(
                "slatedb_etcd_shared_writer_clock_waiting",
                "1 while the writer waits for the clock samples of a voter majority or for its \
                 clock bound.",
            )?,
            reopens: gauge(
                "slatedb_etcd_shared_writer_reopens",
                "Writer opens after a fence or a lost manifest race while this member led.",
            )?,
            failures: gauge(
                "slatedb_etcd_shared_writer_consecutive_failures",
                "Shared writer failures in a row on this member, other than fences.",
            )?,
            faults: counter(
                "slatedb_etcd_shared_writer_faults_total",
                "Shared writer faults on this member: errors, clock refusals, and stalls.",
            )?,
            pending_closes: gauge(
                "slatedb_etcd_shared_writer_pending_closes",
                "1 while the close of a failed shared writer waits for its I/O.",
            )?,
            aborted_closes: counter(
                "slatedb_etcd_shared_writer_aborted_closes_total",
                "Closes of failed shared writers that the close of a newer one aborted.",
            )?,
        })
    }
}

impl Writer {
    pub fn new(
        handles: &Handles,
        max_age: Duration,
        clock: Clock,
        prober: Option<Arc<Prober>>,
        retention: crate::state::ViewRetention,
        retained: Option<prometheus::IntGauge>,
        gauges: Option<Gauges>,
    ) -> Self {
        Self {
            leadership: tokio::sync::watch::channel((false, 0)).0,
            term: 0,
            clock,
            transfer: None,
            reopens: Arc::default(),
            retention,
            retained,
            open: OpenWriter::default(),
            #[cfg(test)]
            open_hold: Hold::default(),
            gauges,
            checkpoint_requested: 0,
            remote_checkpoint: 0,
            demand: 0,
            certified: None,
            demand_publication: 0,
            certify_ticks: 0,
            demand_term: 0,
            raised_record: 0,
            #[cfg(test)]
            skip_raise: false,
            health: fault::Health::default(),
            clock_due: None,
            handoff: fault::Handoff::default(),
            proofs: write_proof::Proofs::default(),
            prober,
            progress: Arc::new(Progress::new(0)),
            admin: handles.admin.clone(),
            queue: handles.queue.clone(),
            source: handles.source.clone(),
            max_age,
            stop: None,
            tasks: tokio::task::JoinSet::new(),
            reports: Reports::default(),
            announced: 0,
            applied: Applied {
                based: handles.based.clone(),
                announced: handles.announced.clone(),
                base_id: handles.base_id.clone(),
                #[cfg(test)]
                failures: Arc::default(),
            },
            opened: handles.opened.subscribe(),
            opened_slot: handles.opened.clone(),
            switch_due: false,
            paused: false,
            rejected: Default::default(),
            polling: Arc::default(),
            ticks: 0,
        }
    }

    pub fn running(&self) -> bool {
        self.stop.is_some()
    }

    /// Open the writer on the leader of `term`. Close it on any other member.
    ///
    /// The open fences an older writer. A task of an older term stops before the new one starts.
    pub fn sync(&mut self, leader: bool, term: u64, events: &mpsc::Sender<Event>) {
        self.leadership.send_replace((leader, term));
        if !leader || self.term != term {
            if let Some(stop) = self.stop.take() {
                stop.send_replace(true);
            }
            self.health.end();
            self.handoff.lead(false);
            self.clock_due = None;
            if !leader {
                self.clock.stand_down();
            }
            self.transfer = None;
            self.demand = 0;
            self.certified = None;
            self.demand_publication = 0;
        }
        if !leader || self.running() {
            return;
        }
        self.term = term;
        let health = self.health.begin();
        self.handoff.lead(true);
        let (stop, stopped) = tokio::sync::watch::channel(false);
        self.stop = Some(stop);
        let task = lead(
            Publisher {
                admin: self.admin.clone(),
                source: self.source.clone(),
                progress: self.progress.clone(),
                max_age: self.max_age,
                reports: self.reports.clone(),
                retention: self.retention,
                retained: self.retained.clone(),
                applied: self.applied.clone(),
                health: health.clone(),
            },
            Leading {
                queue: self.queue.clone(),
                events: events.clone(),
                stopped,
                term,
                leadership: self.leadership.subscribe(),
                clock: self.clock.clone(),
                reopens: self.reopens.clone(),
                open: self.open.clone(),
                #[cfg(test)]
                open_hold: self.open_hold.clone(),
                health,
            },
        );
        self.tasks.spawn(async move { (term, task.await) });
    }

    pub async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.send_replace(true);
        }
        // A clean stop closes the writer. A stuck close must not hold the shutdown, also not
        // the close of a failed writer. One deadline covers both.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let joined = async { while self.tasks.join_next().await.is_some() {} };
        if tokio::time::timeout_at(deadline, joined).await.is_err() {
            self.tasks.shutdown().await;
        }
        // A task can abandon a failed writer until it ends, so the close comes after the join.
        if let Some(mut failed) = self.health.take_close() {
            if tokio::time::timeout_at(deadline, &mut failed)
                .await
                .is_err()
            {
                failed.abort();
            }
        }
    }

    /// Request the publication for a manual checkpoint through `index` once apply reaches it.
    /// Return true when this call requested it. Each index is requested once.
    pub fn request_checkpoint(&mut self, index: u64, applied: u64) -> bool {
        if index > applied || index <= self.checkpoint_requested {
            return false;
        }
        self.checkpoint_requested = index;
        self.request_publication(index);
        true
    }

    /// Publish a checkpoint that holds the applied state through `index`.
    pub fn request_publication(&self, index: u64) {
        self.progress.request(self.covering(index));
    }

    /// DST: take the batch through which a publication is due. The DST writer steps publish
    /// without it.
    #[cfg(test)]
    pub(in crate::node) fn take_publish_through(&self) -> Option<u64> {
        self.progress.inner.lock().unwrap().publish_through.take()
    }

    /// The batch sequence that holds the state through `index`. A later no-op entry has no batch,
    /// and a batch that the base released holds no change above the base row.
    pub fn covering(&self, index: u64) -> u64 {
        let base = *self.applied.base_id.borrow();
        index.min(demand::last_change(base, self.queue.position()))
    }

    /// Record the checkpoints that `member` pins. The publisher keeps them.
    pub fn report(&self, member: u64, checkpoints: HashSet<uuid::Uuid>) {
        self.reports
            .lock()
            .unwrap()
            .insert(member, (Instant::now(), checkpoints));
    }

    pub fn status(&self) -> Value {
        json!({
            "written": self.progress.written(),
            "queued_batches": self.queue.len(),
            "announced": self.announced,
            "running": self.running(),
            "paused": self.paused,
            "rejected_base": self.rejected.status(),
            "clock_skew_ms": self.clock.skew.load(std::sync::atomic::Ordering::Relaxed),
            "clock_alarm": self.clock.alarm_name(),
            "clock": self.clock.status(),
            "reopens": self.reopens.load(std::sync::atomic::Ordering::Relaxed),
            "reports": self.reports.lock().unwrap().len(),
            "health": self.health.status(),
            "handoff": self.handoff.status(),
            "write_proofs": self.proofs.status(Instant::now()),
        })
    }

    /// The objects of the shared state that this writer opens.
    pub(in crate::node) fn source(&self) -> &Arc<Source> {
        &self.source
    }

    /// The write probes that this member runs for the leader.
    pub(in crate::node) fn prober(&self) -> Option<Arc<Prober>> {
        self.prober.clone()
    }

    /// The clock check of this member's writer.
    pub(in crate::node) fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Start the write proofs of leader term `term`, or end them with `None`.
    pub(super) fn lead_proofs(&mut self, term: Option<u64>) {
        self.proofs.lead(term);
    }

    /// Check the write proof of `member`. See `Proofs::check`.
    pub(super) fn check_proof(
        &mut self,
        now: Instant,
        term: u64,
        member: u64,
        process: Option<u64>,
    ) -> std::result::Result<(), write_proof::Refusal> {
        self.proofs.check(now, term, member, process)
    }

    /// Record the reply of `member` to its write probe. See `Proofs::complete`.
    pub(super) fn complete_proof(
        &mut self,
        now: Instant,
        term: u64,
        member: u64,
        process: Option<u64>,
        replied: write_proof::Replied,
    ) -> bool {
        self.proofs.complete(now, term, member, process, replied)
    }

    /// Keep only the write proofs of `members`.
    pub(super) fn retain_proofs(&mut self, members: &HashSet<u64>) {
        self.proofs.retain(|member| members.contains(&member));
    }

    /// Drop the write proof of `member` and its running probe.
    pub(in crate::node) fn forget_proof(&mut self, member: u64) {
        self.proofs.forget(member);
    }

    /// Hold the leader transfer to `target` until a checkpoint covers the state through `applied`,
    /// and request that publication.
    pub(in crate::node) fn start_transfer(&mut self, target: u64, applied: u64) {
        self.transfer = Some((target, self.covering(applied)));
        self.request_publication(applied);
    }

    /// Take the target of the held leader transfer when the base at `base` covers its checkpoint
    /// or the wait ended.
    pub(in crate::node) fn take_transfer(&mut self, base: u64, waited: bool) -> Option<u64> {
        let (target, covering) = self.transfer?;
        if base >= covering || waited {
            self.transfer = None;
            return Some(target);
        }
        None
    }

    /// A Defragment of leader term `term` on the open writer, or `None` while it is not open.
    pub(super) fn defragment(&self, term: u64) -> Option<Defragment> {
        // The `Db` and the admin on its clock come as one pair, or the writer is not open.
        let Open { db, admin, gate } = self.open.lock().unwrap().clone()?;
        Some(Defragment {
            db,
            admin,
            owner: Some(gate),
            progress: self.progress.clone(),
            announced: self.applied.announced.clone(),
            leadership: self.leadership.subscribe(),
            term,
            pause: None,
        })
    }

    /// The index through which a checkpoint at `row` holds the whole state. See
    /// `demand::coverage`.
    pub(super) fn coverage(&self, row: u64) -> u64 {
        let base = *self.applied.base_id.borrow();
        demand::coverage(row, base, self.queue.position())
    }

    /// Count a `ViewCheckpoint` entry that this node proposed.
    pub(super) fn note_announced(&mut self) {
        self.announced += 1;
    }

    /// Record the `ViewAck` of `member` on the leader. See `Node::view_ack`.
    pub(super) fn note_view_ack(
        &mut self,
        member: u64,
        checkpoints: HashSet<uuid::Uuid>,
        publish: Option<u64>,
        coverage: Option<u64>,
        health: Option<fault::WriterReport>,
    ) {
        self.report(member, checkpoints);
        self.handoff.note(member, coverage, health);
        // A follower's manual checkpoint. The publication is due once apply reaches it.
        // The follower can group entries in other ways, so its base must reach the index.
        if let Some(index) = publish {
            self.remote_checkpoint = self.remote_checkpoint.max(index);
            self.demand = self.demand.max(index);
        }
    }

    /// Return true when the next tick sends the clock probes.
    pub(super) fn probe_due(&self) -> bool {
        (self.ticks + 1).is_multiple_of(clock::PROBE_TICKS)
    }

    /// Count a node tick. Set the gauges with the base index `base` and the overlay size
    /// `bytes`, request the publication of a follower's manual checkpoint on the leader, and
    /// poll the checkpoints while apply waits. `committed` is the delivered commit index. Return
    /// the tick count.
    pub(super) fn tick(
        &mut self,
        leader: bool,
        applied: u64,
        committed: u64,
        base: u64,
        bytes: u64,
    ) -> u64 {
        self.ticks += 1;
        let ticks = self.ticks;
        if let Some(gauges) = &self.gauges {
            gauges.base.set(base as i64);
            gauges.overlay.set(bytes as i64);
            gauges.paused.set(self.paused.into());
            gauges
                .leading
                .set(self.open.lock().unwrap().is_some().into());
            gauges
                .clock_alarm
                .set(self.clock.alarm_name().is_some().into());
            let waiting = self.clock.waiting() || self.clock.uncertain();
            gauges.clock_waiting.set(waiting.into());
            gauges
                .reopens
                .set(self.reopens.load(std::sync::atomic::Ordering::Relaxed) as i64);
        }
        if leader && self.remote_checkpoint > 0 {
            let index = self.remote_checkpoint;
            self.request_checkpoint(index, applied);
            if index <= self.checkpoint_requested {
                self.remote_checkpoint = 0;
            }
        }
        if ticks.is_multiple_of(POLL_TICKS) && self.paused {
            self.poll_checkpoints(base, committed);
        }
        ticks
    }

    /// While apply waits, open the newest checkpoint above the base out of band. A switch job
    /// then releases the segments that it covers, including the open one.
    ///
    /// The member can adopt only a base at or below the delivered commit `committed`. So the
    /// poll takes the newest base with its row in `(base, committed]`, and a later poll opens a
    /// newer one after the commit reaches it. The name of a checkpoint is only a lower bound of
    /// its row, so the poll checks the row of each opened base.
    fn poll_checkpoints(&self, base: u64, committed: u64) {
        if self.polling.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return;
        }
        let polling = self.polling.clone();
        let admin = self.admin.clone();
        let source = self.source.clone();
        let opened = self.opened_slot.clone();
        tokio::spawn(async move {
            let result = async {
                let newer = newest_base_between(&admin, &source, base + 1, committed).await?;
                if let Some(newer) = newer {
                    opened.offer(newer, 0);
                }
                anyhow::Ok(())
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "cannot poll view checkpoints");
            }
            polling.store(false, std::sync::atomic::Ordering::Release);
        });
    }

    /// Tests: wait until the running checkpoint poll ends.
    #[cfg(test)]
    pub(in crate::node) async fn polled(&self) {
        while self.polling.load(std::sync::atomic::Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    }

    /// The writer report of this member in `term`. See `Health::report`.
    pub(super) fn health_report(&self, term: u64, apply_held_at: Option<u64>) -> WriterReport {
        self.health.report(term, apply_held_at)
    }

    /// Pause apply above twice the overlay limit `limit`, and resume it below the limit. Return
    /// true while apply waits.
    pub(super) fn pause_apply(&mut self, bytes: u64, limit: u64) -> bool {
        if bytes > limit.saturating_mul(2) {
            if !self.paused {
                tracing::warn!(
                    bytes,
                    limit,
                    "pause apply until a base switch frees the overlay"
                );
            }
            self.paused = true;
        } else if bytes < limit {
            self.paused = false;
            // A dropped base explains a pause only. Any way out of the pause ends its report.
            self.rejected.clear();
        }
        self.paused
    }

    /// Take the opened base and its coverage from the slot while apply waits.
    pub(super) fn take_opened(&self) -> Option<(Arc<crate::state::view::Base>, u64)> {
        if !self.paused {
            return None;
        }
        self.opened_slot.take()
    }

    /// Give an opened base back to the slot.
    pub(super) fn offer_opened(&self, base: Arc<crate::state::view::Base>, covers: u64) {
        self.opened_slot.offer(base, covers);
    }

    /// Mark a switch due when a base was offered since the last call. A manual apply, such as
    /// DST, has no event loop that watches the slot.
    #[cfg(test)]
    pub(in crate::node) fn note_offers(&mut self) {
        if self.opened.has_changed().unwrap_or(false) {
            self.opened.borrow_and_update();
            self.switch_due = true;
        }
    }

    /// Take a due switch job.
    pub(in crate::node) fn take_switch(&mut self) -> bool {
        std::mem::take(&mut self.switch_due)
    }

    /// Count a base that an adoption dropped. Return true for the first failure of its
    /// checkpoint.
    pub(super) fn note_rejected(&mut self, rejected: &apply::RejectedBase) -> bool {
        self.rejected.note(rejected)
    }

    /// Drop the demand of an earlier leader term when this member leads `term`.
    pub(super) fn lead_demand(&mut self, term: u64) {
        if self.demand_term != term {
            self.demand_term = term;
            self.demand = 0;
            self.certified = None;
            self.demand_publication = 0;
        }
    }

    /// Serve the demand for a base through the snapshot request `wanted` and the record
    /// `record`. Return the base checkpoint, its coverage, and the tick, when the leader must
    /// certify the base with a new announcement. See `Node::certify_base`.
    pub(super) fn certify(&mut self, wanted: u64, record: u64) -> Option<(uuid::Uuid, u64, u64)> {
        self.certify_ticks += 1;
        let demand = self.demand.max(wanted).max(record);
        let unmet = demand > *self.applied.based.borrow();
        if !unmet {
            self.certified = None;
            return None;
        }
        // A leader with no base checkpoint, such as one that applied the log from its start,
        // gets one from its writer at open: the writer publishes a first checkpoint when it has
        // writes above the newest one, and otherwise announces the newest one if this member
        // did not apply its announcement. The demand is served after that base switch.
        let base = (*self.applied.base_id.borrow())?;
        let position = self.queue.position();
        if position.covered < demand {
            return None;
        }
        let change = demand::last_change(Some(base), position);
        if base.row < change {
            if self.demand_publication < change {
                self.demand_publication = change;
                self.request_publication(change);
            }
            return None;
        }
        let ticks = self.certify_ticks;
        if self
            .certified
            .is_some_and(|(covers, at)| covers >= demand && ticks < at + demand::CERTIFY_TICKS)
        {
            return None;
        }
        let covers = demand::coverage(base.row, Some(base), position);
        let checkpoint = base.checkpoint;
        tracing::info!(%checkpoint, row = base.row, covers, demand, "certify the base for a demand");
        Some((checkpoint, covers, ticks))
    }

    /// Note the certification through `covers` that the leader proposed at tick `ticks`.
    pub(super) fn certified(&mut self, covers: u64, ticks: u64) {
        self.certified = Some((covers, ticks));
    }

    /// The checkpoint and the index of the current base.
    pub(super) fn base(&self) -> Option<(uuid::Uuid, u64)> {
        self.applied
            .base_id
            .borrow()
            .map(|base| (base.checkpoint, base.index))
    }

    /// Take the offer of the checkpoint of record `record` as the base, once for each record.
    pub(super) fn take_raise(&mut self, record: u64) -> bool {
        if self.raised_record >= record {
            return false;
        }
        self.raised_record = record;
        #[cfg(test)]
        if self.skip_raise {
            return false;
        }
        true
    }

    /// Check the writer health on a node tick, and set the health gauges. Only the leader
    /// checks.
    pub(super) fn tick_health(&mut self, leader: bool) {
        if leader {
            self.note_clock_wait();
            self.health.check(self.waiting());
        }
        if let Some(gauges) = &self.gauges {
            let health = &self.health;
            gauges.failures.set(health.consecutive().into());
            gauges.pending_closes.set(health.pending_closes() as i64);
            let counters = [
                (&gauges.faults, health.total()),
                (&gauges.aborted_closes, health.aborted_closes()),
            ];
            for (counter, total) in counters {
                counter.inc_by(total.saturating_sub(counter.get()));
            }
        }
    }

    /// Return true when the leader must hand over its leadership. See `fault::handoff_due`.
    pub(super) fn handoff_due(&self, paused: Option<&apply::Paused>) -> bool {
        fault::handoff_due(&self.health, paused)
    }

    /// Step the handoff of a writer incident, and restart a stalled writer when the step asks.
    /// `moves` holds the targets of the MoveLeader requests in flight. See `Handoff::step`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn step_handoff(
        &mut self,
        due: bool,
        transferee: Option<u64>,
        wait: Duration,
        candidates: &[fault::Candidate],
        term: u64,
        mut moves: Vec<u64>,
        held: bool,
    ) -> fault::Decision {
        moves.extend(self.transfer.map(|(target, _)| target));
        let health = &self.health;
        let decision = self.handoff.step(&fault::Tick {
            due,
            stalled: health.stalled(),
            transferee,
            wait,
            candidates,
            published: health.published_at(),
            incident: health.incident(),
            term,
            moves: &moves,
            held,
        });
        if decision.restart {
            health.restart();
        }
        decision
    }

    /// The time since work that the publisher has not started became due: the oldest unflushed
    /// write at its age limit, or an explicit publication request. A writer that is not open
    /// holds the progress of an earlier one, so only an open writer counts. A batch that waits
    /// for clock samples or for its clock bound counts too, open writer or not. See
    /// `note_clock_wait`.
    pub(super) fn waiting(&self) -> Option<Instant> {
        if self.open.lock().unwrap().is_none() {
            return self.clock_due;
        }
        let aged = self.progress.deadline(self.max_age);
        aged.into_iter()
            .chain(self.progress.requested())
            .chain(self.clock_due)
            .min()
    }

    /// Note on a leader tick whether the clock check holds due work: the writer waits for
    /// samples or for its bound, and a batch waits for the writer. An idle writer that waits
    /// is no due work, so its wait never becomes a stall.
    pub(super) fn note_clock_wait(&mut self) {
        let due = self.clock.waiting() && self.queue.has_after(self.progress.written());
        self.clock_due = due.then(|| self.clock_due.unwrap_or_else(Instant::now));
    }
}

/// Tests: the state of the writer.
#[cfg(test)]
impl Writer {
    pub(in crate::node) fn admin(&self) -> &Arc<slatedb::admin::Admin> {
        &self.admin
    }

    pub(super) fn queue(&self) -> &Arc<Queue> {
        &self.queue
    }

    pub(super) fn progress(&self) -> &Arc<Progress> {
        &self.progress
    }

    pub(super) fn max_age(&self) -> Duration {
        self.max_age
    }

    pub(super) fn term(&self) -> u64 {
        self.term
    }

    pub(super) fn leadership(&self) -> (bool, u64) {
        *self.leadership.borrow()
    }

    pub(super) fn open(&self) -> Option<Open> {
        self.open.lock().unwrap().clone()
    }

    pub(super) fn reopens(&self) -> u64 {
        self.reopens.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(in crate::node) fn health(&self) -> &fault::Health {
        &self.health
    }

    pub(super) fn clock_due(&self) -> Option<Instant> {
        self.clock_due
    }

    pub(super) fn set_clock_due(&mut self, due: Option<Instant>) {
        self.clock_due = due;
    }

    pub(super) fn proofs(&self) -> &write_proof::Proofs {
        &self.proofs
    }

    /// The stranded checks that fail before they read storage.
    pub(super) fn failures(&self) -> Arc<std::sync::atomic::AtomicU32> {
        self.applied.failures.clone()
    }

    pub(super) fn tasks(&mut self) -> &mut tokio::task::JoinSet<(u64, Result<()>)> {
        &mut self.tasks
    }

    /// Pause the next writer task after it publishes an open writer. See `open_hold`.
    pub(super) fn hold_open(
        &self,
        hold: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    ) {
        *self.open_hold.lock().unwrap() = hold;
    }

    pub(super) fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }

    /// DST: skip the offer of a record's checkpoint as the base. See `take_raise`.
    pub(in crate::node) fn skip_raise(&mut self) {
        self.skip_raise = true;
    }

    /// Skew the wall clock of this writer and of its probe replies.
    pub(in crate::node) fn set_clock(&mut self, now_ms: Millis) {
        self.clock.now_ms = now_ms;
    }

    /// DST: find a stranded checkpoint, as a new writer does. See `stranded`.
    pub(super) async fn stranded(&self) -> Result<Option<(uuid::Uuid, u64)>> {
        stranded(&self.admin, &self.source, &self.applied).await
    }
}

/// Wait for the next writer event. A stop that the member requested is not a failure.
pub(in crate::node) async fn next(writer: &mut Writer) -> WriterEvent {
    let leading = writer.stop.is_some();
    let current = writer.term;
    let failed = async {
        loop {
            match writer.tasks.join_next().await {
                // A task of an older term, or of a term that this member left, is obsolete. A
                // newer writer can fence it before it sees its stop.
                Some(Ok((term, result))) if !leading || term != current => {
                    if let Err(error) = result {
                        tracing::warn!(%error, term, "a stopped shared writer failed");
                    }
                    continue;
                }
                Some(Ok((_, Ok(())))) => {
                    return anyhow::anyhow!("a shared state writer task stopped")
                }
                Some(Ok((_, Err(error)))) => return error.context("shared state writer failed"),
                Some(Err(error)) if error.is_cancelled() => continue,
                Some(Err(error)) => {
                    return anyhow::Error::from(error).context("shared state writer stopped")
                }
                None => return std::future::pending().await,
            }
        }
    };
    tokio::select! {
        error = failed => WriterEvent::Failed(error),
        changed = writer.opened.changed() => {
            if changed.is_err() {
                return std::future::pending().await;
            }
            writer.switch_due = true;
            WriterEvent::Opened
        }
    }
}
