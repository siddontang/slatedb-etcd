//! Writer faults in shared mode: classification, the health record, the handoff target, and
//! the member that stays up while its writer fails.

use super::super::tests::{eventually, new_writer, status};
use super::*;
use crate::object::faults::{
    FaultStore, CORRUPT_MANIFEST, DELAY_SHARED, FAIL_PUT, FAIL_STATE_PUT, PAUSE_MANIFEST_READ,
    PAUSE_PUT, STALL_BOOTSTRAP_READ, STALL_MANIFEST_READ, STALL_STATE_PUT,
};
use crate::proto::etcdserverpb::{
    kv_client::KvClient, maintenance_client::MaintenanceClient, MoveLeaderRequest, PutRequest,
    RangeRequest,
};
use crate::server::{Server, ServerConfig};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

#[test]
fn the_outer_slatedb_kind_classifies_an_error() {
    let missing = slatedb::object_store::Error::NotFound {
        path: "manifest".into(),
        source: "gone".into(),
    };
    let data = slatedb::Error::data("an object is missing".into()).with_source(Box::new(missing));
    assert_eq!(classify(&data.into()), Fault::Data);
    let throttled = anyhow::Error::from(slatedb::Error::unavailable("throttled".into()));
    assert_eq!(classify(&throttled.context("flush")), Fault::Unavailable);
    let denied = slatedb::object_store::Error::Generic {
        store: "S3",
        source: "access denied".into(),
    };
    let claim = anyhow::Error::from(denied).context("read the claim");
    assert_eq!(classify(&claim), Fault::Unavailable);
    let panic = slatedb::Error::closed("closed".into(), slatedb::CloseReason::Panic);
    assert_eq!(classify(&panic.into()), Fault::Data);
    let fence = slatedb::Error::closed("fenced".into(), slatedb::CloseReason::Fenced);
    assert_eq!(classify(&fence.into()), Fault::Fenced);
    let stored = crate::state::stored("invalid bootstrap claim").context("publish");
    assert_eq!(classify(&stored), Fault::Data);
    let invariant = anyhow::anyhow!("the base checkpoint has no applied term");
    assert_eq!(classify(&invariant), Fault::Invariant);
}

#[test]
fn the_backoff_doubles_to_its_cap_and_a_publication_ends_the_mark() {
    let health = Health::default();
    let task = health.begin();
    let delays: Vec<_> = (0..7)
        .map(|_| task.failed(Fault::Unavailable, "throttled"))
        .collect();
    assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30].map(Duration::from_secs));
    assert!(health.handoff_due());
    // A writer that opens again is not down. The count stays until a publication.
    task.opened();
    assert!(!health.handoff_due());
    assert_eq!(health.consecutive(), 7);
    assert!(health.report(3, None).failed_secs.is_some());
    task.published();
    assert_eq!(
        health.report(3, None),
        WriterReport {
            failed_secs: None,
            term: 3,
            apply_held_at: None,
        }
    );
    assert_eq!(health.consecutive(), 0);
    // One data error makes a handoff due, and it waits longer.
    let data = Health::default();
    let task = data.begin();
    assert_eq!(
        task.failed(Fault::Data, "bad manifest"),
        Duration::from_secs(30)
    );
    assert!(data.handoff_due());
    assert_eq!(data.status()["last_error_class"], "data");
}

/// Item 9 of the review: a writer that stays open longer than a minute between failed flushes
/// is still down. Only a publication starts a new count, so the third failure hands over.
#[test]
fn a_long_open_between_failed_flushes_keeps_the_count() {
    let health = Health::default();
    let task = health.begin();
    for _ in 0..3 {
        task.opened();
        // The writer waited a long age limit before its flush failed.
        health.record().running = Some(Instant::now() - Duration::from_secs(90));
        task.failed(Fault::Unavailable, "the flush failed");
    }
    assert_eq!(health.consecutive(), 3);
    assert!(health.handoff_due());
}

#[test]
fn the_watchdog_counts_only_due_storage_work() {
    let health = Health::default();
    let task = health.begin();
    // A buffered write under a long age limit is not due yet.
    assert!(!health.check(Some(Instant::now() + Duration::from_secs(30))));
    assert!(!health.handoff_due());
    // A flush that was due one `STALL` ago and did not end is a fault, once.
    let overdue = Instant::now() - STALL - Duration::from_secs(1);
    assert!(health.check(Some(overdue)));
    assert!(!health.check(Some(overdue)));
    assert!(health.handoff_due());
    assert!(health.report(1, None).failed_secs.is_some());
    assert_eq!(health.total(), 1);
    task.done();
    assert!(!health.handoff_due());
}

/// Each durable step of a slow publication restarts the watchdog. A 20 s flush and 11 s of
/// checkpoint work are no fault. One step over `STALL` is.
#[test]
fn each_durable_step_restarts_the_watchdog() {
    let health = Health::default();
    let task = health.begin();
    let ago = |secs| Some(Instant::now() - Duration::from_secs(secs));
    task.due();
    // The flush took 20 s.
    health.record().due = ago(20);
    task.progressed();
    task.due();
    // The checkpoint work took 11 s more.
    let stepped = health.record().due.unwrap();
    health.record().due = Some(stepped - Duration::from_secs(11));
    assert!(!health.check(None));
    assert!(health.report(1, None).failed_secs.is_none());
    // The next step hangs past the limit.
    health.record().due = ago(31);
    assert!(health.check(None));
}

/// Round 2, item 1: a running step has its own timer. A later batch that ages past `STALL`
/// does not make a stall while the step is within its timer.
#[test]
fn a_running_step_keeps_its_own_timer() {
    let health = Health::default();
    let task = health.begin();
    task.due();
    health.record().due = Some(Instant::now() - Duration::from_secs(10));
    let aged = Some(Instant::now() - STALL - Duration::from_secs(1));
    assert!(!health.check(aged));
    // With no step running, the aged batch counts.
    task.done();
    assert!(health.check(aged));
}

/// Round 2, item 2: a failure ends the step. The long wait after a data fault is no stall, and
/// the timer starts again with the I/O of the next try. A restart works the same way.
#[test]
fn a_retry_wait_is_no_stall() {
    let health = Health::default();
    let task = health.begin();
    task.due();
    let delay = task.failed(Fault::Data, "bad manifest");
    assert!(delay >= STALL);
    assert!(health.record().due.is_none());
    assert!(!health.check(None));
    assert_eq!(health.status()["last_error_class"], "data");
    task.due();
    assert!(health.record().due.is_some());
    // A restart after a stall also clears the stall, so the node stops asking for restarts.
    health.record().due = Some(Instant::now() - STALL - Duration::from_secs(1));
    assert!(health.check(None));
    task.restarted();
    assert!(health.record().due.is_none() && !health.stalled());
}

/// A restart request that comes after the stall ended does nothing. The writer made progress,
/// so its next step runs.
#[tokio::test]
async fn a_restart_after_the_stall_ended_does_nothing() {
    let health = Health::default();
    let mut task = health.begin();
    task.due();
    health.record().due = Some(Instant::now() - STALL - Duration::from_secs(1));
    assert!(health.check(None));
    task.progressed();
    health.restart();
    assert!(!health.record().restart);
    let waited = tokio::time::timeout(Duration::from_millis(200), task.restart_requested());
    assert!(waited.await.is_err(), "a late restart request holds");
    // A request during a stall holds.
    health.record().due = Some(Instant::now() - STALL - Duration::from_secs(1));
    assert!(health.check(None));
    health.restart();
    let waited = tokio::time::timeout(Duration::from_millis(200), task.restart_requested());
    assert!(waited.await.is_ok());
}

/// A task of an earlier term changes nothing, also with a late error.
#[test]
fn a_late_result_of_an_earlier_term_changes_nothing() {
    let health = Health::default();
    let old = health.begin();
    old.due();
    health.end();
    let new = health.begin();
    old.failed(Fault::Unavailable, "a late error");
    old.due();
    old.published();
    assert_eq!(health.consecutive(), 0);
    assert_eq!(health.total(), 0);
    assert!(health.record().due.is_none());
    assert!(health.record().published.is_none());
    new.failed(Fault::Unavailable, "a current error");
    assert_eq!(health.consecutive(), 1);
}

fn candidate(id: u64, matched: u64, process: Option<u64>) -> Candidate {
    Candidate {
        id,
        matched,
        process,
    }
}

fn received(process: u64, at: Instant, failed_secs: Option<u64>) -> Received {
    Received {
        process,
        at,
        report: WriterReport {
            failed_secs,
            term: 7,
            apply_held_at: None,
        },
    }
}

fn tick<'a>(candidates: &'a [Candidate], published: Option<Instant>) -> Tick<'a> {
    Tick {
        due: true,
        stalled: false,
        transferee: None,
        wait: Duration::from_secs(2),
        candidates,
        published,
        incident: None,
        term: 7,
        moves: &[],
        held: false,
    }
}

/// Round 2, item 5: Raft ends a transfer after one election timeout. The handoff record ends
/// with it, so a MoveLeader to the same voter that starts later is not aborted.
#[test]
fn a_handoff_never_aborts_a_later_move_leader() {
    let now = Instant::now();
    let mut handoff = Handoff {
        led: Some(now - Duration::from_secs(60)),
        ..Default::default()
    };
    let candidates = [candidate(3, 30, Some(3))];
    handoff.reports.insert(3, received(3, now, None));
    let started = handoff.step(&tick(&candidates, None));
    assert_eq!(started.transfer, Some(3));
    // Raft timed the transfer out.
    let idle = Tick {
        due: false,
        ..tick(&candidates, None)
    };
    assert_eq!(handoff.step(&idle), Decision::default());
    assert!(handoff.pending.is_none());
    // A MoveLeader to the same voter, after the handoff's own wait.
    let moving = Tick {
        due: false,
        transferee: Some(3),
        moves: &[3],
        ..tick(&candidates, None)
    };
    assert_eq!(handoff.step(&moving), Decision::default());
    // Also a MoveLeader that starts while the handoff record still holds the voter.
    handoff.pending = Some((3, now - Duration::from_secs(3)));
    assert_eq!(handoff.step(&moving), Decision::default());
    assert!(handoff.pending.is_none());
}

/// Only a current report from this term without a mark makes a target. A mark does not
/// expire, a member without a report is no target, and a restart is a new process.
#[test]
fn a_fault_handoff_skips_marks_missing_and_old_reports() {
    let now = Instant::now();
    let led = now - Duration::from_secs(20);
    let mut handoff = Handoff {
        led: Some(led),
        ..Default::default()
    };
    let candidates = [
        candidate(2, 10, Some(20)),
        candidate(3, 30, Some(30)),
        candidate(4, 40, Some(40)),
        candidate(5, 50, Some(50)),
        candidate(6, 60, Some(60)),
    ];
    handoff.reports.insert(2, received(20, now, None));
    // A mark of more than five minutes still holds.
    handoff.reports.insert(3, received(30, now, Some(600)));
    // The member restarted. Its new process did not report.
    handoff.reports.insert(4, received(41, now, None));
    // The report came before this member led.
    let early = led - Duration::from_secs(1);
    handoff.reports.insert(5, received(50, early, None));
    // Member 6 sends no writer report.
    handoff.note(6, Some(60), None);
    assert_eq!(handoff.pick(&tick(&candidates, None)), Some(2));
    // An unreachable target is tried once.
    handoff.tried.insert(2);
    assert_eq!(handoff.pick(&tick(&candidates, None)), None);
}

/// Items 5 and 8 of the review: the report of a target comes from after the start of the
/// incident and from the current term. A voter that stopped after its last report, or that led
/// and faulted in a term between, is not a target.
#[test]
fn a_fault_handoff_needs_a_report_from_this_incident_and_term() {
    let now = Instant::now();
    let mut handoff = Handoff {
        led: Some(now - Duration::from_secs(600)),
        ..Default::default()
    };
    let candidates = [candidate(2, 90, Some(2)), candidate(3, 10, Some(3))];
    let incident = now - Duration::from_secs(10);
    // Member 2 is the most caught up, but it last reported long before the fault.
    handoff
        .reports
        .insert(2, received(2, now - Duration::from_secs(300), None));
    handoff.reports.insert(3, received(3, now, None));
    let mut current = tick(&candidates, None);
    current.incident = Some(incident);
    assert_eq!(handoff.pick(&current), Some(3));
    // A report of term 5 arrives in term 7, after member 3 led and faulted in term 6.
    handoff.reports.get_mut(&3).unwrap().report.term = 5;
    assert_eq!(handoff.pick(&current), None);
}

/// A mark that is older than this leader's last publication is a last resort. A late report
/// moves the time of its fault later, never earlier.
#[test]
fn a_mark_from_before_the_last_publication_is_a_last_resort() {
    let now = Instant::now();
    let led = now - Duration::from_secs(120);
    let published = Some(now - Duration::from_secs(20));
    let mut handoff = Handoff {
        led: Some(led),
        ..Default::default()
    };
    let candidates = [
        candidate(2, 90, Some(2)),
        candidate(3, 90, Some(3)),
        candidate(4, 10, Some(4)),
    ];
    // The fault of 2 was 40 s ago, before the publication. The fault of 3 was after it.
    handoff.reports.insert(2, received(2, now, Some(40)));
    handoff.reports.insert(3, received(3, now, Some(5)));
    handoff.reports.insert(4, received(4, now, None));
    assert_eq!(handoff.pick(&tick(&candidates, published)), Some(4));
    handoff.tried.insert(4);
    assert_eq!(handoff.pick(&tick(&candidates, published)), Some(2));
    handoff.tried.insert(2);
    assert_eq!(handoff.pick(&tick(&candidates, published)), None);
    // This incident had no publication, so no mark of it is stale.
    handoff.tried.clear();
    handoff.reports.remove(&4);
    assert_eq!(handoff.pick(&tick(&candidates, None)), None);
}

/// Item 2 of the review: a writer that opens and fails again stays in its incident, so no
/// voter is tried twice. A transfer that does not complete is aborted, so Raft takes
/// proposals again. With no target left, a stall restarts the writer. Only a publication ends
/// the incident.
#[test]
fn an_incident_tries_each_voter_once_until_a_publication() {
    let now = Instant::now();
    let mut handoff = Handoff {
        led: Some(now - Duration::from_secs(60)),
        ..Default::default()
    };
    let candidates = [candidate(2, 20, Some(2)), candidate(3, 30, Some(3))];
    handoff.reports.insert(2, received(2, now, None));
    handoff.reports.insert(3, received(3, now, None));
    let transfer = |target| Decision {
        transfer: Some(target),
        ..Default::default()
    };
    assert_eq!(handoff.step(&tick(&candidates, None)), transfer(3));
    // The writer opens again for a moment. The transfer is still in flight.
    let reopened = Tick {
        due: false,
        transferee: Some(3),
        ..tick(&candidates, None)
    };
    assert_eq!(handoff.step(&reopened), Decision::default());
    // The transfer does not complete in time. The leader aborts it.
    handoff.pending = Some((3, now - Duration::from_secs(3)));
    let aborted = handoff.step(&reopened);
    assert_eq!(
        aborted,
        Decision {
            abort: true,
            ..Default::default()
        }
    );
    // The writer stalls again in the same incident. Member 3 is not tried again.
    assert_eq!(handoff.step(&tick(&candidates, None)), transfer(2));
    handoff.pending = Some((2, now - Duration::from_secs(3)));
    let stalled = Tick {
        stalled: true,
        transferee: Some(2),
        ..tick(&candidates, None)
    };
    let last = Decision {
        abort: true,
        transfer: None,
        restart: true,
    };
    assert_eq!(handoff.step(&stalled), last);
    // A publication of this writer ends the incident.
    let published = tick(&candidates, Some(Instant::now()));
    assert_eq!(handoff.step(&published), transfer(3));
}

/// A shared state with the membership seed, and the writer of its leader.
struct Leader {
    state: StateMachine,
    handles: crate::state::shared::Handles,
    writer: Writer,
    events: mpsc::Sender<Event>,
    announced: mpsc::Receiver<Event>,
}

impl Leader {
    async fn open(store: Arc<dyn ObjectStore>, max_age: Duration, clock: Clock) -> Self {
        let options = super::super::tests::voter_options(false);
        let mut state = StateMachine::open_shared(
            store,
            "root",
            "test",
            crate::state::StateCache::default(),
            &options,
        )
        .await
        .unwrap();
        state.members = Arc::new(crate::cluster::Store::initial(
            &[(1, "127.0.0.1:1".to_owned())].into(),
        ));
        state.seed_for_test(1).await.unwrap();
        let handles = state.shared_handles();
        let (events, announced) = mpsc::channel(64);
        let writer = new_writer(&handles, max_age, clock);
        Self {
            state,
            handles,
            writer,
            events,
            announced,
        }
    }

    fn writer(&mut self) -> &mut Writer {
        &mut self.writer
    }

    fn lead(&mut self, term: u64) {
        let events = self.events.clone();
        self.writer().sync(true, term, &events);
    }

    /// Accept announcements until one covers `index`, or fail after `wait`.
    async fn covered(&mut self, index: u64, wait: Duration) {
        let source = self.handles.source.clone();
        let announced = &mut self.announced;
        let covered = async {
            while let Some(event) = announced.recv().await {
                if let Event::ViewCheckpoint(id, row, reply) = event {
                    if let Some(reply) = reply {
                        let _ = reply.send(true);
                    }
                    if row >= index {
                        return source.open_base(id).await.unwrap();
                    }
                }
            }
            panic!("the writer stopped");
        };
        let base = tokio::time::timeout(wait, covered)
            .await
            .expect("no checkpoint covers the index");
        assert!(base.applied >= index);
    }

    /// Fail when the writer task reports a failure within `wait`.
    async fn no_failure(&mut self, wait: Duration) {
        let failed = async {
            loop {
                if let WriterEvent::Failed(error) = next(&mut self.writer).await {
                    return error;
                }
            }
        };
        if let Ok(error) = tokio::time::timeout(wait, failed).await {
            panic!("the writer stopped the member: {error:#}");
        }
    }

    async fn put(&mut self, key: &str) -> u64 {
        let index = self.apply_put(key).await;
        self.writer().progress().wait_written(index).await.unwrap();
        index
    }

    /// Apply a Put. Do not wait for the materializer.
    async fn apply_put(&mut self, key: &str) -> u64 {
        let put = crate::proto::etcdserverpb::RequestOp {
            request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
                PutRequest {
                    key: key.as_bytes().to_vec(),
                    value: b"value".to_vec(),
                    ..Default::default()
                },
            )),
        };
        let index = self.state.applied + 1;
        self.state.apply_etcd(index, &put).await.unwrap();
        index
    }

    async fn close(mut self) {
        let events = self.events.clone();
        let term = self.writer.term();
        self.writer.sync(false, term, &events);
        self.writer.stop().await;
        self.state.close().await.unwrap();
    }
}

fn clock() -> Clock {
    Clock::system(Duration::from_secs(5), None)
}

/// Item 1 of the review: the publication of a Defragment fails, and the writer opens again.
/// The republish request survives the open, so the Defragment completes with no new write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_defragment_survives_a_failed_publication() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let mut leader = Leader::open(faults.clone(), Duration::from_millis(100), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    for n in 0..3 {
        leader.put(&format!("k{n}")).await;
    }
    let index = leader.state.applied;
    leader.covered(index, Duration::from_secs(10)).await;
    let writer = &leader.writer;
    let (reached, resume) = (Arc::default(), Arc::<tokio::sync::Notify>::default());
    let defragment = Defragment {
        admin: leader.handles.admin.clone(),
        owner: None,
        announced: leader.handles.announced.clone(),
        pause: Some((
            DefragmentStep::Publish,
            Arc::clone(&reached),
            resume.clone(),
        )),
        ..writer.defragment(2).unwrap()
    };
    let health = writer.health().clone();
    let running = tokio::spawn(defragment.run());
    reached.notified().await;
    faults.set(FAIL_STATE_PUT);
    resume.notify_one();
    eventually("the failed publication", async || health.consecutive() > 0).await;
    faults.set(0);
    // Apply each announcement, as the members do, until the Defragment completes.
    let completed = tokio::time::timeout(Duration::from_secs(10), async {
        while !running.is_finished() {
            let next = tokio::time::timeout(Duration::from_millis(200), leader.announced.recv());
            if let Ok(Some(Event::ViewCheckpoint(id, _, reply))) = next.await {
                if let Some(reply) = reply {
                    let _ = reply.send(true);
                }
                let index = leader.state.applied + 1;
                let id = id.to_string();
                leader
                    .state
                    .apply_view_checkpoint(index, &id, 0)
                    .await
                    .unwrap();
            }
        }
        running.await.unwrap()
    });
    let result = completed.await.expect("the Defragment did not complete");
    assert!(result.is_ok(), "{result:?}");
    leader.close().await;
}

/// Item 3 of the review: a read of the bootstrap marker hangs. The watchdog runs during the
/// read, and a step down ends the writer task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_bootstrap_read_starts_the_watchdog_and_ends_at_a_step_down() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let mut leader = Leader::open(faults.clone(), Duration::from_millis(100), clock()).await;
    faults.set(STALL_BOOTSTRAP_READ);
    leader.lead(2);
    let health = leader.writer().health().clone();
    eventually("the watchdog", async || health.record().due.is_some()).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(health.record().due.is_some());
    let events = leader.events.clone();
    leader.writer().sync(false, 2, &events);
    let ended = tokio::time::timeout(Duration::from_secs(2), leader.writer().tasks().join_next());
    let (term, result) = ended.await.expect("the task did not end").unwrap().unwrap();
    assert_eq!(term, 2);
    result.unwrap();
    assert!(health.record().due.is_none());
    faults.set(0);
    leader.close().await;
}

/// Round 2, item 3: the bootstrap read, the open, and the first reads of the publisher are
/// steps, each with its own timer. Two slow steps in a row are no stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_writer_start_step_has_its_own_timer() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let mut leader = Leader::open(faults.clone(), Duration::from_millis(100), clock()).await;
    faults.set(STALL_BOOTSTRAP_READ);
    leader.lead(2);
    let health = leader.writer().health().clone();
    eventually("the bootstrap step", async || health.record().due.is_some()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    // The bootstrap read ends. The open of the shared database hangs instead.
    let opening = Instant::now();
    faults.set(STALL_STATE_PUT);
    eventually("the open step", async || {
        health.record().due.is_some_and(|due| due >= opening)
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    // The open ends. The first read of the publisher hangs instead.
    let reading = Instant::now();
    faults.set(STALL_BOOTSTRAP_READ);
    eventually("the writer open", async || leader.writer.open().is_some()).await;
    let due = health.record().due.unwrap();
    assert!(
        due >= reading,
        "the publisher reads inherit an earlier timer"
    );
    faults.set(0);
    leader.covered(1, Duration::from_secs(10)).await;
    leader.close().await;
}

/// Round 2, item 4: the read after a publication hangs, and an idle Defragment asks for a
/// republish. The request has its own timer, so the watchdog sees the stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_behind_a_hung_read_starts_the_watchdog() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let mut leader = Leader::open(faults.clone(), Duration::from_millis(100), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    let index = leader.put("k").await;
    // The publisher reads the checkpoint list after this announcement. That read hangs.
    let announced = tokio::time::timeout(Duration::from_secs(10), leader.announced.recv());
    let Some(Event::ViewCheckpoint(_, row, Some(reply))) = announced.await.unwrap() else {
        panic!("no announcement");
    };
    assert!(row >= index);
    faults.set(STALL_MANIFEST_READ);
    reply.send(true).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let requested = Instant::now();
    leader.writer().progress().republish();
    let writer = &leader.writer;
    until("the stall", STALL + Duration::from_secs(5), async || {
        writer.health().check(writer.waiting())
    })
    .await;
    assert!(requested.elapsed() >= STALL);
    faults.set(0);
    leader.close().await;
}

/// A clock refusal counts at once, and the next open does not wait for the old writer, whose
/// close waits on a held manifest read. The clock recovers, and the new writer publishes while
/// that read is still held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clock_refusal_does_not_wait_for_the_old_close() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let mut first = Leader::open(faults.clone(), Duration::from_millis(100), clock()).await;
    first.lead(2);
    first.covered(1, Duration::from_secs(10)).await;
    first.put("tick").await;
    first.covered(2, Duration::from_secs(10)).await;
    let events = first.events.clone();
    // The first writer closes completely, so the next writer is the only one that reads the
    // manifest in the background.
    first.writer().sync(false, 2, &events);
    first.writer().stop().await;
    // The first clock check after the open holds the next manifest read of the opened writer,
    // and refuses only once that read waits. The read then holds the close of the refused
    // writer. Each later check reads the real clock, so the next writer opens. A check before
    // the open finds no writer that polls its manifest, so it reads the real clock.
    let armed = Arc::new(AtomicBool::new(true));
    let mut skewed = clock();
    let now = clock().now_ms;
    let (arm, store) = (armed.clone(), faults.clone());
    skewed.now_ms = Arc::new(move || {
        if !arm.swap(false, Ordering::SeqCst) {
            return now();
        }
        // An opened writer polls its manifest in the background every 100 ms. Wait until that
        // poll holds the read.
        store.set(PAUSE_MANIFEST_READ);
        let limit = Instant::now() + Duration::from_millis(500);
        while store.entered.available_permits() == 0 {
            if Instant::now() >= limit {
                store.set(0);
                arm.store(true, Ordering::SeqCst);
                return now();
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        now() - 60_000
    });
    let mut writer = new_writer(&first.handles, Duration::from_millis(100), skewed);
    writer.sync(true, 3, &events);
    let health = writer.health().clone();
    eventually("the clock fault", async || {
        health.status()["last_error_class"] == "clock"
    })
    .await;
    let held = tokio::time::timeout(Duration::from_secs(5), faults.entered.acquire());
    held.await.unwrap().unwrap().forget();
    let index = first.state.applied + 1;
    let put = crate::proto::etcdserverpb::RequestOp {
        request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
            put("after", "v"),
        )),
    };
    first.state.apply_etcd(index, &put).await.unwrap();
    first.covered(index, Duration::from_secs(15)).await;
    assert_eq!(health.pending_closes(), 1);
    faults.release.add_permits(1);
    writer.sync(false, 3, &events);
    writer.stop().await;
    first.close().await;
}

/// A database whose memtable flush hangs in an SST put that never returns.
async fn hung_writer(faults: &Arc<FaultStore>, path: &str) -> Arc<slatedb::Db> {
    let store: Arc<dyn ObjectStore> = faults.clone();
    let db = slatedb::Db::builder(path, store)
        .with_settings(crate::state::slatedb_settings())
        .build()
        .await
        .unwrap();
    let db = Arc::new(db);
    db.put(b"key", b"value").await.unwrap();
    faults.set(PAUSE_PUT);
    let flushing = db.clone();
    tokio::spawn(async move { flushing.flush().await });
    faults.entered.acquire().await.unwrap().forget();
    db
}

/// Item 6 of the review: closes of failed writers whose I/O hangs forever. At most one close
/// stays pending, and a shutdown waits for it no longer than its limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_most_one_hung_close_stays_and_a_shutdown_does_not_wait_for_it() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let mut leader = Leader::open(faults.clone(), Duration::from_millis(100), clock()).await;
    let task = leader.writer().health().begin();
    let mut writers = vec![];
    for n in 0..3 {
        let writer = hung_writer(&faults, &format!("hung-{n}")).await;
        task.abandon(writer.clone());
        writers.push(writer);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let health = leader.writer().health();
    assert_eq!(health.pending_closes(), 1);
    assert_eq!(health.aborted_closes(), 2);
    // Round 2: a task that ends during the shutdown abandons one more writer. The shutdown
    // still takes that close and aborts it within its limit.
    let late = hung_writer(&faults, "hung-late").await;
    let (ending, closing) = (task.clone(), late.clone());
    leader.writer().tasks().spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        ending.abandon(closing);
        (2, Ok(()))
    });
    // The test and the hung flush hold each writer. Only the newest close holds one more.
    let held: Vec<_> = writers.iter().map(Arc::strong_count).collect();
    assert_eq!(held, [2, 2, 3]);
    let started = Instant::now();
    leader.writer().stop().await;
    assert!(
        started.elapsed() < Duration::from_secs(7),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(leader.writer().health().pending_closes(), 0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(Arc::strong_count(&late), 2, "the late close still runs");
    leader.close().await;
}

/// Item 11 of the review: the publisher reads back an applied row that does not decode. The
/// row is in the shared store, so the writer halts and a handoff is due. The member stays up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stored_row_that_does_not_decode_halts_the_writer() {
    let store = Arc::new(InMemory::new());
    let mut leader = Leader::open(store, Duration::from_millis(100), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    let writer = leader.writer();
    let db = writer.open().unwrap().db;
    let mut batch = slatedb::WriteBatch::new();
    batch.put(crate::state::APPLIED, b"bad");
    db.write_with_options(batch, &WriteOptions { seqnum: 2 })
        .await
        .unwrap();
    writer.progress().request(1);
    let health = writer.health().clone();
    eventually("the data fault", async || {
        health.status()["last_error_class"] == "data"
    })
    .await;
    leader.no_failure(Duration::from_secs(2)).await;
    assert!(health.handoff_due());
    leader.close().await;
}

/// A flush outside the publisher fails and SlateDB closes the idle writer. No write follows.
/// The publisher sees the closure, the task opens the writer again, and a publication request
/// that waited before the fault still completes long before the age limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_writer_that_slatedb_closes_opens_again() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let mut leader = Leader::open(faults.clone(), Duration::from_secs(60), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    let index = leader.put("idle").await;
    faults.set(FAIL_PUT);
    let failed = leader.writer().open().unwrap().db;
    assert!(failed.flush().await.is_err());
    let health = leader.writer().health().clone();
    eventually("the writer fault", async || health.consecutive() > 0).await;
    assert_eq!(health.status()["last_error_class"], "unavailable");
    leader.writer().request_publication(index);
    leader.no_failure(Duration::from_secs(2)).await;
    faults.set(0);
    leader.covered(index, Duration::from_secs(20)).await;
    let open = leader.writer().open().unwrap().db;
    assert!(!Arc::ptr_eq(&open, &failed));
    assert!(health.report(2, None).failed_secs.is_none());
    leader.close().await;
}

/// The publisher takes a publication request and its flush fails. The request stays, so the
/// next writer publishes it after the store recovers, with no new write and a long age limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_flush_keeps_the_publication_request() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let mut leader = Leader::open(faults.clone(), Duration::from_secs(60), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    let index = leader.put("requested").await;
    faults.set(FAIL_PUT);
    leader.writer().request_publication(index);
    let health = leader.writer().health().clone();
    eventually("the failed flush", async || health.consecutive() > 0).await;
    leader.no_failure(Duration::from_secs(1)).await;
    faults.set(0);
    leader.covered(index, Duration::from_secs(20)).await;
    leader.close().await;
}

/// A buffered write under an age limit above `STALL` starts no watchdog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_buffered_write_under_a_long_age_limit_is_not_due() {
    let store = Arc::new(InMemory::new());
    let mut leader = Leader::open(store, Duration::from_secs(40), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    leader.put("buffered").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let writer = leader.writer();
    let deadline = writer.progress().deadline(writer.max_age()).unwrap();
    assert!(deadline > Instant::now() + STALL);
    assert!(!writer.health().check(Some(deadline)));
    assert!(!writer.health().handoff_due());
    assert!(writer.health().report(2, None).failed_secs.is_none());
    leader.close().await;
}

/// A pending snapshot restore holds every member's writer. It is not a writer fault, so it
/// starts no watchdog and makes no handoff due.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pending_restore_starts_no_watchdog() {
    use futures::TryStreamExt;
    use slatedb::object_store::ObjectStoreExt;
    let store = Arc::new(InMemory::new());
    let mut leader = Leader::open(store.clone(), Duration::from_millis(100), clock()).await;
    // The open claimed an empty bootstrap. A restore claims it instead.
    let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
    let claim = objects
        .iter()
        .find(|object| object.location.as_ref().ends_with("bootstrap-op.json"))
        .unwrap();
    store.delete(&claim.location).await.unwrap();
    let restore = crate::state::shared::BootstrapOp::Restore {
        snapshot: "pending".into(),
    };
    let claimed = crate::state::shared::claim_bootstrap(&leader.handles.source, &restore)
        .await
        .unwrap();
    assert_eq!(claimed, restore);
    leader.lead(2);
    leader.no_failure(Duration::from_secs(3)).await;
    let health = leader.writer().health();
    assert!(health.record().due.is_none());
    assert!(!health.check(None));
    assert!(!health.handoff_due());
    assert_eq!(health.total(), 0);
    leader.close().await;
}

/// A clock that is too far behind the shared writer clock counts as a writer fault. After three
/// refusals the leader must hand over its leadership.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clock_over_the_skew_limit_makes_a_handoff_due() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut first = Leader::open(store.clone(), Duration::from_millis(100), clock()).await;
    first.lead(2);
    first.covered(1, Duration::from_secs(10)).await;
    first.put("tick").await;
    first.covered(2, Duration::from_secs(10)).await;
    let mut lagging = clock();
    let now = clock().now_ms;
    lagging.now_ms = Arc::new(move || now() - 60_000);
    let events = first.events.clone();
    first.writer().sync(false, 2, &events);
    let mut writer = new_writer(&first.handles, Duration::from_millis(100), lagging);
    writer.sync(true, 3, &first.events);
    let health = writer.health().clone();
    eventually("three clock refusals", async || health.handoff_due()).await;
    assert_eq!(health.status()["last_error_class"], "clock");
    writer.sync(false, 3, &first.events);
    writer.stop().await;
    first.close().await;
}

/// A leader of three voters gets no clock samples. While it is idle, the wait is no due work
/// and never stalls. A batch that waits counts toward `STALL`: the fault is a stall, not a clock
/// fault, so `clock_alarm` stays clear and the handoff is due.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clock_wait_stalls_only_with_a_due_batch() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = clock();
    let voters = [1, 2, 3].map(|id| (id, Some(id))).into_iter().collect();
    clock.configure(2, 1, voters);
    let mut leader = Leader::open(store, Duration::from_millis(100), clock.clone()).await;
    leader.lead(2);
    eventually("the wait for samples", async || clock.waiting()).await;
    // An earlier term of this member wrote each batch, so no batch waits.
    let applied = leader.state.applied;
    let writer = leader.writer();
    writer.progress().reset(applied);
    writer.note_clock_wait();
    assert_eq!(writer.waiting(), None);
    assert!(!writer.health().check(writer.waiting()));
    assert!(!writer.health().handoff_due());
    // A batch now waits for the writer.
    let index = leader.state.applied + 1;
    let put = crate::proto::etcdserverpb::RequestOp {
        request: Some(crate::proto::etcdserverpb::request_op::Request::RequestPut(
            put("k-due", "v"),
        )),
    };
    leader.state.apply_etcd(index, &put).await.unwrap();
    let writer = leader.writer();
    writer.note_clock_wait();
    assert!(writer.clock_due().is_some());
    writer.set_clock_due(Some(Instant::now() - STALL - Duration::from_secs(1)));
    writer.note_clock_wait();
    assert!(writer.health().check(writer.waiting()));
    assert!(writer.health().handoff_due());
    assert_eq!(writer.health().status()["last_error_class"], "stalled");
    assert_eq!(writer.clock().alarm_name(), None);
    leader.close().await;
}

/// The writer clock of three voters is one hour behind the machine clock. The writer task
/// pauses just after it publishes its open writer. A Defragment that reads the slot then gets
/// the `Db` together with an admin on its clock, and the compactor of the writer accepts the
/// job and its outputs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_defragment_gets_the_open_writer_with_its_clock() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut clock = clock();
    let machine = clock.now_ms.clone();
    clock.now_ms = Arc::new(move || machine() - 3_600_000);
    let voters = || [1, 2, 3].map(|id| (id, Some(id))).into_iter().collect();
    clock.configure(2, 1, voters());
    // Both peers agree with the writer clock. Their samples stay fresh.
    let sampling = sample_peers(&clock);
    let mut leader = Leader::open(store, Duration::from_millis(100), clock.clone()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    leader.put("before").await;
    leader.covered(2, Duration::from_secs(10)).await;
    let events = leader.events.clone();
    leader.writer().sync(false, 2, &events);
    leader.writer().stop().await;
    let reached = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    leader
        .writer()
        .hold_open(Some((reached.clone(), resume.clone())));
    clock.configure(3, 1, voters());
    leader.writer().sync(true, 3, &events);
    reached.notified().await;
    let Some(Open { db, admin, .. }) = leader.writer().open() else {
        panic!("the published writer has no slot");
    };
    db.flush().await.unwrap();
    crate::state::maintenance::compact_all(&admin, &|| Ok(()))
        .await
        .unwrap();
    leader.writer().hold_open(None);
    resume.notify_one();
    sampling.abort();
    leader.close().await;
}

/// Issue #106: the writer task of term 3 starts before the task of term 2 ends. The old
/// materializer writes a batch to its memtable only, and it pauses before its progress update.
/// The update comes after the new writer opened. The new materializer must still write the
/// batch, because the new writer does not have it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_update_of_an_old_materializer_does_not_skip_a_batch() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // No flush by age. The batch stays in the memtable of the old writer.
    let mut leader = Leader::open(store, Duration::from_secs(3600), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    let (reached, paused) = std::sync::mpsc::sync_channel(1);
    let (resume, resumed) = std::sync::mpsc::sync_channel(1);
    *leader.writer().progress().hold.lock().unwrap() = Some((reached, resumed));
    let index = leader.apply_put("k").await;
    paused.recv_timeout(Duration::from_secs(10)).unwrap();
    // The new task pauses after its open, before its materializer starts.
    let (opened, start) = (Arc::default(), Arc::default());
    leader
        .writer()
        .hold_open(Some((Arc::clone(&opened), Arc::clone(&start))));
    let events = leader.events.clone();
    leader.writer().sync(true, 3, &events);
    opened.notified().await;
    assert!(leader.writer().progress().written() < index);
    // The old task updates the progress, sees its stop, and ends.
    resume.send(()).unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(10), leader.writer().tasks().join_next());
    assert!(matches!(ended.await.unwrap(), Some(Ok((2, Ok(()))))));
    leader.writer().hold_open(None);
    start.notify_one();
    leader.writer().progress().request(index);
    leader.covered(index, Duration::from_secs(10)).await;
    leader.close().await;
}

/// SlateDB's default clock plus a fixed lead, for an admin that adds a late job.
#[derive(Debug)]
struct AheadBy(Duration, slatedb_common::clock::DefaultSystemClock);

impl slatedb_common::clock::SystemClock for AheadBy {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        self.1.now() + self.0
    }

    fn sleep<'a>(
        &'a self,
        duration: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        self.1.sleep(duration)
    }

    fn ticker<'a>(&'a self, duration: Duration) -> slatedb_common::clock::SystemClockTicker<'a> {
        slatedb_common::clock::SystemClockTicker::new(&self.1, duration)
    }
}

/// Submit a compaction of every L0 SST, with a job ID `ahead` of the machine clock.
async fn submit_ahead(leader: &Leader, ahead: Duration) {
    use slatedb::compactor::{CompactionSpec, SourceId};
    let source = &leader.handles.source;
    let clock = AheadBy(ahead, slatedb_common::clock::DefaultSystemClock::new());
    let admin = slatedb::admin::Admin::builder(source.state(), source.store.clone())
        .with_system_clock(Arc::new(clock))
        .build();
    let manifest = admin.read_manifest(None).await.unwrap().unwrap();
    let sources = manifest.l0().iter().map(|view| SourceId::SstView(view.id));
    let spec = CompactionSpec::new(sources.collect(), 0);
    admin.submit_compaction(spec).await.unwrap();
}

/// The compaction jobs of the shared state.
async fn jobs(leader: &Leader) -> usize {
    let source = &leader.handles.source;
    let admin = slatedb::admin::Admin::builder(source.state(), source.store.clone()).build();
    let compactions = admin.read_compactions(None).await.unwrap();
    compactions.map_or(0, |compactions| compactions.recent_compactions().count())
}

/// Keep the samples of peers 2 and 3 fresh, with clocks that agree with `clock`.
fn sample_peers(clock: &Clock) -> tokio::task::JoinHandle<()> {
    let clock = clock.clone();
    tokio::spawn(async move {
        loop {
            for peer in [2, 3] {
                let probe = clock.probe();
                let now = (clock.mono_ms)();
                let reply = crate::node::ProbeReply {
                    nonce: clock::probe_nonce(&probe),
                    process: peer,
                    clock_ms: (clock.now_ms)(),
                };
                clock.sample(peer, probe, reply, now, now);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
}

/// SlateDB 0.18.1 reports a refused ID time as an `Invalid` error. It is a clock condition. The
/// writer opens again at once a few times with no fault, and then counts a clock fault.
#[test]
fn an_id_time_refusal_opens_the_writer_again_a_few_times() {
    let refused = || {
        let error = slatedb::Error::invalid(
            "invalid clock tick, must be monotonic. last_tick=`50`, next_tick=`30`".into(),
        );
        anyhow::Error::from(error).context("the shared writer closed")
    };
    assert_eq!(classify(&refused()), Fault::Clock);
    let other = anyhow::Error::from(slatedb::Error::invalid("an invalid range".into()));
    assert!(!id_time_refused(&other));
    let health = Health::default();
    let task = health.begin();
    let reopens = std::sync::atomic::AtomicU64::new(0);
    for _ in 0..ID_TIME_REOPENS {
        assert_eq!(
            recover(refused(), 2, &task, &reopens).unwrap(),
            Duration::ZERO
        );
    }
    assert_eq!(health.consecutive(), 0);
    assert!(!health.handoff_due());
    assert_eq!(reopens.load(std::sync::atomic::Ordering::Relaxed), 3);
    assert!(recover(refused(), 2, &task, &reopens).unwrap() > Duration::ZERO);
    assert_eq!(health.status()["last_error_class"], "clock");
    // A publication ends the count.
    task.published();
    assert_eq!(
        recover(refused(), 2, &task, &reopens).unwrap(),
        Duration::ZERO
    );
}

/// A leader of three voters whose samples stay fresh, after its first publication.
async fn sampled_leader() -> (Leader, Clock, tokio::task::JoinHandle<()>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let clock = clock();
    clock.configure(2, 1, three_voters());
    let sampling = sample_peers(&clock);
    let mut leader = Leader::open(store, Duration::from_millis(100), clock.clone()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    (leader, clock, sampling)
}

fn three_voters() -> BTreeMap<u64, Option<u64>> {
    [1, 2, 3].map(|id| (id, Some(id))).into_iter().collect()
}

/// End the term of the writer, and open its successor of term 3.
async fn hand_over(leader: &mut Leader, clock: &Clock) {
    let old = leader.writer().open().unwrap().db;
    let events = leader.events.clone();
    leader.writer().sync(false, 2, &events);
    leader.writer().stop().await;
    clock.configure(3, 1, three_voters());
    leader.writer().sync(true, 3, &events);
    eventually("the successor", async || {
        let slot = leader.writer.open();
        slot.is_some_and(|slot| !Arc::ptr_eq(&slot.db, &old))
    })
    .await;
}

/// Sig-Codex, round 3: a Defragment pauses after its flush, and its term ends. A successor opens.
/// The old Defragment resumes. It submits no job, because its writer term ended, and the
/// successor's `Db` stays open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_defragment_of_an_ended_term_submits_no_job() {
    let (mut leader, clock, sampling) = sampled_leader().await;
    leader.put("before").await;
    leader.covered(2, Duration::from_secs(10)).await;
    let (reached, resume) = (Arc::default(), Arc::default());
    let defragment = Defragment {
        pause: Some((
            DefragmentStep::Compact,
            Arc::clone(&reached),
            Arc::clone(&resume),
        )),
        ..leader.writer().defragment(2).unwrap()
    };
    let paused = tokio::spawn(defragment.run());
    reached.notified().await;
    hand_over(&mut leader, &clock).await;
    let before = jobs(&leader).await;
    resume.notify_one();
    assert!(paused.await.unwrap().is_err());
    assert_eq!(
        jobs(&leader).await,
        before,
        "a Defragment of an ended term added a job"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let successor = leader.writer().open().unwrap().db;
    assert!(successor.status().close_reason.is_none());
    sampling.abort();
    leader.close().await;
}

/// Critic, round 3: a job reaches storage above the successor's floor, as a submission that was
/// already inside the store update does. SlateDB refuses the successor's next compaction ID and
/// closes its `Db`. The successor opens again at once above the job, in the same term, with no
/// fault and no handoff, and it publishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_job_costs_one_reopen_and_no_handoff() {
    let (mut leader, clock, sampling) = sampled_leader().await;
    hand_over(&mut leader, &clock).await;
    // Answer each announcement, and keep the highest announced row.
    let (_, stub) = mpsc::channel(1);
    let mut announced = std::mem::replace(&mut leader.announced, stub);
    let row = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let answering = tokio::spawn({
        let row = row.clone();
        async move {
            while let Some(event) = announced.recv().await {
                if let Event::ViewCheckpoint(_, at, Some(reply)) = event {
                    let _ = reply.send(true);
                    row.fetch_max(at, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
    });
    // The late job lands 3 s above the successor's clock. Four new L0 SSTs then make the
    // compactor of the successor schedule a job below it.
    let writer = leader.writer();
    let reopened = writer.reopens();
    let faults = writer.health().total();
    let first = writer.open().unwrap().db;
    submit_ahead(&leader, Duration::from_secs(3)).await;
    for n in 0..4 {
        leader.put(&format!("k{n}")).await;
        let db = leader.writer().open().unwrap().db;
        let _ = db.flush().await;
    }
    until(
        "the reopen above the job",
        Duration::from_secs(30),
        async || leader.writer.reopens() > reopened,
    )
    .await;
    let index = leader.put("after").await;
    until(
        "a checkpoint of the reopened writer",
        Duration::from_secs(30),
        async || row.load(std::sync::atomic::Ordering::SeqCst) >= index,
    )
    .await;
    answering.abort();
    // One reopen in the same term, onto a new `Db`, with no fault at all.
    let writer = leader.writer();
    let reopens = writer.reopens();
    assert_eq!(reopens, reopened + 1);
    let second = writer.open().unwrap().db;
    assert!(
        !Arc::ptr_eq(&first, &second),
        "the writer did not open a new Db"
    );
    assert_eq!(writer.term(), 3);
    assert_eq!(writer.leadership(), (true, 3));
    assert!(writer.running());
    assert_eq!(
        writer.health().total(),
        faults,
        "{}",
        writer.health().status()
    );
    assert!(!writer.health().handoff_due());
    assert_eq!(writer.health().status()["marked"], false);
    sampling.abort();
    leader.close().await;
}

/// Sig-Codex, round 4: a Defragment of the current term meets a job 3 s above its writer clock.
/// SlateDB refuses the Defragment's job. The reply is a retryable `Unavailable`, not
/// `DataLoss` for corrupt data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_defragment_job_is_retryable() {
    let (mut leader, _clock, sampling) = sampled_leader().await;
    leader.put("before").await;
    leader.covered(2, Duration::from_secs(10)).await;
    submit_ahead(&leader, Duration::from_secs(3)).await;
    let defragment = leader.writer().defragment(2).unwrap();
    let error = defragment.run().await.unwrap_err();
    assert_eq!(
        error.code,
        tonic::Code::Unavailable as i32,
        "{}",
        error.message
    );
    assert_eq!(
        error.message,
        "etcdserver: a newer compaction job stands above the writer clock; retry"
    );
    sampling.abort();
    leader.close().await;
}

/// Sig-Codex, round 4: `sync_writer` refreshes the clock before the leadership receiver. In
/// that window the gate refuses the Defragment's submission while the receiver still shows the
/// term. The reply is the lost-leader error, not `DataLoss`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gate_that_ends_first_gives_the_lost_leader_reply() {
    let (mut leader, clock, sampling) = sampled_leader().await;
    leader.put("before").await;
    leader.covered(2, Duration::from_secs(10)).await;
    let (_term, leadership) = tokio::sync::watch::channel((true, 2));
    let defragment = Defragment {
        leadership,
        ..leader.writer().defragment(2).unwrap()
    };
    clock.stand_down();
    let error = defragment.run().await.unwrap_err();
    assert_eq!(
        error.code,
        tonic::Code::Unavailable as i32,
        "{}",
        error.message
    );
    assert_eq!(
        error.message,
        "etcdserver: the leader changed during Defragment; retry"
    );
    sampling.abort();
    leader.close().await;
}

/// Sig-Codex, round 3: a stored floor 60 s ahead of the clock refuses the writer before its
/// open. The writer epoch in the manifest does not move, so no open and no fence happened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stored_floor_far_ahead_refuses_before_the_open() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut leader = Leader::open(store, Duration::from_millis(100), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    let events = leader.events.clone();
    leader.writer().sync(false, 2, &events);
    leader.writer().stop().await;
    submit_ahead(&leader, Duration::from_secs(60)).await;
    let source = leader.handles.source.clone();
    let epoch = || async {
        let admin = slatedb::admin::Admin::builder(source.state(), source.store.clone()).build();
        admin
            .read_manifest(None)
            .await
            .unwrap()
            .unwrap()
            .writer_epoch()
    };
    let before = epoch().await;
    leader.writer().sync(true, 3, &events);
    let health = leader.writer().health().clone();
    eventually("the clock refusal", async || {
        health.status()["last_error_class"] == "clock"
    })
    .await;
    assert_eq!(leader.writer().clock().alarm_name(), Some("behind"));
    assert!(leader.writer().open().is_none());
    assert_eq!(
        epoch().await,
        before,
        "the writer opened before its clock check"
    );
    leader.close().await;
}

/// An error of this member's own invariants still stops the member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_invariant_error_stops_the_member() {
    let store = Arc::new(InMemory::new());
    let mut leader = Leader::open(store, Duration::from_millis(100), clock()).await;
    *leader.handles.source.fail_writer.lock().unwrap() =
        Some(anyhow::anyhow!("an injected invariant error"));
    leader.lead(2);
    let failed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let WriterEvent::Failed(error) = next(&mut leader.writer).await {
                return error;
            }
        }
    })
    .await
    .expect("the member did not stop");
    assert!(format!("{failed:#}").contains("invariant"), "{failed:#}");
    leader.close().await;
}

/// A Data error halts the writer for the long backoff. The member stays up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_data_error_halts_the_writer_and_keeps_the_member() {
    let store = Arc::new(InMemory::new());
    let mut leader = Leader::open(store, Duration::from_millis(100), clock()).await;
    let data = slatedb::Error::data("an injected manifest that does not decode".into());
    *leader.handles.source.fail_writer.lock().unwrap() = Some(data.into());
    leader.lead(2);
    leader.no_failure(Duration::from_secs(3)).await;
    let health = leader.writer().health().clone();
    assert_eq!(health.status()["last_error_class"], "data");
    assert!(health.handoff_due());
    assert!(leader.writer().open().is_none());
    // The 30 s wait runs with no step timer, so it cannot turn into a stall.
    assert!(health.record().due.is_none());
    leader.close().await;
}

fn shared_config(dir: &std::path::Path, faults: &Arc<FaultStore>) -> ServerConfig {
    let mut config = ServerConfig::single_node(
        dir,
        crate::object::Config::Test(faults.clone(), "root".into()),
    );
    config.view_max_age = Duration::from_millis(100);
    config
}

pub(super) async fn channel(server: &Server) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(server.client_url())
        .unwrap()
        .connect()
        .await
        .unwrap()
}

fn put(key: &str, value: &str) -> PutRequest {
    PutRequest {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
        ..Default::default()
    }
}

/// Read every key with a `k` prefix, with a deadline, so a stopped member fails the test.
async fn keys(server: &Server, serializable: bool) -> Vec<(String, String)> {
    let request = RangeRequest {
        key: b"k".to_vec(),
        range_end: b"l".to_vec(),
        serializable,
        ..Default::default()
    };
    let mut kv = KvClient::new(channel(server).await);
    let reply = tokio::time::timeout(Duration::from_secs(5), kv.range(request))
        .await
        .expect("the read timed out")
        .expect("the read failed")
        .into_inner();
    reply
        .kvs
        .into_iter()
        .map(|kv| {
            let text = |bytes: Vec<u8>| String::from_utf8(bytes).unwrap();
            (text(kv.key), text(kv.value))
        })
        .collect()
}

pub(super) fn health(status: &Value) -> &Value {
    &status["shared_state"]["writer"]["health"]
}

/// The first writer open fails. The member stays up, serves both read kinds and writes from the
/// overlay, and publishes after the store recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_error_keeps_the_leader_serving_reads() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    faults.set(FAIL_STATE_PUT);
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(shared_config(dir.path(), &faults))
        .await
        .unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    let mut kv = KvClient::new(channel(&server).await);
    kv.put(put("k1", "v1")).await.unwrap();
    eventually("two failed opens", async || {
        health(&status(&server).await)["consecutive_failures"].as_u64() >= Some(2)
    })
    .await;
    let expected = vec![("k1".to_owned(), "v1".to_owned())];
    assert_eq!(keys(&server, true).await, expected);
    assert_eq!(keys(&server, false).await, expected);
    faults.set(0);
    eventually("the first checkpoint", async || {
        let status = status(&server).await;
        status["shared_state"]["applied_view_checkpoints"].as_u64() > Some(0)
            && health(&status)["marked"] == false
    })
    .await;
    kv.put(put("k2", "v2")).await.unwrap();
    assert_eq!(keys(&server, false).await.len(), 2);
    server.shutdown().await.unwrap();
}

/// Flushes fail in the middle of a term. The writer opens again and writes the overlay again.
/// After the store recovers, the base alone holds every write with its last value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_error_rewrites_the_overlay_into_the_next_checkpoint() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(shared_config(dir.path(), &faults))
        .await
        .unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    eventually("the first checkpoint", async || {
        status(&server).await["shared_state"]["applied_view_checkpoints"].as_u64() > Some(0)
    })
    .await;
    faults.set(FAIL_PUT);
    let mut kv = KvClient::new(channel(&server).await);
    let mut expected = std::collections::BTreeMap::new();
    for n in 0..20 {
        let (key, value) = (format!("k{:02}", n % 8), format!("v{n}"));
        kv.put(put(&key, &value)).await.unwrap();
        expected.insert(key, value);
    }
    eventually("two failed flushes", async || {
        health(&status(&server).await)["consecutive_failures"].as_u64() >= Some(2)
    })
    .await;
    faults.set(0);
    eventually("a base that holds every write", async || {
        let status = status(&server).await;
        status["shared_state"]["overlay_bytes"] == 0 && health(&status)["marked"] == false
    })
    .await;
    let expected: Vec<_> = expected.into_iter().collect();
    assert_eq!(keys(&server, true).await, expected);
    server.shutdown().await.unwrap();
}

/// The overlay limits hold while the writer fails: proposals fail at the soft limit, and apply
/// pauses at the hard limit. Both end after the store recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_overlay_limits_hold_while_the_writer_fails() {
    let limit = 64 * 1024;
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let mut config = shared_config(dir.path(), &faults);
    config.overlay_max_bytes = limit;
    let server = Server::start(config).await.unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    eventually("the first checkpoint", async || {
        status(&server).await["shared_state"]["applied_view_checkpoints"].as_u64() > Some(0)
    })
    .await;
    faults.set(FAIL_STATE_PUT);
    let channel = channel(&server).await;
    // Each write alone applies above twice the soft limit.
    let puts = (0..8_u8).map(|n| {
        let mut kv = KvClient::new(channel.clone());
        async move { kv.put(put(&format!("k{n}"), &"x".repeat(96 * 1024))).await }
    });
    let written = futures::future::join_all(puts).await;
    assert!(written.iter().any(Result::is_ok));
    eventually("the apply pause", async || {
        status(&server).await["shared_state"]["writer"]["paused"] == true
    })
    .await;
    let mut kv = KvClient::new(channel.clone());
    let rejected = kv.put(put("k-late", "x")).await.unwrap_err();
    assert!(rejected.message().contains("too many"), "{rejected}");
    // Apply alone can fill the overlay, so the pause can come before the first failed flush.
    eventually("a failed flush", async || {
        health(&status(&server).await)["consecutive_failures"].as_u64() > Some(0)
    })
    .await;
    faults.set(0);
    eventually("the resumed apply", async || {
        let status = status(&server).await;
        status["shared_state"]["writer"]["paused"] == false
            && status["shared_state"]["overlay_bytes"].as_u64() < Some(limit)
            && status["applied_index"] == status["commit_index"]
    })
    .await;
    kv.put(put("k-after", "x")).await.unwrap();
    server.shutdown().await.unwrap();
}

/// Three shared voters, each behind its own fault store over one object store. Each member's
/// wall clock is the real clock plus its offset in `clocks`.
pub(super) struct Cluster {
    servers: Vec<Server>,
    faults: Vec<Arc<FaultStore>>,
    pub(super) store: Arc<dyn ObjectStore>,
    clocks: Arc<[std::sync::atomic::AtomicI64; 4]>,
    _dir: tempfile::TempDir,
}

impl Cluster {
    pub(super) async fn start(tweak: impl Fn(&mut ServerConfig)) -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let clocks: Arc<[std::sync::atomic::AtomicI64; 4]> = Arc::default();
        let dir = tempfile::tempdir().unwrap();
        let peers: std::collections::BTreeMap<u64, String> =
            (1..=3).map(|id| (id, crate::server::test_port())).collect();
        let mut faults = vec![];
        let mut servers = vec![];
        for id in 1..=3 {
            let store = Arc::new(FaultStore::new(store.clone()));
            let mut config = shared_config(&dir.path().join(id.to_string()), &store);
            config.id = id;
            config.listen = peers[&id].parse().unwrap();
            config.peers = peers.clone();
            config.client_peers = peers.clone();
            config.view_max_age = Duration::from_millis(200);
            config.strict_reconfiguration = false;
            let (clocks, real) = (clocks.clone(), Clock::system(Duration::ZERO, None).now_ms);
            config.now_ms = Some(Arc::new(move || {
                real() + clocks[id as usize].load(std::sync::atomic::Ordering::SeqCst)
            }));
            tweak(&mut config);
            servers.push(Server::start(config).await.unwrap());
            faults.push(store);
        }
        for server in &servers {
            server.wait_ready(Duration::from_secs(20)).await.unwrap();
        }
        let cluster = Self {
            servers,
            faults,
            store,
            clocks,
            _dir: dir,
        };
        let leader = cluster.leader().await;
        eventually("the first checkpoint", async || {
            let status = status(cluster.member(leader)).await;
            status["shared_state"]["applied_view_checkpoints"].as_u64() > Some(0)
        })
        .await;
        cluster
    }

    /// Set the wall clock of member `id` to the real clock plus `offset_ms`.
    pub(super) fn skew(&self, id: u64, offset_ms: i64) {
        self.clocks[id as usize].store(offset_ms, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn member(&self, id: u64) -> &Server {
        &self.servers[id as usize - 1]
    }

    fn fault(&self, id: u64) -> &FaultStore {
        &self.faults[id as usize - 1]
    }

    /// The member that leads, as it reports itself.
    pub(super) async fn leading(&self) -> Option<u64> {
        for (n, server) in self.servers.iter().enumerate() {
            if status(server).await["role"] == "Leader" {
                return Some(n as u64 + 1);
            }
        }
        None
    }

    pub(super) async fn leader(&self) -> u64 {
        let mut leader = None;
        eventually("a leader", async || {
            leader = self.leading().await;
            leader.is_some()
        })
        .await;
        leader.unwrap()
    }

    /// Record the leaders for `time`, and return them in order without repeats in a row.
    pub(super) async fn leaders(&self, time: Duration) -> Vec<u64> {
        let mut leaders: Vec<u64> = vec![];
        let started = Instant::now();
        while started.elapsed() < time {
            if let Some(leader) = self.leading().await {
                if leaders.last() != Some(&leader) {
                    leaders.push(leader);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        leaders
    }

    /// Every member answers `/status` and a serializable read.
    pub(super) async fn all_up(&self) {
        for server in &self.servers {
            keys(server, true).await;
        }
    }

    pub(super) async fn put(&self, key: &str) -> Result<(), tonic::Status> {
        let leader = self.leader().await;
        let mut kv = KvClient::new(channel(self.member(leader)).await);
        kv.put(put(key, "v")).await.map(|_| ())
    }

    pub(super) async fn shutdown(self) {
        for server in self.servers {
            server.shutdown().await.unwrap();
        }
    }
}

/// Wait up to `time` for `check`. Writer backoffs can exceed the 30 s of `eventually`.
pub(super) async fn until(
    what: &str,
    time: Duration,
    mut check: impl std::ops::AsyncFnMut() -> bool,
) {
    let started = Instant::now();
    while !check().await {
        assert!(started.elapsed() < time, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until member `id` publishes as the leader and its writer has no mark.
pub(super) async fn publishes(cluster: &Cluster, id: u64) {
    until("a publication", Duration::from_secs(60), async || {
        let status = status(cluster.member(id)).await;
        status["shared_state"]["writer"]["announced"].as_u64() > Some(0)
            && health(&status)["marked"] == false
    })
    .await;
}

/// A fault on the leader's path only. Leadership moves once, to a healthy voter, which
/// publishes. The old leader stays up.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_fault_on_the_leader_moves_leadership_once() {
    let cluster = Cluster::start(|_| {}).await;
    let old = cluster.leader().await;
    cluster.fault(old).set(FAIL_STATE_PUT);
    cluster.put("k-fault").await.unwrap();
    let leaders = cluster.leaders(Duration::from_secs(25)).await;
    assert_eq!(leaders.first(), Some(&old), "{leaders:?}");
    assert_eq!(leaders.len(), 2, "{leaders:?}");
    let new = leaders[1];
    cluster.put("k-after").await.unwrap();
    publishes(&cluster, new).await;
    let marked = health(&status(cluster.member(old)).await)["marked"].clone();
    assert_eq!(marked, true);
    cluster.all_up().await;
    assert_eq!(cluster.leader().await, new);
    cluster.shutdown().await;
}

/// A write fault of the whole backend. No member stops, leadership moves at most once per
/// member, and proposals fail at the soft limit. After the fault clears the cluster recovers.
/// Then a later fault on the new leader alone moves leadership to a member whose mark is older
/// than that leader's publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_fault_on_every_member_moves_leadership_at_most_once_per_member() {
    let cluster = Cluster::start(|config| config.overlay_max_bytes = 64 * 1024).await;
    for id in 1..=3 {
        cluster.fault(id).set(FAIL_STATE_PUT);
    }
    let mut rejected = false;
    for n in 0..32 {
        if let Err(status) = cluster.put(&format!("k{n}-{}", "x".repeat(8 * 1024))).await {
            rejected |= status.message().contains("too many");
        }
    }
    assert!(rejected, "no proposal failed at the soft limit");
    let leaders = cluster.leaders(Duration::from_secs(40)).await;
    let mut seen = leaders.clone();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen.len(), leaders.len(), "a member led twice: {leaders:?}");
    cluster.all_up().await;
    for id in 1..=3 {
        cluster.fault(id).set(0);
    }
    let last = cluster.leader().await;
    until(
        "the recovered leader",
        Duration::from_secs(60),
        async || {
            let status = status(cluster.member(last)).await;
            health(&status)["marked"] == false && status["shared_state"]["overlay_bytes"] == 0
        },
    )
    .await;
    cluster.put("k-recovered").await.unwrap();
    // The recovered leader alone fails now. The others failed before its publication.
    cluster.fault(last).set(FAIL_STATE_PUT);
    cluster.put("k-again").await.unwrap();
    let mut moved = None;
    until(
        "a handoff after the second fault",
        Duration::from_secs(60),
        async || {
            moved = cluster.leading().await.filter(|leader| *leader != last);
            moved.is_some()
        },
    )
    .await;
    cluster.put("k-moved").await.unwrap();
    cluster.all_up().await;
    cluster.shutdown().await;
}

/// The writer of the new leader meets a manifest that it cannot read. The writer halts, the
/// member hands over once and stays up, and the next leader writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_data_error_halts_the_writer_and_hands_off_once() {
    let cluster = Cluster::start(|_| {}).await;
    let old = cluster.leader().await;
    let target = (1..=3).find(|id| *id != old).unwrap();
    cluster.fault(target).set(CORRUPT_MANIFEST);
    MaintenanceClient::new(channel(cluster.member(old)).await)
        .move_leader(MoveLeaderRequest { target_id: target })
        .await
        .unwrap();
    let leaders = cluster.leaders(Duration::from_secs(20)).await;
    // The leadership left the target once, and it did not come back.
    assert_eq!(leaders.first(), Some(&target), "{leaders:?}");
    assert_eq!(leaders.len(), 2, "{leaders:?}");
    let status = status(cluster.member(target)).await;
    assert_eq!(health(&status)["last_error_class"], "data");
    keys(cluster.member(target), true).await;
    cluster.put("k-after").await.unwrap();
    cluster.fault(target).set(0);
    cluster.shutdown().await;
}

/// The writer open of the new leader hangs. The watchdog moves leadership within its bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_hanging_open_hands_off_within_the_bound() {
    let cluster = Cluster::start(|_| {}).await;
    let old = cluster.leader().await;
    let target = (1..=3).find(|id| *id != old).unwrap();
    cluster.fault(target).set(STALL_STATE_PUT);
    MaintenanceClient::new(channel(cluster.member(old)).await)
        .move_leader(MoveLeaderRequest { target_id: target })
        .await
        .unwrap();
    let started = Instant::now();
    let mut moved = None;
    while started.elapsed() < STALL + Duration::from_secs(15) && moved.is_none() {
        moved = cluster.leading().await.filter(|leader| *leader != target);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(moved.is_some(), "the leadership did not move");
    assert!(started.elapsed() >= STALL - Duration::from_secs(1));
    let status = status(cluster.member(target)).await;
    assert_eq!(health(&status)["last_error_class"], "stalled");
    cluster.fault(target).set(0);
    cluster.put("k-after").await.unwrap();
    cluster.shutdown().await;
}

/// A flush that is due and hangs, while SlateDB still takes later writes in memory, is a fault
/// after `STALL`. One voter has no target, so it keeps the lead and serves writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stuck_due_flush_is_a_fault_while_writes_continue() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(shared_config(dir.path(), &faults))
        .await
        .unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    eventually("the first checkpoint", async || {
        status(&server).await["shared_state"]["applied_view_checkpoints"].as_u64() > Some(0)
    })
    .await;
    faults.set(STALL_STATE_PUT);
    let mut kv = KvClient::new(channel(&server).await);
    let started = Instant::now();
    let mut n = 0;
    // One voter has no target, so the node restarts the stalled writer. Its new open hangs too.
    while health(&status(&server).await)["last_error_class"] != "stalled" {
        assert!(
            started.elapsed() < STALL + Duration::from_secs(10),
            "no stall"
        );
        kv.put(put(&format!("k{n}"), "v")).await.unwrap();
        n += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(started.elapsed() >= STALL);
    let written = status(&server).await["shared_state"]["writer"]["written"].as_u64();
    assert!(written > Some(2), "{written:?}");
    assert_eq!(health(&status(&server).await)["marked"], true);
    faults.set(0);
    until("the recovered writer", STALL, async || {
        let status = status(&server).await;
        health(&status)["stalled"] == false && health(&status)["marked"] == false
    })
    .await;
    server.shutdown().await.unwrap();
}

/// Items 4 and 6 of the review: an SST put of the writer hangs. One voter has no handoff
/// target, so the node drops the stalled step after `STALL`, and the writer opens again without
/// waiting for the old close. The new writer publishes while the old put still waits. When the
/// old put returns late, the old writer's close ends, and the new writer stays healthy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_upload_restarts_the_writer_within_the_bound() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(shared_config(dir.path(), &faults))
        .await
        .unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    let checkpoints = async || {
        status(&server).await["shared_state"]["applied_view_checkpoints"]
            .as_u64()
            .unwrap()
    };
    eventually("the first checkpoint", async || checkpoints().await > 0).await;
    let before = checkpoints().await;
    faults.set(PAUSE_PUT);
    let mut kv = KvClient::new(channel(&server).await);
    kv.put(put("k-hung", "v")).await.unwrap();
    let entered = tokio::time::timeout(Duration::from_secs(10), faults.entered.acquire());
    entered.await.unwrap().unwrap().forget();
    let started = Instant::now();
    kv.put(put("k-later", "v")).await.unwrap();
    until(
        "a publication after the hang",
        STALL + Duration::from_secs(20),
        async || {
            let status = status(&server).await;
            checkpoints().await > before && status["shared_state"]["overlay_bytes"] == 0
        },
    )
    .await;
    assert!(started.elapsed() >= STALL - Duration::from_secs(1));
    let restarted = status(&server).await;
    assert_eq!(health(&restarted)["last_error_class"], "stalled");
    // The old put still waits, so the old close does too.
    assert_eq!(health(&restarted)["pending_closes"], 1);
    let expected = ["k-hung", "k-later"].map(|key| (key.to_owned(), "v".to_owned()));
    assert_eq!(keys(&server, true).await, expected);
    // The put returns late. The old writer is fenced, and its close ends.
    faults.release.add_permits(1);
    eventually("the old close", async || {
        health(&status(&server).await)["pending_closes"] == 0
    })
    .await;
    kv.put(put("k-after", "v")).await.unwrap();
    eventually("a later publication", async || {
        let status = status(&server).await;
        status["shared_state"]["overlay_bytes"] == 0 && health(&status)["marked"] == false
    })
    .await;
    server.shutdown().await.unwrap();
}

/// Liveness (a): one voter whose writes fail for more than 70 s, past the 30 s backoff cap.
/// After the store recovers, the writer publishes within one backoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_voter_publishes_within_a_backoff_after_a_long_outage() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(shared_config(dir.path(), &faults))
        .await
        .unwrap();
    server.wait_ready(Duration::from_secs(15)).await.unwrap();
    eventually("the first checkpoint", async || {
        status(&server).await["shared_state"]["applied_view_checkpoints"].as_u64() > Some(0)
    })
    .await;
    faults.set(FAIL_STATE_PUT);
    let mut kv = KvClient::new(channel(&server).await);
    kv.put(put("k-outage", "v")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(75)).await;
    let failing = status(&server).await;
    assert!(health(&failing)["consecutive_failures"].as_u64() >= Some(6));
    // The 30 s waits of the backoff are no stall.
    assert_eq!(health(&failing)["stalled"], false);
    assert_eq!(
        health(&failing)["faults"],
        health(&failing)["consecutive_failures"]
    );
    // The store recovers with the latency of S3, so each step spans several ticks.
    faults.set_delay(Duration::from_millis(150));
    faults.set(DELAY_SHARED);
    until(
        "a publication after the outage",
        RETRY_CAP + Duration::from_secs(10),
        async || {
            let status = status(&server).await;
            status["shared_state"]["overlay_bytes"] == 0 && health(&status)["marked"] == false
        },
    )
    .await;
    server.shutdown().await.unwrap();
}

/// Liveness (b): every voter fails long enough to lead once and get a mark. The last leader has
/// no target. After the store recovers, it publishes within one backoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_last_leader_publishes_within_a_backoff_after_a_long_outage() {
    let cluster = Cluster::start(|_| {}).await;
    for id in 1..=3 {
        cluster.fault(id).set(FAIL_STATE_PUT);
    }
    cluster.put("k-outage").await.unwrap();
    tokio::time::sleep(Duration::from_secs(90)).await;
    let last = cluster.leader().await;
    let failing = status(cluster.member(last)).await;
    let failures = health(&failing)["consecutive_failures"].as_u64();
    assert!(failures >= Some(6), "{failures:?}");
    // The 30 s waits of the backoff are no stall.
    assert_eq!(health(&failing)["stalled"], false);
    // The store recovers with the latency of S3, so each step spans several ticks.
    for id in 1..=3 {
        cluster.fault(id).set_delay(Duration::from_millis(150));
        cluster.fault(id).set(DELAY_SHARED);
    }
    until(
        "a publication after the outage",
        RETRY_CAP + Duration::from_secs(10),
        async || {
            let leader = cluster.leader().await;
            let status = status(cluster.member(leader)).await;
            status["shared_state"]["overlay_bytes"] == 0 && health(&status)["marked"] == false
        },
    )
    .await;
    cluster.all_up().await;
    cluster.shutdown().await;
}

/// A publication request for a batch that the materializer has not written stays pending. Each
/// pass that serves nothing keeps the wait of the request, so the watchdog fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_above_the_written_batch_keeps_its_timer() {
    let store = Arc::new(InMemory::new());
    let mut leader = Leader::open(store, Duration::from_millis(100), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    // Answer each announcement, so the publisher keeps making passes.
    let (_, idle) = mpsc::channel(1);
    let mut announced = std::mem::replace(&mut leader.announced, idle);
    let passes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let answered = passes.clone();
    tokio::spawn(async move {
        while let Some(event) = announced.recv().await {
            if let Event::ViewCheckpoint(_, _, Some(reply)) = event {
                answered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = reply.send(true);
            }
        }
    });
    let requested = Instant::now();
    let through = leader.state.applied + 1;
    let writer = &leader.writer;
    writer.progress().request(through);
    until("the stall", STALL + Duration::from_secs(5), async || {
        // Wake the publisher, as later writes and requests do.
        writer.progress().wake.notify_one();
        writer.health().check(writer.waiting())
    })
    .await;
    assert!(requested.elapsed() >= STALL);
    // The publisher kept announcing in that time, so its passes did not reset the wait.
    assert!(passes.load(std::sync::atomic::Ordering::Relaxed) > 5);
    leader.close().await;
}

/// Opens that lose the manifest race again and again make no durable progress, also when each
/// bootstrap check succeeds. The open step keeps its timer, so the watchdog fires, and a
/// restart counts as a fault.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_open_conflicts_raise_the_stall() {
    let store = Arc::new(InMemory::new());
    let mut leader = Leader::open(store, Duration::from_millis(100), clock()).await;
    let conflict = || slatedb::Error::data("version already exists".into()).into();
    *leader.handles.source.fail_writers.lock().unwrap() = Some(conflict);
    leader.lead(2);
    let health = leader.writer().health().clone();
    let started = Instant::now();
    until("the stall", STALL + Duration::from_secs(5), async || {
        health.check(None)
    })
    .await;
    assert!(started.elapsed() >= STALL - Duration::from_secs(1));
    assert!(leader.writer().reopens() > 10);
    health.restart();
    eventually("the restart", async || health.consecutive() > 0).await;
    *leader.handles.source.fail_writers.lock().unwrap() = None;
    leader.covered(1, Duration::from_secs(10)).await;
    leader.close().await;
}

/// A request is overdue when the writer opens again. The new writer gets a whole interval to
/// reach it. A writer that still cannot serve it in that interval trips the watchdog, and the
/// request is served once its batch arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_writer_gets_a_fresh_interval_for_a_request() {
    let store = Arc::new(InMemory::new());
    let mut leader = Leader::open(store, Duration::from_millis(100), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    let through = leader.state.applied + 1;
    let writer = &leader.writer;
    writer.progress().request(through);
    writer.progress().inner.lock().unwrap().requested =
        Some(Instant::now() - STALL - Duration::from_secs(1));
    let old = writer.open().unwrap().db;
    // The overdue request is a stall. Each tick of the node checks the stall and asks for the
    // restart, as for a stall with no target, until the task drops its step.
    let before = writer.health().consecutive();
    eventually("the restart", async || {
        let restarted = writer.health().consecutive() > before;
        if !restarted {
            writer.health().check(writer.waiting());
            writer.health().restart();
        }
        restarted
    })
    .await;
    let reopened = async || {
        let open = writer.open();
        open.is_some_and(|open| !Arc::ptr_eq(&open.db, &old))
    };
    eventually("the replacement writer", reopened).await;
    assert!(!writer.health().check(writer.waiting()));
    let started = Instant::now();
    until("the stall", STALL + Duration::from_secs(5), async || {
        writer.progress().wake.notify_one();
        writer.health().check(writer.waiting())
    })
    .await;
    assert!(started.elapsed() >= STALL - Duration::from_secs(1));
    assert_eq!(leader.put("late").await, through);
    leader.covered(through, Duration::from_secs(10)).await;
    let writer = &leader.writer;
    eventually("the served request", async || {
        writer.progress().requested().is_none()
    })
    .await;
    leader.close().await;
}

/// The requested index is in a stranded checkpoint. The request stays until the check finds
/// that checkpoint, and its announcement serves the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stranded_checkpoint_serves_a_request() {
    use super::super::tests::{stranded_checkpoint_on, STRANDED_AGE};
    let (state, handles, events, mut announcements, lost) =
        stranded_checkpoint_on(Arc::new(InMemory::new())).await;
    let mut writer = new_writer(&handles, STRANDED_AGE, clock());
    writer.request_publication(1);
    writer.sync(true, 3, &events);
    let announced = tokio::time::timeout(Duration::from_secs(10), announcements.0.recv());
    let Some(Event::ViewCheckpoint(id, _, Some(reply))) = announced.await.unwrap() else {
        panic!("no announcement");
    };
    assert_eq!(id, lost);
    assert!(
        writer.progress().requested().is_some(),
        "the check dropped the request"
    );
    reply.send(true).unwrap();
    eventually("the served request", async || {
        writer.progress().requested().is_none()
    })
    .await;
    writer.sync(false, 3, &events);
    writer.stop().await;
    state.close().await.unwrap();
}

/// The check for a stranded checkpoint hangs on a manifest read while a request waits. The read
/// counts against the request, so the watchdog fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_stranded_check_trips_the_request_timer() {
    use super::super::tests::{adopt_announced, stranded_checkpoint_on, STRANDED_AGE};
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    let (mut state, handles, events, mut announcements, lost) =
        stranded_checkpoint_on(faults.clone()).await;
    let mut writer = new_writer(&handles, STRANDED_AGE, clock());
    writer.sync(true, 3, &events);
    assert_eq!(
        announcements.next(Duration::from_secs(10), true).await,
        lost
    );
    adopt_announced(&mut state, lost).await;
    // The publisher sees the announcement applied at its next check.
    tokio::time::sleep(RETRY_MAX + Duration::from_millis(500)).await;
    faults.set(STALL_MANIFEST_READ);
    let requested = Instant::now();
    writer.request_publication(1);
    until("the stall", STALL + Duration::from_secs(5), async || {
        writer.health().check(writer.waiting())
    })
    .await;
    assert!(requested.elapsed() >= STALL - Duration::from_secs(1));
    faults.set(0);
    writer.sync(false, 3, &events);
    writer.stop().await;
    state.close().await.unwrap();
}

/// One pass serves both a checkpoint request and a Defragment republish. After it, neither the
/// demand nor its wait comes back.
#[test]
fn a_pass_that_serves_a_request_and_a_republish_keeps_neither() {
    let progress = Progress::new(5);
    progress.request(3);
    progress.republish();
    let (flush, through) = progress.take_flush(Duration::from_secs(60));
    assert!(flush);
    let republish = progress.take_republish();
    assert!(republish);
    let demand = Demand {
        progress: &progress,
        through,
        republish,
    };
    demand.served();
    let inner = progress.inner.lock().unwrap();
    assert!(inner.publish_through.is_none() && !inner.republish);
    assert!(inner.requested.is_none() && inner.republished.is_none());
}

/// The check for a stranded checkpoint fails again and again while a request waits. The
/// publisher retries it at the backoff, not in a tight loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_stranded_check_retries_at_the_backoff() {
    use super::super::tests::{adopt_announced, stranded_checkpoint_on, STRANDED_AGE};
    use std::sync::atomic::Ordering;
    let (mut state, handles, events, mut announcements, lost) =
        stranded_checkpoint_on(Arc::new(InMemory::new())).await;
    let mut writer = new_writer(&handles, STRANDED_AGE, clock());
    writer.sync(true, 3, &events);
    assert_eq!(
        announcements.next(Duration::from_secs(10), true).await,
        lost
    );
    adopt_announced(&mut state, lost).await;
    tokio::time::sleep(RETRY_MAX + Duration::from_millis(500)).await;
    let failures = writer.failures();
    failures.store(10_000, Ordering::Relaxed);
    writer.request_publication(1);
    tokio::time::sleep(Duration::from_secs(3)).await;
    // The backoff from 100 ms doubles to 2 s, so about five checks fit in 3 s.
    let checks = 10_000 - failures.load(Ordering::Relaxed);
    assert!((1..=8).contains(&checks), "{checks} checks in 3 s");
    failures.store(0, Ordering::Relaxed);
    writer.sync(false, 3, &events);
    writer.stop().await;
    state.close().await.unwrap();
}

/// A checkpoint request waits above the written batch while Defragment republishes publish
/// again and again. A republish serves only itself, so the request keeps its wait and the
/// watchdog fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn republishes_do_not_hide_a_waiting_request() {
    let store = Arc::new(InMemory::new());
    let mut leader = Leader::open(store, Duration::from_millis(100), clock()).await;
    leader.lead(2);
    leader.covered(1, Duration::from_secs(10)).await;
    let (_, idle) = mpsc::channel(1);
    let mut announced = std::mem::replace(&mut leader.announced, idle);
    let publications = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let answered = publications.clone();
    tokio::spawn(async move {
        while let Some(event) = announced.recv().await {
            if let Event::ViewCheckpoint(_, _, Some(reply)) = event {
                answered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = reply.send(true);
            }
        }
    });
    let requested = Instant::now();
    let through = leader.state.applied + 1;
    let writer = &leader.writer;
    writer.progress().request(through);
    until("the stall", STALL + Duration::from_secs(5), async || {
        writer.progress().republish();
        tokio::time::sleep(Duration::from_millis(200)).await;
        writer.health().check(writer.waiting())
    })
    .await;
    assert!(requested.elapsed() >= STALL);
    assert!(publications.load(std::sync::atomic::Ordering::Relaxed) > 20);
    leader.close().await;
}
