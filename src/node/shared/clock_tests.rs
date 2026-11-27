//! The clock checks of the shared writer: the samples, the bound, and the SlateDB clock.

use super::*;

/// The process of each test peer.
const PROCESS: u64 = 7;
/// The wall clock at the start of each test, near the real time.
const START: i64 = 1_800_000_000_000;

/// The wall and monotonic clocks of a test leader.
#[derive(Clone, Default)]
struct Time {
    wall: Arc<AtomicI64>,
    mono: Arc<AtomicI64>,
}

impl Time {
    /// Move both clocks forward.
    fn advance(&self, ms: i64) {
        self.wall.fetch_add(ms, Ordering::SeqCst);
        self.mono.fetch_add(ms, Ordering::SeqCst);
    }

    /// Step only the wall clock, as an operator or NTP does.
    fn step(&self, ms: i64) {
        self.wall.fetch_add(ms, Ordering::SeqCst);
    }

    fn wall(&self) -> i64 {
        self.wall.load(Ordering::SeqCst)
    }

    fn mono(&self) -> i64 {
        self.mono.load(Ordering::SeqCst)
    }
}

/// A leader clock with a 5 s skew limit, and the voters `voters`. Member 1 leads in term 2.
fn leader(voters: &[u64]) -> (Clock, Time) {
    skewed(voters, Duration::from_secs(5))
}

/// A leader clock with the skew limit `max_skew`.
fn skewed(voters: &[u64], max_skew: Duration) -> (Clock, Time) {
    let time = Time::default();
    time.wall.store(START, Ordering::SeqCst);
    let mut clock = Clock::system(max_skew, None);
    let (wall, mono) = (time.wall.clone(), time.mono.clone());
    clock.now_ms = Arc::new(move || wall.load(Ordering::SeqCst));
    clock.mono_ms = Arc::new(move || mono.load(Ordering::SeqCst));
    configure(&clock, 2, voters, PROCESS);
    (clock, time)
}

fn configure(clock: &Clock, term: u64, voters: &[u64], process: u64) {
    let voters = voters.iter().map(|id| (*id, Some(process))).collect();
    clock.configure(term, 1, voters);
}

/// The reply of `peer`, whose clock is `offset` ms from the leader's wall clock at the send.
/// The probe waits `rtt` ms for the reply, and the peer reads its clock at the send.
fn reply(clock: &Clock, time: &Time, peer: u64, offset: i64, rtt: i64) -> bool {
    let probe = clock.probe();
    let sent = time.mono();
    let clock_ms = time.wall() + offset;
    time.advance(rtt);
    let reply = ProbeReply {
        nonce: probe.nonce,
        process: PROCESS,
        clock_ms,
    };
    clock.sample(peer, probe, reply, sent, time.mono())
}

/// The reply of a probe that the leader sent earlier, with its window.
fn late(clock: &Clock, peer: u64, probe: Probe, clock_ms: i64, window: (i64, i64)) -> bool {
    let reply = ProbeReply {
        nonce: probe.nonce,
        process: PROCESS,
        clock_ms,
    };
    clock.sample(peer, probe, reply, window.0, window.1)
}

fn ahead(verdict: Verdict) -> Option<i64> {
    match verdict {
        Verdict::Ahead { by, .. } => Some(by),
        _ => None,
    }
}

/// A sample counts only for the term, the voters, and the voter process of its probe.
#[test]
fn a_reply_of_an_earlier_term_process_or_voter_set_gives_no_sample() {
    let (clock, time) = leader(&[1, 2, 3]);
    let probe = clock.probe();
    configure(&clock, 3, &[1, 2, 3], PROCESS);
    assert!(!late(&clock, 2, probe, START, (0, 0)), "an earlier term");
    let probe = clock.probe();
    configure(&clock, 3, &[1, 2, 3, 4], PROCESS);
    assert!(!late(&clock, 2, probe, START, (0, 0)), "another voter set");
    configure(&clock, 3, &[1, 2, 3, 4], PROCESS + 1);
    assert!(!reply(&clock, &time, 2, 0, 1), "another process");
    // A sample of the voter's process holds until the voter restarts.
    configure(&clock, 3, &[1, 2, 3, 4], PROCESS);
    assert!(reply(&clock, &time, 2, 0, 1));
    assert!(reply(&clock, &time, 3, 0, 1));
    assert!(matches!(clock.verdict(), Verdict::Admit { .. }));
    let restarted = [1, 2, 3, 4].map(|id| (id, Some(PROCESS + u64::from(id == 2))));
    clock.configure(3, 1, restarted.into_iter().collect());
    assert_eq!(clock.verdict(), Verdict::Waiting);
    // A member is never its own peer.
    assert!(!reply(&clock, &time, 1, 0, 1));
}

/// Item 1 of the last review: a delayed reply from before a peer's backward step comes after
/// the reply to a later probe. The later sample holds, so the bound does not use the old fast
/// clock, and the fast leader stays ahead.
#[test]
fn a_delayed_older_reply_never_replaces_a_newer_one() {
    let (clock, time) = leader(&[1, 2, 3]);
    let early = clock.probe();
    let sent = time.mono();
    // The peer clock is 10 s fast when it replies to the early probe. Then it steps back.
    let fast = time.wall() + 10_000;
    time.advance(5);
    assert!(reply(&clock, &time, 2, 0, 5));
    assert!(reply(&clock, &time, 3, 0, 0));
    assert!(!late(&clock, 2, early, fast, (sent, time.mono())));
    // The leader is 10 s ahead of both peers.
    time.step(10_000);
    assert!(ahead(clock.verdict()).is_some_and(|by| by > 4_000));
}

/// Three voters with one missing: the missing voter could agree with the fast leader, so the
/// skew is uncertain, not measured. The tick stays at the cap of the correct peer, and no alarm
/// rises. The reply of the third voter then proves the skew.
#[test]
fn a_missing_voter_makes_a_fast_leader_uncertain() {
    let (clock, time) = leader(&[1, 2, 3]);
    assert_eq!(clock.verdict(), Verdict::Waiting);
    assert!(reply(&clock, &time, 2, -10_000, 0));
    let Verdict::Uncertain(bound) = clock.verdict() else {
        panic!("a missing voter did not make the skew uncertain");
    };
    assert_eq!(bound.cap, time.wall() - 5_000);
    clock.observe();
    assert_eq!(clock.alarm_name(), None);
    assert_eq!(clock.status()["uncertain"], true);
    assert_eq!(clock.status()["waiting"], true);
    assert!(reply(&clock, &time, 3, -10_000, 0));
    assert_eq!(ahead(clock.verdict()), Some(5_000));
    clock.observe();
    assert_eq!(clock.alarm_name(), Some("ahead"));
    assert_eq!(clock.status()["uncertain"], false);
    // The skew ends with a correct leader clock.
    time.step(-10_000);
    clock.observe();
    assert_eq!(clock.alarm_name(), None);
}

/// A correct leader, a peer whose reply is 10 s behind, and a missing correct peer. The
/// missing peer and the leader can form a majority, so the leader is uncertain and admits at
/// the cap. With five voters and one missing, three peers 10 s behind prove the skew.
#[tokio::test]
async fn partial_replies_prove_a_skew_only_when_no_majority_can_support_it() {
    let (clock, time) = leader(&[1, 2, 3]);
    assert!(reply(&clock, &time, 2, -10_000, 0));
    assert!(matches!(clock.verdict(), Verdict::Uncertain(_)));
    let gate = clock.gate().await;
    gate.admit().await.done();
    assert!(read(&gate) <= time.wall() - 5_000);
    assert_eq!(clock.alarm_name(), None);
    assert!(clock.uncertain());

    let (clock, time) = leader(&[1, 2, 3, 4, 5]);
    for peer in 2..=4 {
        assert!(reply(&clock, &time, peer, -10_000, 0));
    }
    assert_eq!(ahead(clock.verdict()), Some(5_000));
}

/// The bound is the `m`-th largest clock of all voters, with the leader's own clock read now.
/// Clocks with a 5 ms limit: the leader at 40 and peers at 100 and 40 give the bound 45, so
/// a retained tick of 100 waits. Peers at 100 and 100 give 105, and the tick of 100 is
/// admitted. A fast leader at 110 with peers at 100 and 40 has the bound 105 and is ahead.
#[tokio::test]
async fn the_bound_is_the_majority_clock_of_all_voters() {
    let case = |leader_ms: i64, peers: [i64; 2]| {
        let (clock, time) = skewed(&[1, 2, 3], Duration::from_millis(5));
        time.wall.store(START + leader_ms, Ordering::SeqCst);
        for (peer, peer_ms) in [2, 3].into_iter().zip(peers) {
            assert!(late(&clock, peer, clock.probe(), START + peer_ms, (0, 0)));
        }
        clock
    };
    let clock = case(40, [100, 40]);
    assert_eq!(
        clock.verdict(),
        Verdict::Admit(Bound {
            cap: START + 45,
            real: START + 40
        })
    );
    let gate = clock.gate().await;
    gate.floor(START + 100);
    let waited = tokio::time::timeout(Duration::from_millis(300), gate.admit()).await;
    assert!(
        waited.is_err(),
        "a retained tick above the bound was admitted"
    );
    assert!(clock.waiting());
    assert_eq!(clock.alarm_name(), None);

    let clock = case(40, [100, 100]);
    let gate = clock.gate().await;
    gate.floor(START + 100);
    let admitted = gate.admit().await;
    assert_eq!(read(&gate), START + 100);
    admitted.done();

    let clock = case(110, [100, 40]);
    let Verdict::Ahead { by, bound } = clock.verdict() else {
        panic!("a fast leader was not ahead");
    };
    assert_eq!((by, bound.cap), (5, START + 105));
}

/// Two fast voters of three are a majority. Neither is ahead.
#[test]
fn two_fast_voters_of_three_are_a_majority() {
    let (clock, time) = leader(&[1, 2, 3]);
    time.step(10_000);
    assert!(reply(&clock, &time, 2, 0, 2));
    assert!(reply(&clock, &time, 3, -10_000, 2));
    let Verdict::Admit(Bound { cap, real }) = clock.verdict() else {
        panic!("a fast majority was ahead");
    };
    assert!(real <= cap);
}

/// Slow replies widen each window. A correct leader clock is above the low edge, but never above
/// the high edge, so it is never ahead. Its cap stays at the low edge.
#[test]
fn slow_replies_never_make_a_correct_leader_ahead() {
    let (clock, time) = leader(&[1, 2, 3, 4, 5]);
    // Each peer reads its correct clock at the send, and each reply takes 8 s.
    let probes: Vec<_> = (2..=5).map(|peer| (peer, clock.probe())).collect();
    let (sent, clock_ms) = (time.mono(), time.wall());
    time.advance(8_000);
    for (peer, probe) in probes {
        assert!(late(&clock, peer, probe, clock_ms, (sent, time.mono())));
    }
    let Verdict::Admit(Bound { cap, real }) = clock.verdict() else {
        panic!("slow replies made a correct leader ahead");
    };
    assert!(cap < real, "the low edge must cap the tick: {cap} {real}");
    // Without any reply in four seconds the leader waits.
    time.advance(FRESH_MS);
    assert_eq!(clock.verdict(), Verdict::Waiting);
}

/// One voter, or a member that does not lead, has no cap.
#[test]
fn one_voter_has_no_cap() {
    let (clock, time) = leader(&[1]);
    time.step(3_600_000);
    assert!(matches!(clock.verdict(), Verdict::Single(_)));
    let (clock, _) = leader(&[1, 2, 3]);
    clock.stand_down();
    assert_eq!(clock.verdict(), Verdict::Off);
}

/// The tick that the SlateDB clock of `gate` returns now.
fn read(gate: &Gate) -> i64 {
    gate.now().timestamp_millis()
}

/// A shared `Db` on `store` with `gate` as its clock.
async fn open(
    store: &Arc<dyn slatedb::object_store::ObjectStore>,
    gate: &Arc<Gate>,
) -> Arc<slatedb::Db> {
    let db = slatedb::Db::builder("state", store.clone())
        .with_settings(crate::state::slatedb_settings())
        .with_system_clock(gate.clone())
        .build()
        .await
        .unwrap();
    Arc::new(db)
}

fn batch(seq: u64) -> Arc<crate::WriteBatch> {
    let mut batch = crate::WriteBatch::new();
    batch.put(format!("key-{seq}"), "value");
    batch.put(b"meta/applied", seq.to_be_bytes());
    Arc::new(batch)
}

/// A wall clock step of +10 s just before a write, with no new sample. The leader reads its
/// own clock at each batch, so the next batch gets the cap: at most the skew limit above the
/// voter majority. The `ahead` diagnostic shows the step.
#[tokio::test]
async fn a_step_ahead_before_a_write_is_capped_at_the_next_write() {
    let (clock, time) = leader(&[1, 2, 3]);
    assert!(reply(&clock, &time, 2, 0, 2));
    assert!(reply(&clock, &time, 3, 0, 2));
    let gate = clock.gate().await;
    let admitted = gate.admit().await;
    assert_eq!(read(&gate), time.wall());
    admitted.done();
    time.step(10_000);
    let Verdict::Ahead { bound, .. } = clock.verdict() else {
        panic!("a fast leader was not ahead");
    };
    let admitted = gate.admit().await;
    assert_eq!(read(&gate), bound.cap);
    assert!(bound.cap <= time.wall() - 10_000 + 5_000);
    admitted.done();
    assert_eq!(clock.alarm_name(), Some("ahead"));
}

/// Slow replies widen each window, so the low edge of the bound falls below the floor of the
/// writer: an earlier writer wrote a tick 1 s ahead of this correct clock. The leader waits.
/// A fast reply ends the wait.
#[tokio::test]
async fn slow_replies_make_a_correct_leader_wait() {
    let (clock, time) = leader(&[1, 2, 3]);
    // Each peer reads its correct clock at the send, and each reply takes 8 s.
    let probes = [(2, clock.probe()), (3, clock.probe())];
    let (sent, clock_ms) = (time.mono(), time.wall());
    time.advance(8_000);
    for (peer, probe) in probes {
        assert!(late(&clock, peer, probe, clock_ms, (sent, time.mono())));
    }
    let gate = clock.gate().await;
    let last = time.wall() + 1_000;
    gate.floor(last);
    let waited = tokio::time::timeout(Duration::from_millis(300), gate.admit()).await;
    assert!(waited.is_err(), "the leader did not wait");
    assert!(clock.waiting());
    assert_eq!(clock.alarm_name(), None);
    assert_eq!(read(&gate), last);
    let admit = tokio::spawn({
        let gate = gate.clone();
        async move { gate.admit().await.done() }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(reply(&clock, &time, 2, 0, 1));
    tokio::time::timeout(Duration::from_secs(1), admit)
        .await
        .expect("a fast reply did not end the wait")
        .unwrap();
    assert!(!clock.waiting());
    assert_eq!(read(&gate), last);
}

/// Write `seq` through the materializer on `db` and wait for it.
async fn write(db: &Arc<slatedb::Db>, gate: &Arc<Gate>, seq: u64) {
    let mut overlay = crate::state::overlay::Overlay::new(seq - 1);
    let progress = Arc::new(Progress::new(seq - 1));
    let queue = overlay.queue();
    let task = tokio::spawn(materialize(
        db.clone(),
        queue,
        progress.clone(),
        gate.clone(),
    ));
    overlay.push(seq, batch(seq));
    progress.wait_written(seq).await.unwrap();
    task.abort();
}

/// The admission of a batch fixes its tick. The batch waits after its admission, while the
/// leader clock jumps, the samples expire, and a newer sample allows a later tick. SlateDB still
/// stamps the batch with the admitted tick, and the reopened manifest holds no higher tick. The
/// bound also falls first: it allowed 105, the first batch wrote 90, fresh samples allow 95,
/// and the second batch is admitted at 94. A next leader with a correct clock writes at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admitted_tick_holds_until_slatedb_writes_the_batch() {
    let store: Arc<dyn slatedb::object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let (clock, time) = skewed(&[1, 2, 3], Duration::from_millis(5));
    let at = |ms: i64| time.wall.store(START + ms, Ordering::SeqCst);
    at(90);
    for peer in [2, 3] {
        assert!(late(&clock, peer, clock.probe(), START + 100, (0, 0)));
    }
    let gate = clock.gate().await;
    let db = open(&store, &gate).await;
    gate.floor(floor(&db.manifest()));
    write(&db, &gate, 1).await;
    db.flush().await.unwrap();
    assert_eq!(db.manifest().last_l0_clock_tick(), START + 90);

    for peer in [2, 3] {
        assert!(late(&clock, peer, clock.probe(), START + 90, (0, 0)));
    }
    at(94);
    let reached = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *gate.hold.lock().unwrap() = Some((reached.clone(), resume.clone()));
    let held = tokio::spawn({
        let (db, gate) = (db.clone(), gate.clone());
        async move { write(&db, &gate, 2).await }
    });
    reached.notified().await;
    at(200);
    time.mono.fetch_add(FRESH_MS, Ordering::SeqCst);
    let now = time.mono();
    assert!(late(&clock, 3, clock.probe(), START + 300, (now, now)));
    assert_eq!(read(&gate), START + 94);
    *gate.hold.lock().unwrap() = None;
    resume.notify_one();
    held.await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // The next leader's clock agrees with the newest samples.
    let (next, time) = skewed(&[1, 2, 3], Duration::from_millis(5));
    time.wall.store(START + 300, Ordering::SeqCst);
    assert!(reply(&next, &time, 2, 0, 0));
    let gate = next.gate().await;
    let db = open(&store, &gate).await;
    let manifest = db.manifest();
    assert_eq!(manifest.last_l0_clock_tick(), START + 94);
    let started = Instant::now();
    assert!(next.pass(floor(&manifest)).await);
    gate.floor(floor(&manifest));
    write(&db, &gate, 3).await;
    assert!(started.elapsed() < Duration::from_secs(1));
    db.flush().await.unwrap();
    assert_eq!(db.manifest().last_l0_clock_tick(), START + 300);
    db.close().await.unwrap();
}

/// SlateDB takes SST IDs from its clock and refuses an L0 SST ID below the newest one. A new
/// writer whose clock is behind that ID time, within the skew limit, waits until its clock
/// passes it. Otherwise its first flush fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_writer_waits_for_the_newest_l0_sst_id() {
    let store: Arc<dyn slatedb::object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let (clock, time) = leader(&[1, 2, 3]);
    assert!(reply(&clock, &time, 2, 0, 0));
    let gate = clock.gate().await;
    let db = open(&store, &gate).await;
    write(&db, &gate, 1).await;
    // The flush takes its SST ID 50 ms after the batch.
    time.advance(50);
    assert!(reply(&clock, &time, 2, 0, 0));
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // The next leader and its peers are 20 ms behind the SST ID.
    let (next, time) = leader(&[1, 2, 3]);
    time.wall.store(START + 30, Ordering::SeqCst);
    assert!(reply(&next, &time, 2, 0, 0));
    let gate = next.gate().await;
    let db = open(&store, &gate).await;
    let manifest = db.manifest();
    assert_eq!(manifest.last_l0_clock_tick(), START);
    assert_eq!(floor(&manifest), START + 50);
    let passed = tokio::spawn({
        let (next, tick) = (next.clone(), floor(&manifest));
        async move { next.pass(tick).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !passed.is_finished(),
        "the writer did not wait for the SST ID"
    );
    time.advance(21);
    assert!(reply(&next, &time, 2, 0, 0));
    assert!(passed.await.unwrap());
    write(&db, &gate, 2).await;
    db.flush().await.unwrap();
    db.close().await.unwrap();
}

/// One voter has no cap. Its writer starts at once, and SlateDB reads its default
/// clock, also with a local clock one hour ahead.
#[tokio::test]
async fn one_voter_keeps_the_slatedb_clock() {
    let (clock, time) = leader(&[1]);
    time.step(3_600_000);
    let gate = tokio::time::timeout(Duration::from_secs(1), clock.gate())
        .await
        .expect("one voter waited for samples");
    let admitted = gate.admit().await;
    let real = DefaultSystemClock::new().now().timestamp_millis();
    assert!((read(&gate) - real).abs() < 1_000);
    admitted.done();
    assert!(!clock.waiting());
    assert_eq!(clock.alarm_name(), None);
}

/// An idle leader with fresh samples. Its SlateDB clock follows the wall clock, so a
/// checkpoint expires and GC removes it. A backward step of the wall clock never moves the
/// SlateDB clock back, and a ticker follows the monotonic clock across a forward step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_leader_clock_advances_and_never_steps_back() {
    let store: Arc<dyn slatedb::object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let (clock, time) = leader(&[1, 2, 3]);
    assert!(reply(&clock, &time, 2, 0, 1));
    let gate = clock.gate().await;
    let db = open(&store, &gate).await;
    let options = CheckpointOptions {
        lifetime: Some(Duration::from_secs(60)),
        ..Default::default()
    };
    let checkpoint = db
        .create_checkpoint(CheckpointScope::Durable, &options)
        .await
        .unwrap();
    // One idle hour. Each peer keeps its samples fresh.
    for _ in 0..60 {
        time.advance(60_000);
        assert!(reply(&clock, &time, 2, 0, 1));
    }
    assert_eq!(read(&gate), time.wall());
    let admin = slatedb::admin::Admin::builder("state", store.clone())
        .with_system_clock(gate.clone())
        .build();
    admin
        .run_gc_once(slatedb::config::GarbageCollectorOptions::default())
        .await
        .unwrap();
    let left = admin.list_checkpoints(None).await.unwrap();
    assert!(left.iter().all(|left| left.id != checkpoint.id), "{left:?}");

    // A backward step of the wall clock.
    let before = read(&gate);
    time.step(-60_000);
    assert!(reply(&clock, &time, 2, 0, 1));
    assert_eq!(read(&gate), before);
    // All clocks step one hour ahead. The ticker still waits its interval.
    let mut ticker = gate.ticker(Duration::from_millis(200));
    ticker.tick().await;
    time.step(3_600_000);
    assert!(reply(&clock, &time, 2, 0, 1));
    let started = Instant::now();
    ticker.tick().await;
    assert!(started.elapsed() >= Duration::from_millis(150));
    assert!(read(&gate) >= before + 3_000_000);
    db.close().await.unwrap();
}

/// A leader clock one hour behind the machine clock, so SlateDB's default clock is far ahead
/// of each voter.
fn behind_machine(voters: &[u64]) -> (Clock, Time) {
    let (clock, time) = leader(voters);
    let machine = DefaultSystemClock::new().now().timestamp_millis();
    time.wall.store(machine - 3_600_000, Ordering::SeqCst);
    (clock, time)
}

/// The newest L0 SST ID time of `manifest`.
fn newest_sst(manifest: &slatedb::VersionedManifest) -> i64 {
    let newest = manifest.l0().iter().map(|view| view.sst.id.value()).max();
    newest.map_or(i64::MIN, |id| id.timestamp_ms() as i64)
}

/// Item 1 of review round 1: the leader writes a batch and stands down, and a flush runs after
/// that. The voter clocks are one hour behind the machine clock. The ended term freezes the
/// SlateDB clock, so the reopened manifest holds no tick and no L0 SST ID time above the last
/// authorized time. No batch gets a tick after the term ended. A compaction while the term lasts
/// takes output SST IDs at or below the cap. After the term, the frozen clock holds them too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ended_term_freezes_the_slatedb_clock() {
    for new_term in [false, true] {
        let store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let (clock, time) = behind_machine(&[1, 2, 3]);
        assert!(reply(&clock, &time, 2, 0, 0));
        assert!(reply(&clock, &time, 3, 0, 0));
        let gate = clock.gate().await;
        let db = open(&store, &gate).await;
        // The admin takes the ID of a submitted compaction from the same clock.
        let admin = slatedb::admin::Admin::builder("state", store.clone())
            .with_system_clock(gate.clone())
            .build();
        write(&db, &gate, 1).await;
        db.flush().await.unwrap();
        // The compactor needs its clock to move: all clocks run 10x fast, with fresh samples.
        let running = tokio::spawn({
            let (clock, time) = (clock.clone(), time.clone());
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    time.advance(100);
                    reply(&clock, &time, 2, 0, 0);
                    reply(&clock, &time, 3, 0, 0);
                }
            }
        });
        dst_compact(&admin, 1).await.unwrap();
        running.abort();
        let _ = running.await;
        let compacted = admin.read_manifest(None).await.unwrap().unwrap();
        assert!(!compacted.compacted().is_empty());
        for view in compacted.compacted().iter().flat_map(|run| run.sst_views()) {
            assert!(view.sst.id.value().timestamp_ms() as i64 <= read(&gate));
        }
        write(&db, &gate, 2).await;
        let authorized = read(&gate);
        assert!(authorized <= time.wall());
        match new_term {
            false => clock.stand_down(),
            true => configure(&clock, 3, &[1, 2, 3], PROCESS),
        }
        assert_eq!(read(&gate), authorized);
        // A flush is queued at the step-down, and its caller drops it. The work still runs.
        drop(tokio::spawn({
            let db = db.clone();
            async move { db.flush().await }
        }));
        let admitted = tokio::time::timeout(Duration::from_millis(200), gate.admit()).await;
        assert!(admitted.is_err(), "a batch got a tick after its term ended");
        db.flush().await.unwrap();
        // The frozen clock holds the time of the compactor too, so the compaction need not
        // end. Any output takes its SST ID from the frozen clock.
        let _ = dst_compact(&admin, 1).await;
        db.close_with_options(slatedb::config::CloseOptions { flush_type: None })
            .await
            .unwrap();
        let manifest = admin.read_manifest(None).await.unwrap().unwrap();
        assert!(manifest.last_l0_clock_tick() <= authorized);
        assert!(newest_sst(&manifest) <= authorized);
        assert!(
            floor(&manifest) <= authorized,
            "{}",
            floor(&manifest) - authorized
        );
        for run in manifest.compacted() {
            for view in run.sst_views() {
                assert!(view.sst.id.value().timestamp_ms() as i64 <= authorized);
            }
        }
    }
}

/// Sig-Codex, round 1: after a backward step of the leader and one peer, the other peer keeps
/// the old time. The next batch waits: only one of three voters supports the retained tick.
/// The manifest keeps the tick of the first batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backward_step_of_a_majority_holds_the_next_batch() {
    let store: Arc<dyn slatedb::object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let (clock, time) = leader(&[1, 2, 3]);
    time.step(60_000);
    assert!(reply(&clock, &time, 2, 0, 0));
    assert!(reply(&clock, &time, 3, 0, 0));
    let gate = clock.gate().await;
    let db = open(&store, &gate).await;
    write(&db, &gate, 1).await;
    let first = read(&gate);
    // The leader and member 3 step back 60 s. Member 2 keeps its clock.
    time.step(-60_000);
    assert!(reply(&clock, &time, 2, 60_000, 0));
    assert!(reply(&clock, &time, 3, 0, 0));
    let held = tokio::time::timeout(Duration::from_millis(300), write(&db, &gate, 2)).await;
    assert!(
        held.is_err(),
        "a batch was admitted above two of three voters"
    );
    assert_eq!(clock.alarm_name(), None);
    db.flush().await.unwrap();
    assert_eq!(db.manifest().last_l0_clock_tick(), first);
    db.close().await.unwrap();
}

/// A lone voter's writer has no check. A promoted learner joins the voters in the same term.
/// The next batch waits for the sample of the new voter, and then gets a bounded tick.
#[tokio::test]
async fn a_promoted_voter_turns_on_the_check_before_the_next_batch() {
    let (clock, time) = leader(&[1]);
    let gate = clock.gate().await;
    gate.admit().await.done();
    configure(&clock, 2, &[1, 2], PROCESS);
    let waited = tokio::time::timeout(Duration::from_millis(300), gate.admit()).await;
    assert!(
        waited.is_err(),
        "a batch was admitted without the new voter's sample"
    );
    assert!(clock.waiting());
    // The check holds also while the samples are missing: the clock does not move.
    let frozen = read(&gate);
    time.advance(1_000);
    assert_eq!(read(&gate), frozen);
    assert!(reply(&clock, &time, 2, 0, 0));
    let admitted = tokio::time::timeout(Duration::from_secs(1), gate.admit())
        .await
        .expect("the sample did not end the wait");
    assert!(read(&gate) <= time.wall().max(frozen));
    admitted.done();
}

/// Clocks with a 5 ms limit: the leader at 40 and both peers at 100 allow a tick of 100. A
/// restart of member 2 drops its sample, and the bound falls to 45.
fn ordered() -> (Clock, Time) {
    let (clock, time) = skewed(&[1, 2, 3], Duration::from_millis(5));
    time.wall.store(START + 40, Ordering::SeqCst);
    for peer in [2, 3] {
        assert!(late(&clock, peer, clock.probe(), START + 100, (0, 0)));
    }
    (clock, time)
}

/// Restart member 2 in another thread. The receiver gets a message when the refresh ends.
fn restart_peer(clock: &Clock) -> (std::sync::mpsc::Receiver<()>, std::thread::JoinHandle<()>) {
    let (done, ended) = std::sync::mpsc::channel();
    let clock = clock.clone();
    let thread = std::thread::spawn(move || {
        let voters = [1, 2, 3].map(|id| (id, Some(PROCESS + u64::from(id == 2))));
        clock.configure(2, 1, voters.into_iter().collect());
        let _ = done.send(());
    });
    (ended, thread)
}

/// Critic, round 2: a refresh of the voters runs wholly before or after a tick decision of
/// `admit`. The test holds the tick lock while `admit` decides, and restarts a voter. The
/// refresh waits for the pin, and the next batch follows the refreshed bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refresh_never_splits_the_admission_of_a_tick() {
    let (clock, _time) = ordered();
    let gate = clock.gate().await;
    gate.floor(START + 100);
    let (locked, release) = (std::sync::mpsc::channel(), std::sync::mpsc::channel::<()>());
    let holder = std::thread::spawn({
        let gate = gate.clone();
        let (locked, release) = (locked.0, release.1);
        move || {
            let _ticks = gate.ticks.lock().unwrap();
            locked.send(()).unwrap();
            let _ = release.recv();
        }
    });
    locked.1.recv().unwrap();
    let admit = tokio::spawn({
        let gate = gate.clone();
        async move { gate.admit().await.done() }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (ended, refresh) = restart_peer(&clock);
    let early = ended.recv_timeout(Duration::from_millis(200)).is_ok();
    release.0.send(()).unwrap();
    holder.join().unwrap();
    refresh.join().unwrap();
    admit.await.unwrap();
    assert!(
        !early,
        "a refresh ran between the verdict and the pin of a tick"
    );
    assert_eq!(read(&gate), START + 100);
    let next = tokio::time::timeout(Duration::from_millis(300), gate.admit()).await;
    assert!(
        next.is_err(),
        "the next batch used the bound from before the refresh"
    );
    assert!(!late(&clock, 2, clock.probe(), START + 100, (0, 0)) || clock.waiting());
}

/// Critic, round 2: the same for the advance of `last` between batches. A refresh waits until
/// the advance ends. After the refresh the samples are too few, and `last` does not advance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refresh_never_splits_the_advance_of_the_clock() {
    let (clock, time) = leader(&[1, 2, 3]);
    assert!(reply(&clock, &time, 2, 0, 0));
    assert!(reply(&clock, &time, 3, 0, 0));
    let gate = clock.gate().await;
    time.advance(50);
    let (reached, paused) = std::sync::mpsc::sync_channel(1);
    let (resume, resumed) = std::sync::mpsc::sync_channel(1);
    *gate.read_hold.lock().unwrap() = Some((reached, resumed));
    let reader = std::thread::spawn({
        let gate = gate.clone();
        move || read(&gate)
    });
    paused.recv().unwrap();
    // Both peers restart, so the samples are too few after the refresh.
    let (done, ended) = std::sync::mpsc::channel();
    let refresh = std::thread::spawn({
        let clock = clock.clone();
        move || {
            configure(&clock, 2, &[1, 2, 3], PROCESS + 1);
            let _ = done.send(());
        }
    });
    let early = ended.recv_timeout(Duration::from_millis(200)).is_ok();
    resume.send(()).unwrap();
    let advanced = reader.join().unwrap();
    refresh.join().unwrap();
    assert!(
        !early,
        "a refresh ran between the verdict and the advance of the clock"
    );
    assert_eq!(advanced, time.wall());
    time.advance(1_000);
    assert_eq!(clock.verdict(), Verdict::Waiting);
    assert_eq!(read(&gate), advanced, "the clock advanced without samples");
}

/// Architecture, round 2: SlateDB refuses a compaction job whose ID time is below the newest
/// job, and such a refusal closes the writer `Db`. A writer that left a job 20 ms ahead of the
/// next writer's clock raises the floor of that writer, so the next writer's job is accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_writer_starts_at_the_newest_compaction_job() {
    use slatedb::compactor::{CompactionSpec, SourceId};
    let store: Arc<dyn slatedb::object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let submit = |gate: &Arc<Gate>| {
        let admin = slatedb::admin::Admin::builder("state", store.clone())
            .with_system_clock(gate.clone())
            .build();
        async move {
            let manifest = admin.read_manifest(None).await.unwrap().unwrap();
            let sources = manifest.l0().iter().map(|view| SourceId::SstView(view.id));
            let spec = CompactionSpec::new(sources.collect(), 0);
            admin.submit_compaction(spec).await
        }
    };
    let (clock, time) = leader(&[1, 2, 3]);
    time.wall.store(START + 10, Ordering::SeqCst);
    assert!(reply(&clock, &time, 2, 0, 0));
    assert!(reply(&clock, &time, 3, 0, 0));
    let gate = clock.gate().await;
    let db = open(&store, &gate).await;
    write(&db, &gate, 1).await;
    db.flush().await.unwrap();
    // The job comes 40 ms after the flush, so only the job is above the next writer's clock.
    time.wall.store(START + 50, Ordering::SeqCst);
    assert!(reply(&clock, &time, 2, 0, 0));
    assert!(reply(&clock, &time, 3, 0, 0));
    submit(&gate).await.unwrap();
    db.close().await.unwrap();

    let (next, time) = leader(&[1, 2, 3]);
    time.wall.store(START + 30, Ordering::SeqCst);
    assert!(reply(&next, &time, 2, 0, 0));
    assert!(reply(&next, &time, 3, 0, 0));
    let gate = next.gate().await;
    let admin = slatedb::admin::Admin::builder("state", store.clone()).build();
    let manifest = admin.read_manifest(None).await.unwrap().unwrap();
    assert!(
        floor(&manifest) <= START + 10,
        "{}",
        floor(&manifest) - START
    );
    let stored = stored_floor(&admin).await.unwrap();
    assert!(stored >= START + 50, "{}", stored - START);
    gate.floor(stored);
    let db = open(&store, &gate).await;
    submit(&gate)
        .await
        .expect("the job of the next writer was refused");
    assert!(db.status().close_reason.is_none());
    db.close().await.unwrap();
}

/// Sig-Codex, round 4: the clock steps back while a new writer waits for its floor. The next
/// wakeup reads the clock again and refuses, with the alarm, instead of an endless wait.
#[tokio::test]
async fn a_backward_step_during_the_floor_wait_refuses() {
    let (clock, time) = leader(&[1, 2, 3]);
    let waiting = tokio::spawn({
        let (clock, tick) = (clock.clone(), time.wall() + 300);
        async move { clock.pass(tick).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished());
    assert_eq!(clock.alarm_name(), None);
    time.step(-10_000);
    let passed = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("the wait did not see the backward step")
        .unwrap();
    assert!(!passed);
    assert_eq!(clock.alarm_name(), Some("behind"));
    assert!(clock.skew.load(Ordering::Relaxed) > 10_000);
}
