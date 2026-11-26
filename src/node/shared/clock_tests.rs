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
/// clock, and the fast leader stays refused.
#[test]
fn a_delayed_older_reply_never_replaces_a_newer_one() {
    let (clock, time) = leader(&[1, 2, 3]);
    let early = clock.probe();
    let sent = time.mono();
    // The peer clock is 10 s fast when it replies to the early probe. Then it steps back.
    let fast = time.wall() + 10_000;
    time.advance(5);
    assert!(reply(&clock, &time, 2, 0, 5));
    assert!(!late(&clock, 2, early, fast, (sent, time.mono())));
    // The leader is 10 s ahead of member 2, and member 3 gave no sample.
    time.step(10_000);
    assert!(ahead(clock.verdict()).is_some_and(|by| by > 4_000));
}

/// Three voters with one missing: the fast leader and its correct peer are the majority.
#[test]
fn a_fast_leader_with_one_correct_peer_and_one_missing_is_ahead() {
    let (clock, time) = leader(&[1, 2, 3]);
    assert_eq!(clock.verdict(), Verdict::Waiting);
    assert!(reply(&clock, &time, 2, -10_000, 2));
    assert_eq!(ahead(clock.verdict()), Some(5_000));
    clock.observe();
    assert_eq!(clock.alarm_name(), Some("ahead"));
    assert_eq!(clock.status()["ahead_ms"], 5_000);
    // The skew ends with a correct leader clock.
    time.step(-10_000);
    clock.observe();
    assert_eq!(clock.alarm_name(), None);
}

/// Two fast voters of three are a majority. Neither is refused.
#[test]
fn two_fast_voters_of_three_are_a_majority() {
    let (clock, time) = leader(&[1, 2, 3]);
    time.step(10_000);
    assert!(reply(&clock, &time, 2, 0, 2));
    assert!(reply(&clock, &time, 3, -10_000, 2));
    let Verdict::Admit { cap, real } = clock.verdict() else {
        panic!("a fast majority was refused");
    };
    assert!(real <= cap);
}

/// Slow replies widen each window. A correct leader clock is above the low edge, but never above
/// the high edge, so it is never refused. Its cap stays at the low edge.
#[test]
fn slow_replies_never_refuse_a_correct_leader() {
    let (clock, time) = leader(&[1, 2, 3, 4, 5]);
    // Each peer reads its correct clock at the send, and each reply takes 8 s.
    let probes: Vec<_> = (2..=5).map(|peer| (peer, clock.probe())).collect();
    let (sent, clock_ms) = (time.mono(), time.wall());
    time.advance(8_000);
    for (peer, probe) in probes {
        assert!(late(&clock, peer, probe, clock_ms, (sent, time.mono())));
    }
    let Verdict::Admit { cap, real } = clock.verdict() else {
        panic!("slow replies refused a correct leader");
    };
    assert!(cap < real, "the low edge must cap the tick: {cap} {real}");
    // Without any reply in four seconds the leader waits.
    time.advance(FRESH_MS);
    assert_eq!(clock.verdict(), Verdict::Waiting);
}

/// One voter, or a member that does not lead, has no ahead check.
#[test]
fn one_voter_has_no_ahead_check() {
    let (clock, time) = leader(&[1]);
    time.step(3_600_000);
    assert_eq!(clock.verdict(), Verdict::Off);
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
/// own clock at each batch, so the next batch is refused as a clock fault.
#[tokio::test]
async fn a_step_ahead_before_a_write_is_refused_at_the_next_write() {
    let (clock, time) = leader(&[1, 2, 3]);
    assert!(reply(&clock, &time, 2, 0, 2));
    assert!(reply(&clock, &time, 3, 0, 2));
    let gate = clock.gate().await;
    let admitted = gate.admit().await.unwrap();
    assert_eq!(read(&gate), time.wall());
    admitted.done();
    time.step(10_000);
    let refused = match gate.admit().await {
        Ok(_) => panic!("a fast leader clock was admitted"),
        Err(error) => error,
    };
    assert_eq!(fault::classify(&refused), fault::Fault::Clock);
    assert_eq!(clock.alarm_name(), Some("ahead"));
    // The SlateDB clock stays at the bound of the majority.
    assert!(read(&gate) < time.wall() - 4_000);
}

/// Slow replies widen each window, so the low edge of the bound falls below the floor of the
/// writer: an earlier writer wrote a tick 1 s ahead of this correct clock. The leader waits
/// and is never refused. A fast reply ends the wait.
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
        async move { gate.admit().await.map(Admitted::done) }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(reply(&clock, &time, 2, 0, 1));
    tokio::time::timeout(Duration::from_secs(1), admit)
        .await
        .expect("a fast reply did not end the wait")
        .unwrap()
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
    assert!(late(&clock, 2, clock.probe(), START + 100, (0, 0)));
    let gate = clock.gate().await;
    let db = open(&store, &gate).await;
    gate.floor(floor(&db.manifest()));
    write(&db, &gate, 1).await;
    db.flush().await.unwrap();
    assert_eq!(db.manifest().last_l0_clock_tick(), START + 90);

    assert!(late(&clock, 2, clock.probe(), START + 90, (0, 0)));
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

/// One voter has no ahead check. Its writer starts at once, and SlateDB reads its default
/// clock, also with a local clock one hour ahead.
#[tokio::test]
async fn one_voter_keeps_the_slatedb_clock() {
    let (clock, time) = leader(&[1]);
    time.step(3_600_000);
    let gate = tokio::time::timeout(Duration::from_secs(1), clock.gate())
        .await
        .expect("one voter waited for samples");
    let admitted = gate.admit().await.unwrap();
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
