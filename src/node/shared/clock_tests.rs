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
    let time = Time::default();
    time.wall.store(START, Ordering::SeqCst);
    let mut clock = Clock::system(Duration::from_secs(5), None);
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
