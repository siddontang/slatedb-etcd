//! A voter whose apply holds is never a handoff target, and a leader whose apply holds hands
//! over its leadership.

use super::*;

fn candidate(id: u64, matched: u64) -> Candidate {
    Candidate {
        id,
        matched,
        process: Some(id),
    }
}

/// A report of `member` in term 7, received now.
fn report(member: u64, failed_secs: Option<u64>, apply_held_at: Option<u64>) -> Received {
    Received {
        process: member,
        at: Instant::now(),
        report: WriterReport {
            failed_secs,
            term: 7,
            apply_held_at,
        },
    }
}

fn tick<'a>(candidates: &'a [Candidate], published: Option<Instant>, held: bool) -> Tick<'a> {
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
        held,
    }
}

fn leading() -> Handoff {
    Handoff {
        led: Some(Instant::now() - Duration::from_secs(60)),
        ..Default::default()
    }
}

/// A publication of the applied prefix does not end the incident of a held leader. The
/// handoff keeps its tried voters and moves on to the next one.
#[test]
fn a_publication_while_held_keeps_the_tried_voters() {
    let mut handoff = leading();
    let candidates = [candidate(2, 20), candidate(3, 30)];
    for id in [2, 3] {
        handoff.reports.insert(id, report(id, None, None));
    }
    let first = handoff.step(&tick(&candidates, None, true));
    assert_eq!(first.transfer, Some(3));
    // Raft ended the transfer. Then the leader's writer published its applied prefix.
    let published = Some(Instant::now());
    let next = handoff.step(&tick(&candidates, published, true));
    assert_eq!(next.transfer, Some(2));
    assert_eq!(handoff.tried, [2, 3].into());
    // The writer mark of a member clears at its publication. The hold does not.
    let health = Health::default();
    let task = health.begin();
    task.failed(Fault::Unavailable, "a store error");
    task.published();
    assert_eq!(health.report(7, Some(12)).apply_held_at, Some(12));
}

/// A voter whose apply holds is never a target: not in a fault handoff, also when its writer
/// mark is older than the leader's last publication, and not in a handoff of last resort.
#[test]
fn a_held_voter_is_never_a_target() {
    let mut handoff = leading();
    let candidates = [candidate(2, 20), candidate(3, 30)];
    // The writer mark of voter 3 is older than the publication below, so the mark is stale.
    handoff.reports.insert(3, report(3, Some(120), Some(9)));
    let published = Some(Instant::now() - Duration::from_secs(10));
    let held_only = [candidate(3, 30)];
    assert_eq!(
        handoff.step(&tick(&held_only, published, false)).transfer,
        None
    );
    assert_eq!(rank(Some(&handoff), published, &candidates), Some(2));
    assert_eq!(rank(Some(&handoff), published, &held_only), None);
    // The same voter without the hold is a stale but valid target.
    handoff.reports.insert(3, report(3, Some(120), None));
    handoff.exhausted = false;
    assert_eq!(
        handoff.step(&tick(&held_only, published, false)).transfer,
        Some(3)
    );
}

/// A leader whose apply waits on one entry for longer than `STALL` hands over its leadership.
#[test]
fn a_held_leader_hands_over() {
    let paused = |age: Duration| super::super::apply::Paused {
        index: 9,
        held: true,
        reason: "corrupt stored data".into(),
        since: Instant::now() - age,
    };
    let health = Health::default();
    assert!(!handoff_due(&health, None));
    assert!(!handoff_due(&health, Some(&paused(Duration::from_secs(1)))));
    let stalled = paused(STALL + Duration::from_secs(1));
    assert!(handoff_due(&health, Some(&stalled)));
    let mut handoff = leading();
    let candidates = [candidate(2, 20)];
    handoff.reports.insert(2, report(2, None, None));
    let tick = Tick {
        due: handoff_due(&health, Some(&stalled)),
        ..tick(&candidates, None, true)
    };
    assert_eq!(handoff.step(&tick).transfer, Some(2));
}
