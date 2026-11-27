//! Clock handoffs of three voters: a leader whose clock is ahead of the voters, and a leader
//! whose clock is behind the shared writer clock.

use super::super::tests::status;
use super::tests::{channel, health, publishes, until, Cluster};
use super::*;
use crate::proto::etcdserverpb::{maintenance_client::MaintenanceClient, MoveLeaderRequest};

/// The default `--max-clock-skew`.
const MAX_SKEW_MS: i64 = 5_000;

fn wall_ms() -> i64 {
    (Clock::system(Duration::ZERO, None).now_ms)()
}

/// The floor of the shared manifest: the later of its tick and its newest L0 SST ID time.
async fn manifest_floor(cluster: &Cluster) -> i64 {
    let admin = slatedb::admin::Admin::builder("root/shared/state", cluster.store.clone()).build();
    let manifest = admin.read_manifest(None).await.unwrap().unwrap();
    clock::floor(&manifest)
}

/// Case 2 of issue #101. The leader's clock steps 10 s ahead of two correct voters. A client Put
/// still succeeds. The leader refuses to materialize it, writes no tick above the voters, and
/// hands over after three clock faults. The next leader writes the Put at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_fast_leader_hands_over_and_the_next_leader_writes_at_once() {
    let cluster = Cluster::start(|_| {}).await;
    let fast = cluster.leader().await;
    cluster.put("k-before").await.unwrap();
    publishes(&cluster, fast).await;
    cluster.skew(fast, 10_000);
    cluster.put("k-fast").await.unwrap();
    let mut next = None;
    until("a handoff", Duration::from_secs(30), async || {
        next = cluster.leading().await.filter(|leader| *leader != fast);
        next.is_some()
    })
    .await;
    let next = next.unwrap();
    let moved = Instant::now();
    let old = status(cluster.member(fast)).await;
    assert_eq!(health(&old)["last_error_class"], "clock", "{old}");
    // The next leader's clock agrees with the manifest, so it publishes without a wait.
    publishes(&cluster, next).await;
    assert!(
        moved.elapsed() < Duration::from_secs(5),
        "{:?}",
        moved.elapsed()
    );
    assert!(manifest_floor(&cluster).await <= wall_ms() + MAX_SKEW_MS);
    cluster.shutdown().await;
}

/// Case 1 of issue #101. MoveLeader hands the lead to a voter whose clock is 60 s behind. Its
/// writer refuses to start, and after three clock faults it hands over to a voter with a
/// correct clock, which writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_slow_leader_hands_over_to_a_voter_with_a_correct_clock() {
    let cluster = Cluster::start(|_| {}).await;
    let old = cluster.leader().await;
    cluster.put("k-before").await.unwrap();
    publishes(&cluster, old).await;
    let slow = (1..=3).find(|id| *id != old).unwrap();
    cluster.skew(slow, -60_000);
    MaintenanceClient::new(channel(cluster.member(old)).await)
        .move_leader(MoveLeaderRequest { target_id: slow })
        .await
        .unwrap();
    let leaders = cluster.leaders(Duration::from_secs(20)).await;
    assert_eq!(leaders.first(), Some(&slow), "{leaders:?}");
    assert_eq!(leaders.len(), 2, "{leaders:?}");
    let status = status(cluster.member(slow)).await;
    assert_eq!(health(&status)["last_error_class"], "clock", "{status}");
    assert_eq!(status["shared_state"]["writer"]["clock_alarm"], "behind");
    cluster.put("k-after").await.unwrap();
    publishes(&cluster, leaders[1]).await;
    cluster.shutdown().await;
}
