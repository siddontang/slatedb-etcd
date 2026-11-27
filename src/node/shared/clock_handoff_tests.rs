//! Clocks of three voters: a leader whose clock is ahead of the voters, and a leader whose
//! clock is behind the shared writer clock.

use super::super::tests::status;
use super::tests::{channel, health, publishes, until, Cluster};
use super::*;
use crate::proto::etcdserverpb::{maintenance_client::MaintenanceClient, MoveLeaderRequest};

/// The default `--max-clock-skew`.
const MAX_SKEW_MS: i64 = 5_000;

fn wall_ms() -> i64 {
    (Clock::system(Duration::ZERO, None).now_ms)()
}

/// The shared manifest.
async fn manifest(cluster: &Cluster) -> slatedb::VersionedManifest {
    let admin = slatedb::admin::Admin::builder("root/shared/state", cluster.store.clone()).build();
    admin.read_manifest(None).await.unwrap().unwrap()
}

/// Case 2 of issue #101. The leader's clock steps 10 s ahead of two correct voters. A client Put
/// still succeeds. The leader writes it at the cap, with no fault and no handoff. So the manifest
/// floor stays at most the skew limit above the voter clocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_fast_leader_writes_at_the_cap() {
    let cluster = Cluster::start(|_| {}).await;
    let fast = cluster.leader().await;
    cluster.put("k-before").await.unwrap();
    publishes(&cluster, fast).await;
    let before = status(cluster.member(fast)).await;
    let written = before["shared_state"]["writer"]["written"]
        .as_u64()
        .unwrap();
    cluster.skew(fast, 10_000);
    cluster.put("k-fast").await.unwrap();
    until("a flush of the Put", Duration::from_secs(30), async || {
        manifest(&cluster).await.last_l0_seq() > written
    })
    .await;
    let after = status(cluster.member(fast)).await;
    assert_eq!(cluster.leading().await, Some(fast));
    assert_eq!(
        health(&after)["faults"],
        health(&before)["faults"],
        "{after}"
    );
    assert!(after["shared_state"]["writer"]["clock"]["ahead_ms"].as_i64() > Some(0));
    assert!(clock::floor(&manifest(&cluster).await) <= wall_ms() + MAX_SKEW_MS);
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
