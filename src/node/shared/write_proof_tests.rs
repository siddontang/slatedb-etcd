//! The write proof of a learner before its promotion.

use super::super::tests::{eventually, status};
use super::*;
use crate::cluster::Command as C;
use crate::object::faults::{FaultStore, FAIL_STATE_DELETE, LOSE_STATE_PUT_REPLY, READ_ONLY_STATE};
use crate::proto::etcdserverpb::{
    cluster_client::ClusterClient, MemberAddRequest, MemberListRequest, MemberPromoteRequest,
};
use crate::server::{Server, ServerConfig};
use futures::TryStreamExt;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

const SECOND: Duration = Duration::from_secs(1);

/// A prober of member 2 on `faults`, and the scratch objects under its shared state.
fn prober(faults: &Arc<FaultStore>) -> Prober {
    let store: Arc<dyn ObjectStore> = faults.clone();
    let cache = crate::state::StateCache::default();
    let source = crate::state::shared::offline_source(store, "root", cache).unwrap();
    Prober::new(2, source)
}

async fn scratch(store: &dyn ObjectStore) -> Vec<String> {
    let prefix = Path::from("root/shared/state/.probe");
    let listed: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
    listed
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect()
}

/// A successful probe creates its scratch object in the shared state, with its member and
/// process in the name, and deletes it.
#[tokio::test]
async fn a_successful_probe_leaves_no_scratch_object() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    prober(&faults).probe(11).await.unwrap();
    let name = format!(
        "root/shared/state/.probe/2-{}-11",
        crate::peer::process_id()
    );
    assert!(faults.written.lock().unwrap().contains(&name));
    assert!(scratch(faults.as_ref()).await.is_empty());
}

/// A read-only member fails each probe at the create. Its refused delete of a scratch object
/// that does not exist does not hide that cause.
#[tokio::test]
async fn a_read_only_member_fails_each_probe_at_the_create() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    faults.set(READ_ONLY_STATE);
    let prober = prober(&faults);
    for nonce in 1..=3 {
        let error = prober.probe(nonce).await.unwrap_err();
        assert!(format!("{error:#}").contains("cannot create"), "{error:#}");
    }
    assert!(scratch(faults.as_ref()).await.is_empty());
}

/// A member that can create but not delete keeps at most one scratch object over many probes.
/// After the delete works again, the next probe removes it.
#[tokio::test]
async fn a_refused_delete_leaves_at_most_one_scratch_object() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    faults.set(FAIL_STATE_DELETE);
    let prober = prober(&faults);
    for nonce in 1..=5 {
        let error = prober.probe(nonce).await.unwrap_err();
        assert!(format!("{error:#}").contains("cannot delete"), "{error:#}");
    }
    assert_eq!(scratch(faults.as_ref()).await.len(), 1);
    faults.set(0);
    prober.probe(6).await.unwrap();
    assert!(scratch(faults.as_ref()).await.is_empty());
}

/// A create that reached the store but lost its reply leaves a scratch object. The next probe
/// deletes it before it creates its own.
#[tokio::test]
async fn a_lost_create_reply_is_cleaned_by_the_next_probe() {
    let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
    faults.set(LOSE_STATE_PUT_REPLY);
    let prober = prober(&faults);
    prober.probe(1).await.unwrap_err();
    assert_eq!(scratch(faults.as_ref()).await.len(), 1);
    prober.probe(2).await.unwrap();
    assert!(scratch(faults.as_ref()).await.is_empty());
}

/// A successful reply of process `process` to `nonce`.
fn proven(nonce: u64, process: u64) -> Replied {
    Replied {
        nonce,
        process: Some(process),
        outcome: Ok(()),
    }
}

/// Start a probe of member 2 from process 7 in term 3 at `now`, and return its nonce.
fn start(proofs: &mut Proofs, now: Instant) -> u64 {
    let refusal = proofs.check(now, 3, 2, Some(7)).unwrap_err();
    refusal.probe.expect("no probe started")
}

/// A proof belongs to one process of the member. A reply of another process, and a proof of
/// an earlier process, prove nothing.
#[test]
fn a_proof_counts_only_for_the_process_that_made_it() {
    let mut proofs = Proofs::default();
    let now = Instant::now();
    let nonce = start(&mut proofs, now);
    assert!(!proofs.complete(now, 3, 2, Some(7), proven(nonce, 8)));
    // The proof is still missing, so the next check starts a new probe.
    let refusal = proofs.check(now, 3, 2, Some(7)).unwrap_err();
    assert!(refusal.message.contains("is missing"), "{refusal:?}");
    let nonce = refusal.probe.unwrap();
    assert!(proofs.complete(now, 3, 2, Some(7), proven(nonce, 7)));
    assert!(proofs.check(now, 3, 2, Some(7)).is_ok());
    // The member restarts. The proof of its earlier process does not count.
    let refusal = proofs.check(now, 3, 2, Some(9)).unwrap_err();
    assert!(refusal.message.contains("is missing"), "{refusal:?}");
    assert!(refusal.probe.is_some());
    // A member with no known process gets no probe.
    let refusal = proofs.check(now, 3, 4, None).unwrap_err();
    assert!(refusal.probe.is_none());
}

/// A proof counts for `PROOF_LIFETIME` from the send of its probe, not from its reply. An
/// expired proof starts a new probe.
#[test]
fn a_proof_expires_at_its_lifetime_from_the_send() {
    let mut proofs = Proofs::default();
    let sent = Instant::now();
    let nonce = start(&mut proofs, sent);
    let replied = sent + 10 * SECOND;
    assert!(proofs.complete(replied, 3, 2, Some(7), proven(nonce, 7)));
    let last = sent + PROOF_LIFETIME - Duration::from_millis(1);
    assert!(proofs.check(last, 3, 2, Some(7)).is_ok());
    let refusal = proofs
        .check(sent + PROOF_LIFETIME, 3, 2, Some(7))
        .unwrap_err();
    assert!(refusal.message.contains("is expired"), "{refusal:?}");
    assert!(refusal.probe.is_some());
}

/// Only the first reply to the running probe counts, before the reply deadline and in the
/// term of the probe. A late, a repeated, or an earlier term's reply proves nothing.
#[test]
fn a_late_repeated_or_earlier_term_reply_proves_nothing() {
    let mut proofs = Proofs::default();
    let sent = Instant::now();
    let nonce = start(&mut proofs, sent);
    assert!(!proofs.complete(sent + REPLY_WAIT, 3, 2, Some(7), proven(nonce, 7)));
    assert!(proofs.check(sent + REPLY_WAIT, 3, 2, Some(7)).is_err());

    // One running probe at a time.
    let sent = sent + REPLY_WAIT + RETRY_CAP;
    let nonce = start(&mut proofs, sent);
    let refusal = proofs.check(sent, 3, 2, Some(7)).unwrap_err();
    assert!(refusal.probe.is_none(), "{refusal:?}");
    assert!(proofs.complete(sent, 3, 2, Some(7), proven(nonce, 7)));
    // A repeated reply does not extend the proof.
    let late = sent + PROOF_LIFETIME - SECOND;
    assert!(!proofs.complete(late, 3, 2, Some(7), proven(nonce, 7)));
    assert!(proofs.check(sent + PROOF_LIFETIME, 3, 2, Some(7)).is_err());

    // A reply that comes after a leader change proves nothing in the new term.
    let now = sent + PROOF_LIFETIME;
    let nonce = proofs.pending(2).unwrap();
    assert!(!proofs.complete(now, 5, 2, Some(7), proven(nonce, 7)));
    assert!(proofs.check(now, 5, 2, Some(7)).is_err());
}

/// A failed probe backs off before the next one, up to `RETRY_CAP`. The refusal names the
/// failure.
#[test]
fn a_failed_probe_backs_off_the_next_probe() {
    let mut proofs = Proofs::default();
    let mut now = Instant::now();
    let mut nonce = start(&mut proofs, now);
    let mut waits = vec![];
    for _ in 0..7 {
        let failed = Replied {
            nonce,
            process: Some(7),
            outcome: Err("denied".into()),
        };
        assert!(proofs.complete(now, 3, 2, Some(7), failed));
        let mut wait = 0;
        loop {
            let refusal = proofs
                .check(now + wait * SECOND, 3, 2, Some(7))
                .unwrap_err();
            assert!(refusal.message.contains("failed: denied"), "{refusal:?}");
            if let Some(next) = refusal.probe {
                nonce = next;
                break;
            }
            wait += 1;
        }
        waits.push(wait);
        now += wait * SECOND;
    }
    assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30]);
}

/// Drive a manual node until its log and apply are idle.
async fn settle(node: &mut Node) {
    for _ in 0..5 {
        node.drive_ready().await.unwrap();
        node.drain_log().await.unwrap();
        node.drain_apply().await.unwrap();
    }
}

/// Elect this one-voter node again, in a new term.
async fn elect(node: &mut Node) {
    settle(node).await;
    let term = node.raw.raft.term;
    node.raw.raft.become_follower(term + 1, raft::INVALID_ID);
    node.raw.campaign().unwrap();
    assert_eq!(node.raw.raft.state, StateRole::Leader);
    settle(node).await;
}

/// A one-voter shared leader with a manual transport.
async fn shared_leader(dir: &std::path::Path) -> Node {
    let mut config = crate::node::tests::options(dir);
    config.peers = BTreeMap::from([(1, "127.0.0.1:1".to_owned())]);
    config.state_mode = crate::state::StateMode::Shared;
    let store = Arc::new(InMemory::new());
    config.object_store = crate::object::Config::Test(store, "root".into());
    let (events, _) = mpsc::channel(256);
    let mut node = Node::open_with_transport(config, events, Some(BTreeMap::new()))
        .await
        .unwrap();
    while node.raw.raft.state != StateRole::Leader {
        node.tick().unwrap();
    }
    settle(&mut node).await;
    node
}

/// The successful reply of process `process` of member 2 to `nonce`.
fn proven_reply(node: &Node, nonce: u64, process: u64) -> Event {
    Event::WriteProbed {
        member: 2,
        nonce,
        reply: Ok(WriteProbeReply {
            nonce,
            process,
            store: node.peer.store.clone(),
            error: None,
        }),
    }
}

/// The nonce of the running probe of member 2.
fn pending(node: &Node) -> u64 {
    node.writer.as_ref().unwrap().proofs.pending(2).unwrap()
}

/// The transport frame of process `process` of member 2.
fn frame(process: u64) -> Event {
    Event::PeerProcess {
        member: 2,
        process: Some(process),
    }
}

/// The node takes a reply only in the term that sent its probe. A reply that comes after a
/// leader change proves nothing, also when this member leads again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_after_a_leader_change_proves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = shared_leader(dir.path()).await;
    let mut responses = vec![];
    node.handle(frame(7), &mut responses).await.unwrap();
    let promote = C::Promote(2);

    // In one term, the reply proves write access.
    node.check_write_proof(&promote).unwrap_err();
    let event = proven_reply(&node, pending(&node), 7);
    node.handle(event, &mut responses).await.unwrap();
    node.check_write_proof(&promote).unwrap();

    // A new term drops the proof. The reply to its probe comes after one more election.
    elect(&mut node).await;
    let error = node.check_write_proof(&promote).unwrap_err();
    assert!(error.message.contains("is missing"), "{error:?}");
    let event = proven_reply(&node, pending(&node), 7);
    elect(&mut node).await;
    node.handle(event, &mut responses).await.unwrap();
    let error = node.check_write_proof(&promote).unwrap_err();
    assert!(error.message.contains("is missing"), "{error:?}");
}

/// A new process of the learner drops its proof and its running probe at once. A late frame
/// of the earlier process can then name that process again. Neither its old proof nor a late
/// reply to its old probe counts after that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_learner_process_drops_its_proof_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = shared_leader(dir.path()).await;
    let mut responses = vec![];
    let promote = C::Promote(2);
    prove(&mut node).await;
    node.handle(frame(8), &mut responses).await.unwrap();
    node.handle(frame(7), &mut responses).await.unwrap();
    let error = node.check_write_proof(&promote).unwrap_err();
    assert!(error.message.contains("is missing"), "{error:?}");

    // The probe of process 7 runs. Process 8 starts, a late frame names 7, and the reply of 7
    // comes.
    let nonce = pending(&node);
    node.handle(frame(8), &mut responses).await.unwrap();
    node.handle(frame(7), &mut responses).await.unwrap();
    let event = proven_reply(&node, nonce, 7);
    node.handle(event, &mut responses).await.unwrap();
    let error = node.check_write_proof(&promote).unwrap_err();
    assert!(error.message.contains("is missing"), "{error:?}");
}

/// The write proofs in the `/status` of `node`.
fn shown(node: &Node) -> Value {
    node.writer.as_ref().unwrap().status()["write_proofs"]["members"].clone()
}

/// Record a proof of process 7 of member 2 on `node`.
async fn prove(node: &mut Node) {
    let mut responses = vec![];
    node.handle(frame(7), &mut responses).await.unwrap();
    node.check_write_proof(&C::Promote(2)).unwrap_err();
    let event = proven_reply(node, pending(node), 7);
    node.handle(event, &mut responses).await.unwrap();
    node.check_write_proof(&C::Promote(2)).unwrap();
    assert_eq!(shown(node)[0]["proven"], true);
}

async fn channel(server: &Server) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(server.client_url())
        .unwrap()
        .connect()
        .await
        .unwrap()
}

fn shared_config(dir: &std::path::Path, faults: &Arc<FaultStore>) -> ServerConfig {
    let mut config = ServerConfig::single_node(dir);
    config.state_mode = crate::state::StateMode::Shared;
    config.object_store = crate::object::Config::Test(faults.clone(), "root".into());
    config.view_max_age = Duration::from_millis(100);
    config.strict_reconfiguration = false;
    config
}

/// One shared voter and a learner. The learner's store runs in fault mode `mode`.
struct Learner {
    voter: Server,
    learner: Server,
    id: u64,
    faults: Arc<FaultStore>,
    store: Arc<dyn ObjectStore>,
    _dir: tempfile::TempDir,
}

impl Learner {
    async fn start(mode: u8) -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let mut peers = BTreeMap::from([(1, crate::server::test_port())]);
        let faults = Arc::new(FaultStore::new(store.clone()));
        let mut config = shared_config(&dir.path().join("1"), &faults);
        config.listen = peers[&1].parse().unwrap();
        config.peers = peers.clone();
        config.client_peers = peers.clone();
        let voter = Server::start(config).await.unwrap();
        voter.wait_ready(Duration::from_secs(20)).await.unwrap();
        eventually("the first checkpoint", async || {
            let status = status(&voter).await;
            status["shared_state"]["applied_view_checkpoints"].as_u64() > Some(0)
        })
        .await;
        let address = crate::server::test_port();
        let added = ClusterClient::new(channel(&voter).await)
            .member_add(MemberAddRequest {
                peer_ur_ls: vec![format!("http://{address}")],
                is_learner: true,
            })
            .await
            .unwrap()
            .into_inner();
        let id = added.member.unwrap().id;
        peers.insert(id, address);
        let faults = Arc::new(FaultStore::new(store.clone()));
        faults.set(mode);
        let mut config = shared_config(&dir.path().join("learner"), &faults);
        config.id = id;
        config.listen = peers[&id].parse().unwrap();
        config.peers = peers.clone();
        config.client_peers = peers;
        config.join = true;
        let learner = Server::start(config).await.unwrap();
        Self {
            voter,
            learner,
            id,
            faults,
            store,
            _dir: dir,
        }
    }

    async fn promote(&self) -> Result<(), tonic::Status> {
        ClusterClient::new(channel(&self.voter).await)
            .member_promote(MemberPromoteRequest { id: self.id })
            .await
            .map(|_| ())
    }

    /// Promote until the leader refuses for the write proof. The learner is in sync then.
    async fn refused(&self) -> tonic::Status {
        let mut refusal = None;
        eventually("a refusal for the write proof", async || {
            let error = self
                .promote()
                .await
                .expect_err("promoted with no write proof");
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error:?}");
            let proof = error.message().contains("write proof");
            refusal = Some(error);
            proof
        })
        .await;
        refusal.unwrap()
    }

    /// Promote for `time`. Every request must fail. Return the last refusal.
    async fn refused_for(&self, time: Duration) -> tonic::Status {
        let started = Instant::now();
        let mut refusal = self.refused().await;
        while started.elapsed() < time {
            tokio::time::sleep(Duration::from_millis(200)).await;
            refusal = self
                .promote()
                .await
                .expect_err("promoted with no write access");
            assert_eq!(refusal.code(), tonic::Code::FailedPrecondition);
        }
        refusal
    }

    async fn is_learner(&self) -> bool {
        let members = ClusterClient::new(channel(&self.voter).await)
            .member_list(MemberListRequest::default())
            .await
            .unwrap()
            .into_inner()
            .members;
        let member = members.iter().find(|member| member.id == self.id).unwrap();
        member.is_learner
    }

    async fn shutdown(self) {
        self.learner.shutdown().await.unwrap();
        self.voter.shutdown().await.unwrap();
    }
}

/// A learner with read-only access to the shared state stays a learner. Each refusal is
/// retryable and names the failed proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_learner_stays_a_learner() {
    let cluster = Learner::start(READ_ONLY_STATE).await;
    let refusal = cluster.refused_for(4 * SECOND).await;
    assert!(
        refusal.message().contains("failed: cannot create"),
        "{refusal:?}"
    );
    assert!(cluster.is_learner().await);
    assert!(scratch(cluster.store.as_ref()).await.is_empty());
    cluster.shutdown().await;
}

/// A learner with write access is promoted after its probe. The first request is refused,
/// because the proof is missing. The probe leaves no scratch object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writable_learner_is_promoted_after_its_probe() {
    let cluster = Learner::start(0).await;
    let refusal = cluster.refused().await;
    assert!(refusal.message().contains("is missing"), "{refusal:?}");
    eventually("the promotion", async || cluster.promote().await.is_ok()).await;
    assert!(!cluster.is_learner().await);
    let written = cluster.faults.written.lock().unwrap().clone();
    assert!(
        written
            .iter()
            .any(|path| path.contains("/shared/state/.probe/")),
        "{written:?}"
    );
    assert!(scratch(cluster.store.as_ref()).await.is_empty());
    let proofs = &status(&cluster.voter).await["shared_state"]["writer"]["write_proofs"];
    assert_eq!(proofs["members"][0]["proven"], true, "{proofs}");
    cluster.shutdown().await;
}

/// A learner that can create but not delete stays a learner over repeated promotions. Its
/// probes leave at most one scratch object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_learner_that_cannot_delete_stays_a_learner_with_one_scratch_object() {
    let cluster = Learner::start(FAIL_STATE_DELETE).await;
    let refusal = cluster.refused_for(5 * SECOND).await;
    assert!(refusal.message().contains("cannot delete"), "{refusal:?}");
    assert!(cluster.is_learner().await);
    let proofs = &status(&cluster.voter).await["shared_state"]["writer"]["write_proofs"];
    let failures = proofs["members"][0]["failures"].as_u64().unwrap();
    assert!(failures >= 2, "{proofs}");
    assert_eq!(scratch(cluster.store.as_ref()).await.len(), 1);
    cluster.shutdown().await;
}
