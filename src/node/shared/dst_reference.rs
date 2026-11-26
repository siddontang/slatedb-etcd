//! The reference state of the simulation and the check of each member view against it.

use super::*;

/// The state that each member view must equal at its applied index.
///
/// The reference applies each committed entry in order with `apply::apply_entries`, the apply
/// function of the members. In shared mode it applies them to an in-memory state from
/// `StateMachine::open_memory`. That state shares only the apply function with the members. It
/// has no overlay, no base, no base switch, no materializer, and no checkpoint, so an error in
/// that code of the members cannot also be in the reference.
///
/// A replicated state with no flags also applies each entry. Its quota flag disables the check,
/// unlike the flag of any member. Replicated simulations use only this state, because their
/// quota rules differ from shared mode. In shared mode the two states must have equal rows and
/// equal loaded fields after each index, so an error in the in-memory state shows.
pub(super) struct Reference {
    memory: Option<StateMachine>,
    replicated: StateMachine,
    /// The indexes at which the in-memory state and the replicated state were equal.
    pub(super) compared: u64,
}

impl Reference {
    /// Open the reference states in `dir`. The seed entry of the first leader gives them the
    /// membership row.
    pub(super) async fn open(dir: &std::path::Path, shared: bool) -> Self {
        let memory = if shared {
            Some(StateMachine::open_memory().await.unwrap())
        } else {
            None
        };
        Self {
            memory,
            replicated: StateMachine::open(dir).await.unwrap(),
            compared: 0,
        }
    }

    fn applied(&self) -> u64 {
        self.replicated.applied
    }

    /// Apply `entry`, the next committed entry. Return the digest of the `etcd/` rows after it.
    async fn apply(&mut self, entry: Entry) -> u32 {
        let header = Header {
            cluster: 0,
            member: 0,
            term: entry.term,
        };
        let replicated = &mut self.replicated;
        apply::apply_entries(replicated, vec![entry.clone()], header)
            .await
            .unwrap();
        let expected = rows_digest(replicated.applied_state()).await.unwrap();
        let Some(memory) = &mut self.memory else {
            return expected;
        };
        apply::apply_entries(memory, vec![entry], header)
            .await
            .unwrap();
        let digest = rows_digest(memory.applied_state()).await.unwrap();
        let index = memory.applied;
        assert_eq!(digest, expected, "the reference rows differ at {index}");
        assert_eq!(
            memory.loaded_fields(),
            replicated.loaded_fields(),
            "the reference fields differ at {index}"
        );
        self.compared += 1;
        digest
    }
}

/// The member state that one view check covered: the applied index and the base of the
/// published view. A base switch changes the view but keeps the applied index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ViewPoint {
    applied: u64,
    checkpoint: Option<uuid::Uuid>,
    base_index: Option<u64>,
}

impl ViewPoint {
    fn of(state: &apply::AppliedState) -> Self {
        Self {
            applied: state.applied,
            checkpoint: state.base_checkpoint,
            base_index: state.view.map(|(index, _, _)| index),
        }
    }
}

impl Simulation {
    /// Run `check_views`, and fail the simulation on a difference.
    pub(super) async fn check_reference(&mut self) {
        if let Err(error) = self.check_views().await {
            panic!("seed {}: {error}", self.seed);
        }
    }

    /// Apply the new committed entries to the reference. Then compare each member view with the
    /// reference at the applied index of the member. The check runs again on a member when its
    /// applied index or its base changes. A view can fail on a deleted object, but it never
    /// returns other data.
    pub(super) async fn check_views(&mut self) -> Result<(), String> {
        let reference = self.reference.as_mut().unwrap();
        while let Some(bytes) = self.committed_entries.get(&(reference.applied() + 1)) {
            let entry = Entry::decode(bytes.as_slice()).unwrap();
            let digest = reference.apply(entry).await;
            self.digests.insert(reference.applied(), digest);
        }
        self.check_loaded();
        for (&id, node) in &self.nodes {
            let point = ViewPoint::of(&node.state);
            let last = self.checked.get(&id).copied();
            if last == Some(point) {
                continue;
            }
            let Some(&expected) = self.digests.get(&point.applied) else {
                continue;
            };
            let Ok(actual) = rows_digest(node.state.read.state()).await else {
                continue;
            };
            if actual != expected {
                return Err(format!(
                    "member {id} view at {} differs from the reference",
                    point.applied
                ));
            }
            match last {
                Some(last) if last.applied == point.applied => self.base_checks += 1,
                _ => self.view_checks += 1,
            }
            self.checked.insert(id, point);
        }
        Ok(())
    }
}

/// The power test of the view check. A follower in an idle cluster switches to an older
/// checkpoint at its applied index, as if an announcement certified that checkpoint there. No
/// entry applies after the switch, so only the check on a base change can find the view that
/// lost the later writes.
#[tokio::test]
async fn the_view_check_finds_a_wrong_base_switch_in_an_idle_cluster() {
    let mut sim = Simulation::with_mode(97, true).await;
    sim.settle(4).await;
    // Some Puts attach this lease.
    sim.lease(LeaseCommand::Grant { id: 77, ttl: 3600 }).await;
    let mut writers = BTreeMap::new();
    sim.write(2).await;
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let follower = sim.follower(0);
    let (_, old, _) = sim.base(follower);
    sim.write(2).await;
    sim.publish_view(&mut writers).await;
    sim.settle(10).await;
    let (_, new, _) = sim.base(follower);
    assert!(old.is_some() && new.is_some() && new != old);
    let applied = sim.nodes[&follower].state.applied;
    assert_eq!(sim.checked[&follower].applied, applied);
    let checks = sim.base_checks;
    // From here the test runs the check, so it sees the result.
    let reference = sim.reference.take();
    let node = &sim.nodes[&follower];
    let base = node.shared_source().unwrap();
    let base = base.open_base(old.unwrap()).await.unwrap();
    node.worker.state().offer_base(base, applied);
    for _ in 0..3 {
        sim.drive().await;
    }
    let state = &sim.nodes[&follower].state;
    assert_eq!((state.applied, state.base_checkpoint), (applied, old));
    sim.reference = reference;
    let error = sim.check_views().await.unwrap_err();
    assert_eq!(
        error,
        format!("member {follower} view at {applied} differs from the reference")
    );
    assert_eq!(sim.base_checks, checks);
}
