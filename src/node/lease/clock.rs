//! Keep the in-memory lease deadlines of the leader and the Expire proposals that wait to apply.

use std::collections::{btree_map, BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::lease::LeaseRecord;

type Leases = Arc<BTreeMap<i64, LeaseRecord>>;

/// The lease deadlines of one leader term.
///
/// Followers keep no deadlines. A leader builds them from the applied lease set, and extends
/// each inherited lease once per term. Each deadline holds the grant generation, so an Expire
/// proposal names the grant that it removes. `Node` gives the Raft term and role, and the
/// applied leases, to each method.
pub(in crate::node) struct LeaseClock {
    /// The Raft term of the deadlines.
    term: u64,
    /// The generation and deadline of each lease.
    deadlines: BTreeMap<i64, (u64, Instant)>,
    /// The Expire proposals of this term that did not apply.
    expiring: BTreeSet<(i64, u64)>,
    /// The lease set that the deadlines follow.
    view: Option<Leases>,
    /// Whether this leader term has built its lease deadlines.
    promoted: bool,
}

/// The leases past their deadline that have no Expire proposal. See `LeaseClock::due`.
pub(in crate::node) struct Due<'a> {
    deadlines: btree_map::Iter<'a, i64, (u64, Instant)>,
    expiring: &'a mut BTreeSet<(i64, u64)>,
    now: Instant,
}

impl LeaseClock {
    pub(in crate::node) fn new() -> Self {
        Self {
            term: 0,
            deadlines: BTreeMap::new(),
            expiring: BTreeSet::new(),
            view: None,
            promoted: false,
        }
    }

    /// Clear the deadlines when the term changes or this member is not the leader. Return
    /// whether the leader must call `rebuild` because the lease set changed.
    pub(in crate::node) fn sync_term(&mut self, term: u64, leader: bool, leases: &Leases) -> bool {
        if !leader || self.term != term {
            self.deadlines.clear();
            self.expiring.clear();
            self.view = None;
            self.promoted = false;
            self.term = term;
        }
        if !leader {
            return false;
        }
        // The clock holds the previous map, so apply must copy it before a change.
        // An equal pointer therefore proves that the lease set did not change.
        !self
            .view
            .as_ref()
            .is_some_and(|view| Arc::ptr_eq(view, leases))
    }

    /// Make the deadlines follow the lease set. The first build of a term adds `extend` to each
    /// lease, like etcd's Promote.
    pub(in crate::node) fn rebuild(&mut self, leases: &Leases, extend: Duration) {
        self.deadlines.retain(|id, _| leases.contains_key(id));
        let now = Instant::now();
        let extend = if self.promoted {
            Duration::ZERO
        } else {
            extend
        };
        self.promoted = true;
        for (&id, lease) in leases.iter() {
            let entry = self.deadlines.entry(id).or_insert((0, now));
            if entry.0 != lease.generation {
                *entry = (
                    lease.generation,
                    now + Duration::from_secs(lease.ttl as u64) + extend,
                );
            }
        }
        self.view = Some(leases.clone());
    }

    /// The leases past their deadline at `now` that have no Expire proposal, in id order.
    pub(in crate::node) fn due(&mut self, now: Instant) -> Due<'_> {
        Due {
            deadlines: self.deadlines.iter(),
            expiring: &mut self.expiring,
            now,
        }
    }

    /// The Expire entry of this lease generation applied, so it no longer waits.
    pub(in crate::node) fn expired(&mut self, id: i64, generation: u64) {
        self.expiring.remove(&(id, generation));
    }

    /// The whole seconds before the deadline of a lease. `None` when this leader has no
    /// deadline for it.
    pub(in crate::node) fn remaining_secs(&self, id: i64) -> Option<i64> {
        let (_, deadline) = self.deadlines.get(&id)?;
        Some(deadline.saturating_duration_since(Instant::now()).as_secs() as i64)
    }

    /// Move the deadline of a lease to `ttl` seconds from now. A lease past its deadline is not
    /// renewed, so an Expire proposal from this term stays valid. Return whether it renewed.
    pub(in crate::node) fn renew(&mut self, id: i64, ttl: i64) -> bool {
        let Some((_, deadline)) = self.deadlines.get_mut(&id) else {
            return false;
        };
        let now = Instant::now();
        if *deadline <= now {
            return false;
        }
        *deadline = now + Duration::from_secs(ttl as u64);
        true
    }
}

impl Due<'_> {
    /// Record the Expire proposal of a due lease, so the next tick does not propose it again.
    pub(in crate::node) fn proposed(&mut self, id: i64, generation: u64) {
        self.expiring.insert((id, generation));
    }
}

impl Iterator for Due<'_> {
    type Item = (i64, u64);

    fn next(&mut self) -> Option<(i64, u64)> {
        for (&id, &(generation, deadline)) in self.deadlines.by_ref() {
            if deadline > self.now || self.expiring.contains(&(id, generation)) {
                continue;
            }
            return Some((id, generation));
        }
        None
    }
}

#[cfg(test)]
impl LeaseClock {
    /// The deadline of a lease. It panics when the lease has none.
    pub(in crate::node) fn deadline(&self, id: i64) -> Instant {
        self.deadlines[&id].1
    }

    pub(in crate::node) fn set_deadline(&mut self, id: i64, deadline: Instant) {
        self.deadlines.get_mut(&id).unwrap().1 = deadline;
    }

    /// The number of Expire proposals that did not apply.
    pub(in crate::node) fn expiring(&self) -> usize {
        self.expiring.len()
    }
}
