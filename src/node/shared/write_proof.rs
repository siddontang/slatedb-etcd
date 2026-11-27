//! The write proof of a learner before its promotion. See "Promotion" in
//! `docs/shared-state.md`.
//!
//! A learner can use read-only credentials, but a voter can become the writer. So before the
//! leader proposes a promotion, the learner creates the scratch object
//! `shared/state/.probe/<member>-<process>` with `PutMode::Create` and deletes it. It uses the
//! store client of its own writer. SlateDB 0.18.1 writes each manifest version with
//! `PutMode::Create` (`slatedb-txn-obj-0.18.1/src/object_store.rs:383`), so a create and a
//! delete show the access that the writer needs. The nonce is only for the freshness check of
//! the leader.
//!
//! The leader binds each probe to its term, the member, and the process of the member, with a
//! random nonce. It accepts only the reply to that nonce, from that process and from the same
//! shared store, before the reply deadline. A proof counts for `PROOF_LIFETIME` from the send of
//! its nonce, so a late or repeated reply cannot extend it.

use super::*;
use serde::{Deserialize, Serialize};
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};

/// The peer route that runs a write probe on a member.
pub(crate) const WRITE_PROBE_ROUTE: &str = "/raft/write-probe";
/// The prefix of the scratch objects under the shared state.
const SCRATCH: &str = ".probe";
/// The leader accepts the reply to a probe for this time after it sent the probe.
pub(super) const REPLY_WAIT: Duration = Duration::from_secs(15);
/// A proof counts for this time after the leader sent its probe.
pub(super) const PROOF_LIFETIME: Duration = Duration::from_secs(60);
/// The longest time of one probe on the member, below `REPLY_WAIT`.
const PROBE_TIME: Duration = Duration::from_secs(10);
/// The first wait after a failed probe. It doubles up to `RETRY_CAP`, so a learner with
/// read-only access does not get a probe on each promotion request.
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_CAP: Duration = Duration::from_secs(30);

/// The body of a write probe.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct WriteProbeRequest {
    /// The member that the leader probes.
    pub member: u64,
    pub nonce: u64,
}

/// The reply of a member to a write probe.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteProbeReply {
    /// The nonce of the probe.
    pub nonce: u64,
    /// The process of the member. See `crate::peer::process_id`.
    pub process: u64,
    /// The store ID of the member's shared store.
    pub store: Option<String>,
    /// The error of a failed probe. `None` when the create and the delete succeeded.
    pub error: Option<String>,
}

/// The write probes of this member, on its own writer's store client.
///
/// Each process of the member uses one scratch key. A create that times out can still reach the
/// store later, also after a later probe. It then lands on the same key, so this process leaves
/// at most one scratch object. The next create finds it and deletes it first.
pub(crate) struct Prober {
    member: u64,
    source: Arc<Source>,
    /// Only one probe runs at a time.
    running: tokio::sync::Mutex<()>,
}

impl Prober {
    pub(in crate::node) fn new(member: u64, source: Arc<Source>) -> Self {
        Self {
            member,
            source,
            running: Default::default(),
        }
    }

    /// The member that this prober belongs to.
    pub(crate) fn member(&self) -> u64 {
        self.member
    }

    /// The scratch key of this process.
    fn path(&self) -> Path {
        let name = format!("{}-{}", self.member, crate::peer::process_id());
        Path::from(format!("{}/{SCRATCH}/{name}", self.source.state()))
    }

    /// Create and delete the scratch object, in at most `PROBE_TIME`.
    pub(crate) async fn probe(&self) -> Result<()> {
        tokio::time::timeout(PROBE_TIME, self.run())
            .await
            .map_err(|_| anyhow::anyhow!("the write probe timed out"))?
    }

    async fn run(&self) -> Result<()> {
        let _running = self.running.lock().await;
        let path = self.path();
        let created = || format!("cannot create {path}");
        match self.create(&path).await {
            // An earlier probe of this process left its object: its delete failed, or its
            // create landed late. Delete it and create once more.
            Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
                self.delete(&path)
                    .await
                    .with_context(|| format!("cannot delete {path} of an earlier write probe"))?;
                self.create(&path).await.with_context(created)?;
            }
            result => result.with_context(created)?,
        }
        // This delete proves the delete access. A refused delete fails the probe, also when the
        // object is gone.
        self.delete(&path)
            .await
            .with_context(|| format!("cannot delete {path}"))
    }

    async fn create(&self, path: &Path) -> slatedb::object_store::Result<()> {
        let payload = PutPayload::from_static(b"write probe\n");
        let options = PutOptions::from(PutMode::Create);
        self.source.store.put_opts(path, payload, options).await?;
        Ok(())
    }

    async fn delete(&self, path: &Path) -> slatedb::object_store::Result<()> {
        match self.source.store.delete(path).await {
            Err(slatedb::object_store::Error::NotFound { .. }) => Ok(()),
            result => result,
        }
    }
}

/// The write proofs that the leader holds in its term.
#[derive(Default)]
pub(in crate::node) struct Proofs {
    term: u64,
    members: BTreeMap<u64, Record>,
}

/// The write proof of one process of a member.
#[derive(Default)]
struct Record {
    process: u64,
    /// The nonce and the send time of the probe that runs.
    pending: Option<(u64, Instant)>,
    /// The send time of the probe of the last proof.
    proven: Option<Instant>,
    /// The error of the last failed probe.
    failed: Option<String>,
    failures: u32,
    /// No new probe starts before this time.
    retry_at: Option<Instant>,
}

impl Record {
    fn fail(&mut self, now: Instant, error: String) {
        self.proven = None;
        self.failed = Some(error);
        self.failures = self.failures.saturating_add(1);
        let wait = RETRY_FIRST.saturating_mul(1 << (self.failures - 1).min(5));
        self.retry_at = Some(now + wait.min(RETRY_CAP));
    }
}

/// A refused promotion: the error text, and the nonce of a new probe for the leader to send.
#[derive(Debug)]
pub(super) struct Refusal {
    pub message: String,
    pub probe: Option<u64>,
}

/// The reply to a probe, as the leader received it.
#[derive(Debug)]
pub(super) struct Replied {
    pub nonce: u64,
    /// The process that replied. `None` when no reply came.
    pub process: Option<u64>,
    pub outcome: std::result::Result<(), String>,
}

impl Proofs {
    /// The record of process `process` of `member` in `term`. A new term drops every record,
    /// and a new process drops the record of its member.
    fn record(&mut self, term: u64, member: u64, process: u64) -> &mut Record {
        if self.term != term {
            self.term = term;
            self.members.clear();
        }
        let record = self.members.entry(member).or_default();
        if record.process != process {
            *record = Record {
                process,
                ..Default::default()
            };
        }
        record
    }

    /// Check the write proof of `member`, whose current process is `process`, in `term` at
    /// `now`. Without a valid proof, start one probe unless one runs or a failure backs off.
    pub(super) fn check(
        &mut self,
        now: Instant,
        term: u64,
        member: u64,
        process: Option<u64>,
    ) -> std::result::Result<(), Refusal> {
        let refuse = |message: String, probe| Err(Refusal { message, probe });
        let Some(process) = process else {
            return refuse(
                format!("member {member} has no known process yet, so it has no write proof"),
                None,
            );
        };
        let record = self.record(term, member, process);
        if record
            .proven
            .is_some_and(|sent| now < sent + PROOF_LIFETIME)
        {
            return Ok(());
        }
        if let Some((_, sent)) = record.pending {
            if now < sent + REPLY_WAIT {
                return refuse(
                    format!("the write probe of member {member} runs; retry the promotion"),
                    None,
                );
            }
            record.pending = None;
            record.fail(now, "the write probe got no reply in time".into());
        }
        let state = match (&record.failed, record.proven) {
            (Some(error), _) => format!("failed: {error}"),
            (None, Some(_)) => "is expired".to_owned(),
            (None, None) => "is missing".to_owned(),
        };
        if let Some(at) = record.retry_at.filter(|at| now < *at) {
            let wait = at.duration_since(now).as_secs_f64().ceil();
            return refuse(
                format!(
                    "the write proof of member {member} {state}; retry the promotion in \
                     {wait} s"
                ),
                None,
            );
        }
        let nonce = uuid::Uuid::new_v4().as_u64_pair().0;
        record.pending = Some((nonce, now));
        refuse(
            format!(
                "the write proof of member {member} {state}; a write probe started, retry the \
                 promotion"
            ),
            Some(nonce),
        )
    }

    /// Record `replied` for `member`, whose current process is `process`, in `term` at `now`.
    /// Return true when it counts: it answers the running probe of that process in this term,
    /// before the reply deadline.
    pub(super) fn complete(
        &mut self,
        now: Instant,
        term: u64,
        member: u64,
        process: Option<u64>,
        replied: Replied,
    ) -> bool {
        let Some(process) = process else {
            return false;
        };
        let record = self.record(term, member, process);
        let Some((nonce, sent)) = record.pending else {
            return false;
        };
        if nonce != replied.nonce || now >= sent + REPLY_WAIT {
            return false;
        }
        record.pending = None;
        // Another process of the member ran the probe. It proves nothing about this one.
        if replied.process.is_some_and(|replier| replier != process) {
            return false;
        }
        match replied.outcome {
            Ok(()) => {
                record.proven = Some(sent);
                record.failed = None;
                record.failures = 0;
                record.retry_at = None;
            }
            Err(error) => record.fail(now, error),
        }
        true
    }

    /// Drop the proof and the running probe of `member`, whose process changed.
    pub(in crate::node) fn forget(&mut self, member: u64) {
        self.members.remove(&member);
    }

    /// The proofs for `/status`. They are diagnostic: a handoff does not use them.
    pub(super) fn status(&self, now: Instant) -> Value {
        let members: Vec<_> = self
            .members
            .iter()
            .map(|(member, record)| {
                json!({
                    "member": member,
                    "process": record.process,
                    "proven": record.proven.is_some_and(|sent| now < sent + PROOF_LIFETIME),
                    "proof_age_ms": record.proven.map(|sent| (now - sent).as_millis() as u64),
                    "probing": record.pending.is_some(),
                    "failed": record.failed,
                    "failures": record.failures,
                })
            })
            .collect();
        json!({"term": self.term, "members": members})
    }

    /// Tests: the nonce of the running probe of `member`.
    #[cfg(test)]
    pub(super) fn pending(&self, member: u64) -> Option<u64> {
        self.members.get(&member)?.pending.map(|(nonce, _)| nonce)
    }
}

impl Node {
    /// Shared mode: refuse the promotion of a learner without a valid write proof. The refusal
    /// is retryable. It names a missing, failed, or expired proof, and it can start a probe.
    pub(in crate::node) fn check_write_proof(
        &mut self,
        request: &crate::cluster::Command,
    ) -> std::result::Result<(), KvError> {
        let crate::cluster::Command::Promote(member) = *request else {
            return Ok(());
        };
        #[cfg(test)]
        if self.skip_write_proof {
            return Ok(());
        }
        let process = self.raw.store().process(member);
        let term = self.raw.raft.term;
        let Some(writer) = &mut self.writer else {
            return Ok(());
        };
        let refusal = match writer.proofs.check(Instant::now(), term, member, process) {
            Ok(()) => return Ok(()),
            Err(refusal) => refusal,
        };
        if let Some(nonce) = refusal.probe {
            self.send_write_probe(member, nonce);
        }
        Err(KvError::new(
            tonic::Code::FailedPrecondition,
            &refusal.message,
        ))
    }

    /// Send the write probe `nonce` to `member`. Its reply comes back as `Event::WriteProbed`.
    /// A manual transport has no peer routes, so the test gives the reply itself.
    fn send_write_probe(&self, member: u64, nonce: u64) {
        if self.manual_transport {
            return;
        }
        let Some(address) = self
            .state
            .members
            .members
            .get(&member)
            .and_then(|member| member.peer_ur_ls.first())
        else {
            return;
        };
        let url = format!("{}{WRITE_PROBE_ROUTE}", crate::cluster::url(address));
        let request = self.peer.headers(self.options.transport.http.post(url));
        let events = self.events.clone();
        tokio::spawn(async move {
            let replied = async {
                let body = WriteProbeRequest { member, nonce };
                let response = request.timeout(REPLY_WAIT).json(&body).send().await?;
                response.error_for_status()?.json::<WriteProbeReply>().await
            };
            let reply = replied.await.map_err(|error| error.to_string());
            let _ = events
                .send(Event::WriteProbed {
                    member,
                    nonce,
                    reply,
                })
                .await;
        });
    }

    /// Record the reply of `member` to the write probe `nonce`.
    pub(in crate::node) fn write_probed(
        &mut self,
        member: u64,
        nonce: u64,
        reply: std::result::Result<WriteProbeReply, String>,
    ) {
        let replied = match reply {
            Ok(reply) if reply.nonce != nonce => return,
            Ok(reply) if reply.store != self.peer.store => Replied {
                nonce,
                process: Some(reply.process),
                outcome: Err(format!(
                    "the member uses the shared store {:?}",
                    reply.store
                )),
            },
            Ok(reply) => Replied {
                nonce,
                process: Some(reply.process),
                outcome: reply.error.map_or(Ok(()), Err),
            },
            Err(error) => Replied {
                nonce,
                process: None,
                outcome: Err(error),
            },
        };
        let process = self.raw.store().process(member);
        let term = self.raw.raft.term;
        let Some(writer) = &mut self.writer else {
            return;
        };
        let failure = replied.outcome.clone().err();
        if !writer
            .proofs
            .complete(Instant::now(), term, member, process, replied)
        {
            return;
        }
        match failure {
            None => tracing::info!(
                member,
                "the learner proved write access to the shared state"
            ),
            Some(error) => tracing::warn!(
                member,
                %error,
                "the learner has no write access to the shared state"
            ),
        }
    }
}

#[cfg(test)]
#[path = "write_proof_tests.rs"]
mod tests;
