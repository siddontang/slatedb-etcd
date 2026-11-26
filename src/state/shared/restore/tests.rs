//! Shared-mode restore: the step table, the batches, the resume, and the lease takeover.

use super::*;
use crate::object::faults::{FaultStore, STALL_STATE_PUT};
use crate::state::shared::tests::{open_shared, put};
use slatedb::object_store::memory::InMemory;
use std::path::Path as FilePath;

const URL: &str = "http://127.0.0.1:2380";

fn input(rows: u64) -> Input {
    Input {
        digest: "aa".to_owned(),
        rows,
        members: "{}".to_owned(),
        alarm_member: 1,
        auth_tokens: DROP_TOKENS.to_owned(),
        transform: TRANSFORM,
    }
}

fn progress(ordinal: u64) -> Progress {
    Progress {
        ordinal,
        prefix: "bb".to_owned(),
        usage: [0; 4],
    }
}

/// The facts of a lease owner that opened its writer on an empty database.
fn owner() -> Facts {
    Facts {
        marker: None,
        claim: Some(BootstrapOp::Restore {
            snapshot: "aa".to_owned(),
        }),
        stored: true,
        input: Some(input(10)),
        lease: Lease::Ours,
        checkpoint: None,
        rows: Some(Rows::Empty),
    }
}

/// Each observation gives one step, for a member that restores and for an ordinary start.
#[test]
fn next_maps_each_observation_to_one_step() {
    let ours = input(10);
    let other = Input {
        digest: "cc".to_owned(),
        ..input(10)
    };
    let id = uuid::Uuid::from_u128(7);
    let claim = |digest: &str| {
        Some(BootstrapOp::Restore {
            snapshot: digest.to_owned(),
        })
    };
    type Case = (&'static str, Facts, Option<Step>, Step);
    let cases: Vec<Case> = vec![
        (
            "marker of this snapshot",
            Facts {
                marker: Some(Some("aa".into())),
                ..owner()
            },
            Some(Step::Done),
            Step::Done,
        ),
        (
            "marker of an ordinary start",
            Facts {
                marker: Some(None),
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::Done,
        ),
        (
            "nothing stored",
            Facts {
                claim: None,
                stored: false,
                input: None,
                lease: Lease::Absent { stale: false },
                rows: None,
                ..owner()
            },
            Some(Step::Claim),
            Step::Claim,
        ),
        (
            "state without a claim",
            Facts {
                claim: None,
                input: None,
                rows: None,
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::Claim,
        ),
        (
            "empty bootstrap claim",
            Facts {
                claim: Some(BootstrapOp::Empty),
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::Done,
        ),
        (
            "claim of another snapshot",
            Facts {
                claim: claim("cc"),
                input: Some(other.clone()),
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::Wait,
        ),
        (
            "claim without input",
            Facts {
                stored: false,
                input: None,
                lease: Lease::Absent { stale: false },
                rows: None,
                ..owner()
            },
            Some(Step::CreateInput),
            Step::Wait,
        ),
        (
            "claim without input, stale",
            Facts {
                stored: false,
                input: None,
                lease: Lease::Absent { stale: true },
                rows: None,
                ..owner()
            },
            Some(Step::CreateInput),
            Step::Pending,
        ),
        (
            "rows before the input",
            Facts {
                input: None,
                rows: None,
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::Wait,
        ),
        (
            "input of another row count",
            Facts {
                input: Some(input(11)),
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::Wait,
        ),
        (
            "input with another transform",
            Facts {
                input: Some(Input {
                    transform: TRANSFORM + 1,
                    ..input(10)
                }),
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::Wait,
        ),
        (
            "input with other members",
            Facts {
                input: Some(Input {
                    members: "[]".to_owned(),
                    alarm_member: 9,
                    ..input(10)
                }),
                ..owner()
            },
            Some(Step::Write(None)),
            Step::Wait,
        ),
        (
            "no lease",
            Facts {
                lease: Lease::Absent { stale: false },
                rows: None,
                ..owner()
            },
            Some(Step::TakeLease),
            Step::Wait,
        ),
        (
            "live lease",
            Facts {
                lease: Lease::Other { stale: false },
                rows: None,
                ..owner()
            },
            Some(Step::Wait),
            Step::Wait,
        ),
        (
            "stale lease",
            Facts {
                lease: Lease::Other { stale: true },
                rows: None,
                ..owner()
            },
            Some(Step::TakeLease),
            Step::Pending,
        ),
        (
            "lease before the writer",
            Facts {
                rows: None,
                ..owner()
            },
            Some(Step::OpenWriter),
            Step::Wait,
        ),
        ("empty rows", owner(), Some(Step::Write(None)), Step::Wait),
        (
            "partial rows",
            Facts {
                rows: Some(Rows::Partial(progress(4))),
                ..owner()
            },
            Some(Step::Write(Some(progress(4)))),
            Step::Wait,
        ),
        (
            "every row without completion",
            Facts {
                rows: Some(Rows::Partial(progress(10))),
                ..owner()
            },
            Some(Step::Write(Some(progress(10)))),
            Step::Wait,
        ),
        (
            "progress past the rows",
            Facts {
                rows: Some(Rows::Partial(progress(11))),
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::Wait,
        ),
        (
            "completion",
            Facts {
                rows: Some(Rows::Complete(11)),
                ..owner()
            },
            Some(Step::Checkpoint),
            Step::Wait,
        ),
        (
            "apply marker of another index",
            Facts {
                rows: Some(Rows::Complete(1)),
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::Wait,
        ),
        (
            "unknown rows",
            Facts {
                rows: Some(Rows::Other),
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::Wait,
        ),
        (
            "completed checkpoint",
            Facts {
                checkpoint: Some((id, 11)),
                lease: Lease::Other { stale: false },
                ..owner()
            },
            Some(Step::Mark(id)),
            Step::Mark(id),
        ),
        (
            "checkpoint before completion",
            Facts {
                checkpoint: Some((id, 0)),
                ..owner()
            },
            Some(Step::FreshPrefix),
            Step::FreshPrefix,
        ),
    ];
    for (name, facts, restoring, ordinary) in cases {
        if let Some(step) = restoring {
            assert_eq!(next(&facts, Some(&ours)), step, "{name}, restoring");
        }
        assert_eq!(next(&facts, None), ordinary, "{name}, ordinary start");
    }
}

/// A restoring member with its own source of the shared store.
#[derive(Clone)]
struct Member {
    source: Arc<Source>,
    admin: Arc<slatedb::admin::Admin>,
}

fn member(store: Arc<dyn ObjectStore>) -> Member {
    let source = offline_source(store.clone(), "cluster", StateCache::default()).unwrap();
    let admin = slatedb::admin::Admin::builder(source.state().as_str(), store).build();
    Member {
        source,
        admin: Arc::new(admin),
    }
}

impl Member {
    async fn restore(&self, restore: &Restore) -> Result<()> {
        seed(&self.source, &self.admin, restore).await
    }

    fn spawn(&self, restore: &Restore) -> tokio::task::JoinHandle<Result<()>> {
        let (member, restore) = (self.clone(), restore.clone());
        tokio::spawn(async move { member.restore(&restore).await })
    }

    async fn marker(&self) -> Option<Option<String>> {
        let marker = get_json::<serde_json::Value>(&self.source, BOOTSTRAP).await;
        marker
            .unwrap()
            .map(|(marker, _)| marker["restore"].as_str().map(str::to_owned))
    }
}

fn store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

fn checked(path: &FilePath, url: &str, hooks: Hooks) -> Restore {
    Restore::for_test(path, url, hooks)
}

fn stopping(stop: Stop) -> Hooks {
    Hooks {
        stop: std::sync::Mutex::new(Some(stop)),
        ..Default::default()
    }
}

/// The auth row of the fixture. It holds a token of the source cluster.
fn auth_row() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "enabled": false,
        "revision": 3,
        "users": {"root": {"password": "", "no_password": true, "roles": []}},
        "roles": {},
        "tokens": {"source-token": {"user": "root", "revision": 3, "expires": u64::MAX}},
    }))
    .unwrap()
}

/// Write a backup of a shared state with `keys` keys, two versions each, to `path`, and return
/// its rows. Add an auth row with a token, an alarm of another member, and a quota row.
async fn fixture(path: &FilePath, keys: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut state = open_shared(store()).await;
    for round in 0..2 {
        for n in 0..keys {
            let index = state.applied + 1;
            let value = format!("value-{round}-{n:03}-{}", "v".repeat(64));
            let request = put(&format!("key/{n:03}"), &value);
            let outcome = state.apply_etcd(index, &request).await;
            outcome.unwrap().result.unwrap();
        }
    }
    let mut stream = state.backup().await.unwrap();
    let mut image = vec![];
    while let Some(chunk) = stream.recv().await {
        image.extend(chunk.unwrap().blob);
    }
    state.close().await.unwrap();
    let data: SnapshotData = crate::codec::decode(&image[..image.len() - 32]).unwrap();
    let mut rows = data.rows;
    let alarms = vec![crate::proto::etcdserverpb::AlarmMember {
        member_id: 77,
        alarm: 1,
    }];
    rows.push((crate::auth::KEY.to_vec(), auth_row()));
    rows.push((
        crate::maintenance::ALARMS.to_vec(),
        serde_json::to_vec(&alarms).unwrap(),
    ));
    rows.push((quota::KEY.to_vec(), 1_i64.to_be_bytes().to_vec()));
    // The membership row stays last, as in every image.
    rows.sort_by(|a, b| {
        let members = |key: &[u8]| key == crate::cluster::KEY;
        (members(&a.0), &a.0).cmp(&(members(&b.0), &b.0))
    });
    backup::write_image(path, rows.clone());
    rows
}

/// The position of the first current row of a key.
fn first_key_row(rows: &[(Vec<u8>, Vec<u8>)]) -> usize {
    rows.iter()
        .position(|(key, _)| key.starts_with(crate::mvcc::CURRENT))
        .unwrap()
}

/// The restored index, the loaded fields, and every stored row of the restored state.
async fn contents(store: Arc<dyn ObjectStore>) -> (u64, String, Vec<(Vec<u8>, Vec<u8>)>) {
    let state = open_shared(store).await;
    let mut rows = vec![];
    let mut scan = state
        .applied_state()
        .scan_under(b"", &Default::default())
        .await
        .unwrap();
    while let Some(row) = scan.next().await.unwrap() {
        rows.push((row.key.to_vec(), row.value.to_vec()));
    }
    drop(scan);
    let fields = (state.applied, state.loaded_fields());
    state.close().await.unwrap();
    (fields.0, fields.1, rows)
}

/// The result of a restore of `path` by one member with flags `URL`, without interruptions.
async fn single(path: &FilePath) -> (u64, String, Vec<(Vec<u8>, Vec<u8>)>) {
    let store = store();
    let restore = checked(path, URL, Hooks::default());
    member(store.clone()).restore(&restore).await.unwrap();
    contents(store).await
}

/// A restore of many batches completes at the row count plus one, also with an MVCC revision
/// that differs. Every member that opens the state starts at that index, and it accepts a put.
/// The restore drops the quota row and the auth tokens, moves the alarms to the restored member,
/// and stores a logical size that a count of the rows confirms.
#[tokio::test]
async fn a_restore_of_many_batches_completes_at_its_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    let rows = fixture(&path, 40).await;
    let store = store();
    let restore = checked(&path, URL, Hooks::default());
    let copied: Vec<_> = rows.iter().filter(|(key, _)| !not_restored(key)).collect();
    let bytes: usize = copied
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum();
    assert!(bytes > FLUSH_BYTES * 4, "{bytes}");
    let copied = copied.len() as u64;
    assert_eq!(restore.input.rows, copied);
    let member = member(store.clone());
    member.restore(&restore).await.unwrap();
    assert_eq!(
        member.marker().await,
        Some(Some(restore.input.digest.clone()))
    );
    let index = copied + 1;
    let (seq, checkpoint) = view_checkpoints(&member.admin)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(seq, index);
    assert_eq!(checkpoint.name, Some(view_name(index)));
    let base = member.source.open_base(checkpoint.id).await.unwrap();
    assert!(base.get(PROGRESS).await.unwrap().is_none());
    assert!(base.get(quota::KEY).await.unwrap().is_none());
    let counter = base.get(LOGICAL_SIZE).await.unwrap().unwrap();
    let counter = <crate::proto::state::LogicalSize as prost::Message>::decode(counter).unwrap();
    let stored = stored_usage(&counter, index).unwrap().unwrap();
    let auth: serde_json::Value =
        serde_json::from_slice(&base.get(crate::auth::KEY).await.unwrap().unwrap()).unwrap();
    assert_eq!(auth["tokens"], serde_json::json!({}));
    assert_eq!(auth["revision"], 4);
    let alarms: Vec<crate::proto::etcdserverpb::AlarmMember> =
        serde_json::from_slice(&base.get(crate::maintenance::ALARMS).await.unwrap().unwrap())
            .unwrap();
    assert_eq!(alarms[0].member_id, 1);
    drop(base);
    let mut states = vec![];
    for _ in 0..3 {
        states.push(open_shared(store.clone()).await);
    }
    for state in &states {
        assert_eq!((state.applied, state.applied_term), (index, TERM));
        assert_ne!(state.revision as u64, index);
        let counted = maintenance::count_logical_usage(state.applied_state()).await;
        assert_eq!(counted.unwrap(), stored);
    }
    let mut state = states.pop().unwrap();
    let revision = state.revision;
    let outcome = state
        .apply_etcd(index + 1, &put("key/after", "restore"))
        .await;
    outcome.unwrap().result.unwrap();
    assert_eq!(state.revision, revision + 1);
    for state in states.into_iter().chain([state]) {
        state.close().await.unwrap();
    }
}

/// A restore stops after two durable flushes. A member with other URL text takes over the stale
/// lease, continues after the stored rows, and produces the state of an uninterrupted restore:
/// the membership row of the first member, no source token, the alarm owner, and no quota row.
#[tokio::test]
async fn a_takeover_continues_after_the_durable_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    fixture(&path, 40).await;
    let store = store();
    let first = checked(&path, URL, stopping(Stop::AfterFlush(2)));
    let error = member(store.clone()).restore(&first).await.unwrap_err();
    assert!(format!("{error:#}").contains("injected stop"), "{error:#}");
    let second = checked(&path, "http://localhost:2380/", Hooks::default());
    assert_ne!(first.input.members, second.input.members);
    let started = Instant::now();
    let member = member(store.clone());
    member.restore(&second).await.unwrap();
    assert!(
        started.elapsed() >= STALE,
        "the takeover waited for a stale lease"
    );
    let resumed = second.hooks.started_after.lock().unwrap().unwrap();
    assert!(resumed > 0 && resumed < second.input.rows, "{resumed}");
    assert_eq!(contents(store).await, single(&path).await);
}

/// A restore stops before its first flush, after its completion, or after its checkpoint. A
/// second member completes it with the state of an uninterrupted restore. Only the stop before
/// the first flush writes rows again.
#[tokio::test]
async fn a_restore_completes_after_a_stop_at_each_stage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    fixture(&path, 10).await;
    let expected = single(&path).await;
    for (stop, rewrites) in [
        (Stop::BeforeFlush, Some(0)),
        (Stop::AfterCompletion, None),
        (Stop::AfterCheckpoint, None),
    ] {
        let store = store();
        let first = checked(&path, URL, stopping(stop));
        member(store.clone()).restore(&first).await.unwrap_err();
        let member = member(store.clone());
        assert_eq!(member.marker().await, None, "{stop:?}");
        let second = checked(&path, URL, Hooks::default());
        member.restore(&second).await.unwrap();
        assert_eq!(*second.hooks.started_after.lock().unwrap(), rewrites);
        assert_eq!(contents(store).await, expected, "{stop:?}");
    }
}

/// Stored rows without the completion batch never become a bootstrap. An ordinary start waits
/// for the lease and then fails. With a checkpoint of the partial rows at the restored index, it
/// also fails. Neither writes a marker.
#[tokio::test]
async fn rows_without_their_completion_never_bootstrap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    fixture(&path, 10).await;
    let restore = checked(&path, URL, stopping(Stop::AfterFlush(1)));
    let member = member(store());
    member.restore(&restore).await.unwrap_err();
    let started = Instant::now();
    let error = resolve_bootstrap(&member.source, &member.admin)
        .await
        .unwrap_err();
    assert!(started.elapsed() >= STALE);
    assert!(format!("{error:#}").contains("pending"), "{error:#}");
    assert_eq!(member.marker().await, None);
    let db = member.source.open_writer().await.unwrap();
    assert!(db.get(PROGRESS).await.unwrap().is_some());
    let options = slatedb::config::CheckpointOptions {
        name: Some(view_name(restore.input.index())),
        ..Default::default()
    };
    let checkpoint = db
        .create_checkpoint(slatedb::config::CheckpointScope::Durable, &options)
        .await
        .unwrap();
    db.close().await.unwrap();
    let error = resolve_bootstrap(&member.source, &member.admin)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("not the restore"),
        "{error:#}"
    );
    let base = member.source.open_base(checkpoint.id).await.unwrap();
    assert!(mark_base(&member.source, &base).await.is_err());
    assert_eq!(member.marker().await, None);
}

/// A restore of another file fails into a partial restore and into a completed one.
#[tokio::test]
async fn a_restore_of_another_file_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    fixture(&path, 10).await;
    let other = dir.path().join("other");
    fixture(&other, 11).await;
    let store = store();
    let member = member(store.clone());
    let partial = checked(&path, URL, stopping(Stop::AfterFlush(1)));
    member.restore(&partial).await.unwrap_err();
    let error = member
        .restore(&checked(&other, URL, Hooks::default()))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("new cluster prefix"),
        "{error:#}"
    );
    member
        .restore(&checked(&path, URL, Hooks::default()))
        .await
        .unwrap();
    let error = member
        .restore(&checked(&other, URL, Hooks::default()))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("new cluster prefix"),
        "{error:#}"
    );
    // The same file restores again with success.
    member
        .restore(&checked(&path, URL, Hooks::default()))
        .await
        .unwrap();
}

/// The file changes after its check. The restore fails before its completion and writes no
/// marker.
#[tokio::test]
async fn a_file_that_changes_after_its_check_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    let mut rows = fixture(&path, 10).await;
    let restore = checked(&path, URL, Hooks::default());
    let changed = first_key_row(&rows);
    rows[changed].1.push(b'x');
    backup::write_image(&path, rows);
    let member = member(store());
    let error = member.restore(&restore).await.unwrap_err();
    assert!(
        format!("{error:#}").contains("snapshot changed during restore"),
        "{error:#}"
    );
    assert_eq!(member.marker().await, None);
}

/// The file changes in an early row after its check, with the same last row. The restore makes
/// rows of the changed file durable and stops. A retry with the original file does not continue
/// after those rows: the prefix digest differs.
#[tokio::test]
async fn a_resume_rejects_rows_of_a_changed_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    let original = fixture(&path, 40).await;
    let first = checked(&path, URL, stopping(Stop::AfterFlush(3)));
    let mut changed = original.clone();
    let row = first_key_row(&changed);
    *changed[row].1.last_mut().unwrap() ^= 1;
    assert_eq!(original.last(), changed.last());
    backup::write_image(&path, changed);
    let store = store();
    member(store.clone()).restore(&first).await.unwrap_err();
    backup::write_image(&path, original);
    let retry = checked(&path, URL, Hooks::default());
    assert_eq!(retry.input.digest, first.input.digest);
    let member = member(store);
    let error = member.restore(&retry).await.unwrap_err();
    assert!(
        format!("{error:#}").contains("new cluster prefix"),
        "{error:#}"
    );
    assert_eq!(member.marker().await, None);
}

/// Three members restore the same file at once. One holds the lease, and all three return.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_members_restore_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    fixture(&path, 20).await;
    let store = store();
    let restores: Vec<_> = (0..3)
        .map(|_| checked(&path, URL, Hooks::default()))
        .collect();
    let tasks: Vec<_> = restores
        .iter()
        .map(|restore| member(store.clone()).spawn(restore))
        .collect();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    let writers = restores
        .iter()
        .filter(|restore| restore.hooks.started_after.lock().unwrap().is_some())
        .count();
    assert_eq!(writers, 1);
    assert_eq!(contents(store).await, single(&path).await);
}

/// A takeover hashes the stored prefix again for longer than `STALE`. It renews the lease while
/// it hashes, so a competing member waits and does not take over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_prefix_check_keeps_the_lease() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    fixture(&path, 40).await;
    let store = store();
    let first = checked(&path, URL, stopping(Stop::AfterFlush(3)));
    member(store.clone()).restore(&first).await.unwrap_err();
    let slow = checked(
        &path,
        URL,
        Hooks {
            rehash_delay: Duration::from_millis(250),
            ..Default::default()
        },
    );
    let started = Instant::now();
    let taking = member(store.clone()).spawn(&slow);
    // Wait until the slow member took the lease, then start a competitor.
    while slow.hooks.started_after.lock().unwrap().is_none() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let competing = checked(&path, URL, Hooks::default());
    let waiting = member(store.clone()).spawn(&competing);
    taking.await.unwrap().unwrap();
    waiting.await.unwrap().unwrap();
    assert!(competing.hooks.started_after.lock().unwrap().is_none());
    assert!(started.elapsed() >= STALE * 3);
    assert_eq!(contents(store).await, single(&path).await);
}

/// The lease owner pauses just before its writer opens. Another member takes over, publishes,
/// and a writer stores a later row. The old owner then opens its writer, finds the marker, and
/// changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owner_paused_before_its_open_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    fixture(&path, 10).await;
    let store = store();
    let (reached, resume) = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    );
    let paused = checked(
        &path,
        URL,
        Hooks {
            before_open: Some((reached.clone(), resume.clone())),
            ..Default::default()
        },
    );
    let old = member(store.clone()).spawn(&paused);
    reached.notified().await;
    let successor = member(store.clone());
    successor
        .restore(&checked(&path, URL, Hooks::default()))
        .await
        .unwrap();
    let index = paused.input.index();
    let db = successor.source.open_writer().await.unwrap();
    let mut batch = slatedb::WriteBatch::new();
    batch.put(b"etcd/after", b"restore");
    batch.put(APPLIED, (index + 1).to_be_bytes());
    db.write_with_options(batch, &slatedb::config::WriteOptions { seqnum: index + 1 })
        .await
        .unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();
    resume.notify_one();
    old.await.unwrap().unwrap();
    assert!(paused.hooks.started_after.lock().unwrap().is_none());
    let db = successor.source.open_writer().await.unwrap();
    assert_eq!(db.manifest().last_l0_seq(), index + 1);
    assert_eq!(
        db.get(b"etcd/after").await.unwrap().as_deref(),
        Some(b"restore".as_slice())
    );
    db.close().await.unwrap();
    assert_eq!(
        successor.marker().await,
        Some(Some(paused.input.digest.clone()))
    );
}

/// The lease owner hangs in a file read, or in a flush while its lease store stays healthy. It
/// stops renewing, so another member takes over and completes the restore. The hung owner then
/// returns without an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_owner_loses_the_lease() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    fixture(&path, 20).await;
    let expected = single(&path).await;
    for flush in [false, true] {
        let store = store();
        let faults = Arc::new(FaultStore::new(store.clone()));
        let hung = checked(
            &path,
            URL,
            Hooks {
                stall_reads: (!flush).into(),
                batch_delay: Duration::from_millis(20),
                ..Default::default()
            },
        );
        let task = member(faults.clone()).spawn(&hung);
        while hung.hooks.started_after.lock().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if flush {
            faults.set(STALL_STATE_PUT);
        }
        let successor = checked(&path, URL, Hooks::default());
        member(store.clone()).restore(&successor).await.unwrap();
        assert!(successor.hooks.started_after.lock().unwrap().is_some());
        hung.hooks.stall_reads.store(false, Ordering::Relaxed);
        faults.set(0);
        task.await.unwrap().unwrap();
        assert_eq!(contents(store).await, expected, "flush {flush}");
    }
}

/// An ordinary start begins while a restore runs for longer than `STALE`. It waits while the
/// restore makes progress, and it opens the restored state after the completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinary_start_waits_for_a_long_restore() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    fixture(&path, 20).await;
    let store = store();
    let slow = checked(
        &path,
        URL,
        Hooks {
            batch_delay: Duration::from_millis(100),
            ..Default::default()
        },
    );
    let started = Instant::now();
    let restoring = member(store.clone()).spawn(&slow);
    while slow.hooks.started_after.lock().unwrap().is_none() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let ordinary = open_shared(store.clone()).await;
    assert!(
        started.elapsed() > STALE * 2,
        "{} rows in {:?}",
        slow.input.rows,
        started.elapsed()
    );
    restoring.await.unwrap().unwrap();
    assert_eq!(ordinary.applied, slow.input.index());
    ordinary.close().await.unwrap();
}

/// The largest row restores. A row one byte larger fails the check of the file.
#[tokio::test]
async fn a_restore_copies_the_largest_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    let key = b"etcd/large".to_vec();
    let value = vec![b'v'; backup::MAX_ROW_BYTES - 16 - key.len()];
    assert_eq!(
        backup::row_size(&key, &value).unwrap(),
        backup::MAX_ROW_BYTES as u64
    );
    backup::write_image(&path, vec![(key.clone(), value.clone())]);
    let store = store();
    member(store.clone())
        .restore(&checked(&path, URL, Hooks::default()))
        .await
        .unwrap();
    let state = open_shared(store).await;
    assert_eq!(state.applied, 2);
    let stored = state.applied_state().get(&key).await.unwrap().unwrap();
    assert_eq!(stored.len(), value.len());
    state.close().await.unwrap();
    let mut larger = value;
    larger.push(b'v');
    backup::write_image(&path, vec![(key, larger)]);
    let members = crate::cluster::Store::initial(&[(1, URL.to_owned())].into());
    let error = StateMachine::restore_shared(&path, 1, &members)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("invalid native snapshot"),
        "{error:#}"
    );
}

/// A shared restore rejects an image whose rows are out of key order.
#[tokio::test]
async fn a_restore_rejects_unordered_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    backup::write_image(
        &path,
        vec![
            (b"etcd/b".to_vec(), b"1".to_vec()),
            (b"etcd/a".to_vec(), b"2".to_vec()),
        ],
    );
    let members = crate::cluster::Store::initial(&[(1, URL.to_owned())].into());
    let error = StateMachine::restore_shared(&path, 1, &members)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unordered"), "{error:#}");
}

/// A restore into a shared state that an ordinary start created fails.
#[tokio::test]
async fn a_restore_into_an_ordinary_state_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot");
    fixture(&path, 2).await;
    let store = store();
    let member = member(store);
    let db = member.source.open_writer().await.unwrap();
    db.put(b"etcd/other", b"value").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();
    let error = member
        .restore(&checked(&path, URL, Hooks::default()))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("new cluster prefix"),
        "{error:#}"
    );
}
