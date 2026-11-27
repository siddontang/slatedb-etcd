//! Bind each member to an owned namespace of an S3-compatible object store.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::{ClientOptions, ObjectStore, ObjectStoreExt, PutMode, RetryConfig};

use crate::state::StateMachine;

#[cfg(test)]
pub(crate) mod faults;

/// The object store of the shared state. Every cluster needs one, a single member too.
#[derive(Clone, Debug)]
pub enum Config {
    S3(S3),
    #[cfg(test)]
    Test(Arc<dyn ObjectStore>, String),
    /// The S3 layout on another store, for tests.
    #[cfg(test)]
    TestS3(Arc<dyn ObjectStore>, S3),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3 {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub prefix: String,
    pub allow_http: bool,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Args {
    /// S3 bucket for state objects. The local Raft log remains required.
    #[arg(long)]
    pub s3_bucket: Option<String>,
    /// S3 region. Credentials use AWS environment variables or workload identity.
    #[arg(long, default_value = "us-east-1")]
    pub s3_region: String,
    /// S3 service endpoint. Omit this option for AWS S3.
    #[arg(long, requires = "s3_bucket")]
    pub s3_endpoint: Option<String>,
    /// Object namespace root. Each cluster and member gets a separate child prefix.
    #[arg(long, default_value = "slatedb-etcd")]
    pub s3_prefix: String,
    /// Allow HTTP to an isolated S3 test service.
    #[arg(long, requires = "s3_bucket")]
    pub s3_allow_http: bool,
}

impl Args {
    pub fn config(self) -> Result<Config> {
        let Some(bucket) = self.s3_bucket else {
            anyhow::bail!(
                "an object store is required: set --s3-bucket to a bucket of S3 or another \
                 S3-compatible store, such as RustFS"
            );
        };
        let mut config = S3 {
            bucket,
            region: self.s3_region,
            endpoint: self.s3_endpoint,
            prefix: self.s3_prefix,
            allow_http: self.s3_allow_http,
        };
        config.validate()?;
        Ok(Config::S3(config))
    }
}

impl S3 {
    pub fn validate(&mut self) -> Result<()> {
        ensure!(
            !self.bucket.is_empty()
                && self
                    .bucket
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b".-".contains(&c)),
            "invalid S3 bucket"
        );
        ensure!(
            !self.region.is_empty()
                && self
                    .region
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-'),
            "invalid S3 region"
        );
        ensure!(
            !self.prefix.is_empty()
                && self
                    .prefix
                    .split('/')
                    .all(|p| !p.is_empty() && p != "." && p != "..")
                && self
                    .prefix
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_/.".contains(&c)),
            "invalid S3 prefix"
        );
        if let Some(endpoint) = &mut self.endpoint {
            let url = reqwest::Url::parse(endpoint).context("invalid S3 endpoint")?;
            ensure!(
                (url.scheme() == "https" || (self.allow_http && url.scheme() == "http"))
                    && url.host_str().is_some()
                    && url.path() == "/"
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "S3 endpoint must be an origin URL; HTTP requires --s3-allow-http"
            );
            *endpoint = url.as_str().trim_end_matches('/').to_owned();
        }
        Ok(())
    }

    pub fn member_prefix(&self, cluster: &str, member: u64) -> String {
        format!("{}/member-{member}", self.cluster_prefix(cluster))
    }

    /// The namespace of one cluster. Shared state mode keeps its database here.
    pub fn cluster_prefix(&self, cluster: &str) -> String {
        format!(
            "{}/{}",
            self.prefix,
            hex::encode(Sha256::digest(cluster.as_bytes()))
        )
    }

    pub fn store(&self) -> Result<Arc<dyn ObjectStore>> {
        // Read credential settings only. Endpoint and TLS policy come from validated options.
        let mut builder = AmazonS3Builder::new();
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_ROLE_ARN",
            "AWS_ROLE_SESSION_NAME",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        ] {
            if let Ok(value) = std::env::var(name) {
                builder = builder.with_config(name.to_ascii_lowercase().parse()?, value);
            }
        }
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        Ok(Arc::new(
            builder
                .with_bucket_name(&self.bucket)
                .with_region(&self.region)
                .with_client_options(
                    ClientOptions::new()
                        .with_allow_http(self.allow_http)
                        .with_connect_timeout(Duration::from_secs(3))
                        .with_timeout(Duration::from_secs(10)),
                )
                .with_retry(RetryConfig {
                    max_retries: 2,
                    retry_timeout: Duration::from_secs(15),
                    ..Default::default()
                })
                .build()?,
        ))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Binding {
    version: u32,
    cluster: String,
    member: u64,
    owner: String,
    s3: Option<S3>,
    /// Always `shared`. A binding of replicated state mode has no mode.
    #[serde(default)]
    mode: Option<Mode>,
    /// Shared mode: the store ID of the cluster claim at the first open. Only the local binding
    /// holds it. A member refuses a store with another ID, because that store was replaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    store_id: Option<uuid::Uuid>,
}

/// The state mode of a binding. Shared state mode is the only one.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Shared,
}

/// The error of a data directory from replicated state mode.
const REPLICATED: &str = "replicated state mode is not supported";

impl Config {
    /// Open the member state. `shared` holds the options of the cluster claim and its views.
    pub(crate) async fn open(
        &self,
        dir: &Path,
        cluster: &str,
        member: u64,
        cache: crate::state::StateCache,
        shared: &crate::state::shared::Options,
    ) -> Result<StateMachine> {
        let s3 = match self {
            Self::S3(s3) => {
                let mut s3 = s3.clone();
                s3.validate()?;
                s3
            }
            #[cfg(test)]
            Self::Test(store, path) => {
                return StateMachine::open_shared(store.clone(), path, cluster, cache, shared).await
            }
            #[cfg(test)]
            Self::TestS3(_, s3) => s3.clone(),
        };
        let binding = bind(dir, cluster, member, Some(s3.clone()))?;
        let store = self.s3_store(&s3)?;
        let prefix = s3.member_prefix(cluster, member);
        let root = s3.cluster_prefix(cluster);
        let claimed =
            claim_shared(store.as_ref(), &root, &prefix, cluster, &binding, shared).await?;
        let shared = bind_store(dir, binding, claimed, shared).await?;
        StateMachine::open_shared(store, &root, cluster, cache, &shared).await
    }
}

impl Config {
    /// The object store of an S3 configuration.
    pub(crate) fn s3_store(&self, s3: &S3) -> Result<Arc<dyn ObjectStore>> {
        #[cfg(test)]
        if let Self::TestS3(store, _) = self {
            return Ok(store.clone());
        }
        s3.store()
    }
}

/// Read the object store binding of a data directory.
#[cfg(test)]
fn read_binding(dir: &Path) -> Result<Binding> {
    let path = dir.join("object-store.json");
    ensure!(
        path.exists(),
        "the data directory has no object store binding"
    );
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

/// Write the binding of a data directory atomically.
fn write_binding(dir: &Path, binding: &Binding) -> Result<()> {
    let temporary = dir.join("object-store.json.pending");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(binding)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, dir.join("object-store.json"))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Claim the member binding and the shared state before the writer opens. A start that loses any
/// claim writes no state, so the cluster that won stays usable.
async fn claim_shared(
    store: &dyn ObjectStore,
    root: &str,
    prefix: &str,
    cluster: &str,
    binding: &Binding,
    shared: &crate::state::shared::Options,
) -> Result<crate::state::shared::Claimed> {
    // A bound data directory only reads the claim, so a replaced store stays unchanged. A joining
    // member is not an initial voter. It checks the existing cluster before it writes anything,
    // so a failed join leaves an empty namespace empty.
    let voters = (!shared.join).then_some(shared.voters.as_slice());
    let read = match binding.store_id {
        Some(id) => Some(crate::state::shared::bound(store, root, cluster, id, voters).await?),
        None if shared.join => Some(crate::state::shared::joined(store, root, cluster).await?),
        None => None,
    };
    // A member keeps no state of its own in its namespace. Its binding records the member.
    claim(store, prefix, BINDING, binding).await?;
    if let Some(claimed) = read {
        return Ok(claimed);
    }
    crate::state::shared::claim(store, root, cluster, shared.quota, &shared.voters).await
}

/// Record the store ID of the claim in the local binding at the first open. Return the options
/// of the state open. The open reads the claim again, read only, and refuses another ID.
async fn bind_store(
    dir: &Path,
    mut binding: Binding,
    claimed: crate::state::shared::Claimed,
    shared: &crate::state::shared::Options,
) -> Result<crate::state::shared::Options> {
    if binding.store_id.is_none() {
        binding.store_id = Some(claimed.store_id);
        write_binding(dir, &binding)?;
    }
    #[cfg(test)]
    tests::before_state_open().await;
    Ok(crate::state::shared::Options {
        quota: claimed.quota,
        store_id: binding.store_id,
        ..shared.clone()
    })
}

/// The binding of a member. It holds no state.
const BINDING: &str = "binding.json";

fn bind(dir: &Path, cluster: &str, member: u64, s3: Option<S3>) -> Result<Binding> {
    let path = dir.join("object-store.json");
    if path.exists() {
        let binding: Binding = serde_json::from_slice(&std::fs::read(&path)?)?;
        ensure!(binding.mode == Some(Mode::Shared), REPLICATED);
        ensure!(
            binding.version == 1
                && binding.cluster == cluster
                && binding.member == member
                && binding.s3 == s3,
            "object store binding differs from the data directory"
        );
        return Ok(binding);
    }
    // Local objects without a binding come from replicated state mode.
    ensure!(!dir.join("objects").exists(), REPLICATED);
    let binding = Binding {
        version: 1,
        cluster: cluster.to_owned(),
        member,
        owner: uuid::Uuid::new_v4().to_string(),
        s3,
        mode: Some(Mode::Shared),
        store_id: None,
    };
    std::fs::create_dir_all(dir)?;
    let temporary = dir.join("object-store.json.pending");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(&binding)?)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    File::open(dir)?.sync_all()?;
    Ok(binding)
}

async fn claim(store: &dyn ObjectStore, prefix: &str, name: &str, binding: &Binding) -> Result<()> {
    // The remote claim comes before the cluster claim, so it never holds the store ID.
    let binding = &Binding {
        store_id: None,
        ..binding.clone()
    };
    let path = format!("{prefix}/{name}").into();
    let data = serde_json::to_vec(binding)?;
    match store
        .put_opts(&path, data.into(), PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(()),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            let existing = store.get(&path).await?.bytes().await?;
            let owner: Binding = serde_json::from_slice(&existing)?;
            ensure!(
                owner == *binding,
                "S3 namespace belongs to another data directory"
            );
            Ok(())
        }
        Err(error) => Err(error).context("cannot claim S3 namespace"),
    }
}

/// The in-memory store of the test member in `dir`. A restart in the same directory opens the
/// same store.
#[cfg(test)]
pub(crate) fn test_store(dir: &Path) -> Arc<dyn ObjectStore> {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};
    static STORES: LazyLock<Mutex<HashMap<std::path::PathBuf, Arc<dyn ObjectStore>>>> =
        LazyLock::new(Mutex::default);
    let mut stores = STORES.lock().unwrap();
    let store = stores.entry(dir.to_owned()).or_insert_with(|| {
        Arc::new(slatedb::object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>
    });
    store.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    thread_local! {
        /// A pause between the binding check and the state open. The open signals the first
        /// `Notify` and waits for the second. A current-thread test runtime keeps it on the
        /// thread of the test.
        static BEFORE_STATE_OPEN: std::cell::RefCell<
            Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
        > = Default::default();
    }

    pub(super) async fn before_state_open() {
        let Some((reached, resume)) = BEFORE_STATE_OPEN.with(|hook| hook.take()) else {
            return;
        };
        reached.notify_one();
        resume.notified().await;
    }

    /// Open member 1 in `dir` with the S3 layout on `store`, close it, and return the store ID
    /// that it sends to its peers.
    async fn open_member(store: Arc<dyn ObjectStore>, dir: &Path) -> Result<uuid::Uuid> {
        let options = crate::state::shared::Options {
            quota: 0,
            voters: vec![1],
            join: false,
            empty_log: true,
            pin_max: Duration::from_secs(300),
            overlay_max_bytes: 1 << 30,
            restore: None,
            record: None,
            store_id: None,
        };
        let state = Config::TestS3(store, s3())
            .open(
                dir,
                "cluster",
                1,
                crate::state::StateCache::default(),
                &options,
            )
            .await?;
        let id = state.store_id();
        state.close().await?;
        Ok(id)
    }
    use futures::StreamExt;
    use slatedb::object_store::memory::InMemory;

    fn s3() -> S3 {
        S3 {
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://127.0.0.1:9000".into()),
            prefix: "tests".into(),
            allow_http: true,
        }
    }

    #[tokio::test]
    async fn namespace_claim_survives_restart_and_rejects_another_owner() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let store = InMemory::new();
        let config = s3();
        let prefix = config.member_prefix("cluster", 1);
        let binding = bind(first.path(), "cluster", 1, Some(config.clone())).unwrap();
        claim(&store, &prefix, BINDING, &binding).await.unwrap();
        let restarted = bind(first.path(), "cluster", 1, Some(config.clone())).unwrap();
        assert_eq!(restarted, binding);
        claim(&store, &prefix, BINDING, &restarted).await.unwrap();
        let other = bind(second.path(), "cluster", 1, Some(config)).unwrap();
        assert!(claim(&store, &prefix, BINDING, &other).await.is_err());
        claim(&store, "different-prefix", BINDING, &other)
            .await
            .unwrap();
    }

    #[test]
    fn binding_rejects_backend_identity_and_namespace_changes() {
        let dir = tempfile::tempdir().unwrap();
        bind(dir.path(), "cluster", 1, Some(s3())).unwrap();
        assert!(bind(dir.path(), "cluster", 1, None).is_err());
        assert!(bind(dir.path(), "other-cluster", 1, Some(s3())).is_err());
        assert!(bind(dir.path(), "cluster", 2, Some(s3())).is_err());
        let mut changed = s3();
        changed.bucket = "other-bucket".into();
        assert!(bind(dir.path(), "cluster", 1, Some(changed)).is_err());
        // A data directory of replicated state mode gets one short refusal.
        let old = tempfile::tempdir().unwrap();
        std::fs::create_dir(old.path().join("objects")).unwrap();
        let error = bind(old.path(), "cluster", 1, Some(s3())).unwrap_err();
        assert_eq!(error.to_string(), REPLICATED);
        let replicated = tempfile::tempdir().unwrap();
        let mut binding = serde_json::to_value(read_binding(dir.path()).unwrap()).unwrap();
        binding.as_object_mut().unwrap().remove("mode");
        let path = replicated.path().join("object-store.json");
        std::fs::write(path, serde_json::to_vec(&binding).unwrap()).unwrap();
        let error = bind(replicated.path(), "cluster", 1, Some(s3())).unwrap_err();
        assert_eq!(error.to_string(), REPLICATED);
    }

    /// Run the claims of a start. Each member keeps one data directory under `dirs`.
    async fn start(store: &InMemory, dirs: &Path, member: u64, voters: &[u64]) -> Result<()> {
        let config = s3();
        let dir = dirs.join(member.to_string());
        let binding = bind(&dir, "cluster", member, Some(config.clone())).unwrap();
        let root = config.cluster_prefix("cluster");
        let prefix = config.member_prefix("cluster", member);
        let options = crate::state::shared::Options {
            quota: 0,
            voters: voters.to_vec(),
            join: false,
            empty_log: true,
            pin_max: Duration::from_secs(300),
            overlay_max_bytes: 1 << 30,
            restore: None,
            record: None,
            store_id: None,
        };
        claim_shared(store, &root, &prefix, "cluster", &binding, &options)
            .await
            .map(drop)
    }

    #[tokio::test]
    async fn one_cluster_namespace_has_one_set_of_initial_voters() {
        // Concurrent first starts with other voters: exactly one wins.
        let store = InMemory::new();
        let dirs = tempfile::tempdir().unwrap();
        let (a, b) = tokio::join!(
            start(&store, dirs.path(), 1, &[1]),
            start(&store, dirs.path(), 2, &[2])
        );
        assert_eq!(u8::from(a.is_ok()) + u8::from(b.is_ok()), 1, "{a:?} {b:?}");
        // The winner restarts. The loser keeps failing.
        let (winner, loser) = if a.is_ok() { (1, 2) } else { (2, 1) };
        start(&store, dirs.path(), winner, &[winner]).await.unwrap();
        assert!(start(&store, dirs.path(), loser, &[loser]).await.is_err());
        // A failed join writes nothing, so a bootstrap still succeeds.
        let dirs = tempfile::tempdir().unwrap();
        let store = InMemory::new();
        let config = s3();
        let binding = bind(&dirs.path().join("4"), "cluster", 4, Some(config.clone())).unwrap();
        let joining = crate::state::shared::Options {
            quota: 0,
            voters: vec![],
            join: true,
            empty_log: true,
            pin_max: Duration::from_secs(300),
            overlay_max_bytes: 1 << 30,
            restore: None,
            record: None,
            store_id: None,
        };
        let root = config.cluster_prefix("cluster");
        let prefix = config.member_prefix("cluster", 4);
        assert!(
            claim_shared(&store, &root, &prefix, "cluster", &binding, &joining)
                .await
                .is_err()
        );
        assert!(store.list(None).next().await.is_none());
        start(&store, dirs.path(), 1, &[1]).await.unwrap();
        // A shared cluster accepts its bootstrap voters in any order.
        let dirs = tempfile::tempdir().unwrap();
        let store = InMemory::new();
        start(&store, dirs.path(), 1, &[1, 2]).await.unwrap();
        start(&store, dirs.path(), 2, &[2, 1]).await.unwrap();
    }

    #[test]
    fn namespace_isolates_members_and_untrusted_cluster_names() {
        let config = s3();
        assert_ne!(config.member_prefix("a", 1), config.member_prefix("a", 2));
        assert_ne!(config.member_prefix("a", 1), config.member_prefix("b", 1));
        assert!(!config.member_prefix("../../secret", 1).contains(".."));
        for endpoint in [
            "http://host",
            "https://user:secret@host",
            "https://host/path",
            "https://host?token=secret",
            "https://host#fragment",
        ] {
            let mut config = s3();
            config.allow_http = false;
            config.endpoint = Some(endpoint.into());
            assert!(config.validate().is_err());
        }
        for prefix in ["", "/absolute", "a//b", "a/../b", "a/./b", "a%2fb"] {
            let mut config = s3();
            config.prefix = prefix.into();
            assert!(config.validate().is_err());
        }
    }

    /// The object keys that shared members touch at startup. The S3 policy in the shared state
    /// guide grants exactly these. Every member reads the shared claims and the shared state. A
    /// voter writes its own binding and the shared keys. A joining learner writes only its own
    /// binding.
    #[tokio::test]
    async fn shared_members_touch_the_keys_of_the_guide_policy() {
        use crate::object::faults::FaultStore;
        let faults = Arc::new(FaultStore::new(Arc::new(InMemory::new())));
        let config = Config::TestS3(faults.clone(), s3());
        let root = format!("{}/", s3().cluster_prefix("cluster"));
        let dirs = tempfile::tempdir().unwrap();
        let open = |member: u64, join: bool| {
            let config = config.clone();
            let dir = dirs.path().join(member.to_string());
            async move {
                let options = crate::state::shared::Options {
                    quota: 0,
                    voters: vec![1, 2],
                    join,
                    empty_log: true,
                    pin_max: Duration::from_secs(300),
                    overlay_max_bytes: 1 << 30,
                    restore: None,
                    record: None,
                    store_id: None,
                };
                config
                    .open(
                        &dir,
                        "cluster",
                        member,
                        crate::state::StateCache::default(),
                        &options,
                    )
                    .await
                    .unwrap()
                    .close()
                    .await
                    .unwrap();
            }
        };
        let keys = |set: &std::sync::Mutex<std::collections::BTreeSet<String>>| {
            let keys: Vec<String> = std::mem::take(&mut *set.lock().unwrap())
                .into_iter()
                .map(|key| key.strip_prefix(&root).expect("a cluster key").to_owned())
                .collect();
            keys
        };
        open(1, false).await;
        let written = keys(&faults.written);
        assert!(
            written
                .iter()
                .all(|key| key == "member-1/binding.json" || key.starts_with("shared/")),
            "{written:?}"
        );
        keys(&faults.read);
        for (member, join) in [(2, false), (3, true)] {
            open(member, join).await;
            let written = keys(&faults.written);
            let own = format!("member-{member}/");
            assert!(
                written
                    .iter()
                    .all(|key| key.starts_with(&own) || (!join && key.starts_with("shared/"))),
                "member {member} wrote {written:?}"
            );
            let read = keys(&faults.read);
            assert!(
                read.iter()
                    .all(|key| key.starts_with("shared/") || key.starts_with(&own)),
                "member {member} read {read:?}"
            );
        }
    }

    /// A member records the store ID of the cluster claim in its local binding. The remote
    /// binding does not hold it, so an unchanged restart opens. A store at the same location with
    /// another claim, or with no claim, was replaced. The member refuses it before any write.
    #[tokio::test]
    async fn a_restart_refuses_a_replaced_shared_store() {
        use futures::StreamExt;
        let dirs = tempfile::tempdir().unwrap();
        let dir = dirs.path().join("1");
        let open = |store: Arc<dyn ObjectStore>| open_member(store, &dir);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sent = open(store.clone()).await.unwrap();
        let id = read_binding(&dir).unwrap().store_id.unwrap();
        assert_eq!(sent, id);
        assert_eq!(open(store.clone()).await.unwrap(), id);
        assert_eq!(read_binding(&dir).unwrap().store_id, Some(id));

        let root = s3().cluster_prefix("cluster");
        store
            .delete(&format!("{root}/shared/owner.json").into())
            .await
            .unwrap();
        let claimed = crate::state::shared::claim(store.as_ref(), &root, "cluster", 0, &[1])
            .await
            .unwrap();
        assert_ne!(claimed.store_id, id);
        let error = open(store).await.unwrap_err();
        assert!(format!("{error:#}").contains("replaced"), "{error:#}");

        let empty: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let error = open(empty.clone()).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("no cluster claim"),
            "{error:#}"
        );
        assert!(empty.list(None).next().await.is_none());
        assert_eq!(read_binding(&dir).unwrap().store_id, Some(id));
    }

    /// The claim changes between the binding check and the state open. The open reads the claim
    /// once more, only reads it, and refuses another store ID or a missing claim. It does not
    /// create the claim again.
    #[tokio::test]
    async fn the_state_open_refuses_a_claim_that_changed_after_the_binding_check() {
        let root = s3().cluster_prefix("cluster");
        let claim_path: slatedb::object_store::path::Path =
            format!("{root}/shared/owner.json").into();
        for replace in [true, false] {
            let dirs = tempfile::tempdir().unwrap();
            let dir = dirs.path().join("1");
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let id = open_member(store.clone(), &dir).await.unwrap();
            let reached = Arc::new(tokio::sync::Notify::new());
            let resume = Arc::new(tokio::sync::Notify::new());
            let hook = Some((reached.clone(), resume.clone()));
            BEFORE_STATE_OPEN.with(|cell| *cell.borrow_mut() = hook);
            let change = async {
                reached.notified().await;
                store.delete(&claim_path).await.unwrap();
                let mut replacement = None;
                if replace {
                    let claimed =
                        crate::state::shared::claim(store.as_ref(), &root, "cluster", 0, &[1]);
                    replacement = Some(claimed.await.unwrap().store_id);
                }
                resume.notify_one();
                replacement
            };
            let (opened, replacement) = tokio::join!(open_member(store.clone(), &dir), change);
            let error = opened.unwrap_err();
            assert!(format!("{error:#}").contains("replaced"), "{error:#}");
            assert_eq!(read_binding(&dir).unwrap().store_id, Some(id));
            let claimed = crate::state::shared::claimed(store.as_ref(), &root, "cluster").await;
            assert_eq!(
                claimed.unwrap().map(|claimed| claimed.store_id),
                replacement
            );
        }
    }
}
