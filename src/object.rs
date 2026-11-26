//! Bind each replica to a local directory or an owned S3 namespace.

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

use crate::state::{StateMachine, StateMode};

pub(crate) mod bounded;
#[cfg(test)]
pub(crate) mod faults;
pub(crate) mod local;
#[cfg(target_os = "linux")]
pub(crate) mod paced;
#[cfg(target_os = "linux")]
mod sst_reads;

#[derive(Clone, Debug, Default)]
pub enum Config {
    #[default]
    Local,
    S3(S3),
    /// A local directory that several members of one host share. Only shared state mode uses
    /// it. It serves tests and single-host experiments. It is not a production layout.
    SharedDirectory(std::path::PathBuf),
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
            return Ok(Config::Local);
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Binding {
    version: u32,
    cluster: String,
    member: u64,
    owner: String,
    s3: Option<S3>,
    /// Replicated bindings omit the field, so they keep their earlier encoding.
    #[serde(default, skip_serializing_if = "StateMode::is_replicated")]
    mode: StateMode,
}

impl Config {
    /// Open the member state. Shared mode uses `shared` for the cluster claim and its views.
    pub(crate) async fn open(
        &self,
        dir: &Path,
        cluster: &str,
        member: u64,
        cache: crate::state::StateCache,
        mode: StateMode,
        shared: &crate::state::shared::Options,
    ) -> Result<StateMachine> {
        let local = crate::state::transfer::Backend::Local;
        #[cfg(test)]
        if let Self::Test(store, path) = self {
            return match mode {
                StateMode::Replicated => {
                    StateMachine::open_store_with_cache(store.clone(), path, cache).await
                }
                StateMode::Shared => {
                    StateMachine::open_shared(store.clone(), path, cluster, cache, shared, local)
                        .await
                }
            };
        }
        if let Self::SharedDirectory(objects) = self {
            ensure!(
                mode == StateMode::Shared,
                "a shared directory needs shared state mode"
            );
            let binding = bind(dir, cluster, member, None, mode)?;
            let store: Arc<dyn ObjectStore> = Arc::new(local_store(objects)?);
            let prefix = format!("member-{member}");
            let quota =
                claim_shared(store.as_ref(), "", &prefix, cluster, &binding, shared).await?;
            let shared = crate::state::shared::Options {
                quota,
                ..shared.clone()
            };
            return StateMachine::open_shared(store, "", cluster, cache, &shared, local).await;
        }
        let s3 = match self {
            Self::S3(s3) => {
                let mut s3 = s3.clone();
                s3.validate()?;
                Some(s3)
            }
            #[cfg(test)]
            Self::TestS3(_, s3) => Some(s3.clone()),
            _ => None,
        };
        let binding = bind(dir, cluster, member, s3, mode)?;
        let Some(s3) = &binding.s3 else {
            let objects = dir.join("objects");
            return match mode {
                StateMode::Replicated => StateMachine::open_with_cache(&objects, cache).await,
                // A member-local store serves one voter. Several members need a shared store.
                StateMode::Shared => {
                    ensure!(
                        shared.voters.len() == 1 && !shared.join,
                        "several shared-mode members need a shared object store"
                    );
                    let store = Arc::new(local_store(&objects)?);
                    StateMachine::open_shared(store, "", cluster, cache, shared, local).await
                }
            };
        };
        let store = self.s3_store(s3)?;
        let prefix = s3.member_prefix(cluster, member);
        let backend = crate::state::transfer::Backend::S3 {
            bucket: s3.bucket.clone(),
            endpoint: s3.endpoint.clone(),
            region: s3.region.clone(),
        };
        let root = s3.cluster_prefix(cluster);
        match mode {
            StateMode::Replicated => {
                claim_replicated(store.as_ref(), &root, &prefix, cluster, &binding).await?;
                StateMachine::open_backend(store, &format!("{prefix}/state"), cache, backend, None)
                    .await
            }
            StateMode::Shared => {
                let quota =
                    claim_shared(store.as_ref(), &root, &prefix, cluster, &binding, shared).await?;
                let shared = crate::state::shared::Options {
                    quota,
                    ..shared.clone()
                };
                StateMachine::open_shared(store, &root, cluster, cache, &shared, backend).await
            }
        }
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

/// A shared-mode store in a local directory. Its calls have a deadline and a limit in flight.
fn local_store(objects: &Path) -> Result<bounded::Bounded<local::LocalStore>> {
    Ok(bounded::Bounded::new(local::LocalStore::open(objects)?))
}

/// Read the object store binding of a data directory.
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

/// The S3 settings of a replicated member directory of `cluster`, or of a member that a
/// conversion already changed to shared mode.
pub(crate) fn converted_s3(dir: &Path, cluster: &str, member: u64) -> Result<(S3, StateMode)> {
    let binding = read_binding(dir)?;
    ensure!(
        binding.version == 1 && binding.cluster == cluster && binding.member == member,
        "the data directory belongs to another cluster or member"
    );
    let s3 = binding
        .s3
        .context("only a member with S3 state can be converted")?;
    Ok((s3, binding.mode))
}

/// Change the binding of a stopped replicated member to shared mode.
///
/// The member writes its shared binding claim first, then its local binding, and then deletes
/// its replicated owner claim. Each step can run again after a crash. A shared member refuses to
/// start while any member namespace still has a replicated owner claim, so the cluster starts
/// only after every member is converted.
pub(crate) async fn convert_binding(
    dir: &Path,
    store: &dyn ObjectStore,
    cluster: &str,
    member: u64,
) -> Result<()> {
    let mut binding = read_binding(dir)?;
    let s3 = binding.s3.clone().context("the member has no S3 state")?;
    let prefix = s3.member_prefix(cluster, member);
    binding.mode = StateMode::Shared;
    claim(store, &prefix, BINDING, &binding).await?;
    write_binding(dir, &binding)?;
    match store.delete(&format!("{prefix}/{OWNER}").into()).await {
        Ok(()) | Err(slatedb::object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Claim the cluster mode and the member namespace of a replicated member.
async fn claim_replicated(
    store: &dyn ObjectStore,
    root: &str,
    prefix: &str,
    cluster: &str,
    binding: &Binding,
) -> Result<()> {
    crate::state::shared::claim_mode(store, root, cluster, StateMode::Replicated).await?;
    claim(store, prefix, OWNER, binding).await
}

/// Claim the cluster mode, the member binding, and the shared state before the writer opens.
///
/// A replicated member namespace from before the mode claim marks a replicated cluster. A start
/// that loses any claim writes no state, so the cluster that won stays usable.
async fn claim_shared(
    store: &dyn ObjectStore,
    root: &str,
    prefix: &str,
    cluster: &str,
    binding: &Binding,
    shared: &crate::state::shared::Options,
) -> Result<u64> {
    ensure!(
        !crate::state::shared::has_replicated_member(store, root).await?,
        "the cluster uses Replicated state mode"
    );
    // A joining member is not an initial voter. It checks the existing cluster before it writes
    // anything, so a failed join leaves an empty namespace empty.
    let joined = if shared.join {
        Some(crate::state::shared::claimed_quota(store, root, cluster).await?)
    } else {
        None
    };
    crate::state::shared::claim_mode(store, root, cluster, StateMode::Shared).await?;
    // A shared-mode member keeps no state of its own. Its binding records the mode.
    claim(store, prefix, BINDING, binding).await?;
    if let Some(quota) = joined {
        return Ok(quota);
    }
    crate::state::shared::claim(store, root, cluster, shared.quota, &shared.voters).await
}

/// The claim of a replicated member namespace.
const OWNER: &str = "owner.json";
/// The binding of a shared-mode member. It holds no state.
const BINDING: &str = "binding.json";

fn bind(
    dir: &Path,
    cluster: &str,
    member: u64,
    s3: Option<S3>,
    mode: StateMode,
) -> Result<Binding> {
    let path = dir.join("object-store.json");
    if path.exists() {
        let binding: Binding = serde_json::from_slice(&std::fs::read(&path)?)?;
        ensure!(
            binding.mode == mode,
            "the data directory uses {:?} state mode",
            binding.mode
        );
        ensure!(
            binding.version == 1
                && binding.cluster == cluster
                && binding.member == member
                && binding.s3 == s3,
            "object store binding differs from the data directory"
        );
        return Ok(binding);
    }
    // Existing local objects cannot be converted by changing a startup option.
    ensure!(
        (s3.is_none() && mode.is_replicated()) || !dir.join("objects").exists(),
        "local state requires migration"
    );
    let binding = Binding {
        version: 1,
        cluster: cluster.to_owned(),
        member,
        owner: uuid::Uuid::new_v4().to_string(),
        s3,
        mode,
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

#[cfg(test)]
mod tests {
    use super::*;
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
        let replicated = StateMode::Replicated;
        let binding = bind(first.path(), "cluster", 1, Some(config.clone()), replicated).unwrap();
        claim(&store, &prefix, OWNER, &binding).await.unwrap();
        let restarted = bind(first.path(), "cluster", 1, Some(config.clone()), replicated).unwrap();
        assert_eq!(restarted, binding);
        claim(&store, &prefix, OWNER, &restarted).await.unwrap();
        let other = bind(second.path(), "cluster", 1, Some(config), replicated).unwrap();
        assert!(claim(&store, &prefix, OWNER, &other).await.is_err());
        claim(&store, "different-prefix", OWNER, &other)
            .await
            .unwrap();
    }

    #[test]
    fn binding_rejects_backend_identity_and_namespace_changes() {
        let dir = tempfile::tempdir().unwrap();
        let mode = StateMode::Replicated;
        bind(dir.path(), "cluster", 1, Some(s3()), mode).unwrap();
        assert!(bind(dir.path(), "cluster", 1, None, mode).is_err());
        assert!(bind(dir.path(), "other-cluster", 1, Some(s3()), mode).is_err());
        assert!(bind(dir.path(), "cluster", 2, Some(s3()), mode).is_err());
        let error = bind(dir.path(), "cluster", 1, Some(s3()), StateMode::Shared).unwrap_err();
        assert!(
            error.to_string().contains("Replicated state mode"),
            "{error}"
        );
        let mut changed = s3();
        changed.bucket = "other-bucket".into();
        assert!(bind(dir.path(), "cluster", 1, Some(changed), mode).is_err());
        let old = tempfile::tempdir().unwrap();
        std::fs::create_dir(old.path().join("objects")).unwrap();
        assert!(bind(old.path(), "cluster", 1, Some(s3()), mode).is_err());
        assert!(bind(old.path(), "cluster", 1, None, StateMode::Shared).is_err());
        bind(old.path(), "cluster", 1, None, mode).unwrap();
        // A replicated binding keeps the encoding of earlier versions.
        let encoded = std::fs::read_to_string(dir.path().join("object-store.json")).unwrap();
        assert!(!encoded.contains("mode"));
    }

    /// Run the claims of a start. Each member keeps one data directory under `dirs`.
    async fn start(
        store: &InMemory,
        dirs: &Path,
        member: u64,
        mode: StateMode,
        voters: &[u64],
    ) -> Result<()> {
        let config = s3();
        let dir = dirs.join(member.to_string());
        let binding = bind(&dir, "cluster", member, Some(config.clone()), mode).unwrap();
        let root = config.cluster_prefix("cluster");
        let prefix = config.member_prefix("cluster", member);
        match mode {
            StateMode::Replicated => {
                claim_replicated(store, &root, &prefix, "cluster", &binding).await
            }
            StateMode::Shared => {
                let options = crate::state::shared::Options {
                    quota: 0,
                    voters: voters.to_vec(),
                    join: false,
                    empty_log: true,
                    pin_max: Duration::from_secs(300),
                    overlay_max_bytes: 1 << 30,
                    restore: None,
                    record: None,
                };
                claim_shared(store, &root, &prefix, "cluster", &binding, &options)
                    .await
                    .map(drop)
            }
        }
    }

    #[tokio::test]
    async fn one_cluster_namespace_has_one_mode_and_one_set_of_initial_voters() {
        // A replicated cluster from before the mode claim refuses a shared member, and its
        // own member still restarts.
        let dirs = tempfile::tempdir().unwrap();
        let store = InMemory::new();
        let config = s3();
        let dir = tempfile::tempdir().unwrap();
        let legacy = bind(
            dir.path(),
            "cluster",
            1,
            Some(config.clone()),
            StateMode::Replicated,
        )
        .unwrap();
        claim(&store, &config.member_prefix("cluster", 1), OWNER, &legacy)
            .await
            .unwrap();
        let error = start(&store, dirs.path(), 2, StateMode::Shared, &[2])
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("Replicated state mode"),
            "{error}"
        );
        let root = config.cluster_prefix("cluster");
        claim_replicated(
            &store,
            &root,
            &config.member_prefix("cluster", 1),
            "cluster",
            &legacy,
        )
        .await
        .unwrap();
        assert!(start(&store, dirs.path(), 3, StateMode::Shared, &[3])
            .await
            .is_err());

        // Concurrent first starts: exactly one wins, in either mode and with other voters.
        for (first, second) in [
            (
                (1, StateMode::Shared, vec![1]),
                (2, StateMode::Shared, vec![2]),
            ),
            (
                (1, StateMode::Shared, vec![1]),
                (2, StateMode::Replicated, vec![2]),
            ),
            (
                (1, StateMode::Replicated, vec![1]),
                (2, StateMode::Shared, vec![1, 2]),
            ),
        ] {
            let store = InMemory::new();
            let dirs = tempfile::tempdir().unwrap();
            let (a, b) = tokio::join!(
                start(&store, dirs.path(), first.0, first.1, &first.2),
                start(&store, dirs.path(), second.0, second.1, &second.2)
            );
            assert_eq!(u8::from(a.is_ok()) + u8::from(b.is_ok()), 1, "{a:?} {b:?}");
            // The winner restarts. The loser keeps failing.
            let (winner, loser) = if a.is_ok() {
                (first, second)
            } else {
                (second, first)
            };
            start(&store, dirs.path(), winner.0, winner.1, &winner.2)
                .await
                .unwrap();
            assert!(start(&store, dirs.path(), loser.0, loser.1, &loser.2)
                .await
                .is_err());
        }
        // A failed join writes nothing, so a replicated bootstrap still succeeds.
        let dirs = tempfile::tempdir().unwrap();
        let store = InMemory::new();
        let config = s3();
        let binding = bind(
            &dirs.path().join("4"),
            "cluster",
            4,
            Some(config.clone()),
            StateMode::Shared,
        )
        .unwrap();
        let joining = crate::state::shared::Options {
            quota: 0,
            voters: vec![],
            join: true,
            empty_log: true,
            pin_max: Duration::from_secs(300),
            overlay_max_bytes: 1 << 30,
            restore: None,
            record: None,
        };
        let root = config.cluster_prefix("cluster");
        let prefix = config.member_prefix("cluster", 4);
        assert!(
            claim_shared(&store, &root, &prefix, "cluster", &binding, &joining)
                .await
                .is_err()
        );
        assert!(store.list(None).next().await.is_none());
        start(&store, dirs.path(), 1, StateMode::Replicated, &[1])
            .await
            .unwrap();
        // A shared cluster accepts its bootstrap voters in any order.
        let dirs = tempfile::tempdir().unwrap();
        let store = InMemory::new();
        start(&store, dirs.path(), 1, StateMode::Shared, &[1, 2])
            .await
            .unwrap();
        start(&store, dirs.path(), 2, StateMode::Shared, &[2, 1])
            .await
            .unwrap();
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
    /// guide grants exactly these. Every member reads `mode.json`, the shared claims, the shared
    /// state, and the owner claims of all members. A voter writes its own binding and the shared
    /// keys. A joining learner writes only its own binding.
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
                };
                config
                    .open(
                        &dir,
                        "cluster",
                        member,
                        crate::state::StateCache::default(),
                        StateMode::Shared,
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
            written.iter().all(|key| key == "mode.json"
                || key == "member-1/binding.json"
                || key.starts_with("shared/")),
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
                read.iter().all(|key| key == "mode.json"
                    || key.starts_with("shared/")
                    || key.starts_with(&own)
                    || (key.starts_with("member-") && key.ends_with("/owner.json"))),
                "member {member} read {read:?}"
            );
        }
    }
}
