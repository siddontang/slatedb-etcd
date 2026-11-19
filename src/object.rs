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

use crate::state::StateMachine;

#[cfg(test)]
pub(crate) mod faults;

#[derive(Clone, Debug, Default)]
pub enum Config {
    #[default]
    Local,
    S3(S3),
    #[cfg(test)]
    Test(Arc<dyn ObjectStore>, String),
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
        format!(
            "{}/{}/member-{member}",
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
}

impl Config {
    pub async fn open(&self, dir: &Path, cluster: &str, member: u64) -> Result<StateMachine> {
        #[cfg(test)]
        if let Self::Test(store, path) = self {
            return StateMachine::open_store(store.clone(), path).await;
        }
        let s3 = match self {
            Self::S3(s3) => {
                let mut s3 = s3.clone();
                s3.validate()?;
                Some(s3)
            }
            _ => None,
        };
        let binding = bind(dir, cluster, member, s3)?;
        let Some(s3) = &binding.s3 else {
            return StateMachine::open(&dir.join("objects")).await;
        };
        let store = s3.store()?;
        let prefix = s3.member_prefix(cluster, member);
        claim(store.as_ref(), &prefix, &binding).await?;
        StateMachine::open_store(store, &format!("{prefix}/state")).await
    }
}

fn bind(dir: &Path, cluster: &str, member: u64, s3: Option<S3>) -> Result<Binding> {
    let path = dir.join("object-store.json");
    if path.exists() {
        let binding: Binding = serde_json::from_slice(&std::fs::read(&path)?)?;
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
        s3.is_none() || !dir.join("objects").exists(),
        "local state requires migration"
    );
    let binding = Binding {
        version: 1,
        cluster: cluster.to_owned(),
        member,
        owner: uuid::Uuid::new_v4().to_string(),
        s3,
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

async fn claim(store: &dyn ObjectStore, prefix: &str, binding: &Binding) -> Result<()> {
    let path = format!("{prefix}/owner.json").into();
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
        claim(&store, &prefix, &binding).await.unwrap();
        let restarted = bind(first.path(), "cluster", 1, Some(config.clone())).unwrap();
        assert_eq!(restarted, binding);
        claim(&store, &prefix, &restarted).await.unwrap();
        let other = bind(second.path(), "cluster", 1, Some(config)).unwrap();
        assert!(claim(&store, &prefix, &other).await.is_err());
        claim(&store, "different-prefix", &other).await.unwrap();
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
        let old = tempfile::tempdir().unwrap();
        std::fs::create_dir(old.path().join("objects")).unwrap();
        assert!(bind(old.path(), "cluster", 1, Some(s3())).is_err());
        bind(old.path(), "cluster", 1, None).unwrap();
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
}
