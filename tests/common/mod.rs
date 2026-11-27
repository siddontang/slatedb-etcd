//! The object store of the integration tests: the RustFS or S3 bucket that
//! `scripts/rustfs_fixture.py` names in the environment. Run the tests through that script.

#![allow(dead_code)]

use sha2::{Digest, Sha256};
use slatedb_etcd::object::{Config, S3};

/// The cluster ID of `ServerConfig::single_node`.
const CLUSTER: &str = "slatedb-raft-poc";

/// The S3 settings of the cluster whose data directories are under `dir`. Every member and every
/// restart under `dir` gets the same prefix.
pub fn s3(dir: &std::path::Path) -> S3 {
    let bucket = std::env::var("SLATEDB_S3_TEST_BUCKET")
        .expect("the integration tests need RustFS: run them through scripts/rustfs_fixture.py");
    let endpoint = std::env::var("SLATEDB_S3_TEST_ENDPOINT").ok();
    let digest = hex::encode(Sha256::digest(dir.to_string_lossy().as_bytes()));
    S3 {
        bucket,
        region: "us-east-1".into(),
        allow_http: endpoint
            .as_deref()
            .is_some_and(|url| url.starts_with("http://")),
        endpoint,
        prefix: format!("tests/{}", &digest[..16]),
    }
}

/// The object store of the cluster under `dir`.
pub fn object_store(dir: &std::path::Path) -> Config {
    Config::S3(s3(dir))
}

/// The SlateDB admin of the shared state of the cluster under `dir`.
pub fn state_admin(dir: &std::path::Path) -> slatedb::admin::Admin {
    let s3 = s3(dir);
    let root = format!("{}/shared/state", s3.cluster_prefix(CLUSTER));
    slatedb::admin::Admin::builder(root.as_str(), s3.store().unwrap()).build()
}

/// A single-member configuration in `dir` on the object store of `dir`.
pub fn single_node(dir: impl AsRef<std::path::Path>) -> slatedb_etcd::server::ServerConfig {
    let dir = dir.as_ref();
    slatedb_etcd::server::ServerConfig::single_node(dir, object_store(dir))
}

/// The object store flags of a member process under `dir`.
pub fn cli_args(dir: &std::path::Path) -> Vec<String> {
    let s3 = s3(dir);
    let mut args = vec![
        "--s3-bucket".into(),
        s3.bucket,
        "--s3-prefix".into(),
        s3.prefix,
    ];
    if let Some(endpoint) = s3.endpoint {
        args.extend(["--s3-endpoint".into(), endpoint]);
    }
    if s3.allow_http {
        args.push("--s3-allow-http".into());
    }
    args
}

/// Return true when the shared state of the cluster under `dir` holds the object `name`, such
/// as `shared/bootstrap.json`.
pub async fn shared_object_exists(dir: &std::path::Path, name: &str) -> bool {
    use slatedb::object_store::ObjectStoreExt;
    let s3 = s3(dir);
    let path = format!("{}/{name}", s3.cluster_prefix(CLUSTER));
    s3.store().unwrap().head(&path.into()).await.is_ok()
}
