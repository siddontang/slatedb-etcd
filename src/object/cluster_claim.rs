//! The cluster claim of the shared state: the cluster, its quota, its initial voters, and the
//! store ID. The first member creates it, and every member checks it. `state::shared` reads its
//! result when it opens the state.

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, ObjectStoreExt, PutMode};

const CLAIM: &str = "shared/owner.json";

/// The path of the claim under `root`, or at the top for an empty `root`.
fn path(root: &str) -> Path {
    if root.is_empty() {
        Path::from(CLAIM)
    } else {
        Path::from(format!("{root}/{CLAIM}"))
    }
}

/// The cluster claim that a member asks for at its start.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Request<'a> {
    /// The quota of a new claim. An existing claim sets the cluster quota.
    pub quota: u64,
    /// The initial voters of a new claim. Every bootstrap member must name the same set.
    pub voters: &'a [u64],
    /// The member joins after MemberAdd. It never creates the claim, and it skips the voter check.
    pub join: bool,
}

/// The cluster claim. The first member creates it, and every member checks it.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Claim {
    version: u32,
    cluster: String,
    /// The logical quota of the cluster. Zero disables it. Every member applies this value.
    quota: u64,
    /// The initial voters. Members with another bootstrap set refuse to open the writer.
    voters: Vec<u64>,
    /// The random ID of this shared store. Members send it with their peer traffic, so members
    /// on another bucket or prefix refuse each other.
    store_id: uuid::Uuid,
}

/// The values of a checked cluster claim.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Claimed {
    pub quota: u64,
    pub store_id: uuid::Uuid,
}

/// Parse a cluster claim. A claim without a store ID does not parse.
fn parse_claim(data: &[u8]) -> Result<Claim> {
    let claim: Claim =
        serde_json::from_slice(data).context("invalid cluster claim, or no valid store ID")?;
    ensure!(
        !claim.store_id.is_nil(),
        "the cluster claim has a nil store ID"
    );
    Ok(claim)
}

/// Read and check an existing claim. Return `None` when the store has no claim.
async fn read_claim(store: &dyn ObjectStore, root: &str, cluster: &str) -> Result<Option<Claim>> {
    let data = match store.get(&path(root)).await {
        Ok(result) => result.bytes().await?,
        Err(slatedb::object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let existing = parse_claim(&data)?;
    ensure!(
        existing.version == 1 && existing.cluster == cluster,
        "the shared state belongs to another cluster"
    );
    Ok(Some(existing))
}

/// Read and check an existing claim. Return `None` when the store has no claim.
pub(crate) async fn claimed(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
) -> Result<Option<Claimed>> {
    Ok(read_claim(store, root, cluster)
        .await?
        .map(|existing| Claimed {
            quota: existing.quota,
            store_id: existing.store_id,
        }))
}

/// Read the claim of the store that a data directory is bound to. Refuse a claim with another
/// store ID, and for an initial voter, a claim with other voters.
///
/// This never creates the claim. A claim that disappears during a start stays absent, and the
/// start fails.
pub(crate) async fn bound(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
    store_id: uuid::Uuid,
    voters: Option<&[u64]>,
) -> Result<Claimed> {
    let existing = read_claim(store, root, cluster).await?;
    let found = existing.as_ref().map(|claim| claim.store_id);
    let Some(existing) = existing.filter(|claim| claim.store_id == store_id) else {
        bail!(
            "the data directory belongs to shared store {store_id}, but the store at this \
             location has {}: the store was replaced. Use the original store, or a new data \
             directory",
            found.map_or("no cluster claim".to_owned(), |id| format!("store ID {id}"))
        );
    };
    if let Some(voters) = voters {
        let mut voters = voters.to_vec();
        voters.sort_unstable();
        ensure!(
            existing.voters == voters,
            "the shared state belongs to other initial voters {:?}",
            existing.voters
        );
    }
    Ok(Claimed {
        quota: existing.quota,
        store_id,
    })
}

/// Read the claim for a joining member. It is not one of the initial voters, so it never creates
/// the claim.
pub(crate) async fn joined(store: &dyn ObjectStore, root: &str, cluster: &str) -> Result<Claimed> {
    claimed(store, root, cluster)
        .await?
        .context("a joining member needs an existing shared cluster")
}

/// Create or check the shared state claim under `root`. Return the cluster quota and the store
/// ID.
///
/// The claim holds the initial voters. Two members that each bootstrap a separate one-voter
/// cluster in the same namespace race on one `PutMode::Create`, so only one opens the writer.
/// The member that creates the claim also picks the store ID.
pub(crate) async fn claim(
    store: &dyn ObjectStore,
    root: &str,
    cluster: &str,
    quota: u64,
    voters: &[u64],
) -> Result<Claimed> {
    let path = path(root);
    let mut voters = voters.to_vec();
    voters.sort_unstable();
    let claim = Claim {
        version: 1,
        cluster: cluster.to_owned(),
        quota,
        voters,
        store_id: uuid::Uuid::new_v4(),
    };
    match store
        .put_opts(
            &path,
            serde_json::to_vec(&claim)?.into(),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) => Ok(Claimed {
            quota,
            store_id: claim.store_id,
        }),
        Err(slatedb::object_store::Error::AlreadyExists { .. }) => {
            let existing = parse_claim(&store.get(&path).await?.bytes().await?)?;
            ensure!(
                existing.version == 1 && existing.cluster == cluster,
                "the shared state belongs to another cluster"
            );
            ensure!(
                existing.voters == claim.voters,
                "the shared state belongs to other initial voters {:?}",
                existing.voters
            );
            if existing.quota != quota {
                tracing::warn!(
                    configured = quota,
                    cluster = existing.quota,
                    "the cluster claim sets the quota"
                );
            }
            Ok(Claimed {
                quota: existing.quota,
                store_id: existing.store_id,
            })
        }
        Err(error) => Err(error).context("cannot claim the shared state"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn the_cluster_claim_fixes_the_quota() {
        let store = InMemory::new();
        assert_eq!(
            claim(&store, "root", "one", 5, &[1]).await.unwrap().quota,
            5
        );
        // A later member uses the claimed quota.
        assert_eq!(
            claim(&store, "root", "one", 7, &[1]).await.unwrap().quota,
            5
        );
        assert!(claim(&store, "root", "two", 5, &[1]).await.is_err());
        // A separate one-voter bootstrap in the same namespace refuses before it opens the writer.
        assert!(claim(&store, "root", "one", 5, &[2]).await.is_err());
    }

    /// Concurrent first voters create one claim, so they get one store ID. A later voter and a
    /// reader get the same ID. Another prefix gets another ID.
    #[tokio::test]
    async fn the_cluster_claim_holds_one_store_id() {
        let store = InMemory::new();
        assert_eq!(claimed(&store, "root", "test").await.unwrap(), None);
        let (first, second) = tokio::join!(
            claim(&store, "root", "test", 0, &[1]),
            claim(&store, "root", "test", 0, &[1])
        );
        let id = first.unwrap().store_id;
        assert_eq!(second.unwrap().store_id, id);
        let claimed = claimed(&store, "root", "test").await.unwrap();
        assert_eq!(claimed.unwrap().store_id, id);
        let other = claim(&store, "other", "test", 0, &[1]).await.unwrap();
        assert_ne!(other.store_id, id);
    }

    /// A claim with no store ID, or with an invalid or nil ID, is refused.
    #[tokio::test]
    async fn a_claim_without_a_valid_store_id_is_refused() {
        let claim = |store_id: Option<&str>| {
            let mut claim = serde_json::json!({
                "version": 1, "cluster": "test", "mode": "shared", "quota": 0, "voters": [1],
            });
            if let Some(id) = store_id {
                claim["store_id"] = id.into();
            }
            serde_json::to_vec(&claim).unwrap()
        };
        let nil = uuid::Uuid::nil().to_string();
        for store_id in [None, Some(""), Some("not-a-uuid"), Some(nil.as_str())] {
            let store = InMemory::new();
            store
                .put(&"root/shared/owner.json".into(), claim(store_id).into())
                .await
                .unwrap();
            let error = super::claim(&store, "root", "test", 0, &[1])
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains("store ID"), "{error:#}");
            assert!(claimed(&store, "root", "test").await.is_err());
        }
    }
}
