//! Classify the errors that a read or a write of the stored state meets.
//!
//! This module uses no other module of the crate, so `mvcc`, `state`, and `node` all sit above
//! it. A client read, apply,
//! and the shared writer use the same rule. The writer adds its own cases, such as a fence, on
//! top of it. See "Failure cases" in `docs/shared-state.md`.
//!
//! A stored error never stops the member. Only an error of the member's own invariants does.

use std::sync::LazyLock;

/// The class of a storage error.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum StorageFault {
    /// The object store failed or timed out after its retries. The same call can succeed later.
    Transient,
    /// A checkpoint object is gone, or a read held its view too long. A read on a newer view
    /// can succeed.
    Missing,
    /// A stored object or row does not decode, or it contradicts another one. Each member that
    /// reads the same data meets the same error.
    Corrupt,
}

/// A stored object of the shared state that does not decode, or that contradicts another one.
/// Each writer reads the same objects, so the error describes the shared store, not the applied
/// state of one member. A writer task halts on it instead of stopping the member.
#[derive(Debug)]
pub(crate) struct StoredData(String);

impl std::fmt::Display for StoredData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StoredData {}

/// A `StoredData` error with `message`.
pub(crate) fn stored(message: impl std::fmt::Display) -> anyhow::Error {
    anyhow::Error::new(StoredData(message.to_string()))
}

/// The source of a read that held its view longer than the pin limit.
#[derive(Debug)]
pub(crate) struct Expired;

impl std::fmt::Display for Expired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the read view expired")
    }
}

impl std::error::Error for Expired {}

/// A classified storage error that crossed a `KvError`. Apply turns a read error back into
/// this type, so the class survives the text of the message.
#[derive(Debug)]
pub(crate) struct Classified {
    pub fault: StorageFault,
    pub message: String,
}

impl std::fmt::Display for Classified {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Classified {}

/// Classify `error`, or return `None` when it is not a storage error.
///
/// The outermost classified cause decides. So a missing object that SlateDB reports as a data
/// error is not a transient error. An object store error with no SlateDB error around it comes
/// from this crate's own storage calls. The bounded store reports its deadline that way, so a
/// timeout is transient too.
pub(crate) fn classify(error: &anyhow::Error) -> Option<StorageFault> {
    error.chain().find_map(cause)
}

/// Classify one cause without its sources, except for a SlateDB error, whose kind decides.
fn cause(error: &(dyn std::error::Error + 'static)) -> Option<StorageFault> {
    if let Some(classified) = error.downcast_ref::<Classified>() {
        return Some(classified.fault);
    }
    if error.is::<StoredData>() || error.is::<prost::DecodeError>() {
        return Some(StorageFault::Corrupt);
    }
    if error.is::<Expired>() {
        return Some(StorageFault::Missing);
    }
    if let Some(error) = error.downcast_ref::<slatedb::Error>() {
        return Some(of_slatedb(error));
    }
    if object_store_error(error).is_some() {
        return Some(StorageFault::Transient);
    }
    None
}

/// Return the object store error in `error`. SlateDB keeps the error of a store call in an
/// `Arc`, and the source of an `Arc` skips the inner error, so both forms count.
fn object_store_error<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a slatedb::object_store::Error> {
    error
        .downcast_ref::<slatedb::object_store::Error>()
        .or_else(|| {
            error
                .downcast_ref::<std::sync::Arc<slatedb::object_store::Error>>()
                .map(|inner| inner.as_ref())
        })
}

/// Classify a SlateDB error by its kind. A closed database is transient: a read on a newer
/// view or a reopened writer can succeed. The writer handles a fence and its own close first.
pub(crate) fn of_slatedb(error: &slatedb::Error) -> StorageFault {
    let has = |test: fn(&(dyn std::error::Error + 'static)) -> bool| {
        std::iter::successors(std::error::Error::source(error), |inner| inner.source()).any(test)
    };
    let missing = |inner: &(dyn std::error::Error + 'static)| {
        matches!(
            object_store_error(inner),
            Some(slatedb::object_store::Error::NotFound { .. })
        )
    };
    match error.kind() {
        slatedb::ErrorKind::Unavailable if has(|inner| inner.is::<Expired>()) => {
            StorageFault::Missing
        }
        slatedb::ErrorKind::Unavailable | slatedb::ErrorKind::Closed(_) => StorageFault::Transient,
        slatedb::ErrorKind::Data if has(missing) => StorageFault::Missing,
        _ => StorageFault::Corrupt,
    }
}

/// Reads that met stored data that does not decode. Each error counts once, where a read meets
/// it, also when one result serves several coalesced reads.
pub(crate) static CORRUPT_READS: LazyLock<prometheus::IntCounter> = LazyLock::new(|| {
    prometheus::IntCounter::new(
        "slatedb_etcd_read_corrupt_total",
        "Reads that met stored data that does not decode or contradicts other stored data.",
    )
    .expect("the counter options are valid")
});

/// Add the storage fault metrics to a member registry.
pub(crate) fn register(registry: &prometheus::Registry) -> anyhow::Result<()> {
    registry.register(Box::new(CORRUPT_READS.clone()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn the_outer_kind_classifies_an_error() {
        let missing = slatedb::object_store::Error::NotFound {
            path: "sst".into(),
            source: "gone".into(),
        };
        // SlateDB keeps the error of a store call in an `Arc`.
        let data = slatedb::Error::data("an object is missing".into())
            .with_source(Box::new(std::sync::Arc::new(missing)));
        assert_eq!(classify(&data.into()), Some(StorageFault::Missing));
        let checksum = slatedb::Error::data("checksum mismatch".into());
        assert_eq!(classify(&checksum.into()), Some(StorageFault::Corrupt));
        let throttled = anyhow::Error::from(slatedb::Error::unavailable("throttled".into()));
        assert_eq!(
            classify(&throttled.context("read")),
            Some(StorageFault::Transient)
        );
        let expired = slatedb::Error::unavailable("expired".into()).with_source(Box::new(Expired));
        assert_eq!(classify(&expired.into()), Some(StorageFault::Missing));
        let timeout = slatedb::object_store::Error::Generic {
            store: "bounded",
            source: "the object store call timed out".into(),
        };
        assert_eq!(classify(&timeout.into()), Some(StorageFault::Transient));
        let row = crate::proto::mvccpb::KeyValue::decode(&b"\xff"[..]).unwrap_err();
        assert_eq!(classify(&row.into()), Some(StorageFault::Corrupt));
        let shared = stored("invalid bootstrap claim").context("publish");
        assert_eq!(classify(&shared), Some(StorageFault::Corrupt));
        let invariant = anyhow::anyhow!("the base checkpoint has no applied term");
        assert_eq!(classify(&invariant), None);
    }
}
