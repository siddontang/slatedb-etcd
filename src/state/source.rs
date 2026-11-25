//! Read applied state through one interface.
//!
//! Apply reads the live database. Pinned read views, Watch catch-up, and maintenance scans read a
//! SlateDB snapshot or the live database. All of them use `StateRead`, so a new state type needs
//! only an implementation of this trait. Writes still go to the SlateDB `Db`.
//!
//! The trait is generic, not a trait object. Point reads and scans are on the hot path, and a
//! trait object would need a boxed future for each call.

use std::future::Future;
use std::ops::Range;

use bytes::Bytes;
use slatedb::config::ScanOptions;
use slatedb::{Db, DbIterator, DbSnapshot};

/// One stored row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub key: Bytes,
    pub value: Bytes,
}

/// Rows of one scan in the key order of its options.
pub trait StateScan: Send {
    /// Return the next row, or `None` after the last row.
    fn next(&mut self) -> impl Future<Output = Result<Option<Row>, slatedb::Error>> + Send;

    /// Move an ascending scan to the first row at or after `key`.
    fn seek(&mut self, key: &[u8]) -> impl Future<Output = Result<(), slatedb::Error>> + Send;
}

/// Point reads and ordered scans of one state.
///
/// Implementations own their data, for example through `Arc`. The `'static` bound lets the
/// compiler prove that boxed futures of recursive transaction evaluation are `Send`.
pub trait StateRead: Send + Sync + 'static {
    type Scan<'a>: StateScan + 'a
    where
        Self: 'a;

    /// Return the value of `key`, or `None` when the key has no row.
    fn get(&self, key: &[u8])
        -> impl Future<Output = Result<Option<Bytes>, slatedb::Error>> + Send;

    /// Scan the rows in `range`.
    fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> impl Future<Output = Result<Self::Scan<'_>, slatedb::Error>> + Send;

    /// Scan the rows whose keys start with `prefix`.
    fn scan_under(
        &self,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> impl Future<Output = Result<Self::Scan<'_>, slatedb::Error>> + Send;
}

impl StateScan for DbIterator {
    async fn next(&mut self) -> Result<Option<Row>, slatedb::Error> {
        Ok(DbIterator::next(self).await?.map(|kv| Row {
            key: kv.key,
            value: kv.value,
        }))
    }

    async fn seek(&mut self, key: &[u8]) -> Result<(), slatedb::Error> {
        DbIterator::seek(self, key).await
    }
}

impl StateRead for Db {
    type Scan<'a> = DbIterator;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        Db::get(self, key).await
    }

    async fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<DbIterator, slatedb::Error> {
        Db::scan_with_options(self, range, options).await
    }

    async fn scan_under(
        &self,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<DbIterator, slatedb::Error> {
        Db::scan_prefix_with_options(self, prefix, .., options).await
    }
}

impl StateRead for DbSnapshot {
    type Scan<'a> = DbIterator;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, slatedb::Error> {
        DbSnapshot::get(self, key).await
    }

    async fn scan_range(
        &self,
        range: Range<Vec<u8>>,
        options: &ScanOptions,
    ) -> Result<DbIterator, slatedb::Error> {
        DbSnapshot::scan_with_options(self, range, options).await
    }

    async fn scan_under(
        &self,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<DbIterator, slatedb::Error> {
        DbSnapshot::scan_prefix_with_options(self, prefix, .., options).await
    }
}

/// Collect every row of a prefix scan. Tests compare states with it.
#[cfg(test)]
pub(crate) async fn rows(state: &impl StateRead, prefix: &[u8]) -> Vec<Row> {
    let mut scan = state
        .scan_under(prefix, &ScanOptions::default())
        .await
        .unwrap();
    let mut rows = vec![];
    while let Some(row) = scan.next().await.unwrap() {
        rows.push(row);
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::config::WriteOptions;

    async fn database(dir: &std::path::Path) -> Db {
        let store = std::sync::Arc::new(
            slatedb::object_store::local::LocalFileSystem::new_with_prefix(dir).unwrap(),
        );
        Db::builder("state", store)
            .with_settings(super::super::slatedb_settings())
            .build()
            .await
            .unwrap()
    }

    async fn check(state: &impl StateRead, expected: &[(&[u8], &[u8])]) {
        let row = |(key, value): &(&[u8], &[u8])| Row {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        };
        assert_eq!(
            state.get(b"a/1").await.unwrap(),
            Some(row(&expected[0]).value)
        );
        assert_eq!(state.get(b"a/9").await.unwrap(), None);
        assert_eq!(
            rows(state, b"a/").await,
            expected.iter().map(row).collect::<Vec<_>>()
        );
        let options = ScanOptions::default();
        let mut scan = state
            .scan_range(b"a/2".to_vec()..b"b".to_vec(), &options)
            .await
            .unwrap();
        let mut found = vec![];
        while let Some(row) = scan.next().await.unwrap() {
            found.push(row);
        }
        assert_eq!(found, expected[1..].iter().map(row).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn database_and_snapshot_reads_match_their_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = database(dir.path()).await;
        let mut batch = slatedb::WriteBatch::new();
        for (key, value) in [
            (&b"a/1"[..], &b"one"[..]),
            (b"a/2", b"two"),
            (b"a/3", b"three"),
            (b"b/1", b"other"),
        ] {
            batch.put(key, value);
        }
        db.write_with_options(batch, &WriteOptions { seqnum: 1 })
            .await
            .unwrap();
        let snapshot = db.snapshot().unwrap();
        let mut batch = slatedb::WriteBatch::new();
        batch.put(b"a/1", b"new");
        batch.delete(b"a/3");
        db.write_with_options(batch, &WriteOptions { seqnum: 2 })
            .await
            .unwrap();
        // The snapshot keeps the rows of its sequence. The database returns the latest rows.
        check(
            snapshot.as_ref(),
            &[(b"a/1", b"one"), (b"a/2", b"two"), (b"a/3", b"three")],
        )
        .await;
        check(&db, &[(b"a/1", b"new"), (b"a/2", b"two")]).await;
        db.close().await.unwrap();
    }
}
