//! Storage abstraction for catalog state.

use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::TableMetadata;

use crate::error::Result;
use crate::model::CatalogRoot;

pub(crate) mod cached;
pub(crate) mod s3;

/// Opaque version token for compare-and-swap operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Version {
    /// Root object does not exist yet.
    Absent,
    /// Root object exists and is identified by an opaque storage token.
    Etag(String),
}

/// Outcome of a [`CatalogStorage::load_root`] call.
///
/// Encapsulates conditional-read semantics so the underlying transport
/// (`ETag`/`If-None-Match` for S3, version number for tests, etc.) never
/// leaks into upper layers.
#[derive(Debug)]
pub(crate) enum LoadOutcome {
    /// Storage object exists and matches the version supplied by the caller.
    /// The caller's cached root is still authoritative.
    NotModified,
    /// Storage object exists; payload and new CAS token are returned.
    Loaded {
        /// Decoded catalog root snapshot.
        root: Arc<CatalogRoot>,
        /// CAS token to be passed to the next [`CatalogStorage::save_root`].
        version: Version,
    },
    /// Storage object does not exist (yet). Callers must treat this as an
    /// empty catalog and CAS against [`Version::Absent`].
    Absent,
}

#[async_trait]
/// Storage backend used by the S3 catalog state machine.
///
/// Roots are exchanged as [`Arc<CatalogRoot>`] so caches and read-only paths
/// can share an immutable snapshot without copying. Writers either clone-on-
/// write (small enough catalogs) or hold the only `Arc` for short windows.
pub(crate) trait CatalogStorage: Send + Sync {
    /// Load the current catalog root.
    ///
    /// When `known` is `Some`, the storage performs a conditional read and
    /// returns [`LoadOutcome::NotModified`] if the remote object still matches
    /// the supplied CAS token. Pass `None` for an unconditional load.
    async fn load_root(&self, known: Option<&Version>) -> Result<LoadOutcome>;

    /// Atomically save the root if the expected version still matches.
    ///
    /// Returns the new [`Version`] of the just-written object so callers can
    /// refresh their CAS token (and any caches) without an extra round-trip.
    async fn save_root(&self, root: Arc<CatalogRoot>, expected: &Version) -> Result<Version>;

    /// Read and deserialize table metadata from a storage URI.
    async fn read_table_metadata(&self, location: &str) -> Result<Arc<TableMetadata>>;

    /// Write metadata to a precomputed storage URI.
    async fn write_table_metadata(&self, location: &str, metadata: &TableMetadata) -> Result<()>;
}
