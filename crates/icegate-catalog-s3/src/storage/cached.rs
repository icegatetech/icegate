//! Caching decorator over [`CatalogStorage`].
//!
//! Holds the last-seen root and table metadata in memory and validates the
//! root cache with conditional reads through [`LoadOutcome::NotModified`].
//! Table metadata is immutable per location (UUID-suffixed paths), so a hit
//! is unconditional.
//!
//! Cache invariants:
//!
//! - Root cache stores `(version, Arc<CatalogRoot>)`. A successful CAS save
//!   updates it with the new pair so the next read avoids a round-trip.
//!   A conflict drops the entry — the next read must refetch.
//! - Metadata cache is keyed by storage location. Entries are immutable, so it
//!   is bounded by an LRU cap (eviction, never invalidation) to keep a
//!   long-lived ingest process from leaking memory.

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::TableMetadata;
use lru::LruCache;
use tokio::sync::{Mutex, RwLock};

use crate::error::{Error, Result, StorageError};
use crate::model::CatalogRoot;
use crate::storage::{CatalogStorage, LoadOutcome, Version};

/// Snapshot of the catalog root paired with its CAS token.
#[derive(Debug, Clone)]
struct CachedRoot {
    version: Version,
    root: Arc<CatalogRoot>,
}

/// Caching decorator. Mirrors the inner storage's [`CatalogStorage`] surface.
pub(crate) struct CachedCatalogStorage {
    inner: Arc<dyn CatalogStorage>,
    root_cache: RwLock<Option<CachedRoot>>,
    // Iceberg metadata is immutable once written (UUID in the filename), so
    // entries are never invalidated — only evicted. An LRU cap bounds memory on
    // a long-lived ingest process, where each commit mints a fresh location read
    // exactly once. The `Mutex` serialises the short get/put critical section;
    // mirrors the `Mutex<LruCache>` used by the queue reader.
    metadata_cache: Mutex<LruCache<String, Arc<TableMetadata>>>,
}

impl CachedCatalogStorage {
    /// Wrap an inner storage with a memory cache.
    ///
    /// `metadata_cache_cap` bounds the LRU metadata cache by entry count.
    pub(crate) fn new(inner: Arc<dyn CatalogStorage>, metadata_cache_cap: NonZeroUsize) -> Self {
        Self {
            inner,
            root_cache: RwLock::new(None),
            metadata_cache: Mutex::new(LruCache::new(metadata_cache_cap)),
        }
    }
}

#[async_trait]
impl CatalogStorage for CachedCatalogStorage {
    async fn load_root(&self, known: Option<&Version>) -> Result<LoadOutcome> {
        // Caller-supplied `known` owns the conditional-read semantics. The
        // cache may substitute its own token only for unconditional callers,
        // where `NotModified` is an implementation detail converted back into
        // a loaded cached snapshot.
        let cached = self.root_cache.read().await.clone();
        let revalidation_version = known.cloned().or_else(|| cached.as_ref().map(|entry| entry.version.clone()));

        match self.inner.load_root(revalidation_version.as_ref()).await? {
            LoadOutcome::Loaded { root, version } => {
                {
                    let mut guard = self.root_cache.write().await;
                    *guard = Some(CachedRoot {
                        version: version.clone(),
                        root: Arc::clone(&root),
                    });
                }
                Ok(LoadOutcome::Loaded { root, version })
            }
            LoadOutcome::NotModified if known.is_some() => Ok(LoadOutcome::NotModified),
            LoadOutcome::NotModified => {
                // Cache hit: inner confirmed our copy is still current.
                // `cached` must be `Some` here because we only sent a
                // conditional read after observing a populated cache.
                let cached = cached.ok_or_else(|| {
                    Error::Storage(StorageError::Io(
                        "inner returned NotModified without a cached snapshot".to_string(),
                    ))
                })?;
                Ok(LoadOutcome::Loaded {
                    root: cached.root,
                    version: cached.version,
                })
            }
            LoadOutcome::Absent => {
                // Root vanished. Drop stale cache so we don't re-send the
                // dead version on the next read.
                self.root_cache.write().await.take();
                Ok(LoadOutcome::Absent)
            }
        }
    }

    async fn save_root(&self, root: Arc<CatalogRoot>, expected: &Version) -> Result<Version> {
        match self.inner.save_root(Arc::clone(&root), expected).await {
            Ok(new_version) => {
                if let Version::Etag(_) = &new_version {
                    let mut guard = self.root_cache.write().await;
                    *guard = Some(CachedRoot {
                        version: new_version.clone(),
                        root,
                    });
                }
                Ok(new_version)
            }
            Err(error) if error.is_conflict() => {
                self.root_cache.write().await.take();
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    // Correctness without a TTL: the key is a per-version metadata location
    // (`{version:05}-{uuid}.metadata.json`), so every new table version is a new
    // key — a cached entry can never go stale for its own key. Freshness of
    // *which* location is current is owned by the root cache, which revalidates
    // every read via `If-None-Match`; this layer only memoises immutable bytes,
    // so even across processes a reader follows the fresh root to a fresh key.
    async fn read_table_metadata(&self, location: &str) -> Result<Arc<TableMetadata>> {
        {
            // Short critical section: look up and clone the Arc, then drop the
            // guard before any await. `get` also refreshes LRU recency.
            let mut cache = self.metadata_cache.lock().await;
            if let Some(metadata) = cache.get(location) {
                return Ok(Arc::clone(metadata));
            }
        }

        let metadata = self.inner.read_table_metadata(location).await?;
        // A race with another reader is harmless — both produce identical
        // metadata for an immutable location.
        self.metadata_cache
            .lock()
            .await
            .put(location.to_string(), Arc::clone(&metadata));
        Ok(metadata)
    }

    async fn write_table_metadata(&self, location: &str, metadata: &TableMetadata) -> Result<()> {
        self.inner.write_table_metadata(location, metadata).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use dashmap::DashMap;
    use iceberg::NamespaceIdent;
    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type, UnboundPartitionSpec,
    };
    use tokio::sync::Mutex;

    use super::*;
    use crate::error::Error;

    /// Generous default cap for cache tests that do not exercise eviction.
    fn test_cap() -> NonZeroUsize {
        NonZeroUsize::new(64).expect("non-zero test cap")
    }

    /// In-memory storage with call counters used to verify cache behaviour.
    #[derive(Default)]
    struct CountingStorage {
        root: Mutex<Option<(Arc<CatalogRoot>, String)>>,
        metadata: DashMap<String, Arc<TableMetadata>>,
        next_etag: AtomicUsize,
        load_root_calls: AtomicUsize,
        load_root_304: AtomicUsize,
        save_root_calls: AtomicUsize,
        read_metadata_calls: AtomicUsize,
    }

    impl CountingStorage {
        fn new() -> Self {
            Self::default()
        }

        fn bump_etag(&self) -> String {
            let next = self.next_etag.fetch_add(1, Ordering::SeqCst) + 1;
            format!("etag-{next}")
        }
    }

    #[async_trait]
    impl CatalogStorage for CountingStorage {
        async fn load_root(&self, known: Option<&Version>) -> Result<LoadOutcome> {
            self.load_root_calls.fetch_add(1, Ordering::SeqCst);
            let guard = self.root.lock().await;
            match guard.as_ref() {
                Some((root, etag)) => {
                    if let Some(Version::Etag(provided)) = known {
                        if provided == etag {
                            self.load_root_304.fetch_add(1, Ordering::SeqCst);
                            return Ok(LoadOutcome::NotModified);
                        }
                    }
                    Ok(LoadOutcome::Loaded {
                        root: Arc::clone(root),
                        version: Version::Etag(etag.clone()),
                    })
                }
                None => Ok(LoadOutcome::Absent),
            }
        }

        async fn save_root(&self, root: Arc<CatalogRoot>, expected: &Version) -> Result<Version> {
            self.save_root_calls.fetch_add(1, Ordering::SeqCst);
            let mut guard = self.root.lock().await;
            let current_version = guard.as_ref().map_or(Version::Absent, |(_, etag)| Version::Etag(etag.clone()));
            if current_version != *expected {
                return Err(Error::CommitConflict);
            }
            let new_etag = self.bump_etag();
            *guard = Some((root, new_etag.clone()));
            drop(guard);
            Ok(Version::Etag(new_etag))
        }

        async fn read_table_metadata(&self, location: &str) -> Result<Arc<TableMetadata>> {
            self.read_metadata_calls.fetch_add(1, Ordering::SeqCst);
            self.metadata
                .get(location)
                .map(|entry| Arc::clone(entry.value()))
                .ok_or_else(|| Error::InvalidMetadata(format!("missing: {location}")))
        }

        async fn write_table_metadata(&self, location: &str, metadata: &TableMetadata) -> Result<()> {
            self.metadata.insert(location.to_string(), Arc::new(metadata.clone()));
            Ok(())
        }
    }

    fn test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema")
    }

    fn make_metadata(location: &str) -> TableMetadata {
        TableMetadataBuilder::new(
            test_schema(),
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            location.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("table metadata")
        .metadata
    }

    fn unwrap_loaded(outcome: LoadOutcome) -> (Arc<CatalogRoot>, Version) {
        match outcome {
            LoadOutcome::Loaded { root, version } => (root, version),
            other => panic!("expected Loaded, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn load_root_caches_and_revalidates_via_if_none_match() {
        let inner = Arc::new(CountingStorage::new());
        // Seed inner with a non-empty root so the first load returns Version::Etag.
        let mut root = CatalogRoot::default();
        root.create_namespace(&NamespaceIdent::new("ns".to_string()), HashMap::new());
        inner.save_root(Arc::new(root), &Version::Absent).await.expect("seed root");
        // Reset counters after seed save_root.
        inner.save_root_calls.store(0, Ordering::SeqCst);

        let cache = CachedCatalogStorage::new(inner.clone(), test_cap());
        let (root_a, version_a) = unwrap_loaded(cache.load_root(None).await.expect("first load"));
        let (root_b, version_b) = unwrap_loaded(cache.load_root(None).await.expect("second load"));

        assert!(matches!(version_a, Version::Etag(_)));
        assert_eq!(version_a, version_b);
        assert!(Arc::ptr_eq(&root_a, &root_b), "cache must hand out the same Arc on hit");
        assert_eq!(inner.load_root_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            inner.load_root_304.load(Ordering::SeqCst),
            1,
            "second load must short-circuit via NotModified"
        );
    }

    #[tokio::test]
    async fn load_root_honors_caller_supplied_known_token() {
        let inner = Arc::new(CountingStorage::new());

        let mut first_root = CatalogRoot::default();
        first_root.create_namespace(&NamespaceIdent::new("ns1".to_string()), HashMap::new());
        let first_version = inner
            .save_root(Arc::new(first_root), &Version::Absent)
            .await
            .expect("seed first root");

        let cache = CachedCatalogStorage::new(inner.clone(), test_cap());
        let (_cached_root, cached_version) = unwrap_loaded(cache.load_root(None).await.expect("prime cache"));
        assert_eq!(cached_version, first_version);

        let mut second_root = CatalogRoot::default();
        second_root.create_namespace(&NamespaceIdent::new("ns2".to_string()), HashMap::new());
        let second_version = inner
            .save_root(Arc::new(second_root), &first_version)
            .await
            .expect("replace inner root behind cache");

        let outcome = cache
            .load_root(Some(&second_version))
            .await
            .expect("load with caller's fresh token");
        assert!(
            matches!(outcome, LoadOutcome::NotModified),
            "caller-supplied fresh token must drive the conditional read"
        );

        let (_loaded_root, loaded_version) = unwrap_loaded(
            cache
                .load_root(Some(&first_version))
                .await
                .expect("load with stale caller token"),
        );
        assert_eq!(loaded_version, second_version);
    }

    #[tokio::test]
    async fn save_root_refreshes_cache_and_skips_extra_load() {
        let inner = Arc::new(CountingStorage::new());
        let cache = CachedCatalogStorage::new(inner.clone(), test_cap());

        let outcome = cache.load_root(None).await.expect("initial absent load");
        assert!(matches!(outcome, LoadOutcome::Absent));
        let mut next = CatalogRoot::default();
        next.create_namespace(&NamespaceIdent::new("ns".to_string()), HashMap::new());
        let new_version = cache
            .save_root(Arc::new(next), &Version::Absent)
            .await
            .expect("save propagates new version");

        // Reading again should consult inner with the cached etag and get NotModified back.
        let calls_before = inner.load_root_calls.load(Ordering::SeqCst);
        let (_root, observed) = unwrap_loaded(cache.load_root(None).await.expect("post-save load"));
        assert_eq!(observed, new_version);
        assert_eq!(
            inner.load_root_calls.load(Ordering::SeqCst),
            calls_before + 1,
            "load_root still consults inner with the cached etag"
        );
        assert_eq!(
            inner.load_root_304.load(Ordering::SeqCst),
            1,
            "and that consultation returns NotModified"
        );
    }

    #[tokio::test]
    async fn save_root_conflict_invalidates_cache() {
        let inner = Arc::new(CountingStorage::new());
        let cache = CachedCatalogStorage::new(inner.clone(), test_cap());

        let outcome = cache.load_root(None).await.expect("absent load");
        assert!(matches!(outcome, LoadOutcome::Absent));
        let next = CatalogRoot::default();
        cache.save_root(Arc::new(next), &Version::Absent).await.expect("first save");

        // Use the now-stale Absent version intentionally — root is no longer absent.
        let stale_save = cache.save_root(Arc::new(CatalogRoot::default()), &Version::Absent).await;
        assert!(matches!(stale_save, Err(error) if error.is_conflict()));

        // After invalidation, the next load_root must NOT receive the stale etag.
        let calls_304_before = inner.load_root_304.load(Ordering::SeqCst);
        let _ = cache.load_root(None).await.expect("post-conflict load");
        assert_eq!(
            inner.load_root_304.load(Ordering::SeqCst),
            calls_304_before,
            "no NotModified expected after cache invalidation"
        );
    }

    #[tokio::test]
    async fn metadata_cache_returns_same_arc_on_hit() {
        let inner = Arc::new(CountingStorage::new());
        let metadata = make_metadata("memory://catalog/tables/t/00000-uuid.metadata.json");
        let location = "memory://catalog/tables/t/00000-uuid.metadata.json".to_string();
        inner.write_table_metadata(&location, &metadata).await.expect("seed metadata");

        let cache = CachedCatalogStorage::new(inner.clone(), test_cap());
        let first = cache.read_table_metadata(&location).await.expect("first");
        let second = cache.read_table_metadata(&location).await.expect("second");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(inner.read_metadata_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn metadata_cache_evicts_beyond_cap() {
        // Append-heavy ingest mints a fresh immutable location per commit; the
        // LRU cap must bound the cache so a long-lived process can't leak memory.
        const CAP: usize = 2;
        let inner = Arc::new(CountingStorage::new());
        let locations: Vec<String> = (0..5)
            .map(|i| format!("memory://catalog/tables/t/{i:05}-uuid.metadata.json"))
            .collect();
        for location in &locations {
            inner
                .write_table_metadata(location, &make_metadata(location))
                .await
                .expect("seed metadata");
        }

        let cache = CachedCatalogStorage::new(inner.clone(), NonZeroUsize::new(CAP).expect("non-zero cap"));
        for location in &locations {
            cache.read_table_metadata(location).await.expect("read metadata");
        }

        assert!(
            cache.metadata_cache.lock().await.len() <= CAP,
            "LRU metadata cache must not grow beyond its cap",
        );
    }
}
