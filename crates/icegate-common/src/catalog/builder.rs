//! Catalog builder factory

use std::{collections::HashMap, sync::Arc, time::Duration};

use iceberg::{
    Catalog, CatalogBuilder as IcebergCatalogBuilder,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_catalog_s3tables::S3TablesCatalogBuilder;

use super::{CacheConfig, CatalogBackend, CatalogConfig};
use crate::error::Result;
use crate::storage::PrefetchConfig;
use crate::storage::cache::{StorageCache, build_storage_cache};
use crate::storage::icegate_storage::IceGateStorageFactory;

/// Handle for gracefully closing the foyer IO cache on shutdown.
///
/// Foyer's `HybridCache` runs background flusher tasks that send work through
/// internal channels. Dropping the cache without closing it first causes
/// "sending on a closed channel" errors as the flusher tasks race against
/// runtime teardown. Calling [`close()`](Self::close) drains the flushers
/// cleanly before the cache is dropped.
///
/// The cache is created **outside** the catalog builder via
/// [`from_config()`](Self::from_config) so that the same cache can be shared
/// with other components (e.g., the WAL object store) without coupling its
/// lifecycle to catalog construction.
///
/// # Usage
///
/// ```ignore
/// let io_cache = IoHandle::from_config(config.catalog.cache.as_ref()).await?;
/// let catalog = CatalogBuilder::from_config(&config.catalog, &io_cache).await?;
/// // ... run servers ...
/// // On shutdown:
/// io_cache.close().await;
/// ```
pub struct IoHandle {
    cache: Option<StorageCache>,
    prefetch: Option<PrefetchConfig>,
    stat_ttl: Option<Duration>,
    max_write_cache_size: Option<usize>,
}

impl IoHandle {
    /// Create a no-op handle (no cache configured).
    ///
    /// Useful in tests and tools that don't need IO caching.
    #[must_use]
    pub const fn noop() -> Self {
        Self {
            cache: None,
            prefetch: None,
            stat_ttl: None,
            max_write_cache_size: None,
        }
    }

    /// Create a handle from optional cache configuration.
    ///
    /// When `config` is `None`, creates a no-op handle whose
    /// [`close()`](Self::close) is a no-op and whose [`cache()`](Self::cache)
    /// returns `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if the foyer cache cannot be built (e.g., memory/disk
    /// size overflow or filesystem error).
    pub async fn from_config(
        cache_config: Option<&CacheConfig>,
        prefetch_config: Option<PrefetchConfig>,
    ) -> Result<Self> {
        let stat_ttl = cache_config.and_then(|cc| cc.stat_ttl_secs).map(Duration::from_secs);
        let max_write_cache_size = cache_config
            .and_then(|cc| cc.max_write_cache_size_mb)
            .map(|mb| mb * 1024 * 1024);
        let cache = match cache_config {
            Some(cc) => Some(build_storage_cache(cc).await?),
            None => None,
        };
        let prefetch = if cache.is_some() { prefetch_config } else { None };
        Ok(Self {
            cache,
            prefetch,
            stat_ttl,
            max_write_cache_size,
        })
    }

    /// Returns a reference to the underlying foyer cache, if configured.
    ///
    /// Clone the returned cache to share it with other components (e.g., WAL
    /// object store). `StorageCache` is `Arc`-based, so clones share the same
    /// cache instance.
    pub const fn cache(&self) -> Option<&StorageCache> {
        self.cache.as_ref()
    }

    /// Returns a reference to the prefetch configuration, if configured.
    pub const fn prefetch(&self) -> Option<&PrefetchConfig> {
        self.prefetch.as_ref()
    }

    /// Returns the stat metadata cache TTL, if configured.
    pub const fn stat_ttl(&self) -> Option<Duration> {
        self.stat_ttl
    }

    /// Returns the max value size (bytes) to cache on writes, if configured.
    pub const fn max_write_cache_size(&self) -> Option<usize> {
        self.max_write_cache_size
    }

    /// Gracefully close the IO cache, draining background flusher tasks.
    ///
    /// No-op when no cache was configured.
    pub async fn close(self) {
        if let Some(cache) = self.cache {
            tracing::info!("Closing IO cache...");
            if let Err(e) = cache.close().await {
                tracing::warn!("IO cache close returned error: {e}");
            }
            tracing::info!("IO cache closed");
        }
    }

    /// Build an [`IceGateStorageFactory`] that injects the shared cache
    /// and an `OpenTelemetry` meter into every `FileIO` created by catalogs.
    ///
    /// The returned factory auto-detects the storage scheme from
    /// [`StorageConfig`] properties, so callers don't need to specify it.
    pub fn storage_factory(&self) -> Arc<dyn iceberg::io::StorageFactory> {
        let meter = opentelemetry::global::meter("iceberg-storage");
        Arc::new(IceGateStorageFactory::new(
            self.cache.clone(),
            Some(meter),
            self.prefetch.clone(),
            self.stat_ttl,
            self.max_write_cache_size,
        ))
    }
}

/// Factory for creating catalog instances from configuration
pub struct CatalogBuilder;

impl CatalogBuilder {
    /// Create a catalog from configuration.
    ///
    /// The `io_cache` handle is created **before** calling this method (via
    /// [`IoHandle::from_config`]) so the same cache can be shared with
    /// other components (e.g., WAL object store) without coupling its
    /// lifecycle to catalog construction.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be created
    pub async fn from_config(config: &CatalogConfig, io_cache: &IoHandle) -> Result<Arc<dyn Catalog>> {
        match &config.backend {
            CatalogBackend::Memory => Self::create_memory_catalog(config).await,
            CatalogBackend::Rest { uri } => Self::create_rest_catalog(config, uri, io_cache).await,
            CatalogBackend::S3Tables { table_bucket_arn } => {
                Self::create_s3tables_catalog(config, table_bucket_arn, io_cache).await
            }
            CatalogBackend::Glue { catalog_id } => {
                Self::create_glue_catalog(config, catalog_id.as_deref(), io_cache).await
            }
        }
    }

    /// Create a memory catalog.
    ///
    /// The Memory catalog stores everything in memory and does not need
    /// the storage factory (no cache, metrics, or prefetch layers).
    async fn create_memory_catalog(config: &CatalogConfig) -> Result<Arc<dyn Catalog>> {
        let mut properties = HashMap::new();
        properties.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), config.warehouse.clone());

        // Add any additional properties from config
        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        let catalog = MemoryCatalogBuilder::default().load("icegate", properties).await?;

        tracing::info!(
            warehouse = %config.warehouse,
            "Created Memory catalog"
        );

        Ok(Arc::new(catalog))
    }

    /// Create an S3 Tables catalog with `IceGateStorageFactory`.
    async fn create_s3tables_catalog(
        config: &CatalogConfig,
        table_bucket_arn: &str,
        io_cache: &IoHandle,
    ) -> Result<Arc<dyn Catalog>> {
        let mut properties = HashMap::new();
        properties.insert("table_bucket_arn".to_string(), table_bucket_arn.to_string());
        properties.insert("warehouse".to_string(), config.warehouse.clone());

        // Add any additional properties from config
        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        let catalog = S3TablesCatalogBuilder::default()
            .with_storage_factory(io_cache.storage_factory())
            .load("s3tables", properties)
            .await?;

        tracing::info!(
            table_bucket_arn = %table_bucket_arn,
            warehouse = %config.warehouse,
            "Created S3 Tables catalog"
        );

        Ok(Arc::new(catalog))
    }

    /// Create an AWS Glue catalog with `IceGateStorageFactory`.
    async fn create_glue_catalog(
        config: &CatalogConfig,
        catalog_id: Option<&str>,
        io_cache: &IoHandle,
    ) -> Result<Arc<dyn Catalog>> {
        let mut properties = HashMap::new();
        properties.insert("warehouse".to_string(), config.warehouse.clone());

        if let Some(id) = catalog_id {
            properties.insert("catalog_id".to_string(), id.to_string());
        }

        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        let catalog = GlueCatalogBuilder::default()
            .with_storage_factory(io_cache.storage_factory())
            .load("glue", properties)
            .await?;

        tracing::info!(
            catalog_id = ?catalog_id,
            warehouse = %config.warehouse,
            "Created Glue catalog"
        );

        Ok(Arc::new(catalog))
    }

    /// Create a REST catalog with `IceGateStorageFactory`.
    ///
    /// The REST catalog **requires** a `StorageFactory` — it will error
    /// at table-load time without one.
    async fn create_rest_catalog(config: &CatalogConfig, uri: &str, io_cache: &IoHandle) -> Result<Arc<dyn Catalog>> {
        // Build REST catalog properties
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), uri.to_string());
        properties.insert("warehouse".to_string(), config.warehouse.clone());

        // Add any additional properties from config
        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        // Create REST catalog with IceGateStorageFactory
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(io_cache.storage_factory())
            .load("rest", properties)
            .await?;

        tracing::info!(
            uri = %uri,
            warehouse = %config.warehouse,
            "Created REST catalog"
        );

        Ok(Arc::new(catalog))
    }
}
