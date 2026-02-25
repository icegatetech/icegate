//! Catalog builder factory

use std::{collections::HashMap, sync::Arc};

use iceberg::{
    Catalog, CatalogBuilder as IcebergCatalogBuilder,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
};
use iceberg_catalog_rest::RestCatalogBuilder;

use super::{CacheConfig, CatalogBackend, CatalogConfig};
use crate::error::{CommonError, Result};

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
/// let io_cache = IoCacheHandle::from_config(config.catalog.cache.as_ref()).await?;
/// let catalog = CatalogBuilder::from_config(&config.catalog, &io_cache).await?;
/// // ... run servers ...
/// // On shutdown:
/// io_cache.close().await;
/// ```
pub struct IoCacheHandle {
    cache: Option<iceberg::io::FoyerCache>,
}

impl IoCacheHandle {
    /// Create a no-op handle (no cache configured).
    ///
    /// Useful in tests and tools that don't need IO caching.
    #[must_use]
    pub const fn noop() -> Self {
        Self { cache: None }
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
    pub async fn from_config(config: Option<&CacheConfig>) -> Result<Self> {
        let cache = match config {
            Some(cache_config) => Some(Self::build_foyer_cache(cache_config).await?),
            None => None,
        };
        Ok(Self { cache })
    }

    /// Returns a reference to the underlying foyer cache, if configured.
    ///
    /// Clone the returned cache to share it with other components (e.g., WAL
    /// object store). `FoyerCache` is `Arc`-based, so clones share the same
    /// cache instance.
    pub const fn cache(&self) -> Option<&iceberg::io::FoyerCache> {
        self.cache.as_ref()
    }

    /// Create an Iceberg IO cache extension from the underlying cache.
    ///
    /// Returns `None` when no cache was configured.
    fn io_cache_extension(&self) -> Option<iceberg::io::IoCacheExtension> {
        self.cache.as_ref().map(|c| iceberg::io::IoCacheExtension::new(c.clone()))
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

    /// Build the foyer hybrid cache from [`CacheConfig`].
    async fn build_foyer_cache(config: &CacheConfig) -> Result<iceberg::io::FoyerCache> {
        let memory_bytes = config.memory_size_mb.checked_mul(1024 * 1024).ok_or_else(|| {
            CommonError::Config(format!(
                "memory_size_mb ({}) overflows when converted to bytes",
                config.memory_size_mb
            ))
        })?;
        let disk_bytes = config.disk_size_mb.checked_mul(1024 * 1024).ok_or_else(|| {
            CommonError::Config(format!(
                "disk_size_mb ({}) overflows when converted to bytes",
                config.disk_size_mb
            ))
        })?;

        foyer::HybridCacheBuilder::new()
            .memory(memory_bytes)
            .storage(foyer::Engine::mixed())
            .with_device_options(foyer::DirectFsDeviceOptions::new(&config.disk_dir).with_capacity(disk_bytes))
            .build()
            .await
            .map_err(|e| CommonError::Config(format!("Failed to build IO cache: {e}")))
    }
}

/// Factory for creating catalog instances from configuration
pub struct CatalogBuilder;

impl CatalogBuilder {
    /// Create a catalog from configuration.
    ///
    /// The `io_cache` handle is created **before** calling this method (via
    /// [`IoCacheHandle::from_config`]) so the same cache can be shared with
    /// other components (e.g., WAL object store) without coupling its
    /// lifecycle to catalog construction.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be created
    pub async fn from_config(config: &CatalogConfig, io_cache: &IoCacheHandle) -> Result<Arc<dyn Catalog>> {
        match &config.backend {
            CatalogBackend::Memory => Self::create_memory_catalog(config).await,
            CatalogBackend::Rest { uri } => Self::create_rest_catalog(config, uri, io_cache).await,
        }
    }

    /// Create a memory catalog
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

    /// Create a REST catalog
    async fn create_rest_catalog(
        config: &CatalogConfig,
        uri: &str,
        io_cache: &IoCacheHandle,
    ) -> Result<Arc<dyn Catalog>> {
        // Build REST catalog properties
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), uri.to_string());
        properties.insert("warehouse".to_string(), config.warehouse.clone());

        // Add any additional properties from config
        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        // Create REST catalog using RestCatalogBuilder
        let mut catalog = RestCatalogBuilder::default().load("rest", properties).await?;

        // Apply IO cache extension if configured
        if let Some(ext) = io_cache.io_cache_extension() {
            catalog = catalog.with_file_io_extension(ext);
            if let Some(ref cache_config) = config.cache {
                tracing::info!(
                    memory_mb = cache_config.memory_size_mb,
                    disk_mb = cache_config.disk_size_mb,
                    disk_dir = %cache_config.disk_dir,
                    "IO cache enabled for REST catalog"
                );
            }
        }

        tracing::info!(
            uri = %uri,
            warehouse = %config.warehouse,
            "Created REST catalog"
        );

        Ok(Arc::new(catalog))
    }
}
