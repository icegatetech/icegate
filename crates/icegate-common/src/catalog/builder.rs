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
/// # Usage
///
/// Store this handle alongside the catalog and close it during shutdown:
///
/// ```ignore
/// let (catalog, cache_handle) = CatalogBuilder::from_config(&config).await?;
/// // ... run servers ...
/// // On shutdown:
/// if let Some(handle) = cache_handle {
///     handle.close().await;
/// }
/// ```
pub struct IoCacheHandle {
    cache: iceberg::io::FoyerCache,
}

impl IoCacheHandle {
    /// Returns a reference to the underlying foyer cache.
    ///
    /// Clone the returned cache to share it with other components (e.g., WAL
    /// object store). `FoyerCache` is `Arc`-based, so clones share the same
    /// cache instance.
    pub const fn cache(&self) -> &iceberg::io::FoyerCache {
        &self.cache
    }

    /// Gracefully close the IO cache, draining background flusher tasks.
    pub async fn close(self) {
        tracing::info!("Closing IO cache...");
        if let Err(e) = self.cache.close().await {
            tracing::warn!("IO cache close returned error: {e}");
        }
        tracing::info!("IO cache closed");
    }
}

/// Factory for creating catalog instances from configuration
pub struct CatalogBuilder;

impl CatalogBuilder {
    /// Create a catalog from configuration.
    ///
    /// Returns the catalog and an optional [`IoCacheHandle`]. When a cache is
    /// configured, the handle **must** be closed during shutdown to avoid
    /// foyer flusher errors. See [`IoCacheHandle::close`].
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be created
    pub async fn from_config(config: &CatalogConfig) -> Result<(Arc<dyn Catalog>, Option<IoCacheHandle>)> {
        match &config.backend {
            CatalogBackend::Memory => {
                let catalog = Self::create_memory_catalog(config).await?;
                Ok((catalog, None))
            }
            CatalogBackend::Rest { uri } => Self::create_rest_catalog(config, uri).await,
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
    ) -> Result<(Arc<dyn Catalog>, Option<IoCacheHandle>)> {
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
        let cache_handle = if let Some(ref cache_config) = config.cache {
            let (io_cache, handle) = Self::build_io_cache(cache_config).await?;
            catalog = catalog.with_file_io_extension(io_cache);
            tracing::info!(
                memory_mb = cache_config.memory_size_mb,
                disk_mb = cache_config.disk_size_mb,
                disk_dir = %cache_config.disk_dir,
                "IO cache enabled for REST catalog"
            );
            Some(handle)
        } else {
            None
        };

        tracing::info!(
            uri = %uri,
            warehouse = %config.warehouse,
            "Created REST catalog"
        );

        Ok((Arc::new(catalog), cache_handle))
    }

    /// Build a foyer hybrid cache from [`CacheConfig`].
    ///
    /// Returns the `IoCacheConfig` for the Iceberg catalog and an
    /// [`IoCacheHandle`] for graceful shutdown.
    async fn build_io_cache(config: &CacheConfig) -> Result<(iceberg::io::IoCacheConfig, IoCacheHandle)> {
        let memory_bytes = config.memory_size_mb * 1024 * 1024;
        let disk_bytes = config.disk_size_mb * 1024 * 1024;

        let cache: iceberg::io::FoyerCache = foyer::HybridCacheBuilder::new()
            .memory(memory_bytes)
            .storage(foyer::Engine::mixed())
            .with_device_options(foyer::DirectFsDeviceOptions::new(&config.disk_dir).with_capacity(disk_bytes))
            .build()
            .await
            .map_err(|e| CommonError::Config(format!("Failed to build IO cache: {e}")))?;

        // Clone the cache handle before wrapping it — HybridCache is Arc-based,
        // so both the IoCacheConfig and IoCacheHandle share the same underlying
        // cache. Closing either handle drains the shared flusher tasks.
        let handle = IoCacheHandle { cache: cache.clone() };

        Ok((iceberg::io::IoCacheConfig::new(cache), handle))
    }
}
