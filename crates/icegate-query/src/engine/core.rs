//! Query execution engine with per-session catalog provider.
//!
//! The `QueryEngine` builds a fresh catalog provider on each
//! `create_session()` call, ensuring every query sees the latest committed
//! Iceberg data. The Iceberg `io-cache` feature (foyer hybrid cache) handles
//! caching expensive S3/FileIO reads, so the per-session rebuild only costs
//! catalog REST API calls.

use std::sync::Arc;

use datafusion::{
    catalog::CatalogProvider,
    execution::{SessionStateBuilder, object_store::ObjectStoreUrl},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_tracing::{InstrumentationOptions, instrument_with_debug_spans};
use iceberg::Catalog;
use object_store::ObjectStore;

use super::QueryEngineConfig;
use super::provider::IcegateCatalogProvider;
use crate::error::{QueryError, Result};

/// Query execution engine with per-session catalog provider.
///
/// `QueryEngine` builds a fresh catalog provider on each `create_session()`
/// call, so every query sees the latest committed Iceberg metadata. The
/// Iceberg `io-cache` (foyer hybrid cache) handles caching expensive
/// S3/FileIO reads underneath.
///
/// # Usage
///
/// ```ignore
/// let (catalog, _) = CatalogBuilder::from_config(&config.catalog).await?;
/// let engine = Arc::new(QueryEngine::new(catalog, config, (wal_store, wal_url)));
///
/// // In handlers:
/// let session_ctx = engine.create_session().await?;
/// // Use session_ctx for query execution...
/// ```
pub struct QueryEngine {
    /// Iceberg catalog for accessing tables.
    catalog: Arc<dyn Catalog>,
    /// Engine configuration.
    config: QueryEngineConfig,
    /// WAL object store and registered URL for hot data reading.
    wal_store: (Arc<dyn ObjectStore>, ObjectStoreUrl),
}

impl QueryEngine {
    /// Create a new `QueryEngine`.
    ///
    /// Stores the catalog and configuration for later use by
    /// `create_session()`. No provider is built at construction time.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Iceberg catalog for accessing tables
    /// * `config` - Engine configuration
    /// * `wal_store` - WAL object store and URL for hot data reading
    #[must_use]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        config: QueryEngineConfig,
        wal_store: (Arc<dyn ObjectStore>, ObjectStoreUrl),
    ) -> Self {
        Self {
            catalog,
            config,
            wal_store,
        }
    }

    /// Create a new `SessionContext` with the Iceberg catalog registered.
    ///
    /// Builds a fresh catalog provider from the Iceberg catalog, ensuring the
    /// session sees the latest committed table metadata. Each call creates a
    /// fresh `SessionContext` (required by `DataFusion` for concurrent queries).
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog provider cannot be created
    #[tracing::instrument(skip(self))]
    pub async fn create_session(&self) -> Result<SessionContext> {
        // Build session config from engine config
        let session_config = SessionConfig::new()
            .with_batch_size(self.config.batch_size)
            .with_target_partitions(self.config.target_partitions);

        // Instrument DataFusion execution plans with tracing spans.
        // Each physical plan node (scan, filter, sort, aggregate, etc.) gets
        // its own debug-level span with optional metrics recording.
        let options = InstrumentationOptions::builder().record_metrics(true).build();
        let instrument_rule = instrument_with_debug_spans!(options: options);

        // Build session state with the instrumentation rule
        let session_state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_default_features()
            .with_physical_optimizer_rule(instrument_rule)
            .build();

        // Create SessionContext
        let session_ctx = SessionContext::new_with_state(session_state);

        // Register the WAL object store with this session's RuntimeEnv.
        // Each SessionContext built via SessionStateBuilder::new()...build()
        // owns its own RuntimeEnv, so register_object_store must be called
        // per-session (not once globally) for DataFusion's ParquetSource to
        // resolve WAL Parquet files through the registered URL.
        let (ref store, ref url) = self.wal_store;
        session_ctx.runtime_env().register_object_store(url.as_ref(), Arc::clone(store));

        // Build a fresh provider from the catalog
        let provider: Arc<dyn CatalogProvider> = {
            let p = IcegateCatalogProvider::try_new(
                Arc::clone(&self.catalog),
                url.clone(),
                Arc::clone(store),
                self.config.wal_base_path.clone(),
                self.config.batch_size,
            )
            .await
            .map_err(|e| QueryError::Config(format!("Failed to create IcegateCatalogProvider: {e}")))?;
            Arc::new(p)
        };

        session_ctx.register_catalog(&self.config.catalog_name, provider);

        Ok(session_ctx)
    }

    /// Get reference to the underlying Iceberg catalog.
    ///
    /// Useful for handlers that need direct catalog access.
    #[must_use]
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Get reference to the engine configuration.
    #[must_use]
    pub const fn config(&self) -> &QueryEngineConfig {
        &self.config
    }
}

impl std::fmt::Debug for QueryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryEngine")
            .field("catalog", &"Arc<dyn Catalog>")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
