//! Query execution engine with cached catalog provider
//!
//! The `QueryEngine` provides pre-configured `SessionContext` instances with
//! the Iceberg catalog already registered. It caches the
//! `IcebergCatalogProvider` to avoid the 50-500ms network round-trip on every
//! query.

use std::{sync::Arc, time::Duration};

use datafusion::{
    catalog::CatalogProvider,
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use iceberg::Catalog;
use iceberg_datafusion::IcebergCatalogProvider;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::QueryEngineConfig;
use crate::error::{QueryError, Result};

/// Cached `IcebergCatalogProvider` with thread-safe access
struct CachedProvider {
    provider: Arc<dyn CatalogProvider>,
}

/// Query execution engine with cached catalog provider
///
/// `QueryEngine` provides pre-configured `SessionContext` instances with
/// the Iceberg catalog already registered. It caches the
/// `IcebergCatalogProvider` to avoid the 50-500ms network round-trip on every
/// query.
///
/// # Usage
///
/// ```ignore
/// let catalog = CatalogBuilder::from_config(&config.catalog).await?;
/// let engine = Arc::new(QueryEngine::new(catalog, config).await?);
/// engine.start_refresh_task(cancel_token);
///
/// // In handlers:
/// let session_ctx = engine.create_session().await?;
/// // Use session_ctx for query execution...
/// ```
pub struct QueryEngine {
    /// Iceberg catalog for accessing tables
    catalog: Arc<dyn Catalog>,
    /// Cached catalog provider (refreshed periodically)
    cached_provider: RwLock<Option<CachedProvider>>,
    /// Engine configuration
    config: QueryEngineConfig,
}

impl QueryEngine {
    /// Create a new `QueryEngine`
    ///
    /// Initializes the catalog provider cache. The provider is created
    /// immediately to ensure the engine is ready to serve queries.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial catalog provider cannot be created
    pub async fn new(catalog: Arc<dyn Catalog>, config: QueryEngineConfig) -> Result<Self> {
        let engine = Self {
            catalog,
            cached_provider: RwLock::new(None),
            config,
        };

        // Initialize the provider cache
        engine.refresh_provider().await?;

        Ok(engine)
    }

    /// Create a new `SessionContext` with the Iceberg catalog registered
    ///
    /// This is the primary method handlers should use. Each call creates
    /// a fresh `SessionContext` (required by `DataFusion` for concurrent
    /// queries) but reuses the cached `IcebergCatalogProvider`.
    ///
    /// # Errors
    ///
    /// Returns an error if the cached provider is not available
    pub async fn create_session(&self) -> Result<SessionContext> {
        // Build session config from engine config
        let session_config = SessionConfig::new()
            .with_batch_size(self.config.batch_size)
            .with_target_partitions(self.config.target_partitions);

        // Build session state
        let session_state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_default_features()
            .build();

        // Create SessionContext
        let session_ctx = SessionContext::new_with_state(session_state);

        // Get cached provider and register catalog
        let guard = self.cached_provider.read().await;
        let Some(cached) = guard.as_ref() else {
            return Err(QueryError::Config("Catalog provider not initialized".to_string()));
        };
        let provider = Arc::clone(&cached.provider);
        drop(guard);

        session_ctx.register_catalog(&self.config.catalog_name, provider);

        Ok(session_ctx)
    }

    /// Refresh the cached `IcebergCatalogProvider`
    ///
    /// This fetches fresh catalog metadata from the Iceberg catalog.
    /// Called periodically by the background refresh task.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog provider cannot be created
    pub async fn refresh_provider(&self) -> Result<()> {
        let provider = IcebergCatalogProvider::try_new(Arc::clone(&self.catalog))
            .await
            .map_err(|e| QueryError::Config(format!("Failed to create IcebergCatalogProvider: {e}")))?;

        {
            let mut guard = self.cached_provider.write().await;
            *guard = Some(CachedProvider {
                provider: Arc::new(provider) as Arc<dyn CatalogProvider>,
            });
        }

        tracing::debug!("IcebergCatalogProvider cache refreshed");

        Ok(())
    }

    /// Get reference to the underlying Iceberg catalog
    ///
    /// Useful for handlers that need direct catalog access
    #[must_use]
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Get reference to the engine configuration
    #[must_use]
    pub const fn config(&self) -> &QueryEngineConfig {
        &self.config
    }

    /// Start the background provider refresh task
    ///
    /// Spawns a background task that periodically refreshes the cached
    /// `IcebergCatalogProvider`. The task runs until the cancellation token
    /// is triggered.
    ///
    /// If `provider_refresh_seconds` is 0 in the config, this method returns
    /// immediately without spawning a task.
    pub fn start_refresh_task(self: &Arc<Self>, cancel_token: CancellationToken) {
        if self.config.provider_refresh_seconds == 0 {
            tracing::info!("Periodic provider refresh disabled");
            return;
        }

        let engine = Arc::clone(self);
        let interval = Duration::from_secs(self.config.provider_refresh_seconds);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            // Skip the first tick (provider was just initialized)
            interval_timer.tick().await;

            loop {
                tokio::select! {
                    () = cancel_token.cancelled() => {
                        tracing::info!("Provider refresh task shutting down");
                        break;
                    }
                    _ = interval_timer.tick() => {
                        if let Err(e) = engine.refresh_provider().await {
                            tracing::warn!("Failed to refresh catalog provider: {}", e);
                        }
                    }
                }
            }
        });

        tracing::info!(
            interval_seconds = self.config.provider_refresh_seconds,
            "Started periodic provider refresh task"
        );
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
