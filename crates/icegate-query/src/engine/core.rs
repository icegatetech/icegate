//! Query execution engine with background-refreshed catalog provider.
//!
//! The `QueryEngine` keeps a cached catalog provider that is refreshed
//! in the background every `refresh_interval` (default 15s). The cached
//! provider remains valid for up to `max_age` (default 30s). Queries
//! never block on catalog rebuilds unless the cache is completely cold
//! (first query or after a prolonged refresh failure).

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::{
    catalog::CatalogProvider,
    execution::{SessionStateBuilder, object_store::ObjectStoreUrl},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_tracing::{InstrumentationOptions, instrument_with_debug_spans};
use iceberg::Catalog;
use icegate_queue::ParquetQueueReader;
use object_store::ObjectStore;
use tokio::sync::watch;

use super::QueryEngineConfig;
use super::provider::IcegateCatalogProvider;
use crate::error::Result;

/// Fixed DataFusion object store URL for WAL segments.
///
/// This is an arbitrary registry key — DataFusion uses it to look up the
/// object store at query time. It is not a real URL.
pub const WAL_STORE_URL: &str = "wal://queue";

/// Default interval between background catalog refreshes.
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(15);

/// Default maximum age before the cached provider is considered stale.
/// If the background refresh fails for longer than this, queries will
/// block on a synchronous rebuild.
const DEFAULT_MAX_AGE: Duration = Duration::from_secs(30);

/// Cached catalog provider with creation timestamp.
#[derive(Clone)]
struct CachedProvider {
    /// The cached catalog provider (thread-safe, immutable after creation).
    provider: Arc<dyn CatalogProvider>,
    /// When this provider was built — used for staleness detection.
    created_at: Instant,
}

/// Query execution engine with background-refreshed catalog provider.
///
/// `QueryEngine` keeps a `watch` channel with the latest
/// `IcegateCatalogProvider`. A background task rebuilds the provider
/// every `refresh_interval` so queries see fresh metadata without
/// blocking on catalog REST calls. The Iceberg `io-cache` (foyer hybrid
/// cache) handles caching expensive S3/FileIO reads underneath.
///
/// # Usage
///
/// ```ignore
/// let catalog = CatalogBuilder::from_config(&config.catalog, &io_cache).await?;
/// let reader = Arc::new(ParquetQueueReader::new(prefix, store.clone(), batch_size)?);
/// let engine = Arc::new(QueryEngine::new(catalog, config, store, reader));
///
/// // Start background refresh (must be called once after construction).
/// engine.start_background_refresh();
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
    /// WAL object store for DataFusion runtime registration.
    wal_store: Arc<dyn ObjectStore>,
    /// Shared WAL queue reader for segment listing and reading.
    wal_reader: Arc<ParquetQueueReader>,
    /// Watch channel receiver for the cached catalog provider.
    /// `None` means the cache is cold (no provider built yet).
    provider_rx: watch::Receiver<Option<CachedProvider>>,
    /// Watch channel sender — held to keep the channel alive and used
    /// by the background refresh task.
    provider_tx: Arc<watch::Sender<Option<CachedProvider>>>,
    /// Maximum age before a cached provider is considered too stale.
    max_age: Duration,
    /// Interval between background refresh cycles.
    refresh_interval: Duration,
}

impl QueryEngine {
    /// Create a new `QueryEngine`.
    ///
    /// The catalog provider cache starts cold. Call
    /// [`start_background_refresh`] after construction to begin
    /// periodic catalog refreshes.
    ///
    /// IMPORTANT: `wal_reader` is required to operate on WAL segments in `wal_store`.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Iceberg catalog for accessing tables
    /// * `config` - Engine configuration
    /// * `wal_store` - WAL object store for DataFusion runtime registration
    /// * `wal_reader` - Shared queue reader for WAL segment operations
    #[must_use]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        config: QueryEngineConfig,
        wal_store: Arc<dyn ObjectStore>,
        wal_reader: Arc<ParquetQueueReader>,
    ) -> Self {
        let (provider_tx, provider_rx) = watch::channel(None);
        Self {
            catalog,
            config,
            wal_store,
            wal_reader,
            provider_rx,
            provider_tx: Arc::new(provider_tx),
            max_age: DEFAULT_MAX_AGE,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }

    /// Start the background catalog refresh task.
    ///
    /// Spawns a tokio task that rebuilds the catalog provider every
    /// `refresh_interval`. The task holds a `Weak` reference to the
    /// engine and stops automatically when the engine is dropped.
    pub fn start_background_refresh(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            // Build the initial provider eagerly so the first query
            // doesn't have to wait.
            let Some(engine) = weak.upgrade() else { return };
            match Self::build_provider(Arc::clone(&engine.catalog), Arc::clone(&engine.wal_reader)).await {
                Ok(provider) => {
                    let _ = engine.provider_tx.send(Some(CachedProvider {
                        provider,
                        created_at: Instant::now(),
                    }));
                    tracing::info!("Initial catalog provider built");
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to build initial catalog provider");
                }
            }
            let refresh_interval = engine.refresh_interval;
            drop(engine); // Release strong ref during sleep

            loop {
                tokio::time::sleep(refresh_interval).await;

                let Some(engine) = weak.upgrade() else {
                    tracing::debug!("Catalog refresh task stopping: engine dropped");
                    break;
                };

                if engine.provider_tx.is_closed() {
                    tracing::debug!("Catalog refresh task stopping: channel closed");
                    break;
                }

                match Self::build_provider(Arc::clone(&engine.catalog), Arc::clone(&engine.wal_reader)).await {
                    Ok(provider) => {
                        let _ = engine.provider_tx.send(Some(CachedProvider {
                            provider,
                            created_at: Instant::now(),
                        }));
                        tracing::debug!("Catalog provider refreshed");
                    }
                    Err(e) => {
                        // Keep serving the stale provider — it's better than
                        // failing queries. The max_age check in create_session
                        // will force a synchronous rebuild if staleness exceeds
                        // the threshold.
                        tracing::warn!(error = %e, "Background catalog refresh failed, keeping stale provider");
                    }
                }
                drop(engine); // Release strong ref during sleep
            }
        });
    }

    /// Build a new catalog provider from the Iceberg catalog.
    async fn build_provider(
        catalog: Arc<dyn Catalog>,
        wal_reader: Arc<ParquetQueueReader>,
    ) -> Result<Arc<dyn CatalogProvider>> {
        let p = IcegateCatalogProvider::try_new(catalog, wal_reader).await?;
        Ok(Arc::new(p))
    }

    /// Create a new `SessionContext` with the Iceberg catalog registered.
    ///
    /// Uses the background-refreshed catalog provider when available.
    /// Falls back to a synchronous rebuild if the cache is cold or too
    /// stale (older than `max_age`).
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
        let wal_url = ObjectStoreUrl::parse(WAL_STORE_URL)
            .map_err(|e| crate::error::QueryError::Config(format!("Invalid WAL store URL: {e}")))?;
        session_ctx
            .runtime_env()
            .register_object_store(wal_url.as_ref(), Arc::clone(&self.wal_store));

        // Get the cached provider or fall back to synchronous rebuild.
        let provider = self.get_provider().await?;

        session_ctx.register_catalog(&self.config.catalog_name, provider);

        Ok(session_ctx)
    }

    /// Get the cached catalog provider, or rebuild synchronously if the
    /// cache is cold or too stale.
    async fn get_provider(&self) -> Result<Arc<dyn CatalogProvider>> {
        let cached = self.provider_rx.borrow().clone();

        if let Some(cached) = cached {
            if cached.created_at.elapsed() < self.max_age {
                return Ok(cached.provider);
            }
            // Provider is too stale — fall through to synchronous rebuild.
            tracing::warn!(
                age_ms = cached.created_at.elapsed().as_millis(),
                max_age_ms = self.max_age.as_millis(),
                "Cached catalog provider exceeded max_age, rebuilding synchronously"
            );
        }

        // Cold cache or stale provider — synchronous rebuild.
        let provider = Self::build_provider(Arc::clone(&self.catalog), Arc::clone(&self.wal_reader)).await?;

        // Update the watch channel so subsequent queries use this provider.
        let _ = self.provider_tx.send(Some(CachedProvider {
            provider: Arc::clone(&provider),
            created_at: Instant::now(),
        }));

        Ok(provider)
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
            .field("refresh_interval", &self.refresh_interval)
            .field("max_age", &self.max_age)
            .finish_non_exhaustive()
    }
}
