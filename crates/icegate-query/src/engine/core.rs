//! Query execution engine with background-refreshed catalog provider.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use datafusion::{
    catalog::CatalogProvider,
    execution::{
        SessionStateBuilder, memory_pool::FairSpillPool, object_store::ObjectStoreUrl, runtime_env::RuntimeEnvBuilder,
    },
    prelude::{SessionConfig, SessionContext},
};
use datafusion_tracing::{InstrumentationOptions, instrument_with_debug_spans};
use iceberg::Catalog;
use icegate_queue::ParquetQueueReader;
use object_store::ObjectStore;
use tokio::sync::watch;

use super::QueryEngineConfig;
use super::provider::{IcegateCatalogProvider, WalQueryConfig};
use crate::error::Result;

/// Fixed DataFusion object store URL for WAL segments.
///
/// This is an arbitrary registry key — DataFusion uses it to look up the
/// object store at query time. It is not a real URL.
pub const WAL_STORE_URL: &str = "wal://queue";

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
/// let catalog = CatalogBuilder::from_config(&config.catalog, &io_cache, shutdown_token).await?;
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
    /// Guards synchronous rebuilds so only one task rebuilds at a time.
    rebuild_lock: tokio::sync::Mutex<()>,
    /// Ensures `start_background_refresh` is called at most once.
    refresh_started: AtomicBool,
    /// Maximum age before a cached provider is considered too stale.
    max_age: Duration,
    /// Interval between background refresh cycles.
    refresh_interval: Duration,
    /// WAL query configuration derived from engine config.
    wal_config: WalQueryConfig,
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
        let max_age = Duration::from_secs(config.max_age_secs);
        let refresh_interval = Duration::from_secs(config.refresh_interval_secs);
        let wal_config = WalQueryConfig {
            enabled: config.wal_query_enabled,
            metadata_size_hint: config.wal_metadata_size_hint,
        };
        Self {
            catalog,
            config,
            wal_store,
            wal_reader,
            provider_rx,
            provider_tx: Arc::new(provider_tx),
            rebuild_lock: tokio::sync::Mutex::new(()),
            refresh_started: AtomicBool::new(false),
            max_age,
            refresh_interval,
            wal_config,
        }
    }

    /// Start the background catalog refresh task.
    ///
    /// Spawns a tokio task that rebuilds the catalog provider every
    /// `refresh_interval`. The task holds a `Weak` reference to the
    /// engine and stops automatically when the engine is dropped.
    pub fn start_background_refresh(self: &Arc<Self>) {
        if self.refresh_started.swap(true, Ordering::SeqCst) {
            tracing::warn!("start_background_refresh called more than once, ignoring");
            return;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            // Build the initial provider eagerly so the first query
            // doesn't have to wait.
            let Some(engine) = weak.upgrade() else { return };
            {
                // Hold rebuild_lock so synchronous rebuilds in get_provider
                // don't race with the initial eager build.
                let _guard = engine.rebuild_lock.lock().await;
                match Self::build_provider(
                    Arc::clone(&engine.catalog),
                    Arc::clone(&engine.wal_reader),
                    engine.wal_config.clone(),
                )
                .await
                {
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

                {
                    let _guard = engine.rebuild_lock.lock().await;
                    match Self::build_provider(
                        Arc::clone(&engine.catalog),
                        Arc::clone(&engine.wal_reader),
                        engine.wal_config.clone(),
                    )
                    .await
                    {
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
                }
                drop(engine); // Release strong ref during sleep
            }
        });
    }

    /// Build a new catalog provider from the Iceberg catalog.
    async fn build_provider(
        catalog: Arc<dyn Catalog>,
        wal_reader: Arc<ParquetQueueReader>,
        wal_config: WalQueryConfig,
    ) -> Result<Arc<dyn CatalogProvider>> {
        let p = IcegateCatalogProvider::try_new(catalog, wal_reader, wal_config).await?;
        Ok(Arc::new(p))
    }

    /// Create a new `SessionContext` with the Iceberg catalog registered.
    ///
    /// Uses the background-refreshed catalog provider when available.
    /// Falls back to a synchronous rebuild if the cache is cold or too
    /// stale (older than `max_age`). The session's memory pool is unbounded.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog provider cannot be created
    #[tracing::instrument(skip(self))]
    pub async fn create_session(&self) -> Result<SessionContext> {
        self.build_session(SessionStateBuilder::new()).await
    }

    /// Create the session [`Self::create_session`] creates, with every query it runs
    /// drawing from one memory pool of `limit_bytes`.
    ///
    /// The pool is a `FairSpillPool`, and the runtime keeps the default disk manager, so
    /// spilling to temporary files stays enabled: a sort larger than the pool completes
    /// (`a_session_with_a_memory_limit_spills_a_sort_larger_than_its_pool`). A
    /// reservation the pool cannot grant fails the query with
    /// `DataFusionError::ResourcesExhausted`, possibly wrapped in context; callers find it
    /// with `DataFusionError::find_root`.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime environment or the catalog provider cannot be
    /// created
    #[tracing::instrument(skip(self))]
    pub async fn create_session_with_memory_limit(&self, limit_bytes: usize) -> Result<SessionContext> {
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(limit_bytes)))
            .build_arc()?;
        self.build_session(SessionStateBuilder::new().with_runtime_env(runtime_env))
            .await
    }

    /// Finish `state_builder` with the engine's configuration, instrumentation, WAL
    /// store and catalog provider. A builder without a runtime environment gets
    /// DataFusion's default one, whose memory pool is unbounded.
    async fn build_session(&self, state_builder: SessionStateBuilder) -> Result<SessionContext> {
        // `information_schema` is enabled here once, on the session config,
        // rather than via a per-request `SET datafusion.catalog.information_schema`
        // statement: it lets Flight SQL BI tools introspect via `SHOW TABLES`
        // / `information_schema.*`, and is a harmless no-op for the HTTP query
        // paths that never read those virtual tables.
        let session_config = SessionConfig::new()
            .with_batch_size(self.config.batch_size)
            .with_target_partitions(self.config.target_partitions)
            .with_information_schema(true);

        // Instrument DataFusion execution plans with tracing spans.
        let options = InstrumentationOptions::builder().record_metrics(true).build();
        let instrument_rule = instrument_with_debug_spans!(options: options);

        let session_state = state_builder
            .with_config(session_config)
            .with_default_features()
            .with_physical_optimizer_rule(instrument_rule)
            .build();

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

        // Single-flight rebuild guard: only one task rebuilds at a time.
        let _guard = self.rebuild_lock.lock().await;

        // Re-check after acquiring lock — another task may have rebuilt.
        let rechecked = self.provider_rx.borrow().clone();
        if let Some(cached) = rechecked
            && cached.created_at.elapsed() < self.max_age
        {
            return Ok(cached.provider);
        }

        // Cold cache or stale provider — synchronous rebuild.
        let provider = Self::build_provider(
            Arc::clone(&self.catalog),
            Arc::clone(&self.wal_reader),
            self.wal_config.clone(),
        )
        .await?;

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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::error::DataFusionError;
    use datafusion::execution::memory_pool::{FairSpillPool, MemoryLimit};
    use icegate_common::{CatalogBackend, CatalogConfig, IoHandle, catalog::CatalogBuilder};
    use icegate_queue::ParquetQueueReader;
    use object_store::ObjectStore;
    use tokio_util::sync::CancellationToken;

    use super::QueryEngine;
    use crate::engine::QueryEngineConfig;

    /// Rows the refused and the unbounded sorts order: several batches, and few enough to
    /// sort in memory when the pool is unbounded.
    const SORTED_ROW_COUNT: i64 = 100_000;

    /// A pool below the merge headroom an external sort reserves before it sorts its
    /// first batch (the session's `sort_spill_reservation_bytes`; the test asserts the
    /// relation), so the refusal does not depend on how many rows the sort receives.
    const SMALL_POOL_BYTES: usize = 1024 * 1024;

    /// A pool above the sort's merge headroom, so the sort starts, and below the values
    /// it orders, so it can only finish by spilling (the test asserts both relations).
    const SPILLING_POOL_BYTES: usize = 16 * 1024 * 1024;

    /// Rows the spilling sort orders: their `Int64` values alone outgrow
    /// [`SPILLING_POOL_BYTES`].
    const SPILLED_ROW_COUNT: i64 = 4_000_000;

    /// Builds an engine over an empty in-memory catalog; the returned directory owns
    /// the warehouse and must outlive the engine.
    async fn build_engine() -> (Arc<QueryEngine>, tempfile::TempDir) {
        let warehouse = tempfile::tempdir().expect("create the warehouse directory");
        let catalog_config = CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: warehouse.path().to_str().expect("utf-8 warehouse path").to_string(),
            properties: HashMap::default(),
            cache: None,
        };
        let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new())
            .await
            .expect("build the in-memory catalog");
        let wal_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let wal_reader =
            Arc::new(ParquetQueueReader::new("", Arc::clone(&wal_store), 8192).expect("build the WAL reader"));
        let engine = QueryEngine::new(catalog, QueryEngineConfig::default(), wal_store, wal_reader);
        (Arc::new(engine), warehouse)
    }

    /// A query ordering the values `1..=row_count` from the largest down.
    fn build_descending_sort_sql(row_count: i64) -> String {
        format!("SELECT value FROM generate_series(1, {row_count}) ORDER BY value DESC")
    }

    /// Counts the rows of `batches` and reads the first value of the `value` column.
    fn read_row_count_and_first_value(batches: &[RecordBatch]) -> (usize, i64) {
        let row_count = batches.iter().map(RecordBatch::num_rows).sum();
        let first_batch = batches.iter().find(|batch| batch.num_rows() > 0).expect("a non-empty batch");
        let values = first_batch
            .column_by_name("value")
            .expect("the sorted column is named value")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("generate_series yields Int64 values");
        (row_count, values.value(0))
    }

    #[tokio::test]
    async fn a_session_with_a_memory_limit_refuses_a_sort_beyond_it() {
        let (engine, _warehouse) = build_engine().await;
        let session = engine
            .create_session_with_memory_limit(SMALL_POOL_BYTES)
            .await
            .expect("create the limited session");

        let pool = Arc::clone(&session.runtime_env().memory_pool);
        assert!(
            pool.is::<FairSpillPool>(),
            "the limited session must use a fair spill pool"
        );
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(1_048_576)));
        let merge_headroom = session.state().config().options().execution.sort_spill_reservation_bytes;
        assert!(
            merge_headroom > SMALL_POOL_BYTES,
            "the sort must reserve more than the pool holds before its first batch, reserves {merge_headroom}"
        );

        let error = session
            .sql(&build_descending_sort_sql(SORTED_ROW_COUNT))
            .await
            .expect("plan the sort")
            .collect()
            .await
            .expect_err("the sort must not fit in the pool");
        assert!(
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
            "the refusal must be the pool's, got: {error}"
        );
    }

    #[tokio::test]
    async fn create_session_keeps_the_unlimited_pool_of_the_other_protocols() {
        let (engine, _warehouse) = build_engine().await;
        let session = engine.create_session().await.expect("create the session");

        assert!(matches!(
            session.runtime_env().memory_pool.memory_limit(),
            MemoryLimit::Infinite
        ));

        let batches = session
            .sql(&build_descending_sort_sql(SORTED_ROW_COUNT))
            .await
            .expect("plan the sort")
            .collect()
            .await
            .expect("an unbounded pool sorts every row");
        assert_eq!(read_row_count_and_first_value(&batches), (100_000, 100_000));
    }

    #[tokio::test]
    async fn a_session_with_a_memory_limit_spills_a_sort_larger_than_its_pool() {
        let (engine, _warehouse) = build_engine().await;
        let session = engine
            .create_session_with_memory_limit(SPILLING_POOL_BYTES)
            .await
            .expect("create the limited session");

        let merge_headroom = session.state().config().options().execution.sort_spill_reservation_bytes;
        assert!(
            merge_headroom < SPILLING_POOL_BYTES,
            "the pool must grant the sort's merge headroom of {merge_headroom}"
        );
        let sorted_value_bytes =
            usize::try_from(SPILLED_ROW_COUNT).expect("the row count fits usize") * size_of::<i64>();
        assert!(
            sorted_value_bytes > SPILLING_POOL_BYTES,
            "the sorted values must outgrow the pool, hold {sorted_value_bytes}"
        );

        let batches = session
            .sql(&build_descending_sort_sql(SPILLED_ROW_COUNT))
            .await
            .expect("plan the sort")
            .collect()
            .await
            .expect("the sort must spill instead of failing");
        assert_eq!(read_row_count_and_first_value(&batches), (4_000_000, 4_000_000));
    }
}
