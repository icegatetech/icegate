//! Run command implementation

use std::{path::PathBuf, sync::Arc};

use datafusion::execution::object_store::ObjectStoreUrl;
use icegate_common::{CatalogBuilder, MetricsRuntime, create_object_store, run_metrics_server};
use tokio_util::sync::CancellationToken;

use crate::{QueryConfig, engine::QueryEngine, error::QueryError, infra::metrics::QueryMetrics};

/// Wait for shutdown signal (SIGINT or SIGTERM)
#[allow(clippy::expect_used)] // Signal handler registration failures are critical startup errors
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            tracing::info!("Received SIGINT (Ctrl+C)");
        }
        () = terminate => {
            tracing::info!("Received SIGTERM");
        }
    }
}

/// Execute the run command
///
/// Starts all enabled query servers and runs until Ctrl+C
#[allow(clippy::cognitive_complexity)]
pub async fn execute(config_path: PathBuf) -> Result<(), QueryError> {
    // Load configuration
    let mut config = QueryConfig::from_file(&config_path)?;

    // Initialize tracing with OpenTelemetry
    let tracing_guard = icegate_common::init_tracing(&config.tracing)?;

    tracing::info!("Loading configuration from {:?}", config_path);
    tracing::info!("Configuration loaded successfully");

    // Initialize catalog
    tracing::info!("Initializing catalog");
    let (catalog, io_cache_handle) = CatalogBuilder::from_config(&config.catalog).await?;

    tracing::info!("Catalog initialized successfully");

    // Validate engine config (ensures wal_base_path is set)
    config.engine.validate()?;

    // Extract the shared foyer cache and size limit from the IO cache handle
    // so the WAL object store can share the same hybrid cache as the Iceberg
    // catalog.
    let foyer_cache = io_cache_handle.as_ref().map(|h| h.cache().clone());
    let cache_object_size_limit = config.catalog.cache.as_ref().map(|c| c.object_size_limit_mb * 1024 * 1024);

    // Initialize WAL object store
    tracing::info!(wal_base_path = %config.engine.wal_base_path, "Initializing WAL object store");
    let url = ObjectStoreUrl::parse(&config.engine.wal_base_path)
        .map_err(|e| QueryError::Config(format!("Invalid WAL base path: {e}")))?;
    let (store, prefix) = create_object_store(
        &config.engine.wal_base_path,
        Some(&config.storage.backend),
        foyer_cache.as_ref(),
        cache_object_size_limit,
    )?;
    // Override wal_base_path with the normalized prefix within the object store
    // (e.g., "s3://bucket/prefix/" → "prefix", "s3://bucket/" → "")
    config.engine.wal_base_path = prefix;
    let wal_store = (store, url);

    // Initialize query engine with cached catalog provider
    tracing::info!("Initializing query engine");
    let query_engine = Arc::new(QueryEngine::new(catalog, config.engine.clone(), wal_store));

    // Create cancellation token for coordinated shutdown
    let cancel_token = CancellationToken::new();

    tracing::info!("Query engine initialized successfully");

    // Initialize metrics
    let metrics_runtime = if config.metrics.enabled {
        Some(Arc::new(MetricsRuntime::new("query")?))
    } else {
        None
    };
    let query_metrics = Arc::new(
        metrics_runtime.as_ref().map_or_else(QueryMetrics::new_disabled, |runtime| {
            QueryMetrics::new(&runtime.meter())
        }),
    );

    // Spawn server tasks
    let mut handles = Vec::new();

    // Metrics server
    if let Some(ref runtime) = metrics_runtime {
        let metrics_config = config.metrics.clone();
        let token = cancel_token.clone();
        let registry = runtime.registry();
        let handle = tokio::spawn(async move {
            run_metrics_server(metrics_config, registry, token)
                .await
                .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
        });
        handles.push(handle);
    }

    // Query servers
    if config.loki.enabled {
        let engine = Arc::clone(&query_engine);
        let loki_config = config.loki.clone();
        let token = cancel_token.clone();
        let m = Arc::clone(&query_metrics);
        let handle = tokio::spawn(async move { crate::loki::run(engine, loki_config, token, m).await });
        handles.push(handle);
    }

    if config.prometheus.enabled {
        let engine = Arc::clone(&query_engine);
        let prom_config = config.prometheus.clone();
        let token = cancel_token.clone();
        let handle = tokio::spawn(async move { crate::prometheus::run(engine, prom_config, token).await });
        handles.push(handle);
    }

    if config.tempo.enabled {
        let engine = Arc::clone(&query_engine);
        let tempo_config = config.tempo.clone();
        let token = cancel_token.clone();
        let handle = tokio::spawn(async move { crate::tempo::run(engine, tempo_config, token).await });
        handles.push(handle);
    }

    if handles.is_empty() {
        tracing::warn!("No query servers are enabled in configuration");
        if let Some(handle) = io_cache_handle {
            handle.close().await;
        }
        return Ok(());
    }

    tracing::info!("All enabled query servers started");
    tracing::info!("Press Ctrl+C or send SIGTERM to shutdown");

    // Wait for shutdown signal (SIGINT or SIGTERM)
    shutdown_signal().await;

    tracing::info!("Shutdown signal received, stopping all servers...");

    // Cancel all servers
    cancel_token.cancel();

    // Wait for all servers to stop
    for handle in handles {
        if let Err(e) = handle.await {
            tracing::error!("Server task failed: {}", e);
        }
    }

    tracing::info!("All query servers stopped gracefully");

    // Gracefully close the IO cache to drain foyer's background flusher tasks.
    // This prevents "sending on a closed channel" errors during runtime teardown.
    if let Some(handle) = io_cache_handle {
        handle.close().await;
    }

    // Keep tracing guard alive until the very end
    drop(tracing_guard);

    Ok(())
}
