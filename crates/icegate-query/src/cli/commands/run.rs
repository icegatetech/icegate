//! Run command implementation

use std::{path::PathBuf, sync::Arc};

use futures::stream::{FuturesUnordered, StreamExt};
use icegate_common::{CatalogBuilder, IoCacheHandle, MetricsRuntime, create_object_store, run_metrics_server};
use icegate_queue::ParquetQueueReader;
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
    let config = QueryConfig::from_file(&config_path)?;

    // Initialize tracing with OpenTelemetry
    let tracing_guard = icegate_common::init_tracing(&config.tracing)?;

    tracing::info!("Loading configuration from {:?}", config_path);
    tracing::info!("Configuration loaded successfully");

    // Initialize catalog
    tracing::info!("Initializing catalog");
    let io_cache = IoCacheHandle::from_config(config.catalog.cache.as_ref()).await?;
    let catalog = CatalogBuilder::from_config(&config.catalog, &io_cache).await?;

    tracing::info!("Catalog initialized successfully");

    // Validate engine config
    config.engine.validate()?;

    // Extract the shared foyer cache from the IO cache handle so the WAL
    // object store can share the same hybrid cache as the Iceberg catalog.
    let foyer_cache = io_cache.cache().cloned();

    // Initialize WAL object store from queue config
    tracing::info!(queue_base_path = %config.queue.common.base_path, "Initializing WAL object store");
    let (store, prefix) = create_object_store(
        &config.queue.common.base_path,
        Some(&config.storage.backend),
        foyer_cache.as_ref(),
    )?;

    // Build the shared WAL queue reader
    let wal_reader = ParquetQueueReader::new(prefix, Arc::clone(&store), config.engine.batch_size)
        .map_err(|e| QueryError::Config(format!("Failed to create WAL reader: {e}")))?;

    // Initialize query engine with cached catalog provider
    tracing::info!("Initializing query engine");
    let query_engine = Arc::new(QueryEngine::new(
        catalog,
        config.engine.clone(),
        store,
        Arc::new(wal_reader),
    ));

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
        io_cache.close().await;
        return Ok(());
    }

    tracing::info!("All enabled query servers started");
    tracing::info!("Press Ctrl+C or send SIGTERM to shutdown");
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    let mut handles: FuturesUnordered<_> = handles.into_iter().collect();
    let mut shutdown_started = false;
    let mut failure = None;

    while !handles.is_empty() {
        tokio::select! {
            () = &mut shutdown, if !shutdown_started => {
                tracing::info!("Shutdown signal received, stopping all servers...");
                cancel_token.cancel();
                shutdown_started = true;
            }
            result = handles.next() => {
                let Some(result) = result else {
                    continue;
                };

                match result {
                    Ok(Ok(())) if shutdown_started => {}
                    Ok(Ok(())) => {
                        tracing::error!("Server task exited before shutdown signal");
                        failure.get_or_insert_with(|| {
                            QueryError::Internal("server task exited before shutdown signal".to_string())
                        });
                    }
                    Ok(Err(err)) => {
                        tracing::error!("Server task failed: {err}");
                        failure.get_or_insert_with(|| QueryError::Internal(err.to_string()));
                    }
                    Err(err) => {
                        tracing::error!("Server task join failed: {err}");
                        failure.get_or_insert_with(|| QueryError::Internal(format!("server task join failed: {err}")));
                    }
                }

                if failure.is_some() && !shutdown_started {
                    tracing::info!("Stopping remaining servers after early task exit...");
                    cancel_token.cancel();
                    shutdown_started = true;
                }
            }
        }
    }

    if let Some(err) = failure {
        io_cache.close().await;
        drop(tracing_guard);
        return Err(err);
    }

    tracing::info!("All query servers stopped gracefully");

    // Gracefully close the IO cache to drain foyer's background flusher tasks.
    // This prevents "sending on a closed channel" errors during runtime teardown.
    io_cache.close().await;

    // Keep tracing guard alive until the very end
    drop(tracing_guard);

    Ok(())
}
