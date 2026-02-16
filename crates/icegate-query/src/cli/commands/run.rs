//! Run command implementation

use std::{path::PathBuf, sync::Arc};

use icegate_common::{CatalogBuilder, MetricsRuntime, run_metrics_server};
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
    let catalog = CatalogBuilder::from_config(&config.catalog).await?;

    tracing::info!("Catalog initialized successfully");

    // Initialize query engine with cached IcebergCatalogProvider
    tracing::info!("Initializing query engine");
    let query_engine = Arc::new(QueryEngine::new(catalog, config.engine.clone()).await?);

    // Create cancellation token for coordinated shutdown
    let cancel_token = CancellationToken::new();

    // Start background provider refresh task
    query_engine.start_refresh_task(cancel_token.clone());

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

    // Keep tracing guard alive until the very end
    drop(tracing_guard);

    Ok(())
}
