//! Run command implementation
//!
//! Starts the long-running maintenance service (Parquet compaction) and runs it
//! until a shutdown signal (SIGINT or SIGTERM) is received, then drains the
//! compaction workers gracefully.

use std::path::PathBuf;

use icegate_common::{CatalogBuilder, IoHandle, MetricsRuntime, run_metrics_server};
use tokio_util::sync::CancellationToken;

use crate::{compact::Compactor, config::MaintainConfig, error::MaintainError};

/// Execute the run command.
///
/// Loads the maintain configuration, starts the Prometheus metrics server (when
/// `metrics.enabled`), builds the Iceberg catalog, starts the compaction
/// service, and blocks until a shutdown signal arrives. On shutdown the
/// compaction workers are drained and the metrics server is stopped before
/// returning.
///
/// # Errors
///
/// Returns [`MaintainError`] if the configuration cannot be loaded, the metrics
/// runtime cannot be built, the catalog cannot be built, the compactor cannot
/// start, or the workers stop with an error during shutdown.
pub async fn execute(config_path: PathBuf) -> Result<(), MaintainError> {
    let config = MaintainConfig::from_file(&config_path).map_err(|e| MaintainError::Config(e.to_string()))?;

    // Initialise metrics BEFORE building the compactor: `MetricsRuntime::new`
    // installs the global meter provider, and the compactor's `CompactMetrics`
    // bind to the global meter at construction time — built afterwards they would
    // attach to the no-op provider and never record. A disabled config installs
    // no provider, leaving the instruments inert.
    let metrics_runtime = if config.metrics.enabled {
        Some(MetricsRuntime::new("icegate-maintain")?)
    } else {
        None
    };

    // Serve `/metrics` for the lifetime of the service; the token stops it on
    // shutdown. `run_metrics_server` is a no-op when metrics are disabled, so it
    // is only spawned when a runtime exists.
    let metrics_cancel = CancellationToken::new();
    let metrics_server = metrics_runtime.as_ref().map(|runtime| {
        let registry = runtime.registry();
        let metrics_config = config.metrics.clone();
        let token = metrics_cancel.clone();
        tokio::spawn(async move { run_metrics_server(metrics_config, registry, token).await })
    });

    let catalog = CatalogBuilder::from_config(&config.catalog, &IoHandle::noop()).await?;
    let compactor = Compactor::new(catalog, &config.compaction).await?;
    let handle = compactor.start()?;
    tracing::info!("compaction maintenance service started");
    shutdown_signal().await;
    tracing::info!("shutdown signal received, draining compaction workers");
    handle.shutdown().await?;

    // Stop the metrics server and wait for it to unbind before the runtime (and
    // its meter provider) drop at end of scope.
    metrics_cancel.cancel();
    if let Some(server) = metrics_server {
        match server.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!("metrics server error: {err}"),
            Err(join_err) => tracing::error!("metrics server task failed to join: {join_err}"),
        }
    }
    drop(metrics_runtime);
    Ok(())
}

/// Wait for a shutdown signal (SIGINT or SIGTERM).
///
/// Mirrors the ingest binary's graceful-shutdown handling: it races a
/// `Ctrl+C` (SIGINT) future against a unix `SIGTERM` future and returns as soon
/// as either fires. On non-unix platforms only `Ctrl+C` is observed.
// Signal-handler registration failures are unrecoverable startup errors, so the
// `.expect()` calls here mirror ingest's `run` command (the only `.expect()`
// the project permits outside test code).
#[allow(clippy::expect_used)]
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
