//! Run command implementation
//!
//! Starts the long-running maintenance service (Parquet compaction) and runs it
//! until a shutdown signal (SIGINT or SIGTERM) is received, then drains the
//! compaction workers gracefully.

use std::path::PathBuf;

use icegate_common::{CatalogBuilder, IoHandle, MetricsRuntime, run_metrics_server};
use tokio::task::JoinHandle;
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

    // Cancellation token for coordinated shutdown, created before the catalog so
    // the S3 catalog's CAS/transient retry loops abort promptly on SIGINT/SIGTERM
    // instead of running to their retry budget while the process is shutting down.
    // Distinct from `metrics_cancel`, which must outlive the worker drain so
    // `/metrics` keeps serving until the very end.
    let cancel_token = CancellationToken::new();
    let catalog = CatalogBuilder::from_config(&config.catalog, &IoHandle::noop(), cancel_token.clone()).await?;
    let compactor = Compactor::new(catalog, &config.compaction).await?;
    let handle = compactor.start()?;
    tracing::info!("compaction maintenance service started");

    // Wait for shutdown, but watch the metrics server task at the same time: it
    // only resolves before a shutdown signal if it failed to start (e.g. its port
    // is already bound). Surfacing that here aborts the service immediately rather
    // than deferring the error until shutdown, which could be days later.
    let (metrics_server, metrics_startup_error) = wait_for_shutdown(metrics_server).await;

    // Abort in-flight catalog CAS/transient retry loops before draining: a commit
    // caught mid-retry at SIGTERM stops at the next checkpoint instead of running
    // out its full retry budget, so the worker drain below completes promptly.
    cancel_token.cancel();
    tracing::info!("draining compaction workers");
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

    // Report a metrics startup failure only after the compaction workers have
    // drained and the runtime has been dropped, so shutdown still runs in order.
    if let Some(err) = metrics_startup_error {
        return Err(err);
    }
    Ok(())
}

/// Block until a shutdown signal arrives, while also watching the spawned
/// metrics server task.
///
/// [`run_metrics_server`] runs until its cancellation token fires, so its task
/// only finishes *before* a shutdown signal when it failed to start (typically a
/// `TcpListener::bind` failure on an already-used port). Racing the two lets the
/// service fail fast on that startup error instead of deferring it to shutdown.
///
/// Returns the still-pending metrics handle (so the caller can stop it
/// gracefully) together with the startup error, if the task ended early with one.
async fn wait_for_shutdown(
    metrics_server: Option<JoinHandle<icegate_common::error::Result<()>>>,
) -> (
    Option<JoinHandle<icegate_common::error::Result<()>>>,
    Option<MaintainError>,
) {
    let Some(mut server) = metrics_server else {
        shutdown_signal().await;
        return (None, None);
    };

    tokio::select! {
        () = shutdown_signal() => (Some(server), None),
        joined = &mut server => {
            // The metrics task ended before shutdown: a startup failure. The
            // handle is now consumed, so return `None` in its place.
            let error = match joined {
                Ok(Ok(())) => {
                    tracing::warn!("metrics server stopped before shutdown signal");
                    None
                }
                Ok(Err(err)) => Some(MaintainError::from(err)),
                Err(join_err) => {
                    tracing::error!("metrics server task failed to join: {join_err}");
                    None
                }
            };
            (None, error)
        }
    }
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
