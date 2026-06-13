//! Run command implementation
//!
//! Starts the long-running maintenance service (Parquet compaction) and runs it
//! until a shutdown signal (SIGINT or SIGTERM) is received, then drains the
//! compaction workers gracefully.

use std::path::PathBuf;

use icegate_common::{CatalogBuilder, IoHandle};

use crate::{compact::Compactor, config::MaintainConfig, error::MaintainError};

/// Execute the run command.
///
/// Loads the maintain configuration, builds the Iceberg catalog, starts the
/// compaction service, and blocks until a shutdown signal arrives. On shutdown
/// the compaction workers are drained before returning.
///
/// # Errors
///
/// Returns [`MaintainError`] if the configuration cannot be loaded, the catalog
/// cannot be built, the compactor cannot start, or the workers stop with an
/// error during shutdown.
pub async fn execute(config_path: PathBuf) -> Result<(), MaintainError> {
    let config = MaintainConfig::from_file(&config_path).map_err(|e| MaintainError::Config(e.to_string()))?;
    let catalog = CatalogBuilder::from_config(&config.catalog, &IoHandle::noop()).await?;
    let compactor = Compactor::new(catalog, &config.compaction).await?;
    let handle = compactor.start()?;
    tracing::info!("compaction maintenance service started");
    shutdown_signal().await;
    tracing::info!("shutdown signal received, draining compaction workers");
    handle.shutdown().await?;
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
