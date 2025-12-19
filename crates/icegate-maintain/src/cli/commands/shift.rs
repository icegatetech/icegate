//! Shift command implementation
//!
//! Continuously moves data from the queue to Iceberg tables.

use std::path::PathBuf;

use icegate_common::CatalogBuilder;
use tokio_util::sync::CancellationToken;

use crate::{config::MaintainConfig, error::MaintainError, shift::processor::run_shift};

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

/// Execute the shift command
///
/// Starts the shift processor and runs until Ctrl+C or SIGTERM.
///
/// # Errors
///
/// Returns an error if shift processing fails
pub async fn execute(config_path: PathBuf) -> Result<(), MaintainError> {
    tracing::info!("Loading configuration from {:?}", config_path);
    let config = MaintainConfig::from_file(&config_path).map_err(|e| MaintainError::Config(e.to_string()))?;

    // Validate shift config exists
    let shift_config = config
        .shift
        .ok_or_else(|| MaintainError::Config("shift configuration is required for the shift command".to_string()))?;

    tracing::info!("Initializing catalog");
    let catalog = CatalogBuilder::from_config(&config.catalog).await?;

    tracing::info!("Catalog initialized successfully");

    // Create cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();

    // Spawn the shift processor
    let shift_cancel = cancel_token.clone();
    let storage_config = config.storage.clone();

    let shift_handle =
        tokio::spawn(async move { run_shift(catalog, &storage_config, shift_config, shift_cancel).await });

    tracing::info!("Shift processor started");
    tracing::info!("Press Ctrl+C or send SIGTERM to shutdown");

    // Wait for shutdown signal
    shutdown_signal().await;

    tracing::info!("Shutdown signal received, stopping shift processor...");

    // Cancel the processor
    cancel_token.cancel();

    // Wait for processor to stop
    match shift_handle.await {
        Ok(Ok(())) => {
            tracing::info!("Shift processor stopped gracefully");
        },
        Ok(Err(e)) => {
            tracing::error!("Shift processor error: {}", e);
            return Err(e);
        },
        Err(e) => {
            tracing::error!("Shift processor task failed: {}", e);
            return Err(MaintainError::Shift(format!("task join error: {e}")));
        },
    }

    Ok(())
}
