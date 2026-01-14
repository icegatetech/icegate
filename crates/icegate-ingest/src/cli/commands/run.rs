//! Run command implementation

use std::{path::PathBuf, sync::Arc};

use icegate_common::{catalog::CatalogBuilder, create_object_store};
use icegate_queue::{QueueConfig, ParquetQueueReader, QueueWriter, channel};
use tokio_util::sync::CancellationToken;

use crate::{IngestConfig, error::Result, shift::Shifter};

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
/// Starts all enabled OTLP servers and runs until Ctrl+C
pub async fn execute(config_path: PathBuf) -> Result<()> {
    // Load configuration
    tracing::info!("Loading configuration from {:?}", config_path);
    let config = IngestConfig::from_file(config_path)?;

    tracing::info!("Configuration loaded successfully");

    // Initialize WAL queue based on queue config's base_path
    tracing::info!("Initializing WAL queue");
    let queue_config = config.queue.clone().unwrap_or_else(|| QueueConfig::new("wal"));
    let (write_tx, write_rx) = channel(queue_config.channel_capacity);

    // Create object store based on queue base_path
    let (store, normalized_path) = create_object_store(&queue_config.base_path, Some(&config.storage.backend))?;

    // Update queue config with normalized base path
    let mut queue_config = queue_config;
    queue_config.base_path = normalized_path;

    let writer = QueueWriter::new(queue_config.clone(), Arc::clone(&store));
    let writer_handle = writer.start(write_rx);

    tracing::info!("WAL queue initialized successfully");

    // Initialize shifter (WAL -> Iceberg)
    tracing::info!("Initializing shifter");
    let catalog = CatalogBuilder::from_config(&config.catalog).await?;
    let jobs_storage = config.shift.jobsmanager.storage.to_s3_config()?;
    let shift_config = Arc::new(config.shift.clone());
    let queue_reader = Arc::new(ParquetQueueReader::new(queue_config.base_path.clone(), Arc::clone(&store)));
    let shifter = Shifter::new(catalog, queue_reader, shift_config, jobs_storage).await?;
    let shifter_handle = shifter.start()?;

    tracing::info!("Shifter started successfully");

    // Create a cancellation token for coordinated shutdown
    let cancel_token = CancellationToken::new();

    // Spawn server tasks
    let mut handles = Vec::new();

    // OTLP HTTP server
    if config.otlp_http.enabled {
        let write_channel = write_tx.clone();
        let http_config = config.otlp_http.clone();
        let token = cancel_token.clone();
        let handle = tokio::spawn(async move { crate::otlp_http::run(write_channel, http_config, token).await });
        handles.push(handle);
    }

    // OTLP gRPC server
    if config.otlp_grpc.enabled {
        let write_channel = write_tx.clone();
        let grpc_config = config.otlp_grpc.clone();
        let token = cancel_token.clone();
        let handle = tokio::spawn(async move { crate::otlp_grpc::run(write_channel, grpc_config, token).await });
        handles.push(handle);
    }

    if handles.is_empty() {
        tracing::warn!("No OTLP servers are enabled in configuration");
        tracing::info!("Stopping shifter...");
        shifter_handle.shutdown().await?;
        tracing::info!("Shifter stopped gracefully");
        // Orderly shutdown: close channel so writer loop can exit, then await it
        drop(write_tx);
        return Ok(writer_handle.await??);
    }

    tracing::info!("All enabled OTLP servers started");
    tracing::info!("Press Ctrl+C or send SIGTERM to shutdown");

    // Wait for shutdown signal (SIGINT or SIGTERM)
    shutdown_signal().await;

    tracing::info!("Shutdown signal received, stopping all servers...");

    // Cancel all servers
    cancel_token.cancel();

    // Wait for all servers to stop
    for handle in handles {
        handle.await??;
    }

    tracing::info!("All OTLP servers stopped gracefully");

    tracing::info!("Stopping shifter...");
    shifter_handle.shutdown().await?;
    tracing::info!("Shifter stopped gracefully");

    // Close the write channel so the writer loop can exit
    drop(write_tx);

    // Wait for the writer task to finish
    Ok(writer_handle.await??)
}
