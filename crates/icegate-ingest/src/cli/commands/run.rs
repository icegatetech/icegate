//! Run command implementation

use std::{path::PathBuf, sync::Arc};

use icegate_common::{CatalogBuilder, StorageBackend};
use icegate_queue::{channel, QueueConfig, QueueWriter};
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory};
use tokio_util::sync::CancellationToken;

use crate::{error::IngestError, IngestConfig};

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
#[allow(clippy::cognitive_complexity)]
#[allow(clippy::expect_used)]
pub async fn execute(config_path: PathBuf) -> Result<(), IngestError> {
    // Load configuration
    tracing::info!("Loading configuration from {:?}", config_path);
    let config = IngestConfig::from_file(config_path)?;

    tracing::info!("Configuration loaded successfully");

    // Initialize catalog (CatalogBuilder::from_config already returns Arc<dyn
    // Catalog>)
    tracing::info!("Initializing catalog");
    let _catalog: Arc<_> = CatalogBuilder::from_config(&config.catalog).await?;

    tracing::info!("Catalog initialized successfully");

    // Initialize WAL queue based on queue config's base_path
    tracing::info!("Initializing WAL queue");
    let queue_config = config.queue.clone().unwrap_or_else(|| QueueConfig::new("wal"));
    let (write_tx, write_rx) = channel(queue_config.channel_capacity);

    // Parse queue base_path to determine storage type and create appropriate store
    let base_path = &queue_config.base_path;
    let _writer_handle = if base_path.starts_with("s3://") {
        // Parse S3 URL: s3://bucket/prefix
        let path_without_scheme = base_path.strip_prefix("s3://").unwrap_or(base_path);
        let (bucket, prefix) = path_without_scheme.split_once('/').map_or_else(
            || (path_without_scheme.to_string(), String::new()),
            |(b, p)| (b.to_string(), p.to_string()),
        );

        // Get S3 config from storage backend for endpoint/region settings
        let (endpoint, region) = match &config.storage.backend {
            StorageBackend::S3(s3_config) => (s3_config.endpoint.clone(), s3_config.region.clone()),
            _ => (None, "us-east-1".to_string()),
        };

        let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket).with_region(&region);

        if let Some(endpoint) = &endpoint {
            builder = builder.with_endpoint(endpoint).with_allow_http(true);
        }

        // Use environment variables for credentials
        builder = builder.with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default());
        builder = builder.with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default());

        let store = Arc::new(builder.build().expect("Failed to create S3 object store for queue"));

        // Update queue config to use just the prefix (bucket is handled by store)
        let mut queue_config = queue_config;
        queue_config.base_path = prefix;

        let writer = QueueWriter::new(queue_config, store);
        writer.start(write_rx)
    } else if base_path.starts_with("file://") || base_path.starts_with('/') {
        // Local filesystem
        let path = base_path.strip_prefix("file://").unwrap_or(base_path);
        let store = Arc::new(
            LocalFileSystem::new_with_prefix(path).expect("Failed to create local file system store for queue"),
        );
        let mut queue_config = queue_config;
        queue_config.base_path = String::new();
        let writer = QueueWriter::new(queue_config, store);
        writer.start(write_rx)
    } else {
        // Memory or relative path - use memory store
        let store = Arc::new(InMemory::new());
        let writer = QueueWriter::new(queue_config, store);
        writer.start(write_rx)
    };

    tracing::info!("WAL queue initialized successfully");

    // Create cancellation token for coordinated shutdown
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
        return Ok(());
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
        if let Err(e) = handle.await {
            tracing::error!("Server task failed: {}", e);
        }
    }

    tracing::info!("All OTLP servers stopped gracefully");

    Ok(())
}
