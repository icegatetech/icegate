//! Run command implementation

use std::{
    any::Any,
    path::PathBuf,
    sync::{Arc, mpsc},
    thread,
};

use icegate_common::{IoCacheHandle, MetricsRuntime, catalog::CatalogBuilder, create_object_store, run_metrics_server};
use icegate_queue::{NoopQueueWriterEvents, ParquetQueueReader, QueueConfig, QueueWriter, channel};
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;

use crate::{
    IngestConfig,
    error::{IngestError, Result},
    infra::metrics::{OtlpMetrics, ShiftMetrics, WalWriterMetrics},
    runtime_threads::compute_runtime_threads,
    shift::Shifter,
};

struct ShiftRuntimeHandle {
    shutdown_tx: mpsc::Sender<()>,
    join_handle: thread::JoinHandle<Result<()>>,
}

impl ShiftRuntimeHandle {
    fn shutdown(self) -> Result<()> {
        if self.shutdown_tx.send(()).is_err() {
            tracing::warn!("shift runtime shutdown channel is closed");
        }

        match self.join_handle.join() {
            Ok(result) => result,
            Err(panic) => Err(IngestError::Shift(format!(
                "shift runtime thread panicked: {}",
                panic_payload_to_string(&*panic)
            ))),
        }
    }
}

fn panic_payload_to_string(panic: &(dyn Any + Send)) -> String {
    panic.downcast_ref::<&str>().map_or_else(
        || {
            panic
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_else(|| "unknown panic".to_string())
        },
        |message| (*message).to_string(),
    )
}

fn resolve_shift_startup_failure(
    join_result: thread::Result<Result<()>>,
    fallback_error: Option<IngestError>,
) -> IngestError {
    match join_result {
        Ok(Err(err)) => err,
        Ok(Ok(())) => fallback_error
            .unwrap_or_else(|| IngestError::Shift("shift runtime exited before reporting startup status".to_string())),
        Err(panic) => IngestError::Shift(format!(
            "shift runtime thread panicked: {}",
            panic_payload_to_string(&*panic)
        )),
    }
}

fn spawn_shift_runtime(shifter: Shifter, shift_threads: usize) -> Result<ShiftRuntimeHandle> {
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
    let (startup_tx, startup_rx) = mpsc::sync_channel::<Result<()>>(1);

    let join_handle = thread::Builder::new()
        .name("icegate-shift-runtime".to_string())
        .spawn(move || -> Result<()> {
            let runtime = Builder::new_multi_thread()
                .worker_threads(shift_threads)
                .enable_all()
                .build()
                .map_err(IngestError::Io)?;

            let shifter_handle = {
                let _guard = runtime.enter();
                match shifter.start() {
                    Ok(handle) => {
                        let _ = startup_tx.send(Ok(()));
                        handle
                    }
                    Err(err) => {
                        let error = IngestError::Shift(err.to_string());
                        let _ = startup_tx.send(Err(IngestError::Shift(err.to_string())));
                        return Err(error);
                    }
                }
            };

            if shutdown_rx.recv().is_err() {
                tracing::debug!("shift runtime shutdown sender dropped, stopping");
            }

            runtime.block_on(async { shifter_handle.shutdown().await })?;
            Ok(())
        })
        .map_err(IngestError::Io)?;

    match startup_rx.recv() {
        Ok(Ok(())) => Ok(ShiftRuntimeHandle {
            shutdown_tx,
            join_handle,
        }),
        Ok(Err(err)) => Err(resolve_shift_startup_failure(join_handle.join(), Some(err))),
        Err(_) => Err(resolve_shift_startup_failure(join_handle.join(), None)),
    }
}

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
    let config = IngestConfig::from_file(&config_path)?;

    // Initialize tracing with OpenTelemetry
    let tracing_guard = icegate_common::init_tracing(&config.tracing)?;

    tracing::info!("Loading configuration from {:?}", config_path);
    tracing::info!("Configuration loaded successfully");

    // Initialize WAL queue based on queue config's base_path
    tracing::info!("Initializing WAL queue");
    let queue_config = config.queue.clone().unwrap_or_else(|| QueueConfig::new("wal"));
    let (write_tx, write_rx) = channel(queue_config.common.channel_capacity);

    // Create object store based on queue base_path
    // Ingest writes data — no read cache needed, pass None.
    let (store, normalized_path) = create_object_store(
        &queue_config.common.base_path,
        Some(&config.storage.backend),
        None,
        None,
    )?;

    // Update queue config with normalized base path
    let mut queue_config = queue_config;
    queue_config.common.base_path = normalized_path;

    let metrics_runtime = if config.metrics.enabled {
        Some(Arc::new(MetricsRuntime::new("ingest")?))
    } else {
        None
    };
    let wal_writer_metrics = metrics_runtime.as_ref().map_or_else(
        || WalWriterMetrics::new_disabled(Arc::new(NoopQueueWriterEvents)),
        |runtime| WalWriterMetrics::new(&runtime.meter(), Arc::new(NoopQueueWriterEvents)),
    );
    let writer = QueueWriter::new(queue_config.clone(), Arc::clone(&store)).with_events(Arc::new(wal_writer_metrics));
    let writer_handle = writer.start(write_rx);

    tracing::info!("WAL queue initialized successfully");

    // Initialize shifter (WAL -> Iceberg)
    tracing::info!("Initializing shifter");
    let io_cache = IoCacheHandle::from_config(config.catalog.cache.as_ref()).await?;
    let catalog = CatalogBuilder::from_config(&config.catalog, &io_cache).await?;
    let jobs_storage = config.shift.jobsmanager.storage.to_s3_config()?;
    let shift_config = Arc::new(config.shift.clone());
    let queue_reader = Arc::new(ParquetQueueReader::new(
        queue_config.common.base_path.clone(),
        Arc::clone(&store),
        queue_config.common.max_row_group_size,
    )?);
    let shift_metrics = metrics_runtime.as_ref().map_or_else(ShiftMetrics::new_disabled, |runtime| {
        ShiftMetrics::new(&runtime.meter())
    });
    let jobsmanager_metrics = metrics_runtime
        .as_ref()
        .map_or_else(icegate_jobmanager::Metrics::new_disabled, |runtime| {
            icegate_jobmanager::Metrics::new(&runtime.meter())
        });
    let otlp_metrics = metrics_runtime
        .as_ref()
        .map_or_else(OtlpMetrics::new_disabled, |runtime| OtlpMetrics::new(&runtime.meter()));
    let shifter = Shifter::new(
        catalog,
        queue_reader,
        shift_config,
        jobs_storage,
        shift_metrics,
        jobsmanager_metrics,
    )
    .await?;
    let runtime_plan = compute_runtime_threads();
    tracing::info!(
        available_parallelism = runtime_plan.total,
        main_runtime_threads = runtime_plan.main_threads,
        shift_runtime_threads = runtime_plan.shift_threads,
        "Runtime thread allocation resolved"
    );
    let shift_runtime = spawn_shift_runtime(shifter, runtime_plan.shift_threads)?;
    tracing::info!("Shifter started successfully on dedicated runtime");

    // Create a cancellation token for coordinated shutdown
    let cancel_token = CancellationToken::new();

    // Spawn server tasks
    let mut handles = Vec::new();

    if let Some(metrics_runtime) = metrics_runtime.as_ref() {
        let metrics_config = config.metrics.clone();
        let token = cancel_token.clone();
        let registry = metrics_runtime.registry();
        let handle = tokio::spawn(async move {
            run_metrics_server(metrics_config, registry, token)
                .await
                .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)
        });
        handles.push(handle);
    }

    // OTLP HTTP server
    if config.otlp_http.enabled {
        let write_channel = write_tx.clone();
        let http_config = config.otlp_http.clone();
        let token = cancel_token.clone();
        let metrics = otlp_metrics.clone();
        let handle =
            tokio::spawn(async move { crate::otlp_http::run(write_channel, metrics, http_config, token).await });
        handles.push(handle);
    }

    // OTLP gRPC server
    if config.otlp_grpc.enabled {
        let write_channel = write_tx.clone();
        let grpc_config = config.otlp_grpc.clone();
        let token = cancel_token.clone();
        let metrics = otlp_metrics.clone();
        let handle =
            tokio::spawn(async move { crate::otlp_grpc::run(write_channel, metrics, grpc_config, token).await });
        handles.push(handle);
    }

    let mut server_result = Ok(());
    if handles.is_empty() {
        tracing::warn!("No OTLP servers are enabled in configuration");
    } else {
        tracing::info!("All enabled OTLP servers started");
        tracing::info!("Press Ctrl+C or send SIGTERM to shutdown");

        // Wait for shutdown signal (SIGINT or SIGTERM)
        shutdown_signal().await;

        tracing::info!("Shutdown signal received, stopping all servers...");

        // Cancel all servers
        cancel_token.cancel();

        // Wait for all servers to stop
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    server_result = Err(IngestError::Other(err));
                    break;
                }
                Err(err) => {
                    server_result = Err(IngestError::Join(err));
                    break;
                }
            }
        }

        if server_result.is_ok() {
            tracing::info!("All OTLP servers stopped gracefully");
        }
    }

    // Close the write channel so the writer loop can exit
    drop(write_tx);

    // Wait for the writer task to finish
    let writer_result = writer_handle.await;

    tracing::info!("Stopping shifter...");
    let shift_result = shift_runtime.shutdown();
    if shift_result.is_ok() {
        tracing::info!("Shifter stopped gracefully");
    }

    // Gracefully close the IO cache to drain foyer's background flusher tasks.
    io_cache.close().await;

    // Keep tracing guard alive until the very end
    drop(tracing_guard);

    server_result?;
    shift_result?;
    writer_result??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::{IngestError, resolve_shift_startup_failure};

    #[test]
    fn startup_failure_prefers_join_error() {
        let error =
            resolve_shift_startup_failure(Ok(Err(IngestError::Io(io::Error::other("runtime build failed")))), None);
        assert!(matches!(error, IngestError::Io(_)));
        assert!(error.to_string().contains("runtime build failed"));
    }

    #[test]
    fn startup_failure_uses_fallback_when_join_is_ok() {
        let fallback = IngestError::Shift("reported startup error".to_string());
        let error = resolve_shift_startup_failure(Ok(Ok(())), Some(fallback));
        assert!(matches!(error, IngestError::Shift(_)));
        assert!(error.to_string().contains("reported startup error"));
    }

    #[test]
    fn startup_failure_reports_panic_payload() {
        let panic_payload: Box<dyn std::any::Any + Send> = Box::new("panic at startup");
        let error = resolve_shift_startup_failure(Err(panic_payload), None);
        assert!(matches!(error, IngestError::Shift(_)));
        assert!(error.to_string().contains("panic at startup"));
    }
}
