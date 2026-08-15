//! Run command implementation

use std::{
    any::Any,
    collections::HashMap,
    io,
    path::PathBuf,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use futures::stream::{FuturesUnordered, StreamExt};
use icegate_common::{
    IoHandle, LOGS_TABLE, LOGS_TOPIC, METRICS_TABLE, METRICS_TOPIC, MemoryPressure, MetricsRuntime, OPERATIONS_TABLE,
    OPERATIONS_TOPIC, SPANS_TABLE, SPANS_TOPIC,
    catalog::CatalogBuilder,
    parquet_encoding::{
        LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS, METRICS_BLOOM_COLUMNS, METRICS_COLUMN_ENCODINGS,
        OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS, SPANS_BLOOM_COLUMNS, SPANS_COLUMN_ENCODINGS,
    },
    run_metrics_server,
};
use icegate_queue::{
    CommittedOffsetsByTopic, NoopQueueWriterEvents, ParquetQueueReader, QueueConfig, QueueWriter, channel,
};
use object_store::ObjectStore;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    IngestConfig,
    error::{IngestError, Result},
    infra::metrics::{
        ObjectStoreMetricsDecorator, OtlpMetrics, QueueReaderS3Metrics, QueueWriterS3Metrics, ShiftMetrics,
        WalWriterMetrics,
    },
    runtime_threads::compute_runtime_threads,
    shift::{ShiftJobSpec, Shifter},
    wal::SortColumnsDescriptor,
};

struct ShiftRuntimeHandle {
    shutdown_tx: mpsc::Sender<()>,
    join_handle: thread::JoinHandle<Result<()>>,
}

struct WalRuntimeHandle {
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

impl WalRuntimeHandle {
    fn shutdown(self) -> Result<()> {
        match self.join_handle.join() {
            Ok(result) => result,
            Err(panic) => Err(IngestError::Other(Box::new(io::Error::other(format!(
                "wal runtime thread panicked: {}",
                panic_payload_to_string(&*panic)
            ))))),
        }
    }
}

type ServerTaskResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
type ServerTaskHandle = tokio::task::JoinHandle<ServerTaskResult>;

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

fn resolve_wal_startup_failure(
    join_result: thread::Result<Result<()>>,
    fallback_error: Option<IngestError>,
) -> IngestError {
    match join_result {
        Ok(Err(err)) => err,
        Ok(Ok(())) => fallback_error.unwrap_or_else(|| {
            IngestError::Other(Box::new(io::Error::other(
                "wal runtime exited before reporting startup status",
            )))
        }),
        Err(panic) => IngestError::Other(Box::new(io::Error::other(format!(
            "wal runtime thread panicked: {}",
            panic_payload_to_string(&*panic)
        )))),
    }
}

fn spawn_wal_runtime(
    writer: QueueWriter,
    write_rx: icegate_queue::WriteReceiver,
    wal_threads: usize,
) -> Result<WalRuntimeHandle> {
    let (startup_tx, startup_rx) = mpsc::sync_channel::<Result<()>>(1);

    let join_handle = thread::Builder::new()
        .name("icegate-wal-runtime".to_string())
        .spawn(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(wal_threads)
                .thread_name("icegate-wal")
                .enable_all()
                .build()
                .map_err(IngestError::Io)?;

            // Readiness is reported only once offset recovery inside `start` has
            // succeeded: a queue whose counter cannot be recovered would
            // otherwise be reported as started and only fail on the first write,
            // long after the process claimed to be up.
            let writer_handle = match runtime.block_on(writer.start(write_rx)) {
                Ok(handle) => {
                    let _ = startup_tx.send(Ok(()));
                    handle
                }
                Err(err) => {
                    let error = IngestError::Queue(err);
                    let _ = startup_tx.send(Err(IngestError::Other(Box::new(io::Error::other(error.to_string())))));
                    return Err(error);
                }
            };

            runtime.block_on(writer_handle).map_err(IngestError::Join)??;
            Ok(())
        })
        .map_err(IngestError::Io)?;

    match startup_rx.recv() {
        Ok(Ok(())) => Ok(WalRuntimeHandle { join_handle }),
        Ok(Err(err)) => Err(resolve_wal_startup_failure(join_handle.join(), Some(err))),
        Err(_) => Err(resolve_wal_startup_failure(join_handle.join(), None)),
    }
}

/// Hard deadline for reading one table's committed WAL offset at startup.
///
/// A catalog that does not answer must not hold ingest back: OTLP reception is
/// the service's job, and a missing offset only costs the writer its head start
/// (it then recovers the counter from the segments themselves). The deadline is
/// a constant rather than a config field because a second deployment-config pair
/// ([`AGENTS.md`] "Deployment configs come in pairs") for one number is a worse
/// trade than a value nobody tunes.
const COMMITTED_OFFSET_TIMEOUT: Duration = Duration::from_secs(10);

/// Read, per shift job, the WAL offset its Iceberg table records as committed.
///
/// The result seeds [`QueueWriter::with_committed_offsets`], which is what lets
/// the writer resume correctly on a queue whose lower segments WAL cleanup has
/// already removed.
///
/// A topic is left OUT of the map — never defaulted to zero — whenever the
/// answer cannot be trusted: the table does not exist yet, the catalog is slow
/// or down, or the offset cannot be resolved from the snapshot chain. Ingest
/// then starts and receives data with the writer recovering from segments alone,
/// which is exactly the previous behaviour.
///
/// The lookups run concurrently — they share nothing but the catalog — so a
/// catalog that accepts connections and then goes quiet costs one `timeout` in
/// total rather than one per topic. That delay is paid before the OTLP listeners
/// bind, where it counts against the pod's readiness probe.
async fn resolve_wal_committed_offsets(
    catalog: &Arc<dyn iceberg::Catalog>,
    jobs: &[ShiftJobSpec],
    timeout: Duration,
) -> CommittedOffsetsByTopic {
    let mut lookups: FuturesUnordered<_> = jobs
        .iter()
        .map(|job| async move {
            let ident = icegate_common::icegate_table_ident(job.table);
            (job, tokio::time::timeout(timeout, catalog.load_table(&ident)).await)
        })
        .collect();

    let mut offsets = CommittedOffsetsByTopic::with_capacity(jobs.len());
    while let Some((job, loaded)) = lookups.next().await {
        match loaded {
            Ok(Ok(table)) => match icegate_common::resolve_committed_offset(table.metadata()) {
                Ok(Some(offset)) => {
                    tracing::info!(
                        topic = job.topic,
                        table = job.table,
                        offset,
                        "Resolved committed WAL offset"
                    );
                    offsets.insert(job.topic.to_string(), offset);
                }
                Ok(None) => tracing::info!(
                    topic = job.topic,
                    table = job.table,
                    "Table has no snapshot yet, WAL recovery starts from the segments"
                ),
                Err(error) => tracing::warn!(
                    topic = job.topic,
                    table = job.table,
                    %error,
                    "Failed to resolve the committed WAL offset, WAL recovery starts from the segments"
                ),
            },
            Ok(Err(error)) => tracing::warn!(
                topic = job.topic,
                table = job.table,
                %error,
                "Failed to load the table, WAL recovery starts from the segments"
            ),
            Err(_elapsed) => tracing::warn!(
                topic = job.topic,
                table = job.table,
                timeout_secs = timeout.as_secs(),
                "Catalog did not answer in time, WAL recovery starts from the segments"
            ),
        }
    }
    offsets
}

fn spawn_shift_runtime(shifter: Shifter, shift_threads: usize) -> Result<ShiftRuntimeHandle> {
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();
    let (startup_tx, startup_rx) = mpsc::sync_channel::<Result<()>>(1);

    let join_handle = thread::Builder::new()
        .name("icegate-shift-runtime".to_string())
        .spawn(move || -> Result<()> {
            let mut builder = Builder::new_multi_thread();
            builder.worker_threads(shift_threads).enable_all();
            #[cfg(tokio_unstable)]
            builder.enable_metrics_poll_time_histogram();
            let runtime = builder.build().map_err(IngestError::Io)?;

            let shifter_handle = {
                let _guard = runtime.enter();
                opentelemetry_instrumentation_tokio::Config::new()
                    .with_label("runtime.name", "shift")
                    .observe_current_runtime();
                info!("Shift starting on runtime {}", runtime.handle().id());

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

async fn run_servers_until_shutdown(handles: Vec<ServerTaskHandle>, cancel_token: &CancellationToken) -> Result<()> {
    if handles.is_empty() {
        tracing::warn!("No OTLP servers are enabled in configuration");
        return Ok(());
    }

    tracing::info!("All enabled OTLP servers started");
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
                            IngestError::Other(Box::new(io::Error::other(
                                "server task exited before shutdown signal",
                            )))
                        });
                    }
                    Ok(Err(err)) => {
                        tracing::error!("Server task failed: {err}");
                        failure.get_or_insert(IngestError::Other(err));
                    }
                    Err(err) => {
                        tracing::error!("Server task join failed: {err}");
                        failure.get_or_insert(IngestError::Join(err));
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
        return Err(err);
    }

    tracing::info!("All OTLP servers stopped gracefully");
    Ok(())
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

    // Initialize metrics early so that the global meter provider is available
    // for OpenDAL's OtelMetricsLayer and Iceberg's IceGateStorageFactory.
    let metrics_runtime = if config.metrics.enabled {
        Some(Arc::new(MetricsRuntime::new("ingest")?))
    } else {
        None
    };

    // Initialize WAL queue based on queue config's base_path
    tracing::info!("Initializing WAL queue");
    let queue_config = config.queue.clone().unwrap_or_else(|| QueueConfig::new("wal"));
    let (write_tx, write_rx) = channel(queue_config.common.channel_capacity);

    let io_cache = IoHandle::from_config(config.catalog.cache.as_ref(), Some(&config.storage.backend)).await?;

    // Run the fallible startup/run path, then always close the IO cache.
    // Foyer's background flusher tasks need a clean close to avoid channel errors.
    let result = run_services(
        &config,
        &io_cache,
        metrics_runtime.as_ref(),
        queue_config,
        write_tx,
        write_rx,
    )
    .await;

    // Gracefully close the IO cache to drain foyer's background flusher tasks.
    // This runs regardless of whether the startup/run path succeeded or failed.
    io_cache.close().await;

    // Keep tracing guard alive until the very end
    drop(tracing_guard);

    result
}

/// Start all services (WAL writer, shifter, OTLP servers) and run until
/// shutdown, returning the combined result.
///
/// Extracted from [`execute`] so that `io_cache.close()` is always called
/// in the caller regardless of early `?` returns here.
#[allow(clippy::too_many_lines)]
async fn run_services(
    config: &IngestConfig,
    io_cache: &IoHandle,
    metrics_runtime: Option<&Arc<MetricsRuntime>>,
    queue_config: QueueConfig,
    write_tx: icegate_queue::WriteChannel,
    write_rx: icegate_queue::WriteReceiver,
) -> Result<()> {
    if let (Some(cache), Some(runtime)) = (io_cache.cache(), metrics_runtime.as_ref()) {
        icegate_common::register_foyer_metrics(cache, &runtime.meter());
    }

    // Create object store based on queue base_path.
    // Read cache, prefetch, and stat TTL come from io_cache, which the
    // shifter's queue reader shares through this store.
    let (store, normalized_path) = io_cache
        .object_store_operator_registry()
        .resolve_object_store(&queue_config.common.base_path)?;

    // Update queue config with normalized base path
    let mut queue_config = queue_config;
    queue_config.common.base_path = normalized_path;
    let wal_writer_metrics = metrics_runtime.as_ref().map_or_else(
        || WalWriterMetrics::new_disabled(Arc::new(NoopQueueWriterEvents)),
        |runtime| WalWriterMetrics::new(&runtime.meter(), Arc::new(NoopQueueWriterEvents)),
    );
    let queue_writer_store: Arc<dyn ObjectStore> = metrics_runtime.as_ref().map_or_else(
        || Arc::clone(&store),
        |runtime| {
            Arc::new(ObjectStoreMetricsDecorator::new(
                Arc::clone(&store),
                QueueWriterS3Metrics::new(&runtime.meter()),
            ))
        },
    );
    // Match the per-table bloom-filter policy used by the iceberg
    // shift writer (see `parquet_encoding::{LOGS,SPANS}_BLOOM_COLUMNS`
    // and `ShiftJobSpec::bloom_filter_columns`) so equality lookups on
    // `trace_id` / `span_id` skip row groups in fresh WAL data too —
    // the query engine reads from both WAL and Iceberg, and Tempo
    // trace-by-id / Loki LogQL `{trace_id="..."}` hit either.
    let wal_bloom_filter_columns = HashMap::from([
        (LOGS_TOPIC.to_string(), LOGS_BLOOM_COLUMNS),
        (SPANS_TOPIC.to_string(), SPANS_BLOOM_COLUMNS),
        (OPERATIONS_TOPIC.to_string(), OPERATIONS_BLOOM_COLUMNS),
    ]);
    let wal_column_encodings = HashMap::from([
        (LOGS_TOPIC.to_string(), LOGS_COLUMN_ENCODINGS),
        (SPANS_TOPIC.to_string(), SPANS_COLUMN_ENCODINGS),
        (METRICS_TOPIC.to_string(), METRICS_COLUMN_ENCODINGS),
        (OPERATIONS_TOPIC.to_string(), OPERATIONS_COLUMN_ENCODINGS),
    ]);
    // Cancellation token for coordinated shutdown. Created before the catalog
    // so the S3 catalog's CAS/transient retry loops abort promptly on SIGINT/
    // SIGTERM instead of running to their retry budget during shutdown.
    let cancel_token = CancellationToken::new();

    // Process-wide memory-pressure guard shared by every OTLP surface. Inert (never
    // sheds, no sampler) when no finite cgroup memory limit is detected (dev/CI/
    // bare-metal); one sampler is bound to `cancel_token` when a limit exists.
    let memory_pressure = MemoryPressure::spawn(config.memory_pressure.clone(), cancel_token.clone());

    // The catalog is built BEFORE the WAL writer starts: the writer's offset
    // recovery needs the committed offsets that only the catalog can answer for
    // (see `resolve_wal_committed_offsets`). The shifter below is built from this
    // same handle, so nothing is constructed twice.
    tracing::info!("Initializing catalog");
    let catalog = CatalogBuilder::from_config(&config.catalog, io_cache, cancel_token.clone()).await?;
    let jobs: &[ShiftJobSpec] = &[
        ShiftJobSpec {
            job_name: "shift_logs",
            topic: LOGS_TOPIC,
            table: LOGS_TABLE,
            descriptor: SortColumnsDescriptor::logs()?,
            planner_partition_spec: &crate::shift::CURRENT_PLANNER_PARTITION_SPEC,
            bloom_filter_columns: LOGS_BLOOM_COLUMNS,
            column_encodings: LOGS_COLUMN_ENCODINGS,
        },
        ShiftJobSpec {
            job_name: "shift_spans",
            topic: SPANS_TOPIC,
            table: SPANS_TABLE,
            descriptor: SortColumnsDescriptor::spans()?,
            planner_partition_spec: &crate::shift::CURRENT_PLANNER_PARTITION_SPEC,
            bloom_filter_columns: SPANS_BLOOM_COLUMNS,
            column_encodings: SPANS_COLUMN_ENCODINGS,
        },
        ShiftJobSpec {
            job_name: "shift_metrics",
            topic: METRICS_TOPIC,
            table: METRICS_TABLE,
            descriptor: SortColumnsDescriptor::metrics()?,
            planner_partition_spec: &crate::shift::CURRENT_PLANNER_PARTITION_SPEC,
            bloom_filter_columns: METRICS_BLOOM_COLUMNS,
            column_encodings: METRICS_COLUMN_ENCODINGS,
        },
        ShiftJobSpec {
            job_name: "shift_operations",
            topic: OPERATIONS_TOPIC,
            table: OPERATIONS_TABLE,
            descriptor: SortColumnsDescriptor::operations()?,
            planner_partition_spec: &crate::shift::CURRENT_PLANNER_PARTITION_SPEC,
            bloom_filter_columns: OPERATIONS_BLOOM_COLUMNS,
            column_encodings: OPERATIONS_COLUMN_ENCODINGS,
        },
    ];
    let committed_offsets = resolve_wal_committed_offsets(&catalog, jobs, COMMITTED_OFFSET_TIMEOUT).await;

    let writer = QueueWriter::new(queue_config.clone(), queue_writer_store)
        .with_events(Arc::new(wal_writer_metrics))
        .with_bloom_filter_columns(wal_bloom_filter_columns)
        .with_column_encodings(wal_column_encodings)
        .with_committed_offsets(committed_offsets);

    // Run the WAL writer on a dedicated runtime so flush I/O is not
    // blocked by OTLP request processing on the main runtime.
    let wal_threads = compute_runtime_threads();
    let wal_runtime = spawn_wal_runtime(writer, write_rx, wal_threads.main_threads)?;

    tracing::info!("WAL queue initialized on dedicated runtime");

    // Initialize shifter (WAL -> Iceberg)
    tracing::info!("Initializing shifter");
    let jobs_storage = config.shift.jobsmanager.storage.to_s3_config()?;
    let shift_config = Arc::new(config.shift.clone());
    let queue_reader_store: Arc<dyn ObjectStore> = metrics_runtime.as_ref().map_or_else(
        || Arc::clone(&store),
        |runtime| {
            Arc::new(ObjectStoreMetricsDecorator::new(
                Arc::clone(&store),
                QueueReaderS3Metrics::new(&runtime.meter()),
            ))
        },
    );
    let queue_reader = Arc::new(
        ParquetQueueReader::new(
            queue_config.common.base_path.clone(),
            queue_reader_store,
            queue_config.common.max_row_group_size,
        )?
        .with_plan_segment_read_parallelism(shift_config.read.plan_segment_read_parallelism)?,
    );
    let shift_metrics = metrics_runtime.as_ref().map_or_else(ShiftMetrics::new_disabled, |runtime| {
        ShiftMetrics::new(&runtime.meter())
    });
    // Without a metrics runtime there is no meter to record into, so the pool is handed the
    // crate's no-op sink rather than instruments nothing collects.
    let jobsmanager_metrics: Arc<dyn jobmanager::MetricsSink> = metrics_runtime.as_ref().map_or_else(
        || Arc::new(jobmanager::NoopMetrics) as Arc<dyn jobmanager::MetricsSink>,
        |runtime| Arc::new(jobmanager::OtelMetrics::new(&runtime.meter())),
    );
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
        jobs,
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
        let wal_row_group_size = queue_config.common.max_row_group_size;
        let operations_enabled = config.operations.enabled;
        let http_config = config.otlp_http.clone();
        let token = cancel_token.clone();
        let metrics = otlp_metrics.clone();
        let guard = memory_pressure.clone();
        let handle = tokio::spawn(async move {
            crate::otlp_http::run(
                write_channel,
                wal_row_group_size,
                operations_enabled,
                metrics,
                http_config,
                token,
                guard,
            )
            .await
        });
        handles.push(handle);
    }

    // OTLP gRPC server
    if config.otlp_grpc.enabled {
        let write_channel = write_tx.clone();
        let wal_row_group_size = queue_config.common.max_row_group_size;
        let operations_enabled = config.operations.enabled;
        let grpc_config = config.otlp_grpc.clone();
        let token = cancel_token.clone();
        let metrics = otlp_metrics.clone();
        let guard = memory_pressure.clone();
        let handle = tokio::spawn(async move {
            crate::otlp_grpc::run(
                write_channel,
                wal_row_group_size,
                operations_enabled,
                metrics,
                grpc_config,
                token,
                guard,
            )
            .await
        });
        handles.push(handle);
    }

    let server_result = run_servers_until_shutdown(handles, &cancel_token).await;

    // Close the write channel so the writer loop can exit
    drop(write_tx);

    // Wait for the writer task to finish on its dedicated runtime thread.
    let writer_result = wal_runtime.shutdown();

    tracing::info!("Stopping shifter...");
    let shift_result = shift_runtime.shutdown();
    if shift_result.is_ok() {
        tracing::info!("Shifter stopped gracefully");
    }

    server_result?;
    shift_result?;
    writer_result?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::pending;
    use std::io;
    use std::sync::Arc;

    use iceberg::table::Table;
    use iceberg::transaction::ApplyTransactionAction;
    use iceberg::{Namespace, NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation, TableIdent};
    use icegate_common::{LOGS_TABLE, LOGS_TOPIC, parquet_encoding::LOGS_BLOOM_COLUMNS};

    use super::{
        COMMITTED_OFFSET_TIMEOUT, IngestError, LOGS_COLUMN_ENCODINGS, ShiftJobSpec, SortColumnsDescriptor,
        resolve_shift_startup_failure, resolve_wal_committed_offsets,
    };

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

    /// One shift job over the `logs` table, enough to drive
    /// [`super::resolve_wal_committed_offsets`] without the other three.
    fn logs_job() -> Vec<ShiftJobSpec> {
        vec![ShiftJobSpec {
            job_name: "shift_logs",
            topic: LOGS_TOPIC,
            table: LOGS_TABLE,
            descriptor: SortColumnsDescriptor::logs().expect("logs sort descriptor"),
            planner_partition_spec: &crate::shift::CURRENT_PLANNER_PARTITION_SPEC,
            bloom_filter_columns: LOGS_BLOOM_COLUMNS,
            column_encodings: LOGS_COLUMN_ENCODINGS,
        }]
    }

    /// A table whose newest snapshot records the offset hands it to the WAL
    /// writer, so recovery starts from the committed boundary rather than from
    /// whatever segments happen to survive.
    #[tokio::test]
    async fn committed_offsets_carry_the_offset_the_table_records() {
        const COMMITTED_OFFSET: u64 = 42;

        let (catalog, table) = crate::shift::test_utils::create_logs_table(LOGS_TABLE).await;
        let tx = iceberg::transaction::Transaction::new(&table);
        let tx = tx
            .fast_append()
            .set_snapshot_properties(HashMap::from([(
                icegate_common::WAL_OFFSET_PROPERTY.to_string(),
                COMMITTED_OFFSET.to_string(),
            )]))
            .apply(tx)
            .expect("apply fast append");
        tx.commit(catalog.as_ref()).await.expect("commit snapshot");

        let offsets = resolve_wal_committed_offsets(&catalog, &logs_job(), COMMITTED_OFFSET_TIMEOUT).await;

        assert_eq!(offsets.get(LOGS_TOPIC), Some(&COMMITTED_OFFSET));
    }

    /// A catalog that cannot answer must NOT stop ingest: the topic is left out
    /// of the map, the writer recovers from the segments, and OTLP reception
    /// starts. Here the table simply does not exist, which is the same failure
    /// shape as an unreachable catalog from this function's point of view.
    #[tokio::test]
    async fn an_unavailable_table_yields_an_empty_map_rather_than_an_error() {
        let (catalog, _table) = crate::shift::test_utils::create_logs_table("some_other_table").await;

        let offsets = resolve_wal_committed_offsets(&catalog, &logs_job(), COMMITTED_OFFSET_TIMEOUT).await;

        assert!(offsets.is_empty());
    }

    /// A freshly created table has no snapshot and therefore no committed
    /// offset. That is not a failure and not offset 0 — the topic is absent, and
    /// the writer recovers from the segments alone.
    #[tokio::test]
    async fn a_table_without_snapshots_is_absent_from_the_map() {
        let (catalog, _table) = crate::shift::test_utils::create_logs_table(LOGS_TABLE).await;

        let offsets = resolve_wal_committed_offsets(&catalog, &logs_job(), COMMITTED_OFFSET_TIMEOUT).await;

        assert!(offsets.is_empty());
    }

    /// A table WITH snapshots but no recorded offset anywhere in the chain is
    /// the broken-invariant case, and the topic must stay out of the map. A zero
    /// substituted here would be far worse than the missing head start it looks
    /// like: on a topic whose lower segments cleanup has removed, a committed
    /// offset of 0 makes recovery declare the queue unresumable and takes ingest
    /// down at startup.
    #[tokio::test]
    async fn an_unresolvable_offset_leaves_the_topic_out_of_the_map() {
        let (catalog, table) = crate::shift::test_utils::create_logs_table(LOGS_TABLE).await;
        let tx = iceberg::transaction::Transaction::new(&table);
        // Some property, deliberately not the WAL offset: a snapshot carrying
        // neither data files nor properties cannot be committed at all.
        let tx = tx
            .fast_append()
            .set_snapshot_properties(HashMap::from([("icegate.test.commit".to_string(), "1".to_string())]))
            .apply(tx)
            .expect("apply fast append");
        tx.commit(catalog.as_ref()).await.expect("commit snapshot");

        let offsets = resolve_wal_committed_offsets(&catalog, &logs_job(), COMMITTED_OFFSET_TIMEOUT).await;

        assert!(offsets.is_empty());
    }

    /// A catalog that accepts the connection and then goes silent must cost
    /// ingest the timeout and nothing more: OTLP reception is the service's job,
    /// and the listeners bind only after this returns. The paused clock is what
    /// makes the deadline observable without waiting for it.
    #[tokio::test(start_paused = true)]
    async fn a_silent_catalog_costs_the_timeout_and_no_more() {
        let catalog: Arc<dyn iceberg::Catalog> = Arc::new(StallingCatalog);

        // The outer bound is what turns a lookup that never gives up into a
        // failed test rather than a hung one; on the paused clock both deadlines
        // are virtual, so the inner one still decides the outcome.
        let offsets = tokio::time::timeout(
            COMMITTED_OFFSET_TIMEOUT * 2,
            resolve_wal_committed_offsets(&catalog, &logs_job(), COMMITTED_OFFSET_TIMEOUT),
        )
        .await
        .expect("a silent catalog must not hold ingest past its own deadline");

        assert!(offsets.is_empty());
    }

    /// Catalog whose every call never completes.
    ///
    /// Models the failure the startup deadline exists for — a catalog that
    /// accepted the request and then went quiet — without a sleep whose duration
    /// the test would have to guess.
    #[derive(Debug)]
    struct StallingCatalog;

    #[async_trait::async_trait]
    impl iceberg::Catalog for StallingCatalog {
        async fn list_namespaces(&self, _parent: Option<&NamespaceIdent>) -> IcebergResult<Vec<NamespaceIdent>> {
            pending().await
        }

        async fn create_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> IcebergResult<Namespace> {
            pending().await
        }

        async fn get_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
            pending().await
        }

        async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> IcebergResult<bool> {
            pending().await
        }

        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> IcebergResult<()> {
            pending().await
        }

        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<()> {
            pending().await
        }

        async fn list_tables(&self, _namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
            pending().await
        }

        async fn create_table(&self, _namespace: &NamespaceIdent, _creation: TableCreation) -> IcebergResult<Table> {
            pending().await
        }

        async fn load_table(&self, _table: &TableIdent) -> IcebergResult<Table> {
            pending().await
        }

        async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
            pending().await
        }

        async fn purge_table(&self, _table: &TableIdent) -> IcebergResult<()> {
            pending().await
        }

        async fn table_exists(&self, _table: &TableIdent) -> IcebergResult<bool> {
            pending().await
        }

        async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
            pending().await
        }

        async fn register_table(&self, _table: &TableIdent, _metadata_location: String) -> IcebergResult<Table> {
            pending().await
        }

        async fn update_table(&self, _commit: TableCommit) -> IcebergResult<Table> {
            pending().await
        }
    }
}
