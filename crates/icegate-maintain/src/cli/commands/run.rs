//! Run command implementation
//!
//! Starts the long-running maintenance services (Parquet compaction and, when
//! enabled, orphan-file GC and the LLM pricing crawler) and runs them until a
//! shutdown signal (SIGINT or SIGTERM) is received, then drains all worker
//! pools gracefully.

use std::path::PathBuf;
use std::sync::Arc;

use icegate_common::{CatalogBuilder, IoHandle, MetricsRuntime, run_metrics_server};
use icegate_queue::QueueCleaner;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    compact::{Compactor, CompactorHandle},
    config::MaintainConfig,
    error::MaintainError,
    gc::{GcRunner, GcRunnerHandle, GcRunnerSpec},
    pricing::{PricingRunner, PricingRunnerHandle},
    wal_cleanup::runner::{WalCleanupRunner, WalCleanupRunnerHandle},
};

/// The worker pools this command has started, in start order.
///
/// Every pool is optional because the services are brought up one at a time and
/// a failure part-way through has to drain exactly what is already running —
/// [`Self::drain`] is that one path, used by the failure exits and by the
/// shutdown at the end alike.
#[derive(Default)]
struct MaintenanceServices {
    compactor: Option<CompactorHandle>,
    gc: Option<GcRunnerHandle>,
    wal_cleanup: Option<WalCleanupRunnerHandle>,
    pricing: Option<PricingRunnerHandle>,
}

impl MaintenanceServices {
    /// Drain every started pool, then report the FIRST failure.
    ///
    /// Every pool is drained whatever the others do: returning early would leave
    /// a running worker behind a failure it had nothing to do with. Failures
    /// after the first are logged here, named, rather than discarded — the
    /// return value can only carry one.
    ///
    /// # Errors
    ///
    /// Returns the first [`MaintainError`] a pool stopped with.
    async fn drain(self) -> Result<(), MaintainError> {
        let mut outcomes: Vec<(&'static str, Result<(), MaintainError>)> = Vec::with_capacity(4);
        if let Some(handle) = self.compactor {
            outcomes.push(("compactor", handle.shutdown().await));
        }
        if let Some(handle) = self.gc {
            outcomes.push(("gc", handle.shutdown().await));
        }
        if let Some(handle) = self.wal_cleanup {
            outcomes.push(("wal cleanup", handle.shutdown().await));
        }
        if let Some(handle) = self.pricing {
            outcomes.push(("pricing", handle.shutdown().await));
        }

        let mut first_error: Option<MaintainError> = None;
        for (service, outcome) in outcomes {
            if let Err(error) = outcome {
                if first_error.is_none() {
                    first_error = Some(error);
                } else {
                    tracing::error!("{service} drain error (suppressed by an earlier drain failure): {error}");
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

/// The metrics server task, or `None` when metrics are disabled.
type MetricsServerTask = Option<JoinHandle<icegate_common::error::Result<()>>>;

/// Unwind a partially started service stack and return the failure that caused
/// it.
///
/// One path for every startup failure: cancel the shared token so in-flight
/// catalog retries stop, drain whatever is already running, then stop the
/// metrics server. Without it each new service adds another copy of this block
/// to the startup sequence, and a copy that forgets one of the three steps leaks
/// a worker pool or the metrics task.
async fn abort_startup(
    error: MaintainError,
    cancel_token: &CancellationToken,
    services: MaintenanceServices,
    metrics_cancel: &CancellationToken,
    metrics_server: MetricsServerTask,
) -> MaintainError {
    cancel_token.cancel();
    if let Err(drain_error) = services.drain().await {
        tracing::error!("worker drain error after a startup failure: {drain_error}");
    }
    metrics_cancel.cancel();
    if let Some(server) = metrics_server {
        let _ = server.await;
    }
    error
}

/// Execute the run command.
///
/// Loads the maintain configuration, initialises tracing, starts the Prometheus
/// metrics server (when `metrics.enabled`), builds the Iceberg catalog, starts
/// the compaction service and (when enabled) the orphan-file GC service, WAL
/// cleanup, and the LLM pricing crawler, then blocks until a shutdown signal
/// arrives. On shutdown all worker pools are drained and the metrics server is
/// stopped before returning.
///
/// # Errors
///
/// Returns [`MaintainError`] if the configuration cannot be loaded, the tracing
/// block is invalid or its subscriber cannot be initialised, the metrics runtime
/// cannot be built, the catalog cannot be built, any of the four services cannot
/// start, or any worker pool stops with an error during shutdown.
pub async fn execute(config_path: PathBuf) -> Result<(), MaintainError> {
    let config = MaintainConfig::from_file(&config_path).map_err(|e| MaintainError::Config(e.to_string()))?;

    // Tracing first: everything below logs, and the subscriber must be installed
    // before the first event so no startup line is dropped. `MaintainConfig` does
    // not validate this block (see its `validate` doc), so validate it here — the
    // sample ratio and the endpoint requirement are checked before any exporter is
    // built. The guard flushes pending spans when it drops at end of scope.
    config.tracing.validate()?;
    let _tracing_guard = icegate_common::init_tracing(&config.tracing)?;

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
    let mut services = MaintenanceServices::default();
    let io = match start_services(&mut services, &config, &cancel_token).await {
        Ok(io) => io,
        Err(error) => {
            return Err(abort_startup(error, &cancel_token, services, &metrics_cancel, metrics_server).await);
        }
    };

    // Wait for shutdown, but watch the metrics server task at the same time: it
    // only resolves before a shutdown signal if it failed to start (e.g. its port
    // is already bound). Surfacing that here aborts the service immediately rather
    // than deferring the error until shutdown, which could be days later.
    let (metrics_server, metrics_startup_error) = wait_for_shutdown(metrics_server).await;

    // Abort in-flight catalog CAS/transient retry loops before draining: a commit
    // caught mid-retry at SIGTERM stops at the next checkpoint instead of running
    // out its full retry budget, so the worker drain below completes promptly.
    cancel_token.cancel();
    tracing::info!("draining maintenance workers");
    // Defer surfacing a worker-drain failure until the metrics server has been
    // stopped below, so a drain error never leaks the metrics task.
    let drain_result = services.drain().await;

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
    // Closed once nothing reads through it any more — the pools have drained and
    // the metrics server has unbound — as [`IoHandle::close`] requires. This is
    // the ordinary stop; a startup failure never reaches here and drops the
    // handle instead, which is safe only while maintain configures no cache (see
    // [`start_services`]).
    io.close().await;
    drop(metrics_runtime);

    // Surface a worker-drain failure first, then any deferred metrics startup
    // error — both only after the metrics server has unbound and the runtime has
    // been dropped, so shutdown still runs in order on either error path.
    drain_result?;
    if let Some(err) = metrics_startup_error {
        return Err(err);
    }
    Ok(())
}

/// Bring up the IO handle, the catalog, and every enabled worker pool, in start
/// order.
///
/// Each pool that starts is recorded in `services` before the next one is built,
/// so a failure part-way through leaves the caller holding exactly what is
/// running. Every step leaves through one `?`: that is what makes
/// [`abort_startup`] the single unwind path it claims to be — a step wired past
/// it would leak the pools already started and the metrics task.
///
/// Returns the [`IoHandle`] the catalog and the sweeps were built from; the
/// caller owns it for the lifetime of the service and closes it after the drain.
/// A failure part-way through returns no handle: the one built here is dropped
/// unclosed, which stays safe only because the handle carries no cache — give
/// maintain one, and it has to be created by the caller so both exits can close
/// it.
///
/// # Errors
///
/// Returns [`MaintainError`] if the storage backend or the catalog cannot be
/// built, or if any of the four services fails to start.
async fn start_services(
    services: &mut MaintenanceServices,
    config: &MaintainConfig,
    cancel_token: &CancellationToken,
) -> Result<IoHandle, MaintainError> {
    // Maintenance reads table data through the catalog and never through the
    // foyer cache, so the handle carries no cache — only the storage backend
    // the GC sweep addresses, and the operator registries built from it.
    let io = IoHandle::from_config(None, Some(&config.storage.backend)).await?;
    let catalog = CatalogBuilder::from_config(&config.catalog, &io, cancel_token.clone()).await?;
    let compactor = Compactor::new(Arc::clone(&catalog), &config.compaction).await?;
    services.compactor = Some(compactor.start()?);
    tracing::info!("compaction maintenance service started");

    // Start the orphan-file sweep when enabled. Each runner is dropped after
    // `start()`; the returned handle owns the workers.
    if config.gc.enabled {
        let spec = GcRunnerSpec {
            operator_registry: io.object_store_operator_registry(),
            config: config.gc.clone(),
        };
        services.gc = Some(GcRunner::new(Arc::clone(&catalog), spec).await?.start()?);
        tracing::info!("orphan-file gc service started");
    }

    // WAL cleanup runs on a pool of its own, so a sweep holding a worker cannot
    // stand between two cleanup cycles.
    if config.wal_cleanup.enabled {
        let cleaner = build_wal_cleaner(&io, config)?;
        services.wal_cleanup = Some(
            WalCleanupRunner::new(Arc::clone(&catalog), cleaner, config.wal_cleanup.clone())
                .await?
                .start()?,
        );
        tracing::info!("wal cleanup service started");
    }

    // Start the pricing crawler alongside the others when enabled.
    if config.pricing.enabled {
        services.pricing = Some(PricingRunner::new(Arc::clone(&catalog), &config.pricing).await?.start()?);
        tracing::info!("pricing crawler service started");
    }

    Ok(io)
}

/// Build the cleaner addressing the WAL queue.
///
/// The WAL lives in its own bucket, distinct from the one holding the tables,
/// and the operator registry keys operators by the bucket in the path — so the
/// same storage credentials that reach the tables have to reach the queue too.
/// Resolving it here, before any job is registered, turns a misconfigured
/// `queue.common.base_path` into a failed startup instead of a job that fails
/// every cycle.
///
/// Called only while cleanup is enabled, and only after the config has been
/// loaded — `MaintainConfig::validate` has already rejected a `base_path` that
/// names no persistent store, so what remains here is resolving it.
///
/// # Errors
///
/// Returns [`MaintainError`] when the queue's object store cannot be resolved.
fn build_wal_cleaner(io: &IoHandle, config: &MaintainConfig) -> Result<Arc<QueueCleaner>, MaintainError> {
    let (store, normalized_path) = io
        .object_store_operator_registry()
        .resolve_object_store(&config.queue.common.base_path)?;
    tracing::info!(
        base_path = %config.queue.common.base_path,
        "wal cleanup addressing the queue"
    );
    Ok(Arc::new(QueueCleaner::new(normalized_path, store)))
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
