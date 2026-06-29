//! Background orphan-file garbage collection for Iceberg tables.
//!
//! The garbage collector lists each table's object-storage prefix and deletes
//! data and metadata files that the current table metadata no longer references
//! and that are older than a grace period. It mirrors [`crate::compact`]: one
//! jobmanager job per enabled table, run on a scan interval.

/// GC configuration: grace period, table list, and scan interval.
pub mod config;
/// Pure orphan-classification logic: canonical keys and sweep decisions.
pub mod decide;
/// Orphan-sweep instruments recorded to the OpenTelemetry global meter.
pub mod metrics;
/// Referenced-path set builder: the files a table currently references.
pub mod reachable;
/// The orphan-file sweep: list, diff against the referenced set, and delete.
pub mod sweep;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Duration as ChronoDuration;
use iceberg::Catalog;
use icegate_common::{EVENTS_TABLE, LOGS_TABLE, METRICS_TABLE, OPERATIONS_TABLE, SPANS_TABLE, StorageConfig};
use icegate_jobmanager::registry::TaskExecutorFn;
use icegate_jobmanager::{
    CachedStorage, Error as JobError, ImmutableTask, JobCode, JobDefinition, JobDefinitionRegistry, JobManager,
    JobRegistry, JobsManager, JobsManagerConfig, JobsManagerHandle, Metrics as JobMetrics, S3Storage, TaskCode,
    TaskDefinition, WorkerConfig,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use crate::error::{MaintainError, Result};
use crate::gc::config::{GcConfig, GcOrphansConfig};
use crate::gc::metrics::GcMetrics;
use crate::gc::sweep::run_sweep;

/// Task code for the single per-table GC task.
pub const GC_TASK_CODE: &str = "gc";

/// Static identity of one table's GC job.
#[derive(Clone, Copy)]
struct GcTableSpec {
    /// Stable job name used in the registry and logs (e.g. `gc_logs`).
    job_name: &'static str,
    /// Iceberg table name within the `icegate` namespace.
    table: &'static str,
}

/// Build the per-table specs for the enabled tables.
fn enabled_gc_tables(config: &GcConfig) -> Vec<GcTableSpec> {
    let mut specs = Vec::new();
    if config.logs_enabled {
        specs.push(GcTableSpec {
            job_name: "gc_logs",
            table: LOGS_TABLE,
        });
    }
    if config.spans_enabled {
        specs.push(GcTableSpec {
            job_name: "gc_spans",
            table: SPANS_TABLE,
        });
    }
    if config.events_enabled {
        specs.push(GcTableSpec {
            job_name: "gc_events",
            table: EVENTS_TABLE,
        });
    }
    if config.metrics_enabled {
        specs.push(GcTableSpec {
            job_name: "gc_metrics",
            table: METRICS_TABLE,
        });
    }
    if config.operations_enabled {
        specs.push(GcTableSpec {
            job_name: "gc_operations",
            table: OPERATIONS_TABLE,
        });
    }
    specs
}

/// Per-table GC task executor.
struct GcExecutor {
    catalog: Arc<dyn Catalog>,
    storage: StorageConfig,
    table: &'static str,
    orphans: GcOrphansConfig,
    metrics: GcMetrics,
}

impl GcExecutor {
    async fn run(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), JobError> {
        let span = info_span!("gc_sweep", table = self.table);
        self.run_sweep_task(task, manager, cancel).instrument(span).await
    }

    async fn run_sweep_task(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), JobError> {
        if !self.orphans.enabled {
            return manager.complete_task(task.id(), Vec::new());
        }
        let start = std::time::Instant::now();
        let now = chrono::Utc::now();
        let summary = run_sweep(
            &self.catalog,
            &self.storage,
            self.table,
            &self.orphans,
            now,
            &self.metrics,
            cancel,
        )
        .await
        .map_err(|e| JobError::TaskExecution(format!("gc sweep of table '{}' failed: {e}", self.table)))?;
        self.metrics.record_duration(self.table, start.elapsed().as_secs_f64());
        tracing::info!(
            table = self.table,
            scanned = summary.scanned,
            deleted = summary.deleted,
            bytes_reclaimed = summary.bytes_reclaimed,
            dry_run = self.orphans.dry_run,
            "gc sweep complete"
        );
        manager.complete_task(task.id(), Vec::new())
    }
}

/// Wrap a [`GcExecutor`] as a jobmanager [`TaskExecutorFn`].
fn gc_executor_fn(executor: Arc<GcExecutor>) -> TaskExecutorFn {
    Arc::new(move |task, manager, cancel| {
        let executor = Arc::clone(&executor);
        Box::pin(async move { executor.run(task.as_ref(), manager, &cancel).await })
    })
}

/// Runs orphan-file garbage collection inside the maintain process.
pub struct GcRunner {
    manager: JobsManager,
}

/// Handle for draining a running [`GcRunner`].
pub struct GcRunnerHandle {
    handle: JobsManagerHandle,
}

impl GcRunner {
    /// Build a runner with one GC job per enabled table.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if the config is invalid, no tables are enabled,
    /// or the jobmanager storage cannot be constructed.
    pub async fn new(catalog: Arc<dyn Catalog>, storage: &StorageConfig, config: &GcConfig) -> Result<Self> {
        Self::new_with_max_iterations(catalog, storage, config, None).await
    }

    /// Test seam: like [`Self::new`] but caps each job at `max_iterations`
    /// discovery cycles.
    ///
    /// # Errors
    ///
    /// See [`Self::new`].
    #[doc(hidden)]
    pub async fn new_with_max_iterations(
        catalog: Arc<dyn Catalog>,
        storage: &StorageConfig,
        config: &GcConfig,
        max_iterations: Option<u64>,
    ) -> Result<Self> {
        config.validate()?;
        let specs = enabled_gc_tables(config);
        if specs.is_empty() {
            return Err(MaintainError::Config(
                "no gc tables enabled: at least one of logs/spans/events/metrics/operations must be enabled"
                    .to_string(),
            ));
        }

        let interval_secs = i64::try_from(config.jobsmanager.scan_interval_secs)
            .map_err(|_| MaintainError::Config("gc.jobsmanager.scan_interval_secs is too large".to_string()))?;
        let interval = ChronoDuration::seconds(interval_secs);
        let timeout_secs = i64::try_from(config.orphans.sweep_timeout_secs)
            .map_err(|_| MaintainError::Config("gc.orphans.sweep_timeout_secs is too large".to_string()))?;
        let timeout = ChronoDuration::seconds(timeout_secs);

        let metrics = GcMetrics::new();
        let mut job_defs: Vec<JobDefinition> = Vec::with_capacity(specs.len());
        for spec in specs {
            let executor = Arc::new(GcExecutor {
                catalog: Arc::clone(&catalog),
                storage: storage.clone(),
                table: spec.table,
                orphans: config.orphans.clone(),
                metrics: metrics.clone(),
            });
            let initial =
                TaskDefinition::new(TaskCode::new(GC_TASK_CODE), Vec::new(), timeout).map_err(map_job_error)?;
            let mut executors: HashMap<TaskCode, TaskExecutorFn> = HashMap::new();
            executors.insert(TaskCode::new(GC_TASK_CODE), gc_executor_fn(executor));

            let mut job_def = JobDefinition::new(JobCode::new(spec.job_name), vec![initial], executors)
                .map_err(map_job_error)?
                .with_iteration_interval(interval)
                .map_err(map_job_error)?;
            if let Some(max) = max_iterations {
                job_def = job_def.with_max_iterations(max).map_err(map_job_error)?;
            }
            job_defs.push(job_def);
        }

        let job_registry = Arc::new(JobRegistry::new(job_defs).map_err(map_job_error)?);
        let job_metrics = JobMetrics::new_disabled();
        let registry_dyn: Arc<dyn JobDefinitionRegistry> = job_registry.clone();
        let s3_storage = Arc::new(
            S3Storage::new(
                config.jobsmanager.storage.to_s3_storage_config()?,
                registry_dyn,
                job_metrics.clone(),
            )
            .await
            .map_err(map_job_error)?,
        );
        let cached_storage = Arc::new(CachedStorage::new(s3_storage, job_metrics.clone()));

        let manager_config = JobsManagerConfig {
            worker_count: config.jobsmanager.worker_count,
            worker_config: WorkerConfig {
                poll_interval: std::time::Duration::from_millis(config.jobsmanager.poll_interval_ms),
                ..Default::default()
            },
        };
        let manager =
            JobsManager::new(cached_storage, manager_config, job_registry, job_metrics).map_err(map_job_error)?;
        Ok(Self { manager })
    }

    /// Start the GC workers. Returns a handle that drains them on shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if the jobmanager fails to start.
    pub fn start(&self) -> Result<GcRunnerHandle> {
        let handle = self.manager.start().map_err(map_job_error)?;
        Ok(GcRunnerHandle { handle })
    }
}

impl GcRunnerHandle {
    /// Cancel the workers and wait for them to drain.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if a worker stopped with an error.
    pub async fn shutdown(self) -> Result<()> {
        self.handle.shutdown().await.map_err(map_job_error)
    }
}

/// Map any [`Display`](std::fmt::Display) error (jobmanager constructors,
/// duration overflow) into [`MaintainError::Config`].
fn map_job_error<E: std::fmt::Display>(error: E) -> MaintainError {
    MaintainError::Config(error.to_string())
}
