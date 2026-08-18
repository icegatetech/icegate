//! Background reclamation of orphan Iceberg files.
//!
//! The orphan collector lists each table's object-storage prefix and deletes
//! data and metadata files that the current table metadata no longer references
//! and that are older than a grace period. It mirrors [`crate::compact`]: one
//! jobmanager job per enabled table, run on a scan interval.
//!
//! The other deletion loop of the deployment, WAL segment cleanup, runs on its
//! own pool ([`crate::wal_cleanup::runner`]) — a sweep may hold a worker for up to
//! `gc.orphans.sweep_timeout_secs`, which would make the cleanup interval a
//! lower bound rather than a cadence.

/// GC configuration: grace period, table list, and scan interval.
pub mod config;
/// Pure orphan-classification logic: object-key parsing and sweep decisions.
pub mod decide;
/// Orphan-sweep instruments recorded to the OpenTelemetry global meter.
pub mod metrics;
/// Referenced-path set builder: the files a table currently references.
pub mod reachable;
/// The orphan-file sweep: list, diff against the referenced set, and delete.
pub mod sweep;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use iceberg::Catalog;
use icegate_common::{EVENTS_TABLE, LOGS_TABLE, METRICS_TABLE, OPERATIONS_TABLE, OperatorRegistry, SPANS_TABLE};
use jobmanager::{
    Error as JobError, JobsManager, JobsManagerHandle, OtelMetrics, TaskCode, TaskContext, TaskDefinition,
    TaskExecutor, TaskOutcome, TaskResult,
};
use tracing::{Instrument, info_span};

use crate::error::{MaintainError, Result};
use crate::gc::config::{GC_CONFIG_BLOCK, GcConfig, GcOrphansConfig};
use crate::gc::metrics::GcMetrics;
use crate::gc::sweep::run_sweep;

/// Task code for the single per-table GC task.
pub const GC_TASK_CODE: &str = "gc";

/// Static identity of one table's GC job.
#[derive(Debug, Clone, Copy)]
struct GcTableSpec {
    /// Stable job name used in the registry and logs (e.g. `gc_logs`).
    job_name: &'static str,
    /// Iceberg table name within the `icegate` namespace.
    table: &'static str,
}

/// Build the per-table specs for the tables GC is enabled for.
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

/// Resolve the jobs to register, naming the switch that left the set empty.
///
/// Two different settings produce no job — the master switch and the per-table
/// flags — and an operator can only act on the one that is actually theirs, so
/// the refusals are kept apart and each names its own config key.
///
/// # Errors
///
/// Returns [`MaintainError::Config`] when `gc.enabled` is off, and when it is on
/// with no table enabled.
fn resolve_gc_jobs(config: &GcConfig) -> Result<Vec<GcTableSpec>> {
    if !config.enabled {
        return Err(MaintainError::Config(
            "gc is disabled: set gc.enabled to run the orphan sweep".to_string(),
        ));
    }
    let specs = enabled_gc_tables(config);
    if specs.is_empty() {
        return Err(MaintainError::Config(
            "gc is enabled but no table is: enable at least one of \
             gc.{logs,spans,events,metrics,operations}_enabled"
                .to_string(),
        ));
    }
    Ok(specs)
}

/// Per-table GC task executor.
struct GcExecutor {
    catalog: Arc<dyn Catalog>,
    /// Shared by every table's executor: one operator per bucket for the whole
    /// process, however many tables sweep through it.
    operator_registry: Arc<OperatorRegistry>,
    table: &'static str,
    orphans: GcOrphansConfig,
    metrics: GcMetrics,
}

impl GcExecutor {
    async fn run(&self, ctx: &TaskContext) -> std::result::Result<TaskOutcome, JobError> {
        let span = info_span!("gc_sweep", table = self.table);
        self.run_sweep_task(ctx).instrument(span).await
    }

    async fn run_sweep_task(&self, ctx: &TaskContext) -> std::result::Result<TaskOutcome, JobError> {
        if !self.orphans.enabled {
            return Ok(TaskOutcome::empty());
        }
        let start = std::time::Instant::now();
        let now = chrono::Utc::now();
        let summary = run_sweep(
            &self.catalog,
            &self.operator_registry,
            self.table,
            &self.orphans,
            now,
            &self.metrics,
            ctx.cancel_token(),
        )
        .await
        .map_err(|e| JobError::Other(format!("gc sweep of table '{}' failed: {e}", self.table)))?;
        self.metrics.record_duration(self.table, start.elapsed().as_secs_f64());
        tracing::info!(
            table = self.table,
            scanned = summary.scanned,
            deleted = summary.deleted,
            bytes_reclaimed = summary.bytes_reclaimed,
            dry_run = self.orphans.dry_run,
            "gc sweep complete"
        );
        Ok(TaskOutcome::empty())
    }
}

#[async_trait]
impl TaskExecutor for GcExecutor {
    async fn execute(&self, ctx: TaskContext) -> TaskResult {
        Ok(self.run(&ctx).await?)
    }
}

/// Everything the runner registers jobs from, beside the catalog.
pub struct GcRunnerSpec {
    /// The process's operator registry: every table's sweep resolves its store
    /// through it, so one bucket costs one `OpenDAL` operator no matter how many
    /// tables are swept how often.
    pub operator_registry: Arc<OperatorRegistry>,
    /// Orphan-sweep configuration.
    pub config: GcConfig,
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
    /// Build a runner with one orphan-sweep job per enabled table, as
    /// [`GcRunnerSpec`] describes them.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if the config block is invalid, `gc.enabled` is
    /// off, no table is enabled, or the jobmanager storage cannot be
    /// constructed.
    pub async fn new(catalog: Arc<dyn Catalog>, spec: GcRunnerSpec) -> Result<Self> {
        Self::new_with_max_iterations(catalog, spec, None).await
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
        spec: GcRunnerSpec,
        max_iterations: Option<u64>,
    ) -> Result<Self> {
        let GcRunnerSpec {
            operator_registry,
            config,
        } = spec;
        config.validate()?;
        let specs = resolve_gc_jobs(&config)?;

        let interval = Duration::from_secs(config.jobsmanager.scan_interval_secs);
        let timeout = Duration::from_secs(config.orphans.sweep_timeout_secs);

        let metrics = GcMetrics::new();
        // Enable the jobmanager's own metrics (job/task durations and statuses,
        // task-steal events, optimistic-concurrency save retries, and job-state
        // storage S3 latency / cache hits): `GcMetrics` covers sweep outcomes but
        // not this job-execution machinery. Binds to the global meter installed by
        // `MetricsRuntime`; inert when metrics are disabled (no provider set).
        let mut builder = JobsManager::builder()
            .s3(config.jobsmanager.storage.to_s3_storage_config(GC_CONFIG_BLOCK)?)
            .workers(config.jobsmanager.worker_count)
            .poll_interval(Duration::from_millis(config.jobsmanager.poll_interval_ms))
            .metrics(Arc::new(OtelMetrics::new(&opentelemetry::global::meter(
                "icegate-maintain",
            ))));

        for spec in specs {
            let executor = Arc::new(GcExecutor {
                catalog: Arc::clone(&catalog),
                operator_registry: Arc::clone(&operator_registry),
                table: spec.table,
                orphans: config.orphans.clone(),
                metrics: metrics.clone(),
            });
            builder = builder.job(spec.job_name, move |job| {
                job.every(interval);
                if let Some(max) = max_iterations {
                    job.max_iterations(max);
                }
                job.add_task(TaskDefinition::new(TaskCode::new(GC_TASK_CODE), timeout), executor);
            });
        }

        let manager = builder.build().await.map_err(map_job_error)?;
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

#[cfg(test)]
mod tests {
    use super::{enabled_gc_tables, resolve_gc_jobs};
    use crate::gc::config::GcConfig;

    /// The master switch is honoured where the jobs are built, so a disabled
    /// block registers no sweep at all rather than five no-op tasks — and the
    /// refusal has to name `gc.enabled`, because that key is what the operator
    /// sets in the chart and in Compose, and a message pointing at the per-table
    /// flags sends them to a setting that is already on.
    #[test]
    fn a_disabled_block_is_refused_by_the_name_of_the_master_switch() {
        let error = resolve_gc_jobs(&GcConfig::default()).expect_err("a disabled block registers no sweep");

        let message = error.to_string();
        assert!(
            message.contains("gc.enabled"),
            "the master switch is not named: {message}"
        );
    }

    /// The other empty set, which the same message used to cover: the block is
    /// on and every table is off, so the per-table keys are the ones to name.
    #[test]
    fn an_enabled_block_without_tables_is_refused_by_the_per_table_keys() {
        let config = GcConfig {
            enabled: true,
            logs_enabled: false,
            spans_enabled: false,
            events_enabled: false,
            metrics_enabled: false,
            operations_enabled: false,
            ..GcConfig::default()
        };

        let error = resolve_gc_jobs(&config).expect_err("an empty table set registers no sweep");

        let message = error.to_string();
        assert!(
            message.contains("gc.{logs,spans,events,metrics,operations}_enabled"),
            "the per-table keys are not named: {message}"
        );
    }

    /// All five tables, and only the enabled ones.
    #[test]
    fn every_enabled_table_gets_a_sweep_job() {
        let config = GcConfig {
            enabled: true,
            events_enabled: false,
            ..GcConfig::default()
        };

        let tables: Vec<&str> = enabled_gc_tables(&config).iter().map(|spec| spec.table).collect();

        assert_eq!(tables, vec!["logs", "spans", "metrics", "operations"]);
    }
}
