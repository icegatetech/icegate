//! Parquet compaction for IceGate Iceberg tables.
//!
//! This module assembles the compaction feature into a long-running
//! [`Compactor`] background service, mirroring ingest's `Shifter` wiring.
//!
//! There is one jobmanager job per enabled table, each a `PLAN -> REWRITE`
//! pipeline.
//!
//! * The **PLAN** task ([`COMPACT_PLAN_TASK_CODE`]) loads the table fresh,
//!   enumerates its data files with sort-key bounds, bin-packs them into rewrite
//!   groups via [`planner::plan_rewrite_groups`], and dynamically fans out one
//!   **REWRITE** task per group. It schedules NO commit task — every REWRITE
//!   commits its own atomic replace.
//! * Each **REWRITE** task ([`COMPACT_REWRITE_TASK_CODE`]) delegates to a
//!   per-table [`rewrite::RewriteExecutor`], which k-way-merges the group's
//!   inputs into target-sized Parquet and atomically swaps them for the outputs
//!   via the generic [`iceberg::transaction::Transaction::rewrite_files`] action.
//!
//! ## Generic catalog
//!
//! The replace commit goes through [`iceberg::transaction::Transaction::commit`],
//! which is generic over `&dyn iceberg::Catalog` and performs the
//! optimistic-concurrency retry internally, so [`Compactor`] holds an
//! `Arc<dyn Catalog>` constructed by the caller via
//! [`icegate_common::CatalogBuilder::from_config`] (the same way `migrate` builds
//! its catalog).

/// Configuration for the compaction process.
pub mod config;
/// Iceberg data-file `MergeSource` adapter for the k-way merger.
pub mod iceberg_source;
/// Compaction metrics recorded to the OpenTelemetry global meter.
pub mod metrics;
/// Compaction planner: bin-packs data files into rewrite groups.
pub mod planner;
/// Rewrite task executor: merges a rewrite group and replaces its inputs.
pub mod rewrite;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Duration as ChronoDuration;
use iceberg::Catalog;
use icegate_common::iceberg_write::WriteConfig;
use icegate_common::manifest_scan::list_data_files_with_stats;
use icegate_common::merge::sort_key::SortColumnsDescriptor;
use icegate_common::parquet_encoding::{
    EVENTS_BLOOM_COLUMNS, EVENTS_COLUMN_ENCODINGS, LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS, METRICS_BLOOM_COLUMNS,
    METRICS_COLUMN_ENCODINGS, SPANS_BLOOM_COLUMNS, SPANS_COLUMN_ENCODINGS,
};
use icegate_common::parquet_writer::ColumnEncoding;
use icegate_common::{EVENTS_TABLE, LOGS_TABLE, METRICS_TABLE, SPANS_TABLE, icegate_table_ident};
use icegate_jobmanager::registry::TaskExecutorFn;
use icegate_jobmanager::{
    CachedStorage, Error as JobError, ImmutableTask, JobCode, JobDefinition, JobDefinitionRegistry, JobManager,
    JobRegistry, JobsManager, JobsManagerConfig, JobsManagerHandle, Metrics as JobMetrics, S3Storage, TaskCode,
    TaskDefinition, WorkerConfig,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span};

use crate::compact::config::CompactionConfig;
use crate::compact::metrics::CompactMetrics;
use crate::compact::planner::{PlannerLimits, plan_rewrite_groups};
use crate::compact::rewrite::{RewriteExecutor, RewriteInput, RewriteOutcome};
use crate::error::{MaintainError, Result};

/// Task code for the compaction PLAN task (enumerate + fan-out REWRITEs).
pub const COMPACT_PLAN_TASK_CODE: &str = "compact_plan";
/// Task code for the compaction REWRITE task (merge one group + atomic replace).
pub const COMPACT_REWRITE_TASK_CODE: &str = "compact_rewrite";

/// Specification for one per-table compaction job.
///
/// All fields are `'static` so the spec is `Copy` and can be captured by value
/// into the per-table executors without lifetime juggling.
#[derive(Clone, Copy)]
pub struct CompactJobSpec {
    /// Stable job name used in the job registry and logs (e.g. `compact_logs`).
    pub job_name: &'static str,
    /// Iceberg table name within the `icegate` namespace (e.g. `logs`).
    pub table: &'static str,
    /// Sort descriptor for the table, used both to enumerate data-file bounds
    /// and to order rows during the merge.
    pub descriptor: &'static SortColumnsDescriptor,
    /// Columns that should get a Parquet bloom filter on the rewritten files.
    pub bloom_filter_columns: &'static [&'static str],
    /// Per-column Parquet encoding overrides for the rewritten files.
    pub column_encodings: &'static [ColumnEncoding],
}

impl CompactJobSpec {
    /// Build the [`WriteConfig`] for this table's rewrite output from the
    /// compaction config's shared Parquet tunables and the spec's encoding
    /// policy.
    const fn write_config(&self, config: &CompactionConfig) -> WriteConfig {
        WriteConfig {
            row_group_size: config.row_group_size,
            data_page_size_limit_bytes: config.data_page_size_limit_bytes,
            max_file_size_bytes: config.target_file_size_bytes,
            bloom_filter_columns: self.bloom_filter_columns,
            column_encodings: self.column_encodings,
        }
    }
}

/// Build the per-table specs for the tables enabled in `config`.
///
/// Each spec's [`SortColumnsDescriptor`] is resolved once from the static schema;
/// only tables whose per-table enable flag is set are included.
///
/// # Errors
///
/// Returns [`MaintainError::Schema`] if a table's sort descriptor cannot be
/// resolved from the schema.
fn enabled_specs(config: &CompactionConfig) -> Result<Vec<CompactJobSpec>> {
    let mut specs = Vec::new();
    if config.logs_enabled {
        specs.push(CompactJobSpec {
            job_name: "compact_logs",
            table: LOGS_TABLE,
            descriptor: SortColumnsDescriptor::logs()?,
            bloom_filter_columns: LOGS_BLOOM_COLUMNS,
            column_encodings: LOGS_COLUMN_ENCODINGS,
        });
    }
    if config.spans_enabled {
        specs.push(CompactJobSpec {
            job_name: "compact_spans",
            table: SPANS_TABLE,
            descriptor: SortColumnsDescriptor::spans()?,
            bloom_filter_columns: SPANS_BLOOM_COLUMNS,
            column_encodings: SPANS_COLUMN_ENCODINGS,
        });
    }
    if config.events_enabled {
        specs.push(CompactJobSpec {
            job_name: "compact_events",
            table: EVENTS_TABLE,
            descriptor: SortColumnsDescriptor::events()?,
            bloom_filter_columns: EVENTS_BLOOM_COLUMNS,
            column_encodings: EVENTS_COLUMN_ENCODINGS,
        });
    }
    if config.metrics_enabled {
        specs.push(CompactJobSpec {
            job_name: "compact_metrics",
            table: METRICS_TABLE,
            descriptor: SortColumnsDescriptor::metrics()?,
            bloom_filter_columns: METRICS_BLOOM_COLUMNS,
            column_encodings: METRICS_COLUMN_ENCODINGS,
        });
    }
    Ok(specs)
}

/// Per-table PLAN executor: enumerate the table's data files, bin-pack them into
/// rewrite groups, and fan out one REWRITE task per group.
///
/// Holds everything one job's PLAN task needs by value/`Arc` so the executor
/// closure is `'static`.
struct PlanExecutor {
    catalog: Arc<dyn Catalog>,
    spec: CompactJobSpec,
    planner_limits: PlannerLimits,
    rewrite_timeout: ChronoDuration,
    /// Compaction instruments shared with this job's REWRITE executor; the PLAN
    /// path records the partition compacted/skipped and group fan-out counts.
    metrics: CompactMetrics,
}

impl PlanExecutor {
    /// Run one PLAN task: load the table fresh, plan rewrite groups against its
    /// current `main` snapshot, submit a REWRITE task per group, and complete
    /// the PLAN task.
    ///
    /// If the table has no current snapshot (freshly created, never committed
    /// to) or the planner finds no work, the task completes with no REWRITE
    /// tasks submitted.
    async fn run(&self, task: &dyn ImmutableTask, manager: &dyn JobManager) -> std::result::Result<(), JobError> {
        let span = info_span!("compact_plan", table = self.spec.table);
        self.run_plan(task, manager).instrument(span).await
    }

    /// The instrumented body of [`Self::run`], split out so the `compact_plan`
    /// span wraps the whole async future (an [`tracing::Span::entered`] guard
    /// cannot be held across the `.await` points without making the future
    /// `!Send`).
    async fn run_plan(&self, task: &dyn ImmutableTask, manager: &dyn JobManager) -> std::result::Result<(), JobError> {
        let table_ident = icegate_table_ident(self.spec.table);
        let table = self
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| JobError::TaskExecution(format!("failed to load table '{}': {e}", self.spec.table)))?;

        // Without a current snapshot there is nothing to compact. The rewrite
        // transaction guards its own concurrency, so the planner no longer needs
        // to capture a base snapshot id for the REWRITE tasks.
        if table.metadata().current_snapshot_id().is_none() {
            info!(
                "compact plan: table '{}' has no snapshot, nothing to do",
                self.spec.table
            );
            return manager.complete_task(task.id(), Vec::new());
        }

        let stats = list_data_files_with_stats(&table, self.spec.descriptor)
            .await
            .map_err(|e| JobError::TaskExecution(format!("failed to enumerate data files: {e}")))?;

        let outcome = plan_rewrite_groups(stats, &self.planner_limits);
        let group_count = outcome.groups.len();
        // PLAN telemetry: groups fanned out, plus the partition compacted/skipped
        // split. `usize -> u64` is lossless on every supported (<= 64-bit) target.
        self.metrics.record_plan(
            self.spec.table,
            group_count as u64,
            outcome.partitions_compacted as u64,
            outcome.partitions_skipped as u64,
        );

        for group in outcome.groups {
            // Every file in a planner group shares a partition key; an empty
            // group never occurs (`plan_rewrite_groups` only emits non-empty
            // groups), but guard defensively rather than index blindly.
            let Some(first) = group.first() else { continue };
            let partition_key = first.partition_key().to_string();
            // Input paths in sort-key (`min_key`) order: that is the position
            // contract the REWRITE executor relies on. The planner preserves
            // cluster order, but re-sorting here keeps the contract explicit and
            // independent of upstream ordering guarantees.
            let mut ordered = group;
            ordered.sort_by(|left, right| left.min_key().compare(right.min_key()));
            let input_file_paths: Vec<String> =
                ordered.iter().map(|stats| stats.data_file.file_path().to_string()).collect();

            let rewrite_input = RewriteInput {
                table: self.spec.table.to_string(),
                partition_key,
                input_file_paths,
            };
            let payload = serde_json::to_vec(&rewrite_input)
                .map_err(|e| JobError::TaskExecution(format!("failed to serialize rewrite input: {e}")))?;
            let rewrite_task =
                TaskDefinition::new(TaskCode::new(COMPACT_REWRITE_TASK_CODE), payload, self.rewrite_timeout)?;
            manager.add_task(rewrite_task)?;
        }

        info!(
            "compact plan: table '{}' scheduled {} rewrite task(s)",
            self.spec.table, group_count
        );
        manager.complete_task(task.id(), Vec::new())
    }
}

/// Per-table REWRITE executor: deserialize one rewrite group and delegate to the
/// table's [`RewriteExecutor`].
struct RewriteTaskExecutor {
    executor: RewriteExecutor,
    table: &'static str,
}

impl RewriteTaskExecutor {
    /// Run one REWRITE task: parse its [`RewriteInput`], execute the merge +
    /// atomic replace, and complete the task. A clean abort (a sibling compactor
    /// already took an input) is a successful completion, not a failure: the
    /// next PLAN scan re-derives a group from whatever files remain.
    async fn run(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), JobError> {
        // The `(tenant, day)` partition is only known after parsing the input, so
        // declare the field empty and fill it in below via `Span::record`.
        let span = info_span!("compact_rewrite", table = self.table, partition = tracing::field::Empty);
        self.run_rewrite(task, manager, cancel).instrument(span).await
    }

    /// The instrumented body of [`Self::run`], split out so the `compact_rewrite`
    /// span wraps the whole async future (the span guard cannot cross the
    /// `.await` points without making the future `!Send`).
    async fn run_rewrite(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), JobError> {
        let input: RewriteInput = serde_json::from_slice(task.get_input())
            .map_err(|e| JobError::TaskExecution(format!("failed to parse rewrite input: {e}")))?;
        // Tag the active `compact_rewrite` span with the partition now that it is
        // known, matching the span's `(table, partition)` attribute contract.
        tracing::Span::current().record("partition", input.partition_key.as_str());

        let outcome = self
            .executor
            .execute(&input, cancel)
            .await
            .map_err(|e| JobError::TaskExecution(format!("rewrite of table '{}' failed: {e}", self.table)))?;

        match outcome {
            RewriteOutcome::Committed {
                input_files,
                output_files,
                rows,
                ..
            } => {
                info!(
                    "compact rewrite: table '{}' replaced {} file(s) with {} file(s) holding {} row(s)",
                    self.table, input_files, output_files, rows
                );
            }
            RewriteOutcome::Aborted => {
                info!(
                    "compact rewrite: table '{}' aborted (an input was taken by a sibling compactor)",
                    self.table
                );
            }
        }

        manager.complete_task(task.id(), Vec::new())
    }
}

/// Runs compaction jobs inside the maintain process.
pub struct Compactor {
    manager: JobsManager,
}

/// Handle for stopping a running [`Compactor`].
pub struct CompactorHandle {
    handle: JobsManagerHandle,
}

impl Compactor {
    /// Create a new compactor over the given generic Iceberg catalog.
    ///
    /// Builds one jobmanager job per enabled table (a `PLAN -> REWRITE`
    /// pipeline), wires the shared S3-backed job-state storage, and returns a
    /// ready-to-[`start`](Self::start) compactor.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if no table is enabled, a table's sort
    /// descriptor cannot be resolved, the job-state storage configuration is
    /// invalid, or the jobmanager cannot be constructed.
    pub async fn new(catalog: Arc<dyn Catalog>, config: &CompactionConfig) -> Result<Self> {
        Self::new_with_max_iterations(catalog, config, None).await
    }

    /// Create a compactor that stops after `max_iterations` discovery cycles per
    /// job.
    ///
    /// This is the deterministic entry point used by the end-to-end test to run
    /// exactly one compaction cycle: with `max_iterations = Some(1)` each job
    /// runs its initial PLAN iteration (fanning out and draining its REWRITE
    /// tasks) and never starts a second iteration. `None` runs the job
    /// indefinitely on the configured interval (the production behavior of
    /// [`Self::new`]).
    ///
    /// # Errors
    ///
    /// Same as [`Self::new`].
    ///
    /// This is a test seam: production callers use [`Self::new`]. It stays `pub`
    /// only so the end-to-end integration test (a separate crate) can drive a
    /// single deterministic cycle, and is `#[doc(hidden)]` so it does not appear
    /// in the public API docs.
    #[doc(hidden)]
    pub async fn new_with_max_iterations(
        catalog: Arc<dyn Catalog>,
        config: &CompactionConfig,
        max_iterations: Option<u64>,
    ) -> Result<Self> {
        let specs = enabled_specs(config)?;
        if specs.is_empty() {
            return Err(MaintainError::Config(
                "no compaction tables enabled: at least one of logs/spans/events/metrics must be enabled".to_string(),
            ));
        }
        config.jobs_storage.validate()?;

        let planner_limits = PlannerLimits {
            target_file_size_bytes: config.target_file_size_bytes,
            max_group_input_bytes: config.max_group_input_bytes,
            min_input_files: config.min_input_files,
            max_skippable_tail_files: config.max_skippable_tail_files,
        };
        // The REWRITE task deadline is its own knob, NOT the discovery period: a
        // rewrite that legitimately runs longer than one scan interval must not
        // be declared expired (which would let another worker duplicate it). The
        // iteration interval is the discovery cadence.
        let rewrite_timeout = rewrite_timeout(config)?;
        let iteration_interval = scan_interval(config)?;

        let mut job_defs: Vec<JobDefinition> = Vec::with_capacity(specs.len());
        for spec in specs {
            // One metrics instance per job, cloned into both executors so the
            // PLAN and REWRITE paths record to the same instruments (the clone is
            // a cheap `Arc`-handle copy).
            let metrics = CompactMetrics::new();
            let plan_executor = Arc::new(PlanExecutor {
                catalog: Arc::clone(&catalog),
                spec,
                planner_limits,
                rewrite_timeout,
                metrics: metrics.clone(),
            });
            let rewrite_executor = Arc::new(RewriteTaskExecutor {
                executor: RewriteExecutor::new(
                    Arc::clone(&catalog),
                    spec.write_config(config),
                    spec.descriptor,
                    metrics,
                ),
                table: spec.table,
            });

            let initial_plan = TaskDefinition::new(TaskCode::new(COMPACT_PLAN_TASK_CODE), Vec::new(), rewrite_timeout)
                .map_err(map_job_error)?;

            let mut executors: HashMap<TaskCode, TaskExecutorFn> = HashMap::new();
            executors.insert(TaskCode::new(COMPACT_PLAN_TASK_CODE), plan_executor_fn(plan_executor));
            executors.insert(
                TaskCode::new(COMPACT_REWRITE_TASK_CODE),
                rewrite_executor_fn(rewrite_executor),
            );

            let mut job_def = JobDefinition::new(JobCode::new(spec.job_name), vec![initial_plan], executors)
                .map_err(map_job_error)?
                .with_iteration_interval(iteration_interval)
                .map_err(map_job_error)?;
            if let Some(max) = max_iterations {
                job_def = job_def.with_max_iterations(max).map_err(map_job_error)?;
            }
            job_defs.push(job_def);
        }

        let job_registry = Arc::new(JobRegistry::new(job_defs).map_err(map_job_error)?);
        let metrics = JobMetrics::new_disabled();

        // `S3Storage::new` takes the registry as a trait object; coerce the
        // concrete `Arc<JobRegistry>` clone up front so the call site is explicit.
        let registry_dyn: Arc<dyn JobDefinitionRegistry> = job_registry.clone();
        let s3_storage = Arc::new(
            S3Storage::new(
                config.jobs_storage.to_s3_storage_config()?,
                registry_dyn,
                metrics.clone(),
            )
            .await
            .map_err(map_job_error)?,
        );
        let cached_storage = Arc::new(CachedStorage::new(s3_storage, metrics.clone()));

        let manager_config = JobsManagerConfig {
            worker_count: config.worker_count,
            worker_config: WorkerConfig {
                poll_interval: std::time::Duration::from_millis(config.poll_interval_ms),
                ..Default::default()
            },
        };

        let manager = JobsManager::new(cached_storage, manager_config, job_registry, metrics).map_err(map_job_error)?;

        Ok(Self { manager })
    }

    /// Start the compactor workers and return a handle for shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if the jobmanager workers cannot be started.
    pub fn start(&self) -> Result<CompactorHandle> {
        let handle = self.manager.start().map_err(map_job_error)?;
        Ok(CompactorHandle { handle })
    }
}

impl CompactorHandle {
    /// Stop the compactor workers and wait for them to finish.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if a worker stopped with an error.
    pub async fn shutdown(self) -> Result<()> {
        self.handle.shutdown().await.map_err(map_job_error)
    }
}

/// Build the [`TaskExecutorFn`] for a job's PLAN task from a [`PlanExecutor`].
///
/// The jobmanager calls executors with a borrowed `&dyn JobManager` it owns, so
/// the closure clones the `Arc<PlanExecutor>` and reads the task input via the
/// [`ImmutableTask`] trait, exactly like ingest's shift executor.
fn plan_executor_fn(plan_executor: Arc<PlanExecutor>) -> TaskExecutorFn {
    Arc::new(move |task, manager, _cancel| {
        let plan_executor = Arc::clone(&plan_executor);
        Box::pin(async move { plan_executor.run(task.as_ref(), manager).await })
    })
}

/// Build the [`TaskExecutorFn`] for a job's REWRITE task from a
/// [`RewriteTaskExecutor`]. The cancellation token is forwarded so an in-flight
/// merge stops promptly on shutdown.
fn rewrite_executor_fn(rewrite_executor: Arc<RewriteTaskExecutor>) -> TaskExecutorFn {
    Arc::new(move |task, manager, cancel| {
        let rewrite_executor = Arc::clone(&rewrite_executor);
        Box::pin(async move { rewrite_executor.run(task.as_ref(), manager, &cancel).await })
    })
}

/// Convert the compaction `scan_interval_secs` into a positive [`ChronoDuration`]
/// for the jobmanager iteration (discovery) interval.
///
/// # Errors
///
/// Returns [`MaintainError::Config`] if the configured interval is zero or
/// overflows an `i64` number of seconds.
fn scan_interval(config: &CompactionConfig) -> Result<ChronoDuration> {
    positive_duration(config.scan_interval_secs, "compaction.scan_interval_secs")
}

/// Convert the compaction `rewrite_timeout_secs` into a positive
/// [`ChronoDuration`] for the per-REWRITE (and initial PLAN) task deadline.
///
/// # Errors
///
/// Returns [`MaintainError::Config`] if the configured timeout is zero or
/// overflows an `i64` number of seconds.
fn rewrite_timeout(config: &CompactionConfig) -> Result<ChronoDuration> {
    positive_duration(config.rewrite_timeout_secs, "compaction.rewrite_timeout_secs")
}

/// Convert a positive seconds count into a [`ChronoDuration`], rejecting zero
/// and `i64` overflow. `field` names the config key for the error message.
///
/// # Errors
///
/// Returns [`MaintainError::Config`] if `secs` is zero or exceeds `i64`.
fn positive_duration(secs: u64, field: &str) -> Result<ChronoDuration> {
    if secs == 0 {
        return Err(MaintainError::Config(format!("{field} must be greater than zero")));
    }
    let secs = i64::try_from(secs).map_err(|_| MaintainError::Config(format!("{field} exceeds i64")))?;
    Ok(ChronoDuration::seconds(secs))
}

/// Map a jobmanager (or other [`Display`](std::fmt::Display)) error into a
/// [`MaintainError`]. Generic over the error type so it works as a
/// `map_err` argument for the various jobmanager constructors that each return
/// their own error wrapper.
fn map_job_error<E: std::fmt::Display>(error: E) -> MaintainError {
    MaintainError::Config(error.to_string())
}
