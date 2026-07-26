//! Compactor service assembly.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Duration as ChronoDuration;
use iceberg::Catalog;
use icegate_common::iceberg_write::WriteConfig;
use icegate_common::merge::sort_key::SortColumnsDescriptor;
use icegate_common::parquet_encoding::{
    EVENTS_BLOOM_COLUMNS, EVENTS_COLUMN_ENCODINGS, LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS, METRICS_BLOOM_COLUMNS,
    METRICS_COLUMN_ENCODINGS, OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS, SPANS_BLOOM_COLUMNS,
    SPANS_COLUMN_ENCODINGS,
};
use icegate_common::parquet_writer::ColumnEncoding;
use icegate_common::{EVENTS_TABLE, LOGS_TABLE, METRICS_TABLE, OPERATIONS_TABLE, SPANS_TABLE};
use jobmanager::{
    CachedStorage, JobCode, JobDefinition, JobDefinitionRegistry, JobRegistry, JobsManager, JobsManagerConfig,
    JobsManagerHandle, Metrics as JobMetrics, S3Storage, TaskCode, TaskDefinition, TaskExecutorFn, WorkerConfig,
};

use crate::compact::config::CompactionConfig;
use crate::compact::data::planner::PlannerLimits;
use crate::compact::data::rewrite::CompactFilesExecutor;
use crate::compact::manifest::rewrite::ManifestCompactExecutor;
use crate::compact::metrics::CompactMetrics;
use crate::compact::tasks::{
    CompactFilesRunner, CompactManifestRunner, FILES_TASK_CODE, MANIFEST_TASK_CODE, PLAN_TASK_CODE, PlanTaskRunner,
    manifest_executor_fn, plan_executor_fn, rewrite_executor_fn,
};
use crate::error::{MaintainError, Result};

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
            row_group_size: config.data.row_group_size,
            data_page_size_limit_bytes: config.data.data_page_size_limit_bytes,
            max_file_size_bytes: config.data.target_file_size_bytes,
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
    if config.operations_enabled {
        specs.push(CompactJobSpec {
            job_name: "compact_operations",
            table: OPERATIONS_TABLE,
            descriptor: SortColumnsDescriptor::operations()?,
            bloom_filter_columns: OPERATIONS_BLOOM_COLUMNS,
            column_encodings: OPERATIONS_COLUMN_ENCODINGS,
        });
    }
    Ok(specs)
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
        // Validate every tunable (and the job-state storage) up front. Each field
        // is `#[serde(default)]`, so a malformed config loads with zeros that
        // would otherwise silently disable compaction (e.g. a zero
        // `max_group_input_bytes` drops every group, or a zero `max_merge_size_ratio`
        // relies on the planner's defensive clamp) instead of erroring here.
        config.validate()?;
        let specs = enabled_specs(config)?;
        if specs.is_empty() {
            return Err(MaintainError::Config(
                "no compaction tables enabled: at least one of logs/spans/events/metrics/operations must be enabled"
                    .to_string(),
            ));
        }

        let planner_limits = PlannerLimits {
            target_file_size_bytes: config.data.target_file_size_bytes,
            max_group_input_bytes: config.data.max_group_input_bytes,
            min_input_files: config.data.min_input_files,
            max_skippable_tail_files: config.data.max_skippable_tail_files,
            max_merge_size_ratio: config.data.max_merge_size_ratio,
        };
        // The REWRITE task deadline is its own knob, NOT the discovery period: a
        // rewrite that legitimately runs longer than one scan interval must not
        // be declared expired (which would let another worker duplicate it). The
        // iteration interval is the discovery cadence.
        let rewrite_timeout = rewrite_timeout(config)?;
        let manifest_rewrite_timeout = manifest_rewrite_timeout(config)?;
        let iteration_interval = scan_interval(config)?;

        let mut job_defs: Vec<JobDefinition> = Vec::with_capacity(specs.len());
        for spec in specs {
            // One metrics instance per job, cloned into every task runner so the
            // PLAN, REWRITE, and MANIFEST paths record to the same instruments
            // (the clone is a cheap `Arc`-handle copy).
            let metrics = CompactMetrics::new();
            let plan_runner = Arc::new(PlanTaskRunner {
                catalog: Arc::clone(&catalog),
                spec,
                planner_limits,
                compact_files_timeout: rewrite_timeout,
                compact_manifest_timeout: manifest_rewrite_timeout,
                metrics: metrics.clone(),
            });
            let manifest_runner = Arc::new(CompactManifestRunner::new(
                ManifestCompactExecutor::new(
                    Arc::clone(&catalog),
                    config.manifest.target_size_bytes,
                    config.manifest.candidate_size_ratio,
                    config.manifest.max_manifests_per_commit,
                    metrics.clone(),
                ),
                spec.table,
            ));
            let rewrite_runner = Arc::new(CompactFilesRunner::new(
                CompactFilesExecutor::new(
                    Arc::clone(&catalog),
                    spec.write_config(config),
                    spec.descriptor,
                    metrics,
                ),
                spec.table,
            ));

            let initial_plan = TaskDefinition::new(TaskCode::new(PLAN_TASK_CODE), Vec::new(), rewrite_timeout)
                .map_err(map_job_error)?;

            let mut executors: HashMap<TaskCode, TaskExecutorFn> = HashMap::new();
            executors.insert(TaskCode::new(PLAN_TASK_CODE), plan_executor_fn(plan_runner));
            executors.insert(TaskCode::new(FILES_TASK_CODE), rewrite_executor_fn(rewrite_runner));
            executors.insert(TaskCode::new(MANIFEST_TASK_CODE), manifest_executor_fn(manifest_runner));

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
                config.jobsmanager.storage.to_s3_storage_config()?,
                registry_dyn,
                metrics.clone(),
            )
            .await
            .map_err(map_job_error)?,
        );
        let cached_storage = Arc::new(CachedStorage::new(s3_storage, metrics.clone()));

        let manager_config = JobsManagerConfig {
            worker_count: config.jobsmanager.worker_count,
            worker_config: WorkerConfig {
                poll_interval: std::time::Duration::from_millis(config.jobsmanager.poll_interval_ms),
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

/// Convert the compaction `scan_interval_secs` into a positive [`ChronoDuration`]
/// for the jobmanager iteration (discovery) interval.
///
/// # Errors
///
/// Returns [`MaintainError::Config`] if the configured interval is zero or
/// overflows an `i64` number of seconds.
fn scan_interval(config: &CompactionConfig) -> Result<ChronoDuration> {
    positive_duration(
        config.jobsmanager.scan_interval_secs,
        "compaction.jobsmanager.scan_interval_secs",
    )
}

/// Convert the compaction `rewrite_timeout_secs` into a positive
/// [`ChronoDuration`] for the per-REWRITE (and initial PLAN) task deadline.
///
/// # Errors
///
/// Returns [`MaintainError::Config`] if the configured timeout is zero or
/// overflows an `i64` number of seconds.
fn rewrite_timeout(config: &CompactionConfig) -> Result<ChronoDuration> {
    positive_duration(config.data.rewrite_timeout_secs, "compaction.data.rewrite_timeout_secs")
}

/// Convert the compaction `manifest_rewrite_timeout_secs` into a positive
/// [`ChronoDuration`] for the `compact_manifest` task deadline.
///
/// # Errors
///
/// Returns [`MaintainError::Config`] if the configured timeout is zero or
/// overflows an `i64` number of seconds.
fn manifest_rewrite_timeout(config: &CompactionConfig) -> Result<ChronoDuration> {
    positive_duration(
        config.manifest.rewrite_timeout_secs,
        "compaction.manifest.rewrite_timeout_secs",
    )
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
