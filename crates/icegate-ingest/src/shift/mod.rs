//! Shift operations: moving data from WAL to Iceberg.

/// Commit task runner for shift operations.
pub mod commit_runner;
/// Configuration for shift operations.
pub mod config;
/// Task executors for shift operations.
pub mod executor;
/// Iceberg writer utilities for shift operations.
pub mod iceberg_storage;
/// Metrics and instrumentation helpers for shift operations.
pub mod instrumentation;
/// Parquet metadata reader utilities for shift operations.
pub mod parquet_meta_reader;
#[cfg(test)]
mod pipeline_it_tests;
#[cfg(test)]
mod pipeline_tests;
/// Plan task runner for shift operations.
pub mod plan_runner;
mod planner;
mod planner_partitioning;
mod row_groups_merger;
/// Shift task runner for shift operations.
pub mod shift_runner;
#[cfg(test)]
mod test_utils;
/// Task timeout estimation utilities.
mod timeout;

use std::{sync::Arc, time::Duration};

use commit_runner::CommitTaskRunnerImpl;
pub(crate) use config::ShiftConfig;
pub(crate) use executor::{
    COMMIT_TASK_CODE, CommitExecutor, CommitInput, PLAN_TASK_CODE, PlanExecutor, PlannedRowGroup, SHIFT_TASK_CODE,
    SegmentToRead, ShiftExecutor, ShiftInput, ShiftOutput,
};
use iceberg::Catalog;
pub(crate) use iceberg_storage::IcebergStorage;
use iceberg_storage::writer_max_parquet_bytes;
use icegate_common::parquet_writer::ColumnEncoding;
use icegate_queue::ParquetQueueReader;
use instrumentation::{
    CommitTaskRunnerWithMetrics, PlanTaskRunnerWithMetrics, QueueReaderWithMetrics, RowGroupsMergerObserverWithMetrics,
    ShiftTaskRunnerWithMetrics, StorageWithMetrics,
};
use jobmanager::{JobsManager, JobsManagerHandle, MetricsSink, S3StorageConfig, TaskDefinition};
use plan_runner::PlanTaskRunnerImpl;
pub use planner_partitioning::{CURRENT_PLANNER_PARTITION_SPEC, PlannerPartitionSpec};
use shift_runner::ShiftTaskRunnerImpl;
use timeout::TimeoutEstimator;

use crate::{
    error::{IngestError, Result},
    infra::metrics::ShiftMetrics,
    wal::SortColumnsDescriptor,
};

/// Specification for one per-signal shift job.
#[derive(Clone, Copy)]
pub struct ShiftJobSpec {
    /// Stable job name used in job registry and logs (e.g. `shift_logs`).
    pub job_name: &'static str,
    /// WAL topic to read from (e.g. `LOGS_TOPIC`).
    pub topic: &'static str,
    /// Iceberg table name to commit to (e.g. `LOGS_TABLE`).
    pub table: &'static str,
    /// Sort descriptor shared with the WAL sorter for this topic.
    pub descriptor: &'static SortColumnsDescriptor,
    /// Explicit limited partition spec used by the shift planner adapter.
    pub planner_partition_spec: &'static PlannerPartitionSpec,
    /// Columns that should get a Parquet bloom filter when written to
    /// Iceberg. Use `&[]` to disable bloom filters for the table.
    ///
    /// Bloom filters cheaply skip whole row groups when an equality
    /// predicate on a high-cardinality column does not match, which
    /// is the access pattern for Tempo trace-by-id and any future
    /// log-by-trace lookup.
    pub bloom_filter_columns: &'static [&'static str],
    /// Per-column Parquet encoding overrides applied when writing to
    /// Iceberg. Use `&[]` to fall back to parquet-rs defaults.
    ///
    /// See [`icegate_common::parquet_encoding`] for the per-table
    /// encoding lists shared between this writer and the WAL writer.
    pub column_encodings: &'static [ColumnEncoding],
}

/// Runs shift jobs inside the ingest process.
pub struct Shifter {
    manager: JobsManager,
}

/// Handle for stopping a running shifter.
pub struct ShifterHandle {
    handle: JobsManagerHandle,
}

impl Shifter {
    /// Create a new shifter instance.
    ///
    /// # Errors
    ///
    /// Returns [`IngestError`] if the shift configuration is invalid, no job is configured, a
    /// task runner rejects its settings, or the jobs manager cannot be built.
    pub async fn new(
        catalog: Arc<dyn Catalog>,
        queue_reader: Arc<ParquetQueueReader>,
        shift_config: Arc<ShiftConfig>,
        jobs_storage: S3StorageConfig,
        shift_metrics: ShiftMetrics,
        jobs_manager_metrics: Arc<dyn MetricsSink>,
        jobs: &[ShiftJobSpec],
    ) -> Result<Self> {
        Self::new_with_max_iterations(
            catalog,
            queue_reader,
            shift_config,
            jobs_storage,
            shift_metrics,
            jobs_manager_metrics,
            jobs,
            None,
        )
        .await
    }

    /// Create a shifter whose jobs stop after `max_iterations` plan cycles.
    ///
    /// With `max_iterations = Some(1)` each job runs one plan iteration - fanning out its shift
    /// tasks and draining them through the commit task - and never starts a second one. `None`
    /// runs every job on the configured interval, which is what [`Self::new`] does.
    ///
    /// This is a test seam: production callers use [`Self::new`]. The component test drives the
    /// real assembly through it rather than rebuilding the task topology, so a task that is no
    /// longer registered here fails that test instead of passing a parallel copy of the wiring.
    ///
    /// # Errors
    ///
    /// Same as [`Self::new`].
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_max_iterations(
        catalog: Arc<dyn Catalog>,
        queue_reader: Arc<ParquetQueueReader>,
        shift_config: Arc<ShiftConfig>,
        jobs_storage: S3StorageConfig,
        shift_metrics: ShiftMetrics,
        jobs_manager_metrics: Arc<dyn MetricsSink>,
        jobs: &[ShiftJobSpec],
        max_iterations: Option<u64>,
    ) -> Result<Self> {
        shift_config.validate()?;
        if jobs.is_empty() {
            return Err(IngestError::Shift("no shift jobs configured".to_string()));
        }

        let queue_reader = Arc::new(QueueReaderWithMetrics::new(queue_reader, shift_metrics.clone()));
        let timeouts = TimeoutEstimator::new(&shift_config.timeouts);
        let plan_timeout = timeouts.plan_timeout();
        let iteration_interval = Duration::from_millis(shift_config.jobsmanager.iteration_interval_millisecs);

        let mut builder = JobsManager::builder()
            .s3(jobs_storage)
            .workers(shift_config.jobsmanager.worker_count)
            .poll_interval(Duration::from_millis(shift_config.jobsmanager.poll_interval_ms))
            .metrics(jobs_manager_metrics);

        for spec in jobs {
            let storage = Arc::new(IcebergStorage::new(
                Arc::clone(&catalog),
                spec.table,
                shift_config.as_ref(),
                writer_max_parquet_bytes(shift_config.read.upper_bound_input_mb_per_task * 1024 * 1024),
                spec.bloom_filter_columns,
                spec.column_encodings,
            ));
            let plan_runner = Arc::new(PlanTaskRunnerImpl::new(
                Arc::clone(&queue_reader),
                Arc::clone(&storage),
                shift_config.clone(),
                timeouts.clone(),
                spec.topic,
                spec.planner_partition_spec,
            ));
            let plan_runner = Arc::new(PlanTaskRunnerWithMetrics::new(
                plan_runner,
                shift_metrics.clone(),
                spec.topic,
            ));

            let shift_storage = Arc::new(StorageWithMetrics::new(storage, shift_metrics.clone(), spec.topic));
            let shift_storage_for_runner = Arc::clone(&shift_storage);
            let row_groups_merger_observer = Arc::new(RowGroupsMergerObserverWithMetrics::new(
                shift_metrics.clone(),
                spec.topic,
            ));
            let shift_runner = Arc::new(
                ShiftTaskRunnerImpl::new(
                    Arc::clone(&queue_reader),
                    shift_storage_for_runner,
                    spec.topic,
                    shift_config.write.row_group_size,
                    spec.descriptor,
                )?
                .with_row_groups_merger_observer(row_groups_merger_observer)
                .with_segment_read_parallelism(shift_config.read.shift_segment_read_parallelism)?,
            );
            let shift_runner = Arc::new(ShiftTaskRunnerWithMetrics::new(
                shift_runner,
                shift_metrics.clone(),
                spec.topic,
            ));
            let commit_runner = Arc::new(CommitTaskRunnerImpl::new(Arc::clone(&shift_storage), spec.topic));
            let commit_runner = Arc::new(CommitTaskRunnerWithMetrics::new(
                commit_runner,
                shift_metrics.clone(),
                spec.topic,
            ));

            builder = builder.job(spec.job_name, move |job| {
                job.every(iteration_interval);
                if let Some(max_iterations) = max_iterations {
                    job.max_iterations(max_iterations);
                }
                job.add_task(
                    TaskDefinition::new(PLAN_TASK_CODE, plan_timeout),
                    Arc::new(PlanExecutor::new(plan_runner)),
                );
                // The shift and commit tasks are created by the plan task at runtime, so only
                // their executors are registered here.
                job.add_task_executor(SHIFT_TASK_CODE, Arc::new(ShiftExecutor::new(shift_runner)));
                job.add_task_executor(COMMIT_TASK_CODE, Arc::new(CommitExecutor::new(commit_runner)));
            });
        }

        let manager = builder.build().await.map_err(map_shift_error)?;

        Ok(Self { manager })
    }

    /// Start shifter workers and return a handle for shutdown.
    pub fn start(&self) -> Result<ShifterHandle> {
        let handle = self.manager.start().map_err(map_shift_error)?;
        Ok(ShifterHandle { handle })
    }
}

impl ShifterHandle {
    /// Stop shifter workers and wait for completion.
    pub async fn shutdown(self) -> Result<()> {
        self.handle.shutdown().await.map_err(map_shift_error)
    }
}

fn map_shift_error<E: std::fmt::Display>(error: E) -> IngestError {
    IngestError::Shift(error.to_string())
}
