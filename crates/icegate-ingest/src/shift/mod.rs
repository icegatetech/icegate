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
/// Plan task runner for shift operations.
pub mod plan_runner;
/// Shift task runner for shift operations.
pub mod shift_runner;
/// Task timeout estimation utilities.
mod timeout;

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Duration as ChronoDuration;
use commit_runner::CommitTaskRunnerImpl;
pub use config::ShiftConfig;
pub use executor::{
    COMMIT_TASK_CODE, CommitInput, Executor, PLAN_TASK_CODE, SHIFT_TASK_CODE, SegmentToRead, ShiftInput, ShiftOutput,
};
use iceberg::Catalog;
pub use iceberg_storage::{IcebergStorage, WrittenDataFiles};
use icegate_common::{LOGS_TABLE, LOGS_TOPIC, METRICS_TABLE, METRICS_TOPIC};
use icegate_jobmanager::{
    CachedStorage, JobDefinition, JobRegistry, JobsManager, JobsManagerConfig, JobsManagerHandle,
    Metrics as JobMetrics, S3Storage, TaskCode, TaskDefinition, WorkerConfig, s3_storage::S3StorageConfig,
};
use icegate_queue::ParquetQueueReader;
use instrumentation::{
    CommitTaskRunnerWithMetrics, PlanTaskRunnerWithMetrics, QueueReaderWithMetrics, ShiftTaskRunnerWithMetrics,
    StorageWithMetrics,
};
pub use parquet_meta_reader::data_files_from_parquet_paths;
use plan_runner::PlanTaskRunnerImpl;
use shift_runner::ShiftTaskRunnerImpl;
use timeout::TimeoutEstimator;

use crate::{
    error::{IngestError, Result},
    infra::metrics::ShiftMetrics,
};

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
    /// Registers shift jobs for all WAL-backed topics (logs and metrics).
    /// Each topic gets its own plan/shift/commit pipeline with independent
    /// executors and metrics.
    pub async fn new(
        catalog: Arc<dyn Catalog>,
        queue_reader: Arc<ParquetQueueReader>,
        shift_config: Arc<ShiftConfig>,
        jobs_storage: S3StorageConfig,
        shift_metrics: ShiftMetrics,
        jobs_manager_metrics: JobMetrics,
    ) -> Result<Self> {
        shift_config.validate()?;

        let iteration_interval_ms =
            i64::try_from(shift_config.jobsmanager.iteration_interval_millisecs).map_err(|_| {
                IngestError::Shift(format!(
                    "jobsmanager.iteration_interval_millisecs {} exceeds i64",
                    shift_config.jobsmanager.iteration_interval_millisecs
                ))
            })?;
        let iteration_interval = ChronoDuration::milliseconds(iteration_interval_ms);
        let queue_reader = Arc::new(QueueReaderWithMetrics::new(queue_reader, shift_metrics.clone()));

        // Create a shift job for each WAL-backed topic
        let logs_job = Self::create_job_definition(
            "shift_logs",
            LOGS_TABLE,
            LOGS_TOPIC,
            &catalog,
            &queue_reader,
            &shift_config,
            &shift_metrics,
            iteration_interval,
        )?;
        let metrics_job = Self::create_job_definition(
            "shift_metrics",
            METRICS_TABLE,
            METRICS_TOPIC,
            &catalog,
            &queue_reader,
            &shift_config,
            &shift_metrics,
            iteration_interval,
        )?;

        let job_registry = Arc::new(JobRegistry::new(vec![logs_job, metrics_job]).map_err(map_shift_error)?);

        let s3_storage = Arc::new(
            S3Storage::new(jobs_storage, job_registry.clone(), jobs_manager_metrics.clone())
                .await
                .map_err(map_shift_error)?,
        );
        let cached_storage = Arc::new(CachedStorage::new(s3_storage, jobs_manager_metrics.clone()));

        let manager_config = JobsManagerConfig {
            worker_count: shift_config.jobsmanager.worker_count,
            worker_config: WorkerConfig {
                poll_interval: Duration::from_millis(shift_config.jobsmanager.poll_interval_ms),
                ..Default::default()
            },
        };

        let manager = JobsManager::new(
            cached_storage,
            manager_config,
            job_registry,
            jobs_manager_metrics.clone(),
        )
        .map_err(map_shift_error)?;

        Ok(Self { manager })
    }

    /// Creates a `JobDefinition` for shifting a single WAL topic to its
    /// Iceberg table.
    #[allow(clippy::too_many_arguments)]
    fn create_job_definition(
        job_code: &str,
        table_name: &str,
        topic: &str,
        catalog: &Arc<dyn Catalog>,
        queue_reader: &Arc<QueueReaderWithMetrics<ParquetQueueReader>>,
        shift_config: &Arc<ShiftConfig>,
        shift_metrics: &ShiftMetrics,
        iteration_interval: ChronoDuration,
    ) -> Result<JobDefinition> {
        let storage = Arc::new(IcebergStorage::new(Arc::clone(catalog), table_name, shift_config.as_ref()));
        let timeouts = TimeoutEstimator::new(&shift_config.timeouts)?;
        let plan_timeout = timeouts.plan_timeout();

        let plan_runner = Arc::new(PlanTaskRunnerImpl::new(
            Arc::clone(queue_reader),
            Arc::clone(&storage),
            shift_config.clone(),
            timeouts,
            topic,
        ));
        let plan_runner = Arc::new(PlanTaskRunnerWithMetrics::new(plan_runner, shift_metrics.clone(), topic));

        let shift_storage = Arc::new(StorageWithMetrics::new(storage, shift_metrics.clone(), topic));
        let shift_runner = Arc::new(
            ShiftTaskRunnerImpl::new(Arc::clone(queue_reader), Arc::clone(&shift_storage), topic)
                .with_segment_read_parallelism(shift_config.read.shift_segment_read_parallelism)?,
        );
        let shift_runner = Arc::new(ShiftTaskRunnerWithMetrics::new(shift_runner, shift_metrics.clone(), topic));

        let commit_runner = Arc::new(CommitTaskRunnerImpl::new(Arc::clone(&shift_storage), topic));
        let commit_runner = Arc::new(CommitTaskRunnerWithMetrics::new(
            commit_runner,
            shift_metrics.clone(),
            topic,
        ));

        let executor = Arc::new(Executor::new(plan_runner, shift_runner, commit_runner));

        let initial_task =
            TaskDefinition::new(TaskCode::new(PLAN_TASK_CODE), vec![], plan_timeout).map_err(map_shift_error)?;

        let mut executors = HashMap::new();
        executors.insert(TaskCode::new(PLAN_TASK_CODE), Arc::clone(&executor).plan_executor());
        executors.insert(TaskCode::new(SHIFT_TASK_CODE), Arc::clone(&executor).shift_executor());
        executors.insert(TaskCode::new(COMMIT_TASK_CODE), Arc::clone(&executor).commit_executor());

        JobDefinition::new(job_code.into(), vec![initial_task], executors)
            .map_err(map_shift_error)?
            .with_iteration_interval(iteration_interval)
            .map_err(map_shift_error)
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
