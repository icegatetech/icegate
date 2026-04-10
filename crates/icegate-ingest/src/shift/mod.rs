//! Shift operations: moving data from WAL to Iceberg.

/// Backpressure controller for shift overload protection.
pub mod backpressure;
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
mod row_groups_merger;
/// Shift task runner for shift operations.
pub mod shift_runner;
/// Task timeout estimation utilities.
mod timeout;

use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::{collections::HashMap, sync::Arc, time::Duration};

use backpressure::BackpressureController;
use chrono::Duration as ChronoDuration;
use commit_runner::CommitTaskRunnerImpl;
pub(crate) use config::ShiftConfig;
pub(crate) use executor::{
    COMMIT_TASK_CODE, CommitInput, Executor, PLAN_TASK_CODE, PlannedRowGroup, SHIFT_TASK_CODE, SegmentToRead,
    ShiftInput, ShiftOutput,
};
use iceberg::Catalog;
pub(crate) use iceberg_storage::IcebergStorage;
use icegate_common::{LOGS_TABLE, LOGS_TOPIC};
use icegate_jobmanager::{
    CachedStorage, JobDefinition, JobRegistry, JobsManager, JobsManagerConfig, JobsManagerHandle,
    Metrics as JobMetrics, S3Storage, TaskCode, TaskDefinition, WorkerConfig, s3_storage::S3StorageConfig,
};
use icegate_queue::ParquetQueueReader;
use instrumentation::{
    CommitTaskRunnerWithMetrics, PlanTaskRunnerWithMetrics, QueueReaderWithMetrics, ShiftTaskRunnerWithMetrics,
    StorageWithMetrics,
};
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
    rejection_probability: Arc<AtomicU32>,
}

/// Handle for stopping a running shifter.
pub struct ShifterHandle {
    handle: JobsManagerHandle,
}

impl Shifter {
    /// Create a new shifter instance.
    pub async fn new(
        catalog: Arc<dyn Catalog>,
        queue_reader: Arc<ParquetQueueReader>,
        shift_config: Arc<ShiftConfig>,
        jobs_storage: S3StorageConfig,
        shift_metrics: ShiftMetrics,
        mut jobs_manager_metrics: JobMetrics,
    ) -> Result<Self> {
        shift_config.validate()?;
        let storage = Arc::new(IcebergStorage::new(
            Arc::clone(&catalog),
            LOGS_TABLE,
            shift_config.as_ref(),
        ));
        let timeouts = TimeoutEstimator::new(&shift_config.timeouts)?;
        let plan_timeout = timeouts.plan_timeout();
        let queue_reader = Arc::new(QueueReaderWithMetrics::new(queue_reader, shift_metrics.clone()));
        let plan_runner = Arc::new(PlanTaskRunnerImpl::new(
            Arc::clone(&queue_reader),
            Arc::clone(&storage),
            shift_config.clone(),
            timeouts,
            LOGS_TOPIC,
        ));
        let plan_runner = Arc::new(PlanTaskRunnerWithMetrics::new(
            plan_runner,
            shift_metrics.clone(),
            LOGS_TOPIC,
        ));

        let shift_storage = Arc::new(StorageWithMetrics::new(storage, shift_metrics.clone(), LOGS_TOPIC));
        let shift_storage_for_runner = Arc::clone(&shift_storage);
        let shift_runner = Arc::new(
            ShiftTaskRunnerImpl::new(
                queue_reader,
                shift_storage_for_runner,
                LOGS_TOPIC,
                shift_config.write.row_group_size,
            )?
            .with_segment_read_parallelism(shift_config.read.shift_segment_read_parallelism)?,
        );
        let shift_runner = Arc::new(ShiftTaskRunnerWithMetrics::new(
            shift_runner,
            shift_metrics.clone(),
            LOGS_TOPIC,
        ));
        let shift_metrics_for_bp = shift_metrics.clone();
        let commit_runner = Arc::new(CommitTaskRunnerImpl::new(Arc::clone(&shift_storage), LOGS_TOPIC));
        let commit_runner = Arc::new(CommitTaskRunnerWithMetrics::new(
            commit_runner,
            shift_metrics,
            LOGS_TOPIC,
        ));

        let executor = Arc::new(Executor::new(plan_runner, shift_runner, commit_runner));

        let initial_task =
            TaskDefinition::new(TaskCode::new(PLAN_TASK_CODE), vec![], plan_timeout).map_err(map_shift_error)?;

        let mut executors = HashMap::new();
        executors.insert(TaskCode::new(PLAN_TASK_CODE), Arc::clone(&executor).plan_executor());
        executors.insert(TaskCode::new(SHIFT_TASK_CODE), Arc::clone(&executor).shift_executor());
        executors.insert(TaskCode::new(COMMIT_TASK_CODE), Arc::clone(&executor).commit_executor());

        let iteration_interval_ms =
            i64::try_from(shift_config.jobsmanager.iteration_interval_millisecs).map_err(|_| {
                IngestError::Shift(format!(
                    "jobsmanager.iteration_interval_millisecs {} exceeds i64",
                    shift_config.jobsmanager.iteration_interval_millisecs
                ))
            })?;
        let job_def = JobDefinition::new("shift_logs".into(), vec![initial_task], executors)
            .map_err(map_shift_error)?
            .with_iteration_interval(ChronoDuration::milliseconds(iteration_interval_ms))
            .map_err(map_shift_error)?;
        let job_registry = Arc::new(JobRegistry::new(vec![job_def]).map_err(map_shift_error)?);

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

        let controller = Arc::new(Mutex::new(BackpressureController::new(
            shift_config.backpressure.clone(),
            shift_config.jobsmanager.iteration_interval_millisecs,
        )));
        // Lock is safe here: freshly created, no contention possible.
        #[allow(clippy::expect_used)]
        let rejection_probability = controller
            .lock()
            .expect("backpressure controller lock poisoned")
            .rejection_probability_atomic();

        jobs_manager_metrics.set_on_iteration_complete(Arc::new(move |_code, duration| {
            // Panic on poisoned lock is intentional: if the controller panicked,
            // the shift process is in an unrecoverable state.
            #[allow(clippy::expect_used)]
            let mut ctrl = controller.lock().expect("backpressure controller lock poisoned");
            let was_zero = ctrl.rejection_probability() == 0.0;
            ctrl.update(duration);
            let p = ctrl.rejection_probability();

            #[allow(clippy::cast_precision_loss)]
            let duration_ms = duration.as_millis() as f64;
            shift_metrics_for_bp.record_backpressure(
                p,
                ctrl.last_load_ratio(),
                ctrl.last_error(),
                ctrl.integral(),
                duration_ms,
            );

            tracing::debug!(
                rejection_probability = p,
                load_ratio = ctrl.last_load_ratio(),
                error = ctrl.last_error(),
                integral = ctrl.integral(),
                "Shift backpressure update"
            );

            if was_zero && p > 0.0 {
                tracing::info!(rejection_probability = p, "Shift backpressure activated");
            } else if !was_zero && p == 0.0 {
                tracing::info!("Shift backpressure deactivated");
            }
            if p > 0.5 {
                tracing::warn!(rejection_probability = p, "Shift backpressure high");
            }
        }));

        let manager = JobsManager::new(
            cached_storage,
            manager_config,
            job_registry,
            jobs_manager_metrics.clone(),
        )
        .map_err(map_shift_error)?;

        Ok(Self {
            manager,
            rejection_probability,
        })
    }

    /// Start shifter workers and return a handle for shutdown.
    pub fn start(&self) -> Result<ShifterHandle> {
        let handle = self.manager.start().map_err(map_shift_error)?;
        Ok(ShifterHandle { handle })
    }

    /// Get the shared rejection probability atomic for OTLP handlers.
    pub fn rejection_probability(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.rejection_probability)
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
