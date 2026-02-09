use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use icegate_jobmanager::{Error, JobManager};
use icegate_queue::{QueueReader, SegmentsPlan, Topic};
use tokio_util::sync::CancellationToken;

use super::{
    commit_runner::{CommitResult, CommitTaskFailure, CommitTaskRunner},
    executor::TaskStatus,
    iceberg_storage::{Storage, WrittenDataFiles},
    plan_runner::{PlanTaskResult, PlanTaskRunner},
    shift_runner::{ShiftTaskFailure, ShiftTaskFailureReason, ShiftTaskResult, ShiftTaskRunner},
};
use crate::infra::metrics::ShiftMetrics;

/// Plan task runner wrapper that records metrics.
pub struct PlanTaskRunnerWithMetrics<R> {
    inner: Arc<R>,
    metrics: ShiftMetrics,
    topic: Topic,
}

impl<R> PlanTaskRunnerWithMetrics<R>
where
    R: PlanTaskRunner + 'static,
{
    /// Create a new plan runner wrapper.
    pub fn new(inner: Arc<R>, metrics: ShiftMetrics, topic: impl Into<String>) -> Self {
        Self {
            inner,
            metrics,
            topic: topic.into(),
        }
    }
}

#[async_trait]
impl<R> PlanTaskRunner for PlanTaskRunnerWithMetrics<R>
where
    R: PlanTaskRunner + 'static,
{
    async fn run(
        &self,
        task_id: uuid::Uuid,
        manager: &dyn JobManager,
        cancel_token: &CancellationToken,
    ) -> Result<PlanTaskResult, Error> {
        let start = Instant::now();
        let result = self.inner.run(task_id, manager, cancel_token).await;
        match result {
            Ok(plan_result) => {
                self.metrics
                    .record_plan_duration(start.elapsed(), &self.topic, plan_result.status.as_str());

                let backlog = plan_result.last_offset.map_or(0, |last_offset| {
                    if last_offset >= plan_result.start_offset {
                        last_offset - plan_result.start_offset + 1
                    } else {
                        0
                    }
                });
                self.metrics.record_backlog_segments(backlog, &self.topic);

                if matches!(plan_result.status, TaskStatus::Ok | TaskStatus::Empty) {
                    self.metrics.add_planned_segments(plan_result.segments_count, &self.topic);
                    self.metrics
                        .add_planned_record_batches(plan_result.record_batches_total, &self.topic);
                    self.metrics.add_planned_tasks(plan_result.shift_task_ids.len(), &self.topic);
                    self.metrics.record_task_success(super::PLAN_TASK_CODE, &self.topic);
                } else if matches!(plan_result.status, TaskStatus::Cancelled) {
                    self.metrics
                        .record_task_failure(super::PLAN_TASK_CODE, "cancelled", &self.topic);
                } else {
                    self.metrics
                        .record_task_failure(super::PLAN_TASK_CODE, plan_result.status.as_str(), &self.topic);
                }
                Ok(plan_result)
            }
            Err(err) => {
                // TODO(med): map error types to TaskStatus, for example, cancel to Cancelled (this will require refactoring in the Job Manager)
                self.metrics
                    .record_plan_duration(start.elapsed(), &self.topic, TaskStatus::Error.as_str());
                self.metrics
                    .record_task_failure(super::PLAN_TASK_CODE, "queue_plan", &self.topic);
                Err(err)
            }
        }
    }
}

/// Shift task runner wrapper that records metrics.
pub struct ShiftTaskRunnerWithMetrics<R> {
    inner: Arc<R>,
    metrics: ShiftMetrics,
    topic: Topic,
}

impl<R> ShiftTaskRunnerWithMetrics<R>
where
    R: ShiftTaskRunner + 'static,
{
    /// Create a new shift runner wrapper.
    pub fn new(inner: Arc<R>, metrics: ShiftMetrics, topic: impl Into<String>) -> Self {
        Self {
            inner,
            metrics,
            topic: topic.into(),
        }
    }
}

#[async_trait]
impl<R> ShiftTaskRunner for ShiftTaskRunnerWithMetrics<R>
where
    R: ShiftTaskRunner + 'static,
{
    async fn run(
        &self,
        task: Arc<dyn icegate_jobmanager::ImmutableTask>,
        manager: &dyn JobManager,
        cancel_token: &CancellationToken,
    ) -> Result<ShiftTaskResult, ShiftTaskFailure> {
        let result = self.inner.run(task, manager, cancel_token).await;
        match &result {
            Ok(shift_result) => match shift_result.status {
                TaskStatus::Ok => {
                    self.metrics
                        .record_segment_record_batches_per_task(shift_result.record_batches_total, &self.topic);
                    self.metrics.record_segment_rows_per_task(shift_result.rows_total, &self.topic);
                    self.metrics.add_parquet_files(shift_result.parquet_files_total, &self.topic);
                    self.metrics.add_bytes_written(shift_result.bytes_written_total, &self.topic);
                    self.metrics.record_task_success(super::SHIFT_TASK_CODE, &self.topic);
                }
                TaskStatus::Cancelled => {
                    self.metrics.record_task_failure(
                        super::SHIFT_TASK_CODE,
                        ShiftTaskFailureReason::Cancelled.as_str(),
                        &self.topic,
                    );
                }
                TaskStatus::Empty => {
                    self.metrics.record_task_failure(
                        super::SHIFT_TASK_CODE,
                        ShiftTaskFailureReason::EmptyBatches.as_str(),
                        &self.topic,
                    );
                }
                TaskStatus::Error | TaskStatus::Timeout => {
                    self.metrics.record_task_failure(
                        super::SHIFT_TASK_CODE,
                        ShiftTaskFailureReason::Write.as_str(),
                        &self.topic,
                    );
                }
            },
            Err(failure) => {
                self.metrics
                    .record_task_failure(super::SHIFT_TASK_CODE, failure.reason().as_str(), &self.topic);
            }
        }
        result
    }
}

/// Commit task runner wrapper that records metrics.
pub struct CommitTaskRunnerWithMetrics<R> {
    inner: Arc<R>,
    metrics: ShiftMetrics,
    topic: Topic,
}

impl<R> CommitTaskRunnerWithMetrics<R>
where
    R: CommitTaskRunner + 'static,
{
    /// Create a new commit runner wrapper.
    pub fn new(inner: Arc<R>, metrics: ShiftMetrics, topic: impl Into<String>) -> Self {
        Self {
            inner,
            metrics,
            topic: topic.into(),
        }
    }
}

#[async_trait]
impl<R> CommitTaskRunner for CommitTaskRunnerWithMetrics<R>
where
    R: CommitTaskRunner + 'static,
{
    async fn run(
        &self,
        task: Arc<dyn icegate_jobmanager::ImmutableTask>,
        manager: &dyn JobManager,
        cancel_token: &CancellationToken,
    ) -> Result<CommitResult, CommitTaskFailure> {
        let result = self.inner.run(task, manager, cancel_token).await;
        match &result {
            Ok(commit_result) => {
                if commit_result.already_committed {
                    self.metrics.add_already_committed(&self.topic);
                }
                if matches!(commit_result.status, TaskStatus::Ok) {
                    self.metrics.record_task_success(super::COMMIT_TASK_CODE, &self.topic);
                } else {
                    self.metrics.record_task_failure(
                        super::COMMIT_TASK_CODE,
                        commit_result.status.as_str(),
                        &self.topic,
                    );
                }
            }
            Err(failure) => {
                self.metrics
                    .record_task_failure(super::COMMIT_TASK_CODE, failure.reason().as_str(), &self.topic);
            }
        }
        result
    }
}

/// Queue reader wrapper that records metrics.
pub struct QueueReaderWithMetrics<Q> {
    inner: Arc<Q>,
    metrics: ShiftMetrics,
}

impl<Q> QueueReaderWithMetrics<Q>
where
    Q: QueueReader + 'static,
{
    /// Create a new queue reader wrapper.
    pub const fn new(inner: Arc<Q>, metrics: ShiftMetrics) -> Self {
        Self { inner, metrics }
    }
}

#[async_trait]
impl<Q> QueueReader for QueueReaderWithMetrics<Q>
where
    Q: QueueReader + 'static,
{
    async fn plan_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        group_by_column_name: &str,
        max_record_batches_per_task: usize,
        max_input_bytes_per_task: u64,
        cancel_token: &CancellationToken,
    ) -> icegate_queue::Result<SegmentsPlan> {
        let start = Instant::now();
        let result = self
            .inner
            .plan_segments(
                topic,
                start_offset,
                group_by_column_name,
                max_record_batches_per_task,
                max_input_bytes_per_task,
                cancel_token,
            )
            .await;
        match &result {
            Ok(plan) => {
                self.metrics
                    .record_queue_plan_duration(start.elapsed(), topic.as_str(), TaskStatus::Ok.as_str());
                for group in &plan.groups {
                    self.metrics
                        .record_planned_input_bytes_per_task(group.input_bytes_total, topic.as_str());
                }
            }
            Err(_) => {
                self.metrics
                    .record_queue_plan_duration(start.elapsed(), topic.as_str(), TaskStatus::Error.as_str());
            }
        }
        result
    }

    async fn read_segment(
        &self,
        topic: &Topic,
        offset: u64,
        record_batch_idxs: &[usize],
        cancel_token: &CancellationToken,
    ) -> icegate_queue::Result<Vec<arrow::record_batch::RecordBatch>> {
        let start = Instant::now();
        let result = self.inner.read_segment(topic, offset, record_batch_idxs, cancel_token).await;
        match &result {
            Ok(_) => self.metrics.record_queue_read_segment_duration(
                start.elapsed(),
                topic.as_str(),
                TaskStatus::Ok.as_str(),
            ),
            Err(_) => self.metrics.record_queue_read_segment_duration(
                start.elapsed(),
                topic.as_str(),
                TaskStatus::Error.as_str(),
            ),
        }
        result
    }
}

/// Storage wrapper that records metrics.
pub struct StorageWithMetrics<S> {
    inner: Arc<S>,
    metrics: ShiftMetrics,
    topic: Topic,
}

impl<S> StorageWithMetrics<S>
where
    S: Storage + 'static,
{
    /// Create a new storage wrapper.
    pub fn new(inner: Arc<S>, metrics: ShiftMetrics, topic: impl Into<String>) -> Self {
        Self {
            inner,
            metrics,
            topic: topic.into(),
        }
    }
}

#[async_trait]
impl<S> Storage for StorageWithMetrics<S>
where
    S: Storage + 'static,
{
    async fn get_last_offset(&self, cancel_token: &CancellationToken) -> crate::error::Result<Option<u64>> {
        self.inner.get_last_offset(cancel_token).await
    }

    async fn write_record_batches(
        &self,
        batches: Vec<arrow::record_batch::RecordBatch>,
        cancel_token: &CancellationToken,
    ) -> crate::error::Result<WrittenDataFiles> {
        let start = Instant::now();
        let result = self.inner.write_record_batches(batches, cancel_token).await;
        match &result {
            Ok(_) => self
                .metrics
                .record_parquet_write_duration(start.elapsed(), &self.topic, TaskStatus::Ok.as_str()),
            Err(_) => {
                self.metrics
                    .record_parquet_write_duration(start.elapsed(), &self.topic, TaskStatus::Error.as_str());
            }
        }
        result
    }

    async fn get_data_files(
        &self,
        parquet_paths: &[String],
        cancel_token: &CancellationToken,
    ) -> crate::error::Result<Vec<iceberg::spec::DataFile>> {
        let start = Instant::now();
        let result = self.inner.get_data_files(parquet_paths, cancel_token).await;
        match &result {
            Ok(_) => self
                .metrics
                .record_get_data_files_duration(start.elapsed(), &self.topic, TaskStatus::Ok.as_str()),
            Err(_) => {
                self.metrics
                    .record_get_data_files_duration(start.elapsed(), &self.topic, TaskStatus::Error.as_str());
            }
        }
        result
    }

    async fn commit(
        &self,
        data_files: Vec<iceberg::spec::DataFile>,
        record_type: &str,
        last_offset: u64,
        cancel_token: &CancellationToken,
    ) -> crate::error::Result<usize> {
        let start = Instant::now();
        let result = self.inner.commit(data_files, record_type, last_offset, cancel_token).await;
        match &result {
            Ok(_) => self
                .metrics
                .record_commit_duration(start.elapsed(), &self.topic, TaskStatus::Ok.as_str()),
            Err(_) => self
                .metrics
                .record_commit_duration(start.elapsed(), &self.topic, TaskStatus::Error.as_str()),
        }
        result
    }
}
