use std::sync::Arc;
use std::time::Duration;

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, MeterProvider as _},
};
use opentelemetry_sdk::metrics::SdkMeterProvider;

use crate::{JobCode, JobStatus, TaskCode, TaskStatus};

/// Callback invoked when a job iteration completes.
pub type IterationCompleteCallback = Arc<dyn Fn(&JobCode, Duration) + Send + Sync>;

/// Metrics collector for job manager operations.
pub struct Metrics {
    enabled: bool,
    job_duration: Histogram<f64>,
    task_duration: Histogram<f64>,
    s3_latency: Histogram<f64>,
    cache_hits: Counter<u64>,
    cache_misses: Counter<u64>,
    task_stolen: Counter<u64>,
    save_conflict_retries: Counter<u64>,
    on_iteration_complete: Option<IterationCompleteCallback>,
}

// Manual `Clone` implementation because `Arc<dyn Fn>` does not derive `Clone`
// automatically, but `Arc::clone` is available.
impl Clone for Metrics {
    fn clone(&self) -> Self {
        Self {
            enabled: self.enabled,
            job_duration: self.job_duration.clone(),
            task_duration: self.task_duration.clone(),
            s3_latency: self.s3_latency.clone(),
            cache_hits: self.cache_hits.clone(),
            cache_misses: self.cache_misses.clone(),
            task_stolen: self.task_stolen.clone(),
            save_conflict_retries: self.save_conflict_retries.clone(),
            on_iteration_complete: self.on_iteration_complete.clone(),
        }
    }
}

impl Metrics {
    pub fn new_disabled() -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("jobmanager");

        Self {
            enabled: false,
            job_duration: meter.f64_histogram("icegate_jobmanager_job_duration").build(),
            task_duration: meter.f64_histogram("icegate_jobmanager_task_duration").build(),
            s3_latency: meter.f64_histogram("icegate_jobmanager_storage_s3_latency").build(),
            cache_hits: meter.u64_counter("icegate_jobmanager_storage_cache_hits").build(),
            cache_misses: meter.u64_counter("icegate_jobmanager_storage_cache_misses").build(),
            task_stolen: meter.u64_counter("icegate_jobmanager_task_stolen").build(),
            save_conflict_retries: meter.u64_counter("icegate_jobmanager_save_conflict_retry").build(),
            on_iteration_complete: None,
        }
    }

    pub fn new(meter: &Meter) -> Self {
        let job_duration = meter
            .f64_histogram("icegate_jobmanager_job_duration")
            .with_description("Duration of job execution from start to finish")
            .with_unit("s")
            .build();

        let task_duration = meter
            .f64_histogram("icegate_jobmanager_task_duration")
            .with_description("Duration of individual task execution")
            .with_unit("s")
            .build();

        let s3_latency = meter
            .f64_histogram("icegate_jobmanager_storage_s3_latency")
            .with_description("Latency of S3 GET and PUT operations")
            .with_unit("s")
            .build();

        let cache_hits = meter
            .u64_counter("icegate_jobmanager_storage_cache_hits")
            .with_description("Total number of cache hits")
            .with_unit("1")
            .build();

        let cache_misses = meter
            .u64_counter("icegate_jobmanager_storage_cache_misses")
            .with_description("Total number of cache misses")
            .with_unit("1")
            .build();

        let task_stolen = meter
            .u64_counter("icegate_jobmanager_task_stolen")
            .with_description("Total number of task steal events due to worker mismatch")
            .with_unit("1")
            .build();

        let save_conflict_retries = meter
            .u64_counter("icegate_jobmanager_save_conflict_retry")
            .with_description("Total number of save retries caused by optimistic concurrency conflicts")
            .with_unit("1")
            .build();

        Self {
            enabled: true,
            job_duration,
            task_duration,
            s3_latency,
            cache_hits,
            cache_misses,
            task_stolen,
            save_conflict_retries,
            on_iteration_complete: None,
        }
    }

    /// Set a callback to be invoked when a job iteration completes.
    pub fn set_on_iteration_complete(&mut self, callback: IterationCompleteCallback) {
        self.on_iteration_complete = Some(callback);
    }

    /// Record the completion of a job iteration, emitting metrics and invoking
    /// the iteration-complete callback (if set).
    ///
    /// The callback fires regardless of the `enabled` flag because backpressure
    /// is a functional concern, not an observability concern.
    pub fn record_job_iteration_complete(&self, code: &JobCode, status: &JobStatus, duration: Duration) {
        if self.enabled {
            self.job_duration.record(
                duration.as_secs_f64(),
                &[
                    KeyValue::new("code", code.to_string()),
                    KeyValue::new("status", format!("{status:?}")),
                ],
            );
        }
        if let Some(ref callback) = self.on_iteration_complete {
            callback(code, duration);
        }
    }

    pub fn record_task_processed(
        &self,
        job_code: &JobCode,
        task_code: &TaskCode,
        status: &TaskStatus,
        duration: Duration,
    ) {
        if !self.enabled {
            return;
        }
        self.task_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("job_code", job_code.to_string()),
                KeyValue::new("task_code", task_code.to_string()),
                KeyValue::new("status", format!("{status:?}")),
            ],
        );
    }

    pub fn record_s3_operation(&self, operation: &str, status: &str, duration: Duration) {
        if !self.enabled {
            return;
        }
        self.s3_latency.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("operation", operation.to_string()),
                KeyValue::new("http_status", status.to_string()),
            ],
        );
    }

    pub fn record_cache_hit(&self, method: &str) {
        if !self.enabled {
            return;
        }
        self.cache_hits.add(1, &[KeyValue::new("method", method.to_string())]);
    }

    pub fn record_cache_miss(&self, method: &str) {
        if !self.enabled {
            return;
        }
        self.cache_misses.add(1, &[KeyValue::new("method", method.to_string())]);
    }

    pub fn record_task_stolen(&self, job_code: &JobCode, task_code: &TaskCode, phase: &'static str) {
        if !self.enabled {
            return;
        }
        self.task_stolen.add(
            1,
            &[
                KeyValue::new("job_code", job_code.to_string()),
                KeyValue::new("task_code", task_code.to_string()),
                KeyValue::new("phase", phase),
            ],
        );
    }

    pub fn record_save_conflict_retry(&self, job_code: &JobCode, phase: &'static str) {
        if !self.enabled {
            return;
        }
        self.save_conflict_retries.add(
            1,
            &[
                KeyValue::new("job_code", job_code.to_string()),
                KeyValue::new("phase", phase),
            ],
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[test]
    fn iteration_callback_is_called_on_job_complete() {
        let called = Arc::new(AtomicU64::new(0));
        let called_clone = Arc::clone(&called);

        let mut metrics = Metrics::new_disabled();
        metrics.set_on_iteration_complete(Arc::new(move |_code, duration| {
            assert!(duration.as_millis() > 0 || duration.is_zero());
            called_clone.fetch_add(1, Ordering::Relaxed);
        }));

        let code = JobCode::from("test_job");
        metrics.record_job_iteration_complete(&code, &JobStatus::Completed, Duration::from_millis(500));

        assert_eq!(called.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn iteration_callback_not_called_when_unset() {
        let metrics = Metrics::new_disabled();
        let code = JobCode::from("test_job");
        // Should not panic
        metrics.record_job_iteration_complete(&code, &JobStatus::Completed, Duration::from_millis(100));
    }
}
