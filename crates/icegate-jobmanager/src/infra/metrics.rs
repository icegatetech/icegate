use std::time::Duration;

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, MeterProvider as _},
};
use opentelemetry_sdk::metrics::SdkMeterProvider;

use crate::{JobCode, JobStatus, TaskCode, TaskStatus};

#[derive(Clone)]
pub struct Metrics {
    enabled: bool,
    job_duration: Histogram<f64>,
    task_duration: Histogram<f64>,
    s3_latency: Histogram<f64>,
    cache_hits: Counter<u64>,
    cache_misses: Counter<u64>,
    task_stolen: Counter<u64>,
    save_conflict_retries: Counter<u64>,
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
        }
    }

    pub fn record_job_iteration_complete(&self, code: &JobCode, status: &JobStatus, duration: Duration) {
        if !self.enabled {
            return;
        }
        self.job_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("code", code.to_string()),
                KeyValue::new("status", format!("{status:?}")),
            ],
        );
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
