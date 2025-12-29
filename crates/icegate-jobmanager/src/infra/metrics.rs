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
}

impl Metrics {
    pub fn new_disabled() -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("jobmanager");

        Self {
            enabled: false,
            job_duration: meter.f64_histogram("jobmanager_job_duration_seconds").build(),
            task_duration: meter.f64_histogram("jobmanager_task_duration_seconds").build(),
            s3_latency: meter.f64_histogram("jobmanager_storage_s3_latency_seconds").build(),
            cache_hits: meter.u64_counter("jobmanager_storage_cache_hits_total").build(),
            cache_misses: meter.u64_counter("jobmanager_storage_cache_misses_total").build(),
        }
    }

    pub fn new(meter: Meter) -> Self {
        let job_duration = meter
            .f64_histogram("jobmanager_job_duration_seconds")
            .with_description("Duration of job execution from start to finish")
            .with_unit("s")
            .build();

        let task_duration = meter
            .f64_histogram("jobmanager_task_duration_seconds")
            .with_description("Duration of individual task execution")
            .with_unit("s")
            .build();

        let s3_latency = meter
            .f64_histogram("jobmanager_storage_s3_latency_seconds")
            .with_description("Latency of S3 GET and PUT operations")
            .with_unit("s")
            .build();

        let cache_hits = meter
            .u64_counter("jobmanager_storage_cache_hits_total")
            .with_description("Total number of cache hits")
            .with_unit("1")
            .build();

        let cache_misses = meter
            .u64_counter("jobmanager_storage_cache_misses_total")
            .with_description("Total number of cache misses")
            .with_unit("1")
            .build();

        Self {
            enabled: true,
            job_duration,
            task_duration,
            s3_latency,
            cache_hits,
            cache_misses,
        }
    }

    pub fn record_job_iteration_complete(&self, code: &JobCode, status: &JobStatus, duration: Duration) {
        if !self.enabled {
            return;
        }
        self.job_duration.record(duration.as_secs_f64(), &[
            KeyValue::new("code", code.to_string()),
            KeyValue::new("status", format!("{:?}", status)),
        ]);
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
        self.task_duration.record(duration.as_secs_f64(), &[
            KeyValue::new("job_code", job_code.to_string()),
            KeyValue::new("task_code", task_code.to_string()),
            KeyValue::new("status", format!("{:?}", status)),
        ]);
    }

    pub fn record_s3_operation(&self, operation: &str, status: &str, duration: Duration) {
        if !self.enabled {
            return;
        }
        self.s3_latency.record(duration.as_secs_f64(), &[
            KeyValue::new("operation", operation.to_string()),
            KeyValue::new("http_status", status.to_string()),
        ]);
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
}
