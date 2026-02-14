//! Prometheus metrics for the query service.
//!
//! Provides a `QueryMetrics` struct that records OpenTelemetry metrics for
//! every phase of the query request lifecycle: HTTP handling, LogQL parsing,
//! query planning, DataFusion execution, and response formatting.

use std::time::{Duration, Instant};

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram, Meter, MeterProvider as _},
};
use opentelemetry_sdk::metrics::SdkMeterProvider;

/// Metrics recorded throughout the query request lifecycle.
///
/// Follows the same `new(&Meter)` / `new_disabled()` pattern used by
/// `icegate-ingest` metrics. When disabled, all recording methods are
/// short-circuited.
#[derive(Clone)]
pub struct QueryMetrics {
    enabled: bool,
    requests_total: Counter<u64>,
    request_duration: Histogram<f64>,
    parse_duration: Histogram<f64>,
    plan_duration: Histogram<f64>,
    execute_duration: Histogram<f64>,
    format_duration: Histogram<f64>,
    result_rows: Histogram<f64>,
    result_bytes: Histogram<f64>,
    session_create_duration: Histogram<f64>,
    provider_refresh_duration: Histogram<f64>,
    provider_refresh_errors_total: Counter<u64>,
    errors_total: Counter<u64>,
    active_queries: Gauge<i64>,
}

impl QueryMetrics {
    /// Build a metrics recorder that performs no-ops.
    ///
    /// All instruments are created against a throw-away provider so that
    /// recording calls are valid but produce no observable output.
    pub fn new_disabled() -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("query");

        Self {
            enabled: false,
            requests_total: meter.u64_counter("icegate_query_requests_total").build(),
            request_duration: meter.f64_histogram("icegate_query_request_duration").build(),
            parse_duration: meter.f64_histogram("icegate_query_parse_duration").build(),
            plan_duration: meter.f64_histogram("icegate_query_plan_duration").build(),
            execute_duration: meter.f64_histogram("icegate_query_execute_duration").build(),
            format_duration: meter.f64_histogram("icegate_query_format_duration").build(),
            result_rows: meter.f64_histogram("icegate_query_result_rows").build(),
            result_bytes: meter.f64_histogram("icegate_query_result_bytes").build(),
            session_create_duration: meter.f64_histogram("icegate_query_session_create_duration").build(),
            provider_refresh_duration: meter.f64_histogram("icegate_query_provider_refresh_duration").build(),
            provider_refresh_errors_total: meter.u64_counter("icegate_query_provider_refresh_errors_total").build(),
            errors_total: meter.u64_counter("icegate_query_errors_total").build(),
            active_queries: meter.i64_gauge("icegate_query_active_queries").build(),
        }
    }

    /// Build a metrics recorder using the provided meter.
    pub fn new(meter: &Meter) -> Self {
        let requests_total = meter
            .u64_counter("icegate_query_requests_total")
            .with_description("Total query requests")
            .build();
        let request_duration = meter
            .f64_histogram("icegate_query_request_duration")
            .with_description("End-to-end request duration")
            .with_unit("s")
            .build();
        let parse_duration = meter
            .f64_histogram("icegate_query_parse_duration")
            .with_description("LogQL/PromQL parse duration")
            .with_unit("s")
            .build();
        let plan_duration = meter
            .f64_histogram("icegate_query_plan_duration")
            .with_description("Query planning duration")
            .with_unit("s")
            .build();
        let execute_duration = meter
            .f64_histogram("icegate_query_execute_duration")
            .with_description("DataFusion execute (df.collect) duration")
            .with_unit("s")
            .build();
        let format_duration = meter
            .f64_histogram("icegate_query_format_duration")
            .with_description("Result formatting duration")
            .with_unit("s")
            .build();
        let result_rows = meter
            .f64_histogram("icegate_query_result_rows")
            .with_description("Number of rows in result")
            .build();
        let result_bytes = meter
            .f64_histogram("icegate_query_result_bytes")
            .with_description("Approximate result size")
            .with_unit("By")
            .build();
        let session_create_duration = meter
            .f64_histogram("icegate_query_session_create_duration")
            .with_description("SessionContext creation duration")
            .with_unit("s")
            .build();
        let provider_refresh_duration = meter
            .f64_histogram("icegate_query_provider_refresh_duration")
            .with_description("IcebergCatalogProvider refresh duration")
            .with_unit("s")
            .build();
        let provider_refresh_errors_total = meter
            .u64_counter("icegate_query_provider_refresh_errors_total")
            .with_description("Provider refresh failures")
            .build();
        let errors_total = meter
            .u64_counter("icegate_query_errors_total")
            .with_description("Query errors by phase")
            .build();
        let active_queries = meter
            .i64_gauge("icegate_query_active_queries")
            .with_description("Currently executing queries")
            .build();

        Self {
            enabled: true,
            requests_total,
            request_duration,
            parse_duration,
            plan_duration,
            execute_duration,
            format_duration,
            result_rows,
            result_bytes,
            session_create_duration,
            provider_refresh_duration,
            provider_refresh_errors_total,
            errors_total,
            active_queries,
        }
    }

    // ========================================================================
    // Request-level metrics
    // ========================================================================

    /// Record a completed request.
    pub fn add_request(&self, api: &str, endpoint: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.requests_total.add(
            1,
            &[
                KeyValue::new("api", api.to_string()),
                KeyValue::new("endpoint", endpoint.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record end-to-end request duration.
    pub fn record_request_duration(&self, duration: Duration, api: &str, endpoint: &str) {
        if !self.enabled {
            return;
        }
        self.request_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("api", api.to_string()),
                KeyValue::new("endpoint", endpoint.to_string()),
            ],
        );
    }

    /// Record active queries gauge value.
    pub fn record_active_queries(&self, count: i64, api: &str) {
        if !self.enabled {
            return;
        }
        self.active_queries.record(count, &[KeyValue::new("api", api.to_string())]);
    }

    // ========================================================================
    // Phase-level metrics
    // ========================================================================

    /// Record parse phase duration.
    pub fn record_parse_duration(&self, duration: Duration, api: &str) {
        if !self.enabled {
            return;
        }
        self.parse_duration
            .record(duration.as_secs_f64(), &[KeyValue::new("api", api.to_string())]);
    }

    /// Record plan phase duration.
    pub fn record_plan_duration(&self, duration: Duration, api: &str, plan_type: &str) {
        if !self.enabled {
            return;
        }
        self.plan_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("api", api.to_string()),
                KeyValue::new("plan_type", plan_type.to_string()),
            ],
        );
    }

    /// Record execute phase duration.
    pub fn record_execute_duration(&self, duration: Duration, api: &str, plan_type: &str) {
        if !self.enabled {
            return;
        }
        self.execute_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("api", api.to_string()),
                KeyValue::new("plan_type", plan_type.to_string()),
            ],
        );
    }

    /// Record format phase duration.
    pub fn record_format_duration(&self, duration: Duration, api: &str, result_type: &str) {
        if !self.enabled {
            return;
        }
        self.format_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("api", api.to_string()),
                KeyValue::new("result_type", result_type.to_string()),
            ],
        );
    }

    /// Record result row count.
    #[allow(clippy::cast_precision_loss)]
    pub fn record_result_rows(&self, count: usize, api: &str, plan_type: &str) {
        if !self.enabled {
            return;
        }
        self.result_rows.record(
            count as f64,
            &[
                KeyValue::new("api", api.to_string()),
                KeyValue::new("plan_type", plan_type.to_string()),
            ],
        );
    }

    /// Record approximate result size in bytes.
    #[allow(clippy::cast_precision_loss)]
    pub fn record_result_bytes(&self, bytes: usize, api: &str, plan_type: &str) {
        if !self.enabled {
            return;
        }
        self.result_bytes.record(
            bytes as f64,
            &[
                KeyValue::new("api", api.to_string()),
                KeyValue::new("plan_type", plan_type.to_string()),
            ],
        );
    }

    // ========================================================================
    // Engine-level metrics
    // ========================================================================

    /// Record session context creation duration.
    pub fn record_session_create_duration(&self, duration: Duration) {
        if !self.enabled {
            return;
        }
        self.session_create_duration.record(duration.as_secs_f64(), &[]);
    }

    /// Record provider refresh duration.
    pub fn record_provider_refresh_duration(&self, duration: Duration) {
        if !self.enabled {
            return;
        }
        self.provider_refresh_duration.record(duration.as_secs_f64(), &[]);
    }

    /// Record a provider refresh failure.
    pub fn add_provider_refresh_error(&self) {
        if !self.enabled {
            return;
        }
        self.provider_refresh_errors_total.add(1, &[]);
    }

    /// Record an error by phase.
    pub fn add_error(&self, api: &str, error_type: &str) {
        if !self.enabled {
            return;
        }
        self.errors_total.add(
            1,
            &[
                KeyValue::new("api", api.to_string()),
                KeyValue::new("error_type", error_type.to_string()),
            ],
        );
    }
}

/// Helper that tracks timing for a single query request.
///
/// Increments the active-queries gauge on creation and decrements on drop,
/// ensuring the gauge stays consistent even when errors cause early returns.
pub struct QueryRequestRecorder<'a> {
    metrics: &'a QueryMetrics,
    api: &'a str,
    endpoint: &'a str,
    request_start: Instant,
    finished: bool,
}

impl<'a> QueryRequestRecorder<'a> {
    /// Create a new recorder for a single query request.
    ///
    /// Increments the active-queries gauge immediately.
    pub fn new(metrics: &'a QueryMetrics, api: &'a str, endpoint: &'a str) -> Self {
        metrics.record_active_queries(1, api);
        Self {
            metrics,
            api,
            endpoint,
            request_start: Instant::now(),
            finished: false,
        }
    }

    /// Finish the request with the given status. Records request count and
    /// duration, and decrements the active-queries gauge.
    pub fn finish(&mut self, status: &str) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.metrics.add_request(self.api, self.endpoint, status);
        self.metrics
            .record_request_duration(self.request_start.elapsed(), self.api, self.endpoint);
        self.metrics.record_active_queries(-1, self.api);
    }
}

impl Drop for QueryRequestRecorder<'_> {
    fn drop(&mut self) {
        // If the caller forgot to call finish (e.g. early return on error),
        // record the request as an error to keep the gauge consistent.
        if !self.finished {
            self.finish("error");
        }
    }
}
