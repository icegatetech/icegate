//! Prometheus metrics for the query service.
//!
//! Provides a `QueryMetrics` struct that records OpenTelemetry metrics for
//! every phase of the query request lifecycle: HTTP handling, LogQL parsing,
//! query planning, DataFusion execution, and response formatting.

use std::time::{Duration, Instant};

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, MeterProvider as _, UpDownCounter},
};
use opentelemetry_sdk::metrics::SdkMeterProvider;

/// Histogram bucket boundaries (in seconds) for fast sub-phases like parse,
/// plan, and format, which typically complete in low milliseconds.
const FAST_DURATION_BOUNDARIES: &[f64] = &[0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0];

/// Histogram bucket boundaries (in seconds) for end-to-end and I/O-bound
/// durations like request, execute, and session creation.
const DURATION_BOUNDARIES: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Histogram bucket boundaries (in bytes) for byte-level scan metrics.
/// Covers from 512 B up to ~100 MB in roughly exponential steps.
const BYTES_BOUNDARIES: &[f64] = &[
    0.0,
    512.0,
    1_024.0,
    10_240.0,
    102_400.0,
    1_024_000.0,
    10_240_000.0,
    102_400_000.0,
];

/// Histogram bucket boundaries for row-count scan metrics.
/// Covers from 0 up to 10 M rows in roughly exponential steps.
const ROWS_BOUNDARIES: &[f64] = &[
    0.0,
    10.0,
    100.0,
    1_000.0,
    10_000.0,
    100_000.0,
    1_000_000.0,
    10_000_000.0,
];

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
    errors_total: Counter<u64>,
    active_queries: UpDownCounter<i64>,

    // Per-source scan metrics
    wal_scan_rows: Histogram<f64>,
    wal_scan_bytes: Histogram<f64>,
    wal_scan_compressed_bytes: Histogram<f64>,
    iceberg_scan_rows: Histogram<f64>,
    iceberg_scan_bytes: Histogram<f64>,
    iceberg_scan_compressed_bytes: Histogram<f64>,
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
            errors_total: meter.u64_counter("icegate_query_errors_total").build(),
            active_queries: meter.i64_up_down_counter("icegate_query_active_queries").build(),
            wal_scan_rows: meter.f64_histogram("icegate_query_wal_scan_rows").build(),
            wal_scan_bytes: meter.f64_histogram("icegate_query_wal_scan_bytes").build(),
            wal_scan_compressed_bytes: meter.f64_histogram("icegate_query_wal_scan_compressed_bytes").build(),
            iceberg_scan_rows: meter.f64_histogram("icegate_query_iceberg_scan_rows").build(),
            iceberg_scan_bytes: meter.f64_histogram("icegate_query_iceberg_scan_bytes").build(),
            iceberg_scan_compressed_bytes: meter.f64_histogram("icegate_query_iceberg_scan_compressed_bytes").build(),
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
            .with_boundaries(DURATION_BOUNDARIES.to_vec())
            .build();
        let parse_duration = meter
            .f64_histogram("icegate_query_parse_duration")
            .with_description("LogQL/PromQL parse duration")
            .with_unit("s")
            .with_boundaries(FAST_DURATION_BOUNDARIES.to_vec())
            .build();
        let plan_duration = meter
            .f64_histogram("icegate_query_plan_duration")
            .with_description("Query planning duration")
            .with_unit("s")
            .with_boundaries(FAST_DURATION_BOUNDARIES.to_vec())
            .build();
        let execute_duration = meter
            .f64_histogram("icegate_query_execute_duration")
            .with_description("DataFusion execute (df.collect) duration")
            .with_unit("s")
            .with_boundaries(DURATION_BOUNDARIES.to_vec())
            .build();
        let format_duration = meter
            .f64_histogram("icegate_query_format_duration")
            .with_description("Result formatting duration")
            .with_unit("s")
            .with_boundaries(FAST_DURATION_BOUNDARIES.to_vec())
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
            .with_boundaries(FAST_DURATION_BOUNDARIES.to_vec())
            .build();
        let errors_total = meter
            .u64_counter("icegate_query_errors_total")
            .with_description("Query errors by phase")
            .build();
        let active_queries = meter
            .i64_up_down_counter("icegate_query_active_queries")
            .with_description("Currently executing queries")
            .build();

        let wal_scan_rows = meter
            .f64_histogram("icegate_query_wal_scan_rows")
            .with_description("Rows read from WAL per query")
            .with_boundaries(ROWS_BOUNDARIES.to_vec())
            .build();
        let wal_scan_bytes = meter
            .f64_histogram("icegate_query_wal_scan_bytes")
            .with_description("Decompressed bytes from WAL per query")
            .with_unit("By")
            .with_boundaries(BYTES_BOUNDARIES.to_vec())
            .build();
        let wal_scan_compressed_bytes = meter
            .f64_histogram("icegate_query_wal_scan_compressed_bytes")
            .with_description("Compressed bytes scanned from WAL Parquet files per query")
            .with_unit("By")
            .with_boundaries(BYTES_BOUNDARIES.to_vec())
            .build();
        let iceberg_scan_rows = meter
            .f64_histogram("icegate_query_iceberg_scan_rows")
            .with_description("Rows read from Iceberg per query")
            .with_boundaries(ROWS_BOUNDARIES.to_vec())
            .build();
        let iceberg_scan_bytes = meter
            .f64_histogram("icegate_query_iceberg_scan_bytes")
            .with_description("Decompressed bytes from Iceberg per query")
            .with_unit("By")
            .with_boundaries(BYTES_BOUNDARIES.to_vec())
            .build();
        let iceberg_scan_compressed_bytes = meter
            .f64_histogram("icegate_query_iceberg_scan_compressed_bytes")
            .with_description("Compressed file sizes from Iceberg manifest per query")
            .with_unit("By")
            .with_boundaries(BYTES_BOUNDARIES.to_vec())
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
            errors_total,
            active_queries,
            wal_scan_rows,
            wal_scan_bytes,
            wal_scan_compressed_bytes,
            iceberg_scan_rows,
            iceberg_scan_bytes,
            iceberg_scan_compressed_bytes,
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

    /// Adjust the active queries counter by `delta` (+1 to increment, -1 to decrement).
    pub fn add_active_queries(&self, delta: i64, api: &str) {
        if !self.enabled {
            return;
        }
        self.active_queries.add(delta, &[KeyValue::new("api", api.to_string())]);
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

    /// Record per-source scan metrics extracted from the physical plan tree.
    #[allow(clippy::cast_precision_loss)]
    pub fn record_source_metrics(&self, source: &crate::engine::SourceMetrics, api: &str) {
        if !self.enabled {
            return;
        }
        let attrs = &[KeyValue::new("api", api.to_string())];
        self.wal_scan_rows.record(source.wal_rows as f64, attrs);
        self.wal_scan_bytes.record(source.wal_bytes as f64, attrs);
        self.wal_scan_compressed_bytes.record(source.wal_compressed_bytes as f64, attrs);
        self.iceberg_scan_rows.record(source.iceberg_rows as f64, attrs);
        self.iceberg_scan_bytes.record(source.iceberg_bytes as f64, attrs);
        self.iceberg_scan_compressed_bytes
            .record(source.iceberg_compressed_bytes as f64, attrs);
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
        metrics.add_active_queries(1, api);
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
        self.metrics.add_active_queries(-1, self.api);
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
