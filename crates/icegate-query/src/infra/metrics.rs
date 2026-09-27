//! Prometheus metrics for the query service.
//!
//! Provides a `QueryMetrics` struct that records OpenTelemetry metrics for
//! every phase of the query request lifecycle: HTTP handling, LogQL parsing,
//! query planning, DataFusion execution, and response formatting.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use icegate_common::TenantRejectionRecorder;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, MeterProvider as _, UpDownCounter},
};
use opentelemetry_sdk::metrics::SdkMeterProvider;

/// The `protocol` label of the Flight SQL read surface.
///
/// The three constants below are the single source of that label: all three
/// surfaces resolve the tenant through the same policy, and a label spelled at
/// the call site would drift from the one the dashboards are keyed on.
pub const PROTOCOL_FLIGHT_SQL: &str = "flight_sql";
/// The `protocol` label of the Loki read surface.
pub const PROTOCOL_LOKI: &str = "loki";
/// The `protocol` label of the Tempo read surface.
pub const PROTOCOL_TEMPO: &str = "tempo";

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
    tenant_rejections_total: Counter<u64>,

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
            requests_total: meter.u64_counter("icegate_query_requests").build(),
            request_duration: meter.f64_histogram("icegate_query_request_duration").build(),
            parse_duration: meter.f64_histogram("icegate_query_parse_duration").build(),
            plan_duration: meter.f64_histogram("icegate_query_plan_duration").build(),
            execute_duration: meter.f64_histogram("icegate_query_execute_duration").build(),
            format_duration: meter.f64_histogram("icegate_query_format_duration").build(),
            result_rows: meter.f64_histogram("icegate_query_result_rows").build(),
            result_bytes: meter.f64_histogram("icegate_query_result_bytes").build(),
            session_create_duration: meter.f64_histogram("icegate_query_session_create_duration").build(),
            errors_total: meter.u64_counter("icegate_query_errors").build(),
            active_queries: meter.i64_up_down_counter("icegate_query_active_queries").build(),
            tenant_rejections_total: meter.u64_counter("icegate_query_tenant_rejections").build(),
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
            .u64_counter("icegate_query_requests")
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
            .build();
        let session_create_duration = meter
            .f64_histogram("icegate_query_session_create_duration")
            .with_description("SessionContext creation duration")
            .with_unit("s")
            .with_boundaries(FAST_DURATION_BOUNDARIES.to_vec())
            .build();
        let errors_total = meter
            .u64_counter("icegate_query_errors")
            .with_description("Query errors by phase")
            .build();
        let active_queries = meter
            .i64_up_down_counter("icegate_query_active_queries")
            .with_description("Currently executing queries")
            .build();
        let tenant_rejections_total = meter
            .u64_counter("icegate_query_tenant_rejections")
            .with_description("Requests refused because they carried no usable tenant")
            .build();

        let wal_scan_rows = meter
            .f64_histogram("icegate_query_wal_scan_rows")
            .with_description("Rows read from WAL per query")
            .with_boundaries(ROWS_BOUNDARIES.to_vec())
            .build();
        let wal_scan_bytes = meter
            .f64_histogram("icegate_query_wal_scan_bytes")
            .with_description("Decompressed bytes from WAL per query")
            .with_boundaries(BYTES_BOUNDARIES.to_vec())
            .build();
        let wal_scan_compressed_bytes = meter
            .f64_histogram("icegate_query_wal_scan_compressed_bytes")
            .with_description("Compressed bytes scanned from WAL Parquet files per query")
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
            .with_boundaries(BYTES_BOUNDARIES.to_vec())
            .build();
        let iceberg_scan_compressed_bytes = meter
            .f64_histogram("icegate_query_iceberg_scan_compressed_bytes")
            .with_description("Compressed file sizes from Iceberg manifest per query")
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
            tenant_rejections_total,
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

/// Writes a refused tenant resolution into `icegate_query_tenant_rejections`.
///
/// A newtype over `Arc<QueryMetrics>` because the port demands `Clone` and the
/// routers clone the service — interceptor and recorder included — on every
/// request: a clone of this recorder is one refcount increment, where a clone
/// of [`QueryMetrics`] would bump the refcount of each of its instruments. The
/// port cannot be implemented on `Arc<QueryMetrics>` directly: the orphan rule
/// refuses it, since both `Arc` and the trait are foreign here.
///
/// Stated only as the port, with no inherent method beside it: two definitions
/// of one action on one type resolve by inherent-first and would silently
/// recurse if either call site were written the other way round.
#[derive(Clone)]
pub struct QueryTenantRejectionRecorder(Arc<QueryMetrics>);

impl QueryTenantRejectionRecorder {
    /// Build a recorder writing into `metrics`.
    #[must_use]
    pub const fn new(metrics: Arc<QueryMetrics>) -> Self {
        Self(metrics)
    }
}

impl TenantRejectionRecorder for QueryTenantRejectionRecorder {
    /// `protocol` is one of [`PROTOCOL_FLIGHT_SQL`], [`PROTOCOL_LOKI`],
    /// [`PROTOCOL_TEMPO`]; `reason` comes from
    /// [`TenantRejection::reason`](icegate_common::TenantRejection::reason) and
    /// from nowhere else, since a hand-written string here would drift from the
    /// variants the resolver returns.
    fn add_tenant_rejection(&self, protocol: &str, reason: &str) {
        if !self.0.enabled {
            return;
        }
        self.0.tenant_rejections_total.add(
            1,
            &[
                KeyValue::new("protocol", protocol.to_string()),
                KeyValue::new("reason", reason.to_string()),
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

/// Reading recorded measurements back in a test: a meter provider that exports
/// into memory, and the lookup over what it exported.
///
/// Lives here rather than beside one test module because the routers assert on
/// the same counter this file defines, and how a measurement is found by name
/// and labels should be defined once. Counters export cumulatively and a
/// provider exports again when it shuts down, so the lookup reads the last
/// export that carried the metric rather than adding the exports up.
#[cfg(test)]
pub(crate) mod test_support {
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData},
    };

    /// Meter provider recording into memory, together with the exporter its
    /// measurements land in. They reach the exporter only after `force_flush`.
    pub(crate) fn build_meter_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        (provider, exporter)
    }

    /// Total the counter `metric_name` reached across the data points carrying
    /// **exactly** `expected_labels`; zero when it was never incremented with
    /// them.
    ///
    /// Exact rather than "at least": the label set of this counter is the
    /// contract the dashboards are keyed on, and an extra label is a dimension
    /// change this is what catches.
    pub(crate) fn find_counter_total(
        exporter: &InMemoryMetricExporter,
        metric_name: &str,
        expected_labels: &[(&str, &str)],
    ) -> u64 {
        let mut latest = None;
        for batch in exporter.get_finished_metrics().unwrap_or_default() {
            for scope in batch.scope_metrics() {
                for metric in scope.metrics().filter(|metric| metric.name() == metric_name) {
                    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
                        continue;
                    };
                    let mut export_total = None;
                    for point in sum.data_points() {
                        let labels = metric_labels(&point.attributes().cloned().collect::<Vec<_>>());
                        if labels_match(&labels, expected_labels) {
                            export_total = Some(export_total.unwrap_or(0_u64).saturating_add(point.value()));
                        }
                    }
                    if export_total.is_some() {
                        latest = export_total;
                    }
                }
            }
        }
        latest.unwrap_or(0)
    }

    fn metric_labels(attributes: &[KeyValue]) -> Vec<(String, String)> {
        let mut labels = attributes
            .iter()
            .map(|kv| (kv.key.as_str().to_string(), kv.value.as_str().into_owned()))
            .collect::<Vec<_>>();
        labels.sort();
        labels
    }

    fn labels_match(labels: &[(String, String)], expected: &[(&str, &str)]) -> bool {
        labels.len() == expected.len()
            && expected.iter().all(|(key, value)| {
                labels
                    .iter()
                    .any(|(label_key, label_value)| label_key == key && label_value == value)
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use icegate_common::TenantRejection;
    use opentelemetry::metrics::MeterProvider as _;

    use super::{
        PROTOCOL_FLIGHT_SQL, PROTOCOL_LOKI, PROTOCOL_TEMPO, QueryMetrics, QueryTenantRejectionRecorder,
        TenantRejectionRecorder,
        test_support::{build_meter_provider, find_counter_total},
    };

    /// The label set `icegate_query_tenant_rejections` carries, spelled out:
    /// it is what the dashboards are keyed on, and unlike ingest's counter it
    /// carries no `signal`. Every protocol and every reason is driven, because
    /// a surface passing the wrong constant is invisible from one case. The
    /// expected values are literals rather than the constants, so renaming a
    /// label value fails here instead of silently re-keying the dashboards.
    #[test]
    fn a_refused_request_counts_a_rejection_labelled_by_protocol_and_reason() {
        let (provider, exporter) = build_meter_provider();
        let recorder = QueryTenantRejectionRecorder::new(Arc::new(QueryMetrics::new(
            &provider.meter("test_query_tenant_rejections"),
        )));

        let cases = [
            (PROTOCOL_LOKI, "loki", TenantRejection::Missing, "missing"),
            (PROTOCOL_TEMPO, "tempo", TenantRejection::Invalid, "invalid"),
            (
                PROTOCOL_FLIGHT_SQL,
                "flight_sql",
                TenantRejection::DuplicateHeader,
                "duplicate_header",
            ),
        ];
        for (protocol, _, rejection, _) in cases {
            recorder.add_tenant_rejection(protocol, rejection.reason());
        }

        provider.force_flush().expect("failed to flush metrics");
        for (_, protocol_label, _, reason_label) in cases {
            assert_eq!(
                find_counter_total(
                    &exporter,
                    "icegate_query_tenant_rejections",
                    &[("protocol", protocol_label), ("reason", reason_label)],
                ),
                1,
                "{protocol_label}/{reason_label} must be counted once, under those two labels alone"
            );
        }
    }
}
