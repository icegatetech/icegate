//! OpenTelemetry tracing configuration and utilities.
//!
//! Provides shared tracing configuration and initialization for IceGate services.
//! Uses OTLP exporter with tonic to send traces to Jaeger or other OTLP-compatible backends.

use std::collections::HashMap;
use std::fmt;

use chrono::{SecondsFormat, Utc};
use opentelemetry::global;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_resource_detectors::{
    HostResourceDetector, K8sResourceDetector, OsResourceDetector, ProcessResourceDetector,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::resource::{EnvResourceDetector, SdkProvidedResourceDetector, TelemetryResourceDetector};
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, SdkTracerProvider};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_opentelemetry::{OpenTelemetrySpanExt, OtelData};
use tracing_subscriber::fmt::FormattedFields;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, JsonFields, Writer};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::error::{CommonError, Result};

/// `OpenTelemetry` tracing configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Whether tracing is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// OTLP endpoint for trace export (e.g., `http://jaeger:4317`)
    /// Falls back to `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable
    #[serde(default)]
    pub otlp_endpoint: Option<String>,

    /// Trace sampling ratio (0.0 to 1.0)
    /// - 1.0 = 100% sampling (all traces)
    /// - 0.0 = 0% sampling (no traces)
    /// - 0.1 = 10% sampling
    #[serde(default = "default_sample_ratio")]
    pub sample_ratio: f64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            otlp_endpoint: None,
            sample_ratio: default_sample_ratio(),
        }
    }
}

const fn default_enabled() -> bool {
    true
}

const fn default_sample_ratio() -> f64 {
    1.0
}

impl TracingConfig {
    /// Validate the tracing configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The sample ratio is not between 0.0 and 1.0
    /// - Tracing is enabled but no OTLP endpoint is configured
    pub fn validate(&self) -> Result<()> {
        if self.sample_ratio < 0.0 || self.sample_ratio > 1.0 {
            return Err(CommonError::Config(format!(
                "Tracing sample_ratio must be between 0.0 and 1.0, got {}",
                self.sample_ratio
            )));
        }

        if self.enabled && self.otlp_endpoint.is_none() && std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
            return Err(CommonError::Config(
                "Tracing is enabled but no OTLP endpoint is configured. \
                 Set tracing.otlp_endpoint in config or OTEL_EXPORTER_OTLP_ENDPOINT environment variable"
                    .to_string(),
            ));
        }

        Ok(())
    }

    /// Get the OTLP endpoint, falling back to environment variable.
    fn get_otlp_endpoint(&self) -> Option<String> {
        self.otlp_endpoint
            .clone()
            .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
    }
}

/// RAII guard for tracing initialization.
///
/// Ensures proper shutdown of the tracer provider when dropped.
/// Keep this guard alive for the lifetime of the application.
pub struct TracingGuard {
    provider: Option<SdkTracerProvider>,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            if let Err(e) = provider.shutdown() {
                tracing::error!("Failed to shutdown tracer provider: {e}");
            }
        }
    }
}

/// JSON event formatter that injects the active `OTel` `trace_id` /
/// `span_id` alongside the existing `tracing-subscriber` JSON shape.
///
/// The `OTel` collector's `parse-json-body` operator pairs with this
/// formatter via an embedded `trace:` block — see
/// `config/kustomize/base/otel-collector/configmap.yaml`. When a valid
/// `OTel` context is active on `tracing::Span::current()`, two extra
/// top-level keys (`trace_id`, `span_id`) appear on the JSON line; the
/// collector lifts them straight into the OTLP log record's dedicated
/// trace context fields, completing the trace ↔ logs correlation
/// surfaced in Grafana via `tracesToLogsV2` and `derivedFields` (see
/// `config/docker/grafana/provisioning/datasources/datasources.yaml`).
///
/// Logs emitted outside a tracked span (startup, background tasks
/// without spans, the disabled-tracing path) keep the same JSON shape
/// as before; the new keys are simply omitted.
///
/// Output schema (preserved from the previous
/// `tracing_subscriber::fmt::format::Json` formatter so downstream
/// parsing stays unchanged):
/// `timestamp`, `level`, `fields`, `target`, `span` (`name` plus the
/// span's instance fields rendered via `JsonFields`), `threadName`,
/// `threadId`. New optional keys: `trace_id` (32-hex), `span_id`
/// (16-hex).
#[derive(Debug, Default, Clone, Copy)]
struct TraceContextJsonFormatter;

impl<S, N> FormatEvent<S, N> for TraceContextJsonFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut obj = Map::with_capacity(8);
        let metadata = event.metadata();

        // RFC3339 with nanosecond precision matches what
        // `tracing_subscriber::fmt::format::Json` emitted previously,
        // which the OTel collector's `parser-containerd` /
        // `parser-crio` chain expects to consume downstream.
        obj.insert(
            "timestamp".into(),
            Value::String(Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)),
        );
        obj.insert("level".into(), Value::String(metadata.level().to_string()));

        // Event fields (`fields.message` plus any
        // `tracing::info!(key = value, ...)` k/v pairs).
        let mut visitor = JsonValueVisitor::default();
        event.record(&mut visitor);
        obj.insert("fields".into(), Value::Object(visitor.into_map()));

        obj.insert("target".into(), Value::String(metadata.target().to_string()));

        // Current span — name + instance fields rendered via the
        // `JsonFields` formatter the layer is configured with. We
        // mirror the previous `with_span_list(false)` behavior by
        // surfacing only the immediate span, not the ancestor chain.
        // While here we also extract OTel `trace_id` / `span_id` from
        // the span's `OtelData` extension that the
        // `tracing_opentelemetry` layer attaches in `on_new_span`.
        // Reading the extension via `ctx.lookup_current()` (rather
        // than `Span::current().context()`) is required: inside
        // `format_event` the active dispatcher is a no-op subscriber
        // installed by `fmt::Layer` to prevent log recursion, so the
        // dispatch-based `OpenTelemetrySpanExt::context()` returns an
        // invalid SpanContext. The registry-based lookup uses the
        // real subscriber and works correctly.
        if let Some(span) = ctx.lookup_current() {
            let mut span_obj = Map::new();
            span_obj.insert("name".into(), Value::String(span.metadata().name().to_string()));
            // Scope the `extensions` lock guard so it drops before
            // the final `obj.insert(...)` (clippy::significant_drop_tightening).
            {
                let extensions = span.extensions();
                if let Some(formatted) = extensions.get::<FormattedFields<N>>() {
                    // `JsonFields` renders span instance fields as a
                    // JSON object literal like `{"k":"v"}`. Parse and
                    // merge so downstream collectors (which previously
                    // walked `attributes.span.<key>`) keep working.
                    if let Ok(Value::Object(span_fields)) = serde_json::from_str::<Value>(formatted.fields.as_str()) {
                        for (k, v) in span_fields {
                            span_obj.insert(k, v);
                        }
                    }
                }
                if let Some(otel_data) = extensions.get::<OtelData>() {
                    if let (Some(trace_id), Some(span_id)) = (otel_data.trace_id(), otel_data.span_id()) {
                        obj.insert("trace_id".into(), Value::String(trace_id.to_string()));
                        obj.insert("span_id".into(), Value::String(span_id.to_string()));
                    }
                }
            }
            obj.insert("span".into(), Value::Object(span_obj));
        }

        // Thread info — preserves the previous `with_thread_ids(true)`
        // / `with_thread_names(true)` configuration. Anonymous threads
        // (e.g. some `tokio::spawn_blocking` workers) report no name,
        // matching the previous formatter's omission.
        let thread = std::thread::current();
        if let Some(name) = thread.name() {
            obj.insert("threadName".into(), Value::String(name.to_string()));
        }
        obj.insert("threadId".into(), Value::String(format!("{:?}", thread.id())));

        // One newline-terminated record per event so log shippers
        // framing on `\n` (filelog receiver, fluentd) parse cleanly.
        let line = serde_json::to_string(&Value::Object(obj)).map_err(|_| fmt::Error)?;
        writeln!(writer, "{line}")
    }
}

/// `tracing::field::Visit` impl that collects event fields into a
/// `serde_json::Map`. Numeric/boolean values keep their JSON type;
/// arbitrary `Debug` values fall back to their `format!("{:?}")`
/// rendering, matching what `tracing_subscriber::fmt::format::Json`
/// did previously.
#[derive(Default)]
struct JsonValueVisitor {
    map: Map<String, Value>,
}

impl JsonValueVisitor {
    fn into_map(self) -> Map<String, Value> {
        self.map
    }
}

impl Visit for JsonValueVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.map.insert(field.name().into(), Value::String(value.into()));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.map.insert(field.name().into(), Value::Number(value.into()));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.map.insert(field.name().into(), Value::Number(value.into()));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.map.insert(field.name().into(), Value::Bool(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        // JSON does not represent NaN / infinity; fall back to Null.
        let v = Number::from_f64(value).map_or(Value::Null, Value::Number);
        self.map.insert(field.name().into(), v);
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.map.insert(field.name().into(), Value::String(format!("{value:?}")));
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.map.insert(field.name().into(), Value::String(value.to_string()));
    }
}

/// Initialize `OpenTelemetry` tracing with the given configuration.
///
/// Sets up an OTLP exporter with tonic, configures sampling based on the sample ratio,
/// and initializes the tracing subscriber with an `OpenTelemetry` layer.
///
/// # Arguments
///
/// * `config` - Tracing configuration
///
/// # Returns
///
/// Returns a `TracingGuard` that must be kept alive for the lifetime of the application.
/// When the guard is dropped, it will flush any remaining traces and shut down the tracer provider.
///
/// # Errors
///
/// Returns an error if:
/// - Tracing is enabled but configuration is invalid
/// - Failed to initialize the OTLP exporter
/// - Failed to build the tracer provider
///
/// # Examples
///
/// ```no_run
/// use icegate_common::{TracingConfig, init_tracing};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = TracingConfig {
///     enabled: true,
///     otlp_endpoint: Some("http://jaeger:4317".to_string()),
///     sample_ratio: 1.0,
/// };
///
/// let _guard = init_tracing(&config)?;
///
/// // Use tracing macros
/// tracing::info!("Application started");
///
/// // Keep _guard alive until application shutdown
/// # Ok(())
/// # }
/// ```
pub fn init_tracing(config: &TracingConfig) -> Result<TracingGuard> {
    // If tracing is disabled, just initialize basic logging without OpenTelemetry
    if !config.enabled {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(TraceContextJsonFormatter)
                    .fmt_fields(JsonFields::new()),
            )
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
            )
            .try_init()
            .ok();

        return Ok(TracingGuard { provider: None });
    }

    // Get OTLP endpoint
    let endpoint = config
        .get_otlp_endpoint()
        .ok_or_else(|| CommonError::Config("No OTLP endpoint configured for tracing".to_string()))?;

    // Configure sampler based on sample ratio
    let sampler = if config.sample_ratio >= 1.0 {
        Sampler::AlwaysOn
    } else if config.sample_ratio <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(config.sample_ratio)
    };

    // Create OTLP exporter
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| CommonError::Config(format!("Failed to create OTLP exporter: {e}")))?;

    // Build tracer provider with batch exporter
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(
            Resource::builder_empty()
                .with_detectors(&[
                    Box::new(OsResourceDetector),
                    Box::new(K8sResourceDetector),
                    Box::new(HostResourceDetector::default()),
                    Box::new(ProcessResourceDetector),
                    Box::new(SdkProvidedResourceDetector),
                    Box::new(TelemetryResourceDetector),
                    Box::new(EnvResourceDetector::new()),
                ])
                .build(),
        )
        .build();

    // Set global tracer provider
    global::set_tracer_provider(tracer_provider.clone());
    let tracer = global::tracer("main");

    // Initialize tracing subscriber with OpenTelemetry layer.
    //
    // Layer order: fmt (writes events) → EnvFilter (filters events) →
    // tracing_opentelemetry (mirrors spans into the OTel SDK and
    // attaches the OTel SpanContext to each tracing span via
    // extensions). The fmt layer's `TraceContextJsonFormatter` reads
    // those extensions back at event time via
    // `tracing::Span::current().context()`, so each log line carries
    // the W3C `trace_id` / `span_id` of the span the event was
    // emitted within. The OTel collector's `parse-json-body`
    // operator's embedded `trace:` block (see
    // `config/kustomize/base/otel-collector/configmap.yaml`) then
    // lifts those JSON fields into the OTLP log record's dedicated
    // trace context fields, completing the trace ↔ logs correlation.
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .event_format(TraceContextJsonFormatter)
                .fmt_fields(JsonFields::new()),
        )
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
        .ok();

    Ok(TracingGuard {
        provider: Some(tracer_provider),
    })
}

/// Extracts W3C traceparent from the current tracing span.
///
/// Returns a traceparent header value (format: `{version}-{trace-id}-{parent-id}-{trace-flags}`)
/// that can be used to propagate trace context across async boundaries.
///
/// # Returns
///
/// Returns `Some(String)` with the traceparent header value if a valid trace context exists,
/// or `None` if no valid trace context is available.
///
/// # Examples
///
/// ```no_run
/// use icegate_common::extract_current_trace_context;
///
/// # async fn example() {
/// let trace_context = extract_current_trace_context();
/// if let Some(tc) = trace_context {
///     // Store or propagate trace context
///     println!("Trace context: {}", tc);
/// }
/// # }
/// ```
#[must_use]
pub fn extract_current_trace_context() -> Option<String> {
    let cx = tracing::Span::current().context();
    let mut headers = HashMap::new();
    let propagator = TraceContextPropagator::new();
    propagator.inject_context(&cx, &mut headers);
    headers.get("traceparent").map(ToString::to_string)
}

/// Converts a W3C traceparent string back to an `OpenTelemetry` `Context`.
///
/// This is the inverse of `extract_current_trace_context()`. Falls back to
/// current context if parsing fails.
///
/// # Arguments
///
/// * `traceparent` - W3C traceparent header value in format `{version}-{trace-id}-{parent-id}-{trace-flags}`
///
/// # Returns
///
/// Returns an `OpenTelemetry` `Context` with the parsed trace context.
/// If parsing fails, returns the current context.
///
/// # Examples
///
/// ```no_run
/// use icegate_common::traceparent_to_context;
///
/// # async fn example() {
/// let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
/// let context = traceparent_to_context(traceparent);
/// // Use context as parent for new spans
/// # }
/// ```
#[must_use]
pub fn traceparent_to_context(traceparent: &str) -> opentelemetry::Context {
    let mut headers = HashMap::new();
    headers.insert("traceparent".to_string(), traceparent.to_string());
    let propagator = TraceContextPropagator::new();
    propagator.extract(&headers)
}

/// Adds a span link to the current span from a W3C traceparent string.
///
/// Returns `true` if the link was added successfully, `false` if the
/// traceparent was invalid.
///
/// # Arguments
///
/// * `traceparent` - W3C traceparent header value
///
/// # Returns
///
/// Returns `true` if the span link was successfully added (traceparent is valid),
/// or `false` if the traceparent was invalid.
///
/// # Examples
///
/// ```no_run
/// use icegate_common::add_span_link;
///
/// # async fn example() {
/// let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
/// if add_span_link(traceparent) {
///     println!("Span link added successfully");
/// }
/// # }
/// ```
pub fn add_span_link(traceparent: &str) -> bool {
    let parent_cx = traceparent_to_context(traceparent);
    let flush_span_context = parent_cx.span().span_context().clone();
    if flush_span_context.is_valid() {
        tracing::Span::current().add_link(flush_span_context);
        true
    } else {
        false
    }
}

/// Adds multiple span links to the current span from W3C traceparent strings.
///
/// Returns the number of links successfully added.
///
/// # Arguments
///
/// * `traceparents` - Iterator of W3C traceparent header values
///
/// # Returns
///
/// Returns the count of successfully added span links.
///
/// # Examples
///
/// ```no_run
/// use icegate_common::add_span_links;
///
/// # async fn example() {
/// let traceparents = vec![
///     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
///     "00-5cf92f3577b34da6a3ce929d0e0e4737-00f067aa0ba902b8-01",
/// ];
/// let count = add_span_links(traceparents);
/// println!("Added {} span links", count);
/// # }
/// ```
pub fn add_span_links<'a, I>(traceparents: I) -> usize
where
    I: IntoIterator<Item = &'a str>,
{
    let mut count = 0;
    for traceparent in traceparents {
        if add_span_link(traceparent) {
            count += 1;
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use serde_json::Value;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    /// In-memory `MakeWriter` so tests can capture and parse the
    /// formatter's JSON output without going through stdout.
    #[derive(Clone, Default)]
    struct BufferWriter(Arc<Mutex<Vec<u8>>>);

    impl BufferWriter {
        fn lines(&self) -> Vec<Value> {
            let buf = self.0.lock().expect("buffer mutex");
            std::str::from_utf8(&buf)
                .expect("utf8")
                .lines()
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_str::<Value>(line).expect("each captured line is valid json"))
                .collect()
        }
    }

    impl<'a> MakeWriter<'a> for BufferWriter {
        type Writer = BufferWriterGuard;

        fn make_writer(&'a self) -> Self::Writer {
            BufferWriterGuard(Arc::clone(&self.0))
        }
    }

    struct BufferWriterGuard(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for BufferWriterGuard {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("buffer mutex").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Sanity check: the always-present keys (`timestamp`, `level`,
    /// `target`, `fields`, `threadId`) survive the rewrite, and a log
    /// outside any span has neither a `span` object nor any trace
    /// context keys — preserves shape that downstream collectors
    /// (`json_parser` → `set-severity` chain) consume today.
    #[test]
    fn formatter_emits_required_keys_for_event_without_span() {
        let writer = BufferWriter::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .event_format(TraceContextJsonFormatter)
                .fmt_fields(JsonFields::new())
                .with_writer(writer.clone()),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("hello");
        });

        let lines = writer.lines();
        assert_eq!(lines.len(), 1, "exactly one event captured: {lines:?}");
        let line = &lines[0];
        assert!(line.get("timestamp").and_then(Value::as_str).is_some());
        assert_eq!(line.get("level").and_then(Value::as_str), Some("INFO"));
        assert_eq!(line.get("target").and_then(Value::as_str), Some(module_path!()));
        assert_eq!(line.pointer("/fields/message").and_then(Value::as_str), Some("hello"));
        assert!(line.get("threadId").and_then(Value::as_str).is_some());
        assert!(line.get("span").is_none(), "no span entered → no `span` key: {line:?}");
        assert!(line.get("trace_id").is_none());
        assert!(line.get("span_id").is_none());
    }

    /// Span entered, but no `tracing_opentelemetry` layer attached.
    /// The current span carries no valid `OTel` `SpanContext`, so the
    /// formatter must omit `trace_id` / `span_id` while still emitting
    /// the `span` object (matching `with_span_list(false)` behavior).
    /// Span instance fields (`topic = "logs"` here) must round-trip
    /// through `JsonFields` into the `span` object so dashboards
    /// keying on `attributes.span.<key>` keep working.
    #[test]
    fn formatter_omits_trace_id_when_no_otel_layer() {
        let writer = BufferWriter::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .event_format(TraceContextJsonFormatter)
                .fmt_fields(JsonFields::new())
                .with_writer(writer.clone()),
        );

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("worker", topic = "logs");
            let _entered = span.enter();
            tracing::info!("flushed");
        });

        let lines = writer.lines();
        let line = &lines[0];
        assert_eq!(line.pointer("/span/name").and_then(Value::as_str), Some("worker"));
        assert_eq!(line.pointer("/span/topic").and_then(Value::as_str), Some("logs"));
        assert!(
            line.get("trace_id").is_none(),
            "no otel layer → no valid SpanContext → no trace_id: {line:?}"
        );
        assert!(line.get("span_id").is_none());
    }

    /// With a real `tracing_opentelemetry` layer wired to an SDK
    /// tracer provider, every event emitted inside a span carries the
    /// span's `trace_id` (32 hex chars) and `span_id` (16 hex chars)
    /// — the actual contract the `OTel` collector's embedded `trace:`
    /// block consumes. This is the test that locks the trace ↔ logs
    /// correlation pipeline end-to-end at the formatter boundary.
    #[test]
    fn formatter_emits_trace_id_when_otel_layer_attaches_valid_context() {
        let writer = BufferWriter::default();
        // Tracer provider with no exporter — generates real OTel IDs
        // via `RandomIdGenerator` but discards exported spans.
        // `Sampler::AlwaysOn` is explicit so the test does not depend
        // on the SDK's default sampler heuristics.
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_id_generator(RandomIdGenerator::default())
            .build();
        let tracer = provider.tracer("formatter-test");
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format(TraceContextJsonFormatter)
                    .fmt_fields(JsonFields::new())
                    .with_writer(writer.clone()),
            )
            .with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("request");
            let _entered = span.enter();
            tracing::info!("handled");
        });

        let lines = writer.lines();
        let line = &lines[0];
        let trace_id = line.get("trace_id").and_then(Value::as_str).expect("trace_id present");
        let span_id = line.get("span_id").and_then(Value::as_str).expect("span_id present");
        assert_eq!(trace_id.len(), 32, "32 hex chars: {trace_id}");
        assert_eq!(span_id.len(), 16, "16 hex chars: {span_id}");
        assert!(trace_id.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(span_id.chars().all(|c| c.is_ascii_hexdigit()));
        // Reject the all-zero sentinel that signals an invalid
        // SpanContext — if we ever emit those, it means the formatter
        // forgot to gate on `is_valid()`.
        assert_ne!(trace_id, "0".repeat(32));
        assert_ne!(span_id, "0".repeat(16));
    }
}
