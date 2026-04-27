//! `RecordBatch` → Tempo response formatters.
//!
//! Three responsibilities:
//! - [`spans_to_otlp_json`] — convert a list of span batches into the
//!   OTLP `resourceSpans[].scopeSpans[].spans[]` JSON shape (used for
//!   `GET /api/traces/{traceID}` with `Accept: application/json`).
//! - [`spans_to_otlp_proto`] — same rows, encoded as the OTLP `TracesData`
//!   protobuf message. This is what Grafana's Tempo data source expects by
//!   default (`Accept: application/protobuf`).
//! - [`spansets_to_search_response`] — convert a planner output into
//!   the Tempo search JSON shape (`{traces, metrics}`).

use std::collections::BTreeMap;

use datafusion::arrow::array::{
    Array, AsArray, Int32Array, ListArray, MapArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value as AnyVal},
    resource::v1::Resource,
    trace::v1::{
        ResourceSpans, ScopeSpans, Span, Status, TracesData,
        span::{Event, Link},
        status::StatusCode,
    },
};
use prost::Message;
use serde_json::{Map, Value, json};

use crate::tempo::models::{
    AttributeValue, MatchedSpan, MatchedSpanAttribute, SearchMetrics, SearchResponse, SpanSet, TraceSummary,
};

/// Convert spans into OTLP `resourceSpans` JSON.
///
/// Groups spans by their resource attributes and a synthetic scope (currently
/// always the same since the spans schema flattens scope into the attributes
/// map). Output matches what Grafana's Tempo data source expects.
///
/// # Errors
///
/// Returns [`crate::error::QueryError::Internal`] if a column has an
/// unexpected type — should never happen given the spans schema.
pub fn spans_to_otlp_json(batches: &[RecordBatch]) -> crate::error::Result<Value> {
    if batches.is_empty() {
        return Ok(json!({ "resourceSpans": [] }));
    }

    let mut by_service: BTreeMap<String, JsonServiceGroup> = BTreeMap::new();

    for batch in batches {
        let trace_ids = string_col(batch, "trace_id")?;
        let span_ids = string_col(batch, "span_id")?;
        let parent_ids = string_col_opt(batch, "parent_span_id");
        let names = string_col(batch, "name")?;
        let kinds = i32_col_opt(batch, "kind");
        let statuses = i32_col_opt(batch, "status_code");
        let starts = ts_col(batch, "timestamp")?;
        let ends = ts_col_opt(batch, "end_timestamp");
        let resource_attrs = map_col(batch, "resource_attributes")?;
        let span_attrs = map_col(batch, "span_attributes")?;
        let svc_names = string_col_opt(batch, "service_name");
        let events_col = list_col_opt(batch, "events");
        let links_col = list_col_opt(batch, "links");
        let dropped_events = i32_col_opt(batch, "dropped_events_count");
        let dropped_links = i32_col_opt(batch, "dropped_links_count");

        for row in 0..batch.num_rows() {
            let svc = svc_names
                .as_ref()
                .filter(|c| !c.is_null(row))
                .map(|c| c.value(row).to_string())
                .unwrap_or_default();

            let group = by_service.entry(svc.clone()).or_insert_with(|| JsonServiceGroup {
                service: svc,
                resource_attributes: row_attributes(resource_attrs, row),
                spans: Vec::new(),
            });

            let events = events_col.map(|c| json_events_for_row(c, row)).unwrap_or_default();
            let links = links_col.map(|c| json_links_for_row(c, row)).unwrap_or_default();
            let dropped_events_count = dropped_events
                .filter(|c| !c.is_null(row))
                .map_or(0, |c| u32_from_i32(c.value(row)));
            let dropped_links_count = dropped_links
                .filter(|c| !c.is_null(row))
                .map_or(0, |c| u32_from_i32(c.value(row)));

            let span_value = build_span_value(
                trace_ids.value(row),
                span_ids.value(row),
                parent_ids.as_ref().map(|c| c.value(row)),
                names.value(row),
                kinds.as_ref().map(|c| c.value(row)),
                statuses.as_ref().map(|c| c.value(row)),
                starts.value(row),
                ends.as_ref().map(|c| c.value(row)),
                row_attributes(span_attrs, row),
                events,
                dropped_events_count,
                links,
                dropped_links_count,
            );
            group.spans.push(span_value);
        }
    }

    let mut resource_spans = vec![];
    for group in by_service.into_values() {
        let mut attrs = group.resource_attributes;
        if !group.service.is_empty() && !attrs.iter().any(|v| v.get("key") == Some(&json!("service.name"))) {
            attrs.push(json!({"key": "service.name", "value": {"stringValue": group.service}}));
        }
        resource_spans.push(json!({
            "resource": { "attributes": attrs },
            "scopeSpans": [ { "scope": {"name": "icegate"}, "spans": group.spans } ]
        }));
    }

    Ok(json!({ "resourceSpans": resource_spans }))
}

/// Group accumulator for JSON `resourceSpans` construction.
///
/// Mirrors [`ServiceGroup`] but for the JSON formatter: resource attributes
/// and spans are already serialized into `Value` form by the helpers.
struct JsonServiceGroup {
    /// The `service_name` value extracted from the row's top-level column.
    service: String,
    /// Resource-scoped attribute JSON entries from the first row for this service.
    resource_attributes: Vec<Value>,
    /// Span JSON objects for this service, in row order.
    spans: Vec<Value>,
}

#[allow(clippy::too_many_arguments)]
fn build_span_value(
    trace_id: &str,
    span_id: &str,
    parent: Option<&str>,
    name: &str,
    kind: Option<i32>,
    status: Option<i32>,
    start_micros: i64,
    end_micros: Option<i64>,
    attrs: Vec<Value>,
    events: Vec<Value>,
    dropped_events_count: u32,
    links: Vec<Value>,
    dropped_links_count: u32,
) -> Value {
    let mut span = Map::new();
    span.insert("traceId".into(), json!(trace_id));
    span.insert("spanId".into(), json!(span_id));
    if let Some(p) = parent {
        if !p.is_empty() {
            span.insert("parentSpanId".into(), json!(p));
        }
    }
    span.insert("name".into(), json!(name));
    if let Some(k) = kind {
        span.insert("kind".into(), json!(otlp_kind_name(k)));
    }
    if let Some(s) = status {
        span.insert("status".into(), json!({"code": otlp_status_name(s)}));
    }
    span.insert("startTimeUnixNano".into(), json!(micros_to_nanos_str(start_micros)));
    if let Some(e) = end_micros {
        span.insert("endTimeUnixNano".into(), json!(micros_to_nanos_str(e)));
    }
    span.insert("attributes".into(), Value::Array(attrs));
    if !events.is_empty() {
        span.insert("events".into(), Value::Array(events));
    }
    if dropped_events_count > 0 {
        span.insert("droppedEventsCount".into(), json!(dropped_events_count));
    }
    if !links.is_empty() {
        span.insert("links".into(), Value::Array(links));
    }
    if dropped_links_count > 0 {
        span.insert("droppedLinksCount".into(), json!(dropped_links_count));
    }
    Value::Object(span)
}

const fn otlp_kind_name(k: i32) -> &'static str {
    match k {
        1 => "SPAN_KIND_INTERNAL",
        2 => "SPAN_KIND_SERVER",
        3 => "SPAN_KIND_CLIENT",
        4 => "SPAN_KIND_PRODUCER",
        5 => "SPAN_KIND_CONSUMER",
        _ => "SPAN_KIND_UNSPECIFIED",
    }
}

const fn otlp_status_name(s: i32) -> &'static str {
    match s {
        1 => "STATUS_CODE_OK",
        2 => "STATUS_CODE_ERROR",
        _ => "STATUS_CODE_UNSET",
    }
}

fn row_attributes(map_arr: &MapArray, row: usize) -> Vec<Value> {
    let entries = map_arr.value(row);
    let keys = entries.column(0).as_string::<i32>();
    let values = entries.column(1).as_string::<i32>();
    let mut out = Vec::with_capacity(keys.len());
    for i in 0..keys.len() {
        if !values.is_null(i) {
            out.push(json!({"key": keys.value(i), "value": {"stringValue": values.value(i)}}));
        }
    }
    out
}

// =========================================================================
// Helpers
// =========================================================================

fn string_col<'a>(batch: &'a RecordBatch, name: &str) -> crate::error::Result<&'a StringArray> {
    let arr = column(batch, name)?;
    arr.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| crate::error::QueryError::Internal(format!("column {name} not Utf8")))
}

fn string_col_opt<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .schema()
        .column_with_name(name)
        .and_then(|(idx, _)| batch.column(idx).as_any().downcast_ref::<StringArray>())
}

fn i32_col_opt<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int32Array> {
    batch
        .schema()
        .column_with_name(name)
        .and_then(|(idx, _)| batch.column(idx).as_any().downcast_ref::<Int32Array>())
}

fn ts_col<'a>(batch: &'a RecordBatch, name: &str) -> crate::error::Result<&'a TimestampMicrosecondArray> {
    let arr = column(batch, name)?;
    arr.as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| crate::error::QueryError::Internal(format!("column {name} not TimestampMicros")))
}

fn ts_col_opt<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a TimestampMicrosecondArray> {
    batch
        .schema()
        .column_with_name(name)
        .and_then(|(idx, _)| batch.column(idx).as_any().downcast_ref::<TimestampMicrosecondArray>())
}

fn map_col<'a>(batch: &'a RecordBatch, name: &str) -> crate::error::Result<&'a MapArray> {
    let arr = column(batch, name)?;
    arr.as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| crate::error::QueryError::Internal(format!("column {name} not Map")))
}

/// Get an optional `List<Struct>` column (e.g. `events`, `links`).
///
/// Returns `None` when the column is missing or has the wrong Arrow
/// type, mirroring the rest of the `*_opt` helpers in this module.
/// Some test batches (and possibly older writers) omit these columns
/// entirely; treating their absence as "no nested rows" keeps the
/// formatter resilient.
fn list_col_opt<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a ListArray> {
    batch
        .schema()
        .column_with_name(name)
        .and_then(|(idx, _)| batch.column(idx).as_any().downcast_ref::<ListArray>())
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> crate::error::Result<&'a dyn Array> {
    batch
        .schema()
        .column_with_name(name)
        .map(|(idx, _)| batch.column(idx).as_ref())
        .ok_or_else(|| crate::error::QueryError::Internal(format!("missing column {name}")))
}

// =========================================================================
// Search-response formatter
// =========================================================================

/// Build a Tempo search response from raw planner output.
///
/// Groups the returned span rows by `trace_id`. For each trace:
/// - `root_service_name` / `root_trace_name` are the service name and span
///   name of the best representative span in the matched set, picked in
///   this order:
///   1. An actual root span (`parent_span_id` null or empty), preferring
///      the earliest timestamp when several roots are present.
///   2. Otherwise the earliest-timestamped non-root matched span. This
///      fallback exists because `TraceQL` filters often match only a
///      subset of a trace's spans (e.g. `{resource.service.name=foo}`
///      matches only the `foo` spans even when the trace's root lives in
///      a different service); without the fallback such traces would
///      surface with empty service / name in the search-results table.
/// - `start_time_unix_nano` is the minimum `timestamp` (converted to nanos).
/// - `duration_ms` is the span from min `timestamp` to max `end_timestamp`.
///
/// `BTreeMap` is used to keep the output deterministic, which also makes
/// testing easier downstream.
//
// The function exceeds the project's 150-line clippy threshold because the
// nested `BestSpan` and `Acc` structs plus the per-batch / per-row
// accumulation logic genuinely belong together — splitting them into
// separate helpers is on the cleanup list (architecture review #5) but
// would not improve readability today. Suppress the lint locally rather
// than push the cleanup into this PR.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn spansets_to_search_response(batches: &[RecordBatch]) -> SearchResponse {
    /// Best representative span seen so far for a trace. Ordering
    /// prefers (in priority order):
    ///   1. `is_root == true` (a true OTLP root, i.e. null / empty
    ///      `parent_span_id`).
    ///   2. Largest `duration_micros` — a trace's real root
    ///      encompasses every descendant in time, so the span with the
    ///      longest duration at the same `is_root` tier is the best
    ///      "outermost" candidate. This resolves the ambiguous case
    ///      where multiple spans in a trace have `parent_span_id`
    ///      null (e.g. cross-service traces where each service's entry
    ///      point appears rootless in our data) and the earlier
    ///      "earliest timestamp" tiebreak surfaced a short sibling
    ///      instead of the real root.
    ///   3. Smallest `ts_micros` — last-resort tiebreak for spans with
    ///      identical duration.
    #[derive(Clone)]
    struct BestSpan {
        is_root: bool,
        ts_micros: i64,
        duration_micros: i64,
        service: Option<String>,
        name: Option<String>,
    }

    impl BestSpan {
        /// Return `true` if `candidate` should replace `self`.
        const fn should_replace(&self, candidate: &Self) -> bool {
            match (self.is_root, candidate.is_root) {
                (false, true) => return true,  // upgrade non-root → root
                (true, false) => return false, // never downgrade root → non-root
                _ => {}
            }
            // Same `is_root` tier: longest-duration wins, timestamp
            // breaks ties for equal durations.
            if candidate.duration_micros > self.duration_micros {
                true
            } else if candidate.duration_micros < self.duration_micros {
                false
            } else {
                candidate.ts_micros < self.ts_micros
            }
        }
    }

    #[derive(Default)]
    struct Acc {
        best: Option<BestSpan>,
        /// Per-trace span list surfaced under `spanSets`. Order
        /// preserved from the planner so the root sits at index 0
        /// when present.
        spans: Vec<MatchedSpan>,
        /// Minimum `timestamp` seen, in microseconds. `None` until the
        /// first row arrives — using a real Option instead of a
        /// magic-zero sentinel keeps a synthetic / clock-skewed
        /// epoch-zero span (timestamp = 1970-01-01) from being treated
        /// as "uninitialised" and silently dropping the trace's true
        /// start time.
        start_micros: Option<i64>,
        /// Maximum `end_timestamp` seen, in microseconds. `None` until
        /// the first row arrives.
        end_micros: Option<i64>,
    }

    let mut by_trace: BTreeMap<String, Acc> = BTreeMap::new();

    for batch in batches {
        // `trace_id` is required — skip batches missing it.
        let Some(trace_ids) = batch
            .schema()
            .column_with_name("trace_id")
            .map(|(i, _)| batch.column(i).as_string::<i32>())
        else {
            continue;
        };
        let span_ids = batch
            .schema()
            .column_with_name("span_id")
            .map(|(i, _)| batch.column(i).as_string::<i32>());
        let names = batch
            .schema()
            .column_with_name("name")
            .map(|(i, _)| batch.column(i).as_string::<i32>());
        let svcs = batch
            .schema()
            .column_with_name("service_name")
            .map(|(i, _)| batch.column(i).as_string::<i32>());
        let starts = batch
            .schema()
            .column_with_name("timestamp")
            .and_then(|(i, _)| batch.column(i).as_any().downcast_ref::<TimestampMicrosecondArray>());
        let ends = batch
            .schema()
            .column_with_name("end_timestamp")
            .and_then(|(i, _)| batch.column(i).as_any().downcast_ref::<TimestampMicrosecondArray>());
        let parents = batch
            .schema()
            .column_with_name("parent_span_id")
            .map(|(i, _)| batch.column(i).as_string::<i32>());

        for row in 0..batch.num_rows() {
            let tid = trace_ids.value(row).to_string();
            let acc = by_trace.entry(tid).or_default();
            // A span is the root when `parent_span_id` is null or empty.
            // If the `parent_span_id` column is missing entirely, treat
            // every span as a root (best-effort).
            let is_root = parents.as_ref().is_none_or(|c| c.is_null(row) || c.value(row).is_empty());
            // Use the row's `timestamp` for ordering. Missing or zero
            // timestamp sorts last (fallback to `i64::MAX`).
            let ts_micros = starts.map_or(i64::MAX, |c| c.value(row));
            // Treat a null / empty service or name as "no value" so the
            // candidate can still be picked but won't pollute the summary.
            let service = svcs
                .as_ref()
                .filter(|c| !c.is_null(row))
                .map(|c| c.value(row).to_string())
                .filter(|s| !s.is_empty());
            let name = names
                .as_ref()
                .filter(|c| !c.is_null(row))
                .map(|c| c.value(row).to_string())
                .filter(|s| !s.is_empty());

            let end_micros = ends.map_or(ts_micros, |c| c.value(row));
            let duration_micros = (end_micros - ts_micros).max(0);

            // Build the matched-span entry surfaced in `spanSets`. We
            // emit one per row regardless of whether it satisfied the
            // original spanset filter — the planner's stage 2 already
            // returns ALL spans of matched traces (capped by `spss`,
            // root-first), so each row is something the user is likely
            // to want to see in the inline trace preview.
            if let Some(span_ids) = span_ids {
                let span_id = if span_ids.is_null(row) {
                    String::new()
                } else {
                    span_ids.value(row).to_string()
                };
                acc.spans.push(build_matched_span(
                    span_id,
                    ts_micros,
                    duration_micros,
                    service.as_deref(),
                    name.as_deref(),
                ));
            }

            let candidate = BestSpan {
                is_root,
                ts_micros,
                duration_micros,
                service,
                name,
            };
            match &acc.best {
                None => acc.best = Some(candidate),
                Some(current) if current.should_replace(&candidate) => acc.best = Some(candidate),
                _ => {}
            }

            if let Some(s) = starts {
                let t = s.value(row);
                acc.start_micros = Some(acc.start_micros.map_or(t, |cur| cur.min(t)));
            }
            if let Some(e) = ends {
                let t = e.value(row);
                acc.end_micros = Some(acc.end_micros.map_or(t, |cur| cur.max(t)));
            }
        }
    }

    let traces = by_trace
        .into_iter()
        .map(|(tid, acc)| {
            let (root_service_name, root_trace_name) = acc.best.map_or((None, None), |b| (b.service, b.name));
            // Always emit at least one (possibly empty) `SpanSet` —
            // Grafana's inline-trace renderer reduces over `spanSets`
            // unconditionally and crashes on `undefined`. An empty
            // `spans` array is fine; a missing field is not.
            let matched = acc.spans.len();
            let span_sets = vec![SpanSet {
                spans: acc.spans,
                matched,
            }];
            // Resolve the trace start / end. With no observed timestamps
            // (e.g. column missing entirely), fall back to zero so the
            // serialised payload stays well-formed.
            let start_micros = acc.start_micros.unwrap_or(0);
            let end_micros = acc.end_micros.unwrap_or(start_micros);
            TraceSummary {
                trace_id: tid,
                root_service_name,
                root_trace_name,
                // Tempo reports times in nanoseconds. Our Iceberg rows are in
                // microseconds (per `schema::COL_TIMESTAMP`), so scale up.
                start_time_unix_nano: micros_to_nanos_str(start_micros),
                duration_ms: u64::try_from(((end_micros - start_micros) / 1_000).max(0)).unwrap_or(0),
                span_sets,
            }
        })
        .collect();

    SearchResponse {
        traces,
        metrics: SearchMetrics { total_blocks: 0 },
    }
}

/// Build a [`MatchedSpan`] entry surfaced in a trace summary's
/// `spanSets[0].spans`. Surfaces `service.name` and `name` as string-
/// typed attributes when present; callers can still see richer data via
/// the trace-by-id endpoint.
fn build_matched_span(
    span_id: String,
    ts_micros: i64,
    duration_micros: i64,
    service: Option<&str>,
    name: Option<&str>,
) -> MatchedSpan {
    let mut attributes: Vec<MatchedSpanAttribute> = Vec::new();
    if let Some(svc) = service {
        attributes.push(MatchedSpanAttribute {
            key: "service.name".to_string(),
            value: AttributeValue {
                string_value: svc.to_string(),
            },
        });
    }
    if let Some(span_name) = name {
        attributes.push(MatchedSpanAttribute {
            key: "name".to_string(),
            value: AttributeValue {
                string_value: span_name.to_string(),
            },
        });
    }
    MatchedSpan {
        span_id,
        start_time_unix_nano: ts_micros.saturating_mul(1_000).to_string(),
        duration_nanos: duration_micros.saturating_mul(1_000).to_string(),
        attributes,
    }
}

// =========================================================================
// OTLP protobuf formatter (Grafana's default Content-Type)
// =========================================================================

/// Convert spans into OTLP `TracesData` protobuf bytes.
///
/// Grafana's Tempo data source expects protobuf by default; returning JSON
/// makes it fail with `proto: illegal wireType 6`. This encoder groups spans
/// by `service_name` (our proxy for a resource) and by instrumentation scope
/// (just "icegate" since the spans schema doesn't preserve scope metadata),
/// matching what the JSON formatter emits structurally.
///
/// Trace and span IDs are hex-decoded back to raw bytes for the proto
/// `bytes` fields; invalid hex collapses to empty bytes (proto will then
/// carry zero-length IDs, matching how Grafana renders orphaned rows).
///
/// # Errors
///
/// Returns [`crate::error::QueryError::Internal`] if a required column has
/// an unexpected type.
pub fn spans_to_otlp_proto(batches: &[RecordBatch]) -> crate::error::Result<Vec<u8>> {
    let data = spans_to_traces_data(batches)?;
    Ok(data.encode_to_vec())
}

/// Group accumulator for proto `ResourceSpans` construction.
///
/// Holds per-service state so we can merge spans from the same service into
/// one `ResourceSpans` entry while preserving the resource attributes we saw
/// on the first row encountered.
struct ServiceGroup {
    /// The `service_name` value extracted from the row's top-level column.
    service: String,
    /// Resource-scoped OTLP attributes from the first row for this service.
    resource_attributes: Vec<KeyValue>,
    /// Span-scoped OTLP spans for this service, in row order.
    spans: Vec<Span>,
}

/// Build an OTLP [`TracesData`] from spans `RecordBatch`es.
///
/// Exposed as a helper so tests can inspect the structured message without
/// paying for protobuf encoding.
fn spans_to_traces_data(batches: &[RecordBatch]) -> crate::error::Result<TracesData> {
    // Group by service_name so spans from the same service share a
    // ResourceSpans entry. Within each service, we take the resource_attributes
    // from the first row encountered; differences across spans would create
    // extra ResourceSpans which Grafana handles fine.
    let mut by_service: BTreeMap<String, ServiceGroup> = BTreeMap::new();

    for batch in batches {
        let trace_ids = string_col(batch, "trace_id")?;
        let span_ids = string_col(batch, "span_id")?;
        let parent_ids = string_col_opt(batch, "parent_span_id");
        let names = string_col(batch, "name")?;
        let kinds = i32_col_opt(batch, "kind");
        let statuses = i32_col_opt(batch, "status_code");
        let status_msgs = string_col_opt(batch, "status_message");
        let starts = ts_col(batch, "timestamp")?;
        let ends = ts_col_opt(batch, "end_timestamp");
        let resource_attrs_col = map_col(batch, "resource_attributes")?;
        let span_attrs_col = map_col(batch, "span_attributes")?;
        let svcs = string_col_opt(batch, "service_name");
        let events_col = list_col_opt(batch, "events");
        let links_col = list_col_opt(batch, "links");
        let dropped_events = i32_col_opt(batch, "dropped_events_count");
        let dropped_links = i32_col_opt(batch, "dropped_links_count");

        for row in 0..batch.num_rows() {
            let svc = svcs
                .as_ref()
                .filter(|c| !c.is_null(row))
                .map(|c| c.value(row).to_string())
                .unwrap_or_default();

            let group = by_service.entry(svc.clone()).or_insert_with(|| ServiceGroup {
                service: svc,
                resource_attributes: proto_attributes(resource_attrs_col, row),
                spans: Vec::new(),
            });

            let events = events_col.map(|c| proto_events_for_row(c, row)).unwrap_or_default();
            let links = links_col.map(|c| proto_links_for_row(c, row)).unwrap_or_default();
            let dropped_events_count = dropped_events
                .filter(|c| !c.is_null(row))
                .map_or(0, |c| u32_from_i32(c.value(row)));
            let dropped_links_count = dropped_links
                .filter(|c| !c.is_null(row))
                .map_or(0, |c| u32_from_i32(c.value(row)));

            let span = row_to_proto_span(
                trace_ids.value(row),
                span_ids.value(row),
                parent_ids.as_ref().map(|c| c.value(row)),
                names.value(row),
                kinds.as_ref().map(|c| c.value(row)),
                statuses.as_ref().map(|c| c.value(row)),
                status_msgs
                    .as_ref()
                    .and_then(|c| (!c.is_null(row)).then(|| c.value(row).to_string())),
                starts.value(row),
                ends.as_ref().map(|c| c.value(row)),
                proto_attributes(span_attrs_col, row),
                events,
                dropped_events_count,
                links,
                dropped_links_count,
            );
            group.spans.push(span);
        }
    }

    let resource_spans = by_service
        .into_values()
        .map(|group| {
            // Ensure service.name is present as a resource attribute; some
            // resource_attributes maps won't have it but the top-level
            // service_name column always does.
            let mut resource_attributes = group.resource_attributes;
            if !group.service.is_empty() && !resource_attributes.iter().any(|kv| kv.key == "service.name") {
                resource_attributes.push(KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyVal::StringValue(group.service.clone())),
                    }),
                });
            }
            ResourceSpans {
                resource: Some(Resource {
                    attributes: resource_attributes,
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "icegate".to_string(),
                        version: String::new(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    spans: group.spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }
        })
        .collect();

    Ok(TracesData { resource_spans })
}

#[allow(clippy::too_many_arguments)]
fn row_to_proto_span(
    trace_id: &str,
    span_id: &str,
    parent: Option<&str>,
    name: &str,
    kind: Option<i32>,
    status_code: Option<i32>,
    status_message: Option<String>,
    start_micros: i64,
    end_micros: Option<i64>,
    attributes: Vec<KeyValue>,
    events: Vec<Event>,
    dropped_events_count: u32,
    links: Vec<Link>,
    dropped_links_count: u32,
) -> Span {
    Span {
        trace_id: hex::decode(trace_id).unwrap_or_default(),
        span_id: hex::decode(span_id).unwrap_or_default(),
        trace_state: String::new(),
        parent_span_id: parent.and_then(|p| hex::decode(p).ok()).unwrap_or_default(),
        flags: 0,
        name: name.to_string(),
        kind: kind.unwrap_or(0),
        start_time_unix_nano: micros_to_nanos_u64(start_micros),
        end_time_unix_nano: end_micros.map_or(0, micros_to_nanos_u64),
        attributes,
        dropped_attributes_count: 0,
        events,
        dropped_events_count,
        links,
        dropped_links_count,
        status: status_code.map(|c| Status {
            message: status_message.unwrap_or_default(),
            code: status_code_to_proto(c),
        }),
    }
}

fn proto_attributes(map_arr: &MapArray, row: usize) -> Vec<KeyValue> {
    let entries = map_arr.value(row);
    let keys = entries.column(0).as_string::<i32>();
    let values = entries.column(1).as_string::<i32>();
    let mut out = Vec::with_capacity(keys.len());
    for i in 0..keys.len() {
        if values.is_null(i) {
            continue;
        }
        out.push(KeyValue {
            key: keys.value(i).to_string(),
            value: Some(AnyValue {
                value: Some(AnyVal::StringValue(values.value(i).to_string())),
            }),
        });
    }
    out
}

#[allow(clippy::cast_sign_loss)]
const fn micros_to_nanos_u64(micros: i64) -> u64 {
    // Timestamps before epoch are invalid for OTLP; clamp to 0.
    if micros < 0 {
        0
    } else {
        (micros as u64).saturating_mul(1_000)
    }
}

/// Render an i64 microseconds value as the OTLP `*UnixNano` string.
///
/// Multiplies by `1_000` with `saturating_mul` so a malformed timestamp
/// (e.g. one near `i64::MAX`) produces `i64::MAX` rather than wrapping
/// to a small/negative value.
fn micros_to_nanos_str(micros: i64) -> String {
    micros.saturating_mul(1_000).to_string()
}

/// Reinterpret an `i32` storage value as an OTLP `uint32` count.
///
/// Iceberg stores `dropped_*_count` and link `flags` as signed Int32
/// (no native uint32 type). OTLP uses `uint32`. Negative values are
/// invariant violations from a buggy writer; clamp to 0 rather than
/// surfacing wrap-around to the client.
#[allow(clippy::cast_sign_loss)]
const fn u32_from_i32(v: i32) -> u32 {
    if v < 0 { 0 } else { v as u32 }
}

/// Decode this row's `events` cell into OTLP proto [`Event`]s.
///
/// Returns an empty vector when the cell is null, the column has the
/// wrong Arrow shape, or the list is empty. Robust to per-element nulls
/// and missing optional sub-fields so a malformed write cannot poison
/// the response.
fn proto_events_for_row(events_list: &ListArray, row: usize) -> Vec<Event> {
    if events_list.is_null(row) {
        return vec![];
    }
    let elem = events_list.value(row);
    let Some(st) = elem.as_any().downcast_ref::<StructArray>() else {
        return vec![];
    };
    let timestamps = st
        .column_by_name("timestamp")
        .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>());
    let names = st.column_by_name("name").and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let attributes = st
        .column_by_name("attributes")
        .and_then(|c| c.as_any().downcast_ref::<MapArray>());
    let dropped_attrs = st
        .column_by_name("dropped_attributes_count")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>());

    let mut out = Vec::with_capacity(st.len());
    for i in 0..st.len() {
        if st.is_null(i) {
            continue;
        }
        let time_unix_nano = timestamps
            .filter(|t| !t.is_null(i))
            .map_or(0, |t| micros_to_nanos_u64(t.value(i)));
        let name = names
            .filter(|n| !n.is_null(i))
            .map(|n| n.value(i).to_string())
            .unwrap_or_default();
        let attributes = attributes.map(|a| proto_attributes(a, i)).unwrap_or_default();
        let dropped_attributes_count = dropped_attrs.filter(|d| !d.is_null(i)).map_or(0, |d| u32_from_i32(d.value(i)));
        out.push(Event {
            time_unix_nano,
            name,
            attributes,
            dropped_attributes_count,
        });
    }
    out
}

/// Decode this row's `links` cell into OTLP proto [`Link`]s.
///
/// Trace and span IDs are stored as hex strings (see ingest's
/// `transform.rs`); we hex-decode them back to raw bytes so the wire
/// format matches the OTLP `bytes` typing. Invalid hex collapses to
/// empty bytes, mirroring how `row_to_proto_span` handles malformed
/// span/trace IDs.
fn proto_links_for_row(links_list: &ListArray, row: usize) -> Vec<Link> {
    if links_list.is_null(row) {
        return vec![];
    }
    let elem = links_list.value(row);
    let Some(st) = elem.as_any().downcast_ref::<StructArray>() else {
        return vec![];
    };
    let trace_ids = st
        .column_by_name("trace_id")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let span_ids = st
        .column_by_name("span_id")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let trace_states = st
        .column_by_name("trace_state")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let attributes = st
        .column_by_name("attributes")
        .and_then(|c| c.as_any().downcast_ref::<MapArray>());
    let dropped_attrs = st
        .column_by_name("dropped_attributes_count")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>());
    let flags_arr = st.column_by_name("flags").and_then(|c| c.as_any().downcast_ref::<Int32Array>());

    let mut out = Vec::with_capacity(st.len());
    for i in 0..st.len() {
        if st.is_null(i) {
            continue;
        }
        let trace_id_hex = trace_ids.filter(|t| !t.is_null(i)).map_or("", |t| t.value(i));
        let span_id_hex = span_ids.filter(|s| !s.is_null(i)).map_or("", |s| s.value(i));
        let trace_state = trace_states
            .filter(|t| !t.is_null(i))
            .map(|t| t.value(i).to_string())
            .unwrap_or_default();
        let attributes = attributes.map(|a| proto_attributes(a, i)).unwrap_or_default();
        let dropped_attributes_count = dropped_attrs.filter(|d| !d.is_null(i)).map_or(0, |d| u32_from_i32(d.value(i)));
        let flags = flags_arr.filter(|f| !f.is_null(i)).map_or(0, |f| u32_from_i32(f.value(i)));
        out.push(Link {
            trace_id: hex::decode(trace_id_hex).unwrap_or_default(),
            span_id: hex::decode(span_id_hex).unwrap_or_default(),
            trace_state,
            attributes,
            dropped_attributes_count,
            flags,
        });
    }
    out
}

/// Decode this row's `events` cell into OTLP JSON event objects.
///
/// Mirrors [`proto_events_for_row`] but builds `serde_json::Value`s in
/// the camelCase OTLP/JSON shape (`timeUnixNano` as a stringified nanos
/// value, `droppedAttributesCount`).
fn json_events_for_row(events_list: &ListArray, row: usize) -> Vec<Value> {
    if events_list.is_null(row) {
        return vec![];
    }
    let elem = events_list.value(row);
    let Some(st) = elem.as_any().downcast_ref::<StructArray>() else {
        return vec![];
    };
    let timestamps = st
        .column_by_name("timestamp")
        .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>());
    let names = st.column_by_name("name").and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let attributes = st
        .column_by_name("attributes")
        .and_then(|c| c.as_any().downcast_ref::<MapArray>());
    let dropped_attrs = st
        .column_by_name("dropped_attributes_count")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>());

    let mut out = Vec::with_capacity(st.len());
    for i in 0..st.len() {
        if st.is_null(i) {
            continue;
        }
        let time_unix_nano = timestamps
            .filter(|t| !t.is_null(i))
            .map_or(0_u64, |t| micros_to_nanos_u64(t.value(i)));
        let name = names.filter(|n| !n.is_null(i)).map_or("", |n| n.value(i));
        let attrs = attributes.map(|a| row_attributes(a, i)).unwrap_or_default();
        let dropped = dropped_attrs.filter(|d| !d.is_null(i)).map_or(0, |d| u32_from_i32(d.value(i)));
        let mut ev = Map::new();
        ev.insert("timeUnixNano".into(), json!(time_unix_nano.to_string()));
        ev.insert("name".into(), json!(name));
        ev.insert("attributes".into(), Value::Array(attrs));
        if dropped > 0 {
            ev.insert("droppedAttributesCount".into(), json!(dropped));
        }
        out.push(Value::Object(ev));
    }
    out
}

/// Decode this row's `links` cell into OTLP JSON link objects.
///
/// Trace and span IDs are kept as hex strings — matches the
/// per-span `traceId` / `spanId` shape emitted by [`build_span_value`].
fn json_links_for_row(links_list: &ListArray, row: usize) -> Vec<Value> {
    if links_list.is_null(row) {
        return vec![];
    }
    let elem = links_list.value(row);
    let Some(st) = elem.as_any().downcast_ref::<StructArray>() else {
        return vec![];
    };
    let trace_ids = st
        .column_by_name("trace_id")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let span_ids = st
        .column_by_name("span_id")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let trace_states = st
        .column_by_name("trace_state")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let attributes = st
        .column_by_name("attributes")
        .and_then(|c| c.as_any().downcast_ref::<MapArray>());
    let dropped_attrs = st
        .column_by_name("dropped_attributes_count")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>());
    let flags_arr = st.column_by_name("flags").and_then(|c| c.as_any().downcast_ref::<Int32Array>());

    let mut out = Vec::with_capacity(st.len());
    for i in 0..st.len() {
        if st.is_null(i) {
            continue;
        }
        let trace_id_hex = trace_ids.filter(|t| !t.is_null(i)).map_or("", |t| t.value(i));
        let span_id_hex = span_ids.filter(|s| !s.is_null(i)).map_or("", |s| s.value(i));
        let attrs = attributes.map(|a| row_attributes(a, i)).unwrap_or_default();
        let dropped = dropped_attrs.filter(|d| !d.is_null(i)).map_or(0, |d| u32_from_i32(d.value(i)));
        let flags = flags_arr.filter(|f| !f.is_null(i)).map_or(0, |f| u32_from_i32(f.value(i)));
        let mut lnk = Map::new();
        lnk.insert("traceId".into(), json!(trace_id_hex));
        lnk.insert("spanId".into(), json!(span_id_hex));
        if let Some(ts) = trace_states.filter(|t| !t.is_null(i)) {
            let s = ts.value(i);
            if !s.is_empty() {
                lnk.insert("traceState".into(), json!(s));
            }
        }
        lnk.insert("attributes".into(), Value::Array(attrs));
        if dropped > 0 {
            lnk.insert("droppedAttributesCount".into(), json!(dropped));
        }
        if flags > 0 {
            lnk.insert("flags".into(), json!(flags));
        }
        out.push(Value::Object(lnk));
    }
    out
}

const fn status_code_to_proto(code: i32) -> i32 {
    match code {
        1 => StatusCode::Ok as i32,
        2 => StatusCode::Error as i32,
        _ => StatusCode::Unset as i32,
    }
}

#[cfg(test)]
mod proto_tests {
    use std::sync::Arc;

    use datafusion::arrow::{
        array::{
            ArrayRef, Int32Array, Int64Array, ListArray, MapBuilder, StringArray, StringBuilder, StructArray,
            TimestampMicrosecondArray,
        },
        buffer::OffsetBuffer,
        datatypes::{DataType, Field, Fields, Schema, TimeUnit},
        record_batch::RecordBatch,
    };

    use super::*;

    /// Build a 1-entry `Map<Utf8,Utf8>` array with a single key/value pair.
    /// Shared by both top-level span attributes and the nested attribute
    /// maps inside event/link list elements.
    fn single_attr_map(key: &str, value: &str) -> datafusion::arrow::array::MapArray {
        let mut b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        b.keys().append_value(key);
        b.values().append_value(value);
        b.append(true).expect("map row");
        b.finish()
    }

    fn map_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("keys", DataType::Utf8, false)),
                            Arc::new(Field::new("values", DataType::Utf8, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        )
    }

    /// Build a single-row `RecordBatch` carrying one event and one link.
    /// The event has a name + one attribute; the link points at a hex
    /// `trace_id`/`span_id` with one attribute and `flags=1`.
    ///
    /// This is the fixture that exercises the events/links code paths
    /// in both the proto and JSON formatters.
    fn single_span_batch_with_events_and_links() -> RecordBatch {
        // -- events: List<Struct{timestamp, name, attributes, dropped_attributes_count}> --
        let event_attrs = single_attr_map("exception.message", "boom");
        let event_attrs_dt = event_attrs.data_type().clone();
        let event_fields: Fields = vec![
            Arc::new(Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )),
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(Field::new("attributes", event_attrs_dt, false)),
            Arc::new(Field::new("dropped_attributes_count", DataType::Int32, true)),
        ]
        .into();
        let event_struct = StructArray::new(
            event_fields.clone(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![1_500_000_000_000_000_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec!["exception"])),
                Arc::new(event_attrs),
                Arc::new(Int32Array::from(vec![Some(0)])),
            ],
            None,
        );
        let event_item_field = Arc::new(Field::new("item", DataType::Struct(event_fields), true));
        let events_array = ListArray::new(
            event_item_field.clone(),
            OffsetBuffer::<i32>::from_lengths(std::iter::once(1)),
            Arc::new(event_struct),
            None,
        );

        // -- links: List<Struct{trace_id, span_id, trace_state, attributes, dropped_attributes_count, flags}> --
        let link_attrs = single_attr_map("link.role", "parent");
        let link_attrs_dt = link_attrs.data_type().clone();
        let link_fields: Fields = vec![
            Arc::new(Field::new("trace_id", DataType::Utf8, false)),
            Arc::new(Field::new("span_id", DataType::Utf8, false)),
            Arc::new(Field::new("trace_state", DataType::Utf8, true)),
            Arc::new(Field::new("attributes", link_attrs_dt, false)),
            Arc::new(Field::new("dropped_attributes_count", DataType::Int32, true)),
            Arc::new(Field::new("flags", DataType::Int32, true)),
        ]
        .into();
        let link_struct = StructArray::new(
            link_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["fedcba98765432100123456789abcdef"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["1122334455667788"])),
                Arc::new(StringArray::from(vec![None as Option<&str>])),
                Arc::new(link_attrs),
                Arc::new(Int32Array::from(vec![Some(0)])),
                Arc::new(Int32Array::from(vec![Some(1)])),
            ],
            None,
        );
        let link_item_field = Arc::new(Field::new("item", DataType::Struct(link_fields), true));
        let links_array = ListArray::new(
            link_item_field.clone(),
            OffsetBuffer::<i32>::from_lengths(std::iter::once(1)),
            Arc::new(link_struct),
            None,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("kind", DataType::Int32, true),
            Field::new("status_code", DataType::Int32, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            map_field("resource_attributes"),
            map_field("span_attributes"),
            Field::new("events", DataType::List(event_item_field), true),
            Field::new("links", DataType::List(link_item_field), true),
            Field::new("dropped_events_count", DataType::Int32, true),
            Field::new("dropped_links_count", DataType::Int32, true),
        ]));

        let res_attrs = single_attr_map("k8s.namespace.name", "icegate");
        let span_attrs = single_attr_map("http.method", "GET");

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"])),
                Arc::new(StringArray::from(vec!["aabbccddeeff0011"])),
                Arc::new(StringArray::from(vec!["GET /api"])),
                Arc::new(Int32Array::from(vec![Some(2)])),
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec![Some("frontend")])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_000_000_000_000])),
                Arc::new(res_attrs),
                Arc::new(span_attrs),
                Arc::new(events_array),
                Arc::new(links_array),
                Arc::new(Int32Array::from(vec![Some(2)])),
                Arc::new(Int32Array::from(vec![Some(3)])),
            ],
        )
        .expect("record batch")
    }

    fn single_span_batch() -> RecordBatch {
        fn map_field(name: &str) -> Field {
            Field::new(
                name,
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Arc::new(Field::new("keys", DataType::Utf8, false)),
                                Arc::new(Field::new("values", DataType::Utf8, true)),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            )
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("kind", DataType::Int32, true),
            Field::new("status_code", DataType::Int32, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("duration_micros", DataType::Int64, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            map_field("resource_attributes"),
            map_field("span_attributes"),
        ]));

        let mut res_b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        res_b.keys().append_value("k8s.namespace.name");
        res_b.values().append_value("icegate");
        res_b.append(true).expect("map row");
        let res_attrs = res_b.finish();

        let mut span_b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        span_b.keys().append_value("http.method");
        span_b.values().append_value("GET");
        span_b.append(true).expect("map row");
        let span_attrs = span_b.finish();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"])),
                Arc::new(StringArray::from(vec!["aabbccddeeff0011"])),
                Arc::new(StringArray::from(vec!["GET /api"])),
                Arc::new(Int32Array::from(vec![Some(2)])),
                Arc::new(Int32Array::from(vec![Some(1)])),
                Arc::new(StringArray::from(vec![Some("frontend")])),
                Arc::new(Int64Array::from(vec![Some(100_000)])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_000_000_000_000])),
                Arc::new(res_attrs),
                Arc::new(span_attrs),
            ],
        )
        .expect("record batch")
    }

    #[test]
    fn encodes_to_valid_protobuf() {
        let batches = vec![single_span_batch()];
        let bytes = spans_to_otlp_proto(&batches).expect("proto encode");
        // Round-trip: decode back and check structure.
        let decoded = TracesData::decode(&*bytes).expect("proto decode");
        assert_eq!(decoded.resource_spans.len(), 1);
        let rs = &decoded.resource_spans[0];
        // Resource attributes include both the map entries and the synthesized
        // service.name from the top-level column.
        let resource = rs.resource.as_ref().expect("resource");
        let get = |k: &str| -> Option<&str> {
            resource.attributes.iter().find(|kv| kv.key == k).and_then(|kv| {
                kv.value.as_ref().and_then(|v| match &v.value {
                    Some(AnyVal::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                })
            })
        };
        assert_eq!(get("k8s.namespace.name"), Some("icegate"));
        assert_eq!(get("service.name"), Some("frontend"));

        assert_eq!(rs.scope_spans.len(), 1);
        let spans = &rs.scope_spans[0].spans;
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "GET /api");

        // Span attributes only contain the span-scoped key.
        let span_attrs: Vec<(&str, &str)> = spans[0]
            .attributes
            .iter()
            .filter_map(|kv| {
                let v = kv.value.as_ref()?;
                match &v.value {
                    Some(AnyVal::StringValue(s)) => Some((kv.key.as_str(), s.as_str())),
                    _ => None,
                }
            })
            .collect();
        assert_eq!(span_attrs, vec![("http.method", "GET")]);

        // Trace/span ids decoded from hex.
        assert_eq!(spans[0].trace_id.len(), 16);
        assert_eq!(spans[0].span_id.len(), 8);
        // Start time: 1e15 micros -> 1e18 nanos.
        assert_eq!(spans[0].start_time_unix_nano, 1_000_000_000_000_000_000);
        // Kind = SPAN_KIND_SERVER (2).
        assert_eq!(spans[0].kind, 2);
        // Status = OK (1).
        assert_eq!(spans[0].status.as_ref().expect("status").code, 1);
    }

    #[test]
    fn empty_batches_yield_empty_traces_data() {
        let data = spans_to_traces_data(&[]).expect("empty");
        assert!(data.resource_spans.is_empty());
    }

    #[test]
    fn negative_timestamp_clamps_to_zero_nanos() {
        assert_eq!(micros_to_nanos_u64(-1), 0);
    }

    /// Span events stored in the `events` `List<Struct>` column must
    /// surface in the OTLP proto response — they were previously
    /// hardcoded to an empty vector, hiding all timed annotations from
    /// downstream consumers (Grafana's "Events" tab, OTLP exporters).
    #[test]
    fn events_round_trip_through_proto() {
        let batches = vec![single_span_batch_with_events_and_links()];
        let bytes = spans_to_otlp_proto(&batches).expect("proto encode");
        let decoded = TracesData::decode(&*bytes).expect("proto decode");
        let span = &decoded.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.events.len(), 1, "expected exactly one event on the span");
        let ev = &span.events[0];
        assert_eq!(ev.name, "exception");
        // 1.5e15 micros → 1.5e18 nanos.
        assert_eq!(ev.time_unix_nano, 1_500_000_000_000_000_000);
        assert_eq!(ev.dropped_attributes_count, 0);
        let attrs: Vec<(&str, &str)> = ev
            .attributes
            .iter()
            .filter_map(|kv| {
                let v = kv.value.as_ref()?;
                match &v.value {
                    Some(AnyVal::StringValue(s)) => Some((kv.key.as_str(), s.as_str())),
                    _ => None,
                }
            })
            .collect();
        assert_eq!(attrs, vec![("exception.message", "boom")]);
        // The top-level dropped_events_count column (= 2) must propagate.
        assert_eq!(span.dropped_events_count, 2);
    }

    /// Span links stored in the `links` `List<Struct>` column must
    /// surface in the OTLP proto response with hex-decoded raw bytes
    /// for `trace_id`/`span_id`, matching the OTLP `bytes` typing.
    #[test]
    fn links_round_trip_through_proto() {
        let batches = vec![single_span_batch_with_events_and_links()];
        let bytes = spans_to_otlp_proto(&batches).expect("proto encode");
        let decoded = TracesData::decode(&*bytes).expect("proto decode");
        let span = &decoded.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.links.len(), 1, "expected exactly one link on the span");
        let lnk = &span.links[0];
        assert_eq!(lnk.trace_id.len(), 16, "trace_id should be 16 raw bytes");
        assert_eq!(lnk.span_id.len(), 8, "span_id should be 8 raw bytes");
        // Spot-check the first/last bytes of the hex round-trip:
        // "fedcba98765432100123456789abcdef" → bytes 0xfe…0xef.
        assert_eq!(lnk.trace_id[0], 0xfe);
        assert_eq!(lnk.trace_id[15], 0xef);
        assert_eq!(lnk.span_id[0], 0x11);
        assert_eq!(lnk.span_id[7], 0x88);
        assert_eq!(lnk.flags, 1);
        let attrs: Vec<(&str, &str)> = lnk
            .attributes
            .iter()
            .filter_map(|kv| {
                let v = kv.value.as_ref()?;
                match &v.value {
                    Some(AnyVal::StringValue(s)) => Some((kv.key.as_str(), s.as_str())),
                    _ => None,
                }
            })
            .collect();
        assert_eq!(attrs, vec![("link.role", "parent")]);
        // Top-level dropped_links_count column (= 3) must propagate.
        assert_eq!(span.dropped_links_count, 3);
    }

    /// JSON formatter must also emit `events` and `links` arrays under
    /// the OTLP camelCase keys. Trace and span IDs in JSON keep their
    /// hex-string encoding to match how the surrounding span emits its
    /// own `traceId` / `spanId`.
    #[test]
    fn events_and_links_appear_in_otlp_json() {
        let batches = vec![single_span_batch_with_events_and_links()];
        let value = spans_to_otlp_json(&batches).expect("json build");
        let span = &value["resourceSpans"][0]["scopeSpans"][0]["spans"][0];

        let events = span.get("events").expect("events array present");
        assert_eq!(events.as_array().expect("events is array").len(), 1);
        assert_eq!(events[0]["name"], "exception");
        assert_eq!(events[0]["timeUnixNano"], "1500000000000000000");
        let event_attrs = events[0]["attributes"].as_array().expect("event attrs");
        assert_eq!(event_attrs[0]["key"], "exception.message");
        assert_eq!(event_attrs[0]["value"]["stringValue"], "boom");

        let links = span.get("links").expect("links array present");
        assert_eq!(links.as_array().expect("links is array").len(), 1);
        assert_eq!(links[0]["traceId"], "fedcba98765432100123456789abcdef");
        assert_eq!(links[0]["spanId"], "1122334455667788");
        assert_eq!(links[0]["flags"], 1);
        let link_attrs = links[0]["attributes"].as_array().expect("link attrs");
        assert_eq!(link_attrs[0]["key"], "link.role");
        assert_eq!(link_attrs[0]["value"]["stringValue"], "parent");

        assert_eq!(span["droppedEventsCount"], 2);
        assert_eq!(span["droppedLinksCount"], 3);
    }
}

#[cfg(test)]
mod search_response_tests {
    use std::sync::Arc;

    use datafusion::arrow::{
        array::{RecordBatch, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
    };

    use super::spansets_to_search_response;

    /// Build a minimal record batch carrying just the columns the search
    /// formatter actually inspects. `parents[i]` of `None` means "no
    /// `parent_span_id` column value" (null), `Some("")` means an empty
    /// string. Both are treated as roots by the formatter.
    ///
    /// Each row gets a synthetic `span_id` of the form `span-{i}` so
    /// `spanSets` assertions can pin specific spans without callers
    /// having to hand-craft hex IDs.
    fn build_batch(
        trace_ids: &[&str],
        names: &[&str],
        services: &[Option<&str>],
        parents: &[Option<&str>],
        start_micros: &[i64],
        end_micros: &[i64],
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("end_timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]));
        let span_ids: Vec<String> = (0..trace_ids.len()).map(|i| format!("span-{i}")).collect();
        let span_ids_refs: Vec<&str> = span_ids.iter().map(String::as_str).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(trace_ids.to_vec())),
                Arc::new(StringArray::from(span_ids_refs)),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(services.to_vec())),
                Arc::new(StringArray::from(parents.to_vec())),
                Arc::new(TimestampMicrosecondArray::from(start_micros.to_vec())),
                Arc::new(TimestampMicrosecondArray::from(end_micros.to_vec())),
            ],
        )
        .expect("record batch")
    }

    /// When the matched set contains a true root span (null
    /// `parent_span_id`), the summary uses the root's service / name even
    /// though a child span is also present.
    #[test]
    fn root_span_wins_over_child_in_same_trace() {
        let batch = build_batch(
            &["trace-a", "trace-a"],
            &["root-span", "child-span"],
            &[Some("frontend"), Some("backend")],
            // First row is root (null parent), second row is a child.
            &[None, Some("aabbccdd")],
            &[1_000, 2_000],
            &[1_500, 2_500],
        );
        let resp = spansets_to_search_response(&[batch]);
        assert_eq!(resp.traces.len(), 1);
        let summary = &resp.traces[0];
        assert_eq!(summary.trace_id, "trace-a");
        assert_eq!(summary.root_service_name.as_deref(), Some("frontend"));
        assert_eq!(summary.root_trace_name.as_deref(), Some("root-span"));
    }

    /// When only child spans are matched (the root lives outside the
    /// filter — e.g. `{resource.service.name=foo}` only matches `foo`'s
    /// child spans), the summary falls back to the earliest matched span
    /// instead of leaving the service / name empty in the results table.
    #[test]
    fn child_only_matches_fall_back_to_earliest_span() {
        let batch = build_batch(
            &["trace-b", "trace-b"],
            &["later-child", "earlier-child"],
            &[Some("backend"), Some("backend")],
            // Both rows are children — no row's parent_span_id is null/empty.
            &[Some("aabbccdd"), Some("eeff0011")],
            // Earlier-child has the smaller `timestamp`.
            &[2_000, 1_000],
            &[2_500, 1_500],
        );
        let resp = spansets_to_search_response(&[batch]);
        assert_eq!(resp.traces.len(), 1);
        let summary = &resp.traces[0];
        assert_eq!(summary.root_service_name.as_deref(), Some("backend"));
        assert_eq!(summary.root_trace_name.as_deref(), Some("earlier-child"));
    }

    /// An empty-string `parent_span_id` (some writers serialise root spans
    /// this way) is still treated as a root.
    #[test]
    fn empty_parent_span_id_counts_as_root() {
        let batch = build_batch(
            &["trace-c"],
            &["root-span"],
            &[Some("frontend")],
            &[Some("")],
            &[1_000],
            &[1_500],
        );
        let resp = spansets_to_search_response(&[batch]);
        assert_eq!(resp.traces[0].root_service_name.as_deref(), Some("frontend"));
        assert_eq!(resp.traces[0].root_trace_name.as_deref(), Some("root-span"));
    }

    /// Regression: Grafana's inline-trace renderer crashes with
    /// `TypeError: Cannot read properties of undefined (reading 'reduce')`
    /// when a trace summary lacks a `spanSets` array. Even an empty
    /// trace must serialise with at least one (possibly empty) entry.
    #[test]
    fn span_sets_is_always_populated_even_for_a_trace_with_one_span() {
        let batch = build_batch(
            &["trace-d"],
            &["only-span"],
            &[Some("frontend")],
            &[None],
            &[1_000_000],
            &[1_500_000],
        );
        let resp = spansets_to_search_response(&[batch]);
        let summary = &resp.traces[0];
        assert_eq!(summary.span_sets.len(), 1, "exactly one span set per trace today");
        let span_set = &summary.span_sets[0];
        assert_eq!(span_set.matched, 1);
        assert_eq!(span_set.spans.len(), 1);
        let span = &span_set.spans[0];
        assert_eq!(span.span_id, "span-0");
        // 1_000_000 micros → 1_000_000_000 nanos.
        assert_eq!(span.start_time_unix_nano, "1000000000");
        // (1_500_000 - 1_000_000) micros → 500_000_000 nanos.
        assert_eq!(span.duration_nanos, "500000000");
        // service.name + name attributes are surfaced.
        let attrs: std::collections::BTreeMap<String, String> = span
            .attributes
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.string_value.clone()))
            .collect();
        assert_eq!(attrs.get("service.name").map(String::as_str), Some("frontend"));
        assert_eq!(attrs.get("name").map(String::as_str), Some("only-span"));
    }

    /// Regression for trace `2c63412de72dcf9e330dc915368da83f`:
    /// `ingest_logs` is the true root (null `parent_span_id`,
    /// duration ~237 ms) and every other span is a short
    /// non-root child. Even when the stage-2 row order hands us the
    /// short non-root children before the root, the formatter must
    /// pick the root for `rootTraceName`.
    #[test]
    fn longest_root_wins_when_ordered_after_shorter_children() {
        let batch = build_batch(
            // Mimic the planner giving us shorter children first and
            // the long-running root last — earlier "earliest-timestamp
            // wins" logic would have picked the first child.
            &["2c63412d", "2c63412d", "2c63412d"],
            &["process_request", "logs_to_record_batch", "ingest_logs"],
            &[Some("icegate-ingest"), Some("icegate-ingest"), Some("icegate-ingest")],
            // Only `ingest_logs` is a root; others have non-null parents.
            &[Some("205018405f0c7fdc"), Some("205018405f0c7fdc"), None],
            // Timestamps in micros mirroring the real trace: children
            // actually *start* after the root, but the delta is tiny.
            &[1_776_641_085_216_579, 1_776_641_085_215_763, 1_776_641_085_215_706],
            // End timestamps: children are microseconds long, root is ~237 ms.
            &[1_776_641_085_216_648, 1_776_641_085_215_846, 1_776_641_085_453_613],
        );
        let resp = spansets_to_search_response(&[batch]);
        assert_eq!(resp.traces.len(), 1);
        let summary = &resp.traces[0];
        assert_eq!(
            summary.root_service_name.as_deref(),
            Some("icegate-ingest"),
            "unexpected root service for trace {}",
            summary.trace_id
        );
        assert_eq!(
            summary.root_trace_name.as_deref(),
            Some("ingest_logs"),
            "root trace name should be the null-parent span, not a short child"
        );
    }

    /// A trace with two null-parent spans (e.g. cross-service
    /// propagation quirk) must pick the longer-duration one — it is
    /// the span that actually encompasses the rest of the trace.
    #[test]
    fn longest_duration_root_wins_among_multiple_null_parent_spans() {
        let batch = build_batch(
            &["trace-x", "trace-x"],
            &["short-root", "long-root"],
            &[Some("svc-a"), Some("svc-b")],
            // Both have null parents — ambiguous "multi-root" trace.
            &[None, None],
            // short-root starts earlier but is tiny; long-root encompasses everything.
            &[1_000, 2_000],
            &[1_100, 10_000],
        );
        let resp = spansets_to_search_response(&[batch]);
        let summary = &resp.traces[0];
        assert_eq!(summary.root_trace_name.as_deref(), Some("long-root"));
        assert_eq!(summary.root_service_name.as_deref(), Some("svc-b"));
    }

    /// Multi-span trace should surface every span we received in
    /// `spanSets[0].spans`, with `matched` reflecting the count.
    #[test]
    fn span_sets_contains_every_returned_span_in_order() {
        let batch = build_batch(
            &["trace-e", "trace-e"],
            &["root-e", "child-e"],
            &[Some("svc"), Some("svc")],
            &[None, Some("aabbccdd")],
            &[1_000, 2_000],
            &[5_000, 3_000],
        );
        let resp = spansets_to_search_response(&[batch]);
        let summary = &resp.traces[0];
        let span_set = &summary.span_sets[0];
        assert_eq!(span_set.matched, 2);
        let names: Vec<&str> = span_set
            .spans
            .iter()
            .flat_map(|s| s.attributes.iter())
            .filter(|kv| kv.key == "name")
            .map(|kv| kv.value.string_value.as_str())
            .collect();
        assert_eq!(names, vec!["root-e", "child-e"]);
    }
}
