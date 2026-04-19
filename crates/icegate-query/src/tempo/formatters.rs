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

use datafusion::arrow::{
    array::{Array, AsArray, Int32Array, MapArray, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::Int32Type,
};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value as AnyVal},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, TracesData, status::StatusCode},
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
    span.insert("startTimeUnixNano".into(), json!((start_micros * 1_000).to_string()));
    if let Some(e) = end_micros {
        span.insert("endTimeUnixNano".into(), json!((e * 1_000).to_string()));
    }
    span.insert("attributes".into(), Value::Array(attrs));
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
        .map(|(idx, _)| batch.column(idx).as_string::<i32>())
}

fn i32_col_opt<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int32Array> {
    batch
        .schema()
        .column_with_name(name)
        .map(|(idx, _)| batch.column(idx).as_primitive::<Int32Type>())
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
        /// Minimum `timestamp` seen, in microseconds. Zero acts as
        /// "uninitialised" since Iceberg timestamps are always positive.
        start_micros: i64,
        /// Maximum `end_timestamp` seen, in microseconds.
        end_micros: i64,
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
            let is_root = parents.as_ref().map_or(true, |c| c.is_null(row) || c.value(row).is_empty());
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
                if acc.start_micros == 0 || t < acc.start_micros {
                    acc.start_micros = t;
                }
            }
            if let Some(e) = ends {
                let t = e.value(row);
                if t > acc.end_micros {
                    acc.end_micros = t;
                }
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
            TraceSummary {
                trace_id: tid,
                root_service_name,
                root_trace_name,
                // Tempo reports times in nanoseconds. Our Iceberg rows are in
                // microseconds (per `schema::COL_TIMESTAMP`), so scale up.
                start_time_unix_nano: (acc.start_micros * 1_000).to_string(),
                duration_ms: u64::try_from(((acc.end_micros - acc.start_micros) / 1_000).max(0)).unwrap_or(0),
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
        events: vec![],
        dropped_events_count: 0,
        links: vec![],
        dropped_links_count: 0,
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
    if micros < 0 { 0 } else { (micros as u64) * 1_000 }
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
        array::{Int32Array, Int64Array, MapBuilder, StringArray, StringBuilder, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };

    use super::*;

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
