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

use crate::tempo::models::{SearchMetrics, SearchResponse, TraceSummary};

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
/// - `root_service_name` / `root_trace_name` come from the first span whose
///   `parent_span_id` is null or empty (i.e., the root span).
/// - `start_time_unix_nano` is the minimum `timestamp` (converted to nanos).
/// - `duration_ms` is the span from min `timestamp` to max `end_timestamp`.
///
/// `BTreeMap` is used to keep the output deterministic, which also makes
/// testing easier downstream.
#[must_use]
pub fn spansets_to_search_response(batches: &[RecordBatch]) -> SearchResponse {
    // One accumulator per trace. All fields have sensible `Default`s so
    // `or_default()` + in-place mutation is ergonomic.
    #[derive(Default)]
    struct Acc {
        root_service: Option<String>,
        root_name: Option<String>,
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
            if is_root {
                acc.root_service = svcs.as_ref().map(|c| c.value(row).to_string());
                acc.root_name = names.as_ref().map(|c| c.value(row).to_string());
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
        .map(|(tid, acc)| TraceSummary {
            trace_id: tid,
            root_service_name: acc.root_service,
            root_trace_name: acc.root_name,
            // Tempo reports times in nanoseconds. Our Iceberg rows are in
            // microseconds (per `schema::COL_TIMESTAMP`), so scale up.
            start_time_unix_nano: (acc.start_micros * 1_000).to_string(),
            duration_ms: u64::try_from(((acc.end_micros - acc.start_micros) / 1_000).max(0)).unwrap_or(0),
        })
        .collect();

    SearchResponse {
        traces,
        metrics: SearchMetrics { total_blocks: 0 },
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
