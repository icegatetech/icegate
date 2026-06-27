//! OTLP traces (spans) -> Arrow transform.

use std::sync::{Arc, OnceLock};

use arrow::{
    array::{ArrayBuilder, ArrayRef, FixedSizeBinaryBuilder, MapBuilder, MapFieldNames, RecordBatch, StringBuilder},
    datatypes::{DataType, Schema},
};
use iceberg::arrow::schema_to_arrow_schema;
use icegate_common::DEFAULT_TENANT_ID;

use super::attributes::{
    dedupe_dotted_attributes, extract_map_fields_from_nested_struct, extract_map_fields_from_schema_named,
    extract_string_value, flatten_any_value_dotted, is_zero_bytes, merge_dotted_attributes, u32_count_to_i32,
};

/// Returns the Arrow schema for spans, derived from the Iceberg spans schema.
///
/// Uses `icegate_common::schema::spans_schema()` as the source of truth
/// and converts it to Arrow format using
/// `iceberg::arrow::schema_to_arrow_schema()`.
///
/// # Panics
///
/// Panics if the Iceberg schema cannot be created or converted to Arrow.
/// This should never happen in practice as the schema is statically defined.
#[allow(clippy::expect_used)]
pub fn spans_arrow_schema() -> Schema {
    // Cached: the schema is static, but rebuilding it from the Iceberg schema and
    // converting to Arrow on every request is wasted work (twice per traces
    // request, alongside operations). Cloning the cached `Schema` is cheap because
    // Arrow `Fields` are `Arc`-backed.
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let iceberg_schema = icegate_common::schema::spans_schema().expect("spans_schema should always be valid");
            schema_to_arrow_schema(&iceberg_schema).expect("spans schema conversion should succeed")
        })
        .clone()
}

/// Transforms an OTLP traces export request to an Arrow `RecordBatch`.
///
/// Extracts all spans from the request, validating `trace_id` (16 bytes,
/// non-zero) and `span_id` (8 bytes, non-zero) per span. Invalid spans are
/// dropped silently and counted in the second return value so the caller
/// can surface partial-success metrics.
///
/// Produces every column in the spans schema: top-level fields, separate
/// `resource_attributes` and `span_attributes` maps (the latter merging
/// scope-level and span-level attributes with span winning on collision),
/// and the nested `events` / `links` `List<Struct>` arrays. Links with
/// invalid `trace_id` / `span_id` are dropped from the list and counted
/// into the parent span's `dropped_links_count`.
///
/// # Arguments
///
/// * `request` - The OTLP export traces request
/// * `tenant_id` - Tenant identifier (from request metadata or default)
///
/// # Returns
///
/// `(Some(batch), drops)` if at least one span is valid, or
/// `(None, drops)` if zero valid spans remain.
///
/// # Errors
///
/// Returns `IngestError` if schema validation or `RecordBatch` creation fails.
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::expect_used)]
#[allow(clippy::too_many_lines)]
#[tracing::instrument(skip(request))]
pub fn spans_to_record_batch(
    request: &opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    tenant_id: Option<&str>,
) -> crate::error::Result<(Option<RecordBatch>, usize)> {
    use arrow::array::{Int32Builder, Int64Array, ListBuilder, StructBuilder, TimestampMicrosecondBuilder};

    let ingested_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_micros() as i64;

    let total_spans: usize = request
        .resource_spans
        .iter()
        .flat_map(|rs| &rs.scope_spans)
        .map(|ss| ss.spans.len())
        .sum();

    if total_spans == 0 {
        return Ok((None, 0));
    }

    let schema = spans_arrow_schema();
    let (resource_attr_key_field, resource_attr_value_field) =
        extract_map_fields_from_schema_named(&schema, icegate_common::schema::COL_RESOURCE_ATTRIBUTES)?;
    let (span_attr_key_field, span_attr_value_field) =
        extract_map_fields_from_schema_named(&schema, icegate_common::schema::COL_SPAN_ATTRIBUTES)?;

    // Top-level column builders. Events and links are written as all-null
    // placeholders; Tasks 14 and 15 replace these with real list builders.
    let mut tenant_id_builder = StringBuilder::with_capacity(total_spans, total_spans * 16);
    let mut service_name_builder = StringBuilder::with_capacity(total_spans, total_spans * 32);
    let mut trace_id_builder = FixedSizeBinaryBuilder::with_capacity(total_spans, 16);
    let mut span_id_builder = FixedSizeBinaryBuilder::with_capacity(total_spans, 8);
    let mut parent_span_id_builder = FixedSizeBinaryBuilder::with_capacity(total_spans, 8);
    let mut timestamp_builder: Vec<i64> = Vec::with_capacity(total_spans);
    let mut end_timestamp_builder: Vec<i64> = Vec::with_capacity(total_spans);
    let mut ingested_timestamp_builder: Vec<i64> = Vec::with_capacity(total_spans);
    let mut duration_micros_builder: Vec<i64> = Vec::with_capacity(total_spans);
    let mut trace_state_builder = StringBuilder::with_capacity(total_spans, total_spans * 16);
    let mut name_builder = StringBuilder::with_capacity(total_spans, total_spans * 32);
    let mut kind_builder = Int32Builder::with_capacity(total_spans);
    let mut status_code_builder = Int32Builder::with_capacity(total_spans);
    let mut status_message_builder = StringBuilder::with_capacity(total_spans, total_spans * 32);
    let mut resource_attrs_builder = MapBuilder::new(
        Some(MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_keys_field(resource_attr_key_field)
    .with_values_field(resource_attr_value_field);

    let mut span_attrs_builder = MapBuilder::new(
        Some(MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_keys_field(span_attr_key_field)
    .with_values_field(span_attr_value_field);
    let mut flags_builder = Int32Builder::with_capacity(total_spans);
    let mut dropped_attributes_count_builder = Int32Builder::with_capacity(total_spans);
    let mut dropped_events_count_builder = Int32Builder::with_capacity(total_spans);
    let mut dropped_links_count_builder = Int32Builder::with_capacity(total_spans);

    // Events list<struct> builder: struct fields (in schema order) are
    // (timestamp, name, attributes: Map<Utf8,Utf8>, dropped_attributes_count).
    let events_field = schema.field_with_name("events").expect("events field").clone();
    let events_element_field = match events_field.data_type() {
        DataType::List(inner) => inner.clone(),
        _ => {
            return Err(crate::error::IngestError::Validation("events must be List".into()));
        }
    };
    let event_struct_fields = match events_element_field.data_type() {
        DataType::Struct(fs) => fs.clone(),
        _ => {
            return Err(crate::error::IngestError::Validation(
                "events element must be Struct".into(),
            ));
        }
    };
    let (event_attr_key_field, event_attr_value_field) =
        extract_map_fields_from_nested_struct(&event_struct_fields, "attributes")?;

    let mut events_builder = ListBuilder::new(StructBuilder::new(
        event_struct_fields.iter().cloned().collect::<Vec<_>>(),
        vec![
            Box::new(TimestampMicrosecondBuilder::new()) as Box<dyn ArrayBuilder>,
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
            Box::new(
                MapBuilder::new(
                    Some(MapFieldNames {
                        entry: "key_value".to_string(),
                        key: "key".to_string(),
                        value: "value".to_string(),
                    }),
                    StringBuilder::new(),
                    StringBuilder::new(),
                )
                .with_keys_field(event_attr_key_field)
                .with_values_field(event_attr_value_field),
            ) as Box<dyn ArrayBuilder>,
            Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
        ],
    ))
    .with_field(events_element_field);

    // Links list<struct> builder: struct fields (in schema order) are
    // (trace_id, span_id, trace_state?, attributes: Map, dropped_attributes_count, flags?).
    let links_field = schema.field_with_name("links").expect("links field").clone();
    let links_element_field = match links_field.data_type() {
        DataType::List(inner) => inner.clone(),
        _ => {
            return Err(crate::error::IngestError::Validation("links must be List".into()));
        }
    };
    let link_struct_fields = match links_element_field.data_type() {
        DataType::Struct(fs) => fs.clone(),
        _ => {
            return Err(crate::error::IngestError::Validation(
                "links element must be Struct".into(),
            ));
        }
    };
    let (link_attr_key_field, link_attr_value_field) =
        extract_map_fields_from_nested_struct(&link_struct_fields, "attributes")?;

    let mut links_builder = ListBuilder::new(StructBuilder::new(
        link_struct_fields.iter().cloned().collect::<Vec<_>>(),
        vec![
            Box::new(FixedSizeBinaryBuilder::new(16)) as Box<dyn ArrayBuilder>, // trace_id (16 bytes)
            Box::new(FixedSizeBinaryBuilder::new(8)) as Box<dyn ArrayBuilder>,  // span_id (8 bytes)
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,            // trace_state (nullable)
            Box::new(
                MapBuilder::new(
                    Some(MapFieldNames {
                        entry: "key_value".to_string(),
                        key: "key".to_string(),
                        value: "value".to_string(),
                    }),
                    StringBuilder::new(),
                    StringBuilder::new(),
                )
                .with_keys_field(link_attr_key_field)
                .with_values_field(link_attr_value_field),
            ) as Box<dyn ArrayBuilder>,
            Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>, // dropped_attributes_count
            Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>, // flags (nullable)
        ],
    ))
    .with_field(links_element_field);

    let tenant = tenant_id.unwrap_or(DEFAULT_TENANT_ID);
    let empty_attrs: Vec<opentelemetry_proto::tonic::common::v1::KeyValue> = Vec::new();
    let mut drops: usize = 0;

    for resource_spans in &request.resource_spans {
        let resource_attrs = resource_spans.resource.as_ref().map_or(&empty_attrs, |r| &r.attributes);
        let service_name = resource_attrs
            .iter()
            .find(|kv| kv.key == "service.name")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for scope_spans in &resource_spans.scope_spans {
            for span in &scope_spans.spans {
                // Hoist all fixed-size-binary length validation to a single
                // block. `try_into` carries the length check at the type
                // level (slice → &[u8; N]) and `is_zero_bytes` rules out
                // OTLP "absent" sentinels. After this:
                //   - trace_id_arr: `&[u8; 16]`, non-zero — span dropped
                //     and counted otherwise
                //   - span_id_arr : `&[u8; 8]`,  non-zero — span dropped
                //     and counted otherwise
                //   - parent_span_id_arr: `Option<&[u8; 8]>` — `None`
                //     maps to `append_null` (legitimate root span)
                // No FixedSizeBinaryBuilder append below can fail: the
                // builder's `value_length` matches the array's compile-time
                // size, so the post-validation `.expect("…")` lines have
                // no reachable panic path.
                let trace_id_arr: &[u8; 16] = match <&[u8; 16]>::try_from(span.trace_id.as_slice()) {
                    Ok(a) if !is_zero_bytes(a) => a,
                    _ => {
                        drops += 1;
                        continue;
                    }
                };
                let span_id_arr: &[u8; 8] = match <&[u8; 8]>::try_from(span.span_id.as_slice()) {
                    Ok(a) if !is_zero_bytes(a) => a,
                    _ => {
                        drops += 1;
                        continue;
                    }
                };
                let parent_span_id_arr: Option<&[u8; 8]> = match <&[u8; 8]>::try_from(span.parent_span_id.as_slice()) {
                    Ok(a) if !is_zero_bytes(a) => Some(a),
                    _ => None,
                };

                // Tracks how many links were dropped during transform (invalid
                // trace_id/span_id). Added to span.dropped_links_count so the
                // caller sees a faithful total.
                let mut extra_dropped_links: i32 = 0;

                // ── Builder appends ──────────────────────────────────────
                // FixedSizeBinaryBuilder appends go FIRST. Arrow has no
                // public per-builder truncate API, so reordering puts the
                // fallible builders ahead of the infallible ones — any
                // (post-validation, unreachable) failure here leaves
                // tenant/service untouched and avoids row
                // misalignment. `?` converts the never-firing
                // `ArrowError` into `IngestError::Arrow` — a hard,
                // surface-able error rather than a silent panic if the
                // validation chain ever regresses.
                trace_id_builder.append_value(trace_id_arr)?;
                span_id_builder.append_value(span_id_arr)?;
                match parent_span_id_arr {
                    Some(p) => parent_span_id_builder.append_value(p)?,
                    None => parent_span_id_builder.append_null(),
                }

                tenant_id_builder.append_value(tenant);
                match service_name.as_deref() {
                    Some(svc) => service_name_builder.append_value(svc),
                    None => service_name_builder.append_null(),
                }

                let start_micros = (span.start_time_unix_nano / 1000) as i64;
                let end_micros = (span.end_time_unix_nano / 1000) as i64;
                timestamp_builder.push(start_micros);
                end_timestamp_builder.push(end_micros);
                ingested_timestamp_builder.push(ingested_timestamp);
                duration_micros_builder.push((end_micros - start_micros).max(0));

                if span.trace_state.is_empty() {
                    trace_state_builder.append_null();
                } else {
                    trace_state_builder.append_value(&span.trace_state);
                }
                name_builder.append_value(&span.name);

                if span.kind == 0 {
                    kind_builder.append_null();
                } else {
                    kind_builder.append_value(span.kind);
                }

                match span.status.as_ref() {
                    Some(status) => {
                        if status.code == 0 {
                            status_code_builder.append_null();
                        } else {
                            status_code_builder.append_value(status.code);
                        }
                        if status.message.is_empty() {
                            status_message_builder.append_null();
                        } else {
                            status_message_builder.append_value(&status.message);
                        }
                    }
                    None => {
                        status_code_builder.append_null();
                        status_message_builder.append_null();
                    }
                }

                // Resource attributes go to `resource_attributes`. Dedupe
                // ahead of the `MapBuilder` for the same reason as the
                // scope+span path below: downstream `MAP<K,V>` readers
                // disagree on duplicate-key resolution, so we collapse to a
                // single entry per key here.
                let merged_resource_attrs = dedupe_dotted_attributes(resource_attrs);
                for (key, value) in &merged_resource_attrs {
                    resource_attrs_builder.keys().append_value(key);
                    resource_attrs_builder.values().append_value(value);
                }

                // Scope + span attributes fold into `span_attributes` with
                // explicit per-span de-duplication: insertion order into a
                // `MapBuilder` is preserved, but downstream `MAP<K,V>`
                // readers vary on duplicate-key resolution (some surface
                // both, some last-write-wins). We avoid relying on
                // reader-specific semantics by merging into a `BTreeMap`
                // first — scope first, then span — so span attrs win on
                // collision (last-write-wins) and the sorted key order
                // gives a deterministic on-disk attribute layout.
                let scope_attrs = scope_spans.scope.as_ref().map_or(&empty_attrs, |s| &s.attributes);
                let merged_span_attrs = merge_dotted_attributes(scope_attrs, &span.attributes);
                for (key, value) in &merged_span_attrs {
                    span_attrs_builder.keys().append_value(key);
                    span_attrs_builder.values().append_value(value);
                }

                resource_attrs_builder
                    .append(true)
                    .expect("append resource_attributes map entry");
                span_attrs_builder.append(true).expect("append span_attributes map entry");

                // Events list<struct>: one row per parent span.
                {
                    let struct_builder = events_builder.values();
                    for event in &span.events {
                        struct_builder
                            .field_builder::<TimestampMicrosecondBuilder>(0)
                            .expect("event timestamp builder")
                            .append_value((event.time_unix_nano / 1000) as i64);
                        struct_builder
                            .field_builder::<StringBuilder>(1)
                            .expect("event name builder")
                            .append_value(&event.name);
                        let attr_b = struct_builder
                            .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(2)
                            .expect("event attrs builder");
                        for kv in &event.attributes {
                            for (k, v) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
                                attr_b.keys().append_value(&k);
                                attr_b.values().append_value(v);
                            }
                        }
                        attr_b.append(true).expect("append event attrs map");
                        struct_builder
                            .field_builder::<Int32Builder>(3)
                            .expect("event dropped count builder")
                            .append_value(u32_count_to_i32(
                                event.dropped_attributes_count,
                                "event.dropped_attributes_count",
                            )?);
                        struct_builder.append(true);
                    }
                    events_builder.append(true);
                }

                // Links list<struct>: drop entries with invalid ids and count them.
                {
                    let struct_builder = links_builder.values();
                    for link in &span.links {
                        // Length-validated fixed-size arrays mirror the
                        // outer span loop; failure on either id drops the
                        // link and bumps the per-span counter so the
                        // emitted `dropped_links_count` includes our own
                        // drops on top of the OTLP-reported total.
                        let link_trace_id_arr: &[u8; 16] = match <&[u8; 16]>::try_from(link.trace_id.as_slice()) {
                            Ok(a) if !is_zero_bytes(a) => a,
                            _ => {
                                extra_dropped_links += 1;
                                continue;
                            }
                        };
                        let link_span_id_arr: &[u8; 8] = match <&[u8; 8]>::try_from(link.span_id.as_slice()) {
                            Ok(a) if !is_zero_bytes(a) => a,
                            _ => {
                                extra_dropped_links += 1;
                                continue;
                            }
                        };
                        struct_builder
                            .field_builder::<FixedSizeBinaryBuilder>(0)
                            .expect("link trace_id builder")
                            .append_value(link_trace_id_arr)?;
                        struct_builder
                            .field_builder::<FixedSizeBinaryBuilder>(1)
                            .expect("link span_id builder")
                            .append_value(link_span_id_arr)?;
                        let ts_b = struct_builder
                            .field_builder::<StringBuilder>(2)
                            .expect("link trace_state builder");
                        if link.trace_state.is_empty() {
                            ts_b.append_null();
                        } else {
                            ts_b.append_value(&link.trace_state);
                        }
                        let attr_b = struct_builder
                            .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(3)
                            .expect("link attrs builder");
                        for kv in &link.attributes {
                            for (k, v) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
                                attr_b.keys().append_value(&k);
                                attr_b.values().append_value(v);
                            }
                        }
                        attr_b.append(true).expect("append link attrs map");
                        struct_builder
                            .field_builder::<Int32Builder>(4)
                            .expect("link dropped count builder")
                            .append_value(u32_count_to_i32(
                                link.dropped_attributes_count,
                                "link.dropped_attributes_count",
                            )?);
                        let flags_b = struct_builder.field_builder::<Int32Builder>(5).expect("link flags builder");
                        if link.flags == 0 {
                            flags_b.append_null();
                        } else {
                            flags_b.append_value(u32_count_to_i32(link.flags, "link.flags")?);
                        }
                        struct_builder.append(true);
                    }
                    links_builder.append(true);
                }

                if span.flags == 0 {
                    flags_builder.append_null();
                } else {
                    flags_builder.append_value(u32_count_to_i32(span.flags, "span.flags")?);
                }
                if span.dropped_attributes_count == 0 {
                    dropped_attributes_count_builder.append_null();
                } else {
                    dropped_attributes_count_builder.append_value(u32_count_to_i32(
                        span.dropped_attributes_count,
                        "span.dropped_attributes_count",
                    )?);
                }
                if span.dropped_events_count == 0 {
                    dropped_events_count_builder.append_null();
                } else {
                    dropped_events_count_builder.append_value(u32_count_to_i32(
                        span.dropped_events_count,
                        "span.dropped_events_count",
                    )?);
                }
                // Sum u32 and per-transform i32 counter in i64, then narrow:
                // avoids wrapping at the i32 boundary if they happen to add up.
                let total_dropped_links_i64 = i64::from(span.dropped_links_count) + i64::from(extra_dropped_links);
                let total_dropped_links = i32::try_from(total_dropped_links_i64).map_err(|_| {
                    crate::error::IngestError::Validation(format!(
                        "dropped_links_count total exceeds i32::MAX: {total_dropped_links_i64}"
                    ))
                })?;
                if total_dropped_links == 0 {
                    dropped_links_count_builder.append_null();
                } else {
                    dropped_links_count_builder.append_value(total_dropped_links);
                }
            }
        }
    }

    let valid_rows = tenant_id_builder.len();
    if valid_rows == 0 {
        return Ok((None, drops));
    }

    let events_array: ArrayRef = Arc::new(events_builder.finish());
    let links_array: ArrayRef = Arc::new(links_builder.finish());

    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_id_builder.finish()),
        Arc::new(service_name_builder.finish()),
        Arc::new(trace_id_builder.finish()),
        Arc::new(span_id_builder.finish()),
        Arc::new(parent_span_id_builder.finish()),
        Arc::new(arrow::array::TimestampMicrosecondArray::from(timestamp_builder)),
        Arc::new(arrow::array::TimestampMicrosecondArray::from(end_timestamp_builder)),
        Arc::new(arrow::array::TimestampMicrosecondArray::from(
            ingested_timestamp_builder,
        )),
        Arc::new(Int64Array::from(duration_micros_builder)),
        Arc::new(trace_state_builder.finish()),
        Arc::new(name_builder.finish()),
        Arc::new(kind_builder.finish()),
        Arc::new(status_code_builder.finish()),
        Arc::new(status_message_builder.finish()),
        Arc::new(resource_attrs_builder.finish()),
        Arc::new(flags_builder.finish()),
        Arc::new(dropped_attributes_count_builder.finish()),
        Arc::new(dropped_events_count_builder.finish()),
        Arc::new(dropped_links_count_builder.finish()),
        events_array,
        links_array,
        Arc::new(span_attrs_builder.finish()),
    ];

    let batch = RecordBatch::try_new(Arc::new(schema), columns).map_err(|e| {
        tracing::error!("Failed to create spans RecordBatch: {e}");
        crate::error::IngestError::Validation(format!("Failed to create spans RecordBatch: {e}"))
    })?;

    Ok((Some(batch), drops))
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

    use super::*;

    #[test]
    fn spans_arrow_schema_has_nested_events_and_links() {
        let schema = spans_arrow_schema();
        assert!(schema.field_with_name("trace_id").is_ok());
        assert!(schema.field_with_name("events").is_ok());
        assert!(schema.field_with_name("links").is_ok());
    }

    #[test]
    fn spans_to_record_batch_empty_request_returns_none() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        let request = ExportTraceServiceRequest { resource_spans: vec![] };
        let (batch, drops) = spans_to_record_batch(&request, None).expect("should not error");
        assert!(batch.is_none());
        assert_eq!(drops, 0);
    }

    #[test]
    fn spans_to_record_batch_single_span_populates_top_level_columns() {
        use opentelemetry_proto::tonic::{
            collector::trace::v1::ExportTraceServiceRequest,
            resource::v1::Resource,
            trace::v1::{ResourceSpans, ScopeSpans, Span, Status, status::StatusCode},
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("svc".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        parent_span_id: vec![3u8; 8],
                        trace_state: "state-x".to_string(),
                        name: "http.request".to_string(),
                        kind: 2,
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_000_500_000,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(Status {
                            message: "ok".to_string(),
                            code: StatusCode::Ok as i32,
                        }),
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (batch_opt, drops) = spans_to_record_batch(&request, Some("tenant-1")).expect("ok");
        let batch = batch_opt.expect("batch");
        assert_eq!(drops, 0);
        assert_eq!(batch.num_rows(), 1);

        let trace_id = batch
            .column_by_name("trace_id")
            .expect("trace_id")
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .expect("FixedSizeBinary(16)");
        assert_eq!(trace_id.value(0), &vec![1u8; 16][..]);

        let span_id = batch
            .column_by_name("span_id")
            .expect("span_id")
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .expect("FixedSizeBinary(8)");
        assert_eq!(span_id.value(0), &vec![2u8; 8][..]);

        let duration = batch
            .column_by_name("duration_micros")
            .expect("duration")
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("i64");
        assert_eq!(duration.value(0), 500);
    }

    #[test]
    fn spans_to_record_batch_drops_span_with_invalid_trace_id() {
        use opentelemetry_proto::tonic::{
            collector::trace::v1::ExportTraceServiceRequest,
            resource::v1::Resource,
            trace::v1::{ResourceSpans, ScopeSpans, Span},
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![0u8; 16], // all-zero -> invalid
                        span_id: vec![2u8; 8],
                        parent_span_id: vec![],
                        trace_state: String::new(),
                        name: "x".to_string(),
                        kind: 0,
                        start_time_unix_nano: 1,
                        end_time_unix_nano: 2,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: None,
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (batch_opt, drops) = spans_to_record_batch(&request, None).expect("ok");
        assert!(batch_opt.is_none());
        assert_eq!(drops, 1);
    }

    #[test]
    fn spans_attributes_preserve_dots_and_route_to_correct_map() {
        use arrow::array::{Array, MapArray, StringArray};
        use opentelemetry_proto::tonic::{
            collector::trace::v1::ExportTraceServiceRequest,
            resource::v1::Resource,
            trace::v1::{ResourceSpans, ScopeSpans, Span},
        };

        // Helper to pull (key, value) pairs from a MapArray at row 0.
        fn pairs_for_row_0(map: &MapArray) -> std::collections::BTreeMap<String, String> {
            let entries = map.value(0);
            let entries_struct = entries.as_any().downcast_ref::<arrow::array::StructArray>().expect("struct");
            let keys = entries_struct.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
            let values = entries_struct.column(1).as_any().downcast_ref::<StringArray>().expect("values");
            (0..keys.len())
                .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
                .collect()
        }

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("svc".to_string())),
                            }),
                        },
                        KeyValue {
                            key: "cloud.account.id".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("acc-1".to_string())),
                            }),
                        },
                    ],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        parent_span_id: vec![3u8; 8],
                        trace_state: String::new(),
                        name: "op".to_string(),
                        kind: 2,
                        start_time_unix_nano: 1,
                        end_time_unix_nano: 2,
                        attributes: vec![KeyValue {
                            key: "http.method".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("GET".to_string())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: None,
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (batch, _) = spans_to_record_batch(&request, Some("tenant-1")).expect("ok");
        let batch = batch.expect("batch");

        let resource_attrs = batch
            .column_by_name("resource_attributes")
            .expect("resource_attributes")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map");
        let span_attrs = batch
            .column_by_name("span_attributes")
            .expect("span_attributes")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map");

        let resource_pairs = pairs_for_row_0(resource_attrs);
        let span_pairs = pairs_for_row_0(span_attrs);

        // Resource-originated keys land in resource_attributes, preserving dots.
        assert_eq!(resource_pairs.get("service.name"), Some(&"svc".to_string()));
        assert_eq!(resource_pairs.get("cloud.account.id"), Some(&"acc-1".to_string()));

        // Span-level attributes land in span_attributes with dotted keys.
        assert_eq!(span_pairs.get("http.method"), Some(&"GET".to_string()));

        // Post-split invariant: indexed-column mirror keys (underscore form) must
        // NOT leak into either attribute map. Consumers read from the top-level
        // schema columns (service_name, trace_id, ...) instead.
        for mirror in ["service_name", "trace_id", "span_id", "parent_span_id", "kind", "name"] {
            assert!(
                !resource_pairs.contains_key(mirror),
                "mirror `{mirror}` must not appear in resource_attributes"
            );
            assert!(
                !span_pairs.contains_key(mirror),
                "mirror `{mirror}` must not appear in span_attributes"
            );
        }
    }

    #[test]
    fn spans_events_are_materialized_as_list_of_structs() {
        use arrow::array::{Array, ListArray, StringArray, StructArray, TimestampMicrosecondArray};
        use opentelemetry_proto::tonic::{
            collector::trace::v1::ExportTraceServiceRequest,
            resource::v1::Resource,
            trace::v1::{ResourceSpans, ScopeSpans, Span, span::Event},
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        parent_span_id: vec![],
                        trace_state: String::new(),
                        name: "op".to_string(),
                        kind: 0,
                        start_time_unix_nano: 1_000,
                        end_time_unix_nano: 2_000,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![Event {
                            time_unix_nano: 1_500,
                            name: "cache.hit".to_string(),
                            attributes: vec![KeyValue {
                                key: "db.system".to_string(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("postgres".to_string())),
                                }),
                            }],
                            dropped_attributes_count: 0,
                        }],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: None,
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (batch, _) = spans_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");
        let events = batch
            .column_by_name("events")
            .expect("events")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        let row0 = events.value(0);
        let row0_struct = row0.as_any().downcast_ref::<StructArray>().expect("struct");
        assert_eq!(row0_struct.len(), 1);

        let ts = row0_struct
            .column_by_name("timestamp")
            .expect("timestamp")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("ts");
        assert_eq!(ts.value(0), 1); // 1500 ns / 1000 = 1 μs

        let name = row0_struct
            .column_by_name("name")
            .expect("name")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name");
        assert_eq!(name.value(0), "cache.hit");
    }

    #[test]
    fn spans_links_are_materialized_and_invalid_ids_dropped() {
        use arrow::array::{Array, Int32Array, ListArray, StringArray, StructArray};
        use opentelemetry_proto::tonic::{
            collector::trace::v1::ExportTraceServiceRequest,
            resource::v1::Resource,
            trace::v1::{ResourceSpans, ScopeSpans, Span, span::Link},
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        parent_span_id: vec![],
                        trace_state: String::new(),
                        name: "op".to_string(),
                        kind: 0,
                        start_time_unix_nano: 1,
                        end_time_unix_nano: 2,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![
                            Link {
                                trace_id: vec![9u8; 16],
                                span_id: vec![8u8; 8],
                                trace_state: "tstate".to_string(),
                                attributes: vec![],
                                dropped_attributes_count: 0,
                                flags: 0,
                            },
                            Link {
                                trace_id: vec![9u8; 16],
                                span_id: vec![0u8; 4], // invalid -> dropped
                                trace_state: String::new(),
                                attributes: vec![],
                                dropped_attributes_count: 0,
                                flags: 0,
                            },
                        ],
                        dropped_links_count: 0,
                        status: None,
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (batch, _) = spans_to_record_batch(&request, None).expect("ok");
        let batch = batch.expect("batch");

        let links = batch
            .column_by_name("links")
            .expect("links")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        let row0 = links.value(0);
        let row0_struct = row0.as_any().downcast_ref::<StructArray>().expect("struct");
        assert_eq!(row0_struct.len(), 1); // second link dropped

        let trace_ids = row0_struct
            .column_by_name("trace_id")
            .expect("trace_id")
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .expect("FixedSizeBinary(16)");
        assert_eq!(trace_ids.value(0), &vec![9u8; 16][..]);

        let trace_state = row0_struct
            .column_by_name("trace_state")
            .expect("trace_state")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(trace_state.value(0), "tstate");

        let dropped_links = batch
            .column_by_name("dropped_links_count")
            .expect("dropped_links_count")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("i32");
        assert_eq!(dropped_links.value(0), 1);
    }

    #[test]
    fn spans_split_attributes_into_resource_and_span_columns() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        fn kv(k: &str, v: &str) -> KeyValue {
            KeyValue {
                key: k.to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(v.to_string())),
                }),
            }
        }

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "ingest-test"), kv("k8s.namespace.name", "icegate")],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "scope-a".to_string(),
                        version: "1.0".to_string(),
                        attributes: vec![kv("scope.only.key", "SV")],
                        dropped_attributes_count: 0,
                    }),
                    spans: vec![Span {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        parent_span_id: vec![],
                        trace_state: String::new(),
                        flags: 0,
                        name: "op".to_string(),
                        kind: 2,
                        start_time_unix_nano: 1_000_000_000,
                        end_time_unix_nano: 1_000_010_000,
                        attributes: vec![kv("http.method", "GET"), kv("span.only.key", "SVAL")],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: None,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (maybe_batch, drops) = super::spans_to_record_batch(&request, None).expect("spans transform");
        assert_eq!(drops, 0);
        let batch = maybe_batch.expect("batch produced");
        assert_eq!(batch.num_rows(), 1);

        // Collect the two attribute maps into (key, value) vecs for row 0.
        let resource_attrs_col = batch
            .column_by_name("resource_attributes")
            .expect("resource_attributes column")
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .expect("MapArray");
        let span_attrs_col = batch
            .column_by_name("span_attributes")
            .expect("span_attributes column")
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .expect("MapArray");

        let resource_row = map_row_as_pairs(resource_attrs_col, 0);
        let span_row = map_row_as_pairs(span_attrs_col, 0);

        // Resource-only keys land in resource_attributes.
        assert!(resource_row.iter().any(|(k, v)| k == "service.name" && v == "ingest-test"));
        assert!(resource_row.iter().any(|(k, v)| k == "k8s.namespace.name" && v == "icegate"));

        // Span + scope keys land in span_attributes. No resource-level keys leak.
        assert!(span_row.iter().any(|(k, v)| k == "http.method" && v == "GET"));
        assert!(span_row.iter().any(|(k, v)| k == "span.only.key" && v == "SVAL"));
        assert!(span_row.iter().any(|(k, v)| k == "scope.only.key" && v == "SV"));

        // Regression: indexed-column mirror keys must NOT appear in either map.
        for mirror in &[
            "service_name",
            "trace_id",
            "span_id",
            "parent_span_id",
            "kind",
            "status_code",
            "name",
        ] {
            assert!(
                !resource_row.iter().any(|(k, _)| k == mirror),
                "mirror key `{mirror}` leaked into resource_attributes"
            );
            assert!(
                !span_row.iter().any(|(k, _)| k == mirror),
                "mirror key `{mirror}` leaked into span_attributes"
            );
        }

        // The indexed top-level columns themselves must still be populated.
        let service_name_col = batch
            .column_by_name("service_name")
            .expect("service_name column")
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("StringArray");
        assert_eq!(service_name_col.value(0), "ingest-test");
    }

    /// Convert row `row` of a `MapArray` into a plain `Vec<(String, String)>`.
    fn map_row_as_pairs(map: &arrow::array::MapArray, row: usize) -> Vec<(String, String)> {
        use arrow::array::Array;
        let entries = map.value(row);
        let entries_struct = entries
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("map entries struct");
        let keys = entries_struct
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("keys StringArray");
        let values = entries_struct
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("values StringArray");
        (0..keys.len())
            .filter(|i| !values.is_null(*i))
            .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
            .collect()
    }
}
