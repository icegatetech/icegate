//! OTLP to Arrow transform utilities.
//!
//! Transforms `OpenTelemetry` Protocol log records to Arrow `RecordBatches`
//! matching the Iceberg logs schema.

use std::sync::Arc;

use arrow::{
    array::{ArrayBuilder, ArrayRef, MapBuilder, MapFieldNames, RecordBatch, StringBuilder, TimestampMicrosecondArray},
    datatypes::{DataType, Schema},
};
use iceberg::arrow::schema_to_arrow_schema;
use icegate_common::DEFAULT_TENANT_ID;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, any_value::Value},
};

/// Returns the Arrow schema for logs, derived from the Iceberg schema.
///
/// Uses `icegate_common::schema::logs_schema()` as the source of truth
/// and converts it to Arrow format using
/// `iceberg::arrow::schema_to_arrow_schema()`.
///
/// # Panics
///
/// Panics if the Iceberg schema cannot be created or converted to Arrow.
/// This should never happen in practice as the schema is statically defined.
#[allow(clippy::expect_used)]
pub fn logs_arrow_schema() -> Schema {
    let iceberg_schema = icegate_common::schema::logs_schema().expect("logs_schema should always be valid");
    schema_to_arrow_schema(&iceberg_schema).expect("schema conversion should succeed")
}

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
    let iceberg_schema = icegate_common::schema::spans_schema().expect("spans_schema should always be valid");
    schema_to_arrow_schema(&iceberg_schema).expect("spans schema conversion should succeed")
}

/// Transforms an OTLP logs export request to an Arrow `RecordBatch`.
///
/// Extracts all log records from the request, merging resource and scope
/// attributes into each log record's attributes.
///
/// # Arguments
///
/// * `request` - The OTLP export logs request
/// * `tenant_id` - Tenant identifier (from request metadata or default)
/// * `account_id` - Optional account identifier
///
/// # Returns
///
/// Arrow `RecordBatch` matching the logs schema wrapped in `Ok(Some(_))`,
/// or `Ok(None)` if no records are present.
///
/// # Errors
///
/// Returns `IngestError` if:
/// - Schema validation fails
/// - `RecordBatch` creation fails
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_possible_truncation)] // Timestamp fits in i64 for practical purposes
#[allow(clippy::expect_used)] // Byte lengths are validated before append
#[tracing::instrument(skip(request))]
pub fn logs_to_record_batch(
    request: &ExportLogsServiceRequest,
    tenant_id: Option<&str>,
) -> crate::error::Result<Option<RecordBatch>> {
    let ingested_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time went backwards")
        .as_micros() as i64;

    // Count total log records for capacity hints
    let total_records: usize = request
        .resource_logs
        .iter()
        .flat_map(|rl| &rl.scope_logs)
        .map(|sl| sl.log_records.len())
        .sum();

    if total_records == 0 {
        return Ok(None);
    }

    // Get the Arrow schema to extract correct field types for map builder
    let schema = logs_arrow_schema();
    let (key_field, value_field) = extract_map_fields_from_schema(&schema)?;

    // Initialize builders
    let mut tenant_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut account_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut service_name_builder = StringBuilder::with_capacity(total_records, total_records * 32);
    let mut timestamp_builder = Vec::with_capacity(total_records);
    let mut observed_timestamp_builder = Vec::with_capacity(total_records);
    let mut ingested_timestamp_builder = Vec::with_capacity(total_records);
    let mut trace_id_builder = StringBuilder::with_capacity(total_records, total_records * 32);
    let mut span_id_builder = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut severity_text_builder = StringBuilder::with_capacity(total_records, total_records * 8);
    let mut body_builder = StringBuilder::with_capacity(total_records, total_records * 256);

    // Create map builder with correct field names matching Iceberg schema
    let field_names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut attributes_builder = MapBuilder::new(Some(field_names), StringBuilder::new(), StringBuilder::new())
        .with_keys_field(key_field)
        .with_values_field(value_field);

    let tenant = tenant_id.unwrap_or(DEFAULT_TENANT_ID);

    // Empty slice for default attributes
    let empty_attrs: Vec<opentelemetry_proto::tonic::common::v1::KeyValue> = Vec::new();

    // Process each resource_logs -> scope_logs -> log_record
    for resource_logs in &request.resource_logs {
        let resource_attrs = resource_logs.resource.as_ref().map_or(&empty_attrs, |r| &r.attributes);

        // Extract service.name from resource attributes
        let service_name = resource_attrs
            .iter()
            .find(|kv| kv.key == "service.name")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));
        let cloud_account_id = resource_attrs
            .iter()
            .find(|kv| kv.key == "cloud.account.id")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for scope_logs in &resource_logs.scope_logs {
            let scope_attrs = scope_logs.scope.as_ref().map_or(&empty_attrs, |s| &s.attributes);

            for log_record in &scope_logs.log_records {
                // tenant_id
                tenant_id_builder.append_value(tenant);

                // account_id (optional - null if not provided)
                if let Some(ref acc) = cloud_account_id {
                    account_id_builder.append_value(acc);
                } else {
                    account_id_builder.append_null();
                }

                // service_name
                if let Some(ref svc) = service_name {
                    service_name_builder.append_value(svc);
                } else {
                    service_name_builder.append_null();
                }

                // timestamp (nanoseconds to microseconds)
                let timestamp_micros = if log_record.time_unix_nano > 0 {
                    (log_record.time_unix_nano / 1000) as i64
                } else {
                    ingested_timestamp
                };
                timestamp_builder.push(timestamp_micros);

                // observed_timestamp
                let observed_micros = if log_record.observed_time_unix_nano > 0 {
                    (log_record.observed_time_unix_nano / 1000) as i64
                } else {
                    ingested_timestamp
                };
                observed_timestamp_builder.push(observed_micros);

                // ingested_timestamp
                ingested_timestamp_builder.push(ingested_timestamp);

                // trace_id and span_id
                let (trace_id_hex, span_id_hex) =
                    process_trace_span_ids(log_record, &mut trace_id_builder, &mut span_id_builder);

                // severity_text
                if log_record.severity_text.is_empty() {
                    severity_text_builder.append_null();
                } else {
                    severity_text_builder.append_value(&log_record.severity_text);
                }

                // body (JSON-serialized for Loki compatibility)
                if let Some(body_str) = serialize_any_value_to_json(log_record.body.as_ref()) {
                    body_builder.append_value(body_str);
                } else {
                    body_builder.append_null();
                }

                // attributes (merged from resource, scope, and log record)
                // Add resource, scope, and log record attributes (flatten nested, normalize keys)
                add_flattened_attributes(resource_attrs, &mut attributes_builder);
                add_flattened_attributes(scope_attrs, &mut attributes_builder);
                add_flattened_attributes(&log_record.attributes, &mut attributes_builder);

                // Duplicate indexed columns into attributes map for query layer
                add_indexed_columns_to_attributes(
                    &mut attributes_builder,
                    trace_id_hex.as_ref(),
                    span_id_hex.as_ref(),
                    &log_record.severity_text,
                    cloud_account_id.as_ref(),
                );

                attributes_builder.append(true).expect("append map entry");
            }
        }
    }

    // Build arrays
    let schema = Arc::new(schema);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_id_builder.finish()),
        Arc::new(account_id_builder.finish()),
        Arc::new(service_name_builder.finish()),
        Arc::new(TimestampMicrosecondArray::from(timestamp_builder)),
        Arc::new(TimestampMicrosecondArray::from(observed_timestamp_builder)),
        Arc::new(TimestampMicrosecondArray::from(ingested_timestamp_builder)),
        Arc::new(trace_id_builder.finish()),
        Arc::new(span_id_builder.finish()),
        Arc::new(severity_text_builder.finish()),
        Arc::new(body_builder.finish()),
        Arc::new(attributes_builder.finish()),
    ];

    RecordBatch::try_new(schema, columns).map(Some).map_err(|e| {
        tracing::error!("Failed to create RecordBatch: {e}");
        crate::error::IngestError::Validation(format!("Failed to create RecordBatch: {e}"))
    })
}

/// Transforms an OTLP traces export request to an Arrow `RecordBatch`.
///
/// Extracts all spans from the request, validating `trace_id` (16 bytes,
/// non-zero) and `span_id` (8 bytes, non-zero) per span. Invalid spans are
/// dropped silently and counted in the second return value so the caller
/// can surface partial-success metrics.
///
/// Produces every column in the spans schema: top-level fields, the merged
/// attributes map (resource + scope + span attributes plus indexed-column
/// duplicates), and the nested `events` / `links` `List<Struct>` arrays.
/// Links with invalid `trace_id` / `span_id` are dropped from the list and
/// counted into the parent span's `dropped_links_count`.
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
    let mut cloud_account_id_builder = StringBuilder::with_capacity(total_spans, total_spans * 16);
    let mut service_name_builder = StringBuilder::with_capacity(total_spans, total_spans * 32);
    let mut trace_id_builder = StringBuilder::with_capacity(total_spans, total_spans * 32);
    let mut span_id_builder = StringBuilder::with_capacity(total_spans, total_spans * 16);
    let mut parent_span_id_builder = StringBuilder::with_capacity(total_spans, total_spans * 16);
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
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>, // trace_id
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>, // span_id
            Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>, // trace_state (nullable)
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
        let cloud_account_id = resource_attrs
            .iter()
            .find(|kv| kv.key == "cloud.account.id")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for scope_spans in &resource_spans.scope_spans {
            for span in &scope_spans.spans {
                if span.trace_id.len() != 16 || is_zero_bytes(&span.trace_id) {
                    drops += 1;
                    continue;
                }
                if span.span_id.len() != 8 || is_zero_bytes(&span.span_id) {
                    drops += 1;
                    continue;
                }

                // Tracks how many links were dropped during transform (invalid
                // trace_id/span_id). Added to span.dropped_links_count so the
                // caller sees a faithful total.
                let mut extra_dropped_links: i32 = 0;

                tenant_id_builder.append_value(tenant);
                match cloud_account_id.as_deref() {
                    Some(acc) => cloud_account_id_builder.append_value(acc),
                    None => cloud_account_id_builder.append_null(),
                }
                match service_name.as_deref() {
                    Some(svc) => service_name_builder.append_value(svc),
                    None => service_name_builder.append_null(),
                }

                trace_id_builder.append_value(hex::encode(&span.trace_id));
                span_id_builder.append_value(hex::encode(&span.span_id));
                if span.parent_span_id.len() == 8 && !is_zero_bytes(&span.parent_span_id) {
                    parent_span_id_builder.append_value(hex::encode(&span.parent_span_id));
                } else {
                    parent_span_id_builder.append_null();
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

                // Resource attributes go to `resource_attributes`.
                add_flattened_attributes_dotted(resource_attrs, &mut resource_attrs_builder);

                // Scope + span attributes fold into `span_attributes`. Arrow
                // `MapBuilder` preserves insertion order and downstream readers
                // resolve duplicate keys within a row as last-write-wins, so
                // writing scope first and span second makes span.* shadow
                // scope.* on any key collision — the spec-mandated behaviour.
                let scope_attrs = scope_spans.scope.as_ref().map_or(&empty_attrs, |s| &s.attributes);
                add_flattened_attributes_dotted(scope_attrs, &mut span_attrs_builder);
                add_flattened_attributes_dotted(&span.attributes, &mut span_attrs_builder);

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
                        if link.trace_id.len() != 16
                            || is_zero_bytes(&link.trace_id)
                            || link.span_id.len() != 8
                            || is_zero_bytes(&link.span_id)
                        {
                            extra_dropped_links += 1;
                            continue;
                        }
                        struct_builder
                            .field_builder::<StringBuilder>(0)
                            .expect("link trace_id builder")
                            .append_value(hex::encode(&link.trace_id));
                        struct_builder
                            .field_builder::<StringBuilder>(1)
                            .expect("link span_id builder")
                            .append_value(hex::encode(&link.span_id));
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
        Arc::new(cloud_account_id_builder.finish()),
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

/// Extracts a string value from an OTLP `AnyValue` reference.
///
/// Converts various OTLP value types to string representation.
fn extract_any_value_string(value: Option<&AnyValue>) -> Option<String> {
    value.and_then(|v| {
        v.value.as_ref().map(|val| match val {
            Value::StringValue(s) => s.clone(),
            Value::IntValue(i) => i.to_string(),
            Value::DoubleValue(d) => d.to_string(),
            Value::BoolValue(b) => b.to_string(),
            Value::BytesValue(b) => hex::encode(b),
            Value::ArrayValue(arr) => {
                let items: Vec<String> = arr.values.iter().filter_map(|v| extract_any_value_string(Some(v))).collect();
                format!("[{}]", items.join(", "))
            }
            Value::KvlistValue(kvs) => {
                let pairs: Vec<String> = kvs
                    .values
                    .iter()
                    .filter_map(|kv| extract_any_value_string(kv.value.as_ref()).map(|v| format!("{}={}", kv.key, v)))
                    .collect();
                format!("{{{}}}", pairs.join(", "))
            }
        })
    })
}

/// Extracts a string value from an `Option<AnyValue>`.
fn extract_string_value(value: Option<&AnyValue>) -> Option<String> {
    extract_any_value_string(value)
}

/// Serializes an OTLP `AnyValue` to JSON string format.
///
/// This is used specifically for the LogRecord.Body field, which should be
/// JSON-serialized according to Loki requirements.
///
/// # Arguments
///
/// * `value` - OTLP `AnyValue` to serialize
///
/// # Returns
///
/// JSON string representation of the value, or None if value is None
///
/// # Examples
///
/// ```
/// // StringValue("hello") -> "hello" (no quotes)
/// // IntValue(42) -> "42"
/// // ArrayValue([1, 2]) -> "[1,2]"
/// // KvlistValue({a: 1}) -> "{\"a\":1}"
/// ```
fn serialize_any_value_to_json(value: Option<&AnyValue>) -> Option<String> {
    value.and_then(|v| {
        v.value.as_ref().and_then(|val| match val {
            Value::StringValue(s) => Some(s.clone()),
            Value::IntValue(i) => Some(i.to_string()),
            Value::DoubleValue(d) => Some(d.to_string()),
            Value::BoolValue(b) => Some(b.to_string()),
            Value::BytesValue(b) => Some(hex::encode(b)),
            Value::ArrayValue(arr) => {
                let json_array: Vec<serde_json::Value> = arr.values.iter().filter_map(any_value_to_json).collect();
                serde_json::to_string(&json_array).ok()
            }
            Value::KvlistValue(kvs) => {
                let mut json_object = serde_json::Map::new();
                for kv in &kvs.values {
                    if let Some(json_val) = kv.value.as_ref().and_then(any_value_to_json) {
                        json_object.insert(kv.key.clone(), json_val);
                    }
                }
                serde_json::to_string(&json_object).ok()
            }
        })
    })
}

/// Helper to convert `AnyValue` to `serde_json::Value` for JSON serialization.
fn any_value_to_json(value: &AnyValue) -> Option<serde_json::Value> {
    value.value.as_ref().map(|val| match val {
        Value::StringValue(s) => serde_json::Value::String(s.clone()),
        Value::IntValue(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
        Value::DoubleValue(d) => {
            serde_json::Number::from_f64(*d).map_or(serde_json::Value::Null, serde_json::Value::Number)
        }
        Value::BoolValue(b) => serde_json::Value::Bool(*b),
        Value::BytesValue(b) => serde_json::Value::String(hex::encode(b)),
        Value::ArrayValue(arr) => {
            let items: Vec<serde_json::Value> = arr.values.iter().filter_map(any_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        Value::KvlistValue(kvs) => {
            let mut map = serde_json::Map::new();
            for kv in &kvs.values {
                if let Some(v) = kv.value.as_ref().and_then(any_value_to_json) {
                    map.insert(kv.key.clone(), v);
                }
            }
            serde_json::Value::Object(map)
        }
    })
}

/// Checks if a byte slice is all zeros.
fn is_zero_bytes(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}

/// Convert an OTLP `u32` counter into the schema's signed `i32` field.
///
/// OTLP represents `flags` and `dropped_*_count` as `u32`. Iceberg stores
/// them as `Int` (i32). Realistic telemetry values sit far below `i32::MAX`,
/// but `as i32` silently wraps for anything above `2^31 - 1`, producing a
/// negative count that later readers would see as "-2 billion dropped
/// events". Fail the transform instead so the caller can surface the
/// malformed span via `partial_success`.
fn u32_count_to_i32(value: u32, context: &'static str) -> crate::error::Result<i32> {
    i32::try_from(value)
        .map_err(|_| crate::error::IngestError::Validation(format!("{context} exceeds i32::MAX: {value}")))
}

/// Extracts map field metadata from the Arrow schema for attributes field.
///
/// Returns a tuple of (`key_field`, `value_field`) from the schema's map type definition.
///
/// # Errors
///
/// Returns `IngestError::Validation` if:
/// - Schema does not contain an 'attributes' field
/// - The 'attributes' field is not of Map type
/// - The map entries are not of Struct type
/// - The struct does not contain at least 2 fields (key and value)
fn extract_map_fields_from_schema(
    schema: &Schema,
) -> crate::error::Result<(arrow::datatypes::FieldRef, arrow::datatypes::FieldRef)> {
    extract_map_fields_from_schema_named(schema, "attributes")
}

/// Extracts map field metadata from the Arrow schema by field name.
///
/// Used by both `logs_to_record_batch` (via `extract_map_fields_from_schema`)
/// and `spans_to_record_batch` (directly).
fn extract_map_fields_from_schema_named(
    schema: &Schema,
    name: &str,
) -> crate::error::Result<(arrow::datatypes::FieldRef, arrow::datatypes::FieldRef)> {
    let field = schema
        .field_with_name(name)
        .map_err(|_| crate::error::IngestError::Validation(format!("Schema must contain a '{name}' field")))?;
    match field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => {
                if fields.len() < 2 {
                    return Err(crate::error::IngestError::Validation(format!(
                        "Expected at least 2 fields in map entries struct for '{name}', found {}",
                        fields.len()
                    )));
                }
                Ok((fields[0].clone(), fields[1].clone()))
            }
            _ => Err(crate::error::IngestError::Validation(format!(
                "Expected Struct type for map entries in '{name}' field"
            ))),
        },
        _ => Err(crate::error::IngestError::Validation(format!(
            "Expected Map type for '{name}' field, found {:?}",
            field.data_type()
        ))),
    }
}

/// Extracts map key/value field refs from a struct's inner field list.
///
/// Used to populate nested `Map` builders inside the events/links struct arrays.
fn extract_map_fields_from_nested_struct(
    fields: &arrow::datatypes::Fields,
    map_field_name: &str,
) -> crate::error::Result<(arrow::datatypes::FieldRef, arrow::datatypes::FieldRef)> {
    let map_field = fields.iter().find(|f| f.name() == map_field_name).ok_or_else(|| {
        crate::error::IngestError::Validation(format!("nested struct missing '{map_field_name}' field"))
    })?;
    match map_field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(inner_fields) => {
                if inner_fields.len() < 2 {
                    return Err(crate::error::IngestError::Validation(format!(
                        "map entries struct for '{map_field_name}' needs 2+ fields, got {}",
                        inner_fields.len()
                    )));
                }
                Ok((inner_fields[0].clone(), inner_fields[1].clone()))
            }
            _ => Err(crate::error::IngestError::Validation(format!(
                "map entries must be Struct for '{map_field_name}'"
            ))),
        },
        _ => Err(crate::error::IngestError::Validation(format!(
            "'{map_field_name}' must be Map"
        ))),
    }
}

/// Adds flattened attributes to the map builder with key normalization.
///
/// Flattens nested structures and normalizes attribute keys by replacing dots with underscores.
fn add_flattened_attributes(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    attributes_builder: &mut MapBuilder<StringBuilder, StringBuilder>,
) {
    for kv in attributes {
        let flattened = flatten_any_value(&kv.key, kv.value.as_ref());
        for (key, value) in flattened {
            attributes_builder.keys().append_value(normalize_attribute_key(&key));
            attributes_builder.values().append_value(value);
        }
    }
}

/// Appends flattened attributes to the map builder using dot-separator keys.
///
/// Used by spans where OTel-native dotted attribute names must be preserved.
fn add_flattened_attributes_dotted(
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    attributes_builder: &mut MapBuilder<StringBuilder, StringBuilder>,
) {
    for kv in attributes {
        for (key, value) in flatten_any_value_dotted(&kv.key, kv.value.as_ref()) {
            attributes_builder.keys().append_value(&key);
            attributes_builder.values().append_value(value);
        }
    }
}

/// Processes trace and span IDs, returning hex-encoded values if valid.
///
/// Returns a tuple of (`trace_id_hex`, `span_id_hex`) where each is `Some(String)` if valid,
/// or `None` if invalid or all zeros.
fn process_trace_span_ids(
    log_record: &opentelemetry_proto::tonic::logs::v1::LogRecord,
    trace_id_builder: &mut StringBuilder,
    span_id_builder: &mut StringBuilder,
) -> (Option<String>, Option<String>) {
    // trace_id (16 bytes → 32 hex chars)
    let trace_id_hex = if log_record.trace_id.len() == 16 && !is_zero_bytes(&log_record.trace_id) {
        let hex_string = hex::encode(&log_record.trace_id);
        trace_id_builder.append_value(hex_string.clone());
        Some(hex_string)
    } else {
        trace_id_builder.append_null();
        None
    };

    // span_id (8 bytes → 16 hex chars)
    let span_id_hex = if log_record.span_id.len() == 8 && !is_zero_bytes(&log_record.span_id) {
        let hex_string = hex::encode(&log_record.span_id);
        span_id_builder.append_value(hex_string.clone());
        Some(hex_string)
    } else {
        span_id_builder.append_null();
        None
    };

    (trace_id_hex, span_id_hex)
}

/// Adds indexed columns to the attributes map for query layer compatibility.
///
/// Duplicates `trace_id`, `span_id`, `severity_text`, and `account_id` into the attributes map
/// to allow queries to use indexed columns for filtering then switch to attributes for operations.
fn add_indexed_columns_to_attributes(
    attributes_builder: &mut MapBuilder<StringBuilder, StringBuilder>,
    trace_id_hex: Option<&String>,
    span_id_hex: Option<&String>,
    severity_text: &str,
    cloud_account_id: Option<&String>,
) {
    // trace_id
    if let Some(tid) = trace_id_hex {
        attributes_builder.keys().append_value("trace_id");
        attributes_builder.values().append_value(tid);
    }

    // span_id
    if let Some(sid) = span_id_hex {
        attributes_builder.keys().append_value("span_id");
        attributes_builder.values().append_value(sid);
    }

    // severity_text
    if !severity_text.is_empty() {
        attributes_builder.keys().append_value("severity_text");
        attributes_builder.values().append_value(severity_text);
    }

    // level (alias for severity_text for Grafana compatibility)
    if !severity_text.is_empty() {
        attributes_builder.keys().append_value("level"); // Loki support
        attributes_builder.values().append_value(severity_text);
    }

    // cloud_account_id
    if let Some(acc) = cloud_account_id {
        attributes_builder.keys().append_value("cloud_account_id");
        attributes_builder.values().append_value(acc);
    }
}

/// Recursively flattens an OTLP `AnyValue` into key-value pairs.
///
/// Nested `KvlistValue` structures are flattened using underscore separator,
/// matching Loki's behavior with JSON parser.
///
/// # Arguments
///
/// * `prefix` - Key prefix for nested values (use empty string for root)
/// * `value` - OTLP `AnyValue` to flatten
///
/// # Returns
///
/// Vector of (key, value) string pairs representing flattened structure
///
/// # Examples
///
/// ```
/// // Input: prefix="http", value=KvlistValue({method: "GET", details: {code: 200}})
/// // Output: [("http_method", "GET"), ("http_details_code", "200")]
/// ```
fn flatten_any_value(prefix: &str, value: Option<&AnyValue>) -> Vec<(String, String)> {
    let mut result = Vec::new();

    if let Some(v) = value {
        if let Some(val) = &v.value {
            match val {
                Value::KvlistValue(kvs) => {
                    // Recursively flatten nested key-value lists
                    for kv in &kvs.values {
                        let nested_prefix = if prefix.is_empty() {
                            kv.key.clone()
                        } else {
                            format!("{}_{}", prefix, kv.key)
                        };
                        let nested_pairs = flatten_any_value(&nested_prefix, kv.value.as_ref());
                        result.extend(nested_pairs);
                    }
                }
                Value::ArrayValue(arr) => {
                    // Arrays are stringified, not flattened (no indexable keys)
                    let items: Vec<String> =
                        arr.values.iter().filter_map(|v| extract_any_value_string(Some(v))).collect();
                    let stringified = format!("[{}]", items.join(", "));
                    result.push((prefix.to_string(), stringified));
                }
                _ => {
                    // Primitive types: stringify and return
                    if let Some(s) = extract_any_value_string(Some(v)) {
                        result.push((prefix.to_string(), s));
                    }
                }
            }
        }
    }

    result
}

/// Flattens an OTLP `AnyValue` into dotted key-value pairs.
///
/// Mirrors [`flatten_any_value`] but joins nested `KvlistValue` keys with a
/// dot (`.`) separator instead of underscore. Use this for spans and other
/// signals where OTel-native dotted attribute names must be preserved.
///
/// # Arguments
///
/// * `prefix` - key prefix for nested values (empty string at the root).
/// * `value` - OTLP `AnyValue` to flatten.
///
/// # Returns
///
/// Vector of (key, value) string pairs representing the flattened structure.
/// Primitive values yield a single pair `(prefix, stringified_value)`.
/// Arrays are stringified (not flattened) since they have no indexable keys.
fn flatten_any_value_dotted(prefix: &str, value: Option<&AnyValue>) -> Vec<(String, String)> {
    let mut result = Vec::new();
    let Some(v) = value else {
        return result;
    };
    let Some(val) = &v.value else {
        return result;
    };

    match val {
        Value::KvlistValue(kvs) => {
            for kv in &kvs.values {
                let nested_prefix = if prefix.is_empty() {
                    kv.key.clone()
                } else {
                    format!("{prefix}.{}", kv.key)
                };
                result.extend(flatten_any_value_dotted(&nested_prefix, kv.value.as_ref()));
            }
        }
        Value::ArrayValue(arr) => {
            let items: Vec<String> = arr.values.iter().filter_map(|v| extract_any_value_string(Some(v))).collect();
            result.push((prefix.to_string(), format!("[{}]", items.join(", "))));
        }
        _ => {
            if let Some(s) = extract_any_value_string(Some(v)) {
                result.push((prefix.to_string(), s));
            }
        }
    }

    result
}

/// Normalizes an attribute key by replacing dots with underscores.
///
/// `OpenTelemetry` uses dots in attribute names (e.g., "service.name"),
/// but some systems prefer underscores for compatibility.
fn normalize_attribute_key(key: &str) -> String {
    key.replace('.', "_")
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::{
        common::v1::KeyValue,
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };

    use super::*;

    #[test]
    fn test_logs_to_record_batch_empty() {
        let request = ExportLogsServiceRequest { resource_logs: vec![] };

        let batch = logs_to_record_batch(&request, None).expect("should not error");
        assert!(batch.is_none());
    }

    #[test]
    fn test_logs_to_record_batch_single_record() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        observed_time_unix_nano: 1_700_000_000_000_000_000,
                        severity_number: 9, // INFO
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("Test log message".to_string())),
                        }),
                        attributes: vec![KeyValue {
                            key: "custom.attr".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("custom-value".to_string())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0; 16],
                        span_id: vec![0; 8],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let batch = logs_to_record_batch(&request, Some("test-tenant")).expect("should not error");
        assert!(batch.is_some());

        let batch = batch.expect("batch should exist");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 11);
    }

    #[test]
    fn test_normalize_attribute_key() {
        assert_eq!(normalize_attribute_key("service.name"), "service_name");
        assert_eq!(normalize_attribute_key("cloud.account.id"), "cloud_account_id");
        assert_eq!(normalize_attribute_key("no_dots"), "no_dots");
        assert_eq!(normalize_attribute_key(""), "");
    }

    #[test]
    fn test_extract_string_value_types() {
        // String
        let v = AnyValue {
            value: Some(Value::StringValue("hello".to_string())),
        };
        assert_eq!(extract_string_value(Some(&v)), Some("hello".to_string()));

        // Int
        let v = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        assert_eq!(extract_string_value(Some(&v)), Some("42".to_string()));

        // Bool
        let v = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert_eq!(extract_string_value(Some(&v)), Some("true".to_string()));

        // None
        assert_eq!(extract_string_value(None), None);
    }

    #[test]
    fn test_body_json_serialization_primitives() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        // String - returned as-is
        let string_val = AnyValue {
            value: Some(Value::StringValue("hello world".to_string())),
        };
        assert_eq!(
            serialize_any_value_to_json(Some(&string_val)),
            Some("hello world".to_string())
        );

        // Int - stringified
        let int_val = AnyValue {
            value: Some(Value::IntValue(42)),
        };
        assert_eq!(serialize_any_value_to_json(Some(&int_val)), Some("42".to_string()));

        // Bool - stringified
        let bool_val = AnyValue {
            value: Some(Value::BoolValue(true)),
        };
        assert_eq!(serialize_any_value_to_json(Some(&bool_val)), Some("true".to_string()));
    }

    #[test]
    fn test_body_json_serialization_array() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, any_value::Value};

        let array_val = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("tag1".to_string())),
                    },
                    AnyValue {
                        value: Some(Value::IntValue(123)),
                    },
                ],
            })),
        };

        let result = serialize_any_value_to_json(Some(&array_val));
        assert!(result.is_some());

        // Should be valid JSON array
        let parsed: serde_json::Value =
            serde_json::from_str(&result.expect("result should exist")).expect("should parse as JSON");
        assert!(parsed.is_array());
        assert_eq!(parsed[0], "tag1");
        assert_eq!(parsed[1], 123);
    }

    #[test]
    fn test_body_json_serialization_object() {
        use opentelemetry_proto::tonic::common::v1::{KeyValueList, any_value::Value};

        let object_val = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "status".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::IntValue(200)),
                        }),
                    },
                    KeyValue {
                        key: "message".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("OK".to_string())),
                        }),
                    },
                ],
            })),
        };

        let result = serialize_any_value_to_json(Some(&object_val));
        assert!(result.is_some());

        // Should be valid JSON object
        let parsed: serde_json::Value =
            serde_json::from_str(&result.expect("result should exist")).expect("should parse as JSON");
        assert!(parsed.is_object());
        assert_eq!(parsed["status"], 200);
        assert_eq!(parsed["message"], "OK");
    }

    #[test]
    fn test_flatten_nested_attributes() {
        use std::collections::HashMap;

        use opentelemetry_proto::tonic::common::v1::{KeyValueList, any_value::Value};

        // Test nested KvlistValue flattening
        let nested_kv = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "method".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("POST".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "details".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![
                                    KeyValue {
                                        key: "status".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::IntValue(200)),
                                        }),
                                    },
                                    KeyValue {
                                        key: "path".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue("/api/v1".to_string())),
                                        }),
                                    },
                                ],
                            })),
                        }),
                    },
                ],
            })),
        };

        let flattened = flatten_any_value("http", Some(&nested_kv));

        // Should produce 3 flattened entries
        assert_eq!(flattened.len(), 3);

        // Check flattened keys and values
        let map: HashMap<String, String> = flattened.into_iter().collect();
        assert_eq!(map.get("http_method"), Some(&"POST".to_string()));
        assert_eq!(map.get("http_details_status"), Some(&"200".to_string()));
        assert_eq!(map.get("http_details_path"), Some(&"/api/v1".to_string()));
    }

    #[test]
    fn test_flatten_array_attribute() {
        use opentelemetry_proto::tonic::common::v1::{ArrayValue, any_value::Value};

        // Arrays should be stringified, not flattened (no meaningful keys)
        let array_value = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::StringValue("tag1".to_string())),
                    },
                    AnyValue {
                        value: Some(Value::StringValue("tag2".to_string())),
                    },
                ],
            })),
        };

        let flattened = flatten_any_value("tags", Some(&array_value));

        // Should produce single stringified entry
        assert_eq!(flattened.len(), 1);
        assert_eq!(flattened[0].0, "tags");
        assert_eq!(flattened[0].1, "[tag1, tag2]");
    }

    #[test]
    fn test_flatten_primitive_attribute() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        // Primitive values should work as before
        let string_value = AnyValue {
            value: Some(Value::StringValue("simple".to_string())),
        };

        let flattened = flatten_any_value("key", Some(&string_value));

        assert_eq!(flattened.len(), 1);
        assert_eq!(flattened[0].0, "key");
        assert_eq!(flattened[0].1, "simple");
    }

    #[test]
    fn flatten_nested_attributes_with_dot_separator() {
        use std::collections::HashMap;

        use opentelemetry_proto::tonic::common::v1::{KeyValueList, any_value::Value};

        let nested_kv = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "method".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("POST".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "details".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::KvlistValue(KeyValueList {
                                values: vec![KeyValue {
                                    key: "status".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(Value::IntValue(200)),
                                    }),
                                }],
                            })),
                        }),
                    },
                ],
            })),
        };

        let flattened = flatten_any_value_dotted("http", Some(&nested_kv));
        let map: HashMap<String, String> = flattened.into_iter().collect();

        assert_eq!(map.get("http.method"), Some(&"POST".to_string()));
        assert_eq!(map.get("http.details.status"), Some(&"200".to_string()));
    }

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
            .downcast_ref::<arrow::array::StringArray>()
            .expect("utf8");
        assert_eq!(trace_id.value(0), hex::encode(vec![1u8; 16]));

        let span_id = batch
            .column_by_name("span_id")
            .expect("span_id")
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("utf8");
        assert_eq!(span_id.value(0), hex::encode(vec![2u8; 8]));

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
        for mirror in [
            "service_name",
            "cloud_account_id",
            "trace_id",
            "span_id",
            "parent_span_id",
            "kind",
            "name",
        ] {
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
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(trace_ids.value(0), hex::encode(vec![9u8; 16]));

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
            "cloud_account_id",
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
