//! OTLP traces (spans) -> Arrow transform.

use std::sync::{Arc, OnceLock};

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, FixedSizeBinaryBuilder, Int32Builder, MapBuilder, RecordBatch, StringBuilder,
        TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, Schema, TimeUnit},
};
use iceberg::arrow::schema_to_arrow_schema;
use icegate_common::{
    DEFAULT_TENANT_ID,
    schema::{
        COL_ATTRIBUTES, COL_DROPPED_ATTRIBUTES_COUNT, COL_EVENTS, COL_FLAGS, COL_LINKS, COL_NAME,
        COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, COL_SPAN_ATTRIBUTES, COL_SPAN_ID, COL_TIMESTAMP, COL_TRACE_ID,
        COL_TRACE_STATE,
    },
};

use super::attributes::{
    attribute_map_builder, dedupe_dotted_attributes, extract_map_fields_from_nested_struct,
    extract_map_fields_from_schema_named, extract_string_value, is_zero_bytes, now_micros, u32_count_to_i32,
};

/// Process-wide cache of the derived spans Arrow schema.
static SPANS_ARROW_SCHEMA: OnceLock<std::result::Result<Arc<Schema>, String>> = OnceLock::new();

/// Returns the Arrow schema for spans, derived once from the Iceberg spans
/// schema and cached for the lifetime of the process.
///
/// Uses `icegate_common::schema::spans_schema()` as the source of truth and
/// converts it to Arrow via `iceberg::arrow::schema_to_arrow_schema()`. The
/// conversion is memoised because it runs twice per traces request (alongside
/// operations); cloning the cached `Arc` is cheap.
///
/// # Errors
///
/// Returns `IngestError::Validation` if the Iceberg spans schema cannot be built
/// or converted to Arrow. The schema is statically defined, so this does not
/// happen in practice.
pub fn spans_arrow_schema() -> crate::error::Result<Arc<Schema>> {
    match SPANS_ARROW_SCHEMA.get_or_init(|| {
        let iceberg_schema = icegate_common::schema::spans_schema().map_err(|e| e.to_string())?;
        schema_to_arrow_schema(&iceberg_schema).map(Arc::new).map_err(|e| e.to_string())
    }) {
        Ok(schema) => Ok(Arc::clone(schema)),
        Err(message) => Err(crate::error::IngestError::Validation(format!(
            "failed to build spans Arrow schema: {message}"
        ))),
    }
}

/// Error for a `StructBuilder` field slot that the schema says must exist.
///
/// Both the slot order and the slot types come from the schema's own `Fields`
/// (see [`nested_struct_builders`] and [`nested_field_index`]), so a lookup only
/// fails if the schema changed under us. Reporting instead of panicking keeps
/// that from taking down the ingest request path.
fn field_builder_missing(parent_column: &str, field: &str) -> crate::error::IngestError {
    crate::error::IngestError::Validation(format!(
        "'{parent_column}' struct builder is missing the '{field}' field slot"
    ))
}

/// Build one `StructBuilder` slot per field of a nested list-element struct.
///
/// `StructBuilder::new` pairs its `fields` and `builders` arguments by position,
/// so both must come from the same source or a schema reorder silently binds a
/// field to the wrong builder. Deriving the list here from `fields` makes that
/// impossible. Only the Arrow types the spans schema uses inside `events` and
/// `links` are handled; a new type is a schema change that must be taught here.
fn nested_struct_builders(
    fields: &arrow::datatypes::Fields,
    parent_column: &str,
) -> crate::error::Result<Vec<Box<dyn ArrayBuilder>>> {
    fields
        .iter()
        .map(|field| -> crate::error::Result<Box<dyn ArrayBuilder>> {
            Ok(match field.data_type() {
                DataType::Timestamp(TimeUnit::Microsecond, _) => Box::new(TimestampMicrosecondBuilder::new()),
                DataType::Utf8 => Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
                DataType::Int32 => Box::new(Int32Builder::new()),
                DataType::FixedSizeBinary(width) => Box::new(FixedSizeBinaryBuilder::new(*width)),
                DataType::Map(..) => {
                    let (key_field, value_field) = extract_map_fields_from_nested_struct(fields, field.name())?;
                    Box::new(attribute_map_builder(key_field, value_field))
                }
                other => {
                    return Err(crate::error::IngestError::Validation(format!(
                        "'{parent_column}.{}' has unsupported type {other}",
                        field.name()
                    )));
                }
            })
        })
        .collect()
}

/// Position of `field` within a nested list-element struct.
///
/// `StructBuilder` addresses slots by index, so the index must be read from the
/// same `Fields` the builder was constructed from rather than written as a
/// literal that a schema reorder would invalidate.
fn nested_field_index(
    fields: &arrow::datatypes::Fields,
    parent_column: &str,
    field: &str,
) -> crate::error::Result<usize> {
    fields
        .iter()
        .position(|candidate| candidate.name() == field)
        .ok_or_else(|| field_builder_missing(parent_column, field))
}

/// Transforms an OTLP traces export request to an Arrow `RecordBatch`.
///
/// Extracts all spans from the request, validating `trace_id` (16 bytes,
/// non-zero) and `span_id` (8 bytes, non-zero) per span. Invalid spans are
/// dropped silently and counted in the second return value so the caller
/// can surface partial-success metrics.
///
/// Produces every column in the spans schema: top-level fields, one
/// independent map per OTLP level (`resource_attributes`,
/// `scope_attributes`, `span_attributes` — no cross-level folding, so the
/// same key may appear in more than one map), and the nested `events` /
/// `links` `List<Struct>` arrays. Links with invalid `trace_id` / `span_id`
/// are dropped from the list and counted into the parent span's
/// `dropped_links_count`.
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
#[allow(clippy::too_many_lines)]
#[tracing::instrument(skip(request))]
pub fn spans_to_record_batch(
    request: &opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    tenant_id: Option<&str>,
) -> crate::error::Result<(Option<RecordBatch>, usize)> {
    use arrow::array::{Int64Array, ListBuilder, StructBuilder};

    let ingested_timestamp = now_micros()?;

    let total_spans: usize = request
        .resource_spans
        .iter()
        .flat_map(|rs| &rs.scope_spans)
        .map(|ss| ss.spans.len())
        .sum();

    if total_spans == 0 {
        return Ok((None, 0));
    }

    let schema = spans_arrow_schema()?;
    let (resource_attr_key_field, resource_attr_value_field) =
        extract_map_fields_from_schema_named(&schema, COL_RESOURCE_ATTRIBUTES)?;
    let (span_attr_key_field, span_attr_value_field) =
        extract_map_fields_from_schema_named(&schema, COL_SPAN_ATTRIBUTES)?;
    let (scope_attr_key_field, scope_attr_value_field) =
        extract_map_fields_from_schema_named(&schema, COL_SCOPE_ATTRIBUTES)?;

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
    let mut resource_attrs_builder = attribute_map_builder(resource_attr_key_field, resource_attr_value_field);
    let mut span_attrs_builder = attribute_map_builder(span_attr_key_field, span_attr_value_field);
    let mut scope_attrs_builder = attribute_map_builder(scope_attr_key_field, scope_attr_value_field);
    let mut flags_builder = Int32Builder::with_capacity(total_spans);
    let mut dropped_attributes_count_builder = Int32Builder::with_capacity(total_spans);
    let mut dropped_events_count_builder = Int32Builder::with_capacity(total_spans);
    let mut dropped_links_count_builder = Int32Builder::with_capacity(total_spans);

    // Events list<struct> builder. Slot order and slot types both come from
    // `event_struct_fields`, so the schema alone decides the layout.
    let events_field = schema
        .field_with_name(COL_EVENTS)
        .map_err(|error| crate::error::IngestError::Validation(format!("schema is missing '{COL_EVENTS}': {error}")))?
        .clone();
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
    let mut events_builder = ListBuilder::new(StructBuilder::new(
        event_struct_fields.iter().cloned().collect::<Vec<_>>(),
        nested_struct_builders(&event_struct_fields, COL_EVENTS)?,
    ))
    .with_field(events_element_field);

    let event_timestamp_slot = nested_field_index(&event_struct_fields, COL_EVENTS, COL_TIMESTAMP)?;
    let event_name_slot = nested_field_index(&event_struct_fields, COL_EVENTS, COL_NAME)?;
    let event_attributes_slot = nested_field_index(&event_struct_fields, COL_EVENTS, COL_ATTRIBUTES)?;
    let event_dropped_slot = nested_field_index(&event_struct_fields, COL_EVENTS, COL_DROPPED_ATTRIBUTES_COUNT)?;

    // Links list<struct> builder, derived from `link_struct_fields` exactly as
    // the events builder above.
    let links_field = schema
        .field_with_name(COL_LINKS)
        .map_err(|error| crate::error::IngestError::Validation(format!("schema is missing '{COL_LINKS}': {error}")))?
        .clone();
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
    let mut links_builder = ListBuilder::new(StructBuilder::new(
        link_struct_fields.iter().cloned().collect::<Vec<_>>(),
        nested_struct_builders(&link_struct_fields, COL_LINKS)?,
    ))
    .with_field(links_element_field);

    let link_trace_id_slot = nested_field_index(&link_struct_fields, COL_LINKS, COL_TRACE_ID)?;
    let link_span_id_slot = nested_field_index(&link_struct_fields, COL_LINKS, COL_SPAN_ID)?;
    let link_trace_state_slot = nested_field_index(&link_struct_fields, COL_LINKS, COL_TRACE_STATE)?;
    let link_attributes_slot = nested_field_index(&link_struct_fields, COL_LINKS, COL_ATTRIBUTES)?;
    let link_dropped_slot = nested_field_index(&link_struct_fields, COL_LINKS, COL_DROPPED_ATTRIBUTES_COUNT)?;
    let link_flags_slot = nested_field_index(&link_struct_fields, COL_LINKS, COL_FLAGS)?;

    let tenant = tenant_id.unwrap_or(DEFAULT_TENANT_ID);
    let empty_attrs: Vec<opentelemetry_proto::tonic::common::v1::KeyValue> = Vec::new();
    let mut drops: usize = 0;

    for resource_spans in &request.resource_spans {
        let resource_attrs = resource_spans.resource.as_ref().map_or(&empty_attrs, |r| &r.attributes);
        let service_name = resource_attrs
            .iter()
            .find(|kv| kv.key == "service.name")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        // Flattened once per ResourceSpans / ScopeSpans and reused by every
        // span beneath: both inputs are fixed at their own nesting level, so
        // re-flattening them per span would rebuild an identical BTreeMap (and
        // every key/value String in it) for each row of the export.
        let merged_resource_attrs = dedupe_dotted_attributes(resource_attrs);

        for scope_spans in &resource_spans.scope_spans {
            let scope_attrs = scope_spans.scope.as_ref().map_or(&empty_attrs, |s| &s.attributes);
            let merged_scope_attrs = dedupe_dotted_attributes(scope_attrs);

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

                // Resource attributes go to `resource_attributes`. Deduped
                // ahead of the `MapBuilder` for the same reason as the
                // scope+span path below: downstream `MAP<K,V>` readers
                // disagree on duplicate-key resolution, so we collapse to a
                // single entry per key.
                for (key, value) in &merged_resource_attrs {
                    resource_attrs_builder.keys().append_value(key);
                    resource_attrs_builder.values().append_value(value);
                }

                // Scope and span attributes each keep their own column. With no
                // fold there is no precedence to apply: a key present at both
                // levels is stored twice, once per level, and the read path
                // resolves it.
                for (key, value) in &merged_scope_attrs {
                    scope_attrs_builder.keys().append_value(key);
                    scope_attrs_builder.values().append_value(value);
                }
                for (key, value) in &dedupe_dotted_attributes(&span.attributes) {
                    span_attrs_builder.keys().append_value(key);
                    span_attrs_builder.values().append_value(value);
                }

                resource_attrs_builder.append(true).map_err(|error| {
                    crate::error::IngestError::Validation(format!(
                        "failed to append '{COL_RESOURCE_ATTRIBUTES}' map entry: {error}"
                    ))
                })?;
                span_attrs_builder.append(true).map_err(|error| {
                    crate::error::IngestError::Validation(format!(
                        "failed to append '{COL_SPAN_ATTRIBUTES}' map entry: {error}"
                    ))
                })?;
                scope_attrs_builder.append(true).map_err(|error| {
                    crate::error::IngestError::Validation(format!(
                        "failed to append '{COL_SCOPE_ATTRIBUTES}' map entry: {error}"
                    ))
                })?;

                // Events list<struct>: one row per parent span.
                {
                    let struct_builder = events_builder.values();
                    for event in &span.events {
                        struct_builder
                            .field_builder::<TimestampMicrosecondBuilder>(event_timestamp_slot)
                            .ok_or_else(|| field_builder_missing(COL_EVENTS, COL_TIMESTAMP))?
                            .append_value((event.time_unix_nano / 1000) as i64);
                        struct_builder
                            .field_builder::<StringBuilder>(event_name_slot)
                            .ok_or_else(|| field_builder_missing(COL_EVENTS, COL_NAME))?
                            .append_value(&event.name);
                        let attr_b = struct_builder
                            .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(event_attributes_slot)
                            .ok_or_else(|| field_builder_missing(COL_EVENTS, COL_ATTRIBUTES))?;
                        // Deduped for the same reason as the top-level attribute
                        // maps: `flatten_any_value_dotted` can emit one key twice
                        // (duplicate OTLP keys, or a direct `a.b` colliding with a
                        // nested `a` -> `b`), and downstream `MAP<K,V>` readers
                        // disagree on duplicate-key resolution.
                        for (key, value) in &dedupe_dotted_attributes(&event.attributes) {
                            attr_b.keys().append_value(key);
                            attr_b.values().append_value(value);
                        }
                        attr_b.append(true).map_err(|error| {
                            crate::error::IngestError::Validation(format!(
                                "failed to append event attributes map entry: {error}"
                            ))
                        })?;
                        struct_builder
                            .field_builder::<Int32Builder>(event_dropped_slot)
                            .ok_or_else(|| field_builder_missing(COL_EVENTS, COL_DROPPED_ATTRIBUTES_COUNT))?
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
                            .field_builder::<FixedSizeBinaryBuilder>(link_trace_id_slot)
                            .ok_or_else(|| field_builder_missing(COL_LINKS, COL_TRACE_ID))?
                            .append_value(link_trace_id_arr)?;
                        struct_builder
                            .field_builder::<FixedSizeBinaryBuilder>(link_span_id_slot)
                            .ok_or_else(|| field_builder_missing(COL_LINKS, COL_SPAN_ID))?
                            .append_value(link_span_id_arr)?;
                        let ts_b = struct_builder
                            .field_builder::<StringBuilder>(link_trace_state_slot)
                            .ok_or_else(|| field_builder_missing(COL_LINKS, COL_TRACE_STATE))?;
                        if link.trace_state.is_empty() {
                            ts_b.append_null();
                        } else {
                            ts_b.append_value(&link.trace_state);
                        }
                        let attr_b = struct_builder
                            .field_builder::<MapBuilder<StringBuilder, StringBuilder>>(link_attributes_slot)
                            .ok_or_else(|| field_builder_missing(COL_LINKS, COL_ATTRIBUTES))?;
                        // Deduped for the same reason as the top-level attribute
                        // maps: `flatten_any_value_dotted` can emit one key twice
                        // (duplicate OTLP keys, or a direct `a.b` colliding with a
                        // nested `a` -> `b`), and downstream `MAP<K,V>` readers
                        // disagree on duplicate-key resolution.
                        for (key, value) in &dedupe_dotted_attributes(&link.attributes) {
                            attr_b.keys().append_value(key);
                            attr_b.values().append_value(value);
                        }
                        attr_b.append(true).map_err(|error| {
                            crate::error::IngestError::Validation(format!(
                                "failed to append link attributes map entry: {error}"
                            ))
                        })?;
                        struct_builder
                            .field_builder::<Int32Builder>(link_dropped_slot)
                            .ok_or_else(|| field_builder_missing(COL_LINKS, COL_DROPPED_ATTRIBUTES_COUNT))?
                            .append_value(u32_count_to_i32(
                                link.dropped_attributes_count,
                                "link.dropped_attributes_count",
                            )?);
                        let flags_b = struct_builder
                            .field_builder::<Int32Builder>(link_flags_slot)
                            .ok_or_else(|| field_builder_missing(COL_LINKS, COL_FLAGS))?;
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
        Arc::new(scope_attrs_builder.finish()),
    ];

    let batch = RecordBatch::try_new(schema, columns).map_err(|e| {
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
        let schema = spans_arrow_schema().expect("spans arrow schema");
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
                        key_strindex: 0,
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
                            key_strindex: 0,
                            key: "service.name".to_string(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("svc".to_string())),
                            }),
                        },
                        KeyValue {
                            key_strindex: 0,
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
                            key_strindex: 0,
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
                                key_strindex: 0,
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
    fn spans_split_attributes_into_resource_scope_and_span_columns() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        fn kv(k: &str, v: &str) -> KeyValue {
            KeyValue {
                key_strindex: 0,
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

        // Collect the three attribute maps into (key, value) vecs for row 0.
        let resource_attrs_col = batch
            .column_by_name("resource_attributes")
            .expect("resource_attributes column")
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .expect("MapArray");
        let scope_attrs_col = batch
            .column_by_name("scope_attributes")
            .expect("scope_attributes column")
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
        let scope_row = map_row_as_pairs(scope_attrs_col, 0);
        let span_row = map_row_as_pairs(span_attrs_col, 0);

        // Resource-only keys land in resource_attributes.
        assert!(resource_row.iter().any(|(k, v)| k == "service.name" && v == "ingest-test"));
        assert!(resource_row.iter().any(|(k, v)| k == "k8s.namespace.name" && v == "icegate"));

        // Scope-only keys land in scope_attributes now, not span_attributes —
        // the fold is gone, so nothing promotes a scope key onto the span level.
        assert!(scope_row.iter().any(|(k, v)| k == "scope.only.key" && v == "SV"));
        assert!(
            !span_row.iter().any(|(k, _)| k == "scope.only.key"),
            "scope attributes must not leak into span_attributes"
        );

        // Span-only keys land in span_attributes.
        assert!(span_row.iter().any(|(k, v)| k == "http.method" && v == "GET"));
        assert!(span_row.iter().any(|(k, v)| k == "span.only.key" && v == "SVAL"));

        // Regression: indexed-column mirror keys must NOT appear in any map.
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
                !scope_row.iter().any(|(k, _)| k == mirror),
                "mirror key `{mirror}` leaked into scope_attributes"
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

    #[test]
    fn spans_keep_scope_attributes_out_of_span_attributes() {
        let batch = spans_batch_with(
            &[kv("service.name", "api")],
            &[kv("otel.scope.name", "lib"), kv("shared.key", "from-scope")],
            &[kv("http.method", "GET"), kv("shared.key", "from-span")],
        );

        let scope = map_pairs(&batch, "scope_attributes");
        let span = map_pairs(&batch, "span_attributes");

        assert_eq!(scope.get("otel.scope.name").map(String::as_str), Some("lib"));
        assert!(
            !span.contains_key("otel.scope.name"),
            "scope attributes must no longer be folded into span_attributes"
        );

        // Same key at both levels: each keeps its own value, no precedence applied.
        assert_eq!(scope.get("shared.key").map(String::as_str), Some("from-scope"));
        assert_eq!(span.get("shared.key").map(String::as_str), Some("from-span"));
    }

    /// Build a one-span batch: one `ResourceSpans` -> one `ScopeSpans`
    /// (carrying `scope_attrs`) -> one `Span` (carrying `span_attrs`), run
    /// through `spans_to_record_batch`. Shared by tests that assert on the
    /// resulting per-level attribute maps.
    fn spans_batch_with(resource_attrs: &[KeyValue], scope_attrs: &[KeyValue], span_attrs: &[KeyValue]) -> RecordBatch {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: resource_attrs.to_vec(),
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "test-scope".to_string(),
                        version: String::new(),
                        attributes: scope_attrs.to_vec(),
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
                        attributes: span_attrs.to_vec(),
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

        let (batch, _drops) = spans_to_record_batch(&request, Some("t1")).expect("spans transform");
        batch.expect("batch produced")
    }

    // `key_strindex` has no default in the OTLP proto's generated `KeyValue`
    // struct (see the `TODO(otlp-strindex)` note in `attributes.rs`), so
    // every literal in this file sets it explicitly even though these tests
    // only ever exercise the inline `key` string form.
    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key_strindex: 0,
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
        }
    }

    /// The point of deriving both halves from `Fields`: a reordered struct must
    /// still bind every name to a builder of the right type. The previous fixed
    /// 0..N layout would have paired `name` with the timestamp builder here.
    #[test]
    fn nested_struct_builders_follow_field_order_not_a_fixed_layout() {
        use arrow::datatypes::{Field, Fields};

        // Deliberately not the order the spans schema declares.
        let fields = Fields::from(vec![
            Field::new(COL_NAME, DataType::Utf8, false),
            Field::new(COL_DROPPED_ATTRIBUTES_COUNT, DataType::Int32, false),
            Field::new(COL_TIMESTAMP, DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]);

        assert_eq!(nested_field_index(&fields, COL_EVENTS, COL_NAME).expect("name slot"), 0);
        assert_eq!(
            nested_field_index(&fields, COL_EVENTS, COL_DROPPED_ATTRIBUTES_COUNT).expect("dropped slot"),
            1
        );
        assert_eq!(
            nested_field_index(&fields, COL_EVENTS, COL_TIMESTAMP).expect("timestamp slot"),
            2
        );

        let mut builders = nested_struct_builders(&fields, COL_EVENTS).expect("builders");
        assert_eq!(builders.len(), 3);
        assert!(builders[0].as_any_mut().downcast_mut::<StringBuilder>().is_some());
        assert!(builders[1].as_any_mut().downcast_mut::<Int32Builder>().is_some());
        assert!(builders[2].as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>().is_some());
    }

    #[test]
    fn nested_field_index_reports_the_missing_field_by_name() {
        use arrow::datatypes::{Field, Fields};

        let fields = Fields::from(vec![Field::new(COL_NAME, DataType::Utf8, false)]);
        let error = nested_field_index(&fields, COL_LINKS, COL_TRACE_ID).expect_err("must not resolve");
        let message = error.to_string();
        assert!(message.contains(COL_LINKS), "{message}");
        assert!(message.contains(COL_TRACE_ID), "{message}");
    }

    #[test]
    fn nested_struct_builders_rejects_a_type_it_was_not_taught() {
        use arrow::datatypes::{Field, Fields};

        let fields = Fields::from(vec![Field::new("ratio", DataType::Float64, false)]);
        // `Vec<Box<dyn ArrayBuilder>>` is not `Debug`, so `expect_err` is unavailable.
        let Err(error) = nested_struct_builders(&fields, COL_EVENTS) else {
            panic!("an untaught field type must not produce a builder");
        };
        assert!(error.to_string().contains("ratio"), "{error}");
    }

    /// `flatten_any_value_dotted` emits `a.b` twice for this input: once from
    /// the direct `a.b` key, once from flattening the nested `a` -> `b` kvlist.
    /// The nested event and link maps must collapse that to a single entry, as
    /// the top-level attribute maps already do — a `MAP<K,V>` carrying the key
    /// twice is resolved differently by different readers.
    #[test]
    fn nested_event_and_link_attributes_collapse_colliding_dotted_keys() {
        use arrow::array::{ListArray, MapArray, StructArray};
        use opentelemetry_proto::tonic::{
            collector::trace::v1::ExportTraceServiceRequest,
            common::v1::KeyValueList,
            trace::v1::{
                ResourceSpans, ScopeSpans, Span,
                span::{Event, Link},
            },
        };

        // Order matters: the nested form is inserted second, so last-write-wins
        // in the `BTreeMap` makes "nested" the surviving value.
        let colliding = || {
            vec![
                kv("a.b", "direct"),
                KeyValue {
                    key_strindex: 0,
                    key: "a".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::KvlistValue(KeyValueList {
                            values: vec![kv("b", "nested")],
                        })),
                    }),
                },
            ]
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
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
                            name: "evt".to_string(),
                            attributes: colliding(),
                            dropped_attributes_count: 0,
                        }],
                        dropped_events_count: 0,
                        links: vec![Link {
                            trace_id: vec![9u8; 16],
                            span_id: vec![8u8; 8],
                            trace_state: String::new(),
                            attributes: colliding(),
                            dropped_attributes_count: 0,
                            flags: 0,
                        }],
                        dropped_links_count: 0,
                        status: None,
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (batch, _) = spans_to_record_batch(&request, None).expect("spans transform");
        let batch = batch.expect("batch produced");

        for column in [COL_EVENTS, COL_LINKS] {
            let list = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("{column} column"))
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap_or_else(|| panic!("{column} must be a ListArray"));
            let row = list.value(0);
            let entries = row.as_any().downcast_ref::<StructArray>().expect("struct");
            let attrs = entries
                .column_by_name("attributes")
                .expect("attributes")
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("attributes must be a MapArray");

            assert_eq!(
                map_row_as_pairs(attrs, 0),
                vec![("a.b".to_string(), "nested".to_string())],
                "{column}.attributes must carry 'a.b' exactly once"
            );
        }
    }

    /// Collect one row's MAP column into a plain map for assertions.
    fn map_pairs(batch: &RecordBatch, column: &str) -> std::collections::BTreeMap<String, String> {
        use arrow::array::{Array, MapArray, StringArray};
        let arr = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("{column} column"))
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap_or_else(|| panic!("{column} must be MapArray"));
        let entries = arr.value(0);
        let keys = entries.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
        let values = entries.column(1).as_any().downcast_ref::<StringArray>().expect("values");
        (0..keys.len())
            .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
            .collect()
    }
}
