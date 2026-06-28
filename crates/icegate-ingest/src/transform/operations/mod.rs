//! OTLP traces -> `operations` Arrow projection.
//!
//! `operations` is a typed columnar projection over the LLM/GenAI-flavoured
//! subset of trace spans (TRI-72). The materialization is split into a pure,
//! side-effect-free driver ([`projection::project_operation_row`]) over a
//! precedence-ordered registry of per-SDK convention adapters
//! ([`convention::CONVENTIONS`]), plus the thin Arrow driver below.

mod convention;
mod openinference;
mod otel;
mod projection;
mod traceloop;

use std::sync::{Arc, OnceLock};

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Array, Int32Builder, Int64Array, ListBuilder, RecordBatch,
    StringBuilder, TimestampMicrosecondArray,
};
use arrow::datatypes::Schema;
use iceberg::arrow::schema_to_arrow_schema;

use self::projection::{OperationRow, project_operation_row};
use super::attributes::{extract_string_value, list_element_field, now_micros};

/// Returns the Arrow schema for `operations`, derived from the Iceberg schema.
///
/// Uses `icegate_common::schema::operations_schema()` as the source of truth
/// and converts it via `iceberg::arrow::schema_to_arrow_schema()`.
///
/// # Panics
///
/// Panics only if the statically-defined Iceberg schema cannot be created or
/// converted to Arrow, which cannot happen in practice.
#[allow(clippy::expect_used)]
pub fn operations_arrow_schema() -> Schema {
    // Cached: the schema is static, but rebuilding 60+ Iceberg fields and
    // converting to Arrow on every request is wasted work. Cloning the cached
    // `Schema` is cheap because Arrow `Fields` are `Arc`-backed.
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let iceberg_schema =
                icegate_common::schema::operations_schema().expect("operations_schema should always be valid");
            schema_to_arrow_schema(&iceberg_schema).expect("operations schema conversion should succeed")
        })
        .clone()
}

/// Append one optional `Vec<String>` as a NULL-or-populated list entry.
fn append_str_list(builder: &mut ListBuilder<StringBuilder>, value: Option<&Vec<String>>) {
    match value {
        Some(items) => {
            for item in items {
                builder.values().append_value(item);
            }
            builder.append(true);
        }
        None => builder.append_null(),
    }
}

/// Transforms an OTLP traces export request into the `operations` Arrow batch.
///
/// Walks `resource_spans -> scope_spans -> spans`, promoting `service.name`
/// from each resource's attributes, and projects every LLM/GenAI span into one
/// typed operations row via the pure [`project_operation_row`] driver. Non-LLM
/// spans produce no row and are counted (logged as `non_llm_skipped`).
/// Qualifying spans that fail ID validation or strict typed parsing (D6) are
/// dropped and counted in the returned `drops`.
///
/// # Arguments
///
/// * `request` - the OTLP export traces request (same input as the spans path).
/// * `tenant_id` - tenant identifier from request metadata, or the default.
///
/// # Returns
///
/// `(Some(batch), drops)` when at least one operations row is produced, or
/// `(None, drops)` when zero LLM spans yield rows.
///
/// # Errors
///
/// Returns `IngestError` if the ingested-timestamp clock read fails or if
/// `RecordBatch` assembly fails.
#[allow(clippy::too_many_lines)]
// `top_p_b`/`top_k_b` (and peers) intentionally mirror the schema field names
// `top_p`/`top_k`; renaming the builders would diverge from the column source of truth.
#[allow(clippy::similar_names)]
#[tracing::instrument(skip(request))]
pub fn operations_to_record_batch(
    request: &opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    tenant_id: Option<&str>,
) -> crate::error::Result<(Option<RecordBatch>, usize)> {
    let total_spans: usize = request
        .resource_spans
        .iter()
        .flat_map(|rs| &rs.scope_spans)
        .map(|ss| ss.spans.len())
        .sum();

    if total_spans == 0 {
        return Ok((None, 0));
    }

    let tenant = tenant_id.unwrap_or(icegate_common::DEFAULT_TENANT_ID);
    let ingested_at = now_micros()?;
    let schema = operations_arrow_schema();

    let mut rows: Vec<OperationRow> = Vec::with_capacity(total_spans);
    let mut drops: usize = 0;
    let mut non_llm_skipped: usize = 0;

    let empty_attrs: Vec<opentelemetry_proto::tonic::common::v1::KeyValue> = Vec::new();
    // TODO(low): this is a second full walk of every span, independent of the
    // `spans_to_record_batch` pass. Fusing both into one iteration would drop the
    // duplicate walk, but re-serialize the two CPU passes the ingest handlers now
    // run on separate blocking threads (see `write_traces_with_operations_to_wal`).
    // Revisit if this walk becomes a measurable ingest hot-path cost.
    for resource_spans in &request.resource_spans {
        let resource_attrs = resource_spans.resource.as_ref().map_or(&empty_attrs, |r| &r.attributes);
        let service_name = resource_attrs
            .iter()
            .find(|kv| kv.key == "service.name")
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for scope_spans in &resource_spans.scope_spans {
            let scope = scope_spans.scope.as_ref();
            for span in &scope_spans.spans {
                match project_operation_row(span, scope, tenant, service_name.as_deref(), ingested_at) {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => non_llm_skipped += 1,
                    Err(error) => {
                        tracing::debug!(%error, "Dropping operations row (strict projection failure)");
                        drops += 1;
                    }
                }
            }
        }
    }

    if non_llm_skipped > 0 {
        tracing::debug!(
            non_llm_skipped,
            llm_rows = rows.len(),
            "Skipped non-LLM spans during operations projection"
        );
    }

    let row_count = rows.len();
    if row_count == 0 {
        return Ok((None, drops));
    }

    let mut tenant_id_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut trace_id_b = FixedSizeBinaryBuilder::with_capacity(row_count, 16);
    let mut span_id_b = FixedSizeBinaryBuilder::with_capacity(row_count, 8);
    let mut parent_span_id_b = FixedSizeBinaryBuilder::with_capacity(row_count, 8);
    let mut service_name_b = StringBuilder::with_capacity(row_count, row_count * 32);
    let mut scope_name_b = StringBuilder::with_capacity(row_count, row_count * 32);
    let mut scope_version_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut timestamp_b: Vec<i64> = Vec::with_capacity(row_count);
    let mut end_timestamp_b: Vec<i64> = Vec::with_capacity(row_count);
    let mut duration_micros_b: Vec<i64> = Vec::with_capacity(row_count);
    let mut ingested_timestamp_b: Vec<i64> = Vec::with_capacity(row_count);
    let mut operation_name_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut provider_name_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut request_model_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut response_model_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut response_id_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut temperature_b: Vec<Option<f64>> = Vec::with_capacity(row_count);
    let mut top_p_b: Vec<Option<f64>> = Vec::with_capacity(row_count);
    let mut top_k_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut max_tokens_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut frequency_penalty_b: Vec<Option<f64>> = Vec::with_capacity(row_count);
    let mut presence_penalty_b: Vec<Option<f64>> = Vec::with_capacity(row_count);
    let mut seed_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut stream_b = BooleanBuilder::with_capacity(row_count);
    let mut choice_count_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut output_type_b = StringBuilder::with_capacity(row_count, row_count * 8);
    let mut reasoning_effort_b = StringBuilder::with_capacity(row_count, row_count * 8);
    let mut time_to_first_chunk_ms_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut input_tokens_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut output_tokens_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut total_tokens_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut reasoning_tokens_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut cache_creation_input_tokens_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut cache_read_input_tokens_b: Vec<Option<i64>> = Vec::with_capacity(row_count);
    let mut conversation_id_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut user_id_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut tool_name_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut tool_call_id_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut tool_type_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut tool_description_b = StringBuilder::with_capacity(row_count, row_count * 32);
    let mut data_source_id_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut embedding_dimensions_b = Int32Builder::with_capacity(row_count);
    let mut server_address_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut server_port_b = Int32Builder::with_capacity(row_count);
    let mut status_code_b = Int32Builder::with_capacity(row_count);
    let mut status_message_b = StringBuilder::with_capacity(row_count, row_count * 32);
    let mut error_type_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut agent_id_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut agent_name_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut agent_version_b = StringBuilder::with_capacity(row_count, row_count * 8);
    let mut agent_description_b = StringBuilder::with_capacity(row_count, row_count * 32);
    let mut workflow_name_b = StringBuilder::with_capacity(row_count, row_count * 16);
    let mut input_messages_b = StringBuilder::with_capacity(row_count, row_count * 64);
    let mut output_messages_b = StringBuilder::with_capacity(row_count, row_count * 64);
    let mut system_instructions_b = StringBuilder::with_capacity(row_count, row_count * 64);
    let mut tool_definitions_b = StringBuilder::with_capacity(row_count, row_count * 64);
    let mut tool_call_arguments_b = StringBuilder::with_capacity(row_count, row_count * 64);
    let mut tool_call_result_b = StringBuilder::with_capacity(row_count, row_count * 64);
    let stop_sequences_elem = list_element_field(&schema, "stop_sequences")?;
    let finish_reasons_elem = list_element_field(&schema, "finish_reasons")?;
    let encoding_formats_elem = list_element_field(&schema, "encoding_formats")?;
    let mut stop_sequences_b = ListBuilder::new(StringBuilder::new()).with_field(stop_sequences_elem);
    let mut finish_reasons_b = ListBuilder::new(StringBuilder::new()).with_field(finish_reasons_elem);
    let mut encoding_formats_b = ListBuilder::new(StringBuilder::new()).with_field(encoding_formats_elem);

    for row in &rows {
        trace_id_b.append_value(row.trace_id)?;
        span_id_b.append_value(row.span_id)?;
        match row.parent_span_id {
            Some(parent) => parent_span_id_b.append_value(parent)?,
            None => parent_span_id_b.append_null(),
        }
        tenant_id_b.append_value(&row.tenant_id);
        append_opt_str(&mut service_name_b, row.service_name.as_deref());
        append_opt_str(&mut scope_name_b, row.scope_name.as_deref());
        append_opt_str(&mut scope_version_b, row.scope_version.as_deref());
        timestamp_b.push(row.timestamp);
        end_timestamp_b.push(row.end_timestamp);
        duration_micros_b.push(row.duration_micros);
        ingested_timestamp_b.push(row.ingested_timestamp);
        operation_name_b.append_value(&row.operation_name);
        append_opt_str(&mut provider_name_b, row.provider_name.as_deref());
        append_opt_str(&mut request_model_b, row.request_model.as_deref());
        append_opt_str(&mut response_model_b, row.response_model.as_deref());
        append_opt_str(&mut response_id_b, row.response_id.as_deref());
        temperature_b.push(row.temperature);
        top_p_b.push(row.top_p);
        top_k_b.push(row.top_k);
        max_tokens_b.push(row.max_tokens);
        frequency_penalty_b.push(row.frequency_penalty);
        presence_penalty_b.push(row.presence_penalty);
        seed_b.push(row.seed);
        match row.stream {
            Some(value) => stream_b.append_value(value),
            None => stream_b.append_null(),
        }
        choice_count_b.push(row.choice_count);
        append_opt_str(&mut output_type_b, row.output_type.as_deref());
        append_opt_str(&mut reasoning_effort_b, row.reasoning_effort.as_deref());
        time_to_first_chunk_ms_b.push(row.time_to_first_chunk_ms);
        input_tokens_b.push(row.input_tokens);
        output_tokens_b.push(row.output_tokens);
        total_tokens_b.push(row.total_tokens);
        reasoning_tokens_b.push(row.reasoning_tokens);
        cache_creation_input_tokens_b.push(row.cache_creation_input_tokens);
        cache_read_input_tokens_b.push(row.cache_read_input_tokens);
        append_opt_str(&mut conversation_id_b, row.conversation_id.as_deref());
        append_opt_str(&mut user_id_b, row.user_id.as_deref());
        append_opt_str(&mut tool_name_b, row.tool_name.as_deref());
        append_opt_str(&mut tool_call_id_b, row.tool_call_id.as_deref());
        append_opt_str(&mut tool_type_b, row.tool_type.as_deref());
        append_opt_str(&mut tool_description_b, row.tool_description.as_deref());
        append_opt_str(&mut data_source_id_b, row.data_source_id.as_deref());
        match row.embedding_dimensions {
            Some(value) => embedding_dimensions_b.append_value(value),
            None => embedding_dimensions_b.append_null(),
        }
        append_opt_str(&mut server_address_b, row.server_address.as_deref());
        match row.server_port {
            Some(value) => server_port_b.append_value(value),
            None => server_port_b.append_null(),
        }
        match row.status_code {
            Some(value) => status_code_b.append_value(value),
            None => status_code_b.append_null(),
        }
        append_opt_str(&mut status_message_b, row.status_message.as_deref());
        append_opt_str(&mut error_type_b, row.error_type.as_deref());
        append_opt_str(&mut agent_id_b, row.agent_id.as_deref());
        append_opt_str(&mut agent_name_b, row.agent_name.as_deref());
        append_opt_str(&mut agent_version_b, row.agent_version.as_deref());
        append_opt_str(&mut agent_description_b, row.agent_description.as_deref());
        append_opt_str(&mut workflow_name_b, row.workflow_name.as_deref());
        append_opt_str(&mut input_messages_b, row.input_messages.as_deref());
        append_opt_str(&mut output_messages_b, row.output_messages.as_deref());
        append_opt_str(&mut system_instructions_b, row.system_instructions.as_deref());
        append_opt_str(&mut tool_definitions_b, row.tool_definitions.as_deref());
        append_opt_str(&mut tool_call_arguments_b, row.tool_call_arguments.as_deref());
        append_opt_str(&mut tool_call_result_b, row.tool_call_result.as_deref());
        append_str_list(&mut stop_sequences_b, row.stop_sequences.as_ref());
        append_str_list(&mut finish_reasons_b, row.finish_reasons.as_ref());
        append_str_list(&mut encoding_formats_b, row.encoding_formats.as_ref());
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant_id_b.finish()),
        Arc::new(conversation_id_b.finish()),
        Arc::new(trace_id_b.finish()),
        Arc::new(span_id_b.finish()),
        Arc::new(parent_span_id_b.finish()),
        Arc::new(service_name_b.finish()),
        Arc::new(scope_name_b.finish()),
        Arc::new(scope_version_b.finish()),
        Arc::new(TimestampMicrosecondArray::from(timestamp_b)),
        Arc::new(TimestampMicrosecondArray::from(end_timestamp_b)),
        Arc::new(Int64Array::from(duration_micros_b)),
        Arc::new(TimestampMicrosecondArray::from(ingested_timestamp_b)),
        Arc::new(operation_name_b.finish()),
        Arc::new(provider_name_b.finish()),
        Arc::new(request_model_b.finish()),
        Arc::new(response_model_b.finish()),
        Arc::new(response_id_b.finish()),
        Arc::new(Float64Array::from(temperature_b)),
        Arc::new(Float64Array::from(top_p_b)),
        Arc::new(Int64Array::from(top_k_b)),
        Arc::new(Int64Array::from(max_tokens_b)),
        Arc::new(Float64Array::from(frequency_penalty_b)),
        Arc::new(Float64Array::from(presence_penalty_b)),
        Arc::new(Int64Array::from(seed_b)),
        Arc::new(stream_b.finish()),
        Arc::new(Int64Array::from(choice_count_b)),
        Arc::new(output_type_b.finish()),
        Arc::new(reasoning_effort_b.finish()),
        Arc::new(Int64Array::from(time_to_first_chunk_ms_b)),
        Arc::new(Int64Array::from(input_tokens_b)),
        Arc::new(Int64Array::from(output_tokens_b)),
        Arc::new(Int64Array::from(total_tokens_b)),
        Arc::new(Int64Array::from(reasoning_tokens_b)),
        Arc::new(Int64Array::from(cache_creation_input_tokens_b)),
        Arc::new(Int64Array::from(cache_read_input_tokens_b)),
        Arc::new(user_id_b.finish()),
        Arc::new(tool_name_b.finish()),
        Arc::new(tool_call_id_b.finish()),
        Arc::new(tool_type_b.finish()),
        Arc::new(tool_description_b.finish()),
        Arc::new(data_source_id_b.finish()),
        Arc::new(embedding_dimensions_b.finish()),
        Arc::new(server_address_b.finish()),
        Arc::new(server_port_b.finish()),
        Arc::new(status_code_b.finish()),
        Arc::new(status_message_b.finish()),
        Arc::new(error_type_b.finish()),
        Arc::new(agent_id_b.finish()),
        Arc::new(agent_name_b.finish()),
        Arc::new(agent_version_b.finish()),
        Arc::new(agent_description_b.finish()),
        Arc::new(workflow_name_b.finish()),
        Arc::new(input_messages_b.finish()),
        Arc::new(output_messages_b.finish()),
        Arc::new(system_instructions_b.finish()),
        Arc::new(tool_definitions_b.finish()),
        Arc::new(tool_call_arguments_b.finish()),
        Arc::new(tool_call_result_b.finish()),
        Arc::new(stop_sequences_b.finish()),
        Arc::new(finish_reasons_b.finish()),
        Arc::new(encoding_formats_b.finish()),
    ];

    let batch = RecordBatch::try_new(Arc::new(schema), columns).map_err(|e| {
        tracing::error!("Failed to create operations RecordBatch: {e}");
        crate::error::IngestError::Validation(format!("Failed to create operations RecordBatch: {e}"))
    })?;

    Ok((Some(batch), drops))
}

/// Append an `Option<&str>` to a `StringBuilder`, mapping `None` to a NULL.
fn append_opt_str(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(s) => builder.append_value(s),
        None => builder.append_null(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, ListArray, StringArray};
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

    use super::operations_to_record_batch;

    fn kv_str(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
        }
    }

    fn kv_int(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(value)),
            }),
        }
    }

    fn kv_dbl(key: &str, value: f64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::DoubleValue(value)),
            }),
        }
    }

    fn kv_bool(key: &str, value: bool) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::BoolValue(value)),
            }),
        }
    }

    fn span_with(span_id: u8, attributes: Vec<KeyValue>) -> Span {
        Span {
            trace_id: vec![7u8; 16],
            span_id: vec![span_id; 8],
            parent_span_id: Vec::new(),
            trace_state: String::new(),
            flags: 0,
            name: "op".to_string(),
            kind: 0,
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            attributes,
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: Some(Status {
                message: String::new(),
                code: 1,
            }),
        }
    }

    #[test]
    fn one_llm_and_one_non_llm_span_yields_single_row() {
        let llm = span_with(1, vec![kv_str("gen_ai.operation.name", "chat")]);
        let non_llm = span_with(2, vec![kv_str("http.method", "GET")]);

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![llm, non_llm],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (batch_opt, drops) = operations_to_record_batch(&request, Some("tenant-a")).expect("batch ok");
        let batch = batch_opt.expect("one llm span -> batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(drops, 0);
    }

    #[test]
    fn no_llm_spans_yields_none() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span_with(1, vec![kv_str("http.method", "GET")])],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let (batch_opt, drops) = operations_to_record_batch(&request, Some("t")).expect("ok");
        assert!(batch_opt.is_none());
        assert_eq!(drops, 0);
    }

    #[test]
    fn populated_values_land_in_correctly_named_columns() {
        // Guards the hand-ordered `columns` vec against the schema: a populated
        // value of each wire kind (str / f64 / i64 / bool / list / json) must
        // surface in the column the schema names for it, not a same-typed
        // neighbour. `column_by_name` resolves through the schema, so a builder
        // appended at the wrong position would land the value in the wrong column
        // and fail one of these assertions.
        let span = span_with(
            1,
            vec![
                kv_str("gen_ai.operation.name", "chat"),
                kv_str("gen_ai.provider.name", "openai"),
                kv_str("gen_ai.request.model", "gpt-4o"),
                kv_str("gen_ai.conversation.id", "conv-123"),
                kv_dbl("gen_ai.request.temperature", 0.7),
                kv_int("gen_ai.usage.input_tokens", 12),
                kv_bool("gen_ai.request.stream", true),
                KeyValue {
                    key: "gen_ai.response.finish_reasons".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::ArrayValue(ArrayValue {
                            values: vec![AnyValue {
                                value: Some(Value::StringValue("stop".to_string())),
                            }],
                        })),
                    }),
                },
                kv_str("gen_ai.input.messages", "hello"),
            ],
        );

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (batch_opt, drops) = operations_to_record_batch(&request, Some("tenant-a")).expect("batch ok");
        let batch = batch_opt.expect("one llm span -> batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(drops, 0);

        let str_col = |name: &str| -> String {
            batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name} present"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("column {name} is Utf8"))
                .value(0)
                .to_string()
        };

        assert_eq!(str_col("tenant_id"), "tenant-a");
        assert_eq!(str_col("operation_name"), "chat");
        assert_eq!(str_col("provider_name"), "openai");
        assert_eq!(str_col("request_model"), "gpt-4o");
        assert_eq!(str_col("conversation_id"), "conv-123");

        let temperature = batch
            .column_by_name("temperature")
            .expect("temperature column present")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("temperature is Float64");
        assert!((temperature.value(0) - 0.7).abs() < f64::EPSILON);

        let input_tokens = batch
            .column_by_name("input_tokens")
            .expect("input_tokens column present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("input_tokens is Int64");
        assert_eq!(input_tokens.value(0), 12);

        let stream = batch
            .column_by_name("stream")
            .expect("stream column present")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("stream is Boolean");
        assert!(stream.value(0));

        let finish_reasons = batch
            .column_by_name("finish_reasons")
            .expect("finish_reasons column present")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("finish_reasons is List");
        let first = finish_reasons.value(0);
        let reasons = first.as_any().downcast_ref::<StringArray>().expect("list items are Utf8");
        assert_eq!(reasons.len(), 1);
        assert_eq!(reasons.value(0), "stop");

        // The JSON content column must be populated (faithful serialization of the
        // wire value), not null and not misrouted to another string column.
        let input_messages = batch
            .column_by_name("input_messages")
            .expect("input_messages column present")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("input_messages is Utf8");
        assert!(!input_messages.is_null(0), "input_messages must be populated");
        assert!(input_messages.value(0).contains("hello"));
    }
}
