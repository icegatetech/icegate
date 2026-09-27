//! OTLP traces -> `operations` Arrow projection.
//!
//! `operations` is a typed columnar projection over the LLM/GenAI-flavoured
//! subset of trace spans (TRI-72). The materialization is split into a pure,
//! side-effect-free driver ([`projection::project_operation_row`]) over a
//! precedence-ordered registry of per-SDK convention adapters
//! ([`convention::CONVENTIONS`]), plus the thin Arrow driver below.

mod claude_code;
mod convention;
mod openinference;
mod otel;
mod projection;
mod traceloop;

use std::sync::{Arc, OnceLock};

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Array, Float64Builder, Int32Builder, Int64Array,
    ListBuilder, RecordBatch, StringBuilder, StructBuilder, TimestampMicrosecondArray,
};
use arrow::datatypes::{Fields, Schema};
use iceberg::arrow::schema_to_arrow_schema;
use icegate_common::TenantId;
use icegate_common::schema::{
    COL_ANNOTATOR_KIND, COL_ERROR_TYPE, COL_EVALUATION_METADATA, COL_EVALUATIONS, COL_EXPLANATION, COL_IDENTIFIER,
    COL_NAME, COL_RESPONSE_ID, COL_SCORE_LABEL, COL_SCORE_VALUE, COL_TARGET_SCOPE, COL_TARGET_SPAN_ID,
    COL_TARGET_TRACE_ID,
};

use self::projection::{EvaluationResult, OperationRow, project_operation_row};
use super::attributes::{SERVICE_NAME_KEY, extract_string_value, list_element_field, list_struct_fields, now_micros};
use super::nested_builders::{field_builder_missing, nested_field_index, nested_struct_builders};

/// Process-wide cache of the derived operations Arrow schema.
static OPERATIONS_ARROW_SCHEMA: OnceLock<std::result::Result<Arc<Schema>, String>> = OnceLock::new();

/// Returns the Arrow schema for `operations`, derived once from the Iceberg
/// schema and cached for the lifetime of the process.
///
/// Uses `icegate_common::schema::operations_schema()` as the source of truth and
/// converts it via `iceberg::arrow::schema_to_arrow_schema()`. The conversion is
/// memoised because rebuilding 60+ Iceberg fields on every request is wasted
/// work; cloning the cached `Arc` is cheap.
///
/// # Errors
///
/// Returns `IngestError::Validation` if the Iceberg operations schema cannot be
/// built or converted to Arrow. The schema is statically defined, so this does
/// not happen in practice.
pub fn operations_arrow_schema() -> crate::error::Result<Arc<Schema>> {
    match OPERATIONS_ARROW_SCHEMA.get_or_init(|| {
        let iceberg_schema = icegate_common::schema::operations_schema().map_err(|e| e.to_string())?;
        schema_to_arrow_schema(&iceberg_schema).map(Arc::new).map_err(|e| e.to_string())
    }) {
        Ok(schema) => Ok(Arc::clone(schema)),
        Err(message) => Err(crate::error::IngestError::Validation(format!(
            "failed to build operations Arrow schema: {message}"
        ))),
    }
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
    tenant_id: &TenantId,
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

    let ingested_at = now_micros()?;
    let schema = operations_arrow_schema()?;

    let mut rows: Vec<OperationRow> = Vec::with_capacity(total_spans);
    let mut drops: usize = 0;
    let mut non_llm_skipped: usize = 0;
    let mut skipped_evaluations: usize = 0;

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
            .find(|kv| kv.key == SERVICE_NAME_KEY)
            .and_then(|kv| extract_string_value(kv.value.as_ref()));

        for scope_spans in &resource_spans.scope_spans {
            let scope = scope_spans.scope.as_ref();
            for span in &scope_spans.spans {
                match project_operation_row(span, scope, tenant_id, service_name.as_deref(), ingested_at) {
                    Ok(Some(projected)) => {
                        skipped_evaluations += projected.skipped_evaluations;
                        rows.push(projected.row);
                    }
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
    if skipped_evaluations > 0 {
        tracing::debug!(
            skipped_evaluations,
            llm_rows = rows.len(),
            "Skipped incomplete evaluation results during operations projection"
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
    let (evaluations_elem, evaluation_fields) = list_struct_fields(&schema, COL_EVALUATIONS)?;
    let mut evaluations_b = ListBuilder::new(StructBuilder::new(
        evaluation_fields.iter().cloned().collect::<Vec<_>>(),
        nested_struct_builders(&evaluation_fields, COL_EVALUATIONS)?,
    ))
    .with_field(evaluations_elem);
    let evaluation_slots = EvaluationSlots::from_fields(&evaluation_fields)?;

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
        append_evaluations(&mut evaluations_b, &evaluation_slots, row.evaluations.as_ref())?;
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
        Arc::new(evaluations_b.finish()),
    ];

    let batch = RecordBatch::try_new(schema, columns).map_err(|e| {
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

/// Slot indices of the `evaluations` element struct, read from the schema's
/// own `Fields` so a reorder cannot bind a value to the wrong builder.
struct EvaluationSlots {
    name: usize,
    score_value: usize,
    score_label: usize,
    explanation: usize,
    response_id: usize,
    error_type: usize,
    annotator_kind: usize,
    identifier: usize,
    metadata: usize,
    target_scope: usize,
    target_trace_id: usize,
    target_span_id: usize,
}

impl EvaluationSlots {
    /// Resolve every slot by field name.
    ///
    /// # Errors
    ///
    /// Returns `IngestError::Validation` when the schema's element struct lacks
    /// one of the fields — a schema drift, reported rather than panicked on.
    fn from_fields(fields: &Fields) -> crate::error::Result<Self> {
        let slot = |field: &str| nested_field_index(fields, COL_EVALUATIONS, field);
        Ok(Self {
            name: slot(COL_NAME)?,
            score_value: slot(COL_SCORE_VALUE)?,
            score_label: slot(COL_SCORE_LABEL)?,
            explanation: slot(COL_EXPLANATION)?,
            response_id: slot(COL_RESPONSE_ID)?,
            error_type: slot(COL_ERROR_TYPE)?,
            annotator_kind: slot(COL_ANNOTATOR_KIND)?,
            identifier: slot(COL_IDENTIFIER)?,
            metadata: slot(COL_EVALUATION_METADATA)?,
            target_scope: slot(COL_TARGET_SCOPE)?,
            target_trace_id: slot(COL_TARGET_TRACE_ID)?,
            target_span_id: slot(COL_TARGET_SPAN_ID)?,
        })
    }
}

/// Append one optional evaluation list as a NULL-or-populated `List<Struct>`
/// entry, one struct per [`EvaluationResult`].
///
/// # Errors
///
/// Returns `IngestError::Validation` when a struct slot is missing or has an
/// unexpected builder type (schema drift; see [`field_builder_missing`]).
fn append_evaluations(
    builder: &mut ListBuilder<StructBuilder>,
    slots: &EvaluationSlots,
    value: Option<&Vec<EvaluationResult>>,
) -> crate::error::Result<()> {
    let Some(results) = value else {
        builder.append_null();
        return Ok(());
    };
    let struct_builder = builder.values();
    for result in results {
        let mut append_str = |slot: usize, field: &str, text: Option<&str>| -> crate::error::Result<()> {
            struct_builder
                .field_builder::<StringBuilder>(slot)
                .ok_or_else(|| field_builder_missing(COL_EVALUATIONS, field))?
                .append_option(text);
            Ok(())
        };
        append_str(slots.name, COL_NAME, Some(&result.name))?;
        append_str(slots.score_label, COL_SCORE_LABEL, result.score_label.as_deref())?;
        append_str(slots.explanation, COL_EXPLANATION, result.explanation.as_deref())?;
        append_str(slots.response_id, COL_RESPONSE_ID, result.response_id.as_deref())?;
        append_str(slots.error_type, COL_ERROR_TYPE, result.error_type.as_deref())?;
        append_str(
            slots.annotator_kind,
            COL_ANNOTATOR_KIND,
            result.annotator_kind.as_deref(),
        )?;
        append_str(slots.identifier, COL_IDENTIFIER, result.identifier.as_deref())?;
        append_str(slots.metadata, COL_EVALUATION_METADATA, result.metadata.as_deref())?;
        append_str(slots.target_scope, COL_TARGET_SCOPE, Some(result.target_scope.as_str()))?;
        struct_builder
            .field_builder::<Float64Builder>(slots.score_value)
            .ok_or_else(|| field_builder_missing(COL_EVALUATIONS, COL_SCORE_VALUE))?
            .append_option(result.score_value);
        append_fixed_id(
            struct_builder,
            slots.target_trace_id,
            COL_TARGET_TRACE_ID,
            result.target_trace_id.as_ref().map(<[u8; 16]>::as_slice),
        )?;
        append_fixed_id(
            struct_builder,
            slots.target_span_id,
            COL_TARGET_SPAN_ID,
            result.target_span_id.as_ref().map(<[u8; 8]>::as_slice),
        )?;
        struct_builder.append(true);
    }
    builder.append(true);
    Ok(())
}

/// Append one optional fixed-width id to the `evaluations` struct slot `slot`.
///
/// # Errors
///
/// Returns `IngestError::Validation` when the slot is missing or not a
/// fixed-size binary builder, and the Arrow error when `id` does not match
/// the slot's width.
fn append_fixed_id(
    struct_builder: &mut StructBuilder,
    slot: usize,
    field: &str,
    id: Option<&[u8]>,
) -> crate::error::Result<()> {
    let id_builder = struct_builder
        .field_builder::<FixedSizeBinaryBuilder>(slot)
        .ok_or_else(|| field_builder_missing(COL_EVALUATIONS, field))?;
    match id {
        Some(bytes) => id_builder.append_value(bytes)?,
        None => id_builder.append_null(),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
        StructArray,
    };
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span::Event};

    use super::operations_to_record_batch;
    use crate::transform::test_support::test_tenant;

    fn kv_str(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key_strindex: 0,
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
        }
    }

    fn kv_int(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key_strindex: 0,
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(value)),
            }),
        }
    }

    fn kv_dbl(key: &str, value: f64) -> KeyValue {
        KeyValue {
            key_strindex: 0,
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::DoubleValue(value)),
            }),
        }
    }

    fn kv_bool(key: &str, value: bool) -> KeyValue {
        KeyValue {
            key_strindex: 0,
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

    /// Build a `gen_ai.evaluation.result` span event carrying the given attributes.
    fn evaluation_event(attributes: Vec<KeyValue>) -> Event {
        Event {
            time_unix_nano: 1_500_000_000,
            name: "gen_ai.evaluation.result".to_string(),
            attributes,
            dropped_attributes_count: 0,
        }
    }

    fn span_with_events(span_id: u8, attributes: Vec<KeyValue>, events: Vec<Event>) -> Span {
        let mut span = span_with(span_id, attributes);
        span.events = events;
        span
    }

    fn request_with(spans: Vec<Span>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    /// Two LLM spans: one evaluated four times — twice through `OpenInference`
    /// attribute arrays (span and trace scope), twice through OTEL `GenAI`
    /// events — and one not evaluated at all.
    fn evaluated_and_plain_request() -> ExportTraceServiceRequest {
        let evaluated = span_with_events(
            1,
            vec![
                kv_str("gen_ai.operation.name", "chat"),
                kv_str("gen_ai.response.id", "resp-1"),
                kv_str("evaluations.0.evaluation.name", "hallucination"),
                kv_int("evaluations.0.evaluation.score", 1),
                kv_str("evaluations.0.evaluation.annotator_kind", "LLM"),
                kv_str("evaluations.0.evaluation.identifier", "judge-v2"),
                kv_str("evaluations.0.evaluation.metadata", "{\"rubric_version\":\"2\"}"),
                kv_str("trace.evaluations.0.evaluation.name", "retrieval_quality"),
                kv_dbl("trace.evaluations.0.evaluation.score", 0.5),
            ],
            vec![
                evaluation_event(vec![
                    kv_str("gen_ai.evaluation.name", "Relevance"),
                    kv_dbl("gen_ai.evaluation.score.value", 0.9),
                    kv_str("gen_ai.evaluation.score.label", "relevant"),
                ]),
                evaluation_event(vec![
                    kv_str("gen_ai.evaluation.name", "Fluency"),
                    kv_int("gen_ai.evaluation.score.value", 4),
                    kv_str("gen_ai.evaluation.explanation", "reads naturally"),
                    kv_str("gen_ai.response.id", "resp-1"),
                    kv_str("error.type", "timeout"),
                ]),
            ],
        );
        let plain = span_with(2, vec![kv_str("gen_ai.operation.name", "chat")]);
        request_with(vec![evaluated, plain])
    }

    /// Assert the `evaluations` column of `batch` holds the four results of the
    /// evaluated span in row 0 and a NULL list in row 1, every field addressed
    /// by name.
    fn assert_evaluations_column(batch: &RecordBatch) {
        let evaluations = batch
            .column_by_name("evaluations")
            .expect("evaluations column present")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("evaluations is List");
        let first = evaluations.value(0);
        let results = first.as_any().downcast_ref::<StructArray>().expect("elements are Struct");
        assert_eq!(results.len(), 4);
        let strings = |field: &str| -> Vec<Option<String>> {
            results
                .column_by_name(field)
                .unwrap_or_else(|| panic!("element field {field} present"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("element field {field} is Utf8"))
                .iter()
                .map(|value| value.map(str::to_string))
                .collect()
        };
        let text = |value: &str| Some(value.to_string());
        assert_eq!(
            strings("name"),
            vec![
                text("hallucination"),
                text("retrieval_quality"),
                text("Relevance"),
                text("Fluency")
            ]
        );
        assert_eq!(strings("score_label"), vec![None, None, text("relevant"), None]);
        assert_eq!(strings("explanation"), vec![None, None, None, text("reads naturally")]);
        assert_eq!(strings("response_id"), vec![None, None, None, text("resp-1")]);
        assert_eq!(strings("error_type"), vec![None, None, None, text("timeout")]);
        assert_eq!(strings("annotator_kind"), vec![text("LLM"), None, None, None]);
        assert_eq!(strings("identifier"), vec![text("judge-v2"), None, None, None]);
        assert_eq!(
            strings("metadata"),
            vec![text("{\"rubric_version\":\"2\"}"), None, None, None]
        );
        assert_eq!(
            strings("target_scope"),
            vec![text("span"), text("trace"), text("span"), text("span")]
        );
        let scores = results
            .column_by_name("score_value")
            .expect("score_value present")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("score_value is Float64");
        assert_eq!(
            scores.iter().collect::<Vec<_>>(),
            vec![Some(1.0), Some(0.5), Some(0.9), Some(4.0)]
        );
        let ids = |field: &str| -> Vec<Option<Vec<u8>>> {
            results
                .column_by_name(field)
                .unwrap_or_else(|| panic!("element field {field} present"))
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap_or_else(|| panic!("element field {field} is FixedSizeBinary"))
                .iter()
                .map(|value| value.map(<[u8]>::to_vec))
                .collect()
        };
        // Every result is recorded on the span it evaluates; the trace-scoped
        // one names the trace alone.
        let own_trace = Some(vec![7u8; 16]);
        let own_span = Some(vec![1u8; 8]);
        assert_eq!(ids("target_trace_id"), vec![own_trace; 4]);
        assert_eq!(
            ids("target_span_id"),
            vec![own_span.clone(), None, own_span.clone(), own_span]
        );
        assert!(
            evaluations.is_null(1),
            "a span without evaluations is a NULL list, not an empty one"
        );
    }

    #[test]
    fn evaluations_land_as_a_list_of_structs_addressed_by_field_name() {
        // Guards both the position of the new column in the hand-ordered
        // `columns` vec and the slot binding inside the element struct.
        let (batch_opt, drops) =
            operations_to_record_batch(&evaluated_and_plain_request(), &test_tenant("tenant-a")).expect("batch ok");
        let batch = batch_opt.expect("two llm spans -> batch");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(drops, 0);
        assert_evaluations_column(&batch);
    }

    #[test]
    fn evaluations_round_trip_through_the_production_parquet_writer_with_iceberg_field_ids() {
        // Schema-change contract (docs/tests.md): the production writer
        // properties and a real Parquet reader must agree on the nested column,
        // and the Iceberg field ids must survive into the file — the shift
        // writer and every Iceberg reader bind columns by id, not by name.
        use bytes::Bytes;
        use parquet::arrow::ArrowWriter;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let (batch_opt, _) =
            operations_to_record_batch(&evaluated_and_plain_request(), &test_tenant("tenant-a")).expect("batch ok");
        let batch = batch_opt.expect("batch");

        let write_config = crate::shift::config::ShiftWriteConfig::default();
        let properties = icegate_common::parquet_writer::build_writer_properties(
            write_config.row_group_size,
            write_config.data_page_size_limit_bytes,
            icegate_common::parquet_encoding::OPERATIONS_BLOOM_COLUMNS,
            icegate_common::parquet_encoding::OPERATIONS_COLUMN_ENCODINGS,
        );
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), Some(properties)).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
        let bytes = Bytes::from(file);

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .expect("reader")
            .build()
            .expect("build reader");
        let read_back: Vec<RecordBatch> = reader.collect::<Result<_, _>>().expect("read all");
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].num_rows(), 2);
        assert_evaluations_column(&read_back[0]);

        let file_reader = SerializedFileReader::new(bytes).expect("file reader");
        let root = file_reader.metadata().file_metadata().schema_descr().root_schema();
        let evaluations = root
            .get_fields()
            .iter()
            .find(|node| node.name() == "evaluations")
            .expect("evaluations group in the parquet schema");
        let mut ids = Vec::new();
        collect_field_ids(evaluations, &mut ids);
        let names: Vec<&str> = ids.iter().map(|(name, _)| name.as_str()).collect();
        let numbers: Vec<i32> = ids.iter().map(|(_, id)| *id).collect();
        // list, element struct, then the element's fields in declaration order.
        assert_eq!(numbers, (65..=78).collect::<Vec<i32>>());
        assert_eq!(
            &names[2..],
            &[
                "name",
                "score_value",
                "score_label",
                "explanation",
                "response_id",
                "error_type",
                "annotator_kind",
                "identifier",
                "metadata",
                "target_scope",
                "target_trace_id",
                "target_span_id"
            ]
        );
    }

    /// Collect `(name, field_id)` for every node under `node` that carries an
    /// id, in schema order; the repeated `list` wrapper has none and is skipped.
    fn collect_field_ids(node: &parquet::schema::types::Type, out: &mut Vec<(String, i32)>) {
        let info = node.get_basic_info();
        if info.has_id() {
            out.push((node.name().to_string(), info.id()));
        }
        if let parquet::schema::types::Type::GroupType { fields, .. } = node {
            for field in fields {
                collect_field_ids(field, out);
            }
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

        let (batch_opt, drops) = operations_to_record_batch(&request, &test_tenant("tenant-a")).expect("batch ok");
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
        let (batch_opt, drops) = operations_to_record_batch(&request, &test_tenant("t")).expect("ok");
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
                    key_strindex: 0,
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

        let (batch_opt, drops) = operations_to_record_batch(&request, &test_tenant("tenant-a")).expect("batch ok");
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
