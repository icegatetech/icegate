//! Pure projection types for the `operations` transform.
//!
//! This module holds the owned, Arrow-decoupled row type (`OperationRow`), the
//! logical wire-field enum (`OperationField`), the borrowing `AttributeView` over
//! a span's attributes, and the `project_operation_row` driver with its strict
//! typed resolvers.

use std::collections::HashMap;

use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::trace::v1::{Span, span::Event};

use super::convention::{CONVENTIONS, field_precedence};
use crate::error::Result;
use crate::transform::attributes::{
    extract_bool, extract_f64, extract_i64, extract_string_list, extract_string_value, is_zero_bytes, nanos_to_micros,
    serialize_all_attrs_to_json_object, serialize_any_value_to_json, serialize_attrs_to_json_object,
    serialize_indexed_attrs_to_json_array, serialize_message_to_json_array, u32_count_to_i32,
};

/// Borrowing view over a span's attribute list. Built once per span and shared
/// across all field resolutions, so each lookup is O(1) rather than a repeated
/// linear scan. Scope attributes (`scope_name`/`scope_version`) are read directly
/// off the scope by the driver, not through this view.
///
/// On duplicate keys the last value wins, matching the last-write-wins dedupe
/// used by the other OTLP transforms. A `KeyValue` whose `value` is `None` is
/// skipped, so `has`/`get` only report attributes that carry an actual value.
pub(crate) struct AttributeView<'a> {
    by_key: HashMap<&'a str, &'a AnyValue>,
}

impl<'a> AttributeView<'a> {
    /// Builds a view over the given attribute slice. Later entries overwrite
    /// earlier ones on duplicate keys.
    pub(crate) fn new(attrs: &'a [KeyValue]) -> Self {
        let mut by_key = HashMap::with_capacity(attrs.len());
        for kv in attrs {
            if let Some(value) = kv.value.as_ref() {
                by_key.insert(kv.key.as_str(), value);
            }
        }
        Self { by_key }
    }

    /// Returns the borrowed [`AnyValue`] for `key`, or `None` when the key is
    /// absent (or its value was `None`).
    pub(crate) fn get(&self, key: &str) -> Option<&'a AnyValue> {
        self.by_key.get(key).copied()
    }

    /// Returns `true` when `key` is present with a value. Used for marker
    /// detection (a span qualifies as an operation iff any convention's marker
    /// key is present).
    pub(crate) fn has(&self, key: &str) -> bool {
        self.by_key.contains_key(key)
    }
}

/// Logical, wire-sourced operations fields resolved through the convention
/// registry. Mirrored-from-span columns (`tenant_id`, `trace_id`, `span_id`,
/// `parent_span_id`, timing, `service_name`, `status_*`) and scope columns
/// (`scope_name`/`scope_version`) are NOT here — they are read directly off the
/// span/scope, never via the registry. Each variant maps to exactly one
/// attribute-derived schema column (spec section 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum OperationField {
    /// `provider_name` column.
    ProviderName,
    /// `request_model` column.
    RequestModel,
    /// `response_model` column.
    ResponseModel,
    /// `response_id` column.
    ResponseId,
    /// `temperature` column.
    Temperature,
    /// `top_p` column.
    TopP,
    /// `top_k` column (single column for all ops, spec D4).
    TopK,
    /// `max_tokens` column.
    MaxTokens,
    /// `frequency_penalty` column.
    FrequencyPenalty,
    /// `presence_penalty` column.
    PresencePenalty,
    /// `seed` column.
    Seed,
    /// `stream` column.
    Stream,
    /// `choice_count` column.
    ChoiceCount,
    /// `output_type` column.
    OutputType,
    /// `reasoning_effort` column.
    ReasoningEffort,
    /// `stop_sequences` list column.
    StopSequences,
    /// `time_to_first_chunk_ms` column.
    TimeToFirstChunkMs,
    /// `finish_reasons` list column.
    FinishReasons,
    /// `input_tokens` column.
    InputTokens,
    /// `output_tokens` column.
    OutputTokens,
    /// `total_tokens` column.
    TotalTokens,
    /// `reasoning_tokens` column.
    ReasoningTokens,
    /// `cache_creation_input_tokens` column.
    CacheCreationInputTokens,
    /// `cache_read_input_tokens` column.
    CacheReadInputTokens,
    /// `conversation_id` column.
    ConversationId,
    /// `user_id` column.
    UserId,
    /// `tool_name` column.
    ToolName,
    /// `tool_call_id` column.
    ToolCallId,
    /// `tool_type` column.
    ToolType,
    /// `tool_description` column.
    ToolDescription,
    /// `data_source_id` column.
    DataSourceId,
    /// `embedding_dimensions` column.
    EmbeddingDimensions,
    /// `encoding_formats` list column.
    EncodingFormats,
    /// `server_address` column.
    ServerAddress,
    /// `server_port` column.
    ServerPort,
    /// `error_type` column.
    ErrorType,
    /// `agent_id` column.
    AgentId,
    /// `agent_name` column.
    AgentName,
    /// `agent_version` column.
    AgentVersion,
    /// `agent_description` column.
    AgentDescription,
    /// `workflow_name` column.
    WorkflowName,
    /// `input_messages` content column.
    InputMessages,
    /// `output_messages` content column.
    OutputMessages,
    /// `system_instructions` content column.
    SystemInstructions,
    /// `tool_definitions` content column.
    ToolDefinitions,
    /// `tool_call_arguments` content column.
    ToolCallArguments,
    /// `tool_call_result` content column.
    ToolCallResult,
}

/// Owned, Arrow-decoupled projection of one `operations` row.
///
/// Required columns (`tenant_id`, identity, timing, `operation_name`) are plain
/// typed values; every attribute-derived column is `Option<_>`; the three
/// `List<String>` columns are `Option<Vec<String>>` so an absent array becomes a
/// NULL list (not an empty list), matching the schema's nullable list semantics.
/// Fixed-width ids are stored as owned byte arrays so the row outlives the OTLP
/// request buffer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OperationRow {
    /// Partition identity, mirrored from `spans.tenant_id`.
    pub(crate) tenant_id: String,
    /// 16-byte trace id, mirrored from `spans.trace_id`.
    pub(crate) trace_id: [u8; 16],
    /// 8-byte span id, mirrored from `spans.span_id`.
    pub(crate) span_id: [u8; 8],
    /// 8-byte parent span id; `None` for a root span.
    pub(crate) parent_span_id: Option<[u8; 8]>,
    /// Service name, mirrored from `spans.service_name`; `None` when absent.
    pub(crate) service_name: Option<String>,
    /// OTLP `scope_spans.scope.name`.
    pub(crate) scope_name: Option<String>,
    /// OTLP `scope_spans.scope.version`.
    pub(crate) scope_version: Option<String>,
    /// Span start, microseconds; partition + sort source.
    pub(crate) timestamp: i64,
    /// Span end, microseconds.
    pub(crate) end_timestamp: i64,
    /// `(end - start).max(0)` microseconds.
    pub(crate) duration_micros: i64,
    /// Write watermark injected at transform time, microseconds.
    pub(crate) ingested_timestamp: i64,
    /// Canonical lowercase operation name (spec section 4).
    pub(crate) operation_name: String,
    /// LLM provider/vendor name.
    pub(crate) provider_name: Option<String>,
    /// Requested model.
    pub(crate) request_model: Option<String>,
    /// Responding model.
    pub(crate) response_model: Option<String>,
    /// Provider response id.
    pub(crate) response_id: Option<String>,
    /// Sampling temperature.
    pub(crate) temperature: Option<f64>,
    /// Nucleus sampling `top_p`.
    pub(crate) top_p: Option<f64>,
    /// Top-k sampling.
    pub(crate) top_k: Option<i64>,
    /// Max output tokens requested.
    pub(crate) max_tokens: Option<i64>,
    /// Frequency penalty.
    pub(crate) frequency_penalty: Option<f64>,
    /// Presence penalty.
    pub(crate) presence_penalty: Option<f64>,
    /// Sampling seed.
    pub(crate) seed: Option<i64>,
    /// Streaming flag.
    pub(crate) stream: Option<bool>,
    /// Requested choice count.
    pub(crate) choice_count: Option<i64>,
    /// Requested output type (`text`/`json`/`image`/`speech`).
    pub(crate) output_type: Option<String>,
    /// Reasoning effort (vendor extension).
    pub(crate) reasoning_effort: Option<String>,
    /// Requested stop sequences; NULL list when absent.
    pub(crate) stop_sequences: Option<Vec<String>>,
    /// Time-to-first-chunk in milliseconds.
    pub(crate) time_to_first_chunk_ms: Option<i64>,
    /// Response finish reasons; NULL list when absent.
    pub(crate) finish_reasons: Option<Vec<String>>,
    /// Prompt/input token count.
    pub(crate) input_tokens: Option<i64>,
    /// Completion/output token count.
    pub(crate) output_tokens: Option<i64>,
    /// Total token count.
    pub(crate) total_tokens: Option<i64>,
    /// Reasoning token count.
    pub(crate) reasoning_tokens: Option<i64>,
    /// Cache-creation input token count.
    pub(crate) cache_creation_input_tokens: Option<i64>,
    /// Cache-read input token count.
    pub(crate) cache_read_input_tokens: Option<i64>,
    /// Conversation/session id.
    pub(crate) conversation_id: Option<String>,
    /// End-user id.
    pub(crate) user_id: Option<String>,
    /// Tool name (`operation_name == "execute_tool"`).
    pub(crate) tool_name: Option<String>,
    /// Tool call id.
    pub(crate) tool_call_id: Option<String>,
    /// Tool type.
    pub(crate) tool_type: Option<String>,
    /// Tool description.
    pub(crate) tool_description: Option<String>,
    /// Retrieval data-source id (`operation_name == "retrieval"`).
    pub(crate) data_source_id: Option<String>,
    /// Embedding vector dimensionality.
    pub(crate) embedding_dimensions: Option<i32>,
    /// Embedding encoding formats; NULL list when absent.
    pub(crate) encoding_formats: Option<Vec<String>>,
    /// Server address.
    pub(crate) server_address: Option<String>,
    /// Server port.
    pub(crate) server_port: Option<i32>,
    /// OTLP status code, mirrored from `spans.status_code`.
    pub(crate) status_code: Option<i32>,
    /// Status message, mirrored from `spans.status_message`.
    pub(crate) status_message: Option<String>,
    /// Error type (Stable OTEL attribute).
    pub(crate) error_type: Option<String>,
    /// Agent id.
    pub(crate) agent_id: Option<String>,
    /// Agent name.
    pub(crate) agent_name: Option<String>,
    /// Agent version.
    pub(crate) agent_version: Option<String>,
    /// Agent description.
    pub(crate) agent_description: Option<String>,
    /// Workflow name.
    pub(crate) workflow_name: Option<String>,
    /// Input messages (faithful JSON).
    pub(crate) input_messages: Option<String>,
    /// Output messages (faithful JSON).
    pub(crate) output_messages: Option<String>,
    /// System instructions (faithful JSON).
    pub(crate) system_instructions: Option<String>,
    /// Tool definitions (faithful JSON).
    pub(crate) tool_definitions: Option<String>,
    /// Tool call arguments (faithful JSON).
    pub(crate) tool_call_arguments: Option<String>,
    /// Tool call result (faithful JSON).
    pub(crate) tool_call_result: Option<String>,
}

/// Validate a 16-byte non-zero `trace_id`, copying it into a fixed array.
fn validate_trace_id(bytes: &[u8]) -> Result<[u8; 16]> {
    match <[u8; 16]>::try_from(bytes) {
        Ok(arr) if !is_zero_bytes(&arr) => Ok(arr),
        _ => Err(crate::error::IngestError::Validation(
            "operations row has invalid trace_id (expected 16 non-zero bytes)".to_string(),
        )),
    }
}

/// Validate an 8-byte non-zero `span_id`, copying it into a fixed array.
fn validate_span_id(bytes: &[u8]) -> Result<[u8; 8]> {
    match <[u8; 8]>::try_from(bytes) {
        Ok(arr) if !is_zero_bytes(&arr) => Ok(arr),
        _ => Err(crate::error::IngestError::Validation(
            "operations row has invalid span_id (expected 8 non-zero bytes)".to_string(),
        )),
    }
}

/// Resolve the first present `String` value for `field` across the global
/// precedence order. Verbatim string columns use this.
///
/// # Errors
///
/// Returns `IngestError::Validation` if the field's precedence slice is
/// unavailable (an internal registry invariant; see [`field_precedence`]).
fn resolve_str(view: &AttributeView, field: OperationField) -> Result<Option<String>> {
    for &key in field_precedence(field)? {
        if let Some(value) = view.get(key) {
            if let Some(s) = extract_string_value(Some(value)) {
                return Ok(Some(s));
            }
        }
    }
    Ok(None)
}

/// Resolve the first present `i64` for `field`; strict parse (D6).
///
/// # Errors
///
/// Returns `IngestError::Validation` when a present value fails strict parsing.
fn resolve_i64(view: &AttributeView, field: OperationField, context: &'static str) -> Result<Option<i64>> {
    for &key in field_precedence(field)? {
        if let Some(value) = view.get(key) {
            return extract_i64(Some(value), context);
        }
    }
    Ok(None)
}

/// Resolve the first present `f64` for `field`; strict parse (D6).
///
/// # Errors
///
/// Returns `IngestError::Validation` when a present value fails strict parsing.
fn resolve_f64(view: &AttributeView, field: OperationField, context: &'static str) -> Result<Option<f64>> {
    for &key in field_precedence(field)? {
        if let Some(value) = view.get(key) {
            return extract_f64(Some(value), context);
        }
    }
    Ok(None)
}

/// Resolve the first present `bool` for `field`; strict parse (D6).
///
/// # Errors
///
/// Returns `IngestError::Validation` when a present value fails strict parsing.
fn resolve_bool(view: &AttributeView, field: OperationField, context: &'static str) -> Result<Option<bool>> {
    for &key in field_precedence(field)? {
        if let Some(value) = view.get(key) {
            return extract_bool(Some(value), context);
        }
    }
    Ok(None)
}

/// Resolve the first present `Vec<String>` for `field`; strict parse (D6).
///
/// # Errors
///
/// Returns `IngestError::Validation` when a present value fails strict parsing.
fn resolve_str_list(view: &AttributeView, field: OperationField, context: &'static str) -> Result<Option<Vec<String>>> {
    for &key in field_precedence(field)? {
        if let Some(value) = view.get(key) {
            return extract_string_list(Some(value), context);
        }
    }
    Ok(None)
}

/// Resolve the first present JSON-serialized content for `field`.
///
/// # Errors
///
/// Returns `IngestError::Validation` if the field's precedence slice is
/// unavailable (an internal registry invariant; see [`field_precedence`]).
fn resolve_json(view: &AttributeView, field: OperationField) -> Result<Option<String>> {
    for &key in field_precedence(field)? {
        if let Some(value) = view.get(key) {
            if let Some(json) = serialize_any_value_to_json(Some(value)) {
                return Ok(Some(json));
            }
        }
    }
    Ok(None)
}

/// Resolve a content field from span *events*: for the first registered
/// convention that names an event source for `field`, serialize the first
/// matching event's full attribute set into one JSON object. Returns `None` when
/// no convention sources `field` from events, or no such event is present.
fn resolve_json_from_events(events: &[Event], field: OperationField) -> Option<String> {
    for convention in CONVENTIONS {
        let event_names = convention.event_field_names(field);
        if event_names.is_empty() {
            continue;
        }
        for event in events {
            if event_names.contains(&event.name.as_str()) {
                if let Some(json) = serialize_all_attrs_to_json_object(&event.attributes) {
                    return Some(json);
                }
            }
        }
    }
    None
}

/// Resolve a content field as a JSON object of the convention-declared flat span
/// attributes present on the span, keyed by attribute name. Returns `None` when
/// no convention declares object attributes for `field`, or none are present.
fn resolve_json_object_from_attrs(attrs: &[KeyValue], field: OperationField) -> Option<String> {
    for convention in CONVENTIONS {
        let keys = convention.object_field_keys(field);
        if keys.is_empty() {
            continue;
        }
        if let Some(json) = serialize_attrs_to_json_object(attrs, keys) {
            return Some(json);
        }
    }
    None
}

/// Resolve a content field by rebuilding the JSON array a convention flattened
/// into indexed attribute keys. Returns `None` when no convention declares an
/// indexed prefix for `field`, or no attribute carries one.
fn resolve_indexed_array_from_attrs(attrs: &[KeyValue], field: OperationField) -> Option<String> {
    for convention in CONVENTIONS {
        for &prefix in convention.indexed_field_prefixes(field) {
            if let Some(json) = serialize_indexed_attrs_to_json_array(attrs, prefix) {
                return Some(json);
            }
        }
    }
    None
}

/// Resolve a message content field as a single-message JSON array
/// `[{"role": role, "content": <value>}]` from the first present
/// convention-declared `(attribute_key, role)` source. Returns `None` when no
/// convention declares a message source for `field`, or none is present.
fn resolve_message_array_from_attrs(attrs: &[KeyValue], field: OperationField) -> Option<String> {
    for convention in CONVENTIONS {
        for &(key, role) in convention.message_field_keys(field) {
            let content = attrs
                .iter()
                .find(|kv| kv.key == key)
                .and_then(|kv| extract_string_value(kv.value.as_ref()));
            if let Some(content) = content {
                if let Some(json) = serialize_message_to_json_array(role, &content) {
                    return Some(json);
                }
            }
        }
    }
    None
}

/// Resolve a content field through the modes in precedence order: a scalar
/// attribute value ([`resolve_json`]), an array rebuilt from indexed attribute
/// keys ([`resolve_indexed_array_from_attrs`]), a JSON object of flat attributes
/// ([`resolve_json_object_from_attrs`]), a single-message JSON array
/// ([`resolve_message_array_from_attrs`]), then a JSON object from a span event
/// ([`resolve_json_from_events`]). The first mode to produce a value wins.
///
/// # Errors
///
/// Returns `IngestError::Validation` if the field's precedence slice is
/// unavailable (see [`field_precedence`]).
fn resolve_json_incl_events(
    view: &AttributeView,
    attrs: &[KeyValue],
    events: &[Event],
    field: OperationField,
) -> Result<Option<String>> {
    if let Some(json) = resolve_json(view, field)? {
        return Ok(Some(json));
    }
    if let Some(json) = resolve_indexed_array_from_attrs(attrs, field) {
        return Ok(Some(json));
    }
    if let Some(json) = resolve_json_object_from_attrs(attrs, field) {
        return Ok(Some(json));
    }
    if let Some(json) = resolve_message_array_from_attrs(attrs, field) {
        return Ok(Some(json));
    }
    Ok(resolve_json_from_events(events, field))
}

/// Resolve `input_tokens`-style counts via strict `i64` parse.
///
/// # Errors
///
/// Returns `IngestError::Validation` when a present value fails strict parsing.
fn resolve_token(view: &AttributeView, field: OperationField, context: &'static str) -> Result<Option<i64>> {
    // Token counts are non-negative by contract; a negative value would skew
    // downstream usage aggregation, so drop the row instead of persisting it.
    match resolve_i64(view, field, context)? {
        Some(value) if value < 0 => Err(crate::error::IngestError::Validation(format!(
            "{context} must be non-negative: {value}"
        ))),
        other => Ok(other),
    }
}

/// Resolve `time_to_first_chunk_ms`, normalizing the source to milliseconds.
///
/// The source unit is inferred from the matched attribute key: a key whose name
/// ends in `_ms` (e.g. Claude Code's `ttft_ms`) is already milliseconds and is
/// kept as-is; every other key (OTEL's seconds-based
/// `gen_ai.response.time_to_first_chunk`) is seconds and scaled by 1000. The
/// value is validated non-negative and must fit the `i64` millisecond column
/// after conversion (D6); finiteness is already guaranteed by [`extract_f64`].
///
/// Resolves against the first present key in precedence order, matching the
/// other `resolve_*` helpers.
///
/// # Errors
///
/// Returns `IngestError::Validation` when a present value fails strict parsing,
/// is negative, or overflows the `i64` millisecond column.
fn resolve_time_to_first_chunk_ms(view: &AttributeView) -> Result<Option<i64>> {
    for &key in field_precedence(OperationField::TimeToFirstChunkMs)? {
        let Some(value) = view.get(key) else {
            continue;
        };
        let Some(raw) = extract_f64(Some(value), "time_to_first_chunk_ms")? else {
            return Ok(None);
        };
        // A direct `as i64` cast would turn a negative duration into a nonsense
        // latency, so reject it before converting.
        if raw < 0.0 {
            return Err(crate::error::IngestError::Validation(format!(
                "time_to_first_chunk_ms must be a finite non-negative duration: {raw}"
            )));
        }
        // `_ms` source keys are already milliseconds; every other key is seconds.
        let millis = if key.ends_with("_ms") { raw } else { raw * 1000.0 };
        // `i64::MAX as f64` rounds to 2^63; `>=` rejects anything that would
        // overflow the truncating cast below (including a `*1000.0` that pushed a
        // large-but-finite value to infinity).
        #[allow(clippy::cast_precision_loss)]
        let max_millis = i64::MAX as f64;
        if millis >= max_millis {
            return Err(crate::error::IngestError::Validation(format!(
                "time_to_first_chunk_ms out of range: {raw}"
            )));
        }
        #[allow(clippy::cast_possible_truncation)]
        let millis = millis as i64;
        return Ok(Some(millis));
    }
    Ok(None)
}

/// Project one OTLP span (+ scope + tenant) into an optional operations row.
///
/// `Ok(None)` = no convention marker present (non-LLM span; caller counts as
/// `non_llm_skipped`). `Err` = hard projection failure (invalid id, or a failed
/// strict typed parse / token overflow under D6); the caller drops the row and
/// counts it in `drops`. Pure: no I/O, no clock, no globals — `ingested_at`
/// (micros) is injected so a later backfill can replay the original watermark.
///
/// # Errors
///
/// Returns `IngestError::Validation` if the span qualifies as an operation but
/// has an invalid `trace_id`/`span_id`, or if any typed attribute fails strict
/// parsing.
#[allow(clippy::too_many_lines)]
pub(crate) fn project_operation_row(
    span: &Span,
    scope: Option<&InstrumentationScope>,
    tenant_id: &str,
    service_name: Option<&str>,
    ingested_at: i64,
) -> Result<Option<OperationRow>> {
    let view = AttributeView::new(&span.attributes);

    let qualifies = CONVENTIONS.iter().any(|conv| {
        conv.marker_keys().iter().any(|key| view.has(key))
            || conv.name_prefixes().iter().any(|prefix| span.name.starts_with(prefix))
    });
    if !qualifies {
        return Ok(None);
    }

    let trace_id = validate_trace_id(&span.trace_id)?;
    let span_id = validate_span_id(&span.span_id)?;
    let parent_span_id = match <[u8; 8]>::try_from(span.parent_span_id.as_slice()) {
        Ok(arr) if !is_zero_bytes(&arr) => Some(arr),
        _ => None,
    };

    let operation_name = CONVENTIONS
        .iter()
        .find_map(|conv| conv.classify_operation(&span.name, &view))
        .unwrap_or_else(|| "other".to_string());

    let timestamp = nanos_to_micros(span.start_time_unix_nano);
    let end_timestamp = nanos_to_micros(span.end_time_unix_nano);
    let duration_micros = (end_timestamp - timestamp).max(0);

    let (status_code, status_message) = span.status.as_ref().map_or((None, None), |status| {
        let code = if status.code == 0 { None } else { Some(status.code) };
        let message = if status.message.is_empty() {
            None
        } else {
            Some(status.message.clone())
        };
        (code, message)
    });

    let embedding_dimensions = match resolve_i64(&view, OperationField::EmbeddingDimensions, "embedding_dimensions")? {
        Some(value) => {
            let as_u32 = u32::try_from(value).map_err(|_| {
                crate::error::IngestError::Validation(format!("embedding_dimensions out of u32 range: {value}"))
            })?;
            Some(u32_count_to_i32(as_u32, "embedding_dimensions")?)
        }
        None => None,
    };

    // `ServerPort` is a non-negative count column. Mirror the
    // `embedding_dimensions` conversion above (u32 first) so a negative port is
    // rejected rather than silently stored.
    let server_port = match resolve_i64(&view, OperationField::ServerPort, "server_port")? {
        Some(value) => {
            let as_u32 = u32::try_from(value)
                .map_err(|_| crate::error::IngestError::Validation(format!("server_port out of u32 range: {value}")))?;
            Some(u32_count_to_i32(as_u32, "server_port")?)
        }
        None => None,
    };

    let time_to_first_chunk_ms = resolve_time_to_first_chunk_ms(&view)?;

    Ok(Some(OperationRow {
        tenant_id: tenant_id.to_string(),
        trace_id,
        span_id,
        parent_span_id,
        service_name: service_name.map(str::to_string),
        scope_name: scope.map(|s| s.name.clone()).filter(|s| !s.is_empty()),
        scope_version: scope.map(|s| s.version.clone()).filter(|s| !s.is_empty()),
        timestamp,
        end_timestamp,
        duration_micros,
        ingested_timestamp: ingested_at,
        operation_name,
        provider_name: resolve_str(&view, OperationField::ProviderName)?,
        request_model: resolve_str(&view, OperationField::RequestModel)?,
        response_model: resolve_str(&view, OperationField::ResponseModel)?,
        response_id: resolve_str(&view, OperationField::ResponseId)?,
        temperature: resolve_f64(&view, OperationField::Temperature, "temperature")?,
        top_p: resolve_f64(&view, OperationField::TopP, "top_p")?,
        top_k: resolve_i64(&view, OperationField::TopK, "top_k")?,
        max_tokens: resolve_i64(&view, OperationField::MaxTokens, "max_tokens")?,
        frequency_penalty: resolve_f64(&view, OperationField::FrequencyPenalty, "frequency_penalty")?,
        presence_penalty: resolve_f64(&view, OperationField::PresencePenalty, "presence_penalty")?,
        seed: resolve_i64(&view, OperationField::Seed, "seed")?,
        stream: resolve_bool(&view, OperationField::Stream, "stream")?,
        choice_count: resolve_i64(&view, OperationField::ChoiceCount, "choice_count")?,
        output_type: resolve_str(&view, OperationField::OutputType)?,
        reasoning_effort: resolve_str(&view, OperationField::ReasoningEffort)?,
        stop_sequences: resolve_str_list(&view, OperationField::StopSequences, "stop_sequences")?,
        time_to_first_chunk_ms,
        finish_reasons: resolve_str_list(&view, OperationField::FinishReasons, "finish_reasons")?,
        input_tokens: resolve_token(&view, OperationField::InputTokens, "input_tokens")?,
        output_tokens: resolve_token(&view, OperationField::OutputTokens, "output_tokens")?,
        total_tokens: resolve_token(&view, OperationField::TotalTokens, "total_tokens")?,
        reasoning_tokens: resolve_token(&view, OperationField::ReasoningTokens, "reasoning_tokens")?,
        cache_creation_input_tokens: resolve_token(
            &view,
            OperationField::CacheCreationInputTokens,
            "cache_creation_input_tokens",
        )?,
        cache_read_input_tokens: resolve_token(&view, OperationField::CacheReadInputTokens, "cache_read_input_tokens")?,
        conversation_id: resolve_str(&view, OperationField::ConversationId)?,
        user_id: resolve_str(&view, OperationField::UserId)?,
        tool_name: resolve_str(&view, OperationField::ToolName)?,
        tool_call_id: resolve_str(&view, OperationField::ToolCallId)?,
        tool_type: resolve_str(&view, OperationField::ToolType)?,
        tool_description: resolve_str(&view, OperationField::ToolDescription)?,
        data_source_id: resolve_str(&view, OperationField::DataSourceId)?,
        embedding_dimensions,
        encoding_formats: resolve_str_list(&view, OperationField::EncodingFormats, "encoding_formats")?,
        server_address: resolve_str(&view, OperationField::ServerAddress)?,
        server_port,
        status_code,
        status_message,
        error_type: resolve_str(&view, OperationField::ErrorType)?,
        agent_id: resolve_str(&view, OperationField::AgentId)?,
        agent_name: resolve_str(&view, OperationField::AgentName)?,
        agent_version: resolve_str(&view, OperationField::AgentVersion)?,
        agent_description: resolve_str(&view, OperationField::AgentDescription)?,
        workflow_name: resolve_str(&view, OperationField::WorkflowName)?,
        input_messages: resolve_json_incl_events(&view, &span.attributes, &span.events, OperationField::InputMessages)?,
        output_messages: resolve_json_incl_events(
            &view,
            &span.attributes,
            &span.events,
            OperationField::OutputMessages,
        )?,
        system_instructions: resolve_json_incl_events(
            &view,
            &span.attributes,
            &span.events,
            OperationField::SystemInstructions,
        )?,
        tool_definitions: resolve_json_incl_events(
            &view,
            &span.attributes,
            &span.events,
            OperationField::ToolDefinitions,
        )?,
        tool_call_arguments: resolve_json_incl_events(
            &view,
            &span.attributes,
            &span.events,
            OperationField::ToolCallArguments,
        )?,
        tool_call_result: resolve_json_incl_events(
            &view,
            &span.attributes,
            &span.events,
            OperationField::ToolCallResult,
        )?,
    }))
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::trace::v1::{Span, Status, span::Event};

    use super::*;

    /// Build a string-valued OTLP `KeyValue` for tests.
    fn kv_str(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key_strindex: 0,
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
        }
    }

    /// Build an OTLP `KeyValue` with an int value.
    fn kv_int(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key_strindex: 0,
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(value)),
            }),
        }
    }

    /// Build an OTLP `KeyValue` with a double value.
    fn kv_dbl(key: &str, value: f64) -> KeyValue {
        KeyValue {
            key_strindex: 0,
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::DoubleValue(value)),
            }),
        }
    }

    /// Build a minimal valid span carrying the supplied attributes.
    fn span_with(attributes: Vec<KeyValue>) -> Span {
        Span {
            trace_id: vec![1u8; 16],
            span_id: vec![2u8; 8],
            parent_span_id: Vec::new(),
            trace_state: String::new(),
            flags: 0,
            name: "op".to_string(),
            kind: 0,
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 3_000_000_000,
            attributes,
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: Some(Status {
                message: "ok".to_string(),
                code: 1,
            }),
        }
    }

    #[test]
    fn attribute_view_get_and_has_resolve_present_keys() {
        let attrs = vec![
            kv_str("gen_ai.system", "openai"),
            kv_str("gen_ai.request.model", "gpt-4o"),
        ];
        let view = AttributeView::new(&attrs);

        // has() is a cheap presence probe used for marker detection.
        assert!(view.has("gen_ai.system"));
        assert!(view.has("gen_ai.request.model"));
        assert!(!view.has("gen_ai.response.model"));

        // get() returns a borrow into the original KeyValue list.
        let provider = view.get("gen_ai.system").expect("provider value present");
        match provider.value.as_ref() {
            Some(Value::StringValue(s)) => assert_eq!(s, "openai"),
            _ => panic!("expected string value"),
        }
        assert!(view.get("missing.key").is_none());
    }

    #[test]
    fn attribute_view_last_value_wins_on_duplicate_keys() {
        // OTLP allows duplicate keys; the view keeps the last (matching the
        // last-write-wins dedupe used elsewhere in the transform layer).
        let attrs = vec![kv_str("gen_ai.system", "anthropic"), kv_str("gen_ai.system", "openai")];
        let view = AttributeView::new(&attrs);
        let provider = view.get("gen_ai.system").expect("provider present");
        match provider.value.as_ref() {
            Some(Value::StringValue(s)) => assert_eq!(s, "openai"),
            _ => panic!("expected string value"),
        }
    }

    #[test]
    fn attribute_view_skips_keys_with_no_value() {
        // A KeyValue whose value is None must not register as present.
        let attrs = vec![KeyValue {
            key_strindex: 0,
            key: "gen_ai.system".to_string(),
            value: None,
        }];
        let view = AttributeView::new(&attrs);
        assert!(!view.has("gen_ai.system"));
        assert!(view.get("gen_ai.system").is_none());
    }

    #[test]
    fn operation_field_is_constructible_and_comparable() {
        // OperationField is a plain Copy enum used as a registry lookup key.
        let a = OperationField::ProviderName;
        let b = OperationField::ProviderName;
        assert_eq!(a, b);
        assert_ne!(OperationField::ProviderName, OperationField::RequestModel);
    }

    #[test]
    fn operation_row_holds_typed_optional_columns() {
        // OperationRow is owned and Arrow-decoupled: required columns are plain
        // typed values, every attribute-derived column is Option<_>, and the
        // three List<String> columns are Option<Vec<String>> (NULL list, not
        // empty, when absent — see spec section 5 null handling).
        let row = OperationRow {
            tenant_id: "tenant-a".to_string(),
            trace_id: [0xAB; 16],
            span_id: [0xCD; 8],
            parent_span_id: None,
            service_name: None,
            scope_name: Some("my.sdk".to_string()),
            scope_version: Some("1.2.3".to_string()),
            timestamp: 1_000,
            end_timestamp: 2_000,
            duration_micros: 1_000,
            ingested_timestamp: 3_000,
            operation_name: "chat".to_string(),
            provider_name: Some("openai".to_string()),
            request_model: None,
            response_model: None,
            response_id: None,
            temperature: Some(0.7),
            top_p: None,
            top_k: None,
            max_tokens: None,
            frequency_penalty: None,
            presence_penalty: None,
            seed: None,
            stream: Some(true),
            choice_count: None,
            output_type: None,
            reasoning_effort: None,
            stop_sequences: None,
            time_to_first_chunk_ms: None,
            finish_reasons: Some(vec!["stop".to_string()]),
            input_tokens: Some(10),
            output_tokens: Some(20),
            total_tokens: Some(30),
            reasoning_tokens: None,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            conversation_id: None,
            user_id: None,
            tool_name: None,
            tool_call_id: None,
            tool_type: None,
            tool_description: None,
            data_source_id: None,
            embedding_dimensions: None,
            encoding_formats: None,
            server_address: None,
            server_port: None,
            status_code: Some(1),
            status_message: None,
            error_type: None,
            agent_id: None,
            agent_name: None,
            agent_version: None,
            agent_description: None,
            workflow_name: None,
            input_messages: None,
            output_messages: None,
            system_instructions: None,
            tool_definitions: None,
            tool_call_arguments: None,
            tool_call_result: None,
        };

        assert_eq!(row.tenant_id, "tenant-a");
        assert_eq!(row.operation_name, "chat");
        assert_eq!(row.temperature, Some(0.7));
        assert_eq!(row.finish_reasons, Some(vec!["stop".to_string()]));
        assert_eq!(row.stop_sequences, None);
        assert_eq!(row.parent_span_id, None);
    }

    #[test]
    fn otel_only_projects_typed_columns() {
        let span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("gen_ai.provider.name", "openai"),
            kv_str("gen_ai.request.model", "gpt-4o"),
            kv_dbl("gen_ai.request.temperature", 0.7),
            kv_int("gen_ai.usage.input_tokens", 12),
            kv_int("gen_ai.usage.output_tokens", 34),
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
        ]);

        let row = project_operation_row(&span, None, "tenant-a", Some("svc"), 999)
            .expect("projection ok")
            .expect("llm span -> row");

        assert_eq!(row.operation_name, "chat");
        assert_eq!(row.provider_name.as_deref(), Some("openai"));
        assert_eq!(row.request_model.as_deref(), Some("gpt-4o"));
        assert_eq!(row.temperature, Some(0.7));
        assert_eq!(row.input_tokens, Some(12));
        assert_eq!(row.output_tokens, Some(34));
        assert_eq!(row.finish_reasons, Some(vec!["stop".to_string()]));
        assert_eq!(row.tenant_id, "tenant-a");
        assert_eq!(row.service_name.as_deref(), Some("svc"));
        assert_eq!(row.ingested_timestamp, 999);
    }

    #[test]
    fn openinference_only_normalizes_and_resolves() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "RETRIEVER"),
            kv_str("llm.model_name", "text-embedding-3"),
            kv_str("llm.system", "openai"),
            kv_int("llm.token_count.prompt", 7),
            kv_str("session.id", "sess-1"),
        ]);

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("llm span -> row");

        assert_eq!(row.operation_name, "retrieval");
        assert_eq!(row.request_model.as_deref(), Some("text-embedding-3"));
        assert_eq!(row.provider_name.as_deref(), Some("openai"));
        assert_eq!(row.input_tokens, Some(7));
        assert_eq!(row.conversation_id.as_deref(), Some("sess-1"));
    }

    #[test]
    fn traceloop_only_normalizes_and_resolves() {
        let span = span_with(vec![
            kv_str("traceloop.span.kind", "workflow"),
            kv_int("gen_ai.usage.prompt_tokens", 5),
            kv_int("gen_ai.usage.completion_tokens", 9),
            KeyValue {
                key_strindex: 0,
                key: "gen_ai.is_streaming".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::BoolValue(true)),
                }),
            },
            kv_str("traceloop.workflow.name", "wf-1"),
        ]);

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("llm span -> row");

        assert_eq!(row.operation_name, "chain");
        assert_eq!(row.input_tokens, Some(5));
        assert_eq!(row.output_tokens, Some(9));
        assert_eq!(row.stream, Some(true));
        assert_eq!(row.workflow_name.as_deref(), Some("wf-1"));
    }

    #[test]
    fn otel_wins_precedence_over_vendor_keys() {
        let span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("gen_ai.request.model", "otel-model"),
            kv_str("llm.model_name", "oi-model"),
            kv_str("gen_ai.provider.name", "otel-provider"),
            kv_str("llm.system", "oi-provider"),
        ]);

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("llm span -> row");

        assert_eq!(row.request_model.as_deref(), Some("otel-model"));
        assert_eq!(row.provider_name.as_deref(), Some("otel-provider"));
    }

    #[test]
    fn minimal_matching_span_leaves_optionals_null() {
        let span = span_with(vec![kv_str("gen_ai.operation.name", "chat")]);

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("llm span -> row");

        assert_eq!(row.operation_name, "chat");
        assert!(row.temperature.is_none());
        assert!(row.input_tokens.is_none());
        assert!(row.stop_sequences.is_none());
        assert!(row.finish_reasons.is_none());
        assert!(row.parent_span_id.is_none());
    }

    #[test]
    fn non_llm_span_yields_none() {
        let span = span_with(vec![kv_str("http.method", "GET")]);
        assert!(project_operation_row(&span, None, "t", None, 1).expect("ok").is_none());
    }

    #[test]
    fn bad_trace_id_on_matching_span_is_err() {
        let mut span = span_with(vec![kv_str("gen_ai.operation.name", "chat")]);
        span.trace_id = vec![0u8; 16];
        assert!(project_operation_row(&span, None, "t", None, 1).is_err());
    }

    #[test]
    fn malformed_temperature_is_err() {
        let span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("gen_ai.request.temperature", "hot"),
        ]);
        assert!(project_operation_row(&span, None, "t", None, 1).is_err());
    }

    #[test]
    fn duration_micros_clamps_to_zero_when_end_before_start() {
        let mut span = span_with(vec![kv_str("gen_ai.operation.name", "chat")]);
        span.start_time_unix_nano = 3_000_000_000;
        span.end_time_unix_nano = 1_000_000_000;
        let row = project_operation_row(&span, None, "t", None, 1).expect("ok").expect("row");
        assert_eq!(row.duration_micros, 0);
    }

    /// Build a Claude Code span with the given name and attributes.
    fn claude_span(name: &str, attributes: Vec<KeyValue>) -> Span {
        let mut span = span_with(attributes);
        span.name = name.to_string();
        span
    }

    /// Build a `tool.output` span event carrying the given attributes.
    fn tool_output_event(attributes: Vec<KeyValue>) -> Event {
        Event {
            time_unix_nano: 1_500_000_000,
            name: "tool.output".to_string(),
            attributes,
            dropped_attributes_count: 0,
        }
    }

    /// Build a Claude Code span with the given name, attributes, and events.
    fn claude_span_with_events(name: &str, attributes: Vec<KeyValue>, events: Vec<Event>) -> Span {
        let mut span = claude_span(name, attributes);
        span.events = events;
        span
    }

    #[test]
    fn claude_code_llm_request_projects_tokens_and_chat() {
        // Real captured `claude_code.llm_request` attributes (values stringified,
        // as Claude Code emits them). Tokens must land (previously NULL), ttft_ms
        // must stay milliseconds, and provider/model/ids resolve via OTEL/OI.
        let span = claude_span(
            "claude_code.llm_request",
            vec![
                kv_str("gen_ai.request.model", "claude-opus-4-8[1m]"),
                kv_str("gen_ai.system", "anthropic"),
                kv_str("gen_ai.response.id", "req_011Ccznc3e9DSqCxko4AaReK"),
                kv_str("input_tokens", "94"),
                kv_str("output_tokens", "83"),
                kv_str("cache_creation_tokens", "10062"),
                kv_str("cache_read_tokens", "146256"),
                kv_str("ttft_ms", "1305"),
                kv_str("session.id", "c82374f6-6c77-451b-94d1-5fd472cccf1a"),
                kv_str(
                    "user.id",
                    "f1ec8a18ce99fb0e68706cfbb735351381c5c1d945928a5cd0b56a2fbbd2f055",
                ),
                kv_str("span.type", "llm_request"),
            ],
        );

        let row = project_operation_row(&span, None, "tenant-a", Some("claude-code"), 1)
            .expect("projection ok")
            .expect("llm span -> row");

        assert_eq!(row.operation_name, "chat");
        assert_eq!(row.request_model.as_deref(), Some("claude-opus-4-8[1m]"));
        assert_eq!(row.provider_name.as_deref(), Some("anthropic"));
        assert_eq!(row.response_id.as_deref(), Some("req_011Ccznc3e9DSqCxko4AaReK"));
        assert_eq!(row.input_tokens, Some(94));
        assert_eq!(row.output_tokens, Some(83));
        assert_eq!(row.cache_creation_input_tokens, Some(10_062));
        assert_eq!(row.cache_read_input_tokens, Some(146_256));
        // `ttft_ms` is already milliseconds and must NOT be scaled by 1000.
        assert_eq!(row.time_to_first_chunk_ms, Some(1305));
        assert_eq!(
            row.conversation_id.as_deref(),
            Some("c82374f6-6c77-451b-94d1-5fd472cccf1a")
        );
        assert_eq!(
            row.user_id.as_deref(),
            Some("f1ec8a18ce99fb0e68706cfbb735351381c5c1d945928a5cd0b56a2fbbd2f055")
        );
    }

    #[test]
    fn claude_code_interaction_qualifies_by_name_without_gen_ai_marker() {
        // interaction carries no gen_ai.* marker, so it qualifies purely by the
        // `claude_code.` span-name prefix.
        let span = claude_span(
            "claude_code.interaction",
            vec![
                kv_str("session.id", "c82374f6"),
                kv_str("user.id", "f1ec8a18"),
                kv_str("span.type", "interaction"),
            ],
        );

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("interaction span -> row");

        assert_eq!(row.operation_name, "invoke_agent");
        assert_eq!(row.conversation_id.as_deref(), Some("c82374f6"));
        assert_eq!(row.user_id.as_deref(), Some("f1ec8a18"));
    }

    #[test]
    fn claude_code_agent_tool_projects_invoke_subagent() {
        let span = claude_span(
            "claude_code.tool",
            vec![
                kv_str("tool_name", "Agent"),
                kv_str("subagent_type", "code-reviewer"),
                kv_str("gen_ai.tool.call.id", "toolu_015wNSz"),
                kv_str("span.type", "tool"),
            ],
        );

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("tool span -> row");

        assert_eq!(row.operation_name, "invoke_subagent");
        assert_eq!(row.tool_name.as_deref(), Some("Agent"));
        assert_eq!(row.tool_call_id.as_deref(), Some("toolu_015wNSz"));
        // subagent_type names WHICH subagent was dispatched.
        assert_eq!(row.agent_name.as_deref(), Some("code-reviewer"));
    }

    #[test]
    fn claude_code_llm_request_projects_agent_id_and_workflow_name() {
        // agent_id / workflow.name are Claude Code's flat spellings; they populate
        // the agent_id / workflow_name columns OTEL only sources from gen_ai.* keys.
        let span = claude_span(
            "claude_code.llm_request",
            vec![
                kv_str("gen_ai.system", "anthropic"),
                kv_str("agent_id", "agent-7"),
                kv_str("workflow.name", "code-review"),
            ],
        );

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("row");

        assert_eq!(row.operation_name, "chat");
        assert_eq!(row.agent_id.as_deref(), Some("agent-7"));
        assert_eq!(row.workflow_name.as_deref(), Some("code-review"));
    }

    #[test]
    fn claude_code_bash_tool_projects_execute_tool() {
        let span = claude_span(
            "claude_code.tool",
            vec![kv_str("tool_name", "Bash"), kv_str("span.type", "tool")],
        );

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("tool span -> row");

        assert_eq!(row.operation_name, "execute_tool");
        assert_eq!(row.tool_name.as_deref(), Some("Bash"));
    }

    #[test]
    fn claude_code_bash_tool_projects_full_command_as_arguments() {
        let span = claude_span(
            "claude_code.tool",
            vec![
                kv_str("tool_name", "Bash"),
                kv_str("full_command", "git status --short"),
                kv_str("span.type", "tool"),
            ],
        );

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("tool span -> row");

        // Arguments are a JSON object keyed by the attribute name.
        let args: serde_json::Value =
            serde_json::from_str(row.tool_call_arguments.as_deref().expect("args present")).expect("args is json");
        assert_eq!(args["full_command"], "git status --short");
    }

    #[test]
    fn claude_code_read_tool_projects_file_path_as_arguments() {
        // The file_path key covers Read/Edit tools that carry no full_command.
        let span = claude_span(
            "claude_code.tool",
            vec![
                kv_str("tool_name", "Read"),
                kv_str("file_path", "/repo/src/main.rs"),
                kv_str("span.type", "tool"),
            ],
        );

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("tool span -> row");

        let args: serde_json::Value =
            serde_json::from_str(row.tool_call_arguments.as_deref().expect("args present")).expect("args is json");
        assert_eq!(args["file_path"], "/repo/src/main.rs");
    }

    #[test]
    fn claude_code_interaction_projects_user_prompt_as_input_messages() {
        let span = claude_span(
            "claude_code.interaction",
            vec![
                kv_str("span.type", "interaction"),
                kv_str("user_prompt", "/code-review"),
            ],
        );

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("interaction span -> row");

        assert_eq!(row.operation_name, "invoke_agent");
        // The conversation UI requires input_messages to be a JSON array of
        // {role, content} messages, so user_prompt is wrapped as a user message.
        let messages: serde_json::Value =
            serde_json::from_str(row.input_messages.as_deref().expect("input_messages present")).expect("json array");
        assert!(messages.is_array(), "input_messages must be a JSON array");
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "/code-review");
    }

    #[test]
    fn openinference_indexed_messages_project_into_the_content_columns() {
        // OpenInference SDKs (smolagents, LlamaIndex, LangChain) do not emit a
        // messages array; they flatten one across indexed attributes. Before the
        // indexed mode existed these spans projected an operations row with both
        // message columns NULL, silently losing the whole conversation.
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("llm.model_name", "o3-mini"),
            kv_str("llm.input_messages.0.message.role", "system"),
            kv_str("llm.input_messages.0.message.content", "You are helpful"),
            kv_str("llm.input_messages.1.message.role", "user"),
            kv_str("llm.input_messages.1.message.content", "What is 2+2?"),
            kv_str("llm.output_messages.0.message.role", "assistant"),
            kv_str("llm.output_messages.0.message.content", "4"),
        ]);

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("an OpenInference LLM span -> row");

        assert_eq!(row.operation_name, "chat");
        let input: serde_json::Value =
            serde_json::from_str(row.input_messages.as_deref().expect("input_messages present")).expect("json");
        assert_eq!(input.as_array().expect("array").len(), 2);
        assert_eq!(input[0]["role"], "system");
        assert_eq!(input[1]["content"], "What is 2+2?");

        let output: serde_json::Value =
            serde_json::from_str(row.output_messages.as_deref().expect("output_messages present")).expect("json");
        assert_eq!(output[0]["role"], "assistant");
        assert_eq!(output[0]["content"], "4");
    }

    #[test]
    fn openinference_tool_schemas_project_into_tool_definitions() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("llm.tools.0.tool.json_schema", r#"{"name":"web_search"}"#),
        ]);

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("row");

        let tools: serde_json::Value =
            serde_json::from_str(row.tool_definitions.as_deref().expect("tool_definitions present")).expect("json");
        assert_eq!(tools[0]["json_schema"], r#"{"name":"web_search"}"#);
    }

    #[test]
    fn a_whole_messages_array_wins_over_the_flattened_form() {
        // A span carrying both means the same thing by each, so the scalar
        // OTEL attribute is taken and the indexed rebuild is not run.
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("gen_ai.input.messages", r#"[{"role":"user","content":"whole"}]"#),
            kv_str("llm.input_messages.0.message.role", "user"),
            kv_str("llm.input_messages.0.message.content", "flattened"),
        ]);

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("row");

        let input: serde_json::Value =
            serde_json::from_str(row.input_messages.as_deref().expect("present")).expect("json");
        assert_eq!(input[0]["content"], "whole");
    }

    #[test]
    fn openinference_session_id_projects_as_the_conversation_id() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("session.id", "conv-42"),
        ]);

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("row");

        assert_eq!(row.conversation_id.as_deref(), Some("conv-42"));
    }

    #[test]
    fn otel_time_to_first_chunk_seconds_scales_to_millis() {
        // OTEL's `gen_ai.response.time_to_first_chunk` is seconds; the resolver
        // scales it x1000. Guards the unit-aware resolver's seconds branch.
        let span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_dbl("gen_ai.response.time_to_first_chunk", 1.3),
        ]);
        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("row");
        assert_eq!(row.time_to_first_chunk_ms, Some(1300));
    }

    #[test]
    fn claude_code_negative_input_tokens_drops_row() {
        // Claude Code's flat token keys inherit the same strict non-negative
        // contract as every other adapter: a negative count drops the row (D6).
        let span = claude_span("claude_code.llm_request", vec![kv_str("input_tokens", "-5")]);
        assert!(project_operation_row(&span, None, "t", None, 1).is_err());
    }

    #[test]
    fn claude_code_negative_ttft_ms_drops_row() {
        // Exercises the unit-aware resolver's negative guard on the `_ms` path.
        let span = claude_span("claude_code.llm_request", vec![kv_str("ttft_ms", "-1")]);
        assert!(project_operation_row(&span, None, "t", None, 1).is_err());
    }

    #[test]
    fn claude_code_tool_execution_subspan_projects_execute_tool() {
        // tool.execution carries no gen_ai marker and no tool_name; it qualifies
        // by the `claude_code.` name prefix and classifies via the tool family.
        let span = claude_span(
            "claude_code.tool.execution",
            vec![
                kv_str("gen_ai.tool.call.id", "toolu_015wNSz"),
                kv_str("span.type", "tool.execution"),
            ],
        );
        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("tool.execution span -> row");
        assert_eq!(row.operation_name, "execute_tool");
        assert_eq!(row.tool_call_id.as_deref(), Some("toolu_015wNSz"));
    }

    #[test]
    fn claude_code_bash_tool_output_event_is_the_result() {
        // The whole tool.output event is the tool's result (echoed command +
        // output); arguments come from span attributes, not the event.
        let span = claude_span_with_events(
            "claude_code.tool",
            vec![kv_str("tool_name", "Bash"), kv_str("span.type", "tool")],
            vec![tool_output_event(vec![
                kv_str("bash_command", "git status --short"),
                kv_str("output", "M src/main.rs"),
            ])],
        );

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("tool span -> row");

        // Result is the whole event, including the echoed command.
        let result: serde_json::Value =
            serde_json::from_str(row.tool_call_result.as_deref().expect("result present")).expect("result is json");
        assert_eq!(result["output"], "M src/main.rs");
        assert_eq!(result["bash_command"], "git status --short");

        // No full_command span attribute and no event->arguments source -> None.
        assert!(row.tool_call_arguments.is_none());
    }

    #[test]
    fn claude_code_read_tool_output_event_is_the_result() {
        let span = claude_span_with_events(
            "claude_code.tool",
            vec![kv_str("tool_name", "Read"), kv_str("span.type", "tool")],
            vec![tool_output_event(vec![
                kv_str("file_path", "/repo/main.rs"),
                kv_str("content", "fn main() {}"),
            ])],
        );

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("tool span -> row");

        let result: serde_json::Value =
            serde_json::from_str(row.tool_call_result.as_deref().expect("result present")).expect("result is json");
        assert_eq!(result["content"], "fn main() {}");
        assert_eq!(result["file_path"], "/repo/main.rs");

        assert!(row.tool_call_arguments.is_none());
    }

    #[test]
    fn claude_code_tool_arguments_come_from_span_attributes_not_the_event() {
        // full_command (span attribute) is the input; the tool.output event is the
        // result. The two are independent sources.
        let span = claude_span_with_events(
            "claude_code.tool",
            vec![
                kv_str("tool_name", "Bash"),
                kv_str("full_command", "ls -la"),
                kv_str("span.type", "tool"),
            ],
            vec![tool_output_event(vec![
                kv_str("bash_command", "ls -la"),
                kv_str("output", "a\nb"),
            ])],
        );

        let row = project_operation_row(&span, None, "t", None, 1)
            .expect("projection ok")
            .expect("tool span -> row");

        // Arguments come from the span attribute (input), as a JSON object.
        let args: serde_json::Value =
            serde_json::from_str(row.tool_call_arguments.as_deref().expect("args present")).expect("args is json");
        assert_eq!(args["full_command"], "ls -la");
        // Result comes from the event (output).
        let result: serde_json::Value =
            serde_json::from_str(row.tool_call_result.as_deref().expect("result present")).expect("result is json");
        assert_eq!(result["output"], "a\nb");
    }
}
