//! Pure projection types for the `operations` transform.
//!
//! This module holds the owned, Arrow-decoupled row type (`OperationRow`), the
//! logical wire-field enum (`OperationField`), the expected wire-shape enum
//! (`FieldType`), the borrowing `AttributeView` over a span's attributes, and the
//! `project_operation_row` driver with its strict typed resolvers.

use std::collections::HashMap;

use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::trace::v1::Span;

use super::convention::{CONVENTIONS, field_precedence};
use crate::error::Result;
use crate::transform::attributes::{
    extract_bool, extract_f64, extract_i64, extract_string_list, extract_string_value, is_zero_bytes, nanos_to_micros,
    serialize_any_value_to_json, u32_count_to_i32,
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

/// Expected wire shape of an attribute value, so the resolver strict-parses
/// uniformly (spec D6). The source `span_attributes` map stringifies everything,
/// so numeric/bool/list columns are parsed from the raw OTLP `AnyValue`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldType {
    /// Verbatim string value.
    Str,
    /// Signed 64-bit integer (`Long`), e.g. tokens, `top_k`, `seed`.
    I64,
    /// 64-bit float (`Double`), e.g. `temperature`, `top_p`.
    F64,
    /// Boolean, e.g. `stream`.
    Bool,
    /// Non-negative `u32` count parsed into the schema's `Int` column.
    I32Count,
    /// `List<String>`, e.g. `stop_sequences`, `finish_reasons`.
    StrList,
    /// Structured value serialized to faithful JSON (content columns).
    Json,
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
fn resolve_str(view: &AttributeView, field: OperationField) -> Option<String> {
    for (key, _ty) in field_precedence(field) {
        if let Some(value) = view.get(key) {
            if let Some(s) = extract_string_value(Some(value)) {
                return Some(s);
            }
        }
    }
    None
}

/// Resolve the first present `i64` for `field`; strict parse (D6).
///
/// # Errors
///
/// Returns `IngestError::Validation` when a present value fails strict parsing.
fn resolve_i64(view: &AttributeView, field: OperationField, context: &'static str) -> Result<Option<i64>> {
    for (key, _ty) in field_precedence(field) {
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
    for (key, _ty) in field_precedence(field) {
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
    for (key, _ty) in field_precedence(field) {
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
    for (key, _ty) in field_precedence(field) {
        if let Some(value) = view.get(key) {
            return extract_string_list(Some(value), context);
        }
    }
    Ok(None)
}

/// Resolve the first present JSON-serialized content for `field`.
fn resolve_json(view: &AttributeView, field: OperationField) -> Option<String> {
    for (key, _ty) in field_precedence(field) {
        if let Some(value) = view.get(key) {
            if let Some(json) = serialize_any_value_to_json(Some(value)) {
                return Some(json);
            }
        }
    }
    None
}

/// Resolve `input_tokens`-style counts via strict `i64` parse.
///
/// # Errors
///
/// Returns `IngestError::Validation` when a present value fails strict parsing.
fn resolve_token(view: &AttributeView, field: OperationField, context: &'static str) -> Result<Option<i64>> {
    resolve_i64(view, field, context)
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

    let qualifies = CONVENTIONS
        .iter()
        .any(|conv| conv.marker_keys().iter().any(|key| view.has(key)));
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
        .find_map(|conv| conv.classify_operation(&view))
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

    let server_port = match resolve_i64(&view, OperationField::ServerPort, "server_port")? {
        Some(value) => Some(
            i32::try_from(value)
                .map_err(|_| crate::error::IngestError::Validation(format!("server_port out of i32 range: {value}")))?,
        ),
        None => None,
    };

    let time_to_first_chunk_ms =
        resolve_f64(&view, OperationField::TimeToFirstChunkMs, "time_to_first_chunk_ms")?.map(|seconds| {
            #[allow(clippy::cast_possible_truncation)]
            let millis = (seconds * 1000.0) as i64;
            millis
        });

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
        provider_name: resolve_str(&view, OperationField::ProviderName),
        request_model: resolve_str(&view, OperationField::RequestModel),
        response_model: resolve_str(&view, OperationField::ResponseModel),
        response_id: resolve_str(&view, OperationField::ResponseId),
        temperature: resolve_f64(&view, OperationField::Temperature, "temperature")?,
        top_p: resolve_f64(&view, OperationField::TopP, "top_p")?,
        top_k: resolve_i64(&view, OperationField::TopK, "top_k")?,
        max_tokens: resolve_i64(&view, OperationField::MaxTokens, "max_tokens")?,
        frequency_penalty: resolve_f64(&view, OperationField::FrequencyPenalty, "frequency_penalty")?,
        presence_penalty: resolve_f64(&view, OperationField::PresencePenalty, "presence_penalty")?,
        seed: resolve_i64(&view, OperationField::Seed, "seed")?,
        stream: resolve_bool(&view, OperationField::Stream, "stream")?,
        choice_count: resolve_i64(&view, OperationField::ChoiceCount, "choice_count")?,
        output_type: resolve_str(&view, OperationField::OutputType),
        reasoning_effort: resolve_str(&view, OperationField::ReasoningEffort),
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
        conversation_id: resolve_str(&view, OperationField::ConversationId),
        user_id: resolve_str(&view, OperationField::UserId),
        tool_name: resolve_str(&view, OperationField::ToolName),
        tool_call_id: resolve_str(&view, OperationField::ToolCallId),
        tool_type: resolve_str(&view, OperationField::ToolType),
        tool_description: resolve_str(&view, OperationField::ToolDescription),
        data_source_id: resolve_str(&view, OperationField::DataSourceId),
        embedding_dimensions,
        encoding_formats: resolve_str_list(&view, OperationField::EncodingFormats, "encoding_formats")?,
        server_address: resolve_str(&view, OperationField::ServerAddress),
        server_port,
        status_code,
        status_message,
        error_type: resolve_str(&view, OperationField::ErrorType),
        agent_id: resolve_str(&view, OperationField::AgentId),
        agent_name: resolve_str(&view, OperationField::AgentName),
        agent_version: resolve_str(&view, OperationField::AgentVersion),
        agent_description: resolve_str(&view, OperationField::AgentDescription),
        workflow_name: resolve_str(&view, OperationField::WorkflowName),
        input_messages: resolve_json(&view, OperationField::InputMessages),
        output_messages: resolve_json(&view, OperationField::OutputMessages),
        system_instructions: resolve_json(&view, OperationField::SystemInstructions),
        tool_definitions: resolve_json(&view, OperationField::ToolDefinitions),
        tool_call_arguments: resolve_json(&view, OperationField::ToolCallArguments),
        tool_call_result: resolve_json(&view, OperationField::ToolCallResult),
    }))
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::trace::v1::{Span, Status};

    use super::*;

    /// Build a string-valued OTLP `KeyValue` for tests.
    fn kv_str(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
        }
    }

    /// Build an OTLP `KeyValue` with an int value.
    fn kv_int(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(value)),
            }),
        }
    }

    /// Build an OTLP `KeyValue` with a double value.
    fn kv_dbl(key: &str, value: f64) -> KeyValue {
        KeyValue {
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
            key: "gen_ai.system".to_string(),
            value: None,
        }];
        let view = AttributeView::new(&attrs);
        assert!(!view.has("gen_ai.system"));
        assert!(view.get("gen_ai.system").is_none());
    }

    #[test]
    fn operation_field_and_field_type_are_constructible_and_comparable() {
        // OperationField is a plain Copy enum used as a registry lookup key.
        let a = OperationField::ProviderName;
        let b = OperationField::ProviderName;
        assert_eq!(a, b);
        assert_ne!(OperationField::ProviderName, OperationField::RequestModel);

        // FieldType describes the expected wire shape for strict parsing (D6).
        assert_eq!(FieldType::Str, FieldType::Str);
        assert_ne!(FieldType::I64, FieldType::F64);
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
}
