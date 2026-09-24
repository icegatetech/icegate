//! Pure projection types for the `operations` transform.
//!
//! This module holds the owned, Arrow-decoupled row type (`OperationRow`), the
//! logical wire-field enum (`OperationField`), the borrowing `AttributeView` over
//! a span's attributes, and the `project_operation_row` driver with its strict
//! typed resolvers.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

use icegate_common::TenantId;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::trace::v1::{Span, span::Event};

use super::convention::{
    CONVENTIONS, EVALUATION_OPERATION_NAME, EvaluationArraySource, OperationConvention, RegisteredEvaluationArray,
    evaluation_event_filter, field_precedence, registered_evaluation_arrays,
};
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
    /// Memoized parse of each convention-declared JSON blob attribute read so
    /// far, keyed by attribute name. `None` records a blob that is absent or
    /// not a JSON object, so a failed parse is not retried either.
    ///
    /// Interior mutability because the resolvers hold the view by shared
    /// reference; the view is built and dropped inside one span's projection,
    /// so the cell is never shared across threads.
    parsed_blobs: RefCell<HashMap<&'static str, Option<serde_json::Map<String, serde_json::Value>>>>,
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
        Self {
            by_key,
            parsed_blobs: RefCell::new(HashMap::new()),
        }
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

    /// Returns `json_field` out of the JSON object held in the `blob_key`
    /// attribute, parsing that attribute at most once per span.
    ///
    /// A blob that is absent, unparseable, or not a JSON object yields `None`,
    /// as does a field that is missing or JSON `null`. The parse is memoized
    /// because one blob backs many columns — every `OpenInference` sampling
    /// parameter is read out of `llm.invocation_parameters` — and the blob is a
    /// whole vendor request payload, so its parse cost scales with the payload,
    /// not with the one number being read.
    fn blob_field(&self, blob_key: &'static str, json_field: &str) -> Option<serde_json::Value> {
        let mut parsed_blobs = self.parsed_blobs.borrow_mut();
        let fields = parsed_blobs.entry(blob_key).or_insert_with(|| {
            let text = extract_string_value(self.get(blob_key))?;
            match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(serde_json::Value::Object(fields)) => Some(fields),
                _ => None,
            }
        });
        fields
            .as_ref()
            .and_then(|fields| fields.get(json_field))
            .filter(|value| !value.is_null())
            .cloned()
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

/// Fields of one evaluation result resolved through the convention registry
/// into one element of the `evaluations` column. Each variant maps to exactly
/// one field of the element struct. Kept apart from [`OperationField`]: those
/// name whole columns, these name fields inside one nested column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum EvaluationField {
    /// `evaluations[].name` — the evaluation metric.
    Name,
    /// `evaluations[].score_value` — numeric score.
    ScoreValue,
    /// `evaluations[].score_label` — categorical score.
    ScoreLabel,
    /// `evaluations[].explanation` — the judge's free-form reasoning.
    Explanation,
    /// `evaluations[].response_id` — id of the evaluated response.
    ResponseId,
    /// `evaluations[].error_type` — error class when the evaluation failed.
    ErrorType,
    /// `evaluations[].annotator_kind` — kind of judge.
    AnnotatorKind,
    /// `evaluations[].identifier` — producer-assigned result id.
    Identifier,
    /// `evaluations[].metadata` — extra result data as a JSON object.
    Metadata,
}

/// What one evaluation result is about: the span it targets, that span's whole
/// trace, or the session named by the row's `conversation_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvaluationScope {
    /// One span.
    Span,
    /// One trace.
    Trace,
    /// One session.
    Session,
}

impl EvaluationScope {
    /// The value stored in `evaluations[].target_scope`.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Span => "span",
            Self::Trace => "trace",
            Self::Session => "session",
        }
    }
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
    /// Complete evaluation results found on the span, in resolution order;
    /// NULL list when it carries none.
    pub(crate) evaluations: Option<Vec<EvaluationResult>>,
}

/// One complete evaluation result, projected into one element of the
/// `evaluations` column: it always names its metric and states what it is
/// about; everything else is whatever the convention carried.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EvaluationResult {
    /// Evaluation metric name, never empty.
    pub(crate) name: String,
    /// Numeric score.
    pub(crate) score_value: Option<f64>,
    /// Categorical score.
    pub(crate) score_label: Option<String>,
    /// The judge's free-form explanation of the score.
    pub(crate) explanation: Option<String>,
    /// Id of the evaluated response, as the result states it.
    pub(crate) response_id: Option<String>,
    /// Error class when the evaluation itself failed.
    pub(crate) error_type: Option<String>,
    /// Kind of judge that produced the result.
    pub(crate) annotator_kind: Option<String>,
    /// Producer-assigned id, stable across results with the same name and
    /// target.
    pub(crate) identifier: Option<String>,
    /// Extra result data; only ever a JSON object serialized as text.
    pub(crate) metadata: Option<String>,
    /// What the result is about.
    pub(crate) target_scope: EvaluationScope,
    /// Trace of the evaluated span or the evaluated trace; `None` when the
    /// scope names no trace or the target is unknown.
    pub(crate) target_trace_id: Option<[u8; 16]>,
    /// The evaluated span; `None` for the wider scopes or an unknown target.
    pub(crate) target_span_id: Option<[u8; 8]>,
}

/// One span's projection: its `operations` row, plus how many evaluation
/// results the span carried that the row leaves out as incomplete.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProjectedOperation {
    /// The `operations` row.
    pub(crate) row: OperationRow,
    /// Evaluation results found on the span but not stored in
    /// [`OperationRow::evaluations`], because they lack what every result
    /// must carry. Surfaced by the caller; the row itself is still written.
    pub(crate) skipped_evaluations: usize,
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
        if let Some(value) = view.get(key)
            && let Some(s) = extract_string_value(Some(value))
        {
            return Ok(Some(s));
        }
    }
    Ok(None)
}

/// Resolve `field` out of a convention-declared JSON blob attribute.
///
/// Returns the raw JSON value so each typed resolver can apply its own
/// conversion. A blob that is absent, unparseable, or missing the named field
/// yields `None`.
///
/// Deliberately non-fatal, unlike the typed attribute resolvers: a blob is an
/// opaque vendor request payload that the convention makes no type promise
/// about, so a surprise inside it is a value this projection cannot use, not
/// evidence that the span is malformed. Dropping the whole operations row over
/// an unexpected `temperature` in a passthrough payload would lose the span's
/// tokens, model, and messages along with it.
///
/// Each declared blob is parsed at most once per span by
/// [`AttributeView::blob_field`], however many columns read out of it, and not
/// at all for the conventions that declare no blob sources.
fn resolve_blob_value(view: &AttributeView, field: OperationField) -> Option<serde_json::Value> {
    CONVENTIONS.iter().find_map(|convention| {
        convention
            .json_blob_field_keys(field)
            .iter()
            .find_map(|&(blob_key, json_field)| view.blob_field(blob_key, json_field))
    })
}

/// Resolve the first present `i64` for `field`; strict parse (D6).
///
/// Falls back to a JSON blob source ([`resolve_blob_value`]) only when no
/// declared attribute key is present.
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
    // JSON has a single number type, so an SDK that holds `max_tokens` as a
    // float serializes it as `40.0`, which `as_i64` rejects. Accept an integral
    // number in range rather than leaving the column NULL; a real fraction is
    // still not an integer and stays NULL. This mirrors `resolve_f64`, which
    // accepts an integral number for the same reason.
    Ok(resolve_blob_value(view, field).and_then(|value| value.as_i64().or_else(|| blob_number_as_i64(&value))))
}

/// Convert an integral JSON float to `i64`, or `None` when it has a fractional
/// part or a magnitude no longer pinned to a single integer by `f64`.
fn blob_number_as_i64(value: &serde_json::Value) -> Option<i64> {
    // 2^53, the first magnitude at which the representable integers stop being
    // consecutive: from here up, a whole `f64` no longer names one integer, so
    // casting it would store a number nothing necessarily sent. The bound is
    // exclusive because 2^53 is itself where its unrepresentable neighbour
    // lands. It sits far under `i64::MAX`, so nothing reaching the cast can
    // saturate.
    //
    // This bounds only the cast. serde_json is built without
    // `arbitrary_precision`, and its parser is already off by an ulp for some
    // literals just under the bound, which no check here can undo: the text is
    // gone by the time a `Value` arrives.
    const EXACT_INTEGER_BOUND: f64 = 9_007_199_254_740_992.0;
    let number = value.as_f64()?;
    if number.fract() != 0.0 || number.abs() >= EXACT_INTEGER_BOUND {
        return None;
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "checked integral and exactly representable above"
    )]
    Some(number as i64)
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
    // A blob number arrives as JSON, where `1` and `1.0` are the same literal,
    // so an integral value is accepted here rather than demanded as a float.
    Ok(resolve_blob_value(view, field)
        .and_then(|value| value.as_f64())
        .filter(|number| number.is_finite()))
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
    Ok(resolve_blob_value(view, field).and_then(|value| value.as_bool()))
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
    Ok(resolve_singular_list(view, field))
}

/// Resolve a list `field` from a convention that states it as a single string,
/// yielding a one-element list.
///
/// Kept out of [`resolve_str_list`]'s main loop because the two cardinalities
/// cannot share a key list: `extract_string_list` rejects a non-array value,
/// and that rejection drops the entire operations row. A singular key listed
/// beside the array keys would therefore not merely fail to resolve — it would
/// discard the span's whole projection.
fn resolve_singular_list(view: &AttributeView, field: OperationField) -> Option<Vec<String>> {
    for convention in CONVENTIONS {
        for &key in convention.singular_list_field_keys(field) {
            if let Some(value) = extract_string_value(view.get(key)) {
                return Some(vec![value]);
            }
        }
    }
    None
}

/// Resolve the first present JSON-serialized content for `field`.
///
/// # Errors
///
/// Returns `IngestError::Validation` if the field's precedence slice is
/// unavailable (an internal registry invariant; see [`field_precedence`]).
fn resolve_json(view: &AttributeView, field: OperationField) -> Result<Option<String>> {
    for &key in field_precedence(field)? {
        if let Some(value) = view.get(key)
            && let Some(json) = serialize_any_value_to_json(Some(value))
        {
            return Ok(Some(json));
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
            if event_names.contains(&event.name.as_str())
                && let Some(json) = serialize_all_attrs_to_json_object(&event.attributes)
            {
                return Some(json);
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
            if let Some(content) = content
                && let Some(json) = serialize_message_to_json_array(role, &content)
            {
                return Some(json);
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

/// Returns the first registered convention that reads one flat result off the
/// span's own attributes and whose [`EvaluationField::Name`] key is present
/// there. `None` when no span-level result exists; a score or label without a
/// name is not a result.
fn find_flat_evaluation_convention(view: &AttributeView) -> Option<&'static dyn OperationConvention> {
    CONVENTIONS.iter().copied().find(|convention| {
        convention.states_flat_evaluation()
            && convention
                .evaluation_field_keys(EvaluationField::Name)
                .iter()
                .any(|key| view.has(key))
    })
}

/// Returns the first registered convention that names `event` as an
/// evaluation-result carrier.
fn find_evaluation_event_convention(event: &Event) -> Option<&'static dyn OperationConvention> {
    CONVENTIONS
        .iter()
        .copied()
        .find(|convention| convention.evaluation_event_names().contains(&event.name.as_str()))
}

/// Whether `span` carries any evaluation result: a flat result on its own
/// attributes, a registered attribute array, or an evaluation event.
///
/// Runs for every span that no operation marker qualified, so each check is a
/// hash lookup or a pass over the span's events — never a scan of its keys.
fn carries_evaluation(view: &AttributeView, span: &Span) -> bool {
    find_flat_evaluation_convention(view).is_some()
        || registered_evaluation_arrays()
            .iter()
            .any(|array| view.has(&array.first_name_key))
        || span
            .events
            .iter()
            .any(|event| evaluation_event_filter().contains(&event.name.as_str()))
}

/// What the results recorded on one span are about.
#[derive(Debug, Clone, Copy)]
enum EvaluationSubject {
    /// A known span: the span itself when results are recorded inline on an
    /// operation, or the span a post-hoc carrier's single link points at.
    Span {
        /// Trace of the subject span.
        trace_id: [u8; 16],
        /// The subject span.
        span_id: [u8; 8],
    },
    /// An evaluation span that points at no single span: the evaluated span is
    /// unknown, though the trace the results are recorded in is not.
    Trace {
        /// The trace the evaluation span is recorded in.
        trace_id: [u8; 16],
    },
    /// A carrier whose single link carries unusable ids.
    Unknown,
}

/// Decide what the results recorded on `span` are about.
///
/// Results recorded on any operation other than an evaluation are about that
/// operation's own span. An evaluation span's results describe something else:
/// the span its single link points at when it is a post-hoc carrier (exactly
/// one link, none dropped by the SDK), and otherwise no span it identifies.
/// Parentage is never taken for the target — the conventions do not give a
/// parent that meaning.
fn resolve_evaluation_subject(
    span: &Span,
    is_evaluation_span: bool,
    trace_id: [u8; 16],
    span_id: [u8; 8],
) -> EvaluationSubject {
    if !is_evaluation_span {
        return EvaluationSubject::Span { trace_id, span_id };
    }
    match span.links.as_slice() {
        [link] if span.dropped_links_count == 0 => {
            let linked_trace = <[u8; 16]>::try_from(link.trace_id.as_slice())
                .ok()
                .filter(|id| !is_zero_bytes(id));
            let linked_span = <[u8; 8]>::try_from(link.span_id.as_slice())
                .ok()
                .filter(|id| !is_zero_bytes(id));
            match (linked_trace, linked_span) {
                (Some(trace_id), Some(span_id)) => EvaluationSubject::Span { trace_id, span_id },
                (None, _) | (_, None) => EvaluationSubject::Unknown,
            }
        }
        _ => EvaluationSubject::Trace { trace_id },
    }
}

/// The `(target_trace_id, target_span_id)` of a result with `scope` whose
/// carrying span is about `subject`. A session result names neither: its
/// session is the row's `conversation_id`.
const fn resolve_evaluation_target(
    scope: EvaluationScope,
    subject: EvaluationSubject,
) -> (Option<[u8; 16]>, Option<[u8; 8]>) {
    match (scope, subject) {
        (EvaluationScope::Span, EvaluationSubject::Span { trace_id, span_id }) => (Some(trace_id), Some(span_id)),
        (EvaluationScope::Trace, EvaluationSubject::Span { trace_id, .. } | EvaluationSubject::Trace { trace_id }) => {
            (Some(trace_id), None)
        }
        (EvaluationScope::Span, EvaluationSubject::Trace { .. } | EvaluationSubject::Unknown)
        | (EvaluationScope::Trace, EvaluationSubject::Unknown)
        | (
            EvaluationScope::Session,
            EvaluationSubject::Span { .. } | EvaluationSubject::Trace { .. } | EvaluationSubject::Unknown,
        ) => (None, None),
    }
}

/// Where one evaluation result is read from, which decides the fields it
/// states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvaluationCarrier {
    /// A result's own attribute set — an evaluation event or an array element:
    /// every field found there belongs to the result.
    ResultAttributes,
    /// The span's own attributes read as one flat result: only the metric and
    /// its outcome belong to it. `error.type` and the response id there
    /// describe the span's own operation, and stay in the row's columns.
    SpanAttributes,
}

impl EvaluationCarrier {
    /// Whether a result read from this carrier states `field`.
    const fn states(self, field: EvaluationField) -> bool {
        match self {
            Self::ResultAttributes => true,
            Self::SpanAttributes => matches!(
                field,
                EvaluationField::Name
                    | EvaluationField::ScoreValue
                    | EvaluationField::ScoreLabel
                    | EvaluationField::Explanation
            ),
        }
    }
}

/// Last value `key` carries in `attributes`, skipping value-less entries: the
/// answer [`AttributeView::get`] gives, without building its index for the few
/// keys one evaluation event is read through.
fn last_attribute_value<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a AnyValue> {
    attributes
        .iter()
        .rev()
        .find_map(|kv| if kv.key == key { kv.value.as_ref() } else { None })
}

/// `text` when it holds a JSON object — the only shape `metadata` may store —
/// checked without building the value.
fn json_object_text(text: String) -> Option<String> {
    let is_object = text.trim_start().starts_with('{') && serde_json::from_str::<serde::de::IgnoredAny>(&text).is_ok();
    is_object.then_some(text)
}

/// How one result is read: the convention whose keys name its fields, where its
/// attributes sit, and what it is about.
struct EvaluationReading<'c> {
    /// Convention whose [`OperationConvention::evaluation_field_keys`] apply.
    convention: &'c dyn OperationConvention,
    /// Where the attributes sit, which decides the fields read.
    carrier: EvaluationCarrier,
    /// What the result is about.
    scope: EvaluationScope,
    /// What the carrying span's results are about.
    subject: EvaluationSubject,
}

/// Resolve one evaluation result, reading each key's value with `lookup`;
/// `None` when the result is incomplete (see [`is_complete_evaluation`]).
///
/// Strings are read verbatim (as every string column is), except `metadata`,
/// kept only when it is a JSON object; `score_value` is a strict `f64` parse,
/// so a present non-numeric score is a projection failure that drops the whole
/// row (D6).
///
/// # Errors
///
/// Returns `IngestError::Validation` when a present score fails strict parsing.
fn resolve_evaluation_result<'a>(
    lookup: impl Fn(&str) -> Option<&'a AnyValue>,
    reading: &EvaluationReading<'_>,
) -> Result<Option<EvaluationResult>> {
    let keys = |field: EvaluationField| -> &'static [&'static str] {
        if reading.carrier.states(field) {
            reading.convention.evaluation_field_keys(field)
        } else {
            &[]
        }
    };
    let first_str = |field: EvaluationField| keys(field).iter().find_map(|&key| extract_string_value(lookup(key)));
    let mut score_value = None;
    for &key in keys(EvaluationField::ScoreValue) {
        if let Some(value) = lookup(key) {
            score_value = extract_f64(Some(value), "evaluations.score_value")?;
            break;
        }
    }
    let name = first_str(EvaluationField::Name);
    let score_label = first_str(EvaluationField::ScoreLabel);
    let explanation = first_str(EvaluationField::Explanation);
    let error_type = first_str(EvaluationField::ErrorType);
    let has_outcome = score_value.is_some() || score_label.is_some() || explanation.is_some() || error_type.is_some();
    let Some(name) = name.filter(|name| is_complete_evaluation(name, has_outcome)) else {
        return Ok(None);
    };
    let (target_trace_id, target_span_id) = resolve_evaluation_target(reading.scope, reading.subject);
    Ok(Some(EvaluationResult {
        name,
        score_value,
        score_label,
        explanation,
        response_id: first_str(EvaluationField::ResponseId),
        error_type,
        annotator_kind: first_str(EvaluationField::AnnotatorKind),
        identifier: first_str(EvaluationField::Identifier),
        metadata: first_str(EvaluationField::Metadata).and_then(json_object_text),
        target_scope: reading.scope,
        target_trace_id,
        target_span_id,
    }))
}

/// Split an attribute key of `source` into its element index and field suffix;
/// `None` for a key that only shares the prefix.
fn split_array_key<'k>(key: &'k str, source: &EvaluationArraySource) -> Option<(usize, &'k str)> {
    let (index, rest) = key.strip_prefix(source.prefix)?.strip_prefix('.')?.split_once('.')?;
    let index = index.parse::<usize>().ok()?;
    let field = rest.strip_prefix(source.element)?.strip_prefix('.')?;
    Some((index, field))
}

/// Resolve every element of `array` found in `attributes`, one result per
/// index in ascending order, into `found`.
///
/// # Errors
///
/// Returns `IngestError::Validation` when an element's present score fails
/// strict parsing (D6).
fn resolve_array_evaluations(
    attributes: &[KeyValue],
    array: &RegisteredEvaluationArray,
    subject: EvaluationSubject,
    found: &mut Vec<Option<EvaluationResult>>,
) -> Result<()> {
    let mut elements: BTreeMap<usize, Vec<(&str, &AnyValue)>> = BTreeMap::new();
    for kv in attributes {
        if let Some(value) = kv.value.as_ref()
            && let Some((index, field)) = split_array_key(&kv.key, array.source)
        {
            elements.entry(index).or_default().push((field, value));
        }
    }
    let reading = EvaluationReading {
        convention: array.convention,
        carrier: EvaluationCarrier::ResultAttributes,
        scope: array.source.scope,
        subject,
    };
    for fields in elements.values() {
        // Last write wins within an element, as it does across span attributes.
        let lookup = |key: &str| fields.iter().rev().find_map(|&(field, value)| (field == key).then_some(value));
        found.push(resolve_evaluation_result(lookup, &reading)?);
    }
    Ok(())
}

/// Evaluation results resolved from one span.
struct ResolvedEvaluations {
    /// Complete results in resolution order; `None` when none survived, so the
    /// column is a NULL list rather than an empty one.
    results: Option<Vec<EvaluationResult>>,
    /// Results found on the span but left out by [`is_complete_evaluation`].
    skipped: usize,
}

/// Whether a result is one the `evaluations` column stores: it names its
/// metric and carries an outcome — a score, a label, an explanation, or the
/// error the evaluation ended in. Without a name a score cannot be attributed
/// to any metric, and without an outcome there is nothing to store.
const fn is_complete_evaluation(name: &str, has_outcome: bool) -> bool {
    !name.is_empty() && has_outcome
}

/// Resolve every evaluation result `span` carries, in this order: the flat
/// result on its own attributes, then each registered attribute array present
/// on it (registry order, elements by index), then one result per evaluation
/// event (event order). Incomplete results are counted and left out rather
/// than failing the row.
///
/// # Errors
///
/// Returns `IngestError::Validation` when any present score fails strict
/// parsing (D6): the whole row is dropped, never a partial list.
fn resolve_evaluations(view: &AttributeView, span: &Span, subject: EvaluationSubject) -> Result<ResolvedEvaluations> {
    let mut found: Vec<Option<EvaluationResult>> = Vec::new();
    if let Some(convention) = find_flat_evaluation_convention(view) {
        let reading = EvaluationReading {
            convention,
            carrier: EvaluationCarrier::SpanAttributes,
            scope: EvaluationScope::Span,
            subject,
        };
        found.push(resolve_evaluation_result(|key| view.get(key), &reading)?);
    }
    for array in registered_evaluation_arrays() {
        if view.has(&array.first_name_key) {
            resolve_array_evaluations(&span.attributes, array, subject, &mut found)?;
        }
    }
    for event in &span.events {
        // The cached name filter settles most events without asking any
        // convention; only an evaluation event pays for the owner lookup.
        if !evaluation_event_filter().contains(&event.name.as_str()) {
            continue;
        }
        if let Some(convention) = find_evaluation_event_convention(event) {
            let reading = EvaluationReading {
                convention,
                carrier: EvaluationCarrier::ResultAttributes,
                scope: EvaluationScope::Span,
                subject,
            };
            found.push(resolve_evaluation_result(
                |key| last_attribute_value(&event.attributes, key),
                &reading,
            )?);
        }
    }
    let found_count = found.len();
    let results: Vec<EvaluationResult> = found.into_iter().flatten().collect();
    Ok(ResolvedEvaluations {
        skipped: found_count - results.len(),
        results: (!results.is_empty()).then_some(results),
    })
}

/// Project one OTLP span (+ scope + tenant) into an optional operations row.
///
/// `Ok(None)` = no convention marker and no evaluation evidence present
/// (non-LLM span; caller counts as `non_llm_skipped`). `Err` = hard projection
/// failure (invalid id, or a failed strict typed parse / token overflow under
/// D6); the caller drops the row and counts it in `drops`. Incomplete
/// evaluation results do not fail the row: they are left out of it and counted
/// in [`ProjectedOperation::skipped_evaluations`]. Pure: no I/O, no clock, no
/// globals — `ingested_at` (micros) is injected so a later backfill can replay
/// the original watermark.
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
    tenant_id: &TenantId,
    service_name: Option<&str>,
    ingested_at: i64,
) -> Result<Option<ProjectedOperation>> {
    let view = AttributeView::new(&span.attributes);

    // Evaluation evidence qualifies a span on its own — a dedicated evaluator
    // span has nothing else to qualify it — but it is kept apart from the
    // operation markers because it cannot say what kind of operation the span
    // is; see the `operation_name` fallback below.
    let qualified_by_operation_marker = CONVENTIONS.iter().any(|conv| {
        conv.marker_keys().iter().any(|key| view.has(key))
            || conv.name_prefixes().iter().any(|prefix| span.name.starts_with(prefix))
    });
    if !qualified_by_operation_marker && !carries_evaluation(&view, span) {
        return Ok(None);
    }

    let trace_id = validate_trace_id(&span.trace_id)?;
    let span_id = validate_span_id(&span.span_id)?;
    let parent_span_id = match <[u8; 8]>::try_from(span.parent_span_id.as_slice()) {
        Ok(arr) if !is_zero_bytes(&arr) => Some(arr),
        _ => None,
    };

    // Adapters decide first, so an LLM span carrying evaluation results keeps
    // its own operation. A span no adapter can classify is an evaluation only
    // when evaluation evidence alone qualified it (a dedicated evaluator span);
    // a span an operation marker qualified is an evaluated operation of an
    // unknown kind, so it stays `other`.
    let operation_name = CONVENTIONS
        .iter()
        .find_map(|conv| conv.classify_operation(&span.name, &view))
        .unwrap_or_else(|| {
            if qualified_by_operation_marker {
                "other".to_string()
            } else {
                EVALUATION_OPERATION_NAME.to_string()
            }
        });
    let subject = resolve_evaluation_subject(span, operation_name == EVALUATION_OPERATION_NAME, trace_id, span_id);
    let ResolvedEvaluations {
        results: evaluations,
        skipped: skipped_evaluations,
    } = resolve_evaluations(&view, span, subject)?;

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

    let row = OperationRow {
        tenant_id: tenant_id.as_ref().to_string(),
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
        evaluations,
    };
    Ok(Some(ProjectedOperation {
        row,
        skipped_evaluations,
    }))
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::trace::v1::{
        Span, Status,
        span::{Event, Link},
    };

    use super::*;
    use crate::error::IngestError;
    use crate::transform::test_support::test_tenant;

    /// Project `span` and keep only its row: most cases here are about the
    /// columns, not about the incomplete evaluation results left out of them.
    fn project_row(
        span: &Span,
        scope: Option<&InstrumentationScope>,
        tenant_id: &TenantId,
        service_name: Option<&str>,
        ingested_at: i64,
    ) -> Result<Option<OperationRow>> {
        project_operation_row(span, scope, tenant_id, service_name, ingested_at)
            .map(|projected| projected.map(|projected| projected.row))
    }

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
            evaluations: None,
        };

        assert_eq!(row.tenant_id, "tenant-a");
        assert_eq!(row.operation_name, "chat");
        assert_eq!(row.temperature, Some(0.7));
        assert_eq!(row.finish_reasons, Some(vec!["stop".to_string()]));
        assert_eq!(row.stop_sequences, None);
        assert_eq!(row.parent_span_id, None);
        assert_eq!(row.evaluations, None);
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

        let row = project_row(&span, None, &test_tenant("tenant-a"), Some("svc"), 999)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
            .expect("projection ok")
            .expect("llm span -> row");

        assert_eq!(row.request_model.as_deref(), Some("otel-model"));
        assert_eq!(row.provider_name.as_deref(), Some("otel-provider"));
    }

    #[test]
    fn minimal_matching_span_leaves_optionals_null() {
        let span = span_with(vec![kv_str("gen_ai.operation.name", "chat")]);

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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
        assert!(project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").is_none());
    }

    #[test]
    fn bad_trace_id_on_matching_span_is_err() {
        let mut span = span_with(vec![kv_str("gen_ai.operation.name", "chat")]);
        span.trace_id = vec![0u8; 16];
        assert!(project_row(&span, None, &test_tenant("t"), None, 1).is_err());
    }

    #[test]
    fn malformed_temperature_is_err() {
        let span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("gen_ai.request.temperature", "hot"),
        ]);
        assert!(project_row(&span, None, &test_tenant("t"), None, 1).is_err());
    }

    #[test]
    fn duration_micros_clamps_to_zero_when_end_before_start() {
        let mut span = span_with(vec![kv_str("gen_ai.operation.name", "chat")]);
        span.start_time_unix_nano = 3_000_000_000;
        span.end_time_unix_nano = 1_000_000_000;
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.duration_micros, 0);
    }

    /// Build a `gen_ai.evaluation.result` span event carrying the given attributes.
    fn evaluation_event(attributes: Vec<KeyValue>) -> Event {
        Event {
            time_unix_nano: 2_500_000_000,
            name: "gen_ai.evaluation.result".to_string(),
            attributes,
            dropped_attributes_count: 0,
        }
    }

    /// Trace id [`span_with`] gives every fixture span.
    const OWN_TRACE_ID: [u8; 16] = [1u8; 16];
    /// Span id [`span_with`] gives every fixture span.
    const OWN_SPAN_ID: [u8; 8] = [2u8; 8];

    /// A span-scoped result named `name` about the fixture span itself, with
    /// every other field empty; each case overrides the fields it is about.
    fn result_about_own_span(name: &str) -> EvaluationResult {
        EvaluationResult {
            name: name.to_string(),
            score_value: None,
            score_label: None,
            explanation: None,
            response_id: None,
            error_type: None,
            annotator_kind: None,
            identifier: None,
            metadata: None,
            target_scope: EvaluationScope::Span,
            target_trace_id: Some(OWN_TRACE_ID),
            target_span_id: Some(OWN_SPAN_ID),
        }
    }

    /// Build a span link to the given ids.
    fn link_to(trace_id: &[u8], span_id: &[u8]) -> Link {
        Link {
            trace_id: trace_id.to_vec(),
            span_id: span_id.to_vec(),
            trace_state: String::new(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            flags: 0,
        }
    }

    #[test]
    fn evaluation_events_on_an_llm_span_project_one_result_each_in_event_order() {
        let mut span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("gen_ai.response.id", "resp-1"),
        ]);
        span.events = vec![
            evaluation_event(vec![
                kv_str("gen_ai.evaluation.name", "Relevance"),
                kv_dbl("gen_ai.evaluation.score.value", 0.9),
                kv_str("gen_ai.evaluation.score.label", "relevant"),
                kv_str("gen_ai.response.id", "resp-1"),
            ]),
            evaluation_event(vec![
                kv_str("gen_ai.evaluation.name", "Fluency"),
                // An integer score is widened, as every double column is.
                kv_int("gen_ai.evaluation.score.value", 4),
                kv_str("gen_ai.evaluation.explanation", "reads naturally"),
                kv_str("error.type", "timeout"),
            ]),
        ];
        let row = project_row(&span, None, &test_tenant("tenant-a"), Some("svc"), 999)
            .expect("projection ok")
            .expect("llm span -> row");
        // The LLM span keeps its own classification, and results recorded on it
        // are about it.
        assert_eq!(row.operation_name, "chat");
        assert_eq!(
            row.evaluations,
            Some(vec![
                EvaluationResult {
                    score_value: Some(0.9),
                    score_label: Some("relevant".to_string()),
                    response_id: Some("resp-1".to_string()),
                    ..result_about_own_span("Relevance")
                },
                EvaluationResult {
                    score_value: Some(4.0),
                    explanation: Some("reads naturally".to_string()),
                    error_type: Some("timeout".to_string()),
                    ..result_about_own_span("Fluency")
                },
            ])
        );
        // An event's error.type belongs to that result, not to the row.
        assert_eq!(row.error_type, None);
    }

    #[test]
    fn dedicated_evaluator_span_with_only_an_evaluation_event_qualifies_as_evaluation() {
        let mut span = span_with(vec![kv_str("http.method", "POST")]);
        span.events = vec![evaluation_event(vec![
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_dbl("gen_ai.evaluation.score.value", 1.0),
        ])];
        let row = project_row(&span, None, &test_tenant("t"), None, 1)
            .expect("ok")
            .expect("event-only evaluator span -> row");
        assert_eq!(row.operation_name, "evaluation");
        // An evaluation span's results are about something else, and it names
        // no linked span, so what they evaluate is unknown.
        assert_eq!(
            row.evaluations,
            Some(vec![EvaluationResult {
                score_value: Some(1.0),
                target_trace_id: None,
                target_span_id: None,
                ..result_about_own_span("Relevance")
            }])
        );
    }

    #[test]
    fn an_unclassified_llm_span_carrying_evaluations_stays_other() {
        // A provider marker qualified the span, so it is an operation of a kind
        // no adapter can name; results evaluate it, they do not make it an
        // evaluator.
        let mut span = span_with(vec![kv_str("gen_ai.provider.name", "openai")]);
        span.events = vec![evaluation_event(vec![
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_dbl("gen_ai.evaluation.score.value", 0.9),
        ])];
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.operation_name, "other");
        assert_eq!(
            row.evaluations,
            Some(vec![EvaluationResult {
                score_value: Some(0.9),
                ..result_about_own_span("Relevance")
            }])
        );
    }

    #[test]
    fn a_flat_result_comes_before_event_results() {
        let mut span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("gen_ai.evaluation.name", "Groundedness"),
            kv_dbl("gen_ai.evaluation.score.value", 0.25),
            kv_str("gen_ai.evaluation.score.label", "fail"),
            kv_str("gen_ai.evaluation.explanation", "cites nothing"),
        ]);
        span.events = vec![evaluation_event(vec![
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_dbl("gen_ai.evaluation.score.value", 0.75),
        ])];
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(
            row.evaluations,
            Some(vec![
                EvaluationResult {
                    score_value: Some(0.25),
                    score_label: Some("fail".to_string()),
                    explanation: Some("cites nothing".to_string()),
                    ..result_about_own_span("Groundedness")
                },
                EvaluationResult {
                    score_value: Some(0.75),
                    ..result_about_own_span("Relevance")
                },
            ])
        );
    }

    #[test]
    fn a_flat_result_leaves_the_spans_own_error_and_response_id_to_the_row() {
        // On the span itself those keys describe the span's own operation — a
        // failed LLM call, the LLM's response — not the evaluation of it.
        let span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("error.type", "timeout"),
            kv_str("gen_ai.response.id", "resp-2"),
            kv_str("gen_ai.evaluation.name", "Groundedness"),
            kv_dbl("gen_ai.evaluation.score.value", 0.25),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.error_type, Some("timeout".to_string()));
        assert_eq!(row.response_id, Some("resp-2".to_string()));
        assert_eq!(
            row.evaluations,
            Some(vec![EvaluationResult {
                score_value: Some(0.25),
                ..result_about_own_span("Groundedness")
            }])
        );
    }

    #[test]
    fn dedicated_evaluator_span_with_only_evaluation_attributes_is_an_evaluation_row() {
        let mut span = span_with(vec![
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_dbl("gen_ai.evaluation.score.value", 0.9),
            kv_str("gen_ai.response.id", "resp-1"),
        ]);
        span.parent_span_id = vec![9u8; 8];
        let row = project_row(&span, None, &test_tenant("t"), None, 1)
            .expect("ok")
            .expect("evaluator span -> row");
        assert_eq!(row.operation_name, "evaluation");
        // Its parent stays on the row, but parentage never names what a result
        // is about: with no link, the target is unknown.
        assert_eq!(row.parent_span_id, Some([9u8; 8]));
        assert_eq!(row.response_id, Some("resp-1".to_string()));
        assert_eq!(
            row.evaluations,
            Some(vec![EvaluationResult {
                score_value: Some(0.9),
                target_trace_id: None,
                target_span_id: None,
                ..result_about_own_span("Relevance")
            }])
        );
    }

    #[test]
    fn openinference_span_evaluations_project_in_index_order_with_their_provenance() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("evaluations.0.evaluation.name", "hallucination"),
            kv_int("evaluations.0.evaluation.score", 1),
            kv_str("evaluations.0.evaluation.label", "hallucinated"),
            kv_str("evaluations.0.evaluation.explanation", "The claim is not supported."),
            kv_str("evaluations.0.evaluation.annotator_kind", "LLM"),
            kv_str("evaluations.0.evaluation.identifier", "judge-v2"),
            kv_str("evaluations.0.evaluation.metadata", "{\"rubric_version\":\"2\"}"),
            // Indices order as numbers, not as text, and need not be contiguous.
            kv_dbl("evaluations.10.evaluation.score", 0.3),
            kv_str("evaluations.10.evaluation.name", "toxicity"),
            kv_str("evaluations.2.evaluation.name", "relevance"),
            kv_str("evaluations.2.evaluation.label", "relevant"),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.operation_name, "chat");
        assert_eq!(
            row.evaluations,
            Some(vec![
                EvaluationResult {
                    score_value: Some(1.0),
                    score_label: Some("hallucinated".to_string()),
                    explanation: Some("The claim is not supported.".to_string()),
                    annotator_kind: Some("LLM".to_string()),
                    identifier: Some("judge-v2".to_string()),
                    metadata: Some("{\"rubric_version\":\"2\"}".to_string()),
                    ..result_about_own_span("hallucination")
                },
                EvaluationResult {
                    score_label: Some("relevant".to_string()),
                    ..result_about_own_span("relevance")
                },
                EvaluationResult {
                    score_value: Some(0.3),
                    ..result_about_own_span("toxicity")
                },
            ])
        );
    }

    #[test]
    fn openinference_annotations_are_read_as_evaluations() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "CHAIN"),
            kv_str("annotations.0.annotation.name", "hallucination"),
            kv_int("annotations.0.annotation.score", 0),
            kv_str("annotations.0.annotation.annotator_kind", "HUMAN"),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(
            row.evaluations,
            Some(vec![EvaluationResult {
                score_value: Some(0.0),
                annotator_kind: Some("HUMAN".to_string()),
                ..result_about_own_span("hallucination")
            }])
        );
    }

    #[test]
    fn openinference_trace_and_session_feedback_keep_their_scope() {
        // Neither is about the span it is recorded on, so neither may read as
        // span feedback: a trace result names the trace, a session result the
        // session in the row's `conversation_id`.
        let span = span_with(vec![
            kv_str("openinference.span.kind", "CHAIN"),
            kv_str("session.id", "session-123"),
            kv_str("trace.evaluations.0.evaluation.name", "retrieval_quality"),
            kv_dbl("trace.evaluations.0.evaluation.score", 0.92),
            kv_str("session.annotations.0.annotation.name", "conversational_coherence"),
            kv_str("session.annotations.0.annotation.label", "coherent"),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.conversation_id, Some("session-123".to_string()));
        assert_eq!(
            row.evaluations,
            Some(vec![
                EvaluationResult {
                    score_value: Some(0.92),
                    target_scope: EvaluationScope::Trace,
                    target_span_id: None,
                    ..result_about_own_span("retrieval_quality")
                },
                EvaluationResult {
                    score_label: Some("coherent".to_string()),
                    target_scope: EvaluationScope::Session,
                    target_trace_id: None,
                    target_span_id: None,
                    ..result_about_own_span("conversational_coherence")
                },
            ])
        );
    }

    #[test]
    fn a_span_carrying_only_openinference_evaluations_is_an_evaluation() {
        let span = span_with(vec![
            kv_str("evaluations.0.evaluation.name", "relevance"),
            kv_dbl("evaluations.0.evaluation.score", 0.8),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1)
            .expect("ok")
            .expect("evaluation-only span -> row");
        assert_eq!(row.operation_name, "evaluation");
        assert_eq!(
            row.evaluations,
            Some(vec![EvaluationResult {
                score_value: Some(0.8),
                target_trace_id: None,
                target_span_id: None,
                ..result_about_own_span("relevance")
            }])
        );
    }

    #[test]
    fn openinference_metadata_that_is_not_a_json_object_is_left_null() {
        // The field's contract is a JSON object; anything else is dropped from
        // the result rather than stored as if it were one.
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("evaluations.0.evaluation.name", "relevance"),
            kv_dbl("evaluations.0.evaluation.score", 0.8),
            kv_str("evaluations.0.evaluation.metadata", "[1, 2]"),
            kv_str("evaluations.1.evaluation.name", "fluency"),
            kv_dbl("evaluations.1.evaluation.score", 0.6),
            kv_str("evaluations.1.evaluation.metadata", "not json"),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        let results = row.evaluations.expect("both results kept");
        assert_eq!(
            results.iter().map(|result| result.metadata.clone()).collect::<Vec<_>>(),
            vec![None, None]
        );
    }

    #[test]
    fn an_openinference_array_is_read_only_through_its_named_first_element() {
        // Indices are zero-based and every element is named, so a named first
        // element is how an array is recognized without scanning every key of
        // every span.
        let unmarked = span_with(vec![
            kv_str("evaluations.1.evaluation.name", "relevance"),
            kv_dbl("evaluations.1.evaluation.score", 0.8),
        ]);
        assert_eq!(
            project_row(&unmarked, None, &test_tenant("t"), None, 1).expect("ok"),
            None
        );
        let mut marked = unmarked;
        marked.attributes.push(kv_str("gen_ai.operation.name", "chat"));
        let row = project_row(&marked, None, &test_tenant("t"), None, 1)
            .expect("ok")
            .expect("row");
        assert_eq!(row.evaluations, None);
    }

    #[test]
    fn keys_that_only_share_an_evaluation_prefix_are_not_results() {
        let span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("evaluations.0.evaluation.name", "relevance"),
            kv_dbl("evaluations.0.evaluation.score", 0.8),
            kv_int("evaluations.count", 1),
            kv_str("evaluations.x.evaluation.name", "not an index"),
            kv_str("evaluations.1.other.name", "not the element"),
            kv_str("evaluationsx.1.evaluation.name", "not the prefix"),
        ]);
        let projected = project_operation_row(&span, None, &test_tenant("t"), None, 1)
            .expect("ok")
            .expect("row");
        assert_eq!(projected.skipped_evaluations, 0);
        assert_eq!(
            projected.row.evaluations,
            Some(vec![EvaluationResult {
                score_value: Some(0.8),
                ..result_about_own_span("relevance")
            }])
        );
    }

    #[test]
    fn element_field_suffixes_are_never_read_off_the_span_itself() {
        // `OpenInference` keys an element's fields by bare suffixes; a span that
        // merely has attributes spelled that way states no result.
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("name", "hallucination"),
            kv_int("score", 1),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.evaluations, None);
    }

    #[test]
    fn attribute_results_come_before_event_results() {
        let mut span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("evaluations.0.evaluation.name", "relevance"),
            kv_dbl("evaluations.0.evaluation.score", 0.8),
        ]);
        span.events = vec![evaluation_event(vec![
            kv_str("gen_ai.evaluation.name", "Fluency"),
            kv_dbl("gen_ai.evaluation.score.value", 0.6),
        ])];
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        let names: Vec<String> = row
            .evaluations
            .expect("two results")
            .into_iter()
            .map(|result| result.name)
            .collect();
        assert_eq!(names, vec!["relevance".to_string(), "Fluency".to_string()]);
    }

    #[test]
    fn an_evaluation_span_with_one_link_is_about_the_linked_span() {
        // A post-hoc carrier: the results describe the span its single link
        // points at, not the carrier.
        let mut span = span_with(vec![
            kv_str("openinference.span.kind", "EVALUATOR"),
            kv_str("evaluations.0.evaluation.name", "hallucination"),
            kv_str("evaluations.0.evaluation.label", "hallucinated"),
            kv_str("trace.evaluations.0.evaluation.name", "retrieval_quality"),
            kv_dbl("trace.evaluations.0.evaluation.score", 0.5),
        ]);
        span.links = vec![link_to(&[7u8; 16], &[8u8; 8])];
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.operation_name, "evaluation");
        assert_eq!(
            row.evaluations,
            Some(vec![
                EvaluationResult {
                    score_label: Some("hallucinated".to_string()),
                    target_trace_id: Some([7u8; 16]),
                    target_span_id: Some([8u8; 8]),
                    ..result_about_own_span("hallucination")
                },
                EvaluationResult {
                    score_value: Some(0.5),
                    target_scope: EvaluationScope::Trace,
                    target_trace_id: Some([7u8; 16]),
                    target_span_id: None,
                    ..result_about_own_span("retrieval_quality")
                },
            ])
        );
    }

    #[test]
    fn an_evaluation_event_on_a_linked_evaluation_span_is_about_the_linked_span() {
        let mut span = span_with(Vec::new());
        span.links = vec![link_to(&[7u8; 16], &[8u8; 8])];
        span.events = vec![evaluation_event(vec![
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_dbl("gen_ai.evaluation.score.value", 0.4),
        ])];
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.operation_name, "evaluation");
        assert_eq!(
            row.evaluations,
            Some(vec![EvaluationResult {
                score_value: Some(0.4),
                target_trace_id: Some([7u8; 16]),
                target_span_id: Some([8u8; 8]),
                ..result_about_own_span("Relevance")
            }])
        );
    }

    #[test]
    fn a_non_evaluation_span_keeps_its_results_even_with_one_link() {
        // A link on an LLM span relates it to other work; its own results are
        // still about itself.
        let mut span = span_with(vec![kv_str("gen_ai.operation.name", "chat")]);
        span.links = vec![link_to(&[7u8; 16], &[8u8; 8])];
        span.events = vec![evaluation_event(vec![
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_dbl("gen_ai.evaluation.score.value", 0.4),
        ])];
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(
            row.evaluations,
            Some(vec![EvaluationResult {
                score_value: Some(0.4),
                ..result_about_own_span("Relevance")
            }])
        );
    }

    #[test]
    fn an_evaluation_span_without_exactly_one_usable_link_has_no_known_target() {
        let event = evaluation_event(vec![
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_dbl("gen_ai.evaluation.score.value", 0.4),
        ]);
        let mut several = span_with(Vec::new());
        several.links = vec![link_to(&[7u8; 16], &[8u8; 8]), link_to(&[5u8; 16], &[6u8; 8])];
        let mut partly_dropped = span_with(Vec::new());
        partly_dropped.links = vec![link_to(&[7u8; 16], &[8u8; 8])];
        partly_dropped.dropped_links_count = 1;
        let mut invalid = span_with(Vec::new());
        invalid.links = vec![link_to(&[0u8; 16], &[8u8; 8])];
        for mut span in [several, partly_dropped, invalid] {
            span.events = vec![event.clone()];
            let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
            assert_eq!(
                row.evaluations,
                Some(vec![EvaluationResult {
                    score_value: Some(0.4),
                    target_trace_id: None,
                    target_span_id: None,
                    ..result_about_own_span("Relevance")
                }]),
                "links: {:?}, dropped: {}",
                span.links.len(),
                span.dropped_links_count
            );
        }
    }

    #[test]
    fn malformed_evaluation_score_drops_the_row_wherever_it_sits() {
        // D6: a present, non-numeric score is a projection failure for a flat
        // result, an event result, and an array element alike.
        let flat = span_with(vec![
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_str("gen_ai.evaluation.score.value", "high"),
        ]);
        let mut event = span_with(vec![kv_str("gen_ai.operation.name", "chat")]);
        event.events = vec![evaluation_event(vec![
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_str("gen_ai.evaluation.score.value", "high"),
        ])];
        let array = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("evaluations.0.evaluation.name", "relevance"),
            kv_str("evaluations.0.evaluation.score", "high"),
        ]);
        for span in [flat, event, array] {
            let error =
                project_row(&span, None, &test_tenant("t"), None, 1).expect_err("strict parse failure drops the row");
            assert!(matches!(error, IngestError::Validation(_)), "{error}");
        }
    }

    #[test]
    fn incomplete_evaluation_results_are_skipped_and_counted_without_dropping_the_row() {
        // A result must name its metric and carry an outcome: a score, a label,
        // an explanation, or the error the evaluation ended in. One that does
        // not cannot be attributed to anything, so it is left out, not stored.
        let mut span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("evaluations.0.evaluation.name", "relevance"),
            kv_str("evaluations.0.evaluation.annotator_kind", "HUMAN"),
        ]);
        span.events = vec![
            evaluation_event(vec![kv_dbl("gen_ai.evaluation.score.value", 0.5)]),
            evaluation_event(Vec::new()),
            evaluation_event(vec![kv_str("gen_ai.evaluation.name", "Relevance")]),
            evaluation_event(vec![
                kv_str("gen_ai.evaluation.name", ""),
                kv_dbl("gen_ai.evaluation.score.value", 0.1),
            ]),
            evaluation_event(vec![
                kv_str("gen_ai.evaluation.name", "Fluency"),
                kv_str("error.type", "timeout"),
            ]),
        ];
        let projected = project_operation_row(&span, None, &test_tenant("t"), None, 1)
            .expect("an incomplete result never fails the row")
            .expect("row");
        assert_eq!(projected.skipped_evaluations, 5);
        assert_eq!(projected.row.operation_name, "chat");
        assert_eq!(
            projected.row.evaluations,
            Some(vec![EvaluationResult {
                error_type: Some("timeout".to_string()),
                ..result_about_own_span("Fluency")
            }])
        );
    }

    #[test]
    fn a_repeated_key_in_an_evaluation_event_resolves_to_its_last_value() {
        // The same last-write-wins rule the span's own attributes follow; an
        // entry without a value does not hide the one before it.
        let mut span = span_with(vec![kv_str("gen_ai.operation.name", "chat")]);
        span.events = vec![evaluation_event(vec![
            kv_str("gen_ai.evaluation.name", "Draft"),
            kv_dbl("gen_ai.evaluation.score.value", 0.1),
            kv_str("gen_ai.evaluation.name", "Relevance"),
            kv_dbl("gen_ai.evaluation.score.value", 0.9),
            KeyValue {
                key_strindex: 0,
                key: "gen_ai.evaluation.name".to_string(),
                value: None,
            },
        ])];
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(
            row.evaluations,
            Some(vec![EvaluationResult {
                score_value: Some(0.9),
                ..result_about_own_span("Relevance")
            }])
        );
    }

    #[test]
    fn a_span_whose_results_are_all_incomplete_stores_a_null_list() {
        let mut span = span_with(vec![kv_str("gen_ai.operation.name", "chat")]);
        span.events = vec![evaluation_event(vec![kv_dbl("gen_ai.evaluation.score.value", 0.5)])];
        let projected = project_operation_row(&span, None, &test_tenant("t"), None, 1)
            .expect("ok")
            .expect("row");
        assert_eq!(projected.skipped_evaluations, 1);
        assert_eq!(projected.row.evaluations, None);
    }

    #[test]
    fn error_type_and_response_id_alone_never_make_an_evaluation_result() {
        // Both keys are shared with row-level columns; only the evaluation name
        // opens a flat result.
        let span = span_with(vec![
            kv_str("gen_ai.operation.name", "chat"),
            kv_str("error.type", "timeout"),
            kv_str("gen_ai.response.id", "resp-1"),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.evaluations, None);
        assert_eq!(row.error_type, Some("timeout".to_string()));
    }

    #[test]
    fn a_score_without_a_name_does_not_qualify_a_span() {
        let span = span_with(vec![kv_dbl("gen_ai.evaluation.score.value", 0.9)]);
        assert_eq!(project_row(&span, None, &test_tenant("t"), None, 1).expect("ok"), None);
    }

    #[test]
    fn an_unrelated_span_event_does_not_qualify_a_span() {
        let mut span = span_with(vec![kv_str("http.method", "GET")]);
        span.events = vec![Event {
            time_unix_nano: 1_500_000_000,
            name: "exception".to_string(),
            attributes: vec![kv_str("exception.type", "IOError")],
            dropped_attributes_count: 0,
        }];
        assert_eq!(project_row(&span, None, &test_tenant("t"), None, 1).expect("ok"), None);
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

        let row = project_row(&span, None, &test_tenant("tenant-a"), Some("claude-code"), 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
            .expect("projection ok")
            .expect("row");

        let input: serde_json::Value =
            serde_json::from_str(row.input_messages.as_deref().expect("present")).expect("json");
        assert_eq!(input[0]["content"], "whole");
    }

    #[test]
    fn openinference_sampling_params_come_out_of_the_invocation_parameters_blob() {
        // OpenInference declares no individual sampling attributes; an SDK puts
        // the whole request payload in one JSON attribute, so these nine typed
        // columns are reachable only through it.
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str(
                "llm.invocation_parameters",
                r#"{"temperature":0.7,"top_p":0.95,"top_k":40,"max_tokens":512,
                    "frequency_penalty":0.1,"presence_penalty":-0.2,"seed":7,"stream":true,"n":3}"#,
            ),
        ]);

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
            .expect("projection ok")
            .expect("row");

        assert!((row.temperature.expect("temperature") - 0.7).abs() < f64::EPSILON);
        assert!((row.top_p.expect("top_p") - 0.95).abs() < f64::EPSILON);
        assert_eq!(row.top_k, Some(40));
        assert_eq!(row.max_tokens, Some(512));
        assert!((row.frequency_penalty.expect("frequency") - 0.1).abs() < f64::EPSILON);
        assert!((row.presence_penalty.expect("presence") + 0.2).abs() < f64::EPSILON);
        assert_eq!(row.seed, Some(7));
        assert_eq!(row.stream, Some(true));
        assert_eq!(row.choice_count, Some(3), "OpenAI spells choice count `n`");
    }

    #[test]
    fn max_completion_tokens_is_accepted_as_max_tokens() {
        // The Responses API and the reasoning models spell it this way; the
        // published TRAIL dataset uses it on 1,508 spans.
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("llm.invocation_parameters", r#"{"max_completion_tokens":8192}"#),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.max_tokens, Some(8192));
    }

    #[test]
    fn an_integral_float_in_the_blob_fills_the_integer_column() {
        // An SDK that keeps these as floats serializes `40.0`, which is the
        // same JSON number as `40`. Reading it as NULL would lose the value.
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str(
                "llm.invocation_parameters",
                r#"{"top_k":40.0,"max_tokens":512.0,"seed":7.0,"n":3.0}"#,
            ),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.top_k, Some(40));
        assert_eq!(row.max_tokens, Some(512));
        assert_eq!(row.seed, Some(7));
        assert_eq!(row.choice_count, Some(3));
    }

    #[test]
    fn the_integral_float_cast_is_bounded_at_the_last_exact_integer() {
        // Driven through the values rather than through JSON text: serde_json's
        // own parser is off by an ulp for some literals immediately under the
        // bound, which would hide where this rule actually cuts.
        for (number, expected) in [
            (9_007_199_254_740_991.0_f64, Some(9_007_199_254_740_991)),
            (-9_007_199_254_740_991.0_f64, Some(-9_007_199_254_740_991)),
            (9_007_199_254_740_992.0_f64, None),
            (-9_007_199_254_740_992.0_f64, None),
        ] {
            let value = serde_json::Value::from(number);
            assert_eq!(
                blob_number_as_i64(&value),
                expected,
                "{number} at the exact-integer bound"
            );
        }
    }

    #[test]
    fn a_fractional_or_out_of_range_number_leaves_the_integer_column_null() {
        // A fraction is not the integer the column holds, and past 2^53 a whole
        // float no longer names one integer: `9007199254740993.0` reaches this
        // code already rounded onto a neighbour. Both stay NULL rather than
        // storing a number the payload never stated.
        for blob in [
            r#"{"max_tokens":512.5}"#,
            r#"{"max_tokens":1e30}"#,
            r#"{"max_tokens":-1e30}"#,
            r#"{"max_tokens":9007199254740993.0}"#,
            r#"{"max_tokens":-9007199254740993.0}"#,
        ] {
            let span = span_with(vec![
                kv_str("openinference.span.kind", "LLM"),
                kv_int("llm.token_count.total", 42),
                kv_str("llm.invocation_parameters", blob),
            ]);
            let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
            assert_eq!(row.max_tokens, None, "blob {blob} must not populate max_tokens");
            assert_eq!(row.total_tokens, Some(42), "the rest of the row survives blob {blob}");
        }
    }

    #[test]
    fn a_typed_attribute_wins_over_the_same_field_inside_a_blob() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_dbl("gen_ai.request.temperature", 0.1),
            kv_str("llm.invocation_parameters", r#"{"temperature":0.9}"#),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert!((row.temperature.expect("temperature") - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn a_malformed_blob_leaves_the_column_null_without_dropping_the_row() {
        // A blob is an opaque vendor payload, not a typed attribute the
        // convention promises, so a surprise inside it must not cost the span
        // its tokens, model, and messages.
        for blob in [
            "not json at all",
            r#"{"temperature":"hot"}"#,
            r#"["temperature", 0.7]"#,
            r#"{"temperature":null}"#,
        ] {
            let span = span_with(vec![
                kv_str("openinference.span.kind", "LLM"),
                kv_int("llm.token_count.total", 42),
                kv_str("llm.invocation_parameters", blob),
            ]);
            let row = project_row(&span, None, &test_tenant("t"), None, 1)
                .expect("a bad blob must not fail the projection")
                .expect("row survives");
            assert_eq!(row.temperature, None, "blob {blob} must not populate temperature");
            assert_eq!(row.total_tokens, Some(42), "the rest of the row survives blob {blob}");
        }
    }

    #[test]
    fn openinference_tool_attributes_project_onto_the_tool_columns() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "TOOL"),
            kv_str("tool.name", "web_search"),
            kv_str("tool.description", "Search the web"),
            kv_str("tool.id", "call_62136355"),
            kv_str("tool.parameters", r#"{"q":"string"}"#),
        ]);

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
            .expect("projection ok")
            .expect("row");

        assert_eq!(row.operation_name, "execute_tool");
        assert_eq!(row.tool_name.as_deref(), Some("web_search"));
        assert_eq!(row.tool_description.as_deref(), Some("Search the web"));
        assert_eq!(row.tool_call_id.as_deref(), Some("call_62136355"));
        assert_eq!(row.tool_definitions.as_deref(), Some(r#"{"q":"string"}"#));
    }

    #[test]
    fn a_tool_json_schema_is_preferred_over_bare_parameters() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "TOOL"),
            kv_str("tool.parameters", r#"{"q":"string"}"#),
            kv_str("tool.json_schema", r#"{"type":"function"}"#),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(
            row.tool_definitions.as_deref(),
            Some(r#"{"type":"function"}"#),
            "the complete definition wins over the argument shape alone"
        );
    }

    #[test]
    fn a_singular_finish_reason_becomes_a_one_element_list() {
        // OTEL's equivalent is an array and OpenInference's is one string. The
        // array resolver rejects a non-array and a rejection drops the row, so
        // this guards that the singular source is handled separately.
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("llm.finish_reason", "stop"),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1)
            .expect("a singular finish reason must not drop the row")
            .expect("row");
        assert_eq!(row.finish_reasons, Some(vec!["stop".to_string()]));
    }

    #[test]
    fn an_array_finish_reason_still_wins_over_the_singular_one() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("llm.finish_reason", "stop"),
            KeyValue {
                key_strindex: 0,
                key: "gen_ai.response.finish_reasons".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::ArrayValue(ArrayValue {
                        values: vec![AnyValue {
                            value: Some(Value::StringValue("length".to_string())),
                        }],
                    })),
                }),
            },
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.finish_reasons, Some(vec!["length".to_string()]));
    }

    #[test]
    fn llm_provider_fills_provider_name_when_no_system_is_stated() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("llm.provider", "azure"),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.provider_name.as_deref(), Some("azure"));
    }

    #[test]
    fn llm_system_outranks_llm_provider_for_provider_name() {
        // `llm.system` names the AI product, which is what this column means
        // elsewhere; `llm.provider` names the host it runs on.
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("llm.provider", "azure"),
            kv_str("llm.system", "openai"),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        assert_eq!(row.provider_name.as_deref(), Some("openai"));
    }

    #[test]
    fn embedding_and_reranker_model_names_fill_request_model() {
        for key in ["embedding.model_name", "reranker.model_name"] {
            let span = span_with(vec![kv_str("openinference.span.kind", "LLM"), kv_str(key, "model-x")]);
            let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
            assert_eq!(
                row.request_model.as_deref(),
                Some("model-x"),
                "{key} must fill the column"
            );
        }
    }

    #[test]
    fn completions_prompts_and_choices_project_into_the_message_columns() {
        // A completions API has no roles, so these rebuild to [{"text": ...}].
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("llm.prompts.0.prompt.text", "def fib(n):"),
            kv_str("llm.choices.0.completion.text", " return n"),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        let input: serde_json::Value =
            serde_json::from_str(row.input_messages.as_deref().expect("present")).expect("json");
        assert_eq!(input[0]["text"], "def fib(n):");
        let output: serde_json::Value =
            serde_json::from_str(row.output_messages.as_deref().expect("present")).expect("json");
        assert_eq!(output[0]["text"], " return n");
    }

    #[test]
    fn chat_messages_outrank_completions_prompts() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("llm.prompts.0.prompt.text", "completions"),
            kv_str("llm.input_messages.0.message.role", "user"),
            kv_str("llm.input_messages.0.message.content", "chat"),
        ]);
        let row = project_row(&span, None, &test_tenant("t"), None, 1).expect("ok").expect("row");
        let input: serde_json::Value =
            serde_json::from_str(row.input_messages.as_deref().expect("present")).expect("json");
        assert_eq!(input[0]["content"], "chat");
    }

    #[test]
    fn openinference_session_id_projects_as_the_conversation_id() {
        let span = span_with(vec![
            kv_str("openinference.span.kind", "LLM"),
            kv_str("session.id", "conv-42"),
        ]);

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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
        let row = project_row(&span, None, &test_tenant("t"), None, 1)
            .expect("projection ok")
            .expect("row");
        assert_eq!(row.time_to_first_chunk_ms, Some(1300));
    }

    #[test]
    fn claude_code_negative_input_tokens_drops_row() {
        // Claude Code's flat token keys inherit the same strict non-negative
        // contract as every other adapter: a negative count drops the row (D6).
        let span = claude_span("claude_code.llm_request", vec![kv_str("input_tokens", "-5")]);
        assert!(project_row(&span, None, &test_tenant("t"), None, 1).is_err());
    }

    #[test]
    fn claude_code_negative_ttft_ms_drops_row() {
        // Exercises the unit-aware resolver's negative guard on the `_ms` path.
        let span = claude_span("claude_code.llm_request", vec![kv_str("ttft_ms", "-1")]);
        assert!(project_row(&span, None, &test_tenant("t"), None, 1).is_err());
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
        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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

        let row = project_row(&span, None, &test_tenant("t"), None, 1)
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
