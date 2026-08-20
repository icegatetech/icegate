//! `OpenInference` semantic-convention adapter.

use super::convention::OperationConvention;
use super::projection::{AttributeView, OperationField};
use crate::transform::attributes::extract_string_value;

/// `OpenInference` (Arize) semantic-convention adapter. Second in the registry,
/// so OTEL wins on any key `OpenInference` also offers.
///
/// Classifies via the `openinference.span.kind` normalization map (spec section
/// 4), and sources the `llm.*` and `tool.*` families. Three of those need a
/// resolution mode the OTEL convention never does, because `OpenInference`
/// states the same facts in a different shape:
///
/// - **Sampling parameters have no attributes of their own.** An SDK puts the
///   whole request payload in `llm.invocation_parameters`, so nine typed columns
///   are reachable only by reading into that JSON — see
///   [`OperationConvention::json_blob_field_keys`].
/// - **Messages are flattened, not arrays.** `llm.input_messages`,
///   `llm.output_messages`, `llm.tools`, and the completions-API
///   `llm.prompts` / `llm.choices` arrive one attribute per field, rebuilt by
///   [`OperationConvention::indexed_field_prefixes`].
/// - **`llm.finish_reason` is one string** where OTEL's equivalent column is an
///   array — see [`OperationConvention::singular_list_field_keys`].
///
/// Attribute families the spec defines that this adapter does *not* source,
/// because `operations` has no column for them: `llm.cost.*`,
/// `llm.prompt_template.*`, `llm.function_call`, and the
/// `llm.token_count.*_details.audio` counts.
pub(crate) struct OpenInference;

impl OperationConvention for OpenInference {
    fn marker_keys(&self) -> &'static [&'static str] {
        &["openinference.span.kind", "llm.model_name", "llm.system"]
    }

    fn field_keys(&self, field: OperationField) -> &'static [&'static str] {
        match field {
            OperationField::RequestModel => &["llm.model_name", "embedding.model_name", "reranker.model_name"],
            OperationField::InputTokens => &["llm.token_count.prompt"],
            OperationField::OutputTokens => &["llm.token_count.completion"],
            OperationField::TotalTokens => &["llm.token_count.total"],
            OperationField::ReasoningTokens => &["llm.token_count.completion_details.reasoning"],
            OperationField::CacheCreationInputTokens => &["llm.token_count.prompt_details.cache_write"],
            OperationField::CacheReadInputTokens => &["llm.token_count.prompt_details.cache_read"],
            OperationField::ConversationId => &["session.id"],
            // The OTEL adapter already offers `llm.system` for this column and
            // is ahead in the registry, so it wins when both are present. That
            // ordering is deliberate: `llm.system` names the AI product
            // ("openai"), which is what `provider_name` means elsewhere, while
            // `llm.provider` names the host it runs on ("azure") and is the
            // better answer only when no product is stated.
            OperationField::ProviderName => &["llm.provider"],
            OperationField::ToolName => &["tool.name"],
            OperationField::ToolDescription => &["tool.description"],
            OperationField::ToolCallId => &["tool.id"],
            // A TOOL span states its own definition flatly, unlike an LLM span,
            // which advertises every tool it may call through the indexed
            // `llm.tools` list below. `json_schema` is the complete definition
            // and `parameters` only the argument shape, so the fuller one wins.
            OperationField::ToolDefinitions => &["tool.json_schema", "tool.parameters"],
            _ => &[],
        }
    }

    fn json_blob_field_keys(&self, field: OperationField) -> &'static [(&'static str, &'static str)] {
        // Every sampling parameter lives inside `llm.invocation_parameters`;
        // OpenInference declares no attribute for any of them individually.
        const BLOB: &str = "llm.invocation_parameters";
        match field {
            OperationField::Temperature => &[(BLOB, "temperature")],
            OperationField::TopP => &[(BLOB, "top_p")],
            OperationField::TopK => &[(BLOB, "top_k")],
            // Chat Completions spells it `max_tokens`; the Responses API and
            // the reasoning models spell it `max_completion_tokens`.
            OperationField::MaxTokens => &[(BLOB, "max_tokens"), (BLOB, "max_completion_tokens")],
            OperationField::FrequencyPenalty => &[(BLOB, "frequency_penalty")],
            OperationField::PresencePenalty => &[(BLOB, "presence_penalty")],
            OperationField::Seed => &[(BLOB, "seed")],
            OperationField::Stream => &[(BLOB, "stream")],
            OperationField::ChoiceCount => &[(BLOB, "n")],
            _ => &[],
        }
    }

    fn singular_list_field_keys(&self, field: OperationField) -> &'static [&'static str] {
        match field {
            // One string, where OTEL's equivalent column is an array.
            OperationField::FinishReasons => &["llm.finish_reason"],
            _ => &[],
        }
    }

    fn indexed_field_prefixes(&self, field: OperationField) -> &'static [&'static str] {
        match field {
            // `llm.prompts` / `llm.choices` are the completions-API counterparts
            // of the chat messages lists. They rebuild to `[{"text": ...}]`
            // rather than `[{"role": ..., "content": ...}]`, because a
            // completions call has no roles to report — the chat prefixes are
            // ordered first so a chat span is never affected.
            OperationField::InputMessages => &["llm.input_messages", "llm.prompts"],
            OperationField::OutputMessages => &["llm.output_messages", "llm.choices"],
            OperationField::ToolDefinitions => &["llm.tools"],
            _ => &[],
        }
    }

    fn classify_operation(&self, _span_name: &str, attrs: &AttributeView) -> Option<String> {
        let kind = extract_string_value(attrs.get("openinference.span.kind"))?;
        // Normalization map per spec section 4. PROMPT and any unrecognized kind
        // collapse to "other" via the catch-all.
        let normalized = match kind.as_str() {
            "LLM" => "chat",
            "EMBEDDING" => "embeddings",
            "RETRIEVER" => "retrieval",
            "TOOL" => "execute_tool",
            "AGENT" => "invoke_agent",
            "CHAIN" => "chain",
            "RERANKER" => "reranker",
            "GUARDRAIL" => "guardrail",
            "EVALUATOR" => "evaluator",
            _ => "other",
        };
        Some(normalized.to_string())
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

    use super::*;

    fn kv_str(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key_strindex: 0,
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
        }
    }

    fn classify(kind: &str) -> Option<String> {
        let attrs = vec![kv_str("openinference.span.kind", kind)];
        let view = AttributeView::new(&attrs);
        OpenInference.classify_operation("op", &view)
    }

    #[test]
    fn markers_cover_openinference_keys() {
        let markers = OpenInference.marker_keys();
        assert!(markers.contains(&"openinference.span.kind"));
        assert!(markers.contains(&"llm.model_name"));
        assert!(markers.contains(&"llm.system"));
    }

    #[test]
    fn request_model_sources_llm_model_name() {
        // Embedding and reranker spans name their model with their own key,
        // so all three fill the same column.
        let keys = OpenInference.field_keys(OperationField::RequestModel);
        assert_eq!(keys, &["llm.model_name", "embedding.model_name", "reranker.model_name"]);
    }

    #[test]
    fn input_tokens_source_oi_token_count() {
        let keys = OpenInference.field_keys(OperationField::InputTokens);
        assert_eq!(keys, &["llm.token_count.prompt"]);
    }

    #[test]
    fn reasoning_tokens_source_oi_completion_details() {
        let keys = OpenInference.field_keys(OperationField::ReasoningTokens);
        assert_eq!(keys, &["llm.token_count.completion_details.reasoning"]);
    }

    #[test]
    fn conversation_id_sources_session_id() {
        let keys = OpenInference.field_keys(OperationField::ConversationId);
        assert_eq!(keys, &["session.id"]);
    }

    #[test]
    fn message_columns_source_the_indexed_prefixes() {
        // OpenInference has no whole-array attribute for these; it flattens the
        // array across `<prefix>.<index>.message.<field>` keys.
        // Chat first, then the completions-API counterparts, so a chat span is
        // never resolved from `llm.prompts`.
        assert_eq!(
            OpenInference.indexed_field_prefixes(OperationField::InputMessages),
            &["llm.input_messages", "llm.prompts"]
        );
        assert_eq!(
            OpenInference.indexed_field_prefixes(OperationField::OutputMessages),
            &["llm.output_messages", "llm.choices"]
        );
        assert_eq!(
            OpenInference.indexed_field_prefixes(OperationField::ToolDefinitions),
            &["llm.tools"]
        );
    }

    #[test]
    fn message_columns_have_no_scalar_source() {
        // The indexed prefixes are the only source; declaring a scalar key as
        // well would claim an attribute this convention never emits.
        assert_eq!(OpenInference.field_keys(OperationField::InputMessages), &[] as &[&str]);
        assert_eq!(OpenInference.field_keys(OperationField::OutputMessages), &[] as &[&str]);
    }

    #[test]
    fn fields_without_an_indexed_source_declare_none() {
        assert_eq!(
            OpenInference.indexed_field_prefixes(OperationField::RequestModel),
            &[] as &[&str]
        );
    }

    #[test]
    fn classify_normalizes_span_kind_per_spec() {
        assert_eq!(classify("LLM"), Some("chat".to_string()));
        assert_eq!(classify("EMBEDDING"), Some("embeddings".to_string()));
        assert_eq!(classify("RETRIEVER"), Some("retrieval".to_string()));
        assert_eq!(classify("TOOL"), Some("execute_tool".to_string()));
        assert_eq!(classify("AGENT"), Some("invoke_agent".to_string()));
        assert_eq!(classify("CHAIN"), Some("chain".to_string()));
        assert_eq!(classify("PROMPT"), Some("other".to_string()));
    }

    #[test]
    fn classify_lowercases_passthrough_kinds() {
        // RERANKER / GUARDRAIL / EVALUATOR are lowercased verbatim (spec section 4).
        assert_eq!(classify("RERANKER"), Some("reranker".to_string()));
        assert_eq!(classify("GUARDRAIL"), Some("guardrail".to_string()));
        assert_eq!(classify("EVALUATOR"), Some("evaluator".to_string()));
    }

    #[test]
    fn classify_is_none_without_span_kind() {
        let attrs = vec![kv_str("llm.system", "openai")];
        let view = AttributeView::new(&attrs);
        assert_eq!(OpenInference.classify_operation("op", &view), None);
    }
}
