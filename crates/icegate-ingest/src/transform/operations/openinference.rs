//! `OpenInference` semantic-convention adapter.

use super::convention::OperationConvention;
use super::projection::{AttributeView, OperationField};
use crate::transform::attributes::extract_string_value;

/// `OpenInference` (Arize) semantic-convention adapter. Sources `llm.*` token
/// counts, `llm.model_name`, `session.id`, and classifies via the
/// `openinference.span.kind` normalization map (spec section 4). Second in the
/// registry, so OTEL wins on any key `OpenInference` also offers.
pub(crate) struct OpenInference;

impl OperationConvention for OpenInference {
    fn marker_keys(&self) -> &'static [&'static str] {
        &["openinference.span.kind", "llm.model_name", "llm.system"]
    }

    fn field_keys(&self, field: OperationField) -> &'static [&'static str] {
        match field {
            OperationField::RequestModel => &["llm.model_name"],
            OperationField::InputTokens => &["llm.token_count.prompt"],
            OperationField::OutputTokens => &["llm.token_count.completion"],
            OperationField::TotalTokens => &["llm.token_count.total"],
            OperationField::ReasoningTokens => &["llm.token_count.completion_details.reasoning"],
            OperationField::CacheCreationInputTokens => &["llm.token_count.prompt_details.cache_write"],
            OperationField::CacheReadInputTokens => &["llm.token_count.prompt_details.cache_read"],
            OperationField::ConversationId => &["session.id"],
            _ => &[],
        }
    }

    fn classify_operation(&self, attrs: &AttributeView) -> Option<String> {
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
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.to_string())),
            }),
        }
    }

    fn classify(kind: &str) -> Option<String> {
        let attrs = vec![kv_str("openinference.span.kind", kind)];
        let view = AttributeView::new(&attrs);
        OpenInference.classify_operation(&view)
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
        let keys = OpenInference.field_keys(OperationField::RequestModel);
        assert_eq!(keys, &["llm.model_name"]);
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
        assert_eq!(OpenInference.classify_operation(&view), None);
    }
}
