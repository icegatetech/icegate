//! Traceloop semantic-convention adapter.

use super::convention::OperationConvention;
use super::projection::{AttributeView, FieldType, OperationField};
use crate::transform::attributes::extract_string_value;

/// Traceloop (`OpenLLMetry`) semantic-convention adapter. Sources Traceloop's
/// legacy/flat token spellings, `gen_ai.is_streaming`, and
/// `traceloop.workflow.name`, and classifies via the `traceloop.span.kind` map
/// (spec section 4). Last in the registry, so it only fills keys OTEL and
/// `OpenInference` did not already provide.
pub(crate) struct Traceloop;

impl OperationConvention for Traceloop {
    fn marker_keys(&self) -> &'static [&'static str] {
        &["traceloop.span.kind"]
    }

    fn field_keys(&self, field: OperationField) -> &'static [(&'static str, FieldType)] {
        match field {
            OperationField::InputTokens => &[("gen_ai.usage.prompt_tokens", FieldType::I64)],
            OperationField::OutputTokens => &[("gen_ai.usage.completion_tokens", FieldType::I64)],
            OperationField::ReasoningTokens => &[("gen_ai.usage.reasoning_tokens", FieldType::I64)],
            OperationField::CacheReadInputTokens => &[("gen_ai.usage.cache_read_input_tokens", FieldType::I64)],
            OperationField::Stream => &[("gen_ai.is_streaming", FieldType::Bool)],
            OperationField::WorkflowName => &[("traceloop.workflow.name", FieldType::Str)],
            _ => &[],
        }
    }

    fn classify_operation(&self, attrs: &AttributeView) -> Option<String> {
        let kind = extract_string_value(attrs.get("traceloop.span.kind"))?;
        let normalized = match kind.as_str() {
            "workflow" | "task" => "chain",
            "agent" => "invoke_agent",
            "tool" => "execute_tool",
            // "unknown" and any other value fall through to the catch-all.
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
        let attrs = vec![kv_str("traceloop.span.kind", kind)];
        let view = AttributeView::new(&attrs);
        Traceloop.classify_operation(&view)
    }

    #[test]
    fn marker_is_traceloop_span_kind() {
        assert_eq!(Traceloop.marker_keys(), &["traceloop.span.kind"]);
    }

    #[test]
    fn input_tokens_fallback_to_prompt_tokens() {
        let keys = Traceloop.field_keys(OperationField::InputTokens);
        assert_eq!(keys, &[("gen_ai.usage.prompt_tokens", FieldType::I64)]);
    }

    #[test]
    fn output_tokens_fallback_to_completion_tokens() {
        let keys = Traceloop.field_keys(OperationField::OutputTokens);
        assert_eq!(keys, &[("gen_ai.usage.completion_tokens", FieldType::I64)]);
    }

    #[test]
    fn reasoning_tokens_fallback_to_flat_key() {
        let keys = Traceloop.field_keys(OperationField::ReasoningTokens);
        assert_eq!(keys, &[("gen_ai.usage.reasoning_tokens", FieldType::I64)]);
    }

    #[test]
    fn cache_read_fallback_to_flat_key() {
        let keys = Traceloop.field_keys(OperationField::CacheReadInputTokens);
        assert_eq!(keys, &[("gen_ai.usage.cache_read_input_tokens", FieldType::I64)]);
    }

    #[test]
    fn stream_fallback_to_is_streaming() {
        let keys = Traceloop.field_keys(OperationField::Stream);
        assert_eq!(keys, &[("gen_ai.is_streaming", FieldType::Bool)]);
    }

    #[test]
    fn workflow_name_sources_traceloop_key() {
        let keys = Traceloop.field_keys(OperationField::WorkflowName);
        assert_eq!(keys, &[("traceloop.workflow.name", FieldType::Str)]);
    }

    #[test]
    fn classify_normalizes_traceloop_kinds() {
        assert_eq!(classify("workflow"), Some("chain".to_string()));
        assert_eq!(classify("task"), Some("chain".to_string()));
        assert_eq!(classify("agent"), Some("invoke_agent".to_string()));
        assert_eq!(classify("tool"), Some("execute_tool".to_string()));
        assert_eq!(classify("unknown"), Some("other".to_string()));
        assert_eq!(classify("anything_else"), Some("other".to_string()));
    }

    #[test]
    fn classify_is_none_without_span_kind() {
        let attrs = vec![kv_str("traceloop.workflow.name", "my_flow")];
        let view = AttributeView::new(&attrs);
        assert_eq!(Traceloop.classify_operation(&view), None);
    }
}
