//! OTEL `GenAI` semantic-convention adapter.

use super::convention::OperationConvention;
use super::projection::{AttributeView, FieldType, OperationField};
use crate::transform::attributes::extract_string_value;

/// OTEL `GenAI` semantic-convention adapter. Sources the canonical `gen_ai.*`
/// keys (with deprecated `gen_ai.system` / `OpenInference` `llm.system` kept as
/// provider fallbacks), and classifies via verbatim `gen_ai.operation.name`.
/// First in the registry, so OTEL wins on every shared key.
pub(crate) struct OtelGenAi;

impl OperationConvention for OtelGenAi {
    fn marker_keys(&self) -> &'static [&'static str] {
        &["gen_ai.operation.name", "gen_ai.provider.name", "gen_ai.system"]
    }

    #[allow(clippy::too_many_lines)]
    fn field_keys(&self, field: OperationField) -> &'static [(&'static str, FieldType)] {
        match field {
            OperationField::ProviderName => &[
                ("gen_ai.provider.name", FieldType::Str),
                ("gen_ai.system", FieldType::Str),
                ("llm.system", FieldType::Str),
            ],
            OperationField::RequestModel => &[
                ("gen_ai.request.model", FieldType::Str),
                ("llm.model_name", FieldType::Str),
            ],
            OperationField::ResponseModel => &[("gen_ai.response.model", FieldType::Str)],
            OperationField::ResponseId => &[("gen_ai.response.id", FieldType::Str)],
            OperationField::Temperature => &[("gen_ai.request.temperature", FieldType::F64)],
            OperationField::TopP => &[("gen_ai.request.top_p", FieldType::F64)],
            OperationField::TopK => &[("gen_ai.request.top_k", FieldType::I64)],
            OperationField::MaxTokens => &[("gen_ai.request.max_tokens", FieldType::I64)],
            OperationField::FrequencyPenalty => &[("gen_ai.request.frequency_penalty", FieldType::F64)],
            OperationField::PresencePenalty => &[("gen_ai.request.presence_penalty", FieldType::F64)],
            OperationField::Seed => &[("gen_ai.request.seed", FieldType::I64)],
            OperationField::Stream => &[("gen_ai.request.stream", FieldType::Bool)],
            OperationField::ChoiceCount => &[
                ("gen_ai.request.choice_count", FieldType::I64),
                ("gen_ai.request.choice.count", FieldType::I64),
            ],
            OperationField::OutputType => &[("gen_ai.output.type", FieldType::Str)],
            OperationField::ReasoningEffort => &[("gen_ai.request.reasoning_effort", FieldType::Str)],
            OperationField::StopSequences => &[("gen_ai.request.stop_sequences", FieldType::StrList)],
            OperationField::TimeToFirstChunkMs => &[("gen_ai.response.time_to_first_chunk", FieldType::F64)],
            OperationField::FinishReasons => &[("gen_ai.response.finish_reasons", FieldType::StrList)],
            OperationField::InputTokens => &[("gen_ai.usage.input_tokens", FieldType::I64)],
            OperationField::OutputTokens => &[("gen_ai.usage.output_tokens", FieldType::I64)],
            OperationField::TotalTokens => &[("gen_ai.usage.total_tokens", FieldType::I64)],
            OperationField::ReasoningTokens => &[("gen_ai.usage.reasoning.output_tokens", FieldType::I64)],
            OperationField::CacheCreationInputTokens => &[
                ("gen_ai.usage.cache_creation.input_tokens", FieldType::I64),
                ("cache_creation_input_tokens", FieldType::I64),
            ],
            OperationField::CacheReadInputTokens => &[("gen_ai.usage.cache_read.input_tokens", FieldType::I64)],
            OperationField::ConversationId => &[("gen_ai.conversation.id", FieldType::Str)],
            OperationField::UserId => &[
                ("user.id", FieldType::Str),
                ("gen_ai.user", FieldType::Str),
                ("llm.user", FieldType::Str),
            ],
            OperationField::ToolName => &[("gen_ai.tool.name", FieldType::Str)],
            OperationField::ToolCallId => &[("gen_ai.tool.call.id", FieldType::Str)],
            OperationField::ToolType => &[("gen_ai.tool.type", FieldType::Str)],
            OperationField::ToolDescription => &[("gen_ai.tool.description", FieldType::Str)],
            OperationField::DataSourceId => &[("gen_ai.data_source.id", FieldType::Str)],
            OperationField::EmbeddingDimensions => &[("gen_ai.embeddings.dimension.count", FieldType::I32Count)],
            OperationField::EncodingFormats => &[("gen_ai.request.encoding_formats", FieldType::StrList)],
            OperationField::ServerAddress => &[("server.address", FieldType::Str)],
            OperationField::ServerPort => &[("server.port", FieldType::I32Count)],
            OperationField::ErrorType => &[("error.type", FieldType::Str)],
            OperationField::AgentId => &[("gen_ai.agent.id", FieldType::Str)],
            OperationField::AgentName => &[("gen_ai.agent.name", FieldType::Str)],
            OperationField::AgentVersion => &[("gen_ai.agent.version", FieldType::Str)],
            OperationField::AgentDescription => &[("gen_ai.agent.description", FieldType::Str)],
            OperationField::WorkflowName => &[("gen_ai.workflow.name", FieldType::Str)],
            OperationField::InputMessages => &[("gen_ai.input.messages", FieldType::Json)],
            OperationField::OutputMessages => &[("gen_ai.output.messages", FieldType::Json)],
            OperationField::SystemInstructions => &[("gen_ai.system_instructions", FieldType::Json)],
            OperationField::ToolDefinitions => &[("gen_ai.tool.definitions", FieldType::Json)],
            OperationField::ToolCallArguments => &[("gen_ai.tool.call.arguments", FieldType::Json)],
            OperationField::ToolCallResult => &[("gen_ai.tool.call.result", FieldType::Json)],
        }
    }

    fn classify_operation(&self, attrs: &AttributeView) -> Option<String> {
        // Verbatim, lowercased: preserves text_completion / generate_content /
        // invoke_workflow / plan without special-casing (spec section 4).
        extract_string_value(attrs.get("gen_ai.operation.name")).map(|name| name.to_lowercase())
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

    #[test]
    fn markers_cover_otel_genai_keys() {
        let markers = OtelGenAi.marker_keys();
        assert!(markers.contains(&"gen_ai.operation.name"));
        assert!(markers.contains(&"gen_ai.provider.name"));
        assert!(markers.contains(&"gen_ai.system"));
    }

    #[test]
    fn provider_name_field_keys_are_ordered_otel_first() {
        // provider precedence within OTEL: gen_ai.provider.name, then deprecated
        // gen_ai.system, then llm.system (spec section 3 provider_model row).
        let keys: Vec<&str> = OtelGenAi
            .field_keys(OperationField::ProviderName)
            .iter()
            .map(|(key, _)| *key)
            .collect();
        assert_eq!(keys, vec!["gen_ai.provider.name", "gen_ai.system", "llm.system"]);
    }

    #[test]
    fn temperature_field_key_is_typed_f64() {
        let keys = OtelGenAi.field_keys(OperationField::Temperature);
        assert_eq!(keys, &[("gen_ai.request.temperature", FieldType::F64)]);
    }

    #[test]
    fn input_tokens_field_key_is_typed_i64() {
        let keys = OtelGenAi.field_keys(OperationField::InputTokens);
        assert_eq!(keys.first(), Some(&("gen_ai.usage.input_tokens", FieldType::I64)));
    }

    #[test]
    fn stop_sequences_field_key_is_typed_str_list() {
        let keys = OtelGenAi.field_keys(OperationField::StopSequences);
        assert_eq!(keys, &[("gen_ai.request.stop_sequences", FieldType::StrList)]);
    }

    #[test]
    fn input_messages_field_key_is_typed_json() {
        let keys = OtelGenAi.field_keys(OperationField::InputMessages);
        assert_eq!(keys, &[("gen_ai.input.messages", FieldType::Json)]);
    }

    #[test]
    fn classify_operation_returns_lowercased_verbatim() {
        let attrs = vec![kv_str("gen_ai.operation.name", "Text_Completion")];
        let view = AttributeView::new(&attrs);
        assert_eq!(OtelGenAi.classify_operation(&view), Some("text_completion".to_string()));
    }

    #[test]
    fn classify_operation_is_none_without_operation_name() {
        let attrs = vec![kv_str("gen_ai.system", "openai")];
        let view = AttributeView::new(&attrs);
        assert_eq!(OtelGenAi.classify_operation(&view), None);
    }
}
