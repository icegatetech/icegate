//! OTEL `GenAI` semantic-convention adapter.

use super::convention::OperationConvention;
use super::projection::{AttributeView, OperationField};
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

    fn field_keys(&self, field: OperationField) -> &'static [&'static str] {
        match field {
            OperationField::ProviderName => &["gen_ai.provider.name", "gen_ai.system", "llm.system"],
            OperationField::RequestModel => &["gen_ai.request.model", "llm.model_name"],
            OperationField::ResponseModel => &["gen_ai.response.model"],
            OperationField::ResponseId => &["gen_ai.response.id"],
            OperationField::Temperature => &["gen_ai.request.temperature"],
            OperationField::TopP => &["gen_ai.request.top_p"],
            OperationField::TopK => &["gen_ai.request.top_k"],
            OperationField::MaxTokens => &["gen_ai.request.max_tokens"],
            OperationField::FrequencyPenalty => &["gen_ai.request.frequency_penalty"],
            OperationField::PresencePenalty => &["gen_ai.request.presence_penalty"],
            OperationField::Seed => &["gen_ai.request.seed"],
            OperationField::Stream => &["gen_ai.request.stream"],
            OperationField::ChoiceCount => &["gen_ai.request.choice_count", "gen_ai.request.choice.count"],
            OperationField::OutputType => &["gen_ai.output.type"],
            OperationField::ReasoningEffort => &["gen_ai.request.reasoning_effort"],
            OperationField::StopSequences => &["gen_ai.request.stop_sequences"],
            OperationField::TimeToFirstChunkMs => &["gen_ai.response.time_to_first_chunk"],
            OperationField::FinishReasons => &["gen_ai.response.finish_reasons"],
            OperationField::InputTokens => &["gen_ai.usage.input_tokens"],
            OperationField::OutputTokens => &["gen_ai.usage.output_tokens"],
            OperationField::TotalTokens => &["gen_ai.usage.total_tokens"],
            OperationField::ReasoningTokens => &["gen_ai.usage.reasoning.output_tokens"],
            OperationField::CacheCreationInputTokens => &[
                "gen_ai.usage.cache_creation.input_tokens",
                "cache_creation_input_tokens",
            ],
            OperationField::CacheReadInputTokens => &["gen_ai.usage.cache_read.input_tokens"],
            OperationField::ConversationId => &["gen_ai.conversation.id"],
            OperationField::UserId => &["user.id", "gen_ai.user", "llm.user"],
            OperationField::ToolName => &["gen_ai.tool.name"],
            OperationField::ToolCallId => &["gen_ai.tool.call.id"],
            OperationField::ToolType => &["gen_ai.tool.type"],
            OperationField::ToolDescription => &["gen_ai.tool.description"],
            OperationField::DataSourceId => &["gen_ai.data_source.id"],
            OperationField::EmbeddingDimensions => &["gen_ai.embeddings.dimension.count"],
            OperationField::EncodingFormats => &["gen_ai.request.encoding_formats"],
            OperationField::ServerAddress => &["server.address"],
            OperationField::ServerPort => &["server.port"],
            OperationField::ErrorType => &["error.type"],
            OperationField::AgentId => &["gen_ai.agent.id"],
            OperationField::AgentName => &["gen_ai.agent.name"],
            OperationField::AgentVersion => &["gen_ai.agent.version"],
            OperationField::AgentDescription => &["gen_ai.agent.description"],
            OperationField::WorkflowName => &["gen_ai.workflow.name"],
            OperationField::InputMessages => &["gen_ai.input.messages"],
            OperationField::OutputMessages => &["gen_ai.output.messages"],
            OperationField::SystemInstructions => &["gen_ai.system_instructions"],
            OperationField::ToolDefinitions => &["gen_ai.tool.definitions"],
            OperationField::ToolCallArguments => &["gen_ai.tool.call.arguments"],
            OperationField::ToolCallResult => &["gen_ai.tool.call.result"],
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
        let keys = OtelGenAi.field_keys(OperationField::ProviderName);
        assert_eq!(keys, &["gen_ai.provider.name", "gen_ai.system", "llm.system"]);
    }

    #[test]
    fn temperature_sourced_from_request_temperature() {
        let keys = OtelGenAi.field_keys(OperationField::Temperature);
        assert_eq!(keys, &["gen_ai.request.temperature"]);
    }

    #[test]
    fn input_tokens_sourced_from_usage_input_tokens() {
        let keys = OtelGenAi.field_keys(OperationField::InputTokens);
        assert_eq!(keys.first(), Some(&"gen_ai.usage.input_tokens"));
    }

    #[test]
    fn stop_sequences_sourced_from_request_stop_sequences() {
        let keys = OtelGenAi.field_keys(OperationField::StopSequences);
        assert_eq!(keys, &["gen_ai.request.stop_sequences"]);
    }

    #[test]
    fn input_messages_sourced_from_input_messages() {
        let keys = OtelGenAi.field_keys(OperationField::InputMessages);
        assert_eq!(keys, &["gen_ai.input.messages"]);
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
