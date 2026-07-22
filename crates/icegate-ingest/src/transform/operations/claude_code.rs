//! Claude Code semantic-convention adapter.

use super::convention::OperationConvention;
use super::projection::{AttributeView, OperationField};
use crate::transform::attributes::extract_string_value;

/// Claude Code (`service.name = claude-code`) semantic-convention adapter.
///
/// Claude Code spans are named `claude_code.<span.type>` and only the
/// `llm_request` span carries `gen_ai.*` markers; `interaction`, `tool`,
/// `tool.execution`, and `tool.blocked_on_user` carry none. Qualification is
/// therefore by span-name prefix (`claude_code.`), not by a marker attribute.
///
/// Sources Claude Code's flat, vendor-specific keys: token counts
/// (`input_tokens`/`output_tokens`/`cache_creation_tokens`/`cache_read_tokens`),
/// `tool_name`, the millisecond-native `ttft_ms`, the flat agent/workflow
/// spellings (`agent_id`/`workflow.name`/`subagent_type`), tool-call arguments
/// (`full_command`/`file_path`), and the interaction's `user_prompt` input
/// message. Provider, model,
/// response id, finish reasons, tool-call id, conversation id, and user id are
/// already sourced by the OTEL/`OpenInference` adapters from the `gen_ai.*` /
/// `session.id` / `user.id` keys these spans also carry. Registered last, so it
/// only fills fields the earlier adapters did not.
pub(crate) struct ClaudeCode;

impl OperationConvention for ClaudeCode {
    fn marker_keys(&self) -> &'static [&'static str] {
        // Claude Code qualifies by span name (see `name_prefixes`), not by an
        // attribute marker, so it contributes nothing to the marker union.
        &[]
    }

    fn name_prefixes(&self) -> &'static [&'static str] {
        &["claude_code."]
    }

    fn field_keys(&self, field: OperationField) -> &'static [&'static str] {
        match field {
            OperationField::InputTokens => &["input_tokens"],
            OperationField::OutputTokens => &["output_tokens"],
            OperationField::CacheCreationInputTokens => &["cache_creation_tokens"],
            OperationField::CacheReadInputTokens => &["cache_read_tokens"],
            OperationField::ToolName => &["tool_name"],
            // Millisecond-native; the `_ms` suffix tells the driver's unit-aware
            // resolver not to scale it (unlike OTEL's seconds-based key).
            OperationField::TimeToFirstChunkMs => &["ttft_ms"],
            // Claude Code's flat agent/workflow spellings (vs OTEL's
            // `gen_ai.agent.*` / `gen_ai.workflow.name`). `subagent_type` names the
            // dispatched subagent on `invoke_subagent` tool spans. All three are
            // gated/context-conditional, so best-effort like every other field.
            OperationField::AgentId => &["agent_id"],
            OperationField::WorkflowName => &["workflow.name"],
            OperationField::AgentName => &["subagent_type"],
            _ => &[],
        }
    }

    fn object_field_keys(&self, field: OperationField) -> &'static [&'static str] {
        // The tool's input arguments are flat span attributes — `full_command`
        // (Bash) or `file_path` (Read/Edit) — projected as one JSON object keyed
        // by attribute name, e.g. `{"full_command": "…"}`.
        match field {
            OperationField::ToolCallArguments => &["full_command", "file_path"],
            _ => &[],
        }
    }

    fn event_field_names(&self, field: OperationField) -> &'static [&'static str] {
        // The whole `tool.output` span event (gated by OTEL_LOG_TOOL_CONTENT) is
        // the tool's result: its echoed input (bash_command / file_path) and its
        // output payload (output / content) together form the output object. The
        // tool's *input arguments* come from the span attributes (see field_keys).
        match field {
            OperationField::ToolCallResult => &["tool.output"],
            _ => &[],
        }
    }

    fn message_field_keys(&self, field: OperationField) -> &'static [(&'static str, &'static str)] {
        // The interaction span's `user_prompt` is a bare prompt string; wrap it as
        // a single `user` message so conversation views (which expect a
        // `[{"role","content"}]` array) render it.
        match field {
            OperationField::InputMessages => &[("user_prompt", "user")],
            _ => &[],
        }
    }

    fn classify_operation(&self, span_name: &str, attrs: &AttributeView) -> Option<String> {
        // Claude Code names every span `claude_code.<span.type>`; classify by
        // exact name, or by the `tool` prefix for the tool family.
        let name = span_name.strip_prefix("claude_code.")?;
        let operation = match name {
            "llm_request" => "chat",
            "interaction" => "invoke_agent",
            // `tool`, `tool.execution`, `tool.blocked_on_user`. Only the parent
            // `tool` span carries `tool_name`; the `Agent` dispatcher is a nested
            // agent invocation, kept distinct from a plain tool run.
            _ if name == "tool" || name.starts_with("tool.") => {
                match extract_string_value(attrs.get("tool_name")).as_deref() {
                    Some("Agent") => "invoke_subagent",
                    _ => "execute_tool",
                }
            }
            _ => "other",
        };
        Some(operation.to_string())
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

    /// Classify by span name with the given attributes.
    fn classify(span_name: &str, attrs: &[KeyValue]) -> Option<String> {
        let view = AttributeView::new(attrs);
        ClaudeCode.classify_operation(span_name, &view)
    }

    #[test]
    fn qualifies_by_claude_code_name_prefix() {
        assert_eq!(ClaudeCode.name_prefixes(), &["claude_code."]);
    }

    #[test]
    fn declares_no_attribute_markers() {
        // Claude Code qualifies by span name, not by an attribute marker, so it
        // must not widen the attribute-marker union filter.
        assert_eq!(ClaudeCode.marker_keys(), &[] as &[&str]);
    }

    #[test]
    fn token_fields_source_flat_claude_code_keys() {
        assert_eq!(ClaudeCode.field_keys(OperationField::InputTokens), &["input_tokens"]);
        assert_eq!(ClaudeCode.field_keys(OperationField::OutputTokens), &["output_tokens"]);
        assert_eq!(
            ClaudeCode.field_keys(OperationField::CacheCreationInputTokens),
            &["cache_creation_tokens"]
        );
        assert_eq!(
            ClaudeCode.field_keys(OperationField::CacheReadInputTokens),
            &["cache_read_tokens"]
        );
    }

    #[test]
    fn tool_name_sources_flat_tool_name_key() {
        assert_eq!(ClaudeCode.field_keys(OperationField::ToolName), &["tool_name"]);
    }

    #[test]
    fn time_to_first_chunk_sources_ttft_ms() {
        assert_eq!(ClaudeCode.field_keys(OperationField::TimeToFirstChunkMs), &["ttft_ms"]);
    }

    #[test]
    fn agent_and_workflow_fields_source_flat_claude_code_keys() {
        // Claude Code uses flat spellings (agent_id / workflow.name / subagent_type),
        // not the OTEL gen_ai.agent.* / gen_ai.workflow.name keys.
        assert_eq!(ClaudeCode.field_keys(OperationField::AgentId), &["agent_id"]);
        assert_eq!(ClaudeCode.field_keys(OperationField::WorkflowName), &["workflow.name"]);
        assert_eq!(ClaudeCode.field_keys(OperationField::AgentName), &["subagent_type"]);
    }

    #[test]
    fn tool_call_arguments_object_from_full_command_and_file_path() {
        // Tool input arguments are assembled into a JSON object from these flat
        // span attributes (keyed by name), not resolved as a scalar field_keys value.
        assert_eq!(
            ClaudeCode.object_field_keys(OperationField::ToolCallArguments),
            &["full_command", "file_path"]
        );
        assert_eq!(ClaudeCode.field_keys(OperationField::ToolCallArguments), &[] as &[&str]);
    }

    #[test]
    fn input_messages_wraps_user_prompt_as_user_message() {
        // user_prompt is wrapped into a `[{"role":"user","content":…}]` message
        // array, not resolved as a scalar field_keys value.
        assert_eq!(
            ClaudeCode.message_field_keys(OperationField::InputMessages),
            &[("user_prompt", "user")]
        );
        assert_eq!(ClaudeCode.field_keys(OperationField::InputMessages), &[] as &[&str]);
    }

    #[test]
    fn tool_call_result_sources_the_whole_tool_output_event() {
        // The entire tool.output event is the result; arguments are NOT
        // event-sourced (they come from span attributes).
        assert_eq!(
            ClaudeCode.event_field_names(OperationField::ToolCallResult),
            &["tool.output"]
        );
        assert_eq!(
            ClaudeCode.event_field_names(OperationField::ToolCallArguments),
            &[] as &[&str]
        );
    }

    #[test]
    fn non_tool_output_fields_declare_no_event_sources() {
        assert_eq!(
            ClaudeCode.event_field_names(OperationField::InputMessages),
            &[] as &[&str]
        );
    }

    #[test]
    fn unsourced_field_returns_empty() {
        assert_eq!(ClaudeCode.field_keys(OperationField::Temperature), &[] as &[&str]);
    }

    #[test]
    fn llm_request_classifies_as_chat() {
        assert_eq!(classify("claude_code.llm_request", &[]), Some("chat".to_string()));
    }

    #[test]
    fn interaction_classifies_as_invoke_agent() {
        assert_eq!(
            classify("claude_code.interaction", &[]),
            Some("invoke_agent".to_string())
        );
    }

    #[test]
    fn tool_with_agent_tool_name_classifies_as_invoke_subagent() {
        // The subagent dispatcher is a nested agent invocation, kept distinct
        // from the top-level interaction and from plain tool runs.
        assert_eq!(
            classify("claude_code.tool", &[kv_str("tool_name", "Agent")]),
            Some("invoke_subagent".to_string())
        );
    }

    #[test]
    fn tool_with_other_tool_name_classifies_as_execute_tool() {
        assert_eq!(
            classify("claude_code.tool", &[kv_str("tool_name", "Bash")]),
            Some("execute_tool".to_string())
        );
    }

    #[test]
    fn tool_subspans_classify_as_execute_tool() {
        // tool.execution / tool.blocked_on_user carry no tool_name and fall to
        // execute_tool via the `claude_code.tool` prefix.
        assert_eq!(
            classify("claude_code.tool.execution", &[]),
            Some("execute_tool".to_string())
        );
        assert_eq!(
            classify("claude_code.tool.blocked_on_user", &[]),
            Some("execute_tool".to_string())
        );
    }

    #[test]
    fn unknown_claude_code_span_classifies_as_other() {
        assert_eq!(classify("claude_code.future_kind", &[]), Some("other".to_string()));
    }

    #[test]
    fn non_claude_code_name_is_not_classified() {
        // A span whose name is not claude_code.* is left for other adapters.
        assert_eq!(classify("op", &[kv_str("tool_name", "Agent")]), None);
    }
}
