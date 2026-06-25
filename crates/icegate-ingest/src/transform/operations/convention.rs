//! Convention registry for the `operations` transform.

use std::sync::OnceLock;

use super::openinference::OpenInference;
use super::otel::OtelGenAi;
use super::projection::{AttributeView, FieldType, OperationField};
use super::traceloop::Traceloop;

/// One client-SDK / semantic-convention family (OTEL `GenAI`, `OpenInference`,
/// Traceloop, future SDKs). Adding an SDK means implementing this trait — almost
/// entirely static tables — and appending one entry to [`CONVENTIONS`]; the core
/// driver, schema, WAL, and shift wiring stay untouched (spec D9 / section 6).
pub(crate) trait OperationConvention: Send + Sync {
    /// Marker attribute keys. A span qualifies as an operation iff ANY
    /// registered convention reports a present marker; the section 4 filter is
    /// the union of every adapter's markers.
    fn marker_keys(&self) -> &'static [&'static str];

    /// Ordered candidate attribute keys this convention offers for `field`
    /// (most-specific first). Empty slice when this convention does not source
    /// the field.
    fn field_keys(&self, field: OperationField) -> &'static [(&'static str, FieldType)];

    /// Classifies this convention's span-kind into a canonical `operation_name`,
    /// or `None` when this convention cannot decide (the next adapter then tries).
    fn classify_operation(&self, attrs: &AttributeView) -> Option<String>;
}

/// Precedence-ordered convention registry: earlier wins on shared keys. This
/// slice is the whole extension surface — append an adapter to add an SDK.
pub(crate) static CONVENTIONS: &[&dyn OperationConvention] = &[&OtelGenAi, &OpenInference, &Traceloop];

/// Flattens every convention's `field_keys(field)` into a single
/// precedence-ordered vector, preserving registry order. Pulled out as a free
/// function (taking the convention slice) so it is unit-testable with stub
/// adapters independently of [`CONVENTIONS`].
fn flatten_field_keys(
    conventions: &[&dyn OperationConvention],
    field: OperationField,
) -> Vec<(&'static str, FieldType)> {
    let mut flattened = Vec::new();
    for convention in conventions {
        flattened.extend_from_slice(convention.field_keys(field));
    }
    flattened
}

/// Computes the deduplicated union of every convention's marker keys (the
/// section 4 materialization filter). Order is registry order with first
/// occurrence kept; duplicates from shared keys are dropped. Free function over
/// the convention slice for stub-based unit testing.
#[cfg(test)]
fn union_markers(conventions: &[&dyn OperationConvention]) -> Vec<&'static str> {
    let mut seen = Vec::new();
    for convention in conventions {
        for marker in convention.marker_keys() {
            if !seen.contains(marker) {
                seen.push(*marker);
            }
        }
    }
    seen
}

/// The complete ordered list of [`OperationField`] variants, used to build the
/// per-field precedence index once. Kept in sync with the enum by the
/// `field_precedence_covers_every_field` registry test.
const ALL_FIELDS: &[OperationField] = &[
    OperationField::ProviderName,
    OperationField::RequestModel,
    OperationField::ResponseModel,
    OperationField::ResponseId,
    OperationField::Temperature,
    OperationField::TopP,
    OperationField::TopK,
    OperationField::MaxTokens,
    OperationField::FrequencyPenalty,
    OperationField::PresencePenalty,
    OperationField::Seed,
    OperationField::Stream,
    OperationField::ChoiceCount,
    OperationField::OutputType,
    OperationField::ReasoningEffort,
    OperationField::StopSequences,
    OperationField::TimeToFirstChunkMs,
    OperationField::FinishReasons,
    OperationField::InputTokens,
    OperationField::OutputTokens,
    OperationField::TotalTokens,
    OperationField::ReasoningTokens,
    OperationField::CacheCreationInputTokens,
    OperationField::CacheReadInputTokens,
    OperationField::ConversationId,
    OperationField::UserId,
    OperationField::ToolName,
    OperationField::ToolCallId,
    OperationField::ToolType,
    OperationField::ToolDescription,
    OperationField::DataSourceId,
    OperationField::EmbeddingDimensions,
    OperationField::EncodingFormats,
    OperationField::ServerAddress,
    OperationField::ServerPort,
    OperationField::ErrorType,
    OperationField::AgentId,
    OperationField::AgentName,
    OperationField::AgentVersion,
    OperationField::AgentDescription,
    OperationField::WorkflowName,
    OperationField::InputMessages,
    OperationField::OutputMessages,
    OperationField::SystemInstructions,
    OperationField::ToolDefinitions,
    OperationField::ToolCallArguments,
    OperationField::ToolCallResult,
];

/// Lazily-built global precedence index: one flattened precedence slice per
/// `OperationField`, indexed by the field's position in [`ALL_FIELDS`].
type PrecedenceIndex = Vec<Vec<(&'static str, FieldType)>>;

/// Cached, lazily-initialized per-field precedence index over [`CONVENTIONS`].
static FIELD_PRECEDENCE: OnceLock<PrecedenceIndex> = OnceLock::new();

/// Returns the precomputed global precedence slice for `field`: every
/// convention's candidate keys for that field flattened in registry order, with
/// the first present key in a span winning. Computed once via `OnceLock`, so the
/// hot path walks a flat slice with no per-field trait dispatch.
// Invariant: every OperationField variant is listed in ALL_FIELDS (guarded by the
// field_precedence_covers_every_field test), so the position lookup never returns None.
#[allow(clippy::expect_used)]
pub(crate) fn field_precedence(field: OperationField) -> &'static [(&'static str, FieldType)] {
    let index = FIELD_PRECEDENCE
        .get_or_init(|| ALL_FIELDS.iter().map(|field| flatten_field_keys(CONVENTIONS, *field)).collect());
    let position = ALL_FIELDS
        .iter()
        .position(|candidate| *candidate == field)
        .expect("every OperationField variant is listed in ALL_FIELDS");
    &index[position]
}

/// Returns the deduplicated union of every registered convention's marker keys —
/// the section 4 materialization filter. Computed over [`CONVENTIONS`].
#[cfg(test)]
pub(crate) fn marker_filter() -> Vec<&'static str> {
    union_markers(CONVENTIONS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::operations::projection::{AttributeView, FieldType, OperationField};

    /// First stub convention: sources `ProviderName` from key "a.provider" and
    /// declares marker "a.marker".
    struct StubA;

    impl OperationConvention for StubA {
        fn marker_keys(&self) -> &'static [&'static str] {
            &["a.marker", "shared.marker"]
        }

        fn field_keys(&self, field: OperationField) -> &'static [(&'static str, FieldType)] {
            match field {
                OperationField::ProviderName => &[("a.provider", FieldType::Str)],
                _ => &[],
            }
        }

        fn classify_operation(&self, _attrs: &AttributeView) -> Option<String> {
            None
        }
    }

    /// Second stub convention: also sources `ProviderName` (from "b.provider")
    /// and declares marker "b.marker" plus the shared marker.
    struct StubB;

    impl OperationConvention for StubB {
        fn marker_keys(&self) -> &'static [&'static str] {
            &["b.marker", "shared.marker"]
        }

        fn field_keys(&self, field: OperationField) -> &'static [(&'static str, FieldType)] {
            match field {
                OperationField::ProviderName => &[("b.provider", FieldType::Str)],
                _ => &[],
            }
        }

        fn classify_operation(&self, _attrs: &AttributeView) -> Option<String> {
            None
        }
    }

    #[test]
    fn flatten_precedence_preserves_registry_order() {
        // StubA precedes StubB, so its key for a shared field comes first.
        let stubs: &[&dyn OperationConvention] = &[&StubA, &StubB];
        let precedence = flatten_field_keys(stubs, OperationField::ProviderName);
        let keys: Vec<&str> = precedence.iter().map(|(key, _)| *key).collect();
        assert_eq!(keys, vec!["a.provider", "b.provider"]);
    }

    #[test]
    fn union_of_markers_deduplicates_shared_keys() {
        // The materialization filter is the union of all adapters' marker_keys()
        // (spec section 4). Shared keys appear exactly once.
        let stubs: &[&dyn OperationConvention] = &[&StubA, &StubB];
        let mut markers = union_markers(stubs);
        markers.sort_unstable();
        assert_eq!(markers, vec!["a.marker", "b.marker", "shared.marker"]);
    }

    #[test]
    fn field_precedence_is_cached_and_stable() {
        // The real CONVENTIONS-backed precedence index is computed once and is
        // referentially stable across calls (OnceLock).
        let first = field_precedence(OperationField::ProviderName);
        let second = field_precedence(OperationField::ProviderName);
        assert!(std::ptr::eq(first.as_ptr(), second.as_ptr()));
    }

    #[test]
    fn field_precedence_covers_every_field() {
        // Guards ALL_FIELDS against enum drift: every variant resolves without
        // panicking on the .position() lookup inside field_precedence.
        for field in ALL_FIELDS {
            let _ = field_precedence(*field);
        }
    }

    #[test]
    fn marker_filter_equals_union_of_all_adapter_markers() {
        // Spec section 4: the materialization filter is the union of every
        // adapter's marker_keys() — register an SDK, widen the filter for free.
        let mut markers = marker_filter();
        markers.sort_unstable();
        assert_eq!(
            markers,
            vec![
                "gen_ai.operation.name",
                "gen_ai.provider.name",
                "gen_ai.system",
                "llm.model_name",
                "llm.system",
                "openinference.span.kind",
                "traceloop.span.kind",
            ]
        );
    }

    #[test]
    fn input_tokens_precedence_is_otel_then_oi_then_traceloop() {
        let keys: Vec<&str> = field_precedence(OperationField::InputTokens)
            .iter()
            .map(|(key, _)| *key)
            .collect();
        assert_eq!(
            keys,
            vec![
                "gen_ai.usage.input_tokens",
                "llm.token_count.prompt",
                "gen_ai.usage.prompt_tokens"
            ]
        );
    }

    #[test]
    fn request_model_precedence_puts_otel_before_openinference() {
        // Both OTEL (gen_ai.request.model, llm.model_name) and OpenInference
        // (llm.model_name) source RequestModel; OTEL's keys come first.
        let keys: Vec<&str> = field_precedence(OperationField::RequestModel)
            .iter()
            .map(|(key, _)| *key)
            .collect();
        assert_eq!(keys.first(), Some(&"gen_ai.request.model"));
        assert!(keys.contains(&"llm.model_name"));
    }
}
