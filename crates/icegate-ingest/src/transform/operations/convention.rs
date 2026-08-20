//! Convention registry for the `operations` transform.

use std::sync::OnceLock;

use super::claude_code::ClaudeCode;
use super::openinference::OpenInference;
use super::otel::OtelGenAi;
use super::projection::{AttributeView, OperationField};
use super::traceloop::Traceloop;
use crate::error::{IngestError, Result};

/// One client-SDK / semantic-convention family (OTEL `GenAI`, `OpenInference`,
/// Traceloop, future SDKs). Adding an SDK means implementing this trait — almost
/// entirely static tables — and appending one entry to [`CONVENTIONS`]; the core
/// driver, schema, WAL, and shift wiring stay untouched (spec D9 / section 6).
pub(crate) trait OperationConvention: Send + Sync {
    /// Marker attribute keys. A span qualifies as an operation iff ANY
    /// registered convention reports a present marker; the section 4 filter is
    /// the union of every adapter's markers.
    fn marker_keys(&self) -> &'static [&'static str];

    /// Span-name prefixes that qualify a span for this convention even when no
    /// marker attribute is present. Name-based SDKs (e.g. Claude Code, whose
    /// non-LLM spans carry no `gen_ai.*` marker) opt in here; attribute-marker
    /// conventions inherit the empty default. Kept separate from
    /// [`Self::marker_keys`] so name-based qualification does not widen the
    /// attribute-marker union filter.
    fn name_prefixes(&self) -> &'static [&'static str] {
        &[]
    }

    /// Ordered candidate attribute keys this convention offers for `field`
    /// (most-specific first). Empty slice when this convention does not source
    /// the field.
    fn field_keys(&self, field: OperationField) -> &'static [&'static str];

    /// Attribute keys this convention assembles into a JSON object for a content
    /// `field`, keyed by attribute name — used when the field is a set of flat
    /// attributes rather than a single value (e.g. a tool call's flat input
    /// arguments). Resolved after the scalar [`Self::field_keys`] and before
    /// event objects ([`Self::event_field_names`]). Default: none.
    fn object_field_keys(&self, _field: OperationField) -> &'static [&'static str] {
        &[]
    }

    /// Span *event* names whose full attribute set this convention projects into
    /// `field` as a single JSON object keyed by attribute name — used when a
    /// field's value lives in a span event rather than an attribute (e.g. a tool
    /// result). Attribute sources ([`Self::field_keys`]) are resolved first and
    /// win. Default: no event-sourced fields.
    fn event_field_names(&self, _field: OperationField) -> &'static [&'static str] {
        &[]
    }

    /// Attribute-key prefixes under which this convention flattens a JSON array
    /// into indexed keys (`<prefix>.<index>.<singular>.<field>`), to be rebuilt
    /// into the array `field` expects. Used by SDKs that do not emit a
    /// structured array attribute at all — `OpenInference` spreads a prompt
    /// across one attribute per message field. Resolved after the scalar
    /// [`Self::field_keys`] and before the object, message, and event modes,
    /// since a convention that offers both a whole array and its flattened
    /// pieces means the same thing by each and the whole array is cheaper.
    /// Default: none.
    fn indexed_field_prefixes(&self, _field: OperationField) -> &'static [&'static str] {
        &[]
    }

    /// `(attribute_key, role)` sources this convention wraps into a single-message
    /// JSON array `[{"role": role, "content": <value>}]` for a message content
    /// `field` — used when an SDK emits a bare prompt/response string rather than
    /// a structured messages array (the shape conversation UIs parse). Resolved
    /// after the scalar, object, and event modes. Default: none.
    fn message_field_keys(&self, _field: OperationField) -> &'static [(&'static str, &'static str)] {
        &[]
    }

    /// Classifies a span into a canonical `operation_name` from its `span_name`
    /// and attributes, or `None` when this convention cannot decide (the next
    /// adapter then tries). `span_name` is the OTLP span name, letting name-based
    /// conventions classify by exact name or prefix.
    fn classify_operation(&self, span_name: &str, attrs: &AttributeView) -> Option<String>;
}

/// Precedence-ordered convention registry: earlier wins on shared keys. This
/// slice is the whole extension surface — append an adapter to add an SDK.
pub(crate) static CONVENTIONS: &[&dyn OperationConvention] = &[&OtelGenAi, &OpenInference, &Traceloop, &ClaudeCode];

/// Flattens every convention's `field_keys(field)` into a single
/// precedence-ordered vector, preserving registry order. Pulled out as a free
/// function (taking the convention slice) so it is unit-testable with stub
/// adapters independently of [`CONVENTIONS`].
fn flatten_field_keys(conventions: &[&dyn OperationConvention], field: OperationField) -> Vec<&'static str> {
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
type PrecedenceIndex = Vec<Vec<&'static str>>;

/// Cached, lazily-initialized per-field precedence index over [`CONVENTIONS`].
static FIELD_PRECEDENCE: OnceLock<PrecedenceIndex> = OnceLock::new();

/// Returns the precomputed global precedence slice for `field`: every
/// convention's candidate keys for that field flattened in registry order, with
/// the first present key in a span winning. Computed once via `OnceLock`, so the
/// hot path walks a flat slice with no per-field trait dispatch.
///
/// # Errors
///
/// Returns [`IngestError::Validation`] if `field` is absent from [`ALL_FIELDS`]
/// (or the index has no entry for it). This is an internal registry invariant,
/// guarded by the `field_precedence_covers_every_field` test; surfacing it as an
/// error rather than a panic keeps a future enum-drift bug from aborting the
/// ingest hot path — the affected row is dropped instead.
pub(crate) fn field_precedence(field: OperationField) -> Result<&'static [&'static str]> {
    let index = FIELD_PRECEDENCE
        .get_or_init(|| ALL_FIELDS.iter().map(|field| flatten_field_keys(CONVENTIONS, *field)).collect());
    let position = ALL_FIELDS.iter().position(|candidate| *candidate == field).ok_or_else(|| {
        IngestError::Validation(format!("operations field {field:?} is missing from supported fields"))
    })?;
    index
        .get(position)
        .map(Vec::as_slice)
        .ok_or_else(|| IngestError::Validation(format!("operations precedence index has no entry for {field:?}")))
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
    use crate::transform::operations::projection::{AttributeView, OperationField};

    /// First stub convention: sources `ProviderName` from key "a.provider" and
    /// declares marker "a.marker".
    struct StubA;

    impl OperationConvention for StubA {
        fn marker_keys(&self) -> &'static [&'static str] {
            &["a.marker", "shared.marker"]
        }

        fn field_keys(&self, field: OperationField) -> &'static [&'static str] {
            match field {
                OperationField::ProviderName => &["a.provider"],
                _ => &[],
            }
        }

        fn classify_operation(&self, _span_name: &str, _attrs: &AttributeView) -> Option<String> {
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

        fn field_keys(&self, field: OperationField) -> &'static [&'static str] {
            match field {
                OperationField::ProviderName => &["b.provider"],
                _ => &[],
            }
        }

        fn classify_operation(&self, _span_name: &str, _attrs: &AttributeView) -> Option<String> {
            None
        }
    }

    #[test]
    fn flatten_precedence_preserves_registry_order() {
        // StubA precedes StubB, so its key for a shared field comes first.
        let stubs: &[&dyn OperationConvention] = &[&StubA, &StubB];
        let keys = flatten_field_keys(stubs, OperationField::ProviderName);
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
        let first = field_precedence(OperationField::ProviderName).expect("precedence available");
        let second = field_precedence(OperationField::ProviderName).expect("precedence available");
        assert!(std::ptr::eq(first.as_ptr(), second.as_ptr()));
    }

    #[test]
    fn field_precedence_covers_every_field() {
        // Guards ALL_FIELDS against enum drift: every variant resolves to a
        // precedence slice instead of erroring on the position lookup inside
        // field_precedence.
        for field in ALL_FIELDS {
            assert!(
                field_precedence(*field).is_ok(),
                "{field:?} must resolve to a precedence slice"
            );
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
    fn input_tokens_precedence_is_otel_then_oi_then_traceloop_then_claude_code() {
        let keys: Vec<&str> = field_precedence(OperationField::InputTokens)
            .expect("precedence available")
            .to_vec();
        assert_eq!(
            keys,
            vec![
                "gen_ai.usage.input_tokens",
                "llm.token_count.prompt",
                "gen_ai.usage.prompt_tokens",
                "input_tokens"
            ]
        );
    }

    #[test]
    fn request_model_precedence_puts_otel_before_openinference() {
        // Both OTEL (gen_ai.request.model, llm.model_name) and OpenInference
        // (llm.model_name) source RequestModel; OTEL's keys come first.
        let keys: Vec<&str> = field_precedence(OperationField::RequestModel)
            .expect("precedence available")
            .to_vec();
        assert_eq!(keys.first(), Some(&"gen_ai.request.model"));
        assert!(keys.contains(&"llm.model_name"));
    }
}
