//! Cross-provider model identity.
//!
//! `canonical_id` namespaces a model by its **author**, not the platform serving
//! it, so the same artifact sold through Bedrock, Vertex, `OpenRouter`, and the
//! vendor directly shares one id and rolls up in a single `GROUP BY`.
//!
//! Heuristics are acceptable here and nowhere else in this crate: price is keyed
//! on `(provider, model)`, never on `canonical_id`, so a wrong id degrades
//! grouping accuracy but can never mis-price a span. That containment is what
//! makes a rules ladder with a `None` fallback the right design here.

use super::source::RateObservation;

/// Providers whose model ids are already `<author>/<model>`.
const PASSTHROUGH_PROVIDERS: &[&str] = &["openrouter"];

/// Providers that serve models under their own author namespace.
const SELF_AUTHORED_PROVIDERS: &[&str] = &[
    "anthropic",
    "openai",
    "cohere",
    "mistral_ai",
    "deepseek",
    "groq",
    "x_ai",
    "perplexity",
];

/// Curated `(provider, model) -> canonical_id` overrides.
///
/// Ladder rule 2. Entries here win over the structural rules below; add a row
/// whenever a structural rule would produce the wrong artifact.
///
/// Still empty after Task 16 (Bedrock): every id `bedrock.rs` maps
/// (`anthropic.claude-3-{sonnet,haiku}-<date>-v1:0`, `meta.llama3-8b-instruct-v1:0`)
/// already reduces to the correct artifact through the `aws.bedrock` structural
/// rule below, so a curated row would be redundant. Kept as a `const` (rather
/// than deleted) so the ladder's rung 2 exists in code before it has data, and
/// so dropping in rows later needs no plumbing change. Known gap: the
/// structural rule collapses `anthropic.claude-v2` (2.0) and
/// `anthropic.claude-v2:1` (2.1) to the same id (`strip_bedrock_version` has no
/// date to stop the digit-trim at), which is a real grouping-accuracy defect --
/// left unfixed here because the correct direct-vendor id for those two legacy
/// models could not be confirmed, and a guessed fix would violate this
/// ladder's own "never guess" rule.
const CURATED: &[(&str, &str, &str)] = &[];

/// Strip an AWS Bedrock invocation suffix (`-v2:0`, `-v1:0`).
///
/// The digit-trim only ever removes a trailing version number, never the
/// model's date suffix: it stops at the first non-digit character, which is
/// always the `v` marker (a letter), so a date immediately to the left of `v`
/// is untouched regardless of how many version digits follow it.
fn strip_bedrock_version(model: &str) -> &str {
    model
        .rsplit_once(':')
        .map_or(model, |(head, _)| head)
        .trim_end_matches(|c: char| c.is_ascii_digit())
        .trim_end_matches("-v")
}

/// Derive the canonical id for one `(provider, model)` pair.
///
/// Returns `None` when no rule fires -- never a guess.
#[must_use]
pub fn derive_canonical_id(provider: &str, model: &str) -> Option<String> {
    // Rule 1: the source already supplies an author-namespaced id.
    if PASSTHROUGH_PROVIDERS.contains(&provider) && model.contains('/') {
        return Some(model.to_string());
    }

    // Rule 2: curated overrides.
    if let Some((_, _, canonical)) = CURATED.iter().find(|(p, m, _)| *p == provider && *m == model) {
        return Some((*canonical).to_string());
    }

    // Rule 3: structural rules per provider family.
    if SELF_AUTHORED_PROVIDERS.contains(&provider) {
        return Some(format!("{provider}/{model}"));
    }
    if provider == "aws.bedrock" {
        // `anthropic.claude-3-5-sonnet-20241022-v2:0` -> `anthropic/claude-3-5-sonnet-20241022`
        let base = strip_bedrock_version(model);
        return base.split_once('.').map(|(author, rest)| format!("{author}/{rest}"));
    }
    if provider == "gcp.vertex_ai" {
        // `publishers/anthropic/models/claude-...` -> `anthropic/claude-...`
        let parts: Vec<&str> = model.split('/').collect();
        if let ["publishers", author, "models", rest @ ..] = parts.as_slice() {
            return Some(format!("{author}/{}", rest.join("/")));
        }
    }

    // Rule 4: no rule fires.
    None
}

/// Fill in `canonical_id` on every observation that does not already carry one.
///
/// Never overwrites an id a source already supplied (e.g. `OpenRouter`, which
/// derives it from its own ids at ingestion).
pub fn apply_canonical_ids(rates: &mut [RateObservation]) {
    for rate in &mut *rates {
        if rate.canonical_id.is_none() {
            rate.canonical_id = derive_canonical_id(&rate.provider, &rate.model);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use chrono::DateTime;

    use super::{apply_canonical_ids, derive_canonical_id};
    use crate::pricing::source::{RateObservation, ValidFromSource};

    fn now() -> chrono::DateTime<chrono::Utc> {
        DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp")
    }

    /// A minimal observation for `apply_canonical_ids` assertions to vary.
    fn sample(provider: &str, model: &str, canonical_id: Option<&str>) -> RateObservation {
        RateObservation {
            provider: provider.to_string(),
            model: model.to_string(),
            canonical_id: canonical_id.map(str::to_string),
            service_tier: "standard".to_string(),
            region: "global".to_string(),
            min_input_tokens: 0,
            max_input_tokens: None,
            valid_from: now(),
            valid_from_source: ValidFromSource::Observed,
            input_usd_per_1m: Some(1.0),
            output_usd_per_1m: Some(2.0),
            cache_read_usd_per_1m: None,
            cache_write_usd_per_1m: None,
            reasoning_usd_per_1m: None,
            request_usd: None,
            image_input_usd_per_unit: None,
            image_output_usd_per_unit: None,
            audio_input_usd_per_second: None,
            audio_output_usd_per_second: None,
            currency: "USD".to_string(),
            source: "test".to_string(),
            source_url: None,
        }
    }

    #[test]
    fn direct_vendor_models_namespace_by_author() {
        assert_eq!(
            derive_canonical_id("anthropic", "claude-3-5-sonnet-20241022").as_deref(),
            Some("anthropic/claude-3-5-sonnet-20241022")
        );
        assert_eq!(
            derive_canonical_id("openai", "gpt-4o-2024-08-06").as_deref(),
            Some("openai/gpt-4o-2024-08-06")
        );
    }

    #[test]
    fn bedrock_ids_reduce_to_the_same_artifact_as_the_direct_vendor() {
        // This is the whole point of canonical_id: a cross-provider rollup must
        // recognise these as one model.
        assert_eq!(
            derive_canonical_id("aws.bedrock", "anthropic.claude-3-5-sonnet-20241022-v2:0").as_deref(),
            Some("anthropic/claude-3-5-sonnet-20241022")
        );
    }

    #[test]
    fn vertex_publisher_paths_unwrap() {
        assert_eq!(
            derive_canonical_id(
                "gcp.vertex_ai",
                "publishers/anthropic/models/claude-3-5-sonnet-20241022"
            )
            .as_deref(),
            Some("anthropic/claude-3-5-sonnet-20241022")
        );
    }

    #[test]
    fn unknown_shapes_yield_none_rather_than_a_guess() {
        // A wrong canonical_id costs grouping accuracy; a guessed one costs trust.
        assert_eq!(derive_canonical_id("some_new_provider", "weird::model::name"), None);
    }

    #[test]
    fn openrouter_ids_pass_through_unchanged() {
        assert_eq!(
            derive_canonical_id("openrouter", "anthropic/claude-3.5-sonnet").as_deref(),
            Some("anthropic/claude-3.5-sonnet")
        );
    }

    #[test]
    fn bedrock_and_direct_anthropic_agree_on_the_same_canonical_id() {
        // The entire point of the column: two rows from different providers for
        // the same underlying model must key identically under `GROUP BY
        // canonical_id`, even though their `(provider, model)` price keys never
        // match.
        let bedrock = derive_canonical_id("aws.bedrock", "anthropic.claude-3-5-sonnet-20241022-v2:0");
        let direct = derive_canonical_id("anthropic", "claude-3-5-sonnet-20241022");
        assert_eq!(bedrock, direct);
    }

    #[test]
    fn apply_canonical_ids_fills_in_missing_ids() {
        let mut rates = vec![sample("anthropic", "claude-3-5-sonnet-20241022", None)];
        apply_canonical_ids(&mut rates);
        assert_eq!(
            rates[0].canonical_id.as_deref(),
            Some("anthropic/claude-3-5-sonnet-20241022")
        );
    }

    #[test]
    fn apply_canonical_ids_never_overwrites_a_source_supplied_id() {
        // OpenRouter sets canonical_id itself from its own model id; a structural
        // rule must not clobber it even if one would also fire here.
        let mut rates = vec![sample("openrouter", "anthropic/claude-3.5-sonnet", Some("already-set"))];
        apply_canonical_ids(&mut rates);
        assert_eq!(rates[0].canonical_id.as_deref(), Some("already-set"));
    }

    #[test]
    fn apply_canonical_ids_leaves_unresolvable_rows_none() {
        let mut rates = vec![sample("some_new_provider", "weird::model::name", None)];
        apply_canonical_ids(&mut rates);
        assert_eq!(rates[0].canonical_id, None);
    }
}
