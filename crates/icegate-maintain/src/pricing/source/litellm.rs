//! `LiteLLM`'s community-maintained rate card.
//!
//! This is the only non-first-party source, which is why it is scoped to the
//! providers that publish no machine-readable feed at all, and why the delta and
//! cardinality guards are blocking rather than advisory.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;

use super::{PriceSource, RateObservation, TOKENS_PER_MILLION, TierBoundary, ValidFromSource, expand_tiers};
use crate::error::{MaintainError, Result};
use crate::pricing::config::GLOBAL_REGION;

/// Providers `LiteLLM` owns unless config overrides it.
///
/// Deliberately excludes `bedrock`, `vertex_ai`, and `azure`: those publish
/// first-party, region-accurate feeds, and letting two sources emit the same key
/// would make the table flap between them on alternate crawls.
pub const DEFAULT_OWNED_PROVIDERS: &[&str] = &[
    "openai",
    "anthropic",
    "cohere",
    "mistral_ai",
    "deepseek",
    "groq",
    "x_ai",
    "perplexity",
];

/// Upstream documentation placeholder that is not a model.
const SAMPLE_SPEC_KEY: &str = "sample_spec";

/// Matches a per-token cost field carrying a context-tier suffix, e.g.
/// `input_cost_per_token_above_272k_tokens`. Anchored at both ends: `LiteLLM` also
/// ships pricing-mode variants that share the `above_<N>k_tokens` fragment but
/// add a further suffix -- `input_cost_per_token_above_272k_tokens_priority` --
/// and an unanchored match would misread those as an extra tier.
static TIER_FIELD: LazyLock<Regex> = LazyLock::new(|| {
    // `expect`/`unwrap` are denied workspace-wide; this pattern is a hardcoded
    // constant, so a compile failure here is an invariant violation, not a
    // runtime condition, and the `unreachable!` arm documents exactly that.
    match Regex::new(r"^(input|output)_cost_per_token_above_(\d+)k_tokens$") {
        Ok(pattern) => pattern,
        Err(e) => unreachable!("hardcoded tier-field pattern must compile: {e}"),
    }
});

#[derive(Debug, Deserialize)]
struct Entry {
    #[serde(default)]
    litellm_provider: Option<String>,
    #[serde(default)]
    input_cost_per_token: Option<f64>,
    #[serde(default)]
    output_cost_per_token: Option<f64>,
    #[serde(default)]
    cache_read_input_token_cost: Option<f64>,
    #[serde(default)]
    cache_creation_input_token_cost: Option<f64>,
    /// Every field not named above, scanned for `TIER_FIELD` matches. `LiteLLM`
    /// ships dozens of unrelated fields per entry (`supports_vision`,
    /// `max_output_tokens`, ...); catching them here rather than declaring each
    /// keeps the struct focused on what this source actually consumes.
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

/// Collect every `TIER_FIELD` match on an entry's extra fields, merging the
/// input and output side of the same boundary into one [`TierBoundary`].
///
/// `LiteLLM` never tiers the cache columns, so those stay `None` and
/// [`expand_tiers`] carries the base row's cache rates forward — the correct
/// reading of a payload that revises only the token directions.
fn scan_tier_boundaries(extra: &serde_json::Map<String, serde_json::Value>) -> Vec<TierBoundary> {
    let mut by_boundary: BTreeMap<i64, (Option<f64>, Option<f64>)> = BTreeMap::new();
    for (key, value) in extra {
        let Some(caps) = TIER_FIELD.captures(key) else {
            continue;
        };
        // A tier field with a non-numeric value is not a rate this source can
        // use; skip just that field rather than failing the whole entry.
        let Some(rate) = value.as_f64() else {
            continue;
        };
        // The regex already constrains group 2 to `\d+`, so this only fails on
        // a boundary too large for `i64`, which no real context window is.
        let Ok(thousands) = caps[2].parse::<i64>() else {
            continue;
        };
        let slot = by_boundary.entry(thousands * 1000).or_insert((None, None));
        // The regex captures only "input" or "output"; anything else means the
        // pattern was edited without updating this match, so leave the slot
        // untouched rather than aborting the worker thread on it.
        match &caps[1] {
            "input" => slot.0 = Some(rate * TOKENS_PER_MILLION),
            "output" => slot.1 = Some(rate * TOKENS_PER_MILLION),
            _ => {}
        }
    }
    by_boundary
        .into_iter()
        .map(
            |(boundary_tokens, (input_usd_per_1m, output_usd_per_1m))| TierBoundary {
                boundary_tokens,
                input_usd_per_1m,
                output_usd_per_1m,
                cache_read_usd_per_1m: None,
                cache_write_usd_per_1m: None,
            },
        )
        .collect()
}

/// Map `LiteLLM`'s provider label to the `OTel` `gen_ai.provider.name` value that
/// `operations.provider_name` stores.
fn normalize_provider(raw: &str) -> String {
    match raw {
        "vertex_ai" => "gcp.vertex_ai".to_string(),
        "bedrock" => "aws.bedrock".to_string(),
        "mistral" => "mistral_ai".to_string(),
        "xai" => "x_ai".to_string(),
        other => other.to_string(),
    }
}

/// Strip `LiteLLM`'s provider prefix so the model matches what telemetry emits.
fn strip_provider_prefix(key: &str) -> &str {
    key.split_once('/').map_or(key, |(_, rest)| rest)
}

/// Decode `LiteLLM`'s `model_prices_and_context_window.json`.
///
/// `owned` filters by **normalised** provider name; entries for other providers
/// are dropped so this source can never collide with a first-party one.
///
/// # Errors
///
/// Returns an error if the body does not decode as a JSON object of entries.
pub fn parse_litellm(body: &str, owned: &[String], now: DateTime<Utc>) -> Result<Vec<RateObservation>> {
    let raw: BTreeMap<String, serde_json::Value> =
        serde_json::from_str(body).map_err(|e| MaintainError::Storage(format!("litellm: decode failed: {e}")))?;

    let mut observations = Vec::new();
    for (key, value) in raw {
        if key == SAMPLE_SPEC_KEY {
            continue;
        }
        // Entries vary widely upstream; one that does not fit the rate shape is
        // skipped rather than failing the whole card.
        let Ok(entry) = serde_json::from_value::<Entry>(value) else {
            continue;
        };
        let Some(raw_provider) = entry.litellm_provider.as_deref() else {
            continue;
        };
        let provider = normalize_provider(raw_provider);
        if !owned.iter().any(|p| p == &provider) {
            continue;
        }
        if entry.input_cost_per_token.is_none() && entry.output_cost_per_token.is_none() {
            continue;
        }

        let base = RateObservation {
            provider: provider.clone(),
            model: strip_provider_prefix(&key).to_string(),
            canonical_id: None,
            service_tier: "standard".to_string(),
            region: GLOBAL_REGION.to_string(),
            min_input_tokens: 0,
            max_input_tokens: None,
            valid_from: now,
            valid_from_source: ValidFromSource::Observed,
            input_usd_per_1m: entry.input_cost_per_token.map(|v| v * TOKENS_PER_MILLION),
            output_usd_per_1m: entry.output_cost_per_token.map(|v| v * TOKENS_PER_MILLION),
            cache_read_usd_per_1m: entry.cache_read_input_token_cost.map(|v| v * TOKENS_PER_MILLION),
            cache_write_usd_per_1m: entry.cache_creation_input_token_cost.map(|v| v * TOKENS_PER_MILLION),
            reasoning_usd_per_1m: None,
            request_usd: None,
            image_input_usd_per_unit: None,
            image_output_usd_per_unit: None,
            audio_input_usd_per_second: None,
            audio_output_usd_per_second: None,
            currency: "USD".to_string(),
            source: "litellm".to_string(),
            source_url: None,
        };

        let boundaries = scan_tier_boundaries(&entry.extra);
        observations.extend(expand_tiers(&base, &boundaries));
    }

    Ok(observations)
}

/// Fetches the `LiteLLM` community rate card.
pub struct LiteLlmSource {
    /// Endpoint to fetch, normally the raw `model_prices_and_context_window.json`
    /// URL on `BerriAI/litellm`'s default branch.
    url: String,
    /// Normalised provider names this source is authoritative for.
    owned: Vec<String>,
    /// Hard cap on response size, enforced by [`super::fetch_capped_body`].
    max_response_bytes: usize,
}

impl LiteLlmSource {
    /// Build a source fetching from `url`, owning `owned` providers.
    ///
    /// Pass an empty `owned` to use [`DEFAULT_OWNED_PROVIDERS`].
    #[must_use]
    pub fn new(url: String, owned: Vec<String>, max_response_bytes: usize) -> Self {
        let owned = if owned.is_empty() {
            DEFAULT_OWNED_PROVIDERS.iter().map(|s| (*s).to_string()).collect()
        } else {
            owned
        };
        Self {
            url,
            owned,
            max_response_bytes,
        }
    }
}

#[async_trait]
impl PriceSource for LiteLlmSource {
    fn name(&self) -> &'static str {
        "litellm"
    }

    fn owned_providers(&self) -> Vec<String> {
        self.owned.clone()
    }

    async fn fetch_rates(&self, client: &Client, now: DateTime<Utc>) -> Result<Vec<RateObservation>> {
        let body = super::fetch_capped_body(client, &self.url, self.max_response_bytes).await?;
        let mut rates = parse_litellm(&body, &self.owned, now)?;
        for rate in &mut rates {
            rate.source_url = Some(self.url.clone());
        }
        Ok(rates)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::{DEFAULT_OWNED_PROVIDERS, parse_litellm};

    const FIXTURE: &str = include_str!("../../../tests/fixtures/litellm_model_prices.json");

    fn owned() -> Vec<String> {
        DEFAULT_OWNED_PROVIDERS.iter().map(|s| (*s).to_string()).collect()
    }

    fn now() -> chrono::DateTime<chrono::Utc> {
        DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp")
    }

    #[test]
    fn converts_per_token_floats_to_usd_per_million() {
        // `claude-4-opus-20250514` carries no context tier, so this exercises the
        // plain float-to-per-million conversion in isolation from tier expansion.
        let rates = parse_litellm(FIXTURE, &owned(), now()).expect("fixture parses");
        let opus = rates
            .iter()
            .find(|r| r.model == "claude-4-opus-20250514")
            .expect("opus present");
        assert_eq!(opus.provider, "anthropic");
        assert!((opus.input_usd_per_1m.unwrap() - 15.0).abs() < 1e-9);
        assert!((opus.output_usd_per_1m.unwrap() - 75.0).abs() < 1e-9);
        assert!((opus.cache_read_usd_per_1m.unwrap() - 1.5).abs() < 1e-9);
        assert!((opus.cache_write_usd_per_1m.unwrap() - 18.75).abs() < 1e-9);
        assert_eq!(opus.max_input_tokens, None, "untiered model stays unbounded");
    }

    #[test]
    fn drops_providers_it_does_not_own() {
        // LiteLLM publishes Bedrock rates too. Ownership is structural: dropping
        // them here is what makes source collisions impossible.
        let rates = parse_litellm(FIXTURE, &owned(), now()).expect("fixture parses");
        assert!(!rates.iter().any(|r| r.provider.contains("bedrock")));
        assert!(!rates.iter().any(|r| r.model.starts_with("bedrock/")));
    }

    #[test]
    fn expands_a_single_context_tier_into_two_rows() {
        // `_above_200k_tokens` fields are how LiteLLM expresses a context tier;
        // they become a second row keyed on min_input_tokens, chained so the
        // base row's upper bound matches the tier row's lower bound exactly.
        let rates = parse_litellm(FIXTURE, &owned(), now()).expect("fixture parses");
        let tiers: Vec<i64> = rates
            .iter()
            .filter(|r| r.model == "claude-4-sonnet-20250514")
            .map(|r| r.min_input_tokens)
            .collect();
        assert_eq!(tiers.len(), 2, "exactly base + one tier row");
        assert!(tiers.contains(&0), "base tier present");
        assert!(tiers.contains(&200_000), "above-200k tier present");

        let base = rates
            .iter()
            .find(|r| r.model == "claude-4-sonnet-20250514" && r.min_input_tokens == 0)
            .expect("base tier");
        assert_eq!(base.max_input_tokens, Some(200_000), "base tier is bounded by the next");

        let tier = rates
            .iter()
            .find(|r| r.model == "claude-4-sonnet-20250514" && r.min_input_tokens == 200_000)
            .expect("tier row");
        assert_eq!(tier.max_input_tokens, None, "top tier is unbounded");
        assert!((tier.input_usd_per_1m.unwrap() - 6.0).abs() < 1e-9);
        assert!((tier.output_usd_per_1m.unwrap() - 22.5).abs() < 1e-9);
    }

    #[test]
    fn ignores_priority_and_flex_suffixed_fields_when_detecting_tiers() {
        // `gpt-5.5` ships `input_cost_per_token_above_272k_tokens` (a real
        // context tier) alongside `input_cost_per_token_priority` and
        // `input_cost_per_token_flex` (pricing modes, not tiers). The tier
        // regex must be anchored so it detects only the former.
        let rates = parse_litellm(FIXTURE, &owned(), now()).expect("fixture parses");
        let gpt_rows: Vec<_> = rates.iter().filter(|r| r.model == "gpt-5.5").collect();
        assert_eq!(
            gpt_rows.len(),
            2,
            "exactly base + one 272k tier row, no rows from priority/flex"
        );

        let tier = gpt_rows.iter().find(|r| r.min_input_tokens == 272_000).expect("272k tier row");
        // 45.0 is the true `_above_272k_tokens` output rate; the `_priority`
        // field on the same model is 60.0. Asserting 45.0 catches a regex that
        // accidentally matched the `_priority` suffix too.
        assert!((tier.output_usd_per_1m.unwrap() - 45.0).abs() < 1e-9);
    }

    #[test]
    fn chains_multiple_tier_boundaries_into_contiguous_non_overlapping_rows() {
        // No live model has two boundaries today, but a future upstream change
        // could introduce one silently; this proves the chaining logic handles
        // N boundaries, not just one, without leaving a gap or an overlap.
        let synthetic = r#"{
            "synthetic-three-tier-model": {
                "input_cost_per_token": 0.000001,
                "output_cost_per_token": 0.000002,
                "input_cost_per_token_above_100k_tokens": 0.000003,
                "output_cost_per_token_above_100k_tokens": 0.000004,
                "input_cost_per_token_above_300k_tokens": 0.000005,
                "output_cost_per_token_above_300k_tokens": 0.000006,
                "litellm_provider": "openai"
            }
        }"#;
        let rates = parse_litellm(synthetic, &owned(), now()).expect("synthetic payload parses");
        assert_eq!(rates.len(), 3, "base + two tiers");

        let mut rows = rates;
        rows.sort_by_key(|r| r.min_input_tokens);

        assert_eq!(rows[0].min_input_tokens, 0);
        assert_eq!(rows[0].max_input_tokens, Some(100_000));
        assert_eq!(rows[0].input_usd_per_1m, Some(1.0));
        assert_eq!(rows[0].output_usd_per_1m, Some(2.0));

        assert_eq!(rows[1].min_input_tokens, 100_000);
        assert_eq!(rows[1].max_input_tokens, Some(300_000));
        assert_eq!(rows[1].input_usd_per_1m, Some(3.0));
        assert_eq!(rows[1].output_usd_per_1m, Some(4.0));

        assert_eq!(rows[2].min_input_tokens, 300_000);
        assert_eq!(rows[2].max_input_tokens, None, "top tier is unbounded");
        assert_eq!(rows[2].input_usd_per_1m, Some(5.0));
        assert_eq!(rows[2].output_usd_per_1m, Some(6.0));
    }

    #[test]
    fn tier_row_inherits_the_base_rate_for_a_direction_it_does_not_override() {
        // A model can override input at a boundary without also overriding
        // output. The tier row must carry the base output rate forward rather
        // than leaving it `None` -- a NULL rate here would later be dropped by
        // the unpriced-row guard, silently losing the model.
        let synthetic = r#"{
            "synthetic-partial-tier-model": {
                "input_cost_per_token": 0.000001,
                "output_cost_per_token": 0.000002,
                "input_cost_per_token_above_128k_tokens": 0.000003,
                "litellm_provider": "openai"
            }
        }"#;
        let rates = parse_litellm(synthetic, &owned(), now()).expect("synthetic payload parses");
        let tier = rates.iter().find(|r| r.min_input_tokens == 128_000).expect("tier row present");
        assert_eq!(tier.input_usd_per_1m, Some(3.0), "input is overridden by the tier");
        assert_eq!(tier.output_usd_per_1m, Some(2.0), "output falls back to the base rate");
    }

    #[test]
    fn skips_the_sample_spec_entry() {
        // Upstream ships a documentation placeholder in the same object.
        let rates = parse_litellm(FIXTURE, &owned(), now()).expect("fixture parses");
        assert!(!rates.iter().any(|r| r.model == "sample_spec"));
    }

    #[test]
    fn rejects_a_malformed_payload() {
        assert!(parse_litellm("not json", &owned(), now()).is_err());
    }
}
