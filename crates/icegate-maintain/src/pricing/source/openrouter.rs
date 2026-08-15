//! `OpenRouter`: an aggregator that resells many vendors' models at its own
//! margin, and is therefore authoritative only for the `openrouter` provider.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;

use super::{PriceSource, RateObservation, TOKENS_PER_MILLION, TierBoundary, ValidFromSource, expand_tiers};
use crate::error::{MaintainError, Result};

/// Provider string `OpenRouter` rows are attributed to.
const PROVIDER: &str = "openrouter";

/// Providers this source owns unless config overrides it.
///
/// `OpenRouter` resells many vendors' models at its own margin, so its rows are
/// never aliases of the upstream vendor's and it owns only itself.
pub const DEFAULT_OWNED_PROVIDERS: &[&str] = &[PROVIDER];

/// Top-level shape of `GET /api/v1/models`. `OpenRouter` also returns
/// `total_count` and `links` alongside `data`; both are ignored here since
/// `serde` drops fields absent from the target struct.
#[derive(Debug, Deserialize)]
struct ModelsResponse {
    data: Vec<ModelEntry>,
}

/// One catalogue entry. `OpenRouter` ids are already `<author>/<model>`, so the
/// same string serves as both `RateObservation::model` and `canonical_id`
/// (ladder rule 1) with no extra derivation.
#[derive(Debug, Deserialize)]
struct ModelEntry {
    id: String,
    pricing: Pricing,
}

/// Every field is a decimal string in **USD per token**, not per million,
/// except `request`, `image`, and `image_output`, which are flat per-unit USD
/// amounts. Confirmed against a live capture of `/api/v1/models`: `prompt` and
/// `completion` are present on every model; the rest are present only where
/// the model actually supports that modality or feature. `request` was absent
/// from every model in the capture, but the field is kept optional in case a
/// future model reintroduces a flat per-call fee.
///
/// Fields the live payload carries but this struct does not map (`web_search`,
/// `audio`, `audio_output`, `input_audio_cache`, `input_cache_write_1h`) have
/// no corresponding `RateObservation` column. `serde` drops them silently
/// since they are not declared below. `overrides` is mapped, expanded into
/// per-tier rows by [`expand_tiers`].
#[derive(Debug, Deserialize)]
struct Pricing {
    prompt: String,
    completion: String,
    #[serde(default)]
    request: Option<String>,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    image_output: Option<String>,
    #[serde(default)]
    input_cache_read: Option<String>,
    #[serde(default)]
    input_cache_write: Option<String>,
    #[serde(default)]
    internal_reasoning: Option<String>,
    /// Context-tier pricing, ascending by `min_prompt_tokens` in practice but
    /// not guaranteed to be sorted by upstream; see [`resolve_tier_boundaries`].
    #[serde(default)]
    overrides: Vec<Override>,
}

/// One tier from `pricing.overrides`. Every rate field but `min_prompt_tokens`
/// is optional: `OpenRouter` emits only the fields a tier changes, so an
/// absent field means "same rate as the tier below", not "free" or "unpriced".
/// `audio`, `input_audio_cache`, and `input_cache_write_1h` also appear on a
/// few tiers but are out of scope, matching the base [`Pricing`] struct.
#[derive(Debug, Deserialize)]
struct Override {
    /// Inclusive lower bound, in tokens (not thousands), of this tier.
    min_prompt_tokens: i64,
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    completion: Option<String>,
    #[serde(default)]
    input_cache_read: Option<String>,
    #[serde(default)]
    input_cache_write: Option<String>,
}

/// Parse a decimal-string USD-per-token rate into USD per 1M tokens.
fn parse_per_million(raw: &str) -> Result<f64> {
    raw.parse::<f64>()
        .map(|value| value * TOKENS_PER_MILLION)
        .map_err(|e| MaintainError::Storage(format!("openrouter: unparseable rate '{raw}': {e}")))
}

/// Parse an optional decimal-string USD-per-token rate into USD per 1M tokens.
fn parse_optional_per_million(raw: Option<&String>) -> Result<Option<f64>> {
    raw.map(|value| parse_per_million(value)).transpose()
}

/// Parse an optional flat USD amount (already per-unit; not scaled).
fn parse_optional_flat(raw: Option<&String>) -> Result<Option<f64>> {
    raw.map(|value| {
        value
            .parse::<f64>()
            .map_err(|e| MaintainError::Storage(format!("openrouter: unparseable amount '{value}': {e}")))
    })
    .transpose()
}

/// Convert `pricing.overrides` into [`TierBoundary`]s, each field already in
/// USD per 1M tokens. Ordering and de-duplication are [`expand_tiers`]'s job.
fn resolve_tier_boundaries(overrides: &[Override]) -> Result<Vec<TierBoundary>> {
    overrides
        .iter()
        .map(|o| {
            Ok(TierBoundary {
                boundary_tokens: o.min_prompt_tokens,
                input_usd_per_1m: parse_optional_per_million(o.prompt.as_ref())?,
                output_usd_per_1m: parse_optional_per_million(o.completion.as_ref())?,
                cache_read_usd_per_1m: parse_optional_per_million(o.input_cache_read.as_ref())?,
                cache_write_usd_per_1m: parse_optional_per_million(o.input_cache_write.as_ref())?,
            })
        })
        .collect()
}

/// Build the `min_input_tokens = 0`, `max_input_tokens = None` base row for an
/// entry. Callers that need per-tier rows pass this to [`expand_tiers`] rather
/// than using it directly.
fn base_observation(entry: &ModelEntry, now: DateTime<Utc>) -> Result<RateObservation> {
    Ok(RateObservation {
        provider: PROVIDER.to_string(),
        canonical_id: Some(entry.id.clone()),
        model: entry.id.clone(),
        service_tier: "standard".to_string(),
        region: super::super::config::GLOBAL_REGION.to_string(),
        min_input_tokens: 0,
        max_input_tokens: None,
        valid_from: now,
        valid_from_source: ValidFromSource::Observed,
        input_usd_per_1m: Some(parse_per_million(&entry.pricing.prompt)?),
        output_usd_per_1m: Some(parse_per_million(&entry.pricing.completion)?),
        cache_read_usd_per_1m: parse_optional_per_million(entry.pricing.input_cache_read.as_ref())?,
        cache_write_usd_per_1m: parse_optional_per_million(entry.pricing.input_cache_write.as_ref())?,
        reasoning_usd_per_1m: parse_optional_per_million(entry.pricing.internal_reasoning.as_ref())?,
        request_usd: parse_optional_flat(entry.pricing.request.as_ref())?,
        image_input_usd_per_unit: parse_optional_flat(entry.pricing.image.as_ref())?,
        image_output_usd_per_unit: parse_optional_flat(entry.pricing.image_output.as_ref())?,
        audio_input_usd_per_second: None,
        audio_output_usd_per_second: None,
        currency: "USD".to_string(),
        source: PROVIDER.to_string(),
        source_url: None,
    })
}

/// Decode an `OpenRouter` `/api/v1/models` body into observations, expanding
/// any `pricing.overrides` into the tiered rows `icegate.prices` keys on.
///
/// # Errors
///
/// Returns an error if the body does not decode or any rate string is not a
/// number.
pub fn parse_openrouter(body: &str, now: DateTime<Utc>) -> Result<Vec<RateObservation>> {
    let response: ModelsResponse =
        serde_json::from_str(body).map_err(|e| MaintainError::Storage(format!("openrouter: decode failed: {e}")))?;

    let mut observations = Vec::with_capacity(response.data.len());
    for entry in &response.data {
        let base = base_observation(entry, now)?;
        let boundaries = resolve_tier_boundaries(&entry.pricing.overrides)?;
        observations.extend(expand_tiers(&base, &boundaries));
    }
    Ok(observations)
}

/// Fetches the `OpenRouter` public model catalogue.
pub struct OpenRouterSource {
    /// Catalogue endpoint, normally `https://openrouter.ai/api/v1/models`.
    url: String,
    /// Provider names this source is authoritative for.
    owned: Vec<String>,
    /// Hard cap on response size, enforced by [`super::fetch_capped_body`].
    max_response_bytes: usize,
}

impl OpenRouterSource {
    /// Build a source that fetches from `url`, owning `owned` providers and
    /// rejecting bodies above `max_response_bytes`.
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
impl PriceSource for OpenRouterSource {
    fn name(&self) -> &'static str {
        PROVIDER
    }

    fn owned_providers(&self) -> Vec<String> {
        self.owned.clone()
    }

    async fn fetch_rates(&self, client: &Client, now: DateTime<Utc>) -> Result<Vec<RateObservation>> {
        let body = super::fetch_capped_body(client, &self.url, self.max_response_bytes).await?;
        let mut rates = parse_openrouter(&body, now)?;
        for rate in &mut rates {
            rate.source_url = Some(self.url.clone());
        }
        Ok(rates)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::parse_openrouter;
    use crate::pricing::source::ValidFromSource;

    const FIXTURE: &str = include_str!("../../../tests/fixtures/openrouter_models.json");

    fn now() -> chrono::DateTime<chrono::Utc> {
        DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp")
    }

    #[test]
    fn converts_per_token_strings_to_usd_per_million() {
        // OpenRouter quotes USD per token: "0.000002" is $2.00 per 1M.
        let rates = parse_openrouter(FIXTURE, now()).expect("fixture parses");
        let sonnet = rates
            .iter()
            .find(|r| r.model == "anthropic/claude-sonnet-5")
            .expect("sonnet present");
        assert!((sonnet.input_usd_per_1m.unwrap() - 2.0).abs() < 1e-9);
        assert!((sonnet.output_usd_per_1m.unwrap() - 10.0).abs() < 1e-9);
        assert!((sonnet.cache_read_usd_per_1m.unwrap() - 0.2).abs() < 1e-9);
        assert!((sonnet.cache_write_usd_per_1m.unwrap() - 2.5).abs() < 1e-9);
    }

    #[test]
    fn attributes_every_row_to_the_openrouter_provider() {
        // OpenRouter resells at its own margin, so its rows are never aliases of
        // the upstream vendor's.
        let rates = parse_openrouter(FIXTURE, now()).expect("fixture parses");
        assert!(!rates.is_empty());
        assert!(rates.iter().all(|r| r.provider == "openrouter"));
        assert!(rates.iter().all(|r| r.region == "global"));
        assert!(rates.iter().all(|r| r.service_tier == "standard"));
        assert!(rates.iter().all(|r| r.valid_from_source == ValidFromSource::Observed));
    }

    #[test]
    fn keeps_variant_suffixed_models_as_distinct_rows() {
        // `:free` and `:nitro` are separately-priced variants, which row-per-
        // native-name absorbs with no special handling.
        let rates = parse_openrouter(FIXTURE, now()).expect("fixture parses");
        assert!(rates.iter().any(|r| r.model.ends_with(":free")));
    }

    #[test]
    fn treats_zero_as_a_real_free_rate_not_a_missing_one() {
        let rates = parse_openrouter(FIXTURE, now()).expect("fixture parses");
        let free = rates.iter().find(|r| r.model.ends_with(":free")).expect("free model present");
        assert_eq!(free.input_usd_per_1m, Some(0.0));
    }

    #[test]
    fn rejects_a_malformed_payload() {
        // A renamed upstream field must fail loudly here, never yield a NULL rate.
        assert!(parse_openrouter("{\"data\": [{\"id\": \"x\"}]}", now()).is_err());
    }

    #[test]
    fn expands_a_single_override_tier_into_two_contiguous_rows() {
        // `openai/gpt-5.6-luna-pro` carries one override at 272000 prompt
        // tokens; without expansion the base row would price a >272k-token
        // request at the cheap base rate instead of the tiered one.
        let rates = parse_openrouter(FIXTURE, now()).expect("fixture parses");
        let rows: Vec<_> = rates.iter().filter(|r| r.model == "openai/gpt-5.6-luna-pro").collect();
        assert_eq!(rows.len(), 2, "exactly base + one tier row");

        let base = rows.iter().find(|r| r.min_input_tokens == 0).expect("base row");
        assert_eq!(base.max_input_tokens, Some(272_000), "base row bounded by the tier");

        let tier = rows.iter().find(|r| r.min_input_tokens == 272_000).expect("tier row");
        assert_eq!(tier.max_input_tokens, None, "top tier is unbounded");
        assert!((tier.input_usd_per_1m.unwrap() - 2.0).abs() < 1e-9);
        assert!((tier.output_usd_per_1m.unwrap() - 9.0).abs() < 1e-9);
    }

    #[test]
    fn expands_two_override_tiers_into_three_chained_rows() {
        // `qwen/qwen3-max-thinking` is one of the four live models with two
        // tiers, so multi-tier chaining is exercised against real data, not
        // just a synthetic payload.
        let rates = parse_openrouter(FIXTURE, now()).expect("fixture parses");
        let mut rows: Vec<_> = rates.iter().filter(|r| r.model == "qwen/qwen3-max-thinking").collect();
        rows.sort_by_key(|r| r.min_input_tokens);
        assert_eq!(rows.len(), 3, "exactly base + two tier rows");

        assert_eq!(rows[0].min_input_tokens, 0);
        assert_eq!(rows[0].max_input_tokens, Some(32_000), "no gap into the first tier");
        assert_eq!(rows[1].min_input_tokens, 32_000);
        assert_eq!(rows[1].max_input_tokens, Some(128_000), "no gap into the second tier");
        assert!((rows[1].input_usd_per_1m.unwrap() - 1.56).abs() < 1e-9);
        assert_eq!(rows[2].min_input_tokens, 128_000);
        assert_eq!(rows[2].max_input_tokens, None, "top tier is unbounded");
        assert!((rows[2].input_usd_per_1m.unwrap() - 1.95).abs() < 1e-9);
    }

    #[test]
    fn tier_row_inherits_cache_read_rate_the_override_does_not_set() {
        // Only 32 of 47 live override objects set `input_cache_read`; a tier
        // that omits it must still carry the base rate forward, or a later
        // guard would reject the row as unpriced and silently drop it.
        let synthetic = r#"{"data": [{
            "id": "synthetic/partial-tier-model",
            "pricing": {
                "prompt": "0.000001",
                "completion": "0.000002",
                "input_cache_read": "0.0000001",
                "overrides": [
                    {"min_prompt_tokens": 128000, "prompt": "0.000002", "completion": "0.000004"}
                ]
            }
        }]}"#;
        let rates = parse_openrouter(synthetic, now()).expect("synthetic payload parses");
        let tier = rates.iter().find(|r| r.min_input_tokens == 128_000).expect("tier row present");
        assert!(
            (tier.cache_read_usd_per_1m.unwrap() - 0.1).abs() < 1e-9,
            "cache-read rate falls back to the base rate"
        );
    }

    #[test]
    fn untiered_model_yields_exactly_one_unbounded_row() {
        // Regression guard: 299 of 342 live models carry no overrides at all,
        // and must not be split into spurious tier rows.
        let rates = parse_openrouter(FIXTURE, now()).expect("fixture parses");
        let rows: Vec<_> = rates.iter().filter(|r| r.model == "anthropic/claude-sonnet-5").collect();
        assert_eq!(rows.len(), 1, "no overrides means exactly one row");
        assert_eq!(rows[0].min_input_tokens, 0);
        assert_eq!(rows[0].max_input_tokens, None);
    }

    #[test]
    fn sorts_out_of_order_boundaries_before_chaining() {
        // The live payload happens to list boundaries ascending, but nothing
        // in the API contract guarantees it; a descending payload must still
        // chain into gap-free, non-overlapping rows.
        let synthetic = r#"{"data": [{
            "id": "synthetic/unsorted-tier-model",
            "pricing": {
                "prompt": "0.000001",
                "completion": "0.000002",
                "overrides": [
                    {"min_prompt_tokens": 128000, "prompt": "0.000003", "completion": "0.000004"},
                    {"min_prompt_tokens": 32000, "prompt": "0.0000015", "completion": "0.0000025"}
                ]
            }
        }]}"#;
        let rates = parse_openrouter(synthetic, now()).expect("synthetic payload parses");
        let mut rows = rates;
        rows.sort_by_key(|r| r.min_input_tokens);
        assert_eq!(rows.len(), 3);

        assert_eq!(
            rows[0].max_input_tokens,
            Some(32_000),
            "base bounded by the lower boundary, not the payload order"
        );
        assert_eq!(rows[1].min_input_tokens, 32_000);
        assert_eq!(rows[1].max_input_tokens, Some(128_000));
        assert!((rows[1].input_usd_per_1m.unwrap() - 1.5).abs() < 1e-9);
        assert_eq!(rows[2].min_input_tokens, 128_000);
        assert_eq!(rows[2].max_input_tokens, None);
        assert!((rows[2].input_usd_per_1m.unwrap() - 3.0).abs() < 1e-9);
    }
}
