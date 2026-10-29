//! `OpenRouter`: an aggregator that resells many vendors' models at its own
//! margin, and is therefore authoritative only for the `openrouter` provider.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;

use super::{
    PriceSource, RateObservation, TOKENS_PER_MILLION, TierBoundary, ValidFromSource, WeekRange, WeekWindow,
    expand_tiers, expand_week_windows,
};
use crate::error::{MaintainError, Result};

/// Provider string `OpenRouter` rows are attributed to.
const PROVIDER: &str = "openrouter";

/// Minutes in one UTC day, the stride between weekdays in a week minute.
const MINUTES_PER_DAY: i32 = 24 * 60;

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
/// per-tier or per-window rows by [`expand_overrides`].
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
    /// Conditional rates: context tiers keyed by `min_prompt_tokens`, or
    /// time-of-day windows keyed by `utc_days`/`utc_start`/`utc_end`. One
    /// model's list holds one kind only; see [`expand_overrides`].
    #[serde(default)]
    overrides: Vec<Override>,
}

/// One entry of `pricing.overrides`: a condition plus the rates that apply
/// while it holds. Every rate field is optional because `OpenRouter` emits only
/// the prices an entry changes; what an absent one falls back to depends on the
/// kind of condition (see [`TierBoundary`] and [`WeekWindow`]). `audio`,
/// `input_audio_cache`, and `input_cache_write_1h` also appear on a few entries
/// but are out of scope, matching the base [`Pricing`] struct.
#[derive(Debug, Deserialize)]
struct Override {
    /// Context-tier condition: the tier's lower bound, in tokens (not
    /// thousands), stored as an inclusive bound.
    #[serde(default)]
    min_prompt_tokens: Option<i64>,
    /// Time-of-day condition: the UTC weekdays the entry applies on, evaluated
    /// at the request instant. Scopes the `utc_start`/`utc_end` window, or the
    /// whole day without one; absent means every day.
    #[serde(default)]
    utc_days: Option<Vec<UtcDay>>,
    /// Time-of-day condition: window start as an `HHMM` UTC clock, inclusive.
    #[serde(default)]
    utc_start: Option<u16>,
    /// Time-of-day condition: window end as an `HHMM` UTC clock, exclusive. A
    /// window whose end is not after its start wraps past midnight, so `0`
    /// ends it at midnight.
    #[serde(default)]
    utc_end: Option<u16>,
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    completion: Option<String>,
    #[serde(default)]
    input_cache_read: Option<String>,
    #[serde(default)]
    input_cache_write: Option<String>,
}

impl Override {
    /// Whether this entry carries any time-of-day condition.
    const fn is_time_window(&self) -> bool {
        self.utc_days.is_some() || self.utc_start.is_some() || self.utc_end.is_some()
    }
}

/// A UTC weekday as `OpenRouter` spells it in `utc_days`. An unknown name is a
/// decode error, not a day skipped.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum UtcDay {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

impl UtcDay {
    /// Days since Monday, matching the Monday-00:00 origin of a week minute.
    const fn days_since_monday(self) -> i32 {
        match self {
            Self::Monday => 0,
            Self::Tuesday => 1,
            Self::Wednesday => 2,
            Self::Thursday => 3,
            Self::Friday => 4,
            Self::Saturday => 5,
            Self::Sunday => 6,
        }
    }
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

/// An error naming `model` and what is wrong with its overrides.
fn override_error(model: &str, problem: &str) -> MaintainError {
    MaintainError::Storage(format!("openrouter: model '{model}': {problem}"))
}

/// Expand `base` by the model's `pricing.overrides`.
///
/// A list of context tiers expands through [`expand_tiers`], a list of
/// time-of-day windows through [`expand_week_windows`]. A list mixing the two,
/// or an entry combining a token threshold with a window, is an error: a row
/// carries either a token range or a week range of the base rate, never a
/// product of both.
///
/// # Errors
///
/// Returns an error if the overrides mix condition kinds, an entry carries no
/// condition, a window is malformed, or a rate string is not a number.
fn expand_overrides(entry: &ModelEntry, base: &RateObservation) -> Result<Vec<RateObservation>> {
    let overrides = &entry.pricing.overrides;
    let time_windows = overrides.iter().filter(|o| o.is_time_window()).count();
    if time_windows == 0 {
        let boundaries = resolve_tier_boundaries(&entry.id, overrides)?;
        return Ok(expand_tiers(base, &boundaries));
    }
    if time_windows < overrides.len() {
        return Err(override_error(
            &entry.id,
            "overrides mix context tiers and time-of-day windows",
        ));
    }
    let windows = overrides
        .iter()
        .map(|o| resolve_week_window(&entry.id, o))
        .collect::<Result<Vec<_>>>()?;
    Ok(expand_week_windows(base, &windows))
}

/// Convert context-tier overrides into [`TierBoundary`]s, each field already in
/// USD per 1M tokens. Ordering and de-duplication are [`expand_tiers`]'s job.
fn resolve_tier_boundaries(model: &str, overrides: &[Override]) -> Result<Vec<TierBoundary>> {
    overrides
        .iter()
        .map(|o| {
            Ok(TierBoundary {
                boundary_tokens: o
                    .min_prompt_tokens
                    .ok_or_else(|| override_error(model, "an override carries no condition"))?,
                input_usd_per_1m: parse_optional_per_million(o.prompt.as_ref())?,
                output_usd_per_1m: parse_optional_per_million(o.completion.as_ref())?,
                cache_read_usd_per_1m: parse_optional_per_million(o.input_cache_read.as_ref())?,
                cache_write_usd_per_1m: parse_optional_per_million(o.input_cache_write.as_ref())?,
            })
        })
        .collect()
}

/// Minutes since midnight of an `HHMM` clock time, or `None` if it names no
/// minute of the day (hour above 23 or minute above 59).
fn minute_of_day(hhmm: u16) -> Option<i32> {
    let (hour, minute) = (hhmm / 100, hhmm % 100);
    (hour < 24 && minute < 60).then(|| i32::from(hour) * 60 + i32::from(minute))
}

/// Convert one time-of-day override into the [`WeekWindow`] it covers.
///
/// Each listed day contributes its own share of the week, and a wrapping
/// window covers both ends of that same day: the day is the request's, and so
/// is the clock time compared against the window.
fn resolve_week_window(model: &str, entry: &Override) -> Result<WeekWindow> {
    if entry.min_prompt_tokens.is_some() {
        return Err(override_error(
            model,
            "an override combines a token threshold with a time window",
        ));
    }
    let daily = match (entry.utc_start, entry.utc_end) {
        (None, None) => None,
        (Some(start), Some(end)) => {
            let start = minute_of_day(start).ok_or_else(|| override_error(model, "utc_start is not an HHMM time"))?;
            let end = minute_of_day(end).ok_or_else(|| override_error(model, "utc_end is not an HHMM time"))?;
            Some((start, end))
        }
        _ => {
            return Err(override_error(
                model,
                "a time window sets only one of utc_start and utc_end",
            ));
        }
    };

    // One bit per weekday, Monday lowest, so a repeated name contributes its
    // day once; every day when `utc_days` is absent.
    let days: u8 = match &entry.utc_days {
        None => 0b111_1111,
        Some(listed) if listed.is_empty() => return Err(override_error(model, "utc_days is empty")),
        Some(listed) => listed.iter().fold(0, |mask, day| mask | (1 << day.days_since_monday())),
    };

    let mut ranges = Vec::new();
    for midnight in (0..7).filter(|&day| days & (1 << day) != 0).map(|day| day * MINUTES_PER_DAY) {
        let spans = match daily {
            None => [Some((0, MINUTES_PER_DAY)), None],
            Some((start, end)) if end > start => [Some((start, end)), None],
            Some((start, end)) => [Some((start, MINUTES_PER_DAY)), (end > 0).then_some((0, end))],
        };
        for (start, end) in spans.into_iter().flatten() {
            // Unreachable for a validated clock: every span is non-empty and
            // inside its own day.
            ranges.push(WeekRange::new(midnight + start, midnight + end).ok_or_else(|| {
                MaintainError::InvariantViolation(format!("openrouter: model '{model}': empty or out-of-week window"))
            })?);
        }
    }

    Ok(WeekWindow {
        ranges,
        input_usd_per_1m: parse_optional_per_million(entry.prompt.as_ref())?,
        output_usd_per_1m: parse_optional_per_million(entry.completion.as_ref())?,
        cache_read_usd_per_1m: parse_optional_per_million(entry.input_cache_read.as_ref())?,
        cache_write_usd_per_1m: parse_optional_per_million(entry.input_cache_write.as_ref())?,
    })
}

/// Build the `min_input_tokens = 0`, `max_input_tokens = None` base row for an
/// entry. Callers pass this to [`expand_overrides`] rather than using it
/// directly.
fn base_observation(entry: &ModelEntry, now: DateTime<Utc>) -> Result<RateObservation> {
    Ok(RateObservation {
        provider: PROVIDER.to_string(),
        canonical_id: Some(entry.id.clone()),
        model: entry.id.clone(),
        service_tier: "standard".to_string(),
        region: super::super::config::GLOBAL_REGION.to_string(),
        min_input_tokens: 0,
        max_input_tokens: None,
        min_week_minute: None,
        max_week_minute: None,
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
/// any `pricing.overrides` into the context-tier or time-of-day rows
/// `icegate.prices` keys on.
///
/// # Errors
///
/// Returns an error if the body does not decode, any rate string is not a
/// number, or any model's overrides take a shape [`expand_overrides`] rejects.
/// One such model fails the whole catalogue.
pub fn parse_openrouter(body: &str, now: DateTime<Utc>) -> Result<Vec<RateObservation>> {
    let response: ModelsResponse =
        serde_json::from_str(body).map_err(|e| MaintainError::Storage(format!("openrouter: decode failed: {e}")))?;

    let mut observations = Vec::with_capacity(response.data.len());
    for entry in &response.data {
        let base = base_observation(entry, now)?;
        observations.extend(expand_overrides(entry, &base)?);
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
    use crate::error::MaintainError;
    use crate::pricing::source::{RateObservation, ValidFromSource};

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
    fn decodes_a_catalogue_mixing_time_of_day_and_context_tier_models() {
        // Regression: from 2026-10-29 the live catalogue carried time-of-day
        // overrides with no `min_prompt_tokens`, and decoding the whole body
        // failed with "missing field `min_prompt_tokens`", so no OpenRouter
        // rate was priced at all. The tier model must still expand as before.
        let synthetic = r#"{"data": [
            {
                "id": "synthetic/context-tier-model",
                "pricing": {
                    "prompt": "0.000001",
                    "completion": "0.000002",
                    "overrides": [
                        {"min_prompt_tokens": 128000, "prompt": "0.000002", "completion": "0.000004"}
                    ]
                }
            },
            {
                "id": "synthetic/off-peak-model",
                "pricing": {
                    "prompt": "0.0000003",
                    "completion": "0.0000012",
                    "overrides": [
                        {"utc_days": ["saturday", "sunday"], "prompt": "0.00000015", "completion": "0.0000006"},
                        {"utc_days": ["monday", "tuesday", "wednesday", "thursday", "friday"],
                         "utc_start": 100, "utc_end": 400,
                         "prompt": "0.0000003", "completion": "0.0000012"}
                    ]
                }
            }
        ]}"#;
        let rates = parse_openrouter(synthetic, now()).expect("a time-of-day override decodes");

        let mut tier_rows: Vec<_> = rates.iter().filter(|r| r.model == "synthetic/context-tier-model").collect();
        tier_rows.sort_by_key(|r| r.min_input_tokens);
        assert_eq!(tier_rows.len(), 2, "base + one context tier");
        assert_eq!(tier_rows[0].max_input_tokens, Some(128_000));
        assert_eq!(tier_rows[1].min_input_tokens, 128_000);
        assert!(
            tier_rows
                .iter()
                .all(|r| r.min_week_minute.is_none() && r.max_week_minute.is_none()),
            "context tiers are not time-scoped"
        );

        // The weekday window restates the base rate, so the week splits only
        // where Saturday 00:00 UTC begins (minute 5 * 1440).
        assert_eq!(
            week_rows(&rates, "synthetic/off-peak-model"),
            vec![
                (Some(0), Some(7_200), Some(0.3), Some(1.2), None),
                (Some(7_200), None, Some(0.15), Some(0.6), None),
            ]
        );
    }

    /// `(min_week_minute, max_week_minute, input, output, cache_read)` of
    /// `model`'s rows in week order, rates rounded to the storage grid so
    /// per-token strings scaled by a million compare exactly.
    #[allow(clippy::type_complexity)]
    fn week_rows(
        rates: &[RateObservation],
        model: &str,
    ) -> Vec<(Option<i32>, Option<i32>, Option<f64>, Option<f64>, Option<f64>)> {
        let round = |rate: Option<f64>| rate.map(|value| (value * 1e10).round() / 1e10);
        let mut rows: Vec<_> = rates
            .iter()
            .filter(|r| r.model == model)
            .map(|r| {
                (
                    r.min_week_minute,
                    r.max_week_minute,
                    round(r.input_usd_per_1m),
                    round(r.output_usd_per_1m),
                    round(r.cache_read_usd_per_1m),
                )
            })
            .collect();
        rows.sort_by_key(|row| row.0);
        rows
    }

    /// A one-model catalogue priced at $2 in / $4 out / $0.20 cache-read per
    /// 1M tokens, carrying `overrides` verbatim.
    fn catalogue_with_overrides(overrides: &str) -> String {
        format!(
            r#"{{"data": [{{"id": "synthetic/windowed-model", "pricing": {{
                "prompt": "0.000002", "completion": "0.000004", "input_cache_read": "0.0000002",
                "overrides": [{overrides}]
            }}}}]}}"#
        )
    }

    #[test]
    fn a_wrapping_window_covers_both_ends_of_its_own_day_not_the_next() {
        // `utc_days` is evaluated at the request instant, so 23:00-01:00 on
        // Monday is Monday 00:00-01:00 and 23:00-24:00 — Tuesday 00:00-01:00
        // keeps the base rate. A price the window omits (completion) stays
        // at the base rate inside it.
        let body = catalogue_with_overrides(
            r#"{"utc_days": ["monday"], "utc_start": 2300, "utc_end": 100,
                "prompt": "0.000001", "input_cache_read": "0.0000001"}"#,
        );
        let rates = parse_openrouter(&body, now()).expect("catalogue parses");
        assert_eq!(
            week_rows(&rates, "synthetic/windowed-model"),
            vec![
                (Some(0), Some(60), Some(1.0), Some(4.0), Some(0.1)),
                (Some(60), Some(1_380), Some(2.0), Some(4.0), Some(0.2)),
                (Some(1_380), Some(1_440), Some(1.0), Some(4.0), Some(0.1)),
                (Some(1_440), None, Some(2.0), Some(4.0), Some(0.2)),
            ]
        );
    }

    #[test]
    fn a_window_without_utc_days_applies_on_every_day() {
        let body = catalogue_with_overrides(r#"{"utc_start": 0, "utc_end": 100, "prompt": "0.000001"}"#);
        let rates = parse_openrouter(&body, now()).expect("catalogue parses");

        // Each day: 00:00-01:00 at $1, the rest of the day at the base $2.
        let mut expected = Vec::new();
        for day in 0..7 {
            let midnight = day * 1_440;
            let next_midnight = (day < 6).then_some(midnight + 1_440);
            expected.push((Some(midnight), Some(midnight + 60), Some(1.0), Some(4.0), Some(0.2)));
            expected.push((Some(midnight + 60), next_midnight, Some(2.0), Some(4.0), Some(0.2)));
        }
        assert_eq!(week_rows(&rates, "synthetic/windowed-model"), expected);
    }

    #[test]
    fn an_end_of_zero_runs_the_window_to_midnight() {
        // Sunday 22:00 is minute 6 * 1440 + 22 * 60.
        let body = catalogue_with_overrides(
            r#"{"utc_days": ["sunday"], "utc_start": 2200, "utc_end": 0, "prompt": "0.000001"}"#,
        );
        let rates = parse_openrouter(&body, now()).expect("catalogue parses");
        assert_eq!(
            week_rows(&rates, "synthetic/windowed-model"),
            vec![
                (Some(0), Some(9_960), Some(2.0), Some(4.0), Some(0.2)),
                (Some(9_960), None, Some(1.0), Some(4.0), Some(0.2)),
            ]
        );
    }

    #[test]
    fn rejects_an_override_shape_it_cannot_price() {
        // Each of these fails the whole source rather than pricing the model
        // at a rate its overrides do not support: a threshold combined with a
        // window, or tiers mixed with windows, has no row shape here; a
        // half-open window, an empty or unknown day, an impossible clock time,
        // or an entry with no condition at all is malformed.
        let cases = [
            (
                "threshold and window in one entry",
                r#"{"min_prompt_tokens": 1000, "utc_start": 100, "utc_end": 400, "prompt": "0.000001"}"#,
            ),
            (
                "tiers and windows in one list",
                r#"{"min_prompt_tokens": 1000, "prompt": "0.000003"}, {"utc_days": ["monday"], "prompt": "0.000001"}"#,
            ),
            ("start without end", r#"{"utc_start": 100, "prompt": "0.000001"}"#),
            ("end without start", r#"{"utc_end": 100, "prompt": "0.000001"}"#),
            ("empty utc_days", r#"{"utc_days": [], "prompt": "0.000001"}"#),
            ("unknown weekday", r#"{"utc_days": ["funday"], "prompt": "0.000001"}"#),
            (
                "hour 24",
                r#"{"utc_start": 2400, "utc_end": 100, "prompt": "0.000001"}"#,
            ),
            (
                "minute 60",
                r#"{"utc_start": 160, "utc_end": 300, "prompt": "0.000001"}"#,
            ),
            ("no condition", r#"{"prompt": "0.000001"}"#),
        ];
        for (case, overrides) in cases {
            let result = parse_openrouter(&catalogue_with_overrides(overrides), now());
            assert!(
                matches!(result, Err(MaintainError::Storage(_))),
                "{case}: expected a decode error, got {result:?}"
            );
        }
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
