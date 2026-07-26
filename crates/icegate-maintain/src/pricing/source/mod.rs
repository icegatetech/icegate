//! The source abstraction: one implementation per upstream rate card.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;

use crate::error::{MaintainError, Result};

/// AWS Bedrock first-party, per-region rate-card source.
pub mod bedrock;
/// LiteLLM community rate-card source.
pub mod litellm;
/// OpenRouter aggregator source.
pub mod openrouter;

/// Tokens per unit of the `*_usd_per_1m` columns.
///
/// Shared by every source that quotes a per-token rate; see
/// [`RateObservation::input_usd_per_1m`].
pub const TOKENS_PER_MILLION: f64 = 1_000_000.0;

/// Where a rate's `valid_from` came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidFromSource {
    /// The source published an effective date (AWS does).
    Vendor,
    /// The crawl that first observed the change; the vendor published none.
    Observed,
}

impl ValidFromSource {
    /// String written to `prices.valid_from_source`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Vendor => "vendor",
            Self::Observed => "observed",
        }
    }
}

/// Identity of one rate line: everything that makes two observations the same
/// series rather than two parallel ones.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RateKey {
    /// Serving platform, matching `operations.provider_name`.
    pub provider: String,
    /// The platform's own model name, matching `operations.request_model`.
    pub model: String,
    /// `standard`, `batch`, `provisioned`, …
    pub service_tier: String,
    /// Reseller region, or `global`.
    pub region: String,
    /// Context-tier lower bound, inclusive.
    pub min_input_tokens: i64,
}

/// One rate line as observed from a source, before guarding and diffing.
#[derive(Debug, Clone, PartialEq)]
pub struct RateObservation {
    /// Serving platform.
    pub provider: String,
    /// The platform's own model name.
    pub model: String,
    /// Cross-provider model identity; `None` when no derivation rule fires.
    pub canonical_id: Option<String>,
    /// Service tier.
    pub service_tier: String,
    /// Reseller region, or `global`.
    pub region: String,
    /// Context-tier lower bound, inclusive.
    pub min_input_tokens: i64,
    /// Context-tier upper bound, exclusive; `None` is unbounded.
    pub max_input_tokens: Option<i64>,
    /// When this rate took effect.
    pub valid_from: DateTime<Utc>,
    /// Whether `valid_from` is the vendor's date or our observation time.
    pub valid_from_source: ValidFromSource,
    /// USD per 1M input tokens.
    pub input_usd_per_1m: Option<f64>,
    /// USD per 1M output tokens.
    pub output_usd_per_1m: Option<f64>,
    /// USD per 1M cache-read tokens.
    pub cache_read_usd_per_1m: Option<f64>,
    /// USD per 1M cache-write tokens.
    pub cache_write_usd_per_1m: Option<f64>,
    /// USD per 1M reasoning tokens.
    pub reasoning_usd_per_1m: Option<f64>,
    /// Flat per-request fee in USD.
    pub request_usd: Option<f64>,
    /// USD per input image.
    pub image_input_usd_per_unit: Option<f64>,
    /// USD per output image.
    pub image_output_usd_per_unit: Option<f64>,
    /// USD per second of input audio.
    pub audio_input_usd_per_second: Option<f64>,
    /// USD per second of output audio.
    pub audio_output_usd_per_second: Option<f64>,
    /// Currency code; only `USD` survives guard 7.
    pub currency: String,
    /// Source identifier, written to `prices.source`.
    pub source: String,
    /// Provenance URL.
    pub source_url: Option<String>,
}

impl RateObservation {
    /// The identity of this rate line.
    #[must_use]
    pub fn key(&self) -> RateKey {
        RateKey {
            provider: self.provider.clone(),
            model: self.model.clone(),
            service_tier: self.service_tier.clone(),
            region: self.region.clone(),
            min_input_tokens: self.min_input_tokens,
        }
    }

    /// Whether any priced column differs from `other`.
    ///
    /// Deliberately ignores `canonical_id`, `valid_from`, `valid_from_source`,
    /// `source`, and `source_url`: those are provenance and identity metadata, and
    /// a change in them is not a price change. Appending a row for them would
    /// pollute the revision history with non-events.
    #[must_use]
    pub fn rates_differ(&self, other: &Self) -> bool {
        self.input_usd_per_1m != other.input_usd_per_1m
            || self.output_usd_per_1m != other.output_usd_per_1m
            || self.cache_read_usd_per_1m != other.cache_read_usd_per_1m
            || self.cache_write_usd_per_1m != other.cache_write_usd_per_1m
            || self.reasoning_usd_per_1m != other.reasoning_usd_per_1m
            || self.request_usd != other.request_usd
            || self.image_input_usd_per_unit != other.image_input_usd_per_unit
            || self.image_output_usd_per_unit != other.image_output_usd_per_unit
            || self.audio_input_usd_per_second != other.audio_input_usd_per_second
            || self.audio_output_usd_per_second != other.audio_output_usd_per_second
            || self.max_input_tokens != other.max_input_tokens
            || self.currency != other.currency
    }
}

/// One context-tier boundary: the rates that start applying once
/// `input_tokens >= boundary_tokens`.
///
/// A `None` rate means "unchanged from the tier below", never "unpriced" — the
/// upstream feeds emit only the columns a tier actually revises. Sources that
/// tier only the token directions (`LiteLLM`) leave the cache columns `None` and
/// get the base row's cache rates carried forward, which is the correct
/// reading of their payloads.
#[derive(Debug, Clone, Copy)]
pub struct TierBoundary {
    /// Inclusive lower bound of this tier, in tokens.
    pub boundary_tokens: i64,
    /// USD per 1M input tokens from this boundary up.
    pub input_usd_per_1m: Option<f64>,
    /// USD per 1M output tokens from this boundary up.
    pub output_usd_per_1m: Option<f64>,
    /// USD per 1M cache-read tokens from this boundary up.
    pub cache_read_usd_per_1m: Option<f64>,
    /// USD per 1M cache-write tokens from this boundary up.
    pub cache_write_usd_per_1m: Option<f64>,
}

/// Expand `base` (a `min_input_tokens = 0`, `max_input_tokens = None` row) into
/// contiguous, non-overlapping per-tier rows.
///
/// Row `i` covers `[boundaries[i-1], boundaries[i])` (the base row covers
/// `[0, boundaries[0])`), and each row's rate is the most recent boundary at or
/// below its lower bound.
///
/// `boundaries` need not be sorted or distinct: it is sorted here, and a
/// boundary at or below zero — or a repeat of the previous one — is dropped,
/// because either would emit a second row sharing the base row's (or the
/// previous tier's) [`RateKey`]. Two rows with one key in a single batch are
/// both appended with the same `valid_from`, after which `LiveRates` keeps one
/// arbitrarily and the other is re-appended on every subsequent crawl.
#[must_use]
pub fn expand_tiers(base: &RateObservation, boundaries: &[TierBoundary]) -> Vec<RateObservation> {
    let mut sorted: Vec<TierBoundary> = boundaries.iter().copied().filter(|b| b.boundary_tokens > 0).collect();
    sorted.sort_by_key(|b| b.boundary_tokens);
    sorted.dedup_by_key(|b| b.boundary_tokens);

    let Some(first) = sorted.first() else {
        return vec![base.clone()];
    };

    let mut rows = Vec::with_capacity(sorted.len() + 1);
    rows.push(RateObservation {
        max_input_tokens: Some(first.boundary_tokens),
        ..base.clone()
    });

    let mut input_usd_per_1m = base.input_usd_per_1m;
    let mut output_usd_per_1m = base.output_usd_per_1m;
    let mut cache_read_usd_per_1m = base.cache_read_usd_per_1m;
    let mut cache_write_usd_per_1m = base.cache_write_usd_per_1m;
    for (i, boundary) in sorted.iter().enumerate() {
        input_usd_per_1m = boundary.input_usd_per_1m.or(input_usd_per_1m);
        output_usd_per_1m = boundary.output_usd_per_1m.or(output_usd_per_1m);
        cache_read_usd_per_1m = boundary.cache_read_usd_per_1m.or(cache_read_usd_per_1m);
        cache_write_usd_per_1m = boundary.cache_write_usd_per_1m.or(cache_write_usd_per_1m);
        rows.push(RateObservation {
            min_input_tokens: boundary.boundary_tokens,
            max_input_tokens: sorted.get(i + 1).map(|b| b.boundary_tokens),
            input_usd_per_1m,
            output_usd_per_1m,
            cache_read_usd_per_1m,
            cache_write_usd_per_1m,
            ..base.clone()
        });
    }
    rows
}

/// One upstream rate card.
///
/// Every implementation decodes into typed `serde` structs so a renamed upstream
/// field is a hard decode error rather than a silently missing rate — the reason
/// this design admits no HTML scraping.
#[async_trait]
pub trait PriceSource: Send + Sync {
    /// Stable identifier recorded in `prices.source`.
    fn name(&self) -> &'static str;

    /// Providers this source is authoritative for.
    ///
    /// Rates for any other provider are discarded, so two sources can never emit
    /// the same key. This makes precedence structural: there is no tiebreak
    /// logic anywhere because collisions are impossible.
    fn owned_providers(&self) -> Vec<String>;

    /// Fetch and normalise the full rate card.
    ///
    /// `now` is the crawl timestamp, used as `valid_from` for sources that
    /// publish no effective date.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the response exceeds the
    /// configured size cap, or the payload does not decode.
    async fn fetch_rates(&self, client: &Client, now: DateTime<Utc>) -> Result<Vec<RateObservation>>;
}

/// Fetch a body, enforcing guard 1: status, and a hard cap on response size.
///
/// The cap matters because the AWS price list is tens of megabytes and a
/// mis-pointed URL could otherwise stream unbounded data into the maintain pod.
/// Streaming chunk-by-chunk means the cap is enforced before the body is
/// buffered, not after.
///
/// # Errors
///
/// Returns an error if the request fails, the status is not success, or the
/// body exceeds `max_bytes`.
pub async fn fetch_capped_body(client: &Client, url: &str, max_bytes: usize) -> Result<String> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| MaintainError::Storage(format!("pricing source '{url}': request failed: {e}")))?
        .error_for_status()
        .map_err(|e| MaintainError::Storage(format!("pricing source '{url}': bad status: {e}")))?;

    read_capped(response.bytes_stream(), max_bytes, url).await
}

/// Accumulate a byte stream into a `String`, rejecting it as soon as it would
/// exceed `max_bytes`.
///
/// Split out from [`fetch_capped_body`] so the bound can be tested without a
/// network round trip: the cap is the guard against an unbounded response, and
/// an untested guard is not a guard.
async fn read_capped<S, E>(mut stream: S, max_bytes: usize, url: &str) -> Result<String>
where
    S: futures::Stream<Item = std::result::Result<bytes::Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    use futures::StreamExt;

    let mut buffer: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| MaintainError::Storage(format!("pricing source '{url}': read failed: {e}")))?;
        if buffer.len() + chunk.len() > max_bytes {
            return Err(MaintainError::Storage(format!(
                "pricing source '{url}': response exceeded max_response_bytes ({max_bytes})"
            )));
        }
        buffer.extend_from_slice(&chunk);
    }

    String::from_utf8(buffer)
        .map_err(|e| MaintainError::Storage(format!("pricing source '{url}': body is not UTF-8: {e}")))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use chrono::DateTime;

    use super::{RateObservation, ValidFromSource};

    impl RateObservation {
        /// A fully-populated observation for assertions to vary one field of.
        fn sample() -> Self {
            Self {
                provider: "anthropic".to_string(),
                model: "claude-opus-4-8".to_string(),
                canonical_id: Some("anthropic/claude-opus-4-8".to_string()),
                service_tier: "standard".to_string(),
                region: "global".to_string(),
                min_input_tokens: 0,
                max_input_tokens: None,
                valid_from: DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp"),
                valid_from_source: ValidFromSource::Observed,
                input_usd_per_1m: Some(5.0),
                output_usd_per_1m: Some(25.0),
                cache_read_usd_per_1m: None,
                cache_write_usd_per_1m: None,
                reasoning_usd_per_1m: None,
                request_usd: None,
                image_input_usd_per_unit: None,
                image_output_usd_per_unit: None,
                audio_input_usd_per_second: None,
                audio_output_usd_per_second: None,
                currency: "USD".to_string(),
                source: "litellm".to_string(),
                source_url: None,
            }
        }
    }

    #[test]
    fn observation_key_excludes_rates_and_provenance() {
        // The key decides what counts as "the same rate line". Two observations
        // differing only in price must share a key, or the diff would append a
        // parallel series instead of a revision.
        let cheap = RateObservation {
            input_usd_per_1m: Some(1.0),
            ..RateObservation::sample()
        };
        let dear = RateObservation {
            input_usd_per_1m: Some(99.0),
            source_url: Some("https://elsewhere.example".to_string()),
            ..RateObservation::sample()
        };
        assert_eq!(cheap.key(), dear.key());
    }

    #[test]
    fn key_differs_when_any_identity_field_changes() {
        // Table-driven over all 5 components of `key()`: a regression that
        // dropped one from the derivation would silently coalesce distinct rate
        // lines (e.g. two regions) into a single series.
        let mutators: [(&str, fn(&mut RateObservation)); 5] = [
            ("provider", |o| o.provider = "openai".to_string()),
            ("model", |o| o.model = "gpt-5".to_string()),
            ("service_tier", |o| o.service_tier = "batch".to_string()),
            ("region", |o| o.region = "eu-west-1".to_string()),
            ("min_input_tokens", |o| o.min_input_tokens = 128_000),
        ];

        for (field, mutate) in mutators {
            let base = RateObservation::sample();
            let mut changed = RateObservation::sample();
            mutate(&mut changed);
            assert_ne!(base.key(), changed.key(), "key() should differ when `{field}` changes");
        }
    }

    #[test]
    fn rates_differ_detects_a_change_in_any_compared_field() {
        // Table-driven over all 12 columns `rates_differ` compares: a regression
        // that dropped one from the comparison would mean a real price change on
        // that column never gets recorded as a new revision.
        let mutators: [(&str, fn(&mut RateObservation)); 12] = [
            ("input_usd_per_1m", |o| o.input_usd_per_1m = Some(999.0)),
            ("output_usd_per_1m", |o| o.output_usd_per_1m = Some(999.0)),
            ("cache_read_usd_per_1m", |o| o.cache_read_usd_per_1m = Some(999.0)),
            ("cache_write_usd_per_1m", |o| o.cache_write_usd_per_1m = Some(999.0)),
            ("reasoning_usd_per_1m", |o| o.reasoning_usd_per_1m = Some(999.0)),
            ("request_usd", |o| o.request_usd = Some(999.0)),
            ("image_input_usd_per_unit", |o| o.image_input_usd_per_unit = Some(999.0)),
            ("image_output_usd_per_unit", |o| {
                o.image_output_usd_per_unit = Some(999.0);
            }),
            ("audio_input_usd_per_second", |o| {
                o.audio_input_usd_per_second = Some(999.0);
            }),
            ("audio_output_usd_per_second", |o| {
                o.audio_output_usd_per_second = Some(999.0);
            }),
            ("max_input_tokens", |o| o.max_input_tokens = Some(999)),
            ("currency", |o| o.currency = "EUR".to_string()),
        ];

        for (field, mutate) in mutators {
            let base = RateObservation::sample();
            let mut changed = RateObservation::sample();
            mutate(&mut changed);
            assert!(
                base.rates_differ(&changed),
                "rates_differ should detect a change in `{field}`"
            );
        }

        assert!(!RateObservation::sample().rates_differ(&RateObservation::sample()));
    }

    #[test]
    fn rates_differ_ignores_provenance() {
        // A source URL change is not a price change and must not append a row.
        let base = RateObservation::sample();
        let relabelled = RateObservation {
            source_url: Some("https://mirror.example".to_string()),
            valid_from_source: ValidFromSource::Vendor,
            ..RateObservation::sample()
        };
        assert!(!base.rates_differ(&relabelled));
    }

    #[test]
    fn valid_from_source_labels_are_stable() {
        // These strings are written to `prices.valid_from_source` and are part of
        // the table's contract; renaming one silently rewrites history semantics.
        assert_eq!(ValidFromSource::Vendor.as_str(), "vendor");
        assert_eq!(ValidFromSource::Observed.as_str(), "observed");
    }

    /// Build a synthetic byte stream from chunk contents, standing in for
    /// `Response::bytes_stream()` without a network round trip.
    fn chunk_stream(
        chunks: &[&[u8]],
    ) -> impl futures::Stream<Item = std::result::Result<bytes::Bytes, std::io::Error>> {
        futures::stream::iter(chunks.iter().copied().map(|chunk| Ok(bytes::Bytes::copy_from_slice(chunk))))
    }

    #[tokio::test]
    async fn read_capped_returns_full_body_under_cap() {
        let body = super::read_capped(chunk_stream(&[b"hello world"]), 100, "http://example.test")
            .await
            .unwrap();
        assert_eq!(body, "hello world");
    }

    #[tokio::test]
    async fn read_capped_rejects_stream_that_crosses_cap_mid_stream() {
        // Three 4-byte chunks against a 10-byte cap: the first two fit (4, then
        // 8), the third pushes the running total to 12. Feeding several chunks
        // proves the check runs per-chunk against the running total, not once
        // against a fully-buffered body.
        let result = super::read_capped(chunk_stream(&[b"aaaa", b"bbbb", b"cccc"]), 10, "http://example.test").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_capped_accepts_body_exactly_at_cap() {
        // An off-by-one in the `>` vs `>=` comparison would reject a legitimate
        // response that lands exactly on the cap.
        let body = super::read_capped(chunk_stream(&[b"aaaaa", b"bbbbb"]), 10, "http://example.test")
            .await
            .unwrap();
        assert_eq!(body, "aaaaabbbbb");
    }

    fn boundary(boundary_tokens: i64, input: Option<f64>) -> super::TierBoundary {
        super::TierBoundary {
            boundary_tokens,
            input_usd_per_1m: input,
            output_usd_per_1m: None,
            cache_read_usd_per_1m: None,
            cache_write_usd_per_1m: None,
        }
    }

    #[test]
    fn expand_tiers_chains_sorted_contiguous_non_overlapping_rows() {
        // The payload order must not decide the chaining: an out-of-order pair
        // still has to produce [0, 32k), [32k, 128k), [128k, inf).
        let rows = super::expand_tiers(
            &RateObservation::sample(),
            &[boundary(128_000, Some(30.0)), boundary(32_000, Some(15.0))],
        );
        assert_eq!(rows.len(), 3);
        assert_eq!((rows[0].min_input_tokens, rows[0].max_input_tokens), (0, Some(32_000)));
        assert_eq!(
            (rows[1].min_input_tokens, rows[1].max_input_tokens),
            (32_000, Some(128_000))
        );
        assert_eq!((rows[2].min_input_tokens, rows[2].max_input_tokens), (128_000, None));
        assert_eq!(rows[1].input_usd_per_1m, Some(15.0));
        assert_eq!(rows[2].input_usd_per_1m, Some(30.0));
    }

    #[test]
    fn expand_tiers_carries_unset_columns_forward() {
        // A boundary that revises only the input direction must not blank the
        // rest: an absent field means "unchanged here", not "unpriced here".
        let rows = super::expand_tiers(&RateObservation::sample(), &[boundary(32_000, Some(15.0))]);
        assert_eq!(rows[1].output_usd_per_1m, RateObservation::sample().output_usd_per_1m);
    }

    #[test]
    fn expand_tiers_drops_a_boundary_that_would_duplicate_a_rate_key() {
        // A boundary at zero collides with the base row, and a repeated
        // boundary collides with the tier before it. Either would put two rows
        // with one `RateKey` in a single batch: both get appended with the same
        // `valid_from`, `LiveRates` then keeps one arbitrarily, and the loser is
        // re-appended on every subsequent crawl.
        let rows = super::expand_tiers(
            &RateObservation::sample(),
            &[
                boundary(0, Some(1.0)),
                boundary(32_000, Some(15.0)),
                boundary(32_000, Some(99.0)),
            ],
        );
        let keys: Vec<_> = rows.iter().map(RateObservation::key).collect();
        let mut distinct = keys.clone();
        distinct.dedup();
        assert_eq!(keys.len(), distinct.len(), "no two rows may share a key");
        assert_eq!(rows.len(), 2, "base + the one surviving boundary");
        assert_eq!(rows[1].min_input_tokens, 32_000);
    }

    #[test]
    fn expand_tiers_without_boundaries_yields_one_unbounded_row() {
        let rows = super::expand_tiers(&RateObservation::sample(), &[]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].min_input_tokens, 0);
        assert_eq!(rows[0].max_input_tokens, None);
    }

    #[tokio::test]
    async fn read_capped_rejects_non_utf8_body() {
        let invalid_utf8: &[u8] = &[0x68, 0x65, 0xFF, 0x6c, 0x6c, 0x6f];
        let result = super::read_capped(chunk_stream(&[invalid_utf8]), 100, "http://example.test").await;
        assert!(result.is_err());
    }
}
