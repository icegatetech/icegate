//! AWS Bedrock: the first **regional** price source.
//!
//! Unlike `OpenRouter` and `LiteLLM`, one model has a distinct rate per AWS
//! region, and AWS publishes a real effective date instead of leaving
//! `valid_from` to the crawl time.
//!
//! ## What a live capture of this feed actually looks like
//!
//! A capture of `AmazonBedrock/current/index.json` on 2026-07-20 (10,912
//! products) confirmed three of the four assumptions this source was scoped
//! against, and forced two changes:
//!
//! - Region **is** a real SKU dimension, confirmed. But AWS's `location`
//!   attribute is a free-text display string ("US East (N. Virginia)"); the
//!   `regionCode` attribute ("us-east-1") is the machine-readable form AWS
//!   already computed from it, present on every product in the capture. This
//!   source reads `regionCode` directly instead of maintaining its own
//!   location-name lookup table, which would silently go stale the day AWS
//!   opens a new region.
//! - `unit: "1K tokens"` and a decimal `pricePerUnit.USD`, confirmed for every
//!   SKU this source prices -- but **not** for the whole feed. A second SKU
//!   shape exists (newer models, `service_tier` `priority`/`flex`, `model`
//!   already an invocation id, no `feature`/`inferenceType`) priced directly in
//!   `"1M tokens"`. [`sku_rate`] guards on the unit string rather than
//!   assuming it, because multiplying an already-per-1M rate by 1,000 again is
//!   the exact order-of-magnitude error this source exists to avoid.
//! - `effectiveDate` is published, confirmed: one value, `"2026-07-01T00:00:00Z"`,
//!   for the entire capture.
//! - `model` is a display name, confirmed, and mapping it is scoped tightly:
//!   see [`DISPLAY_NAME_TO_MODEL_ID`].
//!
//! One further surprise: in this capture, Anthropic's on-demand catalogue is
//! five legacy models (Claude Instant, 2.0, 2.1, 3 Sonnet, 3 Haiku) in two
//! regions (`us-east-1`, `us-west-2`), and **every one has an input-tokens SKU
//! but no output-tokens SKU**. Newer Claude models are not in this feed at
//! all -- Bedrock evidently prices them through a mechanism this bulk index
//! does not cover. A row with no output rate is still emitted (guard 7
//! upstream of this module rejects a row with neither direction priced, not
//! one missing only a direction), and [`RateObservation::output_usd_per_1m`]
//! is simply `None` for these rows until that changes.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;

use super::{PriceSource, RateObservation, ValidFromSource};
use crate::error::{MaintainError, Result};

/// Provider string Bedrock rows are attributed to; matches
/// `operations.provider_name` and the `aws.bedrock` structural rule in
/// `canonical.rs`.
const PROVIDER: &str = "aws.bedrock";
/// Stable identifier recorded in `prices.source`, matched against
/// `pricing.sources[].name` by `build_sources`.
const SOURCE_NAME: &str = "bedrock";
/// Providers this source owns unless config overrides it.
pub const DEFAULT_OWNED_PROVIDERS: &[&str] = &[PROVIDER];
/// The only `feature` value this source prices. The same `usagetype` family
/// also carries Batch Inference, Provisioned Throughput, and Model
/// Customization SKUs for the same models; folding those into a
/// `service_tier = "standard"` row would silently blend discounted or
/// committed pricing into the on-demand rate.
const ON_DEMAND_FEATURE: &str = "On-demand Inference";
/// The only unit this source converts; see the module doc for why any other
/// unit is skipped rather than scaled.
const TOKEN_UNIT: &str = "1K tokens";
/// Multiplier from a per-1K-token rate to the `*_usd_per_1m` columns.
const PER_1K_TOKENS_TO_PER_1M: f64 = 1_000.0;

/// Top-level shape of the AWS Price List Bulk API's `AmazonBedrock` index.
/// The live file also carries `formatVersion`, `disclaimer`, `offerCode`,
/// `version`, `publicationDate`, and `attributesList`; `serde` drops all of
/// them since they are absent from this struct.
#[derive(Debug, Deserialize)]
struct PriceList {
    products: HashMap<String, Product>,
    terms: Terms,
}

/// One SKU's product metadata.
#[derive(Debug, Deserialize)]
struct Product {
    attributes: ProductAttributes,
}

/// The subset of `product.attributes` this source reads. AWS ships many more
/// (`usagetype`, `operation`, `servicecode`, `provider`, ...) with no
/// corresponding use here.
#[derive(Debug, Deserialize)]
struct ProductAttributes {
    /// Display name ("Claude 3 Sonnet"), never an invocation id. Absent on a
    /// handful of SKUs (Titan models carry `titanModel` instead); those are
    /// treated the same as an unmapped name -- skipped, never guessed.
    #[serde(default)]
    model: Option<String>,
    /// Present and equal to [`ON_DEMAND_FEATURE`] only for the SKUs this
    /// source is scoped to.
    #[serde(default)]
    feature: Option<String>,
    /// `"Input tokens"` or `"Output tokens"` for the SKUs this source prices.
    /// Many other values exist (`"Input Video Token Count"`, `"input tokens
    /// batch"`, ...); anything else is out of scope and filtered out.
    #[serde(rename = "inferenceType", default)]
    inference_type: Option<String>,
    /// Machine-readable region ("us-east-1"); see the module doc for why this
    /// source reads it directly instead of parsing the `location` display
    /// string.
    #[serde(rename = "regionCode", default)]
    region_code: Option<String>,
}

/// `terms.OnDemand`: one SKU maps to a map of offer terms.
#[derive(Debug, Deserialize)]
struct Terms {
    #[serde(rename = "OnDemand")]
    on_demand: HashMap<String, HashMap<String, OfferTerm>>,
}

/// One SKU's offer term. Every SKU in a live capture carried exactly one, so
/// [`sku_rate`] reads the map's single entry rather than matching a key.
#[derive(Debug, Deserialize)]
struct OfferTerm {
    #[serde(rename = "effectiveDate")]
    effective_date: DateTime<Utc>,
    #[serde(rename = "priceDimensions")]
    price_dimensions: HashMap<String, PriceDimension>,
}

/// One price dimension. Every offer term in a live capture carried exactly
/// one, covering the full `[0, Inf)` range: Bedrock's on-demand token pricing
/// has no volume tiers.
#[derive(Debug, Deserialize)]
struct PriceDimension {
    unit: String,
    #[serde(rename = "pricePerUnit")]
    price_per_unit: PricePerUnit,
}

/// A price dimension's rate. Only `USD` is read; no SKU in a live capture
/// carried any other currency.
#[derive(Debug, Deserialize)]
struct PricePerUnit {
    #[serde(rename = "USD")]
    usd: String,
}

/// Decode a Bedrock price-list body into its typed shape.
///
/// # Errors
///
/// Returns an error if the body is not valid JSON matching [`PriceList`].
fn decode_price_list(body: &str) -> Result<PriceList> {
    serde_json::from_str(body).map_err(|e| MaintainError::Storage(format!("bedrock: decode failed: {e}")))
}

/// Which side of the exchange a SKU prices.
#[derive(Clone, Copy)]
enum Direction {
    /// Prompt tokens sent to the model.
    Input,
    /// Completion tokens the model returns.
    Output,
}

/// One SKU's parsed rate: USD per 1M tokens and the vendor's effective date.
#[derive(Clone, Copy)]
struct SkuPrice {
    usd_per_1m: f64,
    effective_date: DateTime<Utc>,
}

/// Walk `terms.OnDemand.<sku> -> <offer term> -> priceDimensions -> <rate
/// code>` to a USD-per-1M rate and the vendor's effective date.
///
/// Takes the first (and, per a live capture, only) offer term and price
/// dimension rather than matching by key: the synthetic offer-term-code and
/// rate-code strings carry no meaning of their own.
///
/// Returns `None` if `sku` is absent from `on_demand`, its unit is not
/// [`TOKEN_UNIT`] (see the module doc), or `pricePerUnit.USD` does not parse.
fn sku_rate(on_demand: &HashMap<String, HashMap<String, OfferTerm>>, sku: &str) -> Option<SkuPrice> {
    let offer_term = on_demand.get(sku)?.values().next()?;
    let dimension = offer_term.price_dimensions.values().next()?;
    if dimension.unit != TOKEN_UNIT {
        return None;
    }
    let per_1k: f64 = dimension.price_per_unit.usd.parse().ok()?;
    Some(SkuPrice {
        usd_per_1m: per_1k * PER_1K_TOKENS_TO_PER_1M,
        effective_date: offer_term.effective_date,
    })
}

/// Known Bedrock display names mapped to the invocation id
/// `operations.request_model` actually carries.
///
/// Scoped to names observed in a live capture and confirmed against AWS's own
/// Bedrock model-id documentation -- never derived or guessed. Bedrock exposes
/// on the order of 70 distinct models across a dozen providers in the live
/// feed; an unlisted display name is skipped with a WARN (see
/// [`scope_sku`]) rather than added here speculatively.
const DISPLAY_NAME_TO_MODEL_ID: &[(&str, &str)] = &[
    ("Claude Instant", "anthropic.claude-instant-v1"),
    ("Claude 2.0", "anthropic.claude-v2"),
    ("Claude 2.1", "anthropic.claude-v2:1"),
    ("Claude 3 Sonnet", "anthropic.claude-3-sonnet-20240229-v1:0"),
    ("Claude 3 Haiku", "anthropic.claude-3-haiku-20240307-v1:0"),
    ("Llama 3 8B", "meta.llama3-8b-instruct-v1:0"),
];

/// Map a Bedrock display name to its invocation id.
fn display_name_to_model_id(display: &str) -> Option<&'static str> {
    DISPLAY_NAME_TO_MODEL_ID
        .iter()
        .find(|(name, _)| *name == display)
        .map(|(_, id)| *id)
}

/// One `(model_id, region)` group's rates while folding a SKU's input and
/// output directions together.
struct RateAccumulator {
    input_usd_per_1m: Option<f64>,
    output_usd_per_1m: Option<f64>,
    /// The later of the two directions' effective dates, if they ever differ;
    /// a live capture showed one publication date for the whole file, so this
    /// only matters if that stops being true.
    effective_date: DateTime<Utc>,
}

impl RateAccumulator {
    /// Start a group from its first-seen direction.
    const fn new(direction: Direction, price: SkuPrice) -> Self {
        let (input_usd_per_1m, output_usd_per_1m) = match direction {
            Direction::Input => (Some(price.usd_per_1m), None),
            Direction::Output => (None, Some(price.usd_per_1m)),
        };
        Self {
            input_usd_per_1m,
            output_usd_per_1m,
            effective_date: price.effective_date,
        }
    }

    /// Fold in the other direction's SKU.
    fn merge(&mut self, direction: Direction, price: SkuPrice) {
        match direction {
            Direction::Input => self.input_usd_per_1m = Some(price.usd_per_1m),
            Direction::Output => self.output_usd_per_1m = Some(price.usd_per_1m),
        }
        self.effective_date = self.effective_date.max(price.effective_date);
    }
}

/// One in-scope SKU's identity: everything needed to place its rate in the
/// right `(model_id, region)` group.
struct ScopedSku<'a> {
    sku: &'a str,
    model_id: &'static str,
    region: &'a str,
    direction: Direction,
}

/// Filter one product to a [`ScopedSku`], or `None` if it falls outside what
/// this source prices.
///
/// A SKU for a feature this source does not price (batch, provisioned, ...) or
/// an inference type that is not plain token counting (image, audio, video,
/// ...) is silently out of scope: most of the feed is like this, and it is not
/// a gap. A SKU that reaches [`ON_DEMAND_FEATURE`] token pricing but then has
/// no usable region or display name **is** a gap -- those are logged at WARN.
fn scope_sku<'a>(sku: &'a str, attrs: &'a ProductAttributes) -> Option<ScopedSku<'a>> {
    if attrs.feature.as_deref() != Some(ON_DEMAND_FEATURE) {
        return None;
    }
    let direction = match attrs.inference_type.as_deref() {
        Some("Input tokens") => Direction::Input,
        Some("Output tokens") => Direction::Output,
        _ => return None,
    };
    let region = match attrs.region_code.as_deref() {
        Some(region) if !region.is_empty() => region,
        _ => {
            tracing::warn!(
                sku,
                "bedrock: on-demand token SKU has no regionCode; skipping rather than guessing"
            );
            return None;
        }
    };
    let Some(display_name) = attrs.model.as_deref() else {
        // Titan-family SKUs carry `titanModel` instead of `model`: there is no
        // display name here at all, so there is nothing to look up.
        return None;
    };
    let Some(model_id) = display_name_to_model_id(display_name) else {
        tracing::warn!(
            sku,
            display_name,
            "bedrock: unmapped display name; skipping rather than guessing"
        );
        return None;
    };
    Some(ScopedSku {
        sku,
        model_id,
        region,
        direction,
    })
}

/// Decode a Bedrock `AmazonBedrock/current/index.json` body into observations,
/// one row per `(model, region)` with the input and output SKUs merged.
///
/// # Errors
///
/// Returns an error if the body does not decode as [`PriceList`].
pub fn parse_bedrock(body: &str) -> Result<Vec<RateObservation>> {
    let price_list = decode_price_list(body)?;

    let mut groups: BTreeMap<(&str, &str), RateAccumulator> = BTreeMap::new();
    for (sku, product) in &price_list.products {
        let Some(scoped) = scope_sku(sku, &product.attributes) else {
            continue;
        };
        let Some(price) = sku_rate(&price_list.terms.on_demand, scoped.sku) else {
            tracing::warn!(
                sku = scoped.sku,
                "bedrock: in-scope SKU has no parseable 1K-token rate; skipping"
            );
            continue;
        };
        match groups.entry((scoped.model_id, scoped.region)) {
            Entry::Occupied(mut existing) => existing.get_mut().merge(scoped.direction, price),
            Entry::Vacant(slot) => {
                slot.insert(RateAccumulator::new(scoped.direction, price));
            }
        }
    }

    Ok(groups
        .into_iter()
        .map(|((model_id, region), rates)| RateObservation {
            provider: PROVIDER.to_string(),
            model: model_id.to_string(),
            canonical_id: None,
            service_tier: "standard".to_string(),
            region: region.to_string(),
            min_input_tokens: 0,
            max_input_tokens: None,
            valid_from: rates.effective_date,
            valid_from_source: ValidFromSource::Vendor,
            input_usd_per_1m: rates.input_usd_per_1m,
            output_usd_per_1m: rates.output_usd_per_1m,
            cache_read_usd_per_1m: None,
            cache_write_usd_per_1m: None,
            reasoning_usd_per_1m: None,
            request_usd: None,
            image_input_usd_per_unit: None,
            image_output_usd_per_unit: None,
            audio_input_usd_per_second: None,
            audio_output_usd_per_second: None,
            currency: "USD".to_string(),
            source: SOURCE_NAME.to_string(),
            source_url: None,
        })
        .collect())
}

/// Fetches AWS's public Bedrock price list.
pub struct BedrockSource {
    /// Endpoint to fetch, normally
    /// `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonBedrock/current/index.json`.
    url: String,
    /// Provider names this source is authoritative for.
    owned: Vec<String>,
    /// Hard cap on response size, enforced by [`super::fetch_capped_body`]. A
    /// live capture of this feed was ~15MB; the configured default cap is
    /// deliberately generous relative to that.
    max_response_bytes: usize,
}

impl BedrockSource {
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
impl PriceSource for BedrockSource {
    fn name(&self) -> &'static str {
        SOURCE_NAME
    }

    fn owned_providers(&self) -> Vec<String> {
        self.owned.clone()
    }

    async fn fetch_rates(&self, client: &Client, _now: DateTime<Utc>) -> Result<Vec<RateObservation>> {
        // Bedrock publishes a real effective date (see `sku_rate`), so unlike
        // OpenRouter/LiteLLM this source never falls back to the crawl time.
        let body = super::fetch_capped_body(client, &self.url, self.max_response_bytes).await?;
        let mut rates = parse_bedrock(&body)?;
        for rate in &mut rates {
            rate.source_url = Some(self.url.clone());
        }
        Ok(rates)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::parse_bedrock;
    use crate::pricing::source::ValidFromSource;

    const FIXTURE: &str = include_str!("../../../tests/fixtures/bedrock_price_list.json");

    #[test]
    fn converts_per_1k_units_to_usd_per_million() {
        // "0.0030000000" per 1K tokens is $3.00 per 1M -- the real published
        // rate for Claude 3 Sonnet input on Bedrock.
        let rates = parse_bedrock(FIXTURE).expect("fixture parses");
        let sonnet = rates
            .iter()
            .find(|r| r.model.contains("claude-3-sonnet") && r.region == "us-east-1")
            .expect("sonnet us-east-1 present");
        assert!((sonnet.input_usd_per_1m.unwrap() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn claude_3_sonnet_has_no_output_rate_in_the_live_feed() {
        // A live capture (2026-07-20) has no output-tokens SKU for any
        // Anthropic model in this feed -- only input. Pinning this documents
        // the real shape rather than letting a future edit quietly assume an
        // output SKU exists and start guessing one.
        let rates = parse_bedrock(FIXTURE).expect("fixture parses");
        let sonnet = rates
            .iter()
            .find(|r| r.model.contains("claude-3-sonnet") && r.region == "us-east-1")
            .expect("sonnet us-east-1 present");
        assert!(sonnet.output_usd_per_1m.is_none());
    }

    #[test]
    fn emits_one_row_per_region() {
        // Region is part of the SKU: two regions are two independently-set
        // prices, never collapsed to one `global` row.
        let rates = parse_bedrock(FIXTURE).expect("fixture parses");
        let regions: std::collections::BTreeSet<&str> = rates.iter().map(|r| r.region.as_str()).collect();
        assert!(regions.len() >= 2, "expected rows for more than one region");
        assert!(!regions.contains("global"), "bedrock rows are never global");
    }

    #[test]
    fn merges_input_and_output_skus_into_one_row() {
        // Llama 3 8B (the fixture's non-Claude model) has both directions
        // published in the live feed, unlike the Claude entries -- this is
        // what actually exercises the merge.
        let rates = parse_bedrock(FIXTURE).expect("fixture parses");
        let llama = rates
            .iter()
            .find(|r| r.model == "meta.llama3-8b-instruct-v1:0" && r.region == "us-east-1")
            .expect("llama us-east-1 present");
        assert!((llama.input_usd_per_1m.unwrap() - 0.3).abs() < 1e-9);
        assert!((llama.output_usd_per_1m.unwrap() - 0.6).abs() < 1e-9);
    }

    #[test]
    fn uses_the_published_effective_date() {
        let rates = parse_bedrock(FIXTURE).expect("fixture parses");
        assert!(!rates.is_empty());
        assert!(rates.iter().all(|r| r.valid_from_source == ValidFromSource::Vendor));
        let expected = DateTime::parse_from_rfc3339("2026-07-01T00:00:00Z")
            .expect("valid fixture date")
            .to_utc();
        assert!(rates.iter().all(|r| r.valid_from == expected));
    }

    #[test]
    fn attributes_rows_to_the_aws_bedrock_provider() {
        let rates = parse_bedrock(FIXTURE).expect("fixture parses");
        assert!(rates.iter().all(|r| r.provider == "aws.bedrock"));
        assert!(rates.iter().all(|r| r.source == "bedrock"));
    }

    #[test]
    fn maps_display_names_to_invocation_ids() {
        // "Claude 3 Sonnet" never matches operations.request_model; the row
        // must carry the invocation id telemetry actually emits.
        let rates = parse_bedrock(FIXTURE).expect("fixture parses");
        assert!(rates.iter().any(|r| r.model.starts_with("anthropic.claude-")));
        assert!(!rates.iter().any(|r| r.model.contains(' ')));
    }

    #[test]
    fn skips_an_unmapped_display_name_rather_than_guessing() {
        // A model this source has no curated mapping for must be dropped, not
        // priced under a guessed invocation id.
        let synthetic = r#"{
            "products": {
                "SKU1": {
                    "attributes": {
                        "model": "Some Future Model Nobody Mapped Yet",
                        "feature": "On-demand Inference",
                        "inferenceType": "Input tokens",
                        "regionCode": "us-east-1"
                    }
                }
            },
            "terms": {
                "OnDemand": {
                    "SKU1": {
                        "SKU1.TERM": {
                            "effectiveDate": "2026-07-01T00:00:00Z",
                            "priceDimensions": {
                                "SKU1.TERM.RATE": {
                                    "unit": "1K tokens",
                                    "pricePerUnit": { "USD": "0.0010000000" }
                                }
                            }
                        }
                    }
                }
            }
        }"#;
        let rates = parse_bedrock(synthetic).expect("synthetic payload parses");
        assert!(rates.is_empty());
    }

    #[test]
    fn skips_a_sku_priced_in_a_unit_other_than_1k_tokens() {
        // Regression guard for the real second SKU shape found in a live
        // capture: some Bedrock SKUs already price in "1M tokens". Multiplying
        // one by 1,000 again would be a 1000x overcharge; the SKU must be
        // dropped instead.
        let synthetic = r#"{
            "products": {
                "SKU1": {
                    "attributes": {
                        "model": "Claude 3 Sonnet",
                        "feature": "On-demand Inference",
                        "inferenceType": "Input tokens",
                        "regionCode": "us-east-1"
                    }
                }
            },
            "terms": {
                "OnDemand": {
                    "SKU1": {
                        "SKU1.TERM": {
                            "effectiveDate": "2026-07-01T00:00:00Z",
                            "priceDimensions": {
                                "SKU1.TERM.RATE": {
                                    "unit": "1M tokens",
                                    "pricePerUnit": { "USD": "3.0000000000" }
                                }
                            }
                        }
                    }
                }
            }
        }"#;
        let rates = parse_bedrock(synthetic).expect("synthetic payload parses");
        assert!(rates.is_empty(), "a non-1K-token SKU must be skipped, not scaled");
    }

    #[test]
    fn rejects_a_malformed_payload() {
        // A renamed upstream field must fail loudly here, never yield a NULL
        // rate.
        assert!(parse_bedrock("{\"products\": {\"x\": {}}}").is_err());
    }
}
