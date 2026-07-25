//! Sanity checks applied between fetching and committing.
//!
//! Direct OpenAI and Anthropic rates come from a community-maintained source, so
//! for the two largest providers these guards are the only check that stands
//! between an upstream edit and a wrong dollar figure. They are blocking, not
//! advisory.

use crate::pricing::config::PricingConfig;
use crate::pricing::decimal::carries_subgrid_precision;
use crate::pricing::source::RateObservation;

/// Absolute ceiling applied to every rate column. No real model approaches this;
/// a value above it means a unit-conversion error upstream or in a parser.
///
/// One ceiling covers all ten columns even though they are not all the same
/// unit: `audio_*_usd_per_second` and `image_*_usd_per_unit`/`request_usd` are
/// priced per second or per unit, not per 1M tokens like the rest. A real value
/// in any of those units is orders of magnitude below this bound, so the shared
/// ceiling still catches the failure mode it exists for (a stray `* 1_000_000`
/// or misplaced decimal) without needing a per-column threshold.
const RATE_CEILING_USD_PER_1M: f64 = 10_000.0;

/// Why a row or a source was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// A rate was `NaN`, infinite, or negative.
    NotFinite,
    /// A rate exceeded [`RATE_CEILING_USD_PER_1M`].
    AboveCeiling,
    /// No priced column was populated.
    NoRates,
    /// Currency other than USD.
    NonUsdCurrency,
    /// The source returned too few models against its live count.
    CardinalityFloor,
    /// A rate moved by more than the configured ratio.
    DeltaTooLarge,
    /// A rate carries more precision than the storage grid can hold, so storing
    /// it would silently drop meaningful digits.
    PrecisionTooFine,
}

impl RejectReason {
    /// Stable label for the `rows_rejected_total{reason}` metric.
    ///
    /// These strings are an external contract: a later task exports them as
    /// Prometheus label values, so renaming one silently breaks dashboards and
    /// alerts built against the old name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotFinite => "not_finite",
            Self::AboveCeiling => "above_ceiling",
            Self::NoRates => "no_rates",
            Self::NonUsdCurrency => "non_usd_currency",
            Self::CardinalityFloor => "cardinality_floor",
            Self::DeltaTooLarge => "delta_too_large",
            Self::PrecisionTooFine => "precision_too_fine",
        }
    }
}

/// Result of running the row-level guards over a batch.
#[derive(Debug, Default)]
pub struct GuardOutcome {
    /// Rows that passed every row-level guard.
    pub accepted: Vec<RateObservation>,
    /// Rows that were dropped, with the reason.
    pub rejected: Vec<(RateObservation, RejectReason)>,
}

/// Every USD rate carried by an observation, whatever its unit.
const fn rate_columns(rate: &RateObservation) -> [Option<f64>; 10] {
    [
        rate.input_usd_per_1m,
        rate.output_usd_per_1m,
        rate.cache_read_usd_per_1m,
        rate.cache_write_usd_per_1m,
        rate.reasoning_usd_per_1m,
        rate.request_usd,
        rate.image_input_usd_per_unit,
        rate.image_output_usd_per_unit,
        rate.audio_input_usd_per_second,
        rate.audio_output_usd_per_second,
    ]
}

/// Classify one observation against guards 3, 4, and 7.
///
/// Currency is checked before the all-none check, so a row that is both
/// non-USD and rateless reports [`RejectReason::NonUsdCurrency`] rather than
/// [`RejectReason::NoRates`]. See the ordering test below and the task report
/// for why that is the more useful diagnostic.
fn classify(rate: &RateObservation) -> Option<RejectReason> {
    let columns = rate_columns(rate);
    if columns.iter().flatten().any(|v| !v.is_finite() || *v < 0.0) {
        return Some(RejectReason::NotFinite);
    }
    if columns.iter().flatten().any(|v| *v > RATE_CEILING_USD_PER_1M) {
        return Some(RejectReason::AboveCeiling);
    }
    if rate.currency != "USD" {
        return Some(RejectReason::NonUsdCurrency);
    }
    if columns.iter().all(Option::is_none) {
        return Some(RejectReason::NoRates);
    }
    // Reject rather than silently round: a rate finer than the storage grid
    // cannot be stored without dropping digits, and quantization would do that
    // silently. The row is dropped and counted like any other guard failure; the
    // affected spans surface as unpriced instead of mispriced. See
    // `decimal::carries_subgrid_precision`.
    if columns.iter().flatten().any(|v| carries_subgrid_precision(*v)) {
        return Some(RejectReason::PrecisionTooFine);
    }
    None
}

/// Apply guards 3, 4, and 7 to a batch of observations.
///
/// Takes no config: these three guards are absolute (finite, non-negative,
/// below [`RATE_CEILING_USD_PER_1M`], USD, at least one rate). The tunable
/// guards are [`check_cardinality`] and [`check_delta`], which need
/// source-level and series-level comparison and take the config themselves.
#[must_use]
pub fn apply_row_guards(rates: Vec<RateObservation>) -> GuardOutcome {
    let mut outcome = GuardOutcome::default();
    for rate in rates {
        match classify(&rate) {
            Some(reason) => outcome.rejected.push((rate, reason)),
            None => outcome.accepted.push(rate),
        }
    }
    outcome
}

/// Guard 5: reject a source that returned far fewer models than it has live.
///
/// `live_count` of zero admits everything — the bootstrap crawl has nothing to
/// compare against. That gap is real and deliberate; the runner logs a WARN
/// summary so an operator can spot-check the first crawl.
///
/// # Errors
///
/// Returns [`RejectReason::CardinalityFloor`] when `fetched_count` falls below
/// `min_model_count_ratio × live_count`.
pub fn check_cardinality(
    source: &str,
    fetched_count: usize,
    live_count: usize,
    config: &PricingConfig,
) -> Result<(), RejectReason> {
    if live_count == 0 {
        return Ok(());
    }
    // `usize -> f64` loses precision only above 2^53 models, many orders of
    // magnitude past any real rate card; the ratio comparison below is exact
    // for every count this function will ever see.
    #[allow(clippy::cast_precision_loss)]
    let floor = live_count as f64 * config.min_model_count_ratio;
    #[allow(clippy::cast_precision_loss)]
    let fetched = fetched_count as f64;
    if fetched < floor {
        tracing::error!(
            source,
            fetched_count,
            live_count,
            floor,
            "pricing source returned too few models; refusing to commit its rows"
        );
        return Err(RejectReason::CardinalityFloor);
    }
    Ok(())
}

/// Guard 6: reject a rate that moved by more than `max_change_ratio`.
///
/// Blocks legitimate large price cuts by design: on a money path a move that
/// big should require a human to raise the threshold or set `max_change_ratio`
/// to `0`, not land silently overnight. Transitions involving zero or a missing
/// side have no meaningful ratio and are always allowed.
///
/// # Errors
///
/// Returns [`RejectReason::DeltaTooLarge`] when the ratio is exceeded.
pub fn check_delta(fetched: Option<f64>, live: Option<f64>, config: &PricingConfig) -> Result<(), RejectReason> {
    if config.max_change_ratio <= 0.0 {
        return Ok(());
    }
    let (Some(fetched), Some(live)) = (fetched, live) else {
        return Ok(());
    };
    if fetched == 0.0 || live == 0.0 {
        return Ok(());
    }
    let ratio = if fetched > live { fetched / live } else { live / fetched };
    if ratio > config.max_change_ratio {
        return Err(RejectReason::DeltaTooLarge);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    // `field_reassign_with_default` is allowed here for the same reason as in
    // `pricing::config::tests`: these tests build a `PricingConfig::default()`
    // and then override only the field(s) under test.
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::field_reassign_with_default)]

    use chrono::DateTime;

    use super::{RejectReason, apply_row_guards, check_cardinality, check_delta};
    use crate::pricing::config::PricingConfig;
    use crate::pricing::source::{RateObservation, ValidFromSource};

    fn observation(input: Option<f64>, currency: &str) -> RateObservation {
        RateObservation {
            provider: "anthropic".to_string(),
            model: "claude-opus-4-8".to_string(),
            canonical_id: None,
            service_tier: "standard".to_string(),
            region: "global".to_string(),
            min_input_tokens: 0,
            max_input_tokens: None,
            valid_from: DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp"),
            valid_from_source: ValidFromSource::Observed,
            input_usd_per_1m: input,
            output_usd_per_1m: Some(25.0),
            cache_read_usd_per_1m: None,
            cache_write_usd_per_1m: None,
            reasoning_usd_per_1m: None,
            request_usd: None,
            image_input_usd_per_unit: None,
            image_output_usd_per_unit: None,
            audio_input_usd_per_second: None,
            audio_output_usd_per_second: None,
            currency: currency.to_string(),
            source: "litellm".to_string(),
            source_url: None,
        }
    }

    #[test]
    fn rejects_negative_and_non_finite_rates() {
        let outcome = apply_row_guards(vec![
            observation(Some(-1.0), "USD"),
            observation(Some(f64::NAN), "USD"),
            observation(Some(f64::INFINITY), "USD"),
        ]);
        assert!(outcome.accepted.is_empty());
        assert_eq!(outcome.rejected.len(), 3);
        assert!(outcome.rejected.iter().all(|(_, r)| *r == RejectReason::NotFinite));
    }

    #[test]
    fn rejects_an_absurd_rate() {
        let outcome = apply_row_guards(vec![observation(Some(50_000.0), "USD")]);
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].1, RejectReason::AboveCeiling);
    }

    #[test]
    fn rejects_an_absurd_rate_in_a_column_other_than_the_first() {
        // `rejects_an_absurd_rate` only ever puts the bad value in
        // `input_usd_per_1m`, the first column `rate_columns` lists. That alone
        // cannot distinguish a real per-column scan from a broken implementation
        // that only ever looks at that one field. Put the absurd value in the
        // *last* column instead, with every other column at an ordinary price.
        let mut bad = observation(Some(5.0), "USD");
        bad.audio_output_usd_per_second = Some(50_000.0);
        let outcome = apply_row_guards(vec![bad]);
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].1, RejectReason::AboveCeiling);
    }

    #[test]
    fn rejects_non_usd() {
        let outcome = apply_row_guards(vec![observation(Some(3.0), "EUR")]);
        assert_eq!(outcome.rejected[0].1, RejectReason::NonUsdCurrency);
    }

    #[test]
    fn rejects_a_row_with_no_rates_at_all() {
        let mut bare = observation(None, "USD");
        bare.output_usd_per_1m = None;
        let outcome = apply_row_guards(vec![bare]);
        assert_eq!(outcome.rejected[0].1, RejectReason::NoRates);
    }

    #[test]
    fn non_usd_currency_wins_over_no_rates_for_a_row_that_is_both() {
        // A row that is both non-USD and rateless is ambiguous: which single
        // reason should the metric carry? `classify` checks currency first, so
        // this pins that choice as the contract rather than an accident of
        // statement order. See the task report for why NonUsdCurrency is the
        // more useful diagnostic here: a rateless USD row is unremarkable (the
        // source simply didn't quote every column), but a non-USD row is the
        // one that needs a human decision (add currency conversion, or drop the
        // source) independent of whether it happens to carry any rates.
        let mut both = observation(None, "EUR");
        both.output_usd_per_1m = None;
        let outcome = apply_row_guards(vec![both]);
        assert_eq!(outcome.rejected[0].1, RejectReason::NonUsdCurrency);
    }

    #[test]
    fn accepts_a_well_formed_row() {
        let outcome = apply_row_guards(vec![observation(Some(5.0), "USD")]);
        assert_eq!(outcome.accepted.len(), 1);
        assert!(outcome.rejected.is_empty());
    }

    #[test]
    fn rejects_a_rate_finer_than_the_storage_grid() {
        // A rate with an 11th fractional digit cannot be stored at scale 10.
        // Rather than silently rounding it, the guard drops the row so the
        // affected spans surface as unpriced, not mispriced.
        let outcome = apply_row_guards(vec![observation(Some(3.000_000_000_05), "USD")]);
        assert!(outcome.accepted.is_empty());
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].1, RejectReason::PrecisionTooFine);
    }

    #[test]
    fn accepts_a_grid_representable_rate_with_float_noise() {
        // A clean rate carrying only f64 noise is on the grid and must pass —
        // the precision guard must not false-reject legitimate rates.
        let outcome = apply_row_guards(vec![observation(Some(3.000_000_000_000_000_4), "USD")]);
        assert_eq!(outcome.accepted.len(), 1);
        assert!(outcome.rejected.is_empty());
    }

    #[test]
    fn cardinality_floor_catches_a_truncated_response() {
        let config = PricingConfig::default(); // ratio 0.8
        assert!(check_cardinality("litellm", 500, 1000, &config).is_err());
        assert!(check_cardinality("litellm", 900, 1000, &config).is_ok());
    }

    #[test]
    fn cardinality_floor_admits_the_bootstrap_crawl() {
        // With no live rows there is nothing to compare against. This is the
        // documented unguarded-bootstrap gap, not an oversight — the runner logs
        // a WARN summary for it (see PricingRunner).
        let config = PricingConfig::default();
        assert!(check_cardinality("litellm", 1, 0, &config).is_ok());
    }

    #[test]
    fn delta_guard_blocks_a_ten_times_move() {
        let config = PricingConfig::default(); // max_change_ratio 10.0
        assert!(check_delta(Some(100.0), Some(3.0), &config).is_err());
        assert!(check_delta(Some(3.3), Some(3.0), &config).is_ok());
    }

    #[test]
    fn delta_guard_blocks_a_ten_times_move_in_either_direction() {
        // `delta_guard_blocks_a_ten_times_move` only exercises fetched > live. A
        // ratio computed as a plain `fetched / live` (rather than the larger
        // divided by the smaller) would silently admit a symmetric price crash —
        // exactly the case guard 6 exists to catch per the task brief ("Guard 6
        // blocks legitimate large price cuts"). Assert the fall is blocked too.
        let config = PricingConfig::default();
        assert!(
            check_delta(Some(100.0), Some(3.0), &config).is_err(),
            "10x rise must be blocked"
        );
        assert!(
            check_delta(Some(3.0), Some(100.0), &config).is_err(),
            "10x fall must be blocked"
        );
    }

    #[test]
    fn delta_guard_can_be_disabled() {
        let mut config = PricingConfig::default();
        config.max_change_ratio = 0.0;
        assert!(check_delta(Some(100.0), Some(3.0), &config).is_ok());
    }

    #[test]
    fn delta_guard_ignores_transitions_involving_zero() {
        // A model going free (or leaving free) is a real, legitimate event and
        // has no meaningful ratio.
        let config = PricingConfig::default();
        assert!(check_delta(Some(3.0), Some(0.0), &config).is_ok());
        assert!(check_delta(Some(0.0), Some(3.0), &config).is_ok());
        assert!(check_delta(Some(3.0), None, &config).is_ok());
    }

    #[test]
    fn reject_reason_labels_are_stable() {
        // These strings become `rows_rejected_total{reason}` label values in a
        // later task. Pin all six the way `ValidFromSource::as_str` is pinned in
        // `source::tests` — a rename here silently breaks a dashboard or alert
        // built against the old label.
        assert_eq!(RejectReason::NotFinite.as_str(), "not_finite");
        assert_eq!(RejectReason::AboveCeiling.as_str(), "above_ceiling");
        assert_eq!(RejectReason::NoRates.as_str(), "no_rates");
        assert_eq!(RejectReason::NonUsdCurrency.as_str(), "non_usd_currency");
        assert_eq!(RejectReason::CardinalityFloor.as_str(), "cardinality_floor");
        assert_eq!(RejectReason::DeltaTooLarge.as_str(), "delta_too_large");
    }
}
