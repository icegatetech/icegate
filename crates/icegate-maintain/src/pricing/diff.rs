//! What to append: the difference between the live rate card and a fresh crawl.

use std::collections::HashMap;

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::pricing::config::PricingConfig;
use crate::pricing::guard::{RejectReason, check_delta};
use crate::pricing::source::{RateKey, RateObservation, ValidFromSource};

/// The rate currently in effect for each key.
#[derive(Debug, Default)]
pub struct LiveRates {
    rows: HashMap<RateKey, RateObservation>,
}

impl LiveRates {
    /// Build from the latest row per key, as read from `icegate.prices`.
    ///
    /// The table holds the full revision history, so several rows can share a
    /// key; the one with the greatest `valid_from` wins, because that is the
    /// revision currently in effect.
    #[must_use]
    pub fn from_rows(rows: Vec<RateObservation>) -> Self {
        let mut latest: HashMap<RateKey, RateObservation> = HashMap::new();
        for row in rows {
            let key = row.key();
            match latest.get(&key) {
                Some(existing) if existing.valid_from >= row.valid_from => {}
                _ => {
                    latest.insert(key, row);
                }
            }
        }
        Self { rows: latest }
    }

    /// The rate currently in effect for `key`, if any.
    #[must_use]
    pub fn get(&self, key: &RateKey) -> Option<&RateObservation> {
        self.rows.get(key)
    }

    /// How many keys are live for `source`.
    ///
    /// Every value in `rows` is already the newest revision of its key (see
    /// [`Self::from_rows`]), so this counts distinct keys, not raw table rows;
    /// guard 5 (a later task) divides its fetched count by this number and
    /// would be miscalibrated by a per-revision count.
    #[must_use]
    pub fn count_for_source(&self, source: &str) -> usize {
        self.rows.values().filter(|row| row.source == source).count()
    }

    /// Whether any rate is live at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Rows to append, and rows the delta guard refused.
#[derive(Debug, Default)]
pub struct DiffOutcome {
    /// Rates that are new or changed and passed the delta guard.
    pub to_append: Vec<RateObservation>,
    /// Rates blocked by the delta guard, paired with why.
    pub rejected: Vec<(RateObservation, RejectReason)>,
}

/// The four rate columns the delta guard compares.
///
/// Narrower than the ten columns `guard::apply_row_guards` sanity-checks: only
/// these four carry the token-volume-driven magnitudes a vendor revises in the
/// ordinary course of business, so a 10x move here is the meaningful signal of
/// a mispriced or misparsed row. `reasoning_usd_per_1m` moves in lockstep with
/// `output_usd_per_1m` on the providers that bill it, `request_usd`/image/audio
/// columns are rare, low-magnitude, or provider-specific enough that a
/// percentage-ratio comparison against them is noisy rather than protective,
/// and `max_input_tokens`/`currency` are not prices at all — they are already
/// covered by [`RateObservation::rates_differ`] and the row guards. See the
/// task report for the case that `request_usd` might deserve the same
/// treatment.
const fn rate_columns_to_check(rate: &RateObservation) -> [Option<f64>; 4] {
    [
        rate.input_usd_per_1m,
        rate.output_usd_per_1m,
        rate.cache_read_usd_per_1m,
        rate.cache_write_usd_per_1m,
    ]
}

/// Force `candidate` to supersede `current` by making its `valid_from` strictly
/// later.
///
/// [`LiveRates::from_rows`] resolves a key to its greatest `valid_from` and
/// breaks ties in favour of the row it saw first, so a revision that does not
/// advance the date would never take effect — and, still differing from the row
/// that does, would be re-appended on every subsequent crawl. That is reachable
/// today: the Bedrock source carries AWS's published `effectiveDate`, which a
/// live capture showed as one constant value for the whole file, so a rate can
/// change underneath a date that does not move.
///
/// A vendor date that cannot order the revision is not usable as its effective
/// date, so the row falls back to the crawl time and is relabelled
/// [`ValidFromSource::Observed`] — which is exactly what that variant means.
fn advance_valid_from(candidate: &mut RateObservation, current: &RateObservation, now: DateTime<Utc>) {
    if candidate.valid_from > current.valid_from {
        return;
    }
    // `prices.valid_from` is microsecond-precision, so +1us is the smallest
    // representable step past the superseded row.
    let earliest_usable = current
        .valid_from
        .checked_add_signed(ChronoDuration::microseconds(1))
        .unwrap_or(current.valid_from);
    candidate.valid_from = now.max(earliest_usable);
    candidate.valid_from_source = ValidFromSource::Observed;
}

/// Compare a fresh crawl against the live rate card and decide what to append.
///
/// Order of decisions per key: absent from `live` appends unconditionally
/// (nothing to compare, and a first sighting has no meaningful delta);
/// identical rates skip silently (the steady state); only a genuine price
/// change reaches the delta guard, so a row whose rates are unchanged never
/// counts against guard 6. A row that survives the guard also has its
/// `valid_from` forced past the revision it supersedes (see
/// [`advance_valid_from`]).
///
/// A key absent from `fetched` produces nothing: there is no tombstone in an
/// append-only log, so a retired model attracts no new spans, and a
/// transiently truncated upstream response is a no-op rather than a mass
/// retirement.
#[must_use]
pub fn diff_rates(
    fetched: Vec<RateObservation>,
    live: &LiveRates,
    config: &PricingConfig,
    now: DateTime<Utc>,
) -> DiffOutcome {
    let mut outcome = DiffOutcome::default();
    for mut candidate in fetched {
        let Some(current) = live.get(&candidate.key()) else {
            outcome.to_append.push(candidate);
            continue;
        };
        if !current.rates_differ(&candidate) {
            continue;
        }
        let violation = rate_columns_to_check(current)
            .into_iter()
            .zip(rate_columns_to_check(&candidate))
            .find_map(|(before, after)| check_delta(after, before, config).err());
        match violation {
            Some(reason) => {
                tracing::error!(
                    provider = %candidate.provider,
                    model = %candidate.model,
                    region = %candidate.region,
                    source = %candidate.source,
                    "pricing rate moved beyond max_change_ratio; refusing the row. \
                     Verify against the vendor, then raise pricing.max_change_ratio or set it to 0"
                );
                outcome.rejected.push((candidate, reason));
            }
            None => {
                advance_valid_from(&mut candidate, current, now);
                outcome.to_append.push(candidate);
            }
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use chrono::DateTime;

    use super::{LiveRates, diff_rates};
    use crate::pricing::config::PricingConfig;
    use crate::pricing::guard::RejectReason;
    use crate::pricing::source::{RateObservation, ValidFromSource};

    /// A crawl time later than every fixture's `valid_from`, so `diff_rates`
    /// can always advance a revision past the row it supersedes.
    fn now() -> chrono::DateTime<chrono::Utc> {
        DateTime::from_timestamp(1_800_000_000, 0).expect("valid timestamp")
    }

    fn rate(model: &str, input: f64) -> RateObservation {
        RateObservation {
            provider: "anthropic".to_string(),
            model: model.to_string(),
            canonical_id: None,
            service_tier: "standard".to_string(),
            region: "global".to_string(),
            min_input_tokens: 0,
            max_input_tokens: None,
            valid_from: DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp"),
            valid_from_source: ValidFromSource::Observed,
            input_usd_per_1m: Some(input),
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

    fn live_with(rates: Vec<RateObservation>) -> LiveRates {
        LiveRates::from_rows(rates)
    }

    #[test]
    fn unchanged_rates_append_nothing() {
        // The steady state: most crawls must produce an empty commit.
        let live = live_with(vec![rate("claude-opus-4-8", 5.0)]);
        let outcome = diff_rates(
            vec![rate("claude-opus-4-8", 5.0)],
            &live,
            &PricingConfig::default(),
            now(),
        );
        assert!(outcome.to_append.is_empty());
    }

    #[test]
    fn a_changed_rate_appends_one_row() {
        let live = live_with(vec![rate("claude-opus-4-8", 5.0)]);
        let outcome = diff_rates(
            vec![rate("claude-opus-4-8", 6.0)],
            &live,
            &PricingConfig::default(),
            now(),
        );
        assert_eq!(outcome.to_append.len(), 1);
        assert_eq!(outcome.to_append[0].input_usd_per_1m, Some(6.0));
    }

    #[test]
    fn a_new_model_appends_one_row() {
        let live = live_with(vec![]);
        let outcome = diff_rates(
            vec![rate("claude-opus-4-8", 5.0)],
            &live,
            &PricingConfig::default(),
            now(),
        );
        assert_eq!(outcome.to_append.len(), 1);
    }

    #[test]
    fn a_disappeared_model_appends_nothing() {
        // No tombstone by design: in an effective-dated log, no end date means
        // "still in effect", and a retired model attracts no new spans. This also
        // makes a transiently truncated response a no-op rather than a retirement.
        let live = live_with(vec![rate("claude-opus-4-8", 5.0)]);
        let outcome = diff_rates(vec![], &live, &PricingConfig::default(), now());
        assert!(outcome.to_append.is_empty());
    }

    #[test]
    fn a_delta_violation_is_rejected_not_appended() {
        let live = live_with(vec![rate("claude-opus-4-8", 5.0)]);
        let outcome = diff_rates(
            vec![rate("claude-opus-4-8", 500.0)],
            &live,
            &PricingConfig::default(),
            now(),
        );
        assert!(outcome.to_append.is_empty());
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].1, RejectReason::DeltaTooLarge);
    }

    #[test]
    fn a_new_model_bypasses_the_delta_guard() {
        // Nothing to compare against, so the first sighting is always admitted.
        let live = live_with(vec![]);
        let outcome = diff_rates(
            vec![rate("brand-new", 9_000.0)],
            &live,
            &PricingConfig::default(),
            now(),
        );
        assert_eq!(outcome.to_append.len(), 1);
    }

    #[test]
    fn from_rows_keeps_the_newest_revision_regardless_of_input_order() {
        // The table holds full revision history, so `from_rows` sees the same key
        // more than once. Only the greatest `valid_from` is "in effect" — and that
        // must hold no matter which order the rows arrive in, since a catalog scan
        // makes no ordering guarantee.
        let earlier = RateObservation {
            valid_from: DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp"),
            ..rate("claude-opus-4-8", 5.0)
        };
        let later = RateObservation {
            valid_from: DateTime::from_timestamp(1_770_000_000, 0).expect("valid timestamp"),
            ..rate("claude-opus-4-8", 6.0)
        };

        let forward = LiveRates::from_rows(vec![earlier.clone(), later.clone()]);
        let reversed = LiveRates::from_rows(vec![later.clone(), earlier]);

        for live in [forward, reversed] {
            let current = live.get(&later.key()).expect("key present");
            assert_eq!(
                current.input_usd_per_1m,
                Some(6.0),
                "the row with the greatest valid_from must win regardless of input order"
            );
        }
    }

    #[test]
    fn a_rejected_row_is_not_silently_dropped() {
        // The runner needs the observation itself, not just a count, to log which
        // model/region/source was refused — a bug that kept the reason but
        // dropped the row (or vice versa) would pass a to_append-only assertion.
        let live = live_with(vec![rate("claude-opus-4-8", 5.0)]);
        let candidate = rate("claude-opus-4-8", 500.0);
        let outcome = diff_rates(vec![candidate.clone()], &live, &PricingConfig::default(), now());
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].0, candidate);
        assert_eq!(outcome.rejected[0].1, RejectReason::DeltaTooLarge);
    }

    #[test]
    fn a_revision_whose_valid_from_does_not_advance_is_forced_past_the_live_row() {
        // Bedrock carries AWS's published `effectiveDate`, which a live capture
        // showed as one constant value for the whole file, so a rate can change
        // underneath a date that does not move. Left alone, `LiveRates` would
        // keep the older row (equal `valid_from`, first-seen wins) and this row
        // would be re-appended on every crawl forever.
        let live_row = RateObservation {
            valid_from: now(),
            valid_from_source: ValidFromSource::Vendor,
            ..rate("claude-opus-4-8", 5.0)
        };
        let stale_dated = RateObservation {
            valid_from: now(),
            valid_from_source: ValidFromSource::Vendor,
            ..rate("claude-opus-4-8", 6.0)
        };

        let live = live_with(vec![live_row.clone()]);
        let outcome = diff_rates(vec![stale_dated], &live, &PricingConfig::default(), now());

        assert_eq!(outcome.to_append.len(), 1);
        let appended = &outcome.to_append[0];
        assert!(
            appended.valid_from > live_row.valid_from,
            "the revision must strictly supersede the row it replaces"
        );
        assert_eq!(
            appended.valid_from_source,
            ValidFromSource::Observed,
            "a vendor date that cannot order the revision is relabelled as observed"
        );

        // And the reduction now resolves to the new rate, so the next crawl is
        // a no-op instead of re-appending.
        let next = LiveRates::from_rows(vec![live_row, appended.clone()]);
        assert_eq!(
            next.get(&appended.key()).expect("key present").input_usd_per_1m,
            Some(6.0)
        );
    }

    #[test]
    fn a_revision_with_a_later_vendor_date_keeps_it() {
        // The fallback must not fire when the vendor date already orders the
        // revision: that date is better provenance than the crawl time.
        let live = live_with(vec![rate("claude-opus-4-8", 5.0)]);
        let vendor_dated = RateObservation {
            valid_from: DateTime::from_timestamp(1_790_000_000, 0).expect("valid timestamp"),
            valid_from_source: ValidFromSource::Vendor,
            ..rate("claude-opus-4-8", 6.0)
        };
        let outcome = diff_rates(vec![vendor_dated.clone()], &live, &PricingConfig::default(), now());
        assert_eq!(outcome.to_append[0].valid_from, vendor_dated.valid_from);
        assert_eq!(outcome.to_append[0].valid_from_source, ValidFromSource::Vendor);
    }

    #[test]
    fn count_for_source_counts_distinct_keys_not_rows() {
        // `from_rows` already dedupes to the newest row per key, so a source with
        // several historical revisions of the same key must count as one live
        // key, not one per revision. Guard 5 (a later task) divides the fetched
        // count by this number, so an inflated count would silently raise the
        // cardinality floor.
        let earlier = RateObservation {
            valid_from: DateTime::from_timestamp(1_760_000_000, 0).expect("valid timestamp"),
            ..rate("claude-opus-4-8", 5.0)
        };
        let later = RateObservation {
            valid_from: DateTime::from_timestamp(1_770_000_000, 0).expect("valid timestamp"),
            ..rate("claude-opus-4-8", 6.0)
        };
        let live = LiveRates::from_rows(vec![earlier, later]);
        assert_eq!(live.count_for_source("litellm"), 1);
    }
}
