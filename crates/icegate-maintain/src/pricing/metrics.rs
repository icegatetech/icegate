//! Crawler instruments recorded to the OpenTelemetry global meter.
//!
//! `last_success_unixtime` is the freshness SLI: "when did we last confirm this
//! rate" is an operational signal, which is why `prices` carries no
//! `observed_at` column.
//!
//! `canonical_missing` deliberately carries **no** `source` label: it is
//! derived once per crawl over the pooled candidates from every source, so a
//! per-source value does not exist to report.

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram};

/// The meter name pricing instruments are registered under.
const METER_NAME: &str = "icegate.pricing";
/// Attribute key carrying the source name (`openrouter`, `litellm`, …).
const SOURCE_KEY: &str = "source";
/// Attribute key carrying a row-reject reason; see
/// [`crate::pricing::guard::RejectReason::as_str`].
const REASON_KEY: &str = "reason";

/// Pricing-crawler instruments, labelled by `source` (and `reason` where relevant).
///
/// Mirrors [`crate::gc::metrics::GcMetrics`]: instruments bind to the global
/// meter at construction and are no-ops until a meter provider is installed,
/// so a `PricingMetrics` can be built eagerly and cloned cheaply (each
/// instrument is an internal `Arc`).
#[derive(Clone)]
pub struct PricingMetrics {
    /// Wall-clock duration of one source's fetch.
    crawl_duration: Histogram<f64>,
    /// Unix timestamp a source last completed a successful fetch and guard pass.
    last_success_unixtime: Gauge<i64>,
    /// Rate rows appended to `icegate.prices` in a commit.
    rates_appended: Counter<u64>,
    /// Rows dropped by a guard, labelled by reason.
    rows_rejected: Counter<u64>,
    /// Models a source had live, after row guards, in the most recent crawl.
    models_live: Gauge<u64>,
    /// Candidates with no derivable `canonical_id` in the most recent crawl.
    canonical_missing: Gauge<u64>,
}

impl PricingMetrics {
    /// Build the instruments against the global meter.
    ///
    /// Instrument construction never fails and records are no-ops until a meter
    /// provider is installed, so this is safe to call eagerly when wiring the
    /// crawl executor.
    #[must_use]
    pub fn new() -> Self {
        let meter = global::meter(METER_NAME);
        let crawl_duration = meter
            .f64_histogram("pricing.crawl.duration")
            .with_description("Wall-clock duration of one source's fetch")
            .with_unit("s")
            .build();
        let last_success_unixtime = meter
            .i64_gauge("pricing.source.last_success_unixtime")
            .with_description("Unix timestamp a source last completed a successful fetch and guard pass")
            .build();
        let rates_appended = meter
            .u64_counter("pricing.rates.appended")
            .with_description("Rate rows appended to icegate.prices in a commit")
            .build();
        let rows_rejected = meter
            .u64_counter("pricing.rows.rejected")
            .with_description("Rows dropped by a guard, labelled by reason")
            .build();
        let models_live = meter
            .u64_gauge("pricing.models.live")
            .with_description("Models a source had live, after row guards, in the most recent crawl")
            .build();
        let canonical_missing = meter
            .u64_gauge("pricing.canonical.missing")
            .with_description("Candidates with no derivable canonical_id in the most recent crawl")
            .build();
        Self {
            crawl_duration,
            last_success_unixtime,
            rates_appended,
            rows_rejected,
            models_live,
            canonical_missing,
        }
    }

    fn source_attrs(source: &str) -> [KeyValue; 1] {
        [KeyValue::new(SOURCE_KEY, source.to_string())]
    }

    fn reason_attrs(source: &str, reason: &str) -> [KeyValue; 2] {
        [
            KeyValue::new(SOURCE_KEY, source.to_string()),
            KeyValue::new(REASON_KEY, reason.to_string()),
        ]
    }

    /// Record the wall-clock duration of one source's fetch, in seconds.
    pub fn record_duration(&self, source: &str, secs: f64) {
        self.crawl_duration.record(secs, &Self::source_attrs(source));
    }

    /// Record that `source` completed a successful fetch and guard pass at
    /// `unix_secs`.
    pub fn record_success(&self, source: &str, unix_secs: i64) {
        self.last_success_unixtime.record(unix_secs, &Self::source_attrs(source));
    }

    /// Record rate rows appended to `icegate.prices` attributed to `source`.
    pub fn record_appended(&self, source: &str, count: u64) {
        self.rates_appended.add(count, &Self::source_attrs(source));
    }

    /// Record rows dropped for `source` with `reason`.
    pub fn record_rejected(&self, source: &str, reason: &str, count: u64) {
        self.rows_rejected.add(count, &Self::reason_attrs(source, reason));
    }

    /// Record the number of models `source` had live after row guards this crawl.
    pub fn record_models_live(&self, source: &str, count: u64) {
        self.models_live.record(count, &Self::source_attrs(source));
    }

    /// Record the number of candidates with no derivable `canonical_id` this crawl.
    ///
    /// Crawl-wide, not per source; see the module doc for why it carries no
    /// `source` label.
    pub fn record_canonical_missing(&self, count: u64) {
        self.canonical_missing.record(count, &[]);
    }
}

impl Default for PricingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::PricingMetrics;

    #[test]
    fn pricing_metrics_new_records_without_panicking() {
        let metrics = PricingMetrics::new();
        metrics.record_duration("litellm", 1.5);
        metrics.record_success("litellm", 1_760_000_000);
        metrics.record_appended("litellm", 3);
        metrics.record_rejected("litellm", "cardinality_floor", 1);
        metrics.record_models_live("litellm", 900);
        metrics.record_canonical_missing(2);
    }
}
