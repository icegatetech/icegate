//! `OpenTelemetry` instruments for WAL segment cleanup.
//!
//! Mirrors [`crate::gc::metrics`]: instruments bind to the global meter at
//! construction and are no-ops until a meter provider is installed, so a
//! `WalCleanupMetrics` can be built eagerly and cloned cheaply.

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};

/// The meter name WAL-cleanup instruments are registered under.
const METER_NAME: &str = "icegate.wal_cleanup";
/// Attribute key carrying the WAL topic name.
const TOPIC_KEY: &str = "topic";

/// Cleanup instruments, labelled by `topic`.
#[derive(Clone)]
pub struct WalCleanupMetrics {
    /// Segments identified as being at or below the delete bound.
    segments_found: Counter<u64>,
    /// Segments successfully deleted.
    segments_deleted: Counter<u64>,
    /// Segments whose deletion failed (retried next cycle).
    segments_delete_failed: Counter<u64>,
    /// Bytes reclaimed by deleting segments.
    bytes_reclaimed: Counter<u64>,
    /// Cycles that hit `max_deletes_per_cycle` and left segments behind.
    cycles_truncated: Counter<u64>,
    /// Cycles that deleted nothing because no delete bound could be derived.
    cycles_without_bound: Counter<u64>,
    /// Cycles that left the tail below the delete bound in place.
    cycles_stalled: Counter<u64>,
    /// Delete bound the cycle resolved, as a WAL offset.
    delete_bound: Histogram<u64>,
    /// Wall-clock duration of one topic's cleanup cycle.
    cleanup_duration: Histogram<f64>,
}

impl WalCleanupMetrics {
    /// Build the instruments against the global meter.
    #[must_use]
    pub fn new() -> Self {
        let meter = global::meter(METER_NAME);
        Self {
            segments_found: meter
                .u64_counter("wal_cleanup.segments.found")
                .with_description("WAL segments identified as being at or below the delete bound")
                .build(),
            segments_deleted: meter
                .u64_counter("wal_cleanup.segments.deleted")
                .with_description("WAL segments successfully deleted")
                .build(),
            segments_delete_failed: meter
                .u64_counter("wal_cleanup.segments.delete_failed")
                .with_description("WAL segments whose deletion failed (retried next cycle)")
                .build(),
            bytes_reclaimed: meter
                .u64_counter("wal_cleanup.bytes.reclaimed")
                .with_description("Bytes reclaimed by deleting WAL segments")
                .with_unit("By")
                .build(),
            cycles_truncated: meter
                .u64_counter("wal_cleanup.cycles.truncated")
                .with_description("Cleanup cycles cut short by max_deletes_per_cycle")
                .build(),
            cycles_without_bound: meter
                .u64_counter("wal_cleanup.cycles.without_bound")
                .with_description("Cleanup cycles that deleted nothing because no delete bound could be derived")
                .build(),
            cycles_stalled: meter
                .u64_counter("wal_cleanup.cycles.stalled")
                .with_description(
                    "Cleanup cycles that left segments at or below the delete bound in place: nothing was \
                     reclaimed, or the store refused an unbroken run of deletions",
                )
                .build(),
            delete_bound: meter
                .u64_histogram("wal_cleanup.delete_bound")
                .with_description("WAL offset up to and including which a cycle deleted")
                .build(),
            cleanup_duration: meter
                .f64_histogram("wal_cleanup.duration")
                .with_description("Wall-clock duration of one topic's cleanup cycle")
                .with_unit("s")
                .build(),
        }
    }

    fn topic_attrs(topic: &str) -> [KeyValue; 1] {
        [KeyValue::new(TOPIC_KEY, topic.to_string())]
    }

    /// Record the outcome counters of one completed cycle.
    pub fn record_cycle(&self, topic: &str, summary: &icegate_queue::DeleteSummary) {
        let attrs = Self::topic_attrs(topic);
        self.segments_found.add(summary.found, &attrs);
        self.segments_deleted.add(summary.deleted, &attrs);
        self.segments_delete_failed.add(summary.failed, &attrs);
        self.bytes_reclaimed.add(summary.bytes_reclaimed, &attrs);
        if summary.truncated {
            self.cycles_truncated.add(1, &attrs);
        }
    }

    /// Record the delete bound a cycle resolved.
    pub fn record_delete_bound(&self, topic: &str, bound: u64) {
        self.delete_bound.record(bound, &Self::topic_attrs(topic));
    }

    /// Record a cycle that derived no bound and therefore deleted nothing.
    pub fn record_cycle_without_bound(&self, topic: &str) {
        self.cycles_without_bound.add(1, &Self::topic_attrs(topic));
    }

    /// Record a cycle that left the tail below the bound in place.
    ///
    /// Separate from [`Self::record_cycle_without_bound`] because the two are
    /// opposite verdicts: no bound is the normal state of a young table, while a
    /// bound whose tail survives the cycle means the store is refusing the
    /// deletes and the queue is growing.
    pub fn record_stalled_cycle(&self, topic: &str) {
        self.cycles_stalled.add(1, &Self::topic_attrs(topic));
    }

    /// Record the wall-clock duration of one cycle, in seconds.
    pub fn record_duration(&self, topic: &str, secs: f64) {
        self.cleanup_duration.record(secs, &Self::topic_attrs(topic));
    }
}

impl Default for WalCleanupMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use icegate_queue::DeleteSummary;

    use super::WalCleanupMetrics;

    #[test]
    fn wal_cleanup_metrics_new_records_without_panicking() {
        let metrics = WalCleanupMetrics::new();
        metrics.record_cycle(
            "logs",
            &DeleteSummary {
                found: 10,
                deleted: 9,
                failed: 1,
                bytes_reclaimed: 4096,
                truncated: true,
                is_abandoned: false,
                lowest_remaining: Some(11),
            },
        );
        metrics.record_delete_bound("logs", 10);
        metrics.record_cycle_without_bound("spans");
        metrics.record_stalled_cycle("logs");
        metrics.record_duration("logs", 1.5);
    }
}
