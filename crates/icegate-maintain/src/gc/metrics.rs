//! OpenTelemetry instruments for the orphan-file sweep.
//!
//! Mirrors [`crate::compact::metrics`]: instruments bind to the global meter at
//! construction and are no-ops until a meter provider is installed, so a
//! `GcMetrics` can be built eagerly and cloned cheaply (each instrument is an
//! internal `Arc`).

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};

use crate::gc::decide::ObjectClass;

/// The meter name GC instruments are registered under.
const METER_NAME: &str = "icegate.gc";
/// Attribute key carrying the Iceberg table name.
const TABLE_KEY: &str = "table";
/// Attribute key carrying the object class (`data` or `metadata`).
const KIND_KEY: &str = "kind";

/// Orphan-sweep instruments, labelled by `table` (and `kind` where relevant).
#[derive(Clone)]
pub struct GcMetrics {
    /// Objects listed under a table prefix during a sweep.
    objects_scanned: Counter<u64>,
    /// Unreferenced objects past the grace period identified as orphans.
    orphans_found: Counter<u64>,
    /// Orphan objects successfully deleted.
    orphans_deleted: Counter<u64>,
    /// Orphan objects whose deletion failed (retried next cycle).
    orphans_delete_failed: Counter<u64>,
    /// Bytes reclaimed by deleting orphan objects.
    bytes_reclaimed: Counter<u64>,
    /// Unreferenced objects kept because they are inside the grace period.
    orphans_skipped_young: Counter<u64>,
    /// Sweeps aborted with zero deletes because the referenced set could not be built.
    reference_set_build_failed: Counter<u64>,
    /// Wall-clock duration of one table sweep.
    sweep_duration: Histogram<f64>,
}

impl GcMetrics {
    /// Build the instruments against the global meter.
    ///
    /// Instrument construction never fails and records are no-ops until a meter
    /// provider is installed, so this is safe to call eagerly when wiring a
    /// sweep executor.
    #[must_use]
    pub fn new() -> Self {
        let meter = global::meter(METER_NAME);
        let objects_scanned = meter
            .u64_counter("gc.objects.scanned")
            .with_description("Objects listed under a table prefix during a sweep")
            .build();
        let orphans_found = meter
            .u64_counter("gc.orphans.found")
            .with_description("Unreferenced objects past the grace period identified as orphans")
            .build();
        let orphans_deleted = meter
            .u64_counter("gc.orphans.deleted")
            .with_description("Orphan objects successfully deleted")
            .build();
        let orphans_delete_failed = meter
            .u64_counter("gc.orphans.delete_failed")
            .with_description("Orphan objects whose deletion failed (retried next cycle)")
            .build();
        let bytes_reclaimed = meter
            .u64_counter("gc.bytes.reclaimed")
            .with_description("Bytes reclaimed by deleting orphan objects")
            .with_unit("By")
            .build();
        let orphans_skipped_young = meter
            .u64_counter("gc.orphans.skipped_young")
            .with_description("Unreferenced objects kept because they are inside the grace period")
            .build();
        let reference_set_build_failed = meter
            .u64_counter("gc.reference_set.build_failed")
            .with_description("Sweeps aborted with zero deletes because the referenced set could not be built")
            .build();
        let sweep_duration = meter
            .f64_histogram("gc.sweep.duration")
            .with_description("Wall-clock duration of one table sweep")
            .with_unit("s")
            .build();
        Self {
            objects_scanned,
            orphans_found,
            orphans_deleted,
            orphans_delete_failed,
            bytes_reclaimed,
            orphans_skipped_young,
            reference_set_build_failed,
            sweep_duration,
        }
    }

    fn table_attrs(table: &str) -> [KeyValue; 1] {
        [KeyValue::new(TABLE_KEY, table.to_string())]
    }

    fn kind_attrs(table: &str, class: ObjectClass) -> [KeyValue; 2] {
        let kind = match class {
            ObjectClass::Data => "data",
            ObjectClass::Metadata => "metadata",
        };
        [
            KeyValue::new(TABLE_KEY, table.to_string()),
            KeyValue::new(KIND_KEY, kind),
        ]
    }

    /// Record that an orphan was identified.
    pub fn record_orphan_found(&self, table: &str, class: ObjectClass) {
        self.orphans_found.add(1, &Self::kind_attrs(table, class));
    }

    /// Record a successful orphan deletion and the bytes it reclaimed.
    pub fn record_orphan_deleted(&self, table: &str, class: ObjectClass, bytes: u64) {
        let attrs = Self::kind_attrs(table, class);
        self.orphans_deleted.add(1, &attrs);
        self.bytes_reclaimed.add(bytes, &attrs);
    }

    /// Record a failed orphan deletion.
    pub fn record_orphan_delete_failed(&self, table: &str, class: ObjectClass) {
        self.orphans_delete_failed.add(1, &Self::kind_attrs(table, class));
    }

    /// Record that a sweep aborted because the referenced set could not be built.
    pub fn record_reference_set_build_failed(&self, table: &str) {
        self.reference_set_build_failed.add(1, &Self::table_attrs(table));
    }

    /// Record the scanned-object and skipped-young totals for a completed sweep.
    pub fn record_scan(&self, table: &str, scanned: u64, skipped_young: u64) {
        let attrs = Self::table_attrs(table);
        self.objects_scanned.add(scanned, &attrs);
        self.orphans_skipped_young.add(skipped_young, &attrs);
    }

    /// Record the wall-clock duration of one sweep, in seconds.
    pub fn record_duration(&self, table: &str, secs: f64) {
        self.sweep_duration.record(secs, &Self::table_attrs(table));
    }
}

impl Default for GcMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::GcMetrics;
    use crate::gc::decide::ObjectClass;

    #[test]
    fn gc_metrics_new_records_without_panicking() {
        let metrics = GcMetrics::new();
        metrics.record_orphan_found("logs", ObjectClass::Data);
        metrics.record_orphan_deleted("logs", ObjectClass::Metadata, 4096);
        metrics.record_orphan_delete_failed("logs", ObjectClass::Data);
        metrics.record_reference_set_build_failed("logs");
        metrics.record_scan("logs", 10, 2);
        metrics.record_duration("logs", 1.5);
    }
}
