//! Compaction metrics recorded to the `OpenTelemetry` global meter.
//!
//! [`CompactMetrics`] holds the compaction instruments. They are built from
//! [`opentelemetry::global::meter`] so no meter has to be threaded through the
//! executor constructors: each instrument records to whatever meter provider the
//! run service installs, and is a no-op when none is set (the same contract
//! ingest's `ShiftMetrics` relies on). The instrument names and `table`
//! attribute mirror the shift metrics conventions.
//!
//! `CompactMetrics` is cheap to [`Clone`] (each instrument is an `Arc` handle
//! internally) so one instance is shared across a job's PLAN and REWRITE
//! executors.

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};

/// The meter name compaction instruments are registered under.
const METER_NAME: &str = "icegate.compaction";
/// The attribute key carrying the Iceberg table name on every instrument.
const TABLE_KEY: &str = "table";

/// Compaction instruments, built once from the global meter.
///
/// All counters are `u64` and all histograms are `f64`, matching the shift
/// metric style. Counters carrying byte totals use the `By` unit. Every
/// instrument is tagged with the `table` attribute at record time.
#[derive(Clone)]
pub struct CompactMetrics {
    /// Partitions a PLAN scan produced at least one rewrite group for.
    partitions_compacted: Counter<u64>,
    /// Partitions a PLAN scan skipped (no actionable rewrite group).
    partitions_skipped: Counter<u64>,
    /// Rewrite groups a PLAN scan fanned out as REWRITE tasks.
    groups_planned: Counter<u64>,
    /// Input data files fed into committed rewrites.
    files_in: Counter<u64>,
    /// Output data files produced by committed rewrites.
    files_out: Counter<u64>,
    /// Rows carried through committed rewrites (input total == output total).
    rows: Counter<u64>,
    /// Compressed input bytes consumed by committed rewrites.
    bytes_in: Counter<u64>,
    /// Compressed output bytes produced by committed rewrites.
    bytes_out: Counter<u64>,
    /// Per-rewrite compaction ratio (`input_bytes / output_bytes`).
    ratio: Histogram<f64>,
    /// Rewrites abandoned because an input vanished before commit.
    commit_aborted: Counter<u64>,
}

impl CompactMetrics {
    /// Build the compaction instruments from the `OpenTelemetry` global meter.
    ///
    /// Instrument construction never fails and records are no-ops until a meter
    /// provider is installed, so this is safe to call eagerly when wiring an
    /// executor.
    #[must_use]
    pub fn new() -> Self {
        let meter = global::meter(METER_NAME);
        let partitions_compacted = meter
            .u64_counter("compaction.partitions.compacted")
            .with_description("Partitions for which the planner produced at least one rewrite group")
            .build();
        let partitions_skipped = meter
            .u64_counter("compaction.partitions.skipped")
            .with_description("Partitions the planner skipped (no actionable rewrite group)")
            .build();
        let groups_planned = meter
            .u64_counter("compaction.groups.planned")
            .with_description("Rewrite groups fanned out as REWRITE tasks by the planner")
            .build();
        let files_in = meter
            .u64_counter("compaction.files.in")
            .with_description("Input data files consumed by committed rewrites")
            .build();
        let files_out = meter
            .u64_counter("compaction.files.out")
            .with_description("Output data files produced by committed rewrites")
            .build();
        let rows = meter
            .u64_counter("compaction.rows")
            .with_description("Rows carried through committed rewrites")
            .build();
        let bytes_in = meter
            .u64_counter("compaction.bytes.in")
            .with_description("Compressed input bytes consumed by committed rewrites")
            .with_unit("By")
            .build();
        let bytes_out = meter
            .u64_counter("compaction.bytes.out")
            .with_description("Compressed output bytes produced by committed rewrites")
            .with_unit("By")
            .build();
        let ratio = meter
            .f64_histogram("compaction.ratio")
            .with_description("Per-rewrite compaction ratio (input_bytes / output_bytes)")
            .build();
        let commit_aborted = meter
            .u64_counter("compaction.commit.aborted")
            .with_description("Rewrites abandoned because an input vanished before commit")
            .build();

        Self {
            partitions_compacted,
            partitions_skipped,
            groups_planned,
            files_in,
            files_out,
            rows,
            bytes_in,
            bytes_out,
            ratio,
            commit_aborted,
        }
    }

    /// One `table` attribute, the shared label on every compaction instrument.
    fn table_attrs(table: &str) -> [KeyValue; 1] {
        [KeyValue::new(TABLE_KEY, table.to_string())]
    }

    /// Record the outcome of one PLAN scan: `groups` rewrite groups fanned out
    /// across `compacted` partitions, with `skipped` partitions left untouched.
    pub fn record_plan(&self, table: &str, groups: u64, compacted: u64, skipped: u64) {
        let attrs = Self::table_attrs(table);
        self.groups_planned.add(groups, &attrs);
        self.partitions_compacted.add(compacted, &attrs);
        self.partitions_skipped.add(skipped, &attrs);
    }

    /// Record a committed rewrite: its input/output file counts, row total, and
    /// input/output compressed byte totals (plus the derived ratio histogram).
    #[allow(clippy::cast_precision_loss)]
    pub fn record_rewrite_committed(
        &self,
        table: &str,
        input_files: u64,
        output_files: u64,
        rows: u64,
        input_bytes: u64,
        output_bytes: u64,
    ) {
        let attrs = Self::table_attrs(table);
        self.files_in.add(input_files, &attrs);
        self.files_out.add(output_files, &attrs);
        self.rows.add(rows, &attrs);
        self.bytes_in.add(input_bytes, &attrs);
        self.bytes_out.add(output_bytes, &attrs);
        // Guard against a zero divisor: an empty output (0 bytes) cannot pair
        // with a non-empty input under the rewrite invariants, but recording an
        // infinite/NaN ratio would corrupt the histogram, so skip it instead.
        if output_bytes > 0 {
            self.ratio.record(input_bytes as f64 / output_bytes as f64, &attrs);
        }
    }

    /// Record one aborted rewrite (an input vanished before commit).
    pub fn record_commit_aborted(&self, table: &str) {
        self.commit_aborted.add(1, &Self::table_attrs(table));
    }
}

impl Default for CompactMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::CompactMetrics;

    /// Building the instruments from the global meter must not panic even when
    /// no meter provider is installed (records are no-ops in that case).
    #[test]
    fn compact_metrics_new_constructs_without_panicking() {
        let metrics = CompactMetrics::new();
        // Recording against the no-op provider must also be inert (no panic).
        metrics.record_plan("logs", 3, 2, 1);
        metrics.record_rewrite_committed("logs", 4, 1, 100, 4096, 1024);
        metrics.record_commit_aborted("logs");
    }
}
