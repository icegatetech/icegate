//! Per-source execution metrics extracted from the physical plan tree.
//!
//! After query execution, the physical plan tree retains metrics from each
//! scan node. This module walks the tree and extracts row/byte counts per
//! data source (Iceberg vs WAL) for Loki stats reporting.

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanVisitor;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::visit_execution_plan;

/// Row/byte counts per data source, extracted from the physical plan tree.
#[derive(Debug, Default)]
pub struct SourceMetrics {
    /// Rows read from Iceberg tables.
    pub iceberg_rows: usize,
    /// Decompressed bytes read from Iceberg tables.
    pub iceberg_bytes: usize,
    /// Compressed bytes read from Iceberg Parquet files.
    ///
    /// Currently always 0: iceberg-rust does not expose I/O-level byte
    /// counters, and `FileScanTask.length` (full file size) is not a valid
    /// proxy after column projection and row-group filtering.
    pub iceberg_compressed_bytes: usize,
    /// Rows read from WAL segments.
    pub wal_rows: usize,
    /// Decompressed bytes read from WAL segments.
    pub wal_bytes: usize,
    /// Compressed bytes scanned from WAL Parquet files (`bytes_scanned`
    /// metric from DataFusion's `ParquetSource`).
    pub wal_compressed_bytes: usize,
}

/// Walk the physical plan tree and extract per-source metrics.
///
/// Identifies sources by node name:
/// - `"IcegateIcebergScan"` -> Iceberg (`output_rows`, `output_bytes`)
/// - `"DataSourceExec"` -> WAL (`output_rows`, `output_bytes`, `bytes_scanned`)
pub fn extract_source_metrics(plan: &dyn ExecutionPlan) -> SourceMetrics {
    let mut collector = SourceMetricsCollector::default();
    let _ = visit_execution_plan(plan, &mut collector);
    collector.result
}

/// Plan tree visitor that aggregates per-source metrics.
#[derive(Default)]
struct SourceMetricsCollector {
    result: SourceMetrics,
}

impl ExecutionPlanVisitor for SourceMetricsCollector {
    type Error = std::fmt::Error;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        let name = plan.name();
        let Some(metrics) = plan.metrics() else {
            return Ok(true);
        };

        match name {
            "IcegateIcebergScan" => {
                self.result.iceberg_rows += extract_metric_value(&metrics, "output_rows");
                self.result.iceberg_bytes += extract_metric_value(&metrics, "output_bytes");
                // Note: compressed_bytes is not available from the Iceberg scan.
                // FileScanTask.length is the full file size, not the compressed
                // bytes actually read after column projection and row-group
                // filtering. iceberg-rust does not expose I/O-level byte counters.
            }
            "DataSourceExec" => {
                self.result.wal_rows += extract_metric_value(&metrics, "output_rows");
                self.result.wal_bytes += extract_metric_value(&metrics, "output_bytes");
                self.result.wal_compressed_bytes += extract_metric_value(&metrics, "bytes_scanned");
            }
            _ => {}
        }

        Ok(true)
    }
}

/// Extract a named metric value from a `MetricsSet`, returning 0 if absent.
fn extract_metric_value(metrics: &datafusion::physical_plan::metrics::MetricsSet, name: &str) -> usize {
    metrics
        .iter()
        .filter_map(|m| {
            if m.value().name() == name {
                match m.value() {
                    MetricValue::OutputRows(count)
                    | MetricValue::OutputBytes(count)
                    | MetricValue::Count { count, .. } => Some(count.value()),
                    _ => None,
                }
            } else {
                None
            }
        })
        .sum()
}
