//! Parquet-metadata-direct scan for tag/label discovery on Iceberg tables
//! whose schema follows the `logs`/`spans`/`metrics` shape (a tenant
//! partition column, a timestamp column, some indexed top-level string
//! columns and a single `MAP<String, String>` attributes column).
//!
//! This module bypasses DataFusion entirely for the
//! Loki `/labels`, `/label_values` and Tempo `/api/search/tags`,
//! `/api/search/tag/{name}/values` metadata endpoints. The flow per request is:
//!
//! 1. Push a tenant-plus-time iceberg `Predicate` (optionally extended by
//!    callers with query-specific matchers) into `Table::scan().plan_files()`.
//! 2. For each file, open an [`iceberg::arrow::ArrowFileReader`] and read
//!    only the Parquet footer via `ParquetRecordBatchStreamBuilder::new`.
//! 3. Indexed-column labels are derived from row-group `null_count`
//!    statistics (zero data pages read). Attribute-MAP keys are derived by
//!    projecting only the `attributes.*.key` sub-column's dictionary page —
//!    only the MAP key chunks are fetched from object storage.
//! 4. For value enumeration, one column is projected per request (either the
//!    requested indexed column, or the `attributes` MAP) and values are
//!    collected in-process.
//!
//! **Limitation:** this module scans only committed Iceberg data. WAL
//! (Write-Ahead Log) segments are excluded — recent writes that have not yet
//! been shifted to Iceberg will not appear in metadata discovery results.
//!
//! Semantics: over-approximation is permitted. Matchers that cannot be
//! translated to an iceberg predicate are silently omitted — this only widens
//! the set of row groups considered, never narrows it.
//!
//! ## Generalization across tables
//!
//! The per-table vocabulary (which columns are indexed, which alias to what,
//! which map keys to exclude) is captured in [`MetadataScanConfig`]. Callers
//! build a config for their table (logs, spans, metrics) and pass it to
//! [`scan_labels`] / [`scan_label_values`].

mod error;
mod labels;
mod parquet_reader;
pub(crate) mod predicate;
mod values;

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
pub use error::MetadataScanError;
use futures::{StreamExt, TryStreamExt, stream};
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
pub use predicate::base_predicate;

/// Maximum number of Parquet files processed concurrently per request.
const METADATA_SCAN_CONCURRENCY: usize = 16;

/// Per-table configuration driving the metadata scan.
///
/// Captures everything that differs between tables (logs, spans, metrics):
/// - Which top-level columns are "indexed labels/tags" that should surface
///   in label/tag enumeration.
/// - Which labels are aliases for an underlying column (Grafana-compatible
///   renames such as `level` → `severity_text` or `service` →
///   `service_name`).
/// - Which map keys to hide from discovery (typically high-cardinality IDs
///   that already have a dedicated column).
#[derive(Debug, Clone, Copy)]
pub struct MetadataScanConfig {
    /// Top-level indexed string columns whose distinct-value set is surfaced
    /// as part of the label/tag enumeration output. High-cardinality
    /// identifiers (`trace_id`, `span_id`) typically live in a separate
    /// whitelist used only for value enumeration — see callers.
    pub indexed_columns: &'static [&'static str],
    /// `(alias, underlying)` mappings. For logs: `("level", "severity_text")`
    /// and `("service", "service_name")`. Used both to surface the alias in
    /// `/labels` output and to resolve alias→column in `/label_values`
    /// lookups.
    pub label_aliases: &'static [(&'static str, &'static str)],
    /// MAP<String,String> attribute keys to silently drop from `/labels`
    /// enumeration output. Useful for high-cardinality duplicates of indexed
    /// columns (e.g. the `attributes["trace_id"]` copy of the `trace_id`
    /// column).
    pub excluded_map_keys: &'static [&'static str],
    /// Name of the MAP<STRING,STRING> column to scan for attribute keys and
    /// values. Logs use a single `attributes` column; spans (after the
    /// 2026-04-19 split) have `resource_attributes` and `span_attributes`
    /// scanned via two separate configs.
    pub map_column: &'static str,
}

impl MetadataScanConfig {
    /// Resolve a user-facing label name to its underlying indexed column
    /// name, if it is an alias. Otherwise returns the input unchanged.
    #[must_use]
    pub fn resolve_column<'a>(&self, label: &'a str) -> &'a str {
        for (alias, underlying) in self.label_aliases {
            if *alias == label {
                return underlying;
            }
        }
        label
    }

    /// Whether `label` refers to a top-level indexed column (after alias
    /// resolution).
    #[must_use]
    pub fn is_indexed(&self, label: &str) -> bool {
        let resolved = self.resolve_column(label);
        self.indexed_columns.contains(&resolved)
    }

    /// Aliases that should be surfaced alongside a given underlying column
    /// when it is reported as having values in a row group.
    ///
    /// E.g. for logs with `("level", "severity_text")`, when `severity_text`
    /// has values this yields `["level"]`.
    pub(crate) fn aliases_for<'a>(&'a self, underlying: &'a str) -> impl Iterator<Item = &'static str> + 'a {
        self.label_aliases
            .iter()
            .filter(move |(_, u)| *u == underlying)
            .map(|(a, _)| *a)
    }
}

/// Compute the set of label/tag names visible in `table` for the given
/// tenant, time range, and additional predicate.
///
/// `extra_predicate` is AND'd with the tenant+time base predicate (see
/// [`base_predicate`]). Pass [`Predicate::AlwaysTrue`] to disable.
///
/// # Errors
///
/// Returns an error if iceberg planning fails or if a referenced Parquet file
/// cannot be read.
#[tracing::instrument(
    skip(table, config, extra_predicate),
    fields(
        tenant_id = %tenant_id,
        num_files = tracing::field::Empty,
    )
)]
pub async fn scan_labels(
    table: &Table,
    tenant_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    config: &MetadataScanConfig,
    extra_predicate: Predicate,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let predicate = predicate::combine(base_predicate(tenant_id, start, end), extra_predicate);
    let files = plan_files(table, &predicate).await?;
    tracing::Span::current().record("num_files", files.len());
    let file_io = table.file_io().clone();

    let mut stream = stream::iter(files)
        .map(|task| {
            let file_io = file_io.clone();
            let predicate = predicate.clone();
            async move { scan_labels_for_file(&file_io, task, &predicate, config).await }
        })
        .buffer_unordered(METADATA_SCAN_CONCURRENCY);

    let mut result: BTreeSet<String> = BTreeSet::new();
    while let Some(r) = stream.next().await {
        result.extend(r?);
    }

    Ok(result)
}

/// Compute the distinct values of a single label in `table` for the given
/// tenant, time range, and additional predicate.
///
/// `label_name` may be an alias (e.g. `"level"`) — the alias is resolved to
/// its underlying column via `config.label_aliases`.
///
/// # Errors
///
/// Returns an error if iceberg planning fails or if a referenced Parquet file
/// cannot be read.
#[tracing::instrument(
    skip(table, config, extra_predicate),
    fields(
        tenant_id = %tenant_id,
        label_name = %label_name,
        num_files = tracing::field::Empty,
    )
)]
pub async fn scan_label_values(
    table: &Table,
    tenant_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    config: &MetadataScanConfig,
    label_name: &str,
    extra_predicate: Predicate,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let predicate = predicate::combine(base_predicate(tenant_id, start, end), extra_predicate);
    let kind = values::classify_label(label_name, config);
    let files = plan_files(table, &predicate).await?;
    tracing::Span::current().record("num_files", files.len());
    let file_io = table.file_io().clone();

    let indexed_column = config.resolve_column(label_name).to_string();
    let label_name = label_name.to_string();
    let mut stream = stream::iter(files)
        .map(|task| {
            let file_io = file_io.clone();
            let label = label_name.clone();
            let indexed_column = indexed_column.clone();
            let predicate = predicate.clone();
            async move {
                scan_label_values_for_file(&file_io, task, &predicate, config, kind, &label, &indexed_column).await
            }
        })
        .buffer_unordered(METADATA_SCAN_CONCURRENCY);

    let mut result: BTreeSet<String> = BTreeSet::new();
    while let Some(r) = stream.next().await {
        result.extend(r?);
    }

    Ok(result)
}

/// Plan the files to scan for the given predicate.
async fn plan_files(table: &Table, predicate: &Predicate) -> Result<Vec<FileScanTask>, MetadataScanError> {
    let scan = table
        .scan()
        .with_filter(predicate.clone())
        .build()
        .map_err(MetadataScanError::Iceberg)?;

    let stream = scan.plan_files().await.map_err(MetadataScanError::Iceberg)?;
    let files: Vec<FileScanTask> = stream.try_collect().await.map_err(MetadataScanError::Iceberg)?;
    Ok(files)
}

/// Process one Parquet file for a label/tag enumeration request.
///
/// Neither stage materializes row batches:
/// - Indexed-column labels are derived from `null_count` in row-group
///   statistics (pure Parquet metadata, zero pages).
/// - MAP attribute keys are derived from the dictionary page of the
///   `attributes.*.key` sub-column (one range read per row group, data
///   pages never decoded).
#[tracing::instrument(skip_all, fields(file = %task.data_file_path))]
async fn scan_labels_for_file(
    file_io: &iceberg::io::FileIO,
    task: FileScanTask,
    predicate: &Predicate,
    config: &MetadataScanConfig,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let (mut reader, metadata) = parquet_reader::open_file_direct(file_io, &task).await?;

    let mut out: BTreeSet<String> = BTreeSet::new();
    labels::collect_indexed_from_metadata(&metadata, config, &mut out);
    labels::collect_map_keys_via_dict(&mut reader, &metadata, predicate, config, &mut out).await?;
    Ok(out)
}

/// Process one Parquet file for a label/tag-value enumeration request.
///
/// - Indexed case (e.g. `service_name`, `level`): open the file directly
///   and read the column's dictionary page. No row batches.
/// - MAP case (non-indexed labels): open the file via the record-batch
///   stream builder and project the `attributes` column, so we can
///   correlate `key == label_name` rows with their values.
#[tracing::instrument(skip_all, fields(file = %task.data_file_path, kind = ?kind))]
async fn scan_label_values_for_file(
    file_io: &iceberg::io::FileIO,
    task: FileScanTask,
    predicate: &Predicate,
    config: &MetadataScanConfig,
    kind: values::LabelKind,
    label_name: &str,
    indexed_column: &str,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let mut out: BTreeSet<String> = BTreeSet::new();

    match kind {
        values::LabelKind::Indexed => {
            let (mut reader, metadata) = parquet_reader::open_file_direct(file_io, &task).await?;
            values::collect_indexed_values_via_dict(&mut reader, &metadata, predicate, indexed_column, &mut out)
                .await?;
        }
        values::LabelKind::MapAttribute => {
            let builder = parquet_reader::open_builder(file_io, &task).await?;
            values::stream_map_values(builder, predicate, config, label_name, &mut out).await?;
        }
    }

    Ok(out)
}
