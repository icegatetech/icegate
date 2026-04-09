//! Parquet-metadata-direct scan for `/labels` and `/label_values` on the
//! `logs` table.
//!
//! This module bypasses DataFusion entirely for Loki label metadata
//! endpoints. The flow per request is:
//!
//! 1. Translate the `LogQL` selector (plus tenant + time) into an iceberg
//!    `Predicate` and plan the matching files via `Table::scan().plan_files()`.
//! 2. For each file, open an [`iceberg::arrow::ArrowFileReader`] and read
//!    only the Parquet footer via `ParquetRecordBatchStreamBuilder::new`.
//! 3. For `/labels`, the set of indexed-column labels is derived purely from
//!    row-group `null_count` statistics (zero data pages read). The set of
//!    `attributes` MAP keys is derived by projecting the `attributes` column
//!    and iterating the resulting `MapArray` keys — only the MAP column
//!    chunks are fetched from object storage, not the rest of the row.
//! 4. For `/label_values`, one column is projected per request (either the
//!    requested indexed column, or the `attributes` MAP) and values are
//!    collected in-process.
//!
//! Semantics: over-approximation is permitted. `LogQL` matchers that cannot be
//! translated to an iceberg predicate (regex matchers, matchers on MAP-only
//! labels) are silently omitted — this only widens the set of row groups
//! considered, never narrows it. See
//! `docs/superpowers/specs/2026-04-08-label-metadata-scan-design.md`.

mod error;
mod labels;
mod parquet_reader;
mod predicate;
mod values;

use std::collections::BTreeSet;

pub use error::MetadataScanError;
use futures::{StreamExt, TryStreamExt, stream};
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;

use crate::logql::log::Selector;
use crate::logql::planner::QueryContext;

/// Maximum number of Parquet files processed concurrently per request.
const METADATA_SCAN_CONCURRENCY: usize = 16;

/// Compute the set of label names visible in `logs` for the given tenant,
/// time range and selector.
///
/// # Errors
///
/// Returns an error if iceberg planning fails or if a referenced Parquet file
/// cannot be read.
#[tracing::instrument(
    skip(table, query_ctx, selector),
    fields(
        tenant_id = %query_ctx.tenant_id,
        num_files = tracing::field::Empty,
    )
)]
pub async fn scan_labels(
    table: &Table,
    query_ctx: &QueryContext,
    selector: &Selector,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let (files, predicate) = plan_files(table, query_ctx, selector).await?;
    tracing::Span::current().record("num_files", files.len());
    let file_io = table.file_io().clone();

    let mut stream = stream::iter(files)
        .map(|task| {
            let file_io = file_io.clone();
            let predicate = predicate.clone();
            async move { scan_labels_for_file(&file_io, task, &predicate).await }
        })
        .buffer_unordered(METADATA_SCAN_CONCURRENCY);

    let mut result: BTreeSet<String> = BTreeSet::new();
    while let Some(r) = stream.next().await {
        result.extend(r?);
    }

    Ok(result)
}

/// Compute the distinct values for a single label in `logs` for the given
/// tenant, time range and selector.
///
/// # Errors
///
/// Returns an error if iceberg planning fails or if a referenced Parquet file
/// cannot be read.
#[tracing::instrument(
    skip(table, query_ctx, selector),
    fields(
        tenant_id = %query_ctx.tenant_id,
        label_name = %label_name,
        num_files = tracing::field::Empty,
    )
)]
pub async fn scan_label_values(
    table: &Table,
    query_ctx: &QueryContext,
    selector: &Selector,
    label_name: &str,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let kind = values::classify_label(label_name);
    let (files, predicate) = plan_files(table, query_ctx, selector).await?;
    tracing::Span::current().record("num_files", files.len());
    let file_io = table.file_io().clone();

    // Map label name to its underlying column (e.g. `level` → `severity_text`).
    let indexed_column = predicate::indexed_column_name(label_name);

    let label_name = label_name.to_string();
    let mut stream = stream::iter(files)
        .map(|task| {
            let file_io = file_io.clone();
            let label = label_name.clone();
            let indexed_column = indexed_column.clone();
            let predicate = predicate.clone();
            async move { scan_label_values_for_file(&file_io, task, &predicate, kind, &label, &indexed_column).await }
        })
        .buffer_unordered(METADATA_SCAN_CONCURRENCY);

    let mut result: BTreeSet<String> = BTreeSet::new();
    while let Some(r) = stream.next().await {
        result.extend(r?);
    }

    Ok(result)
}

/// Plan the files to scan by pushing the tenant/time/selector predicate into
/// iceberg.
async fn plan_files(
    table: &Table,
    query_ctx: &QueryContext,
    selector: &Selector,
) -> Result<(Vec<FileScanTask>, Predicate), MetadataScanError> {
    let pred = predicate::full_predicate(&query_ctx.tenant_id, query_ctx.start, query_ctx.end, selector);

    let scan = table
        .scan()
        .with_filter(pred.clone())
        .build()
        .map_err(MetadataScanError::Iceberg)?;

    let stream = scan.plan_files().await.map_err(MetadataScanError::Iceberg)?;
    let files: Vec<FileScanTask> = stream.try_collect().await.map_err(MetadataScanError::Iceberg)?;
    Ok((files, pred))
}

/// Process one Parquet file for a `/labels` request.
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
) -> Result<BTreeSet<String>, MetadataScanError> {
    let (mut reader, metadata) = parquet_reader::open_file_direct(file_io, &task).await?;

    let mut out: BTreeSet<String> = BTreeSet::new();
    labels::collect_indexed_from_metadata(&metadata, &mut out);
    labels::collect_map_keys_via_dict(&mut reader, &metadata, predicate, &mut out).await?;
    Ok(out)
}

/// Process one Parquet file for a `/label_values` request.
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
            values::stream_map_values(builder, predicate, label_name, &mut out).await?;
        }
    }

    Ok(out)
}
