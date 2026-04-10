//! `/label_values` algorithm.
//!
//! Two cases, selected by [`classify_label`]:
//!
//! 1. The label maps to an indexed top-level column (e.g. `service_name`,
//!    `trace_id`, or the `level` alias for `severity_text`) — resolved by
//!    reading only its dictionary page. No row data is decoded.
//! 2. Any other label — routed through the `attributes` MAP lookup.
//!    This case needs correlated key/value access (so we can return the
//!    value for the rows where `key == label_name`) and is implemented
//!    via a column-projected record-batch stream over the `attributes`
//!    column.

use std::collections::BTreeSet;

use datafusion::arrow::array::{Array, MapArray, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::arrow::ArrowFileReader;
use iceberg::expr::Predicate;
use icegate_common::schema::COL_ATTRIBUTES;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;

use super::error::MetadataScanError;
use super::{parquet_reader, predicate};

/// Which code path to use for a given label name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelKind {
    /// Indexed top-level column (e.g. `service_name`, `trace_id`, `level`).
    /// Read via dictionary page only.
    Indexed,
    /// Label stored in the `attributes` MAP. Read via projected record
    /// batches over the `attributes` column.
    MapAttribute,
}

/// Classify a label name as indexed or MAP-stored.
///
/// Delegates to [`predicate::is_indexed_column`] which checks
/// [`LOG_INDEXED_ATTRIBUTE_COLUMNS`](icegate_common::schema::LOG_INDEXED_ATTRIBUTE_COLUMNS)
/// and handles the `level` → `severity_text` alias.
#[must_use]
pub fn classify_label(name: &str) -> LabelKind {
    if predicate::is_indexed_column(name) {
        LabelKind::Indexed
    } else {
        LabelKind::MapAttribute
    }
}

/// Collect distinct values for an indexed top-level string column by
/// reading only its dictionary page for every row group that survives
/// row-group predicate pruning.
///
/// # Errors
///
/// Returns `MetadataScanError::Parquet` if a column chunk fails to decode.
#[tracing::instrument(skip_all, fields(column = column_name))]
pub async fn collect_indexed_values_via_dict(
    reader: &mut ArrowFileReader,
    metadata: &ParquetMetaData,
    predicate: &Predicate,
    column_name: &str,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    let schema = metadata.file_metadata().schema_descr();
    let Some(leaf_idx) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == column_name) else {
        // Column not present in this file — nothing to do. Not an error
        // under over-approximation semantics.
        return Ok(());
    };

    parquet_reader::read_column_dictionaries(reader, metadata, predicate, leaf_idx, out).await
}

/// Project the `attributes` MAP column and collect distinct values for a
/// single label key across surviving row groups. Consumes the builder.
///
/// Row groups whose statistics are incompatible with the given
/// `predicate` (tenant, time range, etc.) are pruned before scanning.
///
/// # Errors
///
/// Returns `MetadataScanError::Parquet` if projected record-batch reads
/// fail, or `MetadataScanError::Schema` if the `attributes` column has
/// an unexpected type.
#[tracing::instrument(skip_all, fields(label_name = label_name, num_batches = tracing::field::Empty, pruned_rgs = tracing::field::Empty))]
pub async fn stream_map_values(
    builder: ParquetRecordBatchStreamBuilder<ArrowFileReader>,
    predicate: &Predicate,
    label_name: &str,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    let schema_descr = builder.parquet_schema();
    let has_attributes = (0..schema_descr.num_columns()).any(|i| {
        schema_descr
            .column(i)
            .path()
            .parts()
            .first()
            .is_some_and(|s| s == COL_ATTRIBUTES)
    });
    if !has_attributes {
        return Ok(());
    }

    // Row-group pruning: only scan row groups whose statistics
    // are compatible with the predicate (tenant_id, time range, etc.).
    let metadata = builder.metadata();
    let total_rgs = metadata.num_row_groups();
    let surviving: Vec<usize> = (0..total_rgs)
        .filter(|&i| parquet_reader::row_group_can_match(metadata.row_group(i), predicate))
        .collect();
    let pruned = total_rgs - surviving.len();
    tracing::Span::current().record("pruned_rgs", pruned);

    let mask = ProjectionMask::columns(schema_descr, [COL_ATTRIBUTES]);
    let mut stream = builder.with_projection(mask).with_row_groups(surviving).build()?;

    let mut num_batches: usize = 0;
    while let Some(batch) = stream.try_next().await? {
        num_batches += 1;
        collect_map_values_from_batch(&batch, label_name, out)?;
    }
    tracing::Span::current().record("num_batches", num_batches);

    Ok(())
}

fn collect_map_values_from_batch(
    batch: &RecordBatch,
    label_name: &str,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    let attr_idx = batch
        .schema()
        .index_of(COL_ATTRIBUTES)
        .map_err(|_| MetadataScanError::Schema("batch missing 'attributes' column".to_string()))?;
    let map_arr = batch
        .column(attr_idx)
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| MetadataScanError::Schema("'attributes' column is not a MapArray".to_string()))?;
    let keys = map_arr
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| MetadataScanError::Schema("'attributes' map keys are not StringArray".to_string()))?;
    let values = map_arr
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| MetadataScanError::Schema("'attributes' map values are not StringArray".to_string()))?;

    for i in 0..keys.len() {
        if keys.is_valid(i) && keys.value(i) == label_name && values.is_valid(i) {
            let v = values.value(i);
            if !out.contains(v) {
                out.insert(v.to_string());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{LabelKind, classify_label};

    #[test]
    fn classify_level_is_indexed() {
        assert_eq!(classify_label("level"), LabelKind::Indexed);
    }

    #[test]
    fn classify_indexed_columns_are_indexed() {
        assert_eq!(classify_label("service_name"), LabelKind::Indexed);
        assert_eq!(classify_label("trace_id"), LabelKind::Indexed);
        assert_eq!(classify_label("span_id"), LabelKind::Indexed);
        assert_eq!(classify_label("severity_text"), LabelKind::Indexed);
        assert_eq!(classify_label("cloud_account_id"), LabelKind::Indexed);
    }

    #[test]
    fn classify_map_attribute_for_non_indexed() {
        assert_eq!(classify_label("pod"), LabelKind::MapAttribute);
        assert_eq!(classify_label("namespace"), LabelKind::MapAttribute);
    }
}
