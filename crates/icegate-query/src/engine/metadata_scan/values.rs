//! `/label_values` algorithm.
//!
//! Two cases, selected by [`classify_label`]:
//!
//! 1. The label is the `level` alias — resolved against the indexed
//!    `severity_text` column by reading only its dictionary page. No
//!    row data is decoded.
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
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;

use super::error::MetadataScanError;
use super::parquet_reader;

/// Which code path to use for a given label name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelKind {
    /// Indexed top-level column (only the `level` alias today). Read via
    /// dictionary page only.
    Indexed,
    /// Label stored in the `attributes` MAP. Read via projected record
    /// batches over the `attributes` column.
    MapAttribute,
}

/// Classify a label name.
#[must_use]
pub fn classify_label(name: &str) -> LabelKind {
    if name == "level" {
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
/// single label key across all row groups. Consumes the builder.
///
/// # Errors
///
/// Returns `MetadataScanError::Parquet` if projected record-batch reads
/// fail.
#[tracing::instrument(skip_all, fields(label_name = label_name, num_batches = tracing::field::Empty))]
pub async fn stream_map_values(
    builder: ParquetRecordBatchStreamBuilder<ArrowFileReader>,
    label_name: &str,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    let schema_descr = builder.parquet_schema();
    let has_attributes = (0..schema_descr.num_columns())
        .any(|i| schema_descr.column(i).path().parts().first().is_some_and(|s| s == "attributes"));
    if !has_attributes {
        return Ok(());
    }

    let mask = ProjectionMask::columns(schema_descr, ["attributes"]);
    let mut stream = builder.with_projection(mask).build()?;

    let mut num_batches: usize = 0;
    while let Some(batch) = stream.try_next().await? {
        num_batches += 1;
        collect_map_values_from_batch(&batch, label_name, out);
    }
    tracing::Span::current().record("num_batches", num_batches);

    Ok(())
}

fn collect_map_values_from_batch(batch: &RecordBatch, label_name: &str, out: &mut BTreeSet<String>) {
    let Ok(attr_idx) = batch.schema().index_of("attributes") else {
        return;
    };
    let Some(map_arr) = batch.column(attr_idx).as_any().downcast_ref::<MapArray>() else {
        return;
    };
    let Some(keys) = map_arr.keys().as_any().downcast_ref::<StringArray>() else {
        return;
    };
    let Some(values) = map_arr.values().as_any().downcast_ref::<StringArray>() else {
        return;
    };

    for i in 0..keys.len() {
        if keys.is_valid(i) && keys.value(i) == label_name && values.is_valid(i) {
            let v = values.value(i);
            if !out.contains(v) {
                out.insert(v.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LabelKind, classify_label};

    #[test]
    fn classify_level_is_indexed() {
        assert_eq!(classify_label("level"), LabelKind::Indexed);
    }

    #[test]
    fn classify_service_name_routes_through_map() {
        assert_eq!(classify_label("service_name"), LabelKind::MapAttribute);
    }
}
