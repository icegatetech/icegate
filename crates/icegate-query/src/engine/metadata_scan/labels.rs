//! `/labels` algorithm.
//!
//! Two sources of labels, neither of which decodes row data:
//!
//! 1. Indexed top-level columns — derived from row-group `null_count`
//!    statistics. Pure Parquet metadata, zero data or dictionary pages
//!    fetched.
//! 2. Attribute MAP keys — derived by pulling only the dictionary page of
//!    the `attributes.*.key` sub-column for every row group via
//!    [`crate::engine::metadata_scan::parquet_reader::read_column_dictionaries`].

use std::collections::BTreeSet;

use iceberg::arrow::ArrowFileReader;
use iceberg::expr::Predicate;
use icegate_common::schema::LOG_INDEXED_ATTRIBUTE_COLUMNS;
use parquet::file::metadata::ParquetMetaData;

use super::error::MetadataScanError;
use super::parquet_reader;

/// Walk row-group statistics and add every indexed column that has at least
/// one non-null value to `out`. Also inserts `"level"` whenever
/// `severity_text` is present (Grafana compatibility alias).
///
/// Pure metadata: no data pages are read.
pub fn collect_indexed_from_metadata(metadata: &ParquetMetaData, out: &mut BTreeSet<String>) {
    let schema = metadata.file_metadata().schema_descr();

    // Map indexed column name -> leaf column index. Done once per file.
    let mut name_to_leaf: Vec<(&'static str, usize)> = Vec::new();
    for &name in LOG_INDEXED_ATTRIBUTE_COLUMNS {
        if let Some(idx) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == name) {
            name_to_leaf.push((name, idx));
        }
    }

    for rg_idx in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(rg_idx);
        let num_rows = rg.num_rows();

        for &(name, leaf_idx) in &name_to_leaf {
            // Fast path: label already recorded. Exception: when we've
            // seen `severity_text` but not yet the `level` alias, keep
            // going so we can add `level` on the first row-group stats
            // check that finds a non-null `severity_text`.
            if out.contains(name) && (name != "severity_text" || out.contains("level")) {
                continue;
            }

            let col = rg.column(leaf_idx);
            // No stats → conservative: assume the column might have values
            // (over-approximation is allowed).
            let has_values = col.statistics().map_or(true, |stats| {
                let null_count = stats.null_count_opt().unwrap_or(0);
                // `num_rows()` is i64 in parquet-rs; coerce to u64 for the
                // comparison. Row counts are always non-negative.
                let total: u64 = u64::try_from(num_rows).unwrap_or(0);
                null_count < total
            });

            if has_values {
                out.insert(name.to_string());
                if name == "severity_text" {
                    out.insert("level".to_string());
                }
            }
        }
    }
}

/// Collect distinct MAP attribute keys by reading only the dictionary
/// page of the `attributes.*.key` sub-column for every row group that
/// survives row-group predicate pruning.
///
/// # Errors
///
/// Returns `MetadataScanError::Parquet` if a column chunk fails to decode.
#[tracing::instrument(skip_all)]
pub async fn collect_map_keys_via_dict(
    reader: &mut ArrowFileReader,
    metadata: &ParquetMetaData,
    predicate: &Predicate,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    let schema = metadata.file_metadata().schema_descr();

    // Find the `attributes.*.key` leaf. Arrow's MAP<Utf8,Utf8> may
    // serialize as `attributes.key_value.key` or `attributes.entries.key`
    // depending on writer version — match on the top-level column + the
    // leaf name rather than a hard-coded path.
    let key_leaf_idx = (0..schema.num_columns()).find(|&i| {
        let col = schema.column(i);
        let parts = col.path().parts();
        parts.first().is_some_and(|s| s == "attributes") && parts.last().is_some_and(|s| s == "key")
    });
    let Some(key_leaf_idx) = key_leaf_idx else {
        // No attributes MAP — nothing to do. Defensive; should not happen
        // for the logs table.
        return Ok(());
    };

    parquet_reader::read_column_dictionaries(reader, metadata, predicate, key_leaf_idx, out).await
}
