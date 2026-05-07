//! Test-only helpers for constructing [`RowGroupBoundaryComponent`] values.
//!
//! Kept out of the production `boundary` module so that the public surface
//! used at runtime stays focused on validation/comparison primitives.

use arrow::record_batch::RecordBatch;

use super::boundary::{RowGroupBoundaryComponent, RowGroupBoundaryRange, RowGroupBoundaryValue};
use super::columns::{SortColumnCache, SortColumnsDescriptor};
use crate::error::{IngestError, Result};

/// Build a string-typed boundary component for tests.
pub(crate) fn boundary_component_string(
    value: Option<String>,
    descending: bool,
    nulls_first: bool,
) -> RowGroupBoundaryComponent {
    RowGroupBoundaryComponent {
        value: value.map(RowGroupBoundaryValue::String),
        descending,
        nulls_first,
    }
}

/// Build a `timestamp_micros`-typed boundary component for tests.
pub(crate) fn boundary_component_timestamp_micros(
    value: Option<i64>,
    descending: bool,
    nulls_first: bool,
) -> RowGroupBoundaryComponent {
    RowGroupBoundaryComponent {
        value: value.map(RowGroupBoundaryValue::TimestampMicros),
        descending,
        nulls_first,
    }
}

pub(crate) fn boundary_component_fixed_bytes(
    value: Option<Vec<u8>>,
    descending: bool,
    nulls_first: bool,
) -> RowGroupBoundaryComponent {
    RowGroupBoundaryComponent {
        value: value.map(RowGroupBoundaryValue::FixedBytes),
        descending,
        nulls_first,
    }
}

/// Build a [`RowGroupBoundaryRange`] from the first and last row of a sorted logs batch.
///
/// # Errors
///
/// Returns an error if the batch is empty, the sort columns are missing, or the resulting range is invalid.
pub(crate) fn logs_row_group_boundary_range_from_batch(batch: &RecordBatch) -> Result<RowGroupBoundaryRange> {
    if batch.num_rows() == 0 {
        return Err(IngestError::Shift(
            "cannot build boundary range from empty WAL row group".to_string(),
        ));
    }
    let sort_columns = SortColumnCache::try_new(batch, SortColumnsDescriptor::logs()?, "WAL sorting")?;
    let last_row_idx = batch.num_rows() - 1;
    let min_key = sort_columns.boundary_key(0);
    let max_key = sort_columns.boundary_key(last_row_idx);
    let range = RowGroupBoundaryRange {
        names: sort_columns.column_names(),
        min_key,
        max_key,
    };
    range.validate().map_err(|err| match err {
        IngestError::Shift(message) => IngestError::Shift(format!("invalid WAL row-group boundary range: {message}")),
        other => other,
    })?;
    Ok(range)
}
