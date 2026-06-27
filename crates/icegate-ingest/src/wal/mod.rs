mod batch;
mod boundary;
mod columns;
mod metadata;
mod sorter;
#[cfg(test)]
pub(crate) mod test_utils;
mod writer;

pub(crate) use batch::{write_logs_batch_to_wal, write_metrics_batch_to_wal, write_traces_with_operations_to_wal};
pub(crate) use boundary::RowGroupBoundaryRange;
// `RowGroupBoundaryKey` / `RowGroupBoundaryValue` are only named from `#[cfg(test)]`
// code in `sorter` and `shift`; their runtime use goes through `RowGroupBoundaryRange`.
#[cfg(test)]
pub(crate) use boundary::{RowGroupBoundaryKey, RowGroupBoundaryValue};
pub(crate) use columns::{SortColumnCache, SortColumnsDescriptor};
pub(crate) use metadata::deserialize_row_group_boundary_range;
#[cfg(test)]
pub(crate) use metadata::serialize_row_group_boundary_range;
pub(crate) use sorter::sort_logs;
pub(crate) use sorter::sort_metrics;
pub(crate) use sorter::sort_operations;
pub(crate) use sorter::sort_spans;
#[cfg(test)]
pub(crate) use test_utils::logs_row_group_boundary_range_from_batch;
pub(crate) use writer::WalAckOutcome;
pub(crate) use writer::submit_sorted_rows_to_wal;
