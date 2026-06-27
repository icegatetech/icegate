//! Row-group boundary key types and the sort-key comparison primitive.
//!
//! These types moved to [`icegate_common::merge::sort_key`] so the Parquet
//! compaction reader can construct boundary keys from Iceberg `Datum` bounds
//! using the same comparison semantics the writer used. They are re-exported
//! here so the existing `crate::wal::{...}` / `super::{...}` paths inside
//! ingest keep resolving unchanged. The unit tests for these types live
//! alongside the definitions in `icegate-common`.

pub(crate) use icegate_common::merge::sort_key::RowGroupBoundaryRange;
// The remaining boundary types are only named from `#[cfg(test)]` code in
// `sorter`, `shift`, and `test_utils`; runtime code reaches them through
// `RowGroupBoundaryRange`.
#[cfg(test)]
pub(crate) use icegate_common::merge::sort_key::{
    RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryValue,
};
