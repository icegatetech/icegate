//! Sort-key descriptors and the per-batch comparison cache.
//!
//! These types moved to [`icegate_common::merge::sort_key`] so the Parquet
//! compaction reader can reuse the exact same Iceberg sort-order comparison
//! plan the WAL sorter and Shifter merger use. They are re-exported here so
//! the existing `crate::wal::{...}` paths inside ingest keep resolving
//! unchanged. The unit tests for these types live alongside the definitions
//! in `icegate-common`.

pub(crate) use icegate_common::merge::sort_key::{SortColumnCache, SortColumnsDescriptor};
