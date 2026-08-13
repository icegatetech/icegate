//! User-defined scalar functions for LogQL operations in DataFusion.

pub mod date_grid;
pub mod map_filter;
pub mod map_insert;
pub mod map_merge_normalized;
pub mod map_normalized_lookup;
pub mod parse;

// Re-export all UDFs for backward compatibility
pub use date_grid::{DateGrid, compute_grid_points, find_matching_grid_indices};
pub use map_filter::{MapDropKeys, MapKeepKeys};
pub use map_insert::MapInsert;
pub use map_merge_normalized::MapMergeNormalized;
pub use map_normalized_lookup::MapGetByNormalizedKey;
pub use parse::{ParseBytes, ParseDuration, ParseNumeric};
