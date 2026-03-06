//! User-defined aggregate functions for LogQL operations in DataFusion.
//!
//! This module provides UDAFs for LogQL-specific aggregation operations:
//! - `ArrayIntersectAgg`: Array intersection for `absent_over_time`
//! - `GridAgg`: Grid-bucketed aggregation for range aggregations

/// Array intersection UDAF for `absent_over_time`.
pub mod array_intersect;

/// Grid-bucketed aggregation UDAF for range aggregations.
pub mod grid_agg;

pub use array_intersect::ArrayIntersectAgg;
pub use grid_agg::{GridAgg, GridAggOp};
