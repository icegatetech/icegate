//! DataFusion implementation of the PromQL planner.

/// PromQL planner implementation.
pub mod planner;

/// Registry for UDFs and UDAFs.
pub mod registry;

/// User-defined aggregate functions for PromQL operations.
pub mod udaf;

/// User-defined scalar functions for PromQL operations.
pub mod udf;

#[cfg(test)]
mod planner_tests;

pub use planner::DataFusionPlanner;
pub use registry::UdfRegistry;
