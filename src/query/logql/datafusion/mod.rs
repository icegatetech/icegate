//! DataFusion implementation of the LogQL planner.

/// `LogQL` planner implementation.
pub mod planner;

/// Registry for UDFs and UDAFs.
pub mod registry;

/// User-defined aggregate functions for LogQL operations.
pub mod udaf;

/// User-defined scalar functions for LogQL operations.
pub mod udf;

#[cfg(test)]
mod planner_tests;

pub use planner::DataFusionPlanner;
pub use registry::UdfRegistry;
