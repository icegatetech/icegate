//! DataFusion implementation of the LogQL planner.

/// `LogQL` planner implementation.
pub mod planner;

/// User-defined functions for LogQL operations.
pub mod udf;

#[cfg(test)]
mod planner_tests;

pub use planner::DataFusionPlanner;
pub use udf::UdfRegistry;
