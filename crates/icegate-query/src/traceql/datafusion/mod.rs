//! `DataFusion` implementation of the `TraceQL` planner.

mod metrics;
mod pipeline;
pub mod planner;
mod registry;
mod selectors;

// Re-exports consumed by the metrics planner (and any downstream callers
// that need a uniform field-ref → group-key conversion). The original
// names are preserved — module-path qualification (`super::pipeline_…`)
// already disambiguates at the call site.
pub(crate) use pipeline::field_ref_to_group_key;
pub use planner::DataFusionPlanner;
pub(crate) use selectors::{intrinsic_column, resource_attribute_lhs, span_attribute_lhs};

#[cfg(test)]
mod planner_tests;
