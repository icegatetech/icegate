//! `DataFusion` implementation of the `TraceQL` planner.

mod metrics;
mod pipeline;
pub mod planner;
mod registry;
mod selectors;

// Re-exports consumed by the metrics planner (and any downstream callers
// that need a uniform field-ref → group-key conversion).
pub(crate) use pipeline::field_ref_to_group_key as pipeline_field_ref_to_group_key;
pub use planner::DataFusionPlanner;
pub(crate) use selectors::{
    intrinsic_column as selectors_intrinsic_column, resource_attribute_lhs as selectors_resource_attribute_lhs,
    span_attribute_lhs as selectors_span_attribute_lhs,
};

#[cfg(test)]
mod planner_tests;
