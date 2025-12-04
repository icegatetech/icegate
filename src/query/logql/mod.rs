//! LogQL query language implementation.
//!
//! This module provides parsing and planning for LogQL queries.
//!
//! # Module Structure
//!
//! - [`common`] - Shared types: Duration, operators, grouping
//! - [`expr`] - Top-level [`LogQLExpr`] enum
//! - [`log`] - Log query types: selectors, pipeline stages
//! - [`metric`] - Metric query types: aggregations, binary operations
//! - [`parser`] - Parser trait for parsing LogQL strings
//! - [`planner`] - Planner trait for creating execution plans
//! - [`antlr`] - ANTLR-based parser implementation

/// ANTLR-based parser implementation
pub mod antlr;
/// Shared types: Duration, operators, grouping, label extraction
pub mod common;
/// DataFusion implementation of the LogQL planner
pub mod datafusion;
/// Top-level LogQL expression enum
pub mod expr;
/// Log query types: selectors, pipeline stages, filters
pub mod log;
/// Metric query types: aggregations, binary operations
pub mod metric;
/// Parser trait for LogQL query parsing
pub mod parser;
/// Planner trait for LogQL query planning
pub mod planner;

// Re-export key types for convenient access
pub use common::{ComparisonOp, Duration, Grouping, GroupingLabel, LabelExtraction, LabelFormatOp, MatchOp};
pub use expr::LogQLExpr;
pub use log::{
    LabelFilterExpr, LabelMatcher, LineFilter, LineFilterOp, LineFilterValue, LogExpr, LogParser, PipelineStage,
    Selector, UnwrapConversion, UnwrapExpr,
};
pub use metric::{
    AtModifier, BinaryOp, BinaryOpModifier, MatchingLabels, MetricExpr, RangeAggregation, RangeAggregationOp,
    RangeExpr, VectorAggregation, VectorAggregationOp, VectorMatchCardinality, VectorMatching,
};
pub use parser::Parser;
pub use planner::Planner;
