//! TraceQL query language implementation.
//!
//! Mirrors the [`crate::logql`] module: ANTLR-based parser, AST, and
//! DataFusion planner. v1 ships the full grammar parser; the planner
//! supports selectors, aggregations, and metrics-mode functions but
//! returns [`crate::error::QueryError::NotImplemented`] for hierarchy
//! operators (`>>`, `>`, `<<`, `<`, `~`), spanset boolean composition,
//! and arithmetic in field expressions.
//!
//! # Module Structure
//!
//! - [`common`] — shared operators, scopes, literals
//! - [`duration`] — duration literal parsing (re-export of `logql::duration`)
//! - [`expr`] — top-level [`TraceQLExpr`] enum
//! - [`spanset`] — span selectors and spanset operators
//! - [`metric`] — pipeline aggregations and metrics-mode functions
//! - [`parser`] — [`Parser`] trait
//! - [`planner`] — [`Planner`] trait and [`QueryContext`]
//! - [`antlr`] — ANTLR-based parser implementation
//! - [`datafusion`] — DataFusion planner implementation

/// ANTLR-based parser implementation.
pub mod antlr;
/// Shared types: comparison operators, scopes, literals, grouping.
pub mod common;
/// DataFusion implementation of the TraceQL planner.
pub mod datafusion;
/// Duration literal parsing.
pub mod duration;
/// Top-level TraceQL expression enum.
pub mod expr;
/// Pipeline aggregations and metrics-mode functions.
pub mod metric;
/// Parser trait for TraceQL query parsing.
pub mod parser;
/// Planner trait for TraceQL query planning.
pub mod planner;
/// Span selectors and spanset operators.
pub mod spanset;

pub use common::{ComparisonOp, FieldRef, IntrinsicField, LiteralValue, Scope};
pub use expr::TraceQLExpr;
pub use metric::{AggregationOp, GroupingKeys, MetricsFunction, PipelineExpr, PipelineStage};
pub use parser::Parser;
pub use planner::{DEFAULT_SEARCH_LIMIT, DEFAULT_SPANS_PER_SPANSET, Planner, QueryContext};
pub use spanset::{SpanFilter, SpanSelector, SpansetExpr, SpansetOp};
