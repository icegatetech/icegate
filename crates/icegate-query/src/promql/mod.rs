//! PromQL query language implementation.
//!
//! This module provides parsing and planning for PromQL queries.
//!
//! # Module Structure
//!
//! - [`common`] - Shared types: operators, grouping, modifiers
//! - [`expr`] - Top-level [`PromQLExpr`] enum
//! - [`selector`] - Selector types: instant and matrix selectors
//! - [`aggregation`] - Aggregation types and operators
//! - [`function`] - Function call types
//! - [`binary`] - Binary/unary operations, subqueries, vector matching
//! - [`parser`] - Parser trait for parsing PromQL strings
//! - [`planner`] - Planner trait for creating execution plans

/// Aggregation types and operators
pub mod aggregation;
/// ANTLR-based parser implementation
pub mod antlr;
/// Binary/unary operations, subqueries, vector matching
pub mod binary;
/// Shared types: operators, grouping, modifiers
pub mod common;
/// DataFusion-based planner implementation
pub mod datafusion;
/// Duration parsing utilities
pub mod duration;
/// Top-level PromQL expression enum
pub mod expr;
/// Function call types
pub mod function;
/// Parser trait for PromQL query parsing
pub mod parser;
/// Planner trait for PromQL query planning
pub mod planner;
/// Selector types: instant and matrix selectors
pub mod selector;

// Re-export key types for convenient access
pub use aggregation::{Aggregation, AggregationOp};
pub use binary::{
    BinaryOp, BinaryOpModifier, BinaryOperator, GroupModifier, Subquery, UnaryOp, UnaryOperator, VectorMatching,
};
pub use common::{AtModifier, Grouping, MatchOp};
pub use expr::PromQLExpr;
pub use function::FunctionCall;
pub use parser::Parser;
pub use planner::{Planner, PromQLQueryContext};
pub use selector::{InstantSelector, LabelMatcher, MatrixSelector};
