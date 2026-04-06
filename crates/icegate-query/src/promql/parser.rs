//! Parser trait for PromQL query parsing.
//!
//! This module defines the `Parser` trait that abstracts over different
//! parsing implementations (e.g., ANTLR-based).

use super::expr::PromQLExpr;
use crate::error::Result;

/// Trait for parsing `PromQL` query strings into AST representations.
///
/// Implementations of this trait parse `PromQL` query strings and produce
/// a high-level [`PromQLExpr`] AST that can be used for further processing
/// (e.g., planning into an execution plan).
pub trait Parser {
    /// Parse a `PromQL` query string into an AST expression.
    ///
    /// # Arguments
    ///
    /// * `query` - The `PromQL` query string to parse
    ///
    /// # Returns
    ///
    /// Returns a [`PromQLExpr`] AST on success, or an error if parsing fails.
    ///
    /// # Errors
    ///
    /// Returns an error if the query string contains syntax errors or
    /// is otherwise malformed.
    fn parse(&self, query: &str) -> Result<PromQLExpr>;
}
