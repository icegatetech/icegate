//! Parser trait for LogQL query parsing.
//!
//! This module defines the `Parser` trait that abstracts over different
//! parsing implementations (e.g., ANTLR-based).

use super::expr::LogQLExpr;
use crate::common::Result;

/// Trait for parsing `LogQL` query strings into AST representations.
///
/// Implementations of this trait parse `LogQL` query strings and produce
/// a high-level [`LogQLExpr`] AST that can be used for further processing
/// (e.g., planning into an execution plan).
pub trait Parser {
    /// Parse a `LogQL` query string into an AST expression.
    ///
    /// # Arguments
    ///
    /// * `query` - The `LogQL` query string to parse
    ///
    /// # Returns
    ///
    /// Returns a [`LogQLExpr`] AST on success, or an error if parsing fails.
    ///
    /// # Errors
    ///
    /// Returns an error if the query string contains syntax errors or
    /// is otherwise malformed.
    fn parse(&self, query: &str) -> Result<LogQLExpr>;
}
