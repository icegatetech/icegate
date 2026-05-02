//! Parser trait for TraceQL query parsing.

use super::expr::TraceQLExpr;
use crate::error::Result;

/// Trait for parsing `TraceQL` query strings into AST representations.
///
/// Implementations parse `TraceQL` strings into the [`TraceQLExpr`] AST,
/// which the planner then translates into an execution plan.
pub trait Parser {
    /// Parse a `TraceQL` query string.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::QueryError::Parse`] when the query string is
    /// malformed (lexer / parser errors collected via the antlr error
    /// listener).
    fn parse(&self, query: &str) -> Result<TraceQLExpr>;
}
