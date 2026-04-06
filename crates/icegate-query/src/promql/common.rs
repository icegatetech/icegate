//! Common types shared across PromQL expression modules.

pub use crate::error::{ParseError, QueryError, parse_error};

/// Match operator for label matchers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchOp {
    /// `=` exact equality
    Eq,
    /// `!=` not equal
    Neq,
    /// `=~` regex match
    Re,
    /// `!~` regex not match
    Nre,
}

impl MatchOp {
    /// Get the string representation of the operator.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Neq => "!=",
            Self::Re => "=~",
            Self::Nre => "!~",
        }
    }
}

/// Grouping clause for aggregations.
///
/// `PromQL` aggregations support `by` and `without` clauses to control
/// which labels are preserved in the output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grouping {
    /// `by (label1, label2, ...)` - aggregate by these labels only
    By(Vec<String>),
    /// `without (label1, label2, ...)` - aggregate by all labels except these
    Without(Vec<String>),
}

/// `@` modifier for specifying evaluation time.
///
/// Allows overriding the evaluation timestamp for a selector or subquery.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AtModifier {
    /// `@ <unix_timestamp>` - evaluate at specific timestamp (seconds)
    Timestamp(f64),
    /// `@ start()` - evaluate at query start time
    Start,
    /// `@ end()` - evaluate at query end time
    End,
}
