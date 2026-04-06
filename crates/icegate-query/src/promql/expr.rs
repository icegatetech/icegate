//! Top-level PromQL expression types.

use super::{
    aggregation::Aggregation,
    binary::{BinaryOp, Subquery, UnaryOp},
    function::FunctionCall,
    selector::{InstantSelector, MatrixSelector},
};

/// Top-level `PromQL` expression.
///
/// Unlike `LogQL` which distinguishes between log queries (streams) and metric
/// queries (time series), `PromQL` is entirely metric-based. All expressions
/// evaluate to one of: instant vector, range vector, scalar, or string.
#[derive(Debug, Clone, PartialEq)]
pub enum PromQLExpr {
    /// Instant vector selector: `http_requests_total{job="api"}`
    InstantSelector(InstantSelector),

    /// Range vector (matrix) selector: `http_requests_total{job="api"}[5m]`
    MatrixSelector(MatrixSelector),

    /// Aggregation: `sum by (job) (rate(http_requests_total[5m]))`
    Aggregation(Aggregation),

    /// Function call: `rate(http_requests_total[5m])`
    FunctionCall(FunctionCall),

    /// Binary operation: `rate(a[5m]) + rate(b[5m])`
    BinaryOp(BinaryOp),

    /// Unary operation: `-http_requests_total`
    UnaryOp(UnaryOp),

    /// Subquery: `rate(http_requests_total[5m])[30m:1m]`
    Subquery(Subquery),

    /// Number literal: `42`, `3.14`
    NumberLiteral(f64),

    /// String literal: `"hello"`
    StringLiteral(String),

    /// Parenthesized expression: `(expr)`
    Parens(Box<Self>),
}

impl PromQLExpr {
    /// Creates a number literal expression.
    #[must_use]
    pub const fn number(value: f64) -> Self {
        Self::NumberLiteral(value)
    }

    /// Creates a string literal expression.
    #[must_use]
    pub fn string(value: impl Into<String>) -> Self {
        Self::StringLiteral(value.into())
    }

    /// Creates a parenthesized expression.
    #[must_use]
    pub fn parens(inner: Self) -> Self {
        Self::Parens(Box::new(inner))
    }
}

impl From<InstantSelector> for PromQLExpr {
    fn from(s: InstantSelector) -> Self {
        Self::InstantSelector(s)
    }
}

impl From<MatrixSelector> for PromQLExpr {
    fn from(s: MatrixSelector) -> Self {
        Self::MatrixSelector(s)
    }
}

impl From<Aggregation> for PromQLExpr {
    fn from(a: Aggregation) -> Self {
        Self::Aggregation(a)
    }
}

impl From<FunctionCall> for PromQLExpr {
    fn from(f: FunctionCall) -> Self {
        Self::FunctionCall(f)
    }
}

impl From<BinaryOp> for PromQLExpr {
    fn from(b: BinaryOp) -> Self {
        Self::BinaryOp(b)
    }
}

impl From<UnaryOp> for PromQLExpr {
    fn from(u: UnaryOp) -> Self {
        Self::UnaryOp(u)
    }
}

impl From<Subquery> for PromQLExpr {
    fn from(s: Subquery) -> Self {
        Self::Subquery(s)
    }
}
