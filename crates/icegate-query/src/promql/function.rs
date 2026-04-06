//! Function call types for PromQL.

use super::expr::PromQLExpr;

/// A `PromQL` function call.
///
/// Represents expressions like `rate(http_requests_total[5m])` or
/// `histogram_quantile(0.95, sum(rate(http_request_duration_bucket[5m])) by (le))`.
///
/// Function names are stored as strings and validated at planning time
/// rather than parse time. This avoids a 70+ variant enum and keeps the
/// parser grammar simple.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    /// Function name (e.g., `rate`, `increase`, `histogram_quantile`)
    pub name: String,
    /// Function arguments
    pub args: Vec<PromQLExpr>,
}

impl FunctionCall {
    /// Creates a new function call.
    pub fn new(name: impl Into<String>, args: Vec<PromQLExpr>) -> Self {
        Self {
            name: name.into(),
            args,
        }
    }
}
