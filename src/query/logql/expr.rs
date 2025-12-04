//! Top-level LogQL expression types.

use super::log::LogExpr;
use super::metric::MetricExpr;

/// Top-level `LogQL` expression.
///
/// `LogQL` supports two types of queries:
/// - **Log queries** return log lines (streams)
/// - **Metric queries** return time series (numeric values over time)
#[derive(Debug, Clone, PartialEq)]
pub enum LogQLExpr {
    /// Log query: returns log streams
    ///
    /// Example: `{job="nginx"} |= "error" | json | status >= 400`
    Log(LogExpr),

    /// Metric query: returns time series
    ///
    /// Example: `sum by (status) (rate({job="nginx"} | json [5m]))`
    Metric(MetricExpr),
}

impl LogQLExpr {
    /// Creates a new log query expression.
    #[must_use]
    pub const fn log(expr: LogExpr) -> Self {
        Self::Log(expr)
    }

    /// Creates a new metric query expression.
    #[must_use]
    pub const fn metric(expr: MetricExpr) -> Self {
        Self::Metric(expr)
    }

    /// Returns true if this is a log query.
    #[must_use]
    pub const fn is_log(&self) -> bool {
        matches!(self, Self::Log(_))
    }

    /// Returns true if this is a metric query.
    #[must_use]
    pub const fn is_metric(&self) -> bool {
        matches!(self, Self::Metric(_))
    }

    /// Returns the log expression if this is a log query.
    #[must_use]
    pub const fn as_log(&self) -> Option<&LogExpr> {
        match self {
            Self::Log(expr) => Some(expr),
            Self::Metric(_) => None,
        }
    }

    /// Returns the metric expression if this is a metric query.
    #[must_use]
    pub const fn as_metric(&self) -> Option<&MetricExpr> {
        match self {
            Self::Log(_) => None,
            Self::Metric(expr) => Some(expr),
        }
    }

    /// Consumes and returns the log expression if this is a log query.
    pub fn into_log(self) -> Option<LogExpr> {
        match self {
            Self::Log(expr) => Some(expr),
            Self::Metric(_) => None,
        }
    }

    /// Consumes and returns the metric expression if this is a metric query.
    pub fn into_metric(self) -> Option<MetricExpr> {
        match self {
            Self::Log(_) => None,
            Self::Metric(expr) => Some(expr),
        }
    }
}

impl From<LogExpr> for LogQLExpr {
    fn from(expr: LogExpr) -> Self {
        Self::Log(expr)
    }
}

impl From<MetricExpr> for LogQLExpr {
    fn from(expr: MetricExpr) -> Self {
        Self::Metric(expr)
    }
}
