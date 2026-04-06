//! Aggregation types for PromQL vector aggregations.

use super::{common::Grouping, expr::PromQLExpr};

/// `PromQL` aggregation operator.
///
/// These operators aggregate across dimensions (labels) to produce
/// fewer time series with aggregated values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationOp {
    /// `sum` - sum of values
    Sum,
    /// `min` - minimum value
    Min,
    /// `max` - maximum value
    Max,
    /// `avg` - average of values
    Avg,
    /// `group` - group by labels (all values become 1)
    Group,
    /// `stddev` - population standard deviation
    Stddev,
    /// `stdvar` - population standard variance
    Stdvar,
    /// `count` - count of elements
    Count,
    /// `count_values` - count of elements with same value
    CountValues,
    /// `bottomk` - bottom k elements by value
    Bottomk,
    /// `topk` - top k elements by value
    Topk,
    /// `quantile` - calculate quantile over dimensions
    Quantile,
}

impl AggregationOp {
    /// Returns the string representation of the operator.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
            Self::Group => "group",
            Self::Stddev => "stddev",
            Self::Stdvar => "stdvar",
            Self::Count => "count",
            Self::CountValues => "count_values",
            Self::Bottomk => "bottomk",
            Self::Topk => "topk",
            Self::Quantile => "quantile",
        }
    }

    /// Returns true if this operation takes a parameter before the expression.
    ///
    /// `topk(5, expr)`, `bottomk(3, expr)`, `quantile(0.95, expr)`,
    /// `count_values("name", expr)` all take a leading parameter.
    pub const fn requires_param(&self) -> bool {
        matches!(self, Self::Topk | Self::Bottomk | Self::Quantile | Self::CountValues)
    }
}

/// A `PromQL` aggregation expression.
///
/// Represents expressions like `sum by (job) (rate(http_requests_total[5m]))`.
#[derive(Debug, Clone, PartialEq)]
pub struct Aggregation {
    /// Aggregation operator
    pub op: AggregationOp,
    /// Inner expression to aggregate
    pub expr: Box<PromQLExpr>,
    /// Optional parameter (k for topk/bottomk, quantile value, label name
    /// for `count_values`)
    pub param: Option<Box<PromQLExpr>>,
    /// Optional grouping clause (`by` or `without`)
    pub grouping: Option<Grouping>,
}

impl Aggregation {
    /// Creates a new aggregation.
    pub fn new(op: AggregationOp, expr: PromQLExpr) -> Self {
        Self {
            op,
            expr: Box::new(expr),
            param: None,
            grouping: None,
        }
    }

    /// Sets the parameter for the aggregation.
    #[must_use]
    pub fn with_param(mut self, param: PromQLExpr) -> Self {
        self.param = Some(Box::new(param));
        self
    }

    /// Sets the grouping clause.
    #[must_use]
    pub fn with_grouping(mut self, grouping: Grouping) -> Self {
        self.grouping = Some(grouping);
        self
    }
}
