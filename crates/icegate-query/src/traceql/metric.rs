//! Pipeline aggregations and metrics-mode functions.
//!
//! TraceQL has two query shapes that share the `|` pipeline operator:
//!
//! - **Search pipeline** ([`PipelineExpr::Search`]) returns spansets,
//!   optionally filtered by an aggregate predicate (`| count() > 3`).
//! - **Metrics mode** ([`PipelineExpr::Metrics`]) returns bucketed time
//!   series for Grafana metrics graphs (`| rate() by (.service)`).
//!
//! The dispatch happens at the terminal stage: any pipeline whose final
//! stage is a [`MetricsFunction`] becomes [`PipelineExpr::Metrics`];
//! everything else is [`PipelineExpr::Search`].

use super::{
    common::{ComparisonOp, FieldRef, LiteralValue},
    spanset::SpansetExpr,
};

/// Top-level pipeline expression: spanset followed by zero or more stages.
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineExpr {
    /// Search-mode pipeline returning spansets.
    Search {
        /// Source spanset.
        source: SpansetExpr,
        /// Pipeline stages applied left-to-right.
        stages: Vec<PipelineStage>,
    },
    /// Metrics-mode pipeline returning bucketed time series.
    Metrics {
        /// Source spanset.
        source: SpansetExpr,
        /// Optional non-terminal aggregation stages applied before the
        /// metrics function (e.g., `| by(.svc) | rate()` is valid in
        /// some Tempo dialects; for v1, only `by` is allowed here).
        prelude: Vec<PipelineStage>,
        /// The terminal metrics function.
        function: MetricsFunction,
        /// Optional `by(...)` grouping for the metrics function.
        group_by: Option<GroupingKeys>,
    },
}

/// A single stage in a search-mode pipeline.
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineStage {
    /// `by(.scope.attr, .other)` — grouping keys for subsequent aggregations.
    By(GroupingKeys),
    /// `count()`, `sum(.attr)`, `avg(.attr)`, etc. — produces an aggregated
    /// row per group (or per trace if no preceding `by`).
    Aggregate {
        /// The aggregation operator.
        op: AggregationOp,
        /// Field argument (`None` for `count()`; required for others).
        arg: Option<FieldRef>,
    },
    /// `count() > 3`, `avg(duration) < 100ms` — filter the aggregated rows
    /// by comparing the aggregate result to a literal.
    AggregateFilter {
        /// The aggregation operator that produces the LHS.
        op: AggregationOp,
        /// Optional argument to that aggregation.
        arg: Option<FieldRef>,
        /// Comparison operator.
        cmp: ComparisonOp,
        /// RHS literal.
        value: LiteralValue,
    },
}

/// Grouping keys for `by(...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingKeys {
    /// Keys (field references) to group by.
    pub keys: Vec<FieldRef>,
}

/// Search-mode aggregation operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationOp {
    /// `count()`.
    Count,
    /// `sum(.attr)`.
    Sum,
    /// `avg(.attr)`.
    Avg,
    /// `min(.attr)`.
    Min,
    /// `max(.attr)`.
    Max,
    /// `quantile_over_time(.attr, p)`.
    Quantile,
}

impl AggregationOp {
    /// Source-form spelling of the operator (without parentheses).
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Quantile => "quantile_over_time",
        }
    }

    /// True if the operator requires a field argument.
    #[must_use]
    pub const fn requires_arg(&self) -> bool {
        !matches!(self, Self::Count)
    }
}

/// Metrics-mode terminal function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricsFunction {
    /// `rate()` — count divided by range duration, bucketed by step.
    Rate,
    /// `count_over_time()` — bucketed count of matching spans.
    CountOverTime,
    /// `histogram_over_time(.attr)` — bucketed histogram of attribute values.
    HistogramOverTime {
        /// The numeric field to histogram.
        field: FieldRef,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traceql::{
        common::IntrinsicField,
        spanset::{SpanSelector, SpansetExpr},
    };

    #[test]
    fn aggregation_op_count_does_not_require_arg() {
        assert!(!AggregationOp::Count.requires_arg());
        assert!(AggregationOp::Avg.requires_arg());
    }

    #[test]
    fn build_search_pipeline_with_count_filter() {
        let p = PipelineExpr::Search {
            source: SpansetExpr::Selector(SpanSelector::all()),
            stages: vec![PipelineStage::AggregateFilter {
                op: AggregationOp::Count,
                arg: None,
                cmp: ComparisonOp::Gt,
                value: LiteralValue::Int(3),
            }],
        };
        assert!(matches!(p, PipelineExpr::Search { .. }));
    }

    #[test]
    fn build_metrics_pipeline_rate_by_intrinsic() {
        let p = PipelineExpr::Metrics {
            source: SpansetExpr::Selector(SpanSelector::all()),
            prelude: vec![],
            function: MetricsFunction::Rate,
            group_by: Some(GroupingKeys {
                keys: vec![FieldRef::Intrinsic(IntrinsicField::RootServiceName)],
            }),
        };
        assert!(matches!(p, PipelineExpr::Metrics { .. }));
    }
}
