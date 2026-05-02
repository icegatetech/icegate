//! Top-level TraceQL expression types.

use super::{metric::PipelineExpr, spanset::SpansetExpr};

/// Top-level `TraceQL` expression.
///
/// A query is either:
/// - A bare spanset (no pipeline) → [`TraceQLExpr::Spanset`].
/// - A spanset with one or more pipeline stages → [`TraceQLExpr::Pipeline`].
#[derive(Debug, Clone, PartialEq)]
pub enum TraceQLExpr {
    /// A bare spanset query (e.g., `{ status = error }`).
    Spanset(SpansetExpr),
    /// A spanset with pipeline stages (e.g., `{ ... } | count() > 3`).
    Pipeline(PipelineExpr),
}

impl TraceQLExpr {
    /// True if this is a metrics-mode query (terminal `| rate()`/etc).
    #[must_use]
    pub const fn is_metrics(&self) -> bool {
        matches!(self, Self::Pipeline(PipelineExpr::Metrics { .. }))
    }

    /// True if this is a search-mode query.
    #[must_use]
    pub const fn is_search(&self) -> bool {
        !self.is_metrics()
    }
}

impl From<SpansetExpr> for TraceQLExpr {
    fn from(s: SpansetExpr) -> Self {
        Self::Spanset(s)
    }
}

impl From<PipelineExpr> for TraceQLExpr {
    fn from(p: PipelineExpr) -> Self {
        Self::Pipeline(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traceql::{
        metric::MetricsFunction,
        spanset::{SpanSelector, SpansetExpr},
    };

    #[test]
    fn bare_spanset_is_search_not_metrics() {
        let e = TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector::all()));
        assert!(e.is_search());
        assert!(!e.is_metrics());
    }

    #[test]
    fn pipeline_with_rate_is_metrics() {
        let e = TraceQLExpr::Pipeline(PipelineExpr::Metrics {
            source: SpansetExpr::Selector(SpanSelector::all()),
            prelude: vec![],
            function: MetricsFunction::Rate,
            group_by: None,
        });
        assert!(e.is_metrics());
    }
}
