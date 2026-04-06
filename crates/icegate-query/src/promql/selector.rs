//! Selector types for PromQL instant and matrix selectors.

use chrono::TimeDelta;

use super::common::{AtModifier, MatchOp};

/// A label matcher in a selector.
///
/// Matches a label name against a value using the specified operator.
/// For example, `job="api"` or `status=~"5.."`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    /// Label name
    pub name: String,
    /// Match operator
    pub op: MatchOp,
    /// Match value (string literal or regex pattern)
    pub value: String,
}

impl LabelMatcher {
    /// Creates a new label matcher.
    pub fn new(name: impl Into<String>, op: MatchOp, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op,
            value: value.into(),
        }
    }
}

/// An instant vector selector.
///
/// Selects the most recent sample for each time series matching the metric
/// name and label matchers. For example: `http_requests_total{job="api"}`.
///
/// The `metric_name` is `Option` because `PromQL` allows selectors without
/// an explicit metric name: `{__name__="http_requests_total"}`.
#[derive(Debug, Clone, PartialEq)]
pub struct InstantSelector {
    /// Metric name (e.g., `http_requests_total`). None when using
    /// `{__name__="..."}` syntax.
    pub metric_name: Option<String>,
    /// Label matchers
    pub matchers: Vec<LabelMatcher>,
    /// Optional offset modifier: `offset 5m`
    pub offset: Option<TimeDelta>,
    /// Optional `@` modifier for evaluation time override
    pub at: Option<AtModifier>,
}

impl InstantSelector {
    /// Creates a new instant selector with a metric name.
    pub fn new(metric_name: impl Into<String>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers: Vec::new(),
            offset: None,
            at: None,
        }
    }

    /// Creates a new instant selector without a metric name (matcher-only).
    pub const fn from_matchers(matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: None,
            matchers,
            offset: None,
            at: None,
        }
    }

    /// Creates a new instant selector with a metric name and matchers.
    pub fn with_matchers(metric_name: impl Into<String>, matchers: Vec<LabelMatcher>) -> Self {
        Self {
            metric_name: Some(metric_name.into()),
            matchers,
            offset: None,
            at: None,
        }
    }

    /// Sets the offset modifier.
    #[must_use]
    pub const fn with_offset(mut self, offset: TimeDelta) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Sets the `@` modifier.
    #[must_use]
    pub const fn with_at(mut self, at: AtModifier) -> Self {
        self.at = Some(at);
        self
    }
}

/// A range vector (matrix) selector.
///
/// Selects all samples within a time range for each matching time series.
/// For example: `http_requests_total{job="api"}[5m]`.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixSelector {
    /// The underlying instant selector
    pub selector: InstantSelector,
    /// Range duration (e.g., `5m`)
    pub range: TimeDelta,
}

impl MatrixSelector {
    /// Creates a new matrix selector.
    pub const fn new(selector: InstantSelector, range: TimeDelta) -> Self {
        Self { selector, range }
    }
}
