//! Planner trait, [`QueryContext`], and constants for TraceQL planning.

use chrono::{DateTime, TimeDelta, Utc};

use super::expr::TraceQLExpr;
use crate::error::Result;

/// Default limit for trace search results when no `limit` is specified.
///
/// Matches Tempo's default per the search API spec.
pub const DEFAULT_SEARCH_LIMIT: usize = 20;

/// Default per-trace span cap (Tempo's `spss`) when the client doesn't
/// supply one — matches the URL-parameter default documented at
/// <https://grafana.com/docs/tempo/latest/api_docs/#search>.
///
/// This is what Grafana sends by default and what the search-result
/// table renders as "Spans Limit". It is *not* the same as Tempo's
/// server-side `default_spans_per_span_set` config knob (100 by
/// default); the two are layered defaults in upstream Tempo and we
/// collapse them to one because we don't have a separate server config
/// layer yet. Setting `spss=0` on the URL disables the cap entirely.
pub const DEFAULT_SPANS_PER_SPANSET: usize = 3;

/// Context for `TraceQL` query planning.
#[derive(Debug, Clone)]
pub struct QueryContext {
    /// Tenant ID for multi-tenancy isolation (extracted from
    /// `X-Scope-OrgID` header).
    pub tenant_id: String,
    /// Start of the query time range (inclusive).
    pub start: DateTime<Utc>,
    /// End of the query time range (inclusive).
    pub end: DateTime<Utc>,
    /// Max number of traces to return for search-mode queries.
    pub limit: Option<usize>,
    /// Per-trace span cap applied in stage 2 of the search planner.
    /// `None` disables the cap (Tempo's `spss=0` semantics); `Some(n)`
    /// keeps at most `n` spans per matched trace, ranking root spans
    /// first so the search-response formatter always has the data it
    /// needs to populate `rootServiceName` / `rootTraceName`.
    pub spans_per_spanset: Option<usize>,
    /// Optional minimum trace duration filter.
    pub min_duration: Option<TimeDelta>,
    /// Optional maximum trace duration filter.
    pub max_duration: Option<TimeDelta>,
    /// Step size for metrics-mode bucketing.
    pub step: Option<TimeDelta>,
    /// Maximum number of grid points allowed per metrics query.
    pub max_grid_points: i64,
}

impl QueryContext {
    /// Default cap on grid points (matches the `LogQL` default).
    pub const DEFAULT_MAX_GRID_POINTS: i64 = 11_000;
}

/// Trait for planning `TraceQL` expressions into execution plans.
pub trait Planner {
    /// The execution plan type produced by this planner.
    type Plan;

    /// Plan a `TraceQL` expression into an execution plan.
    ///
    /// # Errors
    ///
    /// - [`crate::error::QueryError::NotImplemented`] for hierarchy operators,
    ///   spanset boolean composition between distinct selectors, and arithmetic
    ///   in field expressions.
    /// - [`crate::error::QueryError::Plan`] for invalid plan inputs.
    /// - [`crate::error::QueryError::DataFusion`] for downstream planner errors.
    fn plan(&self, expr: TraceQLExpr) -> impl std::future::Future<Output = Result<Self::Plan>> + Send;
}
