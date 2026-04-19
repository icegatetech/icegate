//! Planner trait, [`QueryContext`], and constants for TraceQL planning.

use chrono::{DateTime, TimeDelta, Utc};

use super::expr::TraceQLExpr;
use crate::error::Result;

/// Default limit for trace search results when no `limit` is specified.
///
/// Matches Tempo's default per the search API spec.
pub const DEFAULT_SEARCH_LIMIT: usize = 20;

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
