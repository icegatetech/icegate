//! Planner trait for LogQL query planning.
//!
//! This module defines the `Planner` trait that abstracts over different
//! execution plan implementations (e.g., DataFusion LogicalPlan).

use chrono::{DateTime, TimeDelta, Utc};
use icegate_common::Result;

use super::expr::LogQLExpr;

/// Default limit for log queries when no limit is specified.
///
/// This matches Loki's default of 100 entries per the HTTP API spec:
/// <https://grafana.com/docs/loki/latest/reference/loki-http-api/>
pub const DEFAULT_LOG_LIMIT: usize = 100;

/// Trait for planning `LogQL` expressions into execution plans.
///
/// Implementations of this trait transform a [`LogQLExpr`] AST into
/// an execution plan that can be executed by a query engine.
///
/// The `Plan` associated type allows different implementations to
/// produce different plan types (e.g., `DataFusion`'s `LogicalPlan`).
///
/// # Examples
///
/// ```
/// use chrono::TimeDelta;
/// use datafusion::prelude::SessionContext;
/// use icegate_query::logql::{
///     datafusion::DataFusionPlanner,
///     planner::{Planner, QueryContext},
///     LogExpr, LogQLExpr, Selector,
/// };
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let session_context = SessionContext::new();
/// let query_context = QueryContext {
///     tenant_id: "tenant-1".to_string(),
///     start: chrono::Utc::now(),
///     end: chrono::Utc::now() + TimeDelta::hours(1),
///     limit: None,
///     step: None,
/// };
/// let planner = DataFusionPlanner::new(session_context, query_context);
/// let _plan = planner.plan(LogQLExpr::Log(LogExpr::new(Selector::empty()))).await;
/// # });
/// ```
/// Context for query planning, containing metadata like time range and limits.
#[derive(Debug, Clone)]
pub struct QueryContext {
    /// Tenant ID for multi-tenancy isolation (extracted from X-Scope-OrgID
    /// header).
    pub tenant_id: String,
    /// Start time (inclusive)
    pub start: DateTime<Utc>,
    /// End time (inclusive)
    pub end: DateTime<Utc>,
    /// Max number of results to return
    pub limit: Option<usize>,
    /// Step size for range queries
    pub step: Option<TimeDelta>,
}

/// A trait for planning `LogQL` expressions into execution plans.
pub trait Planner {
    /// The execution plan type produced by this planner.
    type Plan;

    /// Plan a `LogQL` expression into an execution plan.
    ///
    /// # Arguments
    ///
    /// * `expr` - The `LogQL` expression to plan.
    ///
    /// # Returns
    ///
    /// A result containing the execution plan or an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the expression cannot be planned, for example
    /// due to unsupported operations or invalid schema references.
    fn plan(&self, expr: LogQLExpr) -> impl std::future::Future<Output = Result<Self::Plan>> + Send;
}
