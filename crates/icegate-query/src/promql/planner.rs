//! Planner trait for PromQL query planning.
//!
//! This module defines the `Planner` trait that abstracts over different
//! execution plan implementations (e.g., DataFusion DataFrame).

use chrono::{DateTime, TimeDelta, Utc};

use super::expr::PromQLExpr;
use crate::error::Result;

/// Default lookback delta (staleness window) for instant queries.
///
/// Prometheus uses a 5-minute staleness window by default: if no sample
/// is found within 5 minutes of the evaluation timestamp, the series
/// is considered stale.
pub const DEFAULT_LOOKBACK_DELTA: TimeDelta = TimeDelta::minutes(5);

/// Context for `PromQL` query planning.
///
/// Contains metadata like time range, step, and tenant ID needed for
/// planning `PromQL` expressions into execution plans.
///
/// # Instant vs Range queries
///
/// - **Instant queries** evaluate at a single point in time (`time` field).
///   `start` and `end` are set equal to `time`.
/// - **Range queries** evaluate at multiple points from `start` to `end`
///   with the given `step` interval.
#[derive(Debug, Clone)]
pub struct PromQLQueryContext {
    /// Tenant ID for multi-tenancy isolation (extracted from X-Scope-OrgID
    /// header).
    pub tenant_id: String,
    /// Start time (inclusive). For instant queries, equals `time`.
    pub start: DateTime<Utc>,
    /// End time (inclusive). For instant queries, equals `time`.
    pub end: DateTime<Utc>,
    /// Step size for range queries. `None` for instant queries.
    pub step: Option<TimeDelta>,
    /// Staleness window: how far back to look for a sample when evaluating
    /// an instant vector selector. Default: 5 minutes.
    pub lookback_delta: TimeDelta,
}

impl PromQLQueryContext {
    /// Creates a context for an instant query.
    pub fn instant(tenant_id: impl Into<String>, time: DateTime<Utc>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            start: time,
            end: time,
            step: None,
            lookback_delta: DEFAULT_LOOKBACK_DELTA,
        }
    }

    /// Creates a context for a range query.
    pub fn range(tenant_id: impl Into<String>, start: DateTime<Utc>, end: DateTime<Utc>, step: TimeDelta) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            start,
            end,
            step: Some(step),
            lookback_delta: DEFAULT_LOOKBACK_DELTA,
        }
    }

    /// Sets the lookback delta (staleness window).
    #[must_use]
    pub const fn with_lookback_delta(mut self, delta: TimeDelta) -> Self {
        self.lookback_delta = delta;
        self
    }

    /// Returns true if this is an instant query (no step).
    #[must_use]
    pub const fn is_instant(&self) -> bool {
        self.step.is_none()
    }
}

/// Trait for planning `PromQL` expressions into execution plans.
///
/// Implementations transform a [`PromQLExpr`] AST into an execution
/// plan that can be executed by a query engine.
pub trait Planner {
    /// The execution plan type produced by this planner.
    type Plan;

    /// Plan a `PromQL` expression into an execution plan.
    ///
    /// # Arguments
    ///
    /// * `expr` - The `PromQL` expression to plan.
    ///
    /// # Returns
    ///
    /// A result containing the execution plan or an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the expression cannot be planned, for example
    /// due to unsupported operations or invalid schema references.
    fn plan(&self, expr: PromQLExpr) -> impl std::future::Future<Output = Result<Self::Plan>> + Send;
}
