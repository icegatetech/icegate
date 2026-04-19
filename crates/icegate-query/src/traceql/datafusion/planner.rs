//! `DataFusion`-based `TraceQL` query planner.
//!
//! v1 supports search and metrics modes for a subset of `TraceQL`:
//! - Single span selectors with intrinsics, attribute scopes, comparisons,
//!   regex, and boolean composition.
//! - Pipeline aggregations: `count`, `sum`, `avg`, `min`, `max`,
//!   `quantile_over_time`, optionally grouped by `by(...)`, optionally
//!   filtered by `agg() OP literal`.
//! - Metrics-mode functions: `rate`, `count_over_time`,
//!   `histogram_over_time`.
//!
//! Hierarchy operators (`>>`, `>`, `<<`, `<`, `~`), spanset boolean
//! composition between distinct selectors (`{A} && {B}`), and arithmetic
//! in field expressions all parse but return
//! [`crate::error::QueryError::NotImplemented`] from this planner.

use datafusion::{
    logical_expr::{Expr, col, lit},
    prelude::{DataFrame, SessionContext},
    scalar::ScalarValue,
};
use icegate_common::{
    SPANS_TABLE_FQN,
    schema::{COL_TENANT_ID, COL_TIMESTAMP},
};

use super::{metrics, pipeline, selectors};
use crate::{
    error::{QueryError, Result},
    traceql::{
        expr::TraceQLExpr,
        metric::PipelineExpr,
        planner::{DEFAULT_SEARCH_LIMIT, Planner, QueryContext},
    },
};

/// `DataFusion`-based planner for `TraceQL` queries.
pub struct DataFusionPlanner {
    session_ctx: SessionContext,
    query_ctx: QueryContext,
}

impl DataFusionPlanner {
    /// Create a new planner.
    #[must_use]
    pub const fn new(session_ctx: SessionContext, query_ctx: QueryContext) -> Self {
        Self { session_ctx, query_ctx }
    }
}

impl Planner for DataFusionPlanner {
    type Plan = DataFrame;

    #[tracing::instrument(skip_all)]
    async fn plan(&self, expr: TraceQLExpr) -> Result<Self::Plan> {
        match expr {
            TraceQLExpr::Spanset(s) => {
                let df = self.scan_spans().await?;
                let df = self.apply_tenant_and_time(df)?;
                let df = selectors::apply_spanset(df, s)?;
                let limit = self.query_ctx.limit.unwrap_or(DEFAULT_SEARCH_LIMIT);
                let df = df.sort(vec![col(COL_TIMESTAMP).sort(false, true)])?;
                Ok(df.limit(0, Some(limit))?)
            }
            TraceQLExpr::Pipeline(PipelineExpr::Search { source, stages }) => {
                let df = self.scan_spans().await?;
                let df = self.apply_tenant_and_time(df)?;
                let df = selectors::apply_spanset(df, source)?;
                pipeline::apply_search_pipeline(df, &stages, &self.query_ctx)
            }
            TraceQLExpr::Pipeline(PipelineExpr::Metrics {
                source,
                prelude,
                function,
                group_by,
            }) => {
                let df = self.scan_spans().await?;
                let df = self.apply_tenant_and_time(df)?;
                let df = selectors::apply_spanset(df, source)?;
                metrics::apply_metrics(df, &prelude, &function, group_by.as_ref(), &self.query_ctx)
            }
        }
    }
}

impl DataFusionPlanner {
    async fn scan_spans(&self) -> Result<DataFrame> {
        let df = self.session_ctx.table(SPANS_TABLE_FQN).await?;
        Ok(strip_schema_metadata(df)?)
    }

    fn apply_tenant_and_time(&self, df: DataFrame) -> Result<DataFrame> {
        let tenant_filter = col(COL_TENANT_ID).eq(lit(&self.query_ctx.tenant_id));
        let df = df.filter(tenant_filter)?;
        let start_lit = lit(ScalarValue::TimestampMicrosecond(
            Some(self.query_ctx.start.timestamp_micros()),
            None,
        ));
        let end_lit = lit(ScalarValue::TimestampMicrosecond(
            Some(self.query_ctx.end.timestamp_micros()),
            None,
        ));
        let df = df.filter(col(COL_TIMESTAMP).gt_eq(start_lit).and(col(COL_TIMESTAMP).lt_eq(end_lit)))?;
        Ok(df)
    }
}

/// Strip Iceberg PARQUET field metadata (matches the `LogQL` planner trick).
fn strip_schema_metadata(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    let select_exprs: Vec<Expr> = df.schema().inner().fields().iter().map(|f| col(f.name().as_str())).collect();
    df.select(select_exprs)
}

/// Convenience: explicit error for unsupported v1 features.
pub(crate) fn not_implemented(feature: &str) -> QueryError {
    QueryError::NotImplemented(format!("TraceQL feature not yet supported in planner v1: {feature}"))
}
