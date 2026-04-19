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
    arrow::array::{Array, AsArray, RecordBatch},
    functions_aggregate::expr_fn::max,
    functions_window::expr_fn::row_number,
    logical_expr::{Expr, ExprFunctionExt, col, lit},
    prelude::{DataFrame, SessionContext},
    scalar::ScalarValue,
};
use icegate_common::{
    SPANS_TABLE_FQN,
    schema::{COL_DURATION_MICROS, COL_PARENT_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID},
};

use super::{metrics, pipeline, selectors};
use crate::{
    error::{QueryError, Result},
    traceql::{
        expr::TraceQLExpr,
        metric::PipelineExpr,
        planner::{DEFAULT_SEARCH_LIMIT, Planner, QueryContext},
        spanset::SpansetExpr,
    },
};

/// Alias used to sort the stage-1 trace summary by recency before
/// applying the trace-count limit.
const TRACE_LAST_SEEN_ALIAS: &str = "__last_seen";

/// Alias used by stage 2 to rank a trace's spans before applying the
/// per-trace `spss` cap. Dropped from the final projection.
const SPAN_RANK_ALIAS: &str = "__span_rank";

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
            // Search-mode (`/api/search`). Tempo's `limit` parameter
            // counts **traces**, not spans — but a `df.limit(N)` on the
            // filtered span DataFrame caps span rows. With the default
            // `limit=20` and any trace whose matched-span count exceeds
            // a row or two, the row cap silently truncates traces:
            // partial spans for the *N*-th matched trace make it into
            // the result, the rest of that trace's spans (often
            // including the root) are dropped, and the search summary
            // ends up with empty `rootServiceName` / `rootTraceName`.
            //
            // Fix: two-stage plan.
            //   Stage 1 — over the filtered span set, group by
            //     `trace_id` and take the most recent N traces by
            //     `MAX(timestamp)`.
            //   Stage 2 — re-scan the spans table for *all* spans
            //     belonging to those trace IDs (tenant + time predicate
            //     reapplied). The original spanset filter is dropped on
            //     this side because we want every span of the matched
            //     traces — non-matching siblings (notably the root)
            //     are needed by the search-response formatter.
            TraceQLExpr::Spanset(s) => {
                let limit = self.query_ctx.limit.unwrap_or(DEFAULT_SEARCH_LIMIT);
                let trace_ids = self.collect_recent_trace_ids(s, limit).await?;
                self.fetch_spans_for_trace_ids(&trace_ids).await
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

    /// Stage 1 of the search plan: identify the most recent `limit`
    /// distinct trace IDs whose spans match the filter `selector`.
    ///
    /// Aggregates the filtered span set by `trace_id`, ranks each
    /// resulting trace by its most-recent span (`MAX(timestamp)`), and
    /// keeps the top `limit`. This collapses the row-cap problem from
    /// "first N matching spans" to "first N matching traces", which is
    /// the contract Tempo's `limit` query parameter promises.
    async fn collect_recent_trace_ids(&self, selector: SpansetExpr, limit: usize) -> Result<Vec<String>> {
        let df = self.scan_spans().await?;
        let df = self.apply_tenant_and_time(df)?;
        let df = selectors::apply_spanset(df, selector)?;

        let trace_ids_df = df
            .aggregate(
                vec![col(COL_TRACE_ID)],
                vec![max(col(COL_TIMESTAMP)).alias(TRACE_LAST_SEEN_ALIAS)],
            )?
            .sort(vec![col(TRACE_LAST_SEEN_ALIAS).sort(false, true)])?
            .limit(0, Some(limit))?;

        let batches = trace_ids_df.collect().await?;
        Ok(extract_trace_ids(&batches))
    }

    /// Stage 2 of the search plan: re-scan the spans table for every
    /// span belonging to `trace_ids`, dropping the original spanset
    /// filter so non-matching siblings of matched spans (notably the
    /// root) are present in the result. Tenant + time predicate is
    /// reapplied so cross-tenant leakage is impossible even with a
    /// hand-crafted trace ID.
    ///
    /// An empty `trace_ids` list short-circuits to an empty `DataFrame`
    /// with the spans-table schema (via `WHERE false`), saving a
    /// pointless full-table scan when stage 1 returned nothing.
    async fn fetch_spans_for_trace_ids(&self, trace_ids: &[String]) -> Result<DataFrame> {
        let df = self.scan_spans().await?;
        let df = self.apply_tenant_and_time(df)?;
        let df = if trace_ids.is_empty() {
            df.filter(lit(false))?
        } else {
            let id_lits: Vec<Expr> = trace_ids.iter().cloned().map(lit).collect();
            df.filter(col(COL_TRACE_ID).in_list(id_lits, false))?
        };
        let df = self.apply_spans_per_spanset(df)?;
        Ok(df.sort(vec![col(COL_TIMESTAMP).sort(false, true)])?)
    }

    /// Apply Tempo's per-trace span cap (`spss`) via a windowed
    /// `ROW_NUMBER()` per `trace_id`. Spans are ranked
    /// **root-first, longest-first**:
    ///
    ///   1. `parent_span_id IS NULL OR parent_span_id = ''` DESC — a
    ///      true OTLP root always outranks a non-root sibling.
    ///   2. `duration_micros` DESC — within the same root tier, the
    ///      longest-duration span wins. A trace's real root
    ///      encompasses every descendant in time, so if a trace
    ///      somehow ends up with more than one null-parent span (e.g.
    ///      cross-service propagation quirks, partial ingest) the
    ///      outermost one has the biggest duration and stays at rank 1.
    ///   3. `timestamp` ASC — last-resort tiebreak for spans with
    ///      identical duration.
    ///
    /// This guarantees that even with `spss=1` the surviving span
    /// carries enough metadata for the search-response formatter to
    /// fill in `rootServiceName` / `rootTraceName` without picking a
    /// short sibling over the real root.
    ///
    /// Returns the input `DataFrame` unchanged when
    /// `query_ctx.spans_per_spanset` is `None` (Tempo's `spss=0`
    /// "unlimited" semantics).
    fn apply_spans_per_spanset(&self, df: DataFrame) -> Result<DataFrame> {
        let Some(cap) = self.query_ctx.spans_per_spanset else {
            return Ok(df);
        };
        // Boolean expression that evaluates to true for root spans
        // (null or empty `parent_span_id`). Sorting it DESC pins
        // roots to rank 1.
        let parent = col(COL_PARENT_SPAN_ID);
        let is_root_indicator = parent.clone().is_null().or(parent.eq(lit("")));
        let cap_i64 = i64::try_from(cap).unwrap_or(i64::MAX);

        let row_num = row_number()
            .partition_by(vec![col(COL_TRACE_ID)])
            .order_by(vec![
                is_root_indicator.sort(false, true),
                col(COL_DURATION_MICROS).sort(false, false),
                col(COL_TIMESTAMP).sort(true, true),
            ])
            .build()?
            .alias(SPAN_RANK_ALIAS);

        Ok(df
            .window(vec![row_num])?
            .filter(col(SPAN_RANK_ALIAS).lt_eq(lit(cap_i64)))?
            .drop_columns(&[SPAN_RANK_ALIAS])?)
    }
}

/// Extract the `trace_id` string column from stage-1 result batches
/// into a flat `Vec<String>`. Drops null entries — `trace_id` is
/// `NOT NULL` in the spans schema so this should not happen in
/// practice, but defending against it keeps the IN list well-formed.
fn extract_trace_ids(batches: &[RecordBatch]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for batch in batches {
        let Some((idx, _)) = batch.schema().column_with_name(COL_TRACE_ID) else {
            continue;
        };
        let column = batch.column(idx).as_string::<i32>();
        for row in 0..column.len() {
            if column.is_valid(row) {
                out.push(column.value(row).to_string());
            }
        }
    }
    out
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
