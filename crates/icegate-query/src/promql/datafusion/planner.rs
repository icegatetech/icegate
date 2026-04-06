//! DataFusion-based `PromQL` query planner.
//!
//! Converts [`PromQLExpr`] AST nodes into DataFusion [`DataFrame`] plans
//! using the DataFrame API exclusively (no raw SQL).
//!
//! # Phase 1 Scope
//!
//! - Instant selectors with label matchers (eq, neq, re, nre)
//! - Matrix selectors (full sample range)
//! - Basic aggregations: sum, avg, count, min, max
//! - Scalar-vector binary ops (one side is a number literal)
//! - Unary negation
//! - Number/string literals, parenthesized expressions
//!
//! Operations not yet supported return [`QueryError::NotImplemented`].

use datafusion::{
    arrow::datatypes::DataType,
    functions_aggregate::expr_fn::{avg, count, max, min, sum},
    functions_nested::{map_keys::map_keys, map_values::map_values, string::array_to_string},
    logical_expr::{Expr, col, lit},
    prelude::*,
    scalar::ScalarValue,
};
use icegate_common::METRICS_TABLE_FQN;

use crate::{
    error::{QueryError, Result},
    promql::{
        AggregationOp, BinaryOperator, Grouping, MatchOp, PromQLExpr, UnaryOperator,
        planner::{Planner, PromQLQueryContext},
        selector::{InstantSelector, LabelMatcher, MatrixSelector},
    },
};

/// Strips PARQUET field metadata from a `DataFrame` schema.
///
/// Required because Iceberg schemas include `PARQUET:field_id` metadata,
/// but in-memory operations create fields without it. This prevents
/// Arrow schema mismatch errors during joins/unions.
fn strip_schema_metadata(df: DataFrame) -> datafusion::error::Result<DataFrame> {
    let select_exprs: Vec<Expr> = df
        .schema()
        .inner()
        .fields()
        .iter()
        .map(|field| col(field.name().as_str()))
        .collect();

    df.select(select_exprs)
}

/// Builds a filter expression for a single label matcher against the
/// `attributes` MAP column.
///
/// Uses `get_field` for map key access, consistent with the `LogQL` planner.
fn label_matcher_expr(matcher: &LabelMatcher) -> Expr {
    let col_expr = datafusion::functions::core::get_field().call(vec![col("attributes"), lit(matcher.name.as_str())]);

    match matcher.op {
        MatchOp::Eq => col_expr.eq(lit(matcher.value.as_str())),
        MatchOp::Neq => col_expr.not_eq(lit(matcher.value.as_str())),
        // regexp_like returns a boolean: true if the pattern matches
        MatchOp::Re => datafusion::functions::regex::regexp_like().call(vec![col_expr, lit(matcher.value.as_str())]),
        MatchOp::Nre => datafusion::functions::regex::regexp_like()
            .call(vec![col_expr, lit(matcher.value.as_str())])
            .not(),
    }
}

/// A planner that converts `PromQL` expressions into DataFusion `DataFrame`s.
///
/// This is the Phase 1 implementation covering basic selectors, simple
/// aggregations, scalar binary ops, and literals. Unsupported operations
/// return [`QueryError::NotImplemented`].
pub struct DataFusionPlanner {
    session_ctx: SessionContext,
    query_ctx: PromQLQueryContext,
}

impl DataFusionPlanner {
    /// Creates a new `DataFusionPlanner`.
    pub const fn new(session_ctx: SessionContext, query_ctx: PromQLQueryContext) -> Self {
        Self { session_ctx, query_ctx }
    }
}

impl Planner for DataFusionPlanner {
    type Plan = DataFrame;

    #[tracing::instrument(skip_all)]
    async fn plan(&self, expr: PromQLExpr) -> Result<Self::Plan> {
        self.plan_expr(expr).await
    }
}

impl DataFusionPlanner {
    /// Recursively plans a `PromQL` expression into a `DataFrame`.
    ///
    /// Uses `Box::pin` to handle recursive async calls, since expressions
    /// like aggregations and binary ops contain inner expressions.
    fn plan_expr(
        &self,
        expr: PromQLExpr,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<DataFrame>> + Send + '_>> {
        Box::pin(async move {
            match expr {
                PromQLExpr::InstantSelector(sel) => self.plan_instant_selector(&sel).await,
                PromQLExpr::MatrixSelector(sel) => self.plan_matrix_selector(&sel).await,
                PromQLExpr::Aggregation(agg) => self.plan_aggregation(agg).await,
                PromQLExpr::FunctionCall(func) => self.plan_function_call(&func).await,
                PromQLExpr::BinaryOp(binop) => self.plan_binary_op(binop).await,
                PromQLExpr::UnaryOp(unary) => self.plan_unary_op(unary).await,
                PromQLExpr::Subquery(_) => Err(QueryError::NotImplemented("subquery expressions".to_string())),
                PromQLExpr::NumberLiteral(val) => self.plan_number_literal(val),
                PromQLExpr::StringLiteral(val) => self.plan_string_literal(&val),
                PromQLExpr::Parens(inner) => self.plan_expr(*inner).await,
            }
        })
    }

    // ========================================================================
    // Base scan
    // ========================================================================

    /// Builds the base scan `DataFrame` with mandatory tenant and time range
    /// filters, metric name filter, and value coalescing.
    ///
    /// This is the foundation used by both instant and matrix selectors.
    async fn base_scan(
        &self,
        metric_name: Option<&str>,
        matchers: &[LabelMatcher],
        start: ScalarValue,
        end: ScalarValue,
    ) -> Result<DataFrame> {
        // 1. Scan the metrics table
        let df = self.session_ctx.table(METRICS_TABLE_FQN).await?;

        // 2. Strip Iceberg PARQUET metadata to prevent schema mismatch
        let df = strip_schema_metadata(df)?;

        // 3. Mandatory tenant filter
        let df = df.filter(col("tenant_id").eq(lit(self.query_ctx.tenant_id.as_str())))?;

        // 4. Time range filter
        let df = df.filter(col("timestamp").gt_eq(lit(start)).and(col("timestamp").lt_eq(lit(end))))?;

        // 5. Metric name filter (if provided)
        let df = if let Some(name) = metric_name {
            df.filter(col("metric_name").eq(lit(name)))?
        } else {
            df
        };

        // 6. Label matcher filters on attributes MAP
        let df = matchers
            .iter()
            .try_fold(df, |df, matcher| df.filter(label_matcher_expr(matcher)))?;

        // 7. Value coalesce: prefer value_double, fall back to value_int cast to f64
        let df = df.select(vec![
            col("timestamp"),
            col("metric_name"),
            col("attributes"),
            coalesce(vec![col("value_double"), cast(col("value_int"), DataType::Float64)]).alias("value"),
        ])?;

        Ok(df)
    }

    // ========================================================================
    // Instant selector
    // ========================================================================

    /// Plans an instant vector selector.
    ///
    /// Scans with the lookback window applied, then uses `ROW_NUMBER()` window
    /// function partitioned by (`metric_name`, attributes) ordered by timestamp
    /// DESC to select the most recent sample per series.
    async fn plan_instant_selector(&self, sel: &InstantSelector) -> Result<DataFrame> {
        // For instant queries, expand the start by lookback_delta to find
        // stale samples within the staleness window.
        let lookback_us = self.query_ctx.lookback_delta.num_microseconds().unwrap_or(300_000_000);
        let start_us = self.query_ctx.start.timestamp_micros() - lookback_us;
        let end_us = self.query_ctx.end.timestamp_micros();

        let start = ScalarValue::TimestampMicrosecond(Some(start_us), Some("UTC".into()));
        let end = ScalarValue::TimestampMicrosecond(Some(end_us), Some("UTC".into()));

        let df = self.base_scan(sel.metric_name.as_deref(), &sel.matchers, start, end).await?;

        // Use ROW_NUMBER() to get the most recent sample per series.
        // Series identity = (metric_name, attributes).
        //
        // MAP columns cannot be used directly in PARTITION BY, so we
        // serialize keys and values to strings using array_to_string
        // (same approach as the LogQL planner).
        let attr_keys_str = array_to_string(map_keys(col("attributes")), lit("|||")).alias("_attr_keys");
        let attr_vals_str = array_to_string(map_values(col("attributes")), lit("|||")).alias("_attr_vals");

        let df = df.select(vec![
            col("timestamp"),
            col("metric_name"),
            col("attributes"),
            col("value"),
            attr_keys_str,
            attr_vals_str,
        ])?;

        let row_num_expr = datafusion::functions_window::row_number::row_number()
            .partition_by(vec![col("metric_name"), col("_attr_keys"), col("_attr_vals")])
            .order_by(vec![col("timestamp").sort(false, true)])
            .build()?
            .alias("row_num");

        let df = df.select(vec![
            col("timestamp"),
            col("metric_name"),
            col("attributes"),
            col("value"),
            col("_attr_keys"),
            col("_attr_vals"),
            row_num_expr,
        ])?;

        // Keep only the most recent sample per series
        let df = df.filter(col("row_num").eq(lit(1i64)))?;

        // Drop the helper column
        let df = df.select(vec![
            col("timestamp"),
            col("metric_name"),
            col("attributes"),
            col("value"),
        ])?;

        Ok(df)
    }

    // ========================================================================
    // Matrix selector
    // ========================================================================

    /// Plans a range vector (matrix) selector.
    ///
    /// Returns all samples within the range window for each matching series.
    /// The time range is expanded by the matrix range duration.
    async fn plan_matrix_selector(&self, sel: &MatrixSelector) -> Result<DataFrame> {
        let range_us = sel.range.num_microseconds().unwrap_or(0);
        let start_us = self.query_ctx.start.timestamp_micros() - range_us;
        let end_us = self.query_ctx.end.timestamp_micros();

        let start = ScalarValue::TimestampMicrosecond(Some(start_us), Some("UTC".into()));
        let end = ScalarValue::TimestampMicrosecond(Some(end_us), Some("UTC".into()));

        self.base_scan(sel.selector.metric_name.as_deref(), &sel.selector.matchers, start, end)
            .await
    }

    // ========================================================================
    // Aggregation
    // ========================================================================

    /// Plans an aggregation expression.
    ///
    /// Phase 1 supports: sum, avg, count, min, max.
    /// Other aggregation operators return `NotImplemented`.
    async fn plan_aggregation(&self, agg: crate::promql::aggregation::Aggregation) -> Result<DataFrame> {
        let inner_df = self.plan_expr(*agg.expr).await?;

        // Build the aggregate function expression
        let agg_expr = match agg.op {
            AggregationOp::Sum => sum(col("value")).alias("value"),
            AggregationOp::Avg => avg(col("value")).alias("value"),
            AggregationOp::Count => count(col("value")).alias("value"),
            AggregationOp::Min => min(col("value")).alias("value"),
            AggregationOp::Max => max(col("value")).alias("value"),
            other => {
                return Err(QueryError::NotImplemented(format!(
                    "aggregation operator '{}'",
                    other.as_str()
                )));
            }
        };

        // Build group-by expressions from the grouping clause.
        // For `by(labels)`, group by the specified label values extracted from
        // the attributes MAP. For `without(labels)`, Phase 1 returns
        // NotImplemented since it requires enumerating all label keys.
        let group_exprs = match &agg.grouping {
            Some(Grouping::By(labels)) => labels
                .iter()
                .map(|label| {
                    datafusion::functions::core::get_field()
                        .call(vec![col("attributes"), lit(label.as_str())])
                        .alias(label.as_str())
                })
                .collect::<Vec<_>>(),
            Some(Grouping::Without(_)) => {
                return Err(QueryError::NotImplemented(
                    "aggregation with 'without' clause (requires enumerating all label keys)".to_string(),
                ));
            }
            None => vec![],
        };

        let df = inner_df.aggregate(group_exprs, vec![agg_expr])?;
        Ok(df)
    }

    // ========================================================================
    // Function call
    // ========================================================================

    /// Plans a function call expression.
    ///
    /// Phase 1: All function calls return `NotImplemented`. Functions like
    /// `rate`, `increase`, and `irate` require custom UDAF or complex window
    /// logic that will be implemented incrementally.
    #[allow(clippy::unused_async)] // Will become async when functions are implemented
    async fn plan_function_call(&self, func: &crate::promql::function::FunctionCall) -> Result<DataFrame> {
        Err(QueryError::NotImplemented(format!("function '{}'", func.name)))
    }

    // ========================================================================
    // Binary operation
    // ========================================================================

    /// Plans a binary operation expression.
    ///
    /// Phase 1 handles the simple case where one side is a `NumberLiteral`
    /// (scalar-vector arithmetic). Vector-vector operations return
    /// `NotImplemented`.
    async fn plan_binary_op(&self, binop: crate::promql::binary::BinaryOp) -> Result<DataFrame> {
        // Check for scalar on right side: vector <op> scalar
        if let PromQLExpr::NumberLiteral(scalar) = *binop.rhs {
            let df = self.plan_expr(*binop.lhs).await?;
            let new_value = apply_scalar_op(col("value"), lit(scalar), binop.op)?;
            return df.with_column("value", new_value).map_err(QueryError::from);
        }

        // Check for scalar on left side: scalar <op> vector
        if let PromQLExpr::NumberLiteral(scalar) = *binop.lhs {
            let df = self.plan_expr(*binop.rhs).await?;
            let new_value = apply_scalar_op(lit(scalar), col("value"), binop.op)?;
            return df.with_column("value", new_value).map_err(QueryError::from);
        }

        Err(QueryError::NotImplemented(
            "vector-vector binary operations".to_string(),
        ))
    }

    // ========================================================================
    // Unary operation
    // ========================================================================

    /// Plans a unary operation expression.
    ///
    /// - `Neg`: multiplies value by -1
    /// - `Pos`: identity (no-op)
    async fn plan_unary_op(&self, unary: crate::promql::binary::UnaryOp) -> Result<DataFrame> {
        let df = self.plan_expr(*unary.expr).await?;
        match unary.op {
            UnaryOperator::Neg => df.with_column("value", col("value") * lit(-1.0_f64)).map_err(QueryError::from),
            UnaryOperator::Pos => Ok(df),
        }
    }

    // ========================================================================
    // Literals
    // ========================================================================

    /// Plans a number literal as a single-row `DataFrame`.
    fn plan_number_literal(&self, value: f64) -> Result<DataFrame> {
        let df = self.session_ctx.read_batch(
            datafusion::arrow::array::RecordBatch::try_from_iter(vec![(
                "value",
                std::sync::Arc::new(datafusion::arrow::array::Float64Array::from(vec![value]))
                    as std::sync::Arc<dyn datafusion::arrow::array::Array>,
            )])
            .map_err(|e| QueryError::Plan(format!("failed to create literal batch: {e}")))?,
        )?;
        Ok(df)
    }

    /// Plans a string literal as a single-row `DataFrame`.
    fn plan_string_literal(&self, value: &str) -> Result<DataFrame> {
        let df = self.session_ctx.read_batch(
            datafusion::arrow::array::RecordBatch::try_from_iter(vec![(
                "value",
                std::sync::Arc::new(datafusion::arrow::array::StringArray::from(vec![value.to_string()]))
                    as std::sync::Arc<dyn datafusion::arrow::array::Array>,
            )])
            .map_err(|e| QueryError::Plan(format!("failed to create literal batch: {e}")))?,
        )?;
        Ok(df)
    }

    // ========================================================================
    // Labels metadata
    // ========================================================================

    /// Plan a query for distinct label names from the metrics table.
    ///
    /// Returns a `DataFrame` with a single `label` column. Extracts keys
    /// from the `attributes` MAP and adds `__name__` (always present).
    #[tracing::instrument(skip(self))]
    pub async fn plan_labels(&self) -> Result<DataFrame> {
        let df = self.base_labels_scan().await?;

        // Extract distinct keys from the attributes MAP
        let df = df.select(vec![map_keys(col("attributes")).alias("keys")])?;
        let df = df.unnest_columns(&["keys"])?;
        let df = df.aggregate(vec![col("keys").alias("label")], vec![])?;

        // Add __name__ (always present for metrics)
        let name_df = self
            .session_ctx
            .read_batch(
                datafusion::arrow::array::RecordBatch::try_from_iter(vec![(
                    "label",
                    std::sync::Arc::new(datafusion::arrow::array::StringArray::from(vec!["__name__"]))
                        as std::sync::Arc<dyn datafusion::arrow::array::Array>,
                )])
                .map_err(|e| QueryError::Plan(format!("failed to create __name__ batch: {e}")))?,
            )?;

        let df = df.union(name_df)?;
        Ok(df.aggregate(vec![col("label")], vec![])?)
    }

    /// Plan a query for distinct values of a specific label.
    ///
    /// For `__name__`, returns distinct `metric_name` values. For other
    /// labels, extracts values from the `attributes` MAP via `get_field`.
    ///
    /// Returns a `DataFrame` with a single `value` column.
    #[tracing::instrument(skip(self), fields(label_name))]
    pub async fn plan_label_values(&self, label_name: &str) -> Result<DataFrame> {
        let df = self.base_labels_scan().await?;

        let df = if label_name == "__name__" {
            df.select(vec![col("metric_name").alias("value")])?
        } else {
            let value_expr = datafusion::functions::core::get_field()
                .call(vec![col("attributes"), lit(label_name)])
                .alias("value");
            df.select(vec![value_expr])?
                .filter(col("value").is_not_null())?
        };

        let df = df
            .aggregate(vec![col("value")], vec![])?
            .sort(vec![col("value").sort(true, true)])?;

        Ok(df)
    }

    /// Base scan for labels/label_values queries — tenant + time filtered.
    async fn base_labels_scan(&self) -> Result<DataFrame> {
        let df = self.session_ctx.table(METRICS_TABLE_FQN).await?;
        let df = strip_schema_metadata(df)?;
        let df = df.filter(col("tenant_id").eq(lit(self.query_ctx.tenant_id.as_str())))?;

        let start_us = self.query_ctx.start.timestamp_micros();
        let end_us = self.query_ctx.end.timestamp_micros();
        let start = ScalarValue::TimestampMicrosecond(Some(start_us), Some("UTC".into()));
        let end = ScalarValue::TimestampMicrosecond(Some(end_us), Some("UTC".into()));
        let df = df.filter(col("timestamp").gt_eq(lit(start)).and(col("timestamp").lt_eq(lit(end))))?;

        Ok(df)
    }
}

/// Applies a scalar binary operator to two DataFusion expressions.
///
/// Returns `NotImplemented` for comparison and set operators in Phase 1.
fn apply_scalar_op(lhs: Expr, rhs: Expr, op: BinaryOperator) -> Result<Expr> {
    match op {
        BinaryOperator::Add => Ok(lhs + rhs),
        BinaryOperator::Sub => Ok(lhs - rhs),
        BinaryOperator::Mul => Ok(lhs * rhs),
        BinaryOperator::Div => Ok(lhs / rhs),
        BinaryOperator::Mod => Ok(lhs % rhs),
        BinaryOperator::Pow => Ok(datafusion::functions::math::power().call(vec![lhs, rhs])),
        other => Err(QueryError::NotImplemented(format!(
            "binary operator '{}' in scalar context",
            other.as_str()
        ))),
    }
}
