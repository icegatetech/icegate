//! DataFusion-based LogQL query planner.

// Allow warnings for stub implementations - these will be fixed as features are
// implemented
use std::{future::Future, pin::Pin};

/// Internal column name for serialized attribute keys.
const COL_ATTR_KEYS: &str = "_attr_keys";
/// Internal column name for serialized attribute values.
const COL_ATTR_VALS: &str = "_attr_vals";

use chrono::{DateTime, TimeDelta, Utc};
use datafusion::functions_aggregate::expr_fn::{avg, bool_or, first_value, max, min, stddev, var_sample};
use datafusion::{
    arrow::datatypes::{DataType, IntervalMonthDayNano},
    functions::string::octet_length,
    functions_aggregate::expr_fn::{count, last_value, sum},
    logical_expr::{Expr, ExprSchemable, ScalarUDF, col, lit, when},
    prelude::*,
    scalar::ScalarValue,
};
use icegate_common::{LOGS_TABLE_FQN, schema::LOG_INDEXED_ATTRIBUTE_COLUMNS};

use crate::{
    error::{QueryError, Result},
    logql::{
        RangeAggregationOp,
        common::MatchOp,
        expr::LogQLExpr,
        log::{LabelMatcher, LogExpr, Selector},
        metric::MetricExpr,
        planner::{DEFAULT_LOG_LIMIT, Planner, QueryContext, SortDirection},
    },
};

/// Strips PARQUET field metadata from `DataFrame` schema.
/// Required because Iceberg schemas include `PARQUET:field_id` metadata,
/// but in-memory operations (`MemTable`, `map_keys`) create fields without it.
/// This prevents Arrow schema mismatch errors during joins/unions.
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

/// A planner that converts `LogQL` expressions into `DataFusion` `DataFrame`s.
pub struct DataFusionPlanner {
    session_ctx: SessionContext,
    query_ctx: QueryContext,
}

impl DataFusionPlanner {
    /// Creates a new `DataFusionPlanner`.
    pub const fn new(session_ctx: SessionContext, query_ctx: QueryContext) -> Self {
        Self { session_ctx, query_ctx }
    }
}

impl Planner for DataFusionPlanner {
    type Plan = DataFrame;

    async fn plan(&self, expr: LogQLExpr) -> Result<Self::Plan> {
        match expr {
            LogQLExpr::Log(log_expr) => {
                let df = self.plan_log(log_expr, self.query_ctx.start, self.query_ctx.end).await?;

                // Sort by timestamp (ascending for forward, descending for backward)
                // Must apply sort BEFORE limit to get correct N oldest/newest entries
                let ascending = self.query_ctx.direction == SortDirection::Forward;
                let df = df.sort(vec![col("timestamp").sort(ascending, true)])?;

                // Apply limit only for log queries (not metrics)
                // Uses context limit or Loki default of 100 entries
                let limit = self.query_ctx.limit.unwrap_or(DEFAULT_LOG_LIMIT);
                let df = df.limit(0, Some(limit))?;

                // Transform output: hex-encode binary columns, add `level` alias
                Self::transform_output_columns(df)
            }
            LogQLExpr::Metric(metric_expr) => self.plan_metric(metric_expr).await,
        }
    }
}

impl DataFusionPlanner {
    /// Binary columns that need hex encoding for output.
    const BINARY_COLUMNS: [&'static str; 2] = ["trace_id", "span_id"];

    async fn plan_log(&self, expr: LogExpr, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<DataFrame> {
        // 1. Scan the logs table from iceberg.icegate namespace
        let df = self.session_ctx.table(LOGS_TABLE_FQN).await?;

        // 1.5 Strip Iceberg PARQUET metadata to prevent schema mismatch with in-memory
        // tables
        let df = strip_schema_metadata(df)?;

        // 2. Apply MANDATORY tenant filter (multi-tenancy isolation)
        // This filter is applied FIRST and cannot be bypassed by user queries.
        // Since tenant_id is the leading partition key, Iceberg will prune
        // non-matching partitions for efficient query execution.
        let df = df.filter(col("tenant_id").eq(lit(&self.query_ctx.tenant_id)))?;

        // 3. Apply time range filter
        // The timestamp column is Timestamp(Microsecond)
        // Convert DateTime<Utc> to microseconds for comparison
        let ts_col = col("timestamp");
        let start_micros = start.timestamp_micros();
        let end_micros = end.timestamp_micros();
        let start_literal = lit(ScalarValue::TimestampMicrosecond(Some(start_micros), None));
        let end_literal = lit(ScalarValue::TimestampMicrosecond(Some(end_micros), None));
        let df = df.filter(ts_col.clone().gt_eq(start_literal).and(ts_col.lt_eq(end_literal)))?;

        // 4. Apply Selector matchers
        let df = Self::apply_selector(df, expr.selector)?;

        // 5. Apply Pipeline stages
        self.apply_pipeline(df, expr.pipeline)
    }

    /// Plan a query for distinct label names.
    ///
    /// Returns a `DataFrame` with a single column `label` containing all
    /// distinct label names from logs matching the selector and time range.
    /// This includes:
    /// - Attribute keys from the `attributes` MAP column
    /// - Indexed column names that have at least one non-null value
    /// - `level` alias (if `severity_text` has non-null values)
    ///
    /// Labels with only NULL values are excluded from the result.
    pub async fn plan_labels(&self, selector: Selector) -> Result<DataFrame> {
        // Build base query with tenant + time + selector filters
        let log_expr = LogExpr::new(selector);
        let df = self.plan_log(log_expr, self.query_ctx.start, self.query_ctx.end).await?;

        // Query 1: Get distinct attribute keys from MAP column
        let attr_keys_df = df
            .clone()
            .select(vec![map_keys(col("attributes")).alias("attr_keys")])?
            .unnest_columns(&["attr_keys"])?
            .aggregate(vec![col("attr_keys").alias("label")], vec![])?;

        // Query 2: For each indexed column, emit its name only if it has non-null
        // values Build: SELECT 'col_name' as label WHERE col IS NOT NULL LIMIT
        // 1
        let mut union_df = attr_keys_df;

        for &col_name in LOG_INDEXED_ATTRIBUTE_COLUMNS {
            let col_df = df
                .clone()
                .filter(col(col_name).is_not_null())?
                .limit(0, Some(1))?
                .select(vec![lit(col_name).alias("label")])?;
            union_df = union_df.union(col_df)?;
        }

        // Add 'level' if severity_text has non-null values (Grafana compatibility)
        let level_df = df
            .filter(col("severity_text").is_not_null())?
            .limit(0, Some(1))?
            .select(vec![lit("level").alias("label")])?;
        union_df = union_df.union(level_df)?;

        // Deduplicate and return
        Ok(union_df.aggregate(vec![col("label")], vec![])?)
    }

    /// Plan a query for distinct values of a specific label.
    ///
    /// Returns a `DataFrame` with a single column `value` containing all
    /// distinct values for the specified label from logs matching the
    /// selector and time range.
    pub async fn plan_label_values(&self, selector: Selector, label_name: &str) -> Result<DataFrame> {
        // Build base query with tenant + time + selector filters
        let log_expr = LogExpr::new(selector);
        let df = self.plan_log(log_expr, self.query_ctx.start, self.query_ctx.end).await?;

        // Select value column (handle indexed vs attribute columns)
        let internal_name = Self::map_label_to_internal_name(label_name);
        let value_expr = if Self::is_top_level_field(internal_name) {
            // Binary columns (trace_id, span_id) need to be converted to hex strings
            // Cast FixedSizeBinary to Binary first since encode() requires Binary type
            if Self::is_binary_column(internal_name) {
                datafusion::functions::encoding::encode()
                    .call(vec![
                        col(internal_name).cast_to(&DataType::Binary, df.schema())?,
                        lit("hex"),
                    ])
                    .alias("value")
            } else {
                col(internal_name).alias("value")
            }
        } else {
            datafusion::functions::core::get_field()
                .call(vec![col("attributes"), lit(label_name)])
                .alias("value")
        };

        // Filter nulls, get distinct, sort
        let df = df
            .select(vec![value_expr])?
            .filter(col("value").is_not_null())?
            .aggregate(vec![col("value")], vec![])?
            .sort(vec![col("value").sort(true, true)])?;

        Ok(df)
    }

    /// Check if a column stores binary data (`trace_id`, `span_id`).
    fn is_binary_column(name: &str) -> bool {
        matches!(name, "trace_id" | "span_id")
    }

    /// Transform output columns for Loki/Grafana compatibility:
    /// - Encode binary columns (`trace_id`, `span_id`) to hex strings
    /// - Add `level` column as alias of `severity_text` (Grafana expects `level` label)
    ///
    /// This ensures that all output from log queries has:
    /// - String-typed trace/span IDs (simplifies handler code)
    /// - Grafana-compatible `level` label for log level filtering
    fn transform_output_columns(df: DataFrame) -> Result<DataFrame> {
        let schema = df.schema().clone();

        // Build select expressions with transformations
        let mut select_exprs: Vec<Expr> = schema
            .inner()
            .fields()
            .iter()
            .map(|field| {
                let name = field.name().as_str();
                if Self::BINARY_COLUMNS.contains(&name) {
                    // encode(cast(col, Binary), 'hex') converts FixedSizeBinary to hex string
                    datafusion::functions::encoding::encode()
                        .call(vec![
                            col(name).cast_to(&DataType::Binary, &schema).unwrap_or_else(|_| col(name)),
                            lit("hex"),
                        ])
                        .alias(name)
                } else {
                    col(name)
                }
            })
            .collect();

        // Add `level` as alias of `severity_text` for Grafana compatibility
        if schema.has_column_with_unqualified_name("severity_text") {
            select_exprs.push(col("severity_text").alias("level"));
        }

        Ok(df.select(select_exprs)?)
    }

    /// Plan a query for distinct series (unique label combinations).
    ///
    /// Returns a `DataFrame` with indexed columns and attributes MAP for all
    /// distinct series matching any of the provided selectors.
    ///
    /// The approach:
    /// 1. Serialize MAP keys/values to strings (MAP doesn't support GROUP BY)
    /// 2. Convert binary columns (`trace_id`, `span_id`) to hex strings
    /// 3. Group by indexed columns + serialized attributes
    /// 4. Keep one representative attributes MAP using `first_value()`
    #[allow(clippy::items_after_statements)]
    pub async fn plan_series(&self, selectors: &[Selector]) -> Result<DataFrame> {
        use datafusion::{
            functions_aggregate::first_last::first_value,
            functions_nested::{map_keys::map_keys, map_values::map_values, string::array_to_string},
        };

        if selectors.is_empty() {
            return Err(QueryError::Plan("At least one selector is required".to_string()));
        }

        // Plan each selector independently and UNION the results
        // This ensures correct semantics: rows matching ANY selector are returned
        let mut dataframes: Vec<DataFrame> = Vec::with_capacity(selectors.len());
        for selector in selectors {
            let log_expr = LogExpr::new(selector.clone());
            let df = self.plan_log(log_expr, self.query_ctx.start, self.query_ctx.end).await?;
            dataframes.push(df);
        }

        // UNION all DataFrames (the aggregation step below will deduplicate)
        let mut df = dataframes.remove(0);
        for other_df in dataframes {
            df = df.union(other_df)?;
        }

        // Build select with serialized attributes for grouping
        // Convert binary columns to hex strings for proper grouping and output
        // Cast FixedSizeBinary to Binary first since encode() requires Binary type
        let schema = df.schema().clone();
        let mut select_cols: Vec<Expr> = LOG_INDEXED_ATTRIBUTE_COLUMNS
            .iter()
            .map(|&c| {
                if Self::is_binary_column(c) {
                    datafusion::functions::encoding::encode()
                        .call(vec![
                            col(c).cast_to(&DataType::Binary, &schema).unwrap_or_else(|_| col(c)),
                            lit("hex"),
                        ])
                        .alias(c)
                } else {
                    col(c)
                }
            })
            .collect();
        // Add `level` as alias of `severity_text` for Grafana compatibility
        select_cols.push(col("severity_text").alias("level"));
        select_cols.push(col("attributes"));
        // Serialize map keys and values for grouping.
        // Use "|||" delimiter instead of "," to avoid ambiguity when keys/values
        // contain commas.
        select_cols.push(array_to_string(map_keys(col("attributes")), lit("|||")).alias(COL_ATTR_KEYS));
        select_cols.push(array_to_string(map_values(col("attributes")), lit("|||")).alias(COL_ATTR_VALS));

        let df = df.select(select_cols)?;

        // Group by indexed columns + level + serialized attributes
        let mut group_cols: Vec<Expr> = LOG_INDEXED_ATTRIBUTE_COLUMNS.iter().map(|c| col(*c)).collect();
        group_cols.push(col("level"));
        group_cols.push(col(COL_ATTR_KEYS));
        group_cols.push(col(COL_ATTR_VALS));

        let df = df.aggregate(
            group_cols,
            vec![first_value(col("attributes"), vec![]).alias("attributes")],
        )?;

        Ok(df)
    }

    fn plan_metric<'a>(&'a self, expr: MetricExpr) -> Pin<Box<dyn Future<Output = Result<DataFrame>> + Send + 'a>> {
        Box::pin(async move {
            match expr {
                MetricExpr::RangeAggregation(agg) => self.plan_range_aggregation(agg).await,
                MetricExpr::VectorAggregation(agg) => self.plan_vector_aggregation(agg).await,
                MetricExpr::BinaryOp {
                    op: _op,
                    left,
                    right,
                    modifier: _modifier,
                } => {
                    let _left_df = self.plan_metric(*left).await?;
                    let _right_df = self.plan_metric(*right).await?;

                    // TODO: Implement binary operations (vector matching)
                    // This requires joining left and right DataFrames based on labels and
                    // timestamp, applying the operation, and handling the
                    // modifier (on/ignoring, group_left/right).
                    Err(QueryError::NotImplemented(
                        "Binary operations not yet implemented".to_string(),
                    ))
                }
                MetricExpr::Literal(_val) => {
                    // TODO: Implement literal value
                    Err(QueryError::NotImplemented(
                        "Literal value not yet implemented".to_string(),
                    ))
                }
                MetricExpr::Vector(_vals) => {
                    // TODO: Implement vector literal
                    Err(QueryError::NotImplemented(
                        "Vector literal not yet implemented".to_string(),
                    ))
                }
                MetricExpr::LabelReplace { .. } => {
                    // TODO: Implement label replace
                    Err(QueryError::NotImplemented(
                        "Label replace not yet implemented".to_string(),
                    ))
                }
                MetricExpr::Variable(_) => Err(QueryError::NotImplemented("Variable not yet implemented".to_string())),
                MetricExpr::Parens(inner) => self.plan_metric(*inner).await,
            }
        })
    }

    async fn plan_range_aggregation(&self, agg: crate::logql::metric::RangeAggregation) -> Result<DataFrame> {
        if agg.range_expr.unwrap.is_some() {
            self.plan_unwrap_range_aggregation(agg).await
        } else {
            match agg.op {
                RangeAggregationOp::AbsentOverTime => Ok(self.plan_log_range_absent_aggregation(agg).await?),
                _ => Ok(self.plan_log_range_aggregation(agg).await?),
            }
        }
    }

    /// Calculate adjusted time range for range aggregation lookback window.
    ///
    /// `LogQL` range aggregations query a time window extending beyond the requested
    /// range to capture all logs that fall within the lookback window for each grid point.
    fn adjust_time_range_for_lookback(
        query_start: DateTime<Utc>,
        query_end: DateTime<Utc>,
        range: TimeDelta,
        offset: Option<TimeDelta>,
    ) -> (DateTime<Utc>, DateTime<Utc>) {
        let offset_duration = offset.unwrap_or(TimeDelta::zero());
        let adjusted_start = query_start - range - offset_duration;
        let adjusted_end = query_end - offset_duration;
        (adjusted_start, adjusted_end)
    }

    /// Build argument expressions for the `date_grid` UDF.
    ///
    /// Creates five temporal literal expressions required by `DateGrid` UDF.
    ///
    /// # Errors
    /// Returns `QueryError::Config` if step/range/offset duration exceeds i64 limits
    fn build_date_grid_args(
        query_ctx: &QueryContext,
        range: TimeDelta,
        offset: Option<TimeDelta>,
    ) -> Result<(Expr, Expr, Expr, Expr, Expr, TimeDelta)> {
        let start_micros = query_ctx.start.timestamp_micros();
        let end_micros = query_ctx.end.timestamp_micros();

        let step_micros = query_ctx
            .step
            .ok_or(QueryError::Config(
                "Step parameter is required for range aggregation".to_string(),
            ))?
            .num_microseconds()
            .ok_or(QueryError::Config("Step duration too large".to_string()))?;

        let range_nanos = range
            .num_nanoseconds()
            .ok_or(QueryError::Config("Range duration too large".to_string()))?;

        let offset_duration = offset.unwrap_or(TimeDelta::zero());
        let offset_nanos = offset_duration
            .num_nanoseconds()
            .ok_or(QueryError::Config("Offset duration too large".to_string()))?;

        let start_arg = lit(ScalarValue::TimestampMicrosecond(Some(start_micros), None));
        let end_arg = lit(ScalarValue::TimestampMicrosecond(Some(end_micros), None));
        let step_arg = lit(ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
            0,
            0,
            step_micros * 1000,
        ))));
        let range_arg = lit(ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
            0,
            0,
            range_nanos,
        ))));
        let offset_arg = lit(ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
            0,
            0,
            offset_nanos,
        ))));

        Ok((start_arg, end_arg, step_arg, range_arg, offset_arg, offset_duration))
    }

    /// Build grouping expressions for range aggregation.
    ///
    /// MAP columns cannot be used directly in GROUP BY, so we serialize keys/values
    /// to strings for grouping, then preserve the original MAP using `last_value`.
    fn build_label_grouping_exprs(include_timestamp: bool) -> Vec<Expr> {
        use datafusion::functions_nested::{map_keys::map_keys, map_values::map_values, string::array_to_string};

        let mut grouping_exprs = Vec::new();
        if include_timestamp {
            grouping_exprs.push(col("grid_timestamp"));
        }
        grouping_exprs.extend(LOG_INDEXED_ATTRIBUTE_COLUMNS.iter().map(|c| col(*c)));
        grouping_exprs.push(array_to_string(map_keys(col("attributes")), lit("|||")).alias(COL_ATTR_KEYS));
        grouping_exprs.push(array_to_string(map_values(col("attributes")), lit("|||")).alias(COL_ATTR_VALS));
        grouping_exprs
    }

    /// Create aggregation expression to preserve the attributes MAP column.
    ///
    /// Uses `last_value()` to preserve one representative attributes MAP.
    fn preserve_attributes_column() -> Expr {
        last_value(col("attributes"), vec![]).alias("attributes")
    }

    async fn plan_unwrap_range_aggregation(&self, agg: crate::logql::metric::RangeAggregation) -> Result<DataFrame> {
        use datafusion::functions_aggregate::expr_fn::approx_percentile_cont;

        // 1. Extract unwrap expression
        let unwrap = agg
            .range_expr
            .unwrap
            .as_ref()
            .ok_or_else(|| QueryError::Plan("Unwrap expression required for this aggregation".to_string()))?;

        // 2. Calculate adjusted time range (same as plan_log_range_aggregation)
        let (adjusted_start, adjusted_end) = Self::adjust_time_range_for_lookback(
            self.query_ctx.start,
            self.query_ctx.end,
            agg.range_expr.range,
            agg.range_expr.offset,
        );

        // 3. Plan inner log query
        let mut df = self.plan_log(agg.range_expr.log_expr, adjusted_start, adjusted_end).await?;

        // 4. Extract unwrapped value (NULL if label missing or conversion fails)
        let unwrapped_expr = Self::extract_unwrapped_value(&unwrap.label, unwrap.conversion, df.schema());
        df = df.with_column("unwrapped_value", unwrapped_expr)?;

        // 5. Mark rows with conversion errors (unwrapped_value IS NULL)
        df = df.with_column("_has_unwrap_error", col("unwrapped_value").is_null())?;

        // 6. Replace NULL with 0.0 for aggregation (errors still tracked by _has_unwrap_error)
        df = df.with_column(
            "unwrapped_value",
            datafusion::functions::core::coalesce().call(vec![col("unwrapped_value"), lit(0.0)]),
        )?;

        // 7. Apply date_grid UDF (same pattern as plan_log_range_aggregation)
        let (start_arg, end_arg, step_arg, range_arg, offset_arg, _) =
            Self::build_date_grid_args(&self.query_ctx, agg.range_expr.range, agg.range_expr.offset)?;

        let date_grid_udf = ScalarUDF::from(super::udf::DateGrid::new());
        let date_grid_args = vec![
            col("timestamp"),
            start_arg,
            end_arg,
            step_arg,
            range_arg,
            offset_arg,
            lit(false), // inverse=false (normal mode)
        ];
        df = df.with_column("_grid_timestamps", date_grid_udf.call(date_grid_args))?;

        // 8. Unnest grid timestamps
        df = df.unnest_columns(&["_grid_timestamps"])?;
        df = df.with_column("grid_timestamp", col("_grid_timestamps"))?;

        // 9. Build grouping expressions (same as plan_log_range_aggregation)
        let grouping_exprs = Self::build_label_grouping_exprs(true);

        // 10. Apply operation-specific aggregation
        let agg_expr = match agg.op {
            RangeAggregationOp::SumOverTime | RangeAggregationOp::RateCounter => sum(col("unwrapped_value")),
            RangeAggregationOp::AvgOverTime => avg(col("unwrapped_value")),
            RangeAggregationOp::MinOverTime => min(col("unwrapped_value")),
            RangeAggregationOp::MaxOverTime => max(col("unwrapped_value")),
            RangeAggregationOp::StddevOverTime => stddev(col("unwrapped_value")),
            RangeAggregationOp::StdvarOverTime => var_sample(col("unwrapped_value")),
            RangeAggregationOp::FirstOverTime => {
                first_value(
                    col("unwrapped_value"),
                    vec![col("timestamp").sort(true, true)], // ascending order
                )
            }
            RangeAggregationOp::LastOverTime => {
                last_value(col("unwrapped_value"), vec![col("timestamp").sort(true, true)])
            }
            RangeAggregationOp::QuantileOverTime => {
                let phi = agg
                    .param
                    .ok_or_else(|| QueryError::Plan("quantile_over_time requires a parameter (0.0-1.0)".to_string()))?;
                approx_percentile_cont(col("unwrapped_value").sort(true, true), lit(phi), None)
            }
            _ => {
                return Err(QueryError::Plan(format!(
                    "{:?} does not support unwrap expressions",
                    agg.op
                )));
            }
        }
        .alias("_agg_value");

        df = df.aggregate(
            grouping_exprs,
            vec![
                agg_expr,
                Self::preserve_attributes_column(),
                // Track if ANY sample had conversion error
                bool_or(col("_has_unwrap_error")).alias("_group_has_error"),
            ],
        )?;

        // 11. Add __error__ label for groups with conversion errors
        let map_insert_udf = ScalarUDF::from(super::udf::MapInsert::new());
        df = df.with_column(
            "attributes",
            when(
                col("_group_has_error"),
                map_insert_udf.call(vec![col("attributes"), lit("__error__"), lit("true")]),
            )
            .otherwise(col("attributes"))?,
        )?;

        // 12. Calculate final values
        let schema = df.schema().clone();
        let mut select_exprs = vec![col("grid_timestamp").alias("timestamp")];

        let value_expr = match agg.op {
            RangeAggregationOp::RateCounter => {
                let range_nanos = agg
                    .range_expr
                    .range
                    .num_nanoseconds()
                    .ok_or(QueryError::Config("Range duration too large".to_string()))?;
                #[allow(clippy::cast_precision_loss)]
                let range_secs = range_nanos as f64 / 1_000_000_000.0;
                (col("_agg_value").cast_to(&DataType::Float64, &schema)? / lit(range_secs)).alias("value")
            }
            _ => col("_agg_value").alias("value"),
        };

        select_exprs.push(value_expr);
        select_exprs.extend(Self::build_default_label_exprs(&[], &[]));

        df = df.select(select_exprs)?;

        // 13. Return all results (including those with __error__ label)
        Ok(df)
    }

    /// Plans `absent_over_time` aggregation using inverse mode + array intersection.
    ///
    /// `AbsentOverTime` requires fundamentally different logic than other range aggregations:
    /// - Uses `date_grid` with `inverse=true` to emit grid points NOT covered by each timestamp
    /// - Aggregates inverse arrays using `array_intersect_agg` to find grid points excluded by ALL timestamps
    /// - Grid points in the intersection are absent (no matching log entries)
    ///
    /// Algorithm:
    /// 1. Apply `date_grid(timestamp, ..., inverse=true)` to get uncovered grid points
    /// 2. Group by labels and aggregate using `array_intersect_agg`
    /// 3. Unnest the intersection result to get absent grid points
    /// 4. Emit value=1.0 for each absent grid point
    async fn plan_log_range_absent_aggregation(
        &self,
        agg: crate::logql::metric::RangeAggregation,
    ) -> Result<DataFrame> {
        use datafusion::{logical_expr::AggregateUDF, prelude::*};

        // 1. Calculate time parameters (same as other range aggregations)
        let (adjusted_start, adjusted_end) = Self::adjust_time_range_for_lookback(
            self.query_ctx.start,
            self.query_ctx.end,
            agg.range_expr.range,
            agg.range_expr.offset,
        );

        // 2. Plan inner log query
        let mut df = self.plan_log(agg.range_expr.log_expr, adjusted_start, adjusted_end).await?;

        // 3. Check if no logs exist - return empty per Prometheus semantics
        if df.clone().count().await? == 0 {
            return Ok(df.limit(0, Some(0))?);
        }

        // 4. Build date_grid UDF arguments (same as other range aggregations)
        let (start_arg, end_arg, step_arg, range_arg, offset_arg, _) =
            Self::build_date_grid_args(&self.query_ctx, agg.range_expr.range, agg.range_expr.offset)?;

        // 5. Apply date_grid UDF with inverse=true
        let date_grid_udf = ScalarUDF::from(super::udf::DateGrid::new());
        let date_grid_args = vec![
            col("timestamp"),
            start_arg,
            end_arg,
            step_arg,
            range_arg,
            offset_arg,
            lit(true), // inverse=true - returns grid points NOT covered
        ];
        df = df.with_column("inverse_grid_timestamps", date_grid_udf.call(date_grid_args))?;

        // 6. Build label grouping expressions (same pattern as other range aggregations)
        let grouping_exprs = Self::build_label_grouping_exprs(false); // exclude timestamp for absent

        // 7. Aggregate using array_intersect_agg UDAF
        // This finds grid points present in ALL inverse arrays (= absent points)
        let array_intersect_udaf = AggregateUDF::from(super::udaf::ArrayIntersectAgg::new());
        df = df.aggregate(
            grouping_exprs,
            vec![
                array_intersect_udaf
                    .call(vec![col("inverse_grid_timestamps")])
                    .alias("absent_timestamps"),
                Self::preserve_attributes_column(),
            ],
        )?;

        // 8. Unnest absent_timestamps to get one row per absent grid point
        df = df.unnest_columns(&["absent_timestamps"])?;
        df = df.with_column("timestamp", col("absent_timestamps"))?;

        // 9. Add value=1.0 for all absent points
        df = df.with_column("value", lit(1.0))?;

        // 10. Select final output columns (match other range aggregations)
        let mut select_exprs = vec![col("timestamp"), col("value")];
        select_exprs.extend(LOG_INDEXED_ATTRIBUTE_COLUMNS.iter().map(|c| col(*c)));
        select_exprs.push(col("attributes"));

        Ok(df.select(select_exprs)?)
    }

    /// Plans a log range aggregation using `date_grid` UDF + standard aggregations.
    ///
    /// This implementation uses the `date_grid` scalar UDF to generate matching
    /// grid timestamps for each log entry, then applies standard DataFusion
    /// aggregation functions (count, sum) to compute the final values.
    ///
    /// Supports: `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`.
    /// Note: `absent_over_time` is handled separately in `plan_log_range_absent_aggregation`.
    async fn plan_log_range_aggregation(&self, agg: crate::logql::metric::RangeAggregation) -> Result<DataFrame> {
        // 1. Plan the inner LogExpr with extended time range for lookback window
        let (adjusted_start, adjusted_end) = Self::adjust_time_range_for_lookback(
            self.query_ctx.start,
            self.query_ctx.end,
            agg.range_expr.range,
            agg.range_expr.offset,
        );

        let mut df = self.plan_log(agg.range_expr.log_expr, adjusted_start, adjusted_end).await?;

        // 2. Build UDF arguments for date_grid
        let (start_arg, end_arg, step_arg, range_arg, offset_arg, _) =
            Self::build_date_grid_args(&self.query_ctx, agg.range_expr.range, agg.range_expr.offset)?;

        // 3. Apply date_grid UDF to generate grid timestamps for each row
        let date_grid_udf = ScalarUDF::from(super::udf::DateGrid::new());
        let date_grid_args = vec![
            col("timestamp"),
            start_arg,
            end_arg,
            step_arg,
            range_arg,
            offset_arg,
            lit(false),
        ];
        df = df.with_column("_grid_timestamps", date_grid_udf.call(date_grid_args))?;

        // 4. Unnest grid timestamps to expand rows
        df = df.unnest_columns(&["_grid_timestamps"])?;
        df = df.with_column("grid_timestamp", col("_grid_timestamps"))?;

        // 5. Build grouping expressions
        let grouping_exprs = Self::build_label_grouping_exprs(true);

        // 6. Build operation-specific aggregation
        let range_nanos = agg
            .range_expr
            .range
            .num_nanoseconds()
            .ok_or(QueryError::Config("Range duration too large".to_string()))?;
        #[allow(clippy::cast_precision_loss)]
        let range_secs = range_nanos as f64 / 1_000_000_000.0;

        let agg_expr = match agg.op {
            RangeAggregationOp::CountOverTime | RangeAggregationOp::Rate => count(lit(1)).alias("_agg_value"),
            RangeAggregationOp::BytesOverTime | RangeAggregationOp::BytesRate => {
                sum(octet_length().call(vec![col("body")])).alias("_agg_value")
            }
            _ => {
                return Err(QueryError::Plan(
                    "This range aggregation requires an unwrap expression".to_string(),
                ));
            }
        };

        // 7. Aggregate
        df = df.aggregate(grouping_exprs, vec![agg_expr, Self::preserve_attributes_column()])?;

        // 8. Calculate final values
        let schema = df.schema().clone();
        let mut select_exprs = vec![col("grid_timestamp").alias("timestamp")];

        let value_expr = match agg.op {
            RangeAggregationOp::CountOverTime | RangeAggregationOp::BytesOverTime => {
                // Simple cast to Float64, no division
                col("_agg_value").cast_to(&DataType::Float64, &schema)?.alias("value")
            }
            RangeAggregationOp::Rate | RangeAggregationOp::BytesRate => {
                // Rate operations: divide by range_seconds
                (col("_agg_value").cast_to(&DataType::Float64, &schema)? / lit(range_secs)).alias("value")
            }
            _ => {
                return Err(QueryError::Plan(
                    "This range aggregation requires an unwrap expression".to_string(),
                ));
            }
        };

        select_exprs.push(value_expr);
        select_exprs.extend(Self::build_default_label_exprs(&[], &[]));

        df = df.select(select_exprs)?;

        // 9. Filter for sparse output (keep only value > 0)
        Ok(df.filter(col("value").gt(lit(0.0)))?)
    }

    fn build_default_label_columns(with: &[&str], without: &[&str]) -> Vec<String> {
        LOG_INDEXED_ATTRIBUTE_COLUMNS
            .iter()
            .copied()
            .chain(std::iter::once("attributes"))
            .chain(with.iter().copied())
            .filter(|c| !without.contains(c))
            .map(ToString::to_string)
            .collect()
    }

    fn build_default_label_exprs(with: &[&str], without: &[&str]) -> Vec<Expr> {
        Self::build_default_label_columns(with, without).into_iter().map(col).collect()
    }

    async fn plan_vector_aggregation(&self, agg: crate::logql::metric::VectorAggregation) -> Result<DataFrame> {
        use crate::logql::{common::Grouping, metric::MetricExpr};

        // 1. Push grouping down to inner RangeAggregation if present
        // For queries like `sum by (level) (count_over_time({...}[1m]))`,
        // we need the inner range aggregation to also group by `level`.
        let inner_expr = match (*agg.expr, &agg.grouping) {
            (MetricExpr::RangeAggregation(mut range_agg), Some(outer_grouping)) => {
                // Merge outer grouping into inner range aggregation
                let outer_labels = match outer_grouping {
                    Grouping::By(labels) => labels.clone(),
                    Grouping::Without(_) => {
                        return Err(QueryError::NotImplemented(
                            "Grouping::Without is not yet implemented".to_string(),
                        ));
                    }
                };

                // Add outer labels to inner grouping
                let merged_grouping = match range_agg.grouping.take() {
                    Some(Grouping::By(mut inner_labels)) => {
                        // Merge labels, avoiding duplicates
                        for label in outer_labels {
                            if !inner_labels.iter().any(|l| l.name == label.name) {
                                inner_labels.push(label);
                            }
                        }
                        Some(Grouping::By(inner_labels))
                    }
                    Some(Grouping::Without(_)) => {
                        // When outer is By and inner is Without, apply outer By restriction
                        Some(Grouping::By(outer_labels))
                    }
                    None => {
                        // No inner grouping, use outer labels
                        Some(Grouping::By(outer_labels))
                    }
                };
                range_agg.grouping = merged_grouping;
                MetricExpr::RangeAggregation(range_agg)
            }
            (expr, _) => expr,
        };

        // 2. Plan the inner MetricExpr (now with merged grouping)
        let df = self.plan_metric(inner_expr).await?;
        let schema = df.schema();

        // 3. Identify grouping columns for the vector aggregation
        // LogQL: sum by (label1, label2) (...)
        // DataFusion: group_expr = [col("label1"), col("label2")]
        let mut group_exprs = if let Some(grouping) = &agg.grouping {
            let (labels, udf) = match grouping {
                Grouping::By(labels) => (labels, ScalarUDF::from(super::udf::MapKeepKeys::new())),
                Grouping::Without(_) => {
                    return Err(QueryError::NotImplemented(
                        "Grouping::Without is not yet implemented".to_string(),
                    ));
                }
            };

            let mut indexed_attributes = Vec::new();
            let mut attributes = Vec::new();
            for l in labels {
                // Map Loki label names to column names (e.g., level -> severity_text)
                let mapped_name = Self::map_label_to_internal_name(&l.name);
                // Check if column exists directly in schema (using mapped name)
                if Self::is_top_level_field(mapped_name) && schema.inner().column_with_name(mapped_name).is_some() {
                    indexed_attributes.push(mapped_name);
                } else if schema.inner().column_with_name("attributes").is_some() {
                    // Use original label name for attributes map lookup, not mapped name
                    attributes.push(l.name.as_str());
                } else {
                    // Column doesn't exist and no attributes map - the label isn't available
                    return Err(QueryError::NotImplemented(format!(
                        "Label '{}' not available in aggregation result.",
                        l.name
                    )));
                }
            }

            // Build expressions from indexed attributes
            let mut exprs: Vec<Expr> = indexed_attributes.iter().map(|c| col((*c).to_string())).collect();
            if !attributes.is_empty() {
                // Create filtered attributes expression
                let filtered_attrs = udf.call(vec![
                    col("attributes"),
                    make_array(attributes.iter().map(ToString::to_string).map(lit).collect()),
                ]);
                exprs.push(filtered_attrs.alias("attributes"));
            }
            exprs
        } else {
            vec![]
        };
        group_exprs.push(col("timestamp"));

        // 4. Identify aggregation function
        // LogQL: sum, avg, min, max, count, stddev, stdvar, bottomk, topk
        // We need to map agg.op to DataFusion aggregate functions.
        // For now, we'll implement a few common ones.

        // Note: We assume the inner DataFrame produces a "value" column that we
        // aggregate.
        let value_col = col("value");

        let aggr_expr = match agg.op {
            crate::logql::metric::VectorAggregationOp::Sum => datafusion::functions_aggregate::expr_fn::sum(value_col),
            crate::logql::metric::VectorAggregationOp::Avg => datafusion::functions_aggregate::expr_fn::avg(value_col),
            crate::logql::metric::VectorAggregationOp::Min => datafusion::functions_aggregate::expr_fn::min(value_col),
            crate::logql::metric::VectorAggregationOp::Max => datafusion::functions_aggregate::expr_fn::max(value_col),
            crate::logql::metric::VectorAggregationOp::Count => {
                datafusion::functions_aggregate::expr_fn::count(value_col)
            }
            crate::logql::metric::VectorAggregationOp::Stddev => {
                datafusion::functions_aggregate::expr_fn::stddev(value_col)
            }
            crate::logql::metric::VectorAggregationOp::Stdvar => {
                datafusion::functions_aggregate::expr_fn::var_sample(value_col)
            }
            // TODO: Implement topk, bottomk, sort, sort_desc
            _ => {
                return Err(QueryError::NotImplemented(format!(
                    "Vector aggregation op {:?} not supported",
                    agg.op
                )));
            }
        }
        .alias("value");

        // 5. Apply aggregation using DataFrame API
        Ok(df.aggregate(group_exprs, vec![aggr_expr])?)
    }

    /// Apply selector matchers to filter a `DataFrame`.
    ///
    /// Each matcher is converted to a filter expression and applied
    /// sequentially.
    pub fn apply_selector(df: DataFrame, selector: Selector) -> Result<DataFrame> {
        let mut df = df;
        for matcher in selector.matchers {
            let expr = Self::matcher_to_expr(&matcher);
            df = df.filter(expr)?;
        }
        Ok(df)
    }

    /// Convert a `LabelMatcher` to a `DataFusion` filter expression.
    ///
    /// Handles both indexed columns (e.g., `service_name`, `severity_text`) and
    /// attributes from the MAP column.
    ///
    /// For binary columns (`trace_id`, `span_id`), the user-provided hex string
    /// is decoded to binary for comparison with the stored `FixedSizeBinary`
    /// value.
    pub fn matcher_to_expr(matcher: &LabelMatcher) -> Expr {
        let mapped_label = Self::map_label_to_internal_name(&matcher.label);
        let col_expr = if Self::is_top_level_field(&matcher.label) {
            col(mapped_label)
        } else {
            // Access attribute from the "attributes" map
            // using get_field for map access
            datafusion::functions::core::get_field().call(vec![col("attributes"), lit(matcher.label.as_str())])
        };

        // For binary columns (trace_id, span_id), decode the hex string to binary
        // to compare with the stored FixedSizeBinary value
        let val = if Self::is_binary_column(mapped_label) {
            // decode(value, 'hex') converts hex string to Binary
            datafusion::functions::encoding::decode().call(vec![lit(matcher.value.as_str()), lit("hex")])
        } else {
            lit(matcher.value.as_str())
        };

        match matcher.op {
            MatchOp::Eq => col_expr.eq(val),
            MatchOp::Neq => col_expr.not_eq(val),
            // Use regexp_like which returns boolean (true if pattern matches)
            // Note: regex matching on binary columns is not supported, will treat as string
            MatchOp::Re => {
                datafusion::functions::regex::regexp_like().call(vec![col_expr, lit(matcher.value.as_str())])
            }
            MatchOp::Nre => datafusion::functions::regex::regexp_like()
                .call(vec![col_expr, lit(matcher.value.as_str())])
                .not(),
        }
    }

    /// Maps Loki/Grafana label names to actual column names.
    ///
    /// Loki uses different label conventions than `OpenTelemetry`:
    /// - `level` -> `severity_text` (log level)
    /// - `detected_level` -> `severity_text` (Grafana's auto-detected level)
    /// - `service` -> `service_name` (alternative name)
    pub fn map_label_to_internal_name(name: &str) -> &str {
        match name {
            "level" | "detected_level" => "severity_text",
            "service" => "service_name",
            _ => name,
        }
    }

    /// Check if a label name corresponds to a top-level indexed column.
    ///
    /// Top-level fields are stored as separate columns in the Iceberg table,
    /// while other labels are stored in the `attributes` MAP column.
    pub fn is_top_level_field(name: &str) -> bool {
        let mapped = Self::map_label_to_internal_name(name);
        LOG_INDEXED_ATTRIBUTE_COLUMNS.contains(&mapped) || matches!(mapped, "tenant_id" | "timestamp")
    }

    /// Extract label value as Float64 with optional conversion.
    ///
    /// Returns NULL if label is missing or conversion fails.
    /// The NULL return allows error tracking via IS NULL checks.
    ///
    /// # Arguments
    /// - `label`: The label name to extract
    /// - `conversion`: Optional conversion function (bytes, duration, etc.)
    /// - `_schema`: `DataFrame` schema (unused, kept for consistency with signature)
    ///
    /// # Returns
    /// `DataFusion` expression that evaluates to Float64 or NULL
    fn extract_unwrapped_value(
        label: &str,
        conversion: Option<crate::logql::log::UnwrapConversion>,
        _schema: &datafusion::common::DFSchema,
    ) -> Expr {
        use crate::logql::log::UnwrapConversion;

        // 1. Extract label from attributes MAP or indexed column
        let label_expr = if Self::is_top_level_field(label) {
            let internal_name = Self::map_label_to_internal_name(label);
            col(internal_name)
        } else {
            datafusion::functions::core::get_field().call(vec![col("attributes"), lit(label)])
        };

        // 2. Apply conversion UDF (returns Float64 or NULL on error)
        let parse_numeric_udf = ScalarUDF::from(super::udf::ParseNumeric::new());
        let parse_bytes_udf = ScalarUDF::from(super::udf::ParseBytes::new());
        let parse_duration_udf = ScalarUDF::from(super::udf::ParseDuration::new());

        let value_expr = match conversion {
            None => parse_numeric_udf.call(vec![label_expr]),
            Some(UnwrapConversion::Bytes) => parse_bytes_udf.call(vec![label_expr]),
            Some(UnwrapConversion::Duration) => parse_duration_udf.call(vec![label_expr, lit(false)]), // nanoseconds
            Some(UnwrapConversion::DurationSeconds) => parse_duration_udf.call(vec![label_expr, lit(true)]), // seconds
        };

        value_expr.alias("unwrapped_value")
    }

    fn apply_pipeline(&self, mut df: DataFrame, pipeline: Vec<crate::logql::log::PipelineStage>) -> Result<DataFrame> {
        use crate::logql::log::PipelineStage;

        for stage in pipeline {
            df = match stage {
                PipelineStage::LineFilter(filter) => self.apply_line_filter(df, filter)?,
                PipelineStage::LogParser(parser) => self.apply_parser(df, parser)?,
                PipelineStage::LabelFormat(ops) => self.apply_label_format(df, ops)?,
                PipelineStage::LineFormat(_template) => {
                    // TODO: Implement line_format using template engine
                    df
                }
                PipelineStage::Decolorize => self.apply_decolorize(df)?,
                PipelineStage::Drop(labels) => Self::apply_drop(df, &labels)?,
                PipelineStage::Keep(labels) => Self::apply_keep(df, &labels)?,
                PipelineStage::LabelFilter(filter_expr) => Self::apply_label_filter(df, filter_expr)?,
            };
        }

        Ok(df)
    }

    #[allow(clippy::unused_self)]
    fn apply_line_filter(&self, df: DataFrame, filter: crate::logql::log::LineFilter) -> Result<DataFrame> {
        use crate::logql::log::{LineFilterOp, LineFilterValue};

        let body_col = col("body");
        let mut combined_expr: Option<Expr> = None;

        for filter_value in filter.filters {
            let filter_str = match filter_value {
                LineFilterValue::String(s) => s,
                LineFilterValue::Ip(_cidr) => {
                    return Err(QueryError::NotImplemented("IP CIDR filtering".into()));
                }
            };

            let expr = match filter.op {
                LineFilterOp::Contains => {
                    datafusion::functions::string::contains().call(vec![body_col.clone(), lit(filter_str)])
                }
                LineFilterOp::NotContains => datafusion::functions::string::contains()
                    .call(vec![body_col.clone(), lit(filter_str)])
                    .not(),
                LineFilterOp::Match => {
                    datafusion::functions::regex::regexp_like().call(vec![body_col.clone(), lit(filter_str)])
                }
                LineFilterOp::NotMatch => datafusion::functions::regex::regexp_like()
                    .call(vec![body_col.clone(), lit(filter_str)])
                    .not(),
                LineFilterOp::NotPattern => {
                    return Err(QueryError::NotImplemented("pattern matching filter".into()));
                }
            };

            combined_expr = Some(match combined_expr {
                Some(existing) => existing.and(expr),
                None => expr,
            });
        }

        match combined_expr {
            Some(expr) => Ok(df.filter(expr)?),
            None => Ok(df),
        }
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    fn apply_parser(&self, df: DataFrame, parser: crate::logql::log::LogParser) -> Result<DataFrame> {
        use crate::logql::log::LogParser;

        // For parsers, we typically invoke a UDF that extracts attributes from the log
        // body and merges them into the attributes map.
        // Since DataFusion doesn't support "merge into map" easily in a single
        // expression without complex UDFs, we'll assume the UDF returns a
        // Map/Struct and we might need to project it. For now, we'll just
        // invoke the UDF and project the result as "attributes" (merging is complex).
        // A real implementation would likely use a specific "extract_and_merge" UDF.

        let _body_col = col("body");

        match parser {
            LogParser::Json(_fields) => {
                // Call json_parser UDF
                // let udf = self.session_ctx.udf("json_parser")?;
                // let args = vec![body_col];
                // let expr = udf.call(args);
                // For now, we'll just return df as we don't have the UDF registered
                // TODO: Implement JSON parsing
                Ok(df)
            }
            LogParser::Logfmt { .. } => {
                // TODO: Implement Logfmt parsing
                Ok(df)
            }
            LogParser::Regexp(_pattern) => {
                // TODO: Implement Regexp parsing
                Ok(df)
            }
            LogParser::Pattern(_pattern) => {
                // TODO: Implement Pattern parsing
                Ok(df)
            }
            LogParser::Unpack => {
                // TODO: Implement Unpack
                Ok(df)
            }
        }
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    fn apply_label_format(&self, df: DataFrame, ops: Vec<crate::logql::common::LabelFormatOp>) -> Result<DataFrame> {
        use crate::logql::common::LabelFormatOp;

        for op in ops {
            match op {
                LabelFormatOp::Rename { .. } | LabelFormatOp::Template { .. } => {
                    // TODO: Implement label rename
                    // Rename is essentially projecting the src column as dst
                }
            }
        }
        Ok(df)
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    const fn apply_decolorize(&self, df: DataFrame) -> Result<DataFrame> {
        // Call decolorize UDF on body
        // let udf = self.session_ctx.udf("decolorize")?;
        // let expr = udf.call(vec![col("body")]);
        // df.select(vec![expr.alias("body"), col("timestamp"), ...])
        // TODO: Implement decolorize
        Ok(df)
    }

    /// Apply `LogQL` `drop` operator - removes specified labels from attributes
    /// map.
    ///
    /// Uses the `map_drop_keys` UDF to filter the attributes map, removing
    /// entries whose keys match the specified labels.
    fn apply_drop(df: DataFrame, labels: &[crate::logql::common::LabelExtraction]) -> Result<DataFrame> {
        if labels.is_empty() {
            return Ok(df);
        }

        // Build array of label names to drop
        let label_literals: Vec<Expr> = labels.iter().map(|l| lit(l.name.as_str())).collect();

        // Get the map_drop_keys UDF
        let udf = ScalarUDF::from(super::udf::MapDropKeys::new());

        // Create filtered attributes expression
        let filtered_attrs = udf.call(vec![
            col("attributes"),
            datafusion::functions_nested::make_array::make_array(label_literals),
        ]);

        // Select all columns, replacing attributes with filtered version
        let select_exprs: Vec<Expr> = df
            .schema()
            .inner()
            .fields()
            .iter()
            .map(|field| {
                if field.name() == "attributes" {
                    filtered_attrs.clone().alias("attributes")
                } else {
                    col(field.name().as_str())
                }
            })
            .collect();

        Ok(df.select(select_exprs)?)
    }

    /// Apply `LogQL` `keep` operator - keeps only specified labels in
    /// attributes map.
    ///
    /// Uses the `map_keep_keys` UDF to filter the attributes map, keeping
    /// only entries whose keys match the specified labels.
    fn apply_keep(df: DataFrame, labels: &[crate::logql::common::LabelExtraction]) -> Result<DataFrame> {
        if labels.is_empty() {
            // keep with empty list = keep nothing (empty attributes)
            // But this might be unexpected, so we return as-is for now
            return Ok(df);
        }

        // Build array of label names to keep
        let label_literals: Vec<Expr> = labels.iter().map(|l| lit(l.name.as_str())).collect();

        // Get the map_keep_keys UDF
        let udf = ScalarUDF::from(super::udf::MapKeepKeys::new());

        // Create filtered attributes expression
        let filtered_attrs = udf.call(vec![
            col("attributes"),
            datafusion::functions_nested::make_array::make_array(label_literals),
        ]);

        // Select all columns, replacing attributes with filtered version
        let select_exprs: Vec<Expr> = df
            .schema()
            .inner()
            .fields()
            .iter()
            .map(|field| {
                if field.name() == "attributes" {
                    filtered_attrs.clone().alias("attributes")
                } else {
                    col(field.name().as_str())
                }
            })
            .collect();

        Ok(df.select(select_exprs)?)
    }

    fn apply_label_filter(df: DataFrame, filter_expr: crate::logql::log::LabelFilterExpr) -> Result<DataFrame> {
        let expr = Self::label_filter_to_expr(filter_expr)?;
        Ok(df.filter(expr)?)
    }

    #[allow(clippy::items_after_statements)]
    fn label_filter_to_expr(filter: crate::logql::log::LabelFilterExpr) -> Result<Expr> {
        use crate::logql::log::LabelFilterExpr;

        match filter {
            LabelFilterExpr::And(left, right) => {
                let left_expr = Self::label_filter_to_expr(*left)?;
                let right_expr = Self::label_filter_to_expr(*right)?;
                Ok(left_expr.and(right_expr))
            }
            LabelFilterExpr::Or(left, right) => {
                let left_expr = Self::label_filter_to_expr(*left)?;
                let right_expr = Self::label_filter_to_expr(*right)?;
                Ok(left_expr.or(right_expr))
            }
            LabelFilterExpr::Parens(inner) => Self::label_filter_to_expr(*inner),
            LabelFilterExpr::Matcher(matcher) => Ok(Self::matcher_to_expr(&matcher)),
            LabelFilterExpr::Number { label, op, value } => {
                let internal_name = Self::map_label_to_internal_name(&label);
                let col_expr = if Self::is_top_level_field(internal_name) {
                    col(internal_name)
                } else {
                    datafusion::functions::core::get_field().call(vec![col("attributes"), lit(label)])
                };

                use crate::logql::common::ComparisonOp;
                let expr = match op {
                    ComparisonOp::Gt => col_expr.gt(lit(value)),
                    ComparisonOp::Ge => col_expr.gt_eq(lit(value)),
                    ComparisonOp::Lt => col_expr.lt(lit(value)),
                    ComparisonOp::Le => col_expr.lt_eq(lit(value)),
                    ComparisonOp::Eq => col_expr.eq(lit(value)),
                    ComparisonOp::Neq => col_expr.not_eq(lit(value)),
                };
                Ok(expr)
            }
            LabelFilterExpr::Duration { label, op, value } => {
                // Convert duration to nanoseconds and compare
                let internal_name = Self::map_label_to_internal_name(&label);
                let col_expr = if Self::is_top_level_field(internal_name) {
                    col(internal_name)
                } else {
                    datafusion::functions::core::get_field().call(vec![col("attributes"), lit(label)])
                };

                use crate::logql::common::ComparisonOp;
                let nanos = value
                    .num_nanoseconds()
                    .ok_or(QueryError::Config("Duration too large".to_string()))?;
                let expr = match op {
                    ComparisonOp::Gt => col_expr.gt(lit(nanos)),
                    ComparisonOp::Ge => col_expr.gt_eq(lit(nanos)),
                    ComparisonOp::Lt => col_expr.lt(lit(nanos)),
                    ComparisonOp::Le => col_expr.lt_eq(lit(nanos)),
                    ComparisonOp::Eq => col_expr.eq(lit(nanos)),
                    ComparisonOp::Neq => col_expr.not_eq(lit(nanos)),
                };
                Ok(expr)
            }
            LabelFilterExpr::Bytes { label, op, value } => {
                // Compare byte values as u64
                let internal_name = Self::map_label_to_internal_name(&label);
                let col_expr = if Self::is_top_level_field(internal_name) {
                    col(internal_name)
                } else {
                    datafusion::functions::core::get_field().call(vec![col("attributes"), lit(label)])
                };

                use crate::logql::common::ComparisonOp;
                let expr = match op {
                    ComparisonOp::Gt => col_expr.gt(lit(value)),
                    ComparisonOp::Ge => col_expr.gt_eq(lit(value)),
                    ComparisonOp::Lt => col_expr.lt(lit(value)),
                    ComparisonOp::Le => col_expr.lt_eq(lit(value)),
                    ComparisonOp::Eq => col_expr.eq(lit(value)),
                    ComparisonOp::Neq => col_expr.not_eq(lit(value)),
                };
                Ok(expr)
            }
            LabelFilterExpr::Ip { .. } => {
                // TODO: Implement IP filtering using ip_match UDF
                Err(QueryError::NotImplemented(
                    "IP filtering not yet implemented".to_string(),
                ))
            }
        }
    }
}
