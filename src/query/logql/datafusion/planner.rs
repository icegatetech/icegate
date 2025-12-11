//! DataFusion-based LogQL query planner.

// Allow warnings for stub implementations - these will be fixed as features are
// implemented
use std::{future::Future, pin::Pin, sync::Arc};

use chrono::{DateTime, TimeDelta, Utc};
use datafusion::{
    arrow::{
        datatypes::{DataType, Field, IntervalMonthDayNano, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    common::Column,
    datasource::MemTable,
    functions_aggregate::{count::count_udaf, expr_fn::last_value, sum::sum_udaf},
    logical_expr::{
        col, expr::WindowFunction, lit, AggregateUDF, Expr, ExprSchemable, JoinType, ScalarUDF, WindowFrame,
        WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    },
    prelude::*,
    scalar::ScalarValue,
};

use crate::{
    common::{errors::IceGateError, schema::INDEXED_ATTRIBUTE_COLUMNS, Result, LOGS_TABLE_FQN},
    query::logql::{
        common::MatchOp,
        expr::LogQLExpr,
        log::{LabelMatcher, LogExpr, Selector},
        metric::MetricExpr,
        planner::{Planner, QueryContext, DEFAULT_LOG_LIMIT},
        RangeAggregationOp,
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
        Self {
            session_ctx,
            query_ctx,
        }
    }
}

impl Planner for DataFusionPlanner {
    type Plan = DataFrame;

    async fn plan(&self, expr: LogQLExpr) -> Result<Self::Plan> {
        match expr {
            LogQLExpr::Log(log_expr) => {
                let df = self.plan_log(log_expr, self.query_ctx.start, self.query_ctx.end).await?;
                // Apply limit only for log queries (not metrics)
                // Uses context limit or Loki default of 100 entries
                let limit = self.query_ctx.limit.unwrap_or(DEFAULT_LOG_LIMIT);
                let df = df.limit(0, Some(limit))?;
                // Transform output: hex-encode binary columns, add `level` alias
                Self::transform_output_columns(df)
            },
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

        for &col_name in INDEXED_ATTRIBUTE_COLUMNS {
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
    /// - Add `level` column as alias of `severity_text` (Grafana expects
    ///   `level` label)
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
            return Err(IceGateError::Plan("At least one selector is required".to_string()));
        }

        // Plan first selector as base
        let log_expr = LogExpr::new(selectors[0].clone());
        let df = self.plan_log(log_expr, self.query_ctx.start, self.query_ctx.end).await?;

        // If multiple selectors, OR their filters together
        let df = if selectors.len() > 1 {
            let mut or_filters: Vec<Expr> = Vec::new();
            for selector in &selectors[1..] {
                if let Some(filter) = selector.matchers.iter().map(Self::matcher_to_expr).reduce(Expr::and) {
                    or_filters.push(filter);
                }
            }
            if or_filters.is_empty() {
                df
            } else if let Some(or_filter) = or_filters.into_iter().reduce(Expr::or) {
                df.filter(or_filter)?
            } else {
                df
            }
        } else {
            df
        };

        // Build select with serialized attributes for grouping
        // Convert binary columns to hex strings for proper grouping and output
        // Cast FixedSizeBinary to Binary first since encode() requires Binary type
        let schema = df.schema().clone();
        let mut select_cols: Vec<Expr> = INDEXED_ATTRIBUTE_COLUMNS
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
        // Serialize map keys and values for grouping
        select_cols.push(array_to_string(map_keys(col("attributes")), lit(",")).alias("_attr_keys"));
        select_cols.push(array_to_string(map_values(col("attributes")), lit(",")).alias("_attr_vals"));

        let df = df.select(select_cols)?;

        // Group by indexed columns + level + serialized attributes
        let mut group_cols: Vec<Expr> = INDEXED_ATTRIBUTE_COLUMNS.iter().map(|c| col(*c)).collect();
        group_cols.push(col("level"));
        group_cols.push(col("_attr_keys"));
        group_cols.push(col("_attr_vals"));

        let df = df.aggregate(group_cols, vec![
            first_value(col("attributes"), vec![]).alias("attributes")
        ])?;

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
                    Err(IceGateError::NotImplemented(
                        "Binary operations not yet implemented".to_string(),
                    ))
                },
                MetricExpr::Literal(val) => {
                    // Return a DataFrame with a single row containing the literal value
                    let df = self.session_ctx.read_empty()?;
                    Ok(df.select(vec![lit(val).alias("value")])?)
                },
                MetricExpr::Vector(_vals) => {
                    // TODO: Implement vector literal
                    Err(IceGateError::NotImplemented(
                        "Vector literal not yet implemented".to_string(),
                    ))
                },
                MetricExpr::LabelReplace {
                    ..
                } => {
                    // TODO: Implement label replace
                    Err(IceGateError::NotImplemented(
                        "Label replace not yet implemented".to_string(),
                    ))
                },
                MetricExpr::Variable(_) => {
                    Err(IceGateError::NotImplemented("Variable not yet implemented".to_string()))
                },
                MetricExpr::Parens(inner) => self.plan_metric(*inner).await,
            }
        })
    }

    async fn plan_range_aggregation(&self, agg: crate::query::logql::metric::RangeAggregation) -> Result<DataFrame> {
        if agg.range_expr.unwrap.is_some() {
            Ok(self.plan_unwrap_range_aggregation(agg)?)
        } else {
            Ok(self.plan_log_range_aggregation(agg).await?)
        }
    }

    #[allow(clippy::unused_self)]
    fn plan_unwrap_range_aggregation(&self, _agg: crate::query::logql::metric::RangeAggregation) -> Result<DataFrame> {
        Err(IceGateError::NotImplemented(
            "Unwrap aggregation not yet implemented".to_string(),
        ))
    }

    async fn plan_log_range_aggregation(
        &self,
        agg: crate::query::logql::metric::RangeAggregation,
    ) -> Result<DataFrame> {
        // 1. Plan the inner LogExpr with extended time range for lookback window
        // For rate({job="x"}[5m]) evaluated from start to end:
        // - Each evaluation point T needs data from (T - range) to T
        // - So we need data from (start - range) to end
        // - With offset: (start - range - offset) to (end - offset)
        let offset_duration = agg.range_expr.offset.unwrap_or(TimeDelta::zero());

        // Start: context.start - range - offset (for lookback window)
        // End: context.end - offset
        let adjusted_start = self.query_ctx.start - agg.range_expr.range - offset_duration;
        let adjusted_end = self.query_ctx.end - offset_duration;

        let df = self.plan_log(agg.range_expr.log_expr, adjusted_start, adjusted_end).await?;

        // Create data grid, cross join with unique set of labels
        // Note: Exclude timestamp - it comes from the time grid via cross-join
        let mut grouping_for_grid = Self::build_default_label_exprs(&[], &["attributes"]);
        grouping_for_grid.extend(vec![
            array_to_string(map_keys(col("attributes")), lit(",")).alias("attributes_keys"),
            array_to_string(map_values(col("attributes")), lit(",")).alias("attributes_values"),
        ]);
        let timestamps = self.generate_time_grid()?;
        let grid_df = self.create_time_grid(timestamps)?;

        // Get unique label combinations and cross-join with time grid
        let agg_df = df.clone().aggregate(grouping_for_grid, vec![
            last_value(col("attributes"), vec![]).alias("attributes")
        ])?;
        let labeled_grid_df = agg_df.join(grid_df, JoinType::Inner, &[], &[], None)?.alias("grid")?;

        // 2. Build time bucket expression using step from context
        let time_bucket = self.build_time_bucket(agg.range_expr.range);
        let window_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
                0,
                0,
                agg.range_expr.range.num_nanoseconds().unwrap_or(0) + offset_duration.num_nanoseconds().unwrap_or(0),
            )))),
            WindowFrameBound::Preceding(ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
                0,
                0,
                offset_duration.num_nanoseconds().unwrap_or(0),
            )))),
        );

        let mut grouping_for_window = Self::build_default_label_exprs(&[], &["attributes"]);
        grouping_for_window.extend(vec![map_keys(col("attributes")), map_values(col("attributes"))]);

        let range_secs = agg.range_expr.range.as_seconds_f32();
        let window_expr = match agg.op {
            RangeAggregationOp::CountOverTime => {
                Self::build_log_window_expr(count_udaf(), lit(1), window_frame, grouping_for_window, None)?
            },
            RangeAggregationOp::Rate => Self::build_log_window_expr(
                count_udaf(),
                lit(1),
                window_frame,
                grouping_for_window,
                Some(range_secs),
            )?,
            RangeAggregationOp::BytesOverTime => Self::build_log_window_expr(
                sum_udaf(),
                octet_length(col("body")),
                window_frame,
                grouping_for_window,
                None,
            )?,
            RangeAggregationOp::BytesRate => Self::build_log_window_expr(
                sum_udaf(),
                octet_length(col("body")),
                window_frame,
                grouping_for_window,
                Some(range_secs),
            )?,
            RangeAggregationOp::AbsentOverTime => {
                return Err(IceGateError::NotImplemented("Absent over time".to_string()));
            },
            _ => {
                return Err(IceGateError::Plan(
                    "This range aggregation requires an unwrap expression".to_string(),
                ))
            },
        };

        let mut select_list = Self::build_default_label_exprs(&["timestamp"], &[]);
        select_list.extend(vec![window_expr.alias("value"), time_bucket.alias("time_bucket")]);
        let df = df.select(select_list)?;
        let mut agg_list = Self::build_default_label_exprs(&[], &["attributes"]);
        agg_list.extend(vec![
            col("time_bucket").alias("timestamp"),
            array_to_string(map_keys(col("attributes")), lit(",")).alias("attributes_keys"),
            array_to_string(map_values(col("attributes")), lit(",")).alias("attributes_values"),
        ]);
        let df = df
            .aggregate(agg_list, vec![
                last_value(col("value"), vec![col("timestamp").sort(true, false)]).alias("value"),
                last_value(col("attributes"), vec![]).alias("attributes"),
            ])?
            .alias("origin")?;

        let names: Vec<String> =
            Self::build_default_label_columns(&["timestamp", "attributes_keys", "attributes_values"], &["attributes"]);
        let names_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let df = labeled_grid_df.join(df, JoinType::Left, &names_refs, &names_refs, None)?;

        // After LEFT JOIN, both sides have columns like severity_text, causing
        // ambiguity. Select specific columns using qualified names: labels from
        // grid, value from origin.
        let mut final_select = Vec::new();
        // Label columns from grid side (grid has all time points, origin may have gaps)
        for name in Self::build_default_label_columns(&["timestamp"], &["attributes"]) {
            final_select.push(col(Column::new(Some("grid"), &name)));
        }
        // Value from origin (NULL if no matching data point - LEFT JOIN)
        final_select.push(col(Column::new(Some("origin"), "value")));
        // Attributes from grid (preserved label combinations)
        final_select.push(col(Column::new(Some("grid"), "attributes")));
        let df = df.select(final_select)?;

        Ok(df)
    }

    // ========================================================================
    // Range Aggregation Helper Methods
    // ========================================================================

    /// Builds a window expression for log-based range aggregations.
    ///
    /// Creates a window function with the specified aggregation function and
    /// argument, applies the window frame, ordering, and partitioning, then
    /// optionally divides by the rate divisor for rate calculations.
    fn build_log_window_expr(
        agg_func: Arc<AggregateUDF>,
        agg_arg: Expr,
        window_frame: WindowFrame,
        grouping: Vec<Expr>,
        rate_divisor: Option<f32>,
    ) -> Result<Expr> {
        let window_func = WindowFunction::new(WindowFunctionDefinition::from(agg_func), vec![agg_arg]);
        let expr = Expr::from(window_func)
            .window_frame(window_frame)
            .order_by(vec![col("timestamp").sort(true, false)])
            .partition_by(grouping)
            .build()?;

        Ok(match rate_divisor {
            Some(divisor) => expr.div(lit(divisor)),
            None => expr,
        })
    }

    /// Converts microseconds to DataFusion interval literal for `date_bin`.
    /// Uses microseconds since the timestamp column has microsecond precision.
    fn micros_to_interval_literal(micros: i64) -> Expr {
        // DataFusion IntervalMonthDayNano uses nanoseconds internally
        let nanos = micros * 1000;
        lit(ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
            months: 0,
            days: 0,
            nanoseconds: nanos,
        })))
    }

    /// Creates `date_bin` expression for time bucketing.
    /// Uses `context.step` from Loki API for bucketing, falls back to `range`
    /// if not provided. Interval is truncated to microseconds since
    /// timestamp column has microsecond precision.
    fn build_time_bucket(&self, range: chrono::TimeDelta) -> Expr {
        // TODO: rework it to use micros only precision
        let bucket_nanos = self.query_ctx.step.unwrap_or(range).num_nanoseconds().unwrap_or(0);
        // Truncate to microseconds (timestamp column precision)
        let bucket_micros = bucket_nanos / 1000;
        datafusion::functions::datetime::date_bin().call(vec![
            Self::micros_to_interval_literal(bucket_micros),
            col("timestamp"),
            lit(ScalarValue::TimestampMicrosecond(
                Some(self.query_ctx.start.timestamp_micros()),
                None,
            )),
        ])
    }

    fn build_default_label_columns(with: &[&str], without: &[&str]) -> Vec<String> {
        INDEXED_ATTRIBUTE_COLUMNS
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

    // ========================================================================
    // Time Grid Generation (for gap filling)
    // ========================================================================

    /// Generates time bucket timestamps from start to end with step interval.
    /// Returns microseconds (matching timestamp column precision).
    #[allow(clippy::cast_possible_truncation)]
    fn generate_time_grid(&self) -> Result<Vec<i64>> {
        let step_micros = self
            .query_ctx
            .step
            .ok_or(IceGateError::Config(
                "Step parameter is required for metric query".to_string(),
            ))?
            .num_microseconds()
            .ok_or(IceGateError::Config(
                "Step parameter is very big in microsecond ratio".to_string(),
            ))?;

        let start_micros = self.query_ctx.start.timestamp_micros();
        let end_micros = self.query_ctx.end.timestamp_micros();

        let mut timestamps = Vec::new();
        let mut current = start_micros;

        while current <= end_micros {
            timestamps.push(current);
            current += step_micros;
        }

        Ok(timestamps)
    }

    /// Creates a `DataFrame` representing the time grid as an in-memory table.
    fn create_time_grid(&self, timestamps: Vec<i64>) -> Result<DataFrame> {
        use datafusion::arrow::array::TimestampMicrosecondArray;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));

        let time_array = TimestampMicrosecondArray::from(timestamps);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(time_array)])
            .map_err(|e| IceGateError::Config(format!("Failed to create time grid batch: {e}")))?;

        let mem_table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| IceGateError::Config(format!("Failed to create time grid table: {e}")))?;

        Ok(self.session_ctx.read_table(Arc::new(mem_table))?)
    }

    async fn plan_vector_aggregation(&self, agg: crate::query::logql::metric::VectorAggregation) -> Result<DataFrame> {
        use crate::query::logql::{common::Grouping, metric::MetricExpr};

        // 1. Push grouping down to inner RangeAggregation if present
        // For queries like `sum by (level) (count_over_time({...}[1m]))`,
        // we need the inner range aggregation to also group by `level`.
        let inner_expr = match (*agg.expr, &agg.grouping) {
            (MetricExpr::RangeAggregation(mut range_agg), Some(outer_grouping)) => {
                // Merge outer grouping into inner range aggregation
                // TODO: Process By and Without differently - Without should exclude labels
                #[allow(clippy::match_same_arms)]
                let outer_labels = match outer_grouping {
                    Grouping::By(labels) => labels.clone(),
                    Grouping::Without(labels) => labels.clone(),
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
                    },
                    Some(Grouping::Without(inner_labels)) => {
                        // Keep the inner 'without' as-is
                        Some(Grouping::Without(inner_labels))
                    },
                    None => {
                        // No inner grouping, use outer labels
                        Some(Grouping::By(outer_labels))
                    },
                };
                range_agg.grouping = merged_grouping;
                MetricExpr::RangeAggregation(range_agg)
            },
            (expr, _) => expr,
        };

        // 2. Plan the inner MetricExpr (now with merged grouping)
        let df = self.plan_metric(inner_expr).await?;
        let schema = df.schema();

        // 3. Identify grouping columns for the vector aggregation
        // LogQL: sum by (label1, label2) (...)
        // DataFusion: group_expr = [col("label1"), col("label2")]
        // TODO: Process By and Without differently - Without should exclude labels from
        // grouping
        let mut group_exprs = if let Some(grouping) = &agg.grouping {
            let labels = match grouping {
                Grouping::By(labels) | Grouping::Without(labels) => labels,
            };
            let udf = match grouping {
                Grouping::By(_) => ScalarUDF::from(super::udf::MapKeepKeys::new()),
                Grouping::Without(_) => ScalarUDF::from(super::udf::MapDropKeys::new()),
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
                    attributes.push(mapped_name);
                } else {
                    // Column doesn't exist and no attributes map - the label isn't available
                    return Err(IceGateError::NotImplemented(format!(
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
            crate::query::logql::metric::VectorAggregationOp::Sum => {
                datafusion::functions_aggregate::expr_fn::sum(value_col)
            },
            crate::query::logql::metric::VectorAggregationOp::Avg => {
                datafusion::functions_aggregate::expr_fn::avg(value_col)
            },
            crate::query::logql::metric::VectorAggregationOp::Min => {
                datafusion::functions_aggregate::expr_fn::min(value_col)
            },
            crate::query::logql::metric::VectorAggregationOp::Max => {
                datafusion::functions_aggregate::expr_fn::max(value_col)
            },
            crate::query::logql::metric::VectorAggregationOp::Count => {
                datafusion::functions_aggregate::expr_fn::count(value_col)
            },
            crate::query::logql::metric::VectorAggregationOp::Stddev => {
                datafusion::functions_aggregate::expr_fn::stddev(value_col)
            },
            crate::query::logql::metric::VectorAggregationOp::Stdvar => {
                datafusion::functions_aggregate::expr_fn::var_sample(value_col)
            },
            // TODO: Implement topk, bottomk, sort, sort_desc
            _ => {
                return Err(IceGateError::NotImplemented(format!(
                    "Vector aggregation op {:?} not supported",
                    agg.op
                )));
            },
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
            },
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
            _ => name,
        }
    }

    /// Check if a label name corresponds to a top-level indexed column.
    ///
    /// Top-level fields are stored as separate columns in the Iceberg table,
    /// while other labels are stored in the `attributes` MAP column.
    pub fn is_top_level_field(name: &str) -> bool {
        let mapped = Self::map_label_to_internal_name(name);
        INDEXED_ATTRIBUTE_COLUMNS.contains(&mapped) || matches!(mapped, "tenant_id" | "timestamp")
    }

    fn apply_pipeline(
        &self,
        mut df: DataFrame,
        pipeline: Vec<crate::query::logql::log::PipelineStage>,
    ) -> Result<DataFrame> {
        use crate::query::logql::log::PipelineStage;

        for stage in pipeline {
            df = match stage {
                PipelineStage::LineFilter(filter) => self.apply_line_filter(df, filter)?,
                PipelineStage::LogParser(parser) => self.apply_parser(df, parser)?,
                PipelineStage::LabelFormat(ops) => self.apply_label_format(df, ops)?,
                PipelineStage::LineFormat(_template) => {
                    // TODO: Implement line_format using template engine
                    df
                },
                PipelineStage::Decolorize => self.apply_decolorize(df)?,
                PipelineStage::Drop(labels) => Self::apply_drop(df, &labels)?,
                PipelineStage::Keep(labels) => Self::apply_keep(df, &labels)?,
                PipelineStage::LabelFilter(filter_expr) => Self::apply_label_filter(df, filter_expr)?,
            };
        }

        Ok(df)
    }

    #[allow(clippy::unused_self)]
    fn apply_line_filter(&self, df: DataFrame, filter: crate::query::logql::log::LineFilter) -> Result<DataFrame> {
        use crate::query::logql::log::{LineFilterOp, LineFilterValue};

        let body_col = col("body");
        let mut combined_expr: Option<Expr> = None;

        for filter_value in filter.filters {
            let filter_str = match filter_value {
                LineFilterValue::String(s) => s,
                LineFilterValue::Ip(_cidr) => {
                    return Err(IceGateError::NotImplemented("IP CIDR filtering".into()));
                },
            };

            let expr = match filter.op {
                LineFilterOp::Contains => {
                    datafusion::functions::string::contains().call(vec![body_col.clone(), lit(filter_str)])
                },
                LineFilterOp::NotContains => datafusion::functions::string::contains()
                    .call(vec![body_col.clone(), lit(filter_str)])
                    .not(),
                LineFilterOp::Match => {
                    datafusion::functions::regex::regexp_like().call(vec![body_col.clone(), lit(filter_str)])
                },
                LineFilterOp::NotMatch => datafusion::functions::regex::regexp_like()
                    .call(vec![body_col.clone(), lit(filter_str)])
                    .not(),
                LineFilterOp::NotPattern => {
                    return Err(IceGateError::NotImplemented("pattern matching filter".into()));
                },
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
    fn apply_parser(&self, df: DataFrame, parser: crate::query::logql::log::LogParser) -> Result<DataFrame> {
        use crate::query::logql::log::LogParser;

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
            },
            LogParser::Logfmt {
                ..
            } => {
                // TODO: Implement Logfmt parsing
                Ok(df)
            },
            LogParser::Regexp(_pattern) => {
                // TODO: Implement Regexp parsing
                Ok(df)
            },
            LogParser::Pattern(_pattern) => {
                // TODO: Implement Pattern parsing
                Ok(df)
            },
            LogParser::Unpack => {
                // TODO: Implement Unpack
                Ok(df)
            },
        }
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    fn apply_label_format(
        &self,
        df: DataFrame,
        ops: Vec<crate::query::logql::common::LabelFormatOp>,
    ) -> Result<DataFrame> {
        use crate::query::logql::common::LabelFormatOp;

        for op in ops {
            match op {
                LabelFormatOp::Rename {
                    ..
                }
                | LabelFormatOp::Template {
                    ..
                } => {
                    // TODO: Implement label rename
                    // Rename is essentially projecting the src column as dst
                },
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
    fn apply_drop(df: DataFrame, labels: &[crate::query::logql::common::LabelExtraction]) -> Result<DataFrame> {
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
    fn apply_keep(df: DataFrame, labels: &[crate::query::logql::common::LabelExtraction]) -> Result<DataFrame> {
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

    fn apply_label_filter(df: DataFrame, filter_expr: crate::query::logql::log::LabelFilterExpr) -> Result<DataFrame> {
        let expr = Self::label_filter_to_expr(filter_expr)?;
        Ok(df.filter(expr)?)
    }

    #[allow(clippy::items_after_statements)]
    fn label_filter_to_expr(filter: crate::query::logql::log::LabelFilterExpr) -> Result<Expr> {
        use crate::query::logql::log::LabelFilterExpr;

        match filter {
            LabelFilterExpr::And(left, right) => {
                let left_expr = Self::label_filter_to_expr(*left)?;
                let right_expr = Self::label_filter_to_expr(*right)?;
                Ok(left_expr.and(right_expr))
            },
            LabelFilterExpr::Or(left, right) => {
                let left_expr = Self::label_filter_to_expr(*left)?;
                let right_expr = Self::label_filter_to_expr(*right)?;
                Ok(left_expr.or(right_expr))
            },
            LabelFilterExpr::Parens(inner) => Self::label_filter_to_expr(*inner),
            LabelFilterExpr::Matcher(matcher) => Ok(Self::matcher_to_expr(&matcher)),
            LabelFilterExpr::Number {
                label,
                op,
                value,
            } => {
                let col_expr = if Self::is_top_level_field(&label) {
                    col(label)
                } else {
                    datafusion::functions::core::get_field().call(vec![col("attributes"), lit(label)])
                };

                use crate::query::logql::common::ComparisonOp;
                let expr = match op {
                    ComparisonOp::Gt => col_expr.gt(lit(value)),
                    ComparisonOp::Ge => col_expr.gt_eq(lit(value)),
                    ComparisonOp::Lt => col_expr.lt(lit(value)),
                    ComparisonOp::Le => col_expr.lt_eq(lit(value)),
                    ComparisonOp::Eq => col_expr.eq(lit(value)),
                    ComparisonOp::Neq => col_expr.not_eq(lit(value)),
                };
                Ok(expr)
            },
            LabelFilterExpr::Duration {
                label,
                op,
                value,
            } => {
                // Convert duration to nanoseconds and compare
                let col_expr = if Self::is_top_level_field(&label) {
                    col(label)
                } else {
                    datafusion::functions::core::get_field().call(vec![col("attributes"), lit(label)])
                };

                use crate::query::logql::common::ComparisonOp;
                let nanos = value.num_nanoseconds().unwrap_or(0);
                let expr = match op {
                    ComparisonOp::Gt => col_expr.gt(lit(nanos)),
                    ComparisonOp::Ge => col_expr.gt_eq(lit(nanos)),
                    ComparisonOp::Lt => col_expr.lt(lit(nanos)),
                    ComparisonOp::Le => col_expr.lt_eq(lit(nanos)),
                    ComparisonOp::Eq => col_expr.eq(lit(nanos)),
                    ComparisonOp::Neq => col_expr.not_eq(lit(nanos)),
                };
                Ok(expr)
            },
            LabelFilterExpr::Bytes {
                label,
                op,
                value,
            } => {
                // Compare byte values as u64
                let col_expr = if Self::is_top_level_field(&label) {
                    col(label)
                } else {
                    datafusion::functions::core::get_field().call(vec![col("attributes"), lit(label)])
                };

                use crate::query::logql::common::ComparisonOp;
                let expr = match op {
                    ComparisonOp::Gt => col_expr.gt(lit(value)),
                    ComparisonOp::Ge => col_expr.gt_eq(lit(value)),
                    ComparisonOp::Lt => col_expr.lt(lit(value)),
                    ComparisonOp::Le => col_expr.lt_eq(lit(value)),
                    ComparisonOp::Eq => col_expr.eq(lit(value)),
                    ComparisonOp::Neq => col_expr.not_eq(lit(value)),
                };
                Ok(expr)
            },
            LabelFilterExpr::Ip {
                ..
            } => {
                // TODO: Implement IP filtering using ip_match UDF
                Err(IceGateError::NotImplemented(
                    "IP filtering not yet implemented".to_string(),
                ))
            },
        }
    }
}
