//! DataFusion-based LogQL query planner.

// Allow warnings for stub implementations - these will be fixed as features are
// implemented
use std::{future::Future, pin::Pin};

/// Internal column name for serialized attribute keys.
const COL_ATTR_KEYS: &str = "_attr_keys";
/// Internal column name for serialized attribute values.
const COL_ATTR_VALS: &str = "_attr_vals";
/// Internal column name for a once-per-row materialized [`merged_attributes`]
/// result. Never part of a query's output label set: every consumer either
/// projects it away explicitly or, in an aggregate, simply omits it from the
/// group/aggregate expression list, which drops it from that step's output
/// schema.
const COL_MERGED_ATTRS: &str = "_merged_attrs";

/// Name of the merged, normalized attribute map in a `LogQL` query
/// pipeline's output schema: produced by the series/vector/range-aggregation
/// plans below (via [`merged_attributes`] + `map_merge_normalized`) and read
/// back by `loki::formatters` to build the response.
///
/// A `DataFusion` `DataFrame`/`RecordBatch` column, not a stored table
/// column — no Iceberg table carries a top-level `attributes` field any
/// more (see `icegate_common::schema`'s per-level maps —
/// `resource_attributes`, `scope_attributes`, and
/// `log_attributes`/`span_attributes`/`data_point_attributes` — which
/// replaced it). Contrast [`COL_MERGED_ATTRS`] above, which stages the same
/// merge but never leaves this file.
pub(crate) const MERGED_ATTRIBUTES_COLUMN: &str = "attributes";

/// Returns the `FixedSizeBinary` width in bytes for a binary identifier
/// column on the `logs` table, or `None` for non-binary columns. Centralises
/// the column-name → width mapping shared by `matcher_to_expr` and
/// `binary_id_matcher_to_expr`. Only `trace_id` / `span_id` are top-level
/// fields on logs (`parent_span_id` is not indexed there — see
/// `LOG_INDEXED_ATTRIBUTE_COLUMNS`).
const fn binary_id_width(column: &str) -> Option<i32> {
    if string_eq(column, COL_TRACE_ID) {
        Some(16)
    } else if string_eq(column, COL_SPAN_ID) {
        Some(8)
    } else {
        None
    }
}

/// `const`-friendly string equality used by [`binary_id_width`]. Plain `==`
/// on `&str` is not yet `const`-callable on stable Rust 1.95, so we compare
/// the underlying byte slices manually. Both inputs are static column-name
/// constants (`COL_TRACE_ID`, etc.), so this runs at most a few iterations.
const fn string_eq(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.len() != b.len() {
        return false;
    }
    let mut i = 0;
    while i < a.len() {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

use chrono::{DateTime, TimeDelta, Utc};
use datafusion::functions_aggregate::expr_fn::{avg, bool_or, max, min, stddev, var_sample};
use datafusion::{
    arrow::datatypes::{DataType, IntervalMonthDayNano},
    common::DFSchema,
    functions::string::octet_length,
    functions_aggregate::expr_fn::{count, last_value, sum},
    functions_nested::make_array::make_array,
    logical_expr::{Expr, ExprSchemable, ScalarUDF, col, lit, when},
    prelude::*,
    scalar::ScalarValue,
};
use icegate_common::{
    LOGS_TABLE_FQN,
    schema::{
        COL_BODY, COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME,
        COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID, LEVEL_ALIAS,
        LOG_INDEXED_ATTRIBUTE_COLUMNS, LOG_SERIES_LABEL_COLUMNS,
    },
};

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

/// Returns `true` when `schema` already carries the single merged
/// [`MERGED_ATTRIBUTES_COLUMN`] rather than the three per-level attribute
/// columns (`log_attributes`/`scope_attributes`/`resource_attributes`).
///
/// A `LogQL` pipeline collapses the three per-level columns into the merged
/// column the first time a stage needs to read or filter attributes as one
/// map — see [`DataFusionPlanner::apply_attributes_filter`]. Every helper
/// that builds an attribute-reading expression MUST re-check this against
/// the schema of the `DataFrame` it will actually run against, rather than
/// assume the pre-merge, freshly-scanned shape: a `LabelFilter` stage
/// following `drop`/`keep`, or an aggregation whose inner pipeline included
/// `drop`/`keep`, sees a schema where the three raw columns no longer
/// exist — referencing them there is a schema error at plan construction
/// (`No field named ...log_attributes`), not a row-level NULL. This was
/// exactly the C1 regression: `attribute_lookup`/`merged_attributes` used to
/// hardcode the pre-merge columns unconditionally.
///
/// This is the single source of truth for the check: [`attribute_lookup`],
/// [`merged_attributes`], and (transitively, via `merged_attributes`)
/// [`DataFusionPlanner::resolve_current_attributes`] all defer to it, so a
/// future edit cannot make this determination two different ways that drift
/// apart — the failure mode that produced C1 in the first place.
fn schema_has_merged_attributes(schema: &DFSchema) -> bool {
    schema.inner().fields().iter().any(|f| f.name() == MERGED_ATTRIBUTES_COLUMN)
}

/// Resolve a Loki label name against the attribute maps of the `DataFrame`
/// whose schema is `schema`.
///
/// When [`schema_has_merged_attributes`] is `true`, the key is looked up
/// directly in [`MERGED_ATTRIBUTES_COLUMN`] with a single UDF call and no
/// coalesce: the merge that produced that column already applied the
/// log -> scope -> resource precedence below, and its keys are already
/// underscored wire names (`normalize_key` is then a no-op on them), so the
/// same lookup UDF is correct unchanged on this shape. Do not "simplify"
/// this by always coalescing the three per-level columns — once a pipeline
/// stage has merged them, those columns no longer exist on the `DataFrame`
/// and referencing them is a schema error, not a fallback.
///
/// Otherwise it resolves against the three raw per-level maps directly.
/// Precedence is log -> scope -> resource, first map containing a matching
/// key wins. `coalesce` gives exactly that: a key present with an
/// empty-string value is non-NULL and stops the chain, which is the
/// intended reading of "first hit wins".
///
/// The scope split is not addressable from `LogQL` — Loki label names admit no
/// separator that could carry it — so precedence is the only disambiguator.
fn attribute_lookup(label: &str, schema: &DFSchema) -> Expr {
    use datafusion::functions::core::expr_fn::coalesce;

    let udf = ScalarUDF::from(crate::logql::datafusion::udf::MapGetByNormalizedKey::new());

    if schema_has_merged_attributes(schema) {
        udf.call(vec![col(MERGED_ATTRIBUTES_COLUMN), lit(label)])
    } else {
        coalesce(vec![
            udf.call(vec![col(COL_LOG_ATTRIBUTES), lit(label)]),
            udf.call(vec![col(COL_SCOPE_ATTRIBUTES), lit(label)]),
            udf.call(vec![col(COL_RESOURCE_ATTRIBUTES), lit(label)]),
        ])
    }
}

/// The attribute maps of the `DataFrame` whose schema is `schema`, as the
/// single flat map the Loki wire format exposes. Series identity and
/// grouping are defined over wire names, so they operate on this rather than
/// on the per-level columns.
///
/// Schema-aware for the same reason, and by the same [`schema_has_merged_attributes`]
/// check, as [`attribute_lookup`]: when an earlier stage in the same
/// pipeline already produced [`MERGED_ATTRIBUTES_COLUMN`], the raw per-level
/// columns this function would otherwise merge no longer exist on that
/// `DataFrame`, so this returns the existing column directly instead of
/// re-running the merge UDF against columns that are no longer there.
fn merged_attributes(schema: &DFSchema) -> Expr {
    if schema_has_merged_attributes(schema) {
        col(MERGED_ATTRIBUTES_COLUMN)
    } else {
        ScalarUDF::from(crate::logql::datafusion::udf::MapMergeNormalized::new()).call(vec![
            col(COL_RESOURCE_ATTRIBUTES),
            col(COL_SCOPE_ATTRIBUTES),
            col(COL_LOG_ATTRIBUTES),
        ])
    }
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

    #[tracing::instrument(skip_all)]
    async fn plan(&self, expr: LogQLExpr) -> Result<Self::Plan> {
        let df = match expr {
            LogQLExpr::Log(log_expr) => {
                let df = self.plan_log(log_expr, self.query_ctx.start, self.query_ctx.end).await?;

                // Sort by timestamp (ascending for forward, descending for backward)
                // Must apply sort BEFORE limit to get correct N oldest/newest entries
                let ascending = self.query_ctx.direction == SortDirection::Forward;
                let df = df.sort(vec![col(COL_TIMESTAMP).sort(ascending, true)])?;

                // Apply limit only for log queries (not metrics)
                // Uses context limit or Loki default of 100 entries
                let limit = self.query_ctx.limit.unwrap_or(DEFAULT_LOG_LIMIT);
                df.limit(0, Some(limit))?
            }
            LogQLExpr::Metric(metric_expr) => self.plan_metric(metric_expr).await?,
        };
        Ok(df)
    }
}

impl DataFusionPlanner {
    #[tracing::instrument(skip(self, expr))]
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
        let df = df.filter(col(COL_TENANT_ID).eq(lit(&self.query_ctx.tenant_id)))?;

        // 3. Apply time range filter
        // The timestamp column is Timestamp(Microsecond)
        // Convert DateTime<Utc> to microseconds for comparison
        let ts_col = col(COL_TIMESTAMP);
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

    /// Plan a query for distinct series (unique label combinations).
    ///
    /// Returns a `DataFrame` with indexed columns and serialized attribute
    /// strings for all distinct series matching any of the provided selectors.
    ///
    /// Optimized approach (avoids reading `body` and carrying MAP through
    /// aggregation):
    /// 1. Scan only indexed columns + attributes MAP (skip body, timestamps)
    /// 2. Serialize MAP keys/values to `"|||"`-delimited strings
    /// 3. SELECT DISTINCT on indexed columns + serialized attributes
    /// 4. Formatter reconstructs label maps from the serialized strings
    ///
    /// Safety limit: caps output at [`MAX_SERIES_RESULTS`] to prevent OOM on
    /// high-cardinality queries.
    #[tracing::instrument(skip(self, selectors))]
    #[allow(clippy::items_after_statements)]
    pub async fn plan_series(&self, selectors: &[Selector]) -> Result<DataFrame> {
        use datafusion::functions_nested::{map_keys::map_keys, string::array_to_string};

        /// Maximum number of distinct series returned to prevent unbounded
        /// memory growth on high-cardinality queries.
        const MAX_SERIES_RESULTS: usize = 10_000;

        /// High-cardinality attribute MAP keys excluded from series grouping.
        const SERIES_EXCLUDED_MAP_KEYS: &[&str] = &[COL_TRACE_ID, COL_SPAN_ID];

        if selectors.is_empty() {
            return Err(QueryError::Plan("At least one selector is required".to_string()));
        }

        // Plan each selector with a targeted scan that reads only the columns
        // needed for series identification (skips body, timestamps, etc.)
        let mut dataframes: Vec<DataFrame> = Vec::with_capacity(selectors.len());
        for selector in selectors {
            let df = self.plan_series_scan(selector.clone()).await?;
            dataframes.push(df);
        }

        // UNION all DataFrames (DISTINCT below will deduplicate)
        let mut df = dataframes.remove(0);
        for other_df in dataframes {
            df = df.union(other_df)?;
        }

        // Materialize the merge once per row. Left as two separate
        // merged_attributes() calls (one for key extraction, one inside
        // first_value's aggregate argument below), each would re-run the UDF
        // — BTreeMap inserts plus a String allocation per attribute — over
        // every scanned row before GROUP BY collapses duplicates. /series is
        // hit constantly by Grafana's label browser, so that redundancy is
        // on a hot path.
        let merged_expr = merged_attributes(df.schema());
        df = df.with_column(COL_MERGED_ATTRS, merged_expr)?;

        // Project series-visible indexed columns + serialized attributes as flat
        // strings. High-cardinality columns (trace_id, span_id) are excluded via
        // LOG_SERIES_LABEL_COLUMNS.
        let mut select_cols: Vec<Expr> = LOG_SERIES_LABEL_COLUMNS.iter().map(|&c| col(c)).collect();
        // Add `level` as alias of `severity_text` for Grafana compatibility
        select_cols.push(col(COL_SEVERITY_TEXT).alias(LEVEL_ALIAS));

        // Strip high-cardinality keys (trace_id, span_id) from the merged
        // attributes MAP before serialization. Without this, per-request
        // unique values inside the MAP would make every row appear as a
        // distinct series.
        //
        // DataFusion lacks map_filter/map_remove, so we use array_except on
        // the keys array and serialize only the filtered keys for grouping.
        use datafusion::functions_nested::except::array_except;
        let excluded_arr = make_array(SERIES_EXCLUDED_MAP_KEYS.iter().map(|&k| lit(k)).collect());
        let clean_keys = array_except(map_keys(col(COL_MERGED_ATTRS)), excluded_arr);

        // Serialize filtered keys for grouping. Values for non-excluded keys
        // are consistent within a series, so grouping by keys alone is
        // sufficient for deduplication.
        select_cols.push(array_to_string(clean_keys, lit("|||")).alias(COL_ATTR_KEYS));
        // Carry the materialized merge through so first_value below can pick
        // one representative row's attributes per group for the formatter,
        // without recomputing it.
        select_cols.push(col(COL_MERGED_ATTRS));

        let df = df.select(select_cols)?;

        // GROUP BY indexed columns + filtered attr keys. Use first_value to
        // keep one representative attributes MAP per group for the formatter.
        // COL_MERGED_ATTRS is not part of group_cols, so — like every other
        // column this aggregate doesn't name — it is dropped from the output
        // schema; only the alias below survives, under the public name.
        use datafusion::functions_aggregate::first_last::first_value;
        let mut group_cols: Vec<Expr> = LOG_SERIES_LABEL_COLUMNS.iter().map(|c| col(*c)).collect();
        group_cols.push(col(LEVEL_ALIAS));
        group_cols.push(col(COL_ATTR_KEYS));

        let df = df.aggregate(
            group_cols,
            vec![first_value(col(COL_MERGED_ATTRS), vec![]).alias(MERGED_ATTRIBUTES_COLUMN)],
        )?;

        // Safety cap to prevent OOM on high-cardinality results
        let df = df.limit(0, Some(MAX_SERIES_RESULTS))?;

        Ok(df)
    }

    /// Scan only the columns needed for series identification.
    ///
    /// Unlike [`plan_log`] which reads all columns (including `body`), this
    /// method projects only indexed attribute columns and the attributes MAP.
    /// This allows DataFusion/Iceberg to skip reading large columns from
    /// Parquet, significantly reducing memory usage.
    async fn plan_series_scan(&self, selector: Selector) -> Result<DataFrame> {
        let df = self.session_ctx.table(LOGS_TABLE_FQN).await?;
        let df = strip_schema_metadata(df)?;

        // Mandatory tenant isolation filter (partition pruning)
        let df = df.filter(col(COL_TENANT_ID).eq(lit(&self.query_ctx.tenant_id)))?;

        // Time range filter for partition pruning
        let ts_col = col(COL_TIMESTAMP);
        let start_micros = self.query_ctx.start.timestamp_micros();
        let end_micros = self.query_ctx.end.timestamp_micros();
        let start_literal = lit(ScalarValue::TimestampMicrosecond(Some(start_micros), None));
        let end_literal = lit(ScalarValue::TimestampMicrosecond(Some(end_micros), None));
        let df = df.filter(ts_col.clone().gt_eq(start_literal).and(ts_col.lt_eq(end_literal)))?;

        // Apply selector label matchers (operates on indexed columns + attributes)
        let df = Self::apply_selector(df, selector)?;

        // Project ONLY the columns needed for series identification.
        // This is the key optimization: body, observed_timestamp,
        // ingested_timestamp, and high-cardinality columns are never carried
        // through the UNION/DISTINCT pipeline.
        // Note: trace_id/span_id may still be read for selector filters above,
        // but DataFusion's optimizer handles that — they are dropped from output.
        let mut series_cols: Vec<Expr> = LOG_SERIES_LABEL_COLUMNS.iter().map(|&c| col(c)).collect();
        series_cols.push(col(COL_RESOURCE_ATTRIBUTES));
        series_cols.push(col(COL_SCOPE_ATTRIBUTES));
        series_cols.push(col(COL_LOG_ATTRIBUTES));

        Ok(df.select(series_cols)?)
    }

    // Note: #[tracing::instrument] cannot be applied to methods returning Pin<Box<dyn Future>>
    // directly. The outer plan() span covers this call.
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

    #[tracing::instrument(skip(self, agg))]
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

    /// Extract grid parameters as raw microsecond values for `GridAgg` UDAF construction.
    ///
    /// Returns `(start_micros, end_micros, step_micros, range_micros, offset_micros)`.
    ///
    /// # Errors
    /// Returns `QueryError::Config` if step/range/offset duration exceeds i64 limits
    fn extract_grid_params(
        query_ctx: &QueryContext,
        range: TimeDelta,
        offset: Option<TimeDelta>,
    ) -> Result<(i64, i64, i64, i64, i64)> {
        let start_micros = query_ctx.start.timestamp_micros();
        let end_micros = query_ctx.end.timestamp_micros();

        let step_micros = query_ctx
            .step
            .ok_or(QueryError::Config(
                "Step parameter is required for range aggregation".to_string(),
            ))?
            .num_microseconds()
            .ok_or(QueryError::Config("Step duration too large".to_string()))?;

        let range_micros = range
            .num_microseconds()
            .ok_or(QueryError::Config("Range duration too large".to_string()))?;

        let offset_duration = offset.unwrap_or(TimeDelta::zero());
        let offset_micros = offset_duration
            .num_microseconds()
            .ok_or(QueryError::Config("Offset duration too large".to_string()))?;

        Ok((start_micros, end_micros, step_micros, range_micros, offset_micros))
    }

    /// Build grouping expressions for range aggregation.
    ///
    /// MAP columns cannot be used directly in GROUP BY, so we serialize keys/values
    /// to strings for grouping, then preserve the original MAP using `last_value`.
    ///
    /// `attrs` is the attributes-map expression to serialize, and it is used
    /// TWICE (once for keys, once for values). Callers MUST therefore pass a
    /// column reference to an already-materialized merge — see
    /// [`Self::stage_merged_attributes`] — rather than a bare
    /// [`merged_attributes`] call: inlining the UDF here would re-run it per
    /// row per use, which is the cost `plan_series` documents avoiding.
    fn build_label_grouping_exprs(attrs: &Expr, include_timestamp: bool) -> Vec<Expr> {
        use datafusion::functions_nested::{map_keys::map_keys, map_values::map_values, string::array_to_string};

        let mut grouping_exprs = Vec::new();
        if include_timestamp {
            grouping_exprs.push(col("grid_timestamp"));
        }
        grouping_exprs.extend(LOG_INDEXED_ATTRIBUTE_COLUMNS.iter().map(|c| col(*c)));
        grouping_exprs.push(array_to_string(map_keys(attrs.clone()), lit("|||")).alias(COL_ATTR_KEYS));
        grouping_exprs.push(array_to_string(map_values(attrs.clone()), lit("|||")).alias(COL_ATTR_VALS));
        grouping_exprs
    }

    /// Materialize [`merged_attributes`] into [`MERGED_ATTRIBUTES_COLUMN`] and
    /// return the reference to it.
    ///
    /// An aggregation reads the merged map three times — serialized keys,
    /// serialized values, and the preserved map itself — and every one of
    /// those is a separate `map_merge_normalized` invocation per scanned row
    /// unless the merge is staged into a column first. `plan_series` already
    /// stages it for exactly this reason; this is that step, shared.
    ///
    /// Schema-aware through [`merged_attributes`], so calling it on a
    /// `DataFrame` whose pipeline already merged (a `drop`/`keep` stage, or an
    /// inner aggregation) re-uses that column instead of re-reading per-level
    /// columns that no longer exist.
    fn stage_merged_attributes(df: DataFrame) -> Result<(DataFrame, Expr)> {
        let merged_expr = merged_attributes(df.schema());
        let df = df.with_column(MERGED_ATTRIBUTES_COLUMN, merged_expr)?;
        Ok((df, col(MERGED_ATTRIBUTES_COLUMN)))
    }

    /// Build grouping expressions and filtered attributes expression.
    ///
    /// Consolidates grouping logic to avoid duplicate computation of indexed columns and attribute labels.
    ///
    /// `attrs` is the attributes-map expression to filter. Callers on a
    /// freshly-scanned `DataFrame` (the three per-level columns still
    /// separate) pass [`merged_attributes`]; callers where a single
    /// `attributes` column has already been materialized earlier in the same
    /// pipeline (e.g. the outer grouping of a vector aggregation, applied to
    /// the inner range aggregation's already-merged output) pass
    /// `col(MERGED_ATTRIBUTES_COLUMN)` instead — recomputing the merge there would both
    /// re-read columns that no longer exist on that `DataFrame` and discard
    /// any filtering the inner stage already applied.
    ///
    /// Returns:
    /// - Vec<Expr>: Grouping expressions (timestamp, indexed columns, serialized MAP keys/values)
    /// - Expr: Filtered attributes MAP expression for replacing the attributes column
    ///
    /// Filters which labels to include/exclude based on the grouping specification:
    /// - `by (label1, label2)`: Groups only by specified labels
    /// - `without (label1, label2)`: Groups by all labels except specified ones
    ///
    /// Since MAP columns cannot be used in GROUP BY, we:
    /// 1. Filter indexed columns based on the grouping clause
    /// 2. Apply `map_keep_keys` or `map_drop_keys` UDF to filter the attributes MAP
    /// 3. Serialize the filtered MAP keys/values for grouping
    fn build_grouping_with_filtered_attrs(
        attrs: &Expr,
        grouping: &crate::logql::common::Grouping,
        include_timestamp: bool,
    ) -> (Vec<Expr>, Expr) {
        use datafusion::functions_nested::{map_keys::map_keys, map_values::map_values, string::array_to_string};

        use crate::logql::common::Grouping;

        let mut grouping_exprs = Vec::new();
        if include_timestamp {
            grouping_exprs.push(col("grid_timestamp"));
        }

        match grouping {
            Grouping::By(labels) => {
                // Include ONLY specified labels (deduplicate mapped names)
                let mut indexed_cols = Vec::new();
                let mut attr_labels = Vec::new();

                for label in labels {
                    let mapped_name = Self::map_label_to_internal_name(&label.name);
                    if Self::is_top_level_field(mapped_name) {
                        // Indexed column - add to grouping (deduplicate since multiple
                        // labels can map to the same column, e.g. "level" and "detected_level"
                        // both map to "severity_text")
                        if !indexed_cols.contains(&mapped_name) {
                            indexed_cols.push(mapped_name);
                        }
                    } else {
                        // Attribute from MAP - collect for filtering
                        attr_labels.push(label.name.as_str());
                    }
                }

                // Add indexed columns to grouping
                grouping_exprs.extend(indexed_cols.iter().map(|c| col(*c)));

                // Filter attributes MAP to keep only specified labels
                // Always use MapKeepKeys UDF to ensure consistent MAP schema (keys and values as strings)
                // When attr_labels is empty, use a typed empty array to filter out all attributes
                let udf = ScalarUDF::from(super::udf::MapKeepKeys::new());
                let label_array = if attr_labels.is_empty() {
                    // Create typed empty array using arrow_cast to avoid type inference issues
                    // This ensures MapKeepKeys receives a List(Utf8) argument
                    datafusion::functions::core::arrow_cast().call(vec![make_array(vec![]), lit("List(Utf8)")])
                } else {
                    make_array(attr_labels.iter().map(|l| lit(*l)).collect())
                };

                // For grouping (by/without), we only filter by key names (no matchers)
                // Pass NULL arrays for values and ops (simple name-based filtering)
                let null_values = if attr_labels.is_empty() {
                    datafusion::functions::core::arrow_cast().call(vec![make_array(vec![]), lit("List(Utf8)")])
                } else {
                    make_array(vec![lit(ScalarValue::Utf8(None)); attr_labels.len()])
                };
                let null_ops = if attr_labels.is_empty() {
                    datafusion::functions::core::arrow_cast().call(vec![make_array(vec![]), lit("List(Utf8)")])
                } else {
                    make_array(vec![lit(ScalarValue::Utf8(None)); attr_labels.len()])
                };

                let filtered_attrs = udf.call(vec![attrs.clone(), label_array, null_values, null_ops]);

                if attr_labels.is_empty() {
                    // No attributes to group by - use empty MAP serialization
                    grouping_exprs.push(lit("").alias(COL_ATTR_KEYS));
                    grouping_exprs.push(lit("").alias(COL_ATTR_VALS));
                } else {
                    // Serialize filtered MAP for grouping
                    grouping_exprs
                        .push(array_to_string(map_keys(filtered_attrs.clone()), lit("|||")).alias(COL_ATTR_KEYS));
                    grouping_exprs
                        .push(array_to_string(map_values(filtered_attrs.clone()), lit("|||")).alias(COL_ATTR_VALS));
                }

                (grouping_exprs, filtered_attrs)
            }
            Grouping::Without(labels) => {
                // Include ALL labels EXCEPT specified ones
                let mut excluded_indexed_cols = Vec::new();
                let mut excluded_attr_labels = Vec::new();

                for label in labels {
                    let mapped_name = Self::map_label_to_internal_name(&label.name);
                    if Self::is_top_level_field(mapped_name) {
                        excluded_indexed_cols.push(mapped_name);
                    } else {
                        excluded_attr_labels.push(label.name.as_str());
                    }
                }

                // Add indexed columns NOT in exclusion list
                for &col_name in LOG_INDEXED_ATTRIBUTE_COLUMNS {
                    if !excluded_indexed_cols.contains(&col_name) {
                        grouping_exprs.push(col(col_name));
                    }
                }

                // Filter attributes MAP to drop specified labels
                let filtered_attrs = if excluded_attr_labels.is_empty() {
                    // No attributes to exclude - keep full MAP
                    attrs.clone()
                } else {
                    let udf = ScalarUDF::from(super::udf::MapDropKeys::new());
                    let label_array = make_array(excluded_attr_labels.iter().map(|l| lit(*l)).collect());

                    // For grouping (by/without), we only filter by key names (no matchers)
                    // Pass NULL arrays for values and ops (simple name-based filtering)
                    let null_values = make_array(vec![lit(ScalarValue::Utf8(None)); excluded_attr_labels.len()]);
                    let null_ops = make_array(vec![lit(ScalarValue::Utf8(None)); excluded_attr_labels.len()]);

                    udf.call(vec![attrs.clone(), label_array, null_values, null_ops])
                };

                if excluded_attr_labels.is_empty() {
                    // No attributes to exclude - use full MAP serialization
                    grouping_exprs.push(array_to_string(map_keys(attrs.clone()), lit("|||")).alias(COL_ATTR_KEYS));
                    grouping_exprs.push(array_to_string(map_values(attrs.clone()), lit("|||")).alias(COL_ATTR_VALS));
                } else {
                    // Serialize filtered MAP for grouping
                    grouping_exprs
                        .push(array_to_string(map_keys(filtered_attrs.clone()), lit("|||")).alias(COL_ATTR_KEYS));
                    grouping_exprs
                        .push(array_to_string(map_values(filtered_attrs.clone()), lit("|||")).alias(COL_ATTR_VALS));
                }

                (grouping_exprs, filtered_attrs)
            }
        }
    }

    /// Create aggregation expression to preserve the attributes MAP column.
    ///
    /// Uses `last_value()` to preserve one representative attributes MAP.
    /// `attrs` is [`merged_attributes`] on a freshly-scanned `DataFrame`, or
    /// `col(MERGED_ATTRIBUTES_COLUMN)` when an earlier step in the same pipeline (e.g.
    /// `by`/`without` filtering) already materialized the merged column —
    /// see [`Self::build_grouping_with_filtered_attrs`] for the same split.
    fn preserve_attributes_column(attrs: Expr) -> Expr {
        last_value(attrs, vec![]).alias(MERGED_ATTRIBUTES_COLUMN)
    }

    /// Plans unwrap-based range aggregations.
    ///
    /// Plans an unwrap-based range aggregation using `GridAgg` UDAF.
    ///
    /// Instead of materializing `N_logs` x `M_grid_points` intermediate rows via unnest,
    /// the `GridAgg` UDAF accumulates values directly into grid buckets. The unnest
    /// happens post-aggregation where data is already reduced.
    ///
    /// Key simplification: `rate_counter` no longer needs LAG window functions; the
    /// `RateCounterGridAccumulator` handles counter reset detection internally.
    ///
    /// Supports all unwrap-based ops: `sum_over_time`, `avg_over_time`, `min_over_time`,
    /// `max_over_time`, `stddev_over_time`, `stdvar_over_time`, `first_over_time`,
    /// `last_over_time`, `quantile_over_time`, `rate_counter`.
    async fn plan_unwrap_range_aggregation(&self, agg: crate::logql::metric::RangeAggregation) -> Result<DataFrame> {
        use datafusion::logical_expr::AggregateUDF;

        use super::udaf::{GridAgg, GridAggOp, grid_agg::OrderedFloat};

        // 1. Extract unwrap expression
        let unwrap = agg
            .range_expr
            .unwrap
            .as_ref()
            .ok_or_else(|| QueryError::Plan("Unwrap expression required for this aggregation".to_string()))?;

        // 1.5. Validate grouping support for unwrap-based range aggregations
        // Per Loki spec: sum_over_time and rate_counter don't support grouping with unwrap expressions
        if agg.grouping.is_some() {
            match agg.op {
                RangeAggregationOp::SumOverTime | RangeAggregationOp::RateCounter => {
                    return Err(QueryError::Plan(format!(
                        "{} does not support by/without grouping clauses",
                        agg.op.as_str()
                    )));
                }
                _ => {} // Other operations support grouping
            }
        }

        // 2. Calculate adjusted time range for lookback window
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

        // 6. Compute grid parameters (NULL unwrapped_value rows are skipped by GridAgg accumulators)
        let (start_micros, end_micros, step_micros, range_micros, offset_micros) =
            Self::extract_grid_params(&self.query_ctx, agg.range_expr.range, agg.range_expr.offset)?;

        // 7. Map RangeAggregationOp to GridAggOp
        let grid_agg_op = match agg.op {
            RangeAggregationOp::SumOverTime => GridAggOp::Sum,
            RangeAggregationOp::AvgOverTime => GridAggOp::Avg,
            RangeAggregationOp::MinOverTime => GridAggOp::Min,
            RangeAggregationOp::MaxOverTime => GridAggOp::Max,
            RangeAggregationOp::StddevOverTime => GridAggOp::Stddev,
            RangeAggregationOp::StdvarOverTime => GridAggOp::Stdvar,
            RangeAggregationOp::FirstOverTime => GridAggOp::First,
            RangeAggregationOp::LastOverTime => GridAggOp::Last,
            RangeAggregationOp::QuantileOverTime => {
                let phi = agg
                    .param
                    .ok_or_else(|| QueryError::Plan("quantile_over_time requires a parameter (0.0-1.0)".to_string()))?;
                if !(0.0..=1.0).contains(&phi) {
                    return Err(QueryError::Plan(format!(
                        "quantile_over_time parameter must be between 0.0 and 1.0, got: {phi}"
                    )));
                }
                GridAggOp::Quantile(OrderedFloat(phi))
            }
            RangeAggregationOp::RateCounter => GridAggOp::RateCounter,
            _ => {
                return Err(QueryError::Plan(format!(
                    "{:?} does not support unwrap expressions",
                    agg.op
                )));
            }
        };

        // 8. Create GridAgg UDAF
        let grid_agg = GridAgg::new(
            grid_agg_op,
            start_micros,
            end_micros,
            step_micros,
            range_micros,
            offset_micros,
            self.query_ctx.max_grid_points,
        )?;
        let grid_points = grid_agg.grid_points().to_vec();
        let grid_agg_udaf = AggregateUDF::from(grid_agg);

        // 9. Build label grouping with pushdown support (no grid_timestamp — UDAF handles it)
        //
        // The `by`/`without` branch materializes a real `attributes` column via
        // with_column below, so the aggregate must read it back through
        // col(MERGED_ATTRIBUTES_COLUMN) rather than recomputing the merge — recomputing
        // would both re-read the (still-present but now stale for this
        // purpose) raw columns and silently discard the filtering just applied.
        //
        // Staging happens before the branch, so `merged_attributes` sees the
        // pre-merge schema: an inner pipeline stage (e.g. `drop`/`keep`) may
        // have already collapsed the three per-level columns into
        // `attributes`, and it must resolve against that rather than
        // referencing columns that no longer exist (the C1 bug).
        let (mut df, merged_col) = Self::stage_merged_attributes(df)?;
        let (grouping_exprs, attrs_for_preserve) = if let Some(ref grouping) = agg.grouping {
            let (grouping_exprs, filtered_attrs_expr) =
                Self::build_grouping_with_filtered_attrs(&merged_col, grouping, false);
            df = df.with_column(MERGED_ATTRIBUTES_COLUMN, filtered_attrs_expr)?;
            (grouping_exprs, col(MERGED_ATTRIBUTES_COLUMN))
        } else {
            // false = no grid_timestamp (UDAF handles it)
            (Self::build_label_grouping_exprs(&merged_col, false), merged_col)
        };

        // 10. Aggregate: GROUP BY labels only, UDAF accumulates into grid buckets
        df = df.aggregate(
            grouping_exprs,
            vec![
                grid_agg_udaf
                    .call(vec![col(COL_TIMESTAMP), col("unwrapped_value")])
                    .alias("_grid_values"),
                Self::preserve_attributes_column(attrs_for_preserve),
                // Track if ANY sample had conversion error
                bool_or(col("_has_unwrap_error")).alias("_group_has_error"),
            ],
        )?;

        // 11. Add __error__ label for groups with conversion errors
        let map_insert_udf = ScalarUDF::from(super::udf::MapInsert::new());
        df = df.with_column(
            MERGED_ATTRIBUTES_COLUMN,
            when(
                col("_group_has_error"),
                map_insert_udf.call(vec![col(MERGED_ATTRIBUTES_COLUMN), lit("__error__"), lit("true")]),
            )
            .otherwise(col(MERGED_ATTRIBUTES_COLUMN))?,
        )?;

        // 12. Add literal _grid_timestamps column, unnest both, filter NULLs
        let ts_type = DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None);
        let grid_ts_list = ScalarValue::List(ScalarValue::new_list_from_iter(
            grid_points.iter().map(|&ts| ScalarValue::TimestampMicrosecond(Some(ts), None)),
            &ts_type,
            true, // nullable
        ));
        df = df.with_column("_grid_timestamps", lit(grid_ts_list))?;
        df = df.unnest_columns(&["_grid_timestamps", "_grid_values"])?;
        df = df.filter(col("_grid_values").is_not_null())?;

        // 13. Apply rate division for RateCounter
        let range_nanos = agg
            .range_expr
            .range
            .num_nanoseconds()
            .ok_or(QueryError::Config("Range duration too large".to_string()))?;
        if range_nanos <= 0 {
            return Err(QueryError::Config("Range duration must be positive".to_string()));
        }
        #[allow(clippy::cast_precision_loss)]
        let range_secs = range_nanos as f64 / 1_000_000_000.0;

        let value_expr = match agg.op {
            RangeAggregationOp::RateCounter => (col("_grid_values") / lit(range_secs)).alias("value"),
            _ => col("_grid_values").alias("value"),
        };

        // 14. Select output columns (respect grouping to avoid referencing missing columns)
        let mut select_exprs = vec![col("_grid_timestamps").alias("timestamp"), value_expr];
        select_exprs.extend(Self::build_grouped_label_exprs(agg.grouping.as_ref()));

        df = df.select(select_exprs)?;

        // 16. Return all results (including those with __error__ label)
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
        let date_grid_udf = ScalarUDF::from(super::udf::DateGrid::with_max_grid_points(
            self.query_ctx.max_grid_points,
        ));
        let date_grid_args = vec![
            col(COL_TIMESTAMP),
            start_arg,
            end_arg,
            step_arg,
            range_arg,
            offset_arg,
            lit(true), // inverse=true - returns grid points NOT covered
        ];
        df = df.with_column("inverse_grid_timestamps", date_grid_udf.call(date_grid_args))?;

        // 6. Build label grouping expressions (same pattern as other range
        // aggregations). Staging is schema-aware, so an inner `drop`/`keep`
        // stage that already merged is re-used rather than re-merged.
        let (mut df, merged_col) = Self::stage_merged_attributes(df)?;
        let grouping_exprs = Self::build_label_grouping_exprs(&merged_col, false); // exclude timestamp for absent

        // 7. Aggregate using array_intersect_agg UDAF
        // This finds grid points present in ALL inverse arrays (= absent points)
        let array_intersect_udaf = AggregateUDF::from(super::udaf::ArrayIntersectAgg::new());
        let attrs_for_preserve = merged_col;
        df = df.aggregate(
            grouping_exprs,
            vec![
                array_intersect_udaf
                    .call(vec![col("inverse_grid_timestamps")])
                    .alias("absent_timestamps"),
                Self::preserve_attributes_column(attrs_for_preserve),
            ],
        )?;

        // 8. Unnest absent_timestamps to get one row per absent grid point
        df = df.unnest_columns(&["absent_timestamps"])?;
        df = df.with_column("timestamp", col("absent_timestamps"))?;

        // 9. Add value=1.0 for all absent points
        df = df.with_column("value", lit(1.0))?;

        // 10. Select final output columns (match other range aggregations)
        let mut select_exprs = vec![col(COL_TIMESTAMP), col("value")];
        select_exprs.extend(LOG_INDEXED_ATTRIBUTE_COLUMNS.iter().map(|c| col(*c)));
        select_exprs.push(col(MERGED_ATTRIBUTES_COLUMN));

        Ok(df.select(select_exprs)?)
    }

    /// Plans a log range aggregation using `GridAgg` UDAF for grid-bucketed accumulation.
    ///
    /// Instead of materializing `N_logs` x `M_grid_points` intermediate rows via unnest,
    /// the `GridAgg` UDAF accumulates values directly into grid buckets. The unnest
    /// happens post-aggregation where data is already reduced.
    ///
    /// Supports: `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`.
    /// Note: `absent_over_time` is handled separately in `plan_log_range_absent_aggregation`.
    async fn plan_log_range_aggregation(&self, agg: crate::logql::metric::RangeAggregation) -> Result<DataFrame> {
        use datafusion::logical_expr::AggregateUDF;

        use super::udaf::{GridAgg, GridAggOp};

        // 1. Plan the inner LogExpr with extended time range for lookback window
        let (adjusted_start, adjusted_end) = Self::adjust_time_range_for_lookback(
            self.query_ctx.start,
            self.query_ctx.end,
            agg.range_expr.range,
            agg.range_expr.offset,
        );

        let mut df = self.plan_log(agg.range_expr.log_expr, adjusted_start, adjusted_end).await?;

        // 2. Compute grid parameters
        let (start_micros, end_micros, step_micros, range_micros, offset_micros) =
            Self::extract_grid_params(&self.query_ctx, agg.range_expr.range, agg.range_expr.offset)?;

        // 3. For bytes ops, add body length as Float64 column
        let is_bytes_op = matches!(
            agg.op,
            RangeAggregationOp::BytesOverTime | RangeAggregationOp::BytesRate
        );
        if is_bytes_op {
            let schema = df.schema().clone();
            df = df.with_column(
                "_body_bytes",
                octet_length().call(vec![col(COL_BODY)]).cast_to(&DataType::Float64, &schema)?,
            )?;
        }

        // 4. Create GridAgg UDAF with appropriate operation
        let grid_agg_op = match agg.op {
            RangeAggregationOp::CountOverTime | RangeAggregationOp::Rate => GridAggOp::Count,
            RangeAggregationOp::BytesOverTime | RangeAggregationOp::BytesRate => GridAggOp::Sum,
            _ => {
                return Err(QueryError::Plan(
                    "This range aggregation requires an unwrap expression".to_string(),
                ));
            }
        };

        let grid_agg = GridAgg::new(
            grid_agg_op,
            start_micros,
            end_micros,
            step_micros,
            range_micros,
            offset_micros,
            self.query_ctx.max_grid_points,
        )?;
        let grid_points = grid_agg.grid_points().to_vec();
        let grid_agg_udaf = AggregateUDF::from(grid_agg);

        // 5. Build UDAF call arguments
        let udaf_args = if is_bytes_op {
            vec![col(COL_TIMESTAMP), col("_body_bytes")]
        } else {
            vec![col(COL_TIMESTAMP)]
        };

        // 6. Build label grouping with pushdown support
        //
        // See the identical split in plan_unwrap_range_aggregation: the merge
        // is staged into a column first so the three reads below cost one
        // evaluation per row, and the by/without branch then replaces that
        // column with its filtered form.
        let (mut df, merged_col) = Self::stage_merged_attributes(df)?;
        let (grouping_exprs, attrs_for_preserve) = if let Some(ref grouping) = agg.grouping {
            let (grouping_exprs, filtered_attrs_expr) =
                Self::build_grouping_with_filtered_attrs(&merged_col, grouping, false);
            df = df.with_column(MERGED_ATTRIBUTES_COLUMN, filtered_attrs_expr)?;
            (grouping_exprs, col(MERGED_ATTRIBUTES_COLUMN))
        } else {
            // false = no grid_timestamp (UDAF handles it)
            (Self::build_label_grouping_exprs(&merged_col, false), merged_col)
        };

        // 7. Aggregate: GROUP BY labels only, UDAF accumulates into grid buckets
        df = df.aggregate(
            grouping_exprs,
            vec![
                grid_agg_udaf.call(udaf_args).alias("_grid_values"),
                Self::preserve_attributes_column(attrs_for_preserve),
            ],
        )?;

        // 8. Add literal _grid_timestamps column (broadcast grid points to every row)
        let ts_type = DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None);
        let grid_ts_list = ScalarValue::List(ScalarValue::new_list_from_iter(
            grid_points.iter().map(|&ts| ScalarValue::TimestampMicrosecond(Some(ts), None)),
            &ts_type,
            true, // nullable
        ));
        df = df.with_column("_grid_timestamps", lit(grid_ts_list))?;

        // 9. Unnest both grid timestamps and grid values in parallel (post-aggregation)
        df = df.unnest_columns(&["_grid_timestamps", "_grid_values"])?;

        // 10. Filter NULL values (empty buckets)
        df = df.filter(col("_grid_values").is_not_null())?;

        // 11. Apply rate division if Rate/BytesRate
        let range_nanos = agg
            .range_expr
            .range
            .num_nanoseconds()
            .ok_or(QueryError::Config("Range duration too large".to_string()))?;
        if range_nanos <= 0 {
            return Err(QueryError::Config("Range duration must be positive".to_string()));
        }
        #[allow(clippy::cast_precision_loss)]
        let range_secs = range_nanos as f64 / 1_000_000_000.0;

        let value_expr = match agg.op {
            RangeAggregationOp::Rate | RangeAggregationOp::BytesRate => {
                (col("_grid_values") / lit(range_secs)).alias("value")
            }
            _ => col("_grid_values").alias("value"),
        };

        // 12. Select output columns (respect grouping to avoid referencing missing columns)
        let mut select_exprs = vec![col("_grid_timestamps").alias("timestamp"), value_expr];
        select_exprs.extend(Self::build_grouped_label_exprs(agg.grouping.as_ref()));

        df = df.select(select_exprs)?;

        Ok(df)
    }

    fn build_default_label_columns(with: &[&str], without: &[&str]) -> Vec<String> {
        LOG_INDEXED_ATTRIBUTE_COLUMNS
            .iter()
            .copied()
            .chain(std::iter::once(MERGED_ATTRIBUTES_COLUMN))
            .chain(with.iter().copied())
            .filter(|c| !without.contains(c))
            .map(ToString::to_string)
            .collect()
    }

    fn build_default_label_exprs(with: &[&str], without: &[&str]) -> Vec<Expr> {
        Self::build_default_label_columns(with, without).into_iter().map(col).collect()
    }

    /// Builds label select expressions respecting the active grouping.
    ///
    /// When grouping is `None`, selects all default label columns.
    /// When grouping is `By(labels)`, selects only the specified label columns.
    /// When grouping is `Without(labels)`, selects all default labels except excluded ones.
    /// Always includes `attributes`.
    fn build_grouped_label_exprs(grouping: Option<&crate::logql::common::Grouping>) -> Vec<Expr> {
        use crate::logql::common::Grouping;

        match grouping {
            None => Self::build_default_label_exprs(&[], &[]),
            Some(Grouping::By(labels)) => {
                let mut exprs = Vec::new();
                let mut seen = Vec::new();
                for label in labels {
                    let mapped = Self::map_label_to_internal_name(&label.name);
                    if Self::is_top_level_field(mapped) && !seen.contains(&mapped) {
                        exprs.push(col(mapped));
                        seen.push(mapped);
                    }
                }
                exprs.push(col(MERGED_ATTRIBUTES_COLUMN));
                exprs
            }
            Some(Grouping::Without(labels)) => {
                let excluded: Vec<&str> = labels
                    .iter()
                    .map(|l| Self::map_label_to_internal_name(&l.name))
                    .filter(|n| Self::is_top_level_field(n))
                    .collect();
                Self::build_default_label_exprs(&[], &excluded)
            }
        }
    }

    /// Pushes outer grouping down to an inner `RangeAggregation` when supported.
    ///
    /// For queries like `sum by (level) (count_over_time({...}[1m]))`, we need the inner
    /// range aggregation to also group by `level`. However, some range aggregations
    /// (`sum_over_time`, `rate_counter`) don't support grouping, so we skip pushdown
    /// and handle grouping at the vector aggregation level instead.
    fn push_grouping_to_inner_expr(
        inner_expr: MetricExpr,
        outer_grouping: Option<&crate::logql::common::Grouping>,
    ) -> MetricExpr {
        match (inner_expr, outer_grouping) {
            (MetricExpr::RangeAggregation(mut range_agg), Some(outer_grouping)) => {
                // Check if this range aggregation supports grouping
                // Per Loki spec: sum_over_time and rate_counter don't support grouping
                let supports_grouping = !matches!(
                    range_agg.op,
                    RangeAggregationOp::SumOverTime | RangeAggregationOp::RateCounter
                );

                if supports_grouping {
                    range_agg.grouping = Some(Self::merge_groupings(range_agg.grouping.take(), outer_grouping));
                }
                // If !supports_grouping, we don't push grouping down - handle at vector agg level
                MetricExpr::RangeAggregation(range_agg)
            }
            (expr, _) => expr,
        }
    }

    /// Merges inner and outer grouping clauses.
    ///
    /// The outer aggregation's grouping determines the final result structure,
    /// so it takes precedence in cross-type merges.
    fn merge_groupings(
        inner_grouping: Option<crate::logql::common::Grouping>,
        outer_grouping: &crate::logql::common::Grouping,
    ) -> crate::logql::common::Grouping {
        use crate::logql::common::Grouping;

        match (inner_grouping, outer_grouping) {
            // No inner grouping - use outer grouping
            (None, _) => outer_grouping.clone(),

            // Same-type merges: combine label sets
            (Some(Grouping::By(mut inner_labels)), Grouping::By(outer_labels)) => {
                // Both By: merge inclusion lists, avoiding duplicates
                // Example: by (a, b) + by (b, c) → by (a, b, c)
                for label in outer_labels {
                    if !inner_labels.iter().any(|l| l.name == label.name) {
                        inner_labels.push(label.clone());
                    }
                }
                Grouping::By(inner_labels)
            }
            (Some(Grouping::Without(mut inner_labels)), Grouping::Without(outer_labels)) => {
                // Both Without: merge exclusion lists, avoiding duplicates
                // Example: without (a) + without (b, c) → without (a, b, c)
                for label in outer_labels {
                    if !inner_labels.iter().any(|l| l.name == label.name) {
                        inner_labels.push(label.clone());
                    }
                }
                Grouping::Without(inner_labels)
            }

            // Cross-type merges: outer grouping takes complete precedence
            // Rationale: The outer aggregation defines the final output structure,
            // and mixing By/Without semantics is ambiguous. We follow the principle
            // that the outer operation's grouping specification is authoritative.
            (Some(Grouping::By(_)), Grouping::Without(outer_labels)) => {
                // Inner By + Outer Without: discard inner, use outer Without entirely
                // Example: sum without (pod) (count by (node)) → group by all labels except pod
                // The outer Without defines which labels to exclude from the final result.
                Grouping::Without(outer_labels.clone())
            }
            (Some(Grouping::Without(_)), Grouping::By(outer_labels)) => {
                // Inner Without + Outer By: discard inner, use outer By entirely
                // Example: sum by (service) (count without (instance)) → group by service only
                // The outer By defines which labels to keep in the final result.
                // This is more restrictive than Without, as By explicitly specifies
                // the exact labels that should remain.
                Grouping::By(outer_labels.clone())
            }
        }
    }

    #[tracing::instrument(skip(self, agg))]
    async fn plan_vector_aggregation(&self, agg: crate::logql::metric::VectorAggregation) -> Result<DataFrame> {
        // Push grouping down to inner RangeAggregation if present
        let inner_expr = Self::push_grouping_to_inner_expr(*agg.expr, agg.grouping.as_ref());

        // Plan the inner MetricExpr (now with merged grouping)
        // Note: We don't clear agg.grouping after pushdown because the vector aggregation
        // still needs to group by the same labels to preserve the series structure
        let mut df = self.plan_metric(inner_expr).await?;

        // Build grouping expressions and filter attributes if grouping is specified
        // Apply by/without filtering if specified, otherwise group by all labels
        // Also track whether we should preserve attributes in the output
        let (group_exprs, preserve_attrs) = if let Some(ref grouping) = agg.grouping {
            // Get both grouping expressions and filtered attributes in one call
            // This avoids duplicate computation of indexed columns and attribute labels
            //
            // `df` here is the INNER metric's already-planned output, so the
            // per-level columns are gone and the merged `attributes` column is
            // what remains — but only when the inner stage kept any labels at
            // all. An ungrouped inner vector aggregation collapses every
            // series and emits just `timestamp` + `value`, so there is nothing
            // to group by; without this check the plan would reference a
            // column that does not exist and fail as an internal error rather
            // than a query one.
            if !schema_has_merged_attributes(df.schema()) {
                return Err(QueryError::Plan(
                    "grouping requires labels, but the inner aggregation collapsed all series and kept none; \
                     add a `by`/`without` clause to the inner aggregation"
                        .to_string(),
                ));
            }
            // Routed through `merged_attributes` rather than naming the column
            // directly so every attribute-reading site resolves the shape the
            // same way (see `schema_has_merged_attributes`).
            let attrs = merged_attributes(df.schema());
            let (mut grouping_exprs, filtered_attrs_expr) =
                Self::build_grouping_with_filtered_attrs(&attrs, grouping, false);
            grouping_exprs.push(col(COL_TIMESTAMP));

            // Replace attributes column with filtered version BEFORE aggregation
            // This ensures last_value preserves the filtered attributes
            df = df.with_column(MERGED_ATTRIBUTES_COLUMN, filtered_attrs_expr)?;

            // Return the grouping expressions and flag to preserve attributes
            (grouping_exprs, true)
        } else {
            // No grouping specified - collapse all series, group only by timestamp
            // This allows aggregations like sum() to combine values across all series
            // Do not reference COL_ATTR_KEYS or COL_ATTR_VALS as they may not exist
            // from the inner range aggregation
            // Also don't preserve attributes since we're collapsing all labels
            (vec![col(COL_TIMESTAMP)], false)
        };

        // Identify aggregation function
        // LogQL: sum, avg, min, max, count, stddev, stdvar, bottomk, topk
        // We need to map agg.op to DataFusion aggregate functions.
        // For now, we'll implement a few common ones.

        // Note: We assume the inner DataFrame produces a "value" column that we
        // aggregate.
        let value_col = col("value");

        let aggr_expr = match agg.op {
            crate::logql::metric::VectorAggregationOp::Sum => sum(value_col),
            crate::logql::metric::VectorAggregationOp::Avg => avg(value_col),
            crate::logql::metric::VectorAggregationOp::Min => min(value_col),
            crate::logql::metric::VectorAggregationOp::Max => max(value_col),
            crate::logql::metric::VectorAggregationOp::Count => count(value_col),
            crate::logql::metric::VectorAggregationOp::Stddev => stddev(value_col),
            crate::logql::metric::VectorAggregationOp::Stdvar => var_sample(value_col),
            // TODO: Implement topk, bottomk, sort, sort_desc
            _ => {
                return Err(QueryError::NotImplemented(format!(
                    "Vector aggregation op {:?} not supported",
                    agg.op
                )));
            }
        }
        .alias("value");

        // Only preserve attributes when grouping is specified
        // When collapsing all series (no grouping), we don't want any labels in output
        let agg_exprs = if preserve_attrs {
            vec![
                aggr_expr,
                Self::preserve_attributes_column(col(MERGED_ATTRIBUTES_COLUMN)),
            ]
        } else {
            vec![aggr_expr]
        };

        Ok(df.aggregate(group_exprs, agg_exprs)?)
    }

    /// Apply selector matchers to filter a `DataFrame`.
    ///
    /// Each matcher is converted to a filter expression and applied
    /// sequentially.
    pub fn apply_selector(df: DataFrame, selector: Selector) -> Result<DataFrame> {
        let mut df = df;
        for matcher in selector.matchers {
            let expr = Self::matcher_to_expr(&matcher, df.schema());
            df = df.filter(expr)?;
        }
        Ok(df)
    }

    /// Convert a `LabelMatcher` to a `DataFusion` filter expression.
    ///
    /// Indexed columns (e.g., `service_name`, `severity_text`) are matched
    /// directly. Every other label resolves through [`attribute_lookup`],
    /// which searches `log_attributes`, then `scope_attributes`, then
    /// `resource_attributes` and matches against the first of the three that
    /// contains the key — a value present at a more specific level always
    /// wins, even an empty-string one, over a less specific level's value —
    /// unless `schema` (the schema of the `DataFrame` this expression will
    /// run against) already carries the merged column, in which case it
    /// looks the key up there instead; see [`attribute_lookup`].
    ///
    /// `trace_id` and `span_id` are stored as raw `FIXED_LEN_BYTE_ARRAY` and
    /// route through [`Self::binary_id_matcher_to_expr`] so all four match
    /// ops behave correctly on the typed column:
    /// - `=` / `!=` on valid hex emits a typed `FixedSizeBinary` literal.
    /// - `=` / `!=` on invalid hex collapses to `lit(false)` / `lit(true)`
    ///   so DataFusion's three-valued logic doesn't drop the entire row set.
    /// - `=~` / `!~` hex-encodes the column before applying `regexp_like`,
    ///   matching the user's natural string view of the identifier.
    pub fn matcher_to_expr(matcher: &LabelMatcher, schema: &DFSchema) -> Expr {
        let mapped_label = Self::map_label_to_internal_name(&matcher.label);
        let col_expr = if Self::is_top_level_field(&matcher.label) {
            col(mapped_label)
        } else {
            attribute_lookup(matcher.label.as_str(), schema)
        };

        if let Some(width) = binary_id_width(mapped_label) {
            return Self::binary_id_matcher_to_expr(col_expr, matcher.op, &matcher.value, width);
        }

        let val = lit(matcher.value.as_str());
        match matcher.op {
            MatchOp::Eq => col_expr.eq(val),
            MatchOp::Neq => col_expr.not_eq(val),
            MatchOp::Re => datafusion::functions::regex::regexp_like().call(vec![col_expr, val]),
            MatchOp::Nre => datafusion::functions::regex::regexp_like().call(vec![col_expr, val]).not(),
        }
    }

    /// Build a matcher expression for a binary identifier column
    /// (`trace_id` / `span_id`).
    ///
    /// Hex is validated up front so equality and inequality can fall back to
    /// constant boolean literals when the user supplies invalid hex —
    /// preserving SQL set-theoretic semantics: `Neq` matches every valid
    /// row, `Eq` matches none. Both raw forms (`col = NULL`, `col != NULL`)
    /// fold to NULL under three-valued logic, which DataFusion treats as
    /// "drop the row" — the opposite of the user's intent for `Neq`.
    ///
    /// Regex matchers run against the hex-encoded column via the `encode`
    /// UDF; `regexp_like` cannot consume `FixedSizeBinary` directly.
    fn binary_id_matcher_to_expr(col_expr: Expr, op: MatchOp, value: &str, width: i32) -> Expr {
        let expected_len = usize::try_from(width).unwrap_or(0);
        let decoded = hex::decode(value).ok().filter(|b| b.len() == expected_len);
        match (op, decoded) {
            (MatchOp::Eq, Some(b)) => col_expr.eq(lit(ScalarValue::FixedSizeBinary(width, Some(b)))),
            (MatchOp::Neq, Some(b)) => col_expr.not_eq(lit(ScalarValue::FixedSizeBinary(width, Some(b)))),
            // Invalid hex: see fn-level doc for why these collapse to constants.
            (MatchOp::Eq, None) => lit(false),
            (MatchOp::Neq, None) => lit(true),
            (MatchOp::Re, _) => {
                let hex_col = datafusion::functions::encoding::encode().call(vec![col_expr, lit("hex")]);
                datafusion::functions::regex::regexp_like().call(vec![hex_col, lit(value)])
            }
            (MatchOp::Nre, _) => {
                let hex_col = datafusion::functions::encoding::encode().call(vec![col_expr, lit("hex")]);
                datafusion::functions::regex::regexp_like()
                    .call(vec![hex_col, lit(value)])
                    .not()
            }
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
            "level" | "detected_level" => COL_SEVERITY_TEXT,
            "service" => COL_SERVICE_NAME,
            _ => name,
        }
    }

    /// Check if a label name corresponds to a top-level indexed column.
    ///
    /// Top-level fields are stored as separate columns in the Iceberg table,
    /// while other labels are stored in the `attributes` MAP column.
    pub fn is_top_level_field(name: &str) -> bool {
        let mapped = Self::map_label_to_internal_name(name);
        LOG_INDEXED_ATTRIBUTE_COLUMNS.contains(&mapped) || matches!(mapped, COL_TENANT_ID | COL_TIMESTAMP)
    }

    /// Extract label value as Float64 with optional conversion.
    ///
    /// Returns NULL if label is missing or conversion fails.
    /// The NULL return allows error tracking via IS NULL checks.
    ///
    /// # Arguments
    /// - `label`: The label name to extract
    /// - `conversion`: Optional conversion function (bytes, duration, etc.)
    /// - `schema`: schema of the `DataFrame` this expression will run
    ///   against — passed through to [`attribute_lookup`] so a `drop`/`keep`
    ///   stage earlier in the same pipeline (which collapses the per-level
    ///   attribute columns into one before `unwrap` ever runs) is resolved
    ///   correctly instead of referencing columns that no longer exist.
    ///
    /// # Returns
    /// `DataFusion` expression that evaluates to Float64 or NULL
    fn extract_unwrapped_value(
        label: &str,
        conversion: Option<crate::logql::log::UnwrapConversion>,
        schema: &DFSchema,
    ) -> Expr {
        use crate::logql::log::UnwrapConversion;

        // 1. Extract label from the attribute maps or indexed column
        let label_expr = if Self::is_top_level_field(label) {
            let internal_name = Self::map_label_to_internal_name(label);
            col(internal_name)
        } else {
            attribute_lookup(label, schema)
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

        let body_col = col(COL_BODY);
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

        let _body_col = col(COL_BODY);

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
        // let expr = udf.call(vec![col(COL_BODY)]);
        // df.select(vec![expr.alias("body"), col(COL_TIMESTAMP), ...])
        // TODO: Implement decolorize
        Ok(df)
    }

    /// Build parallel arrays for keys, values, and ops from `DropKeepLabel` slices.
    ///
    /// Returns a tuple of (keys, values, ops) as `Vec<Expr>`.
    fn build_drop_keep_arrays(labels: &[crate::logql::log::DropKeepLabel]) -> (Vec<Expr>, Vec<Expr>, Vec<Expr>) {
        let keys: Vec<Expr> = labels.iter().map(|l| lit(l.name.as_str())).collect();

        let values: Vec<Expr> = labels
            .iter()
            .map(|l| {
                l.matcher
                    .as_ref()
                    .map_or_else(|| lit(ScalarValue::Utf8(None)), |matcher| lit(matcher.value.as_str()))
            })
            .collect();

        let ops: Vec<Expr> = labels
            .iter()
            .map(|l| {
                l.matcher
                    .as_ref()
                    .map_or_else(|| lit(ScalarValue::Utf8(None)), |matcher| lit(matcher.op.as_str()))
            })
            .collect();

        (keys, values, ops)
    }

    /// The attributes-map expression a `drop`/`keep` pipeline stage should
    /// filter: [`merged_attributes`] the first time a stage runs (`df` still
    /// carries the three per-level columns from the scan), or the
    /// already-materialized `attributes` column if an earlier stage in the
    /// same pipeline already merged and filtered it. Recomputing the merge in
    /// the second case would both discard the earlier stage's filtering and,
    /// once [`Self::apply_attributes_filter`] has collapsed the per-level
    /// columns away, reference columns that no longer exist.
    ///
    /// Delegates entirely to [`merged_attributes`] (and, through it, to
    /// [`schema_has_merged_attributes`]) rather than re-checking `df`'s
    /// schema here too: this exact "pre- or post-merge" question is asked
    /// from several places in this file, and answering it with one shared
    /// check — instead of a second inline copy that could quietly drift from
    /// the first — is what keeps them all in agreement.
    fn resolve_current_attributes(df: &DataFrame) -> Expr {
        merged_attributes(df.schema())
    }

    /// Apply a filter UDF to the attributes column and select all columns.
    ///
    /// Replaces the attributes source with the filtered version and preserves
    /// all other columns. The source is either the single `attributes` column
    /// (a later stage in the same pipeline) or the three per-level columns
    /// (the first stage) — see [`Self::resolve_current_attributes`]. In the
    /// latter case all three collapse into this one filtered output column:
    /// `drop`/`keep` operate on wire names across every level at once, so
    /// there is no per-level source left to keep once filtering has run.
    fn apply_attributes_filter(df: DataFrame, filtered_attrs: &Expr) -> Result<DataFrame> {
        let mut merged_attrs_emitted = false;
        let select_exprs: Vec<Expr> = df
            .schema()
            .inner()
            .fields()
            .iter()
            .filter_map(|field| {
                let name = field.name().as_str();
                let is_attribute_source = name == MERGED_ATTRIBUTES_COLUMN
                    || name == COL_RESOURCE_ATTRIBUTES
                    || name == COL_SCOPE_ATTRIBUTES
                    || name == COL_LOG_ATTRIBUTES;
                if !is_attribute_source {
                    return Some(col(name));
                }
                if merged_attrs_emitted {
                    None
                } else {
                    merged_attrs_emitted = true;
                    Some(filtered_attrs.clone().alias(MERGED_ATTRIBUTES_COLUMN))
                }
            })
            .collect();

        Ok(df.select(select_exprs)?)
    }

    /// Apply `LogQL` `drop` operator - removes specified labels from attributes
    /// map with optional matcher-based filtering.
    ///
    /// Uses the `map_drop_keys` UDF to filter the attributes map, removing
    /// entries whose keys match the specified labels, optionally with value matching.
    fn apply_drop(df: DataFrame, labels: &[crate::logql::log::DropKeepLabel]) -> Result<DataFrame> {
        if labels.is_empty() {
            return Ok(df);
        }

        let (keys, values, ops) = Self::build_drop_keep_arrays(labels);
        let udf = ScalarUDF::from(super::udf::MapDropKeys::new());
        let filtered_attrs = udf.call(vec![
            Self::resolve_current_attributes(&df),
            make_array(keys),
            make_array(values),
            make_array(ops),
        ]);

        Self::apply_attributes_filter(df, &filtered_attrs)
    }

    /// Apply `LogQL` `keep` operator - keeps only specified labels in
    /// attributes map with optional matcher-based filtering.
    ///
    /// Uses the `map_keep_keys` UDF to filter the attributes map, keeping
    /// only entries whose keys match the specified labels, optionally with value matching.
    fn apply_keep(df: DataFrame, labels: &[crate::logql::log::DropKeepLabel]) -> Result<DataFrame> {
        if labels.is_empty() {
            return Ok(df);
        }

        let (keys, values, ops) = Self::build_drop_keep_arrays(labels);
        let udf = ScalarUDF::from(super::udf::MapKeepKeys::new());
        let filtered_attrs = udf.call(vec![
            Self::resolve_current_attributes(&df),
            make_array(keys),
            make_array(values),
            make_array(ops),
        ]);

        Self::apply_attributes_filter(df, &filtered_attrs)
    }

    /// Applies a `LogQL` `LabelFilter` pipeline stage: `| label ...`.
    ///
    /// Reads `df`'s schema before building the filter expression, not just
    /// before evaluating it: this stage commonly follows `drop`/`keep` in
    /// the same pipeline (e.g. `| drop user_id | request_id="…"`), so the
    /// attribute shape it must resolve against — per-level or already
    /// merged — is whatever `df` carries *at this point*, not at scan time.
    /// See [`schema_has_merged_attributes`] for why this cannot be assumed.
    fn apply_label_filter(df: DataFrame, filter_expr: crate::logql::log::LabelFilterExpr) -> Result<DataFrame> {
        let expr = Self::label_filter_to_expr(filter_expr, df.schema())?;
        Ok(df.filter(expr)?)
    }

    /// Converts a `LabelFilterExpr` to a `DataFusion` predicate against the
    /// attribute shape described by `schema` — see [`Self::apply_label_filter`]
    /// for why the caller cannot use the pipeline's original scan schema.
    #[allow(clippy::items_after_statements)]
    fn label_filter_to_expr(filter: crate::logql::log::LabelFilterExpr, schema: &DFSchema) -> Result<Expr> {
        use crate::logql::log::LabelFilterExpr;

        match filter {
            LabelFilterExpr::And(left, right) => {
                let left_expr = Self::label_filter_to_expr(*left, schema)?;
                let right_expr = Self::label_filter_to_expr(*right, schema)?;
                Ok(left_expr.and(right_expr))
            }
            LabelFilterExpr::Or(left, right) => {
                let left_expr = Self::label_filter_to_expr(*left, schema)?;
                let right_expr = Self::label_filter_to_expr(*right, schema)?;
                Ok(left_expr.or(right_expr))
            }
            LabelFilterExpr::Parens(inner) => Self::label_filter_to_expr(*inner, schema),
            LabelFilterExpr::Matcher(matcher) => Ok(Self::matcher_to_expr(&matcher, schema)),
            LabelFilterExpr::Number { label, op, value } => {
                let internal_name = Self::map_label_to_internal_name(&label);
                let col_expr = if Self::is_top_level_field(internal_name) {
                    col(internal_name)
                } else {
                    attribute_lookup(&label, schema)
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
                    attribute_lookup(&label, schema)
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
                    attribute_lookup(&label, schema)
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

#[cfg(test)]
mod matcher_to_expr_tests {
    //! Behavioral tests for `DataFusionPlanner::matcher_to_expr`.
    //!
    //! `trace_id` and `span_id` are stored as `FIXED_LEN_BYTE_ARRAY`. The
    //! planner has to (a) reject malformed-hex matchers without poisoning
    //! the row set under three-valued logic and (b) make `=~` / `!~` work
    //! against the user-visible hex representation rather than the raw
    //! binary column (which `regexp_like` cannot accept).
    use datafusion::common::DFSchema;

    use super::DataFusionPlanner;
    use crate::logql::log::LabelMatcher;

    /// `trace_id`/`span_id` are top-level indexed columns, so every matcher
    /// in this module resolves through `col(mapped_label)` and never reaches
    /// [`super::attribute_lookup`] — an empty schema is a valid stand-in
    /// here because `matcher_to_expr`'s schema argument is simply unused on
    /// this path, not because these tests care about the merged-vs-per-level
    /// distinction (see `attribute_lookup_tests`/`merged_attributes_tests`
    /// below for tests that do).
    fn empty_schema() -> DFSchema {
        DFSchema::empty()
    }

    #[test]
    fn neq_on_trace_id_with_invalid_hex_matches_all_rows() {
        // Pre-fix this produced `col != typed_NULL`, which DataFusion's
        // three-valued logic folds to NULL → drops every row. The user
        // intent is the opposite: "match anything that is *not* this
        // (uninterpretable) literal" should yield every row.
        let m = LabelMatcher::neq("trace_id", "not-hex");
        let expr = DataFusionPlanner::matcher_to_expr(&m, &empty_schema());
        let s = format!("{expr:?}");
        assert!(
            s.contains("Boolean(true)"),
            "expected always-true literal expression for invalid-hex Neq, got: {s}"
        );
    }

    #[test]
    fn eq_on_trace_id_with_invalid_hex_matches_no_rows() {
        // Pre-fix this produced `col = typed_NULL` → also folded to NULL →
        // dropped every row. Same end result as Neq but for the wrong
        // reason; the post-fix expression is an explicit `false` literal so
        // the planner doesn't touch the row scanner at all.
        let m = LabelMatcher::eq("trace_id", "not-hex");
        let expr = DataFusionPlanner::matcher_to_expr(&m, &empty_schema());
        let s = format!("{expr:?}");
        assert!(
            s.contains("Boolean(false)"),
            "expected always-false literal expression for invalid-hex Eq, got: {s}"
        );
    }

    #[test]
    fn re_on_trace_id_encodes_column_as_hex_before_regex() {
        // `regexp_like` rejects FixedSizeBinary inputs at execution time, so
        // the planner must convert the binary column to its hex string
        // representation before applying the regex. The expression must
        // contain RegexpLike AND EncodeFunc (wrapping the column).
        let m = LabelMatcher::re("trace_id", "^abc");
        let expr = DataFusionPlanner::matcher_to_expr(&m, &empty_schema());
        let s = format!("{expr:?}");
        assert!(s.contains("RegexpLike"), "expected RegexpLike call in: {s}");
        assert!(s.contains("EncodeFunc"), "expected EncodeFunc wrapper in: {s}");
        assert!(s.contains("\"hex\""), "expected hex format literal in: {s}");
    }

    #[test]
    fn eq_on_trace_id_with_valid_hex_uses_typed_binary_literal() {
        let m = LabelMatcher::eq("trace_id", "0102030405060708090a0b0c0d0e0f10");
        let expr = DataFusionPlanner::matcher_to_expr(&m, &empty_schema());
        let s = format!("{expr:?}");
        // Must reach a typed FixedSizeBinary equality, not collapse to a
        // boolean literal — the row scan still needs the typed predicate
        // to drive Iceberg pruning.
        assert!(s.contains("FixedSizeBinary"), "expected typed binary literal in: {s}");
        assert!(s.contains("trace_id"), "expected column reference in: {s}");
    }
}

#[cfg(test)]
mod attribute_lookup_tests {
    //! Behavioral tests for the private [`super::attribute_lookup`] helper.
    //!
    //! The shape test below proves the precedence *order* baked into the
    //! expression tree without ever executing it. `docs/tests.md` requires
    //! execution-based coverage for planner semantics, so the remaining
    //! tests build a one-row batch and evaluate `attribute_lookup` against
    //! it through a real `SessionContext` — including the coalesce failure
    //! mode a shape assertion cannot see: a value present but empty at a
    //! specific level must stop the chain rather than let a less specific
    //! level's value leak through.

    use std::sync::Arc;

    use datafusion::{
        arrow::{
            array::{Array, ArrayRef, MapBuilder, StringArray, StringBuilder},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        common::DFSchema,
        datasource::MemTable,
        logical_expr::Expr,
        prelude::SessionContext,
        scalar::ScalarValue,
    };
    use icegate_common::schema::{COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES};

    /// The `(column, key)` a `map_get_by_normalized_key(col, lit)` call reads.
    ///
    /// Inspects the expression tree rather than its rendering: `docs/tests.md`
    /// rules out substring checks on a formatted plan as a semantic oracle,
    /// and argument ORDER is precisely what this test is about — a textual
    /// scan cannot distinguish argument position from incidental spelling.
    fn map_lookup_target(expr: &Expr) -> (&str, &str) {
        let Expr::ScalarFunction(lookup) = expr else {
            panic!("expected a scalar function, got {expr:?}");
        };
        assert_eq!(lookup.func.name(), "map_get_by_normalized_key");
        assert_eq!(lookup.args.len(), 2, "lookup takes (map, name)");
        let Expr::Column(column) = &lookup.args[0] else {
            panic!("first argument must be a column, got {:?}", lookup.args[0]);
        };
        let Expr::Literal(ScalarValue::Utf8(Some(key)), _) = &lookup.args[1] else {
            panic!("second argument must be a Utf8 literal, got {:?}", lookup.args[1]);
        };
        (column.name(), key.as_str())
    }

    #[test]
    fn attribute_lookup_coalesces_log_then_scope_then_resource() {
        // An empty schema is a valid stand-in for "the three per-level
        // columns have not been merged yet": schema_has_merged_attributes
        // only checks for the presence of MERGED_ATTRIBUTES_COLUMN by name,
        // so its absence is all this shape test needs, regardless of what
        // else the schema does or doesn't contain.
        let expr = super::attribute_lookup("k8s_pod_name", &DFSchema::empty());

        let Expr::ScalarFunction(coalesce) = &expr else {
            panic!("attribute_lookup must produce a coalesce, got {expr:?}");
        };
        assert_eq!(coalesce.func.name(), "coalesce");

        // `coalesce` returns its first non-NULL argument, so argument order IS
        // the precedence: most specific level first.
        let targets: Vec<(&str, &str)> = coalesce.args.iter().map(map_lookup_target).collect();
        assert_eq!(
            targets,
            vec![
                (COL_LOG_ATTRIBUTES, "k8s_pod_name"),
                (COL_SCOPE_ATTRIBUTES, "k8s_pod_name"),
                (COL_RESOURCE_ATTRIBUTES, "k8s_pod_name"),
            ]
        );
    }

    /// One-row fixture carrying ONLY the already-merged `attributes` column
    /// (no `log_attributes`/`scope_attributes`/`resource_attributes` at
    /// all), keyed in already-underscored wire form as a real merge output
    /// would be. Used by the schema-aware branch test below: since this
    /// schema never had the three per-level columns, planning against it
    /// would fail outright (`No field named log_attributes`) if
    /// `attribute_lookup` fell back to an unconditional coalesce — this is
    /// the C1 regression, reproduced at the unit level.
    fn build_merged_only_fixture_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![build_attribute_map_field(
            super::MERGED_ATTRIBUTES_COLUMN,
        )]));
        let attrs = build_attribute_map(&[("http_method", "GET")]);
        RecordBatch::try_new(schema, vec![attrs]).expect("record batch")
    }

    #[tokio::test]
    async fn attribute_lookup_reads_the_merged_column_directly_when_schema_already_has_it() {
        // Guards the exact branch schema_has_merged_attributes exists for —
        // see build_merged_only_fixture_batch's doc for why a "simplified"
        // unconditional coalesce would fail this test at plan construction,
        // not merely produce a wrong value.
        let batch = build_merged_only_fixture_batch();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memtable");
        let ctx = SessionContext::new();
        ctx.register_table("merged_only_fixture", Arc::new(table))
            .expect("register table");

        let df = ctx.table("merged_only_fixture").await.expect("table");
        let lookup_expr = super::attribute_lookup("http_method", df.schema()).alias("resolved");
        let df = df.select(vec![lookup_expr]).expect("select");
        let batches = df.collect().await.expect("collect");
        assert_eq!(batches.len(), 1, "single-partition fixture yields one batch");

        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("resolved column is Utf8");
        assert_eq!(column.len(), 1, "fixture has exactly one row");
        assert_eq!(column.value(0), "GET");
    }

    /// `MAP<Utf8, Utf8>` field shape produced by [`MapBuilder`]'s default
    /// element names (`entries`/`keys`/`values`), mirroring the fixture
    /// convention already used by the `traceql` planner's `MemTable` tests.
    fn build_attribute_map_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("keys", DataType::Utf8, false)),
                            Arc::new(Field::new("values", DataType::Utf8, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        )
    }

    /// Build a one-row `MAP<Utf8, Utf8>` array from stored (dotted) key/value
    /// pairs, in the given order — mirrors how ingest builds each row.
    fn build_attribute_map(pairs: &[(&str, &str)]) -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for (key, value) in pairs {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).expect("map row");
        Arc::new(builder.finish())
    }

    /// Single-row fixture exercising every case the tests below need, keyed
    /// in `OTel` dotted form throughout so a passing lookup also proves the
    /// dotted -> underscored normalization is live end to end:
    ///
    /// - `k8s.pod.name`: present at all three levels (precedence case).
    /// - `k8s.namespace.name`: present at scope + resource, absent at log
    ///   (fall-through case).
    /// - `http.method`: present at all three levels, but *empty string* at
    ///   log (the present-but-empty case that must not fall through).
    fn build_fixture_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            build_attribute_map_field(COL_LOG_ATTRIBUTES),
            build_attribute_map_field(COL_SCOPE_ATTRIBUTES),
            build_attribute_map_field(COL_RESOURCE_ATTRIBUTES),
        ]));

        let log = build_attribute_map(&[("k8s.pod.name", "log-value"), ("http.method", "")]);
        let scope = build_attribute_map(&[
            ("k8s.pod.name", "scope-value"),
            ("k8s.namespace.name", "scope-namespace"),
            ("http.method", "GET"),
        ]);
        let resource = build_attribute_map(&[
            ("k8s.pod.name", "resource-value"),
            ("k8s.namespace.name", "resource-namespace"),
            ("http.method", "POST"),
        ]);

        RecordBatch::try_new(schema, vec![log, scope, resource]).expect("record batch")
    }

    /// Evaluate `attribute_lookup(label)` against [`build_fixture_batch`]
    /// through a real `SessionContext` and return the resulting single
    /// cell, or `None` for SQL NULL.
    async fn evaluate_attribute_lookup(label: &str) -> Option<String> {
        let batch = build_fixture_batch();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memtable");
        let ctx = SessionContext::new();
        ctx.register_table("attribute_fixture", Arc::new(table))
            .expect("register table");

        let df = ctx.table("attribute_fixture").await.expect("table");
        let lookup_expr = super::attribute_lookup(label, df.schema()).alias("resolved");
        let df = df.select(vec![lookup_expr]).expect("select");
        let batches = df.collect().await.expect("collect");
        assert_eq!(batches.len(), 1, "single-partition fixture yields one batch");

        let column = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("resolved column is Utf8");
        assert_eq!(column.len(), 1, "fixture has exactly one row");
        if column.is_null(0) {
            None
        } else {
            Some(column.value(0).to_string())
        }
    }

    #[tokio::test]
    async fn attribute_lookup_prefers_log_value_when_key_present_in_all_three_maps() {
        // Precedence contract: log wins even though scope and resource also
        // carry `k8s.pod.name` (queried here in its Loki-legal underscored
        // form), proving the dotted -> underscored mapping is live too.
        assert_eq!(
            evaluate_attribute_lookup("k8s_pod_name").await,
            Some("log-value".to_string())
        );
    }

    #[tokio::test]
    async fn attribute_lookup_falls_through_to_scope_when_log_lacks_the_key() {
        // `k8s.namespace.name` is absent from log_attributes, so coalesce
        // must skip it and take the scope value, not the resource value.
        assert_eq!(
            evaluate_attribute_lookup("k8s_namespace_name").await,
            Some("scope-namespace".to_string())
        );
    }

    #[tokio::test]
    async fn attribute_lookup_resolves_present_but_empty_log_value_without_falling_through() {
        // The contract this helper exists for: `http.method` is present at
        // log with an EMPTY STRING value, which is non-NULL and must stop
        // the coalesce chain — not fall through to scope's "GET" or
        // resource's "POST". A regression here would silently return a
        // less specific level's value, and no other test would catch it.
        assert_eq!(evaluate_attribute_lookup("http_method").await, Some(String::new()));
    }
}

#[cfg(test)]
mod merged_attributes_tests {
    //! Behavioral tests for the private [`super::merged_attributes`] helper.
    //!
    //! [`super::udf::MapMergeNormalized`] already carries execution-based
    //! coverage of the merge/normalize/precedence rules themselves (see
    //! `udf::map_merge_normalized::tests`); these tests prove the planner
    //! wires that UDF to the three correct columns, in the correct
    //! precedence order, including through a real `SessionContext`.

    use std::sync::Arc;

    use datafusion::{
        arrow::{
            array::{Array, ArrayRef, MapArray, MapBuilder, StringArray, StringBuilder},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        common::DFSchema,
        datasource::MemTable,
        logical_expr::Expr,
        prelude::SessionContext,
    };
    use icegate_common::schema::{COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES};

    #[test]
    fn calls_the_merge_udf_over_resource_scope_and_log_in_precedence_order() {
        // See attribute_lookup_tests's identical use of an empty schema: it
        // only needs to lack MERGED_ATTRIBUTES_COLUMN by name to exercise
        // the not-yet-merged branch this shape test targets.
        let expr = super::merged_attributes(&DFSchema::empty());
        let Expr::ScalarFunction(merge) = &expr else {
            panic!("merged_attributes must produce a scalar function, got {expr:?}");
        };
        assert_eq!(merge.func.name(), "map_merge_normalized");

        // Argument order IS precedence order for MapMergeNormalized (resource
        // first, log last — see its doc comment).
        let columns: Vec<&str> = merge
            .args
            .iter()
            .map(|arg| match arg {
                Expr::Column(column) => column.name(),
                other => panic!("every merge argument must be a column, got {other:?}"),
            })
            .collect();
        assert_eq!(
            columns,
            vec![COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, COL_LOG_ATTRIBUTES]
        );
    }

    /// `MAP<Utf8, Utf8>` field shape produced by [`MapBuilder`]'s default
    /// element names, mirroring [`super::attribute_lookup_tests`]'s fixture
    /// convention (duplicated here rather than shared: the two modules are
    /// siblings under `planner`, so a private helper in one is not visible
    /// from the other).
    fn build_attribute_map_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("keys", DataType::Utf8, false)),
                            Arc::new(Field::new("values", DataType::Utf8, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        )
    }

    fn build_attribute_map(pairs: &[(&str, &str)]) -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for (key, value) in pairs {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).expect("map row");
        Arc::new(builder.finish())
    }

    #[tokio::test]
    async fn merges_and_normalizes_three_levels_through_a_real_session_context() {
        let schema = Arc::new(Schema::new(vec![
            build_attribute_map_field(COL_RESOURCE_ATTRIBUTES),
            build_attribute_map_field(COL_SCOPE_ATTRIBUTES),
            build_attribute_map_field(COL_LOG_ATTRIBUTES),
        ]));
        let resource = build_attribute_map(&[("k8s.pod.name", "resource-value"), ("shared.key", "resource")]);
        let scope = build_attribute_map(&[("shared.key", "scope")]);
        let log = build_attribute_map(&[("http.method", "GET")]);
        let batch = RecordBatch::try_new(schema, vec![resource, scope, log]).expect("record batch");

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memtable");
        let ctx = SessionContext::new();
        ctx.register_table("merge_fixture", Arc::new(table)).expect("register table");

        let df = ctx.table("merge_fixture").await.expect("table");
        let merged_expr = super::merged_attributes(df.schema()).alias("merged");
        let df = df.select(vec![merged_expr]).expect("select");
        let batches = df.collect().await.expect("collect");
        assert_eq!(batches.len(), 1, "single-partition fixture yields one batch");

        let map = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("merged column is MAP");
        let entries = map.value(0);
        let keys = entries.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
        let values = entries.column(1).as_any().downcast_ref::<StringArray>().expect("values");
        let merged: std::collections::BTreeMap<&str, &str> =
            (0..keys.len()).map(|i| (keys.value(i), values.value(i))).collect();

        assert_eq!(
            merged.get("k8s_pod_name"),
            Some(&"resource-value"),
            "dotted keys normalize to wire names"
        );
        assert_eq!(
            merged.get("shared_key"),
            Some(&"scope"),
            "scope wins over resource when log is absent (log-wins precedence)"
        );
        assert_eq!(merged.get("http_method"), Some(&"GET"));
        assert_eq!(merged.len(), 3, "keys from all three levels are unioned");
    }

    #[tokio::test]
    async fn returns_the_existing_column_directly_when_schema_already_has_it() {
        // Guards the branch schema_has_merged_attributes exists for, mirroring
        // attribute_lookup_tests's identical-purpose test: a schema carrying
        // ONLY the already-merged `attributes` column, with none of
        // resource/scope/log_attributes present at all. If merged_attributes
        // "simplified" back to an unconditional merge-UDF call over the three
        // per-level columns, this would fail to plan at all (`No field named
        // resource_attributes`) rather than merely return a wrong value — the
        // C1 regression, reproduced at the unit level.
        let schema = Arc::new(Schema::new(vec![build_attribute_map_field(
            super::MERGED_ATTRIBUTES_COLUMN,
        )]));
        let attrs = build_attribute_map(&[("already_merged_key", "already_merged_value")]);
        let batch = RecordBatch::try_new(schema, vec![attrs]).expect("record batch");

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memtable");
        let ctx = SessionContext::new();
        ctx.register_table("already_merged_fixture", Arc::new(table))
            .expect("register table");

        let df = ctx.table("already_merged_fixture").await.expect("table");
        let merged_expr = super::merged_attributes(df.schema()).alias("merged");
        let df = df.select(vec![merged_expr]).expect("select");
        let batches = df.collect().await.expect("collect");
        assert_eq!(batches.len(), 1, "single-partition fixture yields one batch");

        let map = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("merged column is MAP");
        let entries = map.value(0);
        let keys = entries.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
        let values = entries.column(1).as_any().downcast_ref::<StringArray>().expect("values");
        assert_eq!(keys.len(), 1, "the existing column passes through untouched");
        assert_eq!(keys.value(0), "already_merged_key");
        assert_eq!(values.value(0), "already_merged_value");
    }
}

#[cfg(test)]
mod drop_keep_attributes_tests {
    //! Behavioral tests for [`super::DataFusionPlanner::apply_drop`],
    //! [`super::DataFusionPlanner::apply_keep`], and the shared
    //! [`super::DataFusionPlanner::apply_attributes_filter`] /
    //! [`super::DataFusionPlanner::resolve_current_attributes`] machinery
    //! they route through.
    //!
    //! The three per-level columns must collapse into exactly one
    //! `attributes` output column — not zero (the filtered map silently
    //! disappearing from the output) and not three (stale per-level columns
    //! left behind) — and a second `drop`/`keep` stage in the same pipeline
    //! must operate on that already-merged column rather than recompute a
    //! fresh, unfiltered merge that would silently undo the first stage's
    //! filtering (or error, once the per-level columns are gone).

    use std::{collections::BTreeMap, sync::Arc};

    use datafusion::{
        arrow::{
            array::{Array, ArrayRef, MapArray, MapBuilder, StringArray, StringBuilder},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        dataframe::DataFrame,
        datasource::MemTable,
        prelude::SessionContext,
    };
    use icegate_common::schema::{COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES};

    use super::{DataFusionPlanner, MERGED_ATTRIBUTES_COLUMN};
    use crate::logql::{
        common::{ComparisonOp, MatchOp},
        log::{DropKeepLabel, LabelFilterExpr, LabelMatcher},
    };

    /// See [`super::merged_attributes_tests`]'s identical helper — not
    /// shared for the same sibling-module-privacy reason.
    fn build_attribute_map_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("keys", DataType::Utf8, false)),
                            Arc::new(Field::new("values", DataType::Utf8, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        )
    }

    fn build_attribute_map(pairs: &[(&str, &str)]) -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for (key, value) in pairs {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).expect("map row");
        Arc::new(builder.finish())
    }

    /// One-row `DataFrame` with the three (as yet unmerged) per-level
    /// columns, matching the shape `apply_drop`/`apply_keep` see as the
    /// first pipeline stage: `resource_attributes` carries `pairs`, `scope`
    /// and `log` are empty.
    async fn register_fixture(pairs: &[(&str, &str)]) -> DataFrame {
        let schema = Arc::new(Schema::new(vec![
            build_attribute_map_field(COL_RESOURCE_ATTRIBUTES),
            build_attribute_map_field(COL_SCOPE_ATTRIBUTES),
            build_attribute_map_field(COL_LOG_ATTRIBUTES),
        ]));
        let resource = build_attribute_map(pairs);
        let scope = build_attribute_map(&[]);
        let log = build_attribute_map(&[]);
        let batch = RecordBatch::try_new(schema, vec![resource, scope, log]).expect("record batch");

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memtable");
        let ctx = SessionContext::new();
        ctx.register_table("drop_keep_fixture", Arc::new(table))
            .expect("register table");
        ctx.table("drop_keep_fixture").await.expect("table")
    }

    /// Assert `df` has exactly one `attributes` column (the collapse
    /// contract), then collect it as a (key, value) map.
    async fn collect_single_attributes_map(df: DataFrame) -> BTreeMap<String, String> {
        let attribute_field_count = df
            .schema()
            .inner()
            .fields()
            .iter()
            .filter(|f| f.name() == MERGED_ATTRIBUTES_COLUMN)
            .count();
        assert_eq!(
            attribute_field_count, 1,
            "expected exactly one merged `attributes` column in the output"
        );

        let batches = df.collect().await.expect("collect");
        assert_eq!(batches.len(), 1, "single-partition fixture yields one batch");

        let map = batches[0]
            .column_by_name(MERGED_ATTRIBUTES_COLUMN)
            .expect("attributes column")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("attributes column is MAP");
        let entries = map.value(0);
        let keys = entries.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
        let values = entries.column(1).as_any().downcast_ref::<StringArray>().expect("values");
        (0..keys.len())
            .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
            .collect()
    }

    #[tokio::test]
    async fn drop_collapses_the_three_level_columns_into_one_attributes_column() {
        let df = register_fixture(&[("keep_me", "r1"), ("drop_me", "r2")]).await;
        let df = DataFusionPlanner::apply_drop(df, &[DropKeepLabel::new("drop_me")]).expect("apply_drop");

        let attrs = collect_single_attributes_map(df).await;
        assert_eq!(attrs.get("keep_me").map(String::as_str), Some("r1"));
        assert!(!attrs.contains_key("drop_me"));
    }

    #[tokio::test]
    async fn keep_collapses_the_three_level_columns_into_one_attributes_column() {
        let df = register_fixture(&[("keep_me", "r1"), ("drop_me", "r2")]).await;
        let df = DataFusionPlanner::apply_keep(df, &[DropKeepLabel::new("keep_me")]).expect("apply_keep");

        let attrs = collect_single_attributes_map(df).await;
        assert_eq!(attrs.len(), 1, "only the kept key should survive");
        assert_eq!(attrs.get("keep_me").map(String::as_str), Some("r1"));
    }

    #[tokio::test]
    async fn chained_drop_then_keep_operates_on_the_already_merged_column() {
        let df = register_fixture(&[("a", "1"), ("b", "2"), ("c", "3")]).await;

        // First stage: drop "a". This collapses the three raw columns into
        // one `attributes` column (see the collapse tests above).
        let df = DataFusionPlanner::apply_drop(df, &[DropKeepLabel::new("a")]).expect("apply_drop");

        // Second stage: keep "b". If this recomputed a fresh merge instead
        // of reading the already-merged `attributes` column back, it would
        // either error (the raw columns are gone) or silently resurrect "a"
        // by re-merging the untouched raw data — resolve_current_attributes
        // exists specifically to prevent both.
        let df = DataFusionPlanner::apply_keep(df, &[DropKeepLabel::new("b")]).expect("apply_keep");

        let attrs = collect_single_attributes_map(df).await;
        assert_eq!(attrs.len(), 1, "only 'b' should survive both stages");
        assert_eq!(attrs.get("b").map(String::as_str), Some("2"));
    }

    // ========================================================================
    // C1 regression: LabelFilter after Drop/Keep
    //
    // apply_attributes_filter (above) collapses the three per-level columns
    // into one `attributes` column. A `LabelFilter` pipeline stage reading
    // attributes AFTER that collapse used to still route through the
    // pre-merge coalesce (attribute_lookup ignored the DataFrame it was
    // about to run against), producing `No field named ...log_attributes` at
    // plan construction — a 500 for any `| drop`/`| keep` followed by
    // another attribute-reading stage. These tests execute that exact
    // two-stage pipeline and assert on the resulting rows, not on the plan.
    // ========================================================================

    /// Multi-row sibling of [`build_attribute_map`]: one row per entry of
    /// `rows`, needed below because a single-row fixture cannot distinguish
    /// "the filter matched" from "the filter was never applied".
    fn build_attribute_map_rows(rows: &[&[(&str, &str)]]) -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for pairs in rows {
            for (key, value) in *pairs {
                builder.keys().append_value(key);
                builder.values().append_value(value);
            }
            builder.append(true).expect("map row");
        }
        Arc::new(builder.finish())
    }

    /// Multi-row sibling of [`register_fixture`]: `resource_attributes`
    /// carries one row per entry of `rows`, `scope`/`log` empty on every row.
    async fn register_multi_row_fixture(rows: &[&[(&str, &str)]]) -> DataFrame {
        let schema = Arc::new(Schema::new(vec![
            build_attribute_map_field(COL_RESOURCE_ATTRIBUTES),
            build_attribute_map_field(COL_SCOPE_ATTRIBUTES),
            build_attribute_map_field(COL_LOG_ATTRIBUTES),
        ]));
        let resource = build_attribute_map_rows(rows);
        let empty_rows: Vec<&[(&str, &str)]> = vec![&[]; rows.len()];
        let scope = build_attribute_map_rows(&empty_rows);
        let log = build_attribute_map_rows(&empty_rows);
        let batch = RecordBatch::try_new(schema, vec![resource, scope, log]).expect("record batch");

        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memtable");
        let ctx = SessionContext::new();
        ctx.register_table("drop_keep_multi_row_fixture", Arc::new(table))
            .expect("register table");
        ctx.table("drop_keep_multi_row_fixture").await.expect("table")
    }

    /// Collect `df`'s `attributes` column as one `BTreeMap` per surviving
    /// row, in row order — the multi-row sibling of
    /// [`collect_single_attributes_map`], for tests where the row *count*
    /// (filter selectivity) is itself part of what's under test.
    async fn collect_attributes_per_row(df: DataFrame) -> Vec<BTreeMap<String, String>> {
        let batches = df.collect().await.expect("collect");
        let mut rows = Vec::new();
        for batch in &batches {
            let map = batch
                .column_by_name(MERGED_ATTRIBUTES_COLUMN)
                .expect("attributes column")
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("attributes column is MAP");
            for row in 0..map.len() {
                let entries = map.value(row);
                let keys = entries.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
                let values = entries.column(1).as_any().downcast_ref::<StringArray>().expect("values");
                rows.push(
                    (0..keys.len())
                        .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
                        .collect(),
                );
            }
        }
        rows
    }

    #[tokio::test]
    async fn label_filter_after_drop_matches_a_surviving_label_and_excludes_the_dropped_one() {
        // `{...} | drop user_id | request_id="req-456"` — C1's first repro.
        let df = register_multi_row_fixture(&[
            &[("user_id", "user-123"), ("request_id", "req-456")],
            &[("user_id", "user-999"), ("request_id", "req-000")],
        ])
        .await;

        let df = DataFusionPlanner::apply_drop(df, &[DropKeepLabel::new("user_id")]).expect("apply_drop");
        let filter = LabelFilterExpr::Matcher(LabelMatcher::new("request_id", MatchOp::Eq, "req-456"));
        let df = DataFusionPlanner::apply_label_filter(df, filter).expect("apply_label_filter");

        let rows = collect_attributes_per_row(df).await;
        assert_eq!(
            rows.len(),
            1,
            "only the request_id=\"req-456\" row should survive the filter"
        );
        assert_eq!(
            rows[0].get("request_id").map(String::as_str),
            Some("req-456"),
            "a surviving label must still match after drop"
        );
        assert!(
            !rows[0].contains_key("user_id"),
            "the dropped label must be absent from the output"
        );
    }

    #[tokio::test]
    async fn label_filter_after_keep_matches_a_surviving_label_and_excludes_the_dropped_one() {
        // `{...} | keep user_id | user_id="user-123"` — C1's second repro.
        let df = register_multi_row_fixture(&[
            &[("user_id", "user-123"), ("request_id", "req-456")],
            &[("user_id", "user-999"), ("request_id", "req-456")],
        ])
        .await;

        let df = DataFusionPlanner::apply_keep(df, &[DropKeepLabel::new("user_id")]).expect("apply_keep");
        let filter = LabelFilterExpr::Matcher(LabelMatcher::new("user_id", MatchOp::Eq, "user-123"));
        let df = DataFusionPlanner::apply_label_filter(df, filter).expect("apply_label_filter");

        let rows = collect_attributes_per_row(df).await;
        assert_eq!(
            rows.len(),
            1,
            "only the user_id=\"user-123\" row should survive the filter"
        );
        assert_eq!(
            rows[0].get("user_id").map(String::as_str),
            Some("user-123"),
            "a surviving label must still match after keep"
        );
        assert_eq!(
            rows[0].len(),
            1,
            "keep user_id must have already dropped every other label, including request_id"
        );
    }

    #[tokio::test]
    async fn numeric_label_filter_after_drop_compares_the_surviving_label_correctly() {
        // `{...} | drop user_id | http_duration_ms > 100` — C1's third repro.
        let df = register_multi_row_fixture(&[
            &[("user_id", "u1"), ("http_duration_ms", "150")],
            &[("user_id", "u2"), ("http_duration_ms", "50")],
        ])
        .await;

        let df = DataFusionPlanner::apply_drop(df, &[DropKeepLabel::new("user_id")]).expect("apply_drop");
        let filter = LabelFilterExpr::Number {
            label: "http_duration_ms".to_string(),
            op: ComparisonOp::Gt,
            value: 100.0,
        };
        let df = DataFusionPlanner::apply_label_filter(df, filter).expect("apply_label_filter");

        let rows = collect_attributes_per_row(df).await;
        assert_eq!(rows.len(), 1, "only the 150 > 100 row should survive the filter");
        assert_eq!(
            rows[0].get("http_duration_ms").map(String::as_str),
            Some("150"),
            "a surviving label must still compare correctly after drop"
        );
        assert!(
            !rows[0].contains_key("user_id"),
            "the dropped label must be absent from the output"
        );
    }
}
