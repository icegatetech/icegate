//! Metrics-mode planner: bucketed time series.
//!
//! Builds a deterministic time-bucket column (`floor(ts_micros / step) *
//! step`) and groups by it (plus any `by(...)` keys) to produce a
//! Prometheus-style time series. `rate()` divides the per-bucket count by
//! the step in seconds; `count_over_time()` emits the raw count per bucket.
//!
//! `histogram_over_time` is deferred — it requires a histogram UDF that is
//! not yet shared from the `LogQL` planner.

use datafusion::{
    arrow::datatypes::DataType,
    common::DFSchema,
    functions_aggregate::expr_fn::count,
    logical_expr::{Expr, ExprSchemable, lit},
    prelude::{DataFrame, col},
};
use icegate_common::schema::COL_TIMESTAMP;

use super::planner::not_implemented;
use crate::{
    error::Result,
    traceql::{
        metric::{GroupingKeys, MetricsFunction, PipelineStage},
        planner::QueryContext,
    },
};

/// Default step (60 seconds) used when [`QueryContext::step`] is `None`.
const DEFAULT_STEP_NANOS: i64 = 60_000_000_000;

/// Bucket alias — exposed in the output schema so downstream Tempo
/// formatting can locate the time-series x-axis column.
const BUCKET_ALIAS: &str = "__bucket";

/// Aggregated metric column alias.
const METRIC_ALIAS: &str = "__metric";

/// Apply a metrics-mode terminal function to a filtered spans `DataFrame`.
///
/// Composition order: time-bucket column + optional `by(...)` keys, grouped
/// then aggregated; `rate` then divides by `step_seconds`.
///
/// # Errors
///
/// - [`crate::error::QueryError::NotImplemented`] when a `prelude` stage is
///   anything other than `by(...)` (v1 only allows `by` in the prelude).
/// - [`crate::error::QueryError::NotImplemented`] for `histogram_over_time`
///   (v1 deferred — needs a `DataFusion` histogram UDF).
/// - [`crate::error::QueryError::DataFusion`] propagates underlying planner
///   errors (e.g., `cast_to`, aggregate, `with_column`).
pub fn apply_metrics(
    df: DataFrame,
    prelude: &[PipelineStage],
    function: &MetricsFunction,
    group_by: Option<&GroupingKeys>,
    ctx: &QueryContext,
) -> Result<DataFrame> {
    let step_ns = ctx.step.and_then(|s| s.num_nanoseconds()).unwrap_or(DEFAULT_STEP_NANOS);

    // Group keys: time bucket first (becomes the x-axis), then optional
    // user-provided `by(...)` keys from prelude and/or the terminal function.
    let mut group_keys: Vec<Expr> = Vec::with_capacity(1 + prelude.len() + group_by.map_or(0, |g| g.keys.len()));
    group_keys.push(time_bucket_expr(step_ns)?);

    for stage in prelude {
        if let PipelineStage::By(GroupingKeys { keys }) = stage {
            for k in keys {
                group_keys.push(super::pipeline_field_ref_to_group_key(k));
            }
        } else {
            return Err(not_implemented("metrics-mode prelude stage other than `by`"));
        }
    }
    if let Some(g) = group_by {
        for k in &g.keys {
            group_keys.push(super::pipeline_field_ref_to_group_key(k));
        }
    }

    let agg_expr: Expr = match function {
        MetricsFunction::Rate | MetricsFunction::CountOverTime => count(lit(1_i64)).alias(METRIC_ALIAS),
        MetricsFunction::HistogramOverTime { .. } => {
            return Err(not_implemented("histogram_over_time"));
        }
    };

    let df = df.aggregate(group_keys, vec![agg_expr])?;

    // Rate: divide the bucketed count by step in seconds. Cast to f64 so the
    // division is floating-point.
    let df = match function {
        MetricsFunction::Rate => {
            #[allow(clippy::cast_precision_loss)] // step_ns ≤ ~3.6e12 (1h) → fits in f64 exactly
            let step_secs = step_ns as f64 / 1_000_000_000.0;
            let schema = DFSchema::empty();
            let rate_expr = col(METRIC_ALIAS).cast_to(&DataType::Float64, &schema)? / lit(step_secs);
            df.with_column(METRIC_ALIAS, rate_expr)?
        }
        MetricsFunction::CountOverTime | MetricsFunction::HistogramOverTime { .. } => df,
    };

    Ok(df)
}

/// Build the per-row time-bucket expression.
///
/// Computes `floor(ts_micros / step_micros) * step_micros` so every row
/// inside the same `step` window collapses to a single bucket value. The
/// result is i64 micros — a deterministic, integer-arithmetic alternative
/// to a `date_grid` UDF for v1.
fn time_bucket_expr(step_ns: i64) -> Result<Expr> {
    let step_micros = step_ns / 1_000;
    let schema = DFSchema::empty();
    // Cast Timestamp(Microsecond) to Int64 micros so integer arithmetic works
    // uniformly. DataFusion implicitly truncates toward zero for integer
    // division, which matches `floor` for non-negative epoch micros.
    let ts_micros = col(COL_TIMESTAMP).cast_to(&DataType::Int64, &schema)?;
    Ok(((ts_micros / lit(step_micros)) * lit(step_micros)).alias(BUCKET_ALIAS))
}
