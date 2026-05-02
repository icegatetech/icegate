//! Search-mode pipeline planner.
//!
//! Implements `count() / sum() / avg() / min() / max()` aggregations,
//! optionally grouped by `by(...)`, and `agg() OP literal` aggregate
//! filters.

use datafusion::{
    functions::core::expr_fn::coalesce,
    functions_aggregate::expr_fn::{avg, count, max, min, sum},
    logical_expr::{Expr, lit},
    prelude::{DataFrame, col},
    scalar::ScalarValue,
};
use icegate_common::schema::COL_TRACE_ID;

use super::planner::{not_implemented, validation};
use crate::{
    error::Result,
    traceql::{
        common::{ComparisonOp, FieldRef, IntrinsicField, LiteralValue, ParentScope, Scope},
        metric::{AggregationOp, GroupingKeys, PipelineStage},
        planner::QueryContext,
    },
};

/// Apply pipeline stages to a filtered spans `DataFrame`.
///
/// The default group key is `trace_id` (one row per matching trace). A
/// `by(...)` stage replaces the group keys for any subsequent aggregation.
///
/// # Errors
///
/// - [`crate::error::QueryError::NotImplemented`] for `quantile_over_time`
///   (v1 deferred), regex on aggregate results, and aggregations that
///   require a field argument but receive `None`.
/// - [`crate::error::QueryError::DataFusion`] propagates any underlying
///   `DataFusion` planner error.
pub fn apply_search_pipeline(mut df: DataFrame, stages: &[PipelineStage], _ctx: &QueryContext) -> Result<DataFrame> {
    // Default grouping: one row per trace. `by(...)` overrides this for
    // subsequent aggregations.
    let mut group_keys: Vec<Expr> = vec![col(COL_TRACE_ID)];

    for stage in stages {
        match stage {
            PipelineStage::By(GroupingKeys { keys }) => {
                group_keys = keys.iter().map(field_ref_to_group_key).collect::<Result<Vec<_>>>()?;
            }
            PipelineStage::Aggregate { op, arg, percentile } => {
                df = apply_aggregate(df, &group_keys, *op, arg.as_ref(), *percentile)?;
            }
            PipelineStage::AggregateFilter {
                op,
                arg,
                percentile,
                cmp,
                value,
            } => {
                df = apply_aggregate(df, &group_keys, *op, arg.as_ref(), *percentile)?;
                df = apply_aggregate_filter(df, *op, *cmp, value)?;
            }
        }
    }

    Ok(df)
}

/// Convert a `FieldRef` into the `DataFusion` `Expr` used as a `GROUP BY`
/// key.
///
/// Intrinsics that map to real columns are resolved through
/// [`super::intrinsic_column`] (with `trace_id` short-circuited to a plain
/// column reference for clarity); attributes are resolved through
/// [`group_key_for_attribute`] which picks the right map column per scope.
///
/// Intrinsics that aren't modelled by a real spans column (e.g.
/// `traceDuration`, `rootName`) are rejected with a clear server-side
/// error: `intrinsic_column` would otherwise return `lit(NULL)` and silently
/// collapse every row into a single NULL group, which is worse than a 400.
pub(crate) fn field_ref_to_group_key(field: &FieldRef) -> Result<Expr> {
    Ok(match field {
        // Short-circuit the common case so the optimizer sees a plain column ref.
        FieldRef::Intrinsic(IntrinsicField::TraceID) => col(COL_TRACE_ID),
        // Modelled intrinsics — `intrinsic_column` returns a real column ref.
        FieldRef::Intrinsic(
            i @ (IntrinsicField::Name
            | IntrinsicField::Status
            | IntrinsicField::StatusMessage
            | IntrinsicField::Kind
            | IntrinsicField::Duration
            | IntrinsicField::SpanID),
        ) => super::intrinsic_column(*i),
        FieldRef::Intrinsic(other) => {
            return Err(not_implemented(&format!(
                "grouping by intrinsic {other:?} is not supported"
            )));
        }
        FieldRef::Attribute { scope, name } => group_key_for_attribute(*scope, name)?,
    })
}

/// Pick a single value expression for GROUP BY when grouping by an attribute.
///
/// Filters use OR-of-comparisons for `Scope::Any`, but GROUP BY needs ONE
/// value per row. We COALESCE resource first, then span — collision (same
/// key present in both maps with different values) produces the resource
/// value, which matches `OTel` Semantic Conventions (resource attrs are the
/// more stable identity).
///
/// `Scope::Event` / `Scope::Link` raise `NotImplemented`: returning a
/// NULL literal would silently collapse every row into a single group,
/// which is worse than a clear server-side error.
fn group_key_for_attribute(scope: Scope, name: &str) -> Result<Expr> {
    Ok(match scope {
        Scope::Resource | Scope::Parent(ParentScope::Resource) => super::resource_attribute_lhs(name),
        Scope::Span | Scope::Parent(ParentScope::Span) => super::span_attribute_lhs(name),
        Scope::Any => coalesce(vec![
            super::resource_attribute_lhs(name),
            super::span_attribute_lhs(name),
        ]),
        Scope::Event | Scope::Link => {
            return Err(not_implemented("grouping by event/link attributes"));
        }
    })
}

/// Build and apply a single aggregation over `df`.
fn apply_aggregate(
    df: DataFrame,
    group_keys: &[Expr],
    op: AggregationOp,
    arg: Option<&FieldRef>,
    percentile: Option<f64>,
) -> Result<DataFrame> {
    let agg_alias = format!("__agg_{}", op.as_str());
    let agg_expr: Expr = match op {
        AggregationOp::Count => count(lit(1_i64)).alias(agg_alias),
        AggregationOp::Sum => sum(field_arg_expr(arg)?).alias(agg_alias),
        AggregationOp::Avg => avg(field_arg_expr(arg)?).alias(agg_alias),
        AggregationOp::Min => min(field_arg_expr(arg)?).alias(agg_alias),
        AggregationOp::Max => max(field_arg_expr(arg)?).alias(agg_alias),
        AggregationOp::Quantile => {
            let p = percentile
                .ok_or_else(|| validation("quantile_over_time(.field, p) requires a percentile literal in [0, 1]"))?;
            // `approx_percentile_cont` takes a `Sort` for the first arg
            // (the value to estimate the percentile over). Order doesn't
            // matter for percentile estimation — the t-digest is
            // associative — so we sort ascending with nulls last for
            // deterministic explain output.
            datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont(
                field_arg_expr(arg)?.sort(true, false),
                lit(p),
                None,
            )
            .alias(agg_alias)
        }
    };
    Ok(df.aggregate(group_keys.to_vec(), vec![agg_expr])?)
}

/// Apply a `HAVING`-style filter on the aggregated column.
fn apply_aggregate_filter(
    df: DataFrame,
    op: AggregationOp,
    cmp: ComparisonOp,
    value: &LiteralValue,
) -> Result<DataFrame> {
    let alias = format!("__agg_{}", op.as_str());
    let lhs = col(&alias);
    let rhs: Expr = literal_to_filter_expr(value);
    let pred = match cmp {
        ComparisonOp::Eq => lhs.eq(rhs),
        ComparisonOp::Neq => lhs.not_eq(rhs),
        ComparisonOp::Gt => lhs.gt(rhs),
        ComparisonOp::Ge => lhs.gt_eq(rhs),
        ComparisonOp::Lt => lhs.lt(rhs),
        ComparisonOp::Le => lhs.lt_eq(rhs),
        ComparisonOp::Re | ComparisonOp::Nre => {
            return Err(not_implemented("regex on aggregate result"));
        }
    };
    Ok(df.filter(pred)?)
}

/// Resolve the field argument required by `sum/avg/min/max` to a column expr.
fn field_arg_expr(arg: Option<&FieldRef>) -> Result<Expr> {
    let Some(field) = arg else {
        // Caller-side mistake (e.g. `sum()` with no arg) — surface as 400.
        return Err(validation(
            "aggregation `sum / avg / min / max` requires a field argument",
        ));
    };
    Ok(match field {
        FieldRef::Intrinsic(i) => super::intrinsic_column(*i),
        FieldRef::Attribute { scope, name } => group_key_for_attribute(*scope, name)?,
    })
}

/// Coerce a `TraceQL` literal into a `DataFusion` `Expr` suitable for
/// comparison against an aggregate result.
///
/// Duration literals are stored as nanoseconds in the AST; aggregate values
/// over `duration_micros` are in microseconds, so we pre-divide here. Use
/// the same round-up coercion as the selector path
/// ([`super::selectors`]) so a sub-microsecond literal like `500ns` does
/// not silently collapse to `0µs` and start matching every row.
fn literal_to_filter_expr(v: &LiteralValue) -> Expr {
    match v {
        LiteralValue::Int(i) => lit(*i),
        LiteralValue::Float(f) => lit(*f),
        LiteralValue::Duration(ns) => lit(ns.saturating_add(999) / 1_000), // ns → micros, round up
        LiteralValue::Bytes(b) => lit(*b),
        LiteralValue::String(s) => lit(s.clone()),
        LiteralValue::Bool(b) => lit(*b),
        LiteralValue::Nil => lit(ScalarValue::Utf8(None)),
        LiteralValue::Status(s) => lit(s.otlp_code()),
        LiteralValue::Kind(k) => lit(k.otlp_code()),
    }
}
