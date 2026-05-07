//! Translates [`SpansetExpr`] into `DataFusion` filter expressions.

use std::ops::Not;

use datafusion::{
    functions::regex::regexp_like,
    functions_nested::{extract::array_element, map_extract::map_extract},
    logical_expr::{Expr, lit},
    prelude::{DataFrame, col},
    scalar::ScalarValue,
};
use icegate_common::schema::{
    COL_CLOUD_ACCOUNT_ID, COL_DURATION_MICROS, COL_KIND, COL_NAME, COL_RESOURCE_ATTRIBUTES, COL_SERVICE_NAME,
    COL_SPAN_ATTRIBUTES, COL_SPAN_ID, COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_ID,
};

use super::planner::not_implemented;
use crate::{
    error::Result,
    traceql::{
        common::{ComparisonOp, FieldRef, IntrinsicField, LiteralValue, ParentScope, Scope},
        spanset::{SpanFilter, SpanSelector, SpansetExpr},
    },
};

/// Apply a top-level spanset expression as a `WHERE` clause on the spans
/// dataframe.
///
/// # Errors
///
/// Returns [`crate::error::QueryError::NotImplemented`] for hierarchy operators and
/// for spanset boolean composition between distinct selectors.
pub fn apply_spanset(df: DataFrame, expr: SpansetExpr) -> Result<DataFrame> {
    match expr {
        SpansetExpr::Selector(s) => apply_selector(df, s),
        SpansetExpr::Op { op, .. } => Err(not_implemented(&format!("spanset operator '{}'", op.as_str()))),
    }
}

fn apply_selector(df: DataFrame, selector: SpanSelector) -> Result<DataFrame> {
    match selector.filter {
        None => Ok(df),
        Some(filter) => {
            let pred = filter_to_expr(&filter);
            Ok(df.filter(pred)?)
        }
    }
}

fn filter_to_expr(f: &SpanFilter) -> Expr {
    match f {
        SpanFilter::Paren(inner) => filter_to_expr(inner),
        SpanFilter::Not(inner) => Not::not(filter_to_expr(inner)),
        SpanFilter::And(l, r) => filter_to_expr(l).and(filter_to_expr(r)),
        SpanFilter::Or(l, r) => filter_to_expr(l).or(filter_to_expr(r)),
        SpanFilter::Compare { field, op, value } => compare_to_expr(field, *op, value),
    }
}

fn compare_to_expr(field: &FieldRef, op: ComparisonOp, value: &LiteralValue) -> Expr {
    match field {
        FieldRef::Intrinsic(intrinsic) => {
            let lhs = intrinsic_column(*intrinsic);
            let rhs = literal_to_scalar(field, value);
            apply_op(lhs, op, rhs)
        }
        FieldRef::Attribute { scope, name } => attribute_compare(*scope, name, op, field, value),
    }
}

/// Apply a [`ComparisonOp`] between a LHS expression and a RHS literal expression.
fn apply_op(lhs: Expr, op: ComparisonOp, rhs: Expr) -> Expr {
    match op {
        ComparisonOp::Eq => lhs.eq(rhs),
        ComparisonOp::Neq => lhs.not_eq(rhs),
        ComparisonOp::Gt => lhs.gt(rhs),
        ComparisonOp::Ge => lhs.gt_eq(rhs),
        ComparisonOp::Lt => lhs.lt(rhs),
        ComparisonOp::Le => lhs.lt_eq(rhs),
        ComparisonOp::Re => regex_match(lhs, rhs, false),
        ComparisonOp::Nre => regex_match(lhs, rhs, true),
    }
}

/// Map a `TraceQL` intrinsic field to the physical spans-table column (or a
/// NULL literal for v1-unmodelled intrinsics).
pub(crate) fn intrinsic_column(i: IntrinsicField) -> Expr {
    match i {
        IntrinsicField::Name => col(COL_NAME),
        IntrinsicField::Status => col(COL_STATUS_CODE),
        IntrinsicField::StatusMessage => col(COL_STATUS_MESSAGE),
        IntrinsicField::Kind => col(COL_KIND),
        IntrinsicField::Duration => col(COL_DURATION_MICROS),
        IntrinsicField::TraceID => col(COL_TRACE_ID),
        IntrinsicField::SpanID => col(COL_SPAN_ID),
        IntrinsicField::TraceDuration
        | IntrinsicField::RootName
        | IntrinsicField::RootServiceName
        | IntrinsicField::Parent
        | IntrinsicField::EventName
        | IntrinsicField::EventTimeSinceStart
        | IntrinsicField::LinkTraceID
        | IntrinsicField::LinkSpanID => {
            // v1 simplification: these intrinsics are not modelled. Return a
            // NULL literal so comparisons fall through (matches Tempo's
            // behaviour for missing intrinsics).
            //
            // `TraceDuration` is the *trace*'s wall-clock duration (root
            // span end - root span start) — it is NOT the per-span
            // `duration_micros`. Aliasing it to the spans column would
            // silently return wrong results for `{ traceDuration > 5s }`,
            // so until we model trace-level fields we make any comparison
            // against `TraceDuration` produce no matches.
            lit(ScalarValue::Utf8(None))
        }
    }
}

/// Build the comparison expression for an attribute filter, routing to the
/// right spans column(s) per scope.
///
/// Routing:
/// - `Scope::Resource` / `Scope::Parent(Resource)` -> `resource_attributes[name]`
/// - `Scope::Span`     / `Scope::Parent(Span)`     -> `span_attributes[name]`
/// - `Scope::Any` (`.name` shorthand) -> `(resource_cmp) OR (span_cmp)`
/// - `Scope::Event` / `Scope::Link`   -> NULL predicate (v1 unmodelled)
///
/// Well-known indexed resource keys (`service.name`, `cloud.account.id`) are
/// short-circuited to the corresponding top-level column — but only in the
/// Resource and Any paths. A query against `span.service.name` would still
/// go through `span_attributes[service.name]`; that's rare enough that the
/// lost optimization is acceptable.
fn attribute_compare(scope: Scope, name: &str, op: ComparisonOp, field: &FieldRef, value: &LiteralValue) -> Expr {
    match scope {
        Scope::Resource | Scope::Parent(ParentScope::Resource) => {
            apply_op(resource_attribute_lhs(name), op, literal_to_scalar(field, value))
        }
        Scope::Span | Scope::Parent(ParentScope::Span) => {
            apply_op(span_attribute_lhs(name), op, literal_to_scalar(field, value))
        }
        Scope::Any => {
            // OR-of-comparisons. We re-resolve the RHS for each branch so the
            // `literal_to_scalar` field-specific coercion is consistent (it's
            // identical for both branches but building twice is cheap and
            // avoids accidental reuse of a moved Expr).
            let resource_cmp = apply_op(resource_attribute_lhs(name), op, literal_to_scalar(field, value));
            let span_cmp = apply_op(span_attribute_lhs(name), op, literal_to_scalar(field, value));
            resource_cmp.or(span_cmp)
        }
        Scope::Event | Scope::Link => {
            // Event/link attribute scopes aren't modelled in v1 — they live
            // in nested ARRAY<STRUCT> columns. Return a comparison against
            // NULL so no row matches.
            apply_op(lit(ScalarValue::Utf8(None)), op, literal_to_scalar(field, value))
        }
    }
}

/// LHS expression for a resource-scoped attribute lookup.
///
/// Short-circuits to the matching top-level column when possible so partition
/// pruning and row-group skipping can fire.
pub(crate) fn resource_attribute_lhs(name: &str) -> Expr {
    if let Some(indexed_col) = indexed_resource_column(name) {
        return col(indexed_col);
    }
    // `map_extract(map, key)` returns `List<V>` (values for matching
    // entries; typically zero or one). Unwrap to scalar via
    // `array_element(list, 1)` so downstream `=`, `!=`, etc. can
    // coerce against the RHS literal. Missing keys collapse to NULL.
    array_element(map_extract(col(COL_RESOURCE_ATTRIBUTES), lit(name)), lit(1_i64))
}

/// LHS expression for a span-scoped attribute lookup.
///
/// No indexed short-circuit: span-scoped keys live only in the per-span
/// attributes map. (The top-level `service_name` / `cloud_account_id`
/// columns carry resource-level values, so a `span.service.name` filter
/// must stay inside `span_attributes`.)
pub(crate) fn span_attribute_lhs(name: &str) -> Expr {
    array_element(map_extract(col(COL_SPAN_ATTRIBUTES), lit(name)), lit(1_i64))
}

/// Map a `TraceQL` attribute name onto the spans-table column that holds the
/// same value as an indexed top-level field.
///
/// At ingest time, certain `OTel` resource attributes are promoted to
/// dedicated columns (see `crates/icegate-ingest/src/transform.rs`):
/// `service.name` -> `service_name`, `cloud.account.id` -> `cloud_account_id`.
/// Filtering on the indexed column is dramatically faster because the
/// attributes MAP is a per-row blob that can't be pruned.
fn indexed_resource_column(name: &str) -> Option<&'static str> {
    match name {
        "service.name" | "resource.service.name" => Some(COL_SERVICE_NAME),
        "cloud.account.id" | "resource.cloud.account.id" => Some(COL_CLOUD_ACCOUNT_ID),
        _ => None,
    }
}

fn literal_to_scalar(field: &FieldRef, lit_val: &LiteralValue) -> Expr {
    match (field, lit_val) {
        // Status / kind: the LHS is an INT column, so coerce enum literals.
        (FieldRef::Intrinsic(IntrinsicField::Status), LiteralValue::Status(s)) => lit(s.otlp_code()),
        (FieldRef::Intrinsic(IntrinsicField::Kind), LiteralValue::Kind(k)) => lit(k.otlp_code()),
        // Duration: LHS is BIGINT micros, RHS literal is nanos → divide.
        // Round *up* so a sub-microsecond literal like `500ns` does not
        // silently collapse to 0µs and start matching every row. Worst
        // case the user asked for `> 500ns` and we test against `> 1µs`,
        // which is still a meaningful filter.
        // `TraceDuration` is intentionally absent here: its LHS is `Utf8(NULL)`
        // (see `intrinsic_column`), so the literal does not need micro
        // coercion — falling through to the catch-all `Duration` arm below
        // is fine and never matches.
        (FieldRef::Intrinsic(IntrinsicField::Duration), LiteralValue::Duration(ns)) => {
            lit(ns.saturating_add(999) / 1_000)
        }
        // Trace / span identifiers are stored as raw FixedSizeBinary; the
        // literal arrives from TraceQL as a hex string. Decode and emit a
        // typed binary literal so the predicate matches the column type.
        // Invalid hex collapses to a NULL literal, which never matches.
        (FieldRef::Intrinsic(IntrinsicField::TraceID), LiteralValue::String(s)) => match hex::decode(s) {
            Ok(bytes) if bytes.len() == 16 => lit(ScalarValue::FixedSizeBinary(16, Some(bytes))),
            _ => lit(ScalarValue::FixedSizeBinary(16, None)),
        },
        (FieldRef::Intrinsic(IntrinsicField::SpanID), LiteralValue::String(s)) => match hex::decode(s) {
            Ok(bytes) if bytes.len() == 8 => lit(ScalarValue::FixedSizeBinary(8, Some(bytes))),
            _ => lit(ScalarValue::FixedSizeBinary(8, None)),
        },
        // String / numeric / bool — direct.
        (_, LiteralValue::String(s)) => lit(s.clone()),
        (_, LiteralValue::Int(i)) => lit(*i),
        (_, LiteralValue::Float(f)) => lit(*f),
        (_, LiteralValue::Bool(b)) => lit(*b),
        (_, LiteralValue::Bytes(b)) => lit(*b),
        (_, LiteralValue::Nil) => lit(ScalarValue::Utf8(None)),
        // Status/kind on an attribute (e.g., `span.something = error`) — keep
        // the enum's source-form spelling.
        (_, LiteralValue::Status(s)) => lit(match s {
            crate::traceql::common::StatusValue::Ok => "ok",
            crate::traceql::common::StatusValue::Error => "error",
            crate::traceql::common::StatusValue::Unset => "unset",
        }),
        (_, LiteralValue::Kind(k)) => lit(match k {
            crate::traceql::common::KindValue::Server => "server",
            crate::traceql::common::KindValue::Client => "client",
            crate::traceql::common::KindValue::Producer => "producer",
            crate::traceql::common::KindValue::Consumer => "consumer",
            crate::traceql::common::KindValue::Internal => "internal",
            crate::traceql::common::KindValue::Unspecified => "unspecified",
        }),
        // Duration on a non-duration column — fall through with raw nanos.
        (_, LiteralValue::Duration(ns)) => lit(*ns),
    }
}

fn regex_match(lhs: Expr, rhs: Expr, negated: bool) -> Expr {
    // Pass the operands through directly — DataFusion infers Utf8 coercion
    // from `regexp_like`'s signature. The previous `cast_to(...)` calls
    // used an empty `DFSchema` which silently broke plan-time column
    // refs (the cast looks up the column type via the schema and finds
    // nothing), so attribute-scoped patterns like `{ name =~ "GET.*" }`
    // would either misbehave or be rejected during type-checking.
    let m = regexp_like().call(vec![lhs, rhs]);
    if negated { m.not() } else { m }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traceql::common::IntrinsicField;

    #[test]
    fn intrinsic_status_maps_to_status_code_column() {
        let e = intrinsic_column(IntrinsicField::Status);
        // Just check that it doesn't panic and returns a plain column ref.
        assert!(matches!(e, Expr::Column(_)));
    }

    #[test]
    fn duration_literal_is_normalized_to_micros() {
        let e = literal_to_scalar(
            &FieldRef::Intrinsic(IntrinsicField::Duration),
            &LiteralValue::Duration(1_000_000_000),
        );
        // 1s = 1e9 ns = 1e6 micros — encoded as Int64(1_000_000). Assert
        // both "contains 1000000" and "does NOT contain 1000000000" so a
        // regression that drops the ns→µs conversion entirely (and emits
        // the raw nanosecond literal) cannot satisfy the substring check.
        let s = format!("{e:?}");
        assert!(s.contains("1000000"), "expected micros in {s:?}");
        assert!(
            !s.contains("1000000000"),
            "expected ns→µs conversion, got raw nanos in {s:?}"
        );
    }
}
