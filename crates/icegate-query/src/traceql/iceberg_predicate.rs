//! Translate a parsed `TraceQL` query into an iceberg [`Predicate`].
//!
//! The result is suitable for file and row-group pruning on the `spans`
//! table's metadata-scan path. Mirrors the LogQL
//! [`crate::loki::predicate::selector_predicate`] shape for
//! tag-discovery: callers parse a `TraceQL` query string with the
//! existing ANTLR parser, hand the resulting AST to
//! [`translate_query_to_predicate`], and pass the resulting predicate
//! into [`crate::engine::metadata_scan::scan_labels`] /
//! [`crate::engine::metadata_scan::scan_label_values`] /
//! [`crate::engine::metadata_scan::scan_label_int_values`] as the
//! `extra_predicate`.
//!
//! # Translation rules
//!
//! Only conjuncts targeting **indexed top-level columns** with simple
//! `=` / `!=` ops translate. Everything else collapses to
//! [`Predicate::AlwaysTrue`]. The metadata-scan layer documents
//! over-approximation as the contract: dropping a clause only widens
//! the candidate file/row-group set, never narrows it.
//!
//! Pushdownable:
//! - `service.name` / `resource.service.name` → `service_name` column.
//! - Intrinsic `name` → `name` column.
//! - Intrinsic `traceID`, attribute `trace.id` → `trace_id` column.
//! - Intrinsic `spanID`, attribute `span.id` → `span_id` column.
//! - Attribute `parent.span.id` → `parent_span_id` column.
//! - Intrinsic `status` → `status_code` column (INT, OTLP code).
//! - Intrinsic `kind` → `kind` column (INT, OTLP `SpanKind`).
//! - `&&` between any two pushdownable conjuncts (AND-of-translations,
//!   with `AlwaysTrue` collapse so a non-pushdownable side simply
//!   widens the result rather than discarding the whole AND).
//! - `||` between any two pushdownable conjuncts (only when **both**
//!   sides translate; if either side is `AlwaysTrue` the whole OR
//!   becomes `AlwaysTrue` because pruning by one side of a union would
//!   discard files the other side wanted).
//! - `!Compare` foldable into `not_equal_to` / `equal_to`.
//!
//! Dropped → [`Predicate::AlwaysTrue`]:
//! - Regex (`=~`, `!~`).
//! - Range ops (`<`, `<=`, `>`, `>=`) — we don't push range predicates
//!   into iceberg today even when the LHS is indexed.
//! - MAP-attribute predicates (`span.http.method = "GET"`, etc.) —
//!   iceberg's `Predicate` doesn't model the `map_extract`/array-element
//!   chain.
//! - `Scope::Any` (`.foo`) attribute references — would require
//!   OR-of-MAPs, which is itself non-pushdownable.
//! - `Scope::Event`, `Scope::Link`, `Scope::Parent(_)` attributes.
//! - Unmodelled intrinsics (`duration`, `traceDuration`, `rootName`,
//!   `rootServiceName`, `statusMessage`, `Parent`, `event:*`, `link:*`).
//! - Type-mismatched literals (string literal against `status_code`,
//!   integer literal against `service_name`, etc.).
//! - Pipeline queries (`{ ... } | rate()`) — tag-discovery on a
//!   metrics-mode query is nonsensical; safest to drop.
//! - Hierarchy spanset operators (`>>`, `<<`, `~`, `>`, `<`, and their
//!   negated variants) — no metadata-scan analog.
//! - `!subtree` where the inner is not a single foldable comparison.

use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use icegate_common::schema::{
    COL_KIND, COL_NAME, COL_PARENT_SPAN_ID, COL_SERVICE_NAME, COL_SPAN_ID, COL_STATUS_CODE, COL_TRACE_ID,
};

use super::common::{ComparisonOp, FieldRef, IntrinsicField, LiteralValue, Scope};
use super::expr::TraceQLExpr;
use super::spanset::{SpanFilter, SpansetExpr};

/// Translate a parsed `TraceQL` query into an iceberg predicate fragment.
///
/// Returns [`Predicate::AlwaysTrue`] when the query contains nothing
/// pushdownable. See the module docs for the full translation rule
/// table. The result must be AND'ed with the tenant+time base predicate
/// before being passed to iceberg — that combination already happens
/// inside the metadata-scan layer.
#[must_use]
pub fn translate_query_to_predicate(expr: &TraceQLExpr) -> Predicate {
    translate_query_to_predicate_excluding(expr, None)
}

/// Like [`translate_query_to_predicate`] but additionally drops any
/// conjunct that targets `exclude_column`.
///
/// Tag-value endpoints set this to the column whose distinct values
/// they're enumerating: e.g. when the request is "give me distinct
/// `service.name` values" and the user's `q` contains
/// `resource.service.name = "x"`, including that conjunct in the
/// pushdown would shrink the result to `["x"]` — exactly the value the
/// user already typed. Dropping it lets the dropdown still show every
/// service that satisfies the *other* clauses in `q`.
#[must_use]
pub fn translate_query_to_predicate_excluding(expr: &TraceQLExpr, exclude_column: Option<&str>) -> Predicate {
    match expr {
        TraceQLExpr::Spanset(s) => translate_spanset(s, exclude_column),
        // Pipeline queries (`{ ... } | rate()`, `| count() > 3`, …) are
        // metrics-mode or aggregation-mode. Tag-discovery on those is
        // nonsensical — drop to AlwaysTrue rather than partially
        // translating a prelude spanset that may not match the user's
        // intent.
        TraceQLExpr::Pipeline(_) => Predicate::AlwaysTrue,
    }
}

fn translate_spanset(s: &SpansetExpr, exclude_column: Option<&str>) -> Predicate {
    match s {
        SpansetExpr::Selector(sel) => sel
            .filter
            .as_ref()
            .map_or(Predicate::AlwaysTrue, |f| translate_filter(f, exclude_column)),
        // Hierarchy operators (`>>`, `>`, `<<`, `<`, `~`, and their
        // negated forms) join two independent spansets in a way that
        // has no metadata-scan analog. Drop the entire expression.
        // Boolean composition between distinct spansets (`&& ` / `|| `
        // at the SpansetExpr level) is also dropped — the parser
        // encodes within-selector booleans inside `SpanFilter`, so this
        // arm only fires for cross-spanset composition.
        SpansetExpr::Op { .. } => Predicate::AlwaysTrue,
    }
}

fn translate_filter(f: &SpanFilter, exclude_column: Option<&str>) -> Predicate {
    match f {
        SpanFilter::Paren(inner) => translate_filter(inner, exclude_column),
        SpanFilter::Not(inner) => translate_negation(inner, exclude_column),
        SpanFilter::And(l, r) => combine_and(translate_filter(l, exclude_column), translate_filter(r, exclude_column)),
        SpanFilter::Or(l, r) => combine_or(translate_filter(l, exclude_column), translate_filter(r, exclude_column)),
        SpanFilter::Compare { field, op, value } => translate_compare(field, *op, value, exclude_column),
    }
}

/// Negate a sub-filter. Only `Not(Compare(...))` folds into a leaf
/// comparison flip (`Eq` ↔ `Neq`); everything else drops to
/// `AlwaysTrue`. Wrapping arbitrary subtrees in `Predicate::Not(_)` adds
/// no row-group pruning value because
/// [`crate::engine::metadata_scan::parquet_reader::row_group_can_match`]
/// treats `Not(_)` conservatively as "keep".
///
/// Parens are peeled before matching: `!(status = error)` parses as
/// `Not(Paren(Compare))` and must still fold like the bare
/// `Not(Compare)` form.
fn translate_negation(inner: &SpanFilter, exclude_column: Option<&str>) -> Predicate {
    let leaf = peel_parens(inner);
    if let SpanFilter::Compare { field, op, value } = leaf {
        if let Some(flipped) = flip_op(*op) {
            return translate_compare(field, flipped, value, exclude_column);
        }
    }
    Predicate::AlwaysTrue
}

/// Strip any number of `Paren` wrappers from a filter. Parens preserve
/// grouping for round-trip rendering but carry no semantic weight.
fn peel_parens(f: &SpanFilter) -> &SpanFilter {
    let mut cur = f;
    while let SpanFilter::Paren(inner) = cur {
        cur = inner;
    }
    cur
}

const fn flip_op(op: ComparisonOp) -> Option<ComparisonOp> {
    match op {
        ComparisonOp::Eq => Some(ComparisonOp::Neq),
        ComparisonOp::Neq => Some(ComparisonOp::Eq),
        // Range/regex flips don't help us — we drop those at the leaf
        // anyway. Returning None lets the caller collapse to AlwaysTrue.
        _ => None,
    }
}

/// AND-combine two translated predicates. `AlwaysTrue` on either side
/// collapses to the other side (intersection narrows; dropping a clause
/// only widens, which is allowed under over-approximation).
fn combine_and(l: Predicate, r: Predicate) -> Predicate {
    match (l, r) {
        (Predicate::AlwaysTrue, x) | (x, Predicate::AlwaysTrue) => x,
        (a, b) => a.and(b),
    }
}

/// OR-combine two translated predicates. **Both** sides must translate
/// to a non-`AlwaysTrue` predicate; otherwise the entire OR collapses
/// to `AlwaysTrue`. Pruning by one side of a union would discard files
/// the other side wanted to keep.
fn combine_or(l: Predicate, r: Predicate) -> Predicate {
    match (l, r) {
        (Predicate::AlwaysTrue, _) | (_, Predicate::AlwaysTrue) => Predicate::AlwaysTrue,
        (a, b) => a.or(b),
    }
}

fn translate_compare(
    field: &FieldRef,
    op: ComparisonOp,
    value: &LiteralValue,
    exclude_column: Option<&str>,
) -> Predicate {
    let Some(column) = indexed_column_for(field) else {
        return Predicate::AlwaysTrue;
    };
    // Self-reference exclusion: when the caller is enumerating the
    // distinct values of `column`, drop any conjunct that constrains
    // `column` itself. Otherwise the result collapses to whatever
    // value the user already typed, defeating the dropdown.
    if exclude_column == Some(column) {
        return Predicate::AlwaysTrue;
    }
    let Some(datum) = datum_for(field, value) else {
        return Predicate::AlwaysTrue;
    };
    let reference = Reference::new(column);
    match op {
        ComparisonOp::Eq => reference.equal_to(datum),
        ComparisonOp::Neq => reference.not_equal_to(datum),
        // Range ops on indexed columns (`duration > 5s`, `name >
        // "foo"`) are accepted by the grammar but we don't push them
        // into iceberg today. Regex (`=~`, `!~`) never pushes either —
        // iceberg's `Predicate` has no LIKE/regex analog suitable for
        // metadata pruning. Drop both to AlwaysTrue.
        ComparisonOp::Gt
        | ComparisonOp::Ge
        | ComparisonOp::Lt
        | ComparisonOp::Le
        | ComparisonOp::Re
        | ComparisonOp::Nre => Predicate::AlwaysTrue,
    }
}

/// Resolve a `TraceQL` field reference to a top-level indexed column on
/// the spans table. Returns `None` for MAP-attribute lookups, unmodelled
/// intrinsics, and event/link/parent/any scopes.
///
/// `Scope::Any` (`.foo`) drops because the implicit `(resource OR span)`
/// expansion has a non-pushdownable span side and the OR rule requires
/// both sides to translate. Event/link/parent attribute scopes drop
/// because they live in nested `LIST<STRUCT>` columns and aren't
/// modelled for metadata-scan pruning today. Intrinsics not listed
/// here (`duration`, `traceDuration`, `rootName`, `rootServiceName`,
/// `statusMessage`, `Parent`, `event:*`, `link:*`) drop too.
fn indexed_column_for(field: &FieldRef) -> Option<&'static str> {
    match field {
        FieldRef::Intrinsic(IntrinsicField::Name) => Some(COL_NAME),
        FieldRef::Intrinsic(IntrinsicField::Status) => Some(COL_STATUS_CODE),
        FieldRef::Intrinsic(IntrinsicField::Kind) => Some(COL_KIND),
        FieldRef::Intrinsic(IntrinsicField::TraceID) => Some(COL_TRACE_ID),
        FieldRef::Intrinsic(IntrinsicField::SpanID) => Some(COL_SPAN_ID),
        FieldRef::Attribute {
            scope: Scope::Resource,
            name,
        } => indexed_resource_attribute(name),
        FieldRef::Attribute {
            scope: Scope::Span,
            name,
        } => indexed_span_attribute(name),
        FieldRef::Intrinsic(_) | FieldRef::Attribute { .. } => None,
    }
}

/// Resource-scoped attributes that map to an indexed top-level column.
fn indexed_resource_attribute(name: &str) -> Option<&'static str> {
    match name {
        "service.name" => Some(COL_SERVICE_NAME),
        // `resource.X` lookups for keys that aren't promoted to a
        // top-level column live only in `resource_attributes` (MAP).
        // Dropped — over-approximation.
        _ => None,
    }
}

/// Span-scoped attributes that map to an indexed top-level column.
///
/// `trace.id`, `span.id`, `parent.span.id` — these are the OTel/W3C
/// trace-context spellings that ingest extracts to dedicated columns.
fn indexed_span_attribute(name: &str) -> Option<&'static str> {
    match name {
        "trace.id" => Some(COL_TRACE_ID),
        "span.id" => Some(COL_SPAN_ID),
        "parent.span.id" => Some(COL_PARENT_SPAN_ID),
        _ => None,
    }
}

/// Coerce a `TraceQL` literal into an iceberg [`Datum`] of the
/// appropriate physical type for the target field. Mismatched
/// combinations (e.g. integer literal against a string column) return
/// `None` so the caller drops the comparison.
fn datum_for(field: &FieldRef, value: &LiteralValue) -> Option<Datum> {
    match (field, value) {
        // Status / kind: LHS is an INT32 column, RHS may be the typed
        // enum literal (`error`, `ok`, `server`, …) or a raw integer
        // matching the OTLP code.
        (FieldRef::Intrinsic(IntrinsicField::Status), LiteralValue::Status(s)) => Some(Datum::int(s.otlp_code())),
        (FieldRef::Intrinsic(IntrinsicField::Kind), LiteralValue::Kind(k)) => Some(Datum::int(k.otlp_code())),
        (FieldRef::Intrinsic(IntrinsicField::Status | IntrinsicField::Kind), LiteralValue::Int(v)) => {
            i32::try_from(*v).ok().map(Datum::int)
        }
        // Drop string RHS against the INT enum columns: a literal like
        // `{ status = "error" }` (string, not the bare `error` keyword)
        // would otherwise fall through to the `(_, String)` arm and
        // produce a bogus `Datum::string` against an INT32 column.
        // Returning None drops the predicate so the query degrades to
        // an over-approximation rather than a planner type error.
        (FieldRef::Intrinsic(IntrinsicField::Status | IntrinsicField::Kind), LiteralValue::String(_)) => None,
        // String columns: `name`, `service_name`, `trace_id`, `span_id`,
        // `parent_span_id`. RHS must be a string.
        (_, LiteralValue::String(s)) => Some(Datum::string(s.clone())),
        // Anything else (Float, Bytes, Bool, Duration, Nil, enum-on-non-enum,
        // int-on-string-column) drops.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traceql::antlr::AntlrParser;
    use crate::traceql::parser::Parser;

    fn translate(q: &str) -> Predicate {
        let parser = AntlrParser::new();
        let expr = parser.parse(q).unwrap_or_else(|e| panic!("failed to parse {q:?}: {e:?}"));
        translate_query_to_predicate(&expr)
    }

    fn dbg(p: &Predicate) -> String {
        format!("{p:?}")
    }

    // ---------- Pushdown cases ----------

    #[test]
    fn pushes_resource_service_name_eq() {
        let p = translate(r#"{ resource.service.name = "checkout" }"#);
        let s = dbg(&p);
        assert!(s.contains("service_name"), "missing column: {s}");
        assert!(s.contains("checkout"), "missing literal: {s}");
    }

    #[test]
    fn drops_resource_cloud_account_id_eq() {
        // `cloud.account.id` no longer maps to a dedicated top-level
        // column (GH-140) — the resource attribute now lives only in
        // the `resource_attributes` MAP, which iceberg cannot prune on.
        // The translator must therefore drop the conjunct (AlwaysTrue),
        // not invent a phantom `cloud_account_id` column.
        let p = translate(r#"{ resource.cloud.account.id = "123" }"#);
        let s = dbg(&p);
        assert!(!s.contains("cloud_account_id"), "must not push: {s}");
        assert!(matches!(p, Predicate::AlwaysTrue), "expected AlwaysTrue, got: {s}");
    }

    #[test]
    fn pushes_status_eq_error_as_int_code() {
        let p = translate("{ status = error }");
        let s = dbg(&p);
        assert!(s.contains("status_code"), "missing column: {s}");
        // OTLP code for Error is 2 — see traceql::common::StatusValue.
        assert!(s.contains('2'), "missing OTLP code: {s}");
    }

    #[test]
    fn pushes_status_neq_ok() {
        let p = translate("{ status != ok }");
        let s = dbg(&p);
        assert!(s.contains("status_code"));
        // OTLP code for Ok is 1.
        assert!(s.contains('1'));
    }

    #[test]
    fn pushes_kind_eq_server() {
        let p = translate("{ kind = server }");
        let s = dbg(&p);
        assert!(s.contains("kind"), "missing column: {s}");
        // SpanKind::Server is 2.
        assert!(s.contains('2'));
    }

    #[test]
    fn pushes_intrinsic_name_eq() {
        let p = translate(r#"{ name = "GET /api" }"#);
        let s = dbg(&p);
        assert!(s.contains("name"));
        assert!(s.contains("GET /api"));
    }

    #[test]
    fn pushes_intrinsic_traceid_eq() {
        let p = translate(r#"{ traceID = "abcd" }"#);
        let s = dbg(&p);
        assert!(s.contains("trace_id"));
        assert!(s.contains("abcd"));
    }

    #[test]
    fn pushes_span_trace_id_attribute_spelling() {
        let p = translate(r#"{ span.trace.id = "abcd" }"#);
        let s = dbg(&p);
        assert!(s.contains("trace_id"), "missing column: {s}");
    }

    #[test]
    fn pushes_and_of_two_pushdownable() {
        let p = translate("{ status = error && kind = server }");
        let s = dbg(&p);
        assert!(s.contains("status_code"));
        assert!(s.contains("kind"));
    }

    #[test]
    fn and_drops_one_side_when_other_is_map_attribute() {
        // Only the status side survives; the http.method side becomes
        // AlwaysTrue and the AND collapses to just the surviving side.
        let p = translate(r#"{ status = error && span.http.method = "GET" }"#);
        let s = dbg(&p);
        assert!(s.contains("status_code"), "status side should survive: {s}");
        assert!(!s.contains("http.method"), "map-attr should be dropped: {s}");
    }

    #[test]
    fn pushes_or_of_two_pushdownable() {
        let p = translate("{ status = error || kind = server }");
        let s = dbg(&p);
        assert!(s.contains("status_code"));
        assert!(s.contains("kind"));
    }

    // ---------- Drop cases ----------

    #[test]
    fn drops_map_attribute_predicate() {
        let p = translate(r#"{ span.http.method = "GET" }"#);
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn drops_status_string_literal_against_int_column() {
        // `status = "error"` is a *string* literal, not the bare `error`
        // keyword. Pushing it as a string Datum against the INT32
        // status_code column would be a planner type error, so the
        // predicate must be dropped to AlwaysTrue.
        let p = translate(r#"{ status = "error" }"#);
        assert!(
            matches!(p, Predicate::AlwaysTrue),
            "expected AlwaysTrue, got: {}",
            dbg(&p)
        );
    }

    #[test]
    fn drops_kind_string_literal_against_int_column() {
        // Same as above but for `kind`.
        let p = translate(r#"{ kind = "server" }"#);
        assert!(
            matches!(p, Predicate::AlwaysTrue),
            "expected AlwaysTrue, got: {}",
            dbg(&p)
        );
    }

    #[test]
    fn drops_unscoped_attribute_shorthand() {
        // `.service.name = "x"` — Scope::Any. Even though the resource
        // side would push, the span side wouldn't, so the implicit OR
        // collapses to AlwaysTrue.
        let p = translate(r#"{ .service.name = "x" }"#);
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn drops_regex_match() {
        let p = translate(r#"{ name =~ "GET.*" }"#);
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn drops_duration_range() {
        let p = translate("{ duration > 5s }");
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn drops_or_when_one_side_non_pushdownable() {
        let p = translate(r#"{ status = error || span.http.method = "GET" }"#);
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn drops_unmodelled_intrinsic_status_message() {
        let p = translate(r#"{ statusMessage = "ok" }"#);
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn drops_empty_selector() {
        let p = translate("{}");
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn drops_pipeline_query() {
        // Pipeline queries are tag-discovery-nonsensical; safest to
        // drop the whole thing rather than partially translate the
        // prelude.
        let p = translate("{ status = error } | rate()");
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn drops_negation_of_complex_subtree() {
        // `!(A && B)` doesn't fold to a single leaf flip — drop.
        let p = translate("{ !(status = error && kind = server) }");
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn folds_negation_of_leaf_eq() {
        let p = translate("{ !(status = error) }");
        let s = dbg(&p);
        assert!(s.contains("status_code"), "should still target status_code: {s}");
    }

    // ---------- Self-reference exclusion ----------

    fn translate_excluding(q: &str, exclude_column: Option<&str>) -> Predicate {
        let parser = AntlrParser::new();
        let expr = parser.parse(q).unwrap_or_else(|e| panic!("failed to parse {q:?}: {e:?}"));
        translate_query_to_predicate_excluding(&expr, exclude_column)
    }

    #[test]
    fn excludes_self_clause_for_service_name() {
        // Asking for distinct service.name values: the service.name=x
        // clause must NOT be pushed down (otherwise the dropdown
        // collapses to ["x"]).
        let p = translate_excluding(r#"{ resource.service.name = "icegate-ingest" }"#, Some("service_name"));
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn excludes_self_clause_for_status() {
        let p = translate_excluding("{ status = error }", Some("status_code"));
        assert!(matches!(p, Predicate::AlwaysTrue));
    }

    #[test]
    fn keeps_other_clauses_when_self_excluded() {
        // `service.name = "x" && status = error` enumerating service.name:
        // drop the service.name clause, keep status_code = 2.
        let p = translate_excluding(
            r#"{ resource.service.name = "x" && status = error }"#,
            Some("service_name"),
        );
        let s = dbg(&p);
        assert!(s.contains("status_code"), "status side should survive: {s}");
        assert!(!s.contains("service_name"), "service_name side must drop: {s}");
    }

    #[test]
    fn no_exclude_means_normal_pushdown() {
        // Same input but without exclude_column behaves exactly like
        // the non-excluding variant.
        let p = translate_excluding(r#"{ resource.service.name = "x" }"#, None);
        let s = dbg(&p);
        assert!(s.contains("service_name"));
        assert!(s.contains('x'));
    }
}
