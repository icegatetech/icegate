//! Lowering of the Query DSL AST to a DataFusion predicate over one index.
//!
//! A query is lowered once per index. A field typed differently across indices is therefore
//! read with each index's own type, and a field one index lacks matches nothing there. The
//! predicate references only the columns of the tenant-scoped scan, so it can never widen the
//! tenant scope: isolation is the scan's, not the predicate's.

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions::string::expr_fn::{lower, starts_with};
use datafusion::functions_nested::expr_fn::cardinality;
use datafusion::logical_expr::expr::Like;
use datafusion::logical_expr::{Expr, cast, in_list, lit, not, when};
use datafusion::prelude::ident;
use datafusion::scalar::ScalarValue;
use icegate_common::schema::{COL_SPAN_ID, COL_TIMESTAMP, COL_TRACE_ID};
use serde_json::Value;

use crate::engine::fixed_binary::decode_fixed_binary_literal;
use crate::opensearch::catalog::{FieldType, FieldValue, IndexEntry, ResolvedField, TenantSession, native_column_type};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::doc_id::{
    DocumentIdPredicate, decode_document_id, document_row_hash_udf, is_instant_type,
};
use crate::query_dsl::ast::{
    BoolQuery, ExistsQuery, IdsQuery, PrefixQuery, Query, RangeBound, RangeQuery, TermQuery, TermsQuery, WildcardQuery,
};
use crate::query_dsl::date_math::{BoundSide, resolve_date_bound};
use crate::query_dsl::literal::{
    IntegerBound, LiteralError, NumericTerm, parse_boolean_literal, parse_java_double, parse_numeric_term,
    resolve_date_term, resolve_integer_bound, step_exclusive_double,
};

/// What a query is lowered against: one index of one tenant-scoped session, at the request
/// instant.
#[derive(Clone, Copy)]
pub(crate) struct LoweringScope<'a> {
    /// The tenant-scoped session whose tables the predicate reads.
    pub(crate) session: &'a TenantSession,
    /// The index the predicate filters.
    pub(crate) entry: &'a IndexEntry,
    /// The request instant that date math resolves against.
    pub(crate) now: DateTime<Utc>,
}

/// Lower `query` to a predicate over `scope.entry`'s tenant-scoped scan.
///
/// A leaf on a name the index does not resolve matches nothing. `must_not p` keeps the rows
/// where `p` is NULL (`NOT coalesce(p, false)`).
///
/// # Errors
///
/// Returns the 400 `search_phase_execution_exception` with a `query_shard_exception` root cause
/// for a literal of the wrong form for its field (an invalid hex id; a malformed, non-finite or
/// out-of-range number; a boolean other than `true` or `false`; text that is no date) and for
/// `prefix` or `wildcard` on an id, numeric, date or boolean field; `parsing_exception` for
/// `nested` on a data index; `illegal_argument_exception` for the leaves the data path does not
/// lower yet; `500` for a leaf on a column no `OpenSearch` type represents or for an `ids` value
/// of an index with hashed ids and no timestamp column; and the error the index's table failed
/// to open with.
pub(crate) fn lower_query(query: &Query, scope: &LoweringScope<'_>) -> Result<Expr, OpenSearchError> {
    match query {
        Query::MatchAll => Ok(lit(true)),
        Query::MatchNone => Ok(lit(false)),
        Query::Bool(bool_query) => lower_bool(bool_query, scope),
        Query::ConstantScore(inner) => lower_query(inner, scope),
        Query::Term(term) => lower_term(term, scope),
        Query::Terms(terms) => lower_terms(terms, scope),
        Query::Range(range) => lower_range(range, scope),
        Query::Exists(exists) => lower_exists(exists, scope),
        Query::Prefix(prefix) => lower_prefix(prefix, scope),
        Query::Wildcard(wildcard) => lower_wildcard(wildcard, scope),
        Query::Ids(ids) => lower_ids(ids, scope),
        Query::Nested(_) => Err(OpenSearchError::parsing(
            "[nested] queries are supported on system indices only",
        )),
        Query::Match(_)
        | Query::MatchPhrase(_)
        | Query::MatchPhrasePrefix(_)
        | Query::MultiMatch(_)
        | Query::SimpleQueryString(_) => Err(OpenSearchError::illegal_argument(format!(
            "full-text queries on data index [{}] are not supported yet",
            scope.entry.name()
        ))),
    }
}

/// `must` and `filter` all hold; `should` holds at least `minimum_should_match` times (default
/// 1 when there is neither `must` nor `filter`, else 0); no `must_not` holds.
fn lower_bool(query: &BoolQuery, scope: &LoweringScope<'_>) -> Result<Expr, OpenSearchError> {
    let mut conjuncts = Vec::with_capacity(query.must.len() + query.filter.len() + query.must_not.len() + 1);
    for clause in query.must.iter().chain(&query.filter) {
        conjuncts.push(lower_query(clause, scope)?);
    }
    let should_clauses = query
        .should
        .iter()
        .map(|clause| lower_query(clause, scope))
        .collect::<Result<Vec<_>, _>>()?;
    let is_should_required = query.must.is_empty() && query.filter.is_empty() && !should_clauses.is_empty();
    let minimum_should_count = query.minimum_should_match.unwrap_or_else(|| u32::from(is_should_required));
    if minimum_should_count > 0 {
        conjuncts.push(require_should_matches(should_clauses, minimum_should_count)?);
    }
    for clause in &query.must_not {
        conjuncts.push(not(coalesce(vec![lower_query(clause, scope)?, lit(false)])));
    }
    Ok(conjuncts.into_iter().reduce(Expr::and).unwrap_or_else(|| lit(true)))
}

/// At least `minimum_should_count` of `should_clauses` hold, a NULL clause counting as not
/// holding. One required clause is a plain disjunction, which keeps pushdown; more count the
/// matching clauses, and more than there are clauses match nothing.
fn require_should_matches(should_clauses: Vec<Expr>, minimum_should_count: u32) -> Result<Expr, OpenSearchError> {
    let required_count = usize::try_from(minimum_should_count).unwrap_or(usize::MAX);
    if required_count > should_clauses.len() {
        return Ok(lit(false));
    }
    if required_count == 1 {
        return Ok(should_clauses.into_iter().reduce(Expr::or).unwrap_or_else(|| lit(false)));
    }
    let indicators = should_clauses
        .into_iter()
        .map(|clause| {
            when(coalesce(vec![clause, lit(false)]), lit(1_i64))
                .otherwise(lit(0_i64))
                .map_err(|error| OpenSearchError::internal(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let matched_count = indicators.into_iter().reduce(|sum, indicator| sum + indicator);
    Ok(matched_count.map_or_else(|| lit(false), |sum| sum.gt_eq(lit(i64::from(minimum_should_count)))))
}

/// Resolve the field `name` a query leaf reads in `scope.entry`.
///
/// `None` means the index has no field of that name, and the leaf matches nothing there. A
/// column the index has but no `OpenSearch` type represents is the 500 of
/// [`native_column_type`] instead, so a leaf on it never answers as if the column were absent.
///
/// # Errors
///
/// Returns that 500, and the error the index's table failed to open with.
fn resolve_leaf_field(scope: &LoweringScope<'_>, name: &str) -> Result<Option<ResolvedField>, OpenSearchError> {
    if let Some(field) = scope.session.resolve_field(scope.entry, name) {
        return Ok(Some(field));
    }
    // `resolve_field` resolves every column whose Arrow type an `OpenSearch` type represents, so
    // a column it leaves unresolved is one no type represents.
    let Some(data_type) = scope.session.column_data_type(scope.entry, name)? else {
        return Ok(None);
    };
    Err(native_column_type(&Field::new(name, data_type, true))
        .err()
        .unwrap_or_else(|| OpenSearchError::internal(format!("column `{name}` resolved to no field"))))
}

/// The text a `term`, `terms` or range value denotes: a string as is, a number or a boolean
/// in its JSON spelling.
fn render_literal_text(value: &Value, field: &str) -> Result<String, OpenSearchError> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(flag) => Ok(flag.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => Err(OpenSearchError::parsing(format!(
            "the value of [{field}] must be a string, a number or a boolean"
        ))),
    }
}

/// A hex-id literal of `width` bytes, or the query-shard 400 naming the field.
fn decode_hex_literal(
    text: &str,
    width: i32,
    field: &ResolvedField,
    scope: &LoweringScope<'_>,
) -> Result<Expr, OpenSearchError> {
    decode_fixed_binary_literal(text, &DataType::FixedSizeBinary(width))
        .map(lit)
        .map_err(|error| {
            OpenSearchError::query_shard(
                scope.entry.name(),
                &format!(
                    "failed to create query: [{}] holds {width}-byte ids: {error}",
                    field.name
                ),
            )
        })
}

/// The 400 for a leaf this field type does not lower yet.
fn refuse_unsupported_leaf(field: &ResolvedField, leaf: &str, scope: &LoweringScope<'_>) -> OpenSearchError {
    OpenSearchError::illegal_argument(format!(
        "[{leaf}] on field [{}] of type [{}] in data index [{}] is not supported yet",
        field.name,
        field.field_type.name(),
        scope.entry.name()
    ))
}

/// The final query-shard 400 for a term pattern (`prefix`, `wildcard`) on a field that only
/// matches whole parsed values.
fn refuse_term_pattern(field: &ResolvedField, leaf: &str, scope: &LoweringScope<'_>) -> OpenSearchError {
    OpenSearchError::query_shard(
        scope.entry.name(),
        &format!(
            "failed to create query: [{leaf}] queries are not supported on field [{}], which holds {} values",
            field.name,
            match field.value {
                FieldValue::HexId { .. } => "binary id",
                FieldValue::Column { .. } | FieldValue::MapKey { .. } => field.field_type.name(),
            }
        ),
    )
}

/// A `term`: an exact keyword, hex-id, numeric or boolean equality, or the range a date term
/// covers; a name the index lacks, or an `object`, matches nothing.
fn lower_term(query: &TermQuery, scope: &LoweringScope<'_>) -> Result<Expr, OpenSearchError> {
    let Some(field) = resolve_leaf_field(scope, &query.field)? else {
        return Ok(lit(false));
    };
    let text = render_literal_text(&query.value, &query.field)?;
    let read_expr = field.value.build_read_expr();
    match (field.field_type, &field.value) {
        (FieldType::Keyword, FieldValue::HexId { width, .. }) => {
            Ok(read_expr.eq(decode_hex_literal(&text, *width, &field, scope)?))
        }
        (FieldType::Keyword, FieldValue::Column { .. } | FieldValue::MapKey { .. }) => Ok(if query.case_insensitive {
            lower(read_expr).eq(lower(lit(text)))
        } else {
            read_expr.eq(lit(text))
        }),
        (FieldType::Integer | FieldType::Long | FieldType::Double, _) => {
            let (operand, literal_type) = build_numeric_operand(read_expr, &field, scope)?;
            Ok(build_numeric_literal(&text, &literal_type, &field, scope)?
                .map_or_else(|| lit(false), |literal| operand.eq(literal)))
        }
        (FieldType::Boolean, _) => Ok(read_expr.eq(lit(parse_boolean_value(&text, &field, scope)?))),
        (FieldType::Date, _) => {
            let data_type = read_typed_column_type(&field, scope)?;
            lower_date_term(read_expr, &text, &data_type, &field, scope)
        }
        (FieldType::Object, _) => Ok(lit(false)),
        (FieldType::Text, _) => Err(refuse_unsupported_leaf(&field, "term", scope)),
    }
}

/// A `terms`: the value is one of the listed values. Every listed value is read before the
/// leaf answers, so a malformed one is the 400 wherever it stands; a list of no matchable value
/// matches nothing.
fn lower_terms(query: &TermsQuery, scope: &LoweringScope<'_>) -> Result<Expr, OpenSearchError> {
    let Some(field) = resolve_leaf_field(scope, &query.field)? else {
        return Ok(lit(false));
    };
    if query.values.is_empty() {
        return Ok(lit(false));
    }
    let texts = query
        .values
        .iter()
        .map(|value| render_literal_text(value, &query.field))
        .collect::<Result<Vec<_>, _>>()?;
    let read_expr = field.value.build_read_expr();
    match (field.field_type, &field.value) {
        (FieldType::Keyword, FieldValue::HexId { width, .. }) => {
            let literals = texts
                .iter()
                .map(|text| decode_hex_literal(text, *width, &field, scope))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(in_list(read_expr, literals, false))
        }
        (FieldType::Keyword, FieldValue::Column { .. } | FieldValue::MapKey { .. }) => {
            Ok(in_list(read_expr, texts.into_iter().map(lit).collect(), false))
        }
        (FieldType::Integer | FieldType::Long | FieldType::Double, _) => {
            let (operand, literal_type) = build_numeric_operand(read_expr, &field, scope)?;
            let mut literals = Vec::with_capacity(texts.len());
            for text in &texts {
                if let Some(literal) = build_numeric_literal(text, &literal_type, &field, scope)? {
                    literals.push(literal);
                }
            }
            Ok(if literals.is_empty() {
                lit(false)
            } else {
                in_list(operand, literals, false)
            })
        }
        (FieldType::Boolean, _) => {
            let literals = texts
                .iter()
                .map(|text| parse_boolean_value(text, &field, scope).map(lit))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(in_list(read_expr, literals, false))
        }
        (FieldType::Date, _) => {
            let data_type = read_typed_column_type(&field, scope)?;
            let covered_ranges = texts
                .iter()
                .map(|text| lower_date_term(read_expr.clone(), text, &data_type, &field, scope))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(covered_ranges.into_iter().reduce(Expr::or).unwrap_or_else(|| lit(false)))
        }
        (FieldType::Object, _) => Ok(lit(false)),
        (FieldType::Text, _) => Err(refuse_unsupported_leaf(&field, "terms", scope)),
    }
}

/// Join the bound predicates; a range with no bound matches every row holding the field.
fn conjoin_bounds(read_expr: Expr, bounds: Vec<Expr>) -> Expr {
    bounds.into_iter().reduce(Expr::and).unwrap_or_else(|| read_expr.is_not_null())
}

/// A `range`: keyword bounds compare lexicographically, hex-id bounds byte-wise, numeric and
/// boolean bounds by their typed rules, and date bounds as typed timestamps of the column's unit.
fn lower_range(query: &RangeQuery, scope: &LoweringScope<'_>) -> Result<Expr, OpenSearchError> {
    let Some(field) = resolve_leaf_field(scope, &query.field)? else {
        return Ok(lit(false));
    };
    let read_expr = field.value.build_read_expr();
    match (field.field_type, &field.value) {
        (FieldType::Keyword, value) => {
            let build_bound_literal = |bound: &RangeBound| -> Result<Expr, OpenSearchError> {
                let text = render_literal_text(&bound.value, &query.field)?;
                match value {
                    FieldValue::HexId { width, .. } => decode_hex_literal(&text, *width, &field, scope),
                    FieldValue::Column { .. } | FieldValue::MapKey { .. } => Ok(lit(text)),
                }
            };
            let mut bounds = Vec::with_capacity(2);
            if let Some(bound) = &query.lower {
                let literal = build_bound_literal(bound)?;
                bounds.push(if bound.is_inclusive {
                    read_expr.clone().gt_eq(literal)
                } else {
                    read_expr.clone().gt(literal)
                });
            }
            if let Some(bound) = &query.upper {
                let literal = build_bound_literal(bound)?;
                bounds.push(if bound.is_inclusive {
                    read_expr.clone().lt_eq(literal)
                } else {
                    read_expr.clone().lt(literal)
                });
            }
            Ok(conjoin_bounds(read_expr, bounds))
        }
        (FieldType::Date, FieldValue::Column { .. } | FieldValue::HexId { .. }) => {
            lower_date_range(query, read_expr, &field, scope)
        }
        (FieldType::Integer | FieldType::Long, _) => lower_integer_range(query, read_expr, &field, scope),
        (FieldType::Double, _) => lower_double_range(query, read_expr, &field, scope),
        (FieldType::Boolean, _) => lower_boolean_range(query, read_expr, &field, scope),
        (FieldType::Object | FieldType::Date, _) => Ok(lit(false)),
        (FieldType::Text, _) => Err(refuse_unsupported_leaf(&field, "range", scope)),
    }
}

/// The side a range bound is on.
const fn classify_bound_side(is_lower: bool, bound: &RangeBound) -> BoundSide {
    match (is_lower, bound.is_inclusive) {
        (true, true) => BoundSide::LowerInclusive,
        (true, false) => BoundSide::LowerExclusive,
        (false, true) => BoundSide::UpperInclusive,
        (false, false) => BoundSide::UpperExclusive,
    }
}

/// The present bounds of `query`, each with whether it is the lower one.
fn list_present_bounds(query: &RangeQuery) -> impl Iterator<Item = (bool, &RangeBound)> {
    [(true, query.lower.as_ref()), (false, query.upper.as_ref())]
        .into_iter()
        .filter_map(|(is_lower, bound)| bound.map(|present_bound| (is_lower, present_bound)))
}

/// The Arrow type of the column a typed (numeric, boolean or date) field reads.
///
/// # Errors
///
/// Returns `500` when the field is no column of the index's table, and the error the table
/// failed to open with.
fn read_typed_column_type(field: &ResolvedField, scope: &LoweringScope<'_>) -> Result<DataType, OpenSearchError> {
    let (FieldValue::Column { column } | FieldValue::HexId { column, .. }) = &field.value else {
        return Err(OpenSearchError::internal(format!(
            "typed field [{}] is not a column",
            field.name
        )));
    };
    scope
        .session
        .column_data_type(scope.entry, column)?
        .ok_or_else(|| OpenSearchError::internal(format!("column `{column}` vanished from its table")))
}

/// The query-shard 400 for `text`, which is no value of `field`.
fn refuse_literal(
    field: &ResolvedField,
    text: &str,
    error: LiteralError,
    scope: &LoweringScope<'_>,
) -> OpenSearchError {
    OpenSearchError::query_shard(
        scope.entry.name(),
        &format!(
            "failed to create query: [{text}] is not a [{}] value of field [{}]: {error}",
            field.field_type.name(),
            field.name
        ),
    )
}

/// The expression a numeric comparison reads, and the Arrow type of the literals it compares
/// with.
///
/// An integer field reads its column as stored. A `double` field stored in another type (a
/// decimal or a 32-bit float) reads its column cast to a 64-bit double: a double literal cast to
/// the column's type would round, so a stepped exclusive bound or an inexact term could land
/// back on a stored value it must exclude. A filter with that cast is evaluated above an
/// Iceberg-only scan, which never receives it (`engine::provider::iceberg_only`).
fn build_numeric_operand(
    read_expr: Expr,
    field: &ResolvedField,
    scope: &LoweringScope<'_>,
) -> Result<(Expr, DataType), OpenSearchError> {
    let column_type = read_typed_column_type(field, scope)?;
    if field.field_type == FieldType::Double && column_type != DataType::Float64 {
        return Ok((cast(read_expr, DataType::Float64), DataType::Float64));
    }
    Ok((read_expr, column_type))
}

/// The literal of `literal_type` a numeric term compares with; `None` when the term matches no
/// row (a fraction on an integer field).
///
/// # Errors
///
/// Returns the query-shard 400 for a malformed, non-finite or out-of-range term, and `500` when
/// a range-checked value does not convert to `literal_type`.
fn build_numeric_literal(
    text: &str,
    literal_type: &DataType,
    field: &ResolvedField,
    scope: &LoweringScope<'_>,
) -> Result<Option<Expr>, OpenSearchError> {
    let scalar =
        match parse_numeric_term(text, field.field_type).map_err(|error| refuse_literal(field, text, error, scope))? {
            NumericTerm::Integer(value) => ScalarValue::Int64(Some(value)),
            NumericTerm::Double(value) => ScalarValue::Float64(Some(value)),
            NumericTerm::NoMatch => return Ok(None),
        };
    // The term is range-checked for the field's type, which matches `literal_type`, so a failed
    // conversion is a broken invariant, not a client error.
    scalar
        .cast_to(literal_type)
        .map(|typed_scalar| Some(lit(typed_scalar)))
        .map_err(|error| OpenSearchError::internal(format!("cannot type literal [{text}] as {literal_type}: {error}")))
}

/// `text` as a boolean value of `field`, or the query-shard 400 naming it.
fn parse_boolean_value(text: &str, field: &ResolvedField, scope: &LoweringScope<'_>) -> Result<bool, OpenSearchError> {
    parse_boolean_literal(text).ok_or_else(|| refuse_literal(field, text, LiteralError::Malformed, scope))
}

/// A date term: the rows inside the half-open range the term covers, compared as typed
/// timestamps of `data_type`, the field's column type.
fn lower_date_term(
    read_expr: Expr,
    text: &str,
    data_type: &DataType,
    field: &ResolvedField,
    scope: &LoweringScope<'_>,
) -> Result<Expr, OpenSearchError> {
    let (start, end) =
        resolve_date_term(text, None, scope.now).map_err(|error| refuse_literal(field, text, error, scope))?;
    Ok(read_expr
        .clone()
        .gt_eq(build_date_literal(start, data_type)?)
        .and(read_expr.lt(build_date_literal(end, data_type)?)))
}

/// An integer range: each side rounds toward the inside of the range and compares inclusively.
/// Both sides are read before the range answers, so a malformed bound is the 400 even beside a
/// side that admits no value.
fn lower_integer_range(
    query: &RangeQuery,
    read_expr: Expr,
    field: &ResolvedField,
    scope: &LoweringScope<'_>,
) -> Result<Expr, OpenSearchError> {
    let data_type = read_typed_column_type(field, scope)?;
    let mut resolved_bounds = Vec::with_capacity(2);
    for (is_lower, bound) in list_present_bounds(query) {
        let text = render_literal_text(&bound.value, &query.field)?;
        let integer_bound = resolve_integer_bound(&text, field.field_type, classify_bound_side(is_lower, bound))
            .map_err(|error| refuse_literal(field, &text, error, scope))?;
        resolved_bounds.push((is_lower, integer_bound));
    }
    if resolved_bounds
        .iter()
        .any(|(_, integer_bound)| *integer_bound == IntegerBound::Empty)
    {
        return Ok(lit(false));
    }
    let mut bounds = Vec::with_capacity(resolved_bounds.len());
    for (is_lower, integer_bound) in resolved_bounds {
        let IntegerBound::Inclusive(value) = integer_bound else {
            continue;
        };
        // Each bound is rounded into the field's range, the range of its column type, so a
        // failed conversion is a broken invariant, not a client error.
        let literal = lit(ScalarValue::Int64(Some(value)).cast_to(&data_type).map_err(|error| {
            OpenSearchError::internal(format!("cannot type literal [{value}] as {data_type}: {error}"))
        })?);
        bounds.push(if is_lower {
            read_expr.clone().gt_eq(literal)
        } else {
            read_expr.clone().lt_eq(literal)
        });
    }
    Ok(conjoin_bounds(read_expr, bounds))
}

/// A double range: an exclusive bound steps to the adjacent double in IEEE totalOrder, so every
/// comparison is inclusive and statistics that treat `-0.0` and `+0.0` alike cannot prune a
/// matching file. Bounds are not finiteness-checked.
fn lower_double_range(
    query: &RangeQuery,
    read_expr: Expr,
    field: &ResolvedField,
    scope: &LoweringScope<'_>,
) -> Result<Expr, OpenSearchError> {
    let (operand, _) = build_numeric_operand(read_expr, field, scope)?;
    let mut bounds = Vec::with_capacity(2);
    for (is_lower, bound) in list_present_bounds(query) {
        let text = render_literal_text(&bound.value, &query.field)?;
        let value =
            parse_java_double(&text).ok_or_else(|| refuse_literal(field, &text, LiteralError::Malformed, scope))?;
        let literal = lit(step_exclusive_double(value, classify_bound_side(is_lower, bound)));
        bounds.push(if is_lower {
            operand.clone().gt_eq(literal)
        } else {
            operand.clone().lt_eq(literal)
        });
    }
    Ok(conjoin_bounds(operand, bounds))
}

/// A boolean range over `false < true`.
fn lower_boolean_range(
    query: &RangeQuery,
    read_expr: Expr,
    field: &ResolvedField,
    scope: &LoweringScope<'_>,
) -> Result<Expr, OpenSearchError> {
    let mut is_false_admitted = true;
    let mut is_true_admitted = true;
    for (is_lower, bound) in list_present_bounds(query) {
        let limit = parse_boolean_value(&render_literal_text(&bound.value, &query.field)?, field, scope)?;
        let side = classify_bound_side(is_lower, bound);
        is_false_admitted &= check_boolean_bound(false, limit, side);
        is_true_admitted &= check_boolean_bound(true, limit, side);
    }
    Ok(match (is_false_admitted, is_true_admitted) {
        (false, false) => lit(false),
        (true, false) => read_expr.eq(lit(false)),
        (false, true) => read_expr.eq(lit(true)),
        (true, true) => read_expr.is_not_null(),
    })
}

/// Whether `value` lies on the admitted `side` of the boolean bound `limit`, over `false < true`.
fn check_boolean_bound(value: bool, limit: bool, side: BoundSide) -> bool {
    let order = value.cmp(&limit);
    match side {
        BoundSide::LowerInclusive => order.is_ge(),
        BoundSide::LowerExclusive => order.is_gt(),
        BoundSide::UpperInclusive => order.is_le(),
        BoundSide::UpperExclusive => order.is_lt(),
    }
}

/// A date range: each bound resolves to a half-open microsecond boundary (lower `>=`, upper
/// `<`), compared as a typed timestamp of the column's own unit.
fn lower_date_range(
    query: &RangeQuery,
    read_expr: Expr,
    field: &ResolvedField,
    scope: &LoweringScope<'_>,
) -> Result<Expr, OpenSearchError> {
    let data_type = read_typed_column_type(field, scope)?;
    let mut bounds = Vec::with_capacity(2);
    for (is_lower, bound) in list_present_bounds(query) {
        let micros = resolve_date_bound(
            &render_literal_text(&bound.value, &query.field)?,
            query.format,
            query.time_zone.as_ref(),
            scope.now,
            classify_bound_side(is_lower, bound),
        )?;
        let literal = build_date_literal(micros, &data_type)?;
        bounds.push(if is_lower {
            read_expr.clone().gt_eq(literal)
        } else {
            read_expr.clone().lt(literal)
        });
    }
    Ok(conjoin_bounds(read_expr, bounds))
}

/// The literal of a half-open boundary `micros` in the unit of a date column of `data_type`.
///
/// A coarser unit rounds the boundary up: for an integer `x` of unit `u`, `x·u ≥ b` holds
/// exactly when `x ≥ ⌈b/u⌉`, and `x·u < b` exactly when `x < ⌈b/u⌉`, so both sides of a
/// half-open range keep their meaning.
///
/// # Errors
///
/// Returns `500` when `data_type` is not a date type.
pub(crate) fn build_date_literal(micros: i64, data_type: &DataType) -> Result<Expr, OpenSearchError> {
    let divide_rounding_up = |divisor: i64| micros.div_euclid(divisor) + i64::from(micros.rem_euclid(divisor) != 0);
    let scalar = match data_type {
        DataType::Timestamp(TimeUnit::Microsecond, zone) => {
            ScalarValue::TimestampMicrosecond(Some(micros), zone.clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, zone) => {
            ScalarValue::TimestampNanosecond(Some(micros.saturating_mul(1_000)), zone.clone())
        }
        DataType::Timestamp(TimeUnit::Millisecond, zone) => {
            ScalarValue::TimestampMillisecond(Some(divide_rounding_up(1_000)), zone.clone())
        }
        DataType::Timestamp(TimeUnit::Second, zone) => {
            ScalarValue::TimestampSecond(Some(divide_rounding_up(1_000_000)), zone.clone())
        }
        DataType::Date64 => ScalarValue::Date64(Some(divide_rounding_up(1_000))),
        // Any `i64` of microseconds is at most about 1.07e8 days from the epoch, far inside
        // `i32`, so the conversion cannot fail.
        DataType::Date32 => ScalarValue::Date32(Some(
            i32::try_from(divide_rounding_up(86_400_000_000))
                .map_err(|_| OpenSearchError::internal("a microsecond boundary overflowed Date32 days"))?,
        )),
        other => {
            return Err(OpenSearchError::internal(format!(
                "a date column has the non-date type {other}"
            )));
        }
    };
    Ok(lit(scalar))
}

/// What every hashed id of one index lowers with, read once per `ids` query.
struct HashedIdColumns {
    /// The type of the index's timestamp column, the unit its microsecond range is written in.
    timestamp_type: DataType,
    /// Whether the timestamp column may be NULL; such a row's `_id` carries microsecond `0`.
    is_timestamp_nullable: bool,
    /// The canonical row hash over every column of the index, in schema order.
    row_hash: Expr,
}

impl HashedIdColumns {
    /// Read the timestamp column and the columns of `scope`'s index.
    ///
    /// # Errors
    ///
    /// Returns the error the index's table failed to open with, and `500` when the index has no
    /// timestamp column.
    fn from_scope(scope: &LoweringScope<'_>) -> Result<Self, OpenSearchError> {
        let schema = scope.session.table_schema(scope.entry)?;
        let timestamp_field = schema
            .field_with_name(COL_TIMESTAMP)
            .map_err(|_| OpenSearchError::internal("a hashed-id index has no timestamp column"))?;
        let column_names: Vec<String> = schema.fields().iter().map(|field| field.name().clone()).collect();
        let columns = column_names.iter().map(ident).collect();
        Ok(Self {
            timestamp_type: timestamp_field.data_type().clone(),
            is_timestamp_nullable: timestamp_field.is_nullable(),
            row_hash: document_row_hash_udf(column_names).call(columns),
        })
    }

    /// The rows whose `_id` carries `timestamp_micros` and whose canonical hash is `row_hash`:
    /// every row whose own `_id` this is, whatever the type of its timestamp column.
    ///
    /// # Errors
    ///
    /// Returns `500` when a boundary has no literal in the timestamp column's type.
    fn build_predicate(&self, timestamp_micros: i64, row_hash: u64) -> Result<Expr, OpenSearchError> {
        let row_hash_match = self.row_hash.clone().eq(lit(ScalarValue::UInt64(Some(row_hash))));
        if !is_instant_type(&self.timestamp_type) {
            // Such a column gives every row's `_id` microsecond 0, so no other id is of this
            // index's form.
            return Ok(if timestamp_micros == 0 {
                row_hash_match
            } else {
                lit(false)
            });
        }
        // The microsecond is the half-open range `[µs, µs + 1)` in the column's own unit, which
        // rounds both ends up, so it holds the value a row's `_id` microsecond was floored from,
        // whatever the unit, and both scans prune with it.
        let mut instant = ident(COL_TIMESTAMP)
            .gt_eq(build_date_literal(timestamp_micros, &self.timestamp_type)?)
            .and(ident(COL_TIMESTAMP).lt(build_date_literal(
                timestamp_micros.saturating_add(1),
                &self.timestamp_type,
            )?));
        if timestamp_micros == 0 && self.is_timestamp_nullable {
            // A row without a timestamp carries microsecond 0 in its `_id`.
            instant = instant.or(ident(COL_TIMESTAMP).is_null());
        }
        Ok(instant.and(row_hash_match))
    }
}

/// `ids`: each id decoded against this index. A spans-keyed id is a natural-key equality; a
/// hashed id is its timestamp microsecond (prunable; at microsecond 0 also a NULL timestamp) and
/// the canonical row hash, so an `_id` a search returned finds its own row. A malformed id, or
/// one of another index's form, matches nothing.
///
/// # Errors
///
/// Returns the error the index's table failed to open with, and `500` when an index with hashed
/// ids has no timestamp column.
fn lower_ids(query: &IdsQuery, scope: &LoweringScope<'_>) -> Result<Expr, OpenSearchError> {
    let mut disjuncts = Vec::with_capacity(query.values.len());
    // Read on the first hashed id: a query of span ids or of malformed ids never opens the schema.
    let mut hashed_id_columns: Option<HashedIdColumns> = None;
    for id in &query.values {
        match decode_document_id(id, scope.entry) {
            Some(DocumentIdPredicate::Span { trace_id, span_id }) => disjuncts.push(
                ident(COL_TRACE_ID)
                    .eq(lit(ScalarValue::FixedSizeBinary(16, Some(trace_id.to_vec()))))
                    .and(ident(COL_SPAN_ID).eq(lit(ScalarValue::FixedSizeBinary(8, Some(span_id.to_vec()))))),
            ),
            Some(DocumentIdPredicate::Timestamped {
                timestamp_micros,
                row_hash,
            }) => {
                let columns = match hashed_id_columns {
                    Some(ref columns) => columns,
                    None => hashed_id_columns.insert(HashedIdColumns::from_scope(scope)?),
                };
                disjuncts.push(columns.build_predicate(timestamp_micros, row_hash)?);
            }
            None => {}
        }
    }
    Ok(disjuncts.into_iter().reduce(Expr::or).unwrap_or_else(|| lit(false)))
}

/// An `exists`: a scalar or a struct is non-null; a MAP or a list holds at least one entry.
///
/// # Errors
///
/// Returns the error the index's table failed to open with.
fn lower_exists(query: &ExistsQuery, scope: &LoweringScope<'_>) -> Result<Expr, OpenSearchError> {
    let Some(field) = resolve_leaf_field(scope, &query.field)? else {
        return Ok(lit(false));
    };
    let read_expr = field.value.build_read_expr();
    Ok(match (field.field_type, &field.value) {
        (FieldType::Object, FieldValue::Column { column }) => {
            if matches!(
                scope.session.column_data_type(scope.entry, column)?,
                Some(DataType::Struct(_))
            ) {
                read_expr.is_not_null()
            } else {
                cardinality(read_expr).gt(lit(0_u64))
            }
        }
        (FieldType::Object, FieldValue::MapKey { .. } | FieldValue::HexId { .. })
        | (
            FieldType::Text
            | FieldType::Keyword
            | FieldType::Date
            | FieldType::Integer
            | FieldType::Long
            | FieldType::Double
            | FieldType::Boolean,
            _,
        ) => read_expr.is_not_null(),
    })
}

/// A `prefix` on a keyword value; the final query-shard 400 on an id, numeric, date or boolean
/// field.
fn lower_prefix(query: &PrefixQuery, scope: &LoweringScope<'_>) -> Result<Expr, OpenSearchError> {
    let Some(field) = resolve_leaf_field(scope, &query.field)? else {
        return Ok(lit(false));
    };
    match (field.field_type, &field.value) {
        (FieldType::Keyword, FieldValue::Column { .. } | FieldValue::MapKey { .. }) => {
            let read_expr = field.value.build_read_expr();
            Ok(if query.case_insensitive {
                starts_with(lower(read_expr), lower(lit(query.value.clone())))
            } else {
                starts_with(read_expr, lit(query.value.clone()))
            })
        }
        (FieldType::Object, _) => Ok(lit(false)),
        (FieldType::Text, _) => Err(refuse_unsupported_leaf(&field, "prefix", scope)),
        (FieldType::Keyword, FieldValue::HexId { .. })
        | (FieldType::Date | FieldType::Integer | FieldType::Long | FieldType::Double | FieldType::Boolean, _) => {
            Err(refuse_term_pattern(&field, "prefix", scope))
        }
    }
}

/// A `wildcard` over a whole keyword value; the final query-shard 400 on an id, numeric, date
/// or boolean field.
fn lower_wildcard(query: &WildcardQuery, scope: &LoweringScope<'_>) -> Result<Expr, OpenSearchError> {
    let Some(field) = resolve_leaf_field(scope, &query.field)? else {
        return Ok(lit(false));
    };
    match (field.field_type, &field.value) {
        (FieldType::Keyword, FieldValue::Column { .. } | FieldValue::MapKey { .. }) => Ok(Expr::Like(Like::new(
            false,
            Box::new(field.value.build_read_expr()),
            Box::new(lit(translate_wildcard_to_like(&query.pattern))),
            Some(LIKE_ESCAPE),
            query.case_insensitive,
        ))),
        (FieldType::Object, _) => Ok(lit(false)),
        (FieldType::Text, _) => Err(refuse_unsupported_leaf(&field, "wildcard", scope)),
        (FieldType::Keyword, FieldValue::HexId { .. })
        | (FieldType::Date | FieldType::Integer | FieldType::Long | FieldType::Double | FieldType::Boolean, _) => {
            Err(refuse_term_pattern(&field, "wildcard", scope))
        }
    }
}

/// The escape character of the `LIKE` patterns [`translate_wildcard_to_like`] builds.
const LIKE_ESCAPE: char = '\\';

/// Translate a `wildcard` pattern over the whole value to a `LIKE` pattern: `*` is any run,
/// `?` one character, and `\` escapes the next character (a trailing lone `\` is itself). The
/// `LIKE` metacharacters `%`, `_` and the escape itself are escaped, so they only ever match
/// themselves.
fn translate_wildcard_to_like(pattern: &str) -> String {
    let mut like = String::with_capacity(pattern.len() + 4);
    let mut characters = pattern.chars();
    while let Some(character) = characters.next() {
        match character {
            '*' => like.push('%'),
            '?' => like.push('_'),
            '\\' => push_like_literal(&mut like, characters.next().unwrap_or('\\')),
            other => push_like_literal(&mut like, other),
        }
    }
    like
}

/// Append `character` to `like` so that it matches only itself.
fn push_like_literal(like: &mut String, character: char) {
    if matches!(character, '%' | '_' | LIKE_ESCAPE) {
        like.push(LIKE_ESCAPE);
    }
    like.push(character);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use chrono::{DateTime, Utc};
    use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array, RecordBatch, StringArray, StructArray};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, cast, lit};
    use datafusion::physical_plan::{ExecutionPlan, display::DisplayableExecutionPlan};
    use datafusion::prelude::ident;
    use datafusion::scalar::ScalarValue;
    use iceberg::spec::{NestedField, PrimitiveType, StructType, Type};
    use iceberg::{NamespaceIdent, TableCreation};
    use icegate_common::schema::{
        COL_BODY, COL_INPUT_USD_PER_1M, COL_LOG_ATTRIBUTES, COL_MODEL, COL_SERVICE_NAME, COL_SEVERITY_TEXT,
        COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID,
    };
    use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE, OPERATIONS_TABLE, PRICES_TABLE};
    use serde_json::{Value, json};

    use super::{LoweringScope, build_date_literal, lower_query, translate_wildcard_to_like};
    use crate::opensearch::error::{OpenSearchError, render_envelope};
    use crate::query_dsl::parse::parse_query;
    use crate::test_rows::{OPERATION_NAME, STREAM, TEMPERATURE, TOP_K, build_operation_row};
    use crate::test_support::{FIXTURE_COMMITTED_WAL_OFFSET, SearchFixture, TestCell, TestRow, find_index_entry};

    const TENANT_A: &str = "tenant-a";
    const TENANT_B: &str = "tenant-b";
    /// 2025-10-09T08:53:20Z.
    const T0: i64 = 1_760_000_000_000_000;
    const TRACE_ID: [u8; 16] = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
    ];
    const OTHER_TRACE_ID: [u8; 16] = [0x11; 16];
    /// The exact-name table of the struct test, created by that test.
    const STATUSES_TABLE: &str = "statuses";
    /// Its struct column, and the text column the test reads back.
    const STATUS_COLUMN: &str = "status";
    const LABEL_COLUMN: &str = "label";
    /// The exact-name table of the unrepresentable-column test, and its binary column.
    const BLOBS_TABLE: &str = "blobs";
    const PAYLOAD_COLUMN: &str = "payload";

    fn build_request_instant() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-08T12:00:00Z")
            .expect("literal instant")
            .with_timezone(&Utc)
    }

    /// The values of `column` (cast to text) of the rows of `index` that `query` matches, seen
    /// by `tenant`.
    async fn select_matching_values(
        fixture: &SearchFixture,
        tenant: &str,
        index: &str,
        query: Value,
        column: &str,
    ) -> Result<BTreeSet<String>, OpenSearchError> {
        let session = fixture.open_session(tenant).await;
        let entry = find_index_entry(&session, index);
        let scope = LoweringScope {
            session: &session,
            entry,
            now: build_request_instant(),
        };
        let predicate = lower_query(&parse_query(&query)?, &scope)?;
        let plan = session
            .scan_index(entry)?
            .filter(predicate)
            .and_then(|builder| builder.project(vec![cast(ident(column), DataType::Utf8).alias("value")]))
            .and_then(datafusion::logical_expr::LogicalPlanBuilder::build)
            .expect("plan");
        let batches = session.collect(plan).await?;
        Ok(batches
            .iter()
            .flat_map(|batch| {
                let values = batch.column(0).as_string::<i32>();
                (0..values.len()).map(|row| values.value(row).to_string()).collect::<Vec<_>>()
            })
            .collect())
    }

    fn build_name_set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    /// A log row of `tenant` at `micros` with `cells`.
    fn build_log_row(
        tenant: &'static str,
        micros: i64,
        mut cells: Vec<(&'static str, TestCell<'static>)>,
    ) -> TestRow<'static> {
        cells.push((COL_TIMESTAMP, TestCell::Micros(micros)));
        TestRow { tenant, cells }
    }

    /// Log rows with explicit values, in Iceberg and in the WAL, plus tenant B rows equal to
    /// tenant A's in every value a test filters on: no result may ever contain a `b-` body.
    async fn seed_logs(fixture: &SearchFixture, iceberg_rows: &[TestRow<'_>], wal_rows: &[TestRow<'_>]) {
        fixture.commit_rows(LOGS_TABLE, iceberg_rows).await;
        fixture
            .write_wal_segment(LOGS_TABLE, FIXTURE_COMMITTED_WAL_OFFSET + 1, wal_rows)
            .await;
    }

    #[tokio::test]
    async fn bool_must_not_keeps_rows_where_the_predicate_is_null() {
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0)),
                        (COL_BODY, TestCell::Text("a-error")),
                        (COL_SEVERITY_TEXT, TestCell::Text("ERROR")),
                    ],
                },
                TestRow {
                    tenant: TENANT_B,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0)),
                        (COL_BODY, TestCell::Text("b-info")),
                        (COL_SEVERITY_TEXT, TestCell::Text("INFO")),
                    ],
                },
            ],
            &[
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 1)),
                        (COL_BODY, TestCell::Text("a-info")),
                        (COL_SEVERITY_TEXT, TestCell::Text("INFO")),
                    ],
                },
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 2)),
                        (COL_BODY, TestCell::Text("a-no-severity")),
                    ],
                },
                TestRow {
                    tenant: TENANT_B,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 2)),
                        (COL_BODY, TestCell::Text("b-no-severity")),
                    ],
                },
            ],
        )
        .await;

        let matched_values = select_matching_values(
            &fixture,
            TENANT_A,
            LOGS_TABLE,
            json!({"bool": {"must_not": [{"term": {"severity_text": "ERROR"}}]}}),
            COL_BODY,
        )
        .await
        .expect("search");

        assert_eq!(matched_values, build_name_set(&["a-info", "a-no-severity"]));
    }

    #[tokio::test]
    async fn epoch_millis_bounds_include_the_whole_millisecond_at_the_microsecond_boundary() {
        // The bound is X = 1760000000000 ms; its millisecond spans [X·1000, X·1000 + 999] µs.
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[
                build_log_row(TENANT_A, T0 - 1, vec![(COL_BODY, TestCell::Text("a-before"))]),
                build_log_row(TENANT_A, T0, vec![(COL_BODY, TestCell::Text("a-first-microsecond"))]),
                build_log_row(TENANT_B, T0, vec![(COL_BODY, TestCell::Text("b-first-microsecond"))]),
            ],
            &[
                build_log_row(
                    TENANT_A,
                    T0 + 999,
                    vec![(COL_BODY, TestCell::Text("a-last-microsecond"))],
                ),
                build_log_row(
                    TENANT_A,
                    T0 + 1_000,
                    vec![(COL_BODY, TestCell::Text("a-next-millisecond"))],
                ),
                build_log_row(TENANT_B, T0 + 500, vec![(COL_BODY, TestCell::Text("b-inside"))]),
            ],
        )
        .await;

        let inclusive_matches = select_matching_values(
            &fixture,
            TENANT_A,
            LOGS_TABLE,
            json!({"range": {"timestamp": {"gte": 1_760_000_000_000_i64, "lte": 1_760_000_000_000_i64, "format": "epoch_millis"}}}),
            COL_BODY,
        )
        .await
        .expect("search");
        assert_eq!(
            inclusive_matches,
            build_name_set(&["a-first-microsecond", "a-last-microsecond"])
        );

        let exclusive_upper_matches = select_matching_values(
            &fixture,
            TENANT_A,
            LOGS_TABLE,
            json!({"range": {"timestamp": {"gte": "1760000000000", "lt": "1760000000001", "format": "epoch_millis"}}}),
            COL_BODY,
        )
        .await
        .expect("search");
        assert_eq!(
            exclusive_upper_matches,
            build_name_set(&["a-first-microsecond", "a-last-microsecond"])
        );
    }

    #[tokio::test]
    async fn a_term_on_an_unmapped_field_matches_nothing() {
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0)),
                        (COL_BODY, TestCell::Text("a-one")),
                    ],
                },
                TestRow {
                    tenant: TENANT_B,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0)),
                        (COL_BODY, TestCell::Text("b-one")),
                    ],
                },
            ],
            &[
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 1)),
                        (COL_BODY, TestCell::Text("a-two")),
                    ],
                },
                TestRow {
                    tenant: TENANT_B,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 1)),
                        (COL_BODY, TestCell::Text("b-two")),
                    ],
                },
            ],
        )
        .await;

        for query in [
            json!({"term": {"no_such_field": "x"}}),
            json!({"exists": {"field": "no_such_field"}}),
            json!({"prefix": {"no_such_field": "x"}}),
            json!({"range": {"no_such_field": {"gte": "a"}}}),
        ] {
            let matched_values = select_matching_values(&fixture, TENANT_A, LOGS_TABLE, query.clone(), COL_BODY)
                .await
                .expect("search");
            assert_eq!(matched_values, build_name_set(&[]), "{query}");
        }
        let negated_matches = select_matching_values(
            &fixture,
            TENANT_A,
            LOGS_TABLE,
            json!({"bool": {"must_not": [{"term": {"no_such_field": "x"}}]}}),
            COL_BODY,
        )
        .await
        .expect("search");
        assert_eq!(negated_matches, build_name_set(&["a-one", "a-two"]));
    }

    #[tokio::test]
    async fn a_hex_id_term_matches_upper_and_lower_case_hex() {
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0)),
                        (COL_BODY, TestCell::Text("a-traced")),
                        (COL_TRACE_ID, TestCell::Bytes(&TRACE_ID)),
                    ],
                },
                TestRow {
                    tenant: TENANT_B,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0)),
                        (COL_BODY, TestCell::Text("b-traced")),
                        (COL_TRACE_ID, TestCell::Bytes(&TRACE_ID)),
                    ],
                },
            ],
            &[
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 1)),
                        (COL_BODY, TestCell::Text("a-traced-in-wal")),
                        (COL_TRACE_ID, TestCell::Bytes(&TRACE_ID)),
                    ],
                },
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 2)),
                        (COL_BODY, TestCell::Text("a-other-trace")),
                        (COL_TRACE_ID, TestCell::Bytes(&OTHER_TRACE_ID)),
                    ],
                },
            ],
        )
        .await;

        for hex in ["0123456789abcdeffedcba9876543210", "0123456789ABCDEFFEDCBA9876543210"] {
            let matched_values = select_matching_values(
                &fixture,
                TENANT_A,
                LOGS_TABLE,
                json!({"term": {"trace_id": hex}}),
                COL_BODY,
            )
            .await
            .expect("search");
            assert_eq!(
                matched_values,
                build_name_set(&["a-traced", "a-traced-in-wal"]),
                "{hex}"
            );
        }
        let listed_trace_matches = select_matching_values(
            &fixture,
            TENANT_A,
            LOGS_TABLE,
            json!({"terms": {"trace_id": ["0123456789ABCDEFFEDCBA9876543210", "11111111111111111111111111111111"]}}),
            COL_BODY,
        )
        .await
        .expect("search");
        assert_eq!(
            listed_trace_matches,
            build_name_set(&["a-traced", "a-traced-in-wal", "a-other-trace"])
        );
    }

    #[tokio::test]
    async fn an_invalid_hex_id_term_is_a_400() {
        let fixture = SearchFixture::new().await;

        for hex in [
            "zz23456789abcdeffedcba9876543210",
            "0123",
            "0123456789abcdeffedcba98765432100",
        ] {
            let error = select_matching_values(
                &fixture,
                TENANT_A,
                LOGS_TABLE,
                json!({"term": {"trace_id": hex}}),
                COL_BODY,
            )
            .await
            .expect_err("an invalid hex id is refused");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status.as_u16(), 400, "{hex}");
            assert_eq!(body["error"]["type"], "search_phase_execution_exception", "{hex}");
            assert_eq!(body["error"]["root_cause"][0]["type"], "query_shard_exception", "{hex}");
        }
    }

    #[tokio::test]
    async fn a_filter_on_a_key_that_is_both_a_leaf_and_a_prefix_reads_its_stored_key() {
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[TestRow {
                tenant: TENANT_A,
                cells: vec![
                    (COL_TIMESTAMP, TestCell::Micros(T0)),
                    (COL_BODY, TestCell::Text("a-both-keys")),
                    (
                        COL_LOG_ATTRIBUTES,
                        TestCell::Map(&[("http", "x"), ("http.method", "GET")]),
                    ),
                ],
            }],
            &[
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 1)),
                        (COL_BODY, TestCell::Text("a-leaf-only")),
                        (COL_LOG_ATTRIBUTES, TestCell::Map(&[("http", "GET")])),
                    ],
                },
                TestRow {
                    tenant: TENANT_B,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 1)),
                        (COL_BODY, TestCell::Text("b-both-keys")),
                        (
                            COL_LOG_ATTRIBUTES,
                            TestCell::Map(&[("http", "x"), ("http.method", "GET")]),
                        ),
                    ],
                },
            ],
        )
        .await;

        let dotted_key_matches = select_matching_values(
            &fixture,
            TENANT_A,
            LOGS_TABLE,
            json!({"term": {"log_attributes.http.method": "GET"}}),
            COL_BODY,
        )
        .await
        .expect("search");
        assert_eq!(dotted_key_matches, build_name_set(&["a-both-keys"]));

        let leaf_key_matches = select_matching_values(
            &fixture,
            TENANT_A,
            LOGS_TABLE,
            json!({"term": {"log_attributes.http": "GET"}}),
            COL_BODY,
        )
        .await
        .expect("search");
        assert_eq!(leaf_key_matches, build_name_set(&["a-leaf-only"]));
    }

    #[tokio::test]
    async fn prefix_and_wildcard_on_a_keyword_field_are_case_sensitive_unless_case_insensitive() {
        let fixture = SearchFixture::new().await;
        let build_service_row = |tenant: &'static str, micros: i64, service: &'static str| {
            build_log_row(tenant, micros, vec![(COL_SERVICE_NAME, TestCell::Text(service))])
        };
        seed_logs(
            &fixture,
            &[
                build_service_row(TENANT_A, T0, "Checkout"),
                build_service_row(TENANT_A, T0 + 1, "checkout-api"),
            ],
            &[
                build_service_row(TENANT_A, T0 + 2, "cart"),
                build_service_row(TENANT_A, T0 + 3, "50%_off"),
                build_service_row(TENANT_B, T0 + 2, "checkout-b"),
            ],
        )
        .await;

        let cases = [
            (json!({"prefix": {"service_name": "check"}}), vec!["checkout-api"]),
            (
                json!({"prefix": {"service_name": {"value": "CHECK", "case_insensitive": true}}}),
                vec!["Checkout", "checkout-api"],
            ),
            (json!({"wildcard": {"service_name": "C*t"}}), vec!["Checkout"]),
            (
                json!({"wildcard": {"service_name": {"value": "c*t", "case_insensitive": true}}}),
                vec!["Checkout", "cart"],
            ),
            (json!({"wildcard": {"service_name": "?art"}}), vec!["cart"]),
            (json!({"wildcard": {"service_name": "50%_*"}}), vec!["50%_off"]),
            (json!({"wildcard": {"service_name": "5_%*"}}), vec![]),
            (json!({"term": {"service_name": "checkout"}}), vec![]),
            (
                json!({"term": {"service_name": {"value": "checkout", "case_insensitive": true}}}),
                vec!["Checkout"],
            ),
        ];
        for (query, expected) in cases {
            let matched_values =
                select_matching_values(&fixture, TENANT_A, LOGS_TABLE, query.clone(), COL_SERVICE_NAME)
                    .await
                    .expect("search");
            assert_eq!(matched_values, build_name_set(&expected), "{query}");
        }
    }

    /// A keyword range compares byte-wise: `BZZZ` sorts between `B` and `C`, and `EA` above `E`,
    /// whatever their lengths. Each bound has a value below, at and above it.
    #[tokio::test]
    async fn a_range_on_a_keyword_field_compares_lexicographically() {
        let fixture = SearchFixture::new().await;
        let build_severity_row = |tenant: &'static str, micros: i64, severity: &'static str| {
            build_log_row(tenant, micros, vec![(COL_SEVERITY_TEXT, TestCell::Text(severity))])
        };
        seed_logs(
            &fixture,
            &[
                build_severity_row(TENANT_A, T0, "A"),
                build_severity_row(TENANT_A, T0 + 1, "B"),
                build_severity_row(TENANT_A, T0 + 2, "BZZZ"),
                build_severity_row(TENANT_B, T0 + 2, "BB"),
            ],
            &[
                build_severity_row(TENANT_A, T0 + 3, "C"),
                build_severity_row(TENANT_A, T0 + 4, "E"),
                build_severity_row(TENANT_A, T0 + 5, "EA"),
                build_severity_row(TENANT_A, T0 + 6, "F"),
                build_severity_row(TENANT_B, T0 + 3, "CC"),
            ],
        )
        .await;

        let cases = [
            (
                json!({"range": {"severity_text": {"gte": "B", "lt": "E"}}}),
                vec!["B", "BZZZ", "C"],
            ),
            (
                json!({"range": {"severity_text": {"gt": "B", "lte": "E"}}}),
                vec!["BZZZ", "C", "E"],
            ),
            (json!({"range": {"severity_text": {"gt": "E"}}}), vec!["EA", "F"]),
            (json!({"range": {"severity_text": {"lt": "B"}}}), vec!["A"]),
        ];
        for (query, expected) in cases {
            let matched_values =
                select_matching_values(&fixture, TENANT_A, LOGS_TABLE, query.clone(), COL_SEVERITY_TEXT)
                    .await
                    .expect("search");
            assert_eq!(matched_values, build_name_set(&expected), "{query}");
        }
    }

    /// Rows matching none, one, two and three of the three `should` clauses (a missing field is a
    /// NULL clause, which counts as not matching): `minimum_should_match: 2` keeps the last two,
    /// and a minimum above the clause count keeps nothing.
    #[tokio::test]
    async fn bool_minimum_should_match_counts_the_matching_should_clauses() {
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[
                build_log_row(TENANT_A, T0, vec![(COL_BODY, TestCell::Text("a-none"))]),
                build_log_row(
                    TENANT_A,
                    T0 + 1,
                    vec![
                        (COL_BODY, TestCell::Text("a-one")),
                        (COL_SEVERITY_TEXT, TestCell::Text("ERROR")),
                    ],
                ),
                build_log_row(
                    TENANT_B,
                    T0 + 1,
                    vec![
                        (COL_BODY, TestCell::Text("b-three")),
                        (COL_SEVERITY_TEXT, TestCell::Text("ERROR")),
                        (COL_SERVICE_NAME, TestCell::Text("checkout")),
                        (COL_LOG_ATTRIBUTES, TestCell::Map(&[("region", "eu")])),
                    ],
                ),
            ],
            &[
                build_log_row(
                    TENANT_A,
                    T0 + 2,
                    vec![
                        (COL_BODY, TestCell::Text("a-two")),
                        (COL_SEVERITY_TEXT, TestCell::Text("ERROR")),
                        (COL_SERVICE_NAME, TestCell::Text("checkout")),
                    ],
                ),
                build_log_row(
                    TENANT_A,
                    T0 + 3,
                    vec![
                        (COL_BODY, TestCell::Text("a-three")),
                        (COL_SEVERITY_TEXT, TestCell::Text("ERROR")),
                        (COL_SERVICE_NAME, TestCell::Text("checkout")),
                        (COL_LOG_ATTRIBUTES, TestCell::Map(&[("region", "eu")])),
                    ],
                ),
            ],
        )
        .await;
        let should_clauses = json!([
            {"term": {"severity_text": "ERROR"}},
            {"term": {"service_name": "checkout"}},
            {"term": {"log_attributes.region": "eu"}}
        ]);

        let cases = [(2, vec!["a-two", "a-three"]), (3, vec!["a-three"]), (4, vec![])];
        for (minimum_should_count, expected) in cases {
            let query = json!({"bool": {"should": should_clauses, "minimum_should_match": minimum_should_count}});
            let matched_values = select_matching_values(&fixture, TENANT_A, LOGS_TABLE, query.clone(), COL_BODY)
                .await
                .expect("search");
            assert_eq!(matched_values, build_name_set(&expected), "{query}");
        }
    }

    /// A `should` beside no `must` or `filter` needs one matching clause, so rows matching one or
    /// both of the two clauses match and a row matching neither does not. Beside a `must`, the
    /// default minimum is zero and `should` filters nothing.
    #[tokio::test]
    async fn bool_should_alone_requires_one_clause_and_beside_must_filters_nothing() {
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[
                build_log_row(TENANT_A, T0, vec![(COL_BODY, TestCell::Text("a-none"))]),
                build_log_row(
                    TENANT_A,
                    T0 + 1,
                    vec![
                        (COL_BODY, TestCell::Text("a-one")),
                        (COL_SEVERITY_TEXT, TestCell::Text("ERROR")),
                    ],
                ),
                build_log_row(
                    TENANT_B,
                    T0 + 1,
                    vec![
                        (COL_BODY, TestCell::Text("b-two")),
                        (COL_SEVERITY_TEXT, TestCell::Text("ERROR")),
                        (COL_SERVICE_NAME, TestCell::Text("checkout")),
                    ],
                ),
            ],
            &[build_log_row(
                TENANT_A,
                T0 + 2,
                vec![
                    (COL_BODY, TestCell::Text("a-two")),
                    (COL_SEVERITY_TEXT, TestCell::Text("ERROR")),
                    (COL_SERVICE_NAME, TestCell::Text("checkout")),
                ],
            )],
        )
        .await;
        let should_clauses = json!([
            {"term": {"severity_text": "ERROR"}},
            {"term": {"service_name": "checkout"}}
        ]);

        let cases = [
            (json!({"bool": {"should": should_clauses}}), vec!["a-one", "a-two"]),
            (
                json!({"bool": {"must": [{"exists": {"field": "body"}}], "should": should_clauses}}),
                vec!["a-none", "a-one", "a-two"],
            ),
        ];
        for (query, expected) in cases {
            let matched_values = select_matching_values(&fixture, TENANT_A, LOGS_TABLE, query.clone(), COL_BODY)
                .await
                .expect("search");
            assert_eq!(matched_values, build_name_set(&expected), "{query}");
        }
    }

    /// An empty `terms` list names no value, and a `term`, `terms` or `range` on an `object`
    /// field (a MAP column) compares no value: each matches nothing, even on a row whose map
    /// holds the compared text.
    #[tokio::test]
    async fn an_empty_terms_list_or_a_leaf_on_an_object_field_matches_nothing() {
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[build_log_row(
                TENANT_A,
                T0,
                vec![
                    (COL_BODY, TestCell::Text("a-with-map")),
                    (COL_SEVERITY_TEXT, TestCell::Text("ERROR")),
                    (COL_LOG_ATTRIBUTES, TestCell::Map(&[("v", "v")])),
                ],
            )],
            &[build_log_row(
                TENANT_B,
                T0 + 1,
                vec![
                    (COL_BODY, TestCell::Text("b-with-map")),
                    (COL_LOG_ATTRIBUTES, TestCell::Map(&[("v", "v")])),
                ],
            )],
        )
        .await;

        for query in [
            json!({"terms": {"severity_text": []}}),
            json!({"term": {"log_attributes": "v"}}),
            json!({"terms": {"log_attributes": ["v"]}}),
            json!({"range": {"log_attributes": {"gte": "a"}}}),
        ] {
            let matched_values = select_matching_values(&fixture, TENANT_A, LOGS_TABLE, query.clone(), COL_BODY)
                .await
                .expect("search");
            assert_eq!(matched_values, build_name_set(&[]), "{query}");
        }
    }

    /// `exists` on a MAP column needs at least one entry: an empty map and an absent (null) map
    /// both fail it.
    #[tokio::test]
    async fn exists_on_a_map_column_requires_an_entry() {
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[
                build_log_row(
                    TENANT_A,
                    T0,
                    vec![
                        (COL_BODY, TestCell::Text("a-with-entries")),
                        (COL_LOG_ATTRIBUTES, TestCell::Map(&[("k", "v")])),
                    ],
                ),
                build_log_row(
                    TENANT_B,
                    T0,
                    vec![
                        (COL_BODY, TestCell::Text("b-with-entries")),
                        (COL_LOG_ATTRIBUTES, TestCell::Map(&[("k", "v")])),
                    ],
                ),
            ],
            &[
                build_log_row(
                    TENANT_A,
                    T0 + 1,
                    vec![
                        (COL_BODY, TestCell::Text("a-empty")),
                        (COL_LOG_ATTRIBUTES, TestCell::Map(&[])),
                    ],
                ),
                build_log_row(TENANT_A, T0 + 2, vec![(COL_BODY, TestCell::Text("a-absent"))]),
            ],
        )
        .await;

        let matched_values = select_matching_values(
            &fixture,
            TENANT_A,
            LOGS_TABLE,
            json!({"exists": {"field": "log_attributes"}}),
            COL_BODY,
        )
        .await
        .expect("search");

        assert_eq!(matched_values, build_name_set(&["a-with-entries"]));
    }

    /// Create the exact-name table [`STATUSES_TABLE`] (`tenant_id`, `timestamp`, a struct
    /// `status { code }` and a text `label`) and commit one batch built by hand, because the row
    /// helpers fill no struct cell: tenant A holds one row with a status and one with a null
    /// status, tenant B one row with a status.
    async fn seed_statuses(fixture: &SearchFixture) {
        let status_type = Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
            4,
            "code",
            Type::Primitive(PrimitiveType::Int),
        ))]));
        let table_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    COL_TENANT_ID,
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    2,
                    COL_TIMESTAMP,
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
                Arc::new(NestedField::optional(3, STATUS_COLUMN, status_type)),
                Arc::new(NestedField::optional(
                    5,
                    LABEL_COLUMN,
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("statuses schema");
        let creation = TableCreation::builder()
            .name(STATUSES_TABLE.to_string())
            .schema(table_schema)
            .build();
        fixture
            .catalog
            .create_table(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), creation)
            .await
            .expect("create statuses");

        let arrow_schema = fixture.read_arrow_schema(STATUSES_TABLE).await;
        let DataType::Struct(status_fields) = arrow_schema
            .field_with_name(STATUS_COLUMN)
            .expect("status column")
            .data_type()
            .clone()
        else {
            panic!("`{STATUS_COLUMN}` is not a struct");
        };
        let DataType::Timestamp(TimeUnit::Microsecond, timezone) = arrow_schema
            .field_with_name(COL_TIMESTAMP)
            .expect("timestamp column")
            .data_type()
            .clone()
        else {
            panic!("`{COL_TIMESTAMP}` is not a microsecond timestamp");
        };
        let statuses = StructArray::new(
            status_fields,
            vec![Arc::new(Int32Array::from(vec![Some(200), None, Some(500)])) as ArrayRef],
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![TENANT_A, TENANT_A, TENANT_B])),
            Arc::new(
                datafusion::arrow::array::TimestampMicrosecondArray::from(vec![T0, T0 + 1, T0])
                    .with_timezone_opt(timezone),
            ),
            Arc::new(statuses),
            Arc::new(StringArray::from(vec![
                "a-with-status",
                "a-null-status",
                "b-with-status",
            ])),
        ];
        let batch = RecordBatch::try_new(arrow_schema, columns).expect("statuses batch");
        fixture.commit_batch(STATUSES_TABLE, batch).await;
    }

    #[tokio::test]
    async fn exists_on_a_struct_column_is_a_null_check() {
        let fixture = SearchFixture::new().await;
        seed_statuses(&fixture).await;

        let matched_values = select_matching_values(
            &fixture,
            TENANT_A,
            STATUSES_TABLE,
            json!({"exists": {"field": STATUS_COLUMN}}),
            LABEL_COLUMN,
        )
        .await
        .expect("search");

        assert_eq!(matched_values, build_name_set(&["a-with-status"]));
    }

    /// A `term` on a column whose Arrow type no `OpenSearch` type represents (a binary column)
    /// is refused, and so is its negation: neither answers as if the column were absent, which
    /// would match nothing, or every row under `must_not`.
    #[tokio::test]
    async fn a_leaf_on_a_column_no_opensearch_type_represents_is_a_500() {
        let fixture = SearchFixture::new().await;
        let table_schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    COL_TENANT_ID,
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    2,
                    COL_TIMESTAMP,
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
                Arc::new(NestedField::optional(
                    3,
                    PAYLOAD_COLUMN,
                    Type::Primitive(PrimitiveType::Binary),
                )),
            ])
            .build()
            .expect("blobs schema");
        let creation = TableCreation::builder()
            .name(BLOBS_TABLE.to_string())
            .schema(table_schema)
            .build();
        fixture
            .catalog
            .create_table(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), creation)
            .await
            .expect("create blobs");

        for query in [
            json!({"term": {PAYLOAD_COLUMN: "x"}}),
            json!({"bool": {"must_not": [{"term": {PAYLOAD_COLUMN: "x"}}]}}),
        ] {
            let error = select_matching_values(&fixture, TENANT_A, BLOBS_TABLE, query.clone(), COL_TIMESTAMP)
                .await
                .expect_err("a leaf on an unrepresentable column is refused");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status.as_u16(), 500, "{query}");
            assert_eq!(body["error"]["type"], "exception", "{query}");
        }
    }

    #[tokio::test]
    async fn a_range_on_a_hex_id_compares_byte_order() {
        let fixture = SearchFixture::new().await;
        let build_span_row = |tenant: &'static str, micros: i64, span: &'static [u8; 8], body: &'static str| {
            build_log_row(
                tenant,
                micros,
                vec![(COL_SPAN_ID, TestCell::Bytes(span)), (COL_BODY, TestCell::Text(body))],
            )
        };
        seed_logs(
            &fixture,
            &[
                build_span_row(TENANT_A, T0, &[0, 0, 0, 0, 0, 0, 0, 1], "a-01"),
                build_span_row(
                    TENANT_A,
                    T0 + 1,
                    &[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
                    "a-7f",
                ),
            ],
            &[
                build_span_row(TENANT_A, T0 + 2, &[0x80, 0, 0, 0, 0, 0, 0, 0], "a-80"),
                build_span_row(TENANT_A, T0 + 3, &[0xff; 8], "a-ff"),
                build_span_row(TENANT_B, T0 + 3, &[0xff; 8], "b-ff"),
            ],
        )
        .await;

        let cases = [
            (
                json!({"range": {"span_id": {"gte": "8000000000000000"}}}),
                vec!["a-80", "a-ff"],
            ),
            (json!({"range": {"span_id": {"gt": "8000000000000000"}}}), vec!["a-ff"]),
            (
                json!({"range": {"span_id": {"gt": "0000000000000001", "lt": "FFFFFFFFFFFFFFFF"}}}),
                vec!["a-7f", "a-80"],
            ),
            (
                json!({"range": {"span_id": {"lte": "7FFFFFFFFFFFFFFF"}}}),
                vec!["a-01", "a-7f"],
            ),
        ];
        for (query, expected) in cases {
            let matched_values = select_matching_values(&fixture, TENANT_A, LOGS_TABLE, query.clone(), COL_BODY)
                .await
                .expect("search");
            assert_eq!(matched_values, build_name_set(&expected), "{query}");
        }
    }

    #[tokio::test]
    async fn prefix_and_wildcard_on_an_id_numeric_date_or_boolean_field_are_a_query_shard_400() {
        let fixture = SearchFixture::new().await;

        let cases = [
            (LOGS_TABLE, json!({"prefix": {"trace_id": "0123"}})),
            (LOGS_TABLE, json!({"wildcard": {"span_id": "01*"}})),
            (LOGS_TABLE, json!({"prefix": {"timestamp": "2026"}})),
            (LOGS_TABLE, json!({"wildcard": {"observed_timestamp": "2026*"}})),
            (OPERATIONS_TABLE, json!({"prefix": {"top_k": "1"}})),
            (OPERATIONS_TABLE, json!({"wildcard": {"temperature": "0.*"}})),
            (OPERATIONS_TABLE, json!({"prefix": {"stream": "tr"}})),
        ];
        for (index, query) in cases {
            let error = select_matching_values(&fixture, TENANT_A, index, query.clone(), COL_TIMESTAMP)
                .await
                .expect_err("a pattern on a typed field is refused");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status.as_u16(), 400, "{query}");
            assert_eq!(body["error"]["type"], "search_phase_execution_exception", "{query}");
            assert_eq!(
                body["error"]["root_cause"][0]["type"], "query_shard_exception",
                "{query}"
            );
        }
    }

    /// One row per arm of the translation: `*`, `?`, an escaped `*`, an escaped `\`, a trailing
    /// lone `\`, the `LIKE` metacharacters `%` and `_`, and a plain character. The translation is
    /// a `match` over one input character at a time, so these rows cover every arm, and a
    /// pattern is the concatenation of its characters' translations.
    #[test]
    fn wildcard_patterns_translate_to_like_patterns_arm_by_arm() {
        let cases = [
            ("*", "%"),
            ("?", "_"),
            ("\\*", "*"),
            ("\\\\", "\\\\"),
            ("a\\", "a\\\\"),
            ("%", "\\%"),
            ("_", "\\_"),
            ("a", "a"),
            ("50%_*", "50\\%\\_%"),
        ];
        for (pattern, expected) in cases {
            assert_eq!(translate_wildcard_to_like(pattern), expected, "{pattern}");
        }
    }

    /// Each date unit with a boundary that is an exact multiple of the unit and one that is not:
    /// a coarser unit rounds the half-open boundary up, a finer one scales it exactly.
    #[tokio::test]
    async fn date_bounds_round_up_to_the_unit_of_the_column() {
        let zone: Option<Arc<str>> = Some(Arc::from("UTC"));
        let cases = [
            (
                1_500_000,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                ScalarValue::TimestampMicrosecond(Some(1_500_000), None),
            ),
            (
                1_500_000,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                ScalarValue::TimestampNanosecond(Some(1_500_000_000), None),
            ),
            (
                2_000,
                DataType::Timestamp(TimeUnit::Millisecond, zone.clone()),
                ScalarValue::TimestampMillisecond(Some(2), zone.clone()),
            ),
            (
                2_001,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                ScalarValue::TimestampMillisecond(Some(3), None),
            ),
            (
                -1,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                ScalarValue::TimestampMillisecond(Some(0), None),
            ),
            (
                2_000_000,
                DataType::Timestamp(TimeUnit::Second, None),
                ScalarValue::TimestampSecond(Some(2), None),
            ),
            (
                2_000_001,
                DataType::Timestamp(TimeUnit::Second, None),
                ScalarValue::TimestampSecond(Some(3), None),
            ),
            (86_400_000_000, DataType::Date32, ScalarValue::Date32(Some(1))),
            (86_400_000_001, DataType::Date32, ScalarValue::Date32(Some(2))),
            (1_000, DataType::Date64, ScalarValue::Date64(Some(1))),
            (1_001, DataType::Date64, ScalarValue::Date64(Some(2))),
        ];
        for (micros, data_type, expected) in cases {
            assert_eq!(
                build_date_literal(micros, &data_type),
                Ok(lit(expected)),
                "{micros} {data_type}"
            );
        }

        let error = build_date_literal(0, &DataType::Utf8).expect_err("not a date type");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status.as_u16(), 500);
        assert_eq!(body["error"]["type"], "exception");
    }

    /// Whether `expr` is `<column> = <FixedSizeBinary(n) literal of bytes>`.
    fn is_fixed_binary_equality(expr: &Expr, column: &str, bytes: &[u8]) -> bool {
        let Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) = expr
        else {
            return false;
        };
        let is_column = matches!(left.as_ref(), Expr::Column(found) if found.name == column);
        let width = i32::try_from(bytes.len()).expect("width");
        let is_literal = matches!(
            right.as_ref(),
            Expr::Literal(ScalarValue::FixedSizeBinary(found_width, Some(found)), _)
                if *found_width == width && found.as_slice() == bytes
        );
        is_column && is_literal
    }

    fn collect_plan_lines(plan: &Arc<dyn ExecutionPlan>, lines: &mut Vec<(String, String)>) {
        lines.push((
            plan.name().to_string(),
            DisplayableExecutionPlan::new(plan.as_ref()).one_line().to_string(),
        ));
        for child in plan.children() {
            collect_plan_lines(child, lines);
        }
    }

    /// The pruning contract: a `trace_id` term reaches the table scan as a typed
    /// `FixedSizeBinary(16)` equality, so the Iceberg scan receives a `Fixed` predicate and the
    /// WAL Parquet scan a column comparison it prunes with. A `Binary` literal would arrive as a
    /// cast of the column, and neither scan could prune.
    #[tokio::test]
    async fn a_trace_id_term_reaches_the_wal_and_iceberg_scans_as_a_fixed_binary_literal() {
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0)),
                        (COL_BODY, TestCell::Text("a-iceberg")),
                        (COL_TRACE_ID, TestCell::Bytes(&TRACE_ID)),
                    ],
                },
                TestRow {
                    tenant: TENANT_B,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0)),
                        (COL_BODY, TestCell::Text("b-iceberg")),
                        (COL_TRACE_ID, TestCell::Bytes(&TRACE_ID)),
                    ],
                },
            ],
            &[
                TestRow {
                    tenant: TENANT_A,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 1)),
                        (COL_BODY, TestCell::Text("a-wal")),
                        (COL_TRACE_ID, TestCell::Bytes(&TRACE_ID)),
                    ],
                },
                TestRow {
                    tenant: TENANT_B,
                    cells: vec![
                        (COL_TIMESTAMP, TestCell::Micros(T0 + 1)),
                        (COL_BODY, TestCell::Text("b-wal")),
                        (COL_TRACE_ID, TestCell::Bytes(&TRACE_ID)),
                    ],
                },
            ],
        )
        .await;
        let session = fixture.open_session(TENANT_A).await;
        let entry = find_index_entry(&session, LOGS_TABLE);
        let scope = LoweringScope {
            session: &session,
            entry,
            now: build_request_instant(),
        };
        let query = parse_query(&json!({"term": {"trace_id": "0123456789abcdeffedcba9876543210"}})).expect("parse");
        let plan = session
            .scan_index(entry)
            .expect("scan")
            .filter(lower_query(&query, &scope).expect("lower"))
            .expect("filter")
            .build()
            .expect("plan");

        let context = fixture.engine.create_session().await.expect("session");
        let optimized = context.state().optimize(&plan).expect("optimize");
        let mut scan_filters = Vec::new();
        optimized
            .apply(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    scan_filters.extend(scan.filters.iter().cloned());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("walk");
        assert!(
            scan_filters
                .iter()
                .any(|filter| is_fixed_binary_equality(filter, COL_TRACE_ID, &TRACE_ID)),
            "the table scan receives the typed equality, got {scan_filters:?}"
        );

        let physical = context.state().create_physical_plan(&optimized).await.expect("physical plan");
        let mut lines = Vec::new();
        collect_plan_lines(&physical, &mut lines);
        let iceberg_line = lines
            .iter()
            .find(|(name, _)| name == "IcegateIcebergScan")
            .map(|(_, line)| line.clone())
            .expect("an Iceberg scan");
        assert!(
            iceberg_line.contains("trace_id = 0123456789ABCDEFFEDCBA9876543210"),
            "Iceberg predicate: {iceberg_line}"
        );
        let wal_line = lines
            .iter()
            .find(|(name, _)| name == "DataSourceExec")
            .map(|(_, line)| line.clone())
            .expect("a WAL Parquet scan");
        assert!(wal_line.contains("trace_id@"), "WAL predicate: {wal_line}");
        assert!(!wal_line.contains("CAST(trace_id"), "WAL predicate: {wal_line}");

        // Tenant B holds one row with the same trace id in each store; the tenant scope keeps
        // both out, so exactly tenant A's two rows come back.
        let rows = session.collect(plan).await.expect("execute");
        let row_count: usize = rows.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(row_count, 2);
    }

    /// Lower `query` for tenant A on `index` and check it is the query-shard 400.
    async fn assert_query_shard_400(fixture: &SearchFixture, index: &str, query: Value) {
        let error = select_matching_values(fixture, TENANT_A, index, query.clone(), COL_TIMESTAMP)
            .await
            .expect_err("refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status.as_u16(), 400, "{query}");
        assert_eq!(body["error"]["type"], "search_phase_execution_exception", "{query}");
        assert_eq!(
            body["error"]["root_cause"][0]["type"], "query_shard_exception",
            "{query}"
        );
    }

    /// The `-0.0` and `+0.0` rows sit in both stores. Every double bound reaches the scans as an
    /// inclusive comparison, so statistics that treat the two zeros alike prune no matching file.
    #[tokio::test]
    async fn double_comparisons_order_negative_zero_before_positive_zero_on_both_paths() {
        let fixture = SearchFixture::new().await;
        fixture
            .commit_rows(
                OPERATIONS_TABLE,
                &[
                    build_operation_row(
                        TENANT_A,
                        "neg-zero-iceberg",
                        T0,
                        vec![(TEMPERATURE, TestCell::Double(-0.0))],
                    ),
                    build_operation_row(
                        TENANT_A,
                        "pos-zero-iceberg",
                        T0,
                        vec![(TEMPERATURE, TestCell::Double(0.0))],
                    ),
                    build_operation_row(
                        TENANT_A,
                        "minus-one-iceberg",
                        T0,
                        vec![(TEMPERATURE, TestCell::Double(-1.0))],
                    ),
                    build_operation_row(TENANT_B, "b-pos-zero", T0, vec![(TEMPERATURE, TestCell::Double(0.0))]),
                ],
            )
            .await;
        fixture
            .write_wal_segment(
                OPERATIONS_TABLE,
                FIXTURE_COMMITTED_WAL_OFFSET + 1,
                &[
                    build_operation_row(
                        TENANT_A,
                        "neg-zero-wal",
                        T0,
                        vec![(TEMPERATURE, TestCell::Double(-0.0))],
                    ),
                    build_operation_row(TENANT_A, "pos-zero-wal", T0, vec![(TEMPERATURE, TestCell::Double(0.0))]),
                    build_operation_row(TENANT_A, "half-wal", T0, vec![(TEMPERATURE, TestCell::Double(0.5))]),
                    build_operation_row(TENANT_A, "unset-wal", T0, vec![]),
                    build_operation_row(TENANT_B, "b-neg-zero", T0, vec![(TEMPERATURE, TestCell::Double(-0.0))]),
                ],
            )
            .await;

        let cases = [
            (
                json!({"term": {"temperature": "-0.0"}}),
                vec!["neg-zero-iceberg", "neg-zero-wal"],
            ),
            (
                json!({"term": {"temperature": "0.0"}}),
                vec!["pos-zero-iceberg", "pos-zero-wal"],
            ),
            (
                json!({"terms": {"temperature": ["-0.0", "0.5"]}}),
                vec!["neg-zero-iceberg", "neg-zero-wal", "half-wal"],
            ),
            (
                json!({"range": {"temperature": {"gt": "-0.0"}}}),
                vec!["pos-zero-iceberg", "pos-zero-wal", "half-wal"],
            ),
            (
                json!({"range": {"temperature": {"gte": 0}}}),
                vec!["pos-zero-iceberg", "pos-zero-wal", "half-wal"],
            ),
            (
                json!({"range": {"temperature": {"lt": 0}}}),
                vec!["neg-zero-iceberg", "neg-zero-wal", "minus-one-iceberg"],
            ),
            (
                json!({"range": {"temperature": {"lte": "-0.0"}}}),
                vec!["neg-zero-iceberg", "neg-zero-wal", "minus-one-iceberg"],
            ),
            (json!({"range": {"temperature": {"gt": 0}}}), vec!["half-wal"]),
        ];
        for (query, expected) in cases {
            let matched_values =
                select_matching_values(&fixture, TENANT_A, OPERATIONS_TABLE, query.clone(), OPERATION_NAME)
                    .await
                    .expect("search");
            assert_eq!(matched_values, build_name_set(&expected), "{query}");
        }
    }

    /// The range bounds are quoted booleans: a JSON boolean is no range bound.
    #[tokio::test]
    async fn a_boolean_range_orders_false_before_true_and_must_not_keeps_rows_without_the_field() {
        let fixture = SearchFixture::new().await;
        fixture
            .commit_rows(
                OPERATIONS_TABLE,
                &[
                    build_operation_row(TENANT_A, "streamed", T0, vec![(STREAM, TestCell::Bool(true))]),
                    build_operation_row(TENANT_B, "b-not-streamed", T0, vec![(STREAM, TestCell::Bool(false))]),
                ],
            )
            .await;
        fixture
            .write_wal_segment(
                OPERATIONS_TABLE,
                FIXTURE_COMMITTED_WAL_OFFSET + 1,
                &[
                    build_operation_row(TENANT_A, "not-streamed", T0, vec![(STREAM, TestCell::Bool(false))]),
                    build_operation_row(TENANT_A, "unknown", T0, vec![]),
                ],
            )
            .await;

        let cases = [
            (json!({"term": {"stream": true}}), vec!["streamed"]),
            (json!({"term": {"stream": "false"}}), vec!["not-streamed"]),
            (
                json!({"terms": {"stream": [true, false]}}),
                vec!["streamed", "not-streamed"],
            ),
            (json!({"range": {"stream": {"gt": "false"}}}), vec!["streamed"]),
            (json!({"range": {"stream": {"lt": "true"}}}), vec!["not-streamed"]),
            (
                json!({"range": {"stream": {"gte": "false", "lte": "true"}}}),
                vec!["streamed", "not-streamed"],
            ),
            (json!({"range": {"stream": {"gt": "true"}}}), vec![]),
            (
                json!({"bool": {"must_not": [{"term": {"stream": true}}]}}),
                vec!["not-streamed", "unknown"],
            ),
        ];
        for (query, expected) in cases {
            let matched_values =
                select_matching_values(&fixture, TENANT_A, OPERATIONS_TABLE, query.clone(), OPERATION_NAME)
                    .await
                    .expect("search");
            assert_eq!(matched_values, build_name_set(&expected), "{query}");
        }
        for query in [
            json!({"term": {"stream": "TRUE"}}),
            json!({"range": {"stream": {"gte": "1"}}}),
        ] {
            assert_query_shard_400(&fixture, OPERATIONS_TABLE, query).await;
        }
    }

    #[tokio::test]
    async fn a_non_finite_numeric_term_is_malformed_and_a_range_bound_is_not_checked() {
        let fixture = SearchFixture::new().await;
        fixture
            .commit_rows(
                OPERATIONS_TABLE,
                &[
                    build_operation_row(TENANT_A, "warm", T0, vec![(TEMPERATURE, TestCell::Double(0.7))]),
                    build_operation_row(TENANT_A, "cold", T0, vec![(TEMPERATURE, TestCell::Double(-3.5))]),
                    build_operation_row(TENANT_A, "unset", T0, vec![]),
                    build_operation_row(TENANT_B, "b-warm", T0, vec![(TEMPERATURE, TestCell::Double(0.7))]),
                ],
            )
            .await;

        for query in [
            json!({"term": {"temperature": "NaN"}}),
            json!({"term": {"temperature": "Infinity"}}),
            json!({"term": {"temperature": "warm"}}),
            json!({"range": {"temperature": {"gt": "warm"}}}),
        ] {
            assert_query_shard_400(&fixture, OPERATIONS_TABLE, query).await;
        }
        let cases = [
            (
                json!({"range": {"temperature": {"gte": "-Infinity"}}}),
                vec!["warm", "cold"],
            ),
            (
                json!({"range": {"temperature": {"lte": "Infinity"}}}),
                vec!["warm", "cold"],
            ),
            (
                json!({"range": {"temperature": {"gt": "-Infinity", "lt": "0x1p0"}}}),
                vec!["warm", "cold"],
            ),
            (json!({"range": {"temperature": {"gt": 0.7}}}), vec![]),
            (json!({"term": {"temperature": "7e-1"}}), vec!["warm"]),
        ];
        for (query, expected) in cases {
            let matched_values =
                select_matching_values(&fixture, TENANT_A, OPERATIONS_TABLE, query.clone(), OPERATION_NAME)
                    .await
                    .expect("search");
            assert_eq!(matched_values, build_name_set(&expected), "{query}");
        }
    }

    #[tokio::test]
    async fn an_out_of_range_integer_term_is_a_400() {
        let fixture = SearchFixture::new().await;
        fixture
            .commit_rows(
                OPERATIONS_TABLE,
                &[
                    build_operation_row(TENANT_A, "ten", T0, vec![(TOP_K, TestCell::Long(10))]),
                    build_operation_row(TENANT_A, "max", T0, vec![(TOP_K, TestCell::Long(i64::MAX))]),
                    build_operation_row(TENANT_B, "b-ten", T0, vec![(TOP_K, TestCell::Long(10))]),
                ],
            )
            .await;

        for query in [
            json!({"term": {"top_k": "9223372036854775808"}}),
            json!({"term": {"top_k": "ten"}}),
            json!({"terms": {"top_k": ["10", "1e20"]}}),
        ] {
            assert_query_shard_400(&fixture, OPERATIONS_TABLE, query).await;
        }
        let cases = [
            (json!({"term": {"top_k": "1e1"}}), vec!["ten"]),
            (json!({"term": {"top_k": 9_223_372_036_854_775_807_i64}}), vec!["max"]),
            (json!({"term": {"top_k": "10.5"}}), vec![]),
            (json!({"range": {"top_k": {"gt": 9.5, "lt": 10.5}}}), vec!["ten"]),
            (json!({"range": {"top_k": {"gte": "1e19"}}}), vec![]),
            (json!({"terms": {"top_k": ["10.5", "0.25"]}}), vec![]),
            (json!({"terms": {"top_k": ["10", "1e1", "10.5"]}}), vec!["ten"]),
        ];
        for (query, expected) in cases {
            let matched_values =
                select_matching_values(&fixture, TENANT_A, OPERATIONS_TABLE, query.clone(), OPERATION_NAME)
                    .await
                    .expect("search");
            assert_eq!(matched_values, build_name_set(&expected), "{query}");
        }
    }

    /// `1e19` lies past every `long`, so on its own it empties the range; beside a malformed
    /// bound the range is still the 400, whichever side the malformed bound is on.
    #[tokio::test]
    async fn a_malformed_integer_bound_is_a_400_beside_a_bound_that_empties_the_range() {
        let fixture = SearchFixture::new().await;

        for query in [
            json!({"range": {"top_k": {"gte": "1e19", "lte": "abc"}}}),
            json!({"range": {"top_k": {"gte": "abc", "lte": "1e19"}}}),
        ] {
            assert_query_shard_400(&fixture, OPERATIONS_TABLE, query).await;
        }
    }

    /// The `prices` rate columns are decimals typed `double`: they compare as doubles, never as
    /// a double rounded to the column's scale. `1.00000000004` and `1e-11` have no exact value at
    /// scale 10, and the smallest positive double rounds to 0 there. `prices` is a global table,
    /// read whole by every tenant, so it has no cross-tenant case.
    #[tokio::test]
    async fn a_decimal_rate_column_compares_as_a_double() {
        let fixture = SearchFixture::new().await;
        let build_price_row = |model: &'static str, unscaled_rate: i128| TestRow {
            tenant: TENANT_A,
            cells: vec![
                (COL_MODEL, TestCell::Text(model)),
                (COL_INPUT_USD_PER_1M, TestCell::Decimal(unscaled_rate)),
            ],
        };
        fixture
            .commit_rows(
                PRICES_TABLE,
                &[
                    build_price_row("zero", 0),
                    build_price_row("one", 10_000_000_000),
                    build_price_row("one-and-a-bit", 10_000_000_001),
                ],
            )
            .await;

        let cases = [
            (
                json!({"range": {"input_usd_per_1m": {"gt": 0}}}),
                vec!["one", "one-and-a-bit"],
            ),
            (
                json!({"range": {"input_usd_per_1m": {"gte": "1.00000000004"}}}),
                vec!["one-and-a-bit"],
            ),
            (json!({"term": {"input_usd_per_1m": "1e-11"}}), vec![]),
            (json!({"term": {"input_usd_per_1m": "1"}}), vec!["one"]),
            (
                json!({"terms": {"input_usd_per_1m": ["0", "1.0000000001"]}}),
                vec!["zero", "one-and-a-bit"],
            ),
        ];
        for (query, expected) in cases {
            let matched_values = select_matching_values(&fixture, TENANT_A, PRICES_TABLE, query.clone(), COL_MODEL)
                .await
                .expect("search");
            assert_eq!(matched_values, build_name_set(&expected), "{query}");
        }
    }

    #[tokio::test]
    async fn a_date_term_matches_the_rows_inside_the_range_it_covers() {
        let fixture = SearchFixture::new().await;
        seed_logs(
            &fixture,
            &[
                build_log_row(
                    TENANT_A,
                    1_760_000_000_000_000,
                    vec![(COL_BODY, TestCell::Text("a-same-day"))],
                ),
                build_log_row(
                    TENANT_A,
                    1_759_967_999_999_999,
                    vec![(COL_BODY, TestCell::Text("a-day-before"))],
                ),
                build_log_row(
                    TENANT_A,
                    1_759_968_000_000_000,
                    vec![(COL_BODY, TestCell::Text("a-first-microsecond"))],
                ),
            ],
            &[
                build_log_row(
                    TENANT_A,
                    1_760_054_399_999_999,
                    vec![(COL_BODY, TestCell::Text("a-last-microsecond"))],
                ),
                build_log_row(
                    TENANT_A,
                    1_760_054_400_000_000,
                    vec![(COL_BODY, TestCell::Text("a-next-day"))],
                ),
                build_log_row(
                    TENANT_B,
                    1_760_000_000_000_000,
                    vec![(COL_BODY, TestCell::Text("b-same-day"))],
                ),
            ],
        )
        .await;

        // 2025-10-09 covers [1759968000000000, 1760054400000000) µs.
        let day_matches = select_matching_values(
            &fixture,
            TENANT_A,
            LOGS_TABLE,
            json!({"term": {"timestamp": "2025-10-09"}}),
            COL_BODY,
        )
        .await
        .expect("search");
        assert_eq!(
            day_matches,
            build_name_set(&["a-same-day", "a-first-microsecond", "a-last-microsecond"])
        );

        let instant_matches = select_matching_values(
            &fixture,
            TENANT_A,
            LOGS_TABLE,
            json!({"terms": {"timestamp": ["1760000000000", "2025-10-09T23:59:59.999Z"]}}),
            COL_BODY,
        )
        .await
        .expect("search");
        assert_eq!(instant_matches, build_name_set(&["a-same-day", "a-last-microsecond"]));
        assert_query_shard_400(&fixture, LOGS_TABLE, json!({"term": {"timestamp": "yesterday"}})).await;
    }
}
