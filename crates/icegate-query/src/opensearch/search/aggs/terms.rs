//! `terms`: one bucket per distinct value of a field.
//!
//! A level is ranked by its `order` (by default the document count, descending; a metric
//! sub-aggregation may rank it), ties broken by the key ascending, so the buckets answered never
//! depend on scan order. `include` and `exclude`
//! drop keys before ranking, `missing` stands in for a row without the field, and `min_doc_count`
//! drops buckets with fewer documents. `min_doc_count: 0` answers only keys present in the
//! matching rows: no bucket is answered for a value seen only outside them.

use std::cmp::Ordering;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::functions::expr_fn::regexp_like;
use datafusion::logical_expr::{Expr, in_list, lit};
use regex::Regex;
use serde_json::{Map, Value};

use super::assemble::BucketKey;
use super::parse::{IGNORED_EXECUTION_KEYS, MetricPath, expect_count, expect_object, expect_string, parse_metric_path};
use crate::engine::fixed_binary::decode_fixed_binary_literal;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::doc_id::MICROS_PER_MILLISECOND;
use crate::opensearch::search::request::SortOrder;
use crate::query_dsl::regex::{RegexFlags, translate_lucene_regex};

/// Buckets answered when the request gives no `size`.
const DEFAULT_SIZE: usize = 10;

/// A `terms` request.
#[derive(Debug)]
pub(crate) struct TermsSpec {
    /// The field whose values are the bucket keys.
    pub(crate) field: String,
    /// The most buckets answered per parent bucket.
    pub(crate) size: usize,
    /// The ranking, most significant first; ties end ordered by key ascending.
    pub(crate) order: Vec<TermsOrder>,
    /// Keys admitted; every key when absent.
    pub(crate) include: Option<TermsFilter>,
    /// Keys dropped; none when absent.
    pub(crate) exclude: Option<TermsFilter>,
    /// The key of a row without the field; such a row joins no bucket when absent.
    pub(crate) missing: Option<Value>,
    /// Buckets with fewer documents are not answered.
    pub(crate) min_doc_count: u64,
}

impl TermsSpec {
    /// Whether every criterion of the order reads only a bucket's count or key. Such a level is
    /// ranked and cut to `size` in its stage aggregate, and assembly reads its admitted documents
    /// from there; a level ordered by a metric is ranked in assembly from every bucket.
    pub(super) fn is_ranked_by_count_or_key(&self) -> bool {
        self.order
            .iter()
            .all(|criterion| matches!(criterion, TermsOrder::Count(_) | TermsOrder::Key(_)))
    }
}

/// One ranking criterion of a `terms` level.
#[derive(Debug)]
pub(crate) enum TermsOrder {
    /// By document count.
    Count(SortOrder),
    /// By key: byte order for keywords and ids, numeric order for numbers and dates.
    Key(SortOrder),
    /// By a metric sub-aggregation of the level (`<id>`, `<id>.<stat>`, `<id>-orderAgg`). A
    /// bucket the metric has no value for ranks after every bucket that has one, in both
    /// directions.
    Metric {
        /// The ordering metric and, for a multi-value metric, its statistic.
        path: MetricPath,
        /// The direction of the ranking.
        order: SortOrder,
    },
}

/// The keys `include` admits or `exclude` drops.
#[derive(Debug)]
pub(crate) enum TermsFilter {
    /// Keys the anchored pattern matches; string keyword fields only (an encoded-id, numeric,
    /// date or boolean key refuses it, because such a key is never matched as text).
    Pattern(Regex),
    /// Keys equal to one of the values.
    Values(Vec<Value>),
}

/// Parse the body of a `terms` aggregation named `name`.
///
/// # Errors
///
/// `400 parsing_exception` for a body that is not an object, a missing `field`, an unknown key
/// (named under `construct`), a value of the wrong JSON type, a malformed `order` or direction, a
/// malformed metric order path ([`parse_metric_path`]), and a pattern past the length or
/// group-depth limit of [`translate_lucene_regex`] (the limit under `limit`) or using `&`, `~` or
/// a named automaton (under `construct`); `400 illegal_argument_exception` for `size: 0`, a
/// partitioned `include` or `exclude`, and a pattern that is not a valid regular expression.
pub(super) fn parse_terms(name: &str, value: &Value) -> Result<TermsSpec, OpenSearchError> {
    let body = expect_object(name, "terms", value)?;
    let mut field = None;
    let mut size = DEFAULT_SIZE;
    let mut order = vec![TermsOrder::Count(SortOrder::Desc)];
    let mut include = None;
    let mut exclude = None;
    let mut missing = None;
    let mut min_doc_count = 1;
    for (key, value) in body {
        match key.as_str() {
            "field" => field = Some(expect_string(name, key, value)?.to_string()),
            "size" => size = parse_size(name, value)?,
            "order" => order = parse_order(name, value)?,
            "include" => include = Some(parse_filter(name, key, value)?),
            "exclude" => exclude = Some(parse_filter(name, key, value)?),
            "missing" => {
                if !matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_)) {
                    return Err(OpenSearchError::parsing(format!(
                        "[missing] of [{name}] must be a string, a number or a boolean"
                    )));
                }
                missing = Some(value.clone());
            }
            "min_doc_count" => min_doc_count = expect_count(name, key, value)?,
            ignored if IGNORED_EXECUTION_KEYS.contains(&ignored) => {}
            unknown => {
                return Err(OpenSearchError::unsupported_construct(
                    unknown,
                    format!("[terms] of [{name}] does not support [{unknown}]"),
                ));
            }
        }
    }
    Ok(TermsSpec {
        field: field.ok_or_else(|| OpenSearchError::parsing(format!("[terms] of [{name}] requires [field]")))?,
        size,
        order,
        include,
        exclude,
        missing,
        min_doc_count,
    })
}

/// `size`: a positive integer; a value past `usize::MAX` reads as `usize::MAX`, which no level
/// reaches.
fn parse_size(name: &str, value: &Value) -> Result<usize, OpenSearchError> {
    let size = value
        .as_u64()
        .ok_or_else(|| OpenSearchError::parsing(format!("[size] of [{name}] must be a positive integer")))?;
    if size == 0 {
        return Err(OpenSearchError::illegal_argument(format!(
            "[size] of [{name}] must be greater than 0"
        )));
    }
    Ok(usize::try_from(size).unwrap_or(usize::MAX))
}

/// `order`: one object or an array of objects, each mapping `_count`, `_key` or a metric path to
/// a direction, in significance order. An empty order is refused rather than read as the default.
fn parse_order(name: &str, value: &Value) -> Result<Vec<TermsOrder>, OpenSearchError> {
    let refuse_order =
        || OpenSearchError::parsing(format!("[order] of [{name}] must be an object or an array of objects"));
    let clauses: Vec<&Map<String, Value>> = match value {
        Value::Object(clause) => vec![clause],
        Value::Array(clauses) => clauses
            .iter()
            .map(|clause| clause.as_object().ok_or_else(refuse_order))
            .collect::<Result<_, _>>()?,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => return Err(refuse_order()),
    };
    let mut order = Vec::new();
    for clause in clauses {
        for (path, direction) in clause {
            let direction = parse_direction(name, direction)?;
            order.push(match path.as_str() {
                "_count" => TermsOrder::Count(direction),
                "_key" => TermsOrder::Key(direction),
                other => TermsOrder::Metric {
                    path: parse_metric_path(other)?,
                    order: direction,
                },
            });
        }
    }
    if order.is_empty() {
        return Err(refuse_order());
    }
    Ok(order)
}

/// A direction of `order`: `asc` or `desc` in any case.
fn parse_direction(name: &str, value: &Value) -> Result<SortOrder, OpenSearchError> {
    match value.as_str().map(str::to_ascii_lowercase).as_deref() {
        Some("asc") => Ok(SortOrder::Asc),
        Some("desc") => Ok(SortOrder::Desc),
        _ => Err(OpenSearchError::parsing(format!(
            "[order] of [{name}] must be asc or desc"
        ))),
    }
}

/// `include` or `exclude` (`key`): a Lucene regular expression, anchored, or a list of scalar
/// values.
fn parse_filter(name: &str, key: &str, value: &Value) -> Result<TermsFilter, OpenSearchError> {
    match value {
        Value::String(pattern) => Ok(TermsFilter::Pattern(translate_lucene_regex(
            pattern,
            RegexFlags::parse(None)?,
            false,
        )?)),
        Value::Array(values) => {
            if values
                .iter()
                .all(|value| matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_)))
            {
                Ok(TermsFilter::Values(values.clone()))
            } else {
                Err(OpenSearchError::parsing(format!(
                    "[{key}] of [{name}] must list strings, numbers or booleans"
                )))
            }
        }
        Value::Object(_) => Err(OpenSearchError::illegal_argument(format!(
            "a partitioned [{key}] of [{name}] is not supported"
        ))),
        Value::Null | Value::Bool(_) | Value::Number(_) => Err(OpenSearchError::parsing(format!(
            "[{key}] of [{name}] must be a regular expression or an array of values"
        ))),
    }
}

/// `value` as a literal of the key type `data_type`: a keyword takes a string (or a number or
/// boolean as text), a number or a date (epoch milliseconds) a JSON number or a numeric string,
/// a boolean `true` / `false` (or that text), an id its hex string.
///
/// # Errors
///
/// `400 illegal_argument_exception` naming `key` when the value does not fit the type: a
/// fraction or an out-of-range integer for an integer key, a date past the microsecond range,
/// an id of the wrong width or not hex, or a key type no `terms` level reads.
pub(super) fn build_key_literal(
    name: &str,
    key: &str,
    value: &Value,
    data_type: &DataType,
) -> Result<Expr, OpenSearchError> {
    let refuse_value = || {
        OpenSearchError::illegal_argument(format!(
            "[{key}] value [{value}] of [{name}] does not fit the field's type"
        ))
    };
    let scalar = match (data_type, value) {
        (DataType::Utf8, Value::String(text)) => ScalarValue::Utf8(Some(text.clone())),
        (DataType::Utf8, Value::Number(number)) => ScalarValue::Utf8(Some(number.to_string())),
        (DataType::Utf8, Value::Bool(flag)) => ScalarValue::Utf8(Some(flag.to_string())),
        (DataType::Int32, _) => ScalarValue::Int32(Some(
            read_integer(value)
                .and_then(|number| i32::try_from(number).ok())
                .ok_or_else(refuse_value)?,
        )),
        (DataType::Int64, _) => ScalarValue::Int64(Some(read_integer(value).ok_or_else(refuse_value)?)),
        (DataType::Float64, _) => ScalarValue::Float64(Some(read_double(value).ok_or_else(refuse_value)?)),
        (DataType::Boolean, _) => ScalarValue::Boolean(Some(read_boolean(value).ok_or_else(refuse_value)?)),
        (DataType::Timestamp(TimeUnit::Microsecond, None), _) => ScalarValue::TimestampMicrosecond(
            Some(
                read_integer(value)
                    .and_then(|millis| millis.checked_mul(MICROS_PER_MILLISECOND))
                    .ok_or_else(refuse_value)?,
            ),
            None,
        ),
        (DataType::FixedSizeBinary(_), Value::String(text)) => {
            decode_fixed_binary_literal(text, data_type).map_err(|_| refuse_value())?
        }
        // The key types above are every type a `terms` level reads; any other pairing is a value
        // that does not fit.
        _ => return Err(refuse_value()),
    };
    Ok(lit(scalar))
}

/// An integer written as a JSON integer or as decimal text; `None` for anything else.
fn read_integer(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.parse().ok(),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

/// A number written as a JSON number or as decimal text; `None` for anything else.
fn read_double(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.parse().ok(),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

/// A boolean written as JSON or as the text `true` / `false`; `None` for anything else.
fn read_boolean(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(flag) => Some(*flag),
        Value::String(text) if text == "true" => Some(true),
        Value::String(text) if text == "false" => Some(false),
        _ => None,
    }
}

/// The predicate admitting the keys `include` admits and `exclude` does not; `None` when the
/// level filters no key.
///
/// # Errors
///
/// `400 illegal_argument_exception` for a pattern on a key that is not a string keyword (an
/// encoded id included), and for a listed value that does not fit the key type.
pub(super) fn build_filter_predicate(
    name: &str,
    spec: &TermsSpec,
    key: &Expr,
    data_type: &DataType,
) -> Result<Option<Expr>, OpenSearchError> {
    let include = spec
        .include
        .as_ref()
        .map(|filter| admit_keys(name, "include", filter, key, data_type))
        .transpose()?;
    let exclude = spec
        .exclude
        .as_ref()
        .map(|filter| admit_keys(name, "exclude", filter, key, data_type).map(|admit_predicate| !admit_predicate))
        .transpose()?;
    Ok(include.into_iter().chain(exclude).reduce(Expr::and))
}

/// The predicate true for the keys `filter` lists or matches.
fn admit_keys(
    name: &str,
    filter_key: &str,
    filter: &TermsFilter,
    key: &Expr,
    data_type: &DataType,
) -> Result<Expr, OpenSearchError> {
    match filter {
        TermsFilter::Pattern(pattern) if *data_type == DataType::Utf8 => {
            Ok(regexp_like(key.clone(), lit(pattern.as_str()), None))
        }
        TermsFilter::Pattern(_) if matches!(data_type, DataType::FixedSizeBinary(_)) => {
            Err(OpenSearchError::pattern_on_encoded_ids(name, filter_key))
        }
        TermsFilter::Pattern(_) => Err(OpenSearchError::illegal_argument(format!(
            "[{filter_key}] of [{name}] is a regular expression, which applies to string keyword fields only"
        ))),
        TermsFilter::Values(values) => {
            let literals = values
                .iter()
                .map(|value| build_key_literal(name, filter_key, value, data_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(in_list(key.clone(), literals, false))
        }
    }
}

/// One candidate bucket of a `terms` level, as the ranking reads it.
#[derive(Clone, Copy)]
pub(super) struct RankedBucket<'a> {
    /// The bucket's key.
    pub(super) key: &'a BucketKey,
    /// The bucket's document count.
    pub(super) doc_count: u64,
    /// Per criterion of the level's `order`, the bucket's value of the ordering metric: `None`
    /// for `_count` and `_key`, and for a bucket the metric has no value for. An absent entry
    /// reads as `None`, so the slice may be empty when no criterion orders by a metric.
    pub(super) order_values: &'a [Option<f64>],
}

/// The ranking of a `terms` level: the requested criteria in turn, then the key ascending, so
/// the buckets answered never depend on scan order.
pub(super) fn compare_buckets(spec: &TermsSpec, left: RankedBucket<'_>, right: RankedBucket<'_>) -> Ordering {
    for (position, order) in spec.order.iter().enumerate() {
        let ordering = match order {
            TermsOrder::Count(direction) => apply_direction(left.doc_count.cmp(&right.doc_count), *direction),
            TermsOrder::Key(direction) => apply_direction(left.key.cmp(right.key), *direction),
            TermsOrder::Metric { order: direction, .. } => compare_order_values(
                left.order_values.get(position).copied().flatten(),
                right.order_values.get(position).copied().flatten(),
                *direction,
            ),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.key.cmp(right.key)
}

/// Two values of an ordering metric in `direction`; a missing value ranks after every present
/// one in both directions.
fn compare_order_values(left: Option<f64>, right: Option<f64>, direction: SortOrder) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => apply_direction(left.total_cmp(&right), direction),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

/// `ordering`, ascending, turned into the order `direction` asks for.
const fn apply_direction(ordering: Ordering, direction: SortOrder) -> Ordering {
    match direction {
        SortOrder::Asc => ordering,
        SortOrder::Desc => ordering.reverse(),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, col, lit};
    use serde_json::{Value, json};

    use super::{TermsFilter, TermsSpec, build_filter_predicate, build_key_literal};
    use crate::opensearch::error::render_envelope;
    use crate::query_dsl::regex::{RegexFlags, translate_lucene_regex};

    /// A `terms` on `trace_id` with `include` and nothing else set.
    fn build_include_spec(include: TermsFilter) -> TermsSpec {
        TermsSpec {
            field: "trace_id".to_string(),
            size: 10,
            order: Vec::new(),
            include: Some(include),
            exclude: None,
            missing: None,
            min_doc_count: 1,
        }
    }

    #[tokio::test]
    async fn build_key_literal_fits_each_key_type_and_refuses_a_value_that_does_not() {
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        let trace_id = DataType::FixedSizeBinary(16);
        let fitting_cases: [(DataType, Value, ScalarValue); 13] = [
            (
                DataType::Utf8,
                json!("INFO"),
                ScalarValue::Utf8(Some("INFO".to_string())),
            ),
            (DataType::Utf8, json!(42), ScalarValue::Utf8(Some("42".to_string()))),
            (DataType::Utf8, json!(true), ScalarValue::Utf8(Some("true".to_string()))),
            (DataType::Int32, json!(-7), ScalarValue::Int32(Some(-7))),
            (
                DataType::Int32,
                json!("2147483647"),
                ScalarValue::Int32(Some(2_147_483_647)),
            ),
            (
                DataType::Int64,
                json!(9_000_000_000_i64),
                ScalarValue::Int64(Some(9_000_000_000)),
            ),
            (DataType::Float64, json!(-1.5), ScalarValue::Float64(Some(-1.5))),
            (DataType::Float64, json!("2.25"), ScalarValue::Float64(Some(2.25))),
            (DataType::Boolean, json!(false), ScalarValue::Boolean(Some(false))),
            (DataType::Boolean, json!("true"), ScalarValue::Boolean(Some(true))),
            (
                micros.clone(),
                json!(1_791_453_600_000_i64),
                ScalarValue::TimestampMicrosecond(Some(1_791_453_600_000_000), None),
            ),
            (
                micros.clone(),
                json!("1791453600000"),
                ScalarValue::TimestampMicrosecond(Some(1_791_453_600_000_000), None),
            ),
            (
                trace_id.clone(),
                json!("0123456789ABCDEFfedcba9876543210"),
                ScalarValue::FixedSizeBinary(
                    16,
                    Some(vec![
                        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
                    ]),
                ),
            ),
        ];
        for (data_type, value, expected) in fitting_cases {
            let literal = build_key_literal("t", "missing", &value, &data_type).expect("the value fits");

            assert_eq!(literal, lit(expected), "{value} as {data_type}");
        }

        let refused_cases: [(DataType, Value); 11] = [
            (DataType::Utf8, json!(null)),
            (DataType::Int32, json!(2_147_483_648_i64)),
            (DataType::Int32, json!(1.5)),
            (DataType::Int64, json!("ten")),
            (DataType::Float64, json!(true)),
            (DataType::Boolean, json!("yes")),
            (DataType::Boolean, json!(1)),
            (micros.clone(), json!(9_300_000_000_000_000_i64)),
            (trace_id.clone(), json!("not-a-hex-id")),
            (trace_id, json!("0123")),
            (DataType::Float32, json!(1.5)),
        ];
        for (data_type, value) in refused_cases {
            let error = build_key_literal("t", "missing", &value, &data_type).expect_err("the value does not fit");

            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{value} as {data_type}");
            assert_eq!(
                envelope["error"]["type"], "illegal_argument_exception",
                "{value} as {data_type}"
            );
        }
    }

    #[tokio::test]
    async fn a_regex_include_on_an_encoded_id_is_a_400_and_a_listed_hex_id_is_admitted() {
        let trace_id = DataType::FixedSizeBinary(16);
        let pattern = translate_lucene_regex(".*", RegexFlags::ALL, false).expect("a pattern");
        let listed_id = "0123456789abcdeffedcba9876543210";

        let pattern_error = build_filter_predicate(
            "traces",
            &build_include_spec(TermsFilter::Pattern(pattern)),
            &col("trace_id"),
            &trace_id,
        )
        .expect_err("a pattern on an encoded id is refused");
        let listed_predicate = build_filter_predicate(
            "traces",
            &build_include_spec(TermsFilter::Values(vec![json!(listed_id)])),
            &col("trace_id"),
            &trace_id,
        )
        .expect("a listed hex id is admitted");

        let (status, envelope) = render_envelope(pattern_error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(envelope["error"]["type"], "illegal_argument_exception");
        assert_eq!(envelope["error"]["field_storage"], "encoded_id");
        assert_eq!(envelope["error"]["aggregation"], "traces");
        let Some(Expr::InList(in_list)) = listed_predicate else {
            panic!("a listed include is an IN list");
        };
        assert_eq!(*in_list.expr, col("trace_id"));
        assert!(!in_list.negated);
        assert_eq!(
            in_list.list,
            vec![lit(ScalarValue::FixedSizeBinary(
                16,
                Some(vec![
                    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
                ]),
            ))]
        );
    }
}
