//! Multi-membership bucket aggregations: `range`, `date_range`, `filters` and `filter`.
//!
//! A row belongs to every bucket whose condition it meets. Such a level keys each row by the
//! list of its bucket ordinals, and the planner unnests that list in a stage aggregate of its
//! own, so a row repeats for this level and the levels below it only. Statically disjoint
//! ranges, a single filter and `filter` give each row at most one ordinal and need no unnest.
//! A `filter` / `filters` query is lowered per searched index by `plan_aggregation_rows` into a
//! Boolean column of the rows plan, so a field one index does not map matches nothing there.
//!
//! Every bucket of the request is answered in request order, an empty one with `doc_count` 0. A
//! `range` bucket is `[from, to)`: `from` and `to` are echoed as numbers and absent when open,
//! and the key is `<from>-<to>` (`*` for an open end) unless the request names it; each bound
//! of the key is spelled with at least one fraction digit, in `d.dddE<n>` form when its
//! magnitude lies outside `[10^-3, 10^7)`. A `date_range` bucket renders its resolved bounds as
//! epoch-millisecond doubles, each with an `_as_string` rendering in the request's `format`
//! (default `strict_date_time`, UTC). Named `filters` render a keyed object in request order and
//! anonymous ones an array; `filter` renders `doc_count` and its sub-aggregations directly.

use std::cmp::Ordering;
use std::fmt::{self, Display};

use chrono::{DateTime, Utc};
use datafusion::common::ScalarValue;
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::{Expr, Literal, lit, when};
use serde_json::{Map, Number, Value};

use super::plan::LevelKey;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::doc_id::MICROS_PER_MILLISECOND;
use crate::opensearch::search::execute::convert_plan_error;
use crate::opensearch::search::hits::{DateRenderFormat, render_date};
use crate::query_dsl::ast::Query;
use crate::query_dsl::date_math::{BoundSide, TimeZoneSpec, resolve_date_bound};

/// One range of a `range` aggregation: `[from, to)`, an absent end open.
#[derive(Debug)]
pub(crate) struct NumericRange {
    /// The inclusive lower bound.
    pub(crate) from: Option<f64>,
    /// The exclusive upper bound.
    pub(crate) to: Option<f64>,
    /// The bucket's key when the request names it.
    pub(crate) key: Option<String>,
}

/// A `range` request.
#[derive(Debug)]
pub(crate) struct RangeBucketsSpec {
    /// The numeric field the ranges read.
    pub(crate) field: String,
    /// The ranges, in request order; at least one.
    pub(crate) ranges: Vec<NumericRange>,
    /// Whether the buckets render as an object keyed by bucket key.
    pub(crate) is_keyed: bool,
}

/// One bound of a `date_range` range as the request wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DateRangeBound {
    /// A date, date math or epoch-millisecond string, as written; it resolves as a range query's
    /// bound does, so a string of four digits is a year.
    Expression(String),
    /// A JSON integer: epoch milliseconds.
    EpochMillis(i64),
}

impl Display for DateRangeBound {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expression(text) => formatter.write_str(text),
            Self::EpochMillis(millis) => write!(formatter, "{millis}"),
        }
    }
}

/// One range of a `date_range` aggregation, `[from, to)`, its bounds as the request wrote them.
#[derive(Debug)]
pub(crate) struct DateMathRange {
    /// The inclusive lower bound; open when absent.
    pub(crate) from: Option<DateRangeBound>,
    /// The exclusive upper bound; open when absent.
    pub(crate) to: Option<DateRangeBound>,
    /// The bucket's key when the request names it.
    pub(crate) key: Option<String>,
}

/// A `date_range` request.
#[derive(Debug)]
pub(crate) struct DateRangeBucketsSpec {
    /// The date field the ranges read.
    pub(crate) field: String,
    /// The ranges, in request order; at least one.
    pub(crate) ranges: Vec<DateMathRange>,
    /// The zone date math rounds in; UTC when absent.
    pub(crate) time_zone: Option<TimeZoneSpec>,
    /// The format of the `_as_string` renderings; `strict_date_time` when absent.
    pub(crate) format: Option<DateRenderFormat>,
}

/// A `filters` request.
#[derive(Debug)]
pub(crate) struct FiltersSpec {
    /// The bucket queries, in request order.
    pub(crate) filters: FiltersBuckets,
}

/// The bucket queries of a `filters` aggregation; at least one.
#[derive(Debug)]
pub(crate) enum FiltersBuckets {
    /// Named buckets, rendered as an object keyed by name.
    Named(Vec<(String, Query)>),
    /// Anonymous buckets, rendered as an array.
    Anonymous(Vec<Query>),
}

impl FiltersSpec {
    /// The bucket queries, in request order.
    pub(super) fn list_queries(&self) -> Vec<&Query> {
        match &self.filters {
            FiltersBuckets::Named(filters) => filters.iter().map(|(_, query)| query).collect(),
            FiltersBuckets::Anonymous(filters) => filters.iter().collect(),
        }
    }
}

/// The bounds of one `date_range` range, resolved against the request's `now`, in microseconds
/// since the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ResolvedDateRange {
    /// The inclusive lower bound.
    pub(super) from_micros: Option<i64>,
    /// The exclusive upper bound.
    pub(super) to_micros: Option<i64>,
}

/// The bounds of every range of the `date_range` aggregation `name`: an integer as epoch
/// milliseconds, a string as a date or date math resolved against `now` in the spec's zone.
/// Every resolved bound is an instant `chrono` represents, so it renders as a date.
///
/// # Errors
///
/// The date-math errors of a string bound (`400 illegal_argument_exception`); `400
/// illegal_argument_exception` naming `name` under `aggregation` for a bound outside the range
/// `chrono` represents.
pub(super) fn resolve_date_ranges(
    name: &str,
    spec: &DateRangeBucketsSpec,
    now: DateTime<Utc>,
) -> Result<Vec<ResolvedDateRange>, OpenSearchError> {
    let resolve_bound = |bound: &DateRangeBound, side: BoundSide| {
        let micros = match bound {
            DateRangeBound::EpochMillis(millis) => millis.checked_mul(MICROS_PER_MILLISECOND),
            DateRangeBound::Expression(text) => {
                Some(resolve_date_bound(text, None, spec.time_zone.as_ref(), now, side)?)
            }
        };
        micros
            .filter(|micros| DateTime::from_timestamp_micros(*micros).is_some())
            .ok_or_else(|| {
                OpenSearchError::unsupported_aggregation(
                    name,
                    format!("bound [{bound}] of [{name}] lies outside the representable time range"),
                )
            })
    };
    spec.ranges
        .iter()
        .map(|range| {
            Ok(ResolvedDateRange {
                from_micros: range
                    .from
                    .as_ref()
                    .map(|bound| resolve_bound(bound, BoundSide::LowerInclusive))
                    .transpose()?,
                to_micros: range
                    .to
                    .as_ref()
                    .map(|bound| resolve_bound(bound, BoundSide::UpperExclusive))
                    .transpose()?,
            })
        })
        .collect()
}

/// Per range, `value >= from AND value < to`; with both ends open, any row with a value.
pub(super) fn plan_range_conditions<T: Literal + Copy>(value: &Expr, bounds: &[(Option<T>, Option<T>)]) -> Vec<Expr> {
    bounds
        .iter()
        .map(|(from, to)| match (from, to) {
            (Some(from), Some(to)) => value.clone().gt_eq(lit(*from)).and(value.clone().lt(lit(*to))),
            (Some(from), None) => value.clone().gt_eq(lit(*from)),
            (None, Some(to)) => value.clone().lt(lit(*to)),
            (None, None) => value.clone().is_not_null(),
        })
        .collect()
}

/// Key each row by the ordinals of the `conditions` it meets. When at most one can hold for any
/// row, a scalar `CASE` yields that ordinal (or NULL). Otherwise a list holds one entry per
/// condition, NULL where it fails, which the planner unnests, dropping the NULL entries. A NULL
/// condition fails.
///
/// # Errors
///
/// `400 illegal_argument_exception` for more buckets than an `Int32` ordinal numbers; `500` when
/// the expression fails to build.
pub(super) fn plan_ordinals(conditions: Vec<Expr>, is_single_membership: bool) -> Result<LevelKey, OpenSearchError> {
    let build_ordinal_literal = |index: usize| {
        i32::try_from(index)
            .map(lit)
            .map_err(|_| OpenSearchError::illegal_argument(format!("an aggregation lists {index} buckets or more")))
    };
    if is_single_membership {
        let mut cases = conditions.into_iter().enumerate();
        let Some((first_index, first_condition)) = cases.next() else {
            return Ok(LevelKey {
                expr: lit(ScalarValue::Int32(None)),
                is_membership_list: false,
            });
        };
        let mut builder = when(first_condition, build_ordinal_literal(first_index)?);
        for (index, condition) in cases {
            builder = builder.when(condition, build_ordinal_literal(index)?);
        }
        return Ok(LevelKey {
            expr: builder.end().map_err(convert_plan_error)?,
            is_membership_list: false,
        });
    }
    let entries = conditions
        .into_iter()
        .enumerate()
        .map(|(index, condition)| when(condition, build_ordinal_literal(index)?).end().map_err(convert_plan_error))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LevelKey {
        expr: make_array(entries),
        is_membership_list: true,
    })
}

/// Whether no value can lie in two of the `[from, to)` ranges (an absent end is open).
pub(super) fn are_disjoint<T: PartialOrd + Copy>(bounds: &[(Option<T>, Option<T>)]) -> bool {
    let mut sorted_bounds = bounds.to_vec();
    sorted_bounds.sort_by(|left, right| match (left.0, right.0) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    });
    sorted_bounds.windows(2).all(|pair| match (pair[0].1, pair[1].0) {
        (Some(upper), Some(lower)) => upper <= lower,
        (None, _) | (_, None) => false,
    })
}

/// The key and the echoed bounds of a `range` bucket.
pub(super) fn render_range_bounds(range: &NumericRange) -> (String, Map<String, Value>) {
    let spell_bound = |bound: Option<f64>| bound.map_or_else(|| "*".to_string(), format_range_bound);
    let key = range
        .key
        .clone()
        .unwrap_or_else(|| format!("{}-{}", spell_bound(range.from), spell_bound(range.to)));
    let mut bounds = Map::new();
    if let Some(from) = range.from {
        bounds.insert("from".to_string(), render_double(from));
    }
    if let Some(to) = range.to {
        bounds.insert("to".to_string(), render_double(to));
    }
    (key, bounds)
}

/// The key and the bounds of a `date_range` bucket.
///
/// # Errors
///
/// `500` for a bound that does not render as a date, which [`resolve_date_ranges`] rules out.
pub(super) fn render_date_range_bounds(
    range: &DateMathRange,
    resolved_range: ResolvedDateRange,
    format: Option<DateRenderFormat>,
) -> Result<(String, Map<String, Value>), OpenSearchError> {
    let format = format.unwrap_or(DateRenderFormat::StrictDateTime);
    let mut bounds = Map::new();
    let from_text = resolved_range
        .from_micros
        .map(|micros| insert_date_bound(&mut bounds, "from", micros, format))
        .transpose()?;
    let to_text = resolved_range
        .to_micros
        .map(|micros| insert_date_bound(&mut bounds, "to", micros, format))
        .transpose()?;
    let key = range.key.clone().unwrap_or_else(|| {
        format!(
            "{}-{}",
            from_text.as_deref().unwrap_or("*"),
            to_text.as_deref().unwrap_or("*")
        )
    });
    Ok((key, bounds))
}

/// Insert `<name>` (epoch milliseconds) and `<name>_as_string`, and return the string.
///
/// # Errors
///
/// `500` when `micros` does not render as a date in `format`.
#[expect(
    clippy::cast_precision_loss,
    reason = "an epoch millisecond below 2^53 (until the year 287396) converts exactly"
)]
fn insert_date_bound(
    bounds: &mut Map<String, Value>,
    name: &str,
    micros: i64,
    format: DateRenderFormat,
) -> Result<String, OpenSearchError> {
    let Value::String(text) = render_date(micros, format) else {
        return Err(OpenSearchError::internal(format!(
            "the bound [{name}] at [{micros}] microseconds does not render as a date"
        )));
    };
    let millis = micros.div_euclid(MICROS_PER_MILLISECOND);
    bounds.insert(name.to_string(), render_double(millis as f64));
    bounds.insert(format!("{name}_as_string"), Value::String(text.clone()));
    Ok(text)
}

/// A double as a JSON number; `null` for a value JSON cannot hold, which a finite bound never
/// is.
fn render_double(value: f64) -> Value {
    Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// A range bound spelled as a bucket key spells it: the shortest decimal that reads back to the
/// same double, with at least one fraction digit, and `d.dddE<n>` when the magnitude is below
/// 10^-3 or at least 10^7.
pub(super) fn format_range_bound(bound: f64) -> String {
    let magnitude = bound.abs();
    if magnitude == 0.0 || (1e-3..1e7).contains(&magnitude) {
        let text = format!("{bound}");
        return if text.contains('.') { text } else { format!("{text}.0") };
    }
    let text = format!("{bound:e}");
    let (mantissa, exponent) = text.split_once('e').unwrap_or((text.as_str(), "0"));
    if mantissa.contains('.') {
        format!("{mantissa}E{exponent}")
    } else {
        format!("{mantissa}.0E{exponent}")
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use chrono::DateTime;
    use serde_json::{Value, json};

    use super::super::parse::{AggregationKind, parse_aggregations};
    use super::{ResolvedDateRange, are_disjoint, format_range_bound, resolve_date_ranges};
    use crate::opensearch::error::{OpenSearchError, render_envelope};

    #[test]
    fn range_keys_spell_bounds_with_a_fraction_digit_and_exponents_outside_the_plain_range() {
        let cases = [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (100.0, "100.0"),
            (2.5, "2.5"),
            (-0.5, "-0.5"),
            (0.001, "0.001"),
            (1_234_567.0, "1234567.0"),
            (10_000_000.0, "1.0E7"),
            (12_345_678.9, "1.23456789E7"),
            (0.0001, "1.0E-4"),
        ];
        for (bound, expected) in cases {
            assert_eq!(format_range_bound(bound), expected, "{bound}");
        }
    }

    #[test]
    fn disjoint_ranges_are_told_from_overlapping_ones() {
        assert!(are_disjoint(&[
            (None, Some(100.0)),
            (Some(100.0), Some(300.0)),
            (Some(300.0), None)
        ]));
        assert!(are_disjoint(&[(Some(300.0), None), (None, Some(100.0))]));
        assert!(are_disjoint::<f64>(&[(None, None)]));
        assert!(!are_disjoint(&[(None, Some(250.0)), (Some(100.0), Some(400.0))]));
        assert!(!are_disjoint(&[(Some(1.0), None), (Some(2.0), None)]));
        assert!(!are_disjoint(&[(None, None), (Some(5.0), Some(6.0))]));
    }

    /// Resolve the ranges of the `date_range` aggregation `ranges` of `aggregations` at
    /// 2026-10-08T12:34:56.789Z.
    fn resolve_written_ranges(aggregations: &Value) -> Result<Vec<ResolvedDateRange>, OpenSearchError> {
        let tree = parse_aggregations(aggregations)?;
        let AggregationKind::DateRange(spec) = &tree.aggregations[0].kind else {
            panic!("the test wrote a date_range aggregation");
        };
        let now = DateTime::from_timestamp_micros(1_791_462_896_789_000).expect("a valid instant");
        resolve_date_ranges(&tree.aggregations[0].name, spec, now)
    }

    #[test]
    fn a_numeric_date_range_bound_is_epoch_milliseconds_and_a_string_one_is_read_as_a_date() {
        // The number 1000 is one second after the epoch; the string "2026" is the year 2026,
        // 2026-01-01T00:00:00Z.
        let resolved_ranges = resolve_written_ranges(&json!({"ranges": {"date_range": {
            "field": "timestamp",
            "ranges": [{"from": 1000}, {"from": "2026"}]
        }}}))
        .expect("resolve the bounds");

        assert_eq!(
            resolved_ranges,
            vec![
                ResolvedDateRange {
                    from_micros: Some(1_000_000),
                    to_micros: None
                },
                ResolvedDateRange {
                    from_micros: Some(1_767_225_600_000_000),
                    to_micros: None
                },
            ]
        );
    }

    #[tokio::test]
    async fn unrepresentable_or_malformed_date_range_bounds_are_illegal_argument_400s() {
        // 9e15 ms is about the year 287,000: it fits i64 microseconds, not a calendar date.
        for (bound, is_named_by_aggregation) in [
            (json!(9_000_000_000_000_000_i64), true),
            (json!("9000000000000000"), true),
            (json!("now-3x"), false),
        ] {
            let error = resolve_written_ranges(&json!({"ranges": {"date_range": {
                "field": "timestamp",
                "ranges": [{"from": bound.clone()}]
            }}}))
            .expect_err("the bound must be refused");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bound}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{bound}");
            assert_eq!(
                body["error"]["aggregation"] == "ranges",
                is_named_by_aggregation,
                "{bound}"
            );
        }
    }
}
