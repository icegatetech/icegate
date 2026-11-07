//! The `aggs` / `aggregations` object of a search request, parsed into an [`AggregationTree`].
//!
//! Each kind lists the keys it accepts, and any other key is a `parsing_exception` whose
//! `construct` names it: an option ignored without a word would answer other buckets than the
//! client asked for. The keys of [`IGNORED_EXECUTION_KEYS`] tune shard-level execution, which a
//! single-shard listener does not have, so every kind accepts and ignores them. A kind outside
//! IceGate's aggregation surface, or a name that is no aggregation type, is a `parsing_exception`
//! whose `construct` names it; a kind of the surface that this listener does not plan yet is a
//! `400 illegal_argument_exception` whose `aggregation_type` names it.

use std::time::Duration;

use serde_json::{Map, Value};

use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::doc_id::{
    MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MILLISECOND, MICROS_PER_MINUTE, MICROS_PER_SECOND,
};
use crate::opensearch::search::hits::DateRenderFormat;
use crate::query_dsl::date_math::{TimeZoneSpec, parse_time_zone};

/// Keys every kind accepts and ignores: they tune per-shard execution.
pub(super) const IGNORED_EXECUTION_KEYS: [&str; 3] = ["execution_hint", "shard_size", "collect_mode"];

/// Kinds outside IceGate's aggregation surface (with every `geo_` kind).
const OUT_OF_SCOPE_KINDS: [&str; 6] = [
    "significant_terms",
    "significant_text",
    "ip_range",
    "geohash_grid",
    "geotile_grid",
    "auto_date_histogram",
];

/// Out-of-scope kinds whose name does not say so by a prefix.
const OUT_OF_SCOPE_PIPELINES: [&str; 1] = ["extended_stats_bucket"];

/// Kinds of the aggregation surface that are not planned yet.
const NOT_YET_PLANNED_KINDS: [&str; 27] = [
    "terms",
    "histogram",
    "range",
    "date_range",
    "filters",
    "filter",
    "avg",
    "sum",
    "min",
    "max",
    "value_count",
    "stats",
    "cardinality",
    "percentiles",
    "percentile_ranks",
    "extended_stats",
    "top_hits",
    "cumulative_sum",
    "derivative",
    "serial_diff",
    "moving_fn",
    "moving_avg",
    "bucket_script",
    "avg_bucket",
    "sum_bucket",
    "min_bucket",
    "max_bucket",
];

/// The aggregations of one search request, in request order.
#[derive(Debug)]
pub(crate) struct AggregationTree {
    /// The top-level aggregations; each answers under its own name.
    pub(crate) aggregations: Vec<NamedAggregation>,
}

/// One aggregation, the name its answer is keyed by, and its sub-aggregations.
#[derive(Debug)]
pub(crate) struct NamedAggregation {
    /// The request's name for the aggregation, echoed verbatim in the response.
    pub(crate) name: String,
    /// What the aggregation computes.
    pub(crate) kind: AggregationKind,
    /// The sub-aggregations, computed inside each of this aggregation's buckets.
    pub(crate) children: Vec<Self>,
}

/// What one aggregation computes.
#[derive(Debug)]
pub(crate) enum AggregationKind {
    /// Buckets of a date field by time interval.
    DateHistogram(DateHistogramSpec),
}

impl AggregationKind {
    /// The field the aggregation reads.
    pub(crate) fn field(&self) -> &str {
        match self {
            Self::DateHistogram(spec) => &spec.field,
        }
    }
}

/// A `date_histogram` request.
#[derive(Debug)]
pub(crate) struct DateHistogramSpec {
    /// The date field bucketed.
    pub(crate) field: String,
    /// The bucket width.
    pub(crate) interval: HistogramInterval,
    /// The zone local bucket starts are computed in; UTC when absent.
    pub(crate) time_zone: Option<TimeZoneSpec>,
    /// Shift of every bucket start, in microseconds.
    pub(crate) offset_micros: i64,
    /// Buckets with fewer documents are not answered. `0`, the default, also answers the empty
    /// buckets between the first and the last bucket, widened to `extended_bounds`.
    pub(crate) min_doc_count: u64,
    /// Epoch-millisecond bounds empty buckets are answered up to when `min_doc_count` is `0`.
    pub(crate) extended_bounds: Option<(i64, i64)>,
    /// The format of `key_as_string`; a date with the zone's offset when absent.
    pub(crate) format: Option<DateRenderFormat>,
    /// The epoch milliseconds (a JSON integer or a string of digits) a row without the field is
    /// bucketed at; such a row joins no bucket when absent.
    pub(crate) missing: Option<Value>,
}

/// The width of a `date_histogram` bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistogramInterval {
    /// One calendar unit, as long as the local calendar makes it.
    Calendar(CalendarUnit),
    /// A fixed duration.
    Fixed(Duration),
}

/// A calendar unit of `calendar_interval` or of a legacy `interval`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CalendarUnit {
    /// One second.
    Second,
    /// One minute.
    Minute,
    /// One local hour.
    Hour,
    /// One local day.
    Day,
    /// One local week, starting on Monday.
    Week,
    /// One local month.
    Month,
    /// One local quarter, starting in January, April, July or October.
    Quarter,
    /// One local year.
    Year,
}

impl CalendarUnit {
    /// The unit as `date_trunc` names it.
    pub(crate) const fn truncation_name(self) -> &'static str {
        match self {
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        }
    }
}

/// Parse the value of a request's `aggs` or `aggregations` key.
///
/// # Errors
///
/// `400 parsing_exception` for a malformed object, an unknown key, an unknown or out-of-scope
/// kind (the kind under `construct`); `400 illegal_argument_exception` for an invalid option
/// value or a kind not planned yet.
pub(crate) fn parse_aggregations(value: &Value) -> Result<AggregationTree, OpenSearchError> {
    Ok(AggregationTree {
        aggregations: parse_named_aggregations(value)?,
    })
}

fn parse_named_aggregations(value: &Value) -> Result<Vec<NamedAggregation>, OpenSearchError> {
    let Value::Object(entries) = value else {
        return Err(OpenSearchError::parsing(
            "aggregations must be an object keyed by aggregation name",
        ));
    };
    entries.iter().map(|(name, body)| parse_named_aggregation(name, body)).collect()
}

fn parse_named_aggregation(name: &str, body: &Value) -> Result<NamedAggregation, OpenSearchError> {
    let Value::Object(body) = body else {
        return Err(OpenSearchError::parsing(format!(
            "aggregation [{name}] must be an object"
        )));
    };
    let mut kind = None;
    let mut children = None;
    for (key, value) in body {
        match key.as_str() {
            "aggs" | "aggregations" => {
                if children.is_some() {
                    return Err(OpenSearchError::parsing(format!(
                        "found two sub-aggregation definitions in [{name}]"
                    )));
                }
                children = Some(parse_named_aggregations(value)?);
            }
            kind_name => {
                if kind.is_some() {
                    return Err(OpenSearchError::parsing(format!(
                        "found two aggregation types in [{name}], the second is [{kind_name}]"
                    )));
                }
                kind = Some(parse_kind(name, kind_name, value)?);
            }
        }
    }
    let kind = kind.ok_or_else(|| OpenSearchError::parsing(format!("missing aggregation type in [{name}]")))?;
    Ok(NamedAggregation {
        name: name.to_string(),
        kind,
        children: children.unwrap_or_default(),
    })
}

fn parse_kind(name: &str, kind_name: &str, value: &Value) -> Result<AggregationKind, OpenSearchError> {
    match kind_name {
        "date_histogram" => Ok(AggregationKind::DateHistogram(parse_date_histogram(name, value)?)),
        _ if OUT_OF_SCOPE_KINDS.contains(&kind_name)
            || OUT_OF_SCOPE_PIPELINES.contains(&kind_name)
            || kind_name.starts_with("geo_") =>
        {
            Err(OpenSearchError::unsupported_construct(
                kind_name,
                format!("aggregation [{kind_name}] of [{name}] is not supported"),
            ))
        }
        _ if NOT_YET_PLANNED_KINDS.contains(&kind_name) => {
            Err(OpenSearchError::unplanned_aggregation_type(name, kind_name))
        }
        _ => Err(OpenSearchError::unsupported_construct(
            kind_name,
            format!("unknown aggregation type [{kind_name}] in [{name}]"),
        )),
    }
}

fn parse_date_histogram(name: &str, value: &Value) -> Result<DateHistogramSpec, OpenSearchError> {
    let body = expect_object(name, "date_histogram", value)?;
    let mut field = None;
    let mut interval = None;
    let mut time_zone = None;
    let mut offset_micros = 0;
    let mut min_doc_count = 0;
    let mut extended_bounds = None;
    let mut format = None;
    let mut missing = None;
    for (key, value) in body {
        match key.as_str() {
            "field" => field = Some(expect_string(name, key, value)?.to_string()),
            "calendar_interval" => {
                let text = expect_string(name, key, value)?;
                let unit = parse_calendar_unit(text).ok_or_else(|| {
                    OpenSearchError::illegal_argument(format!(
                        "[calendar_interval] [{text}] of [{name}] is not one calendar unit"
                    ))
                })?;
                set_interval(name, &mut interval, HistogramInterval::Calendar(unit))?;
            }
            "fixed_interval" => {
                let text = expect_string(name, key, value)?;
                let duration = parse_fixed_duration(text).ok_or_else(|| {
                    OpenSearchError::illegal_argument(format!(
                        "[fixed_interval] [{text}] of [{name}] is not a positive count followed by ms, s, m, h or d"
                    ))
                })?;
                set_interval(name, &mut interval, HistogramInterval::Fixed(duration))?;
            }
            "interval" => {
                let interval_value = classify_legacy_interval(expect_string(name, key, value)?)?;
                set_interval(name, &mut interval, interval_value)?;
            }
            "time_zone" => time_zone = Some(parse_time_zone(expect_string(name, key, value)?)?),
            "offset" => offset_micros = parse_offset(name, expect_string(name, key, value)?)?,
            "min_doc_count" => min_doc_count = expect_count(name, key, value)?,
            "extended_bounds" => extended_bounds = Some(parse_extended_bounds(name, value)?),
            "format" => format = Some(DateRenderFormat::parse(expect_string(name, key, value)?)?),
            "missing" => {
                read_epoch_millis(name, key, value)?;
                missing = Some(value.clone());
            }
            ignored if IGNORED_EXECUTION_KEYS.contains(&ignored) => {}
            unknown => {
                return Err(OpenSearchError::unsupported_construct(
                    unknown,
                    format!("[date_histogram] of [{name}] does not support [{unknown}]"),
                ));
            }
        }
    }
    Ok(DateHistogramSpec {
        field: field
            .ok_or_else(|| OpenSearchError::parsing(format!("[date_histogram] of [{name}] requires [field]")))?,
        interval: interval.ok_or_else(|| {
            OpenSearchError::illegal_argument(format!(
                "[date_histogram] of [{name}] requires one of [interval], [fixed_interval] or [calendar_interval]"
            ))
        })?,
        time_zone,
        offset_micros,
        min_doc_count,
        extended_bounds,
        format,
        missing,
    })
}

/// Record the one interval key a `date_histogram` may carry.
fn set_interval(
    name: &str,
    slot: &mut Option<HistogramInterval>,
    interval: HistogramInterval,
) -> Result<(), OpenSearchError> {
    if slot.is_some() {
        return Err(OpenSearchError::illegal_argument(format!(
            "[date_histogram] of [{name}] carries more than one of [interval], [fixed_interval] and [calendar_interval]"
        )));
    }
    *slot = Some(interval);
    Ok(())
}

/// Classify a legacy `interval`: one calendar unit (`1d`, `day`, ...) is calendar; any other
/// duration (`7d`, `10m`, `100ms`, ...) is fixed.
///
/// # Errors
///
/// `400 illegal_argument_exception` for a text that is neither.
pub(crate) fn classify_legacy_interval(text: &str) -> Result<HistogramInterval, OpenSearchError> {
    if let Some(unit) = parse_calendar_unit(text) {
        return Ok(HistogramInterval::Calendar(unit));
    }
    parse_fixed_duration(text).map(HistogramInterval::Fixed).ok_or_else(|| {
        OpenSearchError::illegal_argument(format!(
            "[interval] [{text}] is neither one calendar unit nor a positive count followed by ms, s, m, h or d"
        ))
    })
}

fn parse_calendar_unit(text: &str) -> Option<CalendarUnit> {
    match text {
        "1s" | "second" => Some(CalendarUnit::Second),
        "1m" | "minute" => Some(CalendarUnit::Minute),
        "1h" | "hour" => Some(CalendarUnit::Hour),
        "1d" | "day" => Some(CalendarUnit::Day),
        "1w" | "week" => Some(CalendarUnit::Week),
        "1M" | "month" => Some(CalendarUnit::Month),
        "1q" | "quarter" => Some(CalendarUnit::Quarter),
        "1y" | "year" => Some(CalendarUnit::Year),
        _ => None,
    }
}

fn parse_fixed_duration(text: &str) -> Option<Duration> {
    let micros = parse_duration_micros(text, false)?;
    u64::try_from(micros).ok().map(Duration::from_micros)
}

/// A count followed by `ms`, `s`, `m`, `h` or `d`, in microseconds. A zero count is accepted
/// only when `is_zero_allowed`.
fn parse_duration_micros(text: &str, is_zero_allowed: bool) -> Option<i64> {
    let unit_start = text.find(|character: char| !character.is_ascii_digit())?;
    let (digits, unit) = text.split_at(unit_start);
    let count: i64 = digits.parse().ok()?;
    if count == 0 && !is_zero_allowed {
        return None;
    }
    let unit_micros: i64 = match unit {
        "ms" => MICROS_PER_MILLISECOND,
        "s" => MICROS_PER_SECOND,
        "m" => MICROS_PER_MINUTE,
        "h" => MICROS_PER_HOUR,
        "d" => MICROS_PER_DAY,
        _ => return None,
    };
    count.checked_mul(unit_micros)
}

/// `offset`: an optional sign and a duration, in microseconds.
fn parse_offset(name: &str, text: &str) -> Result<i64, OpenSearchError> {
    let (sign, magnitude) = text
        .strip_prefix('-')
        .map_or_else(|| (1, text.strip_prefix('+').unwrap_or(text)), |rest| (-1, rest));
    parse_duration_micros(magnitude, true)
        .map(|micros| sign * micros)
        .ok_or_else(|| {
            OpenSearchError::illegal_argument(format!(
                "[offset] [{text}] of [{name}] is not a signed duration such as +6h or -30m"
            ))
        })
}

fn parse_extended_bounds(name: &str, value: &Value) -> Result<(i64, i64), OpenSearchError> {
    let bounds = expect_object(name, "extended_bounds", value)?;
    let mut min = None;
    let mut max = None;
    for (key, bound) in bounds {
        match key.as_str() {
            "min" => min = Some(read_epoch_millis(name, "extended_bounds.min", bound)?),
            "max" => max = Some(read_epoch_millis(name, "extended_bounds.max", bound)?),
            unknown => {
                return Err(OpenSearchError::unsupported_construct(
                    unknown,
                    format!("[extended_bounds] of [{name}] does not support [{unknown}]"),
                ));
            }
        }
    }
    match (min, max) {
        (Some(min), Some(max)) if min <= max => Ok((min, max)),
        (Some(min), Some(max)) => Err(OpenSearchError::illegal_argument(format!(
            "[extended_bounds.min] [{min}] of [{name}] is greater than [extended_bounds.max] [{max}]"
        ))),
        _ => Err(OpenSearchError::parsing(format!(
            "[extended_bounds] of [{name}] needs both [min] and [max]"
        ))),
    }
}

/// Epoch milliseconds written as a JSON integer or a string of ASCII digits.
///
/// # Errors
///
/// `400 parsing_exception` naming `key` for any other value.
pub(super) fn read_epoch_millis(name: &str, key: &str, value: &Value) -> Result<i64, OpenSearchError> {
    let millis = match value {
        Value::Number(number) => number.as_i64(),
        Value::String(text) if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) => text.parse().ok(),
        _ => None,
    };
    millis.ok_or_else(|| {
        OpenSearchError::parsing(format!(
            "[{key}] of [{name}] must be epoch milliseconds, a JSON integer or a string of digits"
        ))
    })
}

fn expect_object<'a>(name: &str, key: &str, value: &'a Value) -> Result<&'a Map<String, Value>, OpenSearchError> {
    value
        .as_object()
        .ok_or_else(|| OpenSearchError::parsing(format!("[{key}] of [{name}] must be an object")))
}

fn expect_string<'a>(name: &str, key: &str, value: &'a Value) -> Result<&'a str, OpenSearchError> {
    value
        .as_str()
        .ok_or_else(|| OpenSearchError::parsing(format!("[{key}] of [{name}] must be a string")))
}

fn expect_count(name: &str, key: &str, value: &Value) -> Result<u64, OpenSearchError> {
    value
        .as_u64()
        .ok_or_else(|| OpenSearchError::parsing(format!("[{key}] of [{name}] must be a non-negative integer")))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::http::StatusCode;
    use serde_json::json;

    use super::{
        AggregationKind, AggregationTree, CalendarUnit, DateHistogramSpec, HistogramInterval, classify_legacy_interval,
        parse_aggregations,
    };
    use crate::opensearch::error::render_envelope;

    /// The spec of the first top-level aggregation, which the test wrote as a `date_histogram`.
    fn read_date_histogram_spec(tree: &AggregationTree) -> &DateHistogramSpec {
        match &tree.aggregations[0].kind {
            AggregationKind::DateHistogram(spec) => spec,
        }
    }

    #[tokio::test]
    async fn legacy_intervals_classify_single_calendar_units_as_calendar_and_others_as_fixed() {
        let calendar = [
            ("1s", CalendarUnit::Second),
            ("1m", CalendarUnit::Minute),
            ("1h", CalendarUnit::Hour),
            ("1d", CalendarUnit::Day),
            ("1w", CalendarUnit::Week),
            ("1M", CalendarUnit::Month),
            ("1q", CalendarUnit::Quarter),
            ("1y", CalendarUnit::Year),
            ("second", CalendarUnit::Second),
            ("minute", CalendarUnit::Minute),
            ("hour", CalendarUnit::Hour),
            ("day", CalendarUnit::Day),
            ("week", CalendarUnit::Week),
            ("month", CalendarUnit::Month),
            ("quarter", CalendarUnit::Quarter),
            ("year", CalendarUnit::Year),
        ];
        for (text, unit) in calendar {
            assert_eq!(
                classify_legacy_interval(text).expect(text),
                HistogramInterval::Calendar(unit),
                "{text} is one calendar unit"
            );
        }
        let fixed = [
            ("7d", 7 * 86_400_000_000),
            ("10m", 600_000_000),
            ("30d", 30 * 86_400_000_000),
            ("100ms", 100_000),
            ("1ms", 1_000),
            ("90s", 90_000_000),
            ("2h", 7_200_000_000),
        ];
        for (text, micros) in fixed {
            assert_eq!(
                classify_legacy_interval(text).expect(text),
                HistogramInterval::Fixed(Duration::from_micros(micros)),
                "{text} is a fixed duration"
            );
        }
        for text in ["", "0s", "1.5h", "2w", "2M", "1x", "h", "-1h"] {
            let error = classify_legacy_interval(text).expect_err(text);

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{text}");
        }
    }

    #[tokio::test]
    async fn an_out_of_scope_aggregation_is_a_parsing_exception_naming_it() {
        for kind in [
            "significant_terms",
            "auto_date_histogram",
            "geohash_grid",
            "geo_bounds",
            "ip_range",
            "extended_stats_bucket",
        ] {
            let error = parse_aggregations(&json!({"2": {kind: {"field": "service_name"}}})).expect_err(kind);

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{kind}");
            assert_eq!(body["error"]["type"], "parsing_exception", "{kind}");
            assert_eq!(body["error"]["construct"], kind);
        }
    }

    #[tokio::test]
    async fn an_unknown_aggregation_type_is_a_parsing_exception_naming_it() {
        let error = parse_aggregations(&json!({"2": {"date_histograms": {"field": "timestamp"}}}))
            .expect_err("date_histograms is no aggregation type");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
        assert_eq!(body["error"]["construct"], "date_histograms");
    }

    #[tokio::test]
    async fn an_aggregation_served_later_is_a_400_naming_it() {
        let error = parse_aggregations(&json!({"1": {"avg": {"field": "duration_micros"}}}))
            .expect_err("avg is not planned yet");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(body["error"]["aggregation"], "1");
        assert_eq!(body["error"]["aggregation_type"], "avg");
    }

    #[tokio::test]
    async fn an_unknown_key_in_a_date_histogram_is_a_parsing_exception() {
        let error = parse_aggregations(&json!({
            "2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "keyed": true}}
        }))
        .expect_err("keyed is not accepted");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
        assert_eq!(body["error"]["construct"], "keyed");
    }

    #[tokio::test]
    async fn malformed_aggregation_bodies_are_400s_of_their_kind() {
        // Each body with the error type it is refused with and the `construct` it names, if any.
        let cases = [
            (json!([]), "parsing_exception", None),
            (json!({"2": "date_histogram"}), "parsing_exception", None),
            (json!({"2": {}}), "parsing_exception", None),
            (json!({"2": {"aggs": {}}}), "parsing_exception", None),
            (
                json!({"2": {
                    "date_histogram": {"field": "timestamp", "fixed_interval": "1h"},
                    "avg": {"field": "duration_micros"}
                }}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {
                    "date_histogram": {"field": "timestamp", "fixed_interval": "1h"},
                    "aggs": {},
                    "aggregations": {}
                }}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h"}, "aggs": []}}),
                "parsing_exception",
                None,
            ),
            (json!({"2": {"date_histogram": "timestamp"}}), "parsing_exception", None),
            (
                json!({"2": {"date_histogram": {"fixed_interval": "1h"}}}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {"field": 7, "fixed_interval": "1h"}}}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {"field": "timestamp", "calendar_interval": 1}}}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "min_doc_count": -1}}}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "extended_bounds": 5}}}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {
                    "field": "timestamp", "fixed_interval": "1h", "extended_bounds": {"min": 1}
                }}}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {
                    "field": "timestamp", "fixed_interval": "1h", "extended_bounds": {"min": 1, "max": 2, "step": 1}
                }}}),
                "parsing_exception",
                Some("step"),
            ),
            (
                json!({"2": {"date_histogram": {
                    "field": "timestamp", "fixed_interval": "1h", "extended_bounds": {"min": "2026-10-08", "max": 2}
                }}}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "missing": 1.5}}}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "missing": "-5"}}}),
                "parsing_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "offset": "6h+"}}}),
                "illegal_argument_exception",
                None,
            ),
            (
                json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "offset": "+-6h"}}}),
                "illegal_argument_exception",
                None,
            ),
        ];
        for (body, error_type, construct) in cases {
            let error = parse_aggregations(&body).expect_err("refused");

            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(envelope["error"]["type"], error_type, "{body}");
            assert_eq!(
                envelope["error"].get("construct").and_then(serde_json::Value::as_str),
                construct,
                "{body}"
            );
        }
    }

    #[test]
    fn an_absent_min_doc_count_means_zero() {
        let absent = parse_aggregations(&json!({
            "2": {"date_histogram": {"field": "timestamp", "calendar_interval": "1d", "execution_hint": "map"}}
        }))
        .expect("a date_histogram without min_doc_count");
        let explicit = parse_aggregations(&json!({
            "2": {"date_histogram": {"field": "timestamp", "calendar_interval": "1d", "min_doc_count": 1}}
        }))
        .expect("a date_histogram with min_doc_count");

        assert_eq!(read_date_histogram_spec(&absent).min_doc_count, 0);
        assert_eq!(read_date_histogram_spec(&explicit).min_doc_count, 1);
    }

    #[test]
    fn date_histogram_options_parse_into_their_spec() {
        let tree = parse_aggregations(&json!({
            "2": {
                "date_histogram": {
                    "field": "timestamp",
                    "fixed_interval": "30m",
                    "time_zone": "Asia/Kolkata",
                    "offset": "-15m",
                    "extended_bounds": {"min": "1791453600000", "max": 1_791_460_800_000_i64},
                    "format": "epoch_millis",
                    "missing": 1_791_453_600_000_i64
                },
                "aggs": {}
            }
        }))
        .expect("a full date_histogram");

        let spec = read_date_histogram_spec(&tree);
        assert_eq!(tree.aggregations[0].name, "2");
        assert!(tree.aggregations[0].children.is_empty());
        assert_eq!(spec.field, "timestamp");
        assert_eq!(spec.interval, HistogramInterval::Fixed(Duration::from_mins(30)));
        assert!(spec.time_zone.is_some());
        assert_eq!(spec.offset_micros, -900_000_000);
        assert_eq!(spec.extended_bounds, Some((1_791_453_600_000, 1_791_460_800_000)));
        assert!(spec.format.is_some());
        assert_eq!(spec.missing, Some(json!(1_791_453_600_000_i64)));
    }

    #[tokio::test]
    async fn interval_keys_are_exclusive_and_one_is_required() {
        for body in [
            json!({"2": {"date_histogram": {"field": "timestamp"}}}),
            json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "calendar_interval": "1d"}}}),
            json!({"2": {"date_histogram": {"field": "timestamp", "calendar_interval": "2d"}}}),
            json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1w"}}}),
            json!({"2": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "extended_bounds": {"min": 2, "max": 1}}}}),
        ] {
            let error = parse_aggregations(&body).expect_err("refused");
            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(envelope["error"]["type"], "illegal_argument_exception", "{body}");
        }
    }
}
