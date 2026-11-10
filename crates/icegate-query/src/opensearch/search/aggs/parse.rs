//! The `aggs` / `aggregations` object of a search request, parsed into an [`AggregationTree`].
//!
//! Each kind lists the keys it accepts, and any other key is a `parsing_exception` whose
//! `construct` names it: an option ignored without a word would answer other buckets than the
//! client asked for. The keys of [`IGNORED_EXECUTION_KEYS`] tune shard-level execution, which a
//! single-shard listener does not have, so every kind accepts and ignores them. A kind outside
//! IceGate's aggregation surface, or a name that is no aggregation type, is a `parsing_exception`
//! whose `construct` names it.

use std::fmt::Display;
use std::time::Duration;

use serde_json::{Map, Value};

use super::metrics::{ExtendedStatsSpec, MetricSpec, parse_extended_stats};
use super::multi_bucket::{
    DateMathRange, DateRangeBound, DateRangeBucketsSpec, FiltersBuckets, FiltersSpec, NumericRange, RangeBucketsSpec,
};
use super::percentiles::{
    PercentileRanksSpec, PercentilesSpec, parse_percentile_ranks, parse_percentiles, split_percent_path,
};
use super::pipeline::{
    BucketScriptSpec, DerivativeSpec, MovingAvgSpec, MovingFnSpec, PipelineSpec, SerialDiffSpec, parse_bucket_script,
    parse_cumulative_sum, parse_derivative, parse_moving_avg, parse_moving_fn, parse_serial_diff,
    parse_sibling_pipeline,
};
use super::terms::{TermsSpec, parse_terms};
use super::top_hits::{TopHitsSpec, parse_top_hits};
use crate::opensearch::catalog::CrossRowField;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::script::{MetricScript, read_script_source, recognize_metric_script};
use crate::opensearch::search::doc_id::{
    MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MILLISECOND, MICROS_PER_MINUTE, MICROS_PER_SECOND,
};
use crate::opensearch::search::hits::DateRenderFormat;
use crate::query_dsl::ast::Query;
use crate::query_dsl::date_math::{TimeZoneSpec, parse_time_zone};
use crate::query_dsl::literal::parse_java_double;
use crate::query_dsl::parse::parse_query;

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
    /// One bucket per distinct value of a field.
    Terms(TermsSpec),
    /// Fixed-width buckets of a numeric field.
    Histogram(HistogramSpec),
    /// The mean of the bucket's values.
    Avg(MetricSpec),
    /// The sum of the bucket's values; a bucket without values renders 0.
    Sum(MetricSpec),
    /// The smallest value; over a date, epoch milliseconds plus `value_as_string`.
    Min(MetricSpec),
    /// The largest value; over a date, epoch milliseconds plus `value_as_string`.
    Max(MetricSpec),
    /// How many values of the field the bucket holds.
    ValueCount(MetricSpec),
    /// `count`, `min`, `max`, `avg` and `sum` together.
    Stats(MetricSpec),
    /// The exact number of distinct values of the field in the bucket.
    Cardinality(MetricSpec),
    /// Approximate quantiles of the bucket's values.
    Percentiles(PercentilesSpec),
    /// Exact shares, in percent, of the bucket's values at or below given values.
    PercentileRanks(PercentileRanksSpec),
    /// Moments, variances and deviation bounds of the bucket's values.
    ExtendedStats(ExtendedStatsSpec),
    /// The first rows of the bucket in a requested order, rendered as `_search` hits; boxed, since
    /// it holds a whole search request.
    TopHits(Box<TopHitsSpec>),
    /// Numeric `[from, to)` buckets; a row counts in every range it lies in.
    Range(RangeBucketsSpec),
    /// Date-math `[from, to)` buckets resolved against the request's `now`.
    DateRange(DateRangeBucketsSpec),
    /// One bucket per query; a row counts in every bucket it matches.
    Filters(FiltersSpec),
    /// The single bucket of the rows the query matches.
    Filter(Box<Query>),
    /// `cumulative_sum`: the running total across the parent histogram's buckets.
    CumulativeSum(PipelineSpec),
    /// `derivative`: the difference to the previous bucket.
    Derivative(DerivativeSpec),
    /// `serial_diff`: the difference to the bucket `lag` positions earlier.
    SerialDiff(SerialDiffSpec),
    /// `moving_fn`: a recognized moving function over preceding buckets.
    MovingFn(MovingFnSpec),
    /// Legacy `moving_avg`: a moving model over preceding buckets.
    MovingAvg(MovingAvgSpec),
    /// `bucket_script`: arithmetic over values of the same bucket.
    BucketScript(BucketScriptSpec),
    /// `avg_bucket`: the mean over a sibling's buckets.
    AvgBucket(PipelineSpec),
    /// `sum_bucket`: the sum over a sibling's buckets.
    SumBucket(PipelineSpec),
    /// `min_bucket`: the minimum over a sibling's buckets and its keys.
    MinBucket(PipelineSpec),
    /// `max_bucket`: the maximum over a sibling's buckets and its keys.
    MaxBucket(PipelineSpec),
}

impl AggregationKind {
    /// The field the aggregation reads; `None` for `filter` and `filters`, which read queries,
    /// `top_hits`, which reads whole rows, and the pipelines, which read the rendered tree.
    pub(crate) fn field(&self) -> Option<&str> {
        match self {
            Self::DateHistogram(spec) => Some(&spec.field),
            Self::Terms(spec) => Some(&spec.field),
            Self::Histogram(spec) => Some(&spec.field),
            Self::Avg(spec)
            | Self::Sum(spec)
            | Self::Min(spec)
            | Self::Max(spec)
            | Self::ValueCount(spec)
            | Self::Stats(spec)
            | Self::Cardinality(spec) => Some(&spec.field),
            Self::Percentiles(PercentilesSpec { metric, .. })
            | Self::PercentileRanks(PercentileRanksSpec { metric, .. })
            | Self::ExtendedStats(ExtendedStatsSpec { metric, .. }) => Some(&metric.field),
            Self::Range(spec) => Some(&spec.field),
            Self::DateRange(spec) => Some(&spec.field),
            Self::Filters(_)
            | Self::Filter(_)
            | Self::TopHits(_)
            | Self::CumulativeSum(_)
            | Self::Derivative(_)
            | Self::SerialDiff(_)
            | Self::MovingFn(_)
            | Self::MovingAvg(_)
            | Self::BucketScript(_)
            | Self::AvgBucket(_)
            | Self::SumBucket(_)
            | Self::MinBucket(_)
            | Self::MaxBucket(_) => None,
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

/// A `histogram` request.
#[derive(Debug, PartialEq)]
pub(crate) struct HistogramSpec {
    /// The numeric field bucketed.
    pub(crate) field: String,
    /// The bucket width, positive and finite. A value's bucket is `floor(value / interval)`,
    /// rendered at `bucket × interval`.
    pub(crate) interval: f64,
    /// Buckets with fewer documents are not answered. `0`, the default, also answers the empty
    /// buckets between the first and the last bucket, widened to `extended_bounds`.
    pub(crate) min_doc_count: u64,
    /// `(min, max)` the answered buckets reach when `min_doc_count` is `0`; the bounds widen the
    /// range and never narrow it.
    pub(crate) extended_bounds: Option<(f64, f64)>,
}

/// A `terms` order key naming a metric: a sub-aggregation name and, for a multi-value metric,
/// the statistic after the first `.`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MetricPath {
    /// The name of the ordering sub-aggregation (`1`, `2-orderAgg`, `score_stats`).
    pub(crate) aggregation: String,
    /// The statistic of a multi-value metric (`max` in `score_stats.max`); `None` for a
    /// single-value metric.
    pub(crate) statistic: Option<String>,
    /// The percent a `<id>[p]` or `<id>['p']` path selects, in key form (`"50.0"`); `None` for
    /// every other form (an `<id>.p` path keeps `p` in `statistic`).
    pub(crate) percent: Option<String>,
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
/// value.
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
        "terms" => Ok(AggregationKind::Terms(parse_terms(name, value)?)),
        "histogram" => Ok(AggregationKind::Histogram(parse_histogram(name, value)?)),
        "avg" => Ok(AggregationKind::Avg(parse_metric(name, kind_name, value, true)?)),
        "sum" => Ok(AggregationKind::Sum(parse_metric(name, kind_name, value, true)?)),
        "min" => Ok(AggregationKind::Min(parse_metric(name, kind_name, value, true)?)),
        "max" => Ok(AggregationKind::Max(parse_metric(name, kind_name, value, true)?)),
        "value_count" => Ok(AggregationKind::ValueCount(parse_metric(
            name, kind_name, value, false,
        )?)),
        "stats" => Ok(AggregationKind::Stats(parse_metric(name, kind_name, value, false)?)),
        "cardinality" => Ok(AggregationKind::Cardinality(parse_metric(
            name, kind_name, value, false,
        )?)),
        "percentiles" => Ok(AggregationKind::Percentiles(parse_percentiles(name, value)?)),
        "percentile_ranks" => Ok(AggregationKind::PercentileRanks(parse_percentile_ranks(name, value)?)),
        "extended_stats" => Ok(AggregationKind::ExtendedStats(parse_extended_stats(name, value)?)),
        "top_hits" => Ok(AggregationKind::TopHits(Box::new(parse_top_hits(name, value)?))),
        "range" => Ok(AggregationKind::Range(parse_range(name, value)?)),
        "date_range" => Ok(AggregationKind::DateRange(parse_date_range(name, value)?)),
        "filters" => Ok(AggregationKind::Filters(parse_filters(name, value)?)),
        "filter" => Ok(AggregationKind::Filter(Box::new(parse_query(value)?))),
        "cumulative_sum" => Ok(AggregationKind::CumulativeSum(parse_cumulative_sum(name, value)?)),
        "derivative" => Ok(AggregationKind::Derivative(parse_derivative(name, value)?)),
        "serial_diff" => Ok(AggregationKind::SerialDiff(parse_serial_diff(name, value)?)),
        "moving_fn" => Ok(AggregationKind::MovingFn(parse_moving_fn(name, value)?)),
        "moving_avg" => Ok(AggregationKind::MovingAvg(parse_moving_avg(name, value)?)),
        "bucket_script" => Ok(AggregationKind::BucketScript(parse_bucket_script(name, value)?)),
        "avg_bucket" => Ok(AggregationKind::AvgBucket(parse_sibling_pipeline(
            name, kind_name, value,
        )?)),
        "sum_bucket" => Ok(AggregationKind::SumBucket(parse_sibling_pipeline(
            name, kind_name, value,
        )?)),
        "min_bucket" => Ok(AggregationKind::MinBucket(parse_sibling_pipeline(
            name, kind_name, value,
        )?)),
        "max_bucket" => Ok(AggregationKind::MaxBucket(parse_sibling_pipeline(
            name, kind_name, value,
        )?)),
        _ if OUT_OF_SCOPE_KINDS.contains(&kind_name)
            || OUT_OF_SCOPE_PIPELINES.contains(&kind_name)
            || kind_name.starts_with("geo_") =>
        {
            Err(OpenSearchError::unsupported_construct(
                kind_name,
                format!("aggregation [{kind_name}] of [{name}] is not supported"),
            ))
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
            "extended_bounds" => extended_bounds = Some(parse_extended_bounds(name, value, read_epoch_millis)?),
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

/// `extended_bounds`: both `min` and `max`, each read by `read_bound`, `min <= max`.
///
/// # Errors
///
/// `400 parsing_exception` for a malformed object, a missing bound, an unknown key (named under
/// `construct`), and the errors of `read_bound`; `400 illegal_argument_exception` when `min`
/// exceeds `max`.
fn parse_extended_bounds<T: PartialOrd + Display>(
    name: &str,
    value: &Value,
    read_bound: impl Fn(&str, &str, &Value) -> Result<T, OpenSearchError>,
) -> Result<(T, T), OpenSearchError> {
    let bounds = expect_object(name, "extended_bounds", value)?;
    let mut min = None;
    let mut max = None;
    for (key, bound) in bounds {
        match key.as_str() {
            "min" => min = Some(read_bound(name, "extended_bounds.min", bound)?),
            "max" => max = Some(read_bound(name, "extended_bounds.max", bound)?),
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

/// Parse a `terms` order key that names a metric: `<id>`, `<id>.<stat>`, `<id>[<percent>]`,
/// `<id>['<percent>']`, or `<id>-orderAgg` (an ordinary name). The statistic is what follows the
/// first `.`; a bracketed percent is kept in key form ([`split_percent_path`]).
///
/// # Errors
///
/// `400 parsing_exception` for an empty name or statistic, a `>` path into a deeper aggregation
/// (a level orders only by a direct metric sub-aggregation), and a bracket that is unclosed or
/// holds no finite number.
pub(crate) fn parse_metric_path(text: &str) -> Result<MetricPath, OpenSearchError> {
    if text.contains('>') {
        return Err(OpenSearchError::parsing(format!(
            "order path [{text}] names a nested aggregation; order by a direct metric sub-aggregation"
        )));
    }
    if let Some((aggregation, percent)) = split_percent_path(text)? {
        return Ok(MetricPath {
            aggregation: aggregation.to_string(),
            statistic: None,
            percent: Some(percent),
        });
    }
    let (aggregation, statistic) = text
        .split_once('.')
        .map_or((text, None), |(aggregation, statistic)| (aggregation, Some(statistic)));
    if aggregation.is_empty() || statistic.is_some_and(str::is_empty) {
        return Err(OpenSearchError::parsing(format!(
            "order path [{text}] must name a sub-aggregation and, after a dot, a statistic"
        )));
    }
    Ok(MetricPath {
        aggregation: aggregation.to_string(),
        statistic: statistic.map(str::to_string),
        percent: None,
    })
}

/// A plain metric (`avg`, `sum`, `min`, `max`, `value_count`, `stats`, `cardinality`): `field`,
/// plus `missing` and a value `script` when `accepts_value_inputs` (`avg`, `sum`, `min`, `max`).
/// A metric running the trace-list latency script reads the field the script names, so it needs
/// no `field`.
///
/// # Errors
///
/// `400 parsing_exception` for a body that is not an object, a missing `field` (without the
/// latency script), a value of the wrong JSON type, a malformed `script` member, and an unknown
/// key (named under `construct`); `400 illegal_argument_exception` for a script that is neither
/// arithmetic nor the latency script, and, with the latency script, for a `missing` or a `field`
/// other than the one the script reads.
fn parse_metric(
    name: &str,
    kind_name: &str,
    value: &Value,
    accepts_value_inputs: bool,
) -> Result<MetricSpec, OpenSearchError> {
    let body = expect_object(name, kind_name, value)?;
    let mut field = None;
    let mut missing = None;
    let mut value_script = None;
    for (key, value) in body {
        match key.as_str() {
            "field" => field = Some(expect_string(name, key, value)?.to_string()),
            "missing" if accepts_value_inputs => missing = Some(read_lenient_number(name, key, value)?),
            "script" if accepts_value_inputs => value_script = Some(read_value_script(name, value)?),
            ignored if IGNORED_EXECUTION_KEYS.contains(&ignored) => {}
            unknown => return Err(report_unsupported_key(kind_name, name, unknown)),
        }
    }
    Ok(MetricSpec {
        field: resolve_metric_field(kind_name, name, field, missing, value_script.as_ref())?,
        missing,
        value_script,
    })
}

/// The value `script` of the metric aggregation `name`: the trace-list latency script, or
/// arithmetic over `_value` and decimal literals.
///
/// # Errors
///
/// `400 parsing_exception` for a malformed `script` member; `400 illegal_argument_exception`
/// naming any other script under `script`.
pub(super) fn read_value_script(name: &str, value: &Value) -> Result<MetricScript, OpenSearchError> {
    recognize_metric_script(read_script_source(name, value)?)
}

/// The field the metric aggregation `name` of type `kind_name` reads: the `field` the body names,
/// or, under the trace-list latency script, the root duration that script reads.
///
/// The latency script computes its value from `traceGroupFields.durationInNanos` alone and
/// answers 0 for a span without a root; another `field` or a `missing` would make it answer a
/// value the script does not compute, so both are refused rather than applied.
///
/// # Errors
///
/// `400 parsing_exception` for a missing `field` without the latency script; beside the latency
/// script, `400 illegal_argument_exception` for a `missing` (`aggregation` and `key` metadata)
/// and for a `field` other than the root duration (`aggregation` and `field` metadata).
pub(super) fn resolve_metric_field(
    kind_name: &str,
    name: &str,
    field: Option<String>,
    missing: Option<f64>,
    value_script: Option<&MetricScript>,
) -> Result<String, OpenSearchError> {
    let Some(MetricScript::TraceListLatency) = value_script else {
        return field.ok_or_else(|| report_missing_key(kind_name, name, "field"));
    };
    let latency_field = CrossRowField::DurationInNanos.client_name();
    if missing.is_some() {
        return Err(OpenSearchError::reject_latency_script_missing(name));
    }
    match field {
        Some(named_field) if named_field != latency_field => Err(OpenSearchError::reject_latency_script_field(
            name,
            &named_field,
            latency_field,
        )),
        Some(named_field) => Ok(named_field),
        None => Ok(latency_field.to_string()),
    }
}

/// A `histogram`: `field`, `interval`, `min_doc_count` (absent means 0) and `extended_bounds`.
///
/// # Errors
///
/// `400 parsing_exception` for a malformed body, a missing `field` or `interval`, a value of the
/// wrong JSON type, and an unknown key (named under `construct`); `400
/// illegal_argument_exception` for an `interval` that is not positive and for `extended_bounds`
/// whose `min` exceeds its `max`.
fn parse_histogram(name: &str, value: &Value) -> Result<HistogramSpec, OpenSearchError> {
    let body = expect_object(name, "histogram", value)?;
    let mut field = None;
    let mut interval = None;
    let mut min_doc_count = 0;
    let mut extended_bounds = None;
    for (key, value) in body {
        match key.as_str() {
            "field" => field = Some(expect_string(name, key, value)?.to_string()),
            "interval" => interval = Some(read_strict_number(name, key, value)?),
            "min_doc_count" => min_doc_count = expect_count(name, key, value)?,
            "extended_bounds" => extended_bounds = Some(parse_extended_bounds(name, value, read_strict_number)?),
            ignored if IGNORED_EXECUTION_KEYS.contains(&ignored) => {}
            unknown => {
                return Err(OpenSearchError::unsupported_construct(
                    unknown,
                    format!("[histogram] of [{name}] does not support [{unknown}]"),
                ));
            }
        }
    }
    let interval =
        interval.ok_or_else(|| OpenSearchError::parsing(format!("[histogram] of [{name}] requires [interval]")))?;
    if interval <= 0.0 {
        return Err(OpenSearchError::illegal_argument(format!(
            "[interval] of [{name}] must be greater than 0, got [{interval}]"
        )));
    }
    Ok(HistogramSpec {
        field: field.ok_or_else(|| OpenSearchError::parsing(format!("[histogram] of [{name}] requires [field]")))?,
        interval,
        min_doc_count,
        extended_bounds,
    })
}

/// A finite JSON number or a string holding one. The contract takes both forms for a metric's
/// `missing` (and, in later kinds, for percents and moving-window sizes) only.
///
/// # Errors
///
/// `400 parsing_exception` naming `key` for any other value.
pub(super) fn read_lenient_number(name: &str, key: &str, value: &Value) -> Result<f64, OpenSearchError> {
    let number = match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => parse_java_double(text),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    };
    number.filter(|number| number.is_finite()).ok_or_else(|| {
        OpenSearchError::parsing(format!(
            "[{key}] of [{name}] must be a number or a numeric string, got [{value}]"
        ))
    })
}

/// A finite JSON number; a string, even a numeric one, is refused.
///
/// # Errors
///
/// `400 parsing_exception` naming `key` for any other value.
pub(super) fn read_strict_number(name: &str, key: &str, value: &Value) -> Result<f64, OpenSearchError> {
    value
        .as_f64()
        .filter(|number| number.is_finite())
        .ok_or_else(|| OpenSearchError::parsing(format!("[{key}] of [{name}] must be a number, got [{value}]")))
}

/// A `range`: `field`, a non-empty `ranges` array of `{from?, to?, key?}` with numeric bounds,
/// and `keyed` (absent means `false`).
///
/// # Errors
///
/// `400 parsing_exception` for a malformed body, a missing `field` or `ranges`, an empty
/// `ranges`, a value of the wrong JSON type (a numeric string bound included), and an unknown
/// key (named under `construct`).
fn parse_range(name: &str, value: &Value) -> Result<RangeBucketsSpec, OpenSearchError> {
    let body = expect_object(name, "range", value)?;
    let mut field = None;
    let mut ranges = None;
    let mut is_keyed = false;
    for (key, value) in body {
        match key.as_str() {
            "field" => field = Some(expect_string(name, key, value)?.to_string()),
            "ranges" => ranges = Some(read_ranges(name, value, parse_numeric_range)?),
            "keyed" => is_keyed = expect_flag(name, key, value)?,
            ignored if IGNORED_EXECUTION_KEYS.contains(&ignored) => {}
            unknown => return Err(report_unsupported_key("range", name, unknown)),
        }
    }
    Ok(RangeBucketsSpec {
        field: field.ok_or_else(|| OpenSearchError::parsing(format!("[range] of [{name}] requires [field]")))?,
        ranges: ranges.ok_or_else(|| OpenSearchError::parsing(format!("[range] of [{name}] requires [ranges]")))?,
        is_keyed,
    })
}

/// A `date_range`: `field`, a non-empty `ranges` array of `{from?, to?, key?}` whose bounds are
/// date strings or epoch-millisecond integers, `time_zone` and `format`.
///
/// # Errors
///
/// `400 parsing_exception` for a malformed body, a missing `field` or `ranges`, an empty
/// `ranges`, a bound that is neither a string nor an integer of the `i64` range, a value of the
/// wrong JSON type, an unknown time zone, and an unknown key (named under `construct`); `400
/// illegal_argument_exception` for an unknown `format`.
fn parse_date_range(name: &str, value: &Value) -> Result<DateRangeBucketsSpec, OpenSearchError> {
    let body = expect_object(name, "date_range", value)?;
    let mut field = None;
    let mut ranges = None;
    let mut time_zone = None;
    let mut format = None;
    for (key, value) in body {
        match key.as_str() {
            "field" => field = Some(expect_string(name, key, value)?.to_string()),
            "ranges" => ranges = Some(read_ranges(name, value, parse_date_math_range)?),
            "time_zone" => time_zone = Some(parse_time_zone(expect_string(name, key, value)?)?),
            "format" => format = Some(DateRenderFormat::parse(expect_string(name, key, value)?)?),
            ignored if IGNORED_EXECUTION_KEYS.contains(&ignored) => {}
            unknown => return Err(report_unsupported_key("date_range", name, unknown)),
        }
    }
    Ok(DateRangeBucketsSpec {
        field: field.ok_or_else(|| OpenSearchError::parsing(format!("[date_range] of [{name}] requires [field]")))?,
        ranges: ranges
            .ok_or_else(|| OpenSearchError::parsing(format!("[date_range] of [{name}] requires [ranges]")))?,
        time_zone,
        format,
    })
}

/// A `filters`: `filters` as a non-empty object of named queries (kept in request order) or a
/// non-empty array of anonymous ones.
///
/// # Errors
///
/// `400 parsing_exception` for a malformed body, a missing or empty `filters`, a query
/// [`parse_query`] refuses, and an unknown key (named under `construct`).
fn parse_filters(name: &str, value: &Value) -> Result<FiltersSpec, OpenSearchError> {
    let body = expect_object(name, "filters", value)?;
    let mut filters = None;
    for (key, value) in body {
        match key.as_str() {
            "filters" => {
                filters = Some(match value {
                    Value::Object(named) if !named.is_empty() => FiltersBuckets::Named(
                        named
                            .iter()
                            .map(|(bucket, query)| Ok((bucket.clone(), parse_query(query)?)))
                            .collect::<Result<Vec<_>, OpenSearchError>>()?,
                    ),
                    Value::Array(anonymous) if !anonymous.is_empty() => {
                        FiltersBuckets::Anonymous(anonymous.iter().map(parse_query).collect::<Result<Vec<_>, _>>()?)
                    }
                    Value::Null
                    | Value::Bool(_)
                    | Value::Number(_)
                    | Value::String(_)
                    | Value::Array(_)
                    | Value::Object(_) => {
                        return Err(OpenSearchError::parsing(format!(
                            "[filters] of [{name}] must be a non-empty object or array"
                        )));
                    }
                });
            }
            ignored if IGNORED_EXECUTION_KEYS.contains(&ignored) => {}
            unknown => return Err(report_unsupported_key("filters", name, unknown)),
        }
    }
    Ok(FiltersSpec {
        filters: filters
            .ok_or_else(|| OpenSearchError::parsing(format!("[filters] of [{name}] requires [filters]")))?,
    })
}

/// A non-empty `ranges` array, each entry an object parsed by `parse_entry`.
///
/// # Errors
///
/// `400 parsing_exception` for a value that is not a non-empty array of objects, and the errors
/// of `parse_entry`.
fn read_ranges<T>(
    name: &str,
    value: &Value,
    parse_entry: fn(&str, &Map<String, Value>) -> Result<T, OpenSearchError>,
) -> Result<Vec<T>, OpenSearchError> {
    let entries = value
        .as_array()
        .filter(|entries| !entries.is_empty())
        .ok_or_else(|| OpenSearchError::parsing(format!("[ranges] of [{name}] must be a non-empty array")))?;
    entries
        .iter()
        .map(|entry| parse_entry(name, expect_object(name, "ranges", entry)?))
        .collect()
}

/// One entry of a `range`'s `ranges`: numeric `from` and `to`, a string `key`.
///
/// # Errors
///
/// The errors of [`read_range_entry`].
fn parse_numeric_range(name: &str, entry: &Map<String, Value>) -> Result<NumericRange, OpenSearchError> {
    let (from, to, key) = read_range_entry(name, entry, read_strict_number)?;
    Ok(NumericRange { from, to, key })
}

/// One entry of a `date_range`'s `ranges`: `from` and `to` as [`read_date_bound`] reads them, a
/// string `key`.
///
/// # Errors
///
/// The errors of [`read_range_entry`].
fn parse_date_math_range(name: &str, entry: &Map<String, Value>) -> Result<DateMathRange, OpenSearchError> {
    let (from, to, key) = read_range_entry(name, entry, read_date_bound)?;
    Ok(DateMathRange { from, to, key })
}

/// The `from`, `to` and `key` of one `ranges` entry, each bound read by `read_bound`.
///
/// # Errors
///
/// The errors of `read_bound`; `400 parsing_exception` for a `key` that is not a string and an
/// unknown key (named under `construct`).
fn read_range_entry<B>(
    name: &str,
    entry: &Map<String, Value>,
    read_bound: fn(&str, &str, &Value) -> Result<B, OpenSearchError>,
) -> Result<(Option<B>, Option<B>, Option<String>), OpenSearchError> {
    let mut from = None;
    let mut to = None;
    let mut key = None;
    for (entry_key, value) in entry {
        match entry_key.as_str() {
            "from" => from = Some(read_bound(name, "ranges.from", value)?),
            "to" => to = Some(read_bound(name, "ranges.to", value)?),
            "key" => key = Some(expect_string(name, "ranges.key", value)?.to_string()),
            unknown => return Err(report_unsupported_key("ranges", name, unknown)),
        }
    }
    Ok((from, to, key))
}

/// The `400 parsing_exception` of a key the `kind_name` object of the aggregation `name` does not
/// accept, naming `key` under `construct`.
pub(super) fn report_unsupported_key(kind_name: &str, name: &str, key: &str) -> OpenSearchError {
    OpenSearchError::unsupported_construct(key, format!("[{kind_name}] of [{name}] does not support [{key}]"))
}

/// The `400 parsing_exception` of a key the `kind_name` aggregation `name` requires and lacks.
pub(super) fn report_missing_key(kind_name: &str, name: &str, key: &str) -> OpenSearchError {
    OpenSearchError::parsing(format!("[{kind_name}] of [{name}] requires [{key}]"))
}

/// A date bound as written: a string (a date, date math or epoch milliseconds), or a JSON
/// integer of epoch milliseconds.
///
/// # Errors
///
/// `400 parsing_exception` naming `key` for a fractional number, an integer outside the `i64`
/// range, and any other JSON type.
fn read_date_bound(name: &str, key: &str, value: &Value) -> Result<DateRangeBound, OpenSearchError> {
    let refuse_bound = || {
        OpenSearchError::parsing(format!(
            "[{key}] of [{name}] must be a date string or an integer of epoch milliseconds, got [{value}]"
        ))
    };
    match value {
        Value::String(text) => Ok(DateRangeBound::Expression(text.clone())),
        Value::Number(number) => number.as_i64().map(DateRangeBound::EpochMillis).ok_or_else(refuse_bound),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => Err(refuse_bound()),
    }
}

/// `value` as a boolean.
///
/// # Errors
///
/// `400 parsing_exception` naming `key` of `name` for any other value.
pub(super) fn expect_flag(name: &str, key: &str, value: &Value) -> Result<bool, OpenSearchError> {
    value
        .as_bool()
        .ok_or_else(|| OpenSearchError::parsing(format!("[{key}] of [{name}] must be a boolean")))
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

/// `value` as an object.
///
/// # Errors
///
/// `400 parsing_exception` naming `key` of `name` for any other value.
pub(super) fn expect_object<'a>(
    name: &str,
    key: &str,
    value: &'a Value,
) -> Result<&'a Map<String, Value>, OpenSearchError> {
    value
        .as_object()
        .ok_or_else(|| OpenSearchError::parsing(format!("[{key}] of [{name}] must be an object")))
}

/// `value` as a string.
///
/// # Errors
///
/// `400 parsing_exception` naming `key` of `name` for any other value.
pub(super) fn expect_string<'a>(name: &str, key: &str, value: &'a Value) -> Result<&'a str, OpenSearchError> {
    value
        .as_str()
        .ok_or_else(|| OpenSearchError::parsing(format!("[{key}] of [{name}] must be a string")))
}

/// `value` as a non-negative integer.
///
/// # Errors
///
/// `400 parsing_exception` naming `key` of `name` for any other value.
pub(super) fn expect_count(name: &str, key: &str, value: &Value) -> Result<u64, OpenSearchError> {
    value
        .as_u64()
        .ok_or_else(|| OpenSearchError::parsing(format!("[{key}] of [{name}] must be a non-negative integer")))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::http::StatusCode;
    use serde_json::{Value, json};

    use super::super::metrics::MetricSpec;
    use super::super::multi_bucket::{DateRangeBound, FiltersBuckets};
    use super::super::terms::{TermsFilter, TermsOrder, TermsSpec};
    use super::{
        AggregationKind, AggregationTree, CalendarUnit, DateHistogramSpec, HistogramInterval, HistogramSpec,
        MetricPath, classify_legacy_interval, parse_aggregations, parse_metric_path,
    };
    use crate::opensearch::error::{OpenSearchError, render_envelope};
    use crate::opensearch::script::MetricScript;
    use crate::opensearch::search::request::SortOrder;

    /// The spec of the first top-level aggregation, which the test wrote as a `date_histogram`.
    fn read_date_histogram_spec(tree: &AggregationTree) -> &DateHistogramSpec {
        let AggregationKind::DateHistogram(spec) = &tree.aggregations[0].kind else {
            panic!("the test wrote a date_histogram aggregation");
        };
        spec
    }

    /// The spec of the first top-level aggregation, which the test wrote as a `terms`.
    fn read_terms_spec(tree: &AggregationTree) -> &TermsSpec {
        let AggregationKind::Terms(spec) = &tree.aggregations[0].kind else {
            panic!("the test wrote a terms aggregation");
        };
        spec
    }

    #[tokio::test]
    async fn terms_accepts_size_order_include_exclude_missing_and_ignored_hints() {
        let tree = parse_aggregations(&json!({"levels": {"terms": {
            "field": "severity_text", "size": 500, "order": {"_key": "asc"},
            "include": "ERR.*", "exclude": ["DEBUG", "TRACE"], "missing": "unknown", "min_doc_count": 0,
            "execution_hint": "map", "shard_size": 10, "collect_mode": "breadth_first"
        }}}))
        .expect("a full terms aggregation");
        let defaults = parse_aggregations(&json!({"levels": {"terms": {"field": "severity_text"}}}))
            .expect("a terms aggregation with defaults");

        let spec = read_terms_spec(&tree);
        assert_eq!(spec.field, "severity_text");
        assert_eq!(spec.size, 500);
        assert!(matches!(spec.order.as_slice(), [TermsOrder::Key(SortOrder::Asc)]));
        let Some(TermsFilter::Pattern(include)) = &spec.include else {
            panic!("include is a pattern");
        };
        assert!(include.is_match("ERROR"));
        assert!(!include.is_match("AN ERROR"), "the pattern is anchored");
        assert!(
            matches!(&spec.exclude, Some(TermsFilter::Values(values)) if *values == vec![json!("DEBUG"), json!("TRACE")])
        );
        assert_eq!(spec.missing, Some(json!("unknown")));
        assert_eq!(spec.min_doc_count, 0);
        let defaults = read_terms_spec(&defaults);
        assert_eq!(defaults.size, 10);
        assert!(matches!(
            defaults.order.as_slice(),
            [TermsOrder::Count(SortOrder::Desc)]
        ));
        assert_eq!(defaults.min_doc_count, 1);
        assert!(defaults.include.is_none() && defaults.exclude.is_none() && defaults.missing.is_none());

        // Each refused body with the error type it is refused with.
        for (body, expected_type) in [
            (
                json!({"levels": {"terms": {"field": "severity_text", "size": 0}}}),
                "illegal_argument_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "show_term_doc_count_error": true}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "include": {"partition": 0, "num_partitions": 2}}}}),
                "illegal_argument_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "size": "10"}}}),
                "parsing_exception",
            ),
            (json!({"levels": {"terms": "severity_text"}}), "parsing_exception"),
            (json!({"levels": {"terms": {"size": 5}}}), "parsing_exception"),
            (
                json!({"levels": {"terms": {"field": "severity_text", "missing": ["unknown"]}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "missing": null}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "order": []}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "order": {}}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "order": "_count"}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "order": ["_count"]}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "order": {"_count": "up"}}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "include": 7}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "exclude": [{"value": "a"}]}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "include": "a&b"}}}),
                "parsing_exception",
            ),
            (
                json!({"levels": {"terms": {"field": "severity_text", "min_doc_count": -1}}}),
                "parsing_exception",
            ),
        ] {
            let error = parse_aggregations(&body).expect_err("refused");
            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(envelope["error"]["type"], expected_type, "{body}");
        }
    }

    #[test]
    fn terms_order_parses_as_an_object_or_an_array() {
        let object = parse_aggregations(&json!({"t": {"terms": {"field": "f", "order": {"_count": "asc"}}}}))
            .expect("an object order");
        let array = parse_aggregations(
            &json!({"t": {"terms": {"field": "f", "order": [{"_count": "DESC"}, {"_key": "asc"}]}}}),
        )
        .expect("an array order");

        assert!(matches!(
            read_terms_spec(&object).order.as_slice(),
            [TermsOrder::Count(SortOrder::Asc)]
        ));
        assert!(matches!(
            read_terms_spec(&array).order.as_slice(),
            [TermsOrder::Count(SortOrder::Desc), TermsOrder::Key(SortOrder::Asc)]
        ));
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

    /// The kind of the first top-level aggregation of `aggregations`.
    fn parse_first_kind(aggregations: &Value) -> Result<AggregationKind, OpenSearchError> {
        let mut tree = parse_aggregations(aggregations)?;
        Ok(tree.aggregations.swap_remove(0).kind)
    }

    /// Assert that `aggregations` is refused with a 400 whose error type is `expected_type`, and
    /// return the rendered `error` object.
    async fn assert_refusal(aggregations: Value, expected_type: &str) -> Value {
        let error = parse_first_kind(&aggregations).expect_err("the body must be refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{aggregations}");
        assert_eq!(body["error"]["type"], expected_type, "{aggregations}");
        body["error"].clone()
    }

    #[tokio::test]
    async fn numbers_and_numeric_strings_are_accepted_exactly_where_the_contract_allows() {
        // A metric's `missing` takes a JSON number or a numeric string.
        let Ok(AggregationKind::Avg(numeric_missing)) =
            parse_first_kind(&json!({"a": {"avg": {"field": "duration_micros", "missing": 0}}}))
        else {
            panic!("an avg with a numeric missing parses to Avg");
        };
        assert_eq!(
            numeric_missing,
            MetricSpec {
                field: "duration_micros".to_string(),
                missing: Some(0.0),
                value_script: None
            }
        );
        let Ok(AggregationKind::Max(textual_missing)) =
            parse_first_kind(&json!({"a": {"max": {"field": "duration_micros", "missing": "1.5"}}}))
        else {
            panic!("a max with a numeric-string missing parses to Max");
        };
        assert_eq!(
            textual_missing,
            MetricSpec {
                field: "duration_micros".to_string(),
                missing: Some(1.5),
                value_script: None
            }
        );
        let Ok(AggregationKind::Histogram(histogram)) = parse_first_kind(&json!({"h": {"histogram": {
            "field": "duration_micros", "interval": 100, "min_doc_count": 0,
            "extended_bounds": {"min": 0, "max": 600}
        }}})) else {
            panic!("a histogram parses to Histogram");
        };
        assert_eq!(
            histogram,
            HistogramSpec {
                field: "duration_micros".to_string(),
                interval: 100.0,
                min_doc_count: 0,
                extended_bounds: Some((0.0, 600.0)),
            }
        );
        let Ok(AggregationKind::Histogram(defaults)) =
            parse_first_kind(&json!({"h": {"histogram": {"field": "duration_micros", "interval": 2.5}}}))
        else {
            panic!("a histogram with defaults parses to Histogram");
        };
        assert_eq!(
            defaults,
            HistogramSpec {
                field: "duration_micros".to_string(),
                interval: 2.5,
                min_doc_count: 0,
                extended_bounds: None
            }
        );

        // Any other JSON type is a parsing_exception.
        for refused in [
            json!({"a": {"sum": {"field": "duration_micros", "missing": "abc"}}}),
            json!({"a": {"min": {"field": "duration_micros", "missing": true}}}),
            // `interval`, `min_doc_count`, `extended_bounds` and `size` take numbers only.
            json!({"h": {"histogram": {"field": "duration_micros", "interval": "100"}}}),
            json!({"h": {"histogram": {"field": "duration_micros", "interval": 100, "min_doc_count": "1"}}}),
            json!({"h": {"histogram": {
                "field": "duration_micros", "interval": 100, "extended_bounds": {"min": "0", "max": 600}
            }}}),
            json!({"t": {"terms": {"field": "service_name", "size": "10"}}}),
        ] {
            assert_refusal(refused, "parsing_exception").await;
        }
        // `stats` and `value_count` take no `missing`; no metric takes `precision`: an unknown key
        // is a parsing_exception naming it under `construct`.
        for (refused, construct) in [
            (
                json!({"a": {"stats": {"field": "duration_micros", "missing": 0}}}),
                "missing",
            ),
            (
                json!({"a": {"value_count": {"field": "duration_micros", "missing": 0}}}),
                "missing",
            ),
            (
                json!({"a": {"avg": {"field": "duration_micros", "precision": 1}}}),
                "precision",
            ),
            (
                json!({"h": {"histogram": {"field": "duration_micros", "interval": 1, "offset": 1}}}),
                "offset",
            ),
            (
                json!({"h": {"histogram": {
                    "field": "duration_micros", "interval": 1, "extended_bounds": {"min": 0, "max": 1, "step": 1}
                }}}),
                "step",
            ),
        ] {
            let error = assert_refusal(refused, "parsing_exception").await;
            assert_eq!(error["construct"], construct);
        }
        // A number of the right type and an invalid value is an illegal argument.
        for refused in [
            json!({"h": {"histogram": {"field": "duration_micros", "interval": 0}}}),
            json!({"h": {"histogram": {"field": "duration_micros", "interval": -5}}}),
            json!({"h": {"histogram": {
                "field": "duration_micros", "interval": 100, "extended_bounds": {"min": 600, "max": 0}
            }}}),
        ] {
            assert_refusal(refused, "illegal_argument_exception").await;
        }
    }

    #[tokio::test]
    async fn metric_order_paths_parse_ids_statistics_and_order_aggregations() {
        let path = |aggregation: &str, statistic: Option<&str>| MetricPath {
            aggregation: aggregation.to_string(),
            statistic: statistic.map(str::to_string),
            percent: None,
        };
        assert_eq!(parse_metric_path("1").expect("an id"), path("1", None));
        assert_eq!(
            parse_metric_path("2-orderAgg").expect("an order aggregation"),
            path("2-orderAgg", None)
        );
        assert_eq!(
            parse_metric_path("score_stats.max").expect("a statistic"),
            path("score_stats", Some("max"))
        );
        assert_eq!(
            parse_metric_path("1.95").expect("a dotted percent"),
            path("1", Some("95"))
        );
        for refused in ["", ".max", "score_stats.", "outer>inner", "1[50.0"] {
            let error = parse_metric_path(refused).expect_err("the path must be refused");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");
            assert_eq!(body["error"]["type"], "parsing_exception", "{refused}");
        }
        assert_eq!(
            parse_metric_path("1['50.5']").expect("a quoted percent"),
            MetricPath {
                aggregation: "1".to_string(),
                statistic: None,
                percent: Some("50.5".to_string()),
            }
        );

        let Ok(AggregationKind::Terms(spec)) = parse_first_kind(&json!({"services": {"terms": {
            "field": "service_name", "order": {"1": "desc"}
        }}})) else {
            panic!("a terms aggregation parses to Terms");
        };
        assert!(matches!(
            spec.order.as_slice(),
            [TermsOrder::Metric { path: ordering, order: SortOrder::Desc }] if *ordering == path("1", None)
        ));
    }

    #[tokio::test]
    async fn range_date_range_and_filters_parse_their_keys_and_refuse_other_types() {
        let Ok(AggregationKind::Range(range)) = parse_first_kind(&json!({"r": {"range": {
            "field": "duration_micros", "keyed": true,
            "ranges": [{"to": 100}, {"from": 100, "to": 300.5, "key": "middle"}]
        }}})) else {
            panic!("a range parses to Range");
        };
        assert_eq!(range.field, "duration_micros");
        assert!(range.is_keyed);
        assert_eq!(range.ranges.len(), 2);
        assert_eq!(
            (range.ranges[0].from, range.ranges[0].to, range.ranges[0].key.as_deref()),
            (None, Some(100.0), None)
        );
        assert_eq!(
            (range.ranges[1].from, range.ranges[1].to, range.ranges[1].key.as_deref()),
            (Some(100.0), Some(300.5), Some("middle"))
        );

        let Ok(AggregationKind::DateRange(date_range)) = parse_first_kind(&json!({"d": {"date_range": {
            "field": "timestamp", "time_zone": "Asia/Dubai",
            "ranges": [{"from": "now-3d/d", "to": 1_791_462_896_789_i64}]
        }}})) else {
            panic!("a date_range parses to DateRange");
        };
        assert_eq!(
            date_range.ranges[0].from,
            Some(DateRangeBound::Expression("now-3d/d".into()))
        );
        assert_eq!(
            date_range.ranges[0].to,
            Some(DateRangeBound::EpochMillis(1_791_462_896_789))
        );
        assert!(date_range.time_zone.is_some());

        let Ok(AggregationKind::Filters(named_filters)) = parse_first_kind(&json!({"f": {"filters": {"filters": {
            "errors": {"term": {"severity_text": "ERROR"}}, "all": {"match_all": {}}
        }}}})) else {
            panic!("named filters parse to Filters");
        };
        let FiltersBuckets::Named(buckets) = &named_filters.filters else {
            panic!("an object of filters is named");
        };
        let names: Vec<&str> = buckets.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["errors", "all"]);
        let Ok(AggregationKind::Filters(anonymous_filters)) =
            parse_first_kind(&json!({"f": {"filters": {"filters": [{"match_all": {}}]}}}))
        else {
            panic!("anonymous filters parse to Filters");
        };
        assert!(matches!(&anonymous_filters.filters, FiltersBuckets::Anonymous(queries) if queries.len() == 1));
        assert!(matches!(
            parse_first_kind(&json!({"one": {"filter": {"match_all": {}}}})),
            Ok(AggregationKind::Filter(_))
        ));

        for refused_body in [
            json!({"r": {"range": {"field": "duration_micros", "ranges": [{"from": "100"}]}}}),
            json!({"r": {"range": {"field": "duration_micros", "ranges": []}}}),
            json!({"r": {"range": {"field": "duration_micros", "ranges": [5]}}}),
            json!({"r": {"range": {"field": "duration_micros", "ranges": [{"to": 1, "key": 5}]}}}),
            json!({"r": {"range": {"field": "duration_micros", "keyed": "true", "ranges": [{"to": 1}]}}}),
            json!({"r": {"range": {"ranges": [{"to": 1}]}}}),
            json!({"r": {"range": {"field": "duration_micros"}}}),
            json!({"d": {"date_range": {"field": "timestamp", "ranges": [{"from": true}]}}}),
            // A fraction and an integer past i64 are no epoch milliseconds.
            json!({"d": {"date_range": {"field": "timestamp", "ranges": [{"from": 1.5}]}}}),
            json!({"d": {"date_range": {"field": "timestamp", "ranges": [{"from": 18_446_744_073_709_551_615_u64}]}}}),
            json!({"d": {"date_range": {"ranges": [{"to": "now"}]}}}),
            json!({"d": {"date_range": {"field": "timestamp"}}}),
            json!({"d": {"date_range": {"field": "timestamp", "time_zone": "Mars/Base", "ranges": [{"to": "now"}]}}}),
            json!({"f": {"filters": {"filters": {}}}}),
            json!({"f": {"filters": {"filters": []}}}),
            json!({"f": {"filters": {}}}),
            json!({"f": {"filters": {"filters": {"a": {"no_such_query": {}}}}}}),
            json!({"one": {"filter": {"no_such_query": {}}}}),
        ] {
            assert_refusal(refused_body, "parsing_exception").await;
        }
        assert_refusal(
            json!({"d": {"date_range": {"field": "timestamp", "format": "basic_date", "ranges": [{"to": "now"}]}}}),
            "illegal_argument_exception",
        )
        .await;
        // An unknown key is a parsing_exception naming it under `construct`.
        for (refused_body, construct) in [
            (
                json!({"r": {"range": {"field": "duration_micros", "ranges": [{"to": 1, "step": 2}]}}}),
                "step",
            ),
            (
                json!({"r": {"range": {"field": "duration_micros", "ranges": [{"to": 1}], "script": "x"}}}),
                "script",
            ),
            (
                json!({"d": {"date_range": {"field": "timestamp", "ranges": [{"to": "now", "step": 2}]}}}),
                "step",
            ),
            (
                json!({"d": {"date_range": {"field": "timestamp", "ranges": [{"to": "now"}], "keyed": true}}}),
                "keyed",
            ),
            (
                json!({"f": {"filters": {"filters": {"a": {"match_all": {}}}, "other_bucket": true}}}),
                "other_bucket",
            ),
        ] {
            let error = assert_refusal(refused_body, "parsing_exception").await;
            assert_eq!(error["construct"], construct);
        }
    }

    /// The trace-list latency script on one line.
    const LATENCY_SOURCE: &str = "if (doc.containsKey('traceGroupFields.durationInNanos') && !doc['traceGroupFields.durationInNanos'].empty) { return Math.round(doc['traceGroupFields.durationInNanos'].value / 10000) / 100.0 } return 0";

    #[test]
    fn a_metric_running_the_latency_script_alone_reads_the_root_duration() {
        let latency_metric = MetricSpec {
            field: "traceGroupFields.durationInNanos".to_string(),
            missing: None,
            value_script: Some(MetricScript::TraceListLatency),
        };
        let script = json!({"source": LATENCY_SOURCE, "lang": "painless"});

        let Ok(AggregationKind::Max(max_metric)) = parse_first_kind(&json!({"latency": {"max": {"script": script}}}))
        else {
            panic!("a max with the latency script parses to Max");
        };
        let Ok(AggregationKind::Max(named_max_metric)) = parse_first_kind(&json!({"latency": {"max": {
            "field": "traceGroupFields.durationInNanos",
            "script": script
        }}})) else {
            panic!("a max naming the field the latency script reads parses to Max");
        };
        let Ok(AggregationKind::ExtendedStats(extended_stats)) =
            parse_first_kind(&json!({"latency": {"extended_stats": {"script": script}}}))
        else {
            panic!("an extended_stats with the latency script parses to ExtendedStats");
        };
        let Ok(AggregationKind::Percentiles(percentiles)) =
            parse_first_kind(&json!({"latency": {"percentiles": {"script": script}}}))
        else {
            panic!("a percentiles with the latency script parses to Percentiles");
        };

        assert_eq!(max_metric, latency_metric);
        assert_eq!(named_max_metric, latency_metric);
        assert_eq!(extended_stats.metric, latency_metric);
        assert_eq!(percentiles.metric, latency_metric);
    }

    #[tokio::test]
    async fn the_latency_script_refuses_another_field_and_a_missing_while_arithmetic_needs_a_field() {
        let script = json!({"source": LATENCY_SOURCE, "lang": "painless"});
        let refused_fields = [
            json!({"latency": {"max": {"field": "durationInNanos", "script": script}}}),
            json!({"latency": {"percentiles": {"field": "durationInNanos", "script": script}}}),
        ];
        for refused_body in refused_fields {
            let error = assert_refusal(refused_body, "illegal_argument_exception").await;
            assert_eq!(error["aggregation"], "latency", "{error}");
            assert_eq!(error["field"], "durationInNanos", "{error}");
        }
        let refused_missings = [
            json!({"latency": {"max": {"missing": 0, "script": script}}}),
            json!({"latency": {"extended_stats": {"missing": 0, "script": script}}}),
        ];
        for refused_body in refused_missings {
            let error = assert_refusal(refused_body, "illegal_argument_exception").await;
            assert_eq!(error["aggregation"], "latency", "{error}");
            assert_eq!(error["key"], "missing", "{error}");
        }

        // Without the latency script a metric still needs its field.
        assert_refusal(
            json!({"scaled": {"max": {"script": "_value / 1000"}}}),
            "parsing_exception",
        )
        .await;
    }
}
