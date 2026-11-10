//! Metric aggregations: `avg`, `sum`, `min`, `max`, `value_count`, `stats`, `extended_stats`,
//! `cardinality`, `percentiles` and `percentile_ranks`.
//!
//! A metric is computed by the stage aggregate at the granularity of the level that declares
//! it, or over every matching row for a top-level metric. Numeric and boolean fields are read as
//! doubles (a boolean as 0 or 1), dates as epoch milliseconds floored from the stored
//! microseconds, and a metric's `missing` stands in for an absent value before it is
//! aggregated. `value_count` counts the values of the field as they are. A `cardinality` is
//! counted exactly by a branch of its own (`plan.rs`), never by the stage aggregate.
//! `percentiles` and `percentile_ranks` are computed by aggregates of their own
//! (`percentiles.rs`), which the stage aggregate plans from the slot's request.

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::{Expr, cast, lit, when};
use serde_json::{Map, Number, Value, json};

use super::parse::{
    AggregationKind, IGNORED_EXECUTION_KEYS, MetricPath, expect_object, expect_string, read_lenient_number,
    read_strict_number, read_value_script, report_unsupported_key, resolve_metric_field,
};
use super::percentiles::{decode_quantiles, decode_rank_counts, render_percentile_ranks, render_percentiles};
use super::plan::{FieldInput, MetricSlot};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::script::MetricScript;
use crate::opensearch::search::doc_id::MICROS_PER_MILLISECOND;
use crate::opensearch::search::execute::convert_plan_error;
use crate::opensearch::search::hits::{DateRenderFormat, render_date, validate_date_micros};

/// The field a metric aggregation reads and the value an absent one stands for.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MetricSpec {
    /// The field whose values the metric reads.
    pub(crate) field: String,
    /// The value a row without the field contributes; accepted where `value_script` is, as a JSON
    /// number or a numeric string.
    pub(crate) missing: Option<f64>,
    /// A recognized value script, applied to each row's value after `missing` is coalesced; set
    /// only on `avg`, `sum`, `min`, `max`, `extended_stats` and `percentiles`. A statistic it
    /// makes non-finite renders `null` ([`NonFiniteRendering::Null`]).
    pub(crate) value_script: Option<MetricScript>,
}

/// The request type name of `extended_stats`.
const EXTENDED_STATS: &str = "extended_stats";

/// Standard deviations between `avg` and each bound when the request names no `sigma`.
const DEFAULT_SIGMA: f64 = 2.0;

/// An `extended_stats` aggregation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExtendedStatsSpec {
    /// The field and its `missing` value.
    pub(crate) metric: MetricSpec,
    /// Standard deviations between `avg` and each `std_deviation_bounds` member, never negative.
    pub(crate) sigma: f64,
}

/// Parse the `extended_stats` aggregation `name`: `field`, `missing`, `script` (a value script),
/// `sigma`; the keys of [`IGNORED_EXECUTION_KEYS`] are accepted and ignored. Only numbers are
/// read: a field of any other type is refused when the rows are planned.
///
/// # Errors
///
/// `400 parsing_exception` for a body that is not an object, a missing `field` (without the
/// trace-list latency script), an unknown key, a `missing` that is neither a JSON number nor a
/// numeric string, a malformed `script` member, and a `sigma` that is not a JSON number or is
/// negative; `400 illegal_argument_exception` for a script that is neither arithmetic nor the
/// latency script, and as [`resolve_metric_field`] states for a `missing` or `field` beside the
/// latency script.
pub(super) fn parse_extended_stats(name: &str, value: &Value) -> Result<ExtendedStatsSpec, OpenSearchError> {
    let body = expect_object(name, EXTENDED_STATS, value)?;
    let mut field = None;
    let mut missing = None;
    let mut value_script = None;
    let mut sigma = DEFAULT_SIGMA;
    for (key, value) in body {
        match key.as_str() {
            "field" => field = Some(expect_string(name, key, value)?.to_string()),
            "missing" => missing = Some(read_lenient_number(name, key, value)?),
            "script" => value_script = Some(read_value_script(name, value)?),
            "sigma" => sigma = read_strict_number(name, key, value)?,
            ignored if IGNORED_EXECUTION_KEYS.contains(&ignored) => {}
            unknown => return Err(report_unsupported_key(EXTENDED_STATS, name, unknown)),
        }
    }
    if sigma < 0.0 {
        return Err(OpenSearchError::parsing(format!(
            "[sigma] of [{name}] must be greater than or equal to 0, got [{sigma}]"
        )));
    }
    Ok(ExtendedStatsSpec {
        metric: MetricSpec {
            field: resolve_metric_field(EXTENDED_STATS, name, field, missing, value_script.as_ref())?,
            missing,
            value_script,
        },
        sigma,
    })
}

/// One bucket's `extended_stats` statistics.
#[derive(Debug, Clone, Copy)]
pub(super) struct ExtendedStatsValues {
    /// `count(x)`.
    pub(super) count: u64,
    /// `Σx`, `None` when the bucket has no value.
    pub(super) sum: Option<f64>,
    /// `min(x)`.
    pub(super) min: Option<f64>,
    /// `max(x)`.
    pub(super) max: Option<f64>,
    /// `Σx²`.
    pub(super) sum_of_squares: Option<f64>,
}

/// The numeric members a rendered `stats` carries; a `buckets_path` may name each of them.
pub(super) const STATS_MEMBERS: &[&str] = &["count", "min", "max", "avg", "sum"];
/// The numeric members a rendered `extended_stats` carries at its top level; a `buckets_path`
/// may name each of them.
pub(super) const EXTENDED_STATS_MEMBERS: &[&str] = &[
    "count",
    "min",
    "max",
    "avg",
    "sum",
    "sum_of_squares",
    "variance",
    "variance_population",
    "variance_sampling",
    "std_deviation",
    "std_deviation_population",
    "std_deviation_sampling",
];

/// Renders an `extended_stats` result over the `n` values `x` of a bucket.
///
/// `avg = Σx / n`; `variance` and `variance_population` are `Σx² / n − avg²`;
/// `variance_sampling` is `(Σx² − (Σx)² / n) / (n − 1)` and `null` when
/// `n < 2`; both variances are clamped at 0, since rounding can push a zero
/// variance below it. The deviations are their square roots, and each bound is
/// `avg ± sigma · deviation`. A bucket without a value renders `count: 0`,
/// `sum: 0.0` and every other number `null`.
///
/// A statistic that is not finite renders as [`NonFiniteRendering::from_value_script`] states
/// for the spec's value script.
///
/// # Errors
///
/// `500` for a statistic that is not finite when the spec has no value script, such as a bound
/// past the double range under a large `sigma`: an answer of `null` would claim the bucket has
/// no value.
pub(super) fn render_extended_stats(
    spec: &ExtendedStatsSpec,
    values: &ExtendedStatsValues,
) -> Result<Value, OpenSearchError> {
    if values.count == 0 {
        return Ok(json!({
            "count": 0,
            "min": null,
            "max": null,
            "avg": null,
            "sum": 0.0,
            "sum_of_squares": null,
            "variance": null,
            "variance_population": null,
            "variance_sampling": null,
            "std_deviation": null,
            "std_deviation_population": null,
            "std_deviation_sampling": null,
            "std_deviation_bounds": {
                "upper": null, "lower": null,
                "upper_population": null, "lower_population": null,
                "upper_sampling": null, "lower_sampling": null
            }
        }));
    }
    #[expect(clippy::cast_precision_loss, reason = "a bucket's row count is far below 2^53")]
    let count = values.count as f64;
    let sum = values.sum.unwrap_or(0.0);
    let sum_of_squares = values.sum_of_squares.unwrap_or(0.0);
    let average = sum / count;
    // `f64::max` would turn the NaN variance of infinite values (a value script dividing by zero)
    // into a false 0; the comparison keeps it NaN, which renders as a non-finite statistic.
    let clamp_variance = |variance: f64| if variance < 0.0 { 0.0 } else { variance };
    let variance_population = clamp_variance(average.mul_add(-average, sum_of_squares / count));
    // `(Σx)² / n` is taken as `avg · Σx`: it never exceeds `Σx²`, so it stays finite whenever
    // `Σx²` is, where `Σx · Σx` alone could overflow and clamp the variance to a false 0.
    let variance_sampling =
        (values.count > 1).then(|| clamp_variance(average.mul_add(-sum, sum_of_squares) / (count - 1.0)));
    let deviation_population = variance_population.sqrt();
    let deviation_sampling = variance_sampling.map(f64::sqrt);
    let upper_population = spec.sigma.mul_add(deviation_population, average);
    let lower_population = (-spec.sigma).mul_add(deviation_population, average);
    let upper_sampling = deviation_sampling.map(|deviation| spec.sigma.mul_add(deviation, average));
    let lower_sampling = deviation_sampling.map(|deviation| (-spec.sigma).mul_add(deviation, average));
    let rendering = NonFiniteRendering::from_value_script(spec.metric.value_script.as_ref());
    let render_statistic = |value: Option<f64>| render_float(value, rendering);
    Ok(json!({
        "count": values.count,
        "min": render_statistic(values.min)?,
        "max": render_statistic(values.max)?,
        "avg": render_statistic(Some(average))?,
        "sum": render_statistic(Some(sum))?,
        "sum_of_squares": render_statistic(Some(sum_of_squares))?,
        "variance": render_statistic(Some(variance_population))?,
        "variance_population": render_statistic(Some(variance_population))?,
        "variance_sampling": render_statistic(variance_sampling)?,
        "std_deviation": render_statistic(Some(deviation_population))?,
        "std_deviation_population": render_statistic(Some(deviation_population))?,
        "std_deviation_sampling": render_statistic(deviation_sampling)?,
        "std_deviation_bounds": {
            "upper": render_statistic(Some(upper_population))?,
            "lower": render_statistic(Some(lower_population))?,
            "upper_population": render_statistic(Some(upper_population))?,
            "lower_population": render_statistic(Some(lower_population))?,
            "upper_sampling": render_statistic(upper_sampling)?,
            "lower_sampling": render_statistic(lower_sampling)?
        }
    }))
}

/// The metric an aggregation computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetricKind {
    /// `avg`: the mean of the values.
    Avg,
    /// `sum`: the sum of the values; a bucket without values renders 0.
    Sum,
    /// `min`: the smallest value.
    Min,
    /// `max`: the largest value.
    Max,
    /// `value_count`: how many values the bucket holds.
    ValueCount,
    /// `stats`: `count`, `min`, `max`, `avg` and `sum` together.
    Stats,
    /// `cardinality`: the exact number of distinct values.
    Cardinality,
    /// `percentiles`: approximate quantiles from one t-digest per bucket and value.
    Percentiles,
    /// `percentile_ranks`: exact shares of values at or below given values.
    PercentileRanks,
    /// `extended_stats`: the `stats` statistics with the sum of squares, from which the
    /// variances, deviations and bounds are derived.
    ExtendedStats,
}

impl MetricKind {
    /// The aggregation's type name in a request.
    pub(super) const fn read_type_name(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::ValueCount => "value_count",
            Self::Stats => "stats",
            Self::Cardinality => "cardinality",
            Self::Percentiles => "percentiles",
            Self::PercentileRanks => "percentile_ranks",
            Self::ExtendedStats => "extended_stats",
        }
    }

    /// The statistics the stage aggregate computes per bucket for this metric; none for
    /// `cardinality`, which a distinct branch counts. `percentiles` and `percentile_ranks` list
    /// the one list statistic their own aggregate computes, which the stage plans from the slot's
    /// request.
    pub(super) const fn list_statistics(self) -> &'static [Statistic] {
        match self {
            Self::Avg => &[Statistic::Avg],
            Self::Sum => &[Statistic::Sum],
            Self::Min => &[Statistic::Min],
            Self::Max => &[Statistic::Max],
            Self::ValueCount => &[Statistic::Count],
            Self::Stats => &[
                Statistic::Count,
                Statistic::Min,
                Statistic::Max,
                Statistic::Avg,
                Statistic::Sum,
            ],
            Self::Cardinality => &[],
            Self::Percentiles => &[Statistic::Quantiles],
            Self::PercentileRanks => &[Statistic::RankCounts],
            // `Avg` is read only by an `<id>.avg` order path; the rendered `avg` is `Σx / n`.
            Self::ExtendedStats => &[
                Statistic::Count,
                Statistic::Min,
                Statistic::Max,
                Statistic::Avg,
                Statistic::Sum,
                Statistic::SumOfSquares,
            ],
        }
    }
}

/// A statistic of a metric per bucket: computed by the stage aggregate, or, for `Distinct`, by a
/// distinct branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Statistic {
    /// The number of values (`Int64`).
    Count,
    /// The smallest value (`Float64`).
    Min,
    /// The largest value (`Float64`).
    Max,
    /// The mean of the values (`Float64`).
    Avg,
    /// The sum of the values (`Float64`).
    Sum,
    /// The exact number of distinct values (`Int64`), counted by a distinct branch, never by the
    /// stage aggregate.
    Distinct,
    /// The `multi_quantile` estimates of `percentiles`: a list, one per requested percent.
    Quantiles,
    /// The `rank_counts` of `percentile_ranks`: a list, the value count and then one count per
    /// requested value.
    RankCounts,
    /// The sum of the squared values (`Float64`).
    SumOfSquares,
}

impl Statistic {
    /// The suffix of the statistic's column in the stage aggregate's output.
    pub(super) const fn read_column_suffix(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
            Self::Sum => "sum",
            Self::Distinct => "distinct",
            Self::Quantiles => "quantiles",
            Self::RankCounts => "rank_counts",
            Self::SumOfSquares => "sum_of_squares",
        }
    }
}

/// What a metric reads from each matching row.
pub(super) struct MetricInput {
    /// The per-row value: a `Float64` (a date in epoch milliseconds) for every metric but
    /// `value_count` and `cardinality`, which read the field's own value.
    pub(super) value: Expr,
    /// Whether the field is a date, so an extreme also renders as a string.
    pub(super) is_date: bool,
}

/// The aggregate expression computing `statistic` over `value`.
///
/// # Errors
///
/// `500` for `Distinct` (a distinct branch counts it), `Quantiles` (a shared digest) and
/// `RankCounts` (a `rank_counts` aggregate).
pub(super) fn plan_statistic(statistic: Statistic, value: Expr) -> Result<Expr, OpenSearchError> {
    match statistic {
        Statistic::Count => Ok(count(value)),
        Statistic::Min => Ok(min(value)),
        Statistic::Max => Ok(max(value)),
        Statistic::Avg => Ok(avg(value)),
        Statistic::Sum => Ok(sum(value)),
        Statistic::SumOfSquares => Ok(sum(value.clone() * value)),
        Statistic::Distinct | Statistic::Quantiles | Statistic::RankCounts => Err(OpenSearchError::internal(format!(
            "the {statistic:?} statistic is computed by an aggregate of its own, never by plan_statistic"
        ))),
    }
}

/// What `kind` reads from each row of the rows-plan column `field`, `spec.missing` standing in
/// for an absent value and `spec.value_script` applied after it.
///
/// The rows plan holds every field in the Arrow type its field type maps to, and a field no
/// searched index maps as a NULL column; `plan_aggregation_rows` has already refused a mapped
/// field the metric cannot read. Every column but a date is therefore read as a double, the NULL
/// column included.
///
/// # Errors
///
/// `400 illegal_argument_exception` for a value script over a date field; `500` when the
/// epoch-millisecond expression of a date fails to build.
pub(super) fn plan_metric_input(
    kind: MetricKind,
    spec: &MetricSpec,
    field: FieldInput,
) -> Result<MetricInput, OpenSearchError> {
    if matches!(kind, MetricKind::ValueCount | MetricKind::Cardinality) {
        return Ok(MetricInput {
            value: field.expr,
            is_date: false,
        });
    }
    let is_date = matches!(field.data_type, DataType::Timestamp(TimeUnit::Microsecond, _));
    let value = if is_date {
        plan_epoch_millis(field.expr)?
    } else {
        cast(field.expr, DataType::Float64)
    };
    let value = match spec.missing {
        Some(missing) => coalesce(vec![value, lit(missing)]),
        None => value,
    };
    // A date has no `_value` arithmetic: its extremes render as dates, which a scaled value is not.
    let value = match &spec.value_script {
        None => value,
        Some(_) if is_date => {
            return Err(OpenSearchError::illegal_argument(format!(
                "a value script runs only over numeric fields, and [{}] is a date",
                spec.field
            )));
        }
        Some(script) => script.lower_value_script(value),
    };
    Ok(MetricInput { value, is_date })
}

/// Epoch milliseconds of a `Timestamp(µs)`, floored, as a `Float64`.
fn plan_epoch_millis(timestamp: Expr) -> Result<Expr, OpenSearchError> {
    let micros = cast(timestamp, DataType::Int64);
    // Integer division truncates toward zero. An instant before 1970 is first stepped down by
    // one microsecond less than a millisecond, so the division floors it into the millisecond
    // that holds it.
    let floored = when(
        micros.clone().gt_eq(lit(0_i64)),
        micros.clone() / lit(MICROS_PER_MILLISECOND),
    )
    .otherwise((micros - lit(MICROS_PER_MILLISECOND - 1)) / lit(MICROS_PER_MILLISECOND))
    .map_err(convert_plan_error)?;
    Ok(cast(floored, DataType::Float64))
}

/// The statistic a `terms` order path reads from a metric of `kind`: the metric's own value for
/// a single-value metric (`<id>` or `<id>.value`), the named statistic of `stats` and
/// `extended_stats` (which also names `sum_of_squares`).
///
/// # Errors
///
/// `400 aggregation_execution_exception` for a statistic the metric does not compute, for
/// `stats` or `extended_stats` without a statistic, for a derived `extended_stats` statistic (a
/// variance, a deviation or a bound), and for a list metric: a percent of `percentiles` is
/// resolved by the planner before this function is reached, and `percentile_ranks` cannot order
/// buckets.
pub(super) fn resolve_order_statistic(kind: MetricKind, path: &MetricPath) -> Result<Statistic, OpenSearchError> {
    let statistic = match (kind, path.statistic.as_deref()) {
        (MetricKind::Avg, None | Some("value")) | (MetricKind::Stats | MetricKind::ExtendedStats, Some("avg")) => {
            Some(Statistic::Avg)
        }
        (MetricKind::Sum, None | Some("value")) | (MetricKind::Stats | MetricKind::ExtendedStats, Some("sum")) => {
            Some(Statistic::Sum)
        }
        (MetricKind::Min, None | Some("value")) | (MetricKind::Stats | MetricKind::ExtendedStats, Some("min")) => {
            Some(Statistic::Min)
        }
        (MetricKind::Max, None | Some("value")) | (MetricKind::Stats | MetricKind::ExtendedStats, Some("max")) => {
            Some(Statistic::Max)
        }
        (MetricKind::ValueCount, None | Some("value"))
        | (MetricKind::Stats | MetricKind::ExtendedStats, Some("count")) => Some(Statistic::Count),
        (MetricKind::ExtendedStats, Some("sum_of_squares")) => Some(Statistic::SumOfSquares),
        (MetricKind::Cardinality, None | Some("value")) => Some(Statistic::Distinct),
        // A statistic the metric does not compute, `stats` or `extended_stats` without a
        // statistic, and the list metrics.
        (
            MetricKind::Avg
            | MetricKind::Sum
            | MetricKind::Min
            | MetricKind::Max
            | MetricKind::ValueCount
            | MetricKind::Stats
            | MetricKind::ExtendedStats
            | MetricKind::Cardinality
            | MetricKind::Percentiles
            | MetricKind::PercentileRanks,
            _,
        ) => None,
    };
    statistic.ok_or_else(|| {
        let written = path.statistic.as_deref().map_or_else(
            || path.aggregation.clone(),
            |statistic| format!("{}.{statistic}", path.aggregation),
        );
        OpenSearchError::aggregation_execution(&format!(
            "order path [{written}] names no statistic of the [{}] aggregation [{}]",
            kind.read_type_name(),
            path.aggregation
        ))
    })
}

/// Render the metric of `slot` for one bucket from its statistics, `read` returning each one
/// (NULL for a bucket without an aggregated row).
///
/// A single-value metric renders `value`: `null` when the bucket has no value, 0 for an empty
/// `sum`, `value_count` and `cardinality`. `stats` renders `count`, `min`, `max`, `avg` and
/// `sum` (an empty sum is 0). An extreme of a date field also renders `<key>_as_string` at
/// millisecond precision. `percentiles` and `percentile_ranks` render `values` keyed by percent
/// or value, or as an array when not keyed. `extended_stats` renders as [`render_extended_stats`]
/// states. A `max` over the trace-list latency script renders 0, never `null`, for a bucket
/// without a value.
///
/// # Errors
///
/// `500` for a statistic of another type than its aggregate or distinct branch computes, for a
/// statistic that is not finite (a sum or mean that overflows, a quantile estimate) of a metric
/// without a value script (one with a value script renders it as
/// [`NonFiniteRendering::from_value_script`] states), for a date
/// extreme outside the range a date renders in, for a `quantiles` or `rank_counts` list whose
/// length is not the one its request asks for, and for a slot whose kind and aggregation
/// disagree, which `AggregationLayout` never builds.
pub(super) fn render_metric(
    slot: &MetricSlot<'_>,
    is_date: bool,
    read: &dyn Fn(Statistic) -> Result<ScalarValue, OpenSearchError>,
) -> Result<Value, OpenSearchError> {
    let mut object = Map::new();
    let rendering = NonFiniteRendering::from_value_script(slot.spec.value_script.as_ref());
    match slot.kind {
        MetricKind::Avg => {
            object.insert(
                "value".to_string(),
                render_float(read_float(&read(Statistic::Avg)?)?, rendering)?,
            );
        }
        MetricKind::Sum => {
            object.insert("value".to_string(), render_sum(&read(Statistic::Sum)?, rendering)?);
        }
        MetricKind::Min => insert_extreme(&mut object, "value", &read(Statistic::Min)?, is_date, rendering)?,
        MetricKind::Max => {
            insert_extreme(&mut object, "value", &read(Statistic::Max)?, is_date, rendering)?;
            // The latency script that ends in a default renders 0 for a bucket with no row: its
            // value is never null.
            if matches!(
                slot.spec.value_script,
                Some(MetricScript::TraceListLatency {
                    is_zero_when_absent: true
                })
            ) && object.get("value").is_none_or(Value::is_null)
            {
                object.insert("value".to_string(), render_float(Some(0.0), rendering)?);
            }
        }
        MetricKind::ValueCount => {
            object.insert("value".to_string(), Value::from(read_count(&read(Statistic::Count)?)?));
        }
        MetricKind::Stats => {
            object.insert("count".to_string(), Value::from(read_count(&read(Statistic::Count)?)?));
            insert_extreme(&mut object, "min", &read(Statistic::Min)?, is_date, rendering)?;
            insert_extreme(&mut object, "max", &read(Statistic::Max)?, is_date, rendering)?;
            object.insert(
                "avg".to_string(),
                render_float(read_float(&read(Statistic::Avg)?)?, rendering)?,
            );
            object.insert("sum".to_string(), render_sum(&read(Statistic::Sum)?, rendering)?);
        }
        MetricKind::Cardinality => {
            object.insert(
                "value".to_string(),
                Value::from(read_count(&read(Statistic::Distinct)?)?),
            );
        }
        MetricKind::Percentiles => {
            let AggregationKind::Percentiles(spec) = slot.aggregation else {
                return Err(report_mismatched_slot(slot));
            };
            let quantiles = decode_quantiles(&read(Statistic::Quantiles)?, rendering)?;
            check_statistic_length(slot, quantiles.as_ref().map(Vec::len), spec.percents.len())?;
            return Ok(render_percentiles(spec, quantiles.as_deref()));
        }
        MetricKind::PercentileRanks => {
            let AggregationKind::PercentileRanks(spec) = slot.aggregation else {
                return Err(report_mismatched_slot(slot));
            };
            let counts = decode_rank_counts(&read(Statistic::RankCounts)?)?;
            // The list holds the count of values, then one count per requested value.
            check_statistic_length(slot, counts.as_ref().map(Vec::len), spec.values.len() + 1)?;
            return Ok(render_percentile_ranks(spec, counts.as_deref()));
        }
        MetricKind::ExtendedStats => {
            let AggregationKind::ExtendedStats(spec) = slot.aggregation else {
                return Err(report_mismatched_slot(slot));
            };
            let values = ExtendedStatsValues {
                count: read_count(&read(Statistic::Count)?)?,
                sum: read_float(&read(Statistic::Sum)?)?,
                min: read_float(&read(Statistic::Min)?)?,
                max: read_float(&read(Statistic::Max)?)?,
                sum_of_squares: read_float(&read(Statistic::SumOfSquares)?)?,
            };
            return render_extended_stats(spec, &values);
        }
    }
    Ok(Value::Object(object))
}

/// Checks that the list statistic of `slot` holds `expected_length` entries; `length` is `None`
/// for a bucket without a value, which renders every entry `null`.
///
/// # Errors
///
/// `500` for a list of another length: rendering it would leave a requested entry `null`, which
/// claims the bucket has no value.
fn check_statistic_length(
    slot: &MetricSlot<'_>,
    length: Option<usize>,
    expected_length: usize,
) -> Result<(), OpenSearchError> {
    match length {
        Some(length) if length != expected_length => Err(OpenSearchError::internal(format!(
            "metric [{}] of type [{}] holds {length} entries where its request asks for {expected_length}",
            slot.name,
            slot.kind.read_type_name()
        ))),
        Some(_) | None => Ok(()),
    }
}

/// The error of a slot whose metric kind and aggregation disagree.
fn report_mismatched_slot(slot: &MetricSlot<'_>) -> OpenSearchError {
    OpenSearchError::internal(format!(
        "metric [{}] is laid out as [{}] over another aggregation",
        slot.name,
        slot.kind.read_type_name()
    ))
}

/// How a metric renders a statistic that is not finite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NonFiniteRendering {
    /// A `500`: only an overflow inside IceGate's own arithmetic produces it, and an answer of
    /// `null` would claim the bucket has no value.
    Refused,
    /// `null`: the request's value script computed it, and an arithmetic script's non-finite
    /// result renders `null` (a division by zero, the remainder of a division by zero).
    Null,
}

impl NonFiniteRendering {
    /// [`Self::Null`] for a metric with a value script, [`Self::Refused`] for one without.
    pub(super) const fn from_value_script(value_script: Option<&MetricScript>) -> Self {
        if value_script.is_some() {
            Self::Null
        } else {
            Self::Refused
        }
    }
}

/// `value` as a JSON number, or `null` when the bucket has no value; a value that is not finite
/// renders as `rendering` states.
///
/// # Errors
///
/// `500` for a value that is not finite under [`NonFiniteRendering::Refused`].
fn render_float(value: Option<f64>, rendering: NonFiniteRendering) -> Result<Value, OpenSearchError> {
    let Some(number) = value else {
        return Ok(Value::Null);
    };
    match Number::from_f64(number) {
        Some(finite) => Ok(Value::Number(finite)),
        None if rendering == NonFiniteRendering::Null => Ok(Value::Null),
        None => Err(OpenSearchError::internal(format!(
            "metric statistic [{number}] is not finite"
        ))),
    }
}

/// A sum statistic, 0 when the bucket has no value.
///
/// # Errors
///
/// `500` for a statistic that is not a `Float64`, or not finite under
/// [`NonFiniteRendering::Refused`].
fn render_sum(value: &ScalarValue, rendering: NonFiniteRendering) -> Result<Value, OpenSearchError> {
    render_float(Some(read_float(value)?.unwrap_or(0.0)), rendering)
}

/// A `Float64` statistic; `None` when the bucket has no value.
///
/// # Errors
///
/// `500` for a statistic of any other type.
fn read_float(value: &ScalarValue) -> Result<Option<f64>, OpenSearchError> {
    match value {
        ScalarValue::Float64(value) => Ok(*value),
        ScalarValue::Null => Ok(None),
        other => Err(OpenSearchError::internal(format!(
            "metric statistic [{other}] is not a Float64"
        ))),
    }
}

/// A count statistic; 0 when the bucket has no aggregated row.
///
/// # Errors
///
/// `500` for a negative count or a statistic that is not an `Int64`.
fn read_count(value: &ScalarValue) -> Result<u64, OpenSearchError> {
    match value {
        ScalarValue::Int64(Some(count)) => {
            u64::try_from(*count).map_err(|_| OpenSearchError::internal(format!("metric count [{count}] is negative")))
        }
        ScalarValue::Int64(None) | ScalarValue::Null => Ok(0),
        other => Err(OpenSearchError::internal(format!(
            "metric count [{other}] is not an Int64"
        ))),
    }
}

/// Insert the extreme `value` under `key` and, for a date that has one, its string under
/// `<key>_as_string`.
///
/// # Errors
///
/// `500` for a statistic that is not a `Float64`, or not finite under
/// [`NonFiniteRendering::Refused`], and for a date outside the range a date renders in.
fn insert_extreme(
    object: &mut Map<String, Value>,
    key: &str,
    value: &ScalarValue,
    is_date: bool,
    rendering: NonFiniteRendering,
) -> Result<(), OpenSearchError> {
    let number = read_float(value)?;
    object.insert(key.to_string(), render_float(number, rendering)?);
    if is_date && let Some(millis) = number {
        object.insert(format!("{key}_as_string"), render_epoch_millis(millis)?);
    }
    Ok(())
}

/// The epoch milliseconds `millis` of a date extreme as a millisecond-precision UTC date.
///
/// # Errors
///
/// `500` for an instant past the microsecond range, or outside the range a date renders in.
#[expect(
    clippy::cast_possible_truncation,
    reason = "an extreme of a date is an integral epoch millisecond floored from an i64 microsecond, or a \
              `missing` the saturating conversion bounds and the range check then refuses"
)]
fn render_epoch_millis(millis: f64) -> Result<Value, OpenSearchError> {
    let micros = (millis.floor() as i64)
        .checked_mul(MICROS_PER_MILLISECOND)
        .ok_or_else(|| OpenSearchError::internal(format!("epoch millisecond [{millis}] overflows microseconds")))?;
    Ok(render_date(
        validate_date_micros(micros)?,
        DateRenderFormat::StrictDateTime,
    ))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::super::plan::Granularity;
    use super::*;
    use crate::opensearch::error::render_envelope;

    /// The spec an `extended_stats` on `duration_micros` with `sigma` parses to.
    fn build_extended_stats_spec(sigma: f64) -> ExtendedStatsSpec {
        ExtendedStatsSpec {
            metric: MetricSpec {
                field: "duration_micros".to_owned(),
                missing: None,
                value_script: None,
            },
            sigma,
        }
    }

    #[test]
    fn extended_stats_render_population_and_sampling_variants_and_bounds() {
        // Values 100, 200, 300, 600: n = 4, Σx = 1200, Σx² = 500000, avg = 300.
        // Population variance 500000 / 4 − 300² = 35000; sampling variance
        // (500000 − 1200² / 4) / 3 = 46666.666666666664. Square roots
        // 187.08286933869707 and 216.02468994692867; bounds 300 ± 2 · σ.
        let values = ExtendedStatsValues {
            count: 4,
            sum: Some(1200.0),
            min: Some(100.0),
            max: Some(600.0),
            sum_of_squares: Some(500_000.0),
        };
        assert_eq!(
            render_extended_stats(&build_extended_stats_spec(2.0), &values).expect("finite statistics"),
            json!({
                "count": 4,
                "min": 100.0,
                "max": 600.0,
                "avg": 300.0,
                "sum": 1200.0,
                "sum_of_squares": 500_000.0,
                "variance": 35_000.0,
                "variance_population": 35_000.0,
                "variance_sampling": 46_666.666_666_666_664,
                "std_deviation": 187.082_869_338_697_07,
                "std_deviation_population": 187.082_869_338_697_07,
                "std_deviation_sampling": 216.024_689_946_928_67,
                "std_deviation_bounds": {
                    "upper": 674.165_738_677_394_1,
                    "lower": -74.165_738_677_394_13,
                    "upper_population": 674.165_738_677_394_1,
                    "lower_population": -74.165_738_677_394_13,
                    "upper_sampling": 732.049_379_893_857_3,
                    "lower_sampling": -132.049_379_893_857_35
                }
            })
        );
    }

    /// The keys of `rendered` whose values are numbers, sorted.
    fn list_numeric_keys(rendered: &Value) -> Vec<String> {
        let mut keys: Vec<String> = rendered
            .as_object()
            .expect("a rendered metric is an object")
            .iter()
            .filter(|(_, value)| value.is_number())
            .map(|(key, _)| key.clone())
            .collect();
        keys.sort_unstable();
        keys
    }

    /// `members`, sorted.
    fn sort_members(members: &[&str]) -> Vec<String> {
        let mut sorted: Vec<String> = members.iter().map(|member| (*member).to_owned()).collect();
        sorted.sort_unstable();
        sorted
    }

    #[test]
    fn the_buckets_path_member_lists_are_the_numeric_keys_rendered() {
        // Every statistic finite and non-null, so each numeric member renders as a number.
        let values = ExtendedStatsValues {
            count: 4,
            sum: Some(1200.0),
            min: Some(100.0),
            max: Some(600.0),
            sum_of_squares: Some(500_000.0),
        };
        let extended = render_extended_stats(&build_extended_stats_spec(2.0), &values).expect("finite statistics");
        assert_eq!(list_numeric_keys(&extended), sort_members(EXTENDED_STATS_MEMBERS));

        let spec = MetricSpec {
            field: "duration_micros".to_owned(),
            missing: None,
            value_script: None,
        };
        let aggregation = AggregationKind::Stats(spec.clone());
        let slot = MetricSlot {
            name: "1",
            kind: MetricKind::Stats,
            spec: &spec,
            aggregation: &aggregation,
            granularity: Granularity::Top,
        };
        let read = |statistic: Statistic| {
            Ok(match statistic {
                Statistic::Count => ScalarValue::Int64(Some(4)),
                _ => ScalarValue::Float64(Some(300.0)),
            })
        };
        let stats = render_metric(&slot, false, &read).expect("finite statistics");
        assert_eq!(list_numeric_keys(&stats), sort_members(STATS_MEMBERS));
    }

    #[test]
    fn extended_stats_of_one_value_have_no_sampling_variance() {
        let values = ExtendedStatsValues {
            count: 1,
            sum: Some(400.0),
            min: Some(400.0),
            max: Some(400.0),
            sum_of_squares: Some(160_000.0),
        };
        let rendered_stats =
            render_extended_stats(&build_extended_stats_spec(3.0), &values).expect("finite statistics");
        assert_eq!(rendered_stats["variance_population"], json!(0.0));
        assert_eq!(rendered_stats["variance_sampling"], json!(null));
        assert_eq!(rendered_stats["std_deviation_sampling"], json!(null));
        assert_eq!(rendered_stats["std_deviation_bounds"]["upper"], json!(400.0));
        assert_eq!(rendered_stats["std_deviation_bounds"]["upper_sampling"], json!(null));
    }

    #[test]
    fn extended_stats_without_values_render_zero_count_and_null_statistics() {
        let values = ExtendedStatsValues {
            count: 0,
            sum: None,
            min: None,
            max: None,
            sum_of_squares: None,
        };
        assert_eq!(
            render_extended_stats(&build_extended_stats_spec(2.0), &values).expect("an empty bucket renders"),
            json!({
                "count": 0,
                "min": null,
                "max": null,
                "avg": null,
                "sum": 0.0,
                "sum_of_squares": null,
                "variance": null,
                "variance_population": null,
                "variance_sampling": null,
                "std_deviation": null,
                "std_deviation_population": null,
                "std_deviation_sampling": null,
                "std_deviation_bounds": {
                    "upper": null, "lower": null,
                    "upper_population": null, "lower_population": null,
                    "upper_sampling": null, "lower_sampling": null
                }
            })
        );
    }

    #[test]
    fn extended_stats_whose_squared_sum_passes_the_double_range_keep_their_sampling_variance() {
        // Values 8e153 and 1e154: Σx = 1.8e154, so (Σx)² = 3.24e308 is past the double range,
        // while Σx² = 1.64e308 is not. avg = 9e153; the sampling variance is
        // (1e306 + 1e306) / 1 = 2e306 and the population variance 1e306.
        let values = ExtendedStatsValues {
            count: 2,
            sum: Some(1.8e154),
            min: Some(8e153),
            max: Some(1e154),
            sum_of_squares: Some(1.64e308),
        };
        let rendered_stats =
            render_extended_stats(&build_extended_stats_spec(2.0), &values).expect("finite statistics");
        for (key, expected) in [("variance_sampling", 2e306), ("variance_population", 1e306)] {
            let variance = rendered_stats[key].as_f64().expect("a variance");
            assert!((variance - expected).abs() <= expected * 1e-9, "{key}: {variance}");
        }
    }

    #[tokio::test]
    async fn extended_stats_bounds_past_the_double_range_are_a_500() {
        // Values 0 and 2: avg 1, population deviation 1 and sampling deviation √2. With sigma
        // 1e308 every bound is finite; with 1.7e308 the sampling bounds (±2.4e308) are past the
        // double range, and a `null` bound would claim the bucket has no value.
        let values = ExtendedStatsValues {
            count: 2,
            sum: Some(2.0),
            min: Some(0.0),
            max: Some(2.0),
            sum_of_squares: Some(4.0),
        };
        render_extended_stats(&build_extended_stats_spec(1e308), &values).expect("bounds within the double range");
        let error = render_extended_stats(&build_extended_stats_spec(1.7e308), &values)
            .expect_err("a bound past the double range");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"]["type"], json!("exception"));
    }

    #[tokio::test]
    async fn extended_stats_parse_sigma_and_refuse_a_negative_or_non_numeric_one() {
        let default_spec = parse_extended_stats("1", &json!({"field": "duration_micros"})).expect("sigma is optional");
        assert_eq!(default_spec, build_extended_stats_spec(2.0));
        let stated_spec =
            parse_extended_stats("1", &json!({"field": "duration_micros", "sigma": 1.5})).expect("a number");
        assert_eq!(stated_spec, build_extended_stats_spec(1.5));
        let zero_spec = parse_extended_stats("1", &json!({"field": "duration_micros", "sigma": 0})).expect("zero");
        assert_eq!(zero_spec, build_extended_stats_spec(0.0));
        for body in [
            json!({"field": "duration_micros", "sigma": -1}),
            json!({"field": "duration_micros", "sigma": "2"}),
        ] {
            let error = parse_extended_stats("1", &body).expect_err("refused");
            let (_, rendered_error) = render_envelope(error).await;
            assert_eq!(rendered_error["error"]["type"], json!("parsing_exception"), "{body}");
        }
    }

    #[test]
    fn extended_stats_parse_a_numeric_string_missing_and_ignore_an_execution_key() {
        let missing_spec = parse_extended_stats(
            "1",
            &json!({"field": "duration_micros", "missing": "250", "shard_size": 10}),
        )
        .expect("a numeric-string missing and an ignored execution key");
        assert_eq!(
            missing_spec,
            ExtendedStatsSpec {
                metric: MetricSpec {
                    field: "duration_micros".to_owned(),
                    missing: Some(250.0),
                    value_script: None,
                },
                sigma: 2.0,
            }
        );
    }

    #[tokio::test]
    async fn an_extended_stats_without_a_field_or_with_an_unknown_key_is_a_parsing_exception() {
        for body in [
            json!({"field": "duration_micros", "percents": [50]}),
            json!({"sigma": 2}),
        ] {
            let error = parse_extended_stats("1", &body).expect_err("refused");
            let (_, rendered_error) = render_envelope(error).await;
            assert_eq!(rendered_error["error"]["type"], json!("parsing_exception"), "{body}");
        }
    }
}
