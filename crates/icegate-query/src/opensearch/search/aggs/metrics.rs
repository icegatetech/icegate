//! Plain metric aggregations: `avg`, `sum`, `min`, `max`, `value_count` and `stats`.
//!
//! A metric is computed by the stage aggregate at the granularity of the level that declares
//! it, or over every matching row for a top-level metric. Numeric and boolean fields are read as
//! doubles (a boolean as 0 or 1), dates as epoch milliseconds floored from the stored
//! microseconds, and a metric's `missing` stands in for an absent value before it is
//! aggregated. `value_count` counts the values of the field as they are.

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::{Expr, cast, lit, when};
use serde_json::{Map, Number, Value};

use super::parse::MetricPath;
use super::plan::FieldInput;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::doc_id::MICROS_PER_MILLISECOND;
use crate::opensearch::search::execute::convert_plan_error;
use crate::opensearch::search::hits::{DateRenderFormat, render_date, validate_date_micros};

/// The field a metric aggregation reads and the value an absent one stands for.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MetricSpec {
    /// The field whose values the metric reads.
    pub(crate) field: String,
    /// The value a row without the field contributes. Parsing accepts it on `avg`, `sum`, `min`
    /// and `max` only, as a JSON number or a numeric string.
    pub(crate) missing: Option<f64>,
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
        }
    }

    /// The statistics the stage aggregate computes per bucket for this metric.
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
        }
    }
}

/// A statistic the stage aggregate computes per bucket for a metric.
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
        }
    }
}

/// What a metric reads from each matching row.
pub(super) struct MetricInput {
    /// The per-row value: a `Float64` (a date in epoch milliseconds) for every metric but
    /// `value_count`, which reads the field's own value.
    pub(super) value: Expr,
    /// Whether the field is a date, so an extreme also renders as a string.
    pub(super) is_date: bool,
}

/// The aggregate expression computing `statistic` over `value`.
pub(super) fn plan_statistic(statistic: Statistic, value: Expr) -> Expr {
    match statistic {
        Statistic::Count => count(value),
        Statistic::Min => min(value),
        Statistic::Max => max(value),
        Statistic::Avg => avg(value),
        Statistic::Sum => sum(value),
    }
}

/// What `kind` reads from each row of the rows-plan column `field`, `spec.missing` standing in
/// for an absent value.
///
/// The rows plan holds every field in the Arrow type its field type maps to, and a field no
/// searched index maps as a NULL column; `plan_aggregation_rows` has already refused a mapped
/// field the metric cannot read. Every column but a date is therefore read as a double, the NULL
/// column included.
///
/// # Errors
///
/// `500` when the epoch-millisecond expression of a date fails to build.
pub(super) fn plan_metric_input(
    kind: MetricKind,
    spec: &MetricSpec,
    field: FieldInput,
) -> Result<MetricInput, OpenSearchError> {
    if matches!(kind, MetricKind::ValueCount) {
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
/// a single-value metric (`<id>` or `<id>.value`), the named statistic of `stats`.
///
/// # Errors
///
/// `400 aggregation_execution_exception` for a statistic the metric does not compute, and for
/// `stats` without a statistic.
pub(super) fn resolve_order_statistic(kind: MetricKind, path: &MetricPath) -> Result<Statistic, OpenSearchError> {
    let statistic = match (kind, path.statistic.as_deref()) {
        (MetricKind::Avg, None | Some("value")) | (MetricKind::Stats, Some("avg")) => Some(Statistic::Avg),
        (MetricKind::Sum, None | Some("value")) | (MetricKind::Stats, Some("sum")) => Some(Statistic::Sum),
        (MetricKind::Min, None | Some("value")) | (MetricKind::Stats, Some("min")) => Some(Statistic::Min),
        (MetricKind::Max, None | Some("value")) | (MetricKind::Stats, Some("max")) => Some(Statistic::Max),
        (MetricKind::ValueCount, None | Some("value")) | (MetricKind::Stats, Some("count")) => Some(Statistic::Count),
        // A statistic the metric does not compute, or `stats` without a statistic.
        (
            MetricKind::Avg
            | MetricKind::Sum
            | MetricKind::Min
            | MetricKind::Max
            | MetricKind::ValueCount
            | MetricKind::Stats,
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

/// Render a metric of `kind` for one bucket from its statistics, `read` returning each one
/// (NULL for a bucket without an aggregated row).
///
/// A single-value metric renders `value`: `null` when the bucket has no value, 0 for an empty
/// `sum` and `value_count`. `stats` renders `count`, `min`, `max`, `avg` and `sum` (an empty sum is
/// 0). An extreme of a date field also renders `<key>_as_string` at millisecond precision.
///
/// # Errors
///
/// `500` for a statistic of another type than the stage aggregate computes, for a statistic
/// that is not finite (a sum or mean that overflows), and for a date extreme outside the range
/// a date renders in.
pub(super) fn render_metric(
    kind: MetricKind,
    is_date: bool,
    read: &dyn Fn(Statistic) -> Result<ScalarValue, OpenSearchError>,
) -> Result<Value, OpenSearchError> {
    let mut object = Map::new();
    match kind {
        MetricKind::Avg => {
            object.insert("value".to_string(), render_float(read_float(&read(Statistic::Avg)?)?)?);
        }
        MetricKind::Sum => {
            object.insert("value".to_string(), render_sum(&read(Statistic::Sum)?)?);
        }
        MetricKind::Min => insert_extreme(&mut object, "value", &read(Statistic::Min)?, is_date)?,
        MetricKind::Max => insert_extreme(&mut object, "value", &read(Statistic::Max)?, is_date)?,
        MetricKind::ValueCount => {
            object.insert("value".to_string(), Value::from(read_count(&read(Statistic::Count)?)?));
        }
        MetricKind::Stats => {
            object.insert("count".to_string(), Value::from(read_count(&read(Statistic::Count)?)?));
            insert_extreme(&mut object, "min", &read(Statistic::Min)?, is_date)?;
            insert_extreme(&mut object, "max", &read(Statistic::Max)?, is_date)?;
            object.insert("avg".to_string(), render_float(read_float(&read(Statistic::Avg)?)?)?);
            object.insert("sum".to_string(), render_sum(&read(Statistic::Sum)?)?);
        }
    }
    Ok(Value::Object(object))
}

/// `value` as a JSON number, or `null` when the bucket has no value.
///
/// # Errors
///
/// `500` for a value that is not finite, such as a sum that overflows: an answer of `null`
/// would claim the bucket has no value.
fn render_float(value: Option<f64>) -> Result<Value, OpenSearchError> {
    let Some(number) = value else {
        return Ok(Value::Null);
    };
    Number::from_f64(number)
        .map(Value::Number)
        .ok_or_else(|| OpenSearchError::internal(format!("metric statistic [{number}] is not finite")))
}

/// A sum statistic, 0 when the bucket has no value.
///
/// # Errors
///
/// `500` for a statistic that is not a `Float64` or not finite.
fn render_sum(value: &ScalarValue) -> Result<Value, OpenSearchError> {
    render_float(Some(read_float(value)?.unwrap_or(0.0)))
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
/// `500` for a statistic that is not a `Float64` or not finite, and for a date outside the range
/// a date renders in.
fn insert_extreme(
    object: &mut Map<String, Value>,
    key: &str,
    value: &ScalarValue,
    is_date: bool,
) -> Result<(), OpenSearchError> {
    let number = read_float(value)?;
    object.insert(key.to_string(), render_float(number)?);
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
