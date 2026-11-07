//! Rendering of the `_search` response and its hits.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::scalar::ScalarValue;
use serde_json::{Map, Number, Value, json};

use super::doc_id::MICROS_PER_DAY;
use super::request::{SearchRequest, SortClause, SourceFilter, TrackTotalHits};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::glob::matches_glob;

/// One hit of a response page: a row of one index's batch.
pub(crate) struct SearchHit<'a> {
    /// The concrete index name the row was read from (`_index`).
    pub(crate) index: &'a str,
    /// The batch holding the row; its columns are the index's tenant-scoped columns.
    pub(crate) batch: &'a RecordBatch,
    /// The row within `batch`.
    pub(crate) row: usize,
    /// The hit's `_id`.
    pub(crate) document_id: &'a str,
    /// The hit's `sort` values; `None` when the request does not sort.
    pub(crate) sort_values: Option<Vec<Value>>,
    /// The hit's `fields`; `None` when the request asks for none.
    pub(crate) field_values: Option<Map<String, Value>>,
}

/// The `_search` response, with its keys in this fixed order: `took`, `timed_out`,
/// `terminated_early` (only with `terminate_after`), `_shards`, `hits`.
///
/// `hits.total` renders `total` (see [`render_total_hits`]); it is omitted when the total is
/// untracked and the request does not ask for an integer total. Every hit scores `1.0` and
/// `max_score` is `1.0` when the request sorts on `_score` alone or not at all; with any other
/// sort clause both are `null`. `max_score` is `null` when no hit is returned. `hits.hits` is an
/// array even for `size: 0`.
///
/// # Errors
///
/// Returns `500` when a row holds a value no JSON rendering exists for.
pub(crate) fn render_search_response(
    took: Duration,
    total: TotalHits,
    hits: &[SearchHit<'_>],
    request: &SearchRequest,
) -> Result<Value, OpenSearchError> {
    let is_scored = request.sort.iter().all(|clause| matches!(clause, SortClause::Score { .. }));
    let score = if is_scored { json!(1.0) } else { Value::Null };
    let rendered_hits = hits
        .iter()
        .map(|hit| render_hit(hit, &score, request))
        .collect::<Result<Vec<_>, _>>()?;
    let max_score = if rendered_hits.is_empty() { Value::Null } else { score };
    let mut hits_object = Map::new();
    if let Some(total) = render_total_hits(total, request.rest_total_hits_as_int) {
        hits_object.insert("total".to_string(), total);
    }
    hits_object.insert("max_score".to_string(), max_score);
    hits_object.insert("hits".to_string(), Value::Array(rendered_hits));
    let mut response = Map::new();
    response.insert(
        "took".to_string(),
        json!(u64::try_from(took.as_millis()).unwrap_or(u64::MAX)),
    );
    response.insert("timed_out".to_string(), json!(false));
    if request.terminate_after.is_some() {
        response.insert("terminated_early".to_string(), json!(false));
    }
    response.insert(
        "_shards".to_string(),
        json!({"total": 1, "successful": 1, "skipped": 0, "failed": 0}),
    );
    response.insert("hits".to_string(), Value::Object(hits_object));
    Ok(Value::Object(response))
}

/// One hit object: `_index`, `_id`, `_version` (with `version`), `_score` (`score`), `_source`
/// under the request's filter (no key for `_source: false`), `fields` when any value was
/// found, and `sort` when the request sorts.
fn render_hit(hit: &SearchHit<'_>, score: &Value, request: &SearchRequest) -> Result<Value, OpenSearchError> {
    let mut object = Map::new();
    object.insert("_index".to_string(), json!(hit.index));
    object.insert("_id".to_string(), json!(hit.document_id));
    if request.version {
        object.insert("_version".to_string(), json!(1));
    }
    object.insert("_score".to_string(), score.clone());
    if !matches!(request.source, SourceFilter::Nothing) {
        object.insert(
            "_source".to_string(),
            render_source(hit.batch, hit.row, &request.source)?,
        );
    }
    if let Some(field_values) = hit.field_values.as_ref().filter(|values| !values.is_empty()) {
        object.insert("fields".to_string(), Value::Object(field_values.clone()));
    }
    if let Some(sort_values) = &hit.sort_values {
        object.insert("sort".to_string(), Value::Array(sort_values.clone()));
    }
    Ok(Value::Object(object))
}

/// A field sort value: an integer or a double as a number, a string as is, a binary id as
/// lowercase hex, a missing value as `null`.
///
/// # Errors
///
/// Returns `500` for a present value of any other type, which no sort key produces, rather than
/// rendering it as a missing value.
pub(crate) fn render_sort_scalar(value: &ScalarValue) -> Result<Value, OpenSearchError> {
    match value {
        ScalarValue::Int64(Some(number)) => Ok(json!(number)),
        ScalarValue::Float64(Some(number)) => Ok(render_double(*number)),
        ScalarValue::Utf8(Some(text)) => Ok(json!(text)),
        ScalarValue::FixedSizeBinary(_, Some(bytes)) => Ok(json!(hex::encode(bytes))),
        missing_value if missing_value.is_null() => Ok(Value::Null),
        unexpected_value => Err(OpenSearchError::internal(format!(
            "a sort value of the type {} has no rendering",
            unexpected_value.data_type()
        ))),
    }
}

/// A date format `fields` and `docvalue_fields` render with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DateRenderFormat {
    /// `date_time`: milliseconds, `Z`.
    DateTime,
    /// `strict_date_time`: milliseconds, `Z`.
    StrictDateTime,
    /// `strict_date_optional_time` (the default): milliseconds, `Z`.
    StrictDateOptionalTime,
    /// `strict_date_optional_time_nanos`: the stored microseconds, `Z`.
    StrictDateOptionalTimeNanos,
    /// `epoch_millis`: `floor(µs / 1000)` as a string.
    EpochMillis,
}

impl DateRenderFormat {
    /// Parse a format name (case-sensitive).
    ///
    /// # Errors
    ///
    /// Returns a 400 `illegal_argument_exception` naming the format and the supported ones.
    pub(crate) fn parse(text: &str) -> Result<Self, OpenSearchError> {
        match text {
            "date_time" => Ok(Self::DateTime),
            "strict_date_time" => Ok(Self::StrictDateTime),
            "strict_date_optional_time" => Ok(Self::StrictDateOptionalTime),
            "strict_date_optional_time_nanos" => Ok(Self::StrictDateOptionalTimeNanos),
            "epoch_millis" => Ok(Self::EpochMillis),
            unknown_format => Err(OpenSearchError::illegal_argument(format!(
                "unsupported date format [{unknown_format}]; supported: date_time, strict_date_time, \
                 strict_date_optional_time, strict_date_optional_time_nanos, epoch_millis"
            ))),
        }
    }
}

/// `timestamp_micros` rendered in `format`; a millisecond format floors to the millisecond.
///
/// The caller passes an instant in the range `chrono` represents (about ±262,000 years):
/// `null` is the only answer for one outside it, so a caller rendering stored data checks the
/// instant with [`validate_date_micros`] first.
pub(crate) fn render_date(timestamp_micros: i64, format: DateRenderFormat) -> Value {
    let millis = timestamp_micros.div_euclid(1_000);
    match format {
        DateRenderFormat::StrictDateOptionalTimeNanos => {
            render_iso_micros(timestamp_micros).map_or(Value::Null, Value::String)
        }
        DateRenderFormat::DateTime | DateRenderFormat::StrictDateTime | DateRenderFormat::StrictDateOptionalTime => {
            DateTime::from_timestamp_millis(millis).map_or(Value::Null, |instant| {
                json!(instant.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
            })
        }
        DateRenderFormat::EpochMillis => json!(millis.to_string()),
    }
}

/// `micros` unchanged when it lies in the range `chrono` represents, so [`render_date`] renders
/// it.
///
/// # Errors
///
/// Returns `500` for an instant outside that range, as [`render_iso_micros`] does: a stored date
/// that cannot be rendered fails the request rather than rendering as `null`.
pub(crate) fn validate_date_micros(micros: i64) -> Result<i64, OpenSearchError> {
    convert_micros_to_instant(micros).map(|_| micros)
}

/// One `fields` value of `array[row]`: a timestamp or a day in `format` (by default
/// `strict_date_optional_time`), any other value as `_source` renders it.
///
/// # Errors
///
/// Returns `500` for a date outside the range `chrono` represents, and the errors of
/// [`render_array_value`].
pub(crate) fn render_field_value(
    array: &dyn Array,
    row: usize,
    format: Option<DateRenderFormat>,
) -> Result<Value, OpenSearchError> {
    let format = format.unwrap_or(DateRenderFormat::StrictDateOptionalTime);
    match array.data_type() {
        DataType::Timestamp(unit, _) => Ok(render_date(
            validate_date_micros(read_timestamp_micros(array, *unit, row))?,
            format,
        )),
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row);
            let micros = i64::from(days)
                .checked_mul(MICROS_PER_DAY)
                .ok_or_else(|| build_date_range_error(days))?;
            Ok(render_date(validate_date_micros(micros)?, format))
        }
        _ => render_array_value(array, row),
    }
}

/// The `_source` of `batch[row]` under `filter`.
///
/// `All` renders every non-null column; `Nothing` an empty object (the caller then omits
/// `_source`). `Paths` keeps a column an include matches, or every column without includes.
/// Inside a MAP it also keeps the entries whose path `<column>.<stored key>` an include matches,
/// and it drops what an exclude matches. Patterns are globs over the whole path, and a dotted
/// stored key is one key. A MAP column of which no entry is kept renders only when the column
/// itself is kept.
///
/// # Errors
///
/// Returns `500` when a value has no JSON rendering, or a MAP column has non-string keys.
pub(crate) fn render_source(batch: &RecordBatch, row: usize, filter: &SourceFilter) -> Result<Value, OpenSearchError> {
    let (includes, excludes): (&[String], &[String]) = match filter {
        SourceFilter::All => (&[], &[]),
        SourceFilter::Nothing => return Ok(Value::Object(Map::new())),
        SourceFilter::Paths { includes, excludes } => (includes, excludes),
    };
    let matches_any = |patterns: &[String], path: &str| patterns.iter().any(|pattern| matches_glob(pattern, path));
    let mut source = Map::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let name = field.name();
        if column.is_null(row) || matches_any(excludes, name) {
            continue;
        }
        let is_whole_included = includes.is_empty() || matches_any(includes, name);
        let DataType::Map(_, _) = column.data_type() else {
            if is_whole_included {
                source.insert(name.clone(), render_array_value(column.as_ref(), row)?);
            }
            continue;
        };
        let entries = column.as_map().value(row);
        let mut object = Map::with_capacity(entries.len());
        for entry in 0..entries.len() {
            let Value::String(key) = render_array_value(entries.column(0).as_ref(), entry)? else {
                return Err(OpenSearchError::internal("a MAP column has non-string keys"));
            };
            // The entry's path is built only when a pattern could select or drop it.
            if !(is_whole_included && excludes.is_empty()) {
                let path = format!("{name}.{key}");
                if matches_any(excludes, &path) || !(is_whole_included || matches_any(includes, &path)) {
                    continue;
                }
            }
            object.insert(key, render_array_value(entries.column(1).as_ref(), entry)?);
        }
        if is_whole_included || !object.is_empty() {
            source.insert(name.clone(), Value::Object(object));
        }
    }
    Ok(Value::Object(source))
}

/// What a search reports as its total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TotalHits {
    /// The exact number of matching rows.
    Exact(u64),
    /// At least this many rows match (the tracking threshold).
    AtLeast(u64),
    /// The total is not tracked.
    Untracked,
}

/// The total of `counted_rows` under `track`. Under `UpTo(n)` each index counts at most `n + 1`
/// rows, so a sum above `n` proves at least `n` and anything else is exact.
pub(crate) const fn summarize_total_hits(counted_rows: u64, track: &TrackTotalHits) -> TotalHits {
    match track {
        TrackTotalHits::UpTo(threshold) if counted_rows > *threshold => TotalHits::AtLeast(*threshold),
        TrackTotalHits::Exact | TrackTotalHits::UpTo(_) => TotalHits::Exact(counted_rows),
        TrackTotalHits::Disabled => TotalHits::Untracked,
    }
}

/// `hits.total`: `{value, relation}`, an integer with `rest_total_hits_as_int`, `-1` for an
/// untracked total with `rest_total_hits_as_int`, and no key for an untracked total without it.
pub(crate) fn render_total_hits(total: TotalHits, rest_total_hits_as_int: bool) -> Option<Value> {
    match (total, rest_total_hits_as_int) {
        (TotalHits::Exact(value) | TotalHits::AtLeast(value), true) => Some(json!(value)),
        (TotalHits::Exact(value), false) => Some(json!({"value": value, "relation": "eq"})),
        (TotalHits::AtLeast(value), false) => Some(json!({"value": value, "relation": "gte"})),
        (TotalHits::Untracked, true) => Some(json!(-1)),
        (TotalHits::Untracked, false) => None,
    }
}

/// The JSON rendering of `array[row]`.
///
/// Timestamps are ISO-8601 UTC with a six-digit microsecond fraction, fixed-width ids lowercase
/// hex, a MAP one object keyed by its stored keys (dots kept), a list an array, a struct an
/// object without its null fields. Integers render exactly, an unsigned 64-bit one included. A
/// non-finite double renders as the string `NaN`, `Infinity` or `-Infinity`, because JSON has no
/// number for it. A date is `YYYY-MM-DD`; a year past 9999 carries a `+` sign.
///
/// # Errors
///
/// Returns `500` for an Arrow type with no arm below (`Binary`, `Time64`, `Date64` or
/// `FixedSizeList`, for some), and for a timestamp or a date outside the range `chrono`
/// represents.
pub(crate) fn render_array_value(array: &dyn Array, row: usize) -> Result<Value, OpenSearchError> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match array.data_type() {
        DataType::Utf8 => json!(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(array.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(array.as_string_view().value(row)),
        DataType::Boolean => json!(array.as_boolean().value(row)),
        DataType::Int8 => json!(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => json!(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => json!(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => json!(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => json!(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => json!(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => json!(array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => json!(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => render_double(f64::from(array.as_primitive::<Float32Type>().value(row))),
        DataType::Float64 => render_double(array.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal128(_, _) => {
            let text = array.as_primitive::<Decimal128Type>().value_as_string(row);
            render_double(
                text.parse::<f64>()
                    .map_err(|error| OpenSearchError::internal(error.to_string()))?,
            )
        }
        DataType::Timestamp(unit, _) => json!(render_iso_micros(read_timestamp_micros(array, *unit, row))?),
        DataType::Date32 => json!(render_iso_date(array.as_primitive::<Date32Type>().value(row))?),
        DataType::FixedSizeBinary(_) => json!(hex::encode(array.as_fixed_size_binary().value(row))),
        DataType::Map(_, _) => {
            let entries = array.as_map().value(row);
            let keys = entries.column(0);
            let values = entries.column(1);
            let mut object = Map::with_capacity(entries.len());
            for entry in 0..entries.len() {
                let Value::String(key) = render_array_value(keys.as_ref(), entry)? else {
                    return Err(OpenSearchError::internal("a MAP column has non-string keys"));
                };
                object.insert(key, render_array_value(values.as_ref(), entry)?);
            }
            Value::Object(object)
        }
        DataType::List(_) => {
            let items = array.as_list::<i32>().value(row);
            Value::Array(
                (0..items.len())
                    .map(|item| render_array_value(items.as_ref(), item))
                    .collect::<Result<_, _>>()?,
            )
        }
        DataType::LargeList(_) => {
            let items = array.as_list::<i64>().value(row);
            Value::Array(
                (0..items.len())
                    .map(|item| render_array_value(items.as_ref(), item))
                    .collect::<Result<_, _>>()?,
            )
        }
        DataType::Struct(fields) => {
            let structure = array.as_struct();
            let mut object = Map::with_capacity(fields.len());
            for (field, column) in fields.iter().zip(structure.columns()) {
                if !column.is_null(row) {
                    object.insert(field.name().clone(), render_array_value(column.as_ref(), row)?);
                }
            }
            Value::Object(object)
        }
        other => {
            return Err(OpenSearchError::internal(format!(
                "no JSON rendering exists for a column of type {other}"
            )));
        }
    })
}

/// The microseconds since the epoch of a timestamp of `unit` (nanoseconds truncate toward the
/// earlier microsecond).
pub(crate) fn read_timestamp_micros(array: &dyn Array, unit: TimeUnit, row: usize) -> i64 {
    match unit {
        TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row).saturating_mul(1_000_000),
        TimeUnit::Millisecond => array
            .as_primitive::<TimestampMillisecondType>()
            .value(row)
            .saturating_mul(1_000),
        TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row),
        TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row).div_euclid(1_000),
    }
}

/// `micros` as ISO-8601 UTC with a six-digit fraction (`2025-10-09T08:53:20.123456Z`).
///
/// # Errors
///
/// Returns `500` when `micros` is outside the range `chrono` represents.
pub(crate) fn render_iso_micros(micros: i64) -> Result<String, OpenSearchError> {
    convert_micros_to_instant(micros).map(|instant| instant.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
}

/// The UTC instant `micros` after the epoch; the one place a stored timestamp's range is checked,
/// so [`validate_date_micros`] and [`render_iso_micros`] refuse it with the same `500`.
///
/// # Errors
///
/// Returns `500` when `micros` is outside the range `chrono` represents.
fn convert_micros_to_instant(micros: i64) -> Result<DateTime<Utc>, OpenSearchError> {
    DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| OpenSearchError::internal(format!("timestamp {micros} µs is out of range")))
}

/// The day `days` after the epoch as `YYYY-MM-DD`.
///
/// # Errors
///
/// Returns `500` when the day is outside the range `chrono` represents.
fn render_iso_date(days: i32) -> Result<String, OpenSearchError> {
    TimeDelta::try_days(i64::from(days))
        .and_then(|offset| DateTime::UNIX_EPOCH.date_naive().checked_add_signed(offset))
        .map(|date| date.format("%Y-%m-%d").to_string())
        .ok_or_else(|| build_date_range_error(days))
}

/// The `500` for a stored day `days` after the epoch that no date or instant represents, shared
/// by the `fields` and `_source` renderers of a `Date32` value so they refuse it alike.
fn build_date_range_error(days: i32) -> OpenSearchError {
    OpenSearchError::internal(format!("date {days} days after the epoch is out of range"))
}

/// `value` as a JSON number, or the string `NaN`, `Infinity` or `-Infinity` when it is not
/// finite.
fn render_double(value: f64) -> Value {
    Number::from_f64(value).map_or_else(
        || {
            json!(if value.is_nan() {
                "NaN"
            } else if value > 0.0 {
                "Infinity"
            } else {
                "-Infinity"
            })
        },
        Value::Number,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeListArray, LargeStringArray, ListArray,
        MapBuilder, StringArray, StringBuilder, StringViewArray, StructArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    };
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{Field, Fields, Int64Type, Schema};
    use icegate_common::schema::{COL_LOG_ATTRIBUTES, COL_SERVICE_NAME};
    use serde_json::json;

    use super::*;
    use crate::opensearch::error::render_envelope;

    /// A one-entry-per-row MAP of string keys and values; the first row holds two entries whose
    /// keys share a dotted prefix.
    fn build_string_map_array() -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for (key, value) in [("http.method", "GET"), ("http", "x")] {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).expect("map row");
        Arc::new(builder.finish())
    }

    /// A struct row `{code: 200, reason: null}`.
    fn build_status_struct_array() -> ArrayRef {
        let fields = Fields::from(vec![
            Field::new("code", DataType::Int32, true),
            Field::new("reason", DataType::Utf8, true),
        ]);
        Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![Some(200)])) as ArrayRef,
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ],
            Some(NullBuffer::from(vec![true])),
        ))
    }

    /// One row per arm of [`render_array_value`]: each column is rendered at row 0. The arms are
    /// a `match` over the Arrow type, so a row per type covers every arm; the float rows also
    /// cover each non-finite spelling, and the timestamp rows each unit and the truncation of
    /// nanoseconds toward the earlier microsecond.
    #[test]
    fn every_column_type_renders_to_its_json_form() {
        let cases: Vec<(&str, ArrayRef, Value)> = vec![
            ("null", Arc::new(Int32Array::from(vec![None::<i32>])), Value::Null),
            ("utf8", Arc::new(StringArray::from(vec!["text"])), json!("text")),
            (
                "large utf8",
                Arc::new(LargeStringArray::from(vec!["text"])),
                json!("text"),
            ),
            (
                "utf8 view",
                Arc::new(StringViewArray::from(vec!["text"])),
                json!("text"),
            ),
            ("boolean", Arc::new(BooleanArray::from(vec![true])), json!(true)),
            ("int8", Arc::new(Int8Array::from(vec![-8_i8])), json!(-8)),
            ("int16", Arc::new(Int16Array::from(vec![-16_i16])), json!(-16)),
            ("int32", Arc::new(Int32Array::from(vec![-32_i32])), json!(-32)),
            (
                "int64",
                Arc::new(Int64Array::from(vec![i64::MIN])),
                json!(-9_223_372_036_854_775_808_i64),
            ),
            ("uint8", Arc::new(UInt8Array::from(vec![8_u8])), json!(8)),
            ("uint16", Arc::new(UInt16Array::from(vec![16_u16])), json!(16)),
            (
                "uint32",
                Arc::new(UInt32Array::from(vec![u32::MAX])),
                json!(4_294_967_295_u32),
            ),
            (
                "uint64",
                Arc::new(UInt64Array::from(vec![u64::MAX])),
                json!(18_446_744_073_709_551_615_u64),
            ),
            ("float32", Arc::new(Float32Array::from(vec![1.5_f32])), json!(1.5)),
            ("float64", Arc::new(Float64Array::from(vec![2.25_f64])), json!(2.25)),
            ("NaN", Arc::new(Float64Array::from(vec![f64::NAN])), json!("NaN")),
            (
                "infinity",
                Arc::new(Float32Array::from(vec![f32::INFINITY])),
                json!("Infinity"),
            ),
            (
                "negative infinity",
                Arc::new(Float64Array::from(vec![f64::NEG_INFINITY])),
                json!("-Infinity"),
            ),
            (
                "decimal128",
                Arc::new(
                    Decimal128Array::from(vec![12_345_i128])
                        .with_precision_and_scale(10, 2)
                        .expect("decimal type"),
                ),
                json!(123.45),
            ),
            (
                "timestamp seconds",
                Arc::new(TimestampSecondArray::from(vec![1_i64])),
                json!("1970-01-01T00:00:01.000000Z"),
            ),
            (
                "timestamp milliseconds",
                Arc::new(TimestampMillisecondArray::from(vec![1_500_i64])),
                json!("1970-01-01T00:00:01.500000Z"),
            ),
            (
                "timestamp microseconds",
                Arc::new(TimestampMicrosecondArray::from(vec![1_760_000_000_123_456_i64]).with_timezone("UTC")),
                json!("2025-10-09T08:53:20.123456Z"),
            ),
            (
                "timestamp nanoseconds",
                Arc::new(TimestampNanosecondArray::from(vec![1_999_i64])),
                json!("1970-01-01T00:00:00.000001Z"),
            ),
            (
                "timestamp nanoseconds before the epoch",
                Arc::new(TimestampNanosecondArray::from(vec![-1_i64])),
                json!("1969-12-31T23:59:59.999999Z"),
            ),
            ("date32", Arc::new(Date32Array::from(vec![20_000])), json!("2024-10-04")),
            (
                "date32 before the epoch",
                Arc::new(Date32Array::from(vec![-1])),
                json!("1969-12-31"),
            ),
            (
                "date32 in the year 10000",
                Arc::new(Date32Array::from(vec![2_932_897])),
                json!("+10000-01-01"),
            ),
            (
                "fixed-size binary",
                Arc::new(FixedSizeBinaryArray::from(vec![&[0xab_u8, 0x01][..]])),
                json!("ab01"),
            ),
            (
                "map",
                build_string_map_array(),
                json!({"http.method": "GET", "http": "x"}),
            ),
            (
                "list",
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    None,
                    Some(3),
                ])])),
                json!([1, null, 3]),
            ),
            (
                "large list",
                Arc::new(LargeListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(
                    vec![Some(4), Some(5)],
                )])),
                json!([4, 5]),
            ),
            ("struct", build_status_struct_array(), json!({"code": 200})),
        ];
        for (name, array, expected) in cases {
            assert_eq!(render_array_value(array.as_ref(), 0), Ok(expected), "{name}");
        }
    }

    /// Every sort key's value type renders by its own rule and a missing value as `null`; a
    /// present value of a type no sort key produces is a 500 rather than a `null`.
    #[tokio::test]
    async fn sort_values_render_by_their_type_and_an_unexpected_type_is_a_500() {
        let cases = [
            (ScalarValue::Int64(Some(-5)), json!(-5)),
            (ScalarValue::Float64(Some(2.5)), json!(2.5)),
            (ScalarValue::Float64(Some(f64::NAN)), json!("NaN")),
            (ScalarValue::Utf8(Some("a".to_string())), json!("a")),
            (ScalarValue::FixedSizeBinary(2, Some(vec![0xab, 0x01])), json!("ab01")),
            (ScalarValue::Int64(None), Value::Null),
            (ScalarValue::Utf8(None), Value::Null),
            (ScalarValue::FixedSizeBinary(2, None), Value::Null),
        ];
        for (value, expected) in cases {
            assert_eq!(render_sort_scalar(&value), Ok(expected), "{value:?}");
        }
        for value in [ScalarValue::Int32(Some(1)), ScalarValue::Boolean(Some(true))] {
            let error = render_sort_scalar(&value).expect_err("no sort key produces this type");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status.as_u16(), 500, "{value:?}");
            assert_eq!(body["error"]["type"], "exception", "{value:?}");
        }
    }

    /// A type with no rendering arm, and a timestamp or a date past the range `chrono`
    /// represents, are a 500 rather than a dropped or invented value.
    #[tokio::test]
    async fn an_unrenderable_value_is_a_500() {
        let cases: Vec<(&str, ArrayRef)> = vec![
            ("binary", Arc::new(BinaryArray::from(vec![&b"blob"[..]]))),
            (
                "timestamp out of range",
                Arc::new(TimestampSecondArray::from(vec![i64::MAX])),
            ),
            ("date out of range", Arc::new(Date32Array::from(vec![i32::MAX]))),
        ];
        for (name, array) in cases {
            let error = render_array_value(array.as_ref(), 0).expect_err("no JSON rendering");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status.as_u16(), 500, "{name}");
            assert_eq!(body["error"]["type"], "exception", "{name}");
        }
    }

    #[tokio::test]
    async fn date_formats_render_microseconds_or_milliseconds_as_their_precision_states() {
        let micros = 1_760_000_000_123_456;
        let cases = [
            (DateRenderFormat::DateTime, json!("2025-10-09T08:53:20.123Z")),
            (DateRenderFormat::StrictDateTime, json!("2025-10-09T08:53:20.123Z")),
            (
                DateRenderFormat::StrictDateOptionalTime,
                json!("2025-10-09T08:53:20.123Z"),
            ),
            (
                DateRenderFormat::StrictDateOptionalTimeNanos,
                json!("2025-10-09T08:53:20.123456Z"),
            ),
            (DateRenderFormat::EpochMillis, json!("1760000000123")),
        ];
        for (format, expected) in cases {
            assert_eq!(render_date(micros, format), expected, "{format:?}");
        }
        assert_eq!(
            render_date(-1, DateRenderFormat::StrictDateOptionalTime),
            json!("1969-12-31T23:59:59.999Z"),
            "milliseconds floor"
        );
        assert_eq!(
            render_date(-1, DateRenderFormat::StrictDateOptionalTimeNanos),
            json!("1969-12-31T23:59:59.999999Z")
        );
        assert_eq!(render_date(-1, DateRenderFormat::EpochMillis), json!("-1"));

        for (name, format) in [
            ("date_time", DateRenderFormat::DateTime),
            ("strict_date_time", DateRenderFormat::StrictDateTime),
            ("strict_date_optional_time", DateRenderFormat::StrictDateOptionalTime),
            (
                "strict_date_optional_time_nanos",
                DateRenderFormat::StrictDateOptionalTimeNanos,
            ),
            ("epoch_millis", DateRenderFormat::EpochMillis),
        ] {
            assert_eq!(DateRenderFormat::parse(name), Ok(format), "{name}");
        }
        for name in ["basic_date", "DATE_TIME"] {
            let error = DateRenderFormat::parse(name).expect_err("an unsupported format");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status.as_u16(), 400, "{name}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{name}");
        }
    }

    /// A `fields` date renders in its format from a timestamp of any unit and from a day; an
    /// instant outside the range `chrono` represents is a 500, as it is in `_source`, rather than
    /// a `null` or an overflowing multiplication.
    #[tokio::test]
    async fn field_dates_render_in_their_format_and_an_unrepresentable_date_is_a_500() {
        let cases: Vec<(&str, ArrayRef, Option<DateRenderFormat>, Value)> = vec![
            (
                "microseconds, default format",
                Arc::new(TimestampMicrosecondArray::from(vec![1_760_000_000_123_456_i64])),
                None,
                json!("2025-10-09T08:53:20.123Z"),
            ),
            (
                "milliseconds, nanosecond format",
                Arc::new(TimestampMillisecondArray::from(vec![1_500_i64])),
                Some(DateRenderFormat::StrictDateOptionalTimeNanos),
                json!("1970-01-01T00:00:01.500000Z"),
            ),
            (
                "a day before the epoch, epoch milliseconds",
                Arc::new(Date32Array::from(vec![-1])),
                Some(DateRenderFormat::EpochMillis),
                json!("-86400000"),
            ),
            (
                "a day, default format",
                Arc::new(Date32Array::from(vec![20_000])),
                None,
                json!("2024-10-04T00:00:00.000Z"),
            ),
            (
                "a keyword ignores the format",
                Arc::new(StringArray::from(vec!["GET"])),
                Some(DateRenderFormat::DateTime),
                json!("GET"),
            ),
        ];
        for (name, array, format, expected) in cases {
            assert_eq!(render_field_value(array.as_ref(), 0, format), Ok(expected), "{name}");
        }

        let unrepresentable_dates: Vec<(&str, ArrayRef)> = vec![
            (
                "a microsecond timestamp of i64::MAX",
                Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX])),
            ),
            ("a day of i32::MAX", Arc::new(Date32Array::from(vec![i32::MAX]))),
        ];
        for (name, array) in unrepresentable_dates {
            let error = render_field_value(array.as_ref(), 0, None).expect_err("no date rendering");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status.as_u16(), 500, "{name}");
            assert_eq!(body["error"]["type"], "exception", "{name}");
        }
    }

    /// A `service_name` column and a `log_attributes` MAP whose stored keys include a dotted key
    /// and the key that is its dotted prefix.
    fn build_attributes_batch() -> RecordBatch {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for (key, value) in [("http.method", "GET"), ("http", "x"), ("user.email", "a@example.com")] {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).expect("map row");
        let attributes: ArrayRef = Arc::new(builder.finish());
        let schema = Schema::new(vec![
            Field::new(COL_SERVICE_NAME, DataType::Utf8, true),
            Field::new(COL_LOG_ATTRIBUTES, attributes.data_type().clone(), true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec![Some("checkout")])), attributes],
        )
        .expect("batch")
    }

    fn build_paths_filter(includes: &[String], excludes: &[String]) -> SourceFilter {
        SourceFilter::Paths {
            includes: includes.to_vec(),
            excludes: excludes.to_vec(),
        }
    }

    #[test]
    fn source_filtering_keeps_a_map_column_as_one_object_keyed_by_stored_keys() {
        let batch = build_attributes_batch();
        let whole_source = json!({
            (COL_SERVICE_NAME): "checkout",
            (COL_LOG_ATTRIBUTES): {"http.method": "GET", "http": "x", "user.email": "a@example.com"}
        });
        let cases = [
            (SourceFilter::All, whole_source.clone()),
            (build_paths_filter(&[], &[]), whole_source),
            (SourceFilter::Nothing, json!({})),
            (
                build_paths_filter(&[COL_SERVICE_NAME.to_string()], &[]),
                json!({(COL_SERVICE_NAME): "checkout"}),
            ),
            (
                build_paths_filter(&[COL_LOG_ATTRIBUTES.to_string()], &[]),
                json!({(COL_LOG_ATTRIBUTES): {"http.method": "GET", "http": "x", "user.email": "a@example.com"}}),
            ),
            (
                build_paths_filter(&[format!("{COL_LOG_ATTRIBUTES}.http*")], &[]),
                json!({(COL_LOG_ATTRIBUTES): {"http.method": "GET", "http": "x"}}),
            ),
            (
                build_paths_filter(&[], &[format!("{COL_LOG_ATTRIBUTES}.user.*")]),
                json!({(COL_SERVICE_NAME): "checkout", (COL_LOG_ATTRIBUTES): {"http.method": "GET", "http": "x"}}),
            ),
            (
                build_paths_filter(&["*".to_string()], &[COL_LOG_ATTRIBUTES.to_string()]),
                json!({(COL_SERVICE_NAME): "checkout"}),
            ),
            (
                build_paths_filter(&[format!("{COL_LOG_ATTRIBUTES}.nothing")], &[]),
                json!({}),
            ),
        ];
        for (filter, expected) in cases {
            assert_eq!(render_source(&batch, 0, &filter), Ok(expected), "{filter:?}");
        }
    }

    #[test]
    fn track_total_hits_renders_threshold_exact_and_disabled_totals() {
        let cases = [
            (
                12_000,
                TrackTotalHits::UpTo(10_000),
                false,
                Some(json!({"value": 10_000, "relation": "gte"})),
            ),
            (
                10_000,
                TrackTotalHits::UpTo(10_000),
                false,
                Some(json!({"value": 10_000, "relation": "eq"})),
            ),
            (
                9_999,
                TrackTotalHits::UpTo(10_000),
                false,
                Some(json!({"value": 9_999, "relation": "eq"})),
            ),
            (
                12_000,
                TrackTotalHits::Exact,
                false,
                Some(json!({"value": 12_000, "relation": "eq"})),
            ),
            (12_000, TrackTotalHits::Exact, true, Some(json!(12_000))),
            (
                5,
                TrackTotalHits::UpTo(3),
                false,
                Some(json!({"value": 3, "relation": "gte"})),
            ),
            (0, TrackTotalHits::Disabled, false, None),
            (0, TrackTotalHits::Disabled, true, Some(json!(-1))),
        ];
        for (counted_rows, track, rest_total_hits_as_int, expected) in cases {
            let total = summarize_total_hits(counted_rows, &track);
            assert_eq!(
                render_total_hits(total, rest_total_hits_as_int),
                expected,
                "{counted_rows} {track:?}"
            );
        }
        assert_eq!(summarize_total_hits(4, &TrackTotalHits::UpTo(3)), TotalHits::AtLeast(3));
        assert_eq!(summarize_total_hits(3, &TrackTotalHits::UpTo(3)), TotalHits::Exact(3));
    }
}
