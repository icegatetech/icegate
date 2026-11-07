//! Rendering of the `_search` response and its hits.

use std::time::Duration;

use chrono::{DateTime, TimeDelta};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use serde_json::{Map, Number, Value, json};

use super::request::SearchRequest;
use crate::opensearch::error::OpenSearchError;

/// One hit of a response page: a row of one index's batch.
pub(crate) struct SearchHit<'a> {
    /// The concrete index name the row was read from (`_index`).
    pub(crate) index: &'a str,
    /// The batch holding the row; its columns are the index's tenant-scoped columns.
    pub(crate) batch: &'a RecordBatch,
    /// The row within `batch`.
    pub(crate) row: usize,
}

/// The `_search` response, with its keys in this fixed order: `took`, `timed_out`, `_shards`,
/// `hits`.
///
/// `hits.total` is the exact `total` (an integer with `rest_total_hits_as_int`). Every hit
/// scores `1.0`; `max_score` is `1.0` when a hit is returned and `null` otherwise.
///
/// # Errors
///
/// Returns `500` when a row holds a value no JSON rendering exists for.
pub(crate) fn render_search_response(
    took: Duration,
    total: u64,
    hits: &[SearchHit<'_>],
    request: &SearchRequest,
) -> Result<Value, OpenSearchError> {
    let rendered_hits = hits.iter().map(render_hit).collect::<Result<Vec<_>, _>>()?;
    let max_score = if rendered_hits.is_empty() {
        Value::Null
    } else {
        json!(1.0)
    };
    let total = if request.rest_total_hits_as_int {
        json!(total)
    } else {
        json!({"value": total, "relation": "eq"})
    };
    Ok(json!({
        "took": u64::try_from(took.as_millis()).unwrap_or(u64::MAX),
        "timed_out": false,
        "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        "hits": {"total": total, "max_score": max_score, "hits": rendered_hits},
    }))
}

/// One hit object: `_index`, `_score` and the row's `_source`.
fn render_hit(hit: &SearchHit<'_>) -> Result<Value, OpenSearchError> {
    let mut object = Map::new();
    object.insert("_index".to_string(), json!(hit.index));
    object.insert("_score".to_string(), json!(1.0));
    object.insert(
        "_source".to_string(),
        Value::Object(render_row_source(hit.batch, hit.row)?),
    );
    Ok(Value::Object(object))
}

/// The `_source` of `row`: every non-null column, in schema order.
fn render_row_source(batch: &RecordBatch, row: usize) -> Result<Map<String, Value>, OpenSearchError> {
    let mut source = Map::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if column.is_null(row) {
            continue;
        }
        source.insert(field.name().clone(), render_array_value(column.as_ref(), row)?);
    }
    Ok(source)
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
    DateTime::from_timestamp_micros(micros)
        .map(|instant| instant.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
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
        .ok_or_else(|| OpenSearchError::internal(format!("date {days} days after the epoch is out of range")))
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
    use datafusion::arrow::datatypes::{Field, Fields, Int64Type};
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
}
