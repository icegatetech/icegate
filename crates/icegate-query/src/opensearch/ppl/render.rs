//! Rendering of PPL results.

use chrono::{DateTime, NaiveDate};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use serde_json::{Map, Value, json};

use crate::opensearch::search::read_timestamp_micros;
use crate::ppl::{PplColumn, PplError};

/// Microseconds in one second.
const MICROS_PER_SECOND: i64 = 1_000_000;

/// The jdbc response: `{schema: [{name, type}], datarows, total, size}`, where `total` and `size`
/// both count the rows returned. There is no `status` and no `cursor`.
///
/// Cells render as: timestamps `yyyy-MM-dd HH:mm:ss[.f]` in UTC with 1 to 6 fraction digits and
/// trailing zeros dropped (none when zero); dates `yyyy-MM-dd`; fixed-size binary ids as
/// lowercase hex strings; a MAP as one object keyed by its stored keys, never split on the dots
/// inside a key; a struct as an object without its NULL fields; a list as an array; `NaN` and the
/// infinities as the strings `"NaN"`, `"Infinity"` and `"-Infinity"`; NULL as `null`.
///
/// # Errors
///
/// Returns [`PplError::Internal`] when a batch does not match `columns` or holds a type the
/// planner never returns.
pub(crate) fn render_jdbc(columns: &[PplColumn], batches: &[RecordBatch]) -> Result<Value, PplError> {
    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let mut datarows = Vec::with_capacity(row_count);
    for batch in batches {
        if batch.num_columns() != columns.len() {
            return Err(PplError::Internal {
                reason: format!(
                    "a result batch has {} columns, the plan {}",
                    batch.num_columns(),
                    columns.len()
                ),
            });
        }
        for row in 0..batch.num_rows() {
            let cells = batch
                .columns()
                .iter()
                .map(|column| render_cell(column.as_ref(), row))
                .collect::<Result<Vec<_>, _>>()?;
            datarows.push(Value::Array(cells));
        }
    }
    let schema: Vec<Value> = columns
        .iter()
        .map(|column| json!({"name": column.name, "type": column.ppl_type.name()}))
        .collect();
    Ok(json!({"schema": schema, "datarows": datarows, "total": row_count, "size": row_count}))
}

/// The JSON value of row `row` of `array`.
fn render_cell(array: &dyn Array, row: usize) -> Result<Value, PplError> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    let value = match array.data_type() {
        DataType::Utf8 => Value::String(array.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => Value::String(array.as_string::<i64>().value(row).to_string()),
        DataType::Utf8View => Value::String(array.as_string_view().value(row).to_string()),
        DataType::FixedSizeBinary(_) => Value::String(hex::encode(array.as_fixed_size_binary().value(row))),
        DataType::Boolean => Value::Bool(array.as_boolean().value(row)),
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
        DataType::Timestamp(unit, _) => Value::String(format_timestamp(read_timestamp_micros(array, *unit, row))?),
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row);
            Value::String(format_date(i64::from(days))?)
        }
        DataType::Map(_, _) => render_map(array, row)?,
        DataType::Struct(_) => render_struct(array, row)?,
        DataType::List(_) => render_list(array.as_list::<i32>().value(row).as_ref())?,
        DataType::LargeList(_) => render_list(array.as_list::<i64>().value(row).as_ref())?,
        DataType::FixedSizeList(_, _) => render_list(array.as_fixed_size_list().value(row).as_ref())?,
        DataType::Null => Value::Null,
        other => {
            return Err(PplError::Internal {
                reason: format!("a PPL result holds a {other} column"),
            });
        }
    };
    Ok(value)
}

/// A finite double as a JSON number; `NaN` and the infinities as JSON strings, since a JSON number
/// cannot hold them.
fn render_double(value: f64) -> Value {
    if value.is_nan() {
        Value::String("NaN".to_string())
    } else if value.is_infinite() {
        Value::String(if value > 0.0 { "Infinity" } else { "-Infinity" }.to_string())
    } else {
        json!(value)
    }
}

/// `yyyy-MM-dd HH:mm:ss[.f]` in UTC: up to 6 fraction digits, trailing zeros dropped, none when
/// the fraction is zero.
fn format_timestamp(micros: i64) -> Result<String, PplError> {
    let instant = DateTime::from_timestamp_micros(micros).ok_or_else(|| PplError::Internal {
        reason: format!("timestamp {micros} µs is out of range"),
    })?;
    let seconds = instant.format("%Y-%m-%d %H:%M:%S").to_string();
    let fraction = micros.rem_euclid(MICROS_PER_SECOND);
    if fraction == 0 {
        return Ok(seconds);
    }
    let digits = format!("{fraction:06}");
    Ok(format!("{seconds}.{}", digits.trim_end_matches('0')))
}

fn format_date(days_since_epoch: i64) -> Result<String, PplError> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| PplError::Internal {
        reason: "the Unix epoch is not a date".to_string(),
    })?;
    let date = epoch
        .checked_add_signed(chrono::Duration::days(days_since_epoch))
        .ok_or_else(|| PplError::Internal {
            reason: format!("date {days_since_epoch} days after the epoch is out of range"),
        })?;
    Ok(date.format("%Y-%m-%d").to_string())
}

/// A MAP row as one object keyed by its stored keys, in stored order.
fn render_map(array: &dyn Array, row: usize) -> Result<Value, PplError> {
    let entries = array.as_map().value(row);
    let (keys, values) = (entries.column(0), entries.column(1));
    let mut object = Map::with_capacity(entries.len());
    for index in 0..entries.len() {
        let key = match render_cell(keys.as_ref(), index)? {
            Value::String(key) => key,
            other => other.to_string(),
        };
        object.insert(key, render_cell(values.as_ref(), index)?);
    }
    Ok(Value::Object(object))
}

/// A struct row as an object of its non-NULL fields.
fn render_struct(array: &dyn Array, row: usize) -> Result<Value, PplError> {
    let fields = array.as_struct();
    let mut object = Map::with_capacity(fields.num_columns());
    for (field, column) in fields.fields().iter().zip(fields.columns()) {
        let value = render_cell(column.as_ref(), row)?;
        if !value.is_null() {
            object.insert(field.name().clone(), value);
        }
    }
    Ok(Value::Object(object))
}

fn render_list(values: &dyn Array) -> Result<Value, PplError> {
    (0..values.len())
        .map(|index| render_cell(values, index))
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::format_timestamp;

    #[test]
    fn rendered_timestamps_drop_the_trailing_zeros_of_a_six_digit_utc_fraction() {
        let cases = [
            (1_791_460_800_000_000, "2026-10-08 12:00:00"),
            (1_791_460_801_250_000, "2026-10-08 12:00:01.25"),
            (1_791_460_802_000_001, "2026-10-08 12:00:02.000001"),
            (-1, "1969-12-31 23:59:59.999999"),
        ];
        for (micros, expected) in cases {
            assert_eq!(format_timestamp(micros).as_deref(), Ok(expected), "micros {micros}");
        }
        // Random instants on both sides of the epoch against chrono's six-digit form: the
        // rendering is that form with the trailing zeros of the fraction (and a bare dot)
        // removed, and carries no `T` and no zone.
        let mut rng = StdRng::seed_from_u64(0x0005_EED5);
        let lower = Utc
            .with_ymd_and_hms(1900, 1, 1, 0, 0, 0)
            .single()
            .expect("1900")
            .timestamp_micros();
        let upper = Utc
            .with_ymd_and_hms(2200, 1, 1, 0, 0, 0)
            .single()
            .expect("2200")
            .timestamp_micros();
        for _ in 0..2_000 {
            let micros = rng.random_range(lower..upper);
            let rendered_timestamp = format_timestamp(micros).expect("in range");
            let six_digit_form = DateTime::from_timestamp_micros(micros)
                .expect("instant in range")
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string();
            let expected = six_digit_form.trim_end_matches('0').trim_end_matches('.');
            assert_eq!(rendered_timestamp, expected, "micros {micros}");
            assert!(
                !rendered_timestamp.contains('T') && !rendered_timestamp.contains('Z'),
                "rendered {rendered_timestamp}"
            );
        }
    }
}
