//! Rendering of PPL results in the `jdbc`, `viz`, `csv` and `raw` formats. One cell renderer
//! defines how every value renders in all four, and each format writes its body straight from
//! the result batches into memory the query's pool has granted.

use std::collections::HashMap;

use axum::http::header;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, NaiveDate};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use serde::Serialize;
use serde_json::{Map, Number, Value, json};

use super::handlers::PplFormat;
use crate::opensearch::catalog::QueryReservation;
use crate::opensearch::search::read_timestamp_micros;
use crate::ppl::{PplColumn, PplError, PplType};

/// Microseconds in one second.
const MICROS_PER_SECOND: i64 = 1_000_000;

/// An answer rendered in one format: its body and the content type it is served with.
#[derive(Debug)]
pub(crate) struct RenderedAnswer {
    /// The body, UTF-8 text.
    pub(crate) body: Vec<u8>,
    /// The content type of the body.
    pub(crate) content_type: &'static str,
}

impl IntoResponse for RenderedAnswer {
    fn into_response(self) -> Response {
        ([(header::CONTENT_TYPE, self.content_type)], self.body).into_response()
    }
}

/// Renders a planned query's batches in the requested format: `jdbc` and `viz` as
/// [`PPL_DOCUMENT_CONTENT_TYPE`], `csv` and `raw` as [`PPL_FLAT_TEXT_CONTENT_TYPE`].
///
/// Every format writes straight from the Arrow arrays into the body, one cell at a time through
/// [`render_cell`], so the formats cannot disagree on how a value renders and no tree of the whole
/// answer is ever built. The body grows only by what `reservation` grants first, so the answer
/// counts against the per-query memory limit together with the batches it is rendered from.
///
/// # Errors
///
/// [`PplError::MemoryLimitExceeded`] when the body would grow past what the memory pool grants;
/// [`PplError::Internal`] when a batch does not match `columns` or holds a type the planner never
/// returns.
pub(crate) fn render_ppl_answer(
    format: PplFormat,
    columns: &[PplColumn],
    batches: &[RecordBatch],
    reservation: &QueryReservation,
) -> Result<RenderedAnswer, PplError> {
    check_batch_widths(columns, batches)?;
    let mut body = ReservedBody::new(reservation);
    let content_type = match format {
        PplFormat::Jdbc => {
            write_jdbc(&mut body, columns, batches)?;
            PPL_DOCUMENT_CONTENT_TYPE
        }
        PplFormat::Viz => {
            write_viz(&mut body, columns, batches)?;
            PPL_DOCUMENT_CONTENT_TYPE
        }
        PplFormat::Csv => {
            write_flat(&mut body, columns, batches, FlatLayout::Csv)?;
            PPL_FLAT_TEXT_CONTENT_TYPE
        }
        PplFormat::Raw => {
            write_flat(&mut body, columns, batches, FlatLayout::Raw)?;
            PPL_FLAT_TEXT_CONTENT_TYPE
        }
    };
    Ok(RenderedAnswer {
        body: body.bytes,
        content_type,
    })
}

/// A body that grows only by what its reservation grants first.
struct ReservedBody<'r> {
    /// The text written so far.
    bytes: Vec<u8>,
    /// Where every growth of `bytes`' capacity is reserved before it is allocated.
    reservation: &'r QueryReservation,
}

impl<'r> ReservedBody<'r> {
    /// An empty body reserving through `reservation`.
    const fn new(reservation: &'r QueryReservation) -> Self {
        Self {
            bytes: Vec::new(),
            reservation,
        }
    }

    /// Append `text`. When it does not fit the capacity, the capacity at least doubles, as a
    /// `Vec`'s does, so appending stays amortized linear; the added capacity is reserved before it
    /// is allocated.
    ///
    /// # Errors
    ///
    /// [`PplError::MemoryLimitExceeded`] when the pool refuses the added capacity; the body is
    /// unchanged then.
    fn write_bytes(&mut self, text: &[u8]) -> Result<(), PplError> {
        let required_length = self.bytes.len().saturating_add(text.len());
        let capacity = self.bytes.capacity();
        if required_length > capacity {
            let grown_capacity = required_length.max(capacity.saturating_mul(2));
            self.reservation.reserve_bytes(grown_capacity - capacity)?;
            self.bytes.reserve_exact(grown_capacity - self.bytes.len());
        }
        self.bytes.extend_from_slice(text);
        Ok(())
    }

    /// Append the compact JSON text of `value`, through `scratch`, which holds it first.
    ///
    /// # Errors
    ///
    /// As [`Self::write_bytes`].
    fn write_json(&mut self, scratch: &mut Vec<u8>, value: &impl Serialize) -> Result<(), PplError> {
        scratch.clear();
        serde_json::to_writer(&mut *scratch, value).map_err(|error| PplError::Internal {
            reason: format!("a PPL answer value did not serialize: {error}"),
        })?;
        self.write_bytes(scratch)
    }
}

/// Refuses a batch whose width differs from `columns`: every format reads one cell per column.
fn check_batch_widths(columns: &[PplColumn], batches: &[RecordBatch]) -> Result<(), PplError> {
    batches
        .iter()
        .find(|batch| batch.num_columns() != columns.len())
        .map_or(Ok(()), |batch| {
            Err(PplError::Internal {
                reason: format!(
                    "a result batch has {} columns, the plan {}",
                    batch.num_columns(),
                    columns.len()
                ),
            })
        })
}

/// The number of rows of `batches`.
fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Writes the jdbc response: `{schema: [{name, type}], datarows, total, size}`, where `total` and
/// `size` both count the rows returned. There is no `status` and no `cursor`.
///
/// Cells render as: timestamps `yyyy-MM-dd HH:mm:ss[.f]` in UTC with 1 to 6 fraction digits and
/// trailing zeros dropped (none when zero); dates `yyyy-MM-dd`; fixed-size binary ids as
/// lowercase hex strings; a MAP as one object keyed by its stored keys, never split on the dots
/// inside a key; a struct as an object without its NULL fields; a list as an array; `NaN` and the
/// infinities as the strings `"NaN"`, `"Infinity"` and `"-Infinity"`; NULL as `null`.
fn write_jdbc(body: &mut ReservedBody<'_>, columns: &[PplColumn], batches: &[RecordBatch]) -> Result<(), PplError> {
    let schema: Vec<Value> = columns
        .iter()
        .map(|column| json!({"name": column.name, "type": column.ppl_type.name()}))
        .collect();
    let mut scratch = Vec::new();
    body.write_bytes(b"{\"schema\":")?;
    body.write_json(&mut scratch, &schema)?;
    body.write_bytes(b",\"datarows\":[")?;
    let mut is_first_row = true;
    for batch in batches {
        for row in 0..batch.num_rows() {
            scratch.clear();
            if !is_first_row {
                scratch.push(b',');
            }
            is_first_row = false;
            scratch.push(b'[');
            for (index, column) in batch.columns().iter().enumerate() {
                if index > 0 {
                    scratch.push(b',');
                }
                serialize_cell(&mut scratch, column.as_ref(), row)?;
            }
            scratch.push(b']');
            body.write_bytes(&scratch)?;
        }
    }
    let row_count = count_rows(batches);
    body.write_bytes(format!("],\"total\":{row_count},\"size\":{row_count}}}").as_bytes())
}

/// Appends the compact JSON text of the cell of `array` at `row` to `text`.
fn serialize_cell(text: &mut Vec<u8>, array: &dyn Array, row: usize) -> Result<(), PplError> {
    serde_json::to_writer(text, &render_cell(array, row)?).map_err(|error| PplError::Internal {
        reason: format!("a PPL answer value did not serialize: {error}"),
    })
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

/// The content type of a successful `jdbc` or `viz` answer, a JSON document.
const PPL_DOCUMENT_CONTENT_TYPE: &str = "application/json; charset=UTF-8";

/// The content type of a successful `csv` or `raw` answer, flat text. A client that parses every
/// body labelled JSON would fail on it; the E2E case `workbench runs a ppl query, explains it and
/// downloads csv` pins the download through such a client.
const PPL_FLAT_TEXT_CONTENT_TYPE: &str = "text/plain; charset=UTF-8";

/// Writes the `viz` format: `{data: {<col>: [...]}, metadata: {fields: [{name, type}]}, size,
/// status: 200}`, one array per column keyed by column name, each cell rendered as the jdbc
/// response renders it, and each field typed with its `viz` name (see [`name_viz_type`]). A name
/// that two columns share keys one array, in the place of its first column, holding the cells of
/// its last column.
fn write_viz(body: &mut ReservedBody<'_>, columns: &[PplColumn], batches: &[RecordBatch]) -> Result<(), PplError> {
    let mut keyed_columns: Vec<(&str, usize)> = Vec::with_capacity(columns.len());
    let mut key_positions: HashMap<&str, usize> = HashMap::with_capacity(columns.len());
    for (column_index, column) in columns.iter().enumerate() {
        match key_positions.get(column.name.as_str()) {
            Some(&position) => keyed_columns[position].1 = column_index,
            None => {
                key_positions.insert(column.name.as_str(), keyed_columns.len());
                keyed_columns.push((column.name.as_str(), column_index));
            }
        }
    }
    let mut scratch = Vec::new();
    body.write_bytes(b"{\"data\":{")?;
    for (key_index, (name, column_index)) in keyed_columns.into_iter().enumerate() {
        if key_index > 0 {
            body.write_bytes(b",")?;
        }
        body.write_json(&mut scratch, &name)?;
        body.write_bytes(b":[")?;
        let mut is_first_cell = true;
        for batch in batches {
            let column = batch.column(column_index);
            for row in 0..batch.num_rows() {
                scratch.clear();
                if !is_first_cell {
                    scratch.push(b',');
                }
                is_first_cell = false;
                serialize_cell(&mut scratch, column.as_ref(), row)?;
                body.write_bytes(&scratch)?;
            }
        }
        body.write_bytes(b"]")?;
    }
    let fields: Vec<Value> = columns
        .iter()
        .map(|column| json!({ "name": column.name, "type": name_viz_type(column.ppl_type) }))
        .collect();
    body.write_bytes(b"},\"metadata\":")?;
    body.write_json(&mut scratch, &json!({ "fields": fields }))?;
    body.write_bytes(format!(",\"size\":{},\"status\":200}}", count_rows(batches)).as_bytes())
}

/// The `viz` type name of a PPL type: the jdbc name, except that `string` is `keyword`, `int`
/// `integer`, `bigint` `long`, `struct` `object` and `array` `nested`. IceGate has one string
/// type, so a full-text column is `keyword` too.
const fn name_viz_type(ppl_type: PplType) -> &'static str {
    match ppl_type {
        PplType::String => "keyword",
        PplType::Int => "integer",
        PplType::Bigint => "long",
        PplType::Double => "double",
        PplType::Boolean => "boolean",
        PplType::Timestamp => "timestamp",
        PplType::Date => "date",
        PplType::Struct => "object",
        PplType::Array => "nested",
    }
}

/// The two text layouts of a PPL result.
#[derive(Debug, Clone, Copy)]
enum FlatLayout {
    /// `,`-separated, quoted where needed.
    Csv,
    /// `|`-separated, never quoted.
    Raw,
}

impl FlatLayout {
    /// The cell separator.
    const fn separator(self) -> char {
        match self {
            Self::Csv => ',',
            Self::Raw => '|',
        }
    }

    /// Appends one cell to `text`. `csv` double-quotes a cell holding `,`, `"`, `\r` or `\n` and
    /// doubles its embedded quotes; `raw` never quotes.
    fn write_cell(self, text: &mut String, cell: &str) {
        let needs_quotes = matches!(self, Self::Csv) && cell.contains([',', '"', '\r', '\n']);
        if needs_quotes {
            text.push('"');
            for (index, unquoted_part) in cell.split('"').enumerate() {
                if index > 0 {
                    text.push_str("\"\"");
                }
                text.push_str(unquoted_part);
            }
            text.push('"');
        } else {
            text.push_str(cell);
        }
    }
}

/// Writes the header line of column names, then one line per row, joined by `\n` with no
/// trailing line break: comma-separated for `csv`, a cell quoted only when it holds a separator, a
/// quote or a line break; pipe-separated and never quoted for `raw`. A top-level NULL cell is
/// empty, a NULL list column included; `null` appears only inside a list, a MAP or a struct (see
/// [`write_flat_cell`]).
fn write_flat(
    body: &mut ReservedBody<'_>,
    columns: &[PplColumn],
    batches: &[RecordBatch],
    layout: FlatLayout,
) -> Result<(), PplError> {
    let mut line = String::new();
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            line.push(layout.separator());
        }
        layout.write_cell(&mut line, &column.name);
    }
    body.write_bytes(line.as_bytes())?;
    // One buffer for every cell: a cell is written out before `layout` decides on its quoting.
    let mut cell_text = String::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            line.clear();
            line.push('\n');
            for (index, column) in batch.columns().iter().enumerate() {
                if index > 0 {
                    line.push(layout.separator());
                }
                cell_text.clear();
                write_flat_cell(&mut cell_text, &render_cell(column.as_ref(), row)?);
                layout.write_cell(&mut line, &cell_text);
            }
            body.write_bytes(line.as_bytes())?;
        }
    }
    Ok(())
}

/// Appends the text of one top-level cell to `text`: nothing for NULL (a NULL list or MAP column
/// included), otherwise the value as [`write_flat_value`] writes it. A `null` is written only for
/// a NULL inside a list, a MAP or a struct.
fn write_flat_cell(text: &mut String, cell: &Value) {
    if !cell.is_null() {
        write_flat_value(text, cell);
    }
}

/// Appends a jdbc value to `text` as flat text: strings verbatim, lists `[a, b]`, objects
/// `{k=v, k2=v2}` in key byte order with each key whole (a dotted MAP key is never split), and a
/// nested NULL as `null`.
fn write_flat_value(text: &mut String, value: &Value) {
    match value {
        Value::Null => text.push_str("null"),
        Value::Bool(is_true) => text.push_str(if *is_true { "true" } else { "false" }),
        Value::Number(number) => write_plain_number(text, number),
        Value::String(string_value) => text.push_str(string_value),
        Value::Array(items) => {
            text.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    text.push_str(", ");
                }
                write_flat_value(text, item);
            }
            text.push(']');
        }
        Value::Object(entries) => {
            let mut sorted_entries: Vec<(&String, &Value)> = entries.iter().collect();
            sorted_entries.sort_unstable_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            text.push('{');
            for (index, (key, item)) in sorted_entries.into_iter().enumerate() {
                if index > 0 {
                    text.push_str(", ");
                }
                text.push_str(key);
                text.push('=');
                write_flat_value(text, item);
            }
            text.push('}');
        }
    }
}

/// Appends a number in plain decimal: integers as they are, doubles with at least one fraction
/// digit and never an exponent.
///
/// Rust's `Display` for `f64` never uses an exponent and prints the shortest text that reads back
/// as the same double, which is the plain decimal form the flat formats promise. A number rendered
/// from an `f64` answers `None` to `as_i64` and `as_u64`, so a double `3.0` keeps its `.0`.
fn write_plain_number(text: &mut String, number: &Number) {
    if let Some(integer) = number.as_i64() {
        text.push_str(itoa::Buffer::new().format(integer));
    } else if let Some(unsigned_integer) = number.as_u64() {
        text.push_str(itoa::Buffer::new().format(unsigned_integer));
    } else if let Some(double) = number.as_f64() {
        let double_text = double.to_string();
        let is_integral = !double_text.contains('.');
        text.push_str(&double_text);
        if is_integral {
            text.push_str(".0");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, TimeZone, Utc};
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use serde_json::json;

    use super::{
        FlatLayout, PplColumn, PplError, PplFormat, PplType, QueryReservation, RecordBatch, RenderedAnswer,
        format_timestamp, name_viz_type, render_ppl_answer, write_flat_cell,
    };

    /// The columns `body` (string) and `n` (bigint) over the rows `("a,b", 1)` and `(NULL, 2)`.
    fn build_answer_batch() -> (Vec<PplColumn>, RecordBatch) {
        let columns = vec![
            PplColumn {
                name: "body".to_owned(),
                ppl_type: PplType::String,
            },
            PplColumn {
                name: "n".to_owned(),
                ppl_type: PplType::Bigint,
            },
        ];
        let batch = RecordBatch::try_from_iter([
            ("body", Arc::new(StringArray::from(vec![Some("a,b"), None])) as ArrayRef),
            ("n", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ])
        .expect("the answer batch");
        (columns, batch)
    }

    /// `batch` rendered in `format` within a memory pool of `pool_bytes`.
    fn render_answer_in_pool(
        format: PplFormat,
        columns: &[PplColumn],
        batch: &RecordBatch,
        pool_bytes: usize,
    ) -> Result<RenderedAnswer, PplError> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
        let reservation = QueryReservation::new(&pool, "test answer", pool_bytes);
        render_ppl_answer(format, columns, std::slice::from_ref(batch), &reservation)
    }

    #[test]
    fn every_format_writes_its_body_from_the_batch_cells() {
        let (columns, batch) = build_answer_batch();
        let expected_answers = [
            (
                PplFormat::Jdbc,
                r#"{"schema":[{"name":"body","type":"string"},{"name":"n","type":"bigint"}],"datarows":[["a,b",1],[null,2]],"total":2,"size":2}"#,
                "application/json; charset=UTF-8",
            ),
            (
                PplFormat::Viz,
                r#"{"data":{"body":["a,b",null],"n":[1,2]},"metadata":{"fields":[{"name":"body","type":"keyword"},{"name":"n","type":"long"}]},"size":2,"status":200}"#,
                "application/json; charset=UTF-8",
            ),
            (PplFormat::Csv, "body,n\n\"a,b\",1\n,2", "text/plain; charset=UTF-8"),
            (PplFormat::Raw, "body|n\na,b|1\n|2", "text/plain; charset=UTF-8"),
        ];
        for (format, expected_body, expected_content_type) in expected_answers {
            let answer = render_answer_in_pool(format, &columns, &batch, 4_096).expect("the answer fits the pool");
            assert_eq!(
                String::from_utf8(answer.body).expect("UTF-8"),
                expected_body,
                "{format:?}"
            );
            assert_eq!(answer.content_type, expected_content_type, "{format:?}");
        }
    }

    #[test]
    fn a_viz_name_two_columns_share_keys_the_cells_of_the_last_in_the_place_of_the_first() {
        let (mut columns, batch) = build_answer_batch();
        columns[1].name = "body".to_owned();
        let answer = render_answer_in_pool(PplFormat::Viz, &columns, &batch, 4_096).expect("the answer fits the pool");
        assert_eq!(
            String::from_utf8(answer.body).expect("UTF-8"),
            r#"{"data":{"body":[1,2]},"metadata":{"fields":[{"name":"body","type":"keyword"},{"name":"body","type":"long"}]},"size":2,"status":200}"#
        );
    }

    #[test]
    fn a_body_growing_past_the_memory_pool_is_the_memory_limit_error_and_returns_its_memory() {
        let (columns, batch) = build_answer_batch();
        for format in [PplFormat::Jdbc, PplFormat::Viz, PplFormat::Csv, PplFormat::Raw] {
            // Every body above is longer than 16 bytes.
            assert_eq!(
                render_answer_in_pool(format, &columns, &batch, 16).map(|answer| answer.body),
                Err(PplError::MemoryLimitExceeded { limit_bytes: 16 }),
                "{format:?}"
            );
        }
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4_096));
        let reservation = QueryReservation::new(&pool, "test answer", 4_096);
        let answer = render_ppl_answer(PplFormat::Jdbc, &columns, std::slice::from_ref(&batch), &reservation)
            .expect("the answer fits the pool");
        // The body's whole capacity is reserved while the reservation lives, and returned with it.
        assert!(pool.reserved() >= answer.body.len(), "reserved {}", pool.reserved());
        drop(reservation);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn a_batch_narrower_than_the_columns_is_an_internal_error() {
        let (mut columns, batch) = build_answer_batch();
        columns.push(PplColumn {
            name: "extra".to_owned(),
            ppl_type: PplType::String,
        });
        assert!(
            matches!(
                render_answer_in_pool(PplFormat::Jdbc, &columns, &batch, 4_096),
                Err(PplError::Internal { .. })
            ),
            "a batch of two columns under three"
        );
    }

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

    #[test]
    fn flat_cells_render_nested_values_lists_and_plain_decimals() {
        for (value, expected) in [
            (json!(null), ""),
            (json!("plain"), "plain"),
            (json!(true), "true"),
            (json!(42), "42"),
            (json!(-7), "-7"),
            (json!(3.0), "3.0"),
            (json!(0.5), "0.5"),
            (json!(1e21), "1000000000000000000000.0"),
            (json!("NaN"), "NaN"),
            (json!([]), "[]"),
            (json!(["a", null, 2]), "[a, null, 2]"),
            (json!({}), "{}"),
            (
                json!({"http.method": "GET", "Zone": "eu"}),
                "{Zone=eu, http.method=GET}",
            ),
            (json!({"outer": {"b": null, "a": [1.5]}}), "{outer={a=[1.5], b=null}}"),
            (json!({"at": "2026-10-08 12:00:00.5"}), "{at=2026-10-08 12:00:00.5}"),
        ] {
            let mut cell_text = String::new();
            write_flat_cell(&mut cell_text, &value);
            assert_eq!(cell_text, expected, "{value}");
        }
    }

    #[test]
    fn csv_cells_are_quoted_only_when_they_hold_a_separator_quote_or_line_break() {
        for (cell, expected) in [
            ("plain", "plain"),
            ("a,b", "\"a,b\""),
            ("say \"hi\"", "\"say \"\"hi\"\"\""),
            ("line1\nline2", "\"line1\nline2\""),
            ("carriage\rreturn", "\"carriage\rreturn\""),
            ("x|y", "x|y"),
        ] {
            let mut csv_text = String::new();
            FlatLayout::Csv.write_cell(&mut csv_text, cell);
            assert_eq!(csv_text, expected, "{cell:?}");
            let mut raw_text = String::new();
            FlatLayout::Raw.write_cell(&mut raw_text, cell);
            assert_eq!(raw_text, cell, "raw never quotes {cell:?}");
        }
    }

    #[test]
    fn viz_type_names_differ_from_jdbc_for_string_int_bigint_struct_and_array() {
        for (ppl_type, expected) in [
            (PplType::String, "keyword"),
            (PplType::Int, "integer"),
            (PplType::Bigint, "long"),
            (PplType::Double, "double"),
            (PplType::Boolean, "boolean"),
            (PplType::Timestamp, "timestamp"),
            (PplType::Date, "date"),
            (PplType::Struct, "object"),
            (PplType::Array, "nested"),
        ] {
            assert_eq!(name_viz_type(ppl_type), expected, "{ppl_type:?}");
        }
    }
}
