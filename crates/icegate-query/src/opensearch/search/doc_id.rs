//! Document identity of data-index rows: `_id`, the `_doc` sort value and the
//! canonical row hash both are built from.
//!
//! The hash is the first 8 bytes, big-endian, of SHA-256 over the canonical row encoding. That
//! encoding is value-level over the tenant-scoped schema, which the WAL and the Iceberg scans
//! both emit, so a row hashes the same whichever side it is read from. MAP entries are sorted,
//! and NULL is distinct from every value, so the encoding is injective over the rows of one
//! schema: equal rows always share a hash, and distinct rows share one only on a 64-bit
//! collision. The [`DOC_HASH_BITS`] bits a `_doc` value keeps collide sooner, which
//! [`decode_time_and_doc`] documents. Adding a column changes the hash, and therefore the `_id`,
//! of every row of tables without a natural key.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt64Builder};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::common::{Result as DataFusionResult, exec_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use icegate_common::{OPERATIONS_TABLE, SPANS_TABLE};
use sha2::{Digest, Sha256};

use super::hits::read_timestamp_micros;
use crate::opensearch::catalog::IndexEntry;
use crate::opensearch::error::OpenSearchError;

/// Bits of the row hash a `_doc` sort value keeps.
pub(crate) const DOC_HASH_BITS: u32 = 43;
/// The mask of the row-hash bits a `_doc` sort value keeps.
pub(crate) const DOC_HASH_MASK: u64 = (1 << DOC_HASH_BITS) - 1;
/// Microseconds in a millisecond.
pub(crate) const MICROS_PER_MILLISECOND: i64 = 1_000;
/// Microseconds in a second.
pub(crate) const MICROS_PER_SECOND: i64 = 1_000 * MICROS_PER_MILLISECOND;
/// Microseconds in a minute.
pub(crate) const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
/// Microseconds in an hour.
pub(crate) const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
/// Microseconds in a day, the unit step of a `Date32` value.
pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Characters of a hashed `_id`: 8 timestamp bytes and 8 hash bytes, as hex.
const HASHED_DOCUMENT_ID_LENGTH: usize = 32;
/// Characters of a span-keyed `_id`: 16 trace-id bytes and 8 span-id bytes, as hex.
const SPAN_DOCUMENT_ID_LENGTH: usize = 48;

/// Canonical tag of a NULL value.
const TAG_NULL: u8 = 0x00;
/// Canonical tag of a string: then a `u32` BE length and the UTF-8 bytes.
const TAG_STRING: u8 = 0x01;
/// Canonical tag of an integer of at most 32 bits (`u16` and narrower widen): 4 bytes BE.
const TAG_INT32: u8 = 0x02;
/// Canonical tag of a 64-bit integer (`u32` widens): 8 bytes BE.
const TAG_INT64: u8 = 0x03;
/// Canonical tag of a double: its IEEE bits, 8 bytes BE.
const TAG_FLOAT64: u8 = 0x04;
/// Canonical tag of a float: its IEEE bits, 4 bytes BE.
const TAG_FLOAT32: u8 = 0x05;
/// Canonical tag of a boolean: one byte, 0 or 1.
const TAG_BOOLEAN: u8 = 0x06;
/// Canonical tag of a timestamp: a unit byte (0 s, 1 ms, 2 µs, 3 ns) and 8 bytes BE.
const TAG_TIMESTAMP: u8 = 0x07;
/// Canonical tag of a fixed-width binary value: a `u32` BE length and the bytes.
const TAG_FIXED_BINARY: u8 = 0x08;
/// Canonical tag of a variable-width binary value: a `u32` BE length and the bytes.
const TAG_BINARY: u8 = 0x09;
/// Canonical tag of a decimal: the scale byte and 16 bytes BE.
const TAG_DECIMAL: u8 = 0x0A;
/// Canonical tag of a MAP: a `u32` BE count and the entries in encoded-byte order.
const TAG_MAP: u8 = 0x0B;
/// Canonical tag of a list: a `u32` BE count and the elements.
const TAG_LIST: u8 = 0x0C;
/// Canonical tag of a struct: a `u32` BE count and (name, value) per field.
const TAG_STRUCT: u8 = 0x0D;
/// Canonical tag of a date: 4 bytes BE of days since the epoch.
const TAG_DATE32: u8 = 0x0E;
/// Canonical tag of a `u64`: 8 bytes BE.
const TAG_UINT64: u8 = 0x0F;

/// Why a row has no canonical encoding.
#[derive(Debug, thiserror::Error)]
enum CanonicalEncodingError {
    /// A column has a type the encoding does not cover.
    #[error("no canonical encoding exists for a column of type {0}")]
    UnencodableType(DataType),
    /// A string, binary or collection is longer than a `u32` length prefix holds.
    #[error("a value is too long for the canonical encoding")]
    TooLong,
}

/// What an `ids` value selects in one index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DocumentIdPredicate {
    /// The row at this microsecond whose canonical hash is `row_hash`.
    Timestamped {
        /// The row's timestamp in microseconds since the epoch.
        timestamp_micros: i64,
        /// The row's canonical hash.
        row_hash: u64,
    },
    /// The span with this natural key.
    Span {
        /// The span's trace id.
        trace_id: [u8; 16],
        /// The span's id.
        span_id: [u8; 8],
    },
}

/// Whether rows of `table` are keyed by `trace_id`‖`span_id` instead of a timestamp and hash.
pub(crate) fn is_span_keyed(table: &str) -> bool {
    table == SPANS_TABLE || table == OPERATIONS_TABLE
}

/// Whether a `timestamp` column of `data_type` holds instants, which its rows' `_id` and `_doc`
/// value then carry; a column of another type contributes microsecond `0` to every row.
pub(crate) const fn is_instant_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
    )
}

/// The microsecond a row's `_id` and `_doc` value carry, read from its `timestamp` column
/// `timestamps`: a timestamp of any unit floored to the microsecond, a date at its first
/// microsecond, and `0` for a NULL or for a column that holds no instants (see
/// [`is_instant_type`]), so every row has one.
pub(crate) fn read_document_timestamp_micros(timestamps: &dyn Array, row: usize) -> i64 {
    if timestamps.is_null(row) {
        return 0;
    }
    match timestamps.data_type() {
        DataType::Timestamp(unit, _) => read_timestamp_micros(timestamps, *unit, row),
        DataType::Date32 => {
            i64::from(timestamps.as_primitive::<Date32Type>().value(row)).saturating_mul(MICROS_PER_DAY)
        }
        DataType::Date64 => timestamps.as_primitive::<Date64Type>().value(row).saturating_mul(1_000),
        _non_instant_type => 0,
    }
}

/// The first 8 bytes, big-endian, of the SHA-256 digest `hasher` has accumulated.
fn compute_hash_prefix(hasher: Sha256) -> u64 {
    let digest = hasher.finalize();
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(prefix)
}

/// The canonical hash of `batch[row]` over every column of `batch`, in schema order.
/// `column_names` are the names of `batch`'s columns in schema order; a caller hashing many rows
/// of one batch reads them once and passes one `encoder` for them all, whose buffers then
/// allocate only while a row is larger than every earlier one.
///
/// # Errors
///
/// Returns `500` when a column has a type outside the canonical encoding, because a fallback
/// would let distinct rows share an `_id`, which `search_after` then skips; and `500` when
/// `column_names` does not name every column of `batch`, because hashing fewer columns would
/// let rows that differ in the others share an `_id`.
pub(crate) fn hash_canonical_row(
    encoder: &mut CanonicalEncoder,
    column_names: &[&str],
    batch: &RecordBatch,
    row: usize,
) -> Result<u64, OpenSearchError> {
    if column_names.len() != batch.num_columns() {
        return Err(OpenSearchError::internal(format!(
            "{} column names were given for a batch of {} columns",
            column_names.len(),
            batch.num_columns()
        )));
    }
    encoder
        .hash_row(column_names, batch.columns(), row)
        .map_err(|error| OpenSearchError::internal(error.to_string()))
}

/// The hash `spans` and `operations` rows carry: over the 24 bytes `trace_id`‖`span_id`.
pub(crate) fn hash_span_key(trace_id: &[u8], span_id: &[u8]) -> u64 {
    compute_hash_prefix(Sha256::new().chain_update(trace_id).chain_update(span_id))
}

/// Where one MAP entry lies in the encoding while the entries are ordered.
#[derive(Debug, Clone, Copy)]
struct MapEntryRange {
    /// The offset of the entry's key encoding.
    key_start: usize,
    /// The offset of the entry's value encoding, just past its key.
    value_start: usize,
    /// The offset just past the entry's value encoding.
    value_end: usize,
}

/// The canonical encoding of one row at a time. Its buffers outlive a row, so a caller hashing
/// many rows with one encoder allocates only while a row is larger than every earlier one.
#[derive(Debug)]
pub(crate) struct CanonicalEncoder {
    /// The encoding of the row being hashed.
    encoding: Vec<u8>,
    /// The entries of the MAP values being ordered, innermost last.
    entry_ranges: Vec<MapEntryRange>,
    /// The entries of the MAP being ordered, copied out while `encoding` is rewritten in order.
    entries_copy: Vec<u8>,
}

impl CanonicalEncoder {
    /// An encoder with room for the encoding of a row of a few short columns.
    pub(crate) fn new() -> Self {
        Self {
            encoding: Vec::with_capacity(256),
            entry_ranges: Vec::new(),
            entries_copy: Vec::new(),
        }
    }

    /// The canonical hash of row `row` of `columns`, named `names`.
    fn hash_row(&mut self, names: &[&str], columns: &[ArrayRef], row: usize) -> Result<u64, CanonicalEncodingError> {
        self.encoding.clear();
        // A row that failed half-way may have left the ranges of its unfinished MAPs behind.
        self.entry_ranges.clear();
        self.encode_row(names, columns, row)?;
        Ok(compute_hash_prefix(Sha256::new().chain_update(&self.encoding)))
    }

    /// Append the canonical encoding of row `row` of `columns` (named `names`) to the encoding.
    fn encode_row(&mut self, names: &[&str], columns: &[ArrayRef], row: usize) -> Result<(), CanonicalEncodingError> {
        for (name, column) in names.iter().zip(columns) {
            push_length_prefixed(&mut self.encoding, name.as_bytes())?;
            self.encode_value(column.as_ref(), row)?;
        }
        Ok(())
    }

    /// Append the canonical encoding of `array[row]` (tag, then value) to the encoding.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalEncodingError::UnencodableType`] for a type outside the encoding and
    /// [`CanonicalEncodingError::TooLong`] for a length a `u32` prefix cannot hold.
    fn encode_value(&mut self, array: &dyn Array, row: usize) -> Result<(), CanonicalEncodingError> {
        let out = &mut self.encoding;
        if array.is_null(row) {
            out.push(TAG_NULL);
            return Ok(());
        }
        match array.data_type() {
            DataType::Utf8 => {
                out.push(TAG_STRING);
                push_length_prefixed(out, array.as_string::<i32>().value(row).as_bytes())?;
            }
            DataType::LargeUtf8 => {
                out.push(TAG_STRING);
                push_length_prefixed(out, array.as_string::<i64>().value(row).as_bytes())?;
            }
            DataType::Utf8View => {
                out.push(TAG_STRING);
                push_length_prefixed(out, array.as_string_view().value(row).as_bytes())?;
            }
            DataType::Int8 => push_int32(out, i32::from(array.as_primitive::<Int8Type>().value(row))),
            DataType::Int16 => push_int32(out, i32::from(array.as_primitive::<Int16Type>().value(row))),
            DataType::Int32 => push_int32(out, array.as_primitive::<Int32Type>().value(row)),
            DataType::UInt8 => push_int32(out, i32::from(array.as_primitive::<UInt8Type>().value(row))),
            DataType::UInt16 => push_int32(out, i32::from(array.as_primitive::<UInt16Type>().value(row))),
            DataType::Int64 => push_int64(out, array.as_primitive::<Int64Type>().value(row)),
            DataType::UInt32 => push_int64(out, i64::from(array.as_primitive::<UInt32Type>().value(row))),
            DataType::UInt64 => {
                out.push(TAG_UINT64);
                out.extend_from_slice(&array.as_primitive::<UInt64Type>().value(row).to_be_bytes());
            }
            DataType::Float64 => {
                out.push(TAG_FLOAT64);
                out.extend_from_slice(&array.as_primitive::<Float64Type>().value(row).to_bits().to_be_bytes());
            }
            DataType::Float32 => {
                out.push(TAG_FLOAT32);
                out.extend_from_slice(&array.as_primitive::<Float32Type>().value(row).to_bits().to_be_bytes());
            }
            DataType::Boolean => {
                out.push(TAG_BOOLEAN);
                out.push(u8::from(array.as_boolean().value(row)));
            }
            DataType::Timestamp(unit, _) => {
                let (code, value) = match unit {
                    TimeUnit::Second => (0, array.as_primitive::<TimestampSecondType>().value(row)),
                    TimeUnit::Millisecond => (1, array.as_primitive::<TimestampMillisecondType>().value(row)),
                    TimeUnit::Microsecond => (2, array.as_primitive::<TimestampMicrosecondType>().value(row)),
                    TimeUnit::Nanosecond => (3, array.as_primitive::<TimestampNanosecondType>().value(row)),
                };
                out.push(TAG_TIMESTAMP);
                out.push(code);
                out.extend_from_slice(&value.to_be_bytes());
            }
            DataType::Date32 => {
                out.push(TAG_DATE32);
                out.extend_from_slice(&array.as_primitive::<Date32Type>().value(row).to_be_bytes());
            }
            DataType::FixedSizeBinary(_) => {
                out.push(TAG_FIXED_BINARY);
                push_length_prefixed(out, array.as_fixed_size_binary().value(row))?;
            }
            DataType::Binary => {
                out.push(TAG_BINARY);
                push_length_prefixed(out, array.as_binary::<i32>().value(row))?;
            }
            DataType::LargeBinary => {
                out.push(TAG_BINARY);
                push_length_prefixed(out, array.as_binary::<i64>().value(row))?;
            }
            DataType::Decimal128(_, scale) => {
                out.push(TAG_DECIMAL);
                out.extend_from_slice(&scale.to_be_bytes());
                out.extend_from_slice(&array.as_primitive::<Decimal128Type>().value(row).to_be_bytes());
            }
            DataType::Map(_, _) => {
                let entries = array.as_map().value(row);
                out.push(TAG_MAP);
                push_count(out, entries.len())?;
                let entries_start = out.len();
                let ranges_start = self.entry_ranges.len();
                for entry in 0..entries.len() {
                    let key_start = self.encoding.len();
                    self.encode_value(entries.column(0).as_ref(), entry)?;
                    let value_start = self.encoding.len();
                    self.encode_value(entries.column(1).as_ref(), entry)?;
                    self.entry_ranges.push(MapEntryRange {
                        key_start,
                        value_start,
                        value_end: self.encoding.len(),
                    });
                }
                self.order_map_entries(entries_start, ranges_start);
            }
            DataType::List(_) => {
                let items = array.as_list::<i32>().value(row);
                out.push(TAG_LIST);
                push_count(out, items.len())?;
                for item in 0..items.len() {
                    self.encode_value(items.as_ref(), item)?;
                }
            }
            DataType::LargeList(_) => {
                let items = array.as_list::<i64>().value(row);
                out.push(TAG_LIST);
                push_count(out, items.len())?;
                for item in 0..items.len() {
                    self.encode_value(items.as_ref(), item)?;
                }
            }
            DataType::Struct(fields) => {
                let structure = array.as_struct();
                out.push(TAG_STRUCT);
                push_count(out, fields.len())?;
                for (field, column) in fields.iter().zip(structure.columns()) {
                    push_length_prefixed(&mut self.encoding, field.name().as_bytes())?;
                    self.encode_value(column.as_ref(), row)?;
                }
            }
            unencodable_type => return Err(CanonicalEncodingError::UnencodableType(unencodable_type.clone())),
        }
        Ok(())
    }

    /// Rewrite the entries of one MAP, encoded from `entries_start` to the end of the encoding and
    /// described by the ranges from `ranges_start`, in the byte order of the encoded key, then of
    /// the encoded value. Entry order is storage order, never meaning, so the encoding sorts it
    /// away.
    fn order_map_entries(&mut self, entries_start: usize, ranges_start: usize) {
        let Self {
            encoding,
            entry_ranges,
            entries_copy,
        } = self;
        let ranges = &mut entry_ranges[ranges_start..];
        let read_entry = |range: &MapEntryRange| {
            (
                &encoding[range.key_start..range.value_start],
                &encoding[range.value_start..range.value_end],
            )
        };
        ranges.sort_unstable_by(|left, right| read_entry(left).cmp(&read_entry(right)));
        entries_copy.clear();
        entries_copy.extend_from_slice(&encoding[entries_start..]);
        encoding.truncate(entries_start);
        for range in ranges.iter() {
            encoding.extend_from_slice(&entries_copy[range.key_start - entries_start..range.value_end - entries_start]);
        }
        entry_ranges.truncate(ranges_start);
    }
}

/// Append `bytes` to `out` behind their `u32` BE length.
fn push_length_prefixed(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), CanonicalEncodingError> {
    let length = u32::try_from(bytes.len()).map_err(|_| CanonicalEncodingError::TooLong)?;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

/// Append `count` to `out` as a `u32` BE.
fn push_count(out: &mut Vec<u8>, count: usize) -> Result<(), CanonicalEncodingError> {
    let count = u32::try_from(count).map_err(|_| CanonicalEncodingError::TooLong)?;
    out.extend_from_slice(&count.to_be_bytes());
    Ok(())
}

/// Append a 32-bit integer under [`TAG_INT32`].
fn push_int32(out: &mut Vec<u8>, value: i32) {
    out.push(TAG_INT32);
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append a 64-bit integer under [`TAG_INT64`].
fn push_int64(out: &mut Vec<u8>, value: i64) {
    out.push(TAG_INT64);
    out.extend_from_slice(&value.to_be_bytes());
}

/// `_id` of a row of a table without a natural key: lowercase hex of the big-endian
/// `timestamp_micros` followed by the big-endian `row_hash` (32 characters).
pub(crate) fn encode_document_id(timestamp_micros: i64, row_hash: u64) -> String {
    let mut id = hex::encode(timestamp_micros.to_be_bytes());
    id.push_str(&hex::encode(row_hash.to_be_bytes()));
    id
}

/// `_id` of a `spans` or `operations` row: lowercase hex of `trace_id`‖`span_id`.
pub(crate) fn encode_span_document_id(trace_id: &[u8], span_id: &[u8]) -> String {
    let mut id = hex::encode(trace_id);
    id.push_str(&hex::encode(span_id));
    id
}

/// What `id` selects in `entry`; `None` when it is no id of that index's form (it then
/// matches nothing).
pub(crate) fn decode_document_id(id: &str, entry: &IndexEntry) -> Option<DocumentIdPredicate> {
    if is_span_keyed(entry.table()) {
        if id.len() != SPAN_DOCUMENT_ID_LENGTH {
            return None;
        }
        let bytes = hex::decode(id).ok()?;
        let (trace_id, span_id) = bytes.split_at(16);
        return Some(DocumentIdPredicate::Span {
            trace_id: trace_id.try_into().ok()?,
            span_id: span_id.try_into().ok()?,
        });
    }
    if id.len() != HASHED_DOCUMENT_ID_LENGTH {
        return None;
    }
    let bytes = hex::decode(id).ok()?;
    let (timestamp, row_hash) = bytes.split_at(8);
    Some(DocumentIdPredicate::Timestamped {
        timestamp_micros: i64::from_be_bytes(timestamp.try_into().ok()?),
        row_hash: u64::from_be_bytes(row_hash.try_into().ok()?),
    })
}

/// `_doc` sort value: `(µs mod 1000) · 2^43 + (h mod 2^43)`, in `[0, 1000 · 2^43)`, below
/// `2^53`, so a JSON number carries it exactly.
pub(crate) fn encode_doc_sort_value(timestamp_micros: i64, row_hash: u64) -> i64 {
    // `row_hash & DOC_HASH_MASK` is below 2^43, so it always fits an `i64`.
    let hash_low = i64::try_from(row_hash & DOC_HASH_MASK).unwrap_or(0);
    timestamp_micros.rem_euclid(1_000) * (1 << DOC_HASH_BITS) + hash_low
}

/// `(µs, h mod 2^43)` of a row whose date sort value is `time_ms` and `_doc` value is `doc`.
/// With `time_ms` 0, the microseconds are the microsecond within the millisecond. The caller
/// validates `doc` to `[0, 1000 · 2^43)`.
///
/// The pair keeps only the low 43 bits of the hash: two distinct rows that share a microsecond
/// and `h mod 2^43` are indistinguishable to `search_after` (probability 2^-43 per pair). Apart
/// from that collision, a scan in `[time, _doc]` order never repeats a row and never skips a
/// distinct row.
pub(crate) fn decode_time_and_doc(time_ms: i64, doc: i64) -> (i64, u64) {
    let micros = time_ms.saturating_mul(1_000).saturating_add(doc >> DOC_HASH_BITS);
    (micros, u64::try_from(doc).unwrap_or(0) & DOC_HASH_MASK)
}

/// The canonical row hash as a scalar UDF named `document_row_hash` over the columns
/// `column_names` (passed in that order), so that `ids` on a hashed id filters inside the scan.
/// It answers the same hash as [`hash_canonical_row`] over a batch of exactly those columns.
pub(crate) fn document_row_hash_udf(column_names: Vec<String>) -> ScalarUDF {
    ScalarUDF::from(DocumentRowHash {
        column_names,
        signature: Signature::variadic_any(Volatility::Immutable),
    })
}

/// The `document_row_hash` UDF: the canonical row hash of its arguments, read as the columns
/// `column_names` in order.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DocumentRowHash {
    /// The names the arguments are encoded under, in argument order.
    column_names: Vec<String>,
    /// Any number of arguments of any type; immutable.
    signature: Signature,
}

impl ScalarUDFImpl for DocumentRowHash {
    fn name(&self) -> &'static str {
        "document_row_hash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != self.column_names.len() {
            return exec_err!(
                "document_row_hash takes {} columns, got {}",
                self.column_names.len(),
                args.args.len()
            );
        }
        let columns = args
            .args
            .iter()
            .map(|argument| argument.to_array(args.number_rows))
            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;
        let names: Vec<&str> = self.column_names.iter().map(String::as_str).collect();
        let mut encoder = CanonicalEncoder::new();
        let mut hashes = UInt64Builder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            let row_hash = encoder
                .hash_row(&names, &columns, row)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            hashes.append_value(row_hash);
        }
        Ok(ColumnarValue::Array(Arc::new(hashes.finish())))
    }
}

/// The span-key hash as a scalar UDF named `span_key_hash` over (`trace_id`, `span_id`), so that
/// SQL orders the rows of a span-keyed table by the hash bits their `_doc` values keep. It
/// answers [`hash_span_key`] per row, and fails the query on a NULL or non-binary span key, which
/// gives a row no `_id`.
pub(crate) fn span_key_hash_udf() -> ScalarUDF {
    ScalarUDF::from(SpanKeyHash {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

/// The `span_key_hash` UDF: [`hash_span_key`] of its two arguments.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SpanKeyHash {
    /// Two arguments of any type, checked when invoked; immutable.
    signature: Signature,
}

impl ScalarUDFImpl for SpanKeyHash {
    fn name(&self) -> &'static str {
        "span_key_hash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let [trace_ids, span_ids] = args.args.as_slice() else {
            return exec_err!("span_key_hash takes 2 columns, got {}", args.args.len());
        };
        let trace_ids = trace_ids.to_array(args.number_rows)?;
        let span_ids = span_ids.to_array(args.number_rows)?;
        let (Some(trace_ids), Some(span_ids)) = (
            trace_ids.as_fixed_size_binary_opt(),
            span_ids.as_fixed_size_binary_opt(),
        ) else {
            return exec_err!("span_key_hash takes two fixed-width binary columns");
        };
        let mut hashes = UInt64Builder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            if trace_ids.is_null(row) || span_ids.is_null(row) {
                return exec_err!("a span-keyed row has a NULL span key");
            }
            hashes.append_value(hash_span_key(trace_ids.value(row), span_ids.value(row)));
        }
        Ok(ColumnarValue::Array(Arc::new(hashes.finish())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, AsArray, FixedSizeBinaryArray, Float64Array, MapBuilder, RecordBatch, StringArray, StringBuilder,
        TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit, UInt64Type};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionContext, ident};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::{
        CanonicalEncoder, DOC_HASH_MASK, DocumentIdPredicate, decode_document_id, decode_time_and_doc,
        encode_doc_sort_value, encode_document_id, encode_span_document_id, hash_canonical_row, hash_span_key,
        span_key_hash_udf,
    };
    use crate::opensearch::catalog::IndexSet;
    use crate::opensearch::error::render_envelope;

    /// A `MAP<string, string>` column with one row per entry of `rows`: the entries in this
    /// order, or NULL.
    fn build_map_column(rows: &[Option<&[(&str, &str)]>]) -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for row in rows {
            match row {
                Some(entries) => {
                    for (key, value) in *entries {
                        builder.keys().append_value(key);
                        builder.values().append_value(value);
                    }
                    builder.append(true).expect("map row");
                }
                None => builder.append(false).expect("map row"),
            }
        }
        Arc::new(builder.finish())
    }

    /// The column names of `batch`, in schema order.
    fn read_column_names(batch: &RecordBatch) -> Vec<String> {
        batch.schema().fields().iter().map(|field| field.name().clone()).collect()
    }

    /// The canonical hash of every row of `batch`, in row order.
    fn hash_every_row(batch: &RecordBatch) -> Vec<u64> {
        let column_names = read_column_names(batch);
        let names: Vec<&str> = column_names.iter().map(String::as_str).collect();
        let mut encoder = CanonicalEncoder::new();
        (0..batch.num_rows())
            .map(|row| hash_canonical_row(&mut encoder, &names, batch, row).expect("hash"))
            .collect()
    }

    fn build_stated_batch() -> RecordBatch {
        let attributes = build_map_column(&[Some(&[("b", "2"), ("a", "1")])]);
        let schema = Schema::new(vec![
            Field::new("service_name", DataType::Utf8, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("log_attributes", attributes.data_type().clone(), true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![Some("api")])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_760_000_000_123_456])),
                attributes,
            ],
        )
        .expect("stated batch")
    }

    /// The canonical bytes are written out by hand from the encoding; the digest was computed
    /// outside the code by piping the same bytes through `xxd -r -p | shasum -a 256`, which
    /// printed `65935e5fbcf6d5e566f88996bcb65dd7325912bdef1af184d85046e436fd700f`.
    #[test]
    fn the_canonical_row_hash_of_a_stated_row_equals_an_independently_computed_digest() {
        let expected_bytes = hex::decode(concat!(
            "0000000c",
            "736572766963655f6e616d65",
            "01",
            "00000003",
            "617069",
            "0000000d",
            "73657665726974795f74657874",
            "00",
            "00000009",
            "74696d657374616d70",
            "07",
            "02",
            "000640b5eecfe240",
            "0000000e",
            "6c6f675f61747472696275746573",
            "0b",
            "00000002",
            "01",
            "00000001",
            "61",
            "01",
            "00000001",
            "31",
            "01",
            "00000001",
            "62",
            "01",
            "00000001",
            "32",
        ))
        .expect("literal hex");
        let batch = build_stated_batch();
        let names: Vec<&str> = vec!["service_name", "severity_text", "timestamp", "log_attributes"];
        let mut encoder = CanonicalEncoder::new();
        encoder.encode_row(&names, batch.columns(), 0).expect("encode");
        assert_eq!(encoder.encoding, expected_bytes);

        let row_hash = hash_canonical_row(&mut encoder, &names, &batch, 0).expect("hash");
        assert_eq!(row_hash, 0x6593_5e5f_bcf6_d5e5);
        assert_eq!(
            encode_document_id(1_760_000_000_123_456, row_hash),
            "000640b5eecfe24065935e5fbcf6d5e5"
        );
        // 456 · 2^43 + (h mod 2^43), with h mod 2^43 = 0x65fbcf6d5e5 = 7008261952997.
        assert_eq!(
            encode_doc_sort_value(1_760_000_000_123_456, row_hash),
            4_018_026_680_079_845
        );
    }

    #[test]
    fn map_entry_order_and_null_versus_empty_never_alias_in_the_canonical_row() {
        let maps = build_map_column(&[
            Some(&[("a", "1"), ("b", "2")]),
            Some(&[("b", "2"), ("a", "1")]),
            Some(&[]),
            None,
            Some(&[("a", "")]),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "attributes",
            maps.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![maps]).expect("batch");
        let hashes = hash_every_row(&batch);
        assert_eq!(hashes.len(), 5);
        assert_eq!(hashes[0], hashes[1], "entry order never matters");
        assert_ne!(hashes[2], hashes[3], "an empty map differs from NULL");
        assert_ne!(
            hashes[2], hashes[4],
            "an entry with an empty value differs from no entry"
        );

        let strings = Arc::new(StringArray::from(vec![Some(""), None]));
        let doubles = Arc::new(Float64Array::from(vec![Some(-0.0), Some(0.0)]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("number", DataType::Float64, true),
        ]));
        let empty_and_null = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![strings, Arc::new(Float64Array::from(vec![Some(1.0), Some(1.0)]))],
        )
        .expect("batch");
        let empty_and_null_hashes = hash_every_row(&empty_and_null);
        assert_ne!(
            empty_and_null_hashes[0], empty_and_null_hashes[1],
            "an empty string differs from NULL"
        );
        let zeros = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("x"), Some("x")])), doubles],
        )
        .expect("batch");
        let zero_hashes = hash_every_row(&zeros);
        assert_ne!(zero_hashes[0], zero_hashes[1], "-0.0 differs from +0.0");
    }

    /// A MAP of MAPs, stored in two entry orders, with one encoder reused across both rows: the
    /// inner entries are ordered before the outer ones, and nothing of the first row leaks into
    /// the second. The bytes are written out by hand from the encoding.
    #[test]
    fn a_reused_encoder_orders_nested_map_entries_into_the_stated_bytes() {
        let mut builder = MapBuilder::new(
            None,
            StringBuilder::new(),
            MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),
        );
        let rows: [&[(&str, &[(&str, &str)])]; 2] = [
            &[("y", &[("b", "2"), ("a", "1")]), ("x", &[])],
            &[("x", &[]), ("y", &[("a", "1"), ("b", "2")])],
        ];
        for outer_entries in rows {
            for (key, inner_entries) in outer_entries {
                builder.keys().append_value(key);
                for (inner_key, inner_value) in *inner_entries {
                    builder.values().keys().append_value(inner_key);
                    builder.values().values().append_value(inner_value);
                }
                builder.values().append(true).expect("inner map");
            }
            builder.append(true).expect("outer map");
        }
        let maps: ArrayRef = Arc::new(builder.finish());
        let expected_bytes = hex::decode(concat!(
            "00000001", "6d", "0b", "00000002", //
            "01", "00000001", "78", "0b", "00000000", //
            "01", "00000001", "79", "0b", "00000002", //
            "01", "00000001", "61", "01", "00000001", "31", //
            "01", "00000001", "62", "01", "00000001", "32",
        ))
        .expect("literal hex");

        let mut encoder = CanonicalEncoder::new();
        for row in 0..2 {
            encoder.hash_row(&["m"], &[Arc::clone(&maps)], row).expect("hash");
            assert_eq!(encoder.encoding, expected_bytes, "row {row}");
        }
    }

    /// A name list that misses a column would hash the row without it, so rows differing only
    /// there would share an `_id`.
    #[tokio::test]
    async fn hashing_with_fewer_names_than_columns_is_a_500() {
        let error = hash_canonical_row(
            &mut CanonicalEncoder::new(),
            &["service_name"],
            &build_stated_batch(),
            0,
        )
        .expect_err("refused");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status.as_u16(), 500);
        assert_eq!(body["error"]["type"], "exception");
    }

    /// The digest was computed outside the code by piping the 24 bytes through
    /// `xxd -r -p | shasum -a 256`, which printed
    /// `b9857c92f8012eff4feffe7e3bfe3f6d15fd7c4e5e0116689a1df3196dd98b00`.
    #[test]
    fn the_span_key_hash_is_the_digest_prefix_of_trace_id_then_span_id() {
        let trace_id = hex::decode("0123456789abcdeffedcba9876543210").expect("trace id");
        let span_id = hex::decode("aabbccdd00112233").expect("span id");

        assert_eq!(hash_span_key(&trace_id, &span_id), 0xb985_7c92_f801_2eff);
    }

    /// `span_key_hash(trace_id, span_id)` evaluated by a query over `trace_ids` and `span_ids`.
    async fn run_span_key_hash(
        trace_ids: Vec<Option<Vec<u8>>>,
        span_ids: Vec<Option<Vec<u8>>>,
    ) -> datafusion::error::Result<Vec<u64>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::FixedSizeBinary(16), true),
            Field::new("span_id", DataType::FixedSizeBinary(8), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    trace_ids.into_iter(),
                    16,
                )?),
                Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    span_ids.into_iter(),
                    8,
                )?),
            ],
        )?;
        let context = SessionContext::new();
        let hashes = context
            .read_table(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?
            .select(vec![
                span_key_hash_udf().call(vec![ident("trace_id"), ident("span_id")]),
            ])?
            .collect()
            .await?;
        Ok(hashes
            .iter()
            .flat_map(|hash_batch| hash_batch.column(0).as_primitive::<UInt64Type>().values().to_vec())
            .collect())
    }

    /// The UDF SQL orders span-keyed rows by answers the span-key hash of each row, here the
    /// independently computed digest prefix of the test above, and fails on a NULL key, which
    /// gives a row no `_id`.
    #[tokio::test]
    async fn the_span_key_hash_udf_answers_the_span_key_hash_and_refuses_a_null_key() {
        let trace_id = hex::decode("0123456789abcdeffedcba9876543210").expect("trace id");
        let span_id = hex::decode("aabbccdd00112233").expect("span id");

        let hashes = run_span_key_hash(vec![Some(trace_id.clone())], vec![Some(span_id)])
            .await
            .expect("hash");
        assert_eq!(hashes, vec![0xb985_7c92_f801_2eff]);
        run_span_key_hash(vec![Some(trace_id)], vec![None])
            .await
            .expect_err("a NULL span id");
    }

    #[test]
    fn document_ids_and_doc_sort_values_round_trip() {
        let mut generator = StdRng::seed_from_u64(0x00D0_C1D5);
        for _ in 0..10_000 {
            let micros = generator.random_range(-(1_i64 << 52)..(1_i64 << 52));
            let row_hash = generator.random::<u64>();
            let doc = encode_doc_sort_value(micros, row_hash);
            assert_eq!(
                decode_time_and_doc(micros.div_euclid(1_000), doc),
                (micros, row_hash & DOC_HASH_MASK),
                "{micros} {row_hash}"
            );
        }
        let indices = IndexSet::from_table_names(vec!["logs".to_string(), "spans".to_string()]);
        let logs = indices.entries().iter().find(|entry| entry.name() == "logs").expect("logs");
        let spans = indices.entries().iter().find(|entry| entry.name() == "spans").expect("spans");
        assert_eq!(
            decode_document_id(&encode_document_id(-5, 0xFFFF_0000_1234_5678), logs),
            Some(DocumentIdPredicate::Timestamped {
                timestamp_micros: -5,
                row_hash: 0xFFFF_0000_1234_5678
            })
        );
        let trace_id = [
            0x01_u8, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
        ];
        let span_id = [0xaa_u8, 0xbb, 0xcc, 0xdd, 0x00, 0x11, 0x22, 0x33];
        let span_document_id = encode_span_document_id(&trace_id, &span_id);
        assert_eq!(span_document_id, "0123456789abcdeffedcba9876543210aabbccdd00112233");
        assert_eq!(
            decode_document_id(&span_document_id, spans),
            Some(DocumentIdPredicate::Span { trace_id, span_id })
        );
    }

    /// `999 · 2^43 + (2^43 - 1) = 1000 · 2^43 - 1`, and `1000 · 2^43` falls short of
    /// `2^53 = 1024 · 2^43` by `24 · 2^43`, so the largest `_doc` value is `2^53 - 1 - 24 · 2^43`.
    #[test]
    fn doc_sort_values_stay_below_two_to_the_fifty_three() {
        let ceiling = 1_i64 << 53;
        for micros in [0, 999, 1_000, -1, -1_000, i64::MAX, i64::MIN, 1_760_000_000_123_999] {
            for row_hash in [0, u64::MAX, DOC_HASH_MASK, DOC_HASH_MASK + 1] {
                let doc = encode_doc_sort_value(micros, row_hash);
                assert!((0..ceiling).contains(&doc), "{micros} {row_hash} gave {doc}");
            }
        }
        assert_eq!(encode_doc_sort_value(999, u64::MAX), ceiling - 1 - (1 << 43) * 24);
    }

    #[test]
    fn a_malformed_id_decodes_to_nothing() {
        let indices = IndexSet::from_table_names(vec!["logs".to_string(), "spans".to_string()]);
        let logs = indices.entries().iter().find(|entry| entry.name() == "logs").expect("logs");
        let spans = indices.entries().iter().find(|entry| entry.name() == "spans").expect("spans");
        for id in [
            "",
            "nope",
            "000640b5eecfe24065935e5fbcf6d5e",
            "000640b5eecfe24065935e5fbcf6d5e5a",
            "zz0640b5eecfe24065935e5fbcf6d5e5",
            "0123456789abcdeffedcba9876543210aabbccdd00112233",
        ] {
            assert_eq!(decode_document_id(id, logs), None, "logs {id:?}");
        }
        for id in [
            "",
            "000640b5eecfe24065935e5fbcf6d5e5",
            "0123456789abcdeffedcba9876543210aabbccdd0011223g",
        ] {
            assert_eq!(decode_document_id(id, spans), None, "spans {id:?}");
        }
    }
}
