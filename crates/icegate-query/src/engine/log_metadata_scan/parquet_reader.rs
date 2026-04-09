//! Parquet file openers and dictionary-page reader backed by iceberg's
//! `FileIO`.
//!
//! Two entry points:
//!
//! - [`open_file_direct`] returns an [`iceberg::arrow::ArrowFileReader`]
//!   together with its already-decoded `ParquetMetaData`. Used by paths
//!   that only need metadata + range reads for dictionary pages.
//! - [`open_builder`] returns a `ParquetRecordBatchStreamBuilder`. Used by
//!   the `/label_values` MAP path, which needs correlated key/value
//!   record batches.
//!
//! [`read_column_dictionaries`] is the shared dictionary-page reader: it
//! evaluates the predicate against row-group statistics to prune, then
//! issues a single batched `get_byte_ranges` call for the surviving
//! row-group column chunks (iceberg's `ArrowFileReader` coalesces and
//! fetches in parallel). Each returned chunk is then decoded to pull only
//! its dictionary page — data pages are never touched.
//!
//! Error localization is via the surrounding tracing span, not the error
//! value. The per-file helpers in `mod.rs` install a `file` span field;
//! tracing consumers resolve the error source from the parent span
//! instead of having to parse a path out of the error message.

use std::collections::BTreeSet;
use std::io::Read;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use iceberg::arrow::ArrowFileReader;
use iceberg::expr::{Predicate, PredicateOperator};
use iceberg::io::{FileIO, FileMetadata};
use iceberg::scan::FileScanTask;
use iceberg::spec::{Datum, PrimitiveLiteral};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::column::page::{Page, PageReader};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::file::statistics::Statistics;

use super::error::MetadataScanError;

/// Open a `FileScanTask` and return an `ArrowFileReader` together with its
/// decoded Parquet metadata.
///
/// # Errors
///
/// Returns `MetadataScanError::Iceberg` if iceberg cannot open the file, and
/// `MetadataScanError::Parquet` if the Parquet footer cannot be decoded.
#[tracing::instrument(skip_all, fields(file = %task.data_file_path, file_size = task.file_size_in_bytes))]
pub async fn open_file_direct(
    file_io: &FileIO,
    task: &FileScanTask,
) -> Result<(ArrowFileReader, Arc<ParquetMetaData>), MetadataScanError> {
    let input = file_io.new_input(&task.data_file_path).map_err(MetadataScanError::Iceberg)?;
    let reader = input.reader().await.map_err(MetadataScanError::Iceberg)?;
    let meta = FileMetadata {
        size: task.file_size_in_bytes,
    };
    let mut arrow_reader = ArrowFileReader::new(meta, reader);

    let metadata = arrow_reader.get_metadata(None).await?;

    Ok((arrow_reader, metadata))
}

/// Open a `FileScanTask` for a record-batch read via
/// `ParquetRecordBatchStreamBuilder`. Used by the `/label_values` MAP path.
///
/// # Errors
///
/// Returns `MetadataScanError::Iceberg` if iceberg cannot open the file, and
/// `MetadataScanError::Parquet` if the Parquet footer cannot be decoded.
#[tracing::instrument(skip_all, fields(file = %task.data_file_path, file_size = task.file_size_in_bytes))]
pub async fn open_builder(
    file_io: &FileIO,
    task: &FileScanTask,
) -> Result<ParquetRecordBatchStreamBuilder<ArrowFileReader>, MetadataScanError> {
    let input = file_io.new_input(&task.data_file_path).map_err(MetadataScanError::Iceberg)?;
    let reader = input.reader().await.map_err(MetadataScanError::Iceberg)?;

    let meta = FileMetadata {
        size: task.file_size_in_bytes,
    };
    let arrow_reader = ArrowFileReader::new(meta, reader);

    Ok(ParquetRecordBatchStreamBuilder::new(arrow_reader).await?)
}

/// Dictionary-page-only reader: for every row group that survives the
/// predicate, fetch the column chunk at `leaf_idx`, pull just its
/// dictionary page and insert each distinct string into `out`.
///
/// Row group I/O is parallelized: all surviving row-group byte ranges are
/// submitted in a single `get_byte_ranges` call, which iceberg's
/// `ArrowFileReader` coalesces into concurrent object-store fetches.
/// Decoding is then performed sequentially on the in-memory buffers
/// (decoding is CPU-cheap compared to the network fetch).
///
/// Data pages are never decoded. Row groups without a dictionary page are
/// skipped (acceptable under over-approximation semantics; the current
/// ingest writer emits dictionaries for every low-cardinality string
/// column). Row groups pruned by `predicate` are skipped before any I/O.
///
/// # Errors
///
/// Returns `MetadataScanError::Parquet` if any column chunk fails to
/// decode. The file path is available via the surrounding tracing span.
#[tracing::instrument(
    skip_all,
    fields(
        leaf_idx = leaf_idx,
        num_row_groups_total = metadata.num_row_groups(),
        num_row_groups_pruned = tracing::field::Empty,
        num_row_groups_no_dict = tracing::field::Empty,
        num_chunks_fetched = tracing::field::Empty,
        bytes_fetched = tracing::field::Empty,
        num_keys_decoded = tracing::field::Empty,
    ),
)]
pub async fn read_column_dictionaries(
    reader: &mut ArrowFileReader,
    metadata: &ParquetMetaData,
    predicate: &Predicate,
    leaf_idx: usize,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    // Stage 1: row-group pruning + range planning. All CPU, no I/O.
    let mut ranges: Vec<Range<u64>> = Vec::new();
    let mut selected_rgs: Vec<usize> = Vec::new();
    let mut pruned: usize = 0;
    let mut no_dict: usize = 0;

    for rg_idx in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(rg_idx);

        if !row_group_can_match(rg, predicate) {
            pruned += 1;
            continue;
        }

        let col_chunk = rg.column(leaf_idx);
        let Some(dict_offset) = col_chunk.dictionary_page_offset() else {
            no_dict += 1;
            continue;
        };

        // Fetch only the dictionary page region, not the full column chunk.
        // The dictionary page is always written immediately before the first
        // data page, so `[dictionary_page_offset, data_page_offset)` bounds it
        // exactly. This avoids (a) pulling megabytes of data pages we never
        // decode and (b) overrunning the object size when the column chunk
        // ends close to EOF, which surfaces as OpenDAL's
        // "fetched data does not cover requested range".
        let data_offset = col_chunk.data_page_offset();
        let Ok(dict_start) = u64::try_from(dict_offset) else {
            no_dict += 1;
            continue;
        };
        let Ok(dict_end) = u64::try_from(data_offset) else {
            no_dict += 1;
            continue;
        };
        if dict_end <= dict_start {
            no_dict += 1;
            continue;
        }
        ranges.push(dict_start..dict_end);
        selected_rgs.push(rg_idx);
    }

    let span = tracing::Span::current();
    span.record("num_row_groups_pruned", pruned);
    span.record("num_row_groups_no_dict", no_dict);
    span.record("num_chunks_fetched", ranges.len());

    if no_dict > 0 {
        // Our ingest writer always emits dictionaries for string columns, so
        // this path should only trigger for externally-produced files. Labels
        // from these row groups will be missing (acceptable under
        // over-approximation semantics).
        tracing::warn!(
            no_dict,
            "skipped row groups without dictionary page; label results may be incomplete"
        );
    }

    if ranges.is_empty() {
        return Ok(());
    }

    // Stage 2: batched fetch. `ArrowFileReader::get_byte_ranges` coalesces
    // adjacent ranges and fetches concurrently (see iceberg-rust's
    // `arrow/reader.rs`).
    let bytes_total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
    span.record("bytes_fetched", bytes_total);

    let chunks: Vec<Bytes> = reader.get_byte_ranges(ranges.clone()).await?;

    // Stage 3: decode each chunk's dictionary page. Sequential here (CPU
    // cost is small and a shared `out` set would need synchronization).
    let keys_before = out.len();
    for (rg_idx, (chunk_bytes, range)) in selected_rgs.iter().copied().zip(chunks.into_iter().zip(ranges.into_iter())) {
        decode_dictionary_from_chunk(metadata, rg_idx, leaf_idx, chunk_bytes, range.start, out)?;
    }
    span.record("num_keys_decoded", out.len() - keys_before);

    Ok(())
}

/// Decode the dictionary page from an already-fetched column chunk
/// buffer.
fn decode_dictionary_from_chunk(
    metadata: &ParquetMetaData,
    rg_idx: usize,
    leaf_idx: usize,
    chunk_bytes: Bytes,
    chunk_start: u64,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    let rg = metadata.row_group(rg_idx);
    let col_chunk = rg.column(leaf_idx);

    // `SerializedPageReader` uses absolute file offsets internally.
    // `OffsetChunk` maps them onto offsets within our in-memory slice.
    let chunk_reader = Arc::new(OffsetChunk {
        bytes: chunk_bytes,
        base: chunk_start,
    });

    let rg_num_rows = usize::try_from(rg.num_rows()).unwrap_or(0);
    let mut page_reader = SerializedPageReader::new(chunk_reader, col_chunk, rg_num_rows, None)?;

    // Parquet always writes the dictionary page first in a dict-encoded
    // column chunk, so one `get_next_page()` gives us exactly the dict
    // page. Drop without touching data pages.
    if let Some(Page::DictionaryPage { buf, num_values, .. }) = page_reader.get_next_page()? {
        decode_plain_byte_array_values(&buf, num_values as usize, out)?;
    }

    Ok(())
}

/// Evaluate `predicate` against `rg`'s row-group statistics.
///
/// Returns `false` **only** when the statistics definitively prove that
/// no row in the group can satisfy the predicate. Any uncertainty —
/// missing stats, unrecognized predicate shape, unsupported literal type,
/// or a predicate referencing a column that is not in the row group —
/// returns `true` (over-approximation: keep the row group).
///
/// Handles the narrow set of predicate shapes produced by
/// [`super::predicate::full_predicate`]: `tenant_id` equality, timestamp
/// `>=` / `<=`, indexed-column `=` / `!=`, and AND composition of those.
pub(super) fn row_group_can_match(rg: &RowGroupMetaData, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::AlwaysFalse => false,
        Predicate::And(expr) => {
            let [left, right] = expr.inputs();
            row_group_can_match(rg, left) && row_group_can_match(rg, right)
        }
        Predicate::Or(expr) => {
            let [left, right] = expr.inputs();
            row_group_can_match(rg, left) || row_group_can_match(rg, right)
        }
        Predicate::Binary(bin) => eval_binary_on_stats(rg, bin.term().name(), bin.op(), bin.literal()),
        // AlwaysTrue, NOT, IS NULL, IS NOT NULL, IN, NOT IN — conservative
        // keep.
        Predicate::AlwaysTrue | Predicate::Not(_) | Predicate::Unary(_) | Predicate::Set(_) => true,
    }
}

fn eval_binary_on_stats(rg: &RowGroupMetaData, col_name: &str, op: PredicateOperator, datum: &Datum) -> bool {
    let schema = rg.schema_descr();
    let Some(leaf_idx) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == col_name) else {
        return true;
    };
    let col = rg.column(leaf_idx);
    let Some(stats) = col.statistics() else {
        return true;
    };

    match (stats, datum.literal()) {
        (Statistics::ByteArray(s), PrimitiveLiteral::String(v)) => {
            let min = s.min_opt().map(parquet::data_type::ByteArray::data);
            let max = s.max_opt().map(parquet::data_type::ByteArray::data);
            eval_range_ord(op, min, max, v.as_bytes())
        }
        (Statistics::Int64(s), PrimitiveLiteral::Long(v)) => eval_range_ord(op, s.min_opt(), s.max_opt(), v),
        (Statistics::Int32(s), PrimitiveLiteral::Int(v)) => eval_range_ord(op, s.min_opt(), s.max_opt(), v),
        _ => true,
    }
}

/// Evaluate a comparison `op` between `[min, max]` and `value`. Returns
/// `true` if any value in the range could satisfy the predicate, `false`
/// only when it definitively cannot. Missing `min` or `max` returns
/// `true` (conservative).
fn eval_range_ord<T: ?Sized + Ord>(op: PredicateOperator, min: Option<&T>, max: Option<&T>, value: &T) -> bool {
    use std::cmp::Ordering;
    match op {
        PredicateOperator::Eq => {
            min.map_or(true, |mn| mn.cmp(value) != Ordering::Greater)
                && max.map_or(true, |mx| mx.cmp(value) != Ordering::Less)
        }
        PredicateOperator::NotEq => match (min, max) {
            (Some(mn), Some(mx)) => !(mn.cmp(value) == Ordering::Equal && mx.cmp(value) == Ordering::Equal),
            _ => true,
        },
        PredicateOperator::LessThan => min.map_or(true, |mn| mn.cmp(value) == Ordering::Less),
        PredicateOperator::LessThanOrEq => min.map_or(true, |mn| mn.cmp(value) != Ordering::Greater),
        PredicateOperator::GreaterThan => max.map_or(true, |mx| mx.cmp(value) == Ordering::Greater),
        PredicateOperator::GreaterThanOrEq => max.map_or(true, |mx| mx.cmp(value) != Ordering::Less),
        _ => true,
    }
}

/// Decode PLAIN-encoded `BYTE_ARRAY` values from a dictionary-page buffer
/// and insert each distinct UTF-8 string into `out`.
///
/// Parquet's PLAIN encoding for variable-length byte arrays is
/// `[len: u32 LE][bytes]` repeated. Non-UTF-8 entries are silently
/// skipped (attribute keys and string column values are always UTF-8 in
/// our schema).
fn decode_plain_byte_array_values(
    buf: &[u8],
    num_values: usize,
    out: &mut BTreeSet<String>,
) -> Result<(), ParquetError> {
    let mut i: usize = 0;
    let mut decoded: usize = 0;
    while decoded < num_values && i + 4 <= buf.len() {
        let len = u32::from_le_bytes([buf[i], buf[i + 1], buf[i + 2], buf[i + 3]]) as usize;
        i += 4;
        if i + len > buf.len() {
            return Err(ParquetError::General(format!(
                "truncated PLAIN BYTE_ARRAY dictionary page: value {decoded} declares \
                 length {len} at offset {} but only {} bytes remain",
                i - 4,
                buf.len() - i,
            )));
        }
        if let Ok(s) = std::str::from_utf8(&buf[i..i + len]) {
            if !out.contains(s) {
                out.insert(s.to_string());
            }
        }
        i += len;
        decoded += 1;
    }

    if decoded < num_values {
        return Err(ParquetError::General(format!(
            "truncated PLAIN BYTE_ARRAY dictionary page: expected {num_values} values \
             but buffer exhausted after {decoded} (buffer length: {})",
            buf.len(),
        )));
    }

    Ok(())
}

/// A [`ChunkReader`] that serves a sub-slice of a file from an in-memory
/// buffer, translating absolute file offsets to offsets within the
/// buffer. `base` is the absolute file offset at which `bytes[0]` lives.
struct OffsetChunk {
    bytes: Bytes,
    base: u64,
}

impl Length for OffsetChunk {
    fn len(&self) -> u64 {
        self.base + self.bytes.len() as u64
    }
}

impl ChunkReader for OffsetChunk {
    type T = BytesCursor;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let rel_usize = self.translate(start)?;
        if rel_usize > self.bytes.len() {
            return Err(ParquetError::General(format!("OffsetChunk: offset {start} past end")));
        }
        Ok(BytesCursor {
            bytes: self.bytes.slice(rel_usize..),
            pos: 0,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let rel_usize = self.translate(start)?;
        if rel_usize + length > self.bytes.len() {
            return Err(ParquetError::General(format!(
                "OffsetChunk: read of {length} at {start} past end"
            )));
        }
        Ok(self.bytes.slice(rel_usize..rel_usize + length))
    }
}

impl OffsetChunk {
    fn translate(&self, start: u64) -> parquet::errors::Result<usize> {
        let rel = start.checked_sub(self.base).ok_or_else(|| {
            ParquetError::General(format!(
                "OffsetChunk: requested offset {start} below base {}",
                self.base
            ))
        })?;
        usize::try_from(rel).map_err(|e| ParquetError::General(e.to_string()))
    }
}

/// Simple forward-only `Read` cursor over a `Bytes` slice.
struct BytesCursor {
    bytes: Bytes,
    pos: usize,
}

impl Read for BytesCursor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.bytes.len().saturating_sub(self.pos);
        let n = remaining.min(buf.len());
        buf[..n].copy_from_slice(&self.bytes[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}
