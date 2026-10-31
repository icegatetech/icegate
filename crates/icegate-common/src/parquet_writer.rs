//! Parquet [`WriterProperties`] builder shared across IceGate writers.
//!
//! The builder owns the cross-cutting writer policy (PARQUET_2_0, ZSTD,
//! page-level statistics, page-size limit, row-group cap, bloom filters
//! after the data of every row group) and applies the
//! per-column encoding overrides + bloom-filter list supplied by the
//! caller. Encoding overrides themselves live in
//! [`crate::parquet_encoding`] — this module is purely about *how* the
//! Parquet writer is configured, not *which* columns get which encoding.
//!
//! Splitting these two concerns is intentional: the encoding policy is
//! data-shape-driven (per-table, per-column), whereas the writer
//! properties (compression, page size, row-group size, dictionary
//! disable) are global.
//!
//! See [`crate::parquet_encoding`] for the rationale behind each
//! encoding choice and the hard exclusion for tag-value-enumerated
//! columns.
//!
//! # Why dictionary is force-disabled
//!
//! parquet-rs treats `set_column_encoding` as a **fallback** that only
//! fires after the per-column dictionary page exceeds
//! `dictionary_page_size_limit` (1 MiB default). At IceGate's row-group
//! sizes (~20,000 rows) most numeric and timestamp dictionaries never
//! reach that limit, so the override never runs and the column stays at
//! `RLE_DICTIONARY`. Forcing dictionary off makes the configured
//! encoding the primary encoding.

use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{BloomFilterPosition, EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;

/// A `(column_name, encoding)` override. Dictionary is **always**
/// disabled for the named column when this override is applied; see the
/// module-level docs for why.
pub type ColumnEncoding = (&'static str, Encoding);

/// Build the Parquet [`WriterProperties`] used for IceGate Iceberg shift
/// writes (and any test that wants to reproduce the production encoding
/// policy).
///
/// `column_encodings` pins the per-column encoding override and
/// **force-disables dictionary** for each named column — see the
/// module-level docs. `bloom_filter_columns` enables a per-column
/// bloom filter sized for `row_group_size` distinct values (every row
/// contributes at most one new value, so this is the natural NDV cap).
/// Bloom filters are written at the end of the file, after the data of
/// every row group.
///
/// Both lists are intentionally borrowed: the writer properties built
/// here own the column paths internally, so callers can pass static
/// slices without further allocation.
#[must_use]
pub fn build_writer_properties(
    row_group_size: usize,
    data_page_size_limit_bytes: usize,
    bloom_filter_columns: &[&str],
    column_encodings: &[ColumnEncoding],
) -> WriterProperties {
    // PARQUET_2_0 is required for the DELTA / BYTE_STREAM_SPLIT encodings
    // configured below; every reader IceGate supports (DataFusion, Trino,
    // modern Spark/Athena) accepts files written at this version.
    let mut builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_size_limit(data_page_size_limit_bytes)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(row_group_size))
        // `End` keeps the bloom filters of all row groups and columns in one
        // contiguous byte range after the column data, so one range request
        // covers them; with `AfterRowGroup` the data of every row group separates
        // them. Pinned by `build_writer_properties_writes_bloom_filters_after_column_data`.
        .set_bloom_filter_position(BloomFilterPosition::End);

    // Dictionary is force-disabled on every column with an explicit
    // encoding override: parquet-rs treats `set_column_encoding` as a
    // fallback that only fires after the dictionary page exceeds 1 MiB,
    // which never happens for our row-group sizes — so without disabling
    // dictionary the configured encoding (DELTA_*, BYTE_STREAM_SPLIT)
    // would never run.
    for &(col, encoding) in column_encodings {
        let path = ColumnPath::from(col);
        builder = builder
            .set_column_dictionary_enabled(path.clone(), false)
            .set_column_encoding(path, encoding);
    }

    let ndv = u64::try_from(row_group_size).unwrap_or(u64::MAX);
    for col in bloom_filter_columns {
        let path = ColumnPath::from(*col);
        builder = builder
            .set_column_bloom_filter_enabled(path.clone(), true)
            .set_column_bloom_filter_ndv(path, ndv);
    }

    builder.build()
}

#[cfg(test)]
pub(crate) mod test_support {
    use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};

    /// Asserts that every bloom filter of `metadata` starts at or after the end of the
    /// last column chunk of the file, i.e. that the file was written with
    /// `BloomFilterPosition::End` and has more than one row group with bloom filters.
    pub(crate) fn assert_bloom_filters_after_column_data(metadata: &ParquetMetaData) {
        // With a single row group the `End` and `AfterRowGroup` layouts are
        // byte-identical, so the check below would pin nothing.
        assert!(
            metadata.num_row_groups() >= 2,
            "the layout check needs at least two row groups, got {}",
            metadata.num_row_groups()
        );

        // Row groups are not assumed to be in file order, so take the maximum
        // over every column chunk.
        let column_data_end = metadata
            .row_groups()
            .iter()
            .flat_map(RowGroupMetaData::columns)
            .map(|column| {
                let (start, length) = column.byte_range();
                start + length
            })
            .max()
            .expect("a file with row groups has column chunks");

        let mut bloom_filter_count = 0_usize;
        for (row_group_idx, row_group) in metadata.row_groups().iter().enumerate() {
            for column in row_group.columns() {
                let Some(bloom_filter_offset) = column.bloom_filter_offset() else {
                    continue;
                };
                bloom_filter_count += 1;
                let bloom_filter_offset =
                    u64::try_from(bloom_filter_offset).expect("bloom filter offset is not negative");
                assert!(
                    bloom_filter_offset >= column_data_end,
                    "row group {row_group_idx}, column {}: bloom filter at {bloom_filter_offset} starts before the \
                     end of the column data at {column_data_end}",
                    column.column_path().string()
                );
            }
        }
        assert!(bloom_filter_count > 0, "the file has no bloom filters");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Encoding;
    use parquet::file::metadata::RowGroupMetaData;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::{ColumnEncoding, build_writer_properties, test_support};

    /// End-to-end check that a `FixedSizeBinary(16)` column written with
    /// the `DELTA_BYTE_ARRAY` override (a) actually emits that encoding
    /// and (b) round-trips byte-for-byte through parquet-rs.
    ///
    /// Guards two production-critical invariants:
    /// 1. `set_column_dictionary_enabled(false)` keeps firing for fixed
    ///    binary columns — without it, parquet-rs treats the override as a
    ///    fallback that triggers only after the dictionary page exceeds
    ///    1 `MiB` (which never happens at `IceGate`'s row-group sizes), so
    ///    the override would silently be a no-op.
    /// 2. parquet-rs accepts `DELTA_BYTE_ARRAY` for `FIXED_LEN_BYTE_ARRAY`
    ///    physical types — earlier parquet-rs versions only allowed it on
    ///    `BYTE_ARRAY`. A future upgrade dropping that support would now
    ///    fail loudly here.
    #[test]
    fn delta_byte_array_roundtrips_fixed_size_binary_16() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let mut builder = FixedSizeBinaryBuilder::with_capacity(3, 16);
        for i in 0u8..3 {
            let mut bytes = [0u8; 16];
            bytes[0] = i;
            builder.append_value(bytes).expect("append 16-byte value");
        }
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()]).expect("record batch");

        let overrides: &[ColumnEncoding] = &[("trace_id", Encoding::DELTA_BYTE_ARRAY)];
        let props = build_writer_properties(
            /* row_group_size */ 16,
            /* data_page_size_limit_bytes */ 1 << 20,
            /* bloom_filter_columns */ &[],
            overrides,
        );
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).expect("ArrowWriter");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }

        // Byte-for-byte readback equality.
        let buf = Bytes::from(buf);
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(buf.clone()).expect("reader builder");
        let mut reader = reader_builder.build().expect("reader");
        let read_batch = reader.next().expect("one batch").expect("batch ok");
        assert_eq!(read_batch.column(0), &array, "FixedSizeBinary roundtrip mismatch");

        // Encoding metadata: DELTA_BYTE_ARRAY must be present, RLE_DICTIONARY
        // must NOT be present (since we force-disabled dictionary on the
        // override column). `encodings()` returns an iterator; collect once
        // so we can scan it twice for the contains assertions.
        let file_reader = SerializedFileReader::new(buf).expect("file reader");
        let row_group = file_reader.get_row_group(0).expect("row group 0");
        let encodings: Vec<Encoding> = row_group.metadata().column(0).encodings().collect();
        assert!(
            encodings.contains(&Encoding::DELTA_BYTE_ARRAY),
            "expected DELTA_BYTE_ARRAY in encodings, got {encodings:?}"
        );
        assert!(
            !encodings.contains(&Encoding::RLE_DICTIONARY),
            "dictionary encoding leaked through despite set_column_dictionary_enabled(false): {encodings:?}"
        );
    }

    /// A `FixedSizeBinary(16)` `trace_id` written with the full production policy
    /// (`DELTA_BYTE_ARRAY` encoding + bloom filter) must emit EXACT min/max
    /// statistics.
    ///
    /// This is the property the compaction pipeline depends on: iceberg-rust's
    /// `MinMaxColAggregator` records a column's manifest lower/upper bound ONLY
    /// when the parquet statistic reports `*_is_exact()`. A missing `trace_id`
    /// bound makes a data file's sort-key envelope start with a null component,
    /// which both isolates the file during overlap clustering (single-file
    /// rewrites) and breaks query pruning on `trace_id`. `trace_id` is 16 bytes —
    /// far below parquet's 64-byte default statistics-truncation length — so its
    /// statistic is stored verbatim and stays exact; this test pins that so a
    /// future encoding/statistics change can't silently drop the bound. (Older
    /// data files that predate this policy may still lack the bound; the
    /// compaction invariant tolerates that, see `icegate-maintain`'s
    /// `first_envelope_conflict`.)
    #[test]
    fn build_writer_properties_emits_exact_statistics_for_fixed16_trace_id() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let mut builder = FixedSizeBinaryBuilder::with_capacity(4, 16);
        for i in 0u8..4 {
            let mut bytes = [0u8; 16];
            // Vary the leading bytes so min and max are distinct, non-trivial
            // 16-byte values (mirroring real trace ids).
            bytes[0] = i.wrapping_mul(40);
            bytes[15] = i;
            builder.append_value(bytes).expect("append 16-byte value");
        }
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).expect("record batch");

        // Exactly the production spans policy for trace_id: bloom filter + the
        // DELTA_BYTE_ARRAY encoding override.
        let props = build_writer_properties(
            /* row_group_size */ 16,
            /* data_page_size_limit_bytes */ 1 << 20,
            /* bloom_filter_columns */ &["trace_id"],
            /* column_encodings */ &[("trace_id", Encoding::DELTA_BYTE_ARRAY)],
        );
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).expect("ArrowWriter");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }

        let file_reader = SerializedFileReader::new(Bytes::from(buf)).expect("file reader");
        let row_group = file_reader.get_row_group(0).expect("row group 0");
        let statistics = row_group
            .metadata()
            .column(0)
            .statistics()
            .expect("trace_id column must carry statistics");

        // EXACT min and max are what make iceberg-rust record the manifest bound.
        assert!(
            statistics.min_is_exact(),
            "trace_id min statistic must be exact so the manifest lower bound is recorded"
        );
        assert!(
            statistics.max_is_exact(),
            "trace_id max statistic must be exact so the manifest upper bound is recorded"
        );
    }

    /// Pins the bloom filter layout of a file with several row groups and several
    /// bloom filter columns: the bloom filters start after all column data and
    /// form one contiguous byte range.
    #[test]
    fn build_writer_properties_writes_bloom_filters_after_column_data() {
        const ROW_GROUP_SIZE: usize = 16;
        const ROW_GROUPS: usize = 3;
        const BLOOM_FILTER_COLUMNS: &[&str] = &["trace_id", "span_id"];
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("span_id", DataType::FixedSizeBinary(8), false),
        ]));
        let rows = ROW_GROUP_SIZE * ROW_GROUPS;
        let mut trace_ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut span_ids = FixedSizeBinaryBuilder::with_capacity(rows, 8);
        for row in 0..rows {
            let row_bytes = u64::try_from(row).expect("row fits u64").to_be_bytes();
            let mut trace_id = [0u8; 16];
            trace_id[..8].copy_from_slice(&row_bytes);
            trace_ids.append_value(trace_id).expect("append 16-byte value");
            span_ids.append_value(row_bytes).expect("append 8-byte value");
        }
        let columns: Vec<ArrayRef> = vec![Arc::new(trace_ids.finish()), Arc::new(span_ids.finish())];
        let batch = RecordBatch::try_new(schema.clone(), columns).expect("record batch");

        let props = build_writer_properties(
            ROW_GROUP_SIZE,
            /* data_page_size_limit_bytes */ 1 << 20,
            BLOOM_FILTER_COLUMNS,
            /* column_encodings */
            &[
                ("trace_id", Encoding::DELTA_BYTE_ARRAY),
                ("span_id", Encoding::DELTA_BYTE_ARRAY),
            ],
        );
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).expect("ArrowWriter");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }

        let file_reader = SerializedFileReader::new(Bytes::from(buf)).expect("file reader");
        let metadata = file_reader.metadata();
        assert_eq!(
            metadata.num_row_groups(),
            ROW_GROUPS,
            "one row group per {ROW_GROUP_SIZE} rows"
        );
        test_support::assert_bloom_filters_after_column_data(metadata);

        let mut bloom_filter_ranges: Vec<(i64, i64)> = metadata
            .row_groups()
            .iter()
            .flat_map(RowGroupMetaData::columns)
            .map(|column| {
                let offset = column.bloom_filter_offset().expect("every column has a bloom filter");
                let length = column.bloom_filter_length().expect("bloom filter length is recorded");
                (offset, i64::from(length))
            })
            .collect();
        assert_eq!(
            bloom_filter_ranges.len(),
            ROW_GROUPS * BLOOM_FILTER_COLUMNS.len(),
            "one bloom filter per column chunk"
        );
        bloom_filter_ranges.sort_unstable();
        for pair in bloom_filter_ranges.windows(2) {
            let (offset, length) = pair[0];
            assert_eq!(
                offset + length,
                pair[1].0,
                "bloom filter at {offset} with length {length} is not followed by the next bloom filter"
            );
        }
    }
}
