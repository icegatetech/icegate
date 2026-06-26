//! Parquet [`WriterProperties`] builder shared across IceGate writers.
//!
//! The builder owns the cross-cutting writer policy (PARQUET_2_0, ZSTD,
//! page-level statistics, page-size limit, row-group cap) and applies the
//! per-column encoding overrides + bloom-filter list supplied by the
//! caller. Encoding overrides themselves live in
//! [`crate::parquet_encoding`] — this module is purely about *how* the
//! Parquet writer is configured, not *which* columns get which encoding.
//!
//! Splitting these two concerns is intentional: the encoding policy is
//! data-shape-driven (per-table, per-column), whereas the writer
//! properties (compression, page size, row-group size, dictionary
//! disable) are global and only differ between WAL and shift writers in
//! the row-group/page-size dimensions.
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
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
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
        .set_max_row_group_size(row_group_size);

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
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Encoding;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::{ColumnEncoding, build_writer_properties};

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
}
