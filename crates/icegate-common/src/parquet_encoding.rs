//! Per-table, per-column Parquet encoding overrides for IceGate tables.
//!
//! Each entry in a list pins a column to a specific Parquet encoding
//! **and disables dictionary encoding for that column** — without
//! disabling dictionary, parquet-rs treats `set_column_encoding` as a
//! fallback that only fires after the per-column dictionary page
//! exceeds `dictionary_page_size_limit` (1 MiB default). At IceGate's
//! row-group sizes (20,000 rows) most numeric and timestamp dictionaries
//! never reach that limit, so the override never runs and the column
//! stays at RLE_DICTIONARY. Forcing dictionary off makes the configured
//! encoding the primary encoding.
//!
//! The encodings below also require `WriterVersion::PARQUET_2_0` — they
//! are not part of the PARQUET_1_0 spec.
//!
//! Lists are **per-table** because effective encoding depends on data
//! shape per table; e.g. timestamp columns benefit from `DELTA_BINARY_PACKED`
//! on every table, but free-form text columns only appear (with
//! `DELTA_LENGTH_BYTE_ARRAY`) where the table actually has them.
//!
//! Inclusion criterion: a column appears here only if forcing the
//! configured encoding (dictionary off) clearly beats dictionary+RLE
//! given the column's cardinality and value distribution. Low-cardinality
//! enums (`severity_text`, `kind`, `status_code`, `metric_type`,
//! `is_monotonic`, `aggregation_temporality`), leading sort-key strings
//! (`cloud_account_id`, `service_name`, `metric_name`,
//! `service_instance_id`), and small-range ints
//! (`flags`, `dropped_*_count`, `scale`, `*_offset`) are deliberately
//! omitted — dictionary already wins for those.
//!
//! Hard exclusion: any column surfaced through tag-value enumeration
//! (Tempo `/api/v*/search/tag/<name>/values`, Loki
//! `/loki/api/v1/label/<name>/values`) **must not** appear here. The
//! query-side `engine::metadata_scan` reads only Parquet dictionary
//! pages, so disabling the dictionary makes enumeration return the empty
//! set for that column and the corresponding Grafana picker goes blank.
//! The canonical "enumerated string columns" list is whatever
//! `crate::tempo::metadata::target_column_for_tag` resolves to a STRING
//! column — currently the spans `name` intrinsic, plus `service_name`
//! and `cloud_account_id`. Treat that mapping as load-bearing when
//! adding new entries here.
//!
//! Encodings used:
//! - `DELTA_BINARY_PACKED` (INT32/INT64): timestamps, durations, monotonic
//!   counters. Sort orders end with `timestamp DESC` so values are
//!   near-monotonic within a row group; deltas pack to ~1–2 bytes per row.
//! - `DELTA_LENGTH_BYTE_ARRAY` (BYTE_ARRAY): high-cardinality
//!   variable-length payloads (`body`, `status_message`, `trace_state`,
//!   `event_name`). The spans `name` column is deliberately excluded —
//!   it is enumerated via `/api/v2/search/tag/name/values` and so must
//!   keep its dictionary page (see hard exclusion above).
//! - `DELTA_BYTE_ARRAY` (FIXED_LEN_BYTE_ARRAY): `trace_id`, `span_id`,
//!   `parent_span_id`. Every IceGate sort order ends with
//!   `(service_name, timestamp DESC)`, so a trace's rows cluster
//!   contiguously inside a row group even on tables where the IDs are
//!   not themselves sort keys (logs, events, spans). That clustering
//!   produces long runs of identical values plus prefix-shared neighbours
//!   — `DELTA_BYTE_ARRAY` collapses both to (shared-prefix-length,
//!   suffix-bytes), beating dictionary + RLE on high-cardinality random
//!   IDs that spill the dictionary anyway. Empirically ~3 B/row for a
//!   16-byte trace_id and ~4 B/row for an 8-byte span_id on logs.
//! - `BYTE_STREAM_SPLIT` (DOUBLE): metric float columns; pure byte-plane
//!   shuffle that pays off only when followed by a compression codec
//!   (IceGate uses ZSTD).

use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;

/// A `(column_name, encoding)` override. Dictionary is **always**
/// disabled for the named column when this override is applied; see
/// the module-level docs for why.
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

/// Encoding overrides for the `logs` table.
///
/// Sort order: `cloud_account_id, service_name, timestamp DESC`. Sort-key
/// strings stay on dictionary; timestamps get `DELTA_BINARY_PACKED`;
/// `trace_id`/`span_id` get `DELTA_BYTE_ARRAY` (see module-level docs for
/// why).
pub const LOGS_COLUMN_ENCODINGS: &[ColumnEncoding] = &[
    ("timestamp", Encoding::DELTA_BINARY_PACKED),
    ("observed_timestamp", Encoding::DELTA_BINARY_PACKED),
    ("ingested_timestamp", Encoding::DELTA_BINARY_PACKED),
    ("trace_id", Encoding::DELTA_BYTE_ARRAY),
    ("span_id", Encoding::DELTA_BYTE_ARRAY),
];

/// Encoding overrides for the `spans` table.
///
/// Sort order: `cloud_account_id, service_name, timestamp DESC`.
/// `trace_id`/`span_id`/`parent_span_id` get `DELTA_BYTE_ARRAY` for the
/// same reason as logs — adjacent rows of a trace cluster together inside
/// the (service, timestamp) bucket.
///
/// `name` is intentionally absent: it is the spans `name` intrinsic
/// surfaced via `/api/v2/search/tag/name/values`, and the Tempo metadata
/// scan reads only dictionary pages. Forcing a non-dict encoding for
/// `name` empties the Grafana "Span Name" picker. See the module-level
/// "Hard exclusion" docs.
pub const SPANS_COLUMN_ENCODINGS: &[ColumnEncoding] = &[
    ("timestamp", Encoding::DELTA_BINARY_PACKED),
    ("end_timestamp", Encoding::DELTA_BINARY_PACKED),
    ("ingested_timestamp", Encoding::DELTA_BINARY_PACKED),
    ("duration_micros", Encoding::DELTA_BINARY_PACKED),
    ("status_message", Encoding::DELTA_LENGTH_BYTE_ARRAY),
    ("trace_state", Encoding::DELTA_LENGTH_BYTE_ARRAY),
    ("trace_id", Encoding::DELTA_BYTE_ARRAY),
    ("span_id", Encoding::DELTA_BYTE_ARRAY),
    ("parent_span_id", Encoding::DELTA_BYTE_ARRAY),
];

/// Encoding overrides for the `events` table.
///
/// Sort order: `cloud_account_id, service_name, timestamp DESC`.
/// `trace_id`/`span_id` get `DELTA_BYTE_ARRAY` — same reasoning as logs.
pub const EVENTS_COLUMN_ENCODINGS: &[ColumnEncoding] = &[
    ("timestamp", Encoding::DELTA_BINARY_PACKED),
    ("observed_timestamp", Encoding::DELTA_BINARY_PACKED),
    ("ingested_timestamp", Encoding::DELTA_BINARY_PACKED),
    ("trace_id", Encoding::DELTA_BYTE_ARRAY),
    ("span_id", Encoding::DELTA_BYTE_ARRAY),
];

/// Encoding overrides for the `metrics` table.
///
/// Sort order: `cloud_account_id, metric_name, service_name,
/// service_instance_id, timestamp DESC`. Float-valued metric columns
/// get `BYTE_STREAM_SPLIT` to expose byte-plane locality to ZSTD.
pub const METRICS_COLUMN_ENCODINGS: &[ColumnEncoding] = &[
    ("timestamp", Encoding::DELTA_BINARY_PACKED),
    ("start_timestamp", Encoding::DELTA_BINARY_PACKED),
    ("ingested_timestamp", Encoding::DELTA_BINARY_PACKED),
    ("count", Encoding::DELTA_BINARY_PACKED),
    ("zero_count", Encoding::DELTA_BINARY_PACKED),
    ("value_double", Encoding::BYTE_STREAM_SPLIT),
    ("sum", Encoding::BYTE_STREAM_SPLIT),
    ("min", Encoding::BYTE_STREAM_SPLIT),
    ("max", Encoding::BYTE_STREAM_SPLIT),
    ("zero_threshold", Encoding::BYTE_STREAM_SPLIT),
];
