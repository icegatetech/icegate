//! Per-table, per-column Parquet encoding overrides for IceGate tables.
//!
//! Each entry in a list pins a column to a specific Parquet encoding
//! **and disables dictionary encoding for that column** when consumed by
//! [`crate::parquet_writer::build_writer_properties`]. Without disabling
//! dictionary, parquet-rs treats `set_column_encoding` as a fallback
//! that only fires after the per-column dictionary page exceeds
//! `dictionary_page_size_limit` (1 MiB default). At IceGate's row-group
//! sizes (~20,000 rows) most numeric and timestamp dictionaries never
//! reach that limit, so the override never runs and the column stays at
//! `RLE_DICTIONARY`. Forcing dictionary off makes the configured
//! encoding the primary encoding.
//!
//! The encodings below also require `WriterVersion::PARQUET_2_0` — they
//! are not part of the `PARQUET_1_0` spec.
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

use parquet::basic::Encoding;

use crate::parquet_writer::ColumnEncoding;
use crate::schema::{
    COL_COUNT, COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_INGESTED_TIMESTAMP, COL_MAX, COL_MIN,
    COL_OBSERVED_TIMESTAMP, COL_PARENT_SPAN_ID, COL_SPAN_ID, COL_START_TIMESTAMP, COL_STATUS_MESSAGE, COL_SUM,
    COL_TIMESTAMP, COL_TRACE_ID, COL_TRACE_STATE, COL_VALUE_DOUBLE, COL_ZERO_COUNT, COL_ZERO_THRESHOLD,
};

/// Encoding overrides for the `logs` table.
///
/// Sort order: `cloud_account_id, service_name, timestamp DESC`. Sort-key
/// strings stay on dictionary; timestamps get `DELTA_BINARY_PACKED`;
/// `trace_id`/`span_id` get `DELTA_BYTE_ARRAY` (see module-level docs for
/// why).
pub const LOGS_COLUMN_ENCODINGS: &[ColumnEncoding] = &[
    (COL_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_OBSERVED_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_INGESTED_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_TRACE_ID, Encoding::DELTA_BYTE_ARRAY),
    (COL_SPAN_ID, Encoding::DELTA_BYTE_ARRAY),
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
    (COL_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_END_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_INGESTED_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_DURATION_MICROS, Encoding::DELTA_BINARY_PACKED),
    (COL_STATUS_MESSAGE, Encoding::DELTA_LENGTH_BYTE_ARRAY),
    (COL_TRACE_STATE, Encoding::DELTA_LENGTH_BYTE_ARRAY),
    (COL_TRACE_ID, Encoding::DELTA_BYTE_ARRAY),
    (COL_SPAN_ID, Encoding::DELTA_BYTE_ARRAY),
    (COL_PARENT_SPAN_ID, Encoding::DELTA_BYTE_ARRAY),
];

/// Encoding overrides for the `events` table.
///
/// Sort order: `cloud_account_id, service_name, timestamp DESC`.
/// `trace_id`/`span_id` get `DELTA_BYTE_ARRAY` — same reasoning as logs.
pub const EVENTS_COLUMN_ENCODINGS: &[ColumnEncoding] = &[
    (COL_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_OBSERVED_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_INGESTED_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_TRACE_ID, Encoding::DELTA_BYTE_ARRAY),
    (COL_SPAN_ID, Encoding::DELTA_BYTE_ARRAY),
];

/// Encoding overrides for the `metrics` table.
///
/// Sort order: `cloud_account_id, metric_name, service_name,
/// service_instance_id, timestamp DESC`. Float-valued metric columns
/// get `BYTE_STREAM_SPLIT` to expose byte-plane locality to ZSTD.
pub const METRICS_COLUMN_ENCODINGS: &[ColumnEncoding] = &[
    (COL_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_START_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_INGESTED_TIMESTAMP, Encoding::DELTA_BINARY_PACKED),
    (COL_COUNT, Encoding::DELTA_BINARY_PACKED),
    (COL_ZERO_COUNT, Encoding::DELTA_BINARY_PACKED),
    (COL_VALUE_DOUBLE, Encoding::BYTE_STREAM_SPLIT),
    (COL_SUM, Encoding::BYTE_STREAM_SPLIT),
    (COL_MIN, Encoding::BYTE_STREAM_SPLIT),
    (COL_MAX, Encoding::BYTE_STREAM_SPLIT),
    (COL_ZERO_THRESHOLD, Encoding::BYTE_STREAM_SPLIT),
];
