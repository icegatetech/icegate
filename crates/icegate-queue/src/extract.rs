//! Generic field-extraction descriptors used by [`crate::QueueReader::plan_segments`].
//!
//! Callers describe which per-row-group facts they need extracted from a WAL
//! parquet footer (e.g. a column-stats singleton, or a key/value-encoded
//! row-group payload), and the reader returns those values keyed by field
//! name in [`crate::RowGroupPlanEntry::extracted`]. This keeps the queue
//! reader agnostic of caller-specific concepts like "tenant" or "boundary
//! range".

use std::collections::HashMap;

use parquet::file::{metadata::ParquetMetaData, statistics::Statistics};

use crate::error::{QueueError, Result};

/// Strategy used to materialise a field's value from the parquet metadata of
/// a single row group.
#[derive(Debug, Clone)]
pub enum FieldExtractor {
    /// Read column statistics for `column_name`; require the row group to
    /// contain a single (utf8) value (`min == max`) and return it.
    ///
    /// Use for columns that are guaranteed to be partitioned per-row-group at
    /// write time (e.g. `tenant_id`).
    ColumnStatsUtf8Singleton {
        /// Parquet schema column name.
        column_name: String,
    },
    /// Read column statistics for `column_name` and return the physical
    /// `(min, max)` range in microseconds. Required for partition decisions
    /// that depend on the actual span of values across the row group rather
    /// than on the timestamps of the boundary rows under a composite sort
    /// key (which can hide non-monotonic interior timestamps).
    ///
    /// The column must be parquet `INT64` (the physical layout used for
    /// `Timestamp(Microsecond, _)` arrow types). Returns `None` for the row
    /// group when statistics or min/max are absent (e.g. all-null column).
    ColumnStatsTimestampMicrosRange {
        /// Parquet schema column name.
        column_name: String,
    },
    /// Read the file-level key/value metadata entry under `key`, parse it as
    /// the WAL row-group payload format, and index it by `row_group_idx`.
    ///
    /// Use for opaque per-row-group payloads written by the WAL writer (e.g.
    /// a serialized `RowGroupBoundaryRange`).
    FileKeyValueRowGroupPayload {
        /// File-level key/value metadata key.
        key: String,
    },
}

/// One field to extract per row group, identified by `name` in the resulting
/// [`crate::RowGroupPlanEntry::extracted`] map.
#[derive(Debug, Clone)]
pub struct ExtractField {
    /// Caller-chosen field name; used as the map key in the result.
    pub name: String,
    /// Extraction strategy.
    pub extractor: FieldExtractor,
}

/// Value materialised by an extractor for one row group.
///
/// Extend this enum when adding non-utf8 extractors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractedValue {
    /// UTF-8 string value (covers both column-stats singletons and
    /// JSON-encoded row-group payloads).
    Utf8(String),
    /// Physical `(min, max)` range of a microsecond-precision timestamp
    /// column, in microseconds. Inclusive on both ends.
    TimestampMicrosRange(i64, i64),
}

impl ExtractedValue {
    /// Borrow the string payload if this is a [`Self::Utf8`] value, otherwise
    /// return `None`.
    #[must_use]
    pub fn as_utf8(&self) -> Option<&str> {
        match self {
            Self::Utf8(s) => Some(s.as_str()),
            Self::TimestampMicrosRange(_, _) => None,
        }
    }

    /// Consume the value and return the inner [`String`] if it is a
    /// [`Self::Utf8`] payload, otherwise return `None`.
    #[must_use]
    pub fn into_utf8(self) -> Option<String> {
        match self {
            Self::Utf8(s) => Some(s),
            Self::TimestampMicrosRange(_, _) => None,
        }
    }
}

/// Per-segment resolved view of an [`ExtractField`]: column index lookup,
/// JSON-parsed payload, etc. — done once per segment so the per-row-group
/// loop avoids redundant work.
pub(crate) enum ResolvedField<'name> {
    ColumnStatsUtf8Singleton {
        name: &'name str,
        column_name: &'name str,
        column_idx: usize,
    },
    ColumnStatsTimestampMicrosRange {
        name: &'name str,
        column_name: &'name str,
        column_idx: usize,
    },
    FileKeyValueRowGroupPayload {
        name: &'name str,
        payloads_by_row_group: HashMap<usize, String>,
    },
}

impl<'name> ResolvedField<'name> {
    pub(crate) fn resolve(field: &'name ExtractField, parquet_meta: &ParquetMetaData, wal_offset: u64) -> Result<Self> {
        match &field.extractor {
            FieldExtractor::ColumnStatsUtf8Singleton { column_name } => {
                let column_idx = column_index(parquet_meta, column_name, wal_offset)?;
                Ok(Self::ColumnStatsUtf8Singleton {
                    name: field.name.as_str(),
                    column_name: column_name.as_str(),
                    column_idx,
                })
            }
            FieldExtractor::ColumnStatsTimestampMicrosRange { column_name } => {
                let column_idx = column_index(parquet_meta, column_name, wal_offset)?;
                Ok(Self::ColumnStatsTimestampMicrosRange {
                    name: field.name.as_str(),
                    column_name: column_name.as_str(),
                    column_idx,
                })
            }
            FieldExtractor::FileKeyValueRowGroupPayload { key } => {
                let payloads_by_row_group = file_kv_row_group_payloads(parquet_meta, key, wal_offset)?;
                Ok(Self::FileKeyValueRowGroupPayload {
                    name: field.name.as_str(),
                    payloads_by_row_group,
                })
            }
        }
    }

    pub(crate) const fn name(&self) -> &str {
        match self {
            Self::ColumnStatsUtf8Singleton { name, .. }
            | Self::ColumnStatsTimestampMicrosRange { name, .. }
            | Self::FileKeyValueRowGroupPayload { name, .. } => name,
        }
    }

    pub(crate) fn extract(
        &self,
        row_group: &parquet::file::metadata::RowGroupMetaData,
        row_group_idx: usize,
        wal_offset: u64,
    ) -> Result<Option<ExtractedValue>> {
        match self {
            Self::ColumnStatsUtf8Singleton {
                column_name,
                column_idx,
                ..
            } => extract_column_stats_utf8_singleton(row_group, *column_idx, column_name, row_group_idx, wal_offset)
                .map(|v| Some(ExtractedValue::Utf8(v))),
            Self::ColumnStatsTimestampMicrosRange {
                column_name,
                column_idx,
                ..
            } => extract_column_stats_timestamp_micros_range(
                row_group,
                *column_idx,
                column_name,
                row_group_idx,
                wal_offset,
            )
            .map(|maybe| maybe.map(|(min, max)| ExtractedValue::TimestampMicrosRange(min, max))),
            Self::FileKeyValueRowGroupPayload {
                payloads_by_row_group, ..
            } => Ok(payloads_by_row_group.get(&row_group_idx).cloned().map(ExtractedValue::Utf8)),
        }
    }
}

fn column_index(parquet_meta: &ParquetMetaData, column_name: &str, wal_offset: u64) -> Result<usize> {
    parquet_meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|col| col.name() == column_name)
        .ok_or_else(|| {
            QueueError::Metadata(format!(
                "column '{column_name}' not found in parquet schema for segment {wal_offset}"
            ))
        })
}

fn extract_column_stats_utf8_singleton(
    row_group: &parquet::file::metadata::RowGroupMetaData,
    column_idx: usize,
    column_name: &str,
    row_group_idx: usize,
    wal_offset: u64,
) -> Result<String> {
    let column = row_group.column(column_idx);
    let stats = column.statistics().ok_or_else(|| {
        QueueError::Metadata(format!(
            "missing statistics for '{column_name}' in row group {row_group_idx} of WAL segment {wal_offset}"
        ))
    })?;

    match stats {
        Statistics::ByteArray(byte_stats) => {
            let min = byte_stats.min_bytes_opt().ok_or_else(|| {
                QueueError::Metadata(format!(
                    "missing min statistic for '{column_name}' in row group {row_group_idx} of WAL segment {wal_offset}"
                ))
            })?;
            let max = byte_stats.max_bytes_opt().ok_or_else(|| {
                QueueError::Metadata(format!(
                    "missing max statistic for '{column_name}' in row group {row_group_idx} of WAL segment {wal_offset}"
                ))
            })?;

            if min != max {
                return Err(QueueError::Metadata(format!(
                    "row group {row_group_idx} contains multiple values for '{column_name}' in WAL segment {wal_offset}"
                )));
            }

            let value = std::str::from_utf8(min).map_err(|e| {
                QueueError::Metadata(format!(
                    "invalid utf8 in '{column_name}' stats for row group {row_group_idx} of WAL segment {wal_offset}: {e}"
                ))
            })?;

            if value.is_empty() {
                return Err(QueueError::Metadata(format!(
                    "empty value in stats for '{column_name}' row group {row_group_idx} of WAL segment {wal_offset}"
                )));
            }

            Ok(value.to_string())
        }
        _ => Err(QueueError::Metadata(format!(
            "unsupported stats type for '{column_name}' in row group {row_group_idx} of WAL segment {wal_offset}"
        ))),
    }
}

/// Extract physical `(min, max)` timestamp microseconds from row-group column
/// stats. Returns `Ok(None)` if either stat is absent — typically the
/// column is all-null in this row group — so the caller can decide on a
/// fallback (e.g. cross-partition bucketing).
fn extract_column_stats_timestamp_micros_range(
    row_group: &parquet::file::metadata::RowGroupMetaData,
    column_idx: usize,
    column_name: &str,
    row_group_idx: usize,
    wal_offset: u64,
) -> Result<Option<(i64, i64)>> {
    let column = row_group.column(column_idx);
    let Some(stats) = column.statistics() else {
        return Ok(None);
    };

    match stats {
        Statistics::Int64(int_stats) => {
            let (Some(&min), Some(&max)) = (int_stats.min_opt(), int_stats.max_opt()) else {
                return Ok(None);
            };
            if min > max {
                return Err(QueueError::Metadata(format!(
                    "row group {row_group_idx} has inverted timestamp stats for '{column_name}' \
                     (min={min} > max={max}) in WAL segment {wal_offset}"
                )));
            }
            Ok(Some((min, max)))
        }
        _ => Err(QueueError::Metadata(format!(
            "expected INT64 stats for timestamp column '{column_name}' in row group {row_group_idx} \
             of WAL segment {wal_offset}"
        ))),
    }
}

fn file_kv_row_group_payloads(
    parquet_meta: &ParquetMetaData,
    key: &str,
    wal_offset: u64,
) -> Result<HashMap<usize, String>> {
    let Some(key_value_metadata) = parquet_meta.file_metadata().key_value_metadata() else {
        return Ok(HashMap::new());
    };
    let Some(metadata_value) = key_value_metadata
        .iter()
        .find(|entry| entry.key == key)
        .and_then(|entry| entry.value.as_ref())
    else {
        return Ok(HashMap::new());
    };
    let entries: Vec<RowGroupMetadataEntry> = serde_json::from_str(metadata_value).map_err(|err| {
        QueueError::Metadata(format!(
            "invalid row-group metadata under '{key}' in WAL segment {wal_offset}: {err}"
        ))
    })?;
    let row_group_count = parquet_meta.row_groups().len();
    let mut payloads = HashMap::with_capacity(entries.len());
    for entry in entries {
        if entry.row_group_idx >= row_group_count {
            return Err(QueueError::Metadata(format!(
                "row-group metadata row_group_idx {} under '{key}' is out of range for WAL segment {wal_offset}",
                entry.row_group_idx
            )));
        }
        if payloads.insert(entry.row_group_idx, entry.payload).is_some() {
            return Err(QueueError::Metadata(format!(
                "duplicate row-group metadata under '{key}' for row group {} in WAL segment {wal_offset}",
                entry.row_group_idx
            )));
        }
    }
    Ok(payloads)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RowGroupMetadataEntry {
    row_group_idx: usize,
    payload: String,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use bytes::Bytes;
    use parquet::{
        arrow::ArrowWriter,
        file::{
            metadata::{KeyValue, ParquetMetaData},
            properties::{EnabledStatistics, WriterProperties},
            reader::FileReader,
            serialized_reader::SerializedFileReader,
        },
    };

    use super::{ExtractField, FieldExtractor, ResolvedField};
    use crate::error::QueueError;

    /// Build a parquet payload from one batch per row group, optionally
    /// embedding file-level key/value metadata. Each `flush()` between batches
    /// closes the current row group, so `batches.len()` row groups end up in
    /// the footer.
    fn build_parquet(batches: Vec<RecordBatch>, kv_metadata: Vec<(String, String)>) -> Bytes {
        assert!(!batches.is_empty(), "test parquet must contain at least one batch");
        let schema = batches[0].schema();
        let kv = kv_metadata
            .into_iter()
            .map(|(key, value)| KeyValue {
                key,
                value: Some(value),
            })
            .collect::<Vec<_>>();
        let mut builder = WriterProperties::builder().set_statistics_enabled(EnabledStatistics::Chunk);
        if !kv.is_empty() {
            builder = builder.set_key_value_metadata(Some(kv));
        }
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(builder.build())).expect("arrow writer");
            for batch in batches {
                writer.write(&batch).expect("write batch");
                writer.flush().expect("flush row group");
            }
            writer.close().expect("close writer");
        }
        Bytes::from(buf)
    }

    fn parse_metadata(bytes: &Bytes) -> ParquetMetaData {
        let reader = SerializedFileReader::new(bytes.clone()).expect("serialized reader");
        reader.metadata().clone()
    }

    /// Single-column UTF-8 batch with the given values; used to populate
    /// column statistics for `ColumnStatsUtf8Singleton` tests.
    fn utf8_batch(column_name: &str, values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(column_name, DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values)) as ArrayRef]).expect("utf8 batch")
    }

    /// Single-column INT64 batch; used to fail the timestamp-range extractor's
    /// physical-type guard by handing it INT64 stats under a non-timestamp
    /// schema and then retargeting the extractor at it.
    fn int64_batch(column_name: &str, values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(column_name, DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef]).expect("int64 batch")
    }

    fn utf8_singleton_field(plan_field: &str, column_name: &str) -> ExtractField {
        ExtractField {
            name: plan_field.to_string(),
            extractor: FieldExtractor::ColumnStatsUtf8Singleton {
                column_name: column_name.to_string(),
            },
        }
    }

    fn timestamp_range_field(plan_field: &str, column_name: &str) -> ExtractField {
        ExtractField {
            name: plan_field.to_string(),
            extractor: FieldExtractor::ColumnStatsTimestampMicrosRange {
                column_name: column_name.to_string(),
            },
        }
    }

    fn payload_field(plan_field: &str, key: &str) -> ExtractField {
        ExtractField {
            name: plan_field.to_string(),
            extractor: FieldExtractor::FileKeyValueRowGroupPayload { key: key.to_string() },
        }
    }

    fn assert_metadata_error<T>(result: Result<T, QueueError>) {
        match result {
            Err(QueueError::Metadata(_)) => {}
            Err(other) => panic!("expected QueueError::Metadata, got {other:?}"),
            Ok(_) => panic!("expected metadata error, got Ok"),
        }
    }

    /// `ColumnStatsUtf8Singleton::resolve` must error when the requested
    /// column does not exist in the parquet schema. Single line of defense
    /// against the planner silently mapping the wrong column to a partition
    /// value.
    #[test]
    fn column_stats_utf8_singleton_missing_column_errors() {
        let bytes = build_parquet(vec![utf8_batch("present", vec![Some("a"), Some("a")])], Vec::new());
        let meta = parse_metadata(&bytes);
        let field = utf8_singleton_field("plan", "absent");

        assert_metadata_error(ResolvedField::resolve(&field, &meta, 7).map(|_| ()));
    }

    /// All-null UTF-8 column produces row-group statistics with no min/max,
    /// so the singleton extractor must surface a metadata error rather than
    /// returning a default-empty value.
    #[test]
    fn column_stats_utf8_singleton_null_value_errors() {
        let bytes = build_parquet(vec![utf8_batch("tenant_id", vec![None, None])], Vec::new());
        let meta = parse_metadata(&bytes);
        let field = utf8_singleton_field("plan", "tenant_id");
        let resolved = ResolvedField::resolve(&field, &meta, 7).expect("resolve ok");
        let row_group = meta.row_group(0);

        assert_metadata_error(resolved.extract(row_group, 0, 7).map(|_| ()));
    }

    /// `ColumnStatsTimestampMicrosRange` requires INT64 statistics. A UTF-8
    /// column targeted by the same extractor must fail the physical-type
    /// guard rather than fall back to a permissive `None`.
    #[test]
    fn column_stats_timestamp_range_wrong_type_errors() {
        // Parquet whose only column is a UTF-8 string column named
        // "timestamp"; pointing the timestamp extractor at it triggers the
        // INT64-only guard.
        let bytes = build_parquet(
            vec![utf8_batch("timestamp", vec![Some("not-a-number"), Some("either")])],
            Vec::new(),
        );
        let meta = parse_metadata(&bytes);
        let field = timestamp_range_field("plan", "timestamp");
        let resolved = ResolvedField::resolve(&field, &meta, 7).expect("resolve ok");
        let row_group = meta.row_group(0);

        assert_metadata_error(resolved.extract(row_group, 0, 7).map(|_| ()));
    }

    /// Malformed JSON in the file-level key/value payload must surface as a
    /// metadata error during `resolve` — the planner cannot recover from a
    /// corrupted boundary payload, and silent loss of the field would
    /// degrade downstream clustering invisibly.
    #[test]
    fn file_kv_payload_malformed_json_errors() {
        let bytes = build_parquet(
            vec![int64_batch("dummy", vec![1])],
            vec![("boundary".to_string(), "{not json".to_string())],
        );
        let meta = parse_metadata(&bytes);
        let field = payload_field("plan", "boundary");

        assert_metadata_error(ResolvedField::resolve(&field, &meta, 7).map(|_| ()));
    }

    /// Two key/value entries pointing at the same `row_group_idx` must error
    /// — the WAL writer never produces duplicates, so a duplicate signals
    /// corruption that we refuse to silently dedup.
    #[test]
    fn file_kv_payload_multiple_row_groups_errors() {
        let entries = serde_json::to_string(&[
            super::RowGroupMetadataEntry {
                row_group_idx: 0,
                payload: "first".to_string(),
            },
            super::RowGroupMetadataEntry {
                row_group_idx: 0,
                payload: "second".to_string(),
            },
        ])
        .expect("serialize entries");
        let bytes = build_parquet(
            vec![int64_batch("dummy", vec![1])],
            vec![("boundary".to_string(), entries)],
        );
        let meta = parse_metadata(&bytes);
        let field = payload_field("plan", "boundary");

        assert_metadata_error(ResolvedField::resolve(&field, &meta, 7).map(|_| ()));
    }
}
