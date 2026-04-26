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
