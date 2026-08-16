//! `/label_values` algorithm.
//!
//! Two cases, selected by [`classify_label`]:
//!
//! 1. The label maps to an indexed top-level column (e.g. `service_name`,
//!    `trace_id`, or the `level` alias for `severity_text`) — resolved by
//!    reading only its dictionary page. No row data is decoded.
//! 2. Any other label — routed through the `attributes` MAP lookup.
//!    This case needs correlated key/value access (so we can return the
//!    value for the rows where `key == label_name`) and is implemented
//!    via a column-projected record-batch stream over the `attributes`
//!    column.

use std::collections::BTreeSet;

use datafusion::arrow::array::{Array, MapArray, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::arrow::ArrowFileReader;
use iceberg::expr::Predicate;
use icegate_common::attribute_key::matches_wire_name;
use icegate_common::schema::COL_TENANT_ID;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;

use super::MetadataScanConfig;
use super::error::MetadataScanError;
use super::parquet_reader;

/// Whether `name` matches a system-reserved column whose values must
/// never be enumerated through tag-discovery, regardless of caller
/// validation. Mirrors the discovery-side guard in
/// [`super::labels::is_system_reserved`] — see that module for the
/// rationale.
fn is_system_reserved_value_column(name: &str) -> bool {
    name.eq_ignore_ascii_case(COL_TENANT_ID)
}

/// Which code path to use for a given label name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelKind {
    /// Indexed top-level column (e.g. `service_name`, `trace_id`, `level`).
    /// Read via dictionary page only.
    Indexed,
    /// Label stored in the `attributes` MAP. Read via projected record
    /// batches over the `attributes` column.
    MapAttribute,
}

/// Classify a label name as indexed or MAP-stored, using the supplied
/// per-table config. Aliases in `config.label_aliases` are resolved to their
/// underlying column before the indexed-column check.
#[must_use]
pub fn classify_label(name: &str, config: &MetadataScanConfig) -> LabelKind {
    if config.is_indexed(name) {
        LabelKind::Indexed
    } else {
        LabelKind::MapAttribute
    }
}

/// Collect distinct values for an indexed top-level string column by
/// reading only its dictionary page for every row group that survives
/// row-group predicate pruning.
///
/// # Errors
///
/// Returns `MetadataScanError::Parquet` if a column chunk fails to decode.
#[tracing::instrument(skip_all, fields(column = column_name))]
pub async fn collect_indexed_values_via_dict(
    reader: &mut ArrowFileReader,
    metadata: &ParquetMetaData,
    predicate: &Predicate,
    column_name: &str,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    if is_system_reserved_value_column(column_name) {
        // Refuse at the metadata-scan layer — tenant_id is system
        // metadata and must never be enumerated as a label/tag value
        // even if HTTP-layer validation is bypassed.
        return Ok(());
    }
    let schema = metadata.file_metadata().schema_descr();
    let Some(leaf_idx) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == column_name) else {
        // Column not present in this file — nothing to do. Not an error
        // under over-approximation semantics.
        return Ok(());
    };

    parquet_reader::read_column_dictionaries(reader, metadata, predicate, leaf_idx, out).await
}

/// Collect distinct values for an indexed top-level INT32 column by
/// reading only its dictionary page for every row group that survives
/// row-group predicate pruning.
///
/// Mirrors [`collect_indexed_values_via_dict`] but for fixed-width
/// integer columns (used for the `status_code` and `kind` enum columns
/// on the spans table). Files lacking the named column contribute
/// nothing — over-approximation semantics.
///
/// # Errors
///
/// Returns `MetadataScanError::Parquet` if a column chunk fails to decode.
#[tracing::instrument(skip_all, fields(column = column_name))]
pub async fn collect_indexed_int_values_via_dict(
    reader: &mut ArrowFileReader,
    metadata: &ParquetMetaData,
    predicate: &Predicate,
    column_name: &str,
    out: &mut BTreeSet<i32>,
) -> Result<(), MetadataScanError> {
    if is_system_reserved_value_column(column_name) {
        // Mirror [`collect_indexed_values_via_dict`] — refuse at the
        // metadata-scan layer for system metadata columns even if the
        // HTTP layer's reserved-name validation is bypassed.
        return Ok(());
    }
    let schema = metadata.file_metadata().schema_descr();
    let Some(leaf_idx) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == column_name) else {
        return Ok(());
    };

    parquet_reader::read_column_int_dictionaries(reader, metadata, predicate, leaf_idx, out).await
}

/// Project the configured MAP column and collect distinct values for a
/// single label key across surviving row groups. Consumes the builder.
///
/// Row groups whose statistics are incompatible with the given
/// `predicate` (tenant, time range, etc.) are pruned before scanning.
///
/// # Errors
///
/// Returns `MetadataScanError::Parquet` if projected record-batch reads
/// fail, or `MetadataScanError::Schema` if the map column has an unexpected
/// type.
#[tracing::instrument(skip_all, fields(map_columns = tracing::field::Empty, label_name = label_name, num_batches = tracing::field::Empty, pruned_rgs = tracing::field::Empty))]
pub async fn stream_map_values(
    builder: ParquetRecordBatchStreamBuilder<ArrowFileReader>,
    predicate: &Predicate,
    configs: &[&MetadataScanConfig],
    label_name: &str,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    if is_system_reserved_value_column(label_name) {
        // Refuse at the metadata-scan layer — even if some ingest path
        // smuggles `tenant_id` into the attributes MAP, we never
        // enumerate its values.
        return Ok(());
    }
    let schema_descr = builder.parquet_schema();
    // Only configs whose map column exists in THIS file take part: a file
    // written before a schema evolution carries some but not all of them, and
    // projecting an absent column is an error rather than an empty result.
    let present: Vec<&&MetadataScanConfig> = configs
        .iter()
        .filter(|config| {
            (0..schema_descr.num_columns()).any(|i| {
                schema_descr
                    .column(i)
                    .path()
                    .parts()
                    .first()
                    .is_some_and(|s| *s == config.map_column)
            })
        })
        .collect();
    if present.is_empty() {
        return Ok(());
    }

    // One projection over every surviving map column, so a request that spans
    // the resource/scope/record levels costs one pass over the file rather
    // than one per level.
    let mut projected: Vec<&str> = present.iter().map(|config| config.map_column).collect();
    projected.sort_unstable();
    projected.dedup();
    tracing::Span::current().record("map_columns", projected.join(","));

    // Row-group pruning: only scan row groups whose statistics
    // are compatible with the predicate (tenant_id, time range, etc.).
    let metadata = builder.metadata();
    let total_rgs = metadata.num_row_groups();
    let surviving: Vec<usize> = (0..total_rgs)
        .filter(|&i| parquet_reader::row_group_can_match(metadata.row_group(i), predicate))
        .collect();
    let pruned = total_rgs - surviving.len();
    tracing::Span::current().record("pruned_rgs", pruned);

    let mask = ProjectionMask::columns(schema_descr, projected);
    let mut stream = builder.with_projection(mask).with_row_groups(surviving).build()?;

    let mut num_batches: usize = 0;
    while let Some(batch) = stream.try_next().await? {
        num_batches += 1;
        for config in &present {
            collect_map_values_from_batch(&batch, config.map_column, label_name, config.normalize_keys, out)?;
        }
    }
    tracing::Span::current().record("num_batches", num_batches);

    Ok(())
}

/// Project two MAP columns and collect values with per-row primary precedence.
///
/// A fallback value is collected only when the same row has no non-null match
/// in the primary map. Missing map columns contribute no values, which preserves
/// metadata-scan behavior across schema evolution.
///
/// # Errors
///
/// Returns `MetadataScanError::Parquet` if projected record-batch reads fail,
/// or `MetadataScanError::Schema` if a projected map has an unexpected type.
#[tracing::instrument(skip_all, fields(primary_map = primary_config.map_column, fallback_map = fallback_config.map_column, label_name = label_name, num_batches = tracing::field::Empty, pruned_rgs = tracing::field::Empty))]
pub async fn stream_coalesced_map_values(
    builder: ParquetRecordBatchStreamBuilder<ArrowFileReader>,
    predicate: &Predicate,
    primary_config: &MetadataScanConfig,
    fallback_config: &MetadataScanConfig,
    label_name: &str,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    if is_system_reserved_value_column(label_name) {
        return Ok(());
    }

    let schema_descr = builder.parquet_schema();
    let projected_columns: Vec<&str> = [primary_config.map_column, fallback_config.map_column]
        .into_iter()
        .filter(|map_column| {
            (0..schema_descr.num_columns()).any(|i| {
                schema_descr
                    .column(i)
                    .path()
                    .parts()
                    .first()
                    .is_some_and(|part| part == map_column)
            })
        })
        .collect();
    if projected_columns.is_empty() {
        return Ok(());
    }

    let metadata = builder.metadata();
    let total_rgs = metadata.num_row_groups();
    let surviving: Vec<usize> = (0..total_rgs)
        .filter(|&i| parquet_reader::row_group_can_match(metadata.row_group(i), predicate))
        .collect();
    tracing::Span::current().record("pruned_rgs", total_rgs - surviving.len());

    let mask = ProjectionMask::columns(schema_descr, projected_columns);
    let mut stream = builder.with_projection(mask).with_row_groups(surviving).build()?;

    let mut num_batches = 0_usize;
    while let Some(batch) = stream.try_next().await? {
        num_batches += 1;
        collect_coalesced_map_values_from_batch(&batch, primary_config, fallback_config, label_name, out)?;
    }
    tracing::Span::current().record("num_batches", num_batches);
    Ok(())
}

/// Whether a raw stored map key satisfies a request for `label_name`,
/// under the table's [`MetadataScanConfig::normalize_keys`] policy.
///
/// `normalize_keys` selects between the two callers of this matching rule:
/// Loki requests the wire-form name (dots replaced by underscores) and
/// needs the stored side normalized before comparison; Tempo/`TraceQL`
/// requests the dotted name as stored and needs an exact comparison, since
/// normalizing the stored side would stop matching entries that are
/// already exact.
///
/// The normalizing arm defers to
/// [`icegate_common::attribute_key::matches_wire_name`] so this shares one
/// definition of the mapping with the matcher, merge, and displayed-label
/// paths.
fn stored_key_matches_label(stored_key: &str, label_name: &str, normalize_keys: bool) -> bool {
    if normalize_keys {
        matches_wire_name(stored_key, label_name)
    } else {
        stored_key == label_name
    }
}

fn collect_map_values_from_batch(
    batch: &RecordBatch,
    map_column: &str,
    label_name: &str,
    normalize_keys: bool,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    let attr_idx = batch
        .schema()
        .index_of(map_column)
        .map_err(|_| MetadataScanError::Schema(format!("batch missing '{map_column}' column")))?;
    let map_arr = batch
        .column(attr_idx)
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| MetadataScanError::Schema(format!("'{map_column}' column is not a MapArray")))?;
    let keys = map_arr
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| MetadataScanError::Schema(format!("'{map_column}' map keys are not StringArray")))?;
    let values = map_arr
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| MetadataScanError::Schema(format!("'{map_column}' map values are not StringArray")))?;

    for i in 0..keys.len() {
        if !keys.is_valid(i) || !values.is_valid(i) {
            continue;
        }
        if !stored_key_matches_label(keys.value(i), label_name, normalize_keys) {
            continue;
        }
        // Two distinct raw keys in the SAME row can both normalize to
        // `label_name` (e.g. `k8s.pod.name` and `k8s_pod_name` both
        // present — ingest dedupes only by the raw string, never by
        // normalized form). The row's actually-DISPLAYED value follows a
        // first-wins rule over stored order (see `loki/formatters.rs`'s
        // `extract_attributes_map` and the `map_get_by_normalized_key` /
        // `merge_attribute_levels` UDFs under `logql/datafusion/udf/`), but
        // every matching key's value is unioned into `out` here regardless
        // of precedence. That OVER-APPROXIMATES: this enumeration can list
        // a value that no row would ever actually display, because a
        // higher-precedence key shadowed it. Acceptable for an enumeration
        // endpoint — Loki's own label-value listing is itself approximate
        // — but written down so it isn't mistaken for an exact set.
        //
        // `BTreeSet::insert` already short-circuits on duplicates and
        // returns `bool` — a separate `contains` check is a wasted O(log
        // n) tree walk per row.
        out.insert(values.value(i).to_string());
    }

    Ok(())
}

struct MapValues<'a> {
    map: &'a MapArray,
    keys: &'a StringArray,
    values: &'a StringArray,
}

fn map_values_from_batch<'a>(
    batch: &'a RecordBatch,
    map_column: &str,
) -> Result<Option<MapValues<'a>>, MetadataScanError> {
    let Ok(index) = batch.schema().index_of(map_column) else {
        return Ok(None);
    };
    let map = batch
        .column(index)
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| MetadataScanError::Schema(format!("'{map_column}' column is not a MapArray")))?;
    let keys = map
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| MetadataScanError::Schema(format!("'{map_column}' map keys are not StringArray")))?;
    let values = map
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| MetadataScanError::Schema(format!("'{map_column}' map values are not StringArray")))?;
    Ok(Some(MapValues { map, keys, values }))
}

fn collect_map_values_for_row(
    map_values: &MapValues<'_>,
    row: usize,
    label_name: &str,
    normalize_keys: bool,
    out: &mut BTreeSet<String>,
) -> Result<bool, MetadataScanError> {
    if !map_values.map.is_valid(row) {
        return Ok(false);
    }
    let offsets = map_values.map.value_offsets();
    let start = usize::try_from(offsets[row])
        .map_err(|_| MetadataScanError::Schema("map row start offset is negative".to_string()))?;
    let end = usize::try_from(offsets[row + 1])
        .map_err(|_| MetadataScanError::Schema("map row end offset is negative".to_string()))?;

    let mut matched = false;
    for index in start..end {
        if !map_values.keys.is_valid(index) || !map_values.values.is_valid(index) {
            continue;
        }
        if stored_key_matches_label(map_values.keys.value(index), label_name, normalize_keys) {
            out.insert(map_values.values.value(index).to_string());
            matched = true;
        }
    }
    Ok(matched)
}

fn collect_coalesced_map_values_from_batch(
    batch: &RecordBatch,
    primary_config: &MetadataScanConfig,
    fallback_config: &MetadataScanConfig,
    label_name: &str,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    let primary = map_values_from_batch(batch, primary_config.map_column)?;
    let fallback = map_values_from_batch(batch, fallback_config.map_column)?;

    for row in 0..batch.num_rows() {
        let has_primary = match &primary {
            Some(values) => collect_map_values_for_row(values, row, label_name, primary_config.normalize_keys, out)?,
            None => false,
        };
        if !has_primary {
            if let Some(values) = &fallback {
                collect_map_values_for_row(values, row, label_name, fallback_config.normalize_keys, out)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, ArrayRef, MapArray, RecordBatch, StringArray, StructArray};
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
    use icegate_common::schema::{COL_LOG_ATTRIBUTES, COL_SPAN_ATTRIBUTES};

    use super::{LabelKind, classify_label, collect_map_values_from_batch, stored_key_matches_label};
    use crate::engine::metadata_scan::MetadataScanConfig;

    const LOG_CFG: MetadataScanConfig = MetadataScanConfig {
        indexed_columns: &["service_name", "severity_text", "trace_id", "span_id"],
        label_aliases: &[("level", "severity_text"), ("service", "service_name")],
        excluded_map_keys: &[],
        map_column: "attributes",
        normalize_keys: true,
    };

    #[test]
    fn classify_level_is_indexed() {
        assert_eq!(classify_label("level", &LOG_CFG), LabelKind::Indexed);
    }

    #[test]
    fn classify_indexed_columns_are_indexed() {
        assert_eq!(classify_label("service_name", &LOG_CFG), LabelKind::Indexed);
        assert_eq!(classify_label("trace_id", &LOG_CFG), LabelKind::Indexed);
        assert_eq!(classify_label("span_id", &LOG_CFG), LabelKind::Indexed);
        assert_eq!(classify_label("severity_text", &LOG_CFG), LabelKind::Indexed);
    }

    #[test]
    fn classify_map_attribute_for_non_indexed() {
        assert_eq!(classify_label("pod", &LOG_CFG), LabelKind::MapAttribute);
        assert_eq!(classify_label("namespace", &LOG_CFG), LabelKind::MapAttribute);
    }

    #[test]
    fn stored_key_matches_label_cases() {
        // (stored_key, label_name, normalize_keys, expected)
        let cases = [
            // Loki semantics: normalize_keys = true, request is wire-form.
            ("user.id", "user_id", true, true),
            // A still-dotted request never matches under normalization: Loki
            // label names cannot contain dots so no real caller sends one,
            // but the rule itself must not special-case it.
            ("user.id", "user.id", true, false),
            // No dot to replace: the allocation-free short-circuit path
            // must still compare correctly.
            ("pod", "pod", true, true),
            // Tempo/TraceQL semantics: normalize_keys = false, the dotted
            // request matches the dotted stored key exactly (AS STORED).
            ("http.method", "http.method", false, true),
            // The opt-in gate: without normalize_keys, a wire-form request
            // must not reach a dotted stored key.
            ("http.method", "http_method", false, false),
            ("user.id", "user_id", false, false),
            // Exact match never depends on the flag.
            ("pod", "pod", false, true),
        ];
        for (stored_key, label_name, normalize_keys, expected) in cases {
            assert_eq!(
                stored_key_matches_label(stored_key, label_name, normalize_keys),
                expected,
                "stored_key={stored_key:?} label_name={label_name:?} normalize_keys={normalize_keys}"
            );
        }
    }

    /// One-row MAP<Utf8,Utf8> `RecordBatch` with a single map column named
    /// `map_column`, holding `pairs` as that row's entries — mirrors
    /// ingest's shape, where entries are deduplicated only by the raw key
    /// string, never by its normalized form.
    fn batch_with_map(map_column: &str, pairs: &[(&str, &str)]) -> RecordBatch {
        let keys = StringArray::from(pairs.iter().map(|(k, _)| *k).collect::<Vec<_>>());
        let values = StringArray::from(pairs.iter().map(|(_, v)| *v).collect::<Vec<_>>());
        let entry_fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ]
        .into();
        let entries = StructArray::new(
            entry_fields.clone(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
            None,
        );
        let entry_field = Arc::new(Field::new("key_value", DataType::Struct(entry_fields), false));
        let pair_count = i32::try_from(pairs.len()).expect("test fixture pair count fits in i32");
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, pair_count]));
        let map_array = MapArray::new(entry_field, offsets, entries, None, false);

        // Derive the outer field's type from the array itself so it always
        // agrees with the nested struct/offset shape `MapArray::new` built,
        // rather than hand-duplicating that shape and risking drift.
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            map_column,
            map_array.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(map_array)]).expect("valid record batch")
    }

    #[test]
    fn dotted_stored_key_reachable_by_underscored_name_when_normalizing() {
        let batch = batch_with_map(COL_LOG_ATTRIBUTES, &[("user.id", "user-123")]);
        let mut out = BTreeSet::new();
        collect_map_values_from_batch(&batch, COL_LOG_ATTRIBUTES, "user_id", true, &mut out).expect("collect");
        assert_eq!(out, BTreeSet::from(["user-123".to_string()]));
    }

    #[test]
    fn dotted_stored_key_matches_only_as_stored_when_not_normalizing() {
        // Tempo/TraceQL semantics: attribute names are dotted and must be
        // matched exactly as stored — this is what makes the opt-out on
        // the Tempo/spans configs meaningful rather than incidental.
        let batch = batch_with_map(COL_SPAN_ATTRIBUTES, &[("http.method", "GET")]);
        let mut out = BTreeSet::new();
        collect_map_values_from_batch(&batch, COL_SPAN_ATTRIBUTES, "http.method", false, &mut out).expect("collect");
        assert_eq!(out, BTreeSet::from(["GET".to_string()]));
    }

    #[test]
    fn underscored_name_does_not_reach_dotted_key_when_not_normalizing() {
        // Proves the opt-in flag genuinely gates rather than being a no-op:
        // without normalize_keys, a wire-form request must not match a
        // dotted stored key.
        let batch = batch_with_map(COL_SPAN_ATTRIBUTES, &[("http.method", "GET")]);
        let mut out = BTreeSet::new();
        collect_map_values_from_batch(&batch, COL_SPAN_ATTRIBUTES, "http_method", false, &mut out).expect("collect");
        assert!(
            out.is_empty(),
            "wire-form request must not match a dotted key AS STORED: {out:?}"
        );
    }

    #[test]
    fn normalization_collision_unions_both_raw_keys_values() {
        // `k8s.pod.name` and `k8s_pod_name` are distinct raw keys that
        // ingest permits to coexist in one row (dedup is by the raw
        // string, never by normalized form). Both normalize to the same
        // wire name, so both values are unioned into the result — the
        // over-approximation documented on `collect_map_values_from_batch`.
        let batch = batch_with_map(
            COL_LOG_ATTRIBUTES,
            &[("k8s.pod.name", "dotted-value"), ("k8s_pod_name", "underscored-value")],
        );
        let mut out = BTreeSet::new();
        collect_map_values_from_batch(&batch, COL_LOG_ATTRIBUTES, "k8s_pod_name", true, &mut out).expect("collect");
        assert_eq!(
            out,
            BTreeSet::from(["dotted-value".to_string(), "underscored-value".to_string()])
        );
    }
}
