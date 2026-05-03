//! Record batch to Loki format conversion.
//!
//! Converts DataFusion `RecordBatch` results to Loki-compatible response
//! formats (streams for log queries, matrix for metric queries).

use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::{
    array::{
        Array, FixedSizeBinaryArray, Float64Array, Int64Array, MapArray, RecordBatch, StringArray,
        TimestampMicrosecondArray, TimestampNanosecondArray,
    },
    datatypes::Schema,
};
use icegate_common::schema::{
    COL_ATTRIBUTES, COL_BODY, COL_CLOUD_ACCOUNT_ID, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TIMESTAMP,
    COL_TRACE_ID, LEVEL_ALIAS, LOG_INDEXED_ATTRIBUTE_COLUMNS, LOG_SERIES_LABEL_COLUMNS,
};

use super::models::{MetricSeries, QueryResult, QueryStats, ResultType, Stream};

// ============================================================================
// Formatting Result
// ============================================================================

/// Result of formatting record batches.
pub struct FormattedResult {
    /// The formatted result.
    pub result: QueryResult,
    /// Result type.
    pub result_type: ResultType,
    /// Bytes processed.
    pub total_bytes: usize,
    /// Lines/samples processed.
    pub total_lines: usize,
    /// Number of batches.
    pub num_batches: usize,
}

impl FormattedResult {
    /// Create query stats from formatting result.
    ///
    /// When `source` is provided, the stats include a per-source breakdown
    /// (Iceberg vs WAL). Otherwise, falls back to the aggregate-only format.
    pub fn to_stats(&self, exec_time: f64, source: Option<&crate::engine::SourceMetrics>) -> QueryStats {
        source.map_or_else(
            || QueryStats::from_metrics(self.total_bytes, self.total_lines, self.num_batches, exec_time),
            |s| QueryStats::from_source_metrics(s, self.total_bytes, self.total_lines, self.num_batches, exec_time),
        )
    }
}

// ============================================================================
// String Interning
// ============================================================================

/// Simple string interner for deduplicating label strings.
///
/// Reduces allocations by reusing `Arc<str>` for repeated label names/values
/// (e.g., `service_name`, `level` appear on every row).
pub struct StringInterner {
    strings: HashMap<String, Arc<str>>,
}

impl StringInterner {
    /// Create a new interner with pre-allocated capacity for typical label
    /// sets.
    pub fn new() -> Self {
        Self {
            strings: HashMap::with_capacity(256),
        }
    }

    /// Intern a string, returning an `Arc<str>` that can be cheaply cloned.
    ///
    /// If the string was seen before, returns the existing `Arc`.
    /// Otherwise, creates a new `Arc` and stores it for future lookups.
    pub fn intern(&mut self, s: &str) -> Arc<str> {
        if let Some(interned) = self.strings.get(s) {
            Arc::clone(interned)
        } else {
            let arc: Arc<str> = Arc::from(s);
            self.strings.insert(s.to_string(), Arc::clone(&arc));
            arc
        }
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Column Indices
// ============================================================================

/// Column indices cached once per batch for efficient row processing.
pub struct BatchColumns {
    pub timestamp: Option<usize>,
    pub body: Option<usize>,
    pub cloud_account_id: Option<usize>,
    pub service_name: Option<usize>,
    pub severity_text: Option<usize>,
    pub level: Option<usize>,
    pub trace_id: Option<usize>,
    pub span_id: Option<usize>,
    pub attributes: Option<usize>,
}

impl BatchColumns {
    /// Extract column indices from schema.
    pub fn from_schema(schema: &Schema) -> Self {
        Self {
            timestamp: schema.index_of(COL_TIMESTAMP).ok(),
            body: schema.index_of(COL_BODY).ok(),
            cloud_account_id: schema.index_of(COL_CLOUD_ACCOUNT_ID).ok(),
            service_name: schema.index_of(COL_SERVICE_NAME).ok(),
            severity_text: schema.index_of(COL_SEVERITY_TEXT).ok(),
            level: schema.index_of(LEVEL_ALIAS).ok(),
            trace_id: schema.index_of(COL_TRACE_ID).ok(),
            span_id: schema.index_of(COL_SPAN_ID).ok(),
            attributes: schema.index_of(COL_ATTRIBUTES).ok(),
        }
    }
}

/// Column indices for metric query batches (matrix format).
///
/// Metric queries return `time_bucket`, `value`, and optional grouping label
/// columns.
pub struct MetricBatchColumns {
    pub timestamp: Option<usize>,
    pub value: Option<usize>,
    pub attributes: Option<usize>,
}

impl MetricBatchColumns {
    /// Extract column indices from schema.
    pub fn from_schema(schema: &Schema) -> Self {
        Self {
            timestamp: schema.index_of(COL_TIMESTAMP).ok(),
            value: schema.index_of("value").ok(),
            attributes: schema.index_of(COL_ATTRIBUTES).ok(),
        }
    }

    /// Returns indices of predefined indexed label columns that exist in the
    /// schema.
    pub fn label_indices(schema: &Schema) -> Vec<(String, usize)> {
        LOG_INDEXED_ATTRIBUTE_COLUMNS
            .iter()
            .filter_map(|&name| schema.index_of(name).ok().map(|idx| (name.to_string(), idx)))
            .collect()
    }
}

// ============================================================================
// Value Extraction Helpers
// ============================================================================

/// Extract timestamp from row (microseconds -> nanoseconds).
fn extract_timestamp(batch: &RecordBatch, cols: &BatchColumns, row: usize) -> i64 {
    let Some(idx) = cols.timestamp else { return 0 };
    let arr = batch.column(idx).as_any();
    if let Some(arr) = arr.downcast_ref::<TimestampMicrosecondArray>() {
        return if arr.is_null(row) { 0 } else { arr.value(row) * 1000 };
    }
    if let Some(arr) = arr.downcast_ref::<TimestampNanosecondArray>() {
        return if arr.is_null(row) { 0 } else { arr.value(row) };
    }
    0
}

/// Extract body string from row.
fn extract_body(batch: &RecordBatch, cols: &BatchColumns, row: usize) -> String {
    cols.body
        .and_then(|idx| {
            batch.column(idx).as_any().downcast_ref::<StringArray>().and_then(|arr| {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row).to_string())
                }
            })
        })
        .unwrap_or_default()
}

/// Read a single row from a `FixedSizeBinary` column, returning `None` for
/// nulls or for columns that cannot be downcast to `FixedSizeBinaryArray`.
fn extract_fixed_bytes(batch: &RecordBatch, idx: usize, row: usize) -> Option<&[u8]> {
    let arr = batch.column(idx).as_any().downcast_ref::<FixedSizeBinaryArray>()?;
    if arr.is_null(row) { None } else { Some(arr.value(row)) }
}

/// Extract labels from indexed columns and `attributes` column.
///
/// Uses string interning to deduplicate repeated label names/values across
/// rows.
fn extract_labels(
    batch: &RecordBatch,
    cols: &BatchColumns,
    row: usize,
    interner: &mut StringInterner,
) -> HashMap<Arc<str>, Arc<str>> {
    let mut labels: HashMap<Arc<str>, Arc<str>> = HashMap::with_capacity(12);

    let extract_string = |idx: usize| -> Option<&str> {
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .and_then(|arr| if arr.is_null(row) { None } else { Some(arr.value(row)) })
    };

    if let Some(idx) = cols.cloud_account_id {
        if let Some(val) = extract_string(idx) {
            labels.insert(interner.intern(COL_CLOUD_ACCOUNT_ID), interner.intern(val));
        }
    }

    if let Some(idx) = cols.service_name {
        if let Some(val) = extract_string(idx) {
            labels.insert(interner.intern(COL_SERVICE_NAME), interner.intern(val));
        }
    }

    if let Some(idx) = cols.severity_text {
        if let Some(val) = extract_string(idx) {
            // `severity_text` is the canonical OTLP field; mirror it as
            // `level` so Grafana's log panel — which colours and filters
            // by the `level` label — works without the LogQL series
            // planner having to alias the column. The series planner
            // does alias `severity_text` → `level` for its DataFrame
            // output, so the second insert below is idempotent on that
            // path; for `plan_log` (raw entries) it's the only place
            // `level` is emitted.
            let severity_value = interner.intern(val);
            labels.insert(interner.intern(COL_SEVERITY_TEXT), Arc::clone(&severity_value));
            labels.insert(interner.intern(LEVEL_ALIAS), severity_value);
        }
    }

    if let Some(idx) = cols.level {
        if let Some(val) = extract_string(idx) {
            labels.insert(interner.intern(LEVEL_ALIAS), interner.intern(val));
        }
    }

    // trace_id / span_id are stored as raw FIXED_LEN_BYTE_ARRAY (16 / 8 bytes).
    // Hex-encode at materialisation time so Loki's wire format keeps emitting
    // the canonical lowercase-hex W3C representation.
    if let Some(idx) = cols.trace_id {
        if let Some(val) = extract_fixed_bytes(batch, idx, row) {
            labels.insert(interner.intern(COL_TRACE_ID), interner.intern(&hex::encode(val)));
        }
    }

    if let Some(idx) = cols.span_id {
        if let Some(val) = extract_fixed_bytes(batch, idx, row) {
            labels.insert(interner.intern(COL_SPAN_ID), interner.intern(&hex::encode(val)));
        }
    }

    if let Some(idx) = cols.attributes {
        extract_attributes_map(batch, idx, row, &mut labels, interner);
    }

    labels
}

/// Extract key-value pairs from attributes `MapArray` column.
fn extract_attributes_map(
    batch: &RecordBatch,
    attr_idx: usize,
    row: usize,
    labels: &mut HashMap<Arc<str>, Arc<str>>,
    interner: &mut StringInterner,
) {
    if let Some(map_arr) = batch.column(attr_idx).as_any().downcast_ref::<MapArray>() {
        if !map_arr.is_null(row) {
            let offsets = map_arr.offsets();
            #[allow(clippy::cast_sign_loss)]
            let start = offsets[row] as usize;
            #[allow(clippy::cast_sign_loss)]
            let end = offsets[row + 1] as usize;

            let keys = map_arr.keys().as_any().downcast_ref::<StringArray>();
            let values = map_arr.values().as_any().downcast_ref::<StringArray>();

            if let (Some(keys), Some(values)) = (keys, values) {
                for i in start..end {
                    if !keys.is_null(i) && !values.is_null(i) {
                        labels.insert(interner.intern(keys.value(i)), interner.intern(values.value(i)));
                    }
                }
            }
        }
    }
}

/// Create a deterministic sorted key for stream grouping.
fn make_stream_key_into(labels: &HashMap<Arc<str>, Arc<str>>, buffer: &mut String) {
    buffer.clear();
    let mut label_pairs: Vec<_> = labels.iter().collect();
    label_pairs.sort_by(|(k1, _), (k2, _)| k1.as_ref().cmp(k2.as_ref()));
    for (i, (k, v)) in label_pairs.iter().enumerate() {
        if i > 0 {
            buffer.push(',');
        }
        buffer.push_str(k);
        buffer.push('=');
        buffer.push_str(v);
    }
}

// ============================================================================
// Streams Format (Log Queries)
// ============================================================================

/// Converts `DataFusion` `RecordBatches` to Loki streams format.
///
/// Groups log entries by their labels and returns formatted streams.
#[tracing::instrument(level = "debug", fields(batches = batches.len()))]
pub fn batches_to_loki_streams(batches: &[RecordBatch]) -> FormattedResult {
    let mut streams: HashMap<String, (HashMap<Arc<str>, Arc<str>>, Vec<(String, String)>)> = HashMap::with_capacity(64);
    let mut interner = StringInterner::new();
    let mut key_buffer = String::with_capacity(256);
    let mut total_bytes: usize = 0;
    let mut total_lines: usize = 0;

    for batch in batches {
        let cols = BatchColumns::from_schema(batch.schema().as_ref());

        for row in 0..batch.num_rows() {
            let timestamp_nanos = extract_timestamp(batch, &cols, row);
            let body = extract_body(batch, &cols, row);
            let labels = extract_labels(batch, &cols, row, &mut interner);

            make_stream_key_into(&labels, &mut key_buffer);

            total_bytes += body.len();
            total_lines += 1;

            let ts_str = itoa::Buffer::new().format(timestamp_nanos).to_string();

            streams
                .entry(key_buffer.clone())
                .or_insert_with(|| (labels, Vec::new()))
                .1
                .push((ts_str, body));
        }
    }

    let result: Vec<Stream> = streams
        .into_values()
        .map(|(labels, values)| {
            let stream: HashMap<String, String> =
                labels.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
            Stream { stream, values }
        })
        .collect();

    FormattedResult {
        result: QueryResult::Streams(result),
        result_type: ResultType::Streams,
        total_bytes,
        total_lines,
        num_batches: batches.len(),
    }
}

// ============================================================================
// Matrix Format (Metric Queries)
// ============================================================================

/// Extract `time_bucket` from row (microseconds → seconds).
#[allow(clippy::cast_precision_loss)]
fn extract_time_bucket_secs(batch: &RecordBatch, cols: &MetricBatchColumns, row: usize) -> i64 {
    let Some(idx) = cols.timestamp else { return 0i64 };
    let arr = batch.column(idx).as_any();
    if let Some(arr) = arr.downcast_ref::<TimestampMicrosecondArray>() {
        return if arr.is_null(row) {
            0i64
        } else {
            arr.value(row) / 1_000_000i64
        };
    }
    if let Some(arr) = arr.downcast_ref::<TimestampNanosecondArray>() {
        return if arr.is_null(row) {
            0i64
        } else {
            arr.value(row) / 1_000_000_000i64
        };
    }
    0i64
}

/// Extract metric value as string (handles Float64, Int64).
fn extract_metric_value(batch: &RecordBatch, cols: &MetricBatchColumns, row: usize) -> String {
    cols.value.map_or_else(
        || "0".to_string(),
        |idx| {
            let col = batch.column(idx);
            if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                if !arr.is_null(row) {
                    return arr.value(row).to_string();
                }
            }
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                if !arr.is_null(row) {
                    return arr.value(row).to_string();
                }
            }
            "0".to_string()
        },
    )
}

/// Maps `OTel` column names to Loki label names for output.
fn map_column_to_label(name: &str) -> &str {
    match name {
        COL_SEVERITY_TEXT => LEVEL_ALIAS,
        COL_SERVICE_NAME => "service",
        _ => name,
    }
}

/// Extract labels from indexed columns and attributes map for metric queries.
fn extract_metric_labels(
    batch: &RecordBatch,
    label_cols: &[(String, usize)],
    attributes_idx: Option<usize>,
    row: usize,
    interner: &mut StringInterner,
) -> HashMap<Arc<str>, Arc<str>> {
    let mut labels = HashMap::with_capacity(label_cols.len() + 8);

    for (col_name, idx) in label_cols {
        let column = batch.column(*idx);
        // Most indexed labels are Utf8; trace_id / span_id / parent_span_id are
        // FixedSizeBinary and need hex-encoding for the wire format.
        if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
            if !arr.is_null(row) {
                let label_name = map_column_to_label(col_name);
                labels.insert(interner.intern(label_name), interner.intern(arr.value(row)));
            }
        } else if let Some(arr) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
            if !arr.is_null(row) {
                let label_name = map_column_to_label(col_name);
                labels.insert(
                    interner.intern(label_name),
                    interner.intern(&hex::encode(arr.value(row))),
                );
            }
        }
    }

    if let Some(attr_idx) = attributes_idx {
        extract_attributes_map(batch, attr_idx, row, &mut labels, interner);
    }

    labels
}

/// Converts `DataFusion` `RecordBatches` to Loki matrix format.
///
/// Groups metric samples by their labels and returns formatted matrix.
#[tracing::instrument(level = "debug", fields(batches = batches.len()))]
pub fn batches_to_loki_matrix(batches: &[RecordBatch]) -> FormattedResult {
    let mut series: HashMap<String, (HashMap<Arc<str>, Arc<str>>, Vec<(i64, String)>)> = HashMap::with_capacity(64);
    let mut interner = StringInterner::new();
    let mut key_buffer = String::with_capacity(256);
    let mut total_samples: usize = 0;

    for batch in batches {
        let cols = MetricBatchColumns::from_schema(batch.schema().as_ref());
        let label_cols = MetricBatchColumns::label_indices(batch.schema().as_ref());

        for row in 0..batch.num_rows() {
            let timestamp_secs = extract_time_bucket_secs(batch, &cols, row);
            let value = extract_metric_value(batch, &cols, row);
            let labels = extract_metric_labels(batch, &label_cols, cols.attributes, row, &mut interner);

            make_stream_key_into(&labels, &mut key_buffer);
            total_samples += 1;

            series
                .entry(key_buffer.clone())
                .or_insert_with(|| (labels, Vec::new()))
                .1
                .push((timestamp_secs, value));
        }
    }

    let result: Vec<MetricSeries> = series
        .into_values()
        .map(|(labels, values)| {
            let metric: HashMap<String, String> =
                labels.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
            MetricSeries { metric, values }
        })
        .collect();

    FormattedResult {
        result: QueryResult::Matrix(result),
        result_type: ResultType::Matrix,
        total_bytes: total_samples * 16, // approximate
        total_lines: total_samples,
        num_batches: batches.len(),
    }
}

// ============================================================================
// Series Format (Metadata Queries)
// ============================================================================

/// High-cardinality attribute keys excluded from series label output.
const SERIES_EXCLUDED_ATTR_KEYS: &[&str] = &[COL_TRACE_ID, COL_SPAN_ID];

/// Convert record batches from `plan_series` to a series list (array of label
/// maps).
///
/// The input batches contain indexed columns, `level`, and an `attributes`
/// MAP column (via `first_value`). High-cardinality keys (`trace_id`,
/// `span_id`) are filtered out during extraction.
pub fn batches_to_series_list(batches: &[RecordBatch]) -> Vec<HashMap<String, String>> {
    let mut series_list = Vec::new();
    let mut interner = StringInterner::new();

    for batch in batches {
        let schema = batch.schema();

        // Resolve column indices once per batch
        let indexed_indices: Vec<(&str, usize)> = LOG_SERIES_LABEL_COLUMNS
            .iter()
            .chain(std::iter::once(&LEVEL_ALIAS))
            .filter_map(|&name| schema.index_of(name).ok().map(|idx| (name, idx)))
            .collect();
        let attr_idx = schema.index_of(COL_ATTRIBUTES).ok();

        for row in 0..batch.num_rows() {
            let mut label_map = HashMap::with_capacity(indexed_indices.len() + 8);

            // Extract indexed columns
            for &(name, idx) in &indexed_indices {
                if let Some(arr) = batch.column(idx).as_any().downcast_ref::<StringArray>() {
                    if !arr.is_null(row) {
                        let val = arr.value(row);
                        if !val.is_empty() {
                            label_map.insert(name.to_string(), val.to_string());
                        }
                    }
                }
            }

            // Extract attributes from MAP, skipping excluded keys
            if let Some(idx) = attr_idx {
                extract_series_attributes(batch, idx, row, &mut label_map, &mut interner);
            }

            series_list.push(label_map);
        }
    }

    series_list
}

/// Extract key-value pairs from the `attributes` `MapArray`, skipping
/// high-cardinality keys defined in [`SERIES_EXCLUDED_ATTR_KEYS`].
fn extract_series_attributes(
    batch: &RecordBatch,
    attr_idx: usize,
    row: usize,
    labels: &mut HashMap<String, String>,
    interner: &mut StringInterner,
) {
    if let Some(map_arr) = batch.column(attr_idx).as_any().downcast_ref::<MapArray>() {
        if !map_arr.is_null(row) {
            let offsets = map_arr.offsets();
            #[allow(clippy::cast_sign_loss)]
            let start = offsets[row] as usize;
            #[allow(clippy::cast_sign_loss)]
            let end = offsets[row + 1] as usize;

            let keys = map_arr.keys().as_any().downcast_ref::<StringArray>();
            let values = map_arr.values().as_any().downcast_ref::<StringArray>();

            if let (Some(keys), Some(values)) = (keys, values) {
                for i in start..end {
                    if !keys.is_null(i) && !values.is_null(i) {
                        let k = keys.value(i);
                        if SERIES_EXCLUDED_ATTR_KEYS.contains(&k) {
                            continue;
                        }
                        // Use interner for deduplication, then convert to owned
                        // String for the output HashMap.
                        let _ = interner.intern(k);
                        labels.insert(k.to_string(), values.value(i).to_string());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::{
        array::{ArrayRef, FixedSizeBinaryArray, MapBuilder, MapFieldNames, StringArray, StringBuilder},
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    /// Build a single-row `RecordBatch` whose schema matches the projection
    /// the `LogQL` planner emits for the formatter, with a MAP that does NOT
    /// mirror the indexed columns (the post-change ingest behaviour).
    fn build_row_batch(
        cloud_account: &str,
        service: &str,
        severity: &str,
        trace_id_hex: &str,
        span_id_hex: &str,
        map_pairs: &[(&str, &str)],
    ) -> RecordBatch {
        let map_field_names = MapFieldNames {
            entry: "entries".to_string(),
            key: "keys".to_string(),
            value: "values".to_string(),
        };
        let mut map_builder = MapBuilder::new(Some(map_field_names), StringBuilder::new(), StringBuilder::new())
            .with_keys_field(Arc::new(Field::new("keys", DataType::Utf8, false)))
            .with_values_field(Arc::new(Field::new("values", DataType::Utf8, true)));
        for (k, v) in map_pairs {
            map_builder.keys().append_value(*k);
            map_builder.values().append_value(*v);
        }
        map_builder.append(true).expect("append map row");
        let attributes_array = map_builder.finish();
        let attributes_field = Field::new(COL_ATTRIBUTES, attributes_array.data_type().clone(), true);

        let cloud_arr: ArrayRef = Arc::new(StringArray::from(vec![cloud_account]));
        let service_arr: ArrayRef = Arc::new(StringArray::from(vec![service]));
        let severity_arr: ArrayRef = Arc::new(StringArray::from(vec![severity]));
        // `level` is the alias the planner adds via `col(severity_text).alias("level")`.
        let level_arr: ArrayRef = Arc::new(StringArray::from(vec![severity]));
        // `trace_id` / `span_id` flow through DataFusion as raw FixedSizeBinary;
        // hex strings only exist at the API boundary.
        let trace_bytes = hex::decode(trace_id_hex).expect("trace_id_hex");
        let span_bytes = hex::decode(span_id_hex).expect("span_id_hex");
        let trace_arr: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter(std::iter::once(trace_bytes.as_slice()))
                .expect("trace FixedSizeBinaryArray"),
        );
        let span_arr: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter(std::iter::once(span_bytes.as_slice()))
                .expect("span FixedSizeBinaryArray"),
        );
        let attrs_arr: ArrayRef = Arc::new(attributes_array);

        let schema = Arc::new(Schema::new(vec![
            Field::new(COL_CLOUD_ACCOUNT_ID, DataType::Utf8, true),
            Field::new(COL_SERVICE_NAME, DataType::Utf8, true),
            Field::new(COL_SEVERITY_TEXT, DataType::Utf8, true),
            Field::new(LEVEL_ALIAS, DataType::Utf8, true),
            Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), true),
            Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), true),
            attributes_field,
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                cloud_arr,
                service_arr,
                severity_arr,
                level_arr,
                trace_arr,
                span_arr,
                attrs_arr,
            ],
        )
        .expect("RecordBatch::try_new")
    }

    /// After dropping the write-time duplication, the indexed-column values
    /// reach the Loki output exclusively through the typed top-level columns.
    /// The formatter is the row-materialisation point that merges them into
    /// the labels map; this test pins that contract.
    #[test]
    fn extract_labels_merges_indexed_columns_when_map_lacks_them() {
        let batch = build_row_batch(
            "acc-1",
            "svc",
            "ERROR",
            "0102030405060708090a0b0c0d0e0f10",
            "1112131415161718",
            &[("foo", "bar")],
        );

        let cols = BatchColumns::from_schema(batch.schema().as_ref());
        let mut interner = StringInterner::new();
        let labels = extract_labels(&batch, &cols, 0, &mut interner);

        // Indexed top-level columns are projected into the labels map.
        assert_eq!(
            labels.get(&Arc::<str>::from(COL_CLOUD_ACCOUNT_ID)).map(AsRef::as_ref),
            Some("acc-1"),
        );
        assert_eq!(
            labels.get(&Arc::<str>::from(COL_SERVICE_NAME)).map(AsRef::as_ref),
            Some("svc"),
        );
        assert_eq!(
            labels.get(&Arc::<str>::from(COL_SEVERITY_TEXT)).map(AsRef::as_ref),
            Some("ERROR"),
        );
        assert_eq!(
            labels.get(&Arc::<str>::from(LEVEL_ALIAS)).map(AsRef::as_ref),
            Some("ERROR")
        );
        assert_eq!(
            labels.get(&Arc::<str>::from(COL_TRACE_ID)).map(AsRef::as_ref),
            Some("0102030405060708090a0b0c0d0e0f10"),
        );
        assert_eq!(
            labels.get(&Arc::<str>::from(COL_SPAN_ID)).map(AsRef::as_ref),
            Some("1112131415161718"),
        );

        // User attribute from the MAP is also surfaced.
        assert_eq!(labels.get(&Arc::<str>::from("foo")).map(AsRef::as_ref), Some("bar"));

        // Sanity: only the seven keys above (six indexed + one user attr).
        assert_eq!(labels.len(), 7, "got labels: {labels:?}");
    }
}
