//! Record batch to Loki format conversion.
//!
//! Converts DataFusion `RecordBatch` results to Loki-compatible response
//! formats (streams for log queries, matrix for metric queries).

use std::{collections::HashMap, sync::Arc};

use datafusion::arrow::{
    array::{
        Array, Float64Array, Int64Array, MapArray, RecordBatch, StringArray, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    },
    datatypes::Schema,
};
use icegate_common::schema::LOG_INDEXED_ATTRIBUTE_COLUMNS;

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
    pub fn to_stats(&self, exec_time: f64) -> QueryStats {
        QueryStats::from_metrics(self.total_bytes, self.total_lines, self.num_batches, exec_time)
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
            timestamp: schema.index_of("timestamp").ok(),
            body: schema.index_of("body").ok(),
            cloud_account_id: schema.index_of("cloud_account_id").ok(),
            service_name: schema.index_of("service_name").ok(),
            severity_text: schema.index_of("severity_text").ok(),
            level: schema.index_of("level").ok(),
            trace_id: schema.index_of("trace_id").ok(),
            span_id: schema.index_of("span_id").ok(),
            attributes: schema.index_of("attributes").ok(),
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
            timestamp: schema.index_of("timestamp").ok(),
            value: schema.index_of("value").ok(),
            attributes: schema.index_of("attributes").ok(),
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
        batch.column(idx).as_any().downcast_ref::<StringArray>().and_then(|arr| {
            if arr.is_null(row) {
                None
            } else {
                Some(arr.value(row))
            }
        })
    };

    if let Some(idx) = cols.cloud_account_id {
        if let Some(val) = extract_string(idx) {
            labels.insert(interner.intern("cloud_account_id"), interner.intern(val));
        }
    }

    if let Some(idx) = cols.service_name {
        if let Some(val) = extract_string(idx) {
            labels.insert(interner.intern("service_name"), interner.intern(val));
        }
    }

    if let Some(idx) = cols.severity_text {
        if let Some(val) = extract_string(idx) {
            labels.insert(interner.intern("severity_text"), interner.intern(val));
        }
    }

    if let Some(idx) = cols.level {
        if let Some(val) = extract_string(idx) {
            labels.insert(interner.intern("level"), interner.intern(val));
        }
    }

    if let Some(idx) = cols.trace_id {
        if let Some(val) = extract_string(idx) {
            labels.insert(interner.intern("trace_id"), interner.intern(val));
        }
    }

    if let Some(idx) = cols.span_id {
        if let Some(val) = extract_string(idx) {
            labels.insert(interner.intern("span_id"), interner.intern(val));
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

            streams.entry(key_buffer.clone()).or_insert_with(|| (labels, Vec::new())).1.push((ts_str, body));
        }
    }

    let result: Vec<Stream> = streams
        .into_values()
        .map(|(labels, values)| {
            let stream: HashMap<String, String> =
                labels.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
            Stream {
                stream,
                values,
            }
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
        return if arr.is_null(row) { 0i64 } else { arr.value(row) / 1_000_000i64 };
    }
    if let Some(arr) = arr.downcast_ref::<TimestampNanosecondArray>() {
        return if arr.is_null(row) { 0i64 } else { arr.value(row) / 1_000_000_000i64 };
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
        "severity_text" => "level",
        "service_name" => "service",
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
        if let Some(arr) = batch.column(*idx).as_any().downcast_ref::<StringArray>() {
            if !arr.is_null(row) {
                let label_name = map_column_to_label(col_name);
                labels.insert(interner.intern(label_name), interner.intern(arr.value(row)));
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
            MetricSeries {
                metric,
                values,
            }
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

/// Convert record batches to series list (array of label maps).
///
/// Each series is a map of label name -> value for all indexed columns
/// and attributes.
pub fn batches_to_series_list(batches: &[RecordBatch]) -> Vec<HashMap<String, String>> {
    let mut series_list = Vec::new();
    let mut interner = StringInterner::new();

    for batch in batches {
        let cols = BatchColumns::from_schema(batch.schema().as_ref());

        for row in 0..batch.num_rows() {
            let labels = extract_labels(batch, &cols, row, &mut interner);
            let label_map: HashMap<String, String> =
                labels.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
            series_list.push(label_map);
        }
    }

    series_list
}

/// Extract string values from a column in record batches.
pub fn extract_string_column(batches: &[RecordBatch], col_idx: usize) -> Vec<String> {
    let mut values = Vec::new();
    for batch in batches {
        if let Some(arr) = batch.column(col_idx).as_any().downcast_ref::<StringArray>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    values.push(arr.value(i).to_string());
                }
            }
        }
    }
    values
}
