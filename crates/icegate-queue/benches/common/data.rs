//! Queue-specific data generation for benchmarks.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// Generate a single `RecordBatch` with queue test data.
///
/// Schema: `tenant_id` (Utf8), `timestamp` (Timestamp), `value` (Float64), `message` (Utf8)
///
/// # Arguments
///
/// * `rows` - Number of rows to generate
/// * `tenant_cardinality` - Number of unique tenant IDs (for testing grouping)
///
/// # Returns
///
/// A `RecordBatch` with the specified number of rows.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_wrap)]
pub fn generate_batch(rows: usize, tenant_cardinality: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new("message", DataType::Utf8, false),
    ]));

    // Generate tenant IDs with configured cardinality
    let tenant_ids: Vec<String> = (0..rows).map(|i| format!("tenant-{}", i % tenant_cardinality)).collect();

    // Generate timestamps (increasing by 1ms per row)
    let base_timestamp = 1_700_000_000_000_000_000_i64; // 2023-11-14
    let timestamps: Vec<i64> = (0..rows).map(|i| base_timestamp + (i as i64 * 1_000_000)).collect();

    // Generate semi-random float values
    let values: Vec<f64> = (0..rows).map(|i| (i % 1000) as f64 * 0.123).collect();

    // Generate semi-compressible messages (realistic for Parquet encoding)
    let messages: Vec<String> = (0..rows)
        .map(|i| {
            format!(
                "Sample message {} with some common text for compression testing",
                i % 100
            )
        })
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(tenant_ids)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
            Arc::new(StringArray::from(messages)) as ArrayRef,
        ],
    )
    .expect("Failed to create RecordBatch")
}

/// Generate multiple batches for throughput testing.
///
/// # Arguments
///
/// * `count` - Number of batches to generate
/// * `rows_per_batch` - Rows in each batch
///
/// # Returns
///
/// A vector of `RecordBatch`es with low tenant cardinality (5 tenants).
pub fn generate_batches_for_throughput(count: usize, rows_per_batch: usize) -> Vec<RecordBatch> {
    (0..count).map(|_| generate_batch(rows_per_batch, 5)).collect()
}

/// Generate batches with configurable tenant cardinality for grouping tests.
///
/// # Arguments
///
/// * `count` - Number of batches to generate
/// * `rows_per_batch` - Rows in each batch
/// * `tenant_cardinality` - Number of unique tenant IDs
///
/// # Returns
///
/// A vector of `RecordBatch`es with the specified tenant cardinality.
pub fn generate_batches_with_grouping(
    count: usize,
    rows_per_batch: usize,
    tenant_cardinality: usize,
) -> Vec<RecordBatch> {
    (0..count).map(|_| generate_batch(rows_per_batch, tenant_cardinality)).collect()
}
