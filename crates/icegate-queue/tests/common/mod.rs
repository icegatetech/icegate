//! Shared test utilities for icegate-queue integration tests.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use icegate_common::testing::{MinIOContainer, create_s3_bucket, create_s3_object_store};
use object_store::ObjectStore;
use uuid::Uuid;

/// Sets up `MinIO` container, creates a unique S3 bucket, and returns an `ObjectStore`.
///
/// # Returns
///
/// A tuple of (`MinIOContainer`, `ObjectStore`, `bucket_name`).
/// The container and bucket are automatically cleaned up when dropped.
///
/// # Errors
///
/// Returns an error if `MinIO` container start fails, bucket creation fails, or object store creation fails.
pub async fn setup_queue_test() -> Result<(MinIOContainer, Arc<dyn ObjectStore>, String), Box<dyn std::error::Error>> {
    let minio = MinIOContainer::start().await?;
    let bucket = format!("test-{}", Uuid::new_v4());
    create_s3_bucket(minio.endpoint(), &bucket).await?;
    let store = create_s3_object_store(minio.endpoint(), &bucket)?;
    Ok((minio, store, bucket))
}

/// Generate test batch using the benchmark data generation utilities.
///
/// # Arguments
///
/// * `rows` - Number of rows to generate
/// * `tenant_cardinality` - Number of unique tenant IDs
///
/// # Errors
///
/// Returns an error if `RecordBatch` creation fails.
pub fn test_batch(rows: usize, tenant_cardinality: usize) -> Result<RecordBatch, arrow::error::ArrowError> {
    // Import from benches/common/data.rs module
    use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new("message", DataType::Utf8, false),
    ]));

    #[allow(clippy::cast_precision_loss, clippy::cast_possible_wrap)]
    let tenant_ids: Vec<String> = (0..rows).map(|i| format!("tenant-{}", i % tenant_cardinality)).collect();

    let base_timestamp = 1_700_000_000_000_000_000_i64;
    let timestamps: Vec<i64> = (0..rows)
        .map(|i| {
            #[allow(clippy::cast_possible_wrap)]
            let offset = i as i64 * 1_000_000;
            base_timestamp + offset
        })
        .collect();

    #[allow(clippy::cast_precision_loss)]
    let values: Vec<f64> = (0..rows).map(|i| (i % 1000) as f64 * 0.123).collect();

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
}
