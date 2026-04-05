//! Shared test utilities for icegate-queue integration tests.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use icegate_common::testing::{MinIOContainer, create_s3_bucket, create_s3_object_store};
use icegate_common::{RowGroupBoundaryKey, RowGroupBoundaryRange};
use icegate_queue::PreparedWalRowGroup;
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
    use arrow::array::{Float64Array, TimestampNanosecondArray};

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

pub fn prepared_row_groups(batches: Vec<RecordBatch>) -> Vec<PreparedWalRowGroup> {
    batches.into_iter().map(PreparedWalRowGroup::new).collect()
}

#[allow(dead_code)]
pub fn logs_batch(rows: Vec<(Option<&str>, Option<&str>, Option<i64>, i64)>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("cloud_account_id", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(cloud_account_id, _, _, _)| *cloud_account_id)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                rows.iter().map(|(_, service_name, _, _)| *service_name).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|(_, _, timestamp, _)| *timestamp).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, _, _, value)| *value).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("logs batch")
}

#[allow(dead_code)]
pub fn logs_row_group_boundary_range(batch: &RecordBatch) -> RowGroupBoundaryRange {
    assert!(
        batch.num_rows() > 0,
        "logs_row_group_boundary_range requires a non-empty batch"
    );

    let cloud_account_id = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cloud_account_id");
    let service_name = batch.column(1).as_any().downcast_ref::<StringArray>().expect("service_name");
    let timestamp = batch
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("timestamp");

    let first_row_idx = 0usize;
    let last_row_idx = batch.num_rows() - 1;
    RowGroupBoundaryRange {
        min_key: RowGroupBoundaryKey {
            cloud_account_id: (!cloud_account_id.is_null(first_row_idx))
                .then(|| cloud_account_id.value(first_row_idx).to_string()),
            service_name: (!service_name.is_null(first_row_idx)).then(|| service_name.value(first_row_idx).to_string()),
            timestamp_micros: (!timestamp.is_null(first_row_idx)).then(|| timestamp.value(first_row_idx)),
        },
        max_key: RowGroupBoundaryKey {
            cloud_account_id: (!cloud_account_id.is_null(last_row_idx))
                .then(|| cloud_account_id.value(last_row_idx).to_string()),
            service_name: (!service_name.is_null(last_row_idx)).then(|| service_name.value(last_row_idx).to_string()),
            timestamp_micros: (!timestamp.is_null(last_row_idx)).then(|| timestamp.value(last_row_idx)),
        },
    }
}

#[allow(dead_code)]
pub fn prepared_logs_row_groups(batches: Vec<RecordBatch>) -> Vec<PreparedWalRowGroup> {
    batches
        .into_iter()
        .map(|batch| {
            let metadata =
                serde_json::to_string(&logs_row_group_boundary_range(&batch)).expect("serialize boundary range");
            PreparedWalRowGroup::new(batch).with_metadata(metadata)
        })
        .collect()
}
