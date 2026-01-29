//! AWS S3 client helpers for testing with MinIO.

use std::sync::Arc;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{
    Client,
    config::{Credentials, SharedCredentialsProvider},
};
use object_store::{ObjectStore, aws::AmazonS3Builder};

/// Create an AWS S3 bucket for testing.
///
/// # Arguments
///
/// * `endpoint` - The S3-compatible endpoint URL (e.g., `MinIO` endpoint)
/// * `bucket_name` - The name of the bucket to create
///
/// # Errors
///
/// Returns an error if the bucket creation fails.
pub async fn create_s3_bucket(endpoint: &str, bucket_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Creating S3 bucket: {}", bucket_name);

    // Configure AWS SDK client for MinIO
    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "static");
    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(SharedCredentialsProvider::new(creds))
        .endpoint_url(endpoint)
        .load()
        .await;

    let client = Client::new(&shared_config);

    // Create bucket
    client.create_bucket().bucket(bucket_name).send().await?;

    tracing::info!("S3 bucket created: {}", bucket_name);
    Ok(())
}

/// Create an `ObjectStore` configured for S3-compatible storage (e.g., `MinIO`).
///
/// # Arguments
///
/// * `endpoint` - The S3-compatible endpoint URL (e.g., `MinIO` endpoint)
/// * `bucket` - The bucket name to use
///
/// # Errors
///
/// Returns an error if the `ObjectStore` cannot be built.
pub fn create_s3_object_store(
    endpoint: &str,
    bucket: &str,
) -> Result<Arc<dyn ObjectStore>, Box<dyn std::error::Error>> {
    tracing::info!("Creating S3 ObjectStore for bucket: {}", bucket);

    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_endpoint(endpoint)
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_allow_http(true)
        .build()?;

    Ok(Arc::new(store))
}
