//! Testing utilities for integration tests and benchmarks.
//!
//! This module provides reusable infrastructure for setting up test environments:
//! - MinIO containers for S3-compatible object storage
//! - AWS S3 client configuration helpers
//!
//! Available only when the `testing` feature is enabled.

/// MinIO testcontainer setup.
pub mod minio;
/// AWS S3 client helpers.
pub mod s3;

pub use minio::MinIOContainer;
pub use s3::{create_s3_bucket, create_s3_object_store};
