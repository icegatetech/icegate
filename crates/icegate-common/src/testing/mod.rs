//! Testing utilities for integration tests and benchmarks.
//!
//! This module provides reusable infrastructure for setting up test environments:
//! - Object-storage containers for S3-compatible object storage
//! - AWS S3 client configuration helpers
//!
//! Available only when the `testing` feature is enabled.

/// Object-storage testcontainer setup.
pub mod object_store;
/// AWS S3 client helpers.
pub mod s3;

pub use object_store::S3TestContainer;
pub use s3::{create_s3_bucket, create_s3_object_store};
