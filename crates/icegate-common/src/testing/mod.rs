//! Testing utilities for integration tests and benchmarks.
//!
//! This module provides reusable infrastructure for setting up test environments:
//! - Object-storage containers for S3-compatible object storage
//! - AWS S3 client configuration helpers
//! - Draining the server task a test or benchmark harness spawned
//!
//! Split across two features so a consumer pays only for what it uses: the
//! object-storage helpers need `testing`, the harness helpers only
//! `test-harness`.

/// Object-storage testcontainer setup.
#[cfg(feature = "testing")]
pub mod container;
/// AWS S3 client helpers.
#[cfg(feature = "testing")]
pub mod s3;
/// Draining a harness-spawned server task.
#[cfg(feature = "test-harness")]
pub mod server_task;

#[cfg(feature = "testing")]
pub use container::S3TestContainer;
#[cfg(feature = "testing")]
pub use s3::{create_s3_bucket, create_s3_object_store};
