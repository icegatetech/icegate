//! S3-backed Iceberg catalog with `root.json` CAS synchronization.

mod catalog;
mod codec;
mod config;
mod error;
mod infra;
mod model;
mod storage;

#[cfg(test)]
mod tests;

pub use catalog::S3Catalog;
pub use config::{CatalogCodecKind, S3CatalogConfig};
pub use error::{Error, Result};
pub use infra::retrier::{Retrier, RetrierConfig, RetryError};
