//! S3-backed Iceberg catalog with `root.json` CAS synchronization.

mod catalog;
mod codec;
mod config;
mod error;
mod model;
mod storage;

pub use catalog::{CommitTableRequest, CommitTableResponse, S3Catalog};
pub use codec::CatalogCodecKind;
pub use config::S3CatalogConfig;
pub use error::{Error, Result};
