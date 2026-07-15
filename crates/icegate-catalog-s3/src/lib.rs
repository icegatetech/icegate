//! S3-backed Iceberg catalog with `root.json` CAS synchronization.

mod config;
mod domain;
mod error;
mod infra;
mod services;
mod storage;

#[cfg(feature = "rest")]
pub mod api;
#[cfg(feature = "rest")]
pub mod cli;

#[cfg(test)]
mod tests;

#[cfg(feature = "rest")]
pub use api::{CatalogApiConfig, CatalogApiConfigError, CatalogPrefix, CatalogPrefixError};
pub use config::{
    CatalogCodecKind, S3CatalogConfig, UnsupportedCatalogCodecError, format_warehouse_uri, resolve_path_style_access,
};
#[cfg(feature = "rest")]
pub use config::{CatalogServerConfig, CatalogServerConfigError, CatalogServerHttpConfig, CatalogServerS3Config};
pub use error::{Error, Result};
pub use infra::retrier::{Retrier, RetrierConfig, RetryError};
pub use services::S3Catalog;
