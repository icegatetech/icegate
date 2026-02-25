//! Catalog management module
//!
//! Provides abstraction over Iceberg catalog implementations.

mod builder;
mod config;

pub use builder::{CatalogBuilder, IoCacheHandle};
pub use config::{CacheConfig, CatalogBackend, CatalogConfig};
