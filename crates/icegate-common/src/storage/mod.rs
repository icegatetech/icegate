//! Storage configuration and builders.
//!
//! Provides:
//! - [`StorageConfig`] / [`StorageBackend`] / [`S3Config`] — YAML/TOML config types
//! - [`create_object_store`] — WAL object store builder (OpenDAL-backed)
//! - [`IceGateStorage`] / [`IceGateStorageFactory`] — Iceberg `Storage` impl
//!   with foyer caching and OpenTelemetry layers
//! - [`StorageCache`] / [`build_storage_cache`] — shared foyer cache utilities

mod builder;
pub mod cache;
mod config;
mod icegate_s3;
pub mod icegate_storage;
pub mod prefetch;

pub use builder::{ObjectStoreWithPath, create_local_store, create_memory_store, create_object_store, create_s3_store};
pub use cache::{StorageCache, build_storage_cache, register_foyer_metrics};
pub use config::{S3Config, StorageBackend, StorageConfig};
pub use icegate_storage::{IceGateStorage, IceGateStorageFactory};
pub use prefetch::PrefetchConfig;
