//! Storage configuration and builders.
//!
//! Provides:
//! - [`StorageConfig`] / [`StorageBackend`] / [`S3Config`] — YAML/TOML config types
//! - [`OperatorRegistry`] — the object stores and `OpenDAL` operators of one
//!   component, each built once
//! - [`IceGateStorage`] / [`IceGateStorageFactory`] — Iceberg `Storage` impl
//!   with foyer caching and OpenTelemetry layers
//! - [`StorageCache`] / [`build_storage_cache`] — shared foyer cache utilities
//!
//! Both operator paths resolve through the registry, which wraps every
//! operator it builds in the layer stack defined by the private `layers`
//! module, whose docs carry the reason operators are built once and reused.

mod builder;
pub mod cache;
mod config;
mod icegate_s3;
pub mod icegate_storage;
mod layers;
pub mod prefetch;
mod registry;

pub use builder::ObjectStoreWithPath;
pub use cache::{StorageCache, build_storage_cache, register_foyer_metrics};
pub use config::{S3Config, StorageBackend, StorageConfig};
pub use icegate_storage::{IceGateStorage, IceGateStorageFactory};
pub use layers::StorageLayersConfig;
pub use prefetch::PrefetchConfig;
pub use registry::OperatorRegistry;
