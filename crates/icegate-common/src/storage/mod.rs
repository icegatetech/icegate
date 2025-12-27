//! Storage configuration module

mod builder;
mod config;

pub use builder::{ObjectStoreWithPath, create_local_store, create_memory_store, create_object_store, create_s3_store};
pub use config::{S3Config, StorageBackend, StorageConfig};
