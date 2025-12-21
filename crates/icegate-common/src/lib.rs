//! Common utilities for IceGate.

/// The Iceberg catalog name registered with `DataFusion`.
pub const ICEBERG_CATALOG: &str = "iceberg";

/// The Iceberg namespace for `IceGate` tables.
pub const ICEGATE_NAMESPACE: &str = "icegate";

/// Table name constants.
pub const LOGS_TABLE: &str = "logs";
/// Table name for distributed trace spans.
pub const SPANS_TABLE: &str = "spans";
/// Table name for semantic events.
pub const EVENTS_TABLE: &str = "events";
/// Table name for metrics.
pub const METRICS_TABLE: &str = "metrics";

/// Fully qualified table name for logs (`iceberg.icegate.logs`).
pub const LOGS_TABLE_FQN: &str = "iceberg.icegate.logs";
/// Fully qualified table name for spans (`iceberg.icegate.spans`).
pub const SPANS_TABLE_FQN: &str = "iceberg.icegate.spans";
/// Fully qualified table name for events (`iceberg.icegate.events`).
pub const EVENTS_TABLE_FQN: &str = "iceberg.icegate.events";
/// Fully qualified table name for metrics (`iceberg.icegate.metrics`).
pub const METRICS_TABLE_FQN: &str = "iceberg.icegate.metrics";

/// Default tenant ID when not provided in request metadata.
pub const DEFAULT_TENANT_ID: &str = "default";

/// Default account ID when not provided in resource attributes.
/// Required because `cloud_account_id` is a partition column.
pub const DEFAULT_ACCOUNT_ID: &str = "default";

/// Topic name for logs in the WAL queue.
pub const LOGS_TOPIC: &str = "logs";

/// Catalog management for Iceberg catalogs.
pub mod catalog;
/// Common configuration utilities.
pub mod config;
/// Error types for common operations.
pub mod error;
/// Schema definitions for Iceberg tables.
pub mod schema;
/// Storage configuration.
pub mod storage;

// Re-export commonly used types
pub use catalog::{CatalogBackend, CatalogBuilder, CatalogConfig};
pub use config::{check_port_conflicts, load_config_file, ServerConfig};
pub use error::Result;
pub use storage::{
    create_local_store, create_memory_store, create_object_store, create_s3_store, ObjectStoreWithPath, S3Config,
    StorageBackend, StorageConfig,
};
