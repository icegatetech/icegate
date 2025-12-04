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

/// Catalog management for Iceberg catalogs.
pub mod catalog;
/// Common configuration utilities.
pub mod config;
/// Error types for IceGate operations.
pub mod errors;
/// Schema definitions for Iceberg tables.
pub mod schema;
/// Storage configuration.
pub mod storage;

// Re-export commonly used types
pub use catalog::{CatalogBackend, CatalogBuilder, CatalogConfig};
pub use config::{ServerConfig, check_port_conflicts, load_config_file};
pub use storage::{S3Config, StorageBackend, StorageConfig};

/// Result type alias for `IceGate` operations.
///
/// This is a convenience type alias for `std::result::Result<T, IceGateError>`.
pub type Result<T> = std::result::Result<T, errors::IceGateError>;

