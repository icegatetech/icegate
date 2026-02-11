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

/// HTTP header / gRPC metadata key for tenant identification (`X-Scope-OrgID`,
/// Grafana/Loki standard). Stored lowercase per HTTP/2 and gRPC conventions;
/// header lookups are case-insensitive.
pub const TENANT_ID_HEADER: &str = "x-scope-orgid";

/// Validate a tenant ID value.
///
/// Returns `true` if `value` is non-empty and contains only ASCII
/// alphanumeric characters, hyphens, or underscores.
pub fn is_valid_tenant_id(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Topic name for logs in the WAL queue.
pub const LOGS_TOPIC: &str = "logs";

/// Catalog management for Iceberg catalogs.
pub mod catalog;
/// Common configuration utilities.
pub mod config;
/// Error types for common operations.
pub mod error;
/// Prometheus metrics utilities.
pub mod metrics;
/// Retry utilities.
pub mod retrier;
/// Schema definitions for Iceberg tables.
pub mod schema;
/// Storage configuration.
pub mod storage;
/// OpenTelemetry tracing configuration and utilities.
pub mod tracing;

/// Testing utilities (available only with `testing` feature).
#[cfg(feature = "testing")]
pub mod testing;

// Re-export commonly used types
pub use catalog::{CatalogBackend, CatalogBuilder, CatalogConfig};
pub use config::{ServerConfig, check_port_conflicts, load_config_file};
pub use error::Result;
pub use metrics::{MetricsConfig, MetricsRuntime, run_metrics_server};
pub use retrier::{Retrier, RetrierConfig, RetryError};
pub use storage::{
    ObjectStoreWithPath, S3Config, StorageBackend, StorageConfig, create_local_store, create_memory_store,
    create_object_store, create_s3_store,
};
pub use tracing::{
    TracingConfig, TracingGuard, add_span_link, add_span_links, extract_current_trace_context, init_tracing,
    traceparent_to_context,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_tenant_ids() {
        assert!(is_valid_tenant_id("default"));
        assert!(is_valid_tenant_id("my-tenant"));
        assert!(is_valid_tenant_id("tenant_123"));
        assert!(is_valid_tenant_id("Org-42_prod"));
        assert!(is_valid_tenant_id("a"));
    }

    #[test]
    fn test_invalid_tenant_ids() {
        assert!(!is_valid_tenant_id(""));
        assert!(!is_valid_tenant_id("has space"));
        assert!(!is_valid_tenant_id("has/slash"));
        assert!(!is_valid_tenant_id("has.dot"));
        assert!(!is_valid_tenant_id("emoji\u{1F600}"));
        assert!(!is_valid_tenant_id("tab\there"));
    }
}
