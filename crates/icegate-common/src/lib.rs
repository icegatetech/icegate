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

/// Build the [`iceberg::TableIdent`] for a table name inside the IceGate
/// namespace ([`ICEGATE_NAMESPACE`]).
///
/// Every IceGate table lives in the same namespace, so the
/// `TableIdent::new(NamespaceIdent::new(ICEGATE_NAMESPACE…), table…)` construction
/// was repeated across the ingest shift, compaction, and migrate paths. Defining
/// it once keeps namespace resolution in a single place.
#[must_use]
pub fn icegate_table_ident(table: &str) -> iceberg::TableIdent {
    iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()),
        table.to_string(),
    )
}

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

/// Resolve a tenant identifier from an optional raw header value.
///
/// Returns the value when present and valid per [`is_valid_tenant_id`],
/// otherwise [`DEFAULT_TENANT_ID`]. This is the single fallback policy
/// shared by every query protocol (Loki, Tempo, Flight SQL); callers only
/// differ in how they pull the raw string out of their header/metadata
/// map, so centralising the validate-or-default step here keeps tenant
/// resolution defined in exactly one place.
#[must_use]
pub fn resolve_tenant_id(header_value: Option<&str>) -> String {
    header_value
        .filter(|value| is_valid_tenant_id(value))
        .map_or_else(|| DEFAULT_TENANT_ID.to_string(), String::from)
}

/// Topic name for logs in the WAL queue.
pub const LOGS_TOPIC: &str = "logs";

/// Topic name for spans in the WAL queue.
pub const SPANS_TOPIC: &str = "spans";

/// Topic name for metrics in the WAL queue.
pub const METRICS_TOPIC: &str = "metrics";

/// Iceberg snapshot summary key for the last committed WAL queue offset.
///
/// Written by the Shifter after each WAL-to-Iceberg commit and read by the
/// query engine to determine the WAL/Iceberg boundary.
pub const WAL_OFFSET_PROPERTY: &str = "icegate.queue.offset";

/// Catalog management for Iceberg catalogs.
pub mod catalog;
/// Common configuration utilities.
pub mod config;
/// Error types for common operations.
pub mod error;
/// Shared Iceberg Parquet write pipeline (Arrow batches → data files).
pub mod iceberg_write;
/// Snapshot data-file enumeration with decoded sort-key bounds (compaction).
pub mod manifest_scan;
/// Sort-merge primitives shared across ingest, the Shifter, and compaction.
pub mod merge;
/// Prometheus metrics utilities.
pub mod metrics;
/// Per-column Parquet encoding overrides shared across writers.
pub mod parquet_encoding;
/// Shared opener for reading existing Iceberg Parquet data files.
pub mod parquet_source;
/// Parquet `WriterProperties` builder shared by ingest writers.
///
/// Consumes the per-column encoding lists from [`parquet_encoding`].
pub mod parquet_writer;
/// Retry utilities.
pub mod retrier;
/// Schema definitions for Iceberg tables.
pub mod schema;
/// Storage configuration.
pub mod storage;
/// OpenTelemetry tracing configuration and utilities.
pub mod tracing;
/// Compaction-safe resolution of the last committed WAL offset from snapshots.
pub mod wal_offset;

/// Testing utilities (available only with `testing` feature).
#[cfg(feature = "testing")]
pub mod testing;

// Re-export commonly used types
pub use catalog::{CatalogBackend, CatalogBuilder, CatalogConfig, IoHandle};
pub use config::{ServerConfig, check_port_conflicts, load_config_file};
pub use error::{CommonError as Error, Result};
pub use manifest_scan::{DataFileStats, list_data_files_with_stats};
pub use metrics::{MetricsConfig, MetricsRuntime, run_metrics_server};
pub use retrier::{Retrier, RetrierConfig, RetryError};
pub use storage::{
    IceGateStorage, IceGateStorageFactory, ObjectStoreWithPath, PrefetchConfig, S3Config, StorageBackend, StorageCache,
    StorageConfig, build_storage_cache, create_local_store, create_memory_store, create_object_store, create_s3_store,
    register_foyer_metrics,
};
pub use tracing::{
    TracingConfig, TracingGuard, add_span_link, add_span_links, extract_current_trace_context, init_tracing,
    traceparent_to_context,
};
pub use wal_offset::resolve_wal_offset;

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

    #[test]
    fn resolve_tenant_id_honours_valid_value() {
        assert_eq!(resolve_tenant_id(Some("tenant-a")), "tenant-a");
    }

    #[test]
    fn resolve_tenant_id_falls_back_on_absent_or_invalid() {
        assert_eq!(resolve_tenant_id(None), DEFAULT_TENANT_ID);
        assert_eq!(resolve_tenant_id(Some("has space")), DEFAULT_TENANT_ID);
        assert_eq!(resolve_tenant_id(Some("")), DEFAULT_TENANT_ID);
    }
}
