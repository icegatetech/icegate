//! Loki API server module
//!
//! Provides Loki-compatible HTTP API for querying logs using LogQL.

mod config;
mod error;
mod executor;
mod formatters;
mod handlers;
mod models;
mod predicate;
mod routes;
mod server;

pub use config::LokiConfig;
use icegate_common::schema::LOG_SERIES_LABEL_COLUMNS;
pub use server::{run, run_with_port_tx};

use crate::engine::metadata_scan::MetadataScanConfig;

/// Per-logs metadata-scan configuration.
///
/// Lists the indexed label columns surfaced in `/labels`, the Grafana-
/// compatible alias renames, and the high-cardinality attribute-map keys
/// (`trace_id`, `span_id`) that should be hidden from `/labels` output.
/// `/label_values` can still resolve these explicitly via the alias logic.
const LOGS_METADATA_CONFIG: MetadataScanConfig = MetadataScanConfig {
    indexed_columns: LOG_SERIES_LABEL_COLUMNS,
    label_aliases: &[
        ("level", "severity_text"),
        ("detected_level", "severity_text"),
        ("service", "service_name"),
    ],
    excluded_map_keys: &["trace_id", "span_id"],
    map_column: icegate_common::schema::COL_ATTRIBUTES,
};

/// Indexed columns eligible for `/label_values` lookup on the logs table.
///
/// Superset of the series-label columns: includes high-cardinality identifiers
/// (`trace_id`, `span_id`) that are hidden from `/labels` but can still be
/// enumerated via the explicit value endpoint.
const LOGS_VALUES_METADATA_CONFIG: MetadataScanConfig = MetadataScanConfig {
    indexed_columns: icegate_common::schema::LOG_INDEXED_ATTRIBUTE_COLUMNS,
    label_aliases: LOGS_METADATA_CONFIG.label_aliases,
    excluded_map_keys: &[],
    map_column: icegate_common::schema::COL_ATTRIBUTES,
};
