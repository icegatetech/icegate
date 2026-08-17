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
use icegate_common::schema::{
    COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, LOG_INDEXED_ATTRIBUTE_COLUMNS,
    LOG_SERIES_LABEL_COLUMNS,
};
pub use server::{run, run_with_port_tx};

use crate::engine::metadata_scan::MetadataScanConfig;

/// Base metadata-scan configuration shared by every element of
/// [`LOGS_METADATA_CONFIGS`].
///
/// Lists the indexed label columns surfaced in `/labels`, the Grafana-
/// compatible alias renames (`level`/`detected_level` → `severity_text`,
/// `service` → `service_name`), and the high-cardinality attribute-map
/// keys (`trace_id`, `span_id`) that should be hidden from `/labels`
/// output to keep the dropdown manageable. `map_column` on this constant
/// is a placeholder: every array built from it overrides the field, since
/// the logs table splits attributes across three MAP columns rather than
/// the single `attributes` column this config type was designed for.
///
/// Note: `/label_values` resolution for `trace_id` / `span_id` is *not*
/// handled by `label_aliases` — those only carry the three rename
/// mappings above. Trace/span-ID values are surfaced via the
/// `indexed_columns` set on [`LOGS_VALUES_METADATA_CONFIG`] (which
/// references `LOG_INDEXED_ATTRIBUTE_COLUMNS`), so the `/label_values`
/// endpoint can enumerate them while `/labels` keeps them hidden.
const LOGS_METADATA_CONFIG: MetadataScanConfig = MetadataScanConfig {
    indexed_columns: LOG_SERIES_LABEL_COLUMNS,
    label_aliases: &[
        ("level", "severity_text"),
        ("detected_level", "severity_text"),
        ("service", "service_name"),
    ],
    excluded_map_keys: &["trace_id", "span_id"],
    map_column: COL_RESOURCE_ATTRIBUTES,
    // Loki label names admit neither dots nor colons, so `/labels` and
    // `/label_values` address a stored `user.id` as `user_id`.
    normalize_keys: true,
};

/// Label-enumeration configs for the logs table, one per stored attribute map.
///
/// `MetadataScanConfig` addresses a single MAP column, so the per-level split is
/// expressed as one config per level. Enumeration output is normalized to wire
/// names and deduplicated by the caller, so two levels holding keys that share a
/// wire name surface as a single label.
const LOGS_METADATA_CONFIGS: [MetadataScanConfig; 3] = [
    MetadataScanConfig {
        map_column: COL_RESOURCE_ATTRIBUTES,
        ..LOGS_METADATA_CONFIG
    },
    MetadataScanConfig {
        map_column: COL_SCOPE_ATTRIBUTES,
        ..LOGS_METADATA_CONFIG
    },
    MetadataScanConfig {
        map_column: COL_LOG_ATTRIBUTES,
        ..LOGS_METADATA_CONFIG
    },
];

/// Base `/label_values` metadata-scan configuration shared by every element
/// of [`LOGS_VALUES_METADATA_CONFIGS`].
///
/// Indexed columns are a superset of the series-label columns: includes
/// high-cardinality identifiers (`trace_id`, `span_id`) that are hidden
/// from `/labels` but can still be enumerated via the explicit value
/// endpoint.
///
/// `label_aliases` and `normalize_keys` are inherited from
/// [`LOGS_METADATA_CONFIG`] so future updates propagate automatically —
/// only `indexed_columns` (broader here) and `excluded_map_keys` (empty
/// here) differ. `map_column` is a placeholder overridden per element by
/// [`LOGS_VALUES_METADATA_CONFIGS`], same as on [`LOGS_METADATA_CONFIG`].
const LOGS_VALUES_METADATA_CONFIG: MetadataScanConfig = MetadataScanConfig {
    indexed_columns: LOG_INDEXED_ATTRIBUTE_COLUMNS,
    label_aliases: LOGS_METADATA_CONFIG.label_aliases,
    excluded_map_keys: &[],
    map_column: LOGS_METADATA_CONFIG.map_column,
    normalize_keys: LOGS_METADATA_CONFIG.normalize_keys,
};

/// `/label_values` metadata-scan configs, one per stored attribute map.
///
/// See [`LOGS_METADATA_CONFIGS`] for why the split is one config per level
/// rather than one config with three map columns.
const LOGS_VALUES_METADATA_CONFIGS: [MetadataScanConfig; 3] = [
    MetadataScanConfig {
        map_column: COL_RESOURCE_ATTRIBUTES,
        ..LOGS_VALUES_METADATA_CONFIG
    },
    MetadataScanConfig {
        map_column: COL_SCOPE_ATTRIBUTES,
        ..LOGS_VALUES_METADATA_CONFIG
    },
    MetadataScanConfig {
        map_column: COL_LOG_ATTRIBUTES,
        ..LOGS_VALUES_METADATA_CONFIG
    },
];

#[cfg(test)]
mod tests {
    use icegate_common::schema::{COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES};

    use super::LOGS_METADATA_CONFIGS;

    #[test]
    fn logs_metadata_configs_cover_every_stored_attribute_column() {
        let covered: Vec<&str> = LOGS_METADATA_CONFIGS.iter().map(|c| c.map_column).collect();
        assert_eq!(
            covered,
            vec![COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, COL_LOG_ATTRIBUTES],
            "label enumeration must scan all three levels"
        );
        assert!(
            LOGS_METADATA_CONFIGS.iter().all(|c| c.map_column != "attributes"),
            "no config may point at the removed merged column"
        );
    }
}
