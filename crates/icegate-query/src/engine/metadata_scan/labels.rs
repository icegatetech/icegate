//! `/labels` algorithm.
//!
//! Two sources of labels, neither of which decodes row data:
//!
//! 1. Indexed top-level columns — derived from row-group `null_count`
//!    statistics. Pure Parquet metadata, zero data or dictionary pages
//!    fetched.
//! 2. Attribute MAP keys — derived by pulling only the dictionary page of
//!    the `attributes.*.key` sub-column for every row group via
//!    [`crate::engine::metadata_scan::parquet_reader::read_column_dictionaries`].

use std::borrow::Cow;
use std::collections::BTreeSet;

use iceberg::arrow::ArrowFileReader;
use iceberg::expr::Predicate;
use icegate_common::attribute_key::normalize_attribute_key;
use icegate_common::schema::COL_TENANT_ID;
use parquet::file::metadata::ParquetMetaData;

use super::MetadataScanConfig;
use super::error::MetadataScanError;
use super::parquet_reader;

/// Tag/label names that are unconditionally hidden from discovery,
/// regardless of caller config. The tenant column is system-level
/// metadata: surfacing it as a tag would let any caller enumerate
/// tenants and constitutes a cross-tenant information leak. Excluded
/// at the metadata-scan layer (the "ground") rather than per-caller so
/// no future config can accidentally re-expose it.
///
/// Comparison is case-insensitive — defenders should not have to know
/// whether a particular ingest path writes `tenant_id`, `Tenant_ID`,
/// etc.
const SYSTEM_RESERVED_TAG_NAMES: &[&str] = &[COL_TENANT_ID];

/// Whether `name` matches a system-reserved tag (case-insensitively).
fn is_system_reserved(name: &str) -> bool {
    SYSTEM_RESERVED_TAG_NAMES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(name))
}

/// Walk row-group statistics and add every indexed column listed in
/// `config.indexed_columns` that has at least one non-null value to `out`.
/// Any alias mapping to a present column is also inserted (e.g. logs' `level`
/// alias for `severity_text`).
///
/// Pure metadata: no data pages are read.
pub fn collect_indexed_from_metadata(
    metadata: &ParquetMetaData,
    config: &MetadataScanConfig,
    out: &mut BTreeSet<String>,
) {
    let schema = metadata.file_metadata().schema_descr();

    // Map indexed column name -> leaf column index. Done once per file.
    // System-reserved names (tenant_id) are dropped here so a misconfigured
    // `indexed_columns` list cannot leak them into discovery.
    let mut name_to_leaf: Vec<(&'static str, usize)> = Vec::new();
    for &name in config.indexed_columns {
        if is_system_reserved(name) {
            continue;
        }
        if let Some(idx) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == name) {
            name_to_leaf.push((name, idx));
        }
    }

    for rg_idx in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(rg_idx);
        let num_rows = rg.num_rows();

        for &(name, leaf_idx) in &name_to_leaf {
            // Fast path: the column AND every alias for it are already
            // recorded. Otherwise we keep going so later row groups can
            // still flip an alias on.
            if out.contains(name) && config.aliases_for(name).all(|a| out.contains(a)) {
                continue;
            }

            let col = rg.column(leaf_idx);
            // No stats → conservative: assume the column might have values
            // (over-approximation is allowed).
            let has_values = col.statistics().is_none_or(|stats| {
                let null_count = stats.null_count_opt().unwrap_or(0);
                // `num_rows()` is i64 in parquet-rs; coerce to u64 for the
                // comparison. Row counts are always non-negative.
                let total: u64 = u64::try_from(num_rows).unwrap_or(0);
                null_count < total
            });

            if has_values {
                out.insert(name.to_string());
                for alias in config.aliases_for(name) {
                    out.insert(alias.to_string());
                }
            }
        }
    }
}

/// Collect distinct MAP attribute keys by reading only the dictionary
/// page of the `attributes.*.key` sub-column for every row group that
/// survives row-group predicate pruning.
///
/// # Errors
///
/// Returns `MetadataScanError::Parquet` if a column chunk fails to decode.
#[tracing::instrument(skip_all)]
pub async fn collect_map_keys_via_dict(
    reader: &mut ArrowFileReader,
    metadata: &ParquetMetaData,
    predicate: &Predicate,
    config: &MetadataScanConfig,
    out: &mut BTreeSet<String>,
) -> Result<(), MetadataScanError> {
    let schema = metadata.file_metadata().schema_descr();

    // Find the `<map_column>.*.key` leaf. Arrow's MAP<Utf8,Utf8> may
    // serialize as `<map_column>.key_value.key` or `<map_column>.entries.key`
    // depending on writer version — match on the top-level column + the
    // leaf name rather than a hard-coded path.
    let key_leaf_idx = (0..schema.num_columns()).find(|&i| {
        let col = schema.column(i);
        let parts = col.path().parts();
        parts.first().is_some_and(|s| s == config.map_column) && parts.last().is_some_and(|s| s == "key")
    });
    let Some(key_leaf_idx) = key_leaf_idx else {
        // Map column not present in this file — nothing to do. Not an error
        // under over-approximation semantics.
        return Ok(());
    };

    parquet_reader::read_column_dictionaries(reader, metadata, predicate, key_leaf_idx, out).await?;
    retain_discoverable_keys(out, config);
    Ok(())
}

/// Reduce raw stored map keys to the label names `/labels` may advertise.
///
/// Under a normalizing policy the keys are rendered as wire names BEFORE the
/// exclusions apply, because both exclusion lists are written in wire form: a
/// stored `trace.id` would otherwise slip past a `trace_id` exclusion and then
/// be normalized into exactly that name by the caller, re-exposing the label
/// the exclusion exists to hide. Normalization is idempotent, so hoisting it
/// here does not change what the caller emits.
fn retain_discoverable_keys(out: &mut BTreeSet<String>, config: &MetadataScanConfig) {
    if config.normalize_keys {
        *out = std::mem::take(out)
            .into_iter()
            .map(|key| match normalize_attribute_key(&key) {
                Cow::Borrowed(_) => key,
                Cow::Owned(wire_name) => wire_name,
            })
            .collect();
    }

    // Hard exclusion: system-reserved keys (tenant_id) are stripped
    // unconditionally regardless of caller config. The tenant column
    // identifies isolation boundaries; surfacing it as a tag is a
    // cross-tenant information leak.
    out.retain(|k| !is_system_reserved(k));
    // Remove caller-configured keys that are not useful for label discovery.
    for &key in config.excluded_map_keys {
        out.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use icegate_common::schema::{COL_LOG_ATTRIBUTES, COL_SPAN_ATTRIBUTES};

    use super::{MetadataScanConfig, is_system_reserved, retain_discoverable_keys};

    #[test]
    fn tenant_id_is_system_reserved() {
        assert!(is_system_reserved("tenant_id"));
    }

    #[test]
    fn tenant_id_match_is_case_insensitive() {
        assert!(is_system_reserved("TENANT_ID"));
        assert!(is_system_reserved("Tenant_Id"));
    }

    #[test]
    fn unrelated_names_are_not_reserved() {
        for ok in [
            "service_name",
            "trace_id",
            "span_id",
            "tenantid", // no underscore — different name
            "x_tenant_id",
            "",
        ] {
            assert!(!is_system_reserved(ok), "{ok} should not be reserved");
        }
    }

    /// Loki-shaped config: wire-form exclusions, normalizing policy.
    const NORMALIZING_CFG: MetadataScanConfig = MetadataScanConfig {
        indexed_columns: &[],
        label_aliases: &[],
        excluded_map_keys: &["trace_id", "span_id"],
        map_column: COL_LOG_ATTRIBUTES,
        normalize_keys: true,
    };

    /// Tempo-shaped config: keys are matched as stored, nothing normalizes.
    const EXACT_CFG: MetadataScanConfig = MetadataScanConfig {
        indexed_columns: &[],
        label_aliases: &[],
        excluded_map_keys: &["trace_id", "span_id"],
        map_column: COL_SPAN_ATTRIBUTES,
        normalize_keys: false,
    };

    fn discoverable(stored: &[&str], config: &MetadataScanConfig) -> Vec<String> {
        let mut out: BTreeSet<String> = stored.iter().map(|k| (*k).to_string()).collect();
        retain_discoverable_keys(&mut out, config);
        out.into_iter().collect()
    }

    #[test]
    fn dotted_key_is_excluded_under_its_wire_name() {
        // The regression: a stored `trace.id` used to survive the wire-form
        // exclusion and then be normalized into `trace_id` by the caller,
        // re-exposing the high-cardinality label the list exists to hide.
        assert_eq!(discoverable(&["trace.id", "pod"], &NORMALIZING_CFG), vec!["pod"]);
    }

    #[test]
    fn dotted_system_reserved_key_is_excluded_under_its_wire_name() {
        assert_eq!(discoverable(&["tenant.id", "pod"], &NORMALIZING_CFG), vec!["pod"]);
    }

    #[test]
    fn surviving_keys_come_back_as_wire_names_when_normalizing() {
        assert_eq!(
            discoverable(&["k8s.pod.name"], &NORMALIZING_CFG),
            vec!["k8s_pod_name".to_string()]
        );
    }

    #[test]
    fn dotted_key_is_kept_as_stored_when_not_normalizing() {
        // Tempo addresses attributes by their dotted name, so neither the
        // rendering nor the wire-form exclusion may apply there.
        assert_eq!(
            discoverable(&["trace.id", "http.method"], &EXACT_CFG),
            vec!["http.method".to_string(), "trace.id".to_string()]
        );
    }
}
