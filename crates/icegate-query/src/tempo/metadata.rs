//! Discover tag names and tag values on the `spans` Iceberg table.
//!
//! Backs the three Tempo metadata endpoints:
//! - `GET /api/search/tags`      — flat list across all scopes.
//! - `GET /api/v2/search/tags`   — grouped by `TraceQL` scope.
//! - `GET /api/search/tag/{name}/values` — distinct values for a tag.
//!
//! All three reuse [`crate::engine::metadata_scan`], which reads Parquet
//! footers / dictionary pages rather than materializing rows — the same
//! code path used by Loki's `/labels` and `/label_values`.
//!
//! Post the 2026-04-19 spans-attributes split, the spans table carries two
//! separate `MAP<String,String>` columns:
//! - `resource_attributes` — keys from OTLP `Resource`.
//! - `span_attributes`     — keys from OTLP `Span` + folded scope attrs.
//!
//! Scope separation in `/api/v2/search/tags` therefore becomes
//! schema-enforced rather than heuristic: each map drives its own scope.

use std::collections::BTreeSet;

use chrono::{DateTime, TimeZone, Utc};
use iceberg::expr::Predicate;
use icegate_common::schema::{
    COL_CLOUD_ACCOUNT_ID, COL_NAME, COL_PARENT_SPAN_ID, COL_RESOURCE_ATTRIBUTES, COL_SERVICE_NAME, COL_SPAN_ATTRIBUTES,
    COL_SPAN_ID, COL_TRACE_ID, SPAN_INDEXED_ATTRIBUTE_COLUMNS, SPAN_SERIES_LABEL_COLUMNS, TRACEQL_INTRINSIC_TAGS,
};
use icegate_common::{ICEGATE_NAMESPACE, SPANS_TABLE};

use super::error::{TempoError, TempoResult};
use super::models::{Scope, ScopeGroup, TagRef};
use super::server::TempoState;
use crate::engine::metadata_scan::{self, MetadataScanConfig};
use crate::error::QueryError;

/// Top-level columns whose presence contributes to the `resource` scope
/// (regardless of whether they also appear in the attribute map).
const RESOURCE_TOP_LEVEL_COLUMNS: &[&str] = &[COL_SERVICE_NAME, COL_CLOUD_ACCOUNT_ID];

/// Metadata-scan config for the `resource_attributes` map.
///
/// `indexed_columns` is empty here — resource-scope top-level columns are
/// handled separately in `list_tags_v2` so we can emit them under the
/// `resource` group without duplicating them in `span`.
const SPANS_RESOURCE_CONFIG: MetadataScanConfig = MetadataScanConfig {
    indexed_columns: &[],
    label_aliases: &[],
    excluded_map_keys: &[],
    map_column: COL_RESOURCE_ATTRIBUTES,
};

/// Metadata-scan config for the `span_attributes` map.
const SPANS_SPAN_CONFIG: MetadataScanConfig = MetadataScanConfig {
    indexed_columns: SPAN_SERIES_LABEL_COLUMNS,
    label_aliases: &[],
    excluded_map_keys: &[],
    map_column: COL_SPAN_ATTRIBUTES,
};

/// Metadata-scan config for span tag-value lookup (`/tag/<name>/values`)
/// against resource-scoped keys.
const SPANS_VALUES_RESOURCE_CONFIG: MetadataScanConfig = MetadataScanConfig {
    indexed_columns: SPAN_INDEXED_ATTRIBUTE_COLUMNS,
    label_aliases: &[],
    excluded_map_keys: &[],
    map_column: COL_RESOURCE_ATTRIBUTES,
};

/// Metadata-scan config for span tag-value lookup (`/tag/<name>/values`)
/// against span-scoped keys.
const SPANS_VALUES_SPAN_CONFIG: MetadataScanConfig = MetadataScanConfig {
    indexed_columns: SPAN_INDEXED_ATTRIBUTE_COLUMNS,
    label_aliases: &[],
    excluded_map_keys: &[],
    map_column: COL_SPAN_ATTRIBUTES,
};

/// Default lookback when a client omits `start` / `end`.
///
/// Generous default matches Tempo: metadata endpoints are expected to be
/// low-traffic and a wide window is safer than missing tags.
const DEFAULT_LOOKBACK_HOURS: i64 = 6;

/// List all distinct tag names for the spans table in the given tenant+time
/// window. Used by the v1 `/api/search/tags` endpoint.
///
/// # Errors
///
/// Returns a [`TempoError`] wrapping the underlying metadata-scan or
/// iceberg error.
pub async fn list_tags_v1(
    state: &TempoState,
    tenant_id: &str,
    start: Option<i64>,
    end: Option<i64>,
) -> TempoResult<Vec<String>> {
    let v2 = list_tags_v2(state, tenant_id, start, end, None).await?;
    let mut out: BTreeSet<String> = BTreeSet::new();
    for group in v2.scopes {
        out.extend(group.tags);
    }
    Ok(out.into_iter().collect())
}

/// List distinct tag names grouped by `TraceQL` scope. Backs
/// `/api/v2/search/tags`.
///
/// If `scope_filter` is `Some`, only that scope appears in the result. The
/// empty or unrecognised scope case (no `scope=` query param) returns all
/// scopes.
///
/// # Errors
///
/// Returns a [`TempoError`] wrapping the underlying metadata-scan or
/// iceberg error.
pub async fn list_tags_v2(
    state: &TempoState,
    tenant_id: &str,
    start: Option<i64>,
    end: Option<i64>,
    scope_filter: Option<Scope>,
) -> TempoResult<super::models::TagsV2Response> {
    let (start_dt, end_dt) = resolve_window(start, end);
    let table = load_spans_table(state).await?;

    let mut groups: Vec<ScopeGroup> = Vec::with_capacity(5);

    // Resource scope: resource_attributes map keys + top-level resource
    // columns that exist in the schema.
    if scope_filter.map_or(true, |s| s == Scope::Resource) {
        let mut tags: BTreeSet<String> = metadata_scan::scan_labels(
            &table,
            tenant_id,
            start_dt,
            end_dt,
            &SPANS_RESOURCE_CONFIG,
            Predicate::AlwaysTrue,
        )
        .await
        .map_err(|e| TempoError(QueryError::from(e)))?;
        for col in RESOURCE_TOP_LEVEL_COLUMNS {
            if table_has_column(&table, col) {
                tags.insert((*col).to_string());
            }
        }
        groups.push(ScopeGroup {
            name: Scope::Resource.as_str().to_string(),
            tags: tags.into_iter().collect(),
        });
    }

    // Span scope: span_attributes map keys + span-specific indexed columns
    // (name).
    if scope_filter.map_or(true, |s| s == Scope::Span) {
        let tags: BTreeSet<String> = metadata_scan::scan_labels(
            &table,
            tenant_id,
            start_dt,
            end_dt,
            &SPANS_SPAN_CONFIG,
            Predicate::AlwaysTrue,
        )
        .await
        .map_err(|e| TempoError(QueryError::from(e)))?;
        groups.push(ScopeGroup {
            name: Scope::Span.as_str().to_string(),
            tags: tags.into_iter().collect(),
        });
    }

    if scope_filter.map_or(true, |s| s == Scope::Intrinsic) {
        groups.push(ScopeGroup {
            name: Scope::Intrinsic.as_str().to_string(),
            tags: TRACEQL_INTRINSIC_TAGS.iter().map(|s| (*s).to_string()).collect(),
        });
    }

    // event and link attributes require unnesting nested LIST<STRUCT> maps —
    // deferred. Return empty lists so Grafana's query builder still shows
    // the sections.
    if scope_filter.map_or(true, |s| s == Scope::Event) {
        groups.push(ScopeGroup {
            name: Scope::Event.as_str().to_string(),
            tags: Vec::new(),
        });
    }
    if scope_filter.map_or(true, |s| s == Scope::Link) {
        groups.push(ScopeGroup {
            name: Scope::Link.as_str().to_string(),
            tags: Vec::new(),
        });
    }

    Ok(super::models::TagsV2Response { scopes: groups })
}

/// Enumerate distinct values for a single tag on the spans table.
///
/// `tag_name` is the dotted form as sent by Grafana (`resource.service.name`,
/// `span.http.method`, `.pod`, or a bare intrinsic like `name`).
///
/// Intrinsics that are numeric (`duration`, `status`, `kind`, …) or
/// derived (`traceDuration`, `rootName`) currently return an empty list —
/// we only enumerate values for columns whose distinct set is cheap to
/// compute via the metadata-scan path.
///
/// # Errors
///
/// Returns a [`TempoError`] wrapping the underlying metadata-scan or
/// iceberg error.
pub async fn list_tag_values(
    state: &TempoState,
    tenant_id: &str,
    tag_name: &str,
    start: Option<i64>,
    end: Option<i64>,
    limit: usize,
) -> TempoResult<Vec<String>> {
    let (start_dt, end_dt) = resolve_window(start, end);
    let table = load_spans_table(state).await?;

    let tag = TagRef::parse(tag_name);
    let mut values: BTreeSet<String> = BTreeSet::new();

    match tag {
        TagRef::Intrinsic(name) => match name {
            "name" => {
                values = scan_values_either(&table, tenant_id, start_dt, end_dt, COL_NAME).await?;
            }
            "rootServiceName" => {
                values = scan_values_either(&table, tenant_id, start_dt, end_dt, COL_SERVICE_NAME).await?;
            }
            "traceID" => {
                values = scan_values_either(&table, tenant_id, start_dt, end_dt, COL_TRACE_ID).await?;
            }
            "spanID" => {
                values = scan_values_either(&table, tenant_id, start_dt, end_dt, COL_SPAN_ID).await?;
            }
            // Intrinsics that we can't cheaply enumerate (numeric,
            // derived). Return empty — still 200, keeps Grafana happy.
            _ => {}
        },
        TagRef::Scoped {
            scope: Scope::Resource,
            key,
        } => {
            let mapped = map_attribute_to_column(key);
            values = metadata_scan::scan_label_values(
                &table,
                tenant_id,
                start_dt,
                end_dt,
                &SPANS_VALUES_RESOURCE_CONFIG,
                mapped.unwrap_or(key),
                Predicate::AlwaysTrue,
            )
            .await
            .map_err(|e| TempoError(QueryError::from(e)))?;
        }
        TagRef::Scoped {
            scope: Scope::Span,
            key,
        } => {
            let mapped = map_attribute_to_column(key);
            values = metadata_scan::scan_label_values(
                &table,
                tenant_id,
                start_dt,
                end_dt,
                &SPANS_VALUES_SPAN_CONFIG,
                mapped.unwrap_or(key),
                Predicate::AlwaysTrue,
            )
            .await
            .map_err(|e| TempoError(QueryError::from(e)))?;
        }
        TagRef::UnscopedAttribute(key) => {
            // `.foo` — scope underspecified. Union values from both maps.
            let mapped = map_attribute_to_column(key).unwrap_or(key);
            let span_values = metadata_scan::scan_label_values(
                &table,
                tenant_id,
                start_dt,
                end_dt,
                &SPANS_VALUES_SPAN_CONFIG,
                mapped,
                Predicate::AlwaysTrue,
            )
            .await
            .map_err(|e| TempoError(QueryError::from(e)))?;
            let res_values = metadata_scan::scan_label_values(
                &table,
                tenant_id,
                start_dt,
                end_dt,
                &SPANS_VALUES_RESOURCE_CONFIG,
                mapped,
                Predicate::AlwaysTrue,
            )
            .await
            .map_err(|e| TempoError(QueryError::from(e)))?;
            values.extend(span_values);
            values.extend(res_values);
        }
        TagRef::Scoped {
            scope: Scope::Event | Scope::Link | Scope::Intrinsic,
            ..
        } => {
            // Nested event/link attrs unsupported for now. A scope of
            // `intrinsic` with a dotted key is nonsensical — return empty.
        }
    }

    let mut out: Vec<String> = values.into_iter().collect();
    out.truncate(limit);
    Ok(out)
}

/// Enumerate distinct values for an indexed top-level column. The column
/// lives outside either map, so the same query works for both configs —
/// we use the span-side config arbitrarily.
async fn scan_values_either(
    table: &iceberg::table::Table,
    tenant_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    column: &str,
) -> TempoResult<BTreeSet<String>> {
    metadata_scan::scan_label_values(
        table,
        tenant_id,
        start,
        end,
        &SPANS_VALUES_SPAN_CONFIG,
        column,
        Predicate::AlwaysTrue,
    )
    .await
    .map_err(|e| TempoError(QueryError::from(e)))
}

/// Map a dotted `OTel` attribute key to a top-level column, when one exists.
///
/// `service.name` → `service_name`; `cloud.account.id` → `cloud_account_id`.
/// Unmapped keys go through as-is and fall back to the MAP lookup path.
fn map_attribute_to_column(key: &str) -> Option<&'static str> {
    match key {
        "service.name" => Some(COL_SERVICE_NAME),
        "cloud.account.id" => Some(COL_CLOUD_ACCOUNT_ID),
        "trace.id" => Some(COL_TRACE_ID),
        "span.id" => Some(COL_SPAN_ID),
        "parent.span.id" => Some(COL_PARENT_SPAN_ID),
        _ => None,
    }
}

/// Check whether a named column exists in the spans table schema.
fn table_has_column(table: &iceberg::table::Table, col: &str) -> bool {
    table.metadata().current_schema().field_by_name(col).is_some()
}

/// Resolve the `[start, end]` window from optional Unix-epoch-second
/// parameters.
fn resolve_window(start: Option<i64>, end: Option<i64>) -> (DateTime<Utc>, DateTime<Utc>) {
    let now = Utc::now();
    let end_dt = end.and_then(|s| Utc.timestamp_opt(s, 0).single()).unwrap_or(now);
    let start_dt = start
        .and_then(|s| Utc.timestamp_opt(s, 0).single())
        .unwrap_or_else(|| end_dt - chrono::Duration::hours(DEFAULT_LOOKBACK_HOURS));
    (start_dt, end_dt)
}

/// Load the `spans` Iceberg table via the query engine's catalog.
async fn load_spans_table(state: &TempoState) -> TempoResult<iceberg::table::Table> {
    let ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])
        .map_err(|e| TempoError(QueryError::Iceberg(e)))?;
    state
        .engine
        .catalog()
        .load_table(&ident)
        .await
        .map_err(|e| TempoError(QueryError::Iceberg(e)))
}

#[cfg(test)]
mod tests {
    use super::{map_attribute_to_column, resolve_window};

    #[test]
    fn map_attribute_maps_service_name() {
        assert_eq!(map_attribute_to_column("service.name"), Some("service_name"));
        assert_eq!(map_attribute_to_column("trace.id"), Some("trace_id"));
    }

    #[test]
    fn map_attribute_passes_through_unknown() {
        assert_eq!(map_attribute_to_column("http.method"), None);
    }

    #[test]
    fn resolve_window_defaults_to_six_hour_lookback() {
        let (start, end) = resolve_window(None, None);
        let diff = end - start;
        assert_eq!(diff.num_hours(), 6);
    }

    #[test]
    fn resolve_window_respects_explicit_start_end() {
        let (start, end) = resolve_window(Some(100), Some(200));
        assert_eq!(start.timestamp(), 100);
        assert_eq!(end.timestamp(), 200);
    }
}
