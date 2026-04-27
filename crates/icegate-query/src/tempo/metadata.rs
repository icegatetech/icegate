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
    COL_SPAN_ID, COL_TRACE_ID, SPAN_INDEXED_ATTRIBUTE_COLUMNS, TRACEQL_INTRINSIC_TAGS,
};
use icegate_common::schema::{COL_KIND, COL_STATUS_CODE};
use icegate_common::{ICEGATE_NAMESPACE, SPANS_TABLE};

use super::error::{TempoError, TempoResult};
use super::models::{Scope, ScopeGroup, TagRef, TagValueV2};
use super::server::TempoState;
use crate::engine::metadata_scan::{self, MetadataScanConfig};
use crate::error::QueryError;

/// Type token applied to free-form string attribute values in the v2
/// tag-values response.
const VALUE_TYPE_STRING: &str = "string";
/// Type token applied to closed-set enum values (`status`, `kind`) in the
/// v2 tag-values response. Grafana renders `keyword`-typed values as a
/// fixed dropdown rather than a free-text input.
const VALUE_TYPE_KEYWORD: &str = "keyword";

/// Top-level resource columns surfaced under the `resource` scope as
/// `OTel`-dotted tag names. Each entry maps the user-facing `TraceQL` /
/// `OTel` attribute name to the underlying physical column. Only entries
/// whose column is present in the table schema are emitted.
///
/// This is what makes `service.name` appear in tag discovery rather than
/// the physical column name `service_name`. The reverse mapping
/// (`OTel` name → column) used by tag-value lookup lives in
/// [`map_attribute_to_column`].
const RESOURCE_TOP_LEVEL_TAGS: &[(&str, &str)] = &[
    ("service.name", COL_SERVICE_NAME),
    ("cloud.account.id", COL_CLOUD_ACCOUNT_ID),
];

/// Metadata-scan config for the `resource_attributes` map.
///
/// `indexed_columns` is empty here — resource-scope top-level columns are
/// handled separately in `list_tags_v2` so we can emit them under their
/// `OTel`-dotted names ([`RESOURCE_TOP_LEVEL_TAGS`]) instead of the
/// physical `snake_case` column names that the metadata-scan layer would
/// otherwise surface.
const SPANS_RESOURCE_CONFIG: MetadataScanConfig = MetadataScanConfig {
    indexed_columns: &[],
    label_aliases: &[],
    excluded_map_keys: &[],
    map_column: COL_RESOURCE_ATTRIBUTES,
};

/// Metadata-scan config for the `span_attributes` map.
///
/// `indexed_columns` is empty by design: the only top-level string columns
/// on the spans table (`cloud_account_id`, `service_name`, `name`) belong
/// to the `resource` scope or are exposed as `TraceQL` intrinsics — they
/// must not leak into `span` scope under their physical column names.
const SPANS_SPAN_CONFIG: MetadataScanConfig = MetadataScanConfig {
    indexed_columns: &[],
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
/// `extra_predicate` is AND'd with the tenant+time base predicate by the
/// metadata-scan layer; pass [`Predicate::AlwaysTrue`] to disable. The
/// over-approximation contract applies — non-pushdownable `TraceQL`
/// clauses are dropped upstream and only widen the candidate row-group
/// set.
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
    extra_predicate: Predicate,
) -> TempoResult<Vec<String>> {
    let v2 = list_tags_v2(state, tenant_id, start, end, None, extra_predicate).await?;
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
/// `extra_predicate` is forwarded to every metadata-scan call so file
/// pruning and row-group pruning can fire on indexed columns (e.g.
/// `service_name`, `status_code`).
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
    extra_predicate: Predicate,
) -> TempoResult<super::models::TagsV2Response> {
    let (start_dt, end_dt) = resolve_window(start, end)?;
    let table = load_spans_table(state).await?;

    let mut groups: Vec<ScopeGroup> = Vec::with_capacity(5);

    // Resource scope: resource_attributes map keys + top-level resource
    // columns that exist in the schema.
    if scope_filter.is_none_or(|s| s == Scope::Resource) {
        let mut tags: BTreeSet<String> = metadata_scan::scan_labels(
            &table,
            tenant_id,
            start_dt,
            end_dt,
            &SPANS_RESOURCE_CONFIG,
            extra_predicate.clone(),
        )
        .await
        .map_err(|e| TempoError(QueryError::from(e)))?;
        for (otel_name, col) in RESOURCE_TOP_LEVEL_TAGS {
            if table_has_column(&table, col) {
                tags.insert((*otel_name).to_string());
            }
        }
        groups.push(ScopeGroup {
            name: Scope::Resource.as_str().to_string(),
            tags: tags.into_iter().collect(),
        });
    }

    // Span scope: span_attributes map keys + span-specific indexed columns
    // (name).
    if scope_filter.is_none_or(|s| s == Scope::Span) {
        let tags: BTreeSet<String> = metadata_scan::scan_labels(
            &table,
            tenant_id,
            start_dt,
            end_dt,
            &SPANS_SPAN_CONFIG,
            extra_predicate.clone(),
        )
        .await
        .map_err(|e| TempoError(QueryError::from(e)))?;
        groups.push(ScopeGroup {
            name: Scope::Span.as_str().to_string(),
            tags: tags.into_iter().collect(),
        });
    }

    if scope_filter.is_none_or(|s| s == Scope::Intrinsic) {
        groups.push(ScopeGroup {
            name: Scope::Intrinsic.as_str().to_string(),
            tags: TRACEQL_INTRINSIC_TAGS.iter().map(|s| (*s).to_string()).collect(),
        });
    }

    // event and link attributes require unnesting nested LIST<STRUCT> maps —
    // deferred. Return empty lists so Grafana's query builder still shows
    // the sections.
    if scope_filter.is_none_or(|s| s == Scope::Event) {
        groups.push(ScopeGroup {
            name: Scope::Event.as_str().to_string(),
            tags: Vec::new(),
        });
    }
    if scope_filter.is_none_or(|s| s == Scope::Link) {
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
/// compute via the metadata-scan path. Status/kind enums are surfaced
/// via the typed [`list_tag_values_v2`] path instead.
///
/// `extra_predicate` is forwarded to every metadata-scan call so indexed
/// `TraceQL` selectors (e.g. `q={ resource.service.name = "x" }`) prune
/// the candidate file/row-group set.
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
    extra_predicate: Predicate,
) -> TempoResult<Vec<String>> {
    let (start_dt, end_dt) = resolve_window(start, end)?;
    let table = load_spans_table(state).await?;

    let tag = TagRef::parse(tag_name);
    let mut values: BTreeSet<String> = BTreeSet::new();

    match tag {
        TagRef::Intrinsic(name) => match name {
            "name" | "span:name" => {
                values = scan_values_either(&table, tenant_id, start_dt, end_dt, COL_NAME, extra_predicate).await?;
            }
            "rootServiceName" | "trace:rootService" => {
                values =
                    scan_values_either(&table, tenant_id, start_dt, end_dt, COL_SERVICE_NAME, extra_predicate).await?;
            }
            // `trace:id` / `span:id` (and the legacy `traceID` /
            // `spanID`) are intentionally NOT enumerated. Per Tempo
            // convention they are omitted from tag discovery and
            // `/tag/{name}/values` returns the empty set — enumerating
            // the distinct set of per-trace / per-span identifiers is
            // both useless (every row contributes a unique value) and
            // expensive. They remain queryable in TraceQL via the
            // parser; predicate pushdown is handled by
            // `target_column_for_tag`.
            //
            // Other intrinsics that we can't cheaply enumerate
            // (numeric: `duration`, `traceDuration`, `span:duration`,
            // `trace:duration`; derived: `rootName`, `trace:rootName`)
            // also fall through here. Return empty — still 200, keeps
            // Grafana happy.
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
                extra_predicate,
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
                extra_predicate,
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
                extra_predicate.clone(),
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
                extra_predicate,
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

/// Enumerate distinct values for a single tag and return them in the
/// `v2` typed shape (`{type, value}` per entry).
///
/// Reuses [`list_tag_values`] for string-typed lookups and adds typed
/// handling for the closed-enum intrinsics (`status`, `kind`) — these
/// scan the corresponding INT column for distinct codes actually
/// present in the time window (with `extra_predicate` applied) and map
/// each code back to its `OTel` spelling. Numeric and derived intrinsics
/// other than `status` / `kind` still return an empty list.
///
/// # Errors
///
/// Returns a [`TempoError`] wrapping the underlying metadata-scan or
/// iceberg error.
pub async fn list_tag_values_v2(
    state: &TempoState,
    tenant_id: &str,
    tag_name: &str,
    start: Option<i64>,
    end: Option<i64>,
    limit: usize,
    extra_predicate: Predicate,
) -> TempoResult<Vec<TagValueV2>> {
    // Status / kind are closed enums that live in INT columns
    // (`status_code`, `kind`). Scan the column dictionary for codes
    // actually present in the window — empty result is intentional
    // (e.g. no `error` rows in the window means no `error` in the
    // dropdown).
    if let TagRef::Intrinsic(name) = TagRef::parse(tag_name) {
        if let Some(values) =
            enum_intrinsic_values_dynamic(state, tenant_id, name, start, end, extra_predicate.clone()).await?
        {
            return Ok(values.into_iter().take(limit).collect());
        }
    }

    let strings = list_tag_values(state, tenant_id, tag_name, start, end, limit, extra_predicate).await?;
    Ok(strings
        .into_iter()
        .map(|value| TagValueV2 {
            value_type: VALUE_TYPE_STRING,
            value,
        })
        .collect())
}

/// Dynamic value set for the closed-enum `TraceQL` intrinsics
/// (`status`, `kind`). Reads the underlying INT32 column via
/// [`metadata_scan::scan_label_int_values`] — which combines Parquet
/// dictionary pages, row-group statistics min/max, and null-count —
/// then maps each surviving OTLP code to its canonical spelling.
///
/// Returns `Ok(None)` for any other intrinsic so callers fall through
/// to the string-value path. Returns `Ok(Some(empty))` when nothing
/// in the window has a status/kind set — Grafana then shows an empty
/// dropdown, matching the requested behaviour: the dropdown reflects
/// data, not the static OTLP enum.
///
/// **NULL handling**: IceGate's spans-ingest writes both OTLP
/// `Status.code = 0` (Unset) and the absent-status case as Parquet
/// NULL (see `crates/icegate-ingest/src/transform.rs::498-521`); same
/// for `SpanKind::Unspecified`. The metadata-scan layer surfaces
/// observed nulls back as the canonical code `0`, so traces with no
/// explicit status still produce an `unset` entry in the dropdown.
async fn enum_intrinsic_values_dynamic(
    state: &TempoState,
    tenant_id: &str,
    intrinsic: &str,
    start: Option<i64>,
    end: Option<i64>,
    extra_predicate: Predicate,
) -> TempoResult<Option<Vec<TagValueV2>>> {
    let (column, code_to_name): (&str, fn(i32) -> Option<&'static str>) = match intrinsic {
        "status" | "span:status" => (COL_STATUS_CODE, status_code_to_str),
        "kind" | "span:kind" => (COL_KIND, kind_code_to_str),
        _ => return Ok(None),
    };
    let (start_dt, end_dt) = resolve_window(start, end)?;
    let table = load_spans_table(state).await?;

    let codes = metadata_scan::scan_label_int_values(&table, tenant_id, start_dt, end_dt, column, extra_predicate)
        .await
        .map_err(|e| TempoError(QueryError::from(e)))?;

    let mut out: Vec<TagValueV2> = Vec::with_capacity(codes.len());
    for code in codes {
        if let Some(name) = code_to_name(code) {
            out.push(TagValueV2 {
                value_type: VALUE_TYPE_KEYWORD,
                value: name.to_string(),
            });
        } else {
            // Schema drift: code seen in data is outside the OTLP enum
            // we know about. Don't render it as a raw integer in a
            // keyword-typed dropdown — drop with a warn so operators
            // notice if the spec grows.
            tracing::warn!(intrinsic, code, "unknown OTLP code in tag-value enumeration");
        }
    }
    Ok(Some(out))
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
    extra_predicate: Predicate,
) -> TempoResult<BTreeSet<String>> {
    metadata_scan::scan_label_values(
        table,
        tenant_id,
        start,
        end,
        &SPANS_VALUES_SPAN_CONFIG,
        column,
        extra_predicate,
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

/// Resolve the underlying physical column being enumerated by a
/// `tag-values` request. Used by the handlers to drop self-referential
/// conjuncts from the `q` pushdown — see
/// [`crate::traceql::translate_query_to_predicate_excluding`].
///
/// Returns `None` for tags that live in MAP attribute columns (e.g.
/// arbitrary span attributes) — there's no top-level column to
/// self-reference, so the standard pushdown rules apply unchanged.
#[must_use]
pub fn target_column_for_tag(tag_name: &str) -> Option<&'static str> {
    match TagRef::parse(tag_name) {
        TagRef::Intrinsic(name) => match name {
            "name" | "span:name" => Some(COL_NAME),
            "rootServiceName" | "trace:rootService" => Some(COL_SERVICE_NAME),
            // `traceID` / `spanID` kept as aliases for backward compat
            // with older Grafana clients still emitting the legacy
            // camelCase intrinsics; canonical modern spelling is the
            // colon form.
            "traceID" | "trace:id" => Some(COL_TRACE_ID),
            "spanID" | "span:id" => Some(COL_SPAN_ID),
            "status" | "span:status" => Some(COL_STATUS_CODE),
            "kind" | "span:kind" => Some(COL_KIND),
            _ => None,
        },
        TagRef::Scoped {
            scope: Scope::Resource | Scope::Span,
            key,
        }
        | TagRef::UnscopedAttribute(key) => map_attribute_to_column(key),
        TagRef::Scoped {
            scope: Scope::Event | Scope::Link | Scope::Intrinsic,
            ..
        } => None,
    }
}

/// Map an OTLP `Status.code` integer back to its canonical `TraceQL`
/// spelling. Inverse of [`crate::traceql::common::StatusValue::otlp_code`].
/// Returns `None` for codes outside the spec — callers drop those rather
/// than render an integer in a keyword-typed dropdown.
const fn status_code_to_str(code: i32) -> Option<&'static str> {
    match code {
        0 => Some("unset"),
        1 => Some("ok"),
        2 => Some("error"),
        _ => None,
    }
}

/// Map an OTLP `SpanKind` integer back to its canonical `TraceQL`
/// spelling. Inverse of [`crate::traceql::common::KindValue::otlp_code`].
/// Returns `None` for codes outside the spec.
const fn kind_code_to_str(code: i32) -> Option<&'static str> {
    match code {
        0 => Some("unspecified"),
        1 => Some("internal"),
        2 => Some("server"),
        3 => Some("client"),
        4 => Some("producer"),
        5 => Some("consumer"),
        _ => None,
    }
}

/// Check whether a named column exists in the spans table schema.
fn table_has_column(table: &iceberg::table::Table, col: &str) -> bool {
    table.metadata().current_schema().field_by_name(col).is_some()
}

/// Resolve the `[start, end]` window from optional Unix-epoch-second
/// parameters and validate it against the server-side caps.
///
/// Defaults when params are missing — Grafana's tag-discovery calls
/// commonly omit both:
/// - `end` missing → `end = now`.
/// - `start` missing → `start = end - DEFAULT_LOOKBACK_HOURS` (6h
///   today). When end is also missing this becomes `now - 6h`,
///   producing a `[now-6h, now]` window — bounded enough that the
///   metadata-scan pruning still has work to do.
///
/// The effective window is recorded on the current tracing span so
/// operators can verify what was actually scanned, regardless of what
/// the client sent. A `debug` log fires whenever a default is
/// applied.
///
/// # Errors
///
/// Returns [`TempoError`] (HTTP 400) when the resolved window is
/// inverted or wider than [`super::validation::MAX_QUERY_WINDOW`].
fn resolve_window(start: Option<i64>, end: Option<i64>) -> TempoResult<(DateTime<Utc>, DateTime<Utc>)> {
    let now = Utc::now();
    let end_dt = end.and_then(|s| Utc.timestamp_opt(s, 0).single()).unwrap_or(now);
    let start_dt = start
        .and_then(|s| Utc.timestamp_opt(s, 0).single())
        .unwrap_or_else(|| end_dt - chrono::Duration::hours(DEFAULT_LOOKBACK_HOURS));
    super::validation::validate_query_window(start_dt, end_dt).map_err(TempoError::new)?;

    // Surface the resolved window to the calling handler's tracing
    // span so dashboards/log searches see what was actually scanned,
    // not just what the client sent. Handler instrument! macros
    // declare these fields as `tracing::field::Empty`.
    let span = tracing::Span::current();
    span.record("effective_start", start_dt.timestamp());
    span.record("effective_end", end_dt.timestamp());
    if start.is_none() || end.is_none() {
        tracing::debug!(
            start_explicit = start.is_some(),
            end_explicit = end.is_some(),
            effective_start = start_dt.timestamp(),
            effective_end = end_dt.timestamp(),
            default_lookback_hours = DEFAULT_LOOKBACK_HOURS,
            "applied default tag-discovery window",
        );
    }

    Ok((start_dt, end_dt))
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
    use super::{kind_code_to_str, map_attribute_to_column, resolve_window, status_code_to_str};
    use crate::traceql::common::{KindValue, StatusValue};

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
    fn status_code_to_str_round_trips_otlp_codes() {
        for v in [StatusValue::Unset, StatusValue::Ok, StatusValue::Error] {
            // Round-trip via the OTel code → spelling helper. Spelling
            // must match what `traceql::common::StatusValue` would print.
            let spelling = status_code_to_str(v.otlp_code()).expect("known code");
            let expected = match v {
                StatusValue::Ok => "ok",
                StatusValue::Error => "error",
                StatusValue::Unset => "unset",
            };
            assert_eq!(spelling, expected);
        }
    }

    #[test]
    fn kind_code_to_str_round_trips_otlp_codes() {
        for v in [
            KindValue::Unspecified,
            KindValue::Internal,
            KindValue::Server,
            KindValue::Client,
            KindValue::Producer,
            KindValue::Consumer,
        ] {
            let spelling = kind_code_to_str(v.otlp_code()).expect("known code");
            let expected = match v {
                KindValue::Unspecified => "unspecified",
                KindValue::Internal => "internal",
                KindValue::Server => "server",
                KindValue::Client => "client",
                KindValue::Producer => "producer",
                KindValue::Consumer => "consumer",
            };
            assert_eq!(spelling, expected);
        }
    }

    #[test]
    fn status_code_to_str_unknown_is_none() {
        assert_eq!(status_code_to_str(99), None);
        assert_eq!(status_code_to_str(-1), None);
    }

    #[test]
    fn kind_code_to_str_unknown_is_none() {
        assert_eq!(kind_code_to_str(99), None);
        assert_eq!(kind_code_to_str(-1), None);
    }

    #[test]
    fn resolve_window_defaults_to_six_hour_lookback() {
        let (start, end) = resolve_window(None, None).expect("default window must validate");
        let diff = end - start;
        assert_eq!(diff.num_hours(), 6);
    }

    #[test]
    fn resolve_window_respects_explicit_start_end() {
        let (start, end) = resolve_window(Some(100), Some(200)).expect("explicit window must validate");
        assert_eq!(start.timestamp(), 100);
        assert_eq!(end.timestamp(), 200);
    }

    #[test]
    fn resolve_window_rejects_inverted_range() {
        let err = resolve_window(Some(200), Some(100)).expect_err("inverted window must reject");
        assert!(matches!(err.0, crate::error::QueryError::Validation(_)));
    }

    #[test]
    fn resolve_window_rejects_oversize_range() {
        // 31-day window — wider than `MAX_QUERY_WINDOW = 30d`.
        let now = chrono::Utc::now().timestamp();
        let start = now - chrono::Duration::days(31).num_seconds();
        let err = resolve_window(Some(start), Some(now)).expect_err("oversize window must reject");
        assert!(matches!(err.0, crate::error::QueryError::Validation(_)));
    }
}
