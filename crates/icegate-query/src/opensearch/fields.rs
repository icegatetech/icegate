//! `_field_caps`, `_mapping` and `_mapping/field`: field discovery for index patterns, field
//! pickers and time-field checks.
//!
//! An expression reads data indices or system indices, never both. A system index's fields come
//! from the mapping it was created with: `_field_caps` derives them from it, `_mapping` answers it
//! as stored, and `_mapping/field` is not served for it.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};

use axum::Json;
use axum::extract::{Extension, State};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use futures::future::try_join_all;
use icegate_common::TenantId;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::catalog::{
    FieldListing, FieldType, IndexEntry, IndexExpression, IndexField, IndexTarget, TenantSession, read_index_options,
};
use super::error::OpenSearchError;
use super::extract::{EndpointParameters, Parameters, PathParameters, RequestBody, deserialize_flag};
use super::glob::matches_glob;
use super::state::OpenSearchState;
use super::system_index::{IndexKeyedAnswer, MappedField, SystemSnapshot};

/// The metadata fields `_field_caps` reports beside an index's own fields, as
/// `(name, searchable, aggregatable)`; each is typed with its own name. Exactly these three, each
/// flagged with what IceGate serves: `_id` is searchable through `ids` queries and not
/// aggregatable; `_index` and `_source` are neither. No other metadata field is reported, so no
/// client is offered a capability IceGate would refuse.
const METADATA_FIELD_CAPABILITIES: [(&str, bool, bool); 3] = [
    ("_id", true, false),
    ("_index", false, false),
    ("_source", false, false),
];

/// What `_field_caps` reads of one field of one index, whether a table's column or a system
/// index's stored mapping declares it.
trait CapabilityField {
    /// The field's name.
    fn name(&self) -> &str;
    /// The type `_field_caps` reports.
    fn type_name(&self) -> &str;
    /// Whether a query can match on the field.
    fn is_searchable(&self) -> bool;
    /// Whether an aggregation can use the field.
    fn is_aggregatable(&self) -> bool;
}

impl CapabilityField for IndexField {
    fn name(&self) -> &str {
        &self.name
    }

    fn type_name(&self) -> &str {
        self.field_type.name()
    }

    fn is_searchable(&self) -> bool {
        self.is_searchable
    }

    fn is_aggregatable(&self) -> bool {
        self.is_aggregatable
    }
}

impl CapabilityField for MappedField {
    fn name(&self) -> &str {
        &self.name
    }

    fn type_name(&self) -> &str {
        &self.type_name
    }

    fn is_searchable(&self) -> bool {
        self.is_searchable
    }

    fn is_aggregatable(&self) -> bool {
        self.is_aggregatable
    }
}

/// Query parameters of `_field_caps`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FieldCapabilitiesParameters {
    /// Comma-separated field patterns; absent means every field.
    fields: Option<String>,
    /// Skip a missing concrete index instead of answering `404`; absent means `404`.
    #[serde(default, deserialize_with = "deserialize_flag")]
    ignore_unavailable: Option<bool>,
    /// Answer an empty result when nothing matches instead of `404`; absent means the empty
    /// result.
    #[serde(default, deserialize_with = "deserialize_flag")]
    allow_no_indices: Option<bool>,
    /// What a wildcard of the expression expands to; absent means open indices.
    expand_wildcards: Option<String>,
}

impl EndpointParameters for FieldCapabilitiesParameters {
    const IGNORED: &'static [&'static str] = &[];
}

/// Query parameters of `_mapping` and `_mapping/field`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MappingParameters {
    /// Skip a missing concrete index instead of answering `404`; absent means `404`.
    #[serde(default, deserialize_with = "deserialize_flag")]
    ignore_unavailable: Option<bool>,
    /// Answer an empty result when nothing matches instead of `404`; absent means the empty
    /// result.
    #[serde(default, deserialize_with = "deserialize_flag")]
    allow_no_indices: Option<bool>,
    /// What a wildcard of the expression expands to; absent means open indices.
    expand_wildcards: Option<String>,
}

impl EndpointParameters for MappingParameters {
    const IGNORED: &'static [&'static str] =
        &["local", "include_defaults", "cluster_manager_timeout", "master_timeout"];
}

/// The fields of every entry of `entries`, in their order, all listed at the same request time
/// `now`.
async fn list_fields_per_index(
    session: &TenantSession,
    entries: Vec<&IndexEntry>,
    now: DateTime<Utc>,
    listing: FieldListing,
) -> Result<Vec<(String, Vec<IndexField>)>, OpenSearchError> {
    try_join_all(entries.into_iter().map(|entry| async move {
        Ok::<_, OpenSearchError>((
            entry.name().to_string(),
            session.list_fields(entry, now, listing).await?,
        ))
    }))
    .await
}

/// The fields every system index of `indices` declares in its stored mapping, in their order.
///
/// # Errors
///
/// Returns `index_not_found_exception` for an index the snapshot does not hold.
fn list_system_fields_per_index(
    snapshot: &SystemSnapshot,
    indices: Vec<String>,
) -> Result<Vec<(String, Vec<MappedField>)>, OpenSearchError> {
    indices
        .into_iter()
        .map(|index| {
            let fields = snapshot
                .find_index(&index)
                .ok_or_else(|| OpenSearchError::index_not_found(&index))?
                .list_mapped_fields();
            Ok((index, fields))
        })
        .collect()
}

/// Whether answering `patterns` needs MAP keys. A MAP key's field name is `<column>.<key>`, so
/// only a pattern holding a wildcard or a dot can name one; any other pattern names a column or a
/// metadata field, and listing the columns enumerates nothing.
fn choose_field_listing(patterns: &[&str]) -> FieldListing {
    if patterns.iter().any(|pattern| pattern.contains(['*', '?', '.'])) {
        FieldListing::WithMapKeys
    } else {
        FieldListing::ColumnsOnly
    }
}

/// `GET|POST [/<index>]/_field_caps`. Both methods answer the same, and an absent body and `{}`
/// are the same request. A system index reports the fields of its stored mapping.
pub(crate) async fn describe_field_capabilities(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    index: Option<PathParameters<String>>,
    Parameters(parameters): Parameters<FieldCapabilitiesParameters>,
    RequestBody(body): RequestBody,
) -> Result<Json<Value>, OpenSearchError> {
    refuse_body_keys(&body)?;
    let options = read_index_options(
        parameters.ignore_unavailable,
        parameters.allow_no_indices,
        parameters.expand_wildcards.as_deref(),
    )?;
    let expression_text = index.map(|PathParameters(text)| text).unwrap_or_default();
    let patterns: Vec<&str> = parameters.fields.as_deref().unwrap_or("*").split(',').collect();
    let expression = IndexExpression::parse(&expression_text)?;
    let session = state.catalog.open_session(&tenant).await?;
    let rendered_capabilities = match session.resolve_target(&expression, options).await? {
        IndexTarget::Data(entries) => {
            let fields_by_index =
                list_fields_per_index(&session, entries, Utc::now(), choose_field_listing(&patterns)).await?;
            render_field_capabilities(&fields_by_index, &patterns)
        }
        IndexTarget::System(indices) => {
            let fields_by_index =
                list_system_fields_per_index(&session.read_system_snapshot().await?.snapshot, indices)?;
            render_field_capabilities(&fields_by_index, &patterns)
        }
    };
    Ok(Json(rendered_capabilities))
}

/// Accept an empty body or `{}`; refuse any key (such as `index_filter`), since ignoring it would
/// answer a different question than the one asked.
fn refuse_body_keys(body: &[u8]) -> Result<(), OpenSearchError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(());
    }
    let object: Map<String, Value> = serde_json::from_slice(body)
        .map_err(|error| OpenSearchError::parsing(format!("request body is not a JSON object: {error}")))?;
    if let Some(key) = object.keys().next() {
        return Err(OpenSearchError::parsing(format!("request does not support [{key}]")));
    }
    Ok(())
}

/// Render `_field_caps` for `fields_by_index` (in index-name order), listing only the fields one
/// of `patterns` matches, the metadata fields included.
fn render_field_capabilities<F: CapabilityField>(fields_by_index: &[(String, Vec<F>)], patterns: &[&str]) -> Value {
    let is_requested = |name: &str| patterns.iter().any(|pattern| matches_glob(pattern, name));
    let mut members_by_field: BTreeMap<&str, BTreeMap<&str, Vec<(&str, &F)>>> = BTreeMap::new();
    for (index, fields) in fields_by_index {
        for field in fields.iter().filter(|field| is_requested(field.name())) {
            members_by_field
                .entry(field.name())
                .or_default()
                .entry(field.type_name())
                .or_default()
                .push((index.as_str(), field));
        }
    }
    let mut rendered_fields = Map::new();
    for (name, members_by_type) in members_by_field {
        let has_type_conflict = members_by_type.len() > 1;
        let rendered_capabilities: Map<String, Value> = members_by_type
            .into_iter()
            .map(|(field_type, members)| {
                (
                    field_type.to_string(),
                    render_capability(field_type, &members, has_type_conflict),
                )
            })
            .collect();
        rendered_fields.insert(name.to_string(), Value::Object(rendered_capabilities));
    }
    for (name, is_searchable, is_aggregatable) in METADATA_FIELD_CAPABILITIES {
        if is_requested(name) {
            rendered_fields.insert(
                name.to_string(),
                json!({ name: {"type": name, "searchable": is_searchable, "aggregatable": is_aggregatable} }),
            );
        }
    }
    let mut body = Map::new();
    body.insert(
        "indices".to_string(),
        fields_by_index.iter().map(|(index, _)| index.as_str()).collect(),
    );
    body.insert("fields".to_string(), Value::Object(rendered_fields));
    Value::Object(body)
}

/// One type's capability entry. `indices` is listed only on a type conflict; the
/// `non_searchable_indices` / `non_aggregatable_indices` lists only when the indices disagree.
fn render_capability<F: CapabilityField>(field_type: &str, members: &[(&str, &F)], has_type_conflict: bool) -> Value {
    let is_searchable = members.iter().all(|(_, field)| field.is_searchable());
    let is_aggregatable = members.iter().all(|(_, field)| field.is_aggregatable());
    let mut capability = json!({"type": field_type, "searchable": is_searchable, "aggregatable": is_aggregatable});
    if has_type_conflict {
        capability["indices"] = members.iter().map(|(index, _)| *index).collect();
    }
    if !is_searchable && members.iter().any(|(_, field)| field.is_searchable()) {
        capability["non_searchable_indices"] = members
            .iter()
            .filter(|(_, field)| !field.is_searchable())
            .map(|(index, _)| *index)
            .collect();
    }
    if !is_aggregatable && members.iter().any(|(_, field)| field.is_aggregatable()) {
        capability["non_aggregatable_indices"] = members
            .iter()
            .filter(|(_, field)| !field.is_aggregatable())
            .map(|(index, _)| *index)
            .collect();
    }
    capability
}

/// A system index as `_mapping` describes it.
#[derive(Serialize)]
struct StoredMappings<'a> {
    /// The mappings as stored.
    mappings: &'a Value,
}

/// `GET [/<index>]/_mapping`: `{<index>: {mappings: {properties}}}` per selected data index, and
/// `{<index>: {mappings: <stored mapping>}}` per selected system index, its mapping as stored.
pub(crate) async fn describe_mappings(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    index: Option<PathParameters<String>>,
    Parameters(parameters): Parameters<MappingParameters>,
) -> Result<Response, OpenSearchError> {
    let options = read_index_options(
        parameters.ignore_unavailable,
        parameters.allow_no_indices,
        parameters.expand_wildcards.as_deref(),
    )?;
    let expression_text = index.map(|PathParameters(text)| text).unwrap_or_default();
    let expression = IndexExpression::parse(&expression_text)?;
    let session = state.catalog.open_session(&tenant).await?;
    match session.resolve_target(&expression, options).await? {
        IndexTarget::Data(entries) => {
            let body: Map<String, Value> =
                list_fields_per_index(&session, entries, Utc::now(), FieldListing::WithMapKeys)
                    .await?
                    .into_iter()
                    .map(|(index, fields)| {
                        let properties = Value::Object(render_properties(&fields)?);
                        Ok((index, build_object("mappings", build_object("properties", properties))))
                    })
                    .collect::<Result<_, OpenSearchError>>()?;
            Ok(Json(Value::Object(body)).into_response())
        }
        IndexTarget::System(indices) => {
            let snapshot = &session.read_system_snapshot().await?.snapshot;
            let descriptions = indices
                .iter()
                .map(|index| {
                    let system_index = snapshot
                        .find_index(index)
                        .ok_or_else(|| OpenSearchError::index_not_found(index))?;
                    Ok((
                        index.as_str(),
                        StoredMappings {
                            mappings: &system_index.mappings,
                        },
                    ))
                })
                .collect::<Result<Vec<_>, OpenSearchError>>()?;
            Ok(Json(IndexKeyedAnswer {
                descriptions: &descriptions,
            })
            .into_response())
        }
    }
}

/// Insert the mapping of type `field_type` at the key path `keys` below `properties`, each
/// missing ancestor as an `object`. A mapping gains `properties` only when a child is inserted
/// below it, so a leaf or an empty object carries its type alone. `properties` is a
/// `serde_json::Map`, whose `preserve_order` backing finds a key in constant time and keeps the
/// first-insertion order.
///
/// # Errors
///
/// Returns `500` when a mapping on the path, or its `properties`, is no JSON object: every node
/// this function builds is one, so the tree was not built here alone.
fn insert_mapping(
    properties: &mut Map<String, Value>,
    keys: &[&str],
    field_type: &'static str,
) -> Result<(), OpenSearchError> {
    let Some((key, descendant_keys)) = keys.split_first() else {
        return Ok(());
    };
    if !properties.contains_key(*key) {
        properties.insert((*key).to_string(), json!({"type": FieldType::Object.name()}));
    }
    let Some(Value::Object(mapping)) = properties.get_mut(*key) else {
        return Err(report_malformed_mapping(key));
    };
    if descendant_keys.is_empty() {
        mapping.insert("type".to_string(), Value::from(field_type));
        return Ok(());
    }
    let Value::Object(child_properties) = mapping.entry("properties").or_insert_with(|| Value::Object(Map::new()))
    else {
        return Err(report_malformed_mapping(key));
    };
    insert_mapping(child_properties, descendant_keys, field_type)
}

/// The `500` for a mapping tree whose node `key`, or its `properties`, is no JSON object.
fn report_malformed_mapping(key: &str) -> OpenSearchError {
    OpenSearchError::internal(format!("the mapping of [{key}] or its properties is no object"))
}

/// Mapping properties in the order of `fields`: the schema column order for a native index and
/// the profile's declared order otherwise, IceGate's contract, kept on the wire because
/// icegate-query enables `serde_json`'s `preserve_order`. A field nests under its parent chain;
/// below each parent it is keyed by the rest of its name, so a stored key with dots stays one
/// property.
///
/// # Errors
///
/// As [`insert_mapping`].
fn render_properties(fields: &[IndexField]) -> Result<Map<String, Value>, OpenSearchError> {
    let parents: HashMap<&str, &str> = fields
        .iter()
        .filter_map(|field| field.parent.as_deref().map(|parent| (field.name.as_str(), parent)))
        .collect();
    let mut properties = Map::new();
    let mut chain: Vec<&str> = Vec::new();
    let mut keys: Vec<&str> = Vec::new();
    for field in fields {
        chain.clear();
        chain.push(field.name.as_str());
        while let Some(parent) = chain.last().and_then(|name| parents.get(name)) {
            chain.push(*parent);
        }
        chain.reverse();
        keys.clear();
        let mut parent_name: Option<&str> = None;
        for &name in &chain {
            keys.push(
                parent_name
                    .and_then(|parent| name.strip_prefix(parent))
                    .and_then(|rest| rest.strip_prefix('.'))
                    .unwrap_or(name),
            );
            parent_name = Some(name);
        }
        insert_mapping(&mut properties, &keys, field.field_type.name())?;
    }
    Ok(properties)
}

/// The name of `field` under its parent: the whole stored key of a MAP key, dots included; the
/// field's own name when it has no parent.
fn read_leaf_name(field: &IndexField) -> &str {
    field
        .parent
        .as_deref()
        .and_then(|parent| field.name.strip_prefix(parent))
        .and_then(|rest| rest.strip_prefix('.'))
        .unwrap_or(&field.name)
}

/// `{key: value}`, taking `value` as built: interpolating it into `json!` would serialize it into
/// a copy.
fn build_object(key: &str, value: Value) -> Value {
    let mut object = Map::new();
    object.insert(key.to_string(), value);
    Value::Object(object)
}

/// `GET /<index>/_mapping/field/<fields>`, keyed by the index expression as requested. Served for
/// data indices only: a system index is refused with `400`, and `GET /<index>/_mapping` answers
/// its stored mapping.
pub(crate) async fn describe_field_mappings(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters((expression_text, fields_text)): PathParameters<(String, String)>,
    Parameters(parameters): Parameters<MappingParameters>,
) -> Result<Json<Value>, OpenSearchError> {
    let options = read_index_options(
        parameters.ignore_unavailable,
        parameters.allow_no_indices,
        parameters.expand_wildcards.as_deref(),
    )?;
    let patterns: Vec<&str> = fields_text.split(',').collect();
    let expression = IndexExpression::parse(&expression_text)?;
    let session = state.catalog.open_session(&tenant).await?;
    match session.resolve_target(&expression, options).await? {
        IndexTarget::Data(entries) => {
            let fields_by_index =
                list_fields_per_index(&session, entries, Utc::now(), choose_field_listing(&patterns)).await?;
            Ok(Json(render_field_mappings(
                &fields_by_index,
                &patterns,
                &expression_text,
            )?))
        }
        IndexTarget::System(_) => Err(OpenSearchError::illegal_argument(
            "field mappings of system indices are not served; GET /<index>/_mapping answers the stored mapping",
        )),
    }
}

/// The field mappings of every field one of `patterns` matches across `fields_by_index`, under
/// `{<expression_text>: {mappings: {<field>: {full_name, mapping: {<leaf>: {type}}}}}}`.
///
/// # Errors
///
/// Returns `illegal_argument_exception` when a matched field has different types in two of the
/// selected indices: one answer per field cannot hold both.
fn render_field_mappings(
    fields_by_index: &[(String, Vec<IndexField>)],
    patterns: &[&str],
    expression_text: &str,
) -> Result<Value, OpenSearchError> {
    let mut mappings: BTreeMap<&str, (FieldType, &IndexField)> = BTreeMap::new();
    for field in fields_by_index.iter().flat_map(|(_, fields)| fields) {
        if !patterns.iter().any(|pattern| matches_glob(pattern, &field.name)) {
            continue;
        }
        match mappings.entry(field.name.as_str()) {
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert((field.field_type, field));
            }
            Entry::Occupied(occupied_entry) => {
                let (existing_type, _) = *occupied_entry.get();
                if existing_type != field.field_type {
                    return Err(OpenSearchError::illegal_argument(format!(
                        "field [{}] has types [{}] and [{}] across [{expression_text}]",
                        field.name,
                        existing_type.name(),
                        field.field_type.name()
                    )));
                }
            }
        }
    }
    let rendered_mappings: Map<String, Value> = mappings
        .into_iter()
        .map(|(name, (field_type, field))| {
            (
                name.to_string(),
                json!({"full_name": name, "mapping": { read_leaf_name(field): {"type": field_type.name()} }}),
            )
        })
        .collect();
    Ok(build_object(
        expression_text,
        build_object("mappings", Value::Object(rendered_mappings)),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::http::StatusCode;
    use bytes::Bytes;
    use icegate_common::LOGS_TABLE;
    use icegate_common::schema::{COL_BODY, COL_LOG_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_TIMESTAMP};
    use opentelemetry::metrics::MeterProvider as _;
    use serde_json::json;

    use super::*;
    use crate::infra::metrics::QueryMetrics;
    use crate::infra::metrics::test_support::{build_meter_provider, find_counter_total};
    use crate::opensearch::catalog::{CatalogSettings, IndexCatalog};
    use crate::opensearch::config::{EmulatedVersion, OpenSearchConfig};
    use crate::opensearch::error::{OpenSearchErrorKind, render_envelope};
    use crate::opensearch::node::NodeIdentity;
    use crate::test_support::{
        LogAttributesRow, LogsWarehouse, build_in_memory_system_index_store, build_logs_warehouse,
        commit_log_attributes_rows,
    };

    fn build_leaf(name: &str, field_type: FieldType, is_searchable: bool, is_aggregatable: bool) -> IndexField {
        IndexField {
            name: name.to_string(),
            field_type,
            is_searchable,
            is_aggregatable,
            parent: None,
        }
    }

    fn list_rendered_names(body: &Value) -> Vec<String> {
        let mut names: Vec<String> = body["fields"].as_object().expect("fields").keys().cloned().collect();
        names.sort_unstable();
        names
    }

    /// A field-discovery request to `logs`, as each handler receives it.
    #[derive(Debug, Clone, Copy)]
    enum FieldRequest {
        /// `_mapping/field/<fields>`.
        FieldMappings(&'static str),
        /// `_field_caps`, with `fields=<fields>` when given.
        FieldCapabilities(Option<&'static str>),
        /// `_mapping`.
        Mappings,
    }

    /// The MAP-key enumerations `request` causes as `tenant-a`, served by a catalog of its own,
    /// so no request is answered from the cache another one filled.
    async fn count_request_enumerations(warehouse: &LogsWarehouse, request: FieldRequest) -> u64 {
        let (provider, exporter) = build_meter_provider();
        let config = OpenSearchConfig::default();
        let system_store = build_in_memory_system_index_store();
        let state = OpenSearchState {
            node: Arc::new(NodeIdentity::new(
                &config.cluster_name,
                EmulatedVersion::parse(&config.version).expect("the default version parses"),
                "127.0.0.1:9200".parse().expect("socket address"),
            )),
            catalog: IndexCatalog::new(
                Arc::clone(&warehouse.engine),
                Arc::new(QueryMetrics::new(&provider.meter("field_request_test"))),
                Arc::clone(&system_store),
                CatalogSettings {
                    field_discovery_window: Duration::from_hours(24),
                    max_query_memory_bytes: 1_073_741_824,
                    max_trace_lookback: Duration::from_hours(1),
                    max_cross_row_traces: 1_000,
                },
            ),
            system_store,
            max_answer_bytes: config.system_indices.max_answer_bytes,
            search_limits: config.search,
        };
        let tenant = Extension(TenantId::new("tenant-a").expect("a valid tenant"));
        let mapping_parameters = MappingParameters {
            ignore_unavailable: None,
            allow_no_indices: None,
            expand_wildcards: None,
        };
        let answer = match request {
            FieldRequest::FieldMappings(fields) => {
                let path = PathParameters((LOGS_TABLE.to_string(), fields.to_string()));
                describe_field_mappings(State(state), tenant, path, Parameters(mapping_parameters))
                    .await
                    .map(IntoResponse::into_response)
            }
            FieldRequest::FieldCapabilities(fields) => {
                let parameters = FieldCapabilitiesParameters {
                    fields: fields.map(str::to_string),
                    ignore_unavailable: None,
                    allow_no_indices: None,
                    expand_wildcards: None,
                };
                let index = Some(PathParameters(LOGS_TABLE.to_string()));
                describe_field_capabilities(
                    State(state),
                    tenant,
                    index,
                    Parameters(parameters),
                    RequestBody(Bytes::new()),
                )
                .await
                .map(IntoResponse::into_response)
            }
            FieldRequest::Mappings => {
                let index = Some(PathParameters(LOGS_TABLE.to_string()));
                describe_mappings(State(state), tenant, index, Parameters(mapping_parameters)).await
            }
        };
        let _answer = answer.expect("an answer");
        provider.force_flush().expect("flushed metrics");
        find_counter_total(
            &exporter,
            "icegate_query_opensearch_map_key_enumerations",
            &[("table", LOGS_TABLE)],
        )
    }

    #[test]
    fn one_type_across_indices_renders_without_an_indices_list() {
        let fields_by_index = vec![
            (
                "logs".to_string(),
                vec![build_leaf(COL_SERVICE_NAME, FieldType::Keyword, true, true)],
            ),
            (
                "spans".to_string(),
                vec![build_leaf(COL_SERVICE_NAME, FieldType::Keyword, true, true)],
            ),
        ];

        let body = render_field_capabilities(&fields_by_index, &[COL_SERVICE_NAME]);

        assert_eq!(
            body,
            json!({
                "indices": ["logs", "spans"],
                "fields": {
                    COL_SERVICE_NAME: {"keyword": {"type": "keyword", "searchable": true, "aggregatable": true}}
                }
            })
        );
    }

    #[test]
    fn a_type_conflict_lists_the_indices_of_each_type() {
        let fields_by_index = vec![
            ("a".to_string(), vec![build_leaf("value", FieldType::Long, true, true)]),
            (
                "b".to_string(),
                vec![build_leaf("value", FieldType::Keyword, true, true)],
            ),
        ];

        let body = render_field_capabilities(&fields_by_index, &["value"]);

        assert_eq!(
            body["fields"]["value"],
            json!({
                "long": {"type": "long", "searchable": true, "aggregatable": true, "indices": ["a"]},
                "keyword": {"type": "keyword", "searchable": true, "aggregatable": true, "indices": ["b"]}
            })
        );
    }

    #[test]
    fn mixed_searchability_lists_the_non_searchable_indices() {
        let fields_by_index = vec![
            ("a".to_string(), vec![build_leaf("f", FieldType::Keyword, true, true)]),
            ("b".to_string(), vec![build_leaf("f", FieldType::Keyword, false, false)]),
        ];

        let body = render_field_capabilities(&fields_by_index, &["f"]);

        assert_eq!(
            body["fields"]["f"]["keyword"],
            json!({
                "type": "keyword",
                "searchable": false,
                "aggregatable": false,
                "non_searchable_indices": ["b"],
                "non_aggregatable_indices": ["b"]
            })
        );
    }

    #[test]
    fn field_patterns_select_by_glob_and_metadata_fields_follow_the_patterns() {
        let fields_by_index = vec![(
            "logs".to_string(),
            vec![
                build_leaf(COL_BODY, FieldType::Text, true, false),
                build_leaf(COL_SEVERITY_TEXT, FieldType::Keyword, true, true),
            ],
        )];

        let severity_capabilities = render_field_capabilities(&fields_by_index, &["sev*"]);
        let every_capability = render_field_capabilities(&fields_by_index, &["*"]);
        let metadata_capabilities = render_field_capabilities(&fields_by_index, &["_*"]);
        let body_and_id_capabilities = render_field_capabilities(&fields_by_index, &[COL_BODY, "_id"]);

        assert_eq!(list_rendered_names(&severity_capabilities), [COL_SEVERITY_TEXT]);
        assert_eq!(
            list_rendered_names(&every_capability),
            ["_id", "_index", "_source", COL_BODY, COL_SEVERITY_TEXT]
        );
        assert_eq!(
            list_rendered_names(&metadata_capabilities),
            ["_id", "_index", "_source"]
        );
        assert_eq!(list_rendered_names(&body_and_id_capabilities), ["_id", COL_BODY]);
    }

    /// `_mapping/field` answers one mapping per field for the whole expression, so a field typed
    /// differently across the selected indices has no answer and is refused rather than one type
    /// being picked.
    #[tokio::test]
    async fn mapping_field_refuses_a_field_typed_differently_across_indices() {
        let fields_by_index = vec![
            ("a".to_string(), vec![build_leaf("value", FieldType::Long, true, true)]),
            (
                "b".to_string(),
                vec![build_leaf("value", FieldType::Keyword, true, true)],
            ),
        ];

        let error = render_field_mappings(&fields_by_index, &["value"], "a,b").expect_err("a type conflict");
        let agreeing_mappings = render_field_mappings(&fields_by_index[..1], &["value"], "a").expect("one type");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(
            agreeing_mappings,
            json!({"a": {"mappings": {"value": {"full_name": "value", "mapping": {"value": {"type": "long"}}}}}})
        );
    }

    /// A field under a structural parent (a MAP column's key) nests under it by its whole stored
    /// key, so a key with dots stays one property, in `_mapping` and in `_mapping/field` alike. A
    /// key listed without its column first still nests under an `object` parent.
    #[test]
    fn a_field_with_a_parent_nests_under_it_by_its_whole_stored_key() {
        let method_field = format!("{COL_LOG_ATTRIBUTES}.http.method");
        let fields_by_index = vec![(
            "logs".to_string(),
            vec![
                IndexField::object(COL_LOG_ATTRIBUTES.to_string()),
                IndexField::leaf(
                    method_field.clone(),
                    FieldType::Keyword,
                    Some(COL_LOG_ATTRIBUTES.to_string()),
                ),
            ],
        )];

        let properties = render_properties(&fields_by_index[0].1).expect("a mapping tree");
        let properties_without_column = render_properties(&fields_by_index[0].1[1..]).expect("a mapping tree");
        let field_mappings =
            render_field_mappings(&fields_by_index, &[method_field.as_str()], "logs").expect("one type");

        let expected_properties =
            json!({COL_LOG_ATTRIBUTES: {"type": "object", "properties": {"http.method": {"type": "keyword"}}}});
        assert_eq!(Value::Object(properties), expected_properties);
        assert_eq!(Value::Object(properties_without_column), expected_properties);
        assert_eq!(
            field_mappings,
            json!({"logs": {"mappings": {method_field.as_str(): {
                "full_name": method_field,
                "mapping": {"http.method": {"type": "keyword"}}
            }}}})
        );
    }

    /// A mapping tree whose node on the path, or that node's `properties`, is no object is a 500,
    /// never a mapping silently left out.
    #[test]
    fn a_mapping_node_that_is_no_object_is_a_500() {
        let mut leaf_as_text = Map::new();
        leaf_as_text.insert("status".to_string(), json!("keyword"));
        let mut properties_as_number = Map::new();
        properties_as_number.insert("status".to_string(), json!({"type": "object", "properties": 1}));

        let refusals = [
            insert_mapping(&mut leaf_as_text, &["status", "code"], "integer").expect_err("node is text"),
            insert_mapping(&mut properties_as_number, &["status", "code"], "integer")
                .expect_err("properties is a number"),
        ];

        for refusal in refusals {
            assert_eq!(refusal.kind(), &OpenSearchErrorKind::Internal);
            assert_eq!(refusal.status(), StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    /// A MAP key's field name is `<column>.<key>`, so only a pattern holding a wildcard or a dot
    /// can name one. Every other lookup lists the columns and enumerates nothing.
    #[test]
    fn only_a_wildcard_or_a_dotted_pattern_lists_map_keys() {
        let method_field = format!("{COL_LOG_ATTRIBUTES}.http.method");
        let cases: [(&[&str], FieldListing); 7] = [
            (&[COL_TIMESTAMP], FieldListing::ColumnsOnly),
            (&[COL_TIMESTAMP, COL_LOG_ATTRIBUTES], FieldListing::ColumnsOnly),
            (&["_id"], FieldListing::ColumnsOnly),
            (&["*"], FieldListing::WithMapKeys),
            (&["time?tamp"], FieldListing::WithMapKeys),
            (&[method_field.as_str()], FieldListing::WithMapKeys),
            (&[COL_TIMESTAMP, "sev*"], FieldListing::WithMapKeys),
        ];
        for (patterns, expected) in cases {
            assert_eq!(choose_field_listing(patterns), expected, "{patterns:?}");
        }
    }

    /// Each handler decides from its own request whether to enumerate: a lookup of a concrete
    /// column on `_mapping/field` or `_field_caps` scans nothing. The wildcard lookups, the default
    /// `fields` of `_field_caps` and `_mapping` (which lists every field) each enumerate once,
    /// which proves the counter sees the scans the concrete lookups skip.
    #[tokio::test]
    async fn a_concrete_field_lookup_enumerates_no_map_keys() {
        let warehouse = build_logs_warehouse().await;
        commit_log_attributes_rows(
            &warehouse.catalog,
            &[LogAttributesRow {
                tenant_id: "tenant-a",
                timestamp: "2026-10-10T12:00:00Z",
                log_attributes: &[("http.method", "GET")],
            }],
        )
        .await;
        let cases = [
            (FieldRequest::FieldMappings(COL_TIMESTAMP), 0),
            (FieldRequest::FieldCapabilities(Some(COL_TIMESTAMP)), 0),
            (FieldRequest::FieldMappings("*"), 1),
            (FieldRequest::FieldCapabilities(Some("*")), 1),
            (FieldRequest::FieldCapabilities(None), 1),
            (FieldRequest::Mappings, 1),
        ];

        for (request, expected) in cases {
            assert_eq!(
                count_request_enumerations(&warehouse, request).await,
                expected,
                "{request:?}"
            );
        }
    }
}
