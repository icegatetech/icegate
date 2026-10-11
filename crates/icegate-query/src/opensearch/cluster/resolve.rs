//! `GET /_resolve/index/<expression>`: the indices and aliases an expression names.
//!
//! Resolution runs with `ignore_unavailable` and `allow_no_indices` set, so a missing concrete
//! name yields `200` with empty lists, never an error.

use axum::Json;
use axum::extract::{Extension, State};
use icegate_common::TenantId;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::opensearch::catalog::{IndexExpression, IndexOptions, merge_name_ordered_lists};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, ExpandWildcards, Parameters, PathParameters};
use crate::opensearch::state::OpenSearchState;

/// Query parameters of `_resolve/index`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolveParameters {
    /// What a wildcard of the expression expands to; absent means open indices.
    expand_wildcards: Option<String>,
}

impl EndpointParameters for ResolveParameters {
    const IGNORED: &'static [&'static str] = &[];
}

/// `GET /_resolve/index/<expression>`: `{indices: [{name, attributes}], aliases: [{name, indices}],
/// data_streams: []}`, each list in name order. A data index is open and has no alias. A
/// dot-prefixed index is listed with the attributes `hidden` and `open`, and only when a part
/// starts with `.` or `expand_wildcards` includes hidden; its aliases are listed with the index
/// each names.
pub(crate) async fn resolve_index(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters(expression_text): PathParameters<String>,
    Parameters(parameters): Parameters<ResolveParameters>,
) -> Result<Json<Value>, OpenSearchError> {
    let expression = IndexExpression::parse(&expression_text)?;
    let options = IndexOptions {
        ignore_unavailable: true,
        allow_no_indices: true,
        expand_wildcards: ExpandWildcards::parse(parameters.expand_wildcards.as_deref())?,
    };
    let session = state.catalog.open_session(&tenant).await?;
    let listing = session.list_targets(&expression, options).await?;
    // Both sides come in name order, so merging them keeps `indices` in name order.
    let named_indices = merge_name_ordered_lists(
        listing
            .data_entries
            .iter()
            .map(|entry| (entry.name(), json!({"name": entry.name(), "attributes": ["open"]}))),
        listing
            .system_listing
            .indices
            .iter()
            .map(|name| (name.as_str(), json!({"name": name, "attributes": ["hidden", "open"]}))),
        |(name, _)| *name,
    );
    let aliases: Vec<Value> = if listing.system_listing.aliases.is_empty() {
        Vec::new()
    } else {
        let snapshot = &session.read_system_snapshot().await?.snapshot;
        listing
            .system_listing
            .aliases
            .iter()
            .map(
                |alias| json!({"name": alias, "indices": snapshot.resolve_name(alias).into_iter().collect::<Vec<_>>()}),
            )
            .collect()
    };
    // The lists are moved into the body: interpolating them into `json!` would serialize copies.
    let mut body = Map::new();
    body.insert(
        "indices".to_string(),
        named_indices.into_iter().map(|(_, index)| index).collect(),
    );
    body.insert("aliases".to_string(), Value::Array(aliases));
    body.insert("data_streams".to_string(), Value::Array(Vec::new()));
    Ok(Json(Value::Object(body)))
}
