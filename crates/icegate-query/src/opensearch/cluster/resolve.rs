//! `GET /_resolve/index/<expression>`: the indices and aliases an expression names.
//!
//! Resolution runs with `ignore_unavailable` and `allow_no_indices` set, so a missing concrete
//! name yields `200` with empty lists, never an error.

use axum::Json;
use axum::extract::{Extension, State};
use icegate_common::TenantId;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::opensearch::catalog::{IndexEntry, IndexExpression, IndexOptions, merge_name_ordered_lists};
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

/// `GET /_resolve/index/<expression>`: `{indices: [{name, aliases?, attributes}], aliases: [{name,
/// indices}], data_streams: []}`, each list in name order. A data index is open and listed by its
/// concrete name, whether the expression reached it by that name or by an alias; one with aliases
/// carries them, and each is listed with the index it names whenever the index is listed. A
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
            .map(|entry| (entry.name(), render_data_index(entry))),
        listing
            .system_listing
            .indices
            .iter()
            .map(|name| (name.as_str(), json!({"name": name, "attributes": ["hidden", "open"]}))),
        |(name, _)| *name,
    );
    let mut data_aliases: Vec<(&str, Value)> = listing
        .data_entries
        .iter()
        .flat_map(|entry| {
            entry
                .alias_names()
                .iter()
                .map(|alias| (*alias, json!({"name": alias, "indices": [entry.name()]})))
        })
        .collect();
    // Each entry's aliases are in name order, but not those of different entries.
    data_aliases.sort_unstable_by_key(|(alias, _)| *alias);
    let system_aliases: Vec<(&str, Value)> = if listing.system_listing.aliases.is_empty() {
        Vec::new()
    } else {
        let snapshot = &session.read_system_snapshot().await?.snapshot;
        listing
            .system_listing
            .aliases
            .iter()
            .map(|alias| {
                (
                    alias.as_str(),
                    json!({"name": alias, "indices": snapshot.resolve_name(alias).into_iter().collect::<Vec<_>>()}),
                )
            })
            .collect()
    };
    let aliases: Vec<Value> =
        merge_name_ordered_lists(data_aliases.into_iter(), system_aliases.into_iter(), |(alias, _)| {
            *alias
        })
        .into_iter()
        .map(|(_, alias)| alias)
        .collect();
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

/// The `_resolve/index` row of a data index: open, with its aliases when it has any.
fn render_data_index(entry: &IndexEntry) -> Value {
    if entry.alias_names().is_empty() {
        json!({"name": entry.name(), "attributes": ["open"]})
    } else {
        json!({"name": entry.name(), "aliases": entry.alias_names(), "attributes": ["open"]})
    }
}
