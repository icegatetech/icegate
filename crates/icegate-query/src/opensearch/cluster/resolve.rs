//! `GET /_resolve/index/<expression>`: the indices an expression names.
//!
//! Resolution runs with `ignore_unavailable` and `allow_no_indices` set, so a missing concrete
//! name yields `200` with empty lists, never an error.

use axum::Json;
use axum::extract::{Extension, State};
use icegate_common::TenantId;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::opensearch::catalog::{IndexExpression, IndexOptions};
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

/// `GET /_resolve/index/<expression>`: `{indices: [{name, attributes}], aliases: [],
/// data_streams: []}`, indices in name order. Every data index is open.
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
    let index_entries: Vec<Value> = session
        .indices()
        .resolve(&expression, options)?
        .into_iter()
        .map(|entry| json!({"name": entry.name(), "attributes": ["open"]}))
        .collect();
    Ok(Json(
        json!({"indices": index_entries, "aliases": [], "data_streams": []}),
    ))
}
