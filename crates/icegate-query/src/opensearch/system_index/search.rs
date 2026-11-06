//! Reading system indices with a query: the documents of the concrete indices a request
//! resolves to, matched in memory by the system evaluator.
//!
//! Matching runs on the blocking pool: it is linear in the snapshot, which `max_snapshot_bytes`
//! lets grow large, and on an async worker it would stall every other request that worker serves.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};

use super::documents::parse_json_body;
use super::evaluate::matches_document;
use super::snapshot::{SystemIndex, SystemSnapshot};
use super::store::run_blocking_work;
use crate::opensearch::catalog::{IndexExpression, IndexTarget, read_index_options};
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, Parameters, PathParameters, RequestBody, deserialize_flag};
use crate::opensearch::state::OpenSearchState;
use crate::query_dsl::ast::Query;
use crate::query_dsl::parse::parse_query;

/// Parameters of `_count`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CountParameters {
    /// Whether a missing concrete name is skipped instead of answered `404`.
    #[serde(default, deserialize_with = "deserialize_flag")]
    ignore_unavailable: Option<bool>,
    /// Whether an expression that selects nothing counts zero instead of answering `404`.
    #[serde(default, deserialize_with = "deserialize_flag")]
    allow_no_indices: Option<bool>,
    /// What wildcards expand to.
    expand_wildcards: Option<String>,
}

impl EndpointParameters for CountParameters {
    const IGNORED: &'static [&'static str] = &["preference", "routing", "ignore_throttled"];
}

/// `GET|POST /{index}/_count` on system indices: `{count, _shards}` with every shard successful.
/// The body may hold only `query`. An expression that selects nothing answers `count` 0, unless
/// `allow_no_indices=false`, which answers 404.
///
/// # Errors
///
/// - `400 parsing_exception` for a malformed body, a body key other than `query`, or a query the
///   parser refuses;
/// - `400 illegal_argument_exception` for a malformed index expression, a bad `expand_wildcards`,
///   a target holding a data index, or a query the evaluator refuses;
/// - `404 index_not_found_exception` for a concrete name that does not exist, unless
///   `ignore_unavailable` is set;
/// - `500` when the session cannot be opened, the store cannot be read, or the blocking work does
///   not finish.
pub(crate) async fn count_documents(
    State(state): State<OpenSearchState>,
    context: RequestContext,
    PathParameters(index): PathParameters<String>,
    Parameters(parameters): Parameters<CountParameters>,
    RequestBody(body): RequestBody,
) -> Result<Json<Value>, OpenSearchError> {
    let query = parse_count_body(&body)?;
    let expression = IndexExpression::parse(&index)?;
    let options = read_index_options(
        parameters.ignore_unavailable,
        parameters.allow_no_indices,
        parameters.expand_wildcards.as_deref(),
    )?;
    let session = state.catalog.open_session(&context.tenant).await?;
    let names = match session.resolve_target(&expression, options).await? {
        IndexTarget::System(names) => names,
        // An expression that selects nothing, when `allow_no_indices` allows that, reads no
        // documents.
        IndexTarget::Data(entries) if entries.is_empty() => {
            return Ok(Json(json!({"count": 0, "_shards": render_search_shards()})));
        }
        IndexTarget::Data(_) => {
            return Err(OpenSearchError::illegal_argument(
                "_count is served for system indices only",
            ));
        }
    };
    // The session caches the snapshot resolution read, so the count evaluates the same state.
    let snapshot = Arc::clone(&session.read_system_snapshot().await?.snapshot);
    let now = context.now;
    let count = run_blocking_work("counting system-index documents", move || {
        count_system_documents(&snapshot, &names, &query, now)
    })
    .await?;
    Ok(Json(json!({"count": count, "_shards": render_search_shards()})))
}

/// A `_count` body: empty (every document) or an object whose only key is `query`.
fn parse_count_body(body: &[u8]) -> Result<Query, OpenSearchError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Query::MatchAll);
    }
    let Value::Object(object) = parse_json_body(body)? else {
        return Err(OpenSearchError::parsing("a _count body must be a JSON object"));
    };
    let mut query = Query::MatchAll;
    for (key, value) in &object {
        match key.as_str() {
            "query" => query = parse_query(value)?,
            other => return Err(OpenSearchError::parsing(format!("request does not support [{other}]"))),
        }
    }
    Ok(query)
}

/// The `_shards` block of a system-index read: one shard, always successful.
pub(crate) fn render_search_shards() -> Value {
    json!({"total": 1, "successful": 1, "skipped": 0, "failed": 0})
}

/// The system indices named by `names`, in the order given. `names` are concrete index names, as
/// a resolved target holds them (aliases already resolved, each index once); this only looks
/// them up.
///
/// # Errors
///
/// `index_not_found_exception` naming a name the snapshot does not hold, which a target
/// resolved against the same snapshot never contains.
pub(crate) fn find_system_indices<'a>(
    snapshot: &'a SystemSnapshot,
    names: &'a [String],
) -> Result<Vec<(&'a str, &'a SystemIndex)>, OpenSearchError> {
    names
        .iter()
        .map(|name| {
            snapshot
                .find_index(name)
                .map(|index| (name.as_str(), index))
                .ok_or_else(|| OpenSearchError::index_not_found(name))
        })
        .collect()
}

/// How many documents of `indices` match `query` at `now`.
///
/// # Errors
///
/// The errors of [`find_system_indices`] and of [`matches_document`].
pub(crate) fn count_system_documents(
    snapshot: &SystemSnapshot,
    indices: &[String],
    query: &Query,
    now: DateTime<Utc>,
) -> Result<u64, OpenSearchError> {
    let mut count = 0_u64;
    for (_, index) in find_system_indices(snapshot, indices)? {
        for (id, document) in index.documents() {
            if matches_document(query, id, &document.source, &index.mappings, now)? {
                count += 1;
            }
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;
    use crate::opensearch::error::render_envelope;

    #[tokio::test]
    async fn a_name_the_snapshot_does_not_hold_is_index_not_found() {
        let names = [".kibana_1".to_string()];

        let error = find_system_indices(&SystemSnapshot::default(), &names).expect_err("refused");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["type"], "index_not_found_exception");
    }
}
