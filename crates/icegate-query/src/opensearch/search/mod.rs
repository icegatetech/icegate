//! `_search`. A request is parsed once into the shared [`request::SearchRequest`], then
//! dispatched on what its index expression resolves to: system indices are searched in memory
//! by the system evaluator, an expression selecting nothing answers no hits, and an expression
//! resolving to data indices is a 400 here.

pub(crate) mod request;

use axum::extract::{RawQuery, State};
use axum::response::Response;
use serde_json::Value;

use self::request::{SearchParameters, parse_search_request, parse_search_value};
use super::catalog::{IndexExpression, IndexTarget, read_index_options};
use super::context::RequestContext;
use super::error::OpenSearchError;
use super::extract::{Parameters, PathParameters, RequestBody};
use super::state::OpenSearchState;
use super::system_index::bulk::{build_json_response, serialize_bounded_answer};
use super::system_index::scroll::{ScrollOpening, open_system_scroll, parse_body_value};
use super::system_index::search::search_system_indices;
use super::system_index::{CurrentSnapshot, run_blocking_work};

/// `GET|POST [/{index}]/_search`. An expression that selects nothing answers no hits, unless
/// `allow_no_indices=false`, which answers 404. `scroll` on a system index opens a scroll (see
/// [`open_system_scroll`]).
///
/// The search and the serialization of its answer run on the blocking pool, because both are
/// linear in the snapshot.
///
/// # Errors
///
/// - the errors of [`parse_search_request`] (with `scroll`, of [`parse_body_value`] and
///   [`parse_search_value`]), [`IndexExpression::parse`], [`read_index_options`],
///   [`search_system_indices`] and, with `scroll`, [`open_system_scroll`];
/// - `400 illegal_argument_exception` for an expression selecting data indices, alone or together
///   with system indices, or an answer past `max_answer_bytes`;
/// - `404 index_not_found_exception` for a concrete name that does not exist, unless
///   `ignore_unavailable` is set, and for an expression that selects nothing under
///   `allow_no_indices=false`;
/// - `500` when the session cannot be opened, the store cannot be read, or the blocking work does
///   not finish.
pub(crate) async fn search_indices(
    State(state): State<OpenSearchState>,
    context: RequestContext,
    index: Option<PathParameters<String>>,
    (Parameters(parameters), RawQuery(query_string)): (Parameters<SearchParameters>, RawQuery),
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    // A scroll keeps its body for the continuations, so the body is read as JSON once and the
    // request borrows from it.
    let (request, scroll_body) = if parameters.scroll.is_some() {
        let body_value = parse_body_value(&body)?;
        (parse_search_value(&body_value, &parameters)?, body_value)
    } else {
        (parse_search_request(&body, &parameters)?, Value::Null)
    };
    let expression = IndexExpression::parse(index.as_ref().map_or("", |PathParameters(index)| index.as_str()))?;
    let options = read_index_options(
        parameters.ignore_unavailable,
        parameters.allow_no_indices,
        parameters.expand_wildcards.as_deref(),
    )?;
    let session = state.catalog.open_session(&context.tenant).await?;
    let (current_snapshot, names) = match session.resolve_target(&expression, options).await? {
        // The session caches the snapshot resolution read, so the search and a scroll's snapshot
        // name both use the state the expression resolved against.
        IndexTarget::System(names) => (session.read_system_snapshot().await?.clone(), names),
        // An expression that selects nothing, when `allow_no_indices` allows that, reads no
        // documents; the request is still validated, so it refuses what any search refuses.
        IndexTarget::Data(entries) if entries.is_empty() => (CurrentSnapshot::default(), Vec::new()),
        IndexTarget::Data(_) => {
            return Err(OpenSearchError::illegal_argument(
                "searching data indices is not available",
            ));
        }
    };
    let maximum_lifetime = state.system_store.max_scroll_lifetime();
    let max_answer_bytes = state.max_answer_bytes;
    let answer_body = run_blocking_work("searching system indices", move || {
        let answer = match parameters.scroll.as_deref() {
            Some(lifetime) => {
                let opening = ScrollOpening {
                    lifetime,
                    maximum_lifetime,
                    query_string: query_string.as_deref().unwrap_or_default(),
                    body: scroll_body,
                };
                open_system_scroll(&current_snapshot, &names, &request, opening, &context)?
            }
            None => search_system_indices(&current_snapshot.snapshot, &names, &request, &context)?,
        };
        serialize_bounded_answer(&answer, max_answer_bytes)
    })
    .await?;
    Ok(build_json_response(answer_body))
}
