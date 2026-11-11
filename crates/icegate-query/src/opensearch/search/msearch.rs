//! `_msearch`: several searches in one NDJSON request.
//!
//! The body is a sequence of line pairs: a header object naming one search's indices and index
//! options, then that search's body. Each item is resolved and executed as `_search` resolves and
//! executes it. The index options of the URL are every item's defaults; a header overrides only
//! the keys it names.
//!
//! A failing item answers its own error envelope with its own `status` and leaves the other
//! items untouched, so one malformed query never hides the answers to the others. A body that
//! cannot be framed into line pairs is refused as a whole, and so is a header that cannot be
//! read: its item has no target to run against, and answering the others would hide a malformed
//! request. Items run one after another in request order: a request then holds one item's result
//! at a time, whatever the number of items. Each item's answer is serialized on the blocking pool
//! as soon as it is built, within what the earlier answered items left of `max_answer_bytes`, so
//! the answered items together stay within the bound and an item past what is left fails alone,
//! while a later item that fits what is left is still answered.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;

use super::execute::{check_result_window, search_data_indices};
use super::request::{SearchParameters, parse_search_request};
use crate::opensearch::SearchLimitsConfig;
use crate::opensearch::catalog::{IndexExpression, IndexOptions, IndexTarget, TenantSession, read_index_options};
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{
    EndpointParameters, ExpandWildcards, Parameters, PathParameters, RequestBody, deserialize_flag,
};
use crate::opensearch::state::OpenSearchState;
use crate::opensearch::system_index::bulk::{build_json_response, serialize_shared_answer};
use crate::opensearch::system_index::run_blocking_work;
use crate::opensearch::system_index::search::search_system_indices;

/// The refusal of an empty body.
const NO_REQUESTS_REASON: &str = "no requests added";

/// The refusal of a body whose last line has no newline: the last item may have been cut off.
const MISSING_TRAILING_NEWLINE_REASON: &str = "The msearch request must be terminated by a newline [\n]";

/// The refusal of a body whose last line is a header: its search body may have been cut off.
const HEADER_WITHOUT_BODY_REASON: &str = "the msearch request ends with a header that has no search body";

/// The refusal of a header that names its indices twice: keeping either key would drop the other.
const BOTH_INDEX_KEYS_REASON: &str = "msearch header names both [index] and [indices]";

/// One search of an `_msearch` request: its header and its unparsed body.
pub(crate) struct MultiSearchItem {
    /// The indices and index options the search reads.
    pub(crate) header: MultiSearchHeader,
    /// The search body, a slice of the request body; an empty line reads as `{}`.
    pub(crate) body: Bytes,
}

/// The header line of one `_msearch` item.
pub(crate) struct MultiSearchHeader {
    /// The indices the header names; `None` when it names none, and the search then reads the
    /// URL's index, or every visible index when the URL names none either.
    pub(crate) index: Option<IndexExpression>,
    /// The URL's index options, overridden by the keys this header names.
    pub(crate) options: IndexOptions,
}

/// Header keys accepted and ignored: they tune routing, caching or scoring, none of which
/// changes the rows a single-shard, unscored search returns.
const IGNORED_HEADER_KEYS: [&str; 5] = [
    "preference",
    "search_type",
    "request_cache",
    "routing",
    "allow_partial_search_results",
];

/// Split an `_msearch` body into its items.
///
/// # Errors
///
/// `400 illegal_argument_exception` when the body is empty, does not end with a newline, ends
/// with a header that has no body, or a header names an unsupported key (carried as `key`), an
/// invalid index expression or an invalid `expand_wildcards`; `400 parsing_exception` when a
/// header is not a JSON object or a header value has the wrong type. Item bodies are not parsed
/// here: a malformed body fails only its own item.
pub(crate) fn parse_msearch_body(
    body: &Bytes,
    defaults: IndexOptions,
) -> Result<Vec<MultiSearchItem>, OpenSearchError> {
    if body.is_empty() {
        return Err(OpenSearchError::illegal_argument(NO_REQUESTS_REASON));
    }
    if body.last() != Some(&b'\n') {
        return Err(OpenSearchError::illegal_argument(MISSING_TRAILING_NEWLINE_REASON));
    }
    let mut lines = split_lines(body);
    let mut items = Vec::new();
    while let Some(header_line) = lines.next() {
        let header = parse_header(&header_line, defaults)?;
        let body_line = lines
            .next()
            .ok_or_else(|| OpenSearchError::illegal_argument(HEADER_WITHOUT_BODY_REASON))?;
        let body = if body_line.iter().all(u8::is_ascii_whitespace) {
            Bytes::from_static(b"{}")
        } else {
            body_line
        };
        items.push(MultiSearchItem { header, body });
    }
    Ok(items)
}

/// The lines of `body` without their terminating newline (and a carriage return before it),
/// each a zero-copy slice. The caller has checked that `body` ends with a newline.
fn split_lines(body: &Bytes) -> impl Iterator<Item = Bytes> {
    let content = body.slice(..body.len() - 1);
    let mut start = 0;
    let mut is_finished = false;
    std::iter::from_fn(move || {
        if is_finished {
            return None;
        }
        let end = content[start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(content.len(), |offset| start + offset);
        is_finished = end == content.len();
        let line_end = if end > start && content[end - 1] == b'\r' {
            end - 1
        } else {
            end
        };
        let line = content.slice(start..line_end);
        start = end + 1;
        Some(line)
    })
}

/// Parse one header line over the URL's `options`. A blank line is a header naming nothing.
fn parse_header(line: &[u8], mut options: IndexOptions) -> Result<MultiSearchHeader, OpenSearchError> {
    if line.iter().all(u8::is_ascii_whitespace) {
        return Ok(MultiSearchHeader { index: None, options });
    }
    let value: Value = serde_json::from_slice(line)
        .map_err(|error| OpenSearchError::parsing(format!("msearch header is not valid JSON: {error}")))?;
    let Value::Object(header) = value else {
        return Err(OpenSearchError::parsing("msearch header must be a JSON object"));
    };
    let mut index = None;
    for (key, value) in &header {
        match key.as_str() {
            "index" | "indices" => {
                if index.is_some() {
                    return Err(OpenSearchError::parsing(BOTH_INDEX_KEYS_REASON));
                }
                index = Some(parse_header_index(key, value)?);
            }
            "ignore_unavailable" => options.ignore_unavailable = parse_header_flag(key, value)?,
            "allow_no_indices" => options.allow_no_indices = parse_header_flag(key, value)?,
            "expand_wildcards" => {
                let expand_wildcards_text = join_header_strings(key, value)?;
                options.expand_wildcards = ExpandWildcards::parse(Some(expand_wildcards_text.as_str()))?;
            }
            ignored_key if IGNORED_HEADER_KEYS.contains(&ignored_key) => {}
            unknown_key => return Err(OpenSearchError::unsupported_header_key(unknown_key)),
        }
    }
    Ok(MultiSearchHeader { index, options })
}

/// The header `index`: a string (one name, a comma list or `""`) or an array of names.
fn parse_header_index(key: &str, value: &Value) -> Result<IndexExpression, OpenSearchError> {
    IndexExpression::parse(&join_header_strings(key, value)?)
}

/// A string, or an array of strings joined with commas.
fn join_header_strings(key: &str, value: &Value) -> Result<String, OpenSearchError> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Array(parts) => parts
            .iter()
            .map(|part| {
                part.as_str()
                    .ok_or_else(|| OpenSearchError::parsing(format!("msearch header [{key}] must hold strings")))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|parts| parts.join(",")),
        _ => Err(OpenSearchError::parsing(format!(
            "msearch header [{key}] must be a string or an array of strings"
        ))),
    }
}

/// A header flag: a JSON boolean or the string `true` / `false`.
fn parse_header_flag(key: &str, value: &Value) -> Result<bool, OpenSearchError> {
    match value {
        Value::Bool(flag) => Ok(*flag),
        Value::String(text) if text == "true" => Ok(true),
        Value::String(text) if text == "false" => Ok(false),
        _ => Err(OpenSearchError::parsing(format!(
            "msearch header [{key}] must be true or false"
        ))),
    }
}

/// Query parameters of `_msearch`.
///
/// The index options are every item's defaults, and a header overrides them key by key.
/// `rest_total_hits_as_int`, `size` and `track_total_hits` apply to every item as the `_search`
/// parameters of the same names apply to one search: `size` and `track_total_hits` override each
/// item body's key.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MultiSearchParameters {
    /// Default for each item: a missing concrete index name is skipped instead of a 404.
    #[serde(default, deserialize_with = "deserialize_flag")]
    ignore_unavailable: Option<bool>,
    /// Default for each item: a wildcard matching nothing is an empty answer instead of a 404.
    #[serde(default, deserialize_with = "deserialize_flag")]
    allow_no_indices: Option<bool>,
    /// Default for each item: which indices a wildcard expands to.
    #[serde(default)]
    expand_wildcards: Option<String>,
    /// Every item renders `hits.total` as an integer.
    #[serde(default, deserialize_with = "deserialize_flag")]
    rest_total_hits_as_int: Option<bool>,
    /// Overrides every item body's `size`.
    #[serde(default)]
    size: Option<usize>,
    /// Overrides every item body's `track_total_hits`: `true`, `false` or an integer.
    #[serde(default)]
    track_total_hits: Option<String>,
}

impl EndpointParameters for MultiSearchParameters {
    /// Accepted with no effect: one replica answers every item from one shard, one item after
    /// another, within the engine's own deadline, so there is no preference, timeout, shard or
    /// search concurrency, throttling, search type or pre-filter to choose.
    const IGNORED: &'static [&'static str] = &[
        "preference",
        "timeout",
        "ignore_throttled",
        "max_concurrent_shard_requests",
        "max_concurrent_searches",
        "search_type",
        "pre_filter_shard_size",
    ];
}

/// `POST /_msearch`: an item whose header names no index reads every visible data index.
///
/// The answer is `{took, responses}`, one response per item in request order, each carrying its
/// own `status`: a search answer with `status: 200`, or the error envelope `_search` would answer
/// for that item.
///
/// # Errors
///
/// The errors of [`answer_multi_search`], which refuse the request as a whole.
pub(crate) async fn run_multi_search(
    State(state): State<OpenSearchState>,
    context: RequestContext,
    Parameters(parameters): Parameters<MultiSearchParameters>,
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    let every_visible_index = IndexExpression::parse("")?;
    answer_multi_search(&state, &context, &every_visible_index, parameters, &body)
        .await
        .map(build_json_response)
}

/// `POST /{index}/_msearch`: an item whose header names no index reads the path's index; the
/// answer is that of [`run_multi_search`].
///
/// # Errors
///
/// `400 illegal_argument_exception` for an invalid index expression in the path, and the errors
/// of [`answer_multi_search`], which refuse the request as a whole.
pub(crate) async fn run_multi_search_on_index(
    State(state): State<OpenSearchState>,
    PathParameters(index): PathParameters<String>,
    context: RequestContext,
    Parameters(parameters): Parameters<MultiSearchParameters>,
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    let path_expression = IndexExpression::parse(&index)?;
    answer_multi_search(&state, &context, &path_expression, parameters, &body)
        .await
        .map(build_json_response)
}

/// Parse the items, run each in request order and assemble their answers. An item whose header
/// names no index reads `fallback_expression`.
///
/// The answered items share `max_answer_bytes`: each item is serialized within what the earlier
/// answered items left of it, and one past that fails alone, so the answered items together hold
/// at most the bound. An item past the bound on its own is refused with `answer_too_large`; one
/// that fits the bound alone but not what is left, with `msearch_answer_too_large`. A refused item
/// takes none of the bound: its envelope follows from the request, not from the data, so a later
/// item that fits what is left is still answered. The refusals and the envelope around the items
/// (`took`, the brackets and separators) come on top of the bound.
///
/// # Errors
///
/// The errors of [`read_index_options`] and [`parse_msearch_body`]; `500` when the session cannot
/// be opened. An item's own failure is answered inside the assembled answer instead.
async fn answer_multi_search(
    state: &OpenSearchState,
    context: &RequestContext,
    fallback_expression: &IndexExpression,
    parameters: MultiSearchParameters,
    body: &Bytes,
) -> Result<Vec<u8>, OpenSearchError> {
    let start_instant = Instant::now();
    let default_options = read_index_options(
        parameters.ignore_unavailable,
        parameters.allow_no_indices,
        parameters.expand_wildcards.as_deref(),
    )?;
    let items = parse_msearch_body(body, default_options)?;
    // Every item body is parsed with the parameters a `_search` carrying the same URL keys would
    // read, so the URL's `size` and `track_total_hits` override each body's key exactly as they
    // override a `_search` body's. The index options are resolved per item from its header.
    let item_parameters = SearchParameters {
        size: parameters.size,
        from: None,
        source: None,
        rest_total_hits_as_int: parameters.rest_total_hits_as_int,
        track_total_hits: parameters.track_total_hits,
        seq_no_primary_term: None,
        ignore_unavailable: None,
        allow_no_indices: None,
        expand_wildcards: None,
        scroll: None,
    };
    let item_settings = ItemSettings {
        index: fallback_expression,
        parameters: &item_parameters,
        limits: state.search_limits,
    };
    let session = state.catalog.open_session(&context.tenant).await?;
    let mut answer_budget = AnswerBudget {
        max_answer_bytes: state.max_answer_bytes,
        used_bytes: 0,
    };
    let mut item_answers = Vec::with_capacity(items.len());
    for item in items {
        let item_answer = match run_item(&session, context, &item_settings, answer_budget, item).await {
            Ok(item_answer) => {
                answer_budget.charge_item_answer(item_answer.len());
                item_answer
            }
            Err(error) => render_item_error(error).await?,
        };
        item_answers.push(item_answer);
    }
    let took = u64::try_from(start_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(assemble_multi_search_answer(took, &item_answers))
}

/// What every item of one `_msearch` request runs with.
struct ItemSettings<'a> {
    /// The index an item whose header names none reads: the path's, or every visible index.
    index: &'a IndexExpression,
    /// The URL parameters every item body is parsed with.
    parameters: &'a SearchParameters,
    /// The search limits every item runs under, as a `_search` does.
    limits: SearchLimitsConfig,
}

/// The share of `max_answer_bytes` the answered items of one `_msearch` request have taken so far.
#[derive(Debug, Clone, Copy)]
struct AnswerBudget {
    /// The bound of the answered items together.
    max_answer_bytes: usize,
    /// The bytes the earlier answered items took; a refused item takes none.
    used_bytes: usize,
}

impl AnswerBudget {
    /// The most bytes the next item answer may take: none once the earlier ones reached the bound.
    const fn compute_remaining_bytes(self) -> usize {
        self.max_answer_bytes.saturating_sub(self.used_bytes)
    }

    /// The refusal of an item answer that fits `max_answer_bytes` on its own but not
    /// [`compute_remaining_bytes`](Self::compute_remaining_bytes): it names the bytes the earlier
    /// answered items took.
    fn build_share_refusal(self) -> OpenSearchError {
        OpenSearchError::msearch_answer_too_large(self.max_answer_bytes, self.used_bytes)
    }

    /// Count an answered item of `answer_bytes` against the bound.
    const fn charge_item_answer(&mut self, answer_bytes: usize) {
        self.used_bytes = self.used_bytes.saturating_add(answer_bytes);
    }
}

/// Run one item exactly as `_search` runs a request on the same indices, and serialize its answer
/// as the item: the search answer's own keys, then `status: 200`.
///
/// A system search and the serialization of either answer run on the blocking pool, because each
/// is linear in the snapshot or the page. `answer_budget` is taken by value: it is a small `Copy`
/// type, moved into the serialization.
///
/// # Errors
///
/// The errors `_search` answers for the same request: those of [`parse_search_request`],
/// [`TenantSession::resolve_target`], [`check_result_window`],
/// [`search_data_indices`] and [`search_system_indices`], and the refusals of
/// [`serialize_item_answer`] for an item answer past what is left of the bound.
async fn run_item(
    session: &TenantSession,
    context: &RequestContext,
    settings: &ItemSettings<'_>,
    answer_budget: AnswerBudget,
    item: MultiSearchItem,
) -> Result<Bytes, OpenSearchError> {
    let request = parse_search_request(&item.body, settings.parameters)?;
    let expression = item.header.index.as_ref().unwrap_or(settings.index);
    let limits = settings.limits;
    let item_answer = match session.resolve_target(expression, item.header.options).await? {
        IndexTarget::Data(entries) => {
            check_result_window(&request, limits)?;
            let answer = search_data_indices(session, &entries, &request, context, &limits).await?;
            run_blocking_work("serializing an msearch data item", move || {
                serialize_item_answer(answer, answer_budget)
            })
            .await?
        }
        IndexTarget::System(names) => {
            check_result_window(&request, limits)?;
            // The session caches the snapshot the target was resolved against, so the search
            // reads the same state.
            let snapshot = Arc::clone(&session.read_system_snapshot().await?.snapshot);
            let context = context.clone();
            run_blocking_work("searching system indices for an msearch item", move || {
                let answer = search_system_indices(&snapshot, &names, &request, &context)?;
                serialize_item_answer(answer, answer_budget)
            })
            .await?
        }
    };
    Ok(Bytes::from(item_answer))
}

/// Serialize a search answer as one item within what `answer_budget` has left: its own keys, then
/// `status: 200`. The answer is taken by value, so it drops where it is serialized.
///
/// # Errors
///
/// The errors of [`serialize_shared_answer`]: `answer_too_large` when the item would pass
/// `max_answer_bytes` on its own, and [`AnswerBudget::build_share_refusal`] when it would pass only
/// what is left; `500` when `answer` is not a JSON object, which no search answer is.
fn serialize_item_answer(mut answer: Value, answer_budget: AnswerBudget) -> Result<Vec<u8>, OpenSearchError> {
    let Value::Object(object) = &mut answer else {
        return Err(OpenSearchError::internal("a search answer is not a JSON object"));
    };
    object.insert("status".to_string(), Value::from(StatusCode::OK.as_u16()));
    serialize_shared_answer(
        &answer,
        answer_budget.compute_remaining_bytes(),
        answer_budget.max_answer_bytes,
        || answer_budget.build_share_refusal(),
    )
}

/// The envelope `_search` would answer for `error`, as one item: `{error, status}`.
///
/// The envelope has one renderer, `OpenSearchError`'s `IntoResponse`, and its body is already the
/// item: the error, then the `status` it is answered with. The item is that body, unchanged.
///
/// # Errors
///
/// `500` when the rendered body cannot be read, which an in-memory body never causes.
async fn render_item_error(error: OpenSearchError) -> Result<Bytes, OpenSearchError> {
    axum::body::to_bytes(error.into_response().into_body(), usize::MAX)
        .await
        .map_err(|failure| OpenSearchError::internal(format!("reading an msearch item error: {failure}")))
}

/// The `_msearch` answer `{"took": took, "responses": [..]}` around `item_answers`, each already a
/// serialized JSON object, in request order. The items are copied once into the answer and never
/// parsed again.
fn assemble_multi_search_answer(took: u64, item_answers: &[Bytes]) -> Vec<u8> {
    const TOOK_PREFIX: &[u8] = b"{\"took\":";
    const RESPONSES_PREFIX: &[u8] = b",\"responses\":[";
    const ANSWER_SUFFIX: &[u8] = b"]}";
    let took_text = took.to_string();
    let item_byte_count: usize = item_answers.iter().map(Bytes::len).sum();
    let separator_count = item_answers.len().saturating_sub(1);
    let mut answer = Vec::with_capacity(
        TOOK_PREFIX.len()
            + took_text.len()
            + RESPONSES_PREFIX.len()
            + item_byte_count
            + separator_count
            + ANSWER_SUFFIX.len(),
    );
    answer.extend_from_slice(TOOK_PREFIX);
    answer.extend_from_slice(took_text.as_bytes());
    answer.extend_from_slice(RESPONSES_PREFIX);
    for (position, item_answer) in item_answers.iter().enumerate() {
        if position > 0 {
            answer.push(b',');
        }
        answer.extend_from_slice(item_answer);
    }
    answer.extend_from_slice(ANSWER_SUFFIX);
    answer
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use bytes::Bytes;

    use super::{
        BOTH_INDEX_KEYS_REASON, HEADER_WITHOUT_BODY_REASON, MISSING_TRAILING_NEWLINE_REASON, NO_REQUESTS_REASON,
        parse_msearch_body,
    };
    use crate::opensearch::catalog::{IndexExpression, IndexOptions};
    use crate::opensearch::error::render_envelope;
    use crate::opensearch::extract::ExpandWildcards;

    /// The defaults `_search` applies when the URL names none: a missing concrete name is a 404,
    /// an empty wildcard match is allowed, wildcards expand to open indices.
    const fn build_default_options() -> IndexOptions {
        IndexOptions {
            ignore_unavailable: false,
            allow_no_indices: true,
            expand_wildcards: ExpandWildcards {
                is_open: true,
                is_hidden: false,
            },
        }
    }

    #[test]
    fn msearch_headers_accept_an_index_as_a_string_a_comma_list_an_empty_string_or_an_array() {
        let body = Bytes::from_static(
            concat!(
                r#"{"index":"logs"}"#,
                "\n",
                "{}\n",
                r#"{"index":"logs,spans","preference":"1760000000000"}"#,
                "\n",
                "{}\n",
                r#"{"index":""}"#,
                "\n",
                "{}\n",
                r#"{"index":["logs","spans"],"ignore_unavailable":true}"#,
                "\n",
                r#"{"size":0}"#,
                "\n",
                "{}\n",
                r#"{"query":{"match_all":{}}}"#,
                "\n",
            )
            .as_bytes(),
        );

        let items = parse_msearch_body(&body, build_default_options()).expect("a valid msearch body");

        let indices: Vec<Option<&str>> = items
            .iter()
            .map(|item| item.header.index.as_ref().map(IndexExpression::text))
            .collect();
        assert_eq!(
            indices,
            vec![Some("logs"), Some("logs,spans"), Some(""), Some("logs,spans"), None]
        );
        assert!(
            !items[0].header.options.ignore_unavailable,
            "the default applies without a header key"
        );
        assert!(
            items[3].header.options.ignore_unavailable,
            "a header key overrides the default"
        );
        assert!(items[4].header.options.allow_no_indices);
        assert_eq!(items[0].body.as_ref(), b"{}");
        assert_eq!(items[3].body.as_ref(), br#"{"size":0}"#);
        assert_eq!(items[4].body.as_ref(), br#"{"query":{"match_all":{}}}"#);
    }

    /// Each header names some keys over `build_default_options`, and every key it does not name
    /// keeps its default. The framing rows show that a carriage return before a newline is not
    /// part of the line, and that a blank line reads as an empty header or an empty body.
    #[test]
    fn msearch_headers_override_only_the_keys_they_name() {
        let open_and_hidden = ExpandWildcards {
            is_open: true,
            is_hidden: true,
        };
        let cases: [(&str, Option<&str>, IndexOptions, &[u8]); 7] = [
            (
                "{\"indices\":\"spans\"}\n{}\n",
                Some("spans"),
                build_default_options(),
                b"{}",
            ),
            (
                "{\"index\":\"logs\",\"ignore_unavailable\":\"true\",\"allow_no_indices\":\"false\"}\n{}\n",
                Some("logs"),
                IndexOptions {
                    ignore_unavailable: true,
                    allow_no_indices: false,
                    ..build_default_options()
                },
                b"{}",
            ),
            (
                "{\"index\":\"logs\",\"allow_no_indices\":false,\"expand_wildcards\":\"open,hidden\"}\n{}\n",
                Some("logs"),
                IndexOptions {
                    allow_no_indices: false,
                    expand_wildcards: open_and_hidden,
                    ..build_default_options()
                },
                b"{}",
            ),
            (
                "{\"index\":\"logs\",\"expand_wildcards\":[\"open\",\"hidden\"]}\n{}\n",
                Some("logs"),
                IndexOptions {
                    expand_wildcards: open_and_hidden,
                    ..build_default_options()
                },
                b"{}",
            ),
            (
                "{\"index\":\"logs\"}\r\n{\"size\":1}\r\n",
                Some("logs"),
                build_default_options(),
                b"{\"size\":1}",
            ),
            ("{\"index\":\"logs\"}\n\n", Some("logs"), build_default_options(), b"{}"),
            ("\n{\"size\":2}\n", None, build_default_options(), b"{\"size\":2}"),
        ];

        for (body, expected_index, expected_options, expected_body) in cases {
            let items = parse_msearch_body(&Bytes::from(body), build_default_options()).expect("a valid msearch body");

            assert_eq!(items.len(), 1, "{body:?}");
            assert_eq!(
                items[0].header.index.as_ref().map(IndexExpression::text),
                expected_index,
                "{body:?}"
            );
            assert_eq!(items[0].header.options, expected_options, "{body:?}");
            assert_eq!(items[0].body.as_ref(), expected_body, "{body:?}");
        }
    }

    #[tokio::test]
    async fn msearch_without_a_trailing_newline_is_a_400() {
        let body = Bytes::from_static(concat!(r#"{"index":"logs"}"#, "\n", "{}").as_bytes());

        let error = parse_msearch_body(&body, build_default_options())
            .err()
            .expect("a body without a trailing newline is refused");

        let (status, envelope) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(envelope["status"], 400);
        assert_eq!(envelope["error"]["type"], "illegal_argument_exception");
        assert_eq!(envelope["error"]["reason"], MISSING_TRAILING_NEWLINE_REASON);
    }

    #[tokio::test]
    async fn a_header_without_a_search_body_is_a_400() {
        let body =
            Bytes::from_static(concat!(r#"{"index":"logs"}"#, "\n", "{}\n", r#"{"index":"logs"}"#, "\n").as_bytes());

        let error = parse_msearch_body(&body, build_default_options())
            .err()
            .expect("a trailing header is refused");

        let (status, envelope) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(envelope["status"], 400);
        assert_eq!(envelope["error"]["type"], "illegal_argument_exception");
        assert_eq!(envelope["error"]["reason"], HEADER_WITHOUT_BODY_REASON);
    }

    #[tokio::test]
    async fn an_unknown_header_key_is_refused_naming_it() {
        let body = Bytes::from_static(concat!(r#"{"index":"logs","typed_keys":true}"#, "\n", "{}\n").as_bytes());

        let error = parse_msearch_body(&body, build_default_options())
            .err()
            .expect("an unknown header key is refused");

        let (status, envelope) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(envelope["status"], 400);
        assert_eq!(envelope["error"]["type"], "illegal_argument_exception");
        assert_eq!(envelope["error"]["key"], "typed_keys");
    }

    #[tokio::test]
    async fn a_header_naming_both_index_and_indices_is_a_400() {
        let body = Bytes::from_static(concat!(r#"{"index":"logs","indices":"spans"}"#, "\n", "{}\n").as_bytes());

        let error = parse_msearch_body(&body, build_default_options())
            .err()
            .expect("a header naming its indices twice is refused");

        let (status, envelope) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(envelope["status"], 400);
        assert_eq!(envelope["error"]["type"], "parsing_exception");
        assert_eq!(envelope["error"]["reason"], BOTH_INDEX_KEYS_REASON);
    }

    #[tokio::test]
    async fn an_empty_body_is_a_400() {
        let error = parse_msearch_body(&Bytes::new(), build_default_options())
            .err()
            .expect("an empty body is refused");

        let (status, envelope) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(envelope["status"], 400);
        assert_eq!(envelope["error"]["type"], "illegal_argument_exception");
        assert_eq!(envelope["error"]["reason"], NO_REQUESTS_REASON);
    }

    /// A header that cannot be read refuses the whole request: its item has no target to run
    /// against, and answering the others would hide a malformed request.
    #[tokio::test]
    async fn msearch_header_refusals_name_their_error_type() {
        let cases = [
            ("{\"index\":\n{}\n", "parsing_exception"),
            ("[\"logs\"]\n{}\n", "parsing_exception"),
            ("{\"index\":7}\n{}\n", "parsing_exception"),
            ("{\"index\":[\"logs\",7]}\n{}\n", "parsing_exception"),
            (
                "{\"index\":\"logs\",\"ignore_unavailable\":1}\n{}\n",
                "parsing_exception",
            ),
            (
                "{\"index\":\"logs\",\"expand_wildcards\":\"sideways\"}\n{}\n",
                "illegal_argument_exception",
            ),
            ("{\"index\":\"_foo\"}\n{}\n", "illegal_argument_exception"),
        ];

        for (body, expected_type) in cases {
            let error = parse_msearch_body(&Bytes::from(body), build_default_options())
                .err()
                .expect("an unreadable header is refused");

            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
            assert_eq!(envelope["status"], 400, "{body:?}");
            assert_eq!(envelope["error"]["type"], expected_type, "{body:?}");
        }
    }
}
