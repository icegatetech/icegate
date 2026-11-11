//! Scroll over system indices.
//!
//! IceGate keeps no per-scroll state: the scroll id is the cursor itself, base64url-encoded JSON
//! naming the snapshot object the initial search read, the concrete index, the offset of the next
//! page in `_id` order, the initial request, and the expiry the initial search fixed. A snapshot
//! object never changes and every page evaluates at the opening instant, so paging by offset
//! returns every matching document exactly once, on any replica, while writes continue. A
//! continuation validates the `scroll` it carries like the initial search does but never moves the
//! expiry: garbage collection keeps a superseded snapshot object for `max_scroll_lifetime_secs`
//! plus the query deadline after its successor appeared, which covers exactly the scrolls that end
//! at most `max_scroll_lifetime_secs` after they opened.
//!
//! The id is not signed, so a client can write its own. It can only name what its tenant may read
//! anyway: the snapshot path is the tenant's, the name must be a snapshot name and the index a
//! system index, and the request inside goes through the parser every search goes through.

use std::time::Duration;
use std::{iter, slice};

use axum::Json;
use axum::extract::State;
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::bulk::{build_json_response, serialize_bounded_answer};
use super::search::{find_system_indices, measure_took, search_system_indices};
use super::snapshot::{SystemSnapshot, validate_system_index_name};
use super::store::{CurrentSnapshot, SNAPSHOT_NAME_PREFIX, run_blocking_work};
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{
    EndpointParameters, NoParameters, Parameters, PathParameters, RequestBody, parse_parameters,
};
use crate::opensearch::search::request::{SearchParameters, SearchRequest, parse_search_body, parse_search_value};
use crate::opensearch::state::OpenSearchState;

/// The position of one scroll: everything a continuation needs, carried by the scroll id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ScrollCursor {
    /// The snapshot object the initial search read (`snap-<uuid>`).
    pub(crate) snapshot_name: String,
    /// The concrete system index the scroll pages.
    pub(crate) index: String,
    /// How many matching documents, in `_id` order, earlier pages returned.
    pub(crate) offset: usize,
    /// The request time of the initial search, in epoch milliseconds.
    pub(crate) opened_at_ms: i64,
    /// The keep-alive the initial search asked for, in milliseconds.
    pub(crate) lifetime_ms: i64,
    /// The query string of the initial search, which continuations parse again.
    pub(crate) query_string: String,
    /// The body of the initial search (`null` for none), which continuations parse again.
    pub(crate) body: Value,
}

impl ScrollCursor {
    /// The scroll id: base64url JSON without padding.
    ///
    /// # Errors
    ///
    /// `500` when the cursor does not serialize; an id is never minted from a failed encoding.
    pub(crate) fn encode(&self) -> Result<String, OpenSearchError> {
        let cursor_json = serde_json::to_vec(self)
            .map_err(|error| OpenSearchError::internal(format!("cannot encode a scroll cursor: {error}")))?;
        Ok(URL_SAFE_NO_PAD.encode(cursor_json))
    }

    /// Decodes a scroll id.
    ///
    /// # Errors
    ///
    /// `illegal_argument_exception` when `text` is not such an id, or names something other than
    /// a snapshot object and a system index.
    pub(crate) fn decode(text: &str) -> Result<Self, OpenSearchError> {
        let cursor: Self = URL_SAFE_NO_PAD
            .decode(text)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .ok_or_else(|| build_malformed_id_error(text))?;
        let is_snapshot_name = cursor
            .snapshot_name
            .strip_prefix(SNAPSHOT_NAME_PREFIX)
            .is_some_and(|uuid| Uuid::parse_str(uuid).is_ok());
        if !is_snapshot_name || validate_system_index_name(&cursor.index).is_err() {
            return Err(build_malformed_id_error(text));
        }
        Ok(cursor)
    }

    /// Whether the scroll ended before `now`: its expiry is the opening time plus the keep-alive
    /// of the initial search, at millisecond precision, and the expiry instant itself is served.
    pub(crate) const fn is_expired_at(&self, now: DateTime<Utc>) -> bool {
        now.timestamp_millis() > self.opened_at_ms.saturating_add(self.lifetime_ms)
    }

    /// The cursor of the page after one that returned `page_len` documents; the expiry is kept.
    #[must_use]
    pub(crate) fn advance(&self, page_len: usize) -> Self {
        Self {
            offset: self.offset.saturating_add(page_len),
            ..self.clone()
        }
    }
}

/// The refusal of a scroll id no replica minted.
fn build_malformed_id_error(scroll_id: &str) -> OpenSearchError {
    OpenSearchError::illegal_argument(format!("cannot parse scroll id [{scroll_id}]"))
}

/// The refusal of a `scroll` keep-alive that is not a `u64` amount followed by a known unit, or
/// whose amount in `d`, `h` or `m` overflows a `u64` of seconds.
fn build_malformed_lifetime_error(text: &str) -> OpenSearchError {
    OpenSearchError::illegal_argument(format!("failed to parse scroll keep-alive [{text}]"))
}

/// Reads a `scroll` keep-alive (`<integer><unit>`, unit `d`, `h`, `m`, `s`, `ms`, `micros` or
/// `nanos`) and refuses one above `maximum`.
///
/// # Errors
///
/// `illegal_argument_exception` for a malformed value or one above `maximum`.
pub(crate) fn parse_scroll_lifetime(text: &str, maximum: Duration) -> Result<Duration, OpenSearchError> {
    let digits_end = text.find(|character: char| !character.is_ascii_digit()).unwrap_or(text.len());
    let (digits, unit) = text.split_at(digits_end);
    let amount: u64 = digits.parse().map_err(|_| build_malformed_lifetime_error(text))?;
    let lifetime = match unit {
        "d" => amount.checked_mul(86_400).map(Duration::from_secs),
        "h" => amount.checked_mul(3_600).map(Duration::from_secs),
        "m" => amount.checked_mul(60).map(Duration::from_secs),
        "s" => Some(Duration::from_secs(amount)),
        "ms" => Some(Duration::from_millis(amount)),
        "micros" => Some(Duration::from_micros(amount)),
        "nanos" => Some(Duration::from_nanos(amount)),
        _ => None,
    }
    .ok_or_else(|| build_malformed_lifetime_error(text))?;
    if lifetime > maximum {
        return Err(OpenSearchError::illegal_argument(format!(
            "scroll keep-alive [{text}] is above opensearch.system_indices.max_scroll_lifetime_secs ({}s)",
            maximum.as_secs()
        )));
    }
    Ok(lifetime)
}

/// Decodes `scroll_id` for a continuation sent at `now`.
///
/// `requested_lifetime` is the continuation's `scroll`: it is validated against `maximum` like
/// the initial search's and otherwise ignored, so the cursor keeps the expiry the initial search
/// fixed.
///
/// # Errors
///
/// `illegal_argument_exception` for a malformed id or `scroll`, or an id whose own keep-alive is
/// negative or above `maximum`, which no initial search could have opened;
/// `search_context_missing_exception` when the scroll expired before `now`.
pub(crate) fn resume_scroll(
    scroll_id: &str,
    requested_lifetime: Option<&str>,
    maximum: Duration,
    now: DateTime<Utc>,
) -> Result<ScrollCursor, OpenSearchError> {
    let cursor = ScrollCursor::decode(scroll_id)?;
    let is_openable_lifetime =
        u128::try_from(cursor.lifetime_ms).is_ok_and(|lifetime_ms| lifetime_ms <= maximum.as_millis());
    if !is_openable_lifetime {
        return Err(build_malformed_id_error(scroll_id));
    }
    if let Some(requested) = requested_lifetime {
        parse_scroll_lifetime(requested, maximum)?;
    }
    if cursor.is_expired_at(now) {
        return Err(OpenSearchError::search_context_missing(scroll_id));
    }
    Ok(cursor)
}

/// Refuses a cursor opening more than `clock_tolerance` after `now`, which no replica minted.
///
/// A replica stamps the opening with its own clock, so a continuation served by a replica whose
/// clock lags may see an opening slightly ahead. `clock_tolerance` is the store's
/// `write_deadline`, the query deadline garbage collection adds to the retention on top of the
/// longest keep-alive, which is what makes that lag safe.
///
/// # Errors
///
/// `illegal_argument_exception` for an opening past the tolerance.
fn refuse_future_opening(
    cursor: &ScrollCursor,
    scroll_id: &str,
    now: DateTime<Utc>,
    clock_tolerance: Duration,
) -> Result<(), OpenSearchError> {
    let tolerance_ms = i64::try_from(clock_tolerance.as_millis()).unwrap_or(i64::MAX);
    if cursor.opened_at_ms > now.timestamp_millis().saturating_add(tolerance_ms) {
        return Err(build_malformed_id_error(scroll_id));
    }
    Ok(())
}

/// What the initial search of a scroll carries besides its parsed request.
pub(crate) struct ScrollOpening<'a> {
    /// The `scroll` keep-alive as sent.
    pub(crate) lifetime: &'a str,
    /// `opensearch.system_indices.max_scroll_lifetime_secs`.
    pub(crate) maximum_lifetime: Duration,
    /// The raw query string, re-parsed by every continuation.
    pub(crate) query_string: &'a str,
    /// The request body as JSON (`null` for none), re-parsed by every continuation.
    pub(crate) body: Value,
}

/// The first page of a scroll over the one system index `names` resolve to in `current_snapshot`,
/// with the scroll id of the next page as its first key.
///
/// # Errors
///
/// `illegal_argument_exception` for a malformed or too long `scroll`, a `sort` (a scroll pages
/// in `_id` order), a `from` other than 0, a `size` of 0, or names resolving to anything but one
/// index; the errors of [`find_system_indices`] and [`search_system_indices`].
pub(crate) fn open_system_scroll(
    current_snapshot: &CurrentSnapshot,
    names: &[String],
    request: &SearchRequest,
    opening: ScrollOpening<'_>,
    context: &RequestContext,
) -> Result<Value, OpenSearchError> {
    let lifetime = parse_scroll_lifetime(opening.lifetime, opening.maximum_lifetime)?;
    if !request.sort.is_empty() {
        return Err(OpenSearchError::illegal_argument(
            "a scroll over system indices pages in _id order and takes no [sort]",
        ));
    }
    if request.from != 0 {
        return Err(OpenSearchError::illegal_argument(
            "[from] is not allowed in a scroll context",
        ));
    }
    if request.size == 0 {
        return Err(OpenSearchError::illegal_argument(
            "[size] cannot be [0] in a scroll context",
        ));
    }
    let concrete_indices = find_system_indices(&current_snapshot.snapshot, names)?;
    let [(index, _)] = concrete_indices.as_slice() else {
        return Err(OpenSearchError::illegal_argument(
            "a scroll reads exactly one system index",
        ));
    };
    let snapshot_name = current_snapshot
        .snapshot_name
        .as_deref()
        .ok_or_else(|| OpenSearchError::internal("a resolved system index has no snapshot object"))?;
    let cursor = ScrollCursor {
        snapshot_name: snapshot_name.to_string(),
        index: (*index).to_string(),
        offset: 0,
        opened_at_ms: context.now.timestamp_millis(),
        lifetime_ms: i64::try_from(lifetime.as_millis())
            .map_err(|_| OpenSearchError::illegal_argument("the scroll keep-alive is out of range"))?,
        query_string: opening.query_string.to_string(),
        body: opening.body,
    };
    search_scroll_page(&current_snapshot.snapshot, &cursor, request, context)
}

/// The page of `snapshot` at `cursor` under `request`, with the scroll id of the next page as its
/// first key.
///
/// The page evaluates at the opening instant, not at the request's: a date-math bound would
/// otherwise move the matching documents under the offset, and pages would repeat or skip
/// documents and report different totals. `took` still measures this request.
///
/// # Errors
///
/// `illegal_argument_exception` for an opening time no instant represents; the errors of
/// [`search_system_indices`].
fn search_scroll_page(
    snapshot: &SystemSnapshot,
    cursor: &ScrollCursor,
    request: &SearchRequest,
    context: &RequestContext,
) -> Result<Value, OpenSearchError> {
    let opening_context = RequestContext {
        tenant: context.tenant.clone(),
        now: DateTime::from_timestamp_millis(cursor.opened_at_ms)
            .ok_or_else(|| OpenSearchError::illegal_argument("the scroll opening time is out of range"))?,
    };
    let mut page = search_system_indices(snapshot, slice::from_ref(&cursor.index), request, &opening_context)?;
    let took = page
        .get_mut("took")
        .ok_or_else(|| OpenSearchError::internal("a search answer carries took"))?;
    *took = json!(measure_took(context.now));
    let next_cursor = cursor.advance(count_page_hits(&page)?);
    prepend_scroll_id(page, next_cursor.encode()?)
}

/// The next page of the scroll at `cursor`: the initial request, parsed again, at the cursor's
/// offset.
///
/// # Errors
///
/// The errors of [`parse_parameters`], [`parse_search_value`] and [`search_scroll_page`], which a
/// cursor no search minted can reach.
fn read_scroll_page(
    snapshot: &SystemSnapshot,
    cursor: &ScrollCursor,
    context: &RequestContext,
) -> Result<Value, OpenSearchError> {
    let parameters = parse_parameters::<SearchParameters>(Some(&cursor.query_string))?;
    let mut request = parse_search_value(&cursor.body, &parameters)?;
    request.from = cursor.offset;
    search_scroll_page(snapshot, cursor, &request, context)
}

/// A search body as JSON: `null` when empty or blank, which a search reads as `{}`, else the
/// object it holds.
///
/// # Errors
///
/// `parsing_exception` for a body that is not JSON or not an object.
pub(crate) fn parse_body_value(body: &[u8]) -> Result<Value, OpenSearchError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Value::Null);
    }
    parse_search_body(body).map(Value::Object)
}

/// Query-string parameters of a scroll continuation.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScrollParameters {
    /// The keep-alive, validated and not applied (the expiry is the initial search's).
    scroll: Option<String>,
    /// The scroll id, when it is not in the path or the body.
    scroll_id: Option<String>,
}

impl EndpointParameters for ScrollParameters {
    /// A continuation renders its totals as the initial search asked: these are accepted and not
    /// read.
    const IGNORED: &'static [&'static str] = &["rest_total_hits_as_int", "track_total_hits"];
}

/// The body a continuation may carry.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScrollBody {
    /// The keep-alive, when it is not in the query string.
    scroll: Option<String>,
    /// The scroll id, when it is not in the path or the query string.
    scroll_id: Option<String>,
}

/// One scroll id or several.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ScrollIds {
    /// A single id.
    One(String),
    /// A list of ids.
    Many(Vec<String>),
}

/// The body of a scroll clearance.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClearScrollBody {
    /// The ids to clear, when the path names none.
    scroll_id: Option<ScrollIds>,
}

/// An optional JSON body: the default when empty or blank.
fn parse_optional_body<T: Default + DeserializeOwned>(body: &[u8]) -> Result<T, OpenSearchError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(body).map_err(|error| OpenSearchError::parsing(format!("malformed scroll body: {error}")))
}

/// `GET|POST /_search/scroll[/{scroll_id}]`: the next page of a system-index scroll.
///
/// The id comes from the path, else the query string, else the body; the keep-alive from the
/// query string, else the body. The page reads the snapshot object the scroll opened on, never a
/// newer one, and is searched and serialized on the blocking pool, like any `_search`.
///
/// # Errors
///
/// - `400 illegal_argument_exception` without an id, for an id no replica minted, a keep-alive
///   [`resume_scroll`] refuses, or a page past `max_answer_bytes`;
/// - `400 parsing_exception` for a malformed body;
/// - `404 search_context_missing_exception` when the scroll expired or its snapshot object is gone;
/// - `500` when the store cannot be read or the blocking work does not finish.
pub(crate) async fn continue_scroll(
    State(state): State<OpenSearchState>,
    context: RequestContext,
    path_id: Option<PathParameters<String>>,
    Parameters(parameters): Parameters<ScrollParameters>,
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    let scroll_body: ScrollBody = parse_optional_body(&body)?;
    let scroll_id = path_id
        .map(|PathParameters(id)| id)
        .or(parameters.scroll_id)
        .or(scroll_body.scroll_id)
        .ok_or_else(|| OpenSearchError::illegal_argument("a scroll continuation needs a scroll id"))?;
    let requested_lifetime = parameters.scroll.or(scroll_body.scroll);
    let cursor = resume_scroll(
        &scroll_id,
        requested_lifetime.as_deref(),
        state.system_store.max_scroll_lifetime(),
        context.now,
    )?;
    refuse_future_opening(&cursor, &scroll_id, context.now, state.system_store.write_deadline())?;
    let snapshot = state
        .system_store
        .read_snapshot_by_name(&context.tenant, &cursor.snapshot_name)
        .await?
        .ok_or_else(|| OpenSearchError::search_context_missing(&scroll_id))?;
    let max_answer_bytes = state.max_answer_bytes;
    let answer_body = run_blocking_work("reading a system-index scroll page", move || {
        let page = read_scroll_page(&snapshot, &cursor, &context)?;
        serialize_bounded_answer(&page, max_answer_bytes)
    })
    .await?;
    Ok(build_json_response(answer_body))
}

/// `DELETE /_search/scroll[/{scroll_id}]`: a scroll holds no server state, so every well-formed id
/// is freed as soon as it is named. Ids come from the path (comma-separated) or the body
/// (`scroll_id`, one or a list); `_all` frees nothing there is to free.
///
/// # Errors
///
/// `400 illegal_argument_exception` without an id or for a malformed one; `400 parsing_exception`
/// for a malformed body.
pub(crate) async fn clear_scroll(
    path_ids: Option<PathParameters<String>>,
    Parameters(_): Parameters<NoParameters>,
    RequestBody(body): RequestBody,
) -> Result<Json<Value>, OpenSearchError> {
    let clearance: ClearScrollBody = parse_optional_body(&body)?;
    let freed_count = match (path_ids, clearance.scroll_id) {
        (Some(PathParameters(ids)), _) => count_freed_scrolls(ids.split(','))?,
        (None, Some(ScrollIds::One(id))) => count_freed_scrolls(iter::once(id.as_str()))?,
        (None, Some(ScrollIds::Many(ids))) => count_freed_scrolls(ids.iter().map(String::as_str))?,
        (None, None) => {
            return Err(OpenSearchError::illegal_argument("clearing a scroll needs a scroll id"));
        }
    };
    Ok(Json(json!({"succeeded": true, "num_freed": freed_count})))
}

/// How many scrolls clearing `ids` frees: none when one of them is `_all`, else one per id.
///
/// # Errors
///
/// `illegal_argument_exception` for a malformed id, unless one of `ids` is `_all`.
fn count_freed_scrolls<'a>(mut ids: impl Iterator<Item = &'a str> + Clone) -> Result<usize, OpenSearchError> {
    if ids.clone().any(|id| id == "_all") {
        return Ok(0);
    }
    ids.try_fold(0, |freed_count, id| ScrollCursor::decode(id).map(|_| freed_count + 1))
}

/// How many hits a search answer holds.
///
/// # Errors
///
/// `500` for an answer without a `hits.hits` list, which no search renders: a count of zero would
/// mint a cursor that never advances.
fn count_page_hits(page: &Value) -> Result<usize, OpenSearchError> {
    page["hits"]["hits"]
        .as_array()
        .map(Vec::len)
        .ok_or_else(|| OpenSearchError::internal("a search answer carries hits.hits"))
}

/// `page` with `_scroll_id` as its first key.
fn prepend_scroll_id(page: Value, scroll_id: String) -> Result<Value, OpenSearchError> {
    let Value::Object(fields) = page else {
        return Err(OpenSearchError::internal("a search answer is a JSON object"));
    };
    let mut answer = Map::with_capacity(fields.len() + 1);
    answer.insert("_scroll_id".to_string(), Value::String(scroll_id));
    answer.extend(fields);
    Ok(Value::Object(answer))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::http::StatusCode;
    use chrono::{DateTime, TimeDelta, Utc};
    use icegate_common::TenantId;
    use serde_json::{Value, json};

    use super::{
        ClearScrollBody, CurrentSnapshot, ScrollBody, ScrollCursor, ScrollIds, ScrollOpening, SystemSnapshot,
        open_system_scroll, parse_body_value, parse_optional_body, parse_scroll_lifetime, prepend_scroll_id,
        read_scroll_page, refuse_future_opening, resume_scroll,
    };
    use crate::opensearch::context::RequestContext;
    use crate::opensearch::error::{OpenSearchError, render_envelope};
    use crate::opensearch::extract::parse_parameters;
    use crate::opensearch::search::request::parse_search_request;
    use crate::opensearch::system_index::snapshot::{DocumentWrite, OpType, SystemOperation, WriteConcurrency};

    const FIFTEEN_MINUTES: Duration = Duration::from_mins(15);

    fn parse_opening_instant() -> DateTime<Utc> {
        "2026-10-08T12:00:00.000Z".parse().expect("a literal instant")
    }

    fn build_cursor() -> ScrollCursor {
        ScrollCursor {
            snapshot_name: "snap-0b0f8c6e-3c1d-4d7c-9a51-6f2b8f0c9e11".to_owned(),
            index: ".kibana_1".to_owned(),
            offset: 0,
            opened_at_ms: parse_opening_instant().timestamp_millis(),
            lifetime_ms: 900_000,
            query_string: "scroll=15m".to_owned(),
            body: json!({"size": 100}),
        }
    }

    /// `.kibana_1` holding `a`, `b` and `c`, last updated 59, 30 and 5 minutes before
    /// [`parse_opening_instant`].
    fn build_dated_snapshot() -> SystemSnapshot {
        let mut snapshot = SystemSnapshot::default();
        snapshot
            .apply(&SystemOperation::CreateIndex {
                name: ".kibana_1".to_string(),
                mappings: json!({"properties": {"updated_at": {"type": "date"}}}),
                settings: json!({}),
            })
            .expect("the index is created");
        for (id, updated_at) in [
            ("a", "2026-10-08T11:01:00.000Z"),
            ("b", "2026-10-08T11:30:00.000Z"),
            ("c", "2026-10-08T11:55:00.000Z"),
        ] {
            snapshot
                .apply(&SystemOperation::IndexDocument(DocumentWrite {
                    index: ".kibana_1".to_string(),
                    id: id.to_string(),
                    source: json!({"updated_at": updated_at}),
                    op_type: OpType::Index,
                    concurrency: WriteConcurrency::default(),
                }))
                .expect("the document is written");
        }
        snapshot
    }

    fn build_context_at(now: DateTime<Utc>) -> RequestContext {
        RequestContext {
            tenant: TenantId::new("tenant-a").expect("a valid tenant"),
            now,
        }
    }

    fn read_hit_ids(page: &Value) -> Vec<&str> {
        page["hits"]["hits"]
            .as_array()
            .expect("hits is an array")
            .iter()
            .map(|hit| hit["_id"].as_str().expect("an id"))
            .collect()
    }

    /// Assert `error` is the `400` a malformed `input` is refused with.
    async fn assert_illegal_argument(error: OpenSearchError, input: &str) {
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{input:?}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception", "{input:?}");
    }

    #[tokio::test]
    async fn scroll_cursors_round_trip_and_a_malformed_one_is_refused() {
        let encoded = build_cursor().encode().expect("encodes");

        assert!(
            encoded
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        );
        assert_eq!(ScrollCursor::decode(&encoded).expect("decodes"), build_cursor());
        let pointer_name = ScrollCursor {
            snapshot_name: "current".to_owned(),
            ..build_cursor()
        }
        .encode()
        .expect("encodes");
        let data_index = ScrollCursor {
            index: "logs".to_owned(),
            ..build_cursor()
        }
        .encode()
        .expect("encodes");
        // "eyJ9" is `{"}` and "e30" is `{}`: base64url, but not a cursor.
        for malformed_id in [
            "",
            "not base64!",
            "eyJ9",
            "e30",
            pointer_name.as_str(),
            data_index.as_str(),
        ] {
            let error = ScrollCursor::decode(malformed_id).expect_err("refused");

            assert_illegal_argument(error, malformed_id).await;
        }
    }

    #[tokio::test]
    async fn a_scroll_lifetime_above_the_maximum_is_refused() {
        let one_hour = Duration::from_hours(1);
        let one_day = Duration::from_hours(24);
        for (text, maximum, expected_lifetime) in [
            ("0s", FIFTEEN_MINUTES, Duration::ZERO),
            ("14m", FIFTEEN_MINUTES, Duration::from_mins(14)),
            ("15m", FIFTEEN_MINUTES, FIFTEEN_MINUTES),
            ("900s", FIFTEEN_MINUTES, FIFTEEN_MINUTES),
            ("900000ms", FIFTEEN_MINUTES, FIFTEEN_MINUTES),
            ("900000000micros", FIFTEEN_MINUTES, FIFTEEN_MINUTES),
            ("900000000000nanos", FIFTEEN_MINUTES, FIFTEEN_MINUTES),
            ("1h", one_hour, one_hour),
            ("1d", one_day, one_day),
            // The last whole day a `u64` of seconds holds: 213,503,982,334,601 days of 24 hours.
            (
                "213503982334601d",
                Duration::MAX,
                Duration::from_hours(5_124_095_576_030_424),
            ),
        ] {
            assert_eq!(
                parse_scroll_lifetime(text, maximum).ok(),
                Some(expected_lifetime),
                "{text}"
            );
        }
        for above_maximum_text in [
            "16m",
            "901s",
            "900001ms",
            "900000001micros",
            "900000000001nanos",
            "1h",
            "1d",
        ] {
            let error = parse_scroll_lifetime(above_maximum_text, FIFTEEN_MINUTES).expect_err("refused");

            assert_illegal_argument(error, above_maximum_text).await;
        }
        // One unit past what a `u64` of seconds holds, for each unit that multiplies, and one past
        // what the amount itself holds.
        for overflowing_text in [
            "213503982334602d",
            "5124095576030432h",
            "307445734561825861m",
            "18446744073709551616s",
        ] {
            let error = parse_scroll_lifetime(overflowing_text, Duration::MAX).expect_err("refused");

            assert_illegal_argument(error, overflowing_text).await;
        }
        for malformed_text in ["", "15", "m", "1.5m", "-1", "15 m", "15M"] {
            let error = parse_scroll_lifetime(malformed_text, FIFTEEN_MINUTES).expect_err("refused");

            assert_illegal_argument(error, malformed_text).await;
        }
    }

    #[tokio::test]
    async fn a_continuation_at_its_lifetime_is_served_and_one_millisecond_past_it_is_missing() {
        let scroll_id = build_cursor().encode().expect("encodes");
        let expiry_instant = parse_opening_instant() + TimeDelta::minutes(15);

        assert!(resume_scroll(&scroll_id, None, FIFTEEN_MINUTES, expiry_instant).is_ok());
        let error = resume_scroll(
            &scroll_id,
            None,
            FIFTEEN_MINUTES,
            expiry_instant + TimeDelta::milliseconds(1),
        )
        .expect_err("expired");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["type"], "search_context_missing_exception");
    }

    #[tokio::test]
    async fn a_continuation_never_extends_the_expiry_set_by_the_initial_search() {
        let scroll_id = build_cursor().encode().expect("encodes");
        let opening_instant = parse_opening_instant();

        let resumed_cursor = resume_scroll(
            &scroll_id,
            Some("15m"),
            FIFTEEN_MINUTES,
            opening_instant + TimeDelta::minutes(10),
        )
        .expect("served ten minutes in");
        let next_id = resumed_cursor.advance(100).encode().expect("encodes");

        let next_cursor = ScrollCursor::decode(&next_id).expect("decodes");
        assert_eq!(next_cursor.offset, 100);
        assert_eq!(next_cursor.opened_at_ms, build_cursor().opened_at_ms);
        assert_eq!(next_cursor.lifetime_ms, 900_000);
        let past_expiry_instant = opening_instant + TimeDelta::minutes(15) + TimeDelta::milliseconds(1);
        let expiry_error =
            resume_scroll(&next_id, Some("15m"), FIFTEEN_MINUTES, past_expiry_instant).expect_err("expired");
        let (status, body) = render_envelope(expiry_error).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["type"], "search_context_missing_exception");
        let keep_alive_error =
            resume_scroll(&next_id, Some("16m"), FIFTEEN_MINUTES, opening_instant).expect_err("above the maximum");
        assert_illegal_argument(keep_alive_error, "16m").await;
    }

    /// A forged keep-alive would outlive what garbage collection retains: `resume_scroll` is what
    /// makes the expiry check mean anything for an id it did not mint.
    #[tokio::test]
    async fn a_cursor_whose_keep_alive_no_search_could_open_is_malformed() {
        let opening_instant = parse_opening_instant();
        let at_maximum_id = build_cursor().encode().expect("encodes");

        assert!(resume_scroll(&at_maximum_id, None, FIFTEEN_MINUTES, opening_instant).is_ok());
        for lifetime_ms in [900_001, -1] {
            let forged_id = ScrollCursor {
                lifetime_ms,
                ..build_cursor()
            }
            .encode()
            .expect("encodes");

            let error = resume_scroll(&forged_id, None, FIFTEEN_MINUTES, opening_instant).expect_err("refused");

            assert_illegal_argument(error, &lifetime_ms.to_string()).await;
        }
    }

    #[tokio::test]
    async fn a_cursor_opening_past_the_clock_tolerance_is_malformed() {
        let now = parse_opening_instant();
        let clock_tolerance = Duration::from_secs(30);
        let build_cursor_opened_at = |offset_ms: i64| ScrollCursor {
            opened_at_ms: now.timestamp_millis() + offset_ms,
            ..build_cursor()
        };

        for offset_ms in [-1, 0, 29_999, 30_000] {
            assert!(
                refuse_future_opening(&build_cursor_opened_at(offset_ms), "id", now, clock_tolerance).is_ok(),
                "{offset_ms}"
            );
        }
        let error =
            refuse_future_opening(&build_cursor_opened_at(30_001), "id", now, clock_tolerance).expect_err("refused");
        assert_illegal_argument(error, "30001").await;
        // A tolerance past what the milliseconds hold saturates instead of overflowing.
        let last_opening = build_cursor_opened_at(i64::MAX - now.timestamp_millis());
        assert!(refuse_future_opening(&last_opening, "id", now, Duration::MAX).is_ok());
    }

    /// Ten minutes in, `now-1h` would no longer reach `a`: evaluated at the continuation's own
    /// `now`, the total would drop to 2 and offset 1 would skip `b`.
    #[test]
    fn every_page_evaluates_date_math_at_the_opening_instant() {
        let cursor = ScrollCursor {
            offset: 1,
            body: json!({"size": 1, "query": {"range": {"updated_at": {"gte": "now-1h"}}}}),
            ..build_cursor()
        };
        let context = build_context_at(parse_opening_instant() + TimeDelta::minutes(10));

        let page = read_scroll_page(&build_dated_snapshot(), &cursor, &context).expect("a page");

        assert_eq!(page["hits"]["total"], json!({"value": 3, "relation": "eq"}));
        assert_eq!(read_hit_ids(&page), ["b"]);
        let next_id = page["_scroll_id"].as_str().expect("a scroll id");
        assert_eq!(
            ScrollCursor::decode(next_id).expect("decodes"),
            ScrollCursor { offset: 2, ..cursor }
        );
    }

    /// The page evaluates at an opening ten minutes old, which must not read as ten minutes of work.
    #[test]
    fn a_continuation_reports_the_time_of_its_own_request_as_took() {
        let now = Utc::now();
        let cursor = ScrollCursor {
            opened_at_ms: (now - TimeDelta::minutes(10)).timestamp_millis(),
            ..build_cursor()
        };

        let page = read_scroll_page(&build_dated_snapshot(), &cursor, &build_context_at(now)).expect("a page");

        let took = page["took"].as_i64().expect("an integer took");
        assert!((0..600_000).contains(&took), "{took}");
    }

    /// A scroll opened without a body or a query string of its own: `null` reads as `{}`.
    #[test]
    fn a_cursor_without_a_body_applies_the_search_defaults() {
        let cursor = ScrollCursor {
            query_string: String::new(),
            body: Value::Null,
            ..build_cursor()
        };

        let page = read_scroll_page(
            &build_dated_snapshot(),
            &cursor,
            &build_context_at(parse_opening_instant()),
        )
        .expect("a page");

        assert_eq!(read_hit_ids(&page), ["a", "b", "c"]);
    }

    #[tokio::test]
    async fn a_cursor_with_an_opening_no_instant_represents_is_refused() {
        let cursor = ScrollCursor {
            opened_at_ms: i64::MAX,
            ..build_cursor()
        };

        let error = read_scroll_page(
            &build_dated_snapshot(),
            &cursor,
            &build_context_at(parse_opening_instant()),
        )
        .expect_err("refused");

        assert_illegal_argument(error, "i64::MAX").await;
    }

    /// A keep-alive past what `lifetime_ms` holds: only a maximum no configuration should hold lets
    /// one through, and the scroll is refused rather than given a truncated expiry.
    #[tokio::test]
    async fn a_keep_alive_past_the_cursors_milliseconds_is_refused() {
        let current_snapshot = CurrentSnapshot {
            snapshot_name: Some(Arc::from("snap-0b0f8c6e-3c1d-4d7c-9a51-6f2b8f0c9e11")),
            snapshot: Arc::new(build_dated_snapshot()),
        };
        let parameters = parse_parameters(None).expect("the parameters parse");
        let request = parse_search_request(b"", &parameters).expect("the request parses");
        let opening = ScrollOpening {
            // `i64::MAX` milliseconds are 106,751,991,167 whole days and a part of one.
            lifetime: "106751991168d",
            maximum_lifetime: Duration::MAX,
            query_string: "scroll=106751991168d",
            body: Value::Null,
        };

        let error = open_system_scroll(
            &current_snapshot,
            &[".kibana_1".to_string()],
            &request,
            opening,
            &build_context_at(parse_opening_instant()),
        )
        .expect_err("refused");

        assert_illegal_argument(error, "106751991168d").await;
    }

    /// A literal `null` is refused: kept in a cursor, it would read as no body, which the same
    /// body sent to a plain search is not.
    #[tokio::test]
    async fn a_body_is_read_as_a_json_object_and_a_blank_one_as_null() {
        for (body, expected_value) in [
            (&b""[..], Value::Null),
            (&b" \n"[..], Value::Null),
            (&br#"{"size": 2}"#[..], json!({"size": 2})),
        ] {
            assert_eq!(parse_body_value(body).expect("parses"), expected_value);
        }

        for refused_body in [&b"{"[..], &b"null"[..], &b"[]"[..]] {
            let (status, answer) = render_envelope(parse_body_value(refused_body).expect_err("refused")).await;

            assert_eq!(status, StatusCode::BAD_REQUEST, "{refused_body:?}");
            assert_eq!(answer["error"]["type"], "parsing_exception", "{refused_body:?}");
        }
    }

    /// A cursor carries the request its continuations parse again, so a forged one reaches the
    /// refusals of the query-string and body parsers.
    #[tokio::test]
    async fn a_cursor_whose_request_does_not_parse_is_refused() {
        let snapshot = build_dated_snapshot();
        let context = build_context_at(parse_opening_instant());
        let unknown_parameter_cursor = ScrollCursor {
            query_string: "unknown=1".to_owned(),
            ..build_cursor()
        };
        let malformed_body_cursor = ScrollCursor {
            body: json!({"size": "x"}),
            ..build_cursor()
        };

        let parameter_error = read_scroll_page(&snapshot, &unknown_parameter_cursor, &context).expect_err("refused");
        let body_error = read_scroll_page(&snapshot, &malformed_body_cursor, &context).expect_err("refused");

        let (parameter_status, parameter_answer) = render_envelope(parameter_error).await;
        assert_eq!(parameter_status, StatusCode::BAD_REQUEST);
        assert_eq!(parameter_answer["error"]["type"], "illegal_argument_exception");
        let (body_status, body_answer) = render_envelope(body_error).await;
        assert_eq!(body_status, StatusCode::BAD_REQUEST);
        assert_eq!(body_answer["error"]["type"], "parsing_exception");
    }

    #[tokio::test]
    async fn an_optional_scroll_body_defaults_when_blank_and_refuses_what_it_does_not_read() {
        for blank_body in [&b""[..], &b" \n"[..]] {
            let scroll_body: ScrollBody = parse_optional_body(blank_body).expect("the default");

            assert_eq!(scroll_body.scroll, None);
            assert_eq!(scroll_body.scroll_id, None);
        }
        let clearance: ClearScrollBody = parse_optional_body(br#"{"scroll_id": ["a", "b"]}"#).expect("a list of ids");
        assert!(matches!(clearance.scroll_id, Some(ScrollIds::Many(ids)) if ids == ["a", "b"]));

        for malformed_body in [&b"{"[..], &br#"{"keep_alive": "1m"}"#[..]] {
            let error = parse_optional_body::<ScrollBody>(malformed_body).expect_err("refused");
            let (status, answer) = render_envelope(error).await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(answer["error"]["type"], "parsing_exception");
        }
    }

    #[tokio::test]
    async fn a_scroll_id_is_the_first_key_and_a_page_that_is_no_object_is_a_500() {
        let answer = prepend_scroll_id(json!({"took": 1, "hits": {}}), "id".to_owned()).expect("an object");

        let keys: Vec<&String> = answer.as_object().expect("an object").keys().collect();
        assert_eq!(keys, ["_scroll_id", "took", "hits"]);
        let (status, envelope) =
            render_envelope(prepend_scroll_id(json!([]), "id".to_owned()).expect_err("refused")).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(envelope["error"]["type"], "exception");
    }
}
