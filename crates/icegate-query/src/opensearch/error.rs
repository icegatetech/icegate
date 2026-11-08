//! The `OpenSearch` error envelope.
//!
//! Every failure the listener answers is rendered here: handler errors, middleware refusals
//! (shed, deadline, tenant) and unmatched routes, so a client never reads a bare string or an
//! empty body. The shape is
//! `{"error": {"root_cause": [{type, reason, <metadata>}], type, reason, <metadata>}, "status": N}`,
//! where the metadata keys (for example the missing `index`) are present only on errors that carry
//! them; a `503` adds `details` beside `reason` and a `Retry-After` header. An error that wraps a
//! shard-level failure renders that failure as its root cause instead of repeating its own keys.
//!
//! `503` is reserved for the conditions a client should retry: memory-pressure shedding, the
//! query deadline and system-index write contention. A limit a request can avoid by asking for
//! less is a `400`, or a `413` for a body or a snapshot that is too large.

use std::collections::BTreeMap;

use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use icegate_common::SHED_RETRY_AFTER_SECS;
use icegate_common::schema::COL_TIMESTAMP;
use serde_json::{Map, Value, json};

use crate::error::QueryError;

/// Content type of every envelope.
const CONTENT_TYPE_JSON: &str = "application/json; charset=UTF-8";

/// An error answered in the `OpenSearch` envelope.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpenSearchError {
    /// HTTP status of the answer, repeated as the envelope's `status`.
    status: StatusCode,
    /// The envelope's `type`, on the error and, unless `root_cause` replaces it, on its root cause.
    error_type: &'static str,
    /// The envelope's `reason`, on the error and, unless `root_cause` replaces it, on its root
    /// cause.
    reason: String,
    /// Keys rendered beside `type` and `reason` on the error and, unless `root_cause` replaces
    /// it, on its root cause. This map and `root_cause` are boxed so that every
    /// `Result<_, OpenSearchError>` stays small (clippy `result_large_err`), and the allocation
    /// lands on the failing path.
    metadata: Box<Map<String, Value>>,
    /// The cause rendered as `root_cause[0]` in place of this error's own, for an error that
    /// wraps a shard-level failure.
    root_cause: Option<Box<Map<String, Value>>>,
}

impl OpenSearchError {
    /// An error of `error_type` answered with `status`.
    fn new(status: StatusCode, error_type: &'static str, reason: impl Into<String>) -> Self {
        Self {
            status,
            error_type,
            reason: reason.into(),
            metadata: Box::default(),
            root_cause: None,
        }
    }

    /// A well-formed request that asks for something invalid, names no usable tenant or carries
    /// an unknown parameter: `400`, `illegal_argument_exception`.
    pub(crate) fn illegal_argument(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "illegal_argument_exception", reason)
    }

    /// The 400 a query gets when the listener's per-query memory pool cannot grant a
    /// reservation even after spilling: `illegal_argument_exception` naming the limit
    /// and the configuration key that sets it, with `limit` carrying the limit in bytes
    /// for a client.
    pub(crate) fn memory_limit_exceeded(limit_bytes: usize) -> Self {
        Self::illegal_argument(format!(
            "the query needs more memory than the per-query limit of [{limit_bytes}] bytes \
             (opensearch.max_query_memory_bytes)"
        ))
        .with_metadata("limit", limit_bytes)
    }

    /// The process sheds load under memory pressure: `503`, `memory_pressure_exception`,
    /// retryable.
    pub(crate) fn memory_pressure() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "memory_pressure_exception",
            "service under memory pressure, retry later",
        )
    }

    /// The request outlived `engine.max_query_duration_secs`: `503`,
    /// `query_deadline_exceeded_exception`, retryable.
    pub(crate) fn deadline_exceeded() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "query_deadline_exceeded_exception",
            "query exceeded engine.max_query_duration_secs",
        )
    }

    /// A failure inside IceGate that the request could not have avoided: `500`, `exception`.
    pub(crate) fn internal(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "exception", reason)
    }

    /// A request body or query the parser refuses, an unsupported construct or an unknown body
    /// key included (`400`).
    pub(crate) fn parsing(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "parsing_exception", reason)
    }

    /// A query construct IceGate does not evaluate (`400`, `parsing_exception`), which `construct`
    /// carries for a client.
    pub(crate) fn unsupported_construct(construct: &str, reason: impl Into<String>) -> Self {
        Self::parsing(reason).with_metadata("construct", construct)
    }

    /// An aggregation of the request this listener refuses to answer, such as a nesting it does
    /// not plan (`400`, `illegal_argument_exception`); `aggregation` carries the request's name
    /// for it.
    pub(crate) fn unsupported_aggregation(aggregation: &str, reason: impl Into<String>) -> Self {
        Self::illegal_argument(reason).with_metadata("aggregation", aggregation)
    }

    /// An aggregation type of IceGate's surface that this listener does not plan yet (`400`,
    /// `illegal_argument_exception`); `aggregation` carries the request's name for it and
    /// `aggregation_type` the type.
    pub(crate) fn unplanned_aggregation_type(aggregation: &str, aggregation_type: &str) -> Self {
        Self::unsupported_aggregation(
            aggregation,
            format!("aggregation [{aggregation_type}] of [{aggregation}] is not supported yet"),
        )
        .with_metadata("aggregation_type", aggregation_type)
    }

    /// A field an aggregation cannot read in one index (`400`, `illegal_argument_exception`):
    /// `field`, its `field_type`, the `index` mapping it so, the `aggregation` and its
    /// `aggregation_type` are carried for a client.
    pub(crate) fn unsupported_aggregation_field(
        field: &str,
        field_type: &str,
        index: &str,
        aggregation: &str,
        aggregation_type: &str,
    ) -> Self {
        Self::unsupported_aggregation(
            aggregation,
            format!(
                "field [{field}] of type [{field_type}] in [{index}] is not supported for aggregation \
                 [{aggregation_type}] [{aggregation}]"
            ),
        )
        .with_metadata("aggregation_type", aggregation_type)
        .with_metadata("field", field)
        .with_metadata("field_type", field_type)
        .with_metadata("index", index)
    }

    /// A regular expression `include` or `exclude`, named by `filter_key`, of an aggregation over
    /// a field holding encoded ids, which take a list of hex ids instead (`400`,
    /// `illegal_argument_exception`): `aggregation` carries the request's name for it and
    /// `field_storage` is `encoded_id`.
    pub(crate) fn pattern_on_encoded_ids(aggregation: &str, filter_key: &str) -> Self {
        Self::unsupported_aggregation(
            aggregation,
            format!(
                "[{filter_key}] of [{aggregation}] is a regular expression; its field holds encoded ids, which \
                 take a list of hex ids, not a pattern"
            ),
        )
        .with_metadata("field_storage", "encoded_id")
    }

    /// A request holding more bucket aggregations, nested or not, than one aggregate groups by
    /// (`400`, `illegal_argument_exception`); `aggregation` carries the request's name for the
    /// top-level aggregation whose buckets pass the limit and `limit` the most bucket
    /// aggregations one request may hold.
    pub(crate) fn aggregation_depth_exceeded(aggregation: &str, limit: usize) -> Self {
        Self::unsupported_aggregation(
            aggregation,
            format!("a request may hold at most {limit} bucket aggregations; [{aggregation}] passes it"),
        )
        .with_metadata("limit", limit)
    }

    /// A query text past a limit of its parser (`400`, `parsing_exception`), whose value `limit`
    /// carries for a client.
    pub(crate) fn query_limit_exceeded(reason: impl Into<String>, limit: impl Into<Value>) -> Self {
        Self::parsing(reason).with_metadata("limit", limit)
    }

    /// The request body exceeds `opensearch.max_request_body_bytes` (`413`).
    pub(crate) fn payload_too_large() -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "illegal_argument_exception",
            "request body exceeds opensearch.max_request_body_bytes",
        )
    }

    /// Attach `key: value` to the error and, unless `root_cause` replaces it, to its root cause.
    fn with_metadata(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.metadata.insert(key.to_string(), value.into());
        self
    }

    /// The error object without `root_cause`: `type`, `reason` and every metadata entry, as a
    /// `_bulk` or `_mget` item carries its failure and as the envelope's error renders it, and its
    /// root cause too unless the error wraps a shard-level failure.
    pub(crate) fn render_cause(&self) -> Map<String, Value> {
        let mut cause = Map::new();
        cause.insert("type".to_string(), Value::String(self.error_type.to_string()));
        cause.insert("reason".to_string(), Value::String(self.reason.clone()));
        for (key, value) in &*self.metadata {
            cause.insert(key.clone(), value.clone());
        }
        cause
    }

    /// The HTTP status of this error, which a `_bulk` item answers as its `status`.
    pub(crate) const fn status(&self) -> StatusCode {
        self.status
    }

    /// The envelope's `reason`, which a refusal that wraps this error quotes as its cause.
    pub(crate) fn reason(&self) -> &str {
        &self.reason
    }

    /// `index` does not exist (`404`). `index` is the concrete name that is missing, or the whole
    /// expression when it matched nothing and the endpoint disallows an empty match.
    pub(crate) fn index_not_found(index: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "index_not_found_exception",
            format!("no such index [{index}]"),
        )
        .with_metadata("index", index)
        .with_metadata("index_uuid", "_na_")
        .with_metadata("resource.id", index)
        .with_metadata("resource.type", "index_or_alias")
    }

    /// A search-phase failure (`400`, phase `query`, grouped) of one shard whose cause is
    /// `root_cause`. The cause renders as the failed shard's `reason` and as `root_cause[0]`; the
    /// failed shard names `index` when one is given.
    fn search_phase_failure(index: Option<&str>, root_cause: &Self) -> Self {
        let shard_cause = root_cause.render_cause();
        let mut failed_shard = Map::new();
        failed_shard.insert("shard".to_string(), json!(0));
        if let Some(index) = index {
            failed_shard.insert("index".to_string(), json!(index));
        }
        failed_shard.insert("reason".to_string(), Value::Object(shard_cause.clone()));
        let mut error = Self::new(
            StatusCode::BAD_REQUEST,
            "search_phase_execution_exception",
            "all shards failed",
        )
        .with_metadata("phase", "query")
        .with_metadata("grouped", true)
        .with_metadata("failed_shards", Value::Array(vec![Value::Object(failed_shard)]));
        error.root_cause = Some(Box::new(shard_cause));
        error
    }

    /// A query that failed on the shard of `index` (`400`): a sort on a field the mapping lacks
    /// without `unmapped_type`, a sort on a field that cannot be sorted on, or a leaf on a field
    /// of the wrong type. Rendered as a `search_phase_execution_exception` (phase `query`,
    /// grouped, one failed shard) whose root cause is the `query_shard_exception`; the entry of
    /// `failed_shards` carries `shard`, `index` and `reason`.
    pub(crate) fn query_shard(index: &str, reason: &str) -> Self {
        let shard_cause = Self::new(StatusCode::BAD_REQUEST, "query_shard_exception", reason)
            .with_metadata("index", index)
            .with_metadata("index_uuid", "_na_");
        Self::search_phase_failure(Some(index), &shard_cause)
    }

    /// `from + size` exceeds `search.max_result_window` (`400`, a search-phase failure whose root
    /// cause is an `illegal_argument_exception`). No index is named: the window is checked before
    /// any index is read.
    pub(crate) fn result_window_too_large(from: usize, size: usize, max: usize) -> Self {
        let cause = Self::illegal_argument(format!(
            "result window is too large: from + size must be less than or equal to [{max}] but was [{}]",
            from.saturating_add(size)
        ));
        Self::search_phase_failure(None, &cause)
    }

    /// One search's aggregations would answer more than `max` buckets (`400`).
    ///
    /// `actual` is the count reached when the limit was crossed: counting stops there, so it is
    /// at least `max + 1` and not the size a complete answer would have. The status is `400`
    /// rather than `503` because retrying cannot shrink the answer.
    pub(crate) fn too_many_buckets(max: usize, actual: usize) -> Self {
        Self::search_phase_failure(
            None,
            &Self::new(
                StatusCode::BAD_REQUEST,
                "too_many_buckets_exception",
                format!(
                    "Trying to create too many buckets. Must be less than or equal to: [{max}] but this \
                     search reached [{actual}]. Narrow the time range or the interval, or raise \
                     opensearch.search.max_buckets."
                ),
            )
            .with_metadata("max_buckets", max),
        )
    }

    /// An aggregation the searched indices cannot answer together, such as one over a field
    /// typed differently across them (`400`). Buckets of keys of different types would otherwise
    /// share one list.
    pub(crate) fn aggregation_execution(reason: &str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "aggregation_execution_exception", reason)
    }

    /// An aggregation over `field`, which the searched indices give more than one type, mapped or
    /// stored (`400`, `aggregation_execution_exception`). `field` and `indices_by_type` (each type
    /// name mapped to the array of indices giving the field that type) are carried for a client.
    pub(crate) fn aggregation_field_conflict(field: &str, reason: &str, indices_by_type: Map<String, Value>) -> Self {
        Self::aggregation_execution(reason)
            .with_metadata("field", field)
            .with_metadata("indices_by_type", Value::Object(indices_by_type))
    }

    /// `_count` addressed to the data indices `index_names`, a comma-joined list (`400`), which
    /// `index` carries for a client. Data indices are counted through `_search` with `size` 0.
    pub(crate) fn count_on_data_indices(index_names: &str) -> Self {
        Self::illegal_argument(format!(
            "_count is not served on data indices [{index_names}]; send _search with size 0"
        ))
        .with_metadata("index", index_names)
    }

    /// A `_search` addressed to `index`, whose rows have no document id because its table has no
    /// timestamp column (`400`), which `index` carries for a client.
    pub(crate) fn missing_document_identity(index: &str) -> Self {
        Self::illegal_argument(format!(
            "index [{index}] has no [{COL_TIMESTAMP}] column, so its rows have no document id; \
             query it through PPL instead of _search"
        ))
        .with_metadata("index", index)
    }

    /// The sort field `field` has different sort types in `first_index` and `other_index` (`400`),
    /// which `field` and `indices` carry for a client.
    pub(crate) fn sort_field_conflict(field: &str, first_index: &str, other_index: &str) -> Self {
        Self::illegal_argument(format!(
            "the sort field [{field}] has different types in the indices [{first_index}] and [{other_index}]"
        ))
        .with_metadata("field", field)
        .with_metadata("indices", json!([first_index, other_index]))
    }

    /// The `fields` or `docvalue_fields` name `field` has a different type in the searched indices
    /// (`400`), so no one rendering projects it; `indices_by_type` maps each type name to the
    /// indices that give `field` that type. `field` and `indices` (that map, as an object of index
    /// name arrays) are carried for a client.
    pub(crate) fn field_projection_conflict(field: &str, indices_by_type: &BTreeMap<&'static str, Vec<&str>>) -> Self {
        let listing: Vec<String> = indices_by_type
            .iter()
            .map(|(field_type, indices)| format!("[{field_type}] in [{}]", indices.join(",")))
            .collect();
        let indices: Map<String, Value> = indices_by_type
            .iter()
            .map(|(field_type, indices)| ((*field_type).to_string(), json!(indices)))
            .collect();
        Self::illegal_argument(format!(
            "field [{field}] has conflicting types: {}; it cannot be projected through fields or docvalue_fields",
            listing.join(", ")
        ))
        .with_metadata("field", field)
        .with_metadata("indices", Value::Object(indices))
    }

    /// The request key `key` names a feature the system indices do not serve (`400`), which `key`
    /// carries for a client. It is refused rather than ignored, because ignoring it would answer a
    /// different request.
    pub(crate) fn unsupported_on_system_indices(key: &str) -> Self {
        Self::illegal_argument(format!("[{key}] is not supported on system indices")).with_metadata("key", key)
    }

    /// An `_msearch` header names the key `key`, which no search option answers to (`400`), and
    /// `key` carries it for a client. It is refused rather than ignored, because ignoring it would
    /// run a different search than the one the header asks for.
    pub(crate) fn unsupported_header_key(key: &str) -> Self {
        Self::illegal_argument(format!("key [{key}] is not supported in the metadata section"))
            .with_metadata("key", key)
    }

    /// The document `id` of `index` holds a value of the sort field `field` that does not read as
    /// the field's type (`400`), which `field`, `index` and `id` carry for a client.
    pub(crate) fn unreadable_sort_value(field: &str, index: &str, id: &str) -> Self {
        Self::illegal_argument(format!(
            "the sort field [{field}] of document [{id}] in index [{index}] holds a value that does not read as its type"
        ))
        .with_metadata("field", field)
        .with_metadata("index", index)
        .with_metadata("id", id)
    }

    /// One `sort` reads the source `field` of `index` more than once (`400`), which `field` and
    /// `index` carry for a client.
    pub(crate) fn repeated_sort_field(field: &str, index: &str) -> Self {
        Self::illegal_argument(format!(
            "the sort reads the field [{field}] of index [{index}] more than once"
        ))
        .with_metadata("field", field)
        .with_metadata("index", index)
    }

    /// The scroll `scroll_id` expired, or the snapshot object it reads is gone (`404`).
    pub(crate) fn search_context_missing(scroll_id: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "search_context_missing_exception",
            format!("No search context found for id [{scroll_id}]"),
        )
    }

    /// The system index `index` already exists (`400`).
    pub(crate) fn resource_already_exists(index: &str) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "resource_already_exists_exception",
            format!("index [{index}/_na_] already exists"),
        )
        .with_metadata("index", index)
        .with_metadata("index_uuid", "_na_")
    }

    /// A write conditioned on a document state that does not hold (`409`): a stale
    /// `if_seq_no` / `if_primary_term`, a condition on a missing document, or a create of an
    /// existing id. An update of a missing document without `upsert` is
    /// [`document_missing`](Self::document_missing) instead, with or without a condition.
    pub(crate) fn version_conflict(index: &str, id: &str) -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "version_conflict_engine_exception",
            format!("[{id}]: version conflict"),
        )
        .with_metadata("index", index)
        .with_metadata("shard", "0")
        .with_metadata("index_uuid", "_na_")
    }

    /// An update of a missing document that carries no `upsert` (`404`), before any condition it
    /// carries is checked.
    pub(crate) fn document_missing(index: &str, id: &str) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "document_missing_exception",
            format!("[{id}]: document missing"),
        )
        .with_metadata("index", index)
        .with_metadata("shard", "0")
        .with_metadata("index_uuid", "_na_")
    }

    /// A write addressed to `index`, which accepts none: a data index, or a name that would create
    /// an index outside the system indices (`403`).
    pub(crate) fn cluster_block(index: &str) -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "cluster_block_exception",
            format!("index [{index}] blocked by: [FORBIDDEN/8/index write (api)];"),
        )
    }

    /// A system-index write could not be confirmed: its compare-and-swap kept being refused or
    /// losing its answer, or its snapshot object could not be stored, until the query deadline, or
    /// a lineage it needed was missing (`503`, retryable). Its outcome is unknown: it may have
    /// landed.
    pub(crate) fn write_contention() -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "write_contention_exception",
            "the system-index write could not be confirmed; its outcome is unknown, retry later",
        )
    }

    /// A system-index write would grow the tenant's decoded snapshot past `max_snapshot_bytes`
    /// (`413`); nothing was written.
    pub(crate) fn snapshot_too_large(max_snapshot_bytes: usize) -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "illegal_argument_exception",
            format!(
                "the write would grow the tenant's system indices past \
                 opensearch.system_indices.max_snapshot_bytes ({max_snapshot_bytes} bytes)"
            ),
        )
    }

    /// An answer would pass `max_answer_bytes` (`400`), which a request avoids by asking for less.
    /// An `_mget` or a `_search` is refused whole; on a `_bulk` only the update item fails.
    pub(crate) fn answer_too_large(max_answer_bytes: usize) -> Self {
        Self::illegal_argument(format!(
            "the answer would pass opensearch.system_indices.max_answer_bytes ({max_answer_bytes} bytes)"
        ))
    }

    /// No route serves `method` on `path`: a `400` `illegal_argument_exception` in the envelope,
    /// never a `404` or `405`.
    pub(crate) fn no_handler(method: &Method, path: &str) -> Self {
        Self::illegal_argument(format!("no handler found for uri [{path}] and method [{method}]"))
    }
}

impl IntoResponse for OpenSearchError {
    fn into_response(self) -> Response {
        let is_retryable = self.status == StatusCode::SERVICE_UNAVAILABLE;
        if self.status.is_server_error() && !is_retryable {
            tracing::error!(error_type = self.error_type, reason = %self.reason, "opensearch request failed");
        }
        let cause = self.render_cause();
        // `self` is not read as a whole again, so the override moves out of it; without one the
        // root cause repeats the error's keys: built once, cloned into the cause list.
        let root_cause = self.root_cause.map_or_else(|| cause.clone(), |root_cause| *root_cause);
        let mut error = Map::new();
        error.insert("root_cause".to_string(), Value::Array(vec![Value::Object(root_cause)]));
        error.extend(cause);
        if is_retryable {
            // `reason` is not read again, so it moves into `details`; `status` is `Copy`.
            error.insert("details".to_string(), Value::String(self.reason));
        }
        let body = json!({"error": Value::Object(error), "status": self.status.as_u16()});
        let mut response = (self.status, body.to_string()).into_response();
        let headers = response.headers_mut();
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE_JSON));
        if is_retryable {
            headers.insert(header::RETRY_AFTER, HeaderValue::from(SHED_RETRY_AFTER_SECS));
        }
        response
    }
}

impl From<QueryError> for OpenSearchError {
    /// A mistake in the request (`Parse`, `Validation`, `NotImplemented`, `Plan`) is a `400`;
    /// every other failure is a `500`.
    fn from(error: QueryError) -> Self {
        let reason = error.to_string();
        match error {
            QueryError::Parse(_) | QueryError::Validation(_) | QueryError::NotImplemented(_) | QueryError::Plan(_) => {
                Self::illegal_argument(reason)
            }
            QueryError::DataFusion(_) | QueryError::Config(_) | QueryError::Iceberg(_) | QueryError::Internal(_) => {
                Self::internal(reason)
            }
        }
    }
}

/// Render `error` and read back its status and JSON body, for assertions on the envelope.
#[cfg(test)]
pub(crate) async fn render_envelope(error: OpenSearchError) -> (StatusCode, Value) {
    read_envelope(error.into_response()).await
}

/// Read the status and the JSON body of `response`, for assertions on an answer in the envelope.
#[cfg(test)]
pub(crate) async fn read_envelope(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("envelope body");
    (status, serde_json::from_slice(&body).expect("the envelope is JSON"))
}

#[cfg(test)]
mod tests {
    use datafusion::error::DataFusionError;
    use serde_json::json;

    use super::*;
    use crate::error::parse_error;

    #[tokio::test]
    async fn an_illegal_argument_renders_type_and_reason_in_error_and_root_cause() {
        let response = OpenSearchError::illegal_argument("bad thing").into_response();
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json; charset=UTF-8")
        );

        let (status, body) = render_envelope(OpenSearchError::illegal_argument("bad thing")).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            json!({
                "error": {
                    "root_cause": [{"type": "illegal_argument_exception", "reason": "bad thing"}],
                    "type": "illegal_argument_exception",
                    "reason": "bad thing"
                },
                "status": 400
            })
        );
    }

    #[tokio::test]
    async fn shed_and_deadline_answer_503_with_details_and_retry_after() {
        for error in [OpenSearchError::memory_pressure(), OpenSearchError::deadline_exceeded()] {
            let response = error.clone().into_response();
            let retry_after = response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u32>().ok());
            assert!(retry_after.is_some_and(|seconds| seconds > 0), "{error:?}");

            let (status, body) = render_envelope(error).await;

            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(body["status"], 503);
            assert!(body["error"]["reason"].is_string());
            assert_eq!(body["error"]["details"], body["error"]["reason"]);
            assert_eq!(body["error"]["root_cause"][0]["type"], body["error"]["type"]);
            assert!(body["error"]["root_cause"][0].get("details").is_none());
        }
    }

    #[tokio::test]
    async fn the_two_503_causes_are_told_apart_by_type() {
        let (_, shed) = render_envelope(OpenSearchError::memory_pressure()).await;
        let (_, deadline) = render_envelope(OpenSearchError::deadline_exceeded()).await;

        assert_eq!(shed["error"]["type"], "memory_pressure_exception");
        assert_eq!(deadline["error"]["type"], "query_deadline_exceeded_exception");
    }

    /// Every variant of [`QueryError`] is driven, because the mapping is a `match` over all of
    /// them and a variant moved to the other arm is invisible from the rest.
    #[tokio::test]
    async fn query_errors_map_to_client_and_server_statuses() {
        let cases = [
            (
                parse_error("bad"),
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
            ),
            (
                QueryError::Validation("bad".to_string()),
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
            ),
            (
                QueryError::NotImplemented("x".to_string()),
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
            ),
            (
                QueryError::Plan("x".to_string()),
                StatusCode::BAD_REQUEST,
                "illegal_argument_exception",
            ),
            (
                QueryError::Internal("x".to_string()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "exception",
            ),
            (
                QueryError::Config("x".to_string()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "exception",
            ),
            (
                QueryError::DataFusion(DataFusionError::Internal("x".to_string())),
                StatusCode::INTERNAL_SERVER_ERROR,
                "exception",
            ),
            (
                QueryError::Iceberg(iceberg::Error::new(iceberg::ErrorKind::Unexpected, "x")),
                StatusCode::INTERNAL_SERVER_ERROR,
                "exception",
            ),
        ];
        for (query_error, expected_status, expected_type) in cases {
            let label = format!("{query_error:?}");

            let (status, body) = render_envelope(OpenSearchError::from(query_error)).await;

            assert_eq!(status, expected_status, "{label}");
            assert_eq!(body["error"]["type"], expected_type, "{label}");
        }
    }

    #[tokio::test]
    async fn an_unmatched_route_is_a_well_formed_400_not_a_bare_string() {
        let (status, body) = render_envelope(OpenSearchError::no_handler(&Method::DELETE, "/_nodes")).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], 400);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(body["error"]["root_cause"][0]["type"], "illegal_argument_exception");
        assert!(body["error"]["reason"].is_string());
    }

    #[tokio::test]
    async fn index_not_found_carries_the_index_metadata_in_error_and_root_cause() {
        let (status, body) = render_envelope(OpenSearchError::index_not_found("nope")).await;

        let cause = json!({
            "type": "index_not_found_exception",
            "reason": "no such index [nope]",
            "index": "nope",
            "index_uuid": "_na_",
            "resource.id": "nope",
            "resource.type": "index_or_alias"
        });
        let mut error = cause.clone();
        error["root_cause"] = json!([cause]);
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, json!({ "error": error, "status": 404 }));
    }

    #[tokio::test]
    async fn system_store_errors_render_their_status_type_and_metadata() {
        let (status, body) = render_envelope(OpenSearchError::resource_already_exists(".kibana_1")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "resource_already_exists_exception");
        assert_eq!(
            body["error"]["root_cause"][0]["type"],
            "resource_already_exists_exception"
        );
        assert_eq!(body["error"]["index"], ".kibana_1");
        assert_eq!(body["error"]["root_cause"][0]["index_uuid"], "_na_");

        let (status, body) = render_envelope(OpenSearchError::cluster_block("logs")).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"]["type"], "cluster_block_exception");
        assert_eq!(
            body["error"]["reason"],
            "index [logs] blocked by: [FORBIDDEN/8/index write (api)];"
        );
        assert_eq!(body["status"], 403);

        let (status, body) = render_envelope(OpenSearchError::write_contention()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["type"], "write_contention_exception");
        assert!(body["error"]["details"].is_string());

        let (status, body) = render_envelope(OpenSearchError::snapshot_too_large(1024)).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(body["status"], 413);

        let (status, body) = render_envelope(OpenSearchError::answer_too_large(1024)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], 400);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        let reason = body["error"]["reason"].as_str().expect("the reason is a string");
        assert!(
            reason.contains("opensearch.system_indices.max_answer_bytes"),
            "{reason}"
        );
        assert!(reason.contains("1024"), "{reason}");
    }

    #[tokio::test]
    async fn version_conflict_and_document_missing_carry_their_index_metadata() {
        let (conflict_status, conflict) =
            render_envelope(OpenSearchError::version_conflict(".kibana_1", "config:3.9.0")).await;
        let (missing_status, missing) =
            render_envelope(OpenSearchError::document_missing(".kibana_1", "config:3.9.0")).await;

        assert_eq!(conflict_status, StatusCode::CONFLICT);
        assert_eq!(conflict["status"], 409);
        assert_eq!(conflict["error"]["type"], "version_conflict_engine_exception");
        assert_eq!(conflict["error"]["index"], ".kibana_1");
        assert_eq!(conflict["error"]["shard"], "0");
        assert_eq!(conflict["error"]["index_uuid"], "_na_");
        assert_eq!(
            conflict["error"]["root_cause"][0]["type"],
            "version_conflict_engine_exception"
        );
        assert_eq!(missing_status, StatusCode::NOT_FOUND);
        assert_eq!(missing["status"], 404);
        assert_eq!(missing["error"]["type"], "document_missing_exception");
        assert_eq!(missing["error"]["index"], ".kibana_1");
    }

    #[tokio::test]
    async fn a_result_window_above_the_limit_is_a_search_phase_failure_with_an_illegal_argument_root_cause() {
        let (status, body) = render_envelope(OpenSearchError::result_window_too_large(9_000, 1_001, 10_000)).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], 400);
        assert_eq!(body["error"]["type"], "search_phase_execution_exception");
        assert_eq!(body["error"]["phase"], "query");
        assert_eq!(body["error"]["grouped"], true);
        assert_eq!(body["error"]["root_cause"][0]["type"], "illegal_argument_exception");
        // No index is named: the window is checked before any index is read.
        assert_eq!(
            body["error"]["failed_shards"],
            json!([{"shard": 0, "reason": body["error"]["root_cause"][0].clone()}])
        );
    }

    #[tokio::test]
    async fn a_query_shard_error_wraps_its_cause_as_the_root_cause() {
        let shard_cause = json!({
            "type": "query_shard_exception",
            "reason": "No mapping found for [title] in order to sort on",
            "index": ".kibana_1",
            "index_uuid": "_na_"
        });

        let (status, body) = render_envelope(OpenSearchError::query_shard(
            ".kibana_1",
            "No mapping found for [title] in order to sort on",
        ))
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body,
            json!({
                "error": {
                    "type": "search_phase_execution_exception",
                    "reason": "all shards failed",
                    "phase": "query",
                    "grouped": true,
                    "failed_shards": [{"shard": 0, "index": ".kibana_1", "reason": shard_cause.clone()}],
                    "root_cause": [shard_cause]
                },
                "status": 400
            })
        );
    }

    #[tokio::test]
    async fn too_many_buckets_is_a_search_phase_400_with_the_bucket_cause_in_root_cause() {
        let (status, body) = render_envelope(OpenSearchError::too_many_buckets(3, 4)).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], 400);
        assert_eq!(body["error"]["type"], "search_phase_execution_exception");
        assert_eq!(body["error"]["phase"], "query");
        assert_eq!(body["error"]["grouped"], true);
        let cause = &body["error"]["root_cause"][0];
        assert_eq!(cause["type"], "too_many_buckets_exception");
        assert_eq!(cause["max_buckets"], 3);
        assert_eq!(
            cause["reason"],
            "Trying to create too many buckets. Must be less than or equal to: [3] but this search \
             reached [4]. Narrow the time range or the interval, or raise \
             opensearch.search.max_buckets."
        );
    }

    #[tokio::test]
    async fn aggregation_execution_renders_its_reason_in_error_and_root_cause() {
        let (status, body) = render_envelope(OpenSearchError::aggregation_execution(
            "[severity_text] cannot be aggregated: it is [keyword] in [logs] and [long] in [audit_events]",
        ))
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "aggregation_execution_exception");
        assert_eq!(
            body["error"]["root_cause"][0]["type"],
            "aggregation_execution_exception"
        );
        assert_eq!(
            body["error"]["reason"],
            "[severity_text] cannot be aggregated: it is [keyword] in [logs] and [long] in [audit_events]"
        );
    }

    #[tokio::test]
    async fn aggregation_field_conflict_carries_the_field_and_the_indices_of_each_type_in_error_and_root_cause() {
        let mut indices_by_type = Map::new();
        indices_by_type.insert("keyword".to_string(), json!(["logs"]));
        indices_by_type.insert("long".to_string(), json!(["audit_events", "audit_trail"]));

        let (status, body) = render_envelope(OpenSearchError::aggregation_field_conflict(
            "severity_text",
            "[severity_text] cannot be aggregated",
            indices_by_type,
        ))
        .await;

        let expected_indices = json!({"keyword": ["logs"], "long": ["audit_events", "audit_trail"]});
        assert_eq!(status, StatusCode::BAD_REQUEST);
        for cause in [&body["error"], &body["error"]["root_cause"][0]] {
            assert_eq!(cause["type"], "aggregation_execution_exception");
            assert_eq!(cause["reason"], "[severity_text] cannot be aggregated");
            assert_eq!(cause["field"], "severity_text");
            assert_eq!(cause["indices_by_type"], expected_indices);
        }
    }

    #[tokio::test]
    async fn the_memory_limit_error_is_an_illegal_argument_naming_the_limit() {
        let (status, body) = render_envelope(OpenSearchError::memory_limit_exceeded(1_048_576)).await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], 400);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(body["error"]["root_cause"][0]["type"], "illegal_argument_exception");
        assert_eq!(body["error"]["limit"], 1_048_576);
        assert_eq!(body["error"]["root_cause"][0]["limit"], 1_048_576);
    }
}
