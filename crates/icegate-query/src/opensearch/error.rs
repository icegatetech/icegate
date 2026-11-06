//! The `OpenSearch` error envelope.
//!
//! Every failure the listener answers is rendered here: handler errors, middleware refusals
//! (shed, deadline, tenant) and unmatched routes, so a client never reads a bare string or an
//! empty body. The shape is
//! `{"error": {"root_cause": [{type, reason, <metadata>}], type, reason, <metadata>}, "status": N}`,
//! where the metadata keys (for example the missing `index`) are present only on errors that carry
//! them; a `503` adds `details` beside `reason` and a `Retry-After` header.
//!
//! `503` is reserved for the conditions a client should retry: memory-pressure shedding, the
//! query deadline and system-index write contention. A limit a request can avoid by asking for
//! less is a `400`, or a `413` for a body or a snapshot that is too large.

use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use icegate_common::SHED_RETRY_AFTER_SECS;
use serde_json::{Map, Value, json};

use crate::error::QueryError;

/// Content type of every envelope.
const CONTENT_TYPE_JSON: &str = "application/json; charset=UTF-8";

/// An error answered in the `OpenSearch` envelope.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OpenSearchError {
    /// HTTP status of the answer, repeated as the envelope's `status`.
    status: StatusCode,
    /// The envelope's `type`, on the error and on its root cause.
    error_type: &'static str,
    /// The envelope's `reason`, on the error and on its root cause.
    reason: String,
    /// Extra keys rendered after `type` and `reason`, on the error and on its root cause.
    metadata: Map<String, Value>,
}

impl OpenSearchError {
    /// An error of `error_type` answered with `status`.
    fn new(status: StatusCode, error_type: &'static str, reason: impl Into<String>) -> Self {
        Self {
            status,
            error_type,
            reason: reason.into(),
            metadata: Map::new(),
        }
    }

    /// A well-formed request that asks for something invalid, names no usable tenant or carries
    /// an unknown parameter: `400`, `illegal_argument_exception`.
    pub(crate) fn illegal_argument(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "illegal_argument_exception", reason)
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

    /// The request body exceeds `opensearch.max_request_body_bytes` (`413`).
    pub(crate) fn payload_too_large() -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "illegal_argument_exception",
            "request body exceeds opensearch.max_request_body_bytes",
        )
    }

    /// Attach `key: value` to the error and its root cause.
    fn with_metadata(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.metadata.insert(key.to_string(), value.into());
        self
    }

    /// The error object without `root_cause`: `type`, `reason` and every metadata entry, as a
    /// `_bulk` or `_mget` item carries its failure and as the envelope's error and root cause
    /// render it.
    pub(crate) fn render_cause(&self) -> Map<String, Value> {
        let mut cause = Map::new();
        cause.insert("type".to_string(), Value::String(self.error_type.to_string()));
        cause.insert("reason".to_string(), Value::String(self.reason.clone()));
        for (key, value) in &self.metadata {
            cause.insert(key.clone(), value.clone());
        }
        cause
    }

    /// The HTTP status of this error, which a `_bulk` item answers as its `status`.
    pub(crate) const fn status(&self) -> StatusCode {
        self.status
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

    /// An answer would pass `max_answer_bytes` (`400`), which a request avoids by naming fewer
    /// documents. An `_mget` is refused whole; on a `_bulk` only the update item fails.
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
        let mut error = Map::new();
        // The root cause repeats the error's keys: built once, cloned into the cause list.
        error.insert(
            "root_cause".to_string(),
            Value::Array(vec![Value::Object(cause.clone())]),
        );
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
}
