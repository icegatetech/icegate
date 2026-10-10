//! The `OpenSearch` error envelope.
//!
//! Every failure the listener answers is rendered here: handler errors, middleware refusals
//! (shed, deadline, tenant) and unmatched routes, so a client never reads a bare string or an
//! empty body. The shape is
//! `{"error": {"root_cause": [{type, reason, <metadata>}], type, reason, <metadata>}, "status": N}`,
//! where the metadata keys (for example the missing `index`) are present only on errors that carry
//! them; a `503` adds `details` beside `reason` and a `Retry-After` header.
//!
//! `503` is reserved for conditions a client should retry unchanged (memory-pressure shedding,
//! the query deadline); a limit a request can avoid by asking for less is a `4xx`, never a `503`.

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

    /// Attach `key: value` to the error and its root cause.
    fn with_metadata(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.metadata.insert(key.to_string(), value.into());
        self
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
        let mut cause = Map::new();
        cause.insert("type".to_string(), json!(self.error_type));
        cause.insert("reason".to_string(), json!(self.reason));
        cause.extend(self.metadata);
        let mut error = Map::new();
        // The root cause repeats the error's keys: built once, cloned into the cause list.
        error.insert(
            "root_cause".to_string(),
            Value::Array(vec![Value::Object(cause.clone())]),
        );
        error.extend(cause);
        if is_retryable {
            error.insert("details".to_string(), json!(self.reason));
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
}
