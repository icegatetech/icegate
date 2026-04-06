//! Prometheus API error handling.
//!
//! Provides error-to-HTTP-response conversion for the Prometheus API via the
//! [`PrometheusError`] newtype wrapper implementing axum's `IntoResponse`.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::error::QueryError;

/// Result type for Prometheus API operations.
pub type PrometheusResult<T> = Result<T, PrometheusError>;

/// Newtype wrapper for `QueryError` that implements `IntoResponse`.
///
/// Enables idiomatic error handling in axum handlers via the `?` operator.
/// Maps query errors to Prometheus-compatible JSON error responses with
/// appropriate HTTP status codes and `errorType` fields.
#[derive(Debug)]
pub struct PrometheusError(pub QueryError);

impl From<QueryError> for PrometheusError {
    fn from(err: QueryError) -> Self {
        Self(err)
    }
}

impl IntoResponse for PrometheusError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self.0 {
            // 400 Bad Request
            QueryError::Parse(_) | QueryError::Validation(_) | QueryError::Plan(_) => {
                (StatusCode::BAD_REQUEST, "bad_data")
            }

            // 501 Not Implemented
            QueryError::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, "not_implemented"),

            // 500 Internal Server Error - execution failures
            QueryError::DataFusion(_) => (StatusCode::INTERNAL_SERVER_ERROR, "execution"),

            // 500 Internal Server Error - infrastructure failures
            QueryError::Iceberg(_) | QueryError::Config(_) | QueryError::Internal(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal")
            }
        };

        (
            status,
            Json(serde_json::json!({
                "status": "error",
                "errorType": error_type,
                "error": self.0.to_string()
            })),
        )
            .into_response()
    }
}
