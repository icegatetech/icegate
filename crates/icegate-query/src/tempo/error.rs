//! Tempo API error handling.
//!
//! Provides error-to-HTTP-response conversion for the Tempo API via the
//! [`TempoError`] newtype wrapper implementing axum's `IntoResponse`.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

use crate::error::QueryError;

/// Result alias for Tempo handlers.
pub type TempoResult<T> = Result<T, TempoError>;

/// Newtype wrapper translating [`QueryError`] into HTTP responses.
///
/// Enables idiomatic error handling in axum handlers via the `?` operator.
#[derive(Debug)]
pub struct TempoError(pub QueryError);

impl From<QueryError> for TempoError {
    fn from(e: QueryError) -> Self {
        Self(e)
    }
}

impl IntoResponse for TempoError {
    fn into_response(self) -> Response {
        let (status, kind) = match &self.0 {
            QueryError::Parse(_) | QueryError::Validation(_) | QueryError::Plan(_) => {
                (StatusCode::BAD_REQUEST, "bad_request")
            }
            QueryError::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, "not_implemented"),
            QueryError::DataFusion(_) => (StatusCode::INTERNAL_SERVER_ERROR, "execution_error"),
            QueryError::Iceberg(_) | QueryError::Config(_) | QueryError::Internal(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
            }
        };
        (
            status,
            Json(json!({
                "status": kind,
                "error": self.0.to_string(),
            })),
        )
            .into_response()
    }
}
