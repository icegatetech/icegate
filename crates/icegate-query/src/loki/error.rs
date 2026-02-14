//! Loki API error handling.
//!
//! Provides error-to-HTTP-response conversion for the Loki API via the
//! [`LokiError`] newtype wrapper implementing axum's `IntoResponse`.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

use super::models::{ErrorType, LokiResponse};
use crate::error::QueryError;

/// Result type for Loki API operations.
pub type LokiResult<T> = Result<T, LokiError>;

/// Newtype wrapper for `QueryError` that implements `IntoResponse`.
///
/// Enables idiomatic error handling in axum handlers via the `?` operator.
#[derive(Debug)]
pub struct LokiError(pub QueryError);

impl From<QueryError> for LokiError {
    fn from(err: QueryError) -> Self {
        Self(err)
    }
}

impl IntoResponse for LokiError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self.0 {
            // 400 Bad Request
            QueryError::Parse(_) | QueryError::Validation(_) => (StatusCode::BAD_REQUEST, ErrorType::BadData),
            QueryError::Plan(_) => (StatusCode::BAD_REQUEST, ErrorType::PlanningError),

            // 501 Not Implemented
            QueryError::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, ErrorType::NotImplemented),

            // 500 Internal Server Error - execution failures
            QueryError::DataFusion(_) => (StatusCode::INTERNAL_SERVER_ERROR, ErrorType::ExecutionError),

            // 500 Internal Server Error - infrastructure failures
            QueryError::Iceberg(_) | QueryError::Config(_) | QueryError::Internal(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, ErrorType::Internal)
            }
        };

        (status, Json(LokiResponse::<()>::error(error_type, self.0.to_string()))).into_response()
    }
}
