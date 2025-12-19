//! OTLP HTTP error handling.
//!
//! Provides error-to-HTTP-response conversion for the OTLP HTTP API via the
//! [`OtlpError`] newtype wrapper implementing axum's `IntoResponse`.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use super::models::{ErrorResponse, ErrorType};
use crate::error::IngestError;

/// Result type for OTLP HTTP operations.
pub type OtlpResult<T> = Result<T, OtlpError>;

/// Newtype wrapper for `IngestError` that implements `IntoResponse`.
///
/// Enables idiomatic error handling in axum handlers via the `?` operator.
#[derive(Debug)]
pub struct OtlpError(pub IngestError);

impl From<IngestError> for OtlpError {
    fn from(err: IngestError) -> Self {
        Self(err)
    }
}

impl IntoResponse for OtlpError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self.0 {
            // 400 Bad Request
            IngestError::Decode(_) | IngestError::Validation(_) => (StatusCode::BAD_REQUEST, ErrorType::BadData),

            // 501 Not Implemented
            IngestError::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, ErrorType::NotImplemented),

            // 500 Internal Server Error
            IngestError::Io(_) | IngestError::Queue(_) | IngestError::Config(_) | IngestError::Iceberg(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, ErrorType::Internal)
            },
        };

        (status, Json(ErrorResponse::new(error_type, self.0.to_string()))).into_response()
    }
}
