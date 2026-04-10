//! OTLP HTTP error handling.
//!
//! Provides error-to-HTTP-response conversion for the OTLP HTTP API via the
//! [`OtlpError`] newtype wrapper implementing axum's `IntoResponse`.

use axum::{
    Json,
    http::{
        StatusCode,
        header::{HeaderValue, RETRY_AFTER},
    },
    response::{IntoResponse, Response},
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

impl From<tokio::task::JoinError> for OtlpError {
    fn from(err: tokio::task::JoinError) -> Self {
        Self(IngestError::from(err))
    }
}

impl IntoResponse for OtlpError {
    fn into_response(self) -> Response {
        // Backpressure gets a 429 with Retry-After header.
        if let IngestError::Backpressure(retry_after_secs) = &self.0 {
            let mut response = (
                StatusCode::TOO_MANY_REQUESTS,
                Json(ErrorResponse::new(ErrorType::Internal, self.0.to_string())),
            )
                .into_response();
            if let Ok(val) = HeaderValue::from_str(&retry_after_secs.to_string()) {
                response.headers_mut().insert(RETRY_AFTER, val);
            }
            return response;
        }

        let (status, error_type) = match &self.0 {
            // 400 Bad Request
            IngestError::Decode(_) | IngestError::Validation(_) => (StatusCode::BAD_REQUEST, ErrorType::BadData),

            // 501 Not Implemented
            IngestError::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, ErrorType::NotImplemented),

            // 500 Internal Server Error
            IngestError::Io(_)
            | IngestError::Queue(_)
            | IngestError::Config(_)
            | IngestError::Iceberg(_)
            | IngestError::Join(_)
            | IngestError::Shift(_)
            | IngestError::ShiftQueueRead(_)
            | IngestError::Arrow(_)
            | IngestError::Other(_)
            | IngestError::Multiple(_) => (StatusCode::INTERNAL_SERVER_ERROR, ErrorType::Internal),

            IngestError::Cancelled => (StatusCode::REQUEST_TIMEOUT, ErrorType::Internal),

            IngestError::MaxAttemptsReached => (StatusCode::SERVICE_UNAVAILABLE, ErrorType::Internal),

            // Handled by early return above; included for exhaustiveness.
            IngestError::Backpressure(_) => unreachable!(),
        };

        (status, Json(ErrorResponse::new(error_type, self.0.to_string()))).into_response()
    }
}
