//! OTLP HTTP error handling.
//!
//! Provides error-to-HTTP-response conversion for the OTLP HTTP API via the
//! [`OtlpError`] newtype wrapper implementing axum's `IntoResponse`.

use axum::{
    Json,
    http::StatusCode,
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

            // Retryable load-shedding: 429 when the WAL channel is full (request
            // not enqueued), 503 when retries are exhausted or the durability ack
            // exceeds its deadline. Lets a stock OTLP collector retry gracefully
            // instead of tripping its own client timeout (GH-158).
            IngestError::QueueFull => (StatusCode::TOO_MANY_REQUESTS, ErrorType::Internal),
            IngestError::MaxAttemptsReached | IngestError::AckTimeout => {
                (StatusCode::SERVICE_UNAVAILABLE, ErrorType::Internal)
            }
        };

        (status, Json(ErrorResponse::new(error_type, self.0.to_string()))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use super::OtlpError;
    use crate::error::IngestError;

    #[test]
    fn queue_full_maps_to_429() {
        assert_eq!(
            OtlpError(IngestError::QueueFull).into_response().status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn ack_timeout_maps_to_503() {
        assert_eq!(
            OtlpError(IngestError::AckTimeout).into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
