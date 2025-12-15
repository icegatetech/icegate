//! Loki API error handling.
//!
//! Implements `IntoResponse` for `IceGateError` to convert errors into
//! properly formatted Loki-compatible HTTP responses.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use super::models::{ErrorType, LokiResponse};
use crate::common::errors::IceGateError;

/// Result type for Loki API operations.
pub type LokiResult<T> = Result<T, IceGateError>;

impl IntoResponse for IceGateError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self {
            // 400 Bad Request
            Self::Parse(_) | Self::Validation(_) | Self::Schema(_) => (StatusCode::BAD_REQUEST, ErrorType::BadData),
            Self::Plan(_) => (StatusCode::BAD_REQUEST, ErrorType::PlanningError),

            // 501 Not Implemented
            Self::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, ErrorType::NotImplemented),

            // 500 Internal Server Error
            Self::DataFusion(_) | Self::Iceberg(_) | Self::Io(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, ErrorType::ExecutionError)
            },
            Self::Config(_) | Self::Collection(_) => (StatusCode::INTERNAL_SERVER_ERROR, ErrorType::Internal),
        };

        (status, Json(LokiResponse::<()>::error(error_type, self.to_string()))).into_response()
    }
}
