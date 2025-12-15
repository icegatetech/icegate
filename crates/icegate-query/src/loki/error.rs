//! Loki API error handling.
//!
//! Provides error-to-HTTP-response conversion for the Loki API via the
//! [`LokiError`] newtype wrapper implementing axum's `IntoResponse`.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use icegate_common::errors::IceGateError;

use super::models::{ErrorType, LokiResponse};

/// Result type for Loki API operations.
pub type LokiResult<T> = Result<T, LokiError>;

/// Newtype wrapper for `IceGateError` that implements `IntoResponse`.
///
/// Enables idiomatic error handling in axum handlers via the `?` operator.
#[derive(Debug)]
pub struct LokiError(pub IceGateError);

impl From<IceGateError> for LokiError {
    fn from(err: IceGateError) -> Self {
        Self(err)
    }
}

impl IntoResponse for LokiError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self.0 {
            // 400 Bad Request
            IceGateError::Parse(_) | IceGateError::Validation(_) | IceGateError::Schema(_) => {
                (StatusCode::BAD_REQUEST, ErrorType::BadData)
            },
            IceGateError::Plan(_) => (StatusCode::BAD_REQUEST, ErrorType::PlanningError),

            // 501 Not Implemented
            IceGateError::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, ErrorType::NotImplemented),

            // 500 Internal Server Error
            IceGateError::DataFusion(_) | IceGateError::Iceberg(_) | IceGateError::Io(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, ErrorType::ExecutionError)
            },
            IceGateError::Config(_) | IceGateError::Collection(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, ErrorType::Internal)
            },
        };

        (status, Json(LokiResponse::<()>::error(error_type, self.0.to_string()))).into_response()
    }
}
