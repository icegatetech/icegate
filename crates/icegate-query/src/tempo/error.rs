//! Tempo API error handling.
//!
//! Provides error-to-HTTP-response conversion for the Tempo API via the
//! [`TempoError`] newtype wrapper implementing axum's `IntoResponse`.
//!
//! # Information-disclosure policy
//!
//! Client-bound responses split [`QueryError`] into two tiers:
//!
//! * **User-input errors** ([`QueryError::Parse`], [`QueryError::Validation`],
//!   [`QueryError::Plan`], [`QueryError::NotImplemented`]) — the message
//!   already comes from caller-supplied data, so it round-trips to the
//!   client verbatim.
//! * **Server / engine errors** ([`QueryError::DataFusion`],
//!   [`QueryError::Iceberg`], [`QueryError::Config`],
//!   [`QueryError::Internal`]) — the underlying message commonly contains
//!   column names, logical-plan fragments, manifest paths, and bucket
//!   names. We emit a generic `"internal error"` message to the client
//!   and log the full detail server-side via [`tracing::error!`] so
//!   operators can correlate by request id.

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
pub struct TempoError(pub(crate) QueryError);

impl TempoError {
    /// Construct a [`TempoError`] from any value convertible into [`QueryError`].
    ///
    /// Prefer this over the implicit tuple constructor so the inner field
    /// can stay crate-private without forcing every call site to import
    /// [`QueryError`] just to wrap it.
    pub fn new(err: impl Into<QueryError>) -> Self {
        Self(err.into())
    }
}

impl From<QueryError> for TempoError {
    fn from(e: QueryError) -> Self {
        Self(e)
    }
}

impl IntoResponse for TempoError {
    fn into_response(self) -> Response {
        // Classify the error, emit telemetry, and pick the message to send back.
        // Internal errors get a redacted message; user-input errors round-trip.
        let (status, kind, message) = match &self.0 {
            QueryError::Parse(_) | QueryError::Validation(_) | QueryError::Plan(_) => {
                (StatusCode::BAD_REQUEST, "bad_request", self.0.to_string())
            }
            QueryError::NotImplemented(_) => (StatusCode::NOT_IMPLEMENTED, "not_implemented", self.0.to_string()),
            QueryError::DataFusion(_) => {
                tracing::error!(error = %self.0, "tempo execution_error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "execution_error",
                    "internal error during query execution".to_string(),
                )
            }
            QueryError::Iceberg(_) | QueryError::Config(_) | QueryError::Internal(_) => {
                tracing::error!(error = %self.0, "tempo internal_error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "internal error".to_string(),
                )
            }
        };
        (
            status,
            Json(json!({
                "status": kind,
                "error": message,
            })),
        )
            .into_response()
    }
}
