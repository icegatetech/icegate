//! Request extractors that reject with the Iceberg error envelope.

use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{FromRequestParts, Path, Query};
use axum::http::StatusCode;
use axum::http::request::Parts;
use serde::de::DeserializeOwned;

use super::error::ApiError;

/// [`Query`] rejecting with an [`ApiError`].
///
/// Axum renders its own rejections as `text/plain`, and a rejected request never
/// reaches a handler or a fallback — so without this the only responses that
/// escape the envelope the pinned spec revision mandates are exactly the
/// malformed requests a client most needs to read.
pub(super) struct ApiQuery<T>(pub(super) T);

impl<T, S> FromRequestParts<S> for ApiQuery<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(query)| Self(query))
            .map_err(|rejection: QueryRejection| to_api_error(rejection.status(), "Query parameters are invalid"))
    }
}

/// [`Path`] rejecting with an [`ApiError`].
///
/// See [`ApiQuery`] for why the axum rejection is not enough.
pub(super) struct ApiPath<T>(pub(super) T);

impl<T, S> FromRequestParts<S> for ApiPath<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(path)| Self(path))
            .map_err(|rejection: PathRejection| to_api_error(rejection.status(), "Request path is invalid"))
    }
}

/// Restate an extractor rejection as an Iceberg error.
///
/// The rejection is read through its status rather than its variants, which are
/// `#[non_exhaustive]`: a `400` is the client's malformed request, and anything
/// else — a route whose parameters do not match the type it extracts into — is
/// this server's own bug and owes the client no detail.
///
/// The rejection text is replaced rather than forwarded for the reason
/// [`ApiError::from`] sanitizes: an error body names what the client got
/// wrong, not what this server parses with.
fn to_api_error(status: StatusCode, message: &'static str) -> ApiError {
    if status == StatusCode::BAD_REQUEST {
        ApiError::BadRequest {
            message: message.to_string(),
        }
    } else {
        ApiError::Internal
    }
}
