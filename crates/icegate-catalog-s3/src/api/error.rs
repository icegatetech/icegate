//! Iceberg REST error conversion.

use axum::response::{IntoResponse, Response};
use axum::{Json, http::StatusCode};
use iceberg::{Error as IcebergError, ErrorKind};

use super::dto::{IcebergErrorModel, IcebergErrorResponse};

/// Error returned by API handlers and rendered as an Iceberg error envelope.
pub(crate) enum ApiError {
    BadRequest { message: String },
    NoSuchNamespace,
    NoSuchTable,
    NoSuchWarehouse,
    ResourceNotFound,
    MethodNotAllowed,
    Internal,
    CatalogStorageUnavailable,
    RequestTimedOut,
}

impl From<IcebergError> for ApiError {
    fn from(error: IcebergError) -> Self {
        match error.kind() {
            ErrorKind::NamespaceNotFound => {
                tracing::debug!(%error, "Catalog namespace not found");
                Self::NoSuchNamespace
            }
            ErrorKind::TableNotFound => {
                tracing::debug!(%error, "Catalog table not found");
                Self::NoSuchTable
            }
            _ if error.retryable() => {
                tracing::warn!(%error, "Catalog storage is temporarily unavailable");
                Self::CatalogStorageUnavailable
            }
            _ => {
                tracing::error!(%error, "Catalog request failed");
                Self::Internal
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match self {
            Self::BadRequest { message } => (StatusCode::BAD_REQUEST, "BadRequestException", message),
            Self::NoSuchNamespace => (
                StatusCode::NOT_FOUND,
                "NoSuchNamespaceException",
                "Namespace not found".to_string(),
            ),
            Self::NoSuchTable => (
                StatusCode::NOT_FOUND,
                "NoSuchTableException",
                "Table not found".to_string(),
            ),
            Self::NoSuchWarehouse => (
                StatusCode::NOT_FOUND,
                "NoSuchWarehouseException",
                "Warehouse not found".to_string(),
            ),
            Self::ResourceNotFound => (
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "Resource not found".to_string(),
            ),
            Self::MethodNotAllowed => (
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowedException",
                "Method not allowed".to_string(),
            ),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                "Internal server error".to_string(),
            ),
            Self::CatalogStorageUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailableException",
                "Catalog storage is temporarily unavailable".to_string(),
            ),
            Self::RequestTimedOut => (
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailableException",
                "Catalog request timed out".to_string(),
            ),
        };
        (
            status,
            Json(IcebergErrorResponse {
                error: IcebergErrorModel {
                    message,
                    error_type: error_type.to_string(),
                    code: status.as_u16(),
                },
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use iceberg::{Error, ErrorKind};

    use super::ApiError;

    #[tokio::test]
    async fn api_errors_render_the_iceberg_error_envelope() {
        let cases = [
            (
                ApiError::BadRequest {
                    message: "Request field is invalid".to_string(),
                },
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Request field is invalid",
            ),
            (
                ApiError::NoSuchNamespace,
                StatusCode::NOT_FOUND,
                "NoSuchNamespaceException",
                "Namespace not found",
            ),
            (
                ApiError::NoSuchTable,
                StatusCode::NOT_FOUND,
                "NoSuchTableException",
                "Table not found",
            ),
            (
                ApiError::NoSuchWarehouse,
                StatusCode::NOT_FOUND,
                "NoSuchWarehouseException",
                "Warehouse not found",
            ),
            (
                ApiError::ResourceNotFound,
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "Resource not found",
            ),
            (
                ApiError::MethodNotAllowed,
                StatusCode::METHOD_NOT_ALLOWED,
                "MethodNotAllowedException",
                "Method not allowed",
            ),
            (
                ApiError::Internal,
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                "Internal server error",
            ),
            (
                ApiError::CatalogStorageUnavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailableException",
                "Catalog storage is temporarily unavailable",
            ),
            (
                ApiError::RequestTimedOut,
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailableException",
                "Catalog request timed out",
            ),
        ];

        for (error, status, error_type, message) in cases {
            assert_error_response(error.into_response(), status, error_type, message).await;
        }
    }

    #[tokio::test]
    async fn iceberg_errors_are_classified_and_sanitized() {
        let cases = [
            (
                Error::new(ErrorKind::NamespaceNotFound, "namespace detail"),
                ExpectedVariant::NoSuchNamespace,
                StatusCode::NOT_FOUND,
                "NoSuchNamespaceException",
                "Namespace not found",
            ),
            (
                Error::new(ErrorKind::TableNotFound, "table detail"),
                ExpectedVariant::NoSuchTable,
                StatusCode::NOT_FOUND,
                "NoSuchTableException",
                "Table not found",
            ),
            (
                Error::new(ErrorKind::Unexpected, "storage detail").with_retryable(true),
                ExpectedVariant::CatalogStorageUnavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailableException",
                "Catalog storage is temporarily unavailable",
            ),
            (
                Error::new(
                    ErrorKind::DataInvalid,
                    "invalid root at s3://private-bucket/catalog/root.json",
                ),
                ExpectedVariant::Internal,
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                "Internal server error",
            ),
        ];

        for (error, expected_variant, status, error_type, message) in cases {
            let api_error = ApiError::from(error);
            expected_variant.assert_matches(&api_error);
            assert_error_response(api_error.into_response(), status, error_type, message).await;
        }
    }

    enum ExpectedVariant {
        NoSuchNamespace,
        NoSuchTable,
        CatalogStorageUnavailable,
        Internal,
    }

    impl ExpectedVariant {
        fn assert_matches(&self, error: &ApiError) {
            match (self, error) {
                (Self::NoSuchNamespace, ApiError::NoSuchNamespace)
                | (Self::NoSuchTable, ApiError::NoSuchTable)
                | (Self::CatalogStorageUnavailable, ApiError::CatalogStorageUnavailable)
                | (Self::Internal, ApiError::Internal) => {}
                _ => panic!("Iceberg error mapped to an unexpected API error variant"),
            }
        }
    }

    async fn assert_error_response(response: Response, status: StatusCode, error_type: &str, message: &str) {
        assert_eq!(response.status(), status);
        let body = to_bytes(response.into_body(), usize::MAX).await.expect("response body");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).expect("Iceberg error response"),
            serde_json::json!({
                "error": {
                    "message": message,
                    "type": error_type,
                    "code": status.as_u16(),
                }
            })
        );
    }
}
