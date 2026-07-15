//! Iceberg identifiers in their REST wire representation.

use iceberg::{NamespaceIdent, TableIdent};

use super::error::ApiError;

/// Separator joining the levels of a multipart namespace on the wire.
///
/// Advertised to clients as the `namespace-separator` override in url-encoded
/// form; requests carry the decoded byte.
pub(super) const NAMESPACE_SEPARATOR: char = '\u{1f}';

/// Render [`NAMESPACE_SEPARATOR`] in the url-encoded form `GET /v1/config`
/// advertises as its `namespace-separator` override.
///
/// Derived rather than written out a second time: the separator a client is
/// told to send and the byte this module parses cannot drift apart.
pub(super) fn encode_namespace_separator() -> String {
    format!("%{:02X}", u32::from(NAMESPACE_SEPARATOR))
}

/// Parse a percent-decoded namespace identifier.
///
/// # Errors
///
/// Returns a `400` when any level is empty, which is what a leading, trailing,
/// or doubled separator produces.
pub(super) fn parse_namespace(decoded_namespace: &str) -> Result<NamespaceIdent, ApiError> {
    let parts = decoded_namespace
        .split(NAMESPACE_SEPARATOR)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if parts.iter().any(String::is_empty) {
        return Err(ApiError::BadRequest {
            message: "Namespace identifier is invalid".to_string(),
        });
    }
    NamespaceIdent::from_vec(parts).map_err(|_| ApiError::BadRequest {
        message: "Namespace identifier is invalid".to_string(),
    })
}

/// Qualify a percent-decoded table name with its namespace.
///
/// # Errors
///
/// Returns a `400` when the name is empty.
pub(super) fn parse_table(namespace: &NamespaceIdent, name: &str) -> Result<TableIdent, ApiError> {
    if name.is_empty() {
        return Err(ApiError::BadRequest {
            message: "Table name cannot be empty".to_string(),
        });
    }
    Ok(TableIdent::new(namespace.clone(), name.to_string()))
}

/// Render a namespace as the level array every REST response carries.
pub(super) fn to_namespace_parts(namespace: &NamespaceIdent) -> Vec<String> {
    namespace.as_ref().to_owned()
}
