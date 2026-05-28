//! Tenant resolution from gRPC request metadata.
//!
//! The tenant identifier is read from the `x-scope-orgid` metadata
//! header — the same convention used by Loki, Tempo, and the
//! Prometheus-compatible endpoints. Any client capable of setting an
//! arbitrary gRPC header (ADBC, JDBC, `grpcurl -H`, the ADBC-backed
//! Apache Arrow Flight SQL ODBC driver) can supply it.
//!
//! Invalid values fall back to [`DEFAULT_TENANT_ID`]. There is no
//! password verification — the Flight SQL endpoint trusts the gateway in
//! front of it, exactly like the existing HTTP servers do with the
//! `X-Scope-OrgID` header.

use icegate_common::{DEFAULT_TENANT_ID, TENANT_ID_HEADER, is_valid_tenant_id};
use tonic::metadata::MetadataMap;

/// Resolve the tenant identifier for this request.
///
/// Reads `x-scope-orgid`; returns [`DEFAULT_TENANT_ID`] when the header
/// is absent or malformed.
pub(super) fn resolve_tenant_id(metadata: &MetadataMap) -> String {
    tenant_from_scope_header(metadata).unwrap_or_else(|| DEFAULT_TENANT_ID.to_owned())
}

fn tenant_from_scope_header(metadata: &MetadataMap) -> Option<String> {
    metadata
        .get(TENANT_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| is_valid_tenant_id(s))
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use tonic::metadata::AsciiMetadataKey;

    use super::*;

    /// Insert a key/value pair on a `MetadataMap`. Wraps the verbose
    /// tonic API so test bodies stay focused on the assertion.
    fn insert(md: &mut MetadataMap, key: &str, value: &str) {
        let key = AsciiMetadataKey::from_bytes(key.as_bytes()).expect("test header key");
        md.insert(key, value.parse().expect("test header value"));
    }

    #[test]
    fn empty_metadata_returns_default() {
        assert_eq!(resolve_tenant_id(&MetadataMap::new()), DEFAULT_TENANT_ID);
    }

    #[test]
    fn scope_header_is_honoured() {
        let mut md = MetadataMap::new();
        insert(&mut md, "x-scope-orgid", "demo");
        assert_eq!(resolve_tenant_id(&md), "demo");
    }

    #[test]
    fn invalid_scope_header_falls_back_to_default() {
        // A space makes the value an invalid tenant id.
        let mut md = MetadataMap::new();
        insert(&mut md, "x-scope-orgid", "has space");
        assert_eq!(resolve_tenant_id(&md), DEFAULT_TENANT_ID);
    }
}
