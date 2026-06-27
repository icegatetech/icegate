//! Tenant resolution from gRPC request metadata.
//!
//! The tenant identifier is read from the `x-scope-orgid` metadata
//! header — the same convention used by Loki, Tempo, and the
//! Prometheus-compatible endpoints. Any client capable of setting an
//! arbitrary gRPC header (ADBC, JDBC, `grpcurl -H`, the ADBC-backed
//! Apache Arrow Flight SQL ODBC driver) can supply it.
//!
//! Invalid values fall back to the default tenant. There is no password
//! verification — the Flight SQL endpoint trusts the gateway in front of
//! it, exactly like the existing HTTP servers do with the `X-Scope-OrgID`
//! header. The validate-or-default policy is shared with the HTTP paths
//! via [`icegate_common::resolve_tenant_id`].

use icegate_common::TENANT_ID_HEADER;
use tonic::metadata::MetadataMap;

/// Resolve the tenant identifier for this request.
///
/// Reads `x-scope-orgid`; returns the default tenant when the header is
/// absent or malformed. Extracts the raw gRPC metadata value and defers
/// the validate-or-default decision to [`icegate_common::resolve_tenant_id`]
/// so every protocol shares one policy.
pub(super) fn resolve_tenant_id(metadata: &MetadataMap) -> String {
    icegate_common::resolve_tenant_id(metadata.get(TENANT_ID_HEADER).and_then(|v| v.to_str().ok()))
}

#[cfg(test)]
mod tests {
    use icegate_common::DEFAULT_TENANT_ID;
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
