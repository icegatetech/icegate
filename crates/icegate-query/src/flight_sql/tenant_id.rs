//! Tenant resolution from gRPC request metadata.
//!
//! Two ingress paths are supported, tried in order:
//!
//! 1. **`x-scope-orgid` metadata header** — the explicit tenant
//!    identifier used by Loki/Tempo/Prom and any client that can set
//!    arbitrary gRPC headers (ADBC, JDBC, `grpcurl -H`).
//! 2. **Basic Auth username** — extracted from the `authorization`
//!    metadata header (`Basic <base64(user:pass)>`). Some Flight SQL
//!    clients — `DuckDB`'s `duckhog` is the motivating one — only know
//!    how to forward credentials and drop unknown URL parameters; for
//!    those, the connection string's `user=<tenant>` becomes the
//!    tenancy identifier.
//!
//! Both paths are subject to [`is_valid_tenant_id`]: invalid values
//! fall back to [`DEFAULT_TENANT_ID`]. There is no password
//! verification — the Flight SQL endpoint trusts the gateway in front
//! of it, exactly like the existing HTTP servers do with the
//! `X-Scope-OrgID` header.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use icegate_common::{DEFAULT_TENANT_ID, TENANT_ID_HEADER, is_valid_tenant_id};
use tonic::metadata::MetadataMap;

/// gRPC metadata header name carrying HTTP Basic credentials.
const AUTHORIZATION_HEADER: &str = "authorization";

/// Prefix on a Basic-auth `authorization` value before the base64
/// payload. Tonic preserves case but the spec calls for `"Basic "`.
const BASIC_AUTH_PREFIX: &str = "Basic ";

/// Resolve the tenant identifier for this request.
///
/// Tries `x-scope-orgid` first; if that's missing or malformed, falls
/// back to the Basic-auth username. Returns [`DEFAULT_TENANT_ID`] if
/// neither path yields a valid id.
pub(super) fn resolve_tenant_id(metadata: &MetadataMap) -> String {
    if let Some(tenant) = tenant_from_scope_header(metadata) {
        return tenant;
    }
    if let Some(user) = basic_auth_username(metadata) {
        if is_valid_tenant_id(&user) {
            tracing::debug!(
                tenant_id = %user,
                "no `{TENANT_ID_HEADER}` header; tenant derived from Basic Auth username"
            );
            return user;
        }
        tracing::debug!(
            user = %user,
            "Basic Auth username is not a valid tenant id; falling back to `{DEFAULT_TENANT_ID}`"
        );
    }
    DEFAULT_TENANT_ID.to_owned()
}

fn tenant_from_scope_header(metadata: &MetadataMap) -> Option<String> {
    metadata
        .get(TENANT_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| is_valid_tenant_id(s))
        .map(str::to_owned)
}

fn basic_auth_username(metadata: &MetadataMap) -> Option<String> {
    let value = metadata.get(AUTHORIZATION_HEADER)?.to_str().ok()?;
    let encoded = value.strip_prefix(BASIC_AUTH_PREFIX)?;
    let decoded = BASE64_STANDARD.decode(encoded).ok()?;
    let s = String::from_utf8(decoded).ok()?;
    let colon = s.find(':')?;
    Some(s[..colon].to_owned())
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

    fn encode_basic(user: &str, pass: &str) -> String {
        format!("Basic {}", BASE64_STANDARD.encode(format!("{user}:{pass}")))
    }

    #[test]
    fn empty_metadata_returns_default() {
        assert_eq!(resolve_tenant_id(&MetadataMap::new()), DEFAULT_TENANT_ID);
    }

    #[test]
    fn scope_header_wins() {
        let mut md = MetadataMap::new();
        insert(&mut md, "x-scope-orgid", "demo");
        insert(&mut md, "authorization", &encode_basic("alice", "anything"));
        assert_eq!(resolve_tenant_id(&md), "demo");
    }

    #[test]
    fn falls_back_to_basic_auth_username() {
        let mut md = MetadataMap::new();
        insert(&mut md, "authorization", &encode_basic("demo", "any-password"));
        assert_eq!(resolve_tenant_id(&md), "demo");
    }

    #[test]
    fn invalid_basic_auth_username_falls_back_to_default() {
        // `user@host.example` is not a valid tenant id (contains `@` and `.`).
        let mut md = MetadataMap::new();
        insert(&mut md, "authorization", &encode_basic("user@host.example", "anything"));
        assert_eq!(resolve_tenant_id(&md), DEFAULT_TENANT_ID);
    }

    #[test]
    fn invalid_scope_header_falls_back_to_basic_auth() {
        // Space in `x-scope-orgid` makes it invalid; Basic Auth wins.
        let mut md = MetadataMap::new();
        insert(&mut md, "x-scope-orgid", "has space");
        insert(&mut md, "authorization", &encode_basic("demo", "pw"));
        assert_eq!(resolve_tenant_id(&md), "demo");
    }

    #[test]
    fn malformed_basic_auth_is_ignored() {
        let mut md = MetadataMap::new();
        insert(&mut md, "authorization", "Basic not-base64!");
        assert_eq!(resolve_tenant_id(&md), DEFAULT_TENANT_ID);
    }

    #[test]
    fn bearer_auth_does_not_count_as_basic() {
        let mut md = MetadataMap::new();
        insert(&mut md, "authorization", "Bearer some.jwt.token");
        assert_eq!(resolve_tenant_id(&md), DEFAULT_TENANT_ID);
    }
}
