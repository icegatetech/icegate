//! Tenant identification and the ingest tenant policy.
//!
//! Two independent policies live here, and they are not interchangeable:
//!
//! - [`resolve_tenant_id`] is the read-side fallback shared by the query
//!   protocols (Loki, Tempo, Flight SQL): an absent or malformed header resolves
//!   to [`DEFAULT_TENANT_ID`].
//! - [`TenantPolicy`] is the write-side decision used by ingest. It never falls
//!   back silently: a request whose header names a tenant the deployment does not
//!   serve is rejected, so a sender is never told its data landed in its own
//!   tenant while it was written to another one.

use std::{fmt, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::error::{CommonError, Result};

/// Default tenant ID when not provided in request metadata.
pub const DEFAULT_TENANT_ID: &str = "default";

/// HTTP header / gRPC metadata key for tenant identification (`X-Scope-OrgID`,
/// Grafana/Loki standard). Stored lowercase per HTTP/2 and gRPC conventions;
/// header lookups are case-insensitive.
pub const TENANT_ID_HEADER: &str = "x-scope-orgid";

/// Whether `value` may name a tenant, as [`TenantId::is_valid`] decides it.
///
/// Kept as a free function for `icegate-ee`, which imports it from this crate's
/// root; every caller inside this crate calls the method directly.
// TODO(high): remove once icegate-ee calls TenantId::is_valid — icegate-lookout
// (lake/scope.rs, api/analyses.rs) and icegate-import (cli/run.rs, export/grpc.rs)
// are the only callers left outside this crate.
pub fn is_valid_tenant_id(value: &str) -> bool {
    TenantId::is_valid(value)
}

/// Resolve a tenant identifier from an optional raw header value.
///
/// Returns the value when present and valid per [`TenantId::is_valid`],
/// otherwise [`DEFAULT_TENANT_ID`]. This is the single fallback policy
/// shared by every query protocol (Loki, Tempo, Flight SQL); callers only
/// differ in how they pull the raw string out of their header/metadata
/// map, so centralising the validate-or-default step here keeps tenant
/// resolution defined in exactly one place.
///
/// Ingest does **not** use this function: a write resolves through
/// [`TenantResolver`], which fails closed instead of defaulting.
#[must_use]
pub fn resolve_tenant_id(header_value: Option<&str>) -> String {
    // TODO(high): deprecated, use TenantResolver in query component
    header_value
        .filter(|value| TenantId::is_valid(value))
        .map_or_else(|| DEFAULT_TENANT_ID.to_string(), String::from)
}

/// The configuration error a single-tenant policy naming an unusable identifier
/// is refused with.
///
/// Shared by [`TenantPolicy::validate`] and [`TenantPolicy::into_resolver`]:
/// both refuse exactly the identifiers [`TenantId::is_valid`] refuses, and an
/// operator reading the two messages must not have to decide whether they mean
/// the same thing.
fn invalid_tenant_id_error(id: &str) -> CommonError {
    CommonError::Config(format!(
        "tenant id {id:?} must be non-empty ASCII alphanumeric, hyphens or underscores"
    ))
}

/// Tenant identifier carried through one ingest request.
///
/// `Arc<str>` rather than `String`: in [`TenantResolver::Single`] the configured
/// identifier is cloned once per request and a clone is a refcount bump, so the
/// single-tenant deployment allocates nothing on the hot path. In
/// [`TenantResolver::Multi`] exactly one allocation per request is unavoidable —
/// the value lives in the request headers, while the request extensions the
/// resolved tenant travels in require `'static`.
///
/// A value of this type has passed [`TenantId::is_valid`]; that is the whole
/// point of the newtype, so a holder never re-validates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantId(Arc<str>);

impl TenantId {
    /// Build a tenant identifier, returning `None` when `value` does not pass
    /// [`Self::is_valid`].
    #[must_use]
    pub fn new(value: &str) -> Option<Self> {
        Self::is_valid(value).then(|| Self(Arc::from(value)))
    }

    /// Whether `value` may name a tenant: non-empty, and ASCII alphanumeric,
    /// hyphens or underscores only.
    ///
    /// The rule itself, for a caller that holds a borrowed value and needs no
    /// [`TenantId`]: building one would allocate an `Arc<str>` to answer a
    /// question about the bytes it was given.
    #[must_use]
    pub fn is_valid(value: &str) -> bool {
        !value.is_empty()
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b':')
    }
}

impl AsRef<str> for TenantId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What the [`TENANT_ID_HEADER`] carried on one request, before any policy is
/// applied.
///
/// [`Self::Duplicated`] is a case of its own rather than "take the first value":
/// two values mean the sender and something on the path disagree about the
/// tenant, and picking either one silently resolves that disagreement in favour
/// of whichever the transport happened to order first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantHeader<'a> {
    /// The header is not present.
    Absent,
    /// The header is present exactly once, with this raw value.
    Once(&'a str),
    /// The header is present more than once.
    Duplicated,
}

impl<'a> TenantHeader<'a> {
    /// Read the header from the values one request carried under
    /// [`TENANT_ID_HEADER`], in the order the transport holds them.
    ///
    /// A `None` item is a value that is present but not readable as text — not
    /// valid UTF-8, or not visible ASCII, which both `HeaderValue::to_str` and
    /// `MetadataValue::to_str` refuse. It becomes [`Self::Once`] on the empty
    /// string, which [`TenantId::is_valid`] rejects, rather than
    /// [`Self::Absent`]: the sender did name a tenant, and reporting it as
    /// absent would let a malformed value fall through to the identifier of a
    /// [`TenantResolver::Single`] deployment.
    ///
    /// At most two items are read, so a request carrying many values costs the
    /// same as one carrying two.
    #[must_use]
    pub fn from_values<I>(values: I) -> Self
    where
        I: IntoIterator<Item = Option<&'a str>>,
    {
        let mut values = values.into_iter();
        let Some(first) = values.next() else {
            return Self::Absent;
        };
        if values.next().is_some() {
            return Self::Duplicated;
        }
        Self::Once(first.unwrap_or(""))
    }
}

/// Why a request carries no usable tenant.
///
/// Values map one-to-one onto the `reason` label of the tenant-rejection
/// counter through [`Self::reason`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantRejection {
    /// No header, and the policy has no tenant of its own to use.
    Missing,
    /// The header value is malformed, or names a tenant this deployment does
    /// not serve.
    Invalid,
    /// The header appears more than once.
    DuplicateHeader,
}

impl TenantRejection {
    /// The metric label for this rejection.
    ///
    /// The single source of the `reason` label values: a recorder that builds
    /// the string itself would drift from the variants.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::DuplicateHeader => "duplicate_header",
        }
    }

    /// What the sender is told about this rejection.
    ///
    /// One text per variant for every protocol: OTLP/HTTP renders it into the
    /// JSON error body and OTLP/gRPC into the `Status` message, so a sender that
    /// switches transport is not told two different things about one refusal.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::Missing => "missing tenant: this deployment takes the tenant from the x-scope-orgid header",
            Self::Invalid => "invalid tenant: x-scope-orgid names a tenant this deployment does not serve",
            Self::DuplicateHeader => "invalid tenant: x-scope-orgid is present more than once",
        }
    }
}

/// How ingest decides which tenant a request writes to.
///
/// Serialised as a YAML tagged union (`tenant: !single` with a nested `id`, or
/// `tenant: !multi`), the same shape as `CatalogBackend` and `StorageBackend`.
/// This is the configuration form; the request path uses [`TenantResolver`],
/// built once at startup by [`Self::into_resolver`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TenantPolicy {
    /// One tenant for the whole deployment. A request may omit the header, but
    /// a header naming any other tenant is rejected.
    Single {
        /// The identifier every request of this deployment writes to.
        id: String,
    },
    /// The tenant comes from the request and only from the request. A request
    /// without a valid header is rejected.
    Multi,
}

impl Default for TenantPolicy {
    /// [`Self::Single`] on [`DEFAULT_TENANT_ID`] — what a configuration with no
    /// `tenant` section means, and what ingest did before the policy existed.
    fn default() -> Self {
        Self::Single {
            id: DEFAULT_TENANT_ID.to_string(),
        }
    }
}

impl TenantPolicy {
    /// Check that the policy can be served.
    ///
    /// # Errors
    ///
    /// Returns [`CommonError::Config`] when [`Self::Single`] names an identifier
    /// that does not pass [`TenantId::is_valid`]. [`Self::Multi`] constrains
    /// nothing and always succeeds.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Single { id } if !TenantId::is_valid(id) => Err(invalid_tenant_id_error(id)),
            Self::Single { .. } | Self::Multi => Ok(()),
        }
    }

    /// Build the request-path resolver.
    ///
    /// The single-tenant identifier is validated and turned into a [`TenantId`]
    /// here, once, so no request re-parses the configured string. The
    /// construction is the validation — [`Self::validate`] refuses exactly the
    /// identifiers [`TenantId::new`] refuses — so a policy accepted here needs no
    /// separate check, and there is no state in which one succeeds and the other
    /// does not.
    ///
    /// # Errors
    ///
    /// Returns [`CommonError::Config`] on the identifiers [`Self::validate`]
    /// rejects.
    pub fn into_resolver(self) -> Result<TenantResolver> {
        match self {
            Self::Single { id } => TenantId::new(&id)
                .map(TenantResolver::Single)
                .ok_or_else(|| invalid_tenant_id_error(&id)),
            Self::Multi => Ok(TenantResolver::Multi),
        }
    }
}

/// Runtime form of [`TenantPolicy`]: the single-tenant identifier is built once
/// at startup, not per request.
#[derive(Debug, Clone)]
pub enum TenantResolver {
    /// Serve exactly this tenant.
    Single(TenantId),
    /// Take the tenant from the request.
    Multi,
}

impl TenantResolver {
    /// Decide the tenant of one request.
    ///
    /// | mode | `Absent` | `Once(value)` | `Duplicated` |
    /// |---|---|---|---|
    /// | `Single(id)` | `id` | `id` when `value == id`, else `Invalid` | `DuplicateHeader` |
    /// | `Multi` | `Missing` | `value` when valid, else `Invalid` | `DuplicateHeader` |
    ///
    /// `Single` consults the header rather than ignoring it so that a sender
    /// addressing another tenant is told so, instead of having its data written
    /// to this deployment's tenant under a name it never asked for.
    ///
    /// # Errors
    ///
    /// Returns the [`TenantRejection`] the request is refused with; the caller
    /// turns it into the protocol's own status and into the `reason` label.
    pub fn resolve_tenant(&self, header: TenantHeader<'_>) -> std::result::Result<TenantId, TenantRejection> {
        match (self, header) {
            (_, TenantHeader::Duplicated) => Err(TenantRejection::DuplicateHeader),
            (Self::Single(id), TenantHeader::Absent) => Ok(id.clone()),
            // A value equal to a validated identifier is itself valid, so the
            // equality check subsumes `TenantId::is_valid` here.
            (Self::Single(id), TenantHeader::Once(value)) if value == id.as_ref() => Ok(id.clone()),
            (Self::Single(_), TenantHeader::Once(_)) => Err(TenantRejection::Invalid),
            (Self::Multi, TenantHeader::Absent) => Err(TenantRejection::Missing),
            (Self::Multi, TenantHeader::Once(value)) => TenantId::new(value).ok_or(TenantRejection::Invalid),
        }
    }

    /// The mode name for the startup log line.
    #[must_use]
    pub const fn mode(&self) -> &'static str {
        match self {
            Self::Single(_) => "single",
            Self::Multi => "multi",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build the resolver of a `single` policy, asserting the policy is sound.
    fn single_resolver(id: &str) -> TenantResolver {
        TenantPolicy::Single { id: id.to_string() }
            .into_resolver()
            .expect("a valid single-tenant policy resolves")
    }

    #[test]
    fn a_tenant_id_may_carry_the_colon_that_separates_org_from_workspace() {
        assert!(is_valid_tenant_id("2q4mHrPd9kL:7xZa1vB3nQe"));
        assert_eq!(
            resolve_tenant_id(Some("2q4mHrPd9kL:7xZa1vB3nQe")),
            "2q4mHrPd9kL:7xZa1vB3nQe"
        );
    }

    /// Widening to `:` must not widen to anything else: path traversal, quotes and
    /// whitespace stay refused, and an empty id is still not an id.
    #[test]
    fn widening_to_the_colon_admits_nothing_else() {
        for refused in ["", "../etc", "a/b", "a'b", "a b", "a.b", "a\\b", "a\"b"] {
            assert!(!is_valid_tenant_id(refused), "must refuse {refused:?}");
        }
    }

    #[test]
    fn test_valid_tenant_ids() {
        assert!(TenantId::is_valid("default"));
        assert!(TenantId::is_valid("my-tenant"));
        assert!(TenantId::is_valid("tenant_123"));
        assert!(TenantId::is_valid("Org-42_prod"));
        assert!(TenantId::is_valid("a"));
    }

    #[test]
    fn test_invalid_tenant_ids() {
        assert!(!TenantId::is_valid(""));
        assert!(!TenantId::is_valid("has space"));
        assert!(!TenantId::is_valid("has/slash"));
        assert!(!TenantId::is_valid("has.dot"));
        assert!(!TenantId::is_valid("emoji\u{1F600}"));
        assert!(!TenantId::is_valid("tab\there"));
    }

    #[test]
    fn resolve_tenant_id_honours_valid_value() {
        assert_eq!(resolve_tenant_id(Some("tenant-a")), "tenant-a");
    }

    #[test]
    fn resolve_tenant_id_falls_back_on_absent_or_invalid() {
        assert_eq!(resolve_tenant_id(None), DEFAULT_TENANT_ID);
        assert_eq!(resolve_tenant_id(Some("has space")), DEFAULT_TENANT_ID);
        assert_eq!(resolve_tenant_id(Some("")), DEFAULT_TENANT_ID);
    }

    #[test]
    fn single_returns_the_configured_tenant_when_the_header_is_absent() {
        let resolved = single_resolver("acme")
            .resolve_tenant(TenantHeader::Absent)
            .expect("single serves its own tenant without a header");
        assert_eq!(resolved.as_ref(), "acme");
    }

    #[test]
    fn single_returns_the_configured_tenant_when_the_header_names_it() {
        let resolved = single_resolver("acme")
            .resolve_tenant(TenantHeader::Once("acme"))
            .expect("a header naming the served tenant is accepted");
        assert_eq!(resolved.as_ref(), "acme");
    }

    #[test]
    fn single_rejects_a_header_naming_another_tenant() {
        assert_eq!(
            single_resolver("acme").resolve_tenant(TenantHeader::Once("victim")),
            Err(TenantRejection::Invalid)
        );
    }

    #[test]
    fn multi_rejects_an_absent_header() {
        assert_eq!(
            TenantResolver::Multi.resolve_tenant(TenantHeader::Absent),
            Err(TenantRejection::Missing)
        );
    }

    #[test]
    fn multi_returns_the_header_value() {
        let resolved = TenantResolver::Multi
            .resolve_tenant(TenantHeader::Once("tenant-b"))
            .expect("a valid header carries the tenant in multi");
        assert_eq!(resolved.as_ref(), "tenant-b");
    }

    #[test]
    fn both_modes_reject_a_duplicated_header() {
        for resolver in [single_resolver("acme"), TenantResolver::Multi] {
            assert_eq!(
                resolver.resolve_tenant(TenantHeader::Duplicated),
                Err(TenantRejection::DuplicateHeader),
                "{} must reject a duplicated header",
                resolver.mode()
            );
        }
    }

    #[test]
    fn both_modes_reject_an_invalid_value() {
        // `org:ws` is the colon-separated form named by the parent task; the
        // rest are the classes `TenantId::is_valid` excludes.
        for value in ["", "has space", "has/slash"] {
            for resolver in [single_resolver("acme"), TenantResolver::Multi] {
                assert_eq!(
                    resolver.resolve_tenant(TenantHeader::Once(value)),
                    Err(TenantRejection::Invalid),
                    "{} must reject {value:?}",
                    resolver.mode()
                );
            }
        }
    }

    #[test]
    fn validate_rejects_a_single_policy_with_an_invalid_id() {
        let policy = TenantPolicy::Single {
            id: "bad/tenant".to_string(),
        };
        assert!(matches!(policy.validate(), Err(CommonError::Config(_))));
        assert!(matches!(policy.into_resolver(), Err(CommonError::Config(_))));
    }

    /// The identifier is spelled out rather than read from [`DEFAULT_TENANT_ID`]:
    /// it is the value already written into the deployed tables, so changing the
    /// constant must fail here rather than silently re-point every configuration
    /// that carries no `tenant` section.
    #[test]
    fn default_policy_is_single_default_tenant() {
        assert_eq!(
            TenantPolicy::default(),
            TenantPolicy::Single {
                id: "default".to_string()
            }
        );
    }

    #[test]
    fn yaml_round_trip_covers_both_tag_forms() {
        // The unit variant is the risky half: `!multi` carries no value, and a
        // tagged null is where an externally tagged enum and serde_yaml are most
        // likely to disagree. The chart renders exactly these two forms.
        let single: TenantPolicy = serde_yaml::from_str("!single\nid: acme\n").expect("single deserializes");
        assert_eq!(single, TenantPolicy::Single { id: "acme".to_string() });

        let multi: TenantPolicy = serde_yaml::from_str("!multi\n").expect("multi deserializes");
        assert_eq!(multi, TenantPolicy::Multi);

        for policy in [single, multi] {
            let encoded = serde_yaml::to_string(&policy).expect("policy serializes");
            let decoded: TenantPolicy = serde_yaml::from_str(&encoded).expect("policy round-trips");
            assert_eq!(decoded, policy);
        }
    }

    #[test]
    fn rejection_reasons_are_distinct_labels() {
        assert_eq!(TenantRejection::Missing.reason(), "missing");
        assert_eq!(TenantRejection::Invalid.reason(), "invalid");
        assert_eq!(TenantRejection::DuplicateHeader.reason(), "duplicate_header");
    }

    #[test]
    fn no_values_read_as_an_absent_header() {
        assert_eq!(TenantHeader::from_values([]), TenantHeader::Absent);
    }

    #[test]
    fn one_value_reads_as_that_value() {
        assert_eq!(TenantHeader::from_values([Some("acme")]), TenantHeader::Once("acme"));
    }

    #[test]
    fn two_values_read_as_duplicated_even_when_they_agree() {
        assert_eq!(
            TenantHeader::from_values([Some("acme"), Some("acme")]),
            TenantHeader::Duplicated
        );
    }

    #[test]
    fn an_unreadable_value_reads_as_a_named_but_invalid_tenant() {
        // The empty string is refused by `TenantId::is_valid`, so the policy
        // sees a named tenant it does not serve rather than an absent header —
        // which in `Single` would have resolved to the deployment's own id.
        let header = TenantHeader::from_values([None]);
        assert_eq!(header, TenantHeader::Once(""));
        assert_eq!(
            single_resolver("acme").resolve_tenant(header),
            Err(TenantRejection::Invalid)
        );
    }

    /// The startup log line is the only place an operator learns which mode the
    /// deployment runs in, and `mode()` is the whole of its observable content.
    /// The expected strings are spelled out, so a swapped arm fails here.
    #[test]
    fn mode_names_the_policy_the_resolver_serves() {
        assert_eq!(single_resolver("acme").mode(), "single");
        assert_eq!(TenantResolver::Multi.mode(), "multi");
    }

    #[test]
    fn tenant_id_rejects_an_invalid_value() {
        assert!(TenantId::new("bad/tenant").is_none());
        assert_eq!(TenantId::new("acme").expect("valid").to_string(), "acme");
    }
}
