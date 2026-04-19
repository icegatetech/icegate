//! Tempo HTTP request and response models.
//!
//! Typed models for Tempo-compatible API responses, providing compile-time
//! safety and clear API contracts instead of dynamic JSON construction.

use serde::{Deserialize, Serialize};

// ============================================================================
// Request Models — Trace lookup & TraceQL search
// ============================================================================

/// Query parameters for `GET /api/traces/{traceID}` and v2 variant.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TraceLookupParams {
    /// Optional Unix epoch seconds — narrows the time range scanned.
    pub start: Option<i64>,
    /// Optional Unix epoch seconds.
    pub end: Option<i64>,
}

/// Query parameters for `GET /api/search`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SearchParams {
    /// `TraceQL` query string. Optional — when absent, returns recent traces.
    pub q: Option<String>,
    /// Start time (Unix epoch nanoseconds OR seconds — accept both).
    pub start: Option<String>,
    /// End time (Unix epoch nanoseconds OR seconds).
    pub end: Option<String>,
    /// Maximum traces to return.
    pub limit: Option<usize>,
    /// Minimum trace duration filter (e.g., "100ms").
    #[serde(rename = "minDuration")]
    pub min_duration: Option<String>,
    /// Maximum trace duration filter.
    #[serde(rename = "maxDuration")]
    pub max_duration: Option<String>,
}

// ============================================================================
// Request Models — Tag discovery
// ============================================================================

/// Query parameters for `/api/search/tags` (v1) and `/api/v2/search/tags`.
#[derive(Debug, Deserialize)]
pub struct TagsQueryParams {
    /// Unix epoch seconds — start of time window (optional).
    pub start: Option<i64>,
    /// Unix epoch seconds — end of time window (optional).
    pub end: Option<i64>,
    /// Optional `TraceQL` filter restricting the row set before tag
    /// discovery. Currently accepted-and-ignored — tag enumeration does
    /// not yet push the filter through. Kept in the struct so Grafana-
    /// supplied values don't trigger deserialisation errors.
    #[serde(default)]
    #[allow(dead_code)]
    pub q: Option<String>,
    /// Scope filter for v2: `resource | span | intrinsic | event | link`.
    /// Ignored by the v1 endpoint.
    pub scope: Option<String>,
}

/// Query parameters for `/api/search/tag/{name}/values`.
#[derive(Debug, Deserialize)]
pub struct TagValuesQueryParams {
    /// Unix epoch seconds — start of time window (optional).
    pub start: Option<i64>,
    /// Unix epoch seconds — end of time window (optional).
    pub end: Option<i64>,
    /// Maximum number of distinct values to return. Defaults to
    /// [`TagValuesQueryParams::DEFAULT_LIMIT`] when unspecified.
    pub limit: Option<usize>,
    /// Accepted for Grafana compatibility but currently ignored.
    #[serde(rename = "maxStaleValues", default)]
    #[allow(dead_code)]
    pub max_stale_values: Option<u64>,
}

impl TagValuesQueryParams {
    /// Default cap on returned distinct values when `limit` is absent.
    pub const DEFAULT_LIMIT: usize = 1000;
}

// ============================================================================
// Response Models — Trace search
// ============================================================================

/// Search response (`{ traces: [...], metrics: { totalBlocks: N } }`).
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    /// Trace summaries.
    pub traces: Vec<TraceSummary>,
    /// Search-time metrics surfaced to clients.
    pub metrics: SearchMetrics,
}

/// One trace summary in a search response.
#[derive(Debug, Serialize)]
pub struct TraceSummary {
    /// Trace identifier (hex-encoded).
    #[serde(rename = "traceID")]
    pub trace_id: String,
    /// Service of the root span, if known.
    #[serde(rename = "rootServiceName", skip_serializing_if = "Option::is_none")]
    pub root_service_name: Option<String>,
    /// Name of the root span, if known.
    #[serde(rename = "rootTraceName", skip_serializing_if = "Option::is_none")]
    pub root_trace_name: Option<String>,
    /// Trace start time, Unix epoch nanoseconds, as a string.
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    /// Total trace duration in milliseconds.
    #[serde(rename = "durationMs")]
    pub duration_ms: u64,
}

/// Metrics summary returned with search results.
#[derive(Debug, Serialize, Default)]
pub struct SearchMetrics {
    /// Total Iceberg row groups (approximation of "blocks" in Tempo terminology).
    #[serde(rename = "totalBlocks")]
    pub total_blocks: u64,
}

// ============================================================================
// Response Models — Tag discovery
// ============================================================================

/// Response body for `GET /api/search/tags` (flat tag-list variant).
#[derive(Debug, Serialize)]
pub struct TagsV1Response {
    /// Distinct tag names discovered in the row set.
    #[serde(rename = "tagNames")]
    pub tag_names: Vec<String>,
}

/// Response body for `GET /api/v2/search/tags` (scoped variant used by
/// Grafana's query builder).
#[derive(Debug, Serialize)]
pub struct TagsV2Response {
    /// One group per `TraceQL` scope: `resource`, `span`, `intrinsic`, …
    pub scopes: Vec<ScopeGroup>,
}

/// A single scope within a [`TagsV2Response`].
#[derive(Debug, Serialize)]
pub struct ScopeGroup {
    /// Scope identifier (`resource`, `span`, `intrinsic`, `event`, `link`).
    pub name: String,
    /// Tag names within this scope.
    pub tags: Vec<String>,
}

/// Response body for `GET /api/search/tag/{name}/values`.
#[derive(Debug, Serialize)]
pub struct TagValuesResponse {
    /// Distinct values for the requested tag.
    #[serde(rename = "tagValues")]
    pub tag_values: Vec<String>,
}

// ============================================================================
// TraceQL Scope Enum
// ============================================================================

/// Parsed `TraceQL` scope. `None` means "all scopes" (v1 or v2 without
/// `scope=` query param).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Resource-level attributes (`OTel` `Resource`).
    Resource,
    /// Span-level attributes.
    Span,
    /// `TraceQL` intrinsic attributes (name, kind, duration, …).
    Intrinsic,
    /// Span event attributes.
    Event,
    /// Span link attributes.
    Link,
}

impl Scope {
    /// Parse an on-the-wire scope string. Returns `None` for unrecognised
    /// values so callers can treat them as "return all scopes".
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "resource" => Some(Self::Resource),
            "span" => Some(Self::Span),
            "intrinsic" => Some(Self::Intrinsic),
            "event" => Some(Self::Event),
            "link" => Some(Self::Link),
            _ => None,
        }
    }

    /// The canonical on-the-wire identifier (lowercase, matching Tempo).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resource => "resource",
            Self::Span => "span",
            Self::Intrinsic => "intrinsic",
            Self::Event => "event",
            Self::Link => "link",
        }
    }
}

// ============================================================================
// Dotted tag-name parsing for /api/search/tag/{name}/values
// ============================================================================

/// A parsed tag reference as used in Tempo's `{name}` path parameter.
///
/// Grafana sends tag names in `.`-prefixed dotted form:
/// - `resource.service.name` — resource-scope key `service.name`
/// - `span.http.method`      — span-scope key `http.method`
/// - `.service.name`         — span- or resource-scope key `service.name`
///   (underspecified scope)
/// - `name`, `duration`, …   — bare `TraceQL` intrinsic
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagRef<'a> {
    /// Bare `TraceQL` intrinsic (no leading dot, no `resource.`/`span.`
    /// prefix).
    Intrinsic(&'a str),
    /// Scoped attribute reference: `resource.foo` / `span.foo` / `event.foo`
    /// / `link.foo`.
    Scoped {
        /// Scope prefix preceding the dot.
        scope: Scope,
        /// Tag key after the scope prefix.
        key: &'a str,
    },
    /// `.foo` form — attribute lookup without an explicit scope. Callers
    /// usually enumerate both `resource` and `span` maps.
    UnscopedAttribute(&'a str),
}

impl<'a> TagRef<'a> {
    /// Parse a tag name as written by Grafana / `TraceQL`.
    #[must_use]
    pub fn parse(name: &'a str) -> Self {
        if let Some(rest) = name.strip_prefix("resource.") {
            return Self::Scoped {
                scope: Scope::Resource,
                key: rest,
            };
        }
        if let Some(rest) = name.strip_prefix("span.") {
            return Self::Scoped {
                scope: Scope::Span,
                key: rest,
            };
        }
        if let Some(rest) = name.strip_prefix("event.") {
            return Self::Scoped {
                scope: Scope::Event,
                key: rest,
            };
        }
        if let Some(rest) = name.strip_prefix("link.") {
            return Self::Scoped {
                scope: Scope::Link,
                key: rest,
            };
        }
        if let Some(rest) = name.strip_prefix('.') {
            return Self::UnscopedAttribute(rest);
        }
        Self::Intrinsic(name)
    }
}

#[cfg(test)]
mod tests {
    use super::{Scope, TagRef};

    #[test]
    fn scope_parse_known_values() {
        assert_eq!(Scope::parse("resource"), Some(Scope::Resource));
        assert_eq!(Scope::parse("span"), Some(Scope::Span));
        assert_eq!(Scope::parse("intrinsic"), Some(Scope::Intrinsic));
        assert_eq!(Scope::parse("event"), Some(Scope::Event));
        assert_eq!(Scope::parse("link"), Some(Scope::Link));
    }

    #[test]
    fn scope_parse_unknown_is_none() {
        assert_eq!(Scope::parse(""), None);
        assert_eq!(Scope::parse("other"), None);
    }

    #[test]
    fn tagref_parse_resource_scope() {
        assert_eq!(
            TagRef::parse("resource.service.name"),
            TagRef::Scoped {
                scope: Scope::Resource,
                key: "service.name",
            }
        );
    }

    #[test]
    fn tagref_parse_span_scope() {
        assert_eq!(
            TagRef::parse("span.http.method"),
            TagRef::Scoped {
                scope: Scope::Span,
                key: "http.method",
            }
        );
    }

    #[test]
    fn tagref_parse_unscoped_attribute() {
        assert_eq!(TagRef::parse(".pod"), TagRef::UnscopedAttribute("pod"));
    }

    #[test]
    fn tagref_parse_intrinsic() {
        assert_eq!(TagRef::parse("name"), TagRef::Intrinsic("name"));
        assert_eq!(TagRef::parse("duration"), TagRef::Intrinsic("duration"));
    }
}
