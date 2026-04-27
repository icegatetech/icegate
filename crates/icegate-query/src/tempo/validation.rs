//! Tempo HTTP-input validation: server-side caps and allow-lists.
//!
//! Centralises every input-bound check the Tempo handlers perform so the
//! limits are one-stop documentation and so unit tests can assert on the
//! constants directly. The handlers convert these checks into HTTP 400
//! responses via [`crate::error::QueryError::Validation`].
//!
//! Caps are deliberately conservative; they protect the query engine
//! from cheap `DoS` vectors (unbounded `limit`, infinite time windows,
//! megabyte-sized `q`) without blocking realistic Grafana traffic. They
//! are tuned for a typical IceGate deployment and intended to be
//! tightened (never loosened) over time.
//!
//! Each constant documents its rationale and the upstream-Tempo /
//! Grafana behaviour it shadows.

use chrono::{DateTime, Duration, Utc};

use crate::error::{QueryError, Result};

// ============================================================================
// Caps (HTTP-input)
// ============================================================================

/// Hard upper bound on `?limit=` in the search API.
///
/// Tempo's default is 20; the highest value Grafana sends today is 200.
/// 1000 leaves headroom for batch tools while making it impossible for a
/// caller to ask for ten million traces.
pub const MAX_SEARCH_LIMIT: usize = 1000;

/// Hard upper bound on the `start..end` window for any query endpoint.
///
/// 30 days matches the longest "explore" window Grafana defaults to. A
/// 30-day window across all tenants is already several seconds of
/// metadata-scan work; anything larger would have to pre-aggregate
/// upstream.
pub const MAX_QUERY_WINDOW: Duration = Duration::days(30);

/// Hard upper bound on the `?q=` `TraceQL` query string in bytes.
///
/// Grafana's query builder produces queries well under 1 `KiB`. 16 `KiB`
/// leaves three orders of magnitude of headroom while preventing a
/// pathological caller from feeding the lexer / parser a multi-megabyte
/// payload (which would block the thread for seconds even with the
/// existing depth-100 visitor guard, since lexing happens first).
pub const MAX_QUERY_LENGTH_BYTES: usize = 16 * 1024;

/// Hard upper bound on a tag-name path parameter
/// (`/api/v2/search/tag/{name}/values`).
pub const MAX_TAG_NAME_LEN: usize = 256;

/// Maximum HTTP request body size accepted by the Tempo router.
///
/// Tempo's `POST /api/search` carries the same parameters as the GET
/// variant; nothing legitimate exceeds 1 `KiB`. Leaving axum's default
/// 2 `MiB` body limit in place means the lexer would still process up to
/// 2 `MiB` of attacker-supplied form data before any validator fires.
pub const MAX_BODY_BYTES: usize = 64 * 1024;

/// Per-request wall-clock timeout.
///
/// Search queries occasionally take seconds when a wide window forces a
/// large metadata scan; 30 s is a comfortable ceiling that still
/// guarantees the tokio reactor never holds a tempo-side request open
/// indefinitely (e.g., on a downstream catalog hang).
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Hard ceiling on total spans returned from a single search request.
///
/// Acts as a safety net for `spss=0` (no per-trace cap) so a single
/// 50K-span trace cannot OOM the formatter. Independent of, and
/// applied after, [`MAX_SEARCH_LIMIT`] and the per-trace `spss` cap.
pub const MAX_TOTAL_SPANS: usize = 100_000;

/// Hard ceiling on spans returned from a single `/api/traces/{id}` call.
///
/// The trace-by-id endpoint contractually returns the full trace, but a
/// pathological 500K-span trace would OOM the OTLP formatter. We cap
/// and signal truncation via the `x-icegate-truncated` response header
/// rather than failing the request.
pub const MAX_TRACE_SPANS: usize = 50_000;

// ============================================================================
// Validators
// ============================================================================

/// Clamp the caller-supplied `limit` to the server-side ceiling.
///
/// `None` falls through to the caller's default; anything above
/// [`MAX_SEARCH_LIMIT`] is silently clamped (matches upstream Tempo's
/// behaviour, which clamps rather than rejects).
//
// `single_option_map` would have us push the `.map` to the caller, but
// the caller is the bottleneck where the cap belongs — keeping the
// clamp logic centralized in this module is the whole point.
#[allow(clippy::single_option_map)]
#[must_use]
pub fn clamp_search_limit(limit: Option<usize>) -> Option<usize> {
    limit.map(|n| n.min(MAX_SEARCH_LIMIT))
}

/// Validate the resolved time window.
///
/// Rejects queries where `start` is after `end` and queries whose
/// window exceeds [`MAX_QUERY_WINDOW`].
///
/// # Errors
///
/// Returns [`QueryError::Validation`] with a human-readable message.
pub fn validate_query_window(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<()> {
    if start > end {
        return Err(QueryError::Validation(format!(
            "start ({start}) must be <= end ({end})"
        )));
    }
    let window = end - start;
    if window > MAX_QUERY_WINDOW {
        return Err(QueryError::Validation(format!(
            "query window {window} exceeds maximum {MAX_QUERY_WINDOW}",
        )));
    }
    Ok(())
}

/// Validate a `TraceQL` query string before parsing it.
///
/// # Errors
///
/// Returns [`QueryError::Validation`] when the string is longer than
/// [`MAX_QUERY_LENGTH_BYTES`].
pub fn validate_query_length(q: &str) -> Result<()> {
    if q.len() > MAX_QUERY_LENGTH_BYTES {
        return Err(QueryError::Validation(format!(
            "query length {} exceeds maximum {MAX_QUERY_LENGTH_BYTES} bytes",
            q.len(),
        )));
    }
    Ok(())
}

/// Validate a tag-name path parameter.
///
/// Tag names appear in URL paths (`/api/v2/search/tag/{name}/values`)
/// so the value reaches the handler URL-decoded but otherwise
/// unconstrained. We allow only ASCII alphanumeric, `_`, `.`, `-`,
/// `:` (used by `event:name` / `link:traceID` intrinsics), plus a
/// leading dot for unscoped attribute references.
///
/// `tenant_id` is explicitly blocked — even if it were ever added to
/// the indexed-columns list a caller must never be able to enumerate
/// tenant identifiers.
///
/// # Errors
///
/// Returns [`QueryError::Validation`] for empty input, too-long input,
/// disallowed characters, and the reserved name `tenant_id`.
pub fn validate_tag_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(QueryError::Validation("tag name must not be empty".into()));
    }
    if name.len() > MAX_TAG_NAME_LEN {
        return Err(QueryError::Validation(format!(
            "tag name length {} exceeds maximum {MAX_TAG_NAME_LEN} bytes",
            name.len(),
        )));
    }
    // Block direct enumeration of the tenant column under any spelling.
    // `last_segment` already covers both the unscoped (`tenant_id`) and
    // scoped (`resource.tenant_id`, `.tenant_id`) cases — `rsplit('.').next()`
    // on a string with no dot returns the whole string.
    let trimmed = name.trim_start_matches('.');
    let last_segment = trimmed.rsplit('.').next().unwrap_or(trimmed);
    if last_segment.eq_ignore_ascii_case("tenant_id") {
        return Err(QueryError::Validation("tag name 'tenant_id' is reserved".into()));
    }
    for c in name.chars() {
        if !is_allowed_tag_char(c) {
            return Err(QueryError::Validation(format!(
                "tag name contains disallowed character '{c}'",
            )));
        }
    }
    Ok(())
}

/// Allow-list character class for tag-name path parameters. Mirrors
/// `is_valid_tenant_id` but additionally permits `.` (dotted scope
/// notation), `:` (used by the `event:` / `link:` intrinsic prefixes),
/// and the `-` / `_` separators that appear in `OTel` semantic-
/// convention keys.
const fn is_allowed_tag_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-' | ':')
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn clamp_passes_small_values() {
        assert_eq!(clamp_search_limit(Some(50)), Some(50));
    }

    #[test]
    fn clamp_caps_oversized_values() {
        assert_eq!(clamp_search_limit(Some(MAX_SEARCH_LIMIT + 1)), Some(MAX_SEARCH_LIMIT));
        assert_eq!(clamp_search_limit(Some(usize::MAX)), Some(MAX_SEARCH_LIMIT));
    }

    #[test]
    fn clamp_preserves_none() {
        assert_eq!(clamp_search_limit(None), None);
    }

    #[test]
    fn window_validator_accepts_short_window() {
        let s = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let e = s + Duration::hours(1);
        assert!(validate_query_window(s, e).is_ok());
    }

    #[test]
    fn window_validator_rejects_inverted_window() {
        let s = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let e = s - Duration::seconds(1);
        let err = validate_query_window(s, e).expect_err("inverted window must reject");
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn window_validator_rejects_oversized_window() {
        let s = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let e = s + MAX_QUERY_WINDOW + Duration::seconds(1);
        let err = validate_query_window(s, e).expect_err("oversized window must reject");
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn query_length_validator_accepts_short_query() {
        assert!(validate_query_length("{}").is_ok());
    }

    #[test]
    fn query_length_validator_rejects_oversize_query() {
        let big = "x".repeat(MAX_QUERY_LENGTH_BYTES + 1);
        let err = validate_query_length(&big).expect_err("oversize query must reject");
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn tag_name_validator_accepts_typical_inputs() {
        for ok in [
            "name",
            "duration",
            "service.name",
            "resource.service.name",
            ".pod",
            "span.http.method",
            "event:name",
            "link:traceID",
            "k8s_pod_name",
            "host-name",
        ] {
            assert!(validate_tag_name(ok).is_ok(), "{ok} should validate");
        }
    }

    #[test]
    fn tag_name_validator_rejects_empty_or_oversize() {
        assert!(matches!(validate_tag_name(""), Err(QueryError::Validation(_))));
        let big = "a".repeat(MAX_TAG_NAME_LEN + 1);
        assert!(matches!(validate_tag_name(&big), Err(QueryError::Validation(_))));
    }

    #[test]
    fn tag_name_validator_rejects_disallowed_chars() {
        for bad in [
            "name with space",
            "back\\slash",
            "control\u{0}null",
            "newline\n",
            "tab\there",
        ] {
            assert!(
                matches!(validate_tag_name(bad), Err(QueryError::Validation(_))),
                "{bad:?} should reject",
            );
        }
    }

    #[test]
    fn tag_name_validator_blocks_tenant_id_column() {
        for tenant in ["tenant_id", "TENANT_ID", "resource.tenant_id", ".tenant_id"] {
            assert!(
                matches!(validate_tag_name(tenant), Err(QueryError::Validation(_))),
                "{tenant} must be reserved",
            );
        }
    }
}
