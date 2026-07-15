//! Table metadata projection and HTTP validators for `loadTable`.

use std::collections::HashSet;

use axum::http::{HeaderMap, HeaderValue, header};
use percent_encoding::percent_decode_str;
use sha2::{Digest, Sha256};

use super::dto::SnapshotSelection;
use super::error::ApiError;
use super::identifier::{NAMESPACE_SEPARATOR, parse_namespace};

/// This intentionally receives `RawQuery`: split the encoded list on literal
/// commas before percent-decoding each identifier, preserving `%2C` in a view
/// name.
pub(super) fn validate_referenced_by(raw_query: Option<&str>) -> Result<(), ApiError> {
    let Some(raw_query) = raw_query else {
        return Ok(());
    };
    for raw_value in raw_query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .filter_map(|(key, value)| (key == "referenced-by").then_some(value))
    {
        for raw_identifier in raw_value.split(',') {
            let identifier = percent_decode_str(raw_identifier)
                .decode_utf8()
                .map_err(|_| ApiError::BadRequest {
                    message: "referenced-by must be valid UTF-8".to_string(),
                })?;
            let Some((namespace, view_name)) = identifier.rsplit_once(NAMESPACE_SEPARATOR) else {
                return Err(ApiError::BadRequest {
                    message: "referenced-by contains an invalid view identifier".to_string(),
                });
            };
            if view_name.is_empty() || parse_namespace(namespace).is_err() {
                return Err(ApiError::BadRequest {
                    message: "referenced-by contains an invalid view identifier".to_string(),
                });
            }
        }
    }
    Ok(())
}

pub(super) fn validate_access_delegation(headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(value) = headers.get("X-Iceberg-Access-Delegation") else {
        return Ok(());
    };
    let value = value.to_str().map_err(|_| ApiError::BadRequest {
        message: "X-Iceberg-Access-Delegation must be valid ASCII".to_string(),
    })?;
    if value
        .split(',')
        .map(str::trim)
        .any(|value| !matches!(value, "vended-credentials" | "remote-signing"))
    {
        return Err(ApiError::BadRequest {
            message: "X-Iceberg-Access-Delegation is invalid".to_string(),
        });
    }
    Ok(())
}

pub(super) fn serialize_metadata(
    metadata: &iceberg::spec::TableMetadata,
    selection: SnapshotSelection,
) -> Result<serde_json::Value, ApiError> {
    let mut metadata = serde_json::to_value(metadata).map_err(|_| ApiError::Internal)?;
    if selection == SnapshotSelection::All {
        return Ok(metadata);
    }
    let referenced_snapshot_ids = referenced_snapshot_ids(&metadata)?;
    // A table without snapshots serializes without the key at all, which is
    // legal metadata and leaves nothing to filter.
    let Some(snapshots) = metadata.get_mut("snapshots") else {
        return Ok(metadata);
    };
    let snapshots = snapshots.as_array_mut().ok_or(ApiError::Internal)?;
    snapshots.retain(|snapshot| {
        snapshot
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|snapshot_id| referenced_snapshot_ids.contains(&snapshot_id))
    });
    Ok(metadata)
}

/// Snapshot ids reachable from the table's refs in serialized `metadata`.
///
/// The `refs` key is absent only for format v1, whose schema has no such field
/// at all; v2/v3 always write it, as `{}` when the table has no ref. Do not
/// delete the v1 branch as unreachable: v1's sole reference is the main branch
/// implied by `current-snapshot-id`, so without resolving it here every snapshot
/// would be filtered out and the response would carry a `current-snapshot-id`
/// pointing at a snapshot no longer in the payload — a silently corrupt table
/// rather than an error. Deriving main from `current-snapshot-id` is exactly how
/// iceberg-rust itself normalises v1 into `TableMetadata::refs` on read.
///
/// Only a present-but-malformed value is an invariant breach.
fn referenced_snapshot_ids(metadata: &serde_json::Value) -> Result<HashSet<i64>, ApiError> {
    let Some(refs) = metadata.get("refs") else {
        return Ok(metadata
            .get("current-snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .into_iter()
            .collect());
    };
    refs.as_object()
        .ok_or(ApiError::Internal)?
        .values()
        .map(|reference| {
            reference
                .get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or(ApiError::Internal)
        })
        .collect()
}

/// Tag one metadata representation.
///
/// The selection is part of the tag because `refs` and `all` project the same
/// metadata version into different payloads, which the spec requires to carry
/// distinct `ETag`s.
pub(super) fn metadata_etag(metadata_location: &str, selection: SnapshotSelection) -> String {
    let mut hasher = Sha256::new();
    hasher.update(metadata_location.as_bytes());
    hasher.update([0]);
    hasher.update(selection.as_str().as_bytes());
    format!("\"{:x}\"", hasher.finalize())
}

/// Decide whether the request's `If-None-Match` precondition already holds for
/// the representation tagged `etag`.
///
/// # Errors
///
/// Returns a `400` when the field value does not parse as the RFC 9110 §13.1.2
/// grammar.
pub(super) fn request_matches_etag(headers: &HeaderMap, etag: &str) -> Result<bool, ApiError> {
    let mut field = IfNoneMatch::default();
    for header_value in headers.get_all(header::IF_NONE_MATCH) {
        // Bytes rather than `to_str`: an opaque-tag admits `obs-text`
        // (`%x80-FF`), which `HeaderValue::to_str` rejects. A tag this server
        // never issued must simply fail to match, not fail the request.
        field.read_field_line(header_value.as_bytes(), etag.as_bytes())?;
    }
    field.evaluate_precondition()
}

/// The logical `If-None-Match` field value, which RFC 9110 §13.1.2 defines as
/// `"*" / #entity-tag` and its collected grammar expands to
/// `"*" / [ entity-tag *( OWS "," OWS entity-tag ) ]`.
///
/// One value, however many header lines carry it: §5.3 joins repeated lines of a
/// list header with a comma without changing the message's meaning, so the lines
/// are folded here rather than answered one by one.
///
/// Separators are tracked, not just elements, because `"*"` is an alternative to
/// the list rather than a member of it: it is the entire field value, so `*` and
/// `*,` are different fields and only the first is one.
#[derive(Default)]
struct IfNoneMatch {
    field_lines: usize,
    has_separator: bool,
    has_wildcard: bool,
    has_match: bool,
}

impl IfNoneMatch {
    /// Fold one header line into the field value.
    ///
    /// # Errors
    ///
    /// Returns a `400` when the line does not parse as list elements.
    fn read_field_line(&mut self, field_line: &[u8], etag: &[u8]) -> Result<(), ApiError> {
        // Every line past the first joins the previous one across a comma, so it
        // contributes a separator even when it carries no element itself.
        self.has_separator |= self.field_lines > 0;
        self.field_lines += 1;
        let mut rest = trim_ows(field_line);
        loop {
            // RFC 9110 §5.6.1.2 requires a recipient to parse and ignore a
            // reasonable number of empty list elements, so a leading, trailing,
            // or doubled comma is a merge artefact rather than an error. The
            // header-size limit already bounds how many can arrive.
            while let Some(tail) = rest.strip_prefix(b",") {
                self.has_separator = true;
                rest = trim_ows(tail);
            }
            if rest.is_empty() {
                return Ok(());
            }
            if let Some(tail) = rest.strip_prefix(b"*") {
                self.has_wildcard = true;
                rest = trim_ows(tail);
            } else {
                // §13.1.2 mandates weak comparison, so the weakness flag never
                // decides a match: reducing to the opaque-tag is the comparison.
                let (opaque_tag, tail) = split_opaque_tag(rest.strip_prefix(b"W/").unwrap_or(rest))?;
                self.has_match |= opaque_tag == etag;
                rest = trim_ows(tail);
            }
            if !rest.is_empty() && !rest.starts_with(b",") {
                return Err(ApiError::BadRequest {
                    message: "If-None-Match is malformed".to_string(),
                });
            }
        }
    }

    /// Answer the precondition once every header line has been folded in.
    ///
    /// # Errors
    ///
    /// Returns a `400` when a wildcard shares the field with a separator, since
    /// that field value is neither alternative of the grammar.
    fn evaluate_precondition(&self) -> Result<bool, ApiError> {
        if self.has_wildcard {
            if self.has_separator {
                return Err(ApiError::BadRequest {
                    message: "If-None-Match is malformed".to_string(),
                });
            }
            // `*` matches whenever a current representation exists, which the
            // caller has already loaded by the time it asks.
            return Ok(true);
        }
        // The list alternative is optional as a whole, so an absent, empty, or
        // comma-only field lists no entity-tag at all: nothing to match, which
        // is an answer rather than a malformed request.
        Ok(self.has_match)
    }
}

/// Split the leading `opaque-tag` off `value`, returning it with its quotes and
/// the unparsed remainder of the list.
///
/// The list is walked by the §8.8.3 grammar rather than split on commas, because
/// `etagc` admits a comma inside the quotes: only a comma outside them separates
/// two tags.
fn split_opaque_tag(value: &[u8]) -> Result<(&[u8], &[u8]), ApiError> {
    let malformed = || ApiError::BadRequest {
        message: "If-None-Match is malformed".to_string(),
    };
    let body = value.strip_prefix(b"\"").ok_or_else(malformed)?;
    let end = body.iter().position(|byte| *byte == b'"').ok_or_else(malformed)?;
    if !body[..end].iter().copied().all(is_etagc) {
        return Err(malformed());
    }
    // `end` indexes into `body`, one byte past the opening quote of `value`.
    Ok(value.split_at(end + 2))
}

/// `etagc` per RFC 9110 §8.8.3: visible ASCII except the `"` the caller has
/// already used as the tag boundary, plus `obs-text`.
const fn is_etagc(byte: u8) -> bool {
    matches!(byte, 0x21 | 0x23..=0x7e | 0x80..=0xff)
}

/// Strip the optional whitespace RFC 9110 §5.6.3 allows around a list element.
fn trim_ows(value: &[u8]) -> &[u8] {
    let is_ows = |byte: &u8| matches!(*byte, b' ' | b'\t');
    let start = value.iter().position(|byte| !is_ows(byte)).unwrap_or(value.len());
    let end = value.iter().rposition(|byte| !is_ows(byte)).map_or(start, |index| index + 1);
    &value[start..end]
}

pub(super) fn insert_etag(headers: &mut HeaderMap, etag: &str) -> Result<(), ApiError> {
    let header_value = HeaderValue::from_str(etag).map_err(|_| ApiError::Internal)?;
    headers.insert(header::ETAG, header_value);
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use super::*;

    const ETAG: &str = "\"cafe\"";

    fn if_none_match(lines: &[&[u8]]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for line in lines {
            headers.append(
                header::IF_NONE_MATCH,
                HeaderValue::from_bytes(line).expect("If-None-Match value"),
            );
        }
        headers
    }

    /// `request_matches_etag` on `headers`, with its error reduced to the status
    /// it renders: `ApiError` is a response, not a comparable value.
    fn match_etag(headers: &HeaderMap) -> Result<bool, StatusCode> {
        request_matches_etag(headers, ETAG).map_err(|error| error.into_response().status())
    }

    /// Header lines as text, so a failure names the case instead of printing the
    /// byte arrays the fixtures are written as.
    fn describe_field_lines(lines: &[&[u8]]) -> String {
        lines
            .iter()
            .map(|line| String::from_utf8_lossy(line).into_owned())
            .collect::<Vec<_>>()
            .join(" | ")
    }

    #[test]
    fn absent_if_none_match_matches_nothing() {
        assert_eq!(match_etag(&HeaderMap::new()), Ok(false));
    }

    #[test]
    fn if_none_match_matches_by_weak_comparison_over_the_whole_field_value() {
        // Expected values follow RFC 9110: §13.1.2 compares these entity-tags
        // weakly, §8.8.3 admits `,` and obs-text inside an opaque-tag, §5.3
        // joins repeated lines into one list, and §5.6.1.2 ignores empty
        // elements.
        for (lines, expected) in [
            (vec![b"\"cafe\"".as_slice()], true),
            (vec![b"\"other\"".as_slice()], false),
            (vec![b"*".as_slice()], true),
            (vec![b"W/\"cafe\"".as_slice()], true),
            (vec![b"W/\"other\"".as_slice()], false),
            (vec![b"\"other\", \"cafe\"".as_slice()], true),
            (vec![b"\"a\",\"b\"".as_slice()], false),
            (vec![b"\"other\"".as_slice(), b"\"cafe\"".as_slice()], true),
            (vec![b"\"other\"".as_slice(), b"\"nope\"".as_slice()], false),
            (vec![b"W/\"other\", \"cafe\", W/\"more\"".as_slice()], true),
            // A comma inside the quotes belongs to the tag, so this single tag
            // must not be read as the two-tag list `"a` and `cafe"`.
            (vec![b"\"a,cafe\"".as_slice()], false),
            (vec![b"\"a,b\", \"cafe\"".as_slice()], true),
            (vec![b"\t \"cafe\" \t".as_slice()], true),
            // Empty elements are a merge artefact a recipient must tolerate.
            (vec![b"\"cafe\",".as_slice()], true),
            (vec![b",\"cafe\"".as_slice()], true),
            (vec![b"\"other\" , ,\"cafe\" ,".as_slice()], true),
            (vec![b",".as_slice(), b"\"cafe\"".as_slice()], true),
            // The list alternative is optional as a whole, so a field that lists
            // no entity-tag matches none rather than failing the request.
            (vec![b"".as_slice()], false),
            (vec![b",".as_slice()], false),
            (vec![b" , ,".as_slice()], false),
            (vec![b"".as_slice(), b" ".as_slice()], false),
            // obs-text is legal in an opaque-tag: a tag this server never issued
            // does not match, and must not be rejected as unparsable.
            (vec![b"\"caf\xe9\"".as_slice()], false),
            (vec![b"\"caf\xe9\", \"cafe\"".as_slice()], true),
        ] {
            assert_eq!(
                match_etag(&if_none_match(&lines)),
                Ok(expected),
                "If-None-Match {}",
                describe_field_lines(&lines),
            );
        }
    }

    #[test]
    fn malformed_if_none_match_is_rejected() {
        for lines in [
            vec![b"cafe".as_slice()],
            vec![b"\"cafe".as_slice()],
            vec![b"cafe\"".as_slice()],
            vec![b"\"cafe\" \"other\"".as_slice()],
            vec![b"W/cafe".as_slice()],
            vec![b"W/ \"cafe\"".as_slice()],
            // `"*"` is the entire field value or nothing: it is an alternative
            // to the list, so it never takes a separator, and §5.3 joins the
            // lines before that rule is applied.
            vec![b"*,".as_slice()],
            vec![b",*".as_slice()],
            vec![b",,*,,".as_slice()],
            vec![b"*, \"cafe\"".as_slice()],
            vec![b"*".as_slice(), b"".as_slice()],
            vec![b"\"other\"".as_slice(), b"*".as_slice()],
            vec![b"*".as_slice(), b"\"cafe\"".as_slice()],
            vec![b"*".as_slice(), b"*".as_slice()],
            // The match on the first line must not excuse the malformed second.
            vec![b"\"cafe\"".as_slice(), b"nope".as_slice()],
        ] {
            assert_eq!(
                match_etag(&if_none_match(&lines)),
                Err(StatusCode::BAD_REQUEST),
                "If-None-Match {}",
                describe_field_lines(&lines),
            );
        }
    }
}
