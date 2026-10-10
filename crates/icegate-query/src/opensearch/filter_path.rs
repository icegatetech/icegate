//! `filter_path`: the generic response filter, applied to every successful JSON response of the
//! listener. An error envelope is never filtered: every refusal stays a full envelope.
//!
//! The value is a comma list of dot paths:
//! - a segment is a glob (`*`, `?`);
//! - `**` matches any depth, zero included;
//! - `\.` is a literal dot inside a key;
//! - a leading `-` excludes the path;
//! - an empty item (an empty value, a doubled or trailing comma) names no path and adds none.
//!
//! Arrays consume no segment: a path applies to each element.

use axum::body::{Body, HttpBody as _};
use axum::extract::Request;
use axum::http::{Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::{Map, Value};

use super::error::OpenSearchError;
use super::glob::matches_glob;

/// One segment of a parsed path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    /// A key glob.
    Name(String),
    /// `**`: any number of keys, zero included.
    AnyDepth,
}

/// A parsed `filter_path`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilterPath {
    /// Paths whose matches are kept; none keeps the whole value.
    inclusions: Vec<Vec<Segment>>,
    /// Paths whose matches are removed from what the inclusions kept.
    exclusions: Vec<Vec<Segment>>,
}

impl FilterPath {
    /// Parse a `filter_path` value. An empty item of the comma list is skipped, so an empty value
    /// keeps the whole response.
    ///
    /// # Errors
    ///
    /// Returns `illegal_argument_exception` for an empty segment or a trailing escape.
    pub(crate) fn parse(text: &str) -> Result<Self, OpenSearchError> {
        let mut inclusions = Vec::new();
        let mut exclusions = Vec::new();
        for expression in text.split(',').filter(|expression| !expression.is_empty()) {
            match expression.strip_prefix('-') {
                Some(excluded_expression) => exclusions.push(split_segments(excluded_expression)?),
                None => inclusions.push(split_segments(expression)?),
            }
        }
        Ok(Self { inclusions, exclusions })
    }

    /// Keep the included paths of `value` (all of it when nothing is included), then remove the
    /// excluded paths. When no included path matches, the result is `{}`.
    pub(crate) fn apply(&self, value: Value) -> Value {
        let included_value = if self.inclusions.is_empty() {
            value
        } else {
            let patterns: Vec<&[Segment]> = self.inclusions.iter().map(Vec::as_slice).collect();
            keep_matched_paths(value, &patterns).unwrap_or_else(|| Value::Object(Map::new()))
        };
        let patterns: Vec<&[Segment]> = self.exclusions.iter().map(Vec::as_slice).collect();
        remove_matched_paths(included_value, &patterns)
    }
}

/// Split one path expression into segments, resolving `\` escapes.
fn split_segments(expression: &str) -> Result<Vec<Segment>, OpenSearchError> {
    let mut names = Vec::new();
    let mut segment_name = String::new();
    let mut characters = expression.chars();
    while let Some(character) = characters.next() {
        match character {
            '\\' => {
                let Some(escaped_character) = characters.next() else {
                    return Err(OpenSearchError::illegal_argument(format!(
                        "filter_path [{expression}] ends with an escape"
                    )));
                };
                segment_name.push(escaped_character);
            }
            '.' => names.push(std::mem::take(&mut segment_name)),
            character => segment_name.push(character),
        }
    }
    names.push(segment_name);
    if names.iter().any(String::is_empty) {
        return Err(OpenSearchError::illegal_argument(format!(
            "filter_path [{expression}] has an empty segment"
        )));
    }
    let mut segments: Vec<Segment> = names
        .into_iter()
        .map(|name| {
            if name == "**" {
                Segment::AnyDepth
            } else {
                Segment::Name(name)
            }
        })
        .collect();
    // `**.**` matches what one `**` matches. With one `**` per run, advancing a pattern pushes at
    // most two patterns and `is_consumed` reads at most two segments, whatever the run length.
    segments
        .dedup_by(|segment, previous_segment| *segment == Segment::AnyDepth && *previous_segment == Segment::AnyDepth);
    Ok(segments)
}

/// Whether nothing but `**` remains of `pattern`, so everything below the current key matches.
fn is_consumed(pattern: &[Segment]) -> bool {
    pattern.iter().all(|segment| *segment == Segment::AnyDepth)
}

/// Push onto `remaining_patterns` what is left of `pattern` once `key` consumed its head.
fn advance_pattern<'a>(pattern: &'a [Segment], key: &str, remaining_patterns: &mut Vec<&'a [Segment]>) {
    match pattern.split_first() {
        None => {}
        Some((Segment::Name(glob), rest)) => {
            if matches_glob(glob, key) {
                remaining_patterns.push(rest);
            }
        }
        Some((Segment::AnyDepth, rest)) => {
            // `**` either absorbs `key` and stays, or matches zero keys so that `rest` consumes it.
            remaining_patterns.push(pattern);
            advance_pattern(rest, key, remaining_patterns);
        }
    }
}

/// What is left of every pattern of `patterns` once `key` consumed its head, without duplicates.
///
/// Every pattern is a suffix of one parsed expression, so there are at most as many as the
/// expressions have suffixes. Without the deduplication a `**` both stays and advances at every
/// key, and the copies multiply with depth: combinatorially in the number of `**` of a path.
fn advance_patterns<'a>(patterns: &[&'a [Segment]], key: &str) -> Vec<&'a [Segment]> {
    let mut remaining_patterns = Vec::new();
    for pattern in patterns {
        advance_pattern(pattern, key, &mut remaining_patterns);
    }
    // A suffix of a parsed expression is identified by its address and length: comparing them
    // costs O(1), where comparing the segments costs the suffix length.
    remaining_patterns.sort_unstable_by_key(|pattern| (pattern.as_ptr(), pattern.len()));
    remaining_patterns.dedup_by_key(|pattern| (pattern.as_ptr(), pattern.len()));
    remaining_patterns
}

/// The part of `value` some pattern of `patterns` reaches, or `None` when none reaches anything.
fn keep_matched_paths(value: Value, patterns: &[&[Segment]]) -> Option<Value> {
    if patterns.iter().any(|pattern| is_consumed(pattern)) {
        return Some(value);
    }
    match value {
        Value::Object(object) => {
            let kept_entries: Map<String, Value> = object
                .into_iter()
                .filter_map(|(key, child)| {
                    let child_patterns = advance_patterns(patterns, &key);
                    if child_patterns.is_empty() {
                        return None;
                    }
                    keep_matched_paths(child, &child_patterns).map(|filtered_child| (key, filtered_child))
                })
                .collect();
            (!kept_entries.is_empty()).then_some(Value::Object(kept_entries))
        }
        Value::Array(items) => {
            let kept_items: Vec<Value> = items
                .into_iter()
                .filter_map(|item| keep_matched_paths(item, patterns))
                .collect();
            (!kept_items.is_empty()).then_some(Value::Array(kept_items))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => None,
    }
}

/// `value` without the parts some pattern of `patterns` reaches in full.
fn remove_matched_paths(value: Value, patterns: &[&[Segment]]) -> Value {
    if patterns.is_empty() {
        return value;
    }
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .filter_map(|(key, child)| {
                    let child_patterns = advance_patterns(patterns, &key);
                    if child_patterns.iter().any(|pattern| is_consumed(pattern)) {
                        return None;
                    }
                    Some((key, remove_matched_paths(child, &child_patterns)))
                })
                .collect(),
        ),
        Value::Array(items) => {
            Value::Array(items.into_iter().map(|item| remove_matched_paths(item, patterns)).collect())
        }
        scalar @ (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)) => scalar,
    }
}

/// Every `filter_path` value of `uri`, joined with `,`: repeating the parameter adds paths.
fn read_filter_path(uri: &Uri) -> Option<String> {
    let pairs: Vec<(String, String)> = serde_urlencoded::from_str(uri.query()?).ok()?;
    let values: Vec<String> = pairs
        .into_iter()
        .filter(|(key, _)| key == "filter_path")
        .map(|(_, value)| value)
        .collect();
    (!values.is_empty()).then(|| values.join(","))
}

/// Apply the request's `filter_path` to a successful JSON response, whatever the method.
///
/// A response is buffered and re-serialized only when its request carries the parameter and it
/// succeeded with a body, so no other request pays for it. An error answer is returned untouched,
/// because every refusal must reach the client as a full envelope; a filter that does not parse
/// is answered `400` before the handler runs.
///
/// A `HEAD` request to a `GET` route reaches this layer with the full `GET` body: axum sets
/// `Content-Length` from the body and empties it only after the route's layers have run. So a
/// `HEAD` answer reports the length of the filtered `GET` answer, and the router test
/// `a_head_request_under_filter_path_answers_like_the_filtered_get` pins that. A success answer
/// without a body, as an explicit `HEAD` route sends, has nothing to filter and passes through.
pub(crate) async fn apply_filter_path(request: Request, next: Next) -> Response {
    let Some(filter_text) = read_filter_path(request.uri()) else {
        return next.run(request).await;
    };
    let filter = match FilterPath::parse(&filter_text) {
        Ok(filter) => filter,
        Err(error) => return error.into_response(),
    };
    let response = next.run(request).await;
    if !response.status().is_success() || response.body().size_hint().exact() == Some(0) {
        return response;
    }
    let is_json = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"));
    if !is_json {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let response_value = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|error| OpenSearchError::internal(format!("reading the response body: {error}")))
        .and_then(|bytes| {
            serde_json::from_slice::<Value>(&bytes)
                .map_err(|error| OpenSearchError::internal(format!("the response is not JSON: {error}")))
        });
    response_value.map_or_else(IntoResponse::into_response, |value| {
        parts.headers.remove(header::CONTENT_LENGTH);
        Response::from_parts(parts, Body::from(filter.apply(value).to_string()))
    })
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use axum::middleware;
    use axum::routing::head;
    use serde_json::{Value, json};
    use tower::ServiceExt as _;

    use super::{FilterPath, Segment, advance_patterns, apply_filter_path};
    use crate::opensearch::error::render_envelope;

    fn apply_filter(filter_text: &str, value: Value) -> Value {
        FilterPath::parse(filter_text).expect("a valid filter").apply(value)
    }

    /// A version-poll-style filter: the version, publish address and IP of each node.
    #[test]
    fn nodes_filter_keeps_version_ip_and_publish_address_of_each_node() {
        let nodes = json!({
            "_nodes": {"total": 2},
            "cluster_name": "icegate",
            "nodes": {
                "n1": {
                    "name": "icegate-node", "version": "3.9.0", "ip": "127.0.0.1",
                    "http": {"publish_address": "127.0.0.1:9200", "bound_address": ["0.0.0.0:9200"]}
                },
                "n2": {
                    "name": "other-node", "version": "3.9.0", "ip": "10.0.0.2",
                    "http": {"publish_address": "10.0.0.2:9200", "bound_address": ["10.0.0.2:9200"]}
                }
            }
        });

        let filtered_nodes = apply_filter("nodes.*.version,nodes.*.http.publish_address,nodes.*.ip", nodes);

        assert_eq!(
            filtered_nodes,
            json!({"nodes": {
                "n1": {"version": "3.9.0", "ip": "127.0.0.1", "http": {"publish_address": "127.0.0.1:9200"}},
                "n2": {"version": "3.9.0", "ip": "10.0.0.2", "http": {"publish_address": "10.0.0.2:9200"}}
            }})
        );
    }

    /// A settings-style filter escapes the dots of a flat key.
    #[test]
    fn escaped_dots_match_one_key_containing_dots() {
        let settings = json!({"persistent": {"plugins.calcite.enabled": "true", "other": "x"}, "defaults": {}});

        let filtered_settings = apply_filter(r"*.plugins\.calcite\.enabled", settings);

        assert_eq!(
            filtered_settings,
            json!({"persistent": {"plugins.calcite.enabled": "true"}})
        );
    }

    #[test]
    fn double_star_matches_at_any_depth_including_zero() {
        let value = json!({"version": "a", "nodes": {"n1": {"version": "b", "ip": "c", "deep": {"version": "d"}}}});

        assert_eq!(
            apply_filter("**.version", value),
            json!({"version": "a", "nodes": {"n1": {"version": "b", "deep": {"version": "d"}}}})
        );
    }

    #[test]
    fn arrays_are_filtered_element_by_element() {
        let value = json!({"hits": {"hits": [{"_id": "1", "_source": {}}, {"_id": "2", "_source": {}}]}});

        assert_eq!(
            apply_filter("hits.hits._id", value),
            json!({"hits": {"hits": [{"_id": "1"}, {"_id": "2"}]}})
        );
    }

    /// An array none of whose elements keeps anything is dropped, not kept as `[]`.
    #[test]
    fn an_array_whose_elements_all_miss_is_dropped() {
        let value = json!({"hits": {"total": 2, "hits": [{"_source": {"a": 1}}, {"_source": {"a": 2}}]}});

        assert_eq!(
            apply_filter("hits.total,hits.hits._id", value),
            json!({"hits": {"total": 2}})
        );
    }

    #[test]
    fn a_trailing_double_star_keeps_the_whole_subtree() {
        let value = json!({"nodes": {"n1": {"http": {"a": 1}}}, "other": 2});

        assert_eq!(
            apply_filter("nodes.**", value),
            json!({"nodes": {"n1": {"http": {"a": 1}}}})
        );
    }

    #[test]
    fn an_exclusion_removes_a_matched_subtree() {
        let value = json!({"nodes": {"n1": {"version": "3.9.0", "http": {"a": 1}}}});

        assert_eq!(
            apply_filter("-nodes.*.http", value),
            json!({"nodes": {"n1": {"version": "3.9.0"}}})
        );
    }

    #[test]
    fn an_exclusion_applies_to_each_array_element() {
        let value = json!({"hits": {"total": 2, "hits": [
            {"_id": "1", "_source": {"a": 1}},
            {"_id": "2", "_source": {"a": 2}}
        ]}});

        assert_eq!(
            apply_filter("-hits.hits._source", value),
            json!({"hits": {"total": 2, "hits": [{"_id": "1"}, {"_id": "2"}]}})
        );
    }

    /// An exclusion path that runs into a scalar before its end reaches nothing in full.
    #[test]
    fn an_exclusion_through_a_scalar_leaves_it() {
        assert_eq!(apply_filter("-a.b", json!({"a": 1, "c": 2})), json!({"a": 1, "c": 2}));
    }

    /// Inclusions select first and exclusions then cut from what they kept, whatever the order of
    /// the expressions in the value.
    #[test]
    fn an_exclusion_cuts_from_what_the_inclusions_kept() {
        let value = json!({
            "cluster_name": "icegate",
            "nodes": {"n1": {"version": "3.9.0", "http": {"publish_address": "a", "bound_address": ["b"]}}}
        });

        assert_eq!(
            apply_filter("-nodes.*.http.bound_address,nodes.**", value),
            json!({"nodes": {"n1": {"version": "3.9.0", "http": {"publish_address": "a"}}}})
        );
    }

    #[test]
    fn no_match_yields_an_empty_object() {
        assert_eq!(apply_filter("absent.path", json!({"present": 1})), json!({}));
    }

    /// An empty item names no path, so it adds none and the other items still apply.
    #[test]
    fn empty_items_of_the_list_add_no_path() {
        let value = json!({"a": 1, "b": 2, "c": 3});
        let cases = [
            ("", json!({"a": 1, "b": 2, "c": 3})),
            ("a,", json!({"a": 1})),
            (",a", json!({"a": 1})),
            ("a,,b", json!({"a": 1, "b": 2})),
        ];
        for (filter_text, filtered_value) in cases {
            assert_eq!(
                apply_filter(filter_text, value.clone()),
                filtered_value,
                "{filter_text:?}"
            );
        }
    }

    /// The last row is an exclusion, whose path is refused by the same rules.
    #[tokio::test]
    async fn a_trailing_escape_or_empty_segment_is_refused() {
        for filter_text in [r"a.b\", "a..b", ".a", "a.", "-a..b"] {
            let error = FilterPath::parse(filter_text).expect_err("refused");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{filter_text:?}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{filter_text:?}");
        }
    }

    #[test]
    fn consecutive_double_stars_parse_as_one() {
        assert_eq!(
            FilterPath::parse("**.**.**.a,-b.**.**").expect("a valid filter"),
            FilterPath {
                inclusions: vec![vec![Segment::AnyDepth, Segment::Name("a".to_string())]],
                exclusions: vec![vec![Segment::Name("b".to_string()), Segment::AnyDepth]],
            }
        );
    }

    /// A long run of `**` costs no more than one `**` and matches no deeper and no shallower.
    #[test]
    fn a_run_of_double_stars_matches_like_one_double_star() {
        let run = "**.".repeat(64);
        let value = json!({"a": {"b": {"c": {"d": 1}}}, "e": [{"f": 2}]});

        assert_eq!(apply_filter(&format!("{run}zz"), value.clone()), json!({}));
        assert_eq!(
            apply_filter(&format!("{run}d"), value),
            json!({"a": {"b": {"c": {"d": 1}}}})
        );
    }

    /// A run of `**` before an absent name stays one live pattern at every depth: the pattern
    /// itself, never one more per `**` of the run.
    #[test]
    fn advancing_a_run_of_double_stars_keeps_one_live_pattern() {
        let filter = FilterPath::parse(&format!("{}zz", "**.".repeat(64))).expect("a valid filter");
        let mut live_patterns: Vec<&[Segment]> = vec![filter.inclusions[0].as_slice()];
        for key in ["a", "b", "c", "d"] {
            live_patterns = advance_patterns(&live_patterns, key);

            assert_eq!(live_patterns.len(), 1, "{key}");
        }
    }

    /// `**.a` repeated 32 times, walked through keys `a`: every live pattern that starts with `**`
    /// both stays and advances past the `a`, so the copies would double per depth. Kept once each,
    /// after `depth` keys the live patterns are the `depth + 1` suffixes `(**.a)^j`, `j` from
    /// `32 - depth` to 32 (`j = 0` being the empty, fully consumed pattern).
    #[test]
    fn advancing_repeated_double_star_pairs_keeps_each_suffix_once() {
        let filter = FilterPath::parse(&["**.a"; 32].join(".")).expect("a valid filter");
        assert_eq!(filter.inclusions[0].len(), 64);
        let mut live_patterns: Vec<&[Segment]> = vec![filter.inclusions[0].as_slice()];
        for depth in 1..=32 {
            live_patterns = advance_patterns(&live_patterns, "a");

            assert_eq!(live_patterns.len(), depth + 1, "depth {depth}");
        }
    }

    /// A success answer without a body, as an explicit `HEAD` route sends with the `GET` headers,
    /// has nothing to filter: it passes through instead of failing to parse as JSON.
    #[tokio::test]
    async fn a_success_answer_without_a_body_passes_through_unfiltered() {
        let app = Router::new()
            .route("/", head(|| async { [(header::CONTENT_TYPE, "application/json")] }))
            .layer(middleware::from_fn(apply_filter_path));
        let request = Request::builder()
            .method("HEAD")
            .uri("/?filter_path=x")
            .body(Body::empty())
            .expect("request");

        let response = app.oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        assert!(body.is_empty());
    }
}
