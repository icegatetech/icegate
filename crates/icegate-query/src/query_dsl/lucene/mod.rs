//! The Lucene `query_string` front end: its options and the parse-tree visitor.
//!
//! A query string is translated once, when the request is parsed, into the type-free
//! full-text tree that each searched index resolves against its own fields. Groups may
//! nest at most [`MAX_QUERY_STRING_DEPTH`] levels deep: the parser, the visitor and the
//! evaluators recurse once per level, so a deeper string is refused before it is parsed,
//! and a string nested past a few levels is parsed on a thread whose stack is sized for
//! the limit ([`translate_query_string`]).

mod visitor;

use serde_json::Value;
pub(crate) use visitor::{QueryStringError, translate_query_string};

use super::{
    ast::{Query, QueryStringClauses, QueryStringQuery},
    date_math::parse_time_zone,
    parse::{
        FUZZINESS_REFUSAL, SLOP_REFUSAL, read_field_patterns, read_flag, read_leaf_object, read_operator,
        read_optional_flag, read_text, refuse_nonzero_option,
    },
};
use crate::opensearch::error::OpenSearchError;

/// How deeply the groups of one query string may nest.
const MAX_QUERY_STRING_DEPTH: usize = 128;

/// The keys a `query_string` leaf accepts besides a numeric `boost`.
const QUERY_STRING_KEYS: &[&str] = &[
    "query",
    "default_field",
    "fields",
    "default_operator",
    "analyze_wildcard",
    "allow_leading_wildcard",
    "lenient",
    "time_zone",
    "fuzziness",
    "phrase_slop",
];

/// Parses a `query_string` leaf and translates its query string once, for every
/// index the request searches.
///
/// Keys: `query` (required), `default_field`, `fields` (each entry may carry `^boost`,
/// which is dropped), `default_operator` (`or` or `and`, any case), `analyze_wildcard`,
/// `allow_leading_wildcard` (default `true`), `lenient`, `time_zone` (an IANA name or an
/// offset), `fuzziness` and `phrase_slop` (accepted absent or zero only) and `boost` (a
/// number, accepted and ignored).
///
/// # Errors
///
/// `parsing_exception` for any other key (`_name` included), a value of the wrong
/// type, a missing `query`, both `fields` and `default_field`, a non-zero `fuzziness`
/// or `phrase_slop` and fuzzy or proximity syntax (each naming the refused construct as
/// `construct`), and groups nested deeper than
/// [`MAX_QUERY_STRING_DEPTH`] (naming it as `limit`); `500` when the thread that parses a
/// deeply nested string cannot start or panics. A syntax error is no error here: it is kept
/// as [`QueryStringClauses::Invalid`] and answered by each searched index.
pub(crate) fn parse_query_string(body: &Value) -> Result<Query, OpenSearchError> {
    let leaf = "query_string";
    let options = read_leaf_object(body, leaf, QUERY_STRING_KEYS)?;
    refuse_nonzero_option(options, "fuzziness", FUZZINESS_REFUSAL, leaf)?;
    refuse_nonzero_option(options, "phrase_slop", SLOP_REFUSAL, leaf)?;
    let read_optional_text = |key: &str| options.get(key).map(|_| read_text(options, key, leaf)).transpose();
    let query = QueryStringQuery {
        query: read_text(options, "query", leaf)?.to_owned(),
        default_field: read_optional_text("default_field")?.map(str::to_owned),
        fields: if options.contains_key("fields") {
            read_field_patterns(options, leaf)?
        } else {
            Vec::new()
        },
        default_operator: read_operator(options, "default_operator", leaf)?,
        analyze_wildcard: read_flag(options, "analyze_wildcard", leaf)?,
        allow_leading_wildcard: read_optional_flag(options, "allow_leading_wildcard", leaf)?.unwrap_or(true),
        lenient: read_optional_flag(options, "lenient", leaf)?,
        time_zone: read_optional_text("time_zone")?.map(parse_time_zone).transpose()?,
    };
    match translate_query_string(&query) {
        Ok(tree) => Ok(Query::LuceneQueryString(QueryStringClauses::Translated(tree))),
        Err(QueryStringError::Invalid(reason)) => Ok(Query::LuceneQueryString(QueryStringClauses::Invalid { reason })),
        Err(QueryStringError::Refused(error)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use chrono::{DateTime, Utc};
    use serde_json::{Value, json};

    use super::{MAX_QUERY_STRING_DEPTH, parse_query_string};
    use crate::{
        opensearch::{error::render_envelope, system_index::evaluate::IndexQueryEvaluator},
        query_dsl::{
            ast::{Query, QueryStringClauses},
            parse::parse_query,
        },
    };

    /// Renders a refused leaf and answers its status and envelope.
    async fn render_refusal(body: &Value) -> (StatusCode, Value) {
        let error = parse_query_string(body).expect_err("refused");
        render_envelope(error).await
    }

    #[tokio::test]
    async fn query_string_options_parse_and_refusals_are_parsing_exceptions() {
        let discover_style = json!({
            "query": "level:ERROR",
            "analyze_wildcard": true,
            "time_zone": "Europe/Berlin",
            "boost": 1
        });
        assert!(matches!(
            parse_query_string(&discover_style),
            Ok(Query::LuceneQueryString(QueryStringClauses::Translated(_)))
        ));
        assert!(matches!(
            parse_query_string(&json!({"query": "a/b"})),
            Ok(Query::LuceneQueryString(QueryStringClauses::Invalid { .. }))
        ));
        // Default fuzziness and phrase slop are accepted.
        for accepted in [
            json!({"query": "a", "phrase_slop": 0}),
            json!({"query": "a", "fuzziness": "0"}),
            json!({"query": "a", "fields": ["body^2", "log_*"], "default_operator": "AND", "lenient": false}),
        ] {
            assert!(
                matches!(
                    parse_query_string(&accepted),
                    Ok(Query::LuceneQueryString(QueryStringClauses::Translated(_)))
                ),
                "{accepted}"
            );
        }
        for refused in [
            json!({"query": "a", "_name": "named"}),
            json!({"default_field": "body"}),
            json!({"query": "a", "default_operator": "XOR"}),
            json!({"query": "a", "fields": ["body"], "default_field": "body"}),
            json!({"query": 7}),
            json!({"query": "a", "fields": ["title^x"]}),
            json!({"query": "a", "time_zone": "Mars/Olympus"}),
        ] {
            let (status, body) = render_refusal(&refused).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");
            assert_eq!(body["error"]["type"], "parsing_exception", "{refused}");
        }
        // A non-default fuzziness or phrase slop is refused naming its key.
        for (refused, construct) in [
            (json!({"query": "a", "phrase_slop": 2}), "phrase_slop"),
            (json!({"query": "a", "fuzziness": 1}), "fuzziness"),
            (json!({"query": "roam~"}), "fuzzy"),
        ] {
            let (status, body) = render_refusal(&refused).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");
            assert_eq!(body["error"]["type"], "parsing_exception", "{refused}");
            assert_eq!(body["error"]["construct"], construct, "{refused}");
        }
    }

    /// `depth` groups around one term: `((a))` for a depth of 2.
    fn build_nested_groups(depth: usize) -> String {
        format!("{}a{}", "(".repeat(depth), ")".repeat(depth))
    }

    #[tokio::test]
    async fn groups_nest_up_to_the_limit_and_one_level_more_is_refused() {
        let now: DateTime<Utc> = "2026-10-08T12:34:56.789Z".parse().expect("a literal instant");
        let mappings = json!({"properties": {"body": {"type": "text"}}});
        let at_limit = json!({"query_string": {"query": build_nested_groups(MAX_QUERY_STRING_DEPTH)}});
        let query = parse_query(&at_limit).expect("the limit itself parses");
        let evaluator = IndexQueryEvaluator::new(&query, ".kibana_1", &mappings, now).expect("prepares");
        for (source, is_match) in [(json!({"body": "a"}), true), (json!({"body": "b"}), false)] {
            assert_eq!(
                evaluator.matches_document("1", &source).expect("evaluates"),
                is_match,
                "{source}"
            );
        }
        let (status, body) = render_refusal(&json!({"query": build_nested_groups(MAX_QUERY_STRING_DEPTH + 1)})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
        assert_eq!(body["error"]["limit"], json!(128));
        // Quoted and escaped parentheses open no group.
        let quoted = format!(
            "\"{}\" AND {}",
            "(".repeat(200),
            build_nested_groups(MAX_QUERY_STRING_DEPTH)
        );
        let escaped = format!(
            "{} AND {}",
            "\\(".repeat(200),
            build_nested_groups(MAX_QUERY_STRING_DEPTH)
        );
        for text in [quoted, escaped] {
            assert!(
                matches!(
                    parse_query_string(&json!({"query": text})),
                    Ok(Query::LuceneQueryString(QueryStringClauses::Translated(_)))
                ),
                "{text}"
            );
        }
    }

    /// Far more groups than the limit, so a depth check that misses them overflows the
    /// stack of the test thread instead of answering.
    const OVERFLOWING_DEPTH: usize = 10_000;

    #[tokio::test]
    async fn groups_after_a_quote_inside_a_range_bound_or_a_regexp_term_are_refused_at_the_limit() {
        for prefix in ["[a\"b TO c] ", "/a\"b/ "] {
            let text = format!("{prefix}{}", build_nested_groups(OVERFLOWING_DEPTH));
            let (status, body) = render_refusal(&json!({"query": text})).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{prefix}");
            assert_eq!(body["error"]["type"], "parsing_exception", "{prefix}");
            assert_eq!(body["error"]["limit"], json!(128), "{prefix}");
        }
    }

    #[tokio::test]
    async fn groups_after_an_unterminated_quote_are_no_tokens_and_the_string_is_a_syntax_error() {
        let text = format!("\"a {}", build_nested_groups(OVERFLOWING_DEPTH));
        assert!(matches!(
            parse_query_string(&json!({"query": text.clone()})),
            Ok(Query::LuceneQueryString(QueryStringClauses::Invalid { .. }))
        ));
        let now: DateTime<Utc> = "2026-10-08T12:34:56.789Z".parse().expect("a literal instant");
        let query = parse_query(&json!({"query_string": {"query": text}})).expect("a syntax error is no refusal");
        let mappings = json!({});
        let error = IndexQueryEvaluator::new(&query, ".kibana_1", &mappings, now)
            .and_then(|evaluator| evaluator.matches_document("1", &json!({})))
            .expect_err("refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "search_phase_execution_exception");
        assert_eq!(body["error"]["root_cause"][0]["type"], "query_shard_exception");
    }
}
