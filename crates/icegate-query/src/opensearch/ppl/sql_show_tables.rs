//! `POST /_plugins/_sql`: only `SHOW TABLES LIKE '<pattern>'` is served; IceGate's query language
//! on this listener is PPL.

use axum::Json;
use axum::extract::State;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::opensearch::catalog::IndexEntry;
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, Parameters, RequestBody};
use crate::opensearch::state::OpenSearchState;

/// The ten JDBC `getTables` columns of the answer, all typed `keyword`.
const SHOW_TABLES_COLUMNS: [&str; 10] = [
    "TABLE_CAT",
    "TABLE_SCHEM",
    "TABLE_NAME",
    "TABLE_TYPE",
    "REMARKS",
    "TYPE_CAT",
    "TYPE_SCHEM",
    "TYPE_NAME",
    "SELF_REFERENCING_COL_NAME",
    "REF_GENERATION",
];

/// The refusal of every statement other than `SHOW TABLES LIKE`.
const SQL_REFUSAL: &str = "SQL is not supported; use PPL";

/// Query parameters of `POST /_plugins/_sql`; the answer is always the jdbc shape, and any parameter
/// other than the ignored `format` is a 400.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SqlParameters {}

impl EndpointParameters for SqlParameters {
    const IGNORED: &'static [&'static str] = &["format"];
}

/// One element of a LIKE pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LikeToken {
    /// `%`: any run of characters, the empty run included.
    AnyRun,
    /// `_`: exactly one character.
    OneCharacter,
    /// A character that must match itself (escaped with `\`, or any other character).
    Literal(char),
}

/// `POST /_plugins/_sql` with `{"query": "SHOW TABLES LIKE '<pattern>'"}`: the tenant's data indices
/// (registry indices and exact-name tables; dot indices never) whose names match the pattern
/// case-sensitively, sorted by name, one row `[<cluster>, null, <index>, "BASE TABLE", null × 6]` each.
///
/// # Errors
///
/// `illegal_argument_exception` for a body that is not JSON; with `key` `query`, for a body that
/// lacks a string `query` (its `value` under `value`, when it has one) and, with the reason
/// [`SQL_REFUSAL`] and the statement under `value`, for any other statement.
pub(crate) async fn show_tables(
    State(state): State<OpenSearchState>,
    context: RequestContext,
    Parameters(SqlParameters {}): Parameters<SqlParameters>,
    RequestBody(body): RequestBody,
) -> Result<Json<Value>, OpenSearchError> {
    let request: Value = serde_json::from_slice(&body)
        .map_err(|error| OpenSearchError::illegal_argument(format!("request body is not valid JSON: {error}")))?;
    let Some(Value::String(statement)) = request.get("query") else {
        return Err(OpenSearchError::invalid_request_key(
            "query",
            request.get("query"),
            "request body must carry a string [query]",
        ));
    };
    let pattern = parse_show_tables_like(statement)
        .ok_or_else(|| OpenSearchError::invalid_request_key("query", request.get("query"), SQL_REFUSAL))?;
    let session = state.catalog.open_session(&context.tenant).await?;
    // The index set lists its entries in name order, and filtering keeps that order.
    let rows: Vec<Value> = session
        .indices()
        .entries()
        .iter()
        .map(IndexEntry::name)
        .filter(|name| matches_like(&pattern, name))
        .map(|name| {
            json!([
                state.node.cluster_name,
                null,
                name,
                "BASE TABLE",
                null,
                null,
                null,
                null,
                null,
                null
            ])
        })
        .collect();
    let schema: Vec<Value> = SHOW_TABLES_COLUMNS
        .iter()
        .map(|name| json!({"name": name, "type": "keyword"}))
        .collect();
    let count = rows.len();
    Ok(Json(json!({
        "schema": schema,
        "datarows": rows,
        "total": count,
        "size": count,
        "status": 200,
    })))
}

/// The LIKE pattern of `SHOW TABLES LIKE '<pattern>'` (keywords ASCII case-insensitive, any
/// whitespace between them, an optional trailing `;`); `None` for any other statement.
fn parse_show_tables_like(statement: &str) -> Option<Vec<LikeToken>> {
    let statement = statement.trim();
    let statement = statement.strip_suffix(';').unwrap_or(statement);
    let rest = strip_keyword(statement, "show")?;
    let rest = strip_keyword(rest, "tables")?;
    let rest = strip_keyword(rest, "like")?;
    parse_like_literal(rest.trim())
}

/// `text` after `keyword` and the whitespace that must follow it.
fn strip_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let text = text.trim_start();
    let head = text.get(..keyword.len())?;
    let rest = text.get(keyword.len()..)?;
    (head.eq_ignore_ascii_case(keyword) && rest.starts_with(char::is_whitespace)).then_some(rest)
}

/// The tokens of a single-quoted LIKE literal: `\` escapes the next character and `''` is a quote.
fn parse_like_literal(quoted: &str) -> Option<Vec<LikeToken>> {
    let inner = quoted.strip_prefix('\'')?.strip_suffix('\'')?;
    let mut tokens = Vec::with_capacity(inner.len());
    let mut characters = inner.chars();
    while let Some(character) = characters.next() {
        tokens.push(match character {
            '\\' => LikeToken::Literal(characters.next()?),
            '\'' => match characters.next() {
                Some('\'') => LikeToken::Literal('\''),
                _ => return None,
            },
            '%' => LikeToken::AnyRun,
            '_' => LikeToken::OneCharacter,
            other => LikeToken::Literal(other),
        });
    }
    Some(tokens)
}

/// Whether `name` matches the pattern, by a greedy walk with a single backtrack point at the last
/// `%`: O(name length × pattern length) steps, without recursion and without allocating.
fn matches_like(pattern: &[LikeToken], name: &str) -> bool {
    // `character` and the resume offset are byte offsets into `name` that always sit on a char
    // boundary: they only ever advance by the UTF-8 length of the char they pass.
    let (mut token, mut character) = (0, 0);
    let mut resume: Option<(usize, usize)> = None;
    while let Some(next_character) = name[character..].chars().next() {
        match pattern.get(token) {
            Some(LikeToken::AnyRun) => {
                resume = Some((token, character));
                token += 1;
            }
            Some(LikeToken::OneCharacter) => {
                token += 1;
                character += next_character.len_utf8();
            }
            Some(LikeToken::Literal(expected)) if *expected == next_character => {
                token += 1;
                character += next_character.len_utf8();
            }
            _ => {
                // The last `%` absorbs one more character, the one at `consumed`, and the walk
                // restarts after it; with no `%` behind there is no match. A backtrack happens
                // only while `character < name.len()`, and `consumed <= character`, so a
                // character at `consumed` always exists; the `else` below cannot run.
                let Some((any_run, consumed)) = resume else {
                    return false;
                };
                let Some(absorbed) = name[consumed..].chars().next() else {
                    return false;
                };
                token = any_run + 1;
                character = consumed + absorbed.len_utf8();
                resume = Some((any_run, character));
            }
        }
    }
    pattern[token..].iter().all(|remaining| *remaining == LikeToken::AnyRun)
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    #[test]
    fn like_patterns_match_runs_single_characters_escapes_and_quotes() {
        let pattern = |text: &str| parse_like_literal(text).expect("a LIKE literal");
        for (literal, name, expected) in [
            ("'%'", "logs", true),
            ("'%'", "", true),
            ("'%s'", "spans", true),
            ("'%s'", "prices_effective", false),
            ("'_ogs'", "logs", true),
            ("'_ogs'", "blogs", false),
            ("'l%s'", "logs", true),
            ("'%a%a%'", "banana", true),
            ("'prices\\_%'", "prices_effective", true),
            ("'prices\\_%'", "pricesXeffective", false),
            ("'it''s'", "it's", true),
            ("'Logs'", "logs", false),
            // Multibyte characters: `_` takes one character whatever its UTF-8 length, and `%`
            // backtracks over a two-byte character.
            ("'_b'", "éb", true),
            ("'__'", "é", false),
            ("'%éb'", "ééb", true),
            ("'%é'", "cafe", false),
        ] {
            assert_eq!(
                matches_like(&pattern(literal), name),
                expected,
                "{literal} against {name:?}"
            );
        }
        assert_eq!(parse_like_literal("'unterminated"), None);
        assert_eq!(parse_like_literal("'a'b'"), None);
        assert_eq!(parse_like_literal("'trailing\\'"), None);
    }

    #[test]
    fn only_show_tables_like_is_recognized() {
        assert_eq!(
            parse_show_tables_like("SHOW tables LIKE '%'"),
            Some(vec![LikeToken::AnyRun])
        );
        assert!(parse_show_tables_like("show\nTABLES\tlike 'x';").is_some());
        for statement in [
            "SHOW TABLES",
            "SHOWTABLES LIKE '%'",
            "SELECT 1",
            "SHOW TABLES LIKE '%' extra",
            "",
        ] {
            assert_eq!(parse_show_tables_like(statement), None, "{statement:?}");
        }
    }

    /// LIKE matching by its definition: `%` tries every split, `_` takes one character, a literal
    /// takes itself. Exponential, so it serves only as the oracle for short inputs.
    fn matches_like_by_recursion(pattern: &[LikeToken], characters: &[char]) -> bool {
        match pattern.split_first() {
            None => characters.is_empty(),
            Some((LikeToken::AnyRun, rest)) => {
                (0..=characters.len()).any(|skipped| matches_like_by_recursion(rest, &characters[skipped..]))
            }
            Some((LikeToken::OneCharacter, rest)) => characters
                .split_first()
                .is_some_and(|(_, tail)| matches_like_by_recursion(rest, tail)),
            Some((LikeToken::Literal(expected), rest)) => characters
                .split_first()
                .is_some_and(|(first, tail)| first == expected && matches_like_by_recursion(rest, tail)),
        }
    }

    #[test]
    fn matches_like_agrees_with_a_recursive_oracle_on_seeded_patterns() {
        const TOKENS: [LikeToken; 4] = [
            LikeToken::AnyRun,
            LikeToken::OneCharacter,
            LikeToken::Literal('a'),
            LikeToken::Literal('b'),
        ];
        const CASES: usize = 4_000;
        let mut rng = StdRng::seed_from_u64(0x5107_7AB1);
        let mut matched_count = 0_usize;
        for _ in 0..CASES {
            let pattern: Vec<LikeToken> = (0..rng.random_range(0..=6))
                .map(|_| TOKENS[rng.random_range(0..TOKENS.len())])
                .collect();
            let name: String = (0..rng.random_range(0..=8))
                .map(|_| if rng.random_bool(0.5) { 'a' } else { 'b' })
                .collect();
            let characters: Vec<char> = name.chars().collect();
            let expected = matches_like_by_recursion(&pattern, &characters);
            assert_eq!(matches_like(&pattern, &name), expected, "{pattern:?} against {name:?}");
            matched_count += usize::from(expected);
        }
        // The seed reaches both outcomes, so the agreement is not a run of one answer.
        assert!(matched_count > 0, "no generated case matches");
        assert!(matched_count < CASES, "every generated case matches");
    }
}
