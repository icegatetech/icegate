//! Query DSL JSON into the [`Query`] tree.
//!
//! The parser is strict: an unknown leaf, an unknown key inside a known leaf and `_name` are
//! `400 parsing_exception` naming them, because ignoring part of a query would return wrong
//! data. Every leaf accepts a numeric `boost` and ignores it: IceGate does not score.
//!
//! Full-text leaves (`match`, `match_phrase`, `match_phrase_prefix`, `multi_match`,
//! `simple_query_string`) refuse a non-zero `fuzziness` or `slop`, and a fuzzy term or sloppy
//! phrase inside a `simple_query_string`: IceGate matches exactly. `max_expansions`,
//! `prefix_length`, `fuzzy_transpositions`, `auto_generate_synonyms_phrase_query` and `tie_breaker`
//! are accepted when they carry their type, and not applied. A field pattern may carry `^boost`,
//! which is dropped; `fields` absent or empty is every field.

use serde_json::{Map, Value};

use super::ast::{
    ALL_FIELDS, BoolQuery, ExistsQuery, IdsQuery, MatchPhrasePrefixQuery, MatchPhraseQuery, MatchQuery,
    MultiMatchQuery, MultiMatchType, NestedQuery, Operator, PrefixQuery, Query, RangeBound, RangeQuery,
    SimpleQueryStringQuery, TermQuery, TermsQuery, WildcardQuery, ZeroTermsQuery,
};
use super::date_math::{DateFormat, parse_time_zone};
use super::simple_query::{SimpleQueryFlags, parse_simple_query_string};
use crate::opensearch::error::OpenSearchError;

/// The `score_mode` values `nested` accepts; IceGate does not score, so each is accepted and
/// has no effect.
const NESTED_SCORE_MODES: [&str; 5] = ["avg", "sum", "min", "max", "none"];

/// Why a `fuzziness` above zero is refused.
const FUZZINESS_REFUSAL: &str = "fuzzy matching is not evaluated";

/// Why a `slop` above zero is refused.
const SLOP_REFUSAL: &str = "sloppy phrases are not evaluated";

/// Parse one query object.
///
/// # Errors
///
/// `parsing_exception` naming the offending leaf or key: a value that is not an object with
/// exactly one leaf, an unknown leaf, an unknown key in a known leaf (`_name` included), a value
/// of the wrong JSON type, a `wildcard` that states its value under both of its keys, a non-zero
/// `fuzziness` or `slop`, or a `simple_query_string` that [`parse_simple_query_string`] refuses.
pub(crate) fn parse_query(value: &Value) -> Result<Query, OpenSearchError> {
    let (leaf, body) = read_single_entry(value, "query")?;
    match leaf {
        "match_all" => read_leaf_object(body, leaf, &[]).map(|_| Query::MatchAll),
        "match_none" => read_leaf_object(body, leaf, &[]).map(|_| Query::MatchNone),
        "bool" => parse_bool(body).map(Query::Bool),
        "term" => parse_term(body).map(Query::Term),
        "terms" => parse_terms(body).map(Query::Terms),
        "ids" => parse_ids(body).map(Query::Ids),
        "range" => parse_range(body).map(Query::Range),
        "exists" => parse_exists(body).map(Query::Exists),
        "prefix" => {
            let (field, value, case_insensitive) =
                parse_pattern_leaf(body, leaf, &["value", "case_insensitive"], &["value"])?;
            Ok(Query::Prefix(PrefixQuery {
                field,
                value,
                case_insensitive,
            }))
        }
        "wildcard" => {
            let (field, pattern, case_insensitive) = parse_pattern_leaf(
                body,
                leaf,
                &["value", "wildcard", "case_insensitive"],
                &["value", "wildcard"],
            )?;
            Ok(Query::Wildcard(WildcardQuery {
                field,
                pattern,
                case_insensitive,
            }))
        }
        "constant_score" => {
            let options = read_leaf_object(body, leaf, &["filter"])?;
            let filter = options
                .get("filter")
                .ok_or_else(|| OpenSearchError::parsing("[constant_score] needs a [filter]"))?;
            Ok(Query::ConstantScore(Box::new(parse_query(filter)?)))
        }
        "nested" => parse_nested(body).map(Query::Nested),
        "match" => parse_match(body).map(Query::Match),
        "match_phrase" => {
            let (field, query) = parse_phrase(body, leaf, &["query", "slop"])?;
            Ok(Query::MatchPhrase(MatchPhraseQuery { field, query }))
        }
        "match_phrase_prefix" => {
            let (field, query) = parse_phrase(body, leaf, &["query", "slop", "max_expansions"])?;
            Ok(Query::MatchPhrasePrefix(MatchPhrasePrefixQuery { field, query }))
        }
        "multi_match" => parse_multi_match(body).map(Query::MultiMatch),
        "simple_query_string" => parse_simple_query_string_leaf(body).map(Query::SimpleQueryString),
        other => Err(OpenSearchError::parsing(format!("unknown query [{other}]"))),
    }
}

/// `keys` joined so that a refusal's single pair of brackets names each of them.
fn render_key_list<'a>(keys: impl Iterator<Item = &'a String>) -> String {
    keys.map(String::as_str).collect::<Vec<_>>().join("], [")
}

/// `value` as an object holding exactly one entry; a refusal of several entries names their keys.
fn read_single_entry<'a>(value: &'a Value, context: &str) -> Result<(&'a str, &'a Value), OpenSearchError> {
    let object = value
        .as_object()
        .ok_or_else(|| OpenSearchError::parsing(format!("[{context}] must be an object")))?;
    let mut entries = object.iter();
    match (entries.next(), entries.next()) {
        (Some((key, body)), None) => Ok((key.as_str(), body)),
        (None, _) => Err(OpenSearchError::parsing(format!("[{context}] is empty"))),
        (Some(_), Some(_)) => Err(OpenSearchError::parsing(format!(
            "[{context}] holds more than one entry: [{}]",
            render_key_list(object.keys())
        ))),
    }
}

/// Refuse a `boost` of `leaf` that is not a number.
fn require_numeric_boost(boost: &Value, leaf: &str) -> Result<(), OpenSearchError> {
    if boost.is_number() {
        return Ok(());
    }
    Err(OpenSearchError::parsing(format!("[{leaf}] [boost] must be a number")))
}

/// `body` as the options object of `leaf`, holding only `allowed` keys and a numeric `boost`.
fn read_leaf_object<'a>(
    body: &'a Value,
    leaf: &str,
    allowed: &[&str],
) -> Result<&'a Map<String, Value>, OpenSearchError> {
    let object = body
        .as_object()
        .ok_or_else(|| OpenSearchError::parsing(format!("[{leaf}] must be an object")))?;
    for (key, value) in object {
        if key == "boost" {
            require_numeric_boost(value, leaf)?;
        } else if !allowed.contains(&key.as_str()) {
            return Err(OpenSearchError::parsing(format!(
                "[{leaf}] query does not support [{key}]"
            )));
        }
    }
    Ok(object)
}

/// A JSON string, number or boolean.
fn read_scalar(value: &Value, leaf: &str) -> Result<Value, OpenSearchError> {
    match value {
        Value::String(_) | Value::Number(_) | Value::Bool(_) => Ok(value.clone()),
        Value::Null | Value::Array(_) | Value::Object(_) => Err(OpenSearchError::parsing(format!(
            "[{leaf}] takes a string, number or boolean value"
        ))),
    }
}

/// The boolean option `key`, `false` when it is absent.
fn read_flag(options: &Map<String, Value>, key: &str, leaf: &str) -> Result<bool, OpenSearchError> {
    Ok(read_optional_flag(options, key, leaf)?.unwrap_or(false))
}

/// The required string option `key`.
fn read_text<'a>(options: &'a Map<String, Value>, key: &str, leaf: &str) -> Result<&'a str, OpenSearchError> {
    options
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| OpenSearchError::parsing(format!("[{leaf}] needs a string [{key}]")))
}

/// `bool`: each clause list as an object or an array, and `minimum_should_match`.
fn parse_bool(body: &Value) -> Result<BoolQuery, OpenSearchError> {
    let options = read_leaf_object(
        body,
        "bool",
        &["must", "filter", "should", "must_not", "minimum_should_match"],
    )?;
    let read_clauses = |key: &str| {
        options
            .get(key)
            .map_or_else(|| Ok(Vec::new()), |value| parse_clauses(value, key))
    };
    Ok(BoolQuery {
        must: read_clauses("must")?,
        filter: read_clauses("filter")?,
        should: read_clauses("should")?,
        must_not: read_clauses("must_not")?,
        minimum_should_match: options
            .get("minimum_should_match")
            .map(parse_minimum_should_match)
            .transpose()?,
    })
}

/// One `bool` clause list, written as a single query or as an array of them.
fn parse_clauses(value: &Value, key: &str) -> Result<Vec<Query>, OpenSearchError> {
    match value {
        Value::Array(clauses) => clauses.iter().map(parse_query).collect(),
        Value::Object(_) => Ok(vec![parse_query(value)?]),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => Err(OpenSearchError::parsing(format!(
            "[bool] [{key}] must be a query or a list of queries"
        ))),
    }
}

/// A non-negative integer, as a JSON number or a string of ASCII digits.
fn parse_minimum_should_match(value: &Value) -> Result<u32, OpenSearchError> {
    let count = match value {
        Value::Number(number) => number.as_u64().and_then(|count| u32::try_from(count).ok()),
        Value::String(text) if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) => text.parse().ok(),
        Value::Null | Value::Bool(_) | Value::String(_) | Value::Array(_) | Value::Object(_) => None,
    };
    count.ok_or_else(|| {
        OpenSearchError::parsing(format!(
            "[minimum_should_match] [{value}] is not a non-negative integer"
        ))
    })
}

/// `term`: `{field: value}` or `{field: {value, case_insensitive, boost}}`.
fn parse_term(body: &Value) -> Result<TermQuery, OpenSearchError> {
    let (field, field_body) = read_single_entry(body, "term")?;
    if field_body.is_object() {
        let options = read_leaf_object(field_body, "term", &["value", "case_insensitive"])?;
        let value = options
            .get("value")
            .ok_or_else(|| OpenSearchError::parsing("[term] needs a [value]"))?;
        return Ok(TermQuery {
            field: field.to_string(),
            value: read_scalar(value, "term")?,
            case_insensitive: read_flag(options, "case_insensitive", "term")?,
        });
    }
    Ok(TermQuery {
        field: field.to_string(),
        value: read_scalar(field_body, "term")?,
        case_insensitive: false,
    })
}

/// `terms`: one field holding a list of values, with an optional `boost` beside it.
fn parse_terms(body: &Value) -> Result<TermsQuery, OpenSearchError> {
    let options = body
        .as_object()
        .ok_or_else(|| OpenSearchError::parsing("[terms] must be an object"))?;
    let mut field_values: Option<(String, Vec<Value>)> = None;
    for (key, value) in options {
        if key == "boost" {
            require_numeric_boost(value, "terms")?;
            continue;
        }
        if field_values.is_some() {
            let fields = render_key_list(options.keys().filter(|option| *option != "boost"));
            return Err(OpenSearchError::parsing(format!(
                "[terms] names more than one field: [{fields}]"
            )));
        }
        let Value::Array(values) = value else {
            return Err(OpenSearchError::parsing(format!(
                "[terms] [{key}] must be a list of values; a terms lookup is not supported"
            )));
        };
        let values = values
            .iter()
            .map(|value| read_scalar(value, "terms"))
            .collect::<Result<Vec<Value>, _>>()?;
        field_values = Some((key.clone(), values));
    }
    let (field, values) = field_values.ok_or_else(|| OpenSearchError::parsing("[terms] names no field"))?;
    Ok(TermsQuery { field, values })
}

/// `ids`: a list of string `values`.
fn parse_ids(body: &Value) -> Result<IdsQuery, OpenSearchError> {
    let options = read_leaf_object(body, "ids", &["values"])?;
    let Some(Value::Array(values)) = options.get("values") else {
        return Err(OpenSearchError::parsing("[ids] needs a list [values]"));
    };
    let values = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| OpenSearchError::parsing("[ids] [values] holds strings"))
        })
        .collect::<Result<Vec<String>, _>>()?;
    Ok(IdsQuery { values })
}

/// A bound value: a string or a number; `null` is an open bound.
fn read_bound(value: &Value) -> Result<Option<Value>, OpenSearchError> {
    match value {
        Value::Null => Ok(None),
        Value::String(_) | Value::Number(_) => Ok(Some(value.clone())),
        Value::Bool(_) | Value::Array(_) | Value::Object(_) => {
            Err(OpenSearchError::parsing("[range] bounds are strings or numbers"))
        }
    }
}

/// The value of `include_lower` or `include_upper`.
fn read_inclusiveness(value: &Value, key: &str) -> Result<bool, OpenSearchError> {
    value
        .as_bool()
        .ok_or_else(|| OpenSearchError::parsing(format!("[range] [{key}] must be a boolean")))
}

/// Keys are applied in body order, as the legacy and current forms overlap: `gte` / `gt` /
/// `from` set the lower bound, and `include_lower` (default true) sets the inclusiveness the
/// lower bound ends up with; likewise for the upper bound.
fn parse_range(body: &Value) -> Result<RangeQuery, OpenSearchError> {
    let (field, field_body) = read_single_entry(body, "range")?;
    let options = read_leaf_object(
        field_body,
        "range",
        &[
            "gte",
            "gt",
            "lte",
            "lt",
            "from",
            "to",
            "include_lower",
            "include_upper",
            "format",
            "time_zone",
        ],
    )?;
    let (mut lower, mut upper) = (None, None);
    let (mut include_lower, mut include_upper) = (true, true);
    let (mut format, mut time_zone) = (None, None);
    for (key, value) in options {
        match key.as_str() {
            "from" => lower = read_bound(value)?,
            "gte" | "gt" => {
                lower = read_bound(value)?;
                include_lower = key == "gte";
            }
            "to" => upper = read_bound(value)?,
            "lte" | "lt" => {
                upper = read_bound(value)?;
                include_upper = key == "lte";
            }
            "include_lower" => include_lower = read_inclusiveness(value, key)?,
            "include_upper" => include_upper = read_inclusiveness(value, key)?,
            "format" => format = Some(DateFormat::parse(read_text(options, key, "range")?)?),
            "time_zone" => time_zone = Some(parse_time_zone(read_text(options, key, "range")?)?),
            // `read_leaf_object` admitted only the keys above and a numeric `boost`.
            _ => {}
        }
    }
    Ok(RangeQuery {
        field: field.to_string(),
        lower: lower.map(|value| RangeBound {
            value,
            is_inclusive: include_lower,
        }),
        upper: upper.map(|value| RangeBound {
            value,
            is_inclusive: include_upper,
        }),
        format,
        time_zone,
    })
}

/// `exists`: a string `field`.
fn parse_exists(body: &Value) -> Result<ExistsQuery, OpenSearchError> {
    let options = read_leaf_object(body, "exists", &["field"])?;
    Ok(ExistsQuery {
        field: read_text(options, "field", "exists")?.to_string(),
    })
}

/// `{field: "value"}` or `{field: {<value key>: "value", case_insensitive, boost}}`, returned as
/// `(field, value, case_insensitive)`. An object holds exactly one of `value_keys`: reading one
/// and dropping another would answer a query the client did not write.
fn parse_pattern_leaf(
    body: &Value,
    leaf: &str,
    allowed: &[&str],
    value_keys: &[&str],
) -> Result<(String, String, bool), OpenSearchError> {
    let (field, field_body) = read_single_entry(body, leaf)?;
    match field_body {
        Value::String(value) => Ok((field.to_string(), value.clone(), false)),
        Value::Object(_) => {
            let options = read_leaf_object(field_body, leaf, allowed)?;
            let mut stated_values = value_keys.iter().filter_map(|key| options.get(*key));
            let value = stated_values
                .next()
                .and_then(Value::as_str)
                .ok_or_else(|| OpenSearchError::parsing(format!("[{leaf}] needs a string value")))?;
            if stated_values.next().is_some() {
                return Err(OpenSearchError::parsing(format!(
                    "[{leaf}] takes one value but holds more than one of [{}]",
                    value_keys.join("], [")
                )));
            }
            Ok((
                field.to_string(),
                value.to_string(),
                read_flag(options, "case_insensitive", leaf)?,
            ))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) => Err(OpenSearchError::parsing(format!(
            "[{leaf}] [{field}] must be a string or an object"
        ))),
    }
}

/// `nested`: a `path`, a `query` and an optional `score_mode`.
fn parse_nested(body: &Value) -> Result<NestedQuery, OpenSearchError> {
    let options = read_leaf_object(body, "nested", &["path", "query", "score_mode"])?;
    if let Some(mode) = options.get("score_mode")
        && !mode.as_str().is_some_and(|mode| NESTED_SCORE_MODES.contains(&mode))
    {
        return Err(OpenSearchError::parsing(format!(
            "[nested] unknown [score_mode] [{mode}]"
        )));
    }
    let query = options
        .get("query")
        .ok_or_else(|| OpenSearchError::parsing("[nested] needs a [query]"))?;
    Ok(NestedQuery {
        path: read_text(options, "path", "nested")?.to_string(),
        query: Box::new(parse_query(query)?),
    })
}

/// `match`: `{field: value}` or `{field: {query, …}}`.
fn parse_match(body: &Value) -> Result<MatchQuery, OpenSearchError> {
    let (field, field_body) = read_single_entry(body, "match")?;
    if !field_body.is_object() {
        return Ok(MatchQuery {
            field: field.to_string(),
            query: read_scalar(field_body, "match")?,
            operator: Operator::Or,
            lenient: None,
            zero_terms_query: ZeroTermsQuery::None,
        });
    }
    let options = read_leaf_object(
        field_body,
        "match",
        &[
            "query",
            "operator",
            "lenient",
            "zero_terms_query",
            "fuzziness",
            "max_expansions",
            "prefix_length",
            "fuzzy_transpositions",
            "auto_generate_synonyms_phrase_query",
        ],
    )?;
    refuse_nonzero_option(options, "fuzziness", FUZZINESS_REFUSAL, "match")?;
    refuse_wrong_option_types(
        options,
        &["max_expansions", "prefix_length"],
        &["fuzzy_transpositions", "auto_generate_synonyms_phrase_query"],
        "match",
    )?;
    let query = options
        .get("query")
        .ok_or_else(|| OpenSearchError::parsing("[match] needs a [query]"))?;
    Ok(MatchQuery {
        field: field.to_string(),
        query: read_scalar(query, "match")?,
        operator: read_operator(options, "operator", "match")?,
        lenient: read_optional_flag(options, "lenient", "match")?,
        zero_terms_query: read_zero_terms_query(options)?,
    })
}

/// A phrase leaf, `{field: "text"}` or `{field: {query, slop, …}}`, as `(field, query)`.
fn parse_phrase(body: &Value, leaf: &str, allowed: &[&str]) -> Result<(String, String), OpenSearchError> {
    let (field, field_body) = read_single_entry(body, leaf)?;
    let query = if field_body.is_object() {
        let options = read_leaf_object(field_body, leaf, allowed)?;
        refuse_nonzero_option(options, "slop", SLOP_REFUSAL, leaf)?;
        refuse_wrong_option_types(options, &["max_expansions"], &[], leaf)?;
        let query = options
            .get("query")
            .ok_or_else(|| OpenSearchError::parsing(format!("[{leaf}] needs a [query]")))?;
        read_query_text(query, leaf)?
    } else {
        read_query_text(field_body, leaf)?
    };
    Ok((field.to_string(), query))
}

/// `multi_match`: `{query, fields, type, …}` with `type` `best_fields` (the default) or `phrase`.
fn parse_multi_match(body: &Value) -> Result<MultiMatchQuery, OpenSearchError> {
    let leaf = "multi_match";
    let options = read_leaf_object(
        body,
        leaf,
        &[
            "query",
            "fields",
            "type",
            "operator",
            "lenient",
            "slop",
            "fuzziness",
            "max_expansions",
            "prefix_length",
            "fuzzy_transpositions",
            "tie_breaker",
            "auto_generate_synonyms_phrase_query",
        ],
    )?;
    refuse_nonzero_option(options, "fuzziness", FUZZINESS_REFUSAL, leaf)?;
    refuse_nonzero_option(options, "slop", SLOP_REFUSAL, leaf)?;
    refuse_wrong_option_types(
        options,
        &["max_expansions", "prefix_length", "tie_breaker"],
        &["fuzzy_transpositions", "auto_generate_synonyms_phrase_query"],
        leaf,
    )?;
    let match_type = match options.get("type") {
        None => MultiMatchType::BestFields,
        Some(Value::String(name)) if name == "best_fields" => MultiMatchType::BestFields,
        Some(Value::String(name)) if name == "phrase" => MultiMatchType::Phrase,
        Some(other) => {
            return Err(OpenSearchError::parsing(format!(
                "[multi_match] [type] {other} is not supported: best_fields and phrase are"
            )));
        }
    };
    let query = options
        .get("query")
        .ok_or_else(|| OpenSearchError::parsing("[multi_match] needs a [query]"))?;
    Ok(MultiMatchQuery {
        fields: read_field_patterns(options, leaf)?,
        query: read_query_text(query, leaf)?,
        match_type,
        operator: read_operator(options, "operator", leaf)?,
        lenient: read_optional_flag(options, "lenient", leaf)?,
    })
}

/// `simple_query_string`, its query parsed here so that every document reuses the tree.
fn parse_simple_query_string_leaf(body: &Value) -> Result<SimpleQueryStringQuery, OpenSearchError> {
    let leaf = "simple_query_string";
    let options = read_leaf_object(body, leaf, &["query", "fields", "default_operator", "flags", "lenient"])?;
    let flags = match options.get("flags") {
        None => SimpleQueryFlags::ALL,
        Some(Value::String(names)) => SimpleQueryFlags::parse(names)?,
        Some(_) => {
            return Err(OpenSearchError::parsing(
                "[simple_query_string] [flags] is a string of flag names joined by |",
            ));
        }
    };
    let default_operator = read_operator(options, "default_operator", leaf)?;
    Ok(SimpleQueryStringQuery {
        query: parse_simple_query_string(read_text(options, "query", leaf)?, flags, default_operator)?,
        fields: read_field_patterns(options, leaf)?,
        default_operator,
        lenient: read_optional_flag(options, "lenient", leaf)?,
    })
}

/// A query text: a string as it is, a number or a boolean in its JSON spelling.
fn read_query_text(value: &Value, leaf: &str) -> Result<String, OpenSearchError> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(_) | Value::Bool(_) => Ok(value.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => Err(OpenSearchError::parsing(format!(
            "[{leaf}] takes a string, number or boolean query"
        ))),
    }
}

/// `fields`: field patterns without their `^boost`; absent or empty is every field.
fn read_field_patterns(options: &Map<String, Value>, leaf: &str) -> Result<Vec<String>, OpenSearchError> {
    let patterns: Vec<&str> = match options.get("fields") {
        None => Vec::new(),
        Some(Value::String(pattern)) => vec![pattern.as_str()],
        Some(Value::Array(patterns)) => patterns
            .iter()
            .map(|pattern| {
                pattern
                    .as_str()
                    .ok_or_else(|| OpenSearchError::parsing(format!("[{leaf}] [fields] holds strings")))
            })
            .collect::<Result<_, _>>()?,
        Some(_) => {
            return Err(OpenSearchError::parsing(format!(
                "[{leaf}] [fields] is a list of fields"
            )));
        }
    };
    if patterns.is_empty() {
        return Ok(vec![ALL_FIELDS.to_string()]);
    }
    patterns.into_iter().map(|pattern| strip_field_boost(pattern, leaf)).collect()
}

/// `title^3` → `title`; the boost must be a number and is otherwise ignored.
fn strip_field_boost(pattern: &str, leaf: &str) -> Result<String, OpenSearchError> {
    match pattern.rsplit_once('^') {
        None => Ok(pattern.to_string()),
        Some((field, boost)) if boost.parse::<f64>().is_ok_and(f64::is_finite) => Ok(field.to_string()),
        Some(_) => Err(OpenSearchError::parsing(format!(
            "[{leaf}] field [{pattern}] has a malformed boost"
        ))),
    }
}

/// `or` (the default) or `and`, in any case.
fn read_operator(options: &Map<String, Value>, key: &str, leaf: &str) -> Result<Operator, OpenSearchError> {
    match options.get(key) {
        None => Ok(Operator::Or),
        Some(Value::String(name)) if name.eq_ignore_ascii_case("or") => Ok(Operator::Or),
        Some(Value::String(name)) if name.eq_ignore_ascii_case("and") => Ok(Operator::And),
        Some(other) => Err(OpenSearchError::parsing(format!(
            "[{leaf}] [{key}] {other} is neither or nor and"
        ))),
    }
}

/// The boolean option `key`, `None` when it is absent.
fn read_optional_flag(options: &Map<String, Value>, key: &str, leaf: &str) -> Result<Option<bool>, OpenSearchError> {
    options.get(key).map_or(Ok(None), |value| {
        value
            .as_bool()
            .map(Some)
            .ok_or_else(|| OpenSearchError::parsing(format!("[{leaf}] [{key}] must be a boolean")))
    })
}

/// `none` (the default) or `all`, in any case.
fn read_zero_terms_query(options: &Map<String, Value>) -> Result<ZeroTermsQuery, OpenSearchError> {
    match options.get("zero_terms_query") {
        None => Ok(ZeroTermsQuery::None),
        Some(Value::String(name)) if name.eq_ignore_ascii_case("none") => Ok(ZeroTermsQuery::None),
        Some(Value::String(name)) if name.eq_ignore_ascii_case("all") => Ok(ZeroTermsQuery::All),
        Some(other) => Err(OpenSearchError::parsing(format!(
            "[match] [zero_terms_query] {other} is neither none nor all"
        ))),
    }
}

/// Zero as a JSON number or a string.
fn is_zero(value: &Value) -> bool {
    value.as_u64() == Some(0) || value.as_str() == Some("0")
}

/// Refuses the option `key` unless it is absent or zero, which is all IceGate accepts of an option
/// asking for inexact matching: `reason` says what it does not evaluate.
fn refuse_nonzero_option(
    options: &Map<String, Value>,
    key: &str,
    reason: &str,
    leaf: &str,
) -> Result<(), OpenSearchError> {
    match options.get(key) {
        Some(value) if !is_zero(value) => Err(OpenSearchError::unsupported_construct(
            key,
            format!("[{leaf}] [{key}] {value} is not supported: {reason}"),
        )),
        Some(_) | None => Ok(()),
    }
}

/// Options accepted and not applied still have to carry their type: `number_keys` must hold JSON
/// numbers and `boolean_keys` JSON booleans.
fn refuse_wrong_option_types(
    options: &Map<String, Value>,
    number_keys: &[&str],
    boolean_keys: &[&str],
    leaf: &str,
) -> Result<(), OpenSearchError> {
    for key in number_keys {
        if options.get(*key).is_some_and(|value| !value.is_number()) {
            return Err(OpenSearchError::parsing(format!("[{leaf}] [{key}] must be a number")));
        }
    }
    for key in boolean_keys {
        read_optional_flag(options, key, leaf)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! A bounded case matrix: every leaf's accepted forms and one refusal per refusal branch.
    //! The JSON shape of each leaf is fixed and its key set closed, so the matrix enumerates the
    //! input space rather than sampling it.

    use axum::http::StatusCode;
    use chrono::FixedOffset;
    use serde_json::{Value, json};

    use super::*;
    use crate::opensearch::error::render_envelope;
    use crate::query_dsl::ast::{
        BoolQuery, ExistsQuery, IdsQuery, MatchPhrasePrefixQuery, MatchPhraseQuery, MatchQuery, MultiMatchQuery,
        MultiMatchType, NestedQuery, Operator, PrefixQuery, RangeBound, RangeQuery, SimpleQueryStringQuery, TermQuery,
        TermsQuery, WildcardQuery, ZeroTermsQuery,
    };
    use crate::query_dsl::date_math::{DateFormat, TimeZoneSpec};
    use crate::query_dsl::simple_query::SimpleQuery;

    fn parse(value: impl Into<Value>) -> Query {
        parse_query(&value.into()).expect("the query parses")
    }

    async fn render_refusal(value: Value) -> (StatusCode, Value) {
        render_envelope(parse_query(&value).expect_err("the query is refused")).await
    }

    fn build_term(field: &str, value: Value) -> Query {
        Query::Term(TermQuery {
            field: field.to_string(),
            value,
            case_insensitive: false,
        })
    }

    #[test]
    fn bool_clauses_parse_as_an_object_or_an_array() {
        let as_objects = parse(json!({"bool": {
            "must": {"term": {"type": "dashboard"}},
            "filter": {"exists": {"field": "namespace"}},
            "should": {"term": {"type": "config"}},
            "must_not": {"term": {"type": "space"}}
        }}));
        let as_arrays = parse(json!({"bool": {
            "must": [{"term": {"type": "dashboard"}}],
            "filter": [{"exists": {"field": "namespace"}}],
            "should": [{"term": {"type": "config"}}],
            "must_not": [{"term": {"type": "space"}}],
            "minimum_should_match": "1",
            "boost": 2.0
        }}));

        let expected = BoolQuery {
            must: vec![build_term("type", json!("dashboard"))],
            filter: vec![Query::Exists(ExistsQuery {
                field: "namespace".to_string(),
            })],
            should: vec![build_term("type", json!("config"))],
            must_not: vec![build_term("type", json!("space"))],
            minimum_should_match: None,
        };
        assert_eq!(as_objects, Query::Bool(expected.clone()));
        assert_eq!(
            as_arrays,
            Query::Bool(BoolQuery {
                minimum_should_match: Some(1),
                ..expected
            })
        );
    }

    #[test]
    fn an_empty_bool_and_the_match_all_leaves_parse_into_their_nodes() {
        assert_eq!(parse(json!({"bool": {}})), Query::Bool(BoolQuery::default()));
        assert_eq!(parse(json!({"match_all": {}})), Query::MatchAll);
        assert_eq!(parse(json!({"match_all": {"boost": 1}})), Query::MatchAll);
        assert_eq!(parse(json!({"match_none": {}})), Query::MatchNone);
    }

    #[test]
    fn term_parses_its_short_and_long_forms_with_boost() {
        assert_eq!(
            parse(json!({"term": {"type": "dashboard"}})),
            build_term("type", json!("dashboard"))
        );
        assert_eq!(parse(json!({"term": {"count": 0}})), build_term("count", json!(0)));
        assert_eq!(
            parse(json!({"term": {"enabled": false}})),
            build_term("enabled", json!(false))
        );
        assert_eq!(
            parse(json!({"term": {"type": {"value": "dashboard", "boost": 1.5}}})),
            build_term("type", json!("dashboard"))
        );
        assert_eq!(
            parse(json!({"term": {"type": {"value": "Dashboard", "case_insensitive": true}}})),
            Query::Term(TermQuery {
                field: "type".to_string(),
                value: json!("Dashboard"),
                case_insensitive: true
            })
        );
    }

    #[test]
    fn range_parses_legacy_from_to_and_include_keys_and_digit_string_bounds() {
        let comparison_range = parse(json!({"range": {"count": {"gt": 1, "lte": "10", "boost": 1}}}));
        let legacy_range =
            parse(json!({"range": {"count": {"from": "1", "to": 10, "include_lower": false, "include_upper": true}}}));
        let open_range = parse(json!({"range": {"count": {"gte": null, "lt": 5}}}));

        let build_bounded_range = |lower: Value, upper: Value| {
            Query::Range(RangeQuery {
                field: "count".to_string(),
                lower: Some(RangeBound {
                    value: lower,
                    is_inclusive: false,
                }),
                upper: Some(RangeBound {
                    value: upper,
                    is_inclusive: true,
                }),
                format: None,
                time_zone: None,
            })
        };
        assert_eq!(comparison_range, build_bounded_range(json!(1), json!("10")));
        assert_eq!(legacy_range, build_bounded_range(json!("1"), json!(10)));
        assert_eq!(
            open_range,
            Query::Range(RangeQuery {
                field: "count".to_string(),
                lower: None,
                upper: Some(RangeBound {
                    value: json!(5),
                    is_inclusive: false
                }),
                format: None,
                time_zone: None,
            })
        );
    }

    #[test]
    fn range_keys_apply_in_body_order() {
        let exclusive_then_included = parse(json!({"range": {"count": {"gt": 1, "include_lower": true}}}));
        let included_then_exclusive = parse(json!({"range": {"count": {"include_lower": true, "gt": 1}}}));

        let with_lower_inclusiveness = |is_inclusive: bool| {
            Query::Range(RangeQuery {
                field: "count".to_string(),
                lower: Some(RangeBound {
                    value: json!(1),
                    is_inclusive,
                }),
                upper: None,
                format: None,
                time_zone: None,
            })
        };
        assert_eq!(exclusive_then_included, with_lower_inclusiveness(true));
        assert_eq!(included_then_exclusive, with_lower_inclusiveness(false));
    }

    #[test]
    fn range_captures_format_and_time_zone() {
        let query = parse(json!({"range": {"updated_at": {
            "gte": "now-90d/d",
            "lte": "1791462896789",
            "format": "epoch_millis",
            "time_zone": "+05:30"
        }}}));
        let named_zone_range = parse(json!({"range": {"updated_at": {
            "lt": "2026-10-08",
            "format": "strict_date_optional_time",
            "time_zone": "Europe/Berlin"
        }}}));
        let date_time_range =
            parse(json!({"range": {"updated_at": {"lt": "2026-10-08T00:00:00.000Z", "format": "date_time"}}}));

        assert_eq!(
            query,
            Query::Range(RangeQuery {
                field: "updated_at".to_string(),
                lower: Some(RangeBound {
                    value: json!("now-90d/d"),
                    is_inclusive: true
                }),
                upper: Some(RangeBound {
                    value: json!("1791462896789"),
                    is_inclusive: true
                }),
                format: Some(DateFormat::EpochMillis),
                time_zone: Some(TimeZoneSpec::Fixed(FixedOffset::east_opt(19_800).expect("offset"))),
            })
        );
        let Query::Range(named_zone_range) = named_zone_range else {
            panic!("a range")
        };
        assert_eq!(named_zone_range.format, Some(DateFormat::StrictDateOptionalTime));
        assert_eq!(
            named_zone_range.time_zone,
            Some(TimeZoneSpec::Named(chrono_tz::Europe::Berlin))
        );
        let Query::Range(date_time_range) = date_time_range else {
            panic!("a range")
        };
        assert_eq!(date_time_range.format, Some(DateFormat::DateTime));
    }

    #[test]
    fn the_remaining_leaves_parse_into_their_nodes() {
        assert_eq!(
            parse(json!({"terms": {"type": ["dashboard", "config"], "boost": 1}})),
            Query::Terms(TermsQuery {
                field: "type".to_string(),
                values: vec![json!("dashboard"), json!("config")]
            })
        );
        assert_eq!(
            parse(json!({"ids": {"values": ["dashboard:a", "config:3.9.0"]}})),
            Query::Ids(IdsQuery {
                values: vec!["dashboard:a".to_string(), "config:3.9.0".to_string()]
            })
        );
        assert_eq!(
            parse(json!({"prefix": {"type": "dash"}})),
            Query::Prefix(PrefixQuery {
                field: "type".to_string(),
                value: "dash".to_string(),
                case_insensitive: false
            })
        );
        assert_eq!(
            parse(json!({"prefix": {"type": {"value": "DASH", "case_insensitive": true, "boost": 1}}})),
            Query::Prefix(PrefixQuery {
                field: "type".to_string(),
                value: "DASH".to_string(),
                case_insensitive: true
            })
        );
        assert_eq!(
            parse(json!({"wildcard": {"type": {"wildcard": "dash*"}}})),
            Query::Wildcard(WildcardQuery {
                field: "type".to_string(),
                pattern: "dash*".to_string(),
                case_insensitive: false
            })
        );
        assert_eq!(
            parse(json!({"constant_score": {"filter": {"term": {"type": "dashboard"}}, "boost": 1}})),
            Query::ConstantScore(Box::new(build_term("type", json!("dashboard"))))
        );
        assert_eq!(
            parse(json!({"nested": {
                "path": "references",
                "query": {"term": {"references.id": "1"}},
                "score_mode": "none"
            }})),
            Query::Nested(NestedQuery {
                path: "references".to_string(),
                query: Box::new(build_term("references.id", json!("1")))
            })
        );
        assert_eq!(
            parse(json!({"bool": {"should": [{"match_all": {}}], "minimum_should_match": 2}})),
            Query::Bool(BoolQuery {
                should: vec![Query::MatchAll],
                minimum_should_match: Some(2),
                ..BoolQuery::default()
            })
        );
        for minimum_should_match in [json!(4_294_967_295_u32), json!("4294967295")] {
            assert_eq!(
                parse(json!({"bool": {"minimum_should_match": minimum_should_match}})),
                Query::Bool(BoolQuery {
                    minimum_should_match: Some(u32::MAX),
                    ..BoolQuery::default()
                })
            );
        }
    }

    #[tokio::test]
    async fn an_unknown_leaf_is_a_parsing_exception_naming_it() {
        for leaf in ["script", "geo_shape", "more_like_this", "percolate"] {
            let (status, body) = render_refusal(json!({leaf: {}})).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{leaf}");
            assert_eq!(body["error"]["type"], "parsing_exception", "{leaf}");
            let reason = body["error"]["reason"].as_str().expect("reason");
            assert!(reason.contains(leaf), "the refusal names [{leaf}]: {reason}");
        }
        for malformed_query in [
            json!({}),
            json!({"term": {"a": 1}, "exists": {"field": "a"}}),
            json!("match_all"),
            json!([]),
        ] {
            let (status, body) = render_refusal(malformed_query.clone()).await;
            assert_eq!(
                (status, body["error"]["type"].clone()),
                (StatusCode::BAD_REQUEST, json!("parsing_exception")),
                "{malformed_query}"
            );
        }
    }

    #[tokio::test]
    async fn an_unknown_key_inside_a_known_leaf_is_a_parsing_exception() {
        let refused_queries = [
            json!({"bool": {"must": [], "adjust_pure_negative": true}}),
            json!({"term": {"type": {"value": "dashboard", "rewrite": "constant_score"}}}),
            json!({"term": {"type": "dashboard", "namespace": "default"}}),
            json!({"term": {"type": ["dashboard"]}}),
            json!({"range": {"count": {"gte": 1, "relation": "within"}}}),
            json!({"range": {"updated_at": {"gte": "now", "format": "yyyy-MM-dd"}}}),
            json!({"range": {"updated_at": {"gte": "now", "time_zone": "Mars/Base"}}}),
            json!({"exists": {"field": "type", "unknown": 1}}),
            json!({"bool": {"minimum_should_match": "50%"}}),
            json!({"bool": {"minimum_should_match": -1}}),
            json!({"term": {"type": {"value": "dashboard", "boost": "high"}}}),
            json!({"terms": {"type": {"index": "lookup", "id": "1", "path": "types"}}}),
            json!({"nested": {"path": "references", "query": {"match_all": {}}, "ignore_unmapped": true}}),
        ];
        for query in refused_queries {
            let (status, body) = render_refusal(query.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
            assert_eq!(body["error"]["type"], "parsing_exception", "{query}");
        }
    }

    #[tokio::test]
    async fn a_leaf_name_key_is_refused() {
        for query in [
            json!({"term": {"type": {"value": "dashboard", "_name": "by-type"}}}),
            json!({"bool": {"must": [], "_name": "outer"}}),
            json!({"exists": {"field": "type", "_name": "has-type"}}),
            // Beside the field, where a leaf holding one field reads it as a second entry.
            json!({"term": {"type": "dashboard", "_name": "by-type"}}),
            json!({"terms": {"type": ["dashboard"], "_name": "by-type"}}),
            json!({"range": {"count": {"gte": 1}, "boost": 1, "_name": "by-count"}}),
            json!({"prefix": {"type": "dash", "_name": "by-type"}}),
            json!({"wildcard": {"type": "dash*", "_name": "by-type"}}),
        ] {
            let (status, body) = render_refusal(query.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
            assert_eq!(body["error"]["type"], "parsing_exception", "{query}");
            assert!(
                body["error"]["reason"].as_str().expect("reason").contains("_name"),
                "{body}"
            );
        }
    }

    #[tokio::test]
    async fn a_missing_or_mistyped_leaf_value_is_a_parsing_exception() {
        let refused_queries = [
            json!({"constant_score": {}}),
            json!({"nested": {"path": "references"}}),
            json!({"nested": {"query": {"match_all": {}}}}),
            json!({"nested": {"path": "references", "query": {"match_all": {}}, "score_mode": "first"}}),
            json!({"ids": {"values": "a"}}),
            json!({"ids": {"values": [1]}}),
            json!({"terms": {"type": ["a"], "title": ["b"]}}),
            json!({"terms": {"boost": 1}}),
            json!({"terms": {"type": ["a"], "boost": "x"}}),
            json!({"terms": 1}),
            json!({"range": {"count": {"gte": true}}}),
            json!({"range": {"count": {"from": 1, "include_lower": "no"}}}),
            json!({"range": {"t": {"gte": "now", "format": 1}}}),
            json!({"exists": {}}),
            json!({"exists": {"field": 1}}),
            json!({"prefix": {"type": 1}}),
            json!({"wildcard": {"type": {"value": 1}}}),
            json!({"prefix": {"type": {"value": "a", "case_insensitive": "yes"}}}),
            json!({"term": {"type": null}}),
            json!({"term": {"type": {"boost": 1}}}),
            json!({"bool": {"must": 1}}),
            json!({"match_all": 1}),
            // One past the largest count, as a number and as a string.
            json!({"bool": {"minimum_should_match": 4_294_967_296_u64}}),
            json!({"bool": {"minimum_should_match": "4294967296"}}),
        ];
        for query in refused_queries {
            let (status, body) = render_refusal(query.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
            assert_eq!(body["error"]["type"], "parsing_exception", "{query}");
        }

        let (status, body) = render_refusal(json!({"wildcard": {"type": {"value": "a*", "wildcard": "b*"}}})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
        let reason = body["error"]["reason"].as_str().expect("reason");
        assert!(
            reason.contains("[value], [wildcard]"),
            "the refusal names both keys: {reason}"
        );
    }

    #[test]
    fn the_full_text_leaves_parse_into_their_nodes() {
        assert_eq!(
            parse(json!({"match": {"title": "error rate"}})),
            Query::Match(MatchQuery {
                field: "title".to_string(),
                query: json!("error rate"),
                operator: Operator::Or,
                lenient: None,
                zero_terms_query: ZeroTermsQuery::None,
            })
        );
        assert_eq!(
            parse(
                json!({"match": {"count": {"query": 0, "operator": "AND", "lenient": true, "zero_terms_query": "all", "fuzziness": "0", "max_expansions": 50, "boost": 2}}})
            ),
            Query::Match(MatchQuery {
                field: "count".to_string(),
                query: json!(0),
                operator: Operator::And,
                lenient: Some(true),
                zero_terms_query: ZeroTermsQuery::All,
            })
        );
        assert_eq!(
            parse(json!({"match": {"title": {"query": "error", "operator": "or", "zero_terms_query": "NONE"}}})),
            Query::Match(MatchQuery {
                field: "title".to_string(),
                query: json!("error"),
                operator: Operator::Or,
                lenient: None,
                zero_terms_query: ZeroTermsQuery::None,
            })
        );
        assert_eq!(
            parse(json!({"match_phrase": {"title": {"query": "error rate", "slop": 0}}})),
            Query::MatchPhrase(MatchPhraseQuery {
                field: "title".to_string(),
                query: "error rate".to_string()
            })
        );
        assert_eq!(
            parse(json!({"match_phrase": {"title": "error rate"}})),
            Query::MatchPhrase(MatchPhraseQuery {
                field: "title".to_string(),
                query: "error rate".to_string()
            })
        );
        assert_eq!(
            parse(json!({"match_phrase": {"count": 0}})),
            Query::MatchPhrase(MatchPhraseQuery {
                field: "count".to_string(),
                query: "0".to_string()
            })
        );
        assert_eq!(
            parse(json!({"match_phrase": {"enabled": {"query": true}}})),
            Query::MatchPhrase(MatchPhraseQuery {
                field: "enabled".to_string(),
                query: "true".to_string()
            })
        );
        assert_eq!(
            parse(json!({"match_phrase_prefix": {"title": {"query": "err", "max_expansions": 10}}})),
            Query::MatchPhrasePrefix(MatchPhrasePrefixQuery {
                field: "title".to_string(),
                query: "err".to_string()
            })
        );
        assert_eq!(
            parse(
                json!({"multi_match": {"query": "error", "fields": ["title^3", "description"], "type": "phrase", "tie_breaker": 0.3}})
            ),
            Query::MultiMatch(MultiMatchQuery {
                fields: vec!["title".to_string(), "description".to_string()],
                query: "error".to_string(),
                match_type: MultiMatchType::Phrase,
                operator: Operator::Or,
                lenient: None,
            })
        );
        assert_eq!(
            parse(json!({"multi_match": {"query": "error"}})),
            Query::MultiMatch(MultiMatchQuery {
                fields: vec!["*".to_string()],
                query: "error".to_string(),
                match_type: MultiMatchType::BestFields,
                operator: Operator::Or,
                lenient: None,
            })
        );
        assert_eq!(
            parse(
                json!({"multi_match": {"query": 7, "fields": "title^1.5", "type": "best_fields", "operator": "and", "lenient": false}})
            ),
            Query::MultiMatch(MultiMatchQuery {
                fields: vec!["title".to_string()],
                query: "7".to_string(),
                match_type: MultiMatchType::BestFields,
                operator: Operator::And,
                lenient: Some(false),
            })
        );
        assert_eq!(
            parse(
                json!({"simple_query_string": {"query": "error* -rate", "fields": ["visualization.title"], "default_operator": "and", "flags": "PREFIX|NOT|WHITESPACE", "lenient": true}})
            ),
            Query::SimpleQueryString(SimpleQueryStringQuery {
                query: SimpleQuery::Bool {
                    must: vec![
                        SimpleQuery::Term {
                            text: "error".to_string(),
                            is_prefix: true
                        },
                        SimpleQuery::Bool {
                            must: Vec::new(),
                            should: vec![SimpleQuery::MatchAll],
                            must_not: vec![SimpleQuery::Term {
                                text: "rate".to_string(),
                                is_prefix: false
                            }],
                        },
                    ],
                    should: Vec::new(),
                    must_not: Vec::new(),
                },
                fields: vec!["visualization.title".to_string()],
                default_operator: Operator::And,
                lenient: Some(true),
            })
        );
        assert_eq!(
            parse(json!({"match_phrase_prefix": {"index-pattern.title": {"query": "", "boost": 1}}})),
            Query::MatchPhrasePrefix(MatchPhrasePrefixQuery {
                field: "index-pattern.title".to_string(),
                query: String::new()
            })
        );
        assert_eq!(
            parse(json!({"simple_query_string": {"query": "error", "fields": []}})),
            Query::SimpleQueryString(SimpleQueryStringQuery {
                query: SimpleQuery::Term {
                    text: "error".to_string(),
                    is_prefix: false
                },
                fields: vec!["*".to_string()],
                default_operator: Operator::Or,
                lenient: None,
            })
        );
    }

    #[tokio::test]
    async fn fuzziness_and_slop_above_zero_are_refused_naming_the_construct() {
        for (query, construct) in [
            (
                json!({"match": {"title": {"query": "error", "fuzziness": "AUTO"}}}),
                "fuzziness",
            ),
            (
                json!({"match": {"title": {"query": "error", "fuzziness": 1}}}),
                "fuzziness",
            ),
            (
                json!({"match_phrase": {"title": {"query": "error rate", "slop": 2}}}),
                "slop",
            ),
            (
                json!({"match_phrase_prefix": {"title": {"query": "error ra", "slop": 1}}}),
                "slop",
            ),
            (json!({"multi_match": {"query": "error", "fuzziness": 2}}), "fuzziness"),
            (
                json!({"multi_match": {"query": "error rate", "type": "phrase", "slop": 1}}),
                "slop",
            ),
            (json!({"simple_query_string": {"query": "eror~1"}}), "fuzzy"),
            (json!({"simple_query_string": {"query": "\"error rate\"~2"}}), "slop"),
        ] {
            let (status, body) = render_refusal(query.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
            assert_eq!(body["error"]["type"], "parsing_exception", "{query}");
            assert_eq!(body["error"]["construct"], construct, "{query}");
        }
        for accepted_query in [
            json!({"match": {"title": {"query": "error", "fuzziness": 0}}}),
            json!({"match": {"title": {"query": "error", "fuzziness": "0"}}}),
            json!({"match_phrase": {"title": {"query": "error rate", "slop": 0}}}),
            json!({"multi_match": {"query": "error rate", "fuzziness": "0", "slop": 0}}),
            json!({"simple_query_string": {"query": "eror~0 \"error rate\"~0"}}),
        ] {
            assert!(parse_query(&accepted_query).is_ok(), "{accepted_query}");
        }
    }

    #[tokio::test]
    async fn unsupported_full_text_options_are_parsing_exceptions() {
        for unsupported_query in [
            json!({"multi_match": {"query": "error", "type": "most_fields"}}),
            json!({"multi_match": {"query": "error", "type": 1}}),
            json!({"match": {"title": {"query": "error", "analyzer": "standard"}}}),
            json!({"simple_query_string": {"query": "error", "flags": "AND|SPELLING"}}),
            json!({"simple_query_string": {"query": "error", "flags": 1}}),
            json!({"simple_query_string": {"query": "error", "quote_field_suffix": ".exact"}}),
            json!({"match": {"title": {"query": "error", "_name": "named"}}}),
            json!({"multi_match": {"query": "error", "fields": [1]}}),
            json!({"multi_match": {"query": "error", "fields": 1}}),
            json!({"simple_query_string": {"query": "error", "fields": ["title^x"]}}),
            json!({"simple_query_string": {"query": "error", "fields": ["title^inf"]}}),
            json!({"match": {"title": {"query": "error", "operator": "xor"}}}),
            json!({"match": {"title": {"query": "error", "operator": 1}}}),
            json!({"simple_query_string": {"query": "error", "default_operator": "xor"}}),
            json!({"match": {"title": {"query": "error", "lenient": "yes"}}}),
            json!({"match": {"title": {"query": "error", "zero_terms_query": "some"}}}),
            json!({"match": {"title": {"query": "error", "max_expansions": "10"}}}),
            json!({"match": {"title": {"query": "error", "fuzzy_transpositions": "true"}}}),
            json!({"match_phrase_prefix": {"title": {"query": "error", "max_expansions": "10"}}}),
            json!({"multi_match": {"query": "error", "tie_breaker": "0.3"}}),
            json!({"multi_match": {"query": "error", "auto_generate_synonyms_phrase_query": "true"}}),
            json!({"match": {"title": {"operator": "and"}}}),
            json!({"match_phrase": {"title": {"boost": 1}}}),
            json!({"multi_match": {"fields": ["title"]}}),
            json!({"simple_query_string": {"fields": ["title"]}}),
            json!({"match": {"title": null}}),
            json!({"match": {"title": ["error"]}}),
            json!({"match": {"title": {"query": null}}}),
            json!({"match_phrase": {"title": null}}),
            json!({"match_phrase": {"title": {"query": null}}}),
            json!({"multi_match": {"query": null}}),
            json!({"simple_query_string": {"query": null}}),
            json!({"simple_query_string": {"query": 1}}),
            json!({"simple_query_string": 1}),
        ] {
            let (status, body) = render_refusal(unsupported_query.clone()).await;
            assert_eq!(
                (status, body["error"]["type"].clone()),
                (StatusCode::BAD_REQUEST, json!("parsing_exception")),
                "{unsupported_query}"
            );
        }
    }
}
