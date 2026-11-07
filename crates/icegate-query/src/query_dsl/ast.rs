//! The Query DSL tree. Nodes hold the request's values as JSON (`serde_json::Value`): a value
//! is typed only once the evaluator knows the field it is compared with.

use serde_json::Value;

use super::date_math::{DateFormat, TimeZoneSpec};
use super::simple_query::SimpleQuery;

/// One query node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Query {
    /// Every document.
    MatchAll,
    /// No document.
    MatchNone,
    /// A boolean combination.
    Bool(BoolQuery),
    /// One exact value.
    Term(TermQuery),
    /// Any of several exact values.
    Terms(TermsQuery),
    /// Documents by `_id`.
    Ids(IdsQuery),
    /// A value interval.
    Range(RangeQuery),
    /// The field holds a value.
    Exists(ExistsQuery),
    /// A value starting with a prefix.
    Prefix(PrefixQuery),
    /// A value matching a wildcard pattern.
    Wildcard(WildcardQuery),
    /// The inner query, with its score ignored.
    ConstantScore(Box<Self>),
    /// The inner query, evaluated within one element of a nested array.
    Nested(NestedQuery),
    /// `match`.
    Match(MatchQuery),
    /// `match_phrase`.
    MatchPhrase(MatchPhraseQuery),
    /// `match_phrase_prefix`.
    MatchPhrasePrefix(MatchPhrasePrefixQuery),
    /// `multi_match`.
    MultiMatch(MultiMatchQuery),
    /// `simple_query_string`.
    SimpleQueryString(SimpleQueryStringQuery),
}

/// `bool`: every `must` and `filter` clause matches, no `must_not` clause matches, and at least
/// `minimum_should_match` `should` clauses match. Without `minimum_should_match` that is one
/// when the query has `should` clauses and neither `must` nor `filter`, and none otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct BoolQuery {
    /// Clauses that must match.
    pub(crate) must: Vec<Query>,
    /// Clauses that must match (no scoring difference on IceGate).
    pub(crate) filter: Vec<Query>,
    /// Clauses counted against `minimum_should_match`.
    pub(crate) should: Vec<Query>,
    /// Clauses that must not match.
    pub(crate) must_not: Vec<Query>,
    /// How many `should` clauses must match.
    pub(crate) minimum_should_match: Option<u32>,
}

/// `term`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TermQuery {
    /// The field.
    pub(crate) field: String,
    /// The value: a JSON string, number or boolean.
    pub(crate) value: Value,
    /// Fold ASCII letter case when comparing strings.
    pub(crate) case_insensitive: bool,
}

/// `terms`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TermsQuery {
    /// The field.
    pub(crate) field: String,
    /// The values; a document matches when it holds any of them.
    pub(crate) values: Vec<Value>,
}

/// `ids`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdsQuery {
    /// The `_id`s to match.
    pub(crate) values: Vec<String>,
}

/// `range`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RangeQuery {
    /// The field.
    pub(crate) field: String,
    /// The lower bound; `None` is open.
    pub(crate) lower: Option<RangeBound>,
    /// The upper bound; `None` is open.
    pub(crate) upper: Option<RangeBound>,
    /// How date bounds are written; `None` accepts a date or epoch milliseconds.
    pub(crate) format: Option<DateFormat>,
    /// The zone of date bounds that carry no offset, and of their rounding.
    pub(crate) time_zone: Option<TimeZoneSpec>,
}

/// One end of a range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RangeBound {
    /// The bound as the request wrote it: a JSON string or number.
    pub(crate) value: Value,
    /// Whether the bound itself matches.
    pub(crate) is_inclusive: bool,
}

/// `exists`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExistsQuery {
    /// The field.
    pub(crate) field: String,
}

/// `prefix`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrefixQuery {
    /// The field.
    pub(crate) field: String,
    /// The prefix.
    pub(crate) value: String,
    /// Fold ASCII letter case.
    pub(crate) case_insensitive: bool,
}

/// `wildcard`: `*` matches any run, `?` one character, `\` escapes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WildcardQuery {
    /// The field.
    pub(crate) field: String,
    /// The pattern.
    pub(crate) pattern: String,
    /// Fold ASCII letter case.
    pub(crate) case_insensitive: bool,
}

/// `nested`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NestedQuery {
    /// The path of the nested array.
    pub(crate) path: String,
    /// The query every clause of which must hold within the same array element.
    pub(crate) query: Box<Query>,
}

/// How the tokens of a full-text query combine, and how unconnected `simple_query_string`
/// clauses join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operator {
    /// One token or clause suffices.
    Or,
    /// Every token or clause must match.
    And,
}

/// The field pattern naming every field: the default of `multi_match` and
/// `simple_query_string`.
pub(crate) const ALL_FIELDS: &str = "*";

/// What a `match` whose text yields no token matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ZeroTermsQuery {
    /// No document (the default).
    None,
    /// Every document.
    All,
}

/// How `multi_match` matches; either way one field must satisfy the whole query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MultiMatchType {
    /// The query's tokens combined with its operator.
    BestFields,
    /// The query as a phrase.
    Phrase,
}

/// `match`: the query text analyzed by the field's kind and combined with `operator`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MatchQuery {
    pub(crate) field: String,
    /// A string, number or boolean.
    pub(crate) query: Value,
    pub(crate) operator: Operator,
    /// Whether a text that does not parse for the field's type matches nothing instead of
    /// failing; `None` is `false`.
    pub(crate) lenient: Option<bool>,
    pub(crate) zero_terms_query: ZeroTermsQuery,
}

/// `match_phrase`: the query's tokens at consecutive positions of one value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MatchPhraseQuery {
    pub(crate) field: String,
    pub(crate) query: String,
}

/// `match_phrase_prefix`: a phrase whose last token prefixes its token. Every token with that
/// prefix counts: `max_expansions` is accepted and not applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MatchPhrasePrefixQuery {
    pub(crate) field: String,
    /// The phrase, its last token a prefix.
    pub(crate) query: String,
}

/// `multi_match` over field patterns (already stripped of `^boost`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MultiMatchQuery {
    pub(crate) fields: Vec<String>,
    pub(crate) query: String,
    pub(crate) match_type: MultiMatchType,
    /// How the query's tokens combine under [`MultiMatchType::BestFields`].
    pub(crate) operator: Operator,
    /// `None` defaults to `true` for the fields `*` and to `false` otherwise.
    pub(crate) lenient: Option<bool>,
}

/// `simple_query_string`, its query parsed with the request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SimpleQueryStringQuery {
    /// The parsed query; every document reuses this tree.
    pub(crate) query: SimpleQuery,
    /// Field patterns (already stripped of `^boost`).
    pub(crate) fields: Vec<String>,
    /// Joins the tokens one term analyzes into; the clauses were joined when parsing.
    pub(crate) default_operator: Operator,
    /// `None` defaults to `true` for the fields `*` and to `false` otherwise.
    pub(crate) lenient: Option<bool>,
}
