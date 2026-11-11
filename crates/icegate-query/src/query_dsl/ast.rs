//! The Query DSL tree. Nodes hold the request's values as JSON (`serde_json::Value`): a value
//! is typed only once the evaluator knows the field it is compared with.

use serde_json::Value;

use super::date_math::{DateFormat, TimeZoneSpec};
use super::full_text::FullTextQuery;
use super::regex::RegexFlags;
use super::simple_query::SimpleQuery;
use super::udf::MatchRegex;

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
    /// `regexp`: a Lucene regular expression over the whole value of a keyword, or each token
    /// of a text field.
    Regexp(RegexpQuery),
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
    /// `query_string`: a Lucene classic query string, translated when the request is parsed.
    LuceneQueryString(QueryStringClauses),
    /// Internal: a predicate over every value of a MAP column. Built by the
    /// full-text resolver for a field selection that covers all of a MAP
    /// column's keys; never parsed from a request.
    MapValues(MapValuesQuery),
    /// Internal: consecutive tokens of one text field. Built by the full-text
    /// resolver for a phrase of two or more tokens; never parsed from a request.
    TokenPhrase(TokenPhraseQuery),
    /// Internal: tokens of one text field joined by an operator, the `term` of each
    /// token in one node, so a value is tokenized once for all of them. Built by the
    /// full-text resolver for a text of two or more tokens; never parsed from a request.
    TokenSet(TokenSetQuery),
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

/// A `regexp` leaf, its pattern matched as given: lowering never normalizes it, so on a text
/// field it is matched against each lower-case token as written. A query-string `/re/` term
/// arrives here with its pattern already normalized for a text field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegexpQuery {
    /// The field.
    pub(crate) field: String,
    /// The Lucene pattern.
    pub(crate) pattern: String,
    /// The enabled optional operators.
    pub(crate) flags: RegexFlags,
    /// Whether ASCII letters match in either case.
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

/// What one value of a MAP column must satisfy. The values are keywords, so
/// every comparison is exact and case-sensitive.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum MapValuePredicate {
    /// Any value: the map has at least one entry with a value.
    AnyValue,
    /// The whole value equals the text.
    Equals(String),
    /// The value starts with the text.
    StartsWith(String),
    /// The whole value matches an anchored pattern.
    Matches(MatchRegex),
    /// The value lies in a range, in UTF-8 byte order.
    InRange(KeywordRange),
}

impl MapValuePredicate {
    /// Whether one stored value satisfies the predicate.
    pub(crate) fn matches(&self, value: &str) -> bool {
        match self {
            Self::AnyValue => true,
            Self::Equals(expected) => value == expected,
            Self::StartsWith(prefix) => value.starts_with(prefix.as_str()),
            Self::Matches(pattern) => pattern.matches(value),
            Self::InRange(range) => range.contains(value),
        }
    }
}

/// A predicate over the values of one MAP column: true when any entry's value
/// satisfies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapValuesQuery {
    /// The MAP column.
    pub(crate) column: String,
    /// What one value must satisfy.
    pub(crate) predicate: MapValuePredicate,
}

/// Consecutive tokens of one text field, in order; with `is_last_token_prefix`
/// the last token only has to start with the last listed one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenPhraseQuery {
    /// The text field.
    pub(crate) field: String,
    /// The analyzed tokens, at least two.
    pub(crate) tokens: Vec<String>,
    /// Whether the last token is matched as a prefix.
    pub(crate) is_last_token_prefix: bool,
}

/// The analyzed tokens of one text on one text field: with `or`, one token of the field equals
/// one of them; with `and`, each of them equals a token of the field. Each token is compared as
/// an unanalyzed `term` on the field is, so the node matches what the `bool` of one `term` per
/// token joined by the operator matches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenSetQuery {
    /// The text field.
    pub(crate) field: String,
    /// The analyzed tokens, at least two, in the order the text holds them.
    pub(crate) tokens: Vec<String>,
    /// How the tokens join.
    pub(crate) operator: Operator,
}

/// A range over keyword-like values in UTF-8 byte order; an absent bound is open.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct KeywordRange {
    /// The lower bound.
    pub(crate) lower: Option<KeywordBound>,
    /// The upper bound.
    pub(crate) upper: Option<KeywordBound>,
}

impl KeywordRange {
    /// Whether `value` lies inside the range.
    pub(crate) fn contains(&self, value: &str) -> bool {
        self.lower.as_ref().is_none_or(|bound| {
            if bound.is_inclusive {
                value >= bound.value.as_str()
            } else {
                value > bound.value.as_str()
            }
        }) && self.upper.as_ref().is_none_or(|bound| {
            if bound.is_inclusive {
                value <= bound.value.as_str()
            } else {
                value < bound.value.as_str()
            }
        })
    }
}

/// One bound of a keyword range.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct KeywordBound {
    /// The bound value.
    pub(crate) value: String,
    /// Whether the bound value itself is inside the range.
    pub(crate) is_inclusive: bool,
}

/// The options of a `query_string` leaf as the DSL parser reads them. The translation reads
/// every field once, when the request is parsed; the parsed query keeps only the translation
/// ([`QueryStringClauses`]).
#[derive(Debug, Clone)]
pub(crate) struct QueryStringQuery {
    /// The query string.
    pub(crate) query: String,
    /// The single default field, when set.
    pub(crate) default_field: Option<String>,
    /// The default fields (names or patterns), already stripped of `^boost`.
    pub(crate) fields: Vec<String>,
    /// The operator between clauses without one, and between the tokens of one text.
    pub(crate) default_operator: Operator,
    /// Whether a prefix term on a text field is analyzed (earlier tokens become terms).
    pub(crate) analyze_wildcard: bool,
    /// Whether a term may start with `*` or `?`.
    pub(crate) allow_leading_wildcard: bool,
    /// Whether malformed or wrong-typed values match nothing; unset means "only under `*`".
    pub(crate) lenient: Option<bool>,
    /// The zone in which date terms and range bounds without an offset are read.
    pub(crate) time_zone: Option<TimeZoneSpec>,
}

/// A `query_string` after translation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueryStringClauses {
    /// The type-free clause tree, resolved per searched index.
    Translated(FullTextQuery),
    /// A syntax error or a refused leading wildcard: each searched index answers it as its
    /// query-shard failure.
    Invalid {
        /// The failure, in IceGate's wording.
        reason: String,
    },
}
