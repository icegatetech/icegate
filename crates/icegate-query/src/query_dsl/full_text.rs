//! The full-text query tree and its resolution against one index's fields.
//!
//! `match`, `match_phrase`, `match_phrase_prefix`, `multi_match` and
//! `simple_query_string` describe text to look for in a set of fields. The tree
//! here keeps that description free of field types, and [`resolve_full_text`]
//! turns it, per searched index, into plain DSL leaves on concrete fields, which
//! the data-path lowering matches. The per-field decisions are made here, once:
//!
//! - a text field is analyzed with the standard analyzer; text yielding no token
//!   there contributes nothing, and a leaf whose every field contributes nothing
//!   is dropped (`Ok(None)`), so the caller decides what a dropped query matches;
//! - every other field compares the whole text as one value of its type; a value
//!   that does not parse matches nothing when the leaf is lenient and is a
//!   query-shard 400 otherwise;
//! - a selection that names no field matches nothing (an unmapped field is not
//!   an error);
//! - the per-field queries of a leaf are alternatives: one matching field is enough.

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::DataType;
use serde_json::{Number, Value};

use super::{
    analyzer::{normalize_standard, tokenize_standard},
    ast::{
        ALL_FIELDS, BoolQuery, MapValuePredicate, MapValuesQuery, Operator, PrefixQuery, Query, RangeBound, RangeQuery,
        TermQuery, TokenPhraseQuery,
    },
    date_math::DateFormat,
    literal::{NumericTerm, parse_boolean_literal, parse_numeric_term, resolve_date_term},
    simple_query::SimpleQuery,
};
use crate::{
    engine::fixed_binary::decode_fixed_binary_literal,
    opensearch::{catalog::FieldType, error::OpenSearchError},
};

/// How a clause takes part in a boolean combination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Occur {
    /// The clause must match.
    Must,
    /// The clause may match; with no required clause, at least one optional one must.
    Should,
    /// The clause must not match.
    MustNot,
}

/// The fields a full-text leaf reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FieldSelector {
    /// One field by its exact name; `*` and `?` in it are literal characters.
    Name(String),
    /// Every searchable field whose name the glob matches (`*` any run, `?` one character).
    Pattern(String),
}

impl FieldSelector {
    /// A name holding `*` or `?` is a pattern; any other name is one field.
    pub(crate) fn from_name_or_pattern(name: &str) -> Self {
        if name.contains(['*', '?']) {
            Self::Pattern(name.to_owned())
        } else {
            Self::Name(name.to_owned())
        }
    }
}

/// The selectors of a request's `fields` list, whose `^boost` suffixes the DSL parser has
/// already stripped; an empty list selects every field ([`ALL_FIELDS`]).
pub(crate) fn build_field_selectors(fields: &[String]) -> Vec<FieldSelector> {
    if fields.is_empty() {
        return vec![FieldSelector::Pattern(ALL_FIELDS.to_owned())];
    }
    fields.iter().map(|field| FieldSelector::from_name_or_pattern(field)).collect()
}

/// Whether a request's `fields` list reaches every field (empty, or holding `*`):
/// the case in which a leaf is lenient unless the request says otherwise.
pub(crate) fn selects_all_fields(fields: &[String]) -> bool {
    fields.is_empty() || fields.iter().any(|field| field == ALL_FIELDS)
}

/// The settings every leaf of one full-text query shares.
#[derive(Debug, Clone)]
pub(crate) struct LeafSettings {
    /// How the tokens of one analyzed text combine.
    pub(crate) operator: Operator,
    /// Whether a malformed or wrong-typed value matches nothing instead of failing.
    pub(crate) is_lenient: bool,
}

impl LeafSettings {
    /// A leaf over `fields` with these settings.
    pub(crate) const fn build_leaf(&self, fields: Vec<FieldSelector>, term: FullTextTerm) -> FullTextLeaf {
        FullTextLeaf {
            fields,
            term,
            operator: self.operator,
            is_lenient: self.is_lenient,
        }
    }
}

/// One leaf: text to look for in the selected fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FullTextLeaf {
    /// The fields read; one matching field is enough.
    pub(crate) fields: Vec<FieldSelector>,
    /// What to look for.
    pub(crate) term: FullTextTerm,
    /// How the tokens of an analyzed text combine.
    pub(crate) operator: Operator,
    /// Whether a malformed or wrong-typed value matches nothing instead of failing.
    pub(crate) is_lenient: bool,
}

/// What a leaf looks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FullTextTerm {
    /// Unquoted text: tokens joined by the leaf's operator on a text field, one
    /// whole value elsewhere.
    Text(String),
    /// Quoted text: consecutive tokens on a text field, one whole value elsewhere.
    Phrase(String),
    /// Consecutive tokens whose last one is a prefix; text fields only.
    PhrasePrefix(String),
    /// A prefix: of one normalized token on a text field, of the whole value on a
    /// keyword; refused on the other field types.
    Prefix(String),
}

/// A full-text query before field types are known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FullTextQuery {
    /// Every row.
    MatchAll,
    /// One leaf.
    Leaf(FullTextLeaf),
    /// Clauses with fixed occurrences; a dropped clause is left out, and a list
    /// left empty, or holding only the optional match-all a negation adds, is
    /// itself dropped.
    Bool(Vec<FullTextClause>),
}

/// One clause of a [`FullTextQuery::Bool`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FullTextClause {
    /// How the clause takes part.
    pub(crate) occur: Occur,
    /// The clause.
    pub(crate) query: FullTextQuery,
}

impl FullTextClause {
    /// Whether the clause is the optional match-all a negation parses into ("everything
    /// except the negated clause"), which on its own contributes nothing.
    const fn is_negation_match_all(&self) -> bool {
        matches!((self.occur, &self.query), (Occur::Should, FullTextQuery::MatchAll))
    }
}

/// The tree of a parsed `simple_query_string` over `fields`. A query with no term
/// ([`SimpleQuery::MatchNone`], only ever the whole query) is an empty clause list,
/// which is dropped and so matches nothing.
pub(crate) fn build_simple_query_tree(
    query: &SimpleQuery,
    fields: &[FieldSelector],
    settings: &LeafSettings,
) -> FullTextQuery {
    let build_leaf_query = |term| FullTextQuery::Leaf(settings.build_leaf(fields.to_vec(), term));
    match query {
        SimpleQuery::Term { text, is_prefix: false } => build_leaf_query(FullTextTerm::Text(text.clone())),
        SimpleQuery::Term { text, is_prefix: true } => build_leaf_query(FullTextTerm::Prefix(text.clone())),
        SimpleQuery::Phrase { text } => build_leaf_query(FullTextTerm::Phrase(text.clone())),
        SimpleQuery::MatchAll => FullTextQuery::MatchAll,
        SimpleQuery::MatchNone => FullTextQuery::Bool(Vec::new()),
        SimpleQuery::Bool { must, should, must_not } => FullTextQuery::Bool(
            [(Occur::Must, must), (Occur::Should, should), (Occur::MustNot, must_not)]
                .into_iter()
                .flat_map(|(occur, queries)| {
                    queries.iter().map(move |query| FullTextClause {
                        occur,
                        query: build_simple_query_tree(query, fields, settings),
                    })
                })
                .collect(),
        ),
    }
}

/// The searchable fields of one index, as the full-text resolver sees them.
pub(crate) trait FullTextFields {
    /// The index a refused value's query-shard failure names.
    fn index_name(&self) -> &str;

    /// The fields a selector picks; an unmapped name picks none.
    ///
    /// # Errors
    ///
    /// A selection the index cannot answer exactly (see
    /// `TenantSession::expand_field_pattern`).
    fn expand_selector(&self, selector: &FieldSelector) -> Result<Vec<SearchField>, OpenSearchError>;
}

/// How a field matches full-text input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SearchKind {
    /// Analyzed by the standard analyzer and matched per token.
    Text,
    /// Matched as one exact, case-sensitive value.
    Keyword,
    /// A fixed-width binary id written as hex.
    HexId {
        /// Bytes of the id.
        width: i32,
    },
    /// A 32-bit integer.
    Integer,
    /// A 64-bit integer.
    Long,
    /// A double.
    Double,
    /// A timestamp.
    Date,
    /// A boolean.
    Boolean,
}

/// One searchable field a selector picked.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SearchField {
    /// A field with one type.
    Typed {
        /// The name a DSL leaf targets.
        name: String,
        /// How the field matches.
        kind: SearchKind,
    },
    /// Every value of a MAP column, as one keyword-like field.
    MapValues {
        /// The MAP column.
        column: String,
    },
}

/// Resolves a full-text tree against one index's fields into plain DSL leaves.
///
/// `Ok(None)` means every clause was dropped because its text analyzed to no
/// token on every field it reached; the caller decides what that matches. A
/// group whose only surviving clause is the optional match-all of a negation is
/// dropped too, so a negated text with no token excludes nothing and selects nothing.
///
/// # Errors
///
/// A query-shard 400 for a malformed or wrong-typed value in a strict leaf, and
/// for a phrase prefix on a field that is not text; any error of
/// [`FullTextFields::expand_selector`].
pub(crate) fn resolve_full_text(
    query: &FullTextQuery,
    fields: &dyn FullTextFields,
    now: DateTime<Utc>,
) -> Result<Option<Query>, OpenSearchError> {
    match query {
        FullTextQuery::MatchAll => Ok(Some(Query::MatchAll)),
        FullTextQuery::Leaf(leaf) => resolve_leaf(leaf, fields, now),
        FullTextQuery::Bool(clauses) => {
            let mut resolved_clauses = Vec::with_capacity(clauses.len());
            let mut is_contributing = false;
            for clause in clauses {
                if let Some(query) = resolve_full_text(&clause.query, fields, now)? {
                    is_contributing |= !clause.is_negation_match_all();
                    resolved_clauses.push((clause.occur, query));
                }
            }
            Ok(if is_contributing {
                combine_clauses(resolved_clauses)
            } else {
                None
            })
        }
    }
}

/// One query from the surviving clauses: none is a dropped list, a single
/// required or optional clause stands alone, anything else is a `bool` (which
/// matches every row minus the excluded ones when every clause is prohibited).
fn combine_clauses(mut clauses: Vec<(Occur, Query)>) -> Option<Query> {
    if let [(Occur::Must | Occur::Should, _)] = clauses.as_slice() {
        return clauses.pop().map(|(_, query)| query);
    }
    if clauses.is_empty() {
        return None;
    }
    let mut must = Vec::new();
    let mut should = Vec::new();
    let mut must_not = Vec::new();
    for (occur, query) in clauses {
        match occur {
            Occur::Must => must.push(query),
            Occur::Should => should.push(query),
            Occur::MustNot => must_not.push(query),
        }
    }
    Some(build_bool_query(must, should, must_not))
}

const fn build_bool_query(must: Vec<Query>, should: Vec<Query>, must_not: Vec<Query>) -> Query {
    Query::Bool(BoolQuery {
        must,
        filter: Vec::new(),
        should,
        must_not,
        minimum_should_match: None,
    })
}

/// Any of the per-field queries; a field answering `match none` adds nothing.
fn combine_alternatives(mut queries: Vec<Query>) -> Query {
    match queries.len() {
        0 => Query::MatchNone,
        1 => queries.pop().unwrap_or(Query::MatchNone),
        _ => build_bool_query(Vec::new(), queries, Vec::new()),
    }
}

/// The operator's combination of term queries: all of them under `AND`, any under `OR`.
fn join_terms(operator: Operator, mut queries: Vec<Query>) -> Option<Query> {
    match (queries.len(), operator) {
        (0, _) => None,
        (1, _) => queries.pop(),
        (_, Operator::And) => Some(build_bool_query(queries, Vec::new(), Vec::new())),
        (_, Operator::Or) => Some(build_bool_query(Vec::new(), queries, Vec::new())),
    }
}

/// One leaf over every field its selectors pick: `match none` when they pick no field,
/// `None` when the text yields no token on every field picked.
fn resolve_leaf(
    leaf: &FullTextLeaf,
    fields: &dyn FullTextFields,
    now: DateTime<Utc>,
) -> Result<Option<Query>, OpenSearchError> {
    let mut selected_fields = Vec::new();
    for selector in &leaf.fields {
        selected_fields.extend(fields.expand_selector(selector)?);
    }
    // Two selectors may pick one field (`*` and its name); one predicate per field
    // suffices, and the sorted order keeps the resolved tree deterministic.
    selected_fields.sort_unstable();
    selected_fields.dedup();
    if selected_fields.is_empty() {
        return Ok(Some(Query::MatchNone));
    }
    let context = LeafContext { leaf, fields, now };
    let mut field_queries = Vec::with_capacity(selected_fields.len());
    let mut is_dropped = true;
    for field in &selected_fields {
        let field_query = match field {
            SearchField::Typed { name, kind } => context.resolve_typed_field(name, *kind)?,
            SearchField::MapValues { column } => Some(context.resolve_map_values(column)?),
        };
        if let Some(query) = field_query {
            is_dropped = false;
            if !matches!(query, Query::MatchNone) {
                field_queries.push(query);
            }
        }
    }
    Ok((!is_dropped).then(|| combine_alternatives(field_queries)))
}

/// What one leaf's resolution reads, shared by its per-field steps.
struct LeafContext<'a> {
    leaf: &'a FullTextLeaf,
    fields: &'a dyn FullTextFields,
    now: DateTime<Utc>,
}

impl LeafContext<'_> {
    /// The leaf on one typed field; `None` when its text yields no token there.
    fn resolve_typed_field(&self, name: &str, kind: SearchKind) -> Result<Option<Query>, OpenSearchError> {
        match (&self.leaf.term, kind) {
            (FullTextTerm::Phrase(text), SearchKind::Text) => Ok(build_phrase_query(name, text, false)),
            (FullTextTerm::Text(text) | FullTextTerm::Phrase(text), _) => {
                self.build_exact_value_query(name, kind, text)
            }
            (FullTextTerm::PhrasePrefix(text), SearchKind::Text) => Ok(build_phrase_query(name, text, true)),
            (FullTextTerm::PhrasePrefix(_), _) => Err(OpenSearchError::query_shard(
                self.fields.index_name(),
                &format!("a phrase prefix needs a text field, and [{name}] is not one"),
            )),
            (FullTextTerm::Prefix(prefix), SearchKind::Text) => {
                Ok(Some(build_prefix_query(name, normalize_standard(prefix))))
            }
            (FullTextTerm::Prefix(prefix), SearchKind::Keyword) => Ok(Some(build_prefix_query(name, prefix.clone()))),
            (FullTextTerm::Prefix(_), _) => self.refuse_malformed_value(&format!(
                "prefix queries need a keyword or text field, and [{name}] is not one"
            )),
        }
    }

    /// The leaf on every value of a MAP column, whose values are keywords.
    fn resolve_map_values(&self, column: &str) -> Result<Query, OpenSearchError> {
        let predicate = match &self.leaf.term {
            FullTextTerm::Text(text) | FullTextTerm::Phrase(text) => MapValuePredicate::Equals(text.clone()),
            FullTextTerm::Prefix(prefix) => MapValuePredicate::StartsWith(prefix.clone()),
            FullTextTerm::PhrasePrefix(_) => {
                return Err(OpenSearchError::query_shard(
                    self.fields.index_name(),
                    &format!("a phrase prefix needs a text field, and the values of [{column}] are keywords"),
                ));
            }
        };
        Ok(Query::MapValues(MapValuesQuery {
            column: column.to_owned(),
            predicate,
        }))
    }

    /// The whole text as one value of the field: analyzed into terms on a text
    /// field, exact on a keyword, parsed on the typed fields.
    fn build_exact_value_query(
        &self,
        name: &str,
        kind: SearchKind,
        text: &str,
    ) -> Result<Option<Query>, OpenSearchError> {
        match kind {
            SearchKind::Text => Ok(join_terms(
                self.leaf.operator,
                tokenize_standard(text)
                    .into_iter()
                    .map(|token| build_term_query(name, Value::String(token.text)))
                    .collect(),
            )),
            SearchKind::Keyword => Ok(Some(build_term_query(name, Value::String(text.to_owned())))),
            // A `term` leaf carries the value as the request wrote it, and its lowering
            // decodes it for the column; this decode only chooses that leaf or a refusal.
            SearchKind::HexId { width } => match decode_fixed_binary_literal(text, &DataType::FixedSizeBinary(width)) {
                Ok(_) => Ok(Some(build_term_query(name, Value::String(text.to_owned())))),
                Err(error) => {
                    self.refuse_malformed_value(&format!("[{text}] is not a {width}-byte hex id for [{name}]: {error}"))
                }
            },
            SearchKind::Integer => self.build_numeric_term_query(name, text, FieldType::Integer),
            SearchKind::Long => self.build_numeric_term_query(name, text, FieldType::Long),
            SearchKind::Double => self.build_numeric_term_query(name, text, FieldType::Double),
            SearchKind::Date => self.build_date_term_query(name, text),
            SearchKind::Boolean => parse_boolean_literal(text).map_or_else(
                || self.refuse_malformed_value(&format!("[{text}] is not a boolean for [{name}]")),
                |value| Ok(Some(build_term_query(name, Value::Bool(value)))),
            ),
        }
    }

    /// A numeric term: its parsed value; nothing for a fraction on an integer field.
    fn build_numeric_term_query(
        &self,
        name: &str,
        text: &str,
        field_type: FieldType,
    ) -> Result<Option<Query>, OpenSearchError> {
        let term = match parse_numeric_term(text, field_type) {
            Ok(term) => term,
            Err(error) => {
                return self
                    .refuse_malformed_value(&format!("[{text}] is not a number of the type of [{name}]: {error}"));
            }
        };
        match term {
            NumericTerm::Integer(value) => Ok(Some(build_term_query(name, Value::from(value)))),
            NumericTerm::Double(value) => Number::from_f64(value).map_or_else(
                || self.refuse_malformed_value(&format!("[{text}] is not a finite number for [{name}]")),
                |number| Ok(Some(build_term_query(name, Value::Number(number)))),
            ),
            NumericTerm::NoMatch => Ok(Some(Query::MatchNone)),
        }
    }

    /// A date term: the half-open microsecond range it covers, written as the
    /// inclusive millisecond range whose epoch-millisecond bounds lower back to it
    /// (`gte X` is `>= X·1000 µs`, `lte Y` is `< (Y+1)·1000 µs`).
    fn build_date_term_query(&self, name: &str, text: &str) -> Result<Option<Query>, OpenSearchError> {
        resolve_date_term(text, None, self.now).map_or_else(
            |error| self.refuse_malformed_value(&format!("[{text}] is not a date for [{name}]: {error}")),
            |(start_micros, end_micros)| Ok(Some(build_millisecond_range(name, start_micros, end_micros))),
        )
    }

    /// A malformed or wrong-typed value: nothing under leniency, a 400 otherwise.
    fn refuse_malformed_value(&self, reason: &str) -> Result<Option<Query>, OpenSearchError> {
        if self.leaf.is_lenient {
            Ok(Some(Query::MatchNone))
        } else {
            Err(OpenSearchError::query_shard(self.fields.index_name(), reason))
        }
    }
}

/// The inclusive epoch-millisecond range covering the half-open microsecond range
/// `[start_micros, end_micros)`.
fn build_millisecond_range(name: &str, start_micros: i64, end_micros: i64) -> Query {
    Query::Range(RangeQuery {
        field: name.to_owned(),
        lower: Some(RangeBound {
            value: Value::from(start_micros.div_euclid(1000)),
            is_inclusive: true,
        }),
        upper: Some(RangeBound {
            value: Value::from(end_micros.div_euclid(1000) - 1),
            is_inclusive: true,
        }),
        format: Some(DateFormat::EpochMillis),
        time_zone: None,
    })
}

/// A phrase on a text field: none for no token, a term or a prefix for one, a
/// token phrase for more.
fn build_phrase_query(name: &str, text: &str, is_last_token_prefix: bool) -> Option<Query> {
    let mut tokens: Vec<String> = tokenize_standard(text).into_iter().map(|token| token.text).collect();
    match (tokens.len(), is_last_token_prefix) {
        (0, _) => None,
        (1, false) => tokens.pop().map(|token| build_term_query(name, Value::String(token))),
        (1, true) => tokens.pop().map(|token| build_prefix_query(name, token)),
        _ => Some(Query::TokenPhrase(TokenPhraseQuery {
            field: name.to_owned(),
            tokens,
            is_last_token_prefix,
        })),
    }
}

fn build_term_query(name: &str, value: Value) -> Query {
    Query::Term(TermQuery {
        field: name.to_owned(),
        value,
        case_insensitive: false,
    })
}

fn build_prefix_query(name: &str, value: String) -> Query {
    Query::Prefix(PrefixQuery {
        field: name.to_owned(),
        value,
        case_insensitive: false,
    })
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use chrono::{DateTime, Utc};
    use serde_json::{Value, json};

    use super::{
        FieldSelector, FullTextClause, FullTextFields, FullTextLeaf, FullTextQuery, FullTextTerm, LeafSettings, Occur,
        SearchField, SearchKind, build_field_selectors, build_simple_query_tree, resolve_full_text, selects_all_fields,
    };
    use crate::{
        opensearch::error::{OpenSearchError, render_envelope},
        query_dsl::{
            ast::{
                BoolQuery, MapValuePredicate, MapValuesQuery, Operator, PrefixQuery, Query, RangeBound, RangeQuery,
                TermQuery, TokenPhraseQuery,
            },
            date_math::DateFormat,
            simple_query::{SimpleQuery, SimpleQueryFlags, parse_simple_query_string},
        },
    };

    /// The searchable fields of a logs-like index, stated for these tests: eight
    /// typed fields and the MAP column `log_attributes`, whose keys resolve as keywords.
    struct StatedFields {
        index: &'static str,
    }

    const TYPED_FIELDS: &[(&str, SearchKind)] = &[
        ("body", SearchKind::Text),
        ("count", SearchKind::Long),
        ("flag", SearchKind::Boolean),
        ("ratio", SearchKind::Double),
        ("severity_text", SearchKind::Keyword),
        ("status", SearchKind::Integer),
        ("timestamp", SearchKind::Date),
        ("trace_id", SearchKind::HexId { width: 16 }),
    ];

    impl FullTextFields for StatedFields {
        fn index_name(&self) -> &str {
            self.index
        }

        /// A name selects the typed field of that name, or one `log_attributes` key as a
        /// keyword. Only the two patterns these tests use are stated: `*` selects every
        /// typed field and every `log_attributes` value, `log_attributes.*` only the values.
        fn expand_selector(&self, selector: &FieldSelector) -> Result<Vec<SearchField>, OpenSearchError> {
            let build_typed_field = |name: &str, kind: SearchKind| SearchField::Typed {
                name: name.to_owned(),
                kind,
            };
            let build_log_attribute_values = || SearchField::MapValues {
                column: String::from("log_attributes"),
            };
            Ok(match selector {
                FieldSelector::Name(name) => match TYPED_FIELDS.iter().find(|(field, _)| *field == name.as_str()) {
                    Some((field, kind)) => vec![build_typed_field(field, *kind)],
                    None if name.starts_with("log_attributes.") => vec![build_typed_field(name, SearchKind::Keyword)],
                    None => Vec::new(),
                },
                FieldSelector::Pattern(pattern) if pattern == "*" => TYPED_FIELDS
                    .iter()
                    .map(|(field, kind)| build_typed_field(field, *kind))
                    .chain([build_log_attribute_values()])
                    .collect(),
                FieldSelector::Pattern(pattern) if pattern == "log_attributes.*" => {
                    vec![build_log_attribute_values()]
                }
                FieldSelector::Pattern(pattern) => {
                    return Err(OpenSearchError::internal(format!(
                        "no expansion is stated for the pattern [{pattern}]"
                    )));
                }
            })
        }
    }

    fn parse_request_instant() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-08T12:34:56.789Z")
            .expect("a literal instant")
            .with_timezone(&Utc)
    }

    fn build_named_selectors(field: &str) -> Vec<FieldSelector> {
        vec![FieldSelector::Name(field.to_owned())]
    }

    fn build_all_fields_selectors() -> Vec<FieldSelector> {
        vec![FieldSelector::Pattern(String::from("*"))]
    }

    const fn build_leaf_query(
        fields: Vec<FieldSelector>,
        term: FullTextTerm,
        operator: Operator,
        is_lenient: bool,
    ) -> FullTextQuery {
        FullTextQuery::Leaf(FullTextLeaf {
            fields,
            term,
            operator,
            is_lenient,
        })
    }

    fn try_resolve_query(query: &FullTextQuery) -> Result<Option<Query>, OpenSearchError> {
        resolve_full_text(query, &StatedFields { index: "logs" }, parse_request_instant())
    }

    fn resolve_query(query: &FullTextQuery) -> Option<Query> {
        try_resolve_query(query).expect("the query resolves")
    }

    fn build_text_term(value: &str) -> FullTextTerm {
        FullTextTerm::Text(value.to_owned())
    }

    fn build_term(field: &str, value: Value) -> Query {
        Query::Term(TermQuery {
            field: field.to_owned(),
            value,
            case_insensitive: false,
        })
    }

    fn build_prefix(field: &str, value: &str) -> Query {
        Query::Prefix(PrefixQuery {
            field: field.to_owned(),
            value: value.to_owned(),
            case_insensitive: false,
        })
    }

    fn build_map_values_query(predicate: MapValuePredicate) -> Query {
        Query::MapValues(MapValuesQuery {
            column: String::from("log_attributes"),
            predicate,
        })
    }

    const fn build_should_query(queries: Vec<Query>) -> Query {
        Query::Bool(BoolQuery {
            must: vec![],
            filter: vec![],
            should: queries,
            must_not: vec![],
            minimum_should_match: None,
        })
    }

    const fn build_must_query(queries: Vec<Query>) -> Query {
        Query::Bool(BoolQuery {
            must: queries,
            filter: vec![],
            should: vec![],
            must_not: vec![],
            minimum_should_match: None,
        })
    }

    /// Asserts that `result` is a query-shard 400 and answers its root cause's reason.
    async fn assert_query_shard_400(result: Result<Option<Query>, OpenSearchError>) -> String {
        let error = result.expect_err("a strict malformed literal is refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "search_phase_execution_exception");
        assert_eq!(body["error"]["root_cause"][0]["type"], "query_shard_exception");
        body["error"]["root_cause"][0]["reason"]
            .as_str()
            .expect("a root cause states its reason")
            .to_owned()
    }

    #[test]
    fn a_text_field_whose_text_has_no_token_is_dropped_and_an_unmapped_field_matches_nothing() {
        assert!(
            resolve_query(&build_leaf_query(
                build_named_selectors("body"),
                build_text_term("!!!"),
                Operator::Or,
                false
            ))
            .is_none()
        );
        assert_eq!(
            resolve_query(&build_leaf_query(
                build_named_selectors("missing"),
                build_text_term("a"),
                Operator::Or,
                false
            )),
            Some(Query::MatchNone)
        );
        // Under `*` the keyword field and the MAP values still compare the whole value
        // `!!!`; every typed field fails to parse it and, lenient, adds nothing.
        assert_eq!(
            resolve_query(&build_leaf_query(
                build_all_fields_selectors(),
                build_text_term("!!!"),
                Operator::Or,
                true
            )),
            Some(build_should_query(vec![
                build_term("severity_text", json!("!!!")),
                build_map_values_query(MapValuePredicate::Equals(String::from("!!!"))),
            ]))
        );
    }

    #[test]
    fn whole_values_match_typed_fields_by_their_type() {
        let rows: Vec<(&str, &str, Query)> = vec![
            ("count", "10", build_term("count", json!(10))),
            ("count", "1.5", Query::MatchNone),
            ("status", "200", build_term("status", json!(200))),
            ("ratio", "1.5", build_term("ratio", json!(1.5))),
            ("flag", "true", build_term("flag", json!(true))),
            (
                "trace_id",
                "0123456789abcdef0123456789abcdef",
                build_term("trace_id", json!("0123456789abcdef0123456789abcdef")),
            ),
            ("severity_text", "ERROR", build_term("severity_text", json!("ERROR"))),
            (
                "log_attributes.http.method",
                "GET",
                build_term("log_attributes.http.method", json!("GET")),
            ),
            (
                "timestamp",
                "2026-10-08",
                // One UTC day: 2026-10-08T00:00:00Z = 1791417600000 ms to the millisecond
                // before 2026-10-09T00:00:00Z = 1791504000000 ms.
                Query::Range(RangeQuery {
                    field: String::from("timestamp"),
                    lower: Some(RangeBound {
                        value: json!(1_791_417_600_000_i64),
                        is_inclusive: true,
                    }),
                    upper: Some(RangeBound {
                        value: json!(1_791_503_999_999_i64),
                        is_inclusive: true,
                    }),
                    format: Some(DateFormat::EpochMillis),
                    time_zone: None,
                }),
            ),
        ];
        for (field, value, expected) in rows {
            assert_eq!(
                resolve_query(&build_leaf_query(
                    build_named_selectors(field),
                    build_text_term(value),
                    Operator::Or,
                    false
                )),
                Some(expected),
                "[{field}:{value}]"
            );
        }
    }

    #[tokio::test]
    async fn malformed_literals_match_nothing_under_leniency_and_are_a_query_shard_400_otherwise() {
        // The strict reason carries the parse error's cause, so a well-formed number
        // outside the field's range reads differently from text that is no number.
        for (field, value, reason) in [
            (
                "count",
                "abc",
                "[abc] is not a number of the type of [count]: malformed value",
            ),
            (
                "status",
                "3000000000",
                "[3000000000] is not a number of the type of [status]: value out of range",
            ),
            (
                "trace_id",
                "xyz",
                "[xyz] is not a 16-byte hex id for [trace_id]: not an even-length hex string",
            ),
            (
                "trace_id",
                "0123",
                "[0123] is not a 16-byte hex id for [trace_id]: expected 16 bytes, got 2",
            ),
            ("flag", "yes", "[yes] is not a boolean for [flag]"),
            (
                "timestamp",
                "not-a-date",
                "[not-a-date] is not a date for [timestamp]: malformed value",
            ),
        ] {
            assert_eq!(
                resolve_query(&build_leaf_query(
                    build_named_selectors(field),
                    build_text_term(value),
                    Operator::Or,
                    true
                )),
                Some(Query::MatchNone),
                "[{field}:{value}]"
            );
            let refusal_reason = assert_query_shard_400(try_resolve_query(&build_leaf_query(
                build_named_selectors(field),
                build_text_term(value),
                Operator::Or,
                false,
            )))
            .await;
            assert_eq!(refusal_reason, reason, "[{field}:{value}]");
        }
    }

    #[tokio::test]
    async fn phrases_of_several_tokens_become_token_phrases_and_one_token_a_term() {
        assert_eq!(
            resolve_query(&build_leaf_query(
                build_named_selectors("body"),
                FullTextTerm::Phrase(String::from("Refused by")),
                Operator::Or,
                false
            )),
            Some(Query::TokenPhrase(TokenPhraseQuery {
                field: String::from("body"),
                tokens: vec![String::from("refused"), String::from("by")],
                is_last_token_prefix: false,
            }))
        );
        assert_eq!(
            resolve_query(&build_leaf_query(
                build_named_selectors("body"),
                FullTextTerm::Phrase(String::from("Refused")),
                Operator::Or,
                false
            )),
            Some(build_term("body", json!("refused")))
        );
        assert_eq!(
            resolve_query(&build_leaf_query(
                build_named_selectors("body"),
                FullTextTerm::PhrasePrefix(String::from("refused b")),
                Operator::Or,
                false
            )),
            Some(Query::TokenPhrase(TokenPhraseQuery {
                field: String::from("body"),
                tokens: vec![String::from("refused"), String::from("b")],
                is_last_token_prefix: true,
            }))
        );
        assert_eq!(
            resolve_query(&build_leaf_query(
                build_named_selectors("body"),
                FullTextTerm::PhrasePrefix(String::from("Ref")),
                Operator::Or,
                false
            )),
            Some(build_prefix("body", "ref"))
        );
        assert_eq!(
            resolve_query(&build_leaf_query(
                build_named_selectors("severity_text"),
                FullTextTerm::Phrase(String::from("a b")),
                Operator::Or,
                false
            )),
            Some(build_term("severity_text", json!("a b")))
        );
        // A phrase prefix needs a text field, lenient or not.
        assert_query_shard_400(try_resolve_query(&build_leaf_query(
            build_named_selectors("severity_text"),
            FullTextTerm::PhrasePrefix(String::from("a")),
            Operator::Or,
            true,
        )))
        .await;
    }

    #[test]
    fn analyzed_text_joins_its_tokens_by_the_operator() {
        let build_token_terms = || {
            vec![
                build_term("body", json!("connection")),
                build_term("body", json!("refused")),
            ]
        };
        assert_eq!(
            resolve_query(&build_leaf_query(
                build_named_selectors("body"),
                build_text_term("Connection Refused"),
                Operator::Or,
                false
            )),
            Some(build_should_query(build_token_terms()))
        );
        assert_eq!(
            resolve_query(&build_leaf_query(
                build_named_selectors("body"),
                build_text_term("Connection Refused"),
                Operator::And,
                false
            )),
            Some(build_must_query(build_token_terms()))
        );
    }

    #[test]
    fn a_star_selection_reaches_each_field_once_and_every_map_value() {
        let mut fields = build_all_fields_selectors();
        fields.push(FieldSelector::Name(String::from("severity_text")));
        assert_eq!(
            resolve_query(&build_leaf_query(fields, build_text_term("GET"), Operator::Or, true)),
            Some(build_should_query(vec![
                build_term("body", json!("get")),
                build_term("severity_text", json!("GET")),
                build_map_values_query(MapValuePredicate::Equals(String::from("GET"))),
            ]))
        );
    }

    #[tokio::test]
    async fn a_prefix_is_normalized_on_text_kept_on_keywords_and_refused_on_typed_fields() {
        assert_eq!(
            resolve_query(&build_leaf_query(
                build_all_fields_selectors(),
                FullTextTerm::Prefix(String::from("Err")),
                Operator::Or,
                true
            )),
            Some(build_should_query(vec![
                build_prefix("body", "err"),
                build_prefix("severity_text", "Err"),
                build_map_values_query(MapValuePredicate::StartsWith(String::from("Err"))),
            ]))
        );
        assert_query_shard_400(try_resolve_query(&build_leaf_query(
            build_named_selectors("count"),
            FullTextTerm::Prefix(String::from("1")),
            Operator::Or,
            false,
        )))
        .await;
    }

    #[test]
    fn simple_query_string_clauses_keep_their_occurrences() {
        let parsed = SimpleQuery::Bool {
            must: vec![SimpleQuery::Term {
                text: String::from("a"),
                is_prefix: false,
            }],
            should: vec![SimpleQuery::Phrase {
                text: String::from("b c"),
            }],
            must_not: vec![
                SimpleQuery::Term {
                    text: String::from("d"),
                    is_prefix: true,
                },
                SimpleQuery::MatchAll,
            ],
        };
        let settings = LeafSettings {
            operator: Operator::Or,
            is_lenient: true,
        };
        let build_clause = |occur, term| FullTextClause {
            occur,
            query: build_leaf_query(build_all_fields_selectors(), term, Operator::Or, true),
        };
        assert_eq!(
            build_simple_query_tree(&parsed, &build_all_fields_selectors(), &settings),
            FullTextQuery::Bool(vec![
                build_clause(Occur::Must, build_text_term("a")),
                build_clause(Occur::Should, FullTextTerm::Phrase(String::from("b c"))),
                build_clause(Occur::MustNot, FullTextTerm::Prefix(String::from("d"))),
                FullTextClause {
                    occur: Occur::MustNot,
                    query: FullTextQuery::MatchAll
                },
            ])
        );
        // A query with no term is an empty clause list: dropped, so it matches nothing.
        assert_eq!(
            build_simple_query_tree(&SimpleQuery::MatchNone, &build_all_fields_selectors(), &settings),
            FullTextQuery::Bool(Vec::new())
        );
        assert!(
            resolve_query(&build_simple_query_tree(
                &SimpleQuery::MatchNone,
                &build_all_fields_selectors(),
                &settings
            ))
            .is_none()
        );
    }

    #[test]
    fn a_fields_list_selects_names_and_patterns_and_defaults_to_every_field() {
        assert_eq!(build_field_selectors(&[]), build_all_fields_selectors());
        // The DSL parser has already stripped each `^boost`: `a^1` is what it leaves of
        // `a^1^2`, and that whole entry is the name.
        assert_eq!(
            build_field_selectors(&[String::from("body"), String::from("log_*"), String::from("a^1")]),
            vec![
                FieldSelector::Name(String::from("body")),
                FieldSelector::Pattern(String::from("log_*")),
                FieldSelector::Name(String::from("a^1")),
            ]
        );
        assert!(selects_all_fields(&[]));
        assert!(selects_all_fields(&[String::from("body"), String::from("*")]));
        assert!(!selects_all_fields(&[String::from("body")]));
    }

    #[test]
    fn a_dropped_clause_leaves_its_siblings_and_an_all_negative_list_excludes() {
        let build_keyword_leaf = || {
            build_leaf_query(
                build_named_selectors("severity_text"),
                build_text_term("a"),
                Operator::Or,
                false,
            )
        };
        let tree = FullTextQuery::Bool(vec![
            FullTextClause {
                occur: Occur::Must,
                query: build_leaf_query(
                    build_named_selectors("body"),
                    build_text_term("!!!"),
                    Operator::Or,
                    false,
                ),
            },
            FullTextClause {
                occur: Occur::Should,
                query: build_keyword_leaf(),
            },
        ]);
        assert_eq!(resolve_query(&tree), Some(build_term("severity_text", json!("a"))));
        let negative_tree = FullTextQuery::Bool(vec![FullTextClause {
            occur: Occur::MustNot,
            query: build_keyword_leaf(),
        }]);
        assert_eq!(
            resolve_query(&negative_tree),
            Some(Query::Bool(BoolQuery {
                must: vec![],
                filter: vec![],
                should: vec![],
                must_not: vec![build_term("severity_text", json!("a"))],
                minimum_should_match: None,
            }))
        );
        assert!(resolve_query(&FullTextQuery::Bool(Vec::new())).is_none());
    }

    /// A negation parses as "everything except it" (`should: [MatchAll]` beside the
    /// `must_not`); when the negated text yields no token, that group contributes nothing,
    /// the match-all included, as on system indices.
    #[test]
    fn a_negated_clause_with_no_token_drops_its_group() {
        let settings = LeafSettings {
            operator: Operator::Or,
            is_lenient: false,
        };
        let build_body_tree = |text: &str| {
            let parsed =
                parse_simple_query_string(text, SimpleQueryFlags::ALL, Operator::Or).expect("the query parses");
            build_simple_query_tree(&parsed, &build_named_selectors("body"), &settings)
        };
        let negated_tree = build_body_tree("-!!!");
        assert_eq!(
            negated_tree,
            FullTextQuery::Bool(vec![
                FullTextClause {
                    occur: Occur::Should,
                    query: FullTextQuery::MatchAll
                },
                FullTextClause {
                    occur: Occur::MustNot,
                    query: build_leaf_query(
                        build_named_selectors("body"),
                        build_text_term("!!!"),
                        Operator::Or,
                        false
                    ),
                },
            ])
        );
        assert!(resolve_query(&negated_tree).is_none());
        assert_eq!(
            resolve_query(&build_body_tree("-!!! foo")),
            Some(build_term("body", json!("foo")))
        );
        assert_eq!(
            resolve_query(&build_body_tree("foo")),
            Some(build_term("body", json!("foo")))
        );
        // A negation whose text does yield a token keeps its match-all: everything except it.
        assert_eq!(
            resolve_query(&build_body_tree("-foo")),
            Some(Query::Bool(BoolQuery {
                must: vec![],
                filter: vec![],
                should: vec![Query::MatchAll],
                must_not: vec![build_term("body", json!("foo"))],
                minimum_should_match: None,
            }))
        );
    }
}
