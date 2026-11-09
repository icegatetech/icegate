//! The full-text query tree and its resolution against one index's fields.
//!
//! `match`, `match_phrase`, `match_phrase_prefix`, `multi_match`,
//! `simple_query_string` and `query_string` describe text to look for in a set of
//! fields. The tree here keeps that description free of field types, and
//! [`resolve_full_text`] turns it, per searched index, into plain DSL leaves on
//! concrete fields, which the data-path lowering and the system-index evaluator
//! match. The per-field decisions are made here, once:
//!
//! - a text field is analyzed with the standard analyzer; text yielding no token
//!   there contributes nothing, and a leaf whose every field contributes nothing
//!   is dropped (`Ok(None)`), so the caller decides what a dropped query matches;
//! - every other field compares the whole text as one value of its type; a value
//!   that does not parse, and a wildcard, prefix, regular expression or range a
//!   field type does not support, match nothing when the leaf is lenient and are a query-shard 400
//!   otherwise;
//! - a selection that names no field matches nothing (an unmapped field is not
//!   an error);
//! - the per-field queries of a leaf are alternatives: one matching field is enough;
//! - a classic clause list (`query_string`) takes each clause's occurrence from
//!   its conjunction and modifier, applied over the clauses that survive analysis
//!   (see [`ClassicQuery`]);
//! - a `query_string` time zone reads an unquoted term on one explicitly named
//!   field and every unquoted range bound; a quoted value is never read in it, and
//!   an epoch-millisecond value read in it is that zone's wall-clock time.

use chrono::{DateTime, Offset, TimeZone, Utc};
use datafusion::arrow::datatypes::DataType;
use serde_json::{Number, Value};

use super::{
    analyzer::{normalize_standard, tokenize_standard},
    ast::{
        ALL_FIELDS, BoolQuery, ExistsQuery, KeywordBound, KeywordRange, MapValuePredicate, MapValuesQuery, Operator,
        PrefixQuery, Query, RangeBound, RangeQuery, RegexpQuery, TermQuery, TokenPhraseQuery, WildcardQuery,
    },
    date_math::{BoundSide, DateFormat, TimeZoneSpec, resolve_date_bound},
    literal::{NumericTerm, parse_boolean_literal, parse_java_double, parse_numeric_term, resolve_date_term},
    regex::{RegexFlags, translate_lucene_regex},
    simple_query::SimpleQuery,
    udf::{MatchRegex, translate_wildcard},
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
    /// The zone of `query_string` date values; `None` for the other leaves.
    pub(crate) time_zone: Option<TimeZoneSpec>,
}

impl LeafSettings {
    /// A leaf over `fields` with these settings.
    pub(crate) fn build_leaf(&self, fields: Vec<FieldSelector>, term: FullTextTerm) -> FullTextLeaf {
        FullTextLeaf {
            fields,
            term,
            operator: self.operator,
            is_lenient: self.is_lenient,
            time_zone: self.time_zone.clone(),
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
    /// The zone of date values without an offset: applied to an unquoted term on
    /// one explicitly named field and to every unquoted range bound.
    pub(crate) time_zone: Option<TimeZoneSpec>,
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
    /// A prefix: of one normalized token on a text field (with `is_analyzed`, the
    /// analyzed tokens whose last one is the prefix), of the whole value on a
    /// keyword; refused on the other field types.
    Prefix {
        /// The prefix, escapes removed.
        prefix: String,
        /// Whether the prefix is analyzed on a text field (`analyze_wildcard`).
        is_analyzed: bool,
    },
    /// A wildcard pattern (`*`, `?`, `\` escapes kept): per normalized token on a
    /// text field, the whole value on a keyword; refused on the other field types.
    Wildcard(String),
    /// A range; an absent bound is open, and with both absent the field exists.
    Range {
        /// The lower bound.
        lower: Option<FullTextBound>,
        /// The upper bound.
        upper: Option<FullTextBound>,
    },
    /// The field has a value.
    Exists,
    /// A Lucene regular expression: per token on a text field with the pattern
    /// normalized, the whole value on a keyword with the pattern verbatim; refused
    /// on the other field types.
    Regexp(String),
}

/// One bound of a full-text range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FullTextBound {
    /// The bound as written, escapes removed.
    pub(crate) text: String,
    /// Whether the bound value itself is inside the range.
    pub(crate) is_inclusive: bool,
    /// Whether it was quoted, which keeps a time zone from applying to it.
    pub(crate) is_quoted: bool,
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
    /// A classic clause list (`query_string`): occurrences come from each clause's
    /// conjunction and modifier over the clauses that survive analysis; a list left
    /// empty is itself dropped.
    Classic(ClassicQuery),
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

/// A clause list of the classic query syntax, before analysis.
///
/// The occurrence of each clause is decided per searched index, over the clauses
/// that survive analysis there, so a clause dropped for want of a token neither
/// matches nor changes a neighbour. Under the `OR` default a clause is required
/// when it carries `+` or follows `AND`, prohibited when it carries `-`, `NOT` or
/// `!`, optional otherwise; an `AND` also makes the previous surviving clause
/// required. Under the `AND` default a clause is required unless it is prohibited
/// or follows `OR`, and an `OR` makes the previous surviving clause optional. A
/// prohibited clause never changes. A multi-term entry occurs as the default
/// operator says (optional under `OR`, required under `AND`) and changes no
/// neighbour. A list whose surviving clauses are all prohibited matches every row
/// minus them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClassicQuery {
    /// The default operator, which the occurrence rules read.
    pub(crate) operator: Operator,
    /// The clauses in order.
    pub(crate) entries: Vec<ClassicEntry>,
}

/// One entry of a classic clause list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClassicEntry {
    /// A clause with the conjunction before it and its modifier, as written.
    Clause {
        /// `AND`, `OR` or nothing before the clause.
        conjunction: Conjunction,
        /// `+`, `-` / `NOT` / `!` or nothing on the clause.
        modifier: Modifier,
        /// The clause.
        query: FullTextQuery,
    },
    /// Adjacent bare terms analyzed together; occurs as the default operator says
    /// and changes no neighbour's occurrence.
    MultiTerm(FullTextLeaf),
}

/// The conjunction written before a classic clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Conjunction {
    /// None written.
    Implicit,
    /// `AND` or `&&`.
    And,
    /// `OR` or `||`.
    Or,
}

/// The modifier written on a classic clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Modifier {
    /// None written.
    Absent,
    /// `+`.
    Required,
    /// `-`, `NOT` or `!`.
    Prohibited,
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
        SimpleQuery::Term { text, is_prefix: true } => build_leaf_query(FullTextTerm::Prefix {
            prefix: text.clone(),
            is_analyzed: false,
        }),
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
    /// A fixed-width binary id written as hex, whose missing id renders `""`: an empty value
    /// matches the rows without one.
    HexIdOrEmpty {
        /// Bytes of the id.
        width: i32,
    },
    /// A keyword shown under a reversible conversion of its stored value (an enum name, NULL as
    /// `""`): matched as one exact value, and never by a pattern, whose inverse no conversion
    /// declares.
    EncodedKeyword,
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
/// for a phrase prefix on a field that is not text; `illegal_argument` for a
/// wildcard on MAP values that does not compile; any error of
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
        FullTextQuery::Classic(classic) => resolve_classic_clauses(classic, fields, now),
    }
}

/// A classic clause list: each surviving clause with the occurrence [`ClassicQuery`] states.
fn resolve_classic_clauses(
    classic: &ClassicQuery,
    fields: &dyn FullTextFields,
    now: DateTime<Utc>,
) -> Result<Option<Query>, OpenSearchError> {
    let mut resolved_clauses = Vec::with_capacity(classic.entries.len());
    for entry in &classic.entries {
        match entry {
            ClassicEntry::MultiTerm(leaf) => {
                if let Some(query) = resolve_leaf(leaf, fields, now)? {
                    resolved_clauses.push((choose_multi_term_occur(classic.operator), query));
                }
            }
            ClassicEntry::Clause {
                conjunction,
                modifier,
                query,
            } => {
                // The conjunction marks the previous surviving clause before this one is
                // known to survive: a dropped clause still joins its neighbours.
                mark_previous_clause(&mut resolved_clauses, classic.operator, *conjunction);
                if let Some(query) = resolve_full_text(query, fields, now)? {
                    resolved_clauses.push((choose_clause_occur(classic.operator, *conjunction, *modifier), query));
                }
            }
        }
    }
    Ok(combine_clauses(resolved_clauses))
}

/// The occurrence of a multi-term entry: the default operator's.
const fn choose_multi_term_occur(operator: Operator) -> Occur {
    match operator {
        Operator::Or => Occur::Should,
        Operator::And => Occur::Must,
    }
}

/// What a conjunction does to the previous surviving clause: `AND` makes it
/// required, `OR` under the `AND` default optional; a prohibited clause stays.
fn mark_previous_clause(clauses: &mut [(Occur, Query)], operator: Operator, conjunction: Conjunction) {
    let Some((occur, _)) = clauses.last_mut() else {
        return;
    };
    if *occur == Occur::MustNot {
        return;
    }
    match (conjunction, operator) {
        (Conjunction::And, _) => *occur = Occur::Must,
        (Conjunction::Or, Operator::And) => *occur = Occur::Should,
        (Conjunction::Or, Operator::Or) | (Conjunction::Implicit, _) => {}
    }
}

/// The occurrence of a classic clause from its conjunction and modifier.
const fn choose_clause_occur(operator: Operator, conjunction: Conjunction, modifier: Modifier) -> Occur {
    match (modifier, operator, conjunction) {
        (Modifier::Prohibited, _, _) => Occur::MustNot,
        (Modifier::Required | Modifier::Absent, Operator::And, Conjunction::Or)
        | (Modifier::Absent, Operator::Or, Conjunction::Implicit | Conjunction::Or) => Occur::Should,
        (Modifier::Required | Modifier::Absent, _, _) => Occur::Must,
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
            (FullTextTerm::Text(text), _) => self.build_exact_value_query(name, kind, text, self.read_term_time_zone()),
            (FullTextTerm::Phrase(text), SearchKind::Text) => Ok(build_phrase_query(name, text, false)),
            (FullTextTerm::Phrase(text), _) => self.build_exact_value_query(name, kind, text, None),
            (FullTextTerm::PhrasePrefix(text), SearchKind::Text) => Ok(build_phrase_query(name, text, true)),
            (FullTextTerm::PhrasePrefix(_), _) => Err(OpenSearchError::query_shard(
                self.fields.index_name(),
                &format!("a phrase prefix needs a text field, and [{name}] is not one"),
            )),
            (FullTextTerm::Prefix { prefix, is_analyzed }, SearchKind::Text) => {
                Ok(self.build_text_prefix_query(name, prefix, *is_analyzed))
            }
            (FullTextTerm::Prefix { prefix, .. }, SearchKind::Keyword) => {
                Ok(Some(build_prefix_query(name, prefix.clone())))
            }
            (FullTextTerm::Prefix { .. }, _) => self.refuse_malformed_value(&format!(
                "prefix queries need a keyword or text field, and [{name}] is not one"
            )),
            // The pattern stays as written: the wildcard leaf lowercases it for a text
            // field and reads it verbatim on a keyword, on both evaluation paths.
            (FullTextTerm::Wildcard(pattern), SearchKind::Text | SearchKind::Keyword) => {
                Ok(Some(Query::Wildcard(WildcardQuery {
                    field: name.to_owned(),
                    pattern: pattern.clone(),
                    case_insensitive: false,
                })))
            }
            (FullTextTerm::Wildcard(_), _) => self.refuse_malformed_value(&format!(
                "wildcard queries need a keyword or text field, and [{name}] is not one"
            )),
            (FullTextTerm::Regexp(pattern), SearchKind::Text) => {
                Ok(Some(build_regexp_query(name, normalize_standard(pattern))))
            }
            (FullTextTerm::Regexp(pattern), SearchKind::Keyword) => Ok(Some(build_regexp_query(name, pattern.clone()))),
            (FullTextTerm::Regexp(_), _) => self.refuse_malformed_value(&format!(
                "regular expressions need a keyword or text field, and [{name}] is not one"
            )),
            (FullTextTerm::Range { lower, upper }, _) => {
                self.build_value_range_query(name, kind, lower.as_ref(), upper.as_ref())
            }
            (FullTextTerm::Exists, _) => Ok(Some(build_exists_query(name))),
        }
    }

    /// The leaf on every value of a MAP column, whose values are keywords.
    fn resolve_map_values(&self, column: &str) -> Result<Query, OpenSearchError> {
        let predicate = match &self.leaf.term {
            FullTextTerm::Text(text) | FullTextTerm::Phrase(text) => MapValuePredicate::Equals(text.clone()),
            FullTextTerm::Prefix { prefix, .. } => MapValuePredicate::StartsWith(prefix.clone()),
            FullTextTerm::Wildcard(pattern) => MapValuePredicate::Matches(translate_wildcard(pattern, false)?),
            FullTextTerm::Regexp(pattern) => MapValuePredicate::Matches(MatchRegex::from_regex(
                translate_lucene_regex(pattern, RegexFlags::ALL, false)?,
            )),
            FullTextTerm::Range { lower, upper } => MapValuePredicate::InRange(KeywordRange {
                lower: lower.as_ref().map(build_keyword_bound),
                upper: upper.as_ref().map(build_keyword_bound),
            }),
            FullTextTerm::Exists => MapValuePredicate::AnyValue,
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

    /// The zone of an unquoted term: only on one explicitly named field.
    const fn read_term_time_zone(&self) -> Option<&TimeZoneSpec> {
        match self.leaf.fields.as_slice() {
            [FieldSelector::Name(_)] => self.leaf.time_zone.as_ref(),
            _ => None,
        }
    }

    /// The whole text as one value of the field: analyzed into terms on a text
    /// field, exact on a keyword, parsed on the typed fields; a date without an
    /// offset is read in `time_zone`.
    fn build_exact_value_query(
        &self,
        name: &str,
        kind: SearchKind,
        text: &str,
        time_zone: Option<&TimeZoneSpec>,
    ) -> Result<Option<Query>, OpenSearchError> {
        match kind {
            SearchKind::Text => Ok(join_terms(
                self.leaf.operator,
                tokenize_standard(text)
                    .into_iter()
                    .map(|token| build_term_query(name, Value::String(token.text)))
                    .collect(),
            )),
            SearchKind::Keyword | SearchKind::EncodedKeyword => {
                Ok(Some(build_term_query(name, Value::String(text.to_owned()))))
            }
            SearchKind::HexIdOrEmpty { .. } if text.is_empty() => {
                Ok(Some(build_term_query(name, Value::String(String::new()))))
            }
            // A `term` leaf carries the value as the request wrote it, and its lowering
            // decodes it for the column; this decode only chooses that leaf or a refusal.
            SearchKind::HexId { width } | SearchKind::HexIdOrEmpty { width } => {
                match decode_fixed_binary_literal(text, &DataType::FixedSizeBinary(width)) {
                    Ok(_) => Ok(Some(build_term_query(name, Value::String(text.to_owned())))),
                    Err(error) => self.refuse_malformed_value(&format!(
                        "[{text}] is not a {width}-byte hex id for [{name}]: {error}"
                    )),
                }
            }
            SearchKind::Integer => self.build_numeric_term_query(name, text, FieldType::Integer),
            SearchKind::Long => self.build_numeric_term_query(name, text, FieldType::Long),
            SearchKind::Double => self.build_numeric_term_query(name, text, FieldType::Double),
            SearchKind::Date => self.build_date_term_query(name, text, time_zone),
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
    /// (`gte X` is `>= X·1000 µs`, `lte Y` is `< (Y+1)·1000 µs`). An
    /// epoch-millisecond term read in a zone is first shifted to that wall-clock time.
    fn build_date_term_query(
        &self,
        name: &str,
        text: &str,
        time_zone: Option<&TimeZoneSpec>,
    ) -> Result<Option<Query>, OpenSearchError> {
        let covered_range = match time_zone {
            Some(zone) if is_epoch_millis_text(text) => shift_epoch_millis(text, zone)
                .ok_or_else(|| format!("[{text}] is not a date for [{name}] in the time zone"))
                .and_then(|shifted| {
                    resolve_date_term(&shifted, None, self.now)
                        .map_err(|error| format!("[{text}] is not a date for [{name}]: {error}"))
                }),
            _ => resolve_date_term(text, time_zone, self.now)
                .map_err(|error| format!("[{text}] is not a date for [{name}]: {error}")),
        };
        covered_range.map_or_else(
            |reason| self.refuse_malformed_value(&reason),
            |(start_micros, end_micros)| Ok(Some(build_millisecond_range(name, start_micros, end_micros))),
        )
    }

    /// A prefix on a text field: the normalized prefix of one token, or, with
    /// `is_analyzed`, the analyzed tokens joined by the operator, the last one a prefix.
    fn build_text_prefix_query(&self, name: &str, prefix: &str, is_analyzed: bool) -> Option<Query> {
        if !is_analyzed {
            return Some(build_prefix_query(name, normalize_standard(prefix)));
        }
        let mut tokens = tokenize_standard(prefix);
        let last_token = tokens.pop()?;
        let mut queries: Vec<Query> = tokens
            .into_iter()
            .map(|token| build_term_query(name, Value::String(token.text)))
            .collect();
        queries.push(build_prefix_query(name, last_token.text));
        join_terms(self.leaf.operator, queries)
    }

    /// A range on one field: normalized bounds per token on a text field,
    /// byte-order bounds on a keyword, parsed bounds on the typed fields.
    fn build_value_range_query(
        &self,
        name: &str,
        kind: SearchKind,
        lower: Option<&FullTextBound>,
        upper: Option<&FullTextBound>,
    ) -> Result<Option<Query>, OpenSearchError> {
        if lower.is_none() && upper.is_none() {
            return Ok(Some(build_exists_query(name)));
        }
        if kind == SearchKind::Date {
            return self.build_date_range_query(name, lower, upper);
        }
        let mut range_bounds = [None, None];
        for (slot, bound) in range_bounds.iter_mut().zip([lower, upper]) {
            let Some(bound) = bound else { continue };
            let value = match kind {
                SearchKind::Text => Some(Value::String(normalize_standard(&bound.text))),
                SearchKind::Keyword | SearchKind::EncodedKeyword => Some(Value::String(bound.text.clone())),
                SearchKind::HexId { width } | SearchKind::HexIdOrEmpty { width } => {
                    decode_fixed_binary_literal(&bound.text, &DataType::FixedSizeBinary(width))
                        .ok()
                        .map(|_| Value::String(bound.text.clone()))
                }
                // The range leaf parses numeric bounds itself; this check only chooses
                // that leaf or a refusal.
                SearchKind::Integer | SearchKind::Long | SearchKind::Double => {
                    parse_java_double(&bound.text).map(|_| Value::String(bound.text.clone()))
                }
                SearchKind::Boolean => parse_boolean_literal(&bound.text).map(Value::Bool),
                SearchKind::Date => None,
            };
            let Some(value) = value else {
                return self
                    .refuse_malformed_value(&format!("[{}] is not a value of the type of [{name}]", bound.text));
            };
            *slot = Some(RangeBound {
                value,
                is_inclusive: bound.is_inclusive,
            });
        }
        let [lower, upper] = range_bounds;
        Ok(Some(build_range_query(name, lower, upper, None, None)))
    }

    /// A date range: each unquoted bound read in the leaf's zone, an
    /// epoch-millisecond bound shifted by that zone's offset first.
    fn build_date_range_query(
        &self,
        name: &str,
        lower: Option<&FullTextBound>,
        upper: Option<&FullTextBound>,
    ) -> Result<Option<Query>, OpenSearchError> {
        let sides = [
            lower.map(|bound| {
                let side = if bound.is_inclusive {
                    BoundSide::LowerInclusive
                } else {
                    BoundSide::LowerExclusive
                };
                (bound, side)
            }),
            upper.map(|bound| {
                let side = if bound.is_inclusive {
                    BoundSide::UpperInclusive
                } else {
                    BoundSide::UpperExclusive
                };
                (bound, side)
            }),
        ];
        let mut readings = [None, None];
        for (slot, side) in readings.iter_mut().zip(sides) {
            let Some((bound, side)) = side else { continue };
            match self.read_date_bound(name, bound, side) {
                Ok(reading) => *slot = Some(reading),
                Err(reason) => return self.refuse_malformed_value(&reason),
            }
        }
        Ok(Some(combine_date_bounds(name, readings)))
    }

    /// One date bound of `name` as the range leaf will read it; `Err` holds why that leaf
    /// could not read it, the parse cause included, so the refusal is made here, under the
    /// leaf's leniency.
    fn read_date_bound(&self, name: &str, bound: &FullTextBound, side: BoundSide) -> Result<DateBoundReading, String> {
        let zone = if bound.is_quoted {
            None
        } else {
            self.leaf.time_zone.as_ref()
        };
        let (text, format, time_zone) = match zone {
            Some(zone) if is_epoch_millis_text(&bound.text) => (
                shift_epoch_millis(&bound.text, zone)
                    .ok_or_else(|| format!("[{}] is not a date for [{name}] in the time zone", bound.text))?,
                Some(DateFormat::EpochMillis),
                None,
            ),
            _ => (bound.text.clone(), choose_date_format(&bound.text), zone.cloned()),
        };
        if let Err(error) = resolve_date_bound(&text, format, time_zone.as_ref(), self.now, side) {
            return Err(format!(
                "[{}] is not a date for [{name}]: {}",
                bound.text,
                error.reason()
            ));
        }
        Ok(DateBoundReading {
            bound: RangeBound {
                value: Value::String(text),
                is_inclusive: bound.is_inclusive,
            },
            format,
            time_zone,
        })
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

/// One date bound as the range leaf reads it.
struct DateBoundReading {
    bound: RangeBound,
    format: Option<DateFormat>,
    time_zone: Option<TimeZoneSpec>,
}

/// One range when both bounds are read alike, otherwise two one-sided ranges that
/// must both hold.
fn combine_date_bounds(name: &str, readings: [Option<DateBoundReading>; 2]) -> Query {
    match readings {
        [Some(lower), Some(upper)] if lower.format == upper.format && lower.time_zone == upper.time_zone => {
            build_range_query(
                name,
                Some(lower.bound),
                Some(upper.bound),
                lower.format,
                lower.time_zone,
            )
        }
        [Some(lower), Some(upper)] => build_bool_query(
            vec![
                build_range_query(name, Some(lower.bound), None, lower.format, lower.time_zone),
                build_range_query(name, None, Some(upper.bound), upper.format, upper.time_zone),
            ],
            Vec::new(),
            Vec::new(),
        ),
        [Some(lower), None] => build_range_query(name, Some(lower.bound), None, lower.format, lower.time_zone),
        [None, Some(upper)] => build_range_query(name, None, Some(upper.bound), upper.format, upper.time_zone),
        [None, None] => build_exists_query(name),
    }
}

fn build_range_query(
    name: &str,
    lower: Option<RangeBound>,
    upper: Option<RangeBound>,
    format: Option<DateFormat>,
    time_zone: Option<TimeZoneSpec>,
) -> Query {
    Query::Range(RangeQuery {
        field: name.to_owned(),
        lower,
        upper,
        format,
        time_zone,
    })
}

/// The inclusive epoch-millisecond range covering the half-open microsecond range
/// `[start_micros, end_micros)`.
fn build_millisecond_range(name: &str, start_micros: i64, end_micros: i64) -> Query {
    build_range_query(
        name,
        Some(RangeBound {
            value: Value::from(start_micros.div_euclid(1000)),
            is_inclusive: true,
        }),
        Some(RangeBound {
            value: Value::from(end_micros.div_euclid(1000) - 1),
            is_inclusive: true,
        }),
        Some(DateFormat::EpochMillis),
        None,
    )
}

/// The format a date range bound is read with: epoch milliseconds for digits (with an
/// optional sign and fraction) that are not a four-digit year, the strict date
/// format for a four-digit year, the default date parsing otherwise.
fn choose_date_format(text: &str) -> Option<DateFormat> {
    if is_epoch_millis_text(text) {
        Some(DateFormat::EpochMillis)
    } else if text.len() == 4 && text.bytes().all(|byte| byte.is_ascii_digit()) {
        Some(DateFormat::StrictDateOptionalTime)
    } else {
        None
    }
}

/// Whether `text` is epoch milliseconds (`-?digits[.digits]`) and not an unsigned
/// four-digit year, which reads as a date.
fn is_epoch_millis_text(text: &str) -> bool {
    let unsigned_text = text.strip_prefix('-').unwrap_or(text);
    let (integer_text, fraction_text) = unsigned_text.split_once('.').unwrap_or((unsigned_text, ""));
    let is_digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    let is_year = integer_text.len() == 4 && fraction_text.is_empty() && unsigned_text.len() == text.len();
    is_digits(integer_text) && (fraction_text.is_empty() || is_digits(fraction_text)) && !is_year
}

/// Epoch milliseconds read as wall-clock time in `zone`: the value minus the
/// zone's offset at that local time, sub-millisecond digits truncated. `None` for
/// a local time the zone skips or a value out of range.
fn shift_epoch_millis(text: &str, zone: &TimeZoneSpec) -> Option<String> {
    let integer_text = text.split_once('.').map_or(text, |(integer_text, _)| integer_text);
    let millis: i64 = integer_text.parse().ok()?;
    let local_time = DateTime::from_timestamp_millis(millis)?.naive_utc();
    let offset_seconds = match zone {
        TimeZoneSpec::Fixed(offset) => offset.local_minus_utc(),
        TimeZoneSpec::Named(named) => named
            .offset_from_local_datetime(&local_time)
            .earliest()?
            .fix()
            .local_minus_utc(),
    };
    millis
        .checked_sub(i64::from(offset_seconds) * 1000)
        .map(|shifted| shifted.to_string())
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

fn build_keyword_bound(bound: &FullTextBound) -> KeywordBound {
    KeywordBound {
        value: bound.text.clone(),
        is_inclusive: bound.is_inclusive,
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

/// A `regexp` leaf of a query-string term: every operator enabled, letter case as written.
fn build_regexp_query(name: &str, pattern: String) -> Query {
    Query::Regexp(RegexpQuery {
        field: name.to_owned(),
        pattern,
        flags: RegexFlags::ALL,
        case_insensitive: false,
    })
}

fn build_exists_query(name: &str) -> Query {
    Query::Exists(ExistsQuery { field: name.to_owned() })
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use chrono::{DateTime, Utc};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use serde_json::{Value, json};

    use super::{
        FieldSelector, FullTextClause, FullTextFields, FullTextLeaf, FullTextQuery, FullTextTerm, LeafSettings, Occur,
        SearchField, SearchKind, build_field_selectors, build_simple_query_tree, resolve_full_text, selects_all_fields,
    };
    use crate::{
        opensearch::error::{OpenSearchError, render_envelope},
        query_dsl::{
            ast::{
                BoolQuery, ExistsQuery, KeywordBound, KeywordRange, MapValuePredicate, MapValuesQuery, Operator,
                PrefixQuery, Query, QueryStringQuery, RangeBound, RangeQuery, RegexpQuery, TermQuery, TokenPhraseQuery,
            },
            date_math::{DateFormat, TimeZoneSpec},
            lucene::{QueryStringError, translate_query_string},
            regex::RegexFlags,
            simple_query::{SimpleQuery, SimpleQueryFlags, parse_simple_query_string},
            udf::MatchRegex,
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
            time_zone: None,
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
    async fn a_malformed_date_range_bound_matches_nothing_under_leniency_and_its_400_states_the_cause() {
        let mut query = build_query_string_options("timestamp:[not-a-date TO *]", None, Operator::Or);
        query.lenient = Some(true);
        assert_eq!(
            try_resolve_query_string(&query).expect("a lenient bound resolves"),
            Some(Query::MatchNone)
        );
        query.lenient = Some(false);
        let refusal_reason = assert_query_shard_400(try_resolve_query_string(&query)).await;
        assert_eq!(
            refusal_reason,
            "[not-a-date] is not a date for [timestamp]: failed to parse date [not-a-date]"
        );
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
                FullTextTerm::Prefix {
                    prefix: String::from("Err"),
                    is_analyzed: false,
                },
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
            FullTextTerm::Prefix {
                prefix: String::from("1"),
                is_analyzed: false,
            },
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
            time_zone: None,
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
                build_clause(
                    Occur::MustNot,
                    FullTextTerm::Prefix {
                        prefix: String::from("d"),
                        is_analyzed: false,
                    }
                ),
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
            time_zone: None,
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

    fn build_query_string_options(text: &str, default_field: Option<&str>, operator: Operator) -> QueryStringQuery {
        QueryStringQuery {
            query: text.to_owned(),
            default_field: default_field.map(str::to_owned),
            fields: Vec::new(),
            default_operator: operator,
            analyze_wildcard: false,
            allow_leading_wildcard: true,
            lenient: None,
            time_zone: None,
        }
    }

    fn try_resolve_query_string(query: &QueryStringQuery) -> Result<Option<Query>, OpenSearchError> {
        try_resolve_query(&translate_query_string(query).expect("the query string translates"))
    }

    fn resolve_query_string(text: &str, default_field: Option<&str>, operator: Operator) -> Option<Query> {
        try_resolve_query_string(&build_query_string_options(text, default_field, operator))
            .expect("the query string resolves")
    }

    const fn build_bool(must: Vec<Query>, should: Vec<Query>, must_not: Vec<Query>) -> Query {
        Query::Bool(BoolQuery {
            must,
            filter: vec![],
            should,
            must_not,
            minimum_should_match: None,
        })
    }

    const fn build_range(
        field: String,
        lower: Option<RangeBound>,
        upper: Option<RangeBound>,
        format: Option<DateFormat>,
        time_zone: Option<TimeZoneSpec>,
    ) -> Query {
        Query::Range(RangeQuery {
            field,
            lower,
            upper,
            format,
            time_zone,
        })
    }

    const fn build_range_bound(value: Value, is_inclusive: bool) -> RangeBound {
        RangeBound { value, is_inclusive }
    }

    fn build_millisecond_range_query(field: &str, first_millis: i64, last_millis: i64) -> Query {
        build_range(
            field.to_owned(),
            Some(build_range_bound(json!(first_millis), true)),
            Some(build_range_bound(json!(last_millis), true)),
            Some(DateFormat::EpochMillis),
            None,
        )
    }

    #[test]
    fn classic_clauses_take_the_stated_occurrences() {
        let build_level_term = |value: &str| build_term("severity_text", json!(value));
        let rows = [
            (
                "a OR b AND c",
                Operator::Or,
                build_bool(
                    vec![build_level_term("b"), build_level_term("c")],
                    vec![build_level_term("a")],
                    vec![],
                ),
            ),
            (
                "a AND b OR c",
                Operator::Or,
                build_bool(
                    vec![build_level_term("a"), build_level_term("b")],
                    vec![build_level_term("c")],
                    vec![],
                ),
            ),
            (
                "a b OR c",
                Operator::Or,
                build_bool(
                    vec![],
                    vec![build_level_term("a"), build_level_term("b"), build_level_term("c")],
                    vec![],
                ),
            ),
            (
                "+a OR b",
                Operator::Or,
                build_bool(vec![build_level_term("a")], vec![build_level_term("b")], vec![]),
            ),
            (
                "-a OR b",
                Operator::Or,
                build_bool(vec![], vec![build_level_term("b")], vec![build_level_term("a")]),
            ),
            (
                "a OR b AND c",
                Operator::And,
                build_bool(
                    vec![build_level_term("b"), build_level_term("c")],
                    vec![build_level_term("a")],
                    vec![],
                ),
            ),
            (
                "a AND b OR c",
                Operator::And,
                build_bool(
                    vec![build_level_term("a")],
                    vec![build_level_term("b"), build_level_term("c")],
                    vec![],
                ),
            ),
            (
                "a b OR c",
                Operator::And,
                build_bool(
                    vec![build_level_term("a")],
                    vec![build_level_term("b"), build_level_term("c")],
                    vec![],
                ),
            ),
            (
                "+a OR b",
                Operator::And,
                build_bool(vec![], vec![build_level_term("a"), build_level_term("b")], vec![]),
            ),
            (
                "-a OR b",
                Operator::And,
                build_bool(vec![], vec![build_level_term("b")], vec![build_level_term("a")]),
            ),
            // A keyword compares the multi-term group's whole text.
            ("foo bar", Operator::Or, build_level_term("foo bar")),
        ];
        for (text, operator, expected) in rows {
            assert_eq!(
                resolve_query_string(text, Some("severity_text"), operator),
                Some(expected),
                "[{text}] under {operator:?}"
            );
        }
    }

    #[test]
    fn zero_token_clauses_are_dropped_and_emptied_groups_removed() {
        // Under `*`: `a` is a token of `body`, a whole value of `severity_text` and of every
        // MAP value; the typed fields cannot parse it and add nothing.
        let build_star_a = || {
            build_bool(
                vec![],
                vec![
                    build_term("body", json!("a")),
                    build_term("severity_text", json!("a")),
                    build_map_values_query(MapValuePredicate::Equals(String::from("a"))),
                ],
                vec![],
            )
        };
        let rows = [
            ("body:\"!!!\"", None),
            ("-body:\"!!!\"", None),
            ("a AND body:\"!!!\"", Some(build_star_a())),
            ("a AND (body:\"!!!\")", Some(build_star_a())),
            // The dropped clause takes no part in the occurrence rules: `AND` makes `a` required.
            (
                "a OR body:\"!!!\" AND severity_text:c",
                Some(build_bool(
                    vec![build_star_a(), build_term("severity_text", json!("c"))],
                    vec![],
                    vec![],
                )),
            ),
            // An unqualified empty phrase is an empty whole value on the keyword-like fields.
            (
                "\"\"",
                Some(build_bool(
                    vec![],
                    vec![
                        build_term("severity_text", json!("")),
                        build_map_values_query(MapValuePredicate::Equals(String::new())),
                    ],
                    vec![],
                )),
            ),
        ];
        for (text, expected) in rows {
            assert_eq!(resolve_query_string(text, None, Operator::Or), expected, "[{text}]");
        }
    }

    #[test]
    fn boolean_literals_under_the_star_default_match_as_stated() {
        let build_equals = |value: &str| build_map_values_query(MapValuePredicate::Equals(value.to_owned()));
        let rows = [
            (
                "true",
                build_bool(
                    vec![],
                    vec![
                        build_term("body", json!("true")),
                        build_term("flag", json!(true)),
                        build_term("severity_text", json!("true")),
                        build_equals("true"),
                    ],
                    vec![],
                ),
            ),
            // `TRUE` is no boolean literal: it matches the text and keyword fields only.
            (
                "TRUE",
                build_bool(
                    vec![],
                    vec![
                        build_term("body", json!("true")),
                        build_term("severity_text", json!("TRUE")),
                        build_equals("TRUE"),
                    ],
                    vec![],
                ),
            ),
            // One malformed value under leniency.
            ("flag:(true false)", Query::MatchNone),
            (
                "flag:(true OR false)",
                build_bool(
                    vec![],
                    vec![build_term("flag", json!(true)), build_term("flag", json!(false))],
                    vec![],
                ),
            ),
            // `must_not` keeps the rows without `flag` (lowered as NOT coalesce(p, false)).
            (
                "-flag:true",
                build_bool(vec![], vec![], vec![build_term("flag", json!(true))]),
            ),
            (
                "flag:[false TO true}",
                build_range(
                    String::from("flag"),
                    Some(build_range_bound(json!(false), true)),
                    Some(build_range_bound(json!(true), false)),
                    None,
                    None,
                ),
            ),
            // Prefix on a boolean is wrong-typed: nothing under leniency.
            ("flag:t*", Query::MatchNone),
        ];
        for (text, expected) in rows {
            assert_eq!(
                resolve_query_string(text, None, Operator::Or),
                Some(expected),
                "[{text}]"
            );
        }
    }

    /// Field-qualified date terms and ranges in `dubai` (UTC+4) and what each resolves to.
    fn list_named_date_rows(dubai: &TimeZoneSpec) -> Vec<(&'static str, Query)> {
        let timestamp = || String::from("timestamp");
        let build_text_bound = |value: &str, is_inclusive| Some(build_range_bound(json!(value), is_inclusive));
        vec![
            // The local day 2026-10-08 in UTC+4: 2026-10-07T20:00:00Z (1791403200000 ms)
            // up to 2026-10-08T20:00:00Z (1791489600000 ms).
            (
                "timestamp:2026-10-08",
                build_millisecond_range_query("timestamp", 1_791_403_200_000, 1_791_489_599_999),
            ),
            // Quoted: never shifted, the UTC day.
            (
                "timestamp:\"2026-10-08\"",
                build_millisecond_range_query("timestamp", 1_791_417_600_000, 1_791_503_999_999),
            ),
            // An explicit `Z` is not shifted: one second from 2026-10-08T12:00:00Z.
            (
                "timestamp:2026-10-08T12\\:00\\:00Z",
                build_millisecond_range_query("timestamp", 1_791_460_800_000, 1_791_460_800_999),
            ),
            // An epoch-millisecond term is read as Dubai wall-clock time: 1791460800000 - 14400000.
            (
                "timestamp:1791460800000",
                build_millisecond_range_query("timestamp", 1_791_446_400_000, 1_791_446_400_000),
            ),
            (
                "timestamp:[2026-10-08 TO 2026-10-09]",
                build_range(
                    timestamp(),
                    build_text_bound("2026-10-08", true),
                    build_text_bound("2026-10-09", true),
                    None,
                    Some(dubai.clone()),
                ),
            ),
            (
                "timestamp:[1791460800000 TO *]",
                build_range(
                    timestamp(),
                    build_text_bound("1791446400000", true),
                    None,
                    Some(DateFormat::EpochMillis),
                    None,
                ),
            ),
            (
                "timestamp:[\"2026-10-08\" TO *]",
                build_range(timestamp(), build_text_bound("2026-10-08", true), None, None, None),
            ),
            (
                "timestamp:[2026 TO *]",
                build_range(
                    timestamp(),
                    build_text_bound("2026", true),
                    None,
                    Some(DateFormat::StrictDateOptionalTime),
                    Some(dubai.clone()),
                ),
            ),
            (
                "timestamp:[1791460800000 TO 2026-10-09]",
                build_bool(
                    vec![
                        build_range(
                            timestamp(),
                            build_text_bound("1791446400000", true),
                            None,
                            Some(DateFormat::EpochMillis),
                            None,
                        ),
                        build_range(
                            timestamp(),
                            None,
                            build_text_bound("2026-10-09", true),
                            None,
                            Some(dubai.clone()),
                        ),
                    ],
                    vec![],
                    vec![],
                ),
            ),
        ]
    }

    #[test]
    fn time_zone_shifts_unquoted_date_terms_and_ranges_as_stated() {
        let dubai = TimeZoneSpec::Named(chrono_tz::Asia::Dubai);
        let resolve_in_dubai = |text: &str| {
            let mut query = build_query_string_options(text, None, Operator::Or);
            query.time_zone = Some(dubai.clone());
            try_resolve_query_string(&query).expect("resolves")
        };
        let build_text_bound = |value: &str, is_inclusive| Some(build_range_bound(json!(value), is_inclusive));
        for (text, expected) in list_named_date_rows(&dubai) {
            assert_eq!(resolve_in_dubai(text), Some(expected), "[{text}]");
        }
        // Under `*` an unquoted term is not read in the zone (the UTC day), a range bound is.
        assert_eq!(
            resolve_in_dubai("2026-10-08"),
            Some(build_bool(
                vec![],
                vec![
                    build_bool(
                        vec![],
                        vec![
                            build_term("body", json!("2026")),
                            build_term("body", json!("10")),
                            build_term("body", json!("08")),
                        ],
                        vec![],
                    ),
                    build_term("severity_text", json!("2026-10-08")),
                    build_millisecond_range_query("timestamp", 1_791_417_600_000, 1_791_503_999_999),
                    build_map_values_query(MapValuePredicate::Equals(String::from("2026-10-08"))),
                ],
                vec![],
            ))
        );
        assert_eq!(
            resolve_in_dubai("[2026-10-08 TO *]"),
            Some(build_bool(
                vec![],
                vec![
                    build_range(
                        String::from("body"),
                        build_text_bound("2026-10-08", true),
                        None,
                        None,
                        None
                    ),
                    build_range(
                        String::from("severity_text"),
                        build_text_bound("2026-10-08", true),
                        None,
                        None,
                        None
                    ),
                    build_range(
                        String::from("timestamp"),
                        build_text_bound("2026-10-08", true),
                        None,
                        None,
                        Some(dubai.clone())
                    ),
                    build_map_values_query(MapValuePredicate::InRange(KeywordRange {
                        lower: Some(KeywordBound {
                            value: String::from("2026-10-08"),
                            is_inclusive: true,
                        }),
                        upper: None,
                    })),
                ],
                vec![],
            ))
        );
    }

    #[test]
    fn an_unqualified_range_is_an_or_of_per_field_ranges_in_each_fields_order() {
        let build_ten_range = |field: &str| {
            build_range(
                field.to_owned(),
                Some(build_range_bound(json!("10"), false)),
                None,
                None,
                None,
            )
        };
        assert_eq!(
            resolve_query_string(">10", None, Operator::Or),
            Some(build_bool(
                vec![],
                vec![
                    build_ten_range("body"),
                    build_ten_range("count"),
                    build_ten_range("ratio"),
                    build_ten_range("severity_text"),
                    build_ten_range("status"),
                    build_range(
                        String::from("timestamp"),
                        Some(build_range_bound(json!("10"), false)),
                        None,
                        Some(DateFormat::EpochMillis),
                        None
                    ),
                    build_map_values_query(MapValuePredicate::InRange(KeywordRange {
                        lower: Some(KeywordBound {
                            value: String::from("10"),
                            is_inclusive: false,
                        }),
                        upper: None,
                    })),
                ],
                vec![],
            ))
        );
    }

    #[tokio::test]
    async fn wildcard_and_prefix_terms_on_an_id_field_are_a_400_and_match_nothing_under_lenient() {
        for text in ["trace_id:ab*", "trace_id:a?c"] {
            assert_eq!(
                resolve_query_string(text, None, Operator::Or),
                Some(Query::MatchNone),
                "[{text}] under leniency"
            );
            assert_query_shard_400(try_resolve_query_string(&build_query_string_options(
                text,
                Some("body"),
                Operator::Or,
            )))
            .await;
        }
    }

    #[tokio::test]
    async fn regexp_terms_resolve_per_field_type() {
        let build_regexp = |field: &str, pattern: &str| {
            Query::Regexp(RegexpQuery {
                field: field.to_owned(),
                pattern: pattern.to_owned(),
                flags: RegexFlags::ALL,
                case_insensitive: false,
            })
        };
        // On a text field the pattern is lowercased by the analyzer's normalizer.
        assert_eq!(
            resolve_query_string("body:/Conn.*/", None, Operator::Or),
            Some(build_regexp("body", "conn.*"))
        );
        // On a keyword, a MAP key among them, it is kept verbatim.
        assert_eq!(
            resolve_query_string("severity_text:/ERR.*/", None, Operator::Or),
            Some(build_regexp("severity_text", "ERR.*"))
        );
        assert_eq!(
            resolve_query_string("log_attributes.http.method:/GE./", None, Operator::Or),
            Some(build_regexp("log_attributes.http.method", "GE."))
        );
        // On every MAP value it is matched whole, verbatim.
        assert_eq!(
            resolve_query_string("log_attributes.\\*:/GE./", None, Operator::Or),
            Some(build_map_values_query(MapValuePredicate::Matches(
                MatchRegex::from_regex(::regex::Regex::new("^(?s:GE.)$").expect("compiles"))
            )))
        );
        // A malformed pattern on the MAP values is the dialect's 400, lenient or not.
        let error = try_resolve_query_string(&build_query_string_options(
            "log_attributes.\\*:/(a/",
            None,
            Operator::Or,
        ))
        .expect_err("a malformed pattern is refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        // On an id, a number, a date or a boolean: nothing under leniency, a query-shard 400
        // otherwise.
        for text in [
            "trace_id:/ab.*/",
            "count:/1.*/",
            "ratio:/1.*/",
            "status:/1.*/",
            "timestamp:/2026.*/",
            "flag:/tr.*/",
        ] {
            assert_eq!(
                resolve_query_string(text, None, Operator::Or),
                Some(Query::MatchNone),
                "[{text}] under leniency"
            );
            assert_query_shard_400(try_resolve_query_string(&build_query_string_options(
                text,
                Some("body"),
                Operator::Or,
            )))
            .await;
        }
    }

    #[test]
    fn star_and_exists_resolve_as_stated() {
        assert_eq!(resolve_query_string("*", None, Operator::Or), Some(Query::MatchAll));
        assert_eq!(
            resolve_query_string("_exists_:*", None, Operator::Or),
            Some(Query::MatchNone)
        );
        assert_eq!(
            resolve_query_string("_exists_:body", None, Operator::Or),
            Some(Query::Exists(ExistsQuery {
                field: String::from("body")
            }))
        );
        assert_eq!(
            resolve_query_string("_exists_:log_attributes.\\*", None, Operator::Or),
            Some(build_map_values_query(MapValuePredicate::AnyValue))
        );
        // Unescaped, the trailing `*` is part of a literal name: the MAP key named `*`.
        assert_eq!(
            resolve_query_string("_exists_:log_attributes.*", None, Operator::Or),
            Some(Query::Exists(ExistsQuery {
                field: String::from("log_attributes.*")
            }))
        );
        assert_eq!(
            resolve_query_string("body:Err*", None, Operator::Or),
            Some(build_prefix("body", "err"))
        );
    }

    /// Fragments the query-string generator joins: terms (ASCII, non-ASCII, numbers, a date, an
    /// address, a boolean, hex), field prefixes (typed, a MAP key, unmapped, two patterns,
    /// `_exists_`), operators, quotes, brackets, escapes (a truncated `\u` among them), `~`, `^`,
    /// wildcards, a regular expression and whitespace.
    const QUERY_STRING_FRAGMENTS: &[&str] = &[
        "a",
        "Err",
        "café",
        "10",
        "-3.5",
        "2026-10-08",
        "user@example.com",
        "true",
        "ab12",
        "body:",
        "count:",
        "flag:",
        "severity_text:",
        "timestamp:",
        "trace_id:",
        "log_attributes.http.method:",
        "missing:",
        "*:",
        "log_attributes.\\*:",
        "_exists_:",
        " AND ",
        " OR ",
        " NOT ",
        " && ",
        " || ",
        "!",
        "+",
        "-",
        "\"",
        "\"a b\"",
        "(",
        ")",
        "[",
        "]",
        "{",
        "}",
        " TO ",
        "\\",
        "\\:",
        "\\*",
        "\\u00e9",
        "\\u12",
        "~",
        "~2",
        "^",
        "^2",
        "*",
        "?",
        "/",
        "/ERR.*/",
        ":",
        ">",
        ">=",
        "<",
        "<=",
        " ",
        "\t",
    ];

    /// The fields of [`StatedFields`], with every pattern selecting all of them: a generated
    /// query string writes patterns no index states, and only the absence of a panic and of a
    /// 500 is checked over it.
    struct GeneratedFields {
        stated_fields: StatedFields,
    }

    impl FullTextFields for GeneratedFields {
        fn index_name(&self) -> &str {
            self.stated_fields.index_name()
        }

        fn expand_selector(&self, selector: &FieldSelector) -> Result<Vec<SearchField>, OpenSearchError> {
            match selector {
                FieldSelector::Name(_) => self.stated_fields.expand_selector(selector),
                FieldSelector::Pattern(_) => {
                    self.stated_fields.expand_selector(&FieldSelector::Pattern(String::from("*")))
                }
            }
        }
    }

    #[test]
    fn generated_query_strings_translate_or_fail_as_a_400_and_never_panic() {
        // A failing input is kept as a literal row of the visitor or resolver tests above, and
        // the visitor or the resolver is fixed; the generator stays as it is.
        let generated_fields = GeneratedFields {
            stated_fields: StatedFields { index: "logs" },
        };
        let mut generator = StdRng::seed_from_u64(0x5EED_0C1A_551C);
        for _ in 0..5_000 {
            let text: String = (0..generator.random_range(1..=12))
                .map(|_| QUERY_STRING_FRAGMENTS[generator.random_range(0..QUERY_STRING_FRAGMENTS.len())])
                .collect();
            let mut query = build_query_string_options(&text, None, Operator::Or);
            if generator.random_bool(0.5) {
                query.default_operator = Operator::And;
            }
            if generator.random_bool(0.3) {
                query.default_field = Some(String::from("body"));
            }
            query.analyze_wildcard = generator.random_bool(0.5);
            query.allow_leading_wildcard = generator.random_bool(0.7);
            if generator.random_bool(0.2) {
                query.lenient = Some(false);
            }
            if generator.random_bool(0.3) {
                query.time_zone = Some(TimeZoneSpec::Named(chrono_tz::America::New_York));
            }
            match translate_query_string(&query) {
                Ok(tree) => {
                    if let Err(error) = resolve_full_text(&tree, &generated_fields, parse_request_instant()) {
                        assert_eq!(
                            error.status(),
                            StatusCode::BAD_REQUEST,
                            "[{text}] resolves to {error:?}"
                        );
                    }
                }
                Err(QueryStringError::Invalid(reason)) => {
                    assert!(!reason.is_empty(), "[{text}] fails without a reason");
                }
                Err(QueryStringError::Refused(error)) => {
                    assert_eq!(
                        error.status(),
                        StatusCode::BAD_REQUEST,
                        "[{text}] is refused as {error:?}"
                    );
                }
            }
        }
    }
}
