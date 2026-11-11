//! In-memory evaluation of the Query DSL against system-index documents, for the store's
//! `_count` and `_search`.
//!
//! A field's type comes from the index's stored mapping (`keyword`, `text`, `date`, `integer`,
//! `long`, `float`, `double`, `boolean`, `object`, `nested`, and multi-fields such as
//! `title.keyword`). A path the mapping does not declare, under a `dynamic` object or absent,
//! is typed from the document's own values: a string is `text` with a `keyword` subfield
//! (`<path>.keyword`), a JSON integer `long`, another number `float`, a boolean `boolean`, an
//! object `object`. A path with neither a mapping nor a value matches nothing. A mapping type
//! outside that list is a 400 when a query names it.
//!
//! Numeric values compare numerically (a numeric string counts as a number), and two integers
//! compare exactly; booleans match `true` / `false` or their strings; dates compare at
//! millisecond precision, the precision IceGate gives `date` values; `case_insensitive` folds
//! ASCII letters only. `ids` and `_id` match the document id.
//!
//! `text` fields are matched through the standard analyzer, token by token, and each array
//! element is analyzed on its own: a `term` or `terms` value is not analyzed and matches when it
//! equals one token of a stored value (`Logs` never matches the token `logs`); a `prefix` or
//! `wildcard` pattern is lowercased the way the analyzer lowercases, without splitting, and
//! matches when one token matches it; a `range` matches when one token lies within it, compared
//! as strings; `exists` holds for any present value, the empty string included.
//!
//! The full-text leaves evaluate each field by its kind. On a `text` field the query text is
//! analyzed: a `match` needs one of its tokens (`or`) or all of them (`and`) in one stored value; a
//! phrase needs its tokens at consecutive positions of one stored value; a phrase prefix also lets
//! its last token prefix the stored token, and every token with that prefix counts; a trailing-`*`
//! term of `simple_query_string` is a prefix of one token, lowercased like the analyzer. On a
//! `keyword` field and on `_id` the whole text equals (or, for a prefix term, starts) one stored
//! value. On a number, date or boolean field the whole text is a typed term, and a text that does
//! not parse, or a prefix term, fails the query on its index unless `lenient`, then it matches
//! nothing. `match_phrase_prefix` fails the same way on all of these but `text`, whatever `lenient`
//! says.
//!
//! A text that yields no token contributes no clause: a `match` then follows `zero_terms_query`, a
//! phrase matches nothing, and inside `simple_query_string` the clause drops out of its group (a
//! group left with nothing drops out too). `multi_match` and `simple_query_string` match when one
//! field matches, and with the fields `*` (or absent) `lenient` defaults to true. `*` and other
//! patterns name the document's leaf paths outside `nested` fields, outside what the mapping does not
//! index (`index: false`, or under an object with `enabled: false`) and outside fields of a type the
//! evaluator does not support; the multi-fields the mapping declares on them (unless themselves
//! `index: false` or of such a type); and the `keyword` subfield of an undeclared string. Inside a
//! `nested` query they name the paths of the current element instead of the document's, and nothing
//! when a step of the query's path is under `enabled: false` or of such a type.
//!
//! What a leaf needs from the query alone (a `term` value or `range` bounds read for a field's
//! kind, a translated `regexp` pattern, and what the mapping declares for a path) is prepared once
//! per request and index, in [`IndexQueryEvaluator`], and reused for every document. A `term`,
//! `terms` or `range` on `_id` or on a field the mapping declares is prepared before any document
//! is read, so a value that does not fit the field is refused whatever the index holds and
//! whichever clause a document reached first; on an undeclared path the kind comes from the
//! document, so the value is read, and refused, only by a document that reaches it. A tree a
//! `query_string` resolves for one document lives only for that document, so its `term` and
//! `range` leaves are read per document.

use std::borrow::Cow;
use std::cell::{OnceCell, RefCell};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use std::rc::Rc;

use chrono::{DateTime, Utc};
use regex::Regex;
use serde_json::Value;

use crate::opensearch::error::OpenSearchError;
use crate::opensearch::glob::matches_glob;
use crate::query_dsl::analyzer::{Token, contains_token_phrase, normalize_standard, tokenize_standard};
use crate::query_dsl::ast::{
    BoolQuery, MatchQuery, MultiMatchQuery, MultiMatchType, NestedQuery, Operator, Query, QueryStringClauses,
    RangeBound, RangeQuery, RegexpQuery, SimpleQueryStringQuery, TokenPhraseQuery, TokenSetQuery, ZeroTermsQuery,
};
use crate::query_dsl::date_math::{BoundSide, parse_stored_date_millis, resolve_date_bound};
use crate::query_dsl::full_text::{
    FieldSelector, FullTextFields, FullTextQuery, SearchField, SearchKind, resolve_full_text, selects_all_fields,
};
use crate::query_dsl::regex::{RegexFlags, translate_lucene_regex};
use crate::query_dsl::simple_query::SimpleQuery;

/// The metadata field naming a document's id.
pub(crate) const ID_FIELD: &str = "_id";

/// The subfield an inferred `text` value carries for exact matching.
const KEYWORD_SUBFIELD_SUFFIX: &str = ".keyword";

/// How the evaluator compares a field's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FieldKind {
    /// The document id (`_id`).
    Identifier,
    /// Exact, case-sensitive strings.
    Keyword,
    /// Analyzed strings.
    Text,
    /// Instants at millisecond precision.
    Date,
    /// `integer` and `long`, and inferred JSON integers.
    Integer,
    /// `float` and `double`, and inferred JSON non-integers.
    Float,
    /// `true` and `false`.
    Boolean,
    /// An object; its leaves are fields of their own.
    Object,
    /// An array of objects queried element by element through `nested`.
    Nested,
}

impl FieldKind {
    /// The kind of a mapping `type`.
    ///
    /// # Errors
    ///
    /// [`OpenSearchError::unqueryable_system_field`] for any other type.
    pub(crate) fn from_mapping_type(type_name: &str, field: &str) -> Result<Self, OpenSearchError> {
        match type_name {
            "keyword" => Ok(Self::Keyword),
            "text" => Ok(Self::Text),
            "date" => Ok(Self::Date),
            "integer" | "long" => Ok(Self::Integer),
            "float" | "double" => Ok(Self::Float),
            "boolean" => Ok(Self::Boolean),
            "object" => Ok(Self::Object),
            "nested" => Ok(Self::Nested),
            other => Err(OpenSearchError::unqueryable_system_field(field, other)),
        }
    }
}

/// What a mapping declares for a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeclaredFieldType {
    /// The declared kind.
    pub(crate) kind: FieldKind,
    /// Whether the path names a multi-field (`title.keyword`), whose values are its parent's.
    pub(crate) is_multi_field: bool,
}

impl DeclaredFieldType {
    /// The path holding the values of `field`, the path this type was declared for: its parent for
    /// a multi-field, `field` itself otherwise.
    pub(crate) fn resolve_source_path(self, field: &str) -> &str {
        if self.is_multi_field {
            field.rsplit_once('.').map_or(field, |(parent, _)| parent)
        } else {
            field
        }
    }
}

/// A request's query prepared for the documents of one index (see the module documentation):
/// built once per request and index, then asked about each document.
pub(crate) struct IndexQueryEvaluator<'q> {
    /// The request's query, whose leaves key the prepared values by their address: it outlives
    /// the evaluator, so an address names one leaf for every document.
    query: &'q Query,
    /// The concrete index, named by query-shard errors.
    index: &'q str,
    /// The index's mappings.
    mappings: &'q Value,
    /// The request's instant, for date math.
    now: DateTime<Utc>,
    /// What the leaves of `query` need, prepared once for every document.
    preparations: LeafPreparations,
}

impl<'q> IndexQueryEvaluator<'q> {
    /// `query` prepared for the index `index` with `mappings`, at the request's instant `now`.
    /// Every `term`, `terms` and `range` on `_id` or on a declared field is read here, before any
    /// document.
    ///
    /// # Errors
    ///
    /// For a `term`, `terms` or `range` on `_id` or on a field `mappings` declares, the refusal
    /// [`Self::matches_document`] would answer for its value on any document (whether or not a
    /// document reaches the leaf), and `illegal_argument_exception` for such a field of a type the
    /// evaluator does not support.
    pub(crate) fn new(
        query: &'q Query,
        index: &'q str,
        mappings: &'q Value,
        now: DateTime<Utc>,
    ) -> Result<Self, OpenSearchError> {
        let evaluator = Self {
            query,
            index,
            mappings,
            now,
            preparations: LeafPreparations::default(),
        };
        evaluator.prepare_declared_leaves(query)?;
        Ok(evaluator)
    }

    /// Whether the document `id` with `source` matches the query.
    ///
    /// # Errors
    ///
    /// - `search_phase_execution_exception` wrapping a `query_shard_exception` on the index for a
    ///   `term` or `terms` value that does not parse for its field's type; for a full-text text
    ///   that does not, or a `simple_query_string` prefix term on a number, date or boolean field,
    ///   without `lenient`; and for a `match_phrase_prefix` on a keyword, `_id`, number, date or
    ///   boolean field.
    /// - `illegal_argument_exception` for a `range` value that does not parse for its field's type,
    ///   a `prefix` or `wildcard` on a non-string field, or a mapping type the evaluator does not
    ///   support.
    pub(crate) fn matches_document(&self, id: &str, source: &Value) -> Result<bool, OpenSearchError> {
        let analyzed_values = AnalyzedValues::default();
        DocumentView {
            index: self.index,
            id,
            source,
            mappings: self.mappings,
            now: self.now,
            nested: None,
            analyzed_values: &analyzed_values,
            preparations: &self.preparations,
            has_stable_leaves: true,
        }
        .matches_query(self.query)
    }

    /// Prepares every `term`, `terms` and `range` value under `query` whose field is `_id` or
    /// declared, answering the first refusal, in the order a document evaluates the clauses.
    fn prepare_declared_leaves(&self, query: &'q Query) -> Result<(), OpenSearchError> {
        match query {
            Query::Term(term) => self.prepare_declared_term_values(&term.field, std::slice::from_ref(&term.value)),
            Query::Terms(terms) => self.prepare_declared_term_values(&terms.field, &terms.values),
            Query::Range(range) => {
                let Some(kind) = self.find_declared_kind(&range.field)? else {
                    return Ok(());
                };
                self.preparations
                    .find_range_operand(range, kind, || build_range_operand(range, kind, self.now))
                    .map(drop)
            }
            Query::Bool(bool_query) => bool_query
                .must
                .iter()
                .chain(&bool_query.filter)
                .chain(&bool_query.must_not)
                .chain(&bool_query.should)
                .try_for_each(|clause| self.prepare_declared_leaves(clause)),
            Query::ConstantScore(inner) => self.prepare_declared_leaves(inner),
            Query::Nested(nested) => self.prepare_declared_leaves(&nested.query),
            Query::MatchAll
            | Query::MatchNone
            | Query::Ids(_)
            | Query::Exists(_)
            | Query::Prefix(_)
            | Query::Wildcard(_)
            | Query::Regexp(_)
            | Query::Match(_)
            | Query::MatchPhrase(_)
            | Query::MatchPhrasePrefix(_)
            | Query::MultiMatch(_)
            | Query::SimpleQueryString(_)
            | Query::LuceneQueryString(_)
            | Query::MapValues(_)
            | Query::TokenPhrase(_)
            | Query::TokenSet(_) => Ok(()),
        }
    }

    /// Prepares each of `values` for `field` when its kind does not depend on a document.
    fn prepare_declared_term_values(&self, field: &str, values: &[Value]) -> Result<(), OpenSearchError> {
        let Some(kind) = self.find_declared_kind(field)? else {
            return Ok(());
        };
        values.iter().try_for_each(|value| {
            self.preparations
                .find_term_operand(value, kind, || build_term_operand(field, value, kind, self.now))
                .map(drop)
                .map_err(|_| refuse_invalid_value(self.index, field, value))
        })
    }

    /// The kind of `field` when no document decides it: `_id`, or a declared field.
    fn find_declared_kind(&self, field: &str) -> Result<Option<FieldKind>, OpenSearchError> {
        if field == ID_FIELD {
            return Ok(Some(FieldKind::Identifier));
        }
        Ok(self
            .preparations
            .find_mapped_field(self.mappings, field)?
            .map(|declared_type| declared_type.kind))
    }
}

/// What the leaves of one request's query need on one index, prepared by the first document that
/// needs it and reused by every later one. `term` values and `range` bounds are keyed by the leaf's
/// address with the kind they were read for: the query outlives the preparations, so an address
/// names one leaf, and an undeclared path can take another kind in another document. Patterns and
/// paths are keyed by their text. Each map grows with the query (the paths, also with the paths
/// the documents' own leaves offer to a field pattern), never with the number of documents.
#[derive(Default)]
struct LeafPreparations {
    /// What the mappings declare for each path asked about.
    mapped_fields: RefCell<HashMap<String, Result<Option<DeclaredFieldType>, OpenSearchError>>>,
    /// `term` and `terms` values read for a field kind, by the value's address.
    term_operands: RefCell<HashMap<(*const Value, FieldKind), Result<Rc<TermOperand<'static>>, OpenSearchError>>>,
    /// `range` bounds read for a field kind, by the leaf's address.
    range_operands:
        RefCell<HashMap<(*const RangeQuery, FieldKind), Result<Rc<RangeOperand<'static>>, OpenSearchError>>>,
    /// `regexp` translations, by the optional operators and the case sensitivity they were
    /// translated with, then by pattern. Two levels, so a lookup borrows the query's pattern
    /// instead of copying it into a key.
    regex_translations: RefCell<HashMap<(RegexFlags, bool), Rc<RegexTranslations>>>,
}

/// The translations of `regexp` patterns for one set of optional operators and case sensitivity:
/// the anchored pattern, or the refusal of the translation, by pattern.
type RegexTranslations = RefCell<HashMap<String, Result<Rc<Regex>, OpenSearchError>>>;

impl LeafPreparations {
    /// What `mappings` declares for `path` (see [`find_mapped_field`]).
    fn find_mapped_field(&self, mappings: &Value, path: &str) -> Result<Option<DeclaredFieldType>, OpenSearchError> {
        find_or_insert_memo_entry(&self.mapped_fields, path, || find_mapped_field(mappings, path))
    }

    /// The `term` value at `value` read for `kind`, built by `build` the first time.
    fn find_term_operand<'v>(
        &self,
        value: &'v Value,
        kind: FieldKind,
        build: impl FnOnce() -> Result<TermOperand<'v>, OpenSearchError>,
    ) -> Result<Rc<TermOperand<'static>>, OpenSearchError> {
        find_or_insert_memo_entry(&self.term_operands, &(std::ptr::from_ref(value), kind), || {
            build().map(|operand| Rc::new(operand.into_owned()))
        })
    }

    /// The bounds of the `range` at `range` read for `kind`, built by `build` the first time.
    fn find_range_operand<'r>(
        &self,
        range: &'r RangeQuery,
        kind: FieldKind,
        build: impl FnOnce() -> Result<RangeOperand<'r>, OpenSearchError>,
    ) -> Result<Rc<RangeOperand<'static>>, OpenSearchError> {
        find_or_insert_memo_entry(&self.range_operands, &(std::ptr::from_ref(range), kind), || {
            build().map(|operand| Rc::new(operand.into_owned()))
        })
    }

    /// The translation of `query`'s pattern (see [`translate_lucene_regex`]), the first time it
    /// is asked for with its flags and case sensitivity.
    fn translate_regexp(&self, query: &RegexpQuery) -> Result<Rc<Regex>, OpenSearchError> {
        let translations = find_or_insert_memo_entry(
            &self.regex_translations,
            &(query.flags, query.case_insensitive),
            Rc::default,
        );
        find_or_insert_memo_entry(&translations, query.pattern.as_str(), || {
            translate_lucene_regex(&query.pattern, query.flags, query.case_insensitive).map(Rc::new)
        })
    }
}

/// The value `memo` holds for `key`, inserting what `compute_value` answers the first time. The
/// lookup borrows `key`; only a miss copies it into the map.
fn find_or_insert_memo_entry<Q, V>(
    memo: &RefCell<HashMap<Q::Owned, V>>,
    key: &Q,
    compute_value: impl FnOnce() -> V,
) -> V
where
    Q: ?Sized + Eq + Hash + ToOwned,
    Q::Owned: Eq + Hash,
    V: Clone,
{
    // Bound to a local so the shared borrow ends before the insert below borrows mutably, and
    // `compute_value` runs with no borrow held, free to use another memo.
    let memoized_value = memo.borrow().get(key).cloned();
    if let Some(value) = memoized_value {
        return value;
    }
    let value = compute_value();
    memo.borrow_mut().insert(key.to_owned(), value.clone());
    value
}

/// A `term` value read for the kind of the field it is compared with.
#[derive(Debug, Clone)]
enum TermOperand<'v> {
    /// A value no stored value equals: on an object or a `nested` field, or one without a text
    /// form on `_id`.
    Unmatchable,
    /// The text the document id must equal.
    IdentifierText(Cow<'v, str>),
    /// The text a whole keyword value must equal.
    WholeText(Cow<'v, str>),
    /// The text one token of a text value must equal.
    TokenText(Cow<'v, str>),
    /// The number a numeric value must equal.
    Number(ComparableNumber),
    /// The boolean a boolean value must equal.
    Flag(bool),
    /// The instants a date term covers, in microseconds: `start` inclusive, `end` exclusive.
    DateSpan {
        /// The first instant covered.
        start: i64,
        /// The first instant past the covered ones.
        end: i64,
    },
}

impl TermOperand<'_> {
    /// The operand owning its text, so it outlives the query value it was read from.
    fn into_owned(self) -> TermOperand<'static> {
        match self {
            Self::Unmatchable => TermOperand::Unmatchable,
            Self::IdentifierText(text) => TermOperand::IdentifierText(Cow::Owned(text.into_owned())),
            Self::WholeText(text) => TermOperand::WholeText(Cow::Owned(text.into_owned())),
            Self::TokenText(text) => TermOperand::TokenText(Cow::Owned(text.into_owned())),
            Self::Number(number) => TermOperand::Number(number),
            Self::Flag(flag) => TermOperand::Flag(flag),
            Self::DateSpan { start, end } => TermOperand::DateSpan { start, end },
        }
    }
}

/// `value` of a `term` on `field` read for `kind`.
///
/// # Errors
///
/// `illegal_argument_exception` for a value that does not parse for `kind`, and the date-math
/// refusals of [`resolve_date_bound`] on a date field. A `term` answers either as the query-shard
/// failure [`refuse_invalid_value`] builds.
fn build_term_operand<'v>(
    field: &str,
    value: &'v Value,
    kind: FieldKind,
    now: DateTime<Utc>,
) -> Result<TermOperand<'v>, OpenSearchError> {
    match kind {
        FieldKind::Object | FieldKind::Nested => Ok(TermOperand::Unmatchable),
        FieldKind::Identifier => Ok(render_text(value).map_or(TermOperand::Unmatchable, TermOperand::IdentifierText)),
        FieldKind::Keyword => render_text(value)
            .map(TermOperand::WholeText)
            .ok_or_else(|| refuse_value(field, value)),
        FieldKind::Text => render_text(value)
            .map(TermOperand::TokenText)
            .ok_or_else(|| refuse_value(field, value)),
        FieldKind::Integer | FieldKind::Float => parse_numeric(value, field).map(TermOperand::Number),
        FieldKind::Boolean => parse_boolean(value, field).map(TermOperand::Flag),
        FieldKind::Date => {
            let text = render_text(value).ok_or_else(|| refuse_value(field, value))?;
            Ok(TermOperand::DateSpan {
                start: resolve_date_bound(&text, None, None, now, BoundSide::LowerInclusive)?,
                end: resolve_date_bound(&text, None, None, now, BoundSide::UpperInclusive)?,
            })
        }
    }
}

/// The bounds of a `range` read for the kind of the field it is compared with.
#[derive(Debug, Clone)]
enum RangeOperand<'v> {
    /// Bounds no stored value lies within: on an object or a `nested` field.
    Unmatchable,
    /// Text bounds the document id must lie within.
    IdentifierText(TextBounds<'v>),
    /// Text bounds a whole keyword value must lie within.
    WholeText(TextBounds<'v>),
    /// Text bounds one token of a text value must lie within.
    TokenText(TextBounds<'v>),
    /// Numeric bounds, for a numeric value.
    Number(NumericBounds),
    /// Boolean bounds ranked by [`rank_boolean`], for a boolean value.
    Flag(NumericBounds),
    /// Date bounds in microseconds, the lower one inclusive and the upper one exclusive; `None`
    /// is open.
    Date {
        /// The first instant within the range.
        lower: Option<i64>,
        /// The first instant past the range.
        upper: Option<i64>,
    },
}

impl RangeOperand<'_> {
    /// The operand owning its text, so it outlives the query it was read from.
    fn into_owned(self) -> RangeOperand<'static> {
        match self {
            Self::Unmatchable => RangeOperand::Unmatchable,
            Self::IdentifierText(bounds) => RangeOperand::IdentifierText(bounds.into_owned()),
            Self::WholeText(bounds) => RangeOperand::WholeText(bounds.into_owned()),
            Self::TokenText(bounds) => RangeOperand::TokenText(bounds.into_owned()),
            Self::Number(bounds) => RangeOperand::Number(bounds),
            Self::Flag(bounds) => RangeOperand::Flag(bounds),
            Self::Date { lower, upper } => RangeOperand::Date { lower, upper },
        }
    }
}

/// The bounds of `range` read for `kind`.
///
/// # Errors
///
/// `illegal_argument_exception` for a bound that does not parse for `kind`, and the date-math
/// refusals of [`resolve_date_bound`] on a date field.
fn build_range_operand(
    range: &RangeQuery,
    kind: FieldKind,
    now: DateTime<Utc>,
) -> Result<RangeOperand<'_>, OpenSearchError> {
    match kind {
        FieldKind::Object | FieldKind::Nested => Ok(RangeOperand::Unmatchable),
        FieldKind::Identifier => TextBounds::from_range(range).map(RangeOperand::IdentifierText),
        FieldKind::Keyword => TextBounds::from_range(range).map(RangeOperand::WholeText),
        FieldKind::Text => TextBounds::from_range(range).map(RangeOperand::TokenText),
        FieldKind::Integer | FieldKind::Float => {
            NumericBounds::from_range(range, parse_numeric).map(RangeOperand::Number)
        }
        FieldKind::Boolean => {
            NumericBounds::from_range(range, |value, field| parse_boolean(value, field).map(rank_boolean))
                .map(RangeOperand::Flag)
        }
        FieldKind::Date => {
            let resolve_side = |bound: &RangeBound, inclusive_side, exclusive_side| {
                resolve_date_side(
                    bound,
                    range,
                    now,
                    if bound.is_inclusive {
                        inclusive_side
                    } else {
                        exclusive_side
                    },
                )
            };
            Ok(RangeOperand::Date {
                lower: range
                    .lower
                    .as_ref()
                    .map(|bound| resolve_side(bound, BoundSide::LowerInclusive, BoundSide::LowerExclusive))
                    .transpose()?,
                upper: range
                    .upper
                    .as_ref()
                    .map(|bound| resolve_side(bound, BoundSide::UpperInclusive, BoundSide::UpperExclusive))
                    .transpose()?,
            })
        }
    }
}

/// `bound` of `range` in microseconds, read as `side`.
fn resolve_date_side(
    bound: &RangeBound,
    range: &RangeQuery,
    now: DateTime<Utc>,
    side: BoundSide,
) -> Result<i64, OpenSearchError> {
    let text = render_text(&bound.value).ok_or_else(|| refuse_value(&range.field, &bound.value))?;
    resolve_date_bound(&text, range.format, range.time_zone.as_ref(), now, side)
}

/// The query-shard failure on `index` for `value`, which does not parse for `field`.
fn refuse_invalid_value(index: &str, field: &str, value: impl Display) -> OpenSearchError {
    OpenSearchError::query_shard(
        index,
        &format!("[{value}] is not a valid value for the field [{field}]"),
    )
}

/// What `mappings` declares for `path`; `None` when it does not declare the path.
///
/// # Errors
///
/// `illegal_argument_exception` when the declared type is not one the evaluator supports.
pub(crate) fn find_mapped_field(mappings: &Value, path: &str) -> Result<Option<DeclaredFieldType>, OpenSearchError> {
    let mut node = mappings;
    let mut is_multi_field = false;
    let mut segments = path.split('.').peekable();
    while let Some(segment) = segments.next() {
        if let Some(child) = node.get("properties").and_then(|properties| properties.get(segment)) {
            node = child;
            continue;
        }
        match node.get("fields").and_then(|fields| fields.get(segment)) {
            Some(subfield) if segments.peek().is_none() => {
                node = subfield;
                is_multi_field = true;
            }
            Some(_) | None => return Ok(None),
        }
    }
    let kind = match node.get("type").and_then(Value::as_str) {
        Some(type_name) => FieldKind::from_mapping_type(type_name, path)?,
        None => FieldKind::Object,
    };
    Ok(Some(DeclaredFieldType { kind, is_multi_field }))
}

/// Collect the non-null values at the dotted `path` under `node`. Arrays are flattened, and a
/// key that itself contains dots is found as well as nested objects. A trailing dot ends the path
/// as its last byte does, so `a.` reads the key `a` beside the key `a.`, and `a..` the key `a.`
/// beside the key `a..`.
pub(crate) fn collect_source_values<'a>(node: &'a Value, path: &str, values: &mut Vec<&'a Value>) {
    match node {
        Value::Null => {}
        Value::Array(elements) => {
            for element in elements {
                collect_source_values(element, path, values);
            }
        }
        Value::Object(object) if !path.is_empty() => {
            let ends = path.match_indices('.').map(|(end, _)| end).chain(std::iter::once(path.len()));
            for end in ends {
                if let Some(child) = object.get(&path[..end]) {
                    collect_source_values(child, path.get(end + 1..).unwrap_or_default(), values);
                }
            }
        }
        Value::Object(_) | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            if path.is_empty() {
                values.push(node);
            }
        }
    }
}

/// The array element a `nested` query is evaluating, and the path that reaches it.
#[derive(Clone, Copy)]
struct NestedScope<'a> {
    /// The path of the nested array.
    path: &'a str,
    /// The current element.
    element: &'a Value,
}

/// A leaf's operand as one document reads it (see `DocumentView::read_operand`).
enum LeafOperand<T> {
    /// Prepared once for the request and shared by every document.
    Prepared(Rc<T>),
    /// Read for this document alone.
    Local(T),
}

impl<T> Deref for LeafOperand<T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            Self::Prepared(operand) => operand,
            Self::Local(operand) => operand,
        }
    }
}

/// The tokens of each stored value one evaluation has analyzed, keyed by the value's address in
/// the document. Every value a field resolves to is borrowed from the document, which outlives
/// the evaluation, so an address names one value for the whole evaluation; a value is therefore
/// tokenized once per document and query however many leaves, terms or fields read it.
#[derive(Default)]
struct AnalyzedValues {
    /// The tokens by the value's address, `None` for a value without a text form. The address is
    /// only compared, never read through.
    tokens: RefCell<HashMap<*const Value, Option<Rc<[Token]>>>>,
}

impl AnalyzedValues {
    /// The standard analyzer's tokens of `value`'s text; `None` for a value without a text form.
    fn tokenize_value(&self, value: &Value) -> Option<Rc<[Token]>> {
        find_or_insert_memo_entry(&self.tokens, &std::ptr::from_ref(value), || {
            render_text(value).map(|text| tokenize_standard(&text).into())
        })
    }

    /// Whether `predicate` holds for one token of `stored_values`; a value without a text form
    /// has no token. The first error ends the search.
    fn matches_any_token(
        &self,
        stored_values: &[&Value],
        predicate: impl Fn(&Token) -> Result<bool, OpenSearchError>,
    ) -> Result<bool, OpenSearchError> {
        matches_any_item(stored_values, |stored_value| {
            self.tokenize_value(stored_value)
                .map_or(Ok(false), |tokens| matches_any_item(&tokens[..], &predicate))
        })
    }
}

/// The values one document holds at a field path, and how to compare them.
struct DocumentFieldValues<'a> {
    /// `None` for a path with neither a mapping nor a value.
    kind: Option<FieldKind>,
    /// The non-null values at the path.
    values: Vec<&'a Value>,
}

/// A field a full-text query reads, resolved once for the evaluation.
struct SearchedField<'a> {
    /// The field the query names, or a field a pattern expanded to.
    name: String,
    /// What the document holds there.
    field_values: DocumentFieldValues<'a>,
}

/// One document under evaluation.
#[derive(Clone, Copy)]
struct DocumentView<'a> {
    /// The concrete index the document belongs to, named by query-shard errors.
    index: &'a str,
    /// The document id, which is not part of `source`.
    id: &'a str,
    /// The stored document.
    source: &'a Value,
    /// The mappings of the document's index.
    mappings: &'a Value,
    /// The request's instant.
    now: DateTime<Utc>,
    /// The array element a `nested` query evaluates within.
    nested: Option<NestedScope<'a>>,
    /// The tokens of the stored values this evaluation has analyzed.
    analyzed_values: &'a AnalyzedValues,
    /// What the request's leaves need on this index, shared by every document.
    preparations: &'a LeafPreparations,
    /// Whether the leaves under evaluation are the request's own, whose addresses key prepared
    /// values; false inside a tree resolved for this document alone, whose addresses a later
    /// document's tree may reuse.
    has_stable_leaves: bool,
}

impl<'a> DocumentView<'a> {
    /// Whether this document matches `query`.
    fn matches_query(&self, query: &'a Query) -> Result<bool, OpenSearchError> {
        match query {
            Query::MatchAll => Ok(true),
            Query::MatchNone => Ok(false),
            Query::Bool(bool_query) => self.matches_bool(bool_query),
            Query::Term(term) => self.matches_term(&term.field, &term.value, term.case_insensitive),
            Query::Terms(terms) => {
                let field_values = self.resolve_field(&terms.field)?;
                // Every value is read, so whether the query fails does not depend on what the
                // document holds.
                terms.values.iter().try_fold(false, |has_match, value| {
                    Ok(self.matches_term_value(&terms.field, &field_values, value, false)? || has_match)
                })
            }
            Query::Ids(ids) => Ok(ids.values.iter().any(|value| value == self.id)),
            Query::Range(range) => self.matches_range(range),
            Query::Exists(exists) => self.has_field(&exists.field),
            Query::Prefix(prefix) => self.matches_pattern(&prefix.field, "prefix", &prefix.value, |text, value| {
                starts_with_folded(text, value, prefix.case_insensitive)
            }),
            Query::Wildcard(wildcard) => {
                self.matches_pattern(&wildcard.field, "wildcard", &wildcard.pattern, |text, pattern| {
                    matches_wildcard(pattern, text, wildcard.case_insensitive)
                })
            }
            Query::Regexp(query) => self.matches_regexp(query),
            Query::ConstantScore(inner) => self.matches_query(inner),
            Query::Nested(nested) => self.matches_nested(nested),
            Query::Match(query) => self.matches_match(query),
            Query::MatchPhrase(query) => self.matches_phrase(&query.field, &query.query, FullTextForm::Phrase),
            Query::MatchPhrasePrefix(query) => {
                self.matches_phrase(&query.field, &query.query, FullTextForm::PhrasePrefix)
            }
            Query::MultiMatch(query) => self.matches_multi_match(query),
            Query::SimpleQueryString(query) => self.matches_simple_query_string(query),
            Query::LuceneQueryString(QueryStringClauses::Invalid { reason }) => {
                Err(OpenSearchError::query_shard(self.index, reason))
            }
            Query::LuceneQueryString(QueryStringClauses::Translated(tree)) => self.matches_query_string(tree),
            Query::TokenPhrase(phrase) => self.matches_token_phrase(phrase),
            Query::TokenSet(token_set) => self.matches_token_set(token_set),
            Query::MapValues(_) => Err(OpenSearchError::internal(
                "a MAP-value leaf built for data indices reached the system-index evaluator",
            )),
        }
    }

    /// A translated `query_string`: resolved against this document's fields, then evaluated
    /// like any other query; a tree whose every clause analyzed to no token matches nothing.
    fn matches_query_string(&self, tree: &FullTextQuery) -> Result<bool, OpenSearchError> {
        let Some(resolved_query) = resolve_full_text(tree, self, self.now)? else {
            return Ok(false);
        };
        // `matches_query` borrows its query for the view's lifetime. The view only holds shared
        // references, so a copy of it at the shorter lifetime of `resolved_query` evaluates the
        // resolved query, keeping the `nested` scope and the analyzed values of this evaluation.
        // Its leaves die with this document, so their addresses key no prepared value.
        let view: DocumentView<'_> = DocumentView {
            has_stable_leaves: false,
            ..*self
        };
        view.matches_query(&resolved_query)
    }

    /// Consecutive tokens of one text field, matched as `match_phrase` matches them; a field of
    /// another kind holds no tokens and does not match.
    fn matches_token_phrase(&self, phrase: &TokenPhraseQuery) -> Result<bool, OpenSearchError> {
        let field_values = self.resolve_field(&phrase.field)?;
        if field_values.kind != Some(FieldKind::Text) {
            return Ok(false);
        }
        Ok(field_values.values.iter().any(|stored_value| {
            self.analyzed_values
                .tokenize_value(stored_value)
                .is_some_and(|tokens| contains_token_phrase(&tokens, &phrase.tokens, phrase.is_last_token_prefix))
        }))
    }

    /// Tokens of one field joined by an operator, each compared as a `term` of it on the field:
    /// the field is resolved once for all of them, and on a text field each stored value is
    /// tokenized once (through the analyzed values). Like the `bool` of those terms, `and` stops
    /// at the first token that does not match and `or` at the first that does.
    fn matches_token_set(&self, query: &TokenSetQuery) -> Result<bool, OpenSearchError> {
        let field_values = self.resolve_field(&query.field)?;
        // The term values built below live for one call, so their addresses key nothing.
        let term_view = Self {
            has_stable_leaves: false,
            ..*self
        };
        let matches_token = |token: &String| {
            if field_values.kind == Some(FieldKind::Text) {
                return self
                    .analyzed_values
                    .matches_any_token(&field_values.values, |stored_token| Ok(stored_token.text == *token));
            }
            term_view.matches_term_value(&query.field, &field_values, &Value::String(token.clone()), false)
        };
        match query.operator {
            Operator::Or => matches_any_item(&query.tokens, matches_token),
            Operator::And => {
                for token in &query.tokens {
                    if !matches_token(token)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
        }
    }

    /// Whether this document satisfies the clauses of `query`, as [`BoolQuery`] states them.
    fn matches_bool(&self, query: &'a BoolQuery) -> Result<bool, OpenSearchError> {
        for clause in query.must.iter().chain(&query.filter) {
            if !self.matches_query(clause)? {
                return Ok(false);
            }
        }
        for clause in &query.must_not {
            if self.matches_query(clause)? {
                return Ok(false);
            }
        }
        let required_count = match query.minimum_should_match {
            Some(minimum) => minimum,
            None if query.must.is_empty() && query.filter.is_empty() && !query.should.is_empty() => 1,
            None => 0,
        };
        if required_count == 0 {
            return Ok(true);
        }
        let mut matched_count = 0_u32;
        for clause in &query.should {
            if self.matches_query(clause)? {
                matched_count += 1;
                if matched_count >= required_count {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Whether one element at the nested path matches the inner query on its own.
    fn matches_nested(&self, nested: &'a NestedQuery) -> Result<bool, OpenSearchError> {
        for element in self.collect_values(&nested.path) {
            let nested_view = Self {
                nested: Some(NestedScope {
                    path: &nested.path,
                    element,
                }),
                ..*self
            };
            if nested_view.matches_query(&nested.query)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The values at `path`, read from the current nested element when `path` is under it.
    fn collect_values(&self, path: &str) -> Vec<&'a Value> {
        let mut values = Vec::new();
        let nested_location = self.nested.and_then(|scope| {
            path.strip_prefix(scope.path)
                .and_then(|rest| rest.strip_prefix('.'))
                .map(|rest| (scope.element, rest))
        });
        match nested_location {
            Some((element, rest)) => collect_source_values(element, rest, &mut values),
            None => collect_source_values(self.source, path, &mut values),
        }
        values
    }

    /// The values `field` holds in this document and their kind: declared by the mappings, else
    /// inferred from the values (see the module documentation).
    fn resolve_field(&self, field: &str) -> Result<DocumentFieldValues<'a>, OpenSearchError> {
        if field == ID_FIELD {
            return Ok(DocumentFieldValues {
                kind: Some(FieldKind::Identifier),
                values: Vec::new(),
            });
        }
        if let Some(declared_type) = self.preparations.find_mapped_field(self.mappings, field)? {
            return Ok(DocumentFieldValues {
                kind: Some(declared_type.kind),
                values: self.collect_values(declared_type.resolve_source_path(field)),
            });
        }
        if let Some(parent) = field.strip_suffix(KEYWORD_SUBFIELD_SUFFIX)
            && self.preparations.find_mapped_field(self.mappings, parent)?.is_none()
        {
            let values = self.collect_values(parent);
            if infer_kind(&values) == Some(FieldKind::Text) {
                return Ok(DocumentFieldValues {
                    kind: Some(FieldKind::Keyword),
                    values,
                });
            }
        }
        let values = self.collect_values(field);
        Ok(DocumentFieldValues {
            kind: infer_kind(&values),
            values,
        })
    }

    /// Whether `field` holds `value`, compared by the field's kind.
    fn matches_term(&self, field: &str, value: &Value, is_case_insensitive: bool) -> Result<bool, OpenSearchError> {
        self.matches_term_value(field, &self.resolve_field(field)?, value, is_case_insensitive)
    }

    /// Whether `field_values`, resolved from `field`, hold `value` of a `term` or `terms`, read
    /// for their kind once per request when the leaf is the request's own. Any value that does not
    /// parse for the field fails the query on this index.
    fn matches_term_value(
        &self,
        field: &str,
        field_values: &DocumentFieldValues<'a>,
        value: &Value,
        is_case_insensitive: bool,
    ) -> Result<bool, OpenSearchError> {
        let Some(kind) = field_values.kind else {
            return Ok(false);
        };
        let build = || build_term_operand(field, value, kind, self.now);
        let operand = self
            .read_operand(|| self.preparations.find_term_operand(value, kind, build), build)
            .map_err(|_| refuse_invalid_value(self.index, field, value))?;
        self.matches_field_values(field_values, &operand, is_case_insensitive)
    }

    /// A leaf's operand for this document: the one the request prepared (`find_prepared`) when
    /// the leaf is the request's own, otherwise one `build` reads for this document alone, since
    /// the leaf's address may name another leaf for a later document.
    fn read_operand<T>(
        &self,
        find_prepared: impl FnOnce() -> Result<Rc<T>, OpenSearchError>,
        build: impl FnOnce() -> Result<T, OpenSearchError>,
    ) -> Result<LeafOperand<T>, OpenSearchError> {
        if self.has_stable_leaves {
            find_prepared().map(LeafOperand::Prepared)
        } else {
            build().map(LeafOperand::Local)
        }
    }

    /// Whether `field_values` hold `operand`, read for their kind. A `terms` compares several
    /// values against one field, and the resolution is the same for each.
    fn matches_field_values(
        &self,
        field_values: &DocumentFieldValues<'a>,
        operand: &TermOperand<'_>,
        is_case_insensitive: bool,
    ) -> Result<bool, OpenSearchError> {
        let stored_values = &field_values.values;
        match operand {
            TermOperand::Unmatchable => Ok(false),
            TermOperand::Number(expected_number) => Ok(stored_values
                .iter()
                .any(|stored_value| read_comparable_number(stored_value) == Some(*expected_number))),
            TermOperand::Flag(expected_flag) => Ok(stored_values
                .iter()
                .any(|stored_value| read_boolean(stored_value) == Some(*expected_flag))),
            TermOperand::DateSpan { start, end } => Ok(stored_values.iter().any(|stored_value| {
                read_date_micros(stored_value).is_some_and(|micros| (*start..*end).contains(&micros))
            })),
            TermOperand::IdentifierText(expected_text) => Ok(equals_text(self.id, expected_text, is_case_insensitive)),
            TermOperand::WholeText(expected_text) => Ok(stored_values.iter().any(|stored_value| {
                render_text(stored_value).is_some_and(|text| equals_text(&text, expected_text, is_case_insensitive))
            })),
            TermOperand::TokenText(expected_text) => self.analyzed_values.matches_any_token(stored_values, |token| {
                Ok(equals_text(&token.text, expected_text, is_case_insensitive))
            }),
        }
    }

    /// Whether `range.field` holds a value within the bounds, compared by the field's kind; the
    /// bounds are read once per request when the leaf is the request's own.
    fn matches_range(&self, range: &RangeQuery) -> Result<bool, OpenSearchError> {
        let field_values = self.resolve_field(&range.field)?;
        let Some(kind) = field_values.kind else {
            return Ok(false);
        };
        let build = || build_range_operand(range, kind, self.now);
        let operand = self.read_operand(|| self.preparations.find_range_operand(range, kind, build), build)?;
        let stored_values = &field_values.values;
        Ok(match &*operand {
            RangeOperand::Unmatchable => false,
            RangeOperand::IdentifierText(bounds) => bounds.contains(self.id),
            RangeOperand::WholeText(bounds) => stored_values
                .iter()
                .any(|stored_value| render_text(stored_value).is_some_and(|text| bounds.contains(&text))),
            RangeOperand::TokenText(bounds) => self
                .analyzed_values
                .matches_any_token(stored_values, |token| Ok(bounds.contains(&token.text)))?,
            RangeOperand::Number(bounds) => stored_values
                .iter()
                .any(|stored_value| read_comparable_number(stored_value).is_some_and(|number| bounds.contains(number))),
            RangeOperand::Flag(bounds) => stored_values
                .iter()
                .any(|stored_value| read_boolean(stored_value).is_some_and(|flag| bounds.contains(rank_boolean(flag)))),
            RangeOperand::Date { lower, upper } => stored_values.iter().any(|stored_value| {
                read_date_micros(stored_value).is_some_and(|micros| {
                    lower.is_none_or(|lower| micros >= lower) && upper.is_none_or(|upper| micros < upper)
                })
            }),
        })
    }

    /// Whether `field` holds a value; an object or a nested field needs a leaf under it.
    fn has_field(&self, field: &str) -> Result<bool, OpenSearchError> {
        let field_values = self.resolve_field(field)?;
        Ok(match field_values.kind {
            None => false,
            Some(FieldKind::Identifier) => true,
            Some(FieldKind::Object | FieldKind::Nested) => field_values.values.iter().any(|value| has_leaf(value)),
            Some(
                FieldKind::Keyword
                | FieldKind::Text
                | FieldKind::Date
                | FieldKind::Integer
                | FieldKind::Float
                | FieldKind::Boolean,
            ) => !field_values.values.is_empty(),
        })
    }

    /// Whether `matcher(text, pattern)` holds for one of the field's values: the id or a keyword
    /// value whole, against `pattern` as given; a token of a `text` value, against `pattern`
    /// lowercased the way the analyzer lowercases.
    fn matches_pattern(
        &self,
        field: &str,
        leaf: &str,
        pattern: &str,
        matcher: impl Fn(&str, &str) -> bool,
    ) -> Result<bool, OpenSearchError> {
        let field_values = self.resolve_field(field)?;
        // Normalized once, and only when a text value is reached.
        let normalized_pattern = OnceCell::new();
        self.match_string_field(
            &field_values,
            |text| matcher(text, pattern),
            |token| matcher(token, normalized_pattern.get_or_init(|| normalize_standard(pattern))),
            || OpenSearchError::illegal_argument(format!("[{leaf}] applies to string fields and [{field}] is not one")),
        )
    }

    /// A `regexp` leaf on this document, its field resolved like every other leaf's (a
    /// multi-field such as `title.keyword` reads its parent's values): each token of a text field,
    /// the whole value of a keyword field or of the id, with the pattern as given; a query-shard
    /// 400 on a date, numeric or boolean field. The pattern is translated before the field is
    /// resolved, so a malformed one fails the query whatever the document and its mappings hold;
    /// it is translated once per request.
    fn matches_regexp(&self, query: &RegexpQuery) -> Result<bool, OpenSearchError> {
        let pattern = self.preparations.translate_regexp(query)?;
        let field_values = self.resolve_field(&query.field)?;
        let is_pattern_match = |text: &str| pattern.is_match(text);
        self.match_string_field(&field_values, is_pattern_match, is_pattern_match, || {
            OpenSearchError::query_shard(
                self.index,
                &format!(
                    "regular expressions need a keyword or text field, and [{}] is not one",
                    query.field
                ),
            )
        })
    }

    /// Whether a string leaf holds for one of `field_values`: `whole_value_matcher` on the id or
    /// on a keyword value, `token_matcher` on each token of a `text` value; nothing on a missing,
    /// object or nested field. A date, numeric or boolean field is the error
    /// `refuse_typed_field` builds.
    fn match_string_field(
        &self,
        field_values: &DocumentFieldValues<'_>,
        whole_value_matcher: impl Fn(&str) -> bool,
        token_matcher: impl Fn(&str) -> bool,
        refuse_typed_field: impl FnOnce() -> OpenSearchError,
    ) -> Result<bool, OpenSearchError> {
        let Some(kind) = field_values.kind else {
            return Ok(false);
        };
        match kind {
            FieldKind::Object | FieldKind::Nested => Ok(false),
            FieldKind::Identifier => Ok(whole_value_matcher(self.id)),
            FieldKind::Keyword => Ok(field_values
                .values
                .iter()
                .any(|stored_value| render_text(stored_value).is_some_and(|text| whole_value_matcher(&text)))),
            FieldKind::Text => self
                .analyzed_values
                .matches_any_token(&field_values.values, |token| Ok(token_matcher(&token.text))),
            FieldKind::Date | FieldKind::Integer | FieldKind::Float | FieldKind::Boolean => Err(refuse_typed_field()),
        }
    }

    /// Whether this document satisfies a `match`.
    fn matches_match(&self, query: &MatchQuery) -> Result<bool, OpenSearchError> {
        let text = render_text(&query.query).ok_or_else(|| refuse_value(&query.field, &query.query))?;
        let field_values = self.resolve_field(&query.field)?;
        let match_result = self.match_full_text(
            &query.field,
            &field_values,
            &QueryText::new(&text, FullTextForm::Tokens(query.operator)),
            query.lenient.unwrap_or(false),
        )?;
        Ok(match_result.unwrap_or(query.zero_terms_query == ZeroTermsQuery::All))
    }

    /// Whether `field` holds the phrase `text`; a text without tokens matches nothing.
    fn matches_phrase(&self, field: &str, text: &str, form: FullTextForm) -> Result<bool, OpenSearchError> {
        let field_values = self.resolve_field(field)?;
        let match_result = self.match_full_text(field, &field_values, &QueryText::new(text, form), false)?;
        Ok(match_result.unwrap_or(false))
    }

    /// Whether one field satisfies a `multi_match` on its own.
    fn matches_multi_match(&self, query: &MultiMatchQuery) -> Result<bool, OpenSearchError> {
        let form = match query.match_type {
            MultiMatchType::BestFields => FullTextForm::Tokens(query.operator),
            MultiMatchType::Phrase => FullTextForm::Phrase,
        };
        let is_lenient = query.lenient.unwrap_or_else(|| selects_all_fields(&query.fields));
        let fields = self.resolve_searched_fields(&query.fields)?;
        let match_result = self.match_any_field(&fields, &QueryText::new(&query.query, form), is_lenient)?;
        Ok(match_result.unwrap_or(false))
    }

    /// Whether this document satisfies a `simple_query_string`.
    fn matches_simple_query_string(&self, query: &SimpleQueryStringQuery) -> Result<bool, OpenSearchError> {
        let is_lenient = query.lenient.unwrap_or_else(|| selects_all_fields(&query.fields));
        let fields = self.resolve_searched_fields(&query.fields)?;
        let match_result = self.evaluate_simple_query(&query.query, &fields, query.default_operator, is_lenient)?;
        Ok(match_result.unwrap_or(false))
    }

    /// The fields `patterns` name for this document, each resolved once for all the query's texts.
    fn resolve_searched_fields(&self, patterns: &[String]) -> Result<Vec<SearchedField<'a>>, OpenSearchError> {
        self.expand_field_patterns(patterns)
            .into_iter()
            .map(|name| {
                let field_values = self.resolve_field(&name)?;
                Ok(SearchedField { name, field_values })
            })
            .collect()
    }

    /// A `simple_query_string` node over `fields`; `None` when it contributes no clause. Every
    /// clause is evaluated, so whether a query fails does not depend on the order of its clauses.
    fn evaluate_simple_query(
        &self,
        node: &SimpleQuery,
        fields: &[SearchedField<'a>],
        operator: Operator,
        is_lenient: bool,
    ) -> Result<Option<bool>, OpenSearchError> {
        match node {
            SimpleQuery::MatchAll => Ok(Some(true)),
            SimpleQuery::MatchNone => Ok(Some(false)),
            SimpleQuery::Term { text, is_prefix } => {
                let form = if *is_prefix {
                    FullTextForm::Prefix
                } else {
                    FullTextForm::Tokens(operator)
                };
                self.match_any_field(fields, &QueryText::new(text, form), is_lenient)
            }
            SimpleQuery::Phrase { text } => {
                self.match_any_field(fields, &QueryText::new(text, FullTextForm::Phrase), is_lenient)
            }
            SimpleQuery::Bool { must, should, must_not } => {
                let evaluate_clauses = |clauses: &[SimpleQuery]| {
                    clauses
                        .iter()
                        .map(|clause| self.evaluate_simple_query(clause, fields, operator, is_lenient))
                        .collect::<Result<Vec<Option<bool>>, OpenSearchError>>()
                };
                let (must_values, should_values, must_not_values) = (
                    evaluate_clauses(must)?,
                    evaluate_clauses(should)?,
                    evaluate_clauses(must_not)?,
                );
                // A group whose clauses all drop out, apart from the match-all a negation adds,
                // drops out itself.
                let is_contributing = must_values.iter().chain(&must_not_values).any(Option::is_some)
                    || should
                        .iter()
                        .zip(&should_values)
                        .any(|(clause, value)| value.is_some() && !matches!(clause, SimpleQuery::MatchAll));
                if !is_contributing {
                    return Ok(None);
                }
                if must_not_values.into_iter().flatten().any(|is_match| is_match) {
                    return Ok(Some(false));
                }
                let mut must_results = must_values.into_iter().flatten().peekable();
                if must_results.peek().is_some() {
                    return Ok(Some(must_results.all(|is_match| is_match)));
                }
                Ok(Some(should_values.into_iter().flatten().any(|is_match| is_match)))
            }
        }
    }

    /// `query_text` against each of `fields`: a match in one field is a match; `None` when no field
    /// contributes a clause. Every field is read, so whether a query fails does not depend on which
    /// field holds a match.
    fn match_any_field(
        &self,
        fields: &[SearchedField<'a>],
        query_text: &QueryText<'_>,
        is_lenient: bool,
    ) -> Result<Option<bool>, OpenSearchError> {
        let mut has_match = false;
        let mut is_contributing = false;
        for field in fields {
            if let Some(is_match) = self.match_full_text(&field.name, &field.field_values, query_text, is_lenient)? {
                has_match |= is_match;
                is_contributing = true;
            }
        }
        Ok(is_contributing.then_some(has_match))
    }

    /// `query_text` against one field by its kind; `None` when the text yields no token on a `text`
    /// field and so contributes no clause.
    fn match_full_text(
        &self,
        field: &str,
        field_values: &DocumentFieldValues<'a>,
        query_text: &QueryText<'_>,
        is_lenient: bool,
    ) -> Result<Option<bool>, OpenSearchError> {
        let Some(kind) = field_values.kind else {
            return Ok(Some(false));
        };
        match kind {
            FieldKind::Object | FieldKind::Nested => Ok(Some(false)),
            FieldKind::Text => Ok(match_analyzed_values(
                &field_values.values,
                self.analyzed_values,
                query_text.analyze_text(),
                query_text.form,
            )),
            FieldKind::Identifier => {
                self.match_whole_values(field, std::iter::once(Cow::Borrowed(self.id)), query_text)
            }
            FieldKind::Keyword => {
                let values = field_values.values.iter().filter_map(|value| render_text(value));
                self.match_whole_values(field, values, query_text)
            }
            FieldKind::Date | FieldKind::Integer | FieldKind::Float | FieldKind::Boolean => match query_text.form {
                FullTextForm::PhrasePrefix => Err(self.refuse_phrase_prefix(field)),
                FullTextForm::Prefix if is_lenient => Ok(Some(false)),
                FullTextForm::Prefix => Err(OpenSearchError::query_shard(
                    self.index,
                    &format!("a prefix applies to text and keyword fields and [{field}] is neither"),
                )),
                FullTextForm::Tokens(_) | FullTextForm::Phrase => {
                    match build_term_operand(field, query_text.read_term_value(), kind, self.now) {
                        Ok(operand) => self.matches_field_values(field_values, &operand, false).map(Some),
                        Err(_) if is_lenient => Ok(Some(false)),
                        Err(_) => Err(refuse_invalid_value(self.index, field, query_text.text)),
                    }
                }
            },
        }
    }

    /// `query_text` against whole values (a keyword field or the id): equal, or a prefix of one for
    /// a prefix term.
    fn match_whole_values<'v>(
        &self,
        field: &str,
        mut values: impl Iterator<Item = Cow<'v, str>>,
        query_text: &QueryText<'_>,
    ) -> Result<Option<bool>, OpenSearchError> {
        let text = query_text.text;
        match query_text.form {
            FullTextForm::PhrasePrefix => Err(self.refuse_phrase_prefix(field)),
            FullTextForm::Prefix => Ok(Some(values.any(|value| value.starts_with(text)))),
            FullTextForm::Tokens(_) | FullTextForm::Phrase => Ok(Some(values.any(|value| value == text))),
        }
    }

    /// The query-shard failure for a `match_phrase_prefix` on `field`, which is not `text`.
    fn refuse_phrase_prefix(&self, field: &str) -> OpenSearchError {
        OpenSearchError::query_shard(
            self.index,
            &format!("[match_phrase_prefix] applies to text fields and [{field}] is not one"),
        )
    }

    /// The concrete fields `patterns` name for this document: a pattern without `*` or `?` names
    /// itself; any other pattern names the [`Self::list_leaf_fields`] it matches.
    fn expand_field_patterns(&self, patterns: &[String]) -> Vec<String> {
        let mut leaf_fields: Option<Vec<String>> = None;
        let mut fields = Vec::new();
        for pattern in patterns {
            if !pattern.contains(['*', '?']) {
                fields.push(pattern.clone());
                continue;
            }
            let leaves = leaf_fields.get_or_insert_with(|| self.list_leaf_fields());
            fields.extend(leaves.iter().filter(|field| matches_glob(pattern, field)).cloned());
        }
        fields.sort_unstable();
        fields.dedup();
        fields
    }

    /// The fields a document offers to `*`: its leaf paths with a value, outside `nested` fields
    /// (those are queried through `nested`), outside what the mapping does not index
    /// (`index: false`, or under an object with `enabled: false`) and outside fields of a type the
    /// evaluator does not support, plus the multi-fields the mapping declares on them (unless
    /// themselves `index: false` or of such a type) and the `keyword` subfield of an undeclared
    /// string. Inside a `nested` query the walk starts at the current element, which is where
    /// [`Self::collect_values`] reads the paths under it, and each step of the query's path passes
    /// the walk's own checks first: `nested` accepts a path that is not a nested field, and a step
    /// that is `nested` is one the query opened.
    fn list_leaf_fields(&self) -> Vec<String> {
        let mut fields = Vec::new();
        let Some(scope) = self.nested else {
            collect_leaf_fields(self.source, Some(self.mappings), "", &mut fields);
            return fields;
        };
        let (visibility, mapping_node) = follow_mapping_path(self.mappings, scope.path);
        if visibility != MemberVisibility::Hidden {
            collect_leaf_fields(scope.element, mapping_node, scope.path, &mut fields);
        }
        fields
    }
}

/// The searchable fields of one system-index document, for the full-text resolver. A field is
/// typed by [`DocumentView::resolve_field`], the resolution every other leaf on this document
/// uses, so a `query_string` and a `match` type a field alike: the stored mapping first
/// (multi-fields such as `title.keyword` included), then the document's own value.
impl FullTextFields for DocumentView<'_> {
    fn index_name(&self) -> &str {
        self.index
    }

    /// A name selects itself; a pattern selects the document's leaf fields it matches
    /// ([`DocumentView::expand_field_patterns`]). A field with no type here (unmapped and
    /// absent) or an object-like type is not selected.
    fn expand_selector(&self, selector: &FieldSelector) -> Result<Vec<SearchField>, OpenSearchError> {
        match selector {
            FieldSelector::Name(name) => {
                let kind = self.resolve_field(name)?.kind.and_then(choose_search_kind);
                Ok(kind.map_or_else(Vec::new, |kind| {
                    vec![SearchField::Typed {
                        name: name.clone(),
                        kind,
                    }]
                }))
            }
            FieldSelector::Pattern(pattern) => {
                let names = self.expand_field_patterns(std::slice::from_ref(pattern));
                let mut fields = Vec::with_capacity(names.len());
                for name in names {
                    if let Some(kind) = self.resolve_field(&name)?.kind.and_then(choose_search_kind) {
                        fields.push(SearchField::Typed { name, kind });
                    }
                }
                Ok(fields)
            }
        }
    }
}

/// How a field the evaluator typed as `kind` meets full-text input. The id matches as a
/// keyword; `integer` and `long` both search as a 64-bit integer; an object or a `nested` field
/// takes no full-text clause.
const fn choose_search_kind(kind: FieldKind) -> Option<SearchKind> {
    match kind {
        FieldKind::Identifier | FieldKind::Keyword => Some(SearchKind::Keyword),
        FieldKind::Text => Some(SearchKind::Text),
        FieldKind::Date => Some(SearchKind::Date),
        FieldKind::Integer => Some(SearchKind::Long),
        FieldKind::Float => Some(SearchKind::Double),
        FieldKind::Boolean => Some(SearchKind::Boolean),
        FieldKind::Object | FieldKind::Nested => None,
    }
}

/// Appends the leaf fields of `node`, which sits at `path`, to `fields`, as
/// [`DocumentView::list_leaf_fields`] states them. `mapping_node` is what the mapping declares for
/// `path`, handed down the walk so that a leaf costs no lookup from the mapping's root.
fn collect_leaf_fields(node: &Value, mapping_node: Option<&Value>, path: &str, fields: &mut Vec<String>) {
    match node {
        Value::Null => {}
        Value::Array(elements) => {
            for element in elements {
                collect_leaf_fields(element, mapping_node, path, fields);
            }
        }
        Value::Object(object) => {
            for (key, child) in object {
                let (visibility, child_mapping) = mapping_node.map_or((MemberVisibility::Visible, None), |mapping| {
                    follow_mapping_path(mapping, key)
                });
                if visibility == MemberVisibility::Visible {
                    let child_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{path}.{key}")
                    };
                    collect_leaf_fields(child, child_mapping, &child_path, fields);
                }
            }
        }
        Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            if path.is_empty() {
                return;
            }
            let Some(mapping) = mapping_node else {
                fields.push(path.to_string());
                if node.is_string() {
                    fields.push(format!("{path}{KEYWORD_SUBFIELD_SUFFIX}"));
                }
                return;
            };
            if !is_option_disabled(mapping, "index") {
                fields.push(path.to_string());
            }
            fields.extend(list_multi_fields(mapping).map(|(name, _)| format!("{path}.{name}")));
        }
    }
}

/// The fields `mappings` declares that a field pattern selects from any document holding them,
/// with their declared kinds: the declared leaves [`DocumentView::list_leaf_fields`] lists, under
/// the same rules, and their multi-fields. Inside a `nested` query on `nested_path` these are the
/// leaves under that path; outside one, the leaves outside every `nested` field. A field the
/// mapping does not declare is not listed: whether a pattern selects it, and its kind, depend on
/// the documents.
pub(crate) fn list_mapped_pattern_fields(mappings: &Value, nested_path: Option<&str>) -> Vec<(String, FieldKind)> {
    let mut fields = Vec::new();
    let Some(path) = nested_path else {
        collect_mapped_leaf_fields(mappings, "", &mut fields);
        return fields;
    };
    if let (MemberVisibility::Visible | MemberVisibility::NestedOnly, Some(mapping_node)) =
        follow_mapping_path(mappings, path)
    {
        collect_mapped_leaf_fields(mapping_node, path, &mut fields);
    }
    fields
}

/// Appends to `fields` the leaves the object `mapping_node`, declared at `path`, declares below it,
/// as [`list_mapped_pattern_fields`] states them. It walks the mapping as [`collect_leaf_fields`]
/// walks a document, without the document.
fn collect_mapped_leaf_fields(mapping_node: &Value, path: &str, fields: &mut Vec<(String, FieldKind)>) {
    let Some(properties) = mapping_node.get("properties").and_then(Value::as_object) else {
        return;
    };
    for (key, member_mapping) in properties {
        if read_member_visibility(mapping_node, key, Some(member_mapping)) != MemberVisibility::Visible {
            continue;
        }
        let member_path = if path.is_empty() {
            key.clone()
        } else {
            format!("{path}.{key}")
        };
        let declared_type = member_mapping.get("type").and_then(Value::as_str);
        // A visible member's type is supported and not `nested`, so the error and `Nested` arms
        // cannot be reached; they are listed because the match is exhaustive.
        match declared_type.map(|type_name| FieldKind::from_mapping_type(type_name, key)) {
            None | Some(Ok(FieldKind::Object)) => collect_mapped_leaf_fields(member_mapping, &member_path, fields),
            Some(Ok(
                kind @ (FieldKind::Identifier
                | FieldKind::Keyword
                | FieldKind::Text
                | FieldKind::Date
                | FieldKind::Integer
                | FieldKind::Float
                | FieldKind::Boolean),
            )) => {
                for (name, subfield) in list_multi_fields(member_mapping) {
                    let subfield_type = subfield.get("type").and_then(Value::as_str);
                    if let Some(Ok(subfield_kind)) =
                        subfield_type.map(|type_name| FieldKind::from_mapping_type(type_name, name))
                    {
                        fields.push((format!("{member_path}.{name}"), subfield_kind));
                    }
                }
                if !is_option_disabled(member_mapping, "index") {
                    fields.push((member_path, kind));
                }
            }
            Some(Ok(FieldKind::Nested) | Err(_)) => {}
        }
    }
}

/// What the mapping lets a pattern read of an object's member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemberVisibility {
    /// Patterns read the member and walk on through it.
    Visible,
    /// A `nested` field, which patterns read only from inside a `nested` query on it.
    NestedOnly,
    /// Patterns never read it: it sets `enabled` false, or its type is not one the evaluator
    /// supports.
    Hidden,
}

/// How visible the dotted `path` under `mapping_node` is to patterns, which is how visible its least
/// visible step is, with the node the mapping declares at its end. Both a `nested` query's path and
/// a document's key can hold dots. A step past what the mapping declares is visible and has no node.
fn follow_mapping_path<'m>(mapping_node: &'m Value, path: &str) -> (MemberVisibility, Option<&'m Value>) {
    let mut visibility = MemberVisibility::Visible;
    let mut end_mapping = Some(mapping_node);
    for segment in path.split('.') {
        let Some(parent_mapping) = end_mapping else {
            break;
        };
        end_mapping = find_mapping_node(parent_mapping, segment);
        match read_member_visibility(parent_mapping, segment, end_mapping) {
            MemberVisibility::Hidden => return (MemberVisibility::Hidden, None),
            MemberVisibility::NestedOnly => visibility = MemberVisibility::NestedOnly,
            MemberVisibility::Visible => {}
        }
    }
    (visibility, end_mapping)
}

/// How the member `key` of an object `mapping_node` maps is visible to patterns. `key` is one path
/// segment, and `member_mapping` is what `properties` declares for it; a member it does not declare
/// can still name a multi-field, which `properties` does not hold. `enabled` is read only from
/// `properties`: on a multi-field it closes nothing.
fn read_member_visibility(mapping_node: &Value, key: &str, member_mapping: Option<&Value>) -> MemberVisibility {
    if member_mapping.is_some_and(|mapping| is_option_disabled(mapping, "enabled")) {
        return MemberVisibility::Hidden;
    }
    member_mapping
        .or_else(|| mapping_node.get("fields")?.get(key))
        .map_or(MemberVisibility::Visible, |mapping| read_type_visibility(mapping, key))
}

/// How the type `member_mapping` declares for `key` is visible to patterns. A node without a type
/// is an object, which is visible.
fn read_type_visibility(member_mapping: &Value, key: &str) -> MemberVisibility {
    let declared_type = member_mapping.get("type").and_then(Value::as_str);
    match declared_type.map(|type_name| FieldKind::from_mapping_type(type_name, key)) {
        Some(Ok(FieldKind::Nested)) => MemberVisibility::NestedOnly,
        Some(Ok(_)) | None => MemberVisibility::Visible,
        Some(Err(_)) => MemberVisibility::Hidden,
    }
}

/// Whether `predicate` holds for any item; the first error ends the search.
fn matches_any_item<T>(
    items: &[T],
    predicate: impl Fn(&T) -> Result<bool, OpenSearchError>,
) -> Result<bool, OpenSearchError> {
    for item in items {
        if predicate(item)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The kind of an undeclared path, from its first value.
fn infer_kind(values: &[&Value]) -> Option<FieldKind> {
    values.iter().find_map(|value| match value {
        Value::String(_) => Some(FieldKind::Text),
        Value::Number(number) if number.is_i64() || number.is_u64() => Some(FieldKind::Integer),
        Value::Number(_) => Some(FieldKind::Float),
        Value::Bool(_) => Some(FieldKind::Boolean),
        Value::Object(_) => Some(FieldKind::Object),
        Value::Null | Value::Array(_) => None,
    })
}

/// A value's text: a string as it is, a number or a boolean in its JSON spelling.
pub(crate) fn render_text(value: &Value) -> Option<Cow<'_, str>> {
    match value {
        Value::String(text) => Some(Cow::Borrowed(text)),
        Value::Number(number) => Some(Cow::Owned(number.to_string())),
        Value::Bool(flag) => Some(Cow::Borrowed(if *flag { "true" } else { "false" })),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

/// Whether `text` equals `expected_text`, ASCII letters folded when `is_case_insensitive`.
fn equals_text(text: &str, expected_text: &str, is_case_insensitive: bool) -> bool {
    if is_case_insensitive {
        text.eq_ignore_ascii_case(expected_text)
    } else {
        text == expected_text
    }
}

/// Whether `text` starts with `prefix`, ASCII letters folded when `is_case_insensitive`.
fn starts_with_folded(text: &str, prefix: &str, is_case_insensitive: bool) -> bool {
    if !is_case_insensitive {
        return text.starts_with(prefix);
    }
    text.as_bytes()
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
}

/// The literal that opens `pattern_tail` and its length in bytes; `None` at the pattern's end. A
/// `\` makes the next character the literal, and a `\` with nothing after it is itself one.
fn read_wildcard_literal(pattern_tail: &str) -> Option<(char, usize)> {
    let mut characters = pattern_tail.chars();
    match characters.next()? {
        '\\' => Some(characters.next().map_or(('\\', 1), |escaped| (escaped, 1 + escaped.len_utf8()))),
        literal => Some((literal, literal.len_utf8())),
    }
}

/// Whether `text` matches the wildcard `pattern`: `*` matches any run of characters (none
/// included), `?` exactly one character, `\` makes the next character literal, and a trailing `\`
/// matches itself. ASCII letters fold when `is_case_insensitive`.
///
/// It walks both strings by byte offset on character boundaries and backtracks only to the last
/// unescaped `*`, so it allocates nothing and costs `O(pattern × text)` in the worst case.
fn matches_wildcard(pattern: &str, text: &str, is_case_insensitive: bool) -> bool {
    let (mut pattern_offset, mut text_offset) = (0, 0);
    // The pattern offset just after the last unescaped `*`, and the text offset that `*` was last
    // tried at.
    let mut last_star_offsets: Option<(usize, usize)> = None;
    while let Some(text_character) = text[text_offset..].chars().next() {
        let pattern_tail = &pattern[pattern_offset..];
        if pattern_tail.starts_with('*') {
            pattern_offset += 1;
            last_star_offsets = Some((pattern_offset, text_offset));
            continue;
        }
        let element_length = if pattern_tail.starts_with('?') {
            Some(1)
        } else {
            read_wildcard_literal(pattern_tail)
                .filter(|(literal, _)| {
                    *literal == text_character || (is_case_insensitive && literal.eq_ignore_ascii_case(&text_character))
                })
                .map(|(_, length)| length)
        };
        if let Some(element_length) = element_length {
            pattern_offset += element_length;
            text_offset += text_character.len_utf8();
        } else {
            let Some((star_end, star_text_offset)) = last_star_offsets else {
                return false;
            };
            // The last `*` absorbs one more character of `text`, and matching resumes after it.
            let absorbed_length = text[star_text_offset..].chars().next().map_or(0, char::len_utf8);
            pattern_offset = star_end;
            text_offset = star_text_offset + absorbed_length;
            last_star_offsets = Some((star_end, text_offset));
        }
    }
    pattern[pattern_offset..].chars().all(|character| character == '*')
}

/// Whether an object or array value holds at least one non-null leaf.
fn has_leaf(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Object(object) => object.values().any(has_leaf),
        Value::Array(elements) => elements.iter().any(has_leaf),
        Value::Bool(_) | Value::Number(_) | Value::String(_) => true,
    }
}

/// How a full-text query text meets one field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FullTextForm {
    /// The tokens combined with an operator (`match`, a `simple_query_string` term).
    Tokens(Operator),
    /// The tokens at consecutive positions.
    Phrase,
    /// The tokens at consecutive positions, the last one a prefix of its token.
    PhrasePrefix,
    /// The text, lowercased, a prefix of one token (a `simple_query_string` term ending in `*`).
    Prefix,
}

/// What a query text is on a `text` field.
enum QueryAnalysis {
    /// The analyzer's tokens of the text.
    Tokens(Vec<Token>),
    /// The text lowercased by the analyzer's rule, unsplit.
    Prefix(String),
}

/// One query text on its way over the fields of one node. What a field needs from it is computed
/// by the first field that needs it and reused by the rest, so its cost per evaluation does not
/// grow with the number of fields.
struct QueryText<'t> {
    /// The text as the query states it.
    text: &'t str,
    /// How the text meets a field.
    form: FullTextForm,
    /// The analysis, once a `text` field has asked for it.
    analysis: OnceCell<QueryAnalysis>,
    /// The text as a typed term, once a number, date or boolean field has asked for it.
    term_value: OnceCell<Value>,
}

impl<'t> QueryText<'t> {
    /// A text not read by any field yet.
    const fn new(text: &'t str, form: FullTextForm) -> Self {
        Self {
            text,
            form,
            analysis: OnceCell::new(),
            term_value: OnceCell::new(),
        }
    }

    /// The analysis of the text: the normalized prefix for a prefix term, the tokens otherwise.
    fn analyze_text(&self) -> &QueryAnalysis {
        self.analysis.get_or_init(|| match self.form {
            FullTextForm::Prefix => QueryAnalysis::Prefix(normalize_standard(self.text)),
            FullTextForm::Tokens(_) | FullTextForm::Phrase | FullTextForm::PhrasePrefix => {
                QueryAnalysis::Tokens(tokenize_standard(self.text))
            }
        })
    }

    /// The text as a term value, which a typed field parses.
    fn read_term_value(&self) -> &Value {
        self.term_value.get_or_init(|| Value::String(self.text.to_string()))
    }
}

/// Full-text matching on the values of a `text` field, each analyzed on its own (through
/// `analyzed_values`, so once per document and query); `None` when the query text yields no token.
fn match_analyzed_values(
    values: &[&Value],
    analyzed_values: &AnalyzedValues,
    analysis: &QueryAnalysis,
    form: FullTextForm,
) -> Option<bool> {
    let query_tokens = match analysis {
        QueryAnalysis::Prefix(prefix) => {
            let has_prefixed_token =
                analyzed_values.matches_any_token(values, |token| Ok(token.text.starts_with(prefix.as_str())));
            // The predicate cannot fail, so this is always `Some`: a prefix term contributes.
            return has_prefixed_token.ok();
        }
        QueryAnalysis::Tokens(query_tokens) => query_tokens,
    };
    if query_tokens.is_empty() {
        return None;
    }
    let contains_token =
        |tokens: &[Token], query_token: &Token| tokens.iter().any(|token| token.text == query_token.text);
    let mut value_tokens = values.iter().filter_map(|value| analyzed_values.tokenize_value(value));
    Some(value_tokens.any(|tokens| match form {
        FullTextForm::Tokens(Operator::Or) => {
            query_tokens.iter().any(|query_token| contains_token(&tokens, query_token))
        }
        FullTextForm::Tokens(Operator::And) => {
            query_tokens.iter().all(|query_token| contains_token(&tokens, query_token))
        }
        FullTextForm::Phrase => contains_token_phrase(&tokens, query_tokens, false),
        // A `Prefix` text has no tokens and returned above; it is listed because the match is exhaustive.
        FullTextForm::PhrasePrefix | FullTextForm::Prefix => contains_token_phrase(&tokens, query_tokens, true),
    }))
}

/// The node `mapping_node` declares through `properties` for one path `segment`; `None` for a
/// segment it does not declare that way, a multi-field among them.
fn find_mapping_node<'m>(mapping_node: &'m Value, segment: &str) -> Option<&'m Value> {
    mapping_node.get("properties")?.get(segment)
}

/// Whether the mapping `node` sets its option `key` to false, as a JSON boolean or as a string.
fn is_option_disabled(node: &Value, key: &str) -> bool {
    node.get(key)
        .is_some_and(|option| matches!(option, Value::Bool(false)) || option.as_str() == Some("false"))
}

/// The multi-fields (`fields`) `mapping_node` declares, by name with their mappings, without those
/// it does not index and those of a type the evaluator does not support.
fn list_multi_fields(mapping_node: &Value) -> impl Iterator<Item = (&String, &Value)> {
    mapping_node
        .get("fields")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter(|(name, subfield)| {
            !is_option_disabled(subfield, "index") && read_type_visibility(subfield, name) != MemberVisibility::Hidden
        })
}

/// The `illegal_argument_exception` for `value`, which does not fit the type of `field`.
fn refuse_value(field: &str, value: &Value) -> OpenSearchError {
    OpenSearchError::illegal_argument(format!("[{value}] is not a valid value for the field [{field}]"))
}

/// A number as the evaluator compares it. Two integers compare exactly, because `f64` rounds an
/// integer past its mantissa and two distinct `long`s would read as equal; any other pair
/// compares as floats.
#[derive(Debug, Clone, Copy)]
struct ComparableNumber {
    /// The value, when it is an integer: `i128` holds every `i64` and `u64`.
    exact_integer: Option<i128>,
    /// The value as a float.
    approximate: f64,
}

impl PartialEq for ComparableNumber {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for ComparableNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.exact_integer, other.exact_integer) {
            (Some(integer), Some(other_integer)) => Some(integer.cmp(&other_integer)),
            (Some(_) | None, _) => self.approximate.partial_cmp(&other.approximate),
        }
    }
}

/// `value` as a number of a query: refused when it holds none.
fn parse_numeric(value: &Value, field: &str) -> Result<ComparableNumber, OpenSearchError> {
    read_comparable_number(value).ok_or_else(|| refuse_value(field, value))
}

/// A finite number, or a string holding one.
pub(crate) fn read_numeric(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok().filter(|number| number.is_finite()),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

/// The integer `value` holds, as a JSON integer or a string; `None` for a fraction or an integer
/// past `i128`, which then compare as floats.
fn read_exact_integer(value: &Value) -> Option<i128> {
    match value {
        Value::Number(number) => number.as_i128(),
        Value::String(text) => text.trim().parse().ok(),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

/// The number [`read_numeric`] finds in `value`, ready to compare.
fn read_comparable_number(value: &Value) -> Option<ComparableNumber> {
    read_numeric(value).map(|approximate| ComparableNumber {
        exact_integer: read_exact_integer(value),
        approximate,
    })
}

/// `value` as a boolean of a query: refused when it holds none.
fn parse_boolean(value: &Value, field: &str) -> Result<bool, OpenSearchError> {
    read_boolean(value).ok_or_else(|| refuse_value(field, value))
}

/// `true` or `false`, as a JSON boolean or its string.
pub(crate) fn read_boolean(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(flag) => Some(*flag),
        Value::String(text) if text == "true" => Some(true),
        Value::String(text) if text == "false" => Some(false),
        Value::Null | Value::Number(_) | Value::String(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

/// `false` before `true`.
fn rank_boolean(flag: bool) -> ComparableNumber {
    ComparableNumber {
        exact_integer: Some(i128::from(flag)),
        approximate: f64::from(u8::from(flag)),
    }
}

/// A stored date in microseconds: a date string or epoch milliseconds.
pub(crate) fn read_date_micros(value: &Value) -> Option<i64> {
    let millis = match value {
        Value::String(text) => parse_stored_date_millis(text)?,
        Value::Number(number) => number.as_i64()?,
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => return None,
    };
    millis.checked_mul(1000)
}

/// Range bounds compared as strings, each as `(text, is_inclusive)`; `None` is open.
#[derive(Debug, Clone)]
struct TextBounds<'v> {
    /// The lower bound.
    lower: Option<(Cow<'v, str>, bool)>,
    /// The upper bound.
    upper: Option<(Cow<'v, str>, bool)>,
}

impl<'v> TextBounds<'v> {
    /// The bounds of `range` in their text form.
    ///
    /// # Errors
    ///
    /// `illegal_argument_exception` for a bound without a text form.
    fn from_range(range: &'v RangeQuery) -> Result<Self, OpenSearchError> {
        let read_bound = |bound: &'v RangeBound| {
            render_text(&bound.value)
                .map(|text| (text, bound.is_inclusive))
                .ok_or_else(|| refuse_value(&range.field, &bound.value))
        };
        Ok(Self {
            lower: range.lower.as_ref().map(read_bound).transpose()?,
            upper: range.upper.as_ref().map(read_bound).transpose()?,
        })
    }

    /// The bounds owning their text.
    fn into_owned(self) -> TextBounds<'static> {
        let own_bound = |(text, is_inclusive): (Cow<'_, str>, bool)| (Cow::Owned(text.into_owned()), is_inclusive);
        TextBounds {
            lower: self.lower.map(own_bound),
            upper: self.upper.map(own_bound),
        }
    }

    /// Whether `text` lies within the bounds.
    fn contains(&self, text: &str) -> bool {
        let is_above_lower = self.lower.as_ref().is_none_or(|(lower, is_inclusive)| {
            let ordering = text.cmp(lower);
            ordering == Ordering::Greater || (ordering == Ordering::Equal && *is_inclusive)
        });
        let is_below_upper = self.upper.as_ref().is_none_or(|(upper, is_inclusive)| {
            let ordering = text.cmp(upper);
            ordering == Ordering::Less || (ordering == Ordering::Equal && *is_inclusive)
        });
        is_above_lower && is_below_upper
    }
}

/// Numeric range bounds as `(value, is_inclusive)`.
#[derive(Debug, Clone, Copy)]
struct NumericBounds {
    /// The lower bound; `None` is open.
    lower: Option<(ComparableNumber, bool)>,
    /// The upper bound; `None` is open.
    upper: Option<(ComparableNumber, bool)>,
}

impl NumericBounds {
    /// The bounds of `range`, each read by `parse_bound`.
    fn from_range(
        range: &RangeQuery,
        parse_bound: impl Fn(&Value, &str) -> Result<ComparableNumber, OpenSearchError>,
    ) -> Result<Self, OpenSearchError> {
        let read_bound =
            |bound: &RangeBound| parse_bound(&bound.value, &range.field).map(|value| (value, bound.is_inclusive));
        Ok(Self {
            lower: range.lower.as_ref().map(read_bound).transpose()?,
            upper: range.upper.as_ref().map(read_bound).transpose()?,
        })
    }

    /// Whether `number` lies within the bounds.
    fn contains(&self, number: ComparableNumber) -> bool {
        let is_above_lower = self.lower.is_none_or(
            |(lower, is_inclusive)| {
                if is_inclusive { number >= lower } else { number > lower }
            },
        );
        let is_below_upper = self.upper.is_none_or(
            |(upper, is_inclusive)| {
                if is_inclusive { number <= upper } else { number < upper }
            },
        );
        is_above_lower && is_below_upper
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use chrono::{DateTime, Utc};
    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng as _};
    use serde_json::{Value, json};

    use super::*;
    use crate::opensearch::error::render_envelope;
    use crate::query_dsl::parse::parse_query;

    /// One element of a drawn wildcard pattern, read by the reference matcher.
    #[derive(Debug, Clone, Copy)]
    enum PatternElement {
        AnyRun,
        AnyCharacter,
        Literal(char),
    }

    /// Characters a drawn pattern or text is made of: one-, two- and three-byte UTF-8, so a matcher
    /// that steps by bytes splits one, and the three characters a pattern gives a meaning to, so a
    /// text holds them literally.
    const WILDCARD_ALPHABET: [char; 6] = ['a', 'ö', '日', '*', '?', '\\'];
    /// Number of drawn pattern and text pairs.
    const WILDCARD_CASE_COUNT: usize = 10_000;

    fn parse_request_instant() -> DateTime<Utc> {
        "2026-10-08T12:34:56.789Z".parse().expect("a literal instant")
    }

    fn evaluate(query: &Value, source: &Value, mappings: &Value) -> Result<bool, OpenSearchError> {
        let query = parse_query(query).expect("the query parses");
        IndexQueryEvaluator::new(&query, ".kibana_1", mappings, parse_request_instant())?
            .matches_document("doc-1", source)
    }

    fn assert_matches(cases: &[(Value, bool)], source: &Value, mappings: &Value) {
        for (query, should_match) in cases {
            let is_match = evaluate(query, source, mappings).expect("the query evaluates");
            assert_eq!(is_match, *should_match, "{query} on {source}");
        }
    }

    async fn assert_refused(query: &Value, source: &Value, mappings: &Value) {
        let error = evaluate(query, source, mappings).expect_err("the query is refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception", "{query}");
    }

    async fn assert_query_shard_refused(query: &Value, source: &Value, mappings: &Value) {
        let error = evaluate(query, source, mappings).expect_err("the query is refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(body["error"]["type"], "search_phase_execution_exception", "{query}");
        assert_eq!(
            body["error"]["root_cause"][0]["type"], "query_shard_exception",
            "{query}"
        );
        assert_eq!(body["error"]["root_cause"][0]["index"], ".kibana_1", "{query}");
    }

    #[test]
    fn an_empty_bool_matches_everything() {
        assert_matches(
            &[
                (json!({"bool": {}}), true),
                (json!({"match_all": {}}), true),
                (json!({"match_none": {}}), false),
            ],
            &json!({"type": "dashboard"}),
            &json!({"properties": {"type": {"type": "keyword"}}}),
        );
        assert_matches(&[(json!({"bool": {}}), true)], &json!({}), &json!({}));
    }

    #[test]
    fn bool_clauses_and_minimum_should_match_decide_the_match() {
        let mappings = json!({"properties": {"type": {"type": "keyword"}, "namespace": {"type": "keyword"}}});
        let source = json!({"type": "dashboard", "namespace": "default"});
        let is_dashboard = json!({"term": {"type": "dashboard"}});
        let is_config = json!({"term": {"type": "config"}});
        let is_default = json!({"term": {"namespace": "default"}});

        assert_matches(
            &[
                (json!({"bool": {"filter": [is_dashboard]}}), true),
                (json!({"bool": {"filter": [is_config]}}), false),
                (json!({"bool": {"filter": [is_dashboard], "must": [is_config]}}), false),
                (json!({"bool": {"must_not": [is_config]}}), true),
                (json!({"bool": {"must_not": [is_dashboard]}}), false),
                // Without `minimum_should_match`, one `should` clause must match only when no
                // `must` or `filter` clause does the selecting.
                (json!({"bool": {"should": [is_dashboard, is_config]}}), true),
                (json!({"bool": {"should": [is_config]}}), false),
                (json!({"bool": {"filter": [is_default], "should": [is_config]}}), true),
                (
                    json!({"bool": {"should": [is_dashboard, is_default], "minimum_should_match": 2}}),
                    true,
                ),
                (
                    json!({"bool": {"should": [is_dashboard, is_config], "minimum_should_match": 2}}),
                    false,
                ),
                (
                    json!({"bool": {"should": [is_config], "minimum_should_match": 0}}),
                    true,
                ),
                (
                    json!({"bool": {"filter": [is_default], "should": [is_config], "minimum_should_match": 1}}),
                    false,
                ),
                (json!({"constant_score": {"filter": is_dashboard}}), true),
                (json!({"constant_score": {"filter": is_config}}), false),
            ],
            &source,
            &mappings,
        );
    }

    #[tokio::test]
    async fn numeric_terms_compare_numerically_and_booleans_match_true_or_false() {
        let mappings = json!({"properties": {
            "count": {"type": "integer"},
            "rating": {"type": "double"},
            "enabled": {"type": "boolean"}
        }});
        let source = json!({"count": 0, "rating": 2.5, "enabled": false});

        assert_matches(
            &[
                (json!({"term": {"count": 0}}), true),
                (json!({"term": {"count": "0"}}), true),
                (json!({"term": {"count": 0.0}}), true),
                (json!({"term": {"count": 1}}), false),
                (json!({"term": {"rating": 2.5}}), true),
                (json!({"term": {"rating": "2.50"}}), true),
                (json!({"terms": {"count": [3, 0]}}), true),
                (json!({"terms": {"count": [3, 4]}}), false),
                (json!({"range": {"count": {"gte": -1, "lt": "1"}}}), true),
                (json!({"range": {"rating": {"gt": 2.5}}}), false),
                (json!({"range": {"rating": {"gte": 2.5}}}), true),
                (json!({"range": {"rating": {"lte": 2.5}}}), true),
                (json!({"range": {"rating": {"lt": 2.5}}}), false),
                (json!({"term": {"enabled": false}}), true),
                (json!({"term": {"enabled": "false"}}), true),
                (json!({"term": {"enabled": true}}), false),
            ],
            &source,
            &mappings,
        );
        assert_matches(
            &[
                (json!({"term": {"count": 3}}), true),
                (json!({"term": {"enabled": true}}), true),
            ],
            &json!({"count": "3", "enabled": "true"}),
            &mappings,
        );
        assert_query_shard_refused(&json!({"term": {"count": "zero"}}), &source, &mappings).await;
        assert_query_shard_refused(&json!({"terms": {"count": [1, "zero"]}}), &source, &mappings).await;
        // `0` matches first, and the value after it still fails the query.
        assert_query_shard_refused(&json!({"terms": {"count": [0, "zero"]}}), &source, &mappings).await;
        // Both parse as floats, and neither is a number a stored value can equal.
        assert_query_shard_refused(&json!({"term": {"count": "NaN"}}), &source, &mappings).await;
        assert_query_shard_refused(&json!({"term": {"count": "inf"}}), &source, &mappings).await;
        assert_query_shard_refused(&json!({"term": {"enabled": "yes"}}), &source, &mappings).await;
        assert_refused(&json!({"range": {"count": {"gte": "zero"}}}), &source, &mappings).await;
        assert_refused(&json!({"range": {"enabled": {"gte": "yes"}}}), &source, &mappings).await;
    }

    #[test]
    fn integers_compare_exactly_past_the_float_mantissa() {
        let mappings = json!({"properties": {"count": {"type": "long"}}});

        // 2^53 + 1 is the first integer `f64` cannot hold: as a float it reads as 2^53.
        assert_matches(
            &[
                (json!({"term": {"count": 9_007_199_254_740_993_i64}}), true),
                (json!({"term": {"count": 9_007_199_254_740_992_i64}}), false),
                (json!({"term": {"count": "9007199254740993"}}), true),
                (json!({"term": {"count": "9007199254740992"}}), false),
                (json!({"terms": {"count": [1, 9_007_199_254_740_992_i64]}}), false),
                (json!({"range": {"count": {"gt": 9_007_199_254_740_992_i64}}}), true),
                (json!({"range": {"count": {"gt": 9_007_199_254_740_993_i64}}}), false),
                (json!({"range": {"count": {"lt": 9_007_199_254_740_994_i64}}}), true),
                (json!({"range": {"count": {"lte": 9_007_199_254_740_992_i64}}}), false),
            ],
            &json!({"count": 9_007_199_254_740_993_i64}),
            &mappings,
        );
        // Past `i64`: a `u64` value is exact as well.
        assert_matches(
            &[
                (json!({"term": {"count": u64::MAX}}), true),
                (json!({"term": {"count": u64::MAX - 1}}), false),
            ],
            &json!({"count": u64::MAX}),
            &mappings,
        );
    }

    #[test]
    fn a_numeric_string_past_i128_compares_as_a_float() {
        let mappings = json!({"properties": {"count": {"type": "long"}}});

        // 2^127 is the first integer `i128` cannot hold. Below it two integers stay exact; from it
        // on a value has no exact reading, so a neighbor that rounds to the same float matches.
        assert_matches(
            &[
                (
                    json!({"term": {"count": "170141183460469231731687303715884105727"}}),
                    true,
                ),
                (
                    json!({"term": {"count": "170141183460469231731687303715884105726"}}),
                    false,
                ),
            ],
            &json!({"count": "170141183460469231731687303715884105727"}),
            &mappings,
        );
        assert_matches(
            &[
                (
                    json!({"term": {"count": "170141183460469231731687303715884105728"}}),
                    true,
                ),
                (
                    json!({"term": {"count": "170141183460469231731687303715884105729"}}),
                    true,
                ),
                (
                    json!({"term": {"count": "170141183460469231731687303715884105727"}}),
                    true,
                ),
                (json!({"term": {"count": 1}}), false),
            ],
            &json!({"count": "170141183460469231731687303715884105728"}),
            &mappings,
        );
    }

    #[test]
    fn a_path_under_a_dynamic_object_is_typed_from_its_source_value() {
        let mappings =
            json!({"dynamic": "strict", "properties": {"migrationVersion": {"dynamic": "true", "type": "object"}}});
        let source = json!({"migrationVersion": {"dashboard": "7.9.3", "level": 3, "ratio": 0.5, "flag": true}});

        assert_matches(
            &[
                (json!({"term": {"migrationVersion.dashboard": "7.9.3"}}), true),
                (json!({"term": {"migrationVersion.dashboard": "7.9.0"}}), false),
                (json!({"term": {"migrationVersion.dashboard.keyword": "7.9.3"}}), true),
                (json!({"prefix": {"migrationVersion.dashboard.keyword": "7.9"}}), true),
                (json!({"term": {"migrationVersion.level": 3}}), true),
                (json!({"term": {"migrationVersion.level": "3"}}), true),
                (json!({"range": {"migrationVersion.ratio": {"gte": 0.4}}}), true),
                (json!({"term": {"migrationVersion.flag": true}}), true),
                (json!({"term": {"migrationVersion.missing": "x"}}), false),
                (json!({"exists": {"field": "migrationVersion.missing"}}), false),
                (json!({"exists": {"field": "migrationVersion"}}), true),
                // Only a string carries a `keyword` subfield, so this path holds nothing.
                (json!({"term": {"migrationVersion.level.keyword": "3"}}), false),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_path_without_a_mapping_or_a_value_matches_nothing() {
        let mappings = json!({"properties": {"type": {"type": "keyword"}}});
        let source = json!({"type": "dashboard"});

        assert_matches(
            &[
                (json!({"term": {"absent": "x"}}), false),
                (json!({"terms": {"absent": ["x"]}}), false),
                (json!({"range": {"absent": {"gte": 1}}}), false),
                (json!({"prefix": {"absent": "x"}}), false),
                (json!({"wildcard": {"absent": "*"}}), false),
                (json!({"exists": {"field": "absent"}}), false),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_leaf_on_an_object_path_matches_nothing() {
        let mappings = json!({"properties": {
            "dashboard": {"properties": {"title": {"type": "text"}}},
            "references": {"type": "nested", "properties": {"id": {"type": "keyword"}}}
        }});
        let source = json!({"dashboard": {"title": "Logs"}, "references": [{"id": "1"}]});

        assert_matches(
            &[
                (json!({"term": {"dashboard": "Logs"}}), false),
                (json!({"term": {"references": "1"}}), false),
                (json!({"range": {"dashboard": {"gte": "a"}}}), false),
                (json!({"prefix": {"dashboard": "L"}}), false),
                (json!({"wildcard": {"references": "*"}}), false),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_declared_multi_field_reads_its_parents_values() {
        let mappings = json!({"properties": {
            "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}
        }});
        let source = json!({"title": "Logs overview"});

        assert_matches(
            &[
                (json!({"term": {"title.keyword": "Logs overview"}}), true),
                (json!({"term": {"title.keyword": "Logs"}}), false),
                (json!({"prefix": {"title.keyword": "Logs"}}), true),
                (json!({"exists": {"field": "title.keyword"}}), true),
                // A subfield has no subfields of its own.
                (json!({"term": {"title.keyword.raw": "Logs overview"}}), false),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn mapped_fields_are_found_by_their_dotted_path() {
        let mappings = json!({"properties": {
            "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "dashboard": {"properties": {"count": {"type": "integer"}}},
            "references": {"type": "nested", "properties": {"id": {"type": "keyword"}}}
        }});
        let build_declared_type = |kind, is_multi_field| Some(DeclaredFieldType { kind, is_multi_field });
        let cases = [
            ("title", build_declared_type(FieldKind::Text, false)),
            ("title.keyword", build_declared_type(FieldKind::Keyword, true)),
            ("dashboard", build_declared_type(FieldKind::Object, false)),
            ("dashboard.count", build_declared_type(FieldKind::Integer, false)),
            ("references", build_declared_type(FieldKind::Nested, false)),
            ("references.id", build_declared_type(FieldKind::Keyword, false)),
            ("dashboard.absent", None),
            ("title.raw", None),
            ("title.keyword.raw", None),
            ("absent", None),
        ];
        for (path, expected_type) in cases {
            assert_eq!(
                find_mapped_field(&mappings, path).expect("the path resolves"),
                expected_type,
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn every_supported_mapping_type_has_its_kind_and_another_type_is_refused() {
        let cases = [
            ("keyword", FieldKind::Keyword),
            ("text", FieldKind::Text),
            ("date", FieldKind::Date),
            ("integer", FieldKind::Integer),
            ("long", FieldKind::Integer),
            ("float", FieldKind::Float),
            ("double", FieldKind::Float),
            ("boolean", FieldKind::Boolean),
            ("object", FieldKind::Object),
            ("nested", FieldKind::Nested),
        ];
        for (type_name, expected_kind) in cases {
            assert_eq!(
                FieldKind::from_mapping_type(type_name, "a").expect("a known type"),
                expected_kind,
                "{type_name}"
            );
        }

        let (status, body) =
            render_envelope(FieldKind::from_mapping_type("geo_point", "location").expect_err("refused")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(body["error"]["field"], "location", "{body}");
        assert_eq!(body["error"]["field_type"], "geo_point", "{body}");
    }

    #[test]
    fn source_values_are_collected_through_arrays_and_dotted_keys() {
        let source = json!({
            "a": {"b": [1, null, {"c": 2}], "d": null},
            "e.f": 3,
            "g": {"h.i": 4},
            "j": [[5, 6]]
        });
        let cases = [
            ("a.b", vec![json!(1), json!({"c": 2})]),
            ("a.b.c", vec![json!(2)]),
            ("a.d", vec![]),
            ("e.f", vec![json!(3)]),
            ("g.h.i", vec![json!(4)]),
            ("j", vec![json!(5), json!(6)]),
            ("absent", vec![]),
        ];
        for (path, expected_values) in cases {
            let mut values = Vec::new();
            collect_source_values(&source, path, &mut values);
            let collected_values: Vec<Value> = values.into_iter().cloned().collect();
            assert_eq!(collected_values, expected_values, "{path}");
        }
    }

    #[test]
    fn exists_on_an_object_field_is_true_when_any_leaf_is_present() {
        let mappings = json!({"properties": {"dashboard": {"properties": {"title": {"type": "text"}}}}});
        let query = json!({"exists": {"field": "dashboard"}});
        let cases = [
            (json!({"dashboard": {"title": "Logs"}}), true),
            (json!({"dashboard": {"meta": {"depth": 1}}}), true),
            (json!({"dashboard": {"panels": [{"id": "p1"}]}}), true),
            (json!({"dashboard": {}}), false),
            (json!({"dashboard": {"title": null}}), false),
            (json!({"dashboard": {"panels": []}}), false),
            (json!({"type": "dashboard"}), false),
        ];
        for (source, should_match) in cases {
            assert_eq!(
                evaluate(&query, &source, &mappings).expect("evaluates"),
                should_match,
                "{source}"
            );
        }
    }

    #[test]
    fn nested_references_match_within_one_element() {
        let mappings = json!({"properties": {"kind": {"type": "keyword"}, "references": {"type": "nested", "properties": {
            "name": {"type": "keyword"}, "type": {"type": "keyword"}, "id": {"type": "keyword"}
        }}}});
        let source = json!({"kind": "dashboard", "references": [
            {"name": "panel_0", "type": "index-pattern", "id": "1"},
            {"name": "panel_1", "type": "visualization", "id": "2"}
        ]});
        let same_element = json!({"bool": {"must": [{"term": {"references.id": "1"}}, {"term": {"references.type": "index-pattern"}}]}});
        let across_elements = json!({"bool": {"must": [{"term": {"references.id": "1"}}, {"term": {"references.type": "visualization"}}]}});

        assert_matches(
            &[
                (json!({"nested": {"path": "references", "query": same_element}}), true),
                (
                    json!({"nested": {"path": "references", "query": across_elements}}),
                    false,
                ),
                // Outside `nested` the elements' values are flattened, so the pair matches.
                (across_elements, true),
                (
                    json!({"nested": {"path": "missing", "query": {"match_all": {}}}}),
                    false,
                ),
                // A path outside the nested array still reads the document.
                (
                    json!({"nested": {"path": "references", "query": {"term": {"kind": "dashboard"}}}}),
                    true,
                ),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_background_style_range_lt_now_minus_90d_matches_only_older_dates() {
        let mappings = json!({"properties": {"updated_at": {"type": "date"}}});
        let query = json!({"range": {"updated_at": {"lt": "now-90d"}}});
        // now - 90 days = 2026-07-10T12:34:56.789Z, an exclusive upper bound.
        let cases = [
            (json!({"updated_at": "2026-07-10T12:34:56.788Z"}), true),
            (json!({"updated_at": "2026-07-10T12:34:56.789Z"}), false),
            (json!({"updated_at": "2026-10-01T00:00:00.000Z"}), false),
            (json!({"updated_at": 1_783_686_896_788_i64}), true),
            (json!({"type": "config"}), false),
            // A value that is no date holds no instant to compare.
            (json!({"updated_at": "not a date"}), false),
            (json!({"updated_at": true}), false),
            // `1.5` is no integer count of milliseconds, and the largest `i64` overflows the
            // microsecond unit.
            (json!({"updated_at": 1.5}), false),
            (json!({"updated_at": 9_223_372_036_854_775_807_i64}), false),
        ];
        for (source, should_match) in cases {
            assert_eq!(
                evaluate(&query, &source, &mappings).expect("evaluates"),
                should_match,
                "{source}"
            );
        }
        assert_matches(
            &[
                (json!({"term": {"updated_at": "2026-07-10"}}), true),
                (json!({"term": {"updated_at": "2026-07-11"}}), false),
            ],
            &json!({"updated_at": "2026-07-10T12:34:56.788Z"}),
            &mappings,
        );
    }

    #[test]
    fn a_date_range_applies_its_format_and_time_zone() {
        let mappings = json!({"properties": {"updated_at": {"type": "date"}}});
        // 16:34:56.788 in Dubai (+04:00) is the stored instant; so is the epoch value.
        let source = json!({"updated_at": "2026-07-10T12:34:56.788Z"});

        assert_matches(
            &[
                (
                    json!({"range": {"updated_at": {"gte": "2026-07-10T16:34:56.788", "time_zone": "Asia/Dubai"}}}),
                    true,
                ),
                (
                    json!({"range": {"updated_at": {"gt": "2026-07-10T16:34:56.788", "time_zone": "Asia/Dubai"}}}),
                    false,
                ),
                (
                    json!({"range": {"updated_at": {"lte": "1783686896788", "format": "epoch_millis"}}}),
                    true,
                ),
                (
                    json!({"range": {"updated_at": {"lt": "1783686896788", "format": "epoch_millis"}}}),
                    false,
                ),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_boolean_range_orders_false_before_true() {
        let mappings = json!({"properties": {"enabled": {"type": "boolean"}}});

        assert_matches(
            &[
                (json!({"range": {"enabled": {"gte": "false", "lt": "true"}}}), true),
                (json!({"range": {"enabled": {"gt": "false"}}}), false),
            ],
            &json!({"enabled": false}),
            &mappings,
        );
    }

    #[test]
    fn a_term_on_a_text_field_matches_only_a_lowercased_token() {
        let mappings = json!({"properties": {"title": {"type": "text"}, "tags": {"type": "text"}}});
        // Tokens: "logs", "overview", "error", "rates"; tags: "alpha", "beta" and "gamma".
        let source = json!({"title": "Logs Overview: ERROR-rates", "tags": ["Alpha beta", "Gamma"]});

        assert_matches(
            &[
                (json!({"term": {"title": "logs"}}), true),
                (json!({"term": {"title": "error"}}), true),
                (json!({"term": {"title": "Logs"}}), false),
                (json!({"term": {"title": "ERROR"}}), false),
                (
                    json!({"term": {"title": {"value": "ERROR", "case_insensitive": true}}}),
                    true,
                ),
                (json!({"term": {"title": "logs overview"}}), false),
                (json!({"term": {"title": "error-rates"}}), false),
                (json!({"terms": {"title": ["missing", "rates"]}}), true),
                (json!({"term": {"tags": "gamma"}}), true),
                (json!({"term": {"tags": "alpha beta"}}), false),
                (json!({"term": {"title": ""}}), false),
            ],
            &source,
            &mappings,
        );
        // A string under a dynamic object is `text` too: the version is one number token.
        assert_matches(
            &[(json!({"term": {"migrationVersion.dashboard": "7.9.3"}}), true)],
            &json!({"migrationVersion": {"dashboard": "7.9.3"}}),
            &json!({"properties": {"migrationVersion": {"dynamic": "true", "type": "object"}}}),
        );
    }

    #[test]
    fn prefix_and_wildcard_on_a_text_field_match_a_normalized_token() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        // Tokens: "visualization", "of", "ünicode", "errors".
        let source = json!({"title": "Visualization of Ünicode Errors"});

        assert_matches(
            &[
                (json!({"prefix": {"title": "Visual"}}), true),
                (json!({"prefix": {"title": "ÜNI"}}), true),
                (json!({"prefix": {"title": "rrors"}}), false),
                (json!({"prefix": {"title": "of ünicode"}}), false),
                (json!({"wildcard": {"title": "E*S"}}), true),
                (json!({"wildcard": {"title": "visuali?ation"}}), true),
                (json!({"wildcard": {"title": "*code err*"}}), false),
                (json!({"wildcard": {"title": "visualization"}}), true),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_range_on_a_text_field_matches_when_one_token_is_within_it() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        // Tokens: "beta", "zeta"; compared as strings, in byte order.
        let source = json!({"title": "Beta ZETA"});

        assert_matches(
            &[
                (json!({"range": {"title": {"gte": "b", "lt": "c"}}}), true),
                (json!({"range": {"title": {"gte": "c", "lte": "d"}}}), false),
                (json!({"range": {"title": {"gte": "zeta"}}}), true),
                (json!({"range": {"title": {"gt": "zeta"}}}), false),
                (json!({"range": {"title": {"lt": "Z"}}}), false),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn exists_on_a_text_field_is_true_for_an_empty_string() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let exists_query = json!({"exists": {"field": "title"}});

        // `""` and `!!!` yield no token but are present values.
        assert_matches(&[(exists_query.clone(), true)], &json!({"title": ""}), &mappings);
        assert_matches(&[(exists_query.clone(), true)], &json!({"title": "!!!"}), &mappings);
        assert_matches(&[(exists_query.clone(), false)], &json!({"title": null}), &mappings);
        assert_matches(&[(exists_query, false)], &json!({}), &mappings);
    }

    #[test]
    fn a_text_field_value_without_text_has_no_token_and_the_next_value_still_matches() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});

        // A stored object has no text to compare: its key is no token, and every leaf reads past
        // it, whichever side of the other value it is on.
        for stored_values in [json!([{"nested": true}, "Logs 1"]), json!(["Logs 1", {"nested": true}])] {
            assert_matches(
                &[
                    (json!({"term": {"title": "1"}}), true),
                    (json!({"term": {"title": "nested"}}), false),
                    (json!({"prefix": {"title": "LO"}}), true),
                    (json!({"prefix": {"title": "ne"}}), false),
                    (json!({"wildcard": {"title": "l*s"}}), true),
                    (json!({"wildcard": {"title": "n*d"}}), false),
                    (json!({"range": {"title": {"gte": "l", "lt": "m"}}}), true),
                    (json!({"range": {"title": {"gte": "n"}}}), false),
                ],
                &json!({"title": stored_values}),
                &mappings,
            );
        }
    }

    #[test]
    fn a_text_field_tokenizes_the_json_spelling_of_a_stored_number_or_boolean() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});

        assert_matches(
            &[
                (json!({"term": {"title": "7"}}), true),
                (json!({"term": {"title": 7}}), true),
                (json!({"term": {"title": "true"}}), true),
                (json!({"term": {"title": true}}), true),
                (json!({"term": {"title": "8"}}), false),
            ],
            &json!({"title": [7, true]}),
            &mappings,
        );
    }

    #[test]
    fn a_stored_value_is_tokenized_once_per_evaluation() {
        let analyzed_values = AnalyzedValues::default();
        let stored_value = json!("Logs overview");
        let equal_stored_value = json!("Logs overview");

        let first_tokens = analyzed_values.tokenize_value(&stored_value).expect("a string has text");
        let repeated_tokens = analyzed_values.tokenize_value(&stored_value).expect("a string has text");
        let equal_value_tokens = analyzed_values.tokenize_value(&equal_stored_value).expect("a string has text");

        assert!(
            Rc::ptr_eq(&first_tokens, &repeated_tokens),
            "the second read is served by the memo"
        );
        assert!(
            !Rc::ptr_eq(&first_tokens, &equal_value_tokens),
            "the memo is keyed by the value's address, not by its text"
        );
        assert_eq!(first_tokens, equal_value_tokens);
        assert_eq!(analyzed_values.tokenize_value(&json!({"no": "text"})), None);
    }

    #[test]
    fn a_text_value_past_the_token_limit_is_cut_and_one_at_the_limit_stays_one_token() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let token_below_limit = "7".repeat(254);
        let token_at_limit = "7".repeat(255);
        let token_above_limit = "7".repeat(256);

        for whole_token in [&token_below_limit, &token_at_limit] {
            assert_matches(
                &[(json!({"term": {"title": whole_token}}), true)],
                &json!({"title": whole_token}),
                &mappings,
            );
        }
        // 256 digits are a token of 255 digits and a token of one digit, so the whole value is no
        // token.
        assert_matches(
            &[
                (json!({"term": {"title": token_above_limit}}), false),
                (json!({"term": {"title": token_at_limit}}), true),
                (json!({"term": {"title": "7"}}), true),
            ],
            &json!({"title": token_above_limit}),
            &mappings,
        );
    }

    #[test]
    fn a_dot_joins_digits_in_a_text_value_only_between_two_digits() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let term_query = json!({"term": {"title": "7"}});

        // `7.3` is one number token, so `7` is not among its tokens; a dot with a digit on one side
        // only is dropped and leaves the token `7`; `""` holds no token.
        let cases = [("7.3", false), ("7..3", true), (".7", true), ("7.", true), ("", false)];
        for (stored_text, should_match) in cases {
            assert_matches(
                &[(term_query.clone(), should_match)],
                &json!({"title": stored_text}),
                &mappings,
            );
        }
    }

    #[tokio::test]
    async fn keyword_prefix_wildcard_and_ids_match_the_stated_values() {
        let mappings = json!({"properties": {"type": {"type": "keyword"}, "title": {"type": "keyword"}, "count": {"type": "integer"}}});
        let source = json!({"type": "Index-Pattern", "title": "a*b", "count": 1});

        assert_matches(
            &[
                (json!({"prefix": {"type": "Index"}}), true),
                (json!({"prefix": {"type": "index"}}), false),
                (
                    json!({"prefix": {"type": {"value": "index", "case_insensitive": true}}}),
                    true,
                ),
                (
                    json!({"prefix": {"type": {"value": "Index-Pattern-and-more", "case_insensitive": true}}}),
                    false,
                ),
                (json!({"wildcard": {"type": "*-Pat?ern"}}), true),
                (
                    json!({"wildcard": {"type": {"value": "index*", "case_insensitive": true}}}),
                    true,
                ),
                (json!({"wildcard": {"type": "index*"}}), false),
                (json!({"wildcard": {"title": "a\\*b"}}), true),
                (json!({"wildcard": {"title": "a\\*c"}}), false),
                (
                    json!({"term": {"type": {"value": "INDEX-PATTERN", "case_insensitive": true}}}),
                    true,
                ),
                (json!({"term": {"type": "index-pattern"}}), false),
                (json!({"terms": {"type": ["config", "Index-Pattern"]}}), true),
                (json!({"terms": {"type": []}}), false),
                (json!({"ids": {"values": ["doc-0", "doc-1"]}}), true),
                (json!({"ids": {"values": ["doc-2"]}}), false),
                (json!({"ids": {"values": []}}), false),
                (json!({"term": {"_id": "doc-1"}}), true),
                (json!({"range": {"type": {"gte": "I", "lt": "J"}}}), true),
                (json!({"range": {"type": {"gt": "Index-Pattern"}}}), false),
                (json!({"range": {"type": {"gte": "Index-Pattern"}}}), true),
                (json!({"range": {"type": {"lt": "Index-Pattern"}}}), false),
                (json!({"range": {"type": {"lte": "Index-Pattern"}}}), true),
                (json!({"range": {"type": {"gte": "J"}}}), false),
                (json!({"range": {"type": {"lte": "A"}}}), false),
            ],
            &source,
            &mappings,
        );
        assert_refused(&json!({"prefix": {"count": "1"}}), &source, &mappings).await;
        for refused_query in [json!({"term": {"geo": "x"}}), json!({"terms": {"geo": []}})] {
            assert_refused(
                &refused_query,
                &source,
                &json!({"properties": {"geo": {"type": "geo_point"}}}),
            )
            .await;
        }
    }

    #[test]
    fn a_keyword_field_compares_the_text_of_every_stored_value() {
        let mappings = json!({"properties": {"namespaces": {"type": "keyword"}}});

        assert_matches(
            &[
                (json!({"term": {"namespaces": "default"}}), true),
                (json!({"term": {"namespaces": "space"}}), true),
                (json!({"term": {"namespaces": "other"}}), false),
                (json!({"term": {"namespaces": 7}}), true),
                (json!({"term": {"namespaces": "7"}}), true),
                (json!({"prefix": {"namespaces": "sp"}}), true),
                (json!({"range": {"namespaces": {"gte": "s"}}}), true),
                (json!({"range": {"namespaces": {"gte": "z"}}}), false),
            ],
            &json!({"namespaces": ["default", {"not": "text"}, "space", 7]}),
            &mappings,
        );
    }

    #[test]
    fn the_id_field_matches_the_document_id() {
        let mappings = json!({"properties": {"type": {"type": "keyword"}}});
        let source = json!({"type": "dashboard"});

        assert_matches(
            &[
                (
                    json!({"term": {"_id": {"value": "DOC-1", "case_insensitive": true}}}),
                    true,
                ),
                (json!({"term": {"_id": "DOC-1"}}), false),
                (json!({"term": {"_id": 1}}), false),
                (json!({"range": {"_id": {"gte": "doc-1", "lt": "doc-2"}}}), true),
                (json!({"range": {"_id": {"gt": "doc-1"}}}), false),
                (json!({"prefix": {"_id": "doc-"}}), true),
                (json!({"prefix": {"_id": "DOC-"}}), false),
                (json!({"wildcard": {"_id": "doc-?"}}), true),
                (json!({"exists": {"field": "_id"}}), true),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_wildcard_walks_stars_escapes_and_multibyte_characters() {
        let cases = [
            ("*", "", true),
            ("", "", true),
            ("", "a", false),
            ("a*", "abc", true),
            ("a*b*c", "aXbYc", true),
            ("a*b*c", "aXbY", false),
            ("a*c", "aXcYc", true),
            ("a?c", "aöc", true),
            ("a?c", "ac", false),
            ("?", "日", true),
            ("a\\*b", "a*b", true),
            ("a\\*b", "aXb", false),
            ("a\\?b", "a?b", true),
            ("a\\?b", "aXb", false),
            ("a\\\\b", "a\\b", true),
            // `\` makes any next character literal, a plain one included.
            ("a\\b", "ab", true),
            // A trailing `\` has nothing to escape, so it matches itself.
            ("a\\", "a\\", true),
            ("a\\", "a", false),
            ("*\\", "xy\\", true),
            // What remains of the pattern at the end of the text may only be stars.
            ("a**", "a", true),
            ("a\\*", "a", false),
            ("a*\\*", "ab*", true),
        ];
        for (pattern, text, should_match) in cases {
            assert_eq!(
                matches_wildcard(pattern, text, false),
                should_match,
                "{pattern:?} against {text:?}"
            );
        }
        let folded_cases = [
            ("INDEX*", "index-pattern", true),
            ("in?ex", "INDEX", true),
            ("\\*A", "*a", true),
            // Folding covers ASCII letters only.
            ("Ö", "ö", false),
        ];
        for (pattern, text, should_match) in folded_cases {
            assert_eq!(
                matches_wildcard(pattern, text, true),
                should_match,
                "{pattern:?} against {text:?}"
            );
        }
    }

    /// The wildcard rule stated directly: a run tries every split of the text, any other element
    /// takes one character. Exponential, which is harmless at the drawn lengths.
    fn matches_elements_naively(elements: &[PatternElement], text: &[char]) -> bool {
        match elements.split_first() {
            None => text.is_empty(),
            Some((PatternElement::AnyRun, elements_rest)) => {
                (0..=text.len()).any(|skipped_count| matches_elements_naively(elements_rest, &text[skipped_count..]))
            }
            Some((element, elements_rest)) => text.split_first().is_some_and(|(character, text_rest)| {
                let is_element_match = match element {
                    PatternElement::Literal(literal) => literal == character,
                    PatternElement::AnyRun | PatternElement::AnyCharacter => true,
                };
                is_element_match && matches_elements_naively(elements_rest, text_rest)
            }),
        }
    }

    /// `elements` spelled as a pattern: a character with a meaning of its own is escaped, and so,
    /// half of the time, is any other.
    fn render_pattern(generator: &mut StdRng, elements: &[PatternElement]) -> String {
        let mut pattern = String::new();
        for element in elements {
            match element {
                PatternElement::AnyRun => pattern.push('*'),
                PatternElement::AnyCharacter => pattern.push('?'),
                PatternElement::Literal(literal) => {
                    if matches!(literal, '*' | '?' | '\\') || generator.random_bool(0.5) {
                        pattern.push('\\');
                    }
                    pattern.push(*literal);
                }
            }
        }
        pattern
    }

    /// A text `elements` matches: each run replaced by up to two drawn characters.
    fn instantiate_elements(generator: &mut StdRng, elements: &[PatternElement]) -> Vec<char> {
        let mut text = Vec::new();
        for element in elements {
            match element {
                PatternElement::AnyRun => {
                    let run_length = generator.random_range(0..=2);
                    text.extend((0..run_length).map(|_| draw_wildcard_character(generator)));
                }
                PatternElement::AnyCharacter => text.push(draw_wildcard_character(generator)),
                PatternElement::Literal(literal) => text.push(*literal),
            }
        }
        text
    }

    fn draw_wildcard_character(generator: &mut StdRng) -> char {
        WILDCARD_ALPHABET[generator.random_range(0..WILDCARD_ALPHABET.len())]
    }

    fn draw_pattern_element(generator: &mut StdRng) -> PatternElement {
        match generator.random_range(0..4) {
            0 => PatternElement::AnyRun,
            1 => PatternElement::AnyCharacter,
            _ => PatternElement::Literal(draw_wildcard_character(generator)),
        }
    }

    /// Half of the texts instantiate their pattern and half are drawn freely, so both outcomes are
    /// exercised; the counts prove it.
    #[test]
    fn a_wildcard_agrees_with_a_naive_reference_on_seeded_patterns_and_texts() {
        let mut generator = StdRng::seed_from_u64(0x1CE6_A7E1);
        let mut matched_count = 0;
        for _ in 0..WILDCARD_CASE_COUNT {
            let element_count = generator.random_range(0..=6);
            let elements: Vec<PatternElement> =
                (0..element_count).map(|_| draw_pattern_element(&mut generator)).collect();
            let pattern = render_pattern(&mut generator, &elements);
            let text_characters = if generator.random_bool(0.5) {
                instantiate_elements(&mut generator, &elements)
            } else {
                let text_length = generator.random_range(0..=8);
                (0..text_length).map(|_| draw_wildcard_character(&mut generator)).collect()
            };
            let text: String = text_characters.iter().collect();
            let should_match = matches_elements_naively(&elements, &text_characters);

            assert_eq!(
                matches_wildcard(&pattern, &text, false),
                should_match,
                "{pattern:?} against {text:?}"
            );
            matched_count += usize::from(should_match);
        }
        assert!(matched_count >= WILDCARD_CASE_COUNT / 4, "{matched_count} matches");
        assert!(
            WILDCARD_CASE_COUNT - matched_count >= WILDCARD_CASE_COUNT / 4,
            "{matched_count} matches"
        );
    }

    #[tokio::test]
    async fn a_date_term_that_does_not_parse_is_a_query_shard_400() {
        let mappings = json!({"properties": {"updated_at": {"type": "date"}}});
        let source = json!({"updated_at": "2026-07-10T12:34:56.788Z"});

        assert_query_shard_refused(&json!({"term": {"updated_at": "not a date"}}), &source, &mappings).await;
        assert_query_shard_refused(&json!({"terms": {"updated_at": ["not a date"]}}), &source, &mappings).await;
    }

    /// A saved-objects mapping: one object per type and `nested` references.
    fn build_find_mappings() -> Value {
        json!({"dynamic": "strict", "properties": {
            "type": {"type": "keyword"},
            "dashboard": {"properties": {"title": {"type": "text"}}},
            "visualization": {"properties": {"title": {"type": "text"}}},
            "index-pattern": {"properties": {"title": {"type": "text"}}},
            "references": {"type": "nested", "properties": {"id": {"type": "keyword"}, "type": {"type": "keyword"}}}
        }})
    }

    /// Saved objects; titles analyze to: "errors", "by", "service" | "error", "rate" | "logs" |
    /// "error", "budget".
    fn build_find_sources() -> Vec<(&'static str, Value)> {
        vec![
            (
                "dashboard:errors",
                json!({"type": "dashboard", "dashboard": {"title": "Errors by service"}, "references": [{"id": "rate", "type": "visualization"}]}),
            ),
            (
                "visualization:rate",
                json!({"type": "visualization", "visualization": {"title": "Error rate"}}),
            ),
            (
                "index-pattern:logs",
                json!({"type": "index-pattern", "index-pattern": {"title": "logs-*"}}),
            ),
            (
                "dashboard:budget",
                json!({"type": "dashboard", "dashboard": {"title": "error budget"}}),
            ),
        ]
    }

    /// The ids of the sources `query` matches, in the order of [`build_find_sources`].
    fn find_matching_ids(query: &Value) -> Vec<&'static str> {
        let query = parse_query(query).expect("the query parses");
        let mappings = build_find_mappings();
        let evaluator = IndexQueryEvaluator::new(&query, ".kibana_1", &mappings, parse_request_instant())
            .expect("the query prepares");
        build_find_sources()
            .into_iter()
            .filter(|(id, source)| evaluator.matches_document(id, source).expect("the query evaluates"))
            .map(|(id, _)| id)
            .collect()
    }

    #[test]
    fn a_find_style_simple_query_string_over_all_fields_matches_the_stated_documents() {
        let build_query = |text: &str, operator: &str| json!({"simple_query_string": {"query": text, "fields": ["*"], "default_operator": operator}});

        assert_eq!(
            find_matching_ids(&build_query("error*", "or")),
            ["dashboard:errors", "visualization:rate", "dashboard:budget"]
        );
        assert_eq!(find_matching_ids(&build_query("errors", "or")), ["dashboard:errors"]);
        // `type` is a keyword field: the whole value matches.
        assert_eq!(
            find_matching_ids(&build_query("dashboard", "or")),
            ["dashboard:errors", "dashboard:budget"]
        );
        assert_eq!(
            find_matching_ids(&build_query("dash*", "or")),
            ["dashboard:errors", "dashboard:budget"]
        );
        // `-rate` is "everything without rate", so with `or` every object lacking the token
        // matches; `rate` inside the nested references is not a field of `*`.
        assert_eq!(
            find_matching_ids(&build_query("error -rate", "or")),
            [
                "dashboard:errors",
                "visualization:rate",
                "index-pattern:logs",
                "dashboard:budget"
            ]
        );
        assert_eq!(
            find_matching_ids(&build_query("error -rate", "and")),
            ["dashboard:budget"]
        );
        assert_eq!(
            find_matching_ids(&build_query("\"error rate\"", "or")),
            ["visualization:rate"]
        );
        // `!!!` yields no token on the text fields and equals no keyword value.
        assert!(find_matching_ids(&build_query("!!!", "or")).is_empty());
        assert!(find_matching_ids(&build_query("", "or")).is_empty());
    }

    #[test]
    fn simple_query_string_over_a_type_field_matches_only_that_field() {
        let build_query = |fields: Value| json!({"simple_query_string": {"query": "error", "fields": fields}});

        assert_eq!(
            find_matching_ids(&build_query(json!(["visualization.title"]))),
            ["visualization:rate"]
        );
        assert_eq!(
            find_matching_ids(&build_query(json!(["dashboard.title^2"]))),
            ["dashboard:budget"]
        );
        assert_eq!(
            find_matching_ids(&build_query(json!(["*.title"]))),
            ["visualization:rate", "dashboard:budget"]
        );
    }

    #[test]
    fn a_clause_without_tokens_drops_out_of_its_group() {
        let build_query = |text: &str, operator: &str| {
            json!({"simple_query_string": {
                "query": text,
                "fields": ["visualization.title"],
                "default_operator": operator
            }})
        };

        // With `and`, a clause that stayed in its group would fail every document.
        assert_eq!(
            find_matching_ids(&build_query("error !!!", "and")),
            ["visualization:rate"]
        );
        // The negated clause drops out, and the match-all it brought drops out with it.
        assert!(find_matching_ids(&build_query("-!!!", "or")).is_empty());
        // `(!!! ???)` is a group left with nothing, so it drops out as one clause.
        assert_eq!(
            find_matching_ids(&build_query("error (!!! ???)", "and")),
            ["visualization:rate"]
        );
        assert!(find_matching_ids(&build_query("!!! ???", "or")).is_empty());
        // A prefix term always contributes: `!!!*` is a prefix of no token.
        assert!(find_matching_ids(&build_query("error !!!*", "and")).is_empty());
    }

    #[test]
    fn the_tokens_of_one_simple_query_string_term_join_with_the_default_operator() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let source = json!({"title": "error rate"});
        let build_query = |text: &str, operator: &str| json!({"simple_query_string": {"query": text, "fields": ["title"], "default_operator": operator}});

        // `\ ` keeps the space inside one term, which the analyzer then splits.
        assert_matches(
            &[
                (build_query("error\\ budget", "or"), true),
                (build_query("error\\ budget", "and"), false),
                (build_query("error\\ rate", "and"), true),
            ],
            &source,
            &mappings,
        );
    }

    #[tokio::test]
    async fn a_prefix_term_on_a_typed_field_is_a_query_shard_400_unless_lenient() {
        let mappings = json!({"properties": {
            "count": {"type": "integer"},
            "updated_at": {"type": "date"},
            "enabled": {"type": "boolean"}
        }});
        let source = json!({"count": 1, "updated_at": "2026-07-10T12:34:56.788Z", "enabled": true});

        for field in ["count", "updated_at", "enabled"] {
            assert_query_shard_refused(
                &json!({"simple_query_string": {"query": "1*", "fields": [field]}}),
                &source,
                &mappings,
            )
            .await;
            assert_matches(
                &[(
                    json!({"simple_query_string": {"query": "1*", "fields": [field], "lenient": true}}),
                    false,
                )],
                &source,
                &mappings,
            );
        }
    }

    #[tokio::test]
    async fn a_typed_field_takes_the_whole_text_as_a_term() {
        let mappings = json!({"properties": {
            "updated_at": {"type": "date"},
            "enabled": {"type": "boolean"},
            "rating": {"type": "double"}
        }});
        let source = json!({"updated_at": "2026-07-10T12:34:56.788Z", "enabled": false, "rating": 2.5});

        assert_matches(
            &[
                (json!({"match": {"updated_at": "2026-07-10"}}), true),
                (json!({"match": {"updated_at": "2026-07-11"}}), false),
                (json!({"match": {"enabled": false}}), true),
                (json!({"match": {"enabled": "true"}}), false),
                (json!({"match_phrase": {"rating": "2.50"}}), true),
                (
                    json!({"simple_query_string": {"query": "2.5", "fields": ["rating"]}}),
                    true,
                ),
                (
                    json!({"multi_match": {"query": "maybe", "fields": ["enabled"], "lenient": true}}),
                    false,
                ),
            ],
            &source,
            &mappings,
        );
        for refused_query in [
            json!({"match": {"updated_at": "not a date"}}),
            json!({"match": {"enabled": "yes"}}),
            json!({"multi_match": {"query": "maybe", "fields": ["enabled"]}}),
            json!({"simple_query_string": {"query": "maybe", "fields": ["rating"]}}),
        ] {
            assert_query_shard_refused(&refused_query, &source, &mappings).await;
        }
    }

    #[tokio::test]
    async fn lenient_defaults_to_true_only_when_the_fields_hold_the_all_fields_pattern() {
        let mappings = json!({"properties": {"count": {"type": "integer"}, "title": {"type": "text"}}});
        let source = json!({"count": 1, "title": "zero"});

        // `zero` is no integer, so only the leniency decides whether `count` refuses it.
        assert_matches(
            &[
                (json!({"multi_match": {"query": "zero"}}), true),
                (json!({"multi_match": {"query": "zero", "fields": ["*"]}}), true),
                (
                    json!({"multi_match": {"query": "zero", "fields": ["*", "count"]}}),
                    true,
                ),
                (json!({"simple_query_string": {"query": "zero"}}), true),
                (
                    json!({"simple_query_string": {"query": "zero", "fields": ["count", "*"]}}),
                    true,
                ),
            ],
            &source,
            &mappings,
        );
        for refused_query in [
            json!({"multi_match": {"query": "zero", "fields": ["*"], "lenient": false}}),
            json!({"multi_match": {"query": "zero", "fields": ["count", "title"]}}),
            json!({"simple_query_string": {"query": "zero", "fields": ["*"], "lenient": false}}),
        ] {
            assert_query_shard_refused(&refused_query, &source, &mappings).await;
        }
    }

    #[tokio::test]
    async fn a_text_that_does_not_parse_refuses_even_when_another_field_matches() {
        let mappings = json!({"properties": {"a_title": {"type": "text"}, "count": {"type": "integer"}}});
        let source = json!({"a_title": "zero", "count": 0});

        // `a_title` holds the text and sorts before `count`, so a search that ended at its first
        // match would answer without reading `count`, and the answer would depend on the document.
        for refused_query in [
            json!({"multi_match": {"query": "zero", "fields": ["count", "a_title"]}}),
            json!({"simple_query_string": {"query": "zero", "fields": ["count", "a_title"]}}),
        ] {
            assert_query_shard_refused(&refused_query, &source, &mappings).await;
        }
    }

    #[tokio::test]
    async fn full_text_on_an_object_an_unknown_path_or_an_unsupported_type_follows_the_field() {
        let mappings = json!({"properties": {
            "dashboard": {"properties": {"title": {"type": "text"}}},
            "references": {"type": "nested", "properties": {"id": {"type": "keyword"}}},
            "geo": {"type": "geo_point"},
            "filters": {"type": "object", "enabled": false}
        }});
        let source = json!({
            "dashboard": {"title": "Logs"},
            "references": [{"id": "rate"}],
            "geo": "x",
            "filters": {"query": "error"}
        });

        assert_matches(
            &[
                (json!({"match": {"dashboard": "logs"}}), false),
                (json!({"match": {"references": "rate"}}), false),
                (json!({"match": {"absent": "logs"}}), false),
                (json!({"match_phrase": {"absent": "logs"}}), false),
                (json!({"match_phrase_prefix": {"dashboard": "logs"}}), false),
                (json!({"multi_match": {"query": "logs", "fields": ["absent"]}}), false),
                (
                    json!({"simple_query_string": {"query": "logs", "fields": ["absent"]}}),
                    false,
                ),
                // Inside `nested` a field reads the current element.
                (
                    json!({"nested": {"path": "references", "query": {"match": {"references.id": "rate"}}}}),
                    true,
                ),
                // A pattern inside `nested` names the element's paths, not the document's.
                (
                    json!({"nested": {"path": "references", "query": {"multi_match": {"query": "rate"}}}}),
                    true,
                ),
                (
                    json!({"nested": {"path": "references", "query": {"simple_query_string": {"query": "rate"}}}}),
                    true,
                ),
                (
                    json!({"nested": {"path": "references", "query": {"multi_match": {"query": "rate", "fields": ["references.*"]}}}}),
                    true,
                ),
                (
                    json!({"nested": {"path": "references", "query": {"multi_match": {"query": "logs"}}}}),
                    false,
                ),
                // `*` passes over the field whose type the evaluator cannot read.
                (json!({"multi_match": {"query": "logs"}}), true),
                // `nested` accepts a path that is not a nested field, and patterns stay outside it
                // all the same: resolving `geo` would be a 400 that `lenient` could not lift.
                (
                    json!({"nested": {"path": "filters", "query": {"multi_match": {"query": "error"}}}}),
                    false,
                ),
                (
                    json!({"nested": {"path": "geo", "query": {"multi_match": {"query": "x"}}}}),
                    false,
                ),
                (
                    json!({"nested": {"path": "geo", "query": {"multi_match": {"query": "x", "lenient": false}}}}),
                    false,
                ),
                // A field named outright is not a pattern.
                (
                    json!({"nested": {"path": "filters", "query": {"match": {"filters.query": "error"}}}}),
                    true,
                ),
            ],
            &source,
            &mappings,
        );
        assert_matches(
            &[(
                json!({"nested": {"path": "geo", "query": {"multi_match": {"query": "1"}}}}),
                false,
            )],
            &json!({"geo": {"lat": 1}}),
            &mappings,
        );
        assert_refused(&json!({"match": {"geo": "x"}}), &source, &mappings).await;
        assert_refused(&json!({"match_phrase": {"geo": "x"}}), &source, &mappings).await;
        assert_refused(
            &json!({"multi_match": {"query": "x", "fields": ["geo"]}}),
            &source,
            &mappings,
        )
        .await;
        assert_refused(
            &json!({"simple_query_string": {"query": "x", "fields": ["geo"]}}),
            &source,
            &mappings,
        )
        .await;
    }

    #[test]
    fn patterns_inside_nested_stop_at_any_step_of_the_path_the_mapping_closes() {
        let build_query =
            |path: &str, text: &str| json!({"nested": {"path": path, "query": {"multi_match": {"query": text}}}});
        let mappings = json!({"properties": {
            "dashboard": {"properties": {"title": {"type": "text"}}},
            "filters": {"type": "object", "enabled": false},
            "panels": {"enabled": false, "properties": {"grid": {"type": "object"}}},
            "geo": {"type": "geo_point"},
            "title": {"type": "text", "fields": {"geo": {"type": "geo_point"}}},
            "references": {"type": "nested", "properties": {
                "parts": {"type": "nested", "properties": {"id": {"type": "keyword"}}}
            }}
        }});

        // Each of these reads as an undeclared path past the step that closes it.
        assert_matches(
            &[
                (build_query("filters.sub", "error"), false),
                (build_query("panels.grid", "error"), false),
                (build_query("geo.lat", "1"), false),
                (build_query("title.geo", "1"), false),
                (build_query("title.geo.x", "1"), false),
            ],
            &json!({
                "filters": {"sub": {"query": "error"}},
                "panels": {"grid": {"query": "error"}},
                "geo": {"lat": {"x": 1}},
                "title": {"geo": {"x": 1}}
            }),
            &mappings,
        );
        // A step that closes nothing leaves the walk open: a `nested` step the query went through,
        // and a step past what the mapping declares.
        assert_matches(
            &[
                (
                    json!({"nested": {"path": "references", "query": {
                        "nested": {"path": "references.parts", "query": {"multi_match": {"query": "rate"}}}
                    }}}),
                    true,
                ),
                (build_query("dashboard.extra", "logs"), true),
                (build_query("extra", "logs"), true),
            ],
            &json!({
                "references": [{"parts": [{"id": "rate"}]}],
                "dashboard": {"extra": {"note": "logs"}},
                "extra": {"note": "logs"}
            }),
            &mappings,
        );
    }

    #[test]
    fn match_phrase_prefix_matches_every_value_sharing_the_prefix() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let query = json!({"match_phrase_prefix": {"title": "service se"}});

        // 80 values whose second token starts with `se`: no cap on how many tokens a prefix
        // reaches.
        for number in 0..80 {
            let source = json!({"title": format!("Service serv{number:02}x")});
            assert!(evaluate(&query, &source, &mappings).expect("evaluates"), "{source}");
        }
        assert_matches(&[(query.clone(), false)], &json!({"title": "service other"}), &mappings);
        assert_matches(&[(query, false)], &json!({"title": "serv01x service"}), &mappings);
    }

    #[tokio::test]
    async fn match_phrase_prefix_on_a_keyword_field_is_a_query_shard_400() {
        let mappings = json!({"properties": {"type": {"type": "keyword"}, "count": {"type": "integer"}}});
        let source = json!({"type": "dashboard", "count": 1});

        for query in [
            json!({"match_phrase_prefix": {"type": "dash"}}),
            json!({"match_phrase_prefix": {"count": "1"}}),
            json!({"match_phrase_prefix": {"_id": "doc"}}),
        ] {
            assert_query_shard_refused(&query, &source, &mappings).await;
        }
    }

    #[tokio::test]
    async fn match_with_zero_on_an_integer_field_compares_numerically() {
        let mappings = json!({"properties": {"count": {"type": "integer"}}});

        assert_matches(
            &[
                (json!({"match": {"count": 0}}), true),
                (json!({"match": {"count": "0"}}), true),
                (json!({"match": {"count": {"query": "0.0"}}}), true),
                (json!({"match": {"count": 1}}), false),
                (json!({"match": {"count": {"query": "zero", "lenient": true}}}), false),
                (json!({"match_phrase": {"count": "0"}}), true),
            ],
            &json!({"count": 0}),
            &mappings,
        );
        assert_matches(
            &[(json!({"match": {"count": 0}}), true)],
            &json!({"count": "0"}),
            &mappings,
        );
        assert_query_shard_refused(&json!({"match": {"count": "zero"}}), &json!({"count": 0}), &mappings).await;
    }

    #[test]
    fn match_follows_zero_terms_query_when_the_text_yields_no_token() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let source = json!({"title": "error rate"});

        assert_matches(
            &[
                (json!({"match": {"title": "!!!"}}), false),
                (
                    json!({"match": {"title": {"query": "!!!", "zero_terms_query": "all"}}}),
                    true,
                ),
                (
                    json!({"match": {"title": {"query": "error budget", "operator": "and"}}}),
                    false,
                ),
                (
                    json!({"match": {"title": {"query": "error budget", "operator": "or"}}}),
                    true,
                ),
                (json!({"match": {"title": "ERROR"}}), true),
                (json!({"match_phrase": {"title": "!!!"}}), false),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_match_phrase_matches_consecutive_tokens_only() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let source = json!({"title": ["error rate by service", "Rate Error"]});

        assert_matches(
            &[
                (json!({"match_phrase": {"title": "Error Rate"}}), true),
                (json!({"match_phrase": {"title": "rate error"}}), true),
                (json!({"match_phrase": {"title": "error service"}}), false),
                (json!({"match_phrase": {"title": "service rate"}}), false),
                (json!({"match_phrase_prefix": {"title": "by serv"}}), true),
                (json!({"match_phrase_prefix": {"title": "rate serv"}}), false),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_match_phrase_prefix_with_boost_and_an_empty_query_is_accepted() {
        let mappings = json!({"properties": {"index-pattern": {"properties": {"title": {"type": "text"}}}}});

        // The empty query parses (its `boost` is ignored) and yields no token: it matches nothing.
        assert_matches(
            &[(
                json!({"match_phrase_prefix": {"index-pattern.title": {"query": "", "boost": 1}}}),
                false,
            )],
            &json!({"index-pattern": {"title": "logs-*"}}),
            &mappings,
        );
    }

    #[test]
    fn multi_match_best_fields_requires_one_field_to_satisfy_the_operator() {
        let mappings = json!({"properties": {"title": {"type": "text"}, "description": {"type": "text"}}});
        let source = json!({"title": "alpha beta", "description": "gamma delta"});
        let build_multi_match = |query: &str, operator: &str, match_type: &str| json!({"multi_match": {"query": query, "fields": ["title", "description"], "operator": operator, "type": match_type}});

        assert_matches(
            &[
                (build_multi_match("alpha gamma", "and", "best_fields"), false),
                (build_multi_match("alpha gamma", "or", "best_fields"), true),
                (build_multi_match("alpha beta", "and", "best_fields"), true),
                (build_multi_match("beta alpha", "or", "phrase"), false),
                (build_multi_match("gamma delta", "or", "phrase"), true),
                (json!({"multi_match": {"query": "delta"}}), true),
                (json!({"multi_match": {"query": "!!!"}}), false),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn all_fields_skip_what_the_mapping_does_not_index() {
        let mappings = json!({"properties": {
            "title": {"type": "text"},
            "summary": {"type": "text", "index": false, "fields": {"keyword": {"type": "keyword"}}},
            "panelsJSON": {"type": "text", "index": false},
            "timeRestore": {"type": "boolean", "index": false},
            "filters": {"type": "object", "enabled": false}
        }});
        let source = json!({
            "title": "Latency",
            "summary": "budget",
            "panelsJSON": "error",
            "timeRestore": true,
            "filters": {"query": "error"}
        });
        let build_query = |text: &str, fields: Value| json!({"simple_query_string": {"query": text, "fields": fields}});

        assert_matches(
            &[
                (build_query("latency", json!(["*"])), true),
                (build_query("error", json!(["*"])), false),
                (build_query("true", json!(["*"])), false),
                (build_query("Budget", json!(["*"])), false),
                // Through `summary.keyword`, which the mapping indexes although `summary` it does not.
                (build_query("budget", json!(["*"])), true),
                (build_query("error", json!(["panels*"])), false),
                // A field named explicitly is read whatever the mapping says of it.
                (build_query("error", json!(["panelsJSON"])), true),
                (build_query("error", json!(["filters.query"])), true),
            ],
            &source,
            &mappings,
        );
        // The options as strings, and a multi-field the mapping does not index either.
        assert_matches(
            &[(build_query("error", json!(["*"])), false)],
            &json!({"body": "error", "meta": {"note": "error"}}),
            &json!({"properties": {
                "body": {"type": "text", "index": "false"},
                "meta": {"type": "object", "enabled": "false"}
            }}),
        );
        assert_matches(
            &[(build_query("!!!", json!(["*"])), false)],
            &json!({"title": "!!!"}),
            &json!({"properties": {
                "title": {"type": "text", "fields": {"raw": {"type": "keyword", "index": false}}}
            }}),
        );
    }

    #[test]
    fn enabled_on_a_multi_field_closes_nothing_to_patterns() {
        let mappings = json!({"properties": {
            "summary": {"type": "text", "index": false, "fields": {"raw": {"type": "keyword", "enabled": false}}}
        }});
        let build_multi_match = |text: &str| json!({"multi_match": {"query": text}});

        // `summary` is not indexed, so only its multi-field can answer.
        assert_matches(
            &[(build_multi_match("budget"), true), (build_multi_match("rate"), false)],
            &json!({"summary": "budget"}),
            &mappings,
        );
        // The other route to the same node: a step of a path, which reads `fields` when `properties`
        // misses. Both routes have to agree on what `enabled` means here.
        assert_matches(
            &[(
                json!({"nested": {"path": "summary.raw", "query": {"multi_match": {"query": "logs"}}}}),
                true,
            )],
            &json!({"summary": {"raw": {"note": "logs"}}}),
            &mappings,
        );
    }

    #[test]
    fn all_fields_reach_declared_multi_fields_and_the_keyword_of_an_undeclared_string() {
        let mappings = json!({"properties": {
            "title": {"type": "text", "fields": {"raw": {"type": "keyword"}}}
        }});
        // Neither value yields a token, so only a whole-value subfield can match them.
        let source = json!({"title": "!!!", "note": "???"});
        let build_query = |text: &str| json!({"simple_query_string": {"query": text}});

        assert_matches(
            &[
                (build_query("!!!"), true),
                (build_query("???"), true),
                (build_query("!!"), false),
                (build_query("??"), false),
                (build_query("!!*"), true),
                (build_query("??*"), true),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn all_fields_pass_over_a_field_of_an_unsupported_type() {
        let build_multi_match = |text: &str| json!({"multi_match": {"query": text}});

        // A multi-field of such a type, which every document holding its parent would offer. The
        // refusal would come from resolving the field, so `lenient` could not lift it.
        assert_matches(
            &[
                (build_multi_match("logs"), true),
                (json!({"multi_match": {"query": "logs", "lenient": false}}), true),
                (build_multi_match("maps"), false),
            ],
            &json!({"title": "logs"}),
            &json!({"properties": {"title": {"type": "text", "fields": {"geo": {"type": "geo_point"}}}}}),
        );
        // An object at such a field: `geo.lat` must not read as an undeclared number under it.
        assert_matches(
            &[(build_multi_match("1"), false)],
            &json!({"geo": {"lat": 1}}),
            &json!({"properties": {"geo": {"type": "geo_point"}}}),
        );
        // An object at a `text` field, whose keys name its multi-fields. Those read the parent's
        // values, so neither key matches, and `geo` must not refuse.
        assert_matches(
            &[(build_multi_match("x"), false), (build_multi_match("1"), false)],
            &json!({"title": {"raw": "x", "geo": 1}}),
            &json!({"properties": {"title": {"type": "text", "fields": {
                "raw": {"type": "keyword"},
                "geo": {"type": "geo_point"}
            }}}}),
        );
    }

    #[test]
    fn all_fields_read_a_dotted_key_as_the_path_it_spells() {
        let mappings = json!({"properties": {
            "dashboard": {"properties": {"title": {"type": "text"}}},
            "filters": {"type": "object", "enabled": false},
            "panels": {"enabled": false, "properties": {"grid": {"type": "text"}}},
            "geo": {"type": "geo_point"},
            "title": {"type": "text", "fields": {"geo": {"type": "geo_point"}}},
            "references": {"type": "nested", "properties": {"id": {"type": "keyword"}}}
        }});

        for (text, source, should_match) in [
            ("logs", json!({"dashboard.title": "logs"}), true),
            ("logs", json!({"dashboard.extra": "logs"}), true),
            ("error", json!({"filters.query": "error"}), false),
            ("error", json!({"panels.grid": "error"}), false),
            ("1", json!({"geo.lat": 1}), false),
            ("1", json!({"title.geo": 1}), false),
            ("rate", json!({"references.id": "rate"}), false),
        ] {
            assert_matches(
                &[(json!({"multi_match": {"query": text}}), should_match)],
                &source,
                &mappings,
            );
        }
    }

    #[test]
    fn all_fields_read_arrays_nulls_and_undeclared_scalars() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let source = json!({"tags": ["alpha", null, "beta"], "size": 5, "flag": true, "empty": null});
        let build_query = |text: &str, fields: Value| json!({"simple_query_string": {"query": text, "fields": fields}});

        assert_matches(
            &[
                (build_query("alpha", json!(["*"])), true),
                (build_query("beta", json!(["*"])), true),
                (build_query("5", json!(["*"])), true),
                (build_query("true", json!(["*"])), true),
                // `size` and `flag` cannot read `zero`, and `*` makes that no match, not a refusal.
                (build_query("zero", json!(["*"])), false),
                (build_query("alpha", json!(["ta?s"])), true),
                (build_query("alpha", json!(["t?s"])), false),
                // `empty` offers no field, so the negated clause has nothing to read and drops out;
                // a field offered with no value would contribute, and the negation would match.
                (build_query("-alpha", json!(["empty*"])), false),
            ],
            &source,
            &mappings,
        );
        // A stored source is always an object: this holds the walk's guard on an empty path.
        assert_matches(&[(build_query("logs", json!(["*"])), false)], &json!("logs"), &mappings);
    }

    #[test]
    fn the_id_field_matches_full_text_as_a_whole_value() {
        let mappings = json!({"properties": {"type": {"type": "keyword"}}});
        let source = json!({"type": "dashboard"});
        let build_query = |text: &str| json!({"simple_query_string": {"query": text, "fields": ["_id"]}});

        assert_matches(
            &[
                (build_query("doc-1"), true),
                (build_query("doc-*"), true),
                (build_query("doc-2"), false),
                (build_query("DOC-1"), false),
                (build_query("DOC*"), false),
                (json!({"match": {"_id": "doc-1"}}), true),
                (json!({"match_phrase": {"_id": "doc-1"}}), true),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_keyword_field_matches_full_text_as_a_whole_value() {
        let mappings = json!({"properties": {"title": {"type": "keyword"}}});
        let source = json!({"title": ["Error rate", 7]});

        assert_matches(
            &[
                (json!({"match": {"title": "Error rate"}}), true),
                (json!({"match": {"title": "error rate"}}), false),
                (json!({"match": {"title": "Error"}}), false),
                (json!({"match": {"title": 7}}), true),
                (json!({"match_phrase": {"title": "Error rate"}}), true),
                (
                    json!({"simple_query_string": {"query": "\"Error rate\"", "fields": ["title"]}}),
                    true,
                ),
                (
                    json!({"simple_query_string": {"query": "Err*", "fields": ["title"]}}),
                    true,
                ),
                (
                    json!({"simple_query_string": {"query": "err*", "fields": ["title"]}}),
                    false,
                ),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_text_field_prefix_term_is_lowercased_and_prefixes_one_token() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let source = json!({"title": ["Error rate", {"not": "text"}]});
        let build_query = |text: &str| json!({"simple_query_string": {"query": text, "fields": ["title"]}});

        assert_matches(
            &[
                (build_query("ERR*"), true),
                (build_query("RA*"), true),
                (build_query("rror*"), false),
                (build_query("not*"), false),
            ],
            &source,
            &mappings,
        );
    }

    #[tokio::test]
    async fn the_deepest_accepted_simple_query_string_evaluates() {
        let build_text = |operator_count: usize| {
            (0..operator_count).fold("error".to_string(), |text, level| {
                let operator = if level % 2 == 0 { '+' } else { '|' };
                format!("{text}{operator}error")
            })
        };
        let build_query = |text: String| json!({"simple_query_string": {"query": text, "fields": ["title"]}});
        let mappings = json!({"properties": {"title": {"type": "text"}}});

        // Each operator differs from the last, so 128 of them nest 128 levels and one more is refused.
        let refusal = parse_query(&build_query(build_text(129))).expect_err("refused");
        let (status, body) = render_envelope(refusal).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
        assert_eq!(body["error"]["limit"], 128);
        let deepest_query = build_query(build_text(128));
        assert_matches(&[(deepest_query.clone(), true)], &json!({"title": "error"}), &mappings);
        assert_matches(&[(deepest_query, false)], &json!({"title": "rate"}), &mappings);
    }

    /// A `query_string` text of several tokens on a text field resolves to one token set, which
    /// answers what one `term` per token joined by the operator answers: `or` needs one token,
    /// `and` needs each token in some stored value, not all of them in one. On a keyword field the
    /// adjacent terms stay one whole value.
    #[test]
    fn a_query_string_text_of_several_tokens_matches_one_or_every_token_of_a_text_field() {
        let mappings = json!({"properties": {"title": {"type": "text"}, "kind": {"type": "keyword"}}});
        let source = json!({"title": ["Connection refused", "upstream timeout"], "kind": "refused timeout"});
        let build_query = |text: &str, field: &str, operator: &str| json!({"query_string": {"query": text, "default_field": field, "default_operator": operator}});
        assert_matches(
            &[
                (build_query("refused absent", "title", "or"), true),
                (build_query("absent missing", "title", "or"), false),
                (build_query("refused timeout", "title", "and"), true),
                (build_query("refused absent", "title", "and"), false),
                (build_query("Refused-Timeout", "title", "and"), true),
                (build_query("refused", "kind", "or"), false),
                (build_query("refused timeout", "kind", "and"), true),
            ],
            &source,
            &mappings,
        );
    }

    #[test]
    fn a_query_string_on_a_system_document_resolves_against_its_mapping_and_its_own_leaves() {
        let mappings = json!({
            "properties": {
                "type": {"type": "keyword"},
                "index-pattern": {"properties": {"title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}}}
            }
        });
        let logs = json!({"type": "index-pattern", "index-pattern": {"title": "Logs-*"}, "note": "first pattern"});
        let metrics = json!({"type": "index-pattern", "index-pattern": {"title": "metrics-*"}});
        let rows = [
            ("index-pattern.title:logs*", true, false),
            // A keyword prefix over the whole value, not normalized; the multi-field reads the
            // values of its parent path.
            ("index-pattern.title.keyword:Logs-*", true, false),
            ("type:index-pattern AND NOT index-pattern.title:metrics", true, false),
            // `note` is not mapped: typed `text` from the document's own value.
            ("note:\"first pattern\"", true, false),
            ("note:pattern", true, false),
            ("note:\"pattern first\"", false, false),
        ];
        for (text, matches_logs, matches_metrics) in rows {
            let query = parse_query(&json!({"query_string": {"query": text}})).expect("the query parses");
            let evaluator =
                IndexQueryEvaluator::new(&query, ".kibana_1", &mappings, parse_request_instant()).expect("prepares");
            assert_eq!(
                evaluator.matches_document("index-pattern:logs", &logs).expect("evaluates"),
                matches_logs,
                "[{text}] on logs"
            );
            assert_eq!(
                evaluator
                    .matches_document("index-pattern:metrics", &metrics)
                    .expect("evaluates"),
                matches_metrics,
                "[{text}] on metrics"
            );
        }
    }

    /// A syntax error is carried by the parsed query and answered by the evaluated index as its
    /// query-shard failure.
    #[tokio::test]
    async fn a_query_string_syntax_error_is_the_query_shard_failure_of_the_evaluated_index() {
        let query = parse_query(&json!({"query_string": {"query": "a/b"}})).expect("the query parses");
        let mappings = json!({});
        let error = IndexQueryEvaluator::new(&query, ".kibana_7", &mappings, parse_request_instant())
            .and_then(|evaluator| evaluator.matches_document("1", &json!({})))
            .expect_err("refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "search_phase_execution_exception");
        assert_eq!(body["error"]["root_cause"][0]["type"], "query_shard_exception");
        assert_eq!(body["error"]["root_cause"][0]["index"], ".kibana_7");
    }

    /// The text field `title` analyzes to the tokens [logs, overview]; a `regexp` reads them as
    /// written, never normalizing the pattern.
    #[tokio::test]
    async fn a_regexp_on_a_system_document_matches_text_per_token_and_keywords_whole() {
        let mappings = json!({"properties": {
            "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "updated_at": {"type": "date"},
            "count": {"type": "integer"},
            "ratio": {"type": "float"},
            "enabled": {"type": "boolean"}
        }});
        let document = json!({
            "title": "Logs Overview",
            "updated_at": "2026-10-08T12:00:00Z",
            "meta": {"owner": "ops"},
            "size": 3
        });
        assert_matches(
            &[
                (json!({"regexp": {"title": "log.*"}}), true),
                (json!({"regexp": {"title": "Log.*"}}), false),
                (json!({"regexp": {"title": "over.*"}}), true),
                (json!({"regexp": {"title": "logs .*"}}), false),
                (
                    json!({"regexp": {"title": {"value": "LOG.*", "case_insensitive": true}}}),
                    true,
                ),
                (json!({"regexp": {"title.keyword": "Logs .*"}}), true),
                (json!({"regexp": {"title.keyword": "logs.*"}}), false),
                (json!({"regexp": {"meta.owner": "op."}}), true),
                (json!({"regexp": {"meta": ".*"}}), false),
                (json!({"regexp": {"_id": "doc-.*"}}), true),
                (json!({"regexp": {"_id": "dashboard.*"}}), false),
                (json!({"regexp": {"missing": ".*"}}), false),
            ],
            &document,
            &mappings,
        );
        for field in ["updated_at", "count", "ratio", "enabled", "size"] {
            assert_query_shard_refused(&json!({"regexp": {field: "1.*"}}), &document, &mappings).await;
        }
    }

    /// The pattern is translated before the field is resolved, so a malformed one fails the query
    /// whatever the document and its mappings hold.
    #[tokio::test]
    async fn a_malformed_regexp_on_an_absent_field_is_refused() {
        assert_refused(
            &json!({"regexp": {"absent": "(a"}}),
            &json!({}),
            &json!({"properties": {}}),
        )
        .await;
    }
}
