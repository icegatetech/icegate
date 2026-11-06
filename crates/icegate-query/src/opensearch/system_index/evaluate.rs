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
//! `text` fields are matched without tokenizing: `exists` is answered, and a `term` is answered
//! when the stored value consists only of ASCII digit groups joined by single dots (a version
//! such as `7.10.0`) and is no longer than `MAX_TOKEN_LENGTH`, because IceGate treats such a value
//! as one token equal to it. Every other leaf on a `text` field is a 400 rather than an answer
//! under a rule other than token matching.

use std::borrow::Cow;
use std::cmp::Ordering;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::opensearch::error::OpenSearchError;
use crate::query_dsl::ast::{BoolQuery, NestedQuery, Query, RangeBound, RangeQuery};
use crate::query_dsl::date_math::{BoundSide, parse_stored_date_millis, resolve_date_bound};

/// The metadata field naming a document's id.
pub(crate) const ID_FIELD: &str = "_id";

/// The subfield an inferred `text` value carries for exact matching.
const KEYWORD_SUBFIELD_SUFFIX: &str = ".keyword";

/// The longest value IceGate treats as one token, in UTF-16 units (digit groups are ASCII, so
/// bytes count the same).
const MAX_TOKEN_LENGTH: usize = 255;

/// How the evaluator compares a field's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// `illegal_argument_exception` naming `field` and its type for any other type.
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
            other => Err(OpenSearchError::illegal_argument(format!(
                "the field [{field}] of type [{other}] cannot be queried on a system index"
            ))),
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

/// Whether the document `id` with `source` matches `query` under `mappings`, with `now` the
/// request's instant for date math.
///
/// # Errors
///
/// `illegal_argument_exception` for a value that does not parse for its field's type, a leaf
/// on a `text` field outside the forms the module documentation lists, a string leaf on a
/// non-string field, or a mapping type the evaluator does not support.
pub(crate) fn matches_document(
    query: &Query,
    id: &str,
    source: &Value,
    mappings: &Value,
    now: DateTime<Utc>,
) -> Result<bool, OpenSearchError> {
    DocumentView {
        id,
        source,
        mappings,
        now,
        nested: None,
    }
    .matches_query(query)
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
/// key that itself contains dots is found as well as nested objects.
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

/// The values one document holds at a field path, and how to compare them.
struct DocumentFieldValues<'a> {
    /// `None` for a path with neither a mapping nor a value.
    kind: Option<FieldKind>,
    /// The non-null values at the path.
    values: Vec<&'a Value>,
}

/// One document under evaluation.
#[derive(Clone, Copy)]
struct DocumentView<'a> {
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
                matches_any_item(&terms.values, |value| {
                    self.matches_field_values(&terms.field, &field_values, value, false)
                })
            }
            Query::Ids(ids) => Ok(ids.values.iter().any(|value| value == self.id)),
            Query::Range(range) => self.matches_range(range),
            Query::Exists(exists) => self.has_field(&exists.field),
            Query::Prefix(prefix) => self.matches_pattern(&prefix.field, "prefix", |text| {
                starts_with_folded(text, &prefix.value, prefix.case_insensitive)
            }),
            Query::Wildcard(wildcard) => self.matches_pattern(&wildcard.field, "wildcard", |text| {
                matches_wildcard(&wildcard.pattern, text, wildcard.case_insensitive)
            }),
            Query::ConstantScore(inner) => self.matches_query(inner),
            Query::Nested(nested) => self.matches_nested(nested),
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
        if let Some(declared_type) = find_mapped_field(self.mappings, field)? {
            let source_path = if declared_type.is_multi_field {
                field.rsplit_once('.').map_or(field, |(parent, _)| parent)
            } else {
                field
            };
            return Ok(DocumentFieldValues {
                kind: Some(declared_type.kind),
                values: self.collect_values(source_path),
            });
        }
        if let Some(parent) = field.strip_suffix(KEYWORD_SUBFIELD_SUFFIX)
            && find_mapped_field(self.mappings, parent)?.is_none()
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
        self.matches_field_values(field, &self.resolve_field(field)?, value, is_case_insensitive)
    }

    /// Whether `field_values`, resolved from `field`, hold `value`. A `terms` compares several
    /// values against one field, and the resolution is the same for each.
    fn matches_field_values(
        &self,
        field: &str,
        field_values: &DocumentFieldValues<'a>,
        value: &Value,
        is_case_insensitive: bool,
    ) -> Result<bool, OpenSearchError> {
        let Some(kind) = field_values.kind else {
            return Ok(false);
        };
        match kind {
            FieldKind::Object | FieldKind::Nested => Ok(false),
            FieldKind::Identifier => Ok(render_text(value)
                .is_some_and(|expected_text| equals_text(self.id, &expected_text, is_case_insensitive))),
            FieldKind::Keyword => {
                let expected_text = render_text(value).ok_or_else(|| refuse_value(field, value))?;
                Ok(field_values.values.iter().any(|stored_value| {
                    render_text(stored_value)
                        .is_some_and(|text| equals_text(&text, &expected_text, is_case_insensitive))
                }))
            }
            FieldKind::Text => matches_text_term(field, value, &field_values.values),
            FieldKind::Integer | FieldKind::Float => {
                let expected_number = parse_numeric(value, field)?;
                Ok(field_values
                    .values
                    .iter()
                    .any(|stored_value| read_comparable_number(stored_value) == Some(expected_number)))
            }
            FieldKind::Boolean => {
                let expected_flag = parse_boolean(value, field)?;
                Ok(field_values
                    .values
                    .iter()
                    .any(|stored_value| read_boolean(stored_value) == Some(expected_flag)))
            }
            FieldKind::Date => {
                let text = render_text(value).ok_or_else(|| refuse_value(field, value))?;
                let start = resolve_date_bound(&text, None, None, self.now, BoundSide::LowerInclusive)?;
                let end = resolve_date_bound(&text, None, None, self.now, BoundSide::UpperInclusive)?;
                Ok(field_values.values.iter().any(|stored_value| {
                    read_date_micros(stored_value).is_some_and(|micros| (start..end).contains(&micros))
                }))
            }
        }
    }

    /// Whether `range.field` holds a value within the bounds, compared by the field's kind.
    fn matches_range(&self, range: &RangeQuery) -> Result<bool, OpenSearchError> {
        let field_values = self.resolve_field(&range.field)?;
        let Some(kind) = field_values.kind else {
            return Ok(false);
        };
        match kind {
            FieldKind::Object | FieldKind::Nested => Ok(false),
            FieldKind::Text => Err(refuse_text_leaf("range", &range.field)),
            FieldKind::Identifier => is_within_text_range(self.id, range),
            FieldKind::Keyword => matches_any_item(&field_values.values, |stored_value| {
                render_text(stored_value).map_or(Ok(false), |text| is_within_text_range(&text, range))
            }),
            FieldKind::Integer | FieldKind::Float => {
                let bounds = NumericBounds::from_range(range, parse_numeric)?;
                Ok(field_values.values.iter().any(|stored_value| {
                    read_comparable_number(stored_value).is_some_and(|number| bounds.contains(number))
                }))
            }
            FieldKind::Boolean => {
                let bounds =
                    NumericBounds::from_range(range, |value, field| parse_boolean(value, field).map(rank_boolean))?;
                Ok(field_values.values.iter().any(|stored_value| {
                    read_boolean(stored_value).is_some_and(|flag| bounds.contains(rank_boolean(flag)))
                }))
            }
            FieldKind::Date => {
                let lower = range
                    .lower
                    .as_ref()
                    .map(|bound| {
                        self.resolve_date_side(bound, range, BoundSide::LowerInclusive, BoundSide::LowerExclusive)
                    })
                    .transpose()?;
                let upper = range
                    .upper
                    .as_ref()
                    .map(|bound| {
                        self.resolve_date_side(bound, range, BoundSide::UpperInclusive, BoundSide::UpperExclusive)
                    })
                    .transpose()?;
                Ok(field_values.values.iter().any(|stored_value| {
                    read_date_micros(stored_value).is_some_and(|micros| {
                        lower.is_none_or(|lower| micros >= lower) && upper.is_none_or(|upper| micros < upper)
                    })
                }))
            }
        }
    }

    /// `bound` in microseconds, read as `inclusive_side` or `exclusive_side` by its inclusiveness.
    fn resolve_date_side(
        &self,
        bound: &RangeBound,
        range: &RangeQuery,
        inclusive_side: BoundSide,
        exclusive_side: BoundSide,
    ) -> Result<i64, OpenSearchError> {
        let text = render_text(&bound.value).ok_or_else(|| refuse_value(&range.field, &bound.value))?;
        let side = if bound.is_inclusive {
            inclusive_side
        } else {
            exclusive_side
        };
        resolve_date_bound(&text, range.format, range.time_zone.as_ref(), self.now, side)
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

    /// Whether `matcher` accepts the text of a value of the string field `field`.
    fn matches_pattern(
        &self,
        field: &str,
        leaf: &str,
        matcher: impl Fn(&str) -> bool,
    ) -> Result<bool, OpenSearchError> {
        let field_values = self.resolve_field(field)?;
        let Some(kind) = field_values.kind else {
            return Ok(false);
        };
        match kind {
            FieldKind::Object | FieldKind::Nested => Ok(false),
            FieldKind::Identifier => Ok(matcher(self.id)),
            FieldKind::Keyword => Ok(field_values
                .values
                .iter()
                .any(|stored_value| render_text(stored_value).is_some_and(|text| matcher(&text)))),
            FieldKind::Text => Err(refuse_text_leaf(leaf, field)),
            FieldKind::Date | FieldKind::Integer | FieldKind::Float | FieldKind::Boolean => {
                Err(OpenSearchError::illegal_argument(format!(
                    "[{leaf}] applies to string fields and [{field}] is not one"
                )))
            }
        }
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
fn render_text(value: &Value) -> Option<Cow<'_, str>> {
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

/// A `term` on a `text` field, answered only for stored values that are a single digit-group
/// token (see the module documentation).
fn matches_text_term(field: &str, value: &Value, stored_values: &[&Value]) -> Result<bool, OpenSearchError> {
    let expected_text = render_text(value).ok_or_else(|| refuse_value(field, value))?;
    // The loop never stops at a match: a later value that is no token still refuses the leaf, so
    // the answer does not depend on the order of a stored array.
    let mut has_expected_token = false;
    for stored_value in stored_values {
        let Some(text) = render_text(stored_value) else {
            continue;
        };
        if !is_single_digit_group_token(&text) {
            return Err(refuse_text_leaf("term", field));
        }
        has_expected_token |= text == expected_text;
    }
    Ok(has_expected_token)
}

/// ASCII digit groups joined by single dots, at most [`MAX_TOKEN_LENGTH`] characters.
fn is_single_digit_group_token(text: &str) -> bool {
    text.len() <= MAX_TOKEN_LENGTH
        && text
            .split('.')
            .all(|group| !group.is_empty() && group.bytes().all(|byte| byte.is_ascii_digit()))
}

/// The `illegal_argument_exception` for `leaf` on the `text` field `field`.
fn refuse_text_leaf(leaf: &str, field: &str) -> OpenSearchError {
    OpenSearchError::illegal_argument(format!(
        "[{leaf}] on the text field [{field}] is not supported on system indices"
    ))
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
fn read_numeric(value: &Value) -> Option<f64> {
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
fn read_boolean(value: &Value) -> Option<bool> {
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
fn read_date_micros(value: &Value) -> Option<i64> {
    let millis = match value {
        Value::String(text) => parse_stored_date_millis(text)?,
        Value::Number(number) => number.as_i64()?,
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => return None,
    };
    millis.checked_mul(1000)
}

/// Whether `text` lies within the range's bounds, compared as strings.
fn is_within_text_range(text: &str, range: &RangeQuery) -> Result<bool, OpenSearchError> {
    let compare_with_bound = |bound: &RangeBound| {
        render_text(&bound.value)
            .map(|bound_text| text.cmp(&bound_text))
            .ok_or_else(|| refuse_value(&range.field, &bound.value))
    };
    if let Some(lower) = &range.lower {
        let ordering = compare_with_bound(lower)?;
        if ordering == Ordering::Less || (ordering == Ordering::Equal && !lower.is_inclusive) {
            return Ok(false);
        }
    }
    if let Some(upper) = &range.upper {
        let ordering = compare_with_bound(upper)?;
        if ordering == Ordering::Greater || (ordering == Ordering::Equal && !upper.is_inclusive) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Numeric range bounds as `(value, is_inclusive)`.
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
        matches_document(
            &parse_query(query).expect("the query parses"),
            "doc-1",
            source,
            mappings,
            parse_request_instant(),
        )
    }

    fn assert_matches(cases: &[(Value, bool)], source: &Value, mappings: &Value) {
        for (query, expected) in cases {
            let is_match = evaluate(query, source, mappings).expect("the query evaluates");
            assert_eq!(is_match, *expected, "{query} on {source}");
        }
    }

    async fn assert_refused(query: &Value, source: &Value, mappings: &Value) {
        let error = evaluate(query, source, mappings).expect_err("the query is refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception", "{query}");
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
        assert_refused(&json!({"term": {"count": "zero"}}), &source, &mappings).await;
        // Both parse as floats, and neither is a number a stored value can equal.
        assert_refused(&json!({"term": {"count": "NaN"}}), &source, &mappings).await;
        assert_refused(&json!({"term": {"count": "inf"}}), &source, &mappings).await;
        assert_refused(&json!({"term": {"enabled": "yes"}}), &source, &mappings).await;
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
        for (path, expected) in cases {
            assert_eq!(
                find_mapped_field(&mappings, path).expect("the path resolves"),
                expected,
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
        for (type_name, expected) in cases {
            assert_eq!(
                FieldKind::from_mapping_type(type_name, "a").expect("a known type"),
                expected,
                "{type_name}"
            );
        }

        let (status, body) =
            render_envelope(FieldKind::from_mapping_type("geo_point", "location").expect_err("refused")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        let reason = body["error"]["reason"].as_str().expect("reason");
        assert!(reason.contains("location") && reason.contains("geo_point"), "{reason}");
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
        for (path, expected) in cases {
            let mut values = Vec::new();
            collect_source_values(&source, path, &mut values);
            let collected: Vec<Value> = values.into_iter().cloned().collect();
            assert_eq!(collected, expected, "{path}");
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
        for (source, expected) in cases {
            assert_eq!(
                evaluate(&query, &source, &mappings).expect("evaluates"),
                expected,
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
        for (source, expected) in cases {
            assert_eq!(
                evaluate(&query, &source, &mappings).expect("evaluates"),
                expected,
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

    #[tokio::test]
    async fn a_text_leaf_is_refused_unless_the_value_is_one_digit_group_token() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let words = json!({"title": "Logs overview"});
        let version = json!({"title": "7.9.3"});

        for refused_query in [
            json!({"term": {"title": "logs"}}),
            json!({"prefix": {"title": "lo"}}),
            json!({"wildcard": {"title": "lo*"}}),
            json!({"range": {"title": {"gte": "a"}}}),
        ] {
            assert_refused(&refused_query, &words, &mappings).await;
        }
        assert_matches(
            &[
                (json!({"term": {"title": "7.9.3"}}), true),
                (json!({"term": {"title": "7.9.0"}}), false),
            ],
            &version,
            &mappings,
        );
        assert_matches(&[(json!({"exists": {"field": "title"}}), true)], &words, &mappings);
        assert_matches(
            &[(json!({"exists": {"field": "title"}}), true)],
            &json!({"title": ""}),
            &mappings,
        );
        // A stored object has no text to compare; a term reads past it to the next value.
        assert_matches(
            &[(json!({"term": {"title": "1"}}), true)],
            &json!({"title": [{"nested": true}, "1"]}),
            &mappings,
        );
        // A value that is no token refuses the leaf from either side of a matching one.
        for stored_values in [json!(["7.9.3", "abc"]), json!(["abc", "7.9.3"])] {
            assert_refused(
                &json!({"term": {"title": "7.9.3"}}),
                &json!({"title": stored_values}),
                &mappings,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn a_token_longer_than_the_limit_is_refused_and_one_at_the_limit_is_answered() {
        let mappings = json!({"properties": {"title": {"type": "text"}}});
        let token_below_limit = "7".repeat(254);
        let token_at_limit = "7".repeat(255);
        let token_above_limit = "7".repeat(256);

        for answered_token in [token_below_limit, token_at_limit] {
            assert_matches(
                &[(json!({"term": {"title": answered_token}}), true)],
                &json!({"title": answered_token}),
                &mappings,
            );
        }
        assert_refused(
            &json!({"term": {"title": token_above_limit}}),
            &json!({"title": token_above_limit}),
            &mappings,
        )
        .await;
        for malformed_group in ["7..3", ".7", "7.", ""] {
            assert_refused(
                &json!({"term": {"title": "7"}}),
                &json!({"title": malformed_group}),
                &mappings,
            )
            .await;
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
        for (pattern, text, expected_match) in cases {
            assert_eq!(
                matches_wildcard(pattern, text, false),
                expected_match,
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
        for (pattern, text, expected_match) in folded_cases {
            assert_eq!(
                matches_wildcard(pattern, text, true),
                expected_match,
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
            let expected_match = matches_elements_naively(&elements, &text_characters);

            assert_eq!(
                matches_wildcard(&pattern, &text, false),
                expected_match,
                "{pattern:?} against {text:?}"
            );
            matched_count += usize::from(expected_match);
        }
        assert!(matched_count >= WILDCARD_CASE_COUNT / 4, "{matched_count} matches");
        assert!(
            WILDCARD_CASE_COUNT - matched_count >= WILDCARD_CASE_COUNT / 4,
            "{matched_count} matches"
        );
    }
}
