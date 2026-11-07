//! Lucene classic query string to the full-text query tree.
//!
//! The grammar crate parses the string; this visitor applies the clause rules of the
//! classic syntax that need no field types: grouping adjacent bare terms into one
//! multi-term clause, recording the `+` / `-` / `!` modifiers and the `AND` / `OR`
//! conjunctions each clause's occurrence is derived from, the `>` / `<` range
//! prefixes, `_exists_`, a lone `*`, and the refusal of fuzzy and proximity syntax.
//! Which clauses survive analysis, and so which occurrence each one gets, is decided
//! per searched index by the full-text resolver, because only the field types tell.

use std::{collections::VecDeque, rc::Rc};

use antlr4rust::tree::{ParseTree, TerminalNode};
use icegate_query_grammar::lucene::{
    BareOperatorTermContextAttrs, ClauseContextAttrs, ConjoinedClauseContextAttrs, ConjunctionContextAttrs,
    FieldNameContextAll, FieldNameContextAttrs, GroupContextAttrs, LuceneParserContextType, ModClauseContextAll,
    ModClauseContextAttrs, ModifierContextAttrs, PlainTermContextAttrs, PrefixTermContextAttrs, QueryContextAll,
    QueryContextAttrs, QuotedTermContextAttrs, RangeBoundContextAll, RangeBoundContextAttrs, RangeContextAttrs,
    RangeEndContextAttrs, RangeStartContextAttrs, RangeTermContextAttrs, RegexpTermContextAttrs, StarTermContextAttrs,
    TermContextAll, TermModifiersContextAll, TermModifiersContextAttrs, TopLevelQueryContextAttrs,
    WildcardTermContextAttrs, measure_group_depth, parse_lucene_query,
};

use super::MAX_QUERY_STRING_DEPTH;
use crate::{
    opensearch::error::OpenSearchError,
    query_dsl::{
        ast::{ALL_FIELDS, QueryStringQuery},
        full_text::{
            ClassicEntry, ClassicQuery, Conjunction, FieldSelector, FullTextBound, FullTextLeaf, FullTextQuery,
            FullTextTerm, LeafSettings, Modifier, build_field_selectors, selects_all_fields,
        },
    },
};

/// The field name the classic syntax reads as "this field exists".
const EXISTS_FIELD: &str = "_exists_";

/// Why a query string has no translation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum QueryStringError {
    /// The request is answered with this error before any index is read: a
    /// `parsing_exception` for fuzzy, proximity and regular expression syntax,
    /// conflicting options and groups nested past the limit; a `500` when the parser
    /// thread of a deeply nested string fails.
    Refused(OpenSearchError),
    /// A syntax error or a refused leading wildcard, in IceGate's wording: answered as
    /// the query-shard failure of each searched index.
    Invalid(String),
}

/// Translates a `query_string` request into the full-text query tree.
///
/// A blank query string (empty, or Lucene whitespace only) translates to an empty
/// clause list, which matches nothing, without being parsed.
///
/// # Errors
///
/// [`QueryStringError::Refused`] with a `parsing_exception` for fuzzy (`construct`
/// `fuzzy`), proximity (`slop`) and regular expression (`regexp`) syntax, for both
/// `fields` and `default_field`, and for groups nested deeper than
/// [`MAX_QUERY_STRING_DEPTH`] (`limit`), and with a `500` when the thread that parses a
/// deeply nested string cannot start or panics; [`QueryStringError::Invalid`] for a
/// syntax error and a refused leading wildcard.
pub(crate) fn translate_query_string(query: &QueryStringQuery) -> Result<FullTextQuery, QueryStringError> {
    if !query.fields.is_empty() && query.default_field.is_some() {
        return Err(QueryStringError::Refused(OpenSearchError::parsing(
            "[query_string] cannot set both [fields] and [default_field]",
        )));
    }
    if query.query.chars().all(is_lucene_whitespace) {
        return Ok(FullTextQuery::Bool(Vec::new()));
    }
    // Measured on the parser's own tokens, so every group the parser recurses into counts.
    let depth = measure_group_depth(&query.query);
    if depth > MAX_QUERY_STRING_DEPTH {
        return Err(QueryStringError::Refused(OpenSearchError::query_limit_exceeded(
            format!("[query_string] nests deeper than the limit of {MAX_QUERY_STRING_DEPTH} levels"),
            MAX_QUERY_STRING_DEPTH,
        )));
    }
    if depth <= MAX_IN_PLACE_DEPTH {
        return translate_parsed_query(query);
    }
    // See `DEEP_PARSE_STACK_BYTES` for why a deep string gets its own thread.
    std::thread::scope(|scope| {
        let worker = std::thread::Builder::new()
            .name(String::from("query-string-parse"))
            .stack_size(DEEP_PARSE_STACK_BYTES)
            .spawn_scoped(scope, || translate_parsed_query(query))
            .map_err(|error| {
                QueryStringError::Refused(OpenSearchError::internal(format!(
                    "the query string parser thread did not start: {error}"
                )))
            })?;
        worker.join().map_err(|_| {
            QueryStringError::Refused(OpenSearchError::internal("the query string parser thread panicked"))
        })?
    })
}

/// Deepest group nesting parsed and translated on the caller's thread; a query string of
/// this depth or less costs no thread.
const MAX_IN_PLACE_DEPTH: usize = 16;

/// Stack of the thread that parses a string nested deeper than [`MAX_IN_PLACE_DEPTH`].
///
/// The thread exists because the generated parser recurses through several rule frames per
/// group, and a caller's thread (a test thread, an async worker) has a stack this crate does
/// not size: [`MAX_QUERY_STRING_DEPTH`] levels overflow such a default stack in an
/// unoptimized build. The size leaves a wide margin over what the limit takes there; the
/// nesting test of `lucene` parses the limit through it.
const DEEP_PARSE_STACK_BYTES: usize = 16 * 1024 * 1024;

/// Parses a non-blank query string and translates its parse tree.
fn translate_parsed_query(query: &QueryStringQuery) -> Result<FullTextQuery, QueryStringError> {
    let tree = parse_lucene_query(&query.query)
        .map_err(|error| QueryStringError::Invalid(format!("failed to parse query [{}]: {error}", query.query)))?;
    let body = tree.query().ok_or_else(build_incomplete_tree_error)?;
    QueryStringTranslation::new(query).translate_query(&body, &ClauseTarget::Default)
}

/// The query-wide settings every clause of one query string shares.
struct QueryStringTranslation {
    default_fields: Vec<FieldSelector>,
    settings: LeafSettings,
    is_prefix_analyzed: bool,
    is_leading_wildcard_allowed: bool,
}

/// The fields a clause applies to: inherited from the enclosing group, or the defaults.
#[derive(Debug, Clone)]
enum ClauseTarget {
    /// The query's default fields.
    Default,
    /// The field or pattern a `field:` prefix names.
    Field(FieldSelector),
    /// `_exists_:`: the clause names a field that must exist.
    Exists,
}

/// One clause of a clause list, before adjacent bare terms are grouped.
struct ParsedClause {
    conjunction: Conjunction,
    modifier: Modifier,
    /// The raw image of an unqualified, unmodified plain term: a multi-term candidate.
    bare_term: Option<String>,
    /// Whether this clause starts with `AND`, `OR` or `*`, which ends a multi-term run before it.
    is_multi_term_boundary: bool,
    query: FullTextQuery,
}

impl QueryStringTranslation {
    fn new(query: &QueryStringQuery) -> Self {
        let (default_fields, is_all_fields) = query.default_field.as_ref().map_or_else(
            || (build_field_selectors(&query.fields), selects_all_fields(&query.fields)),
            |field| (vec![FieldSelector::from_name_or_pattern(field)], field == ALL_FIELDS),
        );
        Self {
            default_fields,
            settings: LeafSettings {
                operator: query.default_operator,
                is_lenient: query.lenient.unwrap_or(is_all_fields),
                time_zone: query.time_zone.clone(),
            },
            is_prefix_analyzed: query.analyze_wildcard,
            is_leading_wildcard_allowed: query.allow_leading_wildcard,
        }
    }

    fn translate_query(
        &self,
        query: &QueryContextAll<'_>,
        target: &ClauseTarget,
    ) -> Result<FullTextQuery, QueryStringError> {
        let first_clause = query.modClause().ok_or_else(build_incomplete_tree_error)?;
        let mut clauses = vec![self.read_clause(Conjunction::Implicit, &first_clause, target)?];
        for conjoined in query.conjoinedClause_all() {
            let conjunction = match conjoined.conjunction() {
                None => Conjunction::Implicit,
                Some(conjunction) if conjunction.AND().is_some() => Conjunction::And,
                Some(_) => Conjunction::Or,
            };
            let mod_clause = conjoined.modClause().ok_or_else(build_incomplete_tree_error)?;
            clauses.push(self.read_clause(conjunction, &mod_clause, target)?);
        }
        Ok(FullTextQuery::Classic(ClassicQuery {
            operator: self.settings.operator,
            entries: self.group_multi_terms(clauses, target)?,
        }))
    }

    fn read_clause(
        &self,
        conjunction: Conjunction,
        mod_clause: &ModClauseContextAll<'_>,
        target: &ClauseTarget,
    ) -> Result<ParsedClause, QueryStringError> {
        let modifier = match mod_clause.modifier() {
            None => Modifier::Absent,
            Some(modifier) if modifier.PLUS().is_some() => Modifier::Required,
            Some(_) => Modifier::Prohibited,
        };
        let clause = mod_clause.clause().ok_or_else(build_incomplete_tree_error)?;
        let field_name = clause.fieldName();
        let clause_target = match &field_name {
            None => target.clone(),
            Some(field_name) => read_target(field_name)?,
        };
        let term = clause.term();
        let is_star_field = field_name.as_ref().is_some_and(|name| name.STAR().is_some());
        let is_star_term = matches!(term.as_deref(), Some(TermContextAll::StarTermContext(_)));
        let bare_term = match (modifier, &field_name, term.as_deref()) {
            (Modifier::Absent, None, Some(TermContextAll::PlainTermContext(plain)))
                if plain.termModifiers().as_deref().is_none_or(has_no_term_modifiers) =>
            {
                plain.TERM().map(|token| token.get_text())
            }
            _ => None,
        };
        let query = match (term, clause.group()) {
            (Some(term), _) => self.translate_term(&term, &clause_target)?,
            (None, Some(group)) => {
                let inner_query = group.query().ok_or_else(build_incomplete_tree_error)?;
                self.translate_query(&inner_query, &clause_target)?
            }
            (None, None) => return Err(build_incomplete_tree_error()),
        };
        Ok(ParsedClause {
            conjunction,
            modifier,
            bare_term,
            is_multi_term_boundary: conjunction != Conjunction::Implicit
                || (modifier == Modifier::Absent && (is_star_field || is_star_term)),
            query,
        })
    }

    /// Groups adjacent bare terms into one multi-term entry: a run starts at an
    /// unmodified, unqualified plain term with no conjunction, and takes each following
    /// such term whose next clause does not start with `AND`, `OR` or `*`. The run's raw
    /// images are joined with one space and then unescaped.
    fn group_multi_terms(
        &self,
        clauses: Vec<ParsedClause>,
        target: &ClauseTarget,
    ) -> Result<Vec<ClassicEntry>, QueryStringError> {
        let mut entries = Vec::with_capacity(clauses.len());
        let mut queued_clauses: VecDeque<ParsedClause> = clauses.into();
        while let Some(clause) = queued_clauses.pop_front() {
            let joined_count = count_joined_terms(&clause, &queued_clauses);
            let Some(mut text) = clause.bare_term.filter(|_| joined_count > 0) else {
                entries.push(ClassicEntry::Clause {
                    conjunction: clause.conjunction,
                    modifier: clause.modifier,
                    query: clause.query,
                });
                continue;
            };
            for following_clause in queued_clauses.drain(..joined_count) {
                let following_term = following_clause.bare_term.ok_or_else(build_incomplete_tree_error)?;
                text.push(' ');
                text.push_str(&following_term);
            }
            entries.push(ClassicEntry::MultiTerm(
                self.build_unquoted_text_leaf(unescape_term(&text)?, target),
            ));
        }
        Ok(entries)
    }

    fn translate_term(
        &self,
        term: &TermContextAll<'_>,
        target: &ClauseTarget,
    ) -> Result<FullTextQuery, QueryStringError> {
        match term {
            TermContextAll::PlainTermContext(plain) => {
                let image = read_token_text(plain.TERM())?;
                refuse_fuzzy(plain.termModifiers().as_deref(), &image)?;
                Ok(FullTextQuery::Leaf(
                    self.build_unquoted_text_leaf(unescape_term(&image)?, target),
                ))
            }
            TermContextAll::BareOperatorTermContext(bare_operator) => {
                let image = read_token_text(bare_operator.BAREOPER())?;
                refuse_fuzzy(bare_operator.termModifiers().as_deref(), &image)?;
                let operator: String = image.chars().take(1).collect();
                Ok(FullTextQuery::Leaf(self.build_unquoted_text_leaf(operator, target)))
            }
            TermContextAll::StarTermContext(star) => {
                refuse_fuzzy(star.termModifiers().as_deref(), "*")?;
                Ok(self.translate_star(target))
            }
            TermContextAll::PrefixTermContext(prefix) => {
                let image = read_token_text(prefix.PREFIXTERM())?;
                refuse_fuzzy(prefix.termModifiers().as_deref(), &image)?;
                if matches!(target, ClauseTarget::Exists) {
                    // An unescaped trailing `*` is part of the literal field name.
                    return Ok(self.build_exists_query(vec![FieldSelector::Name(unescape_term(&image)?)]));
                }
                let stem = image.strip_suffix('*').unwrap_or(&image);
                Ok(self.build_leaf_query(
                    target,
                    FullTextTerm::Prefix {
                        prefix: unescape_term(stem)?,
                        is_analyzed: self.is_prefix_analyzed,
                    },
                ))
            }
            TermContextAll::WildcardTermContext(wildcard) => {
                let image = read_token_text(wildcard.WILDTERM())?;
                refuse_fuzzy(wildcard.termModifiers().as_deref(), &image)?;
                if matches!(target, ClauseTarget::Exists) {
                    return Ok(self.build_exists_query(vec![FieldSelector::Pattern(unescape_term(&image)?)]));
                }
                if !self.is_leading_wildcard_allowed && image.starts_with(['*', '?']) {
                    return Err(QueryStringError::Invalid(format!(
                        "'*' or '?' not allowed as first character in wildcard term [{image}]"
                    )));
                }
                // A wildcard pattern keeps its escapes: `\*` is a literal star there.
                Ok(self.build_leaf_query(target, FullTextTerm::Wildcard(image)))
            }
            TermContextAll::RegexpTermContext(regexp) => {
                let image = read_token_text(regexp.REGEXPTERM())?;
                Err(QueryStringError::Refused(OpenSearchError::unsupported_construct(
                    "regexp",
                    format!("regular expression [{image}] in a query string is not supported"),
                )))
            }
            TermContextAll::QuotedTermContext(quoted) => {
                let image = read_token_text(quoted.QUOTED())?;
                let text = unescape_term(strip_quotes(&image))?;
                refuse_proximity(quoted.termModifiers().as_deref(), &image)?;
                if matches!(target, ClauseTarget::Exists) {
                    return Ok(self.build_exists_query(vec![FieldSelector::from_name_or_pattern(&text)]));
                }
                Ok(self.build_leaf_query(target, FullTextTerm::Phrase(text)))
            }
            TermContextAll::RangeTermContext(range_term) => {
                let range = range_term.range().ok_or_else(build_incomplete_tree_error)?;
                let is_lower_inclusive = range.rangeStart().is_some_and(|start| start.RANGEIN_START().is_some());
                let is_upper_inclusive = range.rangeEnd().is_some_and(|end| end.RANGEIN_END().is_some());
                let lower = range.rangeBound(0).ok_or_else(build_incomplete_tree_error)?;
                let upper = range.rangeBound(1).ok_or_else(build_incomplete_tree_error)?;
                Ok(self.build_leaf_query(
                    target,
                    FullTextTerm::Range {
                        lower: read_bound(&lower, is_lower_inclusive)?,
                        upper: read_bound(&upper, is_upper_inclusive)?,
                    },
                ))
            }
            TermContextAll::Error(_) => Err(build_incomplete_tree_error()),
        }
    }

    /// A lone `*`: every row under the all-fields default or an explicit `*` field,
    /// otherwise "the field exists"; `_exists_:*` names no field and matches nothing.
    fn translate_star(&self, target: &ClauseTarget) -> FullTextQuery {
        match target {
            ClauseTarget::Exists => self.build_exists_query(Vec::new()),
            ClauseTarget::Field(FieldSelector::Pattern(pattern)) if pattern == ALL_FIELDS => FullTextQuery::MatchAll,
            ClauseTarget::Field(selector) => self.build_exists_query(vec![selector.clone()]),
            ClauseTarget::Default if self.has_all_fields_default() => FullTextQuery::MatchAll,
            ClauseTarget::Default => self.build_exists_query(self.default_fields.clone()),
        }
    }

    fn has_all_fields_default(&self) -> bool {
        self.default_fields
            .iter()
            .any(|selector| matches!(selector, FieldSelector::Pattern(pattern) if pattern == ALL_FIELDS))
    }

    /// Unquoted text after escapes are removed: `_exists_`, a `>` / `>=` / `<` /
    /// `<=` range prefix (only on text longer than its first character), or terms.
    fn build_unquoted_text_leaf(&self, text: String, target: &ClauseTarget) -> FullTextLeaf {
        if matches!(target, ClauseTarget::Exists) {
            return self.build_exists_leaf(vec![FieldSelector::from_name_or_pattern(&text)]);
        }
        let term = read_range_prefix(&text).unwrap_or(FullTextTerm::Text(text));
        self.build_leaf(target, term)
    }

    fn build_exists_leaf(&self, fields: Vec<FieldSelector>) -> FullTextLeaf {
        self.settings.build_leaf(fields, FullTextTerm::Exists)
    }

    fn build_exists_query(&self, fields: Vec<FieldSelector>) -> FullTextQuery {
        FullTextQuery::Leaf(self.build_exists_leaf(fields))
    }

    fn build_leaf_query(&self, target: &ClauseTarget, term: FullTextTerm) -> FullTextQuery {
        FullTextQuery::Leaf(self.build_leaf(target, term))
    }

    fn build_leaf(&self, target: &ClauseTarget, term: FullTextTerm) -> FullTextLeaf {
        let fields = match target {
            ClauseTarget::Default => self.default_fields.clone(),
            ClauseTarget::Field(selector) => vec![selector.clone()],
            ClauseTarget::Exists => vec![FieldSelector::Name(String::from(EXISTS_FIELD))],
        };
        self.settings.build_leaf(fields, term)
    }
}

/// The target a `field:` prefix names: every field for `*:`, "exists" for `_exists_:`, else
/// the field or, holding `*` or `?` after unescaping, the pattern.
fn read_target(field_name: &FieldNameContextAll<'_>) -> Result<ClauseTarget, QueryStringError> {
    if field_name.STAR().is_some() {
        return Ok(ClauseTarget::Field(FieldSelector::Pattern(ALL_FIELDS.to_owned())));
    }
    let field = unescape_term(&read_token_text(field_name.TERM())?)?;
    if field == EXISTS_FIELD {
        return Ok(ClauseTarget::Exists);
    }
    Ok(ClauseTarget::Field(FieldSelector::from_name_or_pattern(&field)))
}

/// How many of `following_clauses` join `first_clause` in one multi-term entry.
fn count_joined_terms(first_clause: &ParsedClause, following_clauses: &VecDeque<ParsedClause>) -> usize {
    if first_clause.bare_term.is_none() || first_clause.conjunction != Conjunction::Implicit {
        return 0;
    }
    following_clauses
        .iter()
        .enumerate()
        .take_while(|(position, clause)| {
            clause.bare_term.is_some()
                && clause.conjunction == Conjunction::Implicit
                && following_clauses
                    .get(position + 1)
                    .is_none_or(|next_clause| !next_clause.is_multi_term_boundary)
        })
        .count()
}

/// The one-sided range an unquoted text longer than one character starting with `>`,
/// `>=`, `<` or `<=` stands for; `>=` alone is the range above `=`, exclusive.
fn read_range_prefix(text: &str) -> Option<FullTextTerm> {
    let mut characters = text.chars();
    let first_character = characters.next()?;
    let rest = characters.as_str();
    if rest.is_empty() || !matches!(first_character, '>' | '<') {
        return None;
    }
    let (value, is_inclusive) = match rest.strip_prefix('=') {
        Some(value) if !value.is_empty() => (value, true),
        _ => (rest, false),
    };
    let bound = Some(FullTextBound {
        text: value.to_owned(),
        is_inclusive,
        is_quoted: false,
    });
    Some(if first_character == '>' {
        FullTextTerm::Range {
            lower: bound,
            upper: None,
        }
    } else {
        FullTextTerm::Range {
            lower: None,
            upper: bound,
        }
    })
}

/// One bound of a `[a TO b]` range; `None` for an open `*` bound.
fn read_bound(bound: &RangeBoundContextAll<'_>, is_inclusive: bool) -> Result<Option<FullTextBound>, QueryStringError> {
    if let Some(quoted) = bound.RANGE_QUOTED() {
        return Ok(Some(FullTextBound {
            text: unescape_term(strip_quotes(&quoted.get_text()))?,
            is_inclusive,
            is_quoted: true,
        }));
    }
    let image = bound
        .RANGE_GOOP()
        .or_else(|| bound.RANGE_TO())
        .map(|token| token.get_text())
        .ok_or_else(build_incomplete_tree_error)?;
    if image == "*" {
        return Ok(None);
    }
    Ok(Some(FullTextBound {
        text: unescape_term(&image)?,
        is_inclusive,
        is_quoted: false,
    }))
}

fn has_no_term_modifiers(modifiers: &TermModifiersContextAll<'_>) -> bool {
    modifiers.boost().is_none() && modifiers.FUZZY_SLOP().is_none()
}

/// Any `~` after a term asks for fuzzy matching, which IceGate does not evaluate.
fn refuse_fuzzy(modifiers: Option<&TermModifiersContextAll<'_>>, image: &str) -> Result<(), QueryStringError> {
    let Some(slop) = modifiers.and_then(TermModifiersContextAttrs::FUZZY_SLOP) else {
        return Ok(());
    };
    Err(QueryStringError::Refused(OpenSearchError::unsupported_construct(
        "fuzzy",
        format!("fuzzy query [{image}{}] is not supported", slop.get_text()),
    )))
}

/// `"..."~N` with N above zero asks for a proximity match, which IceGate does not
/// evaluate; `~0`, and a slop that is not a number, are accepted as no slop.
fn refuse_proximity(modifiers: Option<&TermModifiersContextAll<'_>>, image: &str) -> Result<(), QueryStringError> {
    let Some(slop) = modifiers.and_then(TermModifiersContextAttrs::FUZZY_SLOP) else {
        return Ok(());
    };
    let slop_text = slop.get_text();
    let distance = slop_text.strip_prefix('~').and_then(|digits| digits.parse::<f64>().ok());
    match distance {
        Some(distance) if distance > 0.0 => Err(QueryStringError::Refused(OpenSearchError::unsupported_construct(
            "slop",
            format!("proximity query [{image}{slop_text}] is not supported"),
        ))),
        Some(_) | None => Ok(()),
    }
}

/// The text between the double quotes of a quoted term or range bound; an image without
/// both quotes is kept whole.
fn strip_quotes(image: &str) -> &str {
    image.strip_prefix('"').and_then(|rest| rest.strip_suffix('"')).unwrap_or(image)
}

/// Removes the classic escapes: `\x` is `x`, and `\uXXXX` is that UTF-16 unit
/// (two such escapes may form a surrogate pair).
fn unescape_term(text: &str) -> Result<String, QueryStringError> {
    let mut units: Vec<u16> = Vec::with_capacity(text.len());
    let mut characters = text.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            let mut buffer = [0_u16; 2];
            units.extend_from_slice(character.encode_utf16(&mut buffer));
            continue;
        }
        match characters.next() {
            None => {
                return Err(QueryStringError::Invalid(format!(
                    "term [{text}] can not end with escape character"
                )));
            }
            Some('u') => {
                let digits: String = characters.by_ref().take(4).collect();
                let unit = (digits.len() == 4)
                    .then(|| u16::from_str_radix(&digits, 16).ok())
                    .flatten()
                    .ok_or_else(|| {
                        QueryStringError::Invalid(format!("truncated unicode escape sequence in [{text}]"))
                    })?;
                units.push(unit);
            }
            Some(escaped) => {
                let mut buffer = [0_u16; 2];
                units.extend_from_slice(escaped.encode_utf16(&mut buffer));
            }
        }
    }
    String::from_utf16(&units)
        .map_err(|_| QueryStringError::Invalid(format!("unicode escapes in [{text}] do not form characters")))
}

fn read_token_text(token: Option<Rc<TerminalNode<'_, LuceneParserContextType>>>) -> Result<String, QueryStringError> {
    token.map(|token| token.get_text()).ok_or_else(build_incomplete_tree_error)
}

/// The whitespace the classic lexer skips between clauses.
const fn is_lucene_whitespace(character: char) -> bool {
    matches!(character, ' ' | '\t' | '\n' | '\r' | '\u{3000}')
}

/// A parse tree missing a node its rule requires: the parser returned it without
/// an error, which only a grammar and visitor out of step can cause.
fn build_incomplete_tree_error() -> QueryStringError {
    QueryStringError::Invalid(String::from("the query string parse tree is incomplete"))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::{QueryStringError, translate_query_string};
    use crate::{
        opensearch::error::render_envelope,
        query_dsl::{
            ast::{Operator, QueryStringQuery},
            date_math::TimeZoneSpec,
            full_text::{
                ClassicEntry, ClassicQuery, Conjunction, FieldSelector, FullTextBound, FullTextLeaf, FullTextQuery,
                FullTextTerm, Modifier,
            },
        },
    };

    fn build_options(text: &str) -> QueryStringQuery {
        QueryStringQuery {
            query: text.to_owned(),
            default_field: None,
            fields: Vec::new(),
            default_operator: Operator::Or,
            analyze_wildcard: false,
            allow_leading_wildcard: true,
            lenient: None,
            time_zone: None,
        }
    }

    fn build_all_fields_selectors() -> Vec<FieldSelector> {
        vec![FieldSelector::Pattern(String::from("*"))]
    }

    fn build_named_selectors(field: &str) -> Vec<FieldSelector> {
        vec![FieldSelector::Name(field.to_owned())]
    }

    const fn build_leaf(
        fields: Vec<FieldSelector>,
        term: FullTextTerm,
        operator: Operator,
        is_lenient: bool,
    ) -> FullTextLeaf {
        FullTextLeaf {
            fields,
            term,
            operator,
            is_lenient,
            time_zone: None,
        }
    }

    /// A leaf under the all-fields default of `build_options`: `OR`, lenient.
    fn build_star_leaf(term: FullTextTerm) -> FullTextLeaf {
        build_leaf(build_all_fields_selectors(), term, Operator::Or, true)
    }

    fn build_text_term(value: &str) -> FullTextTerm {
        FullTextTerm::Text(value.to_owned())
    }

    fn build_bound(value: &str, is_inclusive: bool) -> FullTextBound {
        FullTextBound {
            text: value.to_owned(),
            is_inclusive,
            is_quoted: false,
        }
    }

    const fn build_clause(conjunction: Conjunction, modifier: Modifier, leaf: FullTextLeaf) -> ClassicEntry {
        ClassicEntry::Clause {
            conjunction,
            modifier,
            query: FullTextQuery::Leaf(leaf),
        }
    }

    const fn build_classic(operator: Operator, entries: Vec<ClassicEntry>) -> FullTextQuery {
        FullTextQuery::Classic(ClassicQuery { operator, entries })
    }

    /// A clause list of one implicit, unmodified leaf clause under `OR`.
    fn build_single_clause(leaf: FullTextLeaf) -> FullTextQuery {
        build_classic(
            Operator::Or,
            vec![build_clause(Conjunction::Implicit, Modifier::Absent, leaf)],
        )
    }

    const fn build_range_term(lower: Option<FullTextBound>, upper: Option<FullTextBound>) -> FullTextTerm {
        FullTextTerm::Range { lower, upper }
    }

    fn translate_options(query: &QueryStringQuery) -> FullTextQuery {
        translate_query_string(query).expect("the query string translates")
    }

    /// Clause lists: conjunctions and modifiers as written, multi-term groups.
    fn list_clause_rows() -> Vec<(&'static str, FullTextQuery)> {
        use Conjunction::{And, Implicit, Or};
        use Modifier::{Absent, Prohibited, Required};
        vec![
            (
                "a OR b AND c",
                build_classic(
                    Operator::Or,
                    vec![
                        build_clause(Implicit, Absent, build_star_leaf(build_text_term("a"))),
                        build_clause(Or, Absent, build_star_leaf(build_text_term("b"))),
                        build_clause(And, Absent, build_star_leaf(build_text_term("c"))),
                    ],
                ),
            ),
            (
                "foo bar",
                build_classic(
                    Operator::Or,
                    vec![ClassicEntry::MultiTerm(build_star_leaf(build_text_term("foo bar")))],
                ),
            ),
            (
                "a b OR c",
                build_classic(
                    Operator::Or,
                    vec![
                        build_clause(Implicit, Absent, build_star_leaf(build_text_term("a"))),
                        build_clause(Implicit, Absent, build_star_leaf(build_text_term("b"))),
                        build_clause(Or, Absent, build_star_leaf(build_text_term("c"))),
                    ],
                ),
            ),
            (
                "a b c:d",
                build_classic(
                    Operator::Or,
                    vec![
                        ClassicEntry::MultiTerm(build_star_leaf(build_text_term("a b"))),
                        build_clause(
                            Implicit,
                            Absent,
                            build_leaf(build_named_selectors("c"), build_text_term("d"), Operator::Or, true),
                        ),
                    ],
                ),
            ),
            (
                "a AND b OR c",
                build_classic(
                    Operator::Or,
                    vec![
                        build_clause(Implicit, Absent, build_star_leaf(build_text_term("a"))),
                        build_clause(And, Absent, build_star_leaf(build_text_term("b"))),
                        build_clause(Or, Absent, build_star_leaf(build_text_term("c"))),
                    ],
                ),
            ),
            (
                "+a OR b",
                build_classic(
                    Operator::Or,
                    vec![
                        build_clause(Implicit, Required, build_star_leaf(build_text_term("a"))),
                        build_clause(Or, Absent, build_star_leaf(build_text_term("b"))),
                    ],
                ),
            ),
            (
                "-a OR b",
                build_classic(
                    Operator::Or,
                    vec![
                        build_clause(Implicit, Prohibited, build_star_leaf(build_text_term("a"))),
                        build_clause(Or, Absent, build_star_leaf(build_text_term("b"))),
                    ],
                ),
            ),
            (
                "foo!bar",
                build_classic(
                    Operator::Or,
                    vec![
                        build_clause(Implicit, Absent, build_star_leaf(build_text_term("foo"))),
                        build_clause(Implicit, Prohibited, build_star_leaf(build_text_term("bar"))),
                    ],
                ),
            ),
            (
                "+ a",
                build_classic(
                    Operator::Or,
                    vec![
                        build_clause(Implicit, Absent, build_star_leaf(build_text_term("+"))),
                        build_clause(Implicit, Absent, build_star_leaf(build_text_term("a"))),
                    ],
                ),
            ),
            (
                "field:(a b)",
                build_classic(
                    Operator::Or,
                    vec![ClassicEntry::Clause {
                        conjunction: Implicit,
                        modifier: Absent,
                        query: build_classic(
                            Operator::Or,
                            vec![ClassicEntry::MultiTerm(build_leaf(
                                build_named_selectors("field"),
                                build_text_term("a b"),
                                Operator::Or,
                                true,
                            ))],
                        ),
                    }],
                ),
            ),
        ]
    }

    /// Single terms: ranges and range prefixes, `_exists_`, field patterns, escapes.
    fn list_term_rows() -> Vec<(&'static str, FullTextQuery)> {
        let build_count_range = |lower, upper| {
            build_single_clause(build_leaf(
                build_named_selectors("count"),
                build_range_term(lower, upper),
                Operator::Or,
                true,
            ))
        };
        vec![
            (
                "{a TO *]",
                build_single_clause(build_star_leaf(build_range_term(Some(build_bound("a", false)), None))),
            ),
            (
                "[a TO b}",
                build_single_clause(build_star_leaf(build_range_term(
                    Some(build_bound("a", true)),
                    Some(build_bound("b", false)),
                ))),
            ),
            ("count:>10", build_count_range(Some(build_bound("10", false)), None)),
            ("count:>=10", build_count_range(Some(build_bound("10", true)), None)),
            ("count:<10", build_count_range(None, Some(build_bound("10", false)))),
            ("count:<=10", build_count_range(None, Some(build_bound("10", true)))),
            (
                ">10",
                build_single_clause(build_star_leaf(build_range_term(Some(build_bound("10", false)), None))),
            ),
            (
                "<=10",
                build_single_clause(build_star_leaf(build_range_term(None, Some(build_bound("10", true))))),
            ),
            (
                "service_name:>\"checkout\"",
                build_classic(
                    Operator::Or,
                    vec![
                        build_clause(
                            Conjunction::Implicit,
                            Modifier::Absent,
                            build_leaf(
                                build_named_selectors("service_name"),
                                build_text_term(">"),
                                Operator::Or,
                                true,
                            ),
                        ),
                        build_clause(
                            Conjunction::Implicit,
                            Modifier::Absent,
                            build_star_leaf(FullTextTerm::Phrase(String::from("checkout"))),
                        ),
                    ],
                ),
            ),
            (
                "> 10",
                build_classic(
                    Operator::Or,
                    vec![ClassicEntry::MultiTerm(build_star_leaf(build_range_term(
                        Some(build_bound(" 10", false)),
                        None,
                    )))],
                ),
            ),
            (
                ">=",
                build_single_clause(build_star_leaf(build_range_term(Some(build_bound("=", false)), None))),
            ),
            (
                "_exists_:f",
                build_single_clause(build_leaf(
                    build_named_selectors("f"),
                    FullTextTerm::Exists,
                    Operator::Or,
                    true,
                )),
            ),
            (
                "_exists_:col.\\*",
                build_single_clause(build_leaf(
                    vec![FieldSelector::Pattern(String::from("col.*"))],
                    FullTextTerm::Exists,
                    Operator::Or,
                    true,
                )),
            ),
            (
                "_exists_:col.*",
                build_single_clause(build_leaf(
                    build_named_selectors("col.*"),
                    FullTextTerm::Exists,
                    Operator::Or,
                    true,
                )),
            ),
            (
                "_exists_:*",
                build_single_clause(build_leaf(Vec::new(), FullTextTerm::Exists, Operator::Or, true)),
            ),
            (
                "resource_attributes.\\*:v",
                build_single_clause(build_leaf(
                    vec![FieldSelector::Pattern(String::from("resource_attributes.*"))],
                    build_text_term("v"),
                    Operator::Or,
                    true,
                )),
            ),
            (
                "user@example.com",
                build_single_clause(build_star_leaf(build_text_term("user@example.com"))),
            ),
            (
                "*",
                build_classic(
                    Operator::Or,
                    vec![ClassicEntry::Clause {
                        conjunction: Conjunction::Implicit,
                        modifier: Modifier::Absent,
                        query: FullTextQuery::MatchAll,
                    }],
                ),
            ),
            ("a^2", build_single_clause(build_star_leaf(build_text_term("a")))),
            (
                "caf\\u00e9",
                build_single_clause(build_star_leaf(build_text_term("café"))),
            ),
            (
                "\"a b\"~0",
                build_single_clause(build_star_leaf(FullTextTerm::Phrase(String::from("a b")))),
            ),
            (
                "foo*",
                build_single_clause(build_star_leaf(FullTextTerm::Prefix {
                    prefix: String::from("foo"),
                    is_analyzed: false,
                })),
            ),
            (
                "f?o",
                build_single_clause(build_star_leaf(FullTextTerm::Wildcard(String::from("f?o")))),
            ),
        ]
    }

    #[test]
    fn query_strings_translate_to_the_stated_ast() {
        for (query, expected) in list_clause_rows().into_iter().chain(list_term_rows()) {
            assert_eq!(
                translate_options(&build_options(query)),
                expected,
                "query string [{query}]"
            );
        }
    }

    #[test]
    fn foo_bar_under_and_is_one_multi_term_with_the_and_operator() {
        let mut query = build_options("foo bar");
        query.default_operator = Operator::And;
        assert_eq!(
            translate_options(&query),
            build_classic(
                Operator::And,
                vec![ClassicEntry::MultiTerm(build_leaf(
                    build_all_fields_selectors(),
                    build_text_term("foo bar"),
                    Operator::And,
                    true
                ))]
            )
        );
    }

    #[test]
    fn a_bare_star_under_a_named_default_field_is_exists() {
        let mut query = build_options("*");
        query.default_field = Some(String::from("body"));
        assert_eq!(
            translate_options(&query),
            build_single_clause(build_leaf(
                build_named_selectors("body"),
                FullTextTerm::Exists,
                Operator::Or,
                false
            ))
        );
    }

    #[test]
    fn range_prefixes_under_a_named_default_field_read_that_field_strictly() {
        let translate_on_count = |text: &str| {
            let mut query = build_options(text);
            query.default_field = Some(String::from("count"));
            translate_options(&query)
        };
        for (text, term) in [
            (">10", build_range_term(Some(build_bound("10", false)), None)),
            (">=10", build_range_term(Some(build_bound("10", true)), None)),
            ("<10", build_range_term(None, Some(build_bound("10", false)))),
            ("<=10", build_range_term(None, Some(build_bound("10", true)))),
        ] {
            assert_eq!(
                translate_on_count(text),
                build_single_clause(build_leaf(build_named_selectors("count"), term, Operator::Or, false)),
                "query string [{text}]"
            );
        }
    }

    #[test]
    fn analyze_wildcard_marks_prefix_terms_for_per_field_analysis() {
        let mut query = build_options("foo-ba*");
        query.analyze_wildcard = true;
        assert_eq!(
            translate_options(&query),
            build_single_clause(build_star_leaf(FullTextTerm::Prefix {
                prefix: String::from("foo-ba"),
                is_analyzed: true,
            }))
        );
    }

    /// Each refusal is a `parsing_exception` whose `construct` names what was refused; the
    /// accepted forms (`"a b"~0`) are rows of the translation table above.
    #[tokio::test]
    async fn fuzzy_and_proximity_constructs_are_refused_naming_them() {
        let refused = [
            ("roam~", "fuzzy"),
            ("roam~1", "fuzzy"),
            ("roam~0.8", "fuzzy"),
            ("ro~", "fuzzy"),
            ("\"a b\"~2", "slop"),
        ];
        for (query, construct) in refused {
            let Err(QueryStringError::Refused(error)) = translate_query_string(&build_options(query)) else {
                panic!("query string [{query}] is not refused");
            };
            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "query string [{query}]");
            assert_eq!(body["error"]["type"], "parsing_exception", "query string [{query}]");
            assert_eq!(body["error"]["construct"], construct, "query string [{query}]");
        }
    }

    #[test]
    fn blank_query_strings_match_nothing() {
        for query in ["", " ", "\t"] {
            assert_eq!(
                translate_options(&build_options(query)),
                FullTextQuery::Bool(Vec::new()),
                "query string [{query:?}]"
            );
        }
    }

    #[test]
    fn leniency_defaults_to_true_only_under_the_star_default_field() {
        let read_leniency = |query: QueryStringQuery| match translate_options(&query) {
            FullTextQuery::Classic(ClassicQuery { entries, .. }) => match entries.as_slice() {
                [
                    ClassicEntry::Clause {
                        query: FullTextQuery::Leaf(leaf),
                        ..
                    },
                ] => leaf.is_lenient,
                other => panic!("one leaf clause expected, got {other:?}"),
            },
            other => panic!("a clause list expected, got {other:?}"),
        };
        assert!(read_leniency(build_options("a")));
        let mut with_default_field = build_options("a");
        with_default_field.default_field = Some(String::from("body"));
        assert!(!read_leniency(with_default_field));
        let mut with_fields = build_options("a");
        with_fields.fields = vec![String::from("body"), String::from("service_name")];
        assert!(!read_leniency(with_fields));
        let mut with_star_fields = build_options("a");
        with_star_fields.fields = vec![String::from("*")];
        assert!(read_leniency(with_star_fields));
        let mut explicit = build_options("a");
        explicit.lenient = Some(false);
        assert!(!read_leniency(explicit));
    }

    #[test]
    fn a_leading_wildcard_is_invalid_when_allow_leading_wildcard_is_false() {
        assert_eq!(
            translate_options(&build_options("*foo")),
            build_single_clause(build_star_leaf(FullTextTerm::Wildcard(String::from("*foo"))))
        );
        let mut refusing = build_options("*foo");
        refusing.allow_leading_wildcard = false;
        assert!(
            matches!(translate_query_string(&refusing), Err(QueryStringError::Invalid(_))),
            "a refused leading wildcard is answered by each searched index"
        );
    }

    #[tokio::test]
    async fn both_fields_and_default_field_are_refused() {
        let mut query = build_options("a");
        query.fields = vec![String::from("body")];
        query.default_field = Some(String::from("body"));
        let Err(QueryStringError::Refused(error)) = translate_query_string(&query) else {
            panic!("both [fields] and [default_field] are not refused");
        };
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
    }

    /// The error's position is the grammar crate's `SyntaxError`, whose tests pin it.
    #[test]
    fn a_syntax_error_is_invalid() {
        assert!(matches!(
            translate_query_string(&build_options("a/b")),
            Err(QueryStringError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn a_regular_expression_term_is_refused_until_the_regex_dialect_exists() {
        let Err(QueryStringError::Refused(error)) = translate_query_string(&build_options("level:/ERR.*/")) else {
            panic!("a regular expression term is not refused");
        };
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
        assert_eq!(body["error"]["construct"], "regexp");
    }

    /// Every leaf of `query`, at any depth of clause lists and groups.
    fn collect_leaves<'tree>(query: &'tree FullTextQuery, leaves: &mut Vec<&'tree FullTextLeaf>) {
        match query {
            FullTextQuery::MatchAll => {}
            FullTextQuery::Leaf(leaf) => leaves.push(leaf),
            FullTextQuery::Bool(clauses) => {
                for clause in clauses {
                    collect_leaves(&clause.query, leaves);
                }
            }
            FullTextQuery::Classic(ClassicQuery { entries, .. }) => {
                for entry in entries {
                    match entry {
                        ClassicEntry::Clause { query, .. } => collect_leaves(query, leaves),
                        ClassicEntry::MultiTerm(leaf) => leaves.push(leaf),
                    }
                }
            }
        }
    }

    #[test]
    fn the_time_zone_is_carried_by_every_leaf() {
        let mut query = build_options("timestamp:2026 AND (a b) AND count:[1 TO 2] AND _exists_:f AND foo*");
        query.time_zone = Some(TimeZoneSpec::Named(chrono_tz::Asia::Dubai));
        let translated = translate_options(&query);
        let mut leaves = Vec::new();
        collect_leaves(&translated, &mut leaves);
        // One leaf of each kind: a field term, a multi-term inside a group, a range,
        // `_exists_` and a prefix.
        let terms: Vec<&FullTextTerm> = leaves.iter().map(|leaf| &leaf.term).collect();
        assert_eq!(
            terms,
            [
                &build_text_term("2026"),
                &build_text_term("a b"),
                &build_range_term(Some(build_bound("1", true)), Some(build_bound("2", true))),
                &FullTextTerm::Exists,
                &FullTextTerm::Prefix {
                    prefix: String::from("foo"),
                    is_analyzed: false,
                },
            ]
        );
        for leaf in leaves {
            assert_eq!(
                leaf.time_zone,
                Some(TimeZoneSpec::Named(chrono_tz::Asia::Dubai)),
                "{leaf:?}"
            );
        }
    }
}
