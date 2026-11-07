//! The `simple_query_string` syntax: whitespace-separated terms with `+`, `|`, `-`, `"…"`,
//! trailing `*`, `~N`, `( )` and `\` escapes. Its structure follows `SimpleQueryParser` of Lucene
//! `releases/lucene/10.5.1`.
//!
//! The parser never reports a syntax error: an unmatched quote or parenthesis is skipped, an
//! operator with nothing to apply to is dropped. It refuses three things that are not syntax:
//!
//! - A `~` after a term or a phrase, followed by nothing or by a positive integer, asks for a fuzzy
//!   term or a sloppy phrase, which IceGate does not evaluate. Any other suffix (`foo~0`, `foo~0.8`,
//!   `foo~-1`, `foo~abc`) matches exactly, and so does a bare `~` after a phrase that ends the text
//!   or its group.
//! - A query nested deeper than `MAX_SIMPLE_QUERY_DEPTH`, counted in parenthesized groups and in the
//!   `Bool` levels of the parsed tree.
//! - A text of more characters than a `u32` counts, the width of a closer's stored position.

use std::num::IntErrorKind;

use super::ast::Operator;
use crate::opensearch::error::OpenSearchError;

/// How deep parenthesized groups may nest, and how many `Bool` levels the parsed tree may hold.
/// The parser recurses once per group, and the tree is evaluated, cloned and dropped recursively,
/// so without a limit a crafted query text overflows a worker's stack and aborts the process.
const MAX_SIMPLE_QUERY_DEPTH: usize = 128;

/// The parsed form of a `simple_query_string` query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SimpleQuery {
    /// One term, analyzed per field when evaluated; `is_prefix` for a trailing `*`.
    Term {
        /// The term text with escapes removed and without the trailing `*`.
        text: String,
        /// Whether the term matches every value it prefixes.
        is_prefix: bool,
    },
    /// A quoted phrase.
    Phrase {
        /// The phrase text with escapes removed.
        text: String,
    },
    /// A boolean group: every `must`, at least one `should` when there is no `must`, no
    /// `must_not`.
    Bool {
        /// Clauses that must match.
        must: Vec<Self>,
        /// Clauses of which at least one matches when `must` is empty.
        should: Vec<Self>,
        /// Clauses that must not match.
        must_not: Vec<Self>,
    },
    /// Matches every document; the positive side of a negated clause.
    MatchAll,
    /// Matches no document: a query with no term.
    MatchNone,
}

/// `+` joins with "must".
const AND: u16 = 1 << 0;
/// `-` negates the next clause.
const NOT: u16 = 1 << 1;
/// `|` joins with "should".
const OR: u16 = 1 << 2;
/// A trailing `*` makes a prefix term.
const PREFIX: u16 = 1 << 3;
/// `"` opens a phrase.
const PHRASE: u16 = 1 << 4;
/// `( )` group.
const PRECEDENCE: u16 = 1 << 5;
/// `\` escapes the next character.
const ESCAPE: u16 = 1 << 6;
/// Whitespace separates terms.
const WHITESPACE: u16 = 1 << 7;
/// `~` after a term states an edit distance.
const FUZZY: u16 = 1 << 8;
/// `~` after a phrase states a slop.
const NEAR: u16 = 1 << 9;

/// The operators a `simple_query_string` query honours, selected by its `flags`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SimpleQueryFlags(u16);

impl SimpleQueryFlags {
    /// Every operator: the default when `flags` is absent.
    pub(crate) const ALL: Self =
        Self(AND | NOT | OR | PREFIX | PHRASE | PRECEDENCE | ESCAPE | WHITESPACE | FUZZY | NEAR);

    /// Reads a `|`-separated list of flag names (`ALL`, `NONE`, `AND`, `OR`, `NOT`, `PREFIX`,
    /// `PHRASE`, `PRECEDENCE`, `ESCAPE`, `WHITESPACE`, `FUZZY`, `NEAR`, `SLOP`), in any case.
    ///
    /// # Errors
    ///
    /// A `parsing_exception` naming an unknown flag; an empty name is one.
    pub(crate) fn parse(text: &str) -> Result<Self, OpenSearchError> {
        text.split('|').try_fold(Self(0), |flags, name| {
            let bits = match name.trim().to_ascii_uppercase().as_str() {
                "ALL" => Self::ALL.0,
                "NONE" => 0,
                "AND" => AND,
                "OR" => OR,
                "NOT" => NOT,
                "PREFIX" => PREFIX,
                "PHRASE" => PHRASE,
                "PRECEDENCE" => PRECEDENCE,
                "ESCAPE" => ESCAPE,
                "WHITESPACE" => WHITESPACE,
                "FUZZY" => FUZZY,
                "NEAR" | "SLOP" => NEAR,
                _ => {
                    return Err(OpenSearchError::parsing(format!(
                        "unknown simple_query_string flag [{name}]"
                    )));
                }
            };
            Ok(Self(flags.0 | bits))
        })
    }

    /// Whether `flag` is on.
    const fn contains(self, flag: u16) -> bool {
        self.0 & flag != 0
    }
}

/// How a clause joins the group it is added to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Occur {
    /// The clause must match.
    Must,
    /// The clause is one alternative.
    Should,
}

impl From<Operator> for Occur {
    fn from(operator: Operator) -> Self {
        match operator {
            Operator::Or => Self::Should,
            Operator::And => Self::Must,
        }
    }
}

/// Where a scan starting fresh at each position of a query text finds its closer: the length of the
/// text when it finds none, so "at or past the end of the range" is the one test for an unclosed
/// opener.
///
/// A `\` escapes relative to where a scan began, so the answer depends on the start: the parser
/// opens a group at the `(` of `x~\(`, which one walk from the left counts as escaped. The tables
/// therefore hold every position. Each entry is read from the entries after it, so filling a table
/// is one pass, and each opener costs one lookup however far its closer lies. Entries are `u32`
/// because a table costs its width per character of the text.
struct ClosingPositions {
    /// By position: the `)` closing a group whose content starts there. Empty without `PRECEDENCE`,
    /// because then the parser opens no group.
    parentheses: Vec<u32>,
    /// By position: the `"` closing a phrase whose content starts there. Empty without `PHRASE`,
    /// because then the parser opens no phrase.
    quotes: Vec<u32>,
}

impl ClosingPositions {
    /// The tables `flags` reads over `characters`.
    ///
    /// # Errors
    ///
    /// A `parsing_exception` for a text whose length an entry cannot state.
    fn from_characters(characters: &[char], flags: SimpleQueryFlags) -> Result<Self, OpenSearchError> {
        let character_count = convert_character_count(characters.len())?;
        let is_escaping = flags.contains(ESCAPE);
        let parentheses = flags
            .contains(PRECEDENCE)
            .then(|| build_closer_table(characters, character_count, ')', Some('('), is_escaping));
        let quotes = flags
            .contains(PHRASE)
            .then(|| build_closer_table(characters, character_count, '"', None, is_escaping));
        Ok(Self {
            parentheses: parentheses.unwrap_or_default(),
            quotes: quotes.unwrap_or_default(),
        })
    }

    /// The `)` closing a group whose content starts at `start`.
    fn find_closing_parenthesis(&self, start: usize) -> usize {
        self.parentheses[start] as usize
    }

    /// The `"` closing a phrase whose content starts at `start`.
    fn find_closing_quote(&self, start: usize) -> usize {
        self.quotes[start] as usize
    }
}

/// `character_count` as a [`ClosingPositions`] entry.
///
/// # Errors
///
/// A `parsing_exception` for a count an entry cannot state.
fn convert_character_count(character_count: usize) -> Result<u32, OpenSearchError> {
    u32::try_from(character_count).map_err(|_| {
        OpenSearchError::query_limit_exceeded(
            format!(
                "[simple_query_string] holds {character_count} characters, past the limit of {}",
                u32::MAX
            ),
            u32::MAX,
        )
    })
}

/// By position, where a scan starting there finds `closer`, `character_count` when it finds none.
/// `nesting_opener` opens a level the scan leaves at that level's own `closer`.
fn build_closer_table(
    characters: &[char],
    character_count: u32,
    closer: char,
    nesting_opener: Option<char>,
    is_escaping: bool,
) -> Vec<u32> {
    // Two entries past the text: a scan starting at its end, and the step over a `\` in its last
    // position or over a group closing with it.
    let mut table = vec![character_count; characters.len() + 2];
    for (&character, position) in characters.iter().zip(0..character_count).rev() {
        let index = position as usize;
        table[index] = if character == '\\' && is_escaping {
            table[index + 2]
        } else if character == closer {
            position
        } else if Some(character) == nesting_opener {
            // A nested group ends at its own closer, and the scan resumes after that.
            table[table[index + 1] as usize + 1]
        } else {
            table[index + 1]
        };
    }
    table
}

/// The parse position and the tree built so far over `characters[index..end]`.
struct ParseState<'a> {
    /// The whole query text.
    characters: &'a [char],
    /// The next character to read.
    index: usize,
    /// One past the last character of this range.
    end: usize,
    /// How many parenthesized groups enclose this range.
    nesting_depth: usize,
    /// The tree of the clauses read so far.
    top: Option<SimpleQuery>,
    /// How many `Bool` levels `top` holds.
    top_depth: usize,
    /// The operator an explicit `+` or `|` set for the next clause.
    current_operation: Option<Occur>,
    /// The operator that joined the last clause.
    previous_operation: Option<Occur>,
    /// How many `-` precede the next clause.
    negations: u32,
}

impl<'a> ParseState<'a> {
    /// A state over `characters[start..end]`, inside `nesting_depth` groups.
    const fn new(characters: &'a [char], start: usize, end: usize, nesting_depth: usize) -> Self {
        Self {
            characters,
            index: start,
            end,
            nesting_depth,
            top: None,
            top_depth: 0,
            current_operation: None,
            previous_operation: None,
            negations: 0,
        }
    }

    /// The character at the current position.
    const fn peek_character(&self) -> char {
        self.characters[self.index]
    }
}

/// The parser of one query: its flags, default operator and closers.
struct SimpleQueryParser {
    /// The operators the query honours.
    flags: SimpleQueryFlags,
    /// Joins clauses no explicit operator joins.
    default_operation: Occur,
    /// The closers of the query text.
    closing_positions: ClosingPositions,
}

impl SimpleQueryParser {
    /// Reads the range of `state` into its tree.
    fn parse_sub_query(&self, state: &mut ParseState<'_>) -> Result<(), OpenSearchError> {
        while state.index < state.end {
            let character = state.peek_character();
            if character == '(' && self.flags.contains(PRECEDENCE) {
                self.consume_sub_query(state)?;
            } else if character == ')' && self.flags.contains(PRECEDENCE) {
                state.index += 1;
            } else if character == '"' && self.flags.contains(PHRASE) {
                self.consume_phrase(state)?;
            } else if character == '+' && self.flags.contains(AND) {
                if state.current_operation.is_none() && state.top.is_some() {
                    state.current_operation = Some(Occur::Must);
                }
                state.index += 1;
            } else if character == '|' && self.flags.contains(OR) {
                if state.current_operation.is_none() && state.top.is_some() {
                    state.current_operation = Some(Occur::Should);
                }
                state.index += 1;
            } else if character == '-' && self.flags.contains(NOT) {
                // A negation applies to the clause that follows, so it survives this iteration.
                state.negations += 1;
                state.index += 1;
                continue;
            } else if is_whitespace(character) && self.flags.contains(WHITESPACE) {
                state.index += 1;
            } else {
                self.consume_token(state)?;
            }
            state.negations = 0;
        }
        Ok(())
    }

    /// Reads the group opening at the current `(`: its clauses join the tree as one branch.
    fn consume_sub_query(&self, state: &mut ParseState<'_>) -> Result<(), OpenSearchError> {
        state.index += 1;
        let start = state.index;
        let closer = self.closing_positions.find_closing_parenthesis(start);
        if closer >= state.end {
            // No closing parenthesis: the opening one is skipped and its content parsed in place.
            return Ok(());
        }
        state.index = closer + 1;
        if closer == start {
            state.current_operation = None;
            return Ok(());
        }
        if state.nesting_depth >= MAX_SIMPLE_QUERY_DEPTH {
            return Err(refuse_deep_query());
        }
        let mut sub_state = ParseState::new(state.characters, start, closer, state.nesting_depth + 1);
        self.parse_sub_query(&mut sub_state)?;
        if let Some(branch) = sub_state.top {
            self.add_branch(state, branch, sub_state.top_depth)?;
        }
        Ok(())
    }

    /// Reads the phrase opening at the current `"`, with its `~` suffix.
    fn consume_phrase(&self, state: &mut ParseState<'_>) -> Result<(), OpenSearchError> {
        state.index += 1;
        let start = state.index;
        let closer = self.closing_positions.find_closing_quote(start);
        if closer >= state.end {
            // No closing quote: the opening one is skipped and the rest parsed as terms.
            return Ok(());
        }
        state.index = closer;
        let has_slop = if closer + 1 < state.end && state.characters[closer + 1] == '~' && self.flags.contains(NEAR) {
            state.index += 1;
            state.index + 1 < state.end
        } else {
            false
        };
        if state.index == start {
            state.current_operation = None;
            state.index += 1;
            return Ok(());
        }
        let phrase = self.remove_escapes(&state.characters[start..closer]);
        if has_slop {
            let suffix = self.read_tilde_suffix(state);
            if is_inexact_suffix(&suffix) {
                return Err(OpenSearchError::unsupported_construct(
                    "slop",
                    format!("phrase slop [~{suffix}] on [\"{phrase}\"] is not supported"),
                ));
            }
        }
        self.add_branch(state, SimpleQuery::Phrase { text: phrase }, 0)?;
        state.index += 1;
        Ok(())
    }

    /// `characters` as text, without the `\` of each escape.
    fn remove_escapes(&self, characters: &[char]) -> String {
        let mut text = String::with_capacity(characters.len());
        let mut is_escaped = false;
        for &character in characters {
            if !is_escaped && character == '\\' && self.flags.contains(ESCAPE) {
                is_escaped = true;
            } else {
                is_escaped = false;
                text.push(character);
            }
        }
        text
    }

    /// Reads the term at the current position, with its trailing `*` or `~` suffix.
    fn consume_token(&self, state: &mut ParseState<'_>) -> Result<(), OpenSearchError> {
        let mut token = String::new();
        let mut is_escaped = false;
        let mut is_prefix = false;
        let mut is_fuzzy = false;
        while state.index < state.end {
            if !is_escaped {
                let character = state.peek_character();
                if character == '\\' && self.flags.contains(ESCAPE) {
                    is_escaped = true;
                    is_prefix = false;
                    state.index += 1;
                    continue;
                } else if self.is_token_finished(state) {
                    break;
                } else if !token.is_empty() && character == '~' && self.flags.contains(FUZZY) {
                    is_fuzzy = true;
                    break;
                }
                is_prefix = !token.is_empty() && character == '*' && self.flags.contains(PREFIX);
            }
            is_escaped = false;
            token.push(state.peek_character());
            state.index += 1;
        }
        if token.is_empty() {
            return Ok(());
        }
        if is_fuzzy {
            let suffix = self.read_tilde_suffix(state);
            if is_inexact_suffix(&suffix) {
                return Err(OpenSearchError::unsupported_construct(
                    "fuzzy",
                    format!("fuzzy query [{token}~{suffix}] is not supported"),
                ));
            }
        }
        // A `*` before the `~` of an accepted suffix is text, so the term stays exact.
        let is_prefix = is_prefix && !is_fuzzy;
        if is_prefix {
            token.pop();
        }
        self.add_branch(state, SimpleQuery::Term { text: token, is_prefix }, 0)
    }

    /// Whether the current character ends a term.
    const fn is_token_finished(&self, state: &ParseState<'_>) -> bool {
        match state.peek_character() {
            '"' => self.flags.contains(PHRASE),
            '|' => self.flags.contains(OR),
            '+' => self.flags.contains(AND),
            '(' | ')' => self.flags.contains(PRECEDENCE),
            character if is_whitespace(character) => self.flags.contains(WHITESPACE),
            _ => false,
        }
    }

    /// Reads the characters after a `~` at the current position, up to the end of the token.
    fn read_tilde_suffix(&self, state: &mut ParseState<'_>) -> String {
        let mut suffix = String::new();
        while state.index < state.end {
            state.index += 1;
            if state.index < state.end {
                if self.is_token_finished(state) {
                    break;
                }
                suffix.push(state.peek_character());
            }
        }
        suffix
    }

    /// Adds `branch`, which holds `branch_depth` `Bool` levels, to the tree, negated when an odd
    /// number of `-` precede it. The depth is checked before the deeper node is built.
    fn add_branch(
        &self,
        state: &mut ParseState<'_>,
        branch: SimpleQuery,
        branch_depth: usize,
    ) -> Result<(), OpenSearchError> {
        let is_negated = state.negations % 2 == 1;
        let branch_depth = branch_depth + usize::from(is_negated);
        let operation = state.current_operation.unwrap_or(self.default_operation);
        let tree_depth = if state.top.is_none() {
            branch_depth
        } else if state.previous_operation == Some(operation) {
            state.top_depth.max(branch_depth + 1)
        } else {
            state.top_depth.max(branch_depth) + 1
        };
        if tree_depth > MAX_SIMPLE_QUERY_DEPTH {
            return Err(refuse_deep_query());
        }
        state.top_depth = tree_depth;
        let branch = if is_negated {
            SimpleQuery::Bool {
                must: Vec::new(),
                should: vec![SimpleQuery::MatchAll],
                must_not: vec![branch],
            }
        } else {
            branch
        };
        let Some(top) = state.top.take() else {
            state.top = Some(branch);
            state.current_operation = None;
            return Ok(());
        };
        // The group grows while the operator stays the same; a change of operator closes the
        // group so far into one clause of a new group, which keeps the precedence of the
        // operators already applied.
        let (mut must, mut should, must_not) = match top {
            SimpleQuery::Bool { must, should, must_not } if state.previous_operation == Some(operation) => {
                (must, should, must_not)
            }
            closed_group => match operation {
                Occur::Must => (vec![closed_group], Vec::new(), Vec::new()),
                Occur::Should => (Vec::new(), vec![closed_group], Vec::new()),
            },
        };
        match operation {
            Occur::Must => must.push(branch),
            Occur::Should => should.push(branch),
        }
        state.top = Some(SimpleQuery::Bool { must, should, must_not });
        state.previous_operation = Some(operation);
        state.current_operation = None;
        Ok(())
    }
}

/// Parses `text` with the operators `flags` enables, joining unconnected clauses with
/// `default_operator`.
///
/// Never reports a syntax error; an empty query is [`SimpleQuery::MatchNone`].
///
/// # Errors
///
/// A `parsing_exception` for a fuzzy term, a phrase with a slop above zero, a query nested deeper
/// than the limit the reason names, or a text longer than a closer's position can state.
pub(crate) fn parse_simple_query_string(
    text: &str,
    flags: SimpleQueryFlags,
    default_operator: Operator,
) -> Result<SimpleQuery, OpenSearchError> {
    let characters: Vec<char> = text.chars().collect();
    let parser = SimpleQueryParser {
        flags,
        default_operation: Occur::from(default_operator),
        closing_positions: ClosingPositions::from_characters(&characters, flags)?,
    };
    let mut state = ParseState::new(&characters, 0, characters.len(), 0);
    parser.parse_sub_query(&mut state)?;
    Ok(state.top.unwrap_or(SimpleQuery::MatchNone))
}

/// The refusal of a query past [`MAX_SIMPLE_QUERY_DEPTH`].
fn refuse_deep_query() -> OpenSearchError {
    OpenSearchError::query_limit_exceeded(
        format!("[simple_query_string] nests deeper than the limit of {MAX_SIMPLE_QUERY_DEPTH} levels"),
        MAX_SIMPLE_QUERY_DEPTH,
    )
}

/// Whether the text after a `~` asks for something inexact: no text at all, or a positive integer
/// of any size. Any other text (a negative number, a fraction, a word) matches exactly.
fn is_inexact_suffix(suffix: &str) -> bool {
    if suffix.is_empty() {
        return true;
    }
    match suffix.parse::<u32>() {
        Ok(distance) => distance > 0,
        Err(error) => *error.kind() == IntErrorKind::PosOverflow,
    }
}

/// Whether `character` separates terms under `WHITESPACE`.
const fn is_whitespace(character: char) -> bool {
    matches!(character, ' ' | '\t' | '\n' | '\r')
}

#[cfg(test)]
mod tests {
    //! Expected trees are derived by hand from the `simple_query_string` rules: whitespace
    //! splits terms; `+` and `|` set the operator joining the next clause when a clause precedes
    //! them; `-` negates the next clause as "everything except it"; a change of operator closes
    //! the group built so far into one clause; quotes, parentheses and `\` group, nest and
    //! escape; an unmatched quote or parenthesis is skipped.

    use axum::http::StatusCode;
    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng as _};
    use serde_json::Value;

    use super::{
        AND, ClosingPositions, OR, SimpleQuery, SimpleQueryFlags, convert_character_count, parse_simple_query_string,
    };
    use crate::opensearch::error::{OpenSearchError, render_envelope};
    use crate::query_dsl::ast::Operator;

    /// Every flag but `ESCAPE`.
    const FLAGS_WITHOUT_ESCAPE: &str = "AND|OR|NOT|PREFIX|PHRASE|PRECEDENCE|WHITESPACE|FUZZY|NEAR";

    /// How many starts of the seeded closer texts must reach each condition under test.
    const MINIMUM_REACHED_STARTS: u32 = 5_000;

    fn build_term(text: &str) -> SimpleQuery {
        SimpleQuery::Term {
            text: text.to_owned(),
            is_prefix: false,
        }
    }

    fn build_prefix_term(text: &str) -> SimpleQuery {
        SimpleQuery::Term {
            text: text.to_owned(),
            is_prefix: true,
        }
    }

    fn build_phrase(text: &str) -> SimpleQuery {
        SimpleQuery::Phrase { text: text.to_owned() }
    }

    fn build_any_of(should: Vec<SimpleQuery>) -> SimpleQuery {
        SimpleQuery::Bool {
            must: Vec::new(),
            should,
            must_not: Vec::new(),
        }
    }

    fn build_all_of(must: Vec<SimpleQuery>) -> SimpleQuery {
        SimpleQuery::Bool {
            must,
            should: Vec::new(),
            must_not: Vec::new(),
        }
    }

    fn build_negation(excluded: SimpleQuery) -> SimpleQuery {
        SimpleQuery::Bool {
            must: Vec::new(),
            should: vec![SimpleQuery::MatchAll],
            must_not: vec![excluded],
        }
    }

    /// The envelope of `error`, which must be the `400 parsing_exception` every refusal here is.
    async fn render_parsing_refusal(error: OpenSearchError, label: &str) -> Value {
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}");
        assert_eq!(body["error"]["type"], "parsing_exception", "{label}");
        body
    }

    fn parse_text(text: &str, flags: &str, operator: Operator) -> SimpleQuery {
        let flags = SimpleQueryFlags::parse(flags).expect("valid flags");
        parse_simple_query_string(text, flags, operator).expect("parses")
    }

    /// `a+b|b+b|…` with `depth` operators: each one differs from the last, so each closes the group
    /// so far into a clause of a new `Bool`.
    fn build_operator_changes(depth: usize) -> String {
        (0..depth).fold("a".to_string(), |query, level| {
            let operator = if level % 2 == 0 { '+' } else { '|' };
            format!("{query}{operator}b")
        })
    }

    /// A text, its flags, the default operator and the tree it parses to.
    type ParseCase = (&'static str, &'static str, Operator, SimpleQuery);

    /// Well-formed queries with every operator enabled.
    fn build_operator_cases() -> Vec<ParseCase> {
        vec![
            (
                "foo bar",
                "ALL",
                Operator::Or,
                build_any_of(vec![build_term("foo"), build_term("bar")]),
            ),
            (
                "foo bar",
                "ALL",
                Operator::And,
                build_all_of(vec![build_term("foo"), build_term("bar")]),
            ),
            (
                "+foo -bar",
                "ALL",
                Operator::Or,
                build_any_of(vec![build_term("foo"), build_negation(build_term("bar"))]),
            ),
            (
                "+foo -bar",
                "ALL",
                Operator::And,
                build_all_of(vec![build_term("foo"), build_negation(build_term("bar"))]),
            ),
            ("-x", "ALL", Operator::Or, build_negation(build_term("x"))),
            ("--x", "ALL", Operator::Or, build_term("x")),
            ("\"a b\"", "ALL", Operator::Or, build_phrase("a b")),
            ("\"a b\"~0", "ALL", Operator::Or, build_phrase("a b")),
            ("\"a b\"~", "ALL", Operator::Or, build_phrase("a b")),
            ("\"a b\"~0.5", "ALL", Operator::Or, build_phrase("a b")),
            ("\"a\\\"b\"", "ALL", Operator::Or, build_phrase("a\"b")),
            ("foo*", "ALL", Operator::Or, build_prefix_term("foo")),
            ("foo\\*", "ALL", Operator::Or, build_term("foo*")),
            ("*", "ALL", Operator::Or, build_term("*")),
            ("foo~0", "ALL", Operator::Or, build_term("foo")),
            ("foo~abc", "ALL", Operator::Or, build_term("foo")),
            ("foo~0.8", "ALL", Operator::Or, build_term("foo")),
            ("foo~-1", "ALL", Operator::Or, build_term("foo")),
            ("foo*~0", "ALL", Operator::Or, build_term("foo*")),
            (
                "a|b",
                "ALL",
                Operator::And,
                build_any_of(vec![build_term("a"), build_term("b")]),
            ),
            (
                "a b +c",
                "ALL",
                Operator::Or,
                build_all_of(vec![
                    build_any_of(vec![build_term("a"), build_term("b")]),
                    build_term("c"),
                ]),
            ),
            (
                "(a b)",
                "ALL",
                Operator::Or,
                build_any_of(vec![build_term("a"), build_term("b")]),
            ),
            (
                "x (a b)",
                "ALL",
                Operator::And,
                build_all_of(vec![
                    build_term("x"),
                    build_all_of(vec![build_term("a"), build_term("b")]),
                ]),
            ),
            ("\\+a", "ALL", Operator::Or, build_term("+a")),
            ("a\\ b", "ALL", Operator::Or, build_term("a b")),
            ("foo-bar", "ALL", Operator::Or, build_term("foo-bar")),
        ]
    }

    /// Unmatched openers and closers, operators with nothing to apply to, queries with no term, and
    /// operators a flag leaves off.
    fn build_skipped_syntax_cases() -> Vec<ParseCase> {
        vec![
            (
                "\"a b",
                "ALL",
                Operator::Or,
                build_any_of(vec![build_term("a"), build_term("b")]),
            ),
            ("(a", "ALL", Operator::Or, build_term("a")),
            ("a)", "ALL", Operator::Or, build_term("a")),
            ("a +", "ALL", Operator::Or, build_term("a")),
            ("a\\", "ALL", Operator::Or, build_term("a")),
            ("\"\"", "ALL", Operator::Or, SimpleQuery::MatchNone),
            ("", "ALL", Operator::Or, SimpleQuery::MatchNone),
            ("   ", "ALL", Operator::Or, SimpleQuery::MatchNone),
            ("()", "ALL", Operator::Or, SimpleQuery::MatchNone),
            ("foo bar", "NONE", Operator::Or, build_term("foo bar")),
            (
                "-foo bar*",
                "WHITESPACE|PREFIX",
                Operator::Or,
                build_any_of(vec![build_term("-foo"), build_prefix_term("bar")]),
            ),
            (
                "a|b c",
                "or",
                Operator::And,
                build_any_of(vec![build_term("a"), build_term("b c")]),
            ),
            (
                "\"a b\" c",
                "WHITESPACE",
                Operator::Or,
                build_any_of(vec![build_term("\"a"), build_term("b\""), build_term("c")]),
            ),
            // The first `(` has no closer of its own, so only the second one groups.
            ("((a)", "ALL", Operator::Or, build_term("a")),
            // The escaped `)` is text, so the group ends at the last one.
            (
                "(a\\) b)",
                "ALL",
                Operator::Or,
                build_any_of(vec![build_term("a)"), build_term("b")]),
            ),
            // Without `ESCAPE` the same `)` ends the group, and `\` is text.
            (
                "(a\\) b)",
                FLAGS_WITHOUT_ESCAPE,
                Operator::Or,
                build_any_of(vec![build_term("a\\"), build_term("b")]),
            ),
            // The escaped quote closes nothing, so the opening quote is skipped.
            (
                "\"a \\\" b",
                "ALL",
                Operator::Or,
                build_any_of(vec![build_term("a"), build_term("\""), build_term("b")]),
            ),
            // The suffix `\` ends at the `(` or `"`, which then opens with nothing escaping it.
            (
                "x~\\(a)",
                "ALL",
                Operator::Or,
                build_any_of(vec![build_term("x"), build_term("a")]),
            ),
            (
                "x~\\\"a b\"",
                "ALL",
                Operator::Or,
                build_any_of(vec![build_term("x"), build_phrase("a b")]),
            ),
            // A group's closer lies past the end of an enclosing range: the inner `(` is skipped.
            ("(x~\\( ) )", "ALL", Operator::Or, build_term("x")),
            // Without their flags, `~` and `(` are text.
            ("foo~1", "WHITESPACE", Operator::Or, build_term("foo~1")),
            ("(a)", "WHITESPACE", Operator::Or, build_term("(a)")),
            (
                "\"a b\"~1",
                "PHRASE",
                Operator::Or,
                build_any_of(vec![build_phrase("a b"), build_term("~1")]),
            ),
        ]
    }

    #[test]
    fn simple_query_strings_parse_to_the_stated_ast() {
        for (text, flags, operator, expected) in build_operator_cases().into_iter().chain(build_skipped_syntax_cases())
        {
            assert_eq!(
                parse_text(text, flags, operator),
                expected,
                "{text:?} with {flags} and {operator:?}"
            );
        }
    }

    #[tokio::test]
    async fn fuzzy_terms_and_sloppy_phrases_are_refused() {
        for (text, construct, reason) in [
            ("foo~", "fuzzy", "fuzzy query [foo~] is not supported"),
            ("foo~1", "fuzzy", "fuzzy query [foo~1] is not supported"),
            ("foo~2 bar", "fuzzy", "fuzzy query [foo~2] is not supported"),
            // One past `u32::MAX` is still a positive integer.
            (
                "foo~4294967296",
                "fuzzy",
                "fuzzy query [foo~4294967296] is not supported",
            ),
            ("\"a b\"~1", "slop", "phrase slop [~1] on [\"a b\"] is not supported"),
            ("\"a b\"~ c", "slop", "phrase slop [~] on [\"a b\"] is not supported"),
            (
                "\"a b\"~4294967296",
                "slop",
                "phrase slop [~4294967296] on [\"a b\"] is not supported",
            ),
        ] {
            let error = parse_simple_query_string(text, SimpleQueryFlags::ALL, Operator::Or).expect_err("refused");
            let body = render_parsing_refusal(error, text).await;
            assert_eq!(body["error"]["construct"], construct, "{text:?}");
            assert_eq!(body["error"]["reason"], reason, "{text:?}");
        }
        for near_flag in ["PHRASE|NEAR", "PHRASE|SLOP"] {
            let flags = SimpleQueryFlags::parse(near_flag).expect("valid flags");
            let error = parse_simple_query_string("\"a b\"~1", flags, Operator::Or).expect_err("refused");
            let body = render_parsing_refusal(error, near_flag).await;
            assert_eq!(body["error"]["construct"], "slop", "{near_flag}");
        }
    }

    #[tokio::test]
    async fn unknown_flags_are_refused() {
        for flags in ["AND|SPELLING", "AND|", ""] {
            let error = SimpleQueryFlags::parse(flags).expect_err("refused");
            render_parsing_refusal(error, flags).await;
        }
    }

    #[test]
    fn flag_names_ignore_case_and_surrounding_spaces() {
        assert_eq!(
            SimpleQueryFlags::parse(" and | Or ").expect("valid flags"),
            SimpleQueryFlags(AND | OR)
        );
    }

    #[test]
    fn arbitrary_strings_never_fail_to_parse() {
        const ALPHABET: &[char] = &[
            'a', 'b', ' ', '\t', '+', '-', '|', '"', '*', '(', ')', '\\', '~', '0', '1', 'é',
        ];
        let mut generator = StdRng::seed_from_u64(0x5EED_5157);
        for _ in 0..5_000 {
            let length = generator.random_range(0..24);
            let text: String = (0..length)
                .map(|_| ALPHABET[generator.random_range(0..ALPHABET.len())])
                .collect();
            for operator in [Operator::Or, Operator::And] {
                let parse_result = parse_simple_query_string(&text, SimpleQueryFlags::ALL, operator);
                if !text.contains('~') {
                    assert!(parse_result.is_ok(), "{text:?}");
                }
            }
        }
    }

    #[tokio::test]
    async fn deep_queries_are_refused_above_the_depth_limit() {
        let build_groups = |depth: usize| format!("{}a{}", "(".repeat(depth), ")".repeat(depth));
        let build_closed_group = |depth: usize| format!("({})+x", build_operator_changes(depth));
        let build_grown_group = |depth: usize| format!("x y ({})", build_operator_changes(depth));
        let build_negated_group = |depth: usize| format!("-({})", build_operator_changes(depth));
        let cases = [
            ("groups", build_groups(127), build_groups(128), build_groups(129)),
            (
                "operator changes",
                build_operator_changes(127),
                build_operator_changes(128),
                build_operator_changes(129),
            ),
            // A closing `Bool`, a growing group and a negation each stack one level on the group.
            (
                "a closed group",
                build_closed_group(126),
                build_closed_group(127),
                build_closed_group(128),
            ),
            (
                "a grown group",
                build_grown_group(126),
                build_grown_group(127),
                build_grown_group(128),
            ),
            (
                "a negated group",
                build_negated_group(126),
                build_negated_group(127),
                build_negated_group(128),
            ),
        ];
        for (name, below_limit, at_limit, above_limit) in cases {
            for accepted_text in [below_limit, at_limit] {
                assert!(
                    parse_simple_query_string(&accepted_text, SimpleQueryFlags::ALL, Operator::Or).is_ok(),
                    "{name}: {accepted_text}"
                );
            }
            let error =
                parse_simple_query_string(&above_limit, SimpleQueryFlags::ALL, Operator::Or).expect_err("refused");
            let body = render_parsing_refusal(error, name).await;
            assert_eq!(body["error"]["limit"], 128, "{name}");
        }
    }

    #[tokio::test]
    async fn a_text_past_the_position_limit_is_refused() {
        // The count alone is tested: a text of this length takes 16 GiB of `char`s to build.
        assert_eq!(convert_character_count(4_294_967_294).expect("fits"), 4_294_967_294);
        assert_eq!(convert_character_count(4_294_967_295).expect("fits"), 4_294_967_295);

        let error = convert_character_count(4_294_967_296).expect_err("refused");

        let body = render_parsing_refusal(error, "one past the limit").await;
        assert_eq!(body["error"]["limit"], 4_294_967_295_u32);
    }

    /// Both openers are found in constant time; a rescan per opener would walk 5 * 10^9 characters.
    #[test]
    fn long_runs_of_unclosed_openers_parse() {
        const OPENER_COUNT: usize = 100_000;

        let unclosed_parentheses = format!("{}a", "(".repeat(OPENER_COUNT));
        assert_eq!(parse_text(&unclosed_parentheses, "ALL", Operator::Or), build_term("a"));

        // Each `"` is reached right after the edit distance `\`, so it opens a phrase that the
        // following `\"` keeps from closing.
        let unclosed_quotes = "x~\\\"".repeat(OPENER_COUNT);
        let SimpleQuery::Bool { should, .. } = parse_text(&unclosed_quotes, "ALL", Operator::Or) else {
            panic!("a group of terms")
        };
        assert_eq!(should, vec![build_term("x"); OPENER_COUNT]);
    }

    /// The scan a group's opener stands for: `depth` opens at one, and an `ESCAPE` skips a character.
    fn scan_for_closing_parenthesis(characters: &[char], start: usize, is_escaping: bool) -> usize {
        let mut depth = 1;
        let mut position = start;
        while position < characters.len() {
            match characters[position] {
                '\\' if is_escaping => position += 1,
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return position;
                    }
                }
                _ => {}
            }
            position += 1;
        }
        characters.len()
    }

    /// The scan a phrase's opener stands for.
    fn scan_for_closing_quote(characters: &[char], start: usize, is_escaping: bool) -> usize {
        let mut position = start;
        while position < characters.len() {
            match characters[position] {
                '\\' if is_escaping => position += 1,
                '"' => return position,
                _ => {}
            }
            position += 1;
        }
        characters.len()
    }

    #[test]
    fn closing_positions_agree_with_a_scan_from_every_position() {
        const ALPHABET: [char; 6] = ['a', ' ', '(', ')', '"', '\\'];
        let mut generator = StdRng::seed_from_u64(0xC105_E125);
        // How many starts reached each condition the tables have to get right.
        let (mut closed_group_count, mut closed_phrase_count) = (0_u32, 0_u32);
        let (mut escape_decided_count, mut nesting_decided_count) = (0_u32, 0_u32);
        for _ in 0..2_000 {
            let length = generator.random_range(0..32);
            let characters: Vec<char> = (0..length)
                .map(|_| ALPHABET[generator.random_range(0..ALPHABET.len())])
                .collect();
            for (flags, is_escaping) in [
                (SimpleQueryFlags::ALL, true),
                (
                    SimpleQueryFlags::parse(FLAGS_WITHOUT_ESCAPE).expect("valid flags"),
                    false,
                ),
            ] {
                let closing_positions = ClosingPositions::from_characters(&characters, flags).expect("fits");
                for start in 0..=characters.len() {
                    let closing_parenthesis = scan_for_closing_parenthesis(&characters, start, is_escaping);
                    let closing_quote = scan_for_closing_quote(&characters, start, is_escaping);
                    assert_eq!(
                        closing_positions.find_closing_parenthesis(start),
                        closing_parenthesis,
                        "{characters:?} from {start}"
                    );
                    assert_eq!(
                        closing_positions.find_closing_quote(start),
                        closing_quote,
                        "{characters:?} from {start}"
                    );
                    closed_group_count += u32::from(closing_parenthesis < characters.len());
                    closed_phrase_count += u32::from(closing_quote < characters.len());
                    if is_escaping {
                        let is_escape_decided = closing_parenthesis
                            != scan_for_closing_parenthesis(&characters, start, false)
                            || closing_quote != scan_for_closing_quote(&characters, start, false);
                        escape_decided_count += u32::from(is_escape_decided);
                    } else {
                        let first_parenthesis = characters[start..]
                            .iter()
                            .position(|&character| character == ')')
                            .map_or(characters.len(), |offset| start + offset);
                        nesting_decided_count += u32::from(closing_parenthesis != first_parenthesis);
                    }
                }
            }
        }
        assert!(closed_group_count >= MINIMUM_REACHED_STARTS, "{closed_group_count}");
        assert!(closed_phrase_count >= MINIMUM_REACHED_STARTS, "{closed_phrase_count}");
        assert!(escape_decided_count >= MINIMUM_REACHED_STARTS, "{escape_decided_count}");
        assert!(
            nesting_decided_count >= MINIMUM_REACHED_STARTS,
            "{nesting_decided_count}"
        );
    }

    #[test]
    fn a_closing_table_is_built_only_for_the_flag_that_reads_it() {
        let characters: Vec<char> = "(a \"b\")".chars().collect();
        // One entry per character and two past the text.
        for (flags, parenthesis_entries, quote_entries) in [
            ("PRECEDENCE", 9, 0),
            ("PHRASE", 0, 9),
            ("PRECEDENCE|PHRASE", 9, 9),
            ("WHITESPACE", 0, 0),
        ] {
            let closing_positions =
                ClosingPositions::from_characters(&characters, SimpleQueryFlags::parse(flags).expect("valid flags"))
                    .expect("fits");
            assert_eq!(closing_positions.parentheses.len(), parenthesis_entries, "{flags}");
            assert_eq!(closing_positions.quotes.len(), quote_entries, "{flags}");
        }
    }
}
