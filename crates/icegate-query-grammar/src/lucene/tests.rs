//! The accept/reject table, the lexer modes and the group depth of the adjusted Lucene
//! grammar.
//!
//! Every row is derived by hand from the classic query parser's rules and the
//! adjustments listed in the crate README.

use std::rc::Rc;

use antlr4rust::tree::ParseTree;

use super::{
    BareOperatorTermContextAttrs, BoostContextAttrs, ClauseContextAttrs, ConjoinedClauseContextAttrs,
    ConjunctionContextAttrs, FieldNameContextAttrs, GroupContextAttrs, ModClauseContextAll, ModClauseContextAttrs,
    ModifierContextAttrs, PlainTermContextAttrs, QueryContextAll, QueryContextAttrs, TermContextAll,
    TermModifiersContextAttrs, TopLevelQueryContextAttrs, measure_group_depth, parse_lucene_query,
};
use crate::SyntaxError;

/// The clauses of a parsed query string, first one first.
fn collect_clauses(text: &str) -> Vec<Rc<ModClauseContextAll<'_>>> {
    let tree = parse_lucene_query(text).expect("the query string parses");
    let query = tree.query().expect("a query");
    let mut clauses = vec![query.modClause().expect("a first clause")];
    clauses.extend(
        query
            .conjoinedClause_all()
            .iter()
            .map(|conjoined| conjoined.modClause().expect("a clause")),
    );
    clauses
}

/// One clause as (conjunction, modifier, field name, term variant, term text). Each text
/// joins the clause's tokens without the skipped whitespace, so a term's text includes its
/// boost and fuzzy slop; a group's variant is `group` and its text spans its parentheses.
type ClauseShape<'a> = (Option<&'a str>, Option<&'a str>, Option<&'a str>, &'a str, &'a str);

/// The name of a `term` alternative, as the grammar labels it.
fn name_term_variant(term: &TermContextAll<'_>) -> &'static str {
    match term {
        TermContextAll::PlainTermContext(_) => "plain",
        TermContextAll::StarTermContext(_) => "star",
        TermContextAll::PrefixTermContext(_) => "prefix",
        TermContextAll::WildcardTermContext(_) => "wildcard",
        TermContextAll::RegexpTermContext(_) => "regexp",
        TermContextAll::BareOperatorTermContext(_) => "bare operator",
        TermContextAll::QuotedTermContext(_) => "quoted",
        TermContextAll::RangeTermContext(_) => "range",
        TermContextAll::Error(_) => "error",
    }
}

/// Asserts that the clauses of `query`, first one first, have exactly the `expected` shapes.
fn assert_clause_shapes(text: &str, query: &QueryContextAll<'_>, expected: &[ClauseShape<'_>]) {
    let first_clause = (None, query.modClause().expect("a first clause"));
    let conjoined_clauses = query.conjoinedClause_all().into_iter().map(|conjoined| {
        (
            conjoined.conjunction().map(|conjunction| conjunction.get_text()),
            conjoined.modClause().expect("a clause"),
        )
    });
    let shapes: Vec<_> = std::iter::once(first_clause)
        .chain(conjoined_clauses)
        .map(|(conjunction, mod_clause)| {
            let clause = mod_clause.clause().expect("a clause");
            let field_name = clause.fieldName().map(|field_name| {
                field_name
                    .TERM()
                    .or_else(|| field_name.STAR())
                    .expect("a field-name token")
                    .get_text()
            });
            let (variant, term_text) = clause.term().map_or_else(
                || ("group", clause.group().expect("a term or a group").get_text()),
                |term| (name_term_variant(&term), term.get_text()),
            );
            (
                conjunction,
                mod_clause.modifier().map(|modifier| modifier.get_text()),
                field_name,
                variant,
                term_text,
            )
        })
        .collect();
    let borrowed_shapes: Vec<ClauseShape<'_>> = shapes
        .iter()
        .map(|(conjunction, modifier, field_name, variant, term_text)| {
            (
                conjunction.as_deref(),
                modifier.as_deref(),
                field_name.as_deref(),
                *variant,
                term_text.as_str(),
            )
        })
        .collect();
    assert_eq!(borrowed_shapes, expected, "[{text}]");
}

/// Asserts that each `(text, line, column, offending text, message)` row fails with exactly
/// that error.
fn assert_syntax_errors(rows: &[(&str, isize, isize, Option<&str>, &str)]) {
    for &(text, line, column, offending_text, message) in rows {
        assert_eq!(
            parse_lucene_query(text).map(|_| ()),
            Err(SyntaxError {
                line,
                column,
                offending_text: offending_text.map(str::to_owned),
                message: message.to_owned()
            }),
            "[{text}]"
        );
    }
}

#[test]
fn classic_query_strings_parse_and_invalid_ones_fail_as_the_adjusted_grammar_states() {
    // (text, clause shapes) in the order of `assert_clause_shapes`.
    let accepted_rows: &[(&str, &[ClauseShape<'_>])] = &[
        (
            "a OR b AND c",
            &[
                (None, None, None, "plain", "a"),
                (Some("OR"), None, None, "plain", "b"),
                (Some("AND"), None, None, "plain", "c"),
            ],
        ),
        // `!` is no term character, so `foo` ends there and `!` is the `NOT` modifier.
        (
            "foo!bar",
            &[
                (None, None, None, "plain", "foo"),
                (None, Some("!"), None, "plain", "bar"),
            ],
        ),
        (
            "+ a",
            &[
                (None, None, None, "bare operator", "+ "),
                (None, None, None, "plain", "a"),
            ],
        ),
        ("roam~0.8", &[(None, None, None, "plain", "roam~0.8")]),
        ("roam~", &[(None, None, None, "plain", "roam~")]),
        ("\"a b\"~2", &[(None, None, None, "quoted", "\"a b\"~2")]),
        ("[a TO b}", &[(None, None, None, "range", "[aTOb}")]),
        ("{* TO 5]", &[(None, None, None, "range", "{*TO5]")]),
        ("age:>=10", &[(None, None, Some("age"), "plain", ">=10")]),
        ("user@example.com", &[(None, None, None, "plain", "user@example.com")]),
        ("field:(a b)", &[(None, None, Some("field"), "group", "(ab)")]),
        ("a^2", &[(None, None, None, "plain", "a^2")]),
        ("a ^2", &[(None, None, None, "plain", "a^2")]),
        (
            "resource_attributes.\\*:v",
            &[(None, None, Some("resource_attributes.\\*"), "plain", "v")],
        ),
        ("/ERR.*/", &[(None, None, None, "regexp", "/ERR.*/")]),
        ("*", &[(None, None, None, "star", "*")]),
        ("*:*", &[(None, None, Some("*"), "star", "*")]),
        ("foo*", &[(None, None, None, "prefix", "foo*")]),
        ("f?o", &[(None, None, None, "wildcard", "f?o")]),
        ("*o", &[(None, None, None, "wildcard", "*o")]),
        (
            "a && b || c",
            &[
                (None, None, None, "plain", "a"),
                (Some("&&"), None, None, "plain", "b"),
                (Some("||"), None, None, "plain", "c"),
            ],
        ),
        ("NOT a", &[(None, Some("NOT"), None, "plain", "a")]),
        ("-a", &[(None, Some("-"), None, "plain", "a")]),
        ("!x", &[(None, Some("!"), None, "plain", "x")]),
        (
            "a - b",
            &[
                (None, None, None, "plain", "a"),
                (None, None, None, "bare operator", "- "),
                (None, None, None, "plain", "b"),
            ],
        ),
        (
            "x:[\"a b\" TO \"c\"]",
            &[(None, None, Some("x"), "range", "[\"a b\"TO\"c\"]")],
        ),
        ("TO", &[(None, None, None, "plain", "TO")]),
        ("a\\ b", &[(None, None, None, "plain", "a\\ b")]),
        ("\u{3000}a", &[(None, None, None, "plain", "a")]),
        (
            "[a TO b] AND c",
            &[
                (None, None, None, "range", "[aTOb]"),
                (Some("AND"), None, None, "plain", "c"),
            ],
        ),
        // `&` is a term character, so the longest match is one term.
        ("a&&b", &[(None, None, None, "plain", "a&&b")]),
        ("(a)^2", &[(None, None, None, "group", "(a)^2")]),
        ("\"a\"^2~1", &[(None, None, None, "quoted", "\"a\"^2~1")]),
        ("caf\\u00e9", &[(None, None, None, "plain", "caf\\u00e9")]),
        ("\"\"", &[(None, None, None, "quoted", "\"\"")]),
    ];
    for &(text, expected) in accepted_rows {
        let tree = parse_lucene_query(text).expect("the query string parses");
        assert_clause_shapes(text, &tree.query().expect("a query"), expected);
    }
    // A group holds a query of its own: `field:(a b)` groups two plain clauses.
    let field_group = collect_clauses("field:(a b)")[0]
        .clause()
        .expect("a clause")
        .group()
        .expect("a group");
    assert_clause_shapes(
        "field:(a b)",
        &field_group.query().expect("a query"),
        &[(None, None, None, "plain", "a"), (None, None, None, "plain", "b")],
    );
    // (text, line, column, offending text): a lexer error is reported at the unmatched
    // character, a parser error at the token the parser stopped on (`None` at the end
    // of the text).
    let rejected_rows = [
        ("a/b", 1, 1, Some("/")),
        ("a^b", 1, 2, Some("b")),
        ("a^ 2", 1, 2, Some(" ")),
        ("a^", 1, 2, None),
        ("()", 1, 1, Some(")")),
        ("a:", 1, 2, None),
        ("(a", 1, 2, None),
        ("a)", 1, 1, Some(")")),
        ("[a TO]", 1, 5, Some("]")),
        ("[a b]", 1, 3, Some("b")),
        ("AND", 1, 0, Some("AND")),
        ("a AND", 1, 5, None),
        ("\"abc", 1, 0, Some("\"")),
        ("f:[a TO b", 1, 9, None),
        (":a", 1, 0, Some(":")),
        ("a OR OR b", 1, 5, Some("OR")),
        ("a:b:c", 1, 3, Some(":")),
        ("", 1, 0, None),
    ];
    for (text, line, column, offending_text) in rejected_rows {
        assert_eq!(
            parse_lucene_query(text)
                .map(|_| ())
                .map_err(|error| (error.line, error.column, error.offending_text)),
            Err((line, column, offending_text.map(str::to_owned))),
            "[{text}] fails"
        );
    }
}

#[test]
fn an_unmatched_character_is_a_lexer_error_at_its_position() {
    let rows = [
        ("a/b", 1, 1, Some("/"), "unexpected character [/]"),
        ("\"abc", 1, 0, Some("\""), "unexpected character [\"]"),
        ("a^b", 1, 2, Some("b"), "unexpected character [b]"),
        ("a\nb/c", 2, 1, Some("/"), "unexpected character [/]"),
        // Columns count characters, not bytes: U+3000 and U+00E9 take three and two bytes.
        ("\u{3000}a/b", 1, 2, Some("/"), "unexpected character [/]"),
        ("caf\u{e9}/", 1, 4, Some("/"), "unexpected character [/]"),
    ];
    assert_syntax_errors(&rows);
}

#[test]
fn a_token_the_grammar_does_not_allow_is_a_parser_error_at_that_token_or_at_the_end_of_the_text() {
    let rows = [
        ("a)", 1, 1, Some(")"), "unexpected [)]"),
        ("a AND", 1, 5, None, "unexpected end of query"),
        ("caf\u{e9})", 1, 4, Some(")"), "unexpected [)]"),
    ];
    assert_syntax_errors(&rows);
}

#[test]
fn a_range_returns_to_the_default_mode_after_its_closing_bracket() {
    let tree = parse_lucene_query("[a TO b] AND c").expect("parses");
    let conjoined_clause = tree.query().expect("a query").conjoinedClause(0).expect("a second clause");
    assert!(conjoined_clause.conjunction().expect("a conjunction").AND().is_some());
    // After `]`, `TO` is an ordinary term again: two clauses, the second a plain term.
    let clauses_after_range = collect_clauses("[a TO b] TO");
    assert_eq!(clauses_after_range.len(), 2);
    let term = clauses_after_range[1].clause().expect("a clause").term().expect("a term");
    let TermContextAll::PlainTermContext(plain_term) = &*term else {
        panic!("a plain term")
    };
    assert_eq!(plain_term.TERM().expect("a TERM").get_text(), "TO");
}

#[test]
fn a_number_is_a_boost_only_after_a_caret() {
    let boosted_clauses = collect_clauses("a^2");
    let term = boosted_clauses[0].clause().expect("a clause").term().expect("a term");
    let TermContextAll::PlainTermContext(plain_term) = &*term else {
        panic!("a plain term")
    };
    let boost = plain_term.termModifiers().expect("modifiers").boost().expect("a boost");
    assert_eq!(boost.NUMBER().expect("a NUMBER").get_text(), "2");
    let number_clauses = collect_clauses("2");
    let term = number_clauses[0].clause().expect("a clause").term().expect("a term");
    let TermContextAll::PlainTermContext(plain_term) = &*term else {
        panic!("a plain term")
    };
    assert_eq!(plain_term.TERM().expect("a TERM").get_text(), "2");
    assert_eq!(
        parse_lucene_query("a^b").map(|_| ()),
        Err(SyntaxError {
            line: 1,
            column: 2,
            offending_text: Some(String::from("b")),
            message: String::from("unexpected character [b]"),
        })
    );
}

#[test]
fn a_bare_operator_followed_by_whitespace_parses_as_a_term() {
    let bare_operator_clauses = collect_clauses("+ a");
    assert_eq!(bare_operator_clauses.len(), 2);
    assert!(bare_operator_clauses[0].modifier().is_none());
    let term = bare_operator_clauses[0].clause().expect("a clause").term().expect("a term");
    let TermContextAll::BareOperatorTermContext(operator) = &*term else {
        panic!("a bare operator term")
    };
    assert_eq!(operator.BAREOPER().expect("a BAREOPER").get_text(), "+ ");
    let required_clauses = collect_clauses("+a");
    assert_eq!(required_clauses.len(), 1);
    assert!(required_clauses[0].modifier().expect("a modifier").PLUS().is_some());
}

#[test]
fn group_depth_counts_only_the_parenthesis_tokens_the_parser_reads() {
    let rows = [
        ("", 0),
        ("((a) (b (c)))", 3),
        // An escaped parenthesis is a term character.
        ("\\(\\( (a)", 1),
        // A quoted phrase is one token.
        ("\"((\" (a)", 1),
        // In the range mode a parenthesis is part of a bound.
        ("[( TO )] (a)", 1),
        ("/a(b/ (c)", 1),
        // A quote with no closing quote matches no rule, so the lexer error takes the rest
        // of the text and the groups after it are no tokens.
        ("(a) \"b ((c))", 1),
        // A quote inside a range bound or a regular expression term opens no phrase.
        ("[a\"b TO c] ((d))", 2),
        ("[\"a TO b] ((c))", 2),
        ("/a\"b/ ((c))", 2),
        // A `)` with no group open closes nothing.
        (")) ((a))", 2),
    ];
    for (text, depth) in rows {
        assert_eq!(measure_group_depth(text), depth, "[{text}]");
    }
}
