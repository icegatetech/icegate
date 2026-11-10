//! Accept/reject lists for the vendored PPL grammar and the lexing rules IceGate adds to it.
//!
//! Every expected outcome is derived by hand from the pinned grammar files beside this module and
//! the stated lexing rules: keywords match in any case through the per-code-point upper case of
//! the look-ahead, and a non-whitespace character that is no token is an error.

use antlr4rust::TokenSource;
use antlr4rust::char_stream::CharStream;
use antlr4rust::int_stream::{EOF, IntStream};
use antlr4rust::token::Token;

use super::char_stream::UpperCaseCharStream;
use super::{
    OpenSearchPPLLexer, build_ppl_parser, find_first_unrecognized_character, measure_ppl_nesting, opensearchppllexer,
    parse_ppl_root,
};

#[test]
fn nesting_depth_counts_the_bracket_tokens_the_parser_reads() {
    let rows = [
        ("", 0),
        ("source=logs", 0),
        ("source=logs | where (a = 1) and ((b = 2))", 2),
        // Parentheses and square brackets open one count of levels: a subsearch inside a group.
        ("source=logs | where (x in [ source=logs | where (y = 1) ])", 3),
        ("source=logs | append [ source=logs | append [ source=logs ] ]", 2),
        ("source=logs | eval x = abs(abs(abs(1)))", 3),
        // A bracket inside a string, a quoted name or a comment is part of that token.
        ("source=logs | where body = '((([[[' and (a = 1)", 1),
        ("source=logs | where body = \"(((\" and (a = 1)", 1),
        ("source=logs | fields `((x` | where (a = 1)", 1),
        ("source=logs /* ((( */ | where (a = 1)", 1),
        // A closing bracket with no level open closes nothing, of either kind.
        ("source=logs | where ]) ((a = 1))", 2),
        // A closing bracket of the other kind closes a level all the same.
        ("source=logs | where ([a = 1)]", 2),
        // A character that is no token neither opens nor closes a level.
        ("source=logs | where (\u{E9}(a = 1))", 2),
    ];
    for (text, depth) in rows {
        assert_eq!(measure_ppl_nesting(text).bracket_depth(), depth, "[{text}]");
    }
    // A run of one million opening parentheses is counted, not parsed.
    assert_eq!(measure_ppl_nesting(&"(".repeat(1_000_000)).bracket_depth(), 1_000_000);
}

#[test]
fn square_bracket_depth_counts_the_square_brackets_alone() {
    let rows = [
        ("", 0),
        ("source=logs | where ((a = 1))", 0),
        ("source=logs | where (x in [ source=logs | where (y = 1) ])", 1),
        ("source=logs | append [ source=logs | append [ source=logs ] ]", 2),
        (
            "source=logs | where x in [ source=logs | where (y in [ source=logs | fields y ]) | fields x ]",
            2,
        ),
        // Two subsearches side by side nest one level each.
        ("source=logs | append [ source=logs ] | append [ source=logs ]", 1),
        // A square bracket inside a string, a quoted name or a comment is part of that token.
        ("source=logs | where body = '[[[' | append [ source=logs ]", 1),
        ("source=logs | fields `[[x` /* [[ */ | append [ source=logs ]", 1),
        // A parenthesis neither opens nor closes a level of square brackets; a `]` with none open
        // closes none.
        ("source=logs | where ([(a = 1)])", 1),
        ("source=logs | where ]] [[a = 1", 2),
        ("source=logs | where [)[", 2),
    ];
    for (text, depth) in rows {
        assert_eq!(measure_ppl_nesting(text).square_bracket_depth(), depth, "[{text}]");
    }
    // A run of one million opening square brackets is counted, not parsed.
    assert_eq!(
        measure_ppl_nesting(&"[".repeat(1_000_000)).square_bracket_depth(),
        1_000_000
    );
}

#[test]
fn operator_count_counts_every_operator_token_wherever_it_stands() {
    let rows = [
        ("", 0),
        ("source=logs | head 5", 1),
        // `source=` holds an `=`. Each logical keyword counts in any case, and so does each
        // comparison it joins.
        ("source=logs | where a = 1 or b = 2 AND c = 3 xor d = 4", 8),
        ("source=logs | where not NOT not a = 1", 5),
        (
            "source=logs | where a == 1 and b != 2 and c < 3 and d <= 4 and e > 5 and f >= 6",
            12,
        ),
        (
            "source=logs | where a regexp 'x' or a like 'x' or a ilike 'x' or contains(a, 'x')",
            8,
        ),
        (
            "source=logs | where a in (1, 2) and b is not null and c not between 1 and 2",
            9,
        ),
        ("source=logs | eval x = 1 * 2 / 3 % 4 + 5 - 6", 7),
        ("source=logs | eval x = transform(list, y -> y + 1)", 4),
        ("source=logs | where t > now() - interval 1 day", 4),
        // A bare quote or backtick is a token of its own; a quoted string or name is one token.
        ("source=logs | where body = 'a = b' | fields `a + b`", 2),
        ("source=logs | where ' \" `", 4),
        // An operator inside a comment counts for nothing.
        ("source=logs /* not and or = + */ | head", 1),
        // Brackets are counted as depth, not as operators.
        ("source=logs | where ((a = 1))", 2),
    ];
    for (text, operator_count) in rows {
        assert_eq!(measure_ppl_nesting(text).operator_count(), operator_count, "[{text}]");
    }
    // A run of one hundred thousand `not` is counted, not parsed.
    assert_eq!(measure_ppl_nesting(&"not ".repeat(100_000)).operator_count(), 100_000);
}

/// The longest run of `delimiter`-quoted text starting at `start`, the way the quoted-string
/// rules read it: the delimiter, any characters other than it or a doubled delimiter, the
/// delimiter. `None` when no such text starts there, so the delimiter is a token of its own.
/// The texts this reads hold no backslash, so the escape of the rules never applies.
fn find_quoted_text_end(characters: &[char], start: usize, delimiter: char) -> Option<usize> {
    let mut longest_end = None;
    let mut index = start + 1;
    while index < characters.len() {
        if characters[index] != delimiter {
            index += 1;
            continue;
        }
        let run_end = characters[index..]
            .iter()
            .position(|&character| character != delimiter)
            .map_or(characters.len(), |offset| index + offset);
        let run_length = run_end - index;
        if run_length % 2 == 1 {
            // Pairs inside the text, then the closing delimiter: the text ends at the run's end.
            return Some(run_end - 1);
        }
        // An even run reads as pairs and the text goes on; it could also close after any pair.
        longest_end = Some(index);
        index = run_end;
    }
    longest_end
}

/// The bracket depth, the square bracket depth and the operator count of `text` over the alphabet
/// of [`measure_ppl_nesting_matches_a_character_model_of_random_texts`], read character by
/// character from the lexing rules of strings, brackets, `+` and `=` / `==`, independently of the
/// lexer.
fn model_ppl_nesting(text: &str) -> (usize, usize, usize) {
    let characters: Vec<char> = text.chars().collect();
    let (mut depth, mut deepest, mut operator_count) = (0_usize, 0_usize, 0_usize);
    let (mut square_depth, mut deepest_square) = (0_usize, 0_usize);
    let mut index = 0;
    while index < characters.len() {
        match characters[index] {
            '(' => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            '[' => {
                depth += 1;
                deepest = deepest.max(depth);
                square_depth += 1;
                deepest_square = deepest_square.max(square_depth);
            }
            ')' => depth = depth.saturating_sub(1),
            ']' => {
                depth = depth.saturating_sub(1);
                square_depth = square_depth.saturating_sub(1);
            }
            '+' => operator_count += 1,
            '=' => {
                operator_count += 1;
                if characters.get(index + 1) == Some(&'=') {
                    index += 1;
                }
            }
            delimiter @ ('\'' | '"' | '`') => match find_quoted_text_end(&characters, index, delimiter) {
                Some(end) => index = end,
                None => operator_count += 1,
            },
            _ => {}
        }
        index += 1;
    }
    (deepest, deepest_square, operator_count)
}

#[test]
fn measure_ppl_nesting_matches_a_character_model_of_random_texts() {
    const ALPHABET: [char; 11] = ['(', ')', '[', ']', '\'', '"', '`', '+', '=', 'q', ' '];
    // xorshift64 with a fixed seed: the same texts on every run.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next_random = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for _ in 0..5_000 {
        let length = usize::try_from(next_random() % 48).expect("a small length");
        let text: String = (0..length)
            .map(|_| ALPHABET[usize::try_from(next_random() % 11).expect("a small index")])
            .collect();
        let nesting = measure_ppl_nesting(&text);
        assert_eq!(
            (
                nesting.bracket_depth(),
                nesting.square_bracket_depth(),
                nesting.operator_count()
            ),
            model_ppl_nesting(&text),
            "[{text}]"
        );
    }
}

#[test]
fn ppl_queries_parse_or_fail_as_the_pinned_grammar_and_lexing_rules_state() {
    // `queryStatement` starts with a command of `pplCommands`; every one of those that reads rows
    // but `describe` requires a `source=` / `index=` clause.
    let accepted_queries = [
        "source=logs",
        "SOURCE=logs | WHERE severity_text = 'ERROR'",
        "source=logs | where severity_text = 'ERROR'",
        "Source=logs | Where severity_text = 'ERROR' | Fields body",
        "search source=logs",
        "sEaRcH SoUrCe=logs | HeAd 5",
        // U+017F LATIN SMALL LETTER LONG S upper-cases to `S`.
        "\u{17F}ource=logs",
        // U+0131 LATIN SMALL LETTER DOTLESS I upper-cases to `I`, so `ın` is the keyword `IN`.
        "source=logs | where severity_text \u{131}n ('ERROR')",
        // `root` is `pplStatement? EOF`, so an empty query parses; refusing it is left to the
        // caller that walks the tree.
        "",
        "source=logs | where severity_text = 'ERROR' |",
        "source=logs | head 10 from 5",
        "source = logs | stats count() by span(timestamp, 1h)",
        "source=logs | sort - `timestamp`",
        "source=logs | fields log_attributes.`http.method`",
        "source=logs,spans | head 1",
        "source=logs* | head 1",
        "describe logs",
        // Whitespace is skipped whatever its kind: TAB and NO-BREAK SPACE.
        "source=logs\t|\u{A0}where severity_text = 'ERROR'",
        "source=logs | stats take(body, 5)",
    ];
    for query in accepted_queries {
        let outcome = parse_ppl_root(query);
        assert!(outcome.is_ok(), "query {query:?}: {:?}", outcome.err());
    }

    // Each refusal as (line, column, offending text); columns count characters from 0.
    let rejected_rows: &[(&str, isize, isize, Option<&str>)] = &[
        // `é` is no token of the grammar; it is the 22nd character.
        ("source=logs | where x\u{E9} = 1", 1, 21, Some("\u{E9}")),
        // No `source=` clause: `searchCommand` (`SEARCH? searchExpression* fromClause ...`) reads
        // every word as a search term, so the required `fromClause` is missing at the end.
        ("where severity_text = 'ERROR'", 1, 29, Some("<EOF>")),
        ("head 5", 1, 6, Some("<EOF>")),
        // `1e6` lexes as one NUMERIC_ID token (`DEC_DIGIT+ ID_LITERAL`, longer than the
        // INTEGER_LITERAL `1`), which only `searchLiteral` and `foreachPlaceholderName` accept, so
        // the expression of `evalClause` fails at that token, which starts at column 23.
        ("source=logs | eval x = 1e6", 1, 23, Some("1e6")),
        // An unterminated string matches no SQUOTA_STRING, so `'` lexes as a bare SINGLE_QUOTE,
        // which only `wildcard` accepts and no expression reaches: the comparison after `=` fails
        // at that token, which starts at column 27.
        ("source=logs | where body = 'unterminated", 1, 27, Some("'")),
        // `take` is an aggregation of `stats`, not a command.
        ("source=logs | take(body, 5)", 1, 14, Some("take")),
    ];
    for (query, line, column, offending_text) in rejected_rows {
        let outcome = parse_ppl_root(query)
            .map(|_| ())
            .map_err(|error| (error.line, error.column, error.offending_text));
        assert_eq!(
            outcome,
            Err((*line, *column, offending_text.map(str::to_string))),
            "query {query:?}"
        );
    }
}

#[test]
fn every_trust_ppl_form_parses() {
    // The PPL forms of trust's statements and filter fragments, with concrete parameters.
    let forms = [
        r"source=spans | stats count() as n",
        r"source=logs | stats count() as n",
        r"source=logs | where `timestamp` >= timestamp('2026-10-08 00:00:00.000000') and `timestamp` < timestamp('2026-10-09 00:00:00.000000') | eval svc = coalesce(service_name, ''), host = coalesce(log_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`, ''), body_hash = coalesce(md5(body), '') | where `timestamp` < timestamp('2026-10-08 12:00:00.000000') or (`timestamp` = timestamp('2026-10-08 12:00:00.000000') and svc > 'api') | where severity_text = 'ERROR' | sort - `timestamp`, svc, host, body_hash | head 51",
        r"source=logs | where `timestamp` >= timestamp('2026-10-08 00:00:00.000000') | where trace_id = '0af7651916cd43dd8448eb211c80319c' | sort - `timestamp` | head 51",
        r"source=logs | where `timestamp` >= timestamp('2026-10-08 00:00:00.000000') | eval bucket_index = timestampdiff(MICROSECOND, timestamp('2026-10-08 00:00:00.000000'), `timestamp`) * 24 / 86400000000, level = case(severity_text = 'ERROR', 'error', severity_text = 'WARN', 'warn' else 'other') | stats count() as n by bucket_index, level",
        r"source=logs | where `timestamp` = timestamp('2026-10-08 12:00:00.123456') | eval svc = coalesce(service_name, ''), host = coalesce(log_attributes.`host.name`, ''), body_hash = coalesce(md5(body), '') | where svc = 'api' and host = 'node-a' and body_hash = '9e107d9d372bb6826bd81d3542a419d6' | head 1",
        r"source=logs | where `timestamp` = timestamp('2026-10-08 12:00:00.123456') | head 1 | append [ source=logs | where `timestamp` < timestamp('2026-10-08 12:00:00.123456') | sort - `timestamp` | head 5 | eval context_row_kind = 'older' ]",
        r"source=logs | stats values(service_name) as v0, values(severity_text) as v1 | eval v0 = mvindex(v0, 0, 200), v1 = mvindex(v1, 0, 200)",
        r"source=spans | where `timestamp` >= timestamp('2026-10-08 00:00:00.000000') and `timestamp` < timestamp('2026-10-09 00:00:00.000000') | where trace_id in [ source=spans | where status_code = 2 | fields trace_id ] | eval root_service = case(isnull(parent_span_id), service_name), is_err = if(status_code = 2, 1, 0) | stats min(`timestamp`) as started_at, max(end_timestamp) as ended_at, count() as span_count, sum(is_err) as error_count, values(root_service) as root_services by trace_id | where span_count > 1 | sort - started_at, trace_id | head 51",
        r"source=spans | where `timestamp` >= timestamp('2026-10-08 00:00:00.000000') | stats count(eval(status_code = 2)) as matched, min(`timestamp`) as started_at by trace_id | where matched > 0 | where started_at < timestamp('2026-10-08 12:00:00.000000') or (started_at = timestamp('2026-10-08 12:00:00.000000') and trace_id > '0af7651916cd43dd8448eb211c80319c') | sort - started_at, trace_id | head 51",
        r"source=spans | where trace_id = '0af7651916cd43dd8448eb211c80319c' | eval is_err = if(status_code = 2, 1, 0) | stats min(`timestamp`) as started_at, max(end_timestamp) as ended_at, count() as span_count, sum(is_err) as error_count by trace_id",
        r"source=spans | where trace_id = '0af7651916cd43dd8448eb211c80319c' and span_id = 'b7ad6b7169203331' | sort `timestamp` | head 10001 | fields `timestamp`, span_id, parent_span_id, name, service_name, duration_micros",
        r"source=operations | where trace_id = '0af7651916cd43dd8448eb211c80319c' | fields - input_messages, output_messages, system_instructions, tool_definitions, output_content | left join max=0 left=o right=p on o.request_model = p.model and o.provider = p.provider and o.`timestamp` >= p.valid_from and (isnull(p.valid_to) or o.`timestamp` < p.valid_to) and o.input_tokens >= p.min_input_tokens [ source=prices_effective | where service_tier = 'standard' and region = 'global' | fields model, provider, valid_from, valid_to, min_input_tokens, input_usd_per_1m, output_usd_per_1m ] | sort - valid_from, - min_input_tokens | dedup trace_id, span_id | eval input_cost = input_tokens * input_usd_per_1m / 1000000.0D | sort `timestamp`, span_id | head 10001",
        r"source=operations | where trace_id in ('0af7651916cd43dd8448eb211c80319c', '1bf7651916cd43dd8448eb211c80319c') and `timestamp` >= timestamp('2026-10-08 00:00:00.000000') | sort `timestamp`, span_id | head 1001",
        r"source=spans | where `timestamp` >= timestamp('2026-10-08 00:00:00.000000') | stats values(case(kind = 2, service_name)) as v0, values(name) as v1",
        r"source=operations | where `timestamp` >= timestamp('2026-10-08 00:00:00.000000') | where trace_id in [ source=operations | where isnotnull(conversation_id) | fields trace_id ] | join left=o right=ct on o.trace_id = ct.ct_trace_id and (isnull(o.conversation_id) or o.conversation_id = ct.ct_conversation_id) [ source=operations | where isnotnull(conversation_id) | rename trace_id as ct_trace_id, conversation_id as ct_conversation_id | fields ct_trace_id, ct_conversation_id ] | stats min(`timestamp`) as started_at, values(request_model) as models, dc(trace_id) as trace_count by ct_conversation_id | where trace_count > 0 | sort - started_at, ct_conversation_id | head 51 from 50",
        r"source=operations | where isnotnull(conversation_id) | stats dc(trace_id) as trace_count by conversation_id | where trace_count > 0 | stats count() as conversations, sum(trace_count) as traces | eval traces = coalesce(traces, 0)",
        r"source=operations | where conversation_id = 'conv-1' | stats min(`timestamp`) as lo, max(end_timestamp) as hi by trace_id | sort lo | head 1001",
        r"source=spans | where `timestamp` >= timestamp('2026-10-08 00:00:00.000000') and `timestamp` < timestamp('2026-10-09 00:00:00.000000') | stats count() as events, count(eval(status_code = 2)) as errored",
        r"source=spans | eval span_kind = case(kind = 2, 'server', kind = 3, 'client' else 'internal'), dur_ms = cast(duration_micros as double) / 1000.0D | stats percentile(dur_ms, 95) as p95, percentile(dur_ms, 57) as p57 by span_kind",
        r"source=operations | eval cost = if(isnull(request_model), 0.0, input_tokens * 0.000003) | stats sum(cost) as cost_usd, count(eval(isnull(request_model))) as unpriced | eval cost_usd = coalesce(cost_usd, 0.0)",
        r"source=operations | eval off = timestampdiff(MICROSECOND, `timestamp`, timestamp('2026-10-09 00:00:00.000000')), idx = (off - 1) / 3600000000 | stats sum(input_tokens) as input_tokens by idx, request_model",
        r"source=operations | where operation_name in ('chat', 'text_completion') | eval ms = duration_micros / 1000 | fields ms | head 5000",
        r"source=marks | where scanned_at >= timestamp('2026-10-08 00:00:00.000000') | sort - scanned_at, - scan_id, - attempt | dedup mark_id | where not retracted | eval matches = if(rule_id = 'r1', true, false) | stats bucket_nullable=true count() as n by issue_fp, issue_level | where n > 0 | sort - n, issue_level, issue_fp | head 20001",
        r"source=marks | sort - scanned_at, - scan_id | streamstats count() as rn by mark_id | where rn = 1",
        r"source=marks | inner join left=m right=k on m.mark_id = k.mark_id [ source=marks | fields mark_id ] | eventstats bucket_nullable=true min(occurred_at) as first_seen, max(occurred_at) as last_seen by issue_fp | stats bucket_nullable=true sum(impact_cost_usd) as impact, count(eval(occurred_at >= timestamp('2026-10-08 00:00:00.000000'))) as bucket_0 by issue_fp",
        r"source=verdicts | where verdict_kind = 'harm' and computed_at >= timestamp('2026-10-08 00:00:00.000000') | eventstats max(computed_at) as newest by subject_key, class_key | where computed_at = newest",
        r"source=marks | stats count() as n, dc(issue_fp) as distinct_issues by span(occurred_at, 1d), rule_id, fp_version, fp_epoch",
        r"source=marks | stats min(from_fp) as old_fp, max(from_fp) as max_fp, min(localization_kind) as localization by to_fp",
        r"source=marks | stats count() as n, max(occurred_at) as last_seen by trace_id | where last_seen < timestamp('2026-10-08 12:00:00.000000') or (last_seen = timestamp('2026-10-08 12:00:00.000000') and trace_id > '0af7651916cd43dd8448eb211c80319c') | sort - last_seen, trace_id | head 51",
        r"source=prices_effective | head 1",
        r"source=prices_effective | sort - valid_from, - min_input_tokens | head 1",
        r"source=metrics | fields metric_name, data_point_attributes, exemplars",
        r"source=spans | where locate('timeout', status_message) > 0",
        r"source=operations | where exists(finish_reasons, x -> x = 'stop') and array_length(finish_reasons) > 0",
        r"source=operations | where false",
        r"source=operations | where 1 = 2",
        r"source=spans | eval host = coalesce(span_attributes.`host.name`, scope_attributes.`host.name`, resource_attributes.`host.name`) | stats count(eval(host = 'node-a')) as _f0 by trace_id | where _f0 > 0",
        r"source=operations | eval duration_ms = timestampdiff(MICROSECOND, `timestamp`, end_timestamp) / 1000 | where duration_ms > 250",
        r"source=operations | eval reasons = mvjoin(finish_reasons, ',') | fields reasons",
    ];
    for form in forms {
        let outcome = parse_ppl_root(form);
        assert!(outcome.is_ok(), "form {form:?}: {:?}", outcome.err());
    }
}

#[test]
fn parsing_stops_at_the_first_error_and_reports_the_offending_token() {
    // Both `where` commands lack an expression; the error names the first `=` (0-based column 20).
    // The second `=`, at column 32, is never reached.
    let error = parse_ppl_root("source=logs | where = 1 | where = 2").expect_err("syntax error");
    assert_eq!(
        (error.line, error.column, error.offending_text.as_deref()),
        (1, 20, Some("="))
    );
    let error = parse_ppl_root("where severity_text = 'ERROR'").expect_err("no source");
    assert_eq!(
        (error.line, error.column, error.offending_text.as_deref()),
        (1, 29, Some("<EOF>"))
    );
}

#[test]
fn a_non_whitespace_error_recognition_token_is_a_syntax_error() {
    let error = parse_ppl_root("source=logs | where x\u{E9} = 1").expect_err("é is no token");
    assert_eq!(
        (error.line, error.column, error.offending_text.as_deref()),
        (1, 21, Some("\u{E9}"))
    );
    // `#` is no token either, and it comes before the parser error at `2` (column 31), so the
    // character error is the one reported.
    let error = parse_ppl_root("source=logs | where body = 1 # 2").expect_err("# is no token");
    assert_eq!(
        (error.line, error.column, error.offending_text.as_deref()),
        (1, 29, Some("#"))
    );
}

#[test]
fn a_parser_error_before_an_unrecognized_character_fetched_by_look_ahead_is_reported() {
    // After the field `a`, `.` may continue the name `a.b` (`qualifiedName` is
    // `ident (DOT ident)*`) or start a second field `.b` (`fieldList` allows a missing comma and
    // `ident` is `(DOT)? ID`), so the loop of `qualifiedName` predicts on two tokens: `.` and the
    // next token on the default channel, `1`. Reading `1` fetches the `é` (column 24) before it.
    // No alternative accepts `1`, so prediction takes the exit alternative, which has finished the
    // decision's rule; nothing after the field `a` accepts `.`, so the parser error is at `.`
    // (column 22), before the fetched `é`.
    let query = "source=logs | where a . \u{E9} 1";
    let error = parse_ppl_root(query).expect_err("`.` ends no expression");
    assert_eq!(
        (error.line, error.column, error.offending_text.as_deref()),
        (1, 22, Some("."))
    );

    // The same parse, inspected: the `é` was fetched and is a character error of its own, so the
    // reported parser error won the ordering rather than being the only error.
    let mut parser = build_ppl_parser(query);
    assert!(parser.root().is_err(), "the parse fails");
    let character_error = find_first_unrecognized_character(&parser.input).expect("`é` was fetched");
    assert_eq!(
        (
            character_error.line,
            character_error.column,
            character_error.offending_text.as_deref()
        ),
        (1, 24, Some("\u{E9}"))
    );
}

#[test]
fn the_case_wrapper_upper_cases_look_ahead_per_code_point_and_keeps_token_text() {
    // ı (U+0131) → I, ſ (U+017F) → S, ß (no single-code-point upper case) stays, ᾀ (U+1F80) →
    // ᾈ (U+1F88, its simple mapping), a → A.
    let text = "\u{131}\u{17F}\u{DF}\u{1F80}a";
    let mut stream = UpperCaseCharStream::new(text);
    for expected_code_point in ['I', 'S', '\u{DF}', '\u{1F88}', 'A'] {
        assert_eq!(stream.la(1), expected_code_point as i32);
        stream.consume();
    }
    assert_eq!(stream.la(1), EOF);
    let last_start = isize::try_from(text.len() - 1).expect("short text");
    assert_eq!(stream.get_text(0, last_start), text);

    let mut lexer = OpenSearchPPLLexer::new(UpperCaseCharStream::new("\u{17F}ource=Logs"));
    let source_token = lexer.next_token();
    assert_eq!(source_token.get_token_type(), opensearchppllexer::SOURCE);
    assert_eq!(source_token.get_text(), "\u{17F}ource");
    let equal_token = lexer.next_token();
    assert_eq!(equal_token.get_token_type(), opensearchppllexer::EQUAL);
    let index_name_token = lexer.next_token();
    assert_eq!(index_name_token.get_token_type(), opensearchppllexer::ID);
    assert_eq!(index_name_token.get_text(), "Logs");
}
