//! The Lucene regular-expression dialect, translated to Rust `regex` syntax.
//!
//! Shared by `query_string` `/re/` terms and the `regexp` leaf. The translation is anchored over
//! the whole input, and `&` intersection, `~` complement and named automata are refused, never
//! approximated: an approximation would answer rows the pattern does not describe.

use std::fmt::Write as _;

use regex::Regex;

use super::udf::{compile_anchored_pattern, push_regex_literal};
use crate::opensearch::error::OpenSearchError;

/// The longest pattern accepted, in characters.
const MAX_REGEX_LENGTH: usize = 1000;

/// The deepest nesting of groups accepted. The translator recurses once per group, so the bound
/// keeps a pattern within [`MAX_REGEX_LENGTH`] from exhausting the stack of the thread that
/// translates it.
const MAX_REGEX_GROUP_DEPTH: usize = 128;

/// The optional operators of the dialect a request enables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RegexFlags(u8);

impl RegexFlags {
    const INTERSECTION: u8 = 1;
    const COMPLEMENT: u8 = 1 << 1;
    const EMPTY: u8 = 1 << 2;
    const ANYSTRING: u8 = 1 << 3;
    const INTERVAL: u8 = 1 << 4;
    /// Set only by `ALL`, together with `INTERVAL`: `<name>` then reads as a named automaton,
    /// which is refused.
    const AUTOMATON: u8 = 1 << 5;
    /// Every operator: the default.
    pub(crate) const ALL: Self =
        Self(Self::INTERSECTION | Self::COMPLEMENT | Self::EMPTY | Self::ANYSTRING | Self::INTERVAL | Self::AUTOMATON);

    /// Reads a `flags` value: `|`-separated names, any case; absent or empty is `ALL`.
    ///
    /// # Errors
    ///
    /// `illegal_argument` (400) for a name that is no flag.
    pub(crate) fn parse(text: Option<&str>) -> Result<Self, OpenSearchError> {
        let Some(text) = text.filter(|text| !text.is_empty()) else {
            return Ok(Self::ALL);
        };
        let mut bits = 0;
        for name in text.split('|') {
            bits |= match name.trim().to_ascii_uppercase().as_str() {
                "ALL" => Self::ALL.0,
                "NONE" => 0,
                "INTERSECTION" => Self::INTERSECTION,
                "COMPLEMENT" => Self::COMPLEMENT,
                "EMPTY" => Self::EMPTY,
                "ANYSTRING" => Self::ANYSTRING,
                "INTERVAL" => Self::INTERVAL,
                _ => {
                    return Err(OpenSearchError::illegal_argument(format!(
                        "unknown regexp flag [{name}]"
                    )));
                }
            };
        }
        Ok(Self(bits))
    }

    const fn has_flag(self, flag: u8) -> bool {
        self.0 & flag != 0
    }
}

/// Translates a Lucene `RegExp` pattern into a Rust regex anchored over the whole input.
///
/// Only the whole empty pattern is the empty string; every other alternative holds at least one
/// element, so a leading `|`, `)` or `&` is a literal character.
///
/// # Errors
///
/// - a 400 `parsing_exception` whose `error.construct` names an enabled `&`, an enabled `~` or a
///   named automaton (`<name>`);
/// - a 400 `parsing_exception` whose `error.limit` is [`MAX_REGEX_LENGTH`] for a longer pattern,
///   or [`MAX_REGEX_GROUP_DEPTH`] for groups nested deeper;
/// - a 400 `illegal_argument_exception` for a syntax error, and for a translation that does not
///   compile.
pub(crate) fn translate_lucene_regex(
    pattern: &str,
    flags: RegexFlags,
    case_insensitive: bool,
) -> Result<Regex, OpenSearchError> {
    if pattern.chars().count() > MAX_REGEX_LENGTH {
        return Err(OpenSearchError::query_limit_exceeded(
            format!("the regular expression is longer than {MAX_REGEX_LENGTH} characters"),
            MAX_REGEX_LENGTH,
        ));
    }
    if pattern.is_empty() {
        return compile_anchored_pattern(pattern, "^(?s:)$");
    }
    let mut translator = RegexTranslator {
        characters: pattern.chars().collect(),
        position: 0,
        group_depth: 0,
        flags,
        is_ascii_case_insensitive: case_insensitive,
    };
    let body = translator.parse_union()?;
    if let Some(unexpected_character) = translator.peek_character() {
        return Err(translator.build_syntax_error(&format!("unexpected [{unexpected_character}]")));
    }
    compile_anchored_pattern(pattern, &format!("^(?s:{body})$"))
}

/// A recursive-descent reading of the Lucene `RegExp` grammar that writes the equivalent Rust
/// syntax: `unionexp`, `interexp`, `concatexp`, `repeatexp`, `complexp`, `charclassexp`,
/// `simpleexp` and `charexp`, in that order.
struct RegexTranslator {
    characters: Vec<char>,
    position: usize,
    /// The groups open at `position`.
    group_depth: usize,
    flags: RegexFlags,
    is_ascii_case_insensitive: bool,
}

impl RegexTranslator {
    fn peek_character(&self) -> Option<char> {
        self.characters.get(self.position).copied()
    }

    fn consume_character_if(&mut self, expected: char) -> bool {
        let is_next = self.peek_character() == Some(expected);
        if is_next {
            self.position += 1;
        }
        is_next
    }

    fn read_character(&mut self) -> Result<char, OpenSearchError> {
        let character = self
            .peek_character()
            .ok_or_else(|| self.build_syntax_error("unexpected end of pattern"))?;
        self.position += 1;
        Ok(character)
    }

    fn build_syntax_error(&self, reason: &str) -> OpenSearchError {
        OpenSearchError::illegal_argument(format!(
            "invalid regular expression at position {}: {reason}",
            self.position
        ))
    }

    fn parse_union(&mut self) -> Result<String, OpenSearchError> {
        let mut alternatives = vec![self.parse_intersection()?];
        while self.consume_character_if('|') {
            alternatives.push(self.parse_intersection()?);
        }
        Ok(if alternatives.len() == 1 {
            alternatives.pop().unwrap_or_default()
        } else {
            format!("(?:{})", alternatives.join("|"))
        })
    }

    fn parse_intersection(&mut self) -> Result<String, OpenSearchError> {
        let concatenation = self.parse_concatenation()?;
        if self.flags.has_flag(RegexFlags::INTERSECTION) && self.peek_character() == Some('&') {
            return Err(OpenSearchError::unsupported_construct(
                "&",
                "the regular expression intersection operator [&] is not supported",
            ));
        }
        Ok(concatenation)
    }

    /// One element or more: the first is read whatever it starts with, so a leading `)`, `|` or
    /// `&` is a literal character there.
    fn parse_concatenation(&mut self) -> Result<String, OpenSearchError> {
        let mut output = self.parse_repetition()?;
        while let Some(character) = self.peek_character() {
            if matches!(character, ')' | '|') || (character == '&' && self.flags.has_flag(RegexFlags::INTERSECTION)) {
                break;
            }
            output.push_str(&self.parse_repetition()?);
        }
        Ok(output)
    }

    fn parse_repetition(&mut self) -> Result<String, OpenSearchError> {
        let mut atom = self.parse_complement()?;
        while let Some(quantifier) = self
            .peek_character()
            .filter(|character| matches!(character, '?' | '*' | '+' | '{'))
        {
            self.position += 1;
            let suffix = if quantifier == '{' {
                self.parse_bounded_repetition()?
            } else {
                quantifier.to_string()
            };
            atom = format!("(?:{atom}){suffix}");
        }
        Ok(atom)
    }

    fn parse_bounded_repetition(&mut self) -> Result<String, OpenSearchError> {
        let minimum = self.parse_integer()?;
        let suffix = if self.consume_character_if(',') {
            if self.peek_character() == Some('}') {
                format!("{{{minimum},}}")
            } else {
                let maximum = self.parse_integer()?;
                if maximum < minimum {
                    return Err(self.build_syntax_error("the repetition maximum is below its minimum"));
                }
                format!("{{{minimum},{maximum}}}")
            }
        } else {
            format!("{{{minimum}}}")
        };
        if !self.consume_character_if('}') {
            return Err(self.build_syntax_error("expected '}'"));
        }
        Ok(suffix)
    }

    fn parse_integer(&mut self) -> Result<u32, OpenSearchError> {
        let start = self.position;
        while self.peek_character().is_some_and(|character| character.is_ascii_digit()) {
            self.position += 1;
        }
        let digits: String = self.characters[start..self.position].iter().collect();
        digits.parse().map_err(|_| self.build_syntax_error("integer expected"))
    }

    fn parse_complement(&mut self) -> Result<String, OpenSearchError> {
        if self.flags.has_flag(RegexFlags::COMPLEMENT) && self.peek_character() == Some('~') {
            return Err(OpenSearchError::unsupported_construct(
                "~",
                "the regular expression complement operator [~] is not supported",
            ));
        }
        self.parse_character_class()
    }

    fn parse_character_class(&mut self) -> Result<String, OpenSearchError> {
        if !self.consume_character_if('[') {
            return self.parse_simple_expression();
        }
        let mut output = String::from(if self.consume_character_if('^') { "[^" } else { "[" });
        loop {
            self.parse_class_item(&mut output)?;
            if self.peek_character().is_none_or(|character| character == ']') {
                break;
            }
        }
        if !self.consume_character_if(']') {
            return Err(self.build_syntax_error("expected ']'"));
        }
        output.push(']');
        Ok(output)
    }

    fn parse_class_item(&mut self, output: &mut String) -> Result<(), OpenSearchError> {
        if let Some(predefined_class) = self.parse_predefined_class()? {
            output.push_str(predefined_class);
            return Ok(());
        }
        let range_start = self.read_literal_character()?;
        if self.consume_character_if('-') {
            let range_end = self.read_literal_character()?;
            if range_start > range_end {
                return Err(self.build_syntax_error(&format!("invalid range [{range_start}-{range_end}]")));
            }
            push_class_character(output, range_start);
            output.push('-');
            push_class_character(output, range_end);
        } else {
            push_class_character(output, range_start);
            if self.is_ascii_case_insensitive && range_start.is_ascii_alphabetic() {
                let other_case = if range_start.is_ascii_lowercase() {
                    range_start.to_ascii_uppercase()
                } else {
                    range_start.to_ascii_lowercase()
                };
                push_class_character(output, other_case);
            }
        }
        Ok(())
    }

    /// `\d`, `\s`, `\w` and their negations; any other ASCII letter after `\` is an error.
    fn parse_predefined_class(&mut self) -> Result<Option<&'static str>, OpenSearchError> {
        let (Some('\\'), Some(letter)) = (self.peek_character(), self.characters.get(self.position + 1).copied())
        else {
            return Ok(None);
        };
        if !letter.is_ascii_alphabetic() {
            return Ok(None);
        }
        let class = match letter {
            'd' => "[0-9]",
            'D' => "[^0-9]",
            's' => "[\\t\\n\\r ]",
            'S' => "[^\\t\\n\\r ]",
            'w' => "[0-9A-Z_a-z]",
            'W' => "[^0-9A-Z_a-z]",
            _ => return Err(self.build_syntax_error(&format!("invalid character class [\\{letter}]"))),
        };
        self.position += 2;
        Ok(Some(class))
    }

    /// One character; `\x` is `x` for any `x` that is not an ASCII letter.
    fn read_literal_character(&mut self) -> Result<char, OpenSearchError> {
        self.consume_character_if('\\');
        self.read_character()
    }

    fn parse_simple_expression(&mut self) -> Result<String, OpenSearchError> {
        if let Some(predefined_class) = self.parse_predefined_class()? {
            return Ok(predefined_class.to_owned());
        }
        let Some(character) = self.peek_character() else {
            return Err(self.build_syntax_error("unexpected end of pattern"));
        };
        match character {
            '.' => {
                self.position += 1;
                Ok(String::from("."))
            }
            '#' if self.flags.has_flag(RegexFlags::EMPTY) => {
                self.position += 1;
                Ok(String::from("[^\\x{0}-\\x{10FFFF}]"))
            }
            '@' if self.flags.has_flag(RegexFlags::ANYSTRING) => {
                self.position += 1;
                Ok(String::from(".*"))
            }
            '"' => {
                self.position += 1;
                let mut output = String::new();
                loop {
                    let quoted_character = self.read_character()?;
                    if quoted_character == '"' {
                        break;
                    }
                    push_regex_literal(&mut output, quoted_character, self.is_ascii_case_insensitive);
                }
                Ok(format!("(?:{output})"))
            }
            '(' => {
                self.position += 1;
                if self.consume_character_if(')') {
                    return Ok(String::from("(?:)"));
                }
                if self.group_depth == MAX_REGEX_GROUP_DEPTH {
                    return Err(OpenSearchError::query_limit_exceeded(
                        format!("the regular expression nests groups deeper than {MAX_REGEX_GROUP_DEPTH}"),
                        MAX_REGEX_GROUP_DEPTH,
                    ));
                }
                self.group_depth += 1;
                let group_body = self.parse_union()?;
                self.group_depth -= 1;
                if !self.consume_character_if(')') {
                    return Err(self.build_syntax_error("expected ')'"));
                }
                Ok(format!("(?:{group_body})"))
            }
            '<' if self.flags.has_flag(RegexFlags::INTERVAL) => {
                self.position += 1;
                self.parse_interval()
            }
            _ => {
                let literal = self.read_literal_character()?;
                let mut output = String::new();
                push_regex_literal(&mut output, literal, self.is_ascii_case_insensitive);
                Ok(output)
            }
        }
    }

    /// `<n-m>`: the decimal integers from `n` to `m`, zero-padded to their common width when
    /// both are written with the same number of digits, otherwise with any number of leading
    /// zeros; `<name>` (a named automaton) is refused. Reached only with `INTERVAL` enabled,
    /// since `AUTOMATON` is never set without it.
    fn parse_interval(&mut self) -> Result<String, OpenSearchError> {
        let start = self.position;
        while self.peek_character().is_some_and(|character| character != '>') {
            self.position += 1;
        }
        let content: String = self.characters[start..self.position].iter().collect();
        if !self.consume_character_if('>') {
            return Err(self.build_syntax_error("expected '>'"));
        }
        let Some((low_text, high_text)) = content.split_once('-') else {
            if self.flags.has_flag(RegexFlags::AUTOMATON) {
                return Err(OpenSearchError::unsupported_construct(
                    &format!("<{content}>"),
                    format!("named automata such as [<{content}>] are not supported"),
                ));
            }
            return Err(self.build_syntax_error(&format!("invalid interval [<{content}>]")));
        };
        let parse_bound = |text: &str| text.parse::<i32>().ok().and_then(|value| u64::try_from(value).ok());
        let (Some(low_bound), Some(high_bound)) = (parse_bound(low_text), parse_bound(high_text)) else {
            return Err(self.build_syntax_error(&format!("invalid interval [<{content}>]")));
        };
        let digits = if low_text.len() == high_text.len() {
            low_text.len()
        } else {
            0
        };
        Ok(build_numeric_interval(
            low_bound.min(high_bound),
            low_bound.max(high_bound),
            digits,
        ))
    }
}

/// Writes a class character; every one that is not ASCII alphanumeric as `\x{HEX}`, so no
/// character can open a nested class or a set operation in Rust syntax.
fn push_class_character(output: &mut String, character: char) {
    if character.is_ascii_alphanumeric() {
        output.push(character);
    } else {
        // Writing to a `String` cannot fail.
        let _ = write!(output, "\\x{{{:X}}}", u32::from(character));
    }
}

/// A pattern for the decimal integers from `low_bound` to `high_bound`: of exactly `digits` digits
/// when `digits` is not zero, otherwise with any number of leading zeros.
fn build_numeric_interval(low_bound: u64, high_bound: u64, digits: usize) -> String {
    if digits > 0 {
        return build_digit_range(
            format!("{low_bound:0digits$}").as_bytes(),
            format!("{high_bound:0digits$}").as_bytes(),
        );
    }
    let low_text = low_bound.to_string();
    let high_text = high_bound.to_string();
    let mut alternatives = Vec::with_capacity(high_text.len() - low_text.len() + 1);
    for length in low_text.len()..=high_text.len() {
        let length_low_bound = if length == low_text.len() {
            low_text.clone()
        } else {
            format!("1{}", "0".repeat(length - 1))
        };
        let length_high_bound = if length == high_text.len() {
            high_text.clone()
        } else {
            "9".repeat(length)
        };
        alternatives.push(build_digit_range(
            length_low_bound.as_bytes(),
            length_high_bound.as_bytes(),
        ));
    }
    format!("0*(?:{})", alternatives.join("|"))
}

/// A pattern for the equal-width digit strings from `low_bound` to `high_bound`: shared leading
/// digits, then at most three alternatives (the rest of the low first digit, the full middle first
/// digits, the rest of the high first digit).
fn build_digit_range(low_bound: &[u8], high_bound: &[u8]) -> String {
    let (Some((&low_first_digit, low_rest)), Some((&high_first_digit, high_rest))) =
        (low_bound.split_first(), high_bound.split_first())
    else {
        return String::new();
    };
    if low_bound == high_bound {
        return String::from_utf8_lossy(low_bound).into_owned();
    }
    if low_first_digit == high_first_digit {
        return format!(
            "{}{}",
            char::from(low_first_digit),
            build_digit_range(low_rest, high_rest)
        );
    }
    let mut alternatives = Vec::with_capacity(3);
    let mut full_low_digit = low_first_digit;
    if low_rest.iter().any(|digit| *digit != b'0') {
        alternatives.push(format!(
            "{}{}",
            char::from(low_first_digit),
            build_digit_range(low_rest, &vec![b'9'; low_rest.len()])
        ));
        full_low_digit += 1;
    }
    let is_high_rest_all_nines = high_rest.iter().all(|digit| *digit == b'9');
    // The first digits differ and `low_bound <= high_bound`, so `high_first_digit` is above
    // `low_first_digit`, never `0`.
    let full_high_digit = if is_high_rest_all_nines {
        high_first_digit
    } else {
        high_first_digit - 1
    };
    if full_low_digit <= full_high_digit {
        let first_digits = if full_low_digit == full_high_digit {
            char::from(full_low_digit).to_string()
        } else {
            format!("[{}-{}]", char::from(full_low_digit), char::from(full_high_digit))
        };
        let rest_digits = match low_rest.len() {
            0 => String::new(),
            1 => String::from("[0-9]"),
            width => format!("[0-9]{{{width}}}"),
        };
        alternatives.push(format!("{first_digits}{rest_digits}"));
    }
    if !is_high_rest_all_nines {
        alternatives.push(format!(
            "{}{}",
            char::from(high_first_digit),
            build_digit_range(&vec![b'0'; high_rest.len()], high_rest)
        ));
    }
    if alternatives.len() == 1 {
        alternatives.pop().unwrap_or_default()
    } else {
        format!("(?:{})", alternatives.join("|"))
    }
}

#[cfg(test)]
mod tests {
    //! The translation table is a bounded case matrix: one row per production of the grammar and
    //! per flag, each with its refusal branch. The fixed-seed literal test samples arbitrary
    //! characters through the escaping rule, the one rule every pattern relies on.

    use axum::http::StatusCode;
    use rand::{Rng as _, SeedableRng as _, rngs::StdRng};

    use super::{RegexFlags, translate_lucene_regex};
    use crate::opensearch::error::render_envelope;

    fn translate_pattern(pattern: &str) -> String {
        translate_lucene_regex(pattern, RegexFlags::ALL, false)
            .expect("the pattern translates")
            .as_str()
            .to_owned()
    }

    #[test]
    fn lucene_regex_translates_to_the_stated_anchored_rust_patterns() {
        let rows = [
            ("", "^(?s:)$"),
            ("ERR.*", "^(?s:ERR(?:.)*)$"),
            ("a?b*c+", "^(?s:(?:a)?(?:b)*(?:c)+)$"),
            ("a@", "^(?s:a.*)$"),
            ("a#", "^(?s:a[^\\x{0}-\\x{10FFFF}])$"),
            ("<1-100>", "^(?s:0*(?:[1-9]|[1-9][0-9]|100))$"),
            ("<01-10>", "^(?s:(?:0[1-9]|10))$"),
            ("<0-5>", "^(?s:[0-5])$"),
            ("<10-1>", "^(?s:0*(?:[1-9]|10))$"),
            (
                "<123-456>",
                "^(?s:(?:1(?:2[3-9]|[3-9][0-9])|[2-3][0-9]{2}|4(?:[0-4][0-9]|5[0-6])))$",
            ),
            ("\"a.b\"", "^(?s:(?:a\\.b))$"),
            ("a.b", "^(?s:a.b)$"),
            ("[a-c\\]x]", "^(?s:[a-c\\x{5D}x])$"),
            ("[^\\d]", "^(?s:[^[0-9]])$"),
            ("[é\\-]", "^(?s:[\\x{E9}\\x{2D}])$"),
            ("\\w+", "^(?s:(?:[0-9A-Z_a-z])+)$"),
            ("\\s\\S\\D\\W", "^(?s:[\\t\\n\\r ][^\\t\\n\\r ][^0-9][^0-9A-Z_a-z])$"),
            ("a{2}", "^(?s:(?:a){2})$"),
            ("a{2,3}", "^(?s:(?:a){2,3})$"),
            ("a{2,}", "^(?s:(?:a){2,})$"),
            ("a|b", "^(?s:(?:a|b))$"),
            ("()", "^(?s:(?:))$"),
            ("(a)", "^(?s:(?:a))$"),
            ("x\\.y\\$", "^(?s:x\\.y\\$)$"),
            // A leading `|`, `)` or `&` starts no operator: it is a literal character.
            ("|a", "^(?s:\\|a)$"),
            ("(|a)", "^(?s:(?:\\|a))$"),
            ("&a", "^(?s:\\&a)$"),
        ];
        for (pattern, expected) in rows {
            assert_eq!(translate_pattern(pattern), expected, "[{pattern}]");
        }
        let no_flags = RegexFlags::parse(Some("NONE")).expect("flags parse");
        assert_eq!(
            translate_lucene_regex("a@#<1-2>&~", no_flags, false)
                .expect("all literal")
                .as_str(),
            "^(?s:a@\\#<1\\-2>\\&\\~)$"
        );
        let case_folded_regex = translate_lucene_regex("Err[x]", RegexFlags::ALL, true).expect("translates");
        assert_eq!(case_folded_regex.as_str(), "^(?s:[eE][rR][rR][xX])$");
        assert!(case_folded_regex.is_match("ERRX"));
        // Quoted letters fold; a class range does not.
        assert_eq!(
            translate_lucene_regex("\"Ab\"[A-C]", RegexFlags::ALL, true)
                .expect("translates")
                .as_str(),
            "^(?s:(?:[aA][bB])[A-C])$"
        );
        let empty_language_regex = translate_lucene_regex("#", RegexFlags::ALL, false).expect("translates");
        assert!(!empty_language_regex.is_match(""));
        assert!(!empty_language_regex.is_match("#"));
    }

    #[test]
    fn numeric_intervals_match_exactly_their_range() {
        let same_width_interval = translate_lucene_regex("<123-456>", RegexFlags::ALL, false).expect("translates");
        let padded_interval = translate_lucene_regex("<001-120>", RegexFlags::ALL, false).expect("translates");
        let unpadded_interval = translate_lucene_regex("<5-120>", RegexFlags::ALL, false).expect("translates");
        for value in 0_u32..1000 {
            assert_eq!(
                same_width_interval.is_match(&value.to_string()),
                (123..=456).contains(&value),
                "{value}"
            );
            assert_eq!(
                padded_interval.is_match(&format!("{value:03}")),
                (1..=120).contains(&value),
                "{value:03}"
            );
            assert_eq!(
                unpadded_interval.is_match(&value.to_string()),
                (5..=120).contains(&value),
                "{value}"
            );
            assert_eq!(
                unpadded_interval.is_match(&format!("00{value}")),
                (5..=120).contains(&value),
                "00{value}"
            );
        }
        // Fixed width: `5` is not three digits.
        assert!(!padded_interval.is_match("5"));
    }

    #[tokio::test]
    async fn enabled_intersection_or_complement_is_refused_naming_the_operator() {
        for (pattern, operator) in [("a&b", "&"), ("~a", "~"), ("a~b", "~")] {
            let error = translate_lucene_regex(pattern, RegexFlags::ALL, false).expect_err("refused");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "[{pattern}]");
            assert_eq!(body["error"]["type"], "parsing_exception", "[{pattern}]");
            assert_eq!(body["error"]["construct"], operator, "[{pattern}]");
        }
        let complement_only = RegexFlags::parse(Some("complement")).expect("flags parse");
        assert_eq!(
            translate_lucene_regex("a&b", complement_only, false)
                .expect("& is literal")
                .as_str(),
            "^(?s:a\\&b)$"
        );
        let intersection_only = RegexFlags::parse(Some("Intersection")).expect("flags parse");
        assert_eq!(
            translate_lucene_regex("~a", intersection_only, false)
                .expect("~ is literal")
                .as_str(),
            "^(?s:\\~a)$"
        );
    }

    #[tokio::test]
    async fn malformed_patterns_and_unknown_flags_are_refused() {
        let interval_only = RegexFlags::parse(Some("INTERVAL")).expect("flags parse");
        // Within the length and group limits, but each `*` wraps the atom in one more group, so
        // the translation nests 300 groups and does not compile.
        let uncompilable_pattern = format!("a{}", "*".repeat(300));
        let malformed_rows = [
            ("<1-2-3>", RegexFlags::ALL),
            ("<1->", RegexFlags::ALL),
            ("<a-b>", RegexFlags::ALL),
            ("<99999999999-1>", RegexFlags::ALL),
            ("<1-2", RegexFlags::ALL),
            // Without named automata, `<abc>` is a malformed interval.
            ("<abc>", interval_only),
            ("\\q", RegexFlags::ALL),
            ("[\\q]", RegexFlags::ALL),
            ("a{3,2}", RegexFlags::ALL),
            ("a{x}", RegexFlags::ALL),
            ("a{2", RegexFlags::ALL),
            ("[z-a]", RegexFlags::ALL),
            ("[a", RegexFlags::ALL),
            ("(a", RegexFlags::ALL),
            ("\"a", RegexFlags::ALL),
            ("a)", RegexFlags::ALL),
            ("a|", RegexFlags::ALL),
            ("(a|)", RegexFlags::ALL),
            ("a\\", RegexFlags::ALL),
            (uncompilable_pattern.as_str(), RegexFlags::ALL),
        ];
        for (pattern, flags) in malformed_rows {
            let error = translate_lucene_regex(pattern, flags, false).expect_err("refused");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "[{pattern}]");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "[{pattern}]");
        }
        let error = translate_lucene_regex("<abc>", RegexFlags::ALL, false).expect_err("refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
        assert_eq!(body["error"]["construct"], "<abc>");
        let error = RegexFlags::parse(Some("INTERSECTION|NOPE")).expect_err("refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(RegexFlags::parse(None).expect("default"), RegexFlags::ALL);
        assert_eq!(RegexFlags::parse(Some("")).expect("default"), RegexFlags::ALL);
        assert_eq!(
            RegexFlags::parse(Some("none|ALL")).expect("flags parse"),
            RegexFlags::ALL
        );
    }

    #[tokio::test]
    async fn a_pattern_over_1000_characters_is_refused() {
        assert_eq!(
            translate_pattern(&"a".repeat(999)),
            format!("^(?s:{})$", "a".repeat(999))
        );
        assert_eq!(
            translate_pattern(&"a".repeat(1000)),
            format!("^(?s:{})$", "a".repeat(1000))
        );
        // The limit counts characters, not bytes: 1000 two-byte characters are accepted.
        assert!(
            translate_lucene_regex(&"é".repeat(1000), RegexFlags::ALL, false)
                .is_ok_and(|regex| regex.is_match(&"é".repeat(1000)))
        );
        let error = translate_lucene_regex(&"a".repeat(1001), RegexFlags::ALL, false).expect_err("refused");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "parsing_exception");
        assert_eq!(body["error"]["limit"], 1000);
    }

    /// 128 nested groups translate and match; one more, and the deepest nesting the length
    /// limit allows (499 groups, unclosed or closed), are refused naming the limit before the
    /// translator recurses into them.
    #[tokio::test]
    async fn groups_nest_up_to_128_levels_and_deeper_is_refused_naming_the_limit() {
        for depth in [127, 128] {
            let pattern = format!("{}a{}", "(".repeat(depth), ")".repeat(depth));
            let regex = translate_lucene_regex(&pattern, RegexFlags::ALL, false).expect("translates");
            assert!(regex.is_match("a"), "{depth} levels");
            assert!(!regex.is_match("aa"), "{depth} levels");
        }
        // `()` opens no group the translator recurses into, at any depth.
        let pattern = format!("{}(){}", "(".repeat(128), ")".repeat(128));
        assert!(translate_lucene_regex(&pattern, RegexFlags::ALL, false).is_ok_and(|regex| regex.is_match("")));
        for pattern in [
            format!("{}a{}", "(".repeat(129), ")".repeat(129)),
            format!("{}a{}", "(".repeat(499), ")".repeat(499)),
            "(".repeat(1000),
        ] {
            assert!(pattern.chars().count() <= 1000);
            let error = translate_lucene_regex(&pattern, RegexFlags::ALL, false).expect_err("refused");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["error"]["type"], "parsing_exception");
            assert_eq!(body["error"]["limit"], 128);
        }
    }

    #[test]
    fn translated_literal_patterns_match_only_themselves() {
        let mut random_generator = StdRng::seed_from_u64(0x1CE6_A7E5);
        let alphabet: Vec<char> = "aZ09@#<>.*+?()[]{}|&~\"\\-^$ é€".chars().collect();
        for _ in 0..500 {
            let length = random_generator.random_range(1..12);
            let text: String = (0..length)
                .map(|_| alphabet[random_generator.random_range(0..alphabet.len())])
                .collect();
            // `\` before any non-letter makes it literal; letters and digits stand for themselves.
            let mut pattern = String::new();
            for character in text.chars() {
                if !character.is_ascii_alphanumeric() {
                    pattern.push('\\');
                }
                pattern.push(character);
            }
            let regex = translate_lucene_regex(&pattern, RegexFlags::ALL, false).expect("a literal translates");
            assert!(regex.is_match(&text), "[{pattern}] matches [{text}]");
            assert!(
                !regex.is_match(&format!("{text}x")),
                "[{pattern}] is anchored at the end"
            );
            assert!(
                !regex.is_match(&format!("x{text}")),
                "[{pattern}] is anchored at the start"
            );
        }
    }
}
