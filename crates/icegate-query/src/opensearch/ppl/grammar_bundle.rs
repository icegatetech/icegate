//! `GET /_plugins/_ppl/_grammar`: the vendored PPL grammar as an autocomplete bundle.
//!
//! Every value is read from the committed `.interp` files of the grammar crate, so re-vendoring the
//! grammar changes the answer with no edit here. The key layout and the `grammarHash` algorithm are
//! IceGate's contract:
//!
//! - The keys, in this order: `bundleVersion` ([`BUNDLE_VERSION`]), `antlrVersion`
//!   ([`ANTLR_VERSION`]), `grammarHash`, `startRuleIndex` (the index of the parser rule `root`, else
//!   0), `lexerSerializedATN`, `lexerRuleNames`, `channelNames` (named channels only, so a `null`
//!   padding line of the `.interp` file is dropped), `modeNames`, `parserSerializedATN`,
//!   `parserRuleNames`, `literalNames` and `symbolicNames` (the parser vocabulary indexed by token
//!   type, `null` where a token has none), `tokenDictionary`, `ignoredTokens`, `rulesToVisit`.
//! - `grammarHash` is `sha256:` plus the lower-case hex SHA-256 over: every lexer ATN value, then
//!   every parser ATN value, each as four big-endian bytes; every lexer rule name, every parser rule
//!   name, every literal token name and every symbolic token name, each as `0x01` followed by its
//!   UTF-8 bytes (`0x00` for a missing name) and then `0xFF`; finally the UTF-8 bytes of
//!   [`ANTLR_VERSION`].

use std::sync::LazyLock;

use axum::http::header;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use icegate_query_grammar::ppl::{PPL_LEXER_INTERP, PPL_PARSER_INTERP};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{NoParameters, Parameters};

/// The ANTLR version the bundle declares and hashes.
const ANTLR_VERSION: &str = "4.13.2";

/// The bundle layout version.
const BUNDLE_VERSION: &str = "1.0";

/// Tokens the bundle lists in `ignoredTokens`, together with every token whose symbolic name ends in
/// `_LITERAL`.
const INTERNAL_NON_LITERAL_TOKENS: [&str; 13] = [
    "ID",
    "NUMERIC_ID",
    "ID_DATE_SUFFIX",
    "CLUSTER",
    "TIME_SNAP",
    "SPANLENGTH",
    "DECIMAL_SPANLENGTH",
    "DQUOTA_STRING",
    "SQUOTA_STRING",
    "BQUOTA_STRING",
    "LINE_COMMENT",
    "BLOCK_COMMENT",
    "ERROR_RECOGNITION",
];

/// Parser rules whose indices the bundle lists in `rulesToVisit`, in this order.
const RULES_TO_VISIT: [&str; 16] = [
    "statsFunctionName",
    "takeAggFunction",
    "integerLiteral",
    "decimalLiteral",
    "keywordsCanBeId",
    "renameClause",
    "qualifiedName",
    "tableQualifiedName",
    "wcQualifiedName",
    "positionFunctionName",
    "searchableKeyWord",
    "stringLiteral",
    "searchCommand",
    "searchComparisonOperator",
    "comparisonOperator",
    "sqlLikeJoinType",
];

/// Where a token-dictionary value comes from.
#[derive(Debug, Clone, Copy)]
enum DictionaryValue {
    /// The token type of this symbolic name.
    Token(&'static str),
    /// The value of this named channel.
    Channel(&'static str),
}

/// The token dictionary, in bundle order.
const TOKEN_DICTIONARY: [(&str, DictionaryValue); 13] = [
    ("WHITESPACE", DictionaryValue::Channel("WHITESPACE")),
    ("FROM", DictionaryValue::Token("FROM")),
    ("OPENING_BRACKET", DictionaryValue::Token("LT_PRTHS")),
    ("CLOSING_BRACKET", DictionaryValue::Token("RT_PRTHS")),
    ("SEARCH", DictionaryValue::Token("SEARCH")),
    ("SOURCE", DictionaryValue::Token("SOURCE")),
    ("PIPE", DictionaryValue::Token("PIPE")),
    ("ID", DictionaryValue::Token("ID")),
    ("EQUAL", DictionaryValue::Token("EQUAL")),
    ("IN", DictionaryValue::Token("IN")),
    ("COMMA", DictionaryValue::Token("COMMA")),
    ("BACKTICK_QUOTE", DictionaryValue::Token("BQUOTA_STRING")),
    ("DOT", DictionaryValue::Token("DOT")),
];

/// The serialized bundle, built on the first request; later requests share its bytes. Serializing
/// once spares every request a re-encoding of ATN arrays thousands of values long.
static GRAMMAR_BUNDLE: LazyLock<Result<Bytes, OpenSearchError>> = LazyLock::new(|| {
    let bundle = build_grammar_bundle()?;
    serde_json::to_vec(&bundle)
        .map(Bytes::from)
        .map_err(|error| OpenSearchError::internal(format!("serializing the PPL grammar bundle failed: {error}")))
});

/// `GET /_plugins/_ppl/_grammar`: the bundle; reads no table.
pub(crate) async fn serve_ppl_grammar(Parameters(NoParameters {}): Parameters<NoParameters>) -> Response {
    match &*GRAMMAR_BUNDLE {
        Ok(bundle) => ([(header::CONTENT_TYPE, "application/json")], bundle.clone()).into_response(),
        Err(error) => error.clone().into_response(),
    }
}

/// Builds the grammar bundle from the committed `.interp` files.
///
/// # Errors
///
/// `OpenSearchError::internal` when an `.interp` file lacks a section, a rule of [`RULES_TO_VISIT`] or a
/// dictionary token: a re-vendored grammar that renamed one must fail loudly, not serve a bundle that
/// points at the wrong rules.
pub(crate) fn build_grammar_bundle() -> Result<Value, OpenSearchError> {
    let lexer = InterpreterData::parse(PPL_LEXER_INTERP)?;
    let parser = InterpreterData::parse(PPL_PARSER_INTERP)?;
    let token_type = |name: &str| {
        parser
            .symbolic_names
            .iter()
            .position(|symbolic| *symbolic == Some(name))
            .ok_or_else(|| report_malformed_grammar(&format!("no token {name}")))
    };
    let mut dictionary = Map::with_capacity(TOKEN_DICTIONARY.len());
    for (key, source) in TOKEN_DICTIONARY {
        let token_value = match source {
            DictionaryValue::Token(name) => token_type(name)?,
            DictionaryValue::Channel(name) => lexer
                .channel_names
                .iter()
                .position(|channel| *channel == name)
                .ok_or_else(|| report_malformed_grammar(&format!("no channel {name}")))?,
        };
        dictionary.insert(key.to_owned(), json!(token_value));
    }
    let ignored_tokens: Vec<usize> = parser
        .symbolic_names
        .iter()
        .enumerate()
        .filter(|(_, name)| {
            name.is_some_and(|name| name.ends_with("_LITERAL") || INTERNAL_NON_LITERAL_TOKENS.contains(&name))
        })
        .map(|(token, _)| token)
        .collect();
    let rules_to_visit = RULES_TO_VISIT
        .iter()
        .map(|rule| {
            parser
                .rule_names
                .iter()
                .position(|name| name == rule)
                .ok_or_else(|| report_malformed_grammar(&format!("no parser rule {rule}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "bundleVersion": BUNDLE_VERSION,
        "antlrVersion": ANTLR_VERSION,
        "grammarHash": compute_grammar_hash(&lexer, &parser),
        "startRuleIndex": find_start_rule_index(&parser.rule_names),
        "lexerSerializedATN": lexer.atn,
        "lexerRuleNames": lexer.rule_names,
        "channelNames": lexer.channel_names,
        "modeNames": lexer.mode_names,
        "parserSerializedATN": parser.atn,
        "parserRuleNames": parser.rule_names,
        "literalNames": parser.literal_names,
        "symbolicNames": parser.symbolic_names,
        "tokenDictionary": dictionary,
        "ignoredTokens": ignored_tokens,
        "rulesToVisit": rules_to_visit,
    }))
}

/// The bundle's `startRuleIndex`: the index of the parser rule `root`, or 0 when the grammar has no
/// rule of that name.
fn find_start_rule_index(rule_names: &[&str]) -> usize {
    rule_names.iter().position(|name| *name == "root").unwrap_or(0)
}

/// The internal error of a committed `.interp` file that does not have the expected shape.
fn report_malformed_grammar(detail: &str) -> OpenSearchError {
    OpenSearchError::internal(format!("the vendored PPL grammar data is malformed: {detail}"))
}

/// The sections of one ANTLR `.interp` file that the bundle reads.
#[derive(Debug, Default)]
struct InterpreterData<'a> {
    /// Literal token names by token type; `None` where a token has no literal.
    literal_names: Vec<Option<&'a str>>,
    /// Symbolic token names by token type; `None` where a token has no name.
    symbolic_names: Vec<Option<&'a str>>,
    /// Rule names by rule index.
    rule_names: Vec<&'a str>,
    /// Named channels by channel value (lexer only).
    channel_names: Vec<&'a str>,
    /// Mode names (lexer only).
    mode_names: Vec<&'a str>,
    /// The serialized ATN.
    atn: Vec<i32>,
}

/// A section of an ANTLR `.interp` file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InterpreterSection {
    /// `token literal names:`
    LiteralNames,
    /// `token symbolic names:`
    SymbolicNames,
    /// `rule names:`
    RuleNames,
    /// `channel names:`
    ChannelNames,
    /// `mode names:`
    ModeNames,
    /// `atn:`
    Atn,
}

impl InterpreterSection {
    /// The section a header line opens; `None` for any other line.
    fn from_header(line: &str) -> Option<Self> {
        match line {
            "token literal names:" => Some(Self::LiteralNames),
            "token symbolic names:" => Some(Self::SymbolicNames),
            "rule names:" => Some(Self::RuleNames),
            "channel names:" => Some(Self::ChannelNames),
            "mode names:" => Some(Self::ModeNames),
            "atn:" => Some(Self::Atn),
            _ => None,
        }
    }

    /// The section's own bit in a `u8` set of sections. The enum has fewer than eight variants
    /// (pinned at compile time below), so every variant's declaration position is a valid shift.
    const fn to_bit(self) -> u8 {
        1 << self as u8
    }
}

// A ninth section would overflow the `u8` shift of `to_bit`; refuse to compile instead.
const _: () = assert!((InterpreterSection::Atn as u8) < 8);

impl<'a> InterpreterData<'a> {
    /// Reads the sections of `text`: a header line `<name>:`, one entry per line, a blank line after
    /// each section. A section that appears twice, a second ATN line or a line outside any section
    /// is refused rather than merged or overwritten.
    fn parse(text: &'a str) -> Result<Self, OpenSearchError> {
        let mut interpreter_data = Self::default();
        let mut section: Option<InterpreterSection> = None;
        let mut seen_sections = 0_u8;
        for line in text.lines() {
            if let Some(header) = InterpreterSection::from_header(line) {
                if seen_sections & header.to_bit() != 0 {
                    return Err(report_malformed_grammar(&format!("the section {line:?} repeats")));
                }
                seen_sections |= header.to_bit();
                section = Some(header);
                continue;
            }
            if line.is_empty() {
                section = None;
                continue;
            }
            let Some(open_section) = section else {
                return Err(report_malformed_grammar(&format!("unexpected line {line:?}")));
            };
            match open_section {
                InterpreterSection::LiteralNames => {
                    interpreter_data.literal_names.push((line != "null").then_some(line));
                }
                InterpreterSection::SymbolicNames => {
                    interpreter_data.symbolic_names.push((line != "null").then_some(line));
                }
                InterpreterSection::RuleNames => interpreter_data.rule_names.push(line),
                // Channel values below the first user channel may be padded with `null` lines; the
                // bundle lists named channels, whose positions are their channel values.
                InterpreterSection::ChannelNames => {
                    if line != "null" {
                        interpreter_data.channel_names.push(line);
                    }
                }
                InterpreterSection::ModeNames => interpreter_data.mode_names.push(line),
                InterpreterSection::Atn => {
                    if !interpreter_data.atn.is_empty() {
                        return Err(report_malformed_grammar("the ATN section has more than one line"));
                    }
                    interpreter_data.atn = parse_atn(line)?;
                }
            }
        }
        if interpreter_data.atn.is_empty()
            || interpreter_data.rule_names.is_empty()
            || interpreter_data.symbolic_names.is_empty()
        {
            return Err(report_malformed_grammar(
                "an .interp file lacks its ATN, rules or vocabulary",
            ));
        }
        Ok(interpreter_data)
    }
}

/// The `[v, v, …]` ATN line as values.
fn parse_atn(line: &str) -> Result<Vec<i32>, OpenSearchError> {
    let values = line
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .ok_or_else(|| report_malformed_grammar("the ATN line is not a bracketed list"))?;
    values
        .split(',')
        .map(|value| {
            value
                .trim()
                .parse::<i32>()
                .map_err(|error| report_malformed_grammar(&format!("ATN value {value:?}: {error}")))
        })
        .collect()
}

/// `sha256:` plus the hex SHA-256 of the ATNs, rule names, vocabulary and ANTLR version, laid out as
/// the module doc states, so a client comparing hashes detects any grammar change.
fn compute_grammar_hash(lexer: &InterpreterData<'_>, parser: &InterpreterData<'_>) -> String {
    let mut digest = Sha256::new();
    for value in lexer.atn.iter().chain(&parser.atn) {
        digest.update(value.to_be_bytes());
    }
    for name in lexer.rule_names.iter().chain(&parser.rule_names) {
        update_name(&mut digest, Some(name));
    }
    for name in parser.literal_names.iter().chain(&parser.symbolic_names) {
        update_name(&mut digest, *name);
    }
    digest.update(ANTLR_VERSION.as_bytes());
    format!("sha256:{}", hex::encode(digest.finalize()))
}

/// Hashes one optional name: a presence byte, its UTF-8 bytes, then a separator so that adjacent
/// names cannot run together.
fn update_name(digest: &mut Sha256, name: Option<&str>) {
    match name {
        Some(name) => {
            digest.update([1_u8]);
            digest.update(name.as_bytes());
        }
        None => digest.update([0_u8]),
    }
    digest.update([0xFF_u8]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::opensearch::error::OpenSearchErrorKind;

    #[test]
    fn interpreter_data_reads_sections_drops_channel_padding_and_refuses_malformed_text() {
        let text = "token literal names:\nnull\n'|'\n\ntoken symbolic names:\nnull\nPIPE\n\nrule names:\nPIPE\n\nchannel names:\nDEFAULT_TOKEN_CHANNEL\nHIDDEN\nnull\nnull\nWHITESPACE\n\nmode names:\nDEFAULT_MODE\n\natn:\n[4, 0, 7]\n";
        let interpreter_data = InterpreterData::parse(text).expect("well-formed data");
        assert_eq!(interpreter_data.literal_names, [None, Some("'|'")]);
        assert_eq!(interpreter_data.symbolic_names, [None, Some("PIPE")]);
        assert_eq!(interpreter_data.rule_names, ["PIPE"]);
        assert_eq!(
            interpreter_data.channel_names,
            ["DEFAULT_TOKEN_CHANNEL", "HIDDEN", "WHITESPACE"]
        );
        assert_eq!(interpreter_data.mode_names, ["DEFAULT_MODE"]);
        assert_eq!(interpreter_data.atn, [4, 0, 7]);
        // Each broken text is the well-formed text above with exactly one defect, so the refusal can
        // only come from that defect: without it the text parses.
        let broken_texts = [
            ("an unbracketed ATN", text.replace("[4, 0, 7]", "4, 0, 7")),
            ("a non-integer ATN value", text.replace("[4, 0, 7]", "[4, x, 7]")),
            ("a line outside any section", format!("stray\n{text}")),
            ("no ATN", text.replace("\natn:\n[4, 0, 7]\n", "")),
            ("no rule names", text.replace("rule names:\nPIPE\n\n", "")),
            (
                "no symbolic names",
                text.replace("token symbolic names:\nnull\nPIPE\n\n", ""),
            ),
            (
                "a repeated section",
                text.replace(
                    "mode names:\nDEFAULT_MODE\n\n",
                    "mode names:\nDEFAULT_MODE\n\nmode names:\nOTHER_MODE\n\n",
                ),
            ),
            ("a second ATN line", text.replace("[4, 0, 7]", "[4, 0, 7]\n[4, 0, 8]")),
        ];
        for (defect, broken_text) in broken_texts {
            assert_ne!(
                broken_text, text,
                "the fixture for {defect} differs from the well-formed text"
            );
            let error = InterpreterData::parse(&broken_text).expect_err("a broken .interp file is refused");
            assert_eq!(error.kind(), &OpenSearchErrorKind::Internal, "{defect}");
        }
    }

    #[test]
    fn the_committed_grammar_builds_a_bundle_with_root_first() {
        let bundle = build_grammar_bundle().expect("the vendored grammar builds");
        // `root` is the first rule of the committed grammar: 0 is its index, not the fallback.
        assert_eq!(bundle["parserRuleNames"][0], "root");
        assert_eq!(bundle["startRuleIndex"], 0);
        assert_eq!(bundle["tokenDictionary"]["WHITESPACE"], 2);
    }

    #[test]
    fn the_start_rule_is_root_wherever_it_stands_and_0_without_it() {
        assert_eq!(find_start_rule_index(&["x", "root"]), 1);
        assert_eq!(find_start_rule_index(&["x"]), 0);
    }
}
