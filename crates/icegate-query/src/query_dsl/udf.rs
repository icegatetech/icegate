//! Full-text scalar functions over the standard analyzer, and MAP-value matching.
//!
//! A text column is matched per token of [`tokenize_standard`], the tokenizer the
//! system-index evaluator also uses, so a data index and a system index agree on
//! what a term matches. Every function answers NULL for a NULL input value, which
//! the `must_not` lowering (`NOT coalesce(p, false)`) relies on. A NULL literal
//! argument is an execution error, except a range bound, where NULL is an open bound.

use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, AsArray, BooleanArray, MapArray, StringArray},
        datatypes::DataType,
    },
    common::{Result, exec_datafusion_err, exec_err},
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility},
    scalar::ScalarValue,
};
use regex::{Regex, RegexBuilder};

use super::{
    analyzer::{contains_token_phrase, tokenize_standard},
    ast::{KeywordBound, KeywordRange, MapValuePredicate},
};
use crate::opensearch::error::OpenSearchError;

/// A compiled pattern compared and hashed by its source text, so a plan holding
/// it stays comparable and hashable like any other expression.
#[derive(Debug, Clone)]
pub(crate) struct MatchRegex(Regex);

impl MatchRegex {
    /// Whether the whole of `text` matches the anchored pattern.
    pub(crate) fn matches(&self, text: &str) -> bool {
        self.0.is_match(text)
    }
}

impl PartialEq for MatchRegex {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for MatchRegex {}

impl Hash for MatchRegex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_str().hash(state);
    }
}

/// `text_term_match(text, term)`: a token of `text` equals `term`.
pub(crate) fn build_text_term_match_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(TextMatch::new(TextMatchKind::Term))
}

/// `text_prefix_match(text, prefix)`: a token of `text` starts with `prefix`.
pub(crate) fn build_text_prefix_match_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(TextMatch::new(TextMatchKind::Prefix))
}

/// `text_phrase_match(text, tokens, is_last_token_prefix)`: the listed tokens
/// occur at consecutive positions of `text`, the last one only as a prefix when
/// the flag is set (see [`contains_token_phrase`]).
pub(crate) fn build_text_phrase_match_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(TextMatch::new(TextMatchKind::Phrase))
}

/// `text_range_match(text, lower, lower_inclusive, upper, upper_inclusive)`: a
/// token of `text` lies in the range, compared in UTF-8 byte order; a NULL bound is open.
pub(crate) fn build_text_range_match_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(TextMatch::new(TextMatchKind::Range))
}

/// `text_regex_match(text)`: a token of `text` matches the anchored pattern. The
/// pattern is compiled once, when the expression is built, not per batch.
pub(crate) fn build_text_regex_match_udf(pattern: MatchRegex) -> ScalarUDF {
    ScalarUDF::new_from_impl(TextMatch::new(TextMatchKind::Regex(pattern)))
}

/// `map_value_match(map)`: an entry of the MAP has a non-NULL value satisfying
/// the predicate; an empty map matches nothing and a NULL map answers NULL.
pub(crate) fn build_map_value_match_udf(predicate: MapValuePredicate) -> ScalarUDF {
    ScalarUDF::new_from_impl(MapValueMatch {
        predicate,
        signature: Signature::any(1, Volatility::Immutable),
    })
}

/// Appends one character to a Rust pattern as a literal outside a class: an
/// ASCII letter becomes the class of both its cases when matching ignores ASCII
/// case (the only case folding a `case_insensitive` leaf asks for).
fn push_regex_literal(output: &mut String, character: char, is_ascii_case_insensitive: bool) {
    if is_ascii_case_insensitive && character.is_ascii_alphabetic() {
        output.push('[');
        output.push(character.to_ascii_lowercase());
        output.push(character.to_ascii_uppercase());
        output.push(']');
    } else {
        let mut buffer = [0_u8; 4];
        output.push_str(&regex::escape(character.encode_utf8(&mut buffer)));
    }
}

/// Translates a wildcard pattern (`*` any run, `?` one character, `\` escaping
/// the next character) into a pattern anchored at both ends of the input.
///
/// # Errors
///
/// `illegal_argument` (400) when the translation does not compile (see
/// [`compile_anchored_pattern`]).
pub(crate) fn translate_wildcard(
    pattern: &str,
    is_ascii_case_insensitive: bool,
) -> Result<MatchRegex, OpenSearchError> {
    let mut output = String::with_capacity(pattern.len() * 2 + 8);
    output.push_str("^(?s:");
    let mut characters = pattern.chars();
    while let Some(character) = characters.next() {
        match character {
            '*' => output.push_str(".*"),
            '?' => output.push('.'),
            // A trailing `\` escapes nothing and stands for itself.
            '\\' => push_regex_literal(
                &mut output,
                characters.next().unwrap_or('\\'),
                is_ascii_case_insensitive,
            ),
            literal => push_regex_literal(&mut output, literal, is_ascii_case_insensitive),
        }
    }
    output.push_str(")$");
    compile_anchored_pattern(pattern, &output)
}

/// Escapes `*`, `?` and `\` so a literal reads as itself in a wildcard pattern.
pub(crate) fn escape_wildcard(literal: &str) -> String {
    let mut escaped = String::with_capacity(literal.len());
    for character in literal.chars() {
        if matches!(character, '*' | '?' | '\\') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

/// Compiles a translated pattern; `source` names the request's pattern in the error.
///
/// # Errors
///
/// `illegal_argument` (400) when the translation does not compile, e.g. when it
/// exceeds the regex engine's size limit.
fn compile_anchored_pattern(source: &str, translated: &str) -> Result<MatchRegex, OpenSearchError> {
    RegexBuilder::new(translated).build().map(MatchRegex).map_err(|error| {
        OpenSearchError::illegal_argument(format!("the pattern [{source}] cannot be compiled: {error}"))
    })
}

/// Which per-token predicate a [`TextMatch`] applies.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TextMatchKind {
    /// A token equals the term argument.
    Term,
    /// A token starts with the prefix argument.
    Prefix,
    /// The listed tokens occur at consecutive positions.
    Phrase,
    /// A token lies in the range the bound arguments state.
    Range,
    /// A token matches the pattern.
    Regex(MatchRegex),
}

/// One implementation for every per-token text predicate: the kind picks the
/// predicate, the function name and its arity.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TextMatch {
    kind: TextMatchKind,
    signature: Signature,
}

impl TextMatch {
    fn new(kind: TextMatchKind) -> Self {
        let arity = match kind {
            TextMatchKind::Regex(_) => 1,
            TextMatchKind::Term | TextMatchKind::Prefix => 2,
            TextMatchKind::Phrase => 3,
            TextMatchKind::Range => 5,
        };
        Self {
            kind,
            signature: Signature::any(arity, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TextMatch {
    fn name(&self) -> &'static str {
        match self.kind {
            TextMatchKind::Term => "text_term_match",
            TextMatchKind::Prefix => "text_prefix_match",
            TextMatchKind::Phrase => "text_phrase_match",
            TextMatchKind::Range => "text_range_match",
            TextMatchKind::Regex(_) => "text_regex_match",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let name = self.name();
        let texts = read_argument_array(&args, name)?;
        let match_flags = match &self.kind {
            TextMatchKind::Term => {
                let Some(term) = read_string_argument(&args.args, 1, name)? else {
                    return exec_err!("{name} argument 1 must not be NULL");
                };
                match_each_text(&texts, name, |text| {
                    tokenize_standard(text).iter().any(|token| token.text == term)
                })?
            }
            TextMatchKind::Prefix => {
                let Some(prefix) = read_string_argument(&args.args, 1, name)? else {
                    return exec_err!("{name} argument 1 must not be NULL");
                };
                match_each_text(&texts, name, |text| {
                    tokenize_standard(text).iter().any(|token| token.text.starts_with(prefix))
                })?
            }
            TextMatchKind::Phrase => {
                let phrase = read_list_argument(&args.args, 1, name)?;
                let is_last_token_prefix = read_boolean_argument(&args.args, 2, name)?;
                match_each_text(&texts, name, |text| {
                    contains_token_phrase(&tokenize_standard(text), &phrase, is_last_token_prefix)
                })?
            }
            TextMatchKind::Range => {
                let range = KeywordRange {
                    lower: read_keyword_bound(&args.args, 1, name)?,
                    upper: read_keyword_bound(&args.args, 3, name)?,
                };
                match_each_text(&texts, name, |text| {
                    tokenize_standard(text).iter().any(|token| range.contains(&token.text))
                })?
            }
            TextMatchKind::Regex(pattern) => match_each_text(&texts, name, |text| {
                tokenize_standard(text).iter().any(|token| pattern.matches(&token.text))
            })?,
        };
        Ok(ColumnarValue::Array(Arc::new(match_flags)))
    }
}

/// Applies `predicate` to every value of a string column; NULL stays NULL.
fn match_each_text(texts: &ArrayRef, name: &str, predicate: impl Fn(&str) -> bool) -> Result<BooleanArray> {
    Ok(match texts.data_type() {
        DataType::Utf8 => texts.as_string::<i32>().iter().map(|text| text.map(&predicate)).collect(),
        DataType::LargeUtf8 => texts.as_string::<i64>().iter().map(|text| text.map(&predicate)).collect(),
        DataType::Utf8View => texts.as_string_view().iter().map(|text| text.map(&predicate)).collect(),
        other => return exec_err!("{name} needs a string column, got {other}"),
    })
}

/// The first argument as an array of the batch's length.
fn read_argument_array(args: &ScalarFunctionArgs, name: &str) -> Result<ArrayRef> {
    match args.args.first() {
        Some(ColumnarValue::Array(array)) => Ok(Arc::clone(array)),
        Some(ColumnarValue::Scalar(scalar)) => scalar.to_array_of_size(args.number_rows),
        None => exec_err!("{name} needs a column argument"),
    }
}

/// The string literal at `index`; `None` for a NULL literal.
fn read_string_argument<'a>(args: &'a [ColumnarValue], index: usize, name: &str) -> Result<Option<&'a str>> {
    let Some(ColumnarValue::Scalar(
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value),
    )) = args.get(index)
    else {
        return exec_err!("{name} argument {index} must be a string literal");
    };
    Ok(value.as_deref())
}

/// The non-NULL boolean literal at `index`.
fn read_boolean_argument(args: &[ColumnarValue], index: usize, name: &str) -> Result<bool> {
    let Some(ColumnarValue::Scalar(ScalarValue::Boolean(Some(value)))) = args.get(index) else {
        return exec_err!("{name} argument {index} must be a boolean literal");
    };
    Ok(*value)
}

/// A range bound from a value argument and the inclusiveness argument after it; NULL is open.
fn read_keyword_bound(args: &[ColumnarValue], index: usize, name: &str) -> Result<Option<KeywordBound>> {
    let Some(value) = read_string_argument(args, index, name)? else {
        return Ok(None);
    };
    Ok(Some(KeywordBound {
        value: value.to_owned(),
        is_inclusive: read_boolean_argument(args, index + 1, name)?,
    }))
}

/// The non-NULL list of non-NULL strings at `index`.
fn read_list_argument(args: &[ColumnarValue], index: usize, name: &str) -> Result<Vec<String>> {
    let Some(ColumnarValue::Scalar(ScalarValue::List(list))) = args.get(index) else {
        return exec_err!("{name} argument {index} must be a list literal");
    };
    if list.is_empty() || list.is_null(0) {
        return exec_err!("{name} argument {index} must not be NULL");
    }
    let values = list.value(0);
    let Some(strings) = values.as_any().downcast_ref::<StringArray>() else {
        return exec_err!("{name} argument {index} must list strings");
    };
    strings
        .iter()
        .map(|value| {
            value
                .map(str::to_owned)
                .ok_or_else(|| exec_datafusion_err!("{name} argument {index} must not list NULL"))
        })
        .collect()
}

/// The MAP-value predicate, configured when the expression is built.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MapValueMatch {
    predicate: MapValuePredicate,
    signature: Signature,
}

impl ScalarUDFImpl for MapValueMatch {
    fn name(&self) -> &'static str {
        "map_value_match"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let maps = read_argument_array(&args, self.name())?;
        let Some(maps) = maps.as_any().downcast_ref::<MapArray>() else {
            return exec_err!("map_value_match needs a MAP column");
        };
        let entries = maps.entries();
        let Some(keys) = entries.column(0).as_any().downcast_ref::<StringArray>() else {
            return exec_err!("map_value_match expects Utf8 map keys");
        };
        let Some(values) = entries.column(1).as_any().downcast_ref::<StringArray>() else {
            return exec_err!("map_value_match expects Utf8 map values");
        };
        let offsets = maps.value_offsets();
        let read_entry_offset = |offset: i32| {
            usize::try_from(offset).map_err(|_| exec_datafusion_err!("map_value_match read the entry offset {offset}"))
        };
        let match_flags = (0..maps.len())
            .map(|row| {
                if maps.is_null(row) {
                    return Ok(None);
                }
                // Offsets index the unsliced children, so they stay valid on a sliced map array.
                let start = read_entry_offset(offsets[row])?;
                let end = read_entry_offset(offsets[row + 1])?;
                Ok(Some((start..end).any(|entry| {
                    !keys.is_null(entry) && !values.is_null(entry) && self.predicate.matches(values.value(entry))
                })))
            })
            .collect::<Result<BooleanArray>>()?;
        Ok(ColumnarValue::Array(Arc::new(match_flags)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::{
            array::{Array, ArrayRef, BooleanArray, MapArray, StringArray, StructArray},
            buffer::{OffsetBuffer, ScalarBuffer},
            datatypes::{DataType, Field, Fields},
        },
        common::config::ConfigOptions,
        common::{DataFusionError, Result},
        logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF},
        scalar::ScalarValue,
    };
    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng as _};

    use super::{
        build_map_value_match_udf, build_text_phrase_match_udf, build_text_prefix_match_udf,
        build_text_range_match_udf, build_text_regex_match_udf, build_text_term_match_udf, escape_wildcard,
        translate_wildcard,
    };
    use crate::query_dsl::ast::MapValuePredicate;

    /// Evaluates `udf` over `values` with the stated literal arguments after it.
    fn try_invoke_match_udf(udf: &ScalarUDF, values: ArrayRef, literals: Vec<ScalarValue>) -> Result<ColumnarValue> {
        let rows = values.len();
        let mut args = vec![ColumnarValue::Array(values)];
        args.extend(literals.into_iter().map(ColumnarValue::Scalar));
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("matches", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::new()),
        })
    }

    /// [`try_invoke_match_udf`], read as the boolean array every function answers.
    fn invoke_match_udf(udf: &ScalarUDF, values: ArrayRef, literals: Vec<ScalarValue>) -> Vec<Option<bool>> {
        let result = try_invoke_match_udf(udf, values, literals).expect("the function evaluates");
        let array = match result {
            ColumnarValue::Array(array) => Some(array),
            ColumnarValue::Scalar(_) => None,
        }
        .expect("a full-text function answers an array");
        let booleans = array.as_any().downcast_ref::<BooleanArray>().expect("a boolean array");
        booleans.iter().collect()
    }

    /// Four `body` values: two sharing the token `connection`, a NULL, and one with no token.
    fn build_body_array() -> ArrayRef {
        Arc::new(StringArray::from(vec![
            Some("Connection refused by upstream"),
            Some("Retrying connection in 5s"),
            None,
            Some("!!!"),
        ]))
    }

    fn build_utf8_literal(value: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(value.to_owned()))
    }

    fn build_phrase_literal(tokens: &[&str]) -> ScalarValue {
        let values: Vec<ScalarValue> = tokens.iter().map(|token| build_utf8_literal(token)).collect();
        ScalarValue::List(ScalarValue::new_list_nullable(&values, &DataType::Utf8))
    }

    /// A NULL literal would otherwise read as some value (an empty term matches no token, an
    /// empty prefix every token), so it is refused rather than answered.
    #[test]
    fn a_null_term_prefix_or_phrase_element_is_an_execution_error() {
        for udf in [build_text_term_match_udf(), build_text_prefix_match_udf()] {
            let error = try_invoke_match_udf(&udf, build_body_array(), vec![ScalarValue::Utf8(None)])
                .expect_err("a NULL literal is refused");
            assert!(
                matches!(error, DataFusionError::Execution(_)),
                "{}: {error}",
                udf.name()
            );
        }
        let phrase_with_null = ScalarValue::List(ScalarValue::new_list_nullable(
            &[build_utf8_literal("refused"), ScalarValue::Utf8(None)],
            &DataType::Utf8,
        ));
        let error = try_invoke_match_udf(
            &build_text_phrase_match_udf(),
            build_body_array(),
            vec![phrase_with_null, ScalarValue::Boolean(Some(false))],
        )
        .expect_err("a NULL phrase element is refused");
        assert!(matches!(error, DataFusionError::Execution(_)), "{error}");
    }

    #[test]
    fn term_phrase_and_prefix_udfs_match_tokens_of_the_standard_analyzer() {
        // Tokens: [connection, refused, by, upstream], [retrying, connection, in, 5s], NULL, [].
        assert_eq!(
            invoke_match_udf(
                &build_text_term_match_udf(),
                build_body_array(),
                vec![build_utf8_literal("connection")]
            ),
            vec![Some(true), Some(true), None, Some(false)]
        );
        assert_eq!(
            invoke_match_udf(
                &build_text_term_match_udf(),
                build_body_array(),
                vec![build_utf8_literal("Connection")]
            ),
            vec![Some(false), Some(false), None, Some(false)]
        );
        assert_eq!(
            invoke_match_udf(
                &build_text_prefix_match_udf(),
                build_body_array(),
                vec![build_utf8_literal("conn")]
            ),
            vec![Some(true), Some(true), None, Some(false)]
        );
        assert_eq!(
            invoke_match_udf(
                &build_text_phrase_match_udf(),
                build_body_array(),
                vec![
                    build_phrase_literal(&["refused", "by"]),
                    ScalarValue::Boolean(Some(false))
                ]
            ),
            vec![Some(true), Some(false), None, Some(false)]
        );
        assert_eq!(
            invoke_match_udf(
                &build_text_phrase_match_udf(),
                build_body_array(),
                vec![
                    build_phrase_literal(&["by", "refused"]),
                    ScalarValue::Boolean(Some(false))
                ]
            ),
            vec![Some(false), Some(false), None, Some(false)]
        );
        assert_eq!(
            invoke_match_udf(
                &build_text_phrase_match_udf(),
                build_body_array(),
                vec![
                    build_phrase_literal(&["refused", "b"]),
                    ScalarValue::Boolean(Some(true))
                ]
            ),
            vec![Some(true), Some(false), None, Some(false)]
        );
    }

    #[test]
    fn range_and_pattern_udfs_match_per_token() {
        // ["re", "ri"): `refused` (row 0) and `retrying` (row 1) fall inside.
        assert_eq!(
            invoke_match_udf(
                &build_text_range_match_udf(),
                build_body_array(),
                vec![
                    build_utf8_literal("re"),
                    ScalarValue::Boolean(Some(true)),
                    build_utf8_literal("ri"),
                    ScalarValue::Boolean(Some(false))
                ]
            ),
            vec![Some(true), Some(true), None, Some(false)]
        );
        // The inclusive range ["b", "by"]: only row 0's `by` lies inside; `5s` sorts before
        // `b` ('5' is 0x35, 'b' is 0x62), and `connection`, `in`, `retrying` after `by`.
        assert_eq!(
            invoke_match_udf(
                &build_text_range_match_udf(),
                build_body_array(),
                vec![
                    build_utf8_literal("b"),
                    ScalarValue::Boolean(Some(true)),
                    build_utf8_literal("by"),
                    ScalarValue::Boolean(Some(true))
                ]
            ),
            vec![Some(true), Some(false), None, Some(false)]
        );
        // An open lower bound (NULL) and the exclusive upper bound `by`: row 0 holds `by`
        // itself and tokens sorting after it; row 1's `5s` sorts before it.
        assert_eq!(
            invoke_match_udf(
                &build_text_range_match_udf(),
                build_body_array(),
                vec![
                    ScalarValue::Utf8(None),
                    ScalarValue::Boolean(Some(false)),
                    build_utf8_literal("by"),
                    ScalarValue::Boolean(Some(false))
                ]
            ),
            vec![Some(false), Some(true), None, Some(false)]
        );
        // The exclusive lower bound `retrying` and an open upper bound: `retrying` is row 1's
        // greatest token, so row 1 is out at its own bound, while row 0's `upstream` sorts after it.
        assert_eq!(
            invoke_match_udf(
                &build_text_range_match_udf(),
                build_body_array(),
                vec![
                    build_utf8_literal("retrying"),
                    ScalarValue::Boolean(Some(false)),
                    ScalarValue::Utf8(None),
                    ScalarValue::Boolean(Some(false))
                ]
            ),
            vec![Some(true), Some(false), None, Some(false)]
        );
        // The exclusive lower bound `upstream`, row 0's greatest token: no token lies above it.
        assert_eq!(
            invoke_match_udf(
                &build_text_range_match_udf(),
                build_body_array(),
                vec![
                    build_utf8_literal("upstream"),
                    ScalarValue::Boolean(Some(false)),
                    ScalarValue::Utf8(None),
                    ScalarValue::Boolean(Some(false))
                ]
            ),
            vec![Some(false), Some(false), None, Some(false)]
        );
        let pattern = translate_wildcard("c*n", false).expect("the wildcard translates");
        assert_eq!(
            invoke_match_udf(&build_text_regex_match_udf(pattern), build_body_array(), vec![]),
            vec![Some(true), Some(true), None, Some(false)]
        );
    }

    #[test]
    fn map_values_match_any_entry() {
        let keys = StringArray::from(vec!["http.method", "region", "http.method"]);
        let values = StringArray::from(vec!["GET", "eu-west-1", "POST"]);
        let fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ]
        .into();
        let entries = StructArray::new(
            fields.clone(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
            None,
        );
        // Row 0: {http.method: GET, region: eu-west-1}; row 1: {http.method: POST}; row 2: {}.
        let maps: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("key_value", DataType::Struct(fields), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2, 3, 3])),
            entries,
            None,
            false,
        ));
        assert_eq!(
            invoke_match_udf(
                &build_map_value_match_udf(MapValuePredicate::Equals(String::from("eu-west-1"))),
                Arc::clone(&maps),
                vec![]
            ),
            vec![Some(true), Some(false), Some(false)]
        );
        assert_eq!(
            invoke_match_udf(
                &build_map_value_match_udf(MapValuePredicate::StartsWith(String::from("P"))),
                maps,
                vec![]
            ),
            vec![Some(false), Some(true), Some(false)]
        );
    }

    #[test]
    fn wildcards_translate_with_ascii_case_folding_only_when_asked() {
        assert_eq!(
            translate_wildcard("f?o*\\*", false).expect("translates").0.as_str(),
            "^(?s:f.o.*\\*)$"
        );
        assert_eq!(
            translate_wildcard("Ab*", true).expect("translates").0.as_str(),
            "^(?s:[aA][bB].*)$"
        );
        assert_eq!(
            translate_wildcard("é*", true).expect("translates").0.as_str(),
            "^(?s:é.*)$"
        );
        assert_eq!(escape_wildcard("a*b?c\\"), "a\\*b\\?c\\\\");
        // A trailing `\` escapes nothing and stands for itself.
        assert_eq!(
            translate_wildcard("a\\", false).expect("translates").0.as_str(),
            "^(?s:a\\\\)$"
        );
    }

    /// One element of a wildcard pattern, as the wildcard rule defines them.
    #[derive(Debug, Clone, Copy)]
    enum WildcardElement {
        /// `*`: any run of characters, none included.
        AnyRun,
        /// `?`: exactly one character.
        AnyCharacter,
        /// A character matching itself (and, case-insensitively, its other ASCII case).
        Literal(char),
    }

    /// Literal characters of the drawn patterns: ASCII letters in both cases, the wildcard
    /// metacharacters and the escape (always escaped), a regex metacharacter, and a non-ASCII
    /// letter in both cases, which ASCII case folding must leave alone.
    const LITERAL_ALPHABET: [char; 10] = ['a', 'A', 'b', 'B', '*', '?', '\\', '.', 'é', 'É'];
    const WILDCARD_CASE_COUNT: usize = 2_000;

    /// The wildcard rule stated directly: a run tries every split of the text, any other
    /// element takes one character. Exponential, which is harmless at the drawn lengths.
    fn matches_elements_naively(elements: &[WildcardElement], text: &[char], is_ascii_case_insensitive: bool) -> bool {
        match elements.split_first() {
            None => text.is_empty(),
            Some((WildcardElement::AnyRun, elements_rest)) => (0..=text.len()).any(|skipped_count| {
                matches_elements_naively(elements_rest, &text[skipped_count..], is_ascii_case_insensitive)
            }),
            Some((element, elements_rest)) => text.split_first().is_some_and(|(character, text_rest)| {
                let is_element_match = match element {
                    WildcardElement::AnyRun | WildcardElement::AnyCharacter => true,
                    WildcardElement::Literal(literal) => {
                        literal == character || (is_ascii_case_insensitive && literal.eq_ignore_ascii_case(character))
                    }
                };
                is_element_match && matches_elements_naively(elements_rest, text_rest, is_ascii_case_insensitive)
            }),
        }
    }

    fn draw_literal(generator: &mut StdRng) -> char {
        LITERAL_ALPHABET[generator.random_range(0..LITERAL_ALPHABET.len())]
    }

    /// `elements` spelled as a pattern: a metacharacter or the escape is escaped, and so,
    /// half of the time, is any other literal. A final literal `\` is sometimes left
    /// unescaped, which the rule reads as the escape character itself.
    fn render_pattern(generator: &mut StdRng, elements: &[WildcardElement]) -> String {
        let mut pattern = String::new();
        for (index, element) in elements.iter().enumerate() {
            match element {
                WildcardElement::AnyRun => pattern.push('*'),
                WildcardElement::AnyCharacter => pattern.push('?'),
                WildcardElement::Literal(literal) => {
                    let is_trailing_escape =
                        *literal == '\\' && index + 1 == elements.len() && generator.random_bool(0.5);
                    if !is_trailing_escape && (matches!(literal, '*' | '?' | '\\') || generator.random_bool(0.5)) {
                        pattern.push('\\');
                    }
                    pattern.push(*literal);
                }
            }
        }
        pattern
    }

    /// A text `elements` matches with its literals as written: each run replaced by up to two
    /// drawn characters. Under case folding a literal is sometimes written in its other case.
    fn instantiate_elements(
        generator: &mut StdRng,
        elements: &[WildcardElement],
        is_ascii_case_insensitive: bool,
    ) -> Vec<char> {
        let mut text = Vec::new();
        for element in elements {
            match element {
                WildcardElement::AnyRun => {
                    let run_length = generator.random_range(0..=2);
                    text.extend((0..run_length).map(|_| draw_literal(generator)));
                }
                WildcardElement::AnyCharacter => text.push(draw_literal(generator)),
                WildcardElement::Literal(literal) => text.push(
                    if is_ascii_case_insensitive && literal.is_ascii_alphabetic() && generator.random_bool(0.5) {
                        if literal.is_ascii_lowercase() {
                            literal.to_ascii_uppercase()
                        } else {
                            literal.to_ascii_lowercase()
                        }
                    } else {
                        *literal
                    },
                ),
            }
        }
        text
    }

    fn draw_element(generator: &mut StdRng) -> WildcardElement {
        match generator.random_range(0..4) {
            0 => WildcardElement::AnyRun,
            1 => WildcardElement::AnyCharacter,
            _ => WildcardElement::Literal(draw_literal(generator)),
        }
    }

    /// Half of the texts instantiate their pattern and half are drawn freely, so both outcomes
    /// are exercised under both case flags; the counts prove it.
    #[test]
    fn a_translated_wildcard_agrees_with_a_naive_reference_on_seeded_patterns_and_texts() {
        let mut generator = StdRng::seed_from_u64(0x0057_1DCA_2D05);
        for is_ascii_case_insensitive in [false, true] {
            let mut matched_count = 0;
            for _ in 0..WILDCARD_CASE_COUNT {
                let element_count = generator.random_range(0..=6);
                let elements: Vec<WildcardElement> = (0..element_count).map(|_| draw_element(&mut generator)).collect();
                let pattern = render_pattern(&mut generator, &elements);
                let text_characters = if generator.random_bool(0.5) {
                    instantiate_elements(&mut generator, &elements, is_ascii_case_insensitive)
                } else {
                    let text_length = generator.random_range(0..=6);
                    (0..text_length).map(|_| draw_literal(&mut generator)).collect()
                };
                let text: String = text_characters.iter().collect();
                let should_match = matches_elements_naively(&elements, &text_characters, is_ascii_case_insensitive);

                let translated = translate_wildcard(&pattern, is_ascii_case_insensitive).expect("the pattern compiles");
                assert_eq!(
                    translated.0.is_match(&text),
                    should_match,
                    "{pattern:?} against {text:?}, case-insensitive: {is_ascii_case_insensitive}"
                );
                matched_count += usize::from(should_match);
            }
            assert!(matched_count >= WILDCARD_CASE_COUNT / 4, "{matched_count} matches");
            assert!(
                WILDCARD_CASE_COUNT - matched_count >= WILDCARD_CASE_COUNT / 4,
                "{matched_count} matches"
            );
        }
    }
}
