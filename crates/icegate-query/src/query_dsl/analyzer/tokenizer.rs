//! The standard tokenizer's scanner: the token rules of Lucene's `StandardTokenizerImpl.jflex` at
//! the release `LUCENE_VERSION` (`opensearch/node.rs`) names, over Unicode 12.1 properties.
//!
//! Each rule is a regular pattern over character sets. The rules compile once into one
//! nondeterministic automaton whose accepting states carry their rule's priority. A scan from
//! a resume point keeps the longest accepted prefix and, among rules accepting that same prefix,
//! the earliest rule. A scan never reads past the [`MAX_TOKEN_UTF16_UNITS`] UTF-16 units that
//! follow the resume point, so a longer run of word characters is cut into tokens of at most that
//! length instead of being dropped. A code point that starts no token is skipped.

use std::sync::LazyLock;

use super::TokenType;
use super::word_break_tables::{
    CODE_POINT_RANGES, COMPLEX_CONTEXT, EMOJI, EMOJI_MODIFIER, EMOJI_MODIFIER_BASE, EXTENDED_PICTOGRAPHIC, Script,
    WordBreak,
};

/// The scanner window: no token is longer than this many UTF-16 code units.
pub(super) const MAX_TOKEN_UTF16_UNITS: usize = 255;

/// VARIATION SELECTOR-15, the text presentation selector.
const TEXT_PRESENTATION_SELECTOR: u32 = 0xFE0E;
/// VARIATION SELECTOR-16, the emoji presentation selector.
const EMOJI_PRESENTATION_SELECTOR: u32 = 0xFE0F;
/// COMBINING ENCLOSING KEYCAP.
const COMBINING_ENCLOSING_KEYCAP: u32 = 0x20E3;
/// TAG SPACE, the first tag specification character.
const TAG_SPECIFICATION_FIRST: u32 = 0xE_0020;
/// TAG TILDE, the last tag specification character.
const TAG_SPECIFICATION_LAST: u32 = 0xE_007E;
/// CANCEL TAG, which ends a tag sequence.
const TAG_TERMINATOR: u32 = 0xE_007F;
/// Characters the rules exclude from the plain emoji class. They still reach the emoji rule
/// through `Extended_Pictographic`.
const ACCIDENTAL_EMOJI: [u32; 5] = [0x00A9, 0x00AE, 0x2122, 0x3030, 0x303D];

/// The Unicode 12.1 properties of one code point the rules read.
#[derive(Debug, Clone, Copy)]
struct CodePointClass {
    /// The `Word_Break` value.
    word_break: WordBreak,
    /// The script, when it is one the rules distinguish.
    script: Script,
    /// The flag constants of the generated tables that hold for the code point.
    flags: u8,
}

impl CodePointClass {
    /// Whether the code point carries `flag`.
    const fn has_flag(self, flag: u8) -> bool {
        self.flags & flag != 0
    }

    /// Whether the code point is `Extend`, `Format` or `ZWJ`, which UAX #29 rule WB4 skips.
    const fn is_extend_format_or_zwj(self) -> bool {
        matches!(self.word_break, WordBreak::Extend | WordBreak::Format | WordBreak::Zwj)
    }

    /// Whether the code point is `ALetter` or `Hebrew_Letter`.
    const fn is_letter(self) -> bool {
        matches!(self.word_break, WordBreak::ALetter | WordBreak::HebrewLetter)
    }
}

/// Looks up the properties of `code_point` in the generated ranges.
fn classify_code_point(code_point: u32) -> CodePointClass {
    let range_lookup = CODE_POINT_RANGES.binary_search_by(|&(first_code_point, last_code_point, ..)| {
        if last_code_point < code_point {
            std::cmp::Ordering::Less
        } else if first_code_point > code_point {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
    range_lookup.map_or(
        CodePointClass {
            word_break: WordBreak::Other,
            script: Script::Other,
            flags: 0,
        },
        |index| {
            let (_, _, word_break, script, flags) = CODE_POINT_RANGES[index];
            CodePointClass {
                word_break,
                script,
                flags,
            }
        },
    )
}

/// A character class of the token rules, named after the macro it transcribes.
#[derive(Debug, Clone, Copy)]
enum CharacterSet {
    /// `[\p{WB:Format}\p{WB:Extend}\p{WB:ZWJ}]`.
    ExtendFormatZwj,
    /// The same set without [`TEXT_PRESENTATION_SELECTOR`] and [`EMOJI_PRESENTATION_SELECTOR`], so
    /// emoji sequences see their selectors.
    ExtendFormatZwjWithoutPresentationSelectors,
    /// The characters [`is_keycap_base`] accepts.
    KeycapBase,
    /// [`COMBINING_ENCLOSING_KEYCAP`].
    CombiningKeycap,
    /// `\p{Extended_Pictographic} | [\p{Emoji} -- regional indicators, keycap bases, accidental
    /// emoji, \p{Emoji_Modifier}]`.
    EmojiCharacter,
    /// `\p{Emoji_Modifier_Base}`.
    EmojiModifierBase,
    /// `\p{Emoji_Modifier}`.
    EmojiModifier,
    /// [`EMOJI_PRESENTATION_SELECTOR`].
    EmojiPresentationSelector,
    /// `\p{WB:ZWJ}`.
    ZeroWidthJoiner,
    /// [`TAG_SPECIFICATION_FIRST`] through [`TAG_SPECIFICATION_LAST`].
    TagSpecification,
    /// [`TAG_TERMINATOR`].
    TagTerminator,
    /// `[\p{Script:Hangul} && [\p{WB:ALetter}\p{WB:Hebrew_Letter}]]`.
    HangulLetter,
    /// `[\p{WB:ALetter}\p{WB:Hebrew_Letter}]`.
    Letter,
    /// `\p{WB:Hebrew_Letter}`.
    HebrewLetter,
    /// `\p{WB:Numeric}`.
    Numeric,
    /// `\p{WB:Katakana}`.
    Katakana,
    /// `[\p{WB:MidLetter}\p{WB:MidNumLet}\p{WB:Single_Quote}]`.
    MidLetter,
    /// `[\p{WB:MidNum}\p{WB:MidNumLet}\p{WB:Single_Quote}]`.
    MidNumeric,
    /// `\p{WB:ExtendNumLet}`.
    ExtendNumLet,
    /// `\p{WB:Single_Quote}`.
    SingleQuote,
    /// `\p{WB:Double_Quote}`.
    DoubleQuote,
    /// `\p{WB:Regional_Indicator}`.
    RegionalIndicator,
    /// `\p{LB:Complex_Context}`.
    ComplexContext,
    /// `\p{Script:Han}`.
    Han,
    /// `\p{Script:Hiragana}`.
    Hiragana,
}

impl CharacterSet {
    /// Whether the set holds `code_point`, whose properties are `class`.
    fn contains(self, code_point: u32, class: CodePointClass) -> bool {
        match self {
            Self::ExtendFormatZwj => class.is_extend_format_or_zwj(),
            Self::ExtendFormatZwjWithoutPresentationSelectors => {
                class.is_extend_format_or_zwj()
                    && code_point != TEXT_PRESENTATION_SELECTOR
                    && code_point != EMOJI_PRESENTATION_SELECTOR
            }
            Self::KeycapBase => is_keycap_base(code_point),
            Self::CombiningKeycap => code_point == COMBINING_ENCLOSING_KEYCAP,
            Self::EmojiCharacter => {
                class.has_flag(EXTENDED_PICTOGRAPHIC)
                    || (class.has_flag(EMOJI)
                        && class.word_break != WordBreak::RegionalIndicator
                        && !is_keycap_base(code_point)
                        && !ACCIDENTAL_EMOJI.contains(&code_point)
                        && !class.has_flag(EMOJI_MODIFIER))
            }
            Self::EmojiModifierBase => class.has_flag(EMOJI_MODIFIER_BASE),
            Self::EmojiModifier => class.has_flag(EMOJI_MODIFIER),
            Self::EmojiPresentationSelector => code_point == EMOJI_PRESENTATION_SELECTOR,
            Self::ZeroWidthJoiner => class.word_break == WordBreak::Zwj,
            Self::TagSpecification => (TAG_SPECIFICATION_FIRST..=TAG_SPECIFICATION_LAST).contains(&code_point),
            Self::TagTerminator => code_point == TAG_TERMINATOR,
            Self::HangulLetter => class.script == Script::Hangul && class.is_letter(),
            Self::Letter => class.is_letter(),
            Self::HebrewLetter => class.word_break == WordBreak::HebrewLetter,
            Self::Numeric => class.word_break == WordBreak::Numeric,
            Self::Katakana => class.word_break == WordBreak::Katakana,
            Self::MidLetter => matches!(
                class.word_break,
                WordBreak::MidLetter | WordBreak::MidNumLet | WordBreak::SingleQuote
            ),
            Self::MidNumeric => matches!(
                class.word_break,
                WordBreak::MidNum | WordBreak::MidNumLet | WordBreak::SingleQuote
            ),
            Self::ExtendNumLet => class.word_break == WordBreak::ExtendNumLet,
            Self::SingleQuote => class.word_break == WordBreak::SingleQuote,
            Self::DoubleQuote => class.word_break == WordBreak::DoubleQuote,
            Self::RegionalIndicator => class.word_break == WordBreak::RegionalIndicator,
            Self::ComplexContext => class.has_flag(COMPLEX_CONTEXT),
            Self::Han => class.script == Script::Han,
            Self::Hiragana => class.script == Script::Hiragana,
        }
    }
}

/// Whether `code_point` can open a keycap sequence: an ASCII digit, `#` or `*`.
const fn is_keycap_base(code_point: u32) -> bool {
    matches!(code_point, 0x30..=0x39 | 0x23 | 0x2A)
}

/// A regular pattern over character sets.
enum Pattern {
    /// One code point of the set.
    Set(CharacterSet),
    /// Each part in turn.
    Sequence(Vec<Self>),
    /// Any one alternative.
    Choice(Vec<Self>),
    /// The inner pattern any number of times, none included.
    ZeroOrMore(Box<Self>),
    /// The inner pattern at least once.
    OneOrMore(Box<Self>),
    /// The inner pattern at most once.
    Optional(Box<Self>),
}

/// One code point of `character_set`.
const fn match_set(character_set: CharacterSet) -> Pattern {
    Pattern::Set(character_set)
}

/// Each of `patterns` in turn.
const fn match_sequence(patterns: Vec<Pattern>) -> Pattern {
    Pattern::Sequence(patterns)
}

/// Any one of `patterns`.
const fn match_choice(patterns: Vec<Pattern>) -> Pattern {
    Pattern::Choice(patterns)
}

/// `pattern` any number of times, none included.
fn repeat_zero_or_more(pattern: Pattern) -> Pattern {
    Pattern::ZeroOrMore(Box::new(pattern))
}

/// `pattern` at least once.
fn repeat_one_or_more(pattern: Pattern) -> Pattern {
    Pattern::OneOrMore(Box::new(pattern))
}

/// `pattern` at most once.
fn repeat_at_most_once(pattern: Pattern) -> Pattern {
    Pattern::Optional(Box::new(pattern))
}

/// `X (Extend | Format | ZWJ)*`: UAX #29 rule WB4 applied to `X`.
fn match_extended_set(character_set: CharacterSet) -> Pattern {
    match_sequence(vec![
        match_set(character_set),
        repeat_zero_or_more(match_set(CharacterSet::ExtendFormatZwj)),
    ])
}

/// `X (Extend | Format | ZWJ)*` without the presentation selectors, for emoji sequences.
fn match_extended_set_without_presentation_selectors(character_set: CharacterSet) -> Pattern {
    match_sequence(vec![
        match_set(character_set),
        repeat_zero_or_more(match_set(CharacterSet::ExtendFormatZwjWithoutPresentationSelectors)),
    ])
}

/// `EmojiCharOrPresSeqOrModSeq`.
fn build_emoji_element() -> Pattern {
    match_choice(vec![
        match_sequence(vec![
            repeat_zero_or_more(match_set(CharacterSet::ZeroWidthJoiner)),
            match_extended_set_without_presentation_selectors(CharacterSet::EmojiCharacter),
            repeat_at_most_once(match_set(CharacterSet::EmojiPresentationSelector)),
        ]),
        match_sequence(vec![
            repeat_at_most_once(match_sequence(vec![
                repeat_zero_or_more(match_set(CharacterSet::ZeroWidthJoiner)),
                match_extended_set_without_presentation_selectors(CharacterSet::EmojiModifierBase),
            ])),
            match_extended_set_without_presentation_selectors(CharacterSet::EmojiModifier),
        ]),
    ])
}

/// The `<EMOJI>` rule: a joined element sequence, a tag sequence, a keycap or a flag.
fn build_emoji_rule() -> Pattern {
    match_choice(vec![
        match_sequence(vec![
            build_emoji_element(),
            match_choice(vec![
                repeat_zero_or_more(match_sequence(vec![
                    match_set(CharacterSet::ZeroWidthJoiner),
                    build_emoji_element(),
                ])),
                match_sequence(vec![
                    repeat_one_or_more(match_set(CharacterSet::TagSpecification)),
                    match_set(CharacterSet::TagTerminator),
                ]),
            ]),
        ]),
        match_sequence(vec![
            match_extended_set_without_presentation_selectors(CharacterSet::KeycapBase),
            repeat_at_most_once(match_set(CharacterSet::EmojiPresentationSelector)),
            match_extended_set_without_presentation_selectors(CharacterSet::CombiningKeycap),
        ]),
        match_sequence(vec![
            match_extended_set(CharacterSet::RegionalIndicator),
            match_extended_set(CharacterSet::RegionalIndicator),
        ]),
    ])
}

/// `NumericEx ( ( ExtendNumLetEx* | MidNumericEx ) NumericEx )*`.
fn build_number_run() -> Pattern {
    match_sequence(vec![
        match_extended_set(CharacterSet::Numeric),
        repeat_zero_or_more(match_sequence(vec![
            match_choice(vec![
                repeat_zero_or_more(match_extended_set(CharacterSet::ExtendNumLet)),
                match_extended_set(CharacterSet::MidNumeric),
            ]),
            match_extended_set(CharacterSet::Numeric),
        ])),
    ])
}

/// The `<NUM>` rule: a number run with any `ExtendNumLet` around it.
fn build_number_rule() -> Pattern {
    match_sequence(vec![
        repeat_zero_or_more(match_extended_set(CharacterSet::ExtendNumLet)),
        build_number_run(),
        repeat_zero_or_more(match_extended_set(CharacterSet::ExtendNumLet)),
    ])
}

/// One alternative of the word rule: a Katakana run, or letters, Hebrew quote forms and numbers.
fn build_word_body() -> Pattern {
    match_choice(vec![
        match_sequence(vec![
            match_extended_set(CharacterSet::Katakana),
            repeat_zero_or_more(match_sequence(vec![
                repeat_zero_or_more(match_extended_set(CharacterSet::ExtendNumLet)),
                match_extended_set(CharacterSet::Katakana),
            ])),
        ]),
        repeat_one_or_more(match_choice(vec![
            match_sequence(vec![
                match_extended_set(CharacterSet::HebrewLetter),
                match_choice(vec![
                    match_extended_set(CharacterSet::SingleQuote),
                    match_sequence(vec![
                        match_extended_set(CharacterSet::DoubleQuote),
                        match_extended_set(CharacterSet::HebrewLetter),
                    ]),
                ]),
            ]),
            build_number_run(),
            match_sequence(vec![
                match_extended_set(CharacterSet::Letter),
                repeat_zero_or_more(match_sequence(vec![
                    match_choice(vec![
                        repeat_zero_or_more(match_extended_set(CharacterSet::ExtendNumLet)),
                        match_extended_set(CharacterSet::MidLetter),
                    ]),
                    match_extended_set(CharacterSet::Letter),
                ])),
            ]),
        ])),
    ])
}

/// The `<ALPHANUM>` rule: word bodies joined by `ExtendNumLet`, with any around them.
fn build_word_rule() -> Pattern {
    match_sequence(vec![
        repeat_zero_or_more(match_extended_set(CharacterSet::ExtendNumLet)),
        build_word_body(),
        repeat_zero_or_more(match_sequence(vec![
            repeat_one_or_more(match_extended_set(CharacterSet::ExtendNumLet)),
            build_word_body(),
        ])),
        repeat_zero_or_more(match_extended_set(CharacterSet::ExtendNumLet)),
    ])
}

/// The rules in priority order: on equal length the earlier rule wins.
fn build_rules() -> Vec<(Pattern, TokenType)> {
    vec![
        (build_emoji_rule(), TokenType::Emoji),
        (build_number_rule(), TokenType::Num),
        (
            repeat_one_or_more(match_extended_set(CharacterSet::HangulLetter)),
            TokenType::Hangul,
        ),
        (
            repeat_one_or_more(match_extended_set(CharacterSet::Katakana)),
            TokenType::Katakana,
        ),
        (build_word_rule(), TokenType::AlphaNum),
        (
            repeat_one_or_more(match_extended_set(CharacterSet::ComplexContext)),
            TokenType::SoutheastAsian,
        ),
        (match_extended_set(CharacterSet::Han), TokenType::Ideographic),
        (match_extended_set(CharacterSet::Hiragana), TokenType::Hiragana),
    ]
}

/// A state of the compiled automaton.
enum State {
    /// Consumes one code point of `character_set`, then continues at `next_state`.
    Consume {
        /// The code points this state accepts.
        character_set: CharacterSet,
        /// The state after the consumed code point.
        next_state: usize,
    },
    /// Continues at every listed state without consuming.
    Split(Vec<usize>),
    /// The rule of priority `priority` accepts the input consumed so far.
    Accept {
        /// The rule's position in [`build_rules`]; lower wins a tie.
        priority: usize,
        /// The type of the token the rule yields.
        kind: TokenType,
    },
}

/// Working sets of one scan, reused across the tokens of one text.
///
/// A state belongs to the current set when its mark equals `generation`, so moving to the next
/// code point empties the set by advancing the generation instead of clearing every mark.
struct ScanBuffers {
    /// The generation each state was last added in.
    marks: Vec<u64>,
    /// The generation of the set being built.
    generation: u64,
    /// The states reached by the input consumed so far.
    active_states: Vec<usize>,
    /// The states the next code point reaches.
    next_states: Vec<usize>,
    /// The stack of states a closure has yet to visit.
    pending_states: Vec<usize>,
}

impl ScanBuffers {
    /// Empty working sets for an automaton of `state_count` states.
    fn new(state_count: usize) -> Self {
        Self {
            marks: vec![0; state_count],
            generation: 0,
            active_states: Vec::new(),
            next_states: Vec::new(),
            pending_states: Vec::new(),
        }
    }
}

/// The token rules compiled into one automaton.
struct Scanner {
    /// Every state; a state refers to another by its index here.
    states: Vec<State>,
    /// The state a scan starts at, which enters every rule.
    start_state: usize,
}

impl Scanner {
    /// Compiles the rules of [`build_rules`].
    fn new() -> Self {
        let mut states = Vec::new();
        let mut entries = Vec::new();
        for (priority, (pattern, kind)) in build_rules().into_iter().enumerate() {
            states.push(State::Accept { priority, kind });
            let accept_state = states.len() - 1;
            entries.push(compile_pattern(&mut states, &pattern, accept_state));
        }
        states.push(State::Split(entries));
        let start_state = states.len() - 1;
        Self { states, start_state }
    }

    /// Adds `state` and every state reachable from it without consuming to `target`, skipping
    /// states already marked in the current generation.
    fn add_state_closure(&self, state: usize, target: &mut Vec<usize>, buffers: &mut ScanBuffers) {
        buffers.pending_states.push(state);
        while let Some(reached_state) = buffers.pending_states.pop() {
            if buffers.marks[reached_state] == buffers.generation {
                continue;
            }
            buffers.marks[reached_state] = buffers.generation;
            target.push(reached_state);
            if let State::Split(targets) = &self.states[reached_state] {
                buffers.pending_states.extend(targets.iter().rev());
            }
        }
    }

    /// Returns the end index (exclusive) and the type of the token starting at `start_index`, or
    /// `None` when no rule accepts a non-empty prefix there.
    fn scan_token(
        &self,
        characters: &[ScannedCharacter],
        start_index: usize,
        buffers: &mut ScanBuffers,
    ) -> Option<(usize, TokenType)> {
        let mut active_states = std::mem::take(&mut buffers.active_states);
        let mut next_states = std::mem::take(&mut buffers.next_states);
        active_states.clear();
        buffers.generation += 1;
        self.add_state_closure(self.start_state, &mut active_states, buffers);
        let mut longest_match = None;
        let mut consumed_units = 0;
        for (index, character) in characters.iter().enumerate().skip(start_index) {
            consumed_units += character.utf16_length;
            if consumed_units > MAX_TOKEN_UTF16_UNITS {
                break;
            }
            buffers.generation += 1;
            next_states.clear();
            for &state in &active_states {
                if let State::Consume {
                    character_set,
                    next_state,
                } = self.states[state]
                    && character_set.contains(character.code_point, character.class)
                {
                    self.add_state_closure(next_state, &mut next_states, buffers);
                }
            }
            if next_states.is_empty() {
                break;
            }
            std::mem::swap(&mut active_states, &mut next_states);
            let accepted_rule = active_states
                .iter()
                .filter_map(|&state| match self.states[state] {
                    State::Accept { priority, kind } => Some((priority, kind)),
                    State::Consume { .. } | State::Split(_) => None,
                })
                .min_by_key(|&(priority, _)| priority);
            if let Some((_, kind)) = accepted_rule {
                longest_match = Some((index + 1, kind));
            }
        }
        buffers.active_states = active_states;
        buffers.next_states = next_states;
        longest_match
    }
}

/// Compiles `pattern` so that matching it continues at `next_state`; returns the entry state.
fn compile_pattern(states: &mut Vec<State>, pattern: &Pattern, next_state: usize) -> usize {
    match pattern {
        Pattern::Set(character_set) => {
            states.push(State::Consume {
                character_set: *character_set,
                next_state,
            });
            states.len() - 1
        }
        Pattern::Sequence(parts) => parts.iter().rev().fold(next_state, |continuation, part| {
            compile_pattern(states, part, continuation)
        }),
        Pattern::Choice(alternatives) => {
            let entries = alternatives
                .iter()
                .map(|alternative| compile_pattern(states, alternative, next_state))
                .collect();
            states.push(State::Split(entries));
            states.len() - 1
        }
        Pattern::Optional(inner_pattern) => {
            let entry = compile_pattern(states, inner_pattern, next_state);
            states.push(State::Split(vec![entry, next_state]));
            states.len() - 1
        }
        Pattern::ZeroOrMore(inner_pattern) => {
            states.push(State::Split(Vec::new()));
            let loop_state = states.len() - 1;
            let entry = compile_pattern(states, inner_pattern, loop_state);
            states[loop_state] = State::Split(vec![entry, next_state]);
            loop_state
        }
        Pattern::OneOrMore(inner_pattern) => {
            states.push(State::Split(Vec::new()));
            let loop_state = states.len() - 1;
            let entry = compile_pattern(states, inner_pattern, loop_state);
            states[loop_state] = State::Split(vec![entry, next_state]);
            entry
        }
    }
}

/// The automaton, compiled on first use and shared by every scan.
static SCANNER: LazyLock<Scanner> = LazyLock::new(Scanner::new);

/// One decoded code point of the input.
#[derive(Debug, Clone, Copy)]
struct ScannedCharacter {
    /// The UTF-8 byte offset of the code point in the text.
    byte_offset: usize,
    /// The code point.
    code_point: u32,
    /// The UTF-16 units the code point takes, which the scanner window counts.
    utf16_length: usize,
    /// The code point's properties.
    class: CodePointClass,
}

/// A token found by the scanner: UTF-8 byte offsets into the scanned text and its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ScannedToken {
    /// The UTF-8 byte offset of the token's first byte.
    pub(super) start: usize,
    /// The UTF-8 byte offset just past the token's last byte.
    pub(super) end: usize,
    /// The token's type.
    pub(super) kind: TokenType,
}

/// Splits `text` into tokens in input order.
pub(super) fn scan_tokens(text: &str) -> Vec<ScannedToken> {
    let characters: Vec<ScannedCharacter> = text
        .char_indices()
        .map(|(byte_offset, character)| ScannedCharacter {
            byte_offset,
            code_point: u32::from(character),
            utf16_length: character.len_utf16(),
            class: classify_code_point(u32::from(character)),
        })
        .collect();
    let scanner = &*SCANNER;
    let mut buffers = ScanBuffers::new(scanner.states.len());
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < characters.len() {
        match scanner.scan_token(&characters, index, &mut buffers) {
            Some((end_index, kind)) => {
                let end_byte = characters.get(end_index).map_or(text.len(), |character| character.byte_offset);
                tokens.push(ScannedToken {
                    start: characters[index].byte_offset,
                    end: end_byte,
                    kind,
                });
                index = end_index;
            }
            None => index += 1,
        }
    }
    tokens
}
