//! The character stream the PPL lexer reads.

use std::borrow::Cow;

use antlr4rust::char_stream::CharStream;
use antlr4rust::input_stream::InputStream;
use antlr4rust::int_stream::IntStream;

/// A character stream whose look-ahead is the simple upper case of each code point, while token
/// text keeps the characters as written.
///
/// The vendored grammar spells every keyword in upper case. Folding the look-ahead per code point
/// makes keyword matching follow the simple case mapping of each character (`ı` U+0131 reads as
/// `I`, `ſ` U+017F as `S`), and a character without a single-code-point upper case reads as
/// itself (`ß`). Token text comes from the underlying input, so identifiers and string literals
/// keep their original case.
pub(crate) struct UpperCaseCharStream<'input> {
    input: InputStream<&'input str>,
}

antlr4rust::tid! {UpperCaseCharStream<'a>}

impl<'input> UpperCaseCharStream<'input> {
    /// A stream over `text`.
    pub(crate) fn new(text: &'input str) -> Self {
        Self {
            input: InputStream::new(text),
        }
    }
}

/// The simple upper case of `code_point`: the single upper-case code point of its case mapping,
/// or the code point itself when its upper case is not a single code point.
///
/// Rust's `char::to_uppercase` is the full mapping. It differs from the simple mapping only where
/// the full mapping has several code points; of those, the Greek letters with a subscript iota
/// have a single-code-point simple mapping, which [`find_iota_subscript_upper_case`] supplies. The
/// result therefore equals the simple mapping for every code point.
fn fold_to_simple_upper_case(code_point: i32) -> i32 {
    let Some(character) = u32::try_from(code_point).ok().and_then(char::from_u32) else {
        return code_point;
    };
    if let Some(folded_char) = find_iota_subscript_upper_case(character) {
        return folded_char as i32;
    }
    let mut upper_case_chars = character.to_uppercase();
    match (upper_case_chars.next(), upper_case_chars.next()) {
        (Some(upper_case_char), None) => upper_case_char as i32,
        _ => code_point,
    }
}

/// The simple upper case of a Greek letter with a subscript iota, whose full upper case is two
/// code points; `None` for every other character.
const fn find_iota_subscript_upper_case(character: char) -> Option<char> {
    let code_point = character as u32;
    let folded_code_point = match code_point {
        0x1F80..=0x1F87 | 0x1F90..=0x1F97 | 0x1FA0..=0x1FA7 => code_point + 8,
        0x1FB3 | 0x1FC3 | 0x1FF3 => code_point + 9,
        _ => return None,
    };
    char::from_u32(folded_code_point)
}

impl IntStream for UpperCaseCharStream<'_> {
    fn consume(&mut self) {
        self.input.consume();
    }

    fn la(&mut self, offset: isize) -> i32 {
        fold_to_simple_upper_case(self.input.la(offset))
    }

    fn mark(&mut self) -> isize {
        self.input.mark()
    }

    fn release(&mut self, marker: isize) {
        self.input.release(marker);
    }

    fn index(&self) -> isize {
        self.input.index()
    }

    fn seek(&mut self, index: isize) {
        self.input.seek(index);
    }

    fn size(&self) -> isize {
        self.input.size()
    }

    fn get_source_name(&self) -> String {
        self.input.get_source_name()
    }
}

impl<'input> CharStream<Cow<'input, str>> for UpperCaseCharStream<'input> {
    fn get_text(&self, start: isize, stop: isize) -> Cow<'input, str> {
        self.input.get_text(start, stop)
    }
}

#[cfg(test)]
mod tests {
    use super::fold_to_simple_upper_case;

    /// The code point of `character` as the lexer's look-ahead reports it.
    fn read_code_point(character: char) -> i32 {
        i32::try_from(u32::from(character)).expect("every char fits an i32")
    }

    #[test]
    fn every_char_folds_to_its_single_code_point_upper_case_or_to_itself() {
        // An invariant check over the full mapping: where a char's full upper case is a single
        // code point, the fold equals it; any other char reads as itself, except the 27 Greek
        // letters with a subscript iota (U+1F80–U+1F87, U+1F90–U+1F97, U+1FA0–U+1FA7, U+1FB3,
        // U+1FC3, U+1FF3), whose fold is the titlecase letter that lower-cases back to them.
        let mut iota_subscript_count = 0;
        for character in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
            let folded_char = u32::try_from(fold_to_simple_upper_case(read_code_point(character)))
                .ok()
                .and_then(char::from_u32)
                .expect("a char folds to a char");
            let full_upper_case: Vec<char> = character.to_uppercase().collect();
            if let [single_upper_case] = full_upper_case.as_slice() {
                assert_eq!(folded_char, *single_upper_case, "U+{:04X}", u32::from(character));
            } else if folded_char != character {
                assert_eq!(full_upper_case.len(), 2, "U+{:04X}", u32::from(character));
                assert_eq!(
                    folded_char.to_lowercase().collect::<String>(),
                    character.to_string(),
                    "U+{:04X}",
                    u32::from(character)
                );
                iota_subscript_count += 1;
            }
        }
        assert_eq!(iota_subscript_count, 27);
        // The end of input and a surrogate are no chars; they read as themselves.
        assert_eq!(fold_to_simple_upper_case(-1), -1);
        assert_eq!(fold_to_simple_upper_case(0xD800), 0xD800);
    }
}
