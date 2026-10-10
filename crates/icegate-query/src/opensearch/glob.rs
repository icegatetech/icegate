//! Shell-style glob matching.

/// Whether `text` matches `pattern`, where `*` matches any run of characters (none included) and
/// `?` exactly one character; every other character matches itself.
///
/// Matching is per `char`, so `?` matches a multibyte character whole. It walks both strings by
/// byte offset on character boundaries and backtracks only to the last `*`, so it allocates
/// nothing and costs `O(pattern × text)` in the worst case.
pub(crate) fn matches_glob(pattern: &str, text: &str) -> bool {
    let (mut pattern_offset, mut text_offset) = (0, 0);
    // The pattern offset just after the last `*`, and the text offset that `*` was last tried at.
    let mut last_star_offsets: Option<(usize, usize)> = None;
    while let Some(text_character) = text[text_offset..].chars().next() {
        match pattern[pattern_offset..].chars().next() {
            Some('*') => {
                pattern_offset += 1;
                last_star_offsets = Some((pattern_offset, text_offset));
            }
            Some(pattern_character) if pattern_character == '?' || pattern_character == text_character => {
                pattern_offset += pattern_character.len_utf8();
                text_offset += text_character.len_utf8();
            }
            Some(_) | None => {
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
    }
    pattern[pattern_offset..]
        .chars()
        .all(|pattern_character| pattern_character == '*')
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng as _};

    use super::matches_glob;

    #[test]
    fn star_matches_any_run_and_question_mark_one_character() {
        let cases = [
            ("*", "", true),
            ("*", "logs", true),
            ("l*s", "logs", true),
            ("l*s", "ls", true),
            ("l*s", "logz", false),
            ("l?gs", "logs", true),
            ("l?gs", "lgs", false),
            ("l?gs", "lögs", true),
            ("*.version", "nodes.version", true),
            ("logs", "logs", true),
            ("logs", "logs2", false),
            ("a*b*c", "aXbYc", true),
            ("a*b*c", "aXbY", false),
            ("**", "", true),
            ("", "", true),
            ("", "a", false),
        ];
        for (pattern, text, expected_match) in cases {
            assert_eq!(
                matches_glob(pattern, text),
                expected_match,
                "{pattern:?} against {text:?}"
            );
        }
    }

    /// Characters a drawn text is made of: one-, two- and three-byte UTF-8, so a matcher that
    /// steps by bytes instead of characters splits one and disagrees with the reference.
    const TEXT_ALPHABET: [char; 4] = ['a', 'b', 'ö', '日'];
    /// Characters a drawn pattern is made of: the text characters and both wildcards.
    const PATTERN_ALPHABET: [char; 6] = ['a', 'b', 'ö', '日', '*', '?'];
    /// Number of drawn pattern and text pairs.
    const CASE_COUNT: usize = 10_000;

    /// The glob rule stated directly: `*` tries every split of the text, `?` takes one character,
    /// any other character must be equal. Exponential, which is harmless at the drawn lengths.
    fn matches_glob_naively(pattern: &[char], text: &[char]) -> bool {
        match pattern.split_first() {
            None => text.is_empty(),
            Some(('*', pattern_rest)) => {
                (0..=text.len()).any(|skipped_count| matches_glob_naively(pattern_rest, &text[skipped_count..]))
            }
            Some((pattern_character, pattern_rest)) => text.split_first().is_some_and(|(text_character, text_rest)| {
                (*pattern_character == '?' || pattern_character == text_character)
                    && matches_glob_naively(pattern_rest, text_rest)
            }),
        }
    }

    /// Up to `max_length` characters drawn from `alphabet`, for a pattern or a text.
    fn draw_characters(generator: &mut StdRng, alphabet: &[char], max_length: usize) -> String {
        let length = generator.random_range(0..=max_length);
        (0..length)
            .map(|_| alphabet[generator.random_range(0..alphabet.len())])
            .collect()
    }

    /// A text `pattern` matches: each `*` replaced by up to two drawn characters, each `?` by one.
    fn instantiate_pattern(generator: &mut StdRng, pattern: &str) -> String {
        pattern
            .chars()
            .map(|character| match character {
                '*' => draw_characters(generator, &TEXT_ALPHABET, 2),
                '?' => TEXT_ALPHABET[generator.random_range(0..TEXT_ALPHABET.len())].to_string(),
                literal => literal.to_string(),
            })
            .collect()
    }

    /// Half of the texts instantiate their pattern and half are drawn freely, so both outcomes are
    /// exercised; the counts prove it.
    #[test]
    fn matching_agrees_with_a_naive_reference_on_seeded_patterns_and_texts() {
        let mut generator = StdRng::seed_from_u64(0x1CE6_A7E0);
        let mut matched_count = 0;
        for _ in 0..CASE_COUNT {
            let pattern = draw_characters(&mut generator, &PATTERN_ALPHABET, 6);
            let text = if generator.random_bool(0.5) {
                instantiate_pattern(&mut generator, &pattern)
            } else {
                draw_characters(&mut generator, &TEXT_ALPHABET, 8)
            };
            let pattern_characters: Vec<char> = pattern.chars().collect();
            let text_characters: Vec<char> = text.chars().collect();
            let expected_match = matches_glob_naively(&pattern_characters, &text_characters);

            assert_eq!(
                matches_glob(&pattern, &text),
                expected_match,
                "{pattern:?} against {text:?}"
            );
            matched_count += usize::from(expected_match);
        }
        assert!(matched_count >= CASE_COUNT / 4, "{matched_count} matches");
        assert!(CASE_COUNT - matched_count >= CASE_COUNT / 4, "{matched_count} matches");
    }
}
