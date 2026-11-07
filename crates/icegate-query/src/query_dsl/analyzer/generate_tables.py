#!/usr/bin/env python3
"""Generate the Unicode tables of the standard analyzer.

Writes two Rust modules beside this script:

* ``word_break_tables.rs``: Word_Break, the Han/Hiragana/Hangul scripts,
  Line_Break=SA and the emoji properties at Unicode 12.1, merged into
  ranges of code points sharing every property;
* ``lowercase_table.rs``: the simple lowercase mapping (UnicodeData.txt
  field 13) at Unicode 16.0.

The source files are downloaded into a temporary directory unless
``--source-dir`` names a directory already holding them.  The output is
deterministic: the same sources always produce byte-identical modules.
"""

import argparse
import pathlib
import tempfile
import urllib.request

SOURCES = {
    "WordBreakProperty.txt": "https://www.unicode.org/Public/12.1.0/ucd/auxiliary/WordBreakProperty.txt",
    "LineBreak.txt": "https://www.unicode.org/Public/12.1.0/ucd/LineBreak.txt",
    "Scripts.txt": "https://www.unicode.org/Public/12.1.0/ucd/Scripts.txt",
    "emoji-data.txt": "https://www.unicode.org/Public/emoji/12.1/emoji-data.txt",
    "UnicodeData.txt": "https://www.unicode.org/Public/16.0.0/ucd/UnicodeData.txt",
}

# Word_Break values the tokenizer distinguishes; CR, LF, Newline and
# WSegSpace break like Other, so they map to it.
WORD_BREAK_VARIANTS = {
    "ALetter": "ALetter",
    "Hebrew_Letter": "HebrewLetter",
    "Numeric": "Numeric",
    "Katakana": "Katakana",
    "MidLetter": "MidLetter",
    "MidNumLet": "MidNumLet",
    "MidNum": "MidNum",
    "Single_Quote": "SingleQuote",
    "Double_Quote": "DoubleQuote",
    "ExtendNumLet": "ExtendNumLet",
    "Regional_Indicator": "RegionalIndicator",
    "Extend": "Extend",
    "Format": "Format",
    "ZWJ": "Zwj",
    "CR": "Other",
    "LF": "Other",
    "Newline": "Other",
    "WSegSpace": "Other",
}

SCRIPT_VARIANTS = {"Han": "Han", "Hiragana": "Hiragana", "Hangul": "Hangul"}

# Bit flags; the names are the constants the generated module declares.
FLAG_CONSTANTS = [
    ("COMPLEX_CONTEXT", "Line_Break=SA (Complex_Context)"),
    ("EMOJI", "Emoji"),
    ("EMOJI_MODIFIER", "Emoji_Modifier"),
    ("EMOJI_MODIFIER_BASE", "Emoji_Modifier_Base"),
    ("EXTENDED_PICTOGRAPHIC", "Extended_Pictographic"),
]
EMOJI_PROPERTY_FLAGS = {
    "Emoji": "EMOJI",
    "Emoji_Modifier": "EMOJI_MODIFIER",
    "Emoji_Modifier_Base": "EMOJI_MODIFIER_BASE",
    "Extended_Pictographic": "EXTENDED_PICTOGRAPHIC",
}
MAX_CODE_POINT = 0x10FFFF


def fetch_sources(directory):
    for name, url in SOURCES.items():
        with urllib.request.urlopen(url) as response:
            (directory / name).write_bytes(response.read())


def read_property_lines(path):
    """Yield (first, last, value) for each data line of a UCD property file."""
    for line in path.read_text(encoding="utf-8").splitlines():
        data = line.split("#", 1)[0].strip()
        if not data:
            continue
        fields = [field.strip() for field in data.split(";")]
        code_points = fields[0]
        if ".." in code_points:
            first, last = (int(part, 16) for part in code_points.split(".."))
        else:
            first = last = int(code_points, 16)
        yield first, last, fields[1]


def format_code_point(code_point):
    digits = f"{code_point:X}"
    if len(digits) <= 4:
        return "0x" + digits.rjust(4, "0")
    return f"0x{digits[:-4]}_{digits[-4:]}"


def build_property_ranges(directory):
    word_break = {}
    script = {}
    flags = {}
    for first, last, value in read_property_lines(directory / "WordBreakProperty.txt"):
        variant = WORD_BREAK_VARIANTS[value]
        for code_point in range(first, last + 1):
            word_break[code_point] = variant
    for first, last, value in read_property_lines(directory / "Scripts.txt"):
        if value in SCRIPT_VARIANTS:
            for code_point in range(first, last + 1):
                script[code_point] = SCRIPT_VARIANTS[value]
    for first, last, value in read_property_lines(directory / "LineBreak.txt"):
        if value == "SA":
            for code_point in range(first, last + 1):
                flags.setdefault(code_point, set()).add("COMPLEX_CONTEXT")
    for first, last, value in read_property_lines(directory / "emoji-data.txt"):
        if value in EMOJI_PROPERTY_FLAGS:
            for code_point in range(first, last + 1):
                flags.setdefault(code_point, set()).add(EMOJI_PROPERTY_FLAGS[value])

    ranges = []
    for code_point in range(MAX_CODE_POINT + 1):
        properties = (
            word_break.get(code_point, "Other"),
            script.get(code_point, "Other"),
            tuple(name for name, _ in FLAG_CONSTANTS if name in flags.get(code_point, ())),
        )
        if properties == ("Other", "Other", ()):
            continue
        if ranges and ranges[-1][1] == code_point - 1 and ranges[-1][2] == properties:
            ranges[-1][1] = code_point
        else:
            ranges.append([code_point, code_point, properties])
    return ranges


def render_flags(names):
    if not names:
        return "0"
    return " | ".join(names)


def write_word_break_tables(ranges, output):
    lines = [
        "//! Unicode 12.1 code point properties of the standard tokenizer.",
        "//!",
        "//! Generated by `generate_tables.py` from the Unicode 12.1.0 `WordBreakProperty.txt`,",
        "//! `LineBreak.txt` and `Scripts.txt` and the emoji 12.1 `emoji-data.txt`, including its",
        "//! reserved `Extended_Pictographic` ranges. Regenerate it with that script; never edit it by",
        "//! hand. A code point absent from [`CODE_POINT_RANGES`] has `Word_Break=Other`, no script of",
        "//! interest and no flag.",
        "",
        "/// The `Word_Break` values the tokenizer distinguishes; `CR`, `LF`, `Newline` and",
        "/// `WSegSpace` break like `Other`.",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub(super) enum WordBreak {",
    ]
    for variant in sorted(set(WORD_BREAK_VARIANTS.values()) | {"Other"}, key=word_break_order):
        lines.append(f"    /// `Word_Break={word_break_source_name(variant)}`.")
        lines.append(f"    {variant},")
    lines += [
        "}",
        "",
        "/// The scripts the tokenizer distinguishes.",
        "#[derive(Debug, Clone, Copy, PartialEq, Eq)]",
        "pub(super) enum Script {",
        "    /// Any script other than the three below.",
        "    Other,",
        "    /// `Script=Han`.",
        "    Han,",
        "    /// `Script=Hiragana`.",
        "    Hiragana,",
        "    /// `Script=Hangul`.",
        "    Hangul,",
        "}",
        "",
    ]
    for bit, (name, description) in enumerate(FLAG_CONSTANTS):
        lines.append(f"/// Flag: `{description}`.")
        lines.append(f"pub(super) const {name}: u8 = 0b{(1 << bit) >> 4:04b}_{(1 << bit) & 0xF:04b};")
    lines += [
        "",
        "/// Inclusive code point ranges sharing one `Word_Break` value, one script and one flag set,",
        "/// sorted and disjoint.",
        "pub(super) const CODE_POINT_RANGES: &[(u32, u32, WordBreak, Script, u8)] = &[",
    ]
    for first, last, (word_break, script, flag_names) in ranges:
        lines.append(
            f"    ({format_code_point(first)}, {format_code_point(last)}, WordBreak::{word_break}, "
            f"Script::{script}, {render_flags(flag_names)}),"
        )
    lines.append("];")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def word_break_order(variant):
    order = ["Other"] + [value for value in WORD_BREAK_VARIANTS.values() if value != "Other"]
    return order.index(variant)


def word_break_source_name(variant):
    if variant == "Other":
        return "Other"
    return next(name for name, value in WORD_BREAK_VARIANTS.items() if value == variant)


def write_lowercase_table(directory, output):
    mappings = []
    for line in (directory / "UnicodeData.txt").read_text(encoding="utf-8").splitlines():
        fields = line.split(";")
        if fields[13]:
            mappings.append((int(fields[0], 16), int(fields[13], 16)))
    lines = [
        "//! Unicode 16.0 simple lowercase mappings of the standard analyzer.",
        "//!",
        "//! Generated by `generate_tables.py` from field 13 of the Unicode 16.0.0 `UnicodeData.txt`.",
        "//! Regenerate it with that script; never edit it by hand. A code point absent from",
        "//! [`SIMPLE_LOWERCASE_MAPPINGS`] lowercases to itself.",
        "",
        "/// `(code point, simple lowercase)` pairs sorted by code point.",
        "pub(super) const SIMPLE_LOWERCASE_MAPPINGS: &[(u32, u32)] = &[",
    ]
    for upper, lower in mappings:
        lines.append(f"    ({format_code_point(upper)}, {format_code_point(lower)}),")
    lines.append("];")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-dir", type=pathlib.Path, help="directory already holding the source files")
    arguments = parser.parse_args()
    output_dir = pathlib.Path(__file__).resolve().parent
    with tempfile.TemporaryDirectory() as temporary:
        source_dir = arguments.source_dir
        if source_dir is None:
            source_dir = pathlib.Path(temporary)
            fetch_sources(source_dir)
        write_word_break_tables(build_property_ranges(source_dir), output_dir / "word_break_tables.rs")
        write_lowercase_table(source_dir, output_dir / "lowercase_table.rs")


if __name__ == "__main__":
    main()
