# Query DSL

`query_dsl` turns the `query` of a search or count request body into one AST
(`ast.rs`, built by `parse.rs`) that every evaluator of the listener shares, and holds the
standard analyzer that full-text matching uses. How each leaf parses and what it refuses is
documented on `parse_query`; this file covers how the AST reaches data indices and the analyzer,
whose contract is a set of external rules rather than code.

## Lowering to a data-index predicate (`lower.rs`)

A data-index search lowers the AST to a DataFusion predicate with `lower_query`, once per index
it searches. A field typed differently in two indices is therefore read with each index's own
type, and a field one index lacks matches nothing there instead of failing the search. Field
names resolve through the index catalog, which looks a MAP key up by its exact stored key without
enumerating the map, so a key that is both a leaf and a prefix (`http` and `http.method`) filters
on the stored key it names. The predicate reads only the columns of the tenant-scoped scan, so it
cannot widen the tenant scope. What each leaf lowers to, and what it refuses, is documented on
`lower_query` and the function lowering that leaf.

## Full-text leaves and Lucene query strings (`full_text.rs`, `lucene/`)

`match`, `match_phrase`, `match_phrase_prefix`, `multi_match`, `simple_query_string` and
`query_string` describe text to look for in a set of fields. `full_text.rs` keeps that
description free of field types and resolves it per searched index into term-level leaves, so
the data path (`lower.rs`) and the system-index evaluator agree on what a query matches. The
per-field rules (analysis, leniency, MAP values, unmapped fields, the `query_string` time zone)
are stated once, in the module documentation of `full_text.rs`; the occurrence rules of a
`query_string` clause list are on `ClassicQuery`.

`query_string` is parsed by the Lucene grammar of `icegate-query-grammar` (its origin and its
differences from the upstream grammars are in that crate's
[README](../../../icegate-query-grammar/README.md)). `lucene/visitor.rs` turns the parse tree into
clause lists once per request, and the resolver applies the occurrence rules per index over the
clauses that survive analysis. What the leaf accepts and refuses, and the nesting limit, are
documented on `lucene::parse_query_string` and `lucene::translate_query_string`.

## The standard analyzer (`analyzer/`)

The analyzer splits text into tokens and lowercases them; no stop words are removed.
`tokenize_standard` is used wherever a leaf is compared with the tokens of a `text` field, and
`normalize_standard` (lowercasing without splitting) wherever a `prefix` or `wildcard` pattern,
or a `query_string` range bound, meets a `text` field.

### Parity target

IceGate's analyzer reproduces three pinned inputs:

- the token rules of Lucene's `StandardTokenizerImpl.jflex` at the release `LUCENE_VERSION`
  (`opensearch/node.rs`) names, the release `GET /` reports as `lucene_version`, transcribed into
  `analyzer/tokenizer.rs`;
- Unicode 12.1 word-break, line-break (`SA`), script and emoji properties;
- the Unicode 16.0 simple lowercase mapping (`UnicodeData.txt` field 13), applied one code point
  at a time: each code point lowercases to exactly one code point through the generated table.

The exact source files are listed in `SOURCES` of `analyzer/generate_tables.py`, whose URLs pin
their versions. A change of the default `opensearch.version` (`opensearch/config.rs`) re-derives
these pins.

### Token rules

The rules the curated table in `analyzer/mod.rs` is derived from; each row of that table names
the inputs it pins.

1. **Scanner window.** Each token is the longest match within the next `MAX_TOKEN_UTF16_UNITS`
   (`analyzer/tokenizer.rs`) UTF-16 units from the resume point, and scanning resumes right after
   it, so a longer run is cut into tokens of at most that length rather than dropped. A surrogate
   pair is never split. A code point that cannot start a token at the resume point is dropped.
   Among rules matching the same length the earlier rule wins, in the order emoji, number, Hangul,
   Katakana, word, Southeast Asian, ideograph, Hiragana.
2. U+202F NARROW NO-BREAK SPACE joins words; NO-BREAK SPACE, EN SPACE, THIN SPACE, OGHAM SPACE
   MARK, ZERO WIDTH SPACE and line breaks split them.
3. WORD JOINER, ZERO WIDTH NO-BREAK SPACE, SOFT HYPHEN, ZERO WIDTH NON-JOINER and ZERO WIDTH
   JOINER stay in the token text.
4. A letter followed by a skin-tone modifier is one `<ALPHANUM>` token; a leading ZERO WIDTH
   JOINER joins the emoji that follows; an emoji wins a tie with a word and is lowercased.
5. `<HANGUL>` is only a run of Hangul letters; Hangul mixed with other letters is `<ALPHANUM>`.
6. `_123` and `123_` are `<NUM>`; `々` is `<ALPHANUM>`, `〆` is dropped, `〇` is
   `<IDEOGRAPHIC>`; Tangut and Nushu are dropped; `ー` after Hiragana is a separate `<KATAKANA>`
   token.
7. `©`, `®`, `™`, U+3030 and U+303D are `<EMOJI>`; `#` before a digit is dropped; a lone or third
   regional indicator is dropped.
8. Letters, marks, digits and ideographs assigned after Unicode 12.1 are not word characters and
   split the word; code points that Unicode 12.1 reserves as `Extended_Pictographic` are
   `<EMOJI>`.

The curated table states UTF-16 offsets and converts in the test; `Token` documents the offsets
the analyzer keeps.

### Regenerating the tables

`analyzer/word_break_tables.rs` and `analyzer/lowercase_table.rs` are generated and committed;
never edit them by hand (`rustfmt.toml` excludes them from formatting). The generator downloads
its sources into a temporary directory, or reads local copies with `--source-dir`:

```bash
python3 -I crates/icegate-query/src/query_dsl/analyzer/generate_tables.py
git diff --exit-code crates/icegate-query/src/query_dsl/analyzer/
cargo test -p icegate-query --lib query_dsl::analyzer
```

Regenerating from unchanged sources changes nothing (the `git diff` above exits 0).

When the pins change: update `SOURCES` in the generator, regenerate, compare the new
`StandardTokenizerImpl.jflex` with the rules `tokenizer.rs` transcribes, and re-derive every row
of the curated table by hand from the token rules and the new property values (the generator's
input files state them). Expected tokens are never taken from the output of the new code.
