# icegate-query-grammar

Generated ANTLR parsers of IceGate's query languages, and the entry points that run them.
This crate holds no query semantics: turning a parse tree into a query is left to the
caller that walks it. `icegate-query` depends on it: the visitor in its
`query_dsl/lucene/visitor.rs` walks the Lucene tree into the query of a `query_string` leaf.

| Module | Start rule | Entry point |
|---|---|---|
| `lucene` | `topLevelQuery` | `parse_lucene_query` |
| `ppl` | `root` | `parse_ppl_root` |

Each entry point stops at the first syntax error and returns it as `SyntaxError`; the doc
comments of the entry point and of `SyntaxError` state where the error is reported and what
it carries.

`lucene` also exposes `measure_group_depth`, which runs only the lexer and answers how many
group levels the parser would recurse through, so a caller can refuse a deeply nested string
before parsing it. Its doc comment states what it counts.

## The Lucene query-string grammar

`src/lucene/LuceneLexer.g4` and `src/lucene/LuceneParser.g4` are a transcription of the
classic query parser of Apache Lucene,
`lucene/queryparser/src/java/org/apache/lucene/queryparser/classic/QueryParser.jj` at tag
`releases/lucene/10.5.1`, written onto the ANTLR Lucene grammar of `antlr/grammars-v4`
(`lucene/LuceneLexer.g4` and `lucene/LuceneParser.g4` at commit
`8af0d4c26c796ea27c15c3d85418f2d0f77c3adb`). Each ANTLR rule quotes the JavaCC rule it
transcribes.

Differences from the grammars-v4 files, which follow Lucene's flexible syntax:

- `@`, `<`, `>` and `=` are term characters: `user@example.com` is one term, and `age:>=10`
  is the field `age` with the term `>=10`; reading the range prefix from the term is left to
  the caller.
- The flexible syntax's `field>=v` rule and its `fn:` interval functions are removed.
- `*` and `?` produce wildcard and prefix terms as the classic lexer does. `STAR` (a lone
  `*`) is defined before `PREFIXTERM` (`foo*`), which is defined before `WILDTERM` (`f?o`,
  `*o`): of two equally long matches the earlier rule wins. The classic `PREFIXTERM`'s lone
  `"*"` alternative is removed because `STAR` always wins it.
- The range mode (`[a TO b]`, `{a TO b}`, mixed brackets, `*` for an open end) returns to the
  default mode after its closing bracket.
- The clause list is flat: `query : modClause conjoinedClause*` with
  `conjoinedClause : conjunction? modClause`, the classic `Query` production without the
  flexible syntax's operator precedence. Grouping adjacent bare terms into one multi-term
  clause and the occurrence of each clause (`addClause`) are left to the caller that walks
  the tree.
- `BAREOPER`: `+`, `-` or `!` followed by whitespace is a term (its first character).
- `FUZZY_SLOP` is one token: `~` followed by a number or by term characters.
- A `BOOST` lexer mode: a number is a `NUMBER` token only right after `^`, and anywhere else
  digits are a term. That mode skips no whitespace, so `a^ 2` fails.
- No fallthrough token: a character no rule matches (`a/b`, an unterminated quote) is a lexer
  error.

## Licences

The two grammar files carry both notices: the MIT licence of the grammars-v4 Lucene grammar
(Copyright (c) 2022 by Bart Kiers), and the attribution of Apache Lucene's `QueryParser.jj`
at `releases/lucene/10.5.1` (Copyright The Apache Software Foundation, Apache License 2.0),
whose JavaCC rules are quoted in comments. The generated Rust files are derived from the two
grammars.

## Regenerating a parser

The tool versions have owners elsewhere and are not repeated here:

- The generator is the antlr4rust ANTLR jar that `make -C crates/icegate-query/src/logql install`
  downloads into `target/antlr4.jar`; that Makefile's `install` target owns its version.
- The runtime is the workspace `antlr4rust` dependency, declared in the root `Cargo.toml` and
  locked in `Cargo.lock`; this crate takes it with `{ workspace = true }`.

Changing the generator or the runtime regenerates every parser built with them in the same
change: this crate's and the LogQL and TraceQL parsers of `icegate-query`.

From the repository root:

```sh
test -f target/antlr4.jar || make -C crates/icegate-query/src/logql install
make -C crates/icegate-query-grammar/src/lucene gen
```

`gen`, in `src/lucene/Makefile`, owns the exact commands. It runs the generator with the
visitor option, renames `LuceneParserParserContext` to `LuceneParserContext` in
`luceneparser.rs` (the committed parser compiles only with that name), prepends a lint
allowance to every generated `.rs` file because generated code is not held to the workspace
lints, and normalizes a single trailing newline on every generated file. Each step is part of
the committed output.

`src/lucene/mod.rs` is hand-written: it declares the generated modules under
`#![allow(missing_docs)]`, keeps `#[allow(dead_code)]` on the base listener and the base
visitor, which nothing uses, and holds the entry point. `rustfmt.toml` ignores the generated
files of `src/lucene/`.

Regeneration is byte-identical: after `make -C crates/icegate-query-grammar/src/lucene gen`,
`git status --short crates/icegate-query-grammar` lists no change unless a grammar changed.

## PPL

`src/ppl/` holds the PPL grammar of the OpenSearch SQL plugin, vendored unchanged, and the parser
generated from it. The grammar contract: every query the pinned grammar accepts parses.

| Pin | Value |
|---|---|
| Repository | `opensearch-project/sql` |
| Tag | `3.9.0.0` |
| Commit | the commit of that tag, held by `UPSTREAM_COMMIT` in `src/ppl/Makefile` |
| Grammar files | `ppl/src/main/antlr/OpenSearchPPLLexer.g4`, `ppl/src/main/antlr/OpenSearchPPLParser.g4` |
| Licence | Apache-2.0; `LICENSE.txt` and `NOTICE` from the repository root are kept beside the grammar |

`src/ppl/Makefile` (`vendor`) downloads from the commit its `UPSTREAM_COMMIT` names. The pin
follows the emulated OpenSearch version: a change of the default `opensearch.version`
(`crates/icegate-query/src/opensearch/config.rs`) re-vendors the grammar from the matching tag
in the same change.

### Regenerating

The generator and the runtime are those of "Regenerating a parser" above. From the repository
root:

```sh
make -C crates/icegate-query-grammar/src/ppl vendor
make -C crates/icegate-query-grammar/src/ppl gen
```

`vendor` downloads the grammar and licence files. `gen`, in `src/ppl/Makefile`, owns the exact
commands: the generator run, the context rename, the lint allowance on every generated `.rs`
file and the single trailing newline on every generated file. The generator prints one warning
(154) about `unionCommand`, which the pinned grammar carries.

`src/ppl/mod.rs` is hand-written: it declares the generated modules under
`#![allow(missing_docs)]`, keeps `#[allow(dead_code)]` on the base listener and the base
visitor, which nothing uses, and holds the entry point. `rustfmt.toml` ignores the generated
files of `src/ppl/`.

Regeneration is byte-identical: running `gen` twice produces the same files.

`icegate-query` builds its PPL grammar bundle (`opensearch/ppl/grammar_bundle.rs`) from
`OpenSearchPPLLexer.interp` and `OpenSearchPPLParser.interp`, so regenerating them changes the
bundle; re-vendoring re-checks that module's `ignoredTokens`, `rulesToVisit` and token-dictionary
lists against the new tag.
