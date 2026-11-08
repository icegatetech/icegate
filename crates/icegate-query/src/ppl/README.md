# PPL

PPL (piped processing language) is the query language of `POST /_plugins/_ppl` on the
OpenSearch listener. A query is a source followed by commands joined by `|`; each command
transforms the rows of the commands before it.

```text
source=logs-* | where severity_text = 'ERROR' | fields timestamp, service_name, body | sort - timestamp | head 20
```

## How a query runs

1. The vendored grammar of the `icegate-query-grammar` crate (`src/ppl/`; the `PPL` section of
   that crate's README states the pinned version and how to regenerate it) parses the text. Keywords match in any case; names keep their case.
2. `build.rs` walks the parse tree into the AST of `ast.rs`. Every command, function and
   argument IceGate does not plan is refused there by name, so an unplanned construct is a 400
   that names it, never a different query.
3. `plan.rs` lowers the AST to one DataFusion logical plan over the tenant's tables, through the
   tenant's `TenantSession`: the tenant scope, index expressions, name resolution and the
   per-query memory limit are those of every other front end of the listener. The analyzer runs
   while planning, so a type error is the query's 400, not a failure during execution.
4. `opensearch/ppl/` executes the plan and renders the jdbc response, or the error envelope.

## Request

The body is a JSON object with a string `query` and an optional integer `fetch_size`. Every other
key is accepted and ignored; the query text alone decides the result. `fetch_size` 0 bounds
nothing; N ≥ 1 keeps the first N rows of the final output, as a trailing `| head N` does. The
`format` query parameter accepts `jdbc` (the default) only; every other value is a 400. A
request that cannot be read is refused in the listener's 400 `illegal_argument_exception`
envelope, which names the offending `key` (and its `value`, when the body gave one) or `format`.

## Response

`{schema: [{name, type}], datarows, total, size}`, where `total` and `size` both count the rows
returned. `schema` lists the output columns in order with their PPL types (`string`, `int`,
`bigint`, `double`, `boolean`, `timestamp`, `date`, `struct`, `array`). `render.rs` states how
each cell renders: timestamps in UTC as `yyyy-MM-dd HH:mm:ss[.f]` with trailing zeros dropped,
fixed-size binary ids as lowercase hex, a MAP column as one object keyed by its stored keys (a
dotted key is never split), `NaN` and the infinities as strings.

## Sources

`search source=<indices>`, `source=<indices>` and `index=<indices>` are the same source. The
index list is an index expression: comma-separated names, `*` wildcards and `-` exclusions,
resolved like the `_search` path with `ignore_unavailable=false`, `allow_no_indices=false` and
open, non-hidden indices. A name that matches nothing is a 404 `INDEX_NOT_FOUND`; names are
case-sensitive. A source on system indices is a 400.

## Commands

| Command | Contract |
|---|---|
| `where <condition>` | Keeps the rows whose condition is true. A comparison with NULL is unknown, and an unknown row is dropped (three-valued logic). |
| `fields [+\|-] <field>, …` | `+` (or no sign) keeps the named fields in the order named; `-` drops them and keeps the rest in their order. `*` inside a name is a wildcard. |
| `sort [+\|-]<field> [asc\|desc], …` | Sorts by each key in turn; `-` or `desc` sorts descending. NULL sorts first ascending and last descending. A key with both a sign and a direction is a 400. |
| `head [N] [from M]` | Skips M rows, then keeps N (`DEFAULT_HEAD_COUNT` in `build.rs` when N is omitted). |

The output columns of a source are the table's columns in table order. A field reference is
resolved against the columns of the pipeline at that point, case-sensitively; a name that
matches none is a 400 `FIELD_NOT_FOUND`.

## Limits

`opensearch.ppl.max_result_rows` (`PplLimitsConfig` in `opensearch/config.rs`) bounds the rows a
query returns, after `fetch_size`. An output with more rows is a 400 `RESOURCE_LIMIT_EXCEEDED`
naming the limit, never a truncated answer; the plan fetches one row beyond the limit, so the
check reads no more than that. The per-query memory limit, the query deadline and load
shedding of the listener bound every PPL query as they bound every other request.

## Errors

A PPL error is answered with content type `text/plain; charset=UTF-8` and the JSON body
`{status, error: {type, reason, details, code, context?}}`:

| Cause | Status | `type` | `code` | `context` |
|---|---|---|---|---|
| Text the grammar rejects, or no source | 400 | `SyntaxCheckException` | `SYNTAX_ERROR` | `{line, column}` of the offending token |
| A source that names no index | 404 | `IndexNotFoundException` | `INDEX_NOT_FOUND` | `{index}` |
| A field reference that matches no column | 400 | `SemanticCheckException` | `FIELD_NOT_FOUND` | `{field}` |
| A command IceGate does not plan | 400 | `UnsupportedOperationException` | `UNSUPPORTED_OPERATION` | `{command}` |
| A function IceGate does not plan | 400 | `UnsupportedOperationException` | `UNSUPPORTED_OPERATION` | `{function}` |
| A well-formed but invalid query (types, arguments, system indices) | 400 | `SemanticCheckException` | `SEMANTIC_ERROR` | — |
| More output rows than the row limit | 400 | `IllegalArgumentException` | `RESOURCE_LIMIT_EXCEEDED` | `{limit}` |
| More memory than the per-query limit | 400 | `IllegalArgumentException` | `RESOURCE_LIMIT_EXCEEDED` | `{limit_bytes}` |
| A failure inside IceGate | 500 | `IllegalStateException` | `EXECUTION_ERROR` | — |

A syntax error's `reason` is fixed and its `details` name the offending token; a 500's `reason`
is fixed and its `details` carry the cause, which is also logged. A shed request, a passed
deadline or write contention keeps the listener's 503 envelope, and a request body that cannot
be read keeps its 400 `illegal_argument_exception` envelope, so the listener has one retry
contract for every route.

## Tenant isolation

Every source resolves through the requesting tenant's session, whose scan carries the tenant
filter, so a PPL query never reads another tenant's rows. `tests/opensearch/ppl.rs` pins this
with two tenants written in the same batch.
