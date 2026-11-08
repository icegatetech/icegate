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
| `eval <field> = <expression>, …` | Applies the assignments in order, each seeing the fields of the ones before it. An assignment to an existing field replaces it in place; a new field is appended. |
| `rename <field> as <name>, …` | Applies the pairs in order; each field keeps its position under its new name, and a field already holding the new name is removed. A wildcard pattern or `*` is a 400. |

The output columns of a source are the table's columns in table order. A field reference is
resolved against the columns of the pipeline at that point, case-sensitively; a name that
matches none is a 400 `FIELD_NOT_FOUND`, except for the promoted resource attributes below.

## Fields and types

A field reference resolves, in this order, to a lambda parameter in scope, the column of that
exact name, a resource attribute the source's profile promotes to a column, or a key of a MAP
column written `<column>.<key>` (the key may itself contain dots; an absent key is NULL).
A promoted resource attribute whose column an earlier command removed, renamed, or replaced
(a `rename` onto its name, an `eval` assigning it) is a 400 `SEMANTIC_ERROR`, and so is a
promoted attribute that the source indices disagree on (a column in some, not in others).
Several source indices are combined with UNION ALL over the union of their columns, in the order
the columns first appear; a column an index lacks is NULL in its rows, and a column typed
differently across the indices is a 400.

- Decimal columns read as `double`, the one numeric type PPL reports for them.
- Strings compare and sort byte-wise (code-point order), never by locale.
- A fixed-size binary id renders as lowercase hex; comparing it with a value of another type is a
  400, because the engine would compare bytes, not the hex the client sees.
- Timestamps are microseconds in UTC. A timestamp literal is `yyyy-MM-dd`, or
  `yyyy-MM-dd HH:mm:ss` (a `T` may separate date and time) with a fraction no finer than a
  microsecond; any other form, and a finer fraction, is a 400.

## Expressions

| Form | Contract |
|---|---|
| `=`, `!=`, `<`, `<=`, `>`, `>=` | NULL on either side is unknown. |
| `<value> like <pattern>` | The same as `like(value, pattern)`. |
| `and`, `or`, `xor`, `not` | Three-valued; `xor` is true when exactly one side is true. |
| `<value> [not] in (<literal>, …)` | Membership in the literal list. |
| `<value> [not] between <low> and <high>` | Both bounds inclusive. |
| `<value> is [not] null` | Never NULL. |
| `+`, `-`, `*`, `/`, `%` | `*`, `/` and `%` bind tighter than `+` and `-`. Integer `/` truncates toward zero; `/` and `%` by zero are NULL. |
| `case(<condition>, <value>, … [else <value>])` | The value of the first true condition, else the `else` value, else NULL. |
| `cast(<value> as <type>)` | Types `double` (`float`), `long`, `int` (`integer`), `string`, `boolean`, `timestamp`. A timestamp cast to `string` renders as the output does. Another type is a 400 naming the function `cast as <type>`. |

## Functions

`functions.rs` holds the library; a function outside it is a 400 naming it. Unless a row says
otherwise, a NULL argument gives NULL, and a wrong number of arguments is a 400.

| Function | Contract |
|---|---|
| `if(c, a, b)` | `a` when `c` is true, else `b`. |
| `coalesce(a, …)`, `ifnull(a, b)`, `nullif(a, b)` | The first non-NULL argument; `a` unless NULL then `b`; NULL when `a = b`, else `a`. |
| `isnull(a)`, `isnotnull(a)` | Never NULL. |
| `concat(a, …)` | NULL when any argument is NULL; other types are cast to strings. |
| `lower`, `upper`, `trim`, `ltrim`, `rtrim` | Unicode case mapping; trimming removes spaces. |
| `substring(s, start[, length])`, `substr` | 1-based. |
| `length(s)` | Length in bytes of the UTF-8 encoding. |
| `locate(sub, s)` | 1-based position of the first occurrence, 0 when absent; case-sensitive. The three-argument form is refused. |
| `like(s, pattern[, case_sensitive])` | `%` matches any run, `_` one character, `\` escapes; without the third argument the match ignores case. The third argument is the literal `true` or `false`. |
| `abs`, `mod(a, b)` | `mod` by zero is NULL. |
| `floor`, `ceil` (`ceiling`) | A `bigint`. |
| `round(x[, digits])` | A `double`. |
| `md5(s)` | Lowercase hex. |
| `timestamp(v)` | A string literal folds to a timestamp literal, so a comparison with a timestamp column can prune scans. Any other string is cast, and a value that is not a timestamp fails the query at execution with a 500 `EXECUTION_ERROR`. A number is a 400; use `from_unixtime`. |
| `timestampdiff(unit, start, end)` | `end - start` in whole units, truncated toward zero, as a `bigint`. Units `MICROSECOND`, `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`; calendar units are a 400. |
| `date_format(v, format)` | `format` is a string literal of MySQL-style specifiers, rendered in UTC: `%Y %y %m %c %d %e %H %k %h %I %l %i %s %S %f %p %M %b %W %a %j %T %r %%`. Another specifier is a 400. |
| `now()` | The request time, taken once per request. |
| `unix_timestamp([v])` | Seconds since the epoch as a `double`; without an argument, of the request time. |
| `from_unixtime(seconds[, format])` | The timestamp `seconds` after the epoch, rounded to the microsecond; with a format, rendered as `date_format` does. |
| `array_length(list)` | 0 for an empty list. |
| `mvjoin(list, separator)` | The elements joined; an empty list gives an empty string. |
| `mvindex(list, index[, end])` | 0-based integer-literal indexes, negative ones counting from the end; with `end`, the elements from `index` to `end` inclusive. A single index outside the list is NULL; a range over the empty list is the empty list. |
| `exists(list, x -> p)`, `forall(list, x -> p)` | Whether any / every element satisfies `p`; `false` / `true` for an empty list. |
| `filter(list, x -> p)`, `transform(list, x -> e)` | The elements satisfying `p`; each element mapped by `e`. |

Array functions return NULL for a NULL list. `exists`, `forall`, `filter` and `transform` take a
list and a lambda of one parameter; another first or second argument, and a lambda anywhere
else, is a 400.

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
