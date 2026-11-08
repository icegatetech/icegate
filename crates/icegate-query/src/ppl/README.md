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
case-sensitive. A source on system indices is a 400. `describe <index>` is a source of its own:
see [`describe`](#describe).

## Commands

| Command | Contract |
|---|---|
| `where <condition>` | Keeps the rows whose condition is true. A comparison with NULL is unknown, and an unknown row is dropped (three-valued logic). |
| `fields [+\|-] <field>, …` | `+` (or no sign) keeps the named fields in the order named; `-` drops them and keeps the rest in their order. `*` inside a name is a wildcard. |
| `sort [+\|-]<field> [asc\|desc], …` | Sorts by each key in turn; `-` or `desc` sorts descending. NULL sorts first ascending and last descending. A key with both a sign and a direction is a 400. |
| `head [N] [from M]` | Skips M rows, then keeps N (`DEFAULT_HEAD_COUNT` in `build.rs` when N is omitted). |
| `eval <field> = <expression>, …` | Applies the assignments in order, each seeing the fields of the ones before it. An assignment to an existing field replaces it in place; a new field is appended. |
| `rename <field> as <name>, …` | Applies the pairs in order; each field keeps its position under its new name, and a field already holding the new name is removed. A wildcard pattern or `*` is a 400. |
| `stats <aggregation>, … [by [span(…)] <field>, …]` | See [Aggregations](#aggregations). |
| `top [N] <field>, … [by <field>, …]`, `rare …` | Per group, the N most (`top`) or least (`rare`) frequent value combinations with their count. See [Ranking](#ranking). |
| `dedup [N] <field>, … [keepempty=true]` | Keeps the first N rows (`DEFAULT_DEDUP_COUNT` in `build.rs` when N is omitted) of each combination of the fields, in the incoming order. Rows with a NULL field are dropped, or with `keepempty=true` all kept. `consecutive=true` and N below 1 are a 400. |
| `timechart [span=<length><unit>] [limit=N] [useother=bool] [timefield=<field>] <aggregation> [by <field>]` | See [Timechart](#timechart). |
| `bin <field> span=<length>[<unit>]` | See [Bin](#bin). |
| `parse <field> '<pattern>'` | See [`parse`](#parse). |
| `fillnull with <v> [in <fields>]`, `fillnull using <f> = <v>, …`, `fillnull value=<v> [<fields>]` | See [`fillnull`](#fillnull). |

The output columns of a source are the table's columns in table order. A field reference is
resolved against the columns of the pipeline at that point, case-sensitively; a name that
matches none is a 400 `FIELD_NOT_FOUND`, except for the promoted resource attributes below.

## Fields and types

A field reference resolves, in this order, to a lambda parameter in scope, the column of that
exact name, a resource attribute the source's profile promotes to a column, or a key of a MAP
column written `<column>.<key>` (the key may itself contain dots; an absent key is NULL).
A field name that starts with `RESERVED_NAME_PREFIX` of `plan.rs`, the prefix of the plans' own
columns, is a 400 `SEMANTIC_ERROR` naming it wherever the query writes it: a reference, a
`fields` pattern, an `eval` or `rename` target, an alias, `timefield` or `countfield`.
A promoted resource attribute whose column an earlier command removed, renamed, or replaced
(a `rename` onto its name, an `eval`, `bin`, `parse` or `fillnull` writing it) is a 400
`SEMANTIC_ERROR`, and so is a
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
  microsecond; any other form, and a finer fraction, is a 400. See
  [Timestamp literals](#timestamp-literals).

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

## Aggregations

`stats` returns one row per group: the aggregations in order, then the `span` key, then the
`by` fields, ordered by the keys in that order, ascending with NULL first. Without keys it
returns exactly one row, also for no input rows. An aggregation's column is named by its alias,
or by its text as written (`count()`).

| Aggregation | Contract |
|---|---|
| `count()` | The number of rows. |
| `count(x)`, `sum`, `avg`, `min`, `max` | Skip NULL; `min` and `max` compare strings byte-wise. |
| `count(eval(<predicate>))` | The number of rows whose predicate is true. |
| `dc(x)`, `distinct_count(x)` | The exact number of distinct non-NULL values. |
| `values(x)` | The distinct non-NULL values, sorted, with no cap; an empty list for a group without one. |
| `percentile(x, p)`, `percentile_approx(x, p)`, `p<N>(x)`, `perc<N>(x)`, `median(x)` | An estimate of the `p`th percentile (`p` in 0..=100, `median` is 50) as a `double`; `tests/opensearch/ppl.rs` states the tolerance it is held to. |

`stats` options: `bucket_nullable=false` drops the rows whose `by` key or `span` key is NULL (by
default they form a group); `partitions`, `allnum`, `delim` and `dedup_splitvalues` are a 400. A
row whose time-`span` field is NULL is always dropped, so a time-span column is never NULL; a
numeric span over a NULL value is a NULL key like any other. Every other aggregation (`take`
among them) is a 400 naming it.

### Spans

`span(<field>, <length><unit>) [as <alias>]` buckets a field; its default column name is
`span(<field>,<length><unit>)`.

- A numeric span of an integer field truncates toward zero (`(x / n) * n`); of any other numeric
  field it floors (`floor(x / n) * n`). A unit on a numeric field, and a whole-number length on
  an integer field beyond the 64-bit integer range, are a 400.
- A time span takes a positive whole length and a unit. Fixed units (milliseconds to days) bucket
  from the Unix epoch; weeks start on Mondays; months, quarters and years start on the first day
  of their calendar period counted from January 1970; all in UTC. A fractional length, a zero
  length, a length above `MAX_EXACT_SPAN_LENGTH` in `build.rs`, a length whose bucket width
  (in microseconds, or in months for months, quarters and years), doubled, overflows the 64-bit
  range, and a bare number on a timestamp are a 400.
- Units: `M` is months and `m` minutes, matched case-sensitively; every other spelling matches in
  any case: `ms`, `millisecond`; `s`, `sec`, `secs`, `second`, `seconds`; `min`, `mins`,
  `minute`, `minutes`; `h`, `hr`, `hrs`, `hour`, `hours`; `d`, `day`, `days`; `w`, `week`,
  `weeks`; `mon`, `month`, `months`; `q`, `qtr`, `qtrs`, `quarter`, `quarters`; `y`, `year`,
  `years`. The pinned lexer also admits `us`, `cs`, `ds`, `milliseconds`, `yr` and `yrs` in a
  span length; no span buckets by them, so they are a 400 naming the spelling.

### Ranking

`top` and `rare` count each combination of the fields per group of the `by` fields and keep the
N most or least frequent (default `DEFAULT_RANK_COUNT` in `build.rs`). Ties rank by the field
values, ascending with NULL last. Output: the `by` fields, the fields, then the count column
(`countfield`, default `DEFAULT_COUNT_FIELD` in `build.rs`; `showcount=false` hides it), rows
ordered by the `by` fields, then rank. NULL values form a combination unless `usenull=false`.
A `countfield` that names an existing column is a 400, and so are `showperc`, `percentfield`
and N below 1.

### Timechart

`timechart` aggregates per time bucket and series. Its columns are the time field, the `by`
field when given, then the aggregation; rows are ordered by time, then series with NULL last;
empty buckets are not filled. The time field is `timefield`, or the implicit one
(`IMPLICIT_TIME_FIELD` in `build.rs`), which a native index does not have: there the query
renames its time column or names `timefield`, and otherwise gets the 400 `FIELD_NOT_FOUND`
naming the implicit field. `span` takes the span units above; without it the span is
`DEFAULT_TIMECHART_SPAN_LENGTH` units of `DEFAULT_TIMECHART_SPAN_UNIT` (both in `build.rs`).

With a `by` field and a `limit` above 0 (default `DEFAULT_TIMECHART_LIMIT` in `build.rs`), the
`limit` series with the greatest sum of the aggregation over all buckets keep their name, ties by
name ascending; the other series merge into the series `OTHER_SERIES` of `plan.rs`, or are
dropped with `useother=false`. Merged rows are re-aggregated per bucket with `min` for `min`,
`max` for `max`, `avg` for `avg` and `sum` for every other aggregation. A NULL series is labelled
`NULL_SERIES` of `plan.rs`; with `useother=false` the NULL series is dropped together with the
series beyond the limit. Because the series rank by a sum, an aggregation that is not numeric
(`min(body)`, `values(x)`) is a 400 there. `limit=0` ranks nothing: it keeps every series under
its own value, and a NULL series stays NULL.

### Bin

`bin` replaces a field's value with its bin.

- Numbers bin to `<start>-<end>` strings of a whole-number width (`floor(x / w) * w`), integers
  exactly and other numbers through a double quotient. The bounds are exact also where a bin
  reaches past a 64-bit integer limit. A fractional width, a fractional length with a unit
  (`1.5h`) and a width that, doubled, overflows the 64-bit range are a 400. A non-integer value
  whose quotient `floor(x / w)` is NaN or beyond the 64-bit integer range fails the query at
  execution with a 500 `EXECUTION_ERROR`.
- Timestamps bin to the bucket start: fixed units (microseconds to hours, and a bare number,
  which is hours) from the Unix epoch, days to midnight UTC of day `floor(d / n) * n` after the
  epoch, and months to the `yyyy-MM` string of month `floor(i / n) * n` after January 1970, so an
  instant before the epoch bins to a start at or before it. A time length is a positive 64-bit
  integer. A fixed-unit length whose width in microseconds, doubled, overflows the 64-bit range
  is a 400, and so is a day or month length above `MAX_DAY_BIN_LENGTH` or `MAX_MONTH_BIN_LENGTH`
  (`ppl/plan.rs`), past which a bin before the epoch would start outside the timestamp range.
- Units: `M` (months), `m` (minutes), `us`, `cs` and `ds`, matched case-sensitively; `ms`; `s`,
  `sec`, `secs`, `second`, `seconds`; `min`, `mins`, `minute`, `minutes`; `h`, `hr`, `hrs`,
  `hour`, `hours`; `d`, `day`, `days`; `mon`, `month`, `months` in any case. Weeks, quarters and
  years do not bin and are a 400.
- `bin … as <name>`, a `bin` without `span`, and the options `bins`, `minspan`, `aligntime`,
  `start`, `end` and a `log` span are a 400.

### Row order after aggregation

`dedup` keeps rows in the incoming order: after a `sort` it keeps the first rows of that order,
and without one the order is the scan's, so which rows of a combination are kept is unspecified.
The order of a `sort` holds until the next `sort` or aggregation, whatever comes between: a
`rename` of a sorted column carries the order to the new name, and a `fields`, `eval`, `bin` or
`rename`, `parse` or `fillnull` that removes or replaces a sorted column keeps the sorted values in
a hidden column
that `dedup` orders by and the output never shows. `stats`, `top`, `rare` and `timechart` define
their own output order, so a `sort` before them does not carry past them.

## `describe`

`describe <index>` answers a jdbc response whose `schema` is the 24 JDBC `getColumns` columns, in this
order: `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`, `TYPE_NAME`, `COLUMN_SIZE`,
`BUFFER_LENGTH`, `DECIMAL_DIGITS`, `NUM_PREC_RADIX`, `NULLABLE`, `REMARKS`, `COLUMN_DEF`,
`SQL_DATA_TYPE`, `SQL_DATETIME_SUB`, `CHAR_OCTET_LENGTH`, `ORDINAL_POSITION`, `IS_NULLABLE`,
`SCOPE_CATALOG`, `SCOPE_SCHEMA`, `SCOPE_TABLE`, `SOURCE_DATA_TYPE`, `IS_AUTOINCREMENT`,
`IS_GENERATEDCOLUMN`. Columns typed `int`: `DATA_TYPE`, `COLUMN_SIZE`, `BUFFER_LENGTH`,
`DECIMAL_DIGITS`, `NUM_PREC_RADIX`, `NULLABLE`, `SQL_DATA_TYPE`, `SQL_DATETIME_SUB`,
`CHAR_OCTET_LENGTH`, `ORDINAL_POSITION`, `SOURCE_DATA_TYPE`; the rest are `string`.
`SOURCE_DATA_TYPE` is a JDBC `smallint`; PPL has no such type, and the column is always null, so
it is typed `int`.

One row per field of the index's field model, in the order `_mapping` lists them: schema column order,
each MAP column followed by its keys in byte order. A key containing dots is one row
(`log_attributes.http.method`). Per row: `TABLE_CAT` is `opensearch.cluster_name`; `TABLE_NAME` is the
index as written; `COLUMN_NAME` the field name; `TYPE_NAME` is `string` for `text` and `keyword`,
`timestamp` for `date`, `int`, `bigint`, `double`, `boolean`, and `struct` for objects; `NUM_PREC_RADIX`
10; `NULLABLE` 2; `ORDINAL_POSITION` the 0-based row number; `IS_NULLABLE` and `IS_GENERATEDCOLUMN`
empty strings; `IS_AUTOINCREMENT` `NO`; every other column null. MAP keys come from field discovery, so
keys only in the WAL are not listed. The source names exactly one data index: an unknown name is the
`IndexNotFoundException` 404, several indices or system indices a 400. Later commands apply to the
rows like to any source.

## `parse`

`parse <field> '<pattern>'` matches the pattern against the **whole** value. Each named group
`(?<name>…)` becomes a `string` field; a group named like an existing field replaces it in place, other
groups are appended in pattern order. A NULL value gives NULL; a value the pattern does not match, or a
group that did not take part in the match, gives `""`. Patterns use the Rust `regex` dialect with this
group syntax: look-around and back-references are a 400, and so are a pattern that does not compile on
its own (an unbalanced one such as `x)|(?<a>.*` included), a pattern without a named group, a group
name with the reserved prefix, and a source that is not a string. Each group field costs one match of
the pattern per row.

## `fillnull`

`fillnull with <v> [in <fields>]`, `fillnull using <f> = <v>, …`, `fillnull value=<v> [<fields>]`.
Without a field list every output column is filled and must accept `<v>`. Strings accept strings,
numbers numbers, booleans booleans, timestamps timestamps; MAP, struct and list columns accept nothing,
and fixed-size binary id columns accept no replacement. An integer column takes only an integer it
holds exactly: a literal inside its range, or a computed integer whose type has no value outside that
range. A fraction, a literal outside the range and a computed integer of a wider type (`1 + 1` is a
`bigint`) are a 400 naming the field and the value. A floating-point or decimal column takes any
number, rounded to its type; only a literal outside its range is a 400. The checks run when the
query is planned, so the cast into an integer column never truncates or goes out of range while it
runs; a computed replacement is itself evaluated as any expression is.
An incompatible type is a 400 naming the field and the PPL types of both; a field listed twice is a
400; an unknown field is `FIELD_NOT_FOUND`.

## Timestamp literals

A string literal compared with a timestamp field (`=`, `!=`, `<`, `<=`, `>`, `>=`, `between`, `in`),
and the argument of `timestamp('…')`, is read as UTC by the one literal grammar of the function
library: `yyyy-MM-dd HH:mm:ss[.f]` (a space or a `T` between date and time) with one to six fraction
digits, or `yyyy-MM-dd` (midnight); surrounding whitespace is ignored. It becomes a microsecond
literal, so the comparison stays a literal comparison in the stored unit, which the WAL and the
Iceberg scans can prune with. More than six fraction digits, or another form, is a 400: stored
timestamps are microseconds, and a finer literal could only be truncated.

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
