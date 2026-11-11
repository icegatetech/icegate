# PPL

PPL (piped processing language) is the query language of `POST /_plugins/_ppl` on the
OpenSearch listener. A query is a source followed by commands joined by `|`; each command
transforms the rows of the commands before it.

```text
source=logs-* | where severity_text = 'ERROR' | fields timestamp, service_name, body | sort - timestamp | head 20
```

## How a query runs

1. The vendored grammar of the `icegate-query-grammar` crate (`src/ppl/`; the `PPL` section of
   [that crate's README](../../../icegate-query-grammar/README.md) states the pinned version and
   how to regenerate it) parses the text. Keywords match in any case; names keep their case.
2. `build.rs` walks the parse tree into the AST of `ast.rs`. Every command, function and
   argument IceGate does not plan is refused there by name, so an unplanned construct is a 400
   that names it, never a different query.
3. `plan.rs` lowers the AST to one DataFusion logical plan over the tenant's tables, through the
   tenant's `TenantSession`: the tenant scope, index expressions, name resolution and the
   per-query memory limit are those of every other front end of the listener. The analyzer runs
   while planning, so a type error is the query's 400, not a failure during execution.
4. `opensearch/ppl/` executes the plan and renders the requested format, or the error envelope.
   `POST /_plugins/_ppl/_explain` stops after planning and answers the DataFusion logical and
   physical plan text instead.

## Request

The body is a JSON object with a string `query` and an optional integer `fetch_size`. Every other
key is accepted and ignored; the query text alone decides the result. `fetch_size` 0 bounds
nothing; N ≥ 1 keeps the first N rows of the final output, as a trailing `| head N` does. The
`format` query parameter accepts `jdbc` (the default), `viz`, `csv` or `raw`, in any case; every
other value is a 400. `csv` and `raw` refuse a `filter_path` parameter with a 400 that carries
`parameter` and `format`, because their body is not JSON. A request that cannot be read is
refused in the listener's 400 `illegal_argument_exception` envelope, which names the offending
`key` (and its `value`, when the body gave one) or `format`.

`POST /_plugins/_ppl/_explain` takes the same body and answers
`{"calcite": {"logical": "<text>", "physical": "<text>"}}`, the DataFusion logical and physical
plans of the query; its `format` parameter accepts only `json`, and another value is a 400 that
carries `format`. The query is planned, never executed. Parse, planning and source errors answer
in the PPL error envelope, as on `/_plugins/_ppl`.

## Response

`jdbc` and `viz` answer `application/json; charset=UTF-8`; `csv` and `raw` answer
`text/plain; charset=UTF-8`, so that a client that parses every body labelled JSON takes their
text as it is. The case `workbench runs a ppl query, explains it and downloads csv` in
`scripts/opensearch-e2e/osd.spec.ts` pins the Workbench's csv download over this answer.

`jdbc` is `{schema: [{name, type}], datarows, total, size}`, where `total` and `size` both count
the rows returned. `schema` lists the output columns in order with their PPL types (`string`, `int`,
`bigint`, `double`, `boolean`, `timestamp`, `date`, `struct`, `array`). `render.rs` states how
each cell renders: timestamps in UTC as `yyyy-MM-dd HH:mm:ss[.f]` with trailing zeros dropped,
fixed-size binary ids as lowercase hex, a MAP column as one object keyed by its stored keys (a
dotted key is never split), `NaN` and the infinities as strings.

`viz` is `{data: {<column>: [...]}, metadata: {fields: [{name, type}]}, size, status: 200}`: one
array per column, keyed by column name, with each cell rendered as `jdbc` renders it. Its type
names differ from `jdbc`'s: `string` is `keyword`, `int` `integer`, `bigint` `long`, `struct`
`object` and `array` `nested`; the others keep their name.

`csv` and `raw` are flat text: a line of column names, then one line per row. `csv` is
comma-separated and quotes a cell only when it holds a separator, a quote or a line break; `raw`
is pipe-separated and never quoted. A NULL cell is empty; every other cell is its `jdbc` value as
text, with numbers in plain decimal and MAPs as `{k=v, …}` with each key whole. `render.rs`
states the exact rules.

## Sources

`search source=<indices>`, `source=<indices>` and `index=<indices>` are the same source. The
index list is an index expression: comma-separated names, `*` wildcards and `-` exclusions,
resolved like the `_search` path with `ignore_unavailable=false`, `allow_no_indices=false` and
open, non-hidden indices. A name that matches nothing is a 404 `INDEX_NOT_FOUND`. A wildcard part
that matches nothing is skipped while another part matches, so only an expression that matches
nothing as a whole is that 404: OSD's associated-logs source
`ss4o_logs-*,opensearch_dashboards_sample_data_otel_logs*` reads the ss4o logs index
(`osd_associated_logs_query_lists_the_logs_of_one_trace` in `tests/opensearch/ppl_ss4o.rs`).
Names are case-sensitive.

A source on system indices is a 400, and so is a source on the Data Prepper span index
(`otel-v1-apm-span-000001`, aliased `otel-v1-apm-span`): its cross-row fields read other rows,
which no PPL projection of one row can. A wildcard that reaches one table through two indices
without naming each (`*` reaches `spans` and `otel-v1-apm-span-000001`) is a 400 as on
`_search`, so no row is read twice. A source that reaches one table through two profiles in any
way, through a wildcard (`*logs*`) or with the names written out (`logs,ss4o_logs-<tenant>`),
is a 400 `SEMANTIC_ERROR` naming the source as written, as is every other refusal of index
resolution (a source mixing data and system indices): the two field models do not union (binary `trace_id` against hex
`traceId`). `_search` reads such a named pair; PPL does not. `describe <index>` lists an index's
fields and reads no rows, so its rules differ: see [`describe`](#describe).
`source=[ <subsearch> ] [as t]` reads a subsearch's output: see [Subsearches](#subsearches).

### ss4o indices

The ss4o indices (`ss4o_logs-<tenant>`, `ss4o_traces-<tenant>`, `ss4o_metrics-<tenant>`, named
for the requesting tenant) read as relations of their profile's field model: the field table
in `opensearch/catalog/ss4o.rs` that `_search` and `describe` read. `profile_source.rs` builds
the relation from that table and declares no field of its own.

- **Columns.** Every field whose parent is no declared object is a column, in the profile's
  declared order (the order `describe` lists), aliases included: `time`, `@message` and
  `observerTime` read their targets. An object (`severity`, `resource`, `status`) is a `struct` of
  its members, NULL when every member is; an attribute object is its MAP; a list column is an
  `array`, and so is the metrics `buckets`, a list of `{min, max, count}` structs, NULL on a row
  that is no histogram (`ss4o_metrics_buckets_read_as_an_array_of_bucket_structs`). `describe`
  lists the same fields with the same types, a list column and `buckets` as `array` too
  (`describe_and_a_bare_source_agree_on_the_ss4o_logs_columns`,
  `describe_and_a_bare_source_agree_on_the_ss4o_traces_and_metrics_columns` and
  `osd_associated_logs_query_lists_the_logs_of_one_trace` in `tests/opensearch/ppl_ss4o.rs`).
- **Values** are those `_source` shows: ids as lowercase hex, times as timestamps, enumerations
  by name (`kind`, `aggregationTemporality`), derived fields computed (`durationInNanos`, `value`,
  the list lengths). An id whose stored column admits NULL (`parentSpanId`) reads as hex text, `""`
  for NULL, as `_source` shows a root span's parent.
- **Names.** A dotted member (`severity.text`, `status.code`), an object below another
  (`resource.attributes`), an attribute key (`attributes.http.method`) or a promoted resource
  attribute (`resource.attributes.service.name`, stored in `service_name`) resolves in every command
  that takes a field, after the columns of that exact name; a `fields` that lists one keeps it as
  a column named by its path. Such a name reads the table's stored columns, which travel with the
  rows as hidden columns, so it holds the stored value whatever an earlier command did to the
  visible columns (`fields - severity | where severity.text = 'ERROR'` filters). A string literal
  compared with `traceId` or `spanId` is an id literal (see [Id literals](#id-literals)), a
  malformed one included.
- **Filters.** A filter on a field that reads its column as stored (an id, a time, a plain value,
  an attribute key) is a predicate on that column, which the scan prunes by as on a native index.
  On an encoded or derived field (`kind`, `status.code`, `traceState`, `durationInNanos`, the
  metrics `kind`, `aggregationTemporality` and `value`, the list lengths) a test against literals
  where the row is kept only when the test is true (a `where` conjunct, below `and` and `or` only)
  lowers through the profile's inverse to a predicate on the stored columns, the one `_search`
  uses: `=`, `<`, `<=`, `>`, `>=`, `in (…)`, `between`, and `is not null` (not on the span kind,
  where a stored kind without a name reads NULL). It keeps exactly the rows the test of the read
  value keeps. A literal of another type than the field reads, `!=`, `not in`, `not between`,
  `is null`, and a test below `not` or in any other expression read the field's value and do not
  prune.
- **`parentSpanId`** reads as lower-case hex text, `""` for a root's NULL parent. A string literal
  compared with it is an id literal in every comparison, `in` and `between`, wherever it stands
  (`where`, `not`, `eval`): either case, compared as lower-case hex, and a malformed one is the 400 of a
  native id column, naming the literal. In a filter position `=`, `!=`, `in` and the ranges not
  bounded by `''` lower to the stored `parent_span_id`, `''` meaning NULL. A root's parent is `""`,
  never NULL, so `!=` and `not` keep the roots, where the native `parent_span_id` (NULL) drops them.
- **Fields PPL cannot read.** A field `_source` reshapes from a whole list of the row (the span
  `events` and `links`; the metrics `positiveBuckets`, `negativeBuckets`, `quantiles` and
  `exemplar`) is no column, and naming it, or a path below it, is a 400 `SEMANTIC_ERROR` naming it.
- **Several source indices.** A profile field that some source indices declare and others do not
  (`severity.text` over `ss4o_logs-*,ss4o_traces-*`) is a 400 `SEMANTIC_ERROR` naming it: its
  expression is the declaring profile's, and reading another index's rows through it could answer
  a value that index never stored. The relations' columns union as any columns do.

Every other contract of this README holds over these relations as over a native index.

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
| `[inner \| left] join [left=<l>] [right=<r>] on <criteria> <right side>` | See [Joins](#joins). |
| `eventstats …`, `streamstats …` | See [Window commands](#window-commands). |
| `append [ <search> \| <commands> ]` | See [`append`](#append). |

The output columns of a source are the table's columns in table order, or for an ss4o index its
profile's columns (see [ss4o indices](#ss4o-indices)). A field reference is
resolved against the columns of the pipeline at that point, case-sensitively; a name that
matches none is a 400 `FIELD_NOT_FOUND`, except for the promoted resource attributes below.

## Fields and types

A field reference resolves, in this order, to a lambda parameter in scope, the column of that
exact name, a resource attribute the source's profile promotes to a column or a field of an ss4o
source's field model (see [ss4o indices](#ss4o-indices)), or a key of a MAP column written
`<column>.<key>` (the key may itself contain dots; an absent key is NULL).
A field name that starts with `RESERVED_NAME_PREFIX` of `plan.rs`, the prefix of the plans' own
columns, is a 400 `SEMANTIC_ERROR` naming it wherever the query writes it: a reference, a
`fields` pattern, an `eval` or `rename` target, an alias, `timefield` or `countfield`.
A promoted resource attribute whose column an earlier command removed, renamed, or replaced
(a `rename` onto its name, an `eval`, `bin`, `parse` or `fillnull` writing it) is a 400
`SEMANTIC_ERROR`, and so is a
promoted attribute that the source indices disagree on (a column in some, not in others).
Several source indices are combined with UNION ALL over the union of their columns, in the order
the columns first appear; a column an index lacks is NULL in its rows, and a column typed
differently across the indices is a 400 naming it, both indices and both PPL types (the Arrow
types when the two share one PPL name).

- Decimal columns read as `double`, the one numeric type PPL reports for them.
- Strings compare and sort byte-wise (code-point order), never by locale.
- A fixed-size binary id renders as lowercase hex. A string literal compared with an id is read as
  hex (see [Id literals](#id-literals)); comparing an id with any other value that is not an id is a
  400 `SEMANTIC_ERROR` naming the id field and both PPL types.
- Timestamps are microseconds in UTC. A timestamp literal is `yyyy-MM-dd`, or
  `yyyy-MM-dd HH:mm:ss` (a `T` may separate date and time) with a fraction no finer than a
  microsecond; any other form, and a finer fraction, is a 400. See
  [Timestamp literals](#timestamp-literals).

## Expressions

| Form | Contract |
|---|---|
| `=`, `!=`, `<`, `<=`, `>`, `>=` | NULL on either side is unknown. |
| `<value> like <pattern>` | The same as `like(value, pattern)`. |
| `and`, `or`, `xor`, `not` | Three-valued; `xor` is true when exactly one side is true, NULL when either side is NULL, and takes boolean operands only. |
| `<value> [not] in (<literal>, …)` | Membership in the literal list. |
| `<value> [not] between <low> and <high>` | Both bounds inclusive. The value must compare with each bound and the two results with each other, as `>=` and `<=` would; otherwise the query is a 400 `SEMANTIC_ERROR` naming the value and the types. |
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
dropped with `useother=false`. Merged rows are re-aggregated per bucket, exactly: `count` and
`sum` by their sum, `min` by `min` and `max` by `max`. The average, distinct count and percentile
of merged series are not determined by the per-series values, so with the other series in play
(a `limit` above 0 and `useother=true`, the defaults) `avg`, `dc` / `distinct_count` and every
percentile spelling are a 400 `SEMANTIC_ERROR` whose `context` names the function as planned
(`avg`, `dc` or `percentile`) and the cause `inexact_other_series`, whether or not the data has
more series than the limit; `useother=false` or `limit=0` charts them. A NULL series is labelled
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
that `dedup` orders by and the output never shows. `eventstats` and `streamstats` keep the
incoming order and record it in a hidden column that later commands read. `stats`, `top`, `rare`
and `timechart` define their own output order, so a `sort` before them does not carry past them.

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
(`log_attributes.http.method`). An index served under a non-native profile (`otel-v1-apm-span-000001`) lists
the profile's fields in the profile's declared order. `describe` reads no rows, so it answers for the Data
Prepper span index although that index cannot be a PPL source (see [Sources](#sources)); over an ss4o index,
its rows without a dot, but for the fields PPL cannot read, name the columns of a bare source, in the same
order (see [ss4o indices](#ss4o-indices)). Per row: `TABLE_CAT` is `opensearch.cluster_name`; `TABLE_NAME` is the
index as written; `COLUMN_NAME` the field name; `TYPE_NAME` is `string` for `text` and `keyword`,
`timestamp` for `date`, `int`, `bigint`, `double`, `boolean`, `array` for an object field that reads a
list column (the type a source reads it as: `events` of `spans`, `bucketCountsList` of
`ss4o_metrics-<tenant>`; `describe_types_a_native_list_column_array_as_a_bare_source_reads_it` in
`tests/opensearch/ppl_ss4o.rs`), and `struct` for every other object; `NUM_PREC_RADIX`
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

## Joins

`[inner | left [outer] | outer] join [type=inner|left|outer] [max=0] [left=<l>] [right=<r>] on <criteria> <right side>`,
with `where` accepted for `on`. The right side is an index expression (`spans`, `spans as s`) or a bracketed
subsearch (`[ source=spans | … ] as s`). Without a type the join is inner; a type written both before `join` and as
`type=` must agree. Each of `type=` and `max=` may be written once.

- **Planning.** The criteria are one predicate of one join. Every equality between a left-only and a right-only
  expression becomes a hash key; the rest (`or`, ranges, `isnull`) is the join's own filter, evaluated while
  matching. A left join therefore keeps a left row whose candidates all fail the residual, with NULL right columns.
  Pinned by `the_join_plan_hashes_on_the_equality_conjuncts_with_a_residual_filter` in
  `tests/opensearch/ppl_trust_delta.rs`.
- **Names in the criteria.** Each side offers the bare name of each column and, for a qualified column,
  `<qualifier>.<name>`: the `left=` / `right=` alias when given, else the table (`logs.service_name`) or a
  subsearch's `as` alias. A name offered twice is not visible and answers `FIELD_NOT_FOUND`; qualify it.
- **Output.** The left columns, then the right columns. A right column whose name a left column has is renamed
  `<right qualifier>.<name>` (`s.service_name`); with nothing to rename it by, or when the renamed name is taken
  too (a chained join reusing `right=s`), the query is a 400 naming the column.
- **Never capped.** The right side runs through the same tenant scope and has no row limit; `max=0` (also when
  absent) joins every matching row. A right side over a system index is a 400 naming it.
- **Refused (400):** `right`, `full`, `cross`, `semi` and `anti` joins, the field-list form without criteria,
  join hints, `max` other than 0, and a repeated `type=` or `max=`. `overwrite=` is accepted and changes
  nothing in the criteria form.

## Subsearches

- **`<expression> in [ <search> | <commands> ]`** is a top-level `and` conjunct of `where`. It is planned as
  `IN (subquery)`, which becomes a semi-join: each row is kept once however many subsearch rows match it.
  Pinned by `where_in_a_subsearch_plans_an_uncapped_semi_join_and_keeps_the_sort_order` in
  `tests/opensearch/ppl_trust_delta.rs`. The subsearch runs through the same tenant scope, returns exactly one
  column, and is never capped. A semi-join keeps no row order, so rows sorted before the `where` are sorted
  again after it. `not in [ … ]`, `(a, b) in [ … ]`,
  and `in [ … ]` under `or` / `not` or outside `where` are a 400.
- **`source=[ <search> | <commands> ] [as t]`** reads the subsearch's output as the pipeline's source. The alias
  qualifies its columns for a later `join` (`t.service_name`); other commands use the bare names. An alias on an
  index source (`source=logs as l`) is a 400; a join names its sides with `left=` / `right=`.
- **System indices:** a subsearch over a system index is a 400 naming it, like a system-index source.

## Window commands

- **`eventstats [bucket_nullable=<bool>] <aggregation> [as <name>], … [by <field>, …]`** adds the aggregates of
  each row's group to that row (the whole input without `by`).
- **`streamstats <aggregation> [as <name>], … [by <field>, …]`** adds running aggregates: each row's value covers
  its group from the first row of the incoming order up to and including itself (`sort … | streamstats count() as
  rn by k | where rn = 1` keeps the first row per `k`).
- **Aggregations:** `count()`, `count(<expr>)`, `sum`, `avg`, `min`, `max`; any other is a 400 naming it. A column
  is named by `as`, else by the call as written (`count()`). A name equal to an input column replaces it in place;
  a name given twice is a 400.
- **NULL keys:** `bucket_nullable=true` (the default) makes NULL keys one group; `false` gives rows with a NULL
  key NULL aggregates. `streamstats` always groups NULL keys and refuses every argument (`current`, `window`,
  `global`, `reset_before`, `reset_after`, `bucket_nullable`): its running window always reaches from the first
  row of the group to the current row. Both commands group by fields only: `by span(…)` is a 400.
- **Order:** both commands keep the incoming order of the rows (see
  [Row order after aggregation](#row-order-after-aggregation)); without one (no `sort`, or after `stats` or
  `join`) the rows have no order to keep. A `where` written after them filters their output; it never moves
  below them.
- **Never capped:** both read every row of their input.

## `append`

`append [ <search> | <commands> ]` adds the branch's rows after the pipeline's rows. The pipeline's rows come
first in their incoming order, the branch's rows follow in the branch's own output order, and chained `append`s
concatenate in the order written. The columns are the pipeline's, then the branch's new ones; a column missing on
one side is NULL there, and a column both sides have with different types is a 400 naming it and both PPL
types, or the Arrow types when the two share one PPL name (no coercion). A type that differs only in the names or metadata of nested fields is the same type (each table
numbers the Iceberg field ids of its `MAP` columns on its own), and the column takes the pipeline's type. The
branch runs through the same tenant scope and is never capped; a branch over a system index is a 400 naming it,
and a branch without a search is a 400. The commands after `append` (`streamstats`, another `append`) see the
rows in this order.

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

The per-query memory limit (`opensearch.max_query_memory_bytes`) also counts the answer: the
output batches are reserved in the query's memory pool as they arrive, at their Arrow size, and
the rendered body grows only by what the pool grants, written cell by cell from the batches on
the blocking pool. An answer that does not fit is a 400 `RESOURCE_LIMIT_EXCEEDED` naming the
limit in bytes, in every format.

Parentheses and square brackets may nest `MAX_PPL_NESTING_DEPTH` levels deep, square brackets
alone `MAX_PPL_SUBSEARCH_DEPTH` levels deep, and a query may hold `MAX_PPL_OPERATOR_COUNT`
operators (all three in `build.rs`). All are counted on the lexer's tokens before the query is
parsed; a bracket or an operator inside a string, a quoted name or a comment counts for nothing.

- Brackets are counted together: a subsearch inside a group is two levels. The parser, the
  builder, the planner and the executed plan recurse once per level.
- Square brackets are counted alone. Every subsearch is written in them (`in`, `exists`, a scalar
  subsearch, `append`, `appendcol`, `appendpipe`, `join`, `multisearch`), so they bound how deeply
  subsearches nest; a square bracket that opens no subsearch counts all the same. Nested
  subsearches are planned on the async worker, where the query deadline cannot preempt them, and
  the cost of planning nested `in` subsearches grows steeply with their depth; the limit keeps it
  well under a second in an unoptimized build.
- Operators are counted over the whole query: every operator token, a logical, comparison or
  arithmetic operator, `source=` and an assignment's `=` included, as `measure_ppl_nesting` of the
  grammar crate lists them. Each operator of a chain such as `a = 1 or a = 1 or …` or
  `not not … a = 1` adds a level of recursion to the parse, the build of the query, the drop of
  the parse tree and the lowering of the expression, without a bracket.

A query beyond a limit is a 400 `RESOURCE_LIMIT_EXCEEDED` naming the limit: as `limit_depth` for
the nesting limit, `limit_subsearch_depth` for the subsearch limit and `limit_operators` for the
operator limit. The limits are checked in that order, and the first one a query exceeds is named.
The brackets and the operators together bound every recursion over a query. Parsing a nested or
operator-heavy query runs on a thread sized for the limits, and the limits keep planning and
execution within the stack of an async worker.

## Errors

A PPL error is answered with content type `application/json; charset=UTF-8` and the JSON body
`{status, error: {type, reason, details, code, context?}}`. The JSON label is deliberate, so
that the Workbench of OpenSearch Dashboards shows the error's reason; the case `a ppl error shows
its reason in the workbench` in `scripts/opensearch-e2e/osd.spec.ts` pins that.

| Cause | Status | `type` | `code` | `context` |
|---|---|---|---|---|
| Text the grammar rejects, or no source | 400 | `SyntaxCheckException` | `SYNTAX_ERROR` | `{line, column}` of the offending token |
| A source that names no index | 404 | `IndexNotFoundException` | `INDEX_NOT_FOUND` | `{index}` |
| A field reference that matches no column | 400 | `SemanticCheckException` | `FIELD_NOT_FOUND` | `{field}` |
| A command IceGate does not plan | 400 | `UnsupportedOperationException` | `UNSUPPORTED_OPERATION` | `{command}` |
| A function IceGate does not plan | 400 | `UnsupportedOperationException` | `UNSUPPORTED_OPERATION` | `{function}` |
| A well-formed but invalid query (types, arguments, system indices) | 400 | `SemanticCheckException` | `SEMANTIC_ERROR` | `{name}` where the refusal names what it is about, else none |
| A `timechart` aggregation the other series cannot merge exactly | 400 | `SemanticCheckException` | `SEMANTIC_ERROR` | `{function, cause}`, `cause` being `inexact_other_series` |
| More output rows than the row limit | 400 | `IllegalArgumentException` | `RESOURCE_LIMIT_EXCEEDED` | `{limit}` |
| More memory than the per-query limit, the answer included | 400 | `IllegalArgumentException` | `RESOURCE_LIMIT_EXCEEDED` | `{limit_bytes}` |
| Brackets nested deeper than the nesting limit | 400 | `IllegalArgumentException` | `RESOURCE_LIMIT_EXCEEDED` | `{limit_depth}` |
| Subsearches nested deeper than the subsearch limit | 400 | `IllegalArgumentException` | `RESOURCE_LIMIT_EXCEEDED` | `{limit_subsearch_depth}` |
| More operators than the operator limit | 400 | `IllegalArgumentException` | `RESOURCE_LIMIT_EXCEEDED` | `{limit_operators}` |
| A failure inside IceGate | 500 | `IllegalStateException` | `EXECUTION_ERROR` | — |

The `name` of a `SEMANTIC_ERROR` is the field, alias, function, option or index the refusal is
about, the operator, keyword or wildcard it refuses (for example `in`, `span` or `*`), or the
argument it refuses (a literal or a pattern, without the quotes the query wrote around it), spelled
as the reason spells it. A client reads what the refusal is about from it, never from the reason,
which is IceGate's own wording; two refusals about the same name share it.

A syntax error's `reason` is fixed and its `details` name the offending token; a 500's `reason`
is fixed and its `details` carry the cause, which is also logged. A shed request, a passed
deadline or write contention keeps the listener's 503 envelope, and a request body that cannot
be read keeps its 400 `illegal_argument_exception` envelope, so the listener has one retry
contract for every route.

## Tenant isolation

Every source resolves through the requesting tenant's session, whose scan carries the tenant
filter, so a PPL query never reads another tenant's rows. `tests/opensearch/ppl.rs` pins this
with two tenants written in the same batch. Every join side, every subsearch and every `append`
branch resolves through that same session; `tests/opensearch/ppl_trust_delta.rs` pins it with
both tenants' rows in one WAL segment.

## Id literals

A string literal compared (`=`, `!=`, `<`, `>`, `<=`, `>=`, `in (…)`, either bound of `between`) with a
fixed-width id (`trace_id`, `span_id`, `parent_span_id`) is read as hex, either case, and folded to the id's own
fixed-width binary value before planning; the literal may stand on either side and the query stays standard PPL
(`trace_id = '<hex>'`). Invalid hex or a wrong number of bytes is a 400 naming the column and the literal. Any
other value compared with an id, listed in an `in (…)` against one or bounding a `between` of one, must itself be
an id: a value of another type (a number, a string column) is a 400 naming the id field and both PPL types. The folded value reaches
both the WAL scan and the Iceberg scan, so row groups are pruned by statistics and bloom filters. Ids render as
lowercase hex, and lowercase hex order is byte order: `<` / `>` on an id agree with `sort` on it, which keyset
paging relies on.

## Clients, end to end

The end-to-end cases that pin how the PPL clients use this endpoint
(`scripts/opensearch-e2e/osd.spec.ts` and `grafana.spec.ts`):
`workbench runs a ppl query, explains it and downloads csv`, `workbench lists the index tree`,
`discover ppl renders documents and a histogram without highlight`,
`observability logs renders events and the count chart`,
`a ppl error shows its reason in the workbench`,
`grafana renders a ppl logs query and a ppl time series`,
`template variables from a lucene find query feed lucene and ppl panels`,
`ad-hoc filters apply to lucene and ppl panels` and
`a multi-value variable interpolates into a ppl panel`.
