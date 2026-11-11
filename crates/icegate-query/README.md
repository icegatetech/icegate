# icegate-query

Query APIs for IceGate. A single Rust binary that exposes the same DataFusion + Iceberg engine through five wire protocols, so existing observability tooling, BI clients, dashboards and ad-hoc analytics tools can all read from the same underlying tables.

## Supported protocols

| Protocol                      | Default port | Transport | Status   | Notes                                                |
|-------------------------------|--------------|-----------|----------|------------------------------------------------------|
| Loki HTTP API (LogQL)         | `3100`       | HTTP/JSON | Stable   | Grafana, `logcli`, Promtail clients                  |
| Prometheus HTTP API (PromQL)  | `9090`       | HTTP/JSON | Stable   | Grafana, `promtool`, alerting tools                  |
| Tempo HTTP API (TraceQL)      | `3200`       | HTTP/JSON | Stable   | Grafana, `tempo-cli`, OTel tracing UIs               |
| Apache Arrow **Flight SQL**   | `8815`       | gRPC      | Beta     | BI tools, ADBC, JDBC, ad-hoc SQL                     |
| OpenSearch HTTP API (Query DSL, Lucene, PPL) | `opensearch.port` | HTTP/JSON | Beta     | OpenSearch Dashboards, Grafana OpenSearch datasource, PPL clients; see [OpenSearch](#opensearch) |

All five share one `QueryEngine` (cached Iceberg catalog provider + DataFusion `SessionContext`); enabling more than one only costs the additional listener. The OpenSearch listener runs each request in a session with its own memory pool (`opensearch.max_query_memory_bytes`); the other protocols do not apply it.

## Flight SQL

The Flight SQL endpoint exposes the IceGate data lake as a generic SQL warehouse over gRPC. Client SQL is parsed and executed by DataFusion against the merged WAL + Iceberg view of the four observability tables: `logs`, `spans`, `events`, `metrics`. There is no IceGate-specific query language to learn — just standard SQL.

### Address & catalog layout

```text
grpc://<host>:8815                  # plaintext gRPC unless the `tls` block is set
catalog:   iceberg
schema:    icegate
tables:    logs | spans | events | metrics
```

Always use the fully-qualified path:

```sql
SELECT count(*) FROM iceberg.icegate.logs;
```

There are intentionally no convenience views in the default catalog: a single canonical path keeps the tenancy guarantee anchored to one place (the `TableProvider` wrapper).

### Tenancy

The Flight SQL session reads the tenant identifier from the `x-scope-orgid` gRPC metadata header — the same Grafana convention used by the HTTP APIs. Any client capable of setting a gRPC header (ADBC, JDBC, the Apache Arrow Flight SQL ODBC driver, `grpcurl -H`) can supply it.

Which tenant a request reads, and which requests are refused, is decided by the `tenant` policy: see [Whose tenant a request reads](#whose-tenant-a-request-reads).

### Isolation guarantees

Defense-in-depth wrapped around the iceberg catalog:

- Every `TableProvider::scan(...)` AND's `tenant_id = '<t>'` into the filter list before delegating to the underlying Iceberg provider. The filter is injected **after** DataFusion's pushdown analysis so no planner pass can strip it.
- The `tenant_id` column is **removed from the schema** the server advertises. `DESCRIBE`, `information_schema.columns`, `SELECT *`, and Flight SQL `get_tables(include_schema=true)` all reflect the filtered schema.
- Referencing `tenant_id` in `WHERE` or `SELECT` fails at SQL planning time with "column not found".
- `iceberg` is partitioned on `tenant_id` (identity transform), so the injected filter is `Exact` pushdown — manifests for other tenants are pruned before any row group is touched.

### Read-only

DDL (`CREATE`/`DROP`/`ALTER`) and DML (`INSERT`/`UPDATE`/`DELETE`) submitted as SQL text or prepared statements are rejected by DataFusion `SQLOptions` after parsing but before execution. `EXPLAIN`, `SHOW`, and `SET` are allowed.

The upstream service applies that `SQLOptions` check only on the SQL and prepared-statement paths, not on the `CommandStatementSubstraitPlan` (Substrait) path. Read-only-ness there does not depend on `SQLOptions`: the engine's `TableProvider`s are read-only (no `insert_into`/`delete`/`update`), so a write expressed as a Substrait plan is still rejected at execution.
Tenant isolation holds on every path — Substrait plans resolve tables through the same tenant-scoped catalog wrapper.

The Flight SQL `BeginTransaction` / `EndTransaction` actions are not implemented — the upstream service returns `Unimplemented`. The endpoint is stateless and read-only; no transactional semantics exist to engage.

### Pushdown — what runs server-side vs client-side

| Operation                              | Pushdown into Iceberg scan                |
|----------------------------------------|-------------------------------------------|
| `tenant_id` partition predicate        | ✅ Always, automatically injected         |
| Time-range filters on `timestamp`      | ✅ Iceberg partition pruning              |
| Equality / range filters on indexed columns | ✅ Parquet statistics                |
| Filters on attribute map keys (e.g. `resource_attributes[...]`) | ⚠️  Limited; reads row groups, evaluates after |
| `IS NULL` / `IS NOT NULL` of a MAP, list or struct column; any filter casting to a floating type | Never; evaluated in DataFusion above the scan, on WAL-backed and Iceberg-only tables alike |
| Projection (column pruning)            | ✅ Parquet column-chunk skip              |
| `LIMIT`                                | ✅ Provided no inexact filters above it   |
| Aggregations (`count`, `sum`, `approx_distinct`) | ❌ Always evaluated in DataFusion |
| Joins                                  | ❌ Evaluated in DataFusion                |
| User-defined functions                 | ❌ Evaluated in DataFusion                |

**Aggregation pushdown into Iceberg is not implemented.** A `SELECT count(*) FROM iceberg.icegate.logs` reads at minimum the manifest stats and at worst every data file. Plan budget accordingly for high-cardinality scans.

**Do not use clients that treat IceGate as a remote scan source** (notably the DuckDB [`airport`](https://github.com/Query-farm/airport) community extension and the DuckDB [`duckhog`](https://github.com/Hugoberry/duckhog) extension). Those clients own the planner: they push only column projection and simple equality/range filters to the server, then pull *raw rows* over the wire and run the aggregation, function evaluation, and joins locally. On a billion-row observability table a single `count(*) GROUP BY hour` ships gigabytes to the client. Use the ADBC / JDBC / ODBC paths instead — DataFusion runs the whole query server-side and only the result set crosses the wire.

## Client compatibility

### Verified

| Client                                      | Notes                                                            |
|---------------------------------------------|------------------------------------------------------------------|
| **ADBC Python** (`adbc-driver-flightsql`)   | Recommended. Native Arrow throughout. See snippet below.         |

```python
import adbc_driver_flightsql.dbapi as fl

with fl.connect(
    uri="grpc://localhost:8815",
    db_kwargs={
        "adbc.flight.sql.rpc.call_header.x-scope-orgid": "demo",
    },
) as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM iceberg.icegate.logs")
    print(cur.fetch_arrow_table().to_pandas())
```

### To verify

The following clients claim Flight SQL support but have not yet been tested against IceGate. Each carries its own quirks (handshake requirements, type coercion, header naming, prepared-statement semantics) and is tracked as a follow-up.

| Client                                      | Known concerns                                                  |
|---------------------------------------------|-----------------------------------------------------------------|
| **Apache Arrow Flight SQL JDBC driver**     | The reference JDBC driver. Used by DBeaver, JetBrains DataGrip, IntelliJ database tools. May require non-empty Handshake payload. |
| **Apache Arrow Flight SQL ODBC driver**     | Open-source ODBC driver in the apache/arrow C++ tree. Path for Tableau, Power BI, Excel. Also the recommended bridge for DuckDB users (via DuckDB's `odbc` core extension). |
| **`flight-sql-cli`** (Apache Arrow)         | Lightweight reference client; verifies vanilla wire protocol.   |
| **DataFusion CLI** (`datafusion-cli`)       | Same DataFusion version coupling — useful for parity checks.    |
| **Tableau**                                 | Connects through JDBC or ODBC; runs schema discovery heavily on connect. |
| **Apache Superset**                         | SQLAlchemy + `flight-sql` dialect; dialect maturity varies.     |
| **Trino** (Flight SQL connector)            | Federation use case; pushdown behaviour against IceGate is the main unknown. |
| **`pyarrow.flight`** (direct, no ADBC)      | Useful for diagnostics; lower-level than ADBC.                  |
| **R: `adbcdrivermanager` + ADBC FlightSQL** | Mirrors the Python driver; good R-shop check.                   |
| **Go: ADBC Flight SQL** (`apache/arrow-adbc`) | Pure Go driver; useful for service-to-service Flight SQL.     |
| **Metabase, Looker, Redash**                | All depend on the JDBC driver behaving well.                    |

### Unsupported on purpose

- **DuckDB [`airport`](https://github.com/Query-farm/airport) community extension** — speaks a different protocol on Arrow Flight transport (msgpack-encoded custom actions, not Flight SQL `CommandStatementQuery`). Even if it appeared to connect, DuckDB would own the planner and pull raw rows over the wire. Use the ODBC bridge instead.
- **DuckDB [`duckhog`](https://github.com/Hugoberry/duckhog) extension** — unmaintained third-party Flight SQL client whose quirks (mandatory `BeginTransaction` handshakes, `user=<tenant>` connection-string parameter as identity, undocumented URL params) IceGate explicitly does not accommodate.

When adding support for a new client, the bare-minimum smoke test is:
1. Connect with the `x-scope-orgid` gRPC header set to `demo` and confirm the session lands as tenant `demo` (server-side debug log).
2. Run `SHOW TABLES` and verify the four `iceberg.icegate.*` tables appear.
3. Run `SELECT count(*) FROM iceberg.icegate.logs` and confirm a row comes back.
4. Verify `DESCRIBE iceberg.icegate.logs` does NOT list `tenant_id`.

### Known caveats

- **Handshake**: the upstream `datafusion-flight-sql-server` returns `Unimplemented` for `do_handshake` by design ([Apache Arrow guidance](https://github.com/apache/arrow/issues/23836)). Use metadata headers instead. BI tools that hard-require a handshake response need configuration tweaks or a future server-side stub.
- **Transactions**: `BeginTransaction` / `EndTransaction` return `Unimplemented`. The endpoint is stateless; clients that issue transactions unconditionally on connect will fail at that step.
- **Parameter binding**: `do_put_prepared_statement_query` is not implemented — most clients fall back to inlined literal SQL.
- **TLS**: off unless the top-level `tls` block of the query configuration is set; see `QueryConfig::tls`. Without it the endpoint serves plain gRPC and TLS is terminated at the gateway.
- **Cancellation**: queries are cancelled when the gRPC stream is dropped by the client; there is no cooperative server-side cancel beyond that.
- **Arrow `Map` columns over older drivers**: the attribute columns are `Map<Utf8, Utf8>` — `resource_attributes`, `scope_attributes`, and one record-level map per table (`log_attributes` on `logs`/`events`, `span_attributes` on `spans`, `data_point_attributes` on `metrics`). JDBC drivers <17.0 and `pyarrow` <14 may render them as `List<Struct<key, value>>`. Workaround: project-specific keys (`resource_attributes['service.version']`) which collapses to `Utf8`. Keys are stored in OTel-native dotted form, so address them as `service.version`, not `service_version`; the dot-to-underscore mapping applies only to the Loki label API.

## OpenSearch

The `opensearch` listener serves IceGate's tables through an OpenSearch 3.x-compatible HTTP API:
the Query DSL, Lucene `query_string` and PPL read the telemetry tables, and dot-prefixed system
indices hold saved objects, such as those of OpenSearch Dashboards
(`saved objects export and import round-trip`). Every query language lowers to one DataFusion
plan through one index catalog (`src/opensearch/catalog/`), and every row read goes through the
tenant-scoped catalog Flight SQL uses (`src/engine/tenant_scope/`); the two metadata reads that
do not, the map-key listing of field discovery and the `_cat/indices` figures, prune to the
request's tenant themselves. How each endpoint answers is documented on its handler under
`src/opensearch/`; this section states the contract a client relies on.

Deep dives: [Query DSL and the standard analyzer](src/query_dsl/README.md),
[PPL](src/ppl/README.md), [system indices](src/opensearch/system_index/README.md), and the
[generated Lucene and PPL parsers](../icegate-query-grammar/README.md). How the clients behave
against IceGate is pinned only by the end-to-end suite in
[`scripts/opensearch-e2e`](../../scripts/opensearch-e2e/README.md); every statement below about
a client names, in parentheses, the case that pins it.

### Endpoints

| Area | Endpoints |
|---|---|
| Cluster | `GET`/`HEAD /`, `GET /_nodes`, `GET /_cluster/health`, `GET /_cluster/settings`, `GET /_cat/indices[/<expr>]`, `GET /_cat/plugins`, `GET /_resolve/index/<expr>`, `GET /<expr>/_settings/index.number_of_shards` |
| Fields | `GET`/`POST [/<expr>]/_field_caps`, `GET [/<expr>]/_mapping`, `GET /<expr>/_mapping/field/<field>` |
| Search | `GET`/`POST [/<expr>]/_search`, `POST [/<expr>]/_msearch` |
| PPL | `POST /_plugins/_ppl`, `POST /_plugins/_ppl/_explain`, `GET /_plugins/_ppl/_grammar`, `POST /_plugins/_sql` (`SHOW TABLES LIKE '<pattern>'` only), `GET /_plugins/_query/_datasources`, `GET /_plugins/_observability/object`, `GET /_plugins/_ml/config/<id>` |
| System indices | `GET`/`HEAD`/`PUT /<index>`, `GET /_alias/<name>`, `GET /<index>/_alias`, `POST /_aliases`, `GET`/`POST /<index>/_refresh`, `GET /_cat/templates/<pattern>`, `DELETE /_template/<name>`, `PUT /<index>/_create/<id>`, `PUT`/`POST`/`GET`/`DELETE /<index>/_doc/<id>`, `POST /<index>/_update/<id>`, `POST`/`PUT [/<index>]/_bulk`, `GET`/`POST [/<index>]/_mget`, `GET`/`POST /<index>/_count`, `_search` as above, `?scroll=` with `GET`/`POST`/`DELETE /_search/scroll[/<id>]` |

`_reindex`, `_update_by_query`, `_delete_by_query` and `_tasks` are routed and refused with a 400
naming the route: IceGate runs no background document tasks. Every error the listener answers,
middleware refusals included, uses the OpenSearch error envelope with a non-empty `root_cause`,
except a PPL query's own error, which uses the PPL envelope the [PPL README](src/ppl/README.md)
states. An unmatched route or method is a 400, never a 404, 405 or 5xx. Data indices are
read-only: a write to one is a 403 `cluster_block_exception`. `filter_path` applies to every
successful JSON response; an error envelope is never filtered.

### Indices and profiles

| Index | Table | Fields |
|---|---|---|
| `logs`, `spans`, `events`, `metrics`, `operations` | the table of the same name | native: the schema's columns under their own names |
| `otel-v1-apm-span-000001`, aliased `otel-v1-apm-span` | `spans` | Data Prepper span documents (hex ids, nested `status`, `traceGroup*`; `src/opensearch/catalog/data_prepper.rs`) |
| `ss4o_logs-<tenant>`, `ss4o_traces-<tenant>`, `ss4o_metrics-<tenant>`, `<tenant>` the requesting tenant as `build_ss4o_index_name` writes it | `logs`, `spans`, `metrics` | ss4o field names (`src/opensearch/catalog/ss4o.rs`) |
| any other table the tenant's catalog lists | the table of the same name | native; resolved by its exact name only, never by a wildcard |
| names starting with `.` | — | system indices, held per tenant on object storage |

Index names match case-sensitively. `_all` or an empty expression means every index a wildcard
can reach. An expression that reaches one table through the indices of several profiles is a 400
naming the table and the profiles unless it names each of those indices explicitly, so a
wildcard, `_all` or an empty expression never returns a row twice; listings (`_cat/indices`,
`_resolve/index`) still list every index. An alias is a second name of its index, not an index:
it selects the index wherever the index's own name would, by name, wildcard or exclusion, the
index is read once however many of its names an expression reaches, and every response names the
index, never the alias. The one exception is `_mapping/field`, whose answer is keyed by the
expression as requested. `_resolve/index` lists the index with its aliases and each alias with
its index; `_cat/indices` and SQL `SHOW TABLES` list the index only. Dot-prefixed names are hidden unless
`expand_wildcards` includes `hidden` or the pattern itself starts with `.`. OpenSearch
Dashboards' index-pattern wizard therefore lists no system index until its switch for system and
hidden indices is on, and offers `timestamp` as the time field of a pattern over `logs`
(`the index pattern wizard lists logs and offers timestamp as its time field`).

### Fields

- On a native index an attribute map is an `object` whose keys are flat `keyword` leaves named
  `<column>.<stored key>`: a stored key's dots are never split, in `_field_caps`, `_mapping`,
  `_source` or PPL values, and when `http` and `http.method` are both stored keys both are
  `keyword` leaves. The ss4o indices keep a stored key's dots the same way; the span index
  `otel-v1-apm-span-000001` writes each `.` of a stored key as `@`, so the key stays one property of its nested document.
  OpenSearch Dashboards lists such keys as flat fields of an index pattern
  (`an index pattern over logs lists dotted map keys as flat fields`,
  `an index pattern over a key that is both a leaf and a prefix lists both`) and filters on them
  (`discover filters on a dotted map key through a filter pill`,
  `discover filters on a key that is both a leaf and a prefix`); Grafana's field lists and ad-hoc
  filter keys offer them (`grafana field pickers and ad-hoc filter keys list dotted map keys`).
- `_mapping` lists a native table's properties in its schema column order, and a map's keys in
  stored-key byte order; a profile index lists its fields in the profile's declared order.
- `ss4o_metrics-<tenant>` serves a histogram's `buckets` as a `nested` field of
  `{min, max, count}` objects, derived from `explicit_bounds` and `bucket_counts`
  (`src/opensearch/catalog/histogram.rs` states the open ends), and `time` as another name of
  `@timestamp`. A query leaf on `buckets` or a member path outside a nested context matches
  nothing (`ss4o_metrics_renders_histogram_buckets_as_min_max_count_objects`,
  `ss4o_metrics_lists_buckets_as_a_nested_field`).
- `_field_caps` reports exactly three metadata fields when the requested patterns select them:
  `_id` (searchable through `ids`, not aggregatable), `_index` and `_source` (neither).
- Field discovery lists the map keys of committed rows within
  `opensearch.field_discovery_window_secs` of the request; a key present only in rows the WAL has
  not committed yet is not listed. A query on any `<map>.<key>` path resolves whether or not the
  key is listed.
- On a native index `body` is `text`, matched with the standard analyzer; every other string is
  `keyword`.

### Search results

These hold for a search over data indices; a search over system indices is documented on
`search_system_indices` (`src/opensearch/system_index/search.rs`).

- No relevance scoring: every hit's `_score` and `hits.max_score` are `1.0` when a request has no
  `sort` or sorts on `_score` alone, and `null` under any other sort; `hits.max_score` is `null`
  on a page with no hits.
- No highlighting: `highlight` is accepted and no hit carries a `highlight`; PPL never returns a
  `_highlight` column, and Discover's PPL mode renders without it
  (`discover ppl renders documents and a histogram without highlight`).
- `hits.total` is exact whenever its `relation` is `eq`.
- `terminate_after` is accepted and not applied: the response then carries
  `terminated_early: false`.
- `_id` is the hex `trace_id` followed by the hex `span_id` on `spans` and `operations`, and the
  row's timestamp with a hash of the whole row elsewhere. Rows identical in every column share
  `_id` and `_doc`. Paging on `[<time field>, _doc]` never repeats a row and never skips a
  distinct row, apart from two distinct rows that share a microsecond and the low hash bits `_doc`
  keeps (`DOC_HASH_BITS` in `src/opensearch/search/doc_id.rs`; probability 2^-`DOC_HASH_BITS` per
  pair, as `decode_time_and_doc` documents). Dashboards' single-document and context views read
  rows by these values (`discover opens a single document and its context`).
- `terms` with `min_doc_count: 0` emits buckets only for keys present in the matching rows, and
  its `missing` bucket only when a matching row lacks the field.

### Aggregations

- `cardinality`, PPL `dc` and `percentile_ranks` are exact; `percentiles` and PPL `percentile` are
  approximate (t-digest).
- Exceeding `opensearch.search.max_buckets` is a 400 (`too_many_buckets_exception` inside
  `search_phase_execution_exception`), never a 503: the listener answers 503 only for
  memory-pressure shedding, the query deadline and a system-index write it could not confirm (its
  outcome is unknown), the conditions a retry can clear.
- Scripts are recognized forms, not a script language: `src/opensearch/script.rs` lists the
  forms it evaluates (arithmetic, `MovingFunctions.*`, the trace-list scripts and the
  saved-object counter update), and any other script is a 400 `illegal_argument_exception`.
  The trace-list scripts are the latency script in Grafana's form, which ends in `return 0`,
  and OpenSearch Dashboards 3.9's two Traces-table scripts: the latency script without that
  `return 0`, which answers null for a trace without a stored root span, and the last-updated
  script, which answers the root span's end time, else the span's own, as raw epoch milliseconds
  (`value` without `value_as_string`). OpenSearch Dashboards 3.9's Traces-table request answers
  in full (`tests/opensearch/osd_traces.rs`); above `opensearch.max_cross_row_traces` traces in its
  time range it is a 400 `illegal_argument_exception` naming `index` and `limit`.
  Grafana's metrics editor panels with `bucket_script` and `moving_avg` render
  (`metrics editor bucket_script and moving_avg panels render`).
- `nested` runs at the top level of a request, over a field every searched index declares
  `nested` (today `buckets` of `ss4o_metrics-<tenant>`); its `doc_count` counts the objects
  of the matching rows, and inside it only `filter` and `sum`, `min`, `max`, `value_count` and
  `avg` run, over the objects' members. Any other placement, path, kind, field or query is a 400
  naming it (`src/opensearch/search/aggs/parse.rs` and `NestedObjectScope` in
  `src/query_dsl/lower.rs` state the rules); `reverse_nested` is a 400. OpenSearch Dashboards
  3.9's Metrics bin-count request answers in full (`tests/opensearch/osd_metrics.rs`).

### `_cat` endpoints

- `_cat/indices` without `h=` lists, in this order,
  `health status index uuid pri rep docs.count docs.deleted store.size pri.store.size`.
  `docs.count`, `store.size` and `pri.store.size` are the requesting tenant's figures over
  committed rows: rows still in the WAL are not counted, a table every tenant shares
  (`GLOBAL_TABLES` in `icegate-common`) is counted whole, and a view's figures are null.
- `_cat/plugins` answers a fixed list, `opensearch-observability` and `opensearch-sql`, each
  with the node name and the version `<opensearch.version>.0`.

### Limits

No limit silently truncates rows: a size, bucket, memory or body limit is an error.

| Key | Breach |
|---|---|
| `opensearch.search.max_result_window` | 400 when `from + size` exceeds it; a `size` exactly at it with no `from` is accepted. The hits a search's `top_hits` can render, each one's `size` times its non-empty buckets summed over every `top_hits`, are bounded by it too: past it, a 400 naming the `top_hits` under `aggregation` and the window under `limit` |
| `opensearch.search.max_buckets` | 400, as above |
| `opensearch.ppl.max_result_rows` (optional) | when set, a PPL result above it is a 400 in the PPL envelope |
| `opensearch.max_query_memory_bytes` | 400 naming the limit |
| `opensearch.max_request_body_bytes` | 413 |
| `opensearch.system_indices.max_snapshot_bytes` | 413; the tenant's system indices are unchanged |
| `opensearch.system_indices.max_answer_bytes` | 400 naming the limit, which `limit` carries; an `_mget` or `_search` is refused whole, on a `_bulk` only the update item fails. The answered items of an `_msearch` share the bound: an item past it on its own fails alone, and an item that fits it alone but not what the earlier answered items left fails alone with those items' bytes also under `used_bytes`; a later item that fits what is left is answered, and a refused item takes none of the bound |
| `opensearch.system_indices.max_scroll_lifetime_secs` | 400 for a `scroll` keep-alive above it |
| `opensearch.max_cross_row_traces` | a filter, sort or aggregation on a `traceGroup*` field over more traces, or `terms` on `traceId` with a larger `size`, is a 400 |
| `engine.max_query_duration_secs` | 503 |

`opensearch.max_trace_lookback_secs` bounds the root-span lookup: for the spans a search
decorates, roots are looked for no further back than this before the earliest of those spans'
starts; a span whose root started earlier has no `traceGroup*` fields.

The defaults are documented on `OpenSearchConfig` (`src/opensearch/config.rs`) and, for Helm,
beside `query.opensearch` in `config/helm/icegate/values.yaml`.

### Version

`opensearch.version` is the version `GET /` and `/_nodes` report: a canonical
`MAJOR.MINOR.PATCH` of major 3. Any other value is refused at startup
(`icegate refuses a pre-release version at startup`). A stock OpenSearch Dashboards boots and
migrates against the default version (`osd boots and migrates against the default version`), and
Grafana's metrics editor offers its metric aggregations at the default version
(`grafana metrics editor on the spans datasource offers the metric aggregations`) and at another
canonical version (`grafana provisioned with 3.10.0 offers the metric aggregations`). An upgrade
of Dashboards from 3.8.0 to 3.9.0 keeps an index pattern and an advanced setting saved before it
(`an osd upgrade from 3.8.0 to 3.9.0 migrates the saved objects`).

For a Dashboards running with `opensearch.ignoreVersionMismatch: false`, see the operator note
beside `query.opensearch.version` in `config/helm/icegate/values.yaml`
(`a strict osd rejects icegate at 3.8.0 and migrates after icegate restarts at 3.9.0`). IceGate
cannot see the deployed Dashboards and does not compare versions with it.

### Connecting the clients

- **OpenSearch Dashboards**: one Dashboards per tenant, with `opensearch.hosts` naming the
  listener and `opensearch.customHeaders` carrying `x-scope-orgid`. With `x-scope-orgid` left out
  of `opensearch.requestHeadersWhitelist`, a header a browser sends cannot switch the tenant
  (`a browser-sent x-scope-orgid cannot switch the tenant`).
  `config/docker/opensearch-dashboards/opensearch_dashboards.yml` is the stand's configuration.
- **Grafana**: one OpenSearch datasource per index whose `database` the panels read, each
  sending `x-scope-orgid` and stating `opensearch.version` as its `version`;
  `config/docker/grafana/provisioning/datasources/datasources.yaml` provisions the stand's three.
  Open a trace by id on the datasource of the span index (its `database` is the alias
  `otel-v1-apm-span`) with a query of the form
  `traceId:<32 lowercase hex digits>` (`grafana opens a trace by id through the traceId input form`).
  On the span index Grafana's form of the latency script answers 0, never null, for a trace
  without a root span, and Grafana's trace list shows no null latency or error count
  (`the grafana trace list shows no null latency or error count`). A span's `events` are its
  stored events only, none is synthesized from its status code, and Grafana shows no error flag
  on a failing span (`a failing span shows no error flag and a span with links renders`). The
  stand's trace-to-logs link uses the field-qualified query
  `trace_id:"<trace id>" AND span_id:"<span id>"` (`trace-to-logs opens the logs of the span`):
  field-qualified id terms can prune data files, while unqualified free text reads every file of
  the time range.
- **PPL clients**: `POST /_plugins/_ppl`; the [PPL README](src/ppl/README.md) states the
  commands, functions, response formats and errors, and lists the Query Workbench and
  Observability Logs cases.

### System indices

A tenant's system indices live in one per-tenant document store on object storage, never in a
telemetry table; a scroll over them expires as its initial search set it, and a continuation
never extends that expiry. The [system-index README](src/opensearch/system_index/README.md)
describes the layout, the compare-and-swap writes, scrolls and garbage collection.

## Whose tenant a request reads

Every protocol resolves the tenant through one policy, the `tenant` block of
`query.yaml`, and the answer decides which rows the request can reach at all.

```yaml
tenant: !single
  id: "default"   # one tenant for the whole deployment
# or
tenant: !multi    # the tenant comes from x-scope-orgid, and only from there
```

The input is the `x-scope-orgid` header, and the two modes read it differently:

| Header             | `!single { id }`                  | `!multi`   |
|--------------------|-----------------------------------|------------|
| absent             | `id`                              | refused    |
| present, `== id`   | `id`                              | that value |
| present, other     | refused                           | that value |
| present, malformed | refused                           | refused    |
| present twice      | refused                           | refused    |

A `query.yaml` without a `tenant` block runs `!single` on `default`. Before the
policy existed such a deployment served a request naming another tenant that
tenant's rows, and a request with a malformed value the rows of `default`; both
are now refused. A deployment reading several tenants sets `tenant: !multi`.

`!single` consults the header rather than ignoring it, so a reader addressing
another tenant is told so instead of being served this deployment's rows under a
name it never asked for. A value is malformed unless it passes
`TenantId::is_valid` in `icegate-common`.

A refusal is the protocol's own: `400` with the protocol's error body on Loki and
Tempo, `INVALID_ARGUMENT` on Flight SQL, and `400` with an `illegal_argument_exception`
in the OpenSearch error envelope on OpenSearch. `INVALID_ARGUMENT` rather than
`UNAUTHENTICATED` because the request is malformed as far as this deployment is
concerned, and no credential would make it succeed as sent. The readiness paths
(`/ready` on Loki and Tempo, and Tempo's `/api/echo`) are exempt: no probe carries
a tenant, and refusing one would restart a pod that is serving correctly. The
OpenSearch listener exempts no path: `GET /` and `GET /_nodes` need a tenant like
every other endpoint.

`!multi` takes the tenant from whoever reaches the port: the header is believed
as sent, and this crate authenticates nobody. So under `!multi` the listeners are
reachable only by callers already trusted to name any tenant — an untrusted
reader on that port reads every tenant by choosing the header value. PromQL is
the one protocol outside all of this: every one of its handlers is a `501` stub
that reads no tenant.

Each refusal increments `icegate_query_tenant_rejections`, a counter with two
labels:

| Label      | Values                                        |
|------------|-----------------------------------------------|
| `protocol` | `loki`, `tempo`, `flight_sql`, `opensearch`   |
| `reason`   | `missing`, `invalid`, `duplicate_header`      |

`missing` is an absent header under `!multi`; `invalid` is a malformed value, or
one naming a tenant a `!single` deployment does not serve; `duplicate_header` is
the header present more than once, which is refused even when the two values
agree — two values mean the sender and something on the path disagree, and
picking either resolves that disagreement silently.

## Configuration

Each protocol has its own block in `query.yaml`:

```yaml
loki:        { enabled: true,  host: 0.0.0.0, port: 3100 }
prometheus:  { enabled: true,  host: 0.0.0.0, port: 9090 }
tempo:       { enabled: true,  host: 0.0.0.0, port: 3200 }
flight_sql:  { enabled: true,  host: 0.0.0.0, port: 8815, max_message_size: 16777216 }
```

Set `enabled: false` on any block to leave its listener unbound. All five servers share one `QueryEngine`, so disabling one does not free engine resources.

The `opensearch` block has more keys than a listener address (limits, the reported version,
the system-index store); `OpenSearchConfig` in `src/opensearch/config.rs` documents each key with
its default, and the Helm chart exposes them under `query.opensearch`.

## Architecture in one sentence

Five protocol-specific handlers (`crate::loki`, `crate::prometheus`, `crate::tempo`, `crate::flight_sql`, `crate::opensearch`) translate their respective wire formats into DataFusion SQL/`LogicalPlan` and run them against a shared engine that surfaces Iceberg cold storage merged with WAL hot data; the tenant of a request is resolved once at the edge — a middleware layer on the HTTP routers, a tonic interceptor on Flight SQL — and the isolation it decides is then enforced in the `TableProvider` layer for Flight SQL and OpenSearch and in the query executors for the other HTTP APIs.
