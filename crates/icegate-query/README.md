# icegate-query

Query APIs for IceGate. A single Rust binary that exposes the same DataFusion + Iceberg engine through four wire protocols, so existing observability tooling, BI clients, and ad-hoc analytics tools can all read from the same underlying tables.

## Supported protocols

| Protocol                      | Default port | Transport | Status   | Notes                                                |
|-------------------------------|--------------|-----------|----------|------------------------------------------------------|
| Loki HTTP API (LogQL)         | `3100`       | HTTP/JSON | Stable   | Grafana, `logcli`, Promtail clients                  |
| Prometheus HTTP API (PromQL)  | `9090`       | HTTP/JSON | Stable   | Grafana, `promtool`, alerting tools                  |
| Tempo HTTP API (TraceQL)      | `3200`       | HTTP/JSON | Stable   | Grafana, `tempo-cli`, OTel tracing UIs               |
| Apache Arrow **Flight SQL**   | `8815`       | gRPC      | Beta     | BI tools, ADBC, JDBC, ad-hoc SQL                     |

All four share one `QueryEngine` (cached Iceberg catalog provider + DataFusion `SessionContext`); enabling more than one only costs the additional listener.

## Flight SQL

The Flight SQL endpoint exposes the IceGate data lake as a generic SQL warehouse over gRPC. Client SQL is parsed and executed by DataFusion against the merged WAL + Iceberg view of the four observability tables: `logs`, `spans`, `events`, `metrics`. There is no IceGate-specific query language to learn — just standard SQL.

### Address & catalog layout

```
grpc://<host>:8815                  # plaintext gRPC (TLS terminated at the gateway)
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

The Flight SQL session reads the tenant identifier from two ingress paths, tried in order:

1. **`x-scope-orgid` gRPC metadata** — explicit and preferred. Same Grafana convention used by the HTTP APIs.
2. **Basic Auth username** — extracted from the `authorization` header (`Basic <base64(user:pass)>`). For clients that drop unknown URL params (DuckDB `duckhog`), `user=<tenant>` in the connection string becomes the tenant identifier.

If neither yields a value matching `[A-Za-z0-9_-]+`, the request falls back to the shared `default` tenant. A diagnostic log lists every ASCII metadata key on the request whenever the fallback fires, so you can see exactly what the client sent (values are never logged — they may carry auth tokens).

There is **no password verification** — the Flight SQL endpoint trusts the gateway in front of it, exactly like the HTTP APIs trust their `X-Scope-OrgID` header.

### Isolation guarantees

Defense-in-depth wrapped around the iceberg catalog:

- Every `TableProvider::scan(...)` AND's `tenant_id = '<t>'` into the filter list before delegating to the underlying Iceberg provider. The filter is injected **after** DataFusion's pushdown analysis so no planner pass can strip it.
- The `tenant_id` column is **removed from the schema** the server advertises. `DESCRIBE`, `information_schema.columns`, `SELECT *`, and Flight SQL `get_tables(include_schema=true)` all reflect the filtered schema.
- Referencing `tenant_id` in `WHERE` or `SELECT` fails at SQL planning time with "column not found".
- `iceberg` is partitioned on `tenant_id` (identity transform), so the injected filter is `Exact` pushdown — manifests for other tenants are pruned before any row group is touched.

### Read-only

DDL (`CREATE`/`DROP`/`ALTER`) and DML (`INSERT`/`UPDATE`/`DELETE`) are rejected by DataFusion `SQLOptions` after parsing but before execution. `EXPLAIN`, `SHOW`, and `SET` are allowed.

Transactions (`BeginTransaction` / `EndTransaction`) are accepted as no-ops so that clients which begin a transaction on connect (DuckDB `duckhog`, some JDBC drivers) can proceed to run queries. No transactional semantics are actually engaged.

### Pushdown — what runs server-side vs client-side

| Operation                              | Pushdown into Iceberg scan                |
|----------------------------------------|-------------------------------------------|
| `tenant_id` partition predicate        | ✅ Always, automatically injected         |
| Time-range filters on `timestamp`      | ✅ Iceberg partition pruning              |
| Equality / range filters on indexed columns | ✅ Parquet statistics                |
| Filters on `attributes[...]` map keys  | ⚠️  Limited; reads row groups, evaluates after |
| Projection (column pruning)            | ✅ Parquet column-chunk skip              |
| `LIMIT`                                | ✅ Provided no inexact filters above it   |
| Aggregations (`count`, `sum`, `approx_distinct`) | ❌ Always evaluated in DataFusion |
| Joins                                  | ❌ Evaluated in DataFusion                |
| User-defined functions                 | ❌ Evaluated in DataFusion                |

**Aggregation pushdown into Iceberg is not implemented.** A `SELECT count(*) FROM iceberg.icegate.logs` reads at minimum the manifest stats and at worst every data file. Plan budget accordingly for high-cardinality scans.

For clients that use IceGate only as a remote scan source (e.g. DuckDB+duckhog), be aware that the client's planner often pulls raw data to its own process and runs the aggregation locally — performance is bound by data transfer volume in that case.

## Client compatibility

### Verified

| Client                                      | Auth path        | Notes                                                            |
|---------------------------------------------|------------------|------------------------------------------------------------------|
| **ADBC Python** (`adbc-driver-flightsql`)   | Metadata header or Basic | Recommended. Native Arrow throughout. See snippet below. |
| **DuckDB `duckhog` extension**              | Basic Auth user  | Uses IceGate as a remote scan source; aggregations run inside DuckDB. Drops unknown URL params, so set the tenant via `user=<tenant>` in the connection string. |

```python
import adbc_driver_flightsql.dbapi as fl

with fl.connect(
    uri="grpc://localhost:8815",
    db_kwargs={
        "username": "demo",
        "password": "anything",
        # or use the explicit header:
        # "adbc.flight.sql.rpc.call_header.x-scope-orgid": "demo",
    },
) as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM iceberg.icegate.logs")
    print(cur.fetch_arrow_table().to_pandas())
```

```sql
-- DuckDB + duckhog
INSTALL duckhog FROM community;
LOAD duckhog;
ATTACH 'hog:iceberg?user=demo&password=anything&flight_server=grpc://localhost:8815' AS iceberg;
SELECT approx_count_distinct(trace_id) FROM iceberg.icegate.logs;
```

### To verify

The following clients claim Flight SQL support but have not yet been tested against IceGate. Each carries its own quirks (handshake requirements, type coercion, header naming, prepared-statement semantics) and is tracked as a follow-up.

| Client                                      | Likely auth path         | Known concerns                                                  |
|---------------------------------------------|--------------------------|-----------------------------------------------------------------|
| **Apache Arrow Flight SQL JDBC driver**     | URL property             | The reference JDBC driver. Used by DBeaver, JetBrains DataGrip, IntelliJ database tools. May require non-empty Handshake payload. |
| **`flight-sql-cli`** (Apache Arrow)         | `--header`               | Lightweight reference client; verifies vanilla wire protocol.   |
| **DataFusion CLI** (`datafusion-cli`)       | `--flight-sql-host`      | Same DataFusion version coupling — useful for parity checks.    |
| **Tableau**                                 | Via JDBC                 | Connects through the JDBC driver above; runs schema discovery heavily on connect. |
| **Apache Superset**                         | SQLAlchemy + `flight-sql` dialect | Dialect maturity varies; check `flight_sql_sqlalchemy_dialect` updates. |
| **Trino** (Flight SQL connector)            | Catalog properties       | Federation use case; pushdown behaviour against IceGate is the main unknown. |
| **`pyarrow.flight`** (direct, no ADBC)      | Custom middleware        | Useful for diagnostics; lower-level than ADBC.                  |
| **R: `adbcdrivermanager` + ADBC FlightSQL** | Same as ADBC Python      | Mirrors the Python driver; good R-shop check.                   |
| **Go: ADBC Flight SQL** (`apache/arrow-adbc`) | Same as ADBC Python    | Pure Go driver; useful for service-to-service Flight SQL.       |
| **Metabase, Looker, Redash**                | JDBC                     | All depend on the JDBC driver behaving well.                    |

When adding support for a new client, the bare-minimum smoke test is:
1. Connect with `user=demo&password=anything` (or the equivalent header) and confirm the session lands as tenant `demo` (server-side debug log).
2. Run `SHOW TABLES` and verify the four `iceberg.icegate.*` tables appear.
3. Run `SELECT count(*) FROM iceberg.icegate.logs` and confirm a row comes back.
4. Verify `DESCRIBE iceberg.icegate.logs` does NOT list `tenant_id`.

### Known caveats

- **Handshake**: the upstream `datafusion-flight-sql-server` returns `Unimplemented` for `do_handshake` by design ([Apache Arrow guidance](https://github.com/apache/arrow/issues/23836)). Use metadata headers or Basic Auth instead. BI tools that hard-require a handshake response need configuration tweaks or a future server-side stub.
- **Parameter binding**: `do_put_prepared_statement_query` is not implemented — most clients fall back to inlined literal SQL.
- **TLS**: no built-in TLS; terminate at the gateway. Plain gRPC only.
- **Cancellation**: queries are cancelled when the gRPC stream is dropped by the client; there is no cooperative server-side cancel beyond that.
- **Arrow `Map` column over older drivers**: the `attributes` column is `Map<Utf8, Utf8>`. JDBC drivers <17.0 and `pyarrow` <14 may render it as `List<Struct<key, value>>`. Workaround: project specific keys (`attributes['service.version']`) which collapses to `Utf8`.

## Configuration

Each protocol has its own block in `query.yaml`:

```yaml
loki:        { enabled: true,  host: 0.0.0.0, port: 3100 }
prometheus:  { enabled: true,  host: 0.0.0.0, port: 9090 }
tempo:       { enabled: true,  host: 0.0.0.0, port: 3200 }
flight_sql:  { enabled: true,  host: 0.0.0.0, port: 8815, max_message_size: 16777216 }
```

Set `enabled: false` on any block to leave its listener unbound. All four servers share one `QueryEngine`, so disabling one does not free engine resources.

## Architecture in one sentence

Four protocol-specific handlers (`crate::loki`, `crate::prometheus`, `crate::tempo`, `crate::flight_sql`) translate their respective wire formats into DataFusion SQL/`LogicalPlan` and run them against a shared engine that surfaces Iceberg cold storage merged with WAL hot data; tenant isolation lives in the `TableProvider` layer for Flight SQL and in route handlers for the HTTP APIs.
