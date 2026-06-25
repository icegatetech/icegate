# Iceberg Schema DDL for OpenTelemetry Data

This document contains Trino SQL DDL statements for creating Iceberg tables to store OpenTelemetry logs, spans (traces), events, and metrics data.

**Catalog:** `iceberg`
**Namespace:** `triplecloud`
**Format:** Apache Iceberg with Parquet file format

## Table of Contents

- [Overview](#overview)
- [Performance Optimizations](#performance-optimizations)
- [Schema Definitions](#schema-definitions)
  - [1. Logs Table](#1-logs-table)
  - [2. Spans Table (Traces)](#2-spans-table-traces)
  - [3. Events Table](#3-events-table)
  - [4. Metrics Table](#4-metrics-table)
  - [5. Operations Table](#5-operations-table)
- [Post-Creation Tasks](#post-creation-tasks)
- [Type Mappings](#type-mappings)
- [Migration Guide](#migration-guide)
  - [Trace and Span ID storage change (GH-135)](#trace-and-span-id-storage-change-gh-135)

---

## Overview

These tables implement the OpenTelemetry Protocol (OTLP) data model for observability data:

- **Logs:** Application log records with severity, body, and attributes
- **Spans:** Distributed trace spans with events and links
- **Events:** Semantic events extracted from logs based on event_name field
- **Metrics:** All metric types (gauge, sum, histogram, exponential histogram, summary)

All tables include:
- Multi-tenancy support (tenant_id)
- Time-based partitioning for efficient querying
- Sorted data for better predicate pushdown
- W3C trace context fields for correlation

---

## Performance Optimizations

The tables use the following Parquet optimizations:

| Property | Value | Purpose |
|----------|-------|---------|
| **Row Group Size** | 64 MB | Balance between compression efficiency and parallelism |
| **Page Size** | 1 MB | Standard default for good read granularity |
| **Page Row Limit** | 20,000 rows | Controls scanning granularity |
| **Compression** | ZSTD | Best balance of compression ratio and speed |

**Partitioning Strategy:**
- `tenant_id` (identity) - Multi-tenancy isolation
- `day(timestamp)` - Time-based partitioning for efficient time-range queries

**Sorting Strategy (table-specific):**
- **Logs/Events:** `service_name` + `timestamp DESC` - Groups by service, recent-first
- **Spans:** `service_name` + `trace_id` + `timestamp DESC` - Groups by service and trace, recent-first
- **Metrics:** `metric_name` + `service_name` + `service_instance_id` + `timestamp DESC` - Groups by metric, service, and service instance, recent-first
- **Operations:** `trace_id` + `timestamp DESC` - Groups a trace's LLM operations together, recent-first (no `service_name` leg; D10)

---

## Schema Definitions

### 1. Logs Table

Based on OpenTelemetry LogRecord message from `opentelemetry/proto/logs/v1/logs.proto`.

```sql
-- Create the logs table
CREATE TABLE iceberg.triplecloud.logs (
    -- Multi-tenancy field
    tenant_id VARCHAR NOT NULL,
    service_name VARCHAR,            -- Optional for flexibility

    -- Timestamp fields (microsecond precision)
    timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    observed_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    ingested_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    -- W3C trace context (for correlation with spans)
    trace_id VARBINARY,  -- 16-byte W3C trace ID (FIXED_LEN_BYTE_ARRAY(16))
    span_id VARBINARY,   -- 8-byte W3C span ID (FIXED_LEN_BYTE_ARRAY(8))

    -- Severity information
    severity_text VARCHAR,

    -- Log body (simplified from AnyValue variant to String)
    body VARCHAR,                    -- Optional - may be empty for some log records

    -- Attributes (merged from resource, scope, and log-level attributes)
    attributes MAP(VARCHAR, VARCHAR) NOT NULL
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'day(timestamp)'],
    sorted_by = ARRAY['service_name', 'timestamp DESC'],  -- Recent-first within service
    compression_codec = 'ZSTD',
    format_version = 2
);

-- Set Iceberg-specific Parquet properties for optimal performance
ALTER TABLE iceberg.triplecloud.logs SET PROPERTIES
    'write.parquet.row-group-size-bytes' = '67108864',      -- 64 MB row groups
    'write.parquet.page-size-bytes' = '1048576',            -- 1 MB page size
    'write.parquet.page-row-limit' = '20000',               -- 20k rows per page
    'write.parquet.dict-size-bytes' = '2097152',            -- 2 MB dictionary
    'write.parquet.compression-codec' = 'zstd';

-- Create table comment
COMMENT ON TABLE iceberg.triplecloud.logs IS
    'OpenTelemetry log records with severity, body, and merged attributes from resource/scope/log levels';
```

---

### 2. Spans Table (Traces)

Based on OpenTelemetry Span message from `opentelemetry/proto/trace/v1/trace.proto`.

```sql
-- Create the spans table
CREATE TABLE iceberg.triplecloud.spans (
    -- Multi-tenancy field
    tenant_id VARCHAR NOT NULL,
    service_name VARCHAR,            -- Optional for flexibility

    -- Trace identifiers (W3C trace context) - sorted by these for trace analysis
    trace_id VARBINARY NOT NULL,      -- 16-byte W3C trace ID (FIXED_LEN_BYTE_ARRAY(16))
    span_id VARBINARY NOT NULL,       -- 8-byte W3C span ID (FIXED_LEN_BYTE_ARRAY(8))
    parent_span_id VARBINARY,         -- 8-byte W3C span ID (FIXED_LEN_BYTE_ARRAY(8)); all-zero or null for root spans

    -- Timestamp fields
    timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    end_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    ingested_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    duration_micros BIGINT NOT NULL,

    trace_state VARCHAR,

    -- Span metadata
    name VARCHAR NOT NULL,
    kind INTEGER,                      -- SpanKind enum value
    status_code INTEGER,               -- StatusCode enum value
    status_message VARCHAR,

    -- Attributes (merged from resource, scope, and span attributes)
    attributes MAP(VARCHAR, VARCHAR) NOT NULL,

    -- Flags and monitoring
    flags INTEGER,
    dropped_attributes_count INTEGER,
    dropped_events_count INTEGER,
    dropped_links_count INTEGER,

    -- Nested events (NO trace_id/span_id - inherits from parent span)
    events ARRAY(ROW(
        timestamp TIMESTAMP(6) WITH TIME ZONE,
        name VARCHAR,
        attributes MAP(VARCHAR, VARCHAR),
        dropped_attributes_count INTEGER
    )),

    -- Nested links (HAS trace_id/span_id - references linked span)
    links ARRAY(ROW(
        trace_id VARBINARY,           -- 16-byte W3C trace ID (FIXED_LEN_BYTE_ARRAY(16))
        span_id VARBINARY,            -- 8-byte W3C span ID (FIXED_LEN_BYTE_ARRAY(8))
        trace_state VARCHAR,
        attributes MAP(VARCHAR, VARCHAR),
        dropped_attributes_count INTEGER,
        flags INTEGER
    ))
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'day(timestamp)'],
    sorted_by = ARRAY['service_name', 'trace_id', 'timestamp DESC'],  -- Recent-first within service and trace
    compression_codec = 'ZSTD',
    format_version = 2
);

-- Set Iceberg-specific Parquet properties
ALTER TABLE iceberg.triplecloud.spans SET PROPERTIES
    'write.parquet.row-group-size-bytes' = '67108864',      -- 64 MB row groups
    'write.parquet.page-size-bytes' = '1048576',            -- 1 MB page size
    'write.parquet.page-row-limit' = '20000',               -- 20k rows per page
    'write.parquet.dict-size-bytes' = '2097152',            -- 2 MB dictionary
    'write.parquet.compression-codec' = 'zstd';

-- Create table comment
COMMENT ON TABLE iceberg.triplecloud.spans IS
    'OpenTelemetry distributed trace spans with nested events and links, merged attributes from resource/scope/span levels';
```

---

### 3. Events Table

OpenTelemetry semantic events extracted from logs based on event_name field.
See: https://opentelemetry.io/docs/specs/semconv/general/events/

```sql
-- Create the events table
CREATE TABLE iceberg.triplecloud.events (
    -- Multi-tenancy field
    tenant_id VARCHAR NOT NULL,
    service_name VARCHAR,            -- Optional for flexibility

    -- Timestamp fields
    timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    observed_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    ingested_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    -- Event identification
    event_domain VARCHAR NOT NULL,
    event_name VARCHAR NOT NULL,

    -- Trace context (for correlation)
    trace_id VARBINARY,               -- 16-byte W3C trace ID (FIXED_LEN_BYTE_ARRAY(16))
    span_id VARBINARY,                -- 8-byte W3C span ID (FIXED_LEN_BYTE_ARRAY(8))

    -- Event attributes
    attributes MAP(VARCHAR, VARCHAR) NOT NULL
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'day(timestamp)'],
    sorted_by = ARRAY['service_name', 'timestamp DESC'],  -- Recent-first within service
    compression_codec = 'ZSTD',
    format_version = 2
);

-- Set Iceberg-specific Parquet properties
ALTER TABLE iceberg.triplecloud.events SET PROPERTIES
    'write.parquet.row-group-size-bytes' = '67108864',      -- 64 MB row groups
    'write.parquet.page-size-bytes' = '1048576',            -- 1 MB page size
    'write.parquet.page-row-limit' = '20000',               -- 20k rows per page
    'write.parquet.dict-size-bytes' = '2097152',            -- 2 MB dictionary
    'write.parquet.compression-codec' = 'zstd';

-- Create table comment
COMMENT ON TABLE iceberg.triplecloud.events IS
    'OpenTelemetry semantic events extracted from logs, identified by event_domain and event_name fields';
```

---

### 4. Metrics Table

Based on OpenTelemetry Metric and DataPoint messages from `opentelemetry/proto/metrics/v1/metrics.proto`.
Combines all metric types (gauge, sum, histogram, exponential_histogram, summary) into a single table.

Data points that fail strict OTLP validation (unset value, unspecified aggregation temporality,
out-of-range count/flags, non-finite value, inconsistent histogram buckets, quantile outside `[0, 1]`,
or out-of-range exponential-histogram scale) are dropped at ingest and reported via OTLP partial success.

```sql
-- Create the metrics table
CREATE TABLE iceberg.triplecloud.metrics (
    -- Multi-tenancy field
    tenant_id VARCHAR NOT NULL,
    service_name VARCHAR,     -- Optional for flexibility
    service_instance_id VARCHAR,     -- Optional for flexibility

    -- Timestamp fields
    timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    start_timestamp TIMESTAMP(6) WITH TIME ZONE,
    ingested_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    -- Metric identification
    metric_name VARCHAR NOT NULL,
    metric_type VARCHAR NOT NULL,         -- gauge, sum, histogram, exponential_histogram, summary
    description VARCHAR,
    unit VARCHAR,

    -- Metric metadata
    aggregation_temporality VARCHAR,      -- DELTA or CUMULATIVE
    is_monotonic BOOLEAN,

    -- Attributes (merged from resource, scope, and metric/data point attributes)
    attributes MAP(VARCHAR, VARCHAR) NOT NULL,

    -- Value fields (for gauge and sum metrics)
    value_double DOUBLE,
    value_int BIGINT,

    -- Common histogram fields (for histogram, exponential_histogram, and summary)
    count BIGINT,
    sum DOUBLE,
    min DOUBLE,
    max DOUBLE,

    -- Standard histogram fields
    bucket_counts ARRAY(BIGINT),
    explicit_bounds ARRAY(DOUBLE),

    -- Exponential histogram fields
    scale INTEGER,
    zero_count BIGINT,
    zero_threshold DOUBLE,
    positive_offset INTEGER,
    positive_bucket_counts ARRAY(BIGINT),
    negative_offset INTEGER,
    negative_bucket_counts ARRAY(BIGINT),

    -- Summary fields
    quantile_values ARRAY(ROW(
        quantile DOUBLE,
        value DOUBLE
    )),

    -- Flags and exemplars
    flags INTEGER,
    exemplars ARRAY(ROW(
        timestamp TIMESTAMP(6) WITH TIME ZONE,
        value_double DOUBLE,
        value_int BIGINT,
        span_id VARBINARY,                -- 8-byte W3C span ID (FIXED_LEN_BYTE_ARRAY(8))
        trace_id VARBINARY,               -- 16-byte W3C trace ID (FIXED_LEN_BYTE_ARRAY(16))
        attributes MAP(VARCHAR, VARCHAR)
    )),

    -- OTLP Metric.metadata (additional KeyValue metadata describing the metric)
    metadata MAP(VARCHAR, VARCHAR)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'day(timestamp)'],
    sorted_by = ARRAY['metric_name', 'service_name', 'service_instance_id', 'timestamp DESC'],  -- Recent-first within metric, service, and service instance
    compression_codec = 'ZSTD',
    format_version = 2
);

-- Set Iceberg-specific Parquet properties
ALTER TABLE iceberg.triplecloud.metrics SET PROPERTIES
    'write.parquet.row-group-size-bytes' = '67108864',      -- 64 MB row groups
    'write.parquet.page-size-bytes' = '1048576',            -- 1 MB page size
    'write.parquet.page-row-limit' = '20000',               -- 20k rows per page
    'write.parquet.dict-size-bytes' = '2097152',            -- 2 MB dictionary
    'write.parquet.compression-codec' = 'zstd';

-- Create table comment
COMMENT ON TABLE iceberg.triplecloud.metrics IS
    'OpenTelemetry metrics (gauge, sum, histogram, exponential histogram, summary) with merged attributes from resource/scope/metric levels';
```

---

### 5. Operations Table

A typed columnar projection icegate maintains over the LLM/GenAI-flavoured subset of `spans`.
Every span carrying at least one LLM/GenAI marker attribute (the union of the OTEL GenAI,
OpenInference, and Traceloop conventions) is projected into exactly one `operations` row
(1:1 by `span_id`), with semantic-convention attributes normalized into typed columns.

Spans whose typed attributes fail strict parsing (non-numeric `temperature`, non-bool
`stream`, token overflow, etc.) are dropped from `operations` at ingest and counted; the
underlying span still lands in `spans`. The `operations` write is best-effort and never
fails the traces OTLP response.

```sql
-- Create the operations table
CREATE TABLE iceberg.triplecloud.operations (
    -- Multi-tenancy field
    tenant_id VARCHAR NOT NULL,

    -- Identity (mirrored from spans / OTLP scope)
    trace_id VARBINARY NOT NULL,          -- 16-byte W3C trace ID (FIXED_LEN_BYTE_ARRAY(16))
    span_id VARBINARY NOT NULL,           -- 8-byte W3C span ID (FIXED_LEN_BYTE_ARRAY(8))
    parent_span_id VARBINARY,             -- 8-byte; NULL for root
    service_name VARCHAR,                  -- Optional; no unknown_service default
    scope_name VARCHAR,                    -- OTLP scope_spans.scope.name
    scope_version VARCHAR,                 -- OTLP scope_spans.scope.version

    -- Timing
    timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,       -- span start; partition + sort source
    end_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    duration_micros BIGINT NOT NULL,                      -- (end - start).max(0) micros
    ingested_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    -- Discrimination
    operation_name VARCHAR NOT NULL,       -- chat | embeddings | retrieval | execute_tool | ... | other

    -- Provider / model
    provider_name VARCHAR,
    request_model VARCHAR,
    response_model VARCHAR,
    response_id VARCHAR,

    -- Sampling
    temperature DOUBLE,
    top_p DOUBLE,
    top_k BIGINT,
    max_tokens BIGINT,
    frequency_penalty DOUBLE,
    presence_penalty DOUBLE,
    seed BIGINT,
    stream BOOLEAN,
    choice_count BIGINT,
    output_type VARCHAR,                   -- text | json | image | speech
    reasoning_effort VARCHAR,

    -- Response
    time_to_first_chunk_ms BIGINT,         -- source seconds x1000

    -- Tokens (non-negative or NULL; never zero-as-missing)
    input_tokens BIGINT,
    output_tokens BIGINT,
    total_tokens BIGINT,
    reasoning_tokens BIGINT,
    cache_creation_input_tokens BIGINT,
    cache_read_input_tokens BIGINT,

    -- Identity context
    conversation_id VARCHAR,
    user_id VARCHAR,

    -- Tool (operation_name = 'execute_tool')
    tool_name VARCHAR,
    tool_call_id VARCHAR,
    tool_type VARCHAR,
    tool_description VARCHAR,

    -- Retrieval (operation_name = 'retrieval')
    data_source_id VARCHAR,

    -- Embeddings (operation_name = 'embeddings')
    embedding_dimensions INTEGER,

    -- Server / status
    server_address VARCHAR,
    server_port INTEGER,
    status_code INTEGER,                   -- OTLP status enum (0=unset->NULL, 1=ok, 2=error)
    status_message VARCHAR,
    error_type VARCHAR,

    -- Agent / workflow
    agent_id VARCHAR,
    agent_name VARCHAR,
    agent_version VARCHAR,
    agent_description VARCHAR,
    workflow_name VARCHAR,

    -- Content (opt-in; JSON-encoded String; icegate stores raw, trust redacts at app layer)
    input_messages VARCHAR,
    output_messages VARCHAR,
    system_instructions VARCHAR,
    tool_definitions VARCHAR,
    tool_call_arguments VARCHAR,
    tool_call_result VARCHAR,

    -- List columns (placed last for contiguous Iceberg field IDs)
    stop_sequences ARRAY(VARCHAR),
    finish_reasons ARRAY(VARCHAR),
    encoding_formats ARRAY(VARCHAR)        -- float | base64
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'day(timestamp)'],
    sorted_by = ARRAY['trace_id', 'timestamp DESC'],  -- Cluster a trace's operations, recent-first
    compression_codec = 'ZSTD',
    format_version = 2
);

-- Set Iceberg-specific Parquet properties
ALTER TABLE iceberg.triplecloud.operations SET PROPERTIES
    'write.parquet.row-group-size-bytes' = '67108864',      -- 64 MB row groups
    'write.parquet.page-size-bytes' = '1048576',            -- 1 MB page size
    'write.parquet.page-row-limit' = '20000',               -- 20k rows per page
    'write.parquet.dict-size-bytes' = '2097152',            -- 2 MB dictionary
    'write.parquet.compression-codec' = 'zstd';

-- Create table comment
COMMENT ON TABLE iceberg.triplecloud.operations IS
    'Typed columnar projection over the LLM/GenAI subset of spans (1:1 by span_id), normalizing gen_ai.* / OpenInference / Traceloop attributes into typed columns';
```

---

## Post-Creation Tasks

After creating the tables, run these commands to optimize query performance:

```sql
-- Collect statistics for better query planning
ANALYZE iceberg.triplecloud.logs;
ANALYZE iceberg.triplecloud.spans;
ANALYZE iceberg.triplecloud.events;
ANALYZE iceberg.triplecloud.metrics;
ANALYZE iceberg.triplecloud.operations;
```

### Query Examples

```sql
-- Query logs for a specific service in a time range
SELECT
    timestamp,
    severity_text,
    body,
    attributes
FROM iceberg.triplecloud.logs
WHERE service_name = 'api-service'
  AND timestamp >= TIMESTAMP '2025-01-01 00:00:00 UTC'
  AND timestamp < TIMESTAMP '2025-01-02 00:00:00 UTC'
ORDER BY timestamp;

-- Query slowest spans for a specific tenant on a given day
SELECT
    trace_id,
    span_id,
    name,
    duration_micros / 1000.0 AS duration_ms
FROM iceberg.triplecloud.spans
WHERE tenant_id = 'tenant-123'
  AND date(timestamp) = DATE '2025-01-01'
ORDER BY duration_micros DESC
LIMIT 10;

-- Query metrics by type
SELECT
    metric_name,
    metric_type,
    timestamp,
    value_double,
    count,
    sum
FROM iceberg.triplecloud.metrics
WHERE tenant_id = 'tenant-123'
  AND date(timestamp) = DATE '2025-01-01'
  AND metric_type = 'histogram'
ORDER BY timestamp;

-- Correlate logs with spans using trace context
SELECT
    l.timestamp AS log_time,
    l.severity_text,
    l.body,
    s.name AS span_name,
    s.duration_micros / 1000.0 AS span_duration_ms
FROM iceberg.triplecloud.logs l
INNER JOIN iceberg.triplecloud.spans s
    ON l.trace_id = s.trace_id
    AND l.span_id = s.span_id
WHERE l.service_name = 'api-service'
  AND date(l.timestamp) = DATE '2025-01-01'
  AND l.severity_text = 'ERROR';
```

---

## Type Mappings

Rust types from `schema.rs` mapped to Trino SQL types:

| Rust Type | Trino SQL Type | Notes |
|-----------|----------------|-------|
| `String` | `VARCHAR` | Variable-length string |
| `Int` | `INTEGER` | 32-bit signed integer |
| `Long` | `BIGINT` | 64-bit signed integer |
| `Boolean` | `BOOLEAN` | True/false value |
| `Double` | `DOUBLE` | 64-bit floating point |
| `Timestamp` | `TIMESTAMP(6) WITH TIME ZONE` | Microsecond precision with timezone |
| `Fixed(16)` (trace_id) | `VARBINARY` | 16-byte W3C trace ID, stored as `FIXED_LEN_BYTE_ARRAY(16)` — see [Migration Guide](#migration-guide) |
| `Fixed(8)` (span_id, parent_span_id) | `VARBINARY` | 8-byte W3C span ID, stored as `FIXED_LEN_BYTE_ARRAY(8)` — see [Migration Guide](#migration-guide) |
| `Map<String, String>` | `MAP(VARCHAR, VARCHAR)` | Key-value pairs |
| `List<T>` | `ARRAY(T)` | Array of elements |
| `Struct` | `ROW(...)` | Nested structure with named fields |

---

## Best Practices

### Partitioning
- The `day(timestamp)` partition is critical for time-range queries
- Avoid over-partitioning (creating too many small partitions)
- Monitor partition sizes - aim for partitions with 100+ MB of data

### Sorting
- Sorting by `service_name` (or `metric_name` on the metrics table) first provides good data locality within tenant partitions
- Additional sort columns (`trace_id`, `service_instance_id`) enable efficient predicate pushdown on sorted data
- Descending timestamp ordering (recent-first) optimizes for common query patterns accessing recent data

### Compression
- ZSTD provides excellent compression with good read/write performance
- Alternative: Use SNAPPY for faster writes if compression ratio is less critical

### Query Optimization
- **Partition columns:** `tenant_id` and `day(timestamp)` - filtering on these enables partition pruning
- **Sort columns:** `service_name`, `metric_name`, `trace_id`, `service_instance_id` - filtering on these benefits from optimized scanning of sorted data (predicate pushdown)
- Use `date(timestamp)` in WHERE clauses to enable partition pruning
- Filter on both partition and sort columns for best performance
- Run `ANALYZE` after bulk data loads to update statistics

### Maintenance
```sql
-- Check table properties
SHOW CREATE TABLE iceberg.triplecloud.logs;

-- View table statistics
SHOW STATS FOR iceberg.triplecloud.logs;

-- Optimize table files (compact small files)
ALTER TABLE iceberg.triplecloud.logs EXECUTE optimize;

-- Expire old snapshots (keep 7 days of history)
ALTER TABLE iceberg.triplecloud.logs EXECUTE expire_snapshots(retention_threshold => '7d');
```

---

**Version:** 1.4
**Last Updated:** 2026-06-25
**Schema Source:** `src/common/schema.rs`

**Notable Changes in v1.4:**
- Added the `operations` table (5th physical table): a typed columnar projection over the LLM/GenAI subset of `spans` (1:1 by `span_id`), partitioned `tenant_id` + `day(timestamp)`, sorted `trace_id` + `timestamp DESC` (no `service_name` leg; D10).

**Notable Changes in v1.3:**
- Removed `cloud_account_id` column from logs, spans, events, and metrics tables.
- Updated sort orders to drop the leading `cloud_account_id`:
  - Logs/Events: `service_name` → `timestamp DESC`
  - Spans: `service_name` → `trace_id` → `timestamp DESC`
  - Metrics: `metric_name` → `service_name` → `service_instance_id` → `timestamp DESC`
- Top-level field IDs shifted down by one to fill the slot freed by `cloud_account_id`; nested-type IDs (maps, lists, struct children) shift accordingly with no gaps.
- `cloud.account.id` resource attribute now lives in the generic attributes MAP only.

**Notable Changes in v1.2:**
- Renamed `account_id` to `cloud_account_id` across all tables for consistency with OTLP attribute naming
- Added `service_name` field to spans table (previously missing, now optional for flexibility)
- Added `service_instance_id` field to metrics table for service instance identification
- Updated sorting strategies:
  - Logs/Events: `cloud_account_id` → `service_name` → `timestamp DESC`
  - Spans: `cloud_account_id` → `service_name` → `trace_id` → `timestamp DESC`
  - Metrics: `cloud_account_id` → `metric_name` → `service_name` → `service_instance_id` → `timestamp DESC`

**Notable Changes in v1.1:**
- Updated field optionality: `cloud_account_id`, `service_name`, and `body` are now optional where applicable
- Updated sorting strategies: logs/events use descending timestamp, spans sorted by trace_id
- Adjusted Parquet row group size to 64 MB and page size to 1 MB for better performance

---

## Migration Guide

### Trace and Span ID storage change (GH-135)

Prior to GH-135, `trace_id`, `span_id`, and `parent_span_id` were stored as
UTF-8 hex strings (`VARCHAR`). They are now stored as fixed-length binary
(`FIXED_LEN_BYTE_ARRAY(16)` for trace IDs, `FIXED_LEN_BYTE_ARRAY(8)` for
span and parent span IDs). The change improves Parquet stats pruning and
lets `DELTA_BYTE_ARRAY` encoding ship adjacent IDs in ~3-4 bytes/row vs.
the dictionary+RLE on hex strings.

**This is a breaking schema change.** `icegate-maintain migrate run`
detects the type difference and refuses to evolve in place — Iceberg's
schema evolution does not permit `STRING → FIXED(N)` promotion, so
operators must perform a destructive migration.

#### Affected tables

- `logs` — `trace_id`, `span_id`
- `spans` — `trace_id`, `span_id`, `parent_span_id`
- `events` — `trace_id`, `span_id`
- `metrics` — exemplar `trace_id`, `span_id` (nested)

#### Procedure

1. **Back up** the affected tables. With Nessie this is a `CREATE BRANCH`
   off the live ref before you drop anything; with a hosted REST catalog,
   snapshot the underlying object-store prefix.
2. **Stop ingest writers** so no new files land in the old schema during
   the cutover.
3. **Drop and recreate** each affected table from the canonical Rust
   schema:
   ```bash
   icegate-maintain migrate create
   ```
   This recreates every table with the new `Fixed(N)` columns.
4. **Resume ingest** — new writes carry the new schema; queries against
   pre-migration data require restoring from the backup branch (queries
   that span both old and new schemas are not supported).

#### Reader compatibility

Any external reader (Trino, Athena, Spark) reading the IceGate tables
directly must be updated to expect `VARBINARY` columns. Queries that
hex-encode the values for display can use `to_hex(trace_id)` (Trino),
`hex(trace_id)` (Spark/Athena), or the engine's equivalent.

#### Root-span sentinel

`parent_span_id` may arrive as either SQL null or 8 zero bytes for root
spans — both are treated as "no parent" by the IceGate Tempo formatters
and TraceQL planner. External readers must apply the same convention
(check `parent_span_id IS NULL OR parent_span_id = X'00000000_00000000'`)
when distinguishing root from child spans.
