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
- [Post-Creation Tasks](#post-creation-tasks)
- [Type Mappings](#type-mappings)

---

## Overview

These tables implement the OpenTelemetry Protocol (OTLP) data model for observability data:

- **Logs:** Application log records with severity, body, and attributes
- **Spans:** Distributed trace spans with events and links
- **Events:** Semantic events extracted from logs based on event_name field
- **Metrics:** All metric types (gauge, sum, histogram, exponential histogram, summary)

All tables include:
- Multi-tenancy support (tenant_id, cloud_account_id)
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
- **Logs/Events:** `cloud_account_id` + `service_name` + `timestamp DESC` - Groups by account and service, recent-first
- **Spans:** `cloud_account_id` + `service_name` + `trace_id` + `timestamp DESC` - Groups by account, service, and trace, recent-first
- **Metrics:** `cloud_account_id` + `metric_name` + `service_name` + `service_instance_id` + `timestamp DESC` - Groups by account, metric, service, and service instance, recent-first

---

## Schema Definitions

### 1. Logs Table

Based on OpenTelemetry LogRecord message from `opentelemetry/proto/logs/v1/logs.proto`.

```sql
-- Create the logs table
CREATE TABLE iceberg.triplecloud.logs (
    -- Multi-tenancy fields
    tenant_id VARCHAR NOT NULL,
    cloud_account_id VARCHAR,        -- Optional for flexibility
    service_name VARCHAR,            -- Optional for flexibility

    -- Timestamp fields (microsecond precision)
    timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    observed_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    ingested_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    -- W3C trace context (for correlation with spans)
    trace_id VARCHAR,    -- W3C trace ID as hex string (32 chars)
    span_id VARCHAR,     -- W3C span ID as hex string (16 chars)

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
    sorted_by = ARRAY['cloud_account_id', 'service_name', 'timestamp DESC'],  -- Recent-first within account and service
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
    -- Multi-tenancy fields
    tenant_id VARCHAR NOT NULL,
    cloud_account_id VARCHAR,        -- Optional for flexibility
    service_name VARCHAR,            -- Optional for flexibility

    -- Trace identifiers (W3C trace context) - sorted by these for trace analysis
    trace_id VARCHAR NOT NULL,        -- W3C trace ID as hex string (32 chars)
    span_id VARCHAR NOT NULL,         -- W3C span ID as hex string (16 chars)
    parent_span_id VARCHAR,           -- W3C span ID as hex string (16 chars)

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
        trace_id VARCHAR,             -- W3C trace ID as hex string (32 chars)
        span_id VARCHAR,              -- W3C span ID as hex string (16 chars)
        trace_state VARCHAR,
        attributes MAP(VARCHAR, VARCHAR),
        dropped_attributes_count INTEGER,
        flags INTEGER
    ))
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'day(timestamp)'],
    sorted_by = ARRAY['cloud_account_id', 'service_name', 'trace_id', 'timestamp DESC'],  -- Recent-first within account, service, and trace
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
    -- Multi-tenancy fields
    tenant_id VARCHAR NOT NULL,
    cloud_account_id VARCHAR,        -- Optional for flexibility
    service_name VARCHAR,            -- Optional for flexibility

    -- Timestamp fields
    timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    observed_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    ingested_timestamp TIMESTAMP(6) WITH TIME ZONE NOT NULL,

    -- Event identification
    event_domain VARCHAR NOT NULL,
    event_name VARCHAR NOT NULL,

    -- Trace context (for correlation)
    trace_id VARCHAR,                 -- W3C trace ID as hex string (32 chars)
    span_id VARCHAR,                  -- W3C span ID as hex string (16 chars)

    -- Event attributes
    attributes MAP(VARCHAR, VARCHAR) NOT NULL
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'day(timestamp)'],
    sorted_by = ARRAY['cloud_account_id', 'service_name', 'timestamp DESC'],  -- Recent-first within account and service
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

```sql
-- Create the metrics table
CREATE TABLE iceberg.triplecloud.metrics (
    -- Multi-tenancy fields
    tenant_id VARCHAR NOT NULL,
    cloud_account_id VARCHAR,        -- Optional for flexibility
    service_name VARCHAR NOT NULL,
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
        span_id VARCHAR,                  -- W3C span ID as hex string (16 chars)
        trace_id VARCHAR,                 -- W3C trace ID as hex string (32 chars)
        attributes MAP(VARCHAR, VARCHAR)
    ))
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['tenant_id', 'day(timestamp)'],
    sorted_by = ARRAY['cloud_account_id', 'metric_name', 'service_name', 'service_instance_id', 'timestamp DESC'],  -- Recent-first within account, metric, service, and service instance
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

## Post-Creation Tasks

After creating the tables, run these commands to optimize query performance:

```sql
-- Collect statistics for better query planning
ANALYZE iceberg.triplecloud.logs;
ANALYZE iceberg.triplecloud.spans;
ANALYZE iceberg.triplecloud.events;
ANALYZE iceberg.triplecloud.metrics;
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
  AND cloud_account_id = 'account-456'
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
| `String` (trace_id) | `VARCHAR` | W3C trace ID as hex string (32 chars) |
| `String` (span_id) | `VARCHAR` | W3C span ID as hex string (16 chars) |
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
- Sorting by `cloud_account_id` first provides good data locality within tenant partitions
- Additional sort columns (`metric_name`, `service_name`, `trace_id`) enable efficient predicate pushdown on sorted data
- Descending timestamp ordering (recent-first) optimizes for common query patterns accessing recent data

### Compression
- ZSTD provides excellent compression with good read/write performance
- Alternative: Use SNAPPY for faster writes if compression ratio is less critical

### Query Optimization
- **Partition columns:** `tenant_id` and `day(timestamp)` - filtering on these enables partition pruning
- **Sort columns:** `cloud_account_id`, `metric_name`, `service_name`, `trace_id` - filtering on these benefits from optimized scanning of sorted data (predicate pushdown)
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

**Version:** 1.2
**Last Updated:** 2025-01-13
**Schema Source:** `src/common/schema.rs`

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
