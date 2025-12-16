# Use Queue-Based Ingestion with Parquet WAL

- Status: accepted
- Deciders: IceGate Core Team
- Date: 2025-12-16

## Context and Problem Statement

IceGate needs to ingest OpenTelemetry data (logs, traces, metrics, events) via OTLP protocol and persist it to Iceberg tables. Direct writes to Iceberg on every OTLP request would cause:

1. Excessive small file creation (each commit creates new data files)
2. High metadata overhead (each commit updates manifest files)
3. Poor write performance under high throughput
4. Risk of data loss during service restarts

How should we buffer incoming OTLP data before committing to Iceberg tables while maintaining exactly-once semantics and supporting real-time queries?

## Decision Drivers

- Exactly-once delivery semantics (no data loss or duplication)
- High write throughput (thousands of records per second)
- Low latency for real-time queries on recent data
- Efficient storage utilization (avoid small file problem)
- Graceful shutdown with flush guarantees
- Recovery from crashes without data loss
- Stateless ingest nodes for horizontal scaling

## Considered Options

1. Direct Iceberg Commits
2. In-Memory Buffer Only
3. WAL on Local Disk
4. WAL on S3 with Parquet

## Decision Outcome

Chosen option: "WAL on S3 with Parquet", because it provides durability without local storage requirements, supports exactly-once semantics through idempotent file writes, and the Parquet format enables real-time queries on WAL data.

### Consequences

- Good, because WAL files are durable on S3 immediately after write acknowledgment
- Good, because Parquet format allows query layer to read WAL files directly for real-time data
- Good, because stateless ingest nodes can scale horizontally
- Good, because sequential offset-based naming with If-None-Match ensures exactly-once
- Bad, because S3 latency adds overhead per WAL file write (~50-100ms)
- Bad, because requires background compaction process to convert WAL to Iceberg commits
- Bad, because increases storage costs during WAL retention period

## Pros and Cons of the Options

### Direct Iceberg Commits

- Good, because simplest implementation
- Good, because data immediately available in Iceberg
- Bad, because creates many small files (1 per batch)
- Bad, because high metadata overhead
- Bad, because poor performance under high throughput

### In-Memory Buffer Only

- Good, because lowest latency for writes
- Good, because simple implementation
- Bad, because data loss on crash/restart
- Bad, because memory pressure under high throughput
- Bad, because no durability guarantees

### WAL on Local Disk

- Good, because fast local disk writes
- Good, because standard WAL approach
- Bad, because requires local persistent storage
- Bad, because complicates horizontal scaling
- Bad, because data not accessible for real-time queries

### WAL on S3 with Parquet (Chosen)

- Good, because durability on S3
- Good, because stateless ingest nodes
- Good, because Parquet files queryable by DataFusion
- Good, because schema-compatible with Iceberg tables
- Good, because supports exactly-once via If-None-Match
- Bad, because S3 latency overhead
- Bad, because requires compaction background process
- Bad, because temporary storage duplication

## Links

- [ADR-0001](0001-use-iceberg-for-storage.md) - Iceberg for storage
- [ADR-0004](0004-parquet-queue-format.md) - Parquet queue format
