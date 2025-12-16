# Use Apache Iceberg for Storage

- Status: accepted
- Deciders: IceGate Core Team
- Date: 2025-06-10

## Context and Problem Statement

IceGate needs a storage format for observability data (logs, traces, metrics, events) that supports:
- ACID transactions for reliable writes
- Schema evolution as requirements change
- Time travel for debugging and auditing
- Efficient queries across large datasets
- Partition pruning for multi-tenant isolation

What table format should we use for storing observability data?

## Decision Drivers

- ACID transaction support for concurrent writes
- Schema evolution without rewriting data
- Time travel capabilities for debugging
- Partition pruning for query performance
- Open format with broad ecosystem support
- Compatibility with multiple query engines

## Considered Options

1. Apache Iceberg
2. Apache Hudi
3. Delta Lake
4. Raw Parquet files

## Decision Outcome

Chosen option: "Apache Iceberg", because it provides the best combination of ACID transactions, schema evolution, and ecosystem support for our observability use case.

### Consequences

- Good, because ACID transactions ensure data consistency during concurrent ingestion
- Good, because schema evolution allows adding new fields without data migration
- Good, because time travel enables debugging historical states
- Good, because partition pruning on tenant_id enables efficient multi-tenant queries
- Good, because open specification allows switching query engines (DataFusion, Trino, Spark)
- Bad, because adds complexity compared to raw Parquet files
- Bad, because requires catalog management (Nessie, REST catalog)

## Pros and Cons of the Options

### Apache Iceberg

- Good, because true ACID transactions with snapshot isolation
- Good, because hidden partitioning simplifies query patterns
- Good, because schema evolution is first-class
- Good, because supported by DataFusion, Trino, Spark, Flink
- Good, because open specification with multiple implementations
- Bad, because requires catalog service for production use

### Apache Hudi

- Good, because ACID transactions
- Good, because record-level updates (useful for late-arriving data)
- Bad, because more complex architecture (copy-on-write vs merge-on-read)
- Bad, because less mature DataFusion integration

### Delta Lake

- Good, because ACID transactions
- Good, because simple file-based catalog option
- Bad, because tighter coupling to Spark ecosystem
- Bad, because open source version has fewer features than commercial

### Raw Parquet Files

- Good, because simplest option
- Good, because no catalog overhead
- Bad, because no ACID transactions
- Bad, because manual schema evolution
- Bad, because no time travel
- Bad, because manual partition management

## Links

- [Apache Iceberg](https://iceberg.apache.org/)
- [iceberg-rust](https://github.com/apache/iceberg-rust)
