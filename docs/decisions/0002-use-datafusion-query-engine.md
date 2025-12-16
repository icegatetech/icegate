# Use Apache DataFusion as Query Engine

- Status: accepted
- Deciders: IceGate Core Team
- Date: 2024-12-16

## Context and Problem Statement

IceGate needs a query engine to execute queries against Iceberg tables. The query engine must:
- Support SQL and custom query languages (LogQL, PromQL)
- Integrate with Apache Iceberg tables
- Run embedded in a Rust application
- Handle large datasets efficiently
- Support predicate pushdown and partition pruning

What query engine should we use for IceGate?

## Decision Drivers

- Native Rust implementation for embedding
- Apache Iceberg integration
- Extensibility for custom query languages
- Performance for analytical queries
- Active open source community

## Considered Options

1. Apache DataFusion
2. DuckDB
3. Polars
4. Custom query engine

## Decision Outcome

Chosen option: "Apache DataFusion", because it provides native Rust implementation, excellent Iceberg integration, and extensibility for custom query language transpilation.

### Consequences

- Good, because native Rust integration with no FFI overhead
- Good, because extensible logical/physical planning for LogQL/PromQL transpilation
- Good, because iceberg-datafusion crate provides seamless Iceberg integration
- Good, because Arrow-native for efficient columnar processing
- Good, because active Apache project with strong community
- Bad, because less mature than established engines like Spark SQL
- Bad, because custom UDF implementation required for some LogQL functions

## Pros and Cons of the Options

### Apache DataFusion

- Good, because pure Rust implementation
- Good, because designed for embedding
- Good, because extensible query planning
- Good, because Arrow-native memory format
- Good, because iceberg-datafusion integration
- Good, because Apache project governance
- Bad, because smaller ecosystem than Spark

### DuckDB

- Good, because excellent single-node performance
- Good, because embedded database design
- Good, because SQL standard compliance
- Bad, because C++ with Rust bindings (FFI overhead)
- Bad, because less extensible for custom query languages
- Bad, because Iceberg integration less mature

### Polars

- Good, because pure Rust
- Good, because excellent DataFrame performance
- Bad, because not designed as query engine
- Bad, because no Iceberg integration
- Bad, because no SQL support (DataFrame API only)

### Custom Query Engine

- Good, because full control over implementation
- Bad, because massive development effort
- Bad, because reinventing solved problems
- Bad, because maintenance burden

## Links

- [Apache DataFusion](https://datafusion.apache.org/)
- [iceberg-datafusion](https://github.com/apache/iceberg-rust)
- [ADR-0001](0001-use-iceberg-for-storage.md) - Iceberg storage decision
