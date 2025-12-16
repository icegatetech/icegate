# Use Parquet for Queue Segment Format

- Status: accepted
- Deciders: IceGate Core Team
- Date: 2025-12-16

## Context and Problem Statement

The queue-based ingestion system (ADR-0003) needs a file format for WAL segments stored on S3. The format must support:

- Efficient columnar storage for analytical queries
- Compression for cost-effective storage
- Schema preservation for Arrow RecordBatches
- Direct querying without conversion
- Row group organization for tenant isolation

What file format should we use for queue segments?

## Decision Drivers

- Compatibility with DataFusion query engine
- Compression efficiency
- Schema preservation
- Row group support for tenant isolation
- Ecosystem compatibility (Arrow, Iceberg)

## Considered Options

1. Apache Parquet
2. Apache ORC
3. Apache Avro
4. Custom binary format

## Decision Outcome

Chosen option: "Apache Parquet", because it provides excellent compression, native Arrow integration, row group support for tenant isolation, and is the same format used by Iceberg data files.

### Consequences

- Good, because same format as Iceberg data files (no conversion needed for compaction)
- Good, because native Arrow integration via parquet crate
- Good, because row groups enable tenant isolation with statistics-based pruning
- Good, because ZSTD compression provides excellent ratio
- Good, because DataFusion can query Parquet files directly
- Bad, because row group overhead for small batches
- Bad, because write latency higher than append-only formats

## Pros and Cons of the Options

### Apache Parquet

- Good, because columnar format ideal for analytics
- Good, because native Arrow integration
- Good, because row groups for tenant partitioning
- Good, because same format as Iceberg data files
- Good, because excellent compression (ZSTD)
- Good, because statistics enable predicate pushdown
- Bad, because not optimized for small writes

### Apache ORC

- Good, because columnar format
- Good, because good compression
- Bad, because less native Arrow integration
- Bad, because different format from Iceberg (requires conversion)
- Bad, because smaller Rust ecosystem

### Apache Avro

- Good, because schema evolution
- Good, because compact binary format
- Bad, because row-based (poor for analytical queries)
- Bad, because no row group concept
- Bad, because requires conversion for Iceberg

### Custom Binary Format

- Good, because optimized for use case
- Bad, because development effort
- Bad, because no ecosystem tooling
- Bad, because requires conversion for queries

## Technical Details

### Segment Path Convention

```
{base_path}/{topic}/{offset}.parquet
```

- `offset`: Zero-padded u64 (20 digits) for lexicographic ordering
- Example: `s3://warehouse/queue/logs/00000000000000000001.parquet`

### Atomic Writes with If-None-Match

S3 PUTs use `If-None-Match: *` header to ensure exactly-once semantics:

```rust
let opts = PutOptions {
    mode: PutMode::Create,  // If-None-Match: *
    ..Default::default()
};
object_store.put_opts(&path, payload, opts).await?;
```

### Tenant Isolation via Row Groups

Each segment contains multiple row groups, one per tenant:

```
/00000000000000000001.parquet
├── RowGroup 0: tenant_id = "acme"
├── RowGroup 1: tenant_id = "globex"
└── RowGroup 2: tenant_id = "initech"
```

Benefits:
- Single file per offset (simpler path structure)
- Row group pruning on read (skip irrelevant tenants)
- Column statistics enable predicate pushdown

## Links

- [ADR-0003](0003-queue-based-ingestion.md) - Queue-based ingestion
- [Apache Parquet](https://parquet.apache.org/)
- [parquet-rs](https://docs.rs/parquet/)
