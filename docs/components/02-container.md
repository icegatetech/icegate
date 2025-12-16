# C4 Container Diagram

The Container diagram shows the high-level containers that make up IceGate.

## Diagram

![](embed:Containers)

## Containers

### Ingest Service

| Property | Value |
|----------|-------|
| **Crate** | `icegate-ingest` |
| **Type** | Library + Binary |
| **Technology** | Rust, Axum, Tonic |
| **Ports** | 4318 (HTTP), 4317 (gRPC) |

Responsibilities:
- Receive OTLP data over HTTP and gRPC
- Transform OTLP to Arrow RecordBatch
- Write to queue with synchronous acknowledgment
- Background compaction from queue to Iceberg

### Query Service

| Property | Value |
|----------|-------|
| **Crate** | `icegate-query` |
| **Type** | Library + Binary |
| **Technology** | Rust, Axum, DataFusion |
| **Ports** | 3100 (Loki), 9090 (Prometheus), 3200 (Tempo) |

Responsibilities:
- Loki-compatible LogQL queries
- Prometheus-compatible PromQL queries
- Tempo-compatible trace queries
- Query both Iceberg tables and queue segments

### Maintain Service

| Property | Value |
|----------|-------|
| **Crate** | `icegate-maintain` |
| **Type** | Library + Binary (CLI) |
| **Technology** | Rust |

Responsibilities:
- Schema migrations
- Table optimization
- Data maintenance tasks

### Queue Library

| Property | Value |
|----------|-------|
| **Crate** | `icegate-queue` |
| **Type** | Library |
| **Technology** | Rust, Parquet, object_store |

Responsibilities:
- Generic WAL-based queue
- Parquet segment writing with If-None-Match
- Segment reading with offset range
- Recovery from crashes

### Common Library

| Property | Value |
|----------|-------|
| **Crate** | `icegate-common` |
| **Type** | Library |
| **Technology** | Rust |

Responsibilities:
- Iceberg catalog abstraction
- Table schema definitions
- Storage backend configuration
- Shared error types

### Queue Storage

| Property | Value |
|----------|-------|
| **Technology** | MinIO (dev), S3 (prod) |
| **Protocol** | S3 API |
| **Bucket** | `queue/` prefix |

Stores:
- Queue segments (Parquet WAL files)
- Organized by `{topic}/{offset}.parquet`

### Iceberg Storage

| Property | Value |
|----------|-------|
| **Technology** | MinIO (dev), S3 (prod) |
| **Protocol** | S3 API |
| **Bucket** | `warehouse/` prefix |

Stores:
- Iceberg data files (Parquet)
- Iceberg manifests
- Iceberg metadata

### Catalog Store

| Property | Value |
|----------|-------|
| **Technology** | Nessie |
| **Protocol** | Iceberg REST Catalog |

Stores:
- Table metadata
- Snapshot history
- Schema versions
