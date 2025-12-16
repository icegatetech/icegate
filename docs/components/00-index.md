# IceGate Architecture

IceGate is an observability data lake engine that stores logs, traces, metrics, and events in Apache Iceberg tables.

## Key Features

- **OTLP Ingestion** - Native OpenTelemetry Protocol support over HTTP and gRPC
- **Grafana Compatible** - Query via Loki, Prometheus, and Tempo APIs
- **Apache Iceberg** - ACID transactions, schema evolution, time travel
- **DataFusion Engine** - High-performance query execution with Apache Arrow
- **SQL Access** - Query with Trino for ad-hoc analytics

## Architecture Overview

| Level | View | Description |
|-------|------|-------------|
| L1 | [System Context](embed:SystemContext) | IceGate and external systems |
| L2 | [Containers](embed:Containers) | Services, libraries, and storage |
| L3 | [Ingest Components](embed:IngestComponents) | OTLP handlers and queue compactor |
| L3 | [Query Components](embed:QueryComponents) | API handlers and query engine |
| L3 | [Queue Components](embed:QueueComponents) | WAL-based queue library |
| L3 | [Maintain Components](embed:MaintainComponents) | Schema migrations |

## Technology Stack

| Layer | Technology |
|-------|------------|
| Language | Rust |
| HTTP Framework | Axum |
| gRPC Framework | Tonic |
| Query Engine | Apache DataFusion |
| Table Format | Apache Iceberg |
| File Format | Apache Parquet |
| Object Storage | S3 / MinIO |
| Catalog | Nessie (Iceberg REST) |

## Documentation

- **[System Context](01-system-context.md)** - External system relationships
- **[Containers](02-container.md)** - Service and library breakdown
- **[Components](03-component.md)** - Internal component details
