# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo build --bin query        # Build only query binary
cargo test                     # Run all tests
cargo test test_name           # Run specific test
cargo test -- --nocapture      # Run tests with output shown
cargo fmt                      # Format code
cargo clippy                   # Run linter (strict config in Cargo.toml)
```

## Project Overview

IceGate is an observability data lake engine that stores logs, traces, metrics, and events in Apache Iceberg tables with DataFusion as the query engine. Data is ingested via OpenTelemetry protocol and queried via Loki/Prometheus/Tempo-compatible APIs.

## Architecture

```
src/
├── lib.rs              # Crate root, exports modules
├── bin/query.rs        # Query binary entry point
├── common/             # Shared infrastructure
│   ├── catalog/        # Iceberg catalog management (CatalogBuilder, CatalogManager)
│   ├── storage/        # Storage backends (S3Config, StorageConfig)
│   ├── schema.rs       # Iceberg table schemas
│   └── errors.rs       # Error types
├── ingest/             # OTLP data ingestion
│   ├── otlp_grpc/      # gRPC receiver
│   └── otlp_http/      # HTTP receiver (handlers, routes, server)
├── query/              # Query APIs
│   ├── logql/          # LogQL implementation (parser, planner, AST)
│   ├── loki/           # Loki API (handlers, routes, server)
│   ├── prometheus/     # Prometheus API
│   ├── tempo/          # Tempo API
│   └── cli/            # Query CLI commands
└── maintain/           # Data maintenance (migrate, optimize)
```

## Key Data Types

Four Iceberg tables defined in `src/common/schema.rs` (DDL in `src/common/SCHEMA.md`):
- **logs** - OpenTelemetry LogRecords
- **spans** - Distributed trace spans with nested events/links
- **events** - Semantic events (extracted from logs)
- **metrics** - All metric types (gauge, sum, histogram, summary)

All tables use: tenant_id partitioning, ZSTD compression, MAP(VARCHAR,VARCHAR) for attributes.

## LogQL Implementation

The LogQL parser uses ANTLR4. Grammar files are in `src/query/logql/antlr/`.

Regenerate parser (requires Java):
```bash
cd src/query/logql && make install  # Download ANTLR jar (first time)
cd src/query/logql && make gen      # Regenerate parser from .g4 files
```

Parser status: Complete. Planner status: Partial (see `src/query/logql/README.md` for feature matrix).

## Development Environment

DevContainer available with MinIO (S3), Iceberg REST catalog. Ports:
- 9000/9001: MinIO API/Console
- 8181: Iceberg REST Catalog

Environment variables for local development:
```bash
AWS_ENDPOINT_URL=http://localhost:9000
ICEBERG_REST_URI=http://localhost:8181
```

## Linting Configuration

Strict clippy/rustc lints configured in `Cargo.toml`:
- `unsafe_code = "forbid"`
- `missing_docs = "deny"`
- `dead_code = "deny"`
- clippy pedantic/nursery enabled

Thresholds in `clippy.toml`: cognitive-complexity=30, too-many-arguments=8, too-many-lines=150.
