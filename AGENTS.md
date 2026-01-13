# AGENTS.md

This file provides guidance to AI agents (Claude Code, Gemini Code Assist, etc.) when working with code in this repository.

## Build Commands

### Cargo Commands

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo build --bin query        # Build only query binary
cargo test                     # Run all tests
cargo test test_name           # Run specific test
cargo test -- --nocapture      # Run tests with output shown
```

### Make Commands

```bash
make test           # Run all tests
make check          # Check all targets
make fmt            # Check code format (DO NOT RUN via rustup because it doesn't respect rustfmt.toml)
make clippy         # Run linter with warnings as errors
make audit          # Run security audit
make install        # Install cargo-audit
make ci             # Run all CI checks (check, fmt, clippy, test, audit)
```

## Project Overview

IceGate is an observability data lake engine that stores logs, traces, metrics, and events in Apache Iceberg tables with DataFusion as the query engine. Data is ingested via OpenTelemetry protocol and queried via Loki/Prometheus/Tempo-compatible APIs.

The architecture is based on open standards:
- **Apache Iceberg:** For the data lake's table format, providing ACID transactions
- **Apache DataFusion and Apache Arrow:** As the query engine
- **Apache Parquet:** For efficient data storage
- **OpenTelemetry:** For data ingestion

## Architecture

This project is organized as a Cargo workspace with 4 crates:

```
crates/
├── icegate-common/           # Shared infrastructure (lib)
│   └── src/
│       ├── lib.rs
│       ├── catalog/          # Iceberg catalog management (CatalogBuilder)
│       ├── storage/          # Storage backends (S3Config, StorageConfig)
│       ├── schema.rs         # Iceberg table schemas
│       └── errors.rs         # Error types
│
├── icegate-query/            # Query APIs + CLI (lib + bin)
│   └── src/
│       ├── lib.rs
│       ├── main.rs           # Query binary entry point
│       ├── cli/              # Query CLI commands
│       ├── engine/           # Query engine
│       ├── loki/             # Loki API (handlers, routes, server)
│       ├── logql/            # LogQL implementation (parser, planner, AST)
│       ├── prometheus/       # Prometheus API
│       └── tempo/            # Tempo API
│
├── icegate-ingest/           # OTLP receivers (lib)
│   └── src/
│       ├── lib.rs
│       ├── config.rs
│       ├── otlp_grpc/        # gRPC receiver
│       └── otlp_http/        # HTTP receiver (handlers, routes, server)
│
└── icegate-maintain/         # Maintenance ops + CLI (lib + bin)
    └── src/
        ├── lib.rs
        ├── main.rs           # Maintain binary entry point
        ├── cli/              # Maintain CLI commands
        ├── config.rs
        └── migrate/          # Schema migration operations
```

**Dependency graph:**
```
icegate-common (no deps on other workspace crates)
    ↑
    ├── icegate-query     (depends: icegate-common)
    ├── icegate-ingest    (depends: icegate-common)
    └── icegate-maintain  (depends: icegate-common)
```

## Key Data Types

Four Iceberg tables defined in `crates/icegate-common/src/schema.rs` (DDL in `crates/icegate-common/src/SCHEMA.md`):
- **logs** - OpenTelemetry LogRecords
- **spans** - Distributed trace spans with nested events/links
- **events** - Semantic events (extracted from logs)
- **metrics** - All metric types (gauge, sum, histogram, summary)

All tables use: tenant_id partitioning, ZSTD compression, MAP(VARCHAR,VARCHAR) for attributes.

## LogQL Implementation

The LogQL parser uses ANTLR4. Grammar files are in `crates/icegate-query/src/logql/antlr/`.

Regenerate parser (requires Java):
```bash
cd crates/icegate-query/src/logql && make install  # Download ANTLR jar (first time)
cd crates/icegate-query/src/logql && make gen      # Regenerate parser from .g4 files
```

Parser status: Complete. Planner status: Partial (see `crates/icegate-query/src/logql/README.md` for feature matrix).

## Development Environment

### Quick Start

```bash
make dev     # Run full stack with hot-reload (watches crates/ for changes)
make debug   # Run without query service for debugging
```

Both commands use Docker Compose to start the full development environment.

### Available Services

The development environment includes:

**Core Infrastructure:**
- **MinIO (S3):** Object storage backend
  - API: `http://localhost:9000`
  - Console: `http://localhost:9001`
  - Credentials: minioadmin/minioadmin

- **Nessie:** Iceberg catalog with REST API
  - API: `http://localhost:19120`
  - Default warehouse: `s3://warehouse/`

**Query & Analytics:**
- **Query Service:** Loki/Prometheus/Tempo-compatible APIs
  - Loki API: `http://localhost:3100`
  - Prometheus API: `http://localhost:9090`
  - Tempo API: `http://localhost:3200`

- **Trino:** Distributed SQL query engine
  - Web UI: `http://localhost:8080`

- **Grafana:** Observability dashboard
  - Web UI: `http://localhost:3000`
  - Anonymous access enabled (no login required)

### Environment Variables

For local development:
```bash
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
```

## Linting Configuration

Strict clippy/rustc lints configured in `Cargo.toml`:
- `unsafe_code = "forbid"`
- `missing_docs = "deny"`
- `dead_code = "deny"`
- clippy pedantic/nursery enabled

Thresholds in `clippy.toml`: cognitive-complexity=30, too-many-arguments=8, too-many-lines=150.

Use rules in `rustfmt.toml` to generate well-formatted code.

## Development Conventions

### Code Style

The project uses `rustfmt` for code formatting. Configuration is in `rustfmt.toml`.
@RUST.md

### Important Instructions

- Do what has been asked; nothing more, nothing less
- NEVER create files unless they're absolutely necessary for achieving your goal
- ALWAYS prefer editing an existing file to creating a new one
- NEVER proactively create documentation files (*.md) or README files unless explicitly requested
- Ensure each file is finishing by new line, do not duplicate if it already exists
- It is better to give an error than to use/calculate/show invalid data.
- NEVER delete TODO comments if the changes do not fully cover the necessary edits in the comment.