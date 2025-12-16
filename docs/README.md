# IceGate Documentation

IceGate is an observability data lake engine that stores logs, traces, metrics, and events in Apache Iceberg tables.

## Contents

### Architecture (C4 Model)

- [Overview](components/00-index.md) - Architecture overview and technology stack
- [System Context](components/01-system-context.md) - External system relationships
- [Containers](components/02-container.md) - Services, libraries, and storage
- [Components](components/03-component.md) - Internal component details

The architecture is defined using [Structurizr DSL](workspace.dsl).

#### Viewing Diagrams

```bash
cd docs

# Recommended: Interactive UI with live reload
make lite      # Opens http://localhost:8080 (requires Docker)

# Static export alternatives
make html      # Generate static HTML site
make serve     # Generate HTML and serve at http://localhost:8000
make mermaid   # Export as Mermaid diagrams
make plantuml  # Export as PlantUML diagrams

# Utilities
make validate  # Validate workspace DSL
make help      # Show all available commands
```

Prerequisites: `brew install structurizr-cli` (for static export) or Docker (for Lite).

### Architecture Decision Records

- [ADR Index](decisions/README.md) - All architecture decisions
- [ADR Template](decisions/template.md) - Template for new ADRs

**Accepted ADRs:**
- [ADR-0001](decisions/0001-use-iceberg-for-storage.md) - Use Apache Iceberg for Storage
- [ADR-0002](decisions/0002-use-datafusion-query-engine.md) - Use Apache DataFusion as Query Engine
- [ADR-0003](decisions/0003-queue-based-ingestion.md) - Use Queue-Based Ingestion with Parquet WAL
- [ADR-0004](decisions/0004-parquet-queue-format.md) - Use Parquet for Queue Segment Format

### Crates

| Crate | Description |
|-------|-------------|
| `icegate-common` | Shared library (catalog, storage, schema) |
| `icegate-queue` | WAL-based data queue |
| `icegate-ingest` | OTLP ingestion service (HTTP/gRPC) |
| `icegate-query` | Loki/Prometheus/Tempo query APIs |
| `icegate-maintain` | Maintenance operations (migrations) |

## Quick Links

- [GitHub Repository](https://github.com/icegatetech/icegate)
- [AGENTS.md](../AGENTS.md) - Build commands and development guide
