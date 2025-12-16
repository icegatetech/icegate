# C4 System Context

The System Context diagram shows IceGate and its relationships with external systems.

## Diagram

![](embed:SystemContext)

## External Systems

### OpenTelemetry Collector

- **Purpose**: Sends telemetry data to IceGate
- **Protocol**: OTLP over HTTP (port 4318) or gRPC (port 4317)
- **Data Types**: Logs, Traces, Metrics

### Grafana

- **Purpose**: Visualization and dashboards
- **Protocol**: HTTP
- **APIs Used**:
  - Loki API (port 3100) for logs
  - Prometheus API (port 9090) for metrics
  - Tempo API (port 3200) for traces

### Trino

- **Purpose**: Distributed SQL queries on Iceberg tables
- **Protocol**: Iceberg REST catalog + S3
- **Usage**: Ad-hoc analytics, data exploration

## IceGate System

IceGate is an observability data lake engine that:

1. **Ingests** telemetry data via OTLP protocol
2. **Stores** data in Apache Iceberg tables on S3
3. **Queries** data via Loki/Prometheus/Tempo-compatible APIs
4. **Maintains** data through compaction and optimization
