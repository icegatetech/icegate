# C4 Component Diagrams

Component diagrams show the internal structure of each container.

## Ingest Service Components

![](embed:IngestComponents)

### Components

| Component | Responsibility |
|-----------|---------------|
| OTLP HTTP Handler | Receives OTLP over HTTP/protobuf on port 4318 |
| OTLP gRPC Handler | Receives OTLP over gRPC on port 4317 |
| Record Transformer | Converts OTLP data to Arrow RecordBatch |
| Queue Compactor | Background task that compacts queue segments to Iceberg |

---

## Query Service Components

![](embed:QueryComponents)

### Components

| Component | Responsibility |
|-----------|---------------|
| Loki API Handler | Grafana Loki-compatible query endpoint |
| Prometheus API Handler | Grafana Prometheus-compatible query endpoint |
| Tempo API Handler | Grafana Tempo-compatible trace endpoint |
| LogQL Parser | Parses LogQL query strings into AST (ANTLR4) |
| LogQL Planner | Converts LogQL AST to DataFusion logical plans |
| Query Engine | DataFusion-based query execution |

---

## Queue Library Components

![](embed:QueueComponents)

### Components

| Component | Responsibility |
|-----------|---------------|
| QueueWriter | Writes Parquet segments with atomic If-None-Match |
| QueueReader | Reads segments by topic and offset range |
| QueueRecovery | Recovers offset state on startup |

---

## Maintain Service Components

![](embed:MaintainComponents)

### Components

| Component | Responsibility |
|-----------|---------------|
| Schema Migrator | Creates and upgrades Iceberg tables |