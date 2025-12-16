workspace "IceGate" "Observability Data Lake Engine" {

    !identifiers hierarchical

    configuration {
        scope softwaresystem
    }

    model {
        # External actors
        otelCollector = softwareSystem "OpenTelemetry Collector" "Sends telemetry data via OTLP protocol" "External"
        grafana = softwareSystem "Grafana" "Visualization and dashboards" "External"
        trino = softwareSystem "Trino" "Distributed SQL query engine" "External"

        # IceGate system
        icegate = softwareSystem "IceGate" "Observability data lake engine storing logs, traces, metrics, and events in Apache Iceberg" {
            # Architecture Decision Records (MADR format)
            !adrs decisions madr

            # Component documentation
            !docs components

            # Containers
            ingestService = container "Ingest Service" "Receives OTLP data, buffers in queue, and compacts to Iceberg" "Rust / Axum / Tonic" {
                otlpHttpHandler = component "OTLP HTTP Handler" "Receives OTLP data over HTTP/protobuf" "Axum"
                otlpGrpcHandler = component "OTLP gRPC Handler" "Receives OTLP data over gRPC" "Tonic"
                recordTransformer = component "Record Transformer" "Converts OTLP to Arrow RecordBatch" "Rust"
                compactor = component "Queue Compactor" "Background task that compacts queue segments to Iceberg" "Rust"
            }

            queryService = container "Query Service" "Provides Loki/Prometheus/Tempo-compatible query APIs" "Rust / Axum / DataFusion" {
                lokiApi = component "Loki API" "LogQL query endpoint" "Axum"
                prometheusApi = component "Prometheus API" "PromQL query endpoint" "Axum"
                tempoApi = component "Tempo API" "Trace query endpoint" "Axum"
                logqlParser = component "LogQL Parser" "Parses LogQL into AST" "ANTLR4"
                logqlPlanner = component "LogQL Planner" "Converts LogQL AST to DataFusion plans" "Rust"
                queryEngine = component "Query Engine" "DataFusion-based query execution" "DataFusion"
            }

            maintainService = container "Maintain Service" "Schema migrations and data maintenance" "Rust / CLI" {
                migrator = component "Schema Migrator" "Creates and upgrades Iceberg tables" "Rust"
            }

            queueLib = container "Queue Library" "Generic WAL-based queue with Parquet on object storage" "Rust Library" {
                queueWriter = component "Queue Writer" "Writes Parquet segments to S3" "Rust"
                queueReader = component "Queue Reader" "Reads Parquet segments from S3" "Rust"
                queueRecovery = component "Queue Recovery" "Recovers offset state on startup" "Rust"
            }

            commonLib = container "Common Library" "Shared utilities, schemas, and abstractions" "Rust Library"

            # External storage
            queueStorage = container "Queue Storage" "Queue segments (Parquet WAL files)" "MinIO / S3" "Database"
            icebergStorage = container "Iceberg Storage" "Iceberg data files and manifests" "MinIO / S3" "Database"
            catalogStore = container "Catalog Store" "Iceberg catalog metadata" "Nessie / REST" "Database"
        }

        # Relationships - External to IceGate
        otelCollector -> icegate.ingestService "Sends OTLP logs/traces/metrics" "HTTP/gRPC"
        grafana -> icegate.queryService "Queries via Loki/Prometheus/Tempo APIs" "HTTP"
        trino -> icegate.catalogStore "Reads table metadata" "REST"
        trino -> icegate.icebergStorage "Reads Parquet files" "S3"

        # Relationships - Internal containers
        icegate.ingestService -> icegate.queueLib "Writes and reads queue" "Rust API"
        icegate.ingestService -> icegate.queueStorage "Reads/writes queue segments" "S3"
        icegate.ingestService -> icegate.icebergStorage "Writes compacted data" "S3"
        icegate.ingestService -> icegate.catalogStore "Commits snapshots" "REST"
        icegate.ingestService -> icegate.commonLib "Uses schemas" "Rust API"

        icegate.queryService -> icegate.queueStorage "Reads queue segments" "S3"
        icegate.queryService -> icegate.icebergStorage "Reads Iceberg data" "S3"
        icegate.queryService -> icegate.catalogStore "Reads table metadata" "REST"
        icegate.queryService -> icegate.queueLib "Reads queue" "Rust API"
        icegate.queryService -> icegate.commonLib "Uses schemas" "Rust API"

        icegate.maintainService -> icegate.icebergStorage "Manages table storage" "S3"
        icegate.maintainService -> icegate.catalogStore "Creates/updates tables" "REST"
        icegate.maintainService -> icegate.commonLib "Uses schemas" "Rust API"

        # Component relationships - Ingest Service
        icegate.ingestService.otlpHttpHandler -> icegate.ingestService.recordTransformer "Parsed OTLP" "Rust"
        icegate.ingestService.otlpGrpcHandler -> icegate.ingestService.recordTransformer "Parsed OTLP" "Rust"
        icegate.ingestService.recordTransformer -> icegate.queueLib.queueWriter "RecordBatch" "Rust API"

        # Component relationships - Queue Library
        icegate.queueLib.queueWriter -> icegate.queueStorage "Writes segments" "S3"
        icegate.queueLib.queueReader -> icegate.queueStorage "Reads segments" "S3"
        icegate.queueLib.queueRecovery -> icegate.queueStorage "Scans offsets" "S3"

        # Component relationships - Query Service
        icegate.queryService.lokiApi -> icegate.queryService.queryEngine "LogQL query" "Rust"
        icegate.queryService.prometheusApi -> icegate.queryService.queryEngine "PromQL query" "Rust"
        icegate.queryService.tempoApi -> icegate.queryService.queryEngine "Trace query" "Rust"
        icegate.queryService.queryEngine -> icegate.queryService.logqlParser "Parses LogQL" "Rust"
        icegate.queryService.queryEngine -> icegate.queryService.logqlPlanner "Converts AST to plan" "Rust"
        icegate.queryService.queryEngine -> icegate.queueStorage "Reads queue" "S3"
        icegate.queryService.queryEngine -> icegate.icebergStorage "Reads Iceberg" "S3"
        icegate.queryService.queryEngine -> icegate.catalogStore "Reads metadata" "REST"
        icegate.queryService.queryEngine -> icegate.queueLib.queueReader "Reads segments" "Rust API"

        # Component relationships - Ingest Service (Compactor)
        icegate.ingestService.compactor -> icegate.queueLib.queueReader "Reads queue segments" "Rust API"
        icegate.ingestService.compactor -> icegate.icebergStorage "Writes Iceberg data" "S3"
        icegate.ingestService.compactor -> icegate.catalogStore "Commits snapshots" "REST"

        # Component relationships - Maintain Service
        icegate.maintainService.migrator -> icegate.catalogStore "Creates tables" "REST"
        icegate.maintainService.migrator -> icegate.icebergStorage "Initializes storage" "S3"
    }

    views {
        systemContext icegate "SystemContext" "System Context diagram" {
            include *
            autoLayout
        }

        container icegate "Containers" "Container diagram" {
            include *
            autoLayout
        }

        component icegate.ingestService "IngestComponents" "Ingest Service components" {
            include *
            autoLayout
        }

        component icegate.queryService "QueryComponents" "Query Service components" {
            include *
            autoLayout
        }

        component icegate.queueLib "QueueComponents" "Queue Library components" {
            include *
            autoLayout
        }

        component icegate.maintainService "MaintainComponents" "Maintain Service components" {
            include *
            autoLayout
        }

        styles {
            element "Software System" {
                background #1168bd
                color #ffffff
            }
            element "External" {
                background #999999
                color #ffffff
            }
            element "Container" {
                background #438dd5
                color #ffffff
            }
            element "Component" {
                background #85bbf0
                color #000000
            }
            element "Database" {
                shape Cylinder
            }
        }
    }
}