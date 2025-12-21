# IceGate

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-prototype-red.svg)]()

An Observability Data Lake engine designed to be fast, easy-to-use, cost-effective, scalable, and fault-tolerant.

IceGate supports open protocols and APIs compatible with standard ingesting and querying tools. All data is persisted in Object Storage, including WAL for ingested data, catalog metadata, and the data layer.

## Features

- **Highly Scalable**: Scale compute resources independently based on workload demands of specific components
- **ACID Transactions**: Full transaction support without requiring a dedicated OLTP database
- **Exactly-Once Delivery**: Reliable data ingestion with no data loss or duplication
- **Real-Time Queries**: Access live data through WAL while maintaining historical query capabilities
- **Open Standards**: Built on Apache Iceberg, Arrow, and Parquet with OpenTelemetry protocol support
- **Cost-Effective**: Object storage-based architecture minimizes infrastructure costs
- **Fault-Tolerant**: Designed for resilience and high availability

## Architecture

IceGate employs a compute-storage separation architecture, allowing independent scaling of processing and storage resources.
This design enables cost-effective scaling where compute resources (Ingest, Query, Maintain, Alert) can be scaled independently based on workload demands,
while all data resides in object storage.

The system consists of five core components for handling observability data (metrics, traces, logs, and events):

### Catalog
- **Technology**: [Apache Iceberg](https://iceberg.apache.org/)
- **Purpose**: Organizes the data lake with ACID transaction support
- **Key Feature**: Custom catalog implementation that doesn't require a dedicated OLTP database while still supporting transactions

### Ingest
- **Protocol**: [OpenTelemetry](https://opentelemetry.io/)
- **Purpose**: Accept observability data and persist it in Object Storage
- **Implementation**: WAL using Parquet files organized in a special way to be compatible with the Storage data layer
- **Delivery Guarantee**: Exactly-once delivery
- **Note**: WAL files can be used by the Query layer to provide real-time data access

### Query
- **Technology**: [Apache DataFusion](https://datafusion.apache.org/) and [Apache Arrow](https://arrow.apache.org/)
- **Purpose**: Query engine for processing logs, metrics, traces, and events
- **Implementation**: Rust-native query engine built on Apache Arrow, providing a foundation to build query engines using various protocols

### Maintain
- **Data Format**: [Apache Parquet](https://parquet.apache.org/)
- **Features**:
    - Statistics and bloom filters for efficient querying
    - Additional custom statistics and filters for optimization
    - Data optimization operations: merge, TTL support, manifest optimization, and orphan resource cleanup
- **Purpose**: Maximize query efficiency and maintain data lake health

### Alert
- **Purpose**: Provides management of alerting rules, analyzing observability data, and generating alert events
- **Features**:
    - Rule management for defining alert conditions
    - Real-time analysis of observability data (logs, metrics, traces)
    - Event generation and delivery based on rule evaluation
- **Data Type**: Events are treated as a dedicated data type alongside logs, metrics, and traces
- **Convention**: Follows [OpenTelemetry Events Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/general/events/)
- **Implementation**: Leverages the Query layer for querying observability data to evaluate alert rules

## Getting Started

### Prerequisites

#### Required Tools
- **Rust** >= 1.92.0 (for Rust 2021 edition support)
- **Cargo** (Rust's package manager and build tool, included with Rust)
- **Git**

#### Optional Tools
- **rustfmt** - for code formatting (included with Rust)
- **clippy** - for linting and static analysis (included with Rust)
- **rust-analyzer** - for IDE support

### Validating Prerequisites

Check if Rust is installed with the correct version:

```bash
# Check Rust version
rustc --version

# Check Cargo version
cargo --version

# Check rustfmt (optional)
rustfmt --version

# Check clippy (optional)
cargo clippy --version
```

You should have Rust 1.92.0 or later installed.

### Installing Rust

If you don't have Rust installed, use rustup (the recommended Rust toolchain installer):

```bash
# Install Rust via rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Follow the prompts to complete installation
# Then reload your shell or run:
source $HOME/.cargo/env

# Verify installation
rustc --version
cargo --version
```

### Installation

Dependencies are managed via Cargo and specified in `Cargo.toml`. They will be automatically downloaded and compiled when you build the project.

## Development

```bash
cargo build            # Build the project
cargo test             # Run tests
make dev               # Start full development stack with Docker
```

For detailed development setup, build commands, and code quality guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Built with:
- [Apache Iceberg](https://iceberg.apache.org/) - Table format for data lakes
- [Apache Arrow](https://arrow.apache.org/) - Columnar memory format
- [Apache Parquet](https://parquet.apache.org/) - Columnar storage format
- [OpenTelemetry](https://opentelemetry.io/) - Observability framework

## Status

IceGate is currently in prototype development. APIs and features are subject to change.
