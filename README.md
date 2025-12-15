# IceGate

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)]()

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
- **Rust** >= 1.80.0 (for Rust 2024 edition support)
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

You should have Rust 1.80.0 or later installed.

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

### Building from Source

The project uses Cargo for building:

```bash
# Build in debug mode (default)
cargo build

# Build in release mode (optimized)
cargo build --release

# Build specific binaries
cargo build --bin query
cargo build --bin ingest
```

Build artifacts will be located in:
- Debug: `target/debug/`
- Release: `target/release/`

### Running Tests

```bash
# Run all tests
cargo test

# Run tests with output shown
cargo test -- --nocapture

# Run tests in release mode
cargo test --release

# Run specific test
cargo test test_name
```

### Code Quality

The project includes configuration for code quality tools:

#### Code Formatting
```bash
# Format all source files
cargo fmt

# Check formatting without making changes
cargo fmt -- --check
```

Configuration: `rustfmt.toml` (if present) or default Rust formatting

#### Linting and Static Analysis
```bash
# Run clippy linter
cargo clippy

# Run clippy with all warnings
cargo clippy -- -W clippy::all

# Run clippy and treat warnings as errors
cargo clippy -- -D warnings
```

Configuration: `clippy.toml` (if present) or default clippy rules

#### Editor Configuration
The project includes `.editorconfig` for consistent coding styles across different editors.

### Project Structure

```
icegate/
├── src/
│   ├── bin/
│   │   ├── query.rs     # Query component binary
│   │   └── ingest.rs    # Ingest component binary
│   └── iceberg/
│       └── mod.rs       # Iceberg catalog module
├── target/              # Build output (generated)
├── Cargo.toml           # Project manifest and dependencies
└── Cargo.lock           # Dependency lock file
```

### Development Workflow

1. **Validate prerequisites**: `rustc --version && cargo --version`
2. **Build**: `cargo build`
3. **Test**: `cargo test`
4. **Format code**: `cargo fmt`
5. **Run linter**: `cargo clippy`
6. **Run binaries**: `cargo run --bin query` or `cargo run --bin ingest`

### Cargo Build Profiles

Cargo supports different build profiles (see `Cargo.toml` for customization):
- **dev** (default) - Debug build with symbols and no optimization (`cargo build`)
- **release** - Optimized release build with maximum performance (`cargo build --release`)
- **test** - Used when running tests (`cargo test`)
- **bench** - Used for benchmarks (`cargo bench`)

## Usage

Detailed usage documentation will be provided as features are implemented.

## Contributing

Contributions are welcome! Here's how you can help:

- **Report bugs** and request features via [GitHub Issues](../../issues)
- **Submit pull requests** for bug fixes or new features
- **Improve documentation** to help others understand IceGate
- **Share your use cases** and feedback

Please check `CONTRIBUTING.md` for detailed guidelines (coming soon).

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Built with:
- [Apache Iceberg](https://iceberg.apache.org/) - Table format for data lakes
- [Apache Arrow](https://arrow.apache.org/) - Columnar memory format
- [Apache Parquet](https://parquet.apache.org/) - Columnar storage format
- [OpenTelemetry](https://opentelemetry.io/) - Observability framework

## Status

IceGate is currently in alpha development. APIs and features are subject to change.
