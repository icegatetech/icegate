# GEMINI.md

## Project Overview

This project, "icegate," is an observability data lake engine built with Rust. Its goal is to provide a fast, scalable, and cost-effective solution for managing observability data (metrics, traces, logs, and events). The project now primarily follows a mono binary approach for its core functionalities, with a dedicated binary for the catalog.

The architecture is based on open standards, including:

*   **Apache Iceberg:** For the data lake's table format, providing ACID transactions.
*   **Apache DataFusion and Apache Arrow:** As the query engine.
*   **Apache Parquet:** For efficient data storage.
*   **OpenTelemetry:** For data ingestion.

The system integrates several core functionalities:

*   **Main Binary (icegate):** This single executable includes the `ingest`, `query`, `maintain`, and `alert` components. By default, all these components run together, but the binary can be configured to run only specific components.
    *   **Ingest:** Ingests observability data via the OpenTelemetry protocol.
    *   **Query:** Provides a query engine for logs, metrics, traces, and events.
    *   **Maintain:** Optimizes data for efficient querying.
    *   **Alert:** Manages alerting rules and generates events.
*   **Dedicated Binary (catalog):** This is a separate executable responsible for managing the data lake's organization using a custom Apache Iceberg catalog implementation.

## Building and Running

The project is built and managed using `cargo`.

### Building

*   **Debug build (all binaries):**
    ```bash
    cargo build
    ```
*   **Release build (all binaries):**
    ```bash
    cargo build --release
    ```
*   **Build specific binaries:**
    ```bash
    # Build the main 'icegate' binary
    cargo build --bin icegate

    # Build the 'catalog' dedicated binary
    cargo build --bin catalog
    ```

### Running

*   **Run the main 'icegate' binary (all components by default):**
    ```bash
    cargo run --bin icegate
    ```
    To run with specific components, command-line arguments would typically be used. (TODO: Add example for running with specific components once the CLI interface is known).

*   **Run the 'catalog' dedicated binary:**
    ```bash
    cargo run --bin catalog
    ```

### Testing

*   **Run all tests:**
    ```bash
    cargo test
    ```

## Development Conventions

### Code Style

The project uses `rustfmt` for code formatting. To format the code, run:

```bash
cargo fmt
```

### Linting

The project uses `clippy` for linting. To run the linter, use:

```bash
cargo clippy
```

The linting rules are configured in the `Cargo.toml` file.
