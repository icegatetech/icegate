# Contributing to IceGate

Thank you for your interest in contributing to IceGate! This document provides guidelines and information for contributors.

## Ways to Contribute

- **Report bugs** and request features via [GitHub Issues](../../issues)
- **Submit pull requests** for bug fixes or new features
- **Improve documentation** to help others understand IceGate
- **Share your use cases** and feedback

## Development Setup

### Prerequisites

- **Rust** >= 1.92.0 (for Rust 2021 edition support)
- **Cargo** (included with Rust)
- **Git**
- **Docker** and **Docker Compose** (for development environment)

### Getting Started

1. Fork and clone the repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/icegate.git
   cd icegate
   ```

2. Verify your Rust installation:
   ```bash
   rustc --version  # Should be 1.92.0 or later
   cargo --version
   ```

3. Build the project:
   ```bash
   cargo build
   ```

4. Run tests:
   ```bash
   cargo test
   ```

### Development Environment

Start the full development stack:
```bash
make dev     # Run full stack with hot-reload
make debug   # Run without query service for debugging
```

This starts MinIO (S3), Nessie (Iceberg catalog), Grafana, and other services via Docker Compose.

## Development Workflow

### Building

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo build --bin query        # Build specific binary
```

### Testing

```bash
cargo test                     # Run all tests
cargo test test_name           # Run specific test
cargo test -- --nocapture      # Run tests with output shown
```

### Code Quality

Before submitting a pull request, ensure your code passes all checks:

```bash
make ci      # Run all CI checks (check, fmt, clippy, test, audit)
```

Or run checks individually:

```bash
make check   # Check all targets compile
make fmt     # Check code formatting
make clippy  # Run linter with warnings as errors
make audit   # Run security audit
```

**Important:** Do not run `cargo +nightly fmt` directly via rustup as it doesn't respect `rustfmt.toml`. Use `make fmt` instead.

### Code Style

- Follow the existing code style in the project
- Code is formatted using `rustfmt` with configuration in `rustfmt.toml`
- Strict linting is enforced via clippy (see `Cargo.toml` for rules)
- Ensure each file ends with a newline

## Pull Request Process

1. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the code style guidelines

3. **Write tests** for new functionality

4. **Run all checks** before committing:
   ```bash
   make ci
   ```

5. **Commit your changes** with clear, descriptive commit messages

6. **Push to your fork** and create a pull request against `main`

7. **Respond to review feedback** and make requested changes

### Pull Request Guidelines

- Keep PRs focused on a single concern
- Include tests for new functionality
- Update documentation if needed
- Ensure all CI checks pass
- Provide a clear description of what the PR does and why

## Project Structure

```
crates/
├── icegate-common/     # Shared infrastructure
├── icegate-query/      # Query APIs + CLI
├── icegate-ingest/     # OTLP receivers
└── icegate-maintain/   # Maintenance operations + CLI
```

See `AGENTS.md` for detailed architecture documentation.

## Code of Conduct

Be respectful and constructive in all interactions. We are committed to providing a welcoming and inclusive environment for all contributors.

## Questions?

If you have questions about contributing, feel free to open an issue for discussion.

## License

By contributing to IceGate, you agree that your contributions will be licensed under the Apache License 2.0.
