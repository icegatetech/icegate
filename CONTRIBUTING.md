# Contributing to IceGate

Thank you for your interest in contributing to IceGate! This document provides guidelines and information for contributors.

## Ways to Contribute

- **Report bugs** and request features via [GitHub Issues](../../issues)
- **Submit pull requests** for bug fixes or new features
- **Improve documentation** to help others understand IceGate
- **Share your use cases** and feedback

## Development Setup

### Prerequisites

- **Rust** at or above the `rust-version` declared in the workspace [`Cargo.toml`](Cargo.toml)
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
   rustc --version  # Must be at or above Cargo.toml's rust-version
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

This starts RustFS (S3), Grafana, and other services via Docker Compose. The Iceberg catalog is IceGate's own S3-backed catalog (no external catalog service by default); Nessie starts only under the optional `analytics` profile.

## Development Workflow

### Building

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo build --bin query        # Build specific binary
```

### Testing

See the project [testing policy](docs/tests.md) for required coverage and test design.

```bash
cargo test -p <crate>          # Run tests for the affected crate
cargo test --workspace         # Run workspace tests with default features
cargo test test_name           # Run specific test
cargo test -- --nocapture      # Run tests with output shown
cargo test -p icegate-catalog-s3 --all-targets --features rest
                               # Run catalog REST feature tests
```

Some integration tests start an S3-compatible object store through testcontainers and
require a working Docker-compatible runtime. If required infrastructure is unavailable,
state which tests were not run; do not silently treat them as passing.

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

3. **Add or update required tests**

4. **Run all checks** before committing:
   ```bash
   make ci
   ```

5. **Commit your changes** with clear, descriptive commit messages

6. **Push to your fork** and create a pull request against `main`

7. **Respond to review feedback** and make requested changes

### Pull Request Guidelines

- Keep PRs focused on a single concern
- Update documentation if needed
- Ensure all CI checks pass
- Provide a clear description of what the PR does and why

## Project Structure

```
crates/
├── icegate-common/     # Shared infrastructure
├── icegate-queue/      # Queue service
├── icegate-query/      # Query APIs + CLI
├── icegate-ingest/     # OTLP receivers
└── icegate-maintain/   # Maintenance operations + CLI
```

See `AGENTS.md` for detailed architecture documentation.

## Third-party fork: icegatetech/iceberg-rust

IceGate uses a fork of `apache/iceberg-rust` for Iceberg table operations. See [#25](../../issues/25) for tracking.

**Required patches:**
- DataFusion 51 + Arrow 57 compatibility (upstream [PR #1830](https://github.com/apache/iceberg-rust/pull/1830) closed)
- Memory catalog builder enhancements for testing

**Upstream plan:** Switch to upstream once DataFusion 51 is officially supported.

**Maintenance:**
- Fork branch: `develop` (continuously rebased on upstream `main`)
- Responsible: IceGate maintainers
- Regression testing: Run `make ci` after each sync

## Dependency: icegatetech/jobmanager

The job/task framework that drives ingest's shift pipeline and maintain's compaction, GC, and
pricing loops lives in its own repository, [icegatetech/jobmanager](https://github.com/icegatetech/jobmanager),
as the `jobmanager` crate. It is a general-purpose tool with no IceGate specifics, so it is
developed, tested, and released on its own; keeping it here would tie a standalone project to
IceGate's release cycle and let IceGate types leak into its API.

**Pin policy:** the crate is not published to a registry, so a version IS a git rev. It is declared
once in the root `Cargo.toml` under `[workspace.dependencies]` and pinned to an immutable rev, never
a branch, so builds are reproducible. Bumping the SHA is a deliberate change: run the ingest and
maintain integration tests with the new rev, since the pin covers runtime behaviour the type system
does not.

**Local co-development.** To work on both repositories at once, override the git source locally
without touching the committed pin:

```toml
# .cargo/config.toml (local only, git-ignored)
[patch."https://github.com/icegatetech/jobmanager"]
jobmanager = { path = "../jobmanager" }
```

Remove the patch before running the checks you intend to trust: with it in place you are testing
your working copy, not the pinned rev that CI will build.

**Audit caveat:** `cargo audit` matches advisories against crates.io versions and skips git
sources, so `jobmanager`'s own dependency tree is not covered by this repository's audit job — its
CI audits it.

## Code of Conduct

Be respectful and constructive in all interactions. We are committed to providing a welcoming and inclusive environment for all contributors.

## Questions?

If you have questions about contributing, feel free to open an issue for discussion.

## License

By contributing to IceGate, you agree that your contributions will be licensed under the Apache License 2.0.
