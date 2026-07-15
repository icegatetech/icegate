# AGENTS.md

Guidance for AI agents working in this repo. This file holds the rules for **how
to write code** here. Operational detail (commands, ports, service URLs) lives in
the executable files and the README — this file points at them, never copies them.

- What IceGate is, features, architecture overview: [README.md](README.md).
- Rust rules (naming, errors, types, testing, imports, formatting): [RUST.md](RUST.md) — binding, not restated below.
- Deep-dive docs are linked per crate in the table below (a `—` means the crate has none yet).

## Operational commands

Do not reproduce these in prose — read the source of truth:

- Build / test / lint / CI: the [`Makefile`](Makefile) targets (`make test` / `check` / `clippy` / `fmt` / `ci`). Lint
  config: `Cargo.toml` + `clippy.toml`.
- Dev environment, services, ports, credentials, quick start, k8s: [README.md](README.md).
- LogQL parser regeneration (Java + ANTLR): [logql/README](crates/icegate-query/src/logql/README.md).

Running rules:

- **Do not** run a full `make ci`, build, `cargo test`, or formatting without an explicit request — prefer targeted
  `cargo test <name>` for the code you touched.
- Format only the affected crate: `cargo +nightly fmt -p <crate>` (plain `cargo fmt` ignores `rustfmt.toml`).

## Crates and where code goes

Place a change in the crate that owns its responsibility; shared infrastructure
goes in `icegate-common`, never copied into a component.

| Crate                | Responsibility                                                                                 | Deep-dive docs                                                                                           |
|----------------------|------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| `icegate-common`     | Shared foundation: Iceberg catalog builder, storage/S3 config, table schema, error types.      | schema [SCHEMA.md](crates/icegate-common/src/SCHEMA.md)                                                  |
| `icegate-catalog-s3` | S3-backed Iceberg REST catalog. Has its own `domain`/`services`/`storage` layers.              | [AGENTS](crates/icegate-catalog-s3/AGENTS.md), [README](crates/icegate-catalog-s3/README.md)             |
| `icegate-queue`      | Generic WAL data queue on object storage: durable-before-ack, exactly-once, per-topic offsets. | [README](crates/icegate-queue/README.md)                                                                 |
| `icegate-ingest`     | OTLP receivers (gRPC/HTTP), transform, WAL write.                                              | per-tenant task model [README](crates/icegate-ingest/README.md)                                                   |
| `icegate-query`      | Query APIs (Loki/Prometheus/Tempo), query engine, LogQL, query CLI.                            | [README](crates/icegate-query/README.md), LogQL [logql/README](crates/icegate-query/src/logql/README.md) |
| `icegate-maintain`   | Background maintenance: compaction, GC, schema migration, maintain CLI.                        | compaction [compact/README](crates/icegate-maintain/src/compact/README.md)                               |
| `icegate-jobmanager` | Background job/task execution framework: registry, execution, S3 job-state.                    | —                                                                                                        |

### Dependency rules
- `icegate-common` is the foundation and depends on no other workspace crate;
- Components depend on `common` plus the library crates they need (`queue`, `jobmanager`, `catalog-s3`).
- `catalog-s3`'s production library pulls no other workspace crate (dev-only dependency on `common`).
- `jobmanager` should not depend on any Icegate component, including should not depend on `common`. The `jobmanager` will be allocated to a separate repository as an independent project.

### Where docs live

- Crate-specific docs live **with the crate**, never in the root `docs/`: agent
  instructions ("how to write code in this crate") go in `crates/<crate>/AGENTS.md`;
  overview, deep-dive, and invariants go in a README beside the code (crate root,
  or the subsystem — e.g. [`compact/README`](crates/icegate-maintain/src/compact/README.md)).
- Root [`docs/`](docs) holds only cross-crate, project-wide material
  (e.g. [tests.md](docs/tests.md)) — never a `docs/<crate>.md`.
- Why: a crate's docs then travel with it (`jobmanager` will move to its own repo)
  and sit next to the code they describe, which resists drift.

## Iceberg and data invariants

- **Schema is single-source.** The four tables (logs, spans, events, metrics) are
  defined once in [`schema.rs`](crates/icegate-common/src/schema.rs) (DDL in
  [SCHEMA.md](crates/icegate-common/src/SCHEMA.md)). **NEVER** hardcode columns or
  types anywhere else — derive from the schema.
- Table conventions (tenant_id partitioning, ZSTD, `MAP(VARCHAR,VARCHAR)`
  attributes) are inherited from the schema, not re-decided at a call site.
- Writes commit as **atomic Iceberg snapshots** with optimistic-concurrency retry
  through the generic catalog; maintenance work **never blocks ingest**. Per-flow
  detail lives in the crate docs (e.g. compaction's README).
- Durability contract of the WAL queue: data is persisted to object storage
  before it is acknowledged (exactly-once). See the queue README.

## Writing code

- [RUST.md](RUST.md) is binding for naming, errors (`thiserror`/`anyhow`), the
  type system, testing, imports, and style. Follow it; do not duplicate it here.
- **A convention is only what is documented** in this file, RUST.md, or `docs/`.
  The mere presence of a pattern in the code is **NOT** a convention — someone may
  have committed junk. Do not justify a decision with "the existing code does X";
  cite the documented rule, or propose adding one if it is missing.
- Separation of responsibility comes first; apply DRY; the lints forbid dead code.
- Schema, config fields, and service ports/URLs are referenced from their source,
  never copied as literals into working code.
- It is better to return an error than to use, calculate, or show invalid data.

## Before a change

- Determine the crate and the layer the change belongs to; shared logic goes to
  `common`, not into a component.
- Cover significant behaviour with tests — read [docs/tests.md](docs/tests.md) first.
- Do not break the Iceberg schema, nor the public API/CLI contracts.

## Before finishing

- Follow [docs/tests.md](docs/tests.md). Report which test commands were run and
  which required tests were not.
- Run targeted tests for the affected functionality; leave full `make ci` to an
  explicit request.
- Keep `TODO` comments intact unless the change fully resolves them.
- Ensure each file ends with a single trailing newline.

## Code style

The project uses `rustfmt`; configuration is in `rustfmt.toml`.
@RUST.md
