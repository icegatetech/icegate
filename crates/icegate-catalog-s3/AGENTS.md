# icegate-catalog-s3

S3-backed Iceberg catalog using a single `root.json` file as the source of truth. No external database — all state lives
in S3.

## Purpose

Implements the `iceberg::Catalog` trait to store the table catalog directly in S3. Atomicity is provided through
compare-and-swap (CAS) using the ETag of the `root.json` object.

The crate has two modes:

- Library mode implements `iceberg::Catalog` and is used through `icegate-common`:
  `CatalogBackend::S3` in configuration -> `CatalogBuilder::create_s3_catalog`.
- Server mode is enabled with the `rest` feature and provides the standalone `catalog` binary.

The `rest` feature is opt-in so library consumers do not compile the REST API, CLI, or their
server dependencies.

## S3 Layout

```text
{warehouse}/catalog/
├── root.json                                  # catalog: all tables, statuses, metadata_location
└── tables/{table-id}/metadata/
    └── {version:05}-{uuid}.json              # table metadata snapshot
```

### Layers and Responsibilities

| Layer    | Path                                   | Responsibility                                                        |
|----------|----------------------------------------|-----------------------------------------------------------------------|
| API      | `api/` (feature `rest`)                | Iceberg REST Catalog HTTP contract: routing, DTOs, extractors, errors |
| CLI      | `cli/` (bin `catalog`, feature `rest`) | Process lifecycle: argument parsing, config loading, serve/shutdown   |
| Services | `services/`                            | `impl iceberg::Catalog`, commit orchestration                        |
| Domain   | `domain/`                              | Catalog entities and invariants (`CatalogRoot`, `DomainError`)       |
| Storage  | `storage/`                             | I/O: load/save root with CAS, read/write table metadata              |
| Infra    | `infra/`                               | Cross-cutting utilities (`Retrier`)                                  |

## Important Instructions

- The component is under active development, not in prod. Backward compatibility is not necessary.
- The work with the metadata of the table **MUST** be carried out strictly according to the Iceberg specification.
- Responsibility **MUST** be strictly divided into layers according to the Layers and Responsibilities section.
- The `api` layer **MUST** access the catalog only through the public `S3Catalog` service and **MUST NOT** depend on `storage`, `codec`, or `domain`.
- Domain entities **SHOULD NOT** depend on other layers.
- The REST API implementation **MUST** fully support the Iceberg REST Catalog specification contract.
