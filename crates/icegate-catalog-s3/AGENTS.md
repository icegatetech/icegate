# icegate-catalog-s3

S3-backed Iceberg catalog using a single `root.json` file as the source of truth. No external database — all state lives in S3.

## Purpose

Implements the `iceberg::Catalog` trait to store the table catalog directly in S3. Atomicity is provided through compare-and-swap (CAS) using the ETag of the `root.json` object.

Used through `icegate-common`: `CatalogBackend::S3` in configuration -> `CatalogBuilder::create_s3_catalog`.

## S3 Layout

```text
{warehouse}/catalog/
├── root.json                                  # catalog: all tables, statuses, metadata_location
└── tables/{table-id}/metadata/
    └── {version:05}-{uuid}.json              # table metadata snapshot
```

## Architecture

```
S3Catalog (iceberg::Catalog)
    └── CatalogStorage (trait)
            ├── S3CatalogStorage   — object_store + S3 CAS
            └── InMemoryCatalogStorage — only in #[cfg(test)]
```

### Layers and Responsibilities

| Layer | File | Responsibility                                    |
|---|---|---------------------------------------------------|
| Catalog | `catalog.rs` | Iceberg API and commit logic orchestration        |
| Domain | `model.rs` | domain entity business logic                      |
| Storage | `storage/` | I/O: load/save root, read/write metadata          |
| Codec | `codec/` | serialization of domain entities in different formats |


## Important Instructions
- The component is under active development, not in prod. Backward compatibility is not necessary.
- The work with the metadata of the table **MUST** be carried out strictly according to the Iceberg specification.