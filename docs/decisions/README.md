# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for IceGate.

## What is an ADR?

An ADR is a document that captures an important architectural decision made along with its context and consequences. We use the [MADR](https://adr.github.io/madr/) (Markdown Any Decision Records) format.

## Index

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [0001](0001-use-iceberg-for-storage.md) | Use Apache Iceberg for Storage | Accepted | 2024-12-16 |
| [0002](0002-use-datafusion-query-engine.md) | Use Apache DataFusion as Query Engine | Accepted | 2024-12-16 |
| [0003](0003-queue-based-ingestion.md) | Use Queue-Based Ingestion with Parquet WAL | Accepted | 2024-12-16 |
| [0004](0004-parquet-queue-format.md) | Use Parquet for Queue Segment Format | Accepted | 2024-12-16 |

## Creating a New ADR

1. Copy `template.md` to `XXXX-title-with-dashes.md`
2. Fill in the template sections
3. Add an entry to this index
4. Submit a PR for review

## Statuses

- **proposed** - Under discussion
- **accepted** - Decision has been made
- **rejected** - Decision was considered but rejected
- **deprecated** - Decision is no longer relevant
- **superseded** - Replaced by another ADR
