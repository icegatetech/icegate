//! Custom DataFusion providers that merge Iceberg (cold) and WAL (hot) data.
//!
//! The standard `iceberg-datafusion` crate has no extension points -- all inner
//! types have `pub(crate)` constructors. This module reimplements the provider
//! chain so that queries transparently read from both Iceberg tables and WAL
//! segments, closing the ~30s data lag between OTLP ingestion and Iceberg commit.
//!
//! # Architecture
//!
//! ```text
//! IcegateCatalogProvider
//!   -> IcegateSchemaProvider (for "icegate" namespace)
//!     -> IcegateTableProvider (for "logs" table)
//!       -> UnionExec(IcegateIcebergScan, DataSourceExec[WAL Parquet])
//! ```

mod catalog;
mod expr_to_predicate;
mod metrics;
mod scan;
mod schema;
mod table;

pub use catalog::IcegateCatalogProvider;
pub use metrics::{SourceMetrics, extract_source_metrics};
