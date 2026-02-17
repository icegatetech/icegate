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

use std::sync::Arc;

pub use catalog::IcegateCatalogProvider;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
pub use metrics::{SourceMetrics, extract_source_metrics};

/// Strip `PARQUET:field_id` metadata from all fields in an Arrow schema.
///
/// Iceberg adds `PARQUET:field_id` metadata to track column identity. This
/// metadata causes schema mismatches when combining Iceberg scans with
/// Parquet scans (WAL) that don't have it. Stripping it produces a "clean"
/// schema that both sources can share.
pub(super) fn strip_parquet_metadata(schema: &ArrowSchemaRef) -> ArrowSchemaRef {
    let clean_fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            let mut metadata = f.metadata().clone();
            metadata.remove("PARQUET:field_id");
            f.as_ref().clone().with_metadata(metadata)
        })
        .collect();
    Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
        clean_fields,
        schema.metadata().clone(),
    ))
}
