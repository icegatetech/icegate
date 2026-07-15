//! Iceberg REST Catalog API.

mod config;
mod dto;
mod error;
mod extract;
mod handlers;
mod identifier;
mod pagination;
mod routes;
mod state;
mod table_metadata;

pub use config::{CatalogApiConfig, CatalogApiConfigError, CatalogPrefix, CatalogPrefixError};
pub use routes::router;

/// Immutable revision of the Apache Iceberg REST `OpenAPI` contract implemented here.
///
/// Source: <https://github.com/apache/iceberg/blob/be27af46df4316b41787a62382c5aa330a173c1e/open-api/rest-catalog-open-api.yaml>
pub const REST_SPEC_REVISION: &str = "be27af46df4316b41787a62382c5aa330a173c1e";
