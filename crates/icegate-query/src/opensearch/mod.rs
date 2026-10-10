//! `OpenSearch`-compatible HTTP API.
//!
//! Cluster and index metadata, Query DSL, Lucene and PPL over IceGate's tables, and saved objects
//! in per-tenant system indices.
//!
//! The tenant of every request is decided by the shared tenant policy (`QueryConfig::tenant`), as
//! on every query surface, and every data read goes through the tenant-scoped catalog of
//! `engine::tenant_scope`.

mod config;

pub use config::OpenSearchConfig;
