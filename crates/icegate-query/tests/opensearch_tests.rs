//! OpenSearch-compatible API integration tests. Every request is a literal in the test, and every
//! expected result is stated there.
//!
//! - `cat`: `_cat/indices` and `_cat/plugins`
//! - `cluster`: the cluster-level endpoints
//! - `data`: table rows with explicit values, written through the production writer
//! - `fields`: `_field_caps`, `_mapping` and `_mapping/field`
//! - `harness`: the listener over a temporary warehouse, and a reqwest client
//! - `indices`: `_resolve/index` and `<index>/_settings`
//! - `listener`: the listener's start-up
//! - `rustfs_conditional`: the object-store guarantees the system-index store builds on
//! - `saved_objects`: system-index document endpoints
//! - `system_indices`: index-level system-index endpoints and their tenant boundary
//! - `system_store_rustfs`: the system-index store against RustFS through the production constructor

mod opensearch;
