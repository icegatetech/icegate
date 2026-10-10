//! OpenSearch-compatible API integration tests. Every request is a literal in the test, and every
//! expected result is stated there.
//!
//! - `cluster`: the cluster-level endpoints
//! - `harness`: the listener over a temporary warehouse, and a reqwest client
//! - `listener`: the listener's start-up
//! - `rustfs_conditional`: the object-store guarantees the system-index store builds on

mod opensearch;
