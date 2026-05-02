//! Tempo API integration tests.
//!
//! Boots a Tempo HTTP server on an ephemeral port against an in-memory
//! iceberg catalog backed by a temp-dir warehouse, writes a small set of
//! span records and exercises the tag-discovery endpoints end-to-end.

mod tempo;
