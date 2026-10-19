//! Flight SQL gRPC integration tests.
//!
//! Tests are organised by category:
//! - `harness`: shared bootstrap (engine, server, client)
//! - `smoke`: handshake + `SELECT 1` round-trip
//! - `tenant`: cross-tenant isolation enforced by the session provider
//! - `read_only`: DDL/DML rejection by `SQLOptions`
//! - `metadata`: catalog/schema/table introspection RPCs
//! - `tls`: the listener served over TLS

mod flight_sql;
#[path = "support/tls_certificate.rs"]
mod tls_certificate;
