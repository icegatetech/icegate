//! Flight SQL gRPC integration tests.
//!
//! Tests are organised by category:
//! - `harness`: shared bootstrap (engine, server, client)
//! - `smoke`: handshake + `SELECT 1` round-trip
//! - `tenant`: cross-tenant isolation enforced by the session provider
//! - `read_only`: DDL/DML rejection by `SQLOptions`
//! - `metadata`: catalog/schema/table introspection RPCs

mod flight_sql;
