//! Apache Arrow Flight SQL gRPC server.
//!
//! Exposes the IceGate `DataFusion` engine over the Flight SQL protocol
//! so JDBC/ODBC drivers, ADBC clients, the `DataFusion` CLI, and
//! BI tools (Tableau, DBeaver, Apache Superset) can query the merged
//! WAL + Iceberg view of the observability tables with no IceGate-
//! specific client code.
//!
//! Tenant isolation is enforced per-request: every gRPC call goes
//! through [`provider::IceGateSessionStateProvider`], which wraps the
//! session's `iceberg` catalog in a tenant-scoped decorator. Clients
//! query the canonical path `iceberg.icegate.<table>` (`logs`, `spans`,
//! `events`, `metrics`); the decorator enforces `tenant_id = '<t>'` at
//! the row level on every scan and hides the `tenant_id` column from the
//! advertised schema. There are intentionally no convenience views — the
//! single qualified path keeps the guarantee anchored to one layer that
//! no lookup can bypass. DDL and DML submitted as SQL text or prepared
//! statements are rejected up-front via `SQLOptions`; the read-only
//! `TableProvider`s are the backstop for any other path (e.g. Substrait
//! plans, which `SQLOptions` does not gate), so the endpoint is strictly
//! read-only either way — data lands through ingest, never via SQL writes.

mod config;
mod provider;
mod server;
mod tenant_catalog;
mod tenant_id;

pub use config::FlightSqlConfig;
pub use server::{run, run_with_port_tx};
