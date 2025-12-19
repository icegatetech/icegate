//! Loki API integration tests
//!
//! Tests are organized by category:
//! - `harness`: Common test utilities and setup
//! - `query_range`: Basic `query_range` and explain endpoint tests
//! - `metrics`: Metric query tests (`count_over_time`, rate, `bytes_rate`, etc.)
//! - `labels`: Label metadata endpoint tests (labels, label values, series)
//! - `tenant`: Tenant isolation tests
//! - `trace`: Trace ID and span ID query tests

mod harness;
mod labels;
mod metrics;
mod query_range;
mod tenant;
mod trace;
