//! `LogQL` query benchmarks
//!
//! Benchmarks three categories of queries:
//! 1. Log stream queries (label selectors, line filters)
//! 2. Range aggregations (`count_over_time`, rate, unwrap operations)
//! 3. Vector aggregations (sum, avg with grouping)

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    missing_docs
)]

use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE};

mod common;
use common::harness::{
    TestServer, write_benchmark_logs, write_benchmark_logs_with_numeric_attrs, write_benchmark_logs_with_varied_labels,
};

/// Benchmark Group 1: Log Stream Queries
fn log_stream_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_stream_queries");
    group.sample_size(10);

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: Create server and populate data
    let (server, catalog) = rt.block_on(async { TestServer::start().await.unwrap() });

    let table = rt.block_on(async {
        catalog
            .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE]).unwrap())
            .await
            .unwrap()
    });

    rt.block_on(async {
        write_benchmark_logs(&table, &catalog, 500).await.unwrap();
    });

    // Benchmark 1: Simple label selector
    group.bench_function("simple_selector", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[("query", "{service_name=\"api\"}")])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 2: Multiple matchers with negation
    group.bench_function("multiple_matchers", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[("query", "{service_name=\"api\", severity_text!=\"ERROR\"}")])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 3: Attribute map access
    group.bench_function("attribute_access", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[("query", "{env=\"prod\"}")])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 4: Line filter (contains)
    group.bench_function("line_filter_contains", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[("query", "{service_name=\"api\"} |= \"processed\"")])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 5: Line filter (regex)
    group.bench_function("line_filter_regex", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[("query", "{service_name=\"api\"} |~ \"processed.*\"")])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    drop(group);

    // Cleanup
    rt.block_on(async {
        server.shutdown().await;
    });
}

/// Benchmark Group 2: Range Aggregations
fn range_aggregations(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_aggregations");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: Create server and populate data
    let (server, catalog) = rt.block_on(async { TestServer::start().await.unwrap() });

    let table = rt.block_on(async {
        catalog
            .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE]).unwrap())
            .await
            .unwrap()
    });

    rt.block_on(async {
        write_benchmark_logs(&table, &catalog, 500).await.unwrap();
    });

    // Benchmark 6: count_over_time
    group.bench_function("count_over_time", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[
                        ("query", "count_over_time({service_name=\"api\"}[5m])"),
                        ("step", "60s"),
                    ])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 7: rate
    group.bench_function("rate", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[("query", "rate({service_name=\"api\"}[5m])"), ("step", "60s")])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 8: bytes_over_time
    group.bench_function("bytes_over_time", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[
                        ("query", "bytes_over_time({service_name=\"api\"}[5m])"),
                        ("step", "60s"),
                    ])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    drop(group);

    // Cleanup
    rt.block_on(async {
        server.shutdown().await;
    });
}

/// Benchmark Group 3: Range Aggregations with Unwrap
fn range_aggregations_unwrap(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_aggregations_unwrap");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: Create server and populate data with numeric attributes
    let (server, catalog) = rt.block_on(async { TestServer::start().await.unwrap() });

    let table = rt.block_on(async {
        catalog
            .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE]).unwrap())
            .await
            .unwrap()
    });

    rt.block_on(async {
        write_benchmark_logs_with_numeric_attrs(&table, &catalog, 500).await.unwrap();
    });

    // Benchmark 9: sum_over_time with unwrap (direct attribute access)
    group.bench_function("sum_over_time_unwrap", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[
                        (
                            "query",
                            "sum_over_time({service_name=\"api\"} | unwrap request_time [5m])",
                        ),
                        ("step", "60s"),
                    ])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 10: avg_over_time with direct unwrap
    group.bench_function("avg_over_time_unwrap", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[
                        ("query", "avg_over_time({service_name=\"api\"} | unwrap latency [5m])"),
                        ("step", "60s"),
                    ])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 11: quantile_over_time
    group.bench_function("quantile_over_time", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[
                        (
                            "query",
                            "quantile_over_time(0.95, {service_name=\"api\"} | unwrap latency [5m])",
                        ),
                        ("step", "60s"),
                    ])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    drop(group);

    // Cleanup
    rt.block_on(async {
        server.shutdown().await;
    });
}

/// Benchmark Group 4: Vector Aggregations
fn vector_aggregations(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_aggregations");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: Create server and populate data with varied labels
    let (server, catalog) = rt.block_on(async { TestServer::start().await.unwrap() });

    let table = rt.block_on(async {
        catalog
            .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE]).unwrap())
            .await
            .unwrap()
    });

    rt.block_on(async {
        write_benchmark_logs_with_varied_labels(&table, &catalog, 500).await.unwrap();
    });

    // Benchmark 12: Simple sum (no grouping)
    group.bench_function("sum_no_grouping", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[("query", "sum(rate({service_name=\"api\"}[5m]))"), ("step", "60s")])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 13: sum by (single label)
    group.bench_function("sum_by_single_label", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[
                        ("query", "sum by (pod) (rate({service_name=\"api\"}[5m]))"),
                        ("step", "60s"),
                    ])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 14: avg by (multiple labels)
    group.bench_function("avg_by_multiple_labels", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[
                        (
                            "query",
                            "avg by (namespace, pod) (count_over_time({service_name=\"api\"}[5m]))",
                        ),
                        ("step", "60s"),
                    ])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    // Benchmark 15: sum without
    group.bench_function("sum_without", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = server
                    .client
                    .get(format!("{}/loki/api/v1/query_range", server.base_url))
                    .header("X-Scope-OrgID", "test-tenant")
                    .query(&[
                        ("query", "sum without (pod) (rate({service_name=\"api\"}[5m]))"),
                        ("step", "60s"),
                    ])
                    .send()
                    .await
                    .unwrap();
            });
        });
    });

    drop(group);

    // Cleanup
    rt.block_on(async {
        server.shutdown().await;
    });
}

criterion_group!(
    benches,
    log_stream_queries,
    range_aggregations,
    range_aggregations_unwrap,
    vector_aggregations
);
criterion_main!(benches);
