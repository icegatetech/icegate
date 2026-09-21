//! `LogQL` query benchmarks
//!
//! Benchmarks three categories of queries:
//! 1. Log stream queries (label selectors, line filters)
//! 2. Range aggregations (`count_over_time`, rate, unwrap operations)
//! 3. Vector aggregations (sum, avg with grouping)
//!
//! A single `TestServer` is shared across all benchmark groups, with all data
//! variants written once during setup to avoid repeated server startup overhead.
//! Each dataset is tagged with a `dataset` attribute so queries only hit the
//! intended 500-row corpus.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    missing_docs
)]

use std::time::Duration;

use criterion::{BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime};
use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE, TENANT_ID_HEADER};
use tokio::runtime::Runtime;

mod common;
use common::harness::{
    BENCH_TENANT_ID, TestServer, write_benchmark_logs, write_benchmark_logs_with_numeric_attrs,
    write_benchmark_logs_with_varied_labels,
};

/// Send one benchmark `query_range` request, naming the tenant the fixtures are
/// written under.
async fn send_query_range(server: &TestServer, params: &[(&str, &str)]) -> reqwest::Response {
    server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header(TENANT_ID_HEADER, BENCH_TENANT_ID)
        .query(params)
        .send()
        .await
        .unwrap()
}

/// Register the benchmark `name` measuring the `query_range` request `params`.
///
/// The request is sent once and checked before it is measured: the measured
/// loop discards the response, so a refusal — a tenant the server does not
/// serve, a query that does not parse — would otherwise be published as the
/// query's time.
fn bench_query_range(
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
    server: &TestServer,
    name: &str,
    params: &[(&str, &str)],
) {
    let status = rt.block_on(send_query_range(server, params)).status();
    assert!(
        status.is_success(),
        "{name}: the benchmark query must succeed before it is measured, got {status}"
    );
    group.bench_function(name, |b| b.iter(|| rt.block_on(send_query_range(server, params))));
}

/// All Loki query benchmarks sharing a single `TestServer`.
///
/// Writing all data variants once and reusing the server across groups avoids
/// 3 extra server startups + data writes, saving ~30-60 seconds.
#[allow(clippy::too_many_lines)]
fn loki_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Setup: single server with all data variants
    let (server, catalog) = rt.block_on(async { TestServer::start().await.unwrap() });

    let table = rt.block_on(async {
        catalog
            .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE]).unwrap())
            .await
            .unwrap()
    });

    rt.block_on(async {
        // Write all data variants to the same table.
        // Each helper tags rows with dataset="baseline"/"numeric"/"varied"
        // so queries only hit the intended 500-row corpus.
        write_benchmark_logs(&table, &catalog, 500).await.unwrap();
        write_benchmark_logs_with_numeric_attrs(&table, &catalog, 500).await.unwrap();
        write_benchmark_logs_with_varied_labels(&table, &catalog, 500).await.unwrap();
    });

    // --- Group 1: Log Stream Queries (dataset=baseline) ---
    {
        let mut group = c.benchmark_group("log_stream_queries");
        group.sample_size(10);

        // Benchmark 1: Simple label selector
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "simple_selector",
            &[("query", "{service_name=\"api\", dataset=\"baseline\"}")],
        );

        // Benchmark 2: Multiple matchers with negation
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "multiple_matchers",
            &[(
                "query",
                "{service_name=\"api\", dataset=\"baseline\", severity_text!=\"ERROR\"}",
            )],
        );

        // Benchmark 3: Attribute map access
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "attribute_access",
            &[("query", "{dataset=\"baseline\", env=\"prod\"}")],
        );

        // Benchmark 4: Line filter (contains)
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "line_filter_contains",
            &[("query", "{service_name=\"api\", dataset=\"baseline\"} |= \"processed\"")],
        );

        // Benchmark 5: Line filter (regex)
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "line_filter_regex",
            &[(
                "query",
                "{service_name=\"api\", dataset=\"baseline\"} |~ \"processed.*\"",
            )],
        );

        drop(group);
    }

    // --- Group 2: Range Aggregations (dataset=baseline) ---
    {
        let mut group = c.benchmark_group("range_aggregations");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(15));

        // Benchmark 6: count_over_time
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "count_over_time",
            &[
                (
                    "query",
                    "count_over_time({service_name=\"api\", dataset=\"baseline\"}[5m])",
                ),
                ("step", "60s"),
            ],
        );

        // Benchmark 7: rate
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "rate",
            &[
                ("query", "rate({service_name=\"api\", dataset=\"baseline\"}[5m])"),
                ("step", "60s"),
            ],
        );

        // Benchmark 8: bytes_over_time
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "bytes_over_time",
            &[
                (
                    "query",
                    "bytes_over_time({service_name=\"api\", dataset=\"baseline\"}[5m])",
                ),
                ("step", "60s"),
            ],
        );

        drop(group);
    }

    // --- Group 3: Range Aggregations with Unwrap (dataset=numeric) ---
    {
        let mut group = c.benchmark_group("range_aggregations_unwrap");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(20));

        // Benchmark 9: sum_over_time with unwrap (direct attribute access)
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "sum_over_time_unwrap",
            &[
                (
                    "query",
                    "sum_over_time({service_name=\"api\", dataset=\"numeric\"} | unwrap request_time [5m])",
                ),
                ("step", "60s"),
            ],
        );

        // Benchmark 10: avg_over_time with direct unwrap
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "avg_over_time_unwrap",
            &[
                (
                    "query",
                    "avg_over_time({service_name=\"api\", dataset=\"numeric\"} | unwrap latency [5m])",
                ),
                ("step", "60s"),
            ],
        );

        // Benchmark 11: quantile_over_time
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "quantile_over_time",
            &[
                (
                    "query",
                    "quantile_over_time(0.95, {service_name=\"api\", dataset=\"numeric\"} | unwrap latency [5m])",
                ),
                ("step", "60s"),
            ],
        );

        drop(group);
    }

    // --- Group 4: Vector Aggregations (dataset=varied) ---
    {
        let mut group = c.benchmark_group("vector_aggregations");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(20));

        // Benchmark 12: Simple sum (no grouping)
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "sum_no_grouping",
            &[
                ("query", "sum(rate({service_name=\"api\", dataset=\"varied\"}[5m]))"),
                ("step", "60s"),
            ],
        );

        // Benchmark 13: sum by (single label)
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "sum_by_single_label",
            &[
                (
                    "query",
                    "sum by (pod) (rate({service_name=\"api\", dataset=\"varied\"}[5m]))",
                ),
                ("step", "60s"),
            ],
        );

        // Benchmark 14: avg by (multiple labels)
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "avg_by_multiple_labels",
            &[
                (
                    "query",
                    "avg by (namespace, pod) (count_over_time({service_name=\"api\", dataset=\"varied\"}[5m]))",
                ),
                ("step", "60s"),
            ],
        );

        // Benchmark 15: sum without
        bench_query_range(
            &mut group,
            &rt,
            &server,
            "sum_without",
            &[
                (
                    "query",
                    "sum without (pod) (rate({service_name=\"api\", dataset=\"varied\"}[5m]))",
                ),
                ("step", "60s"),
            ],
        );

        drop(group);
    }

    // Cleanup
    rt.block_on(async {
        server.shutdown().await;
    });
}

criterion_group!(benches, loki_benchmarks);
criterion_main!(benches);
