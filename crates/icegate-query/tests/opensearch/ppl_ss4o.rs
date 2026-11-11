//! `/_plugins/_ppl` over the ss4o profile indices: each reads its table as a relation of the
//! profile's field names. Every row is written with explicit values and every expected answer is
//! stated here.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::schema::{
    COL_KIND, COL_LOG_ATTRIBUTES, COL_NAME, COL_PARENT_SPAN_ID, COL_RESOURCE_ATTRIBUTES, COL_SERVICE_NAME, COL_SPAN_ID,
    COL_STATUS_CODE, COL_TRACE_ID, COL_VALUE_DOUBLE, COL_VALUE_INT,
};
use icegate_common::{LOGS_TABLE, METRICS_TABLE, SPANS_TABLE};
use reqwest::StatusCode;
use serde_json::json;

use super::data::{Cell, SPAN_ALIAS, SPAN_INDEX, T0, build_span_row, commit_rows};
use super::harness::TestServer;
use super::ppl::{
    OTHER_TENANT, PplAnswer, TENANT, assert_ppl_error, assert_semantic_refusal_naming, post_ppl, read_column_names,
    read_rows, run_query,
};
use super::profiles::{build_log_row, build_metric_row};

const SS4O_LOGS: &str = "ss4o_logs-icegate-default";
const SS4O_TRACES: &str = "ss4o_traces-icegate-default";
const SS4O_METRICS: &str = "ss4o_metrics-icegate-default";

/// The query OSD 3.9's "View associated logs" sends for trace [`TRACE_HEX`].
const ASSOCIATED_LOGS_QUERY: &str = "source = ss4o_logs-*,opensearch_dashboards_sample_data_otel_logs* | where traceId='5b8efff798038103d269b633813fc60c'";

const TRACE_HEX: &str = "5b8efff798038103d269b633813fc60c";
const TRACE: [u8; 16] = [
    0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c,
];
const OTHER_TRACE: [u8; 16] = [0x0c; 16];
const CHECKOUT_SPAN: [u8; 8] = [0x01; 8];
const PAYMENT_SPAN: [u8; 8] = [0x02; 8];
const CART_SPAN: [u8; 8] = [0x03; 8];
const OTHER_TENANT_SPAN: [u8; 8] = [0x04; 8];

/// Four logs, `acme`'s three and `globex`'s one, in one commit. [`build_log_row`] observes each
/// 5 µs after its timestamp; `T0` is 2025-10-09T08:53:20Z.
///
/// | tenant | timestamp | trace / span | severity | body | service | log / resource attributes |
/// |---|---|---|---|---|---|---|
/// | acme | T0 | TRACE / 01… | INFO | checkout started | checkout | `http.method=GET` / `host.name=node-a` |
/// | acme | T0 + 1 s | TRACE / 02… | ERROR | payment declined | checkout | `http.method=POST` / — |
/// | acme | T0 + 120 s | 0c… / 03… | INFO | cart ready | cart | — / — |
/// | globex | T0 | TRACE / 04… | ERROR | globex secret | checkout | — / — |
async fn seed_trace_logs(catalog: &Arc<dyn Catalog>) {
    commit_rows(
        catalog,
        LOGS_TABLE,
        &[
            build_log_row(
                TENANT,
                T0,
                "checkout started",
                "INFO",
                vec![
                    (COL_TRACE_ID, Cell::Bytes(&TRACE)),
                    (COL_SPAN_ID, Cell::Bytes(&CHECKOUT_SPAN)),
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                    (COL_LOG_ATTRIBUTES, Cell::Map(&[("http.method", "GET")])),
                    (COL_RESOURCE_ATTRIBUTES, Cell::Map(&[("host.name", "node-a")])),
                ],
            ),
            build_log_row(
                TENANT,
                T0 + 1_000_000,
                "payment declined",
                "ERROR",
                vec![
                    (COL_TRACE_ID, Cell::Bytes(&TRACE)),
                    (COL_SPAN_ID, Cell::Bytes(&PAYMENT_SPAN)),
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                    (COL_LOG_ATTRIBUTES, Cell::Map(&[("http.method", "POST")])),
                ],
            ),
            build_log_row(
                TENANT,
                T0 + 120_000_000,
                "cart ready",
                "INFO",
                vec![
                    (COL_TRACE_ID, Cell::Bytes(&OTHER_TRACE)),
                    (COL_SPAN_ID, Cell::Bytes(&CART_SPAN)),
                    (COL_SERVICE_NAME, Cell::Text("cart")),
                ],
            ),
            build_log_row(
                OTHER_TENANT,
                T0,
                "globex secret",
                "ERROR",
                vec![
                    (COL_TRACE_ID, Cell::Bytes(&TRACE)),
                    (COL_SPAN_ID, Cell::Bytes(&OTHER_TENANT_SPAN)),
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                ],
            ),
        ],
    )
    .await
    .expect("commit logs");
}

/// The `(name, type)` pairs of an answer's `schema`, in order.
fn read_schema(answer: &PplAnswer) -> Vec<(String, String)> {
    answer.body["schema"]
        .as_array()
        .expect("schema")
        .iter()
        .map(|column| {
            (
                column["name"].as_str().expect("column name").to_string(),
                column["type"].as_str().expect("column type").to_string(),
            )
        })
        .collect()
}

/// `(name, type)` pairs from literals.
fn build_schema(columns: &[(&str, &str)]) -> Vec<(String, String)> {
    columns
        .iter()
        .map(|(name, column_type)| ((*name).to_string(), (*column_type).to_string()))
        .collect()
}

/// The columns of a bare `source=` over the ss4o logs index: the profile's top-level fields in
/// declared order, aliases included, objects as structs.
fn build_ss4o_logs_schema() -> Vec<(String, String)> {
    build_schema(&[
        ("@timestamp", "timestamp"),
        ("time", "timestamp"),
        ("observedTimestamp", "timestamp"),
        ("observerTime", "timestamp"),
        ("traceId", "string"),
        ("spanId", "string"),
        ("severity", "struct"),
        ("body", "string"),
        ("@message", "string"),
        ("serviceName", "string"),
        ("attributes", "struct"),
        ("resource", "struct"),
        ("instrumentationScope", "struct"),
    ])
}

/// OSD's associated-logs query reads the two logs of the trace: the wildcard part that matches
/// no index is skipped (see the PPL README, Sources), and the other tenant's log of the same trace
/// never appears. Every column holds the value `_source` shows.
#[tokio::test]
async fn osd_associated_logs_query_lists_the_logs_of_one_trace() {
    let (server, catalog) = TestServer::start().await;
    seed_trace_logs(&catalog).await;

    let answer = run_query(&server, ASSOCIATED_LOGS_QUERY).await;
    let sorted_answer = run_query(&server, &format!("{ASSOCIATED_LOGS_QUERY} | sort spanId")).await;

    assert_eq!(read_rows(&answer).as_array().map(Vec::len), Some(2), "{}", answer.body);
    assert_eq!(read_schema(&answer), build_ss4o_logs_schema());
    assert_eq!(
        read_rows(&sorted_answer),
        &json!([
            [
                "2025-10-09 08:53:20",
                "2025-10-09 08:53:20",
                "2025-10-09 08:53:20.000005",
                "2025-10-09 08:53:20.000005",
                TRACE_HEX,
                "0101010101010101",
                {"text": "INFO"},
                "checkout started",
                "checkout started",
                "checkout",
                {"http.method": "GET"},
                {"attributes": {"host.name": "node-a"}},
                {"attributes": {}}
            ],
            [
                "2025-10-09 08:53:21",
                "2025-10-09 08:53:21",
                "2025-10-09 08:53:21.000005",
                "2025-10-09 08:53:21.000005",
                TRACE_HEX,
                "0202020202020202",
                {"text": "ERROR"},
                "payment declined",
                "payment declined",
                "checkout",
                {"http.method": "POST"},
                {"attributes": {}},
                {"attributes": {}}
            ]
        ])
    );
    server.shutdown().await;
}

/// Dotted, alias and promoted profile names resolve in every command that takes a field.
#[tokio::test]
async fn ss4o_logs_dotted_and_alias_fields_resolve_in_every_command() {
    let (server, catalog) = TestServer::start().await;
    seed_trace_logs(&catalog).await;
    let cases = [
        (
            format!(
                "source={SS4O_LOGS} | where traceId='{TRACE_HEX}' | fields severity.text, @message, time | sort time"
            ),
            vec!["severity.text", "@message", "time"],
            json!([
                ["INFO", "checkout started", "2025-10-09 08:53:20"],
                ["ERROR", "payment declined", "2025-10-09 08:53:21"]
            ]),
        ),
        (
            format!("source={SS4O_LOGS} | stats count() by serviceName"),
            vec!["count()", "serviceName"],
            json!([[1, "cart"], [2, "checkout"]]),
        ),
        (
            format!("source={SS4O_LOGS} | where attributes.http.method='GET' | fields body"),
            vec!["body"],
            json!([["checkout started"]]),
        ),
        (
            format!("source={SS4O_LOGS} | sort - @timestamp | head 1 | fields body"),
            vec!["body"],
            json!([["cart ready"]]),
        ),
        (
            format!("source={SS4O_LOGS} | where severity.text='ERROR' | fields spanId, severity"),
            vec!["spanId", "severity"],
            json!([["0202020202020202", {"text": "ERROR"}]]),
        ),
        (
            format!(
                "source={SS4O_LOGS} | where resource.attributes.service.name='cart' | fields body, resource.attributes.host.name"
            ),
            vec!["body", "resource.attributes.host.name"],
            json!([["cart ready", null]]),
        ),
        (
            format!("source={SS4O_LOGS} | stats count() as logs by severity.text, observerTime | sort severity.text"),
            vec!["logs", "severity.text", "observerTime"],
            json!([
                [1, "ERROR", "2025-10-09 08:53:21.000005"],
                [1, "INFO", "2025-10-09 08:53:20.000005"],
                [1, "INFO", "2025-10-09 08:55:20.000005"]
            ]),
        ),
        (
            format!("source={SS4O_LOGS} | eval level = lower(severity.text) | where level = 'error' | fields @message"),
            vec!["@message"],
            json!([["payment declined"]]),
        ),
        (
            format!(
                "source={SS4O_LOGS} | where spanId='0101010101010101' | fields traceId, @message, attributes.http.method"
            ),
            vec!["traceId", "@message", "attributes.http.method"],
            json!([[TRACE_HEX, "checkout started", "GET"]]),
        ),
        (
            format!(
                "source={SS4O_LOGS} | where spanId='0101010101010101' | fields resource.attributes, instrumentationScope.attributes"
            ),
            vec!["resource.attributes", "instrumentationScope.attributes"],
            json!([[{"host.name": "node-a"}, {}]]),
        ),
        (
            format!("source={SS4O_LOGS} | fields - severity | where severity.text='ERROR' | fields body"),
            vec!["body"],
            json!([["payment declined"]]),
        ),
        (
            format!(
                "source={SS4O_LOGS} | eventstats count() as logs by serviceName | where severity.text='ERROR' | fields body, logs"
            ),
            vec!["body", "logs"],
            json!([["payment declined", 2]]),
        ),
    ];

    for (query, expected_names, expected_rows) in cases {
        let answer = run_query(&server, &query).await;

        assert_eq!(answer.status, StatusCode::OK, "{query}: {}", answer.body);
        assert_eq!(read_rows(&answer), &expected_rows, "{query}");
        assert_eq!(read_column_names(&answer), expected_names, "{query}");
    }
    server.shutdown().await;
}

/// The Logs explorer's histogram counts the logs per minute of `@timestamp`.
#[tokio::test]
async fn ss4o_logs_time_series_answers_like_the_logs_explorer_histogram() {
    let (server, catalog) = TestServer::start().await;
    seed_trace_logs(&catalog).await;

    let answer = run_query(
        &server,
        &format!("source={SS4O_LOGS} | stats count() by span(@timestamp, 1m)"),
    )
    .await;

    assert_eq!(
        read_rows(&answer),
        &json!([[2, "2025-10-09 08:53:00"], [1, "2025-10-09 08:55:00"]])
    );
    assert_eq!(read_column_names(&answer), vec!["count()", "span(@timestamp,1m)"]);
    server.shutdown().await;
}

/// A trace id that is no hex id is the refusal a malformed id literal gets over the native `logs`
/// index, never an empty answer.
#[tokio::test]
async fn a_malformed_trace_id_over_ss4o_logs_is_refused_like_over_logs() {
    let (server, catalog) = TestServer::start().await;
    seed_trace_logs(&catalog).await;

    let profile_answer = run_query(&server, "source=ss4o_logs-* | where traceId='xyz'").await;
    let native_answer = run_query(&server, "source=logs | where trace_id='xyz'").await;

    assert_semantic_refusal_naming(&profile_answer, "xyz");
    assert_semantic_refusal_naming(&native_answer, "xyz");
    server.shutdown().await;
}

/// `logs` and the ss4o logs index read one table under two field models, which do not union: a
/// source naming both is refused, naming the source as written.
#[tokio::test]
async fn a_source_reaching_logs_through_two_profiles_is_refused_naming_it() {
    let (server, catalog) = TestServer::start().await;
    seed_trace_logs(&catalog).await;

    let answer = run_query(&server, "source=logs,ss4o_logs-icegate-default").await;

    assert_semantic_refusal_naming(&answer, "logs,ss4o_logs-icegate-default");
    server.shutdown().await;
}

/// The Data Prepper span index stays refused under its concrete name and its alias: its
/// cross-row fields have no PPL projection.
#[tokio::test]
async fn other_profile_indices_stay_refused_as_ppl_sources() {
    let (server, catalog) = TestServer::start().await;
    seed_trace_logs(&catalog).await;

    for index in [SPAN_INDEX, SPAN_ALIAS] {
        let answer = run_query(&server, &format!("source={index} | head 1")).await;

        assert_ppl_error(
            &answer,
            StatusCode::BAD_REQUEST,
            "SemanticCheckException",
            "SEMANTIC_ERROR",
        );
    }
    server.shutdown().await;
}

/// The predicate `_explain` shows on the Iceberg scan of `query`, run as [`TENANT`].
async fn read_iceberg_scan_predicate(server: &TestServer, query: &str) -> String {
    let answer = post_ppl(server, TENANT, "/_plugins/_ppl/_explain", &json!({ "query": query })).await;
    assert_eq!(answer.status, StatusCode::OK, "{query}: {}", answer.body);
    let physical_plan = answer.body["calcite"]["physical"].as_str().expect("physical plan text");
    physical_plan
        .lines()
        .find(|line| line.contains("IcegateIcebergScan"))
        .and_then(|line| line.split_once("predicate:["))
        .map_or_else(
            || panic!("no Iceberg scan predicate for {query}:\n{physical_plan}"),
            |(_, predicate)| predicate.to_string(),
        )
}

/// A `traceId` filter and an `@timestamp` range over the ss4o logs index reach the Iceberg scan as
/// predicates on the stored `trace_id` and `timestamp` columns, so the scan prunes by them as on
/// `logs`.
#[tokio::test]
async fn trace_id_and_time_filters_over_ss4o_logs_reach_the_scan_as_stored_column_predicates() {
    let (server, catalog) = TestServer::start().await;
    seed_trace_logs(&catalog).await;
    let time_query = format!("source={SS4O_LOGS} | where @timestamp >= '2025-10-09 08:53:21' | fields body");

    let trace_predicate = read_iceberg_scan_predicate(&server, ASSOCIATED_LOGS_QUERY).await;
    let time_predicate = read_iceberg_scan_predicate(&server, &time_query).await;
    let time_answer = run_query(&server, &time_query).await;

    assert!(trace_predicate.contains("trace_id"), "{trace_predicate}");
    assert!(time_predicate.contains("timestamp"), "{time_predicate}");
    assert_eq!(read_rows(&time_answer), &json!([["payment declined"], ["cart ready"]]));
    server.shutdown().await;
}

/// The top-level fields `describe` lists, with their types: its rows without a dot.
async fn describe_top_level_fields(server: &TestServer, index: &str) -> Vec<(String, String)> {
    let answer = run_query(server, &format!("describe {index}")).await;
    read_rows(&answer)
        .as_array()
        .expect("describe rows")
        .iter()
        .map(|row| {
            (
                row[3].as_str().expect("COLUMN_NAME").to_string(),
                row[5].as_str().expect("TYPE_NAME").to_string(),
            )
        })
        .filter(|(name, _)| !name.contains('.'))
        .collect()
}

/// `describe` lists the same top-level fields, with the same types, as a bare source over the ss4o
/// logs index returns.
#[tokio::test]
async fn describe_and_a_bare_source_agree_on_the_ss4o_logs_columns() {
    let (server, catalog) = TestServer::start().await;
    seed_trace_logs(&catalog).await;

    let described_columns = describe_top_level_fields(&server, SS4O_LOGS).await;
    let source_answer = run_query(&server, &format!("source={SS4O_LOGS} | head 1")).await;

    assert_eq!(read_rows(&source_answer).as_array().map(Vec::len), Some(1));
    assert_eq!(described_columns, read_schema(&source_answer));
    assert_eq!(described_columns, build_ss4o_logs_schema());
    server.shutdown().await;
}

/// Over the ss4o traces and metrics indices, a bare source's columns are the top-level fields
/// `describe` lists, in its order and with its types, but for the fields PPL cannot read. A list
/// column (`bucketCountsList`) is an `array` in both.
#[tokio::test]
async fn describe_and_a_bare_source_agree_on_the_ss4o_traces_and_metrics_columns() {
    let (server, catalog) = TestServer::start().await;
    seed_spans(&catalog).await;
    commit_rows(
        &catalog,
        METRICS_TABLE,
        &[build_metric_row(
            TENANT,
            "cpu.usage",
            "gauge",
            vec![(COL_VALUE_DOUBLE, Cell::Double(0.5))],
        )],
    )
    .await
    .expect("commit metrics");
    let cases: [(&str, &[&str]); 2] = [
        (SS4O_TRACES, &["events", "links"]),
        (
            SS4O_METRICS,
            &["positiveBuckets", "negativeBuckets", "quantiles", "exemplar"],
        ),
    ];

    for (index, unreadable_fields) in cases {
        let described_columns: Vec<(String, String)> = describe_top_level_fields(&server, index)
            .await
            .into_iter()
            .filter(|(name, _)| !unreadable_fields.contains(&name.as_str()))
            .collect();
        let source_answer = run_query(&server, &format!("source={index} | head 1")).await;

        assert_eq!(read_rows(&source_answer).as_array().map(Vec::len), Some(1), "{index}");
        assert_eq!(read_schema(&source_answer), described_columns, "{index}");
    }
    let metrics_columns = describe_top_level_fields(&server, SS4O_METRICS).await;
    assert!(
        metrics_columns.contains(&("bucketCountsList".to_string(), "array".to_string())),
        "{metrics_columns:?}"
    );
    server.shutdown().await;
}

/// Spans of two `acme` traces and a `globex` span of the first one, durations in microseconds:
///
/// | tenant | trace / span | parent | kind | service | duration | status code |
/// |---|---|---|---|---|---|---|
/// | acme | TRACE / 01… | NULL | 2 (server) | frontend | 90 ms | 2 |
/// | acme | TRACE / 02… | 01… | 3 (client) | checkout | 50 ms | NULL |
/// | acme | 0c… / 03… | NULL | 1 (internal) | cart | 20 ms | 0 |
/// | globex | TRACE / 04… | NULL | 2 (server) | frontend | 90 ms | NULL |
async fn seed_spans(catalog: &Arc<dyn Catalog>) {
    commit_rows(
        catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT,
                &TRACE,
                &CHECKOUT_SPAN,
                (T0, 90_000),
                vec![
                    (COL_NAME, Cell::Text("GET /checkout")),
                    (COL_KIND, Cell::Int(2)),
                    (COL_SERVICE_NAME, Cell::Text("frontend")),
                    (COL_STATUS_CODE, Cell::Int(2)),
                ],
            ),
            build_span_row(
                TENANT,
                &TRACE,
                &PAYMENT_SPAN,
                (T0 + 10_000, 50_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&CHECKOUT_SPAN)),
                    (COL_NAME, Cell::Text("charge")),
                    (COL_KIND, Cell::Int(3)),
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                ],
            ),
            build_span_row(
                TENANT,
                &OTHER_TRACE,
                &CART_SPAN,
                (T0, 20_000),
                vec![
                    (COL_NAME, Cell::Text("load cart")),
                    (COL_KIND, Cell::Int(1)),
                    (COL_SERVICE_NAME, Cell::Text("cart")),
                    (COL_STATUS_CODE, Cell::Int(0)),
                ],
            ),
            build_span_row(
                OTHER_TENANT,
                &TRACE,
                &OTHER_TENANT_SPAN,
                (T0, 90_000),
                vec![
                    (COL_NAME, Cell::Text("globex secret")),
                    (COL_KIND, Cell::Int(2)),
                    (COL_SERVICE_NAME, Cell::Text("frontend")),
                ],
            ),
        ],
    )
    .await
    .expect("commit spans");
}

/// The ss4o traces index reads spans under the profile's names: hex ids, a root's parent as
/// `""`, the kind as its `SPAN_KIND_*` name, the duration in nanoseconds. Another tenant's span
/// of the same trace never appears, in rows or in counts.
#[tokio::test]
async fn ss4o_traces_reads_spans_with_profile_names() {
    let (server, catalog) = TestServer::start().await;
    seed_spans(&catalog).await;

    let trace_answer = run_query(
        &server,
        &format!(
            "source={SS4O_TRACES} | where traceId='{TRACE_HEX}' | fields spanId, parentSpanId, kind, serviceName, durationInNanos | sort spanId"
        ),
    )
    .await;
    let service_answer = run_query(&server, &format!("source={SS4O_TRACES} | stats count() by serviceName")).await;

    assert_eq!(
        read_rows(&trace_answer),
        &json!([
            ["0101010101010101", "", "SPAN_KIND_SERVER", "frontend", 90_000_000],
            [
                "0202020202020202",
                "0101010101010101",
                "SPAN_KIND_CLIENT",
                "checkout",
                50_000_000
            ]
        ])
    );
    assert_eq!(
        read_rows(&service_answer),
        &json!([[1, "cart"], [1, "checkout"], [1, "frontend"]])
    );
    server.shutdown().await;
}

/// The ss4o metrics index reads metrics under the profile's names: `kind` is the metric type's
/// name and `value` the point's value, whichever column stores it.
#[tokio::test]
async fn ss4o_metrics_reads_metrics_with_profile_names() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        METRICS_TABLE,
        &[
            build_metric_row(
                TENANT,
                "cpu.usage",
                "gauge",
                vec![(COL_VALUE_DOUBLE, Cell::Double(0.5))],
            ),
            build_metric_row(TENANT, "requests", "sum", vec![(COL_VALUE_INT, Cell::Long(7))]),
            build_metric_row(
                OTHER_TENANT,
                "cpu.usage",
                "gauge",
                vec![(COL_VALUE_DOUBLE, Cell::Double(0.9))],
            ),
        ],
    )
    .await
    .expect("commit metrics");
    let cases = [
        (
            format!("source={SS4O_METRICS} | where name='cpu.usage' | fields name, kind, value"),
            json!([["cpu.usage", "GAUGE", 0.5]]),
        ),
        (
            format!("source={SS4O_METRICS} | where name='requests' | fields kind, value"),
            json!([["SUM", 7.0]]),
        ),
    ];

    for (query, expected_rows) in cases {
        let answer = run_query(&server, &query).await;

        assert_eq!(read_rows(&answer), &expected_rows, "{query}");
    }
    server.shutdown().await;
}

/// A field `_source` reshapes from a whole list of the row (span events and links; the bucket,
/// quantile and exemplar fields) has no PPL value: naming it, or a path below it, is a 400 naming
/// it, and a bare source omits it.
#[tokio::test]
async fn a_field_source_reshapes_from_a_list_is_refused_naming_it() {
    let (server, catalog) = TestServer::start().await;
    seed_spans(&catalog).await;
    commit_rows(
        &catalog,
        METRICS_TABLE,
        &[build_metric_row(
            TENANT,
            "cpu.usage",
            "gauge",
            vec![(COL_VALUE_DOUBLE, Cell::Double(0.5))],
        )],
    )
    .await
    .expect("commit metrics");
    let metric_list_fields: &[&str] = &["positiveBuckets", "negativeBuckets", "quantiles", "exemplar"];
    let span_list_fields: &[&str] = &["events", "links"];
    let cases = [
        (SS4O_METRICS, "quantiles", metric_list_fields),
        (SS4O_METRICS, "exemplar", metric_list_fields),
        (SS4O_METRICS, "positiveBuckets.count", metric_list_fields),
        (SS4O_TRACES, "events", span_list_fields),
        (SS4O_TRACES, "links", span_list_fields),
    ];

    for (index, field, omitted_fields) in cases {
        let answer = run_query(&server, &format!("source={index} | fields serviceName, {field}")).await;
        let bare_answer = run_query(&server, &format!("source={index} | head 1")).await;

        assert_semantic_refusal_naming(&answer, field);
        assert_eq!(read_rows(&bare_answer).as_array().map(Vec::len), Some(1));
        let names = read_column_names(&bare_answer);
        assert!(
            omitted_fields.iter().all(|omitted| !names.iter().any(|name| name == omitted)),
            "{index}: {names:?}"
        );
    }
    server.shutdown().await;
}

/// A profile field one source index declares and another lacks is refused naming it: a union
/// would read the lacking index's rows through an expression made for the other.
#[tokio::test]
async fn a_profile_field_some_source_indices_lack_is_refused_naming_it() {
    let (server, catalog) = TestServer::start().await;
    seed_trace_logs(&catalog).await;
    seed_spans(&catalog).await;

    let refused_answer = run_query(
        &server,
        "source=ss4o_logs-*,ss4o_traces-* | where severity.text='INFO' | fields serviceName",
    )
    .await;
    let shared_answer = run_query(
        &server,
        "source=ss4o_logs-*,ss4o_traces-* | stats count() by serviceName",
    )
    .await;

    assert_semantic_refusal_naming(&refused_answer, "severity.text");
    assert_eq!(
        read_rows(&shared_answer),
        &json!([[2, "cart"], [3, "checkout"], [1, "frontend"]])
    );
    server.shutdown().await;
}

/// Filters on the encoded and derived traces fields lower through the profile's inverses and keep
/// the rows the fields' values select: the kind by name, the status code with NULL as 0, the
/// duration in nanoseconds, the parent id with `""` as a root's. `!=` and `not` read the values
/// themselves. The other tenant's server span never appears.
#[tokio::test]
async fn ss4o_traces_filters_on_encoded_and_derived_fields_select_the_rows_their_values_do() {
    let (server, catalog) = TestServer::start().await;
    seed_spans(&catalog).await;
    let cases = [
        ("kind = 'SPAN_KIND_SERVER'", json!([["0101010101010101"]])),
        (
            "kind in ('SPAN_KIND_CLIENT', 'SPAN_KIND_INTERNAL')",
            json!([["0202020202020202"], ["0303030303030303"]]),
        ),
        ("'SPAN_KIND_SERVER' = kind", json!([["0101010101010101"]])),
        (
            "not kind = 'SPAN_KIND_SERVER'",
            json!([["0202020202020202"], ["0303030303030303"]]),
        ),
        ("status.code = 0", json!([["0202020202020202"], ["0303030303030303"]])),
        ("status.code > 0", json!([["0101010101010101"]])),
        (
            "durationInNanos >= 50000000",
            json!([["0101010101010101"], ["0202020202020202"]]),
        ),
        (
            "durationInNanos between 20000000 and 50000000",
            json!([["0202020202020202"], ["0303030303030303"]]),
        ),
        ("parentSpanId = ''", json!([["0101010101010101"], ["0303030303030303"]])),
        ("parentSpanId = '0101010101010101'", json!([["0202020202020202"]])),
        ("parentSpanId != ''", json!([["0202020202020202"]])),
        (
            "kind = 'SPAN_KIND_SERVER' or durationInNanos < 30000000",
            json!([["0101010101010101"], ["0303030303030303"]]),
        ),
        (
            "traceState is not null",
            json!([["0101010101010101"], ["0202020202020202"], ["0303030303030303"]]),
        ),
    ];

    for (condition, expected_rows) in cases {
        let query = format!("source={SS4O_TRACES} | where {condition} | fields spanId | sort spanId");
        let answer = run_query(&server, &query).await;

        assert_eq!(answer.status, StatusCode::OK, "{query}: {}", answer.body);
        assert_eq!(read_rows(&answer), &expected_rows, "{query}");
    }
    server.shutdown().await;
}

/// Filters on the metrics `kind` and `value` lower through the profile's inverses and keep the
/// rows the values select.
#[tokio::test]
async fn ss4o_metrics_filters_on_kind_and_value_select_the_rows_their_values_do() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        METRICS_TABLE,
        &[
            build_metric_row(
                TENANT,
                "cpu.usage",
                "gauge",
                vec![(COL_VALUE_DOUBLE, Cell::Double(0.5))],
            ),
            build_metric_row(TENANT, "requests", "sum", vec![(COL_VALUE_INT, Cell::Long(7))]),
            build_metric_row(OTHER_TENANT, "requests", "sum", vec![(COL_VALUE_INT, Cell::Long(9))]),
        ],
    )
    .await
    .expect("commit metrics");
    let cases = [
        ("kind = 'SUM'", json!([["requests"]])),
        ("kind > 'GAUGE'", json!([["requests"]])),
        ("value >= 1", json!([["requests"]])),
        ("value = 0.5", json!([["cpu.usage"]])),
    ];

    for (condition, expected_rows) in cases {
        let query = format!("source={SS4O_METRICS} | where {condition} | fields name | sort name");
        let answer = run_query(&server, &query).await;

        assert_eq!(answer.status, StatusCode::OK, "{query}: {}", answer.body);
        assert_eq!(read_rows(&answer), &expected_rows, "{query}");
    }
    server.shutdown().await;
}

/// A filter on the span kind and one on the parent id reach the Iceberg scan as predicates on the
/// stored `kind` and `parent_span_id` columns.
#[tokio::test]
async fn ss4o_traces_kind_and_parent_filters_reach_the_scan_as_stored_column_predicates() {
    let (server, catalog) = TestServer::start().await;
    seed_spans(&catalog).await;

    let kind_predicate = read_iceberg_scan_predicate(
        &server,
        &format!("source={SS4O_TRACES} | where kind = 'SPAN_KIND_SERVER'"),
    )
    .await;
    let parent_predicate = read_iceberg_scan_predicate(
        &server,
        &format!("source={SS4O_TRACES} | where parentSpanId = '0101010101010101'"),
    )
    .await;

    assert!(kind_predicate.contains("kind"), "{kind_predicate}");
    assert!(parent_predicate.contains("parent_span_id"), "{parent_predicate}");
    server.shutdown().await;
}

/// A parent id that is no hex id is the refusal a malformed id literal gets over the native `spans`
/// index, never an empty answer.
#[tokio::test]
async fn a_malformed_parent_span_id_over_ss4o_traces_is_refused_like_over_spans() {
    let (server, catalog) = TestServer::start().await;
    seed_spans(&catalog).await;

    let profile_answer = run_query(&server, &format!("source={SS4O_TRACES} | where parentSpanId='xyz'")).await;
    let native_answer = run_query(&server, "source=spans | where parent_span_id='xyz'").await;

    assert_semantic_refusal_naming(&profile_answer, "xyz");
    assert_semantic_refusal_naming(&native_answer, "xyz");
    server.shutdown().await;
}

/// A wildcard reaching `logs` through both its native index and the ss4o logs index is refused
/// naming the source as written, like the two names written out.
#[tokio::test]
async fn a_wildcard_reaching_logs_through_two_profiles_is_refused_naming_it() {
    let (server, catalog) = TestServer::start().await;
    seed_trace_logs(&catalog).await;

    let answer = run_query(&server, "source=*logs*").await;

    assert_semantic_refusal_naming(&answer, "*logs*");
    server.shutdown().await;
}

/// `describe` of a native index types a list column `array`, the type a bare source reads it as.
#[tokio::test]
async fn describe_types_a_native_list_column_array_as_a_bare_source_reads_it() {
    let (server, catalog) = TestServer::start().await;
    seed_spans(&catalog).await;

    let described_columns = describe_top_level_fields(&server, SPANS_TABLE).await;
    let source_answer = run_query(&server, "source=spans | head 1").await;

    let events_column = ("events".to_string(), "array".to_string());
    assert!(described_columns.contains(&events_column), "{described_columns:?}");
    assert!(
        read_schema(&source_answer).contains(&events_column),
        "{}",
        source_answer.body
    );
    server.shutdown().await;
}

const LETTER_PARENT: [u8; 8] = [0xab; 8];
const OTHER_LETTER_PARENT: [u8; 8] = [0xcd; 8];

/// Three `acme` spans whose parents are written with hex letters: `0a…` under `abab…`, the root
/// `0b…`, and `0c…` under `cdcd…`.
async fn seed_letter_parent_spans(catalog: &Arc<dyn Catalog>) {
    commit_rows(
        catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT,
                &TRACE,
                &[0x0a; 8],
                (T0, 10_000),
                vec![
                    (COL_NAME, Cell::Text("a")),
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&LETTER_PARENT)),
                ],
            ),
            build_span_row(
                TENANT,
                &TRACE,
                &[0x0b; 8],
                (T0, 10_000),
                vec![(COL_NAME, Cell::Text("b"))],
            ),
            build_span_row(
                TENANT,
                &TRACE,
                &[0x0c; 8],
                (T0, 10_000),
                vec![
                    (COL_NAME, Cell::Text("c")),
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&OTHER_LETTER_PARENT)),
                ],
            ),
        ],
    )
    .await
    .expect("commit spans");
}

/// A string literal compared with `parentSpanId` is an id literal wherever the comparison stands:
/// either case selects the same parent, and a test and its negation (`!=`, `not`, `not in`) keep
/// complementary rows; a root's parent reads `""`, so a negation keeps it.
#[tokio::test]
async fn a_parent_span_id_literal_in_either_case_selects_like_a_native_id_literal() {
    let (server, catalog) = TestServer::start().await;
    seed_letter_parent_spans(&catalog).await;
    let cases = [
        ("where parentSpanId = 'ABABABABABABABAB'", json!([["0a0a0a0a0a0a0a0a"]])),
        ("where parentSpanId = 'abababababababab'", json!([["0a0a0a0a0a0a0a0a"]])),
        (
            "where parentSpanId != 'ABABABABABABABAB'",
            json!([["0b0b0b0b0b0b0b0b"], ["0c0c0c0c0c0c0c0c"]]),
        ),
        (
            "where not parentSpanId = 'ABABABABABABABAB'",
            json!([["0b0b0b0b0b0b0b0b"], ["0c0c0c0c0c0c0c0c"]]),
        ),
        (
            "where parentSpanId not in ('ABABABABABABABAB')",
            json!([["0b0b0b0b0b0b0b0b"], ["0c0c0c0c0c0c0c0c"]]),
        ),
        (
            "where parentSpanId between 'ABABABABABABABAB' and 'CDCDCDCDCDCDCDCD'",
            json!([["0a0a0a0a0a0a0a0a"], ["0c0c0c0c0c0c0c0c"]]),
        ),
        (
            "where not parentSpanId between 'ABABABABABABABAB' and 'CDCDCDCDCDCDCDCD'",
            json!([["0b0b0b0b0b0b0b0b"]]),
        ),
    ];
    let native_answer = run_query(
        &server,
        "source=spans | where parent_span_id != 'ABABABABABABABAB' | fields span_id | sort span_id",
    )
    .await;

    for (condition, expected_rows) in cases {
        let query = format!("source={SS4O_TRACES} | {condition} | fields spanId | sort spanId");
        let answer = run_query(&server, &query).await;

        assert_eq!(answer.status, StatusCode::OK, "{query}: {}", answer.body);
        assert_eq!(read_rows(&answer), &expected_rows, "{query}");
    }
    // The native column holds the root's parent as NULL, which `!=` drops; the profile reads it as
    // `""`, which differs from every id. Both exclude the `abab…` child.
    assert_eq!(read_rows(&native_answer), &json!([["0c0c0c0c0c0c0c0c"]]));
    let eval_answer = run_query(
        &server,
        &format!(
            "source={SS4O_TRACES} | eval child = case(parentSpanId = 'ABABABABABABABAB', 'yes' else 'no') | fields spanId, child | sort spanId"
        ),
    )
    .await;
    assert_eq!(
        read_rows(&eval_answer),
        &json!([
            ["0a0a0a0a0a0a0a0a", "yes"],
            ["0b0b0b0b0b0b0b0b", "no"],
            ["0c0c0c0c0c0c0c0c", "no"]
        ])
    );
    server.shutdown().await;
}

/// A malformed parent id is the native malformed-id refusal wherever the comparison stands: under
/// `not`, in `not in`, and in an `eval`.
#[tokio::test]
async fn a_malformed_parent_span_id_is_refused_in_every_comparison_like_over_spans() {
    let (server, catalog) = TestServer::start().await;
    seed_letter_parent_spans(&catalog).await;
    let cases = [
        ("where not parentSpanId = 'xyz'", "where not parent_span_id = 'xyz'"),
        (
            "where parentSpanId not in ('xyz')",
            "where parent_span_id not in ('xyz')",
        ),
        (
            "eval child = case(parentSpanId = 'xyz', 'yes' else 'no')",
            "eval child = case(parent_span_id = 'xyz', 'yes' else 'no')",
        ),
    ];

    for (profile_command, native_command) in cases {
        let profile_answer = run_query(&server, &format!("source={SS4O_TRACES} | {profile_command}")).await;
        let native_answer = run_query(&server, &format!("source=spans | {native_command}")).await;

        assert_semantic_refusal_naming(&profile_answer, "xyz");
        assert_semantic_refusal_naming(&native_answer, "xyz");
    }
    server.shutdown().await;
}
