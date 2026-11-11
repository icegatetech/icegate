//! The Data Prepper and ss4o profiles over real HTTP, and the derived metrics `value` of the
//! native `metrics` index. Every row is written with explicit values and every expected document,
//! field list and row set is stated here.
#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::schema::{
    COL_AGGREGATION_TEMPORALITY, COL_ATTRIBUTES, COL_BODY, COL_BUCKET_COUNTS, COL_DROPPED_ATTRIBUTES_COUNT, COL_EVENTS,
    COL_EXEMPLARS, COL_EXPLICIT_BOUNDS, COL_INGESTED_TIMESTAMP, COL_KIND, COL_LOG_ATTRIBUTES, COL_METRIC_NAME,
    COL_METRIC_TYPE, COL_NAME, COL_NEGATIVE_BUCKET_COUNTS, COL_NEGATIVE_OFFSET, COL_OBSERVED_TIMESTAMP,
    COL_PARENT_SPAN_ID, COL_POSITIVE_BUCKET_COUNTS, COL_POSITIVE_OFFSET, COL_QUANTILE_VALUES, COL_RESOURCE_ATTRIBUTES,
    COL_SCALE, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ATTRIBUTES, COL_SPAN_ID, COL_STATUS_CODE, COL_TIMESTAMP,
    COL_TRACE_ID, COL_TRACE_STATE, COL_VALUE_DOUBLE, COL_VALUE_INT, QUANTILE_FIELD_QUANTILE, QUANTILE_FIELD_VALUE,
};
use icegate_common::{LOGS_TABLE, METRICS_TABLE, SPANS_TABLE};
use icegate_query::opensearch::OpenSearchConfig;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::{
    Cell, Row, RowWriteError, SPAN_ALIAS, SPAN_INDEX, T0, TENANT_A, TENANT_B, build_span_row, commit_rows,
    write_wal_segment,
};
use super::harness::TestServer;
use super::requests::{send_ndjson, send_request};

const SPAN_SEARCH_PATH: &str = "/otel-v1-apm-span/_search";

const TRACE_A: [u8; 16] = [0x0a; 16];
const TRACE_B: [u8; 16] = [0x0b; 16];
const ROOT_SPAN: [u8; 8] = [0x01; 8];
const CHILD_SPAN: [u8; 8] = [0x02; 8];
const OTHER_SPAN: [u8; 8] = [0x03; 8];
const TENANT_B_SPAN: [u8; 8] = [0x04; 8];

/// The ids above as lowercase hex, written out for the expected values.
const TRACE_A_HEX: &str = "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a";
const TRACE_B_HEX: &str = "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b";
const ROOT_SPAN_HEX: &str = "0101010101010101";
const CHILD_SPAN_HEX: &str = "0202020202020202";
const OTHER_SPAN_HEX: &str = "0303030303030303";

/// The `_source.spanId` of every hit, in hit order.
fn read_span_ids(answer: &Value) -> Result<Vec<String>, Box<dyn Error>> {
    answer["hits"]["hits"]
        .as_array()
        .ok_or_else(|| format!("no hits in {answer}"))?
        .iter()
        .map(|hit| {
            hit["_source"]["spanId"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("no spanId in {hit}").into())
        })
        .collect()
}

/// The `_source.spanId` of every hit, sorted: the order of an unsorted search is no contract.
fn read_sorted_span_ids(answer: &Value) -> Result<Vec<String>, Box<dyn Error>> {
    let mut span_ids = read_span_ids(answer)?;
    span_ids.sort();
    Ok(span_ids)
}

/// `POST path` with `body` as `tenant`: the JSON answer of a `200`, and an error naming the
/// status and answer of anything else.
async fn fetch_ok_answer(server: &TestServer, tenant: &str, path: &str, body: &Value) -> Result<Value, Box<dyn Error>> {
    let (status, answer) = send_request(server, Method::POST, tenant, path, Some(body)).await?;
    if status != StatusCode::OK {
        return Err(format!("{path} as {tenant} answered {status}: {answer}").into());
    }
    Ok(answer)
}

/// Search the span index as tenant A and return the sorted span ids of the hits.
async fn search_span_ids(server: &TestServer, query: Value) -> Result<Vec<String>, Box<dyn Error>> {
    let answer = fetch_ok_answer(server, TENANT_A, SPAN_SEARCH_PATH, &json!({"query": query})).await?;
    read_sorted_span_ids(&answer)
}

/// The `error` object of a refused span search as tenant A, with its status.
async fn fetch_search_error(server: &TestServer, body: &Value) -> Result<(StatusCode, Value), Box<dyn Error>> {
    let (status, answer) = send_request(server, Method::POST, TENANT_A, SPAN_SEARCH_PATH, Some(body)).await?;
    Ok((status, answer["error"].clone()))
}

/// Tenant A: trace A's root (`GET /checkout`, server, error, 152 175 µs) and its client child
/// (status NULL, 50 000 µs), and trace B's lone span (kind NULL, 500 µs). Tenant B: a server
/// root in trace A, in the same commit and therefore beside tenant A's rows.
async fn commit_three_spans(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    commit_rows(
        catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &ROOT_SPAN,
                (T0, 152_175),
                vec![
                    (COL_NAME, Cell::Text("GET /checkout")),
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                    (COL_KIND, Cell::Int(2)),
                    (COL_STATUS_CODE, Cell::Int(2)),
                ],
            ),
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &CHILD_SPAN,
                (T0 + 1_000, 50_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("SELECT orders")),
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                    (COL_KIND, Cell::Int(3)),
                ],
            ),
            build_span_row(
                TENANT_A,
                &TRACE_B,
                &OTHER_SPAN,
                (T0 + 2_000, 500),
                vec![(COL_NAME, Cell::Text("cron"))],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_A,
                &TENANT_B_SPAN,
                (T0, 900),
                vec![
                    (COL_NAME, Cell::Text("tenant-b-root")),
                    (COL_KIND, Cell::Int(2)),
                    (COL_STATUS_CODE, Cell::Int(2)),
                ],
            ),
        ],
    )
    .await
}

/// A wildcard reaching one table through two or more profiles without naming either index is
/// refused: the refusal names the expression, the first such table in name order and its profiles
/// in index-name order. `*` reaches `logs` through the native and ss4o indices first; the other
/// expressions reach `spans`. The check runs before any row is read.
#[tokio::test]
async fn a_wildcard_reaching_one_table_through_two_profiles_is_a_400_naming_them() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    for (path, table, profiles) in [
        ("/*/_search", "logs", json!(["native", "ss4o"])),
        ("/sp*,otel*/_search", "spans", json!(["data_prepper", "native"])),
        ("/spans,otel*/_search", "spans", json!(["data_prepper", "native"])),
        ("/otel*,s*/_search", "spans", json!(["data_prepper", "native", "ss4o"])),
    ] {
        let (status, answer) = send_request(
            &server,
            Method::POST,
            TENANT_A,
            path,
            Some(&json!({"query": {"match_all": {}}})),
        )
        .await
        .expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {answer}");
        let expression = path.trim_start_matches('/').trim_end_matches("/_search");
        for cause in [&answer["error"], &answer["error"]["root_cause"][0]] {
            assert_eq!(cause["type"], "illegal_argument_exception", "{path}");
            assert_eq!(cause["index"], expression, "{path}");
            assert_eq!(cause["table"], table, "{path}");
            assert_eq!(cause["profiles"], profiles, "{path}");
        }
    }
    server.shutdown().await;
}

/// One profile per table resolves, and naming both indices of one table reads it once per index.
/// Tenant B's span would add one hit to each total.
#[tokio::test]
async fn logs_and_spans_named_together_resolve() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");
    let match_all = json!({"query": {"match_all": {}}});

    let logs_and_spans = fetch_ok_answer(&server, TENANT_A, "/logs,spans/_search", &match_all)
        .await
        .expect("logs and spans");
    let both_span_indices = fetch_ok_answer(&server, TENANT_A, "/spans,otel-v1-apm-span/_search", &match_all)
        .await
        .expect("both span indices");

    assert_eq!(logs_and_spans["hits"]["total"], json!({"value": 3, "relation": "eq"}));
    assert_eq!(
        both_span_indices["hits"]["total"],
        json!({"value": 6, "relation": "eq"})
    );
    server.shutdown().await;
}

/// OSD's Trace Analytics span pattern reaches the span index by its concrete name, and every hit
/// names that index. Tenant B's span would add one hit to the total.
#[tokio::test]
async fn the_osd_span_pattern_selects_the_span_index_by_its_concrete_name() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/otel-v1-apm-span-*,opensearch_dashboards_sample_data_otel_spans*/_search",
        Some(&json!({"size": 1, "track_total_hits": true})),
    )
    .await
    .expect("search request");

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["hits"]["total"]["value"], json!(3), "{body}");
    assert_eq!(body["hits"]["hits"][0]["_index"], json!(SPAN_INDEX), "{body}");
    server.shutdown().await;
}

/// An alias is a second name of the span index, not a second index: an expression reaching the
/// index through its name and its alias reads each span once, and is no two-profiles 400.
#[tokio::test]
async fn span_wildcards_over_index_and_alias_read_each_span_once() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    for expression in ["otel*", "otel-v1-apm-span*", "otel-v1-apm-span,otel-v1-apm-span-000001"] {
        let (status, body) = send_request(
            &server,
            Method::POST,
            TENANT_A,
            &format!("/{expression}/_search"),
            Some(&json!({"size": 0, "track_total_hits": true})),
        )
        .await
        .expect("search request");

        assert_eq!(status, StatusCode::OK, "{expression}: {body}");
        assert_eq!(body["hits"]["total"]["value"], json!(3), "{expression}");
    }
    server.shutdown().await;
}

/// A request naming the alias answers under the concrete index wherever a response names an
/// index. `_mapping/field` is the exception: it stays keyed by the expression as requested, the
/// key a datasource health check reads back.
#[tokio::test]
async fn a_request_to_the_alias_answers_under_the_concrete_index() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let (caps_status, capabilities) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/otel-v1-apm-span/_field_caps?fields=traceId",
        None,
    )
    .await
    .expect("field caps");
    let (mapping_status, mapping) = send_request(&server, Method::GET, TENANT_A, "/otel-v1-apm-span/_mapping", None)
        .await
        .expect("mapping");
    let (field_status, field_mapping) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/otel-v1-apm-span/_mapping/field/traceId",
        None,
    )
    .await
    .expect("field mapping");
    let (search_status, refusal) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/otel-v1-apm-span/_search",
        Some(&json!({"query": {"term": {"status.code": "x"}}})),
    )
    .await
    .expect("search request");

    assert_eq!(caps_status, StatusCode::OK, "{capabilities}");
    assert_eq!(capabilities["indices"], json!([SPAN_INDEX]));
    assert_eq!(mapping_status, StatusCode::OK, "{mapping}");
    assert_eq!(
        mapping.as_object().map(|indices| indices.keys().cloned().collect::<Vec<_>>()),
        Some(vec![SPAN_INDEX.to_string()])
    );
    assert_eq!(field_status, StatusCode::OK, "{field_mapping}");
    assert_eq!(
        field_mapping
            .as_object()
            .map(|indices| indices.keys().cloned().collect::<Vec<_>>()),
        Some(vec![SPAN_ALIAS.to_string()])
    );
    assert_eq!(search_status, StatusCode::BAD_REQUEST, "{refusal}");
    assert_eq!(refusal["error"]["root_cause"][0]["type"], "query_shard_exception");
    assert_eq!(refusal["error"]["root_cause"][0]["field"], "status.code");
    assert_eq!(refusal["error"]["root_cause"][0]["index"], SPAN_INDEX);
    server.shutdown().await;
}

/// `kind` is an integer in `spans` and a keyword of span-kind names in `otel-v1-apm-span`, so a
/// `terms` over both is the type-conflict refusal, never one bucket list mixing both kinds of key.
#[tokio::test]
async fn spans_and_otel_v1_apm_span_terms_on_kind_is_a_400_never_mixed_buckets() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/spans,otel-v1-apm-span/_search",
        Some(&json!({"size": 0, "aggs": {"kinds": {"terms": {"field": "kind"}}}})),
    )
    .await
    .expect("search request");

    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert_eq!(
        answer["error"]["root_cause"][0]["type"],
        "aggregation_execution_exception"
    );
    assert_eq!(answer["error"]["root_cause"][0]["field"], "kind");
    assert!(answer.get("aggregations").is_none(), "{answer}");
    server.shutdown().await;
}

/// `parentSpanId: ""` selects the spans without a parent; a hex parent selects its children.
#[tokio::test]
async fn a_parent_span_id_term_of_empty_selects_roots() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let roots = search_span_ids(&server, json!({"term": {"parentSpanId": ""}}))
        .await
        .expect("roots");
    let children = search_span_ids(&server, json!({"term": {"parentSpanId": ROOT_SPAN_HEX}}))
        .await
        .expect("children");
    let non_root_span_ids = search_span_ids(&server, json!({"bool": {"must_not": {"term": {"parentSpanId": ""}}}}))
        .await
        .expect("non-roots");

    assert_eq!(roots, [ROOT_SPAN_HEX, OTHER_SPAN_HEX]);
    assert_eq!(children, [CHILD_SPAN_HEX]);
    assert_eq!(non_root_span_ids, [CHILD_SPAN_HEX]);
    server.shutdown().await;
}

/// A span-kind name filters the stored integer: kind 0 also selects the NULL kind it is stored
/// as, and an unknown name matches nothing.
#[tokio::test]
async fn a_kind_term_by_name_filters_the_stored_integer() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let cases = [
        (json!({"term": {"kind": "SPAN_KIND_SERVER"}}), vec![ROOT_SPAN_HEX]),
        (json!({"term": {"kind": "SPAN_KIND_UNSPECIFIED"}}), vec![OTHER_SPAN_HEX]),
        (json!({"term": {"kind": "SPAN_KIND_BOGUS"}}), vec![]),
        (
            json!({"terms": {"kind": ["SPAN_KIND_CLIENT", "SPAN_KIND_SERVER"]}}),
            vec![ROOT_SPAN_HEX, CHILD_SPAN_HEX],
        ),
    ];
    for (query, expected_span_ids) in cases {
        let span_ids = search_span_ids(&server, query.clone()).await.expect("search");

        assert_eq!(span_ids, expected_span_ids, "{query}");
    }
    let answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"query": {"term": {"spanId": OTHER_SPAN_HEX}}}),
    )
    .await
    .expect("the NULL-kind span");
    assert_eq!(answer["hits"]["hits"][0]["_source"]["kind"], "SPAN_KIND_UNSPECIFIED");
    server.shutdown().await;
}

/// A NULL status renders as code `0`, so `status.code: 0` selects it, and a range around `0`
/// keeps it.
#[tokio::test]
async fn status_code_zero_selects_null_and_null_renders_zero() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"query": {"term": {"status.code": 0}}}),
    )
    .await
    .expect("status code 0");
    let errors = search_span_ids(&server, json!({"term": {"status.code": "2"}}))
        .await
        .expect("status code 2");
    let below_one_span_ids = search_span_ids(&server, json!({"range": {"status.code": {"lt": 1}}}))
        .await
        .expect("status code below 1");

    assert_eq!(
        read_sorted_span_ids(&answer).expect("span ids"),
        [CHILD_SPAN_HEX, OTHER_SPAN_HEX]
    );
    for hit in answer["hits"]["hits"].as_array().expect("hits") {
        assert_eq!(hit["_source"]["status"], json!({"code": 0}), "{hit}");
    }
    assert_eq!(errors, [ROOT_SPAN_HEX]);
    assert_eq!(below_one_span_ids, [CHILD_SPAN_HEX, OTHER_SPAN_HEX]);
    server.shutdown().await;
}

/// Nanosecond bounds rewrite to microsecond bounds: `gte 5e7, lt 152 175 000` admits
/// `[50 000, 152 174]` µs; `gt 500 000, lte 152 175 000` admits `[501, 152 175]` µs; a term of
/// 500 001 ns is no whole microsecond, so no span of microsecond precision holds it; and
/// `gt 499 001, lt 500 999` admits only 500 µs.
#[tokio::test]
async fn duration_in_nanos_ranges_rewrite_to_duration_micros() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let cases = [
        (
            json!({"range": {"durationInNanos": {"gte": 50_000_000, "lt": 152_175_000}}}),
            vec![CHILD_SPAN_HEX],
        ),
        (
            json!({"range": {"durationInNanos": {"gt": 500_000, "lte": 152_175_000}}}),
            vec![ROOT_SPAN_HEX, CHILD_SPAN_HEX],
        ),
        (json!({"term": {"durationInNanos": 500_000}}), vec![OTHER_SPAN_HEX]),
        (json!({"term": {"durationInNanos": 500_001}}), vec![]),
        (
            json!({"range": {"durationInNanos": {"gt": 499_001, "lt": 500_999}}}),
            vec![OTHER_SPAN_HEX],
        ),
    ];
    for (query, expected_span_ids) in cases {
        let span_ids = search_span_ids(&server, query.clone()).await.expect("search");

        assert_eq!(span_ids, expected_span_ids, "{query}");
    }
    server.shutdown().await;
}

/// The profile's field model: `date` for every time field, `object` for events and links, and
/// the profile types of its keyword, integer and long fields. `parentSpanId` is searchable but
/// not aggregatable: its stored column admits NULL, which it renders as `""`, so a sort or an
/// aggregation on it is refused. `traceId` and `spanId` are stored in required columns and stay
/// aggregatable.
#[tokio::test]
async fn the_profile_reports_date_for_every_time_field_and_object_for_events_and_links() {
    let (server, _catalog) = TestServer::start().await;
    let capability = |field_type: &str, is_searchable: bool| json!({field_type: {"type": field_type, "searchable": is_searchable, "aggregatable": is_searchable}});

    let (status, answer) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/otel-v1-apm-span/_field_caps?fields=startTime,endTime,events,events.time,links,traceState,name,serviceName,\
         kind,status.code,durationInNanos,traceId,spanId,parentSpanId",
        None,
    )
    .await
    .expect("field caps");
    let (field_status, field_mapping) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/otel-v1-apm-span/_mapping/field/startTime",
        None,
    )
    .await
    .expect("field mapping");
    let (mapping_status, mapping) = send_request(&server, Method::GET, TENANT_A, "/otel-v1-apm-span/_mapping", None)
        .await
        .expect("mapping");

    assert_eq!(status, StatusCode::OK, "{answer}");
    let fields = &answer["fields"];
    assert_eq!(fields["startTime"], capability("date", true));
    assert_eq!(fields["endTime"], capability("date", true));
    assert_eq!(fields["events.time"], capability("date", false));
    assert_eq!(fields["events"], capability("object", false));
    assert_eq!(fields["links"], capability("object", false));
    for keyword in ["traceState", "name", "serviceName", "kind", "traceId", "spanId"] {
        assert_eq!(fields[keyword], capability("keyword", true), "{keyword}");
    }
    assert_eq!(
        fields["parentSpanId"],
        json!({"keyword": {"type": "keyword", "searchable": true, "aggregatable": false}})
    );
    assert_eq!(fields["status.code"], capability("integer", true));
    assert_eq!(fields["durationInNanos"], capability("long", true));
    assert_eq!(answer["indices"], json!([SPAN_INDEX]));
    assert_eq!(field_status, StatusCode::OK, "{field_mapping}");
    assert_eq!(
        field_mapping,
        json!({SPAN_ALIAS: {"mappings": {"startTime": {"full_name": "startTime", "mapping": {"startTime": {"type": "date"}}}}}})
    );
    assert_eq!(mapping_status, StatusCode::OK, "{mapping}");
    let properties = &mapping[SPAN_INDEX]["mappings"]["properties"];
    assert_eq!(
        properties["status"],
        json!({"type": "object", "properties": {"code": {"type": "integer"}, "message": {"type": "keyword"}}})
    );
    assert_eq!(properties["events"]["properties"]["time"], json!({"type": "date"}));
    assert_eq!(properties["span"]["properties"]["attributes"]["type"], "object");
    server.shutdown().await;
}

/// The child span of [`span_source_renders_nested_objects_as_the_contract_states`] as its
/// profile document, derived by hand: `152 175 µs × 1000 = 152 175 000` ns; `T0` is
/// `2025-10-09T08:53:20Z`, so `T0 + 152 175 µs` renders `…20.152175Z` and `T0 + 100 000 µs`
/// renders `…20.100000Z`; kind `3` is `SPAN_KIND_CLIENT`; the NULL status renders `{"code": 0}`
/// and the NULL `status_message` no `message`; the NULL `trace_state` renders `""`; `db.system`
/// writes `db@system`; the event's `droppedAttributesCount` is `0` because the struct member is
/// required and the row helper fills a required member it is not given with zero; the NULL
/// dropped event and link counts render no key.
fn build_expected_child_document() -> Value {
    json!({
        "traceId": TRACE_A_HEX,
        "spanId": CHILD_SPAN_HEX,
        "parentSpanId": ROOT_SPAN_HEX,
        "traceState": "",
        "name": "SELECT orders",
        "kind": "SPAN_KIND_CLIENT",
        "serviceName": "checkout",
        "startTime": "2025-10-09T08:53:20.000000Z",
        "endTime": "2025-10-09T08:53:20.152175Z",
        "durationInNanos": 152_175_000,
        "status": {"code": 0},
        "droppedAttributesCount": 1,
        "span": {"attributes": {"db@system": "postgresql", "http": "x"}},
        "resource": {"attributes": {"service@name": "checkout"}},
        "instrumentationScope": {"attributes": {}},
        "events": [{
            "time": "2025-10-09T08:53:20.100000Z",
            "name": "retry",
            "attributes": {"retry@count": "2"},
            "droppedAttributesCount": 0
        }],
        "links": []
    })
}

/// `_source` is a nested object of the profile's fields; include and exclude patterns apply to
/// its dotted paths; an escaped attribute name filters on the stored dotted key. Tenant B's span
/// shares the trace and span ids, so a leak would add a hit to each answer.
#[tokio::test]
async fn span_source_renders_nested_objects_as_the_contract_states() {
    let (server, catalog) = TestServer::start().await;
    let retry_event: &[(&str, Cell<'_>)] = &[
        (COL_TIMESTAMP, Cell::Micros(T0 + 100_000)),
        (COL_NAME, Cell::Text("retry")),
        (COL_ATTRIBUTES, Cell::Map(&[("retry.count", "2")])),
    ];
    let tenant_b_event: &[(&str, Cell<'_>)] = &[(COL_NAME, Cell::Text("tenant-b-event"))];
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &CHILD_SPAN,
                (T0, 152_175),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("SELECT orders")),
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                    (COL_KIND, Cell::Int(3)),
                    (
                        COL_SPAN_ATTRIBUTES,
                        Cell::Map(&[("db.system", "postgresql"), ("http", "x")]),
                    ),
                    (COL_RESOURCE_ATTRIBUTES, Cell::Map(&[("service.name", "checkout")])),
                    (COL_EVENTS, Cell::Structs(&[retry_event])),
                    (COL_DROPPED_ATTRIBUTES_COUNT, Cell::Int(1)),
                ],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_A,
                &CHILD_SPAN,
                (T0, 900),
                vec![
                    (COL_NAME, Cell::Text("tenant-b-child")),
                    (COL_SPAN_ATTRIBUTES, Cell::Map(&[("db.system", "postgresql")])),
                    (COL_EVENTS, Cell::Structs(&[tenant_b_event])),
                ],
            ),
        ],
    )
    .await
    .expect("commit");
    let child_query = json!({"term": {"spanId": CHILD_SPAN_HEX}});

    let whole_document = fetch_ok_answer(&server, TENANT_A, SPAN_SEARCH_PATH, &json!({"query": child_query}))
        .await
        .expect("the whole document");
    let included_paths = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"query": child_query, "_source": ["status.*", "span.attributes.db@system"]}),
    )
    .await
    .expect("included paths");
    let excluded_paths = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({
            "query": child_query,
            "_source": {"includes": ["status.*", "span.*"], "excludes": ["span.attributes.http"]}
        }),
    )
    .await
    .expect("excluded paths");
    let escaped_key_span_ids = search_span_ids(&server, json!({"term": {"span.attributes.db@system": "postgresql"}}))
        .await
        .expect("by escaped key");
    let eventful_span_ids = search_span_ids(&server, json!({"exists": {"field": "events"}}))
        .await
        .expect("with events");

    assert_eq!(whole_document["hits"]["total"], json!({"value": 1, "relation": "eq"}));
    assert_eq!(whole_document["hits"]["hits"][0]["_index"], SPAN_INDEX);
    assert_eq!(
        whole_document["hits"]["hits"][0]["_source"],
        build_expected_child_document()
    );
    assert_eq!(
        included_paths["hits"]["hits"][0]["_source"],
        json!({"status": {"code": 0}, "span": {"attributes": {"db@system": "postgresql"}}})
    );
    assert_eq!(
        excluded_paths["hits"]["hits"][0]["_source"],
        json!({"status": {"code": 0}, "span": {"attributes": {"db@system": "postgresql"}}})
    );
    assert_eq!(escaped_key_span_ids, [CHILD_SPAN_HEX]);
    assert_eq!(eventful_span_ids, [CHILD_SPAN_HEX]);
    server.shutdown().await;
}

/// PPL reads the native profile only: a source on the Data Prepper index is a semantic 400,
/// while the same rows read through `spans` answer.
#[tokio::test]
async fn a_ppl_source_on_a_profile_index_is_a_semantic_400() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let refused_reply = server
        .post_ppl(
            TENANT_A,
            "/_plugins/_ppl",
            &json!({"query": format!("source={SPAN_ALIAS} | head 1")}),
        )
        .await;
    let native_reply = server
        .post_ppl(
            TENANT_A,
            "/_plugins/_ppl",
            &json!({"query": "source=spans | stats count() as spans"}),
        )
        .await;

    assert_eq!(refused_reply.status, 400, "{}", refused_reply.text);
    assert_eq!(refused_reply.content_type, "application/json; charset=UTF-8");
    let body = refused_reply.parse_json_body();
    assert_eq!(body["error"]["type"], "SemanticCheckException", "{body}");
    assert_eq!(body["error"]["code"], "SEMANTIC_ERROR", "{body}");
    assert_eq!(native_reply.status, 200, "{}", native_reply.text);
    assert_eq!(native_reply.parse_json_body()["datarows"], json!([[3]]));
    server.shutdown().await;
}

/// `describe` reads no rows, so a profile index answers it: its rows list the profile's fields in
/// the profile's declared order, the field model `_mapping` reports, up to the first attribute
/// object (whose keys come from discovery).
#[tokio::test]
async fn describe_on_a_profile_index_lists_its_fields_in_declared_order() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let reply = server
        .post_ppl(
            TENANT_A,
            "/_plugins/_ppl",
            &json!({"query": format!("describe {SPAN_INDEX}")}),
        )
        .await;

    assert_eq!(reply.status, 200, "{}", reply.text);
    let body = reply.parse_json_body();
    let rows = body["datarows"].as_array().expect("datarows");
    let declared_names = [
        "traceId",
        "spanId",
        "parentSpanId",
        "traceState",
        "name",
        "kind",
        "serviceName",
        "startTime",
        "endTime",
        "durationInNanos",
        "status",
        "status.code",
        "status.message",
        "droppedAttributesCount",
        "droppedEventsCount",
        "droppedLinksCount",
        "span",
        "span.attributes",
    ];
    assert!(rows.len() >= declared_names.len(), "{body}");
    let listed_names: Vec<&Value> = rows[..declared_names.len()].iter().map(|row| &row[3]).collect();
    assert_eq!(listed_names, declared_names.map(Value::from).iter().collect::<Vec<_>>());
    assert!(rows.iter().all(|row| row[2] == SPAN_INDEX), "{body}");
    server.shutdown().await;
}

/// A term pattern on an encoded or derived field has no inverse, so it is the query-shard 400 of
/// a term pattern on a typed field, never a pattern over the stored value.
#[tokio::test]
async fn a_term_pattern_on_an_encoded_or_derived_field_is_a_query_shard_400() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    for query in [
        json!({"prefix": {"traceId": "0a"}}),
        json!({"wildcard": {"kind": "SPAN_KIND_*"}}),
        json!({"prefix": {"status.code": "2"}}),
        json!({"prefix": {"durationInNanos": "5"}}),
    ] {
        let (status, error) = fetch_search_error(&server, &json!({"query": query}))
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {error}");
        assert_eq!(error["root_cause"][0]["type"], "query_shard_exception", "{query}");
    }
    server.shutdown().await;
}

/// A literal that an encoded or derived field cannot hold is the query-shard 400 whose root cause
/// names the field as the request did, never the stored column behind it. A `terms` leaf reads
/// every value, so a malformed one refuses the leaf beside a well-formed one.
#[tokio::test]
async fn a_malformed_literal_on_a_span_field_is_a_query_shard_400_naming_the_requested_field() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    for (query, expected_field) in [
        (json!({"term": {"traceId": "zz"}}), "traceId"),
        (
            json!({"terms": {"parentSpanId": [ROOT_SPAN_HEX, "zz"]}}),
            "parentSpanId",
        ),
        (json!({"range": {"spanId": {"gte": "zz"}}}), "spanId"),
        (json!({"term": {"status.code": "x"}}), "status.code"),
        (json!({"range": {"status.code": {"lt": "x"}}}), "status.code"),
        (json!({"term": {"durationInNanos": "x"}}), "durationInNanos"),
        (json!({"range": {"durationInNanos": {"gt": "x"}}}), "durationInNanos"),
    ] {
        let (status, error) = fetch_search_error(&server, &json!({"query": query}))
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {error}");
        let cause = &error["root_cause"][0];
        assert_eq!(cause["type"], "query_shard_exception", "{query}");
        assert_eq!(cause["field"], expected_field, "{query}");
        assert_eq!(cause["index"], SPAN_INDEX, "{query}");
    }
    server.shutdown().await;
}

/// Every span renders an id, a trace state, a kind, a status code and a duration, so `exists` on
/// each is true for every span; `events` exists only on a span with an event.
#[tokio::test]
async fn exists_on_an_id_trace_state_kind_or_status_code_matches_every_span() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    for field in [
        "traceId",
        "parentSpanId",
        "traceState",
        "kind",
        "status.code",
        "durationInNanos",
    ] {
        let span_ids = search_span_ids(&server, json!({"exists": {"field": field}}))
            .await
            .expect("search");

        assert_eq!(span_ids, [ROOT_SPAN_HEX, CHILD_SPAN_HEX, OTHER_SPAN_HEX], "{field}");
    }
    let eventful_span_ids = search_span_ids(&server, json!({"exists": {"field": "events"}}))
        .await
        .expect("search");
    assert!(eventful_span_ids.is_empty(), "{eventful_span_ids:?}");
    server.shutdown().await;
}

/// A range on an encoded field keeps the order of the rendered values: a NULL id renders `""`,
/// below every hex id, so it lies inside a range without a lower bound; a NULL trace state renders
/// `""`, below `"a"`; a kind range admits the kinds whose names lie inside it.
#[tokio::test]
async fn encoded_ranges_keep_the_rendered_order_and_null_rows() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let cases = [
        (
            json!({"range": {"parentSpanId": {"lt": ROOT_SPAN_HEX}}}),
            vec![ROOT_SPAN_HEX, OTHER_SPAN_HEX],
        ),
        (
            json!({"range": {"parentSpanId": {"gte": "0000000000000000"}}}),
            vec![CHILD_SPAN_HEX],
        ),
        (
            json!({"range": {"spanId": {"gt": ROOT_SPAN_HEX}}}),
            vec![CHILD_SPAN_HEX, OTHER_SPAN_HEX],
        ),
        (
            json!({"range": {"traceState": {"lt": "a"}}}),
            vec![ROOT_SPAN_HEX, CHILD_SPAN_HEX, OTHER_SPAN_HEX],
        ),
        (json!({"range": {"traceState": {"gte": "a"}}}), vec![]),
        (
            json!({"range": {"kind": {"gte": "SPAN_KIND_SERVER", "lte": "SPAN_KIND_SERVER"}}}),
            vec![ROOT_SPAN_HEX],
        ),
    ];
    for (query, expected_span_ids) in cases {
        let span_ids = search_span_ids(&server, query.clone()).await.expect("search");

        assert_eq!(span_ids, expected_span_ids, "{query}");
    }
    server.shutdown().await;
}

/// `fields` and `docvalue_fields` read profile fields by name, a profile time in the requested
/// date format; a field pattern on a profile index is refused; a sort reads a profile time as
/// its stored date (epoch milliseconds, `floor(µs / 1000)`) and a hex id as its bytes.
#[tokio::test]
async fn fields_and_sort_read_profile_fields_and_a_field_pattern_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");
    let root_query = json!({"term": {"spanId": ROOT_SPAN_HEX}});

    let fields_answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({
            "query": root_query,
            "_source": false,
            "fields": ["kind", "durationInNanos", "status.code", "traceState"]
        }),
    )
    .await
    .expect("fields");
    let docvalue_answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({
            "query": root_query,
            "_source": false,
            "docvalue_fields": [{"field": "startTime", "format": "date_time"}]
        }),
    )
    .await
    .expect("docvalue fields");
    let start_time_answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"sort": [{"startTime": "desc"}]}),
    )
    .await
    .expect("sorted by start time");
    let id_sorted_answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"sort": [{"traceId": "desc"}, {"spanId": "asc"}]}),
    )
    .await
    .expect("sorted by ids");

    assert_eq!(
        fields_answer["hits"]["hits"][0]["fields"],
        json!({"kind": ["SPAN_KIND_SERVER"], "durationInNanos": [152_175_000], "status.code": [2], "traceState": [""]})
    );
    assert_eq!(
        docvalue_answer["hits"]["hits"][0]["fields"],
        json!({"startTime": ["2025-10-09T08:53:20.000Z"]})
    );
    assert_eq!(
        read_span_ids(&start_time_answer).expect("span ids"),
        [OTHER_SPAN_HEX, CHILD_SPAN_HEX, ROOT_SPAN_HEX]
    );
    let sort_values: Vec<&Value> = start_time_answer["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| &hit["sort"])
        .collect();
    assert_eq!(
        sort_values,
        [
            &json!([1_760_000_000_002_i64]),
            &json!([1_760_000_000_001_i64]),
            &json!([1_760_000_000_000_i64])
        ]
    );
    assert_eq!(
        read_span_ids(&id_sorted_answer).expect("span ids"),
        [OTHER_SPAN_HEX, ROOT_SPAN_HEX, CHILD_SPAN_HEX]
    );
    for key in ["fields", "docvalue_fields"] {
        let (status, error) = fetch_search_error(&server, &json!({"query": root_query, key: ["status.*"]}))
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{key}: {error}");
        for cause in [&error, &error["root_cause"][0]] {
            assert_eq!(cause["type"], "illegal_argument_exception", "{key}");
            assert_eq!(cause["field"], "status.*", "{key}");
            assert_eq!(cause["index"], SPAN_INDEX, "{key}");
        }
    }
    server.shutdown().await;
}

/// `parentSpanId` renders a root's missing id as `""`, which sorts before every id and buckets
/// on its own; its stored bytes order and bucket a missing id elsewhere, so a sort or an
/// aggregation on it is refused, naming the field, the index and the operation. `spanId`, stored
/// on every span, sorts and buckets as before.
#[tokio::test]
async fn a_sort_or_aggregation_on_a_parent_span_id_is_a_400_naming_it() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let cases = [
        (json!({"sort": [{"parentSpanId": "asc"}]}), "sort"),
        (json!({"sort": [{"parentSpanId": "desc"}]}), "sort"),
        (
            json!({"size": 0, "aggs": {"parents": {"terms": {"field": "parentSpanId"}}}}),
            "aggregation",
        ),
        (
            json!({"size": 0, "aggs": {"parent_count": {"cardinality": {"field": "parentSpanId"}}}}),
            "aggregation",
        ),
        (
            json!({"size": 0, "aggs": {"parent_values": {"value_count": {"field": "parentSpanId"}}}}),
            "aggregation",
        ),
    ];
    for (body, operation) in cases {
        let (status, error) = fetch_search_error(&server, &body).await.expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {error}");
        for cause in [&error, &error["root_cause"][0]] {
            assert_eq!(cause["type"], "illegal_argument_exception", "{body}");
            assert_eq!(cause["field"], "parentSpanId", "{body}");
            assert_eq!(cause["index"], SPAN_INDEX, "{body}");
            assert_eq!(cause["operation"], operation, "{body}");
        }
    }
    let span_sorted_answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"sort": [{"spanId": "desc"}]}),
    )
    .await
    .expect("sorted by span id");
    assert_eq!(
        read_span_ids(&span_sorted_answer).expect("span ids"),
        [OTHER_SPAN_HEX, CHILD_SPAN_HEX, ROOT_SPAN_HEX]
    );
    server.shutdown().await;
}

/// A span of `tenant` one hour before the wall clock, inside the default field discovery window,
/// carrying the span attributes `attributes`.
fn build_recent_span_row<'a>(tenant: &'a str, span: &'a [u8], attributes: &'a [(&'a str, &'a str)]) -> Row<'a> {
    let start = (chrono::Utc::now() - chrono::TimeDelta::hours(1)).timestamp_micros();
    build_span_row(
        tenant,
        &TRACE_A,
        span,
        (start, 1_000),
        vec![(COL_SPAN_ATTRIBUTES, Cell::Map(attributes))],
    )
}

/// A stored attribute key holding `@` would read back as a different key under the profile's
/// `@` escaping, so rendering it and listing it are both refused, naming the key. Tenant B, whose
/// keys are plain, renders and lists in the same commit.
#[tokio::test]
async fn a_stored_attribute_key_with_an_at_sign_is_a_400_when_rendered_or_listed() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[
            build_recent_span_row(TENANT_A, &ROOT_SPAN, &[("user@domain", "x")]),
            build_recent_span_row(TENANT_B, &TENANT_B_SPAN, &[("plain", "y")]),
        ],
    )
    .await
    .expect("commit");
    let match_all = json!({"query": {"match_all": {}}});

    let (search_status, search_answer) =
        send_request(&server, Method::POST, TENANT_A, SPAN_SEARCH_PATH, Some(&match_all))
            .await
            .expect("search request");
    let (mapping_status, mapping_answer) =
        send_request(&server, Method::GET, TENANT_A, "/otel-v1-apm-span/_mapping", None)
            .await
            .expect("mapping request");
    let tenant_b_search = fetch_ok_answer(&server, TENANT_B, SPAN_SEARCH_PATH, &match_all)
        .await
        .expect("tenant B search");
    let (tenant_b_mapping_status, tenant_b_mapping) =
        send_request(&server, Method::GET, TENANT_B, "/otel-v1-apm-span/_mapping", None)
            .await
            .expect("tenant B mapping");

    for (status, answer) in [(search_status, &search_answer), (mapping_status, &mapping_answer)] {
        assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
        for cause in [&answer["error"], &answer["error"]["root_cause"][0]] {
            assert_eq!(cause["type"], "illegal_argument_exception", "{answer}");
            assert_eq!(cause["key"], "user@domain", "{answer}");
        }
    }
    assert_eq!(
        tenant_b_search["hits"]["hits"][0]["_source"]["span"],
        json!({"attributes": {"plain": "y"}})
    );
    assert_eq!(tenant_b_mapping_status, StatusCode::OK, "{tenant_b_mapping}");
    assert_eq!(
        tenant_b_mapping[SPAN_INDEX]["mappings"]["properties"]["span"],
        json!({"type": "object", "properties": {"attributes": {"type": "object", "properties": {"plain": {"type": "keyword"}}}}})
    );
    server.shutdown().await;
}

/// A hex id of the profile is read as its stored bytes, so `terms` buckets it under its
/// lowercase hex and `cardinality` counts its distinct ids. Tenant B's span shares trace A, so a
/// leak would raise trace A's count to 3 and the span count to 4.
#[tokio::test]
async fn terms_on_trace_id_of_the_span_profile_buckets_lowercase_hex_ids() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({
            "size": 0,
            "aggs": {
                "traces": {"terms": {"field": "traceId"}},
                "spans": {"terms": {"field": "spanId"}},
                "trace_count": {"cardinality": {"field": "traceId"}},
                "span_count": {"cardinality": {"field": "spanId"}}
            }
        }),
    )
    .await
    .expect("aggregations");

    let aggregations = &answer["aggregations"];
    assert_eq!(
        aggregations["traces"]["buckets"],
        json!([{"key": TRACE_A_HEX, "doc_count": 2}, {"key": TRACE_B_HEX, "doc_count": 1}])
    );
    assert_eq!(
        aggregations["spans"]["buckets"],
        json!([
            {"key": ROOT_SPAN_HEX, "doc_count": 1},
            {"key": CHILD_SPAN_HEX, "doc_count": 1},
            {"key": OTHER_SPAN_HEX, "doc_count": 1}
        ])
    );
    assert_eq!(aggregations["trace_count"]["value"], 2);
    assert_eq!(aggregations["span_count"]["value"], 3);
    server.shutdown().await;
}

/// Tenant A: a root named `error` and two children of it, one carrying the span attribute value
/// `errored`. Tenant B: a root also named `error`, in the same commit.
async fn commit_error_spans(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    commit_rows(
        catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &ROOT_SPAN,
                (T0, 3_000),
                vec![(COL_NAME, Cell::Text("error"))],
            ),
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &CHILD_SPAN,
                (T0 + 1_000, 1_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("charge")),
                    (COL_SPAN_ATTRIBUTES, Cell::Map(&[("outcome", "errored")])),
                ],
            ),
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &OTHER_SPAN,
                (T0 + 2_000, 1_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("cron")),
                ],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_B,
                &TENANT_B_SPAN,
                (T0, 1_000),
                vec![(COL_NAME, Cell::Text("error"))],
            ),
        ],
    )
    .await
}

/// An unqualified lenient `query_string` skips the encoded fields its text cannot be a value of,
/// so it matches on names and attribute values only; a strict pattern on an encoded field is the
/// query-shard 400; an empty parent id selects the root.
#[tokio::test]
async fn a_lenient_query_string_skips_encoded_fields_and_a_strict_pattern_on_one_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    commit_error_spans(&catalog).await.expect("commit spans");

    let exact_text_span_ids = search_span_ids(&server, json!({"query_string": {"query": "error"}}))
        .await
        .expect("error");
    let prefix_text_span_ids = search_span_ids(&server, json!({"query_string": {"query": "err*"}}))
        .await
        .expect("err*");
    let roots = search_span_ids(&server, json!({"query_string": {"query": "parentSpanId:\"\""}}))
        .await
        .expect("roots");

    assert_eq!(exact_text_span_ids, [ROOT_SPAN_HEX]);
    assert_eq!(prefix_text_span_ids, [ROOT_SPAN_HEX, CHILD_SPAN_HEX]);
    assert_eq!(roots, [ROOT_SPAN_HEX]);
    for text in ["traceId:err*", "kind:SPAN*"] {
        let (status, error) = fetch_search_error(
            &server,
            &json!({"query": {"query_string": {"query": text, "lenient": false}}}),
        )
        .await
        .expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{text}: {error}");
        assert_eq!(error["root_cause"][0]["type"], "query_shard_exception", "{text}");
    }
    server.shutdown().await;
}

/// A case-insensitive term on an encoded keyword compares the rendered value: a span-kind name or
/// a trace state in any case. Without `case_insensitive` the same terms match nothing. Tenant B's
/// span carries both values in their stored case.
#[tokio::test]
async fn a_case_insensitive_term_on_kind_and_trace_state_matches_the_rendered_value() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &ROOT_SPAN,
                (T0, 1_000),
                vec![
                    (COL_KIND, Cell::Int(2)),
                    (COL_TRACE_STATE, Cell::Text("Rojo=00f067aa0ba902b7")),
                ],
            ),
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &CHILD_SPAN,
                (T0, 1_000),
                vec![(COL_KIND, Cell::Int(3))],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_A,
                &TENANT_B_SPAN,
                (T0, 1_000),
                vec![
                    (COL_KIND, Cell::Int(3)),
                    (COL_TRACE_STATE, Cell::Text("Rojo=00f067aa0ba902b7")),
                ],
            ),
        ],
    )
    .await
    .expect("commit spans");

    let cases = [
        (
            json!({"term": {"kind": {"value": "span_kind_client", "case_insensitive": true}}}),
            vec![CHILD_SPAN_HEX],
        ),
        (json!({"term": {"kind": {"value": "span_kind_client"}}}), vec![]),
        (
            json!({"term": {"traceState": {"value": "ROJO=00F067AA0BA902B7", "case_insensitive": true}}}),
            vec![ROOT_SPAN_HEX],
        ),
        (
            json!({"term": {"traceState": {"value": "ROJO=00F067AA0BA902B7"}}}),
            vec![],
        ),
    ];
    for (query, expected_span_ids) in cases {
        let span_ids = search_span_ids(&server, query.clone()).await.expect("search");

        assert_eq!(span_ids, expected_span_ids, "{query}");
    }
    server.shutdown().await;
}

const SS4O_LOGS: &str = "ss4o_logs-icegate-default";
const SS4O_TRACES: &str = "ss4o_traces-icegate-default";
const SS4O_METRICS: &str = "ss4o_metrics-icegate-default";
const LOG_TRACE: [u8; 16] = [0x0c; 16];

/// Search `index` as tenant A with `query` and return the status and the answer.
async fn search_tenant_a_index(
    server: &TestServer,
    index: &str,
    query: Value,
) -> Result<(StatusCode, Value), Box<dyn Error>> {
    send_request(
        server,
        Method::POST,
        TENANT_A,
        &format!("/{index}/_search"),
        Some(&json!({"query": query})),
    )
    .await
}

/// The `_source.<field>` text of every hit, sorted: the order of an unsorted search is no
/// contract.
fn read_sorted_source_texts(answer: &Value, field: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut texts: Vec<String> = answer["hits"]["hits"]
        .as_array()
        .ok_or_else(|| format!("no hits in {answer}"))?
        .iter()
        .map(|hit| {
            hit["_source"][field]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("no text `{field}` in {hit}"))
        })
        .collect::<Result<_, _>>()?;
    texts.sort();
    Ok(texts)
}

/// A log row of `tenant` at `micros`, observed 5 µs and ingested 10 µs later, plus `cells`.
pub(super) fn build_log_row<'a>(
    tenant: &'a str,
    micros: i64,
    body: &'a str,
    severity: &'a str,
    mut cells: Vec<(&'a str, Cell<'a>)>,
) -> Row<'a> {
    let mut row_cells = vec![
        (COL_TIMESTAMP, Cell::Micros(micros)),
        (COL_OBSERVED_TIMESTAMP, Cell::Micros(micros + 5)),
        (COL_INGESTED_TIMESTAMP, Cell::Micros(micros + 10)),
        (COL_BODY, Cell::Text(body)),
        (COL_SEVERITY_TEXT, Cell::Text(severity)),
    ];
    row_cells.append(&mut cells);
    Row {
        tenant,
        cells: row_cells,
    }
}

/// A metrics row at `T0`: the required columns plus `cells`.
pub(super) fn build_metric_row<'a>(
    tenant: &'a str,
    name: &'a str,
    metric_type: &'a str,
    cells: Vec<(&'a str, Cell<'a>)>,
) -> Row<'a> {
    build_metric_row_at(tenant, T0, name, metric_type, cells)
}

/// A metrics row at `micros`, ingested then: the required columns plus `cells`.
pub(super) fn build_metric_row_at<'a>(
    tenant: &'a str,
    micros: i64,
    name: &'a str,
    metric_type: &'a str,
    mut cells: Vec<(&'a str, Cell<'a>)>,
) -> Row<'a> {
    let mut row_cells = vec![
        (COL_TIMESTAMP, Cell::Micros(micros)),
        (COL_INGESTED_TIMESTAMP, Cell::Micros(micros)),
        (COL_METRIC_NAME, Cell::Text(name)),
        (COL_METRIC_TYPE, Cell::Text(metric_type)),
    ];
    row_cells.append(&mut cells);
    Row {
        tenant,
        cells: row_cells,
    }
}

/// Each ss4o logs field reads its source: aliases filter like their targets, a resource path to
/// a promoted key (dotted or underscored) and a pattern over the resource attributes read the
/// `service_name` column, attribute keys keep their dots. Tenant B's row, in the same commit,
/// matches every query too. An undeclared field matches nothing and is absent from
/// `_field_caps`, and an alias is not repeated in `_source`.
#[tokio::test]
async fn ss4o_logs_fields_resolve_to_their_sources_and_absent_fields_match_nothing() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_log_row(
                TENANT_A,
                T0,
                "boom",
                "ERROR",
                vec![
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                    (COL_TRACE_ID, Cell::Bytes(&LOG_TRACE)),
                    (COL_LOG_ATTRIBUTES, Cell::Map(&[("http.method", "GET")])),
                ],
            ),
            build_log_row(TENANT_A, T0 + 1_000, "fine", "INFO", vec![]),
            build_log_row(
                TENANT_B,
                T0,
                "tenant b boom",
                "ERROR",
                vec![
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                    (COL_TRACE_ID, Cell::Bytes(&LOG_TRACE)),
                    (COL_LOG_ATTRIBUTES, Cell::Map(&[("http.method", "GET")])),
                ],
            ),
        ],
    )
    .await
    .expect("commit logs");

    for query in [
        json!({"term": {"severity.text": "ERROR"}}),
        json!({"term": {"serviceName": "checkout"}}),
        json!({"term": {"resource.attributes.service.name": "checkout"}}),
        json!({"term": {"resource.attributes.service_name": "checkout"}}),
        json!({"query_string": {"query": "checkout", "fields": ["resource.attributes.*"]}}),
        json!({"term": {"traceId": "0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c"}}),
        json!({"term": {"attributes.http.method": "GET"}}),
        json!({"match": {"@message": "boom"}}),
        json!({"range": {"time": {"lte": 1_760_000_000_000_u64, "format": "epoch_millis"}}}),
        json!({"range": {"observerTime": {"lte": 1_760_000_000_000_u64, "format": "epoch_millis"}}}),
    ] {
        let (status, answer) = search_tenant_a_index(&server, SS4O_LOGS, query.clone())
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::OK, "{query}: {answer}");
        assert_eq!(
            read_sorted_source_texts(&answer, "body").expect("bodies"),
            ["boom"],
            "{query}"
        );
    }
    for query in [
        json!({"term": {"severity.number": 17}}),
        json!({"exists": {"field": "schemaUrl"}}),
        json!({"exists": {"field": "flags"}}),
    ] {
        let (status, answer) = search_tenant_a_index(&server, SS4O_LOGS, query.clone())
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::OK, "{query}: {answer}");
        assert_eq!(answer["hits"]["total"]["value"], 0, "{query}");
    }

    let (_, answer) = search_tenant_a_index(&server, SS4O_LOGS, json!({"term": {"severity.text": "ERROR"}}))
        .await
        .expect("search request");
    assert_eq!(answer["hits"]["total"]["value"], 1, "{answer}");
    assert_eq!(
        answer["hits"]["hits"][0]["_source"],
        json!({
            "@timestamp": "2025-10-09T08:53:20.000000Z",
            "observedTimestamp": "2025-10-09T08:53:20.000005Z",
            "traceId": "0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c",
            "severity": {"text": "ERROR"},
            "body": "boom",
            "serviceName": "checkout",
            "attributes": {"http.method": "GET"},
            "resource": {"attributes": {}},
            "instrumentationScope": {"attributes": {}}
        })
    );

    let (status, answer) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        &format!("/{SS4O_LOGS}/_field_caps?fields=*"),
        None,
    )
    .await
    .expect("field caps");
    assert_eq!(status, StatusCode::OK, "{answer}");
    let fields = answer["fields"].as_object().expect("fields");
    for absent in [
        "severity.number",
        "schemaUrl",
        "flags",
        "droppedAttributesCount",
        "event.name",
    ] {
        assert!(!fields.contains_key(absent), "{absent} is listed");
    }
    assert_eq!(
        fields["body"],
        json!({"text": {"type": "text", "searchable": true, "aggregatable": false}})
    );
    for date_field in ["@timestamp", "time", "observedTimestamp", "observerTime"] {
        assert_eq!(fields[date_field]["date"]["type"], "date", "{date_field}");
    }
    server.shutdown().await;
}

/// The ss4o traces index reads the span fields of the Data Prepper profile with stored attribute
/// keys unescaped, in `_source` and in event attributes too, plus `@timestamp` as an alias of
/// `startTime`. Tenant B's span, in the same commit, matches every query below too: only the
/// tenant scope keeps it out of tenant A's answers.
#[tokio::test]
async fn ss4o_traces_resolve_like_data_prepper_without_at_escaping() {
    let (server, catalog) = TestServer::start().await;
    let retry_event: &[(&str, Cell<'_>)] = &[
        (COL_TIMESTAMP, Cell::Micros(T0 + 100_000)),
        (COL_NAME, Cell::Text("retry")),
        (COL_ATTRIBUTES, Cell::Map(&[("retry.count", "2")])),
        (COL_DROPPED_ATTRIBUTES_COUNT, Cell::Int(0)),
    ];
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &CHILD_SPAN,
                (T0, 50_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("SELECT orders")),
                    (COL_KIND, Cell::Int(3)),
                    (COL_SPAN_ATTRIBUTES, Cell::Map(&[("db.system", "postgresql")])),
                    (COL_EVENTS, Cell::Structs(&[retry_event])),
                ],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_A,
                &TENANT_B_SPAN,
                (T0, 50_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("tenant-b-select")),
                    (COL_KIND, Cell::Int(3)),
                    (COL_SPAN_ATTRIBUTES, Cell::Map(&[("db.system", "postgresql")])),
                ],
            ),
        ],
    )
    .await
    .expect("commit spans");

    for query in [
        json!({"term": {"span.attributes.db.system": "postgresql"}}),
        json!({"term": {"kind": "SPAN_KIND_CLIENT"}}),
        json!({"range": {"@timestamp": {"gte": 1_760_000_000_000_u64, "lte": 1_760_000_000_000_u64}}}),
        json!({"range": {"durationInNanos": {"gte": 50_000_000}}}),
    ] {
        let (status, answer) = search_tenant_a_index(&server, SS4O_TRACES, query.clone())
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::OK, "{query}: {answer}");
        assert_eq!(
            read_sorted_span_ids(&answer).expect("span ids"),
            [CHILD_SPAN_HEX],
            "{query}"
        );
    }
    let (_, answer) = search_tenant_a_index(
        &server,
        SS4O_TRACES,
        json!({"term": {"span.attributes.db@system": "postgresql"}}),
    )
    .await
    .expect("search request");
    assert_eq!(
        answer["hits"]["total"]["value"], 0,
        "the ss4o profile reads no @ spelling"
    );

    let (_, answer) = search_tenant_a_index(&server, SS4O_TRACES, json!({"match_all": {}}))
        .await
        .expect("search request");
    assert_eq!(
        answer["hits"]["total"]["value"], 1,
        "tenant A sees its one span: {answer}"
    );
    let source = &answer["hits"]["hits"][0]["_source"];
    assert_eq!(source["span"], json!({"attributes": {"db.system": "postgresql"}}));
    assert_eq!(
        source["events"],
        json!([{
            "time": "2025-10-09T08:53:20.100000Z",
            "name": "retry",
            "attributes": {"retry.count": "2"},
            "droppedAttributesCount": 0
        }])
    );
    assert_eq!(source["kind"], "SPAN_KIND_CLIENT");
    assert_eq!(source["startTime"], "2025-10-09T08:53:20.000000Z");
    assert!(
        source.get("@timestamp").is_none(),
        "an alias is not repeated in _source"
    );
    assert!(source.get("traceGroup").is_none(), "{source}");

    let (status, answer) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        &format!("/{SS4O_TRACES}/_field_caps?fields=*"),
        None,
    )
    .await
    .expect("field caps");
    assert_eq!(status, StatusCode::OK, "{answer}");
    let fields = answer["fields"].as_object().expect("fields");
    assert_eq!(fields["@timestamp"]["date"]["type"], "date");
    assert!(fields.keys().all(|name| !name.starts_with("traceGroup")), "{fields:?}");
    server.shutdown().await;
}

/// Tenant A: a gauge with an exemplar, a cumulative sum, a delta histogram, an exponential
/// histogram, a summary. Tenant B, in the same commit: a cumulative gauge, a histogram and a
/// summary named like tenant A's and shaped like them, so a leak duplicates a name in every
/// selection of a gauge, a cumulative temporality, the histogram's kind, bucket and bound counts
/// or the summary's kind and quantile count.
async fn commit_metric_shapes(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    let exemplar: &[(&str, Cell<'_>)] = &[
        (COL_TIMESTAMP, Cell::Micros(T0 + 7)),
        (COL_TRACE_ID, Cell::Bytes(&TRACE_A)),
        (COL_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
        (COL_VALUE_DOUBLE, Cell::Double(3.5)),
    ];
    let median: &[(&str, Cell<'_>)] = &[
        (QUANTILE_FIELD_QUANTILE, Cell::Double(0.5)),
        (QUANTILE_FIELD_VALUE, Cell::Double(12.0)),
    ];
    let tail: &[(&str, Cell<'_>)] = &[
        (QUANTILE_FIELD_QUANTILE, Cell::Double(0.99)),
        (QUANTILE_FIELD_VALUE, Cell::Double(40.0)),
    ];
    commit_rows(
        catalog,
        METRICS_TABLE,
        &[
            build_metric_row(
                TENANT_A,
                "queue.depth",
                "gauge",
                vec![
                    (COL_VALUE_INT, Cell::Long(4)),
                    (COL_EXEMPLARS, Cell::Structs(&[exemplar])),
                ],
            ),
            build_metric_row(
                TENANT_A,
                "requests",
                "sum",
                vec![
                    (COL_VALUE_INT, Cell::Long(20)),
                    (COL_AGGREGATION_TEMPORALITY, Cell::Text("CUMULATIVE")),
                ],
            ),
            build_metric_row(
                TENANT_A,
                "latency",
                "histogram",
                vec![
                    (COL_AGGREGATION_TEMPORALITY, Cell::Text("DELTA")),
                    (COL_BUCKET_COUNTS, Cell::Longs(&[1, 2, 3])),
                    (COL_EXPLICIT_BOUNDS, Cell::Doubles(&[0.5, 1.0])),
                ],
            ),
            build_metric_row(
                TENANT_A,
                "payload",
                "exponential_histogram",
                vec![
                    (COL_AGGREGATION_TEMPORALITY, Cell::Text("DELTA")),
                    (COL_SCALE, Cell::Int(0)),
                    (COL_POSITIVE_OFFSET, Cell::Int(-1)),
                    (COL_POSITIVE_BUCKET_COUNTS, Cell::Longs(&[3, 5])),
                    (COL_NEGATIVE_OFFSET, Cell::Int(0)),
                    (COL_NEGATIVE_BUCKET_COUNTS, Cell::Longs(&[7])),
                    (COL_BUCKET_COUNTS, Cell::Longs(&[])),
                ],
            ),
            build_metric_row(
                TENANT_A,
                "rpc.duration",
                "summary",
                vec![(COL_QUANTILE_VALUES, Cell::Structs(&[median, tail]))],
            ),
            build_metric_row(
                TENANT_B,
                "queue.depth",
                "gauge",
                vec![
                    (COL_VALUE_INT, Cell::Long(999)),
                    (COL_AGGREGATION_TEMPORALITY, Cell::Text("CUMULATIVE")),
                ],
            ),
            build_metric_row(
                TENANT_B,
                "latency",
                "histogram",
                vec![
                    (COL_BUCKET_COUNTS, Cell::Longs(&[1, 2, 3])),
                    (COL_EXPLICIT_BOUNDS, Cell::Doubles(&[0.5, 1.0])),
                ],
            ),
            build_metric_row(
                TENANT_B,
                "rpc.duration",
                "summary",
                vec![(COL_QUANTILE_VALUES, Cell::Structs(&[median, tail]))],
            ),
        ],
    )
    .await
}

/// `kind` and `aggregationTemporality` render the upper-case names and filter back to the stored
/// value: a term, a terms list and a range compare the names, `exists` holds where a name
/// renders, and `COUNTER` and `AGGREGATION_TEMPORALITY_UNSPECIFIED` match nothing. Tenant B's
/// gauge is cumulative and it also has a histogram and a summary, so a leak would change the
/// gauge, cumulative, histogram and missing-kind rows.
#[tokio::test]
async fn ss4o_metrics_kind_and_temporality_render_upper_case_enums_and_filter_back() {
    let (server, catalog) = TestServer::start().await;
    commit_metric_shapes(&catalog).await.expect("commit metrics");

    for (query, expected_names) in [
        (json!({"term": {"kind": "GAUGE"}}), vec!["queue.depth"]),
        (json!({"term": {"kind": "EXPONENTIAL_HISTOGRAM"}}), vec!["payload"]),
        (json!({"term": {"kind": "COUNTER"}}), vec![]),
        (
            json!({"terms": {"kind": ["SUM", "HISTOGRAM"]}}),
            vec!["latency", "requests"],
        ),
        // Names compare as keywords: `EXPONENTIAL_HISTOGRAM` and `GAUGE` sort below `HISTOGRAM`.
        (
            json!({"range": {"kind": {"gte": "HISTOGRAM"}}}),
            vec!["latency", "requests"],
        ),
        (
            json!({"term": {"aggregationTemporality": "AGGREGATION_TEMPORALITY_CUMULATIVE"}}),
            vec!["requests"],
        ),
        (
            json!({"term": {"aggregationTemporality": "AGGREGATION_TEMPORALITY_DELTA"}}),
            vec!["latency", "payload"],
        ),
        (
            json!({"term": {"aggregationTemporality": "AGGREGATION_TEMPORALITY_UNSPECIFIED"}}),
            vec![],
        ),
        (
            json!({"exists": {"field": "aggregationTemporality"}}),
            vec!["latency", "payload", "requests"],
        ),
        (
            json!({"bool": {"must_not": {"exists": {"field": "kind"}}}}),
            vec!["rpc.duration"],
        ),
    ] {
        let (status, answer) = search_tenant_a_index(&server, SS4O_METRICS, query.clone())
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::OK, "{query}: {answer}");
        assert_eq!(
            read_sorted_source_texts(&answer, "name").expect("names"),
            expected_names,
            "{query}"
        );
    }

    let (_, answer) = search_tenant_a_index(&server, SS4O_METRICS, json!({"match_all": {}}))
        .await
        .expect("search request");
    let find_source = |name: &str| {
        answer["hits"]["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| &hit["_source"])
            .find(|source| source["name"] == name)
            .expect("a metric of that name")
            .clone()
    };
    assert_eq!(find_source("queue.depth")["kind"], "GAUGE");
    assert!(find_source("queue.depth").get("aggregationTemporality").is_none());
    assert_eq!(
        find_source("requests")["aggregationTemporality"],
        "AGGREGATION_TEMPORALITY_CUMULATIVE"
    );
    assert_eq!(find_source("payload")["kind"], "EXPONENTIAL_HISTOGRAM");
    assert!(find_source("rpc.duration").get("kind").is_none(), "summary has no kind");
    server.shutdown().await;
}

/// The bucket, quantile and exemplar fields render from the arrays and the counts are their
/// lengths, derived by hand: the exponential histogram has scale 0, so `base = 2`; positive
/// offset `-1` gives `(2^-1, 2^0]` and `(2^0, 2^1]`, negative offset `0` gives `[-2^1, -2^0)`
/// written as `min -2.0`, `max -1.0`; `T0 + 7 µs` renders `…20.000007Z`. A NULL list renders the
/// field absent and an empty one `[]` with a count of `0`.
#[tokio::test]
async fn ss4o_metrics_bucket_quantile_and_exemplar_fields_render_and_their_counts_derive_from_the_arrays() {
    let (server, catalog) = TestServer::start().await;
    commit_metric_shapes(&catalog).await.expect("commit metrics");
    let shape_fields = [
        "bucketCountsList",
        "explicitBoundsList",
        "bucketCount",
        "explicitBoundsCount",
        "quantileValuesCount",
        "positiveBuckets",
        "negativeBuckets",
        "quantiles",
        "exemplar",
    ];
    let read_shape_fields = |answer: &Value| -> Value {
        let source = &answer["hits"]["hits"][0]["_source"];
        Value::Object(
            shape_fields
                .iter()
                .filter_map(|name| source.get(*name).map(|value| ((*name).to_string(), value.clone())))
                .collect(),
        )
    };

    for (name, expected_fields) in [
        (
            "latency",
            json!({"bucketCountsList": [1, 2, 3], "explicitBoundsList": [0.5, 1.0], "bucketCount": 3, "explicitBoundsCount": 2}),
        ),
        (
            "payload",
            json!({
                "bucketCountsList": [],
                "bucketCount": 0,
                "positiveBuckets": [{"count": 3, "min": 0.5, "max": 1.0}, {"count": 5, "min": 1.0, "max": 2.0}],
                "negativeBuckets": [{"count": 7, "min": -2.0, "max": -1.0}]
            }),
        ),
        (
            "rpc.duration",
            json!({"quantileValuesCount": 2, "quantiles": [{"quantile": 0.5, "value": 12.0}, {"quantile": 0.99, "value": 40.0}]}),
        ),
        (
            "queue.depth",
            json!({"exemplar": [{
                "time": "2025-10-09T08:53:20.000007Z",
                "traceId": TRACE_A_HEX,
                "spanId": ROOT_SPAN_HEX
            }]}),
        ),
    ] {
        let (status, answer) = search_tenant_a_index(&server, SS4O_METRICS, json!({"term": {"name": name}}))
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::OK, "{name}: {answer}");
        assert_eq!(
            answer["hits"]["total"]["value"], 1,
            "{name}: tenant B's gauge is never returned"
        );
        assert_eq!(read_shape_fields(&answer), expected_fields, "{name}");
    }

    let (status, answer) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        &format!(
            "/{SS4O_METRICS}/_field_caps?fields=positiveBuckets,negativeBuckets,quantiles,exemplar,bucketCount,\
             explicitBoundsCount,quantileValuesCount,value,kind,aggregationTemporality"
        ),
        None,
    )
    .await
    .expect("field caps");
    assert_eq!(status, StatusCode::OK, "{answer}");
    for object in ["positiveBuckets", "negativeBuckets", "quantiles", "exemplar"] {
        assert_eq!(
            answer["fields"][object],
            json!({"object": {"type": "object", "searchable": false, "aggregatable": false}}),
            "{object}"
        );
    }
    for long in ["bucketCount", "explicitBoundsCount", "quantileValuesCount"] {
        assert_eq!(
            answer["fields"][long],
            json!({"long": {"type": "long", "searchable": true, "aggregatable": true}}),
            "{long}"
        );
    }
    assert_eq!(
        answer["fields"]["value"],
        json!({"double": {"type": "double", "searchable": true, "aggregatable": true}})
    );
    for keyword in ["kind", "aggregationTemporality"] {
        assert_eq!(
            answer["fields"][keyword],
            json!({"keyword": {"type": "keyword", "searchable": true, "aggregatable": true}}),
            "{keyword}"
        );
    }
    server.shutdown().await;
}

/// A list length lowers over the length per row: ranges, terms and `exists` select by it, a NULL
/// list has no length and an empty one has `0`, and a fractional length matches nothing. Tenant
/// B's histogram has three buckets and two bounds and its summary two quantiles, so a leak would
/// add a second `latency` to every selection of `latency` and a second `rpc.duration` to the
/// quantile count range.
#[tokio::test]
async fn a_range_on_an_array_length_field_selects_rows_by_length() {
    let (server, catalog) = TestServer::start().await;
    commit_metric_shapes(&catalog).await.expect("commit metrics");

    for (query, expected_names) in [
        (json!({"range": {"bucketCount": {"gte": 3}}}), vec!["latency"]),
        (json!({"range": {"bucketCount": {"lt": 1}}}), vec!["payload"]),
        (json!({"term": {"explicitBoundsCount": 2}}), vec!["latency"]),
        (json!({"term": {"bucketCount": 0}}), vec!["payload"]),
        (json!({"terms": {"bucketCount": [0, 3]}}), vec!["latency", "payload"]),
        (json!({"term": {"bucketCount": 2.5}}), vec![]),
        (json!({"exists": {"field": "bucketCount"}}), vec!["latency", "payload"]),
        (
            json!({"range": {"quantileValuesCount": {"gt": 1, "lte": 2}}}),
            vec!["rpc.duration"],
        ),
    ] {
        let (status, answer) = search_tenant_a_index(&server, SS4O_METRICS, query.clone())
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::OK, "{query}: {answer}");
        assert_eq!(
            read_sorted_source_texts(&answer, "name").expect("names"),
            expected_names,
            "{query}"
        );
    }
    server.shutdown().await;
}

/// A term-level leaf on a bucket, quantile or exemplar field, or on a path below one, has no
/// inverse and is refused naming the field as the request did; the refusal reads no row.
#[tokio::test]
async fn a_leaf_on_a_bucket_quantile_or_exemplar_field_is_a_400_naming_it() {
    let (server, catalog) = TestServer::start().await;
    commit_metric_shapes(&catalog).await.expect("commit metrics");

    for (query, field) in [
        (json!({"term": {"positiveBuckets.count": 3}}), "positiveBuckets.count"),
        (json!({"exists": {"field": "quantiles"}}), "quantiles"),
        (json!({"range": {"exemplar.time": {"gte": 0}}}), "exemplar.time"),
        (json!({"term": {"negativeBuckets": 7}}), "negativeBuckets"),
        (json!({"terms": {"quantiles.value": [12.0]}}), "quantiles.value"),
        (json!({"prefix": {"exemplar.traceId": "0a"}}), "exemplar.traceId"),
    ] {
        let (status, answer) = search_tenant_a_index(&server, SS4O_METRICS, query.clone())
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {answer}");
        assert_eq!(answer["error"]["type"], "illegal_argument_exception", "{query}");
        assert_eq!(answer["error"]["field"], field, "{query}");
    }
    server.shutdown().await;
}

/// A literal that an ss4o metrics field cannot hold is the query-shard 400 whose root cause names
/// the field as the request did, never the stored column behind it, and a term pattern on an
/// enum name or a list length is the query-shard 400 of a term pattern on a typed field.
#[tokio::test]
async fn a_malformed_or_pattern_leaf_on_an_ss4o_metrics_field_is_a_query_shard_400() {
    let (server, catalog) = TestServer::start().await;
    commit_metric_shapes(&catalog).await.expect("commit metrics");

    for (query, expected_field) in [
        (json!({"term": {"bucketCount": "abc"}}), Some("bucketCount")),
        (json!({"range": {"bucketCount": {"gte": "x"}}}), Some("bucketCount")),
        (json!({"term": {"value": "abc"}}), Some("value")),
        (json!({"range": {"value": {"lt": "x"}}}), Some("value")),
        (json!({"prefix": {"kind": "GA"}}), None),
        (json!({"wildcard": {"bucketCount": "1*"}}), None),
    ] {
        let (status, answer) = search_tenant_a_index(&server, SS4O_METRICS, query.clone())
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {answer}");
        let cause = &answer["error"]["root_cause"][0];
        assert_eq!(cause["type"], "query_shard_exception", "{query}");
        assert_eq!(cause["index"], SS4O_METRICS, "{query}");
        if let Some(expected_field) = expected_field {
            assert_eq!(cause["field"], expected_field, "{query}");
        }
    }
    server.shutdown().await;
}

/// Points named after their stored value: `int:<n>` holds `value_int = n`, `double:<x>` holds
/// `value_double = x`; `histogram` holds neither. Tenant B's `int:1` would add a hit to every
/// selection of tenant A's `int:1`.
async fn commit_value_points(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    commit_rows(
        catalog,
        METRICS_TABLE,
        &[
            build_metric_row(TENANT_A, "int:-1", "gauge", vec![(COL_VALUE_INT, Cell::Long(-1))]),
            build_metric_row(TENANT_A, "int:0", "gauge", vec![(COL_VALUE_INT, Cell::Long(0))]),
            build_metric_row(TENANT_A, "int:1", "gauge", vec![(COL_VALUE_INT, Cell::Long(1))]),
            build_metric_row(TENANT_A, "int:2", "gauge", vec![(COL_VALUE_INT, Cell::Long(2))]),
            build_metric_row(
                TENANT_A,
                "double:-0.0",
                "gauge",
                vec![(COL_VALUE_DOUBLE, Cell::Double(-0.0))],
            ),
            build_metric_row(
                TENANT_A,
                "double:0.0",
                "gauge",
                vec![(COL_VALUE_DOUBLE, Cell::Double(0.0))],
            ),
            build_metric_row(
                TENANT_A,
                "double:1.5",
                "gauge",
                vec![(COL_VALUE_DOUBLE, Cell::Double(1.5))],
            ),
            build_metric_row(
                TENANT_A,
                "histogram",
                "histogram",
                vec![(COL_BUCKET_COUNTS, Cell::Longs(&[1]))],
            ),
            build_metric_row(TENANT_B, "int:1", "gauge", vec![(COL_VALUE_INT, Cell::Long(1))]),
        ],
    )
    .await
}

/// `value` averages integer and double points together on the native `metrics` index and on the
/// ss4o metrics index: `(10 + 20 + 0.5) / 3`, the histogram row has no value and tenant B's row
/// is not read. `fields` and `docvalue_fields` render it on the native index from either value
/// column, and not at all on the histogram row.
#[tokio::test]
async fn the_metrics_value_field_averages_integer_and_double_points() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        METRICS_TABLE,
        &[
            build_metric_row(TENANT_A, "a", "gauge", vec![(COL_VALUE_INT, Cell::Long(10))]),
            build_metric_row(TENANT_A, "b", "sum", vec![(COL_VALUE_INT, Cell::Long(20))]),
            build_metric_row(TENANT_A, "c", "gauge", vec![(COL_VALUE_DOUBLE, Cell::Double(0.5))]),
            build_metric_row(TENANT_A, "d", "histogram", vec![(COL_BUCKET_COUNTS, Cell::Longs(&[4]))]),
            build_metric_row(TENANT_B, "a", "gauge", vec![(COL_VALUE_INT, Cell::Long(1_000))]),
        ],
    )
    .await
    .expect("commit metrics");

    for (index, double_field) in [("metrics", "value_double"), (SS4O_METRICS, "value@double")] {
        let (status, answer) = send_request(
            &server,
            Method::POST,
            TENANT_A,
            &format!("/{index}/_search"),
            Some(&json!({
                "size": 0,
                "aggs": {"points": {"avg": {"field": "value"}}, "doubles": {"avg": {"field": double_field}}}
            })),
        )
        .await
        .expect("search request");

        assert_eq!(status, StatusCode::OK, "{index}: {answer}");
        assert_eq!(
            answer["aggregations"]["points"]["value"],
            json!(30.5_f64 / 3.0),
            "{index}"
        );
        assert_eq!(answer["aggregations"]["doubles"]["value"], json!(0.5), "{index}");
    }

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/metrics/_search",
        Some(&json!({"query": {"match_all": {}}, "docvalue_fields": ["value"], "fields": ["value"]})),
    )
    .await
    .expect("search request");
    assert_eq!(status, StatusCode::OK, "{answer}");
    let hits = answer["hits"]["hits"].as_array().expect("hits");
    let mut names: Vec<&str> = hits
        .iter()
        .map(|hit| hit["_source"][COL_METRIC_NAME].as_str().expect("a metric name"))
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["a", "b", "c", "d"], "tenant B's row is not returned");
    let find_fields = |name: &str| {
        hits.iter()
            .find(|hit| hit["_source"][COL_METRIC_NAME] == name)
            .map(|hit| hit["fields"].clone())
            .expect("a hit of that name")
    };
    assert_eq!(find_fields("a")["value"], json!([10.0]));
    assert_eq!(find_fields("b")["value"], json!([20.0]));
    assert_eq!(find_fields("c")["value"], json!([0.5]));
    assert!(find_fields("d").get("value").is_none(), "{answer}");

    let (status, answer) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/metrics/_field_caps?fields=value",
        None,
    )
    .await
    .expect("field caps");
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(
        answer["fields"]["value"],
        json!({"double": {"type": "double", "searchable": true, "aggregatable": true}})
    );
    server.shutdown().await;
}

/// The `value` filter inverse, derived by hand: under IEEE totalOrder `-0.0 < +0.0`, so
/// `lte -0.0` keeps the double `-0.0` and the integer `-1` but not the integer `0`, `gt -0.0`
/// keeps the integer `0` and the double `+0.0`, and the term `-0.0` has no integer match;
/// `(0.9, 1.1)` admits the integers `[⌈0.9⌉, ⌊1.1⌋] = [1, 1]`; `(1, 2)` admits no integer.
#[tokio::test]
async fn the_metrics_value_inverse_uses_ceil_and_floor_integer_bounds_and_negative_zero_rules() {
    let (server, catalog) = TestServer::start().await;
    commit_value_points(&catalog).await.expect("commit points");

    for (query, expected_names) in [
        (json!({"term": {"value": 1}}), vec!["int:1"]),
        (json!({"term": {"value": 1.5}}), vec!["double:1.5"]),
        (json!({"term": {"value": 0}}), vec!["double:0.0", "int:0"]),
        (json!({"term": {"value": -0.0}}), vec!["double:-0.0"]),
        (json!({"terms": {"value": [2, 1.5]}}), vec!["double:1.5", "int:2"]),
        (
            json!({"range": {"value": {"lte": -0.0}}}),
            vec!["double:-0.0", "int:-1"],
        ),
        (
            json!({"range": {"value": {"gt": -0.0}}}),
            vec!["double:0.0", "double:1.5", "int:0", "int:1", "int:2"],
        ),
        (
            json!({"range": {"value": {"gte": 0.5, "lte": 1.5}}}),
            vec!["double:1.5", "int:1"],
        ),
        (json!({"range": {"value": {"gt": 1, "lt": 2}}}), vec!["double:1.5"]),
        (json!({"range": {"value": {"gt": 0.9, "lt": 1.1}}}), vec!["int:1"]),
        (
            json!({"exists": {"field": "value"}}),
            vec![
                "double:-0.0",
                "double:0.0",
                "double:1.5",
                "int:-1",
                "int:0",
                "int:1",
                "int:2",
            ],
        ),
    ] {
        let (status, answer) = search_tenant_a_index(&server, "metrics", query.clone())
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::OK, "{query}: {answer}");
        assert_eq!(
            read_sorted_source_texts(&answer, COL_METRIC_NAME).expect("names"),
            expected_names,
            "{query}"
        );
    }
    server.shutdown().await;
}

/// `describe metrics` lists the derived `value`, typed double, right after `value_int`: it is a
/// field of the index. A PPL field reference resolves stored columns only, so `fields value` is
/// the field-not-found refusal naming it, never an empty column.
#[tokio::test]
async fn ppl_describe_lists_the_metrics_value_and_a_ppl_reference_to_it_is_field_not_found() {
    let (server, catalog) = TestServer::start().await;
    commit_value_points(&catalog).await.expect("commit points");

    let describe_reply = server
        .post_ppl(TENANT_A, "/_plugins/_ppl", &json!({"query": "describe metrics"}))
        .await;
    let fields_reply = server
        .post_ppl(
            TENANT_A,
            "/_plugins/_ppl",
            &json!({"query": "source=metrics | fields value"}),
        )
        .await;

    assert_eq!(describe_reply.status, 200, "{}", describe_reply.text);
    let body = describe_reply.parse_json_body();
    let find_column = |name: &str| {
        body["schema"]
            .as_array()
            .expect("schema")
            .iter()
            .position(|column| column["name"] == name)
            .expect("a describe column of that name")
    };
    let (column_name, type_name) = (find_column("COLUMN_NAME"), find_column("TYPE_NAME"));
    let rows = body["datarows"].as_array().expect("datarows");
    let value_int_position = rows
        .iter()
        .position(|row| row[column_name] == COL_VALUE_INT)
        .expect("value_int is listed");
    assert_eq!(rows[value_int_position + 1][column_name], "value", "{body}");
    assert_eq!(rows[value_int_position + 1][type_name], "double", "{body}");
    assert_eq!(fields_reply.status, 400, "{}", fields_reply.text);
    let fields_body = fields_reply.parse_json_body();
    assert_eq!(fields_body["error"]["code"], "FIELD_NOT_FOUND", "{fields_body}");
    assert_eq!(fields_body["error"]["context"]["field"], "value", "{fields_body}");
    server.shutdown().await;
}

/// A full-text leaf treats a bucket, quantile or exemplar field like any `object` field and
/// matches nothing there, while it matches an enum name as an exact keyword and a list length as
/// a number. Tenant B's gauge would add a hit to the `GAUGE` match.
#[tokio::test]
async fn a_full_text_leaf_matches_nothing_on_a_render_only_field_and_reads_names_and_lengths() {
    let (server, catalog) = TestServer::start().await;
    commit_metric_shapes(&catalog).await.expect("commit metrics");

    for (query, expected_names) in [
        (json!({"match": {"quantiles": "12"}}), vec![]),
        (json!({"match": {"positiveBuckets.count": "3"}}), vec![]),
        (json!({"match": {"kind": "GAUGE"}}), vec!["queue.depth"]),
        (json!({"match": {"bucketCount": "3"}}), vec!["latency"]),
    ] {
        let (status, answer) = search_tenant_a_index(&server, SS4O_METRICS, query.clone())
            .await
            .expect("search request");

        assert_eq!(status, StatusCode::OK, "{query}: {answer}");
        assert_eq!(
            read_sorted_source_texts(&answer, "name").expect("names"),
            expected_names,
            "{query}"
        );
    }
    server.shutdown().await;
}

/// The histogram metric of [`commit_histogram_metrics`].
const HISTOGRAM_METRIC: &str = "http.server.duration";
/// The sum metric of [`commit_histogram_metrics`], one minute after the histogram.
const SUM_METRIC: &str = "http.server.requests";
/// Tenant B's histogram metric: a leak would list it, or add a hit.
const TENANT_B_HISTOGRAM_METRIC: &str = "rpc.server.duration";
/// One minute, in microseconds.
const MINUTE_MICROS: i64 = 60_000_000;

/// Tenant A's histogram at `T0` (bounds `[1.0, 5.0]`, counts `[2, 3, 4]`) and its sum one minute
/// later; tenant B's histogram at `T0`.
async fn commit_histogram_metrics(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    commit_rows(
        catalog,
        METRICS_TABLE,
        &[
            build_metric_row(
                TENANT_A,
                HISTOGRAM_METRIC,
                "histogram",
                vec![
                    (COL_AGGREGATION_TEMPORALITY, Cell::Text("DELTA")),
                    (COL_BUCKET_COUNTS, Cell::Longs(&[2, 3, 4])),
                    (COL_EXPLICIT_BOUNDS, Cell::Doubles(&[1.0, 5.0])),
                ],
            ),
            build_metric_row_at(
                TENANT_A,
                T0 + MINUTE_MICROS,
                SUM_METRIC,
                "sum",
                vec![
                    (COL_VALUE_INT, Cell::Long(7)),
                    (COL_AGGREGATION_TEMPORALITY, Cell::Text("CUMULATIVE")),
                ],
            ),
            build_metric_row(
                TENANT_B,
                TENANT_B_HISTOGRAM_METRIC,
                "histogram",
                vec![
                    (COL_BUCKET_COUNTS, Cell::Longs(&[1, 1])),
                    (COL_EXPLICIT_BOUNDS, Cell::Doubles(&[2.0])),
                ],
            ),
        ],
    )
    .await
}

/// The `_source` of every hit of a search of the ss4o metrics index with `body`, as tenant A.
async fn search_metric_sources(server: &TestServer, body: &Value) -> Vec<Value> {
    let answer = fetch_ok_answer(server, TENANT_A, &format!("/{SS4O_METRICS}/_search"), body)
        .await
        .expect("search request");
    answer["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["_source"].clone())
        .collect()
}

/// A sample-document search reads a histogram's buckets as `{min, max, count}` objects: bucket
/// `i` runs from bound `i - 1` to bound `i`, and an open end is the most negative or the most
/// positive finite `f32` widened to `f64`, as JSON holds no infinity. A row that is not a
/// histogram has no `buckets`, and a leaf on `buckets` or a path below it, outside a nested
/// context, matches nothing: the bucket objects are no fields of the row.
#[tokio::test]
async fn ss4o_metrics_renders_histogram_buckets_as_min_max_count_objects() {
    let (server, catalog) = TestServer::start().await;
    commit_histogram_metrics(&catalog).await.expect("commit metrics");
    let sample_body =
        |name: &str| json!({"size": 1, "query": {"bool": {"must": [{"term": {"name": {"value": name}}}]}}});

    let histogram_answer = server
        .request(Method::POST, Some(TENANT_A), &format!("/{SS4O_METRICS}/_search"))
        .json(&sample_body(HISTOGRAM_METRIC))
        .send()
        .await
        .expect("search request");
    assert_eq!(histogram_answer.status(), StatusCode::OK);
    let histogram_text = histogram_answer.text().await.expect("answer text");
    let sum_sources = search_metric_sources(&server, &sample_body(SUM_METRIC)).await;

    // Pinned on the wire: the test client's JSON parser rounds a 17-digit double by one unit in
    // the last place, which would hide a wrong open end.
    assert!(
        histogram_text.contains(
            r#""buckets":[{"min":-3.4028234663852886e+38,"max":1.0,"count":2},{"min":1.0,"max":5.0,"count":3},{"min":5.0,"max":3.4028234663852886e+38,"count":4}]"#
        ),
        "{histogram_text}"
    );
    assert_eq!(sum_sources.len(), 1, "{sum_sources:?}");
    assert!(sum_sources[0].get("buckets").is_none(), "{}", sum_sources[0]);
    for query in [
        json!({"term": {"buckets.min": 1.0}}),
        json!({"range": {"buckets.count": {"gte": 0}}}),
        json!({"exists": {"field": "buckets"}}),
        json!({"exists": {"field": "buckets.max"}}),
        json!({"term": {"buckets": 2}}),
    ] {
        let sources = search_metric_sources(&server, &json!({"query": query})).await;

        assert_eq!(sources, Vec::<Value>::new(), "{query}");
    }
    server.shutdown().await;
}

/// `time` is the data point's time under another name: a range on it selects the rows the same
/// range on `@timestamp` selects, with bounds written to the second, a dot, the milliseconds and
/// three zeros, and `Z`, and it is not repeated in `_source`. `T0` is `2025-10-09T08:53:20Z` and
/// the sum lies one minute later.
#[tokio::test]
async fn ss4o_metrics_time_is_the_data_point_time_and_filters_like_it() {
    let (server, catalog) = TestServer::start().await;
    commit_histogram_metrics(&catalog).await.expect("commit metrics");
    let window_body = |field: &str, end: &str| {
        json!({"query": {"bool": {"must": [
            {"range": {field: {"gte": "2025-10-09T08:53:20.000000Z", "lte": end}}}
        ]}}})
    };

    for (end, expected_names) in [
        ("2025-10-09T08:53:20.999000Z", vec![HISTOGRAM_METRIC]),
        ("2025-10-09T08:54:20.999000Z", vec![HISTOGRAM_METRIC, SUM_METRIC]),
    ] {
        for field in ["time", "@timestamp"] {
            let sources = search_metric_sources(&server, &window_body(field, end)).await;
            let mut names: Vec<&str> = sources.iter().map(|source| source["name"].as_str().expect("name")).collect();
            names.sort_unstable();

            assert_eq!(names, expected_names, "{field} up to {end}");
            assert!(sources.iter().all(|source| source.get("time").is_none()), "{sources:?}");
        }
    }
    server.shutdown().await;
}

/// `buckets` is a `nested` field whose objects hold a `double` `min` and `max` and a `long`
/// `count`, in `_field_caps` and in `_mapping`; `time` is a `date`.
#[tokio::test]
async fn ss4o_metrics_lists_buckets_as_a_nested_field() {
    let (server, catalog) = TestServer::start().await;
    commit_histogram_metrics(&catalog).await.expect("commit metrics");
    let capability = |field_type: &str, is_searchable: bool| json!({field_type: {"type": field_type, "searchable": is_searchable, "aggregatable": is_searchable}});

    let (caps_status, caps) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        &format!("/{SS4O_METRICS}/_field_caps?fields=buckets*,time"),
        None,
    )
    .await
    .expect("field caps");
    let (mapping_status, mapping) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        &format!("/{SS4O_METRICS}/_mapping"),
        None,
    )
    .await
    .expect("mapping");

    assert_eq!(caps_status, StatusCode::OK, "{caps}");
    assert_eq!(
        caps["fields"],
        json!({
            "buckets": capability("nested", false),
            "buckets.min": capability("double", true),
            "buckets.max": capability("double", true),
            "buckets.count": capability("long", true),
            "time": capability("date", true),
        })
    );
    assert_eq!(mapping_status, StatusCode::OK, "{mapping}");
    let properties = &mapping[SS4O_METRICS]["mappings"]["properties"];
    assert_eq!(
        properties["buckets"],
        json!({"type": "nested", "properties": {
            "min": {"type": "double"},
            "max": {"type": "double"},
            "count": {"type": "long"},
        }})
    );
    assert_eq!(properties["time"], json!({"type": "date"}));
    server.shutdown().await;
}

/// The metric-name request of OSD 3.9's Metrics page answers the histogram metrics
/// only: the sum is no `HISTOGRAM`, and tenant B's histogram is another tenant's.
#[tokio::test]
async fn osd_metric_names_list_only_histograms() {
    let (server, catalog) = TestServer::start().await;
    commit_histogram_metrics(&catalog).await.expect("commit metrics");
    let body = json!({
        "size": 0,
        "query": {"bool": {"filter": [{"term": {"kind": "HISTOGRAM"}}]}},
        "aggs": {"distinct_names": {"terms": {"field": "name", "size": 500}}}
    });

    let answer = fetch_ok_answer(&server, TENANT_A, &format!("/{SS4O_METRICS}/_search"), &body)
        .await
        .expect("search request");

    assert_eq!(
        answer["aggregations"]["distinct_names"]["buckets"],
        json!([{"key": HISTOGRAM_METRIC, "doc_count": 1}])
    );
    server.shutdown().await;
}

const TRACE_C: [u8; 16] = [0x0d; 16];
const TRACE_S: [u8; 16] = [0x0e; 16];
const LATE_CHILD_SPAN: [u8; 8] = [0x05; 8];
const NEAR_CHILD_SPAN: [u8; 8] = [0x06; 8];
const EDGE_CHILD_SPAN: [u8; 8] = [0x08; 8];
const TRACE_C_SPAN: [u8; 8] = [0x07; 8];
const TRACE_C_SPAN_HEX: &str = "0707070707070707";
const TRACE_S_HEX: &str = "0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e";

/// The hits' `_source`, keyed by span id.
fn collect_sources_by_span_id(answer: &Value) -> Result<BTreeMap<String, Value>, Box<dyn Error>> {
    answer["hits"]["hits"]
        .as_array()
        .ok_or_else(|| format!("no hits in {answer}"))?
        .iter()
        .map(|hit| {
            hit["_source"]["spanId"]
                .as_str()
                .map(|span_id| (span_id.to_string(), hit["_source"].clone()))
                .ok_or_else(|| format!("no spanId in {hit}").into())
        })
        .collect()
}

/// The cross-row fields of [`commit_three_spans`], derived by hand: trace A's root ends at
/// `T0 + 152 175 µs` (`…20.152175Z`), lasts `152 175 000 ns` and has status 2; trace B's lone
/// span is its own root, ends at `T0 + 2 000 + 500 µs` (`…20.002500Z`), lasts `500 000 ns`, and
/// its NULL status renders 0. Tenant B's root of trace A, `tenant-b-root`, starts at `T0` like
/// tenant A's, so a lookup leaking across tenants could name it. Each field filters, sorts and
/// aggregates, and `fields`/`docvalue_fields` naming one is refused: it renders in `_source` only.
#[tokio::test]
async fn trace_group_fields_come_from_the_root_span_of_the_same_trace() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    let answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"query": {"match_all": {}}}),
    )
    .await
    .expect("every span");
    let sources = collect_sources_by_span_id(&answer).expect("sources");
    assert_eq!(sources.len(), 3, "{answer}");
    let checkout = json!({
        "endTime": "2025-10-09T08:53:20.152175Z",
        "durationInNanos": 152_175_000,
        "statusCode": 2
    });
    for span in [ROOT_SPAN_HEX, CHILD_SPAN_HEX] {
        assert_eq!(sources[span]["traceGroup"], "GET /checkout", "{span}");
        assert_eq!(sources[span]["traceGroupFields"], checkout, "{span}");
    }
    assert_eq!(sources[OTHER_SPAN_HEX]["traceGroup"], "cron");
    assert_eq!(
        sources[OTHER_SPAN_HEX]["traceGroupFields"],
        json!({"endTime": "2025-10-09T08:53:20.002500Z", "durationInNanos": 500_000, "statusCode": 0})
    );

    assert_eq!(
        search_span_ids(&server, json!({"term": {"traceGroup": "GET /checkout"}}))
            .await
            .expect("by trace group"),
        [ROOT_SPAN_HEX, CHILD_SPAN_HEX]
    );
    assert_eq!(
        search_span_ids(&server, json!({"term": {"traceGroupFields.statusCode": "2"}}))
            .await
            .expect("by root status"),
        [ROOT_SPAN_HEX, CHILD_SPAN_HEX]
    );
    assert_eq!(
        search_span_ids(
            &server,
            json!({"range": {"traceGroupFields.durationInNanos": {"lt": 1_000_000}}})
        )
        .await
        .expect("by root duration"),
        [OTHER_SPAN_HEX]
    );

    let sorted_answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"sort": [{"traceGroupFields.durationInNanos": "asc"}]}),
    )
    .await
    .expect("sorted by root duration");
    let sorted_span_ids = read_span_ids(&sorted_answer).expect("span ids");
    assert_eq!(sorted_span_ids.len(), 3, "{sorted_answer}");
    assert_eq!(sorted_span_ids[0], OTHER_SPAN_HEX, "{sorted_answer}");
    // Both spans of trace A sort on their root's duration, so they tie; the sort leaves their
    // relative order open.
    let mut tied_span_ids = sorted_span_ids[1..].to_vec();
    tied_span_ids.sort();
    assert_eq!(tied_span_ids, [ROOT_SPAN_HEX, CHILD_SPAN_HEX], "{sorted_answer}");

    let grouped_answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"size": 0, "aggs": {"groups": {"terms": {"field": "traceGroup"}}}}),
    )
    .await
    .expect("grouped by trace group");
    assert_eq!(
        grouped_answer["aggregations"]["groups"]["buckets"],
        json!([{"key": "GET /checkout", "doc_count": 2}, {"key": "cron", "doc_count": 1}])
    );

    let (status, capabilities) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/otel-v1-apm-span/_field_caps?fields=traceGroup,traceGroupFields.endTime,traceGroupFields.durationInNanos,\
         traceGroupFields.statusCode",
        None,
    )
    .await
    .expect("field caps");
    assert_eq!(status, StatusCode::OK, "{capabilities}");
    let fields = &capabilities["fields"];
    let capability =
        |field_type: &str| json!({field_type: {"type": field_type, "searchable": true, "aggregatable": true}});
    assert_eq!(fields["traceGroup"], capability("keyword"));
    assert_eq!(fields["traceGroupFields.endTime"], capability("date"));
    assert_eq!(fields["traceGroupFields.durationInNanos"], capability("long"));
    assert_eq!(fields["traceGroupFields.statusCode"], capability("integer"));

    for (body, field) in [
        (json!({"fields": ["traceGroup"]}), "traceGroup"),
        (
            json!({"docvalue_fields": ["traceGroupFields.endTime"]}),
            "traceGroupFields.endTime",
        ),
    ] {
        let (status, error) = fetch_search_error(&server, &body).await.expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {error}");
        assert_eq!(error["type"], "illegal_argument_exception", "{body}");
        assert_eq!(error["field"], field, "{body}: {error}");
        assert_eq!(error["index"], SPAN_INDEX, "{body}: {error}");
    }
    server.shutdown().await;
}

/// One session answers both items. Their cross-row filters select disjoint traces: trace A's root
/// is `GET /checkout`, trace B's is `cron`, so a second item reading the first item's roots would
/// match nothing.
#[tokio::test]
async fn msearch_items_on_the_span_index_each_read_their_own_trace_roots() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");
    let ndjson = concat!(
        r#"{"index": "otel-v1-apm-span"}"#,
        "\n",
        r#"{"query": {"bool": {"filter": [{"term": {"traceId": "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a"}}], "must": [{"term": {"traceGroup": "GET /checkout"}}]}}}"#,
        "\n",
        r#"{"index": "otel-v1-apm-span"}"#,
        "\n",
        r#"{"query": {"bool": {"filter": [{"term": {"traceId": "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b"}}], "must": [{"term": {"traceGroup": "cron"}}]}}}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_msearch", ndjson.to_string())
        .await
        .expect("msearch request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    let items = answer["responses"].as_array().expect("responses");
    assert_eq!(items.len(), 2, "{answer}");
    assert_eq!(items[0]["status"], 200, "{answer}");
    assert_eq!(items[1]["status"], 200, "{answer}");
    assert_eq!(
        read_sorted_span_ids(&items[0]).expect("first item"),
        [ROOT_SPAN_HEX, CHILD_SPAN_HEX]
    );
    assert_eq!(read_sorted_span_ids(&items[1]).expect("second item"), [OTHER_SPAN_HEX]);
    assert_eq!(items[1]["hits"]["hits"][0]["_source"]["traceGroup"], "cron");
    server.shutdown().await;
}

/// With a 60 s lookback, a root at `T0` lies outside the window of a span starting at `T0 + 120 s`
/// (`[T0 + 60 s, T0 + 120 s]`), inside that of a span starting at `T0 + 30 s`, and on the inclusive
/// start of the window of a span starting at `T0 + 60 s`. Tenant B's root
/// of the same trace starts at `T0 + 100 s`, inside the late span's window, so a lookup leaking
/// across tenants would render it there.
#[tokio::test]
async fn a_root_outside_the_lookback_renders_the_fields_absent() {
    let config = OpenSearchConfig {
        max_trace_lookback_secs: 60,
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &ROOT_SPAN,
                (T0, 300_000_000),
                vec![(COL_NAME, Cell::Text("GET /checkout"))],
            ),
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &LATE_CHILD_SPAN,
                (T0 + 120_000_000, 1_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("late")),
                ],
            ),
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &NEAR_CHILD_SPAN,
                (T0 + 30_000_000, 1_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("near")),
                ],
            ),
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &EDGE_CHILD_SPAN,
                (T0 + 60_000_000, 1_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("edge")),
                ],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_A,
                &TENANT_B_SPAN,
                (T0 + 100_000_000, 1_000),
                vec![(COL_NAME, Cell::Text("tenant-b-root"))],
            ),
        ],
    )
    .await
    .expect("commit");

    let late_answer = fetch_tenant_a_span(&server, "0505050505050505").await.expect("the late span");
    let near_answer = fetch_tenant_a_span(&server, "0606060606060606").await.expect("the near span");
    let edge_answer = fetch_tenant_a_span(&server, "0808080808080808").await.expect("the edge span");

    let late_source = &late_answer["hits"]["hits"][0]["_source"];
    assert_eq!(late_source["name"], "late");
    assert!(late_source.get("traceGroup").is_none(), "{late_source}");
    assert!(late_source.get("traceGroupFields").is_none(), "{late_source}");
    let near_source = &near_answer["hits"]["hits"][0]["_source"];
    assert_eq!(near_source["name"], "near");
    assert_eq!(near_source["traceGroup"], "GET /checkout");
    let edge_source = &edge_answer["hits"]["hits"][0]["_source"];
    assert_eq!(edge_source["name"], "edge");
    assert_eq!(edge_source["traceGroup"], "GET /checkout");
    server.shutdown().await;
}

/// The answer of a tenant A search of the span index for the span `span_id`.
async fn fetch_tenant_a_span(server: &TestServer, span_id: &str) -> Result<Value, Box<dyn Error>> {
    fetch_ok_answer(
        server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"query": {"term": {"spanId": span_id}}}),
    )
    .await
}

/// [`commit_three_spans`], then tenant A's lone span of trace C (`batch`, at `T0 + 3 000 µs`,
/// 700 µs) and tenant B's roots of traces B (`tenant-b-cron`, at `T0 + 1 500 µs`, 100 µs) and C
/// (`tenant-b-batch`, at `T0 + 2 500 µs`, 100 µs). Each of these tenant B roots starts before
/// tenant A's root of the same trace, so a root lookup leaking across tenants would take it as the
/// earliest root.
async fn commit_three_traces(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    commit_three_spans(catalog).await?;
    commit_rows(
        catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT_A,
                &TRACE_C,
                &TRACE_C_SPAN,
                (T0 + 3_000, 700),
                vec![(COL_NAME, Cell::Text("batch"))],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_B,
                &TENANT_B_SPAN,
                (T0 + 1_500, 100),
                vec![(COL_NAME, Cell::Text("tenant-b-cron"))],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_C,
                &TENANT_B_SPAN,
                (T0 + 2_500, 100),
                vec![(COL_NAME, Cell::Text("tenant-b-batch"))],
            ),
        ],
    )
    .await
}

/// Tenant A holds three traces (A, B and C) and the limit is two, so a filter, a sort, an
/// aggregation or a `top_hits` sort on a cross-row field whose other filters keep every trace is
/// refused, naming the index and the limit; narrowed to two traces by the other filters, the same
/// cross-row filter answers.
#[tokio::test]
async fn a_filter_or_sort_on_a_cross_row_field_without_a_bounded_outer_query_is_a_400() {
    let config = OpenSearchConfig {
        max_cross_row_traces: 2,
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;
    commit_three_traces(&catalog).await.expect("commit three traces");

    for body in [
        json!({"query": {"term": {"traceGroup": "GET /checkout"}}}),
        json!({"sort": [{"traceGroupFields.durationInNanos": "desc"}]}),
        json!({"size": 0, "aggs": {"groups": {"terms": {"field": "traceGroup"}}}}),
        json!({"size": 0, "aggs": {"latest": {"top_hits": {"sort": [{"traceGroupFields.endTime": "desc"}]}}}}),
    ] {
        let (status, error) = fetch_search_error(&server, &body).await.expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {error}");
        assert_eq!(error["type"], "illegal_argument_exception", "{body}");
        assert_eq!(error["index"], SPAN_INDEX, "{body}");
        assert_eq!(error["limit"], 2, "{body}");
    }

    assert_eq!(
        search_span_ids(
            &server,
            json!({"bool": {
                "filter": [{"terms": {"traceId": [TRACE_A_HEX, TRACE_B_HEX]}}],
                "must": [{"term": {"traceGroup": "GET /checkout"}}]
            }})
        )
        .await
        .expect("two traces"),
        [ROOT_SPAN_HEX, CHILD_SPAN_HEX]
    );
    server.shutdown().await;
}

/// The limit is two and nothing in the request reads a cross-row field, so no roots are attached
/// and `_source` looks up the roots of all three traces of tenant A's four spans after paging, in
/// two chunks. Trace A's root is `GET /checkout`, trace B's `cron` and trace C's `batch`; tenant
/// B's earlier roots of traces B and C would rename the last two if a lookup leaked across tenants.
#[tokio::test]
async fn a_page_with_more_traces_than_max_cross_row_traces_renders_every_trace_group() {
    let config = OpenSearchConfig {
        max_cross_row_traces: 2,
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;
    commit_three_traces(&catalog).await.expect("commit three traces");

    let answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"query": {"match_all": {}}}),
    )
    .await
    .expect("every span");

    let trace_groups: BTreeMap<String, Value> = collect_sources_by_span_id(&answer)
        .expect("sources")
        .into_iter()
        .map(|(span_id, source)| (span_id, source["traceGroup"].clone()))
        .collect();
    assert_eq!(
        trace_groups,
        BTreeMap::from([
            (ROOT_SPAN_HEX.to_string(), json!("GET /checkout")),
            (CHILD_SPAN_HEX.to_string(), json!("GET /checkout")),
            (OTHER_SPAN_HEX.to_string(), json!("cron")),
            (TRACE_C_SPAN_HEX.to_string(), json!("batch")),
        ]),
        "{answer}"
    );
    server.shutdown().await;
}

/// The limit is two and a `terms` filter keeps traces A and B only, so a `top_hits` sorted on the
/// root duration answers. Trace B's root lasts `500 000 ns`, below trace A's `152 175 000 ns`, so
/// trace B's span ranks first and trace A's two spans follow it. Tenant B's root of trace B
/// (`tenant-b-cron`, `100 000 ns`) starts earlier than tenant A's, so a lookup leaking across
/// tenants would change the first hit.
#[tokio::test]
async fn a_top_hits_sorted_on_a_cross_row_field_under_a_bounded_query_ranks_by_the_root() {
    let config = OpenSearchConfig {
        max_cross_row_traces: 2,
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;
    commit_three_traces(&catalog).await.expect("commit three traces");

    let answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({
            "size": 0,
            "query": {"bool": {"filter": [{"terms": {"traceId": [TRACE_A_HEX, TRACE_B_HEX]}}]}},
            "aggs": {"fastest": {"top_hits": {"sort": [{"traceGroupFields.durationInNanos": "asc"}]}}}
        }),
    )
    .await
    .expect("top hits over two traces");

    let ranked_hits: Vec<Value> = answer["aggregations"]["fastest"]["hits"]["hits"]
        .as_array()
        .expect("top hits")
        .iter()
        .map(|hit| {
            json!({
                "spanId": hit["_source"]["spanId"],
                "traceGroup": hit["_source"]["traceGroup"],
                "sort": hit["sort"]
            })
        })
        .collect();
    assert_eq!(ranked_hits.len(), 3, "{answer}");
    assert_eq!(
        ranked_hits[0],
        json!({"spanId": OTHER_SPAN_HEX, "traceGroup": "cron", "sort": [500_000]}),
        "{answer}"
    );
    // Both spans of trace A sort on their root's duration, so they tie; the sort leaves their
    // relative order open.
    let mut tied_hits = ranked_hits[1..].to_vec();
    tied_hits.sort_by(|left, right| left["spanId"].as_str().cmp(&right["spanId"].as_str()));
    assert_eq!(
        tied_hits,
        [
            json!({"spanId": ROOT_SPAN_HEX, "traceGroup": "GET /checkout", "sort": [152_175_000]}),
            json!({"spanId": CHILD_SPAN_HEX, "traceGroup": "GET /checkout", "sort": [152_175_000]}),
        ],
        "{answer}"
    );
    server.shutdown().await;
}

/// With `max_cross_row_traces` at two, a `terms` on the trace id of size two answers the two
/// traces of tenant A as hex keys, most spans first (tenant B's root of trace A, leaking, would
/// count three), and size three is refused naming the aggregation and the limit.
#[tokio::test]
async fn terms_on_the_trace_id_at_max_cross_row_traces_is_accepted_and_one_more_is_a_400() {
    let config = OpenSearchConfig {
        max_cross_row_traces: 2,
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;
    commit_three_spans(&catalog).await.expect("commit spans");
    let build_terms_body =
        |size: u64| json!({"size": 0, "aggs": {"traces": {"terms": {"field": "traceId", "size": size}}}});

    let answer = fetch_ok_answer(&server, TENANT_A, SPAN_SEARCH_PATH, &build_terms_body(2))
        .await
        .expect("size at the limit");
    let (status, error) = fetch_search_error(&server, &build_terms_body(3)).await.expect("search request");

    assert_eq!(
        answer["aggregations"]["traces"]["buckets"],
        json!([{"key": TRACE_A_HEX, "doc_count": 2}, {"key": TRACE_B_HEX, "doc_count": 1}])
    );
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(error["type"], "illegal_argument_exception");
    assert_eq!(error["aggregation"], "traces");
    assert_eq!(error["limit"], 2);
    server.shutdown().await;
}

/// Trace S: tenant A has child spans only; tenant B has the roots. The rows of both tenants share
/// one Parquet commit and one WAL segment. Tenant B's earliest root of S is `tenant-b-root` (`T0`,
/// before its WAL root at `T0 + 500 µs`), and tenant A has none, so no span of tenant A renders a
/// trace group and no cross-row filter of tenant A matches.
#[tokio::test]
async fn trace_group_fields_never_read_another_tenants_root_span_sharing_the_trace_id() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT_A,
                &TRACE_S,
                &CHILD_SPAN,
                (T0 + 1_000, 1_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("tenant-a-child")),
                ],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_S,
                &ROOT_SPAN,
                (T0, 9_000),
                vec![(COL_NAME, Cell::Text("tenant-b-root")), (COL_STATUS_CODE, Cell::Int(2))],
            ),
        ],
    )
    .await
    .expect("commit");
    write_wal_segment(
        &server.wal_store,
        &catalog,
        SPANS_TABLE,
        1,
        &[
            build_span_row(
                TENANT_A,
                &TRACE_S,
                &OTHER_SPAN,
                (T0 + 2_000, 1_000),
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("tenant-a-wal-child")),
                ],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_S,
                &TENANT_B_SPAN,
                (T0 + 500, 9_000),
                vec![(COL_NAME, Cell::Text("tenant-b-wal-root"))],
            ),
        ],
    )
    .await
    .expect("WAL segment");

    let answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"query": {"term": {"traceId": TRACE_S_HEX}}}),
    )
    .await
    .expect("trace S");
    let sources = collect_sources_by_span_id(&answer).expect("sources");
    assert_eq!(
        sources.keys().map(String::as_str).collect::<Vec<_>>(),
        [CHILD_SPAN_HEX, OTHER_SPAN_HEX],
        "tenant A sees its two spans only: {answer}"
    );
    for source in sources.values() {
        assert!(source.get("traceGroup").is_none(), "{source}");
        assert!(source.get("traceGroupFields").is_none(), "{source}");
    }

    for query in [
        json!({"bool": {"filter": [{"term": {"traceId": TRACE_S_HEX}}], "must": [{"term": {"traceGroup": "tenant-b-root"}}]}}),
        json!({"bool": {"filter": [{"term": {"traceId": TRACE_S_HEX}}], "must": [{"term": {"traceGroupFields.statusCode": 2}}]}}),
        json!({"bool": {"filter": [{"term": {"traceId": TRACE_S_HEX}}], "must": [{"exists": {"field": "traceGroup"}}]}}),
    ] {
        let filtered_answer = fetch_ok_answer(&server, TENANT_A, SPAN_SEARCH_PATH, &json!({"query": query}))
            .await
            .expect("a cross-row filter");
        assert_eq!(
            filtered_answer["hits"]["total"]["value"], 0,
            "{query}: {filtered_answer}"
        );
    }

    let grouped_answer = fetch_ok_answer(
        &server,
        TENANT_A,
        SPAN_SEARCH_PATH,
        &json!({"size": 0, "aggs": {"groups": {"terms": {"field": "traceGroup"}}}}),
    )
    .await
    .expect("grouped by trace group");
    assert_eq!(grouped_answer["aggregations"]["groups"]["buckets"], json!([]));

    let tenant_b_answer = fetch_ok_answer(
        &server,
        TENANT_B,
        SPAN_SEARCH_PATH,
        &json!({"query": {"term": {"spanId": "0404040404040404"}}}),
    )
    .await
    .expect("tenant B's WAL root");
    assert_eq!(
        tenant_b_answer["hits"]["hits"][0]["_source"]["traceGroup"],
        "tenant-b-root"
    );
    server.shutdown().await;
}
