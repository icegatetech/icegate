//! The Data Prepper span profile over real HTTP. Every row is written with explicit values and
//! every expected document, field list and row set is stated here.
#![allow(clippy::expect_used)]

use std::error::Error;
use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::SPANS_TABLE;
use icegate_common::schema::{
    COL_ATTRIBUTES, COL_DROPPED_ATTRIBUTES_COUNT, COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_EVENTS,
    COL_INGESTED_TIMESTAMP, COL_KIND, COL_NAME, COL_PARENT_SPAN_ID, COL_RESOURCE_ATTRIBUTES, COL_SERVICE_NAME,
    COL_SPAN_ATTRIBUTES, COL_SPAN_ID, COL_STATUS_CODE, COL_TIMESTAMP, COL_TRACE_ID, COL_TRACE_STATE,
};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::{Cell, Row, RowWriteError, commit_rows};
use super::harness::TestServer;
use super::requests::send_request;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";
/// 2025-10-09T08:53:20Z.
const T0: i64 = 1_760_000_000_000_000;
const SPAN_INDEX: &str = "otel-v1-apm-span";
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

/// A span row: the required columns plus `cells`.
fn build_span_row<'a>(
    tenant: &'a str,
    trace: &'a [u8],
    span: &'a [u8],
    start: i64,
    duration: i64,
    mut cells: Vec<(&'a str, Cell<'a>)>,
) -> Row<'a> {
    let mut row_cells = vec![
        (COL_TRACE_ID, Cell::Bytes(trace)),
        (COL_SPAN_ID, Cell::Bytes(span)),
        (COL_TIMESTAMP, Cell::Micros(start)),
        (COL_END_TIMESTAMP, Cell::Micros(start + duration)),
        (COL_INGESTED_TIMESTAMP, Cell::Micros(start + duration)),
        (COL_DURATION_MICROS, Cell::Long(duration)),
    ];
    row_cells.append(&mut cells);
    Row {
        tenant,
        cells: row_cells,
    }
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
                T0,
                152_175,
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
                T0 + 1_000,
                50_000,
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
                T0 + 2_000,
                500,
                vec![(COL_NAME, Cell::Text("cron"))],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_A,
                &TENANT_B_SPAN,
                T0,
                900,
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

/// A wildcard reaching `spans` and `otel-v1-apm-span` reads one table through two profiles
/// without naming either index, so it is refused: the refusal names the expression, the table
/// and both profiles. The check runs before any row is read.
#[tokio::test]
async fn a_wildcard_reaching_two_profiles_of_spans_is_a_400_naming_them() {
    let (server, catalog) = TestServer::start().await;
    commit_three_spans(&catalog).await.expect("commit spans");

    for path in [
        "/*/_search",
        "/sp*,otel*/_search",
        "/spans,otel*/_search",
        "/otel*,s*/_search",
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
            assert_eq!(cause["table"], "spans", "{path}");
            assert_eq!(cause["profiles"], json!(["data_prepper", "native"]), "{path}");
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
        json!({SPAN_INDEX: {"mappings": {"startTime": {"full_name": "startTime", "mapping": {"startTime": {"type": "date"}}}}}})
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
                T0,
                152_175,
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
                T0,
                900,
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
            &json!({"query": format!("source={SPAN_INDEX} | head 1")}),
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
    assert_eq!(refused_reply.content_type, "text/plain; charset=UTF-8");
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
        start,
        1_000,
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
                T0,
                3_000,
                vec![(COL_NAME, Cell::Text("error"))],
            ),
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &CHILD_SPAN,
                T0 + 1_000,
                1_000,
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
                T0 + 2_000,
                1_000,
                vec![
                    (COL_PARENT_SPAN_ID, Cell::Bytes(&ROOT_SPAN)),
                    (COL_NAME, Cell::Text("cron")),
                ],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_B,
                &TENANT_B_SPAN,
                T0,
                1_000,
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
                T0,
                1_000,
                vec![
                    (COL_KIND, Cell::Int(2)),
                    (COL_TRACE_STATE, Cell::Text("Rojo=00f067aa0ba902b7")),
                ],
            ),
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &CHILD_SPAN,
                T0,
                1_000,
                vec![(COL_KIND, Cell::Int(3))],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_A,
                &TENANT_B_SPAN,
                T0,
                1_000,
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
