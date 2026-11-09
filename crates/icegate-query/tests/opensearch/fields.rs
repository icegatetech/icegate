//! Field discovery over real HTTP: `_field_caps`, `_mapping`, `_mapping/field`. Each request is a
//! literal, and each expected answer is stated here.

use icegate_common::schema::{
    COL_BODY, COL_LOG_ATTRIBUTES, COL_OBSERVED_TIMESTAMP, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES,
    COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID, logs_schema,
};
use icegate_query::opensearch::OpenSearchConfig;
use reqwest::Method;
use serde_json::{Value, json};

use super::data::{LogRow, commit_log_rows};
use super::harness::TestServer;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// 2025-10-09T08:53:20Z, a fixed instant older than the default discovery window, which the
/// listener anchors at the wall clock: rows written at it list their columns, never their MAP
/// keys.
const FIXED_TIMESTAMP_MICROS: i64 = 1_760_000_000_000_000;

const fn build_log_row<'a>(
    tenant_id: &'a str,
    resource_attributes: &'a [(&'a str, &'a str)],
    log_attributes: &'a [(&'a str, &'a str)],
) -> LogRow<'a> {
    LogRow {
        tenant_id,
        timestamp_micros: FIXED_TIMESTAMP_MICROS,
        service_name: Some("checkout"),
        severity_text: Some("INFO"),
        body: Some("payment accepted"),
        resource_attributes,
        log_attributes,
    }
}

/// One hour before the wall clock: inside the default discovery window, which the listener
/// anchors at the request time it reads from the clock.
fn build_recent_log_row<'a>(
    tenant_id: &'a str,
    resource_attributes: &'a [(&'a str, &'a str)],
    log_attributes: &'a [(&'a str, &'a str)],
) -> LogRow<'a> {
    LogRow {
        timestamp_micros: (chrono::Utc::now() - chrono::TimeDelta::hours(1)).timestamp_micros(),
        ..build_log_row(tenant_id, resource_attributes, log_attributes)
    }
}

/// The field name `<map column>.<stored key>`.
fn build_map_key_field_name(map_column: &str, key: &str) -> String {
    format!("{map_column}.{key}")
}

/// `method path` as `tenant`, with `body` as JSON when given: the status and the JSON body. The
/// error is a failed request or a body that is not JSON.
async fn send_request(
    server: &TestServer,
    method: Method,
    tenant: &str,
    path: &str,
    body: Option<&str>,
) -> reqwest::Result<(u16, Value)> {
    let mut request = server.request(method, Some(tenant), path);
    if let Some(body) = body {
        request = request.header("content-type", "application/json").body(body.to_string());
    }
    let response = request.send().await?;
    let status = response.status().as_u16();
    Ok((status, response.json().await?))
}

/// Rows of two tenants committed in one append, so the table is not empty; `tenant_id` is still
/// never a field, and every column reports its native type.
#[tokio::test]
async fn field_caps_reports_native_types_and_hides_tenant_id() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[
            build_log_row(TENANT_A, &[("host.name", "h1")], &[]),
            build_log_row(TENANT_B, &[("host.name", "h2")], &[]),
        ],
    )
    .await
    .expect("commit rows");

    let (status, body) = send_request(&server, Method::GET, TENANT_A, "/logs/_field_caps?fields=*", None)
        .await
        .expect("a JSON answer");

    assert_eq!(status, 200);
    assert_eq!(body["indices"], json!(["logs"]));
    let fields = &body["fields"];
    let text_capability = json!({"text": {"type": "text", "searchable": true, "aggregatable": false}});
    let keyword_capability = json!({"keyword": {"type": "keyword", "searchable": true, "aggregatable": true}});
    let date_capability = json!({"date": {"type": "date", "searchable": true, "aggregatable": true}});
    let object_capability = json!({"object": {"type": "object", "searchable": false, "aggregatable": false}});
    let expected = [
        (COL_BODY, &text_capability),
        (COL_SEVERITY_TEXT, &keyword_capability),
        (COL_SERVICE_NAME, &keyword_capability),
        (COL_TIMESTAMP, &date_capability),
        (COL_OBSERVED_TIMESTAMP, &date_capability),
        (COL_TRACE_ID, &keyword_capability),
        (COL_SPAN_ID, &keyword_capability),
        (COL_RESOURCE_ATTRIBUTES, &object_capability),
        (COL_LOG_ATTRIBUTES, &object_capability),
    ];
    for (name, capability) in expected {
        assert_eq!(&fields[name], capability, "{name}");
    }
    assert!(fields.get(COL_TENANT_ID).is_none());
    assert!(fields.get(format!("{COL_RESOURCE_ATTRIBUTES}.host.name")).is_none());
    server.shutdown().await;
}

/// An index-pattern-wizard-style call: POST, no body, `allow_no_indices=false`. When nothing
/// matches, the answer is the 404 envelope with `index_not_found_exception`.
#[tokio::test]
async fn field_caps_with_no_match_and_no_indices_disallowed_is_the_404_envelope() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/nope*/_field_caps?fields=*&ignore_unavailable=true&allow_no_indices=false",
        None,
    )
    .await
    .expect("a JSON answer");

    assert_eq!(status, 404);
    assert_eq!(body["status"], 404);
    assert_eq!(body["error"]["type"], "index_not_found_exception");
    assert_eq!(body["error"]["index"], "nope*");
    server.shutdown().await;
}

#[tokio::test]
async fn field_caps_accepts_an_empty_body_by_post_and_an_empty_object() {
    let (server, _catalog) = TestServer::start().await;

    let (no_body_status, no_body_answer) =
        send_request(&server, Method::POST, TENANT_A, "/logs/_field_caps?fields=*", None)
            .await
            .expect("a JSON answer");
    let (empty_object_status, empty_object_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_field_caps?fields=*",
        Some("{}"),
    )
    .await
    .expect("a JSON answer");

    assert_eq!((no_body_status, empty_object_status), (200, 200));
    assert_eq!(no_body_answer, empty_object_answer);
    assert_eq!(no_body_answer["indices"], json!(["logs"]));
    server.shutdown().await;
}

/// The `fields` parameter defaults to every field, the metadata fields included.
#[tokio::test]
async fn field_caps_without_a_fields_parameter_lists_every_field() {
    let (server, _catalog) = TestServer::start().await;

    let (default_status, default_answer) = send_request(&server, Method::GET, TENANT_A, "/logs/_field_caps", None)
        .await
        .expect("a JSON answer");
    let (star_status, star_answer) = send_request(&server, Method::GET, TENANT_A, "/logs/_field_caps?fields=*", None)
        .await
        .expect("a JSON answer");

    assert_eq!((default_status, star_status), (200, 200));
    assert_eq!(default_answer, star_answer);
    assert!(default_answer["fields"].get(COL_BODY).is_some());
    assert!(default_answer["fields"].get("_id").is_some());
    server.shutdown().await;
}

#[tokio::test]
async fn field_caps_rejects_a_body_key() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_field_caps?fields=*",
        Some(r#"{"index_filter":{"match_all":{}}}"#),
    )
    .await
    .expect("a JSON answer");

    assert_eq!(status, 400);
    assert_eq!(body["status"], 400);
    assert_eq!(body["error"]["type"], "parsing_exception");
    server.shutdown().await;
}

/// A body that does not parse, or parses to something other than an object, is refused like a
/// body key: there is no request to answer.
#[tokio::test]
async fn field_caps_rejects_a_body_that_is_not_a_json_object() {
    let (server, _catalog) = TestServer::start().await;

    for body in ["{", "[]"] {
        let (status, answer) = send_request(
            &server,
            Method::POST,
            TENANT_A,
            "/logs/_field_caps?fields=*",
            Some(body),
        )
        .await
        .expect("a JSON answer");

        assert_eq!(status, 400, "{body}");
        assert_eq!(answer["status"], 400, "{body}");
        assert_eq!(answer["error"]["type"], "parsing_exception", "{body}");
    }
    server.shutdown().await;
}

/// The limit is inclusive: a body of exactly `max_request_body_bytes` is read (an empty object
/// padded with spaces, so it is accepted), and one byte more is answered `413` in the envelope.
#[tokio::test]
async fn a_body_at_the_limit_is_read_and_one_byte_over_is_answered_413_in_the_envelope() {
    let (server, _catalog) = TestServer::start_with_config(OpenSearchConfig {
        max_request_body_bytes: 1024,
        ..OpenSearchConfig::default()
    })
    .await;
    let body_at_limit = format!("{{{}}}", " ".repeat(1022));
    let body_over_limit = format!("{{{}}}", " ".repeat(1023));
    assert_eq!((body_at_limit.len(), body_over_limit.len()), (1024, 1025));

    let (status_at_limit, _) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_field_caps",
        Some(&body_at_limit),
    )
    .await
    .expect("a JSON answer");
    let (status_over_limit, answer_over_limit) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_field_caps",
        Some(&body_over_limit),
    )
    .await
    .expect("a JSON answer");

    assert_eq!(status_at_limit, 200);
    assert_eq!(status_over_limit, 413);
    assert_eq!(answer_over_limit["status"], 413);
    assert_eq!(answer_over_limit["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

/// A time-field lookup in the style of a datasource health check: the response is keyed by the
/// index name as requested.
#[tokio::test]
async fn mapping_field_is_keyed_by_the_requested_index() {
    let (server, _catalog) = TestServer::start().await;

    for index in ["logs", "spans"] {
        let (status, body) = send_request(
            &server,
            Method::GET,
            TENANT_A,
            &format!("/{index}/_mapping/field/{COL_TIMESTAMP}"),
            None,
        )
        .await
        .expect("a JSON answer");

        assert_eq!(status, 200, "{index}");
        assert_eq!(
            body,
            json!({
                index: {
                    "mappings": {
                        COL_TIMESTAMP: {"full_name": COL_TIMESTAMP, "mapping": {COL_TIMESTAMP: {"type": "date"}}}
                    }
                }
            }),
            "{index}"
        );
    }
    server.shutdown().await;
}

/// A field lookup that matches no field answers the index with empty mappings, not an error.
#[tokio::test]
async fn mapping_field_of_an_unmatched_field_answers_empty_mappings() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = send_request(&server, Method::GET, TENANT_A, "/logs/_mapping/field/nope", None)
        .await
        .expect("a JSON answer");

    assert_eq!((status, body), (200, json!({"logs": {"mappings": {}}})));
    server.shutdown().await;
}

#[tokio::test]
async fn mapping_of_an_unmatched_wildcard_is_empty_and_of_a_missing_name_is_404() {
    let (server, _catalog) = TestServer::start().await;

    let (wildcard_status, wildcard_body) = send_request(&server, Method::GET, TENANT_A, "/nope*/_mapping", None)
        .await
        .expect("a JSON answer");
    let (missing_name_status, missing_name_body) = send_request(&server, Method::GET, TENANT_A, "/nope/_mapping", None)
        .await
        .expect("a JSON answer");

    assert_eq!((wildcard_status, wildcard_body), (200, json!({})));
    assert_eq!(missing_name_status, 404);
    assert_eq!(missing_name_body["error"]["type"], "index_not_found_exception");
    assert_eq!(missing_name_body["error"]["index"], "nope");
    server.shutdown().await;
}

/// `_field_caps` reports exactly three metadata fields, flagged with what IceGate serves: `_id` is
/// searchable through `ids` and not aggregatable; `_index` and `_source` are neither. `_seq_no`,
/// `_routing` and `_doc_count` are never reported. The request is index-pattern-wizard-style
/// (POST, no body); `fields=_*` selects the metadata fields alone.
#[tokio::test]
async fn field_caps_reports_exactly_the_id_index_and_source_metadata_fields() {
    let (server, _catalog) = TestServer::start().await;
    let expected = json!({
        "_id": {"_id": {"type": "_id", "searchable": true, "aggregatable": false}},
        "_index": {"_index": {"type": "_index", "searchable": false, "aggregatable": false}},
        "_source": {"_source": {"type": "_source", "searchable": false, "aggregatable": false}}
    });

    let (all_status, all_fields) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_field_caps?fields=*&ignore_unavailable=true&allow_no_indices=false",
        None,
    )
    .await
    .expect("a JSON answer");
    let (metadata_status, metadata_fields) =
        send_request(&server, Method::POST, TENANT_A, "/logs/_field_caps?fields=_*", None)
            .await
            .expect("a JSON answer");

    assert_eq!((all_status, metadata_status), (200, 200));
    let metadata_capabilities: serde_json::Map<String, Value> = all_fields["fields"]
        .as_object()
        .expect("fields")
        .iter()
        .filter(|(name, _)| name.starts_with('_'))
        .map(|(name, capability)| (name.clone(), capability.clone()))
        .collect();
    assert_eq!(Value::Object(metadata_capabilities), expected);
    assert_eq!(metadata_fields["fields"], expected);
    server.shutdown().await;
}

/// Without an index in the path, `_field_caps` (by GET and by POST) and `_mapping` select every
/// wildcard-visible index, `spans` and `otel-v1-apm-span` among them: one table reached through
/// two profiles without naming either index, which each endpoint refuses, naming the table and
/// its profiles.
#[tokio::test]
async fn field_caps_and_mapping_without_an_index_refuse_a_table_reached_through_two_profiles() {
    let (server, _catalog) = TestServer::start().await;

    for (method, path) in [
        (Method::GET, "/_field_caps?fields=_id"),
        (Method::POST, "/_field_caps?fields=_id"),
        (Method::GET, "/_mapping"),
    ] {
        let (status, body) = send_request(&server, method.clone(), TENANT_A, path, None)
            .await
            .expect("a JSON answer");

        assert_eq!(status, 400, "{method} {path}: {body}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception", "{method} {path}");
        assert_eq!(body["error"]["table"], "spans", "{method} {path}");
        let profiles = body["error"]["profiles"].as_array().expect("the profiles");
        for profile in ["native", "data_prepper"] {
            assert!(profiles.contains(&json!(profile)), "{method} {path}: {profiles:?}");
        }
    }
    server.shutdown().await;
}

/// An index segment that is not UTF-8 once percent-decoded (`%FF`) is refused in the envelope on
/// the routes whose index is optional, never read as an absent index that would select every
/// wildcard-visible one. The control request proves the route is served, so the `400` is the path
/// parameter refused, not an unmatched route.
#[tokio::test]
async fn field_caps_and_mapping_of_an_index_segment_that_is_not_utf8_are_the_400_envelope() {
    let (server, _catalog) = TestServer::start().await;

    let (control_status, _) = send_request(&server, Method::GET, TENANT_A, "/logs/_field_caps?fields=_id", None)
        .await
        .expect("a JSON answer");

    assert_eq!(control_status, 200);
    for path in ["/%FF/_field_caps?fields=_id", "/%FF/_mapping"] {
        let (status, body) = send_request(&server, Method::GET, TENANT_A, path, None)
            .await
            .expect("a JSON answer");

        assert_eq!(status, 400, "{path}");
        assert_eq!(body["status"], 400, "{path}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception", "{path}");
    }
    server.shutdown().await;
}

/// Ignoring an unknown parameter could silently change what a request means, so each field route
/// refuses one; a parameter the route deliberately ignores is accepted. The accepted request on
/// the same route proves the refusal is the route's own, not the unmatched-route `400`.
#[tokio::test]
async fn an_unknown_parameter_is_a_400_on_every_field_route_and_an_ignored_one_is_accepted() {
    let (server, _catalog) = TestServer::start().await;
    let cases = [
        (
            Method::GET,
            "/logs/_field_caps?fields=_id".to_string(),
            "/logs/_field_caps?fields=_id&unknown_knob=1".to_string(),
        ),
        (
            Method::POST,
            "/logs/_field_caps?fields=*".to_string(),
            "/logs/_field_caps?fields=*&unknown_knob=1".to_string(),
        ),
        (
            Method::GET,
            "/logs/_mapping?local=true".to_string(),
            "/logs/_mapping?unknown_knob=1".to_string(),
        ),
        (
            Method::GET,
            "/logs/_mapping?include_defaults=false&master_timeout=30s&cluster_manager_timeout=30s".to_string(),
            "/logs/_mapping?unknown_knob=1".to_string(),
        ),
        (
            Method::GET,
            format!("/logs/_mapping/field/{COL_TIMESTAMP}?local"),
            format!("/logs/_mapping/field/{COL_TIMESTAMP}?unknown_knob=1"),
        ),
    ];

    for (method, accepted_path, refused_path) in cases {
        let (accepted_status, _) = send_request(&server, method.clone(), TENANT_A, &accepted_path, None)
            .await
            .expect("a JSON answer");
        let (refused_status, refused_body) = send_request(&server, method, TENANT_A, &refused_path, None)
            .await
            .expect("a JSON answer");

        assert_eq!(accepted_status, 200, "{accepted_path}");
        assert_eq!(refused_status, 400, "{refused_path}");
        assert_eq!(
            refused_body["error"]["type"], "illegal_argument_exception",
            "{refused_path}"
        );
    }
    server.shutdown().await;
}

/// Index names match case-sensitively: `Logs` is an unknown index on both field endpoints.
#[tokio::test]
async fn field_caps_and_mapping_of_an_index_name_in_another_case_are_the_404_envelope() {
    let (server, _catalog) = TestServer::start().await;

    let (field_caps_status, field_caps_body) =
        send_request(&server, Method::GET, TENANT_A, "/Logs/_field_caps?fields=*", None)
            .await
            .expect("a JSON answer");
    let (mapping_status, mapping_body) = send_request(&server, Method::GET, TENANT_A, "/Logs/_mapping", None)
        .await
        .expect("a JSON answer");

    assert_eq!((field_caps_status, mapping_status), (404, 404));
    assert_eq!(field_caps_body["error"]["type"], "index_not_found_exception");
    assert_eq!(field_caps_body["error"]["index"], "Logs");
    assert_eq!(mapping_body["error"]["type"], "index_not_found_exception");
    assert_eq!(mapping_body["error"]["index"], "Logs");
    server.shutdown().await;
}

/// `_mapping` properties follow the table's schema column order, and a MAP column's keys follow
/// under its `properties` in stored-key byte order. Both orders are IceGate's contract. The
/// expected column order is the canonical `logs` schema without `tenant_id` (the tenant scope
/// hides it), read from `icegate_common::schema` rather than from the listener. The row stores
/// its keys in neither byte nor length order; byte order puts `Zone` (0x5A) before `http` (0x68),
/// where a case-folded order would put it last, so the expected list holds only under byte
/// order. The parsed objects keep the response's key order because icegate-query enables
/// `serde_json`'s `preserve_order`, which this test binary shares.
#[tokio::test]
async fn mapping_properties_follow_the_schema_column_order_and_map_keys_their_byte_order() {
    let (server, catalog) = TestServer::start().await;
    let log_attributes = [
        ("user.email", "a@example.com"),
        ("http.method", "GET"),
        ("Zone", "eu-1"),
        ("http", "x"),
    ];
    commit_log_rows(&catalog, &[build_recent_log_row(TENANT_A, &[], &log_attributes)])
        .await
        .expect("commit rows");

    let (status, body) = send_request(&server, Method::GET, TENANT_A, "/logs/_mapping", None)
        .await
        .expect("a JSON answer");

    assert_eq!(status, 200);
    let properties = &body["logs"]["mappings"]["properties"];
    let property_names: Vec<&str> = properties.as_object().expect("properties").keys().map(String::as_str).collect();
    let schema = logs_schema().expect("the logs schema");
    let expected: Vec<&str> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.as_str())
        .filter(|name| *name != COL_TENANT_ID)
        .collect();
    assert_eq!(property_names, expected);
    let map_keys: Vec<&str> = properties[COL_LOG_ATTRIBUTES]["properties"]
        .as_object()
        .expect("log_attributes properties")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(map_keys, ["Zone", "http", "http.method", "user.email"]);
    server.shutdown().await;
}

#[tokio::test]
async fn field_caps_lists_each_map_key_under_its_own_map_column() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[build_recent_log_row(
            TENANT_A,
            &[("res.only", "1")],
            &[("log.only", "2")],
        )],
    )
    .await
    .expect("commit rows");

    let (status, body) = send_request(&server, Method::GET, TENANT_A, "/logs/_field_caps?fields=*", None)
        .await
        .expect("a JSON answer");

    assert_eq!(status, 200);
    let fields = &body["fields"];
    let keyword_capability = json!({"keyword": {"type": "keyword", "searchable": true, "aggregatable": true}});
    assert_eq!(
        fields[&build_map_key_field_name(COL_RESOURCE_ATTRIBUTES, "res.only")],
        keyword_capability
    );
    assert_eq!(
        fields[&build_map_key_field_name(COL_LOG_ATTRIBUTES, "log.only")],
        keyword_capability
    );
    assert!(fields.get(build_map_key_field_name(COL_LOG_ATTRIBUTES, "res.only")).is_none());
    assert!(
        fields
            .get(build_map_key_field_name(COL_RESOURCE_ATTRIBUTES, "log.only"))
            .is_none()
    );
    assert!(fields.get(build_map_key_field_name(COL_SCOPE_ATTRIBUTES, "res.only")).is_none());
    server.shutdown().await;
}

/// The two tenants are committed in one append, into one snapshot (in separate partitions).
/// Partition pruning alone keeps each tenant's keys out of the other's listing.
#[tokio::test]
async fn field_caps_never_lists_another_tenants_map_keys() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[
            build_recent_log_row(TENANT_A, &[], &[("a.only", "1")]),
            build_recent_log_row(TENANT_B, &[], &[("b.only", "2")]),
        ],
    )
    .await
    .expect("commit rows");

    let (_, first_tenant_capabilities) =
        send_request(&server, Method::GET, TENANT_A, "/logs/_field_caps?fields=*", None)
            .await
            .expect("a JSON answer");
    let (_, second_tenant_capabilities) =
        send_request(&server, Method::GET, TENANT_B, "/logs/_field_caps?fields=*", None)
            .await
            .expect("a JSON answer");
    let (_, third_tenant_mappings) = send_request(&server, Method::GET, "tenant-c", "/logs/_mapping", None)
        .await
        .expect("a JSON answer");

    let a_only = build_map_key_field_name(COL_LOG_ATTRIBUTES, "a.only");
    let b_only = build_map_key_field_name(COL_LOG_ATTRIBUTES, "b.only");
    assert!(first_tenant_capabilities["fields"].get(&a_only).is_some());
    assert!(first_tenant_capabilities["fields"].get(&b_only).is_none());
    assert!(second_tenant_capabilities["fields"].get(&b_only).is_some());
    assert!(second_tenant_capabilities["fields"].get(&a_only).is_none());
    assert_eq!(
        third_tenant_mappings["logs"]["mappings"]["properties"][COL_LOG_ATTRIBUTES],
        json!({"type": "object"})
    );
    server.shutdown().await;
}

#[tokio::test]
async fn mapping_nests_map_keys_by_stored_key_under_their_column() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[build_recent_log_row(
            TENANT_A,
            &[],
            &[("http", "x"), ("http.method", "GET")],
        )],
    )
    .await
    .expect("commit rows");

    let (status, body) = send_request(&server, Method::GET, TENANT_A, "/logs/_mapping", None)
        .await
        .expect("a JSON answer");

    assert_eq!(status, 200);
    assert_eq!(
        body["logs"]["mappings"]["properties"][COL_LOG_ATTRIBUTES],
        json!({
            "type": "object",
            "properties": {"http": {"type": "keyword"}, "http.method": {"type": "keyword"}}
        })
    );
    server.shutdown().await;
}

/// A commit changes the table's snapshot, so the enumeration cached by the first listing is not
/// reused: the next listing holds the new key.
#[tokio::test]
async fn field_caps_lists_a_key_committed_after_an_earlier_listing() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(&catalog, &[build_recent_log_row(TENANT_A, &[], &[("first.key", "1")])])
        .await
        .expect("commit rows");
    let (_, body_before_commit) = send_request(&server, Method::GET, TENANT_A, "/logs/_field_caps?fields=*", None)
        .await
        .expect("a JSON answer");

    commit_log_rows(&catalog, &[build_recent_log_row(TENANT_A, &[], &[("second.key", "2")])])
        .await
        .expect("commit rows");
    let (_, body_after_commit) = send_request(&server, Method::GET, TENANT_A, "/logs/_field_caps?fields=*", None)
        .await
        .expect("a JSON answer");

    let first_key = build_map_key_field_name(COL_LOG_ATTRIBUTES, "first.key");
    let second_key = build_map_key_field_name(COL_LOG_ATTRIBUTES, "second.key");
    assert!(body_before_commit["fields"].get(&first_key).is_some());
    assert!(body_before_commit["fields"].get(&second_key).is_none());
    assert!(body_after_commit["fields"].get(&first_key).is_some());
    assert!(body_after_commit["fields"].get(&second_key).is_some());
    server.shutdown().await;
}
