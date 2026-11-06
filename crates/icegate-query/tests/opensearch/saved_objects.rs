//! System-index document endpoints over real HTTP. Every request is a literal and every expected
//! answer is stated in the test. The requests are saved-object-style: ids `<type>:<id>`,
//! `refresh=wait_for`, conditional updates.

use std::error::Error;
use std::fmt::Write as _;

use futures::future::join_all;
use icegate_common::load_config_file;
use icegate_query::opensearch::{OpenSearchConfig, SystemIndicesConfig};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::harness::TestServer;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// A counter-script-style source with its line breaks and indentation: a line break after the
/// opening backtick, 14- and 16-space indents, 12 spaces before the closing backtick.
const INCREMENT_COUNTER_SOURCE: &str = "\n              if (ctx._source[params.type][params.counterFieldName] == null) {\n                ctx._source[params.type][params.counterFieldName] = params.count;\n              }\n              else {\n                ctx._source[params.type][params.counterFieldName] += params.count;\n              }\n              ctx._source.updated_at = params.time;\n            ";

/// The import size the system-index bounds are sized for: two such imports and a reindexed copy of
/// both fit in `max_snapshot_bytes`.
const IMPORT_LIMIT_BYTES: usize = 26_214_400;

/// What a test returns: a failed request or body ends it with the cause.
type TestResult = Result<(), Box<dyn Error>>;

/// A strict saved-objects-style mapping: typed top-level properties, a dynamic
/// `migrationVersion` object, nested `references` and one object per saved-object type.
fn build_kibana_mappings() -> Value {
    json!({
        "dynamic": "strict",
        "properties": {
            "type": {"type": "keyword"},
            "namespace": {"type": "keyword"},
            "namespaces": {"type": "keyword"},
            "originId": {"type": "keyword"},
            "updated_at": {"type": "date"},
            "migrationVersion": {"dynamic": "true", "type": "object"},
            "references": {
                "type": "nested",
                "properties": {"name": {"type": "keyword"}, "type": {"type": "keyword"}, "id": {"type": "keyword"}}
            },
            "config": {"dynamic": "false", "properties": {"buildNum": {"type": "keyword"}}},
            "dashboard": {"properties": {"title": {"type": "text"}}},
            "index-pattern": {"properties": {"title": {"type": "text"}, "timeFieldName": {"type": "keyword"}}},
            "visualization": {"properties": {"title": {"type": "text"}, "visState": {"type": "text"}}},
            "ui-metric": {"properties": {"count": {"type": "integer"}}}
        },
        "_meta": {"migrationMappingPropertyHashes": {"dashboard": "8a1b3c"}}
    })
}

/// The `_shards` block of every document write answer.
fn build_write_shards() -> Value {
    json!({"total": 1, "successful": 1, "failed": 0})
}

/// Send `body` (or no body) and return the status and the parsed answer (`null` when empty).
async fn send_request(
    server: &TestServer,
    method: Method,
    tenant: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<(StatusCode, Value), Box<dyn Error>> {
    let request = server.request(method, Some(tenant), path);
    let request = match body {
        Some(body) => request.json(body),
        None => request,
    };
    let response = request.send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    let answer = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)?
    };
    Ok((status, answer))
}

/// Send an NDJSON body with `method` and an `application/x-ndjson` content type, and return the
/// status and the parsed answer.
async fn send_ndjson_with_method(
    server: &TestServer,
    method: Method,
    tenant: &str,
    path: &str,
    body: String,
) -> Result<(StatusCode, Value), reqwest::Error> {
    let response = server
        .request(method, Some(tenant), path)
        .header("content-type", "application/x-ndjson")
        .body(body)
        .send()
        .await?;
    let status = response.status();
    Ok((status, response.json::<Value>().await?))
}

/// [`send_ndjson_with_method`] with `POST`.
async fn send_ndjson(
    server: &TestServer,
    tenant: &str,
    path: &str,
    body: String,
) -> Result<(StatusCode, Value), reqwest::Error> {
    send_ndjson_with_method(server, Method::POST, tenant, path, body).await
}

/// Create `.kibana_1` with [`build_kibana_mappings`] and point the alias `.kibana` at it.
///
/// # Errors
///
/// Fails with the status and the body of an answer that is not a success.
async fn create_kibana_index(server: &TestServer, tenant: &str) -> TestResult {
    let index = json!({
        "mappings": build_kibana_mappings(),
        "settings": {"number_of_shards": 1, "auto_expand_replicas": "0-1"}
    });
    let (created_status, created_body) = send_request(server, Method::PUT, tenant, "/.kibana_1", Some(&index)).await?;
    if !created_status.is_success() {
        return Err(format!("creating .kibana_1 answered {created_status}: {created_body}").into());
    }
    let alias = json!({"actions": [{"add": {"index": ".kibana_1", "alias": ".kibana"}}]});
    let (aliased_status, aliased_body) = send_request(server, Method::POST, tenant, "/_aliases", Some(&alias)).await?;
    if !aliased_status.is_success() {
        return Err(format!("aliasing .kibana answered {aliased_status}: {aliased_body}").into());
    }
    Ok(())
}

#[tokio::test]
async fn a_document_round_trips_create_get_update_delete_with_seq_no_and_primary_term() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}, "references": [], "updated_at": "2026-10-08T12:00:00.000Z"});

    let (status, create_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana/_create/dashboard:a?refresh=wait_for",
        Some(&source),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        create_answer,
        json!({"_index": ".kibana_1", "_id": "dashboard:a", "result": "created", "_shards": build_write_shards(), "_seq_no": 0, "_primary_term": 1})
    );

    let (status, read_answer) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:a", None).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        read_answer,
        json!({"_index": ".kibana_1", "_id": "dashboard:a", "_seq_no": 0, "_primary_term": 1, "found": true, "_source": source})
    );

    let update = json!({"doc": {"dashboard": {"title": "Errors"}, "updated_at": "2026-10-08T13:00:00.000Z"}});
    let update_path = "/.kibana/_update/dashboard:a?if_seq_no=0&if_primary_term=1&refresh=wait_for&_source_includes=namespace,namespaces,originId";
    let (status, update_answer) = send_request(&server, Method::POST, TENANT_A, update_path, Some(&update)).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        update_answer,
        json!({
            "_index": ".kibana_1", "_id": "dashboard:a", "result": "updated", "_shards": build_write_shards(),
            "_seq_no": 1, "_primary_term": 1,
            "get": {"_seq_no": 1, "_primary_term": 1, "found": true, "_source": {}}
        })
    );

    let (status, stale_answer) = send_request(&server, Method::POST, TENANT_A, update_path, Some(&update)).await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(stale_answer["status"], 409);
    assert_eq!(stale_answer["error"]["type"], "version_conflict_engine_exception");

    let replacement = json!({"type": "dashboard", "dashboard": {"title": "Traces"}, "references": [], "updated_at": "2026-10-08T14:00:00.000Z"});
    let (status, overwrite_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana/_doc/dashboard:a?if_seq_no=1&if_primary_term=1&refresh=wait_for",
        Some(&replacement),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        overwrite_answer,
        json!({"_index": ".kibana_1", "_id": "dashboard:a", "result": "updated", "_shards": build_write_shards(), "_seq_no": 2, "_primary_term": 1})
    );

    let (_, reread_answer) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:a", None).await?;
    assert_eq!(reread_answer["_source"], replacement);
    assert_eq!(reread_answer["_seq_no"], 2);

    let (status, delete_answer) = send_request(
        &server,
        Method::DELETE,
        TENANT_A,
        "/.kibana/_doc/dashboard:a?refresh=wait_for",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        delete_answer,
        json!({"_index": ".kibana_1", "_id": "dashboard:a", "result": "deleted", "_shards": build_write_shards(), "_seq_no": 3, "_primary_term": 1})
    );

    let (status, missing_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:a", None).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        missing_answer,
        json!({"_index": ".kibana_1", "_id": "dashboard:a", "found": false})
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_percent_encoded_id_is_decoded_in_the_path() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "config", "config": {"buildNum": "9000"}, "updated_at": "2026-10-08T12:00:00.000Z"});

    let (status, create_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana/_create/config%3A3.9.0?refresh=wait_for",
        Some(&source),
    )
    .await?;
    let (decoded_status, decoded_read_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/config:3.9.0", None).await?;
    let (encoded_status, encoded_read_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/config%3A3.9.0", None).await?;
    let (workspace_status, workspace_answer) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/.kibana/_doc/config%3A%3Ccurrent_workspace%3E_3.9.0",
        None,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(create_answer["_id"], "config:3.9.0");
    assert_eq!(
        (
            decoded_status,
            &decoded_read_answer["_id"],
            &decoded_read_answer["_source"]
        ),
        (StatusCode::OK, &json!("config:3.9.0"), &source)
    );
    assert_eq!(
        (encoded_status, &encoded_read_answer["_id"]),
        (StatusCode::OK, &json!("config:3.9.0"))
    );
    assert_eq!(workspace_status, StatusCode::NOT_FOUND);
    assert_eq!(
        workspace_answer,
        json!({"_index": ".kibana_1", "_id": "config:<current_workspace>_3.9.0", "found": false})
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn get_of_a_missing_document_is_404_found_false() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;

    let (status, body) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:missing", None).await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body,
        json!({"_index": ".kibana_1", "_id": "dashboard:missing", "found": false})
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn delete_of_a_missing_document_answers_not_found() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;

    let (status, body) = send_request(
        &server,
        Method::DELETE,
        TENANT_A,
        "/.kibana/_doc/dashboard:missing?refresh=wait_for",
        None,
    )
    .await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body,
        json!({"_index": ".kibana_1", "_id": "dashboard:missing", "result": "not_found", "_shards": build_write_shards(), "_seq_no": 0, "_primary_term": 1})
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_delete_with_a_stale_condition_is_a_version_conflict_and_keeps_the_document() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}});
    let (status, create_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana/_create/dashboard:a",
        Some(&source),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{create_answer}");

    let (stale_status, stale_answer) = send_request(
        &server,
        Method::DELETE,
        TENANT_A,
        "/.kibana/_doc/dashboard:a?if_seq_no=7&if_primary_term=1",
        None,
    )
    .await?;
    let (read_status, read_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:a", None).await?;
    let (matching_status, matching_answer) = send_request(
        &server,
        Method::DELETE,
        TENANT_A,
        "/.kibana/_doc/dashboard:a?if_seq_no=0&if_primary_term=1",
        None,
    )
    .await?;

    assert_eq!(stale_status, StatusCode::CONFLICT);
    assert_eq!(stale_answer["error"]["type"], "version_conflict_engine_exception");
    assert_eq!((read_status, &read_answer["_source"]), (StatusCode::OK, &source));
    assert_eq!(matching_status, StatusCode::OK);
    assert_eq!(matching_answer["result"], "deleted");
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_write_to_a_data_index_is_refused_403_cluster_block() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    let source = json!({"body": "not a saved object"});
    let update = json!({"doc": {"body": "x"}});
    let requests = [
        (Method::PUT, "/logs/_doc/1", Some(&source)),
        (Method::POST, "/logs/_doc/1", Some(&source)),
        (Method::PUT, "/logs/_create/1", Some(&source)),
        (Method::POST, "/logs/_update/1", Some(&update)),
        (Method::DELETE, "/logs/_doc/1", None),
    ];

    for (method, path, body) in requests {
        let (status, answer) = send_request(&server, method.clone(), TENANT_A, path, body).await?;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}");
        assert_eq!(answer["status"], 403, "{method} {path}");
        assert_eq!(answer["error"]["type"], "cluster_block_exception", "{method} {path}");
    }
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn get_of_a_data_index_document_is_refused_400() -> TestResult {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = send_request(&server, Method::GET, TENANT_A, "/logs/_doc/1", None).await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
    Ok(())
}

/// A write creates no index: the system index has to exist first.
#[tokio::test]
async fn a_write_to_a_missing_system_index_is_index_not_found() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}});

    let (status, body) = send_request(&server, Method::PUT, TENANT_A, "/.kibana_9/_doc/x", Some(&source)).await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "index_not_found_exception");
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn doc_with_op_type_create_on_an_existing_id_is_a_version_conflict() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}});
    let path = "/.kibana/_doc/dashboard:a?op_type=create";

    let (first_status, first_answer) = send_request(&server, Method::PUT, TENANT_A, path, Some(&source)).await?;
    let (second_status, second_answer) = send_request(&server, Method::PUT, TENANT_A, path, Some(&source)).await?;

    assert_eq!(
        (first_status, &first_answer["result"]),
        (StatusCode::CREATED, &json!("created"))
    );
    assert_eq!(second_status, StatusCode::CONFLICT);
    assert_eq!(second_answer["error"]["type"], "version_conflict_engine_exception");
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn an_op_type_the_endpoint_does_not_take_is_refused_400() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}});

    for path in ["/.kibana/_create/x?op_type=index", "/.kibana/_doc/x?op_type=upsert"] {
        let (status, body) = send_request(&server, Method::PUT, TENANT_A, path, Some(&source)).await?;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception", "{path}");
    }
    let (status, _) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/x", None).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    server.shutdown().await;
    Ok(())
}

/// Naming an endpoint's own `op_type` is the request its default makes: `index` on `_doc` creates
/// and then replaces, `create` on `_create` creates and then conflicts.
#[tokio::test]
async fn an_op_type_the_endpoint_takes_writes_as_its_default_does() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}});
    let accepted_writes = [
        ("/.kibana/_doc/y?op_type=index", StatusCode::CREATED, "created", 0),
        ("/.kibana/_doc/y?op_type=index", StatusCode::OK, "updated", 1),
        ("/.kibana/_create/z?op_type=create", StatusCode::CREATED, "created", 2),
    ];

    for (path, expected_status, expected_result, expected_seq_no) in accepted_writes {
        let (status, answer) = send_request(&server, Method::PUT, TENANT_A, path, Some(&source)).await?;

        assert_eq!(status, expected_status, "{path}: {answer}");
        assert_eq!(answer["result"], expected_result, "{path}");
        assert_eq!(answer["_seq_no"], expected_seq_no, "{path}");
    }
    let (conflict_status, conflict_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana/_create/z?op_type=create",
        Some(&source),
    )
    .await?;
    assert_eq!(conflict_status, StatusCode::CONFLICT);
    assert_eq!(conflict_answer["error"]["type"], "version_conflict_engine_exception");
    server.shutdown().await;
    Ok(())
}

/// Every refused request is checked at the transport boundary: the query string and the body
/// reach the same refusal the unit tests pin, and none of them writes the document.
#[tokio::test]
async fn a_document_request_that_cannot_apply_is_refused_400_and_writes_nothing() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}});
    let update = json!({"doc": {"dashboard": {"title": "Errors"}}});
    let other_script = json!({"script": {"source": "ctx._source.count += 1", "lang": "painless"}});
    let not_an_object = json!([1]);
    let no_change = json!({});
    let unknown_key = json!({"doc": {"dashboard": {"title": "Errors"}}, "doc_as_upsert": true});
    let cases = [
        (Method::PUT, "/.kibana/_doc/x", &not_an_object, "parsing_exception"),
        (Method::POST, "/.kibana/_update/x", &no_change, "parsing_exception"),
        (Method::POST, "/.kibana/_update/x", &unknown_key, "parsing_exception"),
        (
            Method::POST,
            "/.kibana/_update/x",
            &other_script,
            "illegal_argument_exception",
        ),
        (
            Method::PUT,
            "/.kibana/_doc/x?if_seq_no=0",
            &source,
            "illegal_argument_exception",
        ),
        (
            Method::PUT,
            "/.kibana/_doc/x?if_seq_no=abc&if_primary_term=1",
            &source,
            "illegal_argument_exception",
        ),
        (
            Method::PUT,
            "/.kibana/_create/x?if_seq_no=0&if_primary_term=1",
            &source,
            "illegal_argument_exception",
        ),
        (
            Method::PUT,
            "/.kibana/_doc/x?op_type=create&if_seq_no=0&if_primary_term=1",
            &source,
            "illegal_argument_exception",
        ),
        (
            Method::POST,
            "/.kibana/_update/x?if_primary_term=1",
            &update,
            "illegal_argument_exception",
        ),
    ];

    for (method, path, body, error_type) in cases {
        let (status, answer) = send_request(&server, method.clone(), TENANT_A, path, Some(body)).await?;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path} {body}");
        assert_eq!(answer["error"]["type"], error_type, "{method} {path} {body}");
    }
    let (status, _) = send_request(
        &server,
        Method::DELETE,
        TENANT_A,
        "/.kibana/_doc/x?if_primary_term=1",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, read_answer) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/x", None).await?;
    assert_eq!((status, &read_answer["found"]), (StatusCode::NOT_FOUND, &json!(false)));
    server.shutdown().await;
    Ok(())
}

/// Each parameter struct is exercised both ways: `unknown_knob` is refused on every route, and
/// one parameter of its ignored list is accepted, on a route that then answers as usual.
#[tokio::test]
async fn every_document_endpoint_refuses_an_unknown_parameter_and_accepts_an_ignored_one() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}});
    let update = json!({"doc": {"dashboard": {"title": "Errors"}}});
    let refused_requests = [
        (Method::PUT, "/.kibana/_create/x?unknown_knob=1", Some(&source)),
        (Method::PUT, "/.kibana/_doc/x?unknown_knob=1", Some(&source)),
        (Method::GET, "/.kibana/_doc/x?unknown_knob=1", None),
        (Method::DELETE, "/.kibana/_doc/x?unknown_knob=1", None),
        (Method::POST, "/.kibana/_update/x?unknown_knob=1", Some(&update)),
    ];
    let accepted_requests = [
        (
            Method::PUT,
            "/.kibana/_doc/x?refresh=wait_for",
            Some(&source),
            StatusCode::CREATED,
        ),
        (Method::GET, "/.kibana/_doc/x?realtime=true", None, StatusCode::OK),
        (
            Method::POST,
            "/.kibana/_update/x?retry_on_conflict=3",
            Some(&update),
            StatusCode::OK,
        ),
        (Method::DELETE, "/.kibana/_doc/x?timeout=1m", None, StatusCode::OK),
        (
            Method::POST,
            "/.kibana/_doc/y?wait_for_active_shards=1",
            Some(&source),
            StatusCode::CREATED,
        ),
    ];

    for (method, path, body) in refused_requests {
        let (status, answer) = send_request(&server, method.clone(), TENANT_A, path, body).await?;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}");
        assert_eq!(answer["error"]["type"], "illegal_argument_exception", "{method} {path}");
    }
    for (method, path, body, expected_status) in accepted_requests {
        let (status, answer) = send_request(&server, method.clone(), TENANT_A, path, body).await?;

        assert_eq!(status, expected_status, "{method} {path}: {answer}");
    }
    server.shutdown().await;
    Ok(())
}

/// `get` is opt-in: an update that does not ask for it, or asks not to have it, answers the write
/// alone, and `_source=true` adds the written source. Each row changes the title, so each answer
/// is a real update and not a noop.
#[tokio::test]
async fn an_update_answers_no_get_unless_a_parameter_asks_for_it() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}});
    let (status, create_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana/_create/dashboard:a",
        Some(&source),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{create_answer}");
    let cases = [
        (
            "/.kibana/_update/dashboard:a",
            "Errors",
            json!({
                "_index": ".kibana_1", "_id": "dashboard:a", "result": "updated", "_shards": build_write_shards(),
                "_seq_no": 1, "_primary_term": 1
            }),
        ),
        (
            "/.kibana/_update/dashboard:a?_source=false",
            "Traces",
            json!({
                "_index": ".kibana_1", "_id": "dashboard:a", "result": "updated", "_shards": build_write_shards(),
                "_seq_no": 2, "_primary_term": 1
            }),
        ),
        (
            "/.kibana/_update/dashboard:a?_source=true",
            "Metrics",
            json!({
                "_index": ".kibana_1", "_id": "dashboard:a", "result": "updated", "_shards": build_write_shards(),
                "_seq_no": 3, "_primary_term": 1,
                "get": {"_seq_no": 3, "_primary_term": 1, "found": true,
                        "_source": {"type": "dashboard", "dashboard": {"title": "Metrics"}}}
            }),
        ),
    ];

    for (path, title, expected_answer) in cases {
        let update = json!({"doc": {"dashboard": {"title": title}}});

        let (status, answer) = send_request(&server, Method::POST, TENANT_A, path, Some(&update)).await?;

        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(answer, expected_answer, "{path}");
    }
    server.shutdown().await;
    Ok(())
}

/// A merge that changes nothing keeps the document's `_seq_no` and still answers `get`.
#[tokio::test]
async fn an_update_that_changes_nothing_answers_noop_with_its_seq_no() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}});
    let (status, create_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana/_create/dashboard:a",
        Some(&source),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{create_answer}");

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_update/dashboard:a?_source=true",
        Some(&json!({"doc": {"dashboard": {"title": "Logs"}}})),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        answer,
        json!({
            "_index": ".kibana_1", "_id": "dashboard:a", "result": "noop", "_shards": build_write_shards(),
            "_seq_no": 0, "_primary_term": 1,
            "get": {"_seq_no": 0, "_primary_term": 1, "found": true, "_source": source}
        })
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn an_increment_counter_update_upserts_then_adds_to_the_counter() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let path = "/.kibana/_update/ui-metric:console:opened?refresh=wait_for&_source=true";
    let build_increment = |count: i64, time: &str| {
        json!({
            "script": {
                "source": INCREMENT_COUNTER_SOURCE,
                "lang": "painless",
                "params": {"count": count, "time": time, "type": "ui-metric", "counterFieldName": "count"}
            },
            "upsert": {"type": "ui-metric", "ui-metric": {"count": count}, "updated_at": time}
        })
    };

    let (first_status, first_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        path,
        Some(&build_increment(1, "2026-10-08T12:00:00.000Z")),
    )
    .await?;
    let (second_status, second_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        path,
        Some(&build_increment(2, "2026-10-08T13:00:00.000Z")),
    )
    .await?;

    assert_eq!(first_status, StatusCode::CREATED);
    assert_eq!(
        first_answer,
        json!({
            "_index": ".kibana_1", "_id": "ui-metric:console:opened", "result": "created", "_shards": build_write_shards(),
            "_seq_no": 0, "_primary_term": 1,
            "get": {"_seq_no": 0, "_primary_term": 1, "found": true,
                    "_source": {"type": "ui-metric", "ui-metric": {"count": 1}, "updated_at": "2026-10-08T12:00:00.000Z"}}
        })
    );
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(second_answer["result"], "updated");
    assert_eq!(second_answer["_seq_no"], 1);
    assert_eq!(
        second_answer["get"]["_source"],
        json!({"type": "ui-metric", "ui-metric": {"count": 3}, "updated_at": "2026-10-08T13:00:00.000Z"})
    );
    server.shutdown().await;
    Ok(())
}

/// Eight writers race on one listener, whose write loop coalesces them into shared
/// compare-and-swaps: the first to apply stores the upsert and the rest add to it, in whatever
/// order they queue, so only the final count and each writer's success are asserted.
#[tokio::test]
async fn concurrent_increment_counters_on_one_listener_end_at_exactly_n() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let path = "/.kibana/_update/ui-metric:console:opened?refresh=wait_for";
    let increment = json!({
        "script": {
            "source": INCREMENT_COUNTER_SOURCE,
            "lang": "painless",
            "params": {"count": 1, "time": "2026-10-08T12:00:00.000Z", "type": "ui-metric", "counterFieldName": "count"}
        },
        "upsert": {"type": "ui-metric", "ui-metric": {"count": 1}, "updated_at": "2026-10-08T12:00:00.000Z"}
    });

    let writers = (0..8).map(|_| send_request(&server, Method::POST, TENANT_A, path, Some(&increment)));
    let outcomes = join_all(writers).await;

    for outcome in outcomes {
        let (status, answer) = outcome?;
        assert!(status.is_success(), "{status} {answer}");
    }
    let (status, read_answer) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/.kibana/_doc/ui-metric:console:opened",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(read_answer["_source"]["ui-metric"]["count"], 8);
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn every_document_endpoint_under_tenant_b_sees_none_of_tenant_a_documents() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    create_kibana_index(&server, TENANT_B).await?;
    let source_a = json!({"type": "dashboard", "dashboard": {"title": "Tenant A"}});
    for id in ["dashboard:a", "dashboard:b"] {
        let path = format!("/.kibana/_create/{id}");
        let (status, body) = send_request(&server, Method::PUT, TENANT_A, &path, Some(&source_a)).await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    let (get_status, get_answer) =
        send_request(&server, Method::GET, TENANT_B, "/.kibana/_doc/dashboard:a", None).await?;
    let (delete_status, delete_answer) =
        send_request(&server, Method::DELETE, TENANT_B, "/.kibana/_doc/dashboard:a", None).await?;
    let (update_status, update_answer) = send_request(
        &server,
        Method::POST,
        TENANT_B,
        "/.kibana/_update/dashboard:a",
        Some(&json!({"doc": {"dashboard": {"title": "B"}}})),
    )
    .await?;
    let source_b = json!({"type": "dashboard", "dashboard": {"title": "Tenant B"}});
    let (create_status, create_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_B,
        "/.kibana/_create/dashboard:a",
        Some(&source_b),
    )
    .await?;
    let (index_status, index_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_B,
        "/.kibana/_doc/dashboard:b",
        Some(&source_b),
    )
    .await?;
    let (unknown_status, unknown_tenant_answer) =
        send_request(&server, Method::GET, "tenant-c", "/.kibana/_doc/dashboard:a", None).await?;
    let (a_status, a_read_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:a", None).await?;
    let (second_a_status, second_a_read_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:b", None).await?;

    assert_eq!(
        (get_status, get_answer["found"].clone()),
        (StatusCode::NOT_FOUND, json!(false))
    );
    assert_eq!(
        (delete_status, delete_answer["result"].clone()),
        (StatusCode::NOT_FOUND, json!("not_found"))
    );
    assert_eq!(
        (update_status, update_answer["error"]["type"].clone()),
        (StatusCode::NOT_FOUND, json!("document_missing_exception"))
    );
    // Tenant B's delete took sequence number 0 of its own index, so its create takes 1.
    assert_eq!(
        (create_status, create_answer["_seq_no"].clone()),
        (StatusCode::CREATED, json!(1))
    );
    // Created, not replaced: tenant A's document of that id is not visible to the write.
    assert_eq!(
        (index_status, index_answer["_seq_no"].clone()),
        (StatusCode::CREATED, json!(2))
    );
    assert_eq!(
        (unknown_status, unknown_tenant_answer["error"]["type"].clone()),
        (StatusCode::NOT_FOUND, json!("index_not_found_exception"))
    );
    assert_eq!(a_status, StatusCode::OK);
    assert_eq!(
        (a_read_answer["_source"].clone(), a_read_answer["_seq_no"].clone()),
        (source_a.clone(), json!(0))
    );
    assert_eq!(second_a_status, StatusCode::OK);
    assert_eq!(
        (
            second_a_read_answer["_source"].clone(),
            second_a_read_answer["_seq_no"].clone()
        ),
        (source_a, json!(1))
    );
    server.shutdown().await;
    Ok(())
}

/// `_cat/indices` reports a system index's live document count per tenant: tenant A creates
/// three documents and deletes one, tenant B's own `.kibana_1` holds none.
#[tokio::test]
async fn cat_indices_reports_each_tenants_system_document_count() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    create_kibana_index(&server, TENANT_B).await?;
    for id in ["dashboard:a", "dashboard:b", "dashboard:c"] {
        let source = json!({"type": "dashboard", "dashboard": {"title": id}});
        let path = format!("/.kibana/_create/{id}");
        let (status, body) = send_request(&server, Method::PUT, TENANT_A, &path, Some(&source)).await?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }
    let (delete_status, body) =
        send_request(&server, Method::DELETE, TENANT_A, "/.kibana/_doc/dashboard:c", None).await?;
    assert_eq!(delete_status, StatusCode::OK, "{body}");

    let (a_status, a_rows) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/_cat/indices/.kibana*?format=json",
        None,
    )
    .await?;
    let (b_status, b_rows) = send_request(
        &server,
        Method::GET,
        TENANT_B,
        "/_cat/indices/.kibana*?format=json",
        None,
    )
    .await?;

    let list_counts = |rows: &Value| -> Vec<(Value, Value)> {
        rows.as_array()
            .expect("the rows are an array")
            .iter()
            .map(|row| (row["index"].clone(), row["docs.count"].clone()))
            .collect()
    };
    assert_eq!(a_status, StatusCode::OK);
    assert_eq!(list_counts(&a_rows), [(json!(".kibana_1"), json!("2"))]);
    assert_eq!(b_status, StatusCode::OK);
    assert_eq!(list_counts(&b_rows), [(json!(".kibana_1"), json!("0"))]);
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn bulk_answers_per_item_results_with_seq_no_primary_term_and_get_source() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let body = r#"{"create":{"_index":".kibana","_id":"dashboard:a"}}
{"type":"dashboard","dashboard":{"title":"Logs"}}
{"index":{"_index":".kibana","_id":"dashboard:b"}}
{"type":"dashboard","dashboard":{"title":"Traces"}}
{"update":{"_index":".kibana","_id":"dashboard:a"}}
{"doc":{"dashboard":{"title":"Errors"}}}
{"delete":{"_index":".kibana","_id":"dashboard:b"}}
{"delete":{"_index":".kibana","_id":"dashboard:missing"}}
"#;

    let (status, answer) = send_ndjson(
        &server,
        TENANT_A,
        "/_bulk?refresh=wait_for&_source_includes=type",
        body.to_string(),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert!(answer["took"].is_u64(), "{answer}");
    assert_eq!(answer["errors"], false);
    assert_eq!(
        answer["items"],
        json!([
            {"create": {"_index": ".kibana_1", "_id": "dashboard:a", "result": "created", "_shards": build_write_shards(), "_seq_no": 0, "_primary_term": 1, "status": 201}},
            {"index": {"_index": ".kibana_1", "_id": "dashboard:b", "result": "created", "_shards": build_write_shards(), "_seq_no": 1, "_primary_term": 1, "status": 201}},
            {"update": {"_index": ".kibana_1", "_id": "dashboard:a", "result": "updated", "_shards": build_write_shards(), "_seq_no": 2, "_primary_term": 1, "status": 200,
                        "get": {"_seq_no": 2, "_primary_term": 1, "found": true, "_source": {"type": "dashboard"}}}},
            {"delete": {"_index": ".kibana_1", "_id": "dashboard:b", "result": "deleted", "_shards": build_write_shards(), "_seq_no": 3, "_primary_term": 1, "status": 200}},
            {"delete": {"_index": ".kibana_1", "_id": "dashboard:missing", "result": "not_found", "_shards": build_write_shards(), "_seq_no": 4, "_primary_term": 1, "status": 404}}
        ])
    );
    let (_, read) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:a", None).await?;
    assert_eq!(
        read["_source"],
        json!({"type": "dashboard", "dashboard": {"title": "Errors"}})
    );
    assert_eq!(read["_seq_no"], 2);
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_bulk_item_for_a_data_index_fails_alone_with_cluster_block() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let body = r#"{"index":{"_index":".kibana","_id":"dashboard:a"}}
{"type":"dashboard"}
{"index":{"_index":"logs","_id":"1"}}
{"body":"x"}
{"index":{"_index":".kibana","_id":"dashboard:b"}}
{"type":"dashboard"}
"#;

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_bulk", body.to_string()).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["errors"], true);
    let items = answer["items"].as_array().expect("items");
    assert_eq!(items.len(), 3);
    assert_eq!(
        (
            items[0]["index"]["status"].clone(),
            items[0]["index"]["_seq_no"].clone()
        ),
        (json!(201), json!(0))
    );
    assert_eq!(items[1]["index"]["_index"], "logs");
    assert_eq!(items[1]["index"]["_id"], "1");
    assert_eq!(items[1]["index"]["status"], 403);
    assert_eq!(items[1]["index"]["error"]["type"], "cluster_block_exception");
    assert_eq!(
        (
            items[2]["index"]["status"].clone(),
            items[2]["index"]["_seq_no"].clone()
        ),
        (json!(201), json!(1))
    );
    server.shutdown().await;
    Ok(())
}

/// Every item names a data index: the request still answers `200`, with `errors` set and each
/// item's refusal in request order.
#[tokio::test]
async fn a_bulk_of_only_data_index_items_answers_every_refusal() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    let body = r#"{"delete":{"_index":"logs","_id":"1"}}
{"create":{"_index":"spans","_id":"2"}}
{"body":"x"}
"#;

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_bulk", body.to_string()).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["errors"], true);
    assert_eq!(
        (
            answer["items"][0]["delete"]["status"].clone(),
            answer["items"][0]["delete"]["error"]["type"].clone()
        ),
        (json!(403), json!("cluster_block_exception"))
    );
    assert_eq!(
        (
            answer["items"][1]["create"]["status"].clone(),
            answer["items"][1]["create"]["error"]["type"].clone()
        ),
        (json!(403), json!("cluster_block_exception"))
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_bulk_item_conflict_fails_alone_with_409() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let (status, _) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana/_create/dashboard:a",
        Some(&json!({"type": "dashboard"})),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    let body = r#"{"create":{"_index":".kibana","_id":"dashboard:a"}}
{"type":"dashboard","dashboard":{"title":"Overwritten"}}
{"index":{"_index":".kibana","_id":"dashboard:b"}}
{"type":"dashboard"}
"#;

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_bulk", body.to_string()).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["errors"], true);
    assert_eq!(answer["items"][0]["create"]["status"], 409);
    assert_eq!(
        answer["items"][0]["create"]["error"]["type"],
        "version_conflict_engine_exception"
    );
    assert_eq!(answer["items"][1]["index"]["status"], 201);
    let (_, read) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:a", None).await?;
    assert_eq!(read["_source"], json!({"type": "dashboard"}));
    server.shutdown().await;
    Ok(())
}

/// Items see the items before them: the update and the failed delete read the document the third
/// item wrote, and a merge that changes nothing keeps its `_seq_no` while the upsert takes the next
/// one. The `error` of a failed item carries the same metadata as the envelope of its request.
#[tokio::test]
async fn bulk_items_failing_while_applied_answer_alone_beside_noop_and_upsert_items() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let body = r#"{"index":{"_index":".missing","_id":"dashboard:a"}}
{"type":"dashboard"}
{"update":{"_index":".kibana","_id":"dashboard:missing"}}
{"doc":{"type":"dashboard"}}
{"index":{"_index":".kibana","_id":"dashboard:a"}}
{"type":"dashboard","dashboard":{"title":"Logs"}}
{"update":{"_index":".kibana","_id":"dashboard:a"}}
{"doc":{"dashboard":{"title":"Logs"}}}
{"delete":{"_index":".kibana","_id":"dashboard:a","if_seq_no":9,"if_primary_term":1}}
{"update":{"_index":".kibana","_id":"dashboard:b"}}
{"doc":{"type":"ignored"},"upsert":{"type":"dashboard"}}
"#;

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_bulk?_source=true", body.to_string()).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["errors"], true);
    assert_eq!(
        answer["items"][0],
        json!({"index": {"_index": ".missing", "_id": "dashboard:a", "status": 404, "error": {
            "type": "index_not_found_exception",
            "reason": "no such index [.missing]",
            "index": ".missing",
            "index_uuid": "_na_",
            "resource.id": ".missing",
            "resource.type": "index_or_alias"
        }}})
    );
    assert_eq!(answer["items"][1]["update"]["status"], 404);
    assert_eq!(
        answer["items"][1]["update"]["error"]["type"],
        "document_missing_exception"
    );
    assert_eq!(
        answer["items"][2],
        json!({"index": {"_index": ".kibana_1", "_id": "dashboard:a", "result": "created", "_shards": build_write_shards(), "_seq_no": 0, "_primary_term": 1, "status": 201}})
    );
    assert_eq!(
        answer["items"][3],
        json!({"update": {"_index": ".kibana_1", "_id": "dashboard:a", "result": "noop", "_shards": build_write_shards(), "_seq_no": 0, "_primary_term": 1, "status": 200,
                          "get": {"_seq_no": 0, "_primary_term": 1, "found": true, "_source": {"type": "dashboard", "dashboard": {"title": "Logs"}}}}})
    );
    assert_eq!(answer["items"][4]["delete"]["status"], 409);
    assert_eq!(
        answer["items"][4]["delete"]["error"]["type"],
        "version_conflict_engine_exception"
    );
    assert_eq!(
        answer["items"][5],
        json!({"update": {"_index": ".kibana_1", "_id": "dashboard:b", "result": "created", "_shards": build_write_shards(), "_seq_no": 1, "_primary_term": 1, "status": 201,
                          "get": {"_seq_no": 1, "_primary_term": 1, "found": true, "_source": {"type": "dashboard"}}}})
    );
    let (_, read) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:a", None).await?;
    assert_eq!(read["_seq_no"], 0);
    server.shutdown().await;
    Ok(())
}

/// `get` is opt-in on an update item, as on `_update`: an item answers the write alone unless a
/// parameter asks for its source. Each row changes the title, so each answer is a real update and
/// not a noop.
#[tokio::test]
async fn an_update_item_answers_get_only_when_the_request_asks_for_its_source() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let source = json!({"type": "dashboard", "dashboard": {"title": "Logs"}});
    let (status, create_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana/_create/dashboard:a",
        Some(&source),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{create_answer}");
    let cases = [
        (
            "/_bulk",
            "Errors",
            json!({"update": {
                "_index": ".kibana_1", "_id": "dashboard:a", "result": "updated", "_shards": build_write_shards(),
                "_seq_no": 1, "_primary_term": 1, "status": 200
            }}),
        ),
        (
            "/_bulk?_source=false",
            "Traces",
            json!({"update": {
                "_index": ".kibana_1", "_id": "dashboard:a", "result": "updated", "_shards": build_write_shards(),
                "_seq_no": 2, "_primary_term": 1, "status": 200
            }}),
        ),
        (
            "/_bulk?_source=true",
            "Metrics",
            json!({"update": {
                "_index": ".kibana_1", "_id": "dashboard:a", "result": "updated", "_shards": build_write_shards(),
                "_seq_no": 3, "_primary_term": 1, "status": 200,
                "get": {"_seq_no": 3, "_primary_term": 1, "found": true,
                        "_source": {"type": "dashboard", "dashboard": {"title": "Metrics"}}}
            }}),
        ),
    ];

    for (path, title, expected_item) in cases {
        let body = format!(
            "{{\"update\":{{\"_index\":\".kibana\",\"_id\":\"dashboard:a\"}}}}\n\
             {{\"doc\":{{\"dashboard\":{{\"title\":\"{title}\"}}}}}}\n"
        );

        let (status, answer) = send_ndjson(&server, TENANT_A, path, body).await?;

        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(answer["errors"], false, "{path}");
        assert_eq!(answer["items"][0], expected_item, "{path}");
    }
    server.shutdown().await;
    Ok(())
}

/// Both answers are serialized off the async worker and label themselves. `filter_path` leaves an
/// answer that is not labelled JSON unfiltered, so each row would answer its whole body otherwise.
#[tokio::test]
async fn bulk_and_mget_answers_are_labelled_json_and_honour_filter_path() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let bulk_body = "{\"index\":{\"_index\":\".kibana\",\"_id\":\"dashboard:a\"}}\n{\"type\":\"dashboard\"}\n";
    let mget_body = "{\"docs\":[{\"_index\":\".kibana\",\"_id\":\"dashboard:a\"}]}";
    let cases = [
        ("/_bulk?filter_path=errors", bulk_body, json!({"errors": false})),
        (
            "/_mget?filter_path=docs._id",
            mget_body,
            json!({"docs": [{"_id": "dashboard:a"}]}),
        ),
    ];

    for (path, body, expected_answer) in cases {
        let response = server
            .request(Method::POST, Some(TENANT_A), path)
            .header("content-type", "application/x-ndjson")
            .body(body)
            .send()
            .await?;

        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let content_type = response.headers().get("content-type").and_then(|value| value.to_str().ok());
        assert_eq!(content_type, Some("application/json"), "{path}");
        assert_eq!(response.json::<Value>().await?, expected_answer, "{path}");
    }
    server.shutdown().await;
    Ok(())
}

/// A `PUT` and a path index reach the same handlers as `POST /_bulk`, and an item without `_index`
/// takes the path's; `GET` serves `_mget` with a body, and a data index in the path fails per item.
#[tokio::test]
async fn bulk_and_mget_are_served_on_every_method_and_on_the_index_path() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;

    let (root_put_status, root_put) = send_ndjson_with_method(
        &server,
        Method::PUT,
        TENANT_A,
        "/_bulk",
        "{\"index\":{\"_index\":\".kibana\",\"_id\":\"dashboard:a\"}}\n{\"type\":\"dashboard\"}\n".to_string(),
    )
    .await?;
    let (index_put_status, index_put) = send_ndjson_with_method(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana/_bulk",
        "{\"index\":{\"_id\":\"dashboard:b\"}}\n{\"type\":\"dashboard\"}\n".to_string(),
    )
    .await?;
    let (index_post_status, index_post) = send_ndjson(
        &server,
        TENANT_A,
        "/.kibana/_bulk",
        "{\"delete\":{\"_id\":\"dashboard:b\"}}\n".to_string(),
    )
    .await?;
    let (data_path_status, data_path) = send_ndjson(
        &server,
        TENANT_A,
        "/logs/_bulk",
        "{\"delete\":{\"_id\":\"1\"}}\n".to_string(),
    )
    .await?;
    let (root_get_status, root_get) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/_mget",
        Some(&json!({"docs": [{"_index": ".kibana", "_id": "dashboard:a"}]})),
    )
    .await?;
    let (index_get_status, index_get) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/.kibana/_mget",
        Some(&json!({"docs": [{"_id": "dashboard:a", "_source": ["type"]}]})),
    )
    .await?;
    let (data_mget_status, data_mget) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_mget",
        Some(&json!({"ids": ["1"]})),
    )
    .await?;

    assert_eq!(
        (root_put_status, root_put["items"][0]["index"]["_index"].clone()),
        (StatusCode::OK, json!(".kibana_1"))
    );
    assert_eq!(
        (index_put_status, index_put["items"][0]["index"]["result"].clone()),
        (StatusCode::OK, json!("created"))
    );
    assert_eq!(
        (index_post_status, index_post["items"][0]["delete"]["result"].clone()),
        (StatusCode::OK, json!("deleted"))
    );
    assert_eq!(
        (data_path_status, data_path["items"][0]["delete"]["status"].clone()),
        (StatusCode::OK, json!(403))
    );
    assert_eq!(
        (root_get_status, root_get["docs"][0]["found"].clone()),
        (StatusCode::OK, json!(true))
    );
    assert_eq!(
        (index_get_status, index_get["docs"][0]["_source"].clone()),
        (StatusCode::OK, json!({"type": "dashboard"}))
    );
    assert_eq!(data_mget_status, StatusCode::OK);
    assert_eq!(data_mget["docs"][0]["_index"], "logs");
    assert_eq!(data_mget["docs"][0]["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn an_unknown_parameter_on_bulk_or_mget_is_refused() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;

    let (bulk_status, bulk_answer) = send_ndjson(
        &server,
        TENANT_A,
        "/_bulk?routing=r",
        "{\"delete\":{\"_index\":\".kibana\",\"_id\":\"a\"}}\n".to_string(),
    )
    .await?;
    let (mget_status, mget_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/_mget?routing=r",
        Some(&json!({"docs": [{"_index": ".kibana", "_id": "a"}]})),
    )
    .await?;

    assert_eq!(bulk_status, StatusCode::BAD_REQUEST);
    assert_eq!(bulk_answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(mget_status, StatusCode::BAD_REQUEST);
    assert_eq!(mget_answer["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn mget_returns_found_and_missing_documents_in_request_order() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    for (id, title) in [("dashboard:a", "A"), ("dashboard:b", "B")] {
        let source = json!({"type": "dashboard", "dashboard": {"title": title}});
        let (status, _) = send_request(
            &server,
            Method::PUT,
            TENANT_A,
            &format!("/.kibana/_create/{id}"),
            Some(&source),
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);
    }
    let request = json!({"docs": [
        {"_index": ".kibana", "_id": "dashboard:b"},
        {"_index": ".kibana", "_id": "dashboard:missing"},
        {"_index": ".kibana", "_id": "dashboard:a", "_source": false},
        {"_index": ".kibana_9", "_id": "dashboard:a"},
        {"_index": "logs", "_id": "1"}
    ]});

    let (status, answer) = send_request(&server, Method::POST, TENANT_A, "/_mget", Some(&request)).await?;
    let (ids_status, ids_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_mget",
        Some(&json!({"ids": ["dashboard:a", "dashboard:b"]})),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    let docs = answer["docs"].as_array().expect("docs");
    assert_eq!(docs.len(), 5);
    assert_eq!(
        docs[0],
        json!({"_index": ".kibana_1", "_id": "dashboard:b", "_seq_no": 1, "_primary_term": 1, "found": true,
               "_source": {"type": "dashboard", "dashboard": {"title": "B"}}})
    );
    assert_eq!(
        docs[1],
        json!({"_index": ".kibana_1", "_id": "dashboard:missing", "found": false})
    );
    assert_eq!(
        docs[2],
        json!({"_index": ".kibana_1", "_id": "dashboard:a", "_seq_no": 0, "_primary_term": 1, "found": true})
    );
    assert_eq!(
        (docs[3]["_id"].clone(), docs[3]["error"]["type"].clone()),
        (json!("dashboard:a"), json!("index_not_found_exception"))
    );
    assert_eq!(
        (docs[4]["_index"].clone(), docs[4]["error"]["type"].clone()),
        (json!("logs"), json!("illegal_argument_exception"))
    );
    assert_eq!(ids_status, StatusCode::OK);
    let ordered_ids: Vec<&Value> = ids_answer["docs"]
        .as_array()
        .expect("docs")
        .iter()
        .map(|doc| &doc["_id"])
        .collect();
    assert_eq!(ordered_ids, [&json!("dashboard:a"), &json!("dashboard:b")]);
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_bulk_near_the_import_limit_is_accepted_under_the_body_limit() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let padding = "p".repeat(12_000);
    let mut body = String::with_capacity(IMPORT_LIMIT_BYTES + 2 * padding.len());
    let mut count = 0_usize;
    while body.len() < IMPORT_LIMIT_BYTES {
        writeln!(
            body,
            "{{\"index\":{{\"_index\":\".kibana\",\"_id\":\"visualization:{count}\"}}}}"
        )?;
        writeln!(
            body,
            "{{\"type\":\"visualization\",\"visualization\":{{\"visState\":\"{padding}\"}}}}"
        )?;
        count += 1;
    }
    // The fixture reaches the import limit and stays under the default request-body limit.
    assert!(body.len() >= IMPORT_LIMIT_BYTES, "{}", body.len());
    assert!(
        body.len() < OpenSearchConfig::default().max_request_body_bytes,
        "{}",
        body.len()
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_bulk", body).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["errors"], false);
    assert_eq!(answer["items"].as_array().map(Vec::len), Some(count));
    let last_path = format!("/.kibana/_doc/visualization:{}", count - 1);
    let (read_status, read) = send_request(&server, Method::GET, TENANT_A, &last_path, None).await?;
    assert_eq!((read_status, read["found"].clone()), (StatusCode::OK, json!(true)));
    server.shutdown().await;
    Ok(())
}

/// A one-item bulk body of exactly `length` bytes.
fn build_bulk_body_of_length(length: usize) -> String {
    let action = "{\"index\":{\"_index\":\".kibana\",\"_id\":\"config:padded\"}}\n";
    let prefix = "{\"type\":\"config\",\"config\":{\"buildNum\":\"";
    let suffix = "\"}}\n";
    let padding = length - action.len() - prefix.len() - suffix.len();
    let body = format!("{action}{prefix}{}{suffix}", "9".repeat(padding));
    assert_eq!(body.len(), length);
    body
}

#[tokio::test]
async fn a_bulk_body_over_max_request_body_bytes_is_413() -> TestResult {
    let config = OpenSearchConfig {
        max_request_body_bytes: 4096,
        ..OpenSearchConfig::default()
    };
    let (server, _catalog) = TestServer::start_with_config(config).await;
    create_kibana_index(&server, TENANT_A).await?;

    let (at_status, at_limit) = send_ndjson(&server, TENANT_A, "/_bulk", build_bulk_body_of_length(4096)).await?;
    let (over_status, over_limit) = send_ndjson(&server, TENANT_A, "/_bulk", build_bulk_body_of_length(4097)).await?;

    assert_eq!(at_status, StatusCode::OK);
    assert_eq!(at_limit["errors"], false);
    assert_eq!(over_status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(over_limit["status"], 413);
    assert!(over_limit["error"]["type"].is_string(), "{over_limit}");
    server.shutdown().await;
    Ok(())
}

/// One bulk is one store operation, so the limit refuses it as a whole and none of its items land.
#[tokio::test]
async fn a_bulk_that_would_grow_the_snapshot_past_its_limit_is_413_and_writes_nothing() -> TestResult {
    let config = OpenSearchConfig {
        system_indices: SystemIndicesConfig {
            max_snapshot_bytes: 8192,
            ..SystemIndicesConfig::default()
        },
        ..OpenSearchConfig::default()
    };
    let (server, _catalog) = TestServer::start_with_config(config).await;
    create_kibana_index(&server, TENANT_A).await?;
    let padding = "p".repeat(10_000);
    let body = format!(
        "{{\"index\":{{\"_index\":\".kibana\",\"_id\":\"dashboard:a\"}}}}\n{{\"type\":\"dashboard\"}}\n\
         {{\"index\":{{\"_index\":\".kibana\",\"_id\":\"dashboard:b\"}}}}\n{{\"dashboard\":{{\"title\":\"{padding}\"}}}}\n"
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_bulk", body).await?;
    let (read_status, read) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:a", None).await?;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(answer["status"], 413);
    assert_eq!(
        (read_status, read["found"].clone()),
        (StatusCode::NOT_FOUND, json!(false))
    );
    server.shutdown().await;
    Ok(())
}

/// The key as an operator writes it: a YAML document on disk, read by the loader the binary uses.
#[test]
fn max_answer_bytes_parses_from_a_config_file() -> TestResult {
    let config_file = tempfile::Builder::new().suffix(".yaml").tempfile()?;
    std::fs::write(config_file.path(), "system_indices:\n  max_answer_bytes: 268435457\n")?;

    let config: OpenSearchConfig = load_config_file(config_file.path())?;

    assert_eq!(config.system_indices.max_answer_bytes, 268_435_457);
    Ok(())
}

/// A config document of 2,042 serialized bytes.
fn build_padded_config_source(padding_digit: char) -> Value {
    json!({"type": "config", "config": {"buildNum": padding_digit.to_string().repeat(2000)}})
}

/// A listener bounding answers at `max_answer_bytes`, whose `.kibana` holds `config:a` at `_seq_no`
/// 0 and `config:b` at `_seq_no` 1, both [`build_padded_config_source`]. Validation refuses an answer
/// bound below `max_snapshot_bytes`, so the snapshot bound comes down with it: 6,000 holds these two
/// documents and is at or below every answer bound the tests here use.
async fn start_server_with_answer_bound(max_answer_bytes: usize) -> Result<TestServer, Box<dyn Error>> {
    let system_indices = SystemIndicesConfig {
        max_snapshot_bytes: 6000,
        max_answer_bytes,
        ..SystemIndicesConfig::default()
    };
    system_indices.validate()?;
    let config = OpenSearchConfig {
        system_indices,
        ..OpenSearchConfig::default()
    };
    let (server, _catalog) = TestServer::start_with_config(config).await;
    create_kibana_index(&server, TENANT_A).await?;
    for id in ["config:a", "config:b"] {
        let (status, answer) = send_request(
            &server,
            Method::PUT,
            TENANT_A,
            &format!("/.kibana/_create/{id}"),
            Some(&build_padded_config_source('9')),
        )
        .await?;
        if status != StatusCode::CREATED {
            return Err(format!("creating {id} answered {status}: {answer}").into());
        }
    }
    Ok(server)
}

/// Assert `error`, an `error` object of the envelope or of an item, is the answer bound's refusal.
fn assert_answer_bound_refusal(error: &Value) {
    assert_eq!(error["type"], "illegal_argument_exception", "{error}");
    assert!(
        error["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("opensearch.system_indices.max_answer_bytes")),
        "{error}"
    );
}

/// Three entries of 2,135 bytes, two commas and the 11 bytes of `{"docs":[` and `]}`.
const THREE_ENTRY_ANSWER_BYTES: usize = 6418;

/// The ids repeat, as nothing stops a client from repeating them, and each entry holds the whole
/// document. The answer is stated at its exact length, so these rows are the answer at the bound and
/// one byte below it.
#[tokio::test]
async fn an_mget_answer_at_or_just_below_max_answer_bytes_answers_every_entry_in_request_order() -> TestResult {
    let build_entry = |id: &str, seq_no: i64| {
        json!({"_index": ".kibana_1", "_id": id, "_seq_no": seq_no, "_primary_term": 1, "found": true,
               "_source": build_padded_config_source('9')})
    };
    for max_answer_bytes in [THREE_ENTRY_ANSWER_BYTES, THREE_ENTRY_ANSWER_BYTES + 1] {
        let server = start_server_with_answer_bound(max_answer_bytes).await?;

        let response = server
            .request(Method::POST, Some(TENANT_A), "/.kibana/_mget")
            .json(&json!({"ids": ["config:b", "config:a", "config:b"]}))
            .send()
            .await?;

        assert_eq!(response.status(), StatusCode::OK, "bound {max_answer_bytes}");
        let answer_body = response.bytes().await?;
        assert_eq!(answer_body.len(), THREE_ENTRY_ANSWER_BYTES, "bound {max_answer_bytes}");
        assert_eq!(
            serde_json::from_slice::<Value>(&answer_body)?,
            json!({"docs": [build_entry("config:b", 1), build_entry("config:a", 0), build_entry("config:b", 1)]}),
            "bound {max_answer_bytes}"
        );
        server.shutdown().await;
    }
    Ok(())
}

/// One byte above the bound, the answer is the error envelope and holds no part of `docs`.
#[tokio::test]
async fn an_mget_answer_just_above_max_answer_bytes_is_refused_naming_the_setting() -> TestResult {
    let server = start_server_with_answer_bound(THREE_ENTRY_ANSWER_BYTES - 1).await?;

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_mget",
        Some(&json!({"ids": ["config:b", "config:a", "config:b"]})),
    )
    .await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(answer["status"], 400);
    assert_answer_bound_refusal(&answer["error"]);
    assert!(answer.get("docs").is_none(), "{answer}");
    server.shutdown().await;
    Ok(())
}

/// An unknown id reads no document, and the bound still holds. Each entry is 59 bytes and all but the
/// last take a comma, inside the 11 bytes of `{"docs":[` and `]}`: 100 entries are 6,010 bytes, past
/// a bound of 6,000 with no document in the answer, and 99 are 5,950.
#[tokio::test]
async fn an_mget_of_only_unknown_ids_past_max_answer_bytes_is_refused_and_below_it_is_answered() -> TestResult {
    let server = start_server_with_answer_bound(6000).await?;

    let (over_status, over_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_mget",
        Some(&json!({"ids": vec!["config:missing"; 100]})),
    )
    .await?;
    let (under_status, under_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_mget",
        Some(&json!({"ids": vec!["config:missing"; 99]})),
    )
    .await?;

    assert_eq!(over_status, StatusCode::BAD_REQUEST);
    assert_answer_bound_refusal(&over_answer["error"]);
    assert_eq!(under_status, StatusCode::OK);
    assert_eq!(
        under_answer["docs"],
        Value::Array(vec![
            json!({"_index": ".kibana_1", "_id": "config:missing", "found": false});
            99
        ])
    );
    server.shutdown().await;
    Ok(())
}

/// The bound changes nothing for an answer under it, whichever kinds of entry it holds.
#[tokio::test]
async fn an_mget_under_max_answer_bytes_answers_found_missing_and_failed_entries_unchanged() -> TestResult {
    let server = start_server_with_answer_bound(6000).await?;
    let request = json!({"docs": [
        {"_index": ".kibana", "_id": "config:a"},
        {"_index": ".kibana", "_id": "config:missing"},
        {"_index": ".kibana", "_id": "config:b", "_source": false},
        {"_index": ".kibana_9", "_id": "config:a"},
        {"_index": "logs", "_id": "1"}
    ]});

    let (status, answer) = send_request(&server, Method::POST, TENANT_A, "/_mget", Some(&request)).await?;

    assert_eq!(status, StatusCode::OK);
    let docs = answer["docs"].as_array().expect("docs");
    assert_eq!(docs.len(), 5);
    assert_eq!(
        docs[0],
        json!({"_index": ".kibana_1", "_id": "config:a", "_seq_no": 0, "_primary_term": 1, "found": true,
               "_source": build_padded_config_source('9')})
    );
    assert_eq!(
        docs[1],
        json!({"_index": ".kibana_1", "_id": "config:missing", "found": false})
    );
    assert_eq!(
        docs[2],
        json!({"_index": ".kibana_1", "_id": "config:b", "_seq_no": 1, "_primary_term": 1, "found": true})
    );
    assert_eq!(docs[3]["error"]["type"], "index_not_found_exception");
    assert_eq!(docs[4]["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
    Ok(())
}

/// Each update keeps 2,042 bytes, so the third reaches the bound of 6,000. The writes of a `_bulk`
/// land, so a breach must not fail the request: the fourth update fails alone while the index and
/// the delete after it apply, and the kept items answer `get`.
#[tokio::test]
async fn update_items_past_max_answer_bytes_fail_alone_and_the_kept_items_answer_get() -> TestResult {
    let server = start_server_with_answer_bound(6000).await?;
    let build_update_lines = |id: &str, padding_digit: char| {
        let change = json!({"doc": {"config": {"buildNum": padding_digit.to_string().repeat(2000)}}});
        format!("{{\"update\":{{\"_index\":\".kibana\",\"_id\":\"{id}\"}}}}\n{change}\n")
    };
    let body = [
        build_update_lines("config:a", '1'),
        build_update_lines("config:b", '2'),
        build_update_lines("config:a", '3'),
        build_update_lines("config:a", '4'),
        "{\"index\":{\"_index\":\".kibana\",\"_id\":\"config:c\"}}\n{\"type\":\"config\"}\n".to_string(),
        "{\"delete\":{\"_index\":\".kibana\",\"_id\":\"config:b\"}}\n".to_string(),
    ]
    .concat();
    let build_kept_item = |id: &str, seq_no: i64, padding_digit: char| {
        json!({"update": {
            "_index": ".kibana_1", "_id": id, "result": "updated", "_shards": build_write_shards(),
            "_seq_no": seq_no, "_primary_term": 1, "status": 200,
            "get": {"_seq_no": seq_no, "_primary_term": 1, "found": true,
                    "_source": build_padded_config_source(padding_digit)}
        }})
    };

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_bulk?_source=true", body).await?;
    let (_, stored_document) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/config:a", None).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["errors"], true);
    let items = answer["items"].as_array().expect("items");
    assert_eq!(items.len(), 6);
    assert_eq!(items[0], build_kept_item("config:a", 2, '1'));
    assert_eq!(items[1], build_kept_item("config:b", 3, '2'));
    assert_eq!(items[2], build_kept_item("config:a", 4, '3'));
    assert_eq!(items[3]["update"]["status"], 400);
    assert_eq!(items[3]["update"]["_id"], "config:a");
    assert_answer_bound_refusal(&items[3]["update"]["error"]);
    assert_eq!(items[4]["index"]["_seq_no"], 5);
    assert_eq!(items[4]["index"]["status"], 201);
    assert_eq!(items[5]["delete"]["_seq_no"], 6);
    assert_eq!(items[5]["delete"]["status"], 200);
    assert_eq!(stored_document["_seq_no"], 4);
    assert_eq!(stored_document["_source"], build_padded_config_source('3'));
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn reindex_update_by_query_delete_by_query_and_tasks_are_400s() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    let requests = [
        (Method::POST, "/_reindex?refresh=true&wait_for_completion=false"),
        (Method::POST, "/.kibana/_update_by_query"),
        (Method::POST, "/.kibana/_delete_by_query"),
        (Method::GET, "/_tasks"),
        (Method::GET, "/_tasks/node:1"),
    ];

    for (method, path) in requests {
        let response = server
            .request(method.clone(), Some(TENANT_A), path)
            .json(&json!({}))
            .send()
            .await?;
        let status = response.status();
        let answer = response.json::<Value>().await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}");
        assert_eq!(answer["error"]["type"], "illegal_argument_exception", "{method} {path}");
    }
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn bulk_and_mget_under_tenant_b_see_none_of_tenant_a_documents() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    create_kibana_index(&server, TENANT_B).await?;
    let tenant_a_body = r#"{"index":{"_index":".kibana","_id":"dashboard:a"}}
{"type":"dashboard","dashboard":{"title":"Tenant A"}}
"#;
    let (status, _) = send_ndjson(&server, TENANT_A, "/_bulk", tenant_a_body.to_string()).await?;
    assert_eq!(status, StatusCode::OK);

    let (_, mget) = send_request(
        &server,
        Method::POST,
        TENANT_B,
        "/.kibana/_mget",
        Some(&json!({"ids": ["dashboard:a"]})),
    )
    .await?;
    let tenant_b_delete = "{\"delete\":{\"_index\":\".kibana\",\"_id\":\"dashboard:a\"}}\n";
    let (_, bulk) = send_ndjson(&server, TENANT_B, "/_bulk", tenant_b_delete.to_string()).await?;
    let (_, a_read) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_doc/dashboard:a", None).await?;

    assert_eq!(
        mget["docs"],
        json!([{"_index": ".kibana_1", "_id": "dashboard:a", "found": false}])
    );
    assert_eq!(bulk["items"][0]["delete"]["result"], "not_found");
    assert_eq!(bulk["items"][0]["delete"]["status"], 404);
    assert_eq!(a_read["found"], true);
    assert_eq!(
        a_read["_source"],
        json!({"type": "dashboard", "dashboard": {"title": "Tenant A"}})
    );
    server.shutdown().await;
    Ok(())
}

/// The `_shards` block of every count and search answer.
fn build_expected_shards() -> Value {
    json!({"total": 1, "successful": 1, "skipped": 0, "failed": 0})
}

/// Store `source` under `id` in `.kibana`.
///
/// # Errors
///
/// Fails with the status and the body of an answer that is not a success.
async fn put_saved_object(server: &TestServer, tenant: &str, id: &str, source: &Value) -> TestResult {
    let path = format!("/.kibana/_doc/{id}?refresh=wait_for");
    let (status, body) = send_request(server, Method::PUT, tenant, &path, Some(source)).await?;
    if !status.is_success() {
        return Err(format!("storing {id} answered {status}: {body}").into());
    }
    Ok(())
}

/// A migration-style "are any documents outdated" count: for each type, documents holding that
/// type whose `migrationVersion.<type>` is not the latest version.
fn build_outdated_count_body() -> Value {
    json!({"query": {"bool": {"should": [
        {"bool": {"must": [
            {"exists": {"field": "dashboard"}},
            {"bool": {"must_not": {"term": {"migrationVersion.dashboard": "7.9.3"}}}}
        ]}},
        {"bool": {"must": [
            {"exists": {"field": "index-pattern"}},
            {"bool": {"must_not": {"term": {"migrationVersion.index-pattern": "7.6.0"}}}}
        ]}}
    ]}}})
}

#[tokio::test]
async fn a_migration_style_count_answers_zero_when_every_document_is_current_and_n_when_n_are_outdated() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    put_saved_object(
        &server,
        TENANT_A,
        "dashboard:a",
        &json!({"type": "dashboard", "dashboard": {"title": "A"}, "migrationVersion": {"dashboard": "7.9.3"}}),
    )
    .await?;
    put_saved_object(
        &server,
        TENANT_A,
        "dashboard:b",
        &json!({"type": "dashboard", "dashboard": {"title": "B"}, "migrationVersion": {"dashboard": "7.9.3"}}),
    )
    .await?;
    put_saved_object(&server, TENANT_A, "index-pattern:c", &json!({"type": "index-pattern", "index-pattern": {"title": "logs-*"}, "migrationVersion": {"index-pattern": "7.6.0"}})).await?;
    put_saved_object(
        &server,
        TENANT_A,
        "config:3.9.0",
        &json!({"type": "config", "config": {"buildNum": "9000"}}),
    )
    .await?;

    let (current_status, current_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_count",
        Some(&build_outdated_count_body()),
    )
    .await?;
    put_saved_object(
        &server,
        TENANT_A,
        "dashboard:b",
        &json!({"type": "dashboard", "dashboard": {"title": "B"}, "migrationVersion": {"dashboard": "7.9.0"}}),
    )
    .await?;
    put_saved_object(
        &server,
        TENANT_A,
        "index-pattern:c",
        &json!({"type": "index-pattern", "index-pattern": {"title": "logs-*"}}),
    )
    .await?;
    let (outdated_status, outdated_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_count",
        Some(&build_outdated_count_body()),
    )
    .await?;

    assert_eq!(current_status, StatusCode::OK);
    assert_eq!(current_answer, json!({"count": 0, "_shards": build_expected_shards()}));
    assert_eq!(outdated_status, StatusCode::OK);
    assert_eq!(outdated_answer, json!({"count": 2, "_shards": build_expected_shards()}));
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn count_with_a_body_key_other_than_query_is_a_parsing_exception() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;

    for body in [
        json!({"query": {"match_all": {}}, "size": 0}),
        json!({"aggs": {}}),
        json!({"query": {"match": {"type": "x"}}}),
    ] {
        let (status, answer) = send_request(&server, Method::POST, TENANT_A, "/.kibana/_count", Some(&body)).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(answer["error"]["type"], "parsing_exception", "{body}");
    }
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn count_with_a_malformed_body_or_parameter_is_refused() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;

    let (malformed_status, malformed_answer) =
        send_ndjson(&server, TENANT_A, "/.kibana/_count", "{".to_string()).await?;
    let (array_status, array_answer) =
        send_request(&server, Method::POST, TENANT_A, "/.kibana/_count", Some(&json!([]))).await?;
    let (wildcards_status, wildcards_answer) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/.kibana/_count?expand_wildcards=bogus",
        None,
    )
    .await?;
    let (unknown_status, unknown_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana/_count?unknown=1", None).await?;

    assert_eq!(malformed_status, StatusCode::BAD_REQUEST);
    assert_eq!(malformed_answer["error"]["type"], "parsing_exception");
    assert_eq!(array_status, StatusCode::BAD_REQUEST);
    assert_eq!(array_answer["error"]["type"], "parsing_exception");
    assert_eq!(wildcards_status, StatusCode::BAD_REQUEST);
    assert_eq!(wildcards_answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(unknown_status, StatusCode::BAD_REQUEST);
    assert_eq!(unknown_answer["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn count_accepts_an_ignored_parameter() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    put_saved_object(&server, TENANT_A, "dashboard:a", &json!({"type": "dashboard"})).await?;

    for parameter in ["preference=1700000000", "routing=dashboard", "ignore_throttled=false"] {
        let path = format!("/.kibana/_count?{parameter}");
        let (status, answer) = send_request(&server, Method::GET, TENANT_A, &path, None).await?;

        assert_eq!(status, StatusCode::OK, "{parameter}");
        assert_eq!(
            answer,
            json!({"count": 1, "_shards": build_expected_shards()}),
            "{parameter}"
        );
    }
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn count_with_a_date_bound_past_the_representable_range_is_a_400() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let document = json!({"type": "dashboard", "updated_at": "2026-10-08T12:00:00.000Z"});
    put_saved_object(&server, TENANT_A, "dashboard:a", &document).await?;
    // The instant is representable, but `+18:00` puts its local time past the largest date, and
    // the rounding reads that local time on the blocking pool.
    let body = json!({"query": {"range": {"updated_at": {"gte": "8210266876000000||/d", "time_zone": "+18:00"}}}});

    let (status, answer) = send_request(&server, Method::POST, TENANT_A, "/.kibana/_count", Some(&body)).await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn count_answers_with_fully_successful_shards() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    put_saved_object(&server, TENANT_A, "dashboard:a", &json!({"type": "dashboard"})).await?;
    put_saved_object(&server, TENANT_A, "config:3.9.0", &json!({"type": "config"})).await?;

    let (all_status, all_answer) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_count", None).await?;
    let (empty_query_status, empty_query_answer) =
        send_request(&server, Method::POST, TENANT_A, "/.kibana/_count", Some(&json!({}))).await?;
    let (filtered_status, filtered_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana_1/_count",
        Some(&json!({"query": {"term": {"type": "dashboard"}}})),
    )
    .await?;
    // The wildcard selects the alias and its index: one index, counted once.
    let (wildcard_status, wildcard_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana*/_count", None).await?;
    let (missing_status, missing_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana_9/_count", None).await?;

    let expected_all = json!({"count": 2, "_shards": build_expected_shards()});
    assert_eq!((all_status, all_answer), (StatusCode::OK, expected_all.clone()));
    assert_eq!(
        (empty_query_status, empty_query_answer),
        (StatusCode::OK, expected_all.clone())
    );
    assert_eq!(
        (filtered_status, filtered_answer),
        (StatusCode::OK, json!({"count": 1, "_shards": build_expected_shards()}))
    );
    assert_eq!((wildcard_status, wildcard_answer), (StatusCode::OK, expected_all));
    assert_eq!(
        (missing_status, missing_answer["error"]["type"].clone()),
        (StatusCode::NOT_FOUND, json!("index_not_found_exception"))
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn count_sums_the_documents_of_every_selected_index() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let second_index = json!({"mappings": build_kibana_mappings(), "settings": {}});
    let (created_status, _) = send_request(&server, Method::PUT, TENANT_A, "/.kibana_2", Some(&second_index)).await?;
    assert!(created_status.is_success(), "{created_status}");
    put_saved_object(&server, TENANT_A, "dashboard:a", &json!({"type": "dashboard"})).await?;
    put_saved_object(&server, TENANT_A, "config:3.9.0", &json!({"type": "config"})).await?;
    let (stored_status, _) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana_2/_doc/dashboard:a?refresh=wait_for",
        Some(&json!({"type": "dashboard"})),
    )
    .await?;
    assert!(stored_status.is_success(), "{stored_status}");

    let (all_status, all_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana_1,.kibana_2/_count", None).await?;
    let (filtered_status, filtered_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana_1,.kibana_2/_count",
        Some(&json!({"query": {"term": {"type": "dashboard"}}})),
    )
    .await?;

    assert_eq!(
        (all_status, all_answer),
        (StatusCode::OK, json!({"count": 3, "_shards": build_expected_shards()}))
    );
    assert_eq!(
        (filtered_status, filtered_answer),
        (StatusCode::OK, json!({"count": 2, "_shards": build_expected_shards()}))
    );
    server.shutdown().await;
    Ok(())
}

/// The real clock is the contract here: `now` comes from the request. The two dates sit decades
/// either side of any instant this test can run at, so the outcome does not depend on it.
#[tokio::test]
async fn count_evaluates_date_math_against_the_instant_of_the_request() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let old_document = json!({"type": "dashboard", "updated_at": "2000-01-01T00:00:00.000Z"});
    let future_document = json!({"type": "dashboard", "updated_at": "2999-01-01T00:00:00.000Z"});
    put_saved_object(&server, TENANT_A, "dashboard:old", &old_document).await?;
    put_saved_object(&server, TENANT_A, "dashboard:future", &future_document).await?;
    let body = json!({"query": {"range": {"updated_at": {"lt": "now-90d"}}}});

    let (status, answer) = send_request(&server, Method::POST, TENANT_A, "/.kibana/_count", Some(&body)).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer, json!({"count": 1, "_shards": build_expected_shards()}));
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn count_over_an_empty_target_answers_zero_unless_allow_no_indices_is_false() -> TestResult {
    let (server, _catalog) = TestServer::start().await;

    let (empty_status, empty_answer) = send_request(&server, Method::GET, TENANT_A, "/.kibana_*/_count", None).await?;
    let (disallowed_status, disallowed_answer) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/.kibana_*/_count?allow_no_indices=false",
        None,
    )
    .await?;
    create_kibana_index(&server, TENANT_A).await?;
    let (ignored_status, ignored_answer) = send_request(
        &server,
        Method::GET,
        TENANT_A,
        "/.kibana_9/_count?ignore_unavailable=true",
        None,
    )
    .await?;

    let expected_empty = json!({"count": 0, "_shards": build_expected_shards()});
    assert_eq!((empty_status, empty_answer), (StatusCode::OK, expected_empty.clone()));
    assert_eq!(
        (disallowed_status, disallowed_answer["error"]["type"].clone()),
        (StatusCode::NOT_FOUND, json!("index_not_found_exception"))
    );
    assert_eq!((ignored_status, ignored_answer), (StatusCode::OK, expected_empty));
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn count_on_a_data_index_is_a_400() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;

    let (status, answer) = send_request(&server, Method::GET, TENANT_A, "/logs/_count", None).await?;
    let (mixed_status, mixed_answer) =
        send_request(&server, Method::GET, TENANT_A, "/.kibana_1,logs/_count", None).await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(mixed_status, StatusCode::BAD_REQUEST);
    assert_eq!(mixed_answer["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn count_under_tenant_b_counts_none_of_tenant_a_documents() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    create_kibana_index(&server, TENANT_B).await?;
    put_saved_object(&server, TENANT_A, "dashboard:a", &json!({"type": "dashboard"})).await?;
    put_saved_object(&server, TENANT_A, "dashboard:b", &json!({"type": "dashboard"})).await?;

    let (_, owner_answer) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_count", None).await?;
    let (_, other_tenant_answer) = send_request(&server, Method::GET, TENANT_B, "/.kibana/_count", None).await?;
    let (unknown_status, _) = send_request(&server, Method::GET, "tenant-c", "/.kibana/_count", None).await?;

    assert_eq!(owner_answer["count"], 2);
    assert_eq!(other_tenant_answer["count"], 0);
    assert_eq!(unknown_status, StatusCode::NOT_FOUND);
    server.shutdown().await;
    Ok(())
}

/// A find-style query for one single-namespace type: the type, and either no namespace or the
/// default one, never a multi-namespace document.
fn build_find_index_patterns_body() -> Value {
    json!({
        "seq_no_primary_term": true,
        "query": {"bool": {"filter": [{"bool": {
            "should": [{"bool": {
                "must": [{"term": {"type": "index-pattern"}}],
                "should": [{"bool": {"must_not": [{"exists": {"field": "namespace"}}]}}],
                "minimum_should_match": 1,
                "must_not": [{"exists": {"field": "namespaces"}}]
            }}],
            "minimum_should_match": 1
        }}]}},
        "sort": [{"updated_at": {"order": "desc", "unmapped_type": "date"}}]
    })
}

#[tokio::test]
async fn a_find_style_search_returns_hits_with_seq_no_primary_term_and_an_integer_total() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    let logs = json!({"type": "index-pattern", "index-pattern": {"title": "logs-*"}, "updated_at": "2026-10-02T00:00:00.000Z"});
    let spans = json!({"type": "index-pattern", "index-pattern": {"title": "spans-*"}, "updated_at": "2026-10-03T00:00:00.000Z"});
    put_saved_object(&server, TENANT_A, "index-pattern:logs", &logs).await?;
    put_saved_object(&server, TENANT_A, "index-pattern:spans", &spans).await?;
    put_saved_object(
        &server,
        TENANT_A,
        "index-pattern:other-space",
        &json!({"type": "index-pattern", "namespace": "space-1", "updated_at": "2026-10-04T00:00:00.000Z"}),
    )
    .await?;
    put_saved_object(
        &server,
        TENANT_A,
        "dashboard:a",
        &json!({"type": "dashboard", "updated_at": "2026-10-05T00:00:00.000Z"}),
    )
    .await?;

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_search?size=20&from=0&rest_total_hits_as_int=true",
        Some(&build_find_index_patterns_body()),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert!(answer["took"].is_u64(), "{answer}");
    assert_eq!(answer["timed_out"], false);
    assert_eq!(answer["_shards"], build_expected_shards());
    assert_eq!(
        answer["hits"],
        json!({
            "total": 2,
            "max_score": null,
            "hits": [
                {"_index": ".kibana_1", "_id": "index-pattern:spans", "_seq_no": 1, "_primary_term": 1, "_score": null, "_source": spans, "sort": [1_790_985_600_000_i64]},
                {"_index": ".kibana_1", "_id": "index-pattern:logs", "_seq_no": 0, "_primary_term": 1, "_score": null, "_source": logs, "sort": [1_790_899_200_000_i64]}
            ]
        })
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn search_of_a_missing_system_index_is_404_and_of_an_alias_reads_its_index() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    put_saved_object(&server, TENANT_A, "dashboard:a", &json!({"type": "dashboard"})).await?;

    let (missing_status, missing_answer) =
        send_request(&server, Method::POST, TENANT_A, "/.kibana_9/_search", Some(&json!({}))).await?;
    let (alias_status, alias_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_search",
        Some(&json!({"query": {"ids": {"values": ["dashboard:a"]}}})),
    )
    .await?;
    let (get_status, get_answer) = send_request(&server, Method::GET, TENANT_A, "/.kibana/_search", None).await?;

    assert_eq!(missing_status, StatusCode::NOT_FOUND);
    assert_eq!(missing_answer["error"]["type"], "index_not_found_exception");
    assert_eq!(alias_status, StatusCode::OK);
    assert_eq!(
        alias_answer["hits"],
        json!({
            "total": {"value": 1, "relation": "eq"},
            "max_score": 1.0,
            "hits": [{"_index": ".kibana_1", "_id": "dashboard:a", "_score": 1.0, "_source": {"type": "dashboard"}}]
        })
    );
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(get_answer["hits"]["total"], json!({"value": 1, "relation": "eq"}));
    server.shutdown().await;
    Ok(())
}

/// `from: -1` is refused by the handler's body parser, so the `parsing_exception` proves the
/// index-less route reaches the handler: an unmatched route answers `illegal_argument_exception`.
#[tokio::test]
async fn search_without_an_index_in_the_path_reaches_the_handler_on_get_and_post() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    let body = json!({"from": -1});

    for method in [Method::GET, Method::POST] {
        let (status, answer) = send_request(&server, method.clone(), TENANT_A, "/_search", Some(&body)).await?;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{method}");
        assert_eq!(answer["error"]["type"], "parsing_exception", "{method}");
    }
    server.shutdown().await;
    Ok(())
}

/// The test catalog holds data tables and no system index, so the empty expression selects data
/// indices, which the handler refuses once the body has parsed.
#[tokio::test]
async fn a_valid_search_without_an_index_in_the_path_selects_the_data_indices_and_is_a_400() -> TestResult {
    let (server, _catalog) = TestServer::start().await;

    let (status, answer) = send_request(&server, Method::POST, TENANT_A, "/_search", Some(&json!({}))).await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_search_applies_the_index_options_of_its_query_string() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    let (no_index_status, no_index_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana_*/_search?allow_no_indices=false",
        None,
    )
    .await?;
    let (empty_wildcard_status, empty_wildcard_answer) =
        send_request(&server, Method::POST, TENANT_A, "/.kibana_*/_search", None).await?;
    let (skipped_name_status, skipped_name_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana_9/_search?ignore_unavailable=true",
        None,
    )
    .await?;
    let (disallowed_status, disallowed_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana_9/_search?ignore_unavailable=true&allow_no_indices=false",
        None,
    )
    .await?;
    let (empty_aggregations_status, empty_aggregations_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana_*/_search",
        Some(&json!({"aggs": {}})),
    )
    .await?;
    create_kibana_index(&server, TENANT_A).await?;
    put_saved_object(&server, TENANT_A, "dashboard:a", &json!({"type": "dashboard"})).await?;

    let (wildcard_status, wildcard_answer) =
        send_request(&server, Method::POST, TENANT_A, "/.kibana_*/_search", None).await?;
    let (ignored_status, ignored_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana_1,.kibana_9/_search?ignore_unavailable=true",
        None,
    )
    .await?;
    let (unavailable_status, unavailable_answer) =
        send_request(&server, Method::POST, TENANT_A, "/.kibana_1,.kibana_9/_search", None).await?;
    let (bad_option_status, bad_option_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana_1/_search?expand_wildcards=sideways",
        None,
    )
    .await?;

    assert_eq!(no_index_status, StatusCode::NOT_FOUND);
    assert_eq!(no_index_answer["error"]["type"], "index_not_found_exception");
    let expected_empty_hits = json!({"total": {"value": 0, "relation": "eq"}, "max_score": null, "hits": []});
    assert_eq!(empty_wildcard_status, StatusCode::OK, "{empty_wildcard_answer}");
    assert_eq!(empty_wildcard_answer["hits"], expected_empty_hits);
    assert_eq!(empty_wildcard_answer["timed_out"], false);
    assert_eq!(empty_wildcard_answer["_shards"], build_expected_shards());
    assert_eq!(skipped_name_status, StatusCode::OK, "{skipped_name_answer}");
    assert_eq!(skipped_name_answer["hits"], expected_empty_hits);
    assert_eq!(disallowed_status, StatusCode::NOT_FOUND);
    assert_eq!(disallowed_answer["error"]["type"], "index_not_found_exception");
    assert_eq!(empty_aggregations_status, StatusCode::BAD_REQUEST);
    assert_eq!(empty_aggregations_answer["error"]["type"], "illegal_argument_exception");
    let expected_total = json!({"value": 1, "relation": "eq"});
    assert_eq!(wildcard_status, StatusCode::OK, "{wildcard_answer}");
    assert_eq!(wildcard_answer["hits"]["total"], expected_total);
    assert_eq!(ignored_status, StatusCode::OK, "{ignored_answer}");
    assert_eq!(ignored_answer["hits"]["total"], expected_total);
    assert_eq!(unavailable_status, StatusCode::NOT_FOUND);
    assert_eq!(unavailable_answer["error"]["type"], "index_not_found_exception");
    assert_eq!(bad_option_status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_option_answer["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_search_refuses_an_unknown_parameter_and_accepts_an_ignored_one() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;

    let (refused_status, refused_answer) =
        send_request(&server, Method::POST, TENANT_A, "/.kibana/_search?unknown_knob=1", None).await?;
    let (accepted_status, accepted_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_search?preference=_local&timeout=30s&max_concurrent_shard_requests=5&ignore_throttled=false&search_type=query_then_fetch",
        None,
    )
    .await?;

    assert_eq!(refused_status, StatusCode::BAD_REQUEST);
    assert_eq!(refused_answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(accepted_status, StatusCode::OK, "{accepted_answer}");
    assert_eq!(accepted_answer["hits"]["total"], json!({"value": 0, "relation": "eq"}));
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn aggregations_on_a_system_index_are_a_400() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;

    for body in [
        json!({"size": 0, "aggs": {"types": {"terms": {"field": "type"}}}}),
        json!({"aggregations": {}}),
    ] {
        let (status, answer) = send_request(&server, Method::POST, TENANT_A, "/.kibana/_search", Some(&body)).await?;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(answer["error"]["type"], "illegal_argument_exception", "{body}");
    }
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_search_resolving_to_data_indices_is_a_400() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;

    for path in ["/logs/_search", "/.kibana_1,logs/_search"] {
        let (status, answer) = send_request(&server, Method::POST, TENANT_A, path, Some(&json!({}))).await?;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(answer["error"]["type"], "illegal_argument_exception", "{path}");
    }
    server.shutdown().await;
    Ok(())
}

/// Each sort clause copies a stored value into every hit, so a body far smaller than one document
/// multiplies the answer. With `_source` off the sort values are the only stored bytes: one clause
/// fits the bound and a second doubles them past it. Lengths stay unstated because `took` varies.
#[tokio::test]
async fn a_search_answer_multiplied_past_max_answer_bytes_by_its_sort_clauses_is_refused() -> TestResult {
    let server = start_server_with_answer_bound(6000).await?;
    let clause = json!({"config.buildNum": {"unmapped_type": "keyword"}});

    let (fitting_status, fitting_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_search",
        Some(&json!({"_source": false, "sort": [clause]})),
    )
    .await?;
    let (refused_status, refused_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_search",
        Some(&json!({"_source": false, "sort": [clause, clause]})),
    )
    .await?;

    assert_eq!(fitting_status, StatusCode::OK, "{fitting_answer}");
    assert_eq!(fitting_answer["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    assert_eq!(refused_status, StatusCode::BAD_REQUEST);
    assert_answer_bound_refusal(&refused_answer["error"]);
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn tenant_b_search_returns_none_of_tenant_a_documents() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    create_kibana_index(&server, TENANT_A).await?;
    create_kibana_index(&server, TENANT_B).await?;
    put_saved_object(&server, TENANT_A, "dashboard:a", &json!({"type": "dashboard"})).await?;
    put_saved_object(&server, TENANT_A, "dashboard:b", &json!({"type": "dashboard"})).await?;

    let (_, owner_answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/.kibana/_search?rest_total_hits_as_int=true",
        Some(&json!({})),
    )
    .await?;
    let (_, other_tenant_answer) = send_request(
        &server,
        Method::POST,
        TENANT_B,
        "/.kibana/_search?rest_total_hits_as_int=true",
        Some(&json!({})),
    )
    .await?;

    assert_eq!(owner_answer["hits"]["total"], 2);
    assert_eq!(
        other_tenant_answer["hits"],
        json!({"total": 0, "max_score": null, "hits": []})
    );
    server.shutdown().await;
    Ok(())
}
