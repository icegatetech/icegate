//! System-index document endpoints over real HTTP. Every request is a literal and every expected
//! answer is stated in the test. The requests are saved-object-style: ids `<type>:<id>`,
//! `refresh=wait_for`, conditional updates.

use std::error::Error;

use futures::future::join_all;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::harness::TestServer;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// A counter-script-style source with its line breaks and indentation: a line break after the
/// opening backtick, 14- and 16-space indents, 12 spaces before the closing backtick.
const INCREMENT_COUNTER_SOURCE: &str = "\n              if (ctx._source[params.type][params.counterFieldName] == null) {\n                ctx._source[params.type][params.counterFieldName] = params.count;\n              }\n              else {\n                ctx._source[params.type][params.counterFieldName] += params.count;\n              }\n              ctx._source.updated_at = params.time;\n            ";

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
