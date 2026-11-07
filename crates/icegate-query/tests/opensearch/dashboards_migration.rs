//! Stateful saved-object call sequences: a first boot, a no-op upgrade, a reindexing upgrade, an
//! export and import round trip, and a reindexing upgrade at the snapshot bound.
//!
//! Each test runs one chain against one listener: every step is a literal request, sent in
//! order, with its status and the fields the chain depends on asserted. The chains are
//! Dashboards-style sequences (first boot, no-op boot, full migration) built from the call order
//! IceGate's saved-object contract describes. They pin IceGate's answers to literal
//! Dashboards-style requests; they do not show that a real Dashboards accepts them.
//!
//! The chains hold only requests that read or change system-index state, plus the two cluster
//! reads that frame a boot (`GET /_cat/plugins`, `GET /`). A read a chain repeats, such as
//! `GET /.kibana`, stays at each place it occurs, so the order holds. Left out, and where they
//! are pinned:
//! - `GET /_nodes`: it reads no system index; the `_nodes` tests pin its answer;
//! - `GET /_plugins/_query/_datasources` and routes IceGate does not serve: their own tests pin
//!   the empty list and the unmatched-route 400;
//! - a first page load after the no-op and reindexing upgrades: it is the first boot's page load
//!   again.
#![allow(clippy::expect_used)]

use std::error::Error;

use icegate_query::opensearch::{OpenSearchConfig, SystemIndicesConfig};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::harness::TestServer;
use super::requests::{TestResult, send_ndjson, send_request};

const TENANT: &str = "dashboards-demo";

/// Mapping hashes of the saved-object types, as opaque strings: IceGate stores and returns them
/// verbatim and never computes one.
fn build_current_hashes() -> Value {
    json!({
        "config": "c0ffee0000000000000000000000c0f1",
        "dashboard": "da5b0a4d00000000000000000000da51",
        "index-pattern": "1de5a77e00000000000000000000de51",
        "visualization": "715a11ce00000000000000000000715a",
        "migrationVersion": "4d1c4a7e00000000000000000000aa01",
        "references": "4ef5c0de00000000000000000000aa02",
        "type": "7e5d0a1b00000000000000000000aa03",
        "updated_at": "0a7ed00000000000000000000000aa04"
    })
}

/// A strict, versioned `.kibana` mapping with per-type hashes in `_meta`.
fn build_kibana_mappings(hashes: &Value) -> Value {
    json!({
        "dynamic": "strict",
        "properties": {
            "config": {"dynamic": "false", "properties": {"buildNum": {"type": "keyword"}}},
            "dashboard": {"dynamic": "false", "properties": {"title": {"type": "text"}}},
            "index-pattern": {
                "dynamic": "false",
                "properties": {"title": {"type": "text"}, "type": {"type": "keyword"}}
            },
            "visualization": {"dynamic": "false", "properties": {"title": {"type": "text"}}},
            "migrationVersion": {"dynamic": "true", "type": "object"},
            "namespace": {"type": "keyword"},
            "namespaces": {"type": "keyword"},
            "originId": {"type": "keyword"},
            "references": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "keyword"},
                    "type": {"type": "keyword"}
                }
            },
            "type": {"type": "keyword"},
            "updated_at": {"type": "date"}
        },
        "_meta": {"migrationMappingPropertyHashes": hashes}
    })
}

/// Index settings of the chains' `.kibana_<n>` indices.
fn build_kibana_settings() -> Value {
    json!({"number_of_shards": 1, "auto_expand_replicas": "0-1"})
}

/// Four saved objects at their current migration versions, as `(_id, _source)` pairs in `_id`
/// order: the form a chain reads documents back in.
fn build_saved_object_documents() -> Vec<(String, Value)> {
    [
        (
            "config:3.9.0",
            json!({
                "type": "config",
                "config": {"buildNum": 9000},
                "migrationVersion": {"config": "7.9.0"},
                "references": [],
                "updated_at": "2026-10-01T10:00:00.000Z"
            }),
        ),
        (
            "index-pattern:logs",
            json!({
                "type": "index-pattern",
                "index-pattern": {"title": "logs*", "timeFieldName": "timestamp"},
                "migrationVersion": {"index-pattern": "7.6.0"},
                "references": [],
                "updated_at": "2026-10-01T10:01:00.000Z"
            }),
        ),
        (
            "visualization:errors-by-service",
            json!({
                "type": "visualization",
                "visualization": {"title": "Errors by service"},
                "migrationVersion": {"visualization": "7.10.0"},
                "references": [{"id": "logs", "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}],
                "updated_at": "2026-10-01T10:02:00.000Z"
            }),
        ),
        (
            "visualization:volume",
            json!({
                "type": "visualization",
                "visualization": {"title": "Volume"},
                "migrationVersion": {"visualization": "7.10.0"},
                "references": [{"id": "logs", "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}],
                "updated_at": "2026-10-01T10:03:00.000Z"
            }),
        ),
    ]
    .into_iter()
    .map(|(id, source)| (id.to_owned(), source))
    .collect()
}

/// The migration-style "are documents outdated" count: for each type, documents of that type
/// whose `migrationVersion.<type>` is not the latest version.
fn build_outdated_documents_query() -> Value {
    let build_outdated_clause = |field: &str, latest: &str| {
        json!({"bool": {"must": [
            {"exists": {"field": field}},
            {"bool": {"must_not": {"term": {format!("migrationVersion.{field}"): latest}}}}
        ]}})
    };
    json!({"query": {"bool": {"should": [
        build_outdated_clause("config", "7.9.0"),
        build_outdated_clause("index-pattern", "7.6.0"),
        build_outdated_clause("visualization", "7.10.0")
    ]}}})
}

/// Builds a `_bulk` body of `action` (`index` or `create`) items writing `documents` into `index`,
/// one action line and one source line per document.
fn build_bulk_body(action: &str, index: &str, documents: &[(String, Value)]) -> String {
    let mut body = String::new();
    for (id, source) in documents {
        body.push_str(&json!({action: {"_index": index, "_id": id}}).to_string());
        body.push('\n');
        body.push_str(&source.to_string());
        body.push('\n');
    }
    body
}

/// Fails with `request`, the status and the answer unless the answer is `200 OK`.
fn require_ok(request: &str, status: StatusCode, body: &Value) -> TestResult {
    if status == StatusCode::OK {
        Ok(())
    } else {
        Err(format!("{request} answered {status}: {body}").into())
    }
}

/// Creates `.kibana_1` with `hashes` and points `.kibana` at it.
async fn create_aliased_kibana_1(server: &TestServer, tenant: &str, hashes: &Value) -> TestResult {
    let index = json!({"mappings": build_kibana_mappings(hashes), "settings": build_kibana_settings()});
    let (status, body) = send_request(server, Method::PUT, tenant, "/.kibana_1", Some(&index)).await?;
    require_ok("creating .kibana_1", status, &body)?;
    let alias = json!({"actions": [{"add": {"index": ".kibana_1", "alias": ".kibana"}}]});
    let (status, body) = send_request(server, Method::POST, tenant, "/_aliases", Some(&alias)).await?;
    require_ok("aliasing .kibana", status, &body)
}

/// Creates `.kibana_1` with `hashes`, points `.kibana` at it and writes the saved objects: the
/// state a previous boot left.
async fn set_up_kibana_1(server: &TestServer, tenant: &str, hashes: &Value) -> TestResult {
    create_aliased_kibana_1(server, tenant, hashes).await?;
    let bulk_body = build_bulk_body("index", ".kibana", &build_saved_object_documents());
    let (status, body) = send_ndjson(server, tenant, "/_bulk", bulk_body).await?;
    require_ok("writing the saved objects", status, &body)?;
    if body["errors"] != false {
        return Err(format!("writing the saved objects reported item errors: {body}").into());
    }
    Ok(())
}

/// The hits of a search or scroll answer as `(_id, _source)` pairs, in answer order.
fn read_hit_documents(answer: &Value) -> Result<Vec<(String, Value)>, Box<dyn Error>> {
    answer["hits"]["hits"]
        .as_array()
        .ok_or_else(|| format!("no hits array in {answer}"))?
        .iter()
        .map(|hit| -> Result<(String, Value), Box<dyn Error>> {
            let id = hit["_id"].as_str().ok_or_else(|| format!("a hit without _id: {hit}"))?;
            Ok((id.to_owned(), hit["_source"].clone()))
        })
        .collect()
}

/// Reads every document of `index` as `(_id, _source)` pairs; without a sort, hits come in `_id`
/// order.
async fn read_documents(
    server: &TestServer,
    tenant: &str,
    index: &str,
) -> Result<Vec<(String, Value)>, Box<dyn Error>> {
    let path = format!("/{index}/_search?size=100");
    let (status, body) = send_request(server, Method::POST, tenant, &path, Some(&json!({}))).await?;
    require_ok(&format!("searching {index}"), status, &body)?;
    read_hit_documents(&body)
}

fn assert_fully_successful_shards(body: &Value) {
    assert_eq!(
        body["_shards"],
        json!({"total": 1, "successful": 1, "skipped": 0, "failed": 0}),
        "{body}"
    );
}

/// A step of a reindexing migration that grows the stored state, so a snapshot bound can refuse it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationStep {
    /// Creating `.kibana_2` with the target mapping.
    CreateTarget,
    /// Bulk-writing the copied documents into `.kibana_2`.
    BulkCopy,
    /// Moving `.kibana` from `.kibana_1` onto `.kibana_2`.
    AliasSwap,
}

/// Where a reindexing migration stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationOutcome {
    /// Every step answered success.
    Completed,
    /// The step answered 413 and the alias still names `.kibana_1`.
    RefusedAt(MigrationStep),
}

/// Copies the saved objects of `.kibana_1` into a new `.kibana_2` created with `target_mappings`
/// and moves `.kibana` onto it: create the target, scroll the source, bulk the copy, drain and
/// clear the scroll, swap the alias. A step that grows the state and answers 413 ends the
/// migration there with [`MigrationOutcome::RefusedAt`]; any other failed or unexpected answer is
/// an error carrying it.
async fn reindex_kibana_1_into_kibana_2(
    server: &TestServer,
    tenant: &str,
    target_mappings: &Value,
) -> Result<MigrationOutcome, Box<dyn Error>> {
    let record_refusal =
        |step: MigrationStep, status: StatusCode, body: &Value| -> Result<MigrationOutcome, Box<dyn Error>> {
            if status == StatusCode::PAYLOAD_TOO_LARGE
                && body["status"] == 413
                && body["error"]["type"] == "illegal_argument_exception"
            {
                Ok(MigrationOutcome::RefusedAt(step))
            } else {
                Err(format!("{step:?} answered {status} instead of an illegal_argument_exception 413: {body}").into())
            }
        };
    let index = json!({"mappings": target_mappings, "settings": build_kibana_settings()});
    let (status, body) = send_request(server, Method::PUT, tenant, "/.kibana_2", Some(&index)).await?;
    if status != StatusCode::OK {
        return record_refusal(MigrationStep::CreateTarget, status, &body);
    }

    let (status, page) = send_request(
        server,
        Method::POST,
        tenant,
        "/.kibana_1/_search?scroll=15m",
        Some(&json!({"size": 100})),
    )
    .await?;
    require_ok("opening a scroll over .kibana_1", status, &page)?;
    let scroll_id = page["_scroll_id"].as_str().ok_or_else(|| format!("no _scroll_id in {page}"))?;
    let copied_documents = read_hit_documents(&page)?;
    if copied_documents != build_saved_object_documents() {
        return Err(format!("the scroll over .kibana_1 read {copied_documents:?}").into());
    }
    let bulk_body = build_bulk_body("index", ".kibana_2", &copied_documents);
    let (status, bulk_answer) = send_ndjson(server, tenant, "/_bulk", bulk_body).await?;
    if status != StatusCode::OK {
        return record_refusal(MigrationStep::BulkCopy, status, &bulk_answer);
    }
    if bulk_answer["errors"] != false {
        return Err(format!("the bulk copy reported item errors: {bulk_answer}").into());
    }
    let sequence_numbers = bulk_answer["items"]
        .as_array()
        .ok_or_else(|| format!("no items in {bulk_answer}"))?
        .iter()
        .map(|item| {
            if item["index"]["status"] != 201 {
                return Err(format!("a bulk copy item was not created: {item}"));
            }
            item["index"]["_seq_no"].as_i64().ok_or_else(|| format!("no _seq_no in {item}"))
        })
        .collect::<Result<Vec<i64>, String>>()?;
    if !sequence_numbers.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(format!("the bulk copy's _seq_no values do not increase: {sequence_numbers:?}").into());
    }
    let (status, next_page) = send_request(
        server,
        Method::GET,
        tenant,
        &format!("/_search/scroll/{scroll_id}?scroll=15m"),
        None,
    )
    .await?;
    require_ok("continuing the scroll", status, &next_page)?;
    if next_page["hits"]["hits"] != json!([]) {
        return Err(format!("the scroll continuation is not empty: {next_page}").into());
    }
    let (status, clear_answer) = send_request(
        server,
        Method::DELETE,
        tenant,
        &format!("/_search/scroll/{scroll_id}"),
        None,
    )
    .await?;
    require_ok("clearing the scroll", status, &clear_answer)?;
    if clear_answer["succeeded"] != true {
        return Err(format!("clearing the scroll did not succeed: {clear_answer}").into());
    }

    let (status, body) = send_request(server, Method::GET, tenant, "/_alias/.kibana", None).await?;
    require_ok("reading the .kibana alias", status, &body)?;
    if body != json!({".kibana_1": {"aliases": {".kibana": {}}}}) {
        return Err(format!("before the swap, .kibana does not name only .kibana_1: {body}").into());
    }
    let swap = json!({"actions": [
        {"remove": {"index": ".kibana_1", "alias": ".kibana"}},
        {"add": {"index": ".kibana_2", "alias": ".kibana"}}
    ]});
    let (status, body) = send_request(server, Method::POST, tenant, "/_aliases", Some(&swap)).await?;
    if status != StatusCode::OK {
        return record_refusal(MigrationStep::AliasSwap, status, &body);
    }
    Ok(MigrationOutcome::Completed)
}

#[tokio::test]
async fn first_boot_then_first_page_load_follows_the_migration_call_sequence() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    let version = OpenSearchConfig::default().version;

    for _ in 0..2 {
        let (status, body) = send_request(&server, Method::GET, TENANT, "/.kibana", None).await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["error"]["type"], "index_not_found_exception");
    }
    let (status, body) = send_request(
        &server,
        Method::GET,
        TENANT,
        "/_cat/templates/opensearch_dashboards_index_template*?format=json",
        None,
    )
    .await?;
    assert_eq!((status, body), (StatusCode::OK, json!([])));
    let (status, body) = send_request(
        &server,
        Method::PUT,
        TENANT,
        "/.kibana_1",
        Some(&json!({"mappings": build_kibana_mappings(&build_current_hashes()), "settings": build_kibana_settings()})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true);
    let (status, _) = send_request(&server, Method::GET, TENANT, "/_alias/.kibana", None).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT,
        "/_aliases",
        Some(&json!({"actions": [{"add": {"index": ".kibana_1", "alias": ".kibana"}}]})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true);
    let (status, _) = send_request(&server, Method::GET, TENANT, "/.kibana_1/_refresh", None).await?;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send_request(&server, Method::GET, TENANT, "/_cat/plugins?format=JSON", None).await?;
    assert_eq!(status, StatusCode::OK);
    let components: Vec<&str> = body
        .as_array()
        .expect("plugin list")
        .iter()
        .map(|plugin| plugin["component"].as_str().expect("component"))
        .collect();
    assert_eq!(components, ["opensearch-observability", "opensearch-sql"]);
    let (status, body) = send_request(&server, Method::GET, TENANT, "/", None).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["version"]["number"], version.as_str());

    // Ids are percent-encoded in the path.
    let config_path = format!("/.kibana/_doc/config%3A{version}");
    let (status, body) = send_request(&server, Method::GET, TENANT, &config_path, None).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["found"], false);
    assert_eq!(body["_id"], format!("config:{version}"));
    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT,
        "/.kibana/_search?size=1000&from=0&rest_total_hits_as_int=true",
        Some(&json!({
            "seq_no_primary_term": true,
            "query": {"bool": {"filter": [{"bool": {
                "should": [{"bool": {
                    "must": [{"term": {"type": "config"}}],
                    "must_not": [{"exists": {"field": "namespace"}}, {"exists": {"field": "namespaces"}}]
                }}],
                "minimum_should_match": 1
            }}]}},
            "sort": [{"config.buildNum": {"order": "desc", "unmapped_type": "keyword"}}]
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["hits"]["total"], 0);
    assert_eq!(body["hits"]["hits"], json!([]));
    let config_source = json!({
        "type": "config",
        "config": {"buildNum": 9000},
        "references": [],
        "migrationVersion": {"config": "7.9.0"},
        "updated_at": "2026-10-10T12:00:00.000Z"
    });
    let (status, create_answer) = send_request(
        &server,
        Method::PUT,
        TENANT,
        &format!("/.kibana/_create/config%3A{version}?refresh=wait_for"),
        Some(&config_source),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{create_answer}");
    assert_eq!(create_answer["result"], "created");
    assert_eq!(create_answer["_index"], ".kibana_1");
    assert!(create_answer["_seq_no"].is_i64(), "{create_answer}");
    assert_eq!(create_answer["_primary_term"], 1);
    let (status, body) = send_request(&server, Method::GET, TENANT, &config_path, None).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["found"], true);
    assert_eq!(body["_index"], ".kibana_1");
    assert_eq!(body["_source"], config_source);
    assert_eq!(body["_seq_no"], create_answer["_seq_no"]);
    assert_eq!(body["_primary_term"], 1);
    let (status, body) = send_request(
        &server,
        Method::GET,
        TENANT,
        &format!("/.kibana/_doc/config%3A%3Ccurrent_workspace%3E_{version}"),
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["found"], false);

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_no_op_upgrade_keeps_kibana_1_and_its_documents() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    set_up_kibana_1(&server, TENANT, &build_current_hashes()).await?;
    let documents_before = read_documents(&server, TENANT, ".kibana_1").await?;
    assert_eq!(documents_before, build_saved_object_documents());

    for _ in 0..2 {
        let (status, body) = send_request(&server, Method::GET, TENANT, "/.kibana", None).await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body[".kibana_1"]["aliases"], json!({".kibana": {}}));
        assert_eq!(
            body[".kibana_1"]["mappings"],
            build_kibana_mappings(&build_current_hashes())
        );
    }
    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT,
        "/.kibana/_count",
        Some(&build_outdated_documents_query()),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["count"], 0);
    assert_fully_successful_shards(&body);
    let (status, _) = send_request(&server, Method::GET, TENANT, "/.kibana", None).await?;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_request(&server, Method::GET, TENANT, "/_cat/plugins?format=JSON", None).await?;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_request(&server, Method::GET, TENANT, "/", None).await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_request(&server, Method::GET, TENANT, "/_alias/.kibana", None).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({".kibana_1": {"aliases": {".kibana": {}}}}));
    let (status, _) = send_request(&server, Method::GET, TENANT, "/.kibana_2", None).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(read_documents(&server, TENANT, ".kibana_1").await?, documents_before);

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_reindexing_upgrade_moves_the_alias_to_kibana_2_with_the_same_documents() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    let mut outdated_hashes = build_current_hashes();
    let hashes = outdated_hashes.as_object_mut().expect("hash map");
    hashes.remove("dashboard");
    hashes.remove("visualization");
    set_up_kibana_1(&server, TENANT, &outdated_hashes).await?;
    let documents_before = read_documents(&server, TENANT, ".kibana_1").await?;
    assert_eq!(documents_before, build_saved_object_documents());

    for _ in 0..2 {
        let (status, body) = send_request(&server, Method::GET, TENANT, "/.kibana", None).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body[".kibana_1"]["mappings"], build_kibana_mappings(&outdated_hashes));
    }
    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT,
        "/.kibana/_count",
        Some(&build_outdated_documents_query()),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 0);
    let (status, _) = send_request(&server, Method::GET, TENANT, "/.kibana", None).await?;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send_request(
        &server,
        Method::GET,
        TENANT,
        "/_cat/templates/opensearch_dashboards_index_template*?format=json",
        None,
    )
    .await?;
    assert_eq!((status, body), (StatusCode::OK, json!([])));
    let target_mappings = build_kibana_mappings(&build_current_hashes());
    assert_eq!(
        reindex_kibana_1_into_kibana_2(&server, TENANT, &target_mappings).await?,
        MigrationOutcome::Completed
    );
    let (status, _) = send_request(&server, Method::GET, TENANT, "/.kibana_2/_refresh", None).await?;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_request(&server, Method::GET, TENANT, "/_cat/plugins?format=JSON", None).await?;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send_request(&server, Method::GET, TENANT, "/", None).await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send_request(&server, Method::GET, TENANT, "/_alias/.kibana", None).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({".kibana_2": {"aliases": {".kibana": {}}}}));
    let (status, body) = send_request(&server, Method::GET, TENANT, "/.kibana_1", None).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body[".kibana_1"]["aliases"], json!({}));
    assert_eq!(read_documents(&server, TENANT, ".kibana_2").await?, documents_before);
    assert_eq!(read_documents(&server, TENANT, ".kibana_1").await?, documents_before);

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn saved_objects_round_trip_through_an_export_style_find_and_an_import_style_bulk() -> TestResult {
    let (server, _catalog) = TestServer::start().await;
    set_up_kibana_1(&server, "exporter", &build_current_hashes()).await?;
    create_aliased_kibana_1(&server, "importer", &build_current_hashes()).await?;

    // Without a sort, hits come in `_id` order.
    let (status, export_answer) = send_request(
        &server,
        Method::POST,
        "exporter",
        "/.kibana/_search?size=10000&from=0&rest_total_hits_as_int=true",
        Some(&json!({
            "seq_no_primary_term": true,
            "query": {"bool": {"filter": [{"bool": {
                "should": [{"term": {"type": "index-pattern"}}, {"term": {"type": "visualization"}}],
                "minimum_should_match": 1
            }}]}}
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{export_answer}");
    assert_eq!(export_answer["hits"]["total"], 3);
    let objects = read_hit_documents(&export_answer)?;
    let expected_documents: Vec<(String, Value)> = build_saved_object_documents()
        .into_iter()
        .filter(|(id, _)| !id.starts_with("config:"))
        .collect();
    assert_eq!(objects, expected_documents);
    for hit in export_answer["hits"]["hits"].as_array().expect("hits") {
        assert!(hit["_seq_no"].is_i64() && hit["_primary_term"] == 1, "{hit}");
    }

    let ids: Vec<Value> = objects.iter().map(|(id, _)| json!({"_id": id, "_index": ".kibana"})).collect();
    let (status, conflict_check_answer) =
        send_request(&server, Method::POST, "importer", "/_mget", Some(&json!({"docs": ids}))).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(
        conflict_check_answer["docs"]
            .as_array()
            .expect("docs")
            .iter()
            .all(|doc| doc["found"] == false),
        "{conflict_check_answer}"
    );
    let create_body = build_bulk_body("create", ".kibana", &objects);
    let (status, import_answer) =
        send_ndjson(&server, "importer", "/_bulk?refresh=wait_for", create_body.clone()).await?;
    assert_eq!(status, StatusCode::OK, "{import_answer}");
    assert_eq!(import_answer["errors"], false);
    assert!(
        import_answer["items"]
            .as_array()
            .expect("items")
            .iter()
            .all(|item| item["create"]["status"] == 201),
        "{import_answer}"
    );
    assert_eq!(
        read_documents(&server, "importer", ".kibana_1").await?,
        expected_documents
    );

    let (status, reimport_answer) = send_ndjson(&server, "importer", "/_bulk?refresh=wait_for", create_body).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reimport_answer["errors"], true);
    assert!(
        reimport_answer["items"]
            .as_array()
            .expect("items")
            .iter()
            .all(|item| item["create"]["status"] == 409),
        "{reimport_answer}"
    );

    server.shutdown().await;
    Ok(())
}

/// The bound under test; small enough that a few saved objects reach it.
const SMALL_SNAPSHOT_BOUND: usize = 65_536;

/// Sets up `.kibana_1` for `tenant` and reindexes it into a target mapping whose `_meta.padding`
/// holds `padding` ASCII characters; each character adds one byte to the decoded snapshot.
async fn migrate_with_padding(
    server: &TestServer,
    tenant: &str,
    padding: usize,
) -> Result<MigrationOutcome, Box<dyn Error>> {
    set_up_kibana_1(server, tenant, &build_current_hashes()).await?;
    let mut target_mappings = build_kibana_mappings(&build_current_hashes());
    target_mappings["_meta"]["padding"] = json!("x".repeat(padding));
    reindex_kibana_1_into_kibana_2(server, tenant, &target_mappings).await
}

#[tokio::test]
async fn a_reindexing_migration_at_the_largest_allowed_state_completes_and_one_byte_more_is_413() -> TestResult {
    let config = OpenSearchConfig {
        system_indices: SystemIndicesConfig {
            max_snapshot_bytes: SMALL_SNAPSHOT_BOUND,
            cache_max_bytes: SMALL_SNAPSHOT_BOUND,
            ..SystemIndicesConfig::default()
        },
        ..OpenSearchConfig::default()
    };
    let (server, _catalog) = TestServer::start_with_config(config).await;

    // Each probe runs under its own tenant, so it starts from an empty store.
    let mut largest_fitting_padding = 0;
    let mut smallest_refused_padding = SMALL_SNAPSHOT_BOUND;
    assert_eq!(
        migrate_with_padding(&server, "bound-low", largest_fitting_padding).await?,
        MigrationOutcome::Completed
    );
    assert_ne!(
        migrate_with_padding(&server, "bound-high", smallest_refused_padding).await?,
        MigrationOutcome::Completed
    );
    while smallest_refused_padding - largest_fitting_padding > 1 {
        let middle_padding = largest_fitting_padding + (smallest_refused_padding - largest_fitting_padding) / 2;
        match migrate_with_padding(&server, &format!("bound-{middle_padding}"), middle_padding).await? {
            MigrationOutcome::Completed => largest_fitting_padding = middle_padding,
            MigrationOutcome::RefusedAt(_) => smallest_refused_padding = middle_padding,
        }
    }

    // The largest padding that completes reaches the bound exactly: one more character, one
    // more decoded byte, is refused at the step that grows the state.
    assert!(largest_fitting_padding > 0, "four saved objects alone exceed the bound");
    let tenant = "bound-over";
    let outcome = migrate_with_padding(&server, tenant, largest_fitting_padding + 1).await?;
    assert!(
        matches!(
            outcome,
            MigrationOutcome::RefusedAt(MigrationStep::BulkCopy | MigrationStep::AliasSwap)
        ),
        "{outcome:?}"
    );
    let (status, body) = send_request(&server, Method::GET, tenant, "/_alias/.kibana", None).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({".kibana_1": {"aliases": {".kibana": {}}}}));
    let tenant = "bound-at";
    assert_eq!(
        migrate_with_padding(&server, tenant, largest_fitting_padding).await?,
        MigrationOutcome::Completed
    );
    let (_, body) = send_request(&server, Method::GET, tenant, "/_alias/.kibana", None).await?;
    assert_eq!(body, json!({".kibana_2": {"aliases": {".kibana": {}}}}));

    server.shutdown().await;
    Ok(())
}
