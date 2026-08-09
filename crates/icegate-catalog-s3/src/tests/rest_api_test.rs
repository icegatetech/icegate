#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use bytes::Bytes;
use iceberg::io::MemoryStorageFactory;
use iceberg::spec::{
    FormatVersion, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary, TableMetadataBuilder,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder};
use icegate_common::schema::{logs_partition_spec, logs_schema, logs_sort_order};
use serde_json::Value;
use tower::ServiceExt;

use super::common::{
    InMemoryCatalogStorage, create_table, make_in_memory_catalog, make_in_memory_catalog_with_storage,
};
use crate::api::{CatalogApiConfig, CatalogPrefix, router};
use crate::storage::{CatalogStorage, LoadOutcome};

/// Router over `catalog` with page-size bounds small enough that any multi-item
/// fixture spans several pages.
fn build_router(catalog: Arc<crate::S3Catalog>) -> axum::Router {
    build_router_with_prefix(catalog, None)
}

fn build_router_with_prefix(catalog: Arc<crate::S3Catalog>, catalog_prefix: Option<CatalogPrefix>) -> axum::Router {
    router(catalog, api_config(catalog_prefix, "s3://warehouse/catalog"))
}

fn api_config(catalog_prefix: Option<CatalogPrefix>, warehouse_uri: &str) -> CatalogApiConfig {
    CatalogApiConfig::new(warehouse_uri.to_string(), catalog_prefix, 1, 2).expect("valid api config")
}

fn multisegment_prefix() -> CatalogPrefix {
    CatalogPrefix::try_from("ice/warehouses/my".to_string()).expect("valid catalog prefix")
}

/// Media type the pinned `REST_SPEC_REVISION` gives every catalog response that
/// carries content, success and error alike.
const CATALOG_MEDIA_TYPE: &str = "application/json";

struct ApiResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl ApiResponse {
    fn json(&self) -> Value {
        if self.body.is_empty() {
            return Value::Null;
        }
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|error| panic!("response body is not JSON: {error}; {:?}", self.body))
    }

    fn content_type(&self) -> Option<&str> {
        self.headers
            .get(header::CONTENT_TYPE)
            .map(|value| value.to_str().expect("content type"))
    }
}

async fn send_request(app: &axum::Router, request: Request<Body>) -> ApiResponse {
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX).await.expect("body");
    ApiResponse { status, headers, body }
}

async fn send(app: &axum::Router, method: &str, uri: &str) -> ApiResponse {
    send_request(
        app,
        Request::builder().method(method).uri(uri).body(Body::empty()).expect("request"),
    )
    .await
}

/// `GET uri` conditioned on `etag` — the request a client makes to revalidate a
/// representation it has already cached.
async fn send_revalidation(app: &axum::Router, uri: &str, etag: &HeaderValue) -> ApiResponse {
    send_request(
        app,
        Request::builder()
            .uri(uri)
            .header(header::IF_NONE_MATCH, etag)
            .body(Body::empty())
            .expect("request"),
    )
    .await
}

/// `GET uri`, checking the media type every JSON response owes the client on
/// the way through: the contract holds for error envelopes as much as for
/// success payloads, so it is asserted once here rather than per test.
async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = send(app, "GET", uri).await;
    assert_eq!(
        response
            .content_type()
            .map(|value| value.split(';').next().expect("media type")),
        Some(CATALOG_MEDIA_TYPE),
        "GET {uri} answered {} with content type {:?}",
        response.status,
        response.content_type(),
    );
    (response.status, response.json())
}

async fn head(app: &axum::Router, uri: &str) -> StatusCode {
    let response = send(app, "HEAD", uri).await;
    assert!(
        response.body.is_empty(),
        "HEAD {uri} must answer with an empty body, got {:?}",
        response.body
    );
    response.status
}

/// Give `table` three snapshots: `1` reachable from the `main` branch, `2`
/// reachable only from a tag, and `3` referenced by nothing. Refs of both
/// retention kinds keep their snapshots; only `3` is projected away.
///
/// The metadata is rewritten in place at the location the root already points
/// to, so the catalog serves it unchanged on the next `load_table`. Building a
/// snapshot through a real commit would need data files and a manifest writer —
/// far more machinery than the metadata projection under test needs.
async fn add_snapshots(storage: &InMemoryCatalogStorage, table: &iceberg::table::Table) {
    let metadata = table.metadata().clone();
    let branch_referenced = snapshot(1, 1, metadata.last_updated_ms() + 1);
    let tag_referenced = snapshot(2, 2, metadata.last_updated_ms() + 2);
    let unreferenced = snapshot(3, 3, metadata.last_updated_ms() + 3);
    let updated = metadata
        .into_builder(None)
        .set_branch_snapshot(branch_referenced, "main")
        .expect("set main branch snapshot")
        .add_snapshot(tag_referenced)
        .expect("add tagged snapshot")
        .set_ref(
            "release",
            SnapshotReference::new(2, SnapshotRetention::Tag { max_ref_age_ms: None }),
        )
        .expect("set release tag")
        .add_snapshot(unreferenced)
        .expect("add unreferenced snapshot")
        .build()
        .expect("build metadata")
        .metadata;

    storage
        .write_table_metadata(table.metadata_location().expect("metadata location"), &updated)
        .await
        .expect("write metadata");
}

fn snapshot(snapshot_id: i64, sequence_number: i64, timestamp_ms: i64) -> Snapshot {
    Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(timestamp_ms)
        .with_manifest_list(format!("memory://manifest-list-{snapshot_id}.avro"))
        .with_schema_id(0)
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::new(),
        })
        .build()
}

/// Assert the end-of-listing signal the pinned contract requires: the field is
/// present and `null`. Omitting it is how a server declares it does not paginate
/// at all, and `serde_json` indexing cannot tell the two apart.
fn assert_end_of_listing(json: &Value, uri: &str) {
    assert_eq!(json.get("next-page-token"), Some(&Value::Null), "GET {uri}");
}

fn snapshot_ids(metadata: &Value) -> Vec<i64> {
    let mut ids = metadata["snapshots"]
        .as_array()
        .expect("snapshots")
        .iter()
        .map(|snapshot| snapshot["snapshot-id"].as_i64().expect("snapshot-id"))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn config_advertises_supported_read_endpoints_with_spec_resource_paths() {
    let app = build_router(Arc::new(make_in_memory_catalog()));

    let (status, json) = get(&app, "/v1/config").await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["defaults"].is_object());
    assert!(json["overrides"].is_object());
    assert!(json["endpoints"].is_array());
    assert_eq!(json["overrides"]["namespace-separator"], "%1F");
    // Java `Endpoint.check` matches these OpenAPI operation identifiers by
    // exact string, before substituting the configured catalog prefix in an
    // outgoing request.
    assert_eq!(
        json["endpoints"],
        serde_json::json!([
            "GET /v1/config",
            "GET /v1/{prefix}/namespaces",
            "GET /v1/{prefix}/namespaces/{namespace}",
            "HEAD /v1/{prefix}/namespaces/{namespace}",
            "GET /v1/{prefix}/namespaces/{namespace}/tables",
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        ])
    );
}

/// Guard the router and `GET /v1/config` against drifting apart: every endpoint
/// the server advertises must answer at the address a client derives from it —
/// which is exactly the advertised resource path with the advertised
/// `overrides.prefix` substituted in.
#[tokio::test]
async fn every_advertised_endpoint_is_routed_with_and_without_a_prefix() {
    for (catalog_prefix, mount) in [(None, "/v1"), (Some(multisegment_prefix()), "/v1/ice/warehouses/my")] {
        let catalog = Arc::new(make_in_memory_catalog());
        create_table(&catalog, &NamespaceIdent::new("ns1".to_string()), "tbl").await;
        let advertised_prefix = catalog_prefix.as_ref().map(|prefix| prefix.as_str().to_string());
        let app = build_router_with_prefix(catalog, catalog_prefix);

        let config = get(&app, "/v1/config").await.1;
        // The prefix a client substitutes into `{prefix}` comes from this
        // override; the mount below must be derivable from it alone.
        assert_eq!(
            config["overrides"].get("prefix").and_then(Value::as_str),
            advertised_prefix.as_deref(),
        );
        let advertised = config["endpoints"]
            .as_array()
            .expect("endpoints")
            .iter()
            .map(|endpoint| endpoint.as_str().expect("endpoint").to_string())
            .collect::<Vec<_>>();
        assert_eq!(advertised.len(), 7);

        for endpoint in advertised {
            let (method, resource_path) = endpoint.split_once(' ').expect("advertised endpoint");
            let uri = resource_path
                .replace("/v1/{prefix}", mount)
                .replace("{namespace}", "ns1")
                .replace("{table}", "tbl");
            let response = send(&app, method, &uri).await;
            assert!(
                matches!(response.status, StatusCode::OK | StatusCode::NO_CONTENT),
                "{method} {uri} is advertised but answered {}",
                response.status
            );
        }
    }
}

#[tokio::test]
async fn config_rejects_unknown_warehouse_with_the_iceberg_error_envelope() {
    let app = build_router(Arc::new(make_in_memory_catalog()));

    let (status, json) = get(&app, "/v1/config?warehouse=s3://other/catalog").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json["error"]["type"], "NoSuchWarehouseException");
    assert_eq!(json["error"]["code"], 404);
    assert!(json["error"]["message"].is_string());
}

/// The spec's own `getConfig` example writes the warehouse as
/// `s3://bucket/warehouse/`: a trailing slash names the same warehouse, so a
/// client bootstrapping with one must not be turned away with a 404.
#[tokio::test]
async fn config_accepts_the_served_warehouse_with_and_without_a_trailing_slash() {
    let app = build_router(Arc::new(make_in_memory_catalog()));

    for warehouse in ["s3://warehouse/catalog", "s3://warehouse/catalog/"] {
        let (status, json) = get(&app, &format!("/v1/config?warehouse={warehouse}")).await;

        assert_eq!(status, StatusCode::OK, "warehouse {warehouse:?}");
        assert_eq!(
            json["overrides"]["warehouse"], "s3://warehouse/catalog",
            "warehouse {warehouse:?}",
        );
    }
}

#[tokio::test]
async fn multipart_namespace_with_dot_and_table_name_with_dot_round_trip() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace =
        NamespaceIdent::from_vec(vec!["dogs".to_string(), "owners.and.handlers".to_string()]).expect("namespace");
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    create_table(&catalog, &namespace, "events.v2").await;

    let app = build_router(catalog);
    let (status, json) = get(&app, "/v1/namespaces/dogs%1Fowners.and.handlers/tables").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json["identifiers"][0]["namespace"],
        serde_json::json!(["dogs", "owners.and.handlers"])
    );
    assert_eq!(json["identifiers"][0]["name"], "events.v2");
}

/// The `parent` query parameter runs through a different decoder than the path
/// segment, so a nested parent needs its own round-trip.
#[tokio::test]
async fn nested_parent_query_lists_direct_child_namespaces() {
    let catalog = Arc::new(make_in_memory_catalog());
    for parts in [vec!["a"], vec!["a", "b"], vec!["a", "b", "c"], vec!["a", "b", "d"]] {
        let namespace =
            NamespaceIdent::from_vec(parts.into_iter().map(ToString::to_string).collect()).expect("namespace");
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
    }

    let app = build_router(catalog);
    let (status, json) = get(&app, "/v1/namespaces?parent=a%1Fb").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json["namespaces"],
        serde_json::json!([["a", "b", "c"], ["a", "b", "d"]])
    );
}

#[tokio::test]
async fn missing_namespace_errors_follow_catalog_contract() {
    let app = build_router(Arc::new(make_in_memory_catalog()));

    for uri in [
        "/v1/namespaces?parent=missing",
        "/v1/namespaces/missing",
        "/v1/namespaces/missing/tables",
    ] {
        let (status, json) = get(&app, uri).await;

        assert_eq!(status, StatusCode::NOT_FOUND, "GET {uri}");
        assert_eq!(json["error"]["type"], "NoSuchNamespaceException", "GET {uri}");
        assert_eq!(json["error"]["code"], 404, "GET {uri}");
        assert!(json["error"]["message"].is_string(), "GET {uri}");
    }
}

#[tokio::test]
async fn head_reports_existence_of_namespaces_and_tables() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace = NamespaceIdent::new("ns1".to_string());
    create_table(&catalog, &namespace, "tbl").await;

    let app = build_router(catalog);

    assert_eq!(head(&app, "/v1/namespaces/ns1").await, StatusCode::NO_CONTENT);
    assert_eq!(
        head(&app, "/v1/namespaces/ns1/tables/tbl").await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(head(&app, "/v1/namespaces/missing").await, StatusCode::NOT_FOUND);
    assert_eq!(
        head(&app, "/v1/namespaces/ns1/tables/missing").await,
        StatusCode::NOT_FOUND
    );
}

/// Zero-element collections are answers, not errors: an empty catalog, a
/// `parent=` value that decodes to no parent at all, and an existing namespace
/// with no tables each owe the client an empty array plus the end-of-listing
/// signal.
#[tokio::test]
async fn empty_collections_list_as_empty_arrays_with_end_of_listing() {
    let empty_catalog_app = build_router(Arc::new(make_in_memory_catalog()));
    let (status, json) = get(&empty_catalog_app, "/v1/namespaces").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["namespaces"], serde_json::json!([]));
    assert_end_of_listing(&json, "/v1/namespaces");

    let catalog = Arc::new(make_in_memory_catalog());
    catalog
        .create_namespace(&NamespaceIdent::new("ns1".to_string()), HashMap::new())
        .await
        .expect("create namespace");
    let app = build_router(catalog);

    // An empty `parent=` names no parent, so it must answer exactly like the
    // parameterless top-level listing rather than 404 on a namespace named "".
    for uri in ["/v1/namespaces", "/v1/namespaces?parent="] {
        let (status, json) = get(&app, uri).await;
        assert_eq!(status, StatusCode::OK, "GET {uri}");
        assert_eq!(json["namespaces"], serde_json::json!([["ns1"]]), "GET {uri}");
        assert_end_of_listing(&json, uri);
    }

    let (status, json) = get(&app, "/v1/namespaces/ns1/tables").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["identifiers"], serde_json::json!([]));
    assert_end_of_listing(&json, "/v1/namespaces/ns1/tables");
}

#[tokio::test]
async fn get_namespace_returns_the_exact_stored_properties() {
    let catalog = Arc::new(make_in_memory_catalog());
    let properties = HashMap::from([
        ("owner".to_string(), "dogs".to_string()),
        ("comment".to_string(), "good boys".to_string()),
    ]);
    catalog
        .create_namespace(&NamespaceIdent::new("ns1".to_string()), properties)
        .await
        .expect("create namespace");

    let app = build_router(catalog);
    let (status, json) = get(&app, "/v1/namespaces/ns1").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["namespace"], serde_json::json!(["ns1"]));
    assert_eq!(
        json["properties"],
        serde_json::json!({"owner": "dogs", "comment": "good boys"})
    );
}

#[tokio::test]
async fn routing_mode_disambiguates_prefix_and_namespace_named_namespaces() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace = NamespaceIdent::new("namespaces".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let prefixless = build_router(Arc::clone(&catalog));
    assert_eq!(get(&prefixless, "/v1/namespaces/namespaces").await.0, StatusCode::OK);

    let prefixed = build_router_with_prefix(
        catalog,
        Some(CatalogPrefix::try_from("namespaces".to_string()).expect("valid catalog prefix")),
    );
    assert_eq!(get(&prefixed, "/v1/namespaces").await.0, StatusCode::NOT_FOUND);
    assert_eq!(
        get(&prefixed, "/v1/namespaces/namespaces").await.1["namespaces"],
        serde_json::json!([["namespaces"]])
    );
    assert_eq!(
        get(&prefixed, "/v1/namespaces/namespaces/namespaces").await.1["namespace"],
        serde_json::json!(["namespaces"])
    );
}

#[tokio::test]
async fn literal_encoded_separator_round_trips_in_namespace_path() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace = NamespaceIdent::new("literal%1Ftext".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let app = build_router(catalog);

    let (status, json) = get(&app, "/v1/namespaces/literal%251Ftext").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["namespace"], serde_json::json!(["literal%1Ftext"]));
}

#[tokio::test]
async fn literal_encoded_separator_round_trips_in_parent_query() {
    let catalog = Arc::new(make_in_memory_catalog());
    for parts in [vec!["root%1Fpart"], vec!["root%1Fpart", "child"]] {
        let namespace =
            NamespaceIdent::from_vec(parts.into_iter().map(ToString::to_string).collect()).expect("namespace");
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
    }
    let app = build_router(catalog);

    let (status, json) = get(&app, "/v1/namespaces?parent=root%251Fpart").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["namespaces"], serde_json::json!([["root%1Fpart", "child"]]));
}

#[tokio::test]
async fn load_table_returns_metadata_and_its_location() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let metadata_location = table.metadata_location().expect("metadata location").to_string();

    let app = build_router(catalog);
    let (status, json) = get(&app, "/v1/namespaces/ns1/tables/tbl").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["metadata-location"], metadata_location);
    assert!(json["metadata"].is_object());
    assert!(json["config"].is_object());
    assert_eq!(json["metadata"]["table-uuid"], table.metadata().uuid().to_string());
}

#[tokio::test]
async fn load_table_reports_missing_table() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let app = build_router(catalog);
    let (status, _) = get(&app, "/v1/namespaces/ns1/tables/missing").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// A table between creation and its first commit has no snapshots at all: its
/// v2 metadata omits `snapshots` entirely and carries an empty `refs`. Both
/// selections must still serve it.
#[tokio::test]
async fn load_table_serves_table_without_snapshots_for_every_selection() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace = NamespaceIdent::new("ns1".to_string());
    create_table(&catalog, &namespace, "tbl").await;

    let app = build_router(catalog);
    let (refs_status, refs_json) = get(&app, "/v1/namespaces/ns1/tables/tbl?snapshots=refs").await;
    let (all_status, all_json) = get(&app, "/v1/namespaces/ns1/tables/tbl?snapshots=all").await;

    assert_eq!(refs_status, StatusCode::OK);
    assert_eq!(all_status, StatusCode::OK);
    assert!(refs_json["metadata"]["snapshots"].as_array().is_none_or(Vec::is_empty));
    assert!(all_json["metadata"]["snapshots"].as_array().is_none_or(Vec::is_empty));
}

/// `refs` keeps every snapshot reachable through a ref of either retention
/// kind — branch and tag — and drops only the unreferenced one; the default
/// selection of a query without `snapshots` is `all`, so it must serve the
/// same full snapshot history as an explicit `snapshots=all`.
#[tokio::test]
async fn load_table_selects_snapshots_by_refs() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let catalog = Arc::new(catalog);
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    add_snapshots(&storage, &table).await;

    let app = build_router(catalog);
    let (refs_status, refs_json) = get(&app, "/v1/namespaces/ns1/tables/tbl?snapshots=refs").await;
    let (all_status, all_json) = get(&app, "/v1/namespaces/ns1/tables/tbl?snapshots=all").await;
    let (default_status, default_json) = get(&app, "/v1/namespaces/ns1/tables/tbl").await;

    assert_eq!(refs_status, StatusCode::OK);
    assert_eq!(snapshot_ids(&refs_json["metadata"]), vec![1, 2]);
    assert_eq!(all_status, StatusCode::OK);
    assert_eq!(snapshot_ids(&all_json["metadata"]), vec![1, 2, 3]);
    // Compared by snapshot ids, not full payloads: the `snapshots` array order
    // is not part of the contract and differs between serializations.
    assert_eq!(default_status, StatusCode::OK);
    assert_eq!(snapshot_ids(&default_json["metadata"]), vec![1, 2, 3]);
}

/// Format v1 metadata serializes without a `refs` field at all, so the `refs`
/// projection must fall back to the main branch implied by
/// `current-snapshot-id` instead of filtering every snapshot out. The fixture
/// enters through the public `Catalog::register_table` — the production path
/// by which a v1 table reaches this catalog.
#[tokio::test]
async fn load_table_serves_refs_selection_for_a_registered_format_v1_table() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let catalog = Arc::new(catalog);
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let schema = logs_schema().expect("logs schema");
    let partition_spec = logs_partition_spec(&schema).expect("logs partition spec");
    let sort_order = logs_sort_order(&schema).expect("logs sort order");
    let metadata = TableMetadataBuilder::new(
        schema,
        partition_spec.into_unbound(),
        sort_order,
        "memory://catalog/tables/v1-table".to_string(),
        FormatVersion::V1,
        HashMap::new(),
    )
    .expect("metadata builder")
    .set_branch_snapshot(snapshot(1, 0, 1_000), "main")
    .expect("set main branch snapshot")
    .add_snapshot(snapshot(2, 0, 2_000))
    .expect("add unreferenced snapshot")
    .build()
    .expect("table metadata")
    .metadata;
    let metadata_location = "memory://catalog/tables/v1-table/metadata/00000-v1.metadata.json".to_string();
    storage
        .write_table_metadata(&metadata_location, &metadata)
        .await
        .expect("write metadata");
    catalog
        .register_table(&TableIdent::new(namespace, "tbl".to_string()), metadata_location)
        .await
        .expect("register v1 table");

    let app = build_router(catalog);
    let (all_status, all_json) = get(&app, "/v1/namespaces/ns1/tables/tbl?snapshots=all").await;
    let (refs_status, refs_json) = get(&app, "/v1/namespaces/ns1/tables/tbl?snapshots=refs").await;

    // The fixture must actually exercise the v1 fallback: a metadata payload
    // that carried `refs` would take the ordinary projection path and this
    // test would prove nothing about v1.
    assert_eq!(all_status, StatusCode::OK);
    assert!(
        all_json["metadata"].get("refs").is_none(),
        "format v1 metadata must serialize without a refs field",
    );
    assert_eq!(all_json["metadata"]["format-version"], 1);
    assert_eq!(snapshot_ids(&all_json["metadata"]), vec![1, 2]);
    assert_eq!(refs_status, StatusCode::OK);
    assert_eq!(snapshot_ids(&refs_json["metadata"]), vec![1]);
}

/// `pageToken` is the only signal that opens pagination: without it the server
/// owes the client the whole collection and a `null` continuation token,
/// however large the collection or the requested `pageSize`.
#[tokio::test]
async fn a_list_request_without_a_page_token_returns_every_result() {
    let catalog = Arc::new(make_in_memory_catalog());
    for name in ["a", "b", "c"] {
        catalog
            .create_namespace(&NamespaceIdent::new(name.to_string()), HashMap::new())
            .await
            .expect("create namespace");
        create_table(&catalog, &NamespaceIdent::new("ns1".to_string()), name).await;
    }
    let app = build_router(catalog);

    for uri in ["/v1/namespaces", "/v1/namespaces?pageSize=2"] {
        let (status, json) = get(&app, uri).await;

        assert_eq!(status, StatusCode::OK, "GET {uri}");
        assert_eq!(
            json["namespaces"],
            serde_json::json!([["a"], ["b"], ["c"], ["ns1"]]),
            "GET {uri}",
        );
        assert_end_of_listing(&json, uri);
    }

    for uri in ["/v1/namespaces/ns1/tables", "/v1/namespaces/ns1/tables?pageSize=2"] {
        let (status, json) = get(&app, uri).await;

        assert_eq!(status, StatusCode::OK, "GET {uri}");
        assert_eq!(
            json["identifiers"]
                .as_array()
                .expect("identifiers")
                .iter()
                .map(|identifier| identifier["name"].as_str().expect("name"))
                .collect::<Vec<_>>(),
            ["a", "b", "c"],
            "GET {uri}",
        );
        assert_end_of_listing(&json, uri);
    }
}

/// `pageSize` has a minimum of one, so zero is malformed whether or not the
/// request also opens pagination.
#[tokio::test]
async fn zero_page_size_is_rejected_with_and_without_a_page_token() {
    let catalog = Arc::new(make_in_memory_catalog());
    catalog
        .create_namespace(&NamespaceIdent::new("a".to_string()), HashMap::new())
        .await
        .expect("create namespace");
    let app = build_router(catalog);

    assert_eq!(get(&app, "/v1/namespaces?pageSize=0").await.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        get(&app, "/v1/namespaces?pageToken=&pageSize=0").await.0,
        StatusCode::BAD_REQUEST
    );
}

/// Axum renders an extractor rejection as `text/plain`, and a rejected request
/// reaches neither a handler nor a fallback — so a malformed parameter is
/// exactly the case where a Java client, parsing every body through
/// `ErrorHandlers.defaultErrorHandler`, would get a bare `RESTException` instead
/// of the `BadRequestException` the envelope names.
#[tokio::test]
async fn malformed_query_and_path_parameters_are_rejected_with_the_iceberg_envelope() {
    let catalog = Arc::new(make_in_memory_catalog());
    create_table(&catalog, &NamespaceIdent::new("ns1".to_string()), "tbl").await;
    let app = build_router(catalog);

    for uri in [
        "/v1/namespaces?pageSize=abc",
        "/v1/namespaces?pageSize=-1",
        "/v1/namespaces?pageSize=99999999999999999999",
        "/v1/namespaces?pageToken=&pageSize=abc",
        "/v1/namespaces/ns1/tables?pageSize=abc",
        "/v1/namespaces/ns1/tables?pageSize=-1",
        "/v1/namespaces/ns1/tables?pageSize=99999999999999999999",
        "/v1/namespaces/ns1/tables/tbl?snapshots=bogus",
        "/v1/namespaces/ns1/tables/tbl?snapshots=",
        "/v1/namespaces/%FF",
        "/v1/namespaces/%FF/tables",
        "/v1/namespaces/ns1/tables/%FF",
    ] {
        let (status, json) = get(&app, uri).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "GET {uri}");
        assert_eq!(json["error"]["type"], "BadRequestException", "GET {uri}");
        assert_eq!(json["error"]["code"], 400, "GET {uri}");
        assert!(json["error"]["message"].is_string(), "GET {uri}");
    }
}

/// RFC 9110 forbids content on a `HEAD` response while requiring its headers to
/// stay those of the `GET` being probed. Axum drops the body of every top-level
/// `HEAD` response itself, rejections included, having first measured it into
/// `Content-Length` — so both halves are asserted here: an empty body alone
/// cannot tell that guarantee apart from a layer that zeroed the header too.
#[tokio::test]
async fn head_with_a_malformed_path_parameter_is_rejected_without_content() {
    let app = build_router(Arc::new(make_in_memory_catalog()));

    for uri in ["/v1/namespaces/%FF", "/v1/namespaces/%FF/tables/tbl"] {
        let probed = send(&app, "GET", uri).await;
        let response = send(&app, "HEAD", uri).await;

        assert_eq!(response.status, StatusCode::BAD_REQUEST, "HEAD {uri}");
        assert!(response.body.is_empty(), "HEAD {uri} answered with {:?}", response.body);
        assert_eq!(
            response
                .content_type()
                .map(|value| value.split(';').next().expect("media type")),
            Some(CATALOG_MEDIA_TYPE),
            "HEAD {uri}",
        );
        let probed_content_length = probed.body.len().to_string();
        assert_eq!(
            response
                .headers
                .get(header::CONTENT_LENGTH)
                .map(|value| value.to_str().expect("content length")),
            Some(probed_content_length.as_str()),
            "HEAD {uri} must report the content length of the GET it probes",
        );
    }
}

#[tokio::test]
async fn pagination_honors_page_size_bounds() {
    let catalog = Arc::new(make_in_memory_catalog());
    for name in ["a", "b", "c"] {
        catalog
            .create_namespace(&NamespaceIdent::new(name.to_string()), HashMap::new())
            .await
            .expect("create namespace");
    }
    let app = build_router(catalog);

    // Clamped to the configured maximum of two.
    assert_eq!(
        get(&app, "/v1/namespaces?pageToken=&pageSize=99").await.1["namespaces"]
            .as_array()
            .expect("namespaces")
            .len(),
        2
    );
    assert_eq!(
        get(&app, "/v1/namespaces?pageToken=&pageSize=1").await.1["namespaces"]
            .as_array()
            .expect("namespaces")
            .len(),
        1
    );
    // The configured default of one applies only once pagination is open.
    assert_eq!(
        get(&app, "/v1/namespaces?pageToken=").await.1["namespaces"]
            .as_array()
            .expect("namespaces")
            .len(),
        1
    );
}

#[tokio::test]
async fn router_preserves_error_fallbacks_for_configured_prefix_paths() {
    let app = build_router_with_prefix(
        Arc::new(make_in_memory_catalog()),
        Some(CatalogPrefix::try_from("catalog-a".to_string()).expect("valid catalog prefix")),
    );

    let (unknown_status, unknown) = get(&app, "/v1/catalog-a/unknown").await;
    assert_eq!(unknown_status, StatusCode::NOT_FOUND);
    assert_eq!(unknown["error"]["type"], "NotFoundException");
    assert_eq!(unknown["error"]["code"], 404);
    assert!(unknown["error"]["message"].is_string());

    let unsupported_method = send(&app, "POST", "/v1/catalog-a/namespaces").await;
    assert_eq!(unsupported_method.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        unsupported_method
            .content_type()
            .map(|value| value.split(';').next().expect("media type")),
        Some(CATALOG_MEDIA_TYPE)
    );
    assert_eq!(unsupported_method.json()["error"]["type"], "MethodNotAllowedException");
    assert_eq!(unsupported_method.json()["error"]["code"], 405);
    assert!(unsupported_method.json()["error"]["message"].is_string());
}

/// An empty `pageToken` opens pagination — the form every Java client sends on
/// its first request.
#[tokio::test]
async fn empty_page_token_opens_the_first_page() {
    let catalog = Arc::new(make_in_memory_catalog());
    for name in ["a", "b", "c"] {
        catalog
            .create_namespace(&NamespaceIdent::new(name.to_string()), HashMap::new())
            .await
            .expect("create namespace");
    }

    let app = build_router(catalog);
    let (status, json) = get(&app, "/v1/namespaces?pageToken=").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["namespaces"], serde_json::json!([["a"]]));
    assert!(json["next-page-token"].is_string());
}

#[tokio::test]
async fn paging_namespaces_to_exhaustion_yields_every_namespace_once() {
    let catalog = Arc::new(make_in_memory_catalog());
    let expected = ["a", "b", "c", "d", "e"];
    for name in expected {
        catalog
            .create_namespace(&NamespaceIdent::new(name.to_string()), HashMap::new())
            .await
            .expect("create namespace");
    }

    let app = build_router(catalog);
    let mut collected: Vec<Value> = Vec::new();
    let mut page_token = String::new();
    loop {
        let uri = format!("/v1/namespaces?pageToken={page_token}");
        let (status, json) = get(&app, &uri).await;
        assert_eq!(status, StatusCode::OK);
        collected.extend(json["namespaces"].as_array().expect("namespaces").iter().cloned());
        match json["next-page-token"].as_str() {
            Some(token) => page_token = token.to_string(),
            None => {
                assert_end_of_listing(&json, &uri);
                break;
            }
        }
    }

    assert_eq!(
        collected,
        expected.iter().map(|name| serde_json::json!([name])).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn paging_tables_to_exhaustion_yields_every_table_once() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace = NamespaceIdent::new("ns1".to_string());
    let expected = ["a", "b", "c", "d", "e"];
    for name in expected {
        create_table(&catalog, &namespace, name).await;
    }

    let app = build_router(catalog);
    let mut collected: Vec<String> = Vec::new();
    let mut page_token = String::new();
    loop {
        let uri = format!("/v1/namespaces/ns1/tables?pageToken={page_token}");
        let (status, json) = get(&app, &uri).await;
        assert_eq!(status, StatusCode::OK);
        collected.extend(
            json["identifiers"]
                .as_array()
                .expect("identifiers")
                .iter()
                .map(|identifier| identifier["name"].as_str().expect("name").to_string()),
        );
        match json["next-page-token"].as_str() {
            Some(token) => page_token = token.to_string(),
            None => {
                assert_end_of_listing(&json, &uri);
                break;
            }
        }
    }

    assert_eq!(collected, expected);
}

/// A continuation token is opaque to the client, so the server must reject a
/// token it could not have issued at every decoding stage: transport encoding,
/// payload shape, and payload version.
#[tokio::test]
async fn page_tokens_the_server_never_issued_are_rejected_with_the_iceberg_envelope() {
    let catalog = Arc::new(make_in_memory_catalog());
    catalog
        .create_namespace(&NamespaceIdent::new("a".to_string()), HashMap::new())
        .await
        .expect("create namespace");
    let app = build_router(catalog);

    // The shape below matches the issued token with only the version changed,
    // so its rejection isolates the version guard rather than the JSON parser.
    let unsupported_version = serde_json::json!({
        "version": 99,
        "operation": "namespaces",
        "scope": null,
        "last_identifier": {"namespace": ["a"], "name": null},
    });
    for (case, token) in [
        ("malformed Base64", "not-a-token!!".to_string()),
        ("valid Base64 of invalid JSON", URL_SAFE_NO_PAD.encode(b"{not json")),
        (
            "unsupported token version",
            URL_SAFE_NO_PAD.encode(unsupported_version.to_string()),
        ),
    ] {
        let (status, json) = get(&app, &format!("/v1/namespaces?pageToken={token}")).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{case}");
        assert_eq!(json["error"]["type"], "BadRequestException", "{case}");
        assert_eq!(json["error"]["code"], 400, "{case}");
        assert!(json["error"]["message"].is_string(), "{case}");
    }
}

/// `pageSize` is a per-request bound, not a session setting: a client may
/// resize every follow-up request and the cursor must keep the listing
/// lossless and duplicate-free across the changes.
#[tokio::test]
async fn a_client_may_change_page_size_between_pages_of_one_listing() {
    let catalog = Arc::new(make_in_memory_catalog());
    for name in ["a", "b", "c", "d"] {
        catalog
            .create_namespace(&NamespaceIdent::new(name.to_string()), HashMap::new())
            .await
            .expect("create namespace");
    }
    let app = build_router(catalog);

    let (_, first) = get(&app, "/v1/namespaces?pageToken=&pageSize=1").await;
    assert_eq!(first["namespaces"], serde_json::json!([["a"]]));
    let token = first["next-page-token"].as_str().expect("first continuation token");

    let (_, second) = get(&app, &format!("/v1/namespaces?pageToken={token}&pageSize=2")).await;
    assert_eq!(second["namespaces"], serde_json::json!([["b"], ["c"]]));
    let token = second["next-page-token"].as_str().expect("second continuation token");

    // No `pageSize` at all: the configured default of one takes over.
    let (_, third) = get(&app, &format!("/v1/namespaces?pageToken={token}")).await;
    assert_eq!(third["namespaces"], serde_json::json!([["d"]]));
    assert_end_of_listing(&third, "third page");
}

#[tokio::test]
async fn page_token_from_another_scope_is_rejected() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace = NamespaceIdent::new("ns1".to_string());
    for name in ["a", "b"] {
        create_table(&catalog, &namespace, name).await;
    }
    create_table(&catalog, &NamespaceIdent::new("ns2".to_string()), "a").await;

    let app = build_router(catalog);
    let (_, first_page) = get(&app, "/v1/namespaces/ns1/tables?pageToken=").await;
    let token = first_page["next-page-token"].as_str().expect("next-page-token");

    let (status, _) = get(&app, &format!("/v1/namespaces/ns2/tables?pageToken={token}")).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Namespaces and tables under the same parent share a scope, so only the
/// encoded operation keeps their cursors from being interchangeable.
#[tokio::test]
async fn page_token_from_another_operation_in_the_same_scope_is_rejected() {
    let catalog = Arc::new(make_in_memory_catalog());
    let parent = NamespaceIdent::new("ns1".to_string());
    for name in ["a", "b"] {
        create_table(&catalog, &parent, name).await;
        catalog
            .create_namespace(
                &NamespaceIdent::from_vec(vec!["ns1".to_string(), name.to_string()]).expect("namespace"),
                HashMap::new(),
            )
            .await
            .expect("create namespace");
    }

    let app = build_router(catalog);
    let (_, first_page) = get(&app, "/v1/namespaces?parent=ns1&pageToken=").await;
    let namespaces_token = first_page["next-page-token"].as_str().expect("next-page-token");

    let (status, _) = get(&app, &format!("/v1/namespaces/ns1/tables?pageToken={namespaces_token}")).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn load_table_serves_not_modified_without_a_body_for_a_matching_etag() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace = NamespaceIdent::new("ns1".to_string());
    create_table(&catalog, &namespace, "tbl").await;
    let app = build_router(catalog);

    let loaded = send(&app, "GET", "/v1/namespaces/ns1/tables/tbl").await;
    let etag = loaded.headers[header::ETAG].clone();

    let revalidated = send_revalidation(&app, "/v1/namespaces/ns1/tables/tbl", &etag).await;

    assert_eq!(revalidated.status, StatusCode::NOT_MODIFIED);
    assert_eq!(revalidated.headers[header::ETAG], etag);
    assert!(revalidated.body.is_empty(), "304 must not carry a body");
}

/// `refs` and `all` project the same metadata version into different payloads,
/// which the spec requires to carry distinct validators. The table needs a
/// snapshot outside its refs for the two projections to differ at all: without
/// one, equal payloads would make any pair of validators trivially acceptable.
#[tokio::test]
async fn snapshot_selection_changes_the_metadata_etag() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let catalog = Arc::new(catalog);
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    add_snapshots(&storage, &table).await;
    let app = build_router(catalog);
    let all_uri = "/v1/namespaces/ns1/tables/tbl?snapshots=all";
    let refs_uri = "/v1/namespaces/ns1/tables/tbl?snapshots=refs";

    let all = send(&app, "GET", all_uri).await;
    let refs = send(&app, "GET", refs_uri).await;

    assert_eq!(snapshot_ids(&all.json()["metadata"]), vec![1, 2, 3]);
    assert_eq!(snapshot_ids(&refs.json()["metadata"]), vec![1, 2]);
    let all_etag = all.headers[header::ETAG].clone();
    let refs_etag = refs.headers[header::ETAG].clone();
    assert_ne!(all_etag, refs_etag);

    // Each validator identifies one projection, so it revalidates that
    // projection and never the other one.
    assert_eq!(
        send_revalidation(&app, refs_uri, &refs_etag).await.status,
        StatusCode::NOT_MODIFIED
    );
    assert_eq!(
        send_revalidation(&app, refs_uri, &all_etag).await.status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn namespace_identifiers_with_empty_parts_are_rejected() {
    let app = build_router(Arc::new(make_in_memory_catalog()));

    for uri in [
        "/v1/namespaces/%1Fns",
        "/v1/namespaces/ns%1F",
        "/v1/namespaces/ns%1F%1Fchild",
        "/v1/namespaces?parent=%1Fns",
    ] {
        assert_eq!(get(&app, uri).await.0, StatusCode::BAD_REQUEST, "GET {uri}");
    }
}

#[tokio::test]
async fn referenced_by_requires_a_decodable_fully_qualified_view_identifier() {
    let catalog = Arc::new(make_in_memory_catalog());
    let namespace = NamespaceIdent::new("ns".to_string());
    create_table(&catalog, &namespace, "tbl").await;
    let app = build_router(catalog);

    // `%FF` decodes to a byte no UTF-8 identifier can carry; the rest are
    // decodable but not fully qualified.
    for query in [
        "referenced-by=view",
        "referenced-by=%1Fview",
        "referenced-by=ns%1F",
        "referenced-by=%FF",
    ] {
        let (status, json) = get(&app, &format!("/v1/namespaces/ns/tables/tbl?{query}")).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "query {query}");
        assert_eq!(json["error"]["type"], "BadRequestException", "query {query}");
        assert_eq!(json["error"]["code"], 400, "query {query}");
        assert!(json["error"]["message"].is_string(), "query {query}");
    }
    assert_eq!(
        get(
            &app,
            "/v1/namespaces/ns/tables/tbl?referenced-by=ns%1Fview%2Cwith-comma",
        )
        .await
        .0,
        StatusCode::OK
    );
}

/// `GET uri` carrying one request header — the form `loadTable`'s header
/// validators are driven through. Takes a full `HeaderValue` so a case can
/// carry the non-ASCII bytes a `&str` cannot spell.
async fn send_with_header(app: &axum::Router, uri: &str, name: &str, value: HeaderValue) -> ApiResponse {
    send_request(
        app,
        Request::builder()
            .uri(uri)
            .header(name, value)
            .body(Body::empty())
            .expect("request"),
    )
    .await
}

/// The pinned spec enumerates exactly two delegation schemes, sent as a
/// comma-separated list. Anything else in the list poisons the whole request:
/// serving plain metadata to a client that asked for delegated access would
/// hand it a table it cannot read.
#[tokio::test]
async fn load_table_validates_the_access_delegation_header() {
    let catalog = Arc::new(make_in_memory_catalog());
    create_table(&catalog, &NamespaceIdent::new("ns1".to_string()), "tbl").await;
    let app = build_router(catalog);
    let uri = "/v1/namespaces/ns1/tables/tbl";

    for value in [
        "vended-credentials",
        "remote-signing",
        "vended-credentials,remote-signing",
        "vended-credentials, remote-signing",
    ] {
        let response = send_with_header(
            &app,
            uri,
            "X-Iceberg-Access-Delegation",
            HeaderValue::from_static(value),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "delegation {value:?}");
    }

    for value in [
        HeaderValue::from_static(""),
        HeaderValue::from_static("token-exchange"),
        HeaderValue::from_static("vended-credentials,token-exchange"),
        // A non-ASCII byte is legal in a header value but never in this one:
        // it can spell neither delegation scheme, so it must be a `400`.
        HeaderValue::from_bytes(b"vended-credentials\xFF").expect("non-ASCII header value"),
    ] {
        let response = send_with_header(&app, uri, "X-Iceberg-Access-Delegation", value.clone()).await;
        let json = response.json();

        assert_eq!(response.status, StatusCode::BAD_REQUEST, "delegation {value:?}");
        assert_eq!(json["error"]["type"], "BadRequestException", "delegation {value:?}");
        assert_eq!(json["error"]["code"], 400, "delegation {value:?}");
        assert!(json["error"]["message"].is_string(), "delegation {value:?}");
    }
}

/// The RFC 9110 grammar itself is covered by the `table_metadata` unit tests;
/// this pins the HTTP half of the contract — a malformed field value becomes a
/// `400` in the Iceberg envelope instead of poisoning cache revalidation.
#[tokio::test]
async fn load_table_rejects_a_malformed_if_none_match_with_the_iceberg_envelope() {
    let catalog = Arc::new(make_in_memory_catalog());
    create_table(&catalog, &NamespaceIdent::new("ns1".to_string()), "tbl").await;
    let app = build_router(catalog);

    let response = send_with_header(
        &app,
        "/v1/namespaces/ns1/tables/tbl",
        header::IF_NONE_MATCH.as_str(),
        HeaderValue::from_static("unquoted-tag"),
    )
    .await;
    let json = response.json();

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(json["error"]["type"], "BadRequestException");
    assert_eq!(json["error"]["code"], 400);
    assert!(json["error"]["message"].is_string());
}

/// Storage that fails every `load_root` with the configured error — the
/// deterministic fault injection a real S3 outage cannot provide. Errors are
/// minted per call because [`crate::error::Error`] is not `Clone`.
struct FailingCatalogStorage<F: Fn() -> crate::error::Error + Send + Sync>(F);

#[async_trait::async_trait]
impl<F: Fn() -> crate::error::Error + Send + Sync> crate::storage::CatalogStorage for FailingCatalogStorage<F> {
    async fn load_root(&self, _known: Option<&crate::storage::Version>) -> crate::error::Result<LoadOutcome> {
        Err((self.0)())
    }

    async fn save_root(
        &self,
        _root: Arc<crate::domain::CatalogRoot>,
        _expected: &crate::storage::Version,
    ) -> crate::error::Result<crate::storage::Version> {
        Err((self.0)())
    }

    async fn read_table_metadata(&self, _location: &str) -> crate::error::Result<Arc<iceberg::spec::TableMetadata>> {
        Err((self.0)())
    }

    async fn write_table_metadata(
        &self,
        _location: &str,
        _metadata: &iceberg::spec::TableMetadata,
    ) -> crate::error::Result<()> {
        Err((self.0)())
    }
}

/// Sentinel every injected storage error embeds: a leak of any part of the
/// internal message into an HTTP body trips the assertion even when the leaked
/// fragment carries no `s3://` URI.
const INTERNAL_STORAGE_DETAIL: &str = "INTERNAL_STORAGE_DETAIL_DO_NOT_EXPOSE s3://internal-bucket";

/// A storage failure reaching a handler must keep two promises at the HTTP
/// boundary: the status class tells the client whether retrying can help, and
/// the body never carries the internal detail the storage error was built
/// from.
#[tokio::test]
async fn storage_failures_map_to_sanitized_retryable_and_terminal_responses() {
    use crate::error::{Error, StorageError};

    let cases: [(&str, fn() -> Error, StatusCode, &str); 2] = [
        (
            "retryable",
            || {
                Error::Storage(StorageError::Transient(format!(
                    "connect timeout to {INTERNAL_STORAGE_DETAIL}"
                )))
            },
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
        ),
        (
            "terminal",
            || {
                Error::Storage(StorageError::Io(format!(
                    "invalid root at {INTERNAL_STORAGE_DETAIL}/root.json"
                )))
            },
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
        ),
    ];

    for (case, make_error, status, error_type) in cases {
        let catalog = crate::S3Catalog::with_storage(
            Arc::new(FailingCatalogStorage(make_error)),
            iceberg::io::FileIO::new_with_memory(),
            "memory://catalog/tables".to_string(),
            crate::infra::retrier::Retrier::new(crate::config::cas_retrier_config_default()),
            tokio_util::sync::CancellationToken::new(),
        )
        .expect("every test runs on tokio");
        let app = build_router(Arc::new(catalog));

        let response = send(&app, "GET", "/v1/namespaces").await;
        let json = response.json();

        assert_eq!(response.status, status, "{case}");
        assert_eq!(json["error"]["type"], error_type, "{case}");
        assert_eq!(json["error"]["code"], status.as_u16(), "{case}");
        assert!(json["error"]["message"].is_string(), "{case}");
        let body = String::from_utf8(response.body.to_vec()).expect("response body text");
        assert!(
            !body.contains("INTERNAL_STORAGE_DETAIL_DO_NOT_EXPOSE"),
            "{case}: internal error detail leaked into {body}",
        );
        assert!(!body.contains("s3://"), "{case}: internal URI leaked into {body}");
    }
}

struct CatalogApiServer {
    base_uri: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl CatalogApiServer {
    async fn start(app: axum::Router) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind catalog server");
        let address = listener.local_addr().expect("catalog server address");
        let (shutdown, shutdown_received) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_received.await;
                })
                .await
        });

        tokio::net::TcpStream::connect(address).await.expect("catalog server readiness");
        Self {
            base_uri: format!("http://{address}"),
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    async fn stop(mut self) {
        self.shutdown
            .take()
            .expect("catalog shutdown sender")
            .send(())
            .expect("catalog shutdown receiver");
        tokio::time::timeout(
            Duration::from_secs(5),
            self.task.take().expect("catalog server task handle"),
        )
        .await
        .expect("catalog server shutdown timeout")
        .expect("catalog server task")
        .expect("catalog server result");
    }
}

impl Drop for CatalogApiServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

#[tokio::test]
async fn official_rest_client_completes_read_only_workflow_with_a_multisegment_prefix_over_tcp() {
    let catalog = Arc::new(make_in_memory_catalog());
    let nested_namespace =
        NamespaceIdent::from_vec(vec!["team".to_string(), "namespaces".to_string()]).expect("namespace");
    for namespace in [
        NamespaceIdent::new("alpha".to_string()),
        NamespaceIdent::new("zeta".to_string()),
        nested_namespace.clone(),
    ] {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
    }
    let source_table = create_table(&catalog, &nested_namespace, "orders.v2").await;
    create_table(&catalog, &nested_namespace, "returns").await;

    let app = router(catalog, api_config(Some(multisegment_prefix()), "memory://catalog"));
    let server = CatalogApiServer::start(app).await;
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(MemoryStorageFactory))
        .load(
            "catalog",
            HashMap::from([
                (REST_CATALOG_PROP_URI.to_string(), server.base_uri.clone()),
                (REST_CATALOG_PROP_WAREHOUSE.to_string(), "memory://catalog".to_string()),
            ]),
        )
        .await
        .expect("create REST catalog");

    let namespaces = catalog.list_namespaces(None).await.expect("list namespaces");
    assert_eq!(
        namespaces,
        vec![
            NamespaceIdent::new("alpha".to_string()),
            NamespaceIdent::new("zeta".to_string()),
        ]
    );
    assert_eq!(
        catalog.get_namespace(&nested_namespace).await.expect("get namespace").name(),
        &nested_namespace
    );
    assert!(catalog.namespace_exists(&nested_namespace).await.expect("namespace exists"));
    assert!(
        !catalog
            .namespace_exists(&NamespaceIdent::new("missing".to_string()))
            .await
            .expect("missing namespace")
    );

    let mut tables = catalog.list_tables(&nested_namespace).await.expect("list tables");
    tables.sort_by_key(TableIdent::to_string);
    assert_eq!(
        tables,
        vec![
            TableIdent::new(nested_namespace.clone(), "orders.v2".to_string()),
            TableIdent::new(nested_namespace.clone(), "returns".to_string()),
        ]
    );
    let table_identifier = TableIdent::new(nested_namespace.clone(), "orders.v2".to_string());
    let loaded_table = catalog.load_table(&table_identifier).await.expect("load table");
    assert_eq!(loaded_table.metadata_location(), source_table.metadata_location());
    assert!(catalog.table_exists(&table_identifier).await.expect("table exists"));
    assert!(
        !catalog
            .table_exists(&TableIdent::new(nested_namespace, "missing".to_string()))
            .await
            .expect("missing table")
    );

    server.stop().await;
}
