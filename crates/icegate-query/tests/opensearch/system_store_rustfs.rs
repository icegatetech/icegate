//! The system-index store against real RustFS, built through the production constructor
//! `build_system_object_store` from a storage configuration as a deployment states it.
#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt as _;
use futures::future::join_all;
use icegate_common::testing::{S3TestContainer, create_s3_bucket, create_s3_object_store};
use icegate_common::{S3Config, StorageBackend, StorageConfig};
use icegate_query::QueryConfig;
use icegate_query::engine::QueryEngineConfig;
use icegate_query::opensearch::{OpenSearchConfig, SystemIndicesConfig, build_system_object_store};
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt as _};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use tokio::sync::Barrier;

use super::harness::TestServer;

const BUCKET: &str = "opensearch-system-store";
/// Upper bound for a race of two writers, so a hung request fails the test instead of the run.
const RACE_TIMEOUT: Duration = Duration::from_secs(30);
/// Rounds of the counter race.
const COUNTER_ROUNDS: usize = 50;
/// Writers released per round, split evenly across the two replicas.
const COUNTER_WRITERS_PER_ROUND: usize = 8;
/// Bound for one round, so a hung write fails the test instead of the run.
const COUNTER_ROUND_TIMEOUT: Duration = Duration::from_secs(60);

/// The query configuration a deployment storing its tables in `bucket` of `container` carries,
/// with the listener enabled.
fn build_rustfs_query_config(container: &S3TestContainer, bucket: &str) -> QueryConfig {
    let mut config = QueryConfig {
        storage: StorageConfig {
            backend: StorageBackend::S3(S3Config {
                bucket: bucket.to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some(container.endpoint().to_string()),
                access_key_id: Some(container.username().to_string()),
                secret_access_key: Some(container.password().to_string()),
            }),
        },
        ..QueryConfig::default()
    };
    config.opensearch.enabled = true;
    config
}

/// Start RustFS with an empty bucket and the query configuration a deployment storing its tables
/// in that bucket carries, with the listener enabled. The container guard must outlive every
/// store built from the configuration.
async fn start_rustfs() -> (S3TestContainer, QueryConfig) {
    let container = S3TestContainer::start().await.expect("rustfs container starts");
    create_s3_bucket(container.endpoint(), BUCKET).await.expect("bucket created");
    let config = build_rustfs_query_config(&container, BUCKET);
    (container, config)
}

// The listener runs the probe before binding, so a started listener is a passed probe.
#[tokio::test]
async fn the_startup_probe_passes_against_rustfs_through_the_production_constructor() {
    let (_container, config) = start_rustfs().await;
    let system_object_store = build_system_object_store(&config).expect("the production client");
    assert_eq!(
        system_object_store.location(),
        "s3://opensearch-system-store/opensearch-system"
    );

    let (server, _catalog) = TestServer::start_with_system_store(config.opensearch.clone(), system_object_store).await;
    let response = server
        .request(Method::GET, Some("probe-tenant"), "/")
        .send()
        .await
        .expect("GET /");

    assert_eq!(response.status(), StatusCode::OK);
    server.shutdown().await;
}

/// Two listeners, each with a store built independently through the production constructor:
/// two replicas of one deployment.
async fn start_replicas(config: &QueryConfig) -> (TestServer, TestServer) {
    let first_store = build_system_object_store(config).expect("the first client");
    let second_store = build_system_object_store(config).expect("the second client");
    let (first_replica, _) = TestServer::start_with_system_store(config.opensearch.clone(), first_store).await;
    let (second_replica, _) = TestServer::start_with_system_store(config.opensearch.clone(), second_store).await;
    (first_replica, second_replica)
}

async fn put_index(server: &TestServer, tenant: &str, index: &str) -> StatusCode {
    server
        .request(Method::PUT, Some(tenant), &format!("/{index}"))
        .json(&json!({"mappings": {"dynamic": "strict"}}))
        .send()
        .await
        .expect("PUT index")
        .status()
}

async fn describe_index(server: &TestServer, tenant: &str, index: &str) -> StatusCode {
    server
        .request(Method::GET, Some(tenant), &format!("/{index}"))
        .send()
        .await
        .expect("GET index")
        .status()
}

// Which replica swaps first is a scheduling outcome and is not asserted; the contract is that
// both writes land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_store_instances_racing_on_an_absent_pointer_both_land_their_writes() {
    let (_container, config) = start_rustfs().await;
    let (first_replica, second_replica) = start_replicas(&config).await;
    let barrier = Arc::new(Barrier::new(2));

    // Scoped, so the racing futures' borrows of the replicas end before the replicas shut down.
    let (first_status, second_status) = {
        let racing_puts = [(&first_replica, ".kibana_a"), (&second_replica, ".kibana_b")].map(|(server, index)| {
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                put_index(server, "race", index).await
            }
        });
        let [first_put, second_put] = racing_puts;
        tokio::time::timeout(RACE_TIMEOUT, async { tokio::join!(first_put, second_put) })
            .await
            .expect("both writes finish")
    };

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    for server in [&first_replica, &second_replica] {
        for index in [".kibana_a", ".kibana_b"] {
            assert_eq!(describe_index(server, "race", index).await, StatusCode::OK, "{index}");
        }
    }
    first_replica.shutdown().await;
    second_replica.shutdown().await;
}

#[tokio::test]
async fn a_replica_reading_after_another_replicas_write_sees_it() {
    let (_container, config) = start_rustfs().await;
    let (first_replica, second_replica) = start_replicas(&config).await;
    // The first replica caches the tenant's pointer before the second one writes.
    assert_eq!(put_index(&first_replica, "fresh", ".kibana_0").await, StatusCode::OK);
    assert_eq!(
        describe_index(&first_replica, "fresh", ".kibana_0").await,
        StatusCode::OK
    );

    assert_eq!(put_index(&second_replica, "fresh", ".kibana_1").await, StatusCode::OK);

    assert_eq!(
        describe_index(&first_replica, "fresh", ".kibana_1").await,
        StatusCode::OK
    );
    first_replica.shutdown().await;
    second_replica.shutdown().await;
}

/// The replica that applies an alias caches the state it wrote; a replica that never read the
/// tenant can resolve the alias only from the snapshot object stored in RustFS.
#[tokio::test]
async fn an_alias_applied_on_one_replica_resolves_on_another_that_never_read_the_tenant() {
    let (_container, config) = start_rustfs().await;
    let (first_replica, second_replica) = start_replicas(&config).await;
    assert_eq!(put_index(&first_replica, "aliased", ".kibana_1").await, StatusCode::OK);
    let alias_status = first_replica
        .request(Method::POST, Some("aliased"), "/_aliases")
        .json(&json!({"actions": [{"add": {"index": ".kibana_1", "alias": ".kibana"}}]}))
        .send()
        .await
        .expect("POST _aliases")
        .status();

    let index_through_alias: serde_json::Value = second_replica
        .request(Method::GET, Some("aliased"), "/.kibana")
        .send()
        .await
        .expect("GET through the alias")
        .json()
        .await
        .expect("a JSON body");

    assert_eq!(alias_status, StatusCode::OK);
    assert_eq!(
        index_through_alias,
        json!({".kibana_1": {"aliases": {".kibana": {}}, "mappings": {"dynamic": "strict"}, "settings": {}}})
    );
    first_replica.shutdown().await;
    second_replica.shutdown().await;
}

/// The tenant segment is the tenant identifier verbatim; `:` must survive every S3 request the
/// store sends.
#[tokio::test]
async fn a_colon_tenant_segment_round_trips_create_update_conditional_get_and_head() {
    let (container, config) = start_rustfs().await;
    let (server, _catalog) = TestServer::start_with_system_store(
        config.opensearch.clone(),
        build_system_object_store(&config).expect("client"),
    )
    .await;

    assert_eq!(put_index(&server, "a:b", ".kibana_1").await, StatusCode::OK, "create");
    assert_eq!(
        put_index(&server, "a:b", ".kibana_2").await,
        StatusCode::OK,
        "update with the ETag"
    );
    assert_eq!(describe_index(&server, "a:b", ".kibana_1").await, StatusCode::OK);
    assert_eq!(
        describe_index(&server, "a:b", ".kibana_2").await,
        StatusCode::OK,
        "a conditional GET"
    );
    let head_response = server
        .request(Method::HEAD, Some("a:b"), "/.kibana_1")
        .send()
        .await
        .expect("HEAD");
    assert_eq!(head_response.status(), StatusCode::OK);

    let direct_store = create_s3_object_store(container.endpoint(), BUCKET).expect("a direct client");
    let pointer_path = Path::from("opensearch-system/a:b/current");
    let pointer_meta = direct_store
        .head(&pointer_path)
        .await
        .expect("the pointer is under the verbatim segment");
    let etag = pointer_meta.e_tag.expect("RustFS returns an ETag");
    let unchanged_answer = direct_store
        .get_opts(
            &pointer_path,
            GetOptions {
                if_none_match: Some(etag),
                ..GetOptions::default()
            },
        )
        .await;
    assert!(
        matches!(unchanged_answer, Err(object_store::Error::NotModified { .. })),
        "{unchanged_answer:?}"
    );
    server.shutdown().await;
}

/// One counter increment, upserting the document on first use. Answers the writer's number with
/// the status and body it got, so a failed round can report every writer.
async fn send_increment(
    server: &TestServer,
    round: usize,
    writer: usize,
) -> reqwest::Result<(usize, StatusCode, Value)> {
    let body = json!({
        "script": {
            "source": "if (ctx._source[params.type][params.counterFieldName] == null) { ctx._source[params.type][params.counterFieldName] = params.count; } else { ctx._source[params.type][params.counterFieldName] += params.count; } ctx._source.updated_at = params.time;",
            "lang": "painless",
            "params": {"count": 1, "time": format!("round-{round}-writer-{writer}"), "type": "ui-metric", "counterFieldName": "count"}
        },
        "upsert": {"type": "ui-metric", "ui-metric": {"count": 1}, "updated_at": "first"}
    });
    let response = server
        .request(
            Method::POST,
            Some("tenant-a"),
            "/.kibana/_update/ui-metric:console:opened?refresh=wait_for",
        )
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let answer = response.json::<Value>().await?;
    Ok((writer, status, answer))
}

// Which writer lands first in a round is a scheduling outcome and is not asserted; the contract
// is that every increment lands exactly once, whichever replica took it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increment_counters_across_two_replicas_end_at_exactly_n() {
    let (_container, config) = start_rustfs().await;
    let (replica_a, replica_b) = start_replicas(&config).await;
    let index = json!({"mappings": {"dynamic": "strict", "properties": {"type": {"type": "keyword"}}}, "settings": {"number_of_shards": 1}});
    let create_response = replica_a
        .request(Method::PUT, Some("tenant-a"), "/.kibana_1")
        .json(&index)
        .send()
        .await
        .expect("create");
    assert!(create_response.status().is_success(), "{}", create_response.status());
    let alias = json!({"actions": [{"add": {"index": ".kibana_1", "alias": ".kibana"}}]});
    let alias_response = replica_b
        .request(Method::POST, Some("tenant-a"), "/_aliases")
        .json(&alias)
        .send()
        .await
        .expect("alias");
    assert!(alias_response.status().is_success(), "{}", alias_response.status());

    for round in 1..=COUNTER_ROUNDS {
        let writers = (0..COUNTER_WRITERS_PER_ROUND).map(|writer| {
            let replica = if writer % 2 == 0 { &replica_a } else { &replica_b };
            send_increment(replica, round, writer)
        });
        let outcomes = tokio::time::timeout(COUNTER_ROUND_TIMEOUT, join_all(writers))
            .await
            .unwrap_or_else(|_| panic!("round {round} did not finish within {COUNTER_ROUND_TIMEOUT:?}"));
        let has_failed_writer = outcomes
            .iter()
            .any(|outcome| !matches!(outcome, Ok((_, status, _)) if status.is_success()));
        assert!(!has_failed_writer, "round {round}: all outcomes {outcomes:?}");

        let read_answer = replica_b
            .request(Method::GET, Some("tenant-a"), "/.kibana/_doc/ui-metric:console:opened")
            .send()
            .await
            .expect("the counter is read")
            .json::<Value>()
            .await
            .expect("the read is JSON");
        let expected_count = round * COUNTER_WRITERS_PER_ROUND;
        assert_eq!(
            read_answer["_source"]["ui-metric"]["count"],
            json!(expected_count),
            "round {round}: every increment lands exactly once; outcomes {outcomes:?}"
        );
    }
    replica_a.shutdown().await;
    replica_b.shutdown().await;
}

/// The snapshot objects listed under `tenant_prefix`.
async fn list_snapshot_names(
    store: &Arc<dyn ObjectStore>,
    tenant_prefix: &Path,
) -> object_store::Result<BTreeSet<String>> {
    let listed_objects: Vec<object_store::ObjectMeta> = store.list(Some(tenant_prefix)).try_collect().await?;
    Ok(listed_objects
        .into_iter()
        .filter_map(|metadata| metadata.location.filename().map(str::to_string))
        .filter(|name| name.starts_with("snap-"))
        .collect())
}

/// A write lets a background pass start once the scroll lifetime has passed; the pass deletes the
/// first snapshot object once its successor is older than the retention (the scroll lifetime
/// plus the engine's default query deadline, which the harness runs with), measured on RustFS's
/// clock at whole seconds. The test therefore waits that retention out in real time: the store's
/// clock is what it exercises.
#[tokio::test]
async fn garbage_collection_lists_and_deletes_under_a_colon_tenant_segment() {
    const SCROLL_LIFETIME_SECS: u64 = 1;
    let retention = Duration::from_secs(SCROLL_LIFETIME_SECS + QueryEngineConfig::default().max_query_duration_secs);
    let container = S3TestContainer::start().await.expect("the RustFS container starts");
    let bucket = "opensearch-gc";
    create_s3_bucket(container.endpoint(), bucket)
        .await
        .expect("the bucket is created");
    let system_store = build_system_object_store(&build_rustfs_query_config(&container, bucket))
        .expect("the production constructor builds the store");
    let config = OpenSearchConfig {
        system_indices: SystemIndicesConfig {
            max_scroll_lifetime_secs: SCROLL_LIFETIME_SECS,
            ..SystemIndicesConfig::default()
        },
        ..OpenSearchConfig::default()
    };
    let (server, _catalog) = TestServer::start_with_system_store(config, system_store).await;
    let observer = create_s3_object_store(container.endpoint(), bucket).expect("an observing client");
    let tenant = "a:b";
    let tenant_prefix = Path::from(SystemIndicesConfig::DEFAULT_LOCATION_PREFIX).join(tenant);
    let send_tenant_request =
        |method: Method, path: &str, body: Value| server.request(method, Some(tenant), path).json(&body).send();

    let creation_response = send_tenant_request(
        Method::PUT,
        "/.kibana_1",
        json!({"mappings": {"properties": {"type": {"type": "keyword"}}}}),
    )
    .await
    .expect("answered");
    assert_eq!(creation_response.status(), StatusCode::OK);
    let first_names = list_snapshot_names(&observer, &tenant_prefix)
        .await
        .expect("the tenant prefix lists");
    assert_eq!(first_names.len(), 1, "{first_names:?}");
    let first_write_response = send_tenant_request(Method::PUT, "/.kibana_1/_doc/a", json!({"type": "a"}))
        .await
        .expect("answered");
    assert_eq!(first_write_response.status(), StatusCode::CREATED);
    // Outlive the retention, with two seconds for `Last-Modified`'s whole-second granularity.
    tokio::time::sleep(retention + Duration::from_secs(2)).await;
    let second_write_response = send_tenant_request(Method::PUT, "/.kibana_1/_doc/b", json!({"type": "b"}))
        .await
        .expect("answered");
    assert_eq!(second_write_response.status(), StatusCode::CREATED);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut remaining_names = list_snapshot_names(&observer, &tenant_prefix)
        .await
        .expect("the tenant prefix lists");
    while !remaining_names.is_disjoint(&first_names) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the first snapshot object was never collected: {remaining_names:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
        remaining_names = list_snapshot_names(&observer, &tenant_prefix)
            .await
            .expect("the tenant prefix lists");
    }
    assert_eq!(
        remaining_names.len(),
        2,
        "the two newer snapshot objects stay: {remaining_names:?}"
    );
    for kept_name in ["current", "gc-probe"] {
        observer
            .head(&tenant_prefix.clone().join(kept_name))
            .await
            .unwrap_or_else(|error| panic!("{kept_name} stays under the colon tenant: {error}"));
    }
    let (status, document) = {
        let response = server
            .request(Method::GET, Some(tenant), "/.kibana_1/_doc/a")
            .send()
            .await
            .expect("answered");
        (response.status(), response.json::<Value>().await.expect("JSON"))
    };
    assert_eq!(status, StatusCode::OK);
    assert_eq!(document["_source"], json!({"type": "a"}));
    server.shutdown().await;
}
