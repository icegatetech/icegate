//! `QueryConfig::from_file` over whole configuration documents: the stand's own,
//! and one written before the tenant policy existed. It also reads the stand's
//! Compose document and its `opensearch_dashboards.yml`, to pin the Dashboards
//! image against the version the query document resolves and the tenant header
//! the stand's Dashboards sends.
//!
//! Nothing else loads `config/docker/query.yaml` in CI, and its `tenant` block
//! decides whether the stand's readers are served at all: without it the policy
//! falls back to `single` on `default`, and every datasource of the stand names
//! `demo`.
#![allow(clippy::expect_used)]

use std::{
    collections::BTreeMap,
    error::Error,
    io::Write as _,
    path::{Path, PathBuf},
};

use icegate_common::{
    LOGS_TABLE, SPANS_TABLE, TENANT_ID_HEADER, TenantPolicy, TlsConfig, load_config_file,
    schema::{COL_BODY, COL_SEVERITY_TEXT, COL_TIMESTAMP},
};
use icegate_query::QueryConfig;
use icegate_query::opensearch::build_system_object_store;
use serde::Deserialize;

/// The stand's query document.
const STAND_QUERY_DOCUMENT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/docker/query.yaml");

/// The stand's query reads the tenant each reader names: the plain stand writes
/// `demo`, the auth-proxy stand writes the token's tenant, and both run this one
/// document. That every stand datasource sends the header carrying the tenant
/// the stand writes is pinned by
/// `every_stand_datasource_reads_the_tenant_the_stand_writes` in
/// `crates/icegate-ingest/tests/config_file.rs`.
#[test]
fn the_stand_query_document_reads_the_tenant_each_request_names() {
    let config = QueryConfig::from_file(STAND_QUERY_DOCUMENT).expect("the stand query document must load and validate");

    assert_eq!(config.tenant, TenantPolicy::Multi);
}

/// A complete query document short of its leading top-level sections, which the
/// caller writes in front of it. With none it reads as an operator's configuration
/// written before the `tenant` policy and the `tls` block existed.
///
/// Every block is here because `QueryConfig::from_file` refuses the document
/// without it, not because the case needs it; `tracing` is off so no endpoint is
/// required.
const DOCUMENT_TEMPLATE: &str = r"
catalog:
  backend: !s3
    warehouse: catalog
  warehouse: s3://warehouse/
  properties:
    bucket: warehouse
    region: us-east-1

storage:
  backend: !s3
    bucket: warehouse
    region: us-east-1

queue:
  common:
    base_path: s3://queue/

loki:
  enabled: true
  host: 127.0.0.1
  port: 13100

prometheus:
  enabled: true
  host: 127.0.0.1
  port: 19090

tempo:
  enabled: true
  host: 127.0.0.1
  port: 13200

tracing:
  enabled: false
";

/// Load a document that opens with `sections`, whole top-level YAML sections such as
/// `tenant` or `tls` (empty for none).
///
/// The file is owned by a `NamedTempFile`, so it is removed on success, failure, and
/// panic alike.
fn load_document(sections: &str) -> QueryConfig {
    let mut file = tempfile::NamedTempFile::with_suffix(".yaml").expect("temp config file");
    file.write_all(format!("{sections}{DOCUMENT_TEMPLATE}").as_bytes())
        .expect("write config document");
    file.flush().expect("flush config document");
    QueryConfig::from_file(file.path()).expect("the document must load and validate")
}

/// The field's own promise: a document without the block keeps parsing, and
/// reads the tenant it was served before. Spelled out rather than read from
/// `DEFAULT_TENANT_ID`: it is the value in the deployed tables, so changing the
/// constant must fail here.
#[test]
fn a_document_without_a_tenant_section_reads_the_default_tenant() {
    let config = load_document("");

    assert_eq!(
        config.tenant,
        TenantPolicy::Single {
            id: "default".to_string()
        }
    );
}

/// The reload interval is spelled out rather than read from
/// `DEFAULT_TLS_RELOAD_INTERVAL_SECS`: an operator who leaves it out relies on this
/// number, so changing the constant must fail here.
#[test]
fn a_document_with_a_tls_block_loads_its_paths_and_the_default_reload_interval() {
    let config = load_document("tls:\n  certificate_path: /tls/tls.crt\n  private_key_path: /tls/tls.key\n");

    assert_eq!(
        config.tls,
        Some(TlsConfig {
            certificate_path: PathBuf::from("/tls/tls.crt"),
            private_key_path: PathBuf::from("/tls/tls.key"),
            reload_interval_secs: 30,
        })
    );
}

#[test]
fn a_document_without_a_tls_block_serves_plaintext() {
    assert_eq!(load_document("").tls, None);
}

/// The stand serves the `OpenSearch` API on 9200, the port `docker-compose.yml` publishes for the
/// `query` service.
#[test]
fn the_stand_query_document_serves_opensearch_on_9200() {
    let config = QueryConfig::from_file(STAND_QUERY_DOCUMENT).expect("the stand query document must load and validate");

    assert!(config.opensearch.enabled);
    assert_eq!(config.opensearch.port, 9200);
}

/// The stand's Grafana provisioning, whose `OpenSearch` datasources read the stand's query service.
const STAND_DATASOURCES_DOCUMENT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../config/docker/grafana/provisioning/datasources/datasources.yaml"
);

/// The plugin id a Grafana `OpenSearch` datasource names in its `type`.
const OPENSEARCH_PLUGIN_ID: &str = "grafana-opensearch-datasource";

/// The parts of the stand's datasource provisioning this test reads.
#[derive(Deserialize)]
struct DatasourcesDocument {
    datasources: Vec<Datasource>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Datasource {
    name: String,
    // Grafana's `type` names the plugin that serves the datasource.
    #[serde(rename = "type")]
    plugin_id: String,
    #[serde(default)]
    json_data: DatasourceJsonData,
}

/// The `jsonData` keys this test reads. `interval` and `pdcInjected` are read only to assert
/// that the stand provisions neither.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DatasourceJsonData {
    database: Option<String>,
    time_field: Option<String>,
    log_message_field: Option<String>,
    log_level_field: Option<String>,
    version: Option<String>,
    version_label: Option<String>,
    flavor: Option<String>,
    ppl_enabled: Option<bool>,
    max_concurrent_shard_requests: Option<u64>,
    interval: Option<serde_json::Value>,
    pdc_injected: Option<serde_json::Value>,
}

/// The provisioned `version` and `versionLabel` are a second copy of the version the stand's
/// IceGate reports, so they are tied to it here: a version bump that misses the provisioning
/// file fails this test instead of leaving two versions apart. `from_file` validated the
/// configured version, so it is the canonical version IceGate reports as is. The stand
/// provisions the logs and the metrics-exploration datasource, each with the fixed `jsonData`
/// the stand's `OpenSearch` datasources carry.
///
/// The index and column names the datasources name are copies of the schema's table and
/// column names, so they are read from the schema constants: a renamed table or column fails
/// here instead of leaving a datasource that names something the stand no longer serves.
#[test]
fn every_stand_opensearch_datasource_states_the_version_the_stand_reports() {
    let config = QueryConfig::from_file(STAND_QUERY_DOCUMENT).expect("the stand query document must load and validate");
    let document: DatasourcesDocument =
        load_config_file(Path::new(STAND_DATASOURCES_DOCUMENT)).expect("the stand datasource document must load");

    let opensearch_datasources: Vec<&Datasource> = document
        .datasources
        .iter()
        .filter(|datasource| datasource.plugin_id == OPENSEARCH_PLUGIN_ID)
        .collect();

    let expected_log_fields_by_table = [
        (LOGS_TABLE, Some(COL_BODY), Some(COL_SEVERITY_TEXT)),
        (SPANS_TABLE, None, None),
    ];

    // The provisioning order is not part of the contract, so both sides are put in one order.
    let mut databases: Vec<Option<&str>> = opensearch_datasources
        .iter()
        .map(|datasource| datasource.json_data.database.as_deref())
        .collect();
    databases.sort_unstable();
    let mut expected_databases = expected_log_fields_by_table.map(|(table, ..)| Some(table));
    expected_databases.sort_unstable();
    assert_eq!(databases, expected_databases);
    let expected_label = format!("OpenSearch {}", config.opensearch.version);
    for datasource in opensearch_datasources {
        let json_data = &datasource.json_data;
        assert_eq!(
            json_data.version.as_deref(),
            Some(config.opensearch.version.as_str()),
            "{}",
            datasource.name
        );
        assert_eq!(
            json_data.version_label.as_deref(),
            Some(expected_label.as_str()),
            "{}",
            datasource.name
        );
        assert_eq!(
            json_data.time_field.as_deref(),
            Some(COL_TIMESTAMP),
            "{}",
            datasource.name
        );
        let (_, expected_message_field, expected_level_field) = expected_log_fields_by_table
            .iter()
            .copied()
            .find(|&(table, ..)| json_data.database.as_deref() == Some(table))
            .expect("the databases assertion admits only the tables listed in the expectations");
        assert_eq!(
            json_data.log_message_field.as_deref(),
            expected_message_field,
            "{}",
            datasource.name
        );
        assert_eq!(
            json_data.log_level_field.as_deref(),
            expected_level_field,
            "{}",
            datasource.name
        );
        assert_eq!(json_data.flavor.as_deref(), Some("opensearch"), "{}", datasource.name);
        assert_eq!(json_data.ppl_enabled, Some(true), "{}", datasource.name);
        assert_eq!(json_data.max_concurrent_shard_requests, Some(5), "{}", datasource.name);
        assert!(
            json_data.interval.is_none(),
            "{} must not set interval",
            datasource.name
        );
        assert!(
            json_data.pdc_injected.is_none(),
            "{} must not set pdcInjected",
            datasource.name
        );
    }
}

/// The stand stores its tables in the `warehouse` bucket and sets no system location, so its
/// saved objects live beside the tables under the default prefix. Building the client sends no
/// request, so this needs no running store.
#[test]
fn the_stand_query_document_resolves_the_system_location_to_the_warehouse_bucket() {
    let config = QueryConfig::from_file(STAND_QUERY_DOCUMENT).expect("the stand query document must load and validate");

    let system_object_store = build_system_object_store(&config).expect("the stand's client builds");

    assert_eq!(system_object_store.location(), "s3://warehouse/opensearch-system");
}

/// The stand's Compose document.
const STAND_COMPOSE_DOCUMENT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/docker/docker-compose.yml");

/// The configuration the stand's `OpenSearch` Dashboards runs with.
const STAND_DASHBOARDS_DOCUMENT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../config/docker/opensearch-dashboards/opensearch_dashboards.yml"
);

/// The Compose service of the stand's `OpenSearch` Dashboards.
const DASHBOARDS_SERVICE: &str = "opensearch-dashboards";

/// The services of the stand's Compose document, each read as untyped YAML.
#[derive(Deserialize)]
struct ComposeDocument {
    services: BTreeMap<String, serde_json::Value>,
}

/// The settings of the stand's `opensearch_dashboards.yml` this file pins.
#[derive(Deserialize)]
struct DashboardsDocument {
    #[serde(rename = "opensearch.hosts")]
    hosts: Vec<String>,
    #[serde(rename = "opensearch.customHeaders")]
    custom_headers: BTreeMap<String, String>,
    #[serde(rename = "opensearch.requestHeadersWhitelist")]
    forwarded_headers: Vec<String>,
}

/// `MAJOR.MINOR.PATCH` → `MINOR`, read independently of IceGate's version parser; `None` when
/// `version` is not three dot-separated parts or its minor is not a number.
fn read_minor(version: &str) -> Option<u32> {
    let mut parts = version.split('.');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(_), Some(minor), Some(_), None) => minor.parse().ok(),
        _ => None,
    }
}

/// The stand's `OpenSearch` Dashboards service, as its Compose document declares it.
///
/// # Errors
///
/// Fails when the Compose document does not load or declares no Dashboards service.
fn read_stand_dashboards_service() -> Result<serde_json::Value, Box<dyn Error>> {
    let mut compose: ComposeDocument = load_config_file(Path::new(STAND_COMPOSE_DOCUMENT))?;
    compose
        .services
        .remove(DASHBOARDS_SERVICE)
        .ok_or_else(|| format!("the stand Compose document declares no {DASHBOARDS_SERVICE} service").into())
}

/// The stand pairs one Dashboards image with one IceGate version. IceGate reports the version its
/// configuration resolves (the code default when `config/docker/query.yaml` sets none), and the
/// pairing the stand supports is a Dashboards whose minor is at most that minor.
#[test]
fn the_stand_dashboards_image_minor_is_at_most_the_minor_icegate_reports() {
    let config = QueryConfig::from_file(STAND_QUERY_DOCUMENT).expect("the stand query document must load and validate");
    let service = read_stand_dashboards_service().expect("the stand runs OpenSearch Dashboards");
    let image = service["image"].as_str().expect("the Dashboards service names an image");
    let (repository, tag) = image.rsplit_once(':').expect("the Dashboards image is pinned by tag");
    let dashboards_minor = read_minor(tag).expect("the Dashboards tag is MAJOR.MINOR.PATCH with a numeric minor");
    let icegate_minor =
        read_minor(&config.opensearch.version).expect("IceGate's version is MAJOR.MINOR.PATCH with a numeric minor");

    assert_eq!(repository, "opensearchproject/opensearch-dashboards");
    assert!(
        dashboards_minor <= icegate_minor,
        "Dashboards {tag} pairs with IceGate {}",
        config.opensearch.version
    );
}

/// IceGate trusts the tenant header, so on the stand the Dashboards configuration is the one place that
/// chooses the tenant: it sets the stand's tenant as a custom header, and the header is not in
/// `opensearch.requestHeadersWhitelist`.
#[test]
fn the_stand_dashboards_document_sets_the_tenant_custom_header_and_leaves_it_out_of_the_request_header_whitelist() {
    let config = QueryConfig::from_file(STAND_QUERY_DOCUMENT).expect("the stand query document must load and validate");
    let dashboards_document: DashboardsDocument =
        load_config_file(Path::new(STAND_DASHBOARDS_DOCUMENT)).expect("the stand Dashboards document must load");
    let service = read_stand_dashboards_service().expect("the stand runs OpenSearch Dashboards");

    assert_eq!(
        dashboards_document.hosts,
        [format!("http://query:{}", config.opensearch.port)]
    );
    let tenant_headers: Vec<(&String, &String)> = dashboards_document
        .custom_headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case(TENANT_ID_HEADER))
        .collect();
    assert_eq!(tenant_headers.len(), 1, "{:?}", dashboards_document.custom_headers);
    assert_eq!(tenant_headers[0].1, "demo");
    assert!(
        !dashboards_document
            .forwarded_headers
            .iter()
            .any(|name| name.eq_ignore_ascii_case(TENANT_ID_HEADER)),
        "{:?}",
        dashboards_document.forwarded_headers
    );
    assert_eq!(service["environment"]["DISABLE_SECURITY_DASHBOARDS_PLUGIN"], "true");
    let volumes: Vec<&str> = service["volumes"]
        .as_array()
        .expect("the Dashboards service mounts its configuration")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect();
    assert!(
        volumes.contains(
            &"./opensearch-dashboards/opensearch_dashboards.yml:/usr/share/opensearch-dashboards/config/opensearch_dashboards.yml:ro"
        ),
        "{volumes:?}"
    );
}
