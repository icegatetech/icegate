//! `IngestConfig::from_file` over whole configuration documents.
//!
//! The unit tests in `config.rs` build the struct in memory, so nothing there
//! covers the YAML the deployments actually hand the binary: the tagged `tenant`
//! union is rendered by `icegate.tenantYaml` in the Helm chart and written out
//! in `config/docker/ingest-proxy.yaml`, and a document that fails to
//! deserialize surfaces only as a pod that will not start.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{collections::HashMap, io::Write as _, path::Path};

use icegate_common::{TENANT_ID_HEADER, TenantPolicy, load_config_file};
use icegate_ingest::IngestConfig;
use serde::Deserialize;
use tempfile::NamedTempFile;

/// A complete ingest document with the `tenant` section left to the caller.
///
/// Every other block is here because `IngestConfig::validate` refuses the
/// document without it, not because the case needs it; `tracing` is off so no
/// endpoint is required.
const DOCUMENT_TEMPLATE: &str = r"
{tenant}
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

shift:
  jobsmanager:
    storage:
      endpoint: http://localhost:9000
      bucket: jobs
      region: us-east-1

otlp_http:
  enabled: true
  host: 127.0.0.1
  port: 14318

otlp_grpc:
  enabled: true
  host: 127.0.0.1
  port: 14317

metrics:
  enabled: true
  host: 0.0.0.0
  port: 9091
  path: /metrics

tracing:
  enabled: false
";

/// Load a document whose `tenant` section is `tenant_section` (empty for none).
///
/// The file is owned by a `NamedTempFile`, so it is removed on success, failure,
/// and panic alike.
fn load_document(tenant_section: &str) -> IngestConfig {
    let mut file = NamedTempFile::with_suffix(".yaml").expect("temp config file");
    file.write_all(DOCUMENT_TEMPLATE.replace("{tenant}", tenant_section).as_bytes())
        .expect("write config document");
    file.flush().expect("flush config document");
    IngestConfig::from_file(file.path()).expect("the document must load and validate")
}

#[test]
fn a_document_tagged_multi_loads_as_the_multi_tenant_policy() {
    assert_eq!(load_document("tenant: !multi").tenant, TenantPolicy::Multi);
}

#[test]
fn a_document_tagged_single_loads_as_the_named_tenant() {
    let config = load_document("tenant: !single\n  id: \"acme\"");
    assert_eq!(config.tenant, TenantPolicy::Single { id: "acme".to_string() });
}

#[test]
fn a_document_without_a_tenant_section_keeps_the_default_tenant() {
    // Spelled out rather than read from `DEFAULT_TENANT_ID`: it is the value in
    // the deployed tables, so changing the constant must fail here.
    assert_eq!(
        load_document("").tenant,
        TenantPolicy::Single {
            id: "default".to_string()
        }
    );
}

/// The stand configuration is a second copy of the shape the chart renders under
/// `ingest.authProxy.enabled`, and nothing else reads it in CI. Loading it here
/// is what catches a stand config that stopped parsing or stopped being `!multi`
/// while the auth proxy still expects to write the tenant header itself.
#[test]
fn the_proxy_stand_document_is_multi_tenant_on_the_loopback() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/docker/ingest-proxy.yaml");

    let config = IngestConfig::from_file(path).expect("the proxy stand document must load and validate");

    assert_eq!(config.tenant, TenantPolicy::Multi);
    assert_eq!(config.otlp_http.host, "127.0.0.1");
    assert_eq!(config.otlp_grpc.host, "127.0.0.1");
}

/// The stand's ingest document: it names the tenant every other stand file
/// copies, so both tests below read it as the source of that value.
const STAND_INGEST_DOCUMENT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/docker/ingest.yaml");

/// The parts of the stand collector's document this test reads: the tenant header
/// its OTLP/HTTP exporter puts on every batch.
#[derive(Deserialize)]
struct CollectorDocument {
    exporters: CollectorExporters,
}

#[derive(Deserialize)]
struct CollectorExporters {
    otlphttp: CollectorOtlpHttpExporter,
}

#[derive(Deserialize)]
struct CollectorOtlpHttpExporter {
    headers: HashMap<String, String>,
}

/// The stand's tenant and the header its collector sends are one value written in
/// two files. In `single` a header naming another tenant is refused, so a drift
/// between them leaves a stand that starts clean and answers every export with a
/// 400. The assertion compares the two documents rather than each against a
/// literal: the rule is their equality, not the name they happen to agree on.
#[test]
fn the_stand_serves_exactly_the_tenant_its_collector_names() {
    let config = IngestConfig::from_file(STAND_INGEST_DOCUMENT).expect("the stand document must load and validate");

    let collector: CollectorDocument = load_config_file(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../config/docker/otel-collector/config.yaml"
    )))
    .expect("the stand collector document must load");
    // The exporter spells the header in its wire casing; `TENANT_ID_HEADER` is
    // the lowercase form HTTP/2 and gRPC store.
    let (_, tenant_header) = collector
        .exporters
        .otlphttp
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(TENANT_ID_HEADER))
        .expect("the stand collector must name a tenant on every batch");

    assert_eq!(
        config.tenant,
        TenantPolicy::Single {
            id: tenant_header.clone()
        }
    );
}

/// The stand's Grafana provisioning, whose datasources query IceGate on behalf of
/// the dashboards.
const STAND_DATASOURCES_DOCUMENT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../config/docker/grafana/provisioning/datasources/datasources.yaml"
);

/// The stand's query service as the datasources address it (`query` is its
/// Compose service name). The address is what marks a datasource as a reader of
/// IceGate: the stand also runs a real Prometheus, and IceGate answers the
/// Prometheus API itself, so the datasource `type` tells the two apart in neither
/// direction.
const STAND_QUERY_URL_PREFIX: &str = "http://query:";

/// The parts of the stand's datasource provisioning this test reads: where each
/// datasource reads from, and the tenant header it sends. Grafana splits a header
/// across two blocks — the name is public configuration, the value is a secret —
/// so both are read.
#[derive(Deserialize)]
struct DatasourcesDocument {
    datasources: Vec<Datasource>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Datasource {
    name: String,
    url: Option<String>,
    #[serde(default)]
    json_data: DatasourceJsonData,
    #[serde(default)]
    secure_json_data: DatasourceSecureJsonData,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DatasourceJsonData {
    http_header_name1: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DatasourceSecureJsonData {
    http_header_value1: Option<String>,
}

/// The read side of the stand carries a third copy of the tenant: every Grafana
/// datasource that queries IceGate sends it as `X-Scope-OrgID`. A query naming a
/// tenant nothing wrote to is not refused — the read path resolves the tenant
/// through `resolve_tenant_id` and answers with an empty selection — so a drift
/// from `config/docker/ingest.yaml` surfaces only as dashboards that went blank,
/// with no error anywhere.
///
/// The rule asserted here is "every datasource addressing the stand's query
/// service sends the tenant header, carrying the tenant this stand writes to".
/// Membership is decided by the address rather than by the header, because the
/// header is half of what is under test: a datasource that lost it would leave
/// a header-based selection and take its own failure with it. A datasource
/// deliberately pointed at another tenant of this stand would read nothing, so
/// there is no case for a weaker rule; `Prometheus` and `Jaeger` address services
/// of their own and are not in scope.
#[test]
fn every_stand_datasource_reads_the_tenant_the_stand_writes() {
    let config = IngestConfig::from_file(STAND_INGEST_DOCUMENT).expect("the stand document must load and validate");

    let document: DatasourcesDocument =
        load_config_file(Path::new(STAND_DATASOURCES_DOCUMENT)).expect("the stand datasource document must load");
    let icegate_readers: Vec<Datasource> = document
        .datasources
        .into_iter()
        .filter(|datasource| {
            datasource
                .url
                .as_deref()
                .is_some_and(|url| url.starts_with(STAND_QUERY_URL_PREFIX))
        })
        .collect();

    assert!(
        !icegate_readers.is_empty(),
        "the stand must provision datasources reading {STAND_QUERY_URL_PREFIX}"
    );

    for datasource in icegate_readers {
        // Grafana spells the header in its wire casing; `TENANT_ID_HEADER` is the
        // lowercase form HTTP/2 and gRPC store.
        let header_name = datasource.json_data.http_header_name1.unwrap_or_default();
        assert!(
            header_name.eq_ignore_ascii_case(TENANT_ID_HEADER),
            "the {} datasource reads IceGate, so it must name the tenant through {TENANT_ID_HEADER}, not {header_name:?}",
            datasource.name
        );

        assert_eq!(
            config.tenant,
            TenantPolicy::Single {
                id: datasource.secure_json_data.http_header_value1.unwrap_or_default()
            },
            "the {} datasource must read the tenant the stand writes to",
            datasource.name
        );
    }
}
