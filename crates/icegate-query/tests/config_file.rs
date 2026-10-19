//! `QueryConfig::from_file` over whole configuration documents: the stand's own,
//! and one written before the tenant policy existed.
//!
//! Nothing else loads `config/docker/query.yaml` in CI, and its `tenant` block
//! decides whether the stand's readers are served at all: without it the policy
//! falls back to `single` on `default`, and every datasource of the stand names
//! `demo`.
#![allow(clippy::expect_used)]

use std::{io::Write as _, path::PathBuf};

use icegate_common::{TenantPolicy, TlsConfig};
use icegate_query::QueryConfig;

/// The stand's query reads the tenant each reader names: the plain stand writes
/// `demo`, the auth-proxy stand writes the token's tenant, and both run this one
/// document. That every stand datasource sends the header carrying the tenant
/// the stand writes is pinned by
/// `every_stand_datasource_reads_the_tenant_the_stand_writes` in
/// `crates/icegate-ingest/tests/config_file.rs`.
#[test]
fn the_stand_query_document_reads_the_tenant_each_request_names() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/docker/query.yaml");

    let config = QueryConfig::from_file(path).expect("the stand query document must load and validate");

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
