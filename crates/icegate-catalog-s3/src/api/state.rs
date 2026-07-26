//! Shared immutable state of the Catalog REST API handlers.

use std::collections::HashMap;
use std::sync::Arc;

use super::config::CatalogApiConfig;
use super::dto::CatalogConfigResponse;
use super::identifier::encode_namespace_separator;
use crate::S3Catalog;

/// Everything an API handler may read while serving a request.
///
/// Deliberately free of process and S3 settings: a REST request is answered from
/// the catalog, the validated API configuration, and the configuration response
/// derived from it once at startup.
#[derive(Clone)]
pub(super) struct CatalogApiState {
    pub(super) catalog: Arc<S3Catalog>,
    pub(super) config: Arc<CatalogApiConfig>,
    pub(super) catalog_config_response: Arc<CatalogConfigResponse>,
}

impl CatalogApiState {
    /// `endpoints` is the operation list `GET /v1/config` advertises, supplied by
    /// the router that owns those descriptors: state stores prepared state and
    /// does not reach back into routing to derive it.
    pub(super) fn new(catalog: Arc<S3Catalog>, config: CatalogApiConfig, endpoints: Vec<String>) -> Self {
        let catalog_config_response = Arc::new(build_config_response(&config, endpoints));
        Self {
            catalog,
            config: Arc::new(config),
            catalog_config_response,
        }
    }
}

/// Build the immutable `GET /v1/config` payload.
///
/// The overrides carry only what a client cannot infer: the warehouse this
/// single-warehouse server serves, the separator its namespace paths use, and
/// the prefix its resource paths are mounted under.
fn build_config_response(config: &CatalogApiConfig, endpoints: Vec<String>) -> CatalogConfigResponse {
    let mut overrides = HashMap::from([
        ("warehouse".to_string(), config.warehouse_uri().to_string()),
        ("namespace-separator".to_string(), encode_namespace_separator()),
    ]);
    if let Some(catalog_prefix) = config.catalog_prefix() {
        overrides.insert("prefix".to_string(), catalog_prefix.as_str().to_string());
    }
    CatalogConfigResponse {
        defaults: HashMap::new(),
        overrides,
        endpoints,
    }
}
