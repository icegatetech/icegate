//! `GET /_cluster/settings`: a static answer reporting the PPL engine flags IceGate serves.

use axum::Json;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::opensearch::extract::{EndpointParameters, Parameters, deserialize_flag};

/// Query parameters of `GET /_cluster/settings`; any other parameter is a 400.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClusterSettingsParameters {
    /// `true` renders setting names flat (`plugins.calcite.enabled`) instead of nested objects.
    #[serde(default, deserialize_with = "deserialize_flag")]
    flat_settings: Option<bool>,
    /// `true` adds the `defaults` section, which is empty: IceGate reports no other setting.
    #[serde(default, deserialize_with = "deserialize_flag")]
    include_defaults: Option<bool>,
}

impl EndpointParameters for ClusterSettingsParameters {
    const IGNORED: &'static [&'static str] = &["local", "master_timeout", "cluster_manager_timeout", "timeout"];
}

/// `GET /_cluster/settings`: `plugins.calcite.enabled` and `plugins.calcite.all_join_types.allowed`
/// are always `"true"` under `persistent`: IceGate has one PPL engine and no setting that switches it.
/// Nothing else is reported, and the answer reads no table. `filter_path` applies as on every endpoint.
pub(crate) async fn read_cluster_settings(
    Parameters(parameters): Parameters<ClusterSettingsParameters>,
) -> Json<Value> {
    let persistent = if parameters.flat_settings == Some(true) {
        json!({"plugins.calcite.enabled": "true", "plugins.calcite.all_join_types.allowed": "true"})
    } else {
        json!({"plugins": {"calcite": {"enabled": "true", "all_join_types": {"allowed": "true"}}}})
    };
    let mut body = Map::with_capacity(3);
    body.insert("persistent".to_owned(), persistent);
    body.insert("transient".to_owned(), Value::Object(Map::new()));
    if parameters.include_defaults == Some(true) {
        body.insert("defaults".to_owned(), Value::Object(Map::new()));
    }
    Json(Value::Object(body))
}
