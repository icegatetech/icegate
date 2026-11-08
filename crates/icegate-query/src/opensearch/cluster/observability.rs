//! Plugin endpoints IceGate answers without holding their data: the observability object list and
//! ML configs.

use axum::Json;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, NoParameters, Parameters, PathParameters};

/// Query parameters of `GET /_plugins/_observability/object`: the listed ones are accepted and ignored,
/// since the list is always empty; any other parameter is a 400.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObservabilityObjectParameters {}

impl EndpointParameters for ObservabilityObjectParameters {
    const IGNORED: &'static [&'static str] = &["objectType", "sortOrder", "fromIndex"];
}

/// `GET /_plugins/_observability/object`: IceGate stores no observability objects, so the list is
/// empty; the handler takes no catalog and reads no table.
pub(crate) async fn list_observability_objects(
    Parameters(ObservabilityObjectParameters {}): Parameters<ObservabilityObjectParameters>,
) -> Json<Value> {
    Json(json!({"observabilityObjectList": [], "startIndex": 0, "totalHitRelation": "eq", "totalHits": 0}))
}

/// `GET /_plugins/_ml/config/{id}`: IceGate holds no ML configs, so every id is a 404
/// `status_exception` naming it.
pub(crate) async fn read_ml_config(
    PathParameters(id): PathParameters<String>,
    Parameters(NoParameters {}): Parameters<NoParameters>,
) -> OpenSearchError {
    OpenSearchError::status_not_found(format!("no ML config [{id}]"))
}
