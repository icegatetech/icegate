//! `GET /` and `HEAD /`: the main response.

use axum::Json;
use axum::extract::State;
use serde_json::{Value, json};

use crate::opensearch::node::{
    BUILD_DATE, BUILD_HASH, BUILD_TYPE, DISTRIBUTION, LUCENE_VERSION, MINIMUM_INDEX_COMPATIBILITY_VERSION,
    MINIMUM_WIRE_COMPATIBILITY_VERSION, TAGLINE,
};
use crate::opensearch::state::OpenSearchState;

/// `GET /`, and `HEAD /`, which axum answers from the same route without a body: the node, the
/// cluster and IceGate's build block, whose `number` is the configured version.
pub(crate) async fn describe_cluster(State(state): State<OpenSearchState>) -> Json<Value> {
    let node = &state.node;
    Json(json!({
        "name": node.node_name,
        "cluster_name": node.cluster_name,
        "cluster_uuid": node.cluster_uuid,
        "version": {
            "distribution": DISTRIBUTION,
            "number": node.version.to_string(),
            "build_type": BUILD_TYPE,
            "build_hash": BUILD_HASH,
            "build_date": BUILD_DATE,
            "build_snapshot": false,
            "lucene_version": LUCENE_VERSION,
            "minimum_wire_compatibility_version": MINIMUM_WIRE_COMPATIBILITY_VERSION,
            "minimum_index_compatibility_version": MINIMUM_INDEX_COMPATIBILITY_VERSION
        },
        "tagline": TAGLINE
    }))
}
