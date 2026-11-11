//! `GET /`, `HEAD /` and `GET /_nodes`.

use axum::Json;
use axum::extract::State;
use serde_json::{Map, Value, json};

use crate::opensearch::extract::{NoParameters, Parameters};
use crate::opensearch::node::{
    BUILD_DATE, BUILD_HASH, BUILD_TYPE, DISTRIBUTION, LUCENE_VERSION, MINIMUM_INDEX_COMPATIBILITY_VERSION,
    MINIMUM_WIRE_COMPATIBILITY_VERSION, TAGLINE,
};
use crate::opensearch::state::OpenSearchState;

/// `GET /`, and `HEAD /`, which axum answers from the same route without a body: the node, the
/// cluster and IceGate's build block, whose `number` is the configured version.
pub(crate) async fn describe_cluster(
    State(state): State<OpenSearchState>,
    Parameters(NoParameters {}): Parameters<NoParameters>,
) -> Json<Value> {
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

/// `GET /_nodes`: the one node of the cluster, keyed by its node id, with the version it reports
/// and the address a client reaches it at.
pub(crate) async fn list_nodes(
    State(state): State<OpenSearchState>,
    Parameters(NoParameters {}): Parameters<NoParameters>,
) -> Json<Value> {
    let node = &state.node;
    let ip = node.publish_address.ip().to_string();
    let publish_address = node.publish_address.to_string();
    let mut nodes = Map::new();
    nodes.insert(
        node.node_id.clone(),
        json!({
            "name": node.node_name,
            "host": ip,
            "ip": ip,
            "version": node.version.to_string(),
            "build_type": BUILD_TYPE,
            "build_hash": BUILD_HASH,
            "roles": ["data"],
            "http": {"bound_address": [node.bound_address.to_string()], "publish_address": publish_address}
        }),
    );
    Json(json!({
        "_nodes": {"total": 1, "successful": 1, "failed": 0},
        "cluster_name": node.cluster_name,
        "nodes": nodes
    }))
}
