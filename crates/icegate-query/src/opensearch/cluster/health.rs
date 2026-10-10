//! `GET /_cluster/health`: a single node that is always green. IceGate allocates no shards, so
//! waiting for any status returns at once.

use axum::Json;
use axum::extract::State;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::opensearch::extract::{EndpointParameters, Parameters};
use crate::opensearch::state::OpenSearchState;

/// The health parameters. Every one is accepted and ignored, because the answer cannot vary.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HealthParameters {}

impl EndpointParameters for HealthParameters {
    const IGNORED: &'static [&'static str] = &[
        "level",
        "local",
        "timeout",
        "wait_for_status",
        "wait_for_nodes",
        "wait_for_active_shards",
        "wait_for_no_relocating_shards",
        "wait_for_no_initializing_shards",
        "wait_for_events",
        "cluster_manager_timeout",
        "master_timeout",
    ];
}

/// `GET /_cluster/health`.
pub(crate) async fn report_cluster_health(
    State(state): State<OpenSearchState>,
    Parameters(HealthParameters {}): Parameters<HealthParameters>,
) -> Json<Value> {
    Json(json!({
        "cluster_name": state.node.cluster_name,
        "status": "green",
        "timed_out": false,
        "number_of_nodes": 1,
        "number_of_data_nodes": 1,
        "discovered_cluster_manager": true,
        "active_primary_shards": 0,
        "active_shards": 0,
        "relocating_shards": 0,
        "initializing_shards": 0,
        "unassigned_shards": 0,
        "delayed_unassigned_shards": 0,
        "number_of_pending_tasks": 0,
        "number_of_in_flight_fetch": 0,
        "task_max_waiting_in_queue_millis": 0,
        "active_shards_percent_as_number": 100.0
    }))
}
