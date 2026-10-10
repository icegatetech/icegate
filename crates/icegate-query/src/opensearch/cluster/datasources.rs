//! `GET /_plugins/_query/_datasources`: the data sources of the query plugin. IceGate defines
//! none, so the answer is always the empty list and reads no table.

use axum::Json;
use serde_json::{Value, json};

use crate::opensearch::extract::{NoParameters, Parameters};

/// `GET /_plugins/_query/_datasources`.
pub(crate) async fn list_query_datasources(Parameters(NoParameters {}): Parameters<NoParameters>) -> Json<Value> {
    Json(json!([]))
}
