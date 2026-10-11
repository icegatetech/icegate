//! `GET /<index>/_settings/index.number_of_shards`: every IceGate index reports one shard. No
//! other setting is served.

use axum::Json;
use axum::extract::{Extension, State};
use icegate_common::TenantId;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::opensearch::catalog::{IndexExpression, read_index_options};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, Parameters, PathParameters, deserialize_flag};
use crate::opensearch::state::OpenSearchState;

/// The one setting this endpoint serves.
const NUMBER_OF_SHARDS_SETTING: &str = "index.number_of_shards";

/// Query parameters of `<index>/_settings/<setting>`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SettingsParameters {
    /// Render the setting under its dotted key instead of nested objects; absent means nested.
    #[serde(default, deserialize_with = "deserialize_flag")]
    flat_settings: Option<bool>,
    /// Skip a missing concrete index instead of answering `404`; absent means `404`.
    #[serde(default, deserialize_with = "deserialize_flag")]
    ignore_unavailable: Option<bool>,
    /// Answer an empty object when nothing matches instead of `404`; absent means the empty object.
    #[serde(default, deserialize_with = "deserialize_flag")]
    allow_no_indices: Option<bool>,
    /// What a wildcard of the expression expands to; absent means open indices.
    expand_wildcards: Option<String>,
}

impl EndpointParameters for SettingsParameters {
    const IGNORED: &'static [&'static str] =
        &["local", "include_defaults", "cluster_manager_timeout", "master_timeout"];
}

/// `GET /<index>/_settings/<setting>`: `{<index>: {settings: ...}}` per selected index, with the
/// shard count `"1"` in the flat (`flat_settings`) or the nested form.
pub(crate) async fn read_index_setting(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters((expression_text, setting)): PathParameters<(String, String)>,
    Parameters(parameters): Parameters<SettingsParameters>,
) -> Result<Json<Value>, OpenSearchError> {
    if setting != NUMBER_OF_SHARDS_SETTING {
        return Err(OpenSearchError::illegal_argument(format!(
            "setting [{setting}] is not served; only [{NUMBER_OF_SHARDS_SETTING}] is"
        )));
    }
    let expression = IndexExpression::parse(&expression_text)?;
    let options = read_index_options(
        parameters.ignore_unavailable,
        parameters.allow_no_indices,
        parameters.expand_wildcards.as_deref(),
    )?;
    let settings = if parameters.flat_settings.unwrap_or(false) {
        json!({ NUMBER_OF_SHARDS_SETTING: "1" })
    } else {
        json!({"index": {"number_of_shards": "1"}})
    };
    let session = state.catalog.open_session(&tenant).await?;
    let body: Map<String, Value> = session
        .indices()
        .resolve(&expression, options)?
        .into_iter()
        .map(|entry| (entry.name().to_string(), json!({"settings": settings.clone()})))
        .collect();
    Ok(Json(Value::Object(body)))
}
