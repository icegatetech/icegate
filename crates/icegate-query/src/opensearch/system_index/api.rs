//! Index-level endpoints of the system indices: `GET` / `HEAD /{index}` and `PUT /{index}`.
//!
//! A dot-prefixed name addresses a system index. Any other name addresses a data index, which has
//! no index-level metadata endpoint and accepts no write; an `_`-prefixed single segment is an
//! endpoint name no route serves.

use axum::Json;
use axum::extract::{Extension, State};
use axum::http::Method;
use axum::response::{IntoResponse, Response};
use icegate_common::TenantId;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};

use super::snapshot::{SystemOperation, SystemOutcome, validate_system_index_name};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, NoParameters, Parameters, PathParameters, RequestBody};
use crate::opensearch::state::OpenSearchState;

/// Query parameters of `PUT /{index}`: none is read, the timeouts and shard waits are accepted
/// and ignored because an index of the store is created at once.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateIndexParameters {}

impl EndpointParameters for CreateIndexParameters {
    const IGNORED: &'static [&'static str] = &[
        "wait_for_active_shards",
        "timeout",
        "cluster_manager_timeout",
        "master_timeout",
    ];
}

/// The body of `PUT /{index}`; both keys may be absent, and any other key is refused.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateIndexBody {
    /// The mappings to store verbatim; `{}` when absent.
    mappings: Option<Value>,
    /// The settings to store verbatim; `{}` when absent.
    settings: Option<Value>,
}

/// The answer of `GET /{index}`: `{<index>: <description>}`. It borrows the stored mappings and
/// settings, which can be large, so the answer serializes them without copying them first.
struct IndexDescriptionAnswer<'a> {
    /// The described index.
    index: &'a str,
    /// What the answer says of it.
    description: IndexDescription<'a>,
}

impl Serialize for IndexDescriptionAnswer<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(std::iter::once((self.index, &self.description)))
    }
}

/// One system index as the answer describes it, in this key order.
#[derive(Serialize)]
struct IndexDescription<'a> {
    /// Always `{}`: a system index has no aliases.
    aliases: NoAliases,
    /// The mappings as stored.
    mappings: &'a Value,
    /// The settings as stored.
    settings: &'a Value,
}

/// Serializes as the empty object `{}`.
#[derive(Serialize)]
struct NoAliases {}

/// `GET` / `HEAD /{index}`: `{<index>: {aliases, mappings, settings}}`, the mappings and settings
/// as stored. `HEAD` answers the same status without a body.
///
/// # Errors
///
/// Returns `404` `index_not_found_exception` for a system index the tenant does not hold; `400`
/// for a data-index name or an `_`-prefixed segment; `500` when the store cannot be read.
pub(crate) async fn describe_system_index(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters(index): PathParameters<String>,
    Parameters(_): Parameters<NoParameters>,
) -> Result<Response, OpenSearchError> {
    refuse_endpoint_names(&index, &Method::GET)?;
    if !index.starts_with('.') {
        return Err(OpenSearchError::illegal_argument(format!(
            "[{index}] is a data index: GET and HEAD /<index> describe system indices only"
        )));
    }
    let current_snapshot = state.system_store.read(&tenant).await?;
    let system_index = current_snapshot
        .snapshot
        .find_index(&index)
        .ok_or_else(|| OpenSearchError::index_not_found(&index))?;
    Ok(Json(IndexDescriptionAnswer {
        index: &index,
        description: IndexDescription {
            aliases: NoAliases {},
            mappings: &system_index.mappings,
            settings: &system_index.settings,
        },
    })
    .into_response())
}

/// `PUT /{index}`: create a system index from `{mappings, settings}`; both are stored verbatim.
///
/// # Errors
///
/// Returns `403` `cluster_block_exception` for a non-dot name; `400` for an invalid system-index
/// name, an `_`-prefixed segment, an existing index (`resource_already_exists_exception`) or a
/// body that is not `{mappings, settings}` (`parsing_exception`); and every error of
/// [`SystemIndexStore::write`](super::SystemIndexStore::write).
pub(crate) async fn create_system_index(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters(index): PathParameters<String>,
    Parameters(_): Parameters<CreateIndexParameters>,
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    refuse_endpoint_names(&index, &Method::PUT)?;
    if !index.starts_with('.') {
        return Err(OpenSearchError::cluster_block(&index));
    }
    validate_system_index_name(&index)?;
    let request: CreateIndexBody = if body.iter().all(u8::is_ascii_whitespace) {
        CreateIndexBody::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|error| OpenSearchError::parsing(format!("failed to parse the create-index body: {error}")))?
    };
    let operation = SystemOperation::CreateIndex {
        name: index,
        mappings: request.mappings.unwrap_or_else(|| json!({})),
        settings: request.settings.unwrap_or_else(|| json!({})),
    };
    match state.system_store.write(&tenant, operation).await? {
        SystemOutcome::IndexCreated { name } => Ok(Json(json!({
            "acknowledged": true,
            "shards_acknowledged": true,
            "index": name
        }))
        .into_response()),
    }
}

/// Refuse an `_`-prefixed segment the way an unmatched route is refused: it names an endpoint,
/// never an index.
fn refuse_endpoint_names(index: &str, method: &Method) -> Result<(), OpenSearchError> {
    if index.starts_with('_') {
        return Err(OpenSearchError::no_handler(method, &format!("/{index}")));
    }
    Ok(())
}
