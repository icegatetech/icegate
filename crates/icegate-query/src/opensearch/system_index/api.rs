//! Index-level endpoints of the system indices: index description and creation, aliases, refresh
//! and index templates.
//!
//! A dot-prefixed name addresses a system index or alias. Any other name addresses a data index,
//! which has no index-level metadata endpoint and accepts no write; an `_`-prefixed single segment
//! is an endpoint name no route serves.

use axum::Json;
use axum::extract::{Extension, State};
use axum::http::Method;
use axum::response::{IntoResponse, Response};
use icegate_common::TenantId;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value, json};

use super::snapshot::{AliasAction, SystemOperation, SystemOutcome, SystemSnapshot, validate_system_index_name};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, NoParameters, Parameters, PathParameters, RequestBody};
use crate::opensearch::glob::matches_glob;
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

/// Query parameters of `POST /_aliases` and `DELETE /_template/{name}`: none is read, the timeouts
/// are accepted and ignored because both apply at once.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcknowledgedParameters {}

impl EndpointParameters for AcknowledgedParameters {
    const IGNORED: &'static [&'static str] = &["timeout", "cluster_manager_timeout", "master_timeout"];
}

/// Query parameters of `_refresh`: none is read, the index options are accepted and ignored
/// because a refresh changes nothing.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RefreshParameters {}

impl EndpointParameters for RefreshParameters {
    const IGNORED: &'static [&'static str] = &["ignore_unavailable", "allow_no_indices", "expand_wildcards"];
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

/// The body of `POST /_aliases`; any other key is refused.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasActionsBody {
    /// The actions, in request order.
    actions: Vec<AliasActionBody>,
}

/// One action of `POST /_aliases`, keyed by its kind; any other kind is refused.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AliasActionBody {
    /// `{"add": {index, alias}}`.
    Add(AliasNamingBody),
    /// `{"remove": {index, alias}}`.
    Remove(AliasNamingBody),
    /// `{"remove_index": {index}}`.
    RemoveIndex(IndexNamingBody),
}

/// The index and the alias of one action.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasNamingBody {
    /// The index the alias names.
    index: String,
    /// The alias.
    alias: String,
}

/// The index of a `remove_index` action.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexNamingBody {
    /// The index to delete.
    index: String,
}

/// An answer keyed by index: `{<index>: <description>, ...}`, in the order of `descriptions`. It
/// borrows what it describes, which can be large (the stored mappings and settings), so the
/// answer serializes it without copying it first.
pub(crate) struct IndexKeyedAnswer<'a, D> {
    /// Each described index with what the answer says of it.
    pub(crate) descriptions: &'a [(&'a str, D)],
}

impl<D: Serialize> Serialize for IndexKeyedAnswer<'_, D> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.descriptions.iter().map(|(index, description)| (*index, description)))
    }
}

/// One system index as `GET /{index}` describes it, in this key order.
#[derive(Serialize)]
struct IndexDescription<'a> {
    /// The aliases naming the index.
    aliases: AliasMap<'a>,
    /// The mappings as stored.
    mappings: &'a Value,
    /// The settings as stored.
    settings: &'a Value,
}

/// One system index as `GET /{index}/_alias` describes it.
#[derive(Serialize)]
struct IndexAliases<'a> {
    /// The aliases naming the index.
    aliases: AliasMap<'a>,
}

/// Serializes as `{<alias>: {}}` for every alias naming `index`, in name order.
struct AliasMap<'a> {
    /// The snapshot the aliases are read from.
    snapshot: &'a SystemSnapshot,
    /// The index the aliases name.
    index: &'a str,
}

impl Serialize for AliasMap<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.snapshot.list_aliases_of(self.index).map(|alias| (alias, EmptyObject {})))
    }
}

/// Serializes as the empty object `{}`: an alias carries no filter or routing.
#[derive(Serialize)]
struct EmptyObject {}

/// `GET` / `HEAD /{index}`: `{<concrete index>: {aliases, mappings, settings}}`, the mappings and
/// settings as stored. An alias addresses the index it names. `HEAD` answers the same status
/// without a body.
///
/// # Errors
///
/// Returns `404` `index_not_found_exception` for a name that is neither a system index nor an
/// alias of the tenant; `400` for a data-index name or an `_`-prefixed segment; `500` when the
/// store cannot be read.
pub(crate) async fn describe_system_index(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters(index): PathParameters<String>,
    Parameters(_): Parameters<NoParameters>,
) -> Result<Response, OpenSearchError> {
    refuse_endpoint_names(&index, &Method::GET)?;
    refuse_data_index_metadata(&index)?;
    let current_snapshot = state.system_store.read(&tenant).await?;
    let snapshot = current_snapshot.snapshot.as_ref();
    let concrete_index = snapshot
        .resolve_name(&index)
        .ok_or_else(|| OpenSearchError::index_not_found(&index))?;
    let system_index = snapshot
        .find_index(concrete_index)
        .ok_or_else(|| OpenSearchError::index_not_found(&index))?;
    Ok(Json(IndexKeyedAnswer {
        descriptions: &[(
            concrete_index,
            IndexDescription {
                aliases: AliasMap {
                    snapshot,
                    index: concrete_index,
                },
                mappings: &system_index.mappings,
                settings: &system_index.settings,
            },
        )],
    })
    .into_response())
}

/// `PUT /{index}`: create a system index from `{mappings, settings}`; both are stored verbatim.
///
/// # Errors
///
/// Returns `403` `cluster_block_exception` for a non-dot name; `400` for an invalid system-index
/// name, an `_`-prefixed segment, a name an index or an alias already carries
/// (`resource_already_exists_exception`) or a body that is not `{mappings, settings}`
/// (`parsing_exception`); and every error of
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
        SystemOutcome::AliasesApplied => Err(OpenSearchError::internal(
            "an index creation answered with an alias outcome",
        )),
        SystemOutcome::DocumentWritten(_) | SystemOutcome::DocumentDeleted { .. } => Err(OpenSearchError::internal(
            "the store answered an index operation with a document outcome",
        )),
    }
}

/// `GET /_alias/{name}`: `{<index>: {aliases: {<alias>: {}}}}` for every alias `name` matches (a
/// comma list of names and patterns), each under the index it names.
///
/// # Errors
///
/// Returns `404` `index_not_found_exception` when `name` matches no alias of the tenant; `500`
/// when the store cannot be read.
pub(crate) async fn get_alias(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters(name): PathParameters<String>,
    Parameters(_): Parameters<NoParameters>,
) -> Result<Response, OpenSearchError> {
    let current_snapshot = state.system_store.read(&tenant).await?;
    let snapshot = current_snapshot.snapshot.as_ref();
    let patterns: Vec<&str> = name.split(',').collect();
    let mut body = Map::new();
    for alias in snapshot
        .alias_names()
        .filter(|alias| patterns.iter().any(|pattern| matches_glob(pattern, alias)))
    {
        // Every alias names an index of the same snapshot, so this skips nothing.
        let Some(index) = snapshot.resolve_name(alias) else {
            continue;
        };
        let index_entry = body.entry(index.to_string()).or_insert_with(|| json!({"aliases": {}}));
        index_entry["aliases"][alias] = json!({});
    }
    if body.is_empty() {
        return Err(OpenSearchError::index_not_found(&name));
    }
    Ok(Json(Value::Object(body)).into_response())
}

/// `GET /{index}/_alias`: `{<concrete index>: {aliases: {<alias>: {}}}}` for the index `index`
/// addresses, directly or through an alias.
///
/// # Errors
///
/// Returns `400` naming a data index, which has no aliases; `404` `index_not_found_exception` for
/// a name that is neither a system index nor an alias of the tenant; `500` when the store cannot
/// be read.
pub(crate) async fn get_index_aliases(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters(index): PathParameters<String>,
    Parameters(_): Parameters<NoParameters>,
) -> Result<Response, OpenSearchError> {
    refuse_data_index_metadata(&index)?;
    let current_snapshot = state.system_store.read(&tenant).await?;
    let snapshot = current_snapshot.snapshot.as_ref();
    let concrete_index = snapshot
        .resolve_name(&index)
        .ok_or_else(|| OpenSearchError::index_not_found(&index))?;
    Ok(Json(IndexKeyedAnswer {
        descriptions: &[(
            concrete_index,
            IndexAliases {
                aliases: AliasMap {
                    snapshot,
                    index: concrete_index,
                },
            },
        )],
    })
    .into_response())
}

/// `POST /_aliases`: apply `add`, `remove` and `remove_index` actions in order, all of them or
/// none.
///
/// # Errors
///
/// Returns `403` `cluster_block_exception` for an action naming a data index (nothing is applied);
/// `400` `parsing_exception` for a body that is not `{actions: [...]}` of those three kinds; and
/// every refusal of the actions themselves (see [`SystemSnapshot::apply`]) and every error of
/// [`SystemIndexStore::write`](super::SystemIndexStore::write).
pub(crate) async fn apply_aliases(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    Parameters(_): Parameters<AcknowledgedParameters>,
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    let request: AliasActionsBody = serde_json::from_slice(&body)
        .map_err(|error| OpenSearchError::parsing(format!("failed to parse the _aliases body: {error}")))?;
    let mut actions = Vec::with_capacity(request.actions.len());
    for action_body in request.actions {
        let action = match action_body {
            AliasActionBody::Add(naming) => AliasAction::Add {
                index: naming.index,
                alias: naming.alias,
            },
            AliasActionBody::Remove(naming) => AliasAction::Remove {
                index: naming.index,
                alias: naming.alias,
            },
            AliasActionBody::RemoveIndex(naming) => AliasAction::RemoveIndex { index: naming.index },
        };
        let (AliasAction::Add { index, .. } | AliasAction::Remove { index, .. } | AliasAction::RemoveIndex { index }) =
            &action;
        if !index.starts_with('.') {
            return Err(OpenSearchError::cluster_block(index));
        }
        actions.push(action);
    }
    match state
        .system_store
        .write(&tenant, SystemOperation::ApplyAliasActions { actions })
        .await?
    {
        SystemOutcome::AliasesApplied => Ok(Json(json!({"acknowledged": true})).into_response()),
        SystemOutcome::IndexCreated { .. } => Err(OpenSearchError::internal(
            "an alias request answered with an index outcome",
        )),
        SystemOutcome::DocumentWritten(_) | SystemOutcome::DocumentDeleted { .. } => Err(OpenSearchError::internal(
            "the store answered an alias operation with a document outcome",
        )),
    }
}

/// `GET|POST /{index}/_refresh`: every write of the store is visible to the next read on every
/// replica, so a refresh has nothing to do and reports one successful shard. Reads no index.
pub(crate) async fn refresh_index(
    PathParameters(_): PathParameters<String>,
    Parameters(_): Parameters<RefreshParameters>,
) -> Json<Value> {
    Json(json!({"_shards": {"total": 1, "successful": 1, "failed": 0}}))
}

/// `DELETE /_template/{name}`: acknowledged. The store keeps no index template, so there is none
/// to delete and none an index creation applies.
pub(crate) async fn delete_index_template(
    PathParameters(_): PathParameters<String>,
    Parameters(_): Parameters<AcknowledgedParameters>,
) -> Json<Value> {
    Json(json!({"acknowledged": true}))
}

/// Refuse an `_`-prefixed segment the way an unmatched route is refused: it names an endpoint,
/// never an index.
fn refuse_endpoint_names(index: &str, method: &Method) -> Result<(), OpenSearchError> {
    if index.starts_with('_') {
        return Err(OpenSearchError::no_handler(method, &format!("/{index}")));
    }
    Ok(())
}

/// Refuse an index-level metadata request for a data index: only system indices have one.
fn refuse_data_index_metadata(index: &str) -> Result<(), OpenSearchError> {
    if index.starts_with('.') {
        return Ok(());
    }
    Err(OpenSearchError::illegal_argument(format!(
        "[{index}] is a data index: index-level metadata endpoints serve system indices only"
    )))
}
