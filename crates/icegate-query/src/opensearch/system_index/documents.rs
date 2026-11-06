//! Single-document endpoints of system indices: create, index, get, update and delete.
//!
//! Writes go through the tenant's write loop, which applies them to the current snapshot and
//! swaps the pointer before answering, so a write is visible to the next request on any replica
//! and `refresh` has nothing left to do. Data indices are read-only: a write naming one is
//! `403 cluster_block_exception`. Document ids arrive percent-decoded by the path extractor.
//!
//! Every write and every delete that applies takes the index's next `_seq_no`, whether or not the
//! document existed, so a sequence number is never reused; `_primary_term` is always
//! [`PRIMARY_TERM`]. `if_seq_no` and `if_primary_term` come together or the request is a `400`. A
//! conditioned write on a missing document, or on a document whose `_seq_no` or term differs, is a
//! `409`, except an update without `upsert`, whose missing document is
//! `404 document_missing_exception` before any condition is checked. `_create` (or
//! `op_type=create`) of an existing id is a `409` and may not carry `if_seq_no`. A write to a
//! missing system index is `404 index_not_found_exception`: no write creates an index. A partial
//! `doc` merges objects key by key and replaces every other value, and a merge that changes
//! nothing answers `noop` without taking a `_seq_no`.

use axum::Json;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use icegate_common::TenantId;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::snapshot::{
    DocumentChange, DocumentDelete, DocumentUpdate, DocumentWrite, OpType, PRIMARY_TERM, SystemOperation,
    SystemOutcome, WriteConcurrency, WriteResult, WrittenDocument,
};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, Parameters, PathParameters, RequestBody};
use crate::opensearch::glob::matches_glob;
use crate::opensearch::script::recognize_document_script;
use crate::opensearch::state::OpenSearchState;

/// Parameters every document write ignores: the write is durable and visible when answered,
/// and there is one copy, so there is no refresh, wait or timeout to apply.
const IGNORED_WRITE_PARAMETERS: &[&str] = &["refresh", "timeout", "wait_for_active_shards"];

/// Parameters of `PUT /{index}/_create/{id}` and `PUT|POST /{index}/_doc/{id}`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DocumentWriteParameters {
    /// The `_seq_no` the stored document must have; comes with `if_primary_term`.
    if_seq_no: Option<i64>,
    /// The `_primary_term` the stored document must have; comes with `if_seq_no`.
    if_primary_term: Option<i64>,
    /// `create` or `index`; each endpoint states which it takes and which is its default.
    op_type: Option<String>,
}

impl EndpointParameters for DocumentWriteParameters {
    const IGNORED: &'static [&'static str] = IGNORED_WRITE_PARAMETERS;
}

/// Parameters of `GET /{index}/_doc/{id}`: a read always sees the latest write.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetDocumentParameters {}

impl EndpointParameters for GetDocumentParameters {
    const IGNORED: &'static [&'static str] = &["preference", "realtime", "refresh"];
}

/// Parameters of `DELETE /{index}/_doc/{id}`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeleteDocumentParameters {
    /// The `_seq_no` the stored document must have; comes with `if_primary_term`.
    if_seq_no: Option<i64>,
    /// The `_primary_term` the stored document must have; comes with `if_seq_no`.
    if_primary_term: Option<i64>,
}

impl EndpointParameters for DeleteDocumentParameters {
    const IGNORED: &'static [&'static str] = IGNORED_WRITE_PARAMETERS;
}

/// Parameters of `POST /{index}/_update/{id}`. `retry_on_conflict` is ignored: the write loop
/// applies an update to the snapshot it reads, so there is no internal conflict to retry.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateDocumentParameters {
    /// The `_seq_no` the stored document must have; comes with `if_primary_term`.
    if_seq_no: Option<i64>,
    /// The `_primary_term` the stored document must have; comes with `if_seq_no`.
    if_primary_term: Option<i64>,
    /// `_source`: whether the answer carries `get`, and which paths (see
    /// [`parse_returned_source`]).
    #[serde(rename = "_source")]
    source: Option<String>,
    /// `_source_includes`: the paths `get` returns; wins over `_source`.
    #[serde(rename = "_source_includes")]
    source_includes: Option<String>,
}

impl EndpointParameters for UpdateDocumentParameters {
    const IGNORED: &'static [&'static str] = &["refresh", "timeout", "wait_for_active_shards", "retry_on_conflict"];
}

/// The answer of `GET /{index}/_doc/{id}` for a stored document, in this key order. It borrows the
/// document, which can be large, so the answer serializes it without copying it first.
#[derive(Serialize)]
struct FoundDocument<'a> {
    /// The concrete index.
    #[serde(rename = "_index")]
    index: &'a str,
    /// The document `_id`.
    #[serde(rename = "_id")]
    id: &'a str,
    /// The `_seq_no` of the write that produced this version.
    #[serde(rename = "_seq_no")]
    seq_no: i64,
    /// Always [`PRIMARY_TERM`].
    #[serde(rename = "_primary_term")]
    primary_term: i64,
    /// Always `true`: a missing document has its own answer.
    #[serde(rename = "found")]
    is_found: bool,
    /// The `_source` as stored.
    #[serde(rename = "_source")]
    source: &'a Value,
}

/// `PUT /{index}/_create/{id}`: create a document; an existing id is a version conflict.
///
/// # Errors
///
/// Returns `403` `cluster_block_exception` for a data index; `400` for an `op_type` other than
/// `create`, a condition (a create has no stored document to condition on) or a body that is not a
/// JSON object; and the errors of [`answer_document_write`].
pub(crate) async fn create_document(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters((index, id)): PathParameters<(String, String)>,
    Parameters(parameters): Parameters<DocumentWriteParameters>,
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    let op_type = match parameters.op_type.as_deref() {
        None | Some("create") => OpType::Create,
        Some(other_op_type) => {
            return Err(OpenSearchError::illegal_argument(format!(
                "op_type [{other_op_type}] is not allowed on _create"
            )));
        }
    };
    let write = build_write_request(index, id, &parameters, op_type, &body)?;
    answer_document_write(&state, &tenant, write).await
}

/// `PUT|POST /{index}/_doc/{id}`: create or replace a document (`op_type=create` refuses to
/// replace).
///
/// # Errors
///
/// Returns `403` `cluster_block_exception` for a data index; `400` for an unknown `op_type`, a
/// one-sided condition, a condition with `op_type=create` or a body that is not a JSON object; and
/// the errors of [`answer_document_write`].
pub(crate) async fn index_document(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters((index, id)): PathParameters<(String, String)>,
    Parameters(parameters): Parameters<DocumentWriteParameters>,
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    let op_type = match parameters.op_type.as_deref() {
        None | Some("index") => OpType::Index,
        Some("create") => OpType::Create,
        Some(unknown_op_type) => {
            return Err(OpenSearchError::illegal_argument(format!(
                "unknown op_type [{unknown_op_type}]"
            )));
        }
    };
    let write = build_write_request(index, id, &parameters, op_type, &body)?;
    answer_document_write(&state, &tenant, write).await
}

/// The write a create or index request asks for, refused before the store is touched when it
/// names a data index, carries a one-sided condition or a non-object body.
fn build_write_request(
    index: String,
    id: String,
    parameters: &DocumentWriteParameters,
    op_type: OpType,
    body: &[u8],
) -> Result<DocumentWrite, OpenSearchError> {
    refuse_data_index_write(&index)?;
    let concurrency = WriteConcurrency::new(parameters.if_seq_no, parameters.if_primary_term)?;
    let source = require_document_object(parse_json_body(body)?)?;
    build_document_write(index, id, source, op_type, concurrency)
}

/// Apply `write` through the tenant's write loop and answer it: `201` for a new id, `200` for a
/// replaced document.
///
/// # Errors
///
/// Returns `404` `index_not_found_exception` when the index or alias names no system index; `409`
/// `version_conflict_engine_exception` for a create of an existing id or a condition that does not
/// hold; and every error of [`SystemIndexStore::write`](super::SystemIndexStore::write).
async fn answer_document_write(
    state: &OpenSearchState,
    tenant: &TenantId,
    write: DocumentWrite,
) -> Result<Response, OpenSearchError> {
    let id = write.id.clone();
    let outcome = state.system_store.write(tenant, SystemOperation::IndexDocument(write)).await?;
    let written_document = take_written_document(outcome)?;
    Ok((
        choose_write_status(written_document.result),
        Json(Value::Object(render_write(&written_document, &id))),
    )
        .into_response())
}

/// `GET /{index}/_doc/{id}`: the document, or `404 {found: false}` when the id is unknown.
///
/// # Errors
///
/// Returns `400` for a data index, which serves no document reads; `404`
/// `index_not_found_exception` for a name that is neither a system index nor an alias of the
/// tenant; `500` when the store cannot be read.
pub(crate) async fn get_document(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters((index, id)): PathParameters<(String, String)>,
    Parameters(_): Parameters<GetDocumentParameters>,
) -> Result<Response, OpenSearchError> {
    if !index.starts_with('.') {
        return Err(OpenSearchError::illegal_argument(format!(
            "[{index}] is a data index, which serves no document reads"
        )));
    }
    let current_snapshot = state.system_store.read(&tenant).await?;
    let concrete_index = current_snapshot
        .snapshot
        .resolve_name(&index)
        .ok_or_else(|| OpenSearchError::index_not_found(&index))?;
    let system_index = current_snapshot
        .snapshot
        .find_index(concrete_index)
        .ok_or_else(|| OpenSearchError::index_not_found(&index))?;
    let Some(document) = system_index.find_document(&id) else {
        let answer = json!({"_index": concrete_index, "_id": id, "found": false});
        return Ok((StatusCode::NOT_FOUND, Json(answer)).into_response());
    };
    Ok(Json(FoundDocument {
        index: concrete_index,
        id: &id,
        seq_no: document.seq_no,
        primary_term: PRIMARY_TERM,
        is_found: true,
        source: &document.source,
    })
    .into_response())
}

/// `DELETE /{index}/_doc/{id}`: `result: deleted`, or `404 result: not_found`.
///
/// # Errors
///
/// Returns `403` `cluster_block_exception` for a data index; `400` for a one-sided condition; `404`
/// `index_not_found_exception` when the index or alias names no system index; `409`
/// `version_conflict_engine_exception` for a condition that does not hold; and every error of
/// [`SystemIndexStore::write`](super::SystemIndexStore::write).
pub(crate) async fn delete_document(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters((index, id)): PathParameters<(String, String)>,
    Parameters(parameters): Parameters<DeleteDocumentParameters>,
) -> Result<Response, OpenSearchError> {
    refuse_data_index_write(&index)?;
    let concurrency = WriteConcurrency::new(parameters.if_seq_no, parameters.if_primary_term)?;
    let operation = SystemOperation::DeleteDocument(DocumentDelete {
        index,
        id: id.clone(),
        concurrency,
    });
    match state.system_store.write(&tenant, operation).await? {
        SystemOutcome::DocumentDeleted {
            index,
            found: is_found,
            seq_no,
        } => Ok((
            choose_delete_status(is_found),
            Json(Value::Object(render_delete(&index, &id, is_found, seq_no))),
        )
            .into_response()),
        SystemOutcome::IndexCreated { .. } | SystemOutcome::AliasesApplied | SystemOutcome::DocumentWritten(_) => Err(
            OpenSearchError::internal("the store answered a delete with another operation's outcome"),
        ),
    }
}

/// `POST /{index}/_update/{id}`: merge a partial `doc` or run a recognized script; `upsert`
/// creates a missing document; without it a missing document is `404`.
///
/// # Errors
///
/// Returns `403` `cluster_block_exception` for a data index; `400` for a one-sided condition, a
/// body [`parse_update_body`] refuses or a script that cannot run on the stored document; `404`
/// `index_not_found_exception` when the index or alias names no system index and
/// `document_missing_exception` for a missing document without `upsert`, with or without a
/// condition; `409` `version_conflict_engine_exception` for any other condition that does not
/// hold; and every error of [`SystemIndexStore::write`](super::SystemIndexStore::write).
pub(crate) async fn update_document(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    PathParameters((index, id)): PathParameters<(String, String)>,
    Parameters(parameters): Parameters<UpdateDocumentParameters>,
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    refuse_data_index_write(&index)?;
    let concurrency = WriteConcurrency::new(parameters.if_seq_no, parameters.if_primary_term)?;
    let (change, upsert) = parse_update_body(parse_json_body(&body)?)?;
    let returned_source = parse_returned_source(parameters.source.as_deref(), parameters.source_includes.as_deref());
    let operation = SystemOperation::UpdateDocument(DocumentUpdate {
        index,
        id: id.clone(),
        change,
        upsert,
        concurrency,
    });
    let written_document = take_written_document(state.system_store.write(&tenant, operation).await?)?;
    let mut answer = render_write(&written_document, &id);
    if let Some(includes) = returned_source {
        answer.insert("get".to_string(), render_update_get(&written_document, &includes));
    }
    Ok((
        choose_write_status(written_document.result),
        Json(Value::Object(answer)),
    )
        .into_response())
}

/// The written document of a create, index or update.
///
/// # Errors
///
/// Returns `500` when the store answers with another operation's outcome, which no request can
/// cause.
fn take_written_document(outcome: SystemOutcome) -> Result<WrittenDocument, OpenSearchError> {
    match outcome {
        SystemOutcome::DocumentWritten(written_document) => Ok(written_document),
        SystemOutcome::IndexCreated { .. } | SystemOutcome::AliasesApplied | SystemOutcome::DocumentDeleted { .. } => {
            Err(OpenSearchError::internal(
                "the store answered a write with another operation's outcome",
            ))
        }
    }
}

/// Refuse a write naming a data index: every index whose name does not start with `.` is a
/// data index, and data indices are read-only.
///
/// # Errors
///
/// `403 cluster_block_exception` naming `index`.
pub(crate) fn refuse_data_index_write(index: &str) -> Result<(), OpenSearchError> {
    if index.starts_with('.') {
        Ok(())
    } else {
        Err(OpenSearchError::cluster_block(index))
    }
}

/// Parse a JSON request body.
///
/// # Errors
///
/// `parsing_exception` for an empty or malformed body.
pub(crate) fn parse_json_body(body: &[u8]) -> Result<Value, OpenSearchError> {
    serde_json::from_slice(body).map_err(|error| OpenSearchError::parsing(format!("malformed JSON body: {error}")))
}

/// Accept `source` as a document `_source`.
///
/// # Errors
///
/// `parsing_exception` unless `source` is a JSON object.
pub(crate) fn require_document_object(source: Value) -> Result<Value, OpenSearchError> {
    if source.is_object() {
        Ok(source)
    } else {
        Err(OpenSearchError::parsing("a document source must be a JSON object"))
    }
}

/// Build a create or index write.
///
/// # Errors
///
/// `illegal_argument_exception` for a create conditioned on `if_seq_no`: a create has no
/// stored document to condition on.
pub(crate) fn build_document_write(
    index: String,
    id: String,
    source: Value,
    op_type: OpType,
    concurrency: WriteConcurrency,
) -> Result<DocumentWrite, OpenSearchError> {
    if op_type == OpType::Create && concurrency.is_conditional() {
        return Err(OpenSearchError::illegal_argument(
            "a create does not take if_seq_no and if_primary_term",
        ));
    }
    Ok(DocumentWrite {
        index,
        id,
        source,
        op_type,
        concurrency,
    })
}

/// Parse an update body: exactly one of a partial `doc` (an object) or a recognized `script`,
/// and an optional `upsert` object.
///
/// # Errors
///
/// `parsing_exception` for another key, both or neither of `doc` and `script`, or a non-object
/// `doc` / `upsert`; the errors of [`recognize_document_script`].
pub(crate) fn parse_update_body(body: Value) -> Result<(DocumentChange, Option<Value>), OpenSearchError> {
    let Value::Object(mut object) = body else {
        return Err(OpenSearchError::parsing("an update body must be a JSON object"));
    };
    if let Some(key) = object.keys().find(|key| !matches!(key.as_str(), "doc" | "script" | "upsert")) {
        return Err(OpenSearchError::parsing(format!("update does not support [{key}]")));
    }
    let change = match (object.remove("doc"), object.remove("script")) {
        (Some(partial_document @ Value::Object(_)), None) => DocumentChange::Merge(partial_document),
        (None, Some(script)) => DocumentChange::Script(recognize_document_script(&script)?),
        (Some(_), Some(_)) => return Err(OpenSearchError::parsing("an update takes doc or script, not both")),
        (Some(_), None) => return Err(OpenSearchError::parsing("[doc] must be a JSON object")),
        (None, None) => return Err(OpenSearchError::parsing("an update needs [doc] or [script]")),
    };
    let upsert = match object.remove("upsert") {
        None => None,
        Some(upsert @ Value::Object(_)) => Some(upsert),
        Some(_) => return Err(OpenSearchError::parsing("[upsert] must be a JSON object")),
    };
    Ok((change, upsert))
}

/// Split a comma-separated path list, dropping blanks.
pub(crate) fn split_paths(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(str::to_string)
        .collect()
}

/// Which part of the written `_source` an update answer returns under `get`, from the `_source`
/// and `_source_includes` parameters: `None` returns no `get`, an empty list the whole source,
/// otherwise the listed paths. `_source_includes` wins over any `_source`.
pub(crate) fn parse_returned_source(source: Option<&str>, source_includes: Option<&str>) -> Option<Vec<String>> {
    if let Some(includes) = source_includes {
        return Some(split_paths(includes));
    }
    match source? {
        "true" | "" => Some(Vec::new()),
        "false" => None,
        paths => Some(split_paths(paths)),
    }
}

/// Keep the paths of `source` that `includes` selects and `excludes` does not.
///
/// A pattern is a glob over a dotted path (`dashboard.*`); a selected object keeps its whole
/// subtree minus excluded paths; arrays are filtered element by element at the same path. An
/// empty `includes` selects everything. A scalar source has no paths: an empty `includes` returns
/// it as it is, and any other selects nothing, which answers `{}` as it does for an object no
/// pattern matches.
pub(crate) fn filter_source(source: &Value, includes: &[String], excludes: &[String]) -> Value {
    let selection = SourceSelection { includes, excludes };
    selection
        .filter_value(source, "", includes.is_empty())
        .unwrap_or_else(|| Value::Object(Map::new()))
}

/// The include and exclude patterns of one source filter.
struct SourceSelection<'a> {
    /// The globs selecting paths; empty selects everything.
    includes: &'a [String],
    /// The globs removing paths, whatever `includes` selects.
    excludes: &'a [String],
}

impl SourceSelection<'_> {
    /// The part of `value`, found at the dotted `path`, that the selection keeps; `None` when it
    /// keeps nothing. `is_included` says that `path` or a path enclosing it is selected, so
    /// everything below it is kept unless excluded.
    fn filter_value(&self, value: &Value, path: &str, is_included: bool) -> Option<Value> {
        // Nothing below a selected path can be dropped without an exclude, so the walk would
        // allocate a dotted path per key to rebuild this same value.
        if is_included && self.excludes.is_empty() {
            return Some(value.clone());
        }
        match value {
            Value::Object(object) => {
                let mut kept_entries = Map::new();
                for (key, child) in object {
                    let child_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{path}.{key}")
                    };
                    if self.excludes.iter().any(|pattern| matches_glob(pattern, &child_path)) {
                        continue;
                    }
                    let is_child_included =
                        is_included || self.includes.iter().any(|pattern| matches_glob(pattern, &child_path));
                    if let Some(filtered_child) = self.filter_value(child, &child_path, is_child_included) {
                        kept_entries.insert(key.clone(), filtered_child);
                    }
                }
                (is_included || !kept_entries.is_empty()).then_some(Value::Object(kept_entries))
            }
            Value::Array(elements) => {
                let kept_elements: Vec<Value> = elements
                    .iter()
                    .filter_map(|element| self.filter_value(element, path, is_included))
                    .collect();
                (is_included || !kept_elements.is_empty()).then_some(Value::Array(kept_elements))
            }
            leaf => is_included.then(|| leaf.clone()),
        }
    }
}

/// The answer body of a create, index or update, without `get`.
pub(crate) fn render_write(written_document: &WrittenDocument, id: &str) -> Map<String, Value> {
    let mut answer = Map::new();
    answer.insert("_index".to_string(), Value::String(written_document.index.clone()));
    answer.insert("_id".to_string(), Value::String(id.to_string()));
    answer.insert(
        "result".to_string(),
        Value::String(written_document.result.name().to_string()),
    );
    answer.insert("_shards".to_string(), render_write_shards());
    answer.insert("_seq_no".to_string(), Value::from(written_document.seq_no));
    answer.insert("_primary_term".to_string(), Value::from(written_document.primary_term));
    answer
}

/// The `get` block of an update answer: the written source filtered to `includes` (all of it
/// when `includes` is empty). `written_document` must be an update's outcome: only that carries a
/// source, and any other renders `_source: null`.
pub(crate) fn render_update_get(written_document: &WrittenDocument, includes: &[String]) -> Value {
    let source = written_document
        .source
        .as_ref()
        .map_or(Value::Null, |source| filter_source(source, includes, &[]));
    let mut get_block = Map::new();
    get_block.insert("_seq_no".to_string(), Value::from(written_document.seq_no));
    get_block.insert("_primary_term".to_string(), Value::from(written_document.primary_term));
    get_block.insert("found".to_string(), Value::Bool(true));
    get_block.insert("_source".to_string(), source);
    Value::Object(get_block)
}

/// The answer body of a delete.
pub(crate) fn render_delete(index: &str, id: &str, is_found: bool, seq_no: i64) -> Map<String, Value> {
    let mut answer = Map::new();
    answer.insert("_index".to_string(), Value::String(index.to_string()));
    answer.insert("_id".to_string(), Value::String(id.to_string()));
    answer.insert(
        "result".to_string(),
        Value::String(if is_found { "deleted" } else { "not_found" }.to_string()),
    );
    answer.insert("_shards".to_string(), render_write_shards());
    answer.insert("_seq_no".to_string(), Value::from(seq_no));
    answer.insert("_primary_term".to_string(), Value::from(PRIMARY_TERM));
    answer
}

/// `201` for a created document, `200` otherwise.
pub(crate) const fn choose_write_status(result: WriteResult) -> StatusCode {
    match result {
        WriteResult::Created => StatusCode::CREATED,
        WriteResult::Updated | WriteResult::Noop => StatusCode::OK,
    }
}

/// `200` for a removed document, `404` for an unknown id.
pub(crate) const fn choose_delete_status(is_found: bool) -> StatusCode {
    if is_found {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// One copy of every document: the write reached the only shard.
fn render_write_shards() -> Value {
    json!({"total": 1, "successful": 1, "failed": 0})
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;
    use crate::opensearch::error::render_envelope;
    use crate::opensearch::script::{CounterIncrement, DocumentScript};

    /// A counter script in its one-line form.
    const COUNTER_SOURCE: &str = "if (ctx._source[params.type][params.counterFieldName] == null) { ctx._source[params.type][params.counterFieldName] = params.count; } else { ctx._source[params.type][params.counterFieldName] += params.count; } ctx._source.updated_at = params.time;";

    fn build_paths(paths: &[&str]) -> Vec<String> {
        paths.iter().map(ToString::to_string).collect()
    }

    #[tokio::test]
    async fn an_empty_or_malformed_body_is_a_parsing_exception() {
        for body in [b"".as_slice(), b"{".as_slice()] {
            let error = parse_json_body(body).expect_err("not a JSON value");

            let (status, answer) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
            assert_eq!(answer["error"]["type"], "parsing_exception", "{body:?}");
        }
        assert_eq!(parse_json_body(br#"{"a": 1}"#).expect("a JSON object"), json!({"a": 1}));
    }

    #[tokio::test]
    async fn only_a_json_object_is_a_document_source() {
        for source in [json!([1]), json!("text")] {
            let error = require_document_object(source.clone()).expect_err("not an object");

            let (status, answer) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{source}");
            assert_eq!(answer["error"]["type"], "parsing_exception", "{source}");
        }
        assert_eq!(require_document_object(json!({})).expect("an object"), json!({}));
    }

    #[tokio::test]
    async fn a_create_conditioned_on_if_seq_no_is_refused_and_an_index_is_not() {
        let condition = WriteConcurrency::new(Some(0), Some(1)).expect("both values given");
        let build_write = |op_type, concurrency| {
            build_document_write(".kibana".to_string(), "a".to_string(), json!({}), op_type, concurrency)
        };

        let error = build_write(OpType::Create, condition).expect_err("a create takes no condition");

        let (status, answer) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(answer["error"]["type"], "illegal_argument_exception");
        assert_eq!(
            build_write(OpType::Index, condition)
                .expect("an index takes a condition")
                .concurrency,
            condition
        );
        assert_eq!(
            build_write(OpType::Create, WriteConcurrency::default())
                .expect("an unconditioned create")
                .op_type,
            OpType::Create
        );
    }

    #[tokio::test]
    async fn an_update_body_refuses_anything_but_doc_or_script_with_an_optional_upsert() {
        let refused_bodies = [
            ("a non-object body", json!([1])),
            ("an unknown key", json!({"doc": {}, "doc_as_upsert": true})),
            ("doc and script together", json!({"doc": {}, "script": {"source": "x"}})),
            ("neither doc nor script", json!({"upsert": {}})),
            ("a non-object doc", json!({"doc": 1})),
            ("a non-object upsert", json!({"doc": {}, "upsert": 1})),
        ];
        for (label, body) in refused_bodies {
            let error = parse_update_body(body).expect_err(label);

            let (status, answer) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{label}");
            assert_eq!(answer["error"]["type"], "parsing_exception", "{label}");
        }
    }

    #[test]
    fn an_update_body_parses_a_doc_or_a_recognized_script_with_an_optional_upsert() {
        let counter_script = json!({
            "source": COUNTER_SOURCE,
            "params": {"count": 1, "time": "t", "type": "ui-metric", "counterFieldName": "count"}
        });
        let expected_script = DocumentChange::Script(DocumentScript::IncrementCounter(CounterIncrement {
            object_type: "ui-metric".to_string(),
            counter_field_name: "count".to_string(),
            count: 1,
            time: "t".to_string(),
        }));
        let accepted_bodies = [
            (json!({"doc": {"a": 1}}), DocumentChange::Merge(json!({"a": 1})), None),
            (
                json!({"doc": {"a": 1}, "upsert": {"b": 2}}),
                DocumentChange::Merge(json!({"a": 1})),
                Some(json!({"b": 2})),
            ),
            (
                json!({"script": counter_script, "upsert": {"b": 2}}),
                expected_script,
                Some(json!({"b": 2})),
            ),
        ];
        for (body, expected_change, expected_upsert) in accepted_bodies {
            let (change, upsert) = parse_update_body(body.clone()).expect("an accepted body");

            assert_eq!(change, expected_change, "{body}");
            assert_eq!(upsert, expected_upsert, "{body}");
        }
    }

    #[test]
    fn the_returned_source_is_chosen_by_source_and_source_includes() {
        let cases = [
            ("neither parameter", None, None, None),
            ("_source=true", Some("true"), None, Some(build_paths(&[]))),
            ("an empty _source", Some(""), None, Some(build_paths(&[]))),
            ("_source=false", Some("false"), None, None),
            ("a _source list", Some("a,b"), None, Some(build_paths(&["a", "b"]))),
            (
                "a _source list with blanks",
                Some(" a, ,b,"),
                None,
                Some(build_paths(&["a", "b"])),
            ),
            (
                "_source_includes over _source=false",
                Some("false"),
                Some("a"),
                Some(build_paths(&["a"])),
            ),
            (
                "_source_includes over a _source list",
                Some("x,y"),
                Some("a,b"),
                Some(build_paths(&["a", "b"])),
            ),
        ];
        for (label, source, source_includes, expected) in cases {
            assert_eq!(parse_returned_source(source, source_includes), expected, "{label}");
        }
    }

    /// One row per way a step of the walk can branch: the JSON kind at the path, and whether the
    /// path is selected, excluded or neither.
    #[test]
    fn a_source_is_filtered_to_the_included_paths_minus_the_excluded_ones() {
        let source = json!({
            "type": "dashboard",
            "dashboard": {"title": "Logs", "panels": 2},
            "references": [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}],
            "tags": ["x", "y"]
        });
        let cases = [
            ("an empty includes selects everything", vec![], vec![], source.clone()),
            (
                "an exact path of a scalar",
                vec!["type"],
                vec![],
                json!({"type": "dashboard"}),
            ),
            (
                "an exact path inside an object",
                vec!["dashboard.title"],
                vec![],
                json!({"dashboard": {"title": "Logs"}}),
            ),
            (
                "a selected object keeps its subtree",
                vec!["dashboard"],
                vec![],
                json!({"dashboard": {"title": "Logs", "panels": 2}}),
            ),
            (
                "a glob",
                vec!["dashboard.*"],
                vec![],
                json!({"dashboard": {"title": "Logs", "panels": 2}}),
            ),
            (
                "an array of objects, element by element at the same path",
                vec!["references.id"],
                vec![],
                json!({"references": [{"id": "1"}, {"id": "2"}]}),
            ),
            (
                "an array of scalars at its own path",
                vec!["tags"],
                vec![],
                json!({"tags": ["x", "y"]}),
            ),
            ("no path selected", vec!["missing"], vec![], json!({})),
            (
                "an exclude removes a subtree",
                vec![],
                vec!["dashboard"],
                json!({
                    "type": "dashboard",
                    "references": [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}],
                    "tags": ["x", "y"]
                }),
            ),
            (
                "an exclude inside array elements",
                vec!["references"],
                vec!["references.name"],
                json!({"references": [{"id": "1"}, {"id": "2"}]}),
            ),
            (
                "include and exclude",
                vec!["dashboard.*"],
                vec!["dashboard.panels"],
                json!({"dashboard": {"title": "Logs"}}),
            ),
        ];
        for (label, includes, excludes, expected) in cases {
            let filtered_source = filter_source(&source, &build_paths(&includes), &build_paths(&excludes));

            assert_eq!(filtered_source, expected, "{label}");
        }
    }

    #[test]
    fn a_scalar_source_is_returned_as_it_is_only_when_includes_is_empty() {
        assert_eq!(filter_source(&json!("text"), &[], &[]), json!("text"));
        assert_eq!(filter_source(&json!("text"), &build_paths(&["a"]), &[]), json!({}));
    }
}
