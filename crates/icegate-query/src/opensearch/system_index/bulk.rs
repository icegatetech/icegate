//! `_bulk` and `_mget` on system indices.
//!
//! A `_bulk` body is NDJSON whose last byte is `\n`. Each action line is an object naming one of
//! `create`, `index`, `update` and `delete` (a repeated key collapses to the last one, as in every
//! JSON body on the listener), whose metadata may carry `_index` (else the path index), `_id`
//! (required and not empty: IceGate generates no ids, and no path can name an empty one),
//! `if_seq_no`, `if_primary_term` and `retry_on_conflict` (accepted with no effect: the write loop
//! applies an item once, against the snapshot it reads, so no internal retry exists for it to
//! bound). Any other metadata key, a missing source line or a malformed line is a `400` for the
//! whole request. An item naming a data index is refused alone with `403 cluster_block_exception`.
//!
//! A `_bulk` request is one store operation: its items apply in request order, each sees the
//! effects of the items before it, and they land in one pointer swap. Each item is atomic: a
//! refused or conflicting item changes nothing and the others still land. The answer lists every
//! item in request order with its `status`, and `errors` is true when any item failed; a delete of
//! an unknown id is `404 not_found`, which is not a failure. The `_source` and `_source_includes`
//! parameters return `get._source` on update items, and an update item keeps a source only for that
//! `get`, filtered as it applies. `max_answer_bytes` bounds what the items keep: past it each later
//! update item fails alone with `400` and changes nothing, while creates, indexes and deletes keep
//! applying. The bound acts while the items apply and never on the answer, because a `_bulk` must
//! not fail after the swap: its writes have landed by then.
//!
//! `_mget` reads one snapshot, answers `docs` in request order, and renders a missing index or a
//! data index as that item's `error`. Repeated ids let a small request multiply a stored document,
//! so its entries are rendered one at a time as the answer is serialized into a buffer bounded by
//! `max_answer_bytes`: an answer past the bound is refused whole with `400`, never cut short, and an
//! entry past it costs nothing.
//!
//! Both endpoints parse their body and render and serialize their answer on the blocking pool:
//! each step is linear in a body that a maximum-size import makes tens of megabytes long, and on
//! an async worker it would stall every other request that worker serves.

use std::borrow::Cow;
use std::io;
use std::time::Instant;

use axum::extract::{Extension, MatchedPath, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use icegate_common::TenantId;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Value};

use super::documents::{
    FoundDocument, IGNORED_READ_PARAMETERS, IGNORED_WRITE_PARAMETERS, build_document_write, choose_delete_status,
    choose_write_status, filter_source, find_system_document, parse_json_body, parse_returned_source,
    parse_update_body, refuse_data_index_read, refuse_data_index_write, render_delete, render_get_block,
    render_missing_document, render_write, require_document_object, split_paths,
};
use super::snapshot::{
    DocumentDelete, DocumentUpdate, DocumentWrite, OpType, ReturnedSources, SystemOperation, SystemOutcome,
    SystemSnapshot, WriteConcurrency,
};
use super::store::run_blocking_work;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, Parameters, PathParameters, RequestBody};
use crate::opensearch::state::OpenSearchState;

/// The action of one `_bulk` item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BulkAction {
    /// Create a document; an existing id conflicts.
    Create,
    /// Create or replace a document.
    Index,
    /// Update a document.
    Update,
    /// Delete a document.
    Delete,
}

impl BulkAction {
    /// The action an action line's key names, if it names one.
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "create" => Some(Self::Create),
            "index" => Some(Self::Index),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }

    /// The action's name: the key of its action line and of its answer item.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Index => "index",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// One parsed `_bulk` item.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BulkItem {
    /// A `create` or `index` item.
    Write(DocumentWrite),
    /// An `update` item.
    Update(DocumentUpdate),
    /// A `delete` item.
    Delete(DocumentDelete),
    /// An item refused at parse time: its error is answered without touching a document.
    Refused(RefusedBulkItem),
}

/// An item refused at parse time, with the error its answer carries.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RefusedBulkItem {
    /// The item's action.
    pub(crate) action: BulkAction,
    /// The index as the item named it.
    pub(crate) index: String,
    /// The item's `_id`.
    pub(crate) id: String,
    /// Why the item is refused, for example a data index's `cluster_block_exception`.
    pub(crate) error: OpenSearchError,
}

/// The metadata of one action line.
struct BulkMetadata {
    /// The item's `_index`, or the path index when it names none.
    index: String,
    /// The item's `_id`.
    id: String,
    /// The item's `if_seq_no` and `if_primary_term`.
    concurrency: WriteConcurrency,
}

/// What an item asks for beyond its metadata: its action with the source line that action needs,
/// so no item can be built with a source line it does not take, or without one it does.
enum BulkPayload {
    /// A `create` or `index`, with its document source.
    Document(OpType, Value),
    /// An `update`, with its update body.
    Update(Value),
    /// A `delete`, the one action with no source line.
    Delete,
}

/// Parse a `_bulk` NDJSON body into items, in request order. An item naming a data index parses
/// as [`BulkItem::Refused`]; any other bad line refuses the whole request.
///
/// # Errors
///
/// `illegal_argument_exception` when the body does not end with `\n`; when an action line names no
/// known action, carries an unknown metadata key, a non-string `_index` or `_id`, a non-integer
/// `if_seq_no` or `if_primary_term`, or only one of those two; when an item lacks `_id` or names an
/// empty one, lacks an index (neither `_index` nor `path_index`) or lacks its source line; for a
/// `create` conditioned on `if_seq_no`; and for a script
/// [`recognize_document_script`](crate::opensearch::script::recognize_document_script) refuses.
/// `parsing_exception` for a line that is not JSON, a `create` or `index` source that is not an
/// object, and any other update body [`parse_update_body`] refuses.
pub(crate) fn parse_bulk_body(body: &[u8], path_index: Option<&str>) -> Result<Vec<BulkItem>, OpenSearchError> {
    let Some(content) = body.strip_suffix(b"\n") else {
        return Err(OpenSearchError::illegal_argument(
            "the bulk request must be terminated by a newline [\\n]",
        ));
    };
    let mut lines = content.split(|byte| *byte == b'\n').enumerate();
    let mut items = Vec::new();
    while let Some((line_index, action_line)) = lines.next() {
        let line_number = line_index + 1;
        let (action, metadata) = parse_action_line(action_line, line_number, path_index)?;
        let mut read_source = || {
            let (_, source_line) = lines.next().ok_or_else(|| {
                OpenSearchError::illegal_argument(format!(
                    "the [{}] action on line [{line_number}] has no source line",
                    action.name()
                ))
            })?;
            parse_json_body(source_line)
        };
        let payload = match action {
            BulkAction::Create => BulkPayload::Document(OpType::Create, read_source()?),
            BulkAction::Index => BulkPayload::Document(OpType::Index, read_source()?),
            BulkAction::Update => BulkPayload::Update(read_source()?),
            BulkAction::Delete => BulkPayload::Delete,
        };
        items.push(build_bulk_item(action, metadata, payload)?);
    }
    Ok(items)
}

/// Parse one action line into its action and metadata, `_index` falling back to `path_index`.
fn parse_action_line(
    line: &[u8],
    line_number: usize,
    path_index: Option<&str>,
) -> Result<(BulkAction, BulkMetadata), OpenSearchError> {
    let make_malformed_error = || {
        OpenSearchError::illegal_argument(format!(
            "malformed action on line [{line_number}]: expected one of [create, delete, index, update]"
        ))
    };
    let Value::Object(action_object) = parse_json_body(line)? else {
        return Err(make_malformed_error());
    };
    let mut entries = action_object.into_iter();
    let (Some((name, metadata)), None) = (entries.next(), entries.next()) else {
        return Err(make_malformed_error());
    };
    let action = BulkAction::from_name(&name).ok_or_else(make_malformed_error)?;
    let Value::Object(metadata) = metadata else {
        return Err(make_malformed_error());
    };
    let mut index = path_index.map(str::to_string);
    let mut id = None;
    let (mut if_seq_no, mut if_primary_term) = (None, None);
    for (key, value) in metadata {
        match key.as_str() {
            "_index" => index = Some(read_metadata_text(value, &key, line_number)?),
            "_id" => id = Some(read_metadata_text(value, &key, line_number)?),
            "if_seq_no" => if_seq_no = Some(read_metadata_integer(&value, &key, line_number)?),
            "if_primary_term" => if_primary_term = Some(read_metadata_integer(&value, &key, line_number)?),
            // No effect: see the module doc.
            "retry_on_conflict" => {}
            other_key => {
                return Err(OpenSearchError::illegal_argument(format!(
                    "the action on line [{line_number}] does not support [{other_key}]"
                )));
            }
        }
    }
    let index = index
        .ok_or_else(|| OpenSearchError::illegal_argument(format!("the item on line [{line_number}] names no index")))?;
    let id = id.filter(|id: &String| !id.is_empty()).ok_or_else(|| {
        OpenSearchError::illegal_argument(format!("the item on line [{line_number}] needs a non-empty [_id]"))
    })?;
    let concurrency = WriteConcurrency::new(if_seq_no, if_primary_term)?;
    Ok((action, BulkMetadata { index, id, concurrency }))
}

/// The string `value` of metadata key `key`.
fn read_metadata_text(value: Value, key: &str, line_number: usize) -> Result<String, OpenSearchError> {
    match value {
        Value::String(text) => Ok(text),
        _ => Err(OpenSearchError::illegal_argument(format!(
            "[{key}] on line [{line_number}] must be a string"
        ))),
    }
}

/// The integer `value` of metadata key `key`.
fn read_metadata_integer(value: &Value, key: &str, line_number: usize) -> Result<i64, OpenSearchError> {
    value
        .as_i64()
        .ok_or_else(|| OpenSearchError::illegal_argument(format!("[{key}] on line [{line_number}] must be an integer")))
}

/// The item an action line and its source line ask for. A data index makes it a refused item before
/// its source is checked as a document or an update body, and before a `create` is refused for its
/// condition. The lines themselves were parsed first, so a source line that is not JSON or metadata
/// [`parse_action_line`] refuses still refuses the whole request.
fn build_bulk_item(
    action: BulkAction,
    metadata: BulkMetadata,
    payload: BulkPayload,
) -> Result<BulkItem, OpenSearchError> {
    let BulkMetadata { index, id, concurrency } = metadata;
    if let Err(error) = refuse_data_index_write(&index) {
        return Ok(BulkItem::Refused(RefusedBulkItem {
            action,
            index,
            id,
            error,
        }));
    }
    match payload {
        BulkPayload::Document(op_type, source) => {
            let source = require_document_object(source)?;
            Ok(BulkItem::Write(build_document_write(
                index,
                id,
                source,
                op_type,
                concurrency,
            )?))
        }
        BulkPayload::Update(body) => {
            let (change, upsert) = parse_update_body(body)?;
            Ok(BulkItem::Update(DocumentUpdate {
                index,
                id,
                change,
                upsert,
                concurrency,
            }))
        }
        BulkPayload::Delete => Ok(BulkItem::Delete(DocumentDelete { index, id, concurrency })),
    }
}

/// Parameters of `_bulk`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BulkParameters {
    /// `_source`: whether update items answer `get`, and which paths (see
    /// [`parse_returned_source`]).
    #[serde(rename = "_source")]
    source: Option<String>,
    /// `_source_includes`: the paths `get` returns; wins over `_source`.
    #[serde(rename = "_source_includes")]
    source_includes: Option<String>,
}

impl EndpointParameters for BulkParameters {
    const IGNORED: &'static [&'static str] = IGNORED_WRITE_PARAMETERS;
}

/// The action, requested index and id of one item, kept for its answer after the items move into
/// the store operation.
struct BulkItemDescription {
    /// The item's action, which keys its answer.
    action: BulkAction,
    /// The index as the item named it.
    index: String,
    /// The item's `_id`.
    id: String,
}

impl BulkItemDescription {
    /// The description of `item`, with the index as the request named it.
    fn from_item(item: &BulkItem) -> Self {
        let (action, index, id) = match item {
            BulkItem::Write(write) => {
                let action = match write.op_type {
                    OpType::Create => BulkAction::Create,
                    OpType::Index => BulkAction::Index,
                };
                (action, &write.index, &write.id)
            }
            BulkItem::Update(update) => (BulkAction::Update, &update.index, &update.id),
            BulkItem::Delete(delete) => (BulkAction::Delete, &delete.index, &delete.id),
            BulkItem::Refused(refused_item) => (refused_item.action, &refused_item.index, &refused_item.id),
        };
        Self {
            action,
            index: index.clone(),
            id: id.clone(),
        }
    }
}

/// The answer of `_bulk`, in this key order. It owns its items, so the answer serializes them
/// without copying them into one more value first. Its only documents are the sources update items
/// kept, which `max_answer_bytes` bounded while they applied.
#[derive(Serialize)]
struct BulkAnswer {
    /// How long the request took up to rendering its last item.
    #[serde(rename = "took")]
    took_millis: u64,
    /// Whether any item failed.
    #[serde(rename = "errors")]
    has_errors: bool,
    /// One entry per item, in request order.
    items: Vec<Value>,
}

/// `POST|PUT [/{index}]/_bulk`: apply every item in one store operation and answer each item.
///
/// # Errors
///
/// The request fails as a whole for a refusal of [`parse_bulk_body`], an error of
/// [`SystemIndexStore::write`](super::SystemIndexStore::write), and a `500` when the blocking work
/// does not finish or the store's answer does not match the items, which no request can cause. An
/// item that fails, an update item past `max_answer_bytes` included, is answered in the `items` of a
/// `200`.
pub(crate) async fn write_documents(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    path_index: Option<PathParameters<String>>,
    Parameters(parameters): Parameters<BulkParameters>,
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    let started_at = Instant::now();
    let returned_sources = parse_returned_source(parameters.source.as_deref(), parameters.source_includes.as_deref())
        .map(|includes| ReturnedSources {
            includes,
            max_answer_bytes: state.max_answer_bytes,
        });
    let (descriptions, refusals_or_items) = run_blocking_work("parsing a bulk body", move || {
        let items = parse_bulk_body(&body, path_index.as_ref().map(|PathParameters(index)| index.as_str()))?;
        let descriptions: Vec<BulkItemDescription> = items.iter().map(BulkItemDescription::from_item).collect();
        // Items no store operation takes are dropped here too, off the async worker.
        Ok((descriptions, collect_refusals(&items).ok_or(items)))
    })
    .await?;
    let outcomes = match refusals_or_items {
        Ok(refusals) => refusals,
        Err(items) => {
            let operation = SystemOperation::Bulk {
                items,
                returned_sources,
            };
            match state.system_store.write(&tenant, operation).await? {
                SystemOutcome::Bulk { items } => items,
                SystemOutcome::IndexCreated { .. }
                | SystemOutcome::AliasesApplied
                | SystemOutcome::DocumentWritten(_)
                | SystemOutcome::DocumentDeleted { .. } => {
                    return Err(OpenSearchError::internal(
                        "the store answered a bulk with another operation's outcome",
                    ));
                }
            }
        }
    };
    if outcomes.len() != descriptions.len() {
        return Err(OpenSearchError::internal(
            "the store answered a bulk with a different number of items",
        ));
    }
    let answer_body = run_blocking_work("rendering a bulk answer", move || {
        let has_errors = outcomes.iter().any(Result::is_err);
        let items = descriptions
            .iter()
            .zip(outcomes)
            .map(|(description, outcome)| render_bulk_item(description, outcome))
            .collect::<Result<Vec<Value>, OpenSearchError>>()?;
        let took_millis = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        serialize_bulk_answer(&BulkAnswer {
            took_millis,
            has_errors,
            items,
        })
    })
    .await?;
    Ok(build_json_response(answer_body))
}

/// The answers of a bulk whose every item was refused at parse time, which changes no state.
/// Answering here saves the write queue, the pointer read and the snapshot copy of a store
/// operation. `None` when any item is writable.
fn collect_refusals(items: &[BulkItem]) -> Option<Vec<Result<SystemOutcome, OpenSearchError>>> {
    items
        .iter()
        .map(|item| match item {
            BulkItem::Refused(refused_item) => Some(Err(refused_item.error.clone())),
            BulkItem::Write(_) | BulkItem::Update(_) | BulkItem::Delete(_) => None,
        })
        .collect()
}

/// One entry of a `_bulk` answer's `items`, keyed by its action. An item answers `get` when it kept
/// a source, which moves into the answer: the store already filtered it.
///
/// # Errors
///
/// Returns `500` when the store answers an item with another operation's outcome, which no request
/// can cause: the request fails, so the answer never reports `errors: false` beside a failed item.
fn render_bulk_item(
    description: &BulkItemDescription,
    outcome: Result<SystemOutcome, OpenSearchError>,
) -> Result<Value, OpenSearchError> {
    let answer = match outcome {
        Ok(SystemOutcome::DocumentWritten(mut written_document)) => {
            let mut answer = render_write(&written_document, &description.id);
            answer.insert(
                "status".to_string(),
                Value::from(choose_write_status(written_document.result).as_u16()),
            );
            if let Some(source) = written_document.source.take() {
                answer.insert("get".to_string(), render_get_block(&written_document, source));
            }
            answer
        }
        Ok(SystemOutcome::DocumentDeleted {
            index,
            found: is_found,
            seq_no,
        }) => {
            let mut answer = render_delete(&index, &description.id, is_found, seq_no);
            answer.insert(
                "status".to_string(),
                Value::from(choose_delete_status(is_found).as_u16()),
            );
            answer
        }
        Ok(SystemOutcome::IndexCreated { .. } | SystemOutcome::AliasesApplied | SystemOutcome::Bulk { .. }) => {
            return Err(OpenSearchError::internal(
                "the store answered a bulk item with another operation's outcome",
            ));
        }
        Err(error) => render_bulk_failure(description, &error),
    };
    let mut item = Map::new();
    item.insert(description.action.name().to_string(), Value::Object(answer));
    Ok(Value::Object(item))
}

/// The body of a failed `_bulk` item: the index as the item named it, its `status` and `error`.
fn render_bulk_failure(description: &BulkItemDescription, error: &OpenSearchError) -> Map<String, Value> {
    let mut answer = Map::new();
    answer.insert("_index".to_string(), Value::String(description.index.clone()));
    answer.insert("_id".to_string(), Value::String(description.id.clone()));
    answer.insert("status".to_string(), Value::from(error.status().as_u16()));
    answer.insert("error".to_string(), Value::Object(error.render_cause()));
    answer
}

/// Parameters of `_mget`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MgetParameters {}

impl EndpointParameters for MgetParameters {
    const IGNORED: &'static [&'static str] = IGNORED_READ_PARAMETERS;
}

/// One document `_mget` asks for.
#[derive(Debug, PartialEq, Eq)]
struct MgetRequest {
    /// The index or alias the request named.
    index: String,
    /// The document `_id`.
    id: String,
    /// The `_source` paths to answer: `None` omits `_source`, an empty list answers all of it.
    includes: Option<Vec<String>>,
}

/// The answer of `_mget`.
#[derive(Serialize)]
struct MgetAnswer<'a> {
    /// One entry per requested document, in request order.
    docs: MgetDocuments<'a>,
}

/// The `docs` of an `_mget` answer: a sequence over the requests, rendering each entry as it is
/// serialized. No entry outlives its turn, so the only memory that grows with the answer is the
/// buffer it is serialized into.
struct MgetDocuments<'a> {
    /// The snapshot every entry reads.
    snapshot: &'a SystemSnapshot,
    /// The requested documents, in request order.
    requests: &'a [MgetRequest],
}

impl Serialize for MgetDocuments<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.requests.iter().map(|request| render_mget_document(self.snapshot, request)))
    }
}

/// One entry of an `_mget` answer's `docs`.
#[derive(Serialize)]
#[serde(untagged)]
enum MgetDocument<'a> {
    /// A stored document.
    Found(FoundDocument<'a>),
    /// An unknown id of an existing index.
    Missing(Value),
    /// A document that could not be read, with its `error`.
    Failed(Value),
}

/// `GET|POST [/{index}]/_mget`: `docs` in request order, read from one snapshot.
///
/// # Errors
///
/// Returns the refusals of [`parse_mget_body`], `400` when the answer would pass
/// `max_answer_bytes`, and `500` when the store cannot be read or the blocking work does not finish;
/// a document that cannot be read is answered as that entry's `error`.
pub(crate) async fn get_documents(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    path_index: Option<PathParameters<String>>,
    Parameters(_): Parameters<MgetParameters>,
    RequestBody(body): RequestBody,
) -> Result<Response, OpenSearchError> {
    let requests = run_blocking_work("parsing an _mget body", move || {
        parse_mget_body(&body, path_index.as_ref().map(|PathParameters(index)| index.as_str()))
    })
    .await?;
    let current_snapshot = state.system_store.read(&tenant).await?;
    let max_answer_bytes = state.max_answer_bytes;
    let answer_body = run_blocking_work("rendering an _mget answer", move || {
        let docs = MgetDocuments {
            snapshot: &current_snapshot.snapshot,
            requests: &requests,
        };
        serialize_mget_answer(&MgetAnswer { docs }, max_answer_bytes)
    })
    .await?;
    Ok(build_json_response(answer_body))
}

/// Serialize a `_bulk` answer as JSON, for [`build_json_response`].
///
/// # Errors
///
/// Returns `500` when `answer` does not serialize, which no answer can cause: its map keys are
/// strings.
fn serialize_bulk_answer(answer: &BulkAnswer) -> Result<Vec<u8>, OpenSearchError> {
    serde_json::to_vec(answer).map_err(|error| OpenSearchError::internal(format!("serializing a bulk answer: {error}")))
}

/// Serialize an `_mget` answer as JSON, for [`build_json_response`], into at most `max_answer_bytes`.
///
/// # Errors
///
/// Returns `400` `answer_too_large` when the answer would pass the bound, so no client reads part of
/// one; `500` for any other failure, which no answer can cause: its map keys are strings.
fn serialize_mget_answer(answer: &MgetAnswer<'_>, max_answer_bytes: usize) -> Result<Vec<u8>, OpenSearchError> {
    let mut buffer = BoundedBuffer::new(max_answer_bytes);
    let serialization = serde_json::to_writer(&mut buffer, answer);
    if buffer.has_refused_write {
        return Err(OpenSearchError::answer_too_large(max_answer_bytes));
    }
    serialization.map_err(|error| OpenSearchError::internal(format!("serializing an _mget answer: {error}")))?;
    Ok(buffer.bytes)
}

/// The buffer of one answer, which refuses the write that would pass its bound and remembers that it
/// did, so a caller recognizes a breach from this state and not from the text of an [`io::Error`].
/// Nothing is reserved up front: a small answer allocates for its own size, not for the bound.
struct BoundedBuffer {
    /// The bytes accepted so far, never more than `max_bytes`.
    bytes: Vec<u8>,
    /// The most bytes `bytes` may hold.
    max_bytes: usize,
    /// Whether a write was refused.
    has_refused_write: bool,
}

impl BoundedBuffer {
    /// An empty buffer holding at most `max_bytes`.
    const fn new(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            max_bytes,
            has_refused_write: false,
        }
    }
}

impl io::Write for BoundedBuffer {
    /// Take all of `chunk`, or none of it when it would pass the bound.
    fn write(&mut self, chunk: &[u8]) -> io::Result<usize> {
        if chunk.len() > self.max_bytes - self.bytes.len() {
            self.has_refused_write = true;
            return Err(io::ErrorKind::FileTooLarge.into());
        }
        self.bytes.extend_from_slice(chunk);
        Ok(chunk.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The `200` of a serialized answer. Its content type is what the `filter_path` middleware reads to
/// recognize a JSON answer.
fn build_json_response(answer_body: Vec<u8>) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], answer_body).into_response()
}

/// Parse an `_mget` body: `docs`, `ids` (which need `path_index`) or both, `docs` and `ids` in the
/// order the body lists them.
///
/// # Errors
///
/// `parsing_exception` for a body that is not a JSON object, another key, a `docs` or `ids` that is
/// not an array, an id that is not a string, and the refusals of [`parse_mget_doc`];
/// `illegal_argument_exception` for `ids` without `path_index`.
fn parse_mget_body(body: &[u8], path_index: Option<&str>) -> Result<Vec<MgetRequest>, OpenSearchError> {
    let Value::Object(object) = parse_json_body(body)? else {
        return Err(OpenSearchError::parsing("an _mget body must be a JSON object"));
    };
    let mut requests = Vec::new();
    for (key, value) in object {
        match (key.as_str(), value) {
            ("docs", Value::Array(docs)) => {
                for doc in docs {
                    requests.push(parse_mget_doc(doc, path_index)?);
                }
            }
            ("ids", Value::Array(ids)) => {
                let index =
                    path_index.ok_or_else(|| OpenSearchError::illegal_argument("[ids] needs an index in the path"))?;
                for id in ids {
                    let Value::String(id) = id else {
                        return Err(OpenSearchError::parsing("[ids] holds strings"));
                    };
                    requests.push(MgetRequest {
                        index: index.to_string(),
                        id,
                        includes: Some(Vec::new()),
                    });
                }
            }
            (other_key, _) => {
                return Err(OpenSearchError::parsing(format!(
                    "_mget takes the arrays [docs] and [ids], not this [{other_key}]"
                )));
            }
        }
    }
    Ok(requests)
}

/// Parse one entry of `docs`: `_id`, and optionally `_index` (else `path_index`) and `_source`
/// (a boolean, a comma-separated string or a list of paths).
///
/// # Errors
///
/// `parsing_exception` for an entry that is not an object, another key, a non-string `_index`, or
/// a `_source` of any other shape; `illegal_argument_exception` for a missing or non-string `_id`
/// and for an entry that names no index.
fn parse_mget_doc(doc: Value, path_index: Option<&str>) -> Result<MgetRequest, OpenSearchError> {
    let Value::Object(mut doc) = doc else {
        return Err(OpenSearchError::parsing("each of [docs] is an object"));
    };
    if let Some(key) = doc.keys().find(|key| !matches!(key.as_str(), "_id" | "_index" | "_source")) {
        return Err(OpenSearchError::parsing(format!("_mget docs do not support [{key}]")));
    }
    let Some(Value::String(id)) = doc.remove("_id") else {
        return Err(OpenSearchError::illegal_argument("each of [docs] needs a string [_id]"));
    };
    let index = match doc.remove("_index") {
        Some(Value::String(index)) => index,
        None => path_index
            .map(str::to_string)
            .ok_or_else(|| OpenSearchError::illegal_argument("an _mget doc names no index"))?,
        Some(_) => return Err(OpenSearchError::parsing("[_index] must be a string")),
    };
    let includes = match doc.remove("_source") {
        None | Some(Value::Bool(true)) => Some(Vec::new()),
        Some(Value::Bool(false)) => None,
        Some(Value::String(paths)) => Some(split_paths(&paths)),
        Some(Value::Array(paths)) => Some(
            paths
                .into_iter()
                .map(|path| match path {
                    Value::String(path) => Ok(path),
                    _ => Err(OpenSearchError::parsing("[_source] lists strings")),
                })
                .collect::<Result<Vec<String>, OpenSearchError>>()?,
        ),
        Some(_) => {
            return Err(OpenSearchError::parsing(
                "[_source] must be a boolean, a string or a list",
            ));
        }
    };
    Ok(MgetRequest { index, id, includes })
}

/// The `docs` entry of `request` in `snapshot`. The whole `_source` is borrowed from the snapshot;
/// only a selection of paths builds a new value.
fn render_mget_document<'a>(snapshot: &'a SystemSnapshot, request: &'a MgetRequest) -> MgetDocument<'a> {
    let lookup = refuse_data_index_read(&request.index)
        .and_then(|()| find_system_document(snapshot, &request.index, &request.id));
    let (concrete_index, stored_document) = match lookup {
        Ok(found_location) => found_location,
        Err(error) => return MgetDocument::Failed(render_mget_failure(request, &error)),
    };
    let Some(document) = stored_document else {
        return MgetDocument::Missing(render_missing_document(concrete_index, &request.id));
    };
    let source = request.includes.as_deref().map(|includes| {
        if includes.is_empty() {
            Cow::Borrowed(&document.source)
        } else {
            Cow::Owned(filter_source(&document.source, includes, &[]))
        }
    });
    MgetDocument::Found(FoundDocument::new(concrete_index, &request.id, document.seq_no, source))
}

/// The `docs` entry of a document that could not be read: the index as the request named it and
/// the `error`.
fn render_mget_failure(request: &MgetRequest, error: &OpenSearchError) -> Value {
    let mut answer = Map::new();
    answer.insert("_index".to_string(), Value::String(request.index.clone()));
    answer.insert("_id".to_string(), Value::String(request.id.clone()));
    answer.insert("error".to_string(), Value::Object(error.render_cause()));
    Value::Object(answer)
}

/// `_reindex`, `_update_by_query`, `_delete_by_query` and `_tasks`: refused naming the route.
/// IceGate runs no background document tasks, so it has nothing to start or report.
pub(crate) async fn refuse_document_task_endpoint(route: MatchedPath) -> OpenSearchError {
    OpenSearchError::illegal_argument(format!("[{}] is not supported", route.as_str()))
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use axum::http::StatusCode;
    use serde_json::json;

    use super::*;
    use crate::opensearch::error::render_envelope;
    use crate::opensearch::system_index::snapshot::DocumentChange;

    async fn render_refusal(body: &str) -> (StatusCode, serde_json::Value) {
        let error = parse_bulk_body(body.as_bytes(), Some(".kibana")).expect_err("the body is refused");
        render_envelope(error).await
    }

    #[test]
    fn bulk_actions_create_index_update_delete_parse_with_their_sources() {
        let body = r#"{"create":{"_index":".kibana","_id":"dashboard:a"}}
{"type":"dashboard","dashboard":{"title":"Logs"}}
{"index":{"_id":"dashboard:b","if_seq_no":4,"if_primary_term":1}}
{"type":"dashboard"}
{"update":{"_id":"dashboard:a","retry_on_conflict":3}}
{"doc":{"dashboard":{"title":"Errors"}}}
{"delete":{"_index":".kibana_2","_id":"dashboard:c"}}
"#;

        let items = parse_bulk_body(body.as_bytes(), Some(".kibana")).expect("the bulk parses");

        assert_eq!(
            items,
            vec![
                BulkItem::Write(DocumentWrite {
                    index: ".kibana".to_string(),
                    id: "dashboard:a".to_string(),
                    source: json!({"type": "dashboard", "dashboard": {"title": "Logs"}}),
                    op_type: OpType::Create,
                    concurrency: WriteConcurrency::default(),
                }),
                BulkItem::Write(DocumentWrite {
                    index: ".kibana".to_string(),
                    id: "dashboard:b".to_string(),
                    source: json!({"type": "dashboard"}),
                    op_type: OpType::Index,
                    concurrency: WriteConcurrency {
                        if_seq_no: Some(4),
                        if_primary_term: Some(1)
                    },
                }),
                BulkItem::Update(DocumentUpdate {
                    index: ".kibana".to_string(),
                    id: "dashboard:a".to_string(),
                    change: DocumentChange::Merge(json!({"dashboard": {"title": "Errors"}})),
                    upsert: None,
                    concurrency: WriteConcurrency::default(),
                }),
                BulkItem::Delete(DocumentDelete {
                    index: ".kibana_2".to_string(),
                    id: "dashboard:c".to_string(),
                    concurrency: WriteConcurrency::default(),
                }),
            ]
        );
    }

    #[tokio::test]
    async fn a_bulk_body_without_a_trailing_newline_is_refused() {
        for body in [
            "{\"delete\":{\"_id\":\"dashboard:a\"}}",
            "{\"index\":{\"_id\":\"a\"}}\n{\"type\":\"dashboard\"}",
            "",
        ] {
            let (status, answer) = render_refusal(body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
            assert_eq!(answer["error"]["type"], "illegal_argument_exception", "{body:?}");
        }
    }

    #[tokio::test]
    async fn an_unknown_bulk_action_is_refused() {
        // A line that is JSON but not a one-action object is an illegal argument; a line that is
        // not JSON at all is a parsing error.
        for (body, expected_type) in [
            ("{\"upsert\":{\"_id\":\"a\"}}\n{}\n", "illegal_argument_exception"),
            (
                "{\"index\":{\"_id\":\"a\"},\"delete\":{\"_id\":\"b\"}}\n{}\n",
                "illegal_argument_exception",
            ),
            (
                "{\"index\":{\"_id\":\"a\",\"routing\":\"r\"}}\n{}\n",
                "illegal_argument_exception",
            ),
            ("[\"index\"]\n", "illegal_argument_exception"),
            ("not json\n", "parsing_exception"),
        ] {
            let (status, answer) = render_refusal(body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}");
            assert_eq!(answer["error"]["type"], expected_type, "{body:?}: {answer}");
        }
    }

    #[tokio::test]
    async fn an_item_without_an_id_or_without_its_source_line_is_refused() {
        for body in [
            "{\"index\":{\"_index\":\".kibana\"}}\n{\"type\":\"dashboard\"}\n",
            "{\"index\":{\"_id\":\"a\"}}\n",
            "{\"create\":{\"_id\":\"a\",\"if_seq_no\":1,\"if_primary_term\":1}}\n{}\n",
            "{\"index\":{\"_id\":\"a\"}}\n[1]\n",
        ] {
            let (status, answer) = render_refusal(body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body:?}: {answer}");
        }
        let without_index = parse_bulk_body(b"{\"delete\":{\"_id\":\"a\"}}\n", None).expect_err("no index anywhere");
        let (status, _) = render_envelope(without_index).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// One row per way an action line's metadata or an item's source can be refused. Each is a
    /// refusal of the whole request, so the good item before it must not reach the store.
    #[tokio::test]
    async fn bad_metadata_or_a_bad_source_refuses_the_whole_request() {
        const GOOD_ITEM: &str = "{\"delete\":{\"_id\":\"a\"}}\n";
        let cases = [
            ("{\"index\":1}\n{}\n", "illegal_argument_exception"),
            ("{\"index\":{\"_id\":1}}\n{}\n", "illegal_argument_exception"),
            // No path can name a document stored under an empty id.
            ("{\"index\":{\"_id\":\"\"}}\n{}\n", "illegal_argument_exception"),
            (
                "{\"index\":{\"_id\":\"a\",\"_index\":1}}\n{}\n",
                "illegal_argument_exception",
            ),
            (
                "{\"index\":{\"_id\":\"a\",\"if_seq_no\":\"1\",\"if_primary_term\":1}}\n{}\n",
                "illegal_argument_exception",
            ),
            (
                "{\"index\":{\"_id\":\"a\",\"if_seq_no\":1,\"if_primary_term\":1.5}}\n{}\n",
                "illegal_argument_exception",
            ),
            (
                "{\"index\":{\"_id\":\"a\",\"if_seq_no\":1}}\n{}\n",
                "illegal_argument_exception",
            ),
            (
                "{\"delete\":{\"_id\":\"a\",\"if_primary_term\":1}}\n",
                "illegal_argument_exception",
            ),
            ("{\"index\":{\"_id\":\"a\"}}\nnot json\n", "parsing_exception"),
            ("{\"create\":{\"_id\":\"a\"}}\n[1]\n", "parsing_exception"),
            ("{\"update\":{\"_id\":\"a\"}}\n{\"doc\":1}\n", "parsing_exception"),
            ("{\"update\":{\"_id\":\"a\"}}\n{}\n", "parsing_exception"),
            (
                "{\"update\":{\"_id\":\"a\"}}\n{\"script\":{\"source\":\"x\"}}\n",
                "illegal_argument_exception",
            ),
            (
                "{\"update\":{\"_id\":\"a\"}}\n{\"script\":\"x\"}\n",
                "illegal_argument_exception",
            ),
            ("\n", "parsing_exception"),
            ("{\"delete\":{\"_id\":\"a\"}}\n\n", "parsing_exception"),
            // A data index does not save an item whose lines cannot be read.
            (
                "{\"index\":{\"_index\":\"logs\",\"_id\":\"a\"}}\nnot json\n",
                "parsing_exception",
            ),
            (
                "{\"delete\":{\"_index\":\"logs\",\"_id\":\"a\",\"if_primary_term\":1}}\n",
                "illegal_argument_exception",
            ),
        ];
        for (bad_item, expected_type) in cases {
            let (status, answer) = render_refusal(&format!("{GOOD_ITEM}{bad_item}")).await;

            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad_item:?}");
            assert_eq!(answer["error"]["type"], expected_type, "{bad_item:?}: {answer}");
        }
        let invalid_utf8 = b"{\"index\":{\"_id\":\"a\"}}\n{\"a\":\"\xff\"}\n";
        let error = parse_bulk_body(invalid_utf8, Some(".kibana")).expect_err("the source is not UTF-8");
        let (status, answer) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(answer["error"]["type"], "parsing_exception");
    }

    /// The framing rule stated over every way to truncate a body: cut at any byte, a body parses
    /// only where the cut falls right after a complete item, and then as exactly the items before
    /// the cut. The bytes of one body enumerate what framing can branch on (inside a line, at a
    /// line end, between an action and its source), so random bodies would add no other case.
    #[test]
    fn a_bulk_body_cut_at_any_byte_parses_only_at_an_item_boundary() {
        let body =
            "{\"delete\":{\"_id\":\"a\"}}\n{\"index\":{\"_id\":\"b\"}}\n{\"é\":\"ü\"}\n{\"delete\":{\"_id\":\"c\"}}\n";
        let items = parse_bulk_body(body.as_bytes(), Some(".kibana")).expect("the whole body parses");
        // In bytes: 23 for the first line, 22 + 12 for the second item (`é` and `ü` take two each),
        // 23 for the last line.
        let item_ends: [usize; 3] = [23, 57, 80];
        assert_eq!(items.len(), item_ends.len());
        assert_eq!(item_ends[2], body.len());

        for cut in 0..=body.len() {
            let parsed_items = parse_bulk_body(&body.as_bytes()[..cut], Some(".kibana"));

            match item_ends.iter().position(|item_end| *item_end == cut) {
                Some(position) => {
                    assert_eq!(parsed_items.as_deref(), Ok(&items[..=position]), "cut at {cut}");
                }
                None => assert!(parsed_items.is_err(), "cut at {cut}"),
            }
        }
    }

    /// One row per refusal of the `_mget` parser, with and without an index in the path.
    #[tokio::test]
    async fn an_mget_body_that_cannot_name_its_documents_is_refused() {
        let cases = [
            ("{", Some(".kibana"), "parsing_exception"),
            ("[]", Some(".kibana"), "parsing_exception"),
            ("{\"size\":1}", Some(".kibana"), "parsing_exception"),
            ("{\"docs\":1}", Some(".kibana"), "parsing_exception"),
            ("{\"ids\":\"a\"}", Some(".kibana"), "parsing_exception"),
            ("{\"ids\":[\"a\"]}", None, "illegal_argument_exception"),
            ("{\"ids\":[1]}", Some(".kibana"), "parsing_exception"),
            ("{\"docs\":[1]}", Some(".kibana"), "parsing_exception"),
            (
                "{\"docs\":[{\"_id\":\"a\",\"routing\":\"r\"}]}",
                Some(".kibana"),
                "parsing_exception",
            ),
            (
                "{\"docs\":[{\"_index\":\".kibana\"}]}",
                Some(".kibana"),
                "illegal_argument_exception",
            ),
            (
                "{\"docs\":[{\"_id\":1}]}",
                Some(".kibana"),
                "illegal_argument_exception",
            ),
            ("{\"docs\":[{\"_id\":\"a\"}]}", None, "illegal_argument_exception"),
            (
                "{\"docs\":[{\"_id\":\"a\",\"_index\":1}]}",
                Some(".kibana"),
                "parsing_exception",
            ),
            (
                "{\"docs\":[{\"_id\":\"a\",\"_source\":1}]}",
                Some(".kibana"),
                "parsing_exception",
            ),
            (
                "{\"docs\":[{\"_id\":\"a\",\"_source\":[1]}]}",
                Some(".kibana"),
                "parsing_exception",
            ),
        ];
        for (body, path_index, expected_type) in cases {
            let error = parse_mget_body(body.as_bytes(), path_index).expect_err(body);

            let (status, answer) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(answer["error"]["type"], expected_type, "{body}");
        }
    }

    #[test]
    fn an_mget_body_names_its_documents_in_request_order() {
        let build_request = |index: &str, id: &str, includes: Option<&[&str]>| MgetRequest {
            index: index.to_string(),
            id: id.to_string(),
            includes: includes.map(|paths| paths.iter().map(ToString::to_string).collect()),
        };
        let cases = [
            ("{\"docs\":[]}", vec![]),
            ("{}", vec![]),
            (
                "{\"docs\":[{\"_id\":\"a\"},{\"_id\":\"b\",\"_index\":\".other\"}]}",
                vec![
                    build_request(".kibana", "a", Some(&[])),
                    build_request(".other", "b", Some(&[])),
                ],
            ),
            (
                "{\"docs\":[{\"_id\":\"a\",\"_source\":true},{\"_id\":\"b\",\"_source\":false}]}",
                vec![
                    build_request(".kibana", "a", Some(&[])),
                    build_request(".kibana", "b", None),
                ],
            ),
            (
                "{\"docs\":[{\"_id\":\"a\",\"_source\":\"type, title\"},{\"_id\":\"b\",\"_source\":[\"type\"]}]}",
                vec![
                    build_request(".kibana", "a", Some(&["type", "title"])),
                    build_request(".kibana", "b", Some(&["type"])),
                ],
            ),
            (
                "{\"ids\":[\"b\",\"a\"]}",
                vec![
                    build_request(".kibana", "b", Some(&[])),
                    build_request(".kibana", "a", Some(&[])),
                ],
            ),
            (
                "{\"ids\":[\"b\"],\"docs\":[{\"_id\":\"a\"}]}",
                vec![
                    build_request(".kibana", "b", Some(&[])),
                    build_request(".kibana", "a", Some(&[])),
                ],
            ),
        ];
        for (body, expected) in cases {
            let requests = parse_mget_body(body.as_bytes(), Some(".kibana")).expect(body);

            assert_eq!(requests, expected, "{body}");
        }
    }

    #[tokio::test]
    async fn a_data_index_item_parses_as_refused_with_its_cluster_block() {
        let body = "{\"index\":{\"_index\":\"logs\",\"_id\":\"1\"}}\n{\"body\":\"x\"}\n";

        let items = parse_bulk_body(body.as_bytes(), None).expect("the bulk parses");

        assert_eq!(items.len(), 1);
        let BulkItem::Refused(refused_item) = &items[0] else {
            panic!("expected a refused item, got {:?}", items[0]);
        };
        assert_eq!(
            (
                refused_item.action,
                refused_item.index.as_str(),
                refused_item.id.as_str()
            ),
            (BulkAction::Index, "logs", "1")
        );
        let (status, answer) = render_envelope(refused_item.error.clone()).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(answer["error"]["type"], "cluster_block_exception");
    }

    /// Each row would refuse the whole request on a system index, so the row proves the data index
    /// is checked first and the item fails alone.
    #[test]
    fn a_data_index_item_is_refused_alone_before_its_source_or_its_create_condition_is_checked() {
        let cases = [
            (
                "{\"create\":{\"_index\":\"logs\",\"_id\":\"1\"}}\n[1]\n",
                BulkAction::Create,
            ),
            (
                "{\"index\":{\"_index\":\"logs\",\"_id\":\"1\"}}\n\"text\"\n",
                BulkAction::Index,
            ),
            (
                "{\"create\":{\"_index\":\"logs\",\"_id\":\"1\",\"if_seq_no\":1,\"if_primary_term\":1}}\n{}\n",
                BulkAction::Create,
            ),
            (
                "{\"update\":{\"_index\":\"logs\",\"_id\":\"1\"}}\n{\"doc\":1}\n",
                BulkAction::Update,
            ),
            (
                "{\"update\":{\"_index\":\"logs\",\"_id\":\"1\"}}\n{\"script\":\"x\"}\n",
                BulkAction::Update,
            ),
        ];
        for (body, expected_action) in cases {
            let items = parse_bulk_body(body.as_bytes(), None).expect(body);

            assert_eq!(
                items,
                vec![BulkItem::Refused(RefusedBulkItem {
                    action: expected_action,
                    index: "logs".to_string(),
                    id: "1".to_string(),
                    error: OpenSearchError::cluster_block("logs"),
                })],
                "{body}"
            );
        }
    }

    /// One writable item, wherever it stands, sends the whole bulk to the store.
    #[test]
    fn refusals_are_collected_only_when_every_item_was_refused() {
        let build_refused_item = |index: &str, id: &str| {
            BulkItem::Refused(RefusedBulkItem {
                action: BulkAction::Index,
                index: index.to_string(),
                id: id.to_string(),
                error: OpenSearchError::cluster_block(index),
            })
        };
        let writable_item = BulkItem::Delete(DocumentDelete {
            index: ".kibana".to_string(),
            id: "a".to_string(),
            concurrency: WriteConcurrency::default(),
        });

        let refusals = collect_refusals(&[build_refused_item("logs", "1"), build_refused_item("spans", "2")]);
        let refusals_with_writable_last = collect_refusals(&[build_refused_item("logs", "1"), writable_item.clone()]);
        let refusals_with_writable_first = collect_refusals(&[writable_item, build_refused_item("logs", "1")]);

        assert_eq!(
            refusals,
            Some(vec![
                Err(OpenSearchError::cluster_block("logs")),
                Err(OpenSearchError::cluster_block("spans")),
            ])
        );
        assert_eq!(refusals_with_writable_last, None);
        assert_eq!(refusals_with_writable_first, None);
    }

    #[test]
    fn a_bounded_buffer_takes_a_write_ending_at_its_bound_and_refuses_one_byte_more() {
        let mut filled_buffer = BoundedBuffer::new(4);
        let mut overrun_buffer = BoundedBuffer::new(4);

        filled_buffer.write_all(b"ab").expect("below the bound");
        filled_buffer.write_all(b"cd").expect("ending at the bound");
        assert_eq!(
            (filled_buffer.bytes.as_slice(), filled_buffer.has_refused_write),
            (&b"abcd"[..], false)
        );
        filled_buffer.write_all(b"e").expect_err("one byte past the bound");
        overrun_buffer.write_all(b"abcde").expect_err("one write past the bound");

        assert_eq!(
            (filled_buffer.bytes.as_slice(), filled_buffer.has_refused_write),
            (&b"abcd"[..], true)
        );
        assert_eq!(
            (overrun_buffer.bytes.as_slice(), overrun_buffer.has_refused_write),
            (&b""[..], true)
        );
    }

    /// No request can reach this: `apply` answers each item with its own outcome. Were it reached,
    /// an item could not honestly answer it, because `errors` counts only the item failures.
    #[test]
    fn an_item_answered_with_another_operations_outcome_fails_the_request() {
        let description = BulkItemDescription {
            action: BulkAction::Index,
            index: ".kibana".to_string(),
            id: "a".to_string(),
        };
        let outcomes = [
            SystemOutcome::IndexCreated {
                name: ".kibana_1".to_string(),
            },
            SystemOutcome::AliasesApplied,
            SystemOutcome::Bulk { items: Vec::new() },
        ];
        for outcome in outcomes {
            let error = render_bulk_item(&description, Ok(outcome.clone())).expect_err("no item answer");

            assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR, "{outcome:?}");
        }
    }
}
