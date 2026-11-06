//! The system-index state of one tenant, and the transitions writes apply to it.
//!
//! Transitions are pure and deterministic: the same operation applied to the same snapshot yields
//! the same outcome. The store relies on it when it re-applies an operation after a compare-and-swap
//! that did not land.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::io;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::bulk::BulkItem;
use super::documents::filter_source;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::script::DocumentScript;

/// Characters a system-index name must not contain.
const INVALID_INDEX_NAME_CHARACTERS: [char; 12] = ['\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':'];
/// Longest system-index name, in bytes.
const MAX_INDEX_NAME_BYTES: usize = 255;
/// Mapping types a field can be searched and aggregated on.
const AGGREGATABLE_TYPES: [&str; 14] = [
    "keyword",
    "date",
    "date_nanos",
    "long",
    "integer",
    "short",
    "byte",
    "double",
    "float",
    "half_float",
    "scaled_float",
    "unsigned_long",
    "boolean",
    "ip",
];
/// Mapping types a field can be searched on but not aggregated on.
const SEARCH_ONLY_TYPES: [&str; 2] = ["text", "match_only_text"];
/// The `_primary_term` of every system-index document. The store keeps one copy of each
/// document and never fails over, so the term never advances.
pub(crate) const PRIMARY_TERM: i64 = 1;

/// Everything one tenant keeps in its system indices: the state one snapshot object holds.
///
/// Which operations produced a snapshot is recorded in the lineage of its snapshot object, never
/// here, so the snapshot is exactly the state a reader sees.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct SystemSnapshot {
    /// The system indices, by name.
    indices: BTreeMap<String, SystemIndex>,
    /// Each alias and the one concrete index it names. An alias and an index never share a name.
    #[serde(default)]
    aliases: BTreeMap<String, String>,
}

/// One system index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SystemIndex {
    /// The mappings the index was created with, stored and returned verbatim: `dynamic`, `_meta`,
    /// unknown keys and key order included.
    pub(crate) mappings: Value,
    /// The settings the index was created with, stored and returned verbatim.
    pub(crate) settings: Value,
    /// Documents by `_id`.
    #[serde(default)]
    documents: BTreeMap<String, StoredDocument>,
    /// The `_seq_no` the next write or delete of this index takes. It only grows, so a
    /// sequence number is never reused.
    #[serde(default)]
    next_seq_no: i64,
}

impl SystemIndex {
    /// An index with no documents, holding `mappings` and `settings` verbatim.
    pub(crate) const fn new(mappings: Value, settings: Value) -> Self {
        Self {
            mappings,
            settings,
            documents: BTreeMap::new(),
            next_seq_no: 0,
        }
    }

    /// The document stored under `id`.
    pub(crate) fn find_document(&self, id: &str) -> Option<&StoredDocument> {
        self.documents.get(id)
    }

    /// The documents in `_id` order.
    pub(crate) fn documents(&self) -> impl Iterator<Item = (&str, &StoredDocument)> {
        self.documents.iter().map(|(id, document)| (id.as_str(), document))
    }

    /// The number of documents the index holds; deleted documents are not counted.
    pub(crate) fn document_count(&self) -> u64 {
        u64::try_from(self.documents.len()).unwrap_or(u64::MAX)
    }

    /// Store `source` under `id` with the next `_seq_no`, which it returns.
    fn store_document(&mut self, id: &str, source: Value) -> i64 {
        let seq_no = Self::take_seq_no(&mut self.next_seq_no);
        self.documents.insert(id.to_string(), StoredDocument { source, seq_no });
        seq_no
    }

    /// Hand out the next `_seq_no` of an index's `next_seq_no`; no two calls return the same one.
    /// It takes the field, not the index, so an update can hold its stored document meanwhile.
    const fn take_seq_no(next_seq_no: &mut i64) -> i64 {
        let seq_no = *next_seq_no;
        *next_seq_no += 1;
        seq_no
    }

    /// The fields the stored mapping declares, depth first in mapping order. A property without
    /// a `type` is an `object`; object and nested properties list their children after them, and
    /// multi-fields follow their field.
    pub(crate) fn list_mapped_fields(&self) -> Vec<MappedField> {
        let mut fields = Vec::new();
        if let Some(properties) = self.mappings.get("properties").and_then(Value::as_object) {
            collect_mapped_fields(properties, "", &mut fields);
        }
        fields
    }
}

/// Append the fields of `properties`, named under `prefix`, to `fields`.
fn collect_mapped_fields(properties: &Map<String, Value>, prefix: &str, fields: &mut Vec<MappedField>) {
    for (key, mapping) in properties {
        let name = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        let type_name = mapping.get("type").and_then(Value::as_str).unwrap_or("object");
        let is_aggregatable = AGGREGATABLE_TYPES.contains(&type_name);
        fields.push(MappedField {
            name: name.clone(),
            type_name: type_name.to_string(),
            is_searchable: is_aggregatable || SEARCH_ONLY_TYPES.contains(&type_name),
            is_aggregatable,
        });
        if let Some(multi_fields) = mapping.get("fields").and_then(Value::as_object) {
            collect_mapped_fields(multi_fields, &name, fields);
        }
        if let Some(children) = mapping.get("properties").and_then(Value::as_object) {
            collect_mapped_fields(children, &name, fields);
        }
    }
}

/// One document of a system index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredDocument {
    /// The `_source`, stored and returned verbatim.
    pub(crate) source: Value,
    /// The `_seq_no` of the write that produced this version.
    pub(crate) seq_no: i64,
}

/// How a document write treats an existing document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpType {
    /// Replace an existing document, or create a missing one.
    Index,
    /// Create only: an existing id is a version conflict.
    Create,
}

/// The optimistic-concurrency condition of a write. Both values or neither are set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct WriteConcurrency {
    /// The `_seq_no` the stored document must have.
    pub(crate) if_seq_no: Option<i64>,
    /// The `_primary_term` the stored document must have.
    pub(crate) if_primary_term: Option<i64>,
}

impl WriteConcurrency {
    /// Build a condition from the request's `if_seq_no` and `if_primary_term`.
    ///
    /// # Errors
    ///
    /// `illegal_argument_exception` when only one of the two is given.
    pub(crate) fn new(if_seq_no: Option<i64>, if_primary_term: Option<i64>) -> Result<Self, OpenSearchError> {
        if if_seq_no.is_some() != if_primary_term.is_some() {
            return Err(OpenSearchError::illegal_argument(
                "if_seq_no and if_primary_term must be given together",
            ));
        }
        Ok(Self {
            if_seq_no,
            if_primary_term,
        })
    }

    /// Whether the write is conditioned on the stored document.
    pub(crate) const fn is_conditional(&self) -> bool {
        self.if_seq_no.is_some()
    }

    /// Check the condition against the document the write would replace; `None` is a missing
    /// document, which no condition matches.
    fn check_stored_document(
        &self,
        index: &str,
        id: &str,
        stored_document: Option<&StoredDocument>,
    ) -> Result<(), OpenSearchError> {
        let (Some(if_seq_no), Some(if_primary_term)) = (self.if_seq_no, self.if_primary_term) else {
            return Ok(());
        };
        match stored_document {
            Some(document) if document.seq_no == if_seq_no && if_primary_term == PRIMARY_TERM => Ok(()),
            Some(_) | None => Err(OpenSearchError::version_conflict(index, id)),
        }
    }
}

/// A create or index of one document.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DocumentWrite {
    /// The index or alias the request named.
    pub(crate) index: String,
    /// The document `_id`, already percent-decoded.
    pub(crate) id: String,
    /// The new `_source`, a JSON object.
    pub(crate) source: Value,
    /// Whether an existing document may be replaced.
    pub(crate) op_type: OpType,
    /// The condition on the stored document.
    pub(crate) concurrency: WriteConcurrency,
}

/// What an update changes in an existing document.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DocumentChange {
    /// A partial document merged into `_source`.
    Merge(Value),
    /// A recognized script run on `_source`.
    Script(DocumentScript),
}

/// An update of one document.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DocumentUpdate {
    /// The index or alias the request named.
    pub(crate) index: String,
    /// The document `_id`.
    pub(crate) id: String,
    /// The change applied to an existing document.
    pub(crate) change: DocumentChange,
    /// The `_source` stored as given when the document is missing; the change does not run then.
    pub(crate) upsert: Option<Value>,
    /// The condition on the stored document.
    pub(crate) concurrency: WriteConcurrency,
}

/// A delete of one document.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DocumentDelete {
    /// The index or alias the request named.
    pub(crate) index: String,
    /// The document `_id`.
    pub(crate) id: String,
    /// The condition on the stored document.
    pub(crate) concurrency: WriteConcurrency,
}

/// The `result` of a document write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteResult {
    /// The id was new.
    Created,
    /// An existing document was replaced or changed.
    Updated,
    /// An update changed nothing; no `_seq_no` was taken.
    Noop,
}

impl WriteResult {
    /// The `result` value of a write answer.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Updated => "updated",
            Self::Noop => "noop",
        }
    }
}

/// The outcome of a create, index or update.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WrittenDocument {
    /// The concrete index the request's index or alias resolved to.
    pub(crate) index: String,
    /// What the write did.
    pub(crate) result: WriteResult,
    /// The document's `_seq_no` after the write.
    pub(crate) seq_no: i64,
    /// The document's `_primary_term`.
    pub(crate) primary_term: i64,
    /// What the write keeps of the `_source` for its answer's `get`: nothing on a create or an index,
    /// all of it on `_update`, and on a `_bulk` update only what its `get` renders, which is nothing
    /// when the request asked for no `get`.
    pub(crate) source: Option<Value>,
}

/// What the update items of a `_bulk` return under `get`, and the bound on what they keep for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReturnedSources {
    /// The `_source` paths `get` returns; empty returns all of it.
    pub(crate) includes: Vec<String>,
    /// `opensearch.system_indices.max_answer_bytes`: once the sources kept so far reach it, each
    /// later update item is refused before it changes anything. A bound checked before an item
    /// applies lets the total pass by that one item's source, and no write ever has to be undone.
    pub(crate) max_answer_bytes: usize,
}

/// A change a write asks the store to apply to a tenant's snapshot.
#[derive(Debug, Clone)]
pub(crate) enum SystemOperation {
    /// Create the system index `name`, refused when it exists.
    CreateIndex {
        /// A name [`validate_system_index_name`] accepts.
        name: String,
        /// The mappings to store verbatim.
        mappings: Value,
        /// The settings to store verbatim.
        settings: Value,
    },
    /// Apply alias actions in order, all of them or none.
    ApplyAliasActions {
        /// The actions, in request order.
        actions: Vec<AliasAction>,
    },
    /// Create or replace one document (`_create`, `_doc`).
    IndexDocument(DocumentWrite),
    /// Update one document (`_update`).
    UpdateDocument(DocumentUpdate),
    /// Delete one document.
    DeleteDocument(DocumentDelete),
    /// The items of one `_bulk`, applied in order in at most one pointer swap.
    Bulk {
        /// The parsed items, refused ones included.
        items: Vec<BulkItem>,
        /// What update items return under `get`; `None` when the request asked for no `get`, so
        /// they keep no source.
        returned_sources: Option<ReturnedSources>,
    },
}

/// What applying an operation did.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SystemOutcome {
    /// The index `name` was created.
    IndexCreated {
        /// The created index.
        name: String,
    },
    /// Every alias action of a request was applied.
    AliasesApplied,
    /// A document was created, replaced or updated.
    DocumentWritten(WrittenDocument),
    /// A delete ran. Every delete takes a `_seq_no`, whether or not the document existed.
    DocumentDeleted {
        /// The concrete index.
        index: String,
        /// Whether a document was removed.
        found: bool,
        /// The `_seq_no` the delete took.
        seq_no: i64,
    },
    /// The outcome of each `_bulk` item, in request order.
    Bulk {
        /// One outcome per item: a failing item changed nothing.
        items: Vec<Result<Self, OpenSearchError>>,
    },
}

impl SystemOutcome {
    /// Whether the operation that answered this outcome changed the snapshot, so that the store
    /// must swap it in. A delete always did: it takes a `_seq_no`, whether or not the document
    /// existed. The match names every outcome, so a new one cannot skip the swap by accident.
    pub(crate) fn changes_snapshot(&self) -> bool {
        match self {
            Self::IndexCreated { .. } | Self::AliasesApplied | Self::DocumentDeleted { .. } => true,
            Self::DocumentWritten(written_document) => written_document.result != WriteResult::Noop,
            Self::Bulk { items } => items.iter().flatten().any(Self::changes_snapshot),
        }
    }
}

impl SystemSnapshot {
    /// Apply `operation`, or refuse it and leave the snapshot unchanged.
    ///
    /// # Errors
    ///
    /// Returns `resource_already_exists_exception` when creating an index whose name an index or
    /// an alias carries. For alias actions: `index_not_found_exception` for an action naming an
    /// index that does not exist (or that an earlier action of the request removed), and
    /// `illegal_argument_exception` for an alias name no system index may carry, an alias named
    /// like an index, an alias that already names another index, or a removal of an alias that
    /// does not name the given index. For document operations: `index_not_found_exception` when the
    /// request's index or alias names no system index (a write creates none);
    /// `document_missing_exception` for an update of a missing document without `upsert`, which is
    /// checked before any condition; `version_conflict_engine_exception` for a create of an existing
    /// id or any other condition that does not hold, a condition on a missing document included;
    /// and `illegal_argument_exception` for a script that cannot run on the stored document. A
    /// `_bulk` is never refused as a whole: each item's refusal is among its outcomes, and an update
    /// item past [`ReturnedSources::max_answer_bytes`] is one of those refusals.
    pub(crate) fn apply(&mut self, operation: &SystemOperation) -> Result<SystemOutcome, OpenSearchError> {
        match operation {
            SystemOperation::CreateIndex {
                name,
                mappings,
                settings,
            } => self.create_index(name, mappings, settings),
            SystemOperation::ApplyAliasActions { actions } => self.apply_alias_actions(actions),
            SystemOperation::IndexDocument(write) => self.write_document(write),
            SystemOperation::UpdateDocument(update) => {
                self.update_document(update, Some(&[])).map(SystemOutcome::DocumentWritten)
            }
            SystemOperation::DeleteDocument(delete) => self.delete_document(delete),
            SystemOperation::Bulk {
                items,
                returned_sources,
            } => {
                let mut kept_source_bytes = 0;
                Ok(SystemOutcome::Bulk {
                    items: items
                        .iter()
                        .map(|item| self.apply_bulk_item(item, returned_sources.as_ref(), &mut kept_source_bytes))
                        .collect(),
                })
            }
        }
    }

    /// The system index named `name`, if it exists. An alias name finds nothing.
    pub(crate) fn find_index(&self, name: &str) -> Option<&SystemIndex> {
        self.indices.get(name)
    }

    /// The concrete index `name` addresses: an index name addresses itself, an alias its single
    /// index.
    pub(crate) fn resolve_name(&self, name: &str) -> Option<&str> {
        if let Some((index, _)) = self.indices.get_key_value(name) {
            return Some(index.as_str());
        }
        self.aliases.get(name).map(String::as_str)
    }

    /// Every index name, in name order.
    pub(crate) fn index_names(&self) -> impl Iterator<Item = &str> {
        self.indices.keys().map(String::as_str)
    }

    /// Every alias name, in name order.
    pub(crate) fn alias_names(&self) -> impl Iterator<Item = &str> {
        self.aliases.keys().map(String::as_str)
    }

    /// The aliases naming `index`, in name order.
    pub(crate) fn list_aliases_of<'a>(&'a self, index: &'a str) -> impl Iterator<Item = &'a str> {
        self.aliases
            .iter()
            .filter(move |(_, aliased_index)| aliased_index.as_str() == index)
            .map(|(alias, _)| alias.as_str())
    }

    /// Create `name` with `mappings` and `settings`, refused when an index or an alias carries the
    /// name.
    fn create_index(
        &mut self,
        name: &str,
        mappings: &Value,
        settings: &Value,
    ) -> Result<SystemOutcome, OpenSearchError> {
        if self.indices.contains_key(name) || self.aliases.contains_key(name) {
            return Err(OpenSearchError::resource_already_exists(name));
        }
        self.indices
            .insert(name.to_string(), SystemIndex::new(mappings.clone(), settings.clone()));
        Ok(SystemOutcome::IndexCreated { name: name.to_string() })
    }

    /// Apply `actions` in order against a working copy of the alias map and of the removed
    /// indices; commit only when every action applied. The indices themselves are never copied.
    fn apply_alias_actions(&mut self, actions: &[AliasAction]) -> Result<SystemOutcome, OpenSearchError> {
        let mut aliases = self.aliases.clone();
        let mut removed_indices: BTreeSet<&str> = BTreeSet::new();
        let has_index = |name: &str, removed_indices: &BTreeSet<&str>| {
            self.indices.contains_key(name) && !removed_indices.contains(name)
        };
        for action in actions {
            match action {
                AliasAction::Add { index, alias } => {
                    if !has_index(index, &removed_indices) {
                        return Err(OpenSearchError::index_not_found(index));
                    }
                    validate_system_index_name(alias)?;
                    if has_index(alias, &removed_indices) {
                        return Err(OpenSearchError::illegal_argument(format!(
                            "an index exists with the same name as the alias [{alias}]"
                        )));
                    }
                    if let Some(aliased_index) = aliases.get(alias)
                        && aliased_index != index
                    {
                        return Err(OpenSearchError::illegal_argument(format!(
                            "alias [{alias}] already names index [{aliased_index}]; an alias names one index"
                        )));
                    }
                    aliases.insert(alias.clone(), index.clone());
                }
                AliasAction::Remove { index, alias } => {
                    if !has_index(index, &removed_indices) {
                        return Err(OpenSearchError::index_not_found(index));
                    }
                    if aliases.get(alias) != Some(index) {
                        return Err(OpenSearchError::illegal_argument(format!(
                            "alias [{alias}] is not set on index [{index}]"
                        )));
                    }
                    aliases.remove(alias);
                }
                AliasAction::RemoveIndex { index } => {
                    if !has_index(index, &removed_indices) {
                        return Err(OpenSearchError::index_not_found(index));
                    }
                    removed_indices.insert(index.as_str());
                    aliases.retain(|_, aliased_index| aliased_index != index);
                }
            }
        }
        for index in removed_indices {
            self.indices.remove(index);
        }
        self.aliases = aliases;
        Ok(SystemOutcome::AliasesApplied)
    }

    /// The concrete index `name` (an index or an alias of one index) resolves to, for a write.
    fn find_index_for_write(&mut self, name: &str) -> Result<(String, &mut SystemIndex), OpenSearchError> {
        let concrete_index = self
            .resolve_name(name)
            .ok_or_else(|| OpenSearchError::index_not_found(name))?
            .to_string();
        let index = self
            .indices
            .get_mut(&concrete_index)
            .ok_or_else(|| OpenSearchError::index_not_found(name))?;
        Ok((concrete_index, index))
    }

    /// Create or replace a document; the refusals precede every mutation.
    fn write_document(&mut self, write: &DocumentWrite) -> Result<SystemOutcome, OpenSearchError> {
        let (index_name, index) = self.find_index_for_write(&write.index)?;
        let stored_document = index.find_document(&write.id);
        if write.op_type == OpType::Create && stored_document.is_some() {
            return Err(OpenSearchError::version_conflict(&index_name, &write.id));
        }
        write
            .concurrency
            .check_stored_document(&index_name, &write.id, stored_document)?;
        let result = if stored_document.is_some() {
            WriteResult::Updated
        } else {
            WriteResult::Created
        };
        let seq_no = index.store_document(&write.id, write.source.clone());
        Ok(SystemOutcome::DocumentWritten(WrittenDocument {
            index: index_name,
            result,
            seq_no,
            primary_term: PRIMARY_TERM,
            source: None,
        }))
    }

    /// Change a document, or store the `upsert` of a missing one. A refused change leaves the
    /// stored document as it was, and a merge that changes nothing takes no `_seq_no`. The answer
    /// keeps what [`keep_source`] selects by `includes` of the `_source` after the update.
    fn update_document(
        &mut self,
        update: &DocumentUpdate,
        includes: Option<&[String]>,
    ) -> Result<WrittenDocument, OpenSearchError> {
        let (index_name, index) = self.find_index_for_write(&update.index)?;
        let Some(stored_document) = index.documents.get_mut(&update.id) else {
            let Some(upsert) = &update.upsert else {
                return Err(OpenSearchError::document_missing(&index_name, &update.id));
            };
            update.concurrency.check_stored_document(&index_name, &update.id, None)?;
            let seq_no = index.store_document(&update.id, upsert.clone());
            return Ok(WrittenDocument {
                index: index_name,
                result: WriteResult::Created,
                seq_no,
                primary_term: PRIMARY_TERM,
                source: keep_source(Cow::Borrowed(upsert), includes),
            });
        };
        update
            .concurrency
            .check_stored_document(&index_name, &update.id, Some(&*stored_document))?;
        match &update.change {
            DocumentChange::Merge(partial_document) => {
                // The copy is what detects a noop: the merge cannot say whether it changed anything.
                let mut merged_source = stored_document.source.clone();
                merge_partial_document(&mut merged_source, partial_document);
                if merged_source == stored_document.source {
                    return Ok(WrittenDocument {
                        index: index_name,
                        result: WriteResult::Noop,
                        seq_no: stored_document.seq_no,
                        primary_term: PRIMARY_TERM,
                        source: keep_source(Cow::Owned(merged_source), includes),
                    });
                }
                stored_document.source = merged_source;
            }
            DocumentChange::Script(DocumentScript::IncrementCounter(increment)) => {
                increment.apply_to(&mut stored_document.source)?;
            }
        }
        stored_document.seq_no = SystemIndex::take_seq_no(&mut index.next_seq_no);
        Ok(WrittenDocument {
            index: index_name,
            result: WriteResult::Updated,
            seq_no: stored_document.seq_no,
            primary_term: PRIMARY_TERM,
            source: keep_source(Cow::Borrowed(&stored_document.source), includes),
        })
    }

    /// Delete a document. A delete of a missing id takes a `_seq_no` too, so a sequence number is
    /// never reused whichever way the delete went.
    fn delete_document(&mut self, delete: &DocumentDelete) -> Result<SystemOutcome, OpenSearchError> {
        let (index_name, index) = self.find_index_for_write(&delete.index)?;
        delete
            .concurrency
            .check_stored_document(&index_name, &delete.id, index.find_document(&delete.id))?;
        let is_found = index.documents.remove(&delete.id).is_some();
        let seq_no = SystemIndex::take_seq_no(&mut index.next_seq_no);
        Ok(SystemOutcome::DocumentDeleted {
            index: index_name,
            found: is_found,
            seq_no,
        })
    }

    /// Apply one `_bulk` item. Every arm refuses before it mutates, so a failing item leaves the
    /// snapshot as the earlier items left it. `kept_source_bytes` carries the running total of
    /// [`update_bulk_document`](Self::update_bulk_document) from item to item.
    fn apply_bulk_item(
        &mut self,
        item: &BulkItem,
        returned_sources: Option<&ReturnedSources>,
        kept_source_bytes: &mut usize,
    ) -> Result<SystemOutcome, OpenSearchError> {
        match item {
            BulkItem::Write(write) => self.write_document(write),
            BulkItem::Update(update) => self
                .update_bulk_document(update, returned_sources, kept_source_bytes)
                .map(SystemOutcome::DocumentWritten),
            BulkItem::Delete(delete) => self.delete_document(delete),
            BulkItem::Refused(refused_item) => Err(refused_item.error.clone()),
        }
    }

    /// Apply one `_bulk` update item, keeping what its `get` renders. An item keeps nothing unless
    /// the request asked for `get`, and only an item that keeps a source pays to measure it.
    ///
    /// # Errors
    ///
    /// `answer_too_large` once the sources kept so far have reached the bound, before the update
    /// mutates anything; otherwise the refusals of [`update_document`](Self::update_document).
    fn update_bulk_document(
        &mut self,
        update: &DocumentUpdate,
        returned_sources: Option<&ReturnedSources>,
        kept_source_bytes: &mut usize,
    ) -> Result<WrittenDocument, OpenSearchError> {
        let Some(returned_sources) = returned_sources else {
            return self.update_document(update, None);
        };
        if *kept_source_bytes >= returned_sources.max_answer_bytes {
            return Err(OpenSearchError::answer_too_large(returned_sources.max_answer_bytes));
        }
        let written_document = self.update_document(update, Some(&returned_sources.includes))?;
        let written_source_bytes = written_document.source.as_ref().map_or(0, count_serialized_bytes);
        *kept_source_bytes = kept_source_bytes.saturating_add(written_source_bytes);
        Ok(written_document)
    }
}

/// What an update keeps of `source`, the `_source` it wrote, for its `get`: nothing for `None`, all
/// of it for empty `includes`, otherwise the selected paths. An owned `source` moves into the answer
/// when all of it is kept, so a source the caller was about to drop is not copied.
fn keep_source(source: Cow<'_, Value>, includes: Option<&[String]>) -> Option<Value> {
    let includes = includes?;
    if includes.is_empty() {
        Some(source.into_owned())
    } else {
        Some(filter_source(&source, includes, &[]))
    }
}

/// The length `source` serializes to, the way the answer will write it. Serializing a `Value` into a
/// writer that never fails cannot fail; should it, the source counts as past every bound, so the
/// items after it are refused rather than kept uncounted.
fn count_serialized_bytes(source: &Value) -> usize {
    let mut byte_counter = ByteCounter::default();
    serde_json::to_writer(&mut byte_counter, source).map_or(usize::MAX, |()| byte_counter.byte_count)
}

/// A writer that counts what it is given and keeps none of it, so measuring a source allocates
/// nothing.
#[derive(Default)]
struct ByteCounter {
    /// The bytes written so far.
    byte_count: usize,
}

impl io::Write for ByteCounter {
    fn write(&mut self, chunk: &[u8]) -> io::Result<usize> {
        self.byte_count += chunk.len();
        Ok(chunk.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Merge `partial_document` into `target` the way a partial-document update does: objects merge
/// key by key, recursively; every other value, arrays included, replaces the target's value.
fn merge_partial_document(target: &mut Value, partial_document: &Value) {
    if let (Value::Object(target_object), Value::Object(partial_object)) = (&mut *target, partial_document) {
        for (key, partial_value) in partial_object {
            match target_object.get_mut(key) {
                Some(target_value) if target_value.is_object() && partial_value.is_object() => {
                    merge_partial_document(target_value, partial_value);
                }
                Some(_) | None => {
                    target_object.insert(key.clone(), partial_value.clone());
                }
            }
        }
        return;
    }
    *target = partial_document.clone();
}

/// One action of an `_aliases` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AliasAction {
    /// Make `alias` name `index`.
    Add {
        /// An existing system index.
        index: String,
        /// A name [`validate_system_index_name`] accepts and no index carries.
        alias: String,
    },
    /// Remove `alias`, which must name `index`.
    Remove {
        /// The index the alias names.
        index: String,
        /// The alias to remove.
        alias: String,
    },
    /// Delete `index` and every alias naming it.
    RemoveIndex {
        /// An existing system index.
        index: String,
    },
}

/// One field a stored mapping declares, as `_field_caps` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MappedField {
    /// The dotted path of the field: object and nested properties and multi-fields joined with `.`.
    pub(crate) name: String,
    /// The mapping type; `object` for a property without one.
    pub(crate) type_name: String,
    /// Whether a query can match on the field.
    pub(crate) is_searchable: bool,
    /// Whether an aggregation can bucket or measure the field.
    pub(crate) is_aggregatable: bool,
}

/// Refuse a name no system index may carry: it must start with `.`, must not be `.` or `..`, must
/// be lower case, hold none of the characters an index name may not hold, and fit the length
/// bound.
///
/// # Errors
///
/// Returns `illegal_argument_exception` naming `name`.
pub(crate) fn validate_system_index_name(name: &str) -> Result<(), OpenSearchError> {
    let is_valid = name.starts_with('.')
        && name != "."
        && name != ".."
        && name.len() <= MAX_INDEX_NAME_BYTES
        && !name
            .chars()
            .any(|character| character.is_uppercase() || INVALID_INDEX_NAME_CHARACTERS.contains(&character));
    if is_valid {
        Ok(())
    } else {
        Err(OpenSearchError::illegal_argument(format!(
            "invalid system index name [{name}]: it must start with '.', be lower case, be at most \
             {MAX_INDEX_NAME_BYTES} bytes, not be '.' or '..', and contain none of {INVALID_INDEX_NAME_CHARACTERS:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::*;
    use crate::opensearch::error::render_envelope;
    use crate::opensearch::script::CounterIncrement;

    fn build_index_creation(name: &str, mappings: Value, settings: Value) -> SystemOperation {
        SystemOperation::CreateIndex {
            name: name.to_string(),
            mappings,
            settings,
        }
    }

    #[tokio::test]
    async fn creating_an_existing_index_is_resource_already_exists() {
        let mut snapshot = SystemSnapshot::default();
        snapshot
            .apply(&build_index_creation(
                ".kibana_1",
                json!({"dynamic": "strict"}),
                json!({}),
            ))
            .expect("the first create");
        let snapshot_before_refusal = snapshot.clone();

        let error = snapshot
            .apply(&build_index_creation(".kibana_1", json!({"dynamic": false}), json!({})))
            .expect_err("the second create is refused");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "resource_already_exists_exception");
        assert_eq!(body["error"]["index"], ".kibana_1");
        assert_eq!(snapshot, snapshot_before_refusal, "a refused operation changes nothing");
    }

    #[test]
    fn documents_iterate_in_id_order_whatever_the_write_order() {
        let mut index = SystemIndex::new(json!({}), json!({}));
        index.store_document("config:3.9.0", json!({"written": 1}));
        index.store_document("dashboard:b", json!({"written": 2}));
        index.store_document("dashboard:a", json!({"written": 3}));

        let ids: Vec<&str> = index.documents().map(|(id, _)| id).collect();

        assert_eq!(ids, ["config:3.9.0", "dashboard:a", "dashboard:b"]);
    }

    /// Stored mappings are compared byte for byte (`serde_json` keeps key order in this crate):
    /// the store returns them exactly as they were created, key order included.
    #[test]
    fn mappings_and_settings_are_returned_exactly_as_stored() {
        let mappings_text = r#"{"dynamic":"strict","properties":{"type":{"type":"keyword"},"migrationVersion":{"dynamic":"true","type":"object"},"references":{"type":"nested","properties":{"id":{"type":"keyword"}}}},"_meta":{"migrationMappingPropertyHashes":{"type":"2f4316de49999235636386fe51dc06c1","references":"7997cf5a56cc02bdc9c93361bde732b0"}},"unknown_key":[1,"two",null]}"#;
        let settings_text = r#"{"number_of_shards":1,"auto_expand_replicas":"0-1"}"#;
        let mut snapshot = SystemSnapshot::default();

        let outcome = snapshot
            .apply(&build_index_creation(
                ".kibana_1",
                serde_json::from_str(mappings_text).expect("mappings JSON"),
                serde_json::from_str(settings_text).expect("settings JSON"),
            ))
            .expect("create");

        assert_eq!(
            outcome,
            SystemOutcome::IndexCreated {
                name: ".kibana_1".to_string()
            }
        );
        let index = snapshot.find_index(".kibana_1").expect("the created index");
        assert_eq!(serde_json::to_string(&index.mappings).expect("mappings"), mappings_text);
        assert_eq!(serde_json::to_string(&index.settings).expect("settings"), settings_text);
        assert!(snapshot.find_index(".kibana").is_none());
    }

    #[tokio::test]
    async fn an_invalid_system_index_name_is_refused() {
        let too_long_name = format!(".{}", "a".repeat(255));
        for name in [
            "kibana",
            ".",
            "..",
            ".Kibana",
            ".kib*",
            ".a,b",
            ".a b",
            ".a:b",
            ".a/b",
            ".a#b",
            too_long_name.as_str(),
        ] {
            let error = validate_system_index_name(name).expect_err(name);

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{name}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{name}");
        }
        let longest_name = format!(".{}", "a".repeat(254));
        for name in [
            ".kibana",
            ".kibana_1",
            ".opensearch_dashboards_config",
            longest_name.as_str(),
        ] {
            assert!(validate_system_index_name(name).is_ok(), "{name}");
        }
    }

    fn build_two_indices() -> SystemSnapshot {
        let mut snapshot = SystemSnapshot::default();
        for name in [".kibana_1", ".kibana_2"] {
            snapshot
                .apply(&build_index_creation(name, json!({"dynamic": "strict"}), json!({})))
                .expect("create");
        }
        snapshot
    }

    fn apply_actions(
        snapshot: &mut SystemSnapshot,
        actions: Vec<AliasAction>,
    ) -> Result<SystemOutcome, OpenSearchError> {
        snapshot.apply(&SystemOperation::ApplyAliasActions { actions })
    }

    fn build_add_action(index: &str, alias: &str) -> AliasAction {
        AliasAction::Add {
            index: index.to_string(),
            alias: alias.to_string(),
        }
    }

    fn build_remove_action(index: &str, alias: &str) -> AliasAction {
        AliasAction::Remove {
            index: index.to_string(),
            alias: alias.to_string(),
        }
    }

    fn build_remove_index_action(index: &str) -> AliasAction {
        AliasAction::RemoveIndex {
            index: index.to_string(),
        }
    }

    /// A swap is one request of a removal and an addition: both apply, or neither does.
    #[tokio::test]
    async fn alias_actions_apply_atomically_or_not_at_all() {
        let mut snapshot = build_two_indices();
        apply_actions(&mut snapshot, vec![build_add_action(".kibana_1", ".kibana")]).expect("add");

        let swap_outcome = apply_actions(
            &mut snapshot,
            vec![
                build_remove_action(".kibana_1", ".kibana"),
                build_add_action(".kibana_2", ".kibana"),
            ],
        );

        assert_eq!(swap_outcome.expect("swap"), SystemOutcome::AliasesApplied);
        assert_eq!(snapshot.resolve_name(".kibana"), Some(".kibana_2"));
        let snapshot_before_refusal = snapshot.clone();
        let refused_outcome = apply_actions(
            &mut snapshot,
            vec![
                build_add_action(".kibana_2", ".kibana_extra"),
                build_add_action(".missing", ".other"),
            ],
        );
        let (status, body) =
            render_envelope(refused_outcome.expect_err("the second action names a missing index")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["index"], ".missing");
        assert_eq!(
            snapshot, snapshot_before_refusal,
            "the first action of a refused request is not applied"
        );
        assert_eq!(snapshot.resolve_name(".kibana_extra"), None);
        let unset_alias_outcome = apply_actions(&mut snapshot, vec![build_remove_action(".kibana_1", ".kibana")]);
        let (status, _) = render_envelope(unset_alias_outcome.expect_err("the alias names another index")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn remove_index_drops_the_index_and_its_aliases() {
        let mut snapshot = build_two_indices();
        apply_actions(
            &mut snapshot,
            vec![
                build_add_action(".kibana_1", ".kibana"),
                build_add_action(".kibana_1", ".kibana_old"),
                build_add_action(".kibana_2", ".kibana_new"),
            ],
        )
        .expect("add");

        apply_actions(&mut snapshot, vec![build_remove_index_action(".kibana_1")]).expect("remove_index");

        assert!(snapshot.find_index(".kibana_1").is_none());
        assert_eq!(snapshot.index_names().collect::<Vec<_>>(), [".kibana_2"]);
        assert_eq!(snapshot.alias_names().collect::<Vec<_>>(), [".kibana_new"]);
    }

    /// An alias names exactly one index, and a name is an index or an alias, never both. Removing
    /// an index frees its name for an alias in the same request.
    #[tokio::test]
    async fn an_alias_resolves_to_its_single_concrete_index() {
        let mut snapshot = build_two_indices();
        apply_actions(&mut snapshot, vec![build_add_action(".kibana_1", ".kibana")]).expect("add");

        assert_eq!(snapshot.resolve_name(".kibana"), Some(".kibana_1"));
        assert_eq!(snapshot.resolve_name(".kibana_1"), Some(".kibana_1"));
        assert_eq!(snapshot.resolve_name(".missing"), None);
        assert_eq!(snapshot.list_aliases_of(".kibana_1").collect::<Vec<_>>(), [".kibana"]);
        let second_index_outcome = apply_actions(&mut snapshot, vec![build_add_action(".kibana_2", ".kibana")]);
        let (status, _) = render_envelope(second_index_outcome.expect_err("one index per alias")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let alias_over_index_outcome = apply_actions(&mut snapshot, vec![build_add_action(".kibana_1", ".kibana_2")]);
        let (status, _) = render_envelope(alias_over_index_outcome.expect_err("an index has the name")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let index_over_alias_outcome = snapshot.apply(&build_index_creation(".kibana", json!({}), json!({})));
        let (status, body) = render_envelope(index_over_alias_outcome.expect_err("an alias has the name")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "resource_already_exists_exception");
        apply_actions(
            &mut snapshot,
            vec![
                build_remove_index_action(".kibana_2"),
                build_add_action(".kibana_1", ".kibana_2"),
            ],
        )
        .expect("a removed index's name is free in the same request");
        assert_eq!(snapshot.resolve_name(".kibana_2"), Some(".kibana_1"));
    }

    /// Every action that names a missing index is the missing-index `404`, a removed index
    /// included, and an alias name no system index may carry is refused; a refused request leaves
    /// the snapshot unchanged.
    #[tokio::test]
    async fn an_action_naming_a_missing_index_or_an_invalid_alias_name_is_refused() {
        let mut snapshot = build_two_indices();
        apply_actions(&mut snapshot, vec![build_add_action(".kibana_1", ".kibana")]).expect("add");
        let snapshot_before_refusals = snapshot.clone();
        let missing_index_cases = [
            vec![build_remove_action(".missing", ".kibana")],
            vec![build_remove_index_action(".missing")],
            vec![
                build_remove_index_action(".kibana_2"),
                build_remove_index_action(".kibana_2"),
            ],
            vec![
                build_remove_index_action(".kibana_2"),
                build_add_action(".kibana_2", ".kibana_new"),
            ],
        ];

        for actions in missing_index_cases {
            let refusal = apply_actions(&mut snapshot, actions.clone()).expect_err("a missing index");

            let (status, body) = render_envelope(refusal).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{actions:?}");
            assert_eq!(body["error"]["type"], "index_not_found_exception", "{actions:?}");
        }
        for alias in ["kibana", ".Kibana", ".a,b"] {
            let refusal = apply_actions(&mut snapshot, vec![build_add_action(".kibana_1", alias)])
                .expect_err("an invalid alias name");

            let (status, body) = render_envelope(refusal).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{alias}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{alias}");
        }
        assert_eq!(snapshot, snapshot_before_refusals);
    }

    #[test]
    fn stored_mappings_list_their_fields_with_capabilities() {
        let mappings = json!({
            "dynamic": "strict",
            "properties": {
                "type": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "references": {"type": "nested", "properties": {"id": {"type": "keyword"}}},
                "migrationVersion": {"dynamic": "true", "type": "object"},
                "config": {"dynamic": false, "properties": {}}
            }
        });
        let mut snapshot = SystemSnapshot::default();
        snapshot
            .apply(&build_index_creation(".kibana_1", mappings, json!({})))
            .expect("create");

        let fields = snapshot.find_index(".kibana_1").expect("index").list_mapped_fields();

        let build_field = |name: &str, type_name: &str, is_searchable: bool, is_aggregatable: bool| MappedField {
            name: name.to_string(),
            type_name: type_name.to_string(),
            is_searchable,
            is_aggregatable,
        };
        assert_eq!(
            fields,
            [
                build_field("type", "keyword", true, true),
                build_field("title", "text", true, false),
                build_field("title.keyword", "keyword", true, true),
                build_field("references", "nested", false, false),
                build_field("references.id", "keyword", true, true),
                build_field("migrationVersion", "object", false, false),
                build_field("config", "object", false, false),
            ]
        );
    }

    fn build_snapshot_with_index(name: &str) -> SystemSnapshot {
        let mut snapshot = SystemSnapshot::default();
        snapshot
            .apply(&build_index_creation(name, json!({"properties": {}}), json!({})))
            .expect("the index is created");
        snapshot
    }

    fn build_put_operation(
        index: &str,
        id: &str,
        source: Value,
        op_type: OpType,
        concurrency: WriteConcurrency,
    ) -> SystemOperation {
        SystemOperation::IndexDocument(DocumentWrite {
            index: index.to_string(),
            id: id.to_string(),
            source,
            op_type,
            concurrency,
        })
    }

    fn build_update_operation(index: &str, id: &str, change: DocumentChange, upsert: Option<Value>) -> SystemOperation {
        SystemOperation::UpdateDocument(DocumentUpdate {
            index: index.to_string(),
            id: id.to_string(),
            change,
            upsert,
            concurrency: WriteConcurrency::default(),
        })
    }

    fn build_delete_operation(index: &str, id: &str) -> SystemOperation {
        SystemOperation::DeleteDocument(DocumentDelete {
            index: index.to_string(),
            id: id.to_string(),
            concurrency: WriteConcurrency::default(),
        })
    }

    fn expect_written_document(outcome: SystemOutcome) -> WrittenDocument {
        let SystemOutcome::DocumentWritten(written_document) = outcome else {
            panic!("expected a written document, got {outcome:?}");
        };
        written_document
    }

    fn find_stored_document(snapshot: &SystemSnapshot, index: &str, id: &str) -> Option<StoredDocument> {
        snapshot.find_index(index).and_then(|index| index.find_document(id)).cloned()
    }

    #[tokio::test]
    async fn put_with_a_stale_if_seq_no_is_a_version_conflict() {
        let mut snapshot = build_snapshot_with_index(".kibana_1");
        snapshot
            .apply(&build_put_operation(
                ".kibana_1",
                "a",
                json!({"title": "first"}),
                OpType::Index,
                WriteConcurrency::default(),
            ))
            .expect("the first write lands");

        for (if_seq_no, if_primary_term) in [(7, 1), (0, 2)] {
            let stale_condition =
                WriteConcurrency::new(Some(if_seq_no), Some(if_primary_term)).expect("both values given");
            let error = snapshot
                .apply(&build_put_operation(
                    ".kibana_1",
                    "a",
                    json!({"title": "second"}),
                    OpType::Index,
                    stale_condition,
                ))
                .expect_err("a stale condition conflicts");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::CONFLICT);
            assert_eq!(body["error"]["type"], "version_conflict_engine_exception");
            assert_eq!(body["error"]["index"], ".kibana_1");
        }
        assert_eq!(
            find_stored_document(&snapshot, ".kibana_1", "a"),
            Some(StoredDocument {
                source: json!({"title": "first"}),
                seq_no: 0
            })
        );

        let matching_condition = WriteConcurrency::new(Some(0), Some(1)).expect("both values given");
        let next_document = expect_written_document(
            snapshot
                .apply(&build_put_operation(
                    ".kibana_1",
                    "a",
                    json!({"title": "second"}),
                    OpType::Index,
                    matching_condition,
                ))
                .expect("the matching condition writes"),
        );
        assert_eq!(
            (next_document.result, next_document.seq_no, next_document.primary_term),
            (WriteResult::Updated, 1, 1)
        );
    }

    #[tokio::test]
    async fn a_condition_with_only_one_of_seq_no_and_primary_term_is_refused() {
        for (if_seq_no, if_primary_term) in [(Some(0), None), (None, Some(1))] {
            let error = WriteConcurrency::new(if_seq_no, if_primary_term).expect_err("a one-sided condition");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["error"]["type"], "illegal_argument_exception");
        }
    }

    #[tokio::test]
    async fn create_of_an_existing_id_is_a_version_conflict() {
        let mut snapshot = build_snapshot_with_index(".kibana_1");
        let created_document = expect_written_document(
            snapshot
                .apply(&build_put_operation(
                    ".kibana_1",
                    "a",
                    json!({"title": "first"}),
                    OpType::Create,
                    WriteConcurrency::default(),
                ))
                .expect("a create of a new id lands"),
        );
        assert_eq!(
            (created_document.result, created_document.seq_no),
            (WriteResult::Created, 0)
        );

        let error = snapshot
            .apply(&build_put_operation(
                ".kibana_1",
                "a",
                json!({"title": "second"}),
                OpType::Create,
                WriteConcurrency::default(),
            ))
            .expect_err("a create of an existing id conflicts");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"]["type"], "version_conflict_engine_exception");
        assert_eq!(
            find_stored_document(&snapshot, ".kibana_1", "a"),
            Some(StoredDocument {
                source: json!({"title": "first"}),
                seq_no: 0
            })
        );
    }

    /// A refused write takes no `_seq_no`, so the write after three refusals still takes the
    /// first one.
    #[tokio::test]
    async fn a_condition_on_a_missing_document_is_a_version_conflict_and_takes_no_seq_no() {
        let mut snapshot = build_snapshot_with_index(".kibana_1");
        let condition = WriteConcurrency::new(Some(0), Some(1)).expect("both values given");
        let conditioned_operations = [
            build_put_operation(".kibana_1", "missing", json!({"title": "a"}), OpType::Index, condition),
            SystemOperation::UpdateDocument(DocumentUpdate {
                index: ".kibana_1".to_string(),
                id: "missing".to_string(),
                change: DocumentChange::Merge(json!({"title": "b"})),
                upsert: Some(json!({"title": "c"})),
                concurrency: condition,
            }),
            SystemOperation::DeleteDocument(DocumentDelete {
                index: ".kibana_1".to_string(),
                id: "missing".to_string(),
                concurrency: condition,
            }),
        ];
        let snapshot_before_refusals = snapshot.clone();

        for operation in &conditioned_operations {
            let error = snapshot.apply(operation).expect_err("no document satisfies the condition");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::CONFLICT, "{operation:?}");
            assert_eq!(
                body["error"]["type"], "version_conflict_engine_exception",
                "{operation:?}"
            );
        }

        assert_eq!(
            snapshot, snapshot_before_refusals,
            "a refused operation changes nothing"
        );
        let next_document = expect_written_document(
            snapshot
                .apply(&build_put_operation(
                    ".kibana_1",
                    "a",
                    json!({}),
                    OpType::Index,
                    WriteConcurrency::default(),
                ))
                .expect("write"),
        );
        assert_eq!(next_document.seq_no, 0);
    }

    #[tokio::test]
    async fn a_write_an_update_and_a_delete_naming_a_missing_index_are_index_not_found() {
        let mut snapshot = build_snapshot_with_index(".kibana_1");
        let snapshot_before_refusals = snapshot.clone();
        let operations = [
            build_put_operation(".missing", "a", json!({}), OpType::Index, WriteConcurrency::default()),
            build_update_operation(".missing", "a", DocumentChange::Merge(json!({})), Some(json!({}))),
            build_delete_operation(".missing", "a"),
        ];

        for operation in &operations {
            let error = snapshot.apply(operation).expect_err("no automatic index creation");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{operation:?}");
            assert_eq!(body["error"]["type"], "index_not_found_exception", "{operation:?}");
        }

        assert_eq!(
            snapshot, snapshot_before_refusals,
            "a refused operation changes nothing"
        );
    }

    #[test]
    fn seq_no_is_a_per_index_monotonic_counter_and_primary_term_is_one() {
        let mut snapshot = build_snapshot_with_index(".kibana_1");
        snapshot
            .apply(&build_index_creation(".kibana_2", json!({"properties": {}}), json!({})))
            .expect("the second index is created");
        let no_condition = WriteConcurrency::default();
        let mut write_document = |index: &str, id: &str| {
            expect_written_document(
                snapshot
                    .apply(&build_put_operation(index, id, json!({}), OpType::Index, no_condition))
                    .expect("write"),
            )
        };

        let first_document = write_document(".kibana_1", "a");
        let other_index_document = write_document(".kibana_2", "x");
        let second_document = write_document(".kibana_1", "b");
        let delete_outcome = snapshot.apply(&build_delete_operation(".kibana_1", "a")).expect("delete");
        let missing_delete_outcome = snapshot
            .apply(&build_delete_operation(".kibana_1", "missing"))
            .expect("delete of a missing id");
        let document_after_deletes = expect_written_document(
            snapshot
                .apply(&build_put_operation(
                    ".kibana_1",
                    "c",
                    json!({}),
                    OpType::Index,
                    no_condition,
                ))
                .expect("write"),
        );

        assert_eq!((first_document.seq_no, first_document.primary_term), (0, 1));
        assert_eq!((other_index_document.seq_no, other_index_document.primary_term), (0, 1));
        assert_eq!((second_document.seq_no, second_document.primary_term), (1, 1));
        assert_eq!(
            delete_outcome,
            SystemOutcome::DocumentDeleted {
                index: ".kibana_1".to_string(),
                found: true,
                seq_no: 2
            }
        );
        assert_eq!(
            missing_delete_outcome,
            SystemOutcome::DocumentDeleted {
                index: ".kibana_1".to_string(),
                found: false,
                seq_no: 3
            }
        );
        assert_eq!(
            (document_after_deletes.seq_no, document_after_deletes.primary_term),
            (4, 1)
        );
    }

    #[test]
    fn update_merges_a_partial_doc_and_upserts_a_missing_document() {
        let mut snapshot = build_snapshot_with_index(".kibana_1");
        snapshot
            .apply(&build_put_operation(
                ".kibana_1",
                "dashboard:a",
                json!({"type": "dashboard", "dashboard": {"title": "Logs", "panels": [1, 2]}}),
                OpType::Index,
                WriteConcurrency::default(),
            ))
            .expect("write");
        let merge =
            DocumentChange::Merge(json!({"dashboard": {"panels": [3]}, "updated_at": "2026-10-08T12:00:00.000Z"}));

        let updated_document = expect_written_document(
            snapshot
                .apply(&build_update_operation(".kibana_1", "dashboard:a", merge.clone(), None))
                .expect("update"),
        );
        let repeated_document = expect_written_document(
            snapshot
                .apply(&build_update_operation(".kibana_1", "dashboard:a", merge, None))
                .expect("update"),
        );
        let upserted_document = expect_written_document(
            snapshot
                .apply(&build_update_operation(
                    ".kibana_1",
                    "dashboard:b",
                    DocumentChange::Merge(json!({"dashboard": {"title": "ignored"}})),
                    Some(json!({"type": "dashboard", "dashboard": {"title": "Fresh"}})),
                ))
                .expect("upsert"),
        );

        let merged_source = json!({
            "type": "dashboard",
            "dashboard": {"title": "Logs", "panels": [3]},
            "updated_at": "2026-10-08T12:00:00.000Z"
        });
        assert_eq!(
            updated_document,
            WrittenDocument {
                index: ".kibana_1".to_string(),
                result: WriteResult::Updated,
                seq_no: 1,
                primary_term: 1,
                source: Some(merged_source.clone())
            }
        );
        assert_eq!(
            repeated_document,
            WrittenDocument {
                index: ".kibana_1".to_string(),
                result: WriteResult::Noop,
                seq_no: 1,
                primary_term: 1,
                source: Some(merged_source)
            }
        );
        assert_eq!(
            upserted_document,
            WrittenDocument {
                index: ".kibana_1".to_string(),
                result: WriteResult::Created,
                seq_no: 2,
                primary_term: 1,
                source: Some(json!({"type": "dashboard", "dashboard": {"title": "Fresh"}}))
            }
        );
    }

    /// One row per pair of JSON kinds a merge step can meet: each step branches only on them.
    #[test]
    fn a_partial_document_merges_objects_and_replaces_every_other_value() {
        let cases = [
            (
                "object into object, recursively",
                json!({"a": {"b": 1, "c": {"d": 2, "e": 3}}}),
                json!({"a": {"c": {"e": 4, "f": 5}}}),
                json!({"a": {"b": 1, "c": {"d": 2, "e": 4, "f": 5}}}),
            ),
            (
                "scalar over object",
                json!({"a": {"b": 1}}),
                json!({"a": 5}),
                json!({"a": 5}),
            ),
            (
                "object over scalar",
                json!({"a": 5}),
                json!({"a": {"b": 1}}),
                json!({"a": {"b": 1}}),
            ),
            (
                "array replaces array",
                json!({"a": [1, 2]}),
                json!({"a": [3]}),
                json!({"a": [3]}),
            ),
            ("null replaces", json!({"a": 1}), json!({"a": null}), json!({"a": null})),
            (
                "a new key is added",
                json!({"a": 1}),
                json!({"b": 2}),
                json!({"a": 1, "b": 2}),
            ),
            (
                "a non-object partial document replaces the whole target",
                json!({"a": 1}),
                json!([1]),
                json!([1]),
            ),
        ];
        for (label, mut target, partial_document, expected) in cases {
            merge_partial_document(&mut target, &partial_document);

            assert_eq!(target, expected, "{label}");
        }
    }

    /// A failing item changes nothing, so the items around it leave the snapshot exactly as they
    /// would have alone, `_seq_no` counter included.
    #[test]
    fn a_failing_bulk_item_leaves_the_snapshot_as_the_items_before_it_left_it() {
        let build_write = |id: &str, title: &str, op_type| {
            BulkItem::Write(DocumentWrite {
                index: ".kibana_1".to_string(),
                id: id.to_string(),
                source: json!({"title": title}),
                op_type,
                concurrency: WriteConcurrency::default(),
            })
        };
        let mut snapshot = build_snapshot_with_index(".kibana_1");
        let mut snapshot_without_failures = snapshot.clone();

        let outcome = snapshot
            .apply(&SystemOperation::Bulk {
                items: vec![
                    build_write("a", "first", OpType::Index),
                    build_write("a", "conflicting", OpType::Create),
                    BulkItem::Delete(DocumentDelete {
                        index: ".missing".to_string(),
                        id: "a".to_string(),
                        concurrency: WriteConcurrency::default(),
                    }),
                    build_write("b", "second", OpType::Index),
                ],
                returned_sources: None,
            })
            .expect("a bulk answers per item");
        snapshot_without_failures
            .apply(&SystemOperation::Bulk {
                items: vec![
                    build_write("a", "first", OpType::Index),
                    build_write("b", "second", OpType::Index),
                ],
                returned_sources: None,
            })
            .expect("a bulk answers per item");

        let SystemOutcome::Bulk { items } = outcome else {
            panic!("expected a bulk outcome, got {outcome:?}");
        };
        let is_ok_by_item: Vec<bool> = items.iter().map(Result::is_ok).collect();
        assert_eq!(is_ok_by_item, [true, false, false, true]);
        assert_eq!(snapshot, snapshot_without_failures);
        assert_eq!(
            find_stored_document(&snapshot, ".kibana_1", "b"),
            Some(StoredDocument {
                source: json!({"title": "second"}),
                seq_no: 1
            })
        );
    }

    fn build_title_merge(id: &str, title: &str) -> DocumentUpdate {
        DocumentUpdate {
            index: ".kibana_1".to_string(),
            id: id.to_string(),
            change: DocumentChange::Merge(json!({"title": title})),
            upsert: None,
            concurrency: WriteConcurrency::default(),
        }
    }

    fn build_returned_sources(includes: &[&str], max_answer_bytes: usize) -> ReturnedSources {
        ReturnedSources {
            includes: includes.iter().map(ToString::to_string).collect(),
            max_answer_bytes,
        }
    }

    /// One row per path through an update, crossed with what the request asked `get` to return:
    /// nothing, some paths, all of it.
    #[test]
    fn a_bulk_update_keeps_only_what_its_get_renders() {
        let upsert_item = DocumentUpdate {
            upsert: Some(json!({"title": "Fresh", "type": "config"})),
            ..build_title_merge("b", "ignored")
        };
        let updates = [
            (
                "changed",
                build_title_merge("a", "Errors"),
                WriteResult::Updated,
                json!({"title": "Errors"}),
                json!({"title": "Errors", "type": "dashboard"}),
            ),
            (
                "noop",
                build_title_merge("a", "Logs"),
                WriteResult::Noop,
                json!({"title": "Logs"}),
                json!({"title": "Logs", "type": "dashboard"}),
            ),
            (
                "upsert",
                upsert_item,
                WriteResult::Created,
                json!({"title": "Fresh"}),
                json!({"title": "Fresh", "type": "config"}),
            ),
        ];
        for (label, update, expected_result, selected_source, whole_source) in updates {
            let cases = [
                (None, None),
                (Some(build_returned_sources(&["title"], 1024)), Some(selected_source)),
                (Some(build_returned_sources(&[], 1024)), Some(whole_source)),
            ];
            for (returned_sources, expected_source) in cases {
                let mut snapshot = build_snapshot_with_index(".kibana_1");
                snapshot
                    .apply(&build_put_operation(
                        ".kibana_1",
                        "a",
                        json!({"title": "Logs", "type": "dashboard"}),
                        OpType::Index,
                        WriteConcurrency::default(),
                    ))
                    .expect("put");

                let outcome = snapshot
                    .apply(&SystemOperation::Bulk {
                        items: vec![BulkItem::Update(update.clone())],
                        returned_sources,
                    })
                    .expect("a bulk answers per item");

                let SystemOutcome::Bulk { items } = outcome else {
                    panic!("{label}: expected a bulk outcome, got {outcome:?}");
                };
                let written_document = expect_written_document(items[0].clone().expect("the update applies"));
                assert_eq!(written_document.result, expected_result, "{label}");
                assert_eq!(written_document.source, expected_source, "{label}");
            }
        }
    }

    /// `{"title":"Errors"}` and `{"title":"Traces"}` serialize to 18 bytes each, so a bound of 18 is
    /// reached by the first update and one of 19 by the second: each row pins the check at the bound
    /// and just below it. The index and the delete apply past the bound, and a refused update takes
    /// no `_seq_no`, so the snapshot ends exactly as if the refused items had not been sent.
    #[test]
    fn an_update_item_at_or_past_the_answer_bound_is_refused_while_other_items_apply() {
        let items = vec![
            BulkItem::Update(build_title_merge("a", "Errors")),
            BulkItem::Update(build_title_merge("b", "Traces")),
            BulkItem::Write(DocumentWrite {
                index: ".kibana_1".to_string(),
                id: "c".to_string(),
                source: json!({"title": "Other"}),
                op_type: OpType::Index,
                concurrency: WriteConcurrency::default(),
            }),
            BulkItem::Update(build_title_merge("a", "Metrics")),
            BulkItem::Delete(DocumentDelete {
                index: ".kibana_1".to_string(),
                id: "d".to_string(),
                concurrency: WriteConcurrency::default(),
            }),
        ];
        let cases = [
            (
                18_usize,
                [true, false, true, false, true],
                StoredDocument {
                    source: json!({"title": "Logs"}),
                    seq_no: 1,
                },
                6_i64,
            ),
            (
                19_usize,
                [true, true, true, false, true],
                StoredDocument {
                    source: json!({"title": "Traces"}),
                    seq_no: 4,
                },
                7_i64,
            ),
        ];
        for (max_answer_bytes, expected_is_applied_by_item, expected_document_b, expected_next_seq_no) in cases {
            let mut snapshot = build_snapshot_with_index(".kibana_1");
            for id in ["a", "b", "d"] {
                snapshot
                    .apply(&build_put_operation(
                        ".kibana_1",
                        id,
                        json!({"title": "Logs"}),
                        OpType::Index,
                        WriteConcurrency::default(),
                    ))
                    .expect("put");
            }
            let mut snapshot_without_refused_items = snapshot.clone();

            let outcome = snapshot
                .apply(&SystemOperation::Bulk {
                    items: items.clone(),
                    returned_sources: Some(build_returned_sources(&[], max_answer_bytes)),
                })
                .expect("a bulk answers per item");

            let SystemOutcome::Bulk { items: item_outcomes } = outcome else {
                panic!("expected a bulk outcome, got {outcome:?}");
            };
            let is_applied_by_item: Vec<bool> = item_outcomes.iter().map(Result::is_ok).collect();
            assert_eq!(
                is_applied_by_item, expected_is_applied_by_item,
                "bound {max_answer_bytes}"
            );
            for refusal in item_outcomes.iter().filter(|item_outcome| item_outcome.is_err()) {
                assert_eq!(
                    refusal,
                    &Err(OpenSearchError::answer_too_large(max_answer_bytes)),
                    "bound {max_answer_bytes}"
                );
            }
            assert_eq!(
                find_stored_document(&snapshot, ".kibana_1", "a"),
                Some(StoredDocument {
                    source: json!({"title": "Errors"}),
                    seq_no: 3
                }),
                "bound {max_answer_bytes}"
            );
            assert_eq!(
                find_stored_document(&snapshot, ".kibana_1", "b"),
                Some(expected_document_b),
                "bound {max_answer_bytes}"
            );
            assert_eq!(
                snapshot.find_index(".kibana_1").map(|index| index.next_seq_no),
                Some(expected_next_seq_no),
                "bound {max_answer_bytes}"
            );
            let applied_items = items
                .iter()
                .zip(expected_is_applied_by_item)
                .filter(|(_, is_applied)| *is_applied)
                .map(|(item, _)| item.clone())
                .collect();
            snapshot_without_refused_items
                .apply(&SystemOperation::Bulk {
                    items: applied_items,
                    returned_sources: None,
                })
                .expect("a bulk answers per item");
            assert_eq!(snapshot, snapshot_without_refused_items, "bound {max_answer_bytes}");
        }
    }

    /// The bound counts what an update keeps, not what it stores: the stored source serializes to 37
    /// bytes and its title alone to 18. Against a bound of 19, the whole source ends the bulk after
    /// one update, its title after two, and a request that asks for no `get` keeps nothing to bound.
    #[test]
    fn the_answer_bound_counts_only_what_update_items_keep() {
        let cases = [
            (None, [true, true, true]),
            (Some(build_returned_sources(&["title"], 19)), [true, true, false]),
            (Some(build_returned_sources(&[], 19)), [true, false, false]),
        ];
        for (returned_sources, expected_is_applied_by_item) in cases {
            let mut snapshot = build_snapshot_with_index(".kibana_1");
            snapshot
                .apply(&build_put_operation(
                    ".kibana_1",
                    "a",
                    json!({"title": "Logs", "type": "dashboard"}),
                    OpType::Index,
                    WriteConcurrency::default(),
                ))
                .expect("put");

            let outcome = snapshot
                .apply(&SystemOperation::Bulk {
                    items: ["Errors", "Traces", "Metrics"]
                        .map(|title| BulkItem::Update(build_title_merge("a", title)))
                        .to_vec(),
                    returned_sources,
                })
                .expect("a bulk answers per item");

            let SystemOutcome::Bulk { items } = outcome else {
                panic!("expected a bulk outcome, got {outcome:?}");
            };
            let is_applied_by_item: Vec<bool> = items.iter().map(Result::is_ok).collect();
            assert_eq!(is_applied_by_item, expected_is_applied_by_item);
        }
    }

    /// One row per variant, each `WriteResult`, both kinds of delete and both shapes of `Bulk`.
    #[test]
    fn only_an_outcome_that_changed_the_snapshot_asks_for_a_swap() {
        let build_written = |result| {
            SystemOutcome::DocumentWritten(WrittenDocument {
                index: ".kibana_1".to_string(),
                result,
                seq_no: 0,
                primary_term: 1,
                source: None,
            })
        };
        let build_deleted = |is_found| SystemOutcome::DocumentDeleted {
            index: ".kibana_1".to_string(),
            found: is_found,
            seq_no: 0,
        };
        let cases = [
            (
                "a created index",
                SystemOutcome::IndexCreated {
                    name: ".kibana_1".to_string(),
                },
                true,
            ),
            ("applied aliases", SystemOutcome::AliasesApplied, true),
            ("a created document", build_written(WriteResult::Created), true),
            ("an updated document", build_written(WriteResult::Updated), true),
            ("a noop", build_written(WriteResult::Noop), false),
            ("a delete that removed a document", build_deleted(true), true),
            ("a delete of an unknown id", build_deleted(false), true),
            (
                "a bulk of a refusal and a noop",
                SystemOutcome::Bulk {
                    items: vec![
                        Err(OpenSearchError::version_conflict(".kibana_1", "a")),
                        Ok(build_written(WriteResult::Noop)),
                    ],
                },
                false,
            ),
            (
                "a bulk with an item that changed the snapshot",
                SystemOutcome::Bulk {
                    items: vec![
                        Err(OpenSearchError::version_conflict(".kibana_1", "a")),
                        Ok(build_written(WriteResult::Noop)),
                        Ok(build_written(WriteResult::Updated)),
                    ],
                },
                true,
            ),
        ];
        for (label, outcome, expected) in cases {
            assert_eq!(outcome.changes_snapshot(), expected, "{label}");
        }
    }

    /// The missing document is reported whether or not the update carries a condition: only an
    /// `upsert` makes the condition the deciding check.
    #[tokio::test]
    async fn update_of_a_missing_document_without_upsert_is_not_found() {
        let mut snapshot = build_snapshot_with_index(".kibana_1");
        let condition = WriteConcurrency::new(Some(0), Some(1)).expect("both values given");

        for concurrency in [WriteConcurrency::default(), condition] {
            let error = snapshot
                .apply(&SystemOperation::UpdateDocument(DocumentUpdate {
                    index: ".kibana_1".to_string(),
                    id: "missing".to_string(),
                    change: DocumentChange::Merge(json!({"a": 1})),
                    upsert: None,
                    concurrency,
                }))
                .expect_err("no document and no upsert");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{concurrency:?}");
            assert_eq!(body["error"]["type"], "document_missing_exception", "{concurrency:?}");
        }
        assert_eq!(find_stored_document(&snapshot, ".kibana_1", "missing"), None);
    }

    /// The script runs on the stored document itself, so the stored version is asserted beside
    /// the answer. A refused script takes no `_seq_no`: the write after it takes the next one.
    #[tokio::test]
    async fn increment_counter_adds_its_count_and_sets_updated_at() {
        let mut snapshot = build_snapshot_with_index(".kibana_1");
        for (id, source) in [
            ("ui-metric:a", json!({"type": "ui-metric", "ui-metric": {"count": 4}})),
            ("ui-metric:b", json!({"type": "ui-metric", "ui-metric": {}})),
            ("ui-metric:c", json!({"type": "ui-metric"})),
        ] {
            snapshot
                .apply(&build_put_operation(
                    ".kibana_1",
                    id,
                    source,
                    OpType::Index,
                    WriteConcurrency::default(),
                ))
                .expect("write");
        }
        let build_increment = |count| {
            DocumentChange::Script(DocumentScript::IncrementCounter(CounterIncrement {
                object_type: "ui-metric".to_string(),
                counter_field_name: "count".to_string(),
                count,
                time: "2026-10-08T12:00:00.000Z".to_string(),
            }))
        };

        let added_document = expect_written_document(
            snapshot
                .apply(&build_update_operation(
                    ".kibana_1",
                    "ui-metric:a",
                    build_increment(3),
                    None,
                ))
                .expect("increment"),
        );
        let initialized_document = expect_written_document(
            snapshot
                .apply(&build_update_operation(
                    ".kibana_1",
                    "ui-metric:b",
                    build_increment(2),
                    None,
                ))
                .expect("increment"),
        );
        let error = snapshot
            .apply(&build_update_operation(
                ".kibana_1",
                "ui-metric:c",
                build_increment(1),
                None,
            ))
            .expect_err("no attributes object");
        let document_after_refusal = expect_written_document(
            snapshot
                .apply(&build_put_operation(
                    ".kibana_1",
                    "ui-metric:d",
                    json!({"type": "ui-metric"}),
                    OpType::Index,
                    WriteConcurrency::default(),
                ))
                .expect("write"),
        );

        assert_eq!(
            added_document.source,
            Some(json!({"type": "ui-metric", "ui-metric": {"count": 7}, "updated_at": "2026-10-08T12:00:00.000Z"}))
        );
        assert_eq!(added_document.result, WriteResult::Updated);
        assert_eq!(added_document.seq_no, 3);
        assert_eq!(
            find_stored_document(&snapshot, ".kibana_1", "ui-metric:a"),
            Some(StoredDocument {
                source: json!({"type": "ui-metric", "ui-metric": {"count": 7}, "updated_at": "2026-10-08T12:00:00.000Z"}),
                seq_no: 3
            })
        );
        assert_eq!(
            initialized_document.source,
            Some(json!({"type": "ui-metric", "ui-metric": {"count": 2}, "updated_at": "2026-10-08T12:00:00.000Z"}))
        );
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(
            find_stored_document(&snapshot, ".kibana_1", "ui-metric:c"),
            Some(StoredDocument {
                source: json!({"type": "ui-metric"}),
                seq_no: 2
            })
        );
        assert_eq!(document_after_refusal.seq_no, 5);
    }
}
