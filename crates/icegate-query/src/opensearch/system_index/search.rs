//! Reading system indices with a query: the documents of the concrete indices a request
//! resolves to, matched in memory by the system evaluator.
//!
//! Matching, and serializing a search answer, run on the blocking pool: each is linear in the
//! snapshot, which `max_snapshot_bytes` lets grow large, and on an async worker it would stall
//! every other request that worker serves.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::documents::{filter_source, parse_json_body};
use super::evaluate::{
    FieldKind, ID_FIELD, collect_source_values, find_mapped_field, matches_document, read_boolean, read_date_micros,
    read_numeric, render_text,
};
use super::snapshot::{PRIMARY_TERM, StoredDocument, SystemIndex, SystemSnapshot};
use super::store::run_blocking_work;
use crate::opensearch::catalog::{IndexExpression, IndexTarget, read_index_options};
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, Parameters, PathParameters, RequestBody, deserialize_flag};
use crate::opensearch::search::request::{SearchRequest, SortClause, SortOrder, SourceFilter, TrackTotalHits};
use crate::opensearch::state::OpenSearchState;
use crate::query_dsl::ast::Query;
use crate::query_dsl::parse::parse_query;

/// Parameters of `_count`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CountParameters {
    /// Whether a missing concrete name is skipped instead of answered `404`.
    #[serde(default, deserialize_with = "deserialize_flag")]
    ignore_unavailable: Option<bool>,
    /// Whether an expression that selects nothing counts zero instead of answering `404`.
    #[serde(default, deserialize_with = "deserialize_flag")]
    allow_no_indices: Option<bool>,
    /// What wildcards expand to.
    expand_wildcards: Option<String>,
}

impl EndpointParameters for CountParameters {
    const IGNORED: &'static [&'static str] = &["preference", "routing", "ignore_throttled"];
}

/// `GET|POST /{index}/_count` on system indices: `{count, _shards}` with every shard successful.
/// The body may hold only `query`. An expression that selects nothing answers `count` 0, unless
/// `allow_no_indices=false`, which answers 404.
///
/// # Errors
///
/// - `400 parsing_exception` for a malformed body, a body key other than `query`, or a query the
///   parser refuses;
/// - `400 illegal_argument_exception` for a malformed index expression, a bad `expand_wildcards`,
///   a target holding a data index, or a query the evaluator refuses;
/// - `404 index_not_found_exception` for a concrete name that does not exist, unless
///   `ignore_unavailable` is set;
/// - `500` when the session cannot be opened, the store cannot be read, or the blocking work does
///   not finish.
pub(crate) async fn count_documents(
    State(state): State<OpenSearchState>,
    context: RequestContext,
    PathParameters(index): PathParameters<String>,
    Parameters(parameters): Parameters<CountParameters>,
    RequestBody(body): RequestBody,
) -> Result<Json<Value>, OpenSearchError> {
    let query = parse_count_body(&body)?;
    let expression = IndexExpression::parse(&index)?;
    let options = read_index_options(
        parameters.ignore_unavailable,
        parameters.allow_no_indices,
        parameters.expand_wildcards.as_deref(),
    )?;
    let session = state.catalog.open_session(&context.tenant).await?;
    let names = match session.resolve_target(&expression, options).await? {
        IndexTarget::System(names) => names,
        // An expression that selects nothing, when `allow_no_indices` allows that, reads no
        // documents.
        IndexTarget::Data(entries) if entries.is_empty() => {
            return Ok(Json(json!({"count": 0, "_shards": render_search_shards()})));
        }
        IndexTarget::Data(_) => {
            return Err(OpenSearchError::illegal_argument(
                "_count is served for system indices only",
            ));
        }
    };
    // The session caches the snapshot resolution read, so the count evaluates the same state.
    let snapshot = Arc::clone(&session.read_system_snapshot().await?.snapshot);
    let now = context.now;
    let count = run_blocking_work("counting system-index documents", move || {
        count_system_documents(&snapshot, &names, &query, now)
    })
    .await?;
    Ok(Json(json!({"count": count, "_shards": render_search_shards()})))
}

/// A `_count` body: empty (every document) or an object whose only key is `query`.
fn parse_count_body(body: &[u8]) -> Result<Query, OpenSearchError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Query::MatchAll);
    }
    let Value::Object(object) = parse_json_body(body)? else {
        return Err(OpenSearchError::parsing("a _count body must be a JSON object"));
    };
    let mut query = Query::MatchAll;
    for (key, value) in &object {
        match key.as_str() {
            "query" => query = parse_query(value)?,
            other => return Err(OpenSearchError::parsing(format!("request does not support [{other}]"))),
        }
    }
    Ok(query)
}

/// The `_shards` block of a system-index read: one shard, always successful.
pub(crate) fn render_search_shards() -> Value {
    json!({"total": 1, "successful": 1, "skipped": 0, "failed": 0})
}

/// The system indices named by `names`, in the order given. `names` are concrete index names, as
/// a resolved target holds them (aliases already resolved, each index once); this only looks
/// them up.
///
/// # Errors
///
/// `index_not_found_exception` naming a name the snapshot does not hold, which a target
/// resolved against the same snapshot never contains.
pub(crate) fn find_system_indices<'a>(
    snapshot: &'a SystemSnapshot,
    names: &'a [String],
) -> Result<Vec<(&'a str, &'a SystemIndex)>, OpenSearchError> {
    names
        .iter()
        .map(|name| {
            snapshot
                .find_index(name)
                .map(|index| (name.as_str(), index))
                .ok_or_else(|| OpenSearchError::index_not_found(name))
        })
        .collect()
}

/// How many documents of `indices` match `query` at `now`.
///
/// # Errors
///
/// The errors of [`find_system_indices`] and of [`matches_document`].
pub(crate) fn count_system_documents(
    snapshot: &SystemSnapshot,
    indices: &[String],
    query: &Query,
    now: DateTime<Utc>,
) -> Result<u64, OpenSearchError> {
    let mut count = 0_u64;
    for (index_name, index) in find_system_indices(snapshot, indices)? {
        for (id, document) in index.documents() {
            if matches_document(query, index_name, id, &document.source, &index.mappings, now)? {
                count += 1;
            }
        }
    }
    Ok(count)
}

/// One matching document.
struct SystemHit<'s> {
    /// The name of the index holding the document.
    index: &'s str,
    /// The document id.
    id: &'s str,
    /// The stored document.
    document: &'s StoredDocument,
}

/// A hit with its keys, one per sort clause.
type KeyedHit<'s> = (SystemHit<'s>, Vec<SortKey<'s>>);

/// How one sort clause reads the documents of one index: resolved from the index's mapping once
/// per request, before any document is read.
#[derive(Debug, Clone, Copy)]
enum SortPlan<'r> {
    /// `_score`: equal for every hit.
    Score,
    /// `_doc`: the position in index order.
    Doc,
    /// `_id`: the document id.
    Id,
    /// A field's values of a sortable `kind`, read at `source_path` (a multi-field reads its
    /// parent's values), the best one per `order`.
    Field {
        /// The field the request named: a refusal of a stored value names it, not the path
        /// holding its values.
        field: &'r str,
        /// How the stored values read.
        kind: FieldKind,
        /// The path holding the values.
        source_path: &'r str,
        /// Which value of a multi-valued field counts: the minimum ascending, the maximum
        /// descending.
        order: SortOrder,
    },
}

/// A hit's value for one sort clause.
#[derive(Debug, Clone, PartialEq)]
enum SortKey<'s> {
    /// The document holds no value: last in either order.
    Missing,
    /// A keyword or `_id`.
    Text(Cow<'s, str>),
    /// A floating-point value.
    Number(f64),
    /// An integer, or a date in epoch milliseconds.
    Integer(i64),
    /// A boolean (`false` first).
    Boolean(bool),
    /// The position in index order (index name, then `_id`).
    Position(u64),
    /// The score, equal for every hit.
    Score,
}

/// Search the documents of `indices` with `request`: hits in sort order (ties by `_id`, then
/// index name), paged by `from` and `size`, with the total, scores and per-hit keys the request
/// asks for. `_score` and `hits.max_score` are `1.0` only when the request sorts on nothing but
/// `_score`; `hits.max_score` is `null` too when the page holds no hit. No `indices` answers no
/// hits.
///
/// The sort clauses are checked against every concrete index's mapping before any document is
/// read, so a sort the mapping cannot serve is refused whether or not documents match.
///
/// # Errors
///
/// - `illegal_argument_exception` for aggregations, a mapping type or an `unmapped_type` the
///   evaluator does not support, a sort field of different kinds across `indices`, two sort clauses
///   reading one source of an index, naming the first such index of `indices`, or a stored value
///   that does not read as its sort field's kind;
/// - `query_shard` for a sort on a field the mapping lacks without `unmapped_type`, or on a text,
///   object or nested field;
/// - the errors of [`find_system_indices`] and [`matches_document`].
pub(crate) fn search_system_indices(
    snapshot: &SystemSnapshot,
    indices: &[String],
    request: &SearchRequest,
    context: &RequestContext,
) -> Result<Value, OpenSearchError> {
    if request.aggregations.is_some() {
        return Err(OpenSearchError::illegal_argument(
            "aggregations are not supported on system indices",
        ));
    }
    let concrete_indices = find_system_indices(snapshot, indices)?;
    let sort_plans = concrete_indices
        .iter()
        .map(|&(index_name, index)| plan_sort(&index.mappings, index_name, &request.sort))
        .collect::<Result<Vec<Vec<SortPlan<'_>>>, OpenSearchError>>()?;
    verify_sort_field_kinds(&concrete_indices, &sort_plans, &request.sort)?;
    let mut keyed_hits = Vec::new();
    for (&(index_name, index), clause_plans) in concrete_indices.iter().zip(&sort_plans) {
        for (id, document) in index.documents() {
            if matches_document(
                &request.query,
                index_name,
                id,
                &document.source,
                &index.mappings,
                context.now,
            )? {
                let hit = SystemHit {
                    index: index_name,
                    id,
                    document,
                };
                let position = keyed_hits.len();
                let keys = clause_plans
                    .iter()
                    .map(|plan| compute_sort_key(&hit, *plan, position))
                    .collect::<Result<Vec<SortKey<'_>>, OpenSearchError>>()?;
                keyed_hits.push((hit, keys));
            }
        }
    }
    let total = keyed_hits.len();
    order_page_hits(&mut keyed_hits, request);
    let is_scored = request.sort.iter().all(|clause| matches!(clause, SortClause::Score { .. }));
    let score = if is_scored { json!(1.0) } else { Value::Null };
    let page: Vec<Value> = keyed_hits
        .iter()
        .skip(request.from)
        .take(request.size)
        .map(|(hit, keys)| render_hit(hit, keys, request, &score))
        .collect();
    let mut hits_object = Map::new();
    if let Some(rendered_total) = render_total(total, request) {
        hits_object.insert("total".to_string(), rendered_total);
    }
    let max_score = if is_scored && !page.is_empty() {
        json!(1.0)
    } else {
        Value::Null
    };
    hits_object.insert("max_score".to_string(), max_score);
    hits_object.insert("hits".to_string(), Value::Array(page));
    let took = (Utc::now() - context.now).num_milliseconds().max(0);
    Ok(json!({
        "took": took,
        "timed_out": false,
        "_shards": render_search_shards(),
        "hits": Value::Object(hits_object)
    }))
}

/// Bring the hits of the page `from..from + size` into sort order at those positions; the other
/// hits are never read, so they stay in no particular order and cost a selection, not a sort.
///
/// `_id` and the index name together name one hit, so no two hits tie and the unstable algorithms
/// order them as stable ones would.
fn order_page_hits(keyed_hits: &mut [KeyedHit<'_>], request: &SearchRequest) {
    let compare_hits = |(left_hit, left_keys): &KeyedHit<'_>, (right_hit, right_keys): &KeyedHit<'_>| {
        compare_sort_keys(left_keys, right_keys, &request.sort)
            .then_with(|| left_hit.id.cmp(right_hit.id))
            .then_with(|| left_hit.index.cmp(right_hit.index))
    };
    let page_end = request.from.saturating_add(request.size).min(keyed_hits.len());
    if request.from >= page_end {
        return;
    }
    if page_end < keyed_hits.len() {
        keyed_hits.select_nth_unstable_by(page_end, compare_hits);
    }
    let hits_through_page = &mut keyed_hits[..page_end];
    if request.from > 0 {
        hits_through_page.select_nth_unstable_by(request.from, compare_hits);
    }
    hits_through_page[request.from..].sort_unstable_by(compare_hits);
}

/// The sort plan of `sort` on the index `index_name` holding `mappings`, one entry per clause.
///
/// # Errors
///
/// `query_shard` naming `index_name` for a field the mapping lacks without `unmapped_type`, or a
/// text, object or nested field; `illegal_argument_exception` for a mapping type or an
/// `unmapped_type` the evaluator does not support; and, once every clause has its plan, the error
/// of [`verify_sort_sources`].
fn plan_sort<'r>(
    mappings: &Value,
    index_name: &str,
    sort: &'r [SortClause],
) -> Result<Vec<SortPlan<'r>>, OpenSearchError> {
    let clause_plans = sort
        .iter()
        .map(|clause| match clause {
            SortClause::Score { .. } => Ok(SortPlan::Score),
            SortClause::Doc { .. } => Ok(SortPlan::Doc),
            SortClause::Field { field, .. } if field == ID_FIELD => Ok(SortPlan::Id),
            SortClause::Field {
                field,
                order,
                unmapped_type,
            } => plan_field_sort(mappings, index_name, field, *order, unmapped_type.as_deref()),
        })
        .collect::<Result<Vec<SortPlan<'r>>, OpenSearchError>>()?;
    verify_sort_sources(&clause_plans, index_name)?;
    Ok(clause_plans)
}

/// Refuse two plans of one `sort` that read one source of the index `index_name`.
///
/// Each clause copies what it reads into every hit's `sort`, and `max_answer_bytes` acts only on a
/// rendered page, so a repeated clause would multiply the page's stored bytes before the bound
/// could refuse them. One read per source keeps the stored bytes of a hit to two renderings: once
/// in `_source` and once in `sort`. `_score` and `_doc` read no stored bytes and may repeat.
///
/// A source is what a plan reads, not how the request spelled it, so a field and its multi-field
/// are one read. The document id is tracked apart from the paths because it is no stored value: a
/// mapping can declare `_id`, and `_id.raw` then reads a stored key that is not the id. A path is
/// compared without its trailing dots because [`collect_source_values`] reaches one value through
/// spellings that differ only in them. The comparison is conservative: such paths are refused even
/// when they share no value, as `a` and `a..` do, and the refusal names the path without its dots.
///
/// # Errors
///
/// [`OpenSearchError::repeated_sort_field`] naming `index_name` and `_id` or the path as compared.
fn verify_sort_sources(clause_plans: &[SortPlan<'_>], index_name: &str) -> Result<(), OpenSearchError> {
    let mut is_id_read = false;
    let mut seen_paths = HashSet::with_capacity(clause_plans.len());
    for plan in clause_plans {
        match plan {
            SortPlan::Score | SortPlan::Doc => {}
            SortPlan::Id if is_id_read => return Err(OpenSearchError::repeated_sort_field(ID_FIELD, index_name)),
            SortPlan::Id => is_id_read = true,
            SortPlan::Field { source_path, .. } => {
                let compared_path = source_path.trim_end_matches('.');
                // `insert` answers `false` for a value the set already holds.
                if !seen_paths.insert(compared_path) {
                    return Err(OpenSearchError::repeated_sort_field(compared_path, index_name));
                }
            }
        }
    }
    Ok(())
}

/// The sort plan of `field`: its mapped kind, or `unmapped_type` where the mapping lacks it. The
/// errors are those [`plan_sort`] states for one clause.
fn plan_field_sort<'r>(
    mappings: &Value,
    index_name: &str,
    field: &'r str,
    order: SortOrder,
    unmapped_type: Option<&str>,
) -> Result<SortPlan<'r>, OpenSearchError> {
    let (kind, source_path) = match find_mapped_field(mappings, field)? {
        Some(declared_type) => (declared_type.kind, declared_type.resolve_source_path(field)),
        None => match unmapped_type {
            Some(type_name) => (FieldKind::from_mapping_type(type_name, field)?, field),
            None => {
                return Err(OpenSearchError::query_shard(
                    index_name,
                    &format!("No mapping found for [{field}] in order to sort on"),
                ));
            }
        },
    };
    if matches!(
        kind,
        FieldKind::Text | FieldKind::Object | FieldKind::Nested | FieldKind::Identifier
    ) {
        return Err(OpenSearchError::query_shard(
            index_name,
            &format!("field [{field}] is not sortable"),
        ));
    }
    Ok(SortPlan::Field {
        field,
        kind,
        source_path,
        order,
    })
}

/// Refuse a sort field that reads as different kinds across the indices: keys of different kinds
/// have no common order, and a date and an integer would merge as one epoch number.
///
/// `sort_plans` holds one plan per entry of `concrete_indices`. Comparing every plan with the first
/// is enough: two indices that differ cannot both match the first.
///
/// # Errors
///
/// [`OpenSearchError::sort_field_conflict`] naming the field and both indices.
fn verify_sort_field_kinds(
    concrete_indices: &[(&str, &SystemIndex)],
    sort_plans: &[Vec<SortPlan<'_>>],
    sort: &[SortClause],
) -> Result<(), OpenSearchError> {
    let mut indexed_plans = concrete_indices.iter().zip(sort_plans);
    let Some(((first_index, _), first_plan)) = indexed_plans.next() else {
        return Ok(());
    };
    for ((other_index, _), other_plan) in indexed_plans {
        for ((clause, first_clause_plan), other_clause_plan) in sort.iter().zip(first_plan).zip(other_plan) {
            if let (
                SortClause::Field { field, .. },
                SortPlan::Field { kind: first_kind, .. },
                SortPlan::Field { kind: other_kind, .. },
            ) = (clause, first_clause_plan, other_clause_plan)
                && first_kind != other_kind
            {
                return Err(OpenSearchError::sort_field_conflict(field, first_index, other_index));
            }
        }
    }
    Ok(())
}

/// The key of `hit` under `plan`, with `position` the hit's place in index order: the best value of
/// a multi-valued field, or [`SortKey::Missing`] when the document holds none.
///
/// # Errors
///
/// The error of [`read_sort_key`].
fn compute_sort_key<'s>(
    hit: &SystemHit<'s>,
    plan: SortPlan<'_>,
    position: usize,
) -> Result<SortKey<'s>, OpenSearchError> {
    match plan {
        SortPlan::Score => Ok(SortKey::Score),
        SortPlan::Doc => Ok(SortKey::Position(u64::try_from(position).unwrap_or(u64::MAX))),
        SortPlan::Id => Ok(SortKey::Text(Cow::Borrowed(hit.id))),
        SortPlan::Field {
            field,
            kind,
            source_path,
            order,
        } => {
            let mut values = Vec::new();
            collect_source_values(&hit.document.source, source_path, &mut values);
            let mut best_key = None;
            for value in values {
                let candidate_key = read_sort_key(value, kind, field, hit.index, hit.id)?;
                // First in `order` is the minimum ascending and the maximum descending.
                let is_better = best_key
                    .as_ref()
                    .is_none_or(|best_key| compare_sort_key(&candidate_key, best_key, order).is_lt());
                if is_better {
                    best_key = Some(candidate_key);
                }
            }
            Ok(best_key.unwrap_or(SortKey::Missing))
        }
    }
}

/// The sort key of one stored non-null value of a sortable `kind`.
///
/// # Errors
///
/// [`OpenSearchError::unreadable_sort_value`] when the value does not read as `kind`, so
/// [`SortKey::Missing`] only ever means a document holding no value.
fn read_sort_key<'s>(
    value: &'s Value,
    kind: FieldKind,
    field: &str,
    index: &str,
    id: &str,
) -> Result<SortKey<'s>, OpenSearchError> {
    let sort_key = match kind {
        FieldKind::Keyword => render_text(value).map(SortKey::Text),
        FieldKind::Integer => match value {
            Value::Number(number) => number.as_i64(),
            Value::String(text) => text.trim().parse::<i64>().ok(),
            Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
        }
        .map(SortKey::Integer),
        FieldKind::Float => read_numeric(value).map(SortKey::Number),
        FieldKind::Date => read_date_micros(value).map(|micros| SortKey::Integer(micros / 1000)),
        FieldKind::Boolean => read_boolean(value).map(SortKey::Boolean),
        // `plan_field_sort` refuses these kinds, so no plan reads them.
        FieldKind::Identifier | FieldKind::Text | FieldKind::Object | FieldKind::Nested => None,
    };
    sort_key.ok_or_else(|| OpenSearchError::unreadable_sort_value(field, index, id))
}

/// Compare two hits' keys clause by clause: the first clause that does not tie decides.
fn compare_sort_keys(left_keys: &[SortKey<'_>], right_keys: &[SortKey<'_>], clauses: &[SortClause]) -> Ordering {
    left_keys
        .iter()
        .zip(right_keys)
        .zip(clauses)
        .map(|((left_key, right_key), clause)| {
            let order = match clause {
                SortClause::Field { order, .. } | SortClause::Score { order } | SortClause::Doc { order } => *order,
            };
            compare_sort_key(left_key, right_key, order)
        })
        .find(|ordering| *ordering != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}

/// Compare two keys in `order`; a missing value is last in either order. One clause yields one
/// key variant besides [`SortKey::Missing`]: [`verify_sort_field_kinds`] gives a clause one kind
/// across the indices and [`read_sort_key`] one variant per kind.
fn compare_sort_key(left_key: &SortKey<'_>, right_key: &SortKey<'_>, order: SortOrder) -> Ordering {
    let ordering = match (left_key, right_key) {
        (SortKey::Missing, SortKey::Missing) => return Ordering::Equal,
        (SortKey::Missing, _) => return Ordering::Greater,
        (_, SortKey::Missing) => return Ordering::Less,
        (SortKey::Text(left_text), SortKey::Text(right_text)) => left_text.cmp(right_text),
        (SortKey::Number(left_number), SortKey::Number(right_number)) => left_number.total_cmp(right_number),
        (SortKey::Integer(left_integer), SortKey::Integer(right_integer)) => left_integer.cmp(right_integer),
        (SortKey::Boolean(left_flag), SortKey::Boolean(right_flag)) => left_flag.cmp(right_flag),
        (SortKey::Position(left_position), SortKey::Position(right_position)) => left_position.cmp(right_position),
        // Every hit scores the same. A pair of different variants is unreachable under the
        // invariant above; for it this arm only keeps the function total.
        _ => Ordering::Equal,
    };
    match order {
        SortOrder::Asc => ordering,
        SortOrder::Desc => ordering.reverse(),
    }
}

/// The `sort` value of `key`: the stored string, number or boolean, epoch milliseconds for a
/// date, the ordinal for `_doc`, `null` when missing.
fn render_sort_key(key: &SortKey<'_>) -> Value {
    match key {
        SortKey::Missing => Value::Null,
        SortKey::Text(text) => Value::String(text.to_string()),
        SortKey::Number(number) => json!(number),
        SortKey::Integer(integer) => json!(integer),
        SortKey::Boolean(flag) => json!(flag),
        SortKey::Position(position) => json!(position),
        SortKey::Score => json!(1.0),
    }
}

/// `hit` as an entry of `hits.hits`, carrying what `request` asks for; `sort` only under a sort.
fn render_hit(hit: &SystemHit<'_>, keys: &[SortKey<'_>], request: &SearchRequest, score: &Value) -> Value {
    let mut rendered_hit = Map::new();
    rendered_hit.insert("_index".to_string(), Value::String(hit.index.to_string()));
    rendered_hit.insert("_id".to_string(), Value::String(hit.id.to_string()));
    if request.version {
        rendered_hit.insert("_version".to_string(), json!(1));
    }
    if request.seq_no_primary_term {
        rendered_hit.insert("_seq_no".to_string(), json!(hit.document.seq_no));
        rendered_hit.insert("_primary_term".to_string(), json!(PRIMARY_TERM));
    }
    rendered_hit.insert("_score".to_string(), score.clone());
    match &request.source {
        SourceFilter::All => {
            rendered_hit.insert("_source".to_string(), hit.document.source.clone());
        }
        SourceFilter::Nothing => {}
        SourceFilter::Paths { includes, excludes } => {
            rendered_hit.insert(
                "_source".to_string(),
                filter_source(&hit.document.source, includes, excludes),
            );
        }
    }
    if !request.sort.is_empty() {
        rendered_hit.insert(
            "sort".to_string(),
            Value::Array(keys.iter().map(render_sort_key).collect()),
        );
    }
    Value::Object(rendered_hit)
}

/// `hits.total` per `track_total_hits` and `rest_total_hits_as_int`; `None` omits it.
fn render_total(total: usize, request: &SearchRequest) -> Option<Value> {
    let total = u64::try_from(total).unwrap_or(u64::MAX);
    match request.track_total_hits {
        TrackTotalHits::Disabled => request.rest_total_hits_as_int.then(|| json!(-1)),
        TrackTotalHits::Exact if request.rest_total_hits_as_int => Some(json!(total)),
        TrackTotalHits::Exact => Some(json!({"value": total, "relation": "eq"})),
        TrackTotalHits::UpTo(threshold) => {
            let value = total.min(threshold);
            if request.rest_total_hits_as_int {
                Some(json!(value))
            } else {
                let relation = if total > threshold { "gte" } else { "eq" };
                Some(json!({"value": value, "relation": relation}))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::http::StatusCode;
    use icegate_common::TenantId;

    use super::*;
    use crate::opensearch::error::render_envelope;
    use crate::opensearch::extract::parse_parameters;
    use crate::opensearch::search::request::parse_search_request;
    use crate::opensearch::system_index::snapshot::{DocumentWrite, OpType, SystemOperation, WriteConcurrency};

    fn create_index(snapshot: &mut SystemSnapshot, name: &str, mappings: Value) {
        snapshot
            .apply(&SystemOperation::CreateIndex {
                name: name.to_string(),
                mappings,
                settings: json!({}),
            })
            .expect("the index is created");
    }

    fn write_document(snapshot: &mut SystemSnapshot, index: &str, id: &str, source: &Value) {
        snapshot
            .apply(&SystemOperation::IndexDocument(DocumentWrite {
                index: index.to_string(),
                id: id.to_string(),
                source: source.clone(),
                op_type: OpType::Index,
                concurrency: WriteConcurrency::default(),
            }))
            .expect("the document is written");
    }

    /// `.kibana_1` with one mapped field of every kind the sort tests read, holding `documents`
    /// written in order (so their `_seq_no` is their position).
    fn build_snapshot(documents: &[(&str, Value)]) -> SystemSnapshot {
        let mut snapshot = SystemSnapshot::default();
        create_index(
            &mut snapshot,
            ".kibana_1",
            json!({"properties": {
                "type": {"type": "keyword"},
                "updated_at": {"type": "date"},
                "dashboard": {"properties": {"title": {"type": "text"}}},
                "count": {"type": "integer"},
                "rank": {"type": "integer"},
                "ratio": {"type": "float"},
                "enabled": {"type": "boolean"},
                "labels": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "references": {"type": "nested"},
                "address": {"type": "ip"}
            }}),
        );
        for (id, source) in documents {
            write_document(&mut snapshot, ".kibana_1", id, source);
        }
        snapshot
    }

    fn build_context() -> RequestContext {
        RequestContext {
            tenant: TenantId::new("tenant-a").expect("a valid tenant"),
            now: "2026-10-08T12:34:56.789Z".parse().expect("a literal instant"),
        }
    }

    fn search_named_indices(
        snapshot: &SystemSnapshot,
        names: &[&str],
        body: &Value,
        query_string: &str,
    ) -> Result<Value, OpenSearchError> {
        let parameters = parse_parameters(Some(query_string)).expect("the parameters parse");
        let request = parse_search_request(body.to_string().as_bytes(), &parameters).expect("the request parses");
        let names: Vec<String> = names.iter().map(|name| (*name).to_string()).collect();
        search_system_indices(snapshot, &names, &request, &build_context())
    }

    fn search_kibana_index(
        snapshot: &SystemSnapshot,
        body: &Value,
        query_string: &str,
    ) -> Result<Value, OpenSearchError> {
        search_named_indices(snapshot, &[".kibana_1"], body, query_string)
    }

    fn read_hit_ids(answer: &Value) -> Vec<&str> {
        answer["hits"]["hits"]
            .as_array()
            .expect("hits is an array")
            .iter()
            .map(|hit| hit["_id"].as_str().expect("an id"))
            .collect()
    }

    /// The `sort` array of each hit, in hit order.
    fn read_sort_values(answer: &Value) -> Vec<Value> {
        answer["hits"]["hits"]
            .as_array()
            .expect("hits is an array")
            .iter()
            .map(|hit| hit["sort"].clone())
            .collect()
    }

    #[tokio::test]
    async fn a_name_the_snapshot_does_not_hold_is_index_not_found() {
        let names = [".kibana_1".to_string()];

        let error = find_system_indices(&SystemSnapshot::default(), &names).expect_err("refused");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["type"], "index_not_found_exception");
    }

    #[test]
    fn hits_are_sorted_with_unmapped_type_and_paged_by_from_and_size() {
        let snapshot = build_snapshot(&[
            (
                "a",
                json!({"type": "dashboard", "updated_at": "2026-10-01T00:00:00.000Z"}),
            ),
            (
                "b",
                json!({"type": "dashboard", "updated_at": "2026-10-03T00:00:00.000Z"}),
            ),
            ("c", json!({"type": "config"})),
            (
                "d",
                json!({"type": "dashboard", "updated_at": "2026-10-02T00:00:00.000Z"}),
            ),
        ]);
        let body = json!({
            "sort": [{"updated_at": {"order": "desc"}}, {"absent": {"order": "asc", "unmapped_type": "keyword"}}],
            "from": 1,
            "size": 2
        });

        let page = search_kibana_index(&snapshot, &body, "").expect("searches");
        let ascending_answer =
            search_kibana_index(&snapshot, &json!({"sort": [{"updated_at": "asc"}]}), "").expect("searches");
        let id_sorted_answer =
            search_kibana_index(&snapshot, &json!({"sort": [{"_id": "desc"}]}), "").expect("searches");

        // updated_at descending: b (10-03), d (10-02), a (10-01), then c without a value.
        assert_eq!(read_hit_ids(&page), ["d", "a"]);
        assert_eq!(page["hits"]["hits"][0]["sort"], json!([1_790_899_200_000_i64, null]));
        assert_eq!(page["hits"]["hits"][1]["sort"], json!([1_790_812_800_000_i64, null]));
        assert_eq!(page["hits"]["hits"][0]["_score"], Value::Null);
        assert_eq!(page["hits"]["max_score"], Value::Null);
        assert_eq!(page["hits"]["total"], json!({"value": 4, "relation": "eq"}));
        // Missing values sort last in ascending order as well.
        assert_eq!(read_hit_ids(&ascending_answer), ["a", "d", "b", "c"]);
        assert_eq!(read_hit_ids(&id_sorted_answer), ["d", "c", "b", "a"]);
        assert_eq!(id_sorted_answer["hits"]["hits"][0]["sort"], json!(["d"]));
    }

    #[tokio::test]
    async fn sorting_on_an_unmapped_field_without_unmapped_type_is_a_query_shard_400() {
        let one_document_snapshot = build_snapshot(&[("a", json!({"type": "dashboard"}))]);
        let empty_snapshot = build_snapshot(&[]);
        // The refusal reads the mapping, not the hits: a query matching every document, a query
        // matching none, and an index holding no document are refused alike.
        let cases = [
            ("one hit", &one_document_snapshot, json!({"match_all": {}})),
            ("no hit", &one_document_snapshot, json!({"term": {"type": "config"}})),
            ("empty index", &empty_snapshot, json!({"match_all": {}})),
        ];

        for (case, snapshot, query) in cases {
            let unmapped_error =
                search_kibana_index(snapshot, &json!({"query": query, "sort": [{"absent": "asc"}]}), "")
                    .expect_err(case);
            let text_error = search_kibana_index(snapshot, &json!({"query": query, "sort": ["dashboard.title"]}), "")
                .expect_err(case);
            let object_error =
                search_kibana_index(snapshot, &json!({"query": query, "sort": ["dashboard"]}), "").expect_err(case);
            let nested_error =
                search_kibana_index(snapshot, &json!({"query": query, "sort": ["references"]}), "").expect_err(case);
            let unmapped_text_error = search_kibana_index(
                snapshot,
                &json!({"query": query, "sort": [{"absent": {"unmapped_type": "text"}}]}),
                "",
            )
            .expect_err(case);

            for error in [
                unmapped_error,
                text_error,
                object_error,
                nested_error,
                unmapped_text_error,
            ] {
                let (status, body) = render_envelope(error).await;
                assert_eq!(status, StatusCode::BAD_REQUEST, "{case}");
                assert_eq!(body["error"]["type"], "search_phase_execution_exception", "{case}");
                assert_eq!(
                    body["error"]["root_cause"][0]["type"], "query_shard_exception",
                    "{case}"
                );
                assert_eq!(body["error"]["root_cause"][0]["index"], ".kibana_1", "{case}");
            }
        }
    }

    #[tokio::test]
    async fn a_sort_type_the_evaluator_does_not_support_is_an_illegal_argument() {
        let snapshot = build_snapshot(&[("a", json!({"type": "dashboard"}))]);
        for sort in [json!([{"absent": {"unmapped_type": "geo_point"}}]), json!(["address"])] {
            let error = search_kibana_index(&snapshot, &json!({"sort": sort}), "").expect_err("refused");

            let (status, body) = render_envelope(error).await;

            assert_eq!(
                (status, body["error"]["type"].clone()),
                (StatusCode::BAD_REQUEST, json!("illegal_argument_exception")),
                "{sort}"
            );
        }
    }

    #[tokio::test]
    async fn a_query_the_evaluator_refuses_on_a_stored_document_fails_the_search() {
        let snapshot = build_snapshot(&[("a", json!({"updated_at": "2026-10-01T00:00:00.000Z"}))]);
        let body = json!({"query": {"range": {"updated_at": {"gte": "not a date"}}}});

        let error = search_kibana_index(&snapshot, &body, "").expect_err("refused");

        let (status, answer) = render_envelope(error).await;
        assert_eq!(
            (status, answer["error"]["type"].clone()),
            (StatusCode::BAD_REQUEST, json!("illegal_argument_exception"))
        );
    }

    /// A page is selected, not cut from a full sort, so each window over the hits is compared with
    /// the order stated here: every `from` and `size` up to one past the last hit, `size` 0
    /// included, and a `size` whose page end overflows.
    #[test]
    fn every_page_holds_the_hits_of_the_full_order_at_their_positions() {
        let snapshot = build_snapshot(&[
            ("a", json!({"count": 5})),
            ("b", json!({"count": 2})),
            ("c", json!({"count": 7})),
            ("d", json!({})),
            ("e", json!({"count": 2})),
            ("f", json!({"count": 9})),
            ("g", json!({"count": 1})),
        ]);
        let full_order = ["g", "b", "e", "a", "c", "f", "d"];

        let middle_page =
            search_kibana_index(&snapshot, &json!({"sort": ["count"], "from": 2, "size": 3}), "").expect("searches");
        let last_ordinal_page =
            search_kibana_index(&snapshot, &json!({"sort": ["_doc"], "from": 6, "size": 1}), "").expect("searches");
        let overflowing_page =
            search_kibana_index(&snapshot, &json!({"sort": ["count"], "from": 1, "size": u64::MAX}), "")
                .expect("searches");

        assert_eq!(read_hit_ids(&middle_page), ["e", "a", "c"]);
        assert_eq!(read_sort_values(&middle_page), [json!([2]), json!([5]), json!([7])]);
        // `_doc` is the place among all hits, not on the page.
        assert_eq!(read_sort_values(&last_ordinal_page), [json!([6])]);
        // `from + size` does not fit a `usize`: a page end that wrapped instead of saturating would
        // read as an empty page and leave these hits unsorted.
        assert_eq!(read_hit_ids(&overflowing_page), ["b", "e", "a", "c", "f", "d"]);
        for from in 0..=8 {
            for size in 0..=8 {
                let body = json!({"sort": ["count"], "from": from, "size": size});

                let answer = search_kibana_index(&snapshot, &body, "").expect("searches");

                let expected_ids: Vec<&str> = full_order.iter().skip(from).take(size).copied().collect();
                assert_eq!(read_hit_ids(&answer), expected_ids, "{body}");
                assert_eq!(answer["hits"]["total"], json!({"value": 7, "relation": "eq"}), "{body}");
            }
        }
    }

    #[test]
    fn multi_valued_fields_sort_on_their_minimum_ascending_and_maximum_descending() {
        let snapshot = build_snapshot(&[
            ("a", json!({"labels": ["m", "z"]})),
            ("b", json!({"labels": ["y", "b"]})),
            ("c", json!({"labels": ["c", "x"]})),
            ("d", json!({})),
        ]);

        let ascending_answer =
            search_kibana_index(&snapshot, &json!({"sort": [{"labels": "asc"}]}), "").expect("searches");
        let descending_answer =
            search_kibana_index(&snapshot, &json!({"sort": [{"labels": "desc"}]}), "").expect("searches");

        assert_eq!(read_hit_ids(&ascending_answer), ["b", "c", "a", "d"]);
        assert_eq!(
            read_sort_values(&ascending_answer),
            [json!(["b"]), json!(["c"]), json!(["m"]), json!([null])]
        );
        assert_eq!(read_hit_ids(&descending_answer), ["a", "b", "c", "d"]);
        assert_eq!(
            read_sort_values(&descending_answer),
            [json!(["z"]), json!(["y"]), json!(["x"]), json!([null])]
        );
    }

    #[test]
    fn each_sortable_kind_orders_and_renders_its_sort_value() {
        let snapshot = build_snapshot(&[
            (
                "a",
                json!({"count": 3, "ratio": 0.5, "enabled": true, "title": "beta", "updated_at": "2026-10-02T00:00:00.000Z"}),
            ),
            (
                "b",
                json!({"count": 1, "ratio": 2.5, "enabled": false, "title": "alpha", "updated_at": 1_790_812_800_000_i64}),
            ),
            (
                "c",
                json!({
                    "count": [2, 9],
                    "ratio": [1.5, 0.25],
                    "enabled": [true, false],
                    "title": ["gamma", "delta"],
                    "updated_at": ["2026-10-03T00:00:00.000Z", "2026-10-04T00:00:00.000Z"]
                }),
            ),
            ("d", json!({})),
        ]);
        let cases = [
            (
                "count",
                "asc",
                ["b", "c", "a", "d"],
                [json!(1), json!(2), json!(3), Value::Null],
            ),
            (
                "count",
                "desc",
                ["c", "a", "b", "d"],
                [json!(9), json!(3), json!(1), Value::Null],
            ),
            (
                "ratio",
                "asc",
                ["c", "a", "b", "d"],
                [json!(0.25), json!(0.5), json!(2.5), Value::Null],
            ),
            (
                "ratio",
                "desc",
                ["b", "c", "a", "d"],
                [json!(2.5), json!(1.5), json!(0.5), Value::Null],
            ),
            (
                "enabled",
                "asc",
                ["b", "c", "a", "d"],
                [json!(false), json!(false), json!(true), Value::Null],
            ),
            (
                "enabled",
                "desc",
                ["a", "c", "b", "d"],
                [json!(true), json!(true), json!(false), Value::Null],
            ),
            (
                "title.keyword",
                "asc",
                ["b", "a", "c", "d"],
                [json!("alpha"), json!("beta"), json!("delta"), Value::Null],
            ),
            (
                "title.keyword",
                "desc",
                ["c", "a", "b", "d"],
                [json!("gamma"), json!("beta"), json!("alpha"), Value::Null],
            ),
            (
                "updated_at",
                "asc",
                ["b", "a", "c", "d"],
                [
                    json!(1_790_812_800_000_i64),
                    json!(1_790_899_200_000_i64),
                    json!(1_790_985_600_000_i64),
                    Value::Null,
                ],
            ),
            (
                "updated_at",
                "desc",
                ["c", "a", "b", "d"],
                [
                    json!(1_791_072_000_000_i64),
                    json!(1_790_899_200_000_i64),
                    json!(1_790_812_800_000_i64),
                    Value::Null,
                ],
            ),
        ];

        for (field, order, expected_ids, expected_values) in cases {
            let body = json!({"sort": [{field: order}]});

            let answer = search_kibana_index(&snapshot, &body, "").expect("searches");

            assert_eq!(read_hit_ids(&answer), expected_ids, "{body}");
            assert_eq!(
                read_sort_values(&answer),
                expected_values.map(|value| json!([value])),
                "{body}"
            );
        }
    }

    /// The first clause ties `b` and `c`; the second orders them against the `_id` tie-break.
    #[test]
    fn a_later_sort_clause_orders_the_ties_of_an_earlier_one() {
        let snapshot = build_snapshot(&[
            ("a", json!({"enabled": true, "count": 3})),
            ("b", json!({"enabled": false, "count": 1})),
            ("c", json!({"enabled": false, "count": 9})),
            ("d", json!({})),
        ]);

        let answer = search_kibana_index(&snapshot, &json!({"sort": [{"enabled": "asc"}, {"count": "desc"}]}), "")
            .expect("searches");

        assert_eq!(read_hit_ids(&answer), ["c", "b", "a", "d"]);
        assert_eq!(
            read_sort_values(&answer),
            [
                json!([false, 9]),
                json!([false, 1]),
                json!([true, 3]),
                json!([null, null])
            ]
        );
    }

    #[tokio::test]
    async fn sort_values_read_as_the_mapped_kind_and_unreadable_values_are_refused() {
        let snapshot = build_snapshot(&[
            ("a", json!({"rank": 3})),
            ("b", json!({"rank": "5"})),
            ("c", json!({"rank": 4})),
            ("d", json!({"rank": null})),
        ]);
        let unreadable_values = [
            ("rank", json!(2.5)),
            ("rank", json!("abc")),
            ("rank", json!([1, "abc"])),
            ("rank", json!(true)),
            ("rank", json!(u64::MAX)),
            ("ratio", json!("abc")),
            ("enabled", json!("yes")),
            ("updated_at", json!("not a date")),
            ("labels", json!({"key": "value"})),
        ];
        // A multi-field reads its parent's values, and the refusal names the field the request
        // sorted on, not the parent.
        let multi_field_snapshot = build_snapshot(&[("unreadable", json!({"title": {"key": "value"}}))]);

        let answer = search_kibana_index(&snapshot, &json!({"sort": ["rank"]}), "").expect("searches");

        assert_eq!(read_hit_ids(&answer), ["a", "c", "b", "d"]);
        assert_eq!(
            read_sort_values(&answer),
            [json!([3]), json!([4]), json!([5]), json!([null])]
        );
        for (field, value) in unreadable_values {
            let refused_snapshot = build_snapshot(&[("unreadable", json!({field: value}))]);

            let error = search_kibana_index(&refused_snapshot, &json!({"sort": [field]}), "")
                .expect_err("the value is refused");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{field} {value}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{field} {value}");
            assert_eq!(body["error"]["field"], field, "{field} {value}");
            assert_eq!(body["error"]["index"], ".kibana_1", "{field} {value}");
            assert_eq!(body["error"]["id"], "unreadable", "{field} {value}");
            // The keys are read for every hit, so the refusal does not depend on the page.
            search_kibana_index(&refused_snapshot, &json!({"size": 0, "sort": [field]}), "")
                .expect_err("the value is refused on an empty page too");
        }
        let multi_field_error = search_kibana_index(&multi_field_snapshot, &json!({"sort": ["title.keyword"]}), "")
            .expect_err("the value is refused");
        let (status, body) = render_envelope(multi_field_error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["field"], "title.keyword");
    }

    #[tokio::test]
    async fn a_sort_field_of_different_kinds_across_indices_is_refused() {
        let mut snapshot = build_snapshot(&[("a", json!({"updated_at": "2026-10-01T00:00:00.000Z", "count": 1}))]);
        create_index(
            &mut snapshot,
            ".kibana_2",
            json!({"properties": {"updated_at": {"type": "integer"}}}),
        );
        write_document(&mut snapshot, ".kibana_2", "b", &json!({"updated_at": 5}));
        let names = [".kibana_1", ".kibana_2"];
        // The refusal reads the mappings, not the hits, and an `unmapped_type` counts as the type
        // of the index that lacks the field.
        let refused_bodies = [
            ("updated_at", json!({"sort": ["updated_at"]})),
            (
                "updated_at",
                json!({"query": {"match_none": {}}, "sort": ["updated_at"]}),
            ),
            ("count", json!({"sort": [{"count": {"unmapped_type": "date"}}]})),
        ];

        for (field, body) in refused_bodies {
            let error = search_named_indices(&snapshot, &names, &body, "").expect_err("refused");

            let (status, answer) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(answer["error"]["type"], "illegal_argument_exception", "{body}");
            assert_eq!(answer["error"]["field"], field, "{body}");
            assert_eq!(answer["error"]["indices"], json!(names), "{body}");
        }
        let same_kind_answer = search_named_indices(
            &snapshot,
            &names,
            &json!({"sort": [{"count": {"unmapped_type": "integer"}}]}),
            "",
        )
        .expect("the field reads as one kind in both indices");
        assert_eq!(read_hit_ids(&same_kind_answer), ["a", "b"]);
        assert_eq!(read_sort_values(&same_kind_answer), [json!([1]), json!([null])]);
    }

    /// Assert that `sort` on `names` is refused as reading the source `field` of `index` twice.
    async fn assert_repeated_sort_field(
        snapshot: &SystemSnapshot,
        names: &[&str],
        sort: &Value,
        field: &str,
        index: &str,
    ) {
        let error = search_named_indices(snapshot, names, &json!({"sort": sort}), "").expect_err("refused");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{sort}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception", "{sort}");
        assert_eq!(body["error"]["field"], field, "{sort}");
        assert_eq!(body["error"]["index"], index, "{sort}");
    }

    /// The check runs once every clause has its plan, so the clause no mapping can serve is what
    /// the client hears about, wherever it stands in the `sort`.
    #[tokio::test]
    async fn a_clause_the_mapping_cannot_serve_keeps_its_refusal_beside_a_repeated_field() {
        let snapshot = build_snapshot(&[]);
        let body = json!({"sort": [{"type": "asc"}, {"type": "desc"}, {"absent": "asc"}]});

        let error = search_kibana_index(&snapshot, &body, "").expect_err("refused");

        let (status, answer) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(answer["error"]["type"], "search_phase_execution_exception");
        assert_eq!(answer["error"]["root_cause"][0]["type"], "query_shard_exception");
        assert_eq!(answer["error"]["root_cause"][0]["index"], ".kibana_1");
    }

    /// A multi-field reads its parent's values, so these clauses read `name` once each however
    /// they spell it, and the refusal names that path rather than either spelling.
    #[tokio::test]
    async fn sort_clauses_reaching_one_field_through_its_multi_fields_are_refused_naming_the_field() {
        let mut snapshot = SystemSnapshot::default();
        create_index(
            &mut snapshot,
            ".a_1",
            json!({"properties": {"name": {"type": "keyword", "fields": {
                "raw": {"type": "keyword"},
                "other": {"type": "keyword"}
            }}}}),
        );

        for sort in [
            json!([{"name": "asc"}, {"name.raw": "asc"}]),
            json!([{"name.raw": "asc"}, {"name.other": "desc"}]),
        ] {
            assert_repeated_sort_field(&snapshot, &[".a_1"], &sort, "name", ".a_1").await;
        }
    }

    /// Both indices collide, so the order of the names alone decides which one the refusal names.
    #[tokio::test]
    async fn a_sort_collision_in_several_indices_names_the_first_of_them() {
        let mappings = json!({"properties": {"name": {"type": "keyword", "fields": {"raw": {"type": "keyword"}}}}});
        let mut snapshot = SystemSnapshot::default();
        create_index(&mut snapshot, ".a_1", mappings.clone());
        create_index(&mut snapshot, ".b_1", mappings);
        let sort = json!([{"name": "asc"}, {"name.raw": "asc"}]);

        assert_repeated_sort_field(&snapshot, &[".a_1", ".b_1"], &sort, "name", ".a_1").await;
        assert_repeated_sort_field(&snapshot, &[".b_1", ".a_1"], &sort, "name", ".b_1").await;
    }

    /// `_id.raw` is a multi-field of a mapped `_id`, so it reads the stored key `_id`. That is not
    /// the document id, so the pair is two sources, while the stored key repeated is one.
    #[tokio::test]
    async fn the_document_id_and_a_stored_key_spelled_id_are_two_sources() {
        let mut snapshot = SystemSnapshot::default();
        create_index(
            &mut snapshot,
            ".a_1",
            json!({"properties": {"_id": {"type": "keyword", "fields": {"raw": {"type": "keyword"}}}}}),
        );
        write_document(&mut snapshot, ".a_1", "document-id", &json!({"_id": "stored-key"}));

        let answer = search_named_indices(
            &snapshot,
            &[".a_1"],
            &json!({"sort": [{"_id": "asc"}, {"_id.raw": "asc"}]}),
            "",
        )
        .expect("searches");

        assert_eq!(read_sort_values(&answer), [json!(["document-id", "stored-key"])]);
        assert_repeated_sort_field(
            &snapshot,
            &[".a_1"],
            &json!([{"_id.raw": "asc"}, {"_id.raw": "desc"}]),
            "_id",
            ".a_1",
        )
        .await;
    }

    /// A sort clause on `field` that needs no mapping.
    fn build_keyword_clause(field: &str) -> Value {
        json!({field: {"unmapped_type": "keyword"}})
    }

    /// A path also ends at a trailing dot, so `type.` reaches the value of `type`, and `absent..`
    /// the value of the stored key `absent.`, which `absent.` reaches too. Each spelling is first
    /// shown to reach the value, so these rows are real collisions rather than unusual names.
    #[tokio::test]
    async fn a_trailing_dot_does_not_make_a_sort_field_a_second_source() {
        let snapshot = build_snapshot(&[("a", json!({"type": "dashboard", "absent.": "stored-key"}))]);
        for (field, expected_value) in [
            ("type.", "dashboard"),
            ("absent.", "stored-key"),
            ("absent..", "stored-key"),
        ] {
            let answer =
                search_kibana_index(&snapshot, &json!({"sort": [build_keyword_clause(field)]}), "").expect("searches");
            assert_eq!(read_sort_values(&answer), [json!([expected_value])], "{field}");
        }
        let cases = [
            (json!([{"type": "asc"}, build_keyword_clause("type.")]), "type"),
            (json!([build_keyword_clause("type."), {"type": "desc"}]), "type"),
            (
                json!([build_keyword_clause("absent."), build_keyword_clause("absent..")]),
                "absent",
            ),
        ];

        for (sort, field) in cases {
            assert_repeated_sort_field(&snapshot, &[".kibana_1"], &sort, field, ".kibana_1").await;
        }
    }

    /// The comparison drops trailing dots and nothing else. `a` and `a..` read different keys here,
    /// so their refusal comes from the comparison and not from a shared value, and it names the
    /// path without its dots. A leading dot spells another key, so `.a` beside `a` is two reads.
    #[tokio::test]
    async fn sort_paths_differing_in_trailing_dots_are_refused_and_in_a_leading_dot_are_served() {
        let snapshot = build_snapshot(&[(
            "a",
            json!({"a": "plain-key", "a.": "trailing-dot-key", ".a": "leading-dot-key"}),
        )]);
        for (field, expected_value) in [("a", "plain-key"), ("a..", "trailing-dot-key")] {
            let answer =
                search_kibana_index(&snapshot, &json!({"sort": [build_keyword_clause(field)]}), "").expect("searches");
            assert_eq!(read_sort_values(&answer), [json!([expected_value])], "{field}");
        }

        let leading_dot_answer = search_kibana_index(
            &snapshot,
            &json!({"sort": [build_keyword_clause("a"), build_keyword_clause(".a")]}),
            "",
        )
        .expect("searches");

        assert_eq!(
            read_sort_values(&leading_dot_answer),
            [json!(["plain-key", "leading-dot-key"])]
        );
        assert_repeated_sort_field(
            &snapshot,
            &[".kibana_1"],
            &json!([build_keyword_clause("a"), build_keyword_clause("a..")]),
            "a",
            ".kibana_1",
        )
        .await;
    }

    /// Every string of up to `max_length` bytes over `a`, `b` and `.`, counted in base three.
    fn list_path_texts(max_length: u32) -> Vec<String> {
        (0..=max_length)
            .flat_map(|length| {
                (0..3_usize.pow(length)).map(move |number| {
                    (0..length)
                        .map(|digit| ['a', 'b', '.'][number / 3_usize.pow(digit) % 3])
                        .collect()
                })
            })
            .collect()
    }

    /// An object holding each of `keys` as an array of a text unique in the document and, for
    /// `depth` more levels, the same keys again. The array keeps both under one key: the source
    /// reader flattens it, so one path can end at the text and another can pass through to a child.
    fn build_keyed_source(keys: &[String], key_sequence: &str, depth: usize) -> Value {
        let entries = keys.iter().map(|key| {
            let stored_text = format!("{key_sequence}/{key}");
            let child = if depth == 0 {
                Value::Null
            } else {
                build_keyed_source(keys, &stored_text, depth - 1)
            };
            (key.clone(), json!([stored_text, child]))
        });
        Value::Object(entries.collect())
    }

    /// The reader itself is the oracle: whatever spellings reach one stored text, the check must
    /// refuse every two of them. Keys of up to two bytes at three levels spell paths of up to eight
    /// bytes, and nine leaves room for a trailing dot on each.
    #[test]
    fn every_two_paths_reaching_one_stored_value_are_refused() {
        let source = build_keyed_source(&list_path_texts(2), "", 2);
        let paths = list_path_texts(9);
        let mut reading_paths: HashMap<&str, Vec<&str>> = HashMap::new();
        for path in &paths {
            let mut values = Vec::new();
            collect_source_values(&source, path, &mut values);
            for stored_text in values.into_iter().filter_map(Value::as_str) {
                reading_paths.entry(stored_text).or_default().push(path);
            }
        }
        let plan_field_read = |source_path| SortPlan::Field {
            field: source_path,
            kind: FieldKind::Keyword,
            source_path,
            order: SortOrder::Asc,
        };

        // The fixture reaches the condition at the first level and at the third, whose longer
        // spelling takes the ninth byte of the bound.
        assert_eq!(reading_paths["/a"], ["a", "a."]);
        assert_eq!(reading_paths["/aa/bb/ab"], ["aa.bb.ab", "aa.bb.ab."]);
        for readers in reading_paths.values() {
            for (position, first_path) in readers.iter().enumerate() {
                for second_path in &readers[position + 1..] {
                    let clause_plans = [plan_field_read(*first_path), plan_field_read(*second_path)];
                    let error = verify_sort_sources(&clause_plans, ".a_1").expect_err("both read one value");

                    let cause = error.render_cause();
                    assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{first_path} {second_path}");
                    assert_eq!(
                        cause["type"], "illegal_argument_exception",
                        "{first_path} {second_path}"
                    );
                    assert_eq!(cause["index"], ".a_1", "{first_path} {second_path}");
                }
            }
        }
    }

    #[test]
    fn doc_and_score_sorts_order_and_render() {
        let snapshot = build_snapshot(&[("b", json!({})), ("a", json!({})), ("c", json!({}))]);
        let mut two_index_snapshot = build_snapshot(&[("b", json!({})), ("c", json!({}))]);
        create_index(&mut two_index_snapshot, ".kibana_2", json!({"properties": {}}));
        write_document(&mut two_index_snapshot, ".kibana_2", "a", &json!({}));
        write_document(&mut two_index_snapshot, ".kibana_2", "b", &json!({}));
        let names = [".kibana_1", ".kibana_2"];

        let doc_answer = search_kibana_index(&snapshot, &json!({"sort": ["_doc"]}), "").expect("searches");
        let reversed_doc_answer =
            search_kibana_index(&snapshot, &json!({"sort": [{"_doc": "desc"}]}), "").expect("searches");
        let score_answer = search_kibana_index(&snapshot, &json!({"sort": ["_score"]}), "").expect("searches");
        let two_index_doc_answer =
            search_named_indices(&two_index_snapshot, &names, &json!({"sort": ["_doc"]}), "").expect("searches");
        let tie_break_answer = search_named_indices(&two_index_snapshot, &names, &json!({}), "").expect("searches");

        assert_eq!(read_hit_ids(&doc_answer), ["a", "b", "c"]);
        assert_eq!(read_sort_values(&doc_answer), [json!([0]), json!([1]), json!([2])]);
        assert_eq!(doc_answer["hits"]["hits"][0]["_score"], Value::Null);
        assert_eq!(doc_answer["hits"]["max_score"], Value::Null);
        assert_eq!(read_hit_ids(&reversed_doc_answer), ["c", "b", "a"]);
        assert_eq!(
            read_sort_values(&reversed_doc_answer),
            [json!([2]), json!([1]), json!([0])]
        );
        assert_eq!(read_hit_ids(&score_answer), ["a", "b", "c"]);
        assert_eq!(
            read_sort_values(&score_answer),
            [json!([1.0]), json!([1.0]), json!([1.0])]
        );
        assert_eq!(score_answer["hits"]["hits"][0]["_score"], json!(1.0));
        assert_eq!(score_answer["hits"]["max_score"], json!(1.0));
        let read_hit_addresses = |answer: &Value| -> Vec<(Value, Value)> {
            answer["hits"]["hits"]
                .as_array()
                .expect("hits is an array")
                .iter()
                .map(|hit| (hit["_index"].clone(), hit["_id"].clone()))
                .collect()
        };
        // `_doc` is the order of index name, then `_id`.
        assert_eq!(
            read_hit_addresses(&two_index_doc_answer),
            [
                (json!(".kibana_1"), json!("b")),
                (json!(".kibana_1"), json!("c")),
                (json!(".kibana_2"), json!("a")),
                (json!(".kibana_2"), json!("b")),
            ]
        );
        assert_eq!(
            read_sort_values(&two_index_doc_answer),
            [json!([0]), json!([1]), json!([2]), json!([3])]
        );
        // Without a sort every hit ties, and the tie-break is `_id`, then index name.
        assert_eq!(
            read_hit_addresses(&tie_break_answer),
            [
                (json!(".kibana_2"), json!("a")),
                (json!(".kibana_1"), json!("b")),
                (json!(".kibana_2"), json!("b")),
                (json!(".kibana_1"), json!("c")),
            ]
        );
    }

    #[test]
    fn hits_total_renders_exact_threshold_and_disabled_forms() {
        let snapshot = build_snapshot(&[("a", json!({})), ("b", json!({})), ("c", json!({}))]);
        let cases = [
            (
                json!({"track_total_hits": 0}),
                "",
                Some(json!({"value": 0, "relation": "gte"})),
            ),
            (
                json!({"track_total_hits": 2}),
                "",
                Some(json!({"value": 2, "relation": "gte"})),
            ),
            (
                json!({"track_total_hits": 3}),
                "",
                Some(json!({"value": 3, "relation": "eq"})),
            ),
            (
                json!({"track_total_hits": 4}),
                "",
                Some(json!({"value": 3, "relation": "eq"})),
            ),
            (
                json!({"track_total_hits": true}),
                "",
                Some(json!({"value": 3, "relation": "eq"})),
            ),
            (json!({}), "", Some(json!({"value": 3, "relation": "eq"}))),
            (json!({}), "rest_total_hits_as_int=true", Some(json!(3))),
            (json!({"track_total_hits": false}), "", None),
            (
                json!({"track_total_hits": false}),
                "rest_total_hits_as_int=true",
                Some(json!(-1)),
            ),
        ];

        for (body, query_string, expected_total) in cases {
            let answer = search_kibana_index(&snapshot, &body, query_string).expect("searches");

            assert_eq!(
                answer["hits"].get("total"),
                expected_total.as_ref(),
                "{body} ?{query_string}"
            );
        }
    }

    #[test]
    fn size_zero_and_a_page_past_the_end_return_an_empty_hits_array() {
        let snapshot = build_snapshot(&[("a", json!({})), ("b", json!({}))]);

        let size_zero_answer = search_kibana_index(&snapshot, &json!({"size": 0}), "").expect("searches");
        let last_page_answer = search_kibana_index(&snapshot, &json!({"from": 1}), "").expect("searches");
        let page_at_the_end_answer = search_kibana_index(&snapshot, &json!({"from": 2}), "").expect("searches");
        let page_past_the_end_answer = search_kibana_index(&snapshot, &json!({"from": 3}), "").expect("searches");

        let expected_empty_hits = json!({"total": {"value": 2, "relation": "eq"}, "max_score": null, "hits": []});
        assert_eq!(size_zero_answer["hits"], expected_empty_hits);
        assert_eq!(read_hit_ids(&last_page_answer), ["b"]);
        assert_eq!(last_page_answer["hits"]["max_score"], json!(1.0));
        assert_eq!(page_at_the_end_answer["hits"], expected_empty_hits);
        assert_eq!(page_past_the_end_answer["hits"], expected_empty_hits);
    }

    #[test]
    fn source_filtering_includes_and_excludes_paths() {
        let snapshot = build_snapshot(&[(
            "dashboard:a",
            json!({
                "type": "dashboard",
                "dashboard": {"title": "Logs", "panelsJSON": "[]", "options": {"darkTheme": false}},
                "references": [{"id": "1", "name": "panel_0"}]
            }),
        )]);

        let filtered_answer = search_kibana_index(
            &snapshot,
            &json!({"_source": {"includes": ["dashboard.*", "type"], "excludes": ["dashboard.options"]}}),
            "",
        )
        .expect("searches");
        let omitted_answer = search_kibana_index(&snapshot, &json!({"_source": false}), "").expect("searches");
        let parameter_answer = search_kibana_index(&snapshot, &json!({}), "_source=references").expect("searches");

        assert_eq!(
            filtered_answer["hits"]["hits"][0]["_source"],
            json!({"type": "dashboard", "dashboard": {"title": "Logs", "panelsJSON": "[]"}})
        );
        assert_eq!(omitted_answer["hits"]["hits"][0].get("_source"), None);
        assert_eq!(
            parameter_answer["hits"]["hits"][0]["_source"],
            json!({"references": [{"id": "1", "name": "panel_0"}]})
        );
    }

    #[test]
    fn seq_no_primary_term_adds_both_integers_to_each_hit() {
        let snapshot = build_snapshot(&[("a", json!({"type": "dashboard"})), ("b", json!({"type": "config"}))]);

        let versioned_answer =
            search_kibana_index(&snapshot, &json!({"seq_no_primary_term": true, "version": true}), "")
                .expect("searches");
        let plain_answer = search_kibana_index(&snapshot, &json!({}), "").expect("searches");

        // The hit list is unsorted, and its order is the `_id` tie-break, a contract of this path.
        assert_eq!(
            versioned_answer["hits"]["hits"],
            json!([
                {"_index": ".kibana_1", "_id": "a", "_version": 1, "_seq_no": 0, "_primary_term": 1, "_score": 1.0, "_source": {"type": "dashboard"}},
                {"_index": ".kibana_1", "_id": "b", "_version": 1, "_seq_no": 1, "_primary_term": 1, "_score": 1.0, "_source": {"type": "config"}}
            ])
        );
        assert_eq!(versioned_answer["hits"]["max_score"], json!(1.0));
        assert_eq!(
            plain_answer["hits"]["hits"][0],
            json!({"_index": ".kibana_1", "_id": "a", "_score": 1.0, "_source": {"type": "dashboard"}})
        );
        assert_eq!(plain_answer["timed_out"], false);
        assert_eq!(
            plain_answer["_shards"],
            json!({"total": 1, "successful": 1, "skipped": 0, "failed": 0})
        );
        assert!(plain_answer["took"].is_u64(), "{plain_answer}");
    }
}
