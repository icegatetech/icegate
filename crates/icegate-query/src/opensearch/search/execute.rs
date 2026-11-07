//! Data-index `_search`: one plan per index, merged into one page of hits by the full sort key.
//!
//! Each index is scanned through the tenant scope with the query lowered for that index. Its rows
//! are selected in SQL by the sort key up to its first `_doc`, of which only the microsecond
//! within the millisecond is ordered there. The rest of a `_doc` key is the low
//! [`DOC_HASH_BITS`] bits of the row hash. So when a sort has `_doc`, the rows tied on that SQL
//! part with the last selected row, and with the `search_after` anchor, are fetched again,
//! ordered in SQL by the hash bits (computed by a UDF) and the keys after `_doc` (two-phase
//! top-K). The main top-K computes no row hash. The tie fetches compute it in their ordering,
//! and the fetch of the rows tied with the anchor also in its filter. Every fetch returns at
//! most `from + size` rows, so an index yields at most three times that many candidates. Each
//! candidate is hashed in Rust for its `_id`. The pages of several indices merge by the full
//! key, and a tie goes to the index first in name order. Hits never come from a union that
//! DataFusion coerces: each hit renders its `_source` from its own index's row. A hit's `sort`
//! and `fields` values render with its candidate, because `fields` resolves names through the
//! session; the rest of the answer is linear in the page, so it renders on the blocking pool.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Int64Type, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, Operator, SortExpr, binary_expr, cast, lit};
use datafusion::prelude::ident;
use datafusion::scalar::ScalarValue;
use futures::future::try_join_all;
use icegate_common::schema::{COL_SPAN_ID, COL_TIMESTAMP, COL_TRACE_ID};
use serde_json::{Map, Value, json};

use super::aggs::{AggregationScope, execute_aggregations, parse_aggregations, plan_aggregation_rows};
use super::doc_id::{
    CanonicalEncoder, DOC_HASH_BITS, DOC_HASH_MASK, MICROS_PER_DAY, decode_time_and_doc, document_row_hash_udf,
    encode_doc_sort_value, encode_document_id, encode_span_document_id, hash_canonical_row, hash_span_key,
    is_span_keyed, read_document_timestamp_micros, span_key_hash_udf,
};
use super::hits::{
    SearchHit, render_array_value, render_field_value, render_search_response, render_sort_scalar, summarize_total_hits,
};
use super::request::{DocValueField, SearchRequest, SortClause, SortOrder, TrackTotalHits};
use crate::engine::fixed_binary::decode_fixed_binary_literal;
use crate::error::QueryError;
use crate::opensearch::SearchLimitsConfig;
use crate::opensearch::catalog::{FieldType, FieldValue, IndexEntry, ResolvedField, TenantSession, native_column_type};
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::glob::matches_glob;
use crate::opensearch::system_index::run_blocking_work;
use crate::query_dsl::literal::parse_java_double;
use crate::query_dsl::lower::{LoweringScope, lower_query};

/// The alias of the matching-row count column.
const MATCHING_ROWS: &str = "matching_rows";
/// The prefix of the sort-key columns a page projects after the table's columns.
const SORT_COLUMN_PREFIX: &str = "__sort_";

/// Refuse a request whose `from + size` exceeds `search.max_result_window`.
///
/// # Errors
///
/// Returns [`OpenSearchError::result_window_too_large`].
pub(crate) fn check_result_window(request: &SearchRequest, limits: SearchLimitsConfig) -> Result<(), OpenSearchError> {
    if request.from.saturating_add(request.size) > limits.max_result_window {
        return Err(OpenSearchError::result_window_too_large(
            request.from,
            request.size,
            limits.max_result_window,
        ));
    }
    Ok(())
}

/// Search `entries` (the data indices a request resolved to) and answer the request's
/// aggregations beside its hits.
///
/// The aggregations are parsed and their fields resolved before any scan, so an aggregation the
/// indices cannot answer never costs a scan. They read the same matching rows as the hits: every
/// searched index, the query lowered per index. `aggregations` follows the response's other
/// keys.
///
/// # Errors
///
/// The errors of the hits search; the aggregation errors of `parse_aggregations`,
/// `plan_aggregation_rows` and `execute_aggregations`, among them `400
/// too_many_buckets_exception` past `limits.max_buckets`.
pub(crate) async fn search_data_indices(
    session: &TenantSession,
    entries: &[&IndexEntry],
    request: &SearchRequest,
    context: &RequestContext,
    limits: &SearchLimitsConfig,
) -> Result<Value, OpenSearchError> {
    let aggregation_tree = request.aggregations.as_ref().map(parse_aggregations).transpose()?;
    let aggregation_rows = match &aggregation_tree {
        Some(tree) => Some(plan_aggregation_rows(
            session,
            entries,
            &request.query,
            context.now,
            tree,
        )?),
        None => None,
    };
    let mut response = search_hits(session, entries, request, context).await?;
    let (Some(tree), Some(rows)) = (aggregation_tree, aggregation_rows) else {
        return Ok(response);
    };
    let scope = AggregationScope {
        session,
        max_buckets: limits.max_buckets,
    };
    let aggregations = execute_aggregations(rows, &tree, &scope).await?;
    let Value::Object(body) = &mut response else {
        return Err(OpenSearchError::internal("a search response is a JSON object"));
    };
    body.insert("aggregations".to_string(), Value::Object(aggregations));
    Ok(response)
}

/// The hits part of a data search: everything `search_data_indices` answers except
/// `aggregations`.
///
/// Every index is checked for a document-id rule before any scan: `spans` and `operations` are
/// keyed by `trace_id`‖`span_id`, and any other table must have a `timestamp` column. Hits come
/// in the order of the full sort key, a tie going to the index first in name order, and each
/// carries its `_id` and, when the request sorts, its `sort` values. With `search_after`, only
/// rows strictly after the anchor are returned: a scan in `[time, _doc]` order never repeats a
/// row and never skips a distinct row, except two distinct rows that share a microsecond and
/// `h mod 2^43` (probability 2^-43 per pair). Rows identical in every column share `_id` and
/// `_doc`, so `search_after` anchored on one of them returns none of its identical copies.
/// A row's microsecond is read from its `timestamp` column: a timestamp floored to the
/// microsecond, a date at its first microsecond, and `0` for a NULL or a column of another type.
/// `hits.total` counts the matching rows as `track_total_hits` asks, exactly or up to its
/// threshold (see [`summarize_total_hits`]), and ignores `search_after`. `fields` and
/// `docvalue_fields` resolve against each hit's own index. The caller checks the result window
/// first. The answer is rendered on the blocking pool, because it holds up to `from + size` hits
/// of every column.
///
/// # Errors
///
/// Returns a 400 naming an index without a document-id rule; the 400s of a sort the indices
/// cannot answer alike (a field missing without `unmapped_type`, a text or object field, a field
/// with different sort types across indices, an `unmapped_type` the data path does not sort as);
/// the 400s of a `search_after` without a sort, with `from` above 0, with a value count other than
/// the sort's or with a value that fits no sort key; the lowering errors of [`lower_query`]; the
/// 400s of [`plan_field_requests`]; the 400s of request keys the data path does not serve;
/// planning or execution failures; and `500` when a row has no canonical encoding or no span key,
/// a sort value has another type than its key, a value has no JSON rendering, a field resolves to
/// a column its hit's row does not hold, or the rendering does not finish.
async fn search_hits(
    session: &TenantSession,
    entries: &[&IndexEntry],
    request: &SearchRequest,
    context: &RequestContext,
) -> Result<Value, OpenSearchError> {
    let started_at = Instant::now();
    refuse_unserved_request_keys(request)?;
    for entry in entries {
        require_document_identity(session, entry)?;
    }
    let sort = plan_sort(session, entries, &request.sort)?;
    let field_requests = plan_field_requests(session, entries, request)?;
    let anchor = request
        .search_after
        .as_deref()
        .map(|values| parse_search_anchor(values, &sort, request.from))
        .transpose()?;
    let window = request.from.saturating_add(request.size);
    let pages = try_join_all(entries.iter().enumerate().map(|(position, entry)| {
        let scope = BranchScope {
            session,
            entry,
            position,
            sort: &sort,
            anchor: anchor.as_deref(),
        };
        search_index(scope, request, context, window)
    }))
    .await?;
    let total = summarize_total_hits(
        pages.iter().map(|page| page.matching_rows).sum(),
        &request.track_total_hits,
    );
    let mut candidates: Vec<Candidate<'_>> = pages.into_iter().flat_map(|page| page.candidates).collect();
    sort_candidates(&mut candidates, &sort.keys)?;
    let is_sorted = !sort.keys.is_empty();
    let page_hits = candidates
        .into_iter()
        .skip(request.from)
        .take(request.size)
        .map(|candidate| {
            Ok(PageHit {
                sort_values: is_sorted.then(|| render_sort_values(&candidate)).transpose()?,
                field_values: (!field_requests.is_empty())
                    .then(|| render_field_values(session, &candidate, &field_requests))
                    .transpose()?,
                index_name: candidate.entry.name().to_string(),
                batch: candidate.batch,
                row: candidate.row,
                document_id: candidate.document_id,
            })
        })
        .collect::<Result<Vec<PageHit>, OpenSearchError>>()?;
    // The work outlives the borrow of `request`, so it renders against its own copy.
    let rendering_request = request.clone();
    run_blocking_work("rendering data search hits", move || {
        let mut page_hits = page_hits;
        let hits: Vec<SearchHit<'_>> = page_hits
            .iter_mut()
            .map(
                |PageHit {
                     index_name,
                     batch,
                     row,
                     document_id,
                     sort_values,
                     field_values,
                 }| SearchHit {
                    index: index_name,
                    batch,
                    row: *row,
                    document_id,
                    sort_values: sort_values.take(),
                    field_values: field_values.take(),
                },
            )
            .collect();
        render_search_response(started_at.elapsed(), total, &hits, &rendering_request)
    })
    .await
}

/// One hit of the returned page, owning what its rendering reads, so it moves to the blocking
/// pool.
struct PageHit {
    /// The name of the index the row was read from.
    index_name: String,
    /// The table columns of the batch holding the row.
    batch: Arc<RecordBatch>,
    /// The row within `batch`.
    row: usize,
    /// The row's `_id`.
    document_id: String,
    /// The hit's `sort` values; `None` when the request does not sort.
    sort_values: Option<Vec<Value>>,
    /// The hit's `fields`; `None` when the request asks for none.
    field_values: Option<Map<String, Value>>,
}

/// A DataFusion planning failure as the envelope `From<QueryError>` maps it to.
pub(super) fn convert_plan_error(error: DataFusionError) -> OpenSearchError {
    OpenSearchError::from(QueryError::from(error))
}

/// How one sort clause orders rows; the same for every index of the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortKeyKind {
    /// An integer value: a date's epoch milliseconds, an integer, a boolean as 0 or 1.
    Long,
    /// A double.
    Double,
    /// A keyword string.
    Text,
    /// A fixed-width binary id of this width, rendered as hex.
    Hex(i32),
    /// `_doc`: the microsecond within the millisecond, then the low row-hash bits.
    Doc,
    /// `_score`: every row scores alike.
    Score,
}

impl SortKeyKind {
    /// The name a client reads for a key of this kind.
    const fn name(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Double => "double",
            Self::Text => "keyword",
            Self::Hex(_) => "hex id",
            Self::Doc => "_doc",
            Self::Score => "_score",
        }
    }

    /// The Arrow type of the SQL sort value of this kind.
    const fn build_data_type(self) -> DataType {
        match self {
            Self::Long | Self::Doc | Self::Score => DataType::Int64,
            Self::Double => DataType::Float64,
            Self::Text => DataType::Utf8,
            Self::Hex(width) => DataType::FixedSizeBinary(width),
        }
    }
}

/// One sort key of a request: how it orders and in which direction.
#[derive(Debug, Clone, Copy)]
struct SortKey {
    /// How the key's values compare.
    kind: SortKeyKind,
    /// The direction.
    order: SortOrder,
}

/// The sort of one request: a key per clause, and per index the SQL expressions of the keys.
struct SortPlan {
    /// One key per sort clause, in priority order.
    keys: Vec<SortKey>,
    /// Per index (in request order), the SQL expression of each key: a field's value (a typed
    /// NULL where the index lacks the field), `_doc`'s microsecond within the millisecond, and
    /// `None` for `_score`.
    branch_exprs: Vec<Vec<Option<Expr>>>,
    /// Per index, the low [`DOC_HASH_BITS`] bits of its rows' hash in SQL; `None` when no key is
    /// `_doc`.
    doc_hash_exprs: Vec<Option<Expr>>,
}

impl SortPlan {
    /// How the index at `position` of the request orders its rows in SQL.
    fn read_branch_ordering(&self, position: usize) -> BranchOrdering<'_> {
        BranchOrdering {
            keys: &self.keys,
            key_exprs: &self.branch_exprs[position],
            doc_hash_expr: self.doc_hash_exprs[position].as_ref(),
        }
    }
}

/// Which part of a sort key one column of an SQL ordering carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyPart {
    /// A field key's value.
    FieldValue,
    /// A `_doc` key's microsecond within the millisecond.
    DocMicros,
    /// A `_doc` key's low row-hash bits.
    DocHash,
}

/// One column of an SQL ordering: the part `part` of the sort key at position `key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrderingTerm {
    /// The position of the sort key.
    key: usize,
    /// The part of the key the column carries.
    part: KeyPart,
}

/// The terms a page is selected by: every key up to and including the first `_doc`, of which
/// only the microsecond is ordered here, `_score` excepted (every row scores alike). Ordering by
/// them computes no row hash in SQL.
fn list_prefix_terms(keys: &[SortKey]) -> Vec<OrderingTerm> {
    let mut terms = Vec::with_capacity(keys.len());
    for (position, key) in keys.iter().enumerate() {
        match key.kind {
            SortKeyKind::Score => {}
            SortKeyKind::Doc => {
                terms.push(OrderingTerm {
                    key: position,
                    part: KeyPart::DocMicros,
                });
                break;
            }
            SortKeyKind::Long | SortKeyKind::Double | SortKeyKind::Text | SortKeyKind::Hex(_) => {
                terms.push(OrderingTerm {
                    key: position,
                    part: KeyPart::FieldValue,
                });
            }
        }
    }
    terms
}

/// The terms that order the rows tied on every term of [`list_prefix_terms`]: the hash bits of
/// the first `_doc` key, then every key after it. Empty when no key is `_doc`. The prefix terms
/// followed by these order rows as [`compare_sort_values`] does.
fn list_tie_terms(keys: &[SortKey]) -> Vec<OrderingTerm> {
    let Some(first_doc) = keys.iter().position(|key| key.kind == SortKeyKind::Doc) else {
        return Vec::new();
    };
    let mut terms = vec![OrderingTerm {
        key: first_doc,
        part: KeyPart::DocHash,
    }];
    for (position, key) in keys.iter().enumerate().skip(first_doc + 1) {
        let parts: &[KeyPart] = match key.kind {
            SortKeyKind::Score => &[],
            SortKeyKind::Doc => &[KeyPart::DocMicros, KeyPart::DocHash],
            SortKeyKind::Long | SortKeyKind::Double | SortKeyKind::Text | SortKeyKind::Hex(_) => &[KeyPart::FieldValue],
        };
        terms.extend(parts.iter().map(|part| OrderingTerm {
            key: position,
            part: *part,
        }));
    }
    terms
}

/// One key's value for one row, or for the `search_after` anchor.
#[derive(Debug, Clone)]
enum SortValue {
    /// A field value (a NULL scalar is a missing value, which sorts last in either order).
    Field(ScalarValue),
    /// `_doc`: the microsecond within the millisecond, then the low [`DOC_HASH_BITS`] row-hash
    /// bits.
    Doc {
        /// The microsecond within the millisecond, `0..1000`.
        micros_in_millis: i64,
        /// The low [`DOC_HASH_BITS`] bits of the row hash.
        hash_low: u64,
    },
    /// `_score`.
    Score,
}

impl SortValue {
    /// The value of `part` of this key, as the SQL ordering compares it.
    ///
    /// # Errors
    ///
    /// Returns `500` for a part a value of this kind has not.
    fn read_part(&self, part: KeyPart) -> Result<Cow<'_, ScalarValue>, OpenSearchError> {
        match (self, part) {
            (Self::Field(value), KeyPart::FieldValue) => Ok(Cow::Borrowed(value)),
            (Self::Doc { micros_in_millis, .. }, KeyPart::DocMicros) => {
                Ok(Cow::Owned(ScalarValue::Int64(Some(*micros_in_millis))))
            }
            (Self::Doc { hash_low, .. }, KeyPart::DocHash) => Ok(Cow::Owned(ScalarValue::UInt64(Some(*hash_low)))),
            (Self::Field(_) | Self::Doc { .. } | Self::Score, _) => Err(OpenSearchError::internal(
                "a sort value was read for an ordering part of another kind",
            )),
        }
    }
}

/// Each of `terms` paired with its value in `values`, one value per sort key.
///
/// # Errors
///
/// Returns `500` when a term names a key `values` lacks, or a part its value has not.
fn bind_terms(
    terms: &[OrderingTerm],
    values: &[SortValue],
) -> Result<Vec<(OrderingTerm, ScalarValue)>, OpenSearchError> {
    terms
        .iter()
        .map(|term| Ok((*term, read_term_value(values, *term)?.into_owned())))
        .collect()
}

/// The value of `term` in `values`, one value per sort key.
///
/// # Errors
///
/// Returns `500` when the term names a key `values` lacks, or a part its value has not.
fn read_term_value(values: &[SortValue], term: OrderingTerm) -> Result<Cow<'_, ScalarValue>, OpenSearchError> {
    values
        .get(term.key)
        .ok_or_else(|| OpenSearchError::internal("an ordering term names a sort key the values lack"))?
        .read_part(term.part)
}

/// Whether `values` hold every bound value of `bound_terms`.
///
/// # Errors
///
/// Returns the errors of [`read_term_value`].
fn has_bound_values(
    values: &[SortValue],
    bound_terms: &[(OrderingTerm, ScalarValue)],
) -> Result<bool, OpenSearchError> {
    for (term, bound_value) in bound_terms {
        if read_term_value(values, *term)?.as_ref() != bound_value {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The kind a resolved field sorts as in one index.
///
/// # Errors
///
/// Returns the query-shard 400 for a text or object field, which has no sort value.
fn decide_field_kind(field: &ResolvedField, entry: &IndexEntry) -> Result<SortKeyKind, OpenSearchError> {
    match (field.field_type, &field.value) {
        (FieldType::Date | FieldType::Integer | FieldType::Long | FieldType::Boolean, _) => Ok(SortKeyKind::Long),
        (FieldType::Double, _) => Ok(SortKeyKind::Double),
        (FieldType::Keyword, FieldValue::HexId { width, .. }) => Ok(SortKeyKind::Hex(*width)),
        (FieldType::Keyword, FieldValue::Column { .. } | FieldValue::MapKey { .. }) => Ok(SortKeyKind::Text),
        (FieldType::Text | FieldType::Object, _) => Err(OpenSearchError::query_shard(
            entry.name(),
            &format!(
                "field [{}] of type [{}] is not sortable",
                field.name,
                field.field_type.name()
            ),
        )),
    }
}

/// Whether two resolutions of one name sort as one key: the same field type and, for hex ids,
/// the same width.
fn has_same_sort_type(left: &ResolvedField, right: &ResolvedField) -> bool {
    left.field_type == right.field_type && find_hex_width(left) == find_hex_width(right)
}

/// The width of a hex-id field; `None` for any other field.
const fn find_hex_width(field: &ResolvedField) -> Option<i32> {
    match field.value {
        FieldValue::HexId { width, .. } => Some(width),
        FieldValue::Column { .. } | FieldValue::MapKey { .. } => None,
    }
}

/// The kind an `unmapped_type` sorts its missing values as.
///
/// # Errors
///
/// Returns `illegal_argument_exception` naming a type that is no sortable field type of a data
/// index, rather than sorting it as some other type.
fn classify_unmapped_type(unmapped_type: &str) -> Result<SortKeyKind, OpenSearchError> {
    match unmapped_type {
        "date" | "date_nanos" | "long" | "integer" | "short" | "byte" | "boolean" => Ok(SortKeyKind::Long),
        "double" | "float" | "half_float" | "scaled_float" => Ok(SortKeyKind::Double),
        "keyword" => Ok(SortKeyKind::Text),
        unknown_type => Err(OpenSearchError::illegal_argument(format!(
            "[unmapped_type] [{unknown_type}] is no type a data index sorts on"
        ))),
    }
}

/// Resolve every sort clause for every index, refusing a sort the indices cannot answer alike.
///
/// # Errors
///
/// Returns the errors of [`plan_field_sort`], and the error an index's table failed to open with.
fn plan_sort(
    session: &TenantSession,
    entries: &[&IndexEntry],
    clauses: &[SortClause],
) -> Result<SortPlan, OpenSearchError> {
    let mut keys = Vec::with_capacity(clauses.len());
    let mut branch_exprs: Vec<Vec<Option<Expr>>> = vec![Vec::with_capacity(clauses.len()); entries.len()];
    let mut doc_hash_exprs: Vec<Option<Expr>> = vec![None; entries.len()];
    for clause in clauses {
        match clause {
            SortClause::Score { order } => {
                keys.push(SortKey {
                    kind: SortKeyKind::Score,
                    order: *order,
                });
                for exprs in &mut branch_exprs {
                    exprs.push(None);
                }
            }
            SortClause::Doc { order } => {
                keys.push(SortKey {
                    kind: SortKeyKind::Doc,
                    order: *order,
                });
                for ((entry, exprs), doc_hash_expr) in entries.iter().zip(&mut branch_exprs).zip(&mut doc_hash_exprs) {
                    let micros = build_epoch_micros_expr(session, entry)?;
                    // A row without a timestamp counts as the epoch, as its `_id` and the `_doc`
                    // value Rust computes do, so the SQL order and the Rust order agree on it.
                    exprs.push(Some(coalesce(vec![compute_floor_mod(micros, 1_000), lit(0_i64)])));
                    if doc_hash_expr.is_none() {
                        *doc_hash_expr = Some(build_doc_hash_expr(session, entry)?);
                    }
                }
            }
            SortClause::Field {
                field,
                order,
                unmapped_type,
                ..
            } => {
                let (kind, field_exprs) = plan_field_sort(session, entries, field, unmapped_type.as_deref())?;
                keys.push(SortKey { kind, order: *order });
                for (exprs, field_expr) in branch_exprs.iter_mut().zip(field_exprs) {
                    exprs.push(Some(field_expr));
                }
            }
        }
    }
    Ok(SortPlan {
        keys,
        branch_exprs,
        doc_hash_exprs,
    })
}

/// The low [`DOC_HASH_BITS`] bits of the hash `entry`'s rows carry, in SQL: of the span key on
/// span-keyed tables, and otherwise of the canonical row over every column of the index, the
/// columns [`hash_canonical_row`] hashes a fetched row over. Only the tie fetches use it: in their
/// ordering, and the fetch of the rows tied with the anchor also in its filter. Each returns at
/// most `from + size` rows. The main top-K of a sort computes no row hash.
///
/// # Errors
///
/// Returns the error the index's table failed to open with.
fn build_doc_hash_expr(session: &TenantSession, entry: &IndexEntry) -> Result<Expr, OpenSearchError> {
    let row_hash = if is_span_keyed(entry.table()) {
        span_key_hash_udf().call(vec![ident(COL_TRACE_ID), ident(COL_SPAN_ID)])
    } else {
        let column_names: Vec<String> = session
            .table_schema(entry)?
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let columns = column_names.iter().map(ident).collect();
        document_row_hash_udf(column_names).call(columns)
    };
    Ok(binary_expr(row_hash, Operator::BitwiseAnd, lit(DOC_HASH_MASK)))
}

/// The kind of a sort on `field`, and its SQL expression in each index of `entries`.
///
/// The first index that resolves the name decides the key; every later one must resolve it to
/// the same field type (and hex width), not merely to the same kind: a date and a long both sort
/// as longs, but epoch milliseconds and raw integers must never be merged into one order.
///
/// # Errors
///
/// Returns the query-shard 400 for an index lacking the field without `unmapped_type`, or
/// resolving it to a text or object field; `illegal_argument_exception` from
/// [`OpenSearchError::sort_field_conflict`] for a field with different sort types across indices,
/// and from [`classify_unmapped_type`] for an `unmapped_type` it refuses, whether or not an index
/// lacks the field; and `500` when a sort expression cannot be built.
fn plan_field_sort(
    session: &TenantSession,
    entries: &[&IndexEntry],
    field: &str,
    unmapped_type: Option<&str>,
) -> Result<(SortKeyKind, Vec<Expr>), OpenSearchError> {
    let unmapped_kind = unmapped_type.map(classify_unmapped_type).transpose()?;
    let resolutions: Vec<Option<ResolvedField>> =
        entries.iter().map(|entry| session.resolve_field(entry, field)).collect();
    let mut first_resolution: Option<(SortKeyKind, &ResolvedField, &str)> = None;
    for (entry, resolution) in entries.iter().zip(&resolutions) {
        match resolution {
            Some(resolved_field) => {
                let kind = decide_field_kind(resolved_field, entry)?;
                match first_resolution {
                    Some((_, first_field, first_index)) if !has_same_sort_type(first_field, resolved_field) => {
                        return Err(OpenSearchError::sort_field_conflict(field, first_index, entry.name()));
                    }
                    Some(_) => {}
                    None => first_resolution = Some((kind, resolved_field, entry.name())),
                }
            }
            None if unmapped_kind.is_none() => {
                return Err(OpenSearchError::query_shard(
                    entry.name(),
                    &format!("No mapping found for [{field}] in order to sort on"),
                ));
            }
            None => {}
        }
    }
    // With no index to search, nothing is sorted, whatever the kind.
    let kind = first_resolution.map_or_else(|| unmapped_kind.unwrap_or(SortKeyKind::Text), |(kind, _, _)| kind);
    let field_exprs = entries
        .iter()
        .zip(&resolutions)
        .map(|(entry, resolution)| match resolution {
            Some(resolved_field) => build_field_key_expr(session, entry, resolved_field, kind),
            None => Ok(lit(
                ScalarValue::try_new_null(&kind.build_data_type()).map_err(convert_plan_error)?
            )),
        })
        .collect::<Result<Vec<Expr>, OpenSearchError>>()?;
    Ok((kind, field_exprs))
}

/// `((value mod divisor) + divisor) mod divisor`: a modulo that is never negative.
fn compute_floor_mod(value: Expr, divisor: i64) -> Expr {
    ((value % lit(divisor)) + lit(divisor)) % lit(divisor)
}

/// `floor(value / divisor)` in integer arithmetic.
fn compute_floor_div(value: Expr, divisor: i64) -> Expr {
    (value.clone() - compute_floor_mod(value, divisor)) / lit(divisor)
}

/// The microsecond `entry`'s rows carry in their `_id` and `_doc` value, in SQL, as
/// [`read_document_timestamp_micros`] reads it from a fetched row: a timestamp of any unit floored
/// to the microsecond, a date at its first microsecond, and `0` for a column that holds no
/// instants. A NULL stays NULL here; the caller counts it as `0`.
///
/// # Errors
///
/// Returns `500` when the index has no timestamp column, and the error the index's table failed
/// to open with.
fn build_epoch_micros_expr(session: &TenantSession, entry: &IndexEntry) -> Result<Expr, OpenSearchError> {
    let data_type = session
        .column_data_type(entry, COL_TIMESTAMP)?
        .ok_or_else(|| OpenSearchError::internal(format!("index [{}] has no timestamp column", entry.name())))?;
    let timestamp_value = cast(ident(COL_TIMESTAMP), DataType::Int64);
    Ok(match data_type {
        DataType::Timestamp(TimeUnit::Second, _) => timestamp_value * lit(1_000_000_i64),
        DataType::Timestamp(TimeUnit::Millisecond, _) | DataType::Date64 => timestamp_value * lit(1_000_i64),
        DataType::Timestamp(TimeUnit::Microsecond, _) => timestamp_value,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => compute_floor_div(timestamp_value, 1_000),
        DataType::Date32 => timestamp_value * lit(MICROS_PER_DAY),
        _non_instant_type => lit(0_i64),
    })
}

/// The SQL sort value of a resolved field: dates as epoch milliseconds, integers and booleans
/// as `Int64`, doubles as `Float64`, keywords as `Utf8`, hex ids as stored.
///
/// # Errors
///
/// Returns `500` for a date column of a non-date type or a field resolved to a document key, and
/// the error the index's table failed to open with.
fn build_field_key_expr(
    session: &TenantSession,
    entry: &IndexEntry,
    field: &ResolvedField,
    kind: SortKeyKind,
) -> Result<Expr, OpenSearchError> {
    let field_read_expr = field.value.build_read_expr();
    Ok(match (kind, field.field_type, &field.value) {
        (SortKeyKind::Long, FieldType::Date, FieldValue::Column { column }) => {
            let data_type = session
                .column_data_type(entry, column)?
                .ok_or_else(|| OpenSearchError::internal(format!("date column `{column}` vanished")))?;
            let date_value = cast(field_read_expr, DataType::Int64);
            match data_type {
                DataType::Timestamp(TimeUnit::Second, _) => date_value * lit(1_000_i64),
                DataType::Timestamp(TimeUnit::Millisecond, _) | DataType::Date64 => date_value,
                DataType::Timestamp(TimeUnit::Microsecond, _) => compute_floor_div(date_value, 1_000),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => compute_floor_div(date_value, 1_000_000),
                DataType::Date32 => date_value * lit(86_400_000_i64),
                unexpected_type => {
                    return Err(OpenSearchError::internal(format!(
                        "a date column has the type {unexpected_type}"
                    )));
                }
            }
        }
        (SortKeyKind::Long, _, _) => cast(field_read_expr, DataType::Int64),
        (SortKeyKind::Double, _, _) => cast(field_read_expr, DataType::Float64),
        (SortKeyKind::Text, _, _) => cast(field_read_expr, DataType::Utf8),
        (SortKeyKind::Hex(_), _, _) => field_read_expr,
        (SortKeyKind::Doc | SortKeyKind::Score, _, _) => {
            return Err(OpenSearchError::internal("a field sort resolved to a document key"));
        }
    })
}

/// The anchor of `search_after`: one value per sort key.
///
/// # Errors
///
/// Returns `illegal_argument_exception` for a request without a sort, with `from` above 0, with
/// a value count other than the sort's, or with a value that fits no sort key.
fn parse_search_anchor(values: &[Value], sort: &SortPlan, from: usize) -> Result<Vec<SortValue>, OpenSearchError> {
    if sort.keys.is_empty() {
        return Err(OpenSearchError::illegal_argument(
            "[search_after] needs a [sort]: sort must contain at least one field",
        ));
    }
    if from > 0 {
        return Err(OpenSearchError::illegal_argument(
            "[from] must be 0 when [search_after] is used",
        ));
    }
    if values.len() != sort.keys.len() {
        return Err(OpenSearchError::illegal_argument(format!(
            "search_after has {} value(s) but sort has {}",
            values.len(),
            sort.keys.len()
        )));
    }
    values
        .iter()
        .zip(&sort.keys)
        .enumerate()
        .map(|(position, (value, key))| parse_anchor_value(value, key.kind, position))
        .collect()
}

/// One `search_after` value read as a value of a key of `kind`, the key at `position` of the
/// sort; `null` is a missing value (refused for `_doc`), and a `_doc` value must lie in
/// `[0, 1000 · 2^43)`.
///
/// # Errors
///
/// Returns `illegal_argument_exception` for a value of another form.
fn parse_anchor_value(value: &Value, kind: SortKeyKind, position: usize) -> Result<SortValue, OpenSearchError> {
    let refuse_anchor_value = || {
        OpenSearchError::illegal_argument(format!(
            "[search_after] value [{value}] at position [{position}] does not fit the [{}] sort key",
            kind.name()
        ))
    };
    let text = match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        Value::Array(_) | Value::Object(_) => return Err(refuse_anchor_value()),
    };
    let build_field_value =
        |scalar: ScalarValue| -> Result<SortValue, OpenSearchError> { Ok(SortValue::Field(scalar)) };
    match (kind, text) {
        (SortKeyKind::Score, _) => Ok(SortValue::Score),
        (SortKeyKind::Doc, Some(text)) => {
            let doc: i64 = text.parse().map_err(|_| refuse_anchor_value())?;
            if !(0..1_000_i64 << DOC_HASH_BITS).contains(&doc) {
                return Err(refuse_anchor_value());
            }
            let (micros_in_millis, hash_low) = decode_time_and_doc(0, doc);
            Ok(SortValue::Doc {
                micros_in_millis,
                hash_low,
            })
        }
        (SortKeyKind::Doc, None) => Err(refuse_anchor_value()),
        (_, None) => build_field_value(ScalarValue::try_new_null(&kind.build_data_type()).map_err(convert_plan_error)?),
        (SortKeyKind::Long, Some(text)) => build_field_value(ScalarValue::Int64(Some(
            text.parse().map_err(|_| refuse_anchor_value())?,
        ))),
        (SortKeyKind::Double, Some(text)) => build_field_value(ScalarValue::Float64(Some(
            parse_java_double(&text).ok_or_else(refuse_anchor_value)?,
        ))),
        (SortKeyKind::Text, Some(text)) => build_field_value(ScalarValue::Utf8(Some(text))),
        (SortKeyKind::Hex(width), Some(text)) => build_field_value(
            decode_fixed_binary_literal(&text, &DataType::FixedSizeBinary(width)).map_err(|_| refuse_anchor_value())?,
        ),
    }
}

/// What a search reads from one index.
#[derive(Clone, Copy)]
struct BranchScope<'a> {
    /// The tenant-scoped session the index is read through.
    session: &'a TenantSession,
    /// The index.
    entry: &'a IndexEntry,
    /// The index's position in the request, the tiebreaker between indices.
    position: usize,
    /// The request's sort.
    sort: &'a SortPlan,
    /// The `search_after` anchor, one value per sort key.
    anchor: Option<&'a [SortValue]>,
}

/// One row a page may return, with what ordering and rendering it need.
struct Candidate<'a> {
    /// The index the row was read from.
    entry: &'a IndexEntry,
    /// That index's position in the request.
    position: usize,
    /// The row's table columns (no sort columns), shared by the rows of one fetched batch.
    batch: Arc<RecordBatch>,
    /// The row within `batch`.
    row: usize,
    /// The row's `_id`.
    document_id: String,
    /// The row's timestamp in epoch microseconds; `0` for a row without one.
    timestamp_micros: i64,
    /// The row's hash: canonical, or of the span key on span-keyed tables.
    row_hash: u64,
    /// The row's value for each sort key.
    keys: Vec<SortValue>,
}

/// One index's share of a search.
struct IndexPage<'a> {
    /// How many of the index's rows match the query (ignoring `search_after`), counted as the
    /// request tracks its total: exactly, up to one past the threshold, or not at all (`0`).
    matching_rows: u64,
    /// The index's rows that can be among the first `from + size` hits, in full sort order.
    candidates: Vec<Candidate<'a>>,
}

/// Count the index's matching rows as the request tracks its total and select its candidates,
/// concurrently. Under a threshold `n` the count stops at `n + 1` rows, so no index scans past
/// what the threshold can report.
///
/// # Errors
///
/// Returns the lowering errors of [`lower_query`] and planning or execution failures.
async fn search_index<'a>(
    scope: BranchScope<'a>,
    request: &SearchRequest,
    context: &RequestContext,
    window: usize,
) -> Result<IndexPage<'a>, OpenSearchError> {
    let lowering = LoweringScope {
        session: scope.session,
        entry: scope.entry,
        now: context.now,
    };
    let matching_plan = scope
        .session
        .scan_index(scope.entry)?
        .filter(lower_query(&request.query, &lowering)?)
        .map_err(convert_plan_error)?;
    let count_plan = match request.track_total_hits {
        TrackTotalHits::Disabled => None,
        TrackTotalHits::Exact => Some(matching_plan.clone()),
        TrackTotalHits::UpTo(threshold) => Some(
            matching_plan
                .clone()
                .limit(
                    0,
                    Some(usize::try_from(threshold).unwrap_or(usize::MAX).saturating_add(1)),
                )
                .map_err(convert_plan_error)?,
        ),
    }
    .map(|counted_plan| {
        counted_plan
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1_i64)).alias(MATCHING_ROWS)])
            .and_then(LogicalPlanBuilder::build)
            .map_err(convert_plan_error)
    })
    .transpose()?;
    let (matching_rows, candidates) = futures::try_join!(
        async {
            match count_plan {
                Some(plan) => read_matching_rows(&scope.session.collect(plan).await?),
                None => Ok(0),
            }
        },
        select_candidates(scope, &matching_plan, window)
    )?;
    Ok(IndexPage {
        matching_rows,
        candidates,
    })
}

/// The rows of one index that can be among the first `window` hits, in full sort order.
///
/// Each of at most three fetches returns at most `window` rows: the rows after the anchor on
/// the prefix terms; with a `_doc` key, the rows tied with the anchor on the prefix terms and
/// after it on the rest of the key; and the rows tied with the last selected row on the prefix
/// terms. A tie fetch orders its rows in SQL by the remaining terms, the row-hash bits first, so
/// its limit keeps exactly the rows that sort first.
///
/// # Errors
///
/// Returns the errors of [`fetch_rows`], [`bind_terms`] and [`compare_sort_values`].
async fn select_candidates<'a>(
    scope: BranchScope<'a>,
    matching_plan: &LogicalPlanBuilder,
    window: usize,
) -> Result<Vec<Candidate<'a>>, OpenSearchError> {
    if window == 0 {
        return Ok(Vec::new());
    }
    let ordering = scope.sort.read_branch_ordering(scope.position);
    let prefix_terms = list_prefix_terms(&scope.sort.keys);
    let tie_terms = list_tie_terms(&scope.sort.keys);
    let anchor_prefix = scope.anchor.map(|anchor| bind_terms(&prefix_terms, anchor)).transpose()?;
    let after_filter = anchor_prefix
        .as_deref()
        .map(|bound_terms| ordering.build_after_predicate(bound_terms))
        .transpose()?;
    let mut candidates = Vec::new();
    if let (Some(anchor), Some(anchor_prefix), false) = (scope.anchor, anchor_prefix.as_deref(), tie_terms.is_empty()) {
        let filter = ordering
            .build_equal_predicate(anchor_prefix)?
            .and(ordering.build_after_predicate(&bind_terms(&tie_terms, anchor)?)?);
        for candidate in fetch_rows(scope, matching_plan, Some(filter), &tie_terms, window).await? {
            // SQL kept only the rows after the anchor; the full comparison confirms each.
            if compare_sort_values(&candidate.keys, anchor, &scope.sort.keys)? == Ordering::Greater {
                candidates.push(candidate);
            }
        }
    }
    let mut selected_candidates = fetch_rows(scope, matching_plan, after_filter.clone(), &prefix_terms, window).await?;
    if !tie_terms.is_empty() && selected_candidates.len() == window {
        // Two-phase top-K: the rows tied with the last one on the prefix terms make way for the
        // first `window` of all such rows in the order of the remaining terms.
        let boundary = bind_terms(&prefix_terms, &selected_candidates[window - 1].keys)?;
        let mut candidates_before_boundary = Vec::with_capacity(window);
        for candidate in selected_candidates {
            if !has_bound_values(&candidate.keys, &boundary)? {
                candidates_before_boundary.push(candidate);
            }
        }
        let boundary_filter = ordering.build_equal_predicate(&boundary)?;
        let filter = match after_filter {
            Some(after_anchor_filter) => after_anchor_filter.and(boundary_filter),
            None => boundary_filter,
        };
        candidates_before_boundary.extend(fetch_rows(scope, matching_plan, Some(filter), &tie_terms, window).await?);
        selected_candidates = candidates_before_boundary;
    }
    candidates.extend(selected_candidates);
    sort_candidates(&mut candidates, &scope.sort.keys)?;
    candidates.truncate(window);
    Ok(candidates)
}

/// The SQL expressions one index orders its rows by.
#[derive(Clone, Copy)]
struct BranchOrdering<'a> {
    /// The sort keys, which give each term its direction.
    keys: &'a [SortKey],
    /// Per key, its SQL expression (see [`SortPlan::branch_exprs`]).
    key_exprs: &'a [Option<Expr>],
    /// The row-hash bits a `_doc` key orders by after its microsecond; `None` when no key is
    /// `_doc`.
    doc_hash_expr: Option<&'a Expr>,
}

/// A plan projected to its table columns and one column per sort key, and sorted.
struct SortedProjection {
    /// The projected, sorted plan.
    builder: LogicalPlanBuilder,
    /// How many table columns lead the projection.
    table_column_count: usize,
    /// Per sort key, the position of its value among the projected columns; `None` for a key
    /// without an SQL expression (`_score`).
    sort_columns: Vec<Option<usize>>,
}

impl BranchOrdering<'_> {
    /// The SQL expression of `term`.
    ///
    /// # Errors
    ///
    /// Returns `500` for a term without one: a key the plan lacks, `_score`, or the hash bits
    /// of a sort without `_doc`.
    fn read_term_expr(&self, term: OrderingTerm) -> Result<&Expr, OpenSearchError> {
        match term.part {
            KeyPart::FieldValue | KeyPart::DocMicros => self.key_exprs.get(term.key).and_then(Option::as_ref),
            KeyPart::DocHash => self.doc_hash_expr,
        }
        .ok_or_else(|| OpenSearchError::internal("an ordering term has no SQL expression"))
    }

    /// The direction of `term`.
    ///
    /// # Errors
    ///
    /// Returns `500` for a term naming a key the plan lacks.
    fn read_term_order(&self, term: OrderingTerm) -> Result<SortOrder, OpenSearchError> {
        self.keys
            .get(term.key)
            .map(|key| key.order)
            .ok_or_else(|| OpenSearchError::internal("an ordering term names a sort key the plan lacks"))
    }

    /// Rows holding every bound value of `bound_terms` (a missing value matches a missing row
    /// value); every row for no terms.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Self::read_term_expr`].
    fn build_equal_predicate(&self, bound_terms: &[(OrderingTerm, ScalarValue)]) -> Result<Expr, OpenSearchError> {
        let mut predicate = lit(true);
        for (term, value) in bound_terms {
            predicate = predicate.and(build_value_equality(self.read_term_expr(*term)?.clone(), value));
        }
        Ok(predicate)
    }

    /// Rows strictly after the bound values in the lexicographic order of `bound_terms`, missing
    /// values last in either direction; no row for no terms.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`Self::read_term_expr`] and [`Self::read_term_order`].
    fn build_after_predicate(&self, bound_terms: &[(OrderingTerm, ScalarValue)]) -> Result<Expr, OpenSearchError> {
        let mut disjuncts = Vec::with_capacity(bound_terms.len());
        let mut equal_so_far: Vec<Expr> = Vec::with_capacity(bound_terms.len());
        for (term, value) in bound_terms {
            let expr = self.read_term_expr(*term)?;
            // Nothing sorts after a missing value on this term; any present value sorts before a
            // missing one.
            let after_anchor_expr = if value.is_null() {
                lit(false)
            } else {
                let past_anchor_expr = match self.read_term_order(*term)? {
                    SortOrder::Asc => expr.clone().gt(lit(value.clone())),
                    SortOrder::Desc => expr.clone().lt(lit(value.clone())),
                };
                past_anchor_expr.or(expr.clone().is_null())
            };
            disjuncts.push(equal_so_far.iter().cloned().fold(after_anchor_expr, Expr::and));
            equal_so_far.push(build_value_equality(expr.clone(), value));
        }
        Ok(disjuncts.into_iter().reduce(Expr::or).unwrap_or_else(|| lit(false)))
    }

    /// `builder` projected to its columns and one column per sort key, plus the row-hash bits
    /// when `terms` order by them, then sorted by `terms`, missing values last in either
    /// direction.
    ///
    /// # Errors
    ///
    /// Returns `500` for a term without a projected column, and planning failures.
    fn build_sorted_projection(
        &self,
        builder: LogicalPlanBuilder,
        terms: &[OrderingTerm],
    ) -> Result<SortedProjection, OpenSearchError> {
        let mut projection: Vec<Expr> = builder.schema().columns().into_iter().map(Expr::Column).collect();
        let table_column_count = projection.len();
        let sort_columns: Vec<Option<usize>> = self
            .key_exprs
            .iter()
            .map(|expr| expr.as_ref().map(|expr| push_sort_column(&mut projection, expr)))
            .collect();
        let hash_column = match self.doc_hash_expr {
            Some(expr) if terms.iter().any(|term| term.part == KeyPart::DocHash) => {
                Some(push_sort_column(&mut projection, expr))
            }
            Some(_) | None => None,
        };
        let order_by = terms
            .iter()
            .map(|term| {
                let column = match term.part {
                    KeyPart::FieldValue | KeyPart::DocMicros => sort_columns.get(term.key).copied().flatten(),
                    KeyPart::DocHash => hash_column,
                }
                .ok_or_else(|| OpenSearchError::internal("an ordering term has no projected column"))?;
                let is_ascending = matches!(self.read_term_order(*term)?, SortOrder::Asc);
                Ok(ident(build_sort_column_name(column)).sort(is_ascending, false))
            })
            .collect::<Result<Vec<SortExpr>, OpenSearchError>>()?;
        let mut builder = builder.project(projection).map_err(convert_plan_error)?;
        if !order_by.is_empty() {
            builder = builder.sort(order_by).map_err(convert_plan_error)?;
        }
        Ok(SortedProjection {
            builder,
            table_column_count,
            sort_columns,
        })
    }
}

/// `expr = value`, or `expr IS NULL` for a missing value.
fn build_value_equality(expr: Expr, value: &ScalarValue) -> Expr {
    if value.is_null() {
        expr.is_null()
    } else {
        expr.eq(lit(value.clone()))
    }
}

/// The alias of the sort column at `position` of a projection.
fn build_sort_column_name(position: usize) -> String {
    format!("{SORT_COLUMN_PREFIX}{position}")
}

/// Append `expr` to `projection` as a sort column, and return its position.
fn push_sort_column(projection: &mut Vec<Expr>, expr: &Expr) -> usize {
    let position = projection.len();
    projection.push(expr.clone().alias(build_sort_column_name(position)));
    position
}

/// Where a fetched batch holds what a candidate reads, found once per batch, and the encoder
/// every row of a fetch is hashed with.
struct CandidateColumns<'a> {
    /// The canonical encoder, reused across the rows of a fetch.
    encoder: &'a mut CanonicalEncoder,
    /// The names of the table columns, in schema order, which the canonical hash encodes.
    column_names: Vec<&'a str>,
    /// The position of the timestamp column, if the table has one.
    timestamp_index: Option<usize>,
    /// The position of the trace-id column, if the table has one.
    trace_id_index: Option<usize>,
    /// The position of the span-id column, if the table has one.
    span_id_index: Option<usize>,
    /// Per sort key, the position of its value among the projected columns; `None` for a key
    /// without an SQL expression (`_score`).
    sort_columns: &'a [Option<usize>],
}

/// Fetch the first `limit` rows of `matching_plan` that also pass `filter`, ordered by `terms`.
///
/// # Errors
///
/// Returns planning or execution failures, and the errors of [`BranchOrdering::build_sorted_projection`],
/// [`verify_sort_columns`] and [`build_candidate`].
async fn fetch_rows<'a>(
    scope: BranchScope<'a>,
    matching_plan: &LogicalPlanBuilder,
    filter: Option<Expr>,
    terms: &[OrderingTerm],
    limit: usize,
) -> Result<Vec<Candidate<'a>>, OpenSearchError> {
    let mut builder = matching_plan.clone();
    if let Some(filter) = filter {
        builder = builder.filter(filter).map_err(convert_plan_error)?;
    }
    let SortedProjection {
        builder,
        table_column_count,
        sort_columns,
    } = scope
        .sort
        .read_branch_ordering(scope.position)
        .build_sorted_projection(builder, terms)?;
    let plan = builder
        .limit(0, Some(limit))
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)?;
    let batches = scope.session.collect(plan).await?;
    let table_indices: Vec<usize> = (0..table_column_count).collect();
    let mut encoder = CanonicalEncoder::new();
    let mut candidates = Vec::new();
    for batch in &batches {
        verify_sort_columns(batch, &scope.sort.keys, &sort_columns)?;
        let rows = Arc::new(
            batch
                .project(&table_indices)
                .map_err(|error| convert_plan_error(error.into()))?,
        );
        let schema = rows.schema();
        let mut columns = CandidateColumns {
            encoder: &mut encoder,
            column_names: schema.fields().iter().map(|field| field.name().as_str()).collect(),
            timestamp_index: schema.index_of(COL_TIMESTAMP).ok(),
            trace_id_index: schema.index_of(COL_TRACE_ID).ok(),
            span_id_index: schema.index_of(COL_SPAN_ID).ok(),
            sort_columns: &sort_columns,
        };
        for row in 0..batch.num_rows() {
            candidates.push(build_candidate(scope, &rows, batch, &mut columns, row)?);
        }
    }
    Ok(candidates)
}

/// Refuse a fetched batch whose sort column holds another type than its key's kind: the
/// comparison of sort values relies on every value of a key having that one type.
///
/// # Errors
///
/// Returns `500` naming the projected type and the key's.
fn verify_sort_columns(
    projected: &RecordBatch,
    keys: &[SortKey],
    sort_columns: &[Option<usize>],
) -> Result<(), OpenSearchError> {
    for (key, column) in keys.iter().zip(sort_columns) {
        let Some(column) = column else { continue };
        let projected_type = projected.column(*column).data_type();
        let key_type = key.kind.build_data_type();
        if *projected_type != key_type {
            return Err(OpenSearchError::internal(format!(
                "a sort column of type {projected_type} was projected for a key of type {key_type}"
            )));
        }
    }
    Ok(())
}

/// The candidate of `projected[row]`: its id, its hash and its sort-key values.
///
/// # Errors
///
/// Returns `500` when the row has no canonical encoding, a span-keyed row has no span key, a sort
/// key other than `_score` has no sort column, or a sort value cannot be read.
fn build_candidate<'a>(
    scope: BranchScope<'a>,
    rows: &Arc<RecordBatch>,
    projected: &RecordBatch,
    columns: &mut CandidateColumns<'_>,
    row: usize,
) -> Result<Candidate<'a>, OpenSearchError> {
    let timestamp_micros = columns.timestamp_index.map_or(0, |index| {
        read_document_timestamp_micros(rows.column(index).as_ref(), row)
    });
    let (row_hash, document_id) = if is_span_keyed(scope.entry.table()) {
        let trace_id = read_fixed_bytes(rows, COL_TRACE_ID, columns.trace_id_index, row)?;
        let span_id = read_fixed_bytes(rows, COL_SPAN_ID, columns.span_id_index, row)?;
        (
            hash_span_key(trace_id, span_id),
            encode_span_document_id(trace_id, span_id),
        )
    } else {
        let row_hash = hash_canonical_row(columns.encoder, &columns.column_names, rows, row)?;
        (row_hash, encode_document_id(timestamp_micros, row_hash))
    };
    let keys = scope
        .sort
        .keys
        .iter()
        .zip(columns.sort_columns)
        .map(|(key, column)| match (key.kind, column) {
            (SortKeyKind::Score, _) => Ok(SortValue::Score),
            (SortKeyKind::Doc, Some(_)) => Ok(SortValue::Doc {
                micros_in_millis: timestamp_micros.rem_euclid(1_000),
                hash_low: row_hash & DOC_HASH_MASK,
            }),
            (SortKeyKind::Long | SortKeyKind::Double | SortKeyKind::Text | SortKeyKind::Hex(_), Some(column)) => {
                ScalarValue::try_from_array(projected.column(*column).as_ref(), row)
                    .map(SortValue::Field)
                    .map_err(convert_plan_error)
            }
            (
                SortKeyKind::Long | SortKeyKind::Double | SortKeyKind::Text | SortKeyKind::Hex(_) | SortKeyKind::Doc,
                None,
            ) => Err(OpenSearchError::internal(
                "a sort key other than _score has no sort column",
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Candidate {
        entry: scope.entry,
        position: scope.position,
        batch: Arc::clone(rows),
        row,
        document_id,
        timestamp_micros,
        row_hash,
        keys,
    })
}

/// The bytes of the span-key column `column`, at `column_index`, at `row`.
///
/// # Errors
///
/// Returns `500` when the column is absent, NULL at `row` or not fixed-width binary: a
/// span-keyed row without its key would otherwise get an empty `_id`, shared by every such row.
fn read_fixed_bytes<'r>(
    rows: &'r RecordBatch,
    column: &str,
    column_index: Option<usize>,
    row: usize,
) -> Result<&'r [u8], OpenSearchError> {
    let array = column_index
        .map(|index| rows.column(index))
        .ok_or_else(|| OpenSearchError::internal(format!("a span-keyed row has no `{column}` column")))?;
    if !matches!(array.data_type(), DataType::FixedSizeBinary(_)) || array.is_null(row) {
        return Err(OpenSearchError::internal(format!(
            "the span key `{column}` is NULL or of the type {}",
            array.data_type()
        )));
    }
    Ok(array.as_fixed_size_binary().value(row))
}

/// `ordering` in the direction `order`.
const fn apply_order(ordering: Ordering, order: SortOrder) -> Ordering {
    match order {
        SortOrder::Asc => ordering,
        SortOrder::Desc => ordering.reverse(),
    }
}

/// Two present sort values compared: doubles in IEEE `totalOrder`, others by their natural order.
///
/// # Errors
///
/// Returns `500` for two values of types without a common order.
fn compare_scalars(left: &ScalarValue, right: &ScalarValue) -> Result<Ordering, OpenSearchError> {
    match (left, right) {
        (ScalarValue::Float64(Some(left)), ScalarValue::Float64(Some(right))) => Ok(left.total_cmp(right)),
        _ => left.partial_cmp(right).ok_or_else(|| {
            OpenSearchError::internal(format!(
                "sort values of the types {} and {} have no common order",
                left.data_type(),
                right.data_type()
            ))
        }),
    }
}

/// Compare two key tuples in sort order; a missing value sorts after every present one.
///
/// # Errors
///
/// Returns `500` for tuples of another length than `keys`, or holding values of different kinds
/// or of types without a common order. [`verify_sort_columns`] and the anchor's parsing by key
/// kind rule these out before any comparison.
fn compare_sort_values(left: &[SortValue], right: &[SortValue], keys: &[SortKey]) -> Result<Ordering, OpenSearchError> {
    if left.len() != keys.len() || right.len() != keys.len() {
        return Err(OpenSearchError::internal(format!(
            "sort tuples of {} and {} values were compared over {} keys",
            left.len(),
            right.len(),
            keys.len()
        )));
    }
    for ((left, right), key) in left.iter().zip(right).zip(keys) {
        let ordering = match (left, right) {
            (SortValue::Field(left), SortValue::Field(right)) => match (left.is_null(), right.is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => apply_order(compare_scalars(left, right)?, key.order),
            },
            (
                SortValue::Doc {
                    micros_in_millis: left_micros,
                    hash_low: left_hash,
                },
                SortValue::Doc {
                    micros_in_millis: right_micros,
                    hash_low: right_hash,
                },
            ) => apply_order((left_micros, left_hash).cmp(&(right_micros, right_hash)), key.order),
            (SortValue::Score, SortValue::Score) => Ordering::Equal,
            (SortValue::Field(_) | SortValue::Doc { .. } | SortValue::Score, _) => {
                return Err(OpenSearchError::internal(
                    "sort values of different kinds were compared",
                ));
            }
        };
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

/// Sort `candidates` in full sort order; a tie goes to the index earlier in the request.
///
/// # Errors
///
/// Returns the first error of [`compare_sort_values`]. The candidates are then in an unspecified
/// order, and the caller drops them with the request.
fn sort_candidates(candidates: &mut [Candidate<'_>], keys: &[SortKey]) -> Result<(), OpenSearchError> {
    let mut comparison_error = None;
    candidates.sort_by(|left, right| match compare_sort_values(&left.keys, &right.keys, keys) {
        Ok(ordering) => ordering.then(left.position.cmp(&right.position)),
        Err(error) => {
            comparison_error.get_or_insert(error);
            Ordering::Equal
        }
    });
    comparison_error.map_or(Ok(()), Err)
}

/// The `sort` array of a hit: dates as epoch milliseconds, `_doc` as its sort value, `_score`
/// as `1.0`, a missing value as `null`.
///
/// # Errors
///
/// Returns the errors of [`render_sort_scalar`].
fn render_sort_values(candidate: &Candidate<'_>) -> Result<Vec<Value>, OpenSearchError> {
    candidate
        .keys
        .iter()
        .map(|key| match key {
            SortValue::Field(value) => render_sort_scalar(value),
            SortValue::Doc { .. } => Ok(json!(encode_doc_sort_value(
                candidate.timestamp_micros,
                candidate.row_hash
            ))),
            SortValue::Score => Ok(json!(1.0)),
        })
        .collect()
}

/// The single count of a count plan's answer.
///
/// # Errors
///
/// Returns `500` when the answer is not one non-negative `Int64` count.
fn read_matching_rows(batches: &[RecordBatch]) -> Result<u64, OpenSearchError> {
    let counts = batches
        .first()
        .filter(|batch| batch.num_rows() == 1)
        .and_then(|batch| batch.column(0).as_primitive_opt::<Int64Type>())
        .ok_or_else(|| OpenSearchError::internal("the row count answered no single row"))?;
    u64::try_from(counts.value(0)).map_err(|_| OpenSearchError::internal("the row count is negative"))
}

/// Refuse an index whose rows have no document id.
fn require_document_identity(session: &TenantSession, entry: &IndexEntry) -> Result<(), OpenSearchError> {
    if is_span_keyed(entry.table()) || session.column_data_type(entry, COL_TIMESTAMP)?.is_some() {
        return Ok(());
    }
    Err(OpenSearchError::missing_document_identity(entry.name()))
}

/// Refuse the request keys the data path does not serve.
fn refuse_unserved_request_keys(request: &SearchRequest) -> Result<(), OpenSearchError> {
    if request.seq_no_primary_term {
        return Err(OpenSearchError::illegal_argument(
            "[seq_no_primary_term] applies to system indices only",
        ));
    }
    Ok(())
}

/// The `docvalue_fields` then the `fields` of a request, checked against every index: a
/// concrete name or a pattern-matched column typed differently across the indices is a 400
/// naming the field and the indices of each type, and a format on a concrete non-date field is a
/// 400.
///
/// # Errors
///
/// Returns [`OpenSearchError::field_projection_conflict`] for a name typed differently across
/// `entries`, `illegal_argument_exception` for a format on a concrete field that is no date,
/// the error an index's table failed to open with, and `500` for a column a pattern matches whose
/// type no `OpenSearch` type represents.
fn plan_field_requests<'r>(
    session: &TenantSession,
    entries: &[&IndexEntry],
    request: &'r SearchRequest,
) -> Result<Vec<&'r DocValueField>, OpenSearchError> {
    let requests: Vec<&DocValueField> = request.docvalue_fields.iter().chain(&request.fields).collect();
    for field_request in &requests {
        // The type each index gives every name the request reaches, by name.
        let mut types: BTreeMap<String, BTreeMap<&'static str, Vec<&str>>> = BTreeMap::new();
        for entry in entries {
            if field_request.is_pattern() {
                let schema = session.table_schema(entry)?;
                for field in schema.fields() {
                    if !matches_glob(&field_request.field, field.name()) {
                        continue;
                    }
                    let field_type = native_column_type(field)?;
                    if field_type != FieldType::Object {
                        types
                            .entry(field.name().clone())
                            .or_default()
                            .entry(field_type.name())
                            .or_default()
                            .push(entry.name());
                    }
                }
            } else if let Some(resolved_field) = session.resolve_field(entry, &field_request.field) {
                if field_request.format.is_some() && resolved_field.field_type != FieldType::Date {
                    return Err(OpenSearchError::illegal_argument(format!(
                        "field [{}] of type [{}] in index [{}] does not support formats",
                        field_request.field,
                        resolved_field.field_type.name(),
                        entry.name()
                    )));
                }
                types
                    .entry(field_request.field.clone())
                    .or_default()
                    .entry(resolved_field.field_type.name())
                    .or_default()
                    .push(entry.name());
            }
        }
        if let Some((name, indices_by_type)) = types.iter().find(|(_, indices_by_type)| indices_by_type.len() > 1) {
            return Err(OpenSearchError::field_projection_conflict(name, indices_by_type));
        }
    }
    Ok(requests)
}

/// The `fields` of one hit: each requested name or pattern resolved against the hit's own index,
/// each value a one-element array, a missing value no key; a later request for the same name
/// replaces the earlier one.
///
/// # Errors
///
/// Returns `500` for a value [`render_field_value`] or [`render_array_value`] cannot render, a
/// column of a type no `OpenSearch` type represents, a MAP column that is not a MAP of `Utf8`
/// keys, and a resolved column the hit's batch does not hold: the batch holds every column of
/// the index's table, so a missing one is a broken invariant, not a missing value.
fn render_field_values(
    session: &TenantSession,
    candidate: &Candidate<'_>,
    requests: &[&DocValueField],
) -> Result<Map<String, Value>, OpenSearchError> {
    let batch = candidate.batch.as_ref();
    let row = candidate.row;
    let mut values = Map::new();
    for field_request in requests {
        if field_request.is_pattern() {
            render_pattern_field_values(batch, row, field_request, &mut values)?;
            continue;
        }
        let Some(resolved_field) = session.resolve_field(candidate.entry, &field_request.field) else {
            continue;
        };
        let value = match &resolved_field.value {
            FieldValue::Column { column } | FieldValue::HexId { column, .. } => {
                let array = read_resolved_column(batch, column)?;
                if array.is_null(row) {
                    continue;
                }
                Some(render_field_value(array.as_ref(), row, field_request.format)?)
            }
            FieldValue::MapKey { column, key } => {
                let array = read_resolved_column(batch, column)?;
                if array.is_null(row) {
                    continue;
                }
                let MapEntries {
                    keys,
                    values: map_values,
                    mut entries,
                } = read_map_entries(array.as_ref(), row, column)?;
                entries
                    .find(|&entry| !keys.is_null(entry) && keys.value(entry) == key.as_str())
                    .filter(|&entry| !map_values.is_null(entry))
                    .map(|entry| render_array_value(map_values.as_ref(), entry))
                    .transpose()?
            }
        };
        if let Some(value) = value {
            values.insert(field_request.field.clone(), json!([value]));
        }
    }
    Ok(values)
}

/// The column `column` of a hit's `batch`, which a field of the hit's index resolved to.
///
/// # Errors
///
/// Returns `500` when `batch` has no such column.
fn read_resolved_column<'b>(batch: &'b RecordBatch, column: &str) -> Result<&'b ArrayRef, OpenSearchError> {
    batch
        .column_by_name(column)
        .ok_or_else(|| OpenSearchError::internal(format!("the resolved column `{column}` is not in the hit's batch")))
}

/// The entries of one MAP value, borrowed from its column. `keys` and `values` span the whole
/// column; each index in `entries` addresses one entry of this value in both.
#[derive(Debug, Clone)]
struct MapEntries<'a> {
    keys: &'a StringArray,
    values: &'a ArrayRef,
    entries: Range<usize>,
}

/// The entries of the MAP value at `row` of `array`, the column named `column`. Reads the
/// column's own key and value arrays in place, so a row's entries cost no copy.
///
/// # Errors
///
/// Returns `500` when `array` is not a MAP or its keys are not `Utf8`.
fn read_map_entries<'a>(array: &'a dyn Array, row: usize, column: &str) -> Result<MapEntries<'a>, OpenSearchError> {
    let map = array
        .as_map_opt()
        .ok_or_else(|| OpenSearchError::internal(format!("column `{column}` is not a MAP")))?;
    let keys = map
        .keys()
        .as_string_opt::<i32>()
        .ok_or_else(|| OpenSearchError::internal(format!("the MAP column `{column}` has non-Utf8 keys")))?;
    let offsets = map.value_offsets();
    // Arrow validates a MAP's offsets as non-negative and ascending when the array is built, so
    // `as_usize` (a plain widening for an `i32` offset) cannot wrap.
    Ok(MapEntries {
        keys,
        values: map.values(),
        entries: offsets[row].as_usize()..offsets[row + 1].as_usize(),
    })
}

/// Insert into `values` the `fields` of `batch[row]` that the pattern of `field_request` matches:
/// every non-null scalar column by its name, in `field_request`'s format, and every MAP entry with
/// a value by its path `<column>.<stored key>`, the stored key never split.
///
/// # Errors
///
/// Returns `500` for a value that cannot be rendered, a matched column of a type no `OpenSearch`
/// type represents, and a MAP column that is not a MAP of `Utf8` keys or holds a NULL key.
fn render_pattern_field_values(
    batch: &RecordBatch,
    row: usize,
    field_request: &DocValueField,
    values: &mut Map<String, Value>,
) -> Result<(), OpenSearchError> {
    // One buffer for every entry's path, so an entry the pattern does not match allocates nothing.
    let mut path = String::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if column.is_null(row) {
            continue;
        }
        if let DataType::Map(_, _) = column.data_type() {
            let MapEntries {
                keys,
                values: map_values,
                entries,
            } = read_map_entries(column.as_ref(), row, field.name())?;
            for entry in entries {
                if keys.is_null(entry) {
                    return Err(OpenSearchError::internal(format!(
                        "the MAP column `{}` has a NULL key",
                        field.name()
                    )));
                }
                if map_values.is_null(entry) {
                    continue;
                }
                path.clear();
                path.push_str(field.name());
                path.push('.');
                path.push_str(keys.value(entry));
                if matches_glob(&field_request.field, &path) {
                    values.insert(path.clone(), json!([render_array_value(map_values.as_ref(), entry)?]));
                }
            }
        } else if matches_glob(&field_request.field, field.name()) && native_column_type(field)? != FieldType::Object {
            values.insert(
                field.name().clone(),
                json!([render_field_value(column.as_ref(), row, field_request.format)?]),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::sync::Arc;

    use datafusion::arrow::array::{
        AsArray, FixedSizeBinaryArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
    use datafusion::prelude::{SessionContext, ident};
    use datafusion::scalar::ScalarValue;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};

    use super::{
        BranchOrdering, OrderingTerm, SortKey, SortKeyKind, SortValue, bind_terms, compare_sort_values,
        list_prefix_terms, list_tie_terms, read_fixed_bytes, verify_sort_columns,
    };
    use crate::opensearch::error::{OpenSearchError, render_envelope};
    use crate::opensearch::search::request::SortOrder;

    /// The generated table's row-id column, its column per field kind of [`FIELD_KINDS`], and the
    /// two columns a `_doc` key reads: its microsecond and its row-hash bits.
    const ROW_ID_COLUMN: &str = "row_id";
    const FIELD_COLUMNS: [&str; 3] = ["long_key", "text_key", "double_key"];
    const DOC_MICROS_COLUMN: &str = "doc_micros";
    const DOC_HASH_COLUMN: &str = "doc_hash";
    const FIELD_KINDS: [SortKeyKind; 3] = [SortKeyKind::Long, SortKeyKind::Text, SortKeyKind::Double];
    /// Few distinct values per key, so ties on a key prefix are frequent and later keys decide.
    const TEXT_VALUES: [&str; 4] = ["", "a", "ab", "b"];
    const DOUBLE_VALUES: [f64; 6] = [-1.5, -0.0, 0.0, 2.25, f64::INFINITY, f64::NAN];

    /// One value of a key of `kind`: a field value missing one time in four, or a `_doc` value
    /// from three microseconds and four hash values.
    fn generate_value(generator: &mut StdRng, kind: SortKeyKind) -> SortValue {
        let is_missing = generator.random_range(0..4) == 0;
        match kind {
            SortKeyKind::Long => {
                SortValue::Field(ScalarValue::Int64((!is_missing).then(|| generator.random_range(-2..3))))
            }
            SortKeyKind::Text => SortValue::Field(ScalarValue::Utf8(
                (!is_missing).then(|| TEXT_VALUES[generator.random_range(0..TEXT_VALUES.len())].to_string()),
            )),
            SortKeyKind::Double => SortValue::Field(ScalarValue::Float64(
                (!is_missing).then(|| DOUBLE_VALUES[generator.random_range(0..DOUBLE_VALUES.len())]),
            )),
            SortKeyKind::Doc => SortValue::Doc {
                micros_in_millis: generator.random_range(0..3),
                hash_low: generator.random_range(0..4),
            },
            SortKeyKind::Hex(_) | SortKeyKind::Score => unreachable!("the generated keys are fields and _doc"),
        }
    }

    fn generate_row(generator: &mut StdRng, keys: &[SortKey]) -> Vec<SortValue> {
        keys.iter().map(|key| generate_value(generator, key.kind)).collect()
    }

    /// Every field kind and, three times in four, `_doc`, in a random order and random directions.
    fn generate_keys(generator: &mut StdRng) -> Vec<SortKey> {
        let mut kinds = FIELD_KINDS.to_vec();
        if generator.random_bool(0.75) {
            kinds.push(SortKeyKind::Doc);
        }
        kinds.shuffle(generator);
        kinds
            .into_iter()
            .map(|kind| SortKey {
                kind,
                order: if generator.random_bool(0.5) {
                    SortOrder::Asc
                } else {
                    SortOrder::Desc
                },
            })
            .collect()
    }

    /// The value `row` holds for the key of `kind`; `None` when no key has that kind.
    fn find_value<'r>(row: &'r [SortValue], keys: &[SortKey], kind: SortKeyKind) -> Option<&'r SortValue> {
        keys.iter().position(|key| key.kind == kind).map(|position| &row[position])
    }

    fn read_field_scalar(value: Option<&SortValue>) -> &ScalarValue {
        match value {
            Some(SortValue::Field(scalar)) => scalar,
            other => unreachable!("a field key holds {other:?}"),
        }
    }

    /// `rows` as a batch: the row id, one column per field kind, and the `_doc` columns (zero
    /// when no key is `_doc`).
    fn build_rows_batch(rows: &[Vec<SortValue>], keys: &[SortKey]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(ROW_ID_COLUMN, DataType::Int64, false),
            Field::new(FIELD_COLUMNS[0], DataType::Int64, true),
            Field::new(FIELD_COLUMNS[1], DataType::Utf8, true),
            Field::new(FIELD_COLUMNS[2], DataType::Float64, true),
            Field::new(DOC_MICROS_COLUMN, DataType::Int64, false),
            Field::new(DOC_HASH_COLUMN, DataType::UInt64, false),
        ]));
        let row_ids = Int64Array::from_iter_values((0..rows.len()).map(|row| i64::try_from(row).expect("row id")));
        let longs = Int64Array::from_iter(rows.iter().map(|row| {
            match read_field_scalar(find_value(row, keys, SortKeyKind::Long)) {
                ScalarValue::Int64(value) => *value,
                other => unreachable!("a long key holds {other:?}"),
            }
        }));
        let texts = StringArray::from_iter(rows.iter().map(|row| {
            match read_field_scalar(find_value(row, keys, SortKeyKind::Text)) {
                ScalarValue::Utf8(value) => value.clone(),
                other => unreachable!("a text key holds {other:?}"),
            }
        }));
        let doubles = Float64Array::from_iter(rows.iter().map(|row| {
            match read_field_scalar(find_value(row, keys, SortKeyKind::Double)) {
                ScalarValue::Float64(value) => *value,
                other => unreachable!("a double key holds {other:?}"),
            }
        }));
        let doc_parts: Vec<(i64, u64)> = rows
            .iter()
            .map(|row| match find_value(row, keys, SortKeyKind::Doc) {
                Some(SortValue::Doc {
                    micros_in_millis,
                    hash_low,
                }) => (*micros_in_millis, *hash_low),
                None => (0, 0),
                other => unreachable!("a _doc key holds {other:?}"),
            })
            .collect();
        let doc_micros = Int64Array::from_iter_values(doc_parts.iter().map(|(micros, _)| *micros));
        let doc_hashes = UInt64Array::from_iter_values(doc_parts.iter().map(|(_, hash_low)| *hash_low));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(row_ids),
                Arc::new(longs),
                Arc::new(texts),
                Arc::new(doubles),
                Arc::new(doc_micros),
                Arc::new(doc_hashes),
            ],
        )
        .expect("generated batch")
    }

    /// Per key, the column of the generated table its SQL expression reads.
    fn build_key_exprs(keys: &[SortKey]) -> Vec<Option<Expr>> {
        keys.iter()
            .map(|key| {
                Some(ident(match key.kind {
                    SortKeyKind::Long => FIELD_COLUMNS[0],
                    SortKeyKind::Text => FIELD_COLUMNS[1],
                    SortKeyKind::Double => FIELD_COLUMNS[2],
                    SortKeyKind::Doc => DOC_MICROS_COLUMN,
                    SortKeyKind::Hex(_) | SortKeyKind::Score => unreachable!("the generated keys are fields and _doc"),
                }))
            })
            .collect()
    }

    /// The ids of the rows `plan` answers, in answer order.
    async fn collect_row_ids(context: &SessionContext, plan: LogicalPlan) -> Vec<i64> {
        let batches = context
            .execute_logical_plan(plan)
            .await
            .expect("execute")
            .collect()
            .await
            .expect("collect");
        batches
            .iter()
            .flat_map(|selected| {
                let row_ids = selected.column(0).as_primitive::<Int64Type>().clone();
                (0..row_ids.len()).map(move |row| row_ids.value(row))
            })
            .collect()
    }

    /// The comparison of generated tuples, which have the key's type by construction.
    fn compare_generated_rows(left: &[SortValue], right: &[SortValue], keys: &[SortKey]) -> Ordering {
        compare_sort_values(left, right, keys).expect("generated values of one kind compare")
    }

    /// `compare_sort_values` is a total preorder (antisymmetric and transitive) whatever the
    /// directions, and puts a missing value after every present one in both directions.
    #[test]
    fn sort_value_comparison_is_a_total_preorder_with_missing_values_last() {
        let mut generator = StdRng::seed_from_u64(0x5047_A1E7);
        for _ in 0..20 {
            let keys = generate_keys(&mut generator);
            let rows: Vec<Vec<SortValue>> = (0..30).map(|_| generate_row(&mut generator, &keys)).collect();
            for left in &rows {
                for right in &rows {
                    assert_eq!(
                        compare_generated_rows(left, right, &keys),
                        compare_generated_rows(right, left, &keys).reverse(),
                        "{left:?} {right:?} {keys:?}"
                    );
                    for third in &rows {
                        if compare_generated_rows(left, right, &keys) != Ordering::Greater
                            && compare_generated_rows(right, third, &keys) != Ordering::Greater
                        {
                            assert_ne!(
                                compare_generated_rows(left, third, &keys),
                                Ordering::Greater,
                                "{left:?} {right:?} {third:?} {keys:?}"
                            );
                        }
                    }
                }
            }
        }
        for kind in FIELD_KINDS {
            for order in [SortOrder::Asc, SortOrder::Desc] {
                let keys = [SortKey { kind, order }];
                let missing_value = [SortValue::Field(
                    ScalarValue::try_new_null(&kind.build_data_type()).expect("typed null"),
                )];
                let present_values = match kind {
                    SortKeyKind::Long => vec![ScalarValue::Int64(Some(i64::MIN)), ScalarValue::Int64(Some(i64::MAX))],
                    SortKeyKind::Text => vec![ScalarValue::Utf8(Some(String::new()))],
                    _ => DOUBLE_VALUES.iter().map(|value| ScalarValue::Float64(Some(*value))).collect(),
                };
                for present_value in present_values {
                    assert_eq!(
                        compare_generated_rows(&missing_value, &[SortValue::Field(present_value.clone())], &keys),
                        Ordering::Greater,
                        "{present_value:?} {order:?}"
                    );
                }
            }
        }
    }

    /// The SQL the fetches are built from agrees with the Rust order over the whole key: sorted
    /// by the prefix terms then the tie terms (the row-hash bits of `_doc` included) and limited
    /// to `k` rows, a plan answers rows equal, key for key, to the first `k` rows of the Rust
    /// order; and the after predicate selects exactly the rows the Rust order puts strictly after
    /// the anchor. Missing values, `-0.0`, `+0.0` and `NaN` are among the values.
    #[tokio::test]
    async fn sql_ordering_limit_and_after_predicate_select_the_rows_the_sort_value_order_does() {
        let mut generator = StdRng::seed_from_u64(0x00AF_7E25);
        let mut partial_selection_rounds = 0;
        let mut missing_anchor_value_rounds = 0;
        let mut doc_rounds = 0;
        let context = SessionContext::new();
        for round in 0..40 {
            let keys = generate_keys(&mut generator);
            let rows: Vec<Vec<SortValue>> = (0..40).map(|_| generate_row(&mut generator, &keys)).collect();
            let anchor = if round % 2 == 0 {
                rows[generator.random_range(0..rows.len())].clone()
            } else {
                generate_row(&mut generator, &keys)
            };
            let limit = generator.random_range(1..=rows.len());
            let key_exprs = build_key_exprs(&keys);
            let doc_hash_expr = keys
                .iter()
                .any(|key| key.kind == SortKeyKind::Doc)
                .then(|| ident(DOC_HASH_COLUMN));
            let ordering = BranchOrdering {
                keys: &keys,
                key_exprs: &key_exprs,
                doc_hash_expr: doc_hash_expr.as_ref(),
            };
            let terms: Vec<OrderingTerm> = list_prefix_terms(&keys).into_iter().chain(list_tie_terms(&keys)).collect();
            let batch = build_rows_batch(&rows, &keys);
            let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memory table");
            let scan =
                LogicalPlanBuilder::from(context.read_table(Arc::new(table)).expect("read").into_unoptimized_plan());

            let sorted_plan = ordering
                .build_sorted_projection(scan.clone(), &terms)
                .expect("sorted projection")
                .builder
                .limit(0, Some(limit))
                .and_then(LogicalPlanBuilder::build)
                .expect("sorted plan");
            let sorted_row_ids = collect_row_ids(&context, sorted_plan).await;
            let after_plan = scan
                .filter(
                    ordering
                        .build_after_predicate(&bind_terms(&terms, &anchor).expect("anchor terms"))
                        .expect("after predicate"),
                )
                .and_then(LogicalPlanBuilder::build)
                .expect("after plan");
            let mut selected_row_ids = collect_row_ids(&context, after_plan).await;
            selected_row_ids.sort_unstable();

            let mut rust_order: Vec<usize> = (0..rows.len()).collect();
            rust_order.sort_by(|left, right| compare_generated_rows(&rows[*left], &rows[*right], &keys));
            assert_eq!(sorted_row_ids.len(), limit, "round {round}");
            for (sorted_row_id, rust_row) in sorted_row_ids.iter().zip(&rust_order) {
                let sorted_row = usize::try_from(*sorted_row_id).expect("row id");
                assert_eq!(
                    compare_generated_rows(&rows[sorted_row], &rows[*rust_row], &keys),
                    Ordering::Equal,
                    "round {round}: {keys:?}"
                );
            }
            let expected_row_ids: Vec<i64> = rows
                .iter()
                .enumerate()
                .filter(|(_, row)| compare_generated_rows(row, &anchor, &keys) == Ordering::Greater)
                .map(|(row, _)| i64::try_from(row).expect("row id"))
                .collect();
            assert_eq!(selected_row_ids, expected_row_ids, "round {round}: {anchor:?} {keys:?}");
            if !expected_row_ids.is_empty() && expected_row_ids.len() < rows.len() {
                partial_selection_rounds += 1;
            }
            if anchor
                .iter()
                .any(|value| matches!(value, SortValue::Field(scalar) if scalar.is_null()))
            {
                missing_anchor_value_rounds += 1;
            }
            if doc_hash_expr.is_some() {
                doc_rounds += 1;
            }
        }
        // The seed reaches the cases that matter: anchors that split the rows, anchors with a
        // missing value (after which only later keys can place a row), and sorts with `_doc`.
        assert!(partial_selection_rounds >= 10, "{partial_selection_rounds}");
        assert!(missing_anchor_value_rounds >= 5, "{missing_anchor_value_rounds}");
        assert!(doc_rounds >= 10, "{doc_rounds}");
    }

    /// Asserts that `error` renders as a 500 `exception`.
    async fn assert_internal_error(error: OpenSearchError, case: &str) {
        let (status, body) = render_envelope(error).await;
        assert_eq!(status.as_u16(), 500, "{case}");
        assert_eq!(body["error"]["type"], "exception", "{case}");
    }

    /// A broken invariant of the sort fails the request instead of ordering rows as if they were
    /// equal: values of different kinds, of types without a common order, a tuple shorter than
    /// the keys, and a projected sort column of another type than its key.
    #[tokio::test]
    async fn mismatched_sort_values_and_a_mistyped_sort_column_are_a_500() {
        let keys = [SortKey {
            kind: SortKeyKind::Long,
            order: SortOrder::Asc,
        }];
        let long_value = SortValue::Field(ScalarValue::Int64(Some(1)));
        let cases = [
            (
                "a field and a doc value",
                vec![long_value.clone()],
                vec![SortValue::Doc {
                    micros_in_millis: 0,
                    hash_low: 0,
                }],
            ),
            ("a field and a score", vec![long_value.clone()], vec![SortValue::Score]),
            (
                "a long and a text",
                vec![long_value.clone()],
                vec![SortValue::Field(ScalarValue::Utf8(Some("a".to_string())))],
            ),
            ("a tuple shorter than the keys", Vec::new(), vec![long_value.clone()]),
        ];
        for (case, left, right) in cases {
            let error = compare_sort_values(&left, &right, &keys).expect_err(case);
            assert_internal_error(error, case).await;
        }

        let text_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a"]))],
        )
        .expect("batch");
        let error = verify_sort_columns(&text_batch, &keys, &[Some(0)]).expect_err("a text column for a long key");
        assert_internal_error(error, "a text column for a long key").await;
        assert_eq!(
            verify_sort_columns(
                &text_batch,
                &[SortKey {
                    kind: SortKeyKind::Text,
                    order: SortOrder::Desc
                }],
                &[Some(0)]
            ),
            Ok(())
        );
    }

    /// A span-keyed row without its key would get an empty `_id` shared by every such row.
    #[tokio::test]
    async fn a_missing_null_or_mistyped_span_key_is_a_500() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("span_id", DataType::FixedSizeBinary(2), true),
                Field::new("label", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        [Some(vec![0xab_u8, 0x01]), None].into_iter(),
                        2,
                    )
                    .expect("span ids"),
                ),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("batch");

        assert_eq!(
            read_fixed_bytes(&batch, "span_id", Some(0), 0),
            Ok(&[0xab_u8, 0x01][..])
        );
        for (case, column_index, row) in [("absent", None, 0), ("NULL", Some(0), 1), ("not binary", Some(1), 0)] {
            let error = read_fixed_bytes(&batch, "span_id", column_index, row).expect_err(case);
            assert_internal_error(error, case).await;
        }
    }
}
