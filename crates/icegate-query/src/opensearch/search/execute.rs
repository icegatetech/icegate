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
//! most `from + size` rows, so an index yields at most three times that many candidates. A
//! candidate references its fetched batch and copies none of its values: its sort keys compare
//! straight from the batch's sort columns. Only a sort with `_doc` hashes every candidate in
//! Rust, on the blocking pool, since the key's hash bits order them; otherwise only the rows of
//! the returned page are hashed, for their `_id`, when the page renders. The pages of several
//! indices merge by the full key, and a tie goes to the index first in name order. Hits never
//! come from a union that DataFusion coerces: each hit renders its `_source` from its own index's
//! row. A hit's `sort` and `fields` values render with its candidate, because `fields` resolves
//! names through the session; the rest of the answer, `_id`s included, is linear in the page, so
//! it renders on the blocking pool.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{ArrowNativeType, DataType, Float64Type, Int64Type, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, Operator, SortExpr, binary_expr, cast, lit};
use datafusion::prelude::ident;
use datafusion::scalar::ScalarValue;
use futures::future::try_join_all;
use icegate_common::schema::{COL_SPAN_ID, COL_TIMESTAMP, COL_TRACE_ID};
use serde_json::{Map, Value, json};

use super::aggs::{
    AggregationKind, AggregationScope, AggregationTree, NamedAggregation, TermsOrder, TermsSpec, execute_aggregations,
    parse_aggregations, plan_aggregation_rows,
};
use super::doc_id::{
    CanonicalEncoder, DOC_HASH_BITS, DOC_HASH_MASK, MICROS_PER_DAY, decode_time_and_doc, document_row_hash_udf,
    encode_doc_sort_value, encode_document_id, encode_span_document_id, hash_canonical_row, hash_span_key,
    is_span_keyed, read_document_timestamp_micros, span_key_hash_udf,
};
use super::hits::{
    SearchHit, render_array_value, render_field_value, render_hits, render_search_response, render_sort_scalar,
    summarize_total_hits,
};
use super::request::{DocValueField, SearchRequest, SortClause, SortOrder, SourceFilter, TrackTotalHits};
use crate::engine::fixed_binary::decode_fixed_binary_literal;
use crate::error::QueryError;
use crate::opensearch::SearchLimitsConfig;
use crate::opensearch::catalog::{
    CrossRowField, DATA_PREPPER_SPAN_INDEX, EncodedField, FieldType, FieldValue, IndexEntry, Profile, ProfileTable,
    ResolvedField, TRACE_ID_FIELD, TenantSession, TraceRoots, TraceSelection, find_profile_table,
    lookup_page_trace_roots, lookup_trace_roots, native_column_type, render_derived_value, render_profile_field,
    select_candidate_traces,
};
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::glob::matches_glob;
use crate::opensearch::script::MetricScript;
use crate::opensearch::system_index::run_blocking_work;
use crate::query_dsl::ast::Query;
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
/// too_many_buckets_exception` past `limits.max_buckets`; and the errors of
/// [`prepare_cross_row_reads`].
pub(crate) async fn search_data_indices(
    session: &TenantSession,
    entries: &[&IndexEntry],
    request: &SearchRequest,
    context: &RequestContext,
    limits: &SearchLimitsConfig,
) -> Result<Value, OpenSearchError> {
    let aggregation_tree = request.aggregations.as_ref().map(parse_aggregations).transpose()?.map(Arc::new);
    prepare_cross_row_reads(session, entries, request, aggregation_tree.as_deref(), context.now).await?;
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
        entries,
        max_buckets: limits.max_buckets,
        max_result_window: limits.max_result_window,
        now: context.now,
    };
    let aggregations = execute_aggregations(rows, tree, &scope).await?;
    let Value::Object(body) = &mut response else {
        return Err(OpenSearchError::internal("a search response is a JSON object"));
    };
    body.insert("aggregations".to_string(), Value::Object(aggregations));
    Ok(response)
}

/// The predicate of the request's top-level `must` and `filter` clauses that read no cross-row
/// field of `scope.entry`. Dropping the other clauses, and every `should` and `must_not`, only
/// widens the rows, so the traces it selects include every trace the whole query selects.
///
/// # Errors
///
/// The lowering errors of [`lower_query`] for a kept clause.
fn lower_outer_predicate(query: &Query, scope: &LoweringScope<'_>) -> Result<Expr, OpenSearchError> {
    let conjuncts: Vec<&Query> = match query {
        Query::Bool(bool_query) => bool_query.must.iter().chain(&bool_query.filter).collect(),
        Query::ConstantScore(inner) => return lower_outer_predicate(inner, scope),
        // Every other query is one conjunct by the same rule.
        other => vec![other],
    };
    let mut lowered_conjuncts = Vec::with_capacity(conjuncts.len());
    for conjunct in conjuncts {
        let reads_before = scope.session.count_cross_row_reads();
        let predicate = lower_query(conjunct, scope);
        if scope.session.count_cross_row_reads() == reads_before {
            lowered_conjuncts.push(predicate?);
        }
    }
    Ok(lowered_conjuncts.into_iter().reduce(Expr::and).unwrap_or_else(|| lit(true)))
}

/// Lower the queries of every `filter` and `filters` aggregation of `aggregations`, and resolve
/// the sort fields of every `top_hits`, for the cross-row probe; the results are discarded.
fn probe_aggregation_reads(aggregations: &[NamedAggregation], scope: &LoweringScope<'_>) {
    for aggregation in aggregations {
        match &aggregation.kind {
            AggregationKind::Filter(query) => {
                let _ = lower_query(query, scope);
            }
            AggregationKind::Filters(spec) => {
                for query in spec.iter_queries() {
                    let _ = lower_query(query, scope);
                }
            }
            // A `top_hits` plans its sort only when its hits are collected, after the rows plan,
            // so the probe resolves it here.
            AggregationKind::TopHits(spec) => probe_sort_fields(&spec.request.sort, scope),
            // Each of these reads at most one field, which planning the rows resolves.
            AggregationKind::DateHistogram(_)
            | AggregationKind::Terms(_)
            | AggregationKind::Histogram(_)
            | AggregationKind::Avg(_)
            | AggregationKind::Sum(_)
            | AggregationKind::Min(_)
            | AggregationKind::Max(_)
            | AggregationKind::ValueCount(_)
            | AggregationKind::Stats(_)
            | AggregationKind::Cardinality(_)
            | AggregationKind::Percentiles(_)
            | AggregationKind::PercentileRanks(_)
            | AggregationKind::ExtendedStats(_)
            | AggregationKind::Range(_)
            | AggregationKind::DateRange(_)
            | AggregationKind::CumulativeSum(_)
            | AggregationKind::Derivative(_)
            | AggregationKind::SerialDiff(_)
            | AggregationKind::MovingFn(_)
            | AggregationKind::MovingAvg(_)
            | AggregationKind::BucketScript(_)
            | AggregationKind::AvgBucket(_)
            | AggregationKind::SumBucket(_)
            | AggregationKind::MinBucket(_)
            | AggregationKind::MaxBucket(_) => {}
        }
        probe_aggregation_reads(&aggregation.children, scope);
    }
}

/// Resolve the field of every field clause of `sort` in the scope's index, for the cross-row
/// probe; the resolutions are discarded.
fn probe_sort_fields(sort: &[SortClause], scope: &LoweringScope<'_>) {
    for clause in sort {
        if let SortClause::Field { field, .. } = clause {
            let _ = scope.session.resolve_field(scope.entry, field);
        }
    }
}

/// Whether the request reads a cross-row field of `entry`: lowering the query, resolving the sort
/// fields of the request and of every `top_hits`, planning the aggregation rows and lowering the
/// filter aggregations resolve every field the request names, and an unattached cross-row field
/// counts itself. Nothing is executed and every result is discarded.
fn probe_cross_row_reads(
    session: &TenantSession,
    entry: &IndexEntry,
    request: &SearchRequest,
    tree: Option<&AggregationTree>,
    now: DateTime<Utc>,
) -> bool {
    let reads_before = session.count_cross_row_reads();
    let scope = LoweringScope { session, entry, now };
    let _ = lower_query(&request.query, &scope);
    probe_sort_fields(&request.sort, &scope);
    if let Some(tree) = tree {
        let _ = plan_aggregation_rows(session, &[entry], &Query::MatchAll, now, tree);
        probe_aggregation_reads(&tree.aggregations, &scope);
    }
    session.count_cross_row_reads() != reads_before
}

/// Refuse a `terms` on `traceId` asking for more than `max` traces.
///
/// # Errors
///
/// Returns [`OpenSearchError::trace_terms_size_exceeded`] for the first such `terms`, parents
/// before their children.
fn check_trace_id_terms(aggregations: &[NamedAggregation], max: usize) -> Result<(), OpenSearchError> {
    for aggregation in aggregations {
        if let AggregationKind::Terms(spec) = &aggregation.kind
            && spec.field == TRACE_ID_FIELD
            && spec.size > max
        {
            return Err(OpenSearchError::trace_terms_size_exceeded(&aggregation.name, max));
        }
        check_trace_id_terms(&aggregation.children, max)?;
    }
    Ok(())
}

/// The key-ordered selection of a trace list: the search's only aggregation is a `terms` on
/// `traceId` ordered by `_key` that answers every key it counts (no `include`, `exclude` or
/// `missing`, `min_doc_count` at most 1, which counts every key). Its buckets are then exactly
/// the first `size` trace ids in key order, so only those traces' roots are needed.
fn find_trace_list_selection(tree: Option<&AggregationTree>) -> Option<TraceSelection> {
    let [aggregation] = tree?.aggregations.as_slice() else {
        return None;
    };
    let AggregationKind::Terms(spec) = &aggregation.kind else {
        return None;
    };
    // No `..`: a field added to `TermsSpec` stops this compiling until someone decides whether it
    // keeps the buckets equal to the first keys.
    let TermsSpec {
        field,
        size,
        order,
        include,
        exclude,
        missing,
        min_doc_count,
    } = spec;
    let [TermsOrder::Key(key_order)] = order.as_slice() else {
        return None;
    };
    let is_plain =
        field == TRACE_ID_FIELD && include.is_none() && exclude.is_none() && missing.is_none() && *min_doc_count <= 1;
    is_plain.then_some(TraceSelection::FirstByKey {
        size: *size,
        is_descending: matches!(key_order, SortOrder::Desc),
    })
}

/// Whether no clause of `sort` names a cross-row field (a field read from the trace's root).
fn is_sort_free_of_cross_row(sort: &[SortClause]) -> bool {
    !sort
        .iter()
        .any(|clause| matches!(clause, SortClause::Field { field, .. } if CrossRowField::find_by_name(field).is_some()))
}

/// The name of the first aggregation of `aggregations`, parents before their children, whose
/// metric runs the trace-list latency script.
fn find_trace_list_latency(aggregations: &[NamedAggregation]) -> Option<&str> {
    aggregations.iter().find_map(|aggregation| {
        let script = match &aggregation.kind {
            AggregationKind::Avg(spec)
            | AggregationKind::Sum(spec)
            | AggregationKind::Min(spec)
            | AggregationKind::Max(spec) => spec.value_script.as_ref(),
            AggregationKind::ExtendedStats(spec) => spec.metric.value_script.as_ref(),
            AggregationKind::Percentiles(spec) => spec.metric.value_script.as_ref(),
            // No other kind takes a value script.
            AggregationKind::DateHistogram(_)
            | AggregationKind::Terms(_)
            | AggregationKind::Histogram(_)
            | AggregationKind::ValueCount(_)
            | AggregationKind::Stats(_)
            | AggregationKind::Cardinality(_)
            | AggregationKind::PercentileRanks(_)
            | AggregationKind::Range(_)
            | AggregationKind::DateRange(_)
            | AggregationKind::Filter(_)
            | AggregationKind::Filters(_)
            | AggregationKind::TopHits(_)
            | AggregationKind::CumulativeSum(_)
            | AggregationKind::Derivative(_)
            | AggregationKind::SerialDiff(_)
            | AggregationKind::MovingFn(_)
            | AggregationKind::MovingAvg(_)
            | AggregationKind::BucketScript(_)
            | AggregationKind::AvgBucket(_)
            | AggregationKind::SumBucket(_)
            | AggregationKind::MinBucket(_)
            | AggregationKind::MaxBucket(_) => None,
        };
        if matches!(script, Some(MetricScript::TraceListLatency)) {
            Some(aggregation.name.as_str())
        } else {
            find_trace_list_latency(&aggregation.children)
        }
    })
}

/// Attach to each Data Prepper index of the search the trace roots its cross-row reads need.
///
/// Roots an earlier search on the same session attached (an earlier `_msearch` item) are
/// detached first, so the probe sees this search's own reads. Only when the probe finds a
/// cross-row read are the candidate traces selected and their roots looked up and attached, so
/// every later scan of the index carries the root columns. A trace list (see
/// [`find_trace_list_selection`]) whose query and sort read no cross-row field selects the first
/// `size` traces in key order under its whole query, however many traces the window holds; any
/// other search selects every trace of its outer predicate, at most `max_cross_row_traces`.
///
/// # Errors
///
/// Returns a 400 for the trace-list latency script in a search that also reads an index outside
/// the Data Prepper profile, for a `terms` on `traceId` larger than `max_cross_row_traces` and for
/// more candidate traces than that limit outside a trace list, the lowering errors of the
/// request's query (of its other `must` and `filter` clauses outside a trace list), and the
/// lookups' errors.
async fn prepare_cross_row_reads(
    session: &TenantSession,
    entries: &[&IndexEntry],
    request: &SearchRequest,
    tree: Option<&AggregationTree>,
    now: DateTime<Utc>,
) -> Result<(), OpenSearchError> {
    let settings = session.settings();
    // The latency script reads a root's duration, which only the Data Prepper profile computes.
    if let Some(name) = tree.and_then(|tree| find_trace_list_latency(&tree.aggregations))
        && let Some(entry) = entries.iter().find(|entry| entry.profile() != Profile::DataPrepper)
    {
        return Err(OpenSearchError::latency_script_on_foreign_index(
            name,
            entry.name(),
            DATA_PREPPER_SPAN_INDEX,
        ));
    }
    for entry in entries.iter().copied().filter(|entry| entry.profile() == Profile::DataPrepper) {
        session.detach_trace_roots(entry)?;
        if let Some(tree) = tree {
            check_trace_id_terms(&tree.aggregations, settings.max_cross_row_traces)?;
        }
        if !probe_cross_row_reads(session, entry, request, tree, now) {
            continue;
        }
        let scope = LoweringScope { session, entry, now };
        // A trace list's buckets are its first keys under the whole query, so the whole query
        // selects its traces. This holds only when neither the query nor the sort reads a root
        // field; otherwise the query itself needs the roots of traces outside the first keys.
        let trace_list = match find_trace_list_selection(tree) {
            Some(selection) if is_sort_free_of_cross_row(&request.sort) => {
                let reads_before = session.count_cross_row_reads();
                let full_query = lower_query(&request.query, &scope);
                (session.count_cross_row_reads() == reads_before).then_some((full_query, selection))
            }
            Some(_) | None => None,
        };
        let (outer, selection) = match trace_list {
            Some((full_query, selection)) => (full_query?, selection),
            None => (lower_outer_predicate(&request.query, &scope)?, TraceSelection::Bounded),
        };
        let candidates = select_candidate_traces(session, entry, outer, selection).await?;
        let roots = lookup_trace_roots(
            session,
            &candidates.trace_ids,
            candidates.window_start_micros,
            candidates.window_end_micros,
            settings.max_trace_lookback,
        )
        .await?;
        session.attach_trace_roots(entry, roots)?;
    }
    Ok(())
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
/// planning or execution failures; the errors of [`lookup_rendered_trace_roots`]; and `500` when a
/// row has no canonical encoding or no span key, a sort value has another type than its key, a
/// value has no JSON rendering, a field resolves to a column its hit's row does not hold, or the
/// rendering does not finish.
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
    let page: Vec<Candidate<'_>> = candidates.into_iter().skip(request.from).take(request.size).collect();
    let trace_roots = lookup_rendered_trace_roots(session, &page, &request.source).await?;
    let page_hits = page
        .into_iter()
        .map(|candidate| build_search_hit(session, candidate, &sort, &field_requests))
        .collect::<Result<Vec<PageHit>, OpenSearchError>>()?;
    // The work outlives the borrow of `request`, so it renders against its own copy.
    let rendering_request = request.clone();
    run_blocking_work("rendering data search hits", move || {
        render_page_hits(page_hits, |hits| {
            render_search_response(started_at.elapsed(), total, hits, &rendering_request, &trace_roots)
        })
    })
    .await
}

/// The hit of `candidate`: its index and row, its `sort` values when the request sorts, and its
/// `fields` when the request asks for any. Its `_id` is encoded when the page renders
/// ([`render_page_hits`]), so only the rows a page returns are hashed for one.
///
/// # Errors
///
/// The errors of [`render_sort_values`] and [`render_field_values`].
fn build_search_hit(
    session: &TenantSession,
    candidate: Candidate<'_>,
    sort: &SortPlan,
    field_requests: &FieldRequestPlan<'_>,
) -> Result<PageHit, OpenSearchError> {
    Ok(PageHit {
        sort_values: (!sort.keys.is_empty())
            .then(|| render_sort_values(&candidate, &sort.keys))
            .transpose()?,
        field_values: (!field_requests.requests.is_empty())
            .then(|| render_field_values(session, &candidate, field_requests))
            .transpose()?,
        index_name: candidate.entry.name().to_string(),
        profile_table: find_profile_table(candidate.entry),
        is_span_keyed: is_span_keyed(candidate.entry.table()),
        identity: candidate.identity,
        batch: candidate.batch,
        row: candidate.row,
    })
}

/// Encode the `_id` of every hit of `page_hits`, then `render` the hits as the renderer reads
/// them. A hit whose candidate was hashed for a `_doc` key reuses that hash; any other row is
/// hashed here, so a search hashes only the rows its page returns.
///
/// # Errors
///
/// The errors of [`IdentityColumns::encode_document_id`] and of `render`.
fn render_page_hits<T>(
    mut page_hits: Vec<PageHit>,
    render: impl FnOnce(&[SearchHit<'_>]) -> Result<T, OpenSearchError>,
) -> Result<T, OpenSearchError> {
    let mut encoder = CanonicalEncoder::new();
    let mut document_ids = Vec::with_capacity(page_hits.len());
    // The hits of one fetched batch are often adjacent, so its columns are found once per run.
    let mut batch_columns: Option<(&RecordBatch, IdentityColumns<'_>)> = None;
    for hit in &page_hits {
        let is_same_batch = batch_columns
            .as_ref()
            .is_some_and(|(batch, _)| std::ptr::eq(*batch, hit.batch.as_ref()));
        if !is_same_batch {
            batch_columns = None;
        }
        let (_, columns) = batch_columns.get_or_insert_with(|| {
            (
                hit.batch.as_ref(),
                IdentityColumns::from_batch(&hit.batch, hit.is_span_keyed),
            )
        });
        document_ids.push(columns.encode_document_id(&mut encoder, &hit.batch, hit.row, hit.identity)?);
    }
    let hits: Vec<SearchHit<'_>> = page_hits
        .iter_mut()
        .zip(&document_ids)
        .map(|(hit, document_id)| hit.take_search_hit(document_id))
        .collect();
    render(&hits)
}

/// The roots [`lookup_candidate_trace_roots`] answers for `candidates` when `source` renders a
/// `_source`. `_source` is the only reader of the roots, so for a `source` rendering none this
/// skips the lookup and answers no roots.
///
/// # Errors
///
/// The errors of [`lookup_candidate_trace_roots`].
async fn lookup_rendered_trace_roots(
    session: &TenantSession,
    candidates: &[Candidate<'_>],
    source: &SourceFilter,
) -> Result<TraceRoots, OpenSearchError> {
    if matches!(source, SourceFilter::Nothing) {
        return Ok(TraceRoots::new());
    }
    lookup_candidate_trace_roots(session, candidates).await
}

/// The root spans of the traces of `candidates`, which the `traceGroup*` fields of their hits
/// render from.
///
/// # Errors
///
/// The errors of [`lookup_page_trace_roots`].
async fn lookup_candidate_trace_roots(
    session: &TenantSession,
    candidates: &[Candidate<'_>],
) -> Result<TraceRoots, OpenSearchError> {
    let rows: Vec<(&IndexEntry, &RecordBatch, usize)> = candidates
        .iter()
        .map(|candidate| (candidate.entry, candidate.batch.as_ref(), candidate.row))
        .collect();
    lookup_page_trace_roots(session, &rows).await
}

/// The `_search` hits of `batches`, rows of `entry` that a plan other than `_search`'s read: in
/// each batch the first `table_column_count` columns are the index's table columns in scan
/// order, followed by one column per key of `lower_hit_sort(session, entry, &request.sort)`, in
/// key order; later columns are ignored. Each hit is the one `_search` renders for the row under
/// `request` (`_index`, `_id`, `_score`, `_source`, `fields`, `sort`), one per row, in batch then
/// row order. The `traceGroup*` fields of a Data Prepper hit render from the roots of the hits'
/// own traces. The hits render on the blocking pool, `_id`s included; the caller bounds how many
/// rows `batches` holds.
///
/// # Errors
///
/// A 400 naming an index without a document-id rule; the sort and `fields` errors of `_search`;
/// the errors of [`lookup_rendered_trace_roots`]; `500` for a `_doc` sort clause, a batch
/// narrower than its table and sort columns, a row without a canonical encoding or a span key,
/// a value no JSON rendering exists for, and a rendering that does not finish.
pub(super) async fn render_index_rows_as_hits(
    session: &TenantSession,
    entry: &IndexEntry,
    request: &SearchRequest,
    batches: &[RecordBatch],
    table_column_count: usize,
) -> Result<Vec<Value>, OpenSearchError> {
    require_document_identity(session, entry)?;
    let entries = [entry];
    let sort = plan_sort(session, &entries, &request.sort)?;
    let field_requests = plan_field_requests(session, &entries, request)?;
    // Each key that orders in the plan reads the next column after the table columns.
    let mut sort_column_positions = table_column_count..;
    let sort_columns = sort
        .keys
        .iter()
        .map(|key| match key.kind {
            SortKeyKind::Score => Ok(None),
            SortKeyKind::Long | SortKeyKind::Double | SortKeyKind::Text | SortKeyKind::Hex(_) => {
                Ok(sort_column_positions.next())
            }
            SortKeyKind::Doc => Err(OpenSearchError::internal(
                "a _doc order is decided outside the query plan, so no plan carries its column",
            )),
        })
        .collect::<Result<Vec<Option<usize>>, OpenSearchError>>()?;
    let column_count = table_column_count + sort_columns.iter().flatten().count();
    if batches.iter().any(|batch| batch.num_columns() < column_count) {
        return Err(OpenSearchError::internal(format!(
            "the rows of [{}] hold fewer columns than their {column_count} table and sort columns",
            entry.name()
        )));
    }
    let scope = BranchScope {
        session,
        entry,
        position: 0,
        sort: &sort,
        anchor: None,
    };
    let fetched_batches = split_fetched_batches(&sort.keys, batches, table_column_count, &sort_columns)?;
    // No key is `_doc`, so no row is hashed for its sort value.
    let candidates = build_candidates(scope, fetched_batches, None)?;
    let trace_roots = lookup_rendered_trace_roots(session, &candidates, &request.source).await?;
    let page_hits = candidates
        .into_iter()
        .map(|candidate| build_search_hit(session, candidate, &sort, &field_requests))
        .collect::<Result<Vec<PageHit>, OpenSearchError>>()?;
    // The work outlives the borrow of `request`, so it renders against its own copy.
    let rendering_request = request.clone();
    run_blocking_work("rendering index rows as hits", move || {
        render_page_hits(page_hits, |hits| render_hits(hits, &rendering_request, &trace_roots))
    })
    .await
}

/// One hit of the returned page, owning what its rendering reads, so it moves to the blocking
/// pool.
struct PageHit {
    /// The name of the index the row was read from.
    index_name: String,
    /// The field table of that index when it serves a non-native profile.
    profile_table: Option<&'static ProfileTable>,
    /// Whether the index keys its rows by their span key instead of a timestamp and hash.
    is_span_keyed: bool,
    /// The row's microsecond and hash when a `_doc` key read them; `None` otherwise.
    identity: Option<RowIdentity>,
    /// The table columns of the batch holding the row.
    batch: Arc<RecordBatch>,
    /// The row within `batch`.
    row: usize,
    /// The hit's `sort` values; `None` when the request does not sort.
    sort_values: Option<Vec<Value>>,
    /// The hit's `fields`; `None` when the request asks for none.
    field_values: Option<Map<String, Value>>,
}

impl PageHit {
    /// The hit as the renderer reads it, with `document_id` as its `_id`, borrowing its index and
    /// row. Its `sort` and `fields` values move out into the returned hit, so a second call yields
    /// neither.
    fn take_search_hit<'h>(&'h mut self, document_id: &'h str) -> SearchHit<'h> {
        SearchHit {
            index: &self.index_name,
            batch: &self.batch,
            row: self.row,
            document_id,
            sort_values: self.sort_values.take(),
            field_values: self.field_values.take(),
            profile_table: self.profile_table,
        }
    }
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

/// A `_doc` key's value: the microsecond within the millisecond, then the low [`DOC_HASH_BITS`]
/// row-hash bits, compared in that order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct DocKey {
    /// The microsecond within the millisecond, `0..1000`.
    micros_in_millis: i64,
    /// The low [`DOC_HASH_BITS`] bits of the row hash.
    hash_low: u64,
}

/// What a row's `_id` and `_doc` value are computed from.
#[derive(Debug, Clone, Copy)]
struct RowIdentity {
    /// The row's timestamp in epoch microseconds; `0` for a row without one.
    timestamp_micros: i64,
    /// The row's hash: canonical, or of the span key on span-keyed tables.
    row_hash: u64,
}

impl RowIdentity {
    /// The row's `_doc` key value.
    const fn read_doc_key(self) -> DocKey {
        DocKey {
            micros_in_millis: self.timestamp_micros.rem_euclid(1_000),
            hash_low: self.row_hash & DOC_HASH_MASK,
        }
    }
}

/// One sort-key value as the comparisons read it, borrowed from the array or scalar holding it,
/// so comparing two rows copies no value.
#[derive(Debug, Clone, Copy)]
enum KeyValue<'v> {
    /// A missing field value, which sorts after every present one in either direction.
    Missing,
    /// A long key's value.
    Long(i64),
    /// A double key's value, compared in IEEE `totalOrder`.
    Double(f64),
    /// A keyword key's value, compared by bytes.
    Text(&'v str),
    /// A hex id key's value, compared by bytes.
    Bytes(&'v [u8]),
    /// A `_doc` key's value.
    Doc(DocKey),
    /// `_score`: every row scores alike.
    Score,
}

/// One row's value for each sort key, read in place.
trait SortKeyRow {
    /// How many sort-key values the row holds.
    fn count_values(&self) -> usize;

    /// The value of the key at `position`, which sorts as `kind`.
    ///
    /// # Errors
    ///
    /// Returns `500` when the row holds no value of `kind` at `position`.
    fn read_key_value(&self, position: usize, kind: SortKeyKind) -> Result<KeyValue<'_>, OpenSearchError>;
}

/// A `search_after` anchor, one value per sort key.
impl SortKeyRow for [SortValue] {
    fn count_values(&self) -> usize {
        self.len()
    }

    fn read_key_value(&self, position: usize, kind: SortKeyKind) -> Result<KeyValue<'_>, OpenSearchError> {
        let value = self
            .get(position)
            .ok_or_else(|| OpenSearchError::internal("a sort key was read past the row's values"))?;
        match (kind, value) {
            (SortKeyKind::Score, SortValue::Score) => Ok(KeyValue::Score),
            (
                SortKeyKind::Doc,
                SortValue::Doc {
                    micros_in_millis,
                    hash_low,
                },
            ) => Ok(KeyValue::Doc(DocKey {
                micros_in_millis: *micros_in_millis,
                hash_low: *hash_low,
            })),
            (
                SortKeyKind::Long | SortKeyKind::Double | SortKeyKind::Text | SortKeyKind::Hex(_),
                SortValue::Field(scalar),
            ) => read_scalar_key_value(scalar, kind),
            (_, SortValue::Field(_) | SortValue::Doc { .. } | SortValue::Score) => Err(OpenSearchError::internal(
                format!("a sort value of another kind was read for a [{}] key", kind.name()),
            )),
        }
    }
}

/// The value of a key of `kind` held by `scalar`.
///
/// # Errors
///
/// Returns `500` for a present value of another type than the key's.
fn read_scalar_key_value(scalar: &ScalarValue, kind: SortKeyKind) -> Result<KeyValue<'_>, OpenSearchError> {
    match (kind, scalar) {
        (_, missing_value) if missing_value.is_null() => Ok(KeyValue::Missing),
        (SortKeyKind::Long, ScalarValue::Int64(Some(value))) => Ok(KeyValue::Long(*value)),
        (SortKeyKind::Double, ScalarValue::Float64(Some(value))) => Ok(KeyValue::Double(*value)),
        (SortKeyKind::Text, ScalarValue::Utf8(Some(text))) => Ok(KeyValue::Text(text)),
        (SortKeyKind::Hex(_), ScalarValue::FixedSizeBinary(_, Some(bytes))) => Ok(KeyValue::Bytes(bytes)),
        (_, unexpected_value) => Err(OpenSearchError::internal(format!(
            "a sort value of the type {} was read for a [{}] key",
            unexpected_value.data_type(),
            kind.name()
        ))),
    }
}

/// The value of a key of `kind` at `row` of `array`, a fetched sort column.
///
/// # Errors
///
/// Returns `500` for a column of another type than the key's, and for a `_doc` or `_score` key,
/// whose values no column holds.
fn read_array_key_value(array: &dyn Array, row: usize, kind: SortKeyKind) -> Result<KeyValue<'_>, OpenSearchError> {
    if array.is_null(row) {
        return Ok(KeyValue::Missing);
    }
    let value = match kind {
        SortKeyKind::Long => array
            .as_primitive_opt::<Int64Type>()
            .map(|values| KeyValue::Long(values.value(row))),
        SortKeyKind::Double => array
            .as_primitive_opt::<Float64Type>()
            .map(|values| KeyValue::Double(values.value(row))),
        SortKeyKind::Text => array.as_string_opt::<i32>().map(|values| KeyValue::Text(values.value(row))),
        SortKeyKind::Hex(_) => array
            .as_fixed_size_binary_opt()
            .map(|values| KeyValue::Bytes(values.value(row))),
        SortKeyKind::Doc | SortKeyKind::Score => None,
    };
    value.ok_or_else(|| {
        OpenSearchError::internal(format!(
            "a sort column of the type {} was read for a [{}] key",
            array.data_type(),
            kind.name()
        ))
    })
}

/// The sort-key values of one fetched row, read in place from its batch's sort columns.
#[derive(Debug, Clone, Copy)]
struct ArraySortKeys<'c> {
    /// Per sort key, its column in the fetched batch; `None` for `_doc` and `_score`, which no
    /// column holds.
    columns: &'c [Option<ArrayRef>],
    /// The row within the columns.
    row: usize,
    /// The row's microsecond and hash, which a `_doc` key reads; `None` when no key is `_doc`.
    identity: Option<RowIdentity>,
}

impl SortKeyRow for ArraySortKeys<'_> {
    fn count_values(&self) -> usize {
        self.columns.len()
    }

    fn read_key_value(&self, position: usize, kind: SortKeyKind) -> Result<KeyValue<'_>, OpenSearchError> {
        match kind {
            SortKeyKind::Score => Ok(KeyValue::Score),
            SortKeyKind::Doc => self
                .identity
                .map(|identity| KeyValue::Doc(identity.read_doc_key()))
                .ok_or_else(|| OpenSearchError::internal("a row sorted on _doc was not hashed")),
            SortKeyKind::Long | SortKeyKind::Double | SortKeyKind::Text | SortKeyKind::Hex(_) => {
                let column = self
                    .columns
                    .get(position)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| OpenSearchError::internal("a sort key other than _score has no sort column"))?;
                read_array_key_value(column.as_ref(), self.row, kind)
            }
        }
    }
}

impl ArraySortKeys<'_> {
    /// The row's value for each of `keys`, copied out of the columns: the boundary of a two-phase
    /// top-K, which the next fetch's predicate binds as literals.
    ///
    /// # Errors
    ///
    /// Returns `500` when a key has no column, a `_doc` key no identity, or a value cannot be read.
    fn copy_sort_values(&self, keys: &[SortKey]) -> Result<Vec<SortValue>, OpenSearchError> {
        keys.iter()
            .enumerate()
            .map(|(position, key)| match self.read_key_value(position, key.kind)? {
                KeyValue::Score => Ok(SortValue::Score),
                KeyValue::Doc(DocKey {
                    micros_in_millis,
                    hash_low,
                }) => Ok(SortValue::Doc {
                    micros_in_millis,
                    hash_low,
                }),
                KeyValue::Missing
                | KeyValue::Long(_)
                | KeyValue::Double(_)
                | KeyValue::Text(_)
                | KeyValue::Bytes(_) => self.copy_field_value(position).map(SortValue::Field),
            })
            .collect()
    }

    /// The value of the field key at `position`, copied out of its column.
    ///
    /// # Errors
    ///
    /// Returns `500` when the key has no column or its value cannot be read.
    fn copy_field_value(&self, position: usize) -> Result<ScalarValue, OpenSearchError> {
        let column = self
            .columns
            .get(position)
            .and_then(Option::as_ref)
            .ok_or_else(|| OpenSearchError::internal("a sort key other than _score has no sort column"))?;
        ScalarValue::try_from_array(column.as_ref(), self.row).map_err(convert_plan_error)
    }
}

/// Whether `left` and `right` hold equal values for every term of `terms`, a missing value equal
/// to a missing one.
///
/// # Errors
///
/// Returns the errors of [`SortKeyRow::read_key_value`] and [`compare_term_values`], and `500` for
/// a term naming a key `keys` lacks.
fn has_equal_terms(
    left: &impl SortKeyRow,
    right: &impl SortKeyRow,
    terms: &[OrderingTerm],
    keys: &[SortKey],
) -> Result<bool, OpenSearchError> {
    for term in terms {
        let key = keys
            .get(term.key)
            .ok_or_else(|| OpenSearchError::internal("an ordering term names a sort key the plan lacks"))?;
        let ordering = compare_term_values(
            left.read_key_value(term.key, key.kind)?,
            right.read_key_value(term.key, key.kind)?,
            term.part,
        )?;
        if ordering != Ordering::Equal {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Two values of one key compared on `part` of it, ascending.
///
/// # Errors
///
/// Returns the errors of [`compare_key_values`], and `500` for a `_doc` part of a value that is no
/// `_doc` value.
fn compare_term_values(left: KeyValue<'_>, right: KeyValue<'_>, part: KeyPart) -> Result<Ordering, OpenSearchError> {
    match (part, left, right) {
        (KeyPart::FieldValue, _, _) => compare_key_values(left, right, SortOrder::Asc),
        (KeyPart::DocMicros, KeyValue::Doc(left), KeyValue::Doc(right)) => {
            Ok(left.micros_in_millis.cmp(&right.micros_in_millis))
        }
        (KeyPart::DocHash, KeyValue::Doc(left), KeyValue::Doc(right)) => Ok(left.hash_low.cmp(&right.hash_low)),
        (KeyPart::DocMicros | KeyPart::DocHash, _, _) => Err(OpenSearchError::internal(
            "a _doc ordering part was read from a value of another kind",
        )),
    }
}

/// The kind a resolved field sorts as in one index.
///
/// # Errors
///
/// Returns the query-shard 400 for a text or object field, which has no sort value, and
/// `illegal_argument` for a cross-row field whose index has no trace roots attached.
fn decide_field_kind(field: &ResolvedField, entry: &IndexEntry) -> Result<SortKeyKind, OpenSearchError> {
    match (field.field_type, &field.value) {
        // A search attaches the roots of an index whose sort names a cross-row field before it
        // plans the sort, and the field then resolves to its joined root column, so only a path
        // that does not attach reaches this.
        (_, FieldValue::CrossRow(_)) => Err(OpenSearchError::illegal_argument(format!(
            "field [{}] is read from the root span of each trace; sort on it in a search of its index",
            field.name
        ))),
        (FieldType::Date | FieldType::Integer | FieldType::Long | FieldType::Boolean, _) => Ok(SortKeyKind::Long),
        (FieldType::Double, _) => Ok(SortKeyKind::Double),
        // An encoded hex id sorts as the bytes it renders in hex, every other encoded or derived
        // keyword as its rendered text, which `build_read_expr` reads.
        (
            FieldType::Keyword,
            FieldValue::HexId { width, .. } | FieldValue::Encoded(EncodedField::HexIdOrEmpty { width, .. }),
        ) => Ok(SortKeyKind::Hex(*width)),
        (
            FieldType::Keyword,
            FieldValue::Column { .. } | FieldValue::MapKey { .. } | FieldValue::Encoded(_) | FieldValue::Derived(_),
        ) => Ok(SortKeyKind::Text),
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

/// The width of a hex-id field, stored or encoded; `None` for any other field.
const fn find_hex_width(field: &ResolvedField) -> Option<i32> {
    match field.value {
        FieldValue::HexId { width, .. } | FieldValue::Encoded(EncodedField::HexIdOrEmpty { width, .. }) => Some(width),
        FieldValue::Column { .. }
        | FieldValue::MapKey { .. }
        | FieldValue::Encoded(_)
        | FieldValue::Derived(_)
        | FieldValue::CrossRow(_) => None,
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

/// One key of a hit order over the rows of one index.
pub(super) struct HitSortKey {
    /// The key's sort value as `_search` computes it for the index: a date as epoch
    /// milliseconds, an integer, a boolean as 0 or 1, a double, a keyword, an id as its bytes.
    pub(super) value: Expr,
    /// Whether the key orders ascending; a missing value sorts last in either direction.
    pub(super) is_ascending: bool,
}

/// The keys of `clauses` over the rows of `entry`, in priority order, as `_search` orders that
/// index's hits. A `_score` clause yields no key: every row scores alike.
///
/// # Errors
///
/// The sort errors of `_search` (a field the index does not map and no `unmapped_type` names, a
/// field that is not sortable); `500` for a `_doc` clause, whose order is decided outside the
/// query plan, so no caller may pass one.
pub(super) fn lower_hit_sort(
    session: &TenantSession,
    entry: &IndexEntry,
    clauses: &[SortClause],
) -> Result<Vec<HitSortKey>, OpenSearchError> {
    let SortPlan { keys, branch_exprs, .. } = plan_sort(session, &[entry], clauses)?;
    let key_expressions = branch_exprs
        .into_iter()
        .next()
        .ok_or_else(|| OpenSearchError::internal("a sort over one index planned no expressions for it"))?;
    keys.into_iter()
        .zip(key_expressions)
        .filter_map(|(key, key_expression)| match key.kind {
            SortKeyKind::Score => None,
            SortKeyKind::Doc => Some(Err(OpenSearchError::internal(
                "a _doc order is decided outside the query plan, so it cannot order hits there",
            ))),
            SortKeyKind::Long | SortKeyKind::Double | SortKeyKind::Text | SortKeyKind::Hex(_) => Some(
                key_expression
                    .map(|value| HitSortKey {
                        value,
                        is_ascending: matches!(key.order, SortOrder::Asc),
                    })
                    .ok_or_else(|| OpenSearchError::internal("a field sort key has no SQL expression")),
            ),
        })
        .collect()
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
/// from [`OpenSearchError::unordered_missing_id`] for an encoded id whose stored column admits
/// NULL, and from [`classify_unmapped_type`] for an `unmapped_type` it refuses, whether or not an
/// index lacks the field; the error an index's table failed to open with; and `500` when a sort
/// expression cannot be built.
fn plan_field_sort(
    session: &TenantSession,
    entries: &[&IndexEntry],
    field: &str,
    unmapped_type: Option<&str>,
) -> Result<(SortKeyKind, Vec<Expr>), OpenSearchError> {
    let unmapped_kind = unmapped_type.map(classify_unmapped_type).transpose()?;
    // A field sorts the way a filter reads it: a profile time as its stored date column (its sort
    // value in epoch milliseconds), an escaped attribute key as its stored MAP key.
    let resolutions: Vec<Option<ResolvedField>> = entries
        .iter()
        .map(|entry| session.resolve_field(entry, field).map(ResolvedField::into_lowering_view))
        .collect();
    let mut first_resolution: Option<(SortKeyKind, &ResolvedField, &str)> = None;
    for (entry, resolution) in entries.iter().zip(&resolutions) {
        match resolution {
            Some(resolved_field) => {
                if session.is_nullable_encoded_id(entry, resolved_field)? {
                    return Err(OpenSearchError::unordered_missing_id(field, entry.name(), "sort"));
                }
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

/// One row a page may return, with what ordering and rendering it need. It references its
/// fetched batch and copies none of its values: sort keys compare straight from the batch's sort
/// columns.
struct Candidate<'a> {
    /// The index the row was read from.
    entry: &'a IndexEntry,
    /// That index's position in the request.
    position: usize,
    /// The row's table columns (no sort columns), shared by the rows of one fetched batch.
    batch: Arc<RecordBatch>,
    /// Per sort key, its column in the fetched batch, shared by the rows of that batch; `None`
    /// for `_doc` and `_score`, which no column holds.
    sort_columns: Arc<[Option<ArrayRef>]>,
    /// The row within `batch` and `sort_columns`.
    row: usize,
    /// The row's microsecond and hash, which a `_doc` key reads; `None` when no key is `_doc`.
    identity: Option<RowIdentity>,
}

impl Candidate<'_> {
    /// The row's sort-key values, read in place.
    fn read_sort_keys(&self) -> ArraySortKeys<'_> {
        ArraySortKeys {
            columns: &self.sort_columns,
            row: self.row,
            identity: self.identity,
        }
    }
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
            if compare_sort_values(&candidate.read_sort_keys(), anchor, &scope.sort.keys)? == Ordering::Greater {
                candidates.push(candidate);
            }
        }
    }
    let mut selected_candidates = fetch_rows(scope, matching_plan, after_filter.clone(), &prefix_terms, window).await?;
    if !tie_terms.is_empty() && selected_candidates.len() == window {
        // Two-phase top-K: the rows tied with the last one on the prefix terms make way for the
        // first `window` of all such rows in the order of the remaining terms. The last row is
        // itself tied with itself, so the tie fetch answers it again.
        let boundary_candidate = selected_candidates
            .pop()
            .ok_or_else(|| OpenSearchError::internal("a full fetch holds no last row"))?;
        let boundary_keys = boundary_candidate.read_sort_keys();
        let boundary = bind_terms(&prefix_terms, &boundary_keys.copy_sort_values(&scope.sort.keys)?)?;
        let mut candidates_before_boundary = Vec::with_capacity(window);
        for candidate in selected_candidates {
            if !has_equal_terms(
                &candidate.read_sort_keys(),
                &boundary_keys,
                &prefix_terms,
                &scope.sort.keys,
            )? {
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

/// Where a batch of an index's table columns holds what its rows' `_id` and `_doc` value read,
/// found once per batch.
struct IdentityColumns<'b> {
    /// The names of the table columns, in schema order, which the canonical hash encodes.
    column_names: Vec<&'b str>,
    /// The position of the timestamp column, if the table has one.
    timestamp_index: Option<usize>,
    /// The position of the trace-id column, if the table has one.
    trace_id_index: Option<usize>,
    /// The position of the span-id column, if the table has one.
    span_id_index: Option<usize>,
    /// Whether the rows are keyed by `trace_id`‖`span_id` instead of a timestamp and hash.
    is_span_keyed: bool,
}

impl<'b> IdentityColumns<'b> {
    /// The identity columns of `rows`, a batch of an index's table columns; `is_span_keyed` tells
    /// whether the index keys its rows by their span key.
    fn from_batch(rows: &'b RecordBatch, is_span_keyed: bool) -> Self {
        let schema = rows.schema_ref();
        Self {
            column_names: schema.fields().iter().map(|field| field.name().as_str()).collect(),
            timestamp_index: schema.index_of(COL_TIMESTAMP).ok(),
            trace_id_index: schema.index_of(COL_TRACE_ID).ok(),
            span_id_index: schema.index_of(COL_SPAN_ID).ok(),
            is_span_keyed,
        }
    }

    /// The microsecond and hash of `rows[row]`: the span key's hash on span-keyed tables, the
    /// canonical row's otherwise, hashed with `encoder`.
    ///
    /// # Errors
    ///
    /// Returns `500` when the row has no canonical encoding or a span-keyed row has no span key.
    fn read_identity(
        &self,
        encoder: &mut CanonicalEncoder,
        rows: &RecordBatch,
        row: usize,
    ) -> Result<RowIdentity, OpenSearchError> {
        let timestamp_micros = self.timestamp_index.map_or(0, |index| {
            read_document_timestamp_micros(rows.column(index).as_ref(), row)
        });
        let row_hash = if self.is_span_keyed {
            let (trace_id, span_id) = self.read_span_key(rows, row)?;
            hash_span_key(trace_id, span_id)
        } else {
            hash_canonical_row(encoder, &self.column_names, rows, row)?
        };
        Ok(RowIdentity {
            timestamp_micros,
            row_hash,
        })
    }

    /// The `_id` of `rows[row]`: its span key on span-keyed tables, its microsecond and hash
    /// otherwise, from `identity` when the row was already hashed.
    ///
    /// # Errors
    ///
    /// The errors of [`Self::read_identity`].
    fn encode_document_id(
        &self,
        encoder: &mut CanonicalEncoder,
        rows: &RecordBatch,
        row: usize,
        identity: Option<RowIdentity>,
    ) -> Result<String, OpenSearchError> {
        if self.is_span_keyed {
            let (trace_id, span_id) = self.read_span_key(rows, row)?;
            return Ok(encode_span_document_id(trace_id, span_id));
        }
        let identity = match identity {
            Some(identity) => identity,
            None => self.read_identity(encoder, rows, row)?,
        };
        Ok(encode_document_id(identity.timestamp_micros, identity.row_hash))
    }

    /// The trace id and span id of `rows[row]`.
    ///
    /// # Errors
    ///
    /// The errors of [`read_fixed_bytes`].
    fn read_span_key<'r>(&self, rows: &'r RecordBatch, row: usize) -> Result<(&'r [u8], &'r [u8]), OpenSearchError> {
        Ok((
            read_fixed_bytes(rows, COL_TRACE_ID, self.trace_id_index, row)?,
            read_fixed_bytes(rows, COL_SPAN_ID, self.span_id_index, row)?,
        ))
    }
}

/// One fetched batch, split into what its candidates read.
struct FetchedBatch {
    /// The table columns.
    rows: Arc<RecordBatch>,
    /// Per sort key, its column; `None` for `_doc` and `_score`, which no column holds.
    sort_columns: Arc<[Option<ArrayRef>]>,
}

/// Fetch the first `limit` rows of `matching_plan` that also pass `filter`, ordered by `terms`.
/// When a sort key is `_doc`, every fetched row is hashed for it on the blocking pool.
///
/// # Errors
///
/// Returns planning or execution failures, and the errors of
/// [`BranchOrdering::build_sorted_projection`], [`split_fetched_batches`],
/// [`read_candidate_identities`] and [`build_candidates`].
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
    let fetched_batches = split_fetched_batches(&scope.sort.keys, &batches, table_column_count, &sort_columns)?;
    let identities = read_candidate_identities(scope, &fetched_batches).await?;
    build_candidates(scope, fetched_batches, identities)
}

/// Each of `batches` split into its table columns, the first `table_column_count`, and per sort
/// key the column at its position in `sort_columns`; a `_doc` key reads no column.
///
/// # Errors
///
/// Returns the errors of [`verify_sort_columns`], and `500` when a batch has fewer columns than
/// `table_column_count`.
fn split_fetched_batches(
    keys: &[SortKey],
    batches: &[RecordBatch],
    table_column_count: usize,
    sort_columns: &[Option<usize>],
) -> Result<Vec<FetchedBatch>, OpenSearchError> {
    let table_indices: Vec<usize> = (0..table_column_count).collect();
    batches
        .iter()
        .map(|batch| {
            verify_sort_columns(batch, keys, sort_columns)?;
            let rows = batch
                .project(&table_indices)
                .map_err(|error| convert_plan_error(error.into()))?;
            let key_columns = keys
                .iter()
                .zip(sort_columns)
                .map(|(key, column)| match key.kind {
                    SortKeyKind::Doc | SortKeyKind::Score => None,
                    SortKeyKind::Long | SortKeyKind::Double | SortKeyKind::Text | SortKeyKind::Hex(_) => {
                        column.map(|column| Arc::clone(batch.column(column)))
                    }
                })
                .collect();
            Ok(FetchedBatch {
                rows: Arc::new(rows),
                sort_columns: key_columns,
            })
        })
        .collect()
}

/// The identity of every row of `fetched_batches`, in batch then row order, when a sort key of
/// `scope` is `_doc`, whose value is the row's microsecond and hash bits; `None` otherwise, so a
/// sort without `_doc` hashes no candidate. Hashing reads every column of every row, MAP
/// attributes included, so it runs on the blocking pool.
///
/// # Errors
///
/// The errors of [`IdentityColumns::read_identity`], and `500` when the hashing does not finish.
async fn read_candidate_identities(
    scope: BranchScope<'_>,
    fetched_batches: &[FetchedBatch],
) -> Result<Option<Vec<RowIdentity>>, OpenSearchError> {
    if !scope.sort.keys.iter().any(|key| key.kind == SortKeyKind::Doc) {
        return Ok(None);
    }
    let is_span_keyed = is_span_keyed(scope.entry.table());
    let batches: Vec<Arc<RecordBatch>> = fetched_batches
        .iter()
        .map(|fetched_batch| Arc::clone(&fetched_batch.rows))
        .collect();
    run_blocking_work("hashing candidate rows", move || {
        let mut encoder = CanonicalEncoder::new();
        let mut identities = Vec::with_capacity(batches.iter().map(|rows| rows.num_rows()).sum());
        for rows in &batches {
            let columns = IdentityColumns::from_batch(rows, is_span_keyed);
            for row in 0..rows.num_rows() {
                identities.push(columns.read_identity(&mut encoder, rows, row)?);
            }
        }
        Ok(identities)
    })
    .await
    .map(Some)
}

/// The candidate of every row of `fetched_batches`, in batch then row order, each carrying its
/// identity from `identities` (in the same order) when the sort read one.
///
/// # Errors
///
/// Returns `500` when `identities` holds fewer identities than the batches hold rows.
fn build_candidates(
    scope: BranchScope<'_>,
    fetched_batches: Vec<FetchedBatch>,
    identities: Option<Vec<RowIdentity>>,
) -> Result<Vec<Candidate<'_>>, OpenSearchError> {
    let mut identities = identities.map(Vec::into_iter);
    let mut candidates =
        Vec::with_capacity(fetched_batches.iter().map(|fetched_batch| fetched_batch.rows.num_rows()).sum());
    for fetched_batch in fetched_batches {
        for row in 0..fetched_batch.rows.num_rows() {
            let identity = identities
                .as_mut()
                .map(|remaining| {
                    remaining
                        .next()
                        .ok_or_else(|| OpenSearchError::internal("a fetched row was not hashed"))
                })
                .transpose()?;
            candidates.push(Candidate {
                entry: scope.entry,
                position: scope.position,
                batch: Arc::clone(&fetched_batch.rows),
                sort_columns: Arc::clone(&fetched_batch.sort_columns),
                row,
                identity,
            });
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

/// Two values of one key compared in `order`: doubles in IEEE `totalOrder`, keywords and ids by
/// bytes, others by their natural order; a missing value sorts after every present one in either
/// direction.
///
/// # Errors
///
/// Returns `500` for two present values of different kinds.
fn compare_key_values(left: KeyValue<'_>, right: KeyValue<'_>, order: SortOrder) -> Result<Ordering, OpenSearchError> {
    let ordering = match (left, right) {
        (KeyValue::Missing, KeyValue::Missing) | (KeyValue::Score, KeyValue::Score) => return Ok(Ordering::Equal),
        (KeyValue::Missing, _) => return Ok(Ordering::Greater),
        (_, KeyValue::Missing) => return Ok(Ordering::Less),
        (KeyValue::Long(left), KeyValue::Long(right)) => left.cmp(&right),
        (KeyValue::Double(left), KeyValue::Double(right)) => left.total_cmp(&right),
        (KeyValue::Text(left), KeyValue::Text(right)) => left.cmp(right),
        (KeyValue::Bytes(left), KeyValue::Bytes(right)) => left.cmp(right),
        (KeyValue::Doc(left), KeyValue::Doc(right)) => left.cmp(&right),
        (
            KeyValue::Long(_)
            | KeyValue::Double(_)
            | KeyValue::Text(_)
            | KeyValue::Bytes(_)
            | KeyValue::Doc(_)
            | KeyValue::Score,
            _,
        ) => {
            return Err(OpenSearchError::internal(
                "sort values of different kinds were compared",
            ));
        }
    };
    Ok(apply_order(ordering, order))
}

/// Compare two rows in sort order, key by key; each value is read as its key's kind, so neither
/// row's values are copied.
///
/// # Errors
///
/// Returns `500` for rows of another length than `keys`, or holding a value of another kind than
/// its key's. [`verify_sort_columns`] and the anchor's parsing by key kind rule these out before
/// any comparison.
fn compare_sort_values<L, R>(left: &L, right: &R, keys: &[SortKey]) -> Result<Ordering, OpenSearchError>
where
    L: SortKeyRow + ?Sized,
    R: SortKeyRow + ?Sized,
{
    if left.count_values() != keys.len() || right.count_values() != keys.len() {
        return Err(OpenSearchError::internal(format!(
            "sort tuples of {} and {} values were compared over {} keys",
            left.count_values(),
            right.count_values(),
            keys.len()
        )));
    }
    for (position, key) in keys.iter().enumerate() {
        let ordering = compare_key_values(
            left.read_key_value(position, key.kind)?,
            right.read_key_value(position, key.kind)?,
            key.order,
        )?;
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
    candidates.sort_by(|left, right| {
        match compare_sort_values(&left.read_sort_keys(), &right.read_sort_keys(), keys) {
            Ok(ordering) => ordering.then(left.position.cmp(&right.position)),
            Err(error) => {
                comparison_error.get_or_insert(error);
                Ordering::Equal
            }
        }
    });
    comparison_error.map_or(Ok(()), Err)
}

/// The `sort` array of a page hit over `keys`: dates as epoch milliseconds, `_doc` as its sort
/// value, `_score` as `1.0`, a missing value as `null`. Only a returned hit renders its values,
/// copied out of its fetched columns here.
///
/// # Errors
///
/// Returns the errors of [`render_sort_scalar`], and `500` when a field key has no column or a
/// `_doc` key no identity.
fn render_sort_values(candidate: &Candidate<'_>, keys: &[SortKey]) -> Result<Vec<Value>, OpenSearchError> {
    let sort_keys = candidate.read_sort_keys();
    keys.iter()
        .enumerate()
        .map(|(position, key)| match key.kind {
            SortKeyKind::Score => Ok(json!(1.0)),
            SortKeyKind::Doc => candidate
                .identity
                .map(|identity| json!(encode_doc_sort_value(identity.timestamp_micros, identity.row_hash)))
                .ok_or_else(|| OpenSearchError::internal("a hit sorted on _doc was not hashed")),
            SortKeyKind::Long | SortKeyKind::Double | SortKeyKind::Text | SortKeyKind::Hex(_) => {
                render_sort_scalar(&sort_keys.copy_field_value(position)?)
            }
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

/// How `fields` reads one requested name in the hits of an index of a non-native profile,
/// decided once per search from the name's resolution there.
#[derive(Debug, Clone, Copy)]
enum ProfileFieldRead {
    /// The name is no leaf of the profile: the hits carry no value for it.
    Absent,
    /// A profile time, read from its stored date column so a requested date format applies as on
    /// a native date column.
    Time(&'static str),
    /// Any other leaf, rendered as the index's profile table shows it.
    Rendered(&'static ProfileTable),
}

/// The `docvalue_fields` then the `fields` of a request, with how each reads every index of a
/// non-native profile.
struct FieldRequestPlan<'r> {
    /// The requested names and patterns, `docvalue_fields` first.
    requests: Vec<&'r DocValueField>,
    /// Per request, per index position in the search: how the request reads that index; `None`
    /// for a native index, whose hits resolve the name themselves.
    profile_reads: Vec<Vec<Option<ProfileFieldRead>>>,
}

/// The `docvalue_fields` then the `fields` of a request, checked against every index: a
/// concrete name or a pattern-matched column typed differently across the indices is a 400
/// naming the field and the indices of each type, a format on a concrete non-date field is a
/// 400, and so is a pattern on an index of a non-native profile, whose fields are named one by
/// one. Each concrete name is resolved once per index of a non-native profile here; its hits
/// read that outcome instead of resolving the name again.
///
/// # Errors
///
/// Returns [`OpenSearchError::field_projection_conflict`] for a name typed differently across
/// `entries`, `illegal_argument_exception` for a format on a concrete field that is no date,
/// [`OpenSearchError::cross_row_field_in_field_request`] for a cross-row field of the Data
/// Prepper profile, [`OpenSearchError::field_pattern_on_profile_index`] for a pattern on an index of a non-native
/// profile, the error an index's table failed to open with, and `500` for a column a pattern
/// matches whose type no `OpenSearch` type represents.
fn plan_field_requests<'r>(
    session: &TenantSession,
    entries: &[&IndexEntry],
    request: &'r SearchRequest,
) -> Result<FieldRequestPlan<'r>, OpenSearchError> {
    let requests: Vec<&DocValueField> = request.docvalue_fields.iter().chain(&request.fields).collect();
    let mut profile_reads = Vec::with_capacity(requests.len());
    for field_request in &requests {
        // The type each index gives every name the request reaches, by name.
        let mut types: BTreeMap<String, BTreeMap<&'static str, Vec<&str>>> = BTreeMap::new();
        let mut entry_reads: Vec<Option<ProfileFieldRead>> = Vec::with_capacity(entries.len());
        for entry in entries {
            let profile_table = find_profile_table(entry);
            if field_request.is_pattern() {
                if profile_table.is_some() {
                    return Err(OpenSearchError::field_pattern_on_profile_index(
                        &field_request.field,
                        entry.name(),
                    ));
                }
                entry_reads.push(None);
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
                continue;
            }
            if entry.profile() == Profile::DataPrepper && CrossRowField::find_by_name(&field_request.field).is_some() {
                return Err(OpenSearchError::cross_row_field_in_field_request(
                    &field_request.field,
                    entry.name(),
                ));
            }
            let resolution = session.resolve_field(entry, &field_request.field);
            entry_reads.push(profile_table.map(|table| decide_profile_field_read(table, resolution.as_ref())));
            if let Some(resolved_field) = resolution {
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
        profile_reads.push(entry_reads);
    }
    Ok(FieldRequestPlan {
        requests,
        profile_reads,
    })
}

/// How the hits of an index served by `table` read a requested name that resolved to
/// `resolution` there.
const fn decide_profile_field_read(
    table: &'static ProfileTable,
    resolution: Option<&ResolvedField>,
) -> ProfileFieldRead {
    match resolution {
        None => ProfileFieldRead::Absent,
        Some(ResolvedField {
            value: FieldValue::Encoded(EncodedField::Rfc3339Time { column }),
            ..
        }) => ProfileFieldRead::Time(column),
        Some(_) => ProfileFieldRead::Rendered(table),
    }
}

/// The `fields` of one hit: each requested name or pattern resolved against the hit's own index,
/// each value a one-element array, a missing value no key; a later request for the same name
/// replaces the earlier one. A name on an index of a non-native profile reads as `plan` decided
/// for that index.
///
/// # Errors
///
/// Returns `500` for a value [`render_field_value`], [`render_array_value`],
/// [`render_derived_value`] or [`render_profile_field`] cannot render, a column of a type no
/// `OpenSearch` type represents, a MAP column that is not a MAP of `Utf8` keys, a resolved column
/// the hit's batch does not hold (the batch holds every column of the index's table, so a missing
/// one is a broken invariant, not a missing value), and a plan without the hit's index; and the
/// 400 of [`render_profile_field`] for a stored attribute key its profile cannot spell.
fn render_field_values(
    session: &TenantSession,
    candidate: &Candidate<'_>,
    plan: &FieldRequestPlan<'_>,
) -> Result<Map<String, Value>, OpenSearchError> {
    let batch = candidate.batch.as_ref();
    let row = candidate.row;
    let mut values = Map::new();
    for (field_request, entry_reads) in plan.requests.iter().zip(&plan.profile_reads) {
        let profile_read = entry_reads.get(candidate.position).ok_or_else(|| {
            OpenSearchError::internal(format!(
                "the `fields` plan has no index at position {}",
                candidate.position
            ))
        })?;
        if let Some(profile_read) = profile_read {
            if let Some(value) = render_profile_field_value(candidate, *profile_read, field_request)? {
                values.insert(field_request.field.clone(), json!([value]));
            }
            continue;
        }
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
            // The metrics `value` is the one derived field of a native index; it renders from its row.
            FieldValue::Derived(derived_field) => {
                Some(render_derived_value(*derived_field, batch, row)?).filter(|value| !value.is_null())
            }
            // A native index resolves no encoded or cross-row field.
            FieldValue::Encoded(_) | FieldValue::CrossRow(_) => None,
        };
        if let Some(value) = value {
            values.insert(field_request.field.clone(), json!([value]));
        }
    }
    Ok(values)
}

/// The `fields` value `field_request` reads from `candidate`'s row, on an index of a non-native
/// profile, as `profile_read` decided; `None` when the row has no value or the name is no leaf of
/// the profile. A profile time keeps the requested date format, like a native date column; every
/// other field renders as the profile shows it.
///
/// # Errors
///
/// Returns the errors of [`render_field_value`] and [`render_profile_field`], and `500` for a
/// resolved column the hit's batch does not hold.
fn render_profile_field_value(
    candidate: &Candidate<'_>,
    profile_read: ProfileFieldRead,
    field_request: &DocValueField,
) -> Result<Option<Value>, OpenSearchError> {
    let batch = candidate.batch.as_ref();
    let row = candidate.row;
    match profile_read {
        ProfileFieldRead::Absent => Ok(None),
        ProfileFieldRead::Time(column) => {
            let array = read_resolved_column(batch, column)?;
            (!array.is_null(row))
                .then(|| render_field_value(array.as_ref(), row, field_request.format))
                .transpose()
        }
        ProfileFieldRead::Rendered(table) => {
            Ok(Some(render_profile_field(table, batch, row, &field_request.field)?).filter(|value| !value.is_null()))
        }
    }
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
        ArrayRef, AsArray, FixedSizeBinaryArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
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
        ArraySortKeys, BranchOrdering, OrderingTerm, RowIdentity, SortKey, SortKeyKind, SortValue, bind_terms,
        compare_sort_values, has_equal_terms, list_prefix_terms, list_tie_terms, read_fixed_bytes, read_term_value,
        verify_sort_columns,
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

    /// Fetched rows compared in place from their sort columns order exactly as the values they
    /// hold do, against each other and against an anchor's values; two rows are tied on the prefix
    /// terms exactly when their values there are equal; and a row's values copied out of its
    /// columns are its own. Missing values, `-0.0`, `+0.0` and `NaN` are among the values.
    #[test]
    fn rows_read_in_place_from_their_sort_columns_compare_like_the_values_they_hold() {
        let mut generator = StdRng::seed_from_u64(0x0A77_A5C0);
        let mut tied_pairs = 0;
        let mut untied_pairs = 0;
        for _ in 0..20 {
            let keys = generate_keys(&mut generator);
            let rows: Vec<Vec<SortValue>> = (0..30).map(|_| generate_row(&mut generator, &keys)).collect();
            let batch = build_rows_batch(&rows, &keys);
            let read_column = |name: &str| Some(Arc::clone(batch.column_by_name(name).expect("a generated column")));
            let columns: Vec<Option<ArrayRef>> = keys
                .iter()
                .map(|key| match key.kind {
                    SortKeyKind::Long => read_column(FIELD_COLUMNS[0]),
                    SortKeyKind::Text => read_column(FIELD_COLUMNS[1]),
                    SortKeyKind::Double => read_column(FIELD_COLUMNS[2]),
                    SortKeyKind::Doc => None,
                    SortKeyKind::Hex(_) | SortKeyKind::Score => unreachable!("the generated keys are fields and _doc"),
                })
                .collect();
            // A generated `_doc` value is a microsecond below 1000 and hash bits below 2^43, so an
            // identity of exactly those values yields it back.
            let column_rows: Vec<ArraySortKeys<'_>> = rows
                .iter()
                .enumerate()
                .map(|(row, values)| ArraySortKeys {
                    columns: &columns,
                    row,
                    identity: find_value(values, &keys, SortKeyKind::Doc).map(|value| match value {
                        SortValue::Doc {
                            micros_in_millis,
                            hash_low,
                        } => RowIdentity {
                            timestamp_micros: *micros_in_millis,
                            row_hash: *hash_low,
                        },
                        other => unreachable!("a _doc key holds {other:?}"),
                    }),
                })
                .collect();
            let prefix_terms = list_prefix_terms(&keys);
            for (left, left_values) in column_rows.iter().zip(&rows) {
                let copied_values = left.copy_sort_values(&keys).expect("copy the row's values");
                assert_eq!(
                    compare_generated_rows(&copied_values, left_values, &keys),
                    Ordering::Equal,
                    "{copied_values:?} {left_values:?}"
                );
                for (right, right_values) in column_rows.iter().zip(&rows) {
                    let expected = compare_generated_rows(left_values, right_values, &keys);
                    assert_eq!(
                        compare_sort_values(left, right, &keys).expect("rows of one fetch compare"),
                        expected,
                        "{left_values:?} {right_values:?} {keys:?}"
                    );
                    assert_eq!(
                        compare_sort_values(left, right_values.as_slice(), &keys).expect("a row and an anchor compare"),
                        expected,
                        "{left_values:?} {right_values:?} {keys:?}"
                    );
                    let bound_terms = bind_terms(&prefix_terms, right_values).expect("bind the prefix terms");
                    let is_tied = bound_terms.iter().all(|(term, bound_value)| {
                        read_term_value(left_values, *term).expect("a prefix term value").as_ref() == bound_value
                    });
                    assert_eq!(
                        has_equal_terms(left, right, &prefix_terms, &keys).expect("compare the prefix terms"),
                        is_tied,
                        "{left_values:?} {right_values:?} {keys:?}"
                    );
                    if is_tied {
                        tied_pairs += 1;
                    } else {
                        untied_pairs += 1;
                    }
                }
            }
        }
        // The seed reaches rows tied on the prefix terms, other than a row with itself, and rows
        // that are not.
        assert!(tied_pairs > 20 * 30, "{tied_pairs}");
        assert!(untied_pairs > 0, "{untied_pairs}");
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
            let error = compare_sort_values(left.as_slice(), right.as_slice(), &keys).expect_err(case);
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

#[cfg(test)]
mod pruning_tests {
    //! Which query forms reach the scan as a filter it can prune on: a field-qualified id term
    //! reaches it as a top-level equality; unqualified free text reaches it only as one
    //! disjunction over every field, which prunes nothing.

    use chrono::{DateTime, Utc};
    use datafusion::common::ScalarValue;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::utils::split_conjunction;
    use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator};
    use datafusion::prelude::SessionContext;
    use icegate_common::LOGS_TABLE;
    use icegate_common::schema::COL_TRACE_ID;
    use serde_json::{Value, json};

    use crate::opensearch::catalog::TenantSession;
    use crate::query_dsl::lower::{LoweringScope, lower_query};
    use crate::query_dsl::parse::parse_query;
    use crate::test_support::{SearchFixture, find_index_entry};

    const TRACE_ID_HEX: &str = "5b8efff798038103d269b633813fc60c";
    /// The bytes of `TRACE_ID_HEX`, written out.
    const TRACE_ID: [u8; 16] = [
        0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c,
    ];

    /// The filters the optimizer pushes into the scans of the plan of `query` over `logs`.
    fn collect_optimized_scan_filters(session: &TenantSession, query: &Value) -> Vec<Expr> {
        let entry = find_index_entry(session, LOGS_TABLE);
        let now: DateTime<Utc> = DateTime::parse_from_rfc3339("2026-10-08T12:00:00Z")
            .expect("now")
            .with_timezone(&Utc);
        let parsed_query = parse_query(query).expect("query");
        let predicate = lower_query(&parsed_query, &LoweringScope { session, entry, now }).expect("lowered");
        let plan = session
            .scan_index(entry)
            .expect("scan")
            .filter(predicate)
            .and_then(LogicalPlanBuilder::build)
            .expect("plan");
        let optimized_plan = SessionContext::new().state().optimize(&plan).expect("optimized");
        let mut filters = Vec::new();
        optimized_plan
            .apply(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    filters.extend(scan.filters.iter().cloned());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("walk the plan");
        filters
    }

    /// Whether `expression` is exactly `trace_id = TRACE_ID`, the literal being the column's own
    /// fixed-width bytes: the form a scan prunes on when it is a top-level conjunct.
    fn is_trace_id_equality(expression: &Expr) -> bool {
        match expression {
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            }) => matches!(
                (left.as_ref(), right.as_ref()),
                (Expr::Column(column), Expr::Literal(ScalarValue::FixedSizeBinary(16, Some(bytes)), _))
                    | (Expr::Literal(ScalarValue::FixedSizeBinary(16, Some(bytes)), _), Expr::Column(column))
                    if column.name == COL_TRACE_ID && bytes.as_slice() == TRACE_ID
            ),
            _ => false,
        }
    }

    /// Whether one top-level conjunct of `filter` is the trace id equality. The same equality
    /// nested under an `Or` is not such a conjunct.
    fn has_trace_id_equality_conjunct(filter: &Expr) -> bool {
        split_conjunction(filter).into_iter().any(is_trace_id_equality)
    }

    /// Whether the trace id equality appears anywhere in `filter`, nested or not.
    fn contains_trace_id_equality(filter: &Expr) -> bool {
        filter
            .exists(|expression| Ok(is_trace_id_equality(expression)))
            .expect("walk the filter")
    }

    #[tokio::test]
    async fn field_qualified_id_terms_prune_and_unqualified_text_does_not() {
        let fixture = SearchFixture::new().await;
        let session = fixture.open_session("tenant-a").await;

        let qualified_filters = collect_optimized_scan_filters(&session, &json!({"term": {"trace_id": TRACE_ID_HEX}}));
        let unqualified_filters = collect_optimized_scan_filters(
            &session,
            &json!({"query_string": {"query": format!("\"{TRACE_ID_HEX}\"")}}),
        );

        assert!(
            qualified_filters.iter().any(has_trace_id_equality_conjunct),
            "a field-qualified id term reaches the scan as a top-level fixed-width equality: {qualified_filters:?}"
        );
        // The unqualified text reaches the scan with the same equality inside it, so the negative
        // assertion below holds because of where the equality sits, not because nothing arrived.
        assert!(
            unqualified_filters.iter().any(contains_trace_id_equality),
            "unqualified free text carries the trace id equality into the scan: {unqualified_filters:?}"
        );
        assert!(
            !unqualified_filters.iter().any(has_trace_id_equality_conjunct),
            "unqualified free text reaches the scan only as one disjunction over every field: {unqualified_filters:?}"
        );
    }
}
