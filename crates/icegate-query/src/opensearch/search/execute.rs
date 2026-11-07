//! Data-index `_search`: one plan per index, merged into one page of hits.
//!
//! Each index is scanned through the tenant scope with the query lowered for that index. A
//! search runs the indices concurrently and merges their pages in index order (the resolved
//! name order). Hits never come from a union that DataFusion coerces: each hit renders its
//! `_source` from its own index's row. Rendering is linear in the page, so it runs on the
//! blocking pool.

use std::time::Instant;

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::Int64Type;
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, lit};
use futures::future::try_join_all;
use icegate_common::schema::COL_TIMESTAMP;
use icegate_common::{OPERATIONS_TABLE, SPANS_TABLE};
use serde_json::Value;

use super::hits::{SearchHit, render_search_response};
use super::request::{SearchRequest, SourceFilter, TrackTotalHits};
use crate::error::QueryError;
use crate::opensearch::SearchLimitsConfig;
use crate::opensearch::catalog::{IndexEntry, TenantSession};
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::system_index::run_blocking_work;
use crate::query_dsl::lower::{LoweringScope, lower_query};

/// The alias of the matching-row count column.
const MATCHING_ROWS: &str = "matching_rows";

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

/// Search the data indices `entries` of `session` with `request`.
///
/// Every index is checked for a document-id rule before any scan: `spans` and `operations` are
/// keyed by `trace_id`‖`span_id`, and any other table must have a `timestamp` column.
/// `hits.total` is the exact count of matching rows. The caller checks the result window first.
/// The answer is rendered on the blocking pool, because it holds up to `from + size` hits of
/// every column.
///
/// # Errors
///
/// Returns a 400 naming an index without a document-id rule, the lowering errors of
/// [`lower_query`], the 400s of request keys the data path does not serve yet, planning or
/// execution failures, and `500` when a value has no JSON rendering or the rendering does not
/// finish.
pub(crate) async fn search_data_indices(
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
    let window = request.from.saturating_add(request.size);
    let pages = try_join_all(
        entries
            .iter()
            .map(|entry| search_index(session, entry, request, context, window)),
    )
    .await?;
    let total = pages.iter().map(|page| page.matching_rows).sum();
    // The work outlives the borrow of `request`, so it renders against its own copy.
    let rendering_request = request.clone();
    run_blocking_work("rendering data search hits", move || {
        let hits = select_page(&pages, rendering_request.from, rendering_request.size);
        render_search_response(started_at.elapsed(), total, &hits, &rendering_request)
    })
    .await
}

/// One index's share of a search: its matching-row count and up to `from + size` of its rows.
/// It owns everything it holds, so it moves to the blocking pool for rendering.
struct IndexPage {
    /// The name of the index the rows were read from.
    index_name: String,
    /// How many of the index's rows match the query.
    matching_rows: u64,
    /// The first `from + size` matching rows, in scan order.
    batches: Vec<RecordBatch>,
}

/// A DataFusion planning failure as the envelope `From<QueryError>` maps it to.
fn convert_plan_error(error: DataFusionError) -> OpenSearchError {
    OpenSearchError::from(QueryError::from(error))
}

/// Count `entry`'s matching rows and read up to `window` of them, concurrently.
///
/// # Errors
///
/// Returns the lowering errors of [`lower_query`] and planning or execution failures.
async fn search_index(
    session: &TenantSession,
    entry: &IndexEntry,
    request: &SearchRequest,
    context: &RequestContext,
    window: usize,
) -> Result<IndexPage, OpenSearchError> {
    let scope = LoweringScope {
        session,
        entry,
        now: context.now,
    };
    let matching_plan = session
        .scan_index(entry)?
        .filter(lower_query(&request.query, &scope)?)
        .map_err(convert_plan_error)?;
    let count_plan = matching_plan
        .clone()
        .aggregate(Vec::<Expr>::new(), vec![count(lit(1_i64)).alias(MATCHING_ROWS)])
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)?;
    let page_plan = if window > 0 {
        Some(
            matching_plan
                .limit(0, Some(window))
                .and_then(LogicalPlanBuilder::build)
                .map_err(convert_plan_error)?,
        )
    } else {
        None
    };
    let (counts, batches) = futures::try_join!(session.collect(count_plan), async {
        match page_plan {
            Some(plan) => session.collect(plan).await,
            None => Ok(Vec::new()),
        }
    })?;
    Ok(IndexPage {
        index_name: entry.name().to_string(),
        matching_rows: read_matching_rows(&counts)?,
        batches,
    })
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

/// The hits `from .. from + size` of the concatenation of `pages`, in page order.
fn select_page(pages: &[IndexPage], from: usize, size: usize) -> Vec<SearchHit<'_>> {
    pages
        .iter()
        .flat_map(|page| {
            page.batches.iter().flat_map(move |batch| {
                (0..batch.num_rows()).map(move |row| SearchHit {
                    index: &page.index_name,
                    batch,
                    row,
                })
            })
        })
        .skip(from)
        .take(size)
        .collect()
}

/// Refuse an index whose rows have no document id.
fn require_document_identity(session: &TenantSession, entry: &IndexEntry) -> Result<(), OpenSearchError> {
    let is_span_keyed = entry.table() == SPANS_TABLE || entry.table() == OPERATIONS_TABLE;
    if is_span_keyed || session.column_data_type(entry, COL_TIMESTAMP)?.is_some() {
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
    let unserved_keys = [
        (!request.sort.is_empty(), "sort"),
        (request.aggregations.is_some(), "aggs"),
        (!matches!(request.source, SourceFilter::All), "_source filtering"),
        (request.version, "version"),
        (
            matches!(request.track_total_hits, TrackTotalHits::Disabled),
            "track_total_hits: false",
        ),
    ];
    if let Some((_, key)) = unserved_keys.iter().find(|(is_requested, _)| *is_requested) {
        return Err(OpenSearchError::illegal_argument(format!(
            "[{key}] on data indices is not supported yet"
        )));
    }
    Ok(())
}
