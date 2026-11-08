//! `POST /_plugins/_ppl`.

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use datafusion::arrow::array::RecordBatch;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use serde::Deserialize;
use serde_json::Value;

use super::error::render_ppl_error;
use super::render::render_jdbc;
use crate::opensearch::catalog::TenantSession;
use crate::opensearch::config::PplLimitsConfig;
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, Parameters, RequestBody};
use crate::opensearch::state::OpenSearchState;
use crate::ppl::{PplError, parse_ppl, plan_ppl};

/// The query parameters of `/_plugins/_ppl`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PplQueryParameters {
    /// The response format; absent means `jdbc`.
    format: Option<String>,
}

impl EndpointParameters for PplQueryParameters {
    const IGNORED: &'static [&'static str] = &["mode", "sanitize", "include_metadata"];
}

/// A PPL request body: `{query, fetch_size?}`. Every other key (`datasource`, `sessionId`,
/// `lang`, `highlight`, `queryId`, `profile`, `time_field`, `start_time`, `end_time`,
/// `include_metadata`, …) is accepted and ignored: IceGate has no highlighting and no index
/// pruning by time hints, and the query text alone decides the result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PplRequest {
    /// The PPL query text.
    pub(crate) query: String,
    /// The first N rows of the output to return; `None` returns every row.
    pub(crate) fetch_size: Option<usize>,
}

/// Answer `POST /_plugins/_ppl` with the jdbc response, or an error in its envelope.
pub(crate) async fn execute_ppl_query(
    State(state): State<OpenSearchState>,
    Extension(limits): Extension<PplLimitsConfig>,
    context: RequestContext,
    Parameters(parameters): Parameters<PplQueryParameters>,
    RequestBody(body): RequestBody,
) -> Response {
    let request = match check_format(parameters.format.as_deref()).and_then(|()| read_ppl_request(&body)) {
        Ok(request) => request,
        Err(error) => return error.into_response(),
    };
    let session = match state.catalog.open_session(&context.tenant).await {
        Ok(session) => session,
        Err(error) => return render_ppl_error(&error.into()),
    };
    match run_ppl_query(&session, &limits, &context, &request).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => render_ppl_error(&error),
    }
}

/// Refuse every format but jdbc.
fn check_format(format: Option<&str>) -> Result<(), OpenSearchError> {
    match format {
        None | Some("jdbc") => Ok(()),
        Some(other) => Err(OpenSearchError::unsupported_response_format(other)),
    }
}

/// Read a PPL request body.
///
/// `fetch_size` is a JSON integer: `0` bounds nothing, N ≥ 1 keeps the first N output rows.
///
/// # Errors
///
/// Returns `illegal_argument_exception` for a body that is not JSON or not a JSON object, a
/// missing or non-string `query`, and a `fetch_size` that is negative, fractional or not a number;
/// the last two carry the offending `key`, and its `value` when the body gave one.
pub(crate) fn read_ppl_request(body: &[u8]) -> Result<PplRequest, OpenSearchError> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|error| OpenSearchError::illegal_argument(format!("the request body is not valid JSON: {error}")))?;
    let Value::Object(fields) = value else {
        return Err(OpenSearchError::illegal_argument(
            "the request body must be a JSON object",
        ));
    };
    let query = match fields.get("query") {
        Some(Value::String(query)) => query.clone(),
        Some(other) => {
            return Err(OpenSearchError::invalid_request_key(
                "query",
                Some(other),
                "[query] must be a string",
            ));
        }
        None => {
            return Err(OpenSearchError::invalid_request_key(
                "query",
                None,
                "[query] is required",
            ));
        }
    };
    // `as_u64` reads only a non-negative JSON integer, so a negative number, a fraction and every
    // non-number are refused alike.
    let fetch_size = match fields.get("fetch_size") {
        None | Some(Value::Null) => None,
        Some(value) => match value.as_u64().map(usize::try_from) {
            Some(Ok(0)) => None,
            Some(Ok(size)) => Some(size),
            Some(Err(_)) | None => return Err(build_fetch_size_error(value)),
        },
    };
    Ok(PplRequest { query, fetch_size })
}

/// The refusal of a `fetch_size` that is not a non-negative integer; it carries the value.
fn build_fetch_size_error(value: &Value) -> OpenSearchError {
    OpenSearchError::invalid_request_key(
        "fetch_size",
        Some(value),
        format!("[fetch_size] must be a non-negative integer, not {value}"),
    )
}

/// Parse, plan and run `request` in `session`, and render the jdbc response.
///
/// `fetch_size` keeps the first N rows of the final output, as a trailing `| head N` does; it is
/// a bound the client asked for, never reported as truncation. `limits.max_result_rows` is then
/// checked against the output: the plan fetches one row beyond it, so a breach is detected
/// without reading the rest, and is refused instead of truncated.
pub(crate) async fn run_ppl_query(
    session: &TenantSession,
    limits: &PplLimitsConfig,
    context: &RequestContext,
    request: &PplRequest,
) -> Result<Value, PplError> {
    let query = parse_ppl(&request.query)?;
    let ppl_plan = plan_ppl(&query, session, context).await?;
    let plan = bound_output_rows(ppl_plan.plan, request.fetch_size, limits.max_result_rows)?;
    let batches = session.collect(plan).await?;
    refuse_rows_beyond_limit(&batches, limits.max_result_rows)?;
    render_jdbc(&ppl_plan.columns, &batches)
}

/// `plan` with the client's `fetch_size` applied, then fetching at most one row beyond
/// `max_result_rows`.
fn bound_output_rows(
    plan: LogicalPlan,
    fetch_size: Option<usize>,
    max_result_rows: Option<usize>,
) -> Result<LogicalPlan, PplError> {
    let mut builder = LogicalPlanBuilder::from(plan);
    if let Some(size) = fetch_size {
        builder = builder.limit(0, Some(size))?;
    }
    if let Some(limit) = max_result_rows {
        builder = builder.limit(0, Some(limit.saturating_add(1)))?;
    }
    Ok(builder.build()?)
}

fn refuse_rows_beyond_limit(batches: &[RecordBatch], max_result_rows: Option<usize>) -> Result<(), PplError> {
    let Some(limit) = max_result_rows else {
        return Ok(());
    };
    let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if row_count > limit {
        return Err(PplError::RowLimitExceeded { limit });
    }
    Ok(())
}
