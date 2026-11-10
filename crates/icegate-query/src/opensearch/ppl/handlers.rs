//! `POST /_plugins/_ppl`, answered in the `jdbc`, `viz`, `csv` or `raw` format, and
//! `POST /_plugins/_ppl/_explain`, the DataFusion plans of a query.

use axum::extract::State;
use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use serde::Deserialize;
use serde_json::{Value, json};

use super::error::render_ppl_error;
use super::render::{RenderedAnswer, render_ppl_answer};
use crate::opensearch::catalog::{ReservedBatches, TenantSession};
use crate::opensearch::config::PplLimitsConfig;
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, Parameters, RequestBody};
use crate::opensearch::filter_path::read_filter_path;
use crate::opensearch::state::OpenSearchState;
use crate::opensearch::system_index::run_blocking_work;
use crate::ppl::{PplColumn, PplError, parse_ppl, plan_ppl};

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

/// The response formats of `POST /_plugins/_ppl`, matched ASCII case-insensitively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PplFormat {
    /// `{schema, datarows, total, size}`; the default.
    Jdbc,
    /// One array per column, for charts.
    Viz,
    /// Comma-separated text.
    Csv,
    /// Pipe-separated text.
    Raw,
}

impl PplFormat {
    /// Reads the `format` query parameter; an absent one is `jdbc`.
    ///
    /// # Errors
    ///
    /// `illegal_argument_exception` carrying any other value as `format`.
    pub(crate) fn parse(format_name: Option<&str>) -> Result<Self, OpenSearchError> {
        let Some(format_name) = format_name else {
            return Ok(Self::Jdbc);
        };
        [Self::Jdbc, Self::Viz, Self::Csv, Self::Raw]
            .into_iter()
            .find(|format| format_name.eq_ignore_ascii_case(format.name()))
            .ok_or_else(|| OpenSearchError::unsupported_response_format(format_name))
    }

    /// Whether the format answers text rather than a JSON document.
    const fn is_flat_text(self) -> bool {
        match self {
            Self::Csv | Self::Raw => true,
            Self::Jdbc | Self::Viz => false,
        }
    }

    /// The name of the format, as the `format` parameter spells it; [`Self::parse`] matches
    /// against it.
    const fn name(self) -> &'static str {
        match self {
            Self::Jdbc => "jdbc",
            Self::Viz => "viz",
            Self::Csv => "csv",
            Self::Raw => "raw",
        }
    }
}

/// Answer `POST /_plugins/_ppl` in the requested format, or an error in its envelope.
///
/// `csv` and `raw` with a `filter_path` parameter are a 400 before the query runs: their body is
/// text, which the listener's `filter_path` layer passes through unfiltered, so the parameter
/// would be ignored without a word. `jdbc` and `viz` keep `filter_path`.
pub(crate) async fn execute_ppl_query(
    State(state): State<OpenSearchState>,
    Extension(limits): Extension<PplLimitsConfig>,
    context: RequestContext,
    (Parameters(parameters), uri): (Parameters<PplQueryParameters>, Uri),
    RequestBody(body): RequestBody,
) -> Response {
    let format_and_request = PplFormat::parse(parameters.format.as_deref())
        .and_then(|format| refuse_filter_path_on_flat_text(format, &uri).map(|()| format))
        .and_then(|format| read_ppl_request(&body).map(|request| (format, request)));
    let (format, request) = match format_and_request {
        Ok(format_and_request) => format_and_request,
        Err(error) => return error.into_response(),
    };
    let session = match state.catalog.open_session(&context.tenant).await {
        Ok(session) => session,
        Err(error) => return render_ppl_error(&error.into()),
    };
    let output = run_ppl_query(&session, &limits, &context, &request, &state.node.cluster_name).await;
    let answer = match output {
        Ok((columns, reserved_batches)) => render_answer_on_blocking_pool(format, columns, reserved_batches).await,
        Err(error) => Err(error),
    };
    match answer {
        Ok(answer) => answer.into_response(),
        Err(error) => render_ppl_error(&error),
    }
}

/// Renders `reserved_batches` in `format` on the blocking pool: rendering is linear in the rows
/// and columns of the answer, which must not hold an async worker. The batches' reservation is
/// released once the body is rendered.
///
/// # Errors
///
/// What [`render_ppl_answer`] reports, and [`PplError::Internal`] when the rendering task panics or
/// the runtime shuts down.
async fn render_answer_on_blocking_pool(
    format: PplFormat,
    columns: Vec<PplColumn>,
    reserved_batches: ReservedBatches,
) -> Result<RenderedAnswer, PplError> {
    run_blocking_work("rendering the PPL answer", move || {
        Ok(render_ppl_answer(
            format,
            &columns,
            &reserved_batches.batches,
            &reserved_batches.reservation,
        ))
    })
    .await?
}

/// Refuse a `filter_path` parameter on a text format, whose body the `filter_path` layer cannot
/// filter.
fn refuse_filter_path_on_flat_text(format: PplFormat, uri: &Uri) -> Result<(), OpenSearchError> {
    if format.is_flat_text() && read_filter_path(uri).is_some() {
        return Err(OpenSearchError::unsupported_parameter_with_format(
            "filter_path",
            format.name(),
        ));
    }
    Ok(())
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

/// Parse, plan and run `request` in `session`: the output columns and their rows, held against
/// the session's memory pool.
///
/// `fetch_size` keeps the first N rows of the final output, as a trailing `| head N` does; it is
/// a bound the client asked for, never reported as truncation. `limits.max_result_rows` is then
/// checked against the output: the plan fetches one row beyond it, so a breach is detected
/// without reading the rest, and is refused instead of truncated. Both bounds apply before any
/// format renders, so every format sees the same rows. The output counts against the per-query
/// memory limit as it arrives, so an answer larger than the limit is refused instead of held.
/// `cluster_name` is the listener's `opensearch.cluster_name`, which `describe` reports.
pub(crate) async fn run_ppl_query(
    session: &TenantSession,
    limits: &PplLimitsConfig,
    context: &RequestContext,
    request: &PplRequest,
    cluster_name: &str,
) -> Result<(Vec<PplColumn>, ReservedBatches), PplError> {
    let query = parse_ppl(&request.query)?;
    let ppl_plan = plan_ppl(&query, session, context, cluster_name).await?;
    let plan = bound_output_rows(ppl_plan.plan, request.fetch_size, limits.max_result_rows)?;
    let reserved_batches = session.collect_reserved_batches(plan, PPL_OUTPUT_CONSUMER).await?;
    refuse_rows_beyond_limit(&reserved_batches.batches, limits.max_result_rows)?;
    Ok((ppl_plan.columns, reserved_batches))
}

/// The memory consumer the output of a PPL query, and the answer rendered from it, are reserved
/// under.
const PPL_OUTPUT_CONSUMER: &str = "PPL output";

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

/// The query parameters of `POST /_plugins/_ppl/_explain`; any other parameter is a 400.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExplainParameters {
    /// The answer's format; only `json` (the default) is served.
    format: Option<String>,
}

impl EndpointParameters for ExplainParameters {
    const IGNORED: &'static [&'static str] = &["mode"];
}

/// `POST /_plugins/_ppl/_explain`: the DataFusion logical and physical plans of a PPL query, as
/// `{"calcite": {"logical": "<text>", "physical": "<text>"}}`.
///
/// Request-parsing errors (invalid JSON, no string `query`, an invalid `fetch_size`, a format
/// other than `json`) answer in the `OpenSearch` envelope; parse, planning and source errors in the
/// PPL envelope, an unknown source being the `IndexNotFoundException` 404. The query is planned,
/// never executed: no table data is read.
pub(crate) async fn explain_ppl_query(
    State(state): State<OpenSearchState>,
    context: RequestContext,
    Parameters(parameters): Parameters<ExplainParameters>,
    RequestBody(body): RequestBody,
) -> Response {
    let query = match read_explain_request(&parameters, &body) {
        Ok(query) => query,
        Err(error) => return error.into_response(),
    };
    match explain_query(&state, &context, &query).await {
        Ok(plans) => Json(plans).into_response(),
        Err(error) => render_ppl_error(&error),
    }
}

/// The `query` of an explain request, whose body is read as `POST /_plugins/_ppl` reads it.
fn read_explain_request(parameters: &ExplainParameters, body: &[u8]) -> Result<String, OpenSearchError> {
    if let Some(format_name) = parameters.format.as_deref()
        && !format_name.eq_ignore_ascii_case("json")
    {
        return Err(OpenSearchError::unsupported_explain_format(format_name));
    }
    read_ppl_request(body).map(|request| request.query)
}

/// Plans `query` and asks DataFusion to explain it: the explain plan stringifies the optimized
/// logical plan and the physical plan without reading data.
async fn explain_query(state: &OpenSearchState, context: &RequestContext, query: &str) -> Result<Value, PplError> {
    let query_ast = parse_ppl(query)?;
    let session = state.catalog.open_session(&context.tenant).await?;
    let ppl_plan = plan_ppl(&query_ast, &session, context, &state.node.cluster_name).await?;
    let explain_plan = LogicalPlanBuilder::from(ppl_plan.plan)
        .explain(false, false)
        .and_then(LogicalPlanBuilder::build)
        .map_err(|error| PplError::Internal {
            reason: format!("explain planning failed: {error}"),
        })?;
    let batches = session.collect(explain_plan).await?;
    let mut logical_plan = None;
    let mut physical_plan = None;
    for batch in &batches {
        let (Some(plan_types), Some(plans)) = (
            batch
                .column_by_name("plan_type")
                .and_then(|column| column.as_string_opt::<i32>()),
            batch.column_by_name("plan").and_then(|column| column.as_string_opt::<i32>()),
        ) else {
            return Err(PplError::Internal {
                reason: "the explain output has no plan_type and plan text columns".to_owned(),
            });
        };
        for row in 0..batch.num_rows() {
            match plan_types.value(row) {
                "logical_plan" => logical_plan = Some(plans.value(row).to_owned()),
                "physical_plan" => physical_plan = Some(plans.value(row).to_owned()),
                _ => {}
            }
        }
    }
    match (logical_plan, physical_plan) {
        (Some(logical_plan), Some(physical_plan)) => {
            Ok(json!({ "calcite": { "logical": logical_plan, "physical": physical_plan } }))
        }
        _ => Err(PplError::Internal {
            reason: "the explain output lacks the logical or the physical plan".to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::json;

    use super::*;

    #[test]
    fn ppl_format_reads_the_four_formats_and_refuses_any_other() {
        for (format_name, expected) in [
            (None, PplFormat::Jdbc),
            (Some("jdbc"), PplFormat::Jdbc),
            (Some("viz"), PplFormat::Viz),
            (Some("CSV"), PplFormat::Csv),
            (Some("raw"), PplFormat::Raw),
        ] {
            assert_eq!(
                PplFormat::parse(format_name).expect("accepted format"),
                expected,
                "{format_name:?}"
            );
        }
        for format_name in ["json", "yaml", "", "jdbc "] {
            let error = PplFormat::parse(Some(format_name)).expect_err("a refused format");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{format_name:?}");
            let cause = error.render_cause();
            assert_eq!(cause["type"], json!("illegal_argument_exception"), "{format_name:?}");
            assert_eq!(cause["format"], json!(format_name), "{format_name:?}");
        }
    }
}
