//! Cross-row fields of the Data Prepper profile: values every span of a trace takes from the
//! trace's root span.
//!
//! The root spans are looked up with literal predicates (`trace_id IN (…)`,
//! `parent_span_id IS NULL`, a start-time window) through the tenant-scoped scan, so the lookup
//! prunes like any field-qualified id query and can never read another tenant's root. The roots
//! a filter, sort or aggregation needs are joined to the scan of the index; the roots `_source`
//! needs are looked up after paging.

use std::collections::{BTreeSet, HashMap, hash_map};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeBinaryArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema, TimeUnit, TimestampMicrosecondType};
use datafusion::common::{Column, JoinType, ScalarValue};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::functions_aggregate::expr_fn::{max, min};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, in_list, lit};
use datafusion::prelude::ident;
use icegate_common::SPANS_TABLE;
use icegate_common::schema::{
    COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_NAME, COL_PARENT_SPAN_ID, COL_STATUS_CODE, COL_TIMESTAMP, COL_TRACE_ID,
};
use serde_json::Value;

use super::encoding::{NANOS_PER_MICRO, format_rfc3339_micros, read_text};
use super::field::FieldType;
use super::{IndexEntry, Profile, TenantSession};
use crate::error::QueryError;
use crate::opensearch::error::OpenSearchError;
use crate::query_dsl::lower::build_date_literal;

/// The relation the joined root columns are qualified by.
const TRACE_ROOTS_RELATION: &str = "__icegate_trace_roots";
/// The join key of the joined roots: the trace id they belong to.
const JOINED_TRACE_ID: &str = "__icegate_root_trace_id";
/// Width in bytes of a trace id.
const TRACE_ID_WIDTH: i32 = 16;
/// The first start time of a candidate trace's selected spans.
const START_MIN: &str = "__icegate_start_min";
/// The last start time of a candidate trace's selected spans.
const START_MAX: &str = "__icegate_start_max";

/// A field every span of a trace takes from the trace's root span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CrossRowField {
    /// The root span's name.
    TraceGroup,
    /// The root span's end time.
    EndTime,
    /// The root span's duration in nanoseconds, at microsecond precision.
    DurationInNanos,
    /// The root span's status code, NULL as `0`.
    StatusCode,
}

/// Every cross-row field.
const CROSS_ROW_FIELDS: [CrossRowField; 4] = [
    CrossRowField::TraceGroup,
    CrossRowField::EndTime,
    CrossRowField::DurationInNanos,
    CrossRowField::StatusCode,
];

impl CrossRowField {
    /// The field's name in the Data Prepper profile.
    pub(crate) const fn client_name(self) -> &'static str {
        match self {
            Self::TraceGroup => "traceGroup",
            Self::EndTime => "traceGroupFields.endTime",
            Self::DurationInNanos => "traceGroupFields.durationInNanos",
            Self::StatusCode => "traceGroupFields.statusCode",
        }
    }

    /// The field's `OpenSearch` type.
    pub(crate) const fn field_type(self) -> FieldType {
        match self {
            Self::TraceGroup => FieldType::Keyword,
            Self::EndTime => FieldType::Date,
            Self::DurationInNanos => FieldType::Long,
            Self::StatusCode => FieldType::Integer,
        }
    }

    /// The column of the joined roots that holds the field.
    pub(crate) const fn joined_column(self) -> &'static str {
        match self {
            Self::TraceGroup => "__icegate_trace_group",
            Self::EndTime => "__icegate_trace_group_end_time",
            Self::DurationInNanos => "__icegate_trace_group_duration_nanos",
            Self::StatusCode => "__icegate_trace_group_status_code",
        }
    }

    /// The Arrow type of the joined column.
    pub(crate) const fn data_type(self) -> DataType {
        match self {
            Self::TraceGroup => DataType::Utf8,
            Self::EndTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::DurationInNanos => DataType::Int64,
            Self::StatusCode => DataType::Int32,
        }
    }

    /// The field a joined column holds; `None` for any other column.
    pub(crate) fn find_by_joined_column(column: &str) -> Option<Self> {
        CROSS_ROW_FIELDS.into_iter().find(|field| field.joined_column() == column)
    }

    /// The field a profile name names; `None` for any other name.
    pub(crate) fn find_by_name(name: &str) -> Option<Self> {
        CROSS_ROW_FIELDS.into_iter().find(|field| field.client_name() == name)
    }

    /// A NULL of the field's type: the value an aggregation input reads while no roots are
    /// attached, which no DSL path lets happen (the search attaches first).
    pub(crate) fn build_null_literal(self) -> Expr {
        lit(match self {
            Self::TraceGroup => ScalarValue::Utf8(None),
            Self::EndTime => ScalarValue::TimestampMicrosecond(None, None),
            Self::DurationInNanos => ScalarValue::Int64(None),
            Self::StatusCode => ScalarValue::Int32(None),
        })
    }
}

/// What the cross-row fields read from one trace's root span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraceRoot {
    /// The root span's name.
    pub(crate) name: Option<String>,
    /// The root span's end time, in microseconds since the epoch.
    pub(crate) end_timestamp_micros: i64,
    /// The root span's duration in microseconds.
    pub(crate) duration_micros: i64,
    /// The root span's status code; NULL stores status `0`.
    pub(crate) status_code: Option<i32>,
}

/// The roots found for a set of traces, by trace id. A trace absent from the map has no root in
/// the lookup window.
pub(crate) type TraceRoots = HashMap<[u8; 16], TraceRoot>;

/// The roots attached to one index of a session, with the table every joined scan of the index
/// reads them from, built once per attachment.
#[derive(Debug)]
pub(super) struct AttachedTraceRoots {
    /// The roots, by trace id: what `_source` of a page reads before it looks up any other
    /// trace.
    pub(super) roots: TraceRoots,
    /// The roots as one in-memory table of the joined columns.
    pub(super) table: Arc<MemTable>,
}

/// The `500` for a lookup result column of an unexpected shape.
fn report_lookup_column(column: &str) -> OpenSearchError {
    OpenSearchError::internal(format!("the root lookup returned no usable `{column}` column"))
}

/// The `500` for a root span whose duration in nanoseconds overflows `i64`.
fn report_duration_overflow() -> OpenSearchError {
    OpenSearchError::internal("a root span's duration overflows nanoseconds")
}

/// The trace id at `row` of the `trace_id` column of `batch`.
fn read_trace_id(batch: &RecordBatch, column: &str, row: usize) -> Result<[u8; 16], OpenSearchError> {
    let ids = batch
        .column_by_name(column)
        .and_then(|array| array.as_fixed_size_binary_opt())
        .ok_or_else(|| report_lookup_column(column))?;
    <[u8; 16]>::try_from(ids.value(row)).map_err(|_| report_lookup_column(column))
}

/// The microsecond timestamp at `row` of `column`; `None` when NULL.
fn read_micros(batch: &RecordBatch, column: &str, row: usize) -> Result<Option<i64>, OpenSearchError> {
    let array = batch.column_by_name(column).ok_or_else(|| report_lookup_column(column))?;
    if array.is_null(row) {
        return Ok(None);
    }
    array
        .as_primitive_opt::<TimestampMicrosecondType>()
        .map(|values| Some(values.value(row)))
        .ok_or_else(|| report_lookup_column(column))
}

/// The root lookup of `trace_ids`: their root spans (`parent_span_id IS NULL`) that started in
/// `[window_start_micros - lookback, window_end_micros]`, earliest first, through the
/// tenant-scoped scan of `spans`. `None` when there is no id to look up.
///
/// # Errors
///
/// Returns `404` when the tenant has no `spans` table, and the scan's planning errors.
pub(super) fn plan_trace_root_lookup(
    session: &TenantSession,
    trace_ids: &[[u8; 16]],
    window_start_micros: i64,
    window_end_micros: i64,
    lookback: Duration,
) -> Result<Option<LogicalPlan>, OpenSearchError> {
    if trace_ids.is_empty() {
        return Ok(None);
    }
    let spans = session
        .indices()
        .entries()
        .iter()
        .find(|entry| entry.table() == SPANS_TABLE && entry.profile() == Profile::Native)
        .ok_or_else(|| OpenSearchError::index_not_found(SPANS_TABLE))?;
    let timestamp_type = session
        .column_data_type(spans, COL_TIMESTAMP)?
        .ok_or_else(|| OpenSearchError::internal("the spans table lacks `timestamp`"))?;
    // A lookback past the representable range reaches back to the earliest representable start.
    let lookback_micros = i64::try_from(lookback.as_micros()).unwrap_or(i64::MAX);
    let literals: Vec<Expr> = trace_ids
        .iter()
        .map(|id| lit(ScalarValue::FixedSizeBinary(TRACE_ID_WIDTH, Some(id.to_vec()))))
        .collect();
    let predicate = in_list(ident(COL_TRACE_ID), literals, false)
        .and(ident(COL_PARENT_SPAN_ID).is_null())
        .and(ident(COL_TIMESTAMP).gt_eq(build_date_literal(
            window_start_micros.saturating_sub(lookback_micros),
            &timestamp_type,
        )?))
        .and(ident(COL_TIMESTAMP).lt_eq(build_date_literal(window_end_micros, &timestamp_type)?));
    let plan = session
        .scan_raw(spans)?
        .filter(predicate)
        .and_then(|builder| {
            builder.project(vec![
                ident(COL_TRACE_ID),
                ident(COL_NAME),
                ident(COL_END_TIMESTAMP),
                ident(COL_DURATION_MICROS),
                ident(COL_STATUS_CODE),
                ident(COL_TIMESTAMP),
            ])
        })
        .and_then(|builder| builder.sort(vec![ident(COL_TIMESTAMP).sort(true, false)]))
        .and_then(LogicalPlanBuilder::build)
        .map_err(|error| OpenSearchError::from(QueryError::from(error)))?;
    Ok(Some(plan))
}

/// Look up the root spans of `trace_ids` that started in
/// `[window_start_micros - lookback, window_end_micros]` (see `plan_trace_root_lookup`). A trace
/// with several roots takes the earliest.
///
/// # Errors
///
/// The lookup's planning and execution errors, and `500` for a lookup row of an unexpected
/// shape.
pub(crate) async fn lookup_trace_roots(
    session: &TenantSession,
    trace_ids: &[[u8; 16]],
    window_start_micros: i64,
    window_end_micros: i64,
    lookback: Duration,
) -> Result<HashMap<[u8; 16], TraceRoot>, OpenSearchError> {
    let Some(plan) = plan_trace_root_lookup(session, trace_ids, window_start_micros, window_end_micros, lookback)?
    else {
        return Ok(TraceRoots::new());
    };
    let mut roots = TraceRoots::with_capacity(trace_ids.len());
    for batch in session.collect(plan).await? {
        let names = batch.column_by_name(COL_NAME).ok_or_else(|| report_lookup_column(COL_NAME))?;
        let durations = batch
            .column_by_name(COL_DURATION_MICROS)
            .and_then(|array| array.as_primitive_opt::<Int64Type>())
            .ok_or_else(|| report_lookup_column(COL_DURATION_MICROS))?;
        let statuses = batch
            .column_by_name(COL_STATUS_CODE)
            .and_then(|array| array.as_primitive_opt::<Int32Type>())
            .ok_or_else(|| report_lookup_column(COL_STATUS_CODE))?;
        for row in 0..batch.num_rows() {
            let trace_id = read_trace_id(&batch, COL_TRACE_ID, row)?;
            let end_timestamp_micros =
                read_micros(&batch, COL_END_TIMESTAMP, row)?.ok_or_else(|| report_lookup_column(COL_END_TIMESTAMP))?;
            // The scan may yield any Arrow string encoding for `name`; `read_text` reads them all.
            let name = read_text(names, row)?.map(str::to_string);
            // The rows come earliest first, so the first root of a trace is the one it keeps.
            roots.entry(trace_id).or_insert_with(|| TraceRoot {
                name,
                end_timestamp_micros,
                duration_micros: durations.value(row),
                status_code: (!statuses.is_null(row)).then(|| statuses.value(row)),
            });
        }
    }
    Ok(roots)
}

/// The roots as one batch of the joined columns.
///
/// # Errors
///
/// Returns `500` when a root's duration overflows nanoseconds, as rendering it would, rather
/// than joining a NULL that filters and sorts would read as a missing root.
fn build_trace_root_batch(roots: &TraceRoots) -> Result<RecordBatch, OpenSearchError> {
    let mut fields = vec![Field::new(
        JOINED_TRACE_ID,
        DataType::FixedSizeBinary(TRACE_ID_WIDTH),
        false,
    )];
    fields.extend(
        CROSS_ROW_FIELDS
            .iter()
            .map(|field| Field::new(field.joined_column(), field.data_type(), true)),
    );
    let schema = Arc::new(Schema::new(fields));
    let ids =
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(roots.keys().map(|id| Some(id.to_vec())), TRACE_ID_WIDTH)
            .map_err(|error| OpenSearchError::internal(format!("trace roots cannot be batched: {error}")))?;
    let groups: StringArray = roots.values().map(|root| root.name.as_deref()).collect();
    let ends = TimestampMicrosecondArray::from_iter_values(roots.values().map(|root| root.end_timestamp_micros));
    let durations = roots
        .values()
        .map(|root| {
            root.duration_micros
                .checked_mul(NANOS_PER_MICRO)
                .ok_or_else(report_duration_overflow)
        })
        .collect::<Result<Int64Array, OpenSearchError>>()?;
    let statuses = Int32Array::from_iter_values(roots.values().map(|root| root.status_code.unwrap_or(0)));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ids),
        Arc::new(groups),
        Arc::new(ends),
        Arc::new(durations),
        Arc::new(statuses),
    ];
    RecordBatch::try_new(schema, columns)
        .map_err(|error| OpenSearchError::internal(format!("trace roots cannot be batched: {error}")))
}

/// `roots` as the in-memory table a joined scan reads.
///
/// # Errors
///
/// The errors of [`build_trace_root_batch`], and `500` when the table refuses the batch.
pub(super) fn build_trace_root_table(roots: &TraceRoots) -> Result<Arc<MemTable>, OpenSearchError> {
    let batch = build_trace_root_batch(roots)?;
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])
        .map_err(|error| OpenSearchError::from(QueryError::from(error)))?;
    Ok(Arc::new(table))
}

/// `scan` with the joined root columns of `table` (see [`build_trace_root_table`]): a left join
/// on the trace id, so a span whose trace has no root keeps its row with NULL root columns.
///
/// # Errors
///
/// The planning errors of the join.
pub(super) fn join_trace_roots(
    scan: LogicalPlanBuilder,
    table: Arc<MemTable>,
) -> Result<LogicalPlanBuilder, OpenSearchError> {
    LogicalPlanBuilder::scan(TRACE_ROOTS_RELATION, provider_as_source(table), None)
        .and_then(LogicalPlanBuilder::build)
        .and_then(|roots_plan| {
            scan.join(
                roots_plan,
                JoinType::Left,
                (
                    vec![Column::from_name(COL_TRACE_ID)],
                    vec![Column::from_name(JOINED_TRACE_ID)],
                ),
                None,
            )
        })
        .map_err(|error| OpenSearchError::from(QueryError::from(error)))
}

/// The traces a cross-row read of one search must look up, and the start-time window of their
/// spans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateTraces {
    /// The distinct trace ids.
    pub(crate) trace_ids: Vec<[u8; 16]>,
    /// The earliest start among their selected spans; with no `trace_ids` the window is empty,
    /// this start above its end.
    pub(crate) window_start_micros: i64,
    /// The latest start among their selected spans; with no `trace_ids` the window is empty,
    /// this end below its start.
    pub(crate) window_end_micros: i64,
}

/// Which candidate traces a cross-row read looks up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TraceSelection {
    /// Every trace the outer predicate selects; more than `max_cross_row_traces` is a 400.
    Bounded,
    /// The first `size` trace ids in key order (byte order, which is hex order), reversed when
    /// `is_descending`: exactly the buckets of a `terms` on the trace id ordered by key.
    FirstByKey {
        /// How many traces.
        size: usize,
        /// Whether the key order is descending.
        is_descending: bool,
    },
}

/// The distinct traces of the rows of `entry` that `outer` selects, with their start window.
///
/// Under [`TraceSelection::FirstByKey`] the first `size` traces in key order are selected and no
/// bound applies; `size` is at most `max_cross_row_traces`, as the search checks for a `terms` on
/// `traceId`.
///
/// # Errors
///
/// Returns `too_many_cross_row_traces` when [`TraceSelection::Bounded`] selects more than
/// `max_cross_row_traces` traces, and the scan's errors.
pub(crate) async fn select_candidate_traces(
    session: &TenantSession,
    entry: &IndexEntry,
    outer: Expr,
    selection: TraceSelection,
) -> Result<CandidateTraces, OpenSearchError> {
    let limit = session.settings().max_cross_row_traces;
    let grouped = session.scan_raw(entry)?.filter(outer).and_then(|builder| {
        builder.aggregate(
            vec![ident(COL_TRACE_ID)],
            vec![
                min(ident(COL_TIMESTAMP)).alias(START_MIN),
                max(ident(COL_TIMESTAMP)).alias(START_MAX),
            ],
        )
    });
    let (planned, row_bound) = match selection {
        // One trace past the limit is enough to refuse, so no more are read.
        TraceSelection::Bounded => (
            grouped.and_then(|builder| builder.limit(0, Some(limit.saturating_add(1)))),
            limit,
        ),
        TraceSelection::FirstByKey { size, is_descending } => (
            grouped
                .and_then(|builder| builder.sort(vec![ident(COL_TRACE_ID).sort(!is_descending, false)]))
                .and_then(|builder| builder.limit(0, Some(size))),
            size,
        ),
    };
    let plan = planned
        .and_then(LogicalPlanBuilder::build)
        .map_err(|error| OpenSearchError::from(QueryError::from(error)))?;
    let batches = session.collect(plan).await?;
    collect_candidate_traces(&batches, row_bound)?
        .ok_or_else(|| OpenSearchError::too_many_cross_row_traces(entry.name(), limit))
}

/// The candidates of `batches` (one row per trace with its start window); `Ok(None)` when they
/// hold more than `limit` traces.
fn collect_candidate_traces(batches: &[RecordBatch], limit: usize) -> Result<Option<CandidateTraces>, OpenSearchError> {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if rows > limit {
        return Ok(None);
    }
    let mut candidates = CandidateTraces {
        trace_ids: Vec::with_capacity(rows),
        window_start_micros: i64::MAX,
        window_end_micros: i64::MIN,
    };
    for batch in batches {
        for row in 0..batch.num_rows() {
            let trace_id = read_trace_id(batch, COL_TRACE_ID, row)?;
            let earliest_start = read_micros(batch, START_MIN, row)?.ok_or_else(|| report_lookup_column(START_MIN))?;
            let latest_start = read_micros(batch, START_MAX, row)?.ok_or_else(|| report_lookup_column(START_MAX))?;
            candidates.trace_ids.push(trace_id);
            candidates.window_start_micros = candidates.window_start_micros.min(earliest_start);
            candidates.window_end_micros = candidates.window_end_micros.max(latest_start);
        }
    }
    Ok(Some(candidates))
}

/// The roots `_source` needs for a page of hits: the roots attached for the page's Data Prepper
/// rows where they hold the trace, and a lookup, in chunks of `max_cross_row_traces`, of the
/// other traces of the page over the start window of their rows.
///
/// # Errors
///
/// The lookups' errors, and `500` when a Data Prepper row lacks its trace id or its start.
pub(crate) async fn lookup_page_trace_roots(
    session: &TenantSession,
    page: &[(&IndexEntry, &RecordBatch, usize)],
) -> Result<TraceRoots, OpenSearchError> {
    let mut roots = TraceRoots::new();
    let mut missing_trace_ids: BTreeSet<[u8; 16]> = BTreeSet::new();
    let mut window: Option<(i64, i64)> = None;
    // The attachment depends only on the entry, and a page spans a few entries, so each entry's
    // attached roots are resolved once rather than once per row.
    let mut attached_roots_by_entry: HashMap<&str, Option<Arc<AttachedTraceRoots>>> = HashMap::new();
    for (entry, batch, row) in page.iter().filter(|(entry, _, _)| entry.profile() == Profile::DataPrepper) {
        let trace_id = read_trace_id(batch, COL_TRACE_ID, *row)?;
        let attached_roots = match attached_roots_by_entry.entry(entry.name()) {
            hash_map::Entry::Occupied(occupied) => occupied.into_mut(),
            hash_map::Entry::Vacant(vacant) => vacant.insert(session.find_attached_trace_roots(entry)?),
        };
        if let Some(root) = attached_roots
            .as_deref()
            .and_then(|attached_roots| attached_roots.roots.get(&trace_id))
        {
            roots.insert(trace_id, root.clone());
            continue;
        }
        missing_trace_ids.insert(trace_id);
        let start = read_micros(batch, COL_TIMESTAMP, *row)?.ok_or_else(|| report_lookup_column(COL_TIMESTAMP))?;
        window = Some(window.map_or((start, start), |(earliest_start, latest_start)| {
            (earliest_start.min(start), latest_start.max(start))
        }));
    }
    let Some((window_start, window_end)) = window else {
        return Ok(roots);
    };
    let settings = session.settings();
    let missing_trace_ids: Vec<[u8; 16]> = missing_trace_ids.into_iter().collect();
    for chunk in missing_trace_ids.chunks(settings.max_cross_row_traces.max(1)) {
        roots.extend(lookup_trace_roots(session, chunk, window_start, window_end, settings.max_trace_lookback).await?);
    }
    Ok(roots)
}

/// The value of `field` for the span at `row` of `batch` from `roots`; `Value::Null` when its
/// trace has no root there.
///
/// # Errors
///
/// Returns `500` when the row lacks its trace id or the duration overflows nanoseconds.
pub(crate) fn render_cross_row_field(
    field: CrossRowField,
    batch: &RecordBatch,
    row: usize,
    roots: &TraceRoots,
) -> Result<Value, OpenSearchError> {
    let Some(root) = roots.get(&read_trace_id(batch, COL_TRACE_ID, row)?) else {
        return Ok(Value::Null);
    };
    Ok(match field {
        CrossRowField::TraceGroup => root.name.clone().map_or(Value::Null, Value::String),
        CrossRowField::EndTime => Value::String(format_rfc3339_micros(root.end_timestamp_micros)?),
        CrossRowField::DurationInNanos => root
            .duration_micros
            .checked_mul(NANOS_PER_MICRO)
            .map(Value::from)
            .ok_or_else(report_duration_overflow)?,
        CrossRowField::StatusCode => Value::from(root.status_code.unwrap_or(0)),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::{FixedSizeBinaryArray, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::{Expr, LogicalPlan};
    use datafusion::scalar::ScalarValue;
    use icegate_common::schema::{COL_PARENT_SPAN_ID, COL_TRACE_ID};
    use serde_json::Value;

    use super::{
        CrossRowField, TraceRoot, TraceRoots, build_trace_root_table, plan_trace_root_lookup, render_cross_row_field,
    };
    use crate::opensearch::error::render_envelope;
    use crate::test_support::SearchFixture;

    const TRACE_IDS: [[u8; 16]; 4] = [[0x01; 16], [0x02; 16], [0x03; 16], [0x04; 16]];
    /// 2025-10-09T08:53:20Z.
    const T0: i64 = 1_760_000_000_000_000;

    /// The trace-id bytes `node` compares with `trace_id` as `FixedSizeBinary(16)` literals: the
    /// items of an `IN` list, or the right side of an equality the optimizer may rewrite it to.
    fn collect_trace_literals(node: &Expr, trace_literals: &mut BTreeSet<Vec<u8>>) {
        let is_trace_id = |expr: &Expr| matches!(expr, Expr::Column(column) if column.name == COL_TRACE_ID);
        let mut insert_literal = |expr: &Expr| {
            if let Expr::Literal(ScalarValue::FixedSizeBinary(16, Some(bytes)), _) = expr {
                trace_literals.insert(bytes.clone());
            }
        };
        match node {
            Expr::InList(list) if is_trace_id(&list.expr) => list.list.iter().for_each(&mut insert_literal),
            Expr::BinaryExpr(binary) if is_trace_id(&binary.left) => insert_literal(&binary.right),
            _ => {}
        }
    }

    /// The pruning contract of the root lookup: every trace id reaches the table scan as a
    /// `FixedSizeBinary(16)` literal compared with `trace_id` (an `IN` list, or the equalities
    /// the optimizer may rewrite it to), and `parent_span_id IS NULL` reaches it too. A subquery
    /// or a join would reach the scan as neither, and Iceberg and bloom pruning would not apply.
    #[tokio::test]
    async fn the_root_lookup_uses_literal_in_lists_that_reach_the_scan() {
        let fixture = SearchFixture::new().await;
        let session = fixture.open_session("tenant-a").await;

        let plan = plan_trace_root_lookup(&session, &TRACE_IDS, T0, T0 + 1_000, Duration::from_mins(1))
            .expect("plan")
            .expect("a lookup for four ids");

        let context = fixture.engine.create_session().await.expect("session");
        let optimized = context.state().optimize(&plan).expect("optimize");
        let mut scan_filters: Vec<Expr> = Vec::new();
        optimized
            .apply(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    scan_filters.extend(scan.filters.iter().cloned());
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("walk the plan");

        let mut trace_literals: BTreeSet<Vec<u8>> = BTreeSet::new();
        let mut has_root_predicate = false;
        for filter in &scan_filters {
            filter
                .apply(|node| {
                    collect_trace_literals(node, &mut trace_literals);
                    if let Expr::IsNull(inner) = node
                        && matches!(inner.as_ref(), Expr::Column(column) if column.name == COL_PARENT_SPAN_ID)
                    {
                        has_root_predicate = true;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .expect("walk the filter");
        }

        let expected: BTreeSet<Vec<u8>> = TRACE_IDS.iter().map(|id| id.to_vec()).collect();
        assert_eq!(
            trace_literals, expected,
            "the scan receives every trace id as a literal: {scan_filters:?}"
        );
        assert!(
            has_root_predicate,
            "the scan receives parent_span_id IS NULL: {scan_filters:?}"
        );
        assert!(
            plan_trace_root_lookup(&session, &[], T0, T0, Duration::from_mins(1))
                .expect("plan")
                .is_none(),
            "no trace id plans no lookup"
        );
    }

    /// A root whose duration in nanoseconds overflows `i64` is a 500 both where the roots join a
    /// scan and where `_source` renders them, never a NULL that filters and sorts would read as a
    /// missing root. `i64::MAX / 1000` microseconds is the longest duration that still converts.
    #[tokio::test]
    async fn a_root_duration_overflowing_nanoseconds_is_a_500_when_joined_and_when_rendered() {
        const TRACE_ID: [u8; 16] = [0x0f; 16];
        let build_roots = |duration_micros: i64| {
            TraceRoots::from([(
                TRACE_ID,
                TraceRoot {
                    name: Some("GET /checkout".to_string()),
                    end_timestamp_micros: T0,
                    duration_micros,
                    status_code: None,
                },
            )])
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            COL_TRACE_ID,
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let ids = FixedSizeBinaryArray::try_from_iter([TRACE_ID].into_iter()).expect("ids");
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids)]).expect("batch");
        let longest_micros = 9_223_372_036_854_775;

        let longest_roots = build_roots(longest_micros);
        let overflowing_roots = build_roots(longest_micros + 1);

        assert!(build_trace_root_table(&longest_roots).is_ok());
        assert_eq!(
            render_cross_row_field(CrossRowField::DurationInNanos, &batch, 0, &longest_roots).expect("rendered"),
            Value::from(9_223_372_036_854_775_000_i64)
        );
        let joined_error = build_trace_root_table(&overflowing_roots).expect_err("joined overflow");
        let rendered_error = render_cross_row_field(CrossRowField::DurationInNanos, &batch, 0, &overflowing_roots)
            .expect_err("rendered overflow");
        for error in [joined_error, rendered_error] {
            let (status, body) = render_envelope(error).await;
            assert_eq!(status.as_u16(), 500, "{body}");
        }
    }
}
