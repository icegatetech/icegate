//! trust's own lake tables, created with schemas stated here, and the seed builders parity tests
//! write cost-bearing rows with: the rate card, whose rates are stated as decimal text so the
//! stored decimals hold exactly the written values, and `operations` lines built by struct update
//! from one stated default.
//!
//! `marks`, `verdicts` and `scans` belong to trust. Their schemas carry the columns trust's
//! statements read, with the types trust's decoders read them as, plus the required `tenant_id`
//! an exact-name table needs to be read through the tenant scope.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use iceberg::Catalog;
use iceberg::spec::{NestedField, PrimitiveType, Type};
use icegate_common::schema::{
    COL_CACHE_READ_USD_PER_1M, COL_CACHE_WRITE_USD_PER_1M, COL_CONVERSATION_ID, COL_CURRENCY, COL_DURATION_MICROS,
    COL_END_TIMESTAMP, COL_INGESTED_TIMESTAMP, COL_INPUT_USD_PER_1M, COL_MAX_INPUT_TOKENS, COL_MIN_INPUT_TOKENS,
    COL_MODEL, COL_OUTPUT_USD_PER_1M, COL_PROVIDER, COL_REGION, COL_REQUEST_USD, COL_SERVICE_NAME, COL_SERVICE_TIER,
    COL_SOURCE, COL_SPAN_ID, COL_STATUS_CODE, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID, COL_VALID_FROM,
    COL_VALID_FROM_SOURCE,
};
use icegate_common::{OPERATIONS_TABLE, PRICES_TABLE};

use super::harness::{SECOND_MICROS, TENANT_A, WINDOW_START_MICROS};
use crate::opensearch::data::{Cell, Row, commit_rows, commit_table_columns, create_exact_name_table, required_string};
use crate::opensearch::test_rows::OPERATION_NAME;

/// trust's issue-mark table: one row per scan of one anti-pattern occurrence.
pub const MARKS_TABLE: &str = "marks";
/// trust's statistical-verdict table.
pub const VERDICTS_TABLE: &str = "verdicts";
/// trust's scan audit table: one row per batch attempt.
pub const SCANS_TABLE: &str = "scans";

/// An optional column of `primitive`.
fn build_optional_field(id: i32, name: &str, primitive: PrimitiveType) -> NestedField {
    NestedField::optional(id, name, Type::Primitive(primitive))
}

/// A required column of `primitive`.
fn build_required_field(id: i32, name: &str, primitive: PrimitiveType) -> NestedField {
    NestedField::required(id, name, Type::Primitive(primitive))
}

/// The `marks` columns: ranking keys, the generation, the four fingerprint levels, title
/// inputs, scope, severity and impact. `impact_cost_usd` is money, so `Decimal(38, 10)`.
fn build_marks_fields() -> Vec<NestedField> {
    vec![
        required_string(1, COL_TENANT_ID),
        required_string(2, "mark_id"),
        required_string(3, "scan_id"),
        build_required_field(4, "attempt", PrimitiveType::Int),
        build_required_field(5, "scanned_at", PrimitiveType::Timestamp),
        build_required_field(6, "occurred_at", PrimitiveType::Timestamp),
        build_required_field(7, "retracted", PrimitiveType::Boolean),
        build_required_field(8, "trace_id", PrimitiveType::Fixed(16)),
        build_optional_field(9, "root_span_id", PrimitiveType::Fixed(8)),
        required_string(10, "rule_id"),
        build_required_field(11, "fp_version", PrimitiveType::Int),
        build_required_field(12, "fp_epoch", PrimitiveType::Int),
        required_string(13, "fingerprint"),
        required_string(14, "fingerprint_l1"),
        required_string(15, "fingerprint_l2"),
        required_string(16, "fingerprint_l3"),
        required_string(17, "localization_kind"),
        required_string(18, "localization_key"),
        build_optional_field(19, "anchor_name", PrimitiveType::String),
        build_optional_field(20, "variant", PrimitiveType::String),
        build_optional_field(21, "evidence", PrimitiveType::String),
        build_optional_field(22, "service_name", PrimitiveType::String),
        build_optional_field(23, "environment", PrimitiveType::String),
        build_optional_field(24, "agent_name", PrimitiveType::String),
        build_optional_field(25, "agent_version", PrimitiveType::String),
        build_optional_field(26, "workflow_name", PrimitiveType::String),
        build_optional_field(27, "model", PrimitiveType::String),
        build_optional_field(28, "provider", PrimitiveType::String),
        required_string(29, "severity"),
        build_optional_field(
            30,
            "impact_cost_usd",
            PrimitiveType::Decimal {
                precision: 38,
                scale: 10,
            },
        ),
        build_optional_field(31, "impact_added_latency_micros", PrimitiveType::Long),
        build_optional_field(32, "impact_input_tokens", PrimitiveType::Long),
        build_optional_field(33, "impact_output_tokens", PrimitiveType::Long),
        build_optional_field(34, "impact_cacheable_tokens", PrimitiveType::Long),
        build_optional_field(35, "run_quality_z", PrimitiveType::Double),
    ]
}

/// The `verdicts` columns read by the harm statement.
fn build_verdicts_fields() -> Vec<NestedField> {
    vec![
        required_string(1, COL_TENANT_ID),
        required_string(2, "verdict_kind"),
        required_string(3, "subject_key"),
        required_string(4, "class_key"),
        required_string(5, "pillar"),
        build_required_field(6, "p_adj", PrimitiveType::Double),
        build_optional_field(7, "effect", PrimitiveType::Double),
        build_optional_field(8, "effect_ci_low", PrimitiveType::Double),
        build_optional_field(9, "effect_ci_high", PrimitiveType::Double),
        build_required_field(10, "n_subject", PrimitiveType::Long),
        build_required_field(11, "holds", PrimitiveType::Boolean),
        build_optional_field(12, "harm", PrimitiveType::String),
        build_required_field(13, "computed_at", PrimitiveType::Timestamp),
    ]
}

/// The `scans` columns trust's audit read names.
fn build_scans_fields() -> Vec<NestedField> {
    vec![
        required_string(1, COL_TENANT_ID),
        required_string(2, "scan_id"),
        required_string(3, "status"),
        build_optional_field(4, "degraded_rules", PrimitiveType::String),
        build_required_field(5, "attempt", PrimitiveType::Int),
        build_required_field(6, "rootless", PrimitiveType::Boolean),
        build_required_field(7, "started_at", PrimitiveType::Timestamp),
        build_optional_field(8, "ingest_hi", PrimitiveType::Timestamp),
    ]
}

/// Create `marks`, `verdicts` and `scans`, empty and unpartitioned, in the IceGate namespace.
pub async fn create_trust_tables(catalog: &Arc<dyn Catalog>) {
    for (name, fields) in [
        (MARKS_TABLE, build_marks_fields()),
        (VERDICTS_TABLE, build_verdicts_fields()),
        (SCANS_TABLE, build_scans_fields()),
    ] {
        create_exact_name_table(catalog, name, fields)
            .await
            .unwrap_or_else(|error| panic!("create `{name}`: {error}"));
    }
}

/// A nullable string column.
fn build_string_column<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

/// A nullable 64-bit integer column.
fn build_long_column(values: impl Iterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from_iter(values))
}

/// A required microsecond timestamp column without zone.
fn build_timestamp_column(values: impl Iterator<Item = i64>) -> ArrayRef {
    Arc::new(TimestampMicrosecondArray::from_iter_values(values))
}

/// Write `columns` (by name) into `table_name` as one snapshot through the shared column writer;
/// a column the map lacks is null.
async fn commit_columns(
    catalog: &Arc<dyn Catalog>,
    table_name: &str,
    columns: HashMap<&'static str, ArrayRef>,
    row_count: usize,
) {
    let named_columns: Vec<(&str, ArrayRef)> = columns.into_iter().collect();
    commit_table_columns(catalog, table_name, row_count, &named_columns)
        .await
        .unwrap_or_else(|error| panic!("commit `{table_name}`: {error}"));
}

/// One revision of the rate card. Rates are decimal text, so the stored `Decimal(38, 10)` holds
/// exactly the written value.
pub struct RateCardRow {
    /// `provider`, matched against `operations.provider_name`.
    pub provider: &'static str,
    /// `model`, matched against `operations.request_model`.
    pub model: &'static str,
    /// `service_tier`; trust prices `standard` only.
    pub service_tier: &'static str,
    /// `region`; trust prices `global` only.
    pub region: &'static str,
    /// Inclusive lower bound of the input-token tier.
    pub min_input_tokens: i64,
    /// Exclusive upper bound of the input-token tier; `None` is unbounded.
    pub max_input_tokens: Option<i64>,
    /// When the revision takes effect, in microseconds.
    pub valid_from_micros: i64,
    /// USD per million input tokens.
    pub input_usd_per_1m: Option<&'static str>,
    /// USD per million output tokens.
    pub output_usd_per_1m: Option<&'static str>,
    /// USD per million cache-read tokens.
    pub cache_read_usd_per_1m: Option<&'static str>,
    /// USD per million cache-write tokens.
    pub cache_write_usd_per_1m: Option<&'static str>,
    /// USD per request.
    pub request_usd: Option<&'static str>,
}

/// Twenty days before the window start.
const TWENTY_DAYS_BEFORE_WINDOW: i64 = WINDOW_START_MICROS - 1_728_000 * SECOND_MICROS;
/// Ten days before the window start.
const TEN_DAYS_BEFORE_WINDOW: i64 = WINDOW_START_MICROS - 864_000 * SECOND_MICROS;
/// One day before the window start.
const ONE_DAY_BEFORE_WINDOW: i64 = WINDOW_START_MICROS - 86_400 * SECOND_MICROS;

/// The rate card every cost-bearing statement joins. It is global, so both tenants read it.
///
/// - `openai/gpt-4o` standard: the newer of its two full revisions wins. The older orphaned tier
///   with a higher lower bound also matches an operation above that bound, so the newest-revision
///   dedup, not the tier, decides its price;
/// - `openai/gpt-4o` batch tier: never matched, trust prices the standard tier;
/// - `anthropic/claude-x` standard: two input-token tiers, so a long-context operation is priced
///   by the upper one;
/// - `openai/gpt-mini` standard: no output rate, so its operations are unpriced.
pub static RATE_CARD: [RateCardRow; 7] = [
    RateCardRow {
        provider: "openai",
        model: "gpt-4o",
        service_tier: "standard",
        region: "global",
        min_input_tokens: 0,
        max_input_tokens: None,
        valid_from_micros: TEN_DAYS_BEFORE_WINDOW,
        input_usd_per_1m: Some("2.5"),
        output_usd_per_1m: Some("10.0"),
        cache_read_usd_per_1m: Some("1.25"),
        cache_write_usd_per_1m: None,
        request_usd: None,
    },
    RateCardRow {
        provider: "openai",
        model: "gpt-4o",
        service_tier: "standard",
        region: "global",
        min_input_tokens: 0,
        max_input_tokens: None,
        valid_from_micros: ONE_DAY_BEFORE_WINDOW,
        input_usd_per_1m: Some("2.0"),
        output_usd_per_1m: Some("8.0"),
        cache_read_usd_per_1m: Some("1.0"),
        cache_write_usd_per_1m: None,
        request_usd: None,
    },
    RateCardRow {
        provider: "openai",
        model: "gpt-4o",
        service_tier: "standard",
        region: "global",
        min_input_tokens: 500,
        max_input_tokens: None,
        valid_from_micros: TWENTY_DAYS_BEFORE_WINDOW,
        input_usd_per_1m: Some("9.0"),
        output_usd_per_1m: Some("9.0"),
        cache_read_usd_per_1m: None,
        cache_write_usd_per_1m: None,
        request_usd: None,
    },
    RateCardRow {
        provider: "openai",
        model: "gpt-4o",
        service_tier: "batch",
        region: "global",
        min_input_tokens: 0,
        max_input_tokens: None,
        valid_from_micros: TEN_DAYS_BEFORE_WINDOW,
        input_usd_per_1m: Some("1.25"),
        output_usd_per_1m: Some("5.0"),
        cache_read_usd_per_1m: None,
        cache_write_usd_per_1m: None,
        request_usd: None,
    },
    RateCardRow {
        provider: "anthropic",
        model: "claude-x",
        service_tier: "standard",
        region: "global",
        min_input_tokens: 0,
        max_input_tokens: Some(200_000),
        valid_from_micros: TEN_DAYS_BEFORE_WINDOW,
        input_usd_per_1m: Some("3.0"),
        output_usd_per_1m: Some("15.0"),
        cache_read_usd_per_1m: Some("0.3"),
        cache_write_usd_per_1m: Some("3.75"),
        request_usd: Some("0"),
    },
    RateCardRow {
        provider: "anthropic",
        model: "claude-x",
        service_tier: "standard",
        region: "global",
        min_input_tokens: 200_000,
        max_input_tokens: None,
        valid_from_micros: TEN_DAYS_BEFORE_WINDOW,
        input_usd_per_1m: Some("6.0"),
        output_usd_per_1m: Some("22.5"),
        cache_read_usd_per_1m: None,
        cache_write_usd_per_1m: None,
        request_usd: None,
    },
    RateCardRow {
        provider: "openai",
        model: "gpt-mini",
        service_tier: "standard",
        region: "global",
        min_input_tokens: 0,
        max_input_tokens: None,
        valid_from_micros: TEN_DAYS_BEFORE_WINDOW,
        input_usd_per_1m: Some("0.15"),
        output_usd_per_1m: None,
        cache_read_usd_per_1m: None,
        cache_write_usd_per_1m: None,
        request_usd: None,
    },
];

/// Commit [`RATE_CARD`] into `prices` as one snapshot; `prices_effective` derives `valid_to`.
pub async fn commit_rate_card(catalog: &Arc<dyn Catalog>) {
    let rows = &RATE_CARD;
    let columns: HashMap<&'static str, ArrayRef> = HashMap::from([
        (
            COL_PROVIDER,
            build_string_column(rows.iter().map(|row| Some(row.provider))),
        ),
        (COL_MODEL, build_string_column(rows.iter().map(|row| Some(row.model)))),
        (
            COL_SERVICE_TIER,
            build_string_column(rows.iter().map(|row| Some(row.service_tier))),
        ),
        (COL_REGION, build_string_column(rows.iter().map(|row| Some(row.region)))),
        (
            COL_MIN_INPUT_TOKENS,
            build_long_column(rows.iter().map(|row| Some(row.min_input_tokens))),
        ),
        (
            COL_MAX_INPUT_TOKENS,
            build_long_column(rows.iter().map(|row| row.max_input_tokens)),
        ),
        (
            COL_VALID_FROM,
            build_timestamp_column(rows.iter().map(|row| row.valid_from_micros)),
        ),
        (
            COL_VALID_FROM_SOURCE,
            build_string_column(rows.iter().map(|_| Some("observed"))),
        ),
        (
            COL_INPUT_USD_PER_1M,
            build_string_column(rows.iter().map(|row| row.input_usd_per_1m)),
        ),
        (
            COL_OUTPUT_USD_PER_1M,
            build_string_column(rows.iter().map(|row| row.output_usd_per_1m)),
        ),
        (
            COL_CACHE_READ_USD_PER_1M,
            build_string_column(rows.iter().map(|row| row.cache_read_usd_per_1m)),
        ),
        (
            COL_CACHE_WRITE_USD_PER_1M,
            build_string_column(rows.iter().map(|row| row.cache_write_usd_per_1m)),
        ),
        (
            COL_REQUEST_USD,
            build_string_column(rows.iter().map(|row| row.request_usd)),
        ),
        (COL_CURRENCY, build_string_column(rows.iter().map(|_| Some("USD")))),
        (
            COL_SOURCE,
            build_string_column(rows.iter().map(|_| Some("parity-test"))),
        ),
    ]);
    commit_columns(catalog, PRICES_TABLE, columns, rows.len()).await;
}

/// One `operations` row: it starts `start_offset_micros` after the window start and ends
/// `duration_micros` later; every `None` column stays null.
pub struct OperationLine {
    /// The tenant the row belongs to.
    pub tenant: &'static str,
    /// `trace_id`.
    pub trace_id: [u8; 16],
    /// `span_id`.
    pub span_id: [u8; 8],
    /// `conversation_id`.
    pub conversation_id: Option<&'static str>,
    /// `operation_name`.
    pub operation_name: &'static str,
    /// `provider_name`.
    pub provider_name: Option<&'static str>,
    /// `request_model`.
    pub request_model: Option<&'static str>,
    /// Start relative to [`WINDOW_START_MICROS`].
    pub start_offset_micros: i64,
    /// `duration_micros`; `end_timestamp` is the start plus this.
    pub duration_micros: i64,
    /// `time_to_first_chunk_ms`.
    pub time_to_first_chunk_ms: Option<i64>,
    /// `status_code`.
    pub status_code: Option<i32>,
    /// `input_tokens`.
    pub input_tokens: Option<i64>,
    /// `output_tokens`.
    pub output_tokens: Option<i64>,
    /// `total_tokens`.
    pub total_tokens: Option<i64>,
    /// `cache_read_input_tokens`.
    pub cache_read_input_tokens: Option<i64>,
    /// `cache_creation_input_tokens`.
    pub cache_creation_input_tokens: Option<i64>,
    /// `tool_name`.
    pub tool_name: Option<&'static str>,
    /// `tool_call_id`.
    pub tool_call_id: Option<&'static str>,
    /// `service_name`.
    pub service_name: Option<&'static str>,
    /// `user_id`.
    pub user_id: Option<&'static str>,
    /// `input_messages`.
    pub input_messages: Option<&'static str>,
    /// `output_messages`.
    pub output_messages: Option<&'static str>,
}

/// The base every seed line overrides with struct-update syntax: a `chat` of tenant A at the
/// window start, carrying no tokens.
pub const OPERATION_DEFAULTS: OperationLine = OperationLine {
    tenant: TENANT_A,
    trace_id: [0; 16],
    span_id: [0; 8],
    conversation_id: None,
    operation_name: "chat",
    provider_name: None,
    request_model: None,
    start_offset_micros: 0,
    duration_micros: 1_000_000,
    time_to_first_chunk_ms: None,
    status_code: Some(0),
    input_tokens: None,
    output_tokens: None,
    total_tokens: None,
    cache_read_input_tokens: None,
    cache_creation_input_tokens: None,
    tool_name: None,
    tool_call_id: None,
    service_name: Some("agent"),
    user_id: None,
    input_messages: None,
    output_messages: None,
};

/// One line as an `operations` row; every unset column is left to the shared row defaults.
fn build_operation_row(line: &OperationLine) -> Row<'_> {
    let start = WINDOW_START_MICROS + line.start_offset_micros;
    let mut cells = vec![
        (COL_TRACE_ID, Cell::Bytes(&line.trace_id)),
        (COL_SPAN_ID, Cell::Bytes(&line.span_id)),
        (OPERATION_NAME, Cell::Text(line.operation_name)),
        (COL_TIMESTAMP, Cell::Micros(start)),
        (COL_END_TIMESTAMP, Cell::Micros(start + line.duration_micros)),
        (COL_INGESTED_TIMESTAMP, Cell::Micros(start)),
        (COL_DURATION_MICROS, Cell::Long(line.duration_micros)),
    ];
    let texts = [
        (COL_CONVERSATION_ID, line.conversation_id),
        ("provider_name", line.provider_name),
        ("request_model", line.request_model),
        ("tool_name", line.tool_name),
        ("tool_call_id", line.tool_call_id),
        (COL_SERVICE_NAME, line.service_name),
        ("user_id", line.user_id),
        ("input_messages", line.input_messages),
        ("output_messages", line.output_messages),
    ];
    cells.extend(
        texts
            .into_iter()
            .filter_map(|(column, value)| value.map(|text| (column, Cell::Text(text)))),
    );
    let counters = [
        ("time_to_first_chunk_ms", line.time_to_first_chunk_ms),
        ("input_tokens", line.input_tokens),
        ("output_tokens", line.output_tokens),
        ("total_tokens", line.total_tokens),
        ("cache_read_input_tokens", line.cache_read_input_tokens),
        ("cache_creation_input_tokens", line.cache_creation_input_tokens),
    ];
    cells.extend(
        counters
            .into_iter()
            .filter_map(|(column, value)| value.map(|number| (column, Cell::Long(number)))),
    );
    if let Some(status_code) = line.status_code {
        cells.push((COL_STATUS_CODE, Cell::Int(i64::from(status_code))));
    }
    Row {
        tenant: line.tenant,
        cells,
    }
}

/// Commit `lines` into `operations` as one append: every tenant's rows in one batch.
pub async fn commit_operation_lines(catalog: &Arc<dyn Catalog>, lines: &[OperationLine]) {
    let rows: Vec<Row<'_>> = lines.iter().map(build_operation_row).collect();
    commit_rows(catalog, OPERATIONS_TABLE, &rows).await.expect("operations rows");
}

/// A required 32-bit integer column.
fn build_int_column(values: impl Iterator<Item = i32>) -> ArrayRef {
    Arc::new(Int32Array::from_iter_values(values))
}

/// A required boolean column.
fn build_bool_column(values: impl Iterator<Item = bool>) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(values.map(Some)))
}

/// A nullable fixed-width binary column of `N`-byte ids.
fn build_fixed_id_column<const N: usize>(values: impl Iterator<Item = Option<[u8; N]>>) -> ArrayRef {
    let width = i32::try_from(N).expect("an id width fits i32");
    Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(values, width).expect("ids of one width"))
}

/// A nullable double column.
fn build_double_column(values: impl Iterator<Item = Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from_iter(values))
}

/// The canonical lowercase hyphenated uuid trust requires of a mark id, numbered so tests can
/// order ids by number: `00000000-0000-4000-8000-<number as 12 hex digits>`.
pub fn format_mark_id(mark_number: u64) -> String {
    format!("00000000-0000-4000-8000-{mark_number:012x}")
}

/// One fingerprint at all four levels, for marks whose level-1 to level-3 keys a test never reads.
pub fn repeat_fingerprint(fingerprint: &str) -> [String; 4] {
    std::array::from_fn(|_| fingerprint.to_owned())
}

/// The scope columns of one mark.
#[derive(Clone, Default)]
pub struct MarkScope {
    /// `service_name`.
    pub service_name: Option<&'static str>,
    /// `environment`.
    pub environment: Option<&'static str>,
    /// `agent_name`.
    pub agent_name: Option<&'static str>,
    /// `agent_version`.
    pub agent_version: Option<&'static str>,
    /// `workflow_name`.
    pub workflow_name: Option<&'static str>,
    /// `model`.
    pub model: Option<&'static str>,
    /// `provider`.
    pub provider: Option<&'static str>,
}

/// One `marks` row with explicit values.
#[derive(Clone)]
pub struct MarkRow {
    /// The tenant the row belongs to.
    pub tenant: &'static str,
    /// `mark_id`, in [`format_mark_id`] form.
    pub mark_id: String,
    /// `scan_id`: ranks a mark's rows after `scanned_at`.
    pub scan_id: &'static str,
    /// `attempt`: ranks a mark's rows after `scan_id`.
    pub attempt: i32,
    /// `scanned_at`, in microseconds.
    pub scanned_at_micros: i64,
    /// `occurred_at`, in microseconds.
    pub occurred_at_micros: i64,
    /// `retracted`.
    pub is_retracted: bool,
    /// `trace_id`.
    pub trace_id: [u8; 16],
    /// `root_span_id`.
    pub root_span_id: Option<[u8; 8]>,
    /// `rule_id`.
    pub rule_id: &'static str,
    /// `fp_version`.
    pub fp_version: i32,
    /// `fp_epoch`.
    pub fp_epoch: i32,
    /// `fingerprint`, `fingerprint_l1`, `fingerprint_l2`, `fingerprint_l3`.
    pub fingerprints: [String; 4],
    /// `localization_kind`.
    pub localization_kind: &'static str,
    /// `localization_key`.
    pub localization_key: &'static str,
    /// `anchor_name`.
    pub anchor_name: Option<&'static str>,
    /// `variant`.
    pub variant: Option<&'static str>,
    /// `evidence`.
    pub evidence: Option<&'static str>,
    /// The seven scope columns.
    pub scope: MarkScope,
    /// `severity`: `low`, `medium` or `high`.
    pub severity: &'static str,
    /// `impact_cost_usd` as decimal text.
    pub impact_cost_usd: Option<&'static str>,
    /// `impact_added_latency_micros`.
    pub impact_added_latency_micros: Option<i64>,
    /// `impact_input_tokens`.
    pub impact_input_tokens: Option<i64>,
    /// `impact_output_tokens`.
    pub impact_output_tokens: Option<i64>,
    /// `impact_cacheable_tokens`.
    pub impact_cacheable_tokens: Option<i64>,
    /// `run_quality_z`.
    pub run_quality_z: Option<f64>,
}

impl MarkRow {
    /// A live mark of rule `rule-a` (version 1, epoch 1) scanned when it occurred by `scan-1`,
    /// attempt 1: localized `code` at `src/lib.rs:10`, anchor `anchor`, evidence `{}`, service
    /// `svc`, severity `low`, no impact figures.
    pub fn new(
        tenant: &'static str,
        mark_number: u64,
        occurred_at_micros: i64,
        trace_id: [u8; 16],
        fingerprints: [String; 4],
    ) -> Self {
        Self {
            tenant,
            mark_id: format_mark_id(mark_number),
            scan_id: "scan-1",
            attempt: 1,
            scanned_at_micros: occurred_at_micros,
            occurred_at_micros,
            is_retracted: false,
            trace_id,
            root_span_id: None,
            rule_id: "rule-a",
            fp_version: 1,
            fp_epoch: 1,
            fingerprints,
            localization_kind: "code",
            localization_key: "src/lib.rs:10",
            anchor_name: Some("anchor"),
            variant: None,
            evidence: Some("{}"),
            scope: MarkScope {
                service_name: Some("svc"),
                ..MarkScope::default()
            },
            severity: "low",
            impact_cost_usd: None,
            impact_added_latency_micros: None,
            impact_input_tokens: None,
            impact_output_tokens: None,
            impact_cacheable_tokens: None,
            run_quality_z: None,
        }
    }

    /// The row a later scan writes to retract this mark at `scanned_at_micros`.
    pub fn build_retraction(&self, scanned_at_micros: i64) -> Self {
        Self {
            scan_id: "scan-retract",
            scanned_at_micros,
            is_retracted: true,
            ..self.clone()
        }
    }
}

/// Commit `rows` into `marks` as one snapshot: every tenant's rows in one batch.
pub async fn commit_mark_rows(catalog: &Arc<dyn Catalog>, rows: &[MarkRow]) {
    let columns: HashMap<&'static str, ArrayRef> = HashMap::from([
        (
            COL_TENANT_ID,
            build_string_column(rows.iter().map(|row| Some(row.tenant))),
        ),
        (
            "mark_id",
            build_string_column(rows.iter().map(|row| Some(row.mark_id.as_str()))),
        ),
        ("scan_id", build_string_column(rows.iter().map(|row| Some(row.scan_id)))),
        ("attempt", build_int_column(rows.iter().map(|row| row.attempt))),
        (
            "scanned_at",
            build_timestamp_column(rows.iter().map(|row| row.scanned_at_micros)),
        ),
        (
            "occurred_at",
            build_timestamp_column(rows.iter().map(|row| row.occurred_at_micros)),
        ),
        ("retracted", build_bool_column(rows.iter().map(|row| row.is_retracted))),
        (
            "trace_id",
            build_fixed_id_column(rows.iter().map(|row| Some(row.trace_id))),
        ),
        (
            "root_span_id",
            build_fixed_id_column(rows.iter().map(|row| row.root_span_id)),
        ),
        ("rule_id", build_string_column(rows.iter().map(|row| Some(row.rule_id)))),
        ("fp_version", build_int_column(rows.iter().map(|row| row.fp_version))),
        ("fp_epoch", build_int_column(rows.iter().map(|row| row.fp_epoch))),
        (
            "fingerprint",
            build_string_column(rows.iter().map(|row| Some(row.fingerprints[0].as_str()))),
        ),
        (
            "fingerprint_l1",
            build_string_column(rows.iter().map(|row| Some(row.fingerprints[1].as_str()))),
        ),
        (
            "fingerprint_l2",
            build_string_column(rows.iter().map(|row| Some(row.fingerprints[2].as_str()))),
        ),
        (
            "fingerprint_l3",
            build_string_column(rows.iter().map(|row| Some(row.fingerprints[3].as_str()))),
        ),
        (
            "localization_kind",
            build_string_column(rows.iter().map(|row| Some(row.localization_kind))),
        ),
        (
            "localization_key",
            build_string_column(rows.iter().map(|row| Some(row.localization_key))),
        ),
        (
            "anchor_name",
            build_string_column(rows.iter().map(|row| row.anchor_name)),
        ),
        ("variant", build_string_column(rows.iter().map(|row| row.variant))),
        ("evidence", build_string_column(rows.iter().map(|row| row.evidence))),
        (
            "service_name",
            build_string_column(rows.iter().map(|row| row.scope.service_name)),
        ),
        (
            "environment",
            build_string_column(rows.iter().map(|row| row.scope.environment)),
        ),
        (
            "agent_name",
            build_string_column(rows.iter().map(|row| row.scope.agent_name)),
        ),
        (
            "agent_version",
            build_string_column(rows.iter().map(|row| row.scope.agent_version)),
        ),
        (
            "workflow_name",
            build_string_column(rows.iter().map(|row| row.scope.workflow_name)),
        ),
        ("model", build_string_column(rows.iter().map(|row| row.scope.model))),
        (
            "provider",
            build_string_column(rows.iter().map(|row| row.scope.provider)),
        ),
        (
            "severity",
            build_string_column(rows.iter().map(|row| Some(row.severity))),
        ),
        (
            "impact_cost_usd",
            build_string_column(rows.iter().map(|row| row.impact_cost_usd)),
        ),
        (
            "impact_added_latency_micros",
            build_long_column(rows.iter().map(|row| row.impact_added_latency_micros)),
        ),
        (
            "impact_input_tokens",
            build_long_column(rows.iter().map(|row| row.impact_input_tokens)),
        ),
        (
            "impact_output_tokens",
            build_long_column(rows.iter().map(|row| row.impact_output_tokens)),
        ),
        (
            "impact_cacheable_tokens",
            build_long_column(rows.iter().map(|row| row.impact_cacheable_tokens)),
        ),
        (
            "run_quality_z",
            build_double_column(rows.iter().map(|row| row.run_quality_z)),
        ),
    ]);
    commit_columns(catalog, MARKS_TABLE, columns, rows.len()).await;
}
